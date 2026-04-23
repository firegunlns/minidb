// Package bptree 实现了 B+ 树数据结构
// 本文件实现了持久化B+树，支持磁盘存储、LRU缓存、并发控制、Bloom过滤器和压缩
package bptree

import (
	"bytes"
	"encoding/binary"
	"errors"
	"sync"
	"sync/atomic"
	"time"

	"lns.com/minidb/metrics"
)

// nilPage 表示空页面的特殊ID
const nilPage int64 = -1

// pnode 持久化节点（B+树节点的磁盘版本）
// 与内存节点node的区别：指针被页面ID替代
// mu: 节点级锁（读写锁，用于并发控制）
// pageID: 页面ID
// isLeaf: 是否为叶子节点
// keys: 存储的键
// values: 存储的值（仅叶子节点使用）
// indices: 逻辑索引
// children: 子节点页面ID（仅内部节点使用）
// next: 下一个叶子节点的页面ID（构成链表）
// parent: 父节点页面ID
// dirty: 是否有未写入磁盘的修改
// bloom: Bloom过滤器（仅叶子节点使用，用于快速判断键是否可能存在）
// overflowPages: 溢出页面ID（当节点数据超过单页大小时使用）
type pnode struct {
	mu            sync.RWMutex
	pageID        int64
	isLeaf        bool
	keys          [][]byte
	values        [][]byte
	indices       []int
	children      []int64
	next          int64
	parent        int64
	dirty         bool
	bloom         *BloomFilter
	overflowPages []int64
}

// numKeys 返回节点当前存储的键数量
func (n *pnode) numKeys() int { return len(n.indices) }

// keyAt 返回指定索引位置的键
func (n *pnode) keyAt(i int) []byte { return n.keys[n.indices[i]] }

// valAt 返回指定索引位置的值
func (n *pnode) valAt(i int) []byte { return n.values[n.indices[i]] }

// PersistentBPTree 持久化B+树
// 通过LRU缓冲区缓存将节点持久化到磁盘
// 并发控制使用节点级锁耦合：
// - writeMu 用RWMutex保护rootID：写操作RLock（允许并发），结构变更Lock
// - 读者从不获取writeMu，而是使用每节点RLock螃蟹锁
// - rootID通过原子操作访问，读者在锁定根节点后进行验证
type PersistentBPTree struct {
	writeMu       sync.RWMutex // RLock=允许并发写，Lock=结构变更（创建新root）
	rootID        int64        // 通过sync/atomic访问
	pager         *Pager       // 页面管理器
	cache         *LRUCache    // LRU缓存
	order         int          // B+树阶
	config        Config       // 配置选项
	formatVersion uint32       // 格式版本：1=传统版本，2=支持bloom过滤器和压缩
}

// ---------- open / close ----------

func OpenPersistentBPTree(filePath string, order, cacheSize int, opts ...Option) (*PersistentBPTree, error) {
	if order < 3 {
		order = 3
	}
	if cacheSize < 10 {
		cacheSize = 10
	}

	cfg := defaultConfig()
	for _, o := range opts {
		o(&cfg)
	}

	pager, err := NewPager(filePath, 0)
	if err != nil {
		return nil, err
	}

	formatVersion := uint32(1) // default: legacy format
	h, err := pager.ReadHeader()
	if err == nil && h.PageCount > 0 {
		formatVersion = h.Version
		if formatVersion < 1 {
			formatVersion = 1
		}
	}

	// If features are enabled, use v2 format.
	if cfg.BloomEnabled || cfg.CompressionEnabled {
		formatVersion = 2
	}

	var compressor Compressor
	if cfg.CompressionEnabled {
		compressor = &SnappyCompressor{}
	}

	t := &PersistentBPTree{
		pager:         pager,
		cache:         NewLRUCache(cacheSize, pager, compressor, formatVersion),
		rootID:        nilPage,
		order:         order,
		config:        cfg,
		formatVersion: formatVersion,
	}

	if err == nil && h.PageCount > 0 {
		atomic.StoreInt64(&t.rootID, h.RootPageID)
		t.order = int(h.Order)
	}

	return t, nil
}

func (t *PersistentBPTree) Close() error {
	t.writeMu.Lock()
	defer t.writeMu.Unlock()
	t.cache.Flush()
	rootID := atomic.LoadInt64(&t.rootID)
	t.pager.WriteHeader(rootID, t.order, t.formatVersion)
	return t.pager.Close()
}

func (t *PersistentBPTree) Sync() error {
	t.writeMu.RLock()
	defer t.writeMu.RUnlock()
	t.cache.Flush()
	rootID := atomic.LoadInt64(&t.rootID)
	if err := t.pager.WriteHeader(rootID, t.order, t.formatVersion); err != nil {
		return err
	}
	return t.pager.Sync()
}

// ---------- helpers ----------

func (t *PersistentBPTree) maxKeys() int { return t.order - 1 }
func (t *PersistentBPTree) minKeys() int { return (t.order - 1) / 2 }

func (t *PersistentBPTree) loadAndPin(pageID int64) (*pnode, error) {
	if pageID == nilPage {
		return nil, nil
	}
	return t.cache.GetOrLoad(pageID)
}

func (t *PersistentBPTree) unpin(pageID int64) {
	if pageID == nilPage {
		return
	}
	t.cache.Unpin(pageID)
}

func (t *PersistentBPTree) allocNode(isLeaf bool) (*pnode, error) {
	id, err := t.pager.Allocate()
	if err != nil {
		return nil, err
	}
	n := &pnode{
		pageID: id,
		isLeaf: isLeaf,
		parent: nilPage,
		next:   nilPage,
		dirty:  true,
	}
	if isLeaf && t.config.BloomEnabled {
		n.bloom = NewBloomFilter(t.maxKeys(), t.config.BloomBitsPerKey)
	}
	t.cache.PutPinned(n) // pins=1, caller must unpin
	return n, nil
}

// latchRootRLock atomically reads rootID, pins and read-locks the root node,
// then validates that rootID has not changed. Retries until stable.
// Returns (nil, nil) when the tree is empty.
func (t *PersistentBPTree) latchRootRLock() (*pnode, error) {
	start := time.Now()
	for {
		rootID := atomic.LoadInt64(&t.rootID)
		if rootID == nilPage {
			metrics.BPTreeLatchDuration.WithLabelValues("rlock").Observe(time.Since(start).Seconds())
			return nil, nil
		}
		n, err := t.loadAndPin(rootID)
		if err != nil {
			metrics.BPTreeLatchDuration.WithLabelValues("rlock").Observe(time.Since(start).Seconds())
			return nil, err
		}
		n.mu.RLock()
		if atomic.LoadInt64(&t.rootID) == rootID {
			metrics.BPTreeLatchDuration.WithLabelValues("rlock").Observe(time.Since(start).Seconds())
			return n, nil
		}
		n.mu.RUnlock()
		t.unpin(rootID)
	}
}

// latchRootWLock is the write-lock variant, used after writeMu is held.
func (t *PersistentBPTree) latchRootWLock() (*pnode, error) {
	start := time.Now()
	for {
		rootID := atomic.LoadInt64(&t.rootID)
		if rootID == nilPage {
			metrics.BPTreeLatchDuration.WithLabelValues("wlock").Observe(time.Since(start).Seconds())
			return nil, nil
		}
		n, err := t.loadAndPin(rootID)
		if err != nil {
			metrics.BPTreeLatchDuration.WithLabelValues("wlock").Observe(time.Since(start).Seconds())
			return nil, err
		}
		n.mu.Lock()
		if atomic.LoadInt64(&t.rootID) == rootID {
			metrics.BPTreeLatchDuration.WithLabelValues("wlock").Observe(time.Since(start).Seconds())
			return n, nil
		}
		n.mu.Unlock()
		t.unpin(rootID)
	}
}

// ---------- public API: reads ----------

// Find returns the value associated with key.
func (t *PersistentBPTree) Find(key []byte) ([]byte, bool) {
	start := time.Now()
	root, err := t.latchRootRLock()
	if err != nil || root == nil {
		metrics.BPTreeOpDuration.WithLabelValues("find").Observe(time.Since(start).Seconds())
		metrics.BPTreeOpsTotal.WithLabelValues("find").Inc()
		return nil, false
	}

	cur := root
	for !cur.isLeaf {
		ci := childIndex(cur, key)
		childID := cur.children[ci]
		child, err := t.loadAndPin(childID)
		if err != nil {
			cur.mu.RUnlock()
			t.unpin(cur.pageID)
			metrics.BPTreeOpDuration.WithLabelValues("find").Observe(time.Since(start).Seconds())
			metrics.BPTreeOpsTotal.WithLabelValues("find").Inc()
			return nil, false
		}
		child.mu.RLock()
		cur.mu.RUnlock()
		t.unpin(cur.pageID)
		cur = child
	}

	// Bloom filter: fast negative lookup before binary search.
	if cur.bloom != nil && !cur.bloom.MayContain(key) {
		cur.mu.RUnlock()
		t.unpin(cur.pageID)
		metrics.BPTreeOpDuration.WithLabelValues("find").Observe(time.Since(start).Seconds())
		metrics.BPTreeOpsTotal.WithLabelValues("find").Inc()
		return nil, false
	}

	idx := leafSearch(cur, key)
	if idx < cur.numKeys() && bytes.Equal(cur.keyAt(idx), key) {
		val := copyBytes(cur.valAt(idx))
		cur.mu.RUnlock()
		t.unpin(cur.pageID)
		metrics.BPTreeOpDuration.WithLabelValues("find").Observe(time.Since(start).Seconds())
		metrics.BPTreeOpsTotal.WithLabelValues("find").Inc()
		return val, true
	}

	// Key not found in this leaf. A concurrent split may have created
	// new leaves to the right. Walk the next-chain until we find a leaf
	// whose max key >= our key, or we pass where the key would be.
	// Also handles empty leaves (numKeys == 0) which can occur after
	// concurrent deletes.
	for {
		if cur.numKeys() > 0 {
			if bytes.Compare(key, cur.keyAt(cur.numKeys()-1)) <= 0 {
				// key <= max key in this leaf, but not found. Key doesn't exist.
				break
			}
		}
		// key > max key (or leaf is empty) — check next leaf.
		if cur.next == nilPage {
			break
		}
		nextID := cur.next
		cur.mu.RUnlock()
		t.unpin(cur.pageID)
		var err error
		cur, err = t.loadAndPin(nextID)
		if err != nil {
			metrics.BPTreeOpDuration.WithLabelValues("find").Observe(time.Since(start).Seconds())
			metrics.BPTreeOpsTotal.WithLabelValues("find").Inc()
			return nil, false
		}
		cur.mu.RLock()
		idx = leafSearch(cur, key)
		if idx < cur.numKeys() && bytes.Equal(cur.keyAt(idx), key) {
			val := copyBytes(cur.valAt(idx))
			cur.mu.RUnlock()
			t.unpin(cur.pageID)
			metrics.BPTreeOpDuration.WithLabelValues("find").Observe(time.Since(start).Seconds())
			metrics.BPTreeOpsTotal.WithLabelValues("find").Inc()
			return val, true
		}
	}

	cur.mu.RUnlock()
	t.unpin(cur.pageID)
	metrics.BPTreeOpDuration.WithLabelValues("find").Observe(time.Since(start).Seconds())
	metrics.BPTreeOpsTotal.WithLabelValues("find").Inc()
	return nil, false
}

// RangeScan returns all key-value pairs where start <= key <= end.
func (t *PersistentBPTree) RangeScan(start, end []byte) []KeyValuePair {
	scanStart := time.Now()
	leaf := t.findLeafRLatch(start)

	var results []KeyValuePair
	for leaf != nil {
		for i := 0; i < leaf.numKeys(); i++ {
			k := leaf.keyAt(i)
			if bytes.Compare(k, end) > 0 {
				leaf.mu.RUnlock()
				t.unpin(leaf.pageID)
				metrics.BPTreeOpDuration.WithLabelValues("range_scan").Observe(time.Since(scanStart).Seconds())
				metrics.BPTreeOpsTotal.WithLabelValues("range_scan").Inc()
				return results
			}
			if bytes.Compare(k, start) >= 0 {
				results = append(results, KeyValuePair{
					Key:   copyBytes(k),
					Value: copyBytes(leaf.valAt(i)),
				})
			}
		}
		nextID := leaf.next
		leaf.mu.RUnlock()
		t.unpin(leaf.pageID)
		if nextID == nilPage {
			break
		}
		var err error
		leaf, err = t.loadAndPin(nextID)
		if err != nil {
			break
		}
		leaf.mu.RLock()
	}
	metrics.BPTreeOpDuration.WithLabelValues("range_scan").Observe(time.Since(scanStart).Seconds())
	metrics.BPTreeOpsTotal.WithLabelValues("range_scan").Inc()
	return results
}

// RangeScanFn calls fn for each key-value pair where start <= key <= end.
// Key and value are direct references into the leaf page — fn must not retain
// them after returning. If fn returns false the scan stops immediately.
func (t *PersistentBPTree) RangeScanFn(start, end []byte, fn func(key, value []byte) bool) {
	leaf := t.findLeafRLatch(start)
	for leaf != nil {
		for i := 0; i < leaf.numKeys(); i++ {
			k := leaf.keyAt(i)
			if bytes.Compare(k, end) > 0 {
				leaf.mu.RUnlock()
				t.unpin(leaf.pageID)
				return
			}
			if bytes.Compare(k, start) >= 0 {
				if !fn(k, leaf.valAt(i)) {
					leaf.mu.RUnlock()
					t.unpin(leaf.pageID)
					return
				}
			}
		}
		nextID := leaf.next
		leaf.mu.RUnlock()
		t.unpin(leaf.pageID)
		if nextID == nilPage {
			break
		}
		var err error
		leaf, err = t.loadAndPin(nextID)
		if err != nil {
			break
		}
		leaf.mu.RLock()
	}
}

// CountRange counts keys in [start, end] without reading values.
// It traverses leaf pages and counts keys without copying any value data.
func (t *PersistentBPTree) CountRange(start, end []byte) int64 {
	leaf := t.findLeafRLatch(start)

	var count int64
	for leaf != nil {
		for i := 0; i < leaf.numKeys(); i++ {
			k := leaf.keyAt(i)
			if bytes.Compare(k, end) > 0 {
				leaf.mu.RUnlock()
				t.unpin(leaf.pageID)
				return count
			}
			if bytes.Compare(k, start) >= 0 {
				count++
			}
		}
		nextID := leaf.next
		leaf.mu.RUnlock()
		t.unpin(leaf.pageID)
		if nextID == nilPage {
			break
		}
		var err error
		leaf, err = t.loadAndPin(nextID)
		if err != nil {
			break
		}
		leaf.mu.RLock()
	}
	return count
}

// ScanKeys returns only the keys in [start, end] without any value data.
// Used by MVCC layer for counting distinct PKs without the cost of copying values.
func (t *PersistentBPTree) ScanKeys(start, end []byte) [][]byte {
	leaf := t.findLeafRLatch(start)

	var keys [][]byte
	for leaf != nil {
		for i := 0; i < leaf.numKeys(); i++ {
			k := leaf.keyAt(i)
			if bytes.Compare(k, end) > 0 {
				leaf.mu.RUnlock()
				t.unpin(leaf.pageID)
				return keys
			}
			if bytes.Compare(k, start) >= 0 {
				keys = append(keys, copyBytes(k))
			}
		}
		nextID := leaf.next
		leaf.mu.RUnlock()
		t.unpin(leaf.pageID)
		if nextID == nilPage {
			break
		}
		var err error
		leaf, err = t.loadAndPin(nextID)
		if err != nil {
			break
		}
		leaf.mu.RLock()
	}
	return keys
}

// Count returns the total number of keys in the tree.
func (t *PersistentBPTree) Count() int {
	root, err := t.latchRootRLock()
	if err != nil || root == nil {
		return 0
	}

	cur := root
	for !cur.isLeaf {
		childID := cur.children[0]
		child, err := t.loadAndPin(childID)
		if err != nil {
			cur.mu.RUnlock()
			t.unpin(cur.pageID)
			return 0
		}
		child.mu.RLock()
		cur.mu.RUnlock()
		t.unpin(cur.pageID)
		cur = child
	}

	count := 0
	for cur != nil {
		count += cur.numKeys()
		nextID := cur.next
		cur.mu.RUnlock()
		t.unpin(cur.pageID)
		if nextID == nilPage {
			break
		}
		cur, err = t.loadAndPin(nextID)
		if err != nil {
			break
		}
		cur.mu.RLock()
	}
	return count
}

// Validate checks tree invariants (for testing).
func (t *PersistentBPTree) Validate() bool {
	t.writeMu.Lock()
	defer t.writeMu.Unlock()

	rootID := atomic.LoadInt64(&t.rootID)
	if rootID == nilPage {
		return true
	}
	root, err := t.loadAndPin(rootID)
	if err != nil {
		return false
	}
	root.mu.RLock()
	ok := t.validateLocked(root, true)
	root.mu.RUnlock()
	t.unpin(root.pageID)
	return ok
}

// ---------- public API: writes ----------

// errRetry is returned internally when a concurrent structural change invalidates
// the descent path and the operation needs to be retried.
var errRetry = errors.New("bptree: concurrent split, retry")

// Insert associates key with value, overwriting any existing entry.
func (t *PersistentBPTree) Insert(key, value []byte) error {
	start := time.Now()
	// Fast path: empty tree.
	rootID := atomic.LoadInt64(&t.rootID)
	if rootID == nilPage {
		err := t.insertEmpty(key, value)
		metrics.BPTreeOpDuration.WithLabelValues("insert").Observe(time.Since(start).Seconds())
		metrics.BPTreeOpsTotal.WithLabelValues("insert").Inc()
		return err
	}

	// Try optimistic descent first (no split case).
	t.writeMu.RLock()
	err := t.insertAttempt(key, value)
	t.writeMu.RUnlock()
	for retries := 0; err == errRetry && retries < 256; retries++ {
		// Leaf is full or concurrent split. Use pessimistic crabbing.
		t.writeMu.RLock()
		err = t.insertPessimistic(key, value)
		t.writeMu.RUnlock()
	}
	metrics.BPTreeOpDuration.WithLabelValues("insert").Observe(time.Since(start).Seconds())
	metrics.BPTreeOpsTotal.WithLabelValues("insert").Inc()
	return err
}

func (t *PersistentBPTree) insertEmpty(key, value []byte) error {
	t.writeMu.Lock()
	defer t.writeMu.Unlock()

	// Double-check under writeMu exclusive lock.
	if atomic.LoadInt64(&t.rootID) != nilPage {
		// Tree is no longer empty — use pessimistic path directly
		// (we hold exclusive writeMu, no contention).
		return t.insertPessimisticLocked(key, value)
	}

	n, err := t.allocNode(true)
	if err != nil {
		return err
	}
	n.mu.Lock()
	n.keys = [][]byte{copyBytes(key)}
	n.values = [][]byte{copyBytes(value)}
	n.indices = []int{0}
	atomic.StoreInt64(&t.rootID, n.pageID)
	n.mu.Unlock()
	t.unpin(n.pageID)
	return nil
}

func (t *PersistentBPTree) insertAttempt(key, value []byte) error {
	// Optimistic descent disabled for now — concurrent splits during
	// optimistic descent can cause data corruption (keys placed in wrong
	// leaf, Find misses). Always use pessimistic crabbing which is safe
	// because it holds write locks on all ancestors.
	return errRetry
}

// insertPessimistic uses write-lock crabbing on the full descent path.
// This is the fallback when the optimistic path detects a full leaf (split needed).
// All ancestor locks are retained (no optimistic release) because multiple
// concurrent writers (writeMu is RWMutex) can modify the tree structure.
func (t *PersistentBPTree) insertPessimistic(key, value []byte) error {
	root, err := t.latchRootWLock()
	if err != nil {
		return err
	}
	if root == nil {
		// Tree became empty between Insert's check and now.
		// Release writeMu and retry from the top (insertEmpty path).
		return errRetry
	}

	var chain lockChain
	cur := root

	for !cur.isLeaf {
		ci := childIndex(cur, key)
		childID := cur.children[ci]
		child, err := t.loadAndPin(childID)
		if err != nil {
			cur.mu.Unlock()
			t.unpin(cur.pageID)
			chain.releaseAll(t)
			return err
		}
		child.mu.Lock()
		// Conservative: always keep parent in chain (no optimistic release).
		// Multiple concurrent writers make optimistic release unsafe because
		// another writer's split could propagate up into our path.
		chain.push(cur)
		cur = child
	}

	t.insertIntoLeafLocked(cur, key, value)
	if cur.numKeys() <= t.maxKeys() {
		cur.mu.Unlock()
		t.unpin(cur.pageID)
		chain.releaseAll(t)
		return nil
	}

	return t.splitLeafLocked(cur, &chain)
}

// insertPessimisticLocked is like insertPessimistic but assumes writeMu is
// already held exclusively (called from insertEmpty).
func (t *PersistentBPTree) insertPessimisticLocked(key, value []byte) error {
	rootID := atomic.LoadInt64(&t.rootID)
	if rootID == nilPage {
		return errRetry
	}
	root, err := t.loadAndPin(rootID)
	if err != nil {
		return err
	}
	root.mu.Lock()

	var chain lockChain
	cur := root

	for !cur.isLeaf {
		ci := childIndex(cur, key)
		childID := cur.children[ci]
		child, err := t.loadAndPin(childID)
		if err != nil {
			cur.mu.Unlock()
			t.unpin(cur.pageID)
			chain.releaseAll(t)
			return err
		}
		child.mu.Lock()
		// Safe to use optimistic crabbing here: writeMu is held exclusively,
		// so no other writer can interfere.
		if child.numKeys()+1 <= t.maxKeys() {
			chain.releaseAll(t)
			cur.mu.Unlock()
			t.unpin(cur.pageID)
		} else {
			chain.push(cur)
		}
		cur = child
	}

	t.insertIntoLeafLocked(cur, key, value)
	if cur.numKeys() <= t.maxKeys() {
		cur.mu.Unlock()
		t.unpin(cur.pageID)
		chain.releaseAll(t)
		return nil
	}

	return t.splitLeafLocked(cur, &chain)
}

// Delete removes key from the tree.
func (t *PersistentBPTree) Delete(key []byte) bool {
	start := time.Now()
	t.writeMu.RLock()
	found := t.deleteAttempt(key)
	t.writeMu.RUnlock()
	metrics.BPTreeOpDuration.WithLabelValues("delete").Observe(time.Since(start).Seconds())
	metrics.BPTreeOpsTotal.WithLabelValues("delete").Inc()
	return found
}

// BatchInsert inserts multiple key-value pairs with a single writeMu RLock.
// This is significantly more efficient than individual Insert calls when writing
// multiple entries to the same tree because it amortizes lock overhead.
func (t *PersistentBPTree) BatchInsert(pairs [][2][]byte) error {
	if len(pairs) == 0 {
		return nil
	}
	start := time.Now()
	t.writeMu.RLock()

	// Handle empty tree: insert first pair as root (upgrade to exclusive).
	rootID := atomic.LoadInt64(&t.rootID)
	if rootID == nilPage {
		t.writeMu.RUnlock()
		t.writeMu.Lock()
		// Double-check after acquiring exclusive lock.
		if atomic.LoadInt64(&t.rootID) != nilPage {
			// Another goroutine created the root — downgrade to RLock.
			t.writeMu.Unlock()
			t.writeMu.RLock()
		} else {
			n, err := t.allocNode(true)
			if err != nil {
				t.writeMu.Unlock()
				return err
			}
			n.mu.Lock()
			n.keys = [][]byte{copyBytes(pairs[0][0])}
			n.values = [][]byte{copyBytes(pairs[0][1])}
			n.indices = []int{0}
			atomic.StoreInt64(&t.rootID, n.pageID)
			n.mu.Unlock()
			t.unpin(n.pageID)
			t.writeMu.Unlock()
			t.writeMu.RLock()
			pairs = pairs[1:]
		}
	}

	// Insert remaining pairs with retry on concurrent split.
	var firstErr error
	for _, p := range pairs {
		err := t.insertAttempt(p[0], p[1])
		if err == errRetry {
			for retries := 0; retries < 64; retries++ {
				err = t.insertPessimistic(p[0], p[1])
				if err != errRetry {
					break
				}
			}
		}
		if err != nil {
			firstErr = err
			break
		}
	}

	t.writeMu.RUnlock()
	metrics.BPTreeOpDuration.WithLabelValues("batch_insert").Observe(time.Since(start).Seconds())
	metrics.BPTreeOpsTotal.WithLabelValues("batch_insert").Inc()
	return firstErr
}

// BatchDelete removes multiple keys with a single writeMu RLock.
func (t *PersistentBPTree) BatchDelete(keys [][]byte) {
	if len(keys) == 0 {
		return
	}
	start := time.Now()
	t.writeMu.RLock()
	for _, key := range keys {
		t.deleteAttempt(key)
	}
	t.writeMu.RUnlock()
	metrics.BPTreeOpDuration.WithLabelValues("batch_delete").Observe(time.Since(start).Seconds())
	metrics.BPTreeOpsTotal.WithLabelValues("batch_delete").Inc()
}

func (t *PersistentBPTree) deleteAttempt(key []byte) bool {
	// Always use pessimistic crabbing for deletes. Delete operations
	// are less frequent than inserts in OLTP workloads (TPC-C), and the
	// optimistic descent is risky for deletes because a key may land in
	// the wrong leaf after a concurrent split.
	return t.deletePessimistic(key)
}

// deletePessimistic uses write-lock crabbing for the full descent.
// This is the fallback when the optimistic path detects potential underflow.
func (t *PersistentBPTree) deletePessimistic(key []byte) bool {
	root, err := t.latchRootWLock()
	if err != nil || root == nil {
		return false
	}

	var chain lockChain
	cur := root

	for !cur.isLeaf {
		ci := childIndex(cur, key)
		childID := cur.children[ci]
		child, err := t.loadAndPin(childID)
		if err != nil {
			cur.mu.Unlock()
			t.unpin(cur.pageID)
			chain.releaseAll(t)
			return false
		}
		child.mu.Lock()

		// Conservative: always keep parent in chain (no optimistic release).
		// Concurrent writers make optimistic release unsafe because another
		// writer's merge/borrow could propagate up into our path.
		chain.push(cur)
		cur = child
	}

	leaf := cur
	idx := leafSearch(leaf, key)
	if idx >= leaf.numKeys() || !bytes.Equal(leaf.keyAt(idx), key) {
		leaf.mu.Unlock()
		t.unpin(leaf.pageID)
		chain.releaseAll(t)
		return false
	}

	leaf.indices = append(leaf.indices[:idx], leaf.indices[idx+1:]...)
	leaf.dirty = true
	t.rebuildLeafBloom(leaf)

	if leaf.parent == nilPage || leaf.numKeys() >= t.minKeys() {
		leaf.mu.Unlock()
		t.unpin(leaf.pageID)
		chain.releaseAll(t)
		t.maybeCollapseRoot()
		return true
	}

	t.handleUnderflowLocked(leaf, &chain)
	chain.releaseAll(t)
	t.maybeCollapseRoot()
	return true
}

// ---------- internal: search helpers ----------

func (t *PersistentBPTree) findLeafRLatch(key []byte) *pnode {
	root, err := t.latchRootRLock()
	if err != nil || root == nil {
		return nil
	}
	cur := root
	for !cur.isLeaf {
		ci := childIndex(cur, key)
		childID := cur.children[ci]
		child, err := t.loadAndPin(childID)
		if err != nil {
			cur.mu.RUnlock()
			t.unpin(cur.pageID)
			return nil
		}
		child.mu.RLock()
		cur.mu.RUnlock()
		t.unpin(cur.pageID)
		cur = child
	}
	return cur // RLock'd and pinned
}

func leafSearch(n *pnode, key []byte) int {
	lo, hi := 0, n.numKeys()
	for lo < hi {
		mid := (lo + hi) / 2
		if bytes.Compare(key, n.keyAt(mid)) > 0 {
			lo = mid + 1
		} else {
			hi = mid
		}
	}
	return lo
}

func childIndex(n *pnode, key []byte) int {
	lo, hi := 0, n.numKeys()
	for lo < hi {
		mid := (lo + hi) / 2
		if bytes.Compare(key, n.keyAt(mid)) >= 0 {
			lo = mid + 1
		} else {
			hi = mid
		}
	}
	return lo
}

func (t *PersistentBPTree) childIndexOf(p *pnode, childID int64) int {
	for i, c := range p.children {
		if c == childID {
			return i
		}
	}
	return -1
}

// rebuildLeafBloom reconstructs the bloom filter for a leaf node from its
// current keys. No-op if bloom filters are not enabled.
func (t *PersistentBPTree) rebuildLeafBloom(leaf *pnode) {
	if !t.config.BloomEnabled || !leaf.isLeaf {
		return
	}
	keys := make([][]byte, leaf.numKeys())
	for i := range keys {
		keys[i] = leaf.keyAt(i)
	}
	leaf.bloom = NewBloomFilter(t.maxKeys(), t.config.BloomBitsPerKey)
	leaf.bloom.Rebuild(keys)
}

// ---------- internal: insert ----------

func (t *PersistentBPTree) insertIntoLeafLocked(leaf *pnode, key, value []byte) {
	idx := leafSearch(leaf, key)
	if idx < leaf.numKeys() && bytes.Equal(leaf.keyAt(idx), key) {
		leaf.values[leaf.indices[idx]] = copyBytes(value)
		leaf.dirty = true
		return
	}
	pos := len(leaf.keys)
	leaf.keys = append(leaf.keys, copyBytes(key))
	leaf.values = append(leaf.values, copyBytes(value))
	leaf.indices = append(leaf.indices, 0)
	copy(leaf.indices[idx+1:], leaf.indices[idx:])
	leaf.indices[idx] = pos
	leaf.dirty = true
	if leaf.bloom != nil {
		leaf.bloom.Add(key)
	}
}

func compactPNode(src *pnode, start, end int) ([][]byte, [][]byte, []int) {
	n := end - start
	keys := make([][]byte, n)
	vals := make([][]byte, n)
	indices := make([]int, n)
	for i := 0; i < n; i++ {
		p := src.indices[start+i]
		keys[i] = src.keys[p]
		vals[i] = src.values[p]
		indices[i] = i
	}
	return keys, vals, indices
}

func (t *PersistentBPTree) splitLeafLocked(leaf *pnode, chain *lockChain) error {
	mid := leaf.numKeys() / 2

	lKeys, lVals, lIdx := compactPNode(leaf, 0, mid)
	rKeys, rVals, rIdx := compactPNode(leaf, mid, leaf.numKeys())

	right, err := t.allocNode(true)
	if err != nil {
		leaf.mu.Unlock()
		t.unpin(leaf.pageID)
		chain.releaseAll(t)
		return err
	}
	right.mu.Lock()
	right.keys = rKeys
	right.values = rVals
	right.indices = rIdx
	right.next = leaf.next
	right.parent = leaf.parent

	leaf.keys = lKeys
	leaf.values = lVals
	leaf.indices = lIdx
	leaf.next = right.pageID
	leaf.dirty = true

	// Rebuild bloom filters for both leaves after split.
	if t.config.BloomEnabled {
		lBK := make([][]byte, len(lIdx))
		for i, idx := range lIdx {
			lBK[i] = lKeys[idx]
		}
		leaf.bloom = NewBloomFilter(t.maxKeys(), t.config.BloomBitsPerKey)
		leaf.bloom.Rebuild(lBK)

		rBK := make([][]byte, len(rIdx))
		for i, idx := range rIdx {
			rBK[i] = rKeys[idx]
		}
		right.bloom = NewBloomFilter(t.maxKeys(), t.config.BloomBitsPerKey)
		right.bloom.Rebuild(rBK)
	}

	upKey := right.keyAt(0)
	if err := t.insertIntoParentLocked(leaf, upKey, right, chain); err != nil {
		right.dirty = false
		return err
	}
	return nil
}

func (t *PersistentBPTree) insertIntoParentLocked(left *pnode, key []byte, right *pnode, chain *lockChain) error {
	if left.parent == nilPage {
		// Create new root.
		root, err := t.allocNode(false)
		if err != nil {
			left.mu.Unlock()
			t.unpin(left.pageID)
			right.dirty = false
			right.mu.Unlock()
			t.unpin(right.pageID)
			chain.releaseAll(t)
			return err
		}
		root.mu.Lock()
		root.keys = [][]byte{copyBytes(key)}
		root.indices = []int{0}
		root.children = []int64{left.pageID, right.pageID}
		left.parent = root.pageID
		left.dirty = true
		right.parent = root.pageID
		right.dirty = true
		atomic.StoreInt64(&t.rootID, root.pageID)

		left.mu.Unlock()
		t.unpin(left.pageID)
		right.mu.Unlock()
		t.unpin(right.pageID)
		root.mu.Unlock()
		t.unpin(root.pageID)
		chain.releaseAll(t)
		return nil
	}

	// Get parent: from chain or re-lock.
	parentID := left.parent
	parent := chain.findAndPop(parentID)
	if parent == nil {
		// Re-lock: the chain is empty, so write-lock parent from scratch.
		var err error
		parent, err = t.loadAndPin(parentID)
		if err != nil {
			left.mu.Unlock()
			t.unpin(left.pageID)
			right.dirty = false
			right.mu.Unlock()
			t.unpin(right.pageID)
			chain.releaseAll(t)
			return err
		}
		parent.mu.Lock()
	}

	ci := t.childIndexOf(parent, left.pageID)
	if ci < 0 {
		// Parent was concurrently split and no longer contains left.childID.
		// Release everything and signal retry from the root.
		parent.mu.Unlock()
		t.unpin(parent.pageID)
		left.mu.Unlock()
		t.unpin(left.pageID)
		right.dirty = false
		right.mu.Unlock()
		t.unpin(right.pageID)
		chain.releaseAll(t)
		return errRetry
	}

	pos := len(parent.keys)
	parent.keys = append(parent.keys, copyBytes(key))
	parent.indices = append(parent.indices, 0)
	copy(parent.indices[ci+1:], parent.indices[ci:])
	parent.indices[ci] = pos

	parent.children = append(parent.children, nilPage)
	copy(parent.children[ci+2:], parent.children[ci+1:])
	parent.children[ci+1] = right.pageID
	right.parent = parent.pageID
	right.dirty = true
	parent.dirty = true

	left.mu.Unlock()
	t.unpin(left.pageID)
	right.mu.Unlock()
	t.unpin(right.pageID)

	if parent.numKeys() > t.maxKeys() {
		if err := t.splitInternalLocked(parent, chain); err != nil {
			right.dirty = false
			return err
		}
		return nil
	}

	parent.mu.Unlock()
	t.unpin(parent.pageID)
	chain.releaseAll(t)
	return nil
}

func (t *PersistentBPTree) splitInternalLocked(n *pnode, chain *lockChain) error {
	mid := n.numKeys() / 2
	upKey := n.keyAt(mid)

	lCount := mid
	rCount := n.numKeys() - mid - 1

	lKeys := make([][]byte, lCount)
	lIdx := make([]int, lCount)
	for i := 0; i < lCount; i++ {
		p := n.indices[i]
		lKeys[i] = n.keys[p]
		lIdx[i] = i
	}

	rKeys := make([][]byte, rCount)
	rIdx := make([]int, rCount)
	for i := 0; i < rCount; i++ {
		p := n.indices[mid+1+i]
		rKeys[i] = n.keys[p]
		rIdx[i] = i
	}

	lChildren := make([]int64, lCount+1)
	copy(lChildren, n.children[:lCount+1])

	rChildren := make([]int64, rCount+1)
	copy(rChildren, n.children[mid+1:])

	right, err := t.allocNode(false)
	if err != nil {
		n.mu.Unlock()
		t.unpin(n.pageID)
		chain.releaseAll(t)
		return err
	}
	right.mu.Lock()
	right.keys = rKeys
	right.indices = rIdx
	right.children = rChildren
	right.parent = n.parent

	for _, cid := range rChildren {
		// Check if this child is already locked in the chain
		childNode := chain.find(cid)
		if childNode != nil {
			// Already locked, just update parent
			childNode.parent = right.pageID
			childNode.dirty = true
		} else {
			child, err := t.loadAndPin(cid)
			if err != nil {
				right.dirty = false
				right.mu.Unlock()
				t.unpin(right.pageID)
				n.mu.Unlock()
				t.unpin(n.pageID)
				chain.releaseAll(t)
				return err
			}
			child.mu.Lock()
			child.parent = right.pageID
			child.dirty = true
			child.mu.Unlock()
			t.unpin(cid)
		}
	}

	n.keys = lKeys
	n.indices = lIdx
	n.children = lChildren
	n.dirty = true

	return t.insertIntoParentLocked(n, upKey, right, chain)
}

// ---------- internal: delete ----------

func (t *PersistentBPTree) handleUnderflowLocked(n *pnode, chain *lockChain) {
	if n.parent == nilPage {
		n.mu.Unlock()
		t.unpin(n.pageID)
		return
	}

	parentID := n.parent
	parent := chain.findAndPop(parentID)
	if parent == nil {
		p, err := t.loadAndPin(parentID)
		if err != nil {
			n.mu.Unlock()
			t.unpin(n.pageID)
			chain.releaseAll(t)
			return
		}
		p.mu.Lock()
		parent = p
	}

	ci := t.childIndexOf(parent, n.pageID)

	var leftSib, rightSib *pnode
	if ci > 0 {
		ls, err := t.loadAndPin(parent.children[ci-1])
		if err == nil {
			ls.mu.Lock()
			leftSib = ls
		}
	}
	if ci < len(parent.children)-1 {
		rs, err := t.loadAndPin(parent.children[ci+1])
		if err == nil {
			rs.mu.Lock()
			rightSib = rs
		}
	}

	if n.isLeaf {
		t.handleLeafUnderflowLocked(n, leftSib, rightSib, parent, ci, chain)
	} else {
		t.handleInternalUnderflowLocked(n, leftSib, rightSib, parent, ci, chain)
	}
	// Note: borrow/merge functions unlock all nodes they receive.
	// The unused sibling (if any) is unlocked inside handleLeaf/InternalUnderflowLocked.
}

func (t *PersistentBPTree) handleLeafUnderflowLocked(n, leftSib, rightSib, parent *pnode, ci int, chain *lockChain) {
	if leftSib != nil && leftSib.numKeys() > t.minKeys() {
		t.unlockSib(rightSib)
		t.borrowFromLeftLeafLocked(n, leftSib, parent, ci)
		return
	}
	if rightSib != nil && rightSib.numKeys() > t.minKeys() {
		t.unlockSib(leftSib)
		t.borrowFromRightLeafLocked(n, rightSib, parent, ci)
		return
	}
	if leftSib != nil {
		t.unlockSib(rightSib)
		t.mergeLeavesLocked(leftSib, n, parent, ci, chain)
	} else {
		t.unlockSib(leftSib)
		t.mergeLeavesLocked(n, rightSib, parent, ci+1, chain)
	}
}

func (t *PersistentBPTree) handleInternalUnderflowLocked(n, leftSib, rightSib, parent *pnode, ci int, chain *lockChain) {
	if leftSib != nil && leftSib.numKeys() > t.minKeys() {
		t.unlockSib(rightSib)
		t.borrowFromLeftInternalLocked(n, leftSib, parent, ci)
		return
	}
	if rightSib != nil && rightSib.numKeys() > t.minKeys() {
		t.unlockSib(leftSib)
		t.borrowFromRightInternalLocked(n, rightSib, parent, ci)
		return
	}
	if leftSib != nil {
		t.unlockSib(rightSib)
		t.mergeInternalLocked(leftSib, n, parent, ci, chain)
	} else {
		t.unlockSib(leftSib)
		t.mergeInternalLocked(n, rightSib, parent, ci+1, chain)
	}
}

func (t *PersistentBPTree) unlockSib(sib *pnode) {
	if sib != nil {
		sib.mu.Unlock()
		t.unpin(sib.pageID)
	}
}

// borrowFromLeftLeafLocked: all three nodes are write-locked on entry.
func (t *PersistentBPTree) borrowFromLeftLeafLocked(n, left, p *pnode, ci int) {
	last := left.numKeys() - 1
	physIdx := left.indices[last]

	pos := len(n.keys)
	n.keys = append(n.keys, left.keys[physIdx])
	n.values = append(n.values, left.values[physIdx])
	n.indices = append([]int{pos}, n.indices...)

	left.indices = left.indices[:last]

	p.keys[p.indices[ci-1]] = n.keyAt(0)
	n.dirty = true
	left.dirty = true
	p.dirty = true

	t.rebuildLeafBloom(n)
	t.rebuildLeafBloom(left)

	n.mu.Unlock()
	t.unpin(n.pageID)
	left.mu.Unlock()
	t.unpin(left.pageID)
	p.mu.Unlock()
	t.unpin(p.pageID)
}

func (t *PersistentBPTree) borrowFromRightLeafLocked(n, right, p *pnode, ci int) {
	physIdx := right.indices[0]

	pos := len(n.keys)
	n.keys = append(n.keys, right.keys[physIdx])
	n.values = append(n.values, right.values[physIdx])
	n.indices = append(n.indices, pos)

	right.indices = right.indices[1:]

	p.keys[p.indices[ci]] = right.keyAt(0)
	n.dirty = true
	right.dirty = true
	p.dirty = true

	t.rebuildLeafBloom(n)
	t.rebuildLeafBloom(right)

	n.mu.Unlock()
	t.unpin(n.pageID)
	right.mu.Unlock()
	t.unpin(right.pageID)
	p.mu.Unlock()
	t.unpin(p.pageID)
}

func (t *PersistentBPTree) mergeLeavesLocked(left, right, p *pnode, ci int, chain *lockChain) {
	for i := 0; i < right.numKeys(); i++ {
		physIdx := right.indices[i]
		pos := len(left.keys)
		left.keys = append(left.keys, right.keys[physIdx])
		left.values = append(left.values, right.values[physIdx])
		left.indices = append(left.indices, pos)
	}
	left.next = right.next

	p.indices = append(p.indices[:ci-1], p.indices[ci:]...)
	p.children = append(p.children[:ci], p.children[ci+1:]...)

	left.dirty = true
	p.dirty = true

	t.rebuildLeafBloom(left)

	right.mu.Unlock()
	t.unpin(right.pageID)
	// Unlock left but keep it pinned to prevent eviction. If left becomes
	// the new root (p has 0 keys), we need to re-lock it safely.
	left.mu.Unlock()

	if p.parent != nilPage && p.numKeys() < t.minKeys() {
		t.unpin(left.pageID)
		t.handleUnderflowLocked(p, chain)
		return
	}
	if p.parent == nilPage && p.numKeys() == 0 {
		left.mu.Lock()
		atomic.StoreInt64(&t.rootID, left.pageID)
		left.parent = nilPage
		left.dirty = true
		left.mu.Unlock()
	}
	t.unpin(left.pageID)
	p.mu.Unlock()
	t.unpin(p.pageID)
}

func (t *PersistentBPTree) borrowFromLeftInternalLocked(n, left, p *pnode, ci int) {
	sepKey := p.keyAt(ci - 1)
	last := left.numKeys() - 1

	pos := len(n.keys)
	n.keys = append(n.keys, sepKey)
	n.indices = append([]int{pos}, n.indices...)

	child := left.children[last+1]
	n.children = append([]int64{child}, n.children...)
	childNode, _ := t.loadAndPin(child)
	childNode.mu.Lock()
	childNode.parent = n.pageID
	childNode.dirty = true
	childNode.mu.Unlock()
	t.unpin(child)

	p.keys[p.indices[ci-1]] = left.keyAt(last)

	left.indices = left.indices[:last]
	left.children = left.children[:last+1]

	n.dirty = true
	left.dirty = true
	p.dirty = true

	n.mu.Unlock()
	t.unpin(n.pageID)
	left.mu.Unlock()
	t.unpin(left.pageID)
	p.mu.Unlock()
	t.unpin(p.pageID)
}

func (t *PersistentBPTree) borrowFromRightInternalLocked(n, right, p *pnode, ci int) {
	sepKey := p.keyAt(ci)

	pos := len(n.keys)
	n.keys = append(n.keys, sepKey)
	n.indices = append(n.indices, pos)

	child := right.children[0]
	n.children = append(n.children, child)
	childNode, _ := t.loadAndPin(child)
	childNode.mu.Lock()
	childNode.parent = n.pageID
	childNode.dirty = true
	childNode.mu.Unlock()
	t.unpin(child)

	p.keys[p.indices[ci]] = right.keyAt(0)

	right.indices = right.indices[1:]
	right.children = right.children[1:]

	n.dirty = true
	right.dirty = true
	p.dirty = true

	n.mu.Unlock()
	t.unpin(n.pageID)
	right.mu.Unlock()
	t.unpin(right.pageID)
	p.mu.Unlock()
	t.unpin(p.pageID)
}

func (t *PersistentBPTree) mergeInternalLocked(left, right, p *pnode, ci int, chain *lockChain) {
	sepKey := p.keyAt(ci - 1)

	pos := len(left.keys)
	left.keys = append(left.keys, sepKey)
	left.indices = append(left.indices, pos)

	for i := 0; i < right.numKeys(); i++ {
		physIdx := right.indices[i]
		pos = len(left.keys)
		left.keys = append(left.keys, right.keys[physIdx])
		left.indices = append(left.indices, pos)
	}

	for _, cid := range right.children {
		left.children = append(left.children, cid)
		child, _ := t.loadAndPin(cid)
		child.mu.Lock()
		child.parent = left.pageID
		child.dirty = true
		child.mu.Unlock()
		t.unpin(cid)
	}

	p.indices = append(p.indices[:ci-1], p.indices[ci:]...)
	p.children = append(p.children[:ci], p.children[ci+1:]...)

	left.dirty = true
	p.dirty = true

	right.mu.Unlock()
	t.unpin(right.pageID)
	// Unlock left but keep it pinned to prevent eviction. If left becomes
	// the new root (p has 0 keys), we need to re-lock it safely.
	left.mu.Unlock()

	if p.parent != nilPage && p.numKeys() < t.minKeys() {
		t.unpin(left.pageID)
		t.handleUnderflowLocked(p, chain)
		return
	}
	if p.parent == nilPage && p.numKeys() == 0 {
		left.mu.Lock()
		atomic.StoreInt64(&t.rootID, left.pageID)
		left.parent = nilPage
		left.dirty = true
		left.mu.Unlock()
	}
	t.unpin(left.pageID)
	p.mu.Unlock()
	t.unpin(p.pageID)
}

func (t *PersistentBPTree) maybeCollapseRoot() {
	rootID := atomic.LoadInt64(&t.rootID)
	if rootID == nilPage {
		return
	}
	root, err := t.loadAndPin(rootID)
	if err != nil {
		return
	}
	root.mu.Lock()
	if !root.isLeaf && root.numKeys() == 0 {
		newRootID := root.children[0]
		root.mu.Unlock()
		t.unpin(rootID)

		atomic.StoreInt64(&t.rootID, newRootID)
		newRoot, err := t.loadAndPin(newRootID)
		if err != nil {
			return
		}
		newRoot.mu.Lock()
		newRoot.parent = nilPage
		newRoot.dirty = true
		newRoot.mu.Unlock()
		t.unpin(newRootID)
		return
	}
	if root.isLeaf && root.numKeys() == 0 {
		root.mu.Unlock()
		t.unpin(rootID)
		atomic.StoreInt64(&t.rootID, nilPage)
		return
	}
	root.mu.Unlock()
	t.unpin(rootID)
}

// ---------- validation ----------

func (t *PersistentBPTree) validateLocked(n *pnode, isRoot bool) bool {
	if n.isLeaf {
		for i := 1; i < n.numKeys(); i++ {
			if bytes.Compare(n.keyAt(i-1), n.keyAt(i)) >= 0 {
				return false
			}
		}
		if !isRoot && n.numKeys() < t.minKeys() {
			return false
		}
		if n.numKeys() > t.maxKeys() {
			return false
		}
		return true
	}

	if !isRoot && n.numKeys() < t.minKeys() {
		return false
	}
	if n.numKeys() > t.maxKeys() {
		return false
	}
	if len(n.children) != n.numKeys()+1 {
		return false
	}
	for i := 1; i < n.numKeys(); i++ {
		if bytes.Compare(n.keyAt(i-1), n.keyAt(i)) >= 0 {
			return false
		}
	}
	for _, cid := range n.children {
		child, err := t.loadAndPin(cid)
		if err != nil {
			return false
		}
		child.mu.RLock()
		if child.parent != n.pageID {
			child.mu.RUnlock()
			t.unpin(cid)
			return false
		}
		ok := t.validateLocked(child, false)
		child.mu.RUnlock()
		t.unpin(cid)
		if !ok {
			return false
		}
	}
	return t.checkDepthLocked(n)
}

func (t *PersistentBPTree) checkDepthLocked(n *pnode) bool {
	d := 0
	c := n
	for !c.isLeaf {
		nextID := c.children[0]
		child, err := t.loadAndPin(nextID)
		if err != nil {
			return false
		}
		if c != n {
			c.mu.RUnlock()
			t.unpin(c.pageID)
		}
		child.mu.RLock()
		c = child
		d++
	}
	if c != n {
		c.mu.RUnlock()
		t.unpin(c.pageID)
	}

	var walk func(pageID int64, depth int) bool
	walk = func(pageID int64, depth int) bool {
		nd, err := t.loadAndPin(pageID)
		if err != nil {
			return false
		}
		nd.mu.RLock()
		defer func() {
			nd.mu.RUnlock()
			t.unpin(pageID)
		}()
		if nd.isLeaf {
			return depth == d
		}
		for _, cid := range nd.children {
			if !walk(cid, depth+1) {
				return false
			}
		}
		return true
	}
	return walk(n.pageID, 0)
}

// ---------- serialization ----------

func serializeNode(n *pnode, formatVersion uint32) []byte {
	numKeys := n.numKeys()

	size := 1 + 2 + 8 + 8
	if formatVersion >= 2 {
		size += 2 // bloomLen
		if n.isLeaf && n.bloom != nil {
			size += len(n.bloom.Serialize())
		}
	}
	for i := 0; i < numKeys; i++ {
		size += 2 + len(n.keyAt(i))
	}
	if n.isLeaf {
		for i := 0; i < numKeys; i++ {
			size += 2 + len(n.valAt(i))
		}
	} else {
		size += 8 * (numKeys + 1)
	}

	buf := make([]byte, size)
	off := 0

	if n.isLeaf {
		buf[off] = 1
	}
	off++

	binary.LittleEndian.PutUint16(buf[off:], uint16(numKeys))
	off += 2

	binary.LittleEndian.PutUint64(buf[off:], uint64(n.parent))
	off += 8

	binary.LittleEndian.PutUint64(buf[off:], uint64(n.next))
	off += 8

	// v2: bloom filter data.
	if formatVersion >= 2 {
		if n.isLeaf && n.bloom != nil {
			bd := n.bloom.Serialize()
			binary.LittleEndian.PutUint16(buf[off:], uint16(len(bd)))
			off += 2
			copy(buf[off:], bd)
			off += len(bd)
		} else {
			binary.LittleEndian.PutUint16(buf[off:], 0)
			off += 2
		}
	}

	for i := 0; i < numKeys; i++ {
		k := n.keyAt(i)
		binary.LittleEndian.PutUint16(buf[off:], uint16(len(k)))
		off += 2
		copy(buf[off:], k)
		off += len(k)
	}

	if n.isLeaf {
		for i := 0; i < numKeys; i++ {
			v := n.valAt(i)
			binary.LittleEndian.PutUint16(buf[off:], uint16(len(v)))
			off += 2
			copy(buf[off:], v)
			off += len(v)
		}
	} else {
		for i := 0; i < numKeys+1; i++ {
			binary.LittleEndian.PutUint64(buf[off:], uint64(n.children[i]))
			off += 8
		}
	}

	return buf
}

func deserializeNode(pageID int64, data []byte, formatVersion uint32) *pnode {
	off := 0

	isLeaf := data[off] == 1
	off++

	numKeys := int(binary.LittleEndian.Uint16(data[off:]))
	off += 2

	parent := int64(binary.LittleEndian.Uint64(data[off:]))
	off += 8

	next := int64(binary.LittleEndian.Uint64(data[off:]))
	off += 8

	// v2: parse bloom filter.
	var bloom *BloomFilter
	if formatVersion >= 2 {
		bloomLen := int(binary.LittleEndian.Uint16(data[off:]))
		off += 2
		if isLeaf && bloomLen > 0 {
			bloom = DeserializeBloomFilter(data[off : off+bloomLen])
			off += bloomLen
		}
	}

	keys := make([][]byte, numKeys)
	for i := 0; i < numKeys; i++ {
		klen := int(binary.LittleEndian.Uint16(data[off:]))
		off += 2
		keys[i] = make([]byte, klen)
		copy(keys[i], data[off:])
		off += klen
	}

	var values [][]byte
	var children []int64

	if isLeaf {
		values = make([][]byte, numKeys)
		for i := 0; i < numKeys; i++ {
			vlen := int(binary.LittleEndian.Uint16(data[off:]))
			off += 2
			values[i] = make([]byte, vlen)
			copy(values[i], data[off:])
			off += vlen
		}
	} else {
		children = make([]int64, numKeys+1)
		for i := 0; i < numKeys+1; i++ {
			children[i] = int64(binary.LittleEndian.Uint64(data[off:]))
			off += 8
		}
	}

	indices := make([]int, numKeys)
	for i := range indices {
		indices[i] = i
	}

	return &pnode{
		pageID:   pageID,
		isLeaf:   isLeaf,
		keys:     keys,
		values:   values,
		indices:  indices,
		children: children,
		next:     next,
		parent:   parent,
		bloom:    bloom,
	}
}
