// Package storage 提供存储引擎功能
// 包括：MVCC多版本并发控制、B+树管理、垃圾回收
package storage

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"lns.com/minidb/bptree"
	"lns.com/minidb/metrics"
	"lns.com/minidb/wal"
)

// versionCacheEntry 存储主键的最新提交版本信息
// 用于MVCC读取加速
type versionCacheEntry struct {
	commitTS uint64 // 提交时间戳
	rowData  []byte // 行数据，nil表示已删除
}

// StorageEngine 存储引擎
// 管理多个B+树实例，每个表和索引都有自己的B+树文件
// 实现MVCC并发控制
type StorageEngine struct {
	mu        sync.RWMutex
	dataDir   string                              // 数据目录
	order     int                                 // B+树阶
	cacheSize int                                 // 缓存大小
	trees     map[string]*bptree.PersistentBPTree // B+树映射

	// dirtyPKs 跟踪有旧版本可回收的主键
	dirtyPKs map[string]map[string]struct{}
	dirtyMu  sync.Mutex

	// verCache 版本缓存："treeKey\x00pk" -> *versionCacheEntry
	// 用于避免GetRow和OCC验证时的B+树范围扫描
	verCache      sync.Map
	verCacheStats struct {
		hits   atomic.Int64 // 缓存命中数
		misses atomic.Int64 // 缓存未命中数
	}
}

// OpenEngine creates or opens a StorageEngine backed by files in dataDir.
func OpenEngine(dataDir string, order, cacheSize int) (*StorageEngine, error) {
	e := &StorageEngine{
		dataDir:   dataDir,
		order:     order,
		cacheSize: cacheSize,
		trees:     make(map[string]*bptree.PersistentBPTree),
		dirtyPKs:  make(map[string]map[string]struct{}),
	}

	// Scan data directory for existing .db files and open them.
	// This ensures trees are loaded even when WAL is empty (clean shutdown).
	entries, err := os.ReadDir(dataDir)
	if err != nil {
		return nil, fmt.Errorf("read data dir: %w", err)
	}
	for _, ent := range entries {
		if ent.IsDir() || !strings.HasSuffix(ent.Name(), ".db") {
			continue
		}
		// Skip catalog files — they are managed by the catalog package.
		if strings.HasPrefix(ent.Name(), "__catalog_") {
			continue
		}
		treeKey := ent.Name()
		if _, exists := e.trees[treeKey]; !exists {
			path := filepath.Join(dataDir, treeKey)
			tree, err := bptree.OpenPersistentBPTree(path, order, cacheSize)
			if err != nil {
				return nil, fmt.Errorf("open tree %s: %w", treeKey, err)
			}
			e.trees[treeKey] = tree
		}
	}

	return e, nil
}

// RecoverFromWAL replays committed transactions from the WAL.
func (e *StorageEngine) RecoverFromWAL(w *wal.WAL) error {
	records, err := w.ReadAll()
	if err != nil {
		return fmt.Errorf("read WAL: %w", err)
	}

	committed := make(map[uint64]bool)
	commitTSMap := make(map[uint64]uint64)
	for _, r := range records {
		if r.Type == wal.RecCommit {
			committed[r.TxnTS] = true
			commitTSMap[r.TxnTS] = r.CommitTS
		} else if r.Type == wal.RecAbort {
			delete(committed, r.TxnTS)
		}
	}

	for _, r := range records {
		if r.Type == wal.RecCommit || r.Type == wal.RecAbort || r.Type == wal.RecCheckpoint {
			continue
		}
		if !committed[r.TxnTS] {
			continue
		}

		if err := e.OpenTree(r.TreeKey); err != nil {
			return err
		}

		commitTS := commitTSMap[r.TxnTS]
		isIndex := strings.Contains(r.TreeKey, "__idx__")
		switch r.Type {
		case wal.RecInsert:
			if isIndex {
				if err := e.InsertRaw(r.TreeKey, r.PK, r.RowData); err != nil {
					return err
				}
			} else {
				if err := e.InsertRow(r.TreeKey, r.PK, commitTS, r.RowData); err != nil {
					return err
				}
			}
		case wal.RecUpdate:
			if isIndex {
				if err := e.InsertRaw(r.TreeKey, r.PK, r.RowData); err != nil {
					return err
				}
			} else {
				if err := e.UpdateRow(r.TreeKey, r.PK, commitTS, r.RowData); err != nil {
					return err
				}
			}
		case wal.RecDelete:
			if isIndex {
				e.DeleteRaw(r.TreeKey, r.PK)
			} else {
				if err := e.DeleteRow(r.TreeKey, r.PK, commitTS); err != nil {
					return err
				}
			}
		}
	}

	return nil
}

// Close closes all open B+ trees.
func (e *StorageEngine) Close() {
	e.mu.Lock()
	defer e.mu.Unlock()
	for _, tree := range e.trees {
		tree.Close()
	}
	e.trees = nil
}

// OpenTree opens or creates a B+ tree for the given treeKey.
// treeKey is typically "db__table.db" or "db__table__idx__name.db".
func (e *StorageEngine) OpenTree(treeKey string) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	if _, ok := e.trees[treeKey]; ok {
		return nil
	}
	path := filepath.Join(e.dataDir, treeKey)
	tree, err := bptree.OpenPersistentBPTree(path, e.order, e.cacheSize)
	if err != nil {
		return fmt.Errorf("open tree %s: %w", treeKey, err)
	}
	e.trees[treeKey] = tree
	return nil
}

func (e *StorageEngine) getTree(treeKey string) *bptree.PersistentBPTree {
	e.mu.RLock()
	defer e.mu.RUnlock()
	return e.trees[treeKey]
}

// verCacheKey builds the cache key: treeKey + \x00 + pk
func verCacheKey(treeKey string, pk []byte) string {
	b := make([]byte, len(treeKey)+1+len(pk))
	copy(b, treeKey)
	b[len(treeKey)] = 0
	copy(b[len(treeKey)+1:], pk)
	return string(b)
}

// markDirty records that a PK in the given tree has produced a recyclable old version.
func (e *StorageEngine) markDirty(treeKey string, pk string) {
	e.dirtyMu.Lock()
	if e.dirtyPKs[treeKey] == nil {
		e.dirtyPKs[treeKey] = make(map[string]struct{})
	}
	e.dirtyPKs[treeKey][pk] = struct{}{}
	e.dirtyMu.Unlock()
}

// readdDirty puts a PK back into the dirty set for the next GC pass.
func (e *StorageEngine) readdDirty(treeKey string, pk string) {
	e.dirtyMu.Lock()
	if e.dirtyPKs[treeKey] == nil {
		e.dirtyPKs[treeKey] = make(map[string]struct{})
	}
	e.dirtyPKs[treeKey][pk] = struct{}{}
	e.dirtyMu.Unlock()
}

// --- MVCC row operations ---

// InsertRow inserts a new row version at the given commit timestamp.
func (e *StorageEngine) InsertRow(treeKey string, pk []byte, commitTS uint64, rowData []byte) error {
	tree := e.getTree(treeKey)
	if tree == nil {
		return fmt.Errorf("tree %q not open", treeKey)
	}
	vkey := VersionKey(pk, commitTS)
	mvccVal := EncodeMVCCValue(commitTS, 0, 0, rowData)
	if err := tree.Insert(vkey, mvccVal); err != nil {
		return err
	}
	// Update version cache.
	e.verCache.Store(verCacheKey(treeKey, pk), &versionCacheEntry{commitTS: commitTS, rowData: rowData})
	return nil
}

// GetRow retrieves the visible version of a row at the given read timestamp.
// Returns the row data, the commit timestamp of that version, or nil if not visible.
func (e *StorageEngine) GetRow(treeKey string, pk []byte, readTS uint64) ([]byte, uint64, error) {
	start := time.Now()

	// Fast path: check version cache for the latest committed version.
	ck := verCacheKey(treeKey, pk)
	if v, ok := e.verCache.Load(ck); ok {
		ent := v.(*versionCacheEntry)
		e.verCacheStats.hits.Add(1)
		// The cached entry is the latest version (highest commitTS).
		// If its commitTS is visible at readTS, return it.
		if ent.commitTS <= readTS {
			if ent.rowData == nil {
				// Deleted or tombstone
				metrics.MVCCGetDuration.Observe(time.Since(start).Seconds())
				return nil, 0, nil
			}
			metrics.MVCCGetDuration.Observe(time.Since(start).Seconds())
			metrics.RowsReadTotal.Inc()
			metrics.TableRowsRead.WithLabelValues(treeKey).Inc()
			return ent.rowData, ent.commitTS, nil
		}
		// Cached version is too new for this readTS; fall through to B+ tree scan.
		// This is rare — only happens for old snapshots reading concurrently with new commits.
	} else {
		e.verCacheStats.misses.Add(1)
	}

	tree := e.getTree(treeKey)
	if tree == nil {
		return nil, 0, fmt.Errorf("tree %q not open", treeKey)
	}
	metrics.TableScansTotal.WithLabelValues(treeKey, "get").Inc()
	scanStart, scanEnd := ScanRangeForPK(pk)
	var result []byte
	var commitTS uint64
	var found bool
	tree.RangeScanFn(scanStart, scanEnd, func(key, value []byte) bool {
		xmin, _, flags, rowData, err := DecodeMVCCValue(value)
		if err != nil {
			return true
		}
		if xmin > readTS {
			return true // version too new, skip
		}
		if flags&FlagDeleted != 0 {
			return false // tombstone — row is deleted, stop
		}
		found = true
		result = rowData
		commitTS = xmin
		return false
	})
	if found {
		metrics.MVCCGetDuration.Observe(time.Since(start).Seconds())
		metrics.RowsReadTotal.Inc()
		metrics.TableRowsRead.WithLabelValues(treeKey).Inc()
		return result, commitTS, nil
	}
	metrics.MVCCGetDuration.Observe(time.Since(start).Seconds())
	return nil, 0, nil
}

// GetRowLatest returns the latest committed version regardless of read timestamp.
// Used by OCC validation — reads the latest version from cache or B+ tree.
func (e *StorageEngine) GetRowLatest(treeKey string, pk []byte) ([]byte, uint64, error) {
	// Check version cache first.
	ck := verCacheKey(treeKey, pk)
	if v, ok := e.verCache.Load(ck); ok {
		ent := v.(*versionCacheEntry)
		return ent.rowData, ent.commitTS, nil
	}

	// Fall back to B+ tree scan with max readTS.
	return e.GetRow(treeKey, pk, ^uint64(0))
}

// UpdateRow inserts a new version of the row.
// No xmax update on the old version — just insert the new version.
func (e *StorageEngine) UpdateRow(treeKey string, pk []byte, commitTS uint64, oldRowData []byte) error {
	tree := e.getTree(treeKey)
	if tree == nil {
		return fmt.Errorf("tree %q not open", treeKey)
	}

	// Insert new version only.
	vkey := VersionKey(pk, commitTS)
	mvccVal := EncodeMVCCValue(commitTS, 0, 0, oldRowData)
	if err := tree.Insert(vkey, mvccVal); err != nil {
		return err
	}
	e.markDirty(treeKey, string(pk))
	// Update version cache.
	ck := verCacheKey(treeKey, pk)
	e.verCache.Store(ck, &versionCacheEntry{commitTS: commitTS, rowData: oldRowData})
	return nil
}

// DeleteRow marks a row as deleted by inserting a tombstone.
// No xmax update on the old version — just insert the tombstone.
func (e *StorageEngine) DeleteRow(treeKey string, pk []byte, commitTS uint64) error {
	tree := e.getTree(treeKey)
	if tree == nil {
		return fmt.Errorf("tree %q not open", treeKey)
	}

	// Insert tombstone only.
	vkey := VersionKey(pk, commitTS)
	mvccVal := EncodeMVCCValue(commitTS, 0, FlagDeleted, nil)
	if err := tree.Insert(vkey, mvccVal); err != nil {
		return err
	}
	e.markDirty(treeKey, string(pk))
	// Update version cache: deleted row has nil rowData.
	ck := verCacheKey(treeKey, pk)
	e.verCache.Store(ck, &versionCacheEntry{commitTS: commitTS, rowData: nil})
	return nil
}

// ScanRange iterates over rows in [start, end) key range visible at readTS.
// The callback receives the primary key and row data for each visible row.
// Uses RangeScanFn for true early termination when fn returns false.
func (e *StorageEngine) ScanRange(treeKey string, start, end []byte, readTS uint64, fn func(pk, row []byte) bool) {
	scanStart := time.Now()
	tree := e.getTree(treeKey)
	if tree == nil {
		metrics.MVCCScanDuration.Observe(time.Since(scanStart).Seconds())
		return
	}
	metrics.TableScansTotal.WithLabelValues(treeKey, "scan").Inc()
	verScanStart := make([]byte, len(start)+8)
	copy(verScanStart, start)
	verScanEnd := make([]byte, len(end)+8)
	copy(verScanEnd, end)
	for i := len(end); i < len(verScanEnd); i++ {
		verScanEnd[i] = 0xFF
	}

	var prevPK []byte
	tree.RangeScanFn(verScanStart, verScanEnd, func(key, value []byte) bool {
		pk := KeyPrefix(key)
		if prevPK != nil && bytes.Equal(pk, prevPK) {
			return true // same PK, skip older versions
		}
		xmin, _, flags, rowData, err := DecodeMVCCValue(value)
		if err != nil {
			return true
		}
		if xmin > readTS {
			return true // version created after our snapshot, skip
		}
		prevPK = append(prevPK[:0], pk...)
		if flags&FlagDeleted != 0 {
			return true // tombstone — row is deleted, skip older versions too
		}
		metrics.RowsReadTotal.Inc()
		metrics.TableRowsRead.WithLabelValues(treeKey).Inc()
		pkCopy := make([]byte, len(pk))
		copy(pkCopy, pk)
		return fn(pkCopy, rowData)
	})
	metrics.MVCCScanDuration.Observe(time.Since(scanStart).Seconds())
}

// --- Raw operations (for secondary indexes) ---

// InsertRaw inserts a raw key-value pair without MVCC encoding.
func (e *StorageEngine) InsertRaw(treeKey string, key, value []byte) error {
	tree := e.getTree(treeKey)
	if tree == nil {
		return fmt.Errorf("tree %q not open", treeKey)
	}
	return tree.Insert(key, value)
}

// --- Batch commit operations ---

// TreeWriteBatch holds all prepared B+ tree writes for a single tree.
type TreeWriteBatch struct {
	TreeKey      string
	InsertPairs  [][2][]byte // B+ tree key-value pairs for BatchInsert
	DeleteKeys   [][]byte    // keys for BatchDelete (index trees only)
	CacheUpdates []cacheUpdate
	DirtyPKs     []string
	IsIndex      bool
}

type cacheUpdate struct {
	key string
	ent *versionCacheEntry
}

// PrepareInsertRow builds the B+ tree pairs for a new row insert.
func (e *StorageEngine) PrepareInsertRow(treeKey string, pk []byte, commitTS uint64, rowData []byte) (*TreeWriteBatch, error) {
	vkey := VersionKey(pk, commitTS)
	mvccVal := EncodeMVCCValue(commitTS, 0, 0, rowData)
	return &TreeWriteBatch{
		TreeKey:      treeKey,
		InsertPairs:  [][2][]byte{{vkey, mvccVal}},
		CacheUpdates: []cacheUpdate{{verCacheKey(treeKey, pk), &versionCacheEntry{commitTS: commitTS, rowData: rowData}}},
	}, nil
}

// PrepareUpdateRow builds the B+ tree pairs for a row update.
// No xmax update on the old version — just insert the new version.
func (e *StorageEngine) PrepareUpdateRow(treeKey string, pk []byte, commitTS uint64, newRowData []byte) (*TreeWriteBatch, error) {
	ck := verCacheKey(treeKey, pk)

	// Insert new version only.
	vkey := VersionKey(pk, commitTS)
	mvccVal := EncodeMVCCValue(commitTS, 0, 0, newRowData)

	return &TreeWriteBatch{
		TreeKey:      treeKey,
		InsertPairs:  [][2][]byte{{vkey, mvccVal}},
		CacheUpdates: []cacheUpdate{{ck, &versionCacheEntry{commitTS: commitTS, rowData: newRowData}}},
		DirtyPKs:     []string{string(pk)},
	}, nil
}

// PrepareDeleteRow builds the B+ tree pairs for a row delete (tombstone).
// No xmax update on the old version — just insert the tombstone.
func (e *StorageEngine) PrepareDeleteRow(treeKey string, pk []byte, commitTS uint64) (*TreeWriteBatch, error) {
	ck := verCacheKey(treeKey, pk)

	// Insert tombstone only.
	vkey := VersionKey(pk, commitTS)
	mvccVal := EncodeMVCCValue(commitTS, 0, FlagDeleted, nil)

	return &TreeWriteBatch{
		TreeKey:      treeKey,
		InsertPairs:  [][2][]byte{{vkey, mvccVal}},
		CacheUpdates: []cacheUpdate{{ck, &versionCacheEntry{commitTS: commitTS, rowData: nil}}},
		DirtyPKs:     []string{string(pk)},
	}, nil
}

// MergeBatch adds the contents of another batch into this one (same tree).
func (b *TreeWriteBatch) MergeBatch(other *TreeWriteBatch) {
	b.InsertPairs = append(b.InsertPairs, other.InsertPairs...)
	b.DeleteKeys = append(b.DeleteKeys, other.DeleteKeys...)
	b.CacheUpdates = append(b.CacheUpdates, other.CacheUpdates...)
	b.DirtyPKs = append(b.DirtyPKs, other.DirtyPKs...)
}

// ApplyBatch applies all prepared writes for a single tree.
// It acquires the tree's writeMu once and performs all inserts/deletes.
func (e *StorageEngine) ApplyBatch(batch *TreeWriteBatch) error {
	tree := e.getTree(batch.TreeKey)
	if tree == nil {
		return fmt.Errorf("tree %q not open", batch.TreeKey)
	}

	if len(batch.InsertPairs) > 0 {
		if err := tree.BatchInsert(batch.InsertPairs); err != nil {
			return err
		}
	}
	if len(batch.DeleteKeys) > 0 {
		tree.BatchDelete(batch.DeleteKeys)
	}

	// Update version cache.
	for _, cu := range batch.CacheUpdates {
		e.verCache.Store(cu.key, cu.ent)
	}

	// Mark dirty PKs for GC.
	for _, pk := range batch.DirtyPKs {
		e.markDirty(batch.TreeKey, pk)
	}

	return nil
}

// ScanRaw iterates over raw key-value pairs in [start, end) range.
func (e *StorageEngine) ScanRaw(treeKey string, start, end []byte, fn func(key, value []byte) bool) {
	tree := e.getTree(treeKey)
	if tree == nil {
		return
	}
	kvs := tree.RangeScan(start, end)
	for _, kv := range kvs {
		if !fn(kv.Key, kv.Value) {
			break
		}
	}
}

// DeleteRaw deletes a raw key-value pair without MVCC encoding.
// Used for secondary index maintenance.
func (e *StorageEngine) DeleteRaw(treeKey string, key []byte) bool {
	tree := e.getTree(treeKey)
	if tree == nil {
		return false
	}
	return tree.Delete(key)
}

// ScanAll iterates over all rows in [start, end) range, returning the latest version of each row
// regardless of MVCC visibility. This is used for aggregate queries that need to count all data.
// Uses RangeScanFn for true early termination when fn returns false.
func (e *StorageEngine) ScanAll(treeKey string, start, end []byte, fn func(pk, rowData []byte) bool) {
	tree := e.getTree(treeKey)
	if tree == nil {
		return
	}
	metrics.TableScansTotal.WithLabelValues(treeKey, "scanall").Inc()

	scanStart := make([]byte, len(start)+8)
	copy(scanStart, start)
	scanEnd := make([]byte, len(end)+8)
	copy(scanEnd, end)
	for i := len(end); i < len(scanEnd); i++ {
		scanEnd[i] = 0xFF
	}

	// Use RangeScanFn for early termination — when fn returns false, the scan
	// stops immediately without loading any more leaf pages.
	var prevPK []byte
	tree.RangeScanFn(scanStart, scanEnd, func(key, value []byte) bool {
		pk := KeyPrefix(key)
		if prevPK != nil && bytes.Equal(pk, prevPK) {
			return true // same PK, skip older versions
		}
		prevPK = append(prevPK[:0], pk...)
		_, _, flags, rowData, err := DecodeMVCCValue(value)
		if err != nil {
			return true
		}
		if flags&FlagDeleted != 0 {
			return true // tombstone — row is deleted
		}
		return fn(pk, rowData)
	})
}

// CountAll counts distinct PKs in [start, end) range without decoding MVCC values.
// Uses key-only traversal with PK dedup inline — no value copy, no key materialization.
func (e *StorageEngine) CountAll(treeKey string, start, end []byte) int64 {
	tree := e.getTree(treeKey)
	if tree == nil {
		return 0
	}
	metrics.TableScansTotal.WithLabelValues(treeKey, "count").Inc()

	scanStart := make([]byte, len(start)+8)
	copy(scanStart, start)
	scanEnd := make([]byte, len(end)+8)
	copy(scanEnd, end)
	for i := len(end); i < len(scanEnd); i++ {
		scanEnd[i] = 0xFF
	}

	var count int64
	var prevPK []byte
	tree.RangeScanFn(scanStart, scanEnd, func(key, _ []byte) bool {
		pk := KeyPrefix(key)
		if prevPK != nil && bytes.Equal(pk, prevPK) {
			return true // same PK, skip
		}
		prevPK = append(prevPK[:0], pk...)
		count++
		return true
	})
	return count
}

// --- Garbage collection ---

type versionInfo struct {
	key   []byte
	xmin  uint64
	xmax  uint64
	flags byte
}

// VacuumTree removes stale MVCC versions from the specified tree using a full scan.
// Deprecated: use vacuumDirtyPKs instead for targeted GC.
func (e *StorageEngine) VacuumTree(treeKey string, safeTS uint64, limit int) (int, error) {
	tree := e.getTree(treeKey)
	if tree == nil {
		return 0, nil
	}

	// Full range scan.
	scanStart := []byte{0x00}
	scanEnd := bytes.Repeat([]byte{0xFF}, 32)
	kvs := tree.RangeScan(scanStart, scanEnd)

	// Group by PK prefix, identify GC-eligible versions.
	var toDelete [][]byte
	var currentPK string
	var versions []versionInfo

	for _, kv := range kvs {
		if len(toDelete) >= limit {
			break
		}
		xmin, xmax, flags, _, err := DecodeMVCCValue(kv.Value)
		if err != nil {
			continue
		}

		pk := string(KeyPrefix(kv.Key))
		if pk != currentPK {
			if len(versions) > 0 {
				toDelete = append(toDelete, gcEligible(versions, safeTS)...)
			}
			versions = versions[:0]
			currentPK = pk
		}
		versions = append(versions, versionInfo{key: kv.Key, xmin: xmin, xmax: xmax, flags: flags})
	}
	if len(versions) > 0 && len(toDelete) < limit {
		toDelete = append(toDelete, gcEligible(versions, safeTS)...)
	}

	removed := 0
	for _, key := range toDelete {
		if removed >= limit {
			break
		}
		tree.Delete(key)
		removed++
	}
	return removed, nil
}

// vacuumDirtyPKs performs targeted GC on only the PKs that have been marked dirty.
// It swaps out the current dirty set, processes each PK individually, and re-adds
// any remaining PKs if the limit is exhausted.
func (e *StorageEngine) vacuumDirtyPKs(safeTS uint64, limit int) int {
	e.dirtyMu.Lock()
	dirty := e.dirtyPKs
	e.dirtyPKs = make(map[string]map[string]struct{})
	e.dirtyMu.Unlock()

	removed := 0
	for treeKey, pks := range dirty {
		tree := e.getTree(treeKey)
		if tree == nil {
			continue
		}
		for pkStr := range pks {
			if removed >= limit {
				e.readdDirty(treeKey, pkStr)
				continue
			}
			pk := []byte(pkStr)
			start, end := ScanRangeForPK(pk)
			kvs := tree.RangeScan(start, end)

			var versions []versionInfo
			for _, kv := range kvs {
				xmin, xmax, flags, _, err := DecodeMVCCValue(kv.Value)
				if err != nil {
					continue
				}
				versions = append(versions, versionInfo{key: kv.Key, xmin: xmin, xmax: xmax, flags: flags})
			}

			toDelete := gcEligible(versions, safeTS)
			for _, key := range toDelete {
				if removed >= limit {
					e.readdDirty(treeKey, pkStr)
					break
				}
				tree.Delete(key)
				removed++
			}

			// If there are still multiple versions, re-add to dirty set
			// so future GC passes can clean up remaining old versions.
			if len(versions)-len(toDelete) > 1 {
				e.readdDirty(treeKey, pkStr)
			}
		}
	}
	return removed
}

// gcEligible returns the keys of versions that can be safely removed.
// versions are ordered newest-first (B+ tree ordering with ^commitTS).
// A version can be removed when a newer version is visible to ALL active
// transactions (xmin < safeTS), because no active transaction would ever
// need to see the older version.
func gcEligible(versions []versionInfo, safeTS uint64) [][]byte {
	if len(versions) <= 1 {
		return nil
	}

	// Find the first (newest) version that is universally visible.
	// All versions after it can be removed because this version
	// supersedes them for every active transaction.
	for i := 0; i < len(versions); i++ {
		if versions[i].xmin < safeTS {
			// This version is visible to ALL active transactions.
			// All older versions are superseded.
			var toDelete [][]byte
			for j := i + 1; j < len(versions); j++ {
				toDelete = append(toDelete, versions[j].key)
			}
			return toDelete
		}
	}

	return nil
}

// RunGC performs garbage collection on dirty PKs only.
func (e *StorageEngine) RunGC(safeTS uint64) {
	start := time.Now()
	removed := e.vacuumDirtyPKs(safeTS, 500)
	metrics.GCDuration.Observe(time.Since(start).Seconds())
	metrics.GCPassesTotal.Inc()
	metrics.GCVersionsRemovedTotal.Add(float64(removed))
}

// RunFullGC repeatedly runs vacuumDirtyPKs until no more versions are removed.
// Intended for use after WAL recovery to clean up all accumulated old versions.
func (e *StorageEngine) RunFullGC(safeTS uint64) {
	start := time.Now()
	totalRemoved := 0
	for {
		removed := e.vacuumDirtyPKs(safeTS, 5000)
		if removed == 0 {
			break
		}
		totalRemoved += removed
	}
	if totalRemoved > 0 {
		metrics.GCDuration.Observe(time.Since(start).Seconds())
		metrics.GCPassesTotal.Inc()
		metrics.GCVersionsRemovedTotal.Add(float64(totalRemoved))
	}
}

func (e *StorageEngine) SyncTree(treeKey string) error {
	tree := e.getTree(treeKey)
	if tree == nil {
		return fmt.Errorf("tree %q not open", treeKey)
	}
	return tree.Sync()
}

func (e *StorageEngine) SyncAll() {
	e.mu.RLock()
	trees := make([]*bptree.PersistentBPTree, 0, len(e.trees))
	for _, tree := range e.trees {
		trees = append(trees, tree)
	}
	e.mu.RUnlock()
	for _, tree := range trees {
		tree.Sync()
	}
}
