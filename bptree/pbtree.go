package bptree

import (
	"bytes"
	"encoding/binary"
	"sync"
)

const nilPage int64 = -1

// pnode is the persistent counterpart of the in-memory node.
// Pointers (children, next, parent) are replaced by page IDs.
type pnode struct {
	pageID   int64
	isLeaf   bool
	keys     [][]byte
	values   [][]byte // leaf only
	indices  []int
	children []int64  // page IDs, internal only
	next     int64    // leaf linked-list pointer
	parent   int64
	dirty    bool
}

func (n *pnode) numKeys() int       { return len(n.indices) }
func (n *pnode) keyAt(i int) []byte { return n.keys[n.indices[i]] }
func (n *pnode) valAt(i int) []byte { return n.values[n.indices[i]] }

// PersistentBPTree is a B+ tree that persists nodes to disk through an LRU
// buffer cache. The on-disk representation is managed by a Pager; hot nodes
// are kept in memory and cold nodes are evicted via LRU.
type PersistentBPTree struct {
	mu     sync.RWMutex
	pager  *Pager
	cache  *LRUCache
	rootID int64
	order  int
}

// OpenPersistentBPTree opens or creates a persistent B+ tree stored in filePath.
// order is the B+ tree order (minimum 3). cacheSize controls how many nodes
// are kept in memory before LRU eviction kicks in.
func OpenPersistentBPTree(filePath string, order, cacheSize int) (*PersistentBPTree, error) {
	if order < 3 {
		order = 3
	}
	if cacheSize < 10 {
		cacheSize = 10
	}

	pager, err := NewPager(filePath, 0)
	if err != nil {
		return nil, err
	}

	t := &PersistentBPTree{
		pager:  pager,
		cache:  NewLRUCache(cacheSize, pager),
		rootID: nilPage,
		order:  order,
	}

	// Try to load metadata from an existing file.
	h, err := pager.ReadHeader()
	if err == nil && h.PageCount > 0 {
		t.rootID = h.RootPageID
		t.order = int(h.Order)
	}

	return t, nil
}

// Close flushes all dirty nodes and the file header, then releases resources.
func (t *PersistentBPTree) Close() error {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.cache.Flush()
	t.pager.WriteHeader(t.rootID, t.order)
	return t.pager.Close()
}

func (t *PersistentBPTree) maxKeys() int { return t.order - 1 }
func (t *PersistentBPTree) minKeys() int { return (t.order - 1) / 2 }

// ---------- node helpers ----------

func (t *PersistentBPTree) loadNode(pageID int64) *pnode {
	if pageID == nilPage {
		return nil
	}
	if n := t.cache.Get(pageID); n != nil {
		return n
	}
	data, err := t.pager.Read(pageID)
	if err != nil {
		return nil
	}
	n := deserializeNode(pageID, data)
	t.cache.Put(n)
	return n
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
	t.cache.Put(n)
	return n, nil
}

// ---------- public API ----------

// Find returns the value associated with key.
func (t *PersistentBPTree) Find(key []byte) ([]byte, bool) {
	t.mu.RLock()
	defer t.mu.RUnlock()
	if t.rootID == nilPage {
		return nil, false
	}
	return t.find(t.loadNode(t.rootID), key)
}

// Insert associates key with value, overwriting any existing entry.
func (t *PersistentBPTree) Insert(key, value []byte) error {
	t.mu.Lock()
	defer t.mu.Unlock()

	if t.rootID == nilPage {
		n, err := t.allocNode(true)
		if err != nil {
			return err
		}
		n.keys = [][]byte{copyBytes(key)}
		n.values = [][]byte{copyBytes(value)}
		n.indices = []int{0}
		t.rootID = n.pageID
		return nil
	}

	leaf := t.findLeaf(key)
	t.insertIntoLeaf(leaf, key, value)
	if leaf.numKeys() > t.maxKeys() {
		return t.splitLeaf(leaf)
	}
	return nil
}

// Delete removes key from the tree.
func (t *PersistentBPTree) Delete(key []byte) bool {
	t.mu.Lock()
	defer t.mu.Unlock()

	if t.rootID == nilPage {
		return false
	}
	leaf := t.findLeaf(key)
	idx := leafSearch(leaf, key)
	if idx >= leaf.numKeys() || !bytes.Equal(leaf.keyAt(idx), key) {
		return false
	}

	leaf.indices = append(leaf.indices[:idx], leaf.indices[idx+1:]...)
	leaf.dirty = true

	if leaf.parent != nilPage && leaf.numKeys() < t.minKeys() {
		t.handleUnderflow(leaf)
	}

	root := t.loadNode(t.rootID)
	if root != nil && !root.isLeaf && root.numKeys() == 0 {
		t.rootID = root.children[0]
		nr := t.loadNode(t.rootID)
		nr.parent = nilPage
		nr.dirty = true
	}
	root = t.loadNode(t.rootID)
	if root != nil && root.isLeaf && root.numKeys() == 0 {
		t.rootID = nilPage
	}
	return true
}

// RangeScan returns all key-value pairs where start <= key <= end.
func (t *PersistentBPTree) RangeScan(start, end []byte) []KeyValuePair {
	t.mu.RLock()
	defer t.mu.RUnlock()

	var results []KeyValuePair
	if t.rootID == nilPage {
		return results
	}
	leaf := t.findLeaf(start)
	for leaf != nil {
		for i := 0; i < leaf.numKeys(); i++ {
			k := leaf.keyAt(i)
			if bytes.Compare(k, end) > 0 {
				return results
			}
			if bytes.Compare(k, start) >= 0 {
				results = append(results, KeyValuePair{Key: k, Value: leaf.valAt(i)})
			}
		}
		if leaf.next == nilPage {
			break
		}
		leaf = t.loadNode(leaf.next)
	}
	return results
}

// Count returns the total number of keys in the tree.
func (t *PersistentBPTree) Count() int {
	t.mu.RLock()
	defer t.mu.RUnlock()

	if t.rootID == nilPage {
		return 0
	}
	n := t.loadNode(t.rootID)
	for !n.isLeaf {
		n = t.loadNode(n.children[0])
	}
	count := 0
	for n != nil {
		count += n.numKeys()
		if n.next == nilPage {
			break
		}
		n = t.loadNode(n.next)
	}
	return count
}

// Validate checks tree invariants (for testing).
func (t *PersistentBPTree) Validate() bool {
	t.mu.RLock()
	defer t.mu.RUnlock()
	if t.rootID == nilPage {
		return true
	}
	return t.validate(t.loadNode(t.rootID), true)
}

// ---------- internal: search ----------

func (t *PersistentBPTree) find(n *pnode, key []byte) ([]byte, bool) {
	if n.isLeaf {
		idx := leafSearch(n, key)
		if idx < n.numKeys() && bytes.Equal(n.keyAt(idx), key) {
			return n.valAt(idx), true
		}
		return nil, false
	}
	return t.find(t.loadNode(n.children[childIndex(n, key)]), key)
}

func (t *PersistentBPTree) findLeaf(key []byte) *pnode {
	n := t.loadNode(t.rootID)
	for !n.isLeaf {
		n = t.loadNode(n.children[childIndex(n, key)])
	}
	return n
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

// ---------- internal: insert ----------

func (t *PersistentBPTree) insertIntoLeaf(leaf *pnode, key, value []byte) {
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

func (t *PersistentBPTree) splitLeaf(leaf *pnode) error {
	mid := leaf.numKeys() / 2

	lKeys, lVals, lIdx := compactPNode(leaf, 0, mid)
	rKeys, rVals, rIdx := compactPNode(leaf, mid, leaf.numKeys())

	right, err := t.allocNode(true)
	if err != nil {
		return err
	}
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

	return t.insertIntoParent(leaf, right.keyAt(0), right)
}

func (t *PersistentBPTree) insertIntoParent(left *pnode, key []byte, right *pnode) error {
	if left.parent == nilPage {
		root, err := t.allocNode(false)
		if err != nil {
			return err
		}
		root.keys = [][]byte{copyBytes(key)}
		root.indices = []int{0}
		root.children = []int64{left.pageID, right.pageID}
		left.parent = root.pageID
		left.dirty = true
		right.parent = root.pageID
		right.dirty = true
		t.rootID = root.pageID
		return nil
	}

	p := t.loadNode(left.parent)
	ci := t.childIndexOf(p, left.pageID)

	pos := len(p.keys)
	p.keys = append(p.keys, copyBytes(key))
	p.indices = append(p.indices, 0)
	copy(p.indices[ci+1:], p.indices[ci:])
	p.indices[ci] = pos

	p.children = append(p.children, nilPage)
	copy(p.children[ci+2:], p.children[ci+1:])
	p.children[ci+1] = right.pageID
	right.parent = p.pageID
	right.dirty = true
	p.dirty = true

	if p.numKeys() > t.maxKeys() {
		return t.splitInternal(p)
	}
	return nil
}

func (t *PersistentBPTree) childIndexOf(p *pnode, childID int64) int {
	for i, c := range p.children {
		if c == childID {
			return i
		}
	}
	return -1
}

func (t *PersistentBPTree) splitInternal(n *pnode) error {
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
		return err
	}
	right.keys = rKeys
	right.indices = rIdx
	right.children = rChildren
	right.parent = n.parent

	for _, cid := range rChildren {
		child := t.loadNode(cid)
		child.parent = right.pageID
		child.dirty = true
	}

	n.keys = lKeys
	n.indices = lIdx
	n.children = lChildren
	n.dirty = true

	return t.insertIntoParent(n, upKey, right)
}

// ---------- internal: delete ----------

func (t *PersistentBPTree) handleUnderflow(n *pnode) {
	if n.parent == nilPage {
		return
	}
	p := t.loadNode(n.parent)
	ci := t.childIndexOf(p, n.pageID)

	var leftSib, rightSib *pnode
	if ci > 0 {
		leftSib = t.loadNode(p.children[ci-1])
	}
	if ci < len(p.children)-1 {
		rightSib = t.loadNode(p.children[ci+1])
	}

	if n.isLeaf {
		if leftSib != nil && leftSib.numKeys() > t.minKeys() {
			t.borrowFromLeftLeaf(n, leftSib, p, ci)
			return
		}
		if rightSib != nil && rightSib.numKeys() > t.minKeys() {
			t.borrowFromRightLeaf(n, rightSib, p, ci)
			return
		}
		if leftSib != nil {
			t.mergeLeaves(leftSib, n, p, ci)
		} else {
			t.mergeLeaves(n, rightSib, p, ci+1)
		}
	} else {
		if leftSib != nil && leftSib.numKeys() > t.minKeys() {
			t.borrowFromLeftInternal(n, leftSib, p, ci)
			return
		}
		if rightSib != nil && rightSib.numKeys() > t.minKeys() {
			t.borrowFromRightInternal(n, rightSib, p, ci)
			return
		}
		if leftSib != nil {
			t.mergeInternal(leftSib, n, p, ci)
		} else {
			t.mergeInternal(n, rightSib, p, ci+1)
		}
	}
}

func (t *PersistentBPTree) borrowFromLeftLeaf(n, left, p *pnode, ci int) {
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
}

func (t *PersistentBPTree) borrowFromRightLeaf(n, right, p *pnode, ci int) {
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
}

func (t *PersistentBPTree) mergeLeaves(left, right, p *pnode, ci int) {
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

	if p.parent != nilPage && p.numKeys() < t.minKeys() {
		t.handleUnderflow(p)
	}
	if p.parent == nilPage && p.numKeys() == 0 {
		t.rootID = left.pageID
		left.parent = nilPage
		left.dirty = true
	}
}

func (t *PersistentBPTree) borrowFromLeftInternal(n, left, p *pnode, ci int) {
	sepKey := p.keyAt(ci - 1)
	last := left.numKeys() - 1

	pos := len(n.keys)
	n.keys = append(n.keys, sepKey)
	n.indices = append([]int{pos}, n.indices...)

	child := left.children[last+1]
	n.children = append([]int64{child}, n.children...)
	childNode := t.loadNode(child)
	childNode.parent = n.pageID
	childNode.dirty = true

	p.keys[p.indices[ci-1]] = left.keyAt(last)

	left.indices = left.indices[:last]
	left.children = left.children[:last+1]

	n.dirty = true
	left.dirty = true
	p.dirty = true
}

func (t *PersistentBPTree) borrowFromRightInternal(n, right, p *pnode, ci int) {
	sepKey := p.keyAt(ci)

	pos := len(n.keys)
	n.keys = append(n.keys, sepKey)
	n.indices = append(n.indices, pos)

	child := right.children[0]
	n.children = append(n.children, child)
	childNode := t.loadNode(child)
	childNode.parent = n.pageID
	childNode.dirty = true

	p.keys[p.indices[ci]] = right.keyAt(0)

	right.indices = right.indices[1:]
	right.children = right.children[1:]

	n.dirty = true
	right.dirty = true
	p.dirty = true
}

func (t *PersistentBPTree) mergeInternal(left, right, p *pnode, ci int) {
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
		child := t.loadNode(cid)
		child.parent = left.pageID
		child.dirty = true
	}

	p.indices = append(p.indices[:ci-1], p.indices[ci:]...)
	p.children = append(p.children[:ci], p.children[ci+1:]...)

	left.dirty = true
	p.dirty = true

	if p.parent != nilPage && p.numKeys() < t.minKeys() {
		t.handleUnderflow(p)
	}
	if p.parent == nilPage && p.numKeys() == 0 {
		t.rootID = left.pageID
		left.parent = nilPage
		left.dirty = true
	}
}

// ---------- validation ----------

func (t *PersistentBPTree) validate(n *pnode, isRoot bool) bool {
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
		child := t.loadNode(cid)
		if child.parent != n.pageID {
			return false
		}
		if !t.validate(child, false) {
			return false
		}
	}
	return t.checkDepth(n)
}

func (t *PersistentBPTree) checkDepth(n *pnode) bool {
	d := 0
	c := n
	for !c.isLeaf {
		c = t.loadNode(c.children[0])
		d++
	}
	var walk func(*pnode, int) bool
	walk = func(n *pnode, depth int) bool {
		if n.isLeaf {
			return depth == d
		}
		for _, cid := range n.children {
			if !walk(t.loadNode(cid), depth+1) {
				return false
			}
		}
		return true
	}
	return walk(n, 0)
}

// ---------- serialization ----------

// serializeNode converts a pnode to a compact byte slice.
// Keys are written in logical (sorted) order so that indices can be
// reconstructed as identity on deserialization.
//
// Layout:
//
//	[1]  isLeaf
//	[2]  numKeys   (uint16)
//	[8]  parent    (int64)
//	[8]  next      (int64)
//	per key:   [2] keyLen + keyLen bytes
//	if leaf, per value: [2] valLen + valLen bytes
//	if internal, per child: [8] pageID   (numKeys+1 children)
func serializeNode(n *pnode) []byte {
	numKeys := n.numKeys()

	// Pre-compute size.
	size := 1 + 2 + 8 + 8
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

func deserializeNode(pageID int64, data []byte) *pnode {
	off := 0

	isLeaf := data[off] == 1
	off++

	numKeys := int(binary.LittleEndian.Uint16(data[off:]))
	off += 2

	parent := int64(binary.LittleEndian.Uint64(data[off:]))
	off += 8

	next := int64(binary.LittleEndian.Uint64(data[off:]))
	off += 8

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
	}
}
