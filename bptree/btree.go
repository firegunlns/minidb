package bptree

import (
	"bytes"
)

type BPTree struct {
	root  *node
	order int
}

type node struct {
	isLeaf   bool
	keys     [][]byte
	values   [][]byte
	indices  []int
	children []*node
	next     *node
	parent   *node
}

func New(order int) *BPTree {
	if order < 3 {
		order = 3
	}
	return &BPTree{order: order}
}

func (t *BPTree) maxKeys() int { return t.order - 1 }
func (t *BPTree) minKeys() int { return (t.order - 1) / 2 }

func (n *node) numKeys() int       { return len(n.indices) }
func (n *node) keyAt(i int) []byte { return n.keys[n.indices[i]] }
func (n *node) valAt(i int) []byte { return n.values[n.indices[i]] }

func (t *BPTree) Find(key []byte) ([]byte, bool) {
	if t.root == nil {
		return nil, false
	}
	return t.find(t.root, key)
}

func (t *BPTree) find(n *node, key []byte) ([]byte, bool) {
	if n.isLeaf {
		idx := t.leafSearch(n, key)
		if idx < n.numKeys() && bytes.Equal(n.keyAt(idx), key) {
			return n.valAt(idx), true
		}
		return nil, false
	}
	return t.find(n.children[t.childIndex(n, key)], key)
}

func (t *BPTree) leafSearch(n *node, key []byte) int {
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

func (t *BPTree) childIndex(n *node, key []byte) int {
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

func (t *BPTree) Insert(key, value []byte) {
	if t.root == nil {
		t.root = &node{
			isLeaf:  true,
			keys:    [][]byte{copyBytes(key)},
			values:  [][]byte{copyBytes(value)},
			indices: []int{0},
		}
		return
	}
	leaf := t.findLeaf(key)
	t.insertIntoLeaf(leaf, key, value)
	if leaf.numKeys() > t.maxKeys() {
		t.splitLeaf(leaf)
	}
}

func (t *BPTree) findLeaf(key []byte) *node {
	n := t.root
	for !n.isLeaf {
		n = n.children[t.childIndex(n, key)]
	}
	return n
}

func (t *BPTree) insertIntoLeaf(leaf *node, key, value []byte) {
	idx := t.leafSearch(leaf, key)
	if idx < leaf.numKeys() && bytes.Equal(leaf.keyAt(idx), key) {
		leaf.values[leaf.indices[idx]] = copyBytes(value)
		return
	}
	pos := len(leaf.keys)
	leaf.keys = append(leaf.keys, copyBytes(key))
	leaf.values = append(leaf.values, copyBytes(value))
	leaf.indices = append(leaf.indices, 0)
	copy(leaf.indices[idx+1:], leaf.indices[idx:])
	leaf.indices[idx] = pos
}

func compactLeaf(src *node, start, end int) ([][]byte, [][]byte, []int) {
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

func (t *BPTree) splitLeaf(leaf *node) {
	mid := leaf.numKeys() / 2

	lKeys, lVals, lIdx := compactLeaf(leaf, 0, mid)
	rKeys, rVals, rIdx := compactLeaf(leaf, mid, leaf.numKeys())

	right := &node{
		isLeaf:  true,
		keys:    rKeys,
		values:  rVals,
		indices: rIdx,
		next:    leaf.next,
		parent:  leaf.parent,
	}

	leaf.keys = lKeys
	leaf.values = lVals
	leaf.indices = lIdx
	leaf.next = right

	t.insertIntoParent(leaf, right.keyAt(0), right)
}

func (t *BPTree) insertIntoParent(left *node, key []byte, right *node) {
	if left.parent == nil {
		root := &node{
			isLeaf:   false,
			keys:     [][]byte{copyBytes(key)},
			indices:  []int{0},
			children: []*node{left, right},
		}
		left.parent = root
		right.parent = root
		t.root = root
		return
	}

	p := left.parent
	ci := t.childIndexOf(p, left)

	pos := len(p.keys)
	p.keys = append(p.keys, copyBytes(key))
	p.indices = append(p.indices, 0)
	copy(p.indices[ci+1:], p.indices[ci:])
	p.indices[ci] = pos

	p.children = append(p.children, nil)
	copy(p.children[ci+2:], p.children[ci+1:])
	p.children[ci+1] = right
	right.parent = p

	if p.numKeys() > t.maxKeys() {
		t.splitInternal(p)
	}
}

func (t *BPTree) childIndexOf(p, child *node) int {
	for i, c := range p.children {
		if c == child {
			return i
		}
	}
	return -1
}

func (t *BPTree) splitInternal(n *node) {
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

	lChildren := make([]*node, lCount+1)
	copy(lChildren, n.children[:lCount+1])

	rChildren := make([]*node, rCount+1)
	copy(rChildren, n.children[mid+1:])

	right := &node{
		isLeaf:   false,
		keys:     rKeys,
		indices:  rIdx,
		children: rChildren,
		parent:   n.parent,
	}
	for _, c := range rChildren {
		c.parent = right
	}

	n.keys = lKeys
	n.indices = lIdx
	n.children = lChildren

	t.insertIntoParent(n, upKey, right)
}

func (t *BPTree) Delete(key []byte) bool {
	if t.root == nil {
		return false
	}
	leaf := t.findLeaf(key)
	idx := t.leafSearch(leaf, key)
	if idx >= leaf.numKeys() || !bytes.Equal(leaf.keyAt(idx), key) {
		return false
	}

	leaf.indices = append(leaf.indices[:idx], leaf.indices[idx+1:]...)

	if leaf.parent != nil && leaf.numKeys() < t.minKeys() {
		t.handleUnderflow(leaf)
	}
	if t.root != nil && !t.root.isLeaf && t.root.numKeys() == 0 {
		t.root = t.root.children[0]
		t.root.parent = nil
	}
	if t.root != nil && t.root.isLeaf && t.root.numKeys() == 0 {
		t.root = nil
	}
	return true
}

func (t *BPTree) handleUnderflow(n *node) {
	if n.parent == nil {
		return
	}
	p := n.parent
	ci := t.childIndexOf(p, n)

	var leftSib, rightSib *node
	if ci > 0 {
		leftSib = p.children[ci-1]
	}
	if ci < len(p.children)-1 {
		rightSib = p.children[ci+1]
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

func (t *BPTree) borrowFromLeftLeaf(n, left *node, p *node, ci int) {
	last := left.numKeys() - 1
	physIdx := left.indices[last]

	pos := len(n.keys)
	n.keys = append(n.keys, left.keys[physIdx])
	n.values = append(n.values, left.values[physIdx])
	n.indices = append([]int{pos}, n.indices...)

	left.indices = left.indices[:last]

	p.keys[p.indices[ci-1]] = n.keyAt(0)
}

func (t *BPTree) borrowFromRightLeaf(n, right *node, p *node, ci int) {
	physIdx := right.indices[0]

	pos := len(n.keys)
	n.keys = append(n.keys, right.keys[physIdx])
	n.values = append(n.values, right.values[physIdx])
	n.indices = append(n.indices, pos)

	right.indices = right.indices[1:]

	p.keys[p.indices[ci]] = right.keyAt(0)
}

func (t *BPTree) mergeLeaves(left, right *node, p *node, ci int) {
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

	if p.parent != nil && p.numKeys() < t.minKeys() {
		t.handleUnderflow(p)
	}
	if p.parent == nil && p.numKeys() == 0 {
		t.root = left
		left.parent = nil
	}
}

func (t *BPTree) borrowFromLeftInternal(n, left *node, p *node, ci int) {
	sepKey := p.keyAt(ci - 1)
	last := left.numKeys() - 1

	pos := len(n.keys)
	n.keys = append(n.keys, sepKey)
	n.indices = append([]int{pos}, n.indices...)

	child := left.children[last+1]
	n.children = append([]*node{child}, n.children...)
	child.parent = n

	p.keys[p.indices[ci-1]] = left.keyAt(last)

	left.indices = left.indices[:last]
	left.children = left.children[:last+1]
}

func (t *BPTree) borrowFromRightInternal(n, right *node, p *node, ci int) {
	sepKey := p.keyAt(ci)

	pos := len(n.keys)
	n.keys = append(n.keys, sepKey)
	n.indices = append(n.indices, pos)

	child := right.children[0]
	n.children = append(n.children, child)
	child.parent = n

	p.keys[p.indices[ci]] = right.keyAt(0)

	right.indices = right.indices[1:]
	right.children = right.children[1:]
}

func (t *BPTree) mergeInternal(left, right *node, p *node, ci int) {
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

	for _, c := range right.children {
		left.children = append(left.children, c)
		c.parent = left
	}

	p.indices = append(p.indices[:ci-1], p.indices[ci:]...)
	p.children = append(p.children[:ci], p.children[ci+1:]...)

	if p.parent != nil && p.numKeys() < t.minKeys() {
		t.handleUnderflow(p)
	}
	if p.parent == nil && p.numKeys() == 0 {
		t.root = left
		left.parent = nil
	}
}

func (t *BPTree) RangeScan(start, end []byte) []KeyValuePair {
	var results []KeyValuePair
	if t.root == nil {
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
		leaf = leaf.next
	}
	return results
}

type KeyValuePair struct {
	Key   []byte
	Value []byte
}

func (t *BPTree) Count() int {
	if t.root == nil {
		return 0
	}
	n := t.root
	for !n.isLeaf {
		n = n.children[0]
	}
	count := 0
	for n != nil {
		count += n.numKeys()
		n = n.next
	}
	return count
}

func (t *BPTree) Validate() bool {
	if t.root == nil {
		return true
	}
	return t.validate(t.root, true)
}

func (t *BPTree) validate(n *node, isRoot bool) bool {
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
	for _, c := range n.children {
		if c.parent != n {
			return false
		}
		if !t.validate(c, false) {
			return false
		}
	}
	return t.checkDepth(n)
}

func (t *BPTree) checkDepth(n *node) bool {
	d := 0
	c := n
	for !c.isLeaf {
		c = c.children[0]
		d++
	}
	var walk func(*node, int) bool
	walk = func(n *node, depth int) bool {
		if n.isLeaf {
			return depth == d
		}
		for _, child := range n.children {
			if !walk(child, depth+1) {
				return false
			}
		}
		return true
	}
	return walk(n, 0)
}

func copyBytes(b []byte) []byte {
	if b == nil {
		return nil
	}
	cp := make([]byte, len(b))
	copy(cp, b)
	return cp
}
