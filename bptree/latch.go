package bptree

// lockChain tracks ancestor nodes held with write locks during optimistic
// latch coupling for write operations. Since writeMu serializes all writers,
// released ancestors cannot be modified by other writers.
//
// The chain is ordered from root toward leaf: nodes[0] is the highest
// (closest to root) ancestor still held, nodes[len-1] is the lowest
// (closest to leaf).
type lockChain struct {
	nodes []*pnode
}

func (lc *lockChain) push(n *pnode) {
	lc.nodes = append(lc.nodes, n)
}

// releaseAll unlocks and unpins every node in the chain, then empties it.
func (lc *lockChain) releaseAll(t *PersistentBPTree) {
	for i := len(lc.nodes) - 1; i >= 0; i-- {
		lc.nodes[i].mu.Unlock()
		t.unpin(lc.nodes[i].pageID)
		lc.nodes[i] = nil
	}
	lc.nodes = lc.nodes[:0]
}

// findAndPop looks for the node with the given pageID, removes it from
// the chain, and returns it (still locked). Returns nil if not found.
func (lc *lockChain) findAndPop(pageID int64) *pnode {
	for i := len(lc.nodes) - 1; i >= 0; i-- {
		if lc.nodes[i].pageID == pageID {
			n := lc.nodes[i]
			lc.nodes[i] = nil
			lc.nodes = append(lc.nodes[:i], lc.nodes[i+1:]...)
			return n
		}
	}
	return nil
}

// find looks for the node with the given pageID in the chain.
// Returns the node if found (still locked), nil otherwise.
func (lc *lockChain) find(pageID int64) *pnode {
	for i := len(lc.nodes) - 1; i >= 0; i-- {
		if lc.nodes[i].pageID == pageID {
			return lc.nodes[i]
		}
	}
	return nil
}
