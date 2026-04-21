// Package bptree 实现了 B+ 树数据结构
package bptree

// lockChain 锁链
// 在乐观锁耦合写操作期间跟踪持有写锁的祖先节点
// 由于writeMu串行化所有写操作，已释放的祖先不能被其他写操作修改
// 链从根向叶排序：nodes[0]是最高的（最接近根）祖先，nodes[len-1]是最低的（最接近叶）
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
