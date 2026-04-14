package bptree

import "container/list"

// LRUCache holds recently-accessed pnode objects.
// When the number of cached entries reaches capacity, the least-recently-used
// entry is evicted; if it is dirty it is serialized and flushed to disk via
// the Pager.
type LRUCache struct {
	capacity int
	items    map[int64]*list.Element
	order    *list.List // front = MRU, back = LRU
	pager    *Pager
}

type cacheEntry struct {
	pageID int64
	node   *pnode
}

// NewLRUCache creates a cache that holds up to capacity nodes.
func NewLRUCache(capacity int, pager *Pager) *LRUCache {
	return &LRUCache{
		capacity: capacity,
		items:    make(map[int64]*list.Element),
		order:    list.New(),
		pager:    pager,
	}
}

// Get returns the cached node for pageID, or nil on miss.
// A hit promotes the entry to MRU position.
func (c *LRUCache) Get(pageID int64) *pnode {
	if elem, ok := c.items[pageID]; ok {
		c.order.MoveToFront(elem)
		return elem.Value.(*cacheEntry).node
	}
	return nil
}

// Put adds or updates a node in the cache.
// If the cache is at capacity the LRU entry is evicted first.
func (c *LRUCache) Put(n *pnode) {
	if elem, ok := c.items[n.pageID]; ok {
		c.order.MoveToFront(elem)
		elem.Value.(*cacheEntry).node = n
		return
	}

	for len(c.items) >= c.capacity {
		c.evict()
	}

	entry := &cacheEntry{pageID: n.pageID, node: n}
	elem := c.order.PushFront(entry)
	c.items[n.pageID] = elem
}

// Remove drops a node from the cache without writing it to disk.
func (c *LRUCache) Remove(pageID int64) {
	if elem, ok := c.items[pageID]; ok {
		c.order.Remove(elem)
		delete(c.items, pageID)
	}
}

// Flush writes every dirty node in the cache to disk.
func (c *LRUCache) Flush() {
	for _, elem := range c.items {
		ent := elem.Value.(*cacheEntry)
		if ent.node.dirty {
			data := serializeNode(ent.node)
			c.pager.Write(ent.node.pageID, data)
			ent.node.dirty = false
		}
	}
}

// Len returns the number of entries currently in the cache.
func (c *LRUCache) Len() int {
	return len(c.items)
}

func (c *LRUCache) evict() {
	elem := c.order.Back()
	if elem == nil {
		return
	}
	c.order.Remove(elem)
	ent := elem.Value.(*cacheEntry)
	delete(c.items, ent.pageID)

	if ent.node.dirty {
		data := serializeNode(ent.node)
		c.pager.Write(ent.node.pageID, data)
		ent.node.dirty = false
	}
}
