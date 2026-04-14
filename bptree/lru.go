package bptree

import (
	"container/list"
	"sync"
)

type LRUCache struct {
	mu            sync.Mutex
	capacity      int
	items         map[int64]*list.Element
	order         *list.List // front = MRU, back = LRU
	pager         *Pager
	compressor    Compressor
	formatVersion uint32
}

type cacheEntry struct {
	pageID int64
	node   *pnode
	pins   int // while > 0, entry cannot be evicted
}

func NewLRUCache(capacity int, pager *Pager, compressor Compressor, formatVersion uint32) *LRUCache {
	return &LRUCache{
		capacity:      capacity,
		items:         make(map[int64]*list.Element),
		order:         list.New(),
		pager:         pager,
		compressor:    compressor,
		formatVersion: formatVersion,
	}
}

// GetOrLoad atomically checks the cache and loads from disk if absent.
// The returned node is pinned (pins incremented). Caller must call Unpin when done.
func (c *LRUCache) GetOrLoad(pageID int64) (*pnode, error) {
	c.mu.Lock()
	if elem, ok := c.items[pageID]; ok {
		c.order.MoveToFront(elem)
		ent := elem.Value.(*cacheEntry)
		ent.pins++
		node := ent.node
		c.mu.Unlock()
		return node, nil
	}
	c.mu.Unlock()

	// Load from disk without holding cache lock to avoid deadlock.
	data, err := c.pager.Read(pageID)
	if err != nil {
		return nil, err
	}

	// Decompress if needed.
	if c.compressor != nil {
		data, err = c.compressor.Decompress(data)
		if err != nil {
			return nil, err
		}
	}

	n := deserializeNode(pageID, data, c.formatVersion)

	c.mu.Lock()
	defer c.mu.Unlock()

	// Double-check: another goroutine might have loaded it concurrently.
	if elem, ok := c.items[pageID]; ok {
		ent := elem.Value.(*cacheEntry)
		ent.pins++
		return ent.node, nil
	}

	for len(c.items) >= c.capacity {
		c.evict()
	}

	entry := &cacheEntry{pageID: pageID, node: n, pins: 1}
	elem := c.order.PushFront(entry)
	c.items[pageID] = elem
	return n, nil
}

// PutPinned inserts a newly allocated node into the cache with pins=1.
// Caller must call Unpin when done. Used by allocNode.
func (c *LRUCache) PutPinned(n *pnode) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if _, ok := c.items[n.pageID]; ok {
		return
	}

	for len(c.items) >= c.capacity {
		c.evict()
	}

	entry := &cacheEntry{pageID: n.pageID, node: n, pins: 1}
	elem := c.order.PushFront(entry)
	c.items[n.pageID] = elem
}

// Unpin decrements the pin counter for the given page.
func (c *LRUCache) Unpin(pageID int64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if elem, ok := c.items[pageID]; ok {
		ent := elem.Value.(*cacheEntry)
		ent.pins--
	}
}

func (c *LRUCache) Flush() {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, elem := range c.items {
		ent := elem.Value.(*cacheEntry)
		if ent.node.dirty {
			data := serializeNode(ent.node, c.formatVersion)
			if c.compressor != nil {
				data = c.compressor.Compress(data)
			}
			c.pager.Write(ent.node.pageID, data)
			ent.node.dirty = false
		}
	}
}

func (c *LRUCache) Len() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.items)
}

func (c *LRUCache) evict() {
	elem := c.order.Back()
	for elem != nil {
		ent := elem.Value.(*cacheEntry)
		if ent.pins > 0 {
			elem = elem.Prev()
			continue
		}
		c.order.Remove(elem)
		delete(c.items, ent.pageID)
		if ent.node.dirty {
			data := serializeNode(ent.node, c.formatVersion)
			if c.compressor != nil {
				data = c.compressor.Compress(data)
			}
			c.pager.Write(ent.node.pageID, data)
			ent.node.dirty = false
		}
		return
	}
}
