// Package bptree 实现了 B+ 树数据结构
// 本文件包含 LRU 缓存实现
package bptree

import (
	"container/list"
	"encoding/binary"
	"fmt"
	"log"
	"sync"
	"time"

	"lns.com/minidb/metrics"
)

// LRUCache LRU缓存
// 用于缓存B+树节点，减少磁盘IO
// mu: 互斥锁保护缓存
// capacity: 缓存容量
// items: pageID到缓存条目的映射
// order: 双向链表，front=最近使用(MRU)，back=最久未使用(LRU)
// pager: 页面管理器
// compressor: 压缩器（可选）
// formatVersion: 格式版本
type LRUCache struct {
	mu            sync.Mutex
	capacity      int
	items         map[int64]*list.Element
	order         *list.List
	pager         *Pager
	compressor    Compressor
	formatVersion uint32
}

// cacheEntry 缓存条目
// pageID: 页面ID
// node: 缓存的节点
// pins: 固定计数，大于0时不可驱逐
type cacheEntry struct {
	pageID int64
	node   *pnode
	pins   int
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

const overflowSentinel byte = 0x03

// writeNodeToPager serializes, compresses, and writes a node to the pager.
// If the data exceeds a single page, it transparently uses overflow pages.
func (c *LRUCache) writeNodeToPager(n *pnode) error {
	blob := serializeNode(n, c.formatVersion)
	if c.compressor != nil {
		blob = c.compressor.Compress(blob)
	}

	slotSize := c.pager.slotSize

	// Free old overflow pages from a previous write of this node.
	for _, oid := range n.overflowPages {
		c.pager.Free(oid)
	}
	n.overflowPages = nil

	// Fast path: data fits in one page.
	// Layout: [4B size prefix][data] — the pager adds the size prefix.
	if int64(len(blob))+4 <= slotSize {
		return c.pager.Write(n.pageID, blob)
	}

	// Overflow path.
	maxPrefix := int(slotSize) - 4 - 13 // 4B size + 1B sentinel + 8B overflowID + 4B remaining
	maxChunk := int(slotSize) - 4 - 12  // 4B size + 8B nextPageID + 4B chunkLen

	if maxPrefix <= 0 || maxChunk <= 0 {
		return fmt.Errorf("bptree: slot size %d too small for overflow", slotSize)
	}

	prefix := blob
	if len(prefix) > maxPrefix {
		prefix = prefix[:maxPrefix]
	}
	remaining := blob[len(prefix):]

	var overflowIDs []int64

	// Write overflow chain.
	for len(remaining) > 0 {
		chunk := remaining
		if len(chunk) > maxChunk {
			chunk = chunk[:maxChunk]
		}
		remaining = remaining[len(chunk):]

		oid, err := c.pager.Allocate()
		if err != nil {
			// Clean up allocated overflow pages on error.
			for _, id := range overflowIDs {
				c.pager.Free(id)
			}
			return err
		}

		// nextPageID will be filled in later; -1 for now.
		ovfBuf := make([]byte, 8+4+len(chunk))
		var nilPageID int64 = -1
		binary.LittleEndian.PutUint64(ovfBuf[0:], uint64(nilPageID)) // nextPageID placeholder
		binary.LittleEndian.PutUint32(ovfBuf[8:], uint32(len(chunk)))
		copy(ovfBuf[12:], chunk)

		if err := c.pager.Write(oid, ovfBuf); err != nil {
			c.pager.Free(oid)
			for _, id := range overflowIDs {
				c.pager.Free(id)
			}
			return err
		}
		overflowIDs = append(overflowIDs, oid)
	}

	// Patch nextPageID links: each page points to the next, last stays -1.
	for i := 0; i < len(overflowIDs)-1; i++ {
		raw, err := c.pager.Read(overflowIDs[i])
		if err != nil {
			return err
		}
		binary.LittleEndian.PutUint64(raw[0:], uint64(overflowIDs[i+1]))
		if err := c.pager.Write(overflowIDs[i], raw); err != nil {
			return err
		}
	}

	// Build primary page: [0x03 sentinel][prefix][8B firstOverflowID][4B remainingBytes]
	firstOverflowID := int64(-1)
	remainingBytes := uint32(0)
	if len(overflowIDs) > 0 {
		firstOverflowID = overflowIDs[0]
		// remainingBytes = total bytes in the overflow chain
		remainingBytes = uint32(len(blob) - len(prefix))
	}

	primary := make([]byte, 1+len(prefix)+8+4)
	primary[0] = overflowSentinel
	copy(primary[1:], prefix)
	binary.LittleEndian.PutUint64(primary[1+len(prefix):], uint64(firstOverflowID))
	binary.LittleEndian.PutUint32(primary[1+len(prefix)+8:], remainingBytes)

	if err := c.pager.Write(n.pageID, primary); err != nil {
		return err
	}

	n.overflowPages = overflowIDs
	return nil
}

// readNodeData reads the full node data from the pager, transparently
// reassembling overflow pages. Returns (decompressedData, overflowPageIDs, error).
func (c *LRUCache) readNodeData(pageID int64) ([]byte, []int64, error) {
	rawData, err := c.pager.Read(pageID)
	if err != nil {
		return nil, nil, err
	}

	if len(rawData) > 0 && rawData[0] == overflowSentinel {
		// Overflow primary page.
		// Layout: [0x03][prefix][8B firstOverflowID][4B remainingBytes]
		trailerStart := len(rawData) - 12
		prefix := rawData[1:trailerStart]
		firstOverflowID := int64(binary.LittleEndian.Uint64(rawData[trailerStart:]))
		remainingBytes := binary.LittleEndian.Uint32(rawData[trailerStart+8:])

		// Reassemble full compressed blob from overflow chain.
		fullData := make([]byte, 0, len(prefix)+int(remainingBytes))
		fullData = append(fullData, prefix...)

		var overflowIDs []int64
		curID := firstOverflowID
		for curID != -1 {
			overflowIDs = append(overflowIDs, curID)
			chunkPage, err := c.pager.Read(curID)
			if err != nil {
				return nil, overflowIDs, fmt.Errorf("bptree: reading overflow page %d: %w", curID, err)
			}
			// Layout: [8B nextPageID][4B chunkLen][chunk...]
			if len(chunkPage) < 12 {
				return nil, overflowIDs, fmt.Errorf("bptree: corrupted overflow page %d", curID)
			}
			nextPageID := int64(binary.LittleEndian.Uint64(chunkPage[0:8]))
			chunkLen := int(binary.LittleEndian.Uint32(chunkPage[8:12]))
			if 12+chunkLen > len(chunkPage) {
				return nil, overflowIDs, fmt.Errorf("bptree: corrupted overflow page %d chunk", curID)
			}
			fullData = append(fullData, chunkPage[12:12+chunkLen]...)
			curID = nextPageID
		}

		// Decompress the reassembled blob.
		if c.compressor != nil {
			fullData, err = c.compressor.Decompress(fullData)
			if err != nil {
				return nil, overflowIDs, err
			}
		}
		return fullData, overflowIDs, nil
	}

	// Normal page — decompress if needed.
	data := rawData
	if c.compressor != nil {
		data, err = c.compressor.Decompress(data)
		if err != nil {
			return nil, nil, err
		}
	}
	return data, nil, nil
}

// GetOrLoad atomically checks the cache and loads from disk if absent.
// The returned node is pinned (pins incremented). Caller must call Unpin when done.
func (c *LRUCache) GetOrLoad(pageID int64) (*pnode, error) {
	start := time.Now()
	c.mu.Lock()
	if elem, ok := c.items[pageID]; ok {
		c.order.MoveToFront(elem)
		ent := elem.Value.(*cacheEntry)
		ent.pins++
		node := ent.node
		c.mu.Unlock()
		metrics.CacheGetOrLoadDuration.WithLabelValues("true").Observe(time.Since(start).Seconds())
		metrics.CacheHitsTotal.Inc()
		return node, nil
	}
	c.mu.Unlock()

	// Load from disk without holding cache lock to avoid deadlock.
	data, overflowIDs, err := c.readNodeData(pageID)
	if err != nil {
		return nil, err
	}

	n := deserializeNode(pageID, data, c.formatVersion)
	n.overflowPages = overflowIDs

	c.mu.Lock()
	defer c.mu.Unlock()

	// Double-check: another goroutine might have loaded it concurrently.
	if elem, ok := c.items[pageID]; ok {
		ent := elem.Value.(*cacheEntry)
		ent.pins++
		return ent.node, nil
	}

	// Try to evict unpinned entries. If all are pinned, expand capacity.
	evicted := false
	for len(c.items) >= c.capacity {
		if !c.evictOne() {
			// All entries are pinned — expand capacity to avoid infinite loop.
			c.capacity = len(c.items) + 256
			break
		}
		evicted = true
	}
	_ = evicted

	entry := &cacheEntry{pageID: pageID, node: n, pins: 1}
	elem := c.order.PushFront(entry)
	c.items[pageID] = elem
	metrics.CacheGetOrLoadDuration.WithLabelValues("false").Observe(time.Since(start).Seconds())
	metrics.CacheMissesTotal.Inc()
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
		if !c.evictOne() {
			c.capacity = len(c.items) + 256
			break
		}
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
			if err := c.writeNodeToPager(ent.node); err != nil {
				log.Printf("LRU flush: write page %d failed: %v", ent.node.pageID, err)
			}
			ent.node.dirty = false
		}
	}
}

func (c *LRUCache) Len() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.items)
}

// evictOne tries to evict one unpinned entry. Returns true if an entry was evicted.
func (c *LRUCache) evictOne() bool {
	start := time.Now()
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
			if err := c.writeNodeToPager(ent.node); err != nil {
				log.Printf("LRU evict: write page %d failed: %v", ent.node.pageID, err)
			}
			ent.node.dirty = false
		}
		metrics.CacheEvictDuration.Observe(time.Since(start).Seconds())
		return true
	}
	return false
}
