// Package txn 提供事务管理功能
package txn

import "sync"

// Workspace 事务的私有缓冲区
// 存储事务的写集
type Workspace struct {
	mu       sync.RWMutex
	writes   map[string][]byte // pk_string -> 行数据 (nil = 删除)
	inserted map[string]bool   // pk_string -> true表示本事务插入
}

func NewWorkspace() *Workspace {
	return &Workspace{
		writes:   make(map[string][]byte),
		inserted: make(map[string]bool),
	}
}

// SetWrite buffers a write (insert/update). rowData=nil means delete.
func (w *Workspace) SetWrite(treeKey string, pk []byte, rowData []byte) {
	w.mu.Lock()
	defer w.mu.Unlock()
	key := wsKey(treeKey, pk)
	w.writes[key] = rowData
}

// SetInsert marks a key as inserted by this txn (skip conflict check on commit).
func (w *Workspace) SetInsert(treeKey string, pk []byte) {
	w.mu.Lock()
	defer w.mu.Unlock()
	key := wsKey(treeKey, pk)
	w.inserted[key] = true
}

// SetDelete buffers a delete.
func (w *Workspace) SetDelete(treeKey string, pk []byte) {
	w.mu.Lock()
	defer w.mu.Unlock()
	key := wsKey(treeKey, pk)
	w.writes[key] = nil
}

// GetWrite returns buffered write data for a key, or nil if not buffered.
// The second return value indicates whether the key was buffered.
// If buffered with nil data, it means a delete.
func (w *Workspace) GetWrite(treeKey string, pk []byte) ([]byte, bool) {
	w.mu.RLock()
	defer w.mu.RUnlock()
	key := wsKey(treeKey, pk)
	data, ok := w.writes[key]
	return data, ok
}

// IsInserted checks if a key was inserted by this txn.
func (w *Workspace) IsInserted(treeKey string, pk []byte) bool {
	w.mu.RLock()
	defer w.mu.RUnlock()
	key := wsKey(treeKey, pk)
	return w.inserted[key]
}

// IsDelete checks if a write was a delete (nil rowData).
func (w *Workspace) IsDelete(treeKey string, pk []byte) bool {
	w.mu.RLock()
	defer w.mu.RUnlock()
	key := wsKey(treeKey, pk)
	data, ok := w.writes[key]
	return ok && data == nil
}

func wsKey(treeKey string, pk []byte) string {
	return treeKey + "\x00" + string(pk)
}
