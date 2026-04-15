package txn

import "sync"

// Workspace is a private buffer for a transaction's reads and writes.
type Workspace struct {
	mu       sync.RWMutex
	writes   map[string][]byte // pk_string -> rowData (nil = delete)
	reads    map[string]uint64 // pk_string -> commitTS at read time (for OCC validation)
	readPKs  map[string][]byte // pk_string -> raw pk bytes
	inserted map[string]bool   // pk_string -> true if this txn inserted it (no conflict check needed)
}

func NewWorkspace() *Workspace {
	return &Workspace{
		writes:   make(map[string][]byte),
		reads:    make(map[string]uint64),
		readPKs:  make(map[string][]byte),
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

// RecordRead records a read for OCC validation.
func (w *Workspace) RecordRead(treeKey string, pk []byte, commitTS uint64) {
	w.mu.Lock()
	defer w.mu.Unlock()
	key := wsKey(treeKey, pk)
	w.reads[key] = commitTS
	w.readPKs[key] = pk
}

// IsInserted checks if a key was inserted by this txn.
func (w *Workspace) IsInserted(treeKey string, pk []byte) bool {
	w.mu.RLock()
	defer w.mu.RUnlock()
	key := wsKey(treeKey, pk)
	return w.inserted[key]
}

// ReadSet returns all tracked reads for OCC validation.
func (w *Workspace) ReadSet() map[string]uint64 {
	w.mu.RLock()
	defer w.mu.RUnlock()
	out := make(map[string]uint64, len(w.reads))
	for k, v := range w.reads {
		out[k] = v
	}
	return out
}

// WriteSet returns all buffered writes.
func (w *Workspace) WriteSet() map[string][]byte {
	w.mu.RLock()
	defer w.mu.RUnlock()
	out := make(map[string][]byte, len(w.writes))
	for k, v := range w.writes {
		out[k] = v
	}
	return out
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
