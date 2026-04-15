package txn

import (
	"errors"
	"log"

	"lns.com/minidb/storage"
)

var ErrConflict = errors.New("transaction conflict: read set modified by another transaction")

// Txn represents a single database transaction.
type Txn struct {
	mgr       *Manager
	startTS   uint64 // snapshot timestamp
	commitTS  uint64 // assigned at commit time
	ws        *Workspace
	finalized bool
}

// Manager coordinates transactions with OCC.
type Manager struct {
	engine *storage.StorageEngine
	ts     *TimestampOracle
}

func NewManager(engine *storage.StorageEngine, ts *TimestampOracle) *Manager {
	return &Manager{engine: engine, ts: ts}
}

// Begin starts a new transaction with a snapshot timestamp.
func (m *Manager) Begin() *Txn {
	return &Txn{
		mgr:     m,
		startTS: m.ts.Current(), // snapshot at current time
		ws:      NewWorkspace(),
	}
}

// Get reads a row, checking the workspace first (read-your-writes),
// then falling through to the storage engine at the snapshot timestamp.
func (t *Txn) Get(treeKey string, cols []storage.ColumnDef, pk []byte) ([]byte, error) {
	if t.finalized {
		return nil, errors.New("transaction is finalized")
	}

	// Check workspace first.
	if data, ok := t.ws.GetWrite(treeKey, pk); ok {
		if data == nil {
			return nil, nil // deleted in this txn
		}
		return data, nil
	}

	// Read from engine at snapshot time.
	rowData, commitTS, err := t.mgr.engine.GetRow(treeKey, pk, t.startTS)
	if err != nil {
		return nil, err
	}

	// Record read for OCC validation (skip if this txn inserted the key).
	if !t.ws.IsInserted(treeKey, pk) {
		t.ws.RecordRead(treeKey, pk, commitTS)
	}

	return rowData, nil
}

// Insert adds a new row to the workspace.
func (t *Txn) Insert(treeKey string, pk []byte, rowData []byte) error {
	if t.finalized {
		return errors.New("transaction is finalized")
	}
	t.ws.SetWrite(treeKey, pk, rowData)
	t.ws.SetInsert(treeKey, pk)
	return nil
}

// Update buffers an update in the workspace.
func (t *Txn) Update(treeKey string, cols []storage.ColumnDef, pk []byte, newRow []byte) error {
	if t.finalized {
		return errors.New("transaction is finalized")
	}
	// Record the current version for OCC validation.
	if !t.ws.IsInserted(treeKey, pk) {
		_, commitTS, err := t.mgr.engine.GetRow(treeKey, pk, t.startTS)
		if err != nil {
			return err
		}
		t.ws.RecordRead(treeKey, pk, commitTS)
	}
	t.ws.SetWrite(treeKey, pk, newRow)
	return nil
}

// Delete buffers a delete in the workspace.
func (t *Txn) Delete(treeKey string, cols []storage.ColumnDef, pk []byte) error {
	if t.finalized {
		return errors.New("transaction is finalized")
	}
	if !t.ws.IsInserted(treeKey, pk) {
		_, commitTS, err := t.mgr.engine.GetRow(treeKey, pk, t.startTS)
		if err != nil {
			return err
		}
		t.ws.RecordRead(treeKey, pk, commitTS)
	}
	t.ws.SetDelete(treeKey, pk)
	return nil
}

// Scan iterates over rows in a key range, merging workspace writes with engine data.
func (t *Txn) Scan(treeKey string, cols []storage.ColumnDef, start, end []byte, fn func(pk, row []byte) bool) {
	log.Printf("Txn.Scan finalized=%v treeKey=%s start=%x end=%x startTS=%d", t.finalized, treeKey, start, end, t.startTS)
	if t.finalized {
		return
	}

	// Collect workspace writes in the range.
	wsResults := make(map[string][]byte)
	t.ws.mu.RLock()
	for key, data := range t.ws.writes {
		// Check if this key belongs to the right tree.
		wsPK, ok := t.wsPKInRange(key, treeKey, start, end)
		if ok {
			wsResults[string(wsPK)] = data
		}
	}
	t.ws.mu.RUnlock()

	// Scan engine and combine with workspace.
	seen := make(map[string]bool)
	t.mgr.engine.ScanRange(treeKey, start, end, t.startTS, func(pk, row []byte) bool {
		pkStr := string(pk)
		seen[pkStr] = true
		// Check if workspace overrides this.
		if wsData, ok := wsResults[pkStr]; ok {
			if wsData != nil {
				if !fn(pk, wsData) {
					return false
				}
			}
			// nil = deleted, skip
			return true
		}
		if !fn(pk, row) {
			return false
		}
		return true
	})

	// Add workspace inserts that weren't in the engine scan.
	for pkStr, data := range wsResults {
		if seen[pkStr] {
			continue
		}
		if data == nil {
			continue // delete of non-existent row
		}
		if !fn([]byte(pkStr), data) {
			break
		}
	}
}

// Commit validates the read set and applies writes.
func (t *Txn) Commit() error {
	if t.finalized {
		return errors.New("transaction is finalized")
	}
	t.finalized = true

	// Validate read set: re-read each key and check commitTS hasn't changed.
	readSet := t.ws.ReadSet()
	for key, origTS := range readSet {
		pk := t.ws.readPKs[key]
		// Extract treeKey from the wsKey.
		treeKey := wsKeyToTree(key)
		_, curTS, err := t.mgr.engine.GetRow(treeKey, pk, ^uint64(0)) // read at max ts
		if err != nil {
			return err
		}
		if curTS != origTS {
			return ErrConflict
		}
	}

	// Allocate commit timestamp.
	t.commitTS = t.mgr.ts.Next()

	// Apply writes to the engine.
	writeSet := t.ws.WriteSet()
	for key, rowData := range writeSet {
		treeKey, pk := wsKeyToParts(key)
		if rowData == nil {
			// Delete.
			if err := t.mgr.engine.DeleteRow(treeKey, pk, t.commitTS); err != nil {
				return err
			}
		} else if t.ws.IsInserted(treeKey, pk) {
			// Insert.
			if err := t.mgr.engine.InsertRow(treeKey, pk, t.commitTS, rowData); err != nil {
				return err
			}
		} else {
			// Update.
			if err := t.mgr.engine.UpdateRow(treeKey, pk, t.commitTS, rowData); err != nil {
				return err
			}
		}
	}

	return nil
}

// Rollback discards the transaction.
func (t *Txn) Rollback() {
	t.finalized = true
}

// wsPKInRange extracts the PK bytes from a wsKey if it belongs to the given tree
// and the PK is in [start, end). Returns the PK and true if it matches.
func (t *Txn) wsPKInRange(wsKey string, treeKey string, start, end []byte) ([]byte, bool) {
	prefix := treeKey + "\x00"
	if len(wsKey) <= len(prefix) || wsKey[:len(prefix)] != prefix {
		return nil, false
	}
	pk := []byte(wsKey[len(prefix):])
	if CompareBytes(pk, start) < 0 || CompareBytes(pk, end) >= 0 {
		return nil, false
	}
	return pk, true
}

// wsKeyToTree extracts the treeKey from a wsKey.
func wsKeyToTree(key string) string {
	for i := 0; i < len(key); i++ {
		if key[i] == 0 {
			return key[:i]
		}
	}
	return key
}

// wsKeyToParts splits a wsKey into treeKey and pk bytes.
func wsKeyToParts(key string) (string, []byte) {
	for i := 0; i < len(key); i++ {
		if key[i] == 0 {
			return key[:i], []byte(key[i+1:])
		}
	}
	return key, nil
}

// CompareBytes compares two byte slices.
// Needed here since storage.compareKeys is unexported.
func CompareBytes(a, b []byte) int {
	minLen := len(a)
	if len(b) < minLen {
		minLen = len(b)
	}
	for i := 0; i < minLen; i++ {
		if a[i] < b[i] {
			return -1
		}
		if a[i] > b[i] {
			return 1
		}
	}
	if len(a) < len(b) {
		return -1
	}
	if len(a) > len(b) {
		return 1
	}
	return 0
}
