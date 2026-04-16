package storage

import (
	"fmt"
	"path/filepath"
	"sync"

	"lns.com/minidb/bptree"
	"lns.com/minidb/wal"
)

// StorageEngine manages multiple B+ tree instances for tables and indexes.
// Each table and secondary index gets its own B+ tree file.
type StorageEngine struct {
	mu        sync.RWMutex
	dataDir   string
	order     int
	cacheSize int
	trees     map[string]*bptree.PersistentBPTree
}

// OpenEngine creates or opens a StorageEngine backed by files in dataDir.
func OpenEngine(dataDir string, order, cacheSize int) (*StorageEngine, error) {
	return &StorageEngine{
		dataDir:   dataDir,
		order:     order,
		cacheSize: cacheSize,
		trees:     make(map[string]*bptree.PersistentBPTree),
	}, nil
}

// RecoverFromWAL replays committed transactions from the WAL.
func (e *StorageEngine) RecoverFromWAL(w *wal.WAL) error {
	records, err := w.ReadAll()
	if err != nil {
		return fmt.Errorf("read WAL: %w", err)
	}

	committed := make(map[uint64]bool)
	commitTSMap := make(map[uint64]uint64)
	for _, r := range records {
		if r.Type == wal.RecCommit {
			committed[r.TxnTS] = true
			commitTSMap[r.TxnTS] = r.CommitTS
		} else if r.Type == wal.RecAbort {
			delete(committed, r.TxnTS)
		}
	}

	for _, r := range records {
		if r.Type == wal.RecCommit || r.Type == wal.RecAbort || r.Type == wal.RecCheckpoint {
			continue
		}
		if !committed[r.TxnTS] {
			continue
		}

		if err := e.OpenTree(r.TreeKey); err != nil {
			return err
		}

		commitTS := commitTSMap[r.TxnTS]
		switch r.Type {
		case wal.RecInsert:
			if err := e.InsertRow(r.TreeKey, r.PK, commitTS, r.RowData); err != nil {
				return err
			}
		case wal.RecUpdate:
			if err := e.UpdateRow(r.TreeKey, r.PK, commitTS, r.RowData); err != nil {
				return err
			}
		case wal.RecDelete:
			if err := e.DeleteRow(r.TreeKey, r.PK, commitTS); err != nil {
				return err
			}
		}
	}

	return nil
}

// Close closes all open B+ trees.
func (e *StorageEngine) Close() {
	e.mu.Lock()
	defer e.mu.Unlock()
	for _, tree := range e.trees {
		tree.Close()
	}
	e.trees = nil
}

// OpenTree opens or creates a B+ tree for the given treeKey.
// treeKey is typically "db__table.db" or "db__table__idx__name.db".
func (e *StorageEngine) OpenTree(treeKey string) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	if _, ok := e.trees[treeKey]; ok {
		return nil
	}
	path := filepath.Join(e.dataDir, treeKey)
	tree, err := bptree.OpenPersistentBPTree(path, e.order, e.cacheSize)
	if err != nil {
		return fmt.Errorf("open tree %s: %w", treeKey, err)
	}
	e.trees[treeKey] = tree
	return nil
}

func (e *StorageEngine) getTree(treeKey string) *bptree.PersistentBPTree {
	e.mu.RLock()
	defer e.mu.RUnlock()
	return e.trees[treeKey]
}

// --- MVCC row operations ---

// InsertRow inserts a new row version at the given commit timestamp.
func (e *StorageEngine) InsertRow(treeKey string, pk []byte, commitTS uint64, rowData []byte) error {
	tree := e.getTree(treeKey)
	if tree == nil {
		return fmt.Errorf("tree %q not open", treeKey)
	}
	vkey := VersionKey(pk, commitTS)
	mvccVal := EncodeMVCCValue(commitTS, 0, 0, rowData)
	return tree.Insert(vkey, mvccVal)
}

// GetRow retrieves the visible version of a row at the given read timestamp.
// Returns the row data, the commit timestamp of that version, or nil if not visible.
func (e *StorageEngine) GetRow(treeKey string, pk []byte, readTS uint64) ([]byte, uint64, error) {
	tree := e.getTree(treeKey)
	if tree == nil {
		return nil, 0, fmt.Errorf("tree %q not open", treeKey)
	}
	start, end := ScanRangeForPK(pk)
	kvs := tree.RangeScan(start, end)
	for _, kv := range kvs {
		xmin, xmax, flags, rowData, err := DecodeMVCCValue(kv.Value)
		if err != nil {
			continue
		}
		if IsVisible(xmin, xmax, flags, readTS) {
			return rowData, xmin, nil
		}
	}
	return nil, 0, nil
}

// UpdateRow inserts a new version and marks the old version as superseded.
// oldRowData is used to find the old version's value for setting xmax.
func (e *StorageEngine) UpdateRow(treeKey string, pk []byte, commitTS uint64, oldRowData []byte) error {
	tree := e.getTree(treeKey)
	if tree == nil {
		return fmt.Errorf("tree %q not open", treeKey)
	}
	start, end := ScanRangeForPK(pk)
	kvs := tree.RangeScan(start, end)

	// Find the current visible version and update its xmax.
	for _, kv := range kvs {
		xmin, xmax, flags, rowData, err := DecodeMVCCValue(kv.Value)
		if err != nil {
			continue
		}
		if xmax != 0 || flags&FlagDeleted != 0 {
			continue // already superseded or deleted
		}
		// Update old version: set xmax = commitTS
		newMvccVal := EncodeMVCCValue(xmin, commitTS, flags, rowData)
		if err := tree.Insert(kv.Key, newMvccVal); err != nil {
			return err
		}
		break
	}

	// Insert new version.
	vkey := VersionKey(pk, commitTS)
	mvccVal := EncodeMVCCValue(commitTS, 0, 0, oldRowData)
	return tree.Insert(vkey, mvccVal)
}

// DeleteRow marks a row as deleted by inserting a tombstone and setting xmax on the old version.
func (e *StorageEngine) DeleteRow(treeKey string, pk []byte, commitTS uint64) error {
	tree := e.getTree(treeKey)
	if tree == nil {
		return fmt.Errorf("tree %q not open", treeKey)
	}
	start, end := ScanRangeForPK(pk)
	kvs := tree.RangeScan(start, end)

	// Find the current visible version and mark it with xmax.
	for _, kv := range kvs {
		xmin, xmax, flags, rowData, err := DecodeMVCCValue(kv.Value)
		if err != nil {
			continue
		}
		if xmax != 0 || flags&FlagDeleted != 0 {
			continue
		}
		// Set xmax on old version.
		newMvccVal := EncodeMVCCValue(xmin, commitTS, flags, rowData)
		if err := tree.Insert(kv.Key, newMvccVal); err != nil {
			return err
		}
		break
	}

	// Insert tombstone.
	vkey := VersionKey(pk, commitTS)
	mvccVal := EncodeMVCCValue(commitTS, 0, FlagDeleted, nil)
	return tree.Insert(vkey, mvccVal)
}

// ScanRange iterates over rows in [start, end) key range visible at readTS.
// The callback receives the primary key and row data for each visible row.
func (e *StorageEngine) ScanRange(treeKey string, start, end []byte, readTS uint64, fn func(pk, row []byte) bool) {
	tree := e.getTree(treeKey)
	if tree == nil {
		return
	}
	// We need to scan the raw versioned keys and filter by visibility.
	// Expand start/end to cover all versions.
	// start/end are raw PK ranges, so we need versioned ranges.
	// For the start PK, the versioned start is the PK itself (with 0x00 suffix = newest).
	// For the end PK, we need the PK + 0xFF...FF suffix.
	scanStart := make([]byte, len(start)+8)
	copy(scanStart, start)
	scanEnd := make([]byte, len(end)+8)
	copy(scanEnd, end)
	for i := len(end); i < len(scanEnd); i++ {
		scanEnd[i] = 0xFF
	}

	kvs := tree.RangeScan(scanStart, scanEnd)

	// Group by PK prefix, return first visible version for each.
	seen := make(map[string]bool)
	for _, kv := range kvs {
		pkPrefix := string(KeyPrefix(kv.Key))
		if seen[pkPrefix] {
			continue
		}
		xmin, xmax, flags, rowData, err := DecodeMVCCValue(kv.Value)
		if err != nil {
			continue
		}
		if IsVisible(xmin, xmax, flags, readTS) {
			seen[pkPrefix] = true
			if !fn([]byte(pkPrefix), rowData) {
				break
			}
		}
	}
}

// --- Raw operations (for secondary indexes) ---

// InsertRaw inserts a raw key-value pair without MVCC encoding.
func (e *StorageEngine) InsertRaw(treeKey string, key, value []byte) error {
	tree := e.getTree(treeKey)
	if tree == nil {
		return fmt.Errorf("tree %q not open", treeKey)
	}
	return tree.Insert(key, value)
}

// ScanRaw iterates over raw key-value pairs in [start, end) range.
func (e *StorageEngine) ScanRaw(treeKey string, start, end []byte, fn func(key, value []byte) bool) {
	tree := e.getTree(treeKey)
	if tree == nil {
		return
	}
	kvs := tree.RangeScan(start, end)
	for _, kv := range kvs {
		if !fn(kv.Key, kv.Value) {
			break
		}
	}
}

// ScanAll iterates over all rows in [start, end) range, returning the latest version of each row
// regardless of MVCC visibility. This is used for aggregate queries that need to count all data.
func (e *StorageEngine) ScanAll(treeKey string, start, end []byte, fn func(pk, rowData []byte) bool) {
	tree := e.getTree(treeKey)
	if tree == nil {
		return
	}

	scanStart := make([]byte, len(start)+8)
	copy(scanStart, start)
	scanEnd := make([]byte, len(end)+8)
	copy(scanEnd, end)
	for i := len(end); i < len(scanEnd); i++ {
		scanEnd[i] = 0xFF
	}

	kvs := tree.RangeScan(scanStart, scanEnd)

	seen := make(map[string]bool)
	for _, kv := range kvs {
		pkPrefix := string(KeyPrefix(kv.Key))
		if seen[pkPrefix] {
			continue
		}
		_, _, _, rowData, err := DecodeMVCCValue(kv.Value)
		if err != nil {
			continue
		}
		seen[pkPrefix] = true
		if !fn([]byte(pkPrefix), rowData) {
			break
		}
	}
}
