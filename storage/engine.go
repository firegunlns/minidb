package storage

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"lns.com/minidb/bptree"
	"lns.com/minidb/metrics"
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

	dirtyPKs map[string]map[string]struct{} // treeKey -> set of PK strings that have old versions
	dirtyMu  sync.Mutex
}

// OpenEngine creates or opens a StorageEngine backed by files in dataDir.
func OpenEngine(dataDir string, order, cacheSize int) (*StorageEngine, error) {
	e := &StorageEngine{
		dataDir:   dataDir,
		order:     order,
		cacheSize: cacheSize,
		trees:     make(map[string]*bptree.PersistentBPTree),
		dirtyPKs:  make(map[string]map[string]struct{}),
	}

	// Scan data directory for existing .db files and open them.
	// This ensures trees are loaded even when WAL is empty (clean shutdown).
	entries, err := os.ReadDir(dataDir)
	if err != nil {
		return nil, fmt.Errorf("read data dir: %w", err)
	}
	for _, ent := range entries {
		if ent.IsDir() || !strings.HasSuffix(ent.Name(), ".db") {
			continue
		}
		// Skip catalog files — they are managed by the catalog package.
		if strings.HasPrefix(ent.Name(), "__catalog_") {
			continue
		}
		treeKey := ent.Name()
		if _, exists := e.trees[treeKey]; !exists {
			path := filepath.Join(dataDir, treeKey)
			tree, err := bptree.OpenPersistentBPTree(path, order, cacheSize)
			if err != nil {
				return nil, fmt.Errorf("open tree %s: %w", treeKey, err)
			}
			e.trees[treeKey] = tree
		}
	}

	return e, nil
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

// markDirty records that a PK in the given tree has produced a recyclable old version.
func (e *StorageEngine) markDirty(treeKey string, pk string) {
	e.dirtyMu.Lock()
	if e.dirtyPKs[treeKey] == nil {
		e.dirtyPKs[treeKey] = make(map[string]struct{})
	}
	e.dirtyPKs[treeKey][pk] = struct{}{}
	e.dirtyMu.Unlock()
}

// readdDirty puts a PK back into the dirty set for the next GC pass.
func (e *StorageEngine) readdDirty(treeKey string, pk string) {
	e.dirtyMu.Lock()
	if e.dirtyPKs[treeKey] == nil {
		e.dirtyPKs[treeKey] = make(map[string]struct{})
	}
	e.dirtyPKs[treeKey][pk] = struct{}{}
	e.dirtyMu.Unlock()
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
	start := time.Now()
	tree := e.getTree(treeKey)
	if tree == nil {
		return nil, 0, fmt.Errorf("tree %q not open", treeKey)
	}
	metrics.TableScansTotal.WithLabelValues(treeKey, "get").Inc()
	scanStart, scanEnd := ScanRangeForPK(pk)
	kvs := tree.RangeScan(scanStart, scanEnd)
	for _, kv := range kvs {
		xmin, xmax, flags, rowData, err := DecodeMVCCValue(kv.Value)
		if err != nil {
			continue
		}
		if IsVisible(xmin, xmax, flags, readTS) {
			metrics.MVCCGetDuration.Observe(time.Since(start).Seconds())
			metrics.RowsReadTotal.Inc()
			metrics.TableRowsRead.WithLabelValues(treeKey).Inc()
			return rowData, xmin, nil
		}
	}
	metrics.MVCCGetDuration.Observe(time.Since(start).Seconds())
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
	if err := tree.Insert(vkey, mvccVal); err != nil {
		return err
	}
	e.markDirty(treeKey, string(pk))
	return nil
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
	if err := tree.Insert(vkey, mvccVal); err != nil {
		return err
	}
	e.markDirty(treeKey, string(pk))
	return nil
}

// ScanRange iterates over rows in [start, end) key range visible at readTS.
// The callback receives the primary key and row data for each visible row.
func (e *StorageEngine) ScanRange(treeKey string, start, end []byte, readTS uint64, fn func(pk, row []byte) bool) {
	scanStart := time.Now()
	tree := e.getTree(treeKey)
	if tree == nil {
		metrics.MVCCScanDuration.Observe(time.Since(scanStart).Seconds())
		return
	}
	metrics.TableScansTotal.WithLabelValues(treeKey, "scan").Inc()
	verScanStart := make([]byte, len(start)+8)
	copy(verScanStart, start)
	verScanEnd := make([]byte, len(end)+8)
	copy(verScanEnd, end)
	for i := len(end); i < len(verScanEnd); i++ {
		verScanEnd[i] = 0xFF
	}

	kvs := tree.RangeScan(verScanStart, verScanEnd)

	var prevPK []byte
	for _, kv := range kvs {
		pk := KeyPrefix(kv.Key)
		if prevPK != nil && bytes.Equal(pk, prevPK) {
			continue
		}
		xmin, xmax, flags, rowData, err := DecodeMVCCValue(kv.Value)
		if err != nil {
			continue
		}
		if IsVisible(xmin, xmax, flags, readTS) {
			prevPK = append(prevPK[:0], pk...)
			metrics.RowsReadTotal.Inc()
			metrics.TableRowsRead.WithLabelValues(treeKey).Inc()
			if !fn([]byte(pk), rowData) {
				break
			}
		}
	}
	metrics.MVCCScanDuration.Observe(time.Since(scanStart).Seconds())
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
	metrics.TableScansTotal.WithLabelValues(treeKey, "scanall").Inc()

	scanStart := make([]byte, len(start)+8)
	copy(scanStart, start)
	scanEnd := make([]byte, len(end)+8)
	copy(scanEnd, end)
	for i := len(end); i < len(scanEnd); i++ {
		scanEnd[i] = 0xFF
	}

	kvs := tree.RangeScan(scanStart, scanEnd)

	// Same PK-group optimisation as ScanRange: versions of each PK are
	// adjacent, newest first. Only decode the first version per PK.
	var prevPK []byte
	for _, kv := range kvs {
		pk := KeyPrefix(kv.Key)
		if prevPK != nil && bytes.Equal(pk, prevPK) {
			continue
		}
		prevPK = pk
		_, _, _, rowData, err := DecodeMVCCValue(kv.Value)
		if err != nil {
			continue
		}
		if !fn(pk, rowData) {
			break
		}
	}
}

// CountAll counts distinct PKs in [start, end) range without decoding MVCC values.
// Uses key-only traversal with PK dedup inline — no value copy, no key materialization.
func (e *StorageEngine) CountAll(treeKey string, start, end []byte) int64 {
	tree := e.getTree(treeKey)
	if tree == nil {
		return 0
	}
	metrics.TableScansTotal.WithLabelValues(treeKey, "count").Inc()

	scanStart := make([]byte, len(start)+8)
	copy(scanStart, start)
	scanEnd := make([]byte, len(end)+8)
	copy(scanEnd, end)
	for i := len(end); i < len(scanEnd); i++ {
		scanEnd[i] = 0xFF
	}

	var count int64
	var prevPK []byte
	tree.RangeScanFn(scanStart, scanEnd, func(key, _ []byte) bool {
		pk := KeyPrefix(key)
		if prevPK != nil && bytes.Equal(pk, prevPK) {
			return true // same PK, skip
		}
		prevPK = append(prevPK[:0], pk...)
		count++
		return true
	})
	return count
}

// --- Garbage collection ---

type versionInfo struct {
	key   []byte
	xmin  uint64
	xmax  uint64
	flags byte
}

// VacuumTree removes stale MVCC versions from the specified tree using a full scan.
// Deprecated: use vacuumDirtyPKs instead for targeted GC.
func (e *StorageEngine) VacuumTree(treeKey string, safeTS uint64, limit int) (int, error) {
	tree := e.getTree(treeKey)
	if tree == nil {
		return 0, nil
	}

	// Full range scan.
	scanStart := []byte{0x00}
	scanEnd := bytes.Repeat([]byte{0xFF}, 32)
	kvs := tree.RangeScan(scanStart, scanEnd)

	// Group by PK prefix, identify GC-eligible versions.
	var toDelete [][]byte
	var currentPK string
	var versions []versionInfo

	for _, kv := range kvs {
		if len(toDelete) >= limit {
			break
		}
		xmin, xmax, flags, _, err := DecodeMVCCValue(kv.Value)
		if err != nil {
			continue
		}

		pk := string(KeyPrefix(kv.Key))
		if pk != currentPK {
			if len(versions) > 0 {
				toDelete = append(toDelete, gcEligible(versions, safeTS)...)
			}
			versions = versions[:0]
			currentPK = pk
		}
		versions = append(versions, versionInfo{key: kv.Key, xmin: xmin, xmax: xmax, flags: flags})
	}
	if len(versions) > 0 && len(toDelete) < limit {
		toDelete = append(toDelete, gcEligible(versions, safeTS)...)
	}

	removed := 0
	for _, key := range toDelete {
		if removed >= limit {
			break
		}
		tree.Delete(key)
		removed++
	}
	return removed, nil
}

// vacuumDirtyPKs performs targeted GC on only the PKs that have been marked dirty.
// It swaps out the current dirty set, processes each PK individually, and re-adds
// any remaining PKs if the limit is exhausted.
func (e *StorageEngine) vacuumDirtyPKs(safeTS uint64, limit int) int {
	e.dirtyMu.Lock()
	dirty := e.dirtyPKs
	e.dirtyPKs = make(map[string]map[string]struct{})
	e.dirtyMu.Unlock()

	removed := 0
	for treeKey, pks := range dirty {
		tree := e.getTree(treeKey)
		if tree == nil {
			continue
		}
		for pkStr := range pks {
			if removed >= limit {
				e.readdDirty(treeKey, pkStr)
				continue
			}
			pk := []byte(pkStr)
			start, end := ScanRangeForPK(pk)
			kvs := tree.RangeScan(start, end)

			var versions []versionInfo
			for _, kv := range kvs {
				xmin, xmax, flags, _, err := DecodeMVCCValue(kv.Value)
				if err != nil {
					continue
				}
				versions = append(versions, versionInfo{key: kv.Key, xmin: xmin, xmax: xmax, flags: flags})
			}

			toDelete := gcEligible(versions, safeTS)
			for _, key := range toDelete {
				if removed >= limit {
					e.readdDirty(treeKey, pkStr)
					break
				}
				tree.Delete(key)
				removed++
			}
		}
	}
	return removed
}

// gcEligible returns the keys of versions that can be safely removed.
// versions are ordered newest-first (B+ tree ordering with ^commitTS).
func gcEligible(versions []versionInfo, safeTS uint64) [][]byte {
	if len(versions) <= 1 {
		return nil
	}

	var toDelete [][]byte
	kept := 0

	// Walk newest to oldest. A version is eligible if:
	// - superseded (xmax != 0 && xmax < safeTS), or
	// - old tombstone (flags & FlagDeleted && xmin < safeTS)
	for i := len(versions) - 1; i >= 0; i-- {
		v := versions[i]
		isSuperseded := v.xmax != 0 && v.xmax < safeTS
		isOldTombstone := v.flags&FlagDeleted != 0 && v.xmin < safeTS

		remaining := len(versions) - len(toDelete) - kept
		if remaining <= 1 {
			// Must keep at least one version.
			break
		}

		if isSuperseded || isOldTombstone {
			toDelete = append(toDelete, v.key)
		} else {
			kept++
		}
	}
	return toDelete
}

// RunGC performs garbage collection on dirty PKs only.
func (e *StorageEngine) RunGC(safeTS uint64) {
	start := time.Now()
	removed := e.vacuumDirtyPKs(safeTS, 500)
	metrics.GCDuration.Observe(time.Since(start).Seconds())
	metrics.GCPassesTotal.Inc()
	metrics.GCVersionsRemovedTotal.Add(float64(removed))
}

// RunFullGC repeatedly runs vacuumDirtyPKs until no more versions are removed.
// Intended for use after WAL recovery to clean up all accumulated old versions.
func (e *StorageEngine) RunFullGC(safeTS uint64) {
	start := time.Now()
	totalRemoved := 0
	for {
		removed := e.vacuumDirtyPKs(safeTS, 5000)
		if removed == 0 {
			break
		}
		totalRemoved += removed
	}
	if totalRemoved > 0 {
		metrics.GCDuration.Observe(time.Since(start).Seconds())
		metrics.GCPassesTotal.Inc()
		metrics.GCVersionsRemovedTotal.Add(float64(totalRemoved))
	}
}

func (e *StorageEngine) SyncTree(treeKey string) error {
	tree := e.getTree(treeKey)
	if tree == nil {
		return fmt.Errorf("tree %q not open", treeKey)
	}
	return tree.Sync()
}

func (e *StorageEngine) SyncAll() {
	e.mu.RLock()
	trees := make([]*bptree.PersistentBPTree, 0, len(e.trees))
	for _, tree := range e.trees {
		trees = append(trees, tree)
	}
	e.mu.RUnlock()
	for _, tree := range trees {
		tree.Sync()
	}
}
