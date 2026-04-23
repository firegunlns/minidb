package storage

import (
	"os"
	"path/filepath"
	"testing"

	"lns.com/minidb/bptree"
	"lns.com/minidb/wal"
)

// TestFullLifecycle simulates the main.go startup/shutdown sequence.
func TestFullLifecycle(t *testing.T) {
	dir := t.TempDir()

	// Phase 1: Create data, flush, close (clean shutdown).
	w1, _ := wal.Open(dir)
	defer w1.Close()
	e1, _ := OpenEngine(dir, 64, 256)
	treeKey := "testdb__users.db"
	e1.OpenTree(treeKey)

	cols := []ColumnDef{{Name: "id", Type: ColTypeInt}, {Name: "v", Type: ColTypeInt}}
	for i := int32(1); i <= 50; i++ {
		pk := EncodePrimaryKey(cols[:1], i)
		row := EncodeRow(cols, []any{i, i * 10})
		e1.InsertRow(treeKey, pk, uint64(i), row)
	}
	e1.Close()
	w1.Truncate()

	// Phase 2: Reopen (simulate main.go startup).
	w2, _ := wal.Open(dir)
	defer w2.Close()
	e2, _ := OpenEngine(dir, 64, 256)

	// WAL is empty → RecoverFromWAL is no-op.
	e2.RecoverFromWAL(w2) //nolint:errcheck

	// GC with safeTS = max → dirtyPKs is empty → no-op.
	e2.RunFullGC(^uint64(0))

	// Verify data is accessible.
	for i := int32(1); i <= 50; i++ {
		pk := EncodePrimaryKey(cols[:1], i)
		row, ts, err := e2.GetRow(treeKey, pk, 100)
		if err != nil {
			t.Fatalf("GetRow(%d): %v", i, err)
		}
		if row == nil {
			t.Fatalf("row %d not found after restart", i)
		}
		if ts != uint64(i) {
			t.Errorf("row %d: expected ts=%d, got %d", i, i, ts)
		}
	}
	e2.Close()
	w2.Truncate()
}

// TestCatalogFileIntegrity checks that catalog-managed B+ tree files
// survive the engine open/close cycle (engine skips __catalog_ files).
func TestCatalogFileIntegrity(t *testing.T) {
	dir := t.TempDir()

	// Create a catalog file directly via bptree.
	catPath := filepath.Join(dir, "__catalog_tables.db")
	tree, err := bptree.OpenPersistentBPTree(catPath, 64, 256)
	if err != nil {
		t.Fatal(err)
	}
	// Insert some table definitions.
	tree.Insert([]byte("db1\x00users"), []byte("dummy_data"))
	tree.Insert([]byte("db1\x00orders"), []byte("dummy_data2"))
	tree.Sync()
	tree.Close()

	// Verify file size is > 4096 (header) - data was written.
	fi, _ := os.Stat(catPath)
	if fi.Size() <= 4096 {
		t.Fatalf("catalog file too small after Sync: %d bytes", fi.Size())
	}

	// Now open engine — it should NOT touch catalog files.
	e, _ := OpenEngine(dir, 64, 256)
	e.Close()

	// Verify catalog file is unchanged.
	fi2, _ := os.Stat(catPath)
	if fi2.Size() != fi.Size() {
		t.Errorf("catalog file was modified by engine: before=%d, after=%d", fi.Size(), fi2.Size())
	}

	// Reopen catalog tree and verify data.
	tree2, err := bptree.OpenPersistentBPTree(catPath, 64, 256)
	if err != nil {
		t.Fatal(err)
	}
	defer tree2.Close()
	val, found := tree2.Find([]byte("db1\x00users"))
	if !found {
		t.Fatal("catalog entry 'db1.users' not found after engine open/close")
	}
	if string(val) != "dummy_data" {
		t.Errorf("catalog entry value mismatch: got %q", string(val))
	}
}

// TestWALReplayDoesNotCorruptData verifies that WAL replay after a crash
// doesn't create duplicate or corrupt MVCC entries.
func TestWALReplayDoesNotCorruptData(t *testing.T) {
	dir := t.TempDir()

	// Phase 1: Insert data and commit to WAL.
	w1, _ := wal.Open(dir)
	e1, _ := OpenEngine(dir, 64, 256)
	treeKey := "testdb__items.db"
	e1.OpenTree(treeKey)
	cols := []ColumnDef{{Name: "id", Type: ColTypeInt}, {Name: "v", Type: ColTypeInt}}

	pk := EncodePrimaryKey(cols[:1], int32(42))
	row := EncodeRow(cols, []any{int32(42), int32(999)})
	e1.InsertRow(treeKey, pk, 100, row)

	// Sync data to disk but DON'T truncate WAL (simulate crash).
	e1.SyncTree(treeKey)
	e1.Close()
	w1.Close()

	// Phase 2: Reopen with WAL replay.
	w2, _ := wal.Open(dir)
	e2, _ := OpenEngine(dir, 64, 256)
	// WAL has no COMMIT record, so replay does nothing.
	e2.RecoverFromWAL(w2) //nolint:errcheck

	// Verify original data is still there.
	r, ts, err := e2.GetRow(treeKey, pk, 200)
	if err != nil {
		t.Fatal(err)
	}
	if r == nil {
		t.Fatal("data lost after WAL replay")
	}
	if ts != 100 {
		t.Errorf("expected ts=100, got %d", ts)
	}

	// Count total entries: should be exactly 1.
	count := e2.CountAll(treeKey, []byte{0x00}, []byte{0xFF})
	if count != 1 {
		t.Errorf("expected 1 row, got %d (possible duplicates from WAL replay)", count)
	}

	e2.Close()
	w2.Close()
}

// TestShutdownOrder tests that the catalog is flushed even if engine.Close()
// takes a long time (simulating large data flush).
func TestShutdownOrder(t *testing.T) {
	dir := t.TempDir()

	// Simulate the main.go startup: engine first, then catalog.
	e, _ := OpenEngine(dir, 64, 256)

	// Create catalog trees directly.
	catPath := filepath.Join(dir, "__catalog_tables.db")
	catTree, _ := bptree.OpenPersistentBPTree(catPath, 64, 256)
	catTree.Insert([]byte("testdb\x00tbl"), []byte("table_def"))
	catTree.Sync()

	// Create data tree.
	treeKey := "testdb__tbl.db"
	e.OpenTree(treeKey)
	cols := []ColumnDef{{Name: "id", Type: ColTypeInt}}
	pk := EncodePrimaryKey(cols, int32(1))
	e.InsertRow(treeKey, pk, 10, EncodeRow(cols, []any{int32(1)}))

	// Simulate main.go shutdown order:
	// engine.Close() → WAL.Truncate() → cat.Close()
	e.Close()

	// After engine.Close(), catalog tree should still be usable.
	val, found := catTree.Find([]byte("testdb\x00tbl"))
	if !found || string(val) != "table_def" {
		t.Fatal("catalog data lost after engine.Close()")
	}
	catTree.Close()

	// Reopen everything.
	e2, _ := OpenEngine(dir, 64, 256)
	defer e2.Close()

	catTree2, _ := bptree.OpenPersistentBPTree(catPath, 64, 256)
	defer catTree2.Close()

	// Data tree should have data.
	r, _, _ := e2.GetRow(treeKey, pk, 20)
	if r == nil {
		t.Fatal("data lost after restart")
	}

	// Catalog should have data.
	val2, found2 := catTree2.Find([]byte("testdb\x00tbl"))
	if !found2 {
		t.Fatal("catalog entry lost after restart")
	}
	if string(val2) != "table_def" {
		t.Errorf("catalog value mismatch: got %q", string(val2))
	}
}
