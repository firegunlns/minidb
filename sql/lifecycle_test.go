package sql

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"lns.com/minidb/catalog"
	"lns.com/minidb/storage"
	"lns.com/minidb/txn"
	"lns.com/minidb/wal"
)

// TestMainGoLifecycle simulates the exact main.go startup/shutdown sequence.
func TestMainGoLifecycle(t *testing.T) {
	dir := t.TempDir()

	// ---- Phase 1: Build ----
	// Simulates: OpenEngine → RecoverFromWAL → RunFullGC → catalog.Open
	w1, err := wal.Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	engine1, err := storage.OpenEngine(dir, 64, 4096)
	if err != nil {
		t.Fatal(err)
	}
	engine1.RecoverFromWAL(w1)
	engine1.RunFullGC(^uint64(0))

	cat1, err := catalog.Open(dir)
	if err != nil {
		t.Fatal(err)
	}

	ts1 := txn.OpenTimestampOracle(dir)
	mgr1 := txn.NewManager(engine1, ts1, w1, 0)
	exec1 := NewExecutor(engine1, mgr1, cat1, "")

	// Simulate: USE tpcc → CREATE DATABASE tpcc
	exec1.SetDatabase("tpcc")
	cat1.CreateDatabase("tpcc")

	// Simulate: CREATE TABLE
	exec1.Execute(`CREATE TABLE users (id INT PRIMARY KEY, name VARCHAR(20))`)

	// Verify catalog has the table.
	td, err := cat1.GetTable("tpcc", "users")
	if err != nil {
		t.Fatalf("GetTable after create: %v", err)
	}
	t.Logf("Table: %s, columns: %d", td.Name, len(td.Columns))

	// Verify catalog file has data (not just header).
	catPath := filepath.Join(dir, "__catalog_tables.db")
	fi, _ := os.Stat(catPath)
	t.Logf("Catalog file size after CreateTable: %d bytes", fi.Size())
	if fi.Size() <= 4096 {
		t.Errorf("catalog file too small after CreateTable: %d bytes (expected > 4096)", fi.Size())
	}

	// Simulate: INSERT data via transactions.
	for i := int32(1); i <= 10; i++ {
		exec1.Execute(fmt.Sprintf("INSERT INTO users VALUES (%d, 'user_%d')", i, i))
	}

	// Simulate: SELECT to verify data is there.
	res, err := exec1.Execute("SELECT id, name FROM users WHERE id = 5")
	if err != nil {
		t.Fatalf("SELECT before restart: %v", err)
	}
	sr := res.(*SelectResult)
	if len(sr.Rows) != 1 {
		t.Fatalf("expected 1 row, got %d", len(sr.Rows))
	}

	// Simulate: main.go shutdown: engine.Close → WAL.Truncate → cat.Close
	engine1.Close()
	w1.Truncate()
	cat1.Close()

	// ---- Phase 2: Restart ----
	t.Log("=== Phase 2: Restart ===")
	w2, err := wal.Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	engine2, err := storage.OpenEngine(dir, 64, 4096)
	if err != nil {
		t.Fatal(err)
	}
	engine2.RecoverFromWAL(w2)
	engine2.RunFullGC(^uint64(0))

	cat2, err := catalog.Open(dir)
	if err != nil {
		t.Fatal(err)
	}

	// Verify catalog persisted.
	td2, err := cat2.GetTable("tpcc", "users")
	if err != nil {
		t.Fatalf("GetTable after restart: %v", err)
	}
	if td2.Name != "users" {
		t.Errorf("expected table name 'users', got %q", td2.Name)
	}
	if len(td2.Columns) != 2 {
		t.Errorf("expected 2 columns, got %d", len(td2.Columns))
	}

	// Verify data persisted via engine.
	treeKey := td2.DataFile()
	cols := td2.PrimaryKeyColumns()
	pk := storage.EncodePrimaryKey(cols, int32(5))
	row, _, err := engine2.GetRow(treeKey, pk, ^uint64(0))
	if err != nil {
		t.Fatalf("GetRow after restart: %v", err)
	}
	if row == nil {
		t.Fatal("row with id=5 not found after restart")
	}
	vals, _ := storage.DecodeRow(row, td2.Columns)
	t.Logf("Row after restart: %v", vals)
	if vals[0].(int32) != 5 {
		t.Errorf("expected id=5, got %v", vals[0])
	}

	// Also verify via SELECT.
	ts2 := txn.OpenTimestampOracle(dir)
	mgr2 := txn.NewManager(engine2, ts2, w2, 0)
	exec2 := NewExecutor(engine2, mgr2, cat2, "tpcc")
	res2, err := exec2.Execute("SELECT id, name FROM users WHERE id = 5")
	if err != nil {
		t.Fatalf("SELECT after restart: %v", err)
	}
	sr2 := res2.(*SelectResult)
	if len(sr2.Rows) != 1 {
		t.Fatalf("expected 1 row after restart, got %d", len(sr2.Rows))
	}
	t.Logf("SELECT after restart: %v", sr2.Rows[0])

	// Clean shutdown.
	engine2.Close()
	w2.Truncate()
	cat2.Close()
}

// TestCatalogFilePreCreated tests that catalog files pre-created by NewPager
// (with empty headers) are properly updated by Sync.
func TestCatalogFilePreCreated(t *testing.T) {
	dir := t.TempDir()

	// Pre-create the catalog file (simulating what happens when the file
	// already exists from a previous run).
	catPath := filepath.Join(dir, "__catalog_tables.db")
	f, err := os.Create(catPath)
	if err != nil {
		t.Fatal(err)
	}
	f.Close()

	// Now open catalog, which will open the pre-created file.
	cat, err := catalog.Open(dir)
	if err != nil {
		t.Fatal(err)
	}

	// Create database and table.
	cat.CreateDatabase("testdb")
	cat.CreateTable(&catalog.TableDef{
		Database: "testdb",
		Name:     "t1",
		Columns: []storage.ColumnDef{
			{Name: "id", Type: storage.ColTypeInt},
		},
		PKCols: []int{0},
	})
	cat.Close()

	// Verify file size > 4096 (data was written).
	fi, _ := os.Stat(catPath)
	if fi.Size() <= 4096 {
		t.Fatalf("catalog file too small after CreateTable+Close: %d bytes", fi.Size())
	}

	// Reopen and verify.
	cat2, err := catalog.Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer cat2.Close()
	td, err := cat2.GetTable("testdb", "t1")
	if err != nil {
		t.Fatalf("GetTable after reopen: %v", err)
	}
	if td.Name != "t1" {
		t.Errorf("expected t1, got %q", td.Name)
	}
}

// TestEngineDoesNotTouchCatalogFiles verifies that engine operations
// don't accidentally modify catalog files.
func TestEngineDoesNotTouchCatalogFiles(t *testing.T) {
	dir := t.TempDir()

	// Pre-create catalog with data.
	cat, err := catalog.Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	cat.CreateDatabase("db1")
	cat.CreateTable(&catalog.TableDef{
		Database: "db1",
		Name:     "orders",
		Columns: []storage.ColumnDef{
			{Name: "id", Type: storage.ColTypeInt},
			{Name: "val", Type: storage.ColTypeInt},
		},
		PKCols:  []int{0},
		Indexes: []catalog.IndexDef{{Name: "idx_val", Columns: []string{"val"}}},
	})
	cat.Close()

	// Record catalog file sizes.
	catTablesPath := filepath.Join(dir, "__catalog_tables.db")
	catDbsPath := filepath.Join(dir, "__catalog_dbs.db")
	catAutoIncPath := filepath.Join(dir, "__catalog_autoinc.db")
	sizeBeforeTables, _ := os.Stat(catTablesPath)
	sizeBeforeDbs, _ := os.Stat(catDbsPath)
	sizeBeforeAutoInc, _ := os.Stat(catAutoIncPath)

	// Open engine, do heavy operations.
	engine, err := storage.OpenEngine(dir, 64, 256)
	if err != nil {
		t.Fatal(err)
	}

	// Open data tree and insert lots of data.
	treeKey := "db1__orders.db"
	engine.OpenTree(treeKey)
	cols := []storage.ColumnDef{{Name: "id", Type: storage.ColTypeInt}, {Name: "val", Type: storage.ColTypeInt}}
	for i := int32(0); i < 200; i++ {
		pk := storage.EncodePrimaryKey(cols[:1], i)
		row := storage.EncodeRow(cols, []any{i, i * 10})
		engine.InsertRow(treeKey, pk, uint64(i+1), row)
	}

	// Also open and use an index tree.
	idxTreeKey := "db1__orders__idx__idx_val.db"
	engine.OpenTree(idxTreeKey)
	for i := int32(0); i < 200; i++ {
		idxKey := append(storage.EncodeColumnValue(cols[1], int32(i*10)), storage.EncodePrimaryKey(cols[:1], i)...)
		engine.InsertRaw(idxTreeKey, idxKey, []byte{})
	}

	// Sync and close engine.
	engine.SyncAll()
	engine.Close()

	// Verify catalog files are unchanged.
	sizeAfterTables, _ := os.Stat(catTablesPath)
	sizeAfterDbs, _ := os.Stat(catDbsPath)
	sizeAfterAutoInc, _ := os.Stat(catAutoIncPath)

	if sizeAfterTables.Size() != sizeBeforeTables.Size() {
		t.Errorf("__catalog_tables.db was modified by engine: before=%d, after=%d",
			sizeBeforeTables.Size(), sizeAfterTables.Size())
	}
	if sizeAfterDbs.Size() != sizeBeforeDbs.Size() {
		t.Errorf("__catalog_dbs.db was modified by engine: before=%d, after=%d",
			sizeBeforeDbs.Size(), sizeAfterDbs.Size())
	}
	if sizeAfterAutoInc.Size() != sizeBeforeAutoInc.Size() {
		t.Errorf("__catalog_autoinc.db was modified by engine: before=%d, after=%d",
			sizeBeforeAutoInc.Size(), sizeAfterAutoInc.Size())
	}

	// Reopen catalog and verify data.
	cat2, err := catalog.Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer cat2.Close()
	td, err := cat2.GetTable("db1", "orders")
	if err != nil {
		t.Fatalf("catalog corrupted after engine operations: %v", err)
	}
	if len(td.Indexes) != 1 {
		t.Errorf("expected 1 index, got %d", len(td.Indexes))
	}
}
