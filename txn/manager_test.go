package txn

import (
	"testing"

	"lns.com/bptree/storage"
)

func setupEngine(t *testing.T) (*storage.StorageEngine, *TimestampOracle, func()) {
	t.Helper()
	dir := t.TempDir()
	e, err := storage.OpenEngine(dir, 64, 256)
	if err != nil {
		t.Fatal(err)
	}
	ts := NewTimestampOracle()
	treeKey := "testdb__t.db"
	if err := e.OpenTree(treeKey); err != nil {
		t.Fatal(err)
	}
	return e, ts, func() { e.Close() }
}

func TestTxnBeginCommit(t *testing.T) {
	e, ts, cleanup := setupEngine(t)
	defer cleanup()

	mgr := NewManager(e, ts)
	txn := mgr.Begin()

	cols := []storage.ColumnDef{{Name: "id", Type: storage.ColTypeInt}, {Name: "v", Type: storage.ColTypeInt}}
	treeKey := "testdb__t.db"
	pk := storage.EncodePrimaryKey(cols[:1], int32(1))
	row := storage.EncodeRow(cols, []interface{}{int32(1), int32(42)})

	if err := txn.Insert(treeKey, pk, row); err != nil {
		t.Fatal(err)
	}

	// Before commit, read-your-writes should work within the txn.
	gotRow, err := txn.Get(treeKey, cols, pk)
	if err != nil {
		t.Fatal(err)
	}
	if gotRow == nil {
		t.Fatal("read-your-writes: expected row in workspace")
	}

	if err := txn.Commit(); err != nil {
		t.Fatal(err)
	}

	// After commit, the row should be visible via a new txn.
	txn2 := mgr.Begin()
	defer txn2.Rollback()
	gotRow, err = txn2.Get(treeKey, cols, pk)
	if err != nil {
		t.Fatal(err)
	}
	if gotRow == nil {
		t.Fatal("expected row after commit")
	}
	vals, _ := storage.DecodeRow(gotRow, cols)
	if vals[1].(int32) != 42 {
		t.Errorf("expected val=42, got %v", vals[1])
	}
}

func TestTxnRollback(t *testing.T) {
	e, ts, cleanup := setupEngine(t)
	defer cleanup()

	mgr := NewManager(e, ts)
	txn := mgr.Begin()

	cols := []storage.ColumnDef{{Name: "id", Type: storage.ColTypeInt}, {Name: "v", Type: storage.ColTypeInt}}
	treeKey := "testdb__t.db"
	pk := storage.EncodePrimaryKey(cols[:1], int32(1))
	row := storage.EncodeRow(cols, []interface{}{int32(1), int32(42)})

	txn.Insert(treeKey, pk, row)
	txn.Rollback()

	// Row should not be visible.
	txn2 := mgr.Begin()
	defer txn2.Rollback()
	gotRow, _ := txn2.Get(treeKey, cols, pk)
	if gotRow != nil {
		t.Error("rolled back txn should not be visible")
	}
}

func TestTxnReadYourWrites(t *testing.T) {
	e, ts, cleanup := setupEngine(t)
	defer cleanup()

	mgr := NewManager(e, ts)
	treeKey := "testdb__t.db"
	cols := []storage.ColumnDef{{Name: "id", Type: storage.ColTypeInt}, {Name: "v", Type: storage.ColTypeInt}}

	// Insert a row first.
	txn0 := mgr.Begin()
	pk := storage.EncodePrimaryKey(cols[:1], int32(1))
	txn0.Insert(treeKey, pk, storage.EncodeRow(cols, []interface{}{int32(1), int32(10)}))
	txn0.Commit()

	// Update in a new txn and verify read-your-writes.
	txn1 := mgr.Begin()
	newRow := storage.EncodeRow(cols, []interface{}{int32(1), int32(99)})
	txn1.Update(treeKey, cols, pk, newRow)

	gotRow, _ := txn1.Get(treeKey, cols, pk)
	if gotRow == nil {
		t.Fatal("read-your-writes after update: expected row")
	}
	vals, _ := storage.DecodeRow(gotRow, cols)
	if vals[1].(int32) != 99 {
		t.Errorf("read-your-writes: expected val=99, got %v", vals[1])
	}
	txn1.Commit()
}

func TestTxnDelete(t *testing.T) {
	e, ts, cleanup := setupEngine(t)
	defer cleanup()

	mgr := NewManager(e, ts)
	treeKey := "testdb__t.db"
	cols := []storage.ColumnDef{{Name: "id", Type: storage.ColTypeInt}, {Name: "v", Type: storage.ColTypeInt}}

	// Insert a row.
	txn0 := mgr.Begin()
	pk := storage.EncodePrimaryKey(cols[:1], int32(1))
	txn0.Insert(treeKey, pk, storage.EncodeRow(cols, []interface{}{int32(1), int32(10)}))
	txn0.Commit()

	// Delete in a new txn.
	txn1 := mgr.Begin()
	txn1.Delete(treeKey, cols, pk)

	// Not visible within the deleting txn.
	gotRow, _ := txn1.Get(treeKey, cols, pk)
	if gotRow != nil {
		t.Error("row should not be visible after delete in same txn")
	}
	txn1.Commit()

	// Not visible after commit.
	txn2 := mgr.Begin()
	defer txn2.Rollback()
	gotRow, _ = txn2.Get(treeKey, cols, pk)
	if gotRow != nil {
		t.Error("row should not be visible after delete committed")
	}
}

func TestTxnSnapshotIsolation(t *testing.T) {
	e, ts, cleanup := setupEngine(t)
	defer cleanup()

	mgr := NewManager(e, ts)
	treeKey := "testdb__t.db"
	cols := []storage.ColumnDef{{Name: "id", Type: storage.ColTypeInt}, {Name: "v", Type: storage.ColTypeInt}}

	// Insert a row.
	txn0 := mgr.Begin()
	pk := storage.EncodePrimaryKey(cols[:1], int32(1))
	txn0.Insert(treeKey, pk, storage.EncodeRow(cols, []interface{}{int32(1), int32(10)}))
	txn0.Commit()

	// Start txn1, then update the row in txn2 and commit.
	txn1 := mgr.Begin()
	defer txn1.Rollback()

	txn2 := mgr.Begin()
	newRow := storage.EncodeRow(cols, []interface{}{int32(1), int32(99)})
	txn2.Update(treeKey, cols, pk, newRow)
	txn2.Commit()

	// txn1 should still see the old value (snapshot isolation).
	gotRow, _ := txn1.Get(treeKey, cols, pk)
	if gotRow == nil {
		t.Fatal("txn1 should see row from its snapshot")
	}
	vals, _ := storage.DecodeRow(gotRow, cols)
	if vals[1].(int32) != 10 {
		t.Errorf("snapshot isolation: expected val=10, got %v", vals[1])
	}
}

func TestTxnConflictDetection(t *testing.T) {
	e, ts, cleanup := setupEngine(t)
	defer cleanup()

	mgr := NewManager(e, ts)
	treeKey := "testdb__t.db"
	cols := []storage.ColumnDef{{Name: "id", Type: storage.ColTypeInt}, {Name: "v", Type: storage.ColTypeInt}}

	// Insert a row.
	txn0 := mgr.Begin()
	pk := storage.EncodePrimaryKey(cols[:1], int32(1))
	txn0.Insert(treeKey, pk, storage.EncodeRow(cols, []interface{}{int32(1), int32(10)}))
	txn0.Commit()

	// txn1 reads the row.
	txn1 := mgr.Begin()
	txn1.Get(treeKey, cols, pk)

	// txn2 updates and commits.
	txn2 := mgr.Begin()
	txn2.Update(treeKey, cols, pk, storage.EncodeRow(cols, []interface{}{int32(1), int32(99)}))
	txn2.Commit()

	// txn1 commit should fail due to conflict.
	err := txn1.Commit()
	if err == nil {
		t.Error("expected conflict error on commit")
	}
}

func TestTxnScan(t *testing.T) {
	e, ts, cleanup := setupEngine(t)
	defer cleanup()

	mgr := NewManager(e, ts)
	treeKey := "testdb__t.db"
	cols := []storage.ColumnDef{{Name: "id", Type: storage.ColTypeInt}, {Name: "v", Type: storage.ColTypeInt}}

	// Insert 5 rows.
	txn0 := mgr.Begin()
	for i := int32(1); i <= 5; i++ {
		pk := storage.EncodePrimaryKey(cols[:1], i)
		row := storage.EncodeRow(cols, []interface{}{i, i * 10})
		txn0.Insert(treeKey, pk, row)
	}
	txn0.Commit()

	// Scan in a new txn.
	txn1 := mgr.Begin()
	defer txn1.Rollback()
	start := storage.EncodePrimaryKey(cols[:1], int32(2))
	end := storage.EncodePrimaryKey(cols[:1], int32(4))
	var count int
	txn1.Scan(treeKey, cols, start, end, func(pk, row []byte) bool {
		count++
		return true
	})
	if count != 3 {
		t.Errorf("expected 3 rows in range [2,4], got %d", count)
	}
}
