package storage

import (
	"fmt"
	"os"
	"testing"
)

func TestEngineInsertAndGet(t *testing.T) {
	dir := t.TempDir()
	e, err := OpenEngine(dir, 64, 256)
	if err != nil {
		t.Fatal(err)
	}
	defer e.Close()

	treeKey := "testdb__users.db"
	if err := e.OpenTree(treeKey); err != nil {
		t.Fatal(err)
	}

	cols := []ColumnDef{
		{Name: "id", Type: ColTypeInt},
		{Name: "name", Type: ColTypeVarchar, Length: 20},
	}
	rowData := EncodeRow(cols, []any{int32(1), "Alice"})
	pk := EncodePrimaryKey(cols[:1], int32(1))

	if err := e.InsertRow(treeKey, pk, 100, rowData); err != nil {
		t.Fatal(err)
	}

	gotRow, gotTS, err := e.GetRow(treeKey, pk, 150)
	if err != nil {
		t.Fatal(err)
	}
	if gotRow == nil {
		t.Fatal("expected row, got nil")
	}
	if gotTS != 100 {
		t.Errorf("expected ts=100, got %d", gotTS)
	}
	vals, _ := DecodeRow(gotRow, cols)
	if vals[0].(int32) != 1 || vals[1].(string) != "Alice" {
		t.Errorf("unexpected row values: %v", vals)
	}
}

func TestEngineMultiVersion(t *testing.T) {
	dir := t.TempDir()
	e, _ := OpenEngine(dir, 64, 256)
	defer e.Close()

	treeKey := "testdb__users.db"
	e.OpenTree(treeKey)

	cols := []ColumnDef{
		{Name: "id", Type: ColTypeInt},
		{Name: "val", Type: ColTypeInt},
	}
	pk := EncodePrimaryKey(cols[:1], int32(1))

	// Insert at ts=10
	row1 := EncodeRow(cols, []any{int32(1), int32(100)})
	e.InsertRow(treeKey, pk, 10, row1)

	// Update at ts=20
	row2 := EncodeRow(cols, []any{int32(1), int32(200)})
	e.UpdateRow(treeKey, pk, 20, row2)

	// Read at ts=5 → not found
	r, _, _ := e.GetRow(treeKey, pk, 5)
	if r != nil {
		t.Error("should not be visible at ts=5")
	}

	// Read at ts=15 → old version
	r, ts, _ := e.GetRow(treeKey, pk, 15)
	if r == nil {
		t.Fatal("expected row at ts=15")
	}
	vals, _ := DecodeRow(r, cols)
	if vals[1].(int32) != 100 {
		t.Errorf("at ts=15: expected val=100, got %v", vals[1])
	}
	if ts != 10 {
		t.Errorf("at ts=15: expected ts=10, got %d", ts)
	}

	// Read at ts=25 → new version
	r, ts, _ = e.GetRow(treeKey, pk, 25)
	if r == nil {
		t.Fatal("expected row at ts=25")
	}
	vals, _ = DecodeRow(r, cols)
	if vals[1].(int32) != 200 {
		t.Errorf("at ts=25: expected val=200, got %v", vals[1])
	}
	if ts != 20 {
		t.Errorf("at ts=25: expected ts=20, got %d", ts)
	}
}

func TestEngineDelete(t *testing.T) {
	dir := t.TempDir()
	e, _ := OpenEngine(dir, 64, 256)
	defer e.Close()

	treeKey := "testdb__users.db"
	e.OpenTree(treeKey)

	cols := []ColumnDef{{Name: "id", Type: ColTypeInt}, {Name: "v", Type: ColTypeInt}}
	pk := EncodePrimaryKey(cols[:1], int32(1))
	row := EncodeRow(cols, []any{int32(1), int32(42)})

	e.InsertRow(treeKey, pk, 10, row)

	// Visible at ts=15
	r, _, _ := e.GetRow(treeKey, pk, 15)
	if r == nil {
		t.Fatal("should be visible before delete")
	}

	// Delete at ts=20
	e.DeleteRow(treeKey, pk, 20)

	// Not visible at ts=25
	r, _, _ = e.GetRow(treeKey, pk, 25)
	if r != nil {
		t.Error("should not be visible after delete")
	}

	// Still visible at ts=15
	r, _, _ = e.GetRow(treeKey, pk, 15)
	if r == nil {
		t.Error("should still be visible at ts=15")
	}
}

func TestEngineScanRange(t *testing.T) {
	dir := t.TempDir()
	e, _ := OpenEngine(dir, 64, 256)
	defer e.Close()

	treeKey := "testdb__items.db"
	e.OpenTree(treeKey)

	cols := []ColumnDef{{Name: "id", Type: ColTypeInt}, {Name: "v", Type: ColTypeInt}}

	// Insert 10 rows
	for i := int32(1); i <= 10; i++ {
		pk := EncodePrimaryKey(cols[:1], i)
		row := EncodeRow(cols, []any{i, i * 10})
		e.InsertRow(treeKey, pk, 100, row)
	}

	// Scan range [3, 7]
	start := EncodePrimaryKey(cols[:1], int32(3))
	end := EncodePrimaryKey(cols[:1], int32(7))
	var count int
	e.ScanRange(treeKey, start, end, 200, func(pk []byte, row []byte) bool {
		count++
		return true
	})
	if count != 5 {
		t.Errorf("expected 5 rows in range [3,7], got %d", count)
	}
}

func TestEnginePersistence(t *testing.T) {
	dir := t.TempDir()

	e1, _ := OpenEngine(dir, 64, 256)
	treeKey := "testdb__persist.db"
	e1.OpenTree(treeKey)
	cols := []ColumnDef{{Name: "id", Type: ColTypeInt}, {Name: "v", Type: ColTypeInt}}
	pk := EncodePrimaryKey(cols[:1], int32(1))
	e1.InsertRow(treeKey, pk, 10, EncodeRow(cols, []any{int32(1), int32(99)}))
	e1.Close()

	e2, _ := OpenEngine(dir, 64, 256)
	defer e2.Close()
	e2.OpenTree(treeKey)

	r, ts, err := e2.GetRow(treeKey, pk, 20)
	if err != nil {
		t.Fatal(err)
	}
	if r == nil {
		t.Fatal("row should persist after reopen")
	}
	if ts != 10 {
		t.Errorf("expected ts=10, got %d", ts)
	}
	vals, _ := DecodeRow(r, cols)
	if vals[1].(int32) != 99 {
		t.Errorf("expected val=99, got %v", vals[1])
	}
}

func TestEnginePersistenceWithoutWAL(t *testing.T) {
	dir := t.TempDir()

	// Phase 1: create engine, insert data, close (flushes to disk).
	e1, _ := OpenEngine(dir, 64, 256)
	treeKey := "testdb__walless.db"
	e1.OpenTree(treeKey)
	cols := []ColumnDef{{Name: "id", Type: ColTypeInt}, {Name: "v", Type: ColTypeInt}}

	for i := int32(1); i <= 5; i++ {
		pk := EncodePrimaryKey(cols[:1], i)
		e1.InsertRow(treeKey, pk, uint64(i*10), EncodeRow(cols, []any{i, i * 100}))
	}
	e1.Close()

	// Phase 2: simulate clean shutdown by truncating WAL, then reopen.
	// OpenEngine should load trees from .db files directly.
	walPath := dir + "/wal.log"
	if f, err := os.Create(walPath); err == nil {
		f.Close()
	}

	e2, err := OpenEngine(dir, 64, 256)
	if err != nil {
		t.Fatalf("OpenEngine after WAL truncation: %v", err)
	}
	defer e2.Close()

	// Trees should be auto-loaded — no need to call OpenTree.
	for i := int32(1); i <= 5; i++ {
		pk := EncodePrimaryKey(cols[:1], i)
		r, ts, err := e2.GetRow(treeKey, pk, uint64(i*10+5))
		if err != nil {
			t.Fatal(err)
		}
		if r == nil {
			t.Fatalf("row %d should persist after WAL truncation", i)
		}
		if ts != uint64(i*10) {
			t.Errorf("row %d: expected ts=%d, got %d", i, i*10, ts)
		}
		vals, _ := DecodeRow(r, cols)
		if vals[1].(int32) != i*100 {
			t.Errorf("row %d: expected val=%d, got %v", i, i*100, vals[1])
		}
	}
}

func TestEngineSecondaryIndex(t *testing.T) {
	dir := t.TempDir()
	e, _ := OpenEngine(dir, 64, 256)
	defer e.Close()

	dataTree := "testdb__users.db"
	idxTree := "testdb__users__idx__name.db"
	e.OpenTree(dataTree)
	e.OpenTree(idxTree)

	cols := []ColumnDef{
		{Name: "id", Type: ColTypeInt},
		{Name: "name", Type: ColTypeVarchar, Length: 20},
	}

	// Insert rows
	for i := int32(1); i <= 5; i++ {
		name := fmt.Sprintf("user_%d", i)
		pk := EncodePrimaryKey(cols[:1], i)
		row := EncodeRow(cols, []any{i, name})
		e.InsertRow(dataTree, pk, 100, row)

		// Insert into secondary index: key = (name, pk)
		idxKey := append(EncodeColumnValue(cols[1], name), pk...)
		e.InsertRaw(idxTree, idxKey, []byte{})
	}

	// Lookup via secondary index: find pk for "user_3"
	searchKey := append(EncodeColumnValue(cols[1], "user_3"), []byte{0x00}...)
	searchEnd := append(EncodeColumnValue(cols[1], "user_3"), []byte{0xFF, 0xFF, 0xFF, 0xFF, 0xFF}...)
	var found [][]byte
	e.ScanRaw(idxTree, searchKey, searchEnd, func(key, val []byte) bool {
		// Extract pk from end of key
		pk := key[len(key)-5:] // 1 byte tag + 4 bytes int32
		found = append(found, pk)
		return true
	})
	if len(found) == 0 {
		t.Fatal("expected to find entry in secondary index")
	}
}
