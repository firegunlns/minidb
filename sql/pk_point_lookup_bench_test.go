package sql

import (
	"fmt"
	"testing"

	"lns.com/minidb/catalog"
	"lns.com/minidb/storage"
	"lns.com/minidb/txn"
	"lns.com/minidb/wal"
)

// benchPKEnv sets up a table with a single INT PK and (N) pre-inserted rows.
func benchPKEnv(b *testing.B, numRows int) (*Executor, func()) {
	b.Helper()
	dir := b.TempDir()
	e, _ := storage.OpenEngine(dir, 64, 4096)
	ts := txn.NewTimestampOracle()
	w, _ := wal.Open(dir)
	mgr := txn.NewManager(e, ts, w, 0)
	cat, _ := catalog.Open(dir)
	exec := NewExecutor(e, mgr, cat, "benchdb")

	exec.Execute("CREATE DATABASE benchdb")
	exec.Execute("USE benchdb")
	exec.Execute("CREATE TABLE t (id INT PRIMARY KEY, name VARCHAR(32), val INT)")

	txn0 := mgr.Begin()
	td, _ := cat.GetTable("benchdb", "t")
	treeKey := td.DataFile()
	cols := td.Columns
	pkCols := td.PrimaryKeyColumns()
	for i := 0; i < numRows; i++ {
		pk := storage.EncodePrimaryKey(pkCols, int32(i))
		row := storage.EncodeRow(cols, []any{int32(i), fmt.Sprintf("user_%d", i), int32(i * 100)})
		txn0.Insert(treeKey, pk, row)
	}
	txn0.Commit()

	return exec, func() { w.Close(); e.Close(); cat.Close() }
}

// BenchmarkPKPointLookupCold: tiny table (1 row), measures per-query overhead.
func BenchmarkPKPointLookupCold(b *testing.B) {
	exec, cleanup := benchPKEnv(b, 1)
	defer cleanup()

	sql := "SELECT * FROM t WHERE id = 0"
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		exec.Execute(sql)
	}
}

// BenchmarkPKPointLookupHot: larger table (10K rows), cache warm.
func BenchmarkPKPointLookupHot(b *testing.B) {
	exec, cleanup := benchPKEnv(b, 10000)
	defer cleanup()

	// Warm cache.
	for i := 0; i < 10; i++ {
		exec.Execute(fmt.Sprintf("SELECT * FROM t WHERE id = %d", i))
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		exec.Execute(fmt.Sprintf("SELECT * FROM t WHERE id = %d", i%10000))
	}
}

// BenchmarkPKPointLookupStages measures each stage of a PK point SELECT
// independently using sub-benchmarks to eliminate timer overhead.
func BenchmarkPKPointLookupStages(b *testing.B) {
	dir := b.TempDir()
	e, _ := storage.OpenEngine(dir, 64, 4096)
	ts := txn.NewTimestampOracle()
	w, _ := wal.Open(dir)
	mgr := txn.NewManager(e, ts, w, 0)
	cat, _ := catalog.Open(dir)
	exec := NewExecutor(e, mgr, cat, "benchdb")
	exec.Execute("CREATE DATABASE benchdb")
	exec.Execute("USE benchdb")
	exec.Execute("CREATE TABLE t (id INT PRIMARY KEY, name VARCHAR(32), val INT)")

	td, _ := cat.GetTable("benchdb", "t")
	treeKey := td.DataFile()
	cols := td.Columns
	pkCols := td.PrimaryKeyColumns()
	{
		tx := mgr.Begin()
		for i := 0; i < 100; i++ {
			pk := storage.EncodePrimaryKey(pkCols, int32(i))
			row := storage.EncodeRow(cols, []any{int32(i), fmt.Sprintf("user_%d", i), int32(i * 100)})
			tx.Insert(treeKey, pk, row)
		}
		tx.Commit()
	}

	pk := storage.EncodePrimaryKey(pkCols, int32(42))
	sampleRow := storage.EncodeRow(cols, []any{int32(42), "user_42", int32(4200)})
	p := NewParser()
	readTS := ts.Current()

	b.Logf("")
	b.Logf("=== PK Point SELECT Stage Breakdown ===")

	// Stage 1: Parse
	b.Run("1_Parse", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			_, _ = p.Parse("SELECT * FROM t WHERE id = 42")
		}
	})

	// Stage 2: Begin + Rollback (auto-commit read txn lifecycle)
	b.Run("2_BeginRollback", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			tx := mgr.Begin()
			tx.Rollback()
		}
	})

	// Stage 3: Catalog lookup
	b.Run("3_CatalogLookup", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			td2, _ := cat.GetTable("benchdb", "t")
			_ = td2.DataFile()
		}
	})

	// Stage 4: Encode PK
	b.Run("4_EncodePK", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			_ = storage.EncodePrimaryKey(pkCols, int32(42))
		}
	})

	// Stage 5: txn.Get (workspace miss → MVCC GetRow → verCache hit)
	b.Run("5_TxnGet", func(b *testing.B) {
		tx := mgr.Begin()
		defer tx.Rollback()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_, _ = tx.Get(treeKey, cols, pk)
		}
	})

	// Stage 5a: Storage engine GetRow directly (MVCC layer: verCache → B+ tree scan)
	b.Run("5a_EngineGetRow", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_, _, _ = e.GetRow(treeKey, pk, readTS)
		}
	})

	// Stage 6: DecodeRow
	b.Run("6_DecodeRow", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			storage.DecodeRow(sampleRow, cols)
		}
	})

	// Full end-to-end
	b.Run("0_EndToEnd", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			exec.Execute("SELECT * FROM t WHERE id = 42")
		}
	})

	w.Close()
	e.Close()
	cat.Close()
}
