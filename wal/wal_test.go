package wal

import (
	"testing"
)

func TestWALAppendAndRead(t *testing.T) {
	dir := t.TempDir()
	w, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()

	// Start a txn at ts=1.
	ts1 := w.Append(InsertRecord("tree1", []byte("pk1"), []byte("row1")))
	// Second insert in same txn — must reference the same txnTS.
	rec2 := InsertRecord("tree2", []byte("pk2"), []byte("row2"))
	rec2.TxnTS = ts1
	w.Append(rec2)

	commitTS := w.Append(CommitRecord(ts1))

	// Read back all records.
	records, err := w.ReadAll()
	if err != nil {
		t.Fatal(err)
	}
	if len(records) != 3 {
		t.Fatalf("expected 3 records, got %d", len(records))
	}

	if records[0].Type != RecInsert {
		t.Errorf("expected RecInsert, got %d", records[0].Type)
	}
	if string(records[0].PK) != "pk1" {
		t.Errorf("expected pk1, got %s", records[0].PK)
	}
	if records[0].TxnTS != ts1 {
		t.Errorf("expected txnTS=%d, got %d", ts1, records[0].TxnTS)
	}
	if records[1].Type != RecInsert {
		t.Errorf("expected RecInsert, got %d", records[1].Type)
	}
	if records[1].TxnTS != ts1 {
		t.Errorf("expected txnTS=%d for second record, got %d", ts1, records[1].TxnTS)
	}
	if records[2].Type != RecCommit {
		t.Errorf("expected RecCommit, got %d", records[2].Type)
	}
	if records[2].TxnTS != ts1 {
		t.Errorf("expected txnTS=%d, got %d", ts1, records[2].TxnTS)
	}
	if records[2].CommitTS != commitTS {
		t.Errorf("expected commitTS=%d, got %d", commitTS, records[2].CommitTS)
	}
}

func TestWALRecoveryBasic(t *testing.T) {
	dir := t.TempDir()

	// Write some records and close.
	w, _ := Open(dir)
	ts1 := w.Append(InsertRecord("t1", []byte("pk1"), []byte("row1")))
	rec2 := InsertRecord("t1", []byte("pk2"), []byte("row2"))
	rec2.TxnTS = ts1
	w.Append(rec2)
	w.Append(CommitRecord(ts1))

	ts2 := w.Append(InsertRecord("t1", []byte("pk3"), []byte("row3")))
	// ts2 not committed (crash simulation)
	_ = ts2

	w.Close()

	// Reopen and recover.
	w2, _ := Open(dir)
	defer w2.Close()

	recs, _ := w2.ReadAll()
	if len(recs) != 4 {
		t.Fatalf("expected 4 records, got %d", len(recs))
	}

	committed, aborted := RecoverCommitted(recs)
	if len(committed) != 1 {
		t.Fatalf("expected 1 committed txn, got %d", len(committed))
	}
	if len(aborted) != 1 {
		t.Fatalf("expected 1 aborted txn, got %d", len(aborted))
	}
	if committed[0] != ts1 {
		t.Errorf("committed txn should have ts=%d", ts1)
	}
	if aborted[0] != ts2 {
		t.Errorf("aborted txn should have ts=%d", ts2)
	}
}

func TestWALUpdateDelete(t *testing.T) {
	dir := t.TempDir()
	w, _ := Open(dir)

	ts := w.Append(UpdateRecord("t1", []byte("pk1"), []byte("old"), []byte("new")))
	rec2 := DeleteRecord("t1", []byte("pk2"), []byte("oldrow"))
	rec2.TxnTS = ts
	w.Append(rec2)
	w.Append(CommitRecord(ts))
	w.Close()

	w2, _ := Open(dir)
	defer w2.Close()

	recs, _ := w2.ReadAll()
	if len(recs) != 3 {
		t.Fatalf("expected 3 records, got %d", len(recs))
	}
	if recs[0].Type != RecUpdate {
		t.Errorf("expected RecUpdate, got %d", recs[0].Type)
	}
	if recs[1].Type != RecDelete {
		t.Errorf("expected RecDelete, got %d", recs[1].Type)
	}
	if string(recs[0].OldData) != "old" {
		t.Errorf("expected old='old', got %s", recs[0].OldData)
	}
	if string(recs[0].RowData) != "new" {
		t.Errorf("expected new='new', got %s", recs[0].RowData)
	}
}

func TestWALMultipleTxns(t *testing.T) {
	dir := t.TempDir()
	w, _ := Open(dir)

	ts1 := w.Append(InsertRecord("t1", []byte("a"), []byte("va")))
	w.Append(CommitRecord(ts1))

	ts2 := w.Append(InsertRecord("t1", []byte("b"), []byte("vb")))
	ts3 := w.Append(InsertRecord("t1", []byte("c"), []byte("vc")))
	w.Append(CommitRecord(ts2))
	w.Append(CommitRecord(ts3))

	// Uncommitted.
	ts4 := w.Append(InsertRecord("t1", []byte("d"), []byte("vd")))
	_ = ts4

	w.Close()

	w2, _ := Open(dir)
	defer w2.Close()

	recs, _ := w2.ReadAll()
	committed, aborted := RecoverCommitted(recs)
	if len(committed) != 3 {
		t.Errorf("expected 3 committed, got %d", len(committed))
	}
	if len(aborted) != 1 {
		t.Errorf("expected 1 aborted, got %d", len(aborted))
	}
}

func TestWALCheckpoint(t *testing.T) {
	dir := t.TempDir()
	w, _ := Open(dir)

	ts1 := w.Append(InsertRecord("t1", []byte("a"), []byte("va")))
	w.Append(CommitRecord(ts1))
	w.Append(CheckpointRecord(100))

	ts2 := w.Append(InsertRecord("t1", []byte("b"), []byte("vb")))
	w.Append(CommitRecord(ts2))
	w.Close()

	w2, _ := Open(dir)
	defer w2.Close()

	recs, _ := w2.ReadAll()
	if len(recs) != 5 {
		t.Fatalf("expected 5 records, got %d", len(recs))
	}

	committed, _ := RecoverCommitted(recs)
	if len(committed) != 2 {
		t.Errorf("expected 2 committed txns, got %d", len(committed))
	}

	// Find checkpoint.
	var cpTS uint64
	for _, r := range recs {
		if r.Type == RecCheckpoint {
			cpTS = r.CheckpointTS
		}
	}
	if cpTS != 100 {
		t.Errorf("expected checkpoint ts=100, got %d", cpTS)
	}
}

func TestWALEmpty(t *testing.T) {
	dir := t.TempDir()
	w, _ := Open(dir)
	w.Close()

	w2, _ := Open(dir)
	defer w2.Close()

	recs, _ := w2.ReadAll()
	if len(recs) != 0 {
		t.Errorf("expected 0 records from empty WAL, got %d", len(recs))
	}
}
