package wal

// RecordType identifies the type of WAL record.
type RecordType uint8

const (
	RecInsert    RecordType = 1
	RecUpdate    RecordType = 2
	RecDelete    RecordType = 3
	RecCommit    RecordType = 4
	RecAbort     RecordType = 5
	RecCheckpoint RecordType = 6
)

// Record represents a single WAL entry.
type Record struct {
	Type         RecordType
	TxnTS        uint64 // transaction start timestamp
	CommitTS     uint64 // assigned at commit
	CheckpointTS uint64 // for checkpoint records
	TreeKey      string
	PK           []byte
	RowData      []byte
	OldData      []byte // for updates
}

// Convenience constructors.

func InsertRecord(treeKey string, pk, rowData []byte) Record {
	return Record{Type: RecInsert, TreeKey: treeKey, PK: pk, RowData: rowData}
}

func UpdateRecord(treeKey string, pk, oldData, newRowData []byte) Record {
	return Record{Type: RecUpdate, TreeKey: treeKey, PK: pk, OldData: oldData, RowData: newRowData}
}

func DeleteRecord(treeKey string, pk, oldData []byte) Record {
	return Record{Type: RecDelete, TreeKey: treeKey, PK: pk, OldData: oldData}
}

func CommitRecord(txnTS uint64) Record {
	return Record{Type: RecCommit, TxnTS: txnTS}
}

func AbortRecord(txnTS uint64) Record {
	return Record{Type: RecAbort, TxnTS: txnTS}
}

func CheckpointRecord(ts uint64) Record {
	return Record{Type: RecCheckpoint, CheckpointTS: ts}
}
