// Package wal 提供预写日志功能
package wal

// RecordType WAL记录类型
type RecordType uint8

const (
	RecInsert     RecordType = 1 // 插入记录
	RecUpdate     RecordType = 2 // 更新记录
	RecDelete     RecordType = 3 // 删除记录
	RecCommit     RecordType = 4 // 提交记录
	RecAbort      RecordType = 5 // 中止记录
	RecCheckpoint RecordType = 6 // 检查点记录
)

// Record 单条WAL记录
type Record struct {
	Type         RecordType // 记录类型
	TxnTS        uint64     // 事务开始时间戳
	CommitTS     uint64     // 提交时分配的时间戳
	CheckpointTS uint64     // 检查点时间戳
	TreeKey      string     // B+树键（表或索引名）
	PK           []byte     // 主键
	RowData      []byte     // 行数据
	OldData      []byte     // 旧数据（用于更新）
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
