package wal

import (
	"encoding/binary"
	"hash/crc32"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"

	"lns.com/minidb/metrics"
)

const (
	walFileName = "wal.log"
	magicByte   = 0xDB
)

// WAL is an append-only write-ahead log.
// Record format on disk:
//
//	[1B magic][4B payloadLen][4B CRC32][payload]
//
// Payload format depends on record type:
//
//	[1B type][8B txnTS][8B commitTS][...type-specific data...]
type WAL struct {
	mu        sync.Mutex
	f         *os.File
	tsCounter uint64
}

// Open creates or opens a WAL file in the given directory.
func Open(dir string) (*WAL, error) {
	path := filepath.Join(dir, walFileName)
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0644)
	if err != nil {
		return nil, err
	}
	// Scan to find the highest timestamp.
	w := &WAL{f: f}
	w.scanForTimestamp()
	return w, nil
}

// Close flushes and closes the WAL file.
func (w *WAL) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.f != nil {
		w.f.Sync()
		return w.f.Close()
	}
	return nil
}

// Truncate empties the WAL file. Call after all dirty pages have been flushed
// to their respective B+ tree files so that recovery is not needed.
func (w *WAL) Truncate() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.f != nil {
		if err := w.f.Truncate(0); err != nil {
			return err
		}
		_, err := w.f.Seek(0, 0)
		return err
	}
	return nil
}

// Append writes a record to the WAL and returns the allocated timestamp.
// Note: Sync is NOT called here - callers should sync periodically if needed.
func (w *WAL) Append(rec Record) uint64 {
	start := time.Now()
	w.mu.Lock()
	defer w.mu.Unlock()

	ts := atomic.AddUint64(&w.tsCounter, 1)
	switch rec.Type {
	case RecCommit:
		rec.CommitTS = ts
	case RecCheckpoint:
		// Store CheckpointTS in CommitTS field for serialization.
		rec.CommitTS = rec.CheckpointTS
	case RecAbort:
		// TxnTS already set by caller.
	default:
		if rec.TxnTS == 0 {
			rec.TxnTS = ts
		}
	}

	payload := encodePayload(rec)
	buf := make([]byte, 1+4+4+len(payload))
	buf[0] = magicByte
	binary.BigEndian.PutUint32(buf[1:], uint32(len(payload)))
	crc := crc32.ChecksumIEEE(payload)
	binary.BigEndian.PutUint32(buf[5:], crc)
	copy(buf[9:], payload)

	w.f.Write(buf)
	// Removed: w.f.Sync() - too expensive for every write

	metrics.WALAppendDuration.Observe(time.Since(start).Seconds())
	return ts
}

// Sync flushes the WAL to disk.
func (w *WAL) Sync() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.f.Sync()
}

// ReadAll reads all records from the WAL file.
func (w *WAL) ReadAll() ([]Record, error) {
	w.mu.Lock()
	defer w.mu.Unlock()

	if _, err := w.f.Seek(0, 0); err != nil {
		return nil, err
	}

	var records []Record
	header := make([]byte, 9) // 1 magic + 4 len + 4 crc

	for {
		_, err := w.f.Read(header)
		if err != nil {
			break // EOF or error
		}
		if header[0] != magicByte {
			break // corrupted
		}
		payloadLen := binary.BigEndian.Uint32(header[1:])
		// _ = binary.BigEndian.Uint32(header[5:]) // CRC, could verify

		payload := make([]byte, payloadLen)
		n, err := w.f.Read(payload)
		if err != nil || n != int(payloadLen) {
			break // truncated record
		}

		rec, ok := decodePayload(payload)
		if !ok {
			break // corrupted
		}
		records = append(records, rec)
	}

	return records, nil
}

// scanForTimestamp scans existing records to find the highest timestamp.
func (w *WAL) scanForTimestamp() {
	records, err := w.ReadAll()
	if err != nil {
		return
	}
	var maxTS uint64
	for _, r := range records {
		if r.TxnTS > maxTS {
			maxTS = r.TxnTS
		}
		if r.CommitTS > maxTS {
			maxTS = r.CommitTS
		}
	}
	atomic.StoreUint64(&w.tsCounter, maxTS)
}

func encodePayload(rec Record) []byte {
	// All records start with [1B type][8B txnTS][8B commitTS]
	base := 17
	extra := 0

	switch rec.Type {
	case RecInsert, RecDelete:
		// [2B treeKeyLen][treeKey][4B pkLen][pk][4B rowLen][row]
		extra = 2 + len(rec.TreeKey) + 4 + len(rec.PK) + 4 + len(rec.RowData)
	case RecUpdate:
		// [2B treeKeyLen][treeKey][4B pkLen][pk][4B oldLen][old][4B newLen][new]
		extra = 2 + len(rec.TreeKey) + 4 + len(rec.PK) + 4 + len(rec.OldData) + 4 + len(rec.RowData)
	case RecCommit, RecAbort:
		// just the base
	case RecCheckpoint:
		// [8B checkpointTS] already in commitTS field
	}

	buf := make([]byte, base+extra)
	buf[0] = byte(rec.Type)
	binary.BigEndian.PutUint64(buf[1:], rec.TxnTS)
	binary.BigEndian.PutUint64(buf[9:], rec.CommitTS)

	off := base
	switch rec.Type {
	case RecInsert, RecDelete:
		binary.BigEndian.PutUint16(buf[off:], uint16(len(rec.TreeKey)))
		off += 2
		copy(buf[off:], rec.TreeKey)
		off += len(rec.TreeKey)
		binary.BigEndian.PutUint32(buf[off:], uint32(len(rec.PK)))
		off += 4
		copy(buf[off:], rec.PK)
		off += len(rec.PK)
		binary.BigEndian.PutUint32(buf[off:], uint32(len(rec.RowData)))
		off += 4
		copy(buf[off:], rec.RowData)
	case RecUpdate:
		binary.BigEndian.PutUint16(buf[off:], uint16(len(rec.TreeKey)))
		off += 2
		copy(buf[off:], rec.TreeKey)
		off += len(rec.TreeKey)
		binary.BigEndian.PutUint32(buf[off:], uint32(len(rec.PK)))
		off += 4
		copy(buf[off:], rec.PK)
		off += len(rec.PK)
		binary.BigEndian.PutUint32(buf[off:], uint32(len(rec.OldData)))
		off += 4
		copy(buf[off:], rec.OldData)
		off += len(rec.OldData)
		binary.BigEndian.PutUint32(buf[off:], uint32(len(rec.RowData)))
		off += 4
		copy(buf[off:], rec.RowData)
	}

	return buf
}

func decodePayload(data []byte) (Record, bool) {
	if len(data) < 17 {
		return Record{}, false
	}
	rec := Record{
		Type:     RecordType(data[0]),
		TxnTS:    binary.BigEndian.Uint64(data[1:]),
		CommitTS: binary.BigEndian.Uint64(data[9:]),
	}

	off := 17
	switch rec.Type {
	case RecInsert, RecDelete:
		if off+2 > len(data) {
			return Record{}, false
		}
		tkLen := int(binary.BigEndian.Uint16(data[off:]))
		off += 2
		if off+tkLen > len(data) {
			return Record{}, false
		}
		rec.TreeKey = string(data[off : off+tkLen])
		off += tkLen
		if off+4 > len(data) {
			return Record{}, false
		}
		pkLen := int(binary.BigEndian.Uint32(data[off:]))
		off += 4
		if off+pkLen > len(data) {
			return Record{}, false
		}
		rec.PK = data[off : off+pkLen]
		off += pkLen
		if off+4 > len(data) {
			return Record{}, false
		}
		rowLen := int(binary.BigEndian.Uint32(data[off:]))
		off += 4
		if off+rowLen > len(data) {
			return Record{}, false
		}
		rec.RowData = data[off : off+rowLen]
	case RecUpdate:
		if off+2 > len(data) {
			return Record{}, false
		}
		tkLen := int(binary.BigEndian.Uint16(data[off:]))
		off += 2
		if off+tkLen > len(data) {
			return Record{}, false
		}
		rec.TreeKey = string(data[off : off+tkLen])
		off += tkLen
		if off+4 > len(data) {
			return Record{}, false
		}
		pkLen := int(binary.BigEndian.Uint32(data[off:]))
		off += 4
		if off+pkLen > len(data) {
			return Record{}, false
		}
		rec.PK = data[off : off+pkLen]
		off += pkLen
		if off+4 > len(data) {
			return Record{}, false
		}
		oldLen := int(binary.BigEndian.Uint32(data[off:]))
		off += 4
		if off+oldLen > len(data) {
			return Record{}, false
		}
		rec.OldData = data[off : off+oldLen]
		off += oldLen
		if off+4 > len(data) {
			return Record{}, false
		}
		newLen := int(binary.BigEndian.Uint32(data[off:]))
		off += 4
		if off+newLen > len(data) {
			return Record{}, false
		}
		rec.RowData = data[off : off+newLen]
	case RecCheckpoint:
		rec.CheckpointTS = rec.CommitTS
	}

	return rec, true
}

// RecoverCommitted takes all WAL records and returns the timestamps of
// committed and aborted (incomplete) transactions.
func RecoverCommitted(records []Record) (committed, aborted []uint64) {
	// Find all txn timestamps.
	txnMap := make(map[uint64]bool) // false = uncommitted
	for _, r := range records {
		switch r.Type {
		case RecInsert, RecUpdate, RecDelete:
			txnMap[r.TxnTS] = false
		case RecCommit:
			txnMap[r.TxnTS] = true
		case RecAbort:
			delete(txnMap, r.TxnTS)
			aborted = append(aborted, r.TxnTS)
		}
	}
	for ts, isCommitted := range txnMap {
		if isCommitted {
			committed = append(committed, ts)
		} else {
			aborted = append(aborted, ts)
		}
	}
	return
}

// RecoverRecords returns all records belonging to committed transactions.
func RecoverRecords(records []Record) []Record {
	committedSet := make(map[uint64]bool)
	var committedTxns []uint64
	for _, r := range records {
		if r.Type == RecCommit {
			committedSet[r.TxnTS] = true
			committedTxns = append(committedTxns, r.TxnTS)
		}
	}

	// Collect commit timestamps.
	commitTSMap := make(map[uint64]uint64)
	for _, r := range records {
		if r.Type == RecCommit {
			commitTSMap[r.TxnTS] = r.CommitTS
		}
	}

	var result []Record
	for _, r := range records {
		if r.Type == RecCommit || r.Type == RecAbort || r.Type == RecCheckpoint {
			continue
		}
		if committedSet[r.TxnTS] {
			result = append(result, r)
		}
	}
	return result
}
