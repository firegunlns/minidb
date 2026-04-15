package storage

import (
	"encoding/binary"
)

const (
	FlagDeleted byte = 0x01
)

// VersionKey encodes a primary key + commit timestamp into a B+ tree key.
// Uses uint64_max - commit_ts (big-endian) so that newer versions sort first.
func VersionKey(pk []byte, commitTS uint64) []byte {
	buf := make([]byte, len(pk)+8)
	copy(buf, pk)
	binary.BigEndian.PutUint64(buf[len(pk):], ^commitTS)
	return buf
}

// KeyPrefix extracts the primary key prefix from a versioned key.
func KeyPrefix(key []byte) []byte {
	if len(key) <= 8 {
		return key
	}
	return key[:len(key)-8]
}

// KeyCommitTS extracts the commit timestamp from a versioned key.
func KeyCommitTS(key []byte) uint64 {
	if len(key) < 8 {
		return 0
	}
	return ^binary.BigEndian.Uint64(key[len(key)-8:])
}

// compareKeys returns -1, 0, or 1.
func compareKeys(a, b []byte) int {
	minLen := len(a)
	if len(b) < minLen {
		minLen = len(b)
	}
	for i := 0; i < minLen; i++ {
		if a[i] < b[i] {
			return -1
		}
		if a[i] > b[i] {
			return 1
		}
	}
	if len(a) < len(b) {
		return -1
	}
	if len(a) > len(b) {
		return 1
	}
	return 0
}

// EncodeMVCCValue encodes row data with MVCC metadata.
// Format: [1B flags][8B xmin][8B xmax][row_data]
func EncodeMVCCValue(xmin, xmax uint64, flags byte, rowData []byte) []byte {
	buf := make([]byte, 1+8+8+len(rowData))
	buf[0] = flags
	binary.BigEndian.PutUint64(buf[1:], xmin)
	binary.BigEndian.PutUint64(buf[9:], xmax)
	copy(buf[17:], rowData)
	return buf
}

// DecodeMVCCValue decodes MVCC metadata and row data from a value.
func DecodeMVCCValue(data []byte) (xmin, xmax uint64, flags byte, rowData []byte, err error) {
	if len(data) < 17 {
		return 0, 0, 0, nil, errValueTooShort
	}
	flags = data[0]
	xmin = binary.BigEndian.Uint64(data[1:])
	xmax = binary.BigEndian.Uint64(data[9:])
	rowData = data[17:]
	return
}

var errValueTooShort = &mvccError{"mvcc value too short"}

type mvccError struct{ msg string }

func (e *mvccError) Error() string { return e.msg }

// IsVisible returns whether a row version is visible at the given read timestamp.
func IsVisible(xmin, xmax uint64, flags byte, readTS uint64) bool {
	if flags&FlagDeleted != 0 {
		return false // tombstones are never visible
	}
	if xmin > readTS {
		return false // created after our snapshot
	}
	if xmax != 0 && xmax <= readTS {
		return false // deleted before or at our snapshot
	}
	return true
}

// ScanRangeForPK returns the start and end keys for scanning all versions of a PK.
func ScanRangeForPK(pk []byte) (start, end []byte) {
	// Start: PK + 0x0000000000000000 (newest possible, = max ts)
	// End: PK + 0xFFFFFFFFFFFFFFFF (oldest possible, = ts 0)
	start = make([]byte, len(pk)+8)
	copy(start, pk)
	// start suffix = 0x00...00 = ^uint64_max = ts max → sorts first

	end = make([]byte, len(pk)+8)
	copy(end, pk)
	for i := len(pk); i < len(end); i++ {
		end[i] = 0xFF
	}
	return start, end
}
