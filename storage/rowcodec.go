// Package storage 提供存储引擎功能
package storage

import (
	"encoding/binary"
	"fmt"
	"math"
	"strconv"
	"sync/atomic"
	"time"
)

// ColumnType 列类型枚举
type ColumnType uint8

const (
	ColTypeInt       ColumnType = 0x01 // 32位整数
	ColTypeBigInt    ColumnType = 0x02 // 64位整数
	ColTypeVarchar   ColumnType = 0x03 // 可变字符串
	ColTypeDecimal   ColumnType = 0x04 // 十进制数
	ColTypeTimestamp ColumnType = 0x05 // 时间戳
	ColTypeDouble    ColumnType = 0x06 // 双精度浮点数

	typeTagNull = 0xFF // NULL标记
)

// ColumnDef 列定义
type ColumnDef struct {
	Name      string     // 列名
	Type      ColumnType // 列类型
	Length    int        // VARCHAR最大长度
	Precision int        // DECIMAL精度
	Scale     int        // DECIMAL小数位
	Nullable  bool       // 是否可空
	AutoInc   bool       // 是否自增
	Hidden    bool       // 是否隐藏列(不显示在SELECT *中)
}

// --- Column value encoding ---

// EncodeColumnValue encodes a single column value to bytes.
// Uses order-preserving encoding for sortable types (INT, BIGINT, VARCHAR).
func EncodeColumnValue(col ColumnDef, val any) []byte {
	if val == nil {
		return []byte{typeTagNull}
	}
	switch col.Type {
	case ColTypeInt:
		v := val.(int32)
		// Order-preserving: flip sign bit so negative sorts before positive.
		buf := make([]byte, 1+4)
		buf[0] = byte(ColTypeInt)
		binary.BigEndian.PutUint32(buf[1:], uint32(v)^0x80000000)
		return buf
	case ColTypeBigInt:
		v := val.(int64)
		buf := make([]byte, 1+8)
		buf[0] = byte(ColTypeBigInt)
		binary.BigEndian.PutUint64(buf[1:], uint64(v)^0x8000000000000000)
		return buf
	case ColTypeVarchar:
		v := val.(string)
		// For B+ tree key ordering: raw bytes followed by 0x00 terminator.
		buf := make([]byte, 1+len(v)+1)
		buf[0] = byte(ColTypeVarchar)
		copy(buf[1:], v)
		buf[1+len(v)] = 0x00
		return buf
	case ColTypeDecimal:
		// Store as string for exact precision.
		v := val.(string)
		buf := make([]byte, 1+4+len(v))
		buf[0] = byte(ColTypeDecimal)
		binary.BigEndian.PutUint32(buf[1:], uint32(len(v)))
		copy(buf[5:], v)
		return buf
	case ColTypeTimestamp:
		v := val.(time.Time)
		unixNano := v.UTC().UnixNano()
		buf := make([]byte, 1+8)
		buf[0] = byte(ColTypeTimestamp)
		binary.BigEndian.PutUint64(buf[1:], uint64(unixNano))
		return buf
	case ColTypeDouble:
		v := val.(float64)
		buf := make([]byte, 1+8)
		buf[0] = byte(ColTypeDouble)
		binary.BigEndian.PutUint64(buf[1:], math.Float64bits(v))
		return buf
	default:
		panic(fmt.Sprintf("unknown column type: %d", col.Type))
	}
}

// DecodeColumnValue decodes a single column value from bytes at offset.
// Returns the decoded value and the next offset.
func DecodeColumnValue(data []byte, offset int, col ColumnDef) (any, int) {
	if offset >= len(data) {
		return nil, offset
	}
	tag := data[offset]
	if tag == typeTagNull {
		return nil, offset + 1
	}
	offset++ // skip tag

	switch ColumnType(tag) {
	case ColTypeInt:
		if offset+4 > len(data) {
			return nil, len(data)
		}
		v := int32(binary.BigEndian.Uint32(data[offset:]) ^ 0x80000000)
		return v, offset + 4
	case ColTypeBigInt:
		if offset+8 > len(data) {
			return nil, len(data)
		}
		v := int64(binary.BigEndian.Uint64(data[offset:]) ^ 0x8000000000000000)
		return v, offset + 8
	case ColTypeVarchar:
		start := offset
		end := offset
		for end < len(data) && data[end] != 0x00 {
			end++
		}
		return string(data[start:end]), end + 1 // skip null terminator
	case ColTypeDecimal:
		if offset+4 > len(data) {
			return nil, len(data)
		}
		length := int(binary.BigEndian.Uint32(data[offset:]))
		offset += 4
		if offset+length > len(data) {
			return nil, len(data)
		}
		return string(data[offset : offset+length]), offset + length
	case ColTypeTimestamp:
		if offset+8 > len(data) {
			return nil, len(data)
		}
		nano := int64(binary.BigEndian.Uint64(data[offset:]))
		return time.Unix(0, nano).UTC(), offset + 8
	case ColTypeDouble:
		if offset+8 > len(data) {
			return nil, len(data)
		}
		bits := binary.BigEndian.Uint64(data[offset:])
		return math.Float64frombits(bits), offset + 8
	default:
		return nil, offset
	}
}

// --- Row encoding ---

// EncodeRow encodes a full row into bytes.
func EncodeRow(cols []ColumnDef, vals []any) []byte {
	numCols := len(cols)
	nullBitmap := make([]byte, (numCols+7)/8)

	// Calculate total size.
	size := 2 + len(nullBitmap)
	for i, col := range cols {
		size += len(EncodeColumnValue(col, vals[i]))
		if vals[i] == nil {
			nullBitmap[i/8] |= 1 << (i % 8)
		}
	}

	buf := make([]byte, size)
	binary.BigEndian.PutUint16(buf[0:], uint16(numCols))
	copy(buf[2:], nullBitmap)
	off := 2 + len(nullBitmap)

	for i, col := range cols {
		encoded := EncodeColumnValue(col, vals[i])
		copy(buf[off:], encoded)
		off += len(encoded)
	}
	return buf
}

// DecodeRow decodes a full row from bytes.
// Returns column values and a null bitmap.
func DecodeRow(data []byte, cols []ColumnDef) ([]any, []bool) {
	if len(data) < 2 {
		vals := make([]any, len(cols))
		nulls := make([]bool, len(cols))
		for i := range nulls {
			nulls[i] = true
		}
		return vals, nulls
	}
	numCols := int(binary.BigEndian.Uint16(data))
	bitmapSize := (numCols + 7) / 8
	if 2+bitmapSize > len(data) {
		vals := make([]any, len(cols))
		nulls := make([]bool, len(cols))
		for i := range nulls {
			nulls[i] = true
		}
		return vals, nulls
	}
	nullBitmap := data[2 : 2+bitmapSize]
	off := 2 + bitmapSize

	// Decode only up to min(numCols, len(cols)) to handle schema mismatches.
	n := numCols
	if n > len(cols) {
		n = len(cols)
	}
	vals := make([]any, len(cols))
	nulls := make([]bool, len(cols))

	for i := 0; i < n; i++ {
		nulls[i] = (nullBitmap[i/8]>>(i%8))&1 == 1
		vals[i], off = DecodeColumnValue(data, off, cols[i])
	}
	// Mark extra columns (schema has more columns than data) as null.
	for i := n; i < len(cols); i++ {
		nulls[i] = true
	}
	return vals, nulls
}

// --- Primary key encoding ---

// nullPKCounter provides unique disambiguators for NULL primary keys.
var nullPKCounter uint64

// EncodePrimaryKey encodes a composite primary key from column values.
// When any PK value is nil, a unique 8-byte suffix is appended so that
// multiple rows with NULL PK don't collide in the B-tree.
func EncodePrimaryKey(cols []ColumnDef, pkVals ...any) []byte {
	var buf []byte
	hasNull := false
	for i, col := range cols {
		if pkVals[i] == nil {
			hasNull = true
		}
		buf = append(buf, EncodeColumnValue(col, pkVals[i])...)
	}
	if hasNull {
		// Append a unique 8-byte counter so NULL PK rows don't collide.
		suffix := make([]byte, 8)
		binary.BigEndian.PutUint64(suffix, atomic.AddUint64(&nullPKCounter, 1))
		buf = append(buf, suffix...)
	}
	return buf
}

// EncodeIndexKey builds a secondary index key from index column values and
// the primary key. Format: [idx_col_1][idx_col_2]...[idx_col_N][pk].
func EncodeIndexKey(idxCols []ColumnDef, idxVals []any, pkCols []ColumnDef, pkVals ...any) []byte {
	var buf []byte
	for i, col := range idxCols {
		buf = append(buf, EncodeColumnValue(col, idxVals[i])...)
	}
	buf = append(buf, EncodePrimaryKey(pkCols, pkVals...)...)
	return buf
}

// EncodeIndexKeyWithRawPK builds a secondary index key from index column values
// and raw PK bytes (as returned by the B-tree scan). This avoids re-encoding
// the PK which would generate a new unique rowid suffix.
func EncodeIndexKeyWithRawPK(idxCols []ColumnDef, idxVals []any, rawPK []byte) []byte {
	var buf []byte
	for i, col := range idxCols {
		buf = append(buf, EncodeColumnValue(col, idxVals[i])...)
	}
	buf = append(buf, rawPK...)
	return buf
}

// DecodeIndexKeyPK extracts the primary key bytes from an index key by
// skipping past the encoded index column values.
func DecodeIndexKeyPK(indexKey []byte, idxCols []ColumnDef) []byte {
	offset := 0
	for _, col := range idxCols {
		_, nextOff := DecodeColumnValue(indexKey, offset, col)
		offset = nextOff
	}
	return indexKey[offset:]
}

// --- Helpers ---

// CoerceValue converts a value to the expected Go type for the given column.
func CoerceValue(col ColumnDef, val any) (any, error) {
	if val == nil {
		return nil, nil
	}
	switch col.Type {
	case ColTypeInt:
		switch v := val.(type) {
		case int32:
			return v, nil
		case int:
			return int32(v), nil
		case int64:
			return int32(v), nil
		case uint32:
			return int32(v), nil
		case uint64:
			return int32(v), nil
		case float64:
			return int32(v), nil
		case string:
			i, err := strconv.ParseInt(v, 10, 32)
			return int32(i), err
		}
	case ColTypeBigInt:
		switch v := val.(type) {
		case int64:
			return v, nil
		case int:
			return int64(v), nil
		case int32:
			return int64(v), nil
		case uint32:
			return int64(v), nil
		case uint64:
			return int64(v), nil
		case float64:
			return int64(v), nil
		case string:
			i, err := strconv.ParseInt(v, 10, 64)
			return i, err
		}
	case ColTypeVarchar:
		switch v := val.(type) {
		case string:
			return v, nil
		case []byte:
			return string(v), nil
		}
	case ColTypeDecimal:
		switch v := val.(type) {
		case string:
			return v, nil
		case float64:
			return strconv.FormatFloat(v, 'f', col.Scale, 64), nil
		case int:
			return fmt.Sprintf("%d", v), nil
		}
	case ColTypeTimestamp:
		switch v := val.(type) {
		case time.Time:
			return v, nil
		case string:
			return parseTimestamp(v)
		case []byte:
			return parseTimestamp(string(v))
		}
	case ColTypeDouble:
		switch v := val.(type) {
		case float64:
			return v, nil
		case int:
			return float64(v), nil
		}
	}

	// Fallback: try converting via fmt.Stringer (e.g., TiDB MyDecimal).
	if s, ok := val.(fmt.Stringer); ok {
		str := s.String()
		switch col.Type {
		case ColTypeDecimal:
			return str, nil
		case ColTypeInt:
			i, err := strconv.ParseInt(str, 10, 32)
			return int32(i), err
		case ColTypeBigInt:
			i, err := strconv.ParseInt(str, 10, 64)
			return i, err
		case ColTypeDouble:
			f, err := strconv.ParseFloat(str, 64)
			return f, err
		case ColTypeVarchar:
			return str, nil
		case ColTypeTimestamp:
			return parseTimestamp(str)
		}
	}

	return nil, fmt.Errorf("cannot coerce %T to %v", val, col.Type)
}

func parseTimestamp(s string) (time.Time, error) {
	// Handle TiDB's Go syntax representation: {12 [234 7 4 16 13 20 49 64 221 10 0]}
	// This is a TiDB Datum with binary-encoded timestamp
	if len(s) > 2 && s[0] == '{' {
		// Try to decode TiDB binary timestamp format
		// Format: {type [bytes...]}
		if t, ok := parseTiDBTimestamp(s); ok {
			return t, nil
		}
	}

	for _, format := range []string{
		"2006-01-02 15:04:05",
		"2006-01-02 15:04:05.999999999",
		"2006-01-02T15:04:05",
		time.RFC3339,
	} {
		if t, err := time.Parse(format, s); err == nil {
			return t, nil
		}
	}
	return time.Time{}, fmt.Errorf("cannot parse timestamp: %q", s)
}

func parseTiDBTimestamp(s string) (time.Time, bool) {
	// TiDB Datum binary format: {type [bytes]}
	// Format: [type_flag][year_offset][month][day][hour][minute][second]...
	start := -1
	end := -1
	for i := 0; i < len(s); i++ {
		if s[i] == '[' {
			start = i + 1
		}
		if s[i] == ']' {
			end = i
			break
		}
	}
	if start < 0 || end <= start {
		return time.Time{}, false
	}

	// Extract bytes
	var bytes []byte
	for i := start; i < end; i++ {
		if s[i] == ' ' {
			continue
		}
		// Parse the byte value
		j := i
		for j < end && s[j] >= '0' && s[j] <= '9' {
			j++
		}
		if j > i {
			var val byte
			for k := i; k < j; k++ {
				val = val*10 + (s[k] - '0')
			}
			bytes = append(bytes, val)
			i = j - 1
		}
	}

	// TiDB binary timestamp format:
	// bytes[1] = year offset from 2000
	// bytes[2] = month
	// bytes[3] = day
	// bytes[4] = hour
	// bytes[5] = minute
	// bytes[6] = second
	if len(bytes) >= 7 {
		year := 2000 + int(bytes[1])
		month := int(bytes[2])
		day := int(bytes[3])
		hour := int(bytes[4])
		minute := int(bytes[5])
		second := int(bytes[6])

		if year >= 1900 && year <= 2100 && month >= 1 && month <= 12 && day >= 1 && day <= 31 {
			return time.Date(year, time.Month(month), day, hour, minute, second, 0, time.UTC), true
		}
	}

	return time.Time{}, false
}
