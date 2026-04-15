package storage

import (
	"encoding/binary"
	"fmt"
	"math"
	"strconv"
	"time"
)

// Column types.
type ColumnType uint8

const (
	ColTypeInt       ColumnType = 0x01
	ColTypeBigInt    ColumnType = 0x02
	ColTypeVarchar   ColumnType = 0x03
	ColTypeDecimal   ColumnType = 0x04
	ColTypeTimestamp ColumnType = 0x05
	ColTypeDouble    ColumnType = 0x06

	typeTagNull = 0xFF // null marker
)

type ColumnDef struct {
	Name      string
	Type      ColumnType
	Length    int // VARCHAR max length
	Precision int // DECIMAL precision
	Scale     int // DECIMAL scale
	Nullable  bool
	AutoInc   bool
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
		v := int32(binary.BigEndian.Uint32(data[offset:]) ^ 0x80000000)
		return v, offset + 4
	case ColTypeBigInt:
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
		length := int(binary.BigEndian.Uint32(data[offset:]))
		offset += 4
		return string(data[offset : offset+length]), offset + length
	case ColTypeTimestamp:
		nano := int64(binary.BigEndian.Uint64(data[offset:]))
		return time.Unix(0, nano).UTC(), offset + 8
	case ColTypeDouble:
		bits := binary.BigEndian.Uint64(data[offset:])
		return math.Float64frombits(bits), offset + 8
	default:
		panic(fmt.Sprintf("unknown type tag: %d", tag))
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
	numCols := int(binary.BigEndian.Uint16(data))
	bitmapSize := (numCols + 7) / 8
	nullBitmap := data[2 : 2+bitmapSize]
	off := 2 + bitmapSize

	vals := make([]any, numCols)
	nulls := make([]bool, numCols)

	for i := 0; i < numCols; i++ {
		nulls[i] = (nullBitmap[i/8]>>(i%8))&1 == 1
		vals[i], off = DecodeColumnValue(data, off, cols[i])
	}
	return vals, nulls
}

// --- Primary key encoding ---

// EncodePrimaryKey encodes a composite primary key from column values.
func EncodePrimaryKey(cols []ColumnDef, pkVals ...any) []byte {
	var buf []byte
	for i, col := range cols {
		buf = append(buf, EncodeColumnValue(col, pkVals[i])...)
	}
	return buf
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
			t, err := time.Parse("2006-01-02 15:04:05", v)
			return t, err
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
		}
	}

	return nil, fmt.Errorf("cannot coerce %T to %v", val, col.Type)
}
