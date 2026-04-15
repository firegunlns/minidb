package storage

import (
	"bytes"
	"testing"
	"time"
)

func TestEncodeDecodeInt(t *testing.T) {
	col := ColumnDef{Name: "id", Type: ColTypeInt}
	val := int32(42)
	data := EncodeColumnValue(col, val)
	got, _ := DecodeColumnValue(data, 0, col)
	if got.(int32) != val {
		t.Fatalf("expected %v, got %v", val, got)
	}
}

func TestEncodeDecodeIntNegative(t *testing.T) {
	col := ColumnDef{Name: "val", Type: ColTypeInt}
	val := int32(-100)
	data := EncodeColumnValue(col, val)
	got, _ := DecodeColumnValue(data, 0, col)
	if got.(int32) != val {
		t.Fatalf("expected %v, got %v", val, got)
	}
}

func TestEncodeDecodeBigInt(t *testing.T) {
	col := ColumnDef{Name: "big", Type: ColTypeBigInt}
	val := int64(1234567890123)
	data := EncodeColumnValue(col, val)
	got, _ := DecodeColumnValue(data, 0, col)
	if got.(int64) != val {
		t.Fatalf("expected %v, got %v", val, got)
	}
}

func TestEncodeDecodeBigIntNegative(t *testing.T) {
	col := ColumnDef{Name: "big", Type: ColTypeBigInt}
	val := int64(-9999999999)
	data := EncodeColumnValue(col, val)
	got, _ := DecodeColumnValue(data, 0, col)
	if got.(int64) != val {
		t.Fatalf("expected %v, got %v", val, got)
	}
}

func TestEncodeDecodeVarchar(t *testing.T) {
	col := ColumnDef{Name: "name", Type: ColTypeVarchar, Length: 20}
	val := "hello world"
	data := EncodeColumnValue(col, val)
	got, _ := DecodeColumnValue(data, 0, col)
	if got.(string) != val {
		t.Fatalf("expected %q, got %q", val, got)
	}
}

func TestEncodeDecodeVarcharEmpty(t *testing.T) {
	col := ColumnDef{Name: "name", Type: ColTypeVarchar, Length: 20}
	val := ""
	data := EncodeColumnValue(col, val)
	got, _ := DecodeColumnValue(data, 0, col)
	if got.(string) != val {
		t.Fatalf("expected empty string, got %q", got)
	}
}

func TestEncodeDecodeDecimal(t *testing.T) {
	col := ColumnDef{Name: "tax", Type: ColTypeDecimal, Precision: 12, Scale: 2}
	val := "123.45"
	data := EncodeColumnValue(col, val)
	got, _ := DecodeColumnValue(data, 0, col)
	if got.(string) != val {
		t.Fatalf("expected %q, got %q", val, got)
	}
}

func TestEncodeDecodeDecimalNegative(t *testing.T) {
	col := ColumnDef{Name: "bal", Type: ColTypeDecimal, Precision: 12, Scale: 4}
	val := "-0.1234"
	data := EncodeColumnValue(col, val)
	got, _ := DecodeColumnValue(data, 0, col)
	if got.(string) != val {
		t.Fatalf("expected %q, got %q", val, got)
	}
}

func TestEncodeDecodeTimestamp(t *testing.T) {
	col := ColumnDef{Name: "ts", Type: ColTypeTimestamp}
	val := time.Date(2024, 1, 15, 10, 30, 45, 0, time.UTC)
	data := EncodeColumnValue(col, val)
	got, _ := DecodeColumnValue(data, 0, col)
	if !got.(time.Time).Equal(val) {
		t.Fatalf("expected %v, got %v", val, got)
	}
}

func TestEncodeDecodeNull(t *testing.T) {
	col := ColumnDef{Name: "val", Type: ColTypeInt, Nullable: true}
	data := EncodeColumnValue(col, nil)
	got, next := DecodeColumnValue(data, 0, col)
	if got != nil {
		t.Fatalf("expected nil, got %v", got)
	}
	if next != 1 { // just the null tag
		t.Fatalf("expected next offset 1, got %d", next)
	}
}

func TestEncodeRow(t *testing.T) {
	cols := []ColumnDef{
		{Name: "id", Type: ColTypeInt},
		{Name: "name", Type: ColTypeVarchar, Length: 20},
		{Name: "balance", Type: ColTypeDecimal, Precision: 12, Scale: 2},
	}
	vals := []interface{}{
		int32(1),
		"Alice",
		"99.99",
	}
	data := EncodeRow(cols, vals)
	gotVals, gotNulls := DecodeRow(data, cols)
	for i, v := range gotVals {
		if v != vals[i] {
			t.Errorf("col %d: expected %v (%T), got %v (%T)", i, vals[i], vals[i], v, v)
		}
		if gotNulls[i] {
			t.Errorf("col %d: unexpected null", i)
		}
	}
}

func TestEncodeRowWithNull(t *testing.T) {
	cols := []ColumnDef{
		{Name: "id", Type: ColTypeInt},
		{Name: "name", Type: ColTypeVarchar, Length: 20, Nullable: true},
		{Name: "ts", Type: ColTypeTimestamp, Nullable: true},
	}
	vals := []interface{}{
		int32(42),
		nil,
		nil,
	}
	data := EncodeRow(cols, vals)
	gotVals, gotNulls := DecodeRow(data, cols)
	if gotVals[0].(int32) != 42 {
		t.Fatalf("id: expected 42, got %v", gotVals[0])
	}
	if !gotNulls[1] || !gotNulls[2] {
		t.Fatalf("expected nulls for cols 1,2, got %v", gotNulls)
	}
}

func TestEncodeRowAllTypes(t *testing.T) {
	cols := []ColumnDef{
		{Name: "i", Type: ColTypeInt},
		{Name: "bi", Type: ColTypeBigInt},
		{Name: "v", Type: ColTypeVarchar, Length: 50},
		{Name: "d", Type: ColTypeDecimal, Precision: 10, Scale: 4},
		{Name: "ts", Type: ColTypeTimestamp},
	}
	ts := time.Date(2024, 6, 1, 12, 0, 0, 0, time.UTC)
	vals := []interface{}{
		int32(100),
		int64(999999),
		"test string",
		"12.3456",
		ts,
	}
	data := EncodeRow(cols, vals)
	gotVals, _ := DecodeRow(data, cols)
	if gotVals[0].(int32) != int32(100) {
		t.Errorf("int: got %v", gotVals[0])
	}
	if gotVals[1].(int64) != int64(999999) {
		t.Errorf("bigint: got %v", gotVals[1])
	}
	if gotVals[2].(string) != "test string" {
		t.Errorf("varchar: got %v", gotVals[2])
	}
	if gotVals[3].(string) != "12.3456" {
		t.Errorf("decimal: got %v", gotVals[3])
	}
	if !gotVals[4].(time.Time).Equal(ts) {
		t.Errorf("timestamp: got %v", gotVals[4])
	}
}

func TestIntSortOrder(t *testing.T) {
	// Verify that order-preserving encoding sorts correctly.
	col := ColumnDef{Name: "id", Type: ColTypeInt}
	vals := []int32{-100, -1, 0, 1, 42, 100, 2147483647}
	prev := EncodeColumnValue(col, vals[0])
	for i := 1; i < len(vals); i++ {
		cur := EncodeColumnValue(col, vals[i])
		if bytes.Compare(prev, cur) >= 0 {
			t.Errorf("int %d (%x) not sorted after %d (%x)", vals[i], cur, vals[i-1], prev)
		}
		prev = cur
	}
}

func TestBigIntSortOrder(t *testing.T) {
	col := ColumnDef{Name: "id", Type: ColTypeBigInt}
	vals := []int64{-100, -1, 0, 1, 42, 100, 9223372036854775807}
	prev := EncodeColumnValue(col, vals[0])
	for i := 1; i < len(vals); i++ {
		cur := EncodeColumnValue(col, vals[i])
		if bytes.Compare(prev, cur) >= 0 {
			t.Errorf("bigint %d not sorted after %d", vals[i], vals[i-1])
		}
		prev = cur
	}
}

func TestVarcharSortOrder(t *testing.T) {
	// VARCHAR sorts by byte order: shorter strings come before longer with same prefix,
	// but "b" > "abc" because 'b' > 'a' at the first byte.
	col := ColumnDef{Name: "s", Type: ColTypeVarchar}
	vals := []string{"", "a", "ab", "abc", "b", "ba", "z"}
	prev := EncodeColumnValue(col, vals[0])
	for i := 1; i < len(vals); i++ {
		cur := EncodeColumnValue(col, vals[i])
		if bytes.Compare(prev, cur) >= 0 {
			t.Errorf("varchar %q (%x) not sorted after %q (%x)", vals[i], cur, vals[i-1], prev)
		}
		prev = cur
	}
	// Verify that byte-order matches string comparison.
	if "b" > "abc" != (bytes.Compare([]byte("b"), []byte("abc")) > 0) {
		t.Log("Note: byte order differs from lexicographic for different-length strings")
	}
}

func TestPrimaryKeyEncoding(t *testing.T) {
	// Composite PK: (W_ID int, D_ID int)
	cols := []ColumnDef{
		{Name: "w_id", Type: ColTypeInt},
		{Name: "d_id", Type: ColTypeInt},
	}
	pk1 := EncodePrimaryKey(cols, int32(1), int32(1))
	pk2 := EncodePrimaryKey(cols, int32(1), int32(2))
	pk3 := EncodePrimaryKey(cols, int32(2), int32(1))
	if bytes.Compare(pk1, pk2) >= 0 {
		t.Error("(1,1) should sort before (1,2)")
	}
	if bytes.Compare(pk2, pk3) >= 0 {
		t.Error("(1,2) should sort before (2,1)")
	}
}
