package storage

import (
	"testing"
)

func TestVersionKeyOrdering(t *testing.T) {
	pk := []byte("pk1")
	// Newer commit_ts should sort before older (so RangeScan returns newest first).
	k1 := VersionKey(pk, 100) // ts=100
	k2 := VersionKey(pk, 50)  // ts=50
	if compareKeys(k1, k2) >= 0 {
		t.Errorf("version key ts=100 should sort before ts=50 for same PK")
	}
}

func TestVersionKeyPrefix(t *testing.T) {
	pk := []byte("pk1")
	k1 := VersionKey(pk, 100)
	k2 := VersionKey(pk, 50)
	prefix1 := KeyPrefix(k1)
	prefix2 := KeyPrefix(k2)
	if string(prefix1) != string(pk) {
		t.Errorf("expected prefix %q, got %q", pk, prefix1)
	}
	if string(prefix1) != string(prefix2) {
		t.Error("same PK should have same prefix")
	}
}

func TestVersionKeyDifferentPK(t *testing.T) {
	pk1 := []byte("pk1")
	pk2 := []byte("pk2")
	k1 := VersionKey(pk1, 100)
	k2 := VersionKey(pk2, 50)
	if compareKeys(k1, k2) >= 0 {
		t.Errorf("pk1 should sort before pk2")
	}
}

func TestEncodeDecodeMVCCValue(t *testing.T) {
	rowData := []byte("some row data here")
	val := EncodeMVCCValue(10, 0, 0, rowData) // xmin=10, xmax=0, flags=0
	xmin, xmax, flags, data, err := DecodeMVCCValue(val)
	if err != nil {
		t.Fatal(err)
	}
	if xmin != 10 || xmax != 0 || flags != 0 {
		t.Errorf("expected (10,0,0), got (%d,%d,%d)", xmin, xmax, flags)
	}
	if string(data) != string(rowData) {
		t.Errorf("row data mismatch")
	}
}

func TestEncodeDecodeMVCCValueWithDelete(t *testing.T) {
	rowData := []byte("deleted row")
	val := EncodeMVCCValue(5, 20, FlagDeleted, rowData)
	xmin, xmax, flags, data, err := DecodeMVCCValue(val)
	if err != nil {
		t.Fatal(err)
	}
	if flags&FlagDeleted == 0 {
		t.Error("expected deleted flag")
	}
	if xmin != 5 || xmax != 20 {
		t.Errorf("expected (5,20), got (%d,%d)", xmin, xmax)
	}
	_ = data
}

func TestVisibilityBasic(t *testing.T) {
	// xmin=10, xmax=0 (alive), read at ts=15 → visible
	if !IsVisible(10, 0, 0, 15) {
		t.Error("row created at 10 should be visible at 15")
	}
	// read at ts=5 (before creation) → not visible
	if IsVisible(10, 0, 0, 5) {
		t.Error("row created at 10 should not be visible at 5")
	}
}

func TestVisibilityDeleted(t *testing.T) {
	// xmin=10, xmax=20 (deleted at 20), read at ts=15 → visible (deletion not yet committed)
	if !IsVisible(10, 20, 0, 15) {
		t.Error("row deleted at 20 should be visible at 15")
	}
	// read at ts=25 → not visible (deletion committed)
	if IsVisible(10, 20, 0, 25) {
		t.Error("row deleted at 20 should not be visible at 25")
	}
}

func TestVisibilityTombstone(t *testing.T) {
	// Tombstone with deleted flag → never visible regardless of timestamps
	if IsVisible(10, 0, FlagDeleted, 15) {
		t.Error("tombstone should not be visible")
	}
}

func TestVisibilityExactBoundary(t *testing.T) {
	// xmin=10, read at ts=10 → visible (committed exactly at read time)
	if !IsVisible(10, 0, 0, 10) {
		t.Error("row should be visible when readTS == xmin")
	}
	// xmin=10, xmax=20, read at ts=20 → not visible (deletion visible)
	if IsVisible(10, 20, 0, 20) {
		t.Error("row should not be visible when readTS == xmax")
	}
}
