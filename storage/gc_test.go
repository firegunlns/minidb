package storage

import (
	"os"
	"path/filepath"
	"testing"
)

func setupGCEnv(t *testing.T) (*StorageEngine, string) {
	t.Helper()
	dir := filepath.Join(os.TempDir(), "minidb-gc-test")
	os.RemoveAll(dir)
	os.MkdirAll(dir, 0755)
	t.Cleanup(func() { os.RemoveAll(dir) })

	engine, err := OpenEngine(dir, 64, 256)
	if err != nil {
		t.Fatalf("OpenEngine: %v", err)
	}
	t.Cleanup(func() { engine.Close() })
	return engine, dir
}

func TestGCSupersededVersion(t *testing.T) {
	engine, _ := setupGCEnv(t)
	treeKey := "test__t.db"
	if err := engine.OpenTree(treeKey); err != nil {
		t.Fatal(err)
	}

	pk := []byte("pk1")

	// Insert at ts=10.
	if err := engine.InsertRow(treeKey, pk, 10, []byte("v1")); err != nil {
		t.Fatal(err)
	}

	// Update at ts=20 — new version inserted, PK marked dirty.
	if err := engine.UpdateRow(treeKey, pk, 20, []byte("v2")); err != nil {
		t.Fatal(err)
	}

	// Verify 2 versions exist.
	tree := engine.getTree(treeKey)
	start, end := ScanRangeForPK(pk)
	kvs := tree.RangeScan(start, end)
	if len(kvs) != 2 {
		t.Fatalf("expected 2 versions before GC, got %d", len(kvs))
	}

	// GC with safeTS=30 — the old version (xmin=10 < 30) should be removed.
	engine.RunGC(30)

	// Verify only 1 version remains.
	kvs = tree.RangeScan(start, end)
	if len(kvs) != 1 {
		t.Fatalf("expected 1 version after GC, got %d", len(kvs))
	}
	xmin, xmax, flags, data, _ := DecodeMVCCValue(kvs[0].Value)
	if string(data) != "v2" {
		t.Fatalf("expected v2 to remain, got %s", data)
	}
	if xmin != 20 || xmax != 0 || flags != 0 {
		t.Fatalf("unexpected MVCC metadata: xmin=%d xmax=%d flags=%d", xmin, xmax, flags)
	}
}

func TestGCTombstone(t *testing.T) {
	engine, _ := setupGCEnv(t)
	treeKey := "test__t.db"
	if err := engine.OpenTree(treeKey); err != nil {
		t.Fatal(err)
	}

	pk := []byte("pk1")

	// Insert at ts=10.
	if err := engine.InsertRow(treeKey, pk, 10, []byte("v1")); err != nil {
		t.Fatal(err)
	}

	// Delete at ts=20 — tombstone inserted, PK marked dirty.
	if err := engine.DeleteRow(treeKey, pk, 20); err != nil {
		t.Fatal(err)
	}

	tree := engine.getTree(treeKey)
	start, end := ScanRangeForPK(pk)
	kvs := tree.RangeScan(start, end)
	if len(kvs) != 2 {
		t.Fatalf("expected 2 versions before GC, got %d", len(kvs))
	}

	// GC with safeTS=30 — old version (xmin=10 < 30) is eligible.
	// Tombstone (newest version) is kept.
	// But we must keep at least one version.
	engine.RunGC(30)

	// One version should remain (the tombstone).
	kvs = tree.RangeScan(start, end)
	if len(kvs) != 1 {
		t.Fatalf("expected 1 version after GC, got %d", len(kvs))
	}
	_, _, flags, _, _ := DecodeMVCCValue(kvs[0].Value)
	if flags&FlagDeleted == 0 {
		t.Fatal("expected tombstone to remain")
	}
}

func TestGCKeepsLiveVersion(t *testing.T) {
	engine, _ := setupGCEnv(t)
	treeKey := "test__t.db"
	if err := engine.OpenTree(treeKey); err != nil {
		t.Fatal(err)
	}

	pk := []byte("pk1")

	// Insert at ts=10.
	if err := engine.InsertRow(treeKey, pk, 10, []byte("v1")); err != nil {
		t.Fatal(err)
	}

	// Update at ts=20 — new version inserted, PK marked dirty.
	if err := engine.UpdateRow(treeKey, pk, 20, []byte("v2")); err != nil {
		t.Fatal(err)
	}

	// GC with safeTS=15 — version 2 (xmin=20) is NOT universally visible (20 > 15),
	// so version 1 is still needed. Both versions are kept.
	engine.RunGC(15)

	tree := engine.getTree(treeKey)
	start, end := ScanRangeForPK(pk)
	kvs := tree.RangeScan(start, end)
	if len(kvs) != 2 {
		t.Fatalf("expected 2 versions (version 2 not universally visible yet), got %d", len(kvs))
	}

	// GC with safeTS=25 — version 2 (xmin=20) IS universally visible (20 < 25),
	// so version 1 can be removed.
	engine.RunGC(25)

	kvs = tree.RangeScan(start, end)
	if len(kvs) != 1 {
		t.Fatalf("expected 1 version (old version removed), got %d", len(kvs))
	}
}

func TestGCNeverRemovesAllVersions(t *testing.T) {
	engine, _ := setupGCEnv(t)
	treeKey := "test__t.db"
	if err := engine.OpenTree(treeKey); err != nil {
		t.Fatal(err)
	}

	pk := []byte("pk1")

	// Insert one version at ts=10. InsertRow does NOT mark dirty,
	// so RunGC won't even look at this PK. But even if it did,
	// a single version should be kept.
	if err := engine.InsertRow(treeKey, pk, 10, []byte("v1")); err != nil {
		t.Fatal(err)
	}

	// GC with high safeTS — nothing is dirty, so nothing removed.
	engine.RunGC(100)

	tree := engine.getTree(treeKey)
	start, end := ScanRangeForPK(pk)
	kvs := tree.RangeScan(start, end)
	if len(kvs) != 1 {
		t.Fatalf("expected 1 version (kept), got %d", len(kvs))
	}
}

func TestGCBoundedByLimit(t *testing.T) {
	engine, _ := setupGCEnv(t)
	treeKey := "test__t.db"
	if err := engine.OpenTree(treeKey); err != nil {
		t.Fatal(err)
	}

	// Create 5 PKs, each with 2 versions (insert + update).
	// Each update marks the PK dirty.
	for i := 0; i < 5; i++ {
		pk := []byte{byte(i)}
		if err := engine.InsertRow(treeKey, pk, 10, []byte("v1")); err != nil {
			t.Fatal(err)
		}
		if err := engine.UpdateRow(treeKey, pk, 20, []byte("v2")); err != nil {
			t.Fatal(err)
		}
	}

	// vacuumDirtyPKs with limit=2 — should remove at most 2 versions.
	removed := engine.vacuumDirtyPKs(30, 2)
	if removed > 2 {
		t.Fatalf("expected at most 2 removed, got %d", removed)
	}
	if removed == 0 {
		t.Fatal("expected at least 1 removed")
	}
}

func TestGCMultipleUpdates(t *testing.T) {
	engine, _ := setupGCEnv(t)
	treeKey := "test__t.db"
	if err := engine.OpenTree(treeKey); err != nil {
		t.Fatal(err)
	}

	pk := []byte("pk1")

	// Insert at ts=10, update at ts=20, update at ts=30.
	if err := engine.InsertRow(treeKey, pk, 10, []byte("v1")); err != nil {
		t.Fatal(err)
	}
	if err := engine.UpdateRow(treeKey, pk, 20, []byte("v2")); err != nil {
		t.Fatal(err)
	}
	if err := engine.UpdateRow(treeKey, pk, 30, []byte("v3")); err != nil {
		t.Fatal(err)
	}

	tree := engine.getTree(treeKey)
	start, end := ScanRangeForPK(pk)
	kvs := tree.RangeScan(start, end)
	if len(kvs) != 3 {
		t.Fatalf("expected 3 versions before GC, got %d", len(kvs))
	}

	// GC with safeTS=40 — older versions with xmin < 40 are eligible.
	// v1 (xmin=10) and v2 (xmin=20) should be removed.
	// v3 (xmin=30, newest) is kept.
	engine.RunGC(40)

	kvs = tree.RangeScan(start, end)
	if len(kvs) != 1 {
		t.Fatalf("expected 1 version after GC, got %d", len(kvs))
	}
	_, _, _, data, _ := DecodeMVCCValue(kvs[0].Value)
	if string(data) != "v3" {
		t.Fatalf("expected v3 to remain, got %s", data)
	}
}

func TestGCOnlyDirtyPKs(t *testing.T) {
	engine, _ := setupGCEnv(t)
	treeKey := "test__t.db"
	if err := engine.OpenTree(treeKey); err != nil {
		t.Fatal(err)
	}

	pkDirty := []byte("pk_dirty")
	pkClean := []byte("pk_clean")

	// Both PKs get insert + update.
	if err := engine.InsertRow(treeKey, pkDirty, 10, []byte("v1")); err != nil {
		t.Fatal(err)
	}
	if err := engine.UpdateRow(treeKey, pkDirty, 20, []byte("v2")); err != nil {
		t.Fatal(err)
	}
	if err := engine.InsertRow(treeKey, pkClean, 10, []byte("v1")); err != nil {
		t.Fatal(err)
	}
	if err := engine.UpdateRow(treeKey, pkClean, 20, []byte("v2")); err != nil {
		t.Fatal(err)
	}

	// Manually clear dirty set, then mark only pkDirty.
	engine.dirtyMu.Lock()
	engine.dirtyPKs = map[string]map[string]struct{}{
		treeKey: {string(pkDirty): {}},
	}
	engine.dirtyMu.Unlock()

	// GC should only clean pkDirty, not pkClean.
	engine.RunGC(30)

	tree := engine.getTree(treeKey)

	// pkDirty should have 1 version left.
	start, end := ScanRangeForPK(pkDirty)
	kvs := tree.RangeScan(start, end)
	if len(kvs) != 1 {
		t.Fatalf("expected 1 version for pkDirty after GC, got %d", len(kvs))
	}

	// pkClean should still have 2 versions (was not in dirty set).
	start, end = ScanRangeForPK(pkClean)
	kvs = tree.RangeScan(start, end)
	if len(kvs) != 2 {
		t.Fatalf("expected 2 versions for pkClean (not dirty), got %d", len(kvs))
	}
}

func TestGCReaddsUnprocessedPKs(t *testing.T) {
	engine, _ := setupGCEnv(t)
	treeKey := "test__t.db"
	if err := engine.OpenTree(treeKey); err != nil {
		t.Fatal(err)
	}

	// Create 3 dirty PKs.
	for i := 0; i < 3; i++ {
		pk := []byte{byte(i)}
		if err := engine.InsertRow(treeKey, pk, 10, []byte("v1")); err != nil {
			t.Fatal(err)
		}
		if err := engine.UpdateRow(treeKey, pk, 20, []byte("v2")); err != nil {
			t.Fatal(err)
		}
	}

	// GC with limit=1 — only 1 version removed, remaining PKs re-added.
	removed := engine.vacuumDirtyPKs(30, 1)
	if removed != 1 {
		t.Fatalf("expected exactly 1 removed, got %d", removed)
	}

	// Dirty set should have remaining PKs.
	engine.dirtyMu.Lock()
	remaining := len(engine.dirtyPKs[treeKey])
	engine.dirtyMu.Unlock()
	if remaining == 0 {
		t.Fatal("expected remaining PKs to be re-added to dirty set")
	}

	// Second GC pass should clean the rest.
	engine.RunGC(30)

	// All versions should now be cleaned up — each PK has exactly 1 version.
	tree := engine.getTree(treeKey)
	for i := 0; i < 3; i++ {
		pk := []byte{byte(i)}
		start, end := ScanRangeForPK(pk)
		kvs := tree.RangeScan(start, end)
		if len(kvs) != 1 {
			t.Fatalf("pk %d: expected 1 version after full GC, got %d", i, len(kvs))
		}
	}
}
