package bptree

import (
	"bytes"
	"fmt"
	"path/filepath"
	"sync"
	"testing"
)

// ---------- Test 1: Concurrent Reads ----------

func TestConcurrentReads(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.db")

	tree, err := OpenPersistentBPTree(path, 4, 100)
	if err != nil {
		t.Fatal(err)
	}
	defer tree.Close()

	const n = 1000
	for i := 0; i < n; i++ {
		tree.Insert([]byte(fmt.Sprintf("key_%04d", i)), []byte(fmt.Sprintf("val_%04d", i)))
	}

	var wg sync.WaitGroup
	const goroutines = 8
	const opsPerGoroutine = 1000

	for g := 0; g < goroutines; g++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for i := 0; i < opsPerGoroutine; i++ {
				k := []byte(fmt.Sprintf("key_%04d", i%n))
				v, ok := tree.Find(k)
				if !ok {
					t.Errorf("goroutine %d: key %s not found", id, k)
					return
				}
				expected := fmt.Sprintf("val_%04d", i%n)
				if string(v) != expected {
					t.Errorf("goroutine %d: key %s: got %s, want %s", id, k, v, expected)
					return
				}
			}
		}(g)
	}
	wg.Wait()

	if !tree.Validate() {
		t.Fatal("tree invalid after concurrent reads")
	}
}

// ---------- Test 2: Concurrent Inserts (disjoint ranges) ----------

func TestConcurrentInserts(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.db")

	tree, err := OpenPersistentBPTree(path, 4, 200)
	if err != nil {
		t.Fatal(err)
	}
	defer tree.Close()

	const goroutines = 8
	const keysPerGoroutine = 100

	var wg sync.WaitGroup
	for g := 0; g < goroutines; g++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for i := 0; i < keysPerGoroutine; i++ {
				k := []byte(fmt.Sprintf("g%d_key_%04d", id, i))
				v := []byte(fmt.Sprintf("g%d_val_%04d", id, i))
				if err := tree.Insert(k, v); err != nil {
					t.Errorf("goroutine %d: insert error: %v", id, err)
					return
				}
			}
		}(g)
	}
	wg.Wait()

	expected := goroutines * keysPerGoroutine
	if tree.Count() != expected {
		t.Fatalf("expected %d keys, got %d", expected, tree.Count())
	}
	if !tree.Validate() {
		t.Fatal("tree invalid after concurrent inserts")
	}
}

// ---------- Test 3: Concurrent Insert Overlap ----------

func TestConcurrentInsertOverlap(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.db")

	tree, err := OpenPersistentBPTree(path, 4, 100)
	if err != nil {
		t.Fatal(err)
	}
	defer tree.Close()

	const goroutines = 4
	const keys = 100

	var wg sync.WaitGroup
	for g := 0; g < goroutines; g++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for i := 0; i < keys; i++ {
				k := []byte(fmt.Sprintf("key_%04d", i))
				v := []byte(fmt.Sprintf("val_g%d_%04d", id, i))
				if err := tree.Insert(k, v); err != nil {
					t.Errorf("goroutine %d: insert error: %v", id, err)
					return
				}
			}
		}(g)
	}
	wg.Wait()

	if tree.Count() != keys {
		t.Fatalf("expected %d keys, got %d", keys, tree.Count())
	}
	if !tree.Validate() {
		t.Fatal("tree invalid after overlapping inserts")
	}

	// All keys should be present.
	for i := 0; i < keys; i++ {
		k := []byte(fmt.Sprintf("key_%04d", i))
		if _, ok := tree.Find(k); !ok {
			t.Fatalf("key_%04d not found", i)
		}
	}
}

// ---------- Test 4: Concurrent Deletes ----------

func TestConcurrentDeletes(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.db")

	tree, err := OpenPersistentBPTree(path, 4, 200)
	if err != nil {
		t.Fatal(err)
	}
	defer tree.Close()

	const total = 800
	for i := 0; i < total; i++ {
		tree.Insert([]byte(fmt.Sprintf("key_%04d", i)), []byte(fmt.Sprintf("val_%04d", i)))
	}

	const goroutines = 8
	keysPerG := total / goroutines

	var wg sync.WaitGroup
	for g := 0; g < goroutines; g++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			start := id * keysPerG
			for i := start; i < start+keysPerG; i++ {
				k := []byte(fmt.Sprintf("key_%04d", i))
				if !tree.Delete(k) {
					t.Errorf("goroutine %d: failed to delete key_%04d", id, i)
					return
				}
			}
		}(g)
	}
	wg.Wait()

	if tree.Count() != 0 {
		t.Fatalf("expected 0 keys, got %d", tree.Count())
	}
	if !tree.Validate() {
		t.Fatal("tree invalid after concurrent deletes")
	}
}

// ---------- Test 5: Concurrent Insert + Delete ----------

func TestConcurrentInsertDelete(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.db")

	tree, err := OpenPersistentBPTree(path, 4, 200)
	if err != nil {
		t.Fatal(err)
	}
	defer tree.Close()

	// Pre-populate.
	const preFill = 500
	for i := 0; i < preFill; i++ {
		tree.Insert([]byte(fmt.Sprintf("key_%04d", i)), []byte(fmt.Sprintf("val_%04d", i)))
	}

	var wg sync.WaitGroup

	// Inserter goroutines: insert keys 500-999.
	const inserters = 4
	const insertKeys = 125
	for g := 0; g < inserters; g++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			start := 500 + id*insertKeys
			for i := start; i < start+insertKeys; i++ {
				tree.Insert([]byte(fmt.Sprintf("key_%04d", i)), []byte(fmt.Sprintf("val_%04d", i)))
			}
		}(g)
	}

	// Deleter goroutines: delete keys 0-499.
	const deleters = 4
	deleteKeys := preFill / deleters
	for g := 0; g < deleters; g++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			start := id * deleteKeys
			for i := start; i < start+deleteKeys; i++ {
				tree.Delete([]byte(fmt.Sprintf("key_%04d", i)))
			}
		}(g)
	}

	wg.Wait()

	if !tree.Validate() {
		t.Fatal("tree invalid after concurrent insert+delete")
	}

	// Deleted keys should be gone.
	for i := 0; i < preFill; i++ {
		k := []byte(fmt.Sprintf("key_%04d", i))
		if _, ok := tree.Find(k); ok {
			t.Fatalf("deleted key_%04d still present", i)
		}
	}
	// Inserted keys should be present.
	for i := 500; i < 1000; i++ {
		k := []byte(fmt.Sprintf("key_%04d", i))
		v, ok := tree.Find(k)
		if !ok {
			t.Fatalf("inserted key_%04d not found", i)
		}
		expected := fmt.Sprintf("val_%04d", i)
		if string(v) != expected {
			t.Fatalf("key_%04d: got %s, want %s", i, v, expected)
		}
	}
}

// ---------- Test 6: Concurrent RangeScan + Insert ----------

func TestConcurrentRangeScan(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.db")

	tree, err := OpenPersistentBPTree(path, 4, 200)
	if err != nil {
		t.Fatal(err)
	}
	defer tree.Close()

	for i := 0; i < 500; i++ {
		tree.Insert([]byte(fmt.Sprintf("key_%04d", i)), []byte(fmt.Sprintf("val_%04d", i)))
	}

	var wg sync.WaitGroup

	// Range scanners.
	for g := 0; g < 4; g++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for i := 0; i < 50; i++ {
				start := []byte(fmt.Sprintf("key_%03d0", i%50))
				end := []byte(fmt.Sprintf("key_%03d9", i%50))
				results := tree.RangeScan(start, end)
				for _, kv := range results {
					if bytes.Compare(kv.Key, start) < 0 || bytes.Compare(kv.Key, end) > 0 {
						t.Errorf("goroutine %d: key %s out of range [%s, %s]", id, kv.Key, start, end)
						return
					}
				}
			}
		}(g)
	}

	// Concurrent inserter.
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 500; i < 600; i++ {
			tree.Insert([]byte(fmt.Sprintf("key_%04d", i)), []byte(fmt.Sprintf("val_%04d", i)))
		}
	}()

	wg.Wait()

	if !tree.Validate() {
		t.Fatal("tree invalid after concurrent rangescan+insert")
	}
}

// ---------- Test 7: Cache Pinning Under Concurrency ----------

func TestCachePinningUnderConcurrency(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.db")

	// Very small cache to force frequent eviction.
	tree, err := OpenPersistentBPTree(path, 4, 5)
	if err != nil {
		t.Fatal(err)
	}
	defer tree.Close()

	var wg sync.WaitGroup
	for g := 0; g < 4; g++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for i := 0; i < 50; i++ {
				k := []byte(fmt.Sprintf("g%d_k%04d", id, i))
				tree.Insert(k, []byte(fmt.Sprintf("v%d_%04d", id, i)))
			}
		}(g)
	}
	wg.Wait()

	if !tree.Validate() {
		t.Fatal("tree invalid after concurrent ops with small cache")
	}

	// Verify all keys.
	for g := 0; g < 4; g++ {
		for i := 0; i < 50; i++ {
			k := []byte(fmt.Sprintf("g%d_k%04d", g, i))
			v, ok := tree.Find(k)
			if !ok {
				t.Fatalf("key %s not found", k)
			}
			expected := fmt.Sprintf("v%d_%04d", g, i)
			if string(v) != expected {
				t.Fatalf("key %s: got %s, want %s", k, v, expected)
			}
		}
	}
}

// ---------- Test 8: Read/Write Mix ----------

func TestConcurrentReadWriteMix(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.db")

	tree, err := OpenPersistentBPTree(path, 8, 200)
	if err != nil {
		t.Fatal(err)
	}
	defer tree.Close()

	const n = 500
	for i := 0; i < n; i++ {
		tree.Insert([]byte(fmt.Sprintf("key_%04d", i)), []byte(fmt.Sprintf("val_%04d", i)))
	}

	var wg sync.WaitGroup

	// 4 readers.
	for g := 0; g < 4; g++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for i := 0; i < 500; i++ {
				k := []byte(fmt.Sprintf("key_%04d", i%n))
				tree.Find(k)
			}
		}(g)
	}

	// 2 writers (updating existing keys).
	for g := 0; g < 2; g++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for i := 0; i < 200; i++ {
				k := []byte(fmt.Sprintf("key_%04d", i%n))
				v := []byte(fmt.Sprintf("newval_g%d_%04d", id, i))
				tree.Insert(k, v)
			}
		}(g)
	}

	wg.Wait()

	if !tree.Validate() {
		t.Fatal("tree invalid after concurrent read/write mix")
	}
	if tree.Count() != n {
		t.Fatalf("expected %d keys, got %d", n, tree.Count())
	}
}

// ---------- Test 9: Persistence After Concurrent Ops ----------

func TestPersistenceAfterConcurrentOps(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.db")

	tree, err := OpenPersistentBPTree(path, 4, 100)
	if err != nil {
		t.Fatal(err)
	}

	const n = 500
	var wg sync.WaitGroup
	for g := 0; g < 5; g++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for i := 0; i < n/5; i++ {
				k := []byte(fmt.Sprintf("key_%04d", id*(n/5)+i))
				v := []byte(fmt.Sprintf("val_%04d", id*(n/5)+i))
				tree.Insert(k, v)
			}
		}(g)
	}
	wg.Wait()
	tree.Close()

	// Reopen and verify.
	tree, err = OpenPersistentBPTree(path, 4, 100)
	if err != nil {
		t.Fatal(err)
	}
	defer tree.Close()

	if tree.Count() != n {
		t.Fatalf("expected %d keys after reopen, got %d", n, tree.Count())
	}
	for i := 0; i < n; i++ {
		k := []byte(fmt.Sprintf("key_%04d", i))
		v, ok := tree.Find(k)
		if !ok || string(v) != fmt.Sprintf("val_%04d", i) {
			t.Fatalf("key_%04d missing or wrong after reopen", i)
		}
	}
	if !tree.Validate() {
		t.Fatal("tree invalid after reopen")
	}
}
