package bptree

import (
	"fmt"
	"math/rand"
	"path/filepath"
	"testing"
	"time"
)

func TestPInsert100(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.db")

	tree, err := OpenPersistentBPTree(path, 4, 100)
	if err != nil {
		t.Fatal(err)
	}
	defer tree.Close()

	for i := 0; i < 100; i++ {
		key := []byte(fmt.Sprintf("key_%03d", i))
		val := []byte(fmt.Sprintf("val_%03d", i))
		if err := tree.Insert(key, val); err != nil {
			t.Fatal(err)
		}
	}

	if !tree.Validate() {
		t.Fatal("tree structure invalid after 100 inserts")
	}
	if tree.Count() != 100 {
		t.Fatalf("expected 100 keys, got %d", tree.Count())
	}

	for i := 0; i < 100; i++ {
		key := []byte(fmt.Sprintf("key_%03d", i))
		val, ok := tree.Find(key)
		if !ok {
			t.Fatalf("key %s not found", key)
		}
		if string(val) != fmt.Sprintf("val_%03d", i) {
			t.Fatalf("key %s: wrong value", key)
		}
	}

	_, ok := tree.Find([]byte("key_999"))
	if ok {
		t.Fatal("expected key_999 to not exist")
	}
}

func TestPReverse100(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.db")

	tree, err := OpenPersistentBPTree(path, 4, 100)
	if err != nil {
		t.Fatal(err)
	}
	defer tree.Close()

	for i := 99; i >= 0; i-- {
		key := []byte(fmt.Sprintf("key_%03d", i))
		val := []byte(fmt.Sprintf("val_%03d", i))
		tree.Insert(key, val)
	}

	if !tree.Validate() {
		t.Fatal("tree structure invalid after reverse inserts")
	}
	if tree.Count() != 100 {
		t.Fatalf("expected 100 keys, got %d", tree.Count())
	}
	for i := 0; i < 100; i++ {
		key := []byte(fmt.Sprintf("key_%03d", i))
		val, ok := tree.Find(key)
		if !ok || string(val) != fmt.Sprintf("val_%03d", i) {
			t.Fatalf("unexpected value for key %s", key)
		}
	}
}

func TestPUpdate(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.db")

	tree, err := OpenPersistentBPTree(path, 4, 100)
	if err != nil {
		t.Fatal(err)
	}
	defer tree.Close()

	key := []byte("mykey")
	tree.Insert(key, []byte("v1"))
	val, _ := tree.Find(key)
	if string(val) != "v1" {
		t.Fatalf("expected v1, got %s", val)
	}

	tree.Insert(key, []byte("v2"))
	val, _ = tree.Find(key)
	if string(val) != "v2" {
		t.Fatalf("expected v2, got %s", val)
	}
	if tree.Count() != 1 {
		t.Fatalf("expected 1 key after update, got %d", tree.Count())
	}
}

func TestPRangeScan(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.db")

	tree, err := OpenPersistentBPTree(path, 4, 100)
	if err != nil {
		t.Fatal(err)
	}
	defer tree.Close()

	for i := 0; i < 100; i++ {
		tree.Insert([]byte(fmt.Sprintf("key_%03d", i)), []byte(fmt.Sprintf("val_%03d", i)))
	}

	results := tree.RangeScan([]byte("key_020"), []byte("key_029"))
	if len(results) != 10 {
		t.Fatalf("expected 10 results, got %d", len(results))
	}
	for i, kv := range results {
		expected := fmt.Sprintf("key_%03d", 20+i)
		if string(kv.Key) != expected {
			t.Fatalf("expected key %s, got %s", expected, kv.Key)
		}
	}
}

func TestPDelete(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.db")

	tree, err := OpenPersistentBPTree(path, 4, 100)
	if err != nil {
		t.Fatal(err)
	}
	defer tree.Close()

	for i := 0; i < 20; i++ {
		tree.Insert([]byte(fmt.Sprintf("key_%03d", i)), []byte(fmt.Sprintf("val_%03d", i)))
	}

	for i := 0; i < 20; i++ {
		key := []byte(fmt.Sprintf("key_%03d", i))
		if !tree.Delete(key) {
			t.Fatalf("failed to delete key %s", key)
		}
		if _, found := tree.Find(key); found {
			t.Fatalf("key %s still found after deletion", key)
		}
		if !tree.Validate() {
			t.Fatalf("tree invalid after deleting key_%03d", i)
		}
	}
	if tree.Count() != 0 {
		t.Fatalf("expected 0 keys, got %d", tree.Count())
	}
}

func TestPPersistenceAcrossClose(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.db")

	// Create and populate.
	tree, err := OpenPersistentBPTree(path, 4, 100)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 50; i++ {
		tree.Insert([]byte(fmt.Sprintf("key_%03d", i)), []byte(fmt.Sprintf("val_%03d", i)))
	}
	tree.Close()

	// Reopen and verify.
	tree, err = OpenPersistentBPTree(path, 4, 100)
	if err != nil {
		t.Fatal(err)
	}
	defer tree.Close()

	if tree.Count() != 50 {
		t.Fatalf("expected 50 keys after reopen, got %d", tree.Count())
	}
	for i := 0; i < 50; i++ {
		key := []byte(fmt.Sprintf("key_%03d", i))
		val, ok := tree.Find(key)
		if !ok || string(val) != fmt.Sprintf("val_%03d", i) {
			t.Fatalf("key %s not found or wrong value after reopen", key)
		}
	}
}

func TestPPersistenceWithDelete(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.db")

	tree, err := OpenPersistentBPTree(path, 4, 100)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 30; i++ {
		tree.Insert([]byte(fmt.Sprintf("key_%03d", i)), []byte(fmt.Sprintf("val_%03d", i)))
	}
	// Delete some keys.
	for i := 10; i < 20; i++ {
		tree.Delete([]byte(fmt.Sprintf("key_%03d", i)))
	}
	tree.Close()

	// Reopen.
	tree, err = OpenPersistentBPTree(path, 4, 100)
	if err != nil {
		t.Fatal(err)
	}
	defer tree.Close()

	if tree.Count() != 20 {
		t.Fatalf("expected 20 keys, got %d", tree.Count())
	}
	// Deleted keys should be gone.
	for i := 10; i < 20; i++ {
		key := []byte(fmt.Sprintf("key_%03d", i))
		if _, ok := tree.Find(key); ok {
			t.Fatalf("deleted key %s should not exist", key)
		}
	}
	// Remaining keys should still be there.
	for i := 0; i < 10; i++ {
		key := []byte(fmt.Sprintf("key_%03d", i))
		val, ok := tree.Find(key)
		if !ok || string(val) != fmt.Sprintf("val_%03d", i) {
			t.Fatalf("key %s missing after reopen", key)
		}
	}
	for i := 20; i < 30; i++ {
		key := []byte(fmt.Sprintf("key_%03d", i))
		val, ok := tree.Find(key)
		if !ok || string(val) != fmt.Sprintf("val_%03d", i) {
			t.Fatalf("key %s missing after reopen", key)
		}
	}
}

func TestPLRUEviction(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.db")

	// Very small cache — forces frequent eviction.
	tree, err := OpenPersistentBPTree(path, 4, 5)
	if err != nil {
		t.Fatal(err)
	}
	defer tree.Close()

	for i := 0; i < 100; i++ {
		tree.Insert([]byte(fmt.Sprintf("key_%03d", i)), []byte(fmt.Sprintf("val_%03d", i)))
	}

	if !tree.Validate() {
		t.Fatal("tree structure invalid with small cache")
	}
	if tree.Count() != 100 {
		t.Fatalf("expected 100 keys, got %d", tree.Count())
	}

	// Random-access lookups — many will require loading from disk.
	for i := 0; i < 100; i++ {
		key := []byte(fmt.Sprintf("key_%03d", i))
		val, ok := tree.Find(key)
		if !ok || string(val) != fmt.Sprintf("val_%03d", i) {
			t.Fatalf("key %s not found or wrong value after eviction", key)
		}
	}
}

func TestPLRUEvictionWithDelete(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.db")

	tree, err := OpenPersistentBPTree(path, 4, 5)
	if err != nil {
		t.Fatal(err)
	}
	defer tree.Close()

	for i := 0; i < 50; i++ {
		tree.Insert([]byte(fmt.Sprintf("key_%03d", i)), []byte(fmt.Sprintf("val_%03d", i)))
	}

	// Delete half the keys — this also exercises eviction during rebalancing.
	for i := 0; i < 50; i += 2 {
		if !tree.Delete([]byte(fmt.Sprintf("key_%03d", i))) {
			t.Fatalf("failed to delete key_%03d", i)
		}
	}

	if !tree.Validate() {
		t.Fatal("tree invalid after delete with eviction")
	}

	// Verify remaining keys.
	for i := 1; i < 50; i += 2 {
		key := []byte(fmt.Sprintf("key_%03d", i))
		val, ok := tree.Find(key)
		if !ok || string(val) != fmt.Sprintf("val_%03d", i) {
			t.Fatalf("key %s missing", key)
		}
	}
	// Verify deleted keys.
	for i := 0; i < 50; i += 2 {
		if _, ok := tree.Find([]byte(fmt.Sprintf("key_%03d", i))); ok {
			t.Fatalf("deleted key_%03d still present", i)
		}
	}
}

func TestPRandomInsert(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.db")

	tree, err := OpenPersistentBPTree(path, 5, 100)
	if err != nil {
		t.Fatal(err)
	}
	defer tree.Close()

	rng := rand.New(rand.NewSource(42))
	used := make(map[int]bool)
	var keys []int
	for len(keys) < 100 {
		k := rng.Intn(1000)
		if !used[k] {
			used[k] = true
			keys = append(keys, k)
		}
	}

	for _, k := range keys {
		tree.Insert([]byte(fmt.Sprintf("key_%04d", k)), []byte(fmt.Sprintf("val_%04d", k)))
	}

	if !tree.Validate() {
		t.Fatal("tree structure invalid after random inserts")
	}
	if tree.Count() != 100 {
		t.Fatalf("expected 100 keys, got %d", tree.Count())
	}

	for _, k := range keys {
		key := []byte(fmt.Sprintf("key_%04d", k))
		val, ok := tree.Find(key)
		if !ok || string(val) != fmt.Sprintf("val_%04d", k) {
			t.Fatalf("lookup failed for key_%04d", k)
		}
	}
}

func TestPDifferentOrders(t *testing.T) {
	for _, order := range []int{3, 4, 5, 6, 10, 20} {
		name := fmt.Sprintf("order_%d", order)
		t.Run(name, func(t *testing.T) {
			dir := t.TempDir()
			path := filepath.Join(dir, "test.db")

			tree, err := OpenPersistentBPTree(path, order, 100)
			if err != nil {
				t.Fatal(err)
			}
			defer tree.Close()

			for i := 0; i < 100; i++ {
				tree.Insert([]byte(fmt.Sprintf("key_%03d", i)), []byte(fmt.Sprintf("val_%03d", i)))
			}

			if !tree.Validate() {
				t.Fatalf("tree invalid for order %d", order)
			}
			if tree.Count() != 100 {
				t.Fatalf("order %d: expected 100 keys, got %d", order, tree.Count())
			}
			for i := 0; i < 100; i++ {
				key := []byte(fmt.Sprintf("key_%03d", i))
				val, ok := tree.Find(key)
				if !ok || string(val) != fmt.Sprintf("val_%03d", i) {
					t.Fatalf("order %d: lookup failed for key_%03d", order, i)
				}
			}
		})
	}
}

func TestPInsertBulk(t *testing.T) {
	//dir := t.TempDir()
	path := filepath.Join("test.db")

	tree, err := OpenPersistentBPTree(path, 1024, 1024)
	if err != nil {
		t.Fatal(err)
	}
	defer tree.Close()

	key_num := 100000000
	keys := make([][]byte, key_num)
	vals := make([][]byte, key_num)
	for i := range key_num {
		keys[i] = []byte(fmt.Sprintf("key_%06d", i))
		vals[i] = []byte(fmt.Sprintf("val_%06d", i))
	}

	start := time.Now()
	for i := range key_num {
		//key := []byte(fmt.Sprintf("key_%06d", i))
		//val := []byte(fmt.Sprintf("val_%06d", i))
		if err := tree.Insert(keys[i], vals[i]); err != nil {
			t.Fatal(err)
		}

		if (i+1)%10000000 == 0 {
			fmt.Printf("inserted %d keys, time used is %d ms.\n", i+1, time.Since(start).Milliseconds())
		}
	}

	fmt.Printf("time used is %d ms.\n", time.Since(start).Milliseconds())
	tree.Close()

}
