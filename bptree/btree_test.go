package bptree

import (
	"fmt"
	"math/rand"
	"testing"
	"time"
)

func TestInsert100(t *testing.T) {
	tree := New(4)

	for i := 0; i < 100; i++ {
		key := []byte(fmt.Sprintf("key_%03d", i))
		val := []byte(fmt.Sprintf("val_%03d", i))
		tree.Insert(key, val)
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
		expected := fmt.Sprintf("val_%03d", i)
		if string(val) != expected {
			t.Fatalf("key %s: expected value %s, got %s", key, expected, val)
		}
	}

	_, ok := tree.Find([]byte("key_999"))
	if ok {
		t.Fatal("expected key_999 to not exist")
	}
}

func TestInsertReverse100(t *testing.T) {
	tree := New(4)

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
		if !ok {
			t.Fatalf("key %s not found", key)
		}
		if string(val) != fmt.Sprintf("val_%03d", i) {
			t.Fatalf("unexpected value for key %s", key)
		}
	}
}

func TestInsertRandom100(t *testing.T) {
	tree := New(5)
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
		key := []byte(fmt.Sprintf("key_%04d", k))
		val := []byte(fmt.Sprintf("val_%04d", k))
		tree.Insert(key, val)
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
		if !ok {
			t.Fatalf("key %s not found", key)
		}
		if string(val) != fmt.Sprintf("val_%04d", k) {
			t.Fatalf("unexpected value for key %s", key)
		}
	}
}

func TestUpdate(t *testing.T) {
	tree := New(4)

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

func TestRangeScan(t *testing.T) {
	tree := New(4)

	for i := 0; i < 100; i++ {
		key := []byte(fmt.Sprintf("key_%03d", i))
		val := []byte(fmt.Sprintf("val_%03d", i))
		tree.Insert(key, val)
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

func TestDelete(t *testing.T) {
	tree := New(4)

	for i := 0; i < 20; i++ {
		key := []byte(fmt.Sprintf("key_%03d", i))
		val := []byte(fmt.Sprintf("val_%03d", i))
		tree.Insert(key, val)
	}

	for i := 0; i < 20; i++ {
		key := []byte(fmt.Sprintf("key_%03d", i))
		ok := tree.Delete(key)
		if !ok {
			t.Fatalf("failed to delete key %s", key)
		}

		_, found := tree.Find(key)
		if found {
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

func TestDifferentOrders(t *testing.T) {
	for _, order := range []int{3, 4, 5, 6, 10, 20} {
		t.Run(fmt.Sprintf("order_%d", order), func(t *testing.T) {
			tree := New(order)

			for i := 0; i < 100; i++ {
				key := []byte(fmt.Sprintf("key_%03d", i))
				val := []byte(fmt.Sprintf("val_%03d", i))
				tree.Insert(key, val)
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

func TestBulkInsert(t *testing.T) {
	tree := New(1024)

	keys := make([][]byte, 1000000)
	values := make([][]byte, 1000000)
	for i := 0; i < len(keys); i++ {
		keys[i] = []byte(fmt.Sprintf("key_%d", i))
		values[i] = []byte(fmt.Sprintf("val_%d", i))
	}

	start := time.Now()
	for i := 0; i < 1000000; i++ {
		tree.Insert(keys[i], values[i])
	}
	dur := time.Since(start)

	fmt.Printf("time used is %d ms", dur.Milliseconds())

	if !tree.Validate() {
		t.Fatal("tree structure invalid after 100 inserts")
	}

	if tree.Count() != len(keys) {
		t.Fatalf("expected 100 keys, got %d", tree.Count())
	}
}
