package txn

import (
	"testing"
)

func TestTimestampMonotonic(t *testing.T) {
	ts := NewTimestampOracle()
	v1 := ts.Next()
	v2 := ts.Next()
	v3 := ts.Next()
	if v1 >= v2 || v2 >= v3 {
		t.Errorf("timestamps should be monotonically increasing: %d, %d, %d", v1, v2, v3)
	}
	if v1 != 1 {
		t.Errorf("first timestamp should be 1, got %d", v1)
	}
}

func TestTimestampConcurrent(t *testing.T) {
	ts := NewTimestampOracle()
	n := 1000
	ch := make(chan uint64, n)
	for i := 0; i < n; i++ {
		go func() {
			ch <- ts.Next()
		}()
	}
	seen := make(map[uint64]bool)
	for i := 0; i < n; i++ {
		v := <-ch
		if seen[v] {
			t.Errorf("duplicate timestamp: %d", v)
		}
		seen[v] = true
	}
}
