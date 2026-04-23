// Package txn 提供事务管理功能
package txn

import (
	"encoding/binary"
	"os"
	"path/filepath"
	"sync/atomic"
)

// TimestampOracle 时间戳oracle
// 分配单调递增的时间戳
// 将计数器持久化到文件，以便重启后恢复
type TimestampOracle struct {
	counter uint64 // 时间戳计数器
	path    string // 持久化文件路径
}

func NewTimestampOracle() *TimestampOracle {
	return &TimestampOracle{}
}

// OpenTimestampOracle creates a TimestampOracle and restores the counter from disk.
func OpenTimestampOracle(dataDir string) *TimestampOracle {
	path := filepath.Join(dataDir, "__timestamp.bin")
	ts := &TimestampOracle{path: path}
	// Restore from file.
	data, err := os.ReadFile(path)
	if err == nil && len(data) >= 8 {
		ts.counter = binary.BigEndian.Uint64(data)
	}
	return ts
}

func (t *TimestampOracle) Next() uint64 {
	v := atomic.AddUint64(&t.counter, 1)
	t.persist()
	return v
}

// Current returns the latest allocated timestamp without incrementing.
func (t *TimestampOracle) Current() uint64 {
	return atomic.LoadUint64(&t.counter)
}

// BeginTS returns a timestamp for starting a new transaction.
// It atomically increments the counter first to ensure each Begin gets a unique timestamp.
func (t *TimestampOracle) BeginTS() uint64 {
	return atomic.AddUint64(&t.counter, 1)
}

// EnsureAtLeast bumps the counter to at least minVal if it's lower.
// Used after WAL recovery to guarantee the oracle is not behind committed data.
func (t *TimestampOracle) EnsureAtLeast(minVal uint64) {
	for {
		cur := atomic.LoadUint64(&t.counter)
		if cur >= minVal {
			return
		}
		if atomic.CompareAndSwapUint64(&t.counter, cur, minVal) {
			t.persist()
			return
		}
	}
}

func (t *TimestampOracle) persist() {
	if t.path == "" {
		return
	}
	var buf [8]byte
	binary.BigEndian.PutUint64(buf[:], atomic.LoadUint64(&t.counter))
	os.WriteFile(t.path, buf[:], 0644)
}
