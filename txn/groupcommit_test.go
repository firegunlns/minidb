package txn

import (
	"fmt"
	"sync"
	"sync/atomic"
	"testing"

	"lns.com/minidb/storage"
	"lns.com/minidb/wal"
)

// BenchmarkGroupCommit compares commit throughput with and without group commit.
func BenchmarkGroupCommit(b *testing.B) {
	for _, concurrency := range []int{1, 4, 8, 16} {
		for _, flushMode := range []int{0, 1} {
			name := fmt.Sprintf("concurrency=%d/flushMode=%d", concurrency, flushMode)
			b.Run(name, func(b *testing.B) {
				dir := b.TempDir()
				e, _ := storage.OpenEngine(dir, 64, 4096)
				ts := NewTimestampOracle()
				w, _ := wal.Open(dir)
				mgr := NewManager(e, ts, w, flushMode)

				treeKey := "db__t.db"
				e.OpenTree(treeKey)
				cols := []storage.ColumnDef{
					{Name: "id", Type: storage.ColTypeInt},
					{Name: "v", Type: storage.ColTypeInt},
				}
				pkCols := cols[:1]

				// Pre-insert rows.
				txn0 := mgr.Begin()
				for i := 0; i < 1000; i++ {
					pk := storage.EncodePrimaryKey(pkCols, int32(i))
					row := storage.EncodeRow(cols, []any{int32(i), int32(0)})
					txn0.Insert(treeKey, pk, row)
				}
				txn0.Commit()

				var idx atomic.Int64
				b.ResetTimer()

				var wg sync.WaitGroup
				for g := 0; g < concurrency; g++ {
					wg.Add(1)
					go func() {
						defer wg.Done()
						for {
							i := int(idx.Add(1) - 1)
							if i >= b.N {
								return
							}
							tx := mgr.Begin()
							pk := storage.EncodePrimaryKey(pkCols, int32(i%1000))
							row := storage.EncodeRow(cols, []any{int32(i % 1000), int32(i)})
							tx.Insert(treeKey, pk, row)
							tx.Commit()
						}
					}()
				}
				wg.Wait()

				w.Close()
				e.Close()
			})
		}
	}
}
