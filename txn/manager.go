// Package txn 提供事务管理功能
// 实现MVCC事务机制，无乐观锁验证
package txn

import (
	"errors"
	"sort"
	"strings"
	"sync"
	"time"

	"lns.com/minidb/metrics"
	"lns.com/minidb/storage"
	"lns.com/minidb/wal"
)

var errFinalized = errors.New("transaction is finalized")

// groupCommitter batches WAL fsync calls for concurrent transactions.
//
// When multiple transactions commit concurrently, only the first (leader)
// performs the actual Flush+Sync. All others (followers) block on a condvar
// until the leader finishes. This amortises fsync cost across the group.
//
//	 T1: Append WAL ──→ become leader ──→ Flush ──→ Sync ──→ Broadcast ──→ return
//	 T2: Append WAL ──→ see flushing ──→ Cond.Wait ──────────────────→ return
//	 T3: Append WAL ──→ see flushing ──→ Cond.Wait ──────────────────→ return
//	                                                            ↑ leader done
type groupCommitter struct {
	wal  *wal.WAL
	mu   sync.Mutex
	cond *sync.Cond

	// epoch is incremented after each successful Flush(+Sync).
	// Followers record the epoch on entry and loop until it changes.
	epoch    uint64
	flushing bool

	// syncMode: true = Flush+Sync (flush-log-at-trx-commit=1), false = Flush only (=2)
	syncMode bool
}

func newGroupCommitter(w *wal.WAL, syncMode bool) *groupCommitter {
	gc := &groupCommitter{wal: w, syncMode: syncMode}
	gc.cond = sync.NewCond(&gc.mu)
	return gc
}

// waitFlush joins the current group (or starts a new one) and blocks until
// the WAL has been flushed (+ optionally synced) to disk.
func (gc *groupCommitter) waitFlush() {
	gc.mu.Lock()

	// Fast path: nobody is flushing, become leader.
	if !gc.flushing {
		gc.flushing = true
		gc.mu.Unlock()

		// Leader does the I/O outside the lock.
		gc.wal.Flush()
		if gc.syncMode {
			gc.wal.Sync()
		}

		// Wake up followers.
		gc.mu.Lock()
		gc.epoch++
		gc.flushing = false
		gc.cond.Broadcast()
		gc.mu.Unlock()
		return
	}

	// Slow path: a leader is already flushing — wait for it.
	myEpoch := gc.epoch
	for gc.epoch == myEpoch {
		gc.cond.Wait()
	}
	gc.mu.Unlock()
}

// rowLock provides a per-key mutex for serializing concurrent writes to the same row.
type rowLock struct {
	mu sync.Mutex
}

// rowLockMgr manages row-level write locks.
type rowLockMgr struct {
	locks sync.Map // string -> *rowLock
}

func newRowLockMgr() *rowLockMgr {
	return &rowLockMgr{}
}

// lock acquires write locks for all given keys, in sorted order to prevent deadlocks.
func (r *rowLockMgr) lock(keys []string) {
	sorted := make([]string, len(keys))
	copy(sorted, keys)
	sort.Strings(sorted)
	for _, k := range sorted {
		v, _ := r.locks.LoadOrStore(k, &rowLock{})
		v.(*rowLock).mu.Lock()
	}
}

// unlock releases write locks for all given keys.
func (r *rowLockMgr) unlock(keys []string) {
	for _, k := range keys {
		if v, ok := r.locks.Load(k); ok {
			v.(*rowLock).mu.Unlock()
		}
	}
}

// Txn 单个数据库事务
// 使用MVCC快照隔离：
// 1. 读取时使用startTS快照
// 2. 写操作写入工作空间
// 3. 提交时获取commitTS并应用写入
type Txn struct {
	mgr       *Manager
	startTS   uint64     // 快照时间戳（事务开始时的全局时间）
	commitTS  uint64     // 提交时间戳（分配给提交的事务）
	ws        *Workspace // 工作空间，存储事务的读写集
	finalized bool       // 事务是否已结束
}

// Manager 事务管理器
type Manager struct {
	engine     *storage.StorageEngine
	ts         *TimestampOracle // 时间戳oracle
	wal        *wal.WAL         // WAL日志
	activeMu   sync.Mutex
	activeTxns map[uint64]*Txn // 活跃事务映射
	rowLocks   *rowLockMgr      // 行级写锁

	// flushLogAtCommit WAL刷盘策略（类似innodb_flush_log_at_trx_commit）:
	//   0 = 异步写，不等待刷盘（最快，断电可能丢失最近事务）
	//   1 = 每次提交同步fsync（最安全，默认）—— 使用 group commit
	//   2 = 每次提交等待写入OS页缓存，但不fsync —— 使用 group commit
	flushLogAtCommit int

	// groupCommit batches concurrent Flush+Sync calls. nil when flushLogAtCommit == 0.
	groupCommit *groupCommitter

	// Background GC goroutine.
	gcStopCh chan struct{} // signal to stop
	gcDoneCh chan struct{} // signal that goroutine exited
}

func NewManager(engine *storage.StorageEngine, ts *TimestampOracle, w *wal.WAL, flushLogAtCommit int) *Manager {
	if flushLogAtCommit < 0 || flushLogAtCommit > 2 {
		flushLogAtCommit = 1
	}
	m := &Manager{
		engine:           engine,
		ts:               ts,
		wal:              w,
		activeTxns:       make(map[uint64]*Txn),
		rowLocks:         newRowLockMgr(),
		flushLogAtCommit: flushLogAtCommit,
		gcStopCh:         make(chan struct{}),
		gcDoneCh:         make(chan struct{}),
	}
	if flushLogAtCommit > 0 {
		m.groupCommit = newGroupCommitter(w, flushLogAtCommit == 1)
	}
	go m.backgroundGC()
	return m
}

// Close stops the background GC goroutine.
func (m *Manager) Close() {
	close(m.gcStopCh)
	<-m.gcDoneCh
}

// backgroundGC runs GC continuously when there is work, sleeps when idle.
func (m *Manager) backgroundGC() {
	defer close(m.gcDoneCh)
	for {
		select {
		case <-m.gcStopCh:
			return
		default:
		}
		safeTS := m.MinActiveTS()
		if safeTS > 0 {
			removed := m.engine.RunGC(safeTS)
			if removed > 0 {
				// More work to do — loop immediately.
				continue
			}
		}
		// No work done — sleep briefly before checking again.
		time.Sleep(50 * time.Millisecond)
	}
}

// Begin starts a new transaction with a snapshot timestamp.
func (m *Manager) Begin() *Txn {
	start := time.Now()
	txn := &Txn{
		mgr:     m,
		startTS: m.ts.Current(), // snapshot at current time
		ws:      NewWorkspace(),
	}
	m.activeMu.Lock()
	m.activeTxns[txn.startTS] = txn
	m.activeMu.Unlock()
	metrics.TxnDuration.WithLabelValues("begin").Observe(time.Since(start).Seconds())
	metrics.ActiveTransactions.Inc()
	return txn
}

// Get reads a row, checking the workspace first (read-your-writes),
// then falling through to the storage engine at the snapshot timestamp.
func (t *Txn) Get(treeKey string, cols []storage.ColumnDef, pk []byte) ([]byte, error) {
	if t.finalized {
		return nil, errFinalized
	}

	// Check workspace first.
	if data, ok := t.ws.GetWrite(treeKey, pk); ok {
		if data == nil {
			return nil, nil // deleted in this txn
		}
		return data, nil
	}

	// Read from engine at snapshot time.
	rowData, _, err := t.mgr.engine.GetRow(treeKey, pk, t.startTS)
	return rowData, err
}

// Insert adds a new row to the workspace.
func (t *Txn) Insert(treeKey string, pk []byte, rowData []byte) error {
	if t.finalized {
		return errFinalized
	}
	t.ws.SetWrite(treeKey, pk, rowData)
	t.ws.SetInsert(treeKey, pk)
	return nil
}

// Update buffers an update in the workspace.
func (t *Txn) Update(treeKey string, cols []storage.ColumnDef, pk []byte, newRow []byte) error {
	if t.finalized {
		return errFinalized
	}
	t.ws.SetWrite(treeKey, pk, newRow)
	return nil
}

// Delete buffers a delete in the workspace.
func (t *Txn) Delete(treeKey string, cols []storage.ColumnDef, pk []byte) error {
	if t.finalized {
		return errFinalized
	}
	t.ws.SetDelete(treeKey, pk)
	return nil
}

// Scan iterates over rows in a key range, merging workspace writes with engine data.
func (t *Txn) Scan(treeKey string, cols []storage.ColumnDef, start, end []byte, fn func(pk, row []byte) bool) {
	scanStart := time.Now()
	defer func() {
		metrics.TxnScanDuration.Observe(time.Since(scanStart).Seconds())
	}()

	// Stage 1: Collect workspace writes in the range.
	t0 := time.Now()
	wsResults := make(map[string][]byte)
	t.ws.mu.RLock()
	for key, data := range t.ws.writes {
		// Check if this key belongs to the right tree.
		wsPK, ok := t.wsPKInRange(key, treeKey, start, end)
		if ok {
			wsResults[string(wsPK)] = data
		}
	}
	t.ws.mu.RUnlock()
	metrics.TxnScanWSCollectDuration.Observe(time.Since(t0).Seconds())

	// Stage 2: Scan engine and combine with workspace.
	t1 := time.Now()
	seen := make(map[string]bool)
	isIndex := strings.Contains(treeKey, "__idx__")

	if isIndex {
		// Index trees use raw scan (no MVCC).
		t.mgr.engine.ScanRaw(treeKey, start, end, func(key, value []byte) bool {
			pkStr := string(key)
			seen[pkStr] = true
			if wsData, ok := wsResults[pkStr]; ok {
				if wsData != nil {
					if !fn(key, wsData) {
						return false
					}
				}
				return true
			}
			if !fn(key, value) {
				return false
			}
			return true
		})
	} else {
		// Data trees use MVCC scan.
		t.mgr.engine.ScanRange(treeKey, start, end, t.startTS, func(pk, row []byte) bool {
			pkStr := string(pk)
			seen[pkStr] = true
			// Check if workspace overrides this.
			if wsData, ok := wsResults[pkStr]; ok {
				if wsData != nil {
					if !fn(pk, wsData) {
						return false
					}
				}
				// nil = deleted, skip
				return true
			}
			if !fn(pk, row) {
				return false
			}
			return true
		})
	}
	metrics.TxnScanEngineScanDuration.Observe(time.Since(t1).Seconds())

	// Stage 3: Add workspace inserts that weren't in the engine scan.
	t2 := time.Now()
	for pkStr, data := range wsResults {
		if seen[pkStr] {
			continue
		}
		if data == nil {
			continue // delete of non-existent row
		}
		if !fn([]byte(pkStr), data) {
			break
		}
	}
	metrics.TxnScanWSMergeDuration.Observe(time.Since(t2).Seconds())
}

// Commit writes to WAL and applies writes. No OCC validation.
func (t *Txn) Commit() error {
	if t.finalized {
		return errFinalized
	}
	t.finalized = true
	metrics.ActiveTransactions.Dec()
	t.mgr.activeMu.Lock()
	delete(t.mgr.activeTxns, t.startTS)
	t.mgr.activeMu.Unlock()

	// Fast path: read-only transaction (no writes to apply).
	t.ws.mu.RLock()
	writeCount := len(t.ws.writes)
	if writeCount == 0 {
		t.ws.mu.RUnlock()
		metrics.TxnCommitsTotal.Inc()
		return nil
	}

	// Snapshot writes under lock.
	writeSet := make(map[string][]byte, writeCount)
	writeKeys := make([]string, 0, writeCount)
	for k, v := range t.ws.writes {
		writeSet[k] = v
		writeKeys = append(writeKeys, k)
	}
	inserted := make(map[string]bool, len(t.ws.inserted))
	for k, v := range t.ws.inserted {
		inserted[k] = v
	}
	t.ws.mu.RUnlock()

	// Acquire row-level write locks (sorted to prevent deadlock).
	t.mgr.rowLocks.lock(writeKeys)

	// Allocate commit timestamp.
	t.commitTS = t.mgr.ts.Next()

	// Phase 1: Write WAL records + Phase 2: Prepare B+ tree batches.
	// Combined into a single pass over the write set.
	batches := make(map[string]*storage.TreeWriteBatch, 8)
	for key, rowData := range writeSet {
		treeKey, pk := wsKeyToParts(key)
		isIndex := strings.Contains(treeKey, "__idx__")
		isInserted := inserted[key]

		// WAL record.
		var rec wal.Record
		if rowData == nil && !isInserted {
			rec = wal.DeleteRecord(treeKey, pk, nil)
		} else {
			rec = wal.InsertRecord(treeKey, pk, rowData)
		}
		rec.TxnTS = t.startTS
		rec.CommitTS = t.commitTS
		t.mgr.wal.Append(rec)

		// Prepare B+ tree batch.
		var batch *storage.TreeWriteBatch
		var err error

		if isIndex {
			batch = &storage.TreeWriteBatch{
				TreeKey: treeKey,
				IsIndex: true,
			}
			if rowData == nil && !isInserted {
				batch.DeleteKeys = append(batch.DeleteKeys, pk)
			} else {
				batch.InsertPairs = append(batch.InsertPairs, [2][]byte{pk, rowData})
			}
		} else {
			if rowData == nil {
				batch, err = t.mgr.engine.PrepareDeleteRow(treeKey, pk, t.commitTS)
			} else if isInserted {
				batch, err = t.mgr.engine.PrepareInsertRow(treeKey, pk, t.commitTS, rowData)
			} else {
				batch, err = t.mgr.engine.PrepareUpdateRow(treeKey, pk, t.commitTS, rowData)
			}
			if err != nil {
				t.mgr.rowLocks.unlock(writeKeys)
				return err
			}
			metrics.RowsWrittenTotal.Inc()
		}

		if existing, ok := batches[treeKey]; ok {
			existing.MergeBatch(batch)
		} else {
			batches[treeKey] = batch
		}
	}

	// Phase 3: Apply batch writes sequentially.
	for _, batch := range batches {
		if err := t.mgr.engine.ApplyBatch(batch); err != nil {
			t.mgr.rowLocks.unlock(writeKeys)
			return err
		}
	}

	// Write commit record to WAL.
	commitRec := wal.CommitRecord(t.startTS)
	commitRec.CommitTS = t.commitTS
	t.mgr.wal.Append(commitRec)

	// Flush WAL according to flush-log-at-trx-commit setting.
	// Uses group commit: concurrent transactions share a single Flush(+Sync).
	if t.mgr.groupCommit != nil {
		t.mgr.groupCommit.waitFlush()
	}
	// flushLogAtCommit == 0: no flush, fully async.

	metrics.TxnCommitsTotal.Inc()
	t.mgr.rowLocks.unlock(writeKeys)
	return nil
}

// Rollback discards the transaction.
func (t *Txn) Rollback() {
	if t.finalized {
		return
	}
	t.finalized = true
	metrics.ActiveTransactions.Dec()
	t.mgr.activeMu.Lock()
	delete(t.mgr.activeTxns, t.startTS)
	t.mgr.activeMu.Unlock()
	metrics.TxnRollbacksTotal.Inc()
}

// MinActiveTS returns the minimum startTS of all currently active transactions.
// If no transactions are active, returns the current timestamp.
func (m *Manager) MinActiveTS() uint64 {
	m.activeMu.Lock()
	defer m.activeMu.Unlock()
	if len(m.activeTxns) == 0 {
		return m.ts.Current()
	}
	minTS := uint64(0)
	first := true
	for ts := range m.activeTxns {
		if first || ts < minTS {
			minTS = ts
			first = false
		}
	}
	return minTS
}

// wsPKInRange extracts the PK bytes from a wsKey if it belongs to the given tree
// and the PK is in [start, end). Returns the PK and true if it matches.
func (t *Txn) wsPKInRange(wsKey string, treeKey string, start, end []byte) ([]byte, bool) {
	prefix := treeKey + "\x00"
	if len(wsKey) <= len(prefix) || wsKey[:len(prefix)] != prefix {
		return nil, false
	}
	pk := []byte(wsKey[len(prefix):])
	if CompareBytes(pk, start) < 0 || CompareBytes(pk, end) >= 0 {
		return nil, false
	}
	return pk, true
}

// wsKeyToTree extracts the treeKey from a wsKey.
func wsKeyToTree(key string) string {
	for i := 0; i < len(key); i++ {
		if key[i] == 0 {
			return key[:i]
		}
	}
	return key
}

// wsKeyToParts splits a wsKey into treeKey and pk bytes.
func wsKeyToParts(key string) (string, []byte) {
	for i := 0; i < len(key); i++ {
		if key[i] == 0 {
			return key[:i], []byte(key[i+1:])
		}
	}
	return key, nil
}

// CompareBytes compares two byte slices.
// Needed here since storage.compareKeys is unexported.
func CompareBytes(a, b []byte) int {
	minLen := len(a)
	if len(b) < minLen {
		minLen = len(b)
	}
	for i := 0; i < minLen; i++ {
		if a[i] < b[i] {
			return -1
		}
		if a[i] > b[i] {
			return 1
		}
	}
	if len(a) < len(b) {
		return -1
	}
	if len(a) > len(b) {
		return 1
	}
	return 0
}
