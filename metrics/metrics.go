// Package metrics 提供Prometheus监控指标
// 用于追踪数据库性能、查询延迟、事务统计等
package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
)

// DB buckets: 10us ~ 164ms (指数分布, 因子2, 15个桶)
var dbBuckets = prometheus.ExponentialBuckets(0.00001, 2, 15)

// --- Histograms ---

var (
	// Protocol layer stage durations.
	RewriteDuration = prometheus.NewHistogram(prometheus.HistogramOpts{
		Name:    "minidb_protocol_rewrite_duration_seconds",
		Help:    "SQL rewrite + routing duration (HandleQuery before Execute)",
		Buckets: dbBuckets,
	})
	ConvertResultDuration = prometheus.NewHistogram(prometheus.HistogramOpts{
		Name:    "minidb_protocol_convert_result_duration_seconds",
		Help:    "Result conversion duration (convertResult)",
		Buckets: dbBuckets,
	})
	HandleQueryDuration = prometheus.NewHistogram(prometheus.HistogramOpts{
		Name:    "minidb_protocol_handle_query_duration_seconds",
		Help:    "Full HandleQuery duration including rewrite + execute + convert",
		Buckets: dbBuckets,
	})
	QueryDuration = prometheus.NewHistogram(prometheus.HistogramOpts{
		Name:    "minidb_query_duration_seconds",
		Help:    "End-to-end query processing duration",
		Buckets: dbBuckets,
	})
	ParseDuration = prometheus.NewHistogram(prometheus.HistogramOpts{
		Name:    "minidb_parse_duration_seconds",
		Help:    "SQL parse duration",
		Buckets: dbBuckets,
	})
	ExecuteDuration = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "minidb_execute_duration_seconds",
		Help:    "Statement execution duration",
		Buckets: dbBuckets,
	}, []string{"stmt"})
	TxnDuration = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "minidb_txn_duration_seconds",
		Help:    "Transaction lifecycle duration",
		Buckets: dbBuckets,
	}, []string{"phase"})
	TxnCommitValidateDuration = prometheus.NewHistogram(prometheus.HistogramOpts{
		Name:    "minidb_txn_commit_validate_duration_seconds",
		Help:    "OCC read-set validation duration",
		Buckets: dbBuckets,
	})
	WALAppendDuration = prometheus.NewHistogram(prometheus.HistogramOpts{
		Name:    "minidb_wal_append_duration_seconds",
		Help:    "WAL append duration",
		Buckets: dbBuckets,
	})
	BPTreeOpDuration = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "minidb_bptree_operation_duration_seconds",
		Help:    "B+ tree operation duration",
		Buckets: dbBuckets,
	}, []string{"op"})
	BPTreeLatchDuration = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "minidb_bptree_latch_duration_seconds",
		Help:    "B+ tree root latch duration",
		Buckets: dbBuckets,
	}, []string{"op"})
	CacheGetOrLoadDuration = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "minidb_cache_get_or_load_duration_seconds",
		Help:    "LRU cache GetOrLoad duration",
		Buckets: dbBuckets,
	}, []string{"hit"})
	CacheEvictDuration = prometheus.NewHistogram(prometheus.HistogramOpts{
		Name:    "minidb_cache_evict_duration_seconds",
		Help:    "LRU cache eviction + write-back duration",
		Buckets: dbBuckets,
	})
	PagerIODuration = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "minidb_pager_io_duration_seconds",
		Help:    "Pager read/write duration",
		Buckets: dbBuckets,
	}, []string{"op"})
	MVCCGetDuration = prometheus.NewHistogram(prometheus.HistogramOpts{
		Name:    "minidb_mvcc_get_duration_seconds",
		Help:    "MVCC GetRow duration",
		Buckets: dbBuckets,
	})
	MVCCGetVerCacheHitDuration = prometheus.NewHistogram(prometheus.HistogramOpts{
		Name:    "minidb_mvcc_get_vercache_hit_duration_seconds",
		Help:    "MVCC GetRow verCache hit path duration",
		Buckets: dbBuckets,
	})
	MVCCGetTreeScanDuration = prometheus.NewHistogram(prometheus.HistogramOpts{
		Name:    "minidb_mvcc_get_treescan_duration_seconds",
		Help:    "MVCC GetRow B+tree scan path duration (verCache miss)",
		Buckets: dbBuckets,
	})
	MVCCScanDuration = prometheus.NewHistogram(prometheus.HistogramOpts{
		Name:    "minidb_mvcc_scan_duration_seconds",
		Help:    "MVCC ScanRange duration",
		Buckets: dbBuckets,
	})
	MVCCScanTreeScanDuration = prometheus.NewHistogram(prometheus.HistogramOpts{
		Name:    "minidb_mvcc_scan_treescan_duration_seconds",
		Help:    "MVCC ScanRange: B+tree RangeScan duration (tree traversal + page loads)",
		Buckets: dbBuckets,
	})
	MVCCScanCallbackDuration = prometheus.NewHistogram(prometheus.HistogramOpts{
		Name:    "minidb_mvcc_scan_callback_duration_seconds",
		Help:    "MVCC ScanRange: callback duration (decode + filter + user fn)",
		Buckets: dbBuckets,
	})
	GCDuration = prometheus.NewHistogram(prometheus.HistogramOpts{
		Name:    "minidb_gc_duration_seconds",
		Help:    "GC pass duration",
		Buckets: dbBuckets,
	})
	// txn.Scan() internal stages.
	TxnScanDuration = prometheus.NewHistogram(prometheus.HistogramOpts{
		Name:    "minidb_txn_scan_duration_seconds",
		Help:    "txn.Scan total duration",
		Buckets: dbBuckets,
	})
	TxnScanWSCollectDuration = prometheus.NewHistogram(prometheus.HistogramOpts{
		Name:    "minidb_txn_scan_ws_collect_duration_seconds",
		Help:    "txn.Scan: workspace collection duration (iterate ws.writes, filter by range)",
		Buckets: dbBuckets,
	})
	TxnScanEngineScanDuration = prometheus.NewHistogram(prometheus.HistogramOpts{
		Name:    "minidb_txn_scan_engine_scan_duration_seconds",
		Help:    "txn.Scan: engine ScanRange/ScanRaw duration (MVCC or index scan)",
		Buckets: dbBuckets,
	})
	TxnScanWSMergeDuration = prometheus.NewHistogram(prometheus.HistogramOpts{
		Name:    "minidb_txn_scan_ws_merge_duration_seconds",
		Help:    "txn.Scan: workspace insert merge duration (unseen ws inserts)",
		Buckets: dbBuckets,
	})
	// execSelectSimple() internal stages.
	SelectSimpleDuration = prometheus.NewHistogram(prometheus.HistogramOpts{
		Name:    "minidb_select_simple_duration_seconds",
		Help:    "execSelectSimple total duration",
		Buckets: dbBuckets,
	})
	SelectResolveDuration = prometheus.NewHistogram(prometheus.HistogramOpts{
		Name:    "minidb_select_resolve_duration_seconds",
		Help:    "execSelectSimple: GetTable + column resolution duration",
		Buckets: dbBuckets,
	})
	SelectOptPathDuration = prometheus.NewHistogram(prometheus.HistogramOpts{
		Name:    "minidb_select_opt_path_duration_seconds",
		Help:    "execSelectSimple: optimization path selection (tryINOnPK, tryIndexScan, extractPKRange)",
		Buckets: dbBuckets,
	})
	SelectScanLoopDuration = prometheus.NewHistogram(prometheus.HistogramOpts{
		Name:    "minidb_select_scan_loop_duration_seconds",
		Help:    "execSelectSimple: scan loop duration (t.Scan + DecodeRow + evalWhere + column projection)",
		Buckets: dbBuckets,
	})
	SelectPostProcessDuration = prometheus.NewHistogram(prometheus.HistogramOpts{
		Name:    "minidb_select_post_process_duration_seconds",
		Help:    "execSelectSimple: sort + limit + result building duration",
		Buckets: dbBuckets,
	})
	// Commit() internal stages.
	TxnCommitWALPrepareDuration = prometheus.NewHistogram(prometheus.HistogramOpts{
		Name:    "minidb_txn_commit_wal_prepare_duration_seconds",
		Help:    "Commit Phase 1+2: WAL append + B+ tree batch prepare duration",
		Buckets: dbBuckets,
	})
	TxnCommitApplyDuration = prometheus.NewHistogram(prometheus.HistogramOpts{
		Name:    "minidb_txn_commit_apply_duration_seconds",
		Help:    "Commit Phase 3: ApplyBatch (B+ tree writes) duration",
		Buckets: dbBuckets,
	})
	TxnCommitWaitFlushDuration = prometheus.NewHistogram(prometheus.HistogramOpts{
		Name:    "minidb_txn_commit_wait_flush_duration_seconds",
		Help:    "Commit: group commit waitFlush duration (WAL Flush+Sync)",
		Buckets: dbBuckets,
	})
)

// --- Counters ---

var (
	QueriesTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "minidb_queries_total",
		Help: "Total number of queries",
	}, []string{"type"})
	TxnCommitsTotal = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "minidb_txn_commits_total",
		Help: "Total number of transaction commits",
	})
	TxnRollbacksTotal = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "minidb_txn_rollbacks_total",
		Help: "Total number of transaction rollbacks",
	})
	TxnConflictsTotal = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "minidb_txn_conflicts_total",
		Help: "Total number of transaction conflicts",
	})
	BPTreeOpsTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "minidb_bptree_operations_total",
		Help: "Total B+ tree operations",
	}, []string{"op"})
	CacheHitsTotal = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "minidb_cache_hits_total",
		Help: "Total cache hits",
	})
	CacheMissesTotal = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "minidb_cache_misses_total",
		Help: "Total cache misses",
	})
	PagerReadsTotal = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "minidb_pager_reads_total",
		Help: "Total pager reads",
	})
	PagerWritesTotal = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "minidb_pager_writes_total",
		Help: "Total pager writes",
	})
	RowsReadTotal = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "minidb_rows_read_total",
		Help: "Total rows read",
	})
	RowsWrittenTotal = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "minidb_rows_written_total",
		Help: "Total rows written",
	})
	GCVersionsRemovedTotal = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "minidb_gc_versions_removed_total",
		Help: "Total MVCC versions removed by GC",
	})
	GCPassesTotal = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "minidb_gc_passes_total",
		Help: "Total number of GC passes",
	})
	TableRowsRead = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "minidb_table_rows_read_total",
		Help: "Rows read per table",
	}, []string{"table"})
	TableScansTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "minidb_table_scans_total",
		Help: "Number of scan operations per table",
	}, []string{"table", "op"})
	IndexScanAttempts = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "minidb_index_scan_attempts_total",
		Help: "Index scan attempts: index_used, index_missed, no_eq, no_idx, coerce_fail",
	}, []string{"result"})

	FullScanDebug = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "minidb_full_scan_debug_total",
		Help: "Full table scan diagnosis: reason=where_nil|no_pk_eq|coerce_fail, table=name",
	}, []string{"table", "reason"})
)

// --- Gauges ---

var (
	ActiveConnections = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "minidb_active_connections",
		Help: "Current number of active connections",
	})
	CacheSize = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "minidb_cache_size",
		Help: "Current LRU cache size",
	})
	ActiveTransactions = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "minidb_active_transactions",
		Help: "Current number of active transactions",
	})
)

func init() {
	prometheus.MustRegister(
		// Histograms
		RewriteDuration,
		ConvertResultDuration,
		HandleQueryDuration,
		QueryDuration,
		ParseDuration,
		ExecuteDuration,
		TxnDuration,
		TxnCommitValidateDuration,
		WALAppendDuration,
		BPTreeOpDuration,
		BPTreeLatchDuration,
		CacheGetOrLoadDuration,
		CacheEvictDuration,
		PagerIODuration,
		MVCCGetDuration,
		MVCCGetVerCacheHitDuration,
		MVCCGetTreeScanDuration,
		MVCCScanDuration,
		MVCCScanTreeScanDuration,
		MVCCScanCallbackDuration,
		GCDuration,
		TxnScanDuration,
		TxnScanWSCollectDuration,
		TxnScanEngineScanDuration,
		TxnScanWSMergeDuration,
		SelectSimpleDuration,
		SelectResolveDuration,
		SelectOptPathDuration,
		SelectScanLoopDuration,
		SelectPostProcessDuration,
		TxnCommitWALPrepareDuration,
		TxnCommitApplyDuration,
		TxnCommitWaitFlushDuration,
		// Counters
		QueriesTotal,
		TxnCommitsTotal,
		TxnRollbacksTotal,
		TxnConflictsTotal,
		BPTreeOpsTotal,
		CacheHitsTotal,
		CacheMissesTotal,
		PagerReadsTotal,
		PagerWritesTotal,
		RowsReadTotal,
		RowsWrittenTotal,
		GCVersionsRemovedTotal,
		GCPassesTotal,
		TableRowsRead,
		TableScansTotal,
		IndexScanAttempts,
		FullScanDebug,
		// Gauges
		ActiveConnections,
		CacheSize,
		ActiveTransactions,
	)
}
