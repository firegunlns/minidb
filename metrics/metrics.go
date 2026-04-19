package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
)

// DB buckets: 10us ~ 164ms (exponential, factor 2, 15 buckets)
var dbBuckets = prometheus.ExponentialBuckets(0.00001, 2, 15)

// --- Histograms ---

var (
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
	MVCCScanDuration = prometheus.NewHistogram(prometheus.HistogramOpts{
		Name:    "minidb_mvcc_scan_duration_seconds",
		Help:    "MVCC ScanRange duration",
		Buckets: dbBuckets,
	})
	GCDuration = prometheus.NewHistogram(prometheus.HistogramOpts{
		Name:    "minidb_gc_duration_seconds",
		Help:    "GC pass duration",
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
		MVCCScanDuration,
		GCDuration,
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
		// Gauges
		ActiveConnections,
		CacheSize,
		ActiveTransactions,
	)
}
