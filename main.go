// MiniDB 是一个轻量级的关系型数据库引擎
// 特性：B+树索引、MVCC多版本并发控制、OCC乐观事务、WAL预写日志
package main

import (
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"

	"github.com/prometheus/client_golang/prometheus/promhttp"

	"lns.com/minidb/catalog"
	"lns.com/minidb/metrics"
	"lns.com/minidb/protocol"
	"lns.com/minidb/storage"
	"lns.com/minidb/txn"
	"lns.com/minidb/wal"
)

// 命令行参数
var (
	port                   = flag.Int("port", 3307, "监听端口")
	dataDir                = flag.String("data", "./test/testdb", "数据目录")
	metricsPort            = flag.Int("metrics-port", 2112, "Prometheus监控指标监听端口")
	flushLogAtTrxCommit    = flag.Int("flush-log-at-trx-commit", 1, "WAL刷盘策略: 0=异步写不刷盘(最快), 1=每次提交同步刷盘(最安全,默认), 2=每次提交写OS缓存但不fsync")
)

func printUsage() {
	fmt.Println("minidb - A lightweight relational database engine")
	fmt.Println()
	fmt.Println("Usage: minidb [options]")
	fmt.Println()
	fmt.Println("Options:")
	flag.VisitAll(func(f *flag.Flag) {
		fmt.Printf("  --%s", f.Name)
		if len(f.DefValue) > 0 {
			fmt.Printf(" %s", f.DefValue)
		}
		fmt.Printf("\n      %s\n", f.Usage)
	})
	fmt.Println()
	fmt.Println("Example:")
	fmt.Println("  minidb --port 3307 --data /var/db/minidb")
}

func main() {
	flag.Usage = printUsage
	flag.Parse()

	if flag.NFlag() == 0 && len(flag.Args()) == 0 {
		flag.Usage()
		return
	}

	if flag.Parsed() {
		if flag.Lookup("h") != nil || flag.Lookup("help") != nil {
			flag.Usage()
			return
		}
	}

	// Create data directory.
	if err := os.MkdirAll(*dataDir, 0755); err != nil {
		log.Fatalf("create data dir: %v", err)
	}

	// Open WAL.
	w, err := wal.Open(*dataDir)
	if err != nil {
		log.Fatalf("open WAL: %v", err)
	}
	defer w.Close()

	// Open storage engine.
	engine, err := storage.OpenEngine(*dataDir, 64, 4096)
	if err != nil {
		log.Fatalf("open engine: %v", err)
	}

	// Recover from WAL if needed.
	// Disabled due to deadlock in bptree during concurrent recovery
	maxCommitTS, err := engine.RecoverFromWAL(w)
	if err != nil {
		log.Printf("WAL recovery warning: %v", err)
	}

	// Clean up stale MVCC versions left from previous runs.
	log.Println("Running post-recovery GC...")
	engine.RunFullGC(^uint64(0))
	log.Println("Post-recovery GC done.")

	// Open catalog.
	cat, err := catalog.Open(*dataDir)
	if err != nil {
		log.Fatalf("open catalog: %v", err)
	}

	// Create transaction manager.
	ts := txn.OpenTimestampOracle(*dataDir)
	ts.EnsureAtLeast(maxCommitTS)
	mgr := txn.NewManager(engine, ts, w, *flushLogAtTrxCommit)

	// Start Prometheus metrics server.
	_ = metrics.ActiveConnections // ensure metrics init() runs
	go func() {
		mux := http.NewServeMux()
		mux.Handle("/metrics", promhttp.Handler())
		metricsAddr := fmt.Sprintf("0.0.0.0:%d", *metricsPort)
		log.Printf("Prometheus metrics on %s/metrics", metricsAddr)
		if err := http.ListenAndServe(metricsAddr, mux); err != nil {
			log.Printf("metrics server error: %v", err)
		}
	}()

	// Start server.
	addr := fmt.Sprintf("0.0.0.0:%d", *port)
	svr, err := protocol.NewServer(addr, engine, mgr, cat)
	if err != nil {
		log.Fatalf("create server: %v", err)
	}

	log.Printf("minidb listening on %s", addr)
	log.Printf("Data directory: %s", *dataDir)
	log.Printf("flush-log-at-trx-commit: %d", *flushLogAtTrxCommit)

	// Wait for shutdown signal.
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		if err := svr.Serve(); err != nil {
			log.Printf("server error: %v", err)
		}
	}()

	<-sigCh
	log.Println("shutting down...")
	// Ignore subsequent signals so shutdown can complete.
	signal.Reset(syscall.SIGINT, syscall.SIGTERM)
	svr.Close()
	cat.Close()    // flush catalog first (small, fast)
	engine.Close() // flush all data trees
	w.Truncate()   // data is durable, safe to truncate WAL
}
