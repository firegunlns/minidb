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

var (
	port        = flag.Int("port", 3307, "listen port")
	dataDir     = flag.String("data", "./test/testdb", "data directory")
	metricsPort = flag.Int("metrics-port", 2112, "Prometheus metrics listen port")
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
	if err := engine.RecoverFromWAL(w); err != nil {
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
	mgr := txn.NewManager(engine, ts, w)

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
	svr.Close()
	engine.Close() // flush all trees first
	w.Truncate()   // data is durable, safe to truncate WAL
	cat.Close()
}
