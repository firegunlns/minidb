package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"

	"lns.com/bptree/catalog"
	"lns.com/bptree/protocol"
	"lns.com/bptree/storage"
	"lns.com/bptree/txn"
)

func main() {
	port := flag.Int("port", 3307, "listen port")
	dataDir := flag.String("data", "/tmp/lnsdb", "data directory")
	flag.Parse()

	// Create data directory.
	if err := os.MkdirAll(*dataDir, 0755); err != nil {
		log.Fatalf("create data dir: %v", err)
	}

	// Open storage engine.
	engine, err := storage.OpenEngine(*dataDir, 64, 256)
	if err != nil {
		log.Fatalf("open engine: %v", err)
	}

	// Open catalog.
	cat, err := catalog.Open(*dataDir)
	if err != nil {
		log.Fatalf("open catalog: %v", err)
	}

	// Create transaction manager.
	ts := txn.OpenTimestampOracle(*dataDir)
	mgr := txn.NewManager(engine, ts)

	// Start server.
	addr := fmt.Sprintf("0.0.0.0:%d", *port)
	svr, err := protocol.NewServer(addr, engine, mgr, cat)
	if err != nil {
		log.Fatalf("create server: %v", err)
	}

	log.Printf("LnsDB listening on %s", addr)
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
	cat.Close()
	engine.Close()
}
