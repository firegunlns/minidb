package main

import (
	"flag"
	"fmt"
	"math"
	"math/rand"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"lns.com/minidb/bptree"
)

var (
	flagNumKeys   = flag.Int("num_keys", 10000000, "Number of keys to load")
	flagKeySize   = flag.Int("key_size", 16, "Key size in bytes")
	flagValueSize = flag.Int("value_size", 100, "Value size in bytes")
	flagOrder     = flag.Int("order", 0, "B+ tree order (0=auto-calculate for 32KB page)")
	flagCacheSize = flag.Int("cache", 1024, "LRU cache size (pages)")
	flagDBPath    = flag.String("db", "", "Database file path (default: temp)")
	flagReadOps   = flag.Int("read_ops", 1000000, "Number of read operations per test")
	flagWriteOps  = flag.Int("write_ops", 1000000, "Number of write operations per test")
	flagSkipLoad  = flag.Bool("skip_load", false, "Skip bulk load, use existing DB")
	flagTests     = flag.String("tests", "all", "Comma-separated list of tests to run (or 'all')")
	flagBloom     = flag.Bool("bloom", false, "Enable per-leaf bloom filters")
	flagBloomBits = flag.Int("bloom_bits", 10, "Bloom filter bits per key")
	flagCompress  = flag.Bool("compress", false, "Enable snappy compression")
)

const pageSize = 32768

type result struct {
	name    string
	ops     int
	elapsed time.Duration
	opsSec  float64
	mbSec   float64
	avgUsec float64
	p50     float64
	p75     float64
	p99     float64
	p999    float64
	p9999   float64
}

type rocksdbRef struct {
	name                                               string
	opsSec, mbSec, avgUsec, p50, p75, p99, p999, p9999 float64
}

var refs = map[string]rocksdbRef{
	"bulkload": {
		name:   "RocksDB 7.2 Bulk Load*",
		opsSec: 1003732, mbSec: 402.0, avgUsec: 1.0,
		p50: 0.5, p75: 0.8, p99: 2.0, p999: 7.0, p9999: 22.0,
	},
	"random_read": {
		name:   "RocksDB 7.2 Random Read*",
		opsSec: 136915, mbSec: 34.7, avgUsec: 467.4,
		p50: 615.5, p75: 772.8, p99: 1270.0, p999: 1801.0, p9999: 2840.0,
	},
	"overwrite": {
		name:   "RocksDB 7.2 Overwrite*",
		opsSec: 86617, mbSec: 34.7, avgUsec: 738.9,
		p50: 449.7, p75: 777.6, p99: 10479.0, p999: 30005.0, p9999: 58328.0,
	},
	"range_scan": {
		name:   "RocksDB 7.2 Range Scan*",
		opsSec: 70097, mbSec: 280.8, avgUsec: 913.0,
		p50: 791.9, p75: 1435.5, p99: 1892.0, p999: 2811.0, p9999: 10210.0,
	},
	"read_while_write": {
		name:   "RocksDB 7.2 ReadWhileWriting*",
		opsSec: 98240, mbSec: 31.1, avgUsec: 651.4,
		p50: 600.6, p75: 829.8, p99: 3963.0, p999: 6041.0, p9999: 10139.0,
	},
}

func calcMaxOrder(keySize, valueSize int) int {
	const dataPrefix = 4
	const nodeHeader = 19
	kvOverhead := 2 + keySize + 2 + valueSize
	return (pageSize - dataPrefix - nodeHeader) / kvOverhead
}

func calcPercentiles(lats []float64) (p50, p75, p99, p999, p9999 float64) {
	if len(lats) == 0 {
		return
	}
	s := make([]float64, len(lats))
	copy(s, lats)
	sort.Float64s(s)
	p := func(pct float64) float64 {
		i := int(math.Ceil(pct/100.0*float64(len(s)))) - 1
		if i < 0 {
			i = 0
		}
		if i >= len(s) {
			i = len(s) - 1
		}
		return s[i]
	}
	return p(50), p(75), p(99), p(99.9), p(99.99)
}

func makeResult(name string, ops int, elapsed time.Duration, lats []float64) *result {
	bpOp := float64(*flagKeySize+*flagValueSize) / (1024.0 * 1024.0)
	opsSec := float64(ops) / elapsed.Seconds()
	p50, p75, p99, p999, p9999 := calcPercentiles(lats)
	return &result{
		name: name, ops: ops, elapsed: elapsed,
		opsSec: opsSec, mbSec: opsSec * bpOp,
		avgUsec: float64(elapsed.Microseconds()) / float64(ops),
		p50:     p50, p75: p75, p99: p99, p999: p999, p9999: p9999,
	}
}

func makeResultNoLat(name string, ops int, elapsed time.Duration) *result {
	bpOp := float64(*flagKeySize+*flagValueSize) / (1024.0 * 1024.0)
	opsSec := float64(ops) / elapsed.Seconds()
	return &result{
		name: name, ops: ops, elapsed: elapsed,
		opsSec: opsSec, mbSec: opsSec * bpOp,
		avgUsec: float64(elapsed.Microseconds()) / float64(ops),
	}
}

func (r *result) row() string {
	return fmt.Sprintf("%-42s | %12.0f | %8.1f | %8.1f | %7.1f | %7.1f | %7.1f | %7.1f | %7.1f",
		r.name, r.opsSec, r.mbSec, r.avgUsec, r.p50, r.p75, r.p99, r.p999, r.p9999)
}

func (ref rocksdbRef) row() string {
	return fmt.Sprintf("%-42s | %12.0f | %8.1f | %8.1f | %7.1f | %7.1f | %7.1f | %7.1f | %7.1f",
		ref.name, ref.opsSec, ref.mbSec, ref.avgUsec, ref.p50, ref.p75, ref.p99, ref.p999, ref.p9999)
}

func printTableHeader() {
	fmt.Printf("%-42s | %12s | %8s | %8s | %7s | %7s | %7s | %7s | %7s\n",
		"Test", "ops/sec", "MB/sec", "usec/op", "p50", "p75", "p99", "p99.9", "p99.99")
	fmt.Println(strings.Repeat("-", 155))
}

func printSection(title string) {
	fmt.Printf("\n%s\n%s\n%s\n", strings.Repeat("=", 80), title, strings.Repeat("=", 80))
}

func commaInt(n int) string {
	s := fmt.Sprintf("%d", n)
	if len(s) <= 3 {
		return s
	}
	var out string
	for i, c := range s {
		if i > 0 && (len(s)-i)%3 == 0 {
			out += ","
		}
		out += string(c)
	}
	return out
}

func makeKey(i int) []byte {
	return []byte(fmt.Sprintf("%0*d", *flagKeySize, i))
}

func makeValue(i int) []byte {
	v := make([]byte, *flagValueSize)
	s := fmt.Sprintf("v%010d", i)
	if len(s) > len(v) {
		s = s[:len(v)]
	}
	copy(v, s)
	return v
}

func getDBSize(path string) string {
	fi, err := os.Stat(path)
	if err != nil {
		return "unknown"
	}
	mb := float64(fi.Size()) / (1024.0 * 1024.0)
	if mb > 1024 {
		return fmt.Sprintf("%.2f GB", mb/1024)
	}
	return fmt.Sprintf("%.1f MB", mb)
}

func shouldRun(name string) bool {
	if *flagTests == "all" {
		return true
	}
	for _, t := range strings.Split(*flagTests, ",") {
		if strings.TrimSpace(t) == name {
			return true
		}
	}
	return false
}

func findResult(results []*result, substr string) *result {
	for _, r := range results {
		if strings.Contains(r.name, substr) {
			return r
		}
	}
	return nil
}

func openTree(path string) *bptree.PersistentBPTree {
	var opts []bptree.Option
	if *flagBloom {
		opts = append(opts, bptree.WithBloomFilter(*flagBloomBits))
	}
	if *flagCompress {
		opts = append(opts, bptree.WithCompression())
	}
	tree, err := bptree.OpenPersistentBPTree(path, *flagOrder, *flagCacheSize, opts...)
	if err != nil {
		panic(err)
	}
	return tree
}

func benchSequentialBulkLoad(path string) *result {
	os.Remove(path)
	tree := openTree(path)
	n := *flagNumKeys

	fmt.Printf("  Loading %s keys in sequential order...\n", commaInt(n))
	start := time.Now()
	for i := 0; i < n; i++ {
		k := makeKey(i)
		v := makeValue(i)
		if err := tree.Insert(k, v); err != nil {
			panic(err)
		}
		if (i+1)%1000000 == 0 {
			fmt.Printf("    %s keys loaded, elapsed %d ms\n", commaInt(i+1), time.Since(start).Milliseconds())
		}
	}
	elapsed := time.Since(start)
	tree.Close()

	fmt.Printf("  DB size: %s\n", getDBSize(path))
	return makeResultNoLat("B+ Tree Seq Bulk Load", n, elapsed)
}

func benchRandomBulkLoad(path string) *result {
	os.Remove(path)
	tree := openTree(path)
	n := *flagNumKeys

	perm := rand.Perm(n)
	fmt.Printf("  Loading %s keys in random order...\n", commaInt(n))
	start := time.Now()
	for idx, i := range perm {
		k := makeKey(i)
		v := makeValue(i)
		if err := tree.Insert(k, v); err != nil {
			panic(err)
		}
		if (idx+1)%1000000 == 0 {
			fmt.Printf("    %s keys loaded (random), elapsed %d ms\n", commaInt(idx+1), time.Since(start).Milliseconds())
		}
	}
	elapsed := time.Since(start)
	tree.Close()

	return makeResultNoLat("B+ Tree Random Bulk Load", n, elapsed)
}

func benchRandomRead1T(path string, numOps int) *result {
	tree := openTree(path)
	defer tree.Close()

	n := *flagNumKeys
	rng := rand.New(rand.NewSource(42))
	lats := make([]float64, numOps)

	start := time.Now()
	for i := 0; i < numOps; i++ {
		k := makeKey(rng.Intn(n))
		t0 := time.Now()
		tree.Find(k)
		lats[i] = float64(time.Since(t0).Microseconds())
	}
	elapsed := time.Since(start)

	return makeResult("B+ Tree Random Read 1T", numOps, elapsed, lats)
}

func benchRandomReadCold1T(path string, numOps int) *result {
	tree := openTree(path)
	defer tree.Close()

	n := *flagNumKeys
	rng := rand.New(rand.NewSource(123))
	lats := make([]float64, numOps)

	start := time.Now()
	for i := 0; i < numOps; i++ {
		k := makeKey(rng.Intn(n))
		t0 := time.Now()
		tree.Find(k)
		lats[i] = float64(time.Since(t0).Microseconds())
	}
	elapsed := time.Since(start)

	return makeResult("B+ Tree Random Read 1T (cold)", numOps, elapsed, lats)
}

func benchRandomReadMT(path string, numOps, numThreads int) *result {
	tree := openTree(path)
	defer tree.Close()

	n := *flagNumKeys
	opsPerThread := numOps / numThreads
	var totalOps int64

	fmt.Printf("  Running with %d threads, %d ops/thread...\n", numThreads, opsPerThread)
	start := time.Now()
	var wg sync.WaitGroup
	for t := 0; t < numThreads; t++ {
		wg.Add(1)
		go func(tid int) {
			defer wg.Done()
			rng := rand.New(rand.NewSource(int64(tid)*17 + 42))
			for i := 0; i < opsPerThread; i++ {
				k := makeKey(rng.Intn(n))
				tree.Find(k)
			}
			atomic.AddInt64(&totalOps, int64(opsPerThread))
		}(t)
	}
	wg.Wait()
	elapsed := time.Since(start)
	actualOps := int(totalOps)

	return makeResultNoLat(fmt.Sprintf("B+ Tree Random Read %dT", numThreads), actualOps, elapsed)
}

func benchSequentialRead(path string) *result {
	tree := openTree(path)
	defer tree.Close()

	n := *flagNumKeys
	sampleInterval := 1
	if n > 100000 {
		sampleInterval = n / 100000
	}
	var lats []float64

	start := time.Now()
	for i := 0; i < n; i++ {
		k := makeKey(i)
		t0 := time.Now()
		tree.Find(k)
		if i%sampleInterval == 0 {
			lats = append(lats, float64(time.Since(t0).Microseconds()))
		}
	}
	elapsed := time.Since(start)

	return makeResult("B+ Tree Sequential Read", n, elapsed, lats)
}

func benchRandomWrite1T(path string, numOps int) *result {
	tree := openTree(path)
	defer tree.Close()

	n := *flagNumKeys
	rng := rand.New(rand.NewSource(99))
	lats := make([]float64, numOps)

	start := time.Now()
	for i := 0; i < numOps; i++ {
		ki := rng.Intn(n)
		k := makeKey(ki)
		v := makeValue(ki + 1000000000)
		t0 := time.Now()
		if err := tree.Insert(k, v); err != nil {
			panic(err)
		}
		lats[i] = float64(time.Since(t0).Microseconds())
	}
	elapsed := time.Since(start)

	return makeResult("B+ Tree Random Write 1T", numOps, elapsed, lats)
}

func benchRandomWriteMT(path string, numOps, numThreads int) *result {
	tree := openTree(path)
	defer tree.Close()

	n := *flagNumKeys
	opsPerThread := numOps / numThreads
	var totalOps int64

	fmt.Printf("  Running with %d threads, %d ops/thread...\n", numThreads, opsPerThread)
	start := time.Now()
	var wg sync.WaitGroup
	for t := 0; t < numThreads; t++ {
		wg.Add(1)
		go func(tid int) {
			defer wg.Done()
			rng := rand.New(rand.NewSource(int64(tid)*31 + 99))
			for i := 0; i < opsPerThread; i++ {
				ki := rng.Intn(n)
				k := makeKey(ki)
				v := makeValue(ki + 2000000000)
				if err := tree.Insert(k, v); err != nil {
					panic(err)
				}
			}
			atomic.AddInt64(&totalOps, int64(opsPerThread))
		}(t)
	}
	wg.Wait()
	elapsed := time.Since(start)
	actualOps := int(totalOps)

	return makeResultNoLat(fmt.Sprintf("B+ Tree Random Write %dT", numThreads), actualOps, elapsed)
}

func benchRangeScan(path string, scanSize int) *result {
	tree := openTree(path)
	defer tree.Close()

	n := *flagNumKeys
	numScans := (*flagReadOps) / scanSize
	if numScans < 100 {
		numScans = 100
	}
	if numScans > 10000 {
		numScans = 10000
	}
	rng := rand.New(rand.NewSource(77))
	lats := make([]float64, numScans)

	start := time.Now()
	for i := 0; i < numScans; i++ {
		startIdx := rng.Intn(n - scanSize)
		startKey := makeKey(startIdx)
		endKey := makeKey(startIdx + scanSize)
		t0 := time.Now()
		tree.RangeScan(startKey, endKey)
		lats[i] = float64(time.Since(t0).Microseconds())
	}
	elapsed := time.Since(start)

	return makeResult(fmt.Sprintf("B+ Tree Range Scan (%d keys)", scanSize), numScans, elapsed, lats)
}

func benchRandomDelete1T(path string, numOps int) *result {
	tree := openTree(path)
	defer tree.Close()

	n := *flagNumKeys
	rng := rand.New(rand.NewSource(55))
	var lats []float64
	actualOps := 0

	start := time.Now()
	for i := 0; i < numOps; i++ {
		ki := rng.Intn(n)
		k := makeKey(ki)
		t0 := time.Now()
		deleted := tree.Delete(k)
		lat := float64(time.Since(t0).Microseconds())
		if deleted {
			lats = append(lats, lat)
			actualOps++
		}
	}
	elapsed := time.Since(start)

	if actualOps == 0 {
		return makeResultNoLat("B+ Tree Random Delete 1T (no keys deleted)", 0, elapsed)
	}
	return makeResult("B+ Tree Random Delete 1T", actualOps, elapsed, lats)
}

func benchReadWriteMix(path string, numOps, numThreads int, readRatio float64) *result {
	tree := openTree(path)
	defer tree.Close()

	n := *flagNumKeys
	opsPerThread := numOps / numThreads
	var readCount int64
	var totalCount int64

	fmt.Printf("  Running with %d threads, %.0f%% reads / %.0f%% writes, %d ops/thread...\n",
		numThreads, readRatio*100, (1-readRatio)*100, opsPerThread)

	start := time.Now()
	var wg sync.WaitGroup
	for t := 0; t < numThreads; t++ {
		wg.Add(1)
		go func(tid int) {
			defer wg.Done()
			rng := rand.New(rand.NewSource(int64(tid)*13 + 7))
			localReads := 0
			for i := 0; i < opsPerThread; i++ {
				ki := rng.Intn(n)
				k := makeKey(ki)
				if rng.Float64() < readRatio {
					tree.Find(k)
					localReads++
				} else {
					v := makeValue(ki + 3000000000)
					if err := tree.Insert(k, v); err != nil {
						panic(err)
					}
				}
			}
			atomic.AddInt64(&readCount, int64(localReads))
			atomic.AddInt64(&totalCount, int64(opsPerThread))
		}(t)
	}
	wg.Wait()
	elapsed := time.Since(start)
	actualOps := int(totalCount)

	name := fmt.Sprintf("B+ Tree R/W Mix %dT (%.0fR/%.0fW)",
		numThreads, readRatio*100, (1-readRatio)*100)
	r := makeResultNoLat(name, actualOps, elapsed)

	fmt.Printf("  Total ops: %d (reads: %d, writes: %d)\n", actualOps, readCount, actualOps-int(readCount))
	return r
}

func printHeader(order int) {
	fmt.Println()
	fmt.Println(strings.Repeat("=", 80))
	fmt.Println("         B+ Tree vs RocksDB Performance Benchmark")
	fmt.Println(strings.Repeat("=", 80))
	fmt.Printf("  Go Version:       %s\n", runtime.Version())
	fmt.Printf("  Num CPUs:         %d\n", runtime.NumCPU())
	fmt.Printf("  B+ Tree Order:    %d (max %d keys/node)\n", order, order-1)
	fmt.Printf("  LRU Cache:        %d pages (~%d KB)\n", *flagCacheSize, *flagCacheSize*32)
	fmt.Printf("  Page Size:        32 KB\n")
	fmt.Printf("  Key Size:         %d bytes\n", *flagKeySize)
	fmt.Printf("  Value Size:       %d bytes\n", *flagValueSize)
	fmt.Printf("  KV Pair Size:     %d bytes\n", 4+*flagKeySize+4+*flagValueSize)
	fmt.Printf("  Max keys/leaf:    %d\n", (pageSize-4-19)/(2+*flagKeySize+2+*flagValueSize))
	fmt.Printf("  Total Keys:       %s\n", commaInt(*flagNumKeys))
	fmt.Printf("  Read Ops/Test:    %s\n", commaInt(*flagReadOps))
	fmt.Printf("  Write Ops/Test:   %s\n", commaInt(*flagWriteOps))
	fmt.Printf("  Bloom Filter:     %v\n", *flagBloom)
	if *flagBloom {
		fmt.Printf("  Bloom Bits/Key:   %d\n", *flagBloomBits)
	}
	fmt.Printf("  Compression:      %v\n", *flagCompress)
	fmt.Println()
	fmt.Println("  * RocksDB 7.2.2 reference from official benchmarks:")
	fmt.Println("    https://github.com/facebook/rocksdb/wiki/Performance-Benchmarks")
	fmt.Println("    AWS m5d.2xlarge (8 CPU, 32GB RAM, NVMe SSD), 900M keys, 6GB cache")
	fmt.Println("    Direct comparison is NOT apples-to-apples due to different hardware,")
	fmt.Println("    dataset size, and engine architecture (LSM-tree vs B+ tree).")
	fmt.Println(strings.Repeat("=", 80))
}

func printSummary(results []*result) {
	fmt.Println()
	fmt.Println(strings.Repeat("=", 80))
	fmt.Println("                         Summary Table")
	fmt.Println(strings.Repeat("=", 80))
	printTableHeader()
	for _, r := range results {
		fmt.Println(r.row())
	}
	fmt.Println(strings.Repeat("-", 155))

	fmt.Println()
	fmt.Println("RocksDB 7.2.2 Reference Numbers (900M keys, AWS m5d.2xlarge, NVMe SSD):")
	printTableHeader()
	for _, ref := range refs {
		fmt.Println(ref.row())
	}
	fmt.Println(strings.Repeat("-", 155))

	fmt.Println()
	fmt.Println("Key Observations:")
	fmt.Println("  - Node-level latch coupling: per-node sync.RWMutex with optimistic crabbing")
	fmt.Println("  - Reads: crab with read locks (RLock parent → RLock child → RUnlock parent)")
	fmt.Println("  - Writes: serialized by writeMu; use optimistic write-lock crabbing")
	fmt.Println("  - Reads and writes can proceed concurrently (node-level synchronization)")
	fmt.Println("  - LRU cache with pinning: pinned entries cannot be evicted while in use")
	if *flagBloom {
		fmt.Println("  - Bloom filters: per-leaf, fast negative lookup before binary search")
	}
	if *flagCompress {
		fmt.Println("  - Snappy compression: transparent at cache/pager boundary")
	}
	fmt.Println("  - No WAL, no compaction in this B+ Tree implementation")
	fmt.Println("  - RocksDB has sophisticated LSM-tree with SST files, bloom filters,")
	fmt.Println("    block cache, compaction, compression, and column families")
	fmt.Println("  - For a fairer comparison, both should run on the same hardware with")
	fmt.Println("    the same dataset size and configuration")
	fmt.Println(strings.Repeat("=", 80))
}

func main() {
	flag.Parse()

	order := *flagOrder
	if order <= 0 {
		order = calcMaxOrder(*flagKeySize, *flagValueSize)
		if order > 256 {
			order = 256
		}
		if order < 3 {
			order = 3
		}
		fmt.Printf("  Auto-calculated B+ tree order: %d (max %d keys/node for %d+byte KV in 32KB page)\n",
			order, order-1, *flagKeySize+*flagValueSize)
	} else {
		maxOrder := calcMaxOrder(*flagKeySize, *flagValueSize)
		if order > maxOrder {
			fmt.Printf("  WARNING: order %d exceeds max %d for %d-byte keys + %d-byte values in 32KB page\n",
				order, maxOrder, *flagKeySize, *flagValueSize)
			fmt.Printf("  Auto-adjusting to order %d\n", maxOrder)
			order = maxOrder
		}
	}
	*flagOrder = order

	printHeader(order)

	path := *flagDBPath
	if path == "" {
		path = filepath.Join(os.TempDir(), "bptree_bench.db")
	}

	numKeys := *flagNumKeys
	readOps := *flagReadOps
	writeOps := *flagWriteOps
	var results []*result

	if !*flagSkipLoad {
		if shouldRun("seq_bulkload") {
			printSection("Test 1: Sequential Bulk Load")
			r := benchSequentialBulkLoad(path)
			results = append(results, r)
			printTableHeader()
			fmt.Println(r.row())
			fmt.Println(refs["bulkload"].row())
			fmt.Printf("  Ratio: %.2fx RocksDB ops/sec\n", r.opsSec/refs["bulkload"].opsSec)
		}

		if shouldRun("rand_bulkload") {
			printSection("Test 2: Random Order Bulk Load")
			r := benchRandomBulkLoad(path)
			results = append(results, r)
			printTableHeader()
			fmt.Println(r.row())
			fmt.Println(refs["bulkload"].row())
			fmt.Printf("  Ratio: %.2fx RocksDB ops/sec\n", r.opsSec/refs["bulkload"].opsSec)
		}
	} else {
		fmt.Printf("\n  Skipping bulk load, using existing DB: %s (%s)\n", path, getDBSize(path))
	}

	if shouldRun("random_read_1t") {
		printSection("Test 3: Random Read (1 Thread, Warm Cache)")
		fmt.Println("  Reading random keys from freshly loaded DB (warm cache)...")
		r := benchRandomRead1T(path, readOps)
		results = append(results, r)
		printTableHeader()
		fmt.Println(r.row())
		fmt.Println(refs["random_read"].row())
		fmt.Printf("  Ratio: %.2fx RocksDB ops/sec\n", r.opsSec/refs["random_read"].opsSec)
	}

	if shouldRun("random_read_cold") {
		printSection("Test 4: Random Read (1 Thread, Cold Cache)")
		fmt.Println("  Re-opening DB with empty cache to simulate cold reads...")
		r := benchRandomReadCold1T(path, readOps)
		results = append(results, r)
		printTableHeader()
		fmt.Println(r.row())
		fmt.Println(refs["random_read"].row())
		fmt.Printf("  Ratio: %.2fx RocksDB ops/sec\n", r.opsSec/refs["random_read"].opsSec)
	}

	if shouldRun("random_read_mt") {
		printSection("Test 5: Random Read (Multi-Thread)")
		fmt.Println("  Concurrent reads using RLock + thread-safe LRU cache.")
		for _, threads := range []int{2, 4, 8} {
			if threads > runtime.NumCPU()*2 {
				break
			}
			r := benchRandomReadMT(path, readOps, threads)
			results = append(results, r)
		}
		printTableHeader()
		for _, r := range results {
			if strings.Contains(r.name, "Random Read") && strings.Contains(r.name, "T") &&
				!strings.Contains(r.name, "1T") && !strings.Contains(r.name, "cold") {
				fmt.Println(r.row())
			}
		}
		fmt.Println(strings.Repeat("-", 155))
		r1t := findResult(results, "Random Read 1T")
		if r1t != nil && !strings.Contains(r1t.name, "cold") {
			fmt.Printf("  Note: 1T warm result for reference: %.0f ops/sec\n", r1t.opsSec)
		}
	}

	if shouldRun("seq_read") {
		printSection("Test 6: Sequential Read (1 Thread)")
		fmt.Println("  Reading all keys in sequential order...")
		r := benchSequentialRead(path)
		results = append(results, r)
		printTableHeader()
		fmt.Println(r.row())
	}

	if shouldRun("random_write_1t") {
		printSection("Test 7: Random Write / Overwrite (1 Thread)")
		fmt.Println("  Updating existing keys with new values...")
		r := benchRandomWrite1T(path, writeOps)
		results = append(results, r)
		printTableHeader()
		fmt.Println(r.row())
		fmt.Println(refs["overwrite"].row())
		fmt.Printf("  Ratio: %.2fx RocksDB ops/sec\n", r.opsSec/refs["overwrite"].opsSec)
	}

	if shouldRun("random_write_mt") {
		printSection("Test 8: Random Write (Multi-Thread)")
		fmt.Println("  Note: Writes use exclusive Lock, so threads are fully serialized.")
		for _, threads := range []int{2, 4} {
			r := benchRandomWriteMT(path, writeOps, threads)
			results = append(results, r)
		}
		printTableHeader()
		for _, r := range results {
			if strings.Contains(r.name, "Random Write") && !strings.Contains(r.name, "1T") {
				fmt.Println(r.row())
			}
		}
		fmt.Println(strings.Repeat("-", 155))
		r1t := findResult(results, "Random Write 1T")
		if r1t != nil {
			fmt.Printf("  Note: 1T result for reference: %.0f ops/sec\n", r1t.opsSec)
		}
	}

	if shouldRun("range_scan") {
		printSection("Test 9: Range Scan")
		for _, scanSize := range []int{100, 1000, 10000} {
			fmt.Printf("  Range scan with %d keys per scan...\n", scanSize)
			r := benchRangeScan(path, scanSize)
			results = append(results, r)
		}
		printTableHeader()
		for _, r := range results {
			if strings.Contains(r.name, "Range Scan") {
				fmt.Println(r.row())
			}
		}
		fmt.Println(strings.Repeat("-", 155))
		fmt.Println(refs["range_scan"].row())
		fmt.Println("  (RocksDB range scan iterates ~1100 keys per seek)")
	}

	if shouldRun("delete") {
		printSection("Test 10: Random Delete (1 Thread)")
		fmt.Printf("  Deleting random keys from %s total...\n", commaInt(numKeys))
		r := benchRandomDelete1T(path, writeOps)
		results = append(results, r)
		printTableHeader()
		fmt.Println(r.row())
	}

	if shouldRun("rw_mix") {
		printSection("Test 11: Read/Write Mixed Workload")
		fmt.Println("  Reads use RLock (concurrent), writes use Lock (exclusive).")
		for _, threads := range []int{1, 4} {
			if threads > runtime.NumCPU()*2 && threads > 1 {
				continue
			}
			r := benchReadWriteMix(path, readOps, threads, 0.9)
			results = append(results, r)
		}
		printTableHeader()
		for _, r := range results {
			if strings.Contains(r.name, "R/W Mix") {
				fmt.Println(r.row())
			}
		}
		fmt.Println(strings.Repeat("-", 155))
		fmt.Println(refs["read_while_write"].row())
	}

	printSummary(results)

	if path != *flagDBPath {
		fmt.Printf("\n  DB file: %s (%s)\n", path, getDBSize(path))
		fmt.Println("  Run with -db <path> to reuse, or manually delete.")
	}
}
