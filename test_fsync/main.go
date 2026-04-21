package main

import (
	"fmt"
	"os"
	"syscall"
	"time"
)

func fdatasync(f *os.File) error {
	_, _, e1 := syscall.Syscall(syscall.SYS_FDATASYNC, uintptr(f.Fd()), 0, 0)
	if e1 != 0 {
		return e1
	}
	return nil
}

func main() {
	f, err := os.OpenFile("/tmp/minidb_fsync_bench.dat", os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)
	if err != nil {
		panic(err)
	}
	defer os.Remove(f.Name())
	defer f.Close()

	buf := make([]byte, 256)
	for i := range buf {
		buf[i] = byte(i)
	}

	const N = 1000

	// Warm up.
	f.Write(buf)
	f.Sync()
	f.Truncate(0)
	f.Seek(0, 0)

	fmt.Println("=== fsync vs fdatasync (macOS SYS_FDATASYNC) ===")
	fmt.Printf("%-12s %12s %12s %12s\n", "Method", "Avg", "P50", "P99")

	for _, method := range []string{"fsync", "fdatasync"} {
		durs := make([]time.Duration, N)
		for i := 0; i < N; i++ {
			f.Write(buf)
			t0 := time.Now()
			if method == "fsync" {
				f.Sync()
			} else {
				fdatasync(f)
			}
			durs[i] = time.Since(t0)
			f.Truncate(0)
			f.Seek(0, 0)
		}

		var total time.Duration
		for _, d := range durs {
			total += d
		}
		avg := total / N

		// Insertion sort for percentiles.
		for i := 1; i < len(durs); i++ {
			for j := i; j > 0 && durs[j] < durs[j-1]; j-- {
				durs[j], durs[j-1] = durs[j-1], durs[j]
			}
		}
		p50 := durs[N/2]
		p99 := durs[int(float64(N)*0.99)]

		fmt.Printf("%-12s %12s %12s %12s\n",
			method,
			fmt.Sprintf("%.1f us", float64(avg.Nanoseconds())/1000),
			fmt.Sprintf("%.1f us", float64(p50.Nanoseconds())/1000),
			fmt.Sprintf("%.1f us", float64(p99.Nanoseconds())/1000),
		)
	}

	// Group commit pattern with fdatasync.
	fmt.Println()
	fmt.Println("=== Group commit with fdatasync ===")
	fmt.Printf("%-8s %12s %12s %12s\n", "Group", "perTxn", "syncOnly", "sync%")

	for _, groupSize := range []int{1, 4, 8, 16, 32} {
		const trials = 200
		var syncOnly, total time.Duration

		for t := 0; t < trials; t++ {
			f.Truncate(0)
			f.Seek(0, 0)

			t0 := time.Now()
			for i := 0; i < groupSize; i++ {
				f.Write(buf)
			}
			writeDur := time.Since(t0)

			t1 := time.Now()
			fdatasync(f)
			syncDur := time.Since(t1)

			total += writeDur + syncDur
			syncOnly += syncDur
		}

		perTxnNs := total.Nanoseconds() / int64(trials)
		syncAvgNs := syncOnly.Nanoseconds() / int64(trials)
		fmt.Printf("%-8d %12.1f %12.1f %11.0f%%\n",
			groupSize,
			float64(perTxnNs)/1000,
			float64(syncAvgNs)/1000,
			float64(syncOnly.Nanoseconds())/float64(total.Nanoseconds())*100,
		)
	}
}
