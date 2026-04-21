//go:build linux

package wal

import (
	"os"
	"syscall"
)

// walFileSync uses fdatasync which skips metadata (atime etc.) flush.
// For WAL append-only writes this is sufficient and faster than fsync.
func walFileSync(f *os.File) error {
	return syscall.Fdatasync(int(f.Fd()))
}
