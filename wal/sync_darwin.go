//go:build darwin

package wal

import (
	"os"
	"syscall"
)

// walFileSync uses fdatasync syscall directly.
// Faster than Go's f.Sync() (which calls fsync) because it skips
// metadata flush. For WAL append-only writes this is sufficient.
func walFileSync(f *os.File) error {
	_, _, e1 := syscall.Syscall(syscall.SYS_FDATASYNC, uintptr(f.Fd()), 0, 0)
	if e1 != 0 {
		return e1
	}
	return nil
}
