//go:build linux

package hostlock

import (
	"errors"
	"os"
	"path/filepath"
	"testing"

	"mahoroba.local/mahoroba/internal/namespacelock"
)

func TestM7AcquireRejectsExistingNonExactDataRootWithoutRepair(t *testing.T) {
	directory := filepath.Join(t.TempDir(), "runtime")
	if err := os.Mkdir(directory, 0o750); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(directory, 0o750); err != nil {
		t.Fatal(err)
	}
	if lock, err := Acquire(directory); !errors.Is(err, namespacelock.ErrUnsafeTarget) {
		if lock != nil {
			_ = lock.Close()
		}
		t.Fatalf("Acquire non-exact data root = %v, want ErrUnsafeTarget", err)
	}
	info, err := os.Lstat(directory)
	if err != nil {
		t.Fatal(err)
	}
	if got := info.Mode().Perm(); got != 0o750 {
		t.Fatalf("rejected data root was repaired to %04o, want unchanged 0750", got)
	}
}
