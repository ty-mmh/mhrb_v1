//go:build windows

package hostlock

import (
	"errors"
	"testing"

	"mahoroba.local/mahoroba/internal/namespacelock"
)

func TestM7AcquireRejectsExistingInheritedDataRootDACL(t *testing.T) {
	directory := t.TempDir()
	if lock, err := Acquire(directory); !errors.Is(err, namespacelock.ErrUnsafeTarget) {
		if lock != nil {
			_ = lock.Close()
		}
		t.Fatalf("Acquire inherited data root DACL = %v, want ErrUnsafeTarget", err)
	}
}
