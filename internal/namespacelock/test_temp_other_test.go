//go:build !windows

package namespacelock

import (
	"errors"
	"testing"
)

func namespaceLockTestTempDir(t *testing.T) string {
	t.Helper()
	return t.TempDir()
}

func TestValidateCITestTempFailsClosedOffWindows(t *testing.T) {
	if err := ValidateCITestTemp("/tmp/not-authority"); !errors.Is(err, ErrUnsupported) {
		t.Fatalf("ValidateCITestTemp error = %v, want ErrUnsupported", err)
	}
}
