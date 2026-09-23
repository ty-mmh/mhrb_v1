//go:build !windows

package blob

import (
	"os"
	"testing"
)

func makeManagedRootUnsafe(t *testing.T, path string) {
	t.Helper()
	if err := os.Chmod(path, 0o750); err != nil {
		t.Fatal(err)
	}
}
