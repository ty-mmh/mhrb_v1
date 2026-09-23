//go:build linux

// Package descriptorpath exposes a read-only OS descriptor through a path
// which remains bound to the already-open file. It is intentionally narrow:
// callers must retain and verify the owning handle for the whole consumer
// operation.
package descriptorpath

import (
	"errors"
	"fmt"
	"os"
)

// ReadOnly returns a procfs descriptor path for file. fallback is ignored on
// Linux so consumers can never silently downgrade to reopening a mutable
// namespace path.
func ReadOnly(file *os.File, _ string) (string, error) {
	if file == nil {
		return "", errors.New("descriptorpath: nil file")
	}
	expected, err := file.Stat()
	if err != nil {
		return "", fmt.Errorf("descriptorpath: stat retained file: %w", err)
	}
	path := fmt.Sprintf("/proc/self/fd/%d", file.Fd())
	actual, err := os.Stat(path)
	if err != nil {
		return "", fmt.Errorf("descriptorpath: procfs descriptor unavailable: %w", err)
	}
	if !os.SameFile(expected, actual) {
		return "", errors.New("descriptorpath: procfs descriptor identity differs")
	}
	return path, nil
}
