//go:build windows

// Package descriptorpath validates the namespace path corresponding to a
// retained no-write/no-delete-sharing Windows handle. The caller must keep
// that handle open until the path consumer closes.
package descriptorpath

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
)

// ReadOnly returns fallback only after proving that it still names file. The
// secure callers open file with FILE_SHARE_READ only, so the identity cannot
// be replaced or written while the returned path is in use.
func ReadOnly(file *os.File, fallback string) (string, error) {
	if file == nil {
		return "", errors.New("descriptorpath: nil file")
	}
	if !filepath.IsAbs(fallback) || filepath.Clean(fallback) != fallback {
		return "", errors.New("descriptorpath: fallback must be a canonical absolute path")
	}
	expected, err := file.Stat()
	if err != nil {
		return "", fmt.Errorf("descriptorpath: stat retained file: %w", err)
	}
	actual, err := os.Lstat(fallback)
	if err != nil {
		return "", fmt.Errorf("descriptorpath: stat retained namespace: %w", err)
	}
	if !actual.Mode().IsRegular() || actual.Mode()&os.ModeSymlink != 0 || !os.SameFile(expected, actual) {
		return "", errors.New("descriptorpath: retained namespace identity differs")
	}
	return fallback, nil
}
