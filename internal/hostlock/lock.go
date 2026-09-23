// Package hostlock serializes Mahoroba processes that can mutate Canonical or
// physical blob state in one data directory.
package hostlock

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"

	"mahoroba.local/mahoroba/internal/namespacelock"
)

const lockFilename = ".mahoroba.lock"

// ErrLocked reports that another Mahoroba writer or recovery process owns the
// data directory. The underlying OS lock is tied to its file handle and is
// therefore also released if the owning process exits unexpectedly.
var ErrLocked = errors.New("hostlock: data directory is already in use")

// Lock owns the operating-system lock handle for one data directory.
type Lock struct {
	file     *os.File
	once     sync.Once
	closeErr error
}

// Acquire obtains an exclusive, non-blocking operating-system lock. A missing
// data root is created with exact managed security; an existing non-exact root
// is rejected without repair. The lock file remains as a harmless rendezvous
// point after Close; ownership is the OS lock on its open handle, not the
// file's existence.
func Acquire(dataDir string) (*Lock, error) {
	if dataDir == "" || !filepath.IsAbs(dataDir) {
		return nil, errors.New("hostlock: data directory must be an absolute path")
	}
	directory := filepath.Clean(dataDir)
	namespace, err := namespacelock.Acquire(directory)
	if err != nil {
		if errors.Is(err, namespacelock.ErrBusy) {
			return nil, fmt.Errorf("%w: target namespace for %q is in use", ErrLocked, directory)
		}
		return nil, fmt.Errorf("hostlock: acquire target namespace: %w", err)
	}
	target, err := namespace.OpenOrCreateTarget(0o700)
	if err != nil {
		_ = namespace.Close()
		return nil, fmt.Errorf("hostlock: open target directory: %w", err)
	}
	file, err := target.OpenOrCreateRegular(lockFilename, 0o600)
	if err != nil {
		_ = target.Close()
		_ = namespace.Close()
		return nil, fmt.Errorf("hostlock: open lock file: %w", err)
	}
	if err := platformLock(file); err != nil {
		_ = file.Close()
		_ = target.Close()
		_ = namespace.Close()
		return nil, err
	}
	if err := errors.Join(target.Close(), namespace.Close()); err != nil {
		_ = platformUnlock(file)
		_ = file.Close()
		return nil, fmt.Errorf("hostlock: release target namespace: %w", err)
	}
	return &Lock{file: file}, nil
}

// AcquireBoundTarget obtains the host lock through a target handle that was
// opened while the caller still owns its namespace lock. This preserves the
// required namespace -> host-lock acquisition order for offline diagnostics
// without reopening or creating the target directory by pathname.
func AcquireBoundTarget(target *namespacelock.Target) (*Lock, error) {
	if target == nil {
		return nil, errors.New("hostlock: bound target is required")
	}
	file, err := target.OpenOrCreateRegular(lockFilename, 0o600)
	if err != nil {
		return nil, fmt.Errorf("hostlock: open bound lock file: %w", err)
	}
	if err := platformLock(file); err != nil {
		_ = file.Close()
		return nil, err
	}
	return &Lock{file: file}, nil
}

// Close releases the lock. It is safe to call repeatedly.
func (lock *Lock) Close() error {
	if lock == nil {
		return nil
	}
	lock.once.Do(func() {
		lock.closeErr = errors.Join(platformUnlock(lock.file), lock.file.Close())
	})
	return lock.closeErr
}
