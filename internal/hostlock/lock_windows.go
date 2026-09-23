//go:build windows

package hostlock

import (
	"errors"
	"fmt"
	"os"

	"golang.org/x/sys/windows"
)

func platformLock(file *os.File) error {
	overlapped := new(windows.Overlapped)
	err := windows.LockFileEx(windows.Handle(file.Fd()),
		windows.LOCKFILE_EXCLUSIVE_LOCK|windows.LOCKFILE_FAIL_IMMEDIATELY, 0, 1, 0, overlapped)
	if errors.Is(err, windows.ERROR_LOCK_VIOLATION) {
		return fmt.Errorf("%w: another writer or blob recovery process holds %q", ErrLocked, file.Name())
	}
	if err != nil {
		return fmt.Errorf("hostlock: lock %q: %w", file.Name(), err)
	}
	return nil
}

func platformUnlock(file *os.File) error {
	overlapped := new(windows.Overlapped)
	if err := windows.UnlockFileEx(windows.Handle(file.Fd()), 0, 1, 0, overlapped); err != nil {
		return fmt.Errorf("hostlock: unlock %q: %w", file.Name(), err)
	}
	return nil
}
