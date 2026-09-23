//go:build !windows

package hostlock

import (
	"errors"
	"fmt"
	"os"

	"golang.org/x/sys/unix"
)

func platformLock(file *os.File) error {
	err := unix.Flock(int(file.Fd()), unix.LOCK_EX|unix.LOCK_NB)
	if errors.Is(err, unix.EWOULDBLOCK) || errors.Is(err, unix.EAGAIN) {
		return fmt.Errorf("%w: another writer or blob recovery process holds %q", ErrLocked, file.Name())
	}
	if err != nil {
		return fmt.Errorf("hostlock: lock %q: %w", file.Name(), err)
	}
	return nil
}

func platformUnlock(file *os.File) error {
	if err := unix.Flock(int(file.Fd()), unix.LOCK_UN); err != nil {
		return fmt.Errorf("hostlock: unlock %q: %w", file.Name(), err)
	}
	return nil
}
