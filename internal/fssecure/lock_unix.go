//go:build aix || darwin || dragonfly || freebsd || linux || netbsd || openbsd || solaris

package fssecure

import (
	"errors"
	"fmt"
	"os"

	"golang.org/x/sys/unix"
)

func platformLock(file *os.File) error {
	err := unix.Flock(int(file.Fd()), unix.LOCK_EX|unix.LOCK_NB)
	if errors.Is(err, unix.EWOULDBLOCK) || errors.Is(err, unix.EAGAIN) {
		return ErrBusy
	}
	if err != nil {
		return fmt.Errorf("lock namespace: %w", err)
	}
	return nil
}

func platformUnlock(file *os.File) error {
	if err := unix.Flock(int(file.Fd()), unix.LOCK_UN); err != nil {
		return fmt.Errorf("unlock namespace: %w", err)
	}
	return nil
}
