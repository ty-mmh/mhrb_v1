//go:build linux

// Package configsecure implements the exact Linux/OCI mounted configuration
// file contract. Authority comes from the opened descriptor, never a later
// pathname read.
package configsecure

import (
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"syscall"

	"golang.org/x/sys/unix"
)

const MaxBytes = 1 << 20

var ErrInvalid = errors.New("secure configuration invalid")

func Read(path string) ([]byte, error) {
	if !filepath.IsAbs(path) || filepath.Clean(path) != path {
		return nil, ErrInvalid
	}
	before, err := os.Lstat(path)
	if err != nil || before.Mode()&os.ModeSymlink != 0 || !before.Mode().IsRegular() {
		return nil, ErrInvalid
	}
	fd, err := unix.Open(path, unix.O_RDONLY|unix.O_CLOEXEC|unix.O_NOFOLLOW, 0)
	if err != nil {
		return nil, ErrInvalid
	}
	file := os.NewFile(uintptr(fd), "mahoroba-config")
	if file == nil {
		_ = unix.Close(fd)
		return nil, ErrInvalid
	}
	defer file.Close()
	var opened unix.Stat_t
	if err := unix.Fstat(fd, &opened); err != nil {
		return nil, ErrInvalid
	}
	if opened.Mode&unix.S_IFMT != unix.S_IFREG || opened.Size < 1 || opened.Size > MaxBytes {
		return nil, ErrInvalid
	}
	mode := opened.Mode & 0o7777
	euid, egid := uint32(os.Geteuid()), uint32(os.Getegid())
	ownerMode := opened.Uid == euid && opened.Gid == egid && mode == 0o400
	rootMode := opened.Uid == 0 && opened.Gid == 0 && mode == 0o444
	if !ownerMode && !rootMode {
		return nil, ErrInvalid
	}
	if !sameFile(before, opened) {
		return nil, ErrInvalid
	}
	content, err := io.ReadAll(io.LimitReader(file, MaxBytes+1))
	if err != nil || len(content) != int(opened.Size) || len(content) > MaxBytes {
		return nil, ErrInvalid
	}
	var after unix.Stat_t
	if err := unix.Fstat(fd, &after); err != nil || opened.Dev != after.Dev || opened.Ino != after.Ino ||
		opened.Mode != after.Mode || opened.Uid != after.Uid || opened.Gid != after.Gid || opened.Size != after.Size {
		return nil, ErrInvalid
	}
	pathAfter, err := os.Lstat(path)
	if err != nil || !sameFile(pathAfter, opened) {
		return nil, ErrInvalid
	}
	return content, nil
}

func sameFile(info os.FileInfo, opened unix.Stat_t) bool {
	stat, ok := info.Sys().(*syscall.Stat_t)
	return ok && stat.Dev == opened.Dev && stat.Ino == opened.Ino && stat.Mode == opened.Mode &&
		stat.Uid == opened.Uid && stat.Gid == opened.Gid && stat.Size == opened.Size
}

func Error() error { return fmt.Errorf("%w: mounted config", ErrInvalid) }
