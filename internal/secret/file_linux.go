//go:build linux

package secret

import (
	"bytes"
	"io"
	"os"
	"path/filepath"
	"syscall"

	"golang.org/x/sys/unix"
)

const maxSecretFileBytes = 16 * 1024

func loadFilePlatform(name Name, path string) ([]byte, error) {
	if !filepath.IsAbs(path) || filepath.Clean(path) != path {
		return nil, namedError(ErrFileInvalid, name)
	}
	before, err := os.Lstat(path)
	if err != nil || !before.Mode().IsRegular() || before.Mode()&os.ModeSymlink != 0 {
		return nil, namedError(ErrFileInvalid, name)
	}
	fd, err := unix.Open(path, unix.O_RDONLY|unix.O_CLOEXEC|unix.O_NOFOLLOW, 0)
	if err != nil {
		return nil, namedError(ErrFileInvalid, name)
	}
	file := os.NewFile(uintptr(fd), "secret")
	if file == nil {
		_ = unix.Close(fd)
		return nil, namedError(ErrFileInvalid, name)
	}
	defer file.Close()
	opened, err := file.Stat()
	if err != nil || !opened.Mode().IsRegular() || !os.SameFile(before, opened) ||
		opened.Size() < 1 || opened.Size() > maxSecretFileBytes {
		return nil, namedError(ErrFileInvalid, name)
	}
	if !validLinuxSecretStat(opened) {
		return nil, namedError(ErrFileInvalid, name)
	}
	body, err := io.ReadAll(io.LimitReader(file, maxSecretFileBytes+1))
	if err != nil || int64(len(body)) != opened.Size() || len(body) > maxSecretFileBytes {
		Zero(body)
		return nil, namedError(ErrFileInvalid, name)
	}
	afterHandle, handleErr := file.Stat()
	afterPath, pathErr := os.Lstat(path)
	if handleErr != nil || pathErr != nil || !os.SameFile(opened, afterHandle) || !os.SameFile(opened, afterPath) ||
		afterHandle.Size() != opened.Size() || afterPath.Mode()&os.ModeSymlink != 0 ||
		!validLinuxSecretStat(afterHandle) || !validLinuxSecretStat(afterPath) {
		Zero(body)
		return nil, namedError(ErrFileInvalid, name)
	}
	if len(body) > 0 && body[len(body)-1] == '\n' {
		body[len(body)-1] = 0
		body = body[:len(body)-1]
		if len(body) > 0 && body[len(body)-1] == '\r' {
			body[len(body)-1] = 0
			body = body[:len(body)-1]
		}
	}
	if len(body) == 0 || bytes.ContainsAny(body, "\x00\r\n") {
		Zero(body)
		return nil, namedError(ErrFileInvalid, name)
	}
	return body, nil
}

func validLinuxSecretStat(info os.FileInfo) bool {
	stat, ok := info.Sys().(*syscall.Stat_t)
	return ok && info.Mode().IsRegular() && info.Size() >= 1 && info.Size() <= maxSecretFileBytes &&
		stat.Uid == uint32(unix.Geteuid()) && stat.Mode&0o077 == 0
}
