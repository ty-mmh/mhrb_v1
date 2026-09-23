//go:build linux

package fssecure

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"golang.org/x/sys/unix"
)

func openRootRelative(path string, create bool, policy SecurityPolicy) (*os.File, *os.File, *directoryBinding, string, error) {
	clean := filepath.Clean(path)
	if clean == string(filepath.Separator) {
		return nil, nil, nil, "", fmt.Errorf("%w: filesystem root cannot be a managed blob root", ErrUnsafeFilesystem)
	}
	components := strings.FieldsFunc(strings.TrimPrefix(clean, string(filepath.Separator)), func(r rune) bool { return r == filepath.Separator })
	if len(components) == 0 {
		return nil, nil, nil, "", ErrUnsafeFilesystem
	}
	rootFD, err := unix.Open(string(filepath.Separator), unix.O_RDONLY|unix.O_DIRECTORY|unix.O_CLOEXEC|unix.O_NOFOLLOW, 0)
	if err != nil {
		return nil, nil, nil, "", relativeUnixError("open filesystem root", err)
	}
	current := os.NewFile(uintptr(rootFD), string(filepath.Separator))
	var ancestors *directoryBinding
	for _, component := range components[:len(components)-1] {
		fd, openErr := unix.Openat(int(current.Fd()), component, unix.O_RDONLY|unix.O_DIRECTORY|unix.O_CLOEXEC|unix.O_NOFOLLOW, 0)
		if openErr != nil {
			_ = current.Close()
			_ = ancestors.close()
			return nil, nil, nil, "", relativeUnixError("traverse root component", openErr)
		}
		next := os.NewFile(uintptr(fd), component)
		nextAncestors, bindingErr := retainRawDirectoryBinding(ancestors, current, next, component)
		if bindingErr != nil {
			_ = next.Close()
			_ = current.Close()
			_ = ancestors.close()
			return nil, nil, nil, "", bindingErr
		}
		ancestors = nextAncestors
		current = next
	}
	base := components[len(components)-1]
	file, err := openDirectoryRelative(current, base, create, policy)
	if err != nil {
		_ = current.Close()
		_ = ancestors.close()
		return nil, nil, nil, "", err
	}
	return file, current, ancestors, base, nil
}

func openRootRelativeReadOnly(path string, policy SecurityPolicy) (*os.File, *os.File, *directoryBinding, string, error) {
	return openRootRelative(path, false, policy)
}

func openDirectoryRelative(parent *os.File, name string, create bool, policy SecurityPolicy) (*os.File, error) {
	if parent == nil {
		return nil, ErrIdentityChanged
	}
	if create {
		if err := unix.Mkdirat(int(parent.Fd()), name, 0o700); err != nil && !errors.Is(err, unix.EEXIST) {
			return nil, relativeUnixError("create directory", err)
		}
	}
	fd, err := unix.Openat(int(parent.Fd()), name, unix.O_RDONLY|unix.O_DIRECTORY|unix.O_CLOEXEC|unix.O_NOFOLLOW, 0)
	if err != nil {
		return nil, relativeUnixError("open directory", err)
	}
	file := os.NewFile(uintptr(fd), name)
	if err := validateOpenedSecurity(file, KindDirectory, policy); err != nil {
		_ = file.Close()
		return nil, err
	}
	return file, nil
}

func openDirectoryRelativeForPublish(parent *os.File, name string, policy SecurityPolicy) (*os.File, error) {
	return openDirectoryRelative(parent, name, false, policy)
}

func openDirectoryRelativeReadOnly(parent *os.File, name string, policy SecurityPolicy) (*os.File, error) {
	return openDirectoryRelative(parent, name, false, policy)
}

func openRootRelativeForPublish(path string, policy SecurityPolicy) (*os.File, *os.File, *directoryBinding, string, error) {
	return openRootRelative(path, false, policy)
}

func openRegularRelative(parent *os.File, name string, mode regularOpenMode, policy SecurityPolicy) (*os.File, bool, error) {
	if parent == nil {
		return nil, false, ErrIdentityChanged
	}
	baseFlags := unix.O_CLOEXEC | unix.O_NOFOLLOW
	flags := baseFlags | unix.O_RDONLY
	created := false
	switch mode {
	case regularReadExisting, regularObserveMutable:
	case regularReadWriteExisting:
		flags = baseFlags | unix.O_RDWR
	case regularCreateExclusive:
		flags = baseFlags | unix.O_RDWR | unix.O_CREAT | unix.O_EXCL
		created = true
	case regularOpenOrCreate:
		flags = baseFlags | unix.O_RDWR | unix.O_CREAT | unix.O_EXCL
		created = true
	default:
		return nil, false, ErrUnsafeFilesystem
	}
	fd, err := unix.Openat(int(parent.Fd()), name, flags, 0o600)
	if mode == regularOpenOrCreate && errors.Is(err, unix.EEXIST) {
		created = false
		fd, err = unix.Openat(int(parent.Fd()), name, baseFlags|unix.O_RDWR, 0)
	}
	if err != nil {
		return nil, false, relativeUnixError("open regular file", err)
	}
	file := os.NewFile(uintptr(fd), name)
	if err := validateOpenedSecurity(file, KindRegular, policy); err != nil {
		_ = file.Close()
		return nil, false, err
	}
	return file, created, nil
}

func duplicateOpenFile(file *os.File, name string) (*os.File, error) {
	if file == nil {
		return nil, ErrIdentityChanged
	}
	fd, err := unix.FcntlInt(file.Fd(), unix.F_DUPFD_CLOEXEC, 0)
	if err != nil {
		return nil, relativeUnixError("duplicate directory handle", err)
	}
	return os.NewFile(uintptr(fd), name), nil
}

func renameRelativeNoReplace(sourceParent *os.File, sourceName string, source *os.File, destination *os.File, destinationName string) error {
	if sourceParent == nil || source == nil || destination == nil {
		return ErrIdentityChanged
	}
	if err := unix.Renameat2(int(sourceParent.Fd()), sourceName, int(destination.Fd()), destinationName, unix.RENAME_NOREPLACE); err != nil {
		if errors.Is(err, unix.ENOSYS) || errors.Is(err, unix.EOPNOTSUPP) || errors.Is(err, unix.ENOTSUP) || errors.Is(err, unix.EXDEV) {
			return unsupportedFilesystem("renameat2(RENAME_NOREPLACE) unavailable", err)
		}
		return err
	}
	return nil
}

func removeRelative(parent *os.File, name string, handle *os.File, policy SecurityPolicy) error {
	if parent == nil || handle == nil {
		return ErrIdentityChanged
	}
	expected, err := identityFromHandle(handle, KindRegular)
	if err != nil {
		return err
	}
	current, err := identityRelative(parent, name, KindRegular, policy)
	if err != nil || !expected.Equal(current) {
		return identityChanged(ReasonFileIdentityChanged, err)
	}
	if err := unix.Unlinkat(int(parent.Fd()), name, 0); err != nil {
		return relativeUnixError("handle-bound delete", err)
	}
	return nil
}

func identityRelativeEntry(parent *os.File, name string, kind EntryKind, policy SecurityPolicy) (Identity, error) {
	if parent == nil {
		return Identity{}, ErrIdentityChanged
	}
	flags := unix.O_RDONLY | unix.O_CLOEXEC | unix.O_NOFOLLOW
	if kind == KindDirectory {
		flags |= unix.O_DIRECTORY
	}
	fd, err := unix.Openat(int(parent.Fd()), name, flags, 0)
	if err != nil {
		return Identity{}, relativeUnixError("verify relative identity", err)
	}
	file := os.NewFile(uintptr(fd), name)
	defer file.Close()
	if err := validateOpenedSecurity(file, kind, policy); err != nil {
		return Identity{}, err
	}
	return identityFromHandle(file, kind)
}

func validateRawDirectoryHandle(file *os.File, expected Identity) error {
	current, err := identityFromHandle(file, KindDirectory)
	if err != nil || !expected.Equal(current) {
		return identityChanged(ReasonDirectoryIdentityChanged, err)
	}
	return nil
}

func identityRelativeDirectoryRaw(parent *os.File, name string) (Identity, error) {
	if parent == nil {
		return Identity{}, ErrIdentityChanged
	}
	fd, err := unix.Openat(int(parent.Fd()), name, unix.O_RDONLY|unix.O_DIRECTORY|unix.O_CLOEXEC|unix.O_NOFOLLOW, 0)
	if err != nil {
		return Identity{}, relativeUnixError("verify raw ancestor identity", err)
	}
	file := os.NewFile(uintptr(fd), name)
	defer file.Close()
	return identityFromHandle(file, KindDirectory)
}

func relativeUnixError(operation string, err error) error {
	if errors.Is(err, unix.ENOSYS) || errors.Is(err, unix.ENOTSUP) || errors.Is(err, unix.EOPNOTSUPP) || errors.Is(err, unix.EXDEV) {
		return unsupportedFilesystem(operation, err)
	}
	if errors.Is(err, unix.ELOOP) || errors.Is(err, unix.ENOTDIR) {
		return fmt.Errorf("fssecure: %s: %w", operation, WithReason(ReasonReparseOrSymlink, errors.Join(ErrUnsafeFilesystem, err)))
	}
	return err
}
