//go:build linux

package namespacelock

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"syscall"

	"golang.org/x/sys/unix"
)

type platformIdentity struct {
	device uint64
	inode  uint64
	kind   entryKind
}

type linuxParentPathBinding struct {
	files      []*os.File
	components []string
	identities []platformIdentity
}

func platformBindParentPath(path string, expected platformIdentity) (parentPathBinding, error) {
	clean := filepath.Clean(path)
	if !filepath.IsAbs(clean) {
		return nil, ErrUnsafeTarget
	}
	fd, err := unix.Open(string(filepath.Separator), unix.O_RDONLY|unix.O_DIRECTORY|unix.O_CLOEXEC|unix.O_NOFOLLOW, 0)
	if err != nil {
		return nil, unixPathError("open filesystem root", string(filepath.Separator), err)
	}
	root := os.NewFile(uintptr(fd), string(filepath.Separator))
	rootIdentity, err := platformIdentityOf(root, entryDirectory)
	if err != nil {
		_ = root.Close()
		return nil, err
	}
	binding := &linuxParentPathBinding{files: []*os.File{root}, identities: []platformIdentity{rootIdentity}}
	components := strings.FieldsFunc(strings.TrimPrefix(clean, string(filepath.Separator)), func(r rune) bool {
		return r == filepath.Separator
	})
	current := root
	for _, component := range components {
		childFD, openErr := unix.Openat(int(current.Fd()), component, unix.O_RDONLY|unix.O_DIRECTORY|unix.O_CLOEXEC|unix.O_NOFOLLOW, 0)
		if openErr != nil {
			_ = binding.close()
			return nil, unixRelativeError("bind parent component", component, openErr)
		}
		child := os.NewFile(uintptr(childFD), component)
		identity, identityErr := platformIdentityOf(child, entryDirectory)
		if identityErr != nil {
			_ = child.Close()
			_ = binding.close()
			return nil, identityErr
		}
		binding.files = append(binding.files, child)
		binding.components = append(binding.components, component)
		binding.identities = append(binding.identities, identity)
		current = child
	}
	if len(binding.identities) == 0 || binding.identities[len(binding.identities)-1] != expected {
		_ = binding.close()
		return nil, ErrUnsafeTarget
	}
	return binding, nil
}

func (binding *linuxParentPathBinding) verify() error {
	if binding == nil || len(binding.files) == 0 || len(binding.identities) != len(binding.files) || len(binding.components)+1 != len(binding.files) {
		return ErrUnsafeTarget
	}
	for index, file := range binding.files {
		if err := platformVerifyIdentity(file, binding.identities[index], entryDirectory); err != nil {
			return err
		}
		if index == 0 {
			continue
		}
		fd, err := unix.Openat(int(binding.files[index-1].Fd()), binding.components[index-1], unix.O_RDONLY|unix.O_DIRECTORY|unix.O_CLOEXEC|unix.O_NOFOLLOW, 0)
		if err != nil {
			return unixRelativeError("verify parent component", binding.components[index-1], err)
		}
		current := os.NewFile(uintptr(fd), binding.components[index-1])
		actual, identityErr := platformIdentityOf(current, entryDirectory)
		closeErr := current.Close()
		if identityErr != nil || closeErr != nil {
			return errors.Join(identityErr, closeErr)
		}
		if actual != binding.identities[index] {
			return ErrUnsafeTarget
		}
	}
	return nil
}

func (binding *linuxParentPathBinding) close() error {
	if binding == nil {
		return nil
	}
	var result error
	for index := len(binding.files) - 1; index >= 0; index-- {
		result = errors.Join(result, binding.files[index].Close())
	}
	binding.files = nil
	binding.components = nil
	binding.identities = nil
	return result
}

func platformOpenParent(path string) (*os.File, platformIdentity, error) {
	fd, err := unix.Open(path, unix.O_RDONLY|unix.O_DIRECTORY|unix.O_CLOEXEC|unix.O_NOFOLLOW, 0)
	if err != nil {
		return nil, platformIdentity{}, unixPathError("open canonical parent", path, err)
	}
	file := os.NewFile(uintptr(fd), path)
	identity, err := platformIdentityOf(file, entryDirectory)
	if err != nil {
		_ = file.Close()
		return nil, platformIdentity{}, err
	}
	if err := validateLinuxSecurity(file, entryDirectory, 0); err != nil {
		_ = file.Close()
		return nil, platformIdentity{}, err
	}
	return file, identity, nil
}

func platformOpenRegular(parent *os.File, name string, mode os.FileMode) (*os.File, error) {
	fd, err := unix.Openat(int(parent.Fd()), name, unix.O_RDWR|unix.O_CREAT|unix.O_CLOEXEC|unix.O_NOFOLLOW|unix.O_NONBLOCK, uint32(mode.Perm()))
	if err != nil {
		return nil, unixRelativeError("open regular entry", name, err)
	}
	file := os.NewFile(uintptr(fd), name)
	if _, err := platformIdentityOf(file, entryRegular); err != nil {
		_ = file.Close()
		return nil, err
	}
	if err := validateLinuxSecurity(file, entryRegular, mode.Perm()); err != nil {
		_ = file.Close()
		return nil, err
	}
	return file, nil
}

func platformOpenRegularRead(parent *os.File, name string) (*os.File, error) {
	fd, err := unix.Openat(int(parent.Fd()), name, unix.O_RDONLY|unix.O_CLOEXEC|unix.O_NOFOLLOW|unix.O_NONBLOCK, 0)
	if err != nil {
		return nil, unixRelativeError("open diagnostic regular entry", name, err)
	}
	file := os.NewFile(uintptr(fd), name)
	if _, err := platformIdentityOf(file, entryRegular); err != nil {
		_ = file.Close()
		return nil, err
	}
	if err := validateLinuxSecurity(file, entryRegular, 0o600); err != nil {
		_ = file.Close()
		return nil, err
	}
	return file, nil
}

func platformOpenObservedRegularRead(parent *os.File, name string) (*os.File, error) {
	return platformOpenRegularRead(parent, name)
}

func platformOpenObservedTarget(parent *os.File, name string) (*os.File, bool, error) {
	return platformOpenLinuxTarget(parent, name, 0)
}

func platformOpenTarget(parent *os.File, name string, create bool, mode os.FileMode) (*os.File, bool, error) {
	exactMode := mode.Perm()
	if exactMode == 0 {
		exactMode = 0o700
	}
	file, exists, err := platformOpenLinuxTarget(parent, name, exactMode)
	if err != nil || exists || !create {
		return file, exists, err
	}
	permissions := uint32(exactMode)
	if err := unix.Mkdirat(int(parent.Fd()), name, permissions); err != nil && !errors.Is(err, unix.EEXIST) {
		return nil, false, unixRelativeError("create target", name, err)
	}
	file, exists, err = platformOpenLinuxTarget(parent, name, exactMode)
	if err != nil {
		return nil, false, err
	}
	if !exists || file == nil {
		return nil, false, ErrUnsafeTarget
	}
	return file, false, nil
}

func platformOpenLinuxTarget(parent *os.File, name string, exactMode os.FileMode) (*os.File, bool, error) {
	fd, err := unix.Openat(int(parent.Fd()), name, unix.O_RDONLY|unix.O_DIRECTORY|unix.O_CLOEXEC|unix.O_NOFOLLOW, 0)
	if err == nil {
		file := os.NewFile(uintptr(fd), name)
		if _, identityErr := platformIdentityOf(file, entryDirectory); identityErr != nil {
			_ = file.Close()
			return nil, false, identityErr
		}
		if securityErr := validateLinuxSecurity(file, entryDirectory, exactMode); securityErr != nil {
			_ = file.Close()
			return nil, false, securityErr
		}
		return file, true, nil
	}
	if !errors.Is(err, unix.ENOENT) {
		return nil, false, unixRelativeError("open target", name, err)
	}
	return nil, false, nil
}

func validateLinuxSecurity(file *os.File, kind entryKind, exactMode os.FileMode) error {
	var stat unix.Stat_t
	if err := unix.Fstat(int(file.Fd()), &stat); err != nil {
		return err
	}
	if uint32(stat.Uid) != uint32(unix.Geteuid()) {
		return fmt.Errorf("%w: entry owner differs from effective user", ErrUnsafeTarget)
	}
	permissions := os.FileMode(stat.Mode).Perm()
	if exactMode != 0 {
		if permissions != exactMode.Perm() {
			return fmt.Errorf("%w: regular entry mode is %04o, want %04o", ErrUnsafeTarget, permissions, exactMode.Perm())
		}
		return nil
	}
	if kind == entryDirectory && permissions&0o022 != 0 {
		return fmt.Errorf("%w: directory is group/other writable (%04o)", ErrUnsafeTarget, permissions)
	}
	return nil
}

func platformIdentityOf(file *os.File, kind entryKind) (platformIdentity, error) {
	if file == nil {
		return platformIdentity{}, ErrUnsafeTarget
	}
	var stat unix.Stat_t
	if err := unix.Fstat(int(file.Fd()), &stat); err != nil {
		return platformIdentity{}, err
	}
	mode := stat.Mode & unix.S_IFMT
	if kind == entryDirectory && mode != unix.S_IFDIR || kind == entryRegular && mode != unix.S_IFREG {
		return platformIdentity{}, fmt.Errorf("%w: unexpected entry kind", ErrUnsafeTarget)
	}
	if stat.Dev == 0 || stat.Ino == 0 {
		return platformIdentity{}, ErrUnsupported
	}
	return platformIdentity{device: uint64(stat.Dev), inode: uint64(stat.Ino), kind: kind}, nil
}

func platformVerifyIdentity(file *os.File, expected platformIdentity, kind entryKind) error {
	actual, err := platformIdentityOf(file, kind)
	if err != nil {
		return err
	}
	if actual != expected {
		return ErrUnsafeTarget
	}
	return nil
}

func platformLock(file *os.File) error {
	err := unix.Flock(int(file.Fd()), unix.LOCK_EX|unix.LOCK_NB)
	if errors.Is(err, unix.EWOULDBLOCK) || errors.Is(err, unix.EAGAIN) {
		return fmt.Errorf("%w: %s", ErrBusy, file.Name())
	}
	if err != nil {
		return fmt.Errorf("namespacelock: lock rendezvous: %w", err)
	}
	return nil
}

func platformUnlock(file *os.File) error {
	if err := unix.Flock(int(file.Fd()), unix.LOCK_UN); err != nil {
		return fmt.Errorf("namespacelock: unlock rendezvous: %w", err)
	}
	return nil
}

func unixRelativeError(operation, name string, err error) error {
	if errors.Is(err, unix.ELOOP) || errors.Is(err, unix.ENOTDIR) {
		err = errors.Join(ErrUnsafeTarget, err)
	}
	if errors.Is(err, unix.ENOSYS) || errors.Is(err, unix.ENOTSUP) || errors.Is(err, unix.EOPNOTSUPP) {
		err = errors.Join(ErrUnsupported, err)
	}
	return &os.PathError{Op: operation, Path: name, Err: err}
}

func unixPathError(operation, path string, err error) error {
	if errors.Is(err, syscall.ELOOP) || errors.Is(err, syscall.ENOTDIR) {
		err = errors.Join(ErrUnsafeTarget, err)
	}
	return &os.PathError{Op: operation, Path: path, Err: err}
}
