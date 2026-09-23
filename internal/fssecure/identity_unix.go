//go:build aix || darwin || dragonfly || freebsd || linux || netbsd || openbsd || solaris

package fssecure

import (
	"errors"
	"fmt"
	"os"

	"golang.org/x/sys/unix"
)

func CurrentSecurityPolicy() (SecurityPolicy, error) {
	uid := uint32(unix.Geteuid())
	return SecurityPolicy{uid: uid, ownerSID: "posix"}, nil
}

func nativeStatForHandle(file *os.File) (unix.Stat_t, os.FileInfo, error) {
	if file == nil {
		return unix.Stat_t{}, nil, ErrIdentityChanged
	}
	info := fileInfo(file)
	if info == nil {
		return unix.Stat_t{}, nil, ErrIdentityChanged
	}
	var stat unix.Stat_t
	if err := unix.Fstat(int(file.Fd()), &stat); err != nil {
		return unix.Stat_t{}, nil, err
	}
	return stat, info, nil
}

func identityFromNativeStat(stat unix.Stat_t, info os.FileInfo, kind EntryKind) (Identity, error) {
	if info == nil || !info.Mode().IsRegular() && kind == KindRegular || !info.IsDir() && kind == KindDirectory {
		return Identity{}, fmt.Errorf("%w: invalid entry kind", ErrUnsafeFilesystem)
	}
	if stat.Dev == 0 || stat.Ino == 0 {
		return Identity{}, unsupportedFilesystem("stable POSIX identity unavailable", nil)
	}
	return Identity{volume: uint64(stat.Dev), first: uint64(stat.Ino), kind: kind}, nil
}

func identityFromHandle(file *os.File, kind EntryKind) (Identity, error) {
	stat, info, err := nativeStatForHandle(file)
	if err != nil {
		return Identity{}, err
	}
	identity, err := identityFromNativeStat(stat, info, kind)
	if err != nil {
		return Identity{}, err
	}
	return bindStableIdentityGeneration(file, stat, identity)
}

func validateOpenedSecurity(file *os.File, kind EntryKind, policy SecurityPolicy) error {
	stat, info, err := nativeStatForHandle(file)
	if err != nil {
		return err
	}
	if info.Mode()&os.ModeSymlink != 0 {
		return WithReason(ReasonReparseOrSymlink, ErrUnsafeFilesystem)
	}
	if !policy.valid() || info.Mode().Perm() != map[EntryKind]os.FileMode{KindDirectory: 0o700, KindRegular: 0o600}[kind] {
		return WithReason(ReasonUnsafeMode, ErrUnsafeFilesystem)
	}
	if uint32(stat.Uid) != policy.uid {
		return WithReason(ReasonOwnerMismatch, ErrUnsafeFilesystem)
	}
	return nil
}

func validateDirectoryHandle(file *os.File, expected Identity, policy SecurityPolicy) error {
	identity, err := identityFromHandle(file, KindDirectory)
	if err != nil || !expected.Equal(identity) {
		return identityChanged(ReasonDirectoryIdentityChanged, err)
	}
	return validateOpenedSecurity(file, KindDirectory, policy)
}

func syncDirectory(file *os.File) error {
	if file == nil {
		return ErrIdentityChanged
	}
	if err := file.Sync(); err != nil {
		if errors.Is(err, unix.ENOSYS) || errors.Is(err, unix.ENOTSUP) || errors.Is(err, unix.EOPNOTSUPP) || errors.Is(err, unix.EXDEV) {
			return unsupportedFilesystem("directory fsync unavailable", err)
		}
		return err
	}
	return nil
}
