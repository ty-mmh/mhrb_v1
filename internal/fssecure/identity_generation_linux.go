//go:build linux

package fssecure

import (
	"errors"
	"fmt"
	"os"

	"golang.org/x/sys/unix"
)

func bindStableIdentityGeneration(file *os.File, stat unix.Stat_t, identity Identity) (Identity, error) {
	if file == nil {
		return Identity{}, ErrIdentityChanged
	}
	var evidence unix.Statx_t
	const required = unix.STATX_TYPE | unix.STATX_INO | unix.STATX_BTIME | unix.STATX_MNT_ID
	err := unix.Statx(
		int(file.Fd()), "", unix.AT_EMPTY_PATH|unix.AT_STATX_SYNC_AS_STAT,
		unix.STATX_BASIC_STATS|unix.STATX_BTIME|unix.STATX_MNT_ID, &evidence,
	)
	if err != nil {
		if errors.Is(err, unix.ENOSYS) || errors.Is(err, unix.EINVAL) || errors.Is(err, unix.ENOTSUP) || errors.Is(err, unix.EOPNOTSUPP) {
			return Identity{}, unsupportedFilesystem("stable Linux statx identity generation unavailable", err)
		}
		return Identity{}, fmt.Errorf("fssecure: inspect Linux identity generation: %w", err)
	}
	if evidence.Mask&required != required || evidence.Mnt_id == 0 ||
		evidence.Btime.Sec <= 0 || evidence.Btime.Nsec >= 1_000_000_000 {
		return Identity{}, unsupportedFilesystem("stable Linux statx identity generation unavailable", nil)
	}
	if evidence.Ino != uint64(stat.Ino) || unix.Mkdev(evidence.Dev_major, evidence.Dev_minor) != uint64(stat.Dev) ||
		uint32(evidence.Mode)&unix.S_IFMT != stat.Mode&unix.S_IFMT {
		reason := ReasonFileIdentityChanged
		if identity.kind == KindDirectory {
			reason = ReasonDirectoryIdentityChanged
		}
		return Identity{}, identityChanged(reason, nil)
	}
	identity.mount = evidence.Mnt_id
	identity.generationSeconds = evidence.Btime.Sec
	identity.generationNanos = evidence.Btime.Nsec
	return identity, nil
}
