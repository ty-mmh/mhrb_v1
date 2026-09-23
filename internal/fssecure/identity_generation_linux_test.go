//go:build linux

package fssecure

import (
	"os"
	"path/filepath"
	"testing"

	"golang.org/x/sys/unix"
)

func TestM7LinuxIdentityRequiresStatxBirthGeneration(t *testing.T) {
	path := filepath.Join(t.TempDir(), "identity-generation")
	file, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_RDWR, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	identity, err := identityFromHandle(file, KindRegular)
	if err != nil {
		t.Fatal(err)
	}
	var evidence unix.Statx_t
	if err := unix.Statx(
		int(file.Fd()), "", unix.AT_EMPTY_PATH|unix.AT_STATX_SYNC_AS_STAT,
		unix.STATX_BASIC_STATS|unix.STATX_BTIME|unix.STATX_MNT_ID, &evidence,
	); err != nil {
		t.Fatal(err)
	}
	if identity.mount == 0 || identity.mount != evidence.Mnt_id ||
		identity.generationSeconds != evidence.Btime.Sec || identity.generationNanos != evidence.Btime.Nsec {
		t.Fatalf("identity generation = %d/%d.%09d, statx = %d/%d.%09d",
			identity.mount, identity.generationSeconds, identity.generationNanos,
			evidence.Mnt_id, evidence.Btime.Sec, evidence.Btime.Nsec)
	}
}

func TestM7LinuxSameVolumeRequiresMountIdentity(t *testing.T) {
	identity := Identity{
		volume: 7, first: 42, mount: 11,
		generationSeconds: 1_700_000_000, generationNanos: 123, kind: KindRegular,
	}
	sameMount := identity
	sameMount.first++
	if !identity.SameVolume(sameMount) {
		t.Fatal("same device and mount were not accepted as the same volume")
	}
	differentMount := identity
	differentMount.mount++
	if identity.SameVolume(differentMount) || identity.Equal(differentMount) {
		t.Fatal("same device on a different Linux mount was accepted")
	}
}
