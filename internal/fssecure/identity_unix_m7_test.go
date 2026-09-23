//go:build aix || darwin || dragonfly || freebsd || linux || netbsd || openbsd || solaris

package fssecure

import (
	"errors"
	"os"
	"testing"

	"golang.org/x/sys/unix"
)

func TestM7POSIXZeroIdentityIsUnsafeAndUnsupported(t *testing.T) {
	file, err := os.CreateTemp(t.TempDir(), "identity-")
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	info, err := file.Stat()
	if err != nil {
		t.Fatal(err)
	}
	_, err = identityFromNativeStat(unix.Stat_t{}, info, KindRegular)
	if !errors.Is(err, ErrUnsafeFilesystem) || !errors.Is(err, ErrUnsupportedSecureFilesystem) {
		t.Fatalf("zero POSIX identity sentinel matrix = %v", err)
	}
}

func TestM7POSIXOpenedOwnerAndModeEvidence(t *testing.T) {
	path := t.TempDir() + string(os.PathSeparator) + "managed"
	file, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR|os.O_EXCL, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	policy, err := CurrentSecurityPolicy()
	if err != nil {
		t.Fatal(err)
	}
	wrongOwner := policy
	wrongOwner.uid++
	if wrongOwner.uid == policy.uid {
		wrongOwner.uid--
	}
	err = validateOpenedSecurity(file, KindRegular, wrongOwner)
	if !errors.Is(err, ErrUnsafeFilesystem) {
		t.Fatalf("owner mismatch sentinel = %v", err)
	}
	if reason, ok := Reason(err); !ok || reason != ReasonOwnerMismatch {
		t.Fatalf("owner mismatch reason = %q/%v", reason, ok)
	}
	if err := file.Chmod(0o640); err != nil {
		t.Fatal(err)
	}
	err = validateOpenedSecurity(file, KindRegular, policy)
	if !errors.Is(err, ErrUnsafeFilesystem) {
		t.Fatalf("mode mismatch sentinel = %v", err)
	}
	if reason, ok := Reason(err); !ok || reason != ReasonUnsafeMode {
		t.Fatalf("mode mismatch reason = %q/%v", reason, ok)
	}
}
