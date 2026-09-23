//go:build linux

package fssecure

import (
	"errors"
	"os"
	"path/filepath"
	"testing"

	"golang.org/x/sys/unix"
)

func TestM7RelativeUnixUnsupportedErrorsAreNormalized(t *testing.T) {
	for _, platformErr := range []error{unix.ENOSYS, unix.ENOTSUP, unix.EOPNOTSUPP, unix.EXDEV} {
		err := relativeUnixError("fixture", platformErr)
		if !errors.Is(err, ErrUnsafeFilesystem) || !errors.Is(err, ErrUnsupportedSecureFilesystem) || !errors.Is(err, platformErr) {
			t.Errorf("relative error %v matrix = %v", platformErr, err)
		}
	}
}

func TestM7LinuxPublishSourceNameSwapPreservesUncertainEntries(t *testing.T) {
	policy, err := CurrentSecurityPolicy()
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(t.TempDir(), "managed")
	directory, err := OpenOrCreateRoot(path, policy)
	if err != nil {
		t.Fatal(err)
	}
	defer directory.Close()
	source, err := directory.CreateRegular("source")
	if err != nil {
		t.Fatal(err)
	}
	defer source.Close()
	if _, err := source.File().Write([]byte("trusted")); err != nil {
		t.Fatal(err)
	}
	if err := source.Seal(); err != nil {
		t.Fatal(err)
	}
	replacement, err := directory.CreateRegular("replacement")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := replacement.File().Write([]byte("hostile")); err != nil {
		t.Fatal(err)
	}
	if err := replacement.Seal(); err != nil {
		t.Fatal(err)
	}
	if err := replacement.Close(); err != nil {
		t.Fatal(err)
	}
	source.testBeforeRename = func() {
		if err := os.Rename(filepath.Join(path, "source"), filepath.Join(path, "source-displaced")); err != nil {
			t.Fatal(err)
		}
		if err := os.Rename(filepath.Join(path, "replacement"), filepath.Join(path, "source")); err != nil {
			t.Fatal(err)
		}
	}
	err = directory.PublishNoReplace(source, "canonical")
	if !errors.Is(err, ErrIdentityChanged) {
		t.Fatalf("source-name swap error = %v", err)
	}
	if reason, ok := Reason(err); !ok || reason != ReasonPublishDestinationMismatch {
		t.Fatalf("source-name swap reason = %q/%v", reason, ok)
	}
	if content, err := os.ReadFile(filepath.Join(path, "canonical")); err != nil || string(content) != "hostile" {
		t.Fatalf("uncertain canonical entry was mutated: %q/%v", content, err)
	}
	if content, err := os.ReadFile(filepath.Join(path, "source-displaced")); err != nil || string(content) != "trusted" {
		t.Fatalf("trusted displaced source = %q/%v", content, err)
	}
}
