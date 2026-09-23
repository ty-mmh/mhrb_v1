package fssecure

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
)

func TestNamespaceLockUsesRetainedParentAndNoCreateForAbsentTarget(t *testing.T) {
	policy, err := CurrentSecurityPolicy()
	if err != nil {
		t.Fatal(err)
	}
	container := t.TempDir()
	parent := filepath.Join(container, "managed")
	directory, err := OpenOrCreateRoot(parent, policy)
	if err != nil {
		t.Fatal(err)
	}
	defer directory.Close()
	first, err := directory.AcquireNamespace("target")
	if err != nil {
		t.Fatal(err)
	}
	defer first.Close()
	if exists, _, err := first.TargetExistsUnderLock(filepath.Join(parent, "target")); err != nil || exists {
		t.Fatalf("absent target check = %v/%v", exists, err)
	}
	if _, err := os.Stat(filepath.Join(parent, ".target.mahoroba-namespace.lock")); err != nil {
		t.Fatal(err)
	}
}

func TestNamespaceLockTargetBasenameRecognizesOnlyExactManagedName(t *testing.T) {
	if target, ok := NamespaceLockTargetBasename(".target.mahoroba-namespace.lock"); !ok || target != "target" {
		t.Fatalf("managed lock = %q/%v", target, ok)
	}
	for _, malformed := range []string{
		"target.mahoroba-namespace.lock",
		"..mahoroba-namespace.lock",
		".target.mahoroba-namespace.lock.extra",
		"../.target.mahoroba-namespace.lock",
	} {
		if target, ok := NamespaceLockTargetBasename(malformed); ok || target != "" {
			t.Fatalf("malformed %q accepted as %q", malformed, target)
		}
	}
}

func TestDirectoryOpenRegularRechecksRelativeIdentity(t *testing.T) {
	policy, err := CurrentSecurityPolicy()
	if err != nil {
		t.Fatal(err)
	}
	directory, err := OpenOrCreateRoot(filepath.Join(t.TempDir(), "managed"), policy)
	if err != nil {
		t.Fatal(err)
	}
	defer directory.Close()
	created, err := directory.CreateRegular("object")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := created.File().Write([]byte("ok")); err != nil {
		t.Fatal(err)
	}
	if err := created.Seal(); err != nil {
		t.Fatal(err)
	}
	if err := created.Close(); err != nil {
		t.Fatal(err)
	}
	handle, err := directory.OpenRegular("object")
	if err != nil {
		t.Fatal(err)
	}
	defer handle.Close()
	if err := handle.VerifyIdentity(); err != nil {
		t.Fatal(err)
	}
}

func TestM7HandleBoundDeleteAllowsStaleContentSnapshot(t *testing.T) {
	policy, err := CurrentSecurityPolicy()
	if err != nil {
		t.Fatal(err)
	}
	directory, err := OpenOrCreateRoot(filepath.Join(t.TempDir(), "managed-delete"), policy)
	if err != nil {
		t.Fatal(err)
	}
	defer directory.Close()
	handle, err := directory.CreateRegular("partial")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := handle.File().Write([]byte("snapshot is intentionally stale")); err != nil {
		t.Fatal(err)
	}
	if err := handle.MarkDeleteOnClose(); err != nil {
		t.Fatal(err)
	}
	if err := handle.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := directory.OpenRegular("partial"); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("handle-bound delete left partial entry: %v", err)
	}
}
