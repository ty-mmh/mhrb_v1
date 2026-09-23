//go:build linux

package namespacelock

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
)

func TestM7LinuxNamespaceLockRejectsCanonicalParentReplacement(t *testing.T) {
	container := t.TempDir()
	parent := filepath.Join(container, "parent")
	if err := os.Mkdir(parent, 0o700); err != nil {
		t.Fatal(err)
	}
	lock, err := AcquireExistingParent(filepath.Join(parent, "runtime"))
	if err != nil {
		t.Fatal(err)
	}
	defer lock.Close()

	detached := filepath.Join(container, "detached-parent")
	if err := os.Rename(parent, detached); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(parent, 0o700); err != nil {
		t.Fatal(err)
	}
	if _, err := lock.TargetExists(); !errors.Is(err, ErrUnsafeTarget) {
		t.Fatalf("TargetExists after canonical-parent replacement = %v, want ErrUnsafeTarget", err)
	}
}

func TestM7LinuxTargetObservationRejectsMidflightAncestorReplacement(t *testing.T) {
	container := t.TempDir()
	ancestor := filepath.Join(container, "ancestor")
	parent := filepath.Join(ancestor, "parent")
	if err := os.MkdirAll(filepath.Join(parent, "runtime"), 0o700); err != nil {
		t.Fatal(err)
	}
	lock, err := AcquireExistingParent(filepath.Join(parent, "runtime"))
	if err != nil {
		t.Fatal(err)
	}
	defer lock.Close()

	detached := filepath.Join(container, "detached-ancestor")
	lock.testHook = func(label string) {
		if label != "after_target_observation" {
			return
		}
		lock.testHook = nil
		if err := os.Rename(ancestor, detached); err != nil {
			t.Fatal(err)
		}
		if err := os.MkdirAll(parent, 0o700); err != nil {
			t.Fatal(err)
		}
	}
	if exists, err := lock.TargetExists(); !errors.Is(err, ErrUnsafeTarget) || exists {
		t.Fatalf("TargetExists across mid-observation ancestor replacement = %v, %v, want false, ErrUnsafeTarget", exists, err)
	}
}

func TestM7LinuxNamespaceRejectsWritableParentTargetAndRendezvous(t *testing.T) {
	t.Run("parent", func(t *testing.T) {
		parent := t.TempDir()
		if err := os.Chmod(parent, 0o770); err != nil {
			t.Fatal(err)
		}
		if lock, err := AcquireExistingParent(filepath.Join(parent, "runtime")); !errors.Is(err, ErrUnsafeTarget) {
			if lock != nil {
				_ = lock.Close()
			}
			t.Fatalf("Acquire with group-writable parent = %v, want ErrUnsafeTarget", err)
		}
	})

	t.Run("target", func(t *testing.T) {
		parent := t.TempDir()
		targetPath := filepath.Join(parent, "runtime")
		if err := os.Mkdir(targetPath, 0o700); err != nil {
			t.Fatal(err)
		}
		if err := os.Chmod(targetPath, 0o770); err != nil {
			t.Fatal(err)
		}
		lock, err := AcquireExistingParent(targetPath)
		if err != nil {
			t.Fatal(err)
		}
		defer lock.Close()
		if target, err := lock.OpenOrCreateTarget(0o700); !errors.Is(err, ErrUnsafeTarget) {
			if target != nil {
				_ = target.Close()
			}
			t.Fatalf("Open group-writable target = %v, want ErrUnsafeTarget", err)
		}
	})

	t.Run("rendezvous", func(t *testing.T) {
		parent := t.TempDir()
		path := filepath.Join(parent, ".runtime.mahoroba-namespace.lock")
		if err := os.WriteFile(path, nil, 0o600); err != nil {
			t.Fatal(err)
		}
		if err := os.Chmod(path, 0o660); err != nil {
			t.Fatal(err)
		}
		if lock, err := AcquireExistingParent(filepath.Join(parent, "runtime")); !errors.Is(err, ErrUnsafeTarget) {
			if lock != nil {
				_ = lock.Close()
			}
			t.Fatalf("Acquire with group-writable rendezvous = %v, want ErrUnsafeTarget", err)
		}
	})
}

func TestM7LinuxNewTargetAndRendezvousHaveExactModes(t *testing.T) {
	parent := t.TempDir()
	targetPath := filepath.Join(parent, "runtime")
	lock, err := AcquireExistingParent(targetPath)
	if err != nil {
		t.Fatal(err)
	}
	defer lock.Close()
	target, err := lock.OpenOrCreateTarget(0o700)
	if err != nil {
		t.Fatal(err)
	}
	defer target.Close()
	inner, err := target.OpenOrCreateRegular(".mahoroba.lock", 0o600)
	if err != nil {
		t.Fatal(err)
	}
	defer inner.Close()
	for path, want := range map[string]os.FileMode{
		targetPath: 0o700,
		filepath.Join(parent, ".runtime.mahoroba-namespace.lock"): 0o600,
		filepath.Join(targetPath, ".mahoroba.lock"):               0o600,
	} {
		info, err := os.Lstat(path)
		if err != nil {
			t.Fatal(err)
		}
		if got := info.Mode().Perm(); got != want {
			t.Fatalf("mode %s = %04o, want %04o", path, got, want)
		}
	}
}

func TestM7LinuxOpenOrCreateRejectsNonExactExistingTargetWithoutRepair(t *testing.T) {
	parent := t.TempDir()
	targetPath := filepath.Join(parent, "runtime")
	if err := os.Mkdir(targetPath, 0o750); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(targetPath, 0o750); err != nil {
		t.Fatal(err)
	}
	lock, err := AcquireExistingParent(targetPath)
	if err != nil {
		t.Fatal(err)
	}
	defer lock.Close()
	if target, err := lock.OpenOrCreateTarget(0o700); !errors.Is(err, ErrUnsafeTarget) {
		if target != nil {
			_ = target.Close()
		}
		t.Fatalf("OpenOrCreateTarget non-exact mode = %v, want ErrUnsafeTarget", err)
	}
	info, err := os.Lstat(targetPath)
	if err != nil {
		t.Fatal(err)
	}
	if got := info.Mode().Perm(); got != 0o750 {
		t.Fatalf("rejected target mode was repaired to %04o, want unchanged 0750", got)
	}
}

func TestM7LinuxTargetExistenceUsesObservedPolicyButAuthorityRequiresExactMode(t *testing.T) {
	parent := t.TempDir()
	targetPath := filepath.Join(parent, "runtime")
	if err := os.Mkdir(targetPath, 0o750); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(targetPath, 0o750); err != nil {
		t.Fatal(err)
	}
	lock, err := AcquireExistingParent(targetPath)
	if err != nil {
		t.Fatal(err)
	}
	defer lock.Close()

	if exists, err := lock.TargetExists(); err != nil || !exists {
		t.Fatalf("TargetExists observed target = %v, %v, want true, nil", exists, err)
	}
	if target, err := lock.OpenExistingTarget(); !errors.Is(err, ErrUnsafeTarget) {
		if target != nil {
			_ = target.Close()
		}
		t.Fatalf("OpenExistingTarget observed target = %v, want ErrUnsafeTarget", err)
	}
	if target, err := lock.OpenOrCreateTarget(0o700); !errors.Is(err, ErrUnsafeTarget) {
		if target != nil {
			_ = target.Close()
		}
		t.Fatalf("OpenOrCreateTarget observed target = %v, want ErrUnsafeTarget", err)
	}
	info, err := os.Lstat(targetPath)
	if err != nil {
		t.Fatal(err)
	}
	if got := info.Mode().Perm(); got != 0o750 {
		t.Fatalf("read-only observation mutated target mode to %04o", got)
	}
}
