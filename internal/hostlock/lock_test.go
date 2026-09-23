package hostlock

import (
	"bufio"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"mahoroba.local/mahoroba/internal/fssecure"
	"mahoroba.local/mahoroba/internal/namespacelock"
)

const helperEnvironment = "MAHOROBA_HOSTLOCK_HELPER"

func TestLockContendsAcrossProcessesAndAutoReleasesOnExit(t *testing.T) {
	directory := filepath.Join(t.TempDir(), "runtime")
	command := exec.Command(os.Args[0], "-test.run=^TestHostLockHelperProcess$")
	command.Env = append(os.Environ(), helperEnvironment+"="+directory)
	stdout, err := command.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	if err := command.Start(); err != nil {
		t.Fatal(err)
	}
	defer func() {
		_ = command.Process.Kill()
		_ = command.Wait()
	}()
	ready, err := bufio.NewReader(stdout).ReadString('\n')
	if err != nil || ready != "locked\n" {
		t.Fatalf("helper readiness = %q, %v", ready, err)
	}

	if competing, err := Acquire(directory); !errors.Is(err, ErrLocked) {
		if competing != nil {
			_ = competing.Close()
		}
		t.Fatalf("competing Acquire error = %v, want ErrLocked", err)
	}
	if err := command.Process.Kill(); err != nil {
		t.Fatal(err)
	}
	if err := command.Wait(); err == nil {
		t.Fatal("killed helper exited successfully")
	}

	deadline := time.Now().Add(2 * time.Second)
	for {
		lock, err := Acquire(directory)
		if err == nil {
			if err := lock.Close(); err != nil {
				t.Fatal(err)
			}
			break
		}
		if !errors.Is(err, ErrLocked) || time.Now().After(deadline) {
			t.Fatalf("Acquire after process exit = %v", err)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func TestCloseReleasesLockAndIsIdempotent(t *testing.T) {
	directory := filepath.Join(t.TempDir(), "runtime")
	first, err := Acquire(directory)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := Acquire(directory); !errors.Is(err, ErrLocked) {
		t.Fatalf("second Acquire error = %v, want ErrLocked", err)
	}
	if err := first.Close(); err != nil {
		t.Fatal(err)
	}
	if err := first.Close(); err != nil {
		t.Fatalf("second Close = %v", err)
	}
	second, err := Acquire(directory)
	if err != nil {
		t.Fatal(err)
	}
	if err := second.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestM7AcquireCreatesAbsentTargetUnderNamespaceLock(t *testing.T) {
	parent := t.TempDir()
	directory := filepath.Join(parent, "runtime")
	lock, err := Acquire(directory)
	if err != nil {
		t.Fatal(err)
	}
	defer lock.Close()
	if info, err := os.Lstat(directory); err != nil || !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
		t.Fatalf("created target = %#v, %v", info, err)
	}
	policy, err := fssecure.CurrentSecurityPolicy()
	if err != nil {
		t.Fatal(err)
	}
	root, err := fssecure.OpenRootReadOnly(directory, policy)
	if err != nil {
		t.Fatalf("hostlock-created root is not an exact protected filesystem boundary: %v", err)
	}
	if err := root.Close(); err != nil {
		t.Fatal(err)
	}
	if competing, err := Acquire(directory); !errors.Is(err, ErrLocked) {
		if competing != nil {
			_ = competing.Close()
		}
		t.Fatalf("competing Acquire = %v, want ErrLocked", err)
	}
}

func TestM7AcquireRejectsSymbolicLinkTargetWithoutTouchingReferent(t *testing.T) {
	parent := t.TempDir()
	referent := filepath.Join(parent, "referent")
	if err := os.Mkdir(referent, 0o700); err != nil {
		t.Fatal(err)
	}
	directory := filepath.Join(parent, "runtime")
	if err := os.Symlink(referent, directory); err != nil {
		t.Skipf("symlink creation unavailable: %v", err)
	}
	if lock, err := Acquire(directory); !errors.Is(err, namespacelock.ErrUnsafeTarget) {
		if lock != nil {
			_ = lock.Close()
		}
		t.Fatalf("Acquire symbolic-link target = %v, want ErrUnsafeTarget", err)
	}
	if _, err := os.Stat(filepath.Join(referent, lockFilename)); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("referent host lock was touched: %v", err)
	}
}

func TestHostLockHelperProcess(t *testing.T) {
	directory := os.Getenv(helperEnvironment)
	if directory == "" {
		return
	}
	lock, err := Acquire(directory)
	if err != nil {
		t.Fatal(err)
	}
	defer lock.Close()
	fmt.Println("locked")
	time.Sleep(30 * time.Second)
}
