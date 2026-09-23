package namespacelock

import (
	"bufio"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

const helperEnvironment = "MAHOROBA_NAMESPACELOCK_HELPER"

func TestM7NamespaceLockDoesNotCreateTargetAndRendezvousPresenceIsNotBusy(t *testing.T) {
	parent := namespaceLockTestTempDir(t)
	targetPath := filepath.Join(parent, "runtime")

	first, err := Acquire(targetPath)
	if err != nil {
		t.Fatal(err)
	}
	if exists, err := first.TargetExists(); err != nil || exists {
		t.Fatalf("TargetExists before creation = %v, %v", exists, err)
	}
	lockPath := filepath.Join(parent, ".runtime.mahoroba-namespace.lock")
	if info, err := os.Lstat(lockPath); err != nil || !info.Mode().IsRegular() {
		t.Fatalf("rendezvous = %#v, %v", info, err)
	}
	if err := first.Close(); err != nil {
		t.Fatal(err)
	}

	second, err := Acquire(targetPath)
	if err != nil {
		t.Fatalf("leftover rendezvous treated as busy: %v", err)
	}
	target, err := second.OpenOrCreateTarget(0o700)
	if err != nil {
		t.Fatal(err)
	}
	if err := target.Close(); err != nil {
		t.Fatal(err)
	}
	if exists, err := second.TargetExists(); err != nil || !exists {
		t.Fatalf("TargetExists after creation = %v, %v", exists, err)
	}
	if err := second.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestM7NamespaceLockRegularTargetExistenceIsHandleRelative(t *testing.T) {
	parent := namespaceLockTestTempDir(t)
	lock, err := AcquireExistingParent(filepath.Join(parent, "artifact.jsonl"))
	if err != nil {
		t.Fatal(err)
	}
	defer lock.Close()
	if exists, err := lock.RegularTargetExists(); err != nil || exists {
		t.Fatalf("RegularTargetExists absent = %v, %v", exists, err)
	}
	file, err := platformOpenRegular(lock.parent, lock.key.TargetBasename, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}
	if exists, err := lock.RegularTargetExists(); err != nil || !exists {
		t.Fatalf("RegularTargetExists present = %v, %v", exists, err)
	}
}

func TestM7AcquireExistingParentDoesNotCreateMissingParent(t *testing.T) {
	container := namespaceLockTestTempDir(t)
	parent := filepath.Join(container, "missing")
	if lock, err := AcquireExistingParent(filepath.Join(parent, "runtime")); err == nil {
		_ = lock.Close()
		t.Fatal("AcquireExistingParent succeeded for missing parent")
	}
	if _, err := os.Lstat(parent); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("missing parent was mutated: %v", err)
	}
}

func TestM7NamespaceLockSameTargetParentAliasContends(t *testing.T) {
	container := namespaceLockTestTempDir(t)
	parent := filepath.Join(container, "physical")
	if err := os.Mkdir(parent, 0o700); err != nil {
		t.Fatal(err)
	}
	alias := filepath.Join(container, "alias")
	if err := os.Symlink(parent, alias); err != nil {
		// Lexical aliases exercise the same clean-key rule on hosts which do not
		// grant Windows symlink creation to the test process.
		if err := os.Mkdir(filepath.Join(parent, "child"), 0o700); err != nil {
			t.Fatal(err)
		}
		alias = filepath.Join(parent, "child", "..")
	}

	first, err := Acquire(filepath.Join(parent, "runtime"))
	if err != nil {
		t.Fatal(err)
	}
	defer first.Close()
	if second, err := Acquire(filepath.Join(alias, "runtime")); !errors.Is(err, ErrBusy) {
		if second != nil {
			_ = second.Close()
		}
		t.Fatalf("Acquire through parent alias = %v, want ErrBusy", err)
	}
}

func TestM7NamespaceLockRejectsTargetSymlinkOrReparsePoint(t *testing.T) {
	parent := namespaceLockTestTempDir(t)
	realTarget := filepath.Join(parent, "real")
	if err := os.Mkdir(realTarget, 0o700); err != nil {
		t.Fatal(err)
	}
	targetPath := filepath.Join(parent, "runtime")
	if err := os.Symlink(realTarget, targetPath); err != nil {
		t.Skipf("symlink creation unavailable: %v", err)
	}
	lock, err := Acquire(targetPath)
	if err != nil {
		t.Fatal(err)
	}
	defer lock.Close()
	if target, err := lock.OpenOrCreateTarget(0o700); !errors.Is(err, ErrUnsafeTarget) {
		if target != nil {
			_ = target.Close()
		}
		t.Fatalf("OpenOrCreateTarget symlink error = %v, want ErrUnsafeTarget", err)
	}
}

func TestM7NamespaceLockRejectsSymlinkRendezvous(t *testing.T) {
	parent := namespaceLockTestTempDir(t)
	outside := filepath.Join(parent, "outside")
	if err := os.WriteFile(outside, nil, 0o600); err != nil {
		t.Fatal(err)
	}
	lockPath := filepath.Join(parent, ".runtime.mahoroba-namespace.lock")
	if err := os.Symlink(outside, lockPath); err != nil {
		t.Skipf("symlink creation unavailable: %v", err)
	}
	if lock, err := Acquire(filepath.Join(parent, "runtime")); !errors.Is(err, ErrUnsafeTarget) {
		if lock != nil {
			_ = lock.Close()
		}
		t.Fatalf("Acquire with symlink rendezvous = %v, want ErrUnsafeTarget", err)
	}
}

func TestM7NamespaceTargetCreationRaceIsSerialized(t *testing.T) {
	parent := namespaceLockTestTempDir(t)
	targetPath := filepath.Join(parent, "runtime")
	const competitors = 16
	start := make(chan struct{})
	var absentObservations atomic.Int32
	var failures atomic.Int32
	var group sync.WaitGroup
	group.Add(competitors)
	for range competitors {
		go func() {
			defer group.Done()
			<-start
			deadline := time.Now().Add(5 * time.Second)
			for {
				lock, err := Acquire(targetPath)
				if errors.Is(err, ErrBusy) && time.Now().Before(deadline) {
					time.Sleep(time.Millisecond)
					continue
				}
				if err != nil {
					failures.Add(1)
					return
				}
				exists, err := lock.TargetExists()
				if err != nil {
					failures.Add(1)
					_ = lock.Close()
					return
				}
				if !exists {
					absentObservations.Add(1)
				}
				target, err := lock.OpenOrCreateTarget(0o700)
				if err != nil {
					failures.Add(1)
				} else {
					_ = target.Close()
				}
				_ = lock.Close()
				return
			}
		}()
	}
	close(start)
	group.Wait()
	if got := failures.Load(); got != 0 {
		t.Fatalf("competitor failures = %d", got)
	}
	if got := absentObservations.Load(); got != 1 {
		t.Fatalf("absent observations = %d, want exactly one", got)
	}
}

func TestM7NamespaceLockAutoReleasesAfterOwnerCrash(t *testing.T) {
	parent := namespaceLockTestTempDir(t)
	targetPath := filepath.Join(parent, "runtime")
	command := exec.Command(os.Args[0], "-test.run=^TestNamespaceLockHelperProcess$")
	command.Env = append(os.Environ(), helperEnvironment+"="+targetPath)
	stdout, err := command.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	if err := command.Start(); err != nil {
		t.Fatal(err)
	}
	waited := false
	defer func() {
		if !waited {
			_ = command.Process.Kill()
			_ = command.Wait()
		}
	}()
	ready, err := bufio.NewReader(stdout).ReadString('\n')
	if err != nil || ready != "locked\n" {
		t.Fatalf("helper readiness = %q, %v", ready, err)
	}
	if lock, err := Acquire(targetPath); !errors.Is(err, ErrBusy) {
		if lock != nil {
			_ = lock.Close()
		}
		t.Fatalf("competing Acquire = %v, want ErrBusy", err)
	}
	if err := command.Process.Kill(); err != nil {
		t.Fatal(err)
	}
	if err := command.Wait(); err == nil {
		t.Fatal("killed helper exited successfully")
	}
	waited = true

	deadline := time.Now().Add(2 * time.Second)
	for {
		lock, err := Acquire(targetPath)
		if err == nil {
			if err := lock.Close(); err != nil {
				t.Fatal(err)
			}
			break
		}
		if !errors.Is(err, ErrBusy) || time.Now().After(deadline) {
			t.Fatalf("Acquire after helper crash = %v", err)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func TestNamespaceLockHelperProcess(t *testing.T) {
	targetPath := os.Getenv(helperEnvironment)
	if targetPath == "" {
		return
	}
	lock, err := Acquire(targetPath)
	if err != nil {
		t.Fatal(err)
	}
	defer lock.Close()
	fmt.Println("locked")
	time.Sleep(30 * time.Second)
}
