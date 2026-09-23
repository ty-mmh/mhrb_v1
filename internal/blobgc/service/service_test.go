package service

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"runtime"
	"testing"

	"mahoroba.local/mahoroba/internal/blobgc"
	"mahoroba.local/mahoroba/internal/canonical"
	"mahoroba.local/mahoroba/internal/hostlock"
)

func TestM7BlobGCRejectsOnlineCoordinatorHostLockBeforeOpeningSource(t *testing.T) {
	dataDir := filepath.Join(t.TempDir(), "data")
	lock, err := hostlock.Acquire(dataDir)
	if err != nil {
		if runtime.GOOS == "windows" && os.Getenv("CI") == "" && errors.Is(err, os.ErrPermission) {
			t.Skipf("desktop sandbox cannot create the exact protected data-root ACL: %v", err)
		}
		t.Fatal(err)
	}
	defer lock.Close()
	residentID, err := canonical.ParseID("01J00000000000000000000001")
	if err != nil {
		t.Fatal(err)
	}

	result, err := Run(context.Background(), Request{
		SourceDataDir: dataDir, DatabaseFilename: filepath.Base("mahoroba.db"), ResidentID: residentID,
	})
	if runtime.GOOS == "windows" && os.Getenv("CI") == "" && errors.Is(err, os.ErrPermission) {
		t.Skipf("desktop sandbox cannot reopen the exact protected lock ACL: %v", err)
	}
	if !errors.Is(err, hostlock.ErrLocked) {
		t.Fatalf("Run error = %v, want hostlock.ErrLocked", err)
	}
	if result.CandidateCount != 0 || result.DeletedCount != 0 || result.PhysicalMutation {
		t.Fatalf("busy online gate returned progress: %+v", result)
	}
}

func TestM7BlobGCPostMutationCloseFailureIsPartial(t *testing.T) {
	closeErr := errors.New("injected close failure")
	err := joinCloseResult(blobgc.Result{DeletedCount: 1, PhysicalMutation: true}, nil, closeErr)
	if !errors.Is(err, blobgc.ErrPartial) || !errors.Is(err, closeErr) {
		t.Fatalf("post-mutation close error = %v", err)
	}
	err = joinCloseResult(blobgc.Result{}, nil, closeErr)
	if errors.Is(err, blobgc.ErrPartial) || !errors.Is(err, closeErr) {
		t.Fatalf("pre-mutation close error = %v", err)
	}
}

func TestM7BlobGCMalformedConfirmationIsRejectedBeforeHostLock(t *testing.T) {
	dataDir := t.TempDir()
	residentID, err := canonical.ParseID("01J00000000000000000000001")
	if err != nil {
		t.Fatal(err)
	}
	_, err = Run(context.Background(), Request{
		SourceDataDir: dataDir, DatabaseFilename: "mahoroba.db", ResidentID: residentID,
		Apply: true, Confirm: "sha256:BAD",
	})
	if !errors.Is(err, blobgc.ErrPlanStale) {
		t.Fatalf("malformed confirmation error = %v", err)
	}
	if _, statErr := os.Lstat(filepath.Join(dataDir, ".mahoroba.lock")); !errors.Is(statErr, os.ErrNotExist) {
		t.Fatalf("malformed request acquired host lock: %v", statErr)
	}
}
