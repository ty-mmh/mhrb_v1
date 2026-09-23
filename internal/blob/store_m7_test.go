package blob

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"mahoroba.local/mahoroba/internal/canonical"
	"mahoroba.local/mahoroba/internal/fssecure"
)

func TestM7FinalizeRejectsPostPublicationReplacement(t *testing.T) {
	store := newTestStore(t)
	residentID := testResident(t, "01ARZ3NDEKTSV4RRFFQ69G5FAV")
	staged, err := store.StageBytes(context.Background(), residentID, []byte("trusted"))
	if err != nil {
		t.Fatal(err)
	}
	store.testMutatePublished = func(handle *fssecure.Handle) error {
		return replaceHandleContent(handle, []byte("replacement"))
	}
	if _, err := store.Finalize(context.Background(), residentID, staged); !errors.Is(err, ErrDigestMismatch) {
		t.Fatalf("post-publication replacement error = %v", err)
	}
	if _, err := os.Stat(filepath.Join(store.stagingDir, staged.TemporaryName())); err != nil {
		t.Fatalf("staging evidence was lost: %v", err)
	}
}

func TestM7AcknowledgeRejectsStageReplacement(t *testing.T) {
	store := newTestStore(t)
	residentID := testResident(t, "01ARZ3NDEKTSV4RRFFQ69G5FAV")
	staged, err := store.StageBytes(context.Background(), residentID, []byte("trusted"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.Finalize(context.Background(), residentID, staged); err != nil {
		t.Fatal(err)
	}
	store.testMutateStage = func(handle *fssecure.Handle) error {
		return replaceHandleContent(handle, []byte("replacement"))
	}
	if err := store.Acknowledge(context.Background(), residentID, staged); !errors.Is(err, ErrUnsafeFilesystem) && !errors.Is(err, ErrInvalidStage) {
		t.Fatalf("stage replacement error = %v", err)
	}
	if _, err := os.Stat(filepath.Join(store.stagingDir, staged.TemporaryName())); err != nil {
		t.Fatalf("replacement evidence was lost: %v", err)
	}
}

func TestM7CleanupRejectsOrphanReplacement(t *testing.T) {
	store := newTestStore(t)
	residentID := testResident(t, "01ARZ3NDEKTSV4RRFFQ69G5FAV")
	if _, err := store.StageBytes(context.Background(), residentID, []byte("trusted")); err != nil {
		t.Fatal(err)
	}
	orphans, err := store.ListOrphans(context.Background())
	if err != nil || len(orphans) != 1 {
		t.Fatalf("orphans = %v, %v", orphans, err)
	}
	store.testHook = func(label string) {
		if label == "cleanup_after_reference_check" {
			path := filepath.Join(store.stagingDir, orphans[0].Name)
			if writeErr := os.WriteFile(path, []byte("replacement"), 0o600); writeErr != nil {
				t.Fatal(writeErr)
			}
		}
	}
	if _, err := store.CleanupOrphan(context.Background(), orphans[0], ReferenceCheckFunc(func(context.Context, canonical.ID, canonical.Digest) (bool, error) { return false, nil })); err == nil {
		t.Fatal("orphan replacement unexpectedly removed")
	}
}
