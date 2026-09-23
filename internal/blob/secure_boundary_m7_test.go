package blob

import (
	"errors"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"

	"mahoroba.local/mahoroba/internal/fssecure"
)

func TestM7FileStoreStaticFilesystemBoundary(t *testing.T) {
	_, file, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("runtime caller unavailable")
	}
	source, err := os.ReadFile(filepath.Join(filepath.Dir(file), "store.go"))
	if err != nil {
		t.Fatal(err)
	}
	for _, banned := range []string{
		"os.Stat(", "os.Lstat(", "os.Open(", "os.OpenFile(", "os.CreateTemp(",
		"os.Mkdir(", "os.MkdirAll(", "os.Rename(", "os.Remove(", "os.RemoveAll(", "os.ReadDir(",
	} {
		if strings.Contains(string(source), banned) {
			t.Fatalf("blob store contains direct filesystem primitive %q", banned)
		}
	}
}

func TestM7FileStoreRejectsUnsafeOwnerModeOrACL(t *testing.T) {
	store := newTestStore(t)
	makeManagedRootUnsafe(t, store.Root())
	_, err := store.StageBytes(t.Context(), testResident(t, "01ARZ3NDEKTSV4RRFFQ69G5FAV"), []byte("unsafe"))
	if !errors.Is(err, ErrUnsafeFilesystem) {
		t.Fatalf("unsafe root error = %v", err)
	}
}

func TestM7FileStoreFinalizeExistingObjectIsIdempotent(t *testing.T) {
	store := newTestStore(t)
	residentID := testResident(t, "01ARZ3NDEKTSV4RRFFQ69G5FAV")
	first, err := store.StageBytes(t.Context(), residentID, []byte("same"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.Finalize(t.Context(), residentID, first); err != nil {
		t.Fatal(err)
	}
	second, err := store.StageBytes(t.Context(), residentID, []byte("same"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.Finalize(t.Context(), residentID, second); err != nil {
		t.Fatal(err)
	}
}

func TestM7SecureFilesystemUnsupportedReasonIsSentinelCompatible(t *testing.T) {
	err := errors.Join(ErrUnsafeFilesystem, fssecure.WithReason(fssecure.ReasonUnsupportedFilesystem, errors.Join(fssecure.ErrUnsafeFilesystem, fssecure.ErrUnsupportedSecureFilesystem)))
	if !errors.Is(err, ErrUnsafeFilesystem) || !errors.Is(err, fssecure.ErrUnsafeFilesystem) || !errors.Is(err, fssecure.ErrUnsupportedSecureFilesystem) {
		t.Fatalf("unsupported reason is not sentinel-compatible: %v", err)
	}
	if reason, ok := fssecure.Reason(err); !ok || reason != fssecure.ReasonUnsupportedFilesystem {
		t.Fatalf("reason = %q/%v", reason, ok)
	}
}
