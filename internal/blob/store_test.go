package blob

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"runtime"
	"testing"

	"mahoroba.local/mahoroba/internal/canonical"
	"mahoroba.local/mahoroba/internal/operationalmetrics"
)

func newTestStore(t *testing.T) *FileStore {
	t.Helper()
	store, err := NewFileStore(filepath.Join(t.TempDir(), "blob-root"))
	if err != nil {
		t.Fatal(err)
	}
	return store
}

func newBlobGCTestStore(t *testing.T) *FileStore {
	t.Helper()
	store, err := NewFileStore(filepath.Join(t.TempDir(), "blob-gc-root"))
	if runtime.GOOS == "windows" && os.Getenv("CI") == "" && errors.Is(err, os.ErrPermission) {
		t.Skipf("desktop sandbox cannot grant the exact protected ACL: %v", err)
	}
	if err != nil {
		t.Fatal(err)
	}
	return store
}

func TestM7OpenFileStoreExistingDoesNotInitializeMissingSource(t *testing.T) {
	root := filepath.Join(t.TempDir(), "missing-blob-root")
	if store, err := OpenFileStoreExisting(root); err == nil {
		t.Fatalf("missing existing store opened: %#v", store)
	}
	if _, err := os.Lstat(root); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("existing-store open initialized source: %v", err)
	}
	if store, err := OpenFileStoreReadOnly(root); err == nil {
		t.Fatalf("missing read-only store opened: %#v", store)
	}
	if _, err := os.Lstat(root); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("read-only open initialized source: %v", err)
	}
}

func TestFileStoreStageFinalizeAcknowledgeAndVerifiedRead(t *testing.T) {
	store := newTestStore(t)
	residentID := testResident(t, "01ARZ3NDEKTSV4RRFFQ69G5FAV")
	content := []byte{0x00, 0xff, 'm', 'h', 'r', 'b', '\n'}
	staged, err := store.StageBytes(context.Background(), residentID, content)
	if err != nil {
		t.Fatal(err)
	}
	if staged.ResidentID() != residentID || staged.Digest() != canonical.HashBlob(content) || staged.Size().Int64() != int64(len(content)) {
		t.Fatalf("stage metadata = %s/%s/%d", staged.ResidentID(), staged.Digest(), staged.Size().Int64())
	}
	assertOrphanCount(t, store, 1)

	object, err := store.Finalize(context.Background(), residentID, staged)
	if err != nil {
		t.Fatal(err)
	}
	if object.ResidentID != residentID || object.Digest != staged.Digest() || object.Size != staged.Size() {
		t.Fatalf("object metadata = %+v", object)
	}
	// Finalize deliberately retains the marker until the Canonical commit is
	// acknowledged.
	assertOrphanCount(t, store, 1)
	if err := store.Acknowledge(context.Background(), residentID, staged); err != nil {
		t.Fatal(err)
	}
	assertOrphanCount(t, store, 0)

	got, err := store.Read(context.Background(), residentID, object.Digest)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, content) {
		t.Fatalf("read = %x, want %x", got, content)
	}
	exists, err := store.Exists(residentID, object.Digest)
	if err != nil || !exists {
		t.Fatalf("Exists = %v, %v", exists, err)
	}
}

func TestM7BlobRecoveryObserverReceivesOnlyAggregateOrphanCount(t *testing.T) {
	store, err := NewFileStore(filepath.Join(t.TempDir(), "observer-blob-root"))
	if runtime.GOOS == "windows" && os.Getenv("CI") == "" && errors.Is(err, os.ErrPermission) {
		t.Skipf("desktop sandbox cannot grant the exact protected ACL: %v", err)
	}
	if err != nil {
		t.Fatal(err)
	}
	metrics := operationalmetrics.New()
	store.SetRecoveryObserver(metrics)
	residentID := testResident(t, "01ARZ3NDEKTSV4RRFFQ69G5FAV")
	staged, err := store.StageBytes(context.Background(), residentID, []byte("content never passed to observer"))
	if runtime.GOOS == "windows" && os.Getenv("CI") == "" && errors.Is(err, os.ErrPermission) {
		t.Skipf("desktop sandbox cannot reopen the exact protected ACL: %v", err)
	}
	if err != nil {
		t.Fatal(err)
	}
	if orphans, err := store.ListOrphans(context.Background()); err != nil || len(orphans) != 1 {
		t.Fatalf("ListOrphans = %+v, %v", orphans, err)
	}
	if got := metrics.Snapshot().BlobRecovery.StagingOrphanCount; got != 1 {
		t.Fatalf("staging orphan count = %d", got)
	}
	if _, err := store.Finalize(context.Background(), residentID, staged); err != nil {
		t.Fatal(err)
	}
	if err := store.Acknowledge(context.Background(), residentID, staged); err != nil {
		t.Fatal(err)
	}
	if orphans, err := store.ListOrphans(context.Background()); err != nil || len(orphans) != 0 {
		t.Fatalf("ListOrphans after acknowledge = %+v, %v", orphans, err)
	}
	if got := metrics.Snapshot().BlobRecovery.StagingOrphanCount; got != 0 {
		t.Fatalf("staging orphan count after acknowledge = %d", got)
	}
}

func TestM7FileStoreWalkAndRemoveFinalUseOpaqueValidatedHandle(t *testing.T) {
	store := newBlobGCTestStore(t)
	residentID := testResident(t, "01ARZ3NDEKTSV4RRFFQ69G5FAV")
	content := []byte("unreferenced physical copy")
	staged, err := store.StageBytes(context.Background(), residentID, content)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.Finalize(context.Background(), residentID, staged); err != nil {
		t.Fatal(err)
	}
	if err := store.Acknowledge(context.Background(), residentID, staged); err != nil {
		t.Fatal(err)
	}
	objects, err := store.WalkFinal(context.Background(), residentID)
	if err != nil {
		t.Fatal(err)
	}
	if len(objects) != 1 || objects[0].ResidentID() != residentID || objects[0].Digest() != staged.Digest() ||
		objects[0].Size() != staged.Size() || objects[0].HashAlgorithm() != canonical.HashAlgorithm {
		t.Fatalf("WalkFinal = %#v", objects)
	}
	removed, err := store.RemoveFinal(context.Background(), objects[0])
	if err != nil || !removed {
		t.Fatalf("RemoveFinal = %v, %v", removed, err)
	}
	exists, err := store.Exists(residentID, staged.Digest())
	if err != nil || exists {
		t.Fatalf("removed object exists=%v error=%v", exists, err)
	}
	removed, err = store.RemoveFinal(context.Background(), objects[0])
	if err != nil || removed {
		t.Fatalf("idempotent RemoveFinal = %v, %v", removed, err)
	}
}

func TestM7FileStoreRemoveFinalRejectsIdentitySwapWithoutDeletingReplacement(t *testing.T) {
	store := newBlobGCTestStore(t)
	residentID := testResident(t, "01ARZ3NDEKTSV4RRFFQ69G5FAV")
	content := []byte("identity-bound final object")
	staged, err := store.StageBytes(context.Background(), residentID, content)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.Finalize(context.Background(), residentID, staged); err != nil {
		t.Fatal(err)
	}
	if err := store.Acknowledge(context.Background(), residentID, staged); err != nil {
		t.Fatal(err)
	}
	objects, err := store.WalkFinal(context.Background(), residentID)
	if err != nil || len(objects) != 1 {
		t.Fatalf("WalkFinal = %#v, %v", objects, err)
	}
	path, err := store.objectPath(residentID, staged.Digest(), false)
	if err != nil {
		t.Fatal(err)
	}
	var originalInfo os.FileInfo
	if runtime.GOOS == "linux" {
		originalInfo, err = os.Stat(path)
		if err != nil {
			t.Fatal(err)
		}
	}
	if err := os.Remove(path); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, content, 0o600); err != nil {
		t.Fatal(err)
	}
	if runtime.GOOS == "linux" {
		replacementInfo, statErr := os.Stat(path)
		if statErr != nil {
			t.Fatal(statErr)
		}
		replacementObjects, walkErr := store.WalkFinal(context.Background(), residentID)
		if walkErr != nil || len(replacementObjects) != 1 {
			t.Fatalf("replacement WalkFinal = %#v, %v", replacementObjects, walkErr)
		}
		if objects[0].fileIdentity.Equal(replacementObjects[0].fileIdentity) {
			t.Fatal("replacement generation was accepted as the original identity")
		}
		if os.SameFile(originalInfo, replacementInfo) {
			t.Log("Linux filesystem reused the device/inode; statx generation rejected the ABA replacement")
		}
	}
	removed, err := store.RemoveFinal(context.Background(), objects[0])
	if err == nil || removed {
		t.Fatalf("identity-swapped RemoveFinal = %v, %v", removed, err)
	}
	if _, statErr := os.Stat(path); statErr != nil {
		t.Fatalf("replacement was deleted: %v", statErr)
	}
}

func TestFileStoreDeduplicatesOnlyWithinResidentScope(t *testing.T) {
	store := newTestStore(t)
	residentA := testResident(t, "01ARZ3NDEKTSV4RRFFQ69G5FAV")
	residentB := testResident(t, "01ARZ3NDEKTSV4RRFFQ69G5FAW")
	content := []byte("same logical content")

	first, err := store.StageBytes(context.Background(), residentA, content)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.Finalize(context.Background(), residentA, first); err != nil {
		t.Fatal(err)
	}
	if err := store.Acknowledge(context.Background(), residentA, first); err != nil {
		t.Fatal(err)
	}
	duplicate, err := store.StageBytes(context.Background(), residentA, content)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.Finalize(context.Background(), residentA, duplicate); err != nil {
		t.Fatal(err)
	}
	if err := store.Acknowledge(context.Background(), residentA, duplicate); err != nil {
		t.Fatal(err)
	}

	crossScope, err := store.StageBytes(context.Background(), residentB, content)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.Finalize(context.Background(), residentB, crossScope); err != nil {
		t.Fatal(err)
	}
	if err := store.Acknowledge(context.Background(), residentB, crossScope); err != nil {
		t.Fatal(err)
	}
	assertOrphanCount(t, store, 0)

	pathA, err := store.objectPath(residentA, canonical.HashBlob(content), false)
	if err != nil {
		t.Fatal(err)
	}
	pathB, err := store.objectPath(residentB, canonical.HashBlob(content), false)
	if err != nil {
		t.Fatal(err)
	}
	infoA, err := os.Stat(pathA)
	if err != nil {
		t.Fatal(err)
	}
	infoB, err := os.Stat(pathB)
	if err != nil {
		t.Fatal(err)
	}
	if os.SameFile(infoA, infoB) {
		t.Fatal("cross-resident objects unexpectedly share the same physical file")
	}
	if err := os.WriteFile(pathA, []byte("scope A corrupt"), 0o600); err != nil {
		t.Fatal(err)
	}
	if got, err := store.Read(context.Background(), residentB, canonical.HashBlob(content)); err != nil || !bytes.Equal(got, content) {
		t.Fatalf("scope B changed with scope A: %q, %v", got, err)
	}
}

func TestFileStorePreservesAndCleansUnfinalizedStages(t *testing.T) {
	root := filepath.Join(t.TempDir(), "blob-root")
	store, err := NewFileStore(root)
	if err != nil {
		t.Fatal(err)
	}
	residentID := testResident(t, "01ARZ3NDEKTSV4RRFFQ69G5FAV")
	if _, err := store.StageBytes(context.Background(), residentID, []byte("orphan")); err != nil {
		t.Fatal(err)
	}
	cancelled, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := store.Stage(cancelled, residentID, bytes.NewReader([]byte("partial"))); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled stage error = %v", err)
	}

	reopened, err := NewFileStore(root)
	if err != nil {
		t.Fatal(err)
	}
	orphans, err := reopened.ListOrphans(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(orphans) != 2 {
		t.Fatalf("preserved orphans = %v, want 2", orphans)
	}
	for _, orphan := range orphans {
		result, err := reopened.CleanupOrphan(context.Background(), orphan, ReferenceCheckFunc(
			func(context.Context, canonical.ID, canonical.Digest) (bool, error) { return false, nil },
		))
		if err != nil {
			t.Fatal(err)
		}
		if result.Disposition != CleanupPartialRemoved && result.Disposition != CleanupUnpublishedStageRemoved {
			t.Fatalf("cleanup result = %+v", result)
		}
	}
	assertOrphanCount(t, reopened, 0)
}

func TestFileStoreCleanupRestoresReferencedObjectFromSealedMarker(t *testing.T) {
	store := newTestStore(t)
	residentID := testResident(t, "01ARZ3NDEKTSV4RRFFQ69G5FAV")
	content := []byte("committed content whose published link was lost")
	staged, err := store.StageBytes(context.Background(), residentID, content)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.Finalize(context.Background(), residentID, staged); err != nil {
		t.Fatal(err)
	}
	objectPath, err := store.objectPath(residentID, staged.Digest(), false)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(objectPath); err != nil {
		t.Fatal(err)
	}
	orphans, err := store.ListOrphans(context.Background())
	if err != nil || len(orphans) != 1 {
		t.Fatalf("orphans = %v, %v", orphans, err)
	}
	checks := 0
	result, err := store.CleanupOrphan(context.Background(), orphans[0], ReferenceCheckFunc(
		func(_ context.Context, gotResident canonical.ID, gotDigest canonical.Digest) (bool, error) {
			checks++
			return gotResident == residentID && gotDigest == staged.Digest(), nil
		},
	))
	if err != nil {
		t.Fatal(err)
	}
	if checks != 1 || result.Disposition != CleanupReferencedObjectPreserved {
		t.Fatalf("checks=%d cleanup result=%+v", checks, result)
	}
	got, err := store.Read(context.Background(), residentID, staged.Digest())
	if err != nil || !bytes.Equal(got, content) {
		t.Fatalf("restored content = %q, %v", got, err)
	}
	assertOrphanCount(t, store, 0)
}

func TestFileStoreCleanupSealedMissingObjectFailsClosedWithoutReferenceCheck(t *testing.T) {
	store := newTestStore(t)
	residentID := testResident(t, "01ARZ3NDEKTSV4RRFFQ69G5FAV")
	if _, err := store.StageBytes(context.Background(), residentID, []byte("possibly committed")); err != nil {
		t.Fatal(err)
	}
	orphans, err := store.ListOrphans(context.Background())
	if err != nil || len(orphans) != 1 {
		t.Fatalf("orphans = %v, %v", orphans, err)
	}
	if _, err := store.CleanupOrphan(context.Background(), orphans[0], nil); err == nil {
		t.Fatal("sealed cleanup unexpectedly skipped its Canonical reference check")
	}
	assertOrphanCount(t, store, 1)
}

func TestFileStoreCleanupRequiresReferenceCheckAndPreservesLiveObject(t *testing.T) {
	store := newTestStore(t)
	residentID := testResident(t, "01ARZ3NDEKTSV4RRFFQ69G5FAV")
	content := []byte("committed content")
	staged, err := store.StageBytes(context.Background(), residentID, content)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.Finalize(context.Background(), residentID, staged); err != nil {
		t.Fatal(err)
	}
	orphans, err := store.ListOrphans(context.Background())
	if err != nil || len(orphans) != 1 {
		t.Fatalf("orphans = %v, %v", orphans, err)
	}
	if _, err := store.CleanupOrphan(context.Background(), orphans[0], nil); err == nil {
		t.Fatal("finalized cleanup unexpectedly accepted no reference checker")
	}
	result, err := store.CleanupOrphan(context.Background(), orphans[0], ReferenceCheckFunc(
		func(_ context.Context, gotResident canonical.ID, gotDigest canonical.Digest) (bool, error) {
			return gotResident == residentID && gotDigest == canonical.HashBlob(content), nil
		},
	))
	if err != nil {
		t.Fatal(err)
	}
	if result.Disposition != CleanupReferencedObjectPreserved {
		t.Fatalf("cleanup result = %+v", result)
	}
	if exists, err := store.Exists(residentID, canonical.HashBlob(content)); err != nil || !exists {
		t.Fatalf("live object after cleanup = %v, %v", exists, err)
	}
	assertOrphanCount(t, store, 0)
}

func TestFileStoreCleanupRemovesUnreferencedFinalObject(t *testing.T) {
	store := newTestStore(t)
	residentID := testResident(t, "01ARZ3NDEKTSV4RRFFQ69G5FAV")
	content := []byte("rolled back content")
	staged, err := store.StageBytes(context.Background(), residentID, content)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.Finalize(context.Background(), residentID, staged); err != nil {
		t.Fatal(err)
	}
	orphans, err := store.ListOrphans(context.Background())
	if err != nil || len(orphans) != 1 {
		t.Fatalf("orphans = %v, %v", orphans, err)
	}
	result, err := store.CleanupOrphan(context.Background(), orphans[0], ReferenceCheckFunc(
		func(context.Context, canonical.ID, canonical.Digest) (bool, error) { return false, nil },
	))
	if err != nil {
		t.Fatal(err)
	}
	if result.Disposition != CleanupUnreferencedObjectRemoved {
		t.Fatalf("cleanup result = %+v", result)
	}
	if exists, err := store.Exists(residentID, canonical.HashBlob(content)); err != nil || exists {
		t.Fatalf("orphan object after cleanup = %v, %v", exists, err)
	}
	assertOrphanCount(t, store, 0)
}

func TestFileStoreRejectsScopeMismatchForgedStageOrphanAndCorruption(t *testing.T) {
	store := newTestStore(t)
	residentA := testResident(t, "01ARZ3NDEKTSV4RRFFQ69G5FAV")
	residentB := testResident(t, "01ARZ3NDEKTSV4RRFFQ69G5FAW")
	staged, err := store.StageBytes(context.Background(), residentA, []byte("valid"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.Finalize(context.Background(), residentB, staged); !errors.Is(err, ErrInvalidStage) {
		t.Fatalf("scope mismatch error = %v", err)
	}

	outside := filepath.Join(t.TempDir(), "stage-forged")
	if err := os.WriteFile(outside, []byte("outside"), 0o600); err != nil {
		t.Fatal(err)
	}
	forged := Staged{
		name: filepath.Base(outside), residentID: residentA,
		digest: canonical.HashBlob([]byte("outside")), size: canonical.ByteSize(len("outside")),
	}
	if _, err := store.Finalize(context.Background(), residentA, forged); !errors.Is(err, ErrInvalidStage) {
		t.Fatalf("forged stage error = %v", err)
	}
	orphans, err := store.ListOrphans(context.Background())
	if err != nil || len(orphans) != 1 {
		t.Fatalf("orphans = %v, %v", orphans, err)
	}
	forgedOrphan := orphans[0]
	forgedOrphan.Name = ".." + string(filepath.Separator) + filepath.Base(forgedOrphan.Name)
	if _, err := store.CleanupOrphan(context.Background(), forgedOrphan, nil); !errors.Is(err, ErrInvalidOrphan) {
		t.Fatalf("forged orphan error = %v", err)
	}

	object, err := store.Finalize(context.Background(), residentA, staged)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Acknowledge(context.Background(), residentA, staged); err != nil {
		t.Fatal(err)
	}
	path, err := store.objectPath(residentA, object.Digest, false)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte("corrupt"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Read(context.Background(), residentA, object.Digest); !errors.Is(err, ErrDigestMismatch) {
		t.Fatalf("corrupt read error = %v", err)
	}
}

func TestFileStoreRejectsSymlinkRecoveryCandidate(t *testing.T) {
	store := newTestStore(t)
	target := filepath.Join(t.TempDir(), "target")
	if err := os.WriteFile(target, []byte("target"), 0o600); err != nil {
		t.Fatal(err)
	}
	linkName := "partial-forged"
	link := filepath.Join(store.stagingDir, linkName)
	if err := os.Symlink(target, link); err != nil {
		t.Skipf("symlink creation unavailable: %v", err)
	}
	if orphans, err := store.ListOrphans(context.Background()); err == nil || !errors.Is(err, ErrUnsafeFilesystem) || len(orphans) != 0 {
		t.Fatalf("symlink candidates = %v, %v", orphans, err)
	}
	forged := Orphan{Name: linkName, Kind: OrphanPartial}
	if _, err := store.CleanupOrphan(context.Background(), forged, nil); !errors.Is(err, ErrInvalidOrphan) {
		t.Fatalf("symlink cleanup error = %v", err)
	}
	if _, err := os.Stat(target); err != nil {
		t.Fatalf("symlink target was affected: %v", err)
	}
}

func testResident(t *testing.T, text string) canonical.ID {
	t.Helper()
	id, err := canonical.ParseID(text)
	if err != nil {
		t.Fatal(err)
	}
	return id
}

func assertOrphanCount(t *testing.T, store *FileStore, want int) {
	t.Helper()
	orphans, err := store.ListOrphans(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(orphans) != want {
		t.Fatalf("orphans = %v, want %d", orphans, want)
	}
}
