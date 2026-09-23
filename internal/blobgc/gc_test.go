package blobgc

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"runtime"
	"testing"

	"mahoroba.local/mahoroba/internal/blob"
	"mahoroba.local/mahoroba/internal/canonical"
)

func TestBlobGCCoreRequiresCurrentVersionedContentReferences(t *testing.T) {
	cases := []struct {
		name        string
		mutate      func(*Snapshot)
		wantCurrent bool
	}{
		{name: "current", wantCurrent: true},
		{name: "wrong name", mutate: func(value *Snapshot) { value.ProjectionName = "other" }},
		{name: "wrong version", mutate: func(value *Snapshot) { value.ProjectionVersion = "content-references-v2" }},
		{name: "nonempty dependency", mutate: func(value *Snapshot) {
			value.DependencyVersions = []DependencyVersion{{ProjectionName: "x", ProjectionVersion: "v1"}}
		}},
	}
	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			repository, files, residentID := blobGCFixture(t)
			if testCase.mutate != nil {
				testCase.mutate(&repository.snapshot)
			}
			_, err := Execute(context.Background(), repository, files, Request{ResidentID: residentID})
			if testCase.wantCurrent && err != nil {
				t.Fatalf("current Projection rejected: %v", err)
			}
			if !testCase.wantCurrent && !errors.Is(err, ErrProjectionNotCurrent) {
				t.Fatalf("mismatch error = %v, want ErrProjectionNotCurrent", err)
			}
		})
	}
}

func TestBlobGCCoreRejectsProjectionCaptureErrorsWithoutMutation(t *testing.T) {
	for _, testCase := range []struct {
		name string
		err  error
	}{
		{name: "unbuilt", err: ErrProjectionNotCurrent},
		{name: "stale watermark", err: ErrProjectionNotCurrent},
		{name: "rebuild conflict", err: ErrProjectionNotCurrent},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			repository, files, residentID := blobGCFixture(t)
			repository.captureErr = testCase.err
			result, err := Execute(context.Background(), repository, files, Request{ResidentID: residentID})
			if !errors.Is(err, ErrProjectionNotCurrent) {
				t.Fatalf("Execute error = %v", err)
			}
			if result.DeletedCount != 0 || files.removeCalls != 0 || repository.beginCalls != 0 {
				t.Fatalf("stale/unbuilt gate mutated state: result=%+v remove=%d begin=%d", result, files.removeCalls, repository.beginCalls)
			}
		})
	}
}

func TestBlobGCDryRunDigestApplyAndRetryRequireFreshPlan(t *testing.T) {
	repository, files, residentID := blobGCFixture(t)
	dry, err := Execute(context.Background(), repository, files, Request{ResidentID: residentID})
	if err != nil {
		t.Fatal(err)
	}
	if dry.CandidateCount != 1 || dry.CandidateBytes != 4 || dry.DeletedCount != 0 || dry.RemainingCount != 1 {
		t.Fatalf("dry run = %+v", dry)
	}
	if len(dry.Plan.Candidates) != 1 || dry.Plan.Digest() == "" {
		t.Fatalf("plan = %+v", dry.Plan)
	}
	second, err := Execute(context.Background(), repository, files, Request{ResidentID: residentID})
	if err != nil || second.Plan.Digest() != dry.Plan.Digest() {
		t.Fatalf("deterministic digest = %q/%q error=%v", dry.Plan.Digest(), second.Plan.Digest(), err)
	}
	applied, err := Execute(context.Background(), repository, files, Request{
		ResidentID: residentID, Apply: true, Confirm: dry.Plan.Digest(),
	})
	if err != nil {
		t.Fatal(err)
	}
	if applied.DeletedCount != 1 || applied.RemainingCount != 0 || repository.commitCalls != 1 {
		t.Fatalf("apply = %+v commits=%d", applied, repository.commitCalls)
	}
	if _, err := Execute(context.Background(), repository, files, Request{
		ResidentID: residentID, Apply: true, Confirm: dry.Plan.Digest(),
	}); !errors.Is(err, ErrPlanStale) {
		t.Fatalf("same confirm retry error = %v, want ErrPlanStale", err)
	}
}

func TestM7BlobGCPlanGoldenEmptyAndSQLiteOnly(t *testing.T) {
	repository, files, residentID := blobGCFixture(t)
	dry, err := Execute(context.Background(), repository, files, Request{ResidentID: residentID})
	if err != nil {
		t.Fatal(err)
	}
	wantSQLite := `{"candidates":[{"blob_hash":"sha256:9f86d081884c7d659a2feaa0c55ad015a3bf4f1b2b0b822cd15d6c15b0f00a08","filesystem_byte_size":null,"filesystem_present":false,"hash_algorithm":"sha256","resident_id":"01J00000000000000000000001","sqlite_byte_size":"4","sqlite_present":true}],"captured_head":{"commit_id":"01J00000000000000000000002","commit_seq":"7","committed_at":"100","committed_tz":"UTC","exists":true},"dependency_versions":[],"format_version":"mahoroba-blob-gc-plan-v1","projection_name":"content_references","projection_version":"content-references-v1","resident_id":"01J00000000000000000000001"}`
	encoded, err := dry.Plan.CanonicalBytes()
	if err != nil {
		t.Fatal(err)
	}
	if string(encoded) != wantSQLite || dry.Plan.Digest() != "sha256:14abc06e050f6e717b290f884a45ad9767f62da77422b0a2b7a60dd70b1dd531" {
		t.Fatalf("SQLite-only golden = %s / %s", encoded, dry.Plan.Digest())
	}

	repository.deleted[repository.snapshot.Locators[0].Digest] = true
	empty, err := Execute(context.Background(), repository, files, Request{ResidentID: residentID})
	if err != nil {
		t.Fatal(err)
	}
	wantEmpty := `{"candidates":[],"captured_head":{"commit_id":"01J00000000000000000000002","commit_seq":"7","committed_at":"100","committed_tz":"UTC","exists":true},"dependency_versions":[],"format_version":"mahoroba-blob-gc-plan-v1","projection_name":"content_references","projection_version":"content-references-v1","resident_id":"01J00000000000000000000001"}`
	encoded, err = empty.Plan.CanonicalBytes()
	if err != nil {
		t.Fatal(err)
	}
	if string(encoded) != wantEmpty || empty.Plan.Digest() != "sha256:158ae771e46e378454473798de980a917dd4a77685739b33f34b53206bb25137" {
		t.Fatalf("empty golden = %s / %s", encoded, empty.Plan.Digest())
	}
}

func TestM7BlobGCPlanGoldenFilesystemOnlyAndSharedLocators(t *testing.T) {
	files := newBlobGCFinalStore(t)
	residentID := mustBlobGCID(t, "01J00000000000000000000001")
	filesystemOnly := publishBlobGCTestObject(t, files, residentID, []byte("filesystem-only"))
	shared := publishBlobGCTestObject(t, files, residentID, []byte("shared"))
	databaseOnly := canonical.HashBlob([]byte("database-only"))
	databaseOnlySize, _ := canonical.NewByteSize(int64(len("database-only")))
	sharedSize, _ := canonical.NewByteSize(int64(len("shared")))
	repository, _, _ := blobGCFixture(t)
	repository.snapshot.Locators = []LocatorState{
		{ResidentID: residentID, HashAlgorithm: canonical.HashAlgorithm, Digest: databaseOnly, SQLitePresent: true, SQLiteByteSize: &databaseOnlySize},
		{ResidentID: residentID, HashAlgorithm: canonical.HashAlgorithm, Digest: shared, SQLitePresent: true, SQLiteByteSize: &sharedSize},
	}
	union := &filesystemUnionRepository{fakeBlobGCRepository: repository}

	result, err := Execute(context.Background(), union, files, Request{ResidentID: residentID})
	if err != nil {
		t.Fatal(err)
	}
	if result.CandidateCount != 3 || result.CandidateBytes != int64(len("database-only")+len("filesystem-only")+2*len("shared")) {
		t.Fatalf("union result = %+v", result)
	}
	presence := make(map[canonical.Digest][2]bool)
	for _, candidate := range result.Plan.Candidates {
		presence[candidate.Digest()] = [2]bool{candidate.SQLitePresent, candidate.FilesystemPresent}
	}
	if presence[databaseOnly] != [2]bool{true, false} || presence[filesystemOnly] != [2]bool{false, true} || presence[shared] != [2]bool{true, true} {
		t.Fatalf("union presence = %#v", presence)
	}
	encoded, err := result.Plan.CanonicalBytes()
	if err != nil {
		t.Fatal(err)
	}
	wantUnion := `{"candidates":[{"blob_hash":"sha256:879b72c493b31b23c2ff3e3f7cc7860fd8beb595f00b86f79e0a0fcc594d9ba9","filesystem_byte_size":null,"filesystem_present":false,"hash_algorithm":"sha256","resident_id":"01J00000000000000000000001","sqlite_byte_size":"13","sqlite_present":true},{"blob_hash":"sha256:a4d26868017c0ccffe2efe50944ef4211834660cca834c6e9f86dec6a88246fa","filesystem_byte_size":"6","filesystem_present":true,"hash_algorithm":"sha256","resident_id":"01J00000000000000000000001","sqlite_byte_size":"6","sqlite_present":true},{"blob_hash":"sha256:c51b9b74f4573909775cb7a323d5194452730a9120a7ce3efa4074669c52247b","filesystem_byte_size":"15","filesystem_present":true,"hash_algorithm":"sha256","resident_id":"01J00000000000000000000001","sqlite_byte_size":null,"sqlite_present":false}],"captured_head":{"commit_id":"01J00000000000000000000002","commit_seq":"7","committed_at":"100","committed_tz":"UTC","exists":true},"dependency_versions":[],"format_version":"mahoroba-blob-gc-plan-v1","projection_name":"content_references","projection_version":"content-references-v1","resident_id":"01J00000000000000000000001"}`
	if string(encoded) != wantUnion || result.Plan.Digest() != "sha256:cd67fbbd153bfc3bca1904e779e1371b07735032913e5d8e5ccb5adec9a3c565" {
		t.Fatalf("filesystem/shared union golden = %s / %s", encoded, result.Plan.Digest())
	}
	second, err := Execute(context.Background(), union, files, Request{ResidentID: residentID})
	if err != nil || second.Plan.Digest() != result.Plan.Digest() {
		t.Fatalf("filesystem union plan is not deterministic: %s / %s / %v", result.Plan.Digest(), second.Plan.Digest(), err)
	}
}

func TestM7BlobGCFilesystemCrashForcesPartialAndFreshDryRun(t *testing.T) {
	files := newBlobGCFinalStore(t)
	residentID := mustBlobGCID(t, "01J00000000000000000000001")
	publishBlobGCTestObject(t, files, residentID, []byte("filesystem-only"))
	repository, _, _ := blobGCFixture(t)
	repository.snapshot.Locators = nil
	union := &filesystemUnionRepository{fakeBlobGCRepository: repository}
	dry, err := Execute(context.Background(), union, files, Request{ResidentID: residentID})
	if err != nil || dry.CandidateCount != 1 {
		t.Fatalf("dry run = %+v, %v", dry, err)
	}
	result, err := Execute(context.Background(), union, files, Request{
		ResidentID: residentID, Apply: true, Confirm: dry.Plan.Digest(),
		Failpoint: func(name string) error {
			if name == "after_filesystem_delete" {
				return errors.New("injected crash")
			}
			return nil
		},
	})
	if !errors.Is(err, ErrPartial) || !result.PhysicalMutation || result.DeletedCount != 0 || result.RemainingCount != 1 {
		t.Fatalf("filesystem partial = %+v, %v", result, err)
	}
	fresh, err := Execute(context.Background(), union, files, Request{ResidentID: residentID})
	if err != nil || fresh.CandidateCount != 0 || fresh.Plan.Digest() == dry.Plan.Digest() {
		t.Fatalf("fresh dry run = %+v, %v", fresh, err)
	}
	if _, err := Execute(context.Background(), union, files, Request{
		ResidentID: residentID, Apply: true, Confirm: dry.Plan.Digest(),
	}); !errors.Is(err, ErrPlanStale) {
		t.Fatalf("stale partial retry error = %v", err)
	}
}

func TestBlobGCCoreRejectsFinalObjectDisappearanceAfterFreshPlan(t *testing.T) {
	files := newBlobGCFinalStore(t)
	residentID := mustBlobGCID(t, "01J00000000000000000000001")
	publishBlobGCTestObject(t, files, residentID, []byte("disappearing-final"))
	repository, _, _ := blobGCFixture(t)
	repository.snapshot.Locators = nil
	union := &filesystemUnionRepository{fakeBlobGCRepository: repository}
	vanishing := &vanishingFinalStore{FinalStore: files}
	dry, err := Execute(context.Background(), union, vanishing, Request{ResidentID: residentID})
	if err != nil || dry.CandidateCount != 1 {
		t.Fatalf("dry run=%+v error=%v", dry, err)
	}
	result, err := Execute(context.Background(), union, vanishing, Request{
		ResidentID: residentID, Apply: true, Confirm: dry.Plan.Digest(),
	})
	if !errors.Is(err, ErrPlanStale) || errors.Is(err, ErrPartial) {
		t.Fatalf("disappearance error=%v, want non-partial ErrPlanStale", err)
	}
	if result.DeletedCount != 0 || result.PhysicalMutation || result.RemainingCount != 1 ||
		vanishing.removeCalls != 1 || repository.beginCalls != 1 || repository.commitCalls != 0 {
		t.Fatalf("disappearance result=%+v remove=%d begin=%d commit=%d",
			result, vanishing.removeCalls, repository.beginCalls, repository.commitCalls)
	}
	fresh, freshErr := Execute(context.Background(), union, files, Request{ResidentID: residentID})
	if freshErr != nil || fresh.Plan.Digest() != dry.Plan.Digest() || fresh.CandidateCount != 1 {
		t.Fatalf("rollback changed fresh plan: result=%+v error=%v", fresh, freshErr)
	}
}

func TestM7BlobGCLaterFinalObjectDisappearanceIsPartialAndRequiresFreshPlan(t *testing.T) {
	files := newBlobGCFinalStore(t)
	residentID := mustBlobGCID(t, "01J00000000000000000000001")
	publishBlobGCTestObject(t, files, residentID, []byte("first-final"))
	publishBlobGCTestObject(t, files, residentID, []byte("second-final"))
	repository, _, _ := blobGCFixture(t)
	repository.snapshot.Locators = nil
	union := &filesystemUnionRepository{fakeBlobGCRepository: repository}
	vanishing := &vanishingAfterOneFinalStore{FinalStore: files}
	dry, err := Execute(context.Background(), union, vanishing, Request{ResidentID: residentID})
	if err != nil || dry.CandidateCount != 2 {
		t.Fatalf("dry run=%+v error=%v", dry, err)
	}
	result, err := Execute(context.Background(), union, vanishing, Request{
		ResidentID: residentID, Apply: true, Confirm: dry.Plan.Digest(),
	})
	if !errors.Is(err, ErrPartial) || !errors.Is(err, ErrPlanStale) {
		t.Fatalf("later disappearance error=%v, want partial plan stale", err)
	}
	if result.DeletedCount != 1 || !result.PhysicalMutation || result.RemainingCount != 1 ||
		vanishing.removeCalls != 2 || repository.beginCalls != 2 || repository.commitCalls != 1 {
		t.Fatalf("later disappearance result=%+v remove=%d begin=%d commit=%d",
			result, vanishing.removeCalls, repository.beginCalls, repository.commitCalls)
	}
	fresh, freshErr := Execute(context.Background(), union, files, Request{ResidentID: residentID})
	if freshErr != nil || fresh.CandidateCount != 1 || fresh.Plan.Digest() == dry.Plan.Digest() {
		t.Fatalf("fresh dry run after partial=%+v error=%v", fresh, freshErr)
	}
}

func newBlobGCFinalStore(t *testing.T) *blob.FileStore {
	t.Helper()
	files, err := blob.NewFileStore(filepath.Join(t.TempDir(), "blobs"))
	if runtime.GOOS == "windows" && os.Getenv("CI") == "" && errors.Is(err, os.ErrPermission) {
		t.Skipf("desktop sandbox cannot grant exact protected ACL: %v", err)
	}
	if err != nil {
		t.Fatal(err)
	}
	return files
}

func publishBlobGCTestObject(t *testing.T, files *blob.FileStore, residentID canonical.ID, content []byte) canonical.Digest {
	t.Helper()
	staged, err := files.Stage(context.Background(), residentID, bytes.NewReader(content))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := files.Finalize(context.Background(), residentID, staged); err != nil {
		t.Fatal(err)
	}
	if err := files.Acknowledge(context.Background(), residentID, staged); err != nil {
		t.Fatal(err)
	}
	return staged.Digest()
}

func TestBlobGCPartialRequiresNewDryRunAfterFilesystemSideEffect(t *testing.T) {
	repository, files, residentID := blobGCFixture(t)
	// Model a filesystem-only locator without manufacturing a blob token by
	// using a fake state whose final-store remove path records the side effect.
	// The core partial matrix itself is exercised at the post-commit failpoint,
	// where the locator is durably complete but reporting is interrupted.
	dry, err := Execute(context.Background(), repository, files, Request{ResidentID: residentID})
	if err != nil {
		t.Fatal(err)
	}
	result, err := Execute(context.Background(), repository, files, Request{
		ResidentID: residentID, Apply: true, Confirm: dry.Plan.Digest(),
		Failpoint: func(name string) error {
			if name == "after_candidate_commit" {
				return errors.New("crash")
			}
			return nil
		},
	})
	if !errors.Is(err, ErrPartial) || result.DeletedCount != 1 || result.RemainingCount != 0 {
		t.Fatalf("partial = %+v error=%v", result, err)
	}
	fresh, err := Execute(context.Background(), repository, files, Request{ResidentID: residentID})
	if err != nil || fresh.Plan.Digest() == dry.Plan.Digest() || fresh.CandidateCount != 0 {
		t.Fatalf("fresh dry run = %+v error=%v", fresh, err)
	}
}

func TestM7BlobGCCrashFailpointsRollbackUntilCandidateCommit(t *testing.T) {
	for _, testCase := range []struct {
		name        string
		failpoint   string
		wantPartial bool
		wantDeleted int64
		wantBegin   int
		wantCommit  int
	}{
		{name: "all-candidate preflight", failpoint: "after_all_candidates_preflight"},
		{name: "before filesystem delete", failpoint: "before_filesystem_delete", wantBegin: 1},
		{name: "after filesystem delete", failpoint: "after_filesystem_delete", wantBegin: 1},
		{name: "after SQLite delete", failpoint: "after_sqlite_delete", wantBegin: 1},
		{name: "before candidate commit", failpoint: "before_candidate_commit", wantBegin: 1},
		{name: "after candidate commit", failpoint: "after_candidate_commit", wantPartial: true, wantDeleted: 1, wantBegin: 1, wantCommit: 1},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			repository, files, residentID := blobGCFixture(t)
			dry, err := Execute(context.Background(), repository, files, Request{ResidentID: residentID})
			if err != nil {
				t.Fatal(err)
			}
			result, err := Execute(context.Background(), repository, files, Request{
				ResidentID: residentID, Apply: true, Confirm: dry.Plan.Digest(),
				Failpoint: func(name string) error {
					if name == testCase.failpoint {
						return errors.New("injected crash")
					}
					return nil
				},
			})
			if testCase.wantPartial != errors.Is(err, ErrPartial) {
				t.Fatalf("error = %v, want partial=%v", err, testCase.wantPartial)
			}
			if err == nil {
				t.Fatal("failpoint did not stop apply")
			}
			if result.DeletedCount != testCase.wantDeleted || repository.beginCalls != testCase.wantBegin || repository.commitCalls != testCase.wantCommit {
				t.Fatalf("result=%+v begin=%d commit=%d", result, repository.beginCalls, repository.commitCalls)
			}
			if !testCase.wantPartial {
				fresh, freshErr := Execute(context.Background(), repository, files, Request{ResidentID: residentID})
				if freshErr != nil || fresh.Plan.Digest() != dry.Plan.Digest() || fresh.CandidateCount != 1 {
					t.Fatalf("rollback changed fresh plan: result=%+v error=%v", fresh, freshErr)
				}
			}
		})
	}
}

func TestM7BlobGCDatabaseBoundarySwapMatrix(t *testing.T) {
	for _, failAt := range []int{1, 2, 3, 4, 5, 6} {
		t.Run(string(rune('0'+failAt)), func(t *testing.T) {
			repository, files, residentID := blobGCFixture(t)
			dry, err := Execute(context.Background(), repository, files, Request{ResidentID: residentID})
			if err != nil {
				t.Fatal(err)
			}
			boundary := &blobGCBoundaryProbe{failAt: failAt}
			result, err := Execute(context.Background(), repository, files, Request{
				ResidentID: residentID, Apply: true, Confirm: dry.Plan.Digest(), Boundary: boundary,
			})
			wantPartial := failAt >= 5
			if !errors.Is(err, ErrPlanStale) || errors.Is(err, ErrPartial) != wantPartial {
				t.Fatalf("boundary failure=%v wantPartial=%t", err, wantPartial)
			}
			wantBegin, wantCommit, wantDeleted := 0, 0, int64(0)
			if failAt >= 3 {
				wantBegin = 1
			}
			if failAt >= 5 {
				wantCommit, wantDeleted = 1, 1
			}
			if boundary.calls != failAt || repository.beginCalls != wantBegin ||
				repository.commitCalls != wantCommit || result.DeletedCount != wantDeleted ||
				result.PhysicalMutation != wantPartial {
				t.Fatalf("result=%+v boundary=%d begin=%d commit=%d",
					result, boundary.calls, repository.beginCalls, repository.commitCalls)
			}
			fresh, freshErr := Execute(context.Background(), repository, files, Request{ResidentID: residentID})
			if freshErr != nil {
				t.Fatal(freshErr)
			}
			wantCandidates := int64(1)
			if wantPartial {
				wantCandidates = 0
			}
			if fresh.CandidateCount != wantCandidates ||
				(fresh.Plan.Digest() == dry.Plan.Digest()) != !wantPartial {
				t.Fatalf("fresh result=%+v original=%s", fresh, dry.Plan.Digest())
			}
		})
	}
}

type blobGCBoundaryProbe struct {
	calls  int
	failAt int
}

func (probe *blobGCBoundaryProbe) Verify() error {
	probe.calls++
	if probe.calls == probe.failAt {
		return errors.New("injected GC database identity swap")
	}
	return nil
}

type fakeBlobGCRepository struct {
	snapshot    Snapshot
	captureErr  error
	deleted     map[canonical.Digest]bool
	beginCalls  int
	commitCalls int
}

type filesystemUnionRepository struct{ *fakeBlobGCRepository }

func (repository *filesystemUnionRepository) Capture(
	ctx context.Context,
	residentID canonical.ID,
	objects []blob.FinalObject,
) (Snapshot, error) {
	snapshot, err := repository.fakeBlobGCRepository.Capture(ctx, residentID, nil)
	if err != nil {
		return Snapshot{}, err
	}
	known := make(map[canonical.Digest]bool, len(snapshot.Locators))
	for _, locator := range snapshot.Locators {
		known[locator.Digest] = true
	}
	for _, object := range objects {
		if !known[object.Digest()] {
			snapshot.Locators = append(snapshot.Locators, LocatorState{
				ResidentID: residentID, HashAlgorithm: canonical.HashAlgorithm, Digest: object.Digest(),
			})
		}
	}
	return snapshot, nil
}

func (repository *fakeBlobGCRepository) Capture(context.Context, canonical.ID, []blob.FinalObject) (Snapshot, error) {
	if repository.captureErr != nil {
		return Snapshot{}, repository.captureErr
	}
	result := repository.snapshot
	result.Locators = nil
	for _, locator := range repository.snapshot.Locators {
		if !repository.deleted[locator.Digest] {
			result.Locators = append(result.Locators, locator)
		}
	}
	return result, nil
}

func (repository *fakeBlobGCRepository) BeginCandidate(_ context.Context, candidate Candidate, expected CapturedHead) (CandidateTransaction, error) {
	repository.beginCalls++
	if !repository.snapshot.CapturedHead.Equal(expected) || repository.deleted[candidate.Digest()] {
		return nil, ErrPlanStale
	}
	return &fakeBlobGCTx{repository: repository, digest: candidate.Digest(), present: candidate.SQLitePresent}, nil
}

func (*fakeBlobGCRepository) Maintenance(context.Context) error { return nil }

type fakeBlobGCTx struct {
	repository *fakeBlobGCRepository
	digest     canonical.Digest
	present    bool
	deleted    bool
	closed     bool
}

func (transaction *fakeBlobGCTx) DeleteSQLite(context.Context) (bool, error) {
	transaction.deleted = transaction.present
	return transaction.deleted, nil
}
func (transaction *fakeBlobGCTx) Commit(context.Context) error {
	transaction.closed = true
	transaction.repository.deleted[transaction.digest] = true
	transaction.repository.commitCalls++
	return nil
}
func (transaction *fakeBlobGCTx) Rollback(context.Context) error {
	transaction.closed = true
	return nil
}

type fakeFinalStore struct{ removeCalls int }

type vanishingFinalStore struct {
	FinalStore
	removeCalls int
}

func (store *vanishingFinalStore) RemoveFinal(context.Context, blob.FinalObject) (bool, error) {
	store.removeCalls++
	return false, nil
}

type vanishingAfterOneFinalStore struct {
	FinalStore
	removeCalls int
}

func (store *vanishingAfterOneFinalStore) RemoveFinal(
	ctx context.Context,
	object blob.FinalObject,
) (bool, error) {
	store.removeCalls++
	if store.removeCalls == 1 {
		return store.FinalStore.RemoveFinal(ctx, object)
	}
	return false, nil
}

func (*fakeFinalStore) WalkFinal(context.Context, canonical.ID) ([]blob.FinalObject, error) {
	return []blob.FinalObject{}, nil
}
func (store *fakeFinalStore) RemoveFinal(context.Context, blob.FinalObject) (bool, error) {
	store.removeCalls++
	return true, nil
}

func blobGCFixture(t *testing.T) (*fakeBlobGCRepository, *fakeFinalStore, canonical.ID) {
	t.Helper()
	residentID := mustBlobGCID(t, "01J00000000000000000000001")
	commitID := mustBlobGCID(t, "01J00000000000000000000002")
	commitSeq, _ := canonical.NewCommitSeq(7)
	committedAt := canonical.Instant(100)
	timezone := canonical.MustTimezone("UTC")
	digest := canonical.HashBlob([]byte("test"))
	size, _ := canonical.NewByteSize(4)
	return &fakeBlobGCRepository{
		snapshot: Snapshot{
			CapturedHead: CapturedHead{
				Exists: true, CommitID: &commitID, CommitSeq: &commitSeq,
				CommittedAt: &committedAt, CommittedTZ: &timezone,
			},
			ProjectionName: ProjectionName, ProjectionVersion: ProjectionVersion,
			DependencyVersions: []DependencyVersion{},
			Locators: []LocatorState{{
				ResidentID: residentID, HashAlgorithm: canonical.HashAlgorithm, Digest: digest,
				SQLitePresent: true, SQLiteByteSize: &size,
			}},
		},
		deleted: make(map[canonical.Digest]bool),
	}, &fakeFinalStore{}, residentID
}

func mustBlobGCID(t *testing.T, raw string) canonical.ID {
	t.Helper()
	id, err := canonical.ParseID(raw)
	if err != nil {
		t.Fatal(err)
	}
	return id
}
