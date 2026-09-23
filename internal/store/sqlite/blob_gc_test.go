package sqlite

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"

	"mahoroba.local/mahoroba/internal/blob"
	"mahoroba.local/mahoroba/internal/blobgc"
	"mahoroba.local/mahoroba/internal/canonical"
	"mahoroba.local/mahoroba/internal/projection"
)

func TestM7I88BlobGCRequiresCurrentVersionedContentReferences(t *testing.T) {
	for _, testCase := range []struct {
		name    string
		mutate  func(*testing.T, *blobGCProductionFixture)
		wantErr bool
	}{
		{name: "current"},
		{name: "wrong version", wantErr: true, mutate: func(t *testing.T, fixture *blobGCProductionFixture) {
			fixture.exec(t, `UPDATE projection_watermarks SET projection_version = 'content-references-v2'
				WHERE projection_name = 'content_references' AND resident_id = ?`, fixture.residentID.String())
		}},
		{name: "watermark head differs", wantErr: true, mutate: func(t *testing.T, fixture *blobGCProductionFixture) {
			fixture.exec(t, `UPDATE projection_watermarks SET source_commit_seq = source_commit_seq + 1
				WHERE projection_name = 'content_references' AND resident_id = ?`, fixture.residentID.String())
		}},
		{name: "nonempty dependency", wantErr: true, mutate: func(t *testing.T, fixture *blobGCProductionFixture) {
			fixture.addDependency(t)
		}},
		{name: "body differs", wantErr: true, mutate: func(t *testing.T, fixture *blobGCProductionFixture) {
			fixture.exec(t, `DELETE FROM content_references
				WHERE resident_id = ? AND referrer_kind = 'resident_revision'`, fixture.residentID.String())
		}},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			fixture := newBlobGCProductionFixture(t)
			dry := fixture.dryRun(t)
			if dry.CandidateCount != 1 || dry.DeletedCount != 0 || dry.RemainingCount != 1 {
				t.Fatalf("dry result = %+v", dry)
			}
			if testCase.mutate != nil {
				testCase.mutate(t, fixture)
			}
			result, err := blobgc.Execute(context.Background(), fixture.database.BlobGC(), fixture.files, blobgc.Request{
				ResidentID: fixture.residentID, Apply: true, Confirm: dry.Plan.Digest(),
			})
			if !testCase.wantErr {
				if err != nil || result.DeletedCount != 1 || result.RemainingCount != 0 {
					t.Fatalf("current apply result=%+v error=%v", result, err)
				}
				fixture.restart(t)
				fixture.assertOrphanCopies(t, false, false)
				return
			}
			if !errors.Is(err, blobgc.ErrProjectionNotCurrent) || errors.Is(err, blobgc.ErrPartial) ||
				result.DeletedCount != 0 || result.PhysicalMutation {
				t.Fatalf("projection mismatch result=%+v error=%v", result, err)
			}
			fixture.restart(t)
			fixture.assertOrphanCopies(t, true, true)
		})
	}
}

func TestM7RTI19BlobGCRejectsStaleOrUnbuiltContentReferences(t *testing.T) {
	for _, testCase := range []struct {
		name   string
		mutate func(*testing.T, *blobGCProductionFixture)
	}{
		{name: "unbuilt watermark and body absent", mutate: func(t *testing.T, fixture *blobGCProductionFixture) {
			fixture.exec(t, `DELETE FROM content_references WHERE resident_id = ?`, fixture.residentID.String())
			fixture.exec(t, `DELETE FROM projection_watermarks
				WHERE projection_name = 'content_references' AND resident_id = ?`, fixture.residentID.String())
		}},
		{name: "watermark absent body retained", mutate: func(t *testing.T, fixture *blobGCProductionFixture) {
			fixture.exec(t, `DELETE FROM projection_watermarks
				WHERE projection_name = 'content_references' AND resident_id = ?`, fixture.residentID.String())
		}},
		{name: "stale watermark", mutate: func(t *testing.T, fixture *blobGCProductionFixture) {
			fixture.exec(t, `UPDATE projection_watermarks SET source_commit_seq = source_commit_seq + 1
				WHERE projection_name = 'content_references' AND resident_id = ?`, fixture.residentID.String())
		}},
		{name: "Canonical head advanced", mutate: func(t *testing.T, fixture *blobGCProductionFixture) {
			fixture.exec(t, `INSERT INTO canonical_commits(
				canonical_commit_id,commit_seq,resident_id,committed_at,committed_tz)
				VALUES (?,2,NULL,200,'UTC')`, blobGCTestID(t, "01J00000000000000000000008").String())
		}},
		{name: "dependency set nonempty", mutate: func(t *testing.T, fixture *blobGCProductionFixture) {
			fixture.addDependency(t)
		}},
		{name: "required body row omitted", mutate: func(t *testing.T, fixture *blobGCProductionFixture) {
			fixture.exec(t, `DELETE FROM content_references
				WHERE resident_id = ? AND referrer_kind = 'resident_revision'`, fixture.residentID.String())
		}},
		{name: "body row extra", mutate: func(t *testing.T, fixture *blobGCProductionFixture) {
			fixture.exec(t, `INSERT INTO content_references(
				content_id,resident_id,referrer_kind,referrer_id,referrer_field,
				dedupe_scope_id,blob_hash_algorithm,blob_hash)
				VALUES (?,?,'forged',?,'invented',?,'sha256',?)`, fixture.liveContentID.String(),
				fixture.residentID.String(), blobGCTestID(t, "01J00000000000000000000009").String(),
				fixture.residentID.String(), fixture.liveDigest.Bytes())
		}},
		{name: "locator mismatch", mutate: func(t *testing.T, fixture *blobGCProductionFixture) {
			fixture.exec(t, `UPDATE content_references SET blob_hash = ?
				WHERE resident_id = ? AND referrer_kind = 'resident_revision'`,
				fixture.orphanDigest.Bytes(), fixture.residentID.String())
		}},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			fixture := newBlobGCProductionFixture(t)
			dry := fixture.dryRun(t)
			testCase.mutate(t, fixture)
			result, err := blobgc.Execute(context.Background(), fixture.database.BlobGC(), fixture.files, blobgc.Request{
				ResidentID: fixture.residentID, Apply: true, Confirm: dry.Plan.Digest(),
			})
			if !errors.Is(err, blobgc.ErrProjectionNotCurrent) || errors.Is(err, blobgc.ErrPartial) ||
				result.DeletedCount != 0 || result.PhysicalMutation {
				t.Fatalf("stale/unbuilt result=%+v error=%v", result, err)
			}
			fixture.restart(t)
			fixture.assertOrphanCopies(t, true, true)
		})
	}
}

func TestM7BlobGCRejectsFinalObjectDisappearanceAfterFreshPlan(t *testing.T) {
	fixture := newBlobGCProductionFixture(t)
	dry := fixture.dryRun(t)
	vanishing := &productionVanishingFinalStore{FinalStore: fixture.files}
	result, err := blobgc.Execute(context.Background(), fixture.database.BlobGC(), vanishing, blobgc.Request{
		ResidentID: fixture.residentID, Apply: true, Confirm: dry.Plan.Digest(),
	})
	if !errors.Is(err, blobgc.ErrPlanStale) || errors.Is(err, blobgc.ErrPartial) ||
		result.DeletedCount != 0 || result.RemainingCount != 1 || result.PhysicalMutation || vanishing.removeCalls != 1 {
		t.Fatalf("disappearance result=%+v removeCalls=%d error=%v", result, vanishing.removeCalls, err)
	}
	fixture.assertOrphanCopies(t, true, false)
	fixture.restart(t)
	fixture.assertOrphanCopies(t, true, false)

	fresh := fixture.dryRun(t)
	if fresh.CandidateCount != 1 || fresh.CandidateBytes != int64(len(fixture.orphanContent)) ||
		fresh.Plan.Digest() == dry.Plan.Digest() {
		t.Fatalf("fresh SQLite-only plan=%+v oldDigest=%s", fresh, dry.Plan.Digest())
	}
	stale, staleErr := blobgc.Execute(context.Background(), fixture.database.BlobGC(), fixture.files, blobgc.Request{
		ResidentID: fixture.residentID, Apply: true, Confirm: dry.Plan.Digest(),
	})
	if !errors.Is(staleErr, blobgc.ErrPlanStale) || stale.DeletedCount != 0 || stale.PhysicalMutation {
		t.Fatalf("old confirmation result=%+v error=%v", stale, staleErr)
	}
	fixture.assertOrphanCopies(t, true, false)
	converged, convergeErr := blobgc.Execute(context.Background(), fixture.database.BlobGC(), fixture.files, blobgc.Request{
		ResidentID: fixture.residentID, Apply: true, Confirm: fresh.Plan.Digest(),
	})
	if convergeErr != nil || converged.DeletedCount != 1 || converged.RemainingCount != 0 {
		t.Fatalf("fresh confirmation result=%+v error=%v", converged, convergeErr)
	}
	fixture.restart(t)
	fixture.assertOrphanCopies(t, false, false)
}

func TestM7BlobGCCrossLayerCrashRecoveryUsesLiveFilesystem(t *testing.T) {
	for _, testCase := range []struct {
		name               string
		failpoint          string
		wantPartial        bool
		wantDeleted        int64
		wantSQLite         bool
		wantFilesystem     bool
		wantOldConfirmLive bool
	}{
		{name: "after all candidates preflight", failpoint: "after_all_candidates_preflight", wantSQLite: true, wantFilesystem: true, wantOldConfirmLive: true},
		{name: "before filesystem delete", failpoint: "before_filesystem_delete", wantSQLite: true, wantFilesystem: true, wantOldConfirmLive: true},
		{name: "after filesystem delete", failpoint: "after_filesystem_delete", wantPartial: true, wantSQLite: true},
		{name: "after SQLite delete", failpoint: "after_sqlite_delete", wantPartial: true, wantSQLite: true},
		{name: "before candidate commit", failpoint: "before_candidate_commit", wantPartial: true, wantSQLite: true},
		{name: "after candidate commit", failpoint: "after_candidate_commit", wantPartial: true, wantDeleted: 1},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			fixture := newBlobGCProductionFixture(t)
			dry := fixture.dryRun(t)
			result, err := blobgc.Execute(context.Background(), fixture.database.BlobGC(), fixture.files, blobgc.Request{
				ResidentID: fixture.residentID, Apply: true, Confirm: dry.Plan.Digest(),
				Failpoint: func(name string) error {
					if name == testCase.failpoint {
						return errors.New("injected cross-layer crash")
					}
					return nil
				},
			})
			if err == nil || errors.Is(err, blobgc.ErrPartial) != testCase.wantPartial ||
				result.PhysicalMutation != testCase.wantPartial || result.DeletedCount != testCase.wantDeleted ||
				result.RemainingCount != 1-testCase.wantDeleted {
				t.Fatalf("failpoint result=%+v error=%v wantPartial=%t", result, err, testCase.wantPartial)
			}
			fixture.restart(t)
			fixture.assertOrphanCopies(t, testCase.wantSQLite, testCase.wantFilesystem)
			fresh := fixture.dryRun(t)
			if testCase.wantOldConfirmLive {
				if fresh.Plan.Digest() != dry.Plan.Digest() || fresh.CandidateCount != 1 {
					t.Fatalf("mutation-free fresh plan=%+v oldDigest=%s", fresh, dry.Plan.Digest())
				}
				fixture.applyAndRequireSuccess(t, dry.Plan.Digest())
				fixture.restart(t)
				fixture.assertOrphanCopies(t, false, false)
				return
			}
			if fresh.Plan.Digest() == dry.Plan.Digest() {
				t.Fatalf("post-mutation plan reused old digest %s", dry.Plan.Digest())
			}
			fixture.requireOldConfirmationRejected(t, dry.Plan.Digest())
			if testCase.wantSQLite {
				if fresh.CandidateCount != 1 || fresh.CandidateBytes != int64(len(fixture.orphanContent)) {
					t.Fatalf("SQLite-only recovery plan=%+v", fresh)
				}
				fixture.applyAndRequireSuccess(t, fresh.Plan.Digest())
			} else if fresh.CandidateCount != 0 {
				t.Fatalf("post-commit recovery plan=%+v", fresh)
			}
			fixture.restart(t)
			fixture.assertOrphanCopies(t, false, false)
		})
	}

	t.Run("filesystem delete durability error", func(t *testing.T) {
		fixture := newBlobGCProductionFixture(t)
		dry := fixture.dryRun(t)
		files := &productionDurabilityErrorFinalStore{FinalStore: fixture.files}
		result, err := blobgc.Execute(context.Background(), fixture.database.BlobGC(), files, blobgc.Request{
			ResidentID: fixture.residentID, Apply: true, Confirm: dry.Plan.Digest(),
		})
		if !errors.Is(err, blobgc.ErrPartial) || result.DeletedCount != 0 || result.RemainingCount != 1 ||
			!result.PhysicalMutation || files.removeCalls != 1 {
			t.Fatalf("durability result=%+v removeCalls=%d error=%v", result, files.removeCalls, err)
		}
		fixture.restart(t)
		fixture.assertOrphanCopies(t, true, false)
		fresh := fixture.dryRun(t)
		fixture.requireOldConfirmationRejected(t, dry.Plan.Digest())
		fixture.applyAndRequireSuccess(t, fresh.Plan.Digest())
		fixture.restart(t)
		fixture.assertOrphanCopies(t, false, false)
	})

	for _, mode := range []blobGCCommitOutcomeMode{blobGCCommitThenError, blobGCRollbackThenError} {
		t.Run(string(mode), func(t *testing.T) {
			fixture := newBlobGCProductionFixture(t)
			dry := fixture.dryRun(t)
			repository := &productionAmbiguousBlobGCRepository{Repository: fixture.database.BlobGC(), mode: mode}
			result, err := blobgc.Execute(context.Background(), repository, fixture.files, blobgc.Request{
				ResidentID: fixture.residentID, Apply: true, Confirm: dry.Plan.Digest(),
			})
			if !errors.Is(err, blobgc.ErrPartial) || result.DeletedCount != 0 || result.RemainingCount != 1 || !result.PhysicalMutation {
				t.Fatalf("ambiguous commit result=%+v error=%v", result, err)
			}
			fixture.restart(t)
			wantSQLite := mode == blobGCRollbackThenError
			fixture.assertOrphanCopies(t, wantSQLite, false)
			fresh := fixture.dryRun(t)
			fixture.requireOldConfirmationRejected(t, dry.Plan.Digest())
			if wantSQLite {
				if fresh.CandidateCount != 1 {
					t.Fatalf("rollback recovery plan=%+v", fresh)
				}
				fixture.applyAndRequireSuccess(t, fresh.Plan.Digest())
			} else if fresh.CandidateCount != 0 {
				t.Fatalf("committed recovery plan=%+v", fresh)
			}
			fixture.restart(t)
			fixture.assertOrphanCopies(t, false, false)
		})
	}
}

func TestM7BlobGCSQLiteSnapshotAndCandidateTransactionRecheckZeroReferences(t *testing.T) {
	ctx := context.Background()
	database, residentID, digest := seedBlobGCStore(t, []byte("orphan"))
	files := sqliteBlobGCNoFiles{}
	dry, err := blobgc.Execute(ctx, database.BlobGC(), files, blobgc.Request{ResidentID: residentID})
	if err != nil {
		t.Fatal(err)
	}
	if dry.CandidateCount != 1 || dry.CandidateBytes != int64(len("orphan")) {
		t.Fatalf("dry result = %+v", dry)
	}
	applied, err := blobgc.Execute(ctx, database.BlobGC(), files, blobgc.Request{
		ResidentID: residentID, Apply: true, Confirm: dry.Plan.Digest(),
	})
	if err != nil {
		t.Fatal(err)
	}
	if applied.DeletedCount != 1 || applied.RemainingCount != 0 {
		t.Fatalf("apply result = %+v", applied)
	}
	var count int
	if err := database.reader.QueryRowContext(ctx, `SELECT COUNT(*) FROM blobs
		WHERE dedupe_scope_id = ? AND blob_hash = ?`, residentID.String(), digest.Bytes()).Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 0 {
		t.Fatalf("SQLite blob rows = %d, want 0", count)
	}
}

func TestM7BlobGCCandidateTransactionRejectsReferenceAddedAfterDryRun(t *testing.T) {
	ctx := context.Background()
	database, residentID, digest := seedBlobGCStore(t, []byte("orphan"))
	dry, err := blobgc.Execute(ctx, database.BlobGC(), sqliteBlobGCNoFiles{}, blobgc.Request{ResidentID: residentID})
	if err != nil {
		t.Fatal(err)
	}
	contentID := projectionTestID(t, "01J00000000000000000000004")
	material := make([]byte, 32)
	if _, err := database.writer.ExecContext(ctx, `INSERT INTO content_objects(
		content_id,owner_resident_id,content_class,blob_hash,blob_hash_algorithm,commitment,commitment_salt,
		commitment_hash_algorithm,commitment_domain,canonicalization_version,erasure_state,erasure_policy,created_at,created_tz)
		VALUES (?,?,'event_payload',?,'sha256',?,?,'sha256','mahoroba:content-commitment:v1','mahoroba-jcs-v1',
		'present','independent',100,'UTC')`, contentID.String(), residentID.String(), digest.Bytes(), material, material); err != nil {
		t.Fatal(err)
	}
	if _, err := database.writer.ExecContext(ctx, `INSERT INTO content_references(
		content_id,resident_id,referrer_kind,referrer_id,referrer_field,dedupe_scope_id,blob_hash_algorithm,blob_hash)
		VALUES (?,?,'content_object',?,'blob',?,'sha256',?)`, contentID.String(), residentID.String(),
		contentID.String(), residentID.String(), digest.Bytes()); err != nil {
		t.Fatal(err)
	}

	result, err := blobgc.Execute(ctx, database.BlobGC(), sqliteBlobGCNoFiles{}, blobgc.Request{
		ResidentID: residentID, Apply: true, Confirm: dry.Plan.Digest(),
	})
	if !errors.Is(err, blobgc.ErrPlanStale) || result.DeletedCount != 0 || result.PhysicalMutation {
		t.Fatalf("apply result=%+v error=%v", result, err)
	}
	var count int
	if err := database.reader.QueryRowContext(ctx, `SELECT COUNT(*) FROM blobs
		WHERE dedupe_scope_id = ? AND blob_hash = ?`, residentID.String(), digest.Bytes()).Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 1 {
		t.Fatalf("referenced SQLite blob count = %d, want 1", count)
	}
}

func TestM7BlobGCKeepsSharedLocatorWhenOnePresentContentStillReferencesIt(t *testing.T) {
	ctx := context.Background()
	database, residentID, digest := seedBlobGCStore(t, []byte("shared"))
	contentID := projectionTestID(t, "01J00000000000000000000004")
	material := make([]byte, 32)
	if _, err := database.writer.ExecContext(ctx, `INSERT INTO content_objects(
		content_id,owner_resident_id,content_class,blob_hash,blob_hash_algorithm,commitment,commitment_salt,
		commitment_hash_algorithm,commitment_domain,canonicalization_version,erasure_state,erasure_policy,created_at,created_tz)
		VALUES (?,?,'event_payload',?,'sha256',?,?,'sha256','mahoroba:content-commitment:v1','mahoroba-jcs-v1',
		'present','independent',100,'UTC')`, contentID.String(), residentID.String(), digest.Bytes(), material, material); err != nil {
		t.Fatal(err)
	}
	if _, err := database.writer.ExecContext(ctx, `INSERT INTO content_references(
		content_id,resident_id,referrer_kind,referrer_id,referrer_field,dedupe_scope_id,blob_hash_algorithm,blob_hash)
		VALUES (?,?,'content_object',?,'blob',?,'sha256',?)`, contentID.String(), residentID.String(),
		contentID.String(), residentID.String(), digest.Bytes()); err != nil {
		t.Fatal(err)
	}

	result, err := blobgc.Execute(ctx, database.BlobGC(), sqliteBlobGCNoFiles{}, blobgc.Request{ResidentID: residentID})
	if err != nil {
		t.Fatal(err)
	}
	if result.CandidateCount != 0 || result.CandidateBytes != 0 || result.DeletedCount != 0 {
		t.Fatalf("referenced shared locator became a candidate: %+v", result)
	}
	var blobCount int
	if err := database.reader.QueryRowContext(ctx, `SELECT COUNT(*) FROM blobs
		WHERE dedupe_scope_id = ? AND blob_hash = ?`, residentID.String(), digest.Bytes()).Scan(&blobCount); err != nil {
		t.Fatal(err)
	}
	if blobCount != 1 {
		t.Fatalf("referenced shared locator count = %d, want 1", blobCount)
	}
}

func TestM7BlobGCSQLiteRejectsMissingStaleVersionAndDependencyWatermark(t *testing.T) {
	ctx := context.Background()
	for _, testCase := range []struct {
		name   string
		mutate string
	}{
		{name: "missing", mutate: `DELETE FROM projection_watermarks`},
		{name: "stale", mutate: `UPDATE projection_watermarks SET source_commit_seq = source_commit_seq + 1`},
		{name: "wrong version", mutate: `UPDATE projection_watermarks SET projection_version = 'content-references-v2'`},
		{name: "dependency", mutate: `INSERT INTO projection_watermark_dependencies(
			projection_name,resident_id,dependency_kind,dependency_version_id)
			SELECT projection_name,resident_id,'memory_policy','01J00000000000000000000009' FROM projection_watermarks`},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			database, residentID, _ := seedBlobGCStore(t, []byte("orphan"))
			if _, err := database.writer.ExecContext(ctx, testCase.mutate); err != nil {
				t.Fatal(err)
			}
			result, err := blobgc.Execute(ctx, database.BlobGC(), sqliteBlobGCNoFiles{}, blobgc.Request{ResidentID: residentID})
			if !errors.Is(err, blobgc.ErrProjectionNotCurrent) {
				t.Fatalf("Execute error = %v", err)
			}
			if result.DeletedCount != 0 {
				t.Fatalf("gate failure deleted %d candidates", result.DeletedCount)
			}
		})
	}
}

func TestM7BlobGCSQLiteContentMismatchBlocksEntireApply(t *testing.T) {
	database, residentID, _ := seedBlobGCStoreWithLocator(t, []byte("corrupt"), canonical.HashBlob([]byte("expected")))
	result, err := blobgc.Execute(context.Background(), database.BlobGC(), sqliteBlobGCNoFiles{}, blobgc.Request{ResidentID: residentID})
	if !errors.Is(err, blobgc.ErrContentIntegrity) || result.DeletedCount != 0 {
		t.Fatalf("corrupt blob result=%+v error=%v", result, err)
	}
}

func TestM7BlobGCSQLiteRejectsCurrentWatermarkWithMissingSelfReference(t *testing.T) {
	ctx := context.Background()
	database, residentID, digest := seedBlobGCStore(t, []byte("live"))
	contentID := projectionTestID(t, "01J00000000000000000000004")
	material := make([]byte, 32)
	if _, err := database.writer.ExecContext(ctx, `INSERT INTO content_objects(
		content_id,owner_resident_id,content_class,blob_hash,blob_hash_algorithm,commitment,commitment_salt,
		commitment_hash_algorithm,commitment_domain,canonicalization_version,erasure_state,erasure_policy,created_at,created_tz)
		VALUES (?,?,'event_payload',?,'sha256',?,?,'sha256','mahoroba:content-commitment:v1','mahoroba-jcs-v1',
		'present','independent',100,'UTC')`, contentID.String(), residentID.String(), digest.Bytes(), material, material); err != nil {
		t.Fatal(err)
	}
	result, err := blobgc.Execute(ctx, database.BlobGC(), sqliteBlobGCNoFiles{}, blobgc.Request{ResidentID: residentID})
	if !errors.Is(err, blobgc.ErrProjectionNotCurrent) || result.DeletedCount != 0 || result.PhysicalMutation {
		t.Fatalf("missing self-row result=%+v error=%v", result, err)
	}
}

func TestM7ContentReferencesPublicationGateRequiresEveryResidentAtHead(t *testing.T) {
	ctx := context.Background()
	database, _, _ := seedBlobGCStore(t, []byte("orphan"))
	if err := database.RequireCurrentContentReferences(ctx); err != nil {
		t.Fatalf("current content references rejected: %v", err)
	}

	principalID := projectionTestID(t, "01J00000000000000000000005")
	residentID := projectionTestID(t, "01J00000000000000000000006")
	commitID := projectionTestID(t, "01J00000000000000000000001")
	if _, err := database.writer.ExecContext(ctx, `INSERT INTO principals(
		principal_id,canonical_commit_id,kind,display_name,created_at,created_tz)
		VALUES (?,?, 'resident','second',100,'UTC')`, principalID.String(), commitID.String()); err != nil {
		t.Fatal(err)
	}
	if _, err := database.writer.ExecContext(ctx, `INSERT INTO residents(
		resident_id,canonical_commit_id,principal_id,name,description_content_id,seed_key,
		parent_resident_id,branched_from_seq,branched_at,branched_tz,created_at,created_tz)
		VALUES (?,?,?,'second',NULL,'second',NULL,NULL,NULL,NULL,100,'UTC')`,
		residentID.String(), commitID.String(), principalID.String()); err != nil {
		t.Fatal(err)
	}
	if err := database.RequireCurrentContentReferences(ctx); !errors.Is(err, projection.ErrProjectionNotCurrent) {
		t.Fatalf("missing second-resident watermark error = %v", err)
	}
}

func TestM7ContentReferencesPublicationGateRejectsOrphanWatermarkOnEmptyStore(t *testing.T) {
	ctx := context.Background()
	database := openBlobGCTestStore(t)
	residentID := projectionTestID(t, "01J00000000000000000000006")
	if _, err := database.writer.ExecContext(ctx, `INSERT INTO projection_watermarks(
		projection_name,resident_id,projection_version,source_commit_seq,as_of,as_of_tz)
		VALUES ('content_references',?,'content-references-v1',1,100,'UTC')`, residentID.String()); err != nil {
		t.Fatal(err)
	}
	if err := database.RequireCurrentContentReferences(ctx); !errors.Is(err, projection.ErrProjectionNotCurrent) {
		t.Fatalf("orphan watermark error = %v", err)
	}
}

func TestM7ContentReferencesPublicationGateAcceptsGlobalHeadWithoutResidents(t *testing.T) {
	ctx := context.Background()
	database := openBlobGCTestStore(t)
	commitID := projectionTestID(t, "01J00000000000000000000001")
	if _, err := database.writer.ExecContext(ctx, `INSERT INTO canonical_commits(
		canonical_commit_id,commit_seq,resident_id,committed_at,committed_tz)
		VALUES (?,1,NULL,100,'UTC')`, commitID.String()); err != nil {
		t.Fatal(err)
	}
	if err := database.RequireCurrentContentReferences(ctx); err != nil {
		t.Fatalf("global-only Canonical head rejected: %v", err)
	}
}

func TestM7BlobGCZeroReferenceLookupUsesContentReferencesBlobIndex(t *testing.T) {
	database := openBlobGCTestStore(t)
	rows, err := database.reader.QueryContext(context.Background(), `EXPLAIN QUERY PLAN
		SELECT COUNT(*) FROM content_references
		WHERE resident_id = ? AND dedupe_scope_id = ? AND blob_hash_algorithm = ? AND blob_hash = ?`,
		"01J00000000000000000000001", "01J00000000000000000000001", canonical.HashAlgorithm, make([]byte, 32))
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	used := false
	for rows.Next() {
		var id, parent, unused int
		var detail string
		if err := rows.Scan(&id, &parent, &unused, &detail); err != nil {
			t.Fatal(err)
		}
		if strings.Contains(detail, "idx_content_references_blob") {
			used = true
		}
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	if !used {
		t.Fatal("blob GC zero-reference lookup did not use idx_content_references_blob")
	}
}

type blobGCProductionFixture struct {
	databasePath  string
	blobRoot      string
	database      *Store
	files         *blob.FileStore
	residentID    canonical.ID
	liveContentID canonical.ID
	orphanDigest  canonical.Digest
	liveDigest    canonical.Digest
	orphanContent []byte
}

func newBlobGCProductionFixture(t *testing.T) *blobGCProductionFixture {
	t.Helper()
	ctx := context.Background()
	root := t.TempDir()
	fixture := &blobGCProductionFixture{
		databasePath:  filepath.Join(root, "mahoroba.db"),
		blobRoot:      filepath.Join(root, "blobs"),
		residentID:    blobGCTestID(t, "01J00000000000000000000003"),
		liveContentID: blobGCTestID(t, "01J00000000000000000000004"),
		orphanContent: []byte("production orphan blob"),
	}
	fixture.orphanDigest = canonical.HashBlob(fixture.orphanContent)
	liveContent := []byte("production referenced blob")
	fixture.liveDigest = canonical.HashBlob(liveContent)
	fixture.database = openBlobGCProductionDatabase(t, fixture.databasePath)

	files, err := blob.NewFileStore(fixture.blobRoot)
	if runtime.GOOS == "windows" && os.Getenv("CI") == "" && errors.Is(err, os.ErrPermission) {
		t.Skipf("desktop sandbox cannot grant exact protected ACL for production GC evidence: %v", err)
	}
	if err != nil {
		t.Fatal(err)
	}
	fixture.files = files

	commitID := blobGCTestID(t, "01J00000000000000000000001")
	principalID := blobGCTestID(t, "01J00000000000000000000002")
	revisionID := blobGCTestID(t, "01J00000000000000000000005")
	commitment := canonical.HashBlob([]byte("production content commitment"))
	salt := canonical.HashBlob([]byte("production content commitment salt"))
	tx, err := fixture.database.writer.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	statements := []struct {
		query string
		args  []any
	}{
		{`INSERT INTO canonical_commits(canonical_commit_id,commit_seq,resident_id,committed_at,committed_tz)
			VALUES (?,1,NULL,100,'UTC')`, []any{commitID.String()}},
		{`INSERT INTO principals(principal_id,canonical_commit_id,kind,display_name,created_at,created_tz)
			VALUES (?,?,'resident','resident',100,'UTC')`, []any{principalID.String(), commitID.String()}},
		{`INSERT INTO residents(resident_id,canonical_commit_id,principal_id,name,description_content_id,seed_key,
			parent_resident_id,branched_from_seq,branched_at,branched_tz,created_at,created_tz)
			VALUES (?,?,?,'resident',NULL,'seed',NULL,NULL,NULL,NULL,100,'UTC')`,
			[]any{fixture.residentID.String(), commitID.String(), principalID.String()}},
		{`INSERT INTO blobs(dedupe_scope_id,hash_algorithm,blob_hash,content,byte_size,encoding,compression,created_at,created_tz)
			VALUES (?,'sha256',?,?,?,'binary','none',100,'UTC')`,
			[]any{fixture.residentID.String(), fixture.orphanDigest.Bytes(), fixture.orphanContent, len(fixture.orphanContent)}},
		{`INSERT INTO blobs(dedupe_scope_id,hash_algorithm,blob_hash,content,byte_size,encoding,compression,created_at,created_tz)
			VALUES (?,'sha256',?,?,?,'binary','none',100,'UTC')`,
			[]any{fixture.residentID.String(), fixture.liveDigest.Bytes(), liveContent, len(liveContent)}},
		{`INSERT INTO content_objects(content_id,owner_resident_id,content_class,blob_hash,blob_hash_algorithm,
			commitment,commitment_salt,commitment_hash_algorithm,commitment_domain,canonicalization_version,
			erasure_state,erasure_policy,created_at,created_tz)
			VALUES (?,?,'event_payload',?,'sha256',?,?,'sha256',?,?,'present','independent',100,'UTC')`,
			[]any{fixture.liveContentID.String(), fixture.residentID.String(), fixture.liveDigest.Bytes(),
				commitment.Bytes(), salt.Bytes(), canonical.ContentCommitmentDomain, canonical.CanonicalizationVersion}},
		{`INSERT INTO resident_revisions(
			revision_id,canonical_commit_id,resident_id,revision_class,content_id,parent_revision_id,
			created_by_run_id,reason_content_id,recorded_at,recorded_tz)
			VALUES (?,?,?,'persona',?,NULL,NULL,NULL,100,'UTC')`,
			[]any{revisionID.String(), commitID.String(), fixture.residentID.String(), fixture.liveContentID.String()}},
	}
	for _, statement := range statements {
		if _, err := tx.ExecContext(ctx, statement.query, statement.args...); err != nil {
			_ = tx.Rollback()
			t.Fatal(err)
		}
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}

	staged, err := fixture.files.Stage(ctx, fixture.residentID, bytes.NewReader(fixture.orphanContent))
	if err != nil {
		t.Fatal(err)
	}
	if staged.Digest() != fixture.orphanDigest {
		t.Fatalf("staged digest=%s want=%s", staged.Digest(), fixture.orphanDigest)
	}
	if _, err := fixture.files.Finalize(ctx, fixture.residentID, staged); err != nil {
		t.Fatal(err)
	}
	if err := fixture.files.Acknowledge(ctx, fixture.residentID, staged); err != nil {
		t.Fatal(err)
	}

	commitSeq, err := canonical.NewCommitSeq(1)
	if err != nil {
		t.Fatal(err)
	}
	target := canonical.Head{Exists: true, CommitSeq: commitSeq, CommittedAt: 100}
	references, err := fixture.database.Projection().evaluateContentReferences(ctx, fixture.residentID, target)
	if err != nil {
		t.Fatal(err)
	}
	if len(references) != 2 {
		t.Fatalf("production content references=%d want=2", len(references))
	}
	definition := projection.ContentReferencesDefinition()
	watermark := projection.Watermark{
		ProjectionName: definition.Name, ResidentID: fixture.residentID, ProjectionVersion: definition.Version,
		SourceCommitSeq: commitSeq, AsOf: 100, AsOfTZ: canonical.MustTimezone("UTC"),
		Dependencies: []projection.Dependency{},
	}
	if err := fixture.database.Projection().Apply(ctx, projection.ApplyRequest{
		Definition: definition, ResidentID: fixture.residentID,
		Plan: projection.UpdatePlan{
			Kind: projection.FullBuild, Reason: projection.ReasonUnbuilt, NeedCommitCatchUp: true,
		},
		Evaluation: projection.Evaluation{Value: references}, Watermark: watermark,
	}); err != nil {
		t.Fatal(err)
	}
	if err := fixture.database.RequireCurrentContentReferences(ctx); err != nil {
		t.Fatalf("production content references are not current: %v", err)
	}
	fixture.assertOrphanCopies(t, true, true)
	return fixture
}

func openBlobGCProductionDatabase(t *testing.T, path string) *Store {
	t.Helper()
	database, err := Open(context.Background(), path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = database.Close() })
	return database
}

func (fixture *blobGCProductionFixture) restart(t *testing.T) {
	t.Helper()
	if err := fixture.database.Close(); err != nil {
		t.Fatal(err)
	}
	fixture.database = openBlobGCProductionDatabase(t, fixture.databasePath)
	files, err := blob.OpenFileStoreExisting(fixture.blobRoot)
	if err != nil {
		t.Fatal(err)
	}
	fixture.files = files
}

func (fixture *blobGCProductionFixture) exec(t *testing.T, query string, args ...any) {
	t.Helper()
	if _, err := fixture.database.writer.ExecContext(context.Background(), query, args...); err != nil {
		t.Fatal(err)
	}
}

func (fixture *blobGCProductionFixture) addDependency(t *testing.T) {
	t.Helper()
	fixture.exec(t, `INSERT INTO projection_watermark_dependencies(
		projection_name,resident_id,dependency_kind,dependency_version_id)
		VALUES ('content_references',?,'memory_policy',?)`, fixture.residentID.String(),
		blobGCTestID(t, "01J00000000000000000000007").String())
}

func (fixture *blobGCProductionFixture) dryRun(t *testing.T) blobgc.Result {
	t.Helper()
	result, err := blobgc.Execute(context.Background(), fixture.database.BlobGC(), fixture.files, blobgc.Request{
		ResidentID: fixture.residentID,
	})
	if err != nil {
		t.Fatal(err)
	}
	return result
}

func (fixture *blobGCProductionFixture) applyAndRequireSuccess(t *testing.T, confirm string) {
	t.Helper()
	result, err := blobgc.Execute(context.Background(), fixture.database.BlobGC(), fixture.files, blobgc.Request{
		ResidentID: fixture.residentID, Apply: true, Confirm: confirm,
	})
	if err != nil || result.DeletedCount != 1 || result.RemainingCount != 0 || !result.PhysicalMutation {
		t.Fatalf("recovery apply result=%+v error=%v", result, err)
	}
}

func (fixture *blobGCProductionFixture) requireOldConfirmationRejected(t *testing.T, confirm string) {
	t.Helper()
	result, err := blobgc.Execute(context.Background(), fixture.database.BlobGC(), fixture.files, blobgc.Request{
		ResidentID: fixture.residentID, Apply: true, Confirm: confirm,
	})
	if !errors.Is(err, blobgc.ErrPlanStale) || errors.Is(err, blobgc.ErrPartial) ||
		result.DeletedCount != 0 || result.PhysicalMutation {
		t.Fatalf("old confirmation result=%+v error=%v", result, err)
	}
}

func (fixture *blobGCProductionFixture) assertOrphanCopies(t *testing.T, wantSQLite, wantFilesystem bool) {
	t.Helper()
	ctx := context.Background()
	var sqliteCount int
	if err := fixture.database.reader.QueryRowContext(ctx, `SELECT COUNT(*) FROM blobs
		WHERE dedupe_scope_id = ? AND hash_algorithm = 'sha256' AND blob_hash = ?`,
		fixture.residentID.String(), fixture.orphanDigest.Bytes()).Scan(&sqliteCount); err != nil {
		t.Fatal(err)
	}
	if (sqliteCount == 1) != wantSQLite || sqliteCount < 0 || sqliteCount > 1 {
		t.Fatalf("orphan SQLite count=%d wantPresent=%t", sqliteCount, wantSQLite)
	}
	var liveCount int
	if err := fixture.database.reader.QueryRowContext(ctx, `SELECT COUNT(*) FROM blobs
		WHERE dedupe_scope_id = ? AND hash_algorithm = 'sha256' AND blob_hash = ?`,
		fixture.residentID.String(), fixture.liveDigest.Bytes()).Scan(&liveCount); err != nil {
		t.Fatal(err)
	}
	if liveCount != 1 {
		t.Fatalf("referenced live SQLite count=%d want=1", liveCount)
	}
	objects, err := fixture.files.WalkFinal(ctx, fixture.residentID)
	if err != nil {
		t.Fatal(err)
	}
	found := false
	for _, object := range objects {
		if object.Digest() == fixture.orphanDigest {
			found = true
			if object.ResidentID() != fixture.residentID || object.Size().Int64() != int64(len(fixture.orphanContent)) {
				t.Fatalf("orphan filesystem token has wrong metadata")
			}
		}
	}
	if found != wantFilesystem {
		t.Fatalf("orphan filesystem present=%t want=%t", found, wantFilesystem)
	}
	if wantFilesystem {
		content, err := fixture.files.Read(ctx, fixture.residentID, fixture.orphanDigest)
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(content, fixture.orphanContent) || canonical.HashBlob(content) != fixture.orphanDigest {
			t.Fatal("orphan filesystem content changed")
		}
	}
}

func blobGCTestID(t *testing.T, raw string) canonical.ID {
	t.Helper()
	value, err := canonical.ParseID(raw)
	if err != nil {
		t.Fatal(err)
	}
	return value
}

type productionVanishingFinalStore struct {
	blobgc.FinalStore
	removeCalls int
}

func (store *productionVanishingFinalStore) RemoveFinal(ctx context.Context, object blob.FinalObject) (bool, error) {
	store.removeCalls++
	removed, err := store.FinalStore.RemoveFinal(ctx, object)
	if err != nil || !removed {
		return removed, errors.Join(errors.New("production disappearance delegate did not remove object"), err)
	}
	return false, nil
}

type productionDurabilityErrorFinalStore struct {
	blobgc.FinalStore
	removeCalls int
}

func (store *productionDurabilityErrorFinalStore) RemoveFinal(ctx context.Context, object blob.FinalObject) (bool, error) {
	store.removeCalls++
	removed, err := store.FinalStore.RemoveFinal(ctx, object)
	if err != nil || !removed {
		return removed, errors.Join(errors.New("production durability delegate did not remove object"), err)
	}
	return true, errors.New("injected final-object durability barrier failure")
}

type blobGCCommitOutcomeMode string

const (
	blobGCCommitThenError   blobGCCommitOutcomeMode = "commit then error"
	blobGCRollbackThenError blobGCCommitOutcomeMode = "rollback then error"
)

type productionAmbiguousBlobGCRepository struct {
	blobgc.Repository
	mode blobGCCommitOutcomeMode
}

func (repository *productionAmbiguousBlobGCRepository) BeginCandidate(
	ctx context.Context,
	candidate blobgc.Candidate,
	head blobgc.CapturedHead,
) (blobgc.CandidateTransaction, error) {
	transaction, err := repository.Repository.BeginCandidate(ctx, candidate, head)
	if err != nil {
		return nil, err
	}
	return &productionAmbiguousBlobGCTx{CandidateTransaction: transaction, mode: repository.mode}, nil
}

type productionAmbiguousBlobGCTx struct {
	blobgc.CandidateTransaction
	mode blobGCCommitOutcomeMode
}

func (transaction *productionAmbiguousBlobGCTx) Commit(ctx context.Context) error {
	injected := errors.New("injected ambiguous candidate commit result")
	switch transaction.mode {
	case blobGCCommitThenError:
		return errors.Join(transaction.CandidateTransaction.Commit(ctx), injected)
	case blobGCRollbackThenError:
		return errors.Join(transaction.CandidateTransaction.Rollback(context.WithoutCancel(ctx)), injected)
	default:
		return errors.New("unknown ambiguous commit mode")
	}
}

type sqliteBlobGCNoFiles struct{}

func (sqliteBlobGCNoFiles) WalkFinal(context.Context, canonical.ID) ([]blob.FinalObject, error) {
	return []blob.FinalObject{}, nil
}
func (sqliteBlobGCNoFiles) RemoveFinal(context.Context, blob.FinalObject) (bool, error) {
	return false, errors.New("unexpected filesystem delete")
}

func seedBlobGCStore(t *testing.T, content []byte) (*Store, canonical.ID, canonical.Digest) {
	return seedBlobGCStoreWithLocator(t, content, canonical.HashBlob(content))
}

func seedBlobGCStoreWithLocator(t *testing.T, content []byte, digest canonical.Digest) (*Store, canonical.ID, canonical.Digest) {
	t.Helper()
	ctx := context.Background()
	database := openBlobGCTestStore(t)
	commitID := projectionTestID(t, "01J00000000000000000000001")
	principalID := projectionTestID(t, "01J00000000000000000000002")
	residentID := projectionTestID(t, "01J00000000000000000000003")
	statements := []struct {
		query string
		args  []any
	}{
		{`INSERT INTO canonical_commits(canonical_commit_id,commit_seq,resident_id,committed_at,committed_tz)
			VALUES (?,1,NULL,100,'UTC')`, []any{commitID.String()}},
		{`INSERT INTO principals(principal_id,canonical_commit_id,kind,display_name,created_at,created_tz)
			VALUES (?,?,'resident','resident',100,'UTC')`, []any{principalID.String(), commitID.String()}},
		{`INSERT INTO residents(resident_id,canonical_commit_id,principal_id,name,description_content_id,seed_key,
			parent_resident_id,branched_from_seq,branched_at,branched_tz,created_at,created_tz)
			VALUES (?,?,?,'resident',NULL,'seed',NULL,NULL,NULL,NULL,100,'UTC')`,
			[]any{residentID.String(), commitID.String(), principalID.String()}},
		{`INSERT INTO blobs(dedupe_scope_id,hash_algorithm,blob_hash,content,byte_size,encoding,compression,created_at,created_tz)
			VALUES (?,'sha256',?,?,?,'binary','none',100,'UTC')`,
			[]any{residentID.String(), digest.Bytes(), content, len(content)}},
		{`INSERT INTO projection_watermarks(projection_name,resident_id,projection_version,source_commit_seq,as_of,as_of_tz)
			VALUES ('content_references',?,'content-references-v1',1,100,'UTC')`, []any{residentID.String()}},
	}
	for _, statement := range statements {
		if _, err := database.writer.ExecContext(ctx, statement.query, statement.args...); err != nil {
			t.Fatal(err)
		}
	}
	return database, residentID, digest
}

func openBlobGCTestStore(t *testing.T) *Store {
	t.Helper()
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "blob-gc.db")
	writer, err := openWriter(ctx, path, DefaultOptions())
	if err != nil {
		t.Fatal(err)
	}
	if err := initializeEmptyDatabase(ctx, writer); err != nil {
		_ = writer.Close()
		t.Fatal(err)
	}
	if err := migrateUp(ctx, writer); err != nil {
		_ = writer.Close()
		t.Fatal(err)
	}
	reader, err := openReader(ctx, path, DefaultOptions())
	if err != nil {
		_ = writer.Close()
		t.Fatal(err)
	}
	database := &Store{path: path, writer: writer, reader: reader, writes: newWritePriorityGate()}
	t.Cleanup(func() { _ = database.Close() })
	return database
}
