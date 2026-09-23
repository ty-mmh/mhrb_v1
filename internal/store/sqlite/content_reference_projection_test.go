package sqlite

import (
	"context"
	"errors"
	"testing"

	"mahoroba.local/mahoroba/internal/canonical"
	"mahoroba.local/mahoroba/internal/projection"
)

func TestM7ContentReferencesUseExactHeadSnapshotAndEraseLocatorProjection(t *testing.T) {
	ctx := context.Background()
	database := openProjectionTestStore(t)
	commitID := projectionTestID(t, "01J00000000000000000000010")
	residentID := projectionTestID(t, "01J00000000000000000000011")
	principalID := projectionTestID(t, "01J00000000000000000000012")
	presentID := projectionTestID(t, "01J00000000000000000000013")
	erasedID := projectionTestID(t, "01J00000000000000000000014")
	presentDigest := canonical.HashBlob([]byte("present content"))
	commitment := canonical.HashBlob([]byte("commitment"))
	salt := canonical.HashBlob([]byte("salt"))

	tx, err := database.writer.BeginTx(ctx, nil)
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
			VALUES (?,?,'resident','r',100,'UTC')`, []any{principalID.String(), commitID.String()}},
		{`INSERT INTO residents(resident_id,canonical_commit_id,principal_id,name,description_content_id,seed_key,
			parent_resident_id,branched_from_seq,branched_at,branched_tz,created_at,created_tz)
			VALUES (?,?,?,'r',NULL,'seed',NULL,NULL,NULL,NULL,100,'UTC')`, []any{residentID.String(), commitID.String(), principalID.String()}},
		{`INSERT INTO blobs(dedupe_scope_id,hash_algorithm,blob_hash,content,byte_size,encoding,compression,created_at,created_tz)
			VALUES (?,'sha256',?,?,15,'utf-8','none',100,'UTC')`, []any{residentID.String(), presentDigest.Bytes(), []byte("present content")}},
		{`INSERT INTO content_objects(content_id,owner_resident_id,content_class,blob_hash,blob_hash_algorithm,
			commitment,commitment_salt,commitment_hash_algorithm,commitment_domain,canonicalization_version,
			erasure_state,erasure_policy,created_at,created_tz)
			VALUES (?,?,'event_payload',?,'sha256',?,?,'sha256',? ,?,'present','independent',100,'UTC')`,
			[]any{presentID.String(), residentID.String(), presentDigest.Bytes(), commitment.Bytes(), salt.Bytes(), canonical.ContentCommitmentDomain, canonical.CanonicalizationVersion}},
		{`INSERT INTO content_objects(content_id,owner_resident_id,content_class,blob_hash,blob_hash_algorithm,
			commitment,commitment_salt,commitment_hash_algorithm,commitment_domain,canonicalization_version,
			erasure_state,erasure_policy,created_at,created_tz)
			VALUES (?,?,'event_payload',NULL,'sha256',?,NULL,'sha256',? ,?,'erased','independent',100,'UTC')`,
			[]any{erasedID.String(), residentID.String(), commitment.Bytes(), canonical.ContentCommitmentDomain, canonical.CanonicalizationVersion}},
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

	target := canonical.Head{Exists: true, CommitSeq: mustProjectionCommitSeq(t, 1), CommittedAt: 100}
	references, err := database.Projection().evaluateContentReferences(ctx, residentID, target)
	if err != nil {
		t.Fatal(err)
	}
	if len(references) != 2 {
		t.Fatalf("self references = %d, want 2: %#v", len(references), references)
	}
	var present, erased *projection.ContentReference
	for index := range references {
		switch references[index].ContentID {
		case presentID:
			present = &references[index]
		case erasedID:
			erased = &references[index]
		}
	}
	if present == nil || present.DedupeScopeID == nil || *present.DedupeScopeID != residentID ||
		present.BlobHashAlgorithm == nil || *present.BlobHashAlgorithm != "sha256" ||
		present.BlobHash == nil || *present.BlobHash != presentDigest {
		t.Fatalf("present self locator = %#v", present)
	}
	if erased == nil || erased.DedupeScopeID != nil || erased.BlobHashAlgorithm != nil || erased.BlobHash != nil {
		t.Fatalf("erased self locator was retained: %#v", erased)
	}
	definition := projection.ContentReferencesDefinition()
	watermark := projection.Watermark{
		ProjectionName: definition.Name, ResidentID: residentID, ProjectionVersion: definition.Version,
		SourceCommitSeq: target.CommitSeq, AsOf: 100, AsOfTZ: canonical.MustTimezone("UTC"),
		Dependencies: []projection.Dependency{},
	}
	repository := database.Projection()
	if err := repository.Apply(ctx, projection.ApplyRequest{
		Definition: definition, ResidentID: residentID,
		Plan:       projection.UpdatePlan{Kind: projection.FullBuild, Reason: projection.ReasonUnbuilt, NeedCommitCatchUp: true},
		Evaluation: projection.Evaluation{Value: references}, Watermark: watermark,
	}); err != nil {
		t.Fatal(err)
	}
	var bodyCount int
	if err := database.reader.QueryRowContext(ctx, `SELECT COUNT(*) FROM content_references WHERE resident_id = ?`, residentID.String()).Scan(&bodyCount); err != nil {
		t.Fatal(err)
	}
	if bodyCount != 2 {
		t.Fatalf("persisted content references = %d, want 2", bodyCount)
	}
	if err := database.RequireCurrentContentReferences(ctx); err != nil {
		t.Fatalf("complete authoritative body rejected: %v", err)
	}

	// A matching watermark and complete content_object self rows must not hide
	// a missing non-self row from the code-owned direct-reference catalog.
	revisionID := projectionTestID(t, "01J00000000000000000000016")
	if _, err := database.writer.ExecContext(ctx, `INSERT INTO resident_revisions(
		revision_id,canonical_commit_id,resident_id,revision_class,content_id,parent_revision_id,
		created_by_run_id,reason_content_id,recorded_at,recorded_tz)
		VALUES (?,?,?,'persona',?,NULL,NULL,NULL,100,'UTC')`, revisionID.String(), commitID.String(),
		residentID.String(), presentID.String()); err != nil {
		t.Fatal(err)
	}
	if err := database.RequireCurrentContentReferences(ctx); !errors.Is(err, projection.ErrProjectionNotCurrent) {
		t.Fatalf("missing authoritative non-self row accepted: %v", err)
	}
	if _, err := database.writer.ExecContext(ctx, `INSERT INTO content_references(
		content_id,resident_id,referrer_kind,referrer_id,referrer_field,
		dedupe_scope_id,blob_hash_algorithm,blob_hash)
		VALUES (?,?,'resident_revision',?,'content_id',?,'sha256',?)`, presentID.String(),
		residentID.String(), revisionID.String(), residentID.String(), presentDigest.Bytes()); err != nil {
		t.Fatal(err)
	}
	if err := database.RequireCurrentContentReferences(ctx); err != nil {
		t.Fatalf("repaired authoritative body rejected: %v", err)
	}
	forgedID := projectionTestID(t, "01J00000000000000000000017")
	if _, err := database.writer.ExecContext(ctx, `INSERT INTO content_references(
		content_id,resident_id,referrer_kind,referrer_id,referrer_field,
		dedupe_scope_id,blob_hash_algorithm,blob_hash)
		VALUES (?,?,'forged',?,'invented',?,'sha256',?)`, presentID.String(), residentID.String(),
		forgedID.String(), residentID.String(), presentDigest.Bytes()); err != nil {
		t.Fatal(err)
	}
	if err := database.RequireCurrentContentReferences(ctx); !errors.Is(err, projection.ErrProjectionNotCurrent) {
		t.Fatalf("non-authoritative extra row accepted: %v", err)
	}
	if _, err := database.writer.ExecContext(ctx, `DELETE FROM content_references
		WHERE resident_id=? AND referrer_kind='forged'`, residentID.String()); err != nil {
		t.Fatal(err)
	}
	if err := database.RequireCurrentContentReferences(ctx); err != nil {
		t.Fatalf("authoritative body after extra-row removal rejected: %v", err)
	}
	observed, exists, err := repository.Watermark(ctx, definition.Name, residentID)
	if err != nil || !exists {
		t.Fatalf("content references watermark exists=%v err=%v", exists, err)
	}
	if err := repository.Drop(ctx, projection.DropRequest{
		Definition: definition, ResidentID: residentID, Observed: &observed,
	}); err != nil {
		t.Fatal(err)
	}
	if err := database.reader.QueryRowContext(ctx, `SELECT COUNT(*) FROM content_references WHERE resident_id = ?`, residentID.String()).Scan(&bodyCount); err != nil {
		t.Fatal(err)
	}
	if bodyCount != 0 {
		t.Fatalf("dropped content references = %d, want 0", bodyCount)
	}
	if _, exists, err := repository.Watermark(ctx, definition.Name, residentID); err != nil || exists {
		t.Fatalf("dropped watermark exists=%v err=%v", exists, err)
	}

	commitTwo := projectionTestID(t, "01J00000000000000000000015")
	if _, err := database.writer.ExecContext(ctx, `INSERT INTO canonical_commits(
		canonical_commit_id,commit_seq,resident_id,committed_at,committed_tz) VALUES (?,2,NULL,200,'UTC')`, commitTwo.String()); err != nil {
		t.Fatal(err)
	}
	if _, err := database.Projection().evaluateContentReferences(ctx, residentID, target); !errors.Is(err, projection.ErrPreflightHeadChanged) {
		t.Fatalf("old requested head accepted after Canonical advance: %v", err)
	}
}
