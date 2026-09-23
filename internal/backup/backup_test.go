package backup

import (
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"errors"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"slices"
	"strings"
	"testing"
	"time"

	"mahoroba.local/mahoroba/internal/app"
	"mahoroba.local/mahoroba/internal/blob"
	"mahoroba.local/mahoroba/internal/canonical"
	"mahoroba.local/mahoroba/internal/descriptorpath"
	"mahoroba.local/mahoroba/internal/domain"
	"mahoroba.local/mahoroba/internal/durablepublish"
	"mahoroba.local/mahoroba/internal/fssecure"
	"mahoroba.local/mahoroba/internal/generation"
	"mahoroba.local/mahoroba/internal/hostlock"
	"mahoroba.local/mahoroba/internal/memory"
	"mahoroba.local/mahoroba/internal/projection"
	storesqlite "mahoroba.local/mahoroba/internal/store/sqlite"
)

func TestM7BackupCreateVerifyCommitZeroAndDeterministicManifest(t *testing.T) {
	fixture := newBackupFixture(t)
	first := filepath.Join(fixture.parent, "first")
	second := filepath.Join(fixture.parent, "second")

	firstResult, err := Create(context.Background(), fixture.request(first, false))
	if err != nil {
		t.Fatalf("create first backup: %v", err)
	}
	secondResult, err := Create(context.Background(), fixture.request(second, false))
	if err != nil {
		t.Fatalf("create second backup: %v", err)
	}
	if firstResult.FormatVersion != FormatVersion || firstResult.AuthenticityGuaranteed || firstResult.ProjectionsIncluded {
		t.Fatalf("unexpected result: %+v", firstResult)
	}
	firstVerified, err := Verify(context.Background(), first)
	if err != nil {
		t.Fatalf("verify first backup: %v", err)
	}
	if firstVerified.Manifest.CapturedHead.Exists || firstVerified.Manifest.CapturedHead.CommitID != nil ||
		firstVerified.Manifest.CapturedHead.CommitSeq != nil || firstVerified.Manifest.CapturedHead.CommittedAtUnixMicros != nil ||
		firstVerified.Manifest.CapturedHead.CommittedTZ != nil {
		t.Fatalf("commit-zero head is not exact: %+v", firstVerified.Manifest.CapturedHead)
	}
	if firstVerified.Manifest.Projections.Policy != "excluded_by_default" || len(firstVerified.Manifest.Projections.Entries) != 0 {
		t.Fatalf("default projection contract differs: %+v", firstVerified.Manifest.Projections)
	}
	firstManifest := mustReadFile(t, filepath.Join(first, "manifest.json"))
	secondManifest := mustReadFile(t, filepath.Join(second, "manifest.json"))
	if !bytes.Equal(firstManifest, secondManifest) {
		t.Fatalf("same source snapshot produced different manifests\nfirst=%s\nsecond=%s", firstManifest, secondManifest)
	}
	if secondResult.ByteCount != firstResult.ByteCount || secondResult.FileCount != firstResult.FileCount {
		t.Fatalf("deterministic result counters differ: first=%+v second=%+v", firstResult, secondResult)
	}
}

func TestM7BackupCopyVerifiedStreamsBoundDatabaseThenManifestOrderedBlobs(t *testing.T) {
	fixture := newBackupFixture(t)
	populateBackupResident(t, fixture)
	output := filepath.Join(fixture.parent, "copy-verified")
	if _, err := Create(context.Background(), fixture.request(output, false)); err != nil {
		t.Fatalf("create backup: %v", err)
	}

	var paths []string
	verified, err := CopyVerified(context.Background(), output, func(_ context.Context, entry BundleEntry, reader io.Reader) error {
		body, err := io.ReadAll(reader)
		if err != nil {
			return err
		}
		digest := sha256.Sum256(body)
		if int64(len(body)) != entry.ByteSize || "sha256:"+hex.EncodeToString(digest[:]) != entry.SHA256 {
			return errors.New("sink received bytes outside the bound descriptor")
		}
		paths = append(paths, entry.Path)
		return nil
	})
	if err != nil {
		t.Fatalf("CopyVerified: %v", err)
	}
	if !validDigest(verified.RootIdentitySHA256) {
		t.Fatalf("bundle root identity digest = %q", verified.RootIdentitySHA256)
	}
	wantPaths := []string{DatabaseBundlePath}
	for _, descriptor := range verified.Manifest.BlobFiles {
		wantPaths = append(wantPaths, descriptor.Path)
	}
	if !slices.Equal(paths, wantPaths) {
		t.Fatalf("copy order = %v, want %v", paths, wantPaths)
	}

	_, err = CopyVerified(context.Background(), output, func(_ context.Context, entry BundleEntry, reader io.Reader) error {
		if entry.Path == DatabaseBundlePath {
			_, err := io.CopyN(io.Discard, reader, entry.ByteSize-1)
			return err
		}
		_, err := io.Copy(io.Discard, reader)
		return err
	})
	if err == nil || errors.Is(err, ErrInvalidBundle) {
		t.Fatalf("short CopyVerified sink error = %v, want consumer error distinct from invalid source", err)
	}
}

func TestM7BackupVerifiedSQLiteDescriptorDoesNotReopenSwappedPath(t *testing.T) {
	fixture := newBackupFixture(t)
	output := filepath.Join(fixture.parent, "descriptor-bound")
	if _, err := Create(context.Background(), fixture.request(output, false)); err != nil {
		t.Fatal(err)
	}
	policy, err := fssecure.CurrentSecurityPolicy()
	if err != nil {
		t.Fatal(err)
	}
	root, err := fssecure.OpenRootReadOnly(output, policy)
	if err != nil {
		t.Fatal(err)
	}
	defer root.Close()
	bound, err := openBoundBundleFile(root, DatabaseBundlePath)
	if err != nil {
		t.Fatal(err)
	}
	defer bound.Close()
	databasePath := filepath.Join(output, filepath.FromSlash(DatabaseBundlePath))
	inspectionPath, err := descriptorpath.ReadOnly(bound.handle.File(), databasePath)
	if err != nil {
		t.Fatal(err)
	}
	expected := mustReadFile(t, databasePath)

	if runtime.GOOS == "windows" {
		if err := os.Rename(databasePath, databasePath+".moved"); err == nil {
			t.Fatal("share-read-only verified database handle allowed namespace replacement")
		}
		if writer, err := os.OpenFile(databasePath, os.O_WRONLY, 0); err == nil {
			_ = writer.Close()
			t.Fatal("share-read-only verified database handle allowed external write open")
		}
		inspection, err := storesqlite.OpenImmutableInspection(context.Background(), inspectionPath)
		if err != nil {
			t.Fatalf("open immutable retained Windows database: %v", err)
		}
		if err := inspection.Close(); err != nil {
			t.Fatal(err)
		}
		return
	}

	moved := databasePath + ".moved"
	if err := os.Rename(databasePath, moved); err != nil {
		t.Fatal(err)
	}
	replacement := []byte("not-the-verified-sqlite-database")
	if err := os.WriteFile(databasePath, replacement, 0o600); err != nil {
		t.Fatal(err)
	}
	observed, err := os.ReadFile(inspectionPath)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(observed, expected) || bytes.Equal(observed, replacement) {
		t.Fatal("descriptor-backed inspection followed the replacement namespace path")
	}
	inspection, err := storesqlite.OpenImmutableInspection(context.Background(), inspectionPath)
	if err != nil {
		t.Fatalf("open immutable retained descriptor after path swap: %v", err)
	}
	if err := inspection.Close(); err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(databasePath); err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(moved, databasePath); err != nil {
		t.Fatal(err)
	}
}

func TestM7BackupCreateExcludesOutboxAndProjectionState(t *testing.T) {
	fixture := newBackupFixture(t)
	db := openTestDatabase(t, fixture.database)
	mustExecBackup(t, db, `INSERT INTO analytics_outbox(
		outbox_id, resident_id, source_commit_seq, payload, status, attempt_count, last_error, available_at, dispatched_at
	) VALUES ('01ARZ3NDEKTSV4RRFFQ69G5FAV', NULL, 1, '{}', 'pending', 0, NULL, 1, NULL)`)
	mustExecBackup(t, db, `INSERT INTO search_outbox(
		outbox_id, resident_id, source_commit_seq, payload, status, attempt_count, last_error, available_at, dispatched_at
	) VALUES ('01ARZ3NDEKTSV4RRFFQ69G5FAW', NULL, 1, '{}', 'pending', 0, NULL, 1, NULL)`)
	mustExecBackup(t, db, `INSERT INTO projection_watermarks(
		projection_name, resident_id, projection_version, source_commit_seq, as_of, as_of_tz
	) VALUES ('unknown_projection', '01ARZ3NDEKTSV4RRFFQ69G5FAX', 'unknown-v1', 1, 1, 'UTC')`)
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	output := filepath.Join(fixture.parent, "excluded")
	if _, err := Create(context.Background(), fixture.request(output, false)); err != nil {
		t.Fatalf("create backup: %v", err)
	}
	clone := openTestDatabase(t, filepath.Join(output, filepath.FromSlash(DatabaseBundlePath)))
	defer clone.Close()
	for _, table := range []string{"analytics_outbox", "search_outbox", "projection_watermarks"} {
		var count int
		if err := clone.QueryRow("SELECT COUNT(*) FROM " + table).Scan(&count); err != nil {
			t.Fatalf("count %s: %v", table, err)
		}
		if count != 0 {
			t.Fatalf("backup retained %d %s rows", count, table)
		}
	}
}

func TestM7BackupCreateIncludesExactProductionProjectionRegistryOnRequest(t *testing.T) {
	fixture := newBackupFixture(t)
	residentID := populateBackupResident(t, fixture)
	output := filepath.Join(fixture.parent, "included")
	result, err := Create(context.Background(), fixture.request(output, true))
	if err != nil {
		t.Fatalf("create projection-included backup: %v", err)
	}
	if !result.ProjectionsIncluded {
		t.Fatal("projection-included result did not retain requested policy")
	}
	verified, err := Verify(context.Background(), output)
	if err != nil {
		t.Fatal(err)
	}
	want, err := activeProjectionEntries()
	if err != nil {
		t.Fatal(err)
	}
	if verified.Manifest.Projections.Policy != "included_requested" || !slices.Equal(verified.Manifest.Projections.Entries, want) {
		t.Fatalf("included registry = %+v, want %+v", verified.Manifest.Projections, want)
	}
	clone := openTestDatabase(t, filepath.Join(output, filepath.FromSlash(DatabaseBundlePath)))
	defer clone.Close()
	var referenceCount int
	if err := clone.QueryRow(`SELECT COUNT(*) FROM content_references WHERE resident_id = ?`, residentID.String()).Scan(&referenceCount); err != nil {
		t.Fatal(err)
	}
	if referenceCount == 0 {
		t.Fatal("projection-included backup discarded the content_references body")
	}
	var watermarkCount int
	if err := clone.QueryRow(`SELECT COUNT(*) FROM projection_watermarks
		WHERE projection_name = 'content_references' AND resident_id = ?`, residentID.String()).Scan(&watermarkCount); err != nil {
		t.Fatal(err)
	}
	if watermarkCount != 1 {
		t.Fatalf("content_references watermark count = %d, want 1", watermarkCount)
	}
}

func TestM7BackupCopiesOnlyPresentReferencedDualBlob(t *testing.T) {
	fixture := newBackupFixture(t)
	residentID := populateBackupResident(t, fixture)
	sourceBlobs, err := blob.NewFileStore(filepath.Join(fixture.source, "blobs"))
	if err != nil {
		t.Fatal(err)
	}
	staged, err := sourceBlobs.Stage(context.Background(), residentID, strings.NewReader("filesystem-only-orphan"))
	if err != nil {
		t.Fatal(err)
	}
	if staged.TemporaryName() == "" {
		t.Fatal("orphan staging token is empty")
	}

	output := filepath.Join(fixture.parent, "with-blob")
	if _, err := Create(context.Background(), fixture.request(output, false)); err != nil {
		t.Fatalf("create backup with present blob: %v", err)
	}
	verified, err := Verify(context.Background(), output)
	if err != nil {
		t.Fatalf("verify backup with present blob: %v", err)
	}
	if len(verified.Manifest.BlobFiles) != 1 {
		t.Fatalf("blob_files = %d, want exactly the referenced principles blob", len(verified.Manifest.BlobFiles))
	}
	if verified.Manifest.BlobFiles[0].ResidentID != residentID.String() {
		t.Fatalf("blob resident = %s, want %s", verified.Manifest.BlobFiles[0].ResidentID, residentID)
	}
	if _, err := os.Lstat(filepath.Join(output, "blobs", "staging")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("backup retained source staging state: %v", err)
	}
}

func TestM7BackupRejectsDatabaseFilesystemBlobMismatchBeforePublish(t *testing.T) {
	fixture := newBackupFixture(t)
	residentID := populateBackupResident(t, fixture)
	digest := canonical.HashBlob([]byte("principles"))
	objectPath := filepath.Join(fixture.source, "blobs", "objects", residentID.String(), digest.Hex()[:2], digest.Hex()[2:])
	if err := os.WriteFile(objectPath, []byte("tampered!!"), 0o600); err != nil {
		t.Fatal(err)
	}
	output := filepath.Join(fixture.parent, "must-not-publish")
	if _, err := Create(context.Background(), fixture.request(output, false)); !errors.Is(err, ErrSourceUnavailable) {
		t.Fatalf("mismatched dual blob error = %v, want ErrSourceUnavailable", err)
	}
	if _, err := os.Lstat(output); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("mismatched source published output: %v", err)
	}
}

func TestM7BackupRejectsNonCurrentContentReferencesBeforePublish(t *testing.T) {
	fixture := newBackupFixture(t)
	residentID := populateBackupResident(t, fixture)
	database := openTestDatabase(t, fixture.database)
	mustExecBackup(t, database, `DELETE FROM projection_watermarks
		WHERE projection_name = 'content_references' AND resident_id = ?`, residentID.String())
	if err := database.Close(); err != nil {
		t.Fatal(err)
	}
	output := filepath.Join(fixture.parent, "must-not-publish-stale-content-references")
	if _, err := Create(context.Background(), fixture.request(output, false)); !errors.Is(err, ErrSourceUnavailable) {
		t.Fatalf("stale content references error = %v", err)
	}
	if _, err := os.Lstat(output); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("stale content references published output: %v", err)
	}
}

func TestM7BackupRejectsHalfErasedClaimAliasBeforePublishWithoutFinding(t *testing.T) {
	fixture := newBackupFixture(t)
	populateBackupResidentWithAction(t, fixture, backupClaimGenerator{}, func(application *app.Application, residentID canonical.ID) {
		ctx := context.Background()
		if err := application.ApprovePrinciples(ctx, residentID); err != nil {
			t.Fatal(err)
		}
		if err := application.FinalizeBootstrap(ctx, residentID, "alias persona",
			`{"mandatory_event_types":[],"memory_recall_enabled":false,"version":"memory-policy-v1"}`); err != nil {
			t.Fatal(err)
		}
		if err := application.SelectResident(ctx, residentID); err != nil {
			t.Fatal(err)
		}
		if _, err := application.ActivateMemoryPolicyV4(ctx, app.ActivateMemoryPolicyV4Options{
			ResidentID: residentID, ExpectedFrom: memory.PolicyVersionV1,
			AcknowledgeRecallEnable: true, AcknowledgeSelfTalkExtraction: true,
		}); err != nil {
			t.Fatal(err)
		}
		if _, err := application.Ingress(ctx, "I own a blue bicycle"); err != nil {
			t.Fatal(err)
		}
		if err := application.ProcessResident(ctx, residentID); err != nil {
			t.Fatal(err)
		}
	})
	makeBackupHalfErasedAlias(t, fixture.database)
	before := backupAliasObservation(t, fixture.database)
	output := filepath.Join(fixture.parent, "must-not-publish-half-erased-alias")
	if _, err := Create(context.Background(), fixture.request(output, false)); !errors.Is(err, ErrSourceUnavailable) {
		t.Fatalf("half-erased alias backup error = %v, want ErrSourceUnavailable", err)
	}
	if _, err := os.Lstat(output); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("half-erased alias published output: %v", err)
	}
	after := backupAliasObservation(t, fixture.database)
	if after != before {
		t.Fatalf("fatal alias backup mutated source: before=%+v after=%+v", before, after)
	}
}

func TestM7BackupRejectsHotSQLiteNamespaceBeforePublish(t *testing.T) {
	fixture := newBackupFixture(t)
	policy, err := fssecure.CurrentSecurityPolicy()
	if err != nil {
		t.Fatal(err)
	}
	root, err := fssecure.OpenRoot(fixture.source, policy)
	if err != nil {
		skipBackupWindowsSandbox(t, err)
		t.Fatal(err)
	}
	wal, err := root.OpenOrCreateRegular("mahoroba.db-wal")
	if err != nil {
		_ = root.Close()
		t.Fatal(err)
	}
	if _, err := wal.File().Write([]byte("active-wal")); err != nil {
		t.Fatal(err)
	}
	if err := wal.Seal(); err != nil {
		t.Fatal(err)
	}
	if err := errors.Join(wal.Close(), root.Close()); err != nil {
		t.Fatal(err)
	}
	output := filepath.Join(fixture.parent, "must-not-publish-hot-wal")
	if _, err := Create(context.Background(), fixture.request(output, false)); !errors.Is(err, ErrSourceUnavailable) {
		t.Fatalf("hot WAL backup error=%v", err)
	}
	if _, err := os.Lstat(output); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("hot WAL backup published output: %v", err)
	}
}

type backupAliasState struct {
	commits  int
	findings int
	claims   int
	nullHash int
}

func backupAliasObservation(t *testing.T, path string) backupAliasState {
	t.Helper()
	database := openTestDatabase(t, path)
	defer database.Close()
	var state backupAliasState
	if err := database.QueryRow(`SELECT
		(SELECT COUNT(*) FROM canonical_commits),
		(SELECT COUNT(*) FROM integrity_findings),
		(SELECT COUNT(*) FROM claims),
		(SELECT COUNT(*) FROM claims WHERE statement_hash IS NULL)`).Scan(
		&state.commits, &state.findings, &state.claims, &state.nullHash,
	); err != nil {
		t.Fatal(err)
	}
	return state
}

func makeBackupHalfErasedAlias(t *testing.T, path string) {
	t.Helper()
	database := openTestDatabase(t, path)
	defer database.Close()
	tx, err := database.Begin()
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	var sourceClaim, triggerSQL string
	if err := tx.QueryRow(`SELECT claim_id FROM claims ORDER BY claim_id LIMIT 1`).Scan(&sourceClaim); err != nil {
		t.Fatal(err)
	}
	aliasID, err := canonical.NewSecureIDGenerator().New()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec(`INSERT INTO claims
		SELECT ?, canonical_commit_id, owner_resident_id, subject_principal_id,
		perspective_principal_id, kind, temporal_kind, statement_content_id,
		statement_hash, statement_hash_algorithm, statement_normalization_version,
		created_by_run_id, recorded_at, recorded_tz
		FROM claims WHERE claim_id = ?`, aliasID.String(), sourceClaim); err != nil {
		t.Fatal(err)
	}
	if err := tx.QueryRow(`SELECT sql FROM sqlite_schema
		WHERE type = 'trigger' AND name = 'trg_claims_update_contract'`).Scan(&triggerSQL); err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec(`DROP TRIGGER trg_claims_update_contract`); err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec(`UPDATE claims SET statement_hash = NULL WHERE claim_id = ?`, aliasID.String()); err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec(triggerSQL); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
}

type backupClaimGenerator struct{}

func (backupClaimGenerator) Stream(
	_ context.Context,
	request generation.Request,
	_ generation.DeltaSink,
) (generation.Result, error) {
	if request.Purpose == string(domain.GenerationPurposeMemoryExtraction) {
		return generation.Result{Text: `{"claims":[{"grade":"stated","perspective":"source_actor","source_quote":"blue bicycle","statement":"The owner has a blue bicycle","subject":"source_actor","temporal_kind":"stable"}],"version":"memory-extraction-output-v1"}`}, nil
	}
	return generation.Result{Text: "dialogue response"}, nil
}

func TestM7BackupVerifyRejectsTamperExtraAndIncompleteMarker(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*testing.T, string)
	}{
		{name: "manifest_tamper", mutate: func(t *testing.T, root string) {
			path := filepath.Join(root, "manifest.json")
			body := mustReadFile(t, path)
			body = bytes.Replace(body, []byte(`"binary_version":"test"`), []byte(`"binary_version":"evil"`), 1)
			if err := os.WriteFile(path, body, 0o600); err != nil {
				t.Fatal(err)
			}
		}},
		{name: "extra_file", mutate: func(t *testing.T, root string) {
			if err := os.WriteFile(filepath.Join(root, "extra"), []byte("unexpected"), 0o600); err != nil {
				t.Fatal(err)
			}
		}},
		{name: "incomplete_marker", mutate: func(t *testing.T, root string) {
			if err := os.WriteFile(filepath.Join(root, "COMPLETE"), []byte("sha256:00\n"), 0o600); err != nil {
				t.Fatal(err)
			}
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			fixture := newBackupFixture(t)
			output := filepath.Join(fixture.parent, "bundle")
			if _, err := Create(context.Background(), fixture.request(output, false)); err != nil {
				t.Fatalf("create: %v", err)
			}
			test.mutate(t, output)
			if _, err := Verify(context.Background(), output); !errors.Is(err, ErrInvalidBundle) {
				t.Fatalf("Verify error = %v, want ErrInvalidBundle", err)
			}
		})
	}
}

func TestM7BackupManifestRejectsNestedUnknownDuplicateAndTraversal(t *testing.T) {
	fixture := newBackupFixture(t)
	output := filepath.Join(fixture.parent, "bundle")
	if _, err := Create(context.Background(), fixture.request(output, false)); err != nil {
		t.Fatal(err)
	}
	body := mustReadFile(t, filepath.Join(output, "manifest.json"))
	manifest, err := decodeManifest(body)
	if err != nil {
		t.Fatal(err)
	}

	canonicalBody := body[:len(body)-1]
	duplicate := append(append([]byte(nil), canonicalBody[:len(canonicalBody)-1]...), []byte(`,"service_ready":false}`)...)
	duplicate = append(duplicate, '\n')
	if _, err := decodeManifest(duplicate); !errors.Is(err, ErrInvalidBundle) {
		t.Fatalf("duplicate manifest error = %v", err)
	}

	unknown := bytes.Replace(canonicalBody, []byte(`"go_version":"`), []byte(`"extra":"x","go_version":"`), 1)
	canonicalUnknown, err := canonical.CanonicalizeRFC8785(unknown)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := decodeManifest(append(canonicalUnknown.Bytes(), '\n')); !errors.Is(err, ErrInvalidBundle) {
		t.Fatalf("nested unknown manifest error = %v", err)
	}

	manifest.DatabaseFile.Path = "../mahoroba.db"
	if err := validateManifest(manifest); !errors.Is(err, ErrInvalidBundle) {
		t.Fatalf("traversal manifest error = %v", err)
	}
}

func TestM7BackupCreateRejectsOverlapAndExistingTargetBeforeStaging(t *testing.T) {
	fixture := newBackupFixture(t)
	overlap := filepath.Join(fixture.source, "backup")
	if _, err := Create(context.Background(), fixture.request(overlap, false)); !errors.Is(err, ErrUnsafeOverlap) {
		t.Fatalf("overlap error = %v, want ErrUnsafeOverlap", err)
	}
	entries, err := os.ReadDir(fixture.source)
	if err != nil {
		t.Fatal(err)
	}
	for _, entry := range entries {
		if strings.Contains(entry.Name(), "mahoroba-backup-staging") {
			t.Fatalf("overlap preflight created staging %q", entry.Name())
		}
	}
	existing := filepath.Join(fixture.parent, "existing")
	if err := os.Mkdir(existing, 0o700); err != nil {
		t.Fatal(err)
	}
	if _, err := Create(context.Background(), fixture.request(existing, false)); !errors.Is(err, ErrTargetExists) {
		t.Fatalf("existing target error = %v, want ErrTargetExists", err)
	}
}

func TestM7BackupCreateDurablePublishPostRenameRetryCompletesMatchingMarker(t *testing.T) {
	fixture := newBackupFixture(t)
	output := filepath.Join(fixture.parent, "recoverable-bundle")
	request := fixture.request(output, false)
	request.publishFailpoint = func(point durablepublish.Failpoint) error {
		if point == durablepublish.FailpointAfterPayloadRename {
			return errors.New("injected post-rename crash")
		}
		return nil
	}
	if _, err := Create(context.Background(), request); !errors.Is(err, ErrDurabilityUnknown) {
		t.Fatalf("post-rename Create error = %v, want ErrDurabilityUnknown", err)
	}
	if _, err := os.Stat(filepath.Join(output, durablepublish.MarkerName)); err != nil {
		t.Fatalf("published target marker missing after crash: %v", err)
	}

	recovered, err := Create(context.Background(), fixture.request(output, false))
	if err != nil {
		t.Fatalf("matching retry: %v", err)
	}
	if recovered.ArtifactPath != output || recovered.FormatVersion != FormatVersion {
		t.Fatalf("recovered result = %+v", recovered)
	}
	if _, err := Verify(context.Background(), output); err != nil {
		t.Fatalf("recovered bundle Verify: %v", err)
	}
	if _, err := os.Lstat(filepath.Join(output, durablepublish.MarkerName)); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("target marker remains after recovery: %v", err)
	}
	assertNoBackupPublishSibling(t, fixture.parent, filepath.Base(output))
}

func TestM7BackupCreateDurablePublishNeverResumesDirectoryStaging(t *testing.T) {
	fixture := newBackupFixture(t)
	output := filepath.Join(fixture.parent, "pre-rename-bundle")
	request := fixture.request(output, false)
	request.publishFailpoint = func(point durablepublish.Failpoint) error {
		if point == durablepublish.FailpointAfterMarkersDurable {
			return errors.New("injected pre-rename crash")
		}
		return nil
	}
	if _, err := Create(context.Background(), request); !errors.Is(err, ErrDurabilityUnknown) {
		t.Fatalf("pre-rename Create error = %v, want ErrDurabilityUnknown", err)
	}
	before := backupPublishState(t, fixture.parent, filepath.Base(output))
	if before.stagingCount != 1 || before.siblingCount != 1 || before.targetPresent {
		t.Fatalf("pre-rename state = %+v", before)
	}
	if _, err := Create(context.Background(), fixture.request(output, false)); !errors.Is(err, ErrDurabilityUnknown) {
		t.Fatalf("directory retry error = %v, want ErrDurabilityUnknown", err)
	}
	after := backupPublishState(t, fixture.parent, filepath.Base(output))
	if after != before {
		t.Fatalf("directory retry mutated state: before=%+v after=%+v", before, after)
	}
}

func TestM7BackupCreateDurablePublishRejectsMismatchedRetryWithoutMutation(t *testing.T) {
	fixture := newBackupFixture(t)
	output := filepath.Join(fixture.parent, "mismatched-retry")
	request := fixture.request(output, false)
	request.publishFailpoint = func(point durablepublish.Failpoint) error {
		if point == durablepublish.FailpointAfterPayloadRename {
			return errors.New("injected post-rename crash")
		}
		return nil
	}
	if _, err := Create(context.Background(), request); !errors.Is(err, ErrDurabilityUnknown) {
		t.Fatalf("first Create error = %v", err)
	}
	before := backupPublishState(t, fixture.parent, filepath.Base(output))
	if _, err := Create(context.Background(), fixture.request(output, true)); !errors.Is(err, ErrDurabilityUnknown) {
		t.Fatalf("mismatched retry error = %v, want ErrDurabilityUnknown", err)
	}
	after := backupPublishState(t, fixture.parent, filepath.Base(output))
	if after != before {
		t.Fatalf("mismatched retry mutated marker state: before=%+v after=%+v", before, after)
	}
}

type backupPublishObservation struct {
	targetPresent bool
	stagingCount  int
	siblingCount  int
}

func backupPublishState(t *testing.T, parent, target string) backupPublishObservation {
	t.Helper()
	observation := backupPublishObservation{}
	if _, err := os.Lstat(filepath.Join(parent, target)); err == nil {
		observation.targetPresent = true
	} else if !errors.Is(err, os.ErrNotExist) {
		t.Fatal(err)
	}
	entries, err := os.ReadDir(parent)
	if err != nil {
		t.Fatal(err)
	}
	for _, entry := range entries {
		if strings.HasPrefix(entry.Name(), "."+target+".mahoroba-backup-staging-") {
			observation.stagingCount++
		}
		if strings.HasPrefix(entry.Name(), "."+target+".publish-pending.") {
			observation.siblingCount++
		}
	}
	return observation
}

func assertNoBackupPublishSibling(t *testing.T, parent, target string) {
	t.Helper()
	if state := backupPublishState(t, parent, target); state.siblingCount != 0 {
		t.Fatalf("backup publish sibling remains: %+v", state)
	}
}

func TestM7BackupVerifyRejectsSymlinkEntry(t *testing.T) {
	fixture := newBackupFixture(t)
	output := filepath.Join(fixture.parent, "bundle")
	if _, err := Create(context.Background(), fixture.request(output, false)); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(output, "unexpected-target"), []byte("x"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink("unexpected-target", filepath.Join(output, "extra-link")); err != nil {
		if runtime.GOOS == "windows" {
			// The same no-follow boundary is exercised by fssecure's Hosted
			// Windows reparse suite when local token policy forbids symlinks.
			t.Logf("local Windows token cannot create a symlink: %v", err)
			return
		}
		t.Fatal(err)
	}
	if _, err := Verify(context.Background(), output); !errors.Is(err, ErrInvalidBundle) {
		t.Fatalf("symlink Verify error = %v", err)
	}
}

type backupFixture struct {
	parent   string
	source   string
	database string
}

func newBackupFixture(t *testing.T) backupFixture {
	t.Helper()
	parent := t.TempDir()
	source := filepath.Join(parent, "source")
	// Exercise the production absent-target creation path. os.Mkdir inherits a
	// non-contract DACL on Windows and cannot model an M7 data root.
	lock, err := hostlock.Acquire(source)
	if err != nil {
		skipBackupWindowsSandbox(t, err)
		t.Fatalf("create managed source data root: %v", err)
	}
	if err := lock.Close(); err != nil {
		t.Fatal(err)
	}
	policy, err := fssecure.CurrentSecurityPolicy()
	if err != nil {
		t.Fatal(err)
	}
	root, err := fssecure.OpenRoot(source, policy)
	if err != nil {
		skipBackupWindowsSandbox(t, err)
		t.Fatalf("reopen managed source data root: %v", err)
	}
	databaseHandle, err := root.CreateRegular("mahoroba.db")
	if err != nil {
		_ = root.Close()
		skipBackupWindowsSandbox(t, err)
		t.Fatalf("create protected source database: %v", err)
	}
	if err := errors.Join(databaseHandle.Close(), root.Close()); err != nil {
		t.Fatal(err)
	}
	database := filepath.Join(source, "mahoroba.db")
	store, err := storesqlite.Open(context.Background(), database)
	if err != nil {
		t.Fatalf("create source database: %v", err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := blob.NewFileStore(filepath.Join(source, "blobs")); err != nil {
		t.Fatalf("create source blob layout: %v", err)
	}
	return backupFixture{parent: parent, source: source, database: database}
}

func skipBackupWindowsSandbox(t *testing.T, err error) {
	t.Helper()
	if runtime.GOOS == "windows" && os.Getenv("CI") == "" && errors.Is(err, os.ErrPermission) {
		t.Skipf("desktop sandbox cannot grant exact protected data-root ACL: %v", err)
	}
}

func populateBackupResident(t *testing.T, fixture backupFixture) canonical.ID {
	return populateBackupResidentWithAction(t, fixture, nil, nil)
}

func populateBackupResidentWithAction(
	t *testing.T,
	fixture backupFixture,
	generator generation.Generator,
	action func(*app.Application, canonical.ID),
) canonical.ID {
	t.Helper()
	store, err := storesqlite.Open(context.Background(), fixture.database)
	if err != nil {
		t.Fatal(err)
	}
	objects, err := blob.NewFileStore(filepath.Join(fixture.source, "blobs"))
	if err != nil {
		_ = store.Close()
		t.Fatal(err)
	}
	ids := canonical.NewSecureIDGenerator()
	timezone := canonical.MustTimezone("UTC")
	writer, err := canonical.OpenWriter(context.Background(), canonical.WriterOptions{
		Backend: store.Canonical(), IDs: ids, Clock: canonical.SystemClock{}, Timezone: timezone, QueueCapacity: 16,
	})
	if err != nil {
		_ = store.Close()
		t.Fatal(err)
	}
	registry, err := storesqlite.ActiveProjectionRegistry()
	if err != nil {
		_ = writer.Close(context.Background())
		_ = store.Close()
		t.Fatal(err)
	}
	projectionStore := store.Projection()
	coordinator, err := projection.NewCoordinator(projection.CoordinatorOptions{
		Registry: registry, Source: projectionStore, Store: projectionStore,
		Clock: canonical.SystemClock{}, Timezone: timezone,
		ScanInterval: time.Hour, AsOfRefreshInterval: time.Hour,
		RebuildRetryInterval: time.Minute, MaxStaleness: time.Hour,
	})
	if err != nil {
		_ = writer.Close(context.Background())
		_ = store.Close()
		t.Fatal(err)
	}
	application, err := app.New(app.Options{
		Writer: writer, CommitNotifier: coordinator, Repository: store.Canonical(), IDs: ids, Clock: canonical.SystemClock{}, Timezone: timezone, Blobs: objects,
		Generator: generator, Provider: "test", Model: "test-model",
		MaxAttempts: 1, RetryBackoff: []time.Duration{}, MaxInputBytes: 4096, MaxOutputBytes: 4096, SafetyScanInterval: time.Second,
	})
	if err != nil {
		_ = writer.Close(context.Background())
		_ = store.Close()
		t.Fatal(err)
	}
	state, err := application.BootstrapInit(context.Background(), app.BootstrapInput{
		OwnerName: "owner", Name: "resident", SeedKey: "seed", Principles: "principles",
	})
	if err != nil {
		_ = writer.Close(context.Background())
		_ = store.Close()
		t.Fatal(err)
	}
	if len(state.Residents) != 1 {
		t.Fatalf("bootstrap residents = %d, want 1", len(state.Residents))
	}
	if action != nil {
		action(application, state.Residents[0].ResidentID)
	}
	if err := writer.Close(context.Background()); err != nil {
		_ = store.Close()
		t.Fatal(err)
	}
	if _, err := coordinator.PreflightContentReferences(context.Background()); err != nil {
		_ = store.Close()
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	return state.Residents[0].ResidentID
}

func (fixture backupFixture) request(output string, include bool) CreateRequest {
	return CreateRequest{
		SourceDataDir: fixture.source, DatabaseFilename: "mahoroba.db", Output: output,
		IncludeProjections: include,
		CreatedBy:          CreatedBy{BinaryVersion: "test", GitRevision: "0123456789abcdef", GoVersion: runtime.Version()},
	}
}

func openTestDatabase(t *testing.T, path string) *sql.DB {
	t.Helper()
	db, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	if err := db.Ping(); err != nil {
		_ = db.Close()
		t.Fatal(err)
	}
	return db
}

func mustExecBackup(t *testing.T, db *sql.DB, statement string, arguments ...any) {
	t.Helper()
	if _, err := db.Exec(statement, arguments...); err != nil {
		t.Fatal(err)
	}
}

func mustReadFile(t *testing.T, path string) []byte {
	t.Helper()
	body, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	return body
}
