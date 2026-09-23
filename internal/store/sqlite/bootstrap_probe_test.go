package sqlite

import (
	"context"
	"path/filepath"
	"testing"

	"mahoroba.local/mahoroba/internal/canonical"
)

func TestCOV2BootstrapProbeDoesNotRequireCurrentPipeline(t *testing.T) {
	ctx := context.Background()
	databasePath := filepath.Join(t.TempDir(), "bootstrap-probe.db")
	database, err := Open(ctx, databasePath)
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	repository := database.Canonical()

	initialized, err := repository.BootstrapInitialized(ctx)
	if err != nil || initialized {
		t.Fatalf("fresh initialization probe = %v, %v", initialized, err)
	}
	residentIDs, err := repository.ListResidentIDs(ctx)
	if err != nil || len(residentIDs) != 0 {
		t.Fatalf("fresh resident IDs = %v, %v", residentIDs, err)
	}

	commitID := mustBootstrapProbeID(t, "01J00000000000000000000001")
	residentID := mustBootstrapProbeID(t, "01J00000000000000000000002")
	principalID := mustBootstrapProbeID(t, "01J00000000000000000000003")
	tx, err := database.writer.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO canonical_commits(
		canonical_commit_id, commit_seq, resident_id, committed_at, committed_tz
	) VALUES (?, 1, ?, 100, 'UTC')`, commitID.String(), residentID.String()); err != nil {
		_ = tx.Rollback()
		t.Fatal(err)
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO principals(
		principal_id, canonical_commit_id, kind, display_name, created_at, created_tz
	) VALUES (?, ?, 'resident', 'resident', 100, 'UTC')`, principalID.String(), commitID.String()); err != nil {
		_ = tx.Rollback()
		t.Fatal(err)
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO residents(
		resident_id, canonical_commit_id, principal_id, name, description_content_id, seed_key,
		parent_resident_id, branched_from_seq, branched_at, branched_tz, created_at, created_tz
	) VALUES (?, ?, ?, 'resident', NULL, 'resident', NULL, NULL, NULL, NULL, 100, 'UTC')`,
		residentID.String(), commitID.String(), principalID.String()); err != nil {
		_ = tx.Rollback()
		t.Fatal(err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}

	initialized, err = repository.BootstrapInitialized(ctx)
	if err != nil || !initialized {
		t.Fatalf("initialized probe = %v, %v", initialized, err)
	}
	residentIDs, err = repository.ListResidentIDs(ctx)
	if err != nil || len(residentIDs) != 1 || residentIDs[0] != residentID {
		t.Fatalf("resident IDs without pipeline = %v, %v", residentIDs, err)
	}
	exists, err := repository.ResidentExists(ctx, residentID)
	if err != nil || !exists {
		t.Fatalf("resident existence without pipeline = %v, %v", exists, err)
	}
	unknownID := mustBootstrapProbeID(t, "01J00000000000000000000004")
	exists, err = repository.ResidentExists(ctx, unknownID)
	if err != nil || exists {
		t.Fatalf("unknown resident existence without pipeline = %v, %v", exists, err)
	}

	inspection, err := OpenInspection(ctx, databasePath)
	if err != nil {
		t.Fatal(err)
	}
	defer inspection.Close()
	residentIDs, err = inspection.Canonical().ListResidentIDs(ctx)
	if err != nil || len(residentIDs) != 1 || residentIDs[0] != residentID {
		t.Fatalf("inspection resident IDs without pipeline = %v, %v", residentIDs, err)
	}
	exists, err = inspection.Canonical().ResidentExists(ctx, unknownID)
	if err != nil || exists {
		t.Fatalf("inspection unknown resident existence without pipeline = %v, %v", exists, err)
	}
}

func mustBootstrapProbeID(t *testing.T, raw string) canonical.ID {
	t.Helper()
	id, err := canonical.ParseID(raw)
	if err != nil {
		t.Fatal(err)
	}
	return id
}
