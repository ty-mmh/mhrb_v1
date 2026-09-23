package sqlite

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/pressly/goose/v3"
	"mahoroba.local/mahoroba/internal/assets/migrations"
)

func TestM7V10PopulatedUpgradeReachesV13(t *testing.T) {
	fixture, closeFixture := newSemanticFixtureWithMigrations(t, migrationFilesThrough(t, 10))
	defer closeFixture()
	ctx := context.Background()
	if version, err := currentVersion(ctx, fixture.db); err != nil || version != 10 {
		t.Fatalf("genuine v10 fixture version = %d, err=%v", version, err)
	}
	if got := v10SchemaFingerprint(t, fixture.db); got != genuineV10SchemaFingerprint {
		t.Fatalf("genuine v10 fingerprint = %s, want pinned %s", got, genuineV10SchemaFingerprint)
	}
	before := snapshotMigrationRows(t, fixture)
	if err := migrateUp(ctx, fixture.db); err != nil {
		t.Fatal(err)
	}
	version, err := currentVersion(ctx, fixture.db)
	if err != nil || version != 13 {
		t.Fatalf("version after valid upgrade = %d, err=%v", version, err)
	}
	assertMigrationRowsPreserved(t, fixture, before)
}

func TestM7V11PopulatedUpgradeReachesV13(t *testing.T) {
	fixture, closeFixture := newSemanticFixtureWithMigrations(t, migrationFilesThrough(t, 11))
	defer closeFixture()
	ctx := context.Background()
	if version, err := currentVersion(ctx, fixture.db); err != nil || version != 11 {
		t.Fatalf("genuine v11 fixture version = %d, err=%v", version, err)
	}
	before := snapshotMigrationRows(t, fixture)
	if err := migrateUp(ctx, fixture.db); err != nil {
		t.Fatal(err)
	}
	version, err := currentVersion(ctx, fixture.db)
	if err != nil || version != 13 {
		t.Fatalf("version after valid upgrade = %d, err=%v", version, err)
	}
	assertMigrationRowsPreserved(t, fixture, before)
	var legacyRows, legacyRowsWithM7Envelope int
	if err := fixture.db.QueryRow(`SELECT COUNT(*), COUNT(finding_fingerprint)
		FROM integrity_findings`).Scan(&legacyRows, &legacyRowsWithM7Envelope); err != nil {
		t.Fatal(err)
	}
	if legacyRows == 0 || legacyRowsWithM7Envelope != 0 {
		t.Fatalf("legacy finding rows = %d, rows with M7 envelope = %d", legacyRows, legacyRowsWithM7Envelope)
	}
}

func TestCOVR02V12PopulatedUpgradeReachesV13WithoutCanonicalMutation(t *testing.T) {
	fixture, closeFixture := newSemanticFixtureWithMigrations(t, migrationFilesThrough(t, 12))
	defer closeFixture()
	ctx := context.Background()
	if version, err := currentVersion(ctx, fixture.db); err != nil || version != 12 {
		t.Fatalf("genuine v12 fixture version = %d, err=%v", version, err)
	}
	before := snapshotMigrationRows(t, fixture)
	if err := migrateUp(ctx, fixture.db); err != nil {
		t.Fatal(err)
	}
	version, err := currentVersion(ctx, fixture.db)
	if err != nil || version != 13 {
		t.Fatalf("version after valid upgrade = %d, err=%v", version, err)
	}
	assertMigrationRowsPreserved(t, fixture, before)
	var definition string
	if err := fixture.db.QueryRow(`SELECT sql FROM sqlite_schema WHERE type='index' AND name=?`,
		"idx_generation_runs_commit_resident_purpose_run").Scan(&definition); err != nil {
		t.Fatal(err)
	}
	for _, column := range []string{"canonical_commit_id", "resident_id", "purpose", "generation_run_id"} {
		if !strings.Contains(definition, column) {
			t.Fatalf("COVR-02 index definition %q does not contain %s", definition, column)
		}
	}
}

func TestM7V11DuplicateAssertionIdentityUpgradeFailsClosed(t *testing.T) {
	for _, tc := range []struct {
		name      string
		errorCode string
		seed      func(*testing.T, *semanticFixture)
	}{
		{
			name:      "validity",
			errorCode: "m7_claim_validity_assertion_identity_duplicates_require_static_repair",
			seed: func(t *testing.T, fixture *semanticFixture) {
				for range 2 {
					mustExec(t, fixture.db, `INSERT INTO claim_validity_assertions(
						validity_assertion_id, canonical_commit_id, claim_id, assertion_type,
						valid_from, valid_from_tz, valid_to, valid_to_tz, evidence_event_id,
						confidence, actor_principal_id, reason_code, reason_content_id,
						recorded_at, recorded_tz
					) VALUES (?, ?, ?, 'observed', NULL, NULL, NULL, NULL, ?, 1000000, ?, 'test', NULL, ?, ?)`,
						fixture.ids.new(), fixture.commit["A"], fixture.claim["A"], fixture.event["A"],
						fixture.principal["human"], semanticTime, semanticTZ)
				}
			},
		},
		{
			name:      "view_scope",
			errorCode: "m7_claim_view_scope_assertion_identity_duplicates_require_static_repair",
			seed: func(t *testing.T, fixture *semanticFixture) {
				for range 2 {
					mustExec(t, fixture.db, `INSERT INTO claim_view_scope_assertions(
						view_scope_assertion_id, canonical_commit_id, claim_id, view_scope,
						actor_principal_id, generation_run_id, memory_policy_revision_id,
						reason_code, reason_content_id, recorded_at, recorded_tz
					) VALUES (?, ?, ?, 'resident_ui', ?, NULL, NULL, 'test', NULL, ?, ?)`,
						fixture.ids.new(), fixture.commit["A"], fixture.claim["A"],
						fixture.principal["human"], semanticTime, semanticTZ)
				}
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fixture, closeFixture := newSemanticFixtureWithMigrations(t, migrationFilesThrough(t, 11))
			defer closeFixture()
			tc.seed(t, fixture)
			ctx := context.Background()
			before := snapshotMigrationRows(t, fixture)
			fingerprint, err := applicationSchemaFingerprint(ctx, fixture.db)
			if err != nil {
				t.Fatal(err)
			}
			err = migrateUp(ctx, fixture.db)
			if err == nil || !strings.Contains(err.Error(), tc.errorCode) {
				t.Fatalf("duplicate preflight error = %v", err)
			}
			version, versionErr := currentVersion(ctx, fixture.db)
			if versionErr != nil || version != 11 {
				t.Fatalf("duplicate preflight changed schema version to %d, err=%v", version, versionErr)
			}
			if got, fingerprintErr := applicationSchemaFingerprint(ctx, fixture.db); fingerprintErr != nil || got != fingerprint {
				t.Fatalf("duplicate preflight changed schema fingerprint to %s, err=%v", got, fingerprintErr)
			}
			assertMigrationRowsPreserved(t, fixture, before)
			if got := snapshotMigrationRows(t, fixture)["schema_migrations"]; got != before["schema_migrations"] {
				t.Fatalf("duplicate preflight changed schema migration rows from %d to %d", before["schema_migrations"], got)
			}
		})
	}
}

func TestM7V12IntegrityFindingEnvelopeGuard(t *testing.T) {
	fixture, closeFixture := newSemanticFixture(t)
	defer closeFixture()
	pipelineID := fixture.ids.new()
	mustExec(t, fixture.db, `INSERT INTO pipeline_versions(
		pipeline_version_id, canonical_commit_id, pipeline_kind, version_key,
		definition, recorded_at, recorded_tz
	) VALUES (?, ?, 'integrity_check', 'integrity-check-v1', '{}', ?, ?)`,
		pipelineID, fixture.commit["global"], semanticTime, semanticTZ)

	incomplete := `INSERT INTO integrity_findings(
		integrity_finding_id, canonical_commit_id, resident_id, claim_id,
		finding_kind, source_content_erasure_event_id, pipeline_version_id,
		details_content_id, occurred_at, occurred_tz, recorded_at, recorded_tz
	) VALUES (?, ?, ?, ?, 'canonical_invariant_violation', NULL, ?, NULL, ?, ?, ?, ?)`
	err := executeRolledBack(fixture.db, incomplete,
		fixture.ids.new(), fixture.commit["A"], fixture.resident["A"], fixture.claim["A"],
		pipelineID, semanticTime, semanticTZ, semanticTime, semanticTZ)
	if err == nil || !strings.Contains(err.Error(), "M7 integrity finding envelope is incomplete") {
		t.Fatalf("incomplete M7 finding error = %v", err)
	}

	complete := `INSERT INTO integrity_findings(
		integrity_finding_id, canonical_commit_id, resident_id, claim_id,
		finding_kind, source_content_erasure_event_id, pipeline_version_id,
		details_content_id, occurred_at, occurred_tz, recorded_at, recorded_tz,
		finding_fingerprint, rule_code, target_kind, target_id, target_field
	) VALUES (?, ?, ?, ?, 'canonical_invariant_violation', NULL, ?, NULL, ?, ?, ?, ?, ?, 'claim_statement_pairing', 'claim', ?, 'statement_hash')`
	if err := executeRolledBack(fixture.db, complete,
		fixture.ids.new(), fixture.commit["A"], fixture.resident["A"], fixture.claim["A"],
		pipelineID, semanticTime, semanticTZ, semanticTime, semanticTZ,
		semanticDigest("finding-fingerprint"), fixture.claim["A"]); err != nil {
		t.Fatalf("complete M7 finding was rejected: %v", err)
	}
}

func TestM7V12AssertionIdentityIndexesRejectDuplicates(t *testing.T) {
	fixture, closeFixture := newSemanticFixture(t)
	defer closeFixture()

	validity := `INSERT INTO claim_validity_assertions(
		validity_assertion_id, canonical_commit_id, claim_id, assertion_type,
		valid_from, valid_from_tz, valid_to, valid_to_tz, evidence_event_id,
		confidence, actor_principal_id, reason_code, reason_content_id,
		recorded_at, recorded_tz
	) VALUES (?, ?, ?, 'observed', NULL, NULL, NULL, NULL, ?, 1000000, ?, 'test', NULL, ?, ?)`
	mustExec(t, fixture.db, validity, fixture.ids.new(), fixture.commit["A"], fixture.claim["A"],
		fixture.event["A"], fixture.principal["human"], semanticTime, semanticTZ)
	if err := executeRolledBack(fixture.db, validity, fixture.ids.new(), fixture.commit["A"], fixture.claim["A"],
		fixture.event["A"], fixture.principal["human"], semanticTime, semanticTZ); err == nil {
		t.Fatal("duplicate claim validity assertion identity was allowed")
	}

	viewScope := `INSERT INTO claim_view_scope_assertions(
		view_scope_assertion_id, canonical_commit_id, claim_id, view_scope,
		actor_principal_id, generation_run_id, memory_policy_revision_id,
		reason_code, reason_content_id, recorded_at, recorded_tz
	) VALUES (?, ?, ?, 'resident_ui', ?, NULL, NULL, 'test', NULL, ?, ?)`
	mustExec(t, fixture.db, viewScope, fixture.ids.new(), fixture.commit["A"], fixture.claim["A"],
		fixture.principal["human"], semanticTime, semanticTZ)
	if err := executeRolledBack(fixture.db, viewScope, fixture.ids.new(), fixture.commit["A"], fixture.claim["A"],
		fixture.principal["human"], semanticTime, semanticTZ); err == nil {
		t.Fatal("duplicate claim view-scope assertion identity was allowed")
	}
}

func TestM7V12ResidentAliasPairingKeepsSameCommitAndResidentGuards(t *testing.T) {
	for _, testCase := range []struct {
		name         string
		pairResident string
		pairCommit   string
		wantRejected bool
	}{
		{name: "same resident and commit"},
		{name: "cross resident rejected", pairResident: "B", wantRejected: true},
		{name: "cross commit rejected", pairCommit: "global", wantRejected: true},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			fixture, closeFixture := newSemanticFixture(t)
			defer closeFixture()
			resident := fixture.resident["A"]
			commit := fixture.commit["A"]
			content := fixture.content["A"]["claim"]
			eventID := fixture.ids.new()
			if _, err := fixture.db.Exec(`INSERT INTO content_erasure_events VALUES (?,?,?,?,?,?,?,?,?,?,?,?)`,
				eventID, commit, content, "resident", fixture.principal["human"], "resident_erase", nil, nil,
				semanticTime, semanticTZ, semanticTime, semanticTZ); err != nil {
				t.Fatal(err)
			}
			pairResident := resident
			if testCase.pairResident != "" {
				pairResident = fixture.resident[testCase.pairResident]
			}
			pairCommit := commit
			if testCase.pairCommit != "" {
				pairCommit = fixture.commit[testCase.pairCommit]
			}
			_, err := fixture.db.Exec(`INSERT INTO claim_statement_erasure_events VALUES (?,?,?,?,?,?,?)`,
				fixture.ids.new(), pairCommit, pairResident, fixture.claim["A"], eventID, semanticTime, semanticTZ)
			if testCase.wantRejected {
				if err == nil {
					t.Fatal("weakened alias scope guard accepted invalid pair")
				}
				return
			}
			if err != nil {
				t.Fatalf("resident-scope pair rejected: %v", err)
			}
			if _, err := fixture.db.Exec(`UPDATE claims SET statement_hash=NULL WHERE claim_id=?`, fixture.claim["A"]); err != nil {
				t.Fatalf("resident-scope paired hash erase rejected: %v", err)
			}
			var hash []byte
			if err := fixture.db.QueryRow(`SELECT statement_hash FROM claims WHERE claim_id=?`, fixture.claim["A"]).Scan(&hash); err != nil {
				t.Fatal(err)
			}
			if hash != nil {
				t.Fatalf("statement hash remains %x", hash)
			}
		})
	}
}

func TestM7InvalidV10ClaimIdentityUpgradeRollsBackAndRequestsStaticRepair(t *testing.T) {
	fixture, closeFixture := newSemanticFixtureWithMigrations(t, migrationFilesThrough(t, 10))
	defer closeFixture()
	ctx := context.Background()
	claimID := fixture.claim["A"]
	var contentID string
	if err := fixture.db.QueryRow("SELECT statement_content_id FROM claims WHERE claim_id = ?", claimID).Scan(&contentID); err != nil {
		t.Fatal(err)
	}
	if _, err := fixture.db.Exec("UPDATE content_objects SET erasure_state='erased', blob_hash=NULL, commitment_salt=NULL WHERE content_id = ?", contentID); err != nil {
		t.Fatal(err)
	}
	before := snapshotMigrationRows(t, fixture)
	fingerprint := v10SchemaFingerprint(t, fixture.db)
	err := migrateUp(ctx, fixture.db)
	if err == nil || !strings.Contains(err.Error(), "legacy_claim_identity_requires_static_repair") {
		t.Fatalf("invalid v10 upgrade error = %v", err)
	}
	version, err := currentVersion(ctx, fixture.db)
	if err != nil || version != 10 {
		t.Fatalf("invalid upgrade changed schema version to %d, err=%v", version, err)
	}
	if got := v10SchemaFingerprint(t, fixture.db); got != fingerprint {
		t.Fatalf("invalid upgrade changed schema fingerprint from %s to %s", fingerprint, got)
	}
	assertMigrationRowsPreserved(t, fixture, before)
}

func TestCOVR02V13DownIsRejectedBeforeRepositoryMutation(t *testing.T) {
	fixture, closeFixture := newSemanticFixture(t)
	defer closeFixture()
	ctx := context.Background()
	before := snapshotMigrationRows(t, fixture)
	err := migrateDownOneForTest(ctx, fixture.db, migrations.Files)
	if !errors.Is(err, ErrForwardOnlyMigration) {
		t.Fatalf("down error = %v, want ErrForwardOnlyMigration", err)
	}
	version, err := currentVersion(ctx, fixture.db)
	if err != nil || version != 13 {
		t.Fatalf("down rejection changed schema version to %d, err=%v", version, err)
	}
	assertMigrationRowsPreserved(t, fixture, before)
}

func TestCOVR02RawGooseV13DownGuardIsNonMutating(t *testing.T) {
	fixture, closeFixture := newSemanticFixture(t)
	defer closeFixture()
	ctx := context.Background()
	before := snapshotMigrationRows(t, fixture)
	gooseLegacyMu.Lock()
	defer gooseLegacyMu.Unlock()
	if err := configureGoose(migrations.Files); err != nil {
		t.Fatal(err)
	}
	err := goose.DownContext(ctx, fixture.db, ".")
	if err == nil || !strings.Contains(err.Error(), "migration 13 is forward-only") {
		t.Fatalf("raw Goose down error = %v", err)
	}
	version, err := currentVersion(ctx, fixture.db)
	if err != nil || version != 13 {
		t.Fatalf("raw Goose down changed schema version to %d, err=%v", version, err)
	}
	assertMigrationRowsPreserved(t, fixture, before)
}

func snapshotMigrationRows(t *testing.T, fixture *semanticFixture) map[string]int {
	t.Helper()
	result := make(map[string]int)
	for _, table := range []string{"residents", "content_objects", "generation_runs", "claims", "claim_evidence", "claim_stage_transitions", "claim_validity_assertions", "claim_view_scope_assertions", "content_erasure_events", "integrity_findings", "schema_migrations"} {
		var count int
		if err := fixture.db.QueryRow("SELECT COUNT(*) FROM \"" + table + "\"").Scan(&count); err != nil {
			t.Fatal(err)
		}
		result[table] = count
	}
	return result
}

func assertMigrationRowsPreserved(t *testing.T, fixture *semanticFixture, before map[string]int) {
	t.Helper()
	after := snapshotMigrationRows(t, fixture)
	for table, want := range before {
		if after[table] != want && table != "schema_migrations" {
			t.Fatalf("table %s row count changed from %d to %d", table, want, after[table])
		}
	}
}
