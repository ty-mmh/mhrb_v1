package sqlite

import (
	"context"
	"errors"
	"testing"

	"mahoroba.local/mahoroba/internal/canonical"
	"mahoroba.local/mahoroba/internal/integrity"
)

func TestM7MinimumCheckClaimStatementAliasMatrix(t *testing.T) {
	t.Run("valid_present", func(t *testing.T) {
		fixture, closeFixture := newSemanticFixture(t)
		defer closeFixture()
		makeSemanticFixtureMinimumValid(t, fixture)
		addMinimumCheckSharedClaimAlias(t, fixture, fixture.claim["A"])

		if err := newSQLiteMinimumChecker(fixture.db).Check(context.Background()); err != nil {
			t.Fatalf("MinimumCheck() present alias error = %v", err)
		}
	})

	t.Run("valid_fully_erased", func(t *testing.T) {
		evidence := performM7ClaimErasure(t)
		if err := newSQLiteMinimumChecker(evidence.DB).Check(context.Background()); err != nil {
			t.Fatalf("MinimumCheck() erased alias error = %v", err)
		}
	})
}

func TestM7MinimumCheckRejectsHalfErasedClaimAlias(t *testing.T) {
	fixture, closeFixture := newSemanticFixture(t)
	defer closeFixture()
	makeSemanticFixtureMinimumValid(t, fixture)
	aliasID := addMinimumCheckSharedClaimAlias(t, fixture, fixture.claim["A"])

	// Production SQL cannot produce this restore/import-style corruption. Drop
	// only the update trigger in the private fixture to exercise MinimumCheck's
	// final publication boundary.
	mustExec(t, fixture.db, "DROP TRIGGER trg_claims_update_contract")
	mustExec(t, fixture.db, "UPDATE claims SET statement_hash = NULL WHERE claim_id = ?", aliasID)

	err := newSQLiteMinimumChecker(fixture.db).Check(context.Background())
	if !errors.Is(err, integrity.ErrFatal) {
		t.Fatalf("MinimumCheck() error = %v, want integrity.ErrFatal", err)
	}
	var fatal *integrity.FatalError
	if !errors.As(err, &fatal) || fatal.Code != integrity.ClaimStatementAliasGroupInvalid {
		t.Fatalf("MinimumCheck() fatal = %#v", fatal)
	}

	var originalSet, aliasSet int
	if err := fixture.db.QueryRow(`SELECT
		(SELECT statement_hash IS NOT NULL FROM claims WHERE claim_id = ?),
		(SELECT statement_hash IS NOT NULL FROM claims WHERE claim_id = ?)`,
		fixture.claim["A"], aliasID,
	).Scan(&originalSet, &aliasSet); err != nil {
		t.Fatal(err)
	}
	if originalSet != 1 || aliasSet != 0 {
		t.Fatalf("read-only MinimumCheck changed alias state: original=%d alias=%d", originalSet, aliasSet)
	}
}

func TestM7MinimumCheckRejectsStructuralHistoryBeforeAnyFindingWrite(t *testing.T) {
	tests := []struct {
		name       string
		code       integrity.FatalCode
		breakState func(*testing.T, *semanticFixture)
	}{
		{
			name: "canonical_commit_gap", code: integrity.CanonicalCommitSequenceInvalid,
			breakState: func(t *testing.T, fixture *semanticFixture) {
				mustExec(t, fixture.db, `INSERT INTO canonical_commits(
					canonical_commit_id, commit_seq, resident_id, committed_at, committed_tz
				) VALUES (?, 5, NULL, ?, ?)`, fixture.ids.new(), semanticTime+10, semanticTZ)
			},
		},
		{
			name: "present_content_with_erasure_event", code: integrity.ContentErasureInvariantInvalid,
			breakState: func(t *testing.T, fixture *semanticFixture) {
				mustExec(t, fixture.db, `INSERT INTO content_erasure_events(
					content_erasure_event_id, canonical_commit_id, content_id, erasure_scope,
					actor_principal_id, reason_code, reason_content_id, source_erasure_event_id,
					occurred_at, occurred_tz, recorded_at, recorded_tz
				) VALUES (?, ?, ?, 'content', ?, 'minimum_test', NULL, NULL, ?, ?, ?, ?)`,
					fixture.ids.new(), fixture.commit["A"], fixture.content["A"]["event"],
					fixture.principal["human"], semanticTime+1, semanticTZ, semanticTime+1, semanticTZ)
			},
		},
		{
			name: "present_claim_digest_mismatch", code: integrity.PresentClaimIdentityInvalid,
			breakState: func(t *testing.T, fixture *semanticFixture) {
				mustExec(t, fixture.db, "DROP TRIGGER trg_claims_update_contract")
				mustExec(t, fixture.db, "UPDATE claims SET statement_hash = ? WHERE claim_id = ?",
					canonical.HashBlob([]byte("different")).Bytes(), fixture.claim["A"])
			},
		},
		{
			name: "generation_run_without_outcome", code: integrity.GenerationOutcomeHistoryInvalid,
			breakState: func(t *testing.T, fixture *semanticFixture) {
				mustExec(t, fixture.db, "DROP TRIGGER trg_generation_run_outcomes_no_delete")
				mustExec(t, fixture.db, "DELETE FROM generation_run_outcomes WHERE generation_run_id = ?", fixture.run["A"])
			},
		},
		{
			name: "runtime_selected_resident_missing", code: integrity.RuntimeConfigurationInvalid,
			breakState: func(t *testing.T, fixture *semanticFixture) {
				mustExec(t, fixture.db, `INSERT INTO runtime_config(
					singleton_id, active_resident_id, desired_sessionization_policy_version_id,
					updated_at, updated_tz
				) VALUES (1, ?, NULL, ?, ?)
				ON CONFLICT(singleton_id) DO UPDATE SET active_resident_id=excluded.active_resident_id,
					desired_sessionization_policy_version_id=NULL`,
					fixture.ids.new(), semanticTime, semanticTZ)
			},
		},
		{
			name: "runtime_selected_policy_missing", code: integrity.RuntimeConfigurationInvalid,
			breakState: func(t *testing.T, fixture *semanticFixture) {
				mustExec(t, fixture.db, `INSERT INTO runtime_config(
					singleton_id, active_resident_id, desired_sessionization_policy_version_id,
					updated_at, updated_tz
				) VALUES (1, ?, ?, ?, ?)
				ON CONFLICT(singleton_id) DO UPDATE SET active_resident_id=excluded.active_resident_id,
					desired_sessionization_policy_version_id=excluded.desired_sessionization_policy_version_id`,
					fixture.resident["A"], fixture.ids.new(), semanticTime, semanticTZ)
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			fixture, closeFixture := newSemanticFixture(t)
			defer closeFixture()
			makeSemanticFixtureMinimumValid(t, fixture)
			test.breakState(t, fixture)

			var commitsBefore, findingsBefore, outcomesBefore int
			if err := fixture.db.QueryRow(`SELECT
				(SELECT COUNT(*) FROM canonical_commits),
				(SELECT COUNT(*) FROM integrity_findings),
				(SELECT COUNT(*) FROM generation_run_outcomes)`).Scan(
				&commitsBefore, &findingsBefore, &outcomesBefore); err != nil {
				t.Fatal(err)
			}
			err := newSQLiteMinimumChecker(fixture.db).Check(context.Background())
			var fatal *integrity.FatalError
			if !errors.Is(err, integrity.ErrFatal) || !errors.As(err, &fatal) || fatal.Code != test.code {
				t.Fatalf("MinimumCheck() = %v (%#v), want %s", err, fatal, test.code)
			}
			var commitsAfter, findingsAfter, outcomesAfter int
			if err := fixture.db.QueryRow(`SELECT
				(SELECT COUNT(*) FROM canonical_commits),
				(SELECT COUNT(*) FROM integrity_findings),
				(SELECT COUNT(*) FROM generation_run_outcomes)`).Scan(
				&commitsAfter, &findingsAfter, &outcomesAfter); err != nil {
				t.Fatal(err)
			}
			if commitsAfter != commitsBefore || findingsAfter != findingsBefore || outcomesAfter != outcomesBefore {
				t.Fatalf("read-only gate mutated counts: before=%d/%d/%d after=%d/%d/%d",
					commitsBefore, findingsBefore, outcomesBefore, commitsAfter, findingsAfter, outcomesAfter)
			}
		})
	}
}

// The broad semantic fixture deliberately contains cross-table rows that are
// useful for isolated trigger tests but are not a valid live Canonical state:
// present content already has an erasure event, and generation runs have no
// outcomes. MinimumCheck evidence removes those synthetic probes and installs
// a legal pre-dispatch terminal history before asserting a valid baseline.
func makeSemanticFixtureMinimumValid(t *testing.T, fixture *semanticFixture) {
	t.Helper()
	mustExec(t, fixture.db, "DROP TRIGGER trg_integrity_findings_no_delete")
	mustExec(t, fixture.db, "DROP TRIGGER trg_content_erasure_events_no_delete")
	mustExec(t, fixture.db, "DELETE FROM integrity_findings")
	mustExec(t, fixture.db, "DELETE FROM content_erasure_events")
	for _, residentKey := range []string{"A", "B"} {
		mustExec(t, fixture.db, `INSERT INTO generation_run_outcomes(
			outcome_id, canonical_commit_id, generation_run_id, attempt_no, state,
			output_content_id, prompt_tokens, completion_tokens, latency, estimated_cost,
			error_class, error_detail_content_id, recorded_at, recorded_tz
		) VALUES (?, ?, ?, 0, 'cancelled', NULL, NULL, NULL, NULL, NULL,
			'source_content_erased', NULL, ?, ?)`, fixture.ids.new(), fixture.commit[residentKey],
			fixture.run[residentKey], semanticTime, semanticTZ)
	}
}

func addMinimumCheckSharedClaimAlias(t *testing.T, fixture *semanticFixture, sourceClaimID string) string {
	t.Helper()
	aliasID := fixture.ids.new()
	result, err := fixture.db.Exec(`INSERT INTO claims(
		claim_id, canonical_commit_id, owner_resident_id, subject_principal_id,
		perspective_principal_id, kind, temporal_kind, statement_content_id,
		statement_hash, statement_hash_algorithm, statement_normalization_version,
		created_by_run_id, recorded_at, recorded_tz
	)
	SELECT ?, canonical_commit_id, owner_resident_id, subject_principal_id,
	       perspective_principal_id, kind, temporal_kind, statement_content_id,
	       statement_hash, statement_hash_algorithm, statement_normalization_version,
	       created_by_run_id, recorded_at, recorded_tz
	FROM claims WHERE claim_id = ?`, aliasID, sourceClaimID)
	if err != nil {
		t.Fatal(err)
	}
	if affected, err := result.RowsAffected(); err != nil || affected != 1 {
		t.Fatalf("shared claim alias rows affected = %d, err = %v", affected, err)
	}
	return aliasID
}
