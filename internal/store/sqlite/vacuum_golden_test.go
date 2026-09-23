package sqlite

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"testing"

	"mahoroba.local/mahoroba/internal/canonical"
	"mahoroba.local/mahoroba/internal/domain"
)

func TestM7VACUUMLogicalStatePreservesTypedRepositorySnapshot(t *testing.T) {
	fixture, closeFixture := newSemanticFixture(t)
	defer closeFixture()
	for _, residentKey := range []string{"A", "B"} {
		mustExec(t, fixture.db, `INSERT INTO resident_status_transitions(
			resident_status_transition_id, canonical_commit_id, resident_id, from_status,
			to_status, actor_principal_id, reason_code, reason_content_id,
			occurred_at, occurred_tz, recorded_at, recorded_tz
		) VALUES (?, ?, ?, NULL, 'draft', ?, 'vacuum_fixture', NULL, ?, ?, ?, ?)`,
			fixture.ids.new(), fixture.commit[residentKey], fixture.resident[residentKey],
			fixture.principal["human"], semanticTime, semanticTZ, semanticTime, semanticTZ)
	}
	mustExec(t, fixture.db, `INSERT INTO pipeline_versions(
		pipeline_version_id, canonical_commit_id, pipeline_kind, version_key,
		definition, recorded_at, recorded_tz
	) VALUES (?, ?, 'dialogue', ?, '{}', ?, ?)`, fixture.ids.new(), fixture.commit["global"],
		domain.DialoguePipelineVersionV1, semanticTime, semanticTZ)
	mustExec(t, fixture.db, `INSERT INTO sessionization_policy_versions(
		sessionization_policy_version_id, canonical_commit_id, version_key,
		definition, recorded_at, recorded_tz
	) VALUES (?, ?, ?, '{}', ?, ?)`, fixture.ids.new(), fixture.commit["global"],
		domain.SessionPolicyVersion, semanticTime, semanticTZ)
	eventID := mustParseSnapshotID(fixture.event["A"])
	repurposeMalformedRun(t, fixture, domain.GenerationPurposeDialogue, domain.DialogueObligation(eventID))
	mustExec(t, fixture.db, `INSERT INTO runtime_config(
		singleton_id, active_resident_id, desired_sessionization_policy_version_id,
		updated_at, updated_tz
	) VALUES (1, ?, NULL, ?, ?)
	ON CONFLICT(singleton_id) DO UPDATE SET active_resident_id=excluded.active_resident_id,
		desired_sessionization_policy_version_id=NULL`, fixture.resident["B"], semanticTime, semanticTZ)

	// Seed one valid attempt so the generation repository participates in the
	// golden, in addition to the resident/revision/claim state tables.
	mustExec(t, fixture.db, `INSERT INTO generation_run_inputs(
		generation_run_input_id, canonical_commit_id, generation_run_id, ordinal,
		role, source_type, source_id, inclusion_mode, content_id, recorded_at, recorded_tz
	) VALUES (?, ?, ?, 0, 'user', 'event', ?, 'current_input', ?, ?, ?)`,
		fixture.ids.new(), fixture.commit["A"], fixture.run["A"], fixture.event["A"],
		fixture.content["A"]["input"], semanticTime, semanticTZ)
	outcomeID := fixture.ids.new()
	mustExec(t, fixture.db, `INSERT INTO generation_run_outcomes(
		outcome_id, canonical_commit_id, generation_run_id, attempt_no, state,
		output_content_id, prompt_tokens, completion_tokens, latency, estimated_cost,
		error_class, error_detail_content_id, recorded_at, recorded_tz
	) VALUES (?, ?, ?, 1, 'running', NULL, NULL, NULL, NULL, NULL, NULL, NULL, ?, ?)`,
		outcomeID, fixture.commit["A"], fixture.run["A"], semanticTime, semanticTZ)

	before, err := logicalStateSnapshot(context.Background(), fixture.db, fixture.resident["A"], fixture.run["A"])
	if err != nil {
		t.Fatal(err)
	}
	if _, err := fixture.db.Exec("VACUUM"); err != nil {
		t.Fatalf("VACUUM failed: %v", err)
	}
	after, err := logicalStateSnapshot(context.Background(), fixture.db, fixture.resident["A"], fixture.run["A"])
	if err != nil {
		t.Fatal(err)
	}
	if before != after {
		t.Fatalf("logical state changed across VACUUM\nbefore=%s\nafter=%s", before, after)
	}
}

func logicalStateSnapshot(ctx context.Context, db *sql.DB, residentID, runID string) (string, error) {
	queries := []struct {
		name  string
		query string
		args  []any
	}{
		{"resident_status", `SELECT resident_id, to_status, canonical_commit_id FROM resident_status_transitions WHERE resident_id = ? ORDER BY canonical_commit_id, resident_status_transition_id`, []any{residentID}},
		{"active_revision", `SELECT activation.resident_id, revision.revision_class, activation.revision_id, activation.canonical_commit_id FROM resident_revision_activations activation JOIN resident_revisions revision ON revision.revision_id = activation.revision_id WHERE activation.resident_id = ? ORDER BY revision.revision_class, activation.canonical_commit_id, activation.activation_id`, []any{residentID}},
		{"claim_stage", `SELECT claim_id, to_stage, canonical_commit_id FROM claim_stage_transitions WHERE claim_id IN (SELECT claim_id FROM claims WHERE owner_resident_id = ?) ORDER BY claim_id, canonical_commit_id, stage_transition_id`, []any{residentID}},
		{"claim_status", `SELECT claim_id, to_status, canonical_commit_id FROM claim_status_transitions WHERE claim_id IN (SELECT claim_id FROM claims WHERE owner_resident_id = ?) ORDER BY claim_id, canonical_commit_id, status_transition_id`, []any{residentID}},
		{"claim_validity", `SELECT claim_id, assertion_type, canonical_commit_id FROM claim_validity_assertions WHERE claim_id IN (SELECT claim_id FROM claims WHERE owner_resident_id = ?) ORDER BY claim_id, canonical_commit_id, validity_assertion_id`, []any{residentID}},
		{"claim_scope", `SELECT claim_id, view_scope, canonical_commit_id FROM claim_view_scope_assertions WHERE claim_id IN (SELECT claim_id FROM claims WHERE owner_resident_id = ?) ORDER BY claim_id, canonical_commit_id, view_scope_assertion_id`, []any{residentID}},
		{"generation_outcomes", `SELECT outcome_id, attempt_no, state, COALESCE(error_class, '') FROM generation_run_outcomes WHERE generation_run_id = ? ORDER BY attempt_no DESC, CASE WHEN state IN ('succeeded','failed','cancelled') THEN 1 ELSE 0 END DESC, outcome_id DESC`, []any{runID}},
	}
	values := make([]string, 0, len(queries)+1)
	latest, err := (generationOutcomeRepository{}).Latest(ctx, db, mustParseSnapshotID(runID), mustParseSnapshotID(residentID))
	if err != nil {
		return "", fmt.Errorf("latest generation outcome: %w", err)
	}
	values = append(values, fmt.Sprintf("latest=%s/%d/%s", latest.ID, latest.AttemptNo, latest.State))
	repository := &CanonicalRepository{store: &Store{reader: db}}
	mandatory, next, err := repository.DiscoverMandatoryRecoveryWork(ctx, nil, 256)
	if err != nil {
		return "", fmt.Errorf("mandatory recovery discovery: %w", err)
	}
	mandatoryState := make([]string, 0, len(mandatory))
	for _, work := range mandatory {
		run := ""
		if work.RunID != nil {
			run = work.RunID.String()
		}
		mandatoryState = append(mandatoryState, fmt.Sprintf("%s/%s/%s/%d/%s",
			work.Kind, work.IdempotencyKey, run, work.AttemptNo, work.State))
	}
	values = append(values, fmt.Sprintf("mandatory=%v/next=%v", mandatoryState, next))
	for _, item := range queries {
		rows, err := db.QueryContext(ctx, item.query, item.args...)
		if err != nil {
			return "", fmt.Errorf("snapshot %s: %w", item.name, err)
		}
		rowValues, err := scanSnapshotRows(rows)
		_ = rows.Close()
		if err != nil {
			return "", fmt.Errorf("snapshot %s: %w", item.name, err)
		}
		encoded, err := json.Marshal(rowValues)
		if err != nil {
			return "", err
		}
		values = append(values, item.name+"="+string(encoded))
	}
	return string(mustJSON(values)), nil
}

func scanSnapshotRows(rows *sql.Rows) ([][]string, error) {
	columns, err := rows.Columns()
	if err != nil {
		return nil, err
	}
	result := make([][]string, 0)
	for rows.Next() {
		values := make([]any, len(columns))
		pointers := make([]any, len(columns))
		for index := range values {
			pointers[index] = &values[index]
		}
		if err := rows.Scan(pointers...); err != nil {
			return nil, err
		}
		row := make([]string, len(values))
		for index, value := range values {
			row[index] = fmt.Sprintf("%v", value)
		}
		result = append(result, row)
	}
	return result, rows.Err()
}

func mustJSON(value any) []byte {
	encoded, err := json.Marshal(value)
	if err != nil {
		panic(err)
	}
	return encoded
}

func mustParseSnapshotID(raw string) canonical.ID {
	id, err := canonical.ParseID(raw)
	if err != nil {
		panic(err)
	}
	return id
}
