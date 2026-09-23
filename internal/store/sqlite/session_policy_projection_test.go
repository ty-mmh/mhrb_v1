package sqlite

import (
	"context"
	"errors"
	"testing"

	"mahoroba.local/mahoroba/internal/domain"
)

func TestSessionPolicyResolverUsesRuntimeConfigThenWatermarkOrFailsClosed(t *testing.T) {
	fixture, closeFixture := newSemanticFixture(t)
	defer closeFixture()
	ctx := context.Background()
	residentID := projectionTestID(t, fixture.resident["A"])
	policyID := fixture.ids.new()
	mustExec(t, fixture.db, `INSERT INTO sessionization_policy_versions(
		sessionization_policy_version_id, canonical_commit_id, version_key, definition, recorded_at, recorded_tz
	) VALUES (?, ?, ?, ?, ?, ?)`, policyID, fixture.commit["global"], domain.SessionPolicyVersion,
		`{"idle_gap_microseconds":"1800000000","version":"sessionization-v1"}`, semanticTime, semanticTZ)

	invalidSavedPolicy := fixture.ids.new()
	mustExec(t, fixture.db, `INSERT INTO runtime_config(singleton_id, active_resident_id, desired_sessionization_policy_version_id, updated_at, updated_tz)
		VALUES (1, ?, ?, ?, ?)`, residentID.String(), policyID, semanticTime, semanticTZ)
	mustExec(t, fixture.db, `INSERT INTO projection_watermarks(projection_name, resident_id, projection_version, source_commit_seq, as_of, as_of_tz)
		VALUES ('activity_sessions', ?, 'activity-sessions-v1', 3, ?, ?)`, residentID.String(), semanticTime, semanticTZ)
	mustExec(t, fixture.db, `INSERT INTO projection_watermark_dependencies(projection_name, resident_id, dependency_kind, dependency_version_id)
		VALUES ('activity_sessions', ?, 'sessionization_policy', ?)`, residentID.String(), invalidSavedPolicy)

	resolved, gap, err := fixtureStoreRepository(fixture).ActivitySessionPolicy(ctx, residentID)
	if err != nil || resolved.String() != policyID || gap.Microseconds() != 1_800_000_000 {
		t.Fatalf("runtime_config resolution = %s / %s / %v", resolved, gap, err)
	}

	mustExec(t, fixture.db, `DELETE FROM runtime_config WHERE singleton_id = 1`)
	if _, _, err := fixtureStoreRepository(fixture).ActivitySessionPolicy(ctx, residentID); err == nil {
		t.Fatal("unresolvable saved dependency unexpectedly fell back to a default policy")
	}
	mustExec(t, fixture.db, `DELETE FROM projection_watermark_dependencies WHERE projection_name = 'activity_sessions' AND resident_id = ?`, residentID.String())
	mustExec(t, fixture.db, `INSERT INTO projection_watermark_dependencies(projection_name, resident_id, dependency_kind, dependency_version_id)
		VALUES ('activity_sessions', ?, 'sessionization_policy', ?)`, residentID.String(), policyID)
	resolved, gap, err = fixtureStoreRepository(fixture).ActivitySessionPolicy(ctx, residentID)
	if err != nil || resolved.String() != policyID || gap.Microseconds() != 1_800_000_000 {
		t.Fatalf("watermark resolution = %s / %s / %v", resolved, gap, err)
	}

	mustExec(t, fixture.db, `DELETE FROM projection_watermarks WHERE projection_name = 'activity_sessions' AND resident_id = ?`, residentID.String())
	if _, _, err := fixtureStoreRepository(fixture).ActivitySessionPolicy(ctx, residentID); !errors.Is(err, errSessionPolicyUnresolved) {
		t.Fatalf("missing policy resolution error = %v, want fail closed", err)
	}
}

func TestAtomicIngressSessionPolicyUsesWatermarkFallbackAndFailsClosed(t *testing.T) {
	fixture, closeFixture := newSemanticFixture(t)
	defer closeFixture()
	ctx := context.Background()
	residentID := projectionTestID(t, fixture.resident["A"])
	policyID := fixture.ids.new()
	mustExec(t, fixture.db, `INSERT INTO pipeline_versions(
		pipeline_version_id, canonical_commit_id, pipeline_kind, version_key, definition, recorded_at, recorded_tz
	) VALUES (?, ?, 'dialogue', ?, '{}', ?, ?)`, fixture.ids.new(), fixture.commit["global"], domain.DialoguePipelineVersionV1, semanticTime, semanticTZ)
	mustExec(t, fixture.db, `INSERT INTO sessionization_policy_versions(
		sessionization_policy_version_id, canonical_commit_id, version_key, definition, recorded_at, recorded_tz
	) VALUES (?, ?, ?, ?, ?, ?)`, policyID, fixture.commit["global"], domain.SessionPolicyVersion,
		`{"idle_gap_microseconds":"1800000000","version":"sessionization-v1"}`, semanticTime, semanticTZ)
	for _, class := range []string{"principles", "persona", "memory_policy"} {
		mustExec(t, fixture.db, `INSERT INTO resident_revision_activations(
			activation_id, canonical_commit_id, resident_id, revision_id, actor_principal_id,
			approval_id, reason_code, reason_content_id, recorded_at, recorded_tz
		) VALUES (?, ?, ?, ?, ?, NULL, 'activate', NULL, ?, ?)`, fixture.ids.new(), fixture.commit["A"],
			residentID.String(), fixture.revision["A"][class], fixture.principal["human"], semanticTime, semanticTZ)
	}
	mustExec(t, fixture.db, `INSERT INTO runtime_config(
		singleton_id, active_resident_id, desired_sessionization_policy_version_id, updated_at, updated_tz
	) VALUES (1, ?, NULL, ?, ?)`, residentID.String(), semanticTime, semanticTZ)
	mustExec(t, fixture.db, `INSERT INTO projection_watermarks(
		projection_name, resident_id, projection_version, source_commit_seq, as_of, as_of_tz
	) VALUES ('activity_sessions', ?, 'activity-sessions-v1', 2, ?, ?)`, residentID.String(), semanticTime, semanticTZ)
	mustExec(t, fixture.db, `INSERT INTO projection_watermark_dependencies(
		projection_name, resident_id, dependency_kind, dependency_version_id
	) VALUES ('activity_sessions', ?, 'sessionization_policy', ?)`, residentID.String(), policyID)

	load := func() (dialogueSnapshot, error) {
		sessionID, idleGap, err := fixtureStoreRepository(fixture).ActivitySessionPolicy(ctx, residentID)
		if err != nil {
			return dialogueSnapshot{}, err
		}
		return dialogueSnapshot{sessionID: sessionID, idleGap: idleGap}, nil
	}
	snapshot, err := load()
	if err != nil {
		t.Fatal(err)
	}
	if snapshot.sessionID.String() != policyID || snapshot.idleGap.Microseconds() != 1_800_000_000 {
		t.Fatalf("atomic snapshot policy = %s / %s", snapshot.sessionID, snapshot.idleGap)
	}

	mustExec(t, fixture.db, `DELETE FROM projection_watermarks
		WHERE projection_name = 'activity_sessions' AND resident_id = ?`, residentID.String())
	if _, err := load(); !errors.Is(err, errSessionPolicyUnresolved) {
		t.Fatalf("atomic snapshot without resolver source = %v, want fail closed", err)
	}
}

func fixtureStoreRepository(fixture *semanticFixture) *CanonicalRepository {
	return &CanonicalRepository{store: &Store{reader: fixture.db}}
}
