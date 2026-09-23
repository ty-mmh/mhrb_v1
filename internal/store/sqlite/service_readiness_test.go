package sqlite

import (
	"context"
	"database/sql"
	"reflect"
	"testing"

	"mahoroba.local/mahoroba/internal/canonical"
	"mahoroba.local/mahoroba/internal/domain"
	"mahoroba.local/mahoroba/internal/integrity"
	"mahoroba.local/mahoroba/internal/memory"
	"mahoroba.local/mahoroba/internal/readiness"
)

func TestM7SQLiteServiceReadinessCapturesRichHeadSelectionStatusAndPolicy(t *testing.T) {
	fixture, closeFixture := newSemanticFixture(t)
	defer closeFixture()
	residentID, policyID, headID := configureReadyResident(t, fixture)

	snapshot, err := (&serviceReadinessSource{reader: fixture.db}).CaptureServiceReadiness(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if !snapshot.CapturedHead.Exists || snapshot.CapturedHead.CommitID.String() != headID ||
		snapshot.CapturedHead.CommitSeq.Int64() != 5 ||
		snapshot.CapturedHead.CommittedAt.UnixMicro() != semanticTime+5 ||
		snapshot.CapturedHead.CommittedTZ.String() != semanticTZ {
		t.Fatalf("captured rich head = %+v", snapshot.CapturedHead)
	}
	if snapshot.ActiveResidentID == nil || *snapshot.ActiveResidentID != residentID ||
		snapshot.ActiveResidentStatus != "active" || snapshot.SessionPolicyID == nil ||
		*snapshot.SessionPolicyID != policyID {
		t.Fatalf("captured runtime state = %+v", snapshot)
	}
	if len(snapshot.BlockingPredicates) != 0 {
		t.Fatalf("clean fixture blocking predicates = %+v", snapshot.BlockingPredicates)
	}
	result, err := readiness.EvaluateServiceReadiness(context.Background(), &serviceReadinessSource{reader: fixture.db}, readiness.Request{
		StartupComplete: true,
		Projection: readiness.ProjectionCheckFunc(func(context.Context, readiness.ProjectionRequirement) (bool, error) {
			return true, nil
		}),
	})
	if err != nil || !result.Ready {
		t.Fatalf("ready evaluation = %+v / %v", result, err)
	}
}

func TestM7SQLiteServiceReadinessSessionResolutionIsExactAndFailClosed(t *testing.T) {
	t.Run("runtime config has priority", func(t *testing.T) {
		fixture, closeFixture := newSemanticFixture(t)
		defer closeFixture()
		residentID, configuredID, _ := configureReadyResident(t, fixture)
		otherID := insertOtherReadinessSessionPolicy(t, fixture)
		insertActivitySessionsDependency(t, fixture, residentID, otherID, "activity_sessions")

		snapshot, err := (&serviceReadinessSource{reader: fixture.db}).CaptureServiceReadiness(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		if snapshot.SessionPolicyID == nil || *snapshot.SessionPolicyID != configuredID {
			t.Fatalf("runtime-config policy priority = %v, want %s", snapshot.SessionPolicyID, configuredID)
		}
	})

	t.Run("unique valid activity sessions dependency", func(t *testing.T) {
		fixture, closeFixture := newSemanticFixture(t)
		defer closeFixture()
		residentID, configuredID, _ := configureReadyResident(t, fixture)
		mustExec(t, fixture.db, `UPDATE runtime_config
			SET desired_sessionization_policy_version_id = NULL WHERE singleton_id = 1`)
		insertActivitySessionsDependency(t, fixture, residentID, configuredID, "activity_sessions")

		// A dependency from another Projection must never participate in the
		// readiness fallback set.
		otherID := insertOtherReadinessSessionPolicy(t, fixture)
		insertActivitySessionsDependency(t, fixture, residentID, otherID, "unrelated_projection")
		snapshot, err := (&serviceReadinessSource{reader: fixture.db}).CaptureServiceReadiness(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		if snapshot.SessionPolicyID == nil || *snapshot.SessionPolicyID != configuredID {
			t.Fatalf("activity_sessions fallback = %v, want %s", snapshot.SessionPolicyID, configuredID)
		}

		secondID := mustIntegrityID(t, fixture.ids.new())
		mustExec(t, fixture.db, `INSERT INTO sessionization_policy_versions(
			sessionization_policy_version_id, canonical_commit_id, version_key,
			definition, recorded_at, recorded_tz
		) VALUES (?, ?, 'conflicting-sessionization-v1', '{}', ?, ?)`,
			secondID.String(), fixture.commit["global"], semanticTime, semanticTZ)
		mustExec(t, fixture.db, `INSERT INTO projection_watermark_dependencies(
			projection_name, resident_id, dependency_kind, dependency_version_id
		) VALUES ('activity_sessions', ?, 'sessionization_policy', ?)`, residentID.String(), secondID.String())
		snapshot, err = (&serviceReadinessSource{reader: fixture.db}).CaptureServiceReadiness(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		if snapshot.SessionPolicyID == nil || *snapshot.SessionPolicyID != configuredID {
			t.Fatalf("unique valid dependency with an invalid sibling = %v, want %s", snapshot.SessionPolicyID, configuredID)
		}
	})

	t.Run("invalid selected policy is unresolved", func(t *testing.T) {
		fixture, closeFixture := newSemanticFixture(t)
		defer closeFixture()
		_, _, _ = configureReadyResident(t, fixture)
		invalidID := mustIntegrityID(t, fixture.ids.new())
		mustExec(t, fixture.db, `INSERT INTO sessionization_policy_versions(
			sessionization_policy_version_id, canonical_commit_id, version_key,
			definition, recorded_at, recorded_tz
		) VALUES (?, ?, 'not-sessionization-v1', '{}', ?, ?)`, invalidID.String(), fixture.commit["global"], semanticTime, semanticTZ)
		mustExec(t, fixture.db, `UPDATE runtime_config
			SET desired_sessionization_policy_version_id = ? WHERE singleton_id = 1`, invalidID.String())
		snapshot, err := (&serviceReadinessSource{reader: fixture.db}).CaptureServiceReadiness(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		if snapshot.SessionPolicyID != nil {
			t.Fatalf("invalid configured policy resolved to %s", *snapshot.SessionPolicyID)
		}
	})
}

func TestM7SQLiteServiceReadinessCurrentBlockingPredicatesAndFindingCoverage(t *testing.T) {
	for _, rule := range []readiness.BlockingRule{
		readiness.RuleActiveRequiredRevisionErased,
		readiness.RuleRunningAttemptInputErased,
		readiness.RuleCancellationEnvelopeUnresolvable,
	} {
		for _, withFinding := range []bool{false, true} {
			name := string(rule) + "/missing_finding"
			if withFinding {
				name = string(rule) + "/existing_finding"
			}
			t.Run(name, func(t *testing.T) {
				fixture, closeFixture := newSemanticFixture(t)
				defer closeFixture()
				configureReadyResident(t, fixture)
				candidate := createReadinessPredicate(t, fixture, rule)
				if withFinding {
					insertReadinessFinding(t, fixture, candidate)
				}

				snapshot, err := (&serviceReadinessSource{reader: fixture.db}).CaptureServiceReadiness(context.Background())
				if err != nil {
					t.Fatal(err)
				}
				wantRecorded := uint64(0)
				if withFinding {
					wantRecorded = 1
				}
				want := []readiness.PredicateSummary{{Rule: rule, CurrentCount: 1, RecordedCount: wantRecorded}}
				if !reflect.DeepEqual(snapshot.BlockingPredicates, want) {
					t.Fatalf("blocking predicates = %+v, want %+v", snapshot.BlockingPredicates, want)
				}
				result, err := readiness.EvaluateServiceReadiness(context.Background(), &serviceReadinessSource{reader: fixture.db}, readiness.Request{
					StartupComplete: true,
					Projection: readiness.ProjectionCheckFunc(func(context.Context, readiness.ProjectionRequirement) (bool, error) {
						return true, nil
					}),
				})
				if err != nil {
					t.Fatal(err)
				}
				if result.Ready || !reflect.DeepEqual(result.ReasonCodes, []readiness.ReasonCode{readiness.ReasonIntegrityBlocked}) ||
					result.IntegrityScanIncomplete != !withFinding {
					t.Fatalf("blocking evaluation = %+v", result)
				}
			})
		}
	}
}

func TestM7SQLiteServiceReadinessResolvedPredicateIgnoresHistoricalFinding(t *testing.T) {
	fixture, closeFixture := newSemanticFixture(t)
	defer closeFixture()
	configureReadyResident(t, fixture)
	candidate := createReadinessPredicate(t, fixture, readiness.RuleRunningAttemptInputErased)
	insertReadinessFinding(t, fixture, candidate)

	before, err := (&serviceReadinessSource{reader: fixture.db}).CaptureServiceReadiness(context.Background())
	if err != nil || len(before.BlockingPredicates) != 1 {
		t.Fatalf("initial blocking snapshot = %+v / %v", before, err)
	}
	var runID string
	if err := fixture.db.QueryRow(`SELECT input.generation_run_id
		FROM generation_run_inputs input WHERE input.generation_run_input_id = ?`, candidate.TargetID.String()).Scan(&runID); err != nil {
		t.Fatal(err)
	}
	mustExec(t, fixture.db, `INSERT INTO generation_run_outcomes(
		outcome_id, canonical_commit_id, generation_run_id, attempt_no, state,
		output_content_id, prompt_tokens, completion_tokens, latency, estimated_cost,
		error_class, error_detail_content_id, recorded_at, recorded_tz
	) VALUES (?, ?, ?, 1, 'cancelled', NULL, NULL, NULL, NULL, NULL,
		'source_content_erased', NULL, ?, ?)`, fixture.ids.new(), fixture.commit["A"], runID, semanticTime, semanticTZ)

	after, err := (&serviceReadinessSource{reader: fixture.db}).CaptureServiceReadiness(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(after.BlockingPredicates) != 0 {
		t.Fatalf("terminalized current predicate remained blocking: %+v", after.BlockingPredicates)
	}
	var historical int
	if err := fixture.db.QueryRow(`SELECT COUNT(*) FROM integrity_findings
		WHERE finding_fingerprint = ?`, candidate.Fingerprint.Bytes()).Scan(&historical); err != nil {
		t.Fatal(err)
	}
	if historical != 1 {
		t.Fatalf("historical finding count = %d, want preserved row", historical)
	}
	result, err := readiness.EvaluateServiceReadiness(context.Background(), &serviceReadinessSource{reader: fixture.db}, readiness.Request{
		StartupComplete: true,
		Projection: readiness.ProjectionCheckFunc(func(context.Context, readiness.ProjectionRequirement) (bool, error) {
			return true, nil
		}),
	})
	if err != nil || !result.Ready {
		t.Fatalf("resolved historical finding readiness = %+v / %v", result, err)
	}
}

func TestM7SQLiteServiceReadinessCapturedHeadIsOneReadSnapshot(t *testing.T) {
	fixture, closeFixture := newSemanticFixture(t)
	defer closeFixture()
	residentA, _, headID := configureReadyResident(t, fixture)

	var sequence int
	var name, databasePath string
	if err := fixture.db.QueryRow(`PRAGMA database_list`).Scan(&sequence, &name, &databasePath); err != nil {
		t.Fatal(err)
	}
	reader, err := openReader(context.Background(), databasePath, DefaultOptions())
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()
	newHead := fixture.ids.new()
	source := &serviceReadinessSource{reader: reader}
	source.afterHead = func() {
		mustExec(t, fixture.db, `INSERT INTO canonical_commits(
			canonical_commit_id, commit_seq, resident_id, committed_at, committed_tz
		) VALUES (?, 6, ?, ?, ?)`, newHead, fixture.resident["B"], semanticTime+6, semanticTZ)
		mustExec(t, fixture.db, `UPDATE runtime_config SET active_resident_id = ?
			WHERE singleton_id = 1`, fixture.resident["B"])
	}
	snapshot, err := source.CaptureServiceReadiness(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if snapshot.CapturedHead.CommitID.String() != headID || snapshot.CapturedHead.CommitSeq.Int64() != 5 ||
		snapshot.ActiveResidentID == nil || *snapshot.ActiveResidentID != residentA {
		t.Fatalf("mixed pre/post-write readiness snapshot = %+v", snapshot)
	}
	var actualHead, actualResident string
	if err := fixture.db.QueryRow(`SELECT canonical_commit_id FROM canonical_commits
		ORDER BY commit_seq DESC LIMIT 1`).Scan(&actualHead); err != nil {
		t.Fatal(err)
	}
	if err := fixture.db.QueryRow(`SELECT active_resident_id FROM runtime_config WHERE singleton_id = 1`).Scan(&actualResident); err != nil {
		t.Fatal(err)
	}
	if actualHead != newHead || actualResident != fixture.resident["B"] {
		t.Fatalf("concurrent write did not commit: head=%s resident=%s", actualHead, actualResident)
	}
}

func configureReadyResident(t *testing.T, fixture *semanticFixture) (canonical.ID, canonical.ID, string) {
	t.Helper()
	residentID := mustIntegrityID(t, fixture.resident["A"])
	policyID := insertReadinessSessionPolicy(t, fixture)
	draftCommit, activeCommit := fixture.ids.new(), fixture.ids.new()
	mustExec(t, fixture.db, `INSERT INTO canonical_commits VALUES (?, 4, ?, ?, ?)`,
		draftCommit, residentID.String(), semanticTime+4, semanticTZ)
	mustExec(t, fixture.db, `INSERT INTO canonical_commits VALUES (?, 5, ?, ?, ?)`,
		activeCommit, residentID.String(), semanticTime+5, semanticTZ)
	mustExec(t, fixture.db, `INSERT INTO resident_status_transitions(
		resident_status_transition_id, canonical_commit_id, resident_id, from_status,
		to_status, actor_principal_id, reason_code, reason_content_id,
		occurred_at, occurred_tz, recorded_at, recorded_tz
	) VALUES (?, ?, ?, NULL, 'draft', ?, 'create', NULL, ?, ?, ?, ?)`,
		fixture.ids.new(), draftCommit, residentID.String(), fixture.principal["human"],
		semanticTime+4, semanticTZ, semanticTime+4, semanticTZ)
	mustExec(t, fixture.db, `INSERT INTO resident_status_transitions(
		resident_status_transition_id, canonical_commit_id, resident_id, from_status,
		to_status, actor_principal_id, reason_code, reason_content_id,
		occurred_at, occurred_tz, recorded_at, recorded_tz
	) VALUES (?, ?, ?, 'draft', 'active', ?, 'activate', NULL, ?, ?, ?, ?)`,
		fixture.ids.new(), activeCommit, residentID.String(), fixture.principal["human"],
		semanticTime+5, semanticTZ, semanticTime+5, semanticTZ)
	mustExec(t, fixture.db, `INSERT INTO runtime_config(
		singleton_id, active_resident_id, desired_sessionization_policy_version_id,
		updated_at, updated_tz
	) VALUES (1, ?, ?, ?, ?)`, residentID.String(), policyID.String(), semanticTime+5, semanticTZ)
	activateReadinessPolicyV4(t, fixture, residentID, activeCommit)
	return residentID, policyID, activeCommit
}

// The semantic fixture intentionally retains its V2 policy rows for frozen
// history tests. Readiness tests model a post-cutover serving resident, so
// seed an independently activated V4 revision rather than weakening the V2
// service gate in production.
func activateReadinessPolicyV4(t *testing.T, fixture *semanticFixture, residentID canonical.ID, commitID string) {
	t.Helper()
	encoded, err := memory.DefaultPolicyV4().CanonicalJSON()
	if err != nil {
		t.Fatal(err)
	}
	contentID := fixture.addContent(t, "A", "memory_policy_text", encoded.String(), "resident_only")
	revisionID := fixture.ids.new()
	mustExec(t, fixture.db, `INSERT INTO resident_revisions(
		revision_id, canonical_commit_id, resident_id, revision_class, content_id,
		parent_revision_id, created_by_run_id, reason_content_id, recorded_at, recorded_tz
	) VALUES (?, ?, ?, 'memory_policy', ?, ?, NULL, NULL, ?, ?)`,
		revisionID, commitID, residentID.String(), contentID, fixture.revision["A"]["memory_policy"], semanticTime+5, semanticTZ)
	mustExec(t, fixture.db, `INSERT INTO resident_revision_activations(
		activation_id, canonical_commit_id, resident_id, revision_id, actor_principal_id,
		approval_id, reason_code, reason_content_id, recorded_at, recorded_tz
	) VALUES (?, ?, ?, ?, ?, NULL, 'admin_memory_policy', NULL, ?, ?)`,
		fixture.ids.new(), commitID, residentID.String(), revisionID, fixture.principal["human"], semanticTime+5, semanticTZ)
	fixture.revision["A"]["memory_policy"] = revisionID
}

func insertReadinessSessionPolicy(t *testing.T, fixture *semanticFixture) canonical.ID {
	t.Helper()
	policyID := mustIntegrityID(t, fixture.ids.new())
	mustExec(t, fixture.db, `INSERT INTO sessionization_policy_versions(
		sessionization_policy_version_id, canonical_commit_id, version_key,
		definition, recorded_at, recorded_tz
	) VALUES (?, ?, ?, ?, ?, ?)`, policyID.String(), fixture.commit["global"], domain.SessionPolicyVersion,
		`{"idle_gap_microseconds":"1800000000","version":"sessionization-v1"}`, semanticTime, semanticTZ)
	return policyID
}

func insertOtherReadinessSessionPolicy(t *testing.T, fixture *semanticFixture) canonical.ID {
	t.Helper()
	policyID := mustIntegrityID(t, fixture.ids.new())
	mustExec(t, fixture.db, `INSERT INTO sessionization_policy_versions(
		sessionization_policy_version_id, canonical_commit_id, version_key,
		definition, recorded_at, recorded_tz
	) VALUES (?, ?, 'other-sessionization-v1', '{}', ?, ?)`,
		policyID.String(), fixture.commit["global"], semanticTime, semanticTZ)
	return policyID
}

func insertActivitySessionsDependency(
	t *testing.T,
	fixture *semanticFixture,
	residentID, policyID canonical.ID,
	projectionName string,
) {
	t.Helper()
	mustExec(t, fixture.db, `INSERT INTO projection_watermarks(
		projection_name, resident_id, projection_version, source_commit_seq, as_of, as_of_tz
	) VALUES (?, ?, 'readiness-test-v1', 5, ?, ?)`, projectionName, residentID.String(), semanticTime+5, semanticTZ)
	mustExec(t, fixture.db, `INSERT INTO projection_watermark_dependencies(
		projection_name, resident_id, dependency_kind, dependency_version_id
	) VALUES (?, ?, 'sessionization_policy', ?)`, projectionName, residentID.String(), policyID.String())
}

func createReadinessPredicate(
	t *testing.T,
	fixture *semanticFixture,
	rule readiness.BlockingRule,
) integrity.Candidate {
	t.Helper()
	residentID := mustIntegrityID(t, fixture.resident["A"])
	commitID := fixtureCommitAtSeq(t, fixture.db, 5)
	timezone := canonical.MustTimezone(semanticTZ)
	var input integrity.CandidateInput
	switch rule {
	case readiness.RuleActiveRequiredRevisionErased:
		revisionID := mustIntegrityID(t, fixture.revision["A"]["persona"])
		mustExec(t, fixture.db, `INSERT INTO resident_revision_activations(
			activation_id, canonical_commit_id, resident_id, revision_id, actor_principal_id,
			approval_id, reason_code, reason_content_id, recorded_at, recorded_tz
		) VALUES (?, ?, ?, ?, ?, NULL, 'activate', NULL, ?, ?)`, fixture.ids.new(), commitID,
			residentID.String(), revisionID.String(), fixture.principal["human"], semanticTime+5, semanticTZ)
		erasureID := eraseFixtureContent(t, fixture, fixture.content["A"]["persona"], commitID)
		input = integrity.CandidateInput{
			ResidentID: residentID, Kind: integrity.FindingCanonicalInvariant,
			RuleCode:   integrity.RuleActiveRequiredRevisionErased,
			TargetKind: integrity.TargetResidentRevision, TargetID: revisionID, TargetField: "content_id",
			SourceContentErasureEventID: &erasureID,
			OccurredAt:                  canonical.Instant(semanticTime + 5), OccurredTZ: timezone,
		}
	case readiness.RuleRunningAttemptInputErased:
		inputID := mustIntegrityID(t, fixture.ids.new())
		mustExec(t, fixture.db, `INSERT INTO generation_run_inputs(
			generation_run_input_id, canonical_commit_id, generation_run_id, ordinal, role,
			source_type, source_id, inclusion_mode, content_id, recorded_at, recorded_tz
		) VALUES (?, ?, ?, 0, 'system', 'runtime_projection', NULL,
			'runtime_projection', ?, ?, ?)`, inputID.String(), commitID, fixture.run["A"],
			fixture.content["A"]["input"], semanticTime+5, semanticTZ)
		mustExec(t, fixture.db, `INSERT INTO generation_run_outcomes(
			outcome_id, canonical_commit_id, generation_run_id, attempt_no, state,
			output_content_id, prompt_tokens, completion_tokens, latency, estimated_cost,
			error_class, error_detail_content_id, recorded_at, recorded_tz
		) VALUES (?, ?, ?, 1, 'running', NULL, NULL, NULL, NULL, NULL,
			NULL, NULL, ?, ?)`, fixture.ids.new(), commitID, fixture.run["A"], semanticTime+5, semanticTZ)
		erasureID := eraseFixtureContent(t, fixture, fixture.content["A"]["input"], commitID)
		input = integrity.CandidateInput{
			ResidentID: residentID, Kind: integrity.FindingCanonicalInvariant,
			RuleCode:   integrity.RuleRunningAttemptInputErased,
			TargetKind: integrity.TargetGenerationInput, TargetID: inputID, TargetField: "content_id",
			SourceContentErasureEventID: &erasureID,
			OccurredAt:                  canonical.Instant(semanticTime + 5), OccurredTZ: timezone,
		}
	case readiness.RuleCancellationEnvelopeUnresolvable:
		eventID := mustIntegrityID(t, fixture.event["A"])
		mustExec(t, fixture.db, `UPDATE content_objects
			SET blob_hash = NULL, commitment_salt = NULL, erasure_state = 'erased'
			WHERE content_id = ?`, fixture.content["A"]["event"])
		input = integrity.CandidateInput{
			ResidentID: residentID, Kind: integrity.FindingProvenanceUnresolvable,
			RuleCode:   integrity.RuleCancellationEnvelopeUnresolvable,
			TargetKind: integrity.TargetEvent, TargetID: eventID, TargetField: "cancellation_envelope",
			OccurredAt: canonical.Instant(semanticTime), OccurredTZ: timezone,
		}
	default:
		t.Fatalf("unsupported readiness test rule %q", rule)
	}
	candidate, err := integrity.NewCandidate(input)
	if err != nil {
		t.Fatal(err)
	}
	return candidate
}

func eraseFixtureContent(t *testing.T, fixture *semanticFixture, contentID, commitID string) canonical.ID {
	t.Helper()
	erasureID := mustIntegrityID(t, fixture.ids.new())
	mustExec(t, fixture.db, `INSERT INTO content_erasure_events(
		content_erasure_event_id, canonical_commit_id, content_id, erasure_scope,
		actor_principal_id, reason_code, reason_content_id, source_erasure_event_id,
		occurred_at, occurred_tz, recorded_at, recorded_tz
	) VALUES (?, ?, ?, 'content', ?, 'readiness-test', NULL, NULL, ?, ?, ?, ?)`,
		erasureID.String(), commitID, contentID, fixture.principal["human"],
		semanticTime+5, semanticTZ, semanticTime+5, semanticTZ)
	mustExec(t, fixture.db, `UPDATE content_objects
		SET blob_hash = NULL, commitment_salt = NULL, erasure_state = 'erased'
		WHERE content_id = ?`, contentID)
	return erasureID
}

func insertReadinessFinding(t *testing.T, fixture *semanticFixture, candidate integrity.Candidate) {
	t.Helper()
	pipelineID := mustIntegrityID(t, fixture.ids.new())
	insertExactIntegrityPipeline(t, fixture.db, pipelineID)
	var claimID, sourceID any
	if candidate.ClaimID != nil {
		claimID = candidate.ClaimID.String()
	}
	if candidate.SourceContentErasureEventID != nil {
		sourceID = candidate.SourceContentErasureEventID.String()
	}
	mustExec(t, fixture.db, `INSERT INTO integrity_findings(
		integrity_finding_id, canonical_commit_id, resident_id, claim_id,
		finding_kind, source_content_erasure_event_id, pipeline_version_id,
		details_content_id, occurred_at, occurred_tz, recorded_at, recorded_tz,
		finding_fingerprint, rule_code, target_kind, target_id, target_field
	) VALUES (?, ?, ?, ?, ?, ?, ?, NULL, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		fixture.ids.new(), fixtureCommitAtSeq(t, fixture.db, 5), candidate.ResidentID.String(), claimID,
		string(candidate.Kind), sourceID, pipelineID.String(), candidate.OccurredAt.UnixMicro(),
		candidate.OccurredTZ.String(), semanticTime+5, semanticTZ, candidate.Fingerprint.Bytes(),
		string(candidate.RuleCode), string(candidate.TargetKind), candidate.TargetID.String(), candidate.TargetField)
}

func fixtureCommitAtSeq(t *testing.T, db *sql.DB, seq int64) string {
	t.Helper()
	var id string
	if err := db.QueryRow(`SELECT canonical_commit_id FROM canonical_commits WHERE commit_seq = ?`, seq).Scan(&id); err != nil {
		t.Fatal(err)
	}
	return id
}
