package sqlite

import (
	"context"
	"database/sql"
	"testing"

	"mahoroba.local/mahoroba/internal/canonical"
	"mahoroba.local/mahoroba/internal/domain"
	"mahoroba.local/mahoroba/internal/generation"
	"mahoroba.local/mahoroba/internal/memory"
)

type alignmentMutationFixture struct {
	*memoryClaimMutationFixture
	alignmentPipeline canonical.ID
	activeCommit      canonical.ID
	directID          canonical.ID
	metaID            canonical.ID
	directEvidence    []canonical.ID
	metaEvidence      []canonical.ID
	runID             canonical.ID
	replacementOldID  canonical.ID
	intentRelationID  canonical.ID
}

func newAlignmentMutationFixture(t *testing.T, qualifiedMeta bool) *alignmentMutationFixture {
	fixture := newAlignmentMutationFixtureWithoutRun(t, qualifiedMeta)
	fixture.runID = fixture.seedAlignmentRun(t)
	return fixture
}

func newAlignmentMutationFixtureWithoutRun(t *testing.T, qualifiedMeta bool) *alignmentMutationFixture {
	t.Helper()
	base := newMemoryClaimMutationFixture(t)
	fixture := &alignmentMutationFixture{memoryClaimMutationFixture: base}
	fixture.activeCommit = fixture.newID(t)
	mustExec(t, fixture.semantic.db, `INSERT INTO canonical_commits(
		canonical_commit_id, commit_seq, resident_id, committed_at, committed_tz
	) VALUES (?, 9, ?, ?, ?)`, fixture.activeCommit.String(), fixture.residentID.String(), semanticTime+9, semanticTZ)
	mustExec(t, fixture.semantic.db, `INSERT INTO resident_status_transitions(
		resident_status_transition_id, canonical_commit_id, resident_id, from_status, to_status,
		actor_principal_id, reason_code, reason_content_id, occurred_at, occurred_tz, recorded_at, recorded_tz
	) VALUES (?, ?, ?, 'draft', 'active', ?, 'test_activate', NULL, ?, ?, ?, ?)`, fixture.newID(t).String(),
		fixture.activeCommit.String(), fixture.residentID.String(), fixture.ownerPrincipal.String(),
		semanticTime+9, semanticTZ, semanticTime+9, semanticTZ)
	mustExec(t, fixture.semantic.db, `INSERT INTO resident_revision_activations(
		activation_id, canonical_commit_id, resident_id, revision_id, actor_principal_id,
		approval_id, reason_code, reason_content_id, recorded_at, recorded_tz
	) VALUES (?, ?, ?, ?, ?, NULL, 'test_persona_activation', NULL, ?, ?)`, fixture.newID(t).String(),
		fixture.activeCommit.String(), fixture.residentID.String(), fixture.semantic.revision["A"]["persona"],
		fixture.ownerPrincipal.String(), semanticTime+9, semanticTZ)
	fixture.alignmentPipeline = fixture.newID(t)
	mustExec(t, fixture.semantic.db, `INSERT INTO pipeline_versions(
		pipeline_version_id, canonical_commit_id, pipeline_kind, version_key, definition, recorded_at, recorded_tz
	) VALUES (?, ?, 'memory_alignment', ?, ?, ?, ?)`, fixture.alignmentPipeline.String(),
		fixture.semantic.commit["global"], domain.MemoryAlignmentPipelineVersion,
		`{"version":"memory-alignment-v1"}`, semanticTime, semanticTZ)

	directSeeds := make([]seededClaimEvidence, 0, 3)
	for range 3 {
		eventID := fixture.seedEvent(t, memory.EventUserMessage, fixture.ownerPrincipal, memory.TrustTrusted)
		directSeeds = append(directSeeds, trustedEvidence(eventID, memory.EventUserMessage,
			fixture.ownerPrincipal, memory.PolaritySupport))
	}
	fixture.directID, fixture.directEvidence = fixture.seedClaim(t, memory.ClaimKindDirect,
		fixture.residentPrincipal, fixture.residentPrincipal, directSeeds, memory.StageSediment)
	metaPerspective := fixture.ownerPrincipal
	if !qualifiedMeta {
		metaPerspective = fixture.residentPrincipal
	}
	metaSeeds := make([]seededClaimEvidence, 0, 2)
	for range 2 {
		eventID := fixture.seedEvent(t, memory.EventUserMessage, fixture.ownerPrincipal, memory.TrustTrusted)
		metaSeeds = append(metaSeeds, trustedEvidence(eventID, memory.EventUserMessage,
			fixture.ownerPrincipal, memory.PolaritySupport))
	}
	fixture.metaID, fixture.metaEvidence = fixture.seedClaim(t, memory.ClaimKindMeta,
		fixture.residentPrincipal, metaPerspective, metaSeeds, memory.StageSediment)
	return fixture
}

func newAlignmentReplacementMutationFixture(t *testing.T, qualifiedMeta bool) *alignmentMutationFixture {
	t.Helper()
	fixture := newAlignmentMutationFixtureWithoutRun(t, qualifiedMeta)
	fixture.replacementOldID = fixture.seedSettledDirectClaim(t)
	fixture.intentRelationID = fixture.newID(t)
	mustExec(t, fixture.semantic.db, `INSERT INTO claim_relations(
		claim_relation_id, canonical_commit_id, from_claim_id, to_claim_id,
		relation_type, reason_code, reason_content_id, generation_run_id,
		occurred_at, occurred_tz, recorded_at, recorded_tz
	) VALUES (?, ?, ?, ?, 'contradicts', 'replacement_intent', NULL, NULL, ?, ?, ?, ?)`,
		fixture.intentRelationID.String(), fixture.activeCommit.String(), fixture.directID.String(),
		fixture.replacementOldID.String(), semanticTime+9, semanticTZ, semanticTime+9, semanticTZ)
	fixture.runID = fixture.seedAlignmentRun(t)
	return fixture
}

func (fixture *alignmentMutationFixture) seedAlignmentRun(t *testing.T) canonical.ID {
	t.Helper()
	runID := fixture.newID(t)
	schema, err := memory.AlignmentJSONSchema()
	if err != nil {
		t.Fatal(err)
	}
	contract, err := generation.NewSchemaContract(
		"memory_alignment", memory.AlignmentOutputVersionV1, generation.StructuredOutputPrompt, schema,
	)
	if err != nil {
		t.Fatal(err)
	}
	_, params, err := domain.NewStructuredGeneratorParams(false, canonical.ByteSize(65536),
		contract.Mode, contract.Version, contract.Hash)
	if err != nil {
		t.Fatal(err)
	}
	key := domain.MemoryAlignmentObligation(fixture.directID, fixture.metaID,
		fixture.directEvidence[len(fixture.directEvidence)-1], fixture.metaEvidence[len(fixture.metaEvidence)-1])
	if !fixture.replacementOldID.IsZero() {
		key = domain.MemoryAlignmentReplacementObligation(fixture.directID, fixture.metaID,
			fixture.directEvidence[len(fixture.directEvidence)-1], fixture.metaEvidence[len(fixture.metaEvidence)-1],
			fixture.replacementOldID, fixture.intentRelationID)
	}
	f := fixture.semantic
	mustExec(t, f.db, `INSERT INTO generation_runs VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`,
		runID.String(), fixture.activeCommit.String(), fixture.residentID.String(), "memory_alignment", key,
		"test", "model", nil, "memory_alignment-prompt-v1", fixture.alignmentPipeline.String(),
		"memory_alignment-context-v1", nil, "memory-rendering-v1", f.revision["A"]["principles"],
		f.revision["A"]["persona"], fixture.policyID.String(), nil, nil, nil, nil, nil,
		params.String(),
		semanticTime+9, semanticTZ, 0, `{"memory_recall":"not_applicable"}`, semanticTime+9, semanticTZ)

	sourceIDs := []string{f.revision["A"]["persona"], fixture.policyID.String(), fixture.directID.String(), fixture.metaID.String()}
	sourceTypes := []string{"resident_revision", "resident_revision", "claim", "claim"}
	inclusions := []string{"resident_definition", "resident_definition", "memory_recall", "memory_recall"}
	for index, sourceID := range sourceIDs {
		var sourceContent []byte
		if sourceTypes[index] == "resident_revision" {
			if err := f.db.QueryRow(`SELECT blob.content FROM resident_revisions revision
				JOIN content_objects content ON content.content_id = revision.content_id
				JOIN blobs blob ON blob.dedupe_scope_id = content.owner_resident_id
				 AND blob.hash_algorithm = content.blob_hash_algorithm AND blob.blob_hash = content.blob_hash
				WHERE revision.revision_id = ?`, sourceID).Scan(&sourceContent); err != nil {
				t.Fatal(err)
			}
		} else if err := f.db.QueryRow(`SELECT blob.content FROM claims claim
			JOIN content_objects content ON content.content_id = claim.statement_content_id
			JOIN blobs blob ON blob.dedupe_scope_id = content.owner_resident_id
			 AND blob.hash_algorithm = content.blob_hash_algorithm AND blob.blob_hash = content.blob_hash
			WHERE claim.claim_id = ?`, sourceID).Scan(&sourceContent); err != nil {
			t.Fatal(err)
		}
		contentID := f.addContent(t, "A", "generation_input", string(sourceContent), "independent")
		role := "system"
		if index >= 2 {
			role = "user"
		}
		mustExec(t, f.db, `INSERT INTO generation_run_inputs VALUES (?,?,?,?,?,?,?,?,?,?,?)`,
			fixture.newID(t).String(), fixture.activeCommit.String(), runID.String(), index, role,
			sourceTypes[index], sourceID, inclusions[index], contentID, semanticTime+9, semanticTZ)
	}
	mustExec(t, f.db, `INSERT INTO generation_run_outcomes(
		outcome_id, canonical_commit_id, generation_run_id, attempt_no, state,
		output_content_id, prompt_tokens, completion_tokens, latency, estimated_cost,
		error_class, error_detail_content_id, recorded_at, recorded_tz
	) VALUES (?, ?, ?, 1, 'running', NULL, NULL, NULL, NULL, NULL, NULL, NULL, ?, ?)`,
		fixture.newID(t).String(), fixture.activeCommit.String(), runID.String(), semanticTime+9, semanticTZ)
	return runID
}

func (fixture *alignmentMutationFixture) landing(t *testing.T, aligned bool, confidence string) domain.LandMemoryAlignment {
	t.Helper()
	logical := []byte(`{"aligned":` + map[bool]string{true: "true", false: "false"}[aligned] +
		`,"confidence":"` + confidence + `","version":"memory-alignment-output-v1"}`)
	return domain.LandMemoryAlignment{
		Attempt:       domain.Attempt{RunID: fixture.runID, ResidentID: fixture.residentID, AttemptNo: 1, OutcomeID: fixture.newID(t)},
		DirectClaimID: fixture.directID, MetaClaimID: fixture.metaID,
		DirectEvidenceID:            fixture.directEvidence[len(fixture.directEvidence)-1],
		MetaEvidenceID:              fixture.metaEvidence[len(fixture.metaEvidence)-1],
		AlignmentPipelineVersionID:  fixture.alignmentPipeline,
		MaturationPipelineVersionID: fixture.maturationPipeline, MemoryPolicyRevisionID: fixture.policyID,
		StageTransitionID: fixture.newID(t), StageTransitionDependencyID: fixture.newID(t),
		Output: fixture.content(t, "generation_output", logical, "independent"),
	}
}

func TestM5I40SettledRequiresExternalEvidenceOrExternallySupportedAlignment(t *testing.T) {
	fixture := newAlignmentMutationFixture(t, true)
	value := fixture.landing(t, true, "800000")
	uow, metadata := fixture.beginUoW(t)
	result, err := uow.LandMemoryAlignment(context.Background(), value)
	if err != nil {
		_ = uow.tx.Rollback()
		t.Fatal(err)
	}
	if result.StageTransitionID == nil || *result.StageTransitionID != value.StageTransitionID {
		_ = uow.tx.Rollback()
		t.Fatalf("landing result = %+v", result)
	}
	if err := uow.tx.Commit(); err != nil {
		t.Fatal(err)
	}
	var outcomeCommit, stageCommit, dependencyCommit, stage, dependency string
	if err := fixture.semantic.db.QueryRow(`SELECT outcome.canonical_commit_id,
		transition.canonical_commit_id, dependency.canonical_commit_id,
		transition.to_stage, dependency.dependency_claim_id
		FROM generation_run_outcomes outcome
		JOIN claim_stage_transitions transition ON transition.generation_run_id = outcome.generation_run_id
		JOIN claim_stage_transition_dependencies dependency ON dependency.stage_transition_id = transition.stage_transition_id
		WHERE outcome.outcome_id = ?`, value.OutcomeID.String()).Scan(
		&outcomeCommit, &stageCommit, &dependencyCommit, &stage, &dependency,
	); err != nil {
		t.Fatal(err)
	}
	if outcomeCommit != metadata.CommitID.String() || stageCommit != outcomeCommit || dependencyCommit != outcomeCommit ||
		stage != "settled" || dependency != fixture.metaID.String() {
		t.Fatalf("atomic result commits=%s/%s/%s stage=%s dependency=%s", outcomeCommit, stageCommit, dependencyCommit, stage, dependency)
	}

	second, _ := fixture.beginUoW(t)
	if _, err := second.LandMemoryAlignment(context.Background(), value); err == nil {
		_ = second.tx.Rollback()
		t.Fatal("succeeded alignment landed twice")
	}
	_ = second.tx.Rollback()
}

func TestMemoryAlignmentFalseAndLowConfidenceDoNotMutateStage(t *testing.T) {
	for _, test := range []struct {
		name       string
		aligned    bool
		confidence string
	}{{"false", false, "1000000"}, {"low", true, "799999"}} {
		t.Run(test.name, func(t *testing.T) {
			fixture := newAlignmentMutationFixture(t, true)
			value := fixture.landing(t, test.aligned, test.confidence)
			uow, _ := fixture.beginUoW(t)
			result, err := uow.LandMemoryAlignment(context.Background(), value)
			if err != nil {
				_ = uow.tx.Rollback()
				t.Fatal(err)
			}
			if result.StageTransitionID != nil {
				_ = uow.tx.Rollback()
				t.Fatalf("unexpected stage result = %+v", result)
			}
			if err := uow.tx.Commit(); err != nil {
				t.Fatal(err)
			}
			var outcomes, settled int
			if err := fixture.semantic.db.QueryRow(`SELECT
				(SELECT COUNT(*) FROM generation_run_outcomes WHERE outcome_id = ? AND state = 'succeeded'),
				(SELECT COUNT(*) FROM claim_stage_transitions WHERE claim_id = ? AND to_stage = 'settled')`,
				value.OutcomeID.String(), fixture.directID.String()).Scan(&outcomes, &settled); err != nil {
				t.Fatal(err)
			}
			if outcomes != 1 || settled != 0 {
				t.Fatalf("outcomes=%d settled=%d", outcomes, settled)
			}
		})
	}
}

func TestM5I41DirectSettledRequiresActiveExternallySupportedMeta(t *testing.T) {
	fixture := newAlignmentMutationFixture(t, false)
	value := fixture.landing(t, true, "1000000")
	uow, _ := fixture.beginUoW(t)
	if _, err := uow.LandMemoryAlignment(context.Background(), value); err == nil {
		_ = uow.tx.Rollback()
		t.Fatal("meta without perspective user-message support was accepted")
	}
	_ = uow.tx.Rollback()
	var outcomes int
	if err := fixture.semantic.db.QueryRow(`SELECT COUNT(*) FROM generation_run_outcomes WHERE outcome_id = ?`,
		value.OutcomeID.String()).Scan(&outcomes); err != nil && err != sql.ErrNoRows {
		t.Fatal(err)
	}
	if outcomes != 0 {
		t.Fatalf("failed provenance landing leaked outcome: %d", outcomes)
	}
}

func TestM5I96SettledDirectReplacementIsAtomicAndPriorSettledIsHuman(t *testing.T) {
	t.Run("negative alignment completes without applying replacement", func(t *testing.T) {
		fixture := newAlignmentReplacementMutationFixture(t, true)
		value := fixture.landing(t, false, "800000")
		value.Replacement = &domain.MemoryAlignmentReplacement{
			OldClaimID: fixture.replacementOldID, IntentRelationID: fixture.intentRelationID,
			RelationID: fixture.newID(t), OldStatusTransitionID: fixture.newID(t),
			StatusPipelineVersionID: fixture.statusPipeline,
		}
		uow, _ := fixture.beginUoW(t)
		result, err := uow.LandMemoryAlignment(context.Background(), value)
		if err != nil {
			_ = uow.tx.Rollback()
			t.Fatal(err)
		}
		if result.ReplacementApplied || result.StageTransitionID != nil {
			_ = uow.tx.Rollback()
			t.Fatalf("negative alignment applied replacement: %+v", result)
		}
		if err := uow.tx.Commit(); err != nil {
			t.Fatal(err)
		}
		for _, id := range []canonical.ID{value.Replacement.RelationID, value.Replacement.OldStatusTransitionID} {
			var count int
			if err := fixture.semantic.db.QueryRow(`SELECT
				(SELECT COUNT(*) FROM claim_relations WHERE claim_relation_id = ?) +
				(SELECT COUNT(*) FROM claim_status_transitions WHERE status_transition_id = ?)`,
				id.String(), id.String()).Scan(&count); err != nil || count != 0 {
				t.Fatalf("negative alignment leaked replacement row %s: %d/%v", id, count, err)
			}
		}
	})

	t.Run("forged replacement identity is rejected", func(t *testing.T) {
		fixture := newAlignmentReplacementMutationFixture(t, true)
		value := fixture.landing(t, true, "800000")
		value.Replacement = &domain.MemoryAlignmentReplacement{
			OldClaimID: fixture.replacementOldID, IntentRelationID: fixture.newID(t),
			RelationID: fixture.newID(t), OldStatusTransitionID: fixture.newID(t),
			StatusPipelineVersionID: fixture.statusPipeline,
		}
		uow, _ := fixture.beginUoW(t)
		if _, err := uow.LandMemoryAlignment(context.Background(), value); err == nil {
			_ = uow.tx.Rollback()
			t.Fatal("forged replacement intent was accepted")
		}
		_ = uow.tx.Rollback()
		var outcomeCount int
		if err := fixture.semantic.db.QueryRow(`SELECT COUNT(*) FROM generation_run_outcomes WHERE outcome_id = ?`,
			value.OutcomeID.String()).Scan(&outcomeCount); err != nil || outcomeCount != 0 {
			t.Fatalf("forged replacement leaked outcome: %d/%v", outcomeCount, err)
		}
	})

	t.Run("alignment stage relation and status share commit", func(t *testing.T) {
		fixture := newAlignmentReplacementMutationFixture(t, true)
		oldID := fixture.replacementOldID
		value := fixture.landing(t, true, "800000")
		value.Replacement = &domain.MemoryAlignmentReplacement{
			OldClaimID: oldID, IntentRelationID: fixture.intentRelationID, RelationID: fixture.newID(t),
			OldStatusTransitionID: fixture.newID(t), StatusPipelineVersionID: fixture.statusPipeline,
		}
		uow, metadata := fixture.beginUoW(t)
		result, err := uow.LandMemoryAlignment(context.Background(), value)
		if err != nil {
			_ = uow.tx.Rollback()
			t.Fatal(err)
		}
		if result.StageTransitionID == nil || !result.ReplacementApplied {
			_ = uow.tx.Rollback()
			t.Fatalf("replacement result = %+v", result)
		}
		if err := uow.tx.Commit(); err != nil {
			t.Fatal(err)
		}
		for name, test := range map[string]struct {
			query string
			id    canonical.ID
		}{
			"outcome":    {`SELECT canonical_commit_id FROM generation_run_outcomes WHERE outcome_id = ?`, value.OutcomeID},
			"stage":      {`SELECT canonical_commit_id FROM claim_stage_transitions WHERE stage_transition_id = ?`, value.StageTransitionID},
			"dependency": {`SELECT canonical_commit_id FROM claim_stage_transition_dependencies WHERE stage_transition_dependency_id = ?`, value.StageTransitionDependencyID},
			"relation":   {`SELECT canonical_commit_id FROM claim_relations WHERE claim_relation_id = ?`, value.Replacement.RelationID},
			"status":     {`SELECT canonical_commit_id FROM claim_status_transitions WHERE status_transition_id = ?`, value.Replacement.OldStatusTransitionID},
		} {
			var commitRaw string
			if err := fixture.semantic.db.QueryRow(test.query, test.id.String()).Scan(&commitRaw); err != nil || commitRaw != metadata.CommitID.String() {
				t.Fatalf("%s commit = %q, %v; want %s", name, commitRaw, err, metadata.CommitID)
			}
		}
		var fromClaim, toClaim, relationType, status string
		if err := fixture.semantic.db.QueryRow(`SELECT relation.from_claim_id, relation.to_claim_id,
			relation.relation_type, transition.to_status FROM claim_relations relation
			JOIN claim_status_transitions transition ON transition.trigger_claim_relation_id = relation.claim_relation_id
			WHERE relation.claim_relation_id = ?`, value.Replacement.RelationID.String()).Scan(
			&fromClaim, &toClaim, &relationType, &status,
		); err != nil {
			t.Fatal(err)
		}
		if fromClaim != fixture.directID.String() || toClaim != oldID.String() ||
			relationType != "supersedes" || status != "superseded" {
			t.Fatalf("replacement relation=%s->%s/%s status=%s", fromClaim, toClaim, relationType, status)
		}
	})

	t.Run("late status failure rolls back alignment and replacement", func(t *testing.T) {
		fixture := newAlignmentReplacementMutationFixture(t, true)
		oldID := fixture.replacementOldID
		blockerEvent := fixture.seedEvent(t, memory.EventUserMessage, fixture.ownerPrincipal, memory.TrustTrusted)
		blockerID, _ := fixture.seedClaim(t, memory.ClaimKindOther, fixture.ownerPrincipal,
			fixture.residentPrincipal, []seededClaimEvidence{trustedEvidence(blockerEvent, memory.EventUserMessage,
				fixture.ownerPrincipal, memory.PolaritySupport)}, memory.StageFloating)
		duplicateStatusID := fixture.newID(t)
		mustExec(t, fixture.semantic.db, `INSERT INTO claim_status_transitions(
			status_transition_id, canonical_commit_id, claim_id, from_status, to_status,
			decision_kind, actor_principal_id, trigger_kind, trigger_event_id,
			trigger_evidence_id, trigger_claim_relation_id, trigger_integrity_finding_id,
			pipeline_version_id, memory_policy_revision_id, gate_metrics,
			decision_reason_code, decision_reason_content_id, occurred_at, occurred_tz,
			recorded_at, recorded_tz
		) VALUES (?, ?, ?, 'active', 'invalidated', 'human', ?, NULL, NULL, NULL, NULL,
			NULL, NULL, NULL, NULL, 'human_invalidation', NULL, ?, ?, ?, ?)`,
			duplicateStatusID.String(), fixture.semantic.commit["A"], blockerID.String(),
			fixture.ownerPrincipal.String(), semanticTime, semanticTZ, semanticTime, semanticTZ)
		value := fixture.landing(t, true, "800000")
		value.Replacement = &domain.MemoryAlignmentReplacement{
			OldClaimID: oldID, IntentRelationID: fixture.intentRelationID,
			RelationID: fixture.newID(t), OldStatusTransitionID: duplicateStatusID,
			StatusPipelineVersionID: fixture.statusPipeline,
		}
		uow, _ := fixture.beginUoW(t)
		if _, err := uow.LandMemoryAlignment(context.Background(), value); err == nil {
			_ = uow.tx.Rollback()
			t.Fatal("duplicate late status identity did not fail replacement")
		}
		_ = uow.tx.Rollback()
		for name, test := range map[string]struct {
			query string
			id    canonical.ID
		}{
			"outcome":    {`SELECT COUNT(*) FROM generation_run_outcomes WHERE outcome_id = ?`, value.OutcomeID},
			"stage":      {`SELECT COUNT(*) FROM claim_stage_transitions WHERE stage_transition_id = ?`, value.StageTransitionID},
			"dependency": {`SELECT COUNT(*) FROM claim_stage_transition_dependencies WHERE stage_transition_dependency_id = ?`, value.StageTransitionDependencyID},
			"relation":   {`SELECT COUNT(*) FROM claim_relations WHERE claim_relation_id = ?`, value.Replacement.RelationID},
		} {
			var count int
			if err := fixture.semantic.db.QueryRow(test.query, test.id.String()).Scan(&count); err != nil || count != 0 {
				t.Fatalf("failed replacement leaked %s: count=%d err=%v", name, count, err)
			}
		}
	})

	t.Run("prior settled replacement requires human decision", func(t *testing.T) {
		fixture := newAlignmentReplacementMutationFixture(t, true)
		priorCommit := fixture.newID(t)
		priorCommitSeq := fixture.nextCommitSeq
		fixture.nextCommitSeq++
		mustExec(t, fixture.semantic.db, `INSERT INTO canonical_commits(
			canonical_commit_id, commit_seq, resident_id, committed_at, committed_tz
		) VALUES (?, ?, ?, ?, ?)`, priorCommit.String(), priorCommitSeq, fixture.residentID.String(), semanticTime+10, semanticTZ)
		priorTransition := fixture.newID(t)
		mustExec(t, fixture.semantic.db, `INSERT INTO claim_stage_transitions(
			stage_transition_id, canonical_commit_id, claim_id, from_stage, to_stage,
			gate_metrics, pipeline_version_id, memory_policy_revision_id, generation_run_id,
			reason_code, reason_content_id, occurred_at, occurred_tz, recorded_at, recorded_tz
		) VALUES (?, ?, ?, 'sediment', 'settled', '{}', ?, ?, NULL,
			'external_alignment', NULL, ?, ?, ?, ?)`, priorTransition.String(), priorCommit.String(),
			fixture.directID.String(), fixture.maturationPipeline.String(), fixture.policyID.String(),
			semanticTime+10, semanticTZ, semanticTime+10, semanticTZ)
		value := fixture.landing(t, true, "800000")
		value.Replacement = &domain.MemoryAlignmentReplacement{
			OldClaimID: fixture.replacementOldID, IntentRelationID: fixture.intentRelationID,
			RelationID:            fixture.newID(t),
			OldStatusTransitionID: fixture.newID(t), StatusPipelineVersionID: fixture.statusPipeline,
		}
		second, _ := fixture.beginUoW(t)
		if _, err := second.LandMemoryAlignment(context.Background(), value); err == nil {
			_ = second.tx.Rollback()
			t.Fatal("prior-settled replacement was automatically accepted")
		}
		_ = second.tx.Rollback()
		var relationCount, statusCount int
		_ = fixture.semantic.db.QueryRow(`SELECT COUNT(*) FROM claim_relations WHERE claim_relation_id = ?`,
			value.Replacement.RelationID.String()).Scan(&relationCount)
		_ = fixture.semantic.db.QueryRow(`SELECT COUNT(*) FROM claim_status_transitions WHERE status_transition_id = ?`,
			value.Replacement.OldStatusTransitionID.String()).Scan(&statusCount)
		if relationCount != 0 || statusCount != 0 {
			t.Fatalf("failed replacement leaked relation/status: %d/%d", relationCount, statusCount)
		}
	})
}

func TestMemoryAlignmentNewEvidenceInvalidatesOldKey(t *testing.T) {
	fixture := newAlignmentMutationFixture(t, true)
	oldKey := domain.MemoryAlignmentObligation(fixture.directID, fixture.metaID,
		fixture.directEvidence[len(fixture.directEvidence)-1], fixture.metaEvidence[len(fixture.metaEvidence)-1])
	eventID := fixture.seedEvent(t, memory.EventUserMessage, fixture.ownerPrincipal, memory.TrustTrusted)
	newEvidence := fixture.newID(t)
	mustExec(t, fixture.semantic.db, `INSERT INTO claim_evidence(
		evidence_id, canonical_commit_id, claim_id, event_id, polarity, grade, trust_level, weight,
		derivation, source_evidence_id, memory_policy_revision_id, created_by_run_id,
		reason_code, reason_content_id, recorded_at, recorded_tz
	) VALUES (?, ?, ?, ?, 'support', 'stated', 'trusted', 1000000, 'extracted', NULL, ?, ?,
		'source_stated', NULL, ?, ?)`, newEvidence.String(), fixture.activeCommit.String(), fixture.directID.String(),
		eventID.String(), fixture.policyID.String(), fixture.semantic.run["A"], semanticTime+9, semanticTZ)
	newKey := domain.MemoryAlignmentObligation(fixture.directID, fixture.metaID,
		newEvidence, fixture.metaEvidence[len(fixture.metaEvidence)-1])
	if oldKey == newKey {
		t.Fatal("new evidence did not change obligation key")
	}
	value := fixture.landing(t, true, "1000000")
	uow, _ := fixture.beginUoW(t)
	if _, err := uow.LandMemoryAlignment(context.Background(), value); err == nil {
		_ = uow.tx.Rollback()
		t.Fatal("old evidence-bound run landed after new evidence")
	}
	_ = uow.tx.Rollback()
}
