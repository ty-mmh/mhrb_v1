package sqlite

import (
	"context"
	"database/sql"
	"testing"

	"mahoroba.local/mahoroba/internal/canonical"
	"mahoroba.local/mahoroba/internal/domain"
	"mahoroba.local/mahoroba/internal/memory"
)

type derivedClaimMutationFixture struct {
	*memoryClaimMutationFixture
	abstractionPipeline     canonical.ID
	differentiationPipeline canonical.ID
	activeCommit            canonical.ID
}

func newDerivedClaimMutationFixture(t *testing.T) *derivedClaimMutationFixture {
	t.Helper()
	base := newMemoryClaimMutationFixture(t)
	fixture := &derivedClaimMutationFixture{memoryClaimMutationFixture: base}
	fixture.activeCommit = fixture.newID(t)
	mustExec(t, fixture.semantic.db, `INSERT INTO canonical_commits(
		canonical_commit_id, commit_seq, resident_id, committed_at, committed_tz
	) VALUES (?, 9, ?, ?, ?)`, fixture.activeCommit.String(), fixture.residentID.String(),
		semanticTime+9, semanticTZ)
	mustExec(t, fixture.semantic.db, `INSERT INTO resident_status_transitions(
		resident_status_transition_id, canonical_commit_id, resident_id, from_status, to_status,
		actor_principal_id, reason_code, reason_content_id, occurred_at, occurred_tz, recorded_at, recorded_tz
	) VALUES (?, ?, ?, 'draft', 'active', ?, 'test_activate', NULL, ?, ?, ?, ?)`,
		fixture.newID(t).String(), fixture.activeCommit.String(), fixture.residentID.String(),
		fixture.ownerPrincipal.String(), semanticTime+9, semanticTZ, semanticTime+9, semanticTZ)
	fixture.abstractionPipeline = fixture.seedDerivedPipeline(t, "memory_abstraction",
		domain.MemoryAbstractionPipelineVersion)
	fixture.differentiationPipeline = fixture.seedDerivedPipeline(t, "memory_differentiation",
		domain.MemoryDifferentiationPipelineVersion)
	return fixture
}

func (fixture *derivedClaimMutationFixture) seedDerivedPipeline(
	t *testing.T,
	kind, version string,
) canonical.ID {
	t.Helper()
	id := fixture.newID(t)
	mustExec(t, fixture.semantic.db, `INSERT INTO pipeline_versions(
		pipeline_version_id, canonical_commit_id, pipeline_kind, version_key,
		definition, recorded_at, recorded_tz
	) VALUES (?, ?, ?, ?, ?, ?, ?)`, id.String(), fixture.semantic.commit["global"],
		kind, version, `{"version":"`+version+`"}`, semanticTime, semanticTZ)
	return id
}

func (fixture *derivedClaimMutationFixture) seedDerivedRun(
	t *testing.T,
	purpose domain.GenerationPurpose,
	pipelineID canonical.ID,
) canonical.ID {
	t.Helper()
	runID := fixture.newID(t)
	f := fixture.semantic
	mustExec(t, f.db, `INSERT INTO generation_runs VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`,
		runID.String(), fixture.activeCommit.String(), fixture.residentID.String(), string(purpose),
		"derived-test-"+runID.String(), "test", "model", nil, "prompt-v1", pipelineID.String(),
		"context-v1", nil, "memory-rendering-v1", f.revision["A"]["principles"],
		f.revision["A"]["persona"], fixture.policyID.String(), nil, nil, nil, nil, nil, "{}",
		semanticTime+9, semanticTZ, 0, "{}", semanticTime+9, semanticTZ)
	mustExec(t, f.db, `INSERT INTO generation_run_outcomes(
		outcome_id, canonical_commit_id, generation_run_id, attempt_no, state,
		output_content_id, prompt_tokens, completion_tokens, latency, estimated_cost,
		error_class, error_detail_content_id, recorded_at, recorded_tz
	) VALUES (?, ?, ?, 1, 'running', NULL, NULL, NULL, NULL, NULL, NULL, NULL, ?, ?)`,
		fixture.newID(t).String(), fixture.activeCommit.String(), runID.String(), semanticTime+9, semanticTZ)
	mustExec(t, f.db, `INSERT INTO generation_run_inputs(
		generation_run_input_id, canonical_commit_id, generation_run_id, ordinal,
		role, source_type, source_id, inclusion_mode, content_id, recorded_at, recorded_tz
	) SELECT ?, ?, ?, 0, 'system', 'resident_revision', ?, 'resident_definition',
		revision.content_id, ?, ? FROM resident_revisions revision WHERE revision.revision_id = ?`,
		fixture.newID(t).String(), fixture.activeCommit.String(), runID.String(),
		fixture.policyID.String(), semanticTime+9, semanticTZ, fixture.policyID.String())
	return runID
}

func (fixture *derivedClaimMutationFixture) seedClaimScope(
	t *testing.T,
	claimID canonical.ID,
	scope memory.ViewScope,
) {
	t.Helper()
	mustExec(t, fixture.semantic.db, `INSERT INTO claim_view_scope_assertions(
		view_scope_assertion_id, canonical_commit_id, claim_id, view_scope,
		actor_principal_id, generation_run_id, memory_policy_revision_id,
		reason_code, reason_content_id, recorded_at, recorded_tz
	) VALUES (?, ?, ?, ?, ?, NULL, ?, 'test_initial_scope', NULL, ?, ?)`,
		fixture.newID(t).String(), fixture.semantic.commit["A"], claimID.String(), string(scope),
		fixture.ownerPrincipal.String(), fixture.policyID.String(), semanticTime, semanticTZ)
}

func (fixture *derivedClaimMutationFixture) abstractionValue(
	t *testing.T,
	sources []canonical.ID,
	evidence []canonical.ID,
) domain.LandDerivedClaim {
	t.Helper()
	value := domain.LandDerivedClaim{
		Attempt: domain.Attempt{
			RunID: fixture.seedDerivedRun(t, domain.GenerationPurposeMemoryAbstraction,
				fixture.abstractionPipeline),
			ResidentID: fixture.residentID, AttemptNo: 1, OutcomeID: fixture.newID(t),
		},
		OwnerPrincipalID: fixture.ownerPrincipal, ClaimID: fixture.newID(t),
		TemporalKind: memory.TemporalStable,
		Statement:    fixture.content(t, "claim_statement", []byte("A stable derived fact"), "independent"),
		Output: fixture.content(t, "generation_output", []byte(
			`{"statement":"A stable derived fact","temporal_kind":"stable","version":"memory-derived-output-v1"}`,
		), "independent"),
		PipelineVersionID: fixture.abstractionPipeline, MemoryPolicyRevisionID: fixture.policyID,
		InitialStageID: fixture.newID(t), InitialViewScopeID: fixture.newID(t),
	}
	for index, sourceID := range sources {
		value.Sources = append(value.Sources, domain.DerivedClaimSource{
			ClaimID: sourceID, RelationID: fixture.newID(t),
		})
		value.Evidence = append(value.Evidence, domain.DerivedClaimEvidence{
			EvidenceID: fixture.newID(t), SourceClaimID: sourceID,
			SourceEvidenceID: evidence[index],
		})
	}
	return value
}

func (fixture *derivedClaimMutationFixture) sourceClaims(
	t *testing.T,
	scopes ...memory.ViewScope,
) ([]canonical.ID, []canonical.ID) {
	t.Helper()
	claims := make([]canonical.ID, 0, len(scopes))
	evidence := make([]canonical.ID, 0, len(scopes))
	for _, scope := range scopes {
		eventID := fixture.seedEvent(t, memory.EventUserMessage, fixture.ownerPrincipal, memory.TrustTrusted)
		claimID, evidenceIDs := fixture.seedClaim(t, memory.ClaimKindOther,
			fixture.ownerPrincipal, fixture.residentPrincipal,
			[]seededClaimEvidence{trustedEvidence(eventID, memory.EventUserMessage,
				fixture.ownerPrincipal, memory.PolaritySupport)}, memory.StageFloating)
		fixture.seedClaimScope(t, claimID, scope)
		claims = append(claims, claimID)
		evidence = append(evidence, evidenceIDs[0])
	}
	return claims, evidence
}

func TestM5I78DerivedClaimLandingCreatesInitialScopeAtomically(t *testing.T) {
	fixture := newDerivedClaimMutationFixture(t)
	sourceIDs, sourceEvidence := fixture.sourceClaims(t, memory.ScopeResidentUI, memory.ScopeAdminOnly)
	value := fixture.abstractionValue(t, sourceIDs, sourceEvidence)

	uow, metadata := fixture.beginUoW(t)
	result, err := uow.LandClaimAbstraction(context.Background(), value)
	if err != nil {
		_ = uow.tx.Rollback()
		t.Fatal(err)
	}
	if result.InitialViewScope != memory.ScopeAdminOnly || len(result.EvidenceIDs) != 2 ||
		len(result.RelationIDs) != 2 {
		_ = uow.tx.Rollback()
		t.Fatalf("derived landing result = %+v", result)
	}
	if err := uow.tx.Commit(); err != nil {
		t.Fatal(err)
	}

	var distinctCommits int
	if err := fixture.semantic.db.QueryRow(`SELECT COUNT(DISTINCT canonical_commit_id) FROM (
		SELECT canonical_commit_id FROM claims WHERE claim_id = ?
		UNION ALL SELECT canonical_commit_id FROM claim_evidence WHERE claim_id = ?
		UNION ALL SELECT canonical_commit_id FROM claim_stage_transitions WHERE claim_id = ?
		UNION ALL SELECT canonical_commit_id FROM claim_view_scope_assertions WHERE claim_id = ?
		UNION ALL SELECT canonical_commit_id FROM claim_relations WHERE from_claim_id = ?
		UNION ALL SELECT canonical_commit_id FROM generation_run_outcomes WHERE outcome_id = ?
	)`, value.ClaimID.String(), value.ClaimID.String(), value.ClaimID.String(), value.ClaimID.String(),
		value.ClaimID.String(), value.OutcomeID.String()).Scan(&distinctCommits); err != nil {
		t.Fatal(err)
	}
	if distinctCommits != 1 {
		t.Fatalf("derived Canonical rows span %d commits, want 1 (%s)", distinctCommits, metadata.CommitID)
	}

	var fromStage sql.NullString
	var toStage, scope, stagePolicy, stagePipeline, stageRun string
	if err := fixture.semantic.db.QueryRow(`SELECT stage.from_stage, stage.to_stage,
		scope.view_scope, stage.memory_policy_revision_id, stage.pipeline_version_id,
		stage.generation_run_id FROM claim_stage_transitions stage
		JOIN claim_view_scope_assertions scope ON scope.claim_id = stage.claim_id
		WHERE stage.claim_id = ?`, value.ClaimID.String()).Scan(&fromStage, &toStage, &scope,
		&stagePolicy, &stagePipeline, &stageRun); err != nil {
		t.Fatal(err)
	}
	if fromStage.Valid || toStage != string(memory.StageFloating) || scope != string(memory.ScopeAdminOnly) ||
		stagePolicy != value.MemoryPolicyRevisionID.String() || stagePipeline != value.PipelineVersionID.String() ||
		stageRun != value.RunID.String() {
		t.Fatalf("initial stage/scope provenance = %+v %s %s %s %s %s",
			fromStage, toStage, scope, stagePolicy, stagePipeline, stageRun)
	}

	rows, err := fixture.semantic.db.Query(`SELECT grade, weight, derivation, source_evidence_id,
		memory_policy_revision_id, created_by_run_id FROM claim_evidence
		WHERE claim_id = ? ORDER BY evidence_id`, value.ClaimID.String())
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	count := 0
	for rows.Next() {
		var grade, derivation, sourceRaw, policyRaw, runRaw string
		var weight int64
		if err := rows.Scan(&grade, &weight, &derivation, &sourceRaw, &policyRaw, &runRaw); err != nil {
			t.Fatal(err)
		}
		if grade != string(memory.GradeInferred) || weight != 300_000 ||
			derivation != string(memory.DerivationInherited) || sourceRaw == "" ||
			policyRaw != value.MemoryPolicyRevisionID.String() || runRaw != value.RunID.String() {
			t.Fatalf("inherited evidence = %s/%d/%s/%s/%s/%s", grade, weight,
				derivation, sourceRaw, policyRaw, runRaw)
		}
		count++
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	if count != 2 {
		t.Fatalf("inherited evidence rows = %d, want 2", count)
	}
	for _, sourceID := range sourceIDs {
		var sourceTransitions int
		if err := fixture.semantic.db.QueryRow(`SELECT COUNT(*) FROM claim_status_transitions
			WHERE claim_id = ?`, sourceID.String()).Scan(&sourceTransitions); err != nil || sourceTransitions != 0 {
			t.Fatalf("source claim status changed: count=%d err=%v", sourceTransitions, err)
		}
	}
}

func TestM5I15ExtractionRejectsRecallUsageAsEvidence(t *testing.T) {
	fixture := newDerivedClaimMutationFixture(t)
	sourceIDs, sourceEvidence := fixture.sourceClaims(t, memory.ScopeResidentUI, memory.ScopeResidentUI)
	value := fixture.abstractionValue(t, sourceIDs, sourceEvidence)
	usageID := fixture.newID(t)
	mustExec(t, fixture.semantic.db, `INSERT INTO claim_usages(
		claim_usage_id, canonical_commit_id, claim_id, recall_run_id, generation_run_id,
		usage_type, ordinal, memory_policy_revision_id, exclusion_reason, detection_method,
		detection_confidence, detected_by_run_id, recorded_at, recorded_tz
	) VALUES (?, ?, ?, ?, NULL, 'selected', 0, ?, NULL, NULL, NULL, NULL, ?, ?)`,
		usageID.String(), fixture.semantic.commit["A"], sourceIDs[0].String(),
		fixture.semantic.recall["A"], fixture.policyID.String(), semanticTime, semanticTZ)
	value.Evidence[0].SourceEvidenceID = usageID

	uow, _ := fixture.beginUoW(t)
	if _, err := uow.LandClaimAbstraction(context.Background(), value); err == nil {
		_ = uow.tx.Rollback()
		t.Fatal("Recall claim_usage was accepted as Canonical source evidence")
	}
	_ = uow.tx.Rollback()
	var claimCount, outcomeCount int
	_ = fixture.semantic.db.QueryRow(`SELECT COUNT(*) FROM claims WHERE claim_id = ?`,
		value.ClaimID.String()).Scan(&claimCount)
	_ = fixture.semantic.db.QueryRow(`SELECT COUNT(*) FROM generation_run_outcomes WHERE outcome_id = ?`,
		value.OutcomeID.String()).Scan(&outcomeCount)
	if claimCount != 0 || outcomeCount != 0 {
		t.Fatalf("rejected Recall evidence leaked claim/outcome: %d/%d", claimCount, outcomeCount)
	}
}

func TestM5I78DerivedClaimInitialScopeRollsBackWithLateRelationFailure(t *testing.T) {
	fixture := newDerivedClaimMutationFixture(t)
	sourceIDs, sourceEvidence := fixture.sourceClaims(t, memory.ScopeResidentUI, memory.ScopeAdminOnly)
	value := fixture.abstractionValue(t, sourceIDs, sourceEvidence)
	// The duplicate relation ID fails only after the UoW has attempted the new
	// claim, inherited evidence, terminal outcome, initial stage and scope.
	mustExec(t, fixture.semantic.db, `INSERT INTO claim_relations(
		claim_relation_id, canonical_commit_id, from_claim_id, to_claim_id,
		relation_type, reason_code, reason_content_id, generation_run_id,
		occurred_at, occurred_tz, recorded_at, recorded_tz
	) VALUES (?, ?, ?, ?, 'contradicts', 'test_blocker', NULL, ?, ?, ?, ?, ?)`,
		value.Sources[0].RelationID.String(), fixture.semantic.commit["A"],
		sourceIDs[0].String(), sourceIDs[1].String(), fixture.semantic.run["A"],
		semanticTime, semanticTZ, semanticTime, semanticTZ)

	uow, _ := fixture.beginUoW(t)
	if _, err := uow.LandClaimAbstraction(context.Background(), value); err == nil {
		_ = uow.tx.Rollback()
		t.Fatal("late relation conflict unexpectedly committed")
	}
	_ = uow.tx.Rollback()
	for name, query := range map[string]string{
		"claim":    `SELECT COUNT(*) FROM claims WHERE claim_id = ?`,
		"evidence": `SELECT COUNT(*) FROM claim_evidence WHERE claim_id = ?`,
		"stage":    `SELECT COUNT(*) FROM claim_stage_transitions WHERE claim_id = ?`,
		"scope":    `SELECT COUNT(*) FROM claim_view_scope_assertions WHERE claim_id = ?`,
		"relation": `SELECT COUNT(*) FROM claim_relations WHERE from_claim_id = ?`,
		"outcome":  `SELECT COUNT(*) FROM generation_run_outcomes WHERE outcome_id = ?`,
	} {
		id := value.ClaimID
		if name == "outcome" {
			id = value.OutcomeID
		}
		var count int
		if err := fixture.semantic.db.QueryRow(query, id.String()).Scan(&count); err != nil || count != 0 {
			t.Fatalf("failed derived landing leaked %s: count=%d err=%v", name, count, err)
		}
	}
}

func TestDerivedClaimDifferentiationWritesNewToOldSplitWithoutChangingSourceStatus(t *testing.T) {
	fixture := newDerivedClaimMutationFixture(t)
	sourceIDs, sourceEvidence := fixture.sourceClaims(t, memory.ScopeResidentUI)
	value := fixture.abstractionValue(t, sourceIDs, sourceEvidence)
	value.RunID = fixture.seedDerivedRun(t, domain.GenerationPurposeMemoryDifferentiation,
		fixture.differentiationPipeline)
	value.PipelineVersionID = fixture.differentiationPipeline

	uow, _ := fixture.beginUoW(t)
	result, err := uow.LandClaimDifferentiation(context.Background(), value)
	if err != nil {
		_ = uow.tx.Rollback()
		t.Fatal(err)
	}
	if err := uow.tx.Commit(); err != nil {
		t.Fatal(err)
	}
	var fromRaw, toRaw, relation, reason string
	if err := fixture.semantic.db.QueryRow(`SELECT from_claim_id, to_claim_id,
		relation_type, reason_code FROM claim_relations WHERE claim_relation_id = ?`,
		result.RelationIDs[0].String()).Scan(&fromRaw, &toRaw, &relation, &reason); err != nil {
		t.Fatal(err)
	}
	if fromRaw != value.ClaimID.String() || toRaw != sourceIDs[0].String() ||
		relation != string(memory.RelationSplitFrom) || reason != "admin_differentiation" {
		t.Fatalf("split relation = %s -> %s %s %s", fromRaw, toRaw, relation, reason)
	}
	var sourceTransitions int
	_ = fixture.semantic.db.QueryRow(`SELECT COUNT(*) FROM claim_status_transitions WHERE claim_id = ?`,
		sourceIDs[0].String()).Scan(&sourceTransitions)
	if sourceTransitions != 0 {
		t.Fatalf("differentiation changed source status %d times", sourceTransitions)
	}
}

func TestDerivedClaimWriterRevalidatesActiveV2PolicyPipelineAndOwner(t *testing.T) {
	for _, test := range []struct {
		name   string
		mutate func(*testing.T, *derivedClaimMutationFixture, *domain.LandDerivedClaim)
	}{
		{name: "inactive policy", mutate: func(t *testing.T, fixture *derivedClaimMutationFixture, value *domain.LandDerivedClaim) {
			value.MemoryPolicyRevisionID = claimMutationParseID(t, fixture.semantic.revision["A"]["memory_policy"])
		}},
		{name: "wrong pipeline", mutate: func(_ *testing.T, fixture *derivedClaimMutationFixture, value *domain.LandDerivedClaim) {
			value.PipelineVersionID = fixture.differentiationPipeline
		}},
		{name: "non owner", mutate: func(t *testing.T, fixture *derivedClaimMutationFixture, value *domain.LandDerivedClaim) {
			value.OwnerPrincipalID = fixture.newID(t)
			mustExec(t, fixture.semantic.db, `INSERT INTO principals(
				principal_id, canonical_commit_id, kind, display_name, created_at, created_tz
			) VALUES (?, ?, 'human', 'not-owner', ?, ?)`, value.OwnerPrincipalID.String(),
				fixture.semantic.commit["global"], semanticTime, semanticTZ)
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			fixture := newDerivedClaimMutationFixture(t)
			sourceIDs, sourceEvidence := fixture.sourceClaims(t, memory.ScopeResidentUI, memory.ScopeResidentUI)
			value := fixture.abstractionValue(t, sourceIDs, sourceEvidence)
			test.mutate(t, fixture, &value)
			uow, _ := fixture.beginUoW(t)
			if _, err := uow.LandClaimAbstraction(context.Background(), value); err == nil {
				_ = uow.tx.Rollback()
				t.Fatal("Writer accepted stale policy, mismatched pipeline, or non-owner")
			}
			_ = uow.tx.Rollback()
			var count int
			_ = fixture.semantic.db.QueryRow(`SELECT COUNT(*) FROM claims WHERE claim_id = ?`,
				value.ClaimID.String()).Scan(&count)
			if count != 0 {
				t.Fatal("failed derived landing leaked claim")
			}
		})
	}
}

func TestDerivedClaimRejectsExistingNormalizedIdentity(t *testing.T) {
	fixture := newDerivedClaimMutationFixture(t)
	sourceIDs, sourceEvidence := fixture.sourceClaims(t, memory.ScopeResidentUI, memory.ScopeResidentUI)
	value := fixture.abstractionValue(t, sourceIDs, sourceEvidence)
	normalized, err := memory.NormalizeStatementV1(string(value.Statement.Bytes))
	if err != nil {
		t.Fatal(err)
	}
	duplicateID := fixture.newID(t)
	statementID := fixture.semantic.addContent(t, "A", "claim_statement", "duplicate derived fact", "independent")
	mustExec(t, fixture.semantic.db, `INSERT INTO claims(
		claim_id, canonical_commit_id, owner_resident_id, subject_principal_id,
		perspective_principal_id, kind, temporal_kind, statement_content_id,
		statement_hash, statement_hash_algorithm, statement_normalization_version,
		created_by_run_id, recorded_at, recorded_tz
	) VALUES (?, ?, ?, ?, ?, 'other', 'stable', ?, ?, 'sha256', ?, ?, ?, ?)`,
		duplicateID.String(), fixture.semantic.commit["A"], fixture.residentID.String(),
		fixture.ownerPrincipal.String(), fixture.residentPrincipal.String(), statementID,
		canonical.HashBlob([]byte(normalized)).Bytes(), memory.NormalizationVersionV1,
		fixture.semantic.run["A"], semanticTime, semanticTZ)

	uow, _ := fixture.beginUoW(t)
	if _, err := uow.LandClaimAbstraction(context.Background(), value); err == nil {
		_ = uow.tx.Rollback()
		t.Fatal("existing normalized claim identity was accepted")
	}
	_ = uow.tx.Rollback()
}
