package sqlite

import (
	"bytes"
	"context"
	"database/sql"
	"testing"

	"mahoroba.local/mahoroba/internal/canonical"
	"mahoroba.local/mahoroba/internal/domain"
	"mahoroba.local/mahoroba/internal/memory"
)

type memoryClaimMutationFixture struct {
	semantic           *semanticFixture
	close              func()
	policyID           canonical.ID
	maturationPipeline canonical.ID
	statusPipeline     canonical.ID
	residentID         canonical.ID
	residentPrincipal  canonical.ID
	ownerPrincipal     canonical.ID
	nextCommitSeq      int64
}

type seededClaimEvidence struct {
	EventID   canonical.ID
	EventType memory.EventType
	ActorID   canonical.ID
	Trust     memory.TrustLevel
	Polarity  memory.EvidencePolarity
	Grade     memory.EvidenceGrade
}

func newMemoryClaimMutationFixture(t *testing.T) *memoryClaimMutationFixture {
	t.Helper()
	fixture, closeFixture := newSemanticFixture(t)
	result := &memoryClaimMutationFixture{
		semantic: fixture, close: closeFixture, nextCommitSeq: 10,
		residentID:        claimMutationParseID(t, fixture.resident["A"]),
		residentPrincipal: claimMutationParseID(t, fixture.principal["A"]),
		ownerPrincipal:    claimMutationParseID(t, fixture.principal["human"]),
	}
	policyBytes, err := memory.DefaultPolicyV2().CanonicalJSON()
	if err != nil {
		closeFixture()
		t.Fatal(err)
	}
	policyContent := fixture.addContent(t, "A", "memory_policy_text", policyBytes.String(), "resident_only")
	result.policyID = result.newID(t)
	mustExec(t, fixture.db, `INSERT INTO resident_revisions(
		revision_id, canonical_commit_id, resident_id, revision_class, content_id,
		parent_revision_id, created_by_run_id, reason_content_id, recorded_at, recorded_tz
	) VALUES (?, ?, ?, 'memory_policy', ?, ?, NULL, NULL, ?, ?)`, result.policyID.String(),
		fixture.commit["A"], result.residentID.String(), policyContent,
		fixture.revision["A"]["memory_policy"], semanticTime, semanticTZ)
	mustExec(t, fixture.db, `INSERT INTO resident_revision_activations(
		activation_id, canonical_commit_id, resident_id, revision_id, actor_principal_id,
		approval_id, reason_code, reason_content_id, recorded_at, recorded_tz
	) VALUES (?, ?, ?, ?, ?, NULL, 'm5_test_activation', NULL, ?, ?)`, result.newID(t).String(),
		fixture.commit["A"], result.residentID.String(), result.policyID.String(),
		result.ownerPrincipal.String(), semanticTime, semanticTZ)
	mustExec(t, fixture.db, `INSERT INTO resident_status_transitions(
		resident_status_transition_id, canonical_commit_id, resident_id, from_status, to_status,
		actor_principal_id, reason_code, reason_content_id, occurred_at, occurred_tz, recorded_at, recorded_tz
	) VALUES (?, ?, ?, NULL, 'draft', ?, 'test_owner', NULL, ?, ?, ?, ?)`, result.newID(t).String(),
		fixture.commit["A"], result.residentID.String(), result.ownerPrincipal.String(),
		semanticTime, semanticTZ, semanticTime, semanticTZ)
	result.maturationPipeline = result.newID(t)
	result.statusPipeline = result.newID(t)
	mustExec(t, fixture.db, `INSERT INTO pipeline_versions(
		pipeline_version_id, canonical_commit_id, pipeline_kind, version_key, definition, recorded_at, recorded_tz
	) VALUES (?, ?, 'memory_maturation', ?, ?, ?, ?)`, result.maturationPipeline.String(), fixture.commit["global"],
		domain.MemoryMaturationPipelineVersion, `{"version":"memory-maturation-v1"}`, semanticTime, semanticTZ)
	mustExec(t, fixture.db, `INSERT INTO pipeline_versions(
		pipeline_version_id, canonical_commit_id, pipeline_kind, version_key, definition, recorded_at, recorded_tz
	) VALUES (?, ?, 'memory_status', ?, ?, ?, ?)`, result.statusPipeline.String(), fixture.commit["global"],
		domain.MemoryStatusPipelineVersion, `{"version":"memory-status-v1"}`, semanticTime, semanticTZ)
	t.Cleanup(closeFixture)
	return result
}

func (fixture *memoryClaimMutationFixture) newID(t *testing.T) canonical.ID {
	t.Helper()
	return claimMutationParseID(t, fixture.semantic.ids.new())
}

func (fixture *memoryClaimMutationFixture) addStageCommit(t *testing.T) string {
	t.Helper()
	commitID := fixture.newID(t)
	commitSeq := fixture.nextCommitSeq
	fixture.nextCommitSeq++
	mustExec(t, fixture.semantic.db, "INSERT INTO canonical_commits(canonical_commit_id, commit_seq, resident_id, committed_at, committed_tz) VALUES (?, ?, ?, ?, ?)",
		commitID.String(), commitSeq, fixture.residentID.String(), semanticTime+commitSeq, semanticTZ)
	return commitID.String()
}

func claimMutationParseID(t *testing.T, raw string) canonical.ID {
	t.Helper()
	id, err := canonical.ParseID(raw)
	if err != nil {
		t.Fatal(err)
	}
	return id
}

func (fixture *memoryClaimMutationFixture) content(
	t *testing.T,
	class string,
	logical []byte,
	policy string,
) domain.Content {
	t.Helper()
	salt, err := canonical.ContentSaltFromBytes(bytes.Repeat(
		[]byte{byte(fixture.nextCommitSeq)}, canonical.ContentCommitmentSaltLen,
	))
	if err != nil {
		t.Fatal(err)
	}
	commitment, err := canonical.CommitContent(class, salt, logical)
	if err != nil {
		t.Fatal(err)
	}
	return domain.Content{
		ID: fixture.newID(t), ResidentID: fixture.residentID, Class: class,
		Bytes: append([]byte(nil), logical...), BlobHash: canonical.HashBlob(logical),
		Commitment: commitment, CommitmentSalt: salt, ErasurePolicy: policy,
	}
}

func (fixture *memoryClaimMutationFixture) beginUoW(t *testing.T) (*canonicalUoW, canonical.CommitMetadata) {
	t.Helper()
	ctx := context.Background()
	tx, err := fixture.semantic.db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	commitID := fixture.newID(t)
	commitSeq, err := canonical.NewCommitSeq(fixture.nextCommitSeq)
	if err != nil {
		t.Fatal(err)
	}
	fixture.nextCommitSeq++
	metadata := canonical.CommitMetadata{
		CommitID: commitID, CommitSeq: commitSeq,
		Scope:       canonicalScopeForTest(t, fixture.residentID),
		CommittedAt: canonical.Instant(semanticTime + commitSeq.Int64()),
		CommittedTZ: canonical.MustTimezone(semanticTZ),
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO canonical_commits(
		canonical_commit_id, commit_seq, resident_id, committed_at, committed_tz
	) VALUES (?, ?, ?, ?, ?)`, commitID.String(), commitSeq.Int64(), fixture.residentID.String(),
		metadata.CommittedAt.UnixMicro(), metadata.CommittedTZ.String()); err != nil {
		_ = tx.Rollback()
		t.Fatal(err)
	}
	return &canonicalUoW{tx: tx, metadata: metadata}, metadata
}

func canonicalScopeForTest(t *testing.T, residentID canonical.ID) canonical.Scope {
	t.Helper()
	scope, err := canonical.ResidentScope(residentID)
	if err != nil {
		t.Fatal(err)
	}
	return scope
}

func (fixture *memoryClaimMutationFixture) seedGenerationRun(t *testing.T) canonical.ID {
	t.Helper()
	runID := fixture.newID(t)
	f := fixture.semantic
	mustExec(t, f.db, `INSERT INTO generation_runs VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`,
		runID.String(), f.commit["A"], fixture.residentID.String(), "memory_extraction",
		"claim-test-"+runID.String(), "test", "model", nil, "prompt-v1", fixture.maturationPipeline.String(),
		"context-v1", f.session, "render-v1", f.revision["A"]["principles"], f.revision["A"]["persona"],
		fixture.policyID.String(), f.recall["A"], nil, nil, nil, nil, "{}", semanticTime, semanticTZ, 0, "{}", semanticTime, semanticTZ)
	return runID
}

func (fixture *memoryClaimMutationFixture) seedEvent(
	t *testing.T,
	eventType memory.EventType,
	actor canonical.ID,
	trust memory.TrustLevel,
) canonical.ID {
	t.Helper()
	var seq int64
	if err := fixture.semantic.db.QueryRow(`SELECT COALESCE(MAX(seq), 0) + 1 FROM events WHERE resident_id = ?`,
		fixture.residentID.String()).Scan(&seq); err != nil {
		t.Fatal(err)
	}
	eventID := fixture.newID(t)
	contentID := fixture.semantic.addContent(t, "A", "event_payload", "event-"+eventID.String(), "independent")
	visibility, ingress, screen := "conversation", "local_ui", 1
	var run any
	if eventType != memory.EventUserMessage {
		ingress = "resident_runtime"
		run = fixture.seedGenerationRun(t).String()
	}
	if eventType == memory.EventSelfTalk {
		visibility, screen = "internal", 0
	}
	mustExec(t, fixture.semantic.db, `INSERT INTO events(
		event_id, canonical_commit_id, resident_id, seq, event_type, visibility,
		delivery_screen, delivery_audio, ingress, trust_level, actor_principal_id,
		target_principal_id, generation_run_id, occurred_at, occurred_tz, recorded_at,
		recorded_tz, content_id, payload_commitment, prev_event_hash, event_hash,
		event_hash_algorithm, event_hash_domain, canonicalization_version
	) VALUES (?, ?, ?, ?, ?, ?, ?, 0, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
		'sha256', 'mahoroba:event-hash:v1', 'mahoroba-jcs-v1')`, eventID.String(),
		fixture.semantic.commit["A"], fixture.residentID.String(), seq, string(eventType), visibility,
		screen, ingress, string(trust), actor.String(), fixture.residentPrincipal.String(), run,
		semanticTime+seq, semanticTZ, semanticTime+seq, semanticTZ, contentID,
		semanticDigest("payload-"+eventID.String()), semanticDigest("prev-"+eventID.String()),
		semanticDigest("event-"+eventID.String()))
	return eventID
}

func (fixture *memoryClaimMutationFixture) seedClaim(
	t *testing.T,
	kind memory.ClaimKind,
	subject, perspective canonical.ID,
	evidence []seededClaimEvidence,
	stage memory.ClaimStage,
) (canonical.ID, []canonical.ID) {
	t.Helper()
	claimID := fixture.newID(t)
	statementText := "claim-" + claimID.String()
	statementID := fixture.semantic.addContent(t, "A", "claim_statement", statementText, "independent")
	normalized, err := memory.NormalizeStatementV1(statementText)
	if err != nil {
		t.Fatal(err)
	}
	var kindValue any
	if kind != memory.ClaimKindUnclassified {
		kindValue = string(kind)
	}
	mustExec(t, fixture.semantic.db, `INSERT INTO claims(
		claim_id, canonical_commit_id, owner_resident_id, subject_principal_id,
		perspective_principal_id, kind, temporal_kind, statement_content_id,
		statement_hash, statement_hash_algorithm, statement_normalization_version,
		created_by_run_id, recorded_at, recorded_tz
	) VALUES (?, ?, ?, ?, ?, ?, 'stable', ?, ?, 'sha256', ?, ?, ?, ?)`, claimID.String(),
		fixture.semantic.commit["A"], fixture.residentID.String(), subject.String(), perspective.String(),
		kindValue, statementID, canonical.HashBlob([]byte(normalized)).Bytes(), memory.NormalizationVersionV1,
		fixture.semantic.run["A"], semanticTime, semanticTZ)
	policy := memory.DefaultPolicyV2()
	evidenceIDs := make([]canonical.ID, 0, len(evidence))
	for _, seed := range evidence {
		reason := memory.EvidenceReasonSourceInferred
		if seed.Grade == memory.GradeStated {
			reason = memory.EvidenceReasonSourceStated
		}
		point, err := memory.EvaluateEvidence(policy, memory.EvidencePoint{
			SourceEventID: seed.EventID, EventType: seed.EventType, Polarity: seed.Polarity,
			Grade: seed.Grade, Trust: seed.Trust, Derivation: memory.DerivationExtracted,
			Reason: reason, ActorIsSubject: seed.ActorID == subject,
			ActorIsPerspective: seed.ActorID == perspective,
		})
		if err != nil {
			t.Fatal(err)
		}
		evidenceID := fixture.newID(t)
		evidenceIDs = append(evidenceIDs, evidenceID)
		mustExec(t, fixture.semantic.db, `INSERT INTO claim_evidence(
			evidence_id, canonical_commit_id, claim_id, event_id, polarity, grade,
			trust_level, weight, derivation, source_evidence_id, memory_policy_revision_id,
			created_by_run_id, reason_code, reason_content_id, recorded_at, recorded_tz
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, 'extracted', NULL, ?, ?, ?, NULL, ?, ?)`,
			evidenceID.String(), fixture.semantic.commit["A"], claimID.String(), seed.EventID.String(),
			string(seed.Polarity), string(point.EffectiveGrade), string(seed.Trust),
			point.EffectiveWeight.Millionths(), fixture.policyID.String(), fixture.semantic.run["A"],
			string(reason), semanticTime, semanticTZ)
	}
	initialID := fixture.newID(t)
	mustExec(t, fixture.semantic.db, `INSERT INTO claim_stage_transitions(
		stage_transition_id, canonical_commit_id, claim_id, from_stage, to_stage,
		gate_metrics, pipeline_version_id, memory_policy_revision_id, generation_run_id,
		reason_code, reason_content_id, occurred_at, occurred_tz, recorded_at, recorded_tz
	) VALUES (?, ?, ?, NULL, 'floating', '{}', ?, ?, ?, 'initial_extraction', NULL, ?, ?, ?, ?)`,
		initialID.String(), fixture.semantic.commit["A"], claimID.String(), fixture.maturationPipeline.String(),
		fixture.policyID.String(), fixture.semantic.run["A"], semanticTime, semanticTZ, semanticTime, semanticTZ)
	if stage == memory.StageSediment || stage == memory.StageSettled {
		sedimentCommit := fixture.addStageCommit(t)
		mustExec(t, fixture.semantic.db, `INSERT INTO claim_stage_transitions VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`,
			fixture.newID(t).String(), sedimentCommit, claimID.String(), "floating", "sediment", "{}",
			fixture.maturationPipeline.String(), fixture.policyID.String(), fixture.semantic.run["A"],
			"maturation_threshold", nil, semanticTime, semanticTZ, semanticTime, semanticTZ)
	}
	if stage == memory.StageSettled {
		settledCommit := fixture.addStageCommit(t)
		mustExec(t, fixture.semantic.db, `INSERT INTO claim_stage_transitions VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`,
			fixture.newID(t).String(), settledCommit, claimID.String(), "sediment", "settled", "{}",
			fixture.maturationPipeline.String(), fixture.policyID.String(), fixture.semantic.run["A"],
			"maturation_threshold", nil, semanticTime, semanticTZ, semanticTime, semanticTZ)
	}
	return claimID, evidenceIDs
}

func (fixture *memoryClaimMutationFixture) addEvidenceValue(
	t *testing.T,
	claimID, eventID canonical.ID,
	polarity memory.EvidencePolarity,
	grade memory.EvidenceGrade,
) domain.AddClaimEvidence {
	t.Helper()
	reason := memory.EvidenceReasonSourceInferred
	if grade == memory.GradeStated {
		reason = memory.EvidenceReasonSourceStated
	}
	return domain.AddClaimEvidence{
		ResidentID: fixture.residentID, ClaimID: claimID, EvidenceID: fixture.newID(t),
		SourceEventID: eventID, Polarity: polarity, Grade: grade,
		Derivation: memory.DerivationExtracted, Reason: reason,
		MemoryPolicyRevisionID:      fixture.policyID,
		CreatedByRunID:              claimMutationParseID(t, fixture.semantic.run["A"]),
		MaturationPipelineVersionID: fixture.maturationPipeline,
		SedimentStageTransitionID:   fixture.newID(t), SettledStageTransitionID: fixture.newID(t),
	}
}

func trustedEvidence(eventID canonical.ID, eventType memory.EventType, actorID canonical.ID, polarity memory.EvidencePolarity) seededClaimEvidence {
	grade := memory.GradeStated
	if eventType == memory.EventSelfTalk {
		grade = memory.GradeInferred
	}
	return seededClaimEvidence{
		EventID: eventID, EventType: eventType, ActorID: actorID,
		Trust: memory.TrustTrusted, Polarity: polarity, Grade: grade,
	}
}

func TestM5I8CreateClaimRequiresEvidence(t *testing.T) {
	fixture := newMemoryClaimMutationFixture(t)
	eventID := fixture.seedEvent(t, memory.EventUserMessage, fixture.ownerPrincipal, memory.TrustTrusted)
	statement := fixture.content(t, "claim_statement", []byte("The owner likes tea"), "independent")
	landing := domain.ExtractedClaimLanding{
		ClaimID: fixture.newID(t), EvidenceID: fixture.newID(t),
		InitialStageID: fixture.newID(t), InitialViewScopeID: fixture.newID(t),
		Statement: statement,
	}
	command := domain.LandMemoryExtraction{
		Attempt: domain.Attempt{
			RunID: claimMutationParseID(t, fixture.semantic.run["A"]), ResidentID: fixture.residentID,
		},
		SourceEventID: eventID, PipelineVersionID: fixture.maturationPipeline,
		MemoryPolicyRevisionID: fixture.policyID,
	}
	extracted := memory.ExtractionClaim{
		Statement: string(statement.Bytes), Subject: memory.SelectorSourceActor,
		Perspective: memory.SelectorResident, TemporalKind: memory.TemporalStable,
		Grade: memory.GradeStated, SourceQuote: "likes tea",
	}
	uow, metadata := fixture.beginUoW(t)
	claimID, err := uow.landExtractedClaim(context.Background(), command, landing, extracted,
		memory.DefaultPolicyV2(), memory.EventUserMessage, memory.TrustTrusted,
		fixture.ownerPrincipal, fixture.residentPrincipal)
	if err != nil {
		_ = uow.tx.Rollback()
		t.Fatal(err)
	}
	if claimID != landing.ClaimID {
		_ = uow.tx.Rollback()
		t.Fatalf("landed claim = %s, want %s", claimID, landing.ClaimID)
	}
	if err := uow.tx.Commit(); err != nil {
		t.Fatal(err)
	}

	var claimCommit, evidenceCommit, stageCommit, scopeCommit string
	if err := fixture.semantic.db.QueryRow(`SELECT claim.canonical_commit_id,
		evidence.canonical_commit_id, stage.canonical_commit_id, scope.canonical_commit_id
		FROM claims claim
		JOIN claim_evidence evidence ON evidence.claim_id = claim.claim_id
		JOIN claim_stage_transitions stage ON stage.claim_id = claim.claim_id
		JOIN claim_view_scope_assertions scope ON scope.claim_id = claim.claim_id
		WHERE claim.claim_id = ? AND evidence.evidence_id = ?`, landing.ClaimID.String(),
		landing.EvidenceID.String()).Scan(&claimCommit, &evidenceCommit, &stageCommit, &scopeCommit); err != nil {
		t.Fatal(err)
	}
	for name, got := range map[string]string{
		"claim": claimCommit, "evidence": evidenceCommit, "initial stage": stageCommit, "initial scope": scopeCommit,
	} {
		if got != metadata.CommitID.String() {
			t.Fatalf("%s commit = %s, want %s", name, got, metadata.CommitID)
		}
	}
}

func TestAddClaimEvidenceRevalidatesEveryPersistedPointBeforeAggregation(t *testing.T) {
	fixture := newMemoryClaimMutationFixture(t)
	first := fixture.seedEvent(t, memory.EventUserMessage, fixture.ownerPrincipal, memory.TrustTrusted)
	claimID, _ := fixture.seedClaim(t, memory.ClaimKindOther, fixture.ownerPrincipal,
		fixture.residentPrincipal, []seededClaimEvidence{trustedEvidence(first, memory.EventUserMessage,
			fixture.ownerPrincipal, memory.PolaritySupport)}, memory.StageFloating)
	corruptEvent := fixture.seedEvent(t, memory.EventUserMessage, fixture.ownerPrincipal, memory.TrustTrusted)
	corruptEvidenceID := fixture.newID(t)
	mustExec(t, fixture.semantic.db, `INSERT INTO claim_evidence(
		evidence_id, canonical_commit_id, claim_id, event_id, polarity, grade,
		trust_level, weight, derivation, source_evidence_id, memory_policy_revision_id,
		created_by_run_id, reason_code, reason_content_id, recorded_at, recorded_tz
	) VALUES (?, ?, ?, ?, 'support', 'stated', 'trusted', 1, 'extracted', NULL, ?, ?,
		'source_stated', NULL, ?, ?)`, corruptEvidenceID.String(), fixture.semantic.commit["A"],
		claimID.String(), corruptEvent.String(), fixture.policyID.String(), fixture.semantic.run["A"],
		semanticTime, semanticTZ)

	newEvent := fixture.seedEvent(t, memory.EventUserMessage, fixture.ownerPrincipal, memory.TrustTrusted)
	value := fixture.addEvidenceValue(t, claimID, newEvent, memory.PolaritySupport, memory.GradeStated)
	uow, _ := fixture.beginUoW(t)
	if _, err := uow.AddClaimEvidence(context.Background(), value); err == nil {
		_ = uow.tx.Rollback()
		t.Fatal("pointwise-corrupt historical evidence entered an aggregate decision")
	}
	_ = uow.tx.Rollback()
	var count int
	if err := fixture.semantic.db.QueryRow(`SELECT COUNT(*) FROM claim_evidence WHERE evidence_id = ?`,
		value.EvidenceID.String()).Scan(&count); err != nil || count != 0 {
		t.Fatalf("failed aggregate revalidation leaked new evidence: count=%d err=%v", count, err)
	}
}

func TestAddClaimEvidenceRevalidatesInheritedProvenanceWithPurePolicy(t *testing.T) {
	fixture := newMemoryClaimMutationFixture(t)
	sourceEvent := fixture.seedEvent(t, memory.EventUserMessage, fixture.ownerPrincipal, memory.TrustTrusted)
	_, sourceEvidence := fixture.seedClaim(t, memory.ClaimKindOther, fixture.ownerPrincipal,
		fixture.residentPrincipal, []seededClaimEvidence{trustedEvidence(sourceEvent, memory.EventUserMessage,
			fixture.ownerPrincipal, memory.PolaritySupport)}, memory.StageFloating)
	baseEvent := fixture.seedEvent(t, memory.EventUserMessage, fixture.ownerPrincipal, memory.TrustTrusted)
	targetID, _ := fixture.seedClaim(t, memory.ClaimKindOther, fixture.ownerPrincipal,
		fixture.residentPrincipal, []seededClaimEvidence{trustedEvidence(baseEvent, memory.EventUserMessage,
			fixture.ownerPrincipal, memory.PolaritySupport)}, memory.StageFloating)
	value := fixture.addEvidenceValue(t, targetID, sourceEvent, memory.PolaritySupport, memory.GradeStated)
	value.Derivation = memory.DerivationInherited
	value.Reason = memory.EvidenceReasonInheritedAbstraction
	value.SourceEvidenceID = &sourceEvidence[0]
	uow, _ := fixture.beginUoW(t)
	result, err := uow.AddClaimEvidence(context.Background(), value)
	if err != nil {
		_ = uow.tx.Rollback()
		t.Fatal(err)
	}
	if result.FinalStage != memory.StageSediment {
		_ = uow.tx.Rollback()
		t.Fatalf("inherited point aggregate stage = %s, want sediment", result.FinalStage)
	}
	if err := uow.tx.Commit(); err != nil {
		t.Fatal(err)
	}
	var weight int64
	var sourceRaw, derivation string
	if err := fixture.semantic.db.QueryRow(`SELECT weight, source_evidence_id, derivation
		FROM claim_evidence WHERE evidence_id = ?`, value.EvidenceID.String()).
		Scan(&weight, &sourceRaw, &derivation); err != nil {
		t.Fatal(err)
	}
	if weight != 500_000 || sourceRaw != sourceEvidence[0].String() || derivation != "inherited" {
		t.Fatalf("inherited point = weight %d source %s derivation %s", weight, sourceRaw, derivation)
	}
}

func TestAddClaimEvidenceFixesDecisionTimePolicyAndPipeline(t *testing.T) {
	t.Run("inactive policy", func(t *testing.T) {
		fixture := newMemoryClaimMutationFixture(t)
		eventID := fixture.seedEvent(t, memory.EventUserMessage, fixture.ownerPrincipal, memory.TrustTrusted)
		claimID, _ := fixture.seedClaim(t, memory.ClaimKindOther, fixture.ownerPrincipal,
			fixture.residentPrincipal, []seededClaimEvidence{trustedEvidence(eventID, memory.EventUserMessage,
				fixture.ownerPrincipal, memory.PolaritySupport)}, memory.StageFloating)
		policyBytes, err := memory.DefaultPolicyV2().CanonicalJSON()
		if err != nil {
			t.Fatal(err)
		}
		contentID := fixture.semantic.addContent(t, "A", "memory_policy_text", policyBytes.String(), "resident_only")
		inactivePolicyID := fixture.newID(t)
		mustExec(t, fixture.semantic.db, `INSERT INTO resident_revisions(
			revision_id, canonical_commit_id, resident_id, revision_class, content_id,
			parent_revision_id, created_by_run_id, reason_content_id, recorded_at, recorded_tz
		) VALUES (?, ?, ?, 'memory_policy', ?, ?, NULL, NULL, ?, ?)`, inactivePolicyID.String(),
			fixture.semantic.commit["A"], fixture.residentID.String(), contentID, fixture.policyID.String(),
			semanticTime, semanticTZ)
		newEvent := fixture.seedEvent(t, memory.EventUserMessage, fixture.ownerPrincipal, memory.TrustTrusted)
		value := fixture.addEvidenceValue(t, claimID, newEvent, memory.PolaritySupport, memory.GradeStated)
		value.MemoryPolicyRevisionID = inactivePolicyID
		uow, _ := fixture.beginUoW(t)
		if _, err := uow.AddClaimEvidence(context.Background(), value); err == nil {
			_ = uow.tx.Rollback()
			t.Fatal("inactive policy was accepted as the decision-time policy")
		}
		_ = uow.tx.Rollback()
	})

	t.Run("wrong pipeline kind", func(t *testing.T) {
		fixture := newMemoryClaimMutationFixture(t)
		eventID := fixture.seedEvent(t, memory.EventUserMessage, fixture.ownerPrincipal, memory.TrustTrusted)
		claimID, _ := fixture.seedClaim(t, memory.ClaimKindOther, fixture.ownerPrincipal,
			fixture.residentPrincipal, []seededClaimEvidence{trustedEvidence(eventID, memory.EventUserMessage,
				fixture.ownerPrincipal, memory.PolaritySupport)}, memory.StageFloating)
		newEvent := fixture.seedEvent(t, memory.EventUserMessage, fixture.ownerPrincipal, memory.TrustTrusted)
		value := fixture.addEvidenceValue(t, claimID, newEvent, memory.PolaritySupport, memory.GradeStated)
		value.MaturationPipelineVersionID = fixture.statusPipeline
		uow, _ := fixture.beginUoW(t)
		if _, err := uow.AddClaimEvidence(context.Background(), value); err == nil {
			_ = uow.tx.Rollback()
			t.Fatal("memory_status pipeline was accepted for a maturation decision")
		}
		_ = uow.tx.Rollback()
	})
}

func TestM5I12SettledRejectsIneligibleSupport(t *testing.T) {
	t.Run("no support remains floating", func(t *testing.T) {
		fixture := newMemoryClaimMutationFixture(t)
		first := fixture.seedEvent(t, memory.EventUserMessage, fixture.ownerPrincipal, memory.TrustTrusted)
		claimID, _ := fixture.seedClaim(t, memory.ClaimKindOther, fixture.ownerPrincipal,
			fixture.residentPrincipal, []seededClaimEvidence{trustedEvidence(first, memory.EventUserMessage,
				fixture.ownerPrincipal, memory.PolarityContradict)}, memory.StageFloating)
		second := fixture.seedEvent(t, memory.EventUserMessage, fixture.ownerPrincipal, memory.TrustTrusted)
		value := fixture.addEvidenceValue(t, claimID, second, memory.PolarityContradict, memory.GradeStated)
		uow, _ := fixture.beginUoW(t)
		result, err := uow.AddClaimEvidence(context.Background(), value)
		if err != nil {
			_ = uow.tx.Rollback()
			t.Fatal(err)
		}
		if result.FinalStage != memory.StageFloating || len(result.StageTransitions) != 0 {
			t.Fatalf("unsupported claim advanced: %+v", result)
		}
		if err := uow.tx.Commit(); err != nil {
			t.Fatal(err)
		}
	})

	t.Run("one commit cannot record duplicate stage identities", func(t *testing.T) {
		fixture := newMemoryClaimMutationFixture(t)
		seeds := make([]seededClaimEvidence, 0, 2)
		for range 2 {
			eventID := fixture.seedEvent(t, memory.EventUserMessage, fixture.ownerPrincipal, memory.TrustTrusted)
			seeds = append(seeds, trustedEvidence(eventID, memory.EventUserMessage, fixture.ownerPrincipal, memory.PolaritySupport))
		}
		claimID, _ := fixture.seedClaim(t, memory.ClaimKindOther, fixture.ownerPrincipal,
			fixture.residentPrincipal, seeds, memory.StageFloating)
		third := fixture.seedEvent(t, memory.EventUserMessage, fixture.ownerPrincipal, memory.TrustTrusted)
		value := fixture.addEvidenceValue(t, claimID, third, memory.PolaritySupport, memory.GradeStated)
		uow, _ := fixture.beginUoW(t)
		if _, err := uow.AddClaimEvidence(context.Background(), value); err == nil {
			_ = uow.tx.Rollback()
			t.Fatal("same claim and canonical commit accepted two stage transitions")
		}
		_ = uow.tx.Rollback()
		var count int
		if err := fixture.semantic.db.QueryRow("SELECT COUNT(*) FROM claim_evidence WHERE evidence_id = ?", value.EvidenceID.String()).Scan(&count); err != nil || count != 0 {
			t.Fatalf("failed duplicate-stage UoW leaked evidence: count=%d err=%v", count, err)
		}
		if err := fixture.semantic.db.QueryRow("SELECT COUNT(*) FROM claim_stage_transitions WHERE stage_transition_id IN (?, ?)", value.SedimentStageTransitionID.String(), value.SettledStageTransitionID.String()).Scan(&count); err != nil || count != 0 {
			t.Fatalf("failed duplicate-stage UoW leaked transitions: count=%d err=%v", count, err)
		}
	})
}

func TestM5I39ResidentOriginOutboundEventsCannotBeEvidence(t *testing.T) {
	for _, eventType := range []memory.EventType{memory.EventResidentMessage, memory.EventOutboundInitiative} {
		t.Run(string(eventType), func(t *testing.T) {
			fixture := newMemoryClaimMutationFixture(t)
			base := fixture.seedEvent(t, memory.EventUserMessage, fixture.ownerPrincipal, memory.TrustTrusted)
			claimID, _ := fixture.seedClaim(t, memory.ClaimKindOther, fixture.ownerPrincipal,
				fixture.residentPrincipal, []seededClaimEvidence{trustedEvidence(base, memory.EventUserMessage,
					fixture.ownerPrincipal, memory.PolaritySupport)}, memory.StageFloating)
			forbidden := fixture.seedEvent(t, eventType, fixture.residentPrincipal, memory.TrustTrusted)
			value := fixture.addEvidenceValue(t, claimID, forbidden, memory.PolaritySupport, memory.GradeStated)
			uow, _ := fixture.beginUoW(t)
			if _, err := uow.AddClaimEvidence(context.Background(), value); err == nil {
				_ = uow.tx.Rollback()
				t.Fatal("resident-origin outbound event was accepted as evidence")
			}
			_ = uow.tx.Rollback()
			var count int
			if err := fixture.semantic.db.QueryRow(`SELECT COUNT(*) FROM claim_evidence WHERE evidence_id = ?`,
				value.EvidenceID.String()).Scan(&count); err != nil || count != 0 {
				t.Fatalf("failed evidence leaked: count=%d err=%v", count, err)
			}
		})
	}
}

func TestDirectEvidenceStopsAtSedimentWithoutAlignmentRun(t *testing.T) {
	fixture := newMemoryClaimMutationFixture(t)
	seeds := make([]seededClaimEvidence, 0, 3)
	for range 3 {
		eventID := fixture.seedEvent(t, memory.EventSelfTalk, fixture.residentPrincipal, memory.TrustTrusted)
		seeds = append(seeds, trustedEvidence(eventID, memory.EventSelfTalk, fixture.residentPrincipal, memory.PolaritySupport))
	}
	claimID, _ := fixture.seedClaim(t, memory.ClaimKindDirect, fixture.residentPrincipal,
		fixture.residentPrincipal, seeds, memory.StageFloating)
	fourth := fixture.seedEvent(t, memory.EventSelfTalk, fixture.residentPrincipal, memory.TrustTrusted)
	value := fixture.addEvidenceValue(t, claimID, fourth, memory.PolaritySupport, memory.GradeInferred)
	uow, _ := fixture.beginUoW(t)
	result, err := uow.AddClaimEvidence(context.Background(), value)
	if err != nil {
		_ = uow.tx.Rollback()
		t.Fatal(err)
	}
	if result.FinalStage != memory.StageSediment || len(result.StageTransitions) != 1 {
		t.Fatalf("unaligned direct claim = %+v", result)
	}
	_ = uow.tx.Rollback()
}

func TestM5I42OtherAndNullSettledRequireSubjectUserMessage(t *testing.T) {
	fixture := newMemoryClaimMutationFixture(t)
	seeds := make([]seededClaimEvidence, 0, 2)
	for range 2 {
		eventID := fixture.seedEvent(t, memory.EventUserMessage, fixture.residentPrincipal, memory.TrustTrusted)
		seeds = append(seeds, trustedEvidence(eventID, memory.EventUserMessage, fixture.residentPrincipal, memory.PolaritySupport))
	}
	claimID, _ := fixture.seedClaim(t, memory.ClaimKindOther, fixture.ownerPrincipal,
		fixture.residentPrincipal, seeds, memory.StageFloating)
	third := fixture.seedEvent(t, memory.EventUserMessage, fixture.residentPrincipal, memory.TrustTrusted)
	value := fixture.addEvidenceValue(t, claimID, third, memory.PolaritySupport, memory.GradeStated)
	uow, _ := fixture.beginUoW(t)
	result, err := uow.AddClaimEvidence(context.Background(), value)
	if err != nil {
		_ = uow.tx.Rollback()
		t.Fatal(err)
	}
	if result.FinalStage != memory.StageSediment {
		t.Fatalf("other claim without subject evidence settled: %+v", result)
	}
	_ = uow.tx.Rollback()
}

func TestM5I95MetaSettledRequiresPerspectiveUserMessage(t *testing.T) {
	fixture := newMemoryClaimMutationFixture(t)
	seeds := make([]seededClaimEvidence, 0, 2)
	for range 2 {
		eventID := fixture.seedEvent(t, memory.EventUserMessage, fixture.residentPrincipal, memory.TrustTrusted)
		seeds = append(seeds, trustedEvidence(eventID, memory.EventUserMessage, fixture.residentPrincipal, memory.PolaritySupport))
	}
	claimID, _ := fixture.seedClaim(t, memory.ClaimKindMeta, fixture.residentPrincipal,
		fixture.ownerPrincipal, seeds, memory.StageFloating)
	third := fixture.seedEvent(t, memory.EventUserMessage, fixture.residentPrincipal, memory.TrustTrusted)
	value := fixture.addEvidenceValue(t, claimID, third, memory.PolaritySupport, memory.GradeStated)
	uow, _ := fixture.beginUoW(t)
	result, err := uow.AddClaimEvidence(context.Background(), value)
	if err != nil {
		_ = uow.tx.Rollback()
		t.Fatal(err)
	}
	if result.FinalStage != memory.StageSediment {
		t.Fatalf("meta claim without perspective evidence settled: %+v", result)
	}
	_ = uow.tx.Rollback()
}

func TestM5I81AutomaticSemanticStatusRequiresAuthorizedPrincipalEvent(t *testing.T) {
	t.Run("correction", func(t *testing.T) {
		fixture := newMemoryClaimMutationFixture(t)
		eventID := fixture.seedEvent(t, memory.EventUserMessage, fixture.residentPrincipal, memory.TrustTrusted)
		claimID, evidenceIDs := fixture.seedClaim(t, memory.ClaimKindOther, fixture.ownerPrincipal,
			fixture.residentPrincipal, []seededClaimEvidence{trustedEvidence(eventID, memory.EventUserMessage,
				fixture.residentPrincipal, memory.PolarityContradict)}, memory.StageFloating)
		policyID := fixture.policyID
		value := domain.AutomaticClaimStatusDecision{
			ResidentID: fixture.residentID, ClaimID: claimID, StatusTransitionID: fixture.newID(t),
			ToStatus: memory.StatusInvalidated, Reason: memory.AutomaticReasonExplicitCorrection,
			TriggerKind: domain.ClaimTriggerEvidence, TriggerID: evidenceIDs[0],
			StatusPipelineVersionID: fixture.statusPipeline, MemoryPolicyRevisionID: &policyID,
		}
		uow, _ := fixture.beginUoW(t)
		if _, err := uow.AutomaticClaimStatusDecision(context.Background(), value); err == nil {
			_ = uow.tx.Rollback()
			t.Fatal("wrong-principal evidence authorized automatic correction")
		}
		_ = uow.tx.Rollback()
	})

	t.Run("supersession", func(t *testing.T) {
		fixture := newMemoryClaimMutationFixture(t)
		oldEvent := fixture.seedEvent(t, memory.EventUserMessage, fixture.ownerPrincipal, memory.TrustTrusted)
		oldID, _ := fixture.seedClaim(t, memory.ClaimKindOther, fixture.ownerPrincipal,
			fixture.residentPrincipal, []seededClaimEvidence{trustedEvidence(oldEvent, memory.EventUserMessage,
				fixture.ownerPrincipal, memory.PolaritySupport)}, memory.StageFloating)
		wrongActorEvent := fixture.seedEvent(t, memory.EventUserMessage, fixture.residentPrincipal, memory.TrustTrusted)
		replacementID, _ := fixture.seedClaim(t, memory.ClaimKindOther, fixture.ownerPrincipal,
			fixture.residentPrincipal, []seededClaimEvidence{trustedEvidence(wrongActorEvent, memory.EventUserMessage,
				fixture.residentPrincipal, memory.PolaritySupport)}, memory.StageFloating)
		relationID := fixture.newID(t)
		mustExec(t, fixture.semantic.db, `INSERT INTO claim_relations(
			claim_relation_id, canonical_commit_id, from_claim_id, to_claim_id,
			relation_type, reason_code, reason_content_id, generation_run_id,
			occurred_at, occurred_tz, recorded_at, recorded_tz
		) VALUES (?, ?, ?, ?, 'supersedes', 'explicit_supersession', NULL, ?, ?, ?, ?, ?)`,
			relationID.String(), fixture.semantic.commit["A"], replacementID.String(), oldID.String(),
			fixture.semantic.run["A"], semanticTime, semanticTZ, semanticTime, semanticTZ)
		policyID := fixture.policyID
		value := domain.AutomaticClaimStatusDecision{
			ResidentID: fixture.residentID, ClaimID: oldID, StatusTransitionID: fixture.newID(t),
			ToStatus: memory.StatusSuperseded, Reason: memory.AutomaticReasonExplicitSupersession,
			TriggerKind: domain.ClaimTriggerRelation, TriggerID: relationID,
			StatusPipelineVersionID: fixture.statusPipeline, MemoryPolicyRevisionID: &policyID,
		}
		uow, _ := fixture.beginUoW(t)
		if _, err := uow.AutomaticClaimStatusDecision(context.Background(), value); err == nil {
			_ = uow.tx.Rollback()
			t.Fatal("wrong-principal replacement evidence authorized automatic supersession")
		}
		_ = uow.tx.Rollback()
	})
}

func TestM5I87QuarantineFindingTargetsTransitionClaim(t *testing.T) {
	fixture := newMemoryClaimMutationFixture(t)
	eventOne := fixture.seedEvent(t, memory.EventUserMessage, fixture.ownerPrincipal, memory.TrustTrusted)
	targetID, _ := fixture.seedClaim(t, memory.ClaimKindOther, fixture.ownerPrincipal,
		fixture.residentPrincipal, []seededClaimEvidence{trustedEvidence(eventOne, memory.EventUserMessage,
			fixture.ownerPrincipal, memory.PolaritySupport)}, memory.StageFloating)
	eventTwo := fixture.seedEvent(t, memory.EventUserMessage, fixture.ownerPrincipal, memory.TrustTrusted)
	otherID, _ := fixture.seedClaim(t, memory.ClaimKindOther, fixture.ownerPrincipal,
		fixture.residentPrincipal, []seededClaimEvidence{trustedEvidence(eventTwo, memory.EventUserMessage,
			fixture.ownerPrincipal, memory.PolaritySupport)}, memory.StageFloating)
	findingID := fixture.newID(t)
	mustExec(t, fixture.semantic.db, `INSERT INTO integrity_findings(
		integrity_finding_id, canonical_commit_id, resident_id, claim_id, finding_kind,
		source_content_erasure_event_id, pipeline_version_id, details_content_id,
		occurred_at, occurred_tz, recorded_at, recorded_tz
	) VALUES (?, ?, ?, ?, 'provenance_unresolvable', NULL, ?, NULL, ?, ?, ?, ?)`, findingID.String(),
		fixture.semantic.commit["A"], fixture.residentID.String(), otherID.String(),
		fixture.statusPipeline.String(), semanticTime, semanticTZ, semanticTime, semanticTZ)
	value := domain.AutomaticClaimStatusDecision{
		ResidentID: fixture.residentID, ClaimID: targetID, StatusTransitionID: fixture.newID(t),
		ToStatus: memory.StatusQuarantined, Reason: memory.AutomaticReasonStructuralQuarantine,
		TriggerKind: domain.ClaimTriggerIntegrityFinding, TriggerID: findingID,
		StatusPipelineVersionID: fixture.statusPipeline,
	}
	uow, _ := fixture.beginUoW(t)
	if _, err := uow.AutomaticClaimStatusDecision(context.Background(), value); err == nil {
		_ = uow.tx.Rollback()
		t.Fatal("finding for another claim authorized structural quarantine")
	}
	_ = uow.tx.Rollback()
}

func TestM5I93SettledDirectRejectsAutomaticCorrectionAndUnqualifiedReplacement(t *testing.T) {
	fixture := newMemoryClaimMutationFixture(t)
	metaID := fixture.seedMetaAlignmentClaim(t)
	seeds := make([]seededClaimEvidence, 0, 5)
	for range 4 {
		eventID := fixture.seedEvent(t, memory.EventSelfTalk, fixture.residentPrincipal, memory.TrustTrusted)
		seeds = append(seeds, trustedEvidence(eventID, memory.EventSelfTalk,
			fixture.residentPrincipal, memory.PolaritySupport))
	}
	correctionEventID := fixture.seedEvent(t, memory.EventSelfTalk, fixture.residentPrincipal, memory.TrustTrusted)
	seeds = append(seeds, trustedEvidence(correctionEventID, memory.EventSelfTalk,
		fixture.residentPrincipal, memory.PolarityContradict))
	claimID, evidenceIDs := fixture.seedClaim(t, memory.ClaimKindDirect, fixture.residentPrincipal,
		fixture.residentPrincipal, seeds, memory.StageSettled)
	fixture.seedAlignmentDependency(t, claimID, metaID)
	policyID := fixture.policyID
	value := domain.AutomaticClaimStatusDecision{
		ResidentID: fixture.residentID, ClaimID: claimID, StatusTransitionID: fixture.newID(t),
		ToStatus: memory.StatusInvalidated, Reason: memory.AutomaticReasonExplicitCorrection,
		TriggerKind: domain.ClaimTriggerEvidence, TriggerID: evidenceIDs[len(evidenceIDs)-1],
		StatusPipelineVersionID: fixture.statusPipeline, MemoryPolicyRevisionID: &policyID,
	}
	uow, _ := fixture.beginUoW(t)
	if _, err := uow.AutomaticClaimStatusDecision(context.Background(), value); err == nil {
		_ = uow.tx.Rollback()
		t.Fatal("settled direct claim accepted automatic correction")
	}
	_ = uow.tx.Rollback()
}

func TestM5I11StatusMutationAlwaysAppendsTransition(t *testing.T) {
	fixture := newMemoryClaimMutationFixture(t)
	eventID := fixture.seedEvent(t, memory.EventUserMessage, fixture.ownerPrincipal, memory.TrustTrusted)
	claimID, _ := fixture.seedClaim(t, memory.ClaimKindOther, fixture.ownerPrincipal,
		fixture.residentPrincipal, []seededClaimEvidence{trustedEvidence(eventID, memory.EventUserMessage,
			fixture.ownerPrincipal, memory.PolaritySupport)}, memory.StageFloating)
	quarantine := domain.HumanClaimStatusDecision{
		ResidentID: fixture.residentID, ClaimID: claimID, StatusTransitionID: fixture.newID(t),
		OwnerPrincipalID: fixture.ownerPrincipal, ToStatus: memory.StatusQuarantined,
		Reason: memory.HumanReasonQuarantine,
	}
	uow, _ := fixture.beginUoW(t)
	if _, err := uow.HumanClaimStatusDecision(context.Background(), quarantine); err != nil {
		_ = uow.tx.Rollback()
		t.Fatal(err)
	}
	if err := uow.tx.Commit(); err != nil {
		t.Fatal(err)
	}
	reactivate := domain.HumanClaimStatusDecision{
		ResidentID: fixture.residentID, ClaimID: claimID, StatusTransitionID: fixture.newID(t),
		OwnerPrincipalID: fixture.ownerPrincipal, ToStatus: memory.StatusActive,
		Reason: memory.HumanReasonReactivation,
	}
	uow, _ = fixture.beginUoW(t)
	if _, err := uow.HumanClaimStatusDecision(context.Background(), reactivate); err != nil {
		_ = uow.tx.Rollback()
		t.Fatal(err)
	}
	if err := uow.tx.Commit(); err != nil {
		t.Fatal(err)
	}
	var decisions int
	if err := fixture.semantic.db.QueryRow(`SELECT COUNT(*) FROM claim_status_transitions
		WHERE claim_id = ? AND decision_kind = 'human'`, claimID.String()).Scan(&decisions); err != nil || decisions != 2 {
		t.Fatalf("human status decisions = %d, %v", decisions, err)
	}
}

func TestHumanClaimStatusDecisionRejectsNonOwnerHuman(t *testing.T) {
	fixture := newMemoryClaimMutationFixture(t)
	eventID := fixture.seedEvent(t, memory.EventUserMessage, fixture.ownerPrincipal, memory.TrustTrusted)
	claimID, _ := fixture.seedClaim(t, memory.ClaimKindOther, fixture.ownerPrincipal,
		fixture.residentPrincipal, []seededClaimEvidence{trustedEvidence(eventID, memory.EventUserMessage,
			fixture.ownerPrincipal, memory.PolaritySupport)}, memory.StageFloating)
	nonOwner := fixture.newID(t)
	mustExec(t, fixture.semantic.db, `INSERT INTO principals(
		principal_id, canonical_commit_id, kind, display_name, created_at, created_tz
	) VALUES (?, ?, 'human', 'non-owner', ?, ?)`, nonOwner.String(),
		fixture.semantic.commit["global"], semanticTime, semanticTZ)
	value := domain.HumanClaimStatusDecision{
		ResidentID: fixture.residentID, ClaimID: claimID, StatusTransitionID: fixture.newID(t),
		OwnerPrincipalID: nonOwner, ToStatus: memory.StatusInvalidated,
		Reason: memory.HumanReasonInvalidation,
	}
	uow, _ := fixture.beginUoW(t)
	if _, err := uow.HumanClaimStatusDecision(context.Background(), value); err == nil {
		_ = uow.tx.Rollback()
		t.Fatal("non-owner human changed claim status")
	}
	_ = uow.tx.Rollback()
}

func TestAutomaticStructuralQuarantinePersistsTriggerPipelineAndGateMetrics(t *testing.T) {
	fixture := newMemoryClaimMutationFixture(t)
	eventID := fixture.seedEvent(t, memory.EventUserMessage, fixture.ownerPrincipal, memory.TrustTrusted)
	claimID, _ := fixture.seedClaim(t, memory.ClaimKindOther, fixture.ownerPrincipal,
		fixture.residentPrincipal, []seededClaimEvidence{trustedEvidence(eventID, memory.EventUserMessage,
			fixture.ownerPrincipal, memory.PolaritySupport)}, memory.StageFloating)
	findingID := fixture.newID(t)
	mustExec(t, fixture.semantic.db, `INSERT INTO integrity_findings(
		integrity_finding_id, canonical_commit_id, resident_id, claim_id, finding_kind,
		source_content_erasure_event_id, pipeline_version_id, details_content_id,
		occurred_at, occurred_tz, recorded_at, recorded_tz
	) VALUES (?, ?, ?, ?, 'provenance_unresolvable', NULL, ?, NULL, ?, ?, ?, ?)`, findingID.String(),
		fixture.semantic.commit["A"], fixture.residentID.String(), claimID.String(),
		fixture.statusPipeline.String(), semanticTime, semanticTZ, semanticTime, semanticTZ)
	value := domain.AutomaticClaimStatusDecision{
		ResidentID: fixture.residentID, ClaimID: claimID, StatusTransitionID: fixture.newID(t),
		ToStatus: memory.StatusQuarantined, Reason: memory.AutomaticReasonStructuralQuarantine,
		TriggerKind: domain.ClaimTriggerIntegrityFinding, TriggerID: findingID,
		StatusPipelineVersionID: fixture.statusPipeline,
	}
	uow, _ := fixture.beginUoW(t)
	if _, err := uow.AutomaticClaimStatusDecision(context.Background(), value); err != nil {
		_ = uow.tx.Rollback()
		t.Fatal(err)
	}
	if err := uow.tx.Commit(); err != nil {
		t.Fatal(err)
	}
	var triggerKind, triggerID, pipelineID, metrics, reason, toStatus string
	var policyID sql.NullString
	if err := fixture.semantic.db.QueryRow(`SELECT trigger_kind, trigger_integrity_finding_id,
		pipeline_version_id, memory_policy_revision_id, gate_metrics, decision_reason_code, to_status
		FROM claim_status_transitions WHERE status_transition_id = ?`, value.StatusTransitionID.String()).
		Scan(&triggerKind, &triggerID, &pipelineID, &policyID, &metrics, &reason, &toStatus); err != nil {
		t.Fatal(err)
	}
	if triggerKind != "integrity_finding" || triggerID != findingID.String() ||
		pipelineID != fixture.statusPipeline.String() || policyID.Valid || metrics == "" || metrics == "{}" ||
		reason != "structural_quarantine" || toStatus != "quarantined" {
		t.Fatalf("automatic decision row is incomplete: %s %s %s %+v %s %s %s",
			triggerKind, triggerID, pipelineID, policyID, metrics, reason, toStatus)
	}
}

func (fixture *memoryClaimMutationFixture) seedSettledDirectClaim(t *testing.T) canonical.ID {
	t.Helper()
	metaID := fixture.seedMetaAlignmentClaim(t)
	seeds := make([]seededClaimEvidence, 0, 4)
	for range 4 {
		eventID := fixture.seedEvent(t, memory.EventSelfTalk, fixture.residentPrincipal, memory.TrustTrusted)
		seeds = append(seeds, trustedEvidence(eventID, memory.EventSelfTalk, fixture.residentPrincipal, memory.PolaritySupport))
	}
	claimID, _ := fixture.seedClaim(t, memory.ClaimKindDirect, fixture.residentPrincipal,
		fixture.residentPrincipal, seeds, memory.StageSettled)
	fixture.seedAlignmentDependency(t, claimID, metaID)
	return claimID
}

func (fixture *memoryClaimMutationFixture) seedReplacementCandidate(
	t *testing.T,
	stage memory.ClaimStage,
) (canonical.ID, canonical.ID) {
	t.Helper()
	metaID := fixture.seedMetaAlignmentClaim(t)
	directEvidenceCount := 3
	if stage == memory.StageSettled {
		directEvidenceCount = 4
	}
	directSeeds := make([]seededClaimEvidence, 0, directEvidenceCount)
	for range directEvidenceCount {
		eventID := fixture.seedEvent(t, memory.EventSelfTalk, fixture.residentPrincipal, memory.TrustTrusted)
		directSeeds = append(directSeeds, trustedEvidence(eventID, memory.EventSelfTalk, fixture.residentPrincipal, memory.PolaritySupport))
	}
	directID, _ := fixture.seedClaim(t, memory.ClaimKindDirect, fixture.residentPrincipal,
		fixture.residentPrincipal, directSeeds, stage)
	if stage == memory.StageSettled {
		fixture.seedAlignmentDependency(t, directID, metaID)
	}
	return directID, metaID
}

func (fixture *memoryClaimMutationFixture) seedMetaAlignmentClaim(t *testing.T) canonical.ID {
	t.Helper()
	metaSeeds := make([]seededClaimEvidence, 0, 2)
	for range 2 {
		eventID := fixture.seedEvent(t, memory.EventUserMessage, fixture.ownerPrincipal, memory.TrustTrusted)
		metaSeeds = append(metaSeeds, trustedEvidence(eventID, memory.EventUserMessage, fixture.ownerPrincipal, memory.PolaritySupport))
	}
	metaID, _ := fixture.seedClaim(t, memory.ClaimKindMeta, fixture.residentPrincipal,
		fixture.ownerPrincipal, metaSeeds, memory.StageSediment)
	return metaID
}

func (fixture *memoryClaimMutationFixture) seedAlignmentDependency(
	t *testing.T,
	directClaimID, metaClaimID canonical.ID,
) {
	t.Helper()
	var transitionRaw string
	if err := fixture.semantic.db.QueryRow(`SELECT stage_transition_id
		FROM claim_stage_transitions WHERE claim_id = ? AND to_stage = 'settled'
		ORDER BY stage_transition_id DESC LIMIT 1`, directClaimID.String()).Scan(&transitionRaw); err != nil {
		t.Fatal(err)
	}
	mustExec(t, fixture.semantic.db, `INSERT INTO claim_stage_transition_dependencies(
		stage_transition_dependency_id, canonical_commit_id, stage_transition_id,
		dependency_kind, dependency_claim_id
	) VALUES (?, ?, ?, 'meta_alignment', ?)`, fixture.newID(t).String(),
		fixture.semantic.commit["A"], transitionRaw, metaClaimID.String())
}
