package sqlite

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"

	"mahoroba.local/mahoroba/internal/canonical"
	"mahoroba.local/mahoroba/internal/domain"
	"mahoroba.local/mahoroba/internal/memory"
)

type derivedClaimOperation struct {
	relation        memory.RelationType
	purpose         domain.GenerationPurpose
	pipelineKind    string
	pipelineVersion string
	evidenceReason  memory.EvidenceReason
	reasonCode      string
}

type derivedClaimSourceRecord struct {
	landing domain.DerivedClaimSource
	claim   claimMutationRecord
	scope   memory.ViewScope
}

type derivedClaimEvidenceRecord struct {
	landing   domain.DerivedClaimEvidence
	eventID   canonical.ID
	polarity  memory.EvidencePolarity
	evaluated memory.EvaluatedEvidence
}

func (u *canonicalUoW) LandClaimAbstraction(
	ctx context.Context,
	value domain.LandDerivedClaim,
) (domain.DerivedClaimLandingResult, error) {
	return u.landDerivedClaim(ctx, value, derivedClaimOperation{
		relation:        memory.RelationAbstracts,
		purpose:         domain.GenerationPurposeMemoryAbstraction,
		pipelineKind:    "memory_abstraction",
		pipelineVersion: domain.MemoryAbstractionPipelineVersion,
		evidenceReason:  memory.EvidenceReasonInheritedAbstraction,
		reasonCode:      "admin_abstraction",
	})
}

func (u *canonicalUoW) LandClaimDifferentiation(
	ctx context.Context,
	value domain.LandDerivedClaim,
) (domain.DerivedClaimLandingResult, error) {
	return u.landDerivedClaim(ctx, value, derivedClaimOperation{
		relation:        memory.RelationSplitFrom,
		purpose:         domain.GenerationPurposeMemoryDifferentiation,
		pipelineKind:    "memory_differentiation",
		pipelineVersion: domain.MemoryDifferentiationPipelineVersion,
		evidenceReason:  memory.EvidenceReasonInheritedSplit,
		reasonCode:      "admin_differentiation",
	})
}

func (u *canonicalUoW) landDerivedClaim(
	ctx context.Context,
	value domain.LandDerivedClaim,
	operation derivedClaimOperation,
) (domain.DerivedClaimLandingResult, error) {
	if err := u.requireResidentScope(value.ResidentID); err != nil {
		return domain.DerivedClaimLandingResult{}, err
	}
	if err := u.requireActiveResident(ctx, value.ResidentID); err != nil {
		return domain.DerivedClaimLandingResult{}, err
	}
	if err := u.requireOwnerHuman(ctx, value.ResidentID, value.OwnerPrincipalID); err != nil {
		return domain.DerivedClaimLandingResult{}, err
	}
	policy, err := u.loadDecisionMemoryPolicy(ctx, value.ResidentID, value.MemoryPolicyRevisionID)
	if err != nil {
		return domain.DerivedClaimLandingResult{}, err
	}
	if err := u.requireMemoryPipeline(ctx, value.PipelineVersionID,
		operation.pipelineKind, operation.pipelineVersion); err != nil {
		return domain.DerivedClaimLandingResult{}, err
	}
	if err := u.requireDerivedGenerationRun(ctx, value, operation); err != nil {
		return domain.DerivedClaimLandingResult{}, err
	}
	parsedOutput, canonicalOutput, err := memory.ParseDerivedClaimOutput(value.Output.Bytes)
	if err != nil {
		return domain.DerivedClaimLandingResult{}, fmt.Errorf("sqlite: validate derived structured output: %w", err)
	}
	if !bytes.Equal(value.Output.Bytes, canonicalOutput.Bytes()) ||
		!bytes.Equal(value.Statement.Bytes, []byte(parsedOutput.Statement)) ||
		value.TemporalKind != parsedOutput.TemporalKind {
		return domain.DerivedClaimLandingResult{}, errors.New("sqlite: derived statement or temporal kind differs from structured output")
	}

	sources, identity, scopes, err := u.loadDerivedClaimSources(ctx, value, operation.relation)
	if err != nil {
		return domain.DerivedClaimLandingResult{}, err
	}
	if err := u.rejectDuplicateDerivedClaim(ctx, value, identity); err != nil {
		return domain.DerivedClaimLandingResult{}, err
	}
	evidence, err := u.loadDerivedClaimEvidence(ctx, value, operation, policy, sources)
	if err != nil {
		return domain.DerivedClaimLandingResult{}, err
	}
	scope, err := memory.NarrowestSourceScope(policy, scopes)
	if err != nil {
		return domain.DerivedClaimLandingResult{}, err
	}

	// All policy, provenance, source and envelope checks finish before the first
	// write. The enclosing Canonical transaction then lands every derived row
	// and the terminal generation outcome as one indivisible commit.
	if err := u.insertContent(ctx, value.Output); err != nil {
		return domain.DerivedClaimLandingResult{}, err
	}
	if err := u.insertOutcome(ctx, value.OutcomeID, value.RunID, value.AttemptNo,
		"succeeded", &value.Output.ID, value.PromptTokens, value.CompletionTokens, value.LatencyMicros, ""); err != nil {
		return domain.DerivedClaimLandingResult{}, err
	}
	if err := u.insertContent(ctx, value.Statement); err != nil {
		return domain.DerivedClaimLandingResult{}, err
	}
	if err := u.insertDerivedClaim(ctx, value, identity); err != nil {
		return domain.DerivedClaimLandingResult{}, err
	}
	for _, inherited := range evidence {
		if err := u.insertDerivedClaimEvidence(ctx, value, inherited, operation.evidenceReason); err != nil {
			return domain.DerivedClaimLandingResult{}, err
		}
	}
	if err := u.insertInitialDerivedClaimState(ctx, value, scope, operation.reasonCode,
		len(sources), len(evidence)); err != nil {
		return domain.DerivedClaimLandingResult{}, err
	}
	for _, source := range sources {
		if err := u.insertDerivedClaimRelation(ctx, value, source.landing, operation); err != nil {
			return domain.DerivedClaimLandingResult{}, err
		}
	}

	result := domain.DerivedClaimLandingResult{
		ClaimID: value.ClaimID, InitialStageID: value.InitialStageID,
		InitialViewScopeID: value.InitialViewScopeID, InitialViewScope: scope,
		EvidenceIDs: make([]canonical.ID, 0, len(evidence)),
		RelationIDs: make([]canonical.ID, 0, len(sources)),
	}
	for _, inherited := range evidence {
		result.EvidenceIDs = append(result.EvidenceIDs, inherited.landing.EvidenceID)
	}
	for _, source := range sources {
		result.RelationIDs = append(result.RelationIDs, source.landing.RelationID)
	}
	return result, nil
}

func (u *canonicalUoW) requireDerivedGenerationRun(
	ctx context.Context,
	value domain.LandDerivedClaim,
	operation derivedClaimOperation,
) error {
	var residentRaw, purposeRaw, pipelineRaw, policyRaw string
	if err := u.tx.QueryRowContext(ctx, `SELECT resident_id, purpose, pipeline_version_id,
		memory_policy_revision_id FROM generation_runs WHERE generation_run_id = ?`,
		value.RunID.String()).Scan(&residentRaw, &purposeRaw, &pipelineRaw, &policyRaw); err != nil {
		return fmt.Errorf("sqlite: load derived claim generation run: %w", err)
	}
	if residentRaw != value.ResidentID.String() || purposeRaw != string(operation.purpose) ||
		pipelineRaw != value.PipelineVersionID.String() ||
		policyRaw != value.MemoryPolicyRevisionID.String() {
		return errors.New("sqlite: derived claim generation envelope mismatch")
	}
	attempt, state, err := u.latestOutcome(ctx, value.RunID, value.ResidentID)
	if err != nil {
		return err
	}
	if state != "running" || attempt != value.AttemptNo {
		return fmt.Errorf("sqlite: derived claim landing must terminate running attempt %d; got %s/%d",
			value.AttemptNo, state, attempt)
	}
	return nil
}

func (u *canonicalUoW) loadDerivedClaimSources(
	ctx context.Context,
	value domain.LandDerivedClaim,
	relation memory.RelationType,
) ([]derivedClaimSourceRecord, claimMutationRecord, []memory.ViewScope, error) {
	if relation == memory.RelationAbstracts &&
		(len(value.Sources) < domain.MinimumAbstractionSources || len(value.Sources) > domain.MaximumAbstractionSources) {
		return nil, claimMutationRecord{}, nil, fmt.Errorf("sqlite: abstraction requires %d..%d source claims",
			domain.MinimumAbstractionSources, domain.MaximumAbstractionSources)
	}
	if relation == memory.RelationSplitFrom && len(value.Sources) != 1 {
		return nil, claimMutationRecord{}, nil, errors.New("sqlite: differentiation requires exactly one source claim")
	}

	records := make([]derivedClaimSourceRecord, 0, len(value.Sources))
	scopes := make([]memory.ViewScope, 0, len(value.Sources))
	var identity claimMutationRecord
	for index, source := range value.Sources {
		if _, err := loadEligibleClaimStatement(ctx, u.tx, value.ResidentID, source.ClaimID); err != nil {
			return nil, claimMutationRecord{}, nil, fmt.Errorf("sqlite: load derived source %d: %w", index, err)
		}
		claim, err := u.loadClaimForMutation(ctx, value.ResidentID, source.ClaimID)
		if err != nil {
			return nil, claimMutationRecord{}, nil, fmt.Errorf("sqlite: load derived source %d: %w", index, err)
		}
		status, err := u.currentClaimStatus(ctx, claim.ID)
		if err != nil {
			return nil, claimMutationRecord{}, nil, err
		}
		if status != memory.StatusActive {
			return nil, claimMutationRecord{}, nil, fmt.Errorf("sqlite: derived source claim %s is %s", claim.ID, status)
		}
		if index == 0 {
			identity = claim
		} else if claim.ResidentID != identity.ResidentID || claim.SubjectID != identity.SubjectID ||
			claim.PerspectiveID != identity.PerspectiveID || claim.Kind != identity.Kind {
			return nil, claimMutationRecord{}, nil, errors.New("sqlite: derived source claim identity mismatch")
		}
		scope, err := u.currentClaimViewScope(ctx, claim.ID)
		if err != nil {
			return nil, claimMutationRecord{}, nil, err
		}
		records = append(records, derivedClaimSourceRecord{landing: source, claim: claim, scope: scope})
		scopes = append(scopes, scope)
	}
	return records, identity, scopes, nil
}

func (u *canonicalUoW) currentClaimViewScope(ctx context.Context, claimID canonical.ID) (memory.ViewScope, error) {
	var raw string
	if err := u.tx.QueryRowContext(ctx, `SELECT assertion.view_scope
		FROM claim_view_scope_assertions assertion
		JOIN canonical_commits commit_row
		  ON commit_row.canonical_commit_id = assertion.canonical_commit_id
		WHERE assertion.claim_id = ?
		ORDER BY commit_row.commit_seq DESC, assertion.view_scope_assertion_id DESC LIMIT 1`,
		claimID.String()).Scan(&raw); err != nil {
		return "", fmt.Errorf("sqlite: resolve derived source claim scope: %w", err)
	}
	scope := memory.ViewScope(raw)
	if err := scope.Validate(); err != nil {
		return "", err
	}
	return scope, nil
}

func (u *canonicalUoW) rejectDuplicateDerivedClaim(
	ctx context.Context,
	value domain.LandDerivedClaim,
	identity claimMutationRecord,
) error {
	normalized, err := memory.NormalizeStatementV1(string(value.Statement.Bytes))
	if err != nil {
		return err
	}
	statementHash := canonical.HashBlob([]byte(normalized))
	query := `SELECT EXISTS(SELECT 1 FROM claims claim
		JOIN content_objects content ON content.content_id = claim.statement_content_id
		WHERE claim.owner_resident_id = ? AND claim.subject_principal_id = ? AND claim.perspective_principal_id = ?
		  AND claim.temporal_kind = ? AND claim.statement_hash_algorithm = 'sha256'
		  AND claim.statement_hash IS NOT NULL AND content.erasure_state = 'present'
		  AND claim.statement_hash = ? AND claim.statement_normalization_version = ? AND `
	args := []any{value.ResidentID.String(), identity.SubjectID.String(), identity.PerspectiveID.String(),
		string(value.TemporalKind), statementHash.Bytes(), memory.NormalizationVersionV1}
	if identity.Kind == memory.ClaimKindUnclassified {
		query += `claim.kind IS NULL)`
	} else {
		query += `claim.kind = ?)`
		args = append(args, string(identity.Kind))
	}
	var exists int
	if err := u.tx.QueryRowContext(ctx, query, args...).Scan(&exists); err != nil {
		return fmt.Errorf("sqlite: inspect normalized derived claim identity: %w", err)
	}
	if exists != 0 {
		return errors.New("sqlite: normalized derived claim identity already exists")
	}
	return nil
}

func (u *canonicalUoW) loadDerivedClaimEvidence(
	ctx context.Context,
	value domain.LandDerivedClaim,
	operation derivedClaimOperation,
	policy memory.Policy,
	sources []derivedClaimSourceRecord,
) ([]derivedClaimEvidenceRecord, error) {
	sourceByID := make(map[canonical.ID]derivedClaimSourceRecord, len(sources))
	sourceIDs := make([]canonical.ID, 0, len(sources))
	for _, source := range sources {
		sourceByID[source.claim.ID] = source
		sourceIDs = append(sourceIDs, source.claim.ID)
	}

	records := make([]derivedClaimEvidenceRecord, 0, len(value.Evidence))
	candidates := make([]memory.InheritedEvidenceCandidate, 0, len(value.Evidence))
	seenEvents := make(map[canonical.ID]struct{}, len(value.Evidence))
	for index, inherited := range value.Evidence {
		source, declared := sourceByID[inherited.SourceClaimID]
		if !declared {
			return nil, fmt.Errorf("sqlite: inherited evidence %d references undeclared source claim", index)
		}
		loaded, sourceWeight, err := u.loadDirectSourceEvidence(ctx, inherited, source.claim)
		if err != nil {
			return nil, fmt.Errorf("sqlite: load inherited evidence %d: %w", index, err)
		}
		if _, duplicate := seenEvents[loaded.eventID]; duplicate {
			return nil, errors.New("sqlite: derived claim repeats an underlying source event")
		}
		seenEvents[loaded.eventID] = struct{}{}
		if err := u.requireHighestWeightSourceEvidence(ctx, sourceIDs, loaded.eventID,
			inherited.SourceEvidenceID, sourceWeight); err != nil {
			return nil, err
		}
		sourceEvidenceID := inherited.SourceEvidenceID
		evaluated, err := memory.EvaluateEvidence(policy, memory.EvidencePoint{
			SourceEventID: loaded.eventID, SourceEvidenceID: &sourceEvidenceID,
			EventType: loaded.evaluated.EventType, Polarity: loaded.polarity,
			Grade: memory.GradeInferred, Trust: loaded.evaluated.Trust,
			Derivation: memory.DerivationInherited, InheritanceDepth: 1,
			Reason:             operation.evidenceReason,
			ActorIsSubject:     loaded.evaluated.ActorIsSubject,
			ActorIsPerspective: loaded.evaluated.ActorIsPerspective,
		})
		if err != nil {
			return nil, err
		}
		loaded.landing = inherited
		loaded.evaluated = evaluated
		records = append(records, loaded)
		candidates = append(candidates, memory.InheritedEvidenceCandidate{
			TargetClaimID: value.ClaimID, SourceClaimID: inherited.SourceClaimID,
			SourceEvidenceID: inherited.SourceEvidenceID, SourceEventID: loaded.eventID,
			Relation: operation.relation,
		})
	}
	if err := memory.ValidateInheritanceBatch(policy, candidates); err != nil {
		return nil, err
	}
	return records, nil
}

func (u *canonicalUoW) loadDirectSourceEvidence(
	ctx context.Context,
	inherited domain.DerivedClaimEvidence,
	source claimMutationRecord,
) (derivedClaimEvidenceRecord, int64, error) {
	var claimRaw, eventRaw, polarityRaw, gradeRaw, storedTrustRaw, derivationRaw string
	var policyRaw, reasonRaw string
	var sourceEvidence sql.NullString
	var sourceWeight int64
	var eventTypeRaw, eventTrustRaw, actorRaw string
	if err := u.tx.QueryRowContext(ctx, `SELECT evidence.claim_id, evidence.event_id,
		evidence.polarity, evidence.grade, evidence.trust_level, evidence.weight,
		evidence.derivation, evidence.source_evidence_id, evidence.memory_policy_revision_id,
		evidence.reason_code, event.event_type,
		event.trust_level, event.actor_principal_id
		FROM claim_evidence evidence
		JOIN events event ON event.event_id = evidence.event_id
		WHERE evidence.evidence_id = ? AND event.resident_id = ?`,
		inherited.SourceEvidenceID.String(), source.ResidentID.String()).Scan(
		&claimRaw, &eventRaw, &polarityRaw, &gradeRaw, &storedTrustRaw, &sourceWeight,
		&derivationRaw, &sourceEvidence, &policyRaw, &reasonRaw, &eventTypeRaw,
		&eventTrustRaw, &actorRaw,
	); err != nil {
		return derivedClaimEvidenceRecord{}, 0, fmt.Errorf("resolve Canonical claim_evidence: %w", err)
	}
	if claimRaw != inherited.SourceClaimID.String() || claimRaw != source.ID.String() {
		return derivedClaimEvidenceRecord{}, 0, errors.New("sqlite: inherited evidence does not belong to declared source claim")
	}
	if derivationRaw != string(memory.DerivationExtracted) || sourceEvidence.Valid {
		return derivedClaimEvidenceRecord{}, 0, errors.New("sqlite: inherited evidence source must be direct extracted evidence")
	}
	if storedTrustRaw != eventTrustRaw {
		return derivedClaimEvidenceRecord{}, 0, errors.New("sqlite: source evidence trust differs from underlying event")
	}
	eventID, err := canonical.ParseID(eventRaw)
	if err != nil {
		return derivedClaimEvidenceRecord{}, 0, err
	}
	actorID, err := canonical.ParseID(actorRaw)
	if err != nil {
		return derivedClaimEvidenceRecord{}, 0, err
	}
	polarity := memory.EvidencePolarity(polarityRaw)
	if err := polarity.Validate(); err != nil {
		return derivedClaimEvidenceRecord{}, 0, err
	}
	grade := memory.EvidenceGrade(gradeRaw)
	if err := grade.Validate(); err != nil {
		return derivedClaimEvidenceRecord{}, 0, err
	}
	trust := memory.TrustLevel(eventTrustRaw)
	if err := trust.Validate(); err != nil {
		return derivedClaimEvidenceRecord{}, 0, err
	}
	eventType := memory.EventType(eventTypeRaw)
	if err := eventType.Validate(); err != nil {
		return derivedClaimEvidenceRecord{}, 0, err
	}
	policyID, err := canonical.ParseID(policyRaw)
	if err != nil {
		return derivedClaimEvidenceRecord{}, 0, err
	}
	recordedPolicy, err := u.loadRecordedMemoryPolicy(ctx, source.ResidentID, policyID)
	if err != nil {
		return derivedClaimEvidenceRecord{}, 0, err
	}
	reason := memory.EvidenceReason(reasonRaw)
	sourcePoint, err := memory.EvaluateEvidence(recordedPolicy, memory.EvidencePoint{
		SourceEventID: eventID, EventType: eventType, Polarity: polarity,
		Grade: grade, Trust: trust, Derivation: memory.DerivationExtracted,
		Reason: reason, ActorIsSubject: actorID == source.SubjectID,
		ActorIsPerspective: actorID == source.PerspectiveID,
	})
	if err != nil {
		return derivedClaimEvidenceRecord{}, 0, err
	}
	if sourcePoint.EffectiveGrade != grade || sourcePoint.EffectiveWeight.Millionths() != sourceWeight {
		return derivedClaimEvidenceRecord{}, 0, errors.New("sqlite: source evidence differs from its recorded policy evaluation")
	}
	return derivedClaimEvidenceRecord{
		eventID: eventID, polarity: polarity,
		evaluated: sourcePoint,
	}, sourceWeight, nil
}

func (u *canonicalUoW) loadRecordedMemoryPolicy(
	ctx context.Context,
	residentID, revisionID canonical.ID,
) (memory.Policy, error) {
	var revisionResident, revisionClass, contentClass, erasureState string
	var content []byte
	if err := u.tx.QueryRowContext(ctx, `SELECT revision.resident_id, revision.revision_class,
		object.content_class, object.erasure_state, blob.content
		FROM resident_revisions revision
		JOIN content_objects object ON object.content_id = revision.content_id
		LEFT JOIN blobs blob ON blob.dedupe_scope_id = object.owner_resident_id
		 AND blob.hash_algorithm = object.blob_hash_algorithm
		 AND blob.blob_hash = object.blob_hash
		WHERE revision.revision_id = ?`, revisionID.String()).Scan(&revisionResident,
		&revisionClass, &contentClass, &erasureState, &content); err != nil {
		return memory.Policy{}, fmt.Errorf("sqlite: load source evidence memory policy: %w", err)
	}
	if revisionResident != residentID.String() || revisionClass != "memory_policy" ||
		contentClass != "memory_policy_text" || erasureState != "present" || content == nil {
		return memory.Policy{}, errors.New("sqlite: source evidence memory policy is unavailable or invalid")
	}
	policy, _, err := memory.ParsePolicy(content)
	if err != nil {
		return memory.Policy{}, err
	}
	if err := policy.RequireEnabled(); err != nil {
		return memory.Policy{}, err
	}
	return policy, nil
}

func (u *canonicalUoW) requireHighestWeightSourceEvidence(
	ctx context.Context,
	sourceClaimIDs []canonical.ID,
	eventID, selectedID canonical.ID,
	selectedWeight int64,
) error {
	placeholders := make([]string, len(sourceClaimIDs))
	args := make([]any, 0, len(sourceClaimIDs)+1)
	args = append(args, eventID.String())
	for index, sourceID := range sourceClaimIDs {
		placeholders[index] = "?"
		args = append(args, sourceID.String())
	}
	query := `SELECT evidence_id, weight FROM claim_evidence
		WHERE event_id = ? AND derivation = 'extracted' AND claim_id IN (` +
		strings.Join(placeholders, ",") + `)
		ORDER BY weight DESC, evidence_id ASC LIMIT 1`
	var bestRaw string
	var bestWeight int64
	if err := u.tx.QueryRowContext(ctx, query, args...).Scan(&bestRaw, &bestWeight); err != nil {
		return fmt.Errorf("sqlite: resolve highest-weight underlying evidence: %w", err)
	}
	if bestRaw != selectedID.String() || bestWeight != selectedWeight {
		return errors.New("sqlite: inherited evidence is not the highest-weight row for its underlying event")
	}
	return nil
}

func (u *canonicalUoW) insertDerivedClaim(
	ctx context.Context,
	value domain.LandDerivedClaim,
	identity claimMutationRecord,
) error {
	normalized, err := memory.NormalizeStatementV1(string(value.Statement.Bytes))
	if err != nil {
		return err
	}
	statementHash := canonical.HashBlob([]byte(normalized))
	var kind any
	if identity.Kind != memory.ClaimKindUnclassified {
		kind = string(identity.Kind)
	}
	m := u.metadata
	if _, err := u.tx.ExecContext(ctx, `INSERT INTO claims(
		claim_id, canonical_commit_id, owner_resident_id, subject_principal_id,
		perspective_principal_id, kind, temporal_kind, statement_content_id,
		statement_hash, statement_hash_algorithm, statement_normalization_version,
		created_by_run_id, recorded_at, recorded_tz
	) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 'sha256', ?, ?, ?, ?)`,
		value.ClaimID.String(), m.CommitID.String(), value.ResidentID.String(),
		identity.SubjectID.String(), identity.PerspectiveID.String(), kind,
		string(value.TemporalKind), value.Statement.ID.String(), statementHash.Bytes(),
		memory.NormalizationVersionV1, value.RunID.String(), m.CommittedAt.UnixMicro(),
		m.CommittedTZ.String()); err != nil {
		return fmt.Errorf("sqlite: insert derived claim: %w", err)
	}
	return nil
}

func (u *canonicalUoW) insertDerivedClaimEvidence(
	ctx context.Context,
	value domain.LandDerivedClaim,
	inherited derivedClaimEvidenceRecord,
	reason memory.EvidenceReason,
) error {
	m := u.metadata
	if _, err := u.tx.ExecContext(ctx, `INSERT INTO claim_evidence(
		evidence_id, canonical_commit_id, claim_id, event_id, polarity, grade,
		trust_level, weight, derivation, source_evidence_id, memory_policy_revision_id,
		created_by_run_id, reason_code, reason_content_id, recorded_at, recorded_tz
	) VALUES (?, ?, ?, ?, ?, 'inferred', ?, ?, 'inherited', ?, ?, ?, ?, NULL, ?, ?)`,
		inherited.landing.EvidenceID.String(), m.CommitID.String(), value.ClaimID.String(),
		inherited.eventID.String(), string(inherited.polarity), string(inherited.evaluated.Trust),
		inherited.evaluated.EffectiveWeight.Millionths(), inherited.landing.SourceEvidenceID.String(),
		value.MemoryPolicyRevisionID.String(), value.RunID.String(), string(reason),
		m.CommittedAt.UnixMicro(), m.CommittedTZ.String()); err != nil {
		return fmt.Errorf("sqlite: insert inherited claim evidence: %w", err)
	}
	return nil
}

func (u *canonicalUoW) insertInitialDerivedClaimState(
	ctx context.Context,
	value domain.LandDerivedClaim,
	scope memory.ViewScope,
	reason string,
	sourceCount, evidenceCount int,
) error {
	inheritedCount, err := canonical.NewCount(int64(evidenceCount))
	if err != nil {
		return err
	}
	sources, err := canonical.NewCount(int64(sourceCount))
	if err != nil {
		return err
	}
	gateMetrics, err := canonical.MarshalCanonical(struct {
		InheritedEvidence canonical.Count `json:"inherited_evidence_count"`
		SourceClaims      canonical.Count `json:"source_claim_count"`
	}{InheritedEvidence: inheritedCount, SourceClaims: sources})
	if err != nil {
		return err
	}
	m := u.metadata
	if _, err := u.tx.ExecContext(ctx, `INSERT INTO claim_stage_transitions(
		stage_transition_id, canonical_commit_id, claim_id, from_stage, to_stage,
		gate_metrics, pipeline_version_id, memory_policy_revision_id, generation_run_id,
		reason_code, reason_content_id, occurred_at, occurred_tz, recorded_at, recorded_tz
	) VALUES (?, ?, ?, NULL, 'floating', ?, ?, ?, ?, ?, NULL, ?, ?, ?, ?)`,
		value.InitialStageID.String(), m.CommitID.String(), value.ClaimID.String(),
		gateMetrics.String(), value.PipelineVersionID.String(), value.MemoryPolicyRevisionID.String(),
		value.RunID.String(), reason, m.CommittedAt.UnixMicro(), m.CommittedTZ.String(),
		m.CommittedAt.UnixMicro(), m.CommittedTZ.String()); err != nil {
		return fmt.Errorf("sqlite: insert initial derived claim stage: %w", err)
	}
	if _, err := u.tx.ExecContext(ctx, `INSERT INTO claim_view_scope_assertions(
		view_scope_assertion_id, canonical_commit_id, claim_id, view_scope,
		actor_principal_id, generation_run_id, memory_policy_revision_id, reason_code,
		reason_content_id, recorded_at, recorded_tz
	) VALUES (?, ?, ?, ?, ?, ?, ?, ?, NULL, ?, ?)`,
		value.InitialViewScopeID.String(), m.CommitID.String(), value.ClaimID.String(),
		string(scope), value.OwnerPrincipalID.String(), value.RunID.String(),
		value.MemoryPolicyRevisionID.String(), reason, m.CommittedAt.UnixMicro(),
		m.CommittedTZ.String()); err != nil {
		return fmt.Errorf("sqlite: insert initial derived claim scope: %w", err)
	}
	return nil
}

func (u *canonicalUoW) insertDerivedClaimRelation(
	ctx context.Context,
	value domain.LandDerivedClaim,
	source domain.DerivedClaimSource,
	operation derivedClaimOperation,
) error {
	m := u.metadata
	if _, err := u.tx.ExecContext(ctx, `INSERT INTO claim_relations(
		claim_relation_id, canonical_commit_id, from_claim_id, to_claim_id,
		relation_type, reason_code, reason_content_id, generation_run_id,
		occurred_at, occurred_tz, recorded_at, recorded_tz
	) VALUES (?, ?, ?, ?, ?, ?, NULL, ?, ?, ?, ?, ?)`, source.RelationID.String(),
		m.CommitID.String(), value.ClaimID.String(), source.ClaimID.String(),
		string(operation.relation), operation.reasonCode, value.RunID.String(),
		m.CommittedAt.UnixMicro(), m.CommittedTZ.String(), m.CommittedAt.UnixMicro(),
		m.CommittedTZ.String()); err != nil {
		return fmt.Errorf("sqlite: insert derived claim relation: %w", err)
	}
	return nil
}
