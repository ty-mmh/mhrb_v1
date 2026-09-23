package sqlite

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"mahoroba.local/mahoroba/internal/canonical"
	"mahoroba.local/mahoroba/internal/domain"
	"mahoroba.local/mahoroba/internal/memory"
)

type claimMutationRecord struct {
	ID            canonical.ID
	ResidentID    canonical.ID
	SubjectID     canonical.ID
	PerspectiveID canonical.ID
	Kind          memory.ClaimKind
}

type evaluatedClaimEvidence struct {
	ID      canonical.ID
	Value   memory.EvaluatedEvidence
	ActorID canonical.ID
}

func (u *canonicalUoW) AddClaimEvidence(
	ctx context.Context,
	value domain.AddClaimEvidence,
) (domain.AddClaimEvidenceResult, error) {
	return u.addClaimEvidence(ctx, value)
}

func (u *canonicalUoW) addClaimEvidence(
	ctx context.Context,
	value domain.AddClaimEvidence,
) (domain.AddClaimEvidenceResult, error) {
	return u.addClaimEvidenceWithPolicy(ctx, value, nil)
}

// addClaimEvidenceWithPolicy is used by atomic extraction landing. A nil
// policy enforces the public decision-time activation rule; a non-nil policy
// is the immutable policy already revalidated from the generation envelope.
func (u *canonicalUoW) addClaimEvidenceWithPolicy(
	ctx context.Context,
	value domain.AddClaimEvidence,
	recordedPolicy *memory.Policy,
) (domain.AddClaimEvidenceResult, error) {
	if err := u.requireResidentScope(value.ResidentID); err != nil {
		return domain.AddClaimEvidenceResult{}, err
	}
	claim, err := u.loadClaimForMutation(ctx, value.ResidentID, value.ClaimID)
	if err != nil {
		return domain.AddClaimEvidenceResult{}, err
	}
	status, err := u.currentClaimStatus(ctx, claim.ID)
	if err != nil {
		return domain.AddClaimEvidenceResult{}, err
	}
	if status != memory.StatusActive {
		return domain.AddClaimEvidenceResult{}, fmt.Errorf("sqlite: cannot add evidence to %s claim", status)
	}
	var hasExistingEvidence int
	if err := u.tx.QueryRowContext(ctx, `SELECT EXISTS(
		SELECT 1 FROM claim_evidence WHERE claim_id = ?
	)`, claim.ID.String()).Scan(&hasExistingEvidence); err != nil {
		return domain.AddClaimEvidenceResult{}, fmt.Errorf("sqlite: inspect claim evidence invariant: %w", err)
	}
	if hasExistingEvidence == 0 {
		return domain.AddClaimEvidenceResult{}, errors.New("sqlite: existing claim has no initial evidence")
	}
	var policy memory.Policy
	if recordedPolicy == nil {
		policy, err = u.loadDecisionMemoryPolicy(ctx, value.ResidentID, value.MemoryPolicyRevisionID)
		if err != nil {
			return domain.AddClaimEvidenceResult{}, err
		}
	} else {
		policy = *recordedPolicy
		if err := policy.RequireEnabled(); err != nil {
			return domain.AddClaimEvidenceResult{}, err
		}
	}
	if err := u.requireMemoryPipeline(ctx, value.MaturationPipelineVersionID,
		"memory_maturation", domain.MemoryMaturationPipelineVersion); err != nil {
		return domain.AddClaimEvidenceResult{}, err
	}
	if err := u.requireGenerationRunResident(ctx, value.CreatedByRunID, value.ResidentID); err != nil {
		return domain.AddClaimEvidenceResult{}, err
	}

	point, err := u.buildEvidencePoint(ctx, claim, value)
	if err != nil {
		return domain.AddClaimEvidenceResult{}, err
	}
	evaluated, err := memory.EvaluateEvidence(policy, point)
	if err != nil {
		return domain.AddClaimEvidenceResult{}, err
	}
	var duplicate int
	var source any
	if value.SourceEvidenceID != nil {
		source = value.SourceEvidenceID.String()
	}
	if err := u.tx.QueryRowContext(ctx, `SELECT EXISTS(
		SELECT 1 FROM claim_evidence
		WHERE claim_id = ? AND event_id = ? AND polarity = ? AND derivation = ?
		  AND COALESCE(source_evidence_id, '') = COALESCE(?, '')
	)`, claim.ID.String(), value.SourceEventID.String(), string(value.Polarity),
		string(value.Derivation), source).Scan(&duplicate); err != nil {
		return domain.AddClaimEvidenceResult{}, fmt.Errorf("sqlite: inspect duplicate claim evidence: %w", err)
	}
	if duplicate != 0 {
		return domain.AddClaimEvidenceResult{}, errors.New("sqlite: duplicate logical claim evidence")
	}

	m := u.metadata
	if _, err := u.tx.ExecContext(ctx, `INSERT INTO claim_evidence(
		evidence_id, canonical_commit_id, claim_id, event_id, polarity, grade,
		trust_level, weight, derivation, source_evidence_id, memory_policy_revision_id,
		created_by_run_id, reason_code, reason_content_id, recorded_at, recorded_tz
	) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, NULL, ?, ?)`,
		value.EvidenceID.String(), m.CommitID.String(), claim.ID.String(), value.SourceEventID.String(),
		string(value.Polarity), string(evaluated.EffectiveGrade), string(evaluated.Trust),
		evaluated.EffectiveWeight.Millionths(), string(value.Derivation), source,
		value.MemoryPolicyRevisionID.String(), value.CreatedByRunID.String(), string(value.Reason),
		m.CommittedAt.UnixMicro(), m.CommittedTZ.String(),
	); err != nil {
		return domain.AddClaimEvidenceResult{}, fmt.Errorf("sqlite: insert claim evidence: %w", err)
	}

	aggregate, _, err := u.aggregateClaimEvidence(ctx, policy, claim)
	if err != nil {
		return domain.AddClaimEvidenceResult{}, err
	}
	stage, err := u.currentClaimStage(ctx, claim.ID)
	if err != nil {
		return domain.AddClaimEvidenceResult{}, err
	}
	result := domain.AddClaimEvidenceResult{EvidenceID: value.EvidenceID, FinalStage: stage}
	for stage != memory.StageSettled {
		decision, err := memory.EvaluateMaturation(policy, memory.MaturationInput{
			CurrentStage: stage, Kind: claim.Kind, Evidence: aggregate,
		})
		if err != nil {
			return domain.AddClaimEvidenceResult{}, err
		}
		if !decision.Advance {
			break
		}
		transitionID := value.SedimentStageTransitionID
		if decision.To == memory.StageSettled {
			transitionID = value.SettledStageTransitionID
		}
		if decision.To == memory.StageSettled && claim.Kind == memory.ClaimKindDirect {
			return domain.AddClaimEvidenceResult{}, errors.New("sqlite: direct settlement requires memory_alignment landing")
		}
		if err := u.insertClaimStageTransition(ctx, transitionID, claim.ID, stage, decision,
			value.MaturationPipelineVersionID, value.MemoryPolicyRevisionID, value.CreatedByRunID); err != nil {
			return domain.AddClaimEvidenceResult{}, err
		}
		result.StageTransitions = append(result.StageTransitions, transitionID)
		stage = decision.To
		result.FinalStage = stage
	}
	return result, nil
}

func (u *canonicalUoW) HumanClaimStatusDecision(
	ctx context.Context,
	value domain.HumanClaimStatusDecision,
) (domain.ClaimStatusDecisionResult, error) {
	if err := u.requireResidentScope(value.ResidentID); err != nil {
		return domain.ClaimStatusDecisionResult{}, err
	}
	if err := u.requireOwnerHuman(ctx, value.ResidentID, value.OwnerPrincipalID); err != nil {
		return domain.ClaimStatusDecisionResult{}, err
	}
	claim, err := u.loadClaimForMutation(ctx, value.ResidentID, value.ClaimID)
	if err != nil {
		return domain.ClaimStatusDecisionResult{}, err
	}
	from, err := u.currentClaimStatus(ctx, claim.ID)
	if err != nil {
		return domain.ClaimStatusDecisionResult{}, err
	}
	if !allowedClaimStatusTransition(from, value.ToStatus) {
		return domain.ClaimStatusDecisionResult{}, fmt.Errorf("sqlite: invalid claim status transition %s -> %s", from, value.ToStatus)
	}
	var reasonContent any
	if value.ReasonContentID != nil {
		reasonContent = value.ReasonContentID.String()
	}
	m := u.metadata
	if _, err := u.tx.ExecContext(ctx, `INSERT INTO claim_status_transitions(
		status_transition_id, canonical_commit_id, claim_id, from_status, to_status,
		decision_kind, actor_principal_id, trigger_kind, trigger_event_id,
		trigger_evidence_id, trigger_claim_relation_id, trigger_integrity_finding_id,
		pipeline_version_id, memory_policy_revision_id, gate_metrics,
		decision_reason_code, decision_reason_content_id, occurred_at, occurred_tz,
		recorded_at, recorded_tz
	) VALUES (?, ?, ?, ?, ?, 'human', ?, NULL, NULL, NULL, NULL, NULL,
		NULL, NULL, NULL, ?, ?, ?, ?, ?, ?)`, value.StatusTransitionID.String(),
		m.CommitID.String(), claim.ID.String(), string(from), string(value.ToStatus),
		value.OwnerPrincipalID.String(), string(value.Reason), reasonContent,
		m.CommittedAt.UnixMicro(), m.CommittedTZ.String(), m.CommittedAt.UnixMicro(), m.CommittedTZ.String(),
	); err != nil {
		return domain.ClaimStatusDecisionResult{}, fmt.Errorf("sqlite: insert human claim status transition: %w", err)
	}
	return domain.ClaimStatusDecisionResult{
		TransitionID: value.StatusTransitionID, FromStatus: from, ToStatus: value.ToStatus,
	}, nil
}

func (u *canonicalUoW) AutomaticClaimStatusDecision(
	ctx context.Context,
	value domain.AutomaticClaimStatusDecision,
) (domain.ClaimStatusDecisionResult, error) {
	if err := u.requireResidentScope(value.ResidentID); err != nil {
		return domain.ClaimStatusDecisionResult{}, err
	}
	if err := u.requireMemoryPipeline(ctx, value.StatusPipelineVersionID,
		"memory_status", domain.MemoryStatusPipelineVersion); err != nil {
		return domain.ClaimStatusDecisionResult{}, err
	}
	claim, err := u.loadClaimForMutation(ctx, value.ResidentID, value.ClaimID)
	if err != nil {
		return domain.ClaimStatusDecisionResult{}, err
	}
	from, err := u.currentClaimStatus(ctx, claim.ID)
	if err != nil {
		return domain.ClaimStatusDecisionResult{}, err
	}
	if from != memory.StatusActive || !allowedClaimStatusTransition(from, value.ToStatus) {
		return domain.ClaimStatusDecisionResult{}, fmt.Errorf("sqlite: automatic transition requires active claim, got %s", from)
	}

	var policy memory.Policy
	if value.MemoryPolicyRevisionID != nil {
		policy, err = u.loadDecisionMemoryPolicy(ctx, value.ResidentID, *value.MemoryPolicyRevisionID)
		if err != nil {
			return domain.ClaimStatusDecisionResult{}, err
		}
	}
	gateMetrics, err := u.validateAutomaticStatusTrigger(ctx, value, claim, policy)
	if err != nil {
		return domain.ClaimStatusDecisionResult{}, err
	}
	if err := u.insertAutomaticStatusTransition(ctx, value.StatusTransitionID, claim.ID, from,
		value.ToStatus, value.Reason, value.TriggerKind, value.TriggerID,
		value.StatusPipelineVersionID, value.MemoryPolicyRevisionID, gateMetrics); err != nil {
		return domain.ClaimStatusDecisionResult{}, err
	}
	return domain.ClaimStatusDecisionResult{
		TransitionID: value.StatusTransitionID, FromStatus: from, ToStatus: value.ToStatus,
	}, nil
}

func (u *canonicalUoW) buildEvidencePoint(
	ctx context.Context,
	claim claimMutationRecord,
	value domain.AddClaimEvidence,
) (memory.EvidencePoint, error) {
	var eventTypeRaw, trustRaw, actorRaw string
	if err := u.tx.QueryRowContext(ctx, `SELECT event_type, trust_level, actor_principal_id
		FROM events WHERE event_id = ? AND resident_id = ?`, value.SourceEventID.String(),
		value.ResidentID.String()).Scan(&eventTypeRaw, &trustRaw, &actorRaw); err != nil {
		return memory.EvidencePoint{}, fmt.Errorf("sqlite: load claim evidence event: %w", err)
	}
	actorID, err := canonical.ParseID(actorRaw)
	if err != nil {
		return memory.EvidencePoint{}, err
	}
	if value.Derivation == memory.DerivationInherited {
		if value.SourceEvidenceID == nil {
			return memory.EvidencePoint{}, errors.New("sqlite: inherited evidence lacks source evidence")
		}
		var sourceEvent, sourceDerivation string
		if err := u.tx.QueryRowContext(ctx, `SELECT event_id, derivation FROM claim_evidence
			WHERE evidence_id = ?`, value.SourceEvidenceID.String()).Scan(&sourceEvent, &sourceDerivation); err != nil {
			return memory.EvidencePoint{}, fmt.Errorf("sqlite: load inherited source evidence: %w", err)
		}
		if sourceEvent != value.SourceEventID.String() || sourceDerivation != string(memory.DerivationExtracted) {
			return memory.EvidencePoint{}, errors.New("sqlite: inherited evidence must preserve a direct extracted source event")
		}
	}
	return memory.EvidencePoint{
		SourceEventID: value.SourceEventID, SourceEvidenceID: value.SourceEvidenceID,
		EventType: memory.EventType(eventTypeRaw), Polarity: value.Polarity, Grade: value.Grade,
		Trust: memory.TrustLevel(trustRaw), Derivation: value.Derivation,
		InheritanceDepth: func() int64 {
			if value.Derivation == memory.DerivationInherited {
				return 1
			}
			return 0
		}(),
		Reason: value.Reason, ActorIsSubject: actorID == claim.SubjectID,
		ActorIsPerspective: actorID == claim.PerspectiveID,
	}, nil
}

func (u *canonicalUoW) aggregateClaimEvidence(
	ctx context.Context,
	policy memory.Policy,
	claim claimMutationRecord,
) (memory.EvidenceAggregate, []evaluatedClaimEvidence, error) {
	rows, err := u.tx.QueryContext(ctx, `SELECT evidence.evidence_id, evidence.event_id,
		evidence.polarity, evidence.grade, evidence.trust_level, evidence.weight,
		evidence.derivation, evidence.source_evidence_id, evidence.reason_code,
		evidence.memory_policy_revision_id, policy_revision.resident_id,
		policy_revision.revision_class, policy_content.content_class,
		policy_content.erasure_state, policy_blob.content,
		event.event_type, event.trust_level, event.actor_principal_id,
		source.event_id, source.derivation
		FROM claim_evidence evidence
		JOIN events event ON event.event_id = evidence.event_id
		LEFT JOIN claim_evidence source ON source.evidence_id = evidence.source_evidence_id
		JOIN resident_revisions policy_revision
		  ON policy_revision.revision_id = evidence.memory_policy_revision_id
		JOIN content_objects policy_content ON policy_content.content_id = policy_revision.content_id
		LEFT JOIN blobs policy_blob
		  ON policy_blob.dedupe_scope_id = policy_content.owner_resident_id
		 AND policy_blob.hash_algorithm = policy_content.blob_hash_algorithm
		 AND policy_blob.blob_hash = policy_content.blob_hash
		JOIN canonical_commits commit_row ON commit_row.canonical_commit_id = evidence.canonical_commit_id
		WHERE evidence.claim_id = ?
		ORDER BY commit_row.commit_seq, evidence.evidence_id`, claim.ID.String())
	if err != nil {
		return memory.EvidenceAggregate{}, nil, fmt.Errorf("sqlite: load claim evidence aggregate: %w", err)
	}
	defer rows.Close()
	evaluatedRows := make([]evaluatedClaimEvidence, 0)
	for rows.Next() {
		var evidenceRaw, eventRaw, polarityRaw, gradeRaw, storedTrustRaw, derivationRaw, reasonRaw string
		var policyRaw, policyResidentRaw, policyRevisionClass, policyContentClass, policyErasureState string
		var policyContent []byte
		var sourceRaw, sourceEventRaw, sourceDerivationRaw sql.NullString
		var storedWeight int64
		var eventTypeRaw, eventTrustRaw, actorRaw string
		if err := rows.Scan(&evidenceRaw, &eventRaw, &polarityRaw, &gradeRaw, &storedTrustRaw,
			&storedWeight, &derivationRaw, &sourceRaw, &reasonRaw, &policyRaw, &policyResidentRaw,
			&policyRevisionClass, &policyContentClass, &policyErasureState, &policyContent,
			&eventTypeRaw, &eventTrustRaw, &actorRaw,
			&sourceEventRaw, &sourceDerivationRaw); err != nil {
			return memory.EvidenceAggregate{}, nil, fmt.Errorf("sqlite: scan claim evidence aggregate: %w", err)
		}
		evidenceID, err := canonical.ParseID(evidenceRaw)
		if err != nil {
			return memory.EvidenceAggregate{}, nil, err
		}
		eventID, err := canonical.ParseID(eventRaw)
		if err != nil {
			return memory.EvidenceAggregate{}, nil, err
		}
		actorID, err := canonical.ParseID(actorRaw)
		if err != nil {
			return memory.EvidenceAggregate{}, nil, err
		}
		if storedTrustRaw != eventTrustRaw {
			return memory.EvidenceAggregate{}, nil, fmt.Errorf("sqlite: evidence %s trust differs from source event", evidenceID)
		}
		policyID, err := canonical.ParseID(policyRaw)
		if err != nil {
			return memory.EvidenceAggregate{}, nil, err
		}
		if policyResidentRaw != claim.ResidentID.String() || policyRevisionClass != "memory_policy" ||
			policyContentClass != "memory_policy_text" || policyErasureState != "present" || policyContent == nil {
			return memory.EvidenceAggregate{}, nil, fmt.Errorf(
				"sqlite: evidence %s policy %s is unavailable or outside the claim resident", evidenceID, policyID,
			)
		}
		rowPolicy, _, err := memory.ParsePolicy(policyContent)
		if err != nil {
			return memory.EvidenceAggregate{}, nil, fmt.Errorf(
				"sqlite: parse evidence %s policy %s: %w", evidenceID, policyID, err,
			)
		}
		if err := rowPolicy.RequireEnabled(); err != nil {
			return memory.EvidenceAggregate{}, nil, fmt.Errorf(
				"sqlite: evidence %s policy %s cannot be replayed: %w", evidenceID, policyID, err,
			)
		}
		var sourceID *canonical.ID
		depth := int64(0)
		if sourceRaw.Valid {
			parsed, err := canonical.ParseID(sourceRaw.String)
			if err != nil {
				return memory.EvidenceAggregate{}, nil, err
			}
			sourceID = &parsed
			depth = 1
			if !sourceEventRaw.Valid || !sourceDerivationRaw.Valid || sourceEventRaw.String != eventRaw ||
				sourceDerivationRaw.String != string(memory.DerivationExtracted) {
				return memory.EvidenceAggregate{}, nil, errors.New("sqlite: inherited evidence provenance was laundered")
			}
		}
		point := memory.EvidencePoint{
			SourceEventID: eventID, SourceEvidenceID: sourceID, EventType: memory.EventType(eventTypeRaw),
			Polarity: memory.EvidencePolarity(polarityRaw), Grade: memory.EvidenceGrade(gradeRaw),
			Trust: memory.TrustLevel(eventTrustRaw), Derivation: memory.EvidenceDerivation(derivationRaw),
			InheritanceDepth: depth, Reason: memory.EvidenceReason(reasonRaw),
			ActorIsSubject: actorID == claim.SubjectID, ActorIsPerspective: actorID == claim.PerspectiveID,
		}
		value, err := memory.EvaluateEvidence(rowPolicy, point)
		if err != nil {
			return memory.EvidenceAggregate{}, nil, fmt.Errorf("sqlite: revalidate evidence %s: %w", evidenceID, err)
		}
		if value.EffectiveWeight.Millionths() != storedWeight || string(value.EffectiveGrade) != gradeRaw {
			return memory.EvidenceAggregate{}, nil, fmt.Errorf("sqlite: evidence %s pointwise policy result differs from stored row", evidenceID)
		}
		evaluatedRows = append(evaluatedRows, evaluatedClaimEvidence{ID: evidenceID, Value: value, ActorID: actorID})
	}
	if err := rows.Err(); err != nil {
		return memory.EvidenceAggregate{}, nil, fmt.Errorf("sqlite: iterate claim evidence aggregate: %w", err)
	}
	values := make([]memory.EvaluatedEvidence, len(evaluatedRows))
	for index := range evaluatedRows {
		values[index] = evaluatedRows[index].Value
	}
	aggregate, err := memory.AggregateEvidence(policy, values)
	if err != nil {
		return memory.EvidenceAggregate{}, nil, err
	}
	return aggregate, evaluatedRows, nil
}

func (u *canonicalUoW) insertClaimStageTransition(
	ctx context.Context,
	transitionID, claimID canonical.ID,
	from memory.ClaimStage,
	decision memory.MaturationDecision,
	pipelineID, policyID, runID canonical.ID,
) error {
	metrics, err := decision.Metrics.CanonicalJSON()
	if err != nil {
		return err
	}
	m := u.metadata
	if _, err := u.tx.ExecContext(ctx, `INSERT INTO claim_stage_transitions(
		stage_transition_id, canonical_commit_id, claim_id, from_stage, to_stage,
		gate_metrics, pipeline_version_id, memory_policy_revision_id, generation_run_id,
		reason_code, reason_content_id, occurred_at, occurred_tz, recorded_at, recorded_tz
	) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, NULL, ?, ?, ?, ?)`, transitionID.String(),
		m.CommitID.String(), claimID.String(), string(from), string(decision.To), metrics.String(),
		pipelineID.String(), policyID.String(), runID.String(), string(decision.Reason),
		m.CommittedAt.UnixMicro(), m.CommittedTZ.String(), m.CommittedAt.UnixMicro(), m.CommittedTZ.String(),
	); err != nil {
		return fmt.Errorf("sqlite: insert claim stage transition: %w", err)
	}
	return nil
}

func (u *canonicalUoW) validateAutomaticStatusTrigger(
	ctx context.Context,
	value domain.AutomaticClaimStatusDecision,
	claim claimMutationRecord,
	policy memory.Policy,
) (canonical.CanonicalJSON, error) {
	switch value.Reason {
	case memory.AutomaticReasonExplicitCorrection:
		stage, err := u.currentClaimStage(ctx, claim.ID)
		if err != nil {
			return canonical.CanonicalJSON{}, err
		}
		if claim.Kind == memory.ClaimKindDirect && stage == memory.StageSettled {
			return canonical.CanonicalJSON{}, errors.New("sqlite: settled direct claim rejects automatic explicit correction")
		}
		detail, err := u.loadTriggerEvidence(ctx, policy, claim, value.TriggerID)
		if err != nil {
			return canonical.CanonicalJSON{}, err
		}
		if detail.Value.Polarity != memory.PolarityContradict {
			return canonical.CanonicalJSON{}, errors.New("sqlite: explicit correction requires contradict evidence")
		}
		if err := u.requireKindSpecificActor(ctx, claim, detail, stage); err != nil {
			return canonical.CanonicalJSON{}, err
		}
		return canonical.MarshalCanonical(struct {
			ClaimKind  memory.ClaimKind  `json:"claim_kind"`
			ClaimStage memory.ClaimStage `json:"claim_stage"`
			EvidenceID canonical.ID      `json:"evidence_id"`
			EventType  memory.EventType  `json:"event_type"`
			ActorGate  bool              `json:"actor_gate"`
		}{claim.Kind, stage, value.TriggerID, detail.Value.EventType, true})
	case memory.AutomaticReasonExplicitSupersession:
		return u.validatePriorSupersession(ctx, policy, claim, value.TriggerID)
	case memory.AutomaticReasonStructuralQuarantine:
		var residentRaw, claimRaw, findingKind string
		if err := u.tx.QueryRowContext(ctx, `SELECT resident_id, claim_id, finding_kind
			FROM integrity_findings WHERE integrity_finding_id = ?`, value.TriggerID.String()).
			Scan(&residentRaw, &claimRaw, &findingKind); err != nil {
			return canonical.CanonicalJSON{}, fmt.Errorf("sqlite: load structural finding: %w", err)
		}
		if residentRaw != claim.ResidentID.String() || claimRaw != claim.ID.String() {
			return canonical.CanonicalJSON{}, errors.New("sqlite: structural finding target does not match transition claim")
		}
		return canonical.MarshalCanonical(struct {
			FindingID   canonical.ID `json:"finding_id"`
			FindingKind string       `json:"finding_kind"`
			TargetMatch bool         `json:"target_match"`
		}{value.TriggerID, findingKind, true})
	default:
		return canonical.CanonicalJSON{}, errors.New("sqlite: unsupported automatic claim status reason")
	}
}

func (u *canonicalUoW) loadTriggerEvidence(
	ctx context.Context,
	policy memory.Policy,
	claim claimMutationRecord,
	evidenceID canonical.ID,
) (evaluatedClaimEvidence, error) {
	_, details, err := u.aggregateClaimEvidence(ctx, policy, claim)
	if err != nil {
		return evaluatedClaimEvidence{}, err
	}
	for _, detail := range details {
		if detail.ID == evidenceID {
			return detail, nil
		}
	}
	return evaluatedClaimEvidence{}, errors.New("sqlite: trigger evidence does not belong to target claim")
}

func (u *canonicalUoW) requireKindSpecificActor(
	ctx context.Context,
	claim claimMutationRecord,
	evidence evaluatedClaimEvidence,
	stage memory.ClaimStage,
) error {
	switch claim.Kind {
	case memory.ClaimKindDirect:
		var residentPrincipalRaw string
		if err := u.tx.QueryRowContext(ctx, `SELECT principal_id FROM residents WHERE resident_id = ?`,
			claim.ResidentID.String()).Scan(&residentPrincipalRaw); err != nil {
			return err
		}
		if evidence.Value.EventType != memory.EventSelfTalk || evidence.ActorID.String() != residentPrincipalRaw ||
			stage == memory.StageSettled {
			return errors.New("sqlite: direct automatic semantic decision lacks owner-resident self-talk evidence")
		}
	case memory.ClaimKindOther:
		if evidence.Value.EventType != memory.EventUserMessage || evidence.ActorID != claim.SubjectID {
			return errors.New("sqlite: other automatic semantic decision lacks subject user message")
		}
	case memory.ClaimKindMeta:
		if evidence.Value.EventType != memory.EventUserMessage || evidence.ActorID != claim.PerspectiveID {
			return errors.New("sqlite: meta automatic semantic decision lacks perspective user message")
		}
	default:
		return errors.New("sqlite: unclassified claim requires a human semantic decision")
	}
	return nil
}

func (u *canonicalUoW) validatePriorSupersession(
	ctx context.Context,
	policy memory.Policy,
	oldClaim claimMutationRecord,
	relationID canonical.ID,
) (canonical.CanonicalJSON, error) {
	var fromRaw, toRaw, relationType string
	if err := u.tx.QueryRowContext(ctx, `SELECT from_claim_id, to_claim_id, relation_type
		FROM claim_relations WHERE claim_relation_id = ?`, relationID.String()).
		Scan(&fromRaw, &toRaw, &relationType); err != nil {
		return canonical.CanonicalJSON{}, fmt.Errorf("sqlite: load supersession relation: %w", err)
	}
	if toRaw != oldClaim.ID.String() || relationType != string(memory.RelationSupersedes) {
		return canonical.CanonicalJSON{}, errors.New("sqlite: explicit supersession trigger relation is not replacement -> target")
	}
	replacementID, err := canonical.ParseID(fromRaw)
	if err != nil {
		return canonical.CanonicalJSON{}, err
	}
	replacement, err := u.loadClaimForMutation(ctx, oldClaim.ResidentID, replacementID)
	if err != nil {
		return canonical.CanonicalJSON{}, err
	}
	if replacement.Kind != oldClaim.Kind || replacement.SubjectID != oldClaim.SubjectID ||
		replacement.PerspectiveID != oldClaim.PerspectiveID {
		return canonical.CanonicalJSON{}, errors.New("sqlite: supersession replacement identity mismatch")
	}
	oldStage, err := u.currentClaimStage(ctx, oldClaim.ID)
	if err != nil {
		return canonical.CanonicalJSON{}, err
	}
	if oldClaim.Kind == memory.ClaimKindDirect && oldStage == memory.StageSettled {
		return canonical.CanonicalJSON{}, errors.New("sqlite: prior-settled direct replacement requires human decision")
	}
	status, err := u.currentClaimStatus(ctx, replacement.ID)
	if err != nil {
		return canonical.CanonicalJSON{}, err
	}
	if status != memory.StatusActive {
		return canonical.CanonicalJSON{}, errors.New("sqlite: supersession replacement is not active")
	}
	_, evidence, err := u.aggregateClaimEvidence(ctx, policy, replacement)
	if err != nil {
		return canonical.CanonicalJSON{}, err
	}
	actorSatisfied := false
	for _, point := range evidence {
		if point.Value.Polarity != memory.PolaritySupport {
			continue
		}
		if err := u.requireKindSpecificActor(ctx, oldClaim, point, oldStage); err == nil {
			actorSatisfied = true
			break
		}
	}
	if !actorSatisfied {
		return canonical.CanonicalJSON{}, errors.New("sqlite: replacement lacks kind-specific direct support provenance")
	}
	return canonical.MarshalCanonical(struct {
		RelationID         canonical.ID      `json:"relation_id"`
		ReplacementClaimID canonical.ID      `json:"replacement_claim_id"`
		OldClaimKind       memory.ClaimKind  `json:"old_claim_kind"`
		OldClaimStage      memory.ClaimStage `json:"old_claim_stage"`
		ActorGate          bool              `json:"actor_gate"`
	}{relationID, replacement.ID, oldClaim.Kind, oldStage, true})
}

func (u *canonicalUoW) insertAutomaticStatusTransition(
	ctx context.Context,
	transitionID, claimID canonical.ID,
	from, to memory.ClaimStatus,
	reason memory.AutomaticDecisionReason,
	triggerKind domain.ClaimStatusTriggerKind,
	triggerID, pipelineID canonical.ID,
	policyID *canonical.ID,
	gateMetrics canonical.CanonicalJSON,
) error {
	var triggerEvidence, triggerRelation, triggerFinding any
	switch triggerKind {
	case domain.ClaimTriggerEvidence:
		triggerEvidence = triggerID.String()
	case domain.ClaimTriggerRelation:
		triggerRelation = triggerID.String()
	case domain.ClaimTriggerIntegrityFinding:
		triggerFinding = triggerID.String()
	default:
		return errors.New("sqlite: unsupported automatic status trigger")
	}
	var policy any
	if policyID != nil {
		policy = policyID.String()
	}
	m := u.metadata
	if _, err := u.tx.ExecContext(ctx, `INSERT INTO claim_status_transitions(
		status_transition_id, canonical_commit_id, claim_id, from_status, to_status,
		decision_kind, actor_principal_id, trigger_kind, trigger_event_id,
		trigger_evidence_id, trigger_claim_relation_id, trigger_integrity_finding_id,
		pipeline_version_id, memory_policy_revision_id, gate_metrics,
		decision_reason_code, decision_reason_content_id, occurred_at, occurred_tz,
		recorded_at, recorded_tz
	) VALUES (?, ?, ?, ?, ?, 'automatic', NULL, ?, NULL, ?, ?, ?, ?, ?, ?, ?, NULL, ?, ?, ?, ?)`,
		transitionID.String(), m.CommitID.String(), claimID.String(), string(from), string(to),
		string(triggerKind), triggerEvidence, triggerRelation, triggerFinding, pipelineID.String(), policy,
		gateMetrics.String(), string(reason), m.CommittedAt.UnixMicro(), m.CommittedTZ.String(),
		m.CommittedAt.UnixMicro(), m.CommittedTZ.String(),
	); err != nil {
		return fmt.Errorf("sqlite: insert automatic claim status transition: %w", err)
	}
	return nil
}

func (u *canonicalUoW) loadClaimForMutation(
	ctx context.Context,
	residentID, claimID canonical.ID,
) (claimMutationRecord, error) {
	var residentRaw, subjectRaw, perspectiveRaw string
	var kindRaw sql.NullString
	if err := u.tx.QueryRowContext(ctx, `SELECT owner_resident_id, subject_principal_id,
		perspective_principal_id, kind FROM claims WHERE claim_id = ?`, claimID.String()).
		Scan(&residentRaw, &subjectRaw, &perspectiveRaw, &kindRaw); err != nil {
		return claimMutationRecord{}, fmt.Errorf("sqlite: load claim: %w", err)
	}
	if residentRaw != residentID.String() {
		return claimMutationRecord{}, errors.New("sqlite: claim does not belong to resident")
	}
	subjectID, err := canonical.ParseID(subjectRaw)
	if err != nil {
		return claimMutationRecord{}, err
	}
	perspectiveID, err := canonical.ParseID(perspectiveRaw)
	if err != nil {
		return claimMutationRecord{}, err
	}
	kind := memory.ClaimKindUnclassified
	if kindRaw.Valid {
		kind = memory.ClaimKind(kindRaw.String)
	}
	if err := kind.Validate(); err != nil {
		return claimMutationRecord{}, err
	}
	return claimMutationRecord{
		ID: claimID, ResidentID: residentID, SubjectID: subjectID,
		PerspectiveID: perspectiveID, Kind: kind,
	}, nil
}

func (u *canonicalUoW) currentClaimStage(ctx context.Context, claimID canonical.ID) (memory.ClaimStage, error) {
	rows, err := u.tx.QueryContext(ctx, `SELECT transition.from_stage, transition.to_stage
		FROM claim_stage_transitions transition
		JOIN canonical_commits commit_row ON commit_row.canonical_commit_id = transition.canonical_commit_id
		WHERE transition.claim_id = ?
		ORDER BY commit_row.commit_seq, transition.stage_transition_id`, claimID.String())
	if err != nil {
		return "", err
	}
	defer rows.Close()
	var current memory.ClaimStage
	seen := false
	for rows.Next() {
		var from sql.NullString
		var toRaw string
		if err := rows.Scan(&from, &toRaw); err != nil {
			return "", err
		}
		to := memory.ClaimStage(toRaw)
		if err := to.Validate(); err != nil {
			return "", err
		}
		if !seen {
			if from.Valid || to != memory.StageFloating {
				return "", errors.New("sqlite: claim lacks valid initial floating transition")
			}
			current, seen = to, true
			continue
		}
		if !from.Valid || memory.ClaimStage(from.String) != current || !validStageAdvance(current, to) {
			return "", errors.New("sqlite: invalid claim stage transition history")
		}
		current = to
	}
	if err := rows.Err(); err != nil {
		return "", err
	}
	if !seen {
		return "", errors.New("sqlite: claim has no stage transition")
	}
	return current, nil
}

func (u *canonicalUoW) currentClaimStatus(ctx context.Context, claimID canonical.ID) (memory.ClaimStatus, error) {
	rows, err := u.tx.QueryContext(ctx, `SELECT transition.from_status, transition.to_status
		FROM claim_status_transitions transition
		JOIN canonical_commits commit_row ON commit_row.canonical_commit_id = transition.canonical_commit_id
		WHERE transition.claim_id = ?
		ORDER BY commit_row.commit_seq, transition.status_transition_id`, claimID.String())
	if err != nil {
		return "", err
	}
	defer rows.Close()
	current := memory.StatusActive
	for rows.Next() {
		var fromRaw, toRaw string
		if err := rows.Scan(&fromRaw, &toRaw); err != nil {
			return "", err
		}
		from, to := memory.ClaimStatus(fromRaw), memory.ClaimStatus(toRaw)
		if err := from.Validate(); err != nil {
			return "", err
		}
		if err := to.Validate(); err != nil {
			return "", err
		}
		if from != current || !allowedClaimStatusTransition(from, to) {
			return "", errors.New("sqlite: invalid claim status transition history")
		}
		current = to
	}
	if err := rows.Err(); err != nil {
		return "", err
	}
	return current, nil
}

func (u *canonicalUoW) loadDecisionMemoryPolicy(
	ctx context.Context,
	residentID, expectedRevisionID canonical.ID,
) (memory.Policy, error) {
	var revisionRaw string
	var content []byte
	if err := u.tx.QueryRowContext(ctx, `SELECT revision.revision_id, blob.content
		FROM resident_revision_activations activation
		JOIN canonical_commits commit_row ON commit_row.canonical_commit_id = activation.canonical_commit_id
		JOIN resident_revisions revision ON revision.revision_id = activation.revision_id
		JOIN content_objects content ON content.content_id = revision.content_id
		LEFT JOIN blobs blob ON blob.dedupe_scope_id = content.owner_resident_id
		 AND blob.hash_algorithm = content.blob_hash_algorithm AND blob.blob_hash = content.blob_hash
		WHERE activation.resident_id = ? AND revision.revision_class = 'memory_policy'
		ORDER BY commit_row.commit_seq DESC, activation.activation_id DESC LIMIT 1`, residentID.String()).
		Scan(&revisionRaw, &content); err != nil {
		return memory.Policy{}, fmt.Errorf("sqlite: resolve decision-time memory policy: %w", err)
	}
	if revisionRaw != expectedRevisionID.String() {
		return memory.Policy{}, errors.New("sqlite: supplied memory policy is not active at decision time")
	}
	if content == nil {
		return memory.Policy{}, errors.New("sqlite: decision-time memory policy content is erased")
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

func (u *canonicalUoW) requireMemoryPipeline(
	ctx context.Context,
	pipelineID canonical.ID,
	wantKind, wantVersion string,
) error {
	var kind, version string
	if err := u.tx.QueryRowContext(ctx, `SELECT pipeline_kind, version_key FROM pipeline_versions
		WHERE pipeline_version_id = ?`, pipelineID.String()).Scan(&kind, &version); err != nil {
		return fmt.Errorf("sqlite: load memory pipeline: %w", err)
	}
	if kind != wantKind || version != wantVersion {
		return fmt.Errorf("sqlite: pipeline %s is %s/%s, want %s/%s", pipelineID, kind, version, wantKind, wantVersion)
	}
	return nil
}

func (u *canonicalUoW) requireGenerationRunResident(
	ctx context.Context,
	runID, residentID canonical.ID,
) error {
	var residentRaw string
	if err := u.tx.QueryRowContext(ctx, `SELECT resident_id FROM generation_runs
		WHERE generation_run_id = ?`, runID.String()).Scan(&residentRaw); err != nil {
		return fmt.Errorf("sqlite: load evidence generation run: %w", err)
	}
	if residentRaw != residentID.String() {
		return errors.New("sqlite: evidence generation run belongs to another resident")
	}
	return nil
}

func allowedClaimStatusTransition(from, to memory.ClaimStatus) bool {
	switch from {
	case memory.StatusActive:
		return to == memory.StatusInvalidated || to == memory.StatusSuperseded || to == memory.StatusQuarantined
	case memory.StatusQuarantined:
		return to == memory.StatusActive || to == memory.StatusInvalidated || to == memory.StatusSuperseded
	default:
		return false
	}
}

func validStageAdvance(from, to memory.ClaimStage) bool {
	return (from == memory.StageFloating && to == memory.StageSediment) ||
		(from == memory.StageSediment && to == memory.StageSettled)
}
