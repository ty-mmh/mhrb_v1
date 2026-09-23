package sqlite

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"fmt"

	"mahoroba.local/mahoroba/internal/canonical"
	"mahoroba.local/mahoroba/internal/domain"
	"mahoroba.local/mahoroba/internal/memory"
)

func (u *canonicalUoW) LandMemoryExtraction(
	ctx context.Context,
	value domain.LandMemoryExtraction,
) (domain.MemoryExtractionLandingResult, error) {
	if err := u.requireResidentScope(value.ResidentID); err != nil {
		return domain.MemoryExtractionLandingResult{}, err
	}
	if err := u.requireActiveResident(ctx, value.ResidentID); err != nil {
		return domain.MemoryExtractionLandingResult{}, err
	}
	latestAttempt, state, err := u.latestOutcome(ctx, value.RunID, value.ResidentID)
	if err != nil {
		return domain.MemoryExtractionLandingResult{}, err
	}
	if state != "running" || latestAttempt != value.AttemptNo {
		return domain.MemoryExtractionLandingResult{}, fmt.Errorf(
			"sqlite: memory landing must terminate running attempt %d; got %s/%d",
			latestAttempt, state, value.AttemptNo,
		)
	}
	if err := u.requireExactMemoryPipeline(ctx, value.PipelineVersionID,
		"memory_extraction", domain.MemoryExtractionPipelineVersion); err != nil {
		return domain.MemoryExtractionLandingResult{}, err
	}
	if err := u.requireExactMemoryPipeline(ctx, value.MaturationPipelineVersionID,
		"memory_maturation", domain.MemoryMaturationPipelineVersion); err != nil {
		return domain.MemoryExtractionLandingResult{}, err
	}

	var purpose, idempotencyKey, runPipeline, runPolicy string
	var sessionPolicy sql.NullString
	if err := u.tx.QueryRowContext(ctx, `SELECT purpose, idempotency_key, pipeline_version_id,
		memory_policy_revision_id, sessionization_policy_version_id
		FROM generation_runs WHERE generation_run_id = ? AND resident_id = ?`,
		value.RunID.String(), value.ResidentID.String(),
	).Scan(&purpose, &idempotencyKey, &runPipeline, &runPolicy, &sessionPolicy); err != nil {
		return domain.MemoryExtractionLandingResult{}, fmt.Errorf("sqlite: load memory generation envelope: %w", err)
	}
	request, keyErr := domain.ParseMemoryExtractionObligation(idempotencyKey)
	if keyErr != nil || request.SourceEventID != value.SourceEventID ||
		purpose != string(domain.GenerationPurposeMemoryExtraction) ||
		runPipeline != value.PipelineVersionID.String() || runPolicy != value.MemoryPolicyRevisionID.String() ||
		sessionPolicy.Valid {
		return domain.MemoryExtractionLandingResult{}, errors.New("sqlite: memory generation envelope mismatch")
	}

	var eventTypeRaw, trustRaw, actorRaw, residentPrincipalRaw, erasureState string
	var sourceContent []byte
	if err := u.tx.QueryRowContext(ctx, `SELECT event.event_type, event.trust_level,
		event.actor_principal_id, resident.principal_id, content.erasure_state, blob.content
		FROM events event
		JOIN residents resident ON resident.resident_id = event.resident_id
		JOIN content_objects content ON content.content_id = event.content_id
		LEFT JOIN blobs blob ON blob.dedupe_scope_id = content.owner_resident_id
		 AND blob.hash_algorithm = content.blob_hash_algorithm AND blob.blob_hash = content.blob_hash
		WHERE event.event_id = ? AND event.resident_id = ?`,
		value.SourceEventID.String(), value.ResidentID.String(),
	).Scan(&eventTypeRaw, &trustRaw, &actorRaw, &residentPrincipalRaw, &erasureState, &sourceContent); err != nil {
		return domain.MemoryExtractionLandingResult{}, fmt.Errorf("sqlite: load memory source event: %w", err)
	}
	if erasureState != "present" || sourceContent == nil {
		return domain.MemoryExtractionLandingResult{}, errors.New("sqlite: memory source event content is unavailable")
	}
	eventType := memory.EventType(eventTypeRaw)
	if eventType != memory.EventUserMessage && eventType != memory.EventSelfTalk {
		return domain.MemoryExtractionLandingResult{}, errors.New("sqlite: resident-origin outbound events cannot be memory evidence")
	}

	var policyContent []byte
	if err := u.tx.QueryRowContext(ctx, `SELECT blob.content
		FROM resident_revisions revision
		JOIN content_objects content ON content.content_id = revision.content_id
		LEFT JOIN blobs blob ON blob.dedupe_scope_id = content.owner_resident_id
		 AND blob.hash_algorithm = content.blob_hash_algorithm AND blob.blob_hash = content.blob_hash
		WHERE revision.revision_id = ? AND revision.resident_id = ?
		  AND revision.revision_class = 'memory_policy'`,
		value.MemoryPolicyRevisionID.String(), value.ResidentID.String(),
	).Scan(&policyContent); err != nil {
		return domain.MemoryExtractionLandingResult{}, fmt.Errorf("sqlite: load recorded memory policy: %w", err)
	}
	if policyContent == nil {
		return domain.MemoryExtractionLandingResult{}, errors.New("sqlite: recorded memory policy content is erased")
	}
	policy, _, err := memory.ParsePolicy(policyContent)
	if err != nil {
		return domain.MemoryExtractionLandingResult{}, err
	}
	if err := policy.RequireEnabled(); err != nil {
		return domain.MemoryExtractionLandingResult{}, err
	}
	parsed, _, err := memory.ParseExtractionOutput(value.Output.Bytes, string(sourceContent))
	if err != nil {
		return domain.MemoryExtractionLandingResult{}, err
	}
	if len(parsed.Claims) != len(value.Claims) {
		return domain.MemoryExtractionLandingResult{}, errors.New("sqlite: parsed extraction count does not match landing IDs")
	}
	if err := u.insertContent(ctx, value.Output); err != nil {
		return domain.MemoryExtractionLandingResult{}, err
	}
	if err := u.insertOutcome(ctx, value.OutcomeID, value.RunID, value.AttemptNo, "succeeded", &value.Output.ID,
		value.PromptTokens, value.CompletionTokens, value.LatencyMicros, ""); err != nil {
		return domain.MemoryExtractionLandingResult{}, err
	}

	actorID, err := canonical.ParseID(actorRaw)
	if err != nil {
		return domain.MemoryExtractionLandingResult{}, err
	}
	residentPrincipalID, err := canonical.ParseID(residentPrincipalRaw)
	if err != nil {
		return domain.MemoryExtractionLandingResult{}, err
	}
	trust := memory.TrustLevel(trustRaw)
	result := domain.MemoryExtractionLandingResult{ClaimIDs: make([]canonical.ID, 0, len(parsed.Claims))}
	for index, extracted := range parsed.Claims {
		landing := value.Claims[index]
		claimID, err := u.landExtractedClaim(ctx, value, landing, extracted, policy, eventType,
			trust, actorID, residentPrincipalID)
		if err != nil {
			return domain.MemoryExtractionLandingResult{}, fmt.Errorf("sqlite: land extracted claim %d: %w", index, err)
		}
		result.ClaimIDs = append(result.ClaimIDs, claimID)
	}
	return result, nil
}

func (u *canonicalUoW) landExtractedClaim(
	ctx context.Context,
	command domain.LandMemoryExtraction,
	landing domain.ExtractedClaimLanding,
	extracted memory.ExtractionClaim,
	policy memory.Policy,
	eventType memory.EventType,
	trust memory.TrustLevel,
	actorID canonical.ID,
	residentPrincipalID canonical.ID,
) (canonical.ID, error) {
	if !bytes.Equal(landing.Statement.Bytes, []byte(extracted.Statement)) {
		return canonical.ID{}, errors.New("statement content differs from validated provider output")
	}
	subjectID := actorID
	if extracted.Subject == memory.SelectorResident {
		subjectID = residentPrincipalID
	}
	perspectiveID := actorID
	if extracted.Perspective == memory.SelectorResident {
		perspectiveID = residentPrincipalID
	}
	kind := memory.ClaimKindUnclassified
	switch {
	case subjectID == residentPrincipalID && perspectiveID == residentPrincipalID:
		kind = memory.ClaimKindDirect
	case subjectID != residentPrincipalID && perspectiveID == residentPrincipalID:
		kind = memory.ClaimKindOther
	case subjectID == residentPrincipalID && perspectiveID != residentPrincipalID:
		kind = memory.ClaimKindMeta
	}
	grade := extracted.Grade
	if eventType == memory.EventSelfTalk {
		// The provider never controls self-talk provenance. Policy v0 forces it
		// to inferred before both the reason code and weight are derived.
		grade = policy.Evidence.SelfTalkForcedGrade
	}
	reason := memory.EvidenceReasonSourceInferred
	if grade == memory.GradeStated {
		reason = memory.EvidenceReasonSourceStated
	}
	evaluated, err := memory.EvaluateEvidence(policy, memory.EvidencePoint{
		SourceEventID: command.SourceEventID, EventType: eventType,
		Polarity: memory.PolaritySupport, Grade: grade, Trust: trust,
		Derivation: memory.DerivationExtracted, Reason: reason,
		ActorIsSubject: actorID == subjectID, ActorIsPerspective: actorID == perspectiveID,
	})
	if err != nil {
		return canonical.ID{}, err
	}
	scope, err := memory.InitialScopeForEvent(policy, eventType)
	if err != nil {
		return canonical.ID{}, err
	}
	normalized, err := memory.NormalizeStatementV1(extracted.Statement)
	if err != nil {
		return canonical.ID{}, err
	}
	statementHash := canonical.HashBlob([]byte(normalized))

	var existingRaw string
	query := `SELECT claim.claim_id FROM claims claim
		JOIN content_objects content ON content.content_id = claim.statement_content_id
		WHERE claim.owner_resident_id = ? AND claim.subject_principal_id = ? AND claim.perspective_principal_id = ?
		  AND claim.temporal_kind = ? AND claim.statement_hash_algorithm = 'sha256'
		  AND claim.statement_hash IS NOT NULL AND content.erasure_state = 'present'
		  AND claim.statement_hash = ? AND claim.statement_normalization_version = ? AND `
	args := []any{command.ResidentID.String(), subjectID.String(), perspectiveID.String(),
		string(extracted.TemporalKind), statementHash.Bytes(), memory.NormalizationVersionV1}
	if kind == memory.ClaimKindUnclassified {
		query += `claim.kind IS NULL`
	} else {
		query += `claim.kind = ?`
		args = append(args, string(kind))
	}
	err = u.tx.QueryRowContext(ctx, query, args...).Scan(&existingRaw)
	if err == nil {
		existingID, parseErr := canonical.ParseID(existingRaw)
		if parseErr != nil {
			return canonical.ID{}, parseErr
		}
		var duplicate int
		if err := u.tx.QueryRowContext(ctx, `SELECT EXISTS(
			SELECT 1 FROM claim_evidence WHERE claim_id = ? AND event_id = ?
			  AND polarity = 'support' AND derivation = 'extracted'
			  AND source_evidence_id IS NULL
		)`, existingRaw, command.SourceEventID.String()).Scan(&duplicate); err != nil {
			return canonical.ID{}, err
		}
		if duplicate == 0 {
			_, err := u.addClaimEvidenceWithPolicy(ctx, domain.AddClaimEvidence{
				ResidentID: command.ResidentID, ClaimID: existingID, EvidenceID: landing.EvidenceID,
				SourceEventID: command.SourceEventID, Polarity: memory.PolaritySupport,
				Grade: grade, Derivation: memory.DerivationExtracted, Reason: reason,
				MemoryPolicyRevisionID: command.MemoryPolicyRevisionID, CreatedByRunID: command.RunID,
				MaturationPipelineVersionID: command.MaturationPipelineVersionID,
				SedimentStageTransitionID:   landing.SedimentStageTransitionID,
				SettledStageTransitionID:    landing.SettledStageTransitionID,
			}, &policy)
			if err != nil {
				return canonical.ID{}, err
			}
		}
		return existingID, nil
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return canonical.ID{}, err
	}

	if err := u.insertContent(ctx, landing.Statement); err != nil {
		return canonical.ID{}, err
	}
	m := u.metadata
	var kindValue any
	if kind != memory.ClaimKindUnclassified {
		kindValue = string(kind)
	}
	if _, err := u.tx.ExecContext(ctx, `INSERT INTO claims(
		claim_id, canonical_commit_id, owner_resident_id, subject_principal_id,
		perspective_principal_id, kind, temporal_kind, statement_content_id,
		statement_hash, statement_hash_algorithm, statement_normalization_version,
		created_by_run_id, recorded_at, recorded_tz
	) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 'sha256', ?, ?, ?, ?)`,
		landing.ClaimID.String(), m.CommitID.String(), command.ResidentID.String(), subjectID.String(),
		perspectiveID.String(), kindValue, string(extracted.TemporalKind), landing.Statement.ID.String(),
		statementHash.Bytes(), memory.NormalizationVersionV1, command.RunID.String(),
		m.CommittedAt.UnixMicro(), m.CommittedTZ.String(),
	); err != nil {
		return canonical.ID{}, fmt.Errorf("insert claim: %w", err)
	}
	if err := u.insertExtractedEvidence(ctx, command, landing.EvidenceID, landing.ClaimID, evaluated, reason); err != nil {
		return canonical.ID{}, err
	}
	gateMetrics, err := canonical.MarshalCanonical(struct {
		DistinctEvents canonical.Count  `json:"distinct_support_events"`
		SupportWeight  canonical.Weight `json:"support_weight"`
	}{DistinctEvents: 1, SupportWeight: evaluated.EffectiveWeight})
	if err != nil {
		return canonical.ID{}, err
	}
	if _, err := u.tx.ExecContext(ctx, `INSERT INTO claim_stage_transitions(
		stage_transition_id, canonical_commit_id, claim_id, from_stage, to_stage,
		gate_metrics, pipeline_version_id, memory_policy_revision_id, generation_run_id,
		reason_code, reason_content_id, occurred_at, occurred_tz, recorded_at, recorded_tz
	) VALUES (?, ?, ?, NULL, 'floating', ?, ?, ?, ?, ?, NULL, ?, ?, ?, ?)`,
		landing.InitialStageID.String(), m.CommitID.String(), landing.ClaimID.String(), gateMetrics.String(),
		command.PipelineVersionID.String(), command.MemoryPolicyRevisionID.String(), command.RunID.String(),
		string(memory.StageReasonInitialExtraction), m.CommittedAt.UnixMicro(), m.CommittedTZ.String(),
		m.CommittedAt.UnixMicro(), m.CommittedTZ.String(),
	); err != nil {
		return canonical.ID{}, fmt.Errorf("insert initial claim stage: %w", err)
	}
	if _, err := u.tx.ExecContext(ctx, `INSERT INTO claim_view_scope_assertions(
		view_scope_assertion_id, canonical_commit_id, claim_id, view_scope,
		actor_principal_id, generation_run_id, memory_policy_revision_id, reason_code,
		reason_content_id, recorded_at, recorded_tz
	) VALUES (?, ?, ?, ?, NULL, ?, ?, 'initial_extraction', NULL, ?, ?)`,
		landing.InitialViewScopeID.String(), m.CommitID.String(), landing.ClaimID.String(), string(scope),
		command.RunID.String(), command.MemoryPolicyRevisionID.String(),
		m.CommittedAt.UnixMicro(), m.CommittedTZ.String(),
	); err != nil {
		return canonical.ID{}, fmt.Errorf("insert initial claim view scope: %w", err)
	}
	return landing.ClaimID, nil
}

func (u *canonicalUoW) insertExtractedEvidence(
	ctx context.Context,
	command domain.LandMemoryExtraction,
	evidenceID, claimID canonical.ID,
	evaluated memory.EvaluatedEvidence,
	reason memory.EvidenceReason,
) error {
	m := u.metadata
	if _, err := u.tx.ExecContext(ctx, `INSERT INTO claim_evidence(
		evidence_id, canonical_commit_id, claim_id, event_id, polarity, grade,
		trust_level, weight, derivation, source_evidence_id, memory_policy_revision_id,
		created_by_run_id, reason_code, reason_content_id, recorded_at, recorded_tz
	) VALUES (?, ?, ?, ?, 'support', ?, ?, ?, 'extracted', NULL, ?, ?, ?, NULL, ?, ?)`,
		evidenceID.String(), m.CommitID.String(), claimID.String(), command.SourceEventID.String(),
		string(evaluated.EffectiveGrade), string(evaluated.Trust), evaluated.EffectiveWeight.Millionths(),
		command.MemoryPolicyRevisionID.String(), command.RunID.String(), string(reason),
		m.CommittedAt.UnixMicro(), m.CommittedTZ.String(),
	); err != nil {
		return fmt.Errorf("insert claim evidence: %w", err)
	}
	return nil
}
