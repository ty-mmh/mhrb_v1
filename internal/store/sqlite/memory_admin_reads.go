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

const memoryAdminClaimSelect = `SELECT
	claim.claim_id, claim.owner_resident_id, claim.subject_principal_id,
	claim.perspective_principal_id, COALESCE(claim.kind, ''), claim.temporal_kind,
	CASE WHEN statement.erasure_state <> 'present' OR claim.statement_hash IS NULL
		THEN 'erased' ELSE 'present' END, NULL,
	COALESCE((
		SELECT transition.to_stage
		FROM claim_stage_transitions transition
		JOIN canonical_commits transition_commit
		  ON transition_commit.canonical_commit_id = transition.canonical_commit_id
		WHERE transition.claim_id = claim.claim_id
		ORDER BY transition_commit.commit_seq DESC, transition.stage_transition_id DESC
		LIMIT 1
	), 'floating'),
	COALESCE((
		SELECT transition.to_status
		FROM claim_status_transitions transition
		JOIN canonical_commits transition_commit
		  ON transition_commit.canonical_commit_id = transition.canonical_commit_id
		WHERE transition.claim_id = claim.claim_id
		ORDER BY transition_commit.commit_seq DESC, transition.status_transition_id DESC
		LIMIT 1
	), 'active'),
	(
		SELECT assertion.view_scope
		FROM claim_view_scope_assertions assertion
		JOIN canonical_commits assertion_commit
		  ON assertion_commit.canonical_commit_id = assertion.canonical_commit_id
		WHERE assertion.claim_id = claim.claim_id
		ORDER BY assertion_commit.commit_seq DESC, assertion.view_scope_assertion_id DESC
		LIMIT 1
	),
	claim.created_by_run_id, generation.purpose, generation.pipeline_version_id,
	generation.memory_policy_revision_id, claim.recorded_at, claim.recorded_tz
FROM claims claim
JOIN content_objects statement ON statement.content_id = claim.statement_content_id
JOIN generation_runs generation ON generation.generation_run_id = claim.created_by_run_id
WHERE claim.owner_resident_id = ?`

func (r *CanonicalRepository) ListMemoryClaims(
	ctx context.Context,
	filter domain.MemoryClaimFilter,
) ([]domain.MemoryClaimSummary, error) {
	if err := filter.ResidentID.Validate(); err != nil {
		return nil, fmt.Errorf("sqlite: invalid memory claim resident: %w", err)
	}
	if filter.Stage != "" {
		if err := filter.Stage.Validate(); err != nil {
			return nil, err
		}
	}
	if filter.Status != "" {
		if err := filter.Status.Validate(); err != nil {
			return nil, err
		}
	}
	if filter.Scope != "" {
		if err := filter.Scope.Validate(); err != nil {
			return nil, err
		}
	}
	limit := filter.Limit
	if limit == 0 {
		limit = 100
	}
	if limit < 1 || limit > 1000 {
		return nil, fmt.Errorf("sqlite: memory claim limit must be between 1 and 1000")
	}

	rows, err := r.store.reader.QueryContext(ctx, memoryAdminClaimSelect+
		` ORDER BY claim.recorded_at DESC, claim.claim_id DESC`, filter.ResidentID.String())
	if err != nil {
		return nil, fmt.Errorf("sqlite: list memory claims: %w", err)
	}
	defer rows.Close()
	result := make([]domain.MemoryClaimSummary, 0, limit)
	for rows.Next() {
		claim, err := scanMemoryClaimSummary(rows)
		if err != nil {
			return nil, err
		}
		if filter.Stage != "" && claim.Stage != filter.Stage ||
			filter.Status != "" && claim.Status != filter.Status ||
			filter.Scope != "" && claim.Scope != filter.Scope {
			continue
		}
		result = append(result, claim)
		if len(result) == limit {
			break
		}
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("sqlite: list memory claims: %w", err)
	}
	return result, nil
}

func (r *CanonicalRepository) MemoryClaimProvenance(
	ctx context.Context,
	residentID canonical.ID,
	claimID canonical.ID,
) (domain.MemoryClaimProvenance, error) {
	if err := residentID.Validate(); err != nil {
		return domain.MemoryClaimProvenance{}, err
	}
	if err := claimID.Validate(); err != nil {
		return domain.MemoryClaimProvenance{}, err
	}
	row := r.store.reader.QueryRowContext(ctx, memoryAdminClaimSelect+` AND claim.claim_id = ?`,
		residentID.String(), claimID.String())
	claim, err := scanMemoryClaimSummary(row)
	if errors.Is(err, sql.ErrNoRows) {
		return domain.MemoryClaimProvenance{}, fmt.Errorf("sqlite: memory claim not found")
	}
	if err != nil {
		return domain.MemoryClaimProvenance{}, err
	}
	result := domain.MemoryClaimProvenance{Claim: claim}
	if result.Evidence, err = r.memoryClaimEvidence(ctx, claimID); err != nil {
		return domain.MemoryClaimProvenance{}, err
	}
	if result.StageTransitions, err = r.memoryClaimStages(ctx, claimID); err != nil {
		return domain.MemoryClaimProvenance{}, err
	}
	if result.StatusTransitions, err = r.memoryClaimStatuses(ctx, claimID); err != nil {
		return domain.MemoryClaimProvenance{}, err
	}
	if result.Relations, err = r.memoryClaimRelations(ctx, claimID); err != nil {
		return domain.MemoryClaimProvenance{}, err
	}
	if result.Usages, err = r.memoryClaimUsages(ctx, claimID); err != nil {
		return domain.MemoryClaimProvenance{}, err
	}
	return result, nil
}

type memoryClaimSummaryScanner interface {
	Scan(...any) error
}

func scanMemoryClaimSummary(scanner memoryClaimSummaryScanner) (domain.MemoryClaimSummary, error) {
	var result domain.MemoryClaimSummary
	var claimRaw, residentRaw, subjectRaw, perspectiveRaw string
	var kindRaw, temporalRaw, erasureState string
	var statement []byte
	var stageRaw, statusRaw string
	var scopeRaw sql.NullString
	var runRaw, purposeRaw, pipelineRaw, policyRaw string
	var recordedAt int64
	var recordedTZ string
	if err := scanner.Scan(
		&claimRaw, &residentRaw, &subjectRaw, &perspectiveRaw, &kindRaw, &temporalRaw,
		&erasureState, &statement, &stageRaw, &statusRaw, &scopeRaw,
		&runRaw, &purposeRaw, &pipelineRaw, &policyRaw, &recordedAt, &recordedTZ,
	); err != nil {
		return result, err
	}
	ids := []struct {
		raw string
		to  *canonical.ID
	}{
		{claimRaw, &result.ClaimID}, {residentRaw, &result.ResidentID},
		{subjectRaw, &result.SubjectPrincipalID}, {perspectiveRaw, &result.PerspectivePrincipalID},
		{runRaw, &result.CreatedByRunID}, {pipelineRaw, &result.CreatedByPipelineVersionID},
		{policyRaw, &result.CreatedByMemoryPolicyRevisionID},
	}
	for _, item := range ids {
		id, err := canonical.ParseID(item.raw)
		if err != nil {
			return result, fmt.Errorf("sqlite: decode memory claim identity: %w", err)
		}
		*item.to = id
	}
	result.Kind = memory.ClaimKind(kindRaw)
	result.TemporalKind = memory.TemporalKind(temporalRaw)
	result.Stage = memory.ClaimStage(stageRaw)
	result.Status = memory.ClaimStatus(statusRaw)
	if !scopeRaw.Valid {
		return result, fmt.Errorf("sqlite: claim %s has no view-scope assertion", result.ClaimID)
	}
	result.Scope = memory.ViewScope(scopeRaw.String)
	result.CreatedByPurpose = domain.GenerationPurpose(purposeRaw)
	for _, check := range []func() error{
		result.Kind.Validate, result.TemporalKind.Validate, result.Stage.Validate,
		result.Status.Validate, result.Scope.Validate, result.CreatedByPurpose.Validate,
	} {
		if err := check(); err != nil {
			return result, fmt.Errorf("sqlite: decode memory claim: %w", err)
		}
	}
	result.StatementErased = erasureState != "present"
	if result.StatementErased {
		result.Statement = "[erased]"
	} else {
		// Admin may inspect identity and history, but never receives claim
		// statement material. Keep the existing field for wire compatibility.
		result.Statement = "[redacted]"
	}
	tz, err := canonical.ParseTimezone(recordedTZ)
	if err != nil {
		return result, err
	}
	result.RecordedAt = canonical.Instant(recordedAt)
	result.RecordedTZ = tz
	return result, nil
}

func (r *CanonicalRepository) memoryClaimEvidence(ctx context.Context, claimID canonical.ID) ([]domain.MemoryEvidenceProvenance, error) {
	rows, err := r.store.reader.QueryContext(ctx, `SELECT
		evidence.evidence_id, evidence.event_id, event.event_type, event.actor_principal_id,
		event_content.erasure_state, event_blob.content,
		evidence.polarity, evidence.grade, evidence.trust_level, evidence.weight,
		evidence.derivation, evidence.source_evidence_id, evidence.memory_policy_revision_id,
		evidence.created_by_run_id, generation.purpose, generation.pipeline_version_id,
		evidence.reason_code, evidence.recorded_at, evidence.recorded_tz
		FROM claim_evidence evidence
		JOIN events event ON event.event_id = evidence.event_id
		JOIN content_objects event_content ON event_content.content_id = event.content_id
		LEFT JOIN blobs event_blob
		  ON event_blob.dedupe_scope_id = event_content.owner_resident_id
		 AND event_blob.hash_algorithm = event_content.blob_hash_algorithm
		 AND event_blob.blob_hash = event_content.blob_hash
		JOIN generation_runs generation ON generation.generation_run_id = evidence.created_by_run_id
		JOIN canonical_commits evidence_commit
		  ON evidence_commit.canonical_commit_id = evidence.canonical_commit_id
		WHERE evidence.claim_id = ?
		ORDER BY evidence_commit.commit_seq, evidence.evidence_id`, claimID.String())
	if err != nil {
		return nil, fmt.Errorf("sqlite: load claim evidence provenance: %w", err)
	}
	defer rows.Close()
	result := make([]domain.MemoryEvidenceProvenance, 0)
	for rows.Next() {
		var item domain.MemoryEvidenceProvenance
		var evidenceRaw, eventRaw, eventTypeRaw, actorRaw, erasureState string
		var eventContent []byte
		var polarityRaw, gradeRaw, trustRaw, derivationRaw string
		var sourceEvidenceRaw sql.NullString
		var policyRaw, runRaw, purposeRaw, pipelineRaw, reason, recordedTZ string
		var recordedAt int64
		if err := rows.Scan(
			&evidenceRaw, &eventRaw, &eventTypeRaw, &actorRaw, &erasureState, &eventContent,
			&polarityRaw, &gradeRaw, &trustRaw, &item.Weight, &derivationRaw,
			&sourceEvidenceRaw, &policyRaw, &runRaw, &purposeRaw, &pipelineRaw,
			&reason, &recordedAt, &recordedTZ,
		); err != nil {
			return nil, err
		}
		for _, value := range []struct {
			raw string
			to  *canonical.ID
		}{
			{evidenceRaw, &item.EvidenceID}, {eventRaw, &item.SourceEventID},
			{actorRaw, &item.SourceActorPrincipalID}, {policyRaw, &item.MemoryPolicyRevisionID},
			{runRaw, &item.CreatedByRunID}, {pipelineRaw, &item.PipelineVersionID},
		} {
			parsed, err := canonical.ParseID(value.raw)
			if err != nil {
				return nil, err
			}
			*value.to = parsed
		}
		if item.SourceEvidenceID, err = parseMemoryAdminOptionalID(sourceEvidenceRaw); err != nil {
			return nil, err
		}
		item.SourceEventType = memory.EventType(eventTypeRaw)
		item.Polarity = memory.EvidencePolarity(polarityRaw)
		item.Grade = memory.EvidenceGrade(gradeRaw)
		item.Trust = memory.TrustLevel(trustRaw)
		item.Derivation = memory.EvidenceDerivation(derivationRaw)
		item.CreatedByPurpose = domain.GenerationPurpose(purposeRaw)
		for _, check := range []func() error{
			item.SourceEventType.Validate, item.Polarity.Validate, item.Grade.Validate,
			item.Trust.Validate, item.Derivation.Validate, item.CreatedByPurpose.Validate,
		} {
			if err := check(); err != nil {
				return nil, err
			}
		}
		item.SourceContentErased = erasureState != "present" || eventContent == nil
		if item.SourceContentErased {
			item.SourceEventContent = "[erased]"
		} else {
			item.SourceEventContent = string(eventContent)
		}
		item.ReasonCode = reason
		item.RecordedAt = canonical.Instant(recordedAt)
		item.RecordedTZ, err = canonical.ParseTimezone(recordedTZ)
		if err != nil {
			return nil, err
		}
		result = append(result, item)
	}
	return result, rows.Err()
}

func (r *CanonicalRepository) memoryClaimStages(ctx context.Context, claimID canonical.ID) ([]domain.MemoryStageTransitionView, error) {
	rows, err := r.store.reader.QueryContext(ctx, `SELECT
		transition.stage_transition_id, transition.from_stage, transition.to_stage,
		transition.gate_metrics, transition.pipeline_version_id,
		transition.memory_policy_revision_id, transition.generation_run_id,
		transition.reason_code, transition.recorded_at, transition.recorded_tz
		FROM claim_stage_transitions transition
		JOIN canonical_commits transition_commit
		  ON transition_commit.canonical_commit_id = transition.canonical_commit_id
		WHERE transition.claim_id = ?
		ORDER BY transition_commit.commit_seq, transition.stage_transition_id`, claimID.String())
	if err != nil {
		return nil, fmt.Errorf("sqlite: load claim stage provenance: %w", err)
	}
	defer rows.Close()
	result := make([]domain.MemoryStageTransitionView, 0)
	for rows.Next() {
		var item domain.MemoryStageTransitionView
		var idRaw, toRaw, pipelineRaw, policyRaw, recordedTZ string
		var fromRaw, runRaw sql.NullString
		var recordedAt int64
		if err := rows.Scan(&idRaw, &fromRaw, &toRaw, &item.GateMetrics, &pipelineRaw,
			&policyRaw, &runRaw, &item.ReasonCode, &recordedAt, &recordedTZ); err != nil {
			return nil, err
		}
		var err error
		if item.TransitionID, err = canonical.ParseID(idRaw); err != nil {
			return nil, err
		}
		if item.PipelineVersionID, err = canonical.ParseID(pipelineRaw); err != nil {
			return nil, err
		}
		if item.MemoryPolicyRevisionID, err = canonical.ParseID(policyRaw); err != nil {
			return nil, err
		}
		if item.GenerationRunID, err = parseMemoryAdminOptionalID(runRaw); err != nil {
			return nil, err
		}
		if fromRaw.Valid {
			value := memory.ClaimStage(fromRaw.String)
			if err := value.Validate(); err != nil {
				return nil, err
			}
			item.FromStage = &value
		}
		item.ToStage = memory.ClaimStage(toRaw)
		if err := item.ToStage.Validate(); err != nil {
			return nil, err
		}
		item.RecordedAt = canonical.Instant(recordedAt)
		if item.RecordedTZ, err = canonical.ParseTimezone(recordedTZ); err != nil {
			return nil, err
		}
		result = append(result, item)
	}
	return result, rows.Err()
}

func (r *CanonicalRepository) memoryClaimStatuses(ctx context.Context, claimID canonical.ID) ([]domain.MemoryStatusTransitionView, error) {
	rows, err := r.store.reader.QueryContext(ctx, `SELECT
		transition.status_transition_id, transition.from_status, transition.to_status,
		transition.decision_kind, transition.actor_principal_id, transition.trigger_kind,
		transition.trigger_event_id, transition.trigger_evidence_id,
		transition.trigger_claim_relation_id, transition.trigger_integrity_finding_id,
		transition.pipeline_version_id, transition.memory_policy_revision_id,
		transition.gate_metrics, transition.decision_reason_code,
		transition.recorded_at, transition.recorded_tz
		FROM claim_status_transitions transition
		JOIN canonical_commits transition_commit
		  ON transition_commit.canonical_commit_id = transition.canonical_commit_id
		WHERE transition.claim_id = ?
		ORDER BY transition_commit.commit_seq, transition.status_transition_id`, claimID.String())
	if err != nil {
		return nil, fmt.Errorf("sqlite: load claim status provenance: %w", err)
	}
	defer rows.Close()
	result := make([]domain.MemoryStatusTransitionView, 0)
	for rows.Next() {
		var item domain.MemoryStatusTransitionView
		var idRaw, fromRaw, toRaw, recordedTZ string
		var actorRaw, triggerKind, eventRaw, evidenceRaw, relationRaw, findingRaw sql.NullString
		var pipelineRaw, policyRaw, gateMetrics sql.NullString
		var recordedAt int64
		if err := rows.Scan(
			&idRaw, &fromRaw, &toRaw, &item.DecisionKind, &actorRaw, &triggerKind,
			&eventRaw, &evidenceRaw, &relationRaw, &findingRaw, &pipelineRaw, &policyRaw,
			&gateMetrics, &item.ReasonCode, &recordedAt, &recordedTZ,
		); err != nil {
			return nil, err
		}
		var err error
		if item.TransitionID, err = canonical.ParseID(idRaw); err != nil {
			return nil, err
		}
		item.FromStatus, item.ToStatus = memory.ClaimStatus(fromRaw), memory.ClaimStatus(toRaw)
		if err := item.FromStatus.Validate(); err != nil {
			return nil, err
		}
		if err := item.ToStatus.Validate(); err != nil {
			return nil, err
		}
		if item.ActorPrincipalID, err = parseMemoryAdminOptionalID(actorRaw); err != nil {
			return nil, err
		}
		if triggerKind.Valid {
			item.TriggerKind = triggerKind.String
		}
		for _, raw := range []sql.NullString{eventRaw, evidenceRaw, relationRaw, findingRaw} {
			if raw.Valid {
				item.TriggerID, err = parseMemoryAdminOptionalID(raw)
				if err != nil {
					return nil, err
				}
				break
			}
		}
		if item.PipelineVersionID, err = parseMemoryAdminOptionalID(pipelineRaw); err != nil {
			return nil, err
		}
		if item.MemoryPolicyRevisionID, err = parseMemoryAdminOptionalID(policyRaw); err != nil {
			return nil, err
		}
		if gateMetrics.Valid {
			item.GateMetrics = gateMetrics.String
		}
		item.RecordedAt = canonical.Instant(recordedAt)
		if item.RecordedTZ, err = canonical.ParseTimezone(recordedTZ); err != nil {
			return nil, err
		}
		result = append(result, item)
	}
	return result, rows.Err()
}

func (r *CanonicalRepository) memoryClaimRelations(ctx context.Context, claimID canonical.ID) ([]domain.MemoryRelationView, error) {
	rows, err := r.store.reader.QueryContext(ctx, `SELECT
		relation.claim_relation_id, relation.from_claim_id, relation.to_claim_id,
		relation.relation_type, relation.generation_run_id, relation.reason_code,
		relation.recorded_at, relation.recorded_tz
		FROM claim_relations relation
		JOIN canonical_commits relation_commit
		  ON relation_commit.canonical_commit_id = relation.canonical_commit_id
		WHERE relation.from_claim_id = ? OR relation.to_claim_id = ?
		ORDER BY relation_commit.commit_seq, relation.claim_relation_id`, claimID.String(), claimID.String())
	if err != nil {
		return nil, fmt.Errorf("sqlite: load claim relations: %w", err)
	}
	defer rows.Close()
	result := make([]domain.MemoryRelationView, 0)
	for rows.Next() {
		var item domain.MemoryRelationView
		var idRaw, fromRaw, toRaw, typeRaw, recordedTZ string
		var runRaw sql.NullString
		var recordedAt int64
		if err := rows.Scan(&idRaw, &fromRaw, &toRaw, &typeRaw, &runRaw,
			&item.ReasonCode, &recordedAt, &recordedTZ); err != nil {
			return nil, err
		}
		var err error
		for _, value := range []struct {
			raw string
			to  *canonical.ID
		}{{idRaw, &item.RelationID}, {fromRaw, &item.FromClaimID}, {toRaw, &item.ToClaimID}} {
			parsed, parseErr := canonical.ParseID(value.raw)
			if parseErr != nil {
				return nil, parseErr
			}
			*value.to = parsed
		}
		if item.GenerationRunID, err = parseMemoryAdminOptionalID(runRaw); err != nil {
			return nil, err
		}
		item.RelationType = memory.RelationType(typeRaw)
		if err := item.RelationType.Validate(); err != nil {
			return nil, err
		}
		item.RecordedAt = canonical.Instant(recordedAt)
		if item.RecordedTZ, err = canonical.ParseTimezone(recordedTZ); err != nil {
			return nil, err
		}
		result = append(result, item)
	}
	return result, rows.Err()
}

func (r *CanonicalRepository) memoryClaimUsages(ctx context.Context, claimID canonical.ID) ([]domain.MemoryUsageView, error) {
	rows, err := r.store.reader.QueryContext(ctx, `SELECT
		usage.claim_usage_id, usage.recall_run_id, usage.generation_run_id,
		usage.usage_type, usage.ordinal, usage.memory_policy_revision_id,
		usage.exclusion_reason, usage.recorded_at, usage.recorded_tz
		FROM claim_usages usage
		JOIN canonical_commits usage_commit
		  ON usage_commit.canonical_commit_id = usage.canonical_commit_id
		WHERE usage.claim_id = ?
		ORDER BY usage_commit.commit_seq, usage.claim_usage_id`, claimID.String())
	if err != nil {
		return nil, fmt.Errorf("sqlite: load claim usages: %w", err)
	}
	defer rows.Close()
	result := make([]domain.MemoryUsageView, 0)
	for rows.Next() {
		var item domain.MemoryUsageView
		var idRaw, typeRaw, policyRaw, recordedTZ string
		var recallRaw, generationRaw, exclusionRaw sql.NullString
		var ordinal sql.NullInt64
		var recordedAt int64
		if err := rows.Scan(&idRaw, &recallRaw, &generationRaw, &typeRaw, &ordinal,
			&policyRaw, &exclusionRaw, &recordedAt, &recordedTZ); err != nil {
			return nil, err
		}
		var err error
		if item.UsageID, err = canonical.ParseID(idRaw); err != nil {
			return nil, err
		}
		if item.RecallRunID, err = parseMemoryAdminOptionalID(recallRaw); err != nil {
			return nil, err
		}
		if item.GenerationRunID, err = parseMemoryAdminOptionalID(generationRaw); err != nil {
			return nil, err
		}
		if item.MemoryPolicyRevisionID, err = canonical.ParseID(policyRaw); err != nil {
			return nil, err
		}
		item.UsageType = memory.UsageType(typeRaw)
		if err := item.UsageType.Validate(); err != nil {
			return nil, err
		}
		if ordinal.Valid {
			value := ordinal.Int64
			item.Ordinal = &value
		}
		if exclusionRaw.Valid {
			item.ExclusionReason = exclusionRaw.String
		}
		item.RecordedAt = canonical.Instant(recordedAt)
		if item.RecordedTZ, err = canonical.ParseTimezone(recordedTZ); err != nil {
			return nil, err
		}
		result = append(result, item)
	}
	return result, rows.Err()
}

func parseMemoryAdminOptionalID(raw sql.NullString) (*canonical.ID, error) {
	if !raw.Valid {
		return nil, nil
	}
	id, err := canonical.ParseID(raw.String)
	if err != nil {
		return nil, err
	}
	return &id, nil
}

func (r *CanonicalRepository) ListMemoryPersonaRevisions(
	ctx context.Context,
	residentID canonical.ID,
) ([]domain.MemoryPersonaRevisionView, error) {
	if err := residentID.Validate(); err != nil {
		return nil, err
	}
	var headRaw int64
	if err := r.store.reader.QueryRowContext(ctx, `SELECT COALESCE(MAX(commit_seq), 0)
		FROM canonical_commits WHERE resident_id = ?`, residentID.String()).Scan(&headRaw); err != nil {
		return nil, fmt.Errorf("sqlite: resolve persona Admin head: %w", err)
	}
	head, err := canonical.NewCommitSeq(headRaw)
	if err != nil {
		return nil, fmt.Errorf("sqlite: resident has no Canonical persona history")
	}
	active, err := r.ActiveRevisionAtHead(ctx, residentID, "persona", head)
	if err != nil {
		return nil, err
	}
	rows, err := r.store.reader.QueryContext(ctx, `SELECT
		revision.revision_id, revision.parent_revision_id,
		content.erasure_state, blob.content, revision.created_by_run_id,
		generation.pipeline_version_id, generation.memory_policy_revision_id,
		revision.recorded_at, revision.recorded_tz,
		(SELECT activation.activation_id
		 FROM resident_revision_activations activation
		 JOIN canonical_commits activation_commit
		   ON activation_commit.canonical_commit_id = activation.canonical_commit_id
		 WHERE activation.revision_id = revision.revision_id
		 ORDER BY activation_commit.commit_seq DESC, activation.activation_id DESC LIMIT 1),
		(SELECT activation.actor_principal_id
		 FROM resident_revision_activations activation
		 JOIN canonical_commits activation_commit
		   ON activation_commit.canonical_commit_id = activation.canonical_commit_id
		 WHERE activation.revision_id = revision.revision_id
		 ORDER BY activation_commit.commit_seq DESC, activation.activation_id DESC LIMIT 1),
		(SELECT activation.reason_code
		 FROM resident_revision_activations activation
		 JOIN canonical_commits activation_commit
		   ON activation_commit.canonical_commit_id = activation.canonical_commit_id
		 WHERE activation.revision_id = revision.revision_id
		 ORDER BY activation_commit.commit_seq DESC, activation.activation_id DESC LIMIT 1),
		(SELECT activation.recorded_at
		 FROM resident_revision_activations activation
		 JOIN canonical_commits activation_commit
		   ON activation_commit.canonical_commit_id = activation.canonical_commit_id
		 WHERE activation.revision_id = revision.revision_id
		 ORDER BY activation_commit.commit_seq DESC, activation.activation_id DESC LIMIT 1)
		FROM resident_revisions revision
		JOIN canonical_commits revision_commit
		  ON revision_commit.canonical_commit_id = revision.canonical_commit_id
		JOIN content_objects content ON content.content_id = revision.content_id
		LEFT JOIN blobs blob ON blob.dedupe_scope_id = content.owner_resident_id
		 AND blob.hash_algorithm = content.blob_hash_algorithm AND blob.blob_hash = content.blob_hash
		LEFT JOIN generation_runs generation ON generation.generation_run_id = revision.created_by_run_id
		WHERE revision.resident_id = ? AND revision.revision_class = 'persona'
		ORDER BY revision_commit.commit_seq DESC, revision.revision_id DESC`, residentID.String())
	if err != nil {
		return nil, fmt.Errorf("sqlite: list persona revisions: %w", err)
	}
	defer rows.Close()
	result := make([]domain.MemoryPersonaRevisionView, 0)
	for rows.Next() {
		var item domain.MemoryPersonaRevisionView
		var revisionRaw, erasureState, recordedTZ string
		var parentRaw, runRaw, pipelineRaw, policyRaw sql.NullString
		var activationRaw, actorRaw, reasonRaw sql.NullString
		var content []byte
		var recordedAt int64
		var activatedAt sql.NullInt64
		if err := rows.Scan(
			&revisionRaw, &parentRaw, &erasureState, &content, &runRaw, &pipelineRaw, &policyRaw,
			&recordedAt, &recordedTZ, &activationRaw, &actorRaw, &reasonRaw, &activatedAt,
		); err != nil {
			return nil, err
		}
		if item.RevisionID, err = canonical.ParseID(revisionRaw); err != nil {
			return nil, err
		}
		for _, optional := range []struct {
			raw    sql.NullString
			target **canonical.ID
		}{
			{parentRaw, &item.ParentRevisionID}, {runRaw, &item.CreatedByRunID},
			{pipelineRaw, &item.PipelineVersionID}, {policyRaw, &item.MemoryPolicyRevisionID},
			{activationRaw, &item.LatestActivationID}, {actorRaw, &item.LatestActivationActorID},
		} {
			if *optional.target, err = parseMemoryAdminOptionalID(optional.raw); err != nil {
				return nil, err
			}
		}
		item.ContentErased = erasureState != "present" || content == nil
		if item.ContentErased {
			item.Content = "[erased]"
		} else {
			item.Content = string(content)
		}
		item.Active = item.RevisionID == active.RevisionID
		if reasonRaw.Valid {
			item.LatestActivationReason = reasonRaw.String
		}
		if activatedAt.Valid {
			value := canonical.Instant(activatedAt.Int64)
			item.LatestActivatedAt = &value
		}
		item.RecordedAt = canonical.Instant(recordedAt)
		if item.RecordedTZ, err = canonical.ParseTimezone(recordedTZ); err != nil {
			return nil, err
		}
		result = append(result, item)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return result, nil
}

var _ domain.MemoryAdminRepository = (*CanonicalRepository)(nil)
