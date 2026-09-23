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

func (u *canonicalUoW) LandPersonaRevision(
	ctx context.Context,
	value domain.LandPersonaRevision,
) (domain.PersonaRevisionLandingResult, error) {
	if err := u.requireResidentScope(value.ResidentID); err != nil {
		return domain.PersonaRevisionLandingResult{}, err
	}
	if err := u.requireActiveResident(ctx, value.ResidentID); err != nil {
		return domain.PersonaRevisionLandingResult{}, err
	}
	latestAttempt, state, err := u.latestOutcome(ctx, value.RunID, value.ResidentID)
	if err != nil {
		return domain.PersonaRevisionLandingResult{}, err
	}
	if state != "running" || latestAttempt != value.AttemptNo {
		return domain.PersonaRevisionLandingResult{}, errors.New("sqlite: persona landing must terminate the latest running attempt")
	}

	var purpose, key, pipelineRaw, policyRaw, parentRaw string
	var asOf int64
	if err := u.tx.QueryRowContext(ctx, `SELECT purpose, idempotency_key, pipeline_version_id,
		memory_policy_revision_id, persona_revision_id, as_of FROM generation_runs
		WHERE generation_run_id = ? AND resident_id = ?`, value.RunID.String(), value.ResidentID.String()).Scan(
		&purpose, &key, &pipelineRaw, &policyRaw, &parentRaw, &asOf,
	); err != nil {
		return domain.PersonaRevisionLandingResult{}, fmt.Errorf("sqlite: load persona envelope: %w", err)
	}
	if purpose != string(domain.GenerationPurposePersonaRevision) ||
		key != domain.PersonaRevisionObligation(value.TriggerStageTransitionID) ||
		pipelineRaw != value.PipelineVersionID.String() || policyRaw != value.MemoryPolicyRevisionID.String() ||
		parentRaw != value.ParentPersonaRevisionID.String() {
		return domain.PersonaRevisionLandingResult{}, errors.New("sqlite: persona generation envelope mismatch")
	}

	var currentPersonaRaw string
	var previousPersona, policyContent []byte
	var lastActivationAt int64
	if err := u.tx.QueryRowContext(ctx, `SELECT revision.revision_id, blob.content, activation.recorded_at
		FROM resident_revision_activations activation
		JOIN resident_revisions revision ON revision.revision_id = activation.revision_id
		JOIN canonical_commits commit_row ON commit_row.canonical_commit_id = activation.canonical_commit_id
		JOIN content_objects content ON content.content_id = revision.content_id
		LEFT JOIN blobs blob ON blob.dedupe_scope_id = content.owner_resident_id
		 AND blob.hash_algorithm = content.blob_hash_algorithm AND blob.blob_hash = content.blob_hash
		WHERE activation.resident_id = ? AND revision.revision_class = 'persona'
		ORDER BY commit_row.commit_seq DESC, activation.activation_id DESC LIMIT 1`, value.ResidentID.String()).Scan(
		&currentPersonaRaw, &previousPersona, &lastActivationAt,
	); err != nil {
		return domain.PersonaRevisionLandingResult{}, fmt.Errorf("sqlite: resolve current persona: %w", err)
	}
	if currentPersonaRaw != parentRaw || previousPersona == nil {
		return domain.PersonaRevisionLandingResult{}, errors.New("sqlite: persona parent changed or was erased during generation")
	}
	var currentPolicyRaw string
	if err := u.tx.QueryRowContext(ctx, `SELECT revision.revision_id, blob.content
		FROM resident_revision_activations activation
		JOIN resident_revisions revision ON revision.revision_id = activation.revision_id
		JOIN canonical_commits commit_row ON commit_row.canonical_commit_id = activation.canonical_commit_id
		JOIN content_objects content ON content.content_id = revision.content_id
		LEFT JOIN blobs blob ON blob.dedupe_scope_id = content.owner_resident_id
		 AND blob.hash_algorithm = content.blob_hash_algorithm AND blob.blob_hash = content.blob_hash
		WHERE activation.resident_id = ? AND revision.revision_class = 'memory_policy'
		ORDER BY commit_row.commit_seq DESC, activation.activation_id DESC LIMIT 1`, value.ResidentID.String()).Scan(
		&currentPolicyRaw, &policyContent,
	); err != nil {
		return domain.PersonaRevisionLandingResult{}, fmt.Errorf("sqlite: resolve current persona policy: %w", err)
	}
	if currentPolicyRaw != policyRaw || policyContent == nil {
		return domain.PersonaRevisionLandingResult{}, errors.New("sqlite: persona policy changed or was erased during generation")
	}
	policy, _, err := memory.ParsePolicy(policyContent)
	if err != nil {
		return domain.PersonaRevisionLandingResult{}, err
	}
	if err := policy.RequireEnabled(); err != nil {
		return domain.PersonaRevisionLandingResult{}, err
	}
	parsed, _, err := memory.ParsePersonaRevisionOutput(value.Output.Bytes)
	if err != nil {
		return domain.PersonaRevisionLandingResult{}, err
	}
	if !bytes.Equal(value.PersonaContent.Bytes, []byte(parsed.Persona)) {
		return domain.PersonaRevisionLandingResult{}, errors.New("sqlite: persona content differs from validated output")
	}

	claimCount, err := u.validatePersonaSources(ctx, value.RunID, value.ResidentID)
	if err != nil {
		return domain.PersonaRevisionLandingResult{}, err
	}
	metrics, err := memory.MeasurePersonaEdit(string(previousPersona), parsed.Persona)
	if err != nil {
		return domain.PersonaRevisionLandingResult{}, err
	}
	lastActivation := canonical.Instant(lastActivationAt)
	decision, err := memory.EvaluatePersonaThreshold(policy, memory.PersonaThresholdInput{
		ClaimCount: canonical.Count(claimCount), ChangedBytes: metrics.ChangedBytes,
		ChangedRatio: metrics.ChangedRatio, ChangedLines: metrics.ChangedLines, TotalBytes: metrics.TotalBytes,
		AsOf: u.metadata.CommittedAt, LastActivationAt: &lastActivation,
	})
	if err != nil {
		return domain.PersonaRevisionLandingResult{}, err
	}
	if parsed.Contradiction {
		decision.Eligible = false
		decision.BlockingReasons = append(decision.BlockingReasons, memory.PersonaContradiction)
	}

	if err := u.insertContent(ctx, value.Output); err != nil {
		return domain.PersonaRevisionLandingResult{}, err
	}
	if err := u.insertContent(ctx, value.PersonaContent); err != nil {
		return domain.PersonaRevisionLandingResult{}, err
	}
	if err := u.insertOutcome(ctx, value.OutcomeID, value.RunID, value.AttemptNo, "succeeded", &value.Output.ID,
		value.PromptTokens, value.CompletionTokens, value.LatencyMicros, ""); err != nil {
		return domain.PersonaRevisionLandingResult{}, err
	}
	m := u.metadata
	if _, err := u.tx.ExecContext(ctx, `INSERT INTO resident_revisions(
		revision_id, canonical_commit_id, resident_id, revision_class, content_id,
		parent_revision_id, created_by_run_id, reason_content_id, recorded_at, recorded_tz
	) VALUES (?, ?, ?, 'persona', ?, ?, ?, NULL, ?, ?)`,
		value.PersonaRevisionID.String(), m.CommitID.String(), value.ResidentID.String(),
		value.PersonaContent.ID.String(), value.ParentPersonaRevisionID.String(), value.RunID.String(),
		m.CommittedAt.UnixMicro(), m.CommittedTZ.String(),
	); err != nil {
		return domain.PersonaRevisionLandingResult{}, fmt.Errorf("sqlite: insert persona revision: %w", err)
	}
	result := domain.PersonaRevisionLandingResult{RevisionID: value.PersonaRevisionID}
	for _, reason := range decision.BlockingReasons {
		result.BlockingReasons = append(result.BlockingReasons, string(reason))
	}
	if decision.Eligible {
		var actorRaw string
		if err := u.tx.QueryRowContext(ctx, `SELECT principal_id FROM residents WHERE resident_id = ?`, value.ResidentID.String()).Scan(&actorRaw); err != nil {
			return domain.PersonaRevisionLandingResult{}, err
		}
		if _, err := u.tx.ExecContext(ctx, `INSERT INTO resident_revision_activations(
			activation_id, canonical_commit_id, resident_id, revision_id, actor_principal_id,
			approval_id, reason_code, reason_content_id, recorded_at, recorded_tz
		) VALUES (?, ?, ?, ?, ?, NULL, 'persona_auto_activation', NULL, ?, ?)`,
			value.PersonaActivationID.String(), m.CommitID.String(), value.ResidentID.String(),
			value.PersonaRevisionID.String(), actorRaw, m.CommittedAt.UnixMicro(), m.CommittedTZ.String(),
		); err != nil {
			return domain.PersonaRevisionLandingResult{}, fmt.Errorf("sqlite: activate persona revision: %w", err)
		}
		activationID := value.PersonaActivationID
		result.ActivationID, result.AutoActivated = &activationID, true
	}
	_ = asOf // The frozen request time remains part of the Canonical envelope.
	return result, nil
}

func (u *canonicalUoW) validatePersonaSources(ctx context.Context, runID, residentID canonical.ID) (int64, error) {
	rows, err := u.tx.QueryContext(ctx, `SELECT input.source_type, input.source_id
		FROM generation_run_inputs input WHERE input.generation_run_id = ? ORDER BY input.ordinal`, runID.String())
	if err != nil {
		return 0, err
	}
	defer rows.Close()
	var claimCount, revisionCount int64
	for rows.Next() {
		var sourceType string
		var sourceRaw sql.NullString
		if err := rows.Scan(&sourceType, &sourceRaw); err != nil {
			return 0, err
		}
		if !sourceRaw.Valid {
			return 0, errors.New("sqlite: persona input lacks Canonical source")
		}
		switch sourceType {
		case "resident_revision":
			revisionCount++
		case "claim":
			claimCount++
			claimID, parseErr := canonical.ParseID(sourceRaw.String)
			if parseErr != nil {
				return 0, parseErr
			}
			if _, err := loadEligibleClaimStatement(ctx, u.tx, residentID, claimID); err != nil {
				return 0, fmt.Errorf("sqlite: revalidate persona source: %w", err)
			}
			var owner, stage, scope, status string
			if err := u.tx.QueryRowContext(ctx, `SELECT claim.owner_resident_id,
				(SELECT transition.to_stage FROM claim_stage_transitions transition
				 JOIN canonical_commits commit_row ON commit_row.canonical_commit_id = transition.canonical_commit_id
				 WHERE transition.claim_id = claim.claim_id ORDER BY commit_row.commit_seq DESC, transition.stage_transition_id DESC LIMIT 1),
				(SELECT assertion.view_scope FROM claim_view_scope_assertions assertion
				 JOIN canonical_commits commit_row ON commit_row.canonical_commit_id = assertion.canonical_commit_id
				 WHERE assertion.claim_id = claim.claim_id ORDER BY commit_row.commit_seq DESC, assertion.view_scope_assertion_id DESC LIMIT 1),
				COALESCE((SELECT transition.to_status FROM claim_status_transitions transition
				 JOIN canonical_commits commit_row ON commit_row.canonical_commit_id = transition.canonical_commit_id
				 WHERE transition.claim_id = claim.claim_id ORDER BY commit_row.commit_seq DESC, transition.status_transition_id DESC LIMIT 1), 'active')
				FROM claims claim
				JOIN content_objects content ON content.content_id = claim.statement_content_id
				WHERE claim.claim_id = ? AND content.erasure_state = 'present'
				  AND claim.statement_hash IS NOT NULL`, sourceRaw.String).Scan(&owner, &stage, &scope, &status); err != nil {
				if errors.Is(err, sql.ErrNoRows) {
					return 0, fmt.Errorf("%w: persona claim source %s is erased or ineligible", domain.ErrClaimSourceIneligible, sourceRaw.String)
				}
				return 0, fmt.Errorf("sqlite: revalidate persona source: %w", err)
			}
			if owner != residentID.String() || stage != "settled" || scope != "resident_ui" || status != "active" {
				return 0, errors.New("sqlite: persona source is no longer active settled resident_ui")
			}
		default:
			return 0, fmt.Errorf("sqlite: persona input uses forbidden source type %q", sourceType)
		}
	}
	if err := rows.Err(); err != nil {
		return 0, err
	}
	if revisionCount != 2 || claimCount < 2 || claimCount > 8 {
		return 0, errors.New("sqlite: persona requires exactly current persona and policy plus 2..8 claims")
	}
	return claimCount, nil
}
