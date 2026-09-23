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

func (r *CanonicalRepository) DiscoverPersonaRevisionWork(
	ctx context.Context,
	residentID canonical.ID,
	maxAttempts int,
) (*domain.PersonaRevisionWork, error) {
	if err := residentID.Validate(); err != nil {
		return nil, err
	}
	if maxAttempts < 1 {
		return nil, errors.New("sqlite: persona max attempts must be positive")
	}
	tx, err := r.store.reader.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return nil, fmt.Errorf("sqlite: begin persona read snapshot: %w", err)
	}
	defer tx.Rollback()

	var status string
	err = tx.QueryRowContext(ctx, `SELECT transition.to_status
		FROM resident_status_transitions transition
		JOIN canonical_commits commit_row ON commit_row.canonical_commit_id = transition.canonical_commit_id
		WHERE transition.resident_id = ?
		ORDER BY commit_row.commit_seq DESC, transition.resident_status_transition_id DESC LIMIT 1`, residentID.String()).Scan(&status)
	if errors.Is(err, sql.ErrNoRows) || status != "active" {
		return nil, tx.Commit()
	}
	if err != nil {
		return nil, fmt.Errorf("sqlite: resolve persona resident status: %w", err)
	}

	var triggerRaw, triggerClaimRaw string
	err = tx.QueryRowContext(ctx, `SELECT transition.stage_transition_id, claim.claim_id
		FROM claim_stage_transitions transition
		JOIN claims claim ON claim.claim_id = transition.claim_id
		LEFT JOIN content_objects content ON content.content_id = claim.statement_content_id
		JOIN canonical_commits commit_row ON commit_row.canonical_commit_id = transition.canonical_commit_id
		WHERE claim.owner_resident_id = ? AND transition.to_stage = 'settled'
		  AND claim.statement_hash IS NOT NULL
		  AND (content.content_id IS NULL OR content.erasure_state = 'present')
		ORDER BY commit_row.commit_seq DESC, transition.stage_transition_id DESC LIMIT 1`, residentID.String()).Scan(
		&triggerRaw, &triggerClaimRaw,
	)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, tx.Commit()
	}
	if err != nil {
		return nil, fmt.Errorf("sqlite: discover persona trigger: %w", err)
	}
	triggerID, err := canonical.ParseID(triggerRaw)
	if err != nil {
		return nil, err
	}
	triggerClaimID, err := canonical.ParseID(triggerClaimRaw)
	if err != nil {
		return nil, err
	}
	if _, err := loadEligibleClaimStatement(ctx, tx, residentID, triggerClaimID); err != nil {
		return nil, fmt.Errorf("sqlite: validate persona trigger claim: %w", err)
	}

	work := &domain.PersonaRevisionWork{
		ResidentID: residentID, TriggerStageTransitionID: triggerID, State: domain.WorkPending,
	}
	if err := loadPersonaActiveRevisions(ctx, tx, work); err != nil {
		return nil, err
	}
	policy, _, err := memory.ParsePolicy([]byte(work.PolicyContent))
	if err != nil {
		return nil, fmt.Errorf("sqlite: parse active persona policy: %w", err)
	}
	if err := policy.RequireEnabled(); err != nil {
		return nil, tx.Commit()
	}

	var pipelineRaw, definitionRaw string
	if err := tx.QueryRowContext(ctx, `SELECT pipeline_version_id, definition FROM pipeline_versions
		WHERE pipeline_kind = 'persona_revision' AND version_key = ?`, domain.PersonaRevisionPipelineVersion).Scan(
		&pipelineRaw, &definitionRaw,
	); err != nil {
		return nil, fmt.Errorf("sqlite: resolve persona pipeline: %w", err)
	}
	work.PipelineVersionID, err = canonical.ParseID(pipelineRaw)
	if err != nil {
		return nil, err
	}
	expectedDefinition, err := canonical.MarshalCanonical(struct {
		Version string `json:"version"`
	}{Version: domain.PersonaRevisionPipelineVersion})
	if err != nil || definitionRaw != expectedDefinition.String() {
		return nil, errors.New("sqlite: persona pipeline definition mismatch")
	}

	if err := loadPersonaEligibleClaims(ctx, tx, work, policy.Persona.MaximumClaims); err != nil {
		return nil, err
	}
	if int64(len(work.Claims)) < policy.Persona.MinimumClaims {
		return nil, tx.Commit()
	}

	actionable, err := classifyPersonaRun(ctx, tx, work, maxAttempts)
	if err != nil {
		return nil, err
	}
	if !actionable {
		return nil, tx.Commit()
	}
	if err := tx.Commit(); err != nil {
		return nil, err
	}
	return work, nil
}

func classifyPersonaRun(
	ctx context.Context,
	tx *sql.Tx,
	work *domain.PersonaRevisionWork,
	maxAttempts int,
) (bool, error) {
	var runRaw string
	err := tx.QueryRowContext(ctx, `SELECT generation_run_id FROM generation_runs
		WHERE resident_id = ? AND idempotency_key = ?`, work.ResidentID.String(),
		domain.PersonaRevisionObligation(work.TriggerStageTransitionID)).Scan(&runRaw)
	if errors.Is(err, sql.ErrNoRows) {
		return true, nil
	}
	if err != nil {
		return false, fmt.Errorf("sqlite: resolve persona generation: %w", err)
	}
	runID, err := canonical.ParseID(runRaw)
	if err != nil {
		return false, err
	}
	work.RunID = &runID
	runSummary, err := (generationOutcomeRepository{}).Summary(ctx, tx, runID, work.ResidentID)
	if err != nil {
		return false, fmt.Errorf("sqlite: resolve persona outcome: %w", err)
	}
	runSummary, err = runSummary.Classify(int64(maxAttempts))
	if err != nil {
		return false, fmt.Errorf("sqlite: classify persona outcome: %w", err)
	}
	work.AttemptNo = runSummary.Latest.AttemptNo
	work.RetryCount = runSummary.RetryCount
	switch runSummary.ClassifiedState {
	case domain.WorkRunning:
		work.State = domain.WorkRunning
		return true, nil
	case domain.WorkSucceeded, domain.WorkTerminalFailed:
		return false, nil
	case domain.WorkRetryPending:
		if runSummary.LatestForegroundPreempted {
			work.ForegroundPreempted = true
			work.State = domain.WorkRetryPending
			return true, nil
		}
		if runSummary.RetryEligible {
			work.State = domain.WorkRetryPending
			return true, nil
		}
		return false, nil
	default:
		return false, fmt.Errorf("sqlite: unknown persona outcome %q", runSummary.PersistedState)
	}
}

func loadPersonaActiveRevisions(ctx context.Context, tx *sql.Tx, work *domain.PersonaRevisionWork) error {
	type active struct {
		class string
		id    *canonical.ID
		text  *string
	}
	for _, target := range []active{
		{class: "memory_policy", id: &work.PolicyRevisionID, text: &work.PolicyContent},
		{class: "persona", id: &work.CurrentPersonaRevisionID, text: &work.CurrentPersona},
	} {
		var rawID string
		var content []byte
		var activatedAt int64
		if err := tx.QueryRowContext(ctx, `SELECT revision.revision_id, blob.content, activation.recorded_at
			FROM resident_revision_activations activation
			JOIN resident_revisions revision ON revision.revision_id = activation.revision_id
			JOIN canonical_commits commit_row ON commit_row.canonical_commit_id = activation.canonical_commit_id
			JOIN content_objects content ON content.content_id = revision.content_id
			LEFT JOIN blobs blob ON blob.dedupe_scope_id = content.owner_resident_id
			 AND blob.hash_algorithm = content.blob_hash_algorithm AND blob.blob_hash = content.blob_hash
			WHERE activation.resident_id = ? AND revision.revision_class = ?
			ORDER BY commit_row.commit_seq DESC, activation.activation_id DESC LIMIT 1`, work.ResidentID.String(), target.class).Scan(
			&rawID, &content, &activatedAt,
		); err != nil {
			return fmt.Errorf("sqlite: resolve active persona %s: %w", target.class, err)
		}
		if content == nil {
			return fmt.Errorf("sqlite: active persona %s content is erased", target.class)
		}
		parsed, err := canonical.ParseID(rawID)
		if err != nil {
			return err
		}
		*target.id, *target.text = parsed, string(content)
		if target.class == "persona" {
			instant := canonical.Instant(activatedAt)
			work.LastActivationAt = &instant
		}
	}
	return nil
}

func loadPersonaEligibleClaims(ctx context.Context, tx *sql.Tx, work *domain.PersonaRevisionWork, limit int64) error {
	rows, err := tx.QueryContext(ctx, `SELECT claim.claim_id
		FROM claims claim
		JOIN claim_stage_transitions stage ON stage.claim_id = claim.claim_id
		JOIN canonical_commits stage_commit ON stage_commit.canonical_commit_id = stage.canonical_commit_id
		JOIN claim_view_scope_assertions scope ON scope.claim_id = claim.claim_id
		JOIN canonical_commits scope_commit ON scope_commit.canonical_commit_id = scope.canonical_commit_id
		LEFT JOIN content_objects content ON content.content_id = claim.statement_content_id
		WHERE claim.owner_resident_id = ? AND stage.to_stage = 'settled' AND scope.view_scope = 'resident_ui'
		  AND claim.statement_hash IS NOT NULL
		  AND (content.content_id IS NULL OR content.erasure_state = 'present')
		  AND NOT EXISTS (
			SELECT 1 FROM claim_stage_transitions newer
			JOIN canonical_commits newer_commit ON newer_commit.canonical_commit_id = newer.canonical_commit_id
			WHERE newer.claim_id = claim.claim_id AND
			 (newer_commit.commit_seq > stage_commit.commit_seq OR
				 (newer_commit.commit_seq = stage_commit.commit_seq AND newer.stage_transition_id > stage.stage_transition_id)))
		  AND NOT EXISTS (
			SELECT 1 FROM claim_view_scope_assertions newer
			JOIN canonical_commits newer_commit ON newer_commit.canonical_commit_id = newer.canonical_commit_id
			WHERE newer.claim_id = claim.claim_id AND
			 (newer_commit.commit_seq > scope_commit.commit_seq OR
				 (newer_commit.commit_seq = scope_commit.commit_seq AND newer.view_scope_assertion_id > scope.view_scope_assertion_id)))
		  AND COALESCE((
			SELECT status.to_status FROM claim_status_transitions status
			JOIN canonical_commits status_commit ON status_commit.canonical_commit_id = status.canonical_commit_id
			WHERE status.claim_id = claim.claim_id
			ORDER BY status_commit.commit_seq DESC, status.status_transition_id DESC LIMIT 1
		  ), 'active') = 'active'
			ORDER BY stage_commit.commit_seq DESC, stage.stage_transition_id DESC LIMIT ?`, work.ResidentID.String(), limit)
	if err != nil {
		return fmt.Errorf("sqlite: list persona source claims: %w", err)
	}
	defer rows.Close()
	var claimIDs []canonical.ID
	for rows.Next() {
		var rawID string
		if err := rows.Scan(&rawID); err != nil {
			return err
		}
		id, err := canonical.ParseID(rawID)
		if err != nil {
			return err
		}
		claimIDs = append(claimIDs, id)
	}
	if err := rows.Err(); err != nil {
		return err
	}
	if err := rows.Close(); err != nil {
		return err
	}
	for _, claimID := range claimIDs {
		eligible, err := loadEligibleClaimStatement(ctx, tx, work.ResidentID, claimID)
		if err != nil {
			return fmt.Errorf("sqlite: validate persona source claim %s: %w", claimID, err)
		}
		work.Claims = append(work.Claims, domain.PersonaSourceClaim{
			ClaimID: claimID, Statement: string(eligible.Statement),
		})
	}
	return nil
}

var _ domain.PersonaRevisionWorkRepository = (*CanonicalRepository)(nil)
