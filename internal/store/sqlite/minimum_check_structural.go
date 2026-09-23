package sqlite

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"mahoroba.local/mahoroba/internal/canonical"
	"mahoroba.local/mahoroba/internal/domain"
	"mahoroba.local/mahoroba/internal/integrity"
)

// ValidateMinimumStructure performs the relational checks which must finish
// before a Writer, listener, or artifact publisher is allowed to proceed. All
// database observations are made through one read-only transaction. Returned
// FatalErrors contain identifiers and stable reason codes only; no statement
// bytes, hashes, salts, or filesystem locators cross this boundary.
func (source *minimumCheckSource) ValidateMinimumStructure(
	ctx context.Context,
) (resultErr error) {
	if source == nil || source.reader == nil {
		return errors.New("sqlite: MinimumCheck reader is required")
	}
	tx, err := source.reader.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return fmt.Errorf("sqlite: begin structural MinimumCheck snapshot: %w", err)
	}
	defer func() {
		if resultErr != nil {
			_ = tx.Rollback()
		}
	}()

	if err := validateMinimumCommitSequence(ctx, tx); err != nil {
		return err
	}
	if err := validateMinimumContentErasure(ctx, tx); err != nil {
		return err
	}
	if err := validateMinimumPresentClaims(ctx, tx); err != nil {
		return err
	}
	if err := validateMinimumRuntimeConfiguration(ctx, tx); err != nil {
		return err
	}
	if err := validateMinimumGenerationHistories(ctx, tx); err != nil {
		return err
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("sqlite: finish structural MinimumCheck snapshot: %w", err)
	}
	return nil
}

func validateMinimumRuntimeConfiguration(ctx context.Context, tx *sql.Tx) error {
	var residentRaw, policyRaw sql.NullString
	err := tx.QueryRowContext(ctx, `SELECT active_resident_id,
		desired_sessionization_policy_version_id
		FROM runtime_config WHERE singleton_id = 1`).Scan(&residentRaw, &policyRaw)
	if errors.Is(err, sql.ErrNoRows) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("sqlite: inspect runtime configuration: %w", err)
	}
	if !residentRaw.Valid {
		if policyRaw.Valid {
			return minimumFatal(integrity.RuntimeConfigurationInvalid,
				"runtime_config", "1", "session policy is selected without a resident")
		}
		return nil
	}
	residentID, err := canonical.ParseID(residentRaw.String)
	if err != nil {
		return minimumFatal(integrity.RuntimeConfigurationInvalid,
			"runtime_config", "1", "selected resident identity is invalid")
	}
	var residentExists int
	if err := tx.QueryRowContext(ctx, `SELECT EXISTS(
		SELECT 1 FROM residents WHERE resident_id = ?
	)`, residentID.String()).Scan(&residentExists); err != nil {
		return fmt.Errorf("sqlite: resolve runtime resident selection: %w", err)
	}
	if residentExists == 0 {
		return minimumFatal(integrity.RuntimeConfigurationInvalid,
			"resident", residentID.String(), "runtime selection references a missing resident")
	}
	if !policyRaw.Valid {
		return nil
	}
	policyID, err := canonical.ParseID(policyRaw.String)
	if err != nil {
		return minimumFatal(integrity.RuntimeConfigurationInvalid,
			"runtime_config", "1", "selected session policy identity is invalid")
	}
	var versionKey, definitionRaw string
	err = tx.QueryRowContext(ctx, `SELECT version_key, definition
		FROM sessionization_policy_versions
		WHERE sessionization_policy_version_id = ?`, policyID.String()).Scan(&versionKey, &definitionRaw)
	if errors.Is(err, sql.ErrNoRows) {
		return minimumFatal(integrity.RuntimeConfigurationInvalid,
			"sessionization_policy_version", policyID.String(), "runtime selection references a missing policy")
	}
	if err != nil {
		return fmt.Errorf("sqlite: resolve runtime session policy: %w", err)
	}
	if versionKey != domain.SessionPolicyVersion || !validReadinessSessionDefinition(definitionRaw) {
		return minimumFatal(integrity.RuntimeConfigurationInvalid,
			"sessionization_policy_version", policyID.String(), "runtime policy has an unsupported kind or definition")
	}
	return nil
}

func validateMinimumCommitSequence(ctx context.Context, tx *sql.Tx) error {
	var count, minimum, maximum int64
	if err := tx.QueryRowContext(ctx, `SELECT COUNT(*),
		COALESCE(MIN(commit_seq), 0), COALESCE(MAX(commit_seq), 0)
		FROM canonical_commits`).Scan(&count, &minimum, &maximum); err != nil {
		return fmt.Errorf("sqlite: inspect Canonical commit sequence: %w", err)
	}
	if count == 0 {
		return nil
	}
	if minimum != 1 || maximum != count {
		return minimumFatal(integrity.CanonicalCommitSequenceInvalid,
			"canonical_commit", "head", "commit_seq is not contiguous from one")
	}
	var nonMonotonic int
	if err := tx.QueryRowContext(ctx, `SELECT EXISTS(
		SELECT 1 FROM canonical_commits current
		JOIN canonical_commits previous ON previous.commit_seq = current.commit_seq - 1
		WHERE current.committed_at <= previous.committed_at
	)`).Scan(&nonMonotonic); err != nil {
		return fmt.Errorf("sqlite: inspect Canonical ledger time: %w", err)
	}
	if nonMonotonic != 0 {
		return minimumFatal(integrity.CanonicalCommitSequenceInvalid,
			"canonical_commit", "head", "ledger time is not strictly increasing")
	}
	return nil
}

func validateMinimumContentErasure(ctx context.Context, tx *sql.Tx) error {
	rows, err := tx.QueryContext(ctx, `SELECT content.content_id, content.erasure_state,
		COUNT(event.content_erasure_event_id),
		SUM(CASE WHEN event.canonical_commit_id IS NOT NULL AND (
			commit_row.resident_id IS NULL OR commit_row.resident_id <> content.owner_resident_id OR
			event.recorded_at <> commit_row.committed_at OR event.recorded_tz <> commit_row.committed_tz
		) THEN 1 ELSE 0 END)
	FROM content_objects content
	LEFT JOIN content_erasure_events event ON event.content_id = content.content_id
	LEFT JOIN canonical_commits commit_row ON commit_row.canonical_commit_id = event.canonical_commit_id
	GROUP BY content.content_id, content.erasure_state
	ORDER BY content.content_id`)
	if err != nil {
		return fmt.Errorf("sqlite: inspect content erasure invariant: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var contentID, state string
		var eventCount, invalidMetadata int64
		if err := rows.Scan(&contentID, &state, &eventCount, &invalidMetadata); err != nil {
			return fmt.Errorf("sqlite: scan content erasure invariant: %w", err)
		}
		validCardinality := state == canonical.ContentErasurePresent && eventCount == 0 ||
			state == canonical.ContentErasureErased && eventCount == 1
		if !validCardinality || invalidMetadata != 0 {
			return minimumFatal(integrity.ContentErasureInvariantInvalid,
				"content", contentID, "erasure state, event cardinality, scope, or ledger time differs")
		}
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("sqlite: iterate content erasure invariant: %w", err)
	}
	return nil
}

func validateMinimumPresentClaims(ctx context.Context, tx *sql.Tx) error {
	rows, err := tx.QueryContext(ctx, `SELECT claim.claim_id, claim.owner_resident_id
		FROM claims claim
		JOIN content_objects content ON content.content_id = claim.statement_content_id
		WHERE content.erasure_state = 'present'
		ORDER BY claim.owner_resident_id, claim.claim_id`)
	if err != nil {
		return fmt.Errorf("sqlite: list present claim identities: %w", err)
	}
	type identity struct{ claimID, residentID canonical.ID }
	identities := make([]identity, 0)
	for rows.Next() {
		var claimRaw, residentRaw string
		if err := rows.Scan(&claimRaw, &residentRaw); err != nil {
			_ = rows.Close()
			return fmt.Errorf("sqlite: scan present claim identity: %w", err)
		}
		claimID, claimErr := canonical.ParseID(claimRaw)
		residentID, residentErr := canonical.ParseID(residentRaw)
		if claimErr != nil || residentErr != nil {
			_ = rows.Close()
			return minimumFatal(integrity.PresentClaimIdentityInvalid,
				"claim", claimRaw, "claim or resident identity is invalid")
		}
		identities = append(identities, identity{claimID: claimID, residentID: residentID})
	}
	if err := rows.Err(); err != nil {
		_ = rows.Close()
		return fmt.Errorf("sqlite: iterate present claim identities: %w", err)
	}
	if err := rows.Close(); err != nil {
		return fmt.Errorf("sqlite: close present claim identities: %w", err)
	}
	for _, item := range identities {
		if _, err := loadEligibleClaimStatement(ctx, tx, item.residentID, item.claimID); err != nil {
			return errors.Join(minimumFatal(integrity.PresentClaimIdentityInvalid,
				"claim", item.claimID.String(), "raw or normalized statement identity cannot be reproduced"), err)
		}
	}
	return nil
}

func validateMinimumGenerationHistories(ctx context.Context, tx *sql.Tx) error {
	rows, err := tx.QueryContext(ctx, `SELECT run.generation_run_id, run.resident_id
		FROM generation_runs run
		JOIN canonical_commits commit_row ON commit_row.canonical_commit_id = run.canonical_commit_id
		ORDER BY commit_row.commit_seq, run.generation_run_id`)
	if err != nil {
		return fmt.Errorf("sqlite: list generation histories: %w", err)
	}
	type runIdentity struct{ runID, residentID canonical.ID }
	runs := make([]runIdentity, 0)
	for rows.Next() {
		var runRaw, residentRaw string
		if err := rows.Scan(&runRaw, &residentRaw); err != nil {
			_ = rows.Close()
			return fmt.Errorf("sqlite: scan generation history identity: %w", err)
		}
		runID, runErr := canonical.ParseID(runRaw)
		residentID, residentErr := canonical.ParseID(residentRaw)
		if runErr != nil || residentErr != nil {
			_ = rows.Close()
			return minimumFatal(integrity.GenerationOutcomeHistoryInvalid,
				"generation_run", runRaw, "run or resident identity is invalid")
		}
		runs = append(runs, runIdentity{runID: runID, residentID: residentID})
	}
	if err := rows.Err(); err != nil {
		_ = rows.Close()
		return fmt.Errorf("sqlite: iterate generation histories: %w", err)
	}
	if err := rows.Close(); err != nil {
		return fmt.Errorf("sqlite: close generation histories: %w", err)
	}
	for _, run := range runs {
		if _, err := (generationOutcomeRepository{}).Summary(ctx, tx, run.runID, run.residentID); err != nil {
			return errors.Join(minimumFatal(integrity.GenerationOutcomeHistoryInvalid,
				"generation_run", run.runID.String(), "outcome history is malformed"), err)
		}
	}
	return nil
}

func minimumFatal(code integrity.FatalCode, targetKind, targetID, reason string) error {
	return &integrity.FatalError{Code: code, TargetKind: targetKind, TargetID: targetID, Reason: reason}
}

var _ integrity.StructuralMinimumSource = (*minimumCheckSource)(nil)
