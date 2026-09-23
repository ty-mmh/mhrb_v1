package sqlite

import (
	"context"
	"errors"
	"fmt"

	"mahoroba.local/mahoroba/internal/canonical"
	"mahoroba.local/mahoroba/internal/domain"
)

func (u *canonicalUoW) insertDialogueRecall(ctx context.Context, recall domain.DialogueRecall) error {
	return u.insertDialogueRecallForContext(ctx, recall, domain.DialogueContextPolicyVersionV1)
}

func (u *canonicalUoW) insertDialogueRecallForContext(
	ctx context.Context,
	recall domain.DialogueRecall,
	contextPolicyVersion string,
) error {
	if err := recall.ValidateForContext(contextPolicyVersion); err != nil {
		return err
	}
	if err := u.requireResidentScope(recall.ResidentID); err != nil {
		return err
	}
	if u.metadata.CommitSeq.Int64() <= 1 {
		return errors.New("sqlite: dialogue Recall has no captured Canonical head")
	}
	var activePolicy string
	if err := u.tx.QueryRowContext(ctx, `SELECT activation.revision_id
		FROM resident_revision_activations activation
		JOIN canonical_commits commit_row ON commit_row.canonical_commit_id = activation.canonical_commit_id
		JOIN resident_revisions revision ON revision.revision_id = activation.revision_id
		WHERE activation.resident_id = ? AND revision.resident_id = ?
		  AND revision.revision_class = 'memory_policy' AND commit_row.commit_seq < ?
		ORDER BY commit_row.commit_seq DESC, activation.activation_id DESC LIMIT 1`,
		recall.ResidentID.String(), recall.ResidentID.String(), u.metadata.CommitSeq.Int64(),
	).Scan(&activePolicy); err != nil {
		return fmt.Errorf("sqlite: resolve usage-time memory policy: %w", err)
	}
	if activePolicy != recall.MemoryPolicyRevisionID.String() {
		return errors.New("sqlite: claim usage memory policy is not active at usage time")
	}
	if _, err := u.tx.ExecContext(ctx, `INSERT INTO recall_runs(
		recall_run_id, canonical_commit_id, resident_id, query_content_id, query_conditions,
		pipeline_version_id, memory_policy_revision_id, as_of, as_of_tz,
		context_constraints, recorded_at, recorded_tz
	) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		recall.RunID.String(), u.metadata.CommitID.String(), recall.ResidentID.String(),
		recall.QueryContentID.String(), recall.QueryConditions.String(), recall.PipelineVersionID.String(),
		recall.MemoryPolicyRevisionID.String(), recall.AsOf.UnixMicro(), recall.AsOfTZ.String(),
		recall.ContextConstraints.String(), u.metadata.CommittedAt.UnixMicro(), u.metadata.CommittedTZ.String(),
	); err != nil {
		return fmt.Errorf("sqlite: insert dialogue Recall run: %w", err)
	}
	return nil
}

func (u *canonicalUoW) insertDialogueRecallUsages(
	ctx context.Context,
	recall domain.DialogueRecall,
	generationRunID canonical.ID,
) error {
	if err := generationRunID.Validate(); err != nil {
		return err
	}
	for index, usage := range recall.Usages {
		var exclusion any
		if usage.ExclusionReason != "" {
			exclusion = string(usage.ExclusionReason)
		}
		if _, err := u.tx.ExecContext(ctx, `INSERT INTO claim_usages(
			claim_usage_id, canonical_commit_id, claim_id, recall_run_id, generation_run_id,
			usage_type, ordinal, memory_policy_revision_id, exclusion_reason,
			detection_method, detection_confidence, detected_by_run_id, recorded_at, recorded_tz
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, NULL, NULL, NULL, ?, ?)`,
			usage.ID.String(), u.metadata.CommitID.String(), usage.ClaimID.String(), recall.RunID.String(),
			generationRunID.String(), string(usage.Type), usage.Ordinal.Int64(), recall.MemoryPolicyRevisionID.String(),
			exclusion, u.metadata.CommittedAt.UnixMicro(), u.metadata.CommittedTZ.String(),
		); err != nil {
			return fmt.Errorf("sqlite: insert dialogue Recall usage %d: %w", index, err)
		}
	}
	return nil
}
