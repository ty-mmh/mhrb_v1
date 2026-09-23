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

func (u *canonicalUoW) ActivateMemoryPolicy(
	ctx context.Context,
	value domain.ActivateMemoryPolicy,
) (domain.MemoryPolicyActivationResult, error) {
	if err := u.requireResidentScope(value.ResidentID); err != nil {
		return domain.MemoryPolicyActivationResult{}, err
	}
	if err := u.requireOwnerHuman(ctx, value.ResidentID, value.OwnerPrincipalID); err != nil {
		return domain.MemoryPolicyActivationResult{}, err
	}
	status, err := u.currentStatus(ctx, value.ResidentID)
	if err != nil {
		return domain.MemoryPolicyActivationResult{}, err
	}
	if status != "active" {
		return domain.MemoryPolicyActivationResult{}, fmt.Errorf("sqlite: memory policy activation requires active resident, got %s", status)
	}
	policy, canonicalPolicy, err := memory.ParsePolicy(value.Content.Bytes)
	if err != nil {
		return domain.MemoryPolicyActivationResult{}, fmt.Errorf("sqlite: validate memory policy: %w", err)
	}
	if err := policy.RequireEnabled(); err != nil {
		return domain.MemoryPolicyActivationResult{}, fmt.Errorf("sqlite: activate memory policy: %w", err)
	}

	var currentRevision, currentActivation string
	var currentContent []byte
	err = u.tx.QueryRowContext(ctx, `SELECT revision.revision_id, activation.activation_id, blob.content
		FROM resident_revision_activations activation
		JOIN canonical_commits activation_commit
		  ON activation_commit.canonical_commit_id = activation.canonical_commit_id
		JOIN resident_revisions revision ON revision.revision_id = activation.revision_id
		JOIN content_objects content ON content.content_id = revision.content_id
		LEFT JOIN blobs blob ON blob.dedupe_scope_id = content.owner_resident_id
		 AND blob.hash_algorithm = content.blob_hash_algorithm
		 AND blob.blob_hash = content.blob_hash
		WHERE activation.resident_id = ? AND revision.resident_id = ?
		  AND revision.revision_class = 'memory_policy'
		ORDER BY activation_commit.commit_seq DESC, activation.activation_id DESC
		LIMIT 1`, value.ResidentID.String(), value.ResidentID.String()).Scan(
		&currentRevision, &currentActivation, &currentContent,
	)
	if errors.Is(err, sql.ErrNoRows) {
		return domain.MemoryPolicyActivationResult{}, errors.New("sqlite: resident has no active parent memory policy")
	}
	if err != nil {
		return domain.MemoryPolicyActivationResult{}, fmt.Errorf("sqlite: resolve active parent memory policy: %w", err)
	}
	if currentContent == nil {
		return domain.MemoryPolicyActivationResult{}, errors.New("sqlite: active parent memory policy content is erased")
	}
	currentPolicy, _, err := memory.ParsePolicy(currentContent)
	if err != nil {
		return domain.MemoryPolicyActivationResult{}, fmt.Errorf("sqlite: parse active parent memory policy: %w", err)
	}
	if err := validateMemoryPolicyActivationTransition(currentPolicy, policy, value); err != nil {
		return domain.MemoryPolicyActivationResult{}, err
	}
	if currentPolicy.Version == policy.Version && !bytes.Equal(currentContent, canonicalPolicy.Bytes()) {
		return domain.MemoryPolicyActivationResult{}, fmt.Errorf(
			"sqlite: same-version memory policy activation is limited to an exact lost-response retry",
		)
	}
	if bytes.Equal(currentContent, canonicalPolicy.Bytes()) {
		revisionID, parseErr := canonical.ParseID(currentRevision)
		if parseErr != nil {
			return domain.MemoryPolicyActivationResult{}, parseErr
		}
		activationID, parseErr := canonical.ParseID(currentActivation)
		if parseErr != nil {
			return domain.MemoryPolicyActivationResult{}, parseErr
		}
		return domain.MemoryPolicyActivationResult{
			RevisionID: revisionID, ActivationID: activationID, Changed: false,
			PreviousPolicyVersion: currentPolicy.Version, PolicyVersion: policy.Version,
			RenderingVersion: policy.RenderingVersion,
		}, canonical.ErrNoMutation
	}

	if err := u.insertContent(ctx, value.Content); err != nil {
		return domain.MemoryPolicyActivationResult{}, err
	}
	m := u.metadata
	if _, err := u.tx.ExecContext(ctx, `INSERT INTO resident_revisions(
		revision_id, canonical_commit_id, resident_id, revision_class, content_id,
		parent_revision_id, created_by_run_id, reason_content_id, recorded_at, recorded_tz
	) VALUES (?, ?, ?, 'memory_policy', ?, ?, NULL, NULL, ?, ?)`,
		value.RevisionID.String(), m.CommitID.String(), value.ResidentID.String(),
		value.Content.ID.String(), currentRevision, m.CommittedAt.UnixMicro(), m.CommittedTZ.String(),
	); err != nil {
		return domain.MemoryPolicyActivationResult{}, fmt.Errorf("sqlite: insert memory policy revision: %w", err)
	}
	if _, err := u.tx.ExecContext(ctx, `INSERT INTO resident_revision_activations(
		activation_id, canonical_commit_id, resident_id, revision_id, actor_principal_id,
		approval_id, reason_code, reason_content_id, recorded_at, recorded_tz
	) VALUES (?, ?, ?, ?, ?, NULL, 'admin_memory_policy', NULL, ?, ?)`,
		value.ActivationID.String(), m.CommitID.String(), value.ResidentID.String(),
		value.RevisionID.String(), value.OwnerPrincipalID.String(), m.CommittedAt.UnixMicro(), m.CommittedTZ.String(),
	); err != nil {
		return domain.MemoryPolicyActivationResult{}, fmt.Errorf("sqlite: activate memory policy revision: %w", err)
	}
	return domain.MemoryPolicyActivationResult{
		RevisionID: value.RevisionID, ActivationID: value.ActivationID, Changed: true,
		PreviousPolicyVersion: currentPolicy.Version, PolicyVersion: policy.Version,
		RenderingVersion: policy.RenderingVersion,
	}, nil
}

func validateMemoryPolicyActivationTransition(
	current memory.Policy,
	target memory.Policy,
	value domain.ActivateMemoryPolicy,
) error {
	if value.ExpectedFrom != current.Version {
		return fmt.Errorf("sqlite: memory policy expected-from mismatch: expected %s, current %s",
			value.ExpectedFrom, current.Version)
	}
	if target.Version == current.Version {
		if target.Version == memory.PolicyVersionV2 || target.Version == memory.PolicyVersionV3 ||
			target.Version == memory.PolicyVersionV4 {
			return nil
		}
		return errors.New("sqlite: disabled memory-policy-v1 cannot be activated")
	}
	if target.Version != memory.PolicyVersionV4 {
		return fmt.Errorf("sqlite: memory policy transition %s to %s is disabled; use memory-policy-v4",
			current.Version, target.Version)
	}
	switch current.Version {
	case memory.PolicyVersionV1:
		if !value.AcknowledgeRecallEnable || !value.AcknowledgeSelfTalkExtraction {
			return errors.New("sqlite: memory-policy-v1 to v4 requires recall-enable and self-talk-extraction acknowledgements")
		}
	case memory.PolicyVersionV2:
		if !value.AcknowledgeSelfTalkExtraction {
			return errors.New("sqlite: memory-policy-v2 to v4 requires self-talk-extraction acknowledgement")
		}
	case memory.PolicyVersionV3:
		// V3 already enables Recall and mandatory self-talk extraction.
	case memory.PolicyVersionV4:
		return errors.New("sqlite: memory-policy-v4 cannot transition to a different policy version")
	default:
		return fmt.Errorf("sqlite: unsupported active memory policy %q", current.Version)
	}
	return nil
}
