package sqlite

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"mahoroba.local/mahoroba/internal/autonomy"
	"mahoroba.local/mahoroba/internal/canonical"
	"mahoroba.local/mahoroba/internal/domain"
	"mahoroba.local/mahoroba/internal/memory"
)

// autonomyQueryer is the small common part of *sql.DB and *sql.Tx needed by
// event-time policy resolution. Keeping the query helper transaction-neutral
// lets the read path and the Canonical Writer enforce the same rule.
type autonomyQueryer interface {
	QueryRowContext(context.Context, string, ...any) *sql.Row
}

func autonomySelfTalkTriggerSourceCommit(
	ctx context.Context,
	queryer autonomyQueryer,
	residentID canonical.ID,
	trigger autonomy.Trigger,
) (int64, error) {
	if err := residentID.Validate(); err != nil {
		return 0, err
	}
	if err := trigger.Validate(); err != nil {
		return 0, err
	}
	var query string
	var args []any
	switch trigger.Kind {
	case autonomy.TriggerIdle:
		query = `SELECT source_commit.commit_seq
			FROM events event
			JOIN canonical_commits source_commit ON source_commit.canonical_commit_id = event.canonical_commit_id
			WHERE event.event_id = ? AND event.resident_id = ? AND event.event_type = 'user_message'`
		args = []any{trigger.SourceID.String(), residentID.String()}
	case autonomy.TriggerMetaDependencyStatusChange:
		query = `SELECT source_commit.commit_seq
			FROM claim_status_transitions status
			JOIN canonical_commits source_commit ON source_commit.canonical_commit_id = status.canonical_commit_id
			JOIN claims claim ON claim.claim_id = status.claim_id
			WHERE status.status_transition_id = ? AND claim.owner_resident_id = ?`
		args = []any{trigger.SourceID.String(), residentID.String()}
	case autonomy.TriggerSettledDirectConflict:
		query = `SELECT source_commit.commit_seq
			FROM claim_relations relation
			JOIN canonical_commits source_commit ON source_commit.canonical_commit_id = relation.canonical_commit_id
			JOIN claims source ON source.claim_id = relation.from_claim_id
			JOIN claims target ON target.claim_id = relation.to_claim_id
			WHERE relation.claim_relation_id = ?
			  AND source.owner_resident_id = ? AND target.owner_resident_id = ?`
		args = []any{trigger.SourceID.String(), residentID.String(), residentID.String()}
	default:
		return 0, fmt.Errorf("sqlite: trigger %s is not a self-talk trigger", trigger.Kind)
	}
	var commit int64
	if err := queryer.QueryRowContext(ctx, query, args...).Scan(&commit); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return 0, fmt.Errorf("%w: self-talk trigger source %s", ErrActiveRevisionUnresolved, trigger.SourceID)
		}
		return 0, fmt.Errorf("sqlite: resolve self-talk trigger source commit: %w", err)
	}
	if commit < 1 {
		return 0, fmt.Errorf("sqlite: self-talk trigger source commit is invalid: %d", commit)
	}
	return commit, nil
}

func autonomyMemoryPolicySupportsSelfTalkAtCommit(
	ctx context.Context,
	queryer autonomyQueryer,
	residentID canonical.ID,
	commit int64,
) (bool, error) {
	if err := residentID.Validate(); err != nil {
		return false, err
	}
	if commit < 1 {
		return false, fmt.Errorf("sqlite: invalid event-time policy commit %d", commit)
	}
	var content []byte
	err := queryer.QueryRowContext(ctx, `SELECT blob.content
		FROM resident_revision_activations activation
		JOIN canonical_commits activation_commit
		  ON activation_commit.canonical_commit_id = activation.canonical_commit_id
		JOIN resident_revisions revision ON revision.revision_id = activation.revision_id
		JOIN content_objects object ON object.content_id = revision.content_id
		LEFT JOIN blobs blob ON blob.dedupe_scope_id = object.owner_resident_id
		 AND blob.hash_algorithm = object.blob_hash_algorithm
		 AND blob.blob_hash = object.blob_hash
		WHERE activation.resident_id = ? AND revision.resident_id = ?
		  AND revision.revision_class = 'memory_policy'
		  AND activation_commit.commit_seq <= ?
		ORDER BY activation_commit.commit_seq DESC, activation.activation_id DESC LIMIT 1`,
		residentID.String(), residentID.String(), commit).Scan(&content)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return false, nil
		}
		return false, fmt.Errorf("sqlite: resolve event-time memory policy: %w", err)
	}
	if content == nil {
		return false, nil
	}
	policy, _, err := memory.ParsePolicy(content)
	if err != nil {
		return false, fmt.Errorf("sqlite: parse event-time memory policy: %w", err)
	}
	return policy.Version == memory.PolicyVersionV3 || policy.Version == memory.PolicyVersionV4 || policy.Version == memory.PolicyVersionV5, nil
}

// AutonomousSelfTalkEventTimeEligible reports whether the memory policy that
// was active when a self-talk trigger source was committed was v3 or v4. It is
// intentionally separate from the current scheduler snapshot: activation is
// non-retroactive.
func (r *CanonicalRepository) AutonomousSelfTalkEventTimeEligible(
	ctx context.Context,
	residentID canonical.ID,
	trigger autonomy.Trigger,
) (bool, error) {
	if err := residentID.Validate(); err != nil {
		return false, err
	}
	if err := trigger.Validate(); err != nil {
		return false, err
	}
	var revision domain.ActiveRevision
	var err error
	if trigger.Kind == autonomy.TriggerIdle {
		revision, err = r.ActiveRevisionForEvent(ctx, residentID, "memory_policy", trigger.SourceID)
	} else {
		commit, commitErr := autonomySelfTalkTriggerSourceCommit(ctx, r.store.reader, residentID, trigger)
		if commitErr != nil {
			return false, commitErr
		}
		through, commitErr := canonical.NewCommitSeq(commit)
		if commitErr != nil {
			return false, commitErr
		}
		revision, err = r.ActiveRevisionAtHead(ctx, residentID, "memory_policy", through)
	}
	if err != nil {
		return false, err
	}
	if revision.ContentErased {
		return false, nil
	}
	policy, _, err := memory.ParsePolicy([]byte(revision.Content))
	if err != nil {
		return false, fmt.Errorf("sqlite: parse event-time memory policy %s: %w", revision.RevisionID, err)
	}
	return policy.Version == memory.PolicyVersionV3 || policy.Version == memory.PolicyVersionV4 || policy.Version == memory.PolicyVersionV5, nil
}

func (u *canonicalUoW) requireAutonomousSelfTalkEventTimePolicy(
	ctx context.Context,
	residentID canonical.ID,
	trigger autonomy.Trigger,
) error {
	commit, err := autonomySelfTalkTriggerSourceCommit(ctx, u.tx, residentID, trigger)
	if err != nil {
		return err
	}
	eligible, err := autonomyMemoryPolicySupportsSelfTalkAtCommit(ctx, u.tx, residentID, commit)
	if err != nil {
		return err
	}
	if !eligible {
		return errors.New("sqlite: self-talk requires event-time memory-policy-v3, memory-policy-v4, or memory-policy-v5")
	}
	return nil
}
