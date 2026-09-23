package sqlite

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"mahoroba.local/mahoroba/internal/canonical"
	"mahoroba.local/mahoroba/internal/domain"
)

var ErrActiveRevisionUnresolved = errors.New("sqlite: active revision is unresolved")

func (r *CanonicalRepository) ActiveRevisionAtHead(
	ctx context.Context,
	residentID canonical.ID,
	class string,
	through canonical.CommitSeq,
) (domain.ActiveRevision, error) {
	return r.activeRevision(ctx, residentID, class, through, nil)
}

func (r *CanonicalRepository) ActiveRevisionAsOf(
	ctx context.Context,
	residentID canonical.ID,
	class string,
	through canonical.CommitSeq,
	asOf canonical.Instant,
) (domain.ActiveRevision, error) {
	return r.activeRevision(ctx, residentID, class, through, &asOf)
}

func (r *CanonicalRepository) ActiveRevisionForEvent(
	ctx context.Context,
	residentID canonical.ID,
	class string,
	eventID canonical.ID,
) (domain.ActiveRevision, error) {
	if err := residentID.Validate(); err != nil {
		return domain.ActiveRevision{}, err
	}
	if err := eventID.Validate(); err != nil {
		return domain.ActiveRevision{}, err
	}
	var sourceCommit int64
	err := r.store.reader.QueryRowContext(ctx, `SELECT source_commit.commit_seq
		FROM events event
		JOIN canonical_commits source_commit ON source_commit.canonical_commit_id = event.canonical_commit_id
		WHERE event.event_id = ? AND event.resident_id = ?`, eventID.String(), residentID.String()).Scan(&sourceCommit)
	if errors.Is(err, sql.ErrNoRows) {
		return domain.ActiveRevision{}, fmt.Errorf("%w: source event %s", ErrActiveRevisionUnresolved, eventID)
	}
	if err != nil {
		return domain.ActiveRevision{}, fmt.Errorf("sqlite: resolve source event commit: %w", err)
	}
	through, err := canonical.NewCommitSeq(sourceCommit)
	if err != nil {
		return domain.ActiveRevision{}, err
	}
	return r.activeRevision(ctx, residentID, class, through, nil)
}

func (r *CanonicalRepository) activeRevision(
	ctx context.Context,
	residentID canonical.ID,
	class string,
	through canonical.CommitSeq,
	asOf *canonical.Instant,
) (domain.ActiveRevision, error) {
	if err := residentID.Validate(); err != nil {
		return domain.ActiveRevision{}, err
	}
	if class != "principles" && class != "persona" && class != "memory_policy" {
		return domain.ActiveRevision{}, fmt.Errorf("sqlite: unsupported revision class %q", class)
	}
	if through.Int64() <= 0 {
		return domain.ActiveRevision{}, fmt.Errorf("sqlite: invalid captured revision head")
	}
	query := `SELECT revision.revision_id, activation.activation_id,
		activation_commit.commit_seq, activation.recorded_at, blob.content
		FROM resident_revision_activations activation
		JOIN canonical_commits activation_commit
		  ON activation_commit.canonical_commit_id = activation.canonical_commit_id
		JOIN resident_revisions revision ON revision.revision_id = activation.revision_id
		JOIN content_objects content ON content.content_id = revision.content_id
		LEFT JOIN blobs blob ON blob.dedupe_scope_id = content.owner_resident_id
		 AND blob.hash_algorithm = content.blob_hash_algorithm
		 AND blob.blob_hash = content.blob_hash
		WHERE activation.resident_id = ? AND revision.resident_id = ?
		  AND revision.revision_class = ? AND activation_commit.commit_seq <= ?`
	args := []any{residentID.String(), residentID.String(), class, through.Int64()}
	if asOf != nil {
		query += ` AND activation.recorded_at <= ?`
		args = append(args, asOf.UnixMicro())
	}
	query += ` ORDER BY activation_commit.commit_seq DESC, activation.activation_id DESC LIMIT 1`

	var revisionRaw, activationRaw string
	var commitSeq, activatedAt int64
	var content []byte
	err := r.store.reader.QueryRowContext(ctx, query, args...).Scan(
		&revisionRaw, &activationRaw, &commitSeq, &activatedAt, &content,
	)
	if errors.Is(err, sql.ErrNoRows) {
		return domain.ActiveRevision{}, fmt.Errorf("%w: %s for resident %s", ErrActiveRevisionUnresolved, class, residentID)
	}
	if err != nil {
		return domain.ActiveRevision{}, fmt.Errorf("sqlite: resolve active %s revision: %w", class, err)
	}
	revisionID, err := canonical.ParseID(revisionRaw)
	if err != nil {
		return domain.ActiveRevision{}, err
	}
	activationID, err := canonical.ParseID(activationRaw)
	if err != nil {
		return domain.ActiveRevision{}, err
	}
	parsedCommit, err := canonical.NewCommitSeq(commitSeq)
	if err != nil {
		return domain.ActiveRevision{}, err
	}
	return domain.ActiveRevision{
		RevisionID: revisionID, ActivationID: activationID, ActivationCommit: parsedCommit,
		ActivatedAt: canonical.Instant(activatedAt), Content: string(content), ContentErased: content == nil,
	}, nil
}
