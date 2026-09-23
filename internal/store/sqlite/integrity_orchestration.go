package sqlite

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"mahoroba.local/mahoroba/internal/canonical"
	"mahoroba.local/mahoroba/internal/integrity"
)

// IntegrityPipelineCommit returns the exact Canonical commit that registered
// a persisted pipeline row. It lets offline command results distinguish a
// newly created bootstrap from an exact retry that reused an earlier row.
func (repository *CanonicalRepository) IntegrityPipelineCommit(
	ctx context.Context,
	pipelineID canonical.ID,
) (canonical.CommitMetadata, error) {
	if repository == nil || repository.store == nil || repository.store.reader == nil {
		return canonical.CommitMetadata{}, errors.New("sqlite: integrity pipeline repository is required")
	}
	if err := pipelineID.Validate(); err != nil {
		return canonical.CommitMetadata{}, err
	}
	row := repository.store.reader.QueryRowContext(ctx, `SELECT
		canonical_commit.canonical_commit_id, canonical_commit.commit_seq, canonical_commit.resident_id,
		canonical_commit.committed_at, canonical_commit.committed_tz
		FROM pipeline_versions pipeline
		JOIN canonical_commits canonical_commit
		  ON canonical_commit.canonical_commit_id = pipeline.canonical_commit_id
		WHERE pipeline.pipeline_version_id = ?`, pipelineID.String())
	metadata, err := scanIntegrityCommitMetadata(row)
	if errors.Is(err, sql.ErrNoRows) {
		return canonical.CommitMetadata{}, errors.New("sqlite: integrity pipeline commit is missing")
	}
	if err != nil {
		return canonical.CommitMetadata{}, fmt.Errorf("sqlite: resolve integrity pipeline commit: %w", err)
	}
	return metadata, nil
}

// IntegrityHeadMetadata resolves the reporting identity for an already
// captured Writer head without replacing that head with a newer cursor.
func (repository *CanonicalRepository) IntegrityHeadMetadata(
	ctx context.Context,
	head canonical.Head,
) (integrity.HeadMetadata, error) {
	if repository == nil || repository.store == nil || repository.store.reader == nil {
		return integrity.HeadMetadata{}, errors.New("sqlite: integrity head repository is required")
	}
	if err := head.Validate(); err != nil {
		return integrity.HeadMetadata{}, err
	}
	if !head.Exists {
		var count int
		if err := repository.store.reader.QueryRowContext(ctx,
			`SELECT COUNT(*) FROM canonical_commits`).Scan(&count); err != nil {
			return integrity.HeadMetadata{}, fmt.Errorf("sqlite: verify empty integrity head: %w", err)
		}
		if count != 0 {
			return integrity.HeadMetadata{}, errors.New("sqlite: captured empty integrity head no longer identifies the store")
		}
		return integrity.HeadMetadata{}, nil
	}
	var commitIDRaw, timezoneRaw string
	var committedAt int64
	err := repository.store.reader.QueryRowContext(ctx, `SELECT
		canonical_commit_id, committed_at, committed_tz
		FROM canonical_commits WHERE commit_seq = ?`, head.CommitSeq.Int64()).Scan(
		&commitIDRaw, &committedAt, &timezoneRaw,
	)
	if errors.Is(err, sql.ErrNoRows) {
		return integrity.HeadMetadata{}, errors.New("sqlite: captured integrity head commit is missing")
	}
	if err != nil {
		return integrity.HeadMetadata{}, fmt.Errorf("sqlite: resolve integrity head metadata: %w", err)
	}
	if canonical.Instant(committedAt) != head.CommittedAt {
		return integrity.HeadMetadata{}, errors.New("sqlite: captured integrity head time differs")
	}
	commitID, err := canonical.ParseID(commitIDRaw)
	if err != nil {
		return integrity.HeadMetadata{}, err
	}
	timezone, err := canonical.ParseTimezone(timezoneRaw)
	if err != nil {
		return integrity.HeadMetadata{}, err
	}
	metadata := integrity.HeadMetadata{Head: head, CommitID: commitID, CommittedTZ: timezone}
	if err := metadata.Validate(); err != nil {
		return integrity.HeadMetadata{}, err
	}
	return metadata, nil
}

type integrityCommitScanner interface {
	Scan(...any) error
}

func scanIntegrityCommitMetadata(row integrityCommitScanner) (canonical.CommitMetadata, error) {
	var commitIDRaw, timezoneRaw string
	var residentRaw sql.NullString
	var commitSeqRaw, committedAt int64
	if err := row.Scan(&commitIDRaw, &commitSeqRaw, &residentRaw, &committedAt, &timezoneRaw); err != nil {
		return canonical.CommitMetadata{}, err
	}
	commitID, err := canonical.ParseID(commitIDRaw)
	if err != nil {
		return canonical.CommitMetadata{}, err
	}
	commitSeq, err := canonical.NewCommitSeq(commitSeqRaw)
	if err != nil {
		return canonical.CommitMetadata{}, err
	}
	timezone, err := canonical.ParseTimezone(timezoneRaw)
	if err != nil {
		return canonical.CommitMetadata{}, err
	}
	scope := canonical.GlobalScope()
	if residentRaw.Valid {
		residentID, err := canonical.ParseID(residentRaw.String)
		if err != nil {
			return canonical.CommitMetadata{}, err
		}
		scope, err = canonical.ResidentScope(residentID)
		if err != nil {
			return canonical.CommitMetadata{}, err
		}
	}
	metadata := canonical.CommitMetadata{
		CommitID: commitID, CommitSeq: commitSeq, Scope: scope,
		CommittedAt: canonical.Instant(committedAt), CommittedTZ: timezone,
	}
	if err := metadata.Validate(); err != nil {
		return canonical.CommitMetadata{}, err
	}
	return metadata, nil
}
