package sqlite

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"mahoroba.local/mahoroba/internal/canonical"
	"mahoroba.local/mahoroba/internal/domain"
)

func (u *canonicalUoW) RegisterPipelineVersions(ctx context.Context, value domain.RegisterPipelineVersions) error {
	if !u.metadata.Scope.IsGlobal() {
		return errors.New("sqlite: pipeline registration requires global scope")
	}
	inserted := 0
	for _, version := range value.Versions {
		if err := domain.ValidateExactDialoguePipelineDefinition(version); err != nil {
			return err
		}
		var existingID, existingDefinition string
		err := u.tx.QueryRowContext(ctx, `SELECT pipeline_version_id, definition
			FROM pipeline_versions WHERE pipeline_kind = ? AND version_key = ?`,
			version.Kind, version.VersionKey).Scan(&existingID, &existingDefinition)
		switch {
		case err == nil:
			if existingDefinition != version.Definition.String() {
				return fmt.Errorf("sqlite: conflicting %s/%s pipeline definition", version.Kind, version.VersionKey)
			}
			continue
		case !errors.Is(err, sql.ErrNoRows):
			return fmt.Errorf("sqlite: inspect %s/%s pipeline: %w", version.Kind, version.VersionKey, err)
		}
		if _, err := u.tx.ExecContext(ctx, `INSERT INTO pipeline_versions(
			pipeline_version_id, canonical_commit_id, pipeline_kind, version_key,
			definition, recorded_at, recorded_tz
		) VALUES (?, ?, ?, ?, ?, ?, ?)`,
			version.ID.String(), u.metadata.CommitID.String(), version.Kind, version.VersionKey,
			version.Definition.String(), u.metadata.CommittedAt.UnixMicro(), u.metadata.CommittedTZ.String(),
		); err != nil {
			return fmt.Errorf("sqlite: insert %s/%s pipeline: %w", version.Kind, version.VersionKey, err)
		}
		inserted++
	}
	if inserted == 0 {
		return canonical.ErrNoMutation
	}
	return nil
}
