package sqlite

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"mahoroba.local/mahoroba/internal/canonical"
	"mahoroba.local/mahoroba/internal/domain"
	"mahoroba.local/mahoroba/internal/erasure"
	"mahoroba.local/mahoroba/internal/integrity"
)

var (
	ErrSessionPolicyUnavailable = errors.New("sqlite: selected session policy is unavailable")
	ErrRuntimeConfigUnavailable = errors.New("sqlite: runtime configuration is unavailable")
)

type ErasurePlanningAuthority struct {
	ActorPrincipalID              canonical.ID
	IntegrityPipelineVersionID    canonical.ID
	MemoryStatusPipelineVersionID canonical.ID
}

func (store *Store) ErasurePlanningAuthority(ctx context.Context, residentID canonical.ID) (ErasurePlanningAuthority, error) {
	return resolveErasurePlanningAuthority(ctx, store.reader, residentID)
}

func (inspection *Inspection) ErasurePlanningAuthority(ctx context.Context, residentID canonical.ID) (ErasurePlanningAuthority, error) {
	return resolveErasurePlanningAuthority(ctx, inspection.store.reader, residentID)
}

func resolveErasurePlanningAuthority(ctx context.Context, reader *sql.DB, residentID canonical.ID) (_ ErasurePlanningAuthority, resultErr error) {
	if ctx == nil || reader == nil {
		return ErasurePlanningAuthority{}, errors.New("sqlite: erasure planning authority is unavailable")
	}
	if err := residentID.Validate(); err != nil {
		return ErasurePlanningAuthority{}, err
	}
	tx, err := reader.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return ErasurePlanningAuthority{}, err
	}
	defer func() {
		if err := tx.Rollback(); err != nil && !errors.Is(err, sql.ErrTxDone) {
			resultErr = errors.Join(resultErr, err)
		}
	}()
	var actorRaw string
	if err := tx.QueryRowContext(ctx, `SELECT transition.actor_principal_id
		FROM resident_status_transitions transition
		JOIN canonical_commits commit_row
		  ON commit_row.canonical_commit_id=transition.canonical_commit_id
		JOIN principals principal
		  ON principal.principal_id=transition.actor_principal_id
		WHERE transition.resident_id=? AND transition.from_status IS NULL
		  AND transition.to_status='draft' AND principal.kind='human'
		ORDER BY commit_row.commit_seq, transition.resident_status_transition_id
		LIMIT 1`, residentID.String()).Scan(&actorRaw); err != nil {
		return ErasurePlanningAuthority{}, fmt.Errorf("sqlite: resolve erasure owner human: %w", err)
	}
	authority := ErasurePlanningAuthority{}
	authority.ActorPrincipalID, err = canonical.ParseID(actorRaw)
	if err != nil {
		return ErasurePlanningAuthority{}, err
	}
	authority.IntegrityPipelineVersionID, err = resolveExactPipelineID(
		ctx, tx, integrity.IntegrityPipelineKind, integrity.IntegrityPipelineVersion, integrityPipelineDefinitionV1,
	)
	if err != nil {
		return ErasurePlanningAuthority{}, err
	}
	authority.MemoryStatusPipelineVersionID, err = resolveExactPipelineID(
		ctx, tx, "memory_status", domain.MemoryStatusPipelineVersion, memoryStatusPipelineDefinitionV1,
	)
	if err != nil {
		return ErasurePlanningAuthority{}, err
	}
	if err := tx.Commit(); err != nil {
		return ErasurePlanningAuthority{}, err
	}
	return authority, nil
}

func resolveExactPipelineID(ctx context.Context, tx *sql.Tx, kind, version, definition string) (canonical.ID, error) {
	var raw string
	err := tx.QueryRowContext(ctx, `SELECT pipeline_version_id FROM pipeline_versions
		WHERE pipeline_kind=? AND version_key=? AND definition=?`, kind, version, definition).Scan(&raw)
	if err != nil {
		return canonical.ID{}, errors.Join(erasure.ErrIntegrityPipelineRequired, err)
	}
	id, err := canonical.ParseID(raw)
	if err != nil {
		return canonical.ID{}, errors.Join(erasure.ErrIntegrityPipelineRequired, err)
	}
	return id, nil
}

type SessionPolicySelection struct {
	PreviousVersionID *canonical.ID
	SelectedVersionID canonical.ID
}

// SelectSessionPolicy is the offline Operational transaction. It neither
// creates a Canonical version nor guesses a newest version.
func (store *Store) SelectSessionPolicy(ctx context.Context, versionID canonical.ID) (_ SessionPolicySelection, resultErr error) {
	if ctx == nil || store == nil || store.writer == nil || store.writes == nil {
		return SessionPolicySelection{}, ErrRuntimeConfigUnavailable
	}
	if err := versionID.Validate(); err != nil {
		return SessionPolicySelection{}, errors.Join(ErrSessionPolicyUnavailable, err)
	}
	release, err := store.writes.acquireHigh(ctx)
	if err != nil {
		return SessionPolicySelection{}, err
	}
	defer release()
	if err := store.verifyWritableBoundary("before session policy selection transaction"); err != nil {
		return SessionPolicySelection{}, err
	}
	tx, err := store.writer.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelSerializable})
	if err != nil {
		return SessionPolicySelection{}, err
	}
	defer func() {
		if err := tx.Rollback(); err != nil && !errors.Is(err, sql.ErrTxDone) {
			resultErr = errors.Join(resultErr, err)
		}
	}()
	if err := store.verifyWritableBoundary("after session policy selection transaction open"); err != nil {
		return SessionPolicySelection{}, err
	}
	var versionKey, definition string
	if err := tx.QueryRowContext(ctx, `SELECT version_key,definition
		FROM sessionization_policy_versions
		WHERE sessionization_policy_version_id=?`, versionID.String()).Scan(&versionKey, &definition); err != nil {
		return SessionPolicySelection{}, errors.Join(ErrSessionPolicyUnavailable, err)
	}
	if versionKey != domain.SessionPolicyVersion || !validReadinessSessionDefinition(definition) {
		return SessionPolicySelection{}, ErrSessionPolicyUnavailable
	}
	var previous sql.NullString
	if err := tx.QueryRowContext(ctx, `SELECT desired_sessionization_policy_version_id
		FROM runtime_config WHERE singleton_id=1`).Scan(&previous); err != nil {
		return SessionPolicySelection{}, errors.Join(ErrRuntimeConfigUnavailable, err)
	}
	result := SessionPolicySelection{SelectedVersionID: versionID}
	if previous.Valid {
		id, err := canonical.ParseID(previous.String)
		if err != nil {
			return SessionPolicySelection{}, errors.Join(ErrRuntimeConfigUnavailable, err)
		}
		result.PreviousVersionID = &id
	}
	mutation, err := tx.ExecContext(ctx, `UPDATE runtime_config
		SET desired_sessionization_policy_version_id=? WHERE singleton_id=1`, versionID.String())
	if err != nil {
		return SessionPolicySelection{}, err
	}
	if changed, err := mutation.RowsAffected(); err != nil || changed != 1 {
		return SessionPolicySelection{}, errors.Join(ErrRuntimeConfigUnavailable, err)
	}
	if err := store.verifyWritableBoundary("before session policy selection commit"); err != nil {
		return SessionPolicySelection{}, err
	}
	if err := tx.Commit(); err != nil {
		return SessionPolicySelection{}, err
	}
	return result, store.verifyWritableBoundary("after session policy selection commit")
}
