package sqlite

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"mahoroba.local/mahoroba/internal/canonical"
	"mahoroba.local/mahoroba/internal/domain"
)

// EraseClaimStatement performs content erasure and claim identity erasure in
// the already-open Canonical UoW. The SQL guards are intentionally redundant
// with these checks: callers must receive the same all-or-nothing behavior
// whether a failure is detected by the Writer or by SQLite.
func (u *canonicalUoW) EraseClaimStatement(ctx context.Context, value domain.EraseClaimStatement) (domain.ClaimStatementErasureResult, error) {
	if err := u.requireResidentScope(value.ResidentID); err != nil {
		return domain.ClaimStatementErasureResult{}, err
	}
	if err := u.requireOwnerHuman(ctx, value.ResidentID, value.ActorPrincipalID); err != nil {
		return domain.ClaimStatementErasureResult{}, err
	}

	var ownerRaw, contentRaw string
	var statementHash []byte
	if err := u.tx.QueryRowContext(ctx, `
		SELECT owner_resident_id, statement_content_id, statement_hash
		FROM claims
		WHERE claim_id = ?`, value.ClaimID.String()).Scan(&ownerRaw, &contentRaw, &statementHash); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return domain.ClaimStatementErasureResult{}, fmt.Errorf("sqlite: claim %s not found", value.ClaimID)
		}
		return domain.ClaimStatementErasureResult{}, fmt.Errorf("sqlite: load claim for erasure: %w", err)
	}
	if ownerRaw != value.ResidentID.String() {
		return domain.ClaimStatementErasureResult{}, errors.New("sqlite: claim statement erasure crosses resident scope")
	}
	var contentOwner, erasureState string
	if err := u.tx.QueryRowContext(ctx, `
		SELECT owner_resident_id, erasure_state
		FROM content_objects WHERE content_id = ?`, contentRaw).Scan(&contentOwner, &erasureState); err != nil {
		return domain.ClaimStatementErasureResult{}, fmt.Errorf("sqlite: load claim statement content: %w", err)
	}
	if contentOwner != value.ResidentID.String() {
		return domain.ClaimStatementErasureResult{}, errors.New("sqlite: claim statement content crosses resident scope")
	}
	if erasureState != "present" {
		return domain.ClaimStatementErasureResult{}, errors.New("sqlite: claim statement content is already erased")
	}

	var claimReferences, presentClaimIdentities int
	if err := u.tx.QueryRowContext(ctx, `
		SELECT COUNT(*), COUNT(statement_hash)
		FROM claims
		WHERE statement_content_id = ?`, contentRaw).Scan(&claimReferences, &presentClaimIdentities); err != nil {
		return domain.ClaimStatementErasureResult{}, fmt.Errorf("sqlite: count claim statement content references: %w", err)
	}
	if claimReferences != 1 || presentClaimIdentities != 1 {
		return domain.ClaimStatementErasureResult{}, fmt.Errorf(
			"%w: statement content %s has %d claim references and %d present identities",
			domain.ErrClaimStatementErasureRequiresBatch, contentRaw, claimReferences, presentClaimIdentities,
		)
	}
	if len(statementHash) == 0 {
		return domain.ClaimStatementErasureResult{}, errors.New("sqlite: claim statement identity is already erased")
	}

	m := u.metadata
	var reasonContent any
	if value.ReasonContentID != nil {
		reasonContent = value.ReasonContentID.String()
	}
	if _, err := u.tx.ExecContext(ctx, `INSERT INTO content_erasure_events(
		content_erasure_event_id, canonical_commit_id, content_id, erasure_scope,
		actor_principal_id, reason_code, reason_content_id, source_erasure_event_id,
		occurred_at, occurred_tz, recorded_at, recorded_tz
	) VALUES (?, ?, ?, 'content', ?, ?, ?, NULL, ?, ?, ?, ?)`,
		value.ContentErasureEventID.String(), m.CommitID.String(), contentRaw,
		value.ActorPrincipalID.String(), value.ReasonCode, reasonContent,
		value.OccurredAt.UnixMicro(), value.OccurredTZ.String(),
		m.CommittedAt.UnixMicro(), m.CommittedTZ.String()); err != nil {
		return domain.ClaimStatementErasureResult{}, fmt.Errorf("sqlite: append content erasure event: %w", err)
	}
	if result, err := u.tx.ExecContext(ctx, `UPDATE content_objects
		SET erasure_state = 'erased', blob_hash = NULL, commitment_salt = NULL
		WHERE content_id = ? AND erasure_state = 'present'`, contentRaw); err != nil {
		return domain.ClaimStatementErasureResult{}, fmt.Errorf("sqlite: erase claim statement content: %w", err)
	} else if affected, affectedErr := result.RowsAffected(); affectedErr != nil || affected != 1 {
		return domain.ClaimStatementErasureResult{}, errors.New("sqlite: claim statement content was changed concurrently")
	}

	if _, err := u.tx.ExecContext(ctx, `INSERT INTO claim_statement_erasure_events(
		claim_statement_erasure_event_id, canonical_commit_id, resident_id, claim_id,
		content_erasure_event_id, recorded_at, recorded_tz
	) VALUES (?, ?, ?, ?, ?, ?, ?)`, value.ClaimStatementErasureEventID.String(),
		m.CommitID.String(), value.ResidentID.String(), value.ClaimID.String(),
		value.ContentErasureEventID.String(), m.CommittedAt.UnixMicro(), m.CommittedTZ.String()); err != nil {
		return domain.ClaimStatementErasureResult{}, fmt.Errorf("sqlite: append claim statement erasure event: %w", err)
	}
	if result, err := u.tx.ExecContext(ctx, `UPDATE claims SET statement_hash = NULL WHERE claim_id = ? AND statement_hash IS NOT NULL`, value.ClaimID.String()); err != nil {
		return domain.ClaimStatementErasureResult{}, fmt.Errorf("sqlite: erase claim statement identity: %w", err)
	} else if affected, affectedErr := result.RowsAffected(); affectedErr != nil || affected != 1 {
		return domain.ClaimStatementErasureResult{}, errors.New("sqlite: claim statement identity was changed concurrently")
	}
	return domain.ClaimStatementErasureResult{
		ClaimID:               value.ClaimID,
		ContentID:             mustParseCanonicalID(contentRaw),
		ContentErasureEventID: value.ContentErasureEventID,
		ClaimErasureEventID:   value.ClaimStatementErasureEventID,
	}, nil
}

func mustParseCanonicalID(raw string) canonical.ID {
	id, _ := canonical.ParseID(raw)
	return id
}
