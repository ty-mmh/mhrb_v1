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

func (u *canonicalUoW) SetClaimViewScope(
	ctx context.Context,
	value domain.SetClaimViewScope,
) (domain.SetClaimViewScopeResult, error) {
	if err := u.requireResidentScope(value.ResidentID); err != nil {
		return domain.SetClaimViewScopeResult{}, err
	}
	if err := u.requireOwnerHuman(ctx, value.ResidentID, value.OwnerPrincipalID); err != nil {
		return domain.SetClaimViewScopeResult{}, err
	}
	var ownerRaw string
	if err := u.tx.QueryRowContext(ctx,
		`SELECT owner_resident_id FROM claims WHERE claim_id = ?`, value.ClaimID.String(),
	).Scan(&ownerRaw); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return domain.SetClaimViewScopeResult{}, errors.New("sqlite: memory claim not found")
		}
		return domain.SetClaimViewScopeResult{}, fmt.Errorf("sqlite: load memory claim scope owner: %w", err)
	}
	if ownerRaw != value.ResidentID.String() {
		return domain.SetClaimViewScopeResult{}, errors.New("sqlite: claim belongs to another resident")
	}
	var currentID, currentScope string
	err := u.tx.QueryRowContext(ctx, `SELECT assertion.view_scope_assertion_id, assertion.view_scope
		FROM claim_view_scope_assertions assertion
		JOIN canonical_commits assertion_commit
		  ON assertion_commit.canonical_commit_id = assertion.canonical_commit_id
		WHERE assertion.claim_id = ?
		ORDER BY assertion_commit.commit_seq DESC, assertion.view_scope_assertion_id DESC
		LIMIT 1`, value.ClaimID.String()).Scan(&currentID, &currentScope)
	if errors.Is(err, sql.ErrNoRows) {
		return domain.SetClaimViewScopeResult{}, errors.New("sqlite: claim has no initial view-scope assertion")
	}
	if err != nil {
		return domain.SetClaimViewScopeResult{}, fmt.Errorf("sqlite: load current claim scope: %w", err)
	}
	if memory.ViewScope(currentScope) == value.Scope {
		assertionID, err := canonical.ParseID(currentID)
		if err != nil {
			return domain.SetClaimViewScopeResult{}, err
		}
		return domain.SetClaimViewScopeResult{
			AssertionID: assertionID, Scope: value.Scope, Changed: false,
		}, canonical.ErrNoMutation
	}
	m := u.metadata
	if _, err := u.tx.ExecContext(ctx, `INSERT INTO claim_view_scope_assertions(
		view_scope_assertion_id, canonical_commit_id, claim_id, view_scope,
		actor_principal_id, generation_run_id, memory_policy_revision_id,
		reason_code, reason_content_id, recorded_at, recorded_tz
	) VALUES (?, ?, ?, ?, ?, NULL, NULL, 'owner_scope_change', NULL, ?, ?)`,
		value.AssertionID.String(), m.CommitID.String(), value.ClaimID.String(), string(value.Scope),
		value.OwnerPrincipalID.String(), m.CommittedAt.UnixMicro(), m.CommittedTZ.String(),
	); err != nil {
		return domain.SetClaimViewScopeResult{}, fmt.Errorf("sqlite: append claim view scope: %w", err)
	}
	return domain.SetClaimViewScopeResult{
		AssertionID: value.AssertionID, Scope: value.Scope, Changed: true,
	}, nil
}
