package app

import (
	"context"
	"fmt"

	"mahoroba.local/mahoroba/internal/canonical"
	"mahoroba.local/mahoroba/internal/domain"
	"mahoroba.local/mahoroba/internal/memory"
)

func (a *Application) SetMemoryClaimScope(
	ctx context.Context,
	residentID canonical.ID,
	claimID canonical.ID,
	scope memory.ViewScope,
) (domain.SetClaimViewScopeResult, error) {
	if err := a.ensureAccepting(); err != nil {
		return domain.SetClaimViewScopeResult{}, err
	}
	resident, err := a.repository.Resident(ctx, residentID)
	if err != nil {
		return domain.SetClaimViewScopeResult{}, err
	}
	if resident.Status != "active" {
		return domain.SetClaimViewScopeResult{}, fmt.Errorf("app: claim scope changes require active resident")
	}
	assertionID, err := a.ids.New()
	if err != nil {
		return domain.SetClaimViewScopeResult{}, err
	}
	result, err := a.submit(ctx, domain.SetClaimViewScopeCommand(domain.SetClaimViewScope{
		ResidentID: residentID, ClaimID: claimID, AssertionID: assertionID,
		OwnerPrincipalID: resident.OwnerPrincipalID, Scope: scope,
	}))
	if err != nil {
		return domain.SetClaimViewScopeResult{}, err
	}
	changed, ok := result.Value.(domain.SetClaimViewScopeResult)
	if !ok {
		return domain.SetClaimViewScopeResult{}, fmt.Errorf("app: claim scope mutation returned an unexpected value")
	}
	changed.Changed = !result.Commit.CommitID.IsZero()
	return changed, nil
}

func (a *Application) SetMemoryClaimStatus(
	ctx context.Context,
	residentID canonical.ID,
	claimID canonical.ID,
	to memory.ClaimStatus,
	reason memory.HumanDecisionReason,
) (domain.ClaimStatusDecisionResult, error) {
	if err := a.ensureAccepting(); err != nil {
		return domain.ClaimStatusDecisionResult{}, err
	}
	resident, err := a.repository.Resident(ctx, residentID)
	if err != nil {
		return domain.ClaimStatusDecisionResult{}, err
	}
	if resident.Status != "active" {
		return domain.ClaimStatusDecisionResult{}, fmt.Errorf("app: claim status changes require active resident")
	}
	transitionID, err := a.ids.New()
	if err != nil {
		return domain.ClaimStatusDecisionResult{}, err
	}
	result, err := a.submit(ctx, domain.HumanClaimStatusDecisionCommand(domain.HumanClaimStatusDecision{
		ResidentID: residentID, ClaimID: claimID, StatusTransitionID: transitionID,
		OwnerPrincipalID: resident.OwnerPrincipalID, ToStatus: to, Reason: reason,
	}))
	if err != nil {
		return domain.ClaimStatusDecisionResult{}, err
	}
	decision, ok := result.Value.(domain.ClaimStatusDecisionResult)
	if !ok {
		return domain.ClaimStatusDecisionResult{}, fmt.Errorf("app: claim status mutation returned an unexpected value")
	}
	return decision, nil
}

// CreateMemoryReplacementIntent records an owner-Admin nomination only. It
// does not settle the replacement or mutate either claim status; those remain
// the responsibility of the alignment landing transaction.
func (a *Application) CreateMemoryReplacementIntent(
	ctx context.Context,
	residentID canonical.ID,
	newClaimID canonical.ID,
	oldClaimID canonical.ID,
) (domain.MemoryReplacementIntentResult, error) {
	if err := a.ensureAccepting(); err != nil {
		return domain.MemoryReplacementIntentResult{}, err
	}
	resident, err := a.repository.Resident(ctx, residentID)
	if err != nil {
		return domain.MemoryReplacementIntentResult{}, err
	}
	if resident.Status != "active" {
		return domain.MemoryReplacementIntentResult{}, fmt.Errorf("app: replacement intent requires active resident")
	}
	relationID, err := a.ids.New()
	if err != nil {
		return domain.MemoryReplacementIntentResult{}, err
	}
	result, err := a.submit(ctx, domain.CreateMemoryReplacementIntentCommand(domain.CreateMemoryReplacementIntent{
		ResidentID: residentID, NewClaimID: newClaimID, OldClaimID: oldClaimID,
		RelationID: relationID, OwnerPrincipalID: resident.OwnerPrincipalID,
	}))
	if err != nil {
		return domain.MemoryReplacementIntentResult{}, err
	}
	intent, ok := result.Value.(domain.MemoryReplacementIntentResult)
	if !ok {
		return domain.MemoryReplacementIntentResult{}, fmt.Errorf("app: replacement intent mutation returned an unexpected value")
	}
	return intent, nil
}
