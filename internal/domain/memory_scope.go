package domain

import (
	"context"
	"fmt"

	"mahoroba.local/mahoroba/internal/canonical"
	"mahoroba.local/mahoroba/internal/memory"
)

type SetClaimViewScope struct {
	ResidentID       canonical.ID
	ClaimID          canonical.ID
	AssertionID      canonical.ID
	OwnerPrincipalID canonical.ID
	Scope            memory.ViewScope
}

type SetClaimViewScopeResult struct {
	AssertionID canonical.ID     `json:"assertion_id"`
	Scope       memory.ViewScope `json:"scope"`
	Changed     bool             `json:"changed"`
}

type claimViewScopeMutator interface {
	SetClaimViewScope(context.Context, SetClaimViewScope) (SetClaimViewScopeResult, error)
}

func SetClaimViewScopeCommand(value SetClaimViewScope) canonical.Command {
	scope, _ := canonical.ResidentScope(value.ResidentID)
	return command{
		name: "SetClaimViewScope", scope: scope,
		validate: func() error {
			for _, id := range []canonical.ID{
				value.ResidentID, value.ClaimID, value.AssertionID, value.OwnerPrincipalID,
			} {
				if err := id.Validate(); err != nil {
					return err
				}
			}
			if value.ClaimID == value.AssertionID {
				return fmt.Errorf("domain: scope assertion and claim identities must differ")
			}
			return value.Scope.Validate()
		},
		execute: func(ctx context.Context, store MutationStore) (any, error) {
			mutator, ok := store.(claimViewScopeMutator)
			if !ok {
				return nil, fmt.Errorf("domain: canonical UoW lacks claim scope capability")
			}
			return mutator.SetClaimViewScope(ctx, value)
		},
	}
}
