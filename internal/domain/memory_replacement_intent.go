package domain

import (
	"context"
	"fmt"

	"mahoroba.local/mahoroba/internal/canonical"
)

// CreateMemoryReplacementIntent records the owner-Admin nomination of one
// sediment direct claim as a possible replacement for one settled direct
// claim. It is deliberately not a status decision; alignment landing must
// still revalidate and atomically apply any eventual supersession.
type CreateMemoryReplacementIntent struct {
	ResidentID       canonical.ID
	NewClaimID       canonical.ID
	OldClaimID       canonical.ID
	RelationID       canonical.ID
	OwnerPrincipalID canonical.ID
}

type MemoryReplacementIntentResult struct {
	RelationID canonical.ID `json:"relation_id"`
	NewClaimID canonical.ID `json:"new_claim_id"`
	OldClaimID canonical.ID `json:"old_claim_id"`
	Created    bool         `json:"created"`
}

type memoryReplacementIntentMutator interface {
	CreateMemoryReplacementIntent(context.Context, CreateMemoryReplacementIntent) (MemoryReplacementIntentResult, error)
}

func CreateMemoryReplacementIntentCommand(value CreateMemoryReplacementIntent) canonical.Command {
	scope, _ := canonical.ResidentScope(value.ResidentID)
	return command{
		name: "CreateMemoryReplacementIntent", scope: scope,
		validate: func() error {
			for _, id := range []canonical.ID{
				value.ResidentID, value.NewClaimID, value.OldClaimID,
				value.RelationID, value.OwnerPrincipalID,
			} {
				if err := id.Validate(); err != nil {
					return err
				}
			}
			if value.NewClaimID == value.OldClaimID {
				return fmt.Errorf("domain: replacement and old claim must differ")
			}
			if value.RelationID == value.NewClaimID || value.RelationID == value.OldClaimID {
				return fmt.Errorf("domain: replacement intent relation identity must be distinct")
			}
			return nil
		},
		execute: func(ctx context.Context, store MutationStore) (any, error) {
			mutator, ok := store.(memoryReplacementIntentMutator)
			if !ok {
				return nil, fmt.Errorf("domain: canonical UoW lacks memory replacement intent capability")
			}
			return mutator.CreateMemoryReplacementIntent(ctx, value)
		},
	}
}
