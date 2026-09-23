package domain

import (
	"context"
	"errors"
	"fmt"

	"mahoroba.local/mahoroba/internal/canonical"
)

// ErrClaimSourceIneligible is returned when an explicit semantic operation
// names a claim whose statement bytes are unavailable or whose identity is no
// longer eligible as semantic input. Callers must not silently choose another
// claim after receiving this error.
var ErrClaimSourceIneligible = errors.New("domain: claim source ineligible")

// ErrClaimStatementErasureRequiresBatch is returned when the single-claim
// erasure primitive finds that a statement content object is referenced by
// more than one claim, or that the reference group is already inconsistent.
// The caller must use the M7 batch erasure plan rather than partially erasing
// the shared claim identity.
var ErrClaimStatementErasureRequiresBatch = errors.New("domain: claim statement erasure requires batch")

// EraseClaimStatement is the complete input for the M7 claim-identity
// erasure primitive. The content event and the claim event are committed by
// one Canonical UoW; no erased material or hash is accepted as input.
type EraseClaimStatement struct {
	ResidentID                   canonical.ID
	ClaimID                      canonical.ID
	ClaimStatementErasureEventID canonical.ID
	ContentErasureEventID        canonical.ID
	ActorPrincipalID             canonical.ID
	ReasonCode                   string
	ReasonContentID              *canonical.ID
	OccurredAt                   canonical.Instant
	OccurredTZ                   canonical.Timezone
}

type ClaimStatementErasureResult struct {
	ClaimID               canonical.ID
	ContentID             canonical.ID
	ContentErasureEventID canonical.ID
	ClaimErasureEventID   canonical.ID
}

// ClaimStatementErasureMutator is the narrow Canonical Writer capability
// used by EraseClaimStatementCommand.
type ClaimStatementErasureMutator interface {
	EraseClaimStatement(context.Context, EraseClaimStatement) (ClaimStatementErasureResult, error)
}

func EraseClaimStatementCommand(value EraseClaimStatement) canonical.Command {
	scope, _ := canonical.ResidentScope(value.ResidentID)
	return command{
		name: "EraseClaimStatement", scope: scope,
		validate: func() error {
			for _, id := range []canonical.ID{
				value.ResidentID, value.ClaimID, value.ClaimStatementErasureEventID,
				value.ContentErasureEventID, value.ActorPrincipalID,
			} {
				if err := id.Validate(); err != nil {
					return err
				}
			}
			if value.ReasonCode == "" {
				return fmt.Errorf("domain: claim statement erasure reason is required")
			}
			if err := value.OccurredTZ.Validate(); err != nil {
				return err
			}
			if value.ReasonContentID != nil {
				if err := value.ReasonContentID.Validate(); err != nil {
					return err
				}
			}
			return nil
		},
		execute: func(ctx context.Context, store MutationStore) (any, error) {
			mutator, ok := store.(ClaimStatementErasureMutator)
			if !ok {
				return nil, fmt.Errorf("domain: canonical UoW lacks claim statement erasure capability")
			}
			return mutator.EraseClaimStatement(ctx, value)
		},
	}
}
