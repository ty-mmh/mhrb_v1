package erasure

import (
	"context"
	"fmt"

	"mahoroba.local/mahoroba/internal/canonical"
)

// Failpoint is test-only injection authority passed directly by a trusted
// caller. It is transient, absent from the plan wire format, and invoked only
// inside the transaction so every injected failure must rollback.
type Failpoint func(name string) error

// ApplyBoundary is transient filesystem identity authority. Public Apply uses
// it to revalidate the protected root/database binding inside the Canonical
// transaction; it is never serialized into a plan.
type ApplyBoundary interface {
	Verify() error
}

type ApplyRequest struct {
	Plan    Plan
	Confirm string
	// Blobs is transient filesystem authority. It is deliberately excluded
	// from the plan wire format and is required only for a new mutation. Exact
	// retries are recognized before locator verification because successful
	// erasure has already destroyed the target locators.
	Blobs     BlobReader
	Boundary  ApplyBoundary
	Failpoint Failpoint
}

type Mutator interface {
	ApplyErasure(context.Context, ApplyRequest) (ApplyResult, error)
}

type applyCommand struct {
	request ApplyRequest
	scope   canonical.Scope
}

func ApplyCommand(request ApplyRequest) canonical.Command {
	resident, _ := canonical.ParseID(request.Plan.ResidentID)
	scope, _ := canonical.ResidentScope(resident)
	return &applyCommand{request: request, scope: scope}
}

func (command *applyCommand) Name() string           { return "ApplyErasure" }
func (command *applyCommand) Scope() canonical.Scope { return command.scope }
func (command *applyCommand) RequiresResidentAdmissionFence() bool {
	return command.request.Plan.Scope == ScopeResident
}
func (command *applyCommand) Validate() error {
	if err := validatePlan(command.request.Plan, true); err != nil {
		return err
	}
	if command.request.Plan.PlanState != StateReady {
		return ErrReviewRequired
	}
	if len(command.request.Plan.Blockers) != 0 {
		return ErrBlocked
	}
	if command.request.Confirm != command.request.Plan.Digest {
		return fmt.Errorf("%w: confirmation differs", ErrPlanDigestMismatch)
	}
	resident, err := canonical.ParseID(command.request.Plan.ResidentID)
	if err != nil {
		return err
	}
	want, err := canonical.ResidentScope(resident)
	if err != nil || !want.Equal(command.scope) {
		return fmt.Errorf("%w: command scope differs", ErrInvalidPlan)
	}
	return nil
}
func (command *applyCommand) Execute(ctx context.Context, uow canonical.CanonicalUoW) (any, error) {
	mutator, ok := uow.(Mutator)
	if !ok {
		return nil, fmt.Errorf("erasure: Canonical UoW lacks erasure capability")
	}
	return mutator.ApplyErasure(ctx, command.request)
}
