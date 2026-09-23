package app

import (
	"context"
	"errors"
	"fmt"

	"mahoroba.local/mahoroba/internal/canonical"
	"mahoroba.local/mahoroba/internal/domain"
	"mahoroba.local/mahoroba/internal/generation"
)

// recordBackgroundAttemptFailure appends a generic terminal outcome only while
// the exact run is still running. Archive owns every resident-wide terminal
// outcome after its status transition, so a recorder which loses that race
// unwinds successfully instead of reporting a second, conflicting failure.
//
// The bool reports that a lifecycle boundary had already terminalized the run.
// Callers which would otherwise return a provider error can then suppress that
// stale result after Archive became authoritative.
func (a *Application) recordBackgroundAttemptFailure(
	ctx context.Context,
	prepared domain.PreparedGeneration,
	state string,
	code generation.OutcomeErrorCode,
) (bool, error) {
	hardCtx := context.WithoutCancel(ctx)
	landing := a.backgroundLandingLock(prepared.ResidentID)
	landing.Lock()
	lifecycleTerminal, err := a.failureAttemptLifecycleTerminal(hardCtx, prepared)
	if err != nil {
		landing.Unlock()
		return false, fmt.Errorf("app: reload background generation before failure outcome: %w", err)
	}
	if lifecycleTerminal {
		landing.Unlock()
		return true, nil
	}
	outcomeID, err := a.ids.New()
	if err != nil {
		landing.Unlock()
		return false, err
	}
	command := domain.FailAttemptCommand(domain.FailAttempt{
		Attempt: domain.Attempt{
			RunID: prepared.RunID, ResidentID: prepared.ResidentID,
			AttemptNo: prepared.AttemptNo, OutcomeID: outcomeID,
		},
		State: state, ErrorClass: code.String(),
	})
	result, err := a.writer.Submit(hardCtx, command)
	if err == nil {
		landing.Unlock()
		a.afterSubmit(result, nil, prepared.ResidentID, true)
		return false, nil
	}
	if errors.Is(err, canonical.ErrWriterPoisoned) {
		landing.Unlock()
		a.afterSubmit(result, err, prepared.ResidentID, true)
		return false, fmt.Errorf("app: record background generation failure: %w", err)
	}
	lifecycleTerminal, readErr := a.failureAttemptLifecycleTerminal(hardCtx, prepared)
	landing.Unlock()
	a.afterSubmit(result, err, prepared.ResidentID, true)
	if readErr == nil && lifecycleTerminal {
		return true, nil
	}
	return false, fmt.Errorf("app: record background generation failure: %w", errors.Join(err, readErr))
}

// failureAttemptLifecycleTerminal reads both sides of the lifecycle predicate
// while the caller owns the resident landing lock. An inactive resident owns
// the exact run even before Archive cleanup appends resident_inactive. When the
// resident remains active, submitting the original command preserves the
// backend's exact replay/conflict checks for a different terminal winner.
func (a *Application) failureAttemptLifecycleTerminal(
	ctx context.Context,
	prepared domain.PreparedGeneration,
) (bool, error) {
	resident, err := a.repository.Resident(ctx, prepared.ResidentID)
	if err != nil {
		return false, err
	}
	if _, err := a.repository.Generation(ctx, prepared.RunID); err != nil {
		return false, err
	}
	return resident.Status != "active", nil
}

// rejectGenerationEnvelopeAtResidentBoundary serializes provider-free
// unsupported-envelope terminalization with Archive. Rejection intentionally
// does not require an exact Generation reload: callers use it when that frozen
// envelope may itself be unreadable or invalid.
func (a *Application) rejectGenerationEnvelopeAtResidentBoundary(
	ctx context.Context,
	residentID, runID canonical.ID,
) (domain.GenerationRejectionResult, bool, error) {
	hardCtx := context.WithoutCancel(ctx)
	landing := a.backgroundLandingLock(residentID)
	landing.Lock()
	resident, err := a.repository.Resident(hardCtx, residentID)
	if err != nil {
		landing.Unlock()
		return domain.GenerationRejectionResult{}, false, err
	}
	if resident.Status != "active" {
		landing.Unlock()
		return domain.GenerationRejectionResult{}, true, nil
	}
	ids, err := a.allocateIDs(2)
	if err != nil {
		landing.Unlock()
		return domain.GenerationRejectionResult{}, false, err
	}
	code := generation.MustOutcomeErrorCode(generation.ErrorProviderUnsupported, 0)
	command := domain.RejectGenerationEnvelopeCommand(domain.RejectGenerationEnvelope{
		RunID: runID, ResidentID: residentID,
		RunningOutcomeID: ids[0], RejectedOutcomeID: ids[1], ErrorClass: code.String(),
	})
	result, err := a.writer.Submit(hardCtx, command)
	if err != nil {
		if errors.Is(err, canonical.ErrWriterPoisoned) {
			landing.Unlock()
			a.afterSubmit(result, err, residentID, true)
			return domain.GenerationRejectionResult{}, false, err
		}
		current, readErr := a.repository.Resident(hardCtx, residentID)
		landing.Unlock()
		a.afterSubmit(result, err, residentID, true)
		if readErr == nil && current.Status != "active" {
			return domain.GenerationRejectionResult{}, true, nil
		}
		return domain.GenerationRejectionResult{}, false, errors.Join(err, readErr)
	}
	landing.Unlock()
	a.afterSubmit(result, nil, residentID, true)
	rejected, ok := result.Value.(domain.GenerationRejectionResult)
	if !ok {
		return domain.GenerationRejectionResult{}, false, errors.New(
			"app: generation envelope rejection returned an unexpected value",
		)
	}
	return rejected, false, nil
}
