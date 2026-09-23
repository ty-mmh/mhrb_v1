package app

import (
	"context"
	"errors"
	"fmt"

	"mahoroba.local/mahoroba/internal/canonical"
	"mahoroba.local/mahoroba/internal/domain"
	"mahoroba.local/mahoroba/internal/generation"
)

// ReextractMemoryEvent runs one Admin-requested extraction against the
// current active v2 policy. requestID, rather than policy or pipeline identity,
// defines idempotency; retries reconstruct the exact saved generation inputs.
func (a *Application) ReextractMemoryEvent(
	ctx context.Context,
	residentID, eventID, requestID canonical.ID,
) (domain.MemoryReextractionResult, error) {
	if err := a.ensureAccepting(); err != nil {
		return domain.MemoryReextractionResult{}, err
	}
	for _, id := range []canonical.ID{residentID, eventID, requestID} {
		if err := id.Validate(); err != nil {
			return domain.MemoryReextractionResult{}, err
		}
	}
	repository, ok := a.repository.(domain.MemoryWorkRepository)
	if !ok {
		return domain.MemoryReextractionResult{}, errors.New("app: repository lacks M5 memory work capability")
	}
	lock := a.residentLock(residentID)
	lock.Lock()
	defer lock.Unlock()

	work, exists, err := repository.MemoryReextractionWork(ctx, residentID, eventID, requestID, a.maxAttempts)
	if err != nil {
		return domain.MemoryReextractionResult{}, err
	}
	resident, err := a.repository.Resident(ctx, residentID)
	if err != nil {
		return domain.MemoryReextractionResult{}, err
	}
	if exists && !memoryWorkNeedsExecution(work) {
		return reextractionResult(work, false), nil
	}
	if exists && work.CancellationCode != "" {
		// Erasure/inactivity terminalization is provider-free and must not wait
		// for operational re-selection of an already-durable obligation.
		a.invalidateMemoryExtractionScansForReextraction(residentID)
		if err := a.cancelMemoryExtraction(ctx, work); err != nil && !errors.Is(err, canonical.ErrNoMutation) {
			return domain.MemoryReextractionResult{}, err
		}
		updated, found, err := repository.MemoryReextractionWork(ctx, residentID, eventID, requestID, a.maxAttempts)
		if err != nil || !found {
			if err == nil {
				err = errors.New("app: durable memory re-extraction disappeared")
			}
			return domain.MemoryReextractionResult{}, err
		}
		return reextractionResult(updated, false), nil
	}
	if resident.Status == "active" {
		selected, err := a.memoryExtractionResidentSelected(ctx, residentID)
		if err != nil {
			return domain.MemoryReextractionResult{}, err
		}
		if !selected {
			// Active-but-unselected work is neither provider-executable nor a typed
			// cancellation. A retry after re-selection may create/continue the same
			// request ID without any hidden provider side effect here.
			if exists {
				return reextractionResult(work, false), nil
			}
			return domain.MemoryReextractionResult{}, nil
		}
	}
	if exists {
		a.invalidateMemoryExtractionScansForReextraction(residentID)
		if a.generator == nil {
			return domain.MemoryReextractionResult{}, errors.New("app: generator is not configured")
		}
		if err := a.processMemoryExtractionWork(ctx, work); err != nil &&
			!errors.Is(err, errMemoryForegroundPreempted) {
			return domain.MemoryReextractionResult{}, err
		}
		updated, found, err := repository.MemoryReextractionWork(ctx, residentID, eventID, requestID, a.maxAttempts)
		if err != nil || !found {
			if err == nil {
				err = errors.New("app: durable memory re-extraction disappeared")
			}
			return domain.MemoryReextractionResult{}, err
		}
		return reextractionResult(updated, false), nil
	}

	source, err := a.repository.Event(ctx, residentID, eventID)
	if err != nil {
		return domain.MemoryReextractionResult{}, err
	}
	work = domain.MemoryExtractionWork{
		SourceEvent: source, IdempotencyKey: domain.MemoryReextractionObligation(eventID, requestID),
		PolicyRevisionID: resident.MemoryPolicyRevisionID, State: domain.WorkPending,
	}
	if source.ContentErased {
		work.CancellationCode = generation.MustOutcomeErrorCode(generation.ErrorSourceContentErased, 0).String()
	} else if resident.Status != "active" {
		work.CancellationCode = generation.MustOutcomeErrorCode(generation.ErrorResidentInactive, 0).String()
	}
	if work.CancellationCode != "" {
		a.invalidateMemoryExtractionScansForReextraction(residentID)
		if err := a.cancelMemoryExtraction(ctx, work); err != nil {
			return domain.MemoryReextractionResult{}, err
		}
	} else {
		if a.generator == nil {
			return domain.MemoryReextractionResult{}, errors.New("app: generator is not configured")
		}
		assembly, err := a.assembleMemoryExtraction(ctx, work)
		if err != nil {
			return domain.MemoryReextractionResult{}, err
		}
		a.invalidateMemoryExtractionScansForReextraction(residentID)
		expected := a.captureForegroundEpoch(residentID)
		result, lease, err := a.submitBackgroundMutationAndBeginCall(
			ctx, expected, true,
			domain.PrepareMemoryReextractionCommand(domain.PrepareMemoryReextraction{
				Generation: assembly.Prepare, SourceEventID: eventID, RequestID: requestID,
			}), assembly.Contents,
		)
		if errors.Is(err, errForegroundPreempted) {
			return domain.MemoryReextractionResult{}, nil
		}
		if err != nil {
			return domain.MemoryReextractionResult{}, fmt.Errorf("app: prepare memory re-extraction: %w", err)
		}
		prepared, ok := result.Value.(domain.MemoryReextractionResult)
		if !ok {
			lease.finish()
			return domain.MemoryReextractionResult{}, errors.New("app: memory re-extraction prepare returned an unexpected value")
		}
		work.RunID = &prepared.RunID
		work.AttemptNo = prepared.AttemptNo
		work.State = prepared.State
		if prepared.Changed {
			if err := a.processMemoryExtractionWorkWithAdmission(ctx, work, nil, &lease); err != nil &&
				!errors.Is(err, errMemoryForegroundPreempted) {
				return domain.MemoryReextractionResult{}, err
			}
		} else {
			lease.finish()
		}
	}
	updated, found, err := repository.MemoryReextractionWork(ctx, residentID, eventID, requestID, a.maxAttempts)
	if err != nil || !found {
		if err == nil {
			err = errors.New("app: prepared memory re-extraction is not durable")
		}
		return domain.MemoryReextractionResult{}, err
	}
	return reextractionResult(updated, true), nil
}

func memoryWorkNeedsExecution(work domain.MemoryExtractionWork) bool {
	return work.CancellationCode != "" || work.State == domain.WorkPending ||
		work.State == domain.WorkRunning || work.State == domain.WorkRetryPending
}

func reextractionResult(work domain.MemoryExtractionWork, changed bool) domain.MemoryReextractionResult {
	result := domain.MemoryReextractionResult{
		AttemptNo: work.AttemptNo, State: work.State, Changed: changed,
	}
	if work.RunID != nil {
		result.RunID = *work.RunID
	}
	return result
}

// cancelArchivedResidentMemoryReextractions drains the request-scoped
// re-extraction inventory at one fixed Canonical ceiling. Archive calls this
// after its status commit, so every actionable work item is classified with a
// provider-free resident_inactive cancellation code, including an attempt that
// is paused inside a synchronous observer callback.
func (a *Application) cancelArchivedResidentMemoryReextractions(
	ctx context.Context,
	residentID canonical.ID,
) error {
	repository, ok := a.repository.(domain.MemoryWorkRepository)
	if !ok {
		return nil
	}
	var cursor *domain.MemoryReextractionDiscoveryCursor
	for {
		result, err := repository.DiscoverMemoryReextractionWork(
			ctx,
			residentID,
			domain.MemoryReextractionDiscoveryRequest{
				Cursor: cursor, MaxAttempts: a.maxAttempts,
				Budget: domain.ProductionMemoryDiscoveryBudget(),
			},
		)
		if err != nil {
			return fmt.Errorf("app: discover archived resident memory re-extractions: %w", err)
		}
		if result.Work != nil {
			if result.Work.CancellationCode == "" {
				return errors.New("app: archived resident memory re-extraction lacks a cancellation code")
			}
			if err := a.cancelMemoryExtraction(ctx, *result.Work); err != nil &&
				!errors.Is(err, canonical.ErrNoMutation) {
				return fmt.Errorf("app: cancel archived resident memory re-extraction: %w", err)
			}
		}
		if result.CycleComplete {
			return nil
		}
		if result.NextCursor == nil ||
			memoryReextractionDiscoveryCursorsEqual(cursor, result.NextCursor) {
			return errors.New("app: archived resident memory re-extraction discovery made no progress")
		}
		cursor = result.NextCursor
	}
}
