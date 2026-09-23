package app

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"time"

	"mahoroba.local/mahoroba/internal/blob"
	"mahoroba.local/mahoroba/internal/canonical"
	"mahoroba.local/mahoroba/internal/domain"
	"mahoroba.local/mahoroba/internal/generation"
	"mahoroba.local/mahoroba/internal/memory"
	"mahoroba.local/mahoroba/internal/operationalmetrics"
)

var errMemoryForegroundPreempted = errors.New("app: memory extraction yielded to foreground dialogue")

type memoryExtractionAssembly struct {
	Prepare  domain.PrepareGeneration
	Contents []domain.Content
}

type memoryExtractionDiscoveryRepository interface {
	DiscoverMemoryExtractionWork(
		context.Context,
		canonical.ID,
		domain.MemoryDiscoveryRequest,
	) (domain.MemoryDiscoveryResult, error)
}

func (a *Application) processMemoryExtractionQueue(ctx context.Context, residentID canonical.ID) error {
	a.setMemoryExtractionScansComplete(residentID, false)
	epoch := a.captureForegroundEpoch(residentID)
	repository, ok := a.repository.(domain.MemoryWorkRepository)
	if !ok {
		return errors.New("app: repository lacks M5 memory work capability")
	}

	if !a.memoryReextractionCycleActive(residentID) {
		normalCursor, normalCycleEpoch, current := a.startOrResumeNormalMemoryCycleAtEpoch(residentID, epoch)
		if !current {
			return nil
		}
		normal, err := repository.DiscoverMemoryExtractionWork(ctx, residentID, domain.MemoryDiscoveryRequest{
			Cursor: normalCursor, MaxAttempts: a.maxAttempts,
			Budget: domain.ProductionMemoryDiscoveryBudget(),
		})
		if err != nil {
			return markMandatoryWorkerError(
				err, residentID, canonical.ID{},
				operationalmetrics.MandatoryWorkerPhaseMemoryExtraction, "",
			)
		}
		// Operational progress is published before execution. A failed provider
		// or foreground preemption therefore cannot pin every later turn to one
		// row. Completing normal discovery atomically enters the re-extraction
		// phase so long terminal normal history is not replayed before every
		// re-extraction continuation.
		updated, verified := a.updateNormalMemoryDiscoveryAtEpoch(residentID, normalCycleEpoch, normal)
		if !updated {
			// Foreground invalidation only revokes provider admission. A typed
			// cancellation is provider-free and remains safe to apply idempotently;
			// the next scan can rebuild any cursor progress discarded at the boundary.
			if normal.Work != nil && normal.Work.CancellationCode != "" {
				return a.processQueuedMemoryExtraction(ctx, residentID, *normal.Work)
			}
			return nil
		}
		if normal.Work != nil {
			if normal.Work.CancellationCode == "" && a.foregroundAdvanced(residentID, epoch) {
				return nil
			}
			return a.processQueuedMemoryExtraction(ctx, residentID, *normal.Work)
		}
		if !normal.CycleComplete {
			return nil
		}
		if !verified {
			return nil
		}
	}

	reextractionCursor, reextractionCycleEpoch, current := a.startOrResumeMemoryReextractionCycleAtEpoch(residentID, epoch)
	if !current {
		return nil
	}
	reextraction, err := repository.DiscoverMemoryReextractionWork(ctx, residentID, domain.MemoryReextractionDiscoveryRequest{
		Cursor: reextractionCursor, MaxAttempts: a.maxAttempts,
		Budget: domain.ProductionMemoryDiscoveryBudget(),
	})
	if err != nil {
		return markMandatoryWorkerError(
			err, residentID, canonical.ID{},
			operationalmetrics.MandatoryWorkerPhaseMemoryExtraction, "",
		)
	}
	updated, verified := a.updateMemoryReextractionAtEpoch(residentID, reextractionCycleEpoch, reextraction)
	if !updated {
		// Selection may discard the Operational re-extraction cursor after this
		// durable row was classified. Do not make its provider-free cancellation
		// wait for a later selection/scan cycle.
		if reextraction.Work != nil && reextraction.Work.CancellationCode != "" {
			return a.processQueuedMemoryExtraction(ctx, residentID, *reextraction.Work)
		}
		return nil
	}
	if reextraction.Work != nil {
		if reextraction.Work.CancellationCode == "" && a.foregroundAdvanced(residentID, epoch) {
			return nil
		}
		return a.processQueuedMemoryExtraction(ctx, residentID, *reextraction.Work)
	}
	if !reextraction.CycleComplete {
		return nil
	}
	if !verified {
		return nil
	}
	if !a.memoryNormalDiscoveryCompleteAtEpoch(residentID, epoch) {
		// Foreground advanced while the re-extraction cycle was in progress.
		// Its fixed cycle may finish, but optional work still needs a normal
		// mandatory-memory sweep completed at the new foreground epoch.
		return nil
	}

	selected, err := a.memoryExtractionResidentSelected(ctx, residentID)
	if err != nil {
		return markMandatoryWorkerError(
			err, residentID, canonical.ID{},
			operationalmetrics.MandatoryWorkerPhaseMemoryExtraction, "",
		)
	}
	if !selected {
		a.setMemoryExtractionScansCompleteAtEpoch(residentID, epoch)
		return nil
	}
	if err := a.processMemoryAlignmentQueue(ctx, residentID); err != nil {
		return err
	}
	a.setMemoryExtractionScansCompleteAtEpoch(residentID, epoch)
	return nil
}

func (a *Application) cancelArchivedResidentMemoryExtractions(
	ctx context.Context,
	residentID canonical.ID,
	canCancelRunless bool,
) error {
	repository, ok := a.repository.(domain.MemoryWorkRepository)
	if !ok {
		return nil
	}
	var cursor *domain.MemoryDiscoveryCursor
	for {
		result, err := repository.DiscoverMemoryExtractionWork(ctx, residentID, domain.MemoryDiscoveryRequest{
			Cursor: cursor, MaxAttempts: a.maxAttempts,
			Budget: domain.ProductionMemoryDiscoveryBudget(),
		})
		if err != nil {
			return fmt.Errorf("app: discover archived resident memory extractions: %w", err)
		}
		if result.Work != nil {
			if result.Work.CancellationCode == "" {
				return errors.New("app: archived resident memory extraction lacks a cancellation code")
			}
			if result.Work.RunID != nil || canCancelRunless {
				if err := a.cancelMemoryExtraction(ctx, *result.Work); err != nil &&
					!errors.Is(err, canonical.ErrNoMutation) {
					return fmt.Errorf("app: cancel archived resident memory extraction: %w", err)
				}
			}
		}
		if result.CycleComplete {
			return nil
		}
		if result.NextCursor == nil {
			return errors.New("app: archived resident memory extraction discovery made no progress")
		}
		cursor = result.NextCursor
	}
}

// processQueuedMemoryExtraction executes at most the one item selected by a
// bounded repository pass. Active-but-unselected work remains pending: M7
// permits typed memory cancellation only for erased sources or inactive
// residents, both of which arrive here with CancellationCode already set.
func (a *Application) processQueuedMemoryExtraction(
	ctx context.Context,
	residentID canonical.ID,
	work domain.MemoryExtractionWork,
) error {
	if work.CancellationCode != "" {
		if err := a.cancelQueuedMemoryExtraction(ctx, work); err != nil {
			return markMandatoryWorkerError(
				err, residentID, work.SourceEvent.ID,
				operationalmetrics.MandatoryWorkerPhaseMemoryExtraction, "",
			)
		}
		return nil
	}
	selected, err := a.memoryExtractionResidentSelected(ctx, residentID)
	if err != nil {
		return markMandatoryWorkerError(
			err, residentID, work.SourceEvent.ID,
			operationalmetrics.MandatoryWorkerPhaseMemoryExtraction, "",
		)
	}
	if !selected {
		return nil
	}
	if a.generator == nil {
		return markMandatoryWorkerError(
			errors.New("app: generator is not configured"), residentID, work.SourceEvent.ID,
			operationalmetrics.MandatoryWorkerPhaseMemoryExtraction,
			operationalmetrics.MandatoryWorkerErrorDependencyUnavailable,
		)
	}
	if err := a.processMemoryExtractionWork(ctx, work); err != nil {
		if errors.Is(err, errMemoryForegroundPreempted) {
			return nil
		}
		return markMandatoryWorkerError(
			err, residentID, work.SourceEvent.ID,
			operationalmetrics.MandatoryWorkerPhaseMemoryExtraction, "",
		)
	}
	return nil
}

// cancelQueuedMemoryExtraction treats replay after another worker won the
// terminalization race as success. The cancellation code was classified from
// durable source/resident state, so reapplying it has no provider side effect.
func (a *Application) cancelQueuedMemoryExtraction(ctx context.Context, work domain.MemoryExtractionWork) error {
	err := a.cancelMemoryExtraction(ctx, work)
	if errors.Is(err, canonical.ErrNoMutation) {
		return nil
	}
	return err
}

func (a *Application) memoryExtractionResidentSelected(ctx context.Context, residentID canonical.ID) (bool, error) {
	resident, err := a.repository.Resident(ctx, residentID)
	if err != nil {
		return false, err
	}
	if resident.Status != "active" {
		return false, nil
	}
	selected, err := a.repository.ActiveResident(ctx)
	if err != nil {
		return false, err
	}
	return selected.Status == "active" && selected.ResidentID == residentID, nil
}

// residentActiveButUnselected identifies the Operational state where an
// otherwise active resident must remain provider-idle until it is selected.
// Inactive residents are deliberately reported separately because durable
// mandatory work may still need provider-free typed terminalization.
func (a *Application) residentActiveButUnselected(ctx context.Context, residentID canonical.ID) (bool, error) {
	resident, err := a.repository.Resident(ctx, residentID)
	if err != nil {
		return false, err
	}
	if resident.Status != "active" {
		return false, nil
	}
	selected, err := a.repository.ActiveResident(ctx)
	if err != nil {
		return false, err
	}
	return selected.Status != "active" || selected.ResidentID != residentID, nil
}

// processFairMandatoryMemory advances at most one mandatory extraction whose
// source sequence is no later than the dialogue event just completed. Its bool
// reports that an eligible item was executed or terminalized, so the dialogue
// caller can end the entire resident turn after granting one fair quantum.
// This is intentionally a separate lease from the ordinary queue drain so
// optional or newer evidence cannot starve the source event's required
// extraction.
func (a *Application) processFairMandatoryMemory(
	ctx context.Context,
	residentID canonical.ID,
	throughSeq canonical.Seq,
) (bool, error) {
	// Freeze the handoff epoch before discovery. Any ingress after this point
	// invalidates the entire fair attempt, including provider registration.
	epoch := a.captureForegroundEpoch(residentID)
	repository, ok := a.repository.(memoryExtractionDiscoveryRepository)
	if !ok {
		return false, errors.New("app: repository lacks M5 memory work capability")
	}
	cursor := a.memoryDiscoveryCursor(residentID)
	var result domain.MemoryDiscoveryResult
	for pass := 0; pass < 2; pass++ {
		var err error
		bound := throughSeq
		result, err = repository.DiscoverMemoryExtractionWork(ctx, residentID, domain.MemoryDiscoveryRequest{
			Cursor: cursor, ThroughSeq: &bound, MaxAttempts: a.maxAttempts,
			Budget: domain.ProductionMemoryDiscoveryBudget(),
		})
		if err != nil {
			return false, markMandatoryWorkerError(err, residentID, canonical.ID{}, operationalmetrics.MandatoryWorkerPhaseMemoryExtraction, "")
		}
		eligibleTypedCancellation := result.Work != nil && result.Work.CancellationCode != "" &&
			fairMandatoryMemoryWorkEligible(*result.Work, throughSeq)
		// Persist scan progress before processing the selected item. A failed or
		// preempted provider must not pin every later fair turn to one candidate.
		if !a.updateFairMemoryDiscoveryAtEpoch(residentID, epoch, result) {
			// Cursor publication and provider admission are epoch-scoped, but typed
			// cancellation is not a provider call. Terminalize it now; a later fair
			// scan may safely rediscover the already-terminal obligation.
			if eligibleTypedCancellation {
				return true, a.cancelQueuedMemoryExtraction(ctx, *result.Work)
			}
			// A durable running attempt must not be stranded when the exact
			// foreground epoch changes between discovery and cursor publication.
			// Carry the stale epoch into atomic provider registration so the normal
			// foreground-preempted outcome is recorded without calling the provider.
			if result.Work != nil && result.Work.State == domain.WorkRunning &&
				result.Work.RunID != nil && result.Work.CancellationCode == "" {
				return true, a.processMemoryExtractionWorkWithEpoch(ctx, *result.Work, &epoch)
			}
			return false, errMemoryForegroundPreempted
		}
		if result.Work != nil {
			break
		}
		if !result.CycleComplete || pass == 1 {
			return false, nil
		}
		// A completed prior cycle may have left an eligible item at the wrapped
		// head. Requery once immediately so the fair handoff does not consume a
		// dialogue turn merely observing the wrap boundary. The captured epoch
		// and throughSeq remain fixed, and the two-pass cap preserves boundedness.
		cursor = nil
	}
	if result.Work == nil {
		return false, nil
	}
	work := *result.Work
	if !fairMandatoryMemoryWorkEligible(work, throughSeq) {
		return false, nil
	}
	if work.CancellationCode != "" {
		return true, a.cancelQueuedMemoryExtraction(ctx, work)
	}
	if a.foregroundAdvanced(residentID, epoch) {
		// A running attempt already exists durably. Carry its captured epoch to
		// atomic registration so rejection records foreground_preempted instead
		// of leaving the attempt stranded in running. Pending/retry work has no
		// running attempt to close and may yield without a mutation.
		if work.State == domain.WorkRunning && work.RunID != nil && work.CancellationCode == "" {
			return true, a.processMemoryExtractionWorkWithEpoch(ctx, work, &epoch)
		}
		return false, errMemoryForegroundPreempted
	}
	selected, err := a.memoryExtractionResidentSelected(ctx, residentID)
	if err != nil {
		return false, markMandatoryWorkerError(err, residentID, work.SourceEvent.ID,
			operationalmetrics.MandatoryWorkerPhaseMemoryExtraction, "")
	}
	if !selected {
		return false, errMemoryForegroundPreempted
	}
	if a.generator == nil {
		return false, markMandatoryWorkerError(errors.New("app: generator is not configured"), residentID, work.SourceEvent.ID,
			operationalmetrics.MandatoryWorkerPhaseMemoryExtraction, operationalmetrics.MandatoryWorkerErrorDependencyUnavailable)
	}
	return true, a.processMemoryExtractionWorkWithEpoch(ctx, work, &epoch)
}

func fairMandatoryMemoryWorkEligible(work domain.MemoryExtractionWork, throughSeq canonical.Seq) bool {
	request, err := domain.ParseMemoryExtractionObligation(work.IdempotencyKey)
	return err == nil && request.Mode == domain.MemoryExtractionMandatory && work.SourceEvent.Seq <= throughSeq
}

func (a *Application) memoryDiscoveryCursor(residentID canonical.ID) *domain.MemoryDiscoveryCursor {
	a.memoryStateMu.Lock()
	defer a.memoryStateMu.Unlock()
	return cloneMemoryDiscoveryCursor(a.memoryDiscoveryCursors[residentID])
}

func (a *Application) setMemoryDiscoveryCursor(residentID canonical.ID, cursor *domain.MemoryDiscoveryCursor) {
	a.memoryStateMu.Lock()
	if a.memoryDiscoveryCursors == nil {
		a.memoryDiscoveryCursors = make(map[canonical.ID]*domain.MemoryDiscoveryCursor)
	}
	a.memoryDiscoveryCursors[residentID] = cloneMemoryDiscoveryCursor(cursor)
	a.memoryStateMu.Unlock()
}

func (a *Application) clearMemoryDiscoveryCursor(residentID canonical.ID) {
	a.memoryStateMu.Lock()
	delete(a.memoryDiscoveryCursors, residentID)
	a.memoryStateMu.Unlock()
}

func (a *Application) updateFairMemoryDiscoveryAtEpoch(
	residentID canonical.ID,
	expected uint64,
	result domain.MemoryDiscoveryResult,
) bool {
	a.backgroundMu.Lock()
	defer a.backgroundMu.Unlock()
	if a.foregroundEpoch[residentID] != expected {
		return false
	}
	a.memoryStateMu.Lock()
	defer a.memoryStateMu.Unlock()
	if result.NextCursor != nil {
		if a.memoryDiscoveryCursors == nil {
			a.memoryDiscoveryCursors = make(map[canonical.ID]*domain.MemoryDiscoveryCursor)
		}
		a.memoryDiscoveryCursors[residentID] = cloneMemoryDiscoveryCursor(result.NextCursor)
	}
	if result.CycleComplete {
		delete(a.memoryDiscoveryCursors, residentID)
	}
	return true
}

func cloneMemoryDiscoveryCursor(cursor *domain.MemoryDiscoveryCursor) *domain.MemoryDiscoveryCursor {
	if cursor == nil {
		return nil
	}
	cloned := *cursor
	if cursor.AfterSeq != nil {
		after := *cursor.AfterSeq
		cloned.AfterSeq = &after
	}
	return &cloned
}

func (a *Application) normalMemoryDiscoveryCursor(residentID canonical.ID) *domain.MemoryDiscoveryCursor {
	a.memoryStateMu.Lock()
	defer a.memoryStateMu.Unlock()
	return cloneMemoryDiscoveryCursor(a.normalMemoryDiscoveryCursors[residentID])
}

// startOrResumeNormalMemoryCycleAtEpoch binds a finite cursor cycle to the
// foreground epoch at which its ceiling was first captured. New ingress does
// not reset an in-progress cursor (which would starve deep history), but the
// old cycle can never publish a clean proof for the newer epoch.
func (a *Application) startOrResumeNormalMemoryCycleAtEpoch(
	residentID canonical.ID,
	expected uint64,
) (*domain.MemoryDiscoveryCursor, uint64, bool) {
	a.backgroundMu.Lock()
	defer a.backgroundMu.Unlock()
	if a.foregroundEpoch[residentID] != expected {
		return nil, 0, false
	}
	a.memoryStateMu.Lock()
	defer a.memoryStateMu.Unlock()
	if a.memoryNormalCycleEpochs == nil {
		a.memoryNormalCycleEpochs = make(map[canonical.ID]uint64)
	}
	cycleEpoch, exists := a.memoryNormalCycleEpochs[residentID]
	if !exists {
		cycleEpoch = expected
		a.memoryNormalCycleEpochs[residentID] = cycleEpoch
	}
	return cloneMemoryDiscoveryCursor(a.normalMemoryDiscoveryCursors[residentID]), cycleEpoch, true
}

func (a *Application) setNormalMemoryDiscoveryCursor(residentID canonical.ID, cursor *domain.MemoryDiscoveryCursor) {
	a.memoryStateMu.Lock()
	if a.normalMemoryDiscoveryCursors == nil {
		a.normalMemoryDiscoveryCursors = make(map[canonical.ID]*domain.MemoryDiscoveryCursor)
	}
	a.normalMemoryDiscoveryCursors[residentID] = cloneMemoryDiscoveryCursor(cursor)
	a.memoryStateMu.Unlock()
}

func (a *Application) clearNormalMemoryDiscoveryCursor(residentID canonical.ID) {
	a.memoryStateMu.Lock()
	delete(a.normalMemoryDiscoveryCursors, residentID)
	a.memoryStateMu.Unlock()
}

func (a *Application) updateNormalMemoryDiscoveryAtEpoch(
	residentID canonical.ID,
	cycleEpoch uint64,
	result domain.MemoryDiscoveryResult,
) (bool, bool) {
	a.backgroundMu.Lock()
	defer a.backgroundMu.Unlock()
	currentEpoch := a.foregroundEpoch[residentID]
	a.memoryStateMu.Lock()
	defer a.memoryStateMu.Unlock()
	storedEpoch, exists := a.memoryNormalCycleEpochs[residentID]
	if !exists || storedEpoch != cycleEpoch {
		return false, false
	}
	if result.NextCursor != nil {
		if a.normalMemoryDiscoveryCursors == nil {
			a.normalMemoryDiscoveryCursors = make(map[canonical.ID]*domain.MemoryDiscoveryCursor)
		}
		a.normalMemoryDiscoveryCursors[residentID] = cloneMemoryDiscoveryCursor(result.NextCursor)
	}
	if result.Work != nil {
		if a.memoryNormalCyclesDirty == nil {
			a.memoryNormalCyclesDirty = make(map[canonical.ID]bool)
		}
		a.memoryNormalCyclesDirty[residentID] = true
	}
	if result.CycleComplete {
		if a.memoryNormalCyclesDirty[residentID] {
			// A cycle which returned work cannot prove that the selected item was
			// successfully terminalized. Re-scan the same fixed ceiling from its
			// beginning; capturing a newer ceiling here would let continuous normal
			// ingress postpone the re-extraction phase forever.
			cursor := a.normalMemoryDiscoveryCursors[residentID]
			if cursor == nil {
				return false, false
			}
			a.normalMemoryDiscoveryCursors[residentID] = &domain.MemoryDiscoveryCursor{
				CycleThroughSeq: cursor.CycleThroughSeq,
			}
			delete(a.memoryNormalCyclesDirty, residentID)
			delete(a.memoryReextractionCycles, residentID)
			delete(a.memoryReextractionCycleEpochs, residentID)
			delete(a.memoryNormalCompleteEpochs, residentID)
			return true, false
		}
		delete(a.normalMemoryDiscoveryCursors, residentID)
		delete(a.memoryNormalCycleEpochs, residentID)
		if currentEpoch != cycleEpoch {
			delete(a.memoryReextractionCycles, residentID)
			delete(a.memoryReextractionCycleEpochs, residentID)
			delete(a.memoryNormalCompleteEpochs, residentID)
			return true, false
		}
		if a.memoryReextractionCycles == nil {
			a.memoryReextractionCycles = make(map[canonical.ID]bool)
		}
		if a.memoryNormalCompleteEpochs == nil {
			a.memoryNormalCompleteEpochs = make(map[canonical.ID]uint64)
		}
		if a.memoryReextractionCycleEpochs == nil {
			a.memoryReextractionCycleEpochs = make(map[canonical.ID]uint64)
		}
		a.memoryReextractionCycles[residentID] = true
		a.memoryReextractionCycleEpochs[residentID] = cycleEpoch
		a.memoryNormalCompleteEpochs[residentID] = cycleEpoch
		return true, true
	}
	delete(a.memoryNormalCompleteEpochs, residentID)
	return true, false
}

func (a *Application) memoryReextractionCursor(residentID canonical.ID) *domain.MemoryReextractionDiscoveryCursor {
	a.memoryStateMu.Lock()
	defer a.memoryStateMu.Unlock()
	return cloneMemoryReextractionDiscoveryCursor(a.memoryReextractionCursors[residentID])
}

func (a *Application) startOrResumeMemoryReextractionCycleAtEpoch(
	residentID canonical.ID,
	expected uint64,
) (*domain.MemoryReextractionDiscoveryCursor, uint64, bool) {
	a.backgroundMu.Lock()
	defer a.backgroundMu.Unlock()
	if a.foregroundEpoch[residentID] != expected {
		return nil, 0, false
	}
	a.memoryStateMu.Lock()
	defer a.memoryStateMu.Unlock()
	if !a.memoryReextractionCycles[residentID] {
		return nil, 0, false
	}
	if a.memoryReextractionCycleEpochs == nil {
		a.memoryReextractionCycleEpochs = make(map[canonical.ID]uint64)
	}
	cycleEpoch, exists := a.memoryReextractionCycleEpochs[residentID]
	if !exists {
		cycleEpoch = expected
		a.memoryReextractionCycleEpochs[residentID] = cycleEpoch
	}
	cursor := a.memoryReextractionCursors[residentID]
	if cursor == nil {
		// No ceiling has been captured for this phase yet. A re-extraction
		// admitted before this call will therefore be included by the discovery
		// query which immediately follows while the resident lock is still held.
		delete(a.memoryReextractionRescanPending, residentID)
	}
	return cloneMemoryReextractionDiscoveryCursor(cursor), cycleEpoch, true
}

func (a *Application) setMemoryReextractionCursor(
	residentID canonical.ID,
	cursor *domain.MemoryReextractionDiscoveryCursor,
) {
	a.memoryStateMu.Lock()
	if a.memoryReextractionCursors == nil {
		a.memoryReextractionCursors = make(map[canonical.ID]*domain.MemoryReextractionDiscoveryCursor)
	}
	a.memoryReextractionCursors[residentID] = cloneMemoryReextractionDiscoveryCursor(cursor)
	a.memoryStateMu.Unlock()
}

func (a *Application) clearMemoryReextractionCursor(residentID canonical.ID) {
	a.memoryStateMu.Lock()
	delete(a.memoryReextractionCursors, residentID)
	a.memoryStateMu.Unlock()
}

func (a *Application) updateMemoryReextractionAtEpoch(
	residentID canonical.ID,
	cycleEpoch uint64,
	result domain.MemoryReextractionDiscoveryResult,
) (bool, bool) {
	a.backgroundMu.Lock()
	defer a.backgroundMu.Unlock()
	currentEpoch := a.foregroundEpoch[residentID]
	a.memoryStateMu.Lock()
	defer a.memoryStateMu.Unlock()
	storedEpoch, exists := a.memoryReextractionCycleEpochs[residentID]
	if !exists || storedEpoch != cycleEpoch {
		return false, false
	}
	if result.NextCursor != nil {
		if a.memoryReextractionCursors == nil {
			a.memoryReextractionCursors = make(map[canonical.ID]*domain.MemoryReextractionDiscoveryCursor)
		}
		a.memoryReextractionCursors[residentID] = cloneMemoryReextractionDiscoveryCursor(result.NextCursor)
	}
	if result.Work != nil {
		if a.memoryReextractionCyclesDirty == nil {
			a.memoryReextractionCyclesDirty = make(map[canonical.ID]bool)
		}
		a.memoryReextractionCyclesDirty[residentID] = true
	}
	if result.CycleComplete {
		if a.memoryReextractionCyclesDirty[residentID] {
			// Verify against the same finite commit ceiling that produced the work.
			// Recapturing MAX(commit_seq) would allow a continuous Admin
			// re-extraction stream to keep the normal phase blocked indefinitely.
			cursor := a.memoryReextractionCursors[residentID]
			if cursor == nil {
				return false, false
			}
			a.memoryReextractionCursors[residentID] = &domain.MemoryReextractionDiscoveryCursor{
				CycleThroughCommitSeq: cursor.CycleThroughCommitSeq,
			}
			delete(a.memoryReextractionCyclesDirty, residentID)
			if a.memoryReextractionCycles == nil {
				a.memoryReextractionCycles = make(map[canonical.ID]bool)
			}
			a.memoryReextractionCycles[residentID] = true
			return true, false
		}
		if a.memoryReextractionRescanPending[residentID] {
			// An Admin request was admitted beyond this cycle's immutable ceiling.
			// Finish the finite cycle for fairness, then require a fresh normal and
			// re-extraction verification before optional work can be admitted.
			delete(a.memoryReextractionCursors, residentID)
			delete(a.memoryReextractionCycles, residentID)
			delete(a.memoryReextractionCycleEpochs, residentID)
			delete(a.memoryNormalCompleteEpochs, residentID)
			return true, false
		}
		delete(a.memoryReextractionCursors, residentID)
		delete(a.memoryReextractionCycles, residentID)
		delete(a.memoryReextractionCycleEpochs, residentID)
		return true, currentEpoch == cycleEpoch
	}
	if !result.CycleComplete {
		if a.memoryReextractionCycles == nil {
			a.memoryReextractionCycles = make(map[canonical.ID]bool)
		}
		a.memoryReextractionCycles[residentID] = true
	}
	return true, false
}

func (a *Application) memoryReextractionCycleActive(residentID canonical.ID) bool {
	a.memoryStateMu.Lock()
	defer a.memoryStateMu.Unlock()
	return a.memoryReextractionCycles[residentID]
}

func (a *Application) memoryNormalDiscoveryCompleteAtEpoch(residentID canonical.ID, expected uint64) bool {
	a.memoryStateMu.Lock()
	defer a.memoryStateMu.Unlock()
	completed, ok := a.memoryNormalCompleteEpochs[residentID]
	return ok && completed == expected
}

func cloneMemoryReextractionDiscoveryCursor(
	cursor *domain.MemoryReextractionDiscoveryCursor,
) *domain.MemoryReextractionDiscoveryCursor {
	if cursor == nil {
		return nil
	}
	cloned := *cursor
	if cursor.CompletedThroughCommitSeq != nil {
		completed := *cursor.CompletedThroughCommitSeq
		cloned.CompletedThroughCommitSeq = &completed
	}
	if cursor.ActiveCommit != nil {
		active := *cursor.ActiveCommit
		if cursor.ActiveCommit.AfterRunID != nil {
			afterRunID := *cursor.ActiveCommit.AfterRunID
			active.AfterRunID = &afterRunID
		}
		cloned.ActiveCommit = &active
	}
	return &cloned
}

func memoryReextractionDiscoveryCursorsEqual(
	left *domain.MemoryReextractionDiscoveryCursor,
	right *domain.MemoryReextractionDiscoveryCursor,
) bool {
	if left == nil || right == nil {
		return left == right
	}
	if left.CycleThroughCommitSeq != right.CycleThroughCommitSeq ||
		(left.CompletedThroughCommitSeq == nil) != (right.CompletedThroughCommitSeq == nil) ||
		(left.ActiveCommit == nil) != (right.ActiveCommit == nil) {
		return false
	}
	if left.CompletedThroughCommitSeq != nil &&
		*left.CompletedThroughCommitSeq != *right.CompletedThroughCommitSeq {
		return false
	}
	if left.ActiveCommit == nil {
		return true
	}
	if left.ActiveCommit.CommitID != right.ActiveCommit.CommitID ||
		left.ActiveCommit.CommitSeq != right.ActiveCommit.CommitSeq ||
		(left.ActiveCommit.AfterRunID == nil) != (right.ActiveCommit.AfterRunID == nil) {
		return false
	}
	return left.ActiveCommit.AfterRunID == nil ||
		*left.ActiveCommit.AfterRunID == *right.ActiveCommit.AfterRunID
}

func (a *Application) setMemoryExtractionScansComplete(residentID canonical.ID, complete bool) {
	a.memoryStateMu.Lock()
	if a.memoryExtractionScansComplete == nil {
		a.memoryExtractionScansComplete = make(map[canonical.ID]bool)
	}
	a.memoryExtractionScansComplete[residentID] = complete
	a.memoryStateMu.Unlock()
}

func (a *Application) invalidateMemoryExtractionScansForReextraction(residentID canonical.ID) {
	a.memoryStateMu.Lock()
	if a.memoryExtractionScansComplete == nil {
		a.memoryExtractionScansComplete = make(map[canonical.ID]bool)
	}
	if a.memoryReextractionRescanPending == nil {
		a.memoryReextractionRescanPending = make(map[canonical.ID]bool)
	}
	a.memoryExtractionScansComplete[residentID] = false
	a.memoryReextractionRescanPending[residentID] = true
	a.memoryStateMu.Unlock()
}

func (a *Application) setMemoryExtractionScansCompleteAtEpoch(residentID canonical.ID, expected uint64) bool {
	a.backgroundMu.Lock()
	defer a.backgroundMu.Unlock()
	if a.foregroundEpoch[residentID] != expected {
		return false
	}
	a.memoryStateMu.Lock()
	defer a.memoryStateMu.Unlock()
	if a.memoryExtractionScansComplete == nil {
		a.memoryExtractionScansComplete = make(map[canonical.ID]bool)
	}
	a.memoryExtractionScansComplete[residentID] = true
	return true
}

func (a *Application) memoryExtractionScansAreComplete(residentID canonical.ID) bool {
	a.memoryStateMu.Lock()
	defer a.memoryStateMu.Unlock()
	return a.memoryExtractionScansComplete[residentID]
}

// resetMemoryDiscovery invalidates Operational proof when resident selection
// changes. The next selected turn begins from the oldest durable candidate.
func (a *Application) resetMemoryDiscovery(residentID canonical.ID) {
	a.memoryStateMu.Lock()
	delete(a.memoryDiscoveryCursors, residentID)
	delete(a.normalMemoryDiscoveryCursors, residentID)
	delete(a.memoryNormalCycleEpochs, residentID)
	delete(a.memoryReextractionCursors, residentID)
	delete(a.memoryReextractionCycles, residentID)
	delete(a.memoryReextractionCycleEpochs, residentID)
	delete(a.memoryReextractionRescanPending, residentID)
	delete(a.memoryNormalCyclesDirty, residentID)
	delete(a.memoryReextractionCyclesDirty, residentID)
	delete(a.memoryNormalCompleteEpochs, residentID)
	delete(a.memoryExtractionScansComplete, residentID)
	a.memoryStateMu.Unlock()
}

func (a *Application) processMemoryExtractionWork(ctx context.Context, work domain.MemoryExtractionWork) error {
	return a.processMemoryExtractionWorkWithEpoch(ctx, work, nil)
}

func (a *Application) processMemoryExtractionWorkWithEpoch(ctx context.Context, work domain.MemoryExtractionWork, fairEpoch *uint64) error {
	return a.processMemoryExtractionWorkWithAdmission(ctx, work, fairEpoch, nil)
}

func (a *Application) processMemoryExtractionWorkWithAdmission(
	ctx context.Context,
	work domain.MemoryExtractionWork,
	fairEpoch *uint64,
	admittedLease *backgroundCallLease,
) error {
	var prepared domain.PreparedGeneration
	var err error
	switch work.State {
	case domain.WorkPending:
		if admittedLease != nil {
			return errors.New("app: pending memory extraction already has a provider lease")
		}
		if fairEpoch != nil && a.foregroundAdvanced(work.SourceEvent.ResidentID, *fairEpoch) {
			return errMemoryForegroundPreempted
		}
		assembly, err := a.assembleMemoryExtraction(ctx, work)
		if err != nil {
			return err
		}
		if fairEpoch != nil && a.foregroundAdvanced(work.SourceEvent.ResidentID, *fairEpoch) {
			return errMemoryForegroundPreempted
		}
		expected := a.captureForegroundEpoch(work.SourceEvent.ResidentID)
		requireClean := true
		if fairEpoch != nil {
			expected = *fairEpoch
			requireClean = false
		}
		_, lease, submitErr := a.submitBackgroundMutationAndBeginCall(
			ctx, expected, requireClean,
			domain.PrepareGenerationCommand(assembly.Prepare), assembly.Contents,
		)
		if errors.Is(submitErr, errForegroundPreempted) {
			return errMemoryForegroundPreempted
		}
		if submitErr != nil {
			return fmt.Errorf("app: prepare memory extraction: %w", submitErr)
		}
		admittedLease = &lease
		if !lease.registered {
			return errMemoryForegroundPreempted
		}
		if err := ctx.Err(); err != nil {
			lease.finish()
			return err
		}
		prepared, err = a.repository.Generation(ctx, assembly.Prepare.RunID)
	case domain.WorkRunning:
		if work.RunID == nil {
			return errors.New("app: running memory extraction has no run")
		}
		prepared, err = a.repository.Generation(ctx, *work.RunID)
	case domain.WorkRetryPending:
		if admittedLease != nil {
			return errors.New("app: retry-pending memory extraction already has a provider lease")
		}
		if work.RunID == nil {
			return errors.New("app: retryable memory extraction has no run")
		}
		prepared, err = a.repository.Generation(ctx, *work.RunID)
		if err == nil {
			err = a.validateMemoryExtractionEnvelope(prepared)
		}
		if err != nil {
			return a.rejectMemoryExtractionEnvelope(ctx, work, err)
		}
		if !work.ForegroundPreempted && work.RetryCount > 0 && int(work.RetryCount) <= len(a.retryBackoff) {
			timer := time.NewTimer(a.retryBackoff[work.RetryCount-1])
			select {
			case <-ctx.Done():
				timer.Stop()
				return ctx.Err()
			case <-timer.C:
			}
		}
		outcomeID, err := a.ids.New()
		if err != nil {
			return err
		}
		expected := a.captureForegroundEpoch(work.SourceEvent.ResidentID)
		requireClean := true
		if fairEpoch != nil {
			expected = *fairEpoch
			requireClean = false
		}
		_, lease, submitErr := a.submitBackgroundMutationAndBeginCall(ctx, expected, requireClean,
			domain.StartAttemptCommand(domain.Attempt{
				RunID: *work.RunID, ResidentID: work.SourceEvent.ResidentID,
				AttemptNo: work.AttemptNo + 1, OutcomeID: outcomeID,
			}), nil)
		if errors.Is(submitErr, errForegroundPreempted) {
			return errMemoryForegroundPreempted
		}
		if submitErr != nil {
			return fmt.Errorf("app: start memory extraction retry: %w", submitErr)
		}
		admittedLease = &lease
		prepared, err = a.repository.Generation(ctx, *work.RunID)
	default:
		return fmt.Errorf("app: unsupported memory extraction work state %q", work.State)
	}
	if err != nil {
		if admittedLease != nil && admittedLease.registered {
			admittedLease.finish()
		}
		return a.rejectMemoryExtractionEnvelope(ctx, work, err)
	}
	if prepared.State != domain.WorkRunning {
		if admittedLease != nil && admittedLease.registered {
			admittedLease.finish()
		}
		return nil
	}
	if err := a.validateMemoryExtractionEnvelope(prepared); err != nil {
		if admittedLease != nil && admittedLease.registered {
			admittedLease.finish()
		}
		return a.rejectMemoryExtractionEnvelope(ctx, work, err)
	}
	return a.callAndLandMemoryExtractionWithAdmission(
		ctx, prepared, work.SourceEvent.ID, fairEpoch, admittedLease,
	)
}

func (a *Application) assembleMemoryExtraction(
	ctx context.Context,
	work domain.MemoryExtractionWork,
) (memoryExtractionAssembly, error) {
	repository, ok := a.repository.(domain.MemoryWorkRepository)
	if !ok {
		return memoryExtractionAssembly{}, errors.New("app: repository lacks memory work capability")
	}
	revisions, ok := a.repository.(domain.RevisionRepository)
	if !ok {
		return memoryExtractionAssembly{}, errors.New("app: repository lacks activation-aware revision capability")
	}
	resident, err := a.repository.Resident(ctx, work.SourceEvent.ResidentID)
	if err != nil {
		return memoryExtractionAssembly{}, err
	}
	if resident.Status != "active" || work.SourceEvent.ContentErased {
		return memoryExtractionAssembly{}, errors.New("app: memory extraction source is unavailable")
	}
	key := work.IdempotencyKey
	if key == "" {
		key = domain.MemoryExtractionObligation(work.SourceEvent.ID)
	}
	request, err := domain.ParseMemoryExtractionObligation(key)
	if err != nil || request.SourceEventID != work.SourceEvent.ID {
		return memoryExtractionAssembly{}, errors.New("app: memory extraction work key/source mismatch")
	}
	var policyRevision domain.ActiveRevision
	if request.Mode == domain.MemoryExtractionMandatory {
		policyRevision, err = revisions.ActiveRevisionForEvent(ctx, resident.ResidentID, "memory_policy", work.SourceEvent.ID)
		if err != nil {
			return memoryExtractionAssembly{}, err
		}
	} else {
		policyRevision = domain.ActiveRevision{
			RevisionID: resident.MemoryPolicyRevisionID, Content: resident.MemoryPolicy,
			ContentErased: resident.MemoryPolicy == "[erased]",
		}
	}
	if policyRevision.RevisionID != work.PolicyRevisionID || policyRevision.ContentErased {
		return memoryExtractionAssembly{}, errors.New("app: event-time memory policy changed or is erased")
	}
	policy, _, err := memory.ParsePolicy([]byte(policyRevision.Content))
	if err != nil {
		return memoryExtractionAssembly{}, err
	}
	if err := policy.RequireEnabled(); err != nil {
		return memoryExtractionAssembly{}, err
	}
	eventType := memory.EventType(work.SourceEvent.Type)
	if request.Mode == domain.MemoryExtractionMandatory && !memoryPolicyRequiresEvent(policy, eventType) {
		return memoryExtractionAssembly{}, errors.New("app: source event is not mandatory under its event-time memory policy")
	}
	pipeline, err := repository.PipelineVersion(ctx, "memory_extraction", domain.MemoryExtractionPipelineVersion)
	if err != nil {
		return memoryExtractionAssembly{}, err
	}
	expectedPipeline, err := canonical.MarshalCanonical(struct {
		Version string `json:"version"`
	}{Version: domain.MemoryExtractionPipelineVersion})
	if err != nil || !bytes.Equal(pipeline.Definition.Bytes(), expectedPipeline.Bytes()) {
		return memoryExtractionAssembly{}, errors.New("app: memory extraction pipeline definition mismatch")
	}
	maturation, err := repository.PipelineVersion(ctx, "memory_maturation", domain.MemoryMaturationPipelineVersion)
	if err != nil {
		return memoryExtractionAssembly{}, err
	}
	expectedMaturation, err := canonical.MarshalCanonical(struct {
		Version string `json:"version"`
	}{Version: domain.MemoryMaturationPipelineVersion})
	if err != nil || !bytes.Equal(maturation.Definition.Bytes(), expectedMaturation.Bytes()) {
		return memoryExtractionAssembly{}, errors.New("app: memory maturation pipeline definition mismatch")
	}
	contract, err := a.extractionSchemaContract(a.structuredOutputMode)
	if err != nil {
		return memoryExtractionAssembly{}, err
	}
	if err := contract.Validate(a.generatorCapabilities()); err != nil {
		return memoryExtractionAssembly{}, err
	}
	_, generatorParams, err := domain.NewStructuredGeneratorParams(
		false, canonical.ByteSize(a.maxOutputBytes), contract.Mode, contract.Version, contract.Hash,
	)
	if err != nil {
		return memoryExtractionAssembly{}, err
	}
	dropped, err := canonical.MarshalCanonical(struct {
		MemoryRecall string `json:"memory_recall"`
	}{MemoryRecall: "not_applicable"})
	if err != nil {
		return memoryExtractionAssembly{}, err
	}
	ids, err := a.allocateIDs(2)
	if err != nil {
		return memoryExtractionAssembly{}, err
	}
	assembly := memoryExtractionAssembly{Prepare: domain.PrepareGeneration{
		RunID: ids[0], ResidentID: resident.ResidentID, Purpose: domain.GenerationPurposeMemoryExtraction,
		IdempotencyKey: key,
		Provider:       a.provider, Model: a.model, PipelineVersionID: pipeline.ID,
		PrinciplesRevisionID: resident.PrinciplesRevisionID, PersonaRevisionID: resident.PersonaRevisionID,
		MemoryPolicyRevisionID: policyRevision.RevisionID, AsOf: work.SourceEvent.RecordedAt,
		AsOfTZ: work.SourceEvent.RecordedTZ, DroppedInputSummary: dropped,
		GeneratorParams: generatorParams, RunningOutcomeID: ids[1],
	}}
	if err := pinGenerationVersions(&assembly.Prepare); err != nil {
		return memoryExtractionAssembly{}, err
	}
	candidates := []struct {
		role, text, sourceType, inclusion string
		sourceID                          canonical.ID
	}{
		{string(generation.RoleSystem), resident.Principles, "resident_revision", "resident_definition", resident.PrinciplesRevisionID},
		{string(generation.RoleSystem), resident.Persona, "resident_revision", "resident_definition", resident.PersonaRevisionID},
		{string(generation.RoleSystem), policyRevision.Content, "resident_revision", "resident_definition", policyRevision.RevisionID},
		{string(generation.RoleUser), work.SourceEvent.Content, "event", "current_input", work.SourceEvent.ID},
	}
	total := 0
	for _, candidate := range candidates {
		total += len([]byte(candidate.text))
	}
	if total > a.maxInputBytes {
		return memoryExtractionAssembly{}, fmt.Errorf("app: mandatory memory extraction inputs exceed %d-byte budget", a.maxInputBytes)
	}
	for index, candidate := range candidates {
		content, err := a.newContent(resident.ResidentID, "generation_input", []byte(candidate.text), "independent")
		if err != nil {
			return memoryExtractionAssembly{}, err
		}
		inputID, err := a.ids.New()
		if err != nil {
			return memoryExtractionAssembly{}, err
		}
		sourceID := candidate.sourceID
		assembly.Prepare.Inputs = append(assembly.Prepare.Inputs, domain.GenerationInput{
			ID: inputID, Ordinal: int64(index), Role: candidate.role, SourceType: candidate.sourceType,
			SourceID: &sourceID, InclusionMode: candidate.inclusion, Content: content,
		})
		assembly.Contents = append(assembly.Contents, content)
	}
	return assembly, nil
}

func memoryPolicyRequiresEvent(policy memory.Policy, eventType memory.EventType) bool {
	for _, required := range policy.MandatoryEventTypes {
		if required == eventType {
			return true
		}
	}
	return false
}

func (a *Application) extractionSchemaContract(mode generation.StructuredOutputMode) (generation.SchemaContract, error) {
	schema, err := memory.ExtractionJSONSchema()
	if err != nil {
		return generation.SchemaContract{}, err
	}
	return generation.NewSchemaContract(
		"memory_extraction", memory.ExtractionOutputSchemaVersionV1, mode, schema,
	)
}

func (a *Application) generatorCapabilities() generation.Capabilities {
	if provider, ok := a.generator.(generation.CapabilityProvider); ok {
		return provider.Capabilities()
	}
	return generation.Capabilities{}
}

func (a *Application) validateMemoryExtractionEnvelope(prepared domain.PreparedGeneration) error {
	if prepared.Provider != a.provider {
		return errors.Join(ErrGenerationEnvelopeUnsupported,
			fmt.Errorf("%w: recorded=%q configured=%q", ErrGenerationProviderMismatch, prepared.Provider, a.provider))
	}
	if prepared.Purpose != domain.GenerationPurposeMemoryExtraction {
		return fmt.Errorf("%w: expected memory_extraction, got %q", ErrGenerationEnvelopeUnsupported, prepared.Purpose)
	}
	request, err := domain.ParseMemoryExtractionObligation(prepared.IdempotencyKey)
	if err != nil || request.SourceEventID.IsZero() {
		return fmt.Errorf("%w: invalid memory extraction idempotency key", ErrGenerationEnvelopeUnsupported)
	}
	if err := domain.ValidateGeneratorParamsForPurpose(prepared.Purpose, prepared.GeneratorParams); err != nil {
		return fmt.Errorf("%w: %v", ErrGenerationEnvelopeUnsupported, err)
	}
	if err := validatePreparedGenerationVersions(prepared); err != nil {
		return fmt.Errorf("%w: %v", ErrGenerationEnvelopeUnsupported, err)
	}
	if prepared.GeneratorParams.Streaming || prepared.GeneratorParams.SchemaHash == nil {
		return fmt.Errorf("%w: invalid memory extraction generator parameters", ErrGenerationEnvelopeUnsupported)
	}
	contract, err := a.extractionSchemaContract(prepared.GeneratorParams.StructuredOutputMode)
	if err != nil {
		return err
	}
	if contract.Version != prepared.GeneratorParams.SchemaVersion || contract.Hash != *prepared.GeneratorParams.SchemaHash {
		return fmt.Errorf("%w: structured-output schema version/hash mismatch", ErrGenerationEnvelopeUnsupported)
	}
	if err := contract.Validate(a.generatorCapabilities()); err != nil {
		return fmt.Errorf("%w: %v", ErrGenerationEnvelopeUnsupported, err)
	}
	return nil
}

func (a *Application) callAndLandMemoryExtraction(
	ctx context.Context,
	prepared domain.PreparedGeneration,
	sourceEventID canonical.ID,
) error {
	return a.callAndLandMemoryExtractionWithAdmission(ctx, prepared, sourceEventID, nil, nil)
}

func (a *Application) callAndLandMemoryExtractionWithEpoch(
	ctx context.Context,
	prepared domain.PreparedGeneration,
	sourceEventID canonical.ID,
	fairEpoch *uint64,
) error {
	return a.callAndLandMemoryExtractionWithAdmission(ctx, prepared, sourceEventID, fairEpoch, nil)
}

func (a *Application) callAndLandMemoryExtractionWithAdmission(
	ctx context.Context,
	prepared domain.PreparedGeneration,
	sourceEventID canonical.ID,
	fairEpoch *uint64,
	admittedLease *backgroundCallLease,
) error {
	if err := a.validateMemoryExtractionEnvelope(prepared); err != nil {
		return err
	}
	requestIdentity, err := domain.ParseMemoryExtractionObligation(prepared.IdempotencyKey)
	if err != nil || requestIdentity.SourceEventID != sourceEventID {
		return fmt.Errorf("%w: memory extraction key/source mismatch", ErrGenerationEnvelopeUnsupported)
	}
	source, err := a.repository.Event(ctx, prepared.ResidentID, sourceEventID)
	if err != nil {
		return a.recordMemoryExtractionFailure(ctx, prepared, generation.RuntimeInterruptedErrorCode(), nil)
	}
	contract, err := a.extractionSchemaContract(prepared.GeneratorParams.StructuredOutputMode)
	if err != nil {
		return err
	}
	request := generation.Request{
		GenerationRunID: prepared.RunID.String(), Purpose: string(prepared.Purpose), Model: prepared.Model,
		Streaming: false, MaxOutputBytes: int(prepared.GeneratorParams.MaxOutputBytes), StructuredOutput: &contract,
		Messages: make([]generation.Message, 0, len(prepared.Inputs)),
	}
	for _, input := range prepared.Inputs {
		request.Messages = append(request.Messages, generation.Message{Role: generation.Role(input.Role), Text: string(input.Content.Bytes)})
	}
	var lease backgroundCallLease
	if admittedLease != nil {
		lease = *admittedLease
	} else if fairEpoch != nil {
		lease = a.beginFairBackgroundCallAtEpoch(ctx, prepared.ResidentID, *fairEpoch)
	} else {
		lease = a.beginBackgroundCall(ctx, prepared.ResidentID)
	}
	if !lease.registered {
		return a.recordMemoryExtractionForegroundPreempted(ctx, prepared)
	}
	defer lease.finish()
	started := time.Now()
	result, providerErr := a.generator.Stream(lease.Context, request, func(generation.Delta) {})
	latency := time.Since(started).Microseconds()
	if a.backgroundCallPreempted(context.WithoutCancel(ctx), lease) {
		return a.recordMemoryExtractionForegroundPreempted(ctx, prepared)
	}
	if providerErr != nil {
		code := generation.OutcomeErrorCodeFromError(providerErr)
		if ctx.Err() != nil {
			code = generation.RuntimeInterruptedErrorCode()
		}
		return a.recordMemoryExtractionFailure(context.WithoutCancel(ctx), prepared, code, nil)
	}
	parsed, _, err := memory.ParseExtractionOutput([]byte(result.Text), source.Content)
	if err != nil {
		detail, contentErr := a.newContent(prepared.ResidentID, "error_detail", []byte(result.Text), "independent")
		if contentErr != nil {
			return contentErr
		}
		code := generation.MustOutcomeErrorCode(generation.ErrorInvalidResponse, 0)
		return a.recordMemoryExtractionFailure(ctx, prepared, code, &detail)
	}
	output, err := a.newContent(prepared.ResidentID, "generation_output", []byte(result.Text), "independent")
	if err != nil {
		return a.recordMemoryExtractionLandingFailure(ctx, prepared, err)
	}
	outcomeID, err := a.ids.New()
	if err != nil {
		return a.recordMemoryExtractionLandingFailure(ctx, prepared, err)
	}
	landing := domain.LandMemoryExtraction{
		Attempt:       domain.Attempt{RunID: prepared.RunID, ResidentID: prepared.ResidentID, AttemptNo: prepared.AttemptNo, OutcomeID: outcomeID},
		SourceEventID: sourceEventID, PipelineVersionID: mustPreparedPipelineID(prepared),
		MemoryPolicyRevisionID: mustPreparedMemoryPolicyID(prepared), Output: output,
		PromptTokens: result.PromptTokens, CompletionTokens: result.CompletionTokens, LatencyMicros: latency,
	}
	repository, ok := a.repository.(domain.MemoryWorkRepository)
	if !ok {
		return a.recordMemoryExtractionLandingFailure(ctx, prepared, errors.New("app: repository lacks memory work capability"))
	}
	maturation, err := repository.PipelineVersion(ctx, "memory_maturation", domain.MemoryMaturationPipelineVersion)
	if err != nil {
		return a.recordMemoryExtractionLandingFailure(ctx, prepared, err)
	}
	landing.MaturationPipelineVersionID = maturation.ID
	contents := []domain.Content{output}
	for _, claim := range parsed.Claims {
		statement, err := a.newContent(prepared.ResidentID, "claim_statement", []byte(claim.Statement), "independent")
		if err != nil {
			return a.recordMemoryExtractionLandingFailure(ctx, prepared, err)
		}
		ids, err := a.allocateIDs(6)
		if err != nil {
			return a.recordMemoryExtractionLandingFailure(ctx, prepared, err)
		}
		landing.Claims = append(landing.Claims, domain.ExtractedClaimLanding{
			ClaimID: ids[0], EvidenceID: ids[1], InitialStageID: ids[2],
			InitialViewScopeID: ids[3], SedimentStageTransitionID: ids[4],
			SettledStageTransitionID: ids[5], Statement: statement,
		})
		contents = append(contents, statement)
	}
	if _, err := a.submitBackgroundLandingWithContent(
		ctx, lease, domain.LandMemoryExtractionCommand(landing), contents,
	); errors.Is(err, errForegroundPreempted) {
		return a.recordMemoryExtractionForegroundPreempted(ctx, prepared)
	} else if err != nil {
		return a.recordMemoryExtractionLandingFailure(ctx, prepared, err)
	}
	return nil
}

func (a *Application) recordMemoryExtractionForegroundPreempted(
	ctx context.Context,
	prepared domain.PreparedGeneration,
) error {
	latest, err := a.repository.Generation(context.WithoutCancel(ctx), prepared.RunID)
	if err != nil {
		return err
	}
	if latest.State != domain.WorkRunning {
		// A synchronous lifecycle callback may already have appended the typed
		// terminal outcome for this exact attempt. Treat the later lease check as
		// an idempotent preemption instead of trying to append a second outcome.
		return errMemoryForegroundPreempted
	}
	code := generation.MustOutcomeErrorCode(generation.ErrorForegroundPreempted, 0)
	if err := a.recordMemoryExtractionFailure(context.WithoutCancel(ctx), prepared, code, nil); err != nil {
		latest, readErr := a.repository.Generation(context.WithoutCancel(ctx), prepared.RunID)
		if readErr == nil && latest.State != domain.WorkRunning {
			return errMemoryForegroundPreempted
		}
		return err
	}
	return errMemoryForegroundPreempted
}

func mustPreparedPipelineID(prepared domain.PreparedGeneration) canonical.ID {
	return prepared.PipelineVersionID
}

func mustPreparedMemoryPolicyID(prepared domain.PreparedGeneration) canonical.ID {
	return prepared.MemoryPolicyRevisionID
}

func (a *Application) recordMemoryExtractionFailure(
	ctx context.Context,
	prepared domain.PreparedGeneration,
	code generation.OutcomeErrorCode,
	detail *domain.Content,
) error {
	hardCtx := context.WithoutCancel(ctx)
	landing := a.backgroundLandingLock(prepared.ResidentID)
	landing.Lock()
	lifecycleTerminal, err := a.failureAttemptLifecycleTerminal(hardCtx, prepared)
	if err != nil {
		landing.Unlock()
		return fmt.Errorf("app: reload memory extraction before failure outcome: %w", err)
	}
	if lifecycleTerminal {
		// Archive owns the exact generation from its status commit onward, even
		// before its cleanup appends resident_inactive.
		landing.Unlock()
		return nil
	}
	outcomeID, err := a.ids.New()
	if err != nil {
		landing.Unlock()
		return err
	}
	state := "failed"
	if code.Class() == generation.ErrorForegroundPreempted || code.Class() == generation.ErrorRuntimeInterrupted {
		state = "cancelled"
	}
	command := domain.FailMemoryExtractionCommand(domain.FailMemoryExtraction{
		Attempt: domain.Attempt{RunID: prepared.RunID, ResidentID: prepared.ResidentID, AttemptNo: prepared.AttemptNo, OutcomeID: outcomeID},
		State:   state, ErrorClass: code.String(), ErrorDetail: detail,
	})
	var staged []blob.Staged
	if detail != nil {
		_, staged, err = a.stageContentCommand(hardCtx, command, []domain.Content{*detail})
		if err != nil {
			landing.Unlock()
			return err
		}
	}
	result, err := a.writer.Submit(hardCtx, command)
	if err == nil {
		landing.Unlock()
		a.afterSubmit(result, nil, prepared.ResidentID, true)
		if len(staged) != 0 {
			a.acknowledgeStagedContent(hardCtx, prepared.ResidentID, staged, result)
		}
		return nil
	}
	if errors.Is(err, canonical.ErrWriterPoisoned) {
		landing.Unlock()
		a.afterSubmit(result, err, prepared.ResidentID, true)
		return fmt.Errorf("app: record memory extraction failure: %w", err)
	}
	lifecycleTerminal, readErr := a.failureAttemptLifecycleTerminal(hardCtx, prepared)
	landing.Unlock()
	a.afterSubmit(result, err, prepared.ResidentID, true)
	if readErr == nil && lifecycleTerminal {
		return nil
	}
	return fmt.Errorf("app: record memory extraction failure: %w", errors.Join(err, readErr))
}

func (a *Application) recordMemoryExtractionLandingFailure(
	ctx context.Context,
	prepared domain.PreparedGeneration,
	cause error,
) error {
	if errors.Is(cause, canonical.ErrWriterPoisoned) {
		return fmt.Errorf("app: land memory extraction after provider success: %w", cause)
	}
	code := generation.MustOutcomeErrorCode(generation.ErrorLandingFailure, 0)
	if ctx.Err() != nil {
		code = generation.RuntimeInterruptedErrorCode()
	}
	if err := a.recordMemoryExtractionFailure(context.WithoutCancel(ctx), prepared, code, nil); err != nil {
		return errors.Join(cause, err)
	}
	return nil
}

func (a *Application) rejectMemoryExtractionEnvelope(
	ctx context.Context,
	work domain.MemoryExtractionWork,
	cause error,
) error {
	if cause == nil || errors.Is(cause, context.Canceled) || errors.Is(cause, context.DeadlineExceeded) {
		return cause
	}
	if work.RunID == nil {
		return cause
	}
	_, lifecycleWon, err := a.rejectGenerationEnvelopeAtResidentBoundary(
		ctx, work.SourceEvent.ResidentID, *work.RunID,
	)
	if err != nil {
		return errors.Join(cause, err)
	}
	if lifecycleWon {
		return nil
	}
	return nil
}

func (a *Application) cancelMemoryExtraction(ctx context.Context, work domain.MemoryExtractionWork) error {
	repository := a.repository.(domain.MemoryWorkRepository)
	key := work.IdempotencyKey
	if key == "" {
		key = domain.MemoryExtractionObligation(work.SourceEvent.ID)
	}
	request, requestErr := domain.ParseMemoryExtractionObligation(key)
	if requestErr != nil {
		return requestErr
	}
	if work.RunID == nil && request.Mode == domain.MemoryExtractionMandatory {
		resolver, ok := a.repository.(CancellationEnvelopeResolver)
		if ok {
			resolution, err := resolver.ResolveMandatoryCancellationEnvelope(ctx, domain.MandatoryRecoveryWork{
				Kind: domain.MandatoryRecoveryMemoryExtraction, SourceEvent: work.SourceEvent,
				IdempotencyKey: key, PolicyRevisionID: work.PolicyRevisionID,
				CancellationCode: work.CancellationCode,
			})
			if err != nil {
				return err
			}
			if resolution.Unresolved != nil {
				return errors.New("app: memory cancellation envelope is unresolvable")
			}
			ids, err := a.allocateIDs(3)
			if err != nil {
				return err
			}
			envelope := resolution.Generation
			envelope.RunID = ids[0]
			envelope.RunningOutcomeID = ids[1]
			result, err := a.submit(ctx, domain.CancelMemoryExtractionCommand(domain.CancelMemoryExtraction{
				Generation: envelope, SourceEventID: work.SourceEvent.ID,
				CancelledOutcomeID: ids[2], ErrorClass: work.CancellationCode,
			}))
			if err != nil {
				return fmt.Errorf("app: cancel memory extraction: %w", err)
			}
			if _, ok := result.Value.(domain.MemoryExtractionCancellationResult); !ok {
				return errors.New("app: memory cancellation returned an unexpected value")
			}
			return nil
		}
	}
	resident, err := a.repository.Resident(ctx, work.SourceEvent.ResidentID)
	if err != nil {
		return err
	}
	pipeline, err := repository.PipelineVersion(ctx, "memory_extraction", domain.MemoryExtractionPipelineVersion)
	if err != nil {
		return err
	}
	contract, err := a.extractionSchemaContract(a.structuredOutputMode)
	if err != nil {
		return err
	}
	_, params, err := domain.NewStructuredGeneratorParams(false, canonical.ByteSize(a.maxOutputBytes),
		contract.Mode, contract.Version, contract.Hash)
	if err != nil {
		return err
	}
	dropped, err := canonical.MarshalCanonical(struct {
		Reason string `json:"reason"`
	}{Reason: work.CancellationCode})
	if err != nil {
		return err
	}
	ids, err := a.allocateIDs(3)
	if err != nil {
		return err
	}
	runID := ids[0]
	if work.RunID != nil {
		runID = *work.RunID
	}
	generationEnvelope := domain.PrepareGeneration{
		RunID: runID, ResidentID: resident.ResidentID, Purpose: domain.GenerationPurposeMemoryExtraction,
		IdempotencyKey: key, Provider: a.provider, Model: a.model,
		PipelineVersionID: pipeline.ID, PrinciplesRevisionID: resident.PrinciplesRevisionID,
		PersonaRevisionID: resident.PersonaRevisionID, MemoryPolicyRevisionID: work.PolicyRevisionID,
		AsOf: work.SourceEvent.RecordedAt, AsOfTZ: work.SourceEvent.RecordedTZ,
		DroppedInputSummary: dropped, GeneratorParams: params, RunningOutcomeID: ids[1],
	}
	if err := pinGenerationVersions(&generationEnvelope); err != nil {
		return err
	}
	result, err := a.submit(ctx, domain.CancelMemoryExtractionCommand(domain.CancelMemoryExtraction{
		Generation:    generationEnvelope,
		SourceEventID: work.SourceEvent.ID, CancelledOutcomeID: ids[2], ErrorClass: work.CancellationCode,
	}))
	if err != nil {
		return fmt.Errorf("app: cancel memory extraction: %w", err)
	}
	if _, ok := result.Value.(domain.MemoryExtractionCancellationResult); !ok {
		return errors.New("app: memory cancellation returned an unexpected value")
	}
	return nil
}
