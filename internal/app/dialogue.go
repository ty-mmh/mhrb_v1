package app

import (
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"strings"
	"time"

	"mahoroba.local/mahoroba/internal/canonical"
	"mahoroba.local/mahoroba/internal/domain"
	"mahoroba.local/mahoroba/internal/generation"
	"mahoroba.local/mahoroba/internal/operationalmetrics"
)

const dialogueAssemblyAttemptLimit = 3

type dialogueProjectionReconciler interface {
	ReconcileDialogueAssembly(
		context.Context,
		canonical.ID,
	) (canonical.Head, canonical.Instant, canonical.Timezone, error)
}

type dialogueAssemblyRepository interface {
	AssembleDialogue(context.Context, domain.DialogueAssemblyRequest) (domain.DialogueAssemblyResult, error)
}

func (a *Application) ProcessResident(ctx context.Context, residentID canonical.ID) error {
	if err := a.ensureAccepting(); err != nil {
		return err
	}
	return a.processResident(ctx, residentID)
}

// startOrResumeDialogueScanCycle is called with the resident work lock held.
// It never nests dialogueStateMu with backgroundMu: a concurrent ingress may
// advance immediately after capture, and recordDialogueCleanAtEpoch will then
// reject this cycle at completion.
func (a *Application) startOrResumeDialogueScanCycle(residentID canonical.ID) dialogueScanCycle {
	a.dialogueStateMu.RLock()
	cycle, ok := a.dialogueScanCycles[residentID]
	a.dialogueStateMu.RUnlock()
	if ok {
		return cycle
	}
	cycle = dialogueScanCycle{Epoch: a.captureForegroundEpoch(residentID)}
	a.dialogueStateMu.Lock()
	if existing, exists := a.dialogueScanCycles[residentID]; exists {
		cycle = existing
	} else {
		if a.dialogueScanCycles == nil {
			a.dialogueScanCycles = make(map[canonical.ID]dialogueScanCycle)
		}
		a.dialogueScanCycles[residentID] = cycle
	}
	a.dialogueStateMu.Unlock()
	return cycle
}

func (a *Application) saveDialogueScanCycle(residentID canonical.ID, cycle dialogueScanCycle) {
	a.dialogueStateMu.Lock()
	if a.dialogueScanCycles == nil {
		a.dialogueScanCycles = make(map[canonical.ID]dialogueScanCycle)
	}
	a.dialogueScanCycles[residentID] = cycle
	a.dialogueStateMu.Unlock()
}

func (a *Application) finishDialogueScanCycle(residentID canonical.ID, expectedEpoch uint64) {
	a.dialogueStateMu.Lock()
	if cycle, ok := a.dialogueScanCycles[residentID]; ok && cycle.Epoch == expectedEpoch {
		delete(a.dialogueScanCycles, residentID)
	}
	a.dialogueStateMu.Unlock()
}

func (a *Application) processResident(ctx context.Context, residentID canonical.ID) error {
	lock := a.residentLock(residentID)
	lock.Lock()
	defer lock.Unlock()
	return a.processResidentLocked(ctx, residentID)
}

// processResidentLocked runs one bounded resident turn while its caller owns
// residentLock(residentID). Keeping the lock ownership explicit lets a hard
// archive cleanup avoid self-deadlock when ArchiveResident is invoked from a
// synchronous Observer callback inside an already-running resident turn.
func (a *Application) processResidentLocked(ctx context.Context, residentID canonical.ID) error {
	cycle := a.startOrResumeDialogueScanCycle(residentID)
	restartedForAdvancedEpoch := false
	for {
		result, err := a.repository.DiscoverDialogueWork(ctx, residentID, domain.DialogueDiscoveryRequest{
			Cursor: cycle.Cursor, MaxAttempts: a.maxAttempts,
			Budget: domain.ProductionDialogueDiscoveryBudget(),
		})
		if err != nil {
			return markMandatoryWorkerError(
				err, residentID, canonical.ID{},
				operationalmetrics.MandatoryWorkerPhaseDialogueScan, "",
			)
		}
		if result.NextCursor != nil {
			cycle.Cursor = result.NextCursor
			a.saveDialogueScanCycle(residentID, cycle)
		}
		var next *domain.DialogueWork
		for index := range result.Work {
			if result.Work[index].CancellationCode != "" {
				next = &result.Work[index]
				break
			}
			if result.Work[index].State == domain.WorkPending || result.Work[index].State == domain.WorkRetryPending ||
				result.Work[index].State == domain.WorkRunning {
				next = &result.Work[index]
				break
			}
		}
		if next == nil {
			if !result.CycleComplete {
				// The bounded scan did not establish that all older history is
				// complete. Keep the cycle epoch and cursor. The absence of a
				// matching clean proof fails closed for normal background work.
				a.saveDialogueScanCycle(residentID, cycle)
				return nil
			}
			a.finishDialogueScanCycle(residentID, cycle.Epoch)
			if !a.recordDialogueCleanAtEpoch(residentID, cycle.Epoch) {
				// Ingress advanced during this historical cycle. Do not reset the
				// cursor until the old cycle reached the end (otherwise continuous
				// ingress could starve old obligations). Once it is complete, allow
				// one immediate fresh cycle so a targeted worker can reach normal
				// background drain without waiting for another safety tick. Bound
				// this restart to one per invocation under continuous ingress.
				if restartedForAdvancedEpoch {
					return nil
				}
				cycle = a.startOrResumeDialogueScanCycle(residentID)
				restartedForAdvancedEpoch = true
				continue
			}
			return a.processMemoryExtractionQueue(ctx, residentID)
		}
		if next.CancellationCode != "" {
			if err := a.cancelDialogueWork(ctx, *next, nil, nil); err != nil {
				return markMandatoryWorkerError(
					err, residentID, next.UserEvent.ID,
					operationalmetrics.MandatoryWorkerPhaseAttemptTransition, "",
				)
			}
			yielded, err := a.handoffDialogueToFairMemory(
				ctx, residentID, next.UserEvent.Seq, next.CancellationCode,
			)
			if err != nil {
				return err
			}
			if yielded {
				return nil
			}
			continue
		}
		if a.generator == nil {
			return markMandatoryWorkerError(
				errors.New("app: generator is not configured"), residentID, next.UserEvent.ID,
				operationalmetrics.MandatoryWorkerPhaseFrozenRunRead,
				operationalmetrics.MandatoryWorkerErrorDependencyUnavailable,
			)
		}
		if err := a.processWork(ctx, residentID, *next); err != nil {
			return markMandatoryWorkerError(
				err, residentID, next.UserEvent.ID,
				operationalmetrics.MandatoryWorkerPhaseDialogueLanding, "",
			)
		}
		yielded, err := a.handoffDialogueToFairMemory(ctx, residentID, next.UserEvent.Seq, "")
		if err != nil {
			return err
		}
		if yielded {
			return nil
		}
	}
}

// handoffDialogueToFairMemory gives a durably advanced dialogue source its
// single fair mandatory-memory lease before the scan may advance to a newer
// dialogue. Durable cancellation and successful landing share this ordering
// boundary. Active-but-unselected memory is intentionally excluded by the M7
// contract: it remains pending until the resident is selected again.
func (a *Application) handoffDialogueToFairMemory(
	ctx context.Context,
	residentID canonical.ID,
	throughSeq canonical.Seq,
	dialogueCancellationCode string,
) (bool, error) {
	if dialogueCancellationCode != "" {
		code, err := generation.ParseOutcomeErrorCode(dialogueCancellationCode)
		if err != nil {
			return false, fmt.Errorf("app: parse dialogue handoff cancellation code: %w", err)
		}
		if code.Class() == generation.ErrorResidentUnselected {
			return true, nil
		}
	}
	// An already discovered foreground source gets one fair memory lease only
	// after its dialogue state has advanced durably. The through-seq bound
	// prevents a later ingress from being consumed by this turn.
	worked, err := a.processFairMandatoryMemory(ctx, residentID, throughSeq)
	if err != nil {
		if errors.Is(err, errMemoryForegroundPreempted) {
			return true, nil
		}
		return false, err
	}
	// Once one eligible fair-memory item was executed or terminalized, end this
	// resident turn. Without this yield, the surrounding dialogue backlog loop
	// could hand out another fair lease after processing the next dialogue.
	return worked, nil
}

func (a *Application) cancelDialogueWork(
	ctx context.Context,
	work domain.DialogueWork,
	expectedRunID *canonical.ID,
	expectedAttemptNo *int64,
) error {
	return a.cancelDialogueWorkWithObserver(ctx, work, expectedRunID, expectedAttemptNo, true)
}

func (a *Application) cancelDialogueWorkWithObserver(
	ctx context.Context,
	work domain.DialogueWork,
	expectedRunID *canonical.ID,
	expectedAttemptNo *int64,
	notifyObserver bool,
) error {
	code, err := generation.ParseOutcomeErrorCode(work.CancellationCode)
	if err != nil {
		return fmt.Errorf("app: parse dialogue cancellation code: %w", err)
	}
	if code.Class() != generation.ErrorSourceContentErased && code.Class() != generation.ErrorResidentInactive &&
		code.Class() != generation.ErrorResidentUnselected {
		return fmt.Errorf("app: unsupported dialogue cancellation code %q", code)
	}
	resident, err := a.repository.Resident(ctx, work.UserEvent.ResidentID)
	if err != nil {
		return err
	}
	ids, err := a.allocateIDs(3)
	if err != nil {
		return err
	}
	dropped, err := canonical.MarshalCanonical(struct {
		Backfill canonical.Count `json:"backfill"`
		Live     canonical.Count `json:"live_context"`
	}{Backfill: 0, Live: 0})
	if err != nil {
		return err
	}
	_, params, err := domain.NewUnstructuredGeneratorParams(
		false, canonical.ByteSize(a.maxOutputBytes),
	)
	if err != nil {
		return err
	}
	generationEnvelope := domain.PrepareGeneration{
		RunID: ids[0], ResidentID: resident.ResidentID,
		IdempotencyKey: domain.DialogueObligation(work.UserEvent.ID),
		Provider:       a.provider, Model: a.model,
		PromptTemplateVersion:  domain.DialoguePromptTemplateVersionV1,
		ContextPolicyVersion:   domain.DialogueContextPolicyVersionV3,
		MemoryRenderingVersion: domain.MemoryRenderingVersionV2,
		PipelineVersionID:      resident.PipelineVersionID,
		SessionPolicyID:        &resident.SessionPolicyID, PrinciplesRevisionID: resident.PrinciplesRevisionID,
		PersonaRevisionID: resident.PersonaRevisionID, MemoryPolicyRevisionID: resident.MemoryPolicyRevisionID,
		AsOf: work.UserEvent.RecordedAt, AsOfTZ: work.UserEvent.RecordedTZ,
		DroppedInputSummary: dropped, GeneratorParams: params, RunningOutcomeID: ids[1],
	}
	if work.RunID == nil && expectedRunID == nil {
		resolver, ok := a.repository.(CancellationEnvelopeResolver)
		if !ok {
			return errors.New("app: runless dialogue cancellation requires a source-time envelope resolver")
		}
		resolution, err := resolver.ResolveMandatoryCancellationEnvelope(ctx, domain.MandatoryRecoveryWork{
			Kind: domain.MandatoryRecoveryDialogue, SourceEvent: work.UserEvent,
			IdempotencyKey: domain.DialogueObligation(work.UserEvent.ID), CancellationCode: code.String(),
		})
		if err != nil {
			return err
		}
		if resolution.Unresolved != nil {
			return errors.New("app: dialogue cancellation envelope is unresolvable")
		}
		generationEnvelope = resolution.Generation
		generationEnvelope.RunID = ids[0]
		generationEnvelope.RunningOutcomeID = ids[1]
	}
	command := domain.CancelDialogueCommand(domain.CancelDialogue{
		Generation:    generationEnvelope,
		SourceEventID: work.UserEvent.ID, CancelledOutcomeID: ids[2], ErrorClass: code.String(),
		ExpectedRunID: expectedRunID, ExpectedAttemptNo: expectedAttemptNo,
	})
	result, err := a.submit(ctx, command)
	if err != nil {
		if errors.Is(err, canonical.ErrNoMutation) {
			return nil
		}
		return fmt.Errorf("app: cancel dialogue obligation: %w", err)
	}
	cancellation, ok := result.Value.(domain.DialogueCancellationResult)
	if !ok {
		return errors.New("app: dialogue cancellation returned an unexpected value")
	}
	if result.Commit.CommitID.IsZero() {
		// Canonical ErrNoMutation is returned as a successful logical replay
		// with no Commit metadata. Do not emit the terminal observer twice.
		return nil
	}
	if notifyObserver {
		a.withObserver(func(observer Observer) {
			observer.GenerationFailed(resident.ResidentID, cancellation.RunID, cancellation.AttemptNo, code.String(), true)
		})
	}
	return nil
}

func (a *Application) cancelArchivedResidentDialogues(
	ctx context.Context,
	residentID canonical.ID,
	canCancelRunless bool,
) error {
	var cursor *domain.DialogueDiscoveryCursor
	for {
		result, err := a.repository.DiscoverDialogueWork(ctx, residentID, domain.DialogueDiscoveryRequest{
			Cursor: cursor, MaxAttempts: a.maxAttempts,
			Budget: domain.ProductionDialogueDiscoveryBudget(),
		})
		if err != nil {
			return fmt.Errorf("app: discover archived resident dialogue work: %w", err)
		}
		for _, work := range result.Work {
			if work.CancellationCode == "" {
				continue
			}
			if work.RunID == nil && !canCancelRunless {
				continue
			}
			// Archive may be running synchronously inside this same Observer's
			// callback. The Canonical cancellation is authoritative; suppressing
			// nested notification avoids reacquiring an arbitrary observer mutex.
			if err := a.cancelDialogueWorkWithObserver(ctx, work, nil, nil, false); err != nil {
				return fmt.Errorf("app: cancel archived resident dialogue work: %w", err)
			}
		}
		if result.CycleComplete {
			return nil
		}
		if result.NextCursor == nil {
			return errors.New("app: archived resident dialogue discovery made no progress")
		}
		cursor = result.NextCursor
	}
}

func (a *Application) processWork(ctx context.Context, residentID canonical.ID, work domain.DialogueWork) error {
	var prepared domain.PreparedGeneration
	switch work.State {
	case domain.WorkPending:
		resident, err := a.repository.ActiveResident(ctx)
		if err != nil {
			return err
		}
		if resident.ResidentID != residentID || resident.Status != "active" {
			return errors.New("app: pending dialogue resident is not operationally active")
		}
		prepared, err = a.preparePendingDialogue(ctx, resident, work)
		if err != nil {
			// Assembly/Prepare failure leaves the source obligation pending; a
			// successful Commit B followed by a failed read leaves its run for
			// normal recovery. Neither case is an unsupported-envelope judgment.
			return err
		}
	case domain.WorkRunning:
		if work.RunID == nil {
			return errors.New("app: running work has no generation run")
		}
		selected, err := a.repository.ActiveResident(ctx)
		if err != nil {
			return err
		}
		if selected.Status != "active" || selected.ResidentID != residentID {
			return nil
		}
		prepared, err = a.repository.Generation(ctx, *work.RunID)
		if err != nil {
			return markMandatoryWorkerError(
				a.rejectUnsupportedGeneration(ctx, residentID, work, a.unsupportedEnvelopeError(err)),
				residentID, work.UserEvent.ID,
				operationalmetrics.MandatoryWorkerPhaseFrozenRunRead, "",
			)
		}
	case domain.WorkRetryPending:
		if work.RunID == nil {
			return errors.New("app: retry work has no generation run")
		}
		// Validate the immutable request envelope before changing the durable
		// attempt state. Unsupported work is terminalized atomically without a
		// provider call, rather than being stranded in running state.
		persisted, err := a.repository.Generation(ctx, *work.RunID)
		if err != nil {
			return markMandatoryWorkerError(
				a.rejectUnsupportedGeneration(ctx, residentID, work, a.unsupportedEnvelopeError(err)),
				residentID, work.UserEvent.ID,
				operationalmetrics.MandatoryWorkerPhaseFrozenRunRead, "",
			)
		}
		if err := a.validatePreparedGeneration(persisted); err != nil {
			return a.rejectUnsupportedGeneration(ctx, residentID, work, err)
		}
		if work.AttemptNo > 0 && int(work.AttemptNo) <= len(a.retryBackoff) {
			timer := time.NewTimer(a.retryBackoff[work.AttemptNo-1])
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
		attempt := domain.Attempt{RunID: *work.RunID, ResidentID: residentID, AttemptNo: work.AttemptNo + 1, OutcomeID: outcomeID}
		if _, err := a.submit(ctx, domain.StartAttemptCommand(attempt)); err != nil {
			return markMandatoryWorkerError(
				fmt.Errorf("app: start retry: %w", err), residentID, work.UserEvent.ID,
				operationalmetrics.MandatoryWorkerPhaseAttemptTransition, "",
			)
		}
		prepared, err = a.repository.Generation(ctx, *work.RunID)
		if err != nil {
			return a.rejectUnsupportedGeneration(ctx, residentID, domain.DialogueWork{
				RunID: work.RunID, AttemptNo: work.AttemptNo + 1, State: domain.WorkRunning,
			}, a.unsupportedEnvelopeError(err))
		}
	default:
		return fmt.Errorf("app: unsupported dialogue work state %q", work.State)
	}
	if prepared.State != domain.WorkRunning {
		return nil
	}
	if err := a.validatePreparedGeneration(prepared); err != nil {
		return a.rejectUnsupportedGeneration(ctx, residentID, work, err)
	}
	if err := a.callAndLand(ctx, prepared); err != nil {
		return markMandatoryWorkerError(
			err, residentID, work.UserEvent.ID,
			operationalmetrics.MandatoryWorkerPhaseDialogueLanding, "",
		)
	}
	return nil
}

func (a *Application) preparePendingDialogue(
	ctx context.Context,
	resident domain.ResidentSnapshot,
	work domain.DialogueWork,
) (domain.PreparedGeneration, error) {
	assembler, hasAssembler := a.repository.(dialogueAssemblyRepository)
	reconciler, hasReconciler := a.dialogueReconciler, a.dialogueReconciler != nil
	if !hasAssembler || !hasReconciler {
		return domain.PreparedGeneration{}, errors.New(
			"app: dialogue Assembly requires repository and Projection reconciliation capabilities",
		)
	}

	allocated, err := a.allocateIDs(3 + 2*domain.MaxDialogueInputs + domain.MaxRecallUsages)
	if err != nil {
		return domain.PreparedGeneration{}, err
	}
	inputSalts := make([]canonical.ContentSalt, domain.MaxDialogueInputs)
	for index := range inputSalts {
		inputSalts[index], err = canonical.NewContentSalt(rand.Reader)
		if err != nil {
			return domain.PreparedGeneration{}, err
		}
	}
	_, params, err := domain.NewUnstructuredGeneratorParams(true, canonical.ByteSize(a.maxOutputBytes))
	if err != nil {
		return domain.PreparedGeneration{}, err
	}
	request := domain.DialogueAssemblyRequest{
		SourceEventID:     work.UserEvent.ID,
		ResidentID:        resident.ResidentID,
		RunID:             allocated[0],
		RunningOutcomeID:  allocated[1],
		RecallRunID:       allocated[2],
		InputIDs:          allocated[3 : 3+domain.MaxDialogueInputs],
		InputContentIDs:   allocated[3+domain.MaxDialogueInputs : 3+2*domain.MaxDialogueInputs],
		InputContentSalts: inputSalts,
		RecallUsageIDs:    allocated[3+2*domain.MaxDialogueInputs:],
		Provider:          a.provider,
		Model:             a.model,
		GeneratorParams:   params,
		MaxInputBytes:     int64(a.maxInputBytes),
		LiveEventLimit:    domain.DialogueLiveEventLimit,
	}

	var lastAssemblyErr error
	for attempt := 0; attempt < dialogueAssemblyAttemptLimit; attempt++ {
		head, asOf, asOfTZ, reconcileErr := reconciler.ReconcileDialogueAssembly(ctx, resident.ResidentID)
		if reconcileErr != nil {
			lastAssemblyErr = markMandatoryWorkerError(
				fmt.Errorf("app: reconcile dialogue Assembly projections: %w", reconcileErr),
				resident.ResidentID, work.UserEvent.ID,
				operationalmetrics.MandatoryWorkerPhaseProjectionReconcile,
				operationalmetrics.MandatoryWorkerErrorDependencyUnavailable,
			)
			if ctx.Err() != nil {
				return domain.PreparedGeneration{}, errors.Join(lastAssemblyErr, ctx.Err())
			}
			continue
		}
		request.Target = domain.AssemblyTarget{Head: head, AsOf: asOf, AsOfTZ: asOfTZ}
		assembly, assembleErr := assembler.AssembleDialogue(ctx, request)
		if assembleErr != nil {
			lastAssemblyErr = markMandatoryWorkerError(
				fmt.Errorf("app: assemble dialogue Snapshot: %w", assembleErr),
				resident.ResidentID, work.UserEvent.ID,
				operationalmetrics.MandatoryWorkerPhaseDialogueAssembly, "",
			)
			if ctx.Err() != nil {
				return domain.PreparedGeneration{}, errors.Join(lastAssemblyErr, ctx.Err())
			}
			if !errors.Is(assembleErr, domain.ErrDialogueAssemblyTargetChanged) {
				return domain.PreparedGeneration{}, lastAssemblyErr
			}
			continue
		}
		result, prepareErr := a.submitWithContent(
			ctx,
			domain.PrepareDialogueCommand(assembly.Prepare),
			assembly.Contents,
		)
		if prepareErr != nil {
			lastAssemblyErr = markMandatoryWorkerError(
				fmt.Errorf("app: prepare dialogue: %w", prepareErr),
				resident.ResidentID, work.UserEvent.ID,
				operationalmetrics.MandatoryWorkerPhaseDialoguePrepare, "",
			)
			if !errors.Is(prepareErr, domain.ErrDialogueAssemblyTargetChanged) {
				return domain.PreparedGeneration{}, lastAssemblyErr
			}
			continue
		}
		committed, ok := result.Value.(domain.PrepareDialogueResult)
		if !ok {
			return domain.PreparedGeneration{}, errors.New("app: PrepareDialogue writer returned an unexpected value")
		}
		if err := committed.Validate(); err != nil {
			return domain.PreparedGeneration{}, err
		}
		switch committed.Resolution {
		case domain.PrepareDialoguePreparedCurrentV3,
			domain.PrepareDialogueExistingCurrentV3,
			domain.PrepareDialogueDispatchExistingFrozenRun:
			// The writer has already classified the idempotency key and, for
			// an existing run, revalidated the frozen envelope atomically. The
			// application only reads that exact run here; it never reassembles
			// or rewrites a legacy envelope.
		default:
			return domain.PreparedGeneration{}, fmt.Errorf("app: unsupported PrepareDialogue resolution %q", committed.Resolution)
		}
		prepared, err := a.repository.Generation(ctx, committed.RunID)
		if err != nil {
			return domain.PreparedGeneration{}, markMandatoryWorkerError(
				err, resident.ResidentID, work.UserEvent.ID,
				operationalmetrics.MandatoryWorkerPhaseFrozenRunRead, "",
			)
		}
		return prepared, nil
	}
	if lastAssemblyErr == nil {
		lastAssemblyErr = errors.New("app: dialogue Assembly did not run")
	}
	return domain.PreparedGeneration{}, fmt.Errorf(
		"app: dialogue Assembly Target did not stabilize after %d attempts: %w",
		dialogueAssemblyAttemptLimit,
		lastAssemblyErr,
	)
}

func (a *Application) processPreparedRun(ctx context.Context, residentID, runID canonical.ID) error {
	lock := a.residentLock(residentID)
	lock.Lock()
	defer lock.Unlock()
	if a.generator == nil {
		return errors.New("app: generator is not configured")
	}
	selected, err := a.repository.ActiveResident(ctx)
	if err != nil {
		return err
	}
	if selected.Status != "active" || selected.ResidentID != residentID {
		// Operational selection changed after the combined ingress commit. Do
		// not call the provider for the former selection; the cancellation scan
		// will close the durable obligation as resident_unselected/inactive.
		return nil
	}
	prepared, err := a.repository.Generation(ctx, runID)
	if err != nil {
		return a.rejectUnsupportedGeneration(ctx, residentID, domain.DialogueWork{
			RunID: &runID, AttemptNo: 1, State: domain.WorkRunning,
		}, a.unsupportedEnvelopeError(err))
	}
	if prepared.ResidentID != residentID {
		return errors.New("app: prepared run resident mismatch")
	}
	if prepared.State != domain.WorkRunning {
		return nil
	}
	if err := a.validatePreparedGeneration(prepared); err != nil {
		return a.rejectUnsupportedGeneration(ctx, residentID, domain.DialogueWork{
			RunID: &runID, AttemptNo: prepared.AttemptNo, State: prepared.State,
		}, err)
	}
	return a.callAndLand(ctx, prepared)
}

func (a *Application) callAndLand(ctx context.Context, prepared domain.PreparedGeneration) error {
	if err := a.validatePreparedGeneration(prepared); err != nil {
		return err
	}
	a.withObserver(func(observer Observer) {
		observer.GenerationStarted(prepared.ResidentID, prepared.RunID, prepared.AttemptNo)
	})
	stopped, err := a.stopDialogueAtResidentBoundary(context.WithoutCancel(ctx), prepared)
	if err != nil {
		return err
	}
	if stopped {
		return nil
	}
	request := generation.Request{
		GenerationRunID:  prepared.RunID.String(),
		Purpose:          string(prepared.Purpose.Effective()),
		Model:            prepared.Model,
		Streaming:        prepared.GeneratorParams.Streaming,
		MaxOutputBytes:   int(prepared.GeneratorParams.MaxOutputBytes),
		Messages:         make([]generation.Message, 0, len(prepared.Inputs)),
		StructuredOutput: nil,
	}
	for _, input := range prepared.Inputs {
		request.Messages = append(request.Messages, generation.Message{Role: generation.Role(input.Role), Text: string(input.Content.Bytes)})
	}
	started := time.Now()
	result, err := a.generator.Stream(ctx, request, func(delta generation.Delta) {
		a.withObserver(func(observer Observer) {
			observer.GenerationDelta(prepared.ResidentID, prepared.RunID, delta.Text)
		})
	})
	latency := time.Since(started).Microseconds()
	if err != nil {
		return a.recordFailure(ctx, prepared, err)
	}
	stopped, err = a.stopDialogueAtResidentBoundary(context.WithoutCancel(ctx), prepared)
	if err != nil {
		return err
	}
	if stopped {
		return nil
	}
	resident, err := a.repository.Resident(ctx, prepared.ResidentID)
	if err != nil {
		return a.recordLandingFailure(ctx, prepared, err)
	}
	output, err := a.newContent(prepared.ResidentID, "generation_output", []byte(result.Text), "independent")
	if err != nil {
		return a.recordLandingFailure(ctx, prepared, err)
	}
	ids, err := a.allocateIDs(2)
	if err != nil {
		return a.recordLandingFailure(ctx, prepared, err)
	}
	command := domain.LandDialogueCommand(domain.LandDialogue{
		Attempt: domain.Attempt{RunID: prepared.RunID, ResidentID: prepared.ResidentID, AttemptNo: prepared.AttemptNo, OutcomeID: ids[0]},
		EventID: ids[1], ResidentID: prepared.ResidentID, ResidentPrincipalID: resident.ResidentPrincipalID,
		OwnerPrincipalID: resident.OwnerPrincipalID, Output: output,
		OccurredAt: canonical.InstantFromTime(a.clock.Now()), OccurredTZ: a.timezone,
		PromptTokens: result.PromptTokens, CompletionTokens: result.CompletionTokens, LatencyMicros: latency,
	})
	var writerResult canonical.CommandResult
	for attempt := 0; attempt < 3; attempt++ {
		writerResult, err = a.submitWithContent(ctx, command, []domain.Content{output})
		if err == nil {
			break
		}
		if ctx.Err() != nil {
			return a.recordLandingFailure(ctx, prepared, errors.Join(err, ctx.Err()))
		}
	}
	if err != nil {
		return a.recordLandingFailure(ctx, prepared, err)
	}
	event, ok := writerResult.Value.(domain.Event)
	if !ok {
		return errors.New("app: landing writer returned an unexpected value")
	}
	a.withObserver(func(observer Observer) { observer.ResidentCommitted(event) })
	return nil
}

// stopDialogueAtResidentBoundary closes a running dialogue provider-free when
// an Observer callback or a concurrent lifecycle operation archived or
// unselected its resident. Reloading the durable run first also detects a
// scoped Archive recovery cancellation already committed by the callback.
func (a *Application) stopDialogueAtResidentBoundary(
	ctx context.Context,
	prepared domain.PreparedGeneration,
) (bool, error) {
	latest, err := a.repository.Generation(ctx, prepared.RunID)
	if err != nil {
		return false, err
	}
	if latest.State != domain.WorkRunning {
		return true, nil
	}
	resident, err := a.repository.Resident(ctx, prepared.ResidentID)
	if err != nil {
		return false, err
	}
	var code generation.OutcomeErrorCode
	if resident.Status != "active" {
		code = generation.MustOutcomeErrorCode(generation.ErrorResidentInactive, 0)
	} else {
		selected, err := a.repository.ActiveResident(ctx)
		if err != nil {
			return false, err
		}
		if selected.Status == "active" && selected.ResidentID == prepared.ResidentID {
			return false, nil
		}
		code = generation.MustOutcomeErrorCode(generation.ErrorResidentUnselected, 0)
	}
	const prefix = "dialogue:v1:"
	if !strings.HasPrefix(prepared.IdempotencyKey, prefix) {
		return false, fmt.Errorf("app: lifecycle dialogue has invalid obligation key %q", prepared.IdempotencyKey)
	}
	eventID, err := canonical.ParseID(strings.TrimPrefix(prepared.IdempotencyKey, prefix))
	if err != nil {
		return false, fmt.Errorf("app: parse lifecycle dialogue source event: %w", err)
	}
	event, err := a.repository.Event(ctx, prepared.ResidentID, eventID)
	if err != nil {
		return false, err
	}
	runID := prepared.RunID
	if err := a.cancelDialogueWork(ctx, domain.DialogueWork{
		UserEvent: event, RunID: &runID, AttemptNo: latest.AttemptNo,
		State: latest.State, CancellationCode: code.String(),
	}, nil, nil); err != nil {
		return false, err
	}
	return true, nil
}

func (a *Application) validatePreparedGeneration(prepared domain.PreparedGeneration) error {
	if prepared.Provider != a.provider {
		return errors.Join(ErrGenerationEnvelopeUnsupported,
			fmt.Errorf("%w: recorded=%q configured=%q", ErrGenerationProviderMismatch, prepared.Provider, a.provider))
	}
	if err := domain.ValidateGeneratorParamsForPurpose(prepared.Purpose, prepared.GeneratorParams); err != nil {
		return fmt.Errorf("%w: %v", ErrGenerationEnvelopeUnsupported, err)
	}
	if purpose := prepared.Purpose.Effective(); purpose != domain.GenerationPurposeDialogue {
		if err := domain.ValidateGenerationVersions(prepared.Purpose, domain.GenerationVersionContract{
			PromptTemplateVersion: prepared.PromptTemplateVersion, ContextPolicyVersion: prepared.ContextPolicyVersion,
			MemoryRenderingVersion: prepared.MemoryRenderingVersion,
		}); err != nil {
			return fmt.Errorf("%w: %v", ErrGenerationEnvelopeUnsupported, err)
		}
		return fmt.Errorf("%w: generation purpose %q is not dispatch-enabled", ErrGenerationEnvelopeUnsupported, purpose)
	}
	if err := domain.ValidateDialogueDispatchExecutionContract(domain.DialogueExecutionContract{
		PipelineVersionKey: prepared.PipelineVersionKey, PromptTemplateVersion: prepared.PromptTemplateVersion,
		ContextPolicyVersion: prepared.ContextPolicyVersion, MemoryRenderingVersion: prepared.MemoryRenderingVersion,
	}); err != nil {
		return fmt.Errorf("%w: %v", ErrGenerationEnvelopeUnsupported, err)
	}
	maxInt := int64(^uint(0) >> 1)
	if prepared.GeneratorParams.MaxOutputBytes <= 0 ||
		int64(prepared.GeneratorParams.MaxOutputBytes) > maxInt {
		return fmt.Errorf("%w: output limit is unsupported on this process", ErrGenerationEnvelopeUnsupported)
	}
	if !prepared.GeneratorParams.Streaming {
		return fmt.Errorf("%w: non-streaming dialogue execution is unsupported", ErrGenerationEnvelopeUnsupported)
	}
	return nil
}

func (a *Application) unsupportedEnvelopeError(cause error) error {
	if cause == nil || errors.Is(cause, context.Canceled) || errors.Is(cause, context.DeadlineExceeded) {
		return cause
	}
	return fmt.Errorf("%w: %v", ErrGenerationEnvelopeUnsupported, cause)
}

func (a *Application) rejectUnsupportedGeneration(ctx context.Context, residentID canonical.ID, work domain.DialogueWork, cause error) error {
	if cause == nil || errors.Is(cause, context.Canceled) || errors.Is(cause, context.DeadlineExceeded) {
		return cause
	}
	if work.RunID == nil || work.RunID.IsZero() {
		return cause
	}
	code := generation.MustOutcomeErrorCode(generation.ErrorProviderUnsupported, 0)
	rejected, lifecycleWon, err := a.rejectGenerationEnvelopeAtResidentBoundary(
		ctx, residentID, *work.RunID,
	)
	if err != nil {
		return errors.Join(cause, fmt.Errorf("app: terminalize unsupported generation envelope: %w", err))
	}
	if lifecycleWon {
		return nil
	}
	a.withObserver(func(observer Observer) {
		observer.GenerationFailed(residentID, *work.RunID, rejected.AttemptNo, code.String(), true)
	})
	return cause
}

func (a *Application) recordFailure(ctx context.Context, prepared domain.PreparedGeneration, providerErr error) error {
	code := generation.OutcomeErrorCodeFromError(providerErr)
	if ctx.Err() != nil {
		// A process/run-context cancellation is an interruption, not a terminal
		// judgment about the mandatory dialogue obligation. Persist a code that
		// recovery can classify as retryable on the next start.
		code = generation.RuntimeInterruptedErrorCode()
	}
	_, err := a.recordOutcome(context.WithoutCancel(ctx), prepared, code)
	return err
}

func (a *Application) recordLandingFailure(ctx context.Context, prepared domain.PreparedGeneration, cause error) error {
	landingErr := fmt.Errorf("app: land dialogue after provider success: %w", cause)
	// A poisoned Writer means Commit may have succeeded even though its caller
	// did not receive an acknowledgement. Any follow-up Canonical write would
	// be unsafe; leave the running row for startup recovery to reconcile after
	// the Writer reloads the durable head.
	if errors.Is(cause, canonical.ErrWriterPoisoned) {
		return landingErr
	}
	if errors.Is(cause, domain.ErrClaimSourceIneligible) {
		// A source can be erased after provider success but before the writer
		// commits the resident reply. Preserve the typed ineligibility error
		// while durably cancelling the existing obligation; never choose a
		// different claim or leave the latest running attempt alive.
		if err := a.cancelDialogueForErasedSource(context.WithoutCancel(ctx), prepared); err != nil {
			return errors.Join(landingErr, err)
		}
		return landingErr
	}
	code := generation.MustOutcomeErrorCode(generation.ErrorLandingFailure, 0)
	if ctx.Err() != nil {
		code = generation.RuntimeInterruptedErrorCode()
	}
	alreadyTerminal, err := a.recordOutcome(context.WithoutCancel(ctx), prepared, code)
	if err != nil {
		return errors.Join(landingErr, err)
	}
	if alreadyTerminal {
		return nil
	}
	// Do not immediately call the provider again while local landing is
	// unhealthy. The durable retryable outcome is rediscovered by the safety
	// scan or after restart.
	return landingErr
}

func (a *Application) cancelDialogueForErasedSource(ctx context.Context, prepared domain.PreparedGeneration) error {
	const prefix = "dialogue:v1:"
	if !strings.HasPrefix(prepared.IdempotencyKey, prefix) {
		return fmt.Errorf("app: erased dialogue source has invalid obligation key %q", prepared.IdempotencyKey)
	}
	eventID, err := canonical.ParseID(strings.TrimPrefix(prepared.IdempotencyKey, prefix))
	if err != nil {
		return fmt.Errorf("app: parse erased dialogue source event: %w", err)
	}
	event, err := a.repository.Event(ctx, prepared.ResidentID, eventID)
	if err != nil {
		return fmt.Errorf("app: load erased dialogue source event: %w", err)
	}
	runID := prepared.RunID
	attemptNo := prepared.AttemptNo
	if err := a.cancelDialogueWork(ctx, domain.DialogueWork{
		UserEvent: event, RunID: &runID, AttemptNo: prepared.AttemptNo,
		State: prepared.State, CancellationCode: generation.MustOutcomeErrorCode(generation.ErrorSourceContentErased, 0).String(),
	}, &runID, &attemptNo); err != nil {
		return fmt.Errorf("app: cancel erased dialogue source: %w", err)
	}
	return nil
}

func (a *Application) recordOutcome(
	ctx context.Context,
	prepared domain.PreparedGeneration,
	code generation.OutcomeErrorCode,
) (bool, error) {
	hardCtx := context.WithoutCancel(ctx)
	landing := a.backgroundLandingLock(prepared.ResidentID)
	landing.Lock()
	lifecycleTerminal, err := a.failureAttemptLifecycleTerminal(hardCtx, prepared)
	if err != nil {
		landing.Unlock()
		return false, fmt.Errorf("app: reload generation before failure outcome: %w", err)
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
		Attempt: domain.Attempt{RunID: prepared.RunID, ResidentID: prepared.ResidentID, AttemptNo: prepared.AttemptNo, OutcomeID: outcomeID},
		State:   "failed", ErrorClass: code.String(),
	})
	result, err := a.writer.Submit(hardCtx, command)
	if err != nil {
		if errors.Is(err, canonical.ErrWriterPoisoned) {
			landing.Unlock()
			a.afterSubmit(result, err, prepared.ResidentID, true)
			return false, fmt.Errorf("app: record generation failure: %w", err)
		}
		lifecycleTerminal, readErr := a.failureAttemptLifecycleTerminal(hardCtx, prepared)
		landing.Unlock()
		a.afterSubmit(result, err, prepared.ResidentID, true)
		if readErr == nil && lifecycleTerminal {
			return true, nil
		}
		return false, fmt.Errorf("app: record generation failure: %w", errors.Join(err, readErr))
	}
	landing.Unlock()
	a.afterSubmit(result, nil, prepared.ResidentID, true)
	terminal := !code.Retryable() || prepared.AttemptNo >= int64(a.maxAttempts)
	a.withObserver(func(observer Observer) {
		observer.GenerationFailed(prepared.ResidentID, prepared.RunID, prepared.AttemptNo, code.String(), terminal)
	})
	// A recorded generation error is durable work state, not a process failure.
	// ProcessResident will rediscover it and apply the configured retry policy.
	return false, nil
}
