package app

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"time"

	"mahoroba.local/mahoroba/internal/autonomy"
	"mahoroba.local/mahoroba/internal/canonical"
	"mahoroba.local/mahoroba/internal/domain"
	"mahoroba.local/mahoroba/internal/generation"
	"mahoroba.local/mahoroba/internal/memory"
)

var errAutonomyForegroundPreempted = errors.New("app: autonomous generation yielded to foreground dialogue")

type autonomousAssembly struct {
	Prepare  domain.PrepareAutonomousGeneration
	Contents []domain.Content
}

// ErasureCandidateSink receives metadata-only, best-effort retention hints.
// M6 never mutates content, blobs, or erasure state through this seam.
type ErasureCandidateSink interface {
	NotifyErasureCandidate(autonomy.RetentionCandidate) bool
}

func (a *Application) autonomySchedulingEnabled() bool {
	return a != nil && a.autonomyPolicy != nil && a.autonomySource != nil && a.autonomyClock != nil &&
		(a.autonomyPolicy.SelfTalk.Enabled || a.autonomyPolicy.Initiative.Enabled)
}

func (a *Application) retentionSchedulingEnabled() bool {
	return a != nil && a.autonomyPolicy != nil && a.autonomySource != nil && a.autonomyClock != nil &&
		a.autonomyPolicy.Retention.Mode == autonomy.RetentionCandidateAfter
}

func (a *Application) retentionLoop(ctx context.Context) {
	ticker := a.autonomyClock.NewTicker(a.autonomyPolicy.Retention.ScanInterval)
	defer ticker.Stop()
	a.scanRetentionCandidates(ctx)
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C():
			a.scanRetentionCandidates(ctx)
		}
	}
}

func (a *Application) scanRetentionCandidates(ctx context.Context) {
	residents, err := a.repository.ListResidents(ctx)
	if err != nil {
		return
	}
	for _, resident := range residents {
		if ctx.Err() != nil {
			return
		}
		now := a.autonomyClock.Now().Wall
		capture, err := a.autonomySource.RetentionSources(ctx, autonomy.RetentionRequest{
			ResidentID: resident.ResidentID, WallNow: now,
		})
		if err != nil {
			continue
		}
		candidates, err := autonomy.Candidates(a.autonomyPolicy.Retention, now, capture)
		if err != nil || a.erasureCandidateSink == nil {
			continue
		}
		for _, candidate := range candidates {
			_ = a.erasureCandidateSink.NotifyErasureCandidate(candidate)
		}
	}
}

func (a *Application) observeAutonomyEvent(event domain.Event) {
	if !a.autonomySchedulingEnabled() {
		return
	}
	now := a.autonomyClock.Now()
	anchor := &autonomy.ObservedAnchor{EventID: event.ID, At: now}
	a.autonomyAnchorMu.Lock()
	anchors := a.autonomyAnchors[event.ResidentID]
	switch event.Type {
	case "user_message":
		anchors.LastUser = anchor
	case "self_talk":
		anchors.LastSelfTalk = anchor
	case "outbound_initiative":
		anchors.LastInitiative = anchor
	default:
		a.autonomyAnchorMu.Unlock()
		return
	}
	a.autonomyAnchors[event.ResidentID] = anchors
	a.autonomyAnchorMu.Unlock()
}

func (a *Application) autonomyLoop(ctx context.Context) {
	ticker := a.autonomyClock.NewTicker(a.autonomyPolicy.ScanInterval)
	defer ticker.Stop()
	a.scanAutonomyResidents(ctx)
	for {
		select {
		case <-ctx.Done():
			return
		case residentID := <-a.autonomyHints:
			a.processAutonomyTurn(ctx, residentID)
		case <-ticker.C():
			a.scanAutonomyResidents(ctx)
		}
	}
}

func (a *Application) scanAutonomyResidents(ctx context.Context) {
	residents, err := a.repository.ListResidents(ctx)
	if err != nil {
		return
	}
	for _, resident := range residents {
		if ctx.Err() != nil {
			return
		}
		if resident.Status != "active" {
			continue
		}
		// Mandatory M5 recovery always runs before the one optional generation
		// this scheduler turn is allowed to start. Any foreground/mandatory
		// failure suppresses optional work for this resident and turn.
		a.processAutonomyTurn(ctx, resident.ResidentID)
	}
}

func (a *Application) processAutonomyTurn(ctx context.Context, residentID canonical.ID) {
	if err := a.ProcessResident(ctx, residentID); err != nil {
		return
	}
	// ProcessResident returns nil when a bounded historical scan is incomplete.
	// Optional work must wait for both a stable dialogue-clean proof and complete
	// normal/re-extraction memory sweeps. An empty bounded page is not proof that
	// no older mandatory memory work remains.
	if !a.currentForegroundClean(residentID) || !a.memoryExtractionScansAreComplete(residentID) {
		return
	}
	_ = a.processAutonomyResidentAfterMandatory(ctx, residentID)
}

// processAutonomyResidentAfterMandatory closes the gap between the scheduler's
// initial clean checks and resident-lock acquisition. Another mandatory turn
// may invalidate memory completeness while this turn is waiting for the lock;
// optional work must re-check both proofs after it owns the serialization
// boundary. Foreground ingress after this check is still rejected atomically by
// the background-call epoch admission used by every optional provider.
func (a *Application) processAutonomyResidentAfterMandatory(ctx context.Context, residentID canonical.ID) error {
	if !a.autonomySchedulingEnabled() {
		return nil
	}
	if !a.autonomyResidentSelected(ctx, residentID) {
		return nil
	}
	lock := a.residentLock(residentID)
	lock.Lock()
	defer lock.Unlock()
	if !a.currentForegroundClean(residentID) || !a.memoryExtractionScansAreComplete(residentID) {
		return nil
	}
	if !a.autonomyResidentSelected(ctx, residentID) {
		return nil
	}
	return a.processAutonomyOnce(ctx, residentID)
}

func (a *Application) processAutonomyResident(ctx context.Context, residentID canonical.ID) error {
	if !a.autonomySchedulingEnabled() {
		return nil
	}
	if !a.autonomyResidentSelected(ctx, residentID) {
		return nil
	}
	lock := a.residentLock(residentID)
	lock.Lock()
	defer lock.Unlock()
	return a.processAutonomyOnce(ctx, residentID)
}

func (a *Application) cancelAutonomousResidentWork(ctx context.Context, residentID canonical.ID) error {
	repository, ok := a.repository.(domain.AutonomyWorkRepository)
	if !ok {
		return nil
	}
	for {
		works, err := repository.DiscoverExecutableAutonomousWork(ctx, residentID, a.maxAttempts, 256)
		if err != nil {
			return err
		}
		if len(works) == 0 {
			return nil
		}
		cancelled := 0
		for _, work := range works {
			if work.RunID == nil {
				continue
			}
			ids, err := a.allocateIDs(2)
			if err != nil {
				return err
			}
			if _, err := a.submit(ctx, domain.CancelAutonomousGenerationCommand(domain.CancelAutonomousGeneration{
				RunID: *work.RunID, ResidentID: residentID,
				AttemptNo:        work.AttemptNo,
				RunningOutcomeID: ids[0], CancelledOutcomeID: ids[1],
				ErrorClass: generation.MustOutcomeErrorCode(generation.ErrorResidentInactive, 0).String(),
			})); err != nil && !errors.Is(err, canonical.ErrNoMutation) {
				return err
			}
			cancelled++
		}
		if cancelled == 0 {
			return errors.New("app: autonomous cancellation discovery made no progress")
		}
	}
}

func (a *Application) processAutonomyOnce(ctx context.Context, residentID canonical.ID) error {
	if !a.autonomyResidentSelected(ctx, residentID) {
		return nil
	}
	repository, ok := a.repository.(domain.AutonomyWorkRepository)
	if !ok {
		return errors.New("app: repository lacks autonomous work capability")
	}
	now := a.autonomyClock.Now()
	snapshot, err := a.autonomySource.AutonomySnapshot(ctx, autonomy.SnapshotRequest{
		ResidentID: residentID, WallNow: now.Wall, Timezone: a.timezone, MaxAttempts: a.maxAttempts,
	})
	if err != nil {
		return err
	}
	a.autonomyAnchorMu.Lock()
	anchors := a.autonomyAnchors[residentID]
	a.autonomyAnchorMu.Unlock()
	timing := autonomy.ResolveEvaluationTime(snapshot, now, anchors)
	executable, err := repository.DiscoverExecutableAutonomousWork(ctx, residentID, a.maxAttempts, 128)
	if err != nil {
		return err
	}
	sort.SliceStable(executable, func(i, j int) bool {
		left := autonomousPurposePriority(executable[i].Purpose)
		right := autonomousPurposePriority(executable[j].Purpose)
		return left < right
	})
	processDurable := func(purpose domain.GenerationPurpose) (bool, error) {
		for _, work := range executable {
			if work.Purpose != purpose {
				continue
			}
			if purpose == domain.GenerationPurposeSelfTalk && !a.autonomyPolicy.SelfTalk.Enabled ||
				purpose == domain.GenerationPurposeOutboundInitiative && !a.autonomyPolicy.Initiative.Enabled {
				continue
			}
			eligible, err := a.durableAutonomousWorkSchedulerEligible(ctx, repository, residentID, work)
			if err != nil {
				return false, err
			}
			if !eligible {
				continue
			}
			processed, err := a.processAutonomousTrigger(ctx, repository, residentID, work.Trigger)
			if err != nil || processed {
				return processed, err
			}
		}
		return false, nil
	}
	if processed, err := processDurable(domain.GenerationPurposeSelfTalk); err != nil || processed {
		return err
	}

	if a.autonomyPolicy.SelfTalk.Enabled {
		triggers, err := repository.DiscoverReevaluationTriggers(ctx, residentID, 128)
		if err != nil {
			return err
		}
		if snapshot.LastUser != nil {
			triggers = append(triggers, autonomy.Trigger{
				Kind: autonomy.TriggerIdle, SourceID: snapshot.LastUser.ID,
				Ordinal: snapshot.ConsecutiveSelfTalk + 1,
			})
		}
		for _, trigger := range triggers {
			processed, err := a.processExistingAutonomousTrigger(ctx, repository, residentID, trigger)
			if err != nil || processed {
				return err
			}
			eventTimeEligible, err := repository.AutonomousSelfTalkEventTimeEligible(ctx, residentID, trigger)
			if err != nil {
				return err
			}
			if !eventTimeEligible {
				continue
			}
			decision := autonomy.DecideSelfTalk(*a.autonomyPolicy, snapshot, trigger, timing)
			if !decision.Eligible {
				continue
			}
			processed, err = a.processAutonomousTrigger(ctx, repository, residentID, trigger)
			if err != nil || processed {
				return err
			}
		}
	}
	if processed, err := processDurable(domain.GenerationPurposeOutboundInitiative); err != nil || processed {
		return err
	}
	if a.autonomyPolicy.Initiative.Enabled {
		triggers, err := repository.DiscoverInitiativeTriggers(
			ctx, residentID, canonical.InstantFromTime(now.Wall), a.projectionMaxStaleness, 128,
		)
		if err != nil {
			return err
		}
		for _, trigger := range triggers {
			processed, err := a.processExistingAutonomousTrigger(ctx, repository, residentID, trigger)
			if err != nil || processed {
				return err
			}
			initiativeSnapshot := snapshot
			initiativeSnapshot.ProjectionAvailable = true
			decision := autonomy.DecideInitiative(*a.autonomyPolicy, initiativeSnapshot, trigger, timing)
			if !decision.Eligible {
				continue
			}
			processed, err = a.processAutonomousTrigger(ctx, repository, residentID, trigger)
			if err != nil || processed {
				return err
			}
		}
	}
	return nil
}

func (a *Application) processExistingAutonomousTrigger(
	ctx context.Context,
	repository domain.AutonomyWorkRepository,
	residentID canonical.ID,
	trigger autonomy.Trigger,
) (bool, error) {
	work, err := repository.AutonomousWork(ctx, residentID, trigger, a.maxAttempts)
	if err != nil {
		return false, err
	}
	if work.RunID == nil || (work.State != domain.WorkRunning && work.State != domain.WorkRetryPending) {
		return false, nil
	}
	eligible, err := a.durableAutonomousWorkSchedulerEligible(ctx, repository, residentID, work)
	if err != nil {
		return false, err
	}
	if !eligible {
		return false, nil
	}
	return a.processAutonomousTrigger(ctx, repository, residentID, trigger)
}

func (a *Application) durableAutonomousWorkSchedulerEligible(
	ctx context.Context,
	repository domain.AutonomyWorkRepository,
	residentID canonical.ID,
	work domain.AutonomousWork,
) (bool, error) {
	if work.RunID == nil {
		return false, nil
	}
	prepared, err := a.repository.Generation(ctx, *work.RunID)
	if err != nil {
		// Let the normal processor reject and durably terminalize an invalid
		// envelope rather than hiding it behind the optional-work gate.
		return true, nil
	}
	return a.currentAutonomousSchedulerEligible(ctx, repository, prepared, work.Trigger)
}

func autonomousPurposePriority(purpose domain.GenerationPurpose) int {
	switch purpose {
	case domain.GenerationPurposeSelfTalk:
		return 0
	case domain.GenerationPurposeOutboundInitiative:
		return 1
	default:
		return 2
	}
}

func (a *Application) autonomyResidentSelected(ctx context.Context, residentID canonical.ID) bool {
	selected, err := a.repository.ActiveResident(ctx)
	return err == nil && selected.Status == "active" && selected.ResidentID == residentID
}

func (a *Application) processAutonomousTrigger(
	ctx context.Context,
	repository domain.AutonomyWorkRepository,
	residentID canonical.ID,
	trigger autonomy.Trigger,
) (bool, error) {
	if !a.autonomyResidentSelected(ctx, residentID) {
		return false, nil
	}
	work, err := repository.AutonomousWork(ctx, residentID, trigger, a.maxAttempts)
	if err != nil {
		return false, err
	}
	if work.State == domain.WorkSucceeded || work.State == domain.WorkTerminalFailed {
		return false, nil
	}
	if a.generator == nil {
		return false, errors.New("app: generator is not configured for autonomy")
	}
	var prepared domain.PreparedGeneration
	var foregroundEpoch uint64
	gateChecked := false
	switch work.State {
	case domain.WorkPending:
		assembly, err := a.assembleAutonomous(ctx, repository, residentID, trigger)
		if err != nil {
			if errors.Is(err, domain.ErrAutonomousContextIneligible) {
				return false, nil
			}
			return false, err
		}
		if _, err := a.submitWithContent(ctx, domain.PrepareAutonomousGenerationCommand(assembly.Prepare), assembly.Contents); err != nil {
			return false, fmt.Errorf("app: prepare autonomous generation: %w", err)
		}
		preparedRunID := assembly.Prepare.Generation.RunID
		work.RunID, work.AttemptNo, work.State = &preparedRunID, 1, domain.WorkRunning
		prepared, err = a.repository.Generation(ctx, preparedRunID)
	case domain.WorkRunning:
		if work.RunID == nil {
			return false, errors.New("app: running autonomous work has no run")
		}
		prepared, err = a.repository.Generation(ctx, *work.RunID)
	case domain.WorkRetryPending:
		if work.RunID == nil {
			return false, errors.New("app: retrying autonomous work has no run")
		}
		prepared, err = a.repository.Generation(ctx, *work.RunID)
		if err == nil {
			err = a.validateAutonomousEnvelope(prepared, trigger)
		}
		if err != nil {
			return false, a.rejectAutonomousEnvelope(ctx, work, err)
		}
		if !work.ForegroundPreempted && work.RetryCount > 0 && work.RetryCount <= int64(len(a.retryBackoff)) {
			timer := time.NewTimer(a.retryBackoff[work.RetryCount-1])
			select {
			case <-ctx.Done():
				timer.Stop()
				return false, ctx.Err()
			case <-timer.C:
			}
		}
		foregroundEpoch = a.captureForegroundEpoch(residentID)
		eligible, err := a.currentAutonomousSchedulerEligible(ctx, repository, prepared, trigger)
		if err != nil {
			return false, err
		}
		if !eligible || !a.foregroundCleanAtEpoch(residentID, foregroundEpoch) {
			// No new running outcome exists yet, so a blocked retry remains
			// retry-pending without consuming its budget.
			return true, nil
		}
		gateChecked = true
		outcomeID, err := a.ids.New()
		if err != nil {
			return false, err
		}
		if _, err := a.submit(ctx, domain.StartAttemptCommand(domain.Attempt{
			RunID: prepared.RunID, ResidentID: residentID, AttemptNo: prepared.AttemptNo + 1,
			OutcomeID: outcomeID, MaxAttempts: a.maxAttempts,
		})); err != nil {
			return false, err
		}
		prepared, err = a.repository.Generation(ctx, prepared.RunID)
	default:
		return false, fmt.Errorf("app: unsupported autonomous work state %q", work.State)
	}
	if err != nil {
		return false, a.rejectAutonomousEnvelope(ctx, work, a.unsupportedEnvelopeError(err))
	}
	if err := a.validateAutonomousEnvelope(prepared, trigger); err != nil {
		return false, a.rejectAutonomousEnvelope(ctx, work, err)
	}
	if !gateChecked {
		foregroundEpoch = a.captureForegroundEpoch(residentID)
		eligible, err := a.currentAutonomousSchedulerEligible(ctx, repository, prepared, trigger)
		if err != nil {
			return false, err
		}
		if !eligible || !a.foregroundCleanAtEpoch(residentID, foregroundEpoch) {
			// A durable running attempt is intentionally left unchanged. The
			// next safety scan re-evaluates it after the blocking gate clears.
			return true, nil
		}
	}
	if !a.autonomyResidentSelected(ctx, residentID) {
		return false, nil
	}
	if err := a.callAndLandAutonomousAtEpoch(ctx, prepared, trigger, foregroundEpoch); err != nil {
		if errors.Is(err, errAutonomyForegroundPreempted) {
			return true, nil
		}
		return true, err
	}
	return true, nil
}

func (a *Application) currentAutonomousSchedulerEligible(
	ctx context.Context,
	repository domain.AutonomyWorkRepository,
	prepared domain.PreparedGeneration,
	trigger autonomy.Trigger,
) (bool, error) {
	if a.autonomyPolicy == nil || a.autonomySource == nil || a.autonomyClock == nil {
		return false, nil
	}
	var frozenPolicy autonomy.Policy
	var frozenMaxStaleness time.Duration
	var frozenEvidence *domain.AutonomousProjectionEvidence
	for _, input := range prepared.Inputs {
		if input.SourceType != "runtime_projection" {
			continue
		}
		_, frozenPolicy, frozenMaxStaleness, frozenEvidence, _ =
			domain.ParseAutonomousRuntimeProjection(input.Content.Bytes)
		break
	}
	if err := frozenPolicy.Validate(); err != nil {
		return false, err
	}
	now := a.autonomyClock.Now()
	snapshot, err := a.autonomySource.AutonomySnapshot(ctx, autonomy.SnapshotRequest{
		ResidentID: prepared.ResidentID, WallNow: now.Wall, Timezone: a.timezone,
		MaxAttempts: a.maxAttempts,
	})
	if err != nil {
		return false, err
	}
	a.autonomyAnchorMu.Lock()
	anchors := a.autonomyAnchors[prepared.ResidentID]
	a.autonomyAnchorMu.Unlock()
	timing := autonomy.ResolveEvaluationTime(snapshot, now, anchors)
	if trigger.Kind.IsSelfTalk() {
		eventTimeEligible, err := repository.AutonomousSelfTalkEventTimeEligible(ctx, prepared.ResidentID, trigger)
		if err != nil {
			return false, err
		}
		if !eventTimeEligible {
			return false, nil
		}
		return autonomy.DecideSelfTalk(frozenPolicy, snapshot, trigger, timing).Eligible, nil
	}
	if snapshot.MemoryPolicyVersion != string(memory.PolicyVersionV2) &&
		snapshot.MemoryPolicyVersion != string(memory.PolicyVersionV3) &&
		snapshot.MemoryPolicyVersion != string(memory.PolicyVersionV4) &&
		snapshot.MemoryPolicyVersion != string(memory.PolicyVersionV5) {
		return false, nil
	}
	if frozenEvidence != nil && frozenMaxStaleness > 0 {
		asOf := frozenEvidence.ClaimStates.AsOf.Time()
		snapshot.ProjectionAvailable = !asOf.After(now.Wall) && now.Wall.Sub(asOf) <= frozenMaxStaleness
	}
	availability, ok := repository.(interface {
		AutonomousInitiativeProjectionEligible(
			context.Context, canonical.ID, autonomy.Trigger, canonical.Instant, time.Duration,
		) (bool, error)
	})
	if !ok {
		return false, nil
	}
	available, err := availability.AutonomousInitiativeProjectionEligible(
		ctx, prepared.ResidentID, trigger, canonical.InstantFromTime(now.Wall), frozenMaxStaleness,
	)
	if err != nil {
		return false, err
	}
	snapshot.ProjectionAvailable = snapshot.ProjectionAvailable && available
	return autonomy.DecideInitiative(frozenPolicy, snapshot, trigger, timing).Eligible, nil
}

func (a *Application) assembleAutonomous(
	ctx context.Context,
	repository domain.AutonomyWorkRepository,
	residentID canonical.ID,
	trigger autonomy.Trigger,
) (autonomousAssembly, error) {
	resident, err := a.repository.Resident(ctx, residentID)
	if err != nil {
		return autonomousAssembly{}, err
	}
	if resident.Status != "active" {
		return autonomousAssembly{}, errors.New("app: autonomous resident is not active")
	}
	purpose := domain.GenerationPurposeSelfTalk
	pipelineKind, pipelineVersion := "self_talk", domain.SelfTalkPipelineVersion
	if trigger.Kind.IsInitiative() {
		purpose = domain.GenerationPurposeOutboundInitiative
		pipelineKind, pipelineVersion = "outbound_initiative", domain.OutboundInitiativePipelineVersion
	}
	memoryRepository, ok := a.repository.(domain.MemoryWorkRepository)
	if !ok {
		return autonomousAssembly{}, errors.New("app: repository lacks pipeline discovery for autonomy")
	}
	pipeline, err := memoryRepository.PipelineVersion(ctx, pipelineKind, pipelineVersion)
	if err != nil {
		return autonomousAssembly{}, err
	}
	now := canonical.InstantFromTime(a.autonomyClock.Now().Wall)
	var evidence *domain.AutonomousProjectionEvidence
	if trigger.Kind.IsInitiative() {
		captured, available, err := repository.AutonomousProjectionEvidence(ctx, residentID, now, a.projectionMaxStaleness)
		if err != nil {
			return autonomousAssembly{}, err
		}
		if !available {
			return autonomousAssembly{}, errors.New("app: initiative Projection evidence is unavailable")
		}
		evidence = &captured
	}
	runtimeJSON, err := domain.NewAutonomousRuntimeProjection(
		trigger, *a.autonomyPolicy, a.projectionMaxStaleness, evidence,
	)
	if err != nil {
		return autonomousAssembly{}, err
	}
	contexts, err := repository.AutonomousClaimContexts(ctx, residentID, trigger)
	if err != nil {
		return autonomousAssembly{}, err
	}
	versions, err := domain.GenerationVersionsForPurpose(purpose)
	if err != nil {
		return autonomousAssembly{}, err
	}
	_, params, err := domain.NewUnstructuredGeneratorParams(false, canonical.ByteSize(a.maxOutputBytes))
	if err != nil {
		return autonomousAssembly{}, err
	}
	dropped, err := canonical.MarshalCanonical(struct {
		Claims canonical.Count `json:"claims"`
	}{Claims: canonical.Count(len(contexts))})
	if err != nil {
		return autonomousAssembly{}, err
	}
	ids, err := a.allocateIDs(2)
	if err != nil {
		return autonomousAssembly{}, err
	}
	key, err := trigger.IdempotencyKey(string(a.autonomyPolicy.Version), residentID)
	if err != nil {
		return autonomousAssembly{}, err
	}
	assembly := autonomousAssembly{Prepare: domain.PrepareAutonomousGeneration{
		Generation: domain.PrepareGeneration{
			RunID: ids[0], ResidentID: residentID, Purpose: purpose, IdempotencyKey: key,
			Provider: a.provider, Model: a.model,
			PromptTemplateVersion: versions.PromptTemplateVersion, ContextPolicyVersion: versions.ContextPolicyVersion,
			MemoryRenderingVersion: versions.MemoryRenderingVersion, PipelineVersionID: pipeline.ID,
			PrinciplesRevisionID: resident.PrinciplesRevisionID, PersonaRevisionID: resident.PersonaRevisionID,
			MemoryPolicyRevisionID: resident.MemoryPolicyRevisionID, AsOf: now, AsOfTZ: a.timezone,
			DroppedInputSummary: dropped, GeneratorParams: params, RunningOutcomeID: ids[1],
		},
		MaxAttempts: a.maxAttempts, Trigger: trigger, Policy: *a.autonomyPolicy,
		ProjectionMaxStaleness: a.projectionMaxStaleness,
		ProjectionEvidence:     evidence,
	}}
	type rawInput struct {
		role, text, sourceType, mode string
		sourceID                     *canonical.ID
	}
	principlesID, personaID, memoryID := resident.PrinciplesRevisionID, resident.PersonaRevisionID, resident.MemoryPolicyRevisionID
	inputs := []rawInput{
		{"system", resident.Principles, "resident_revision", "resident_definition", &principlesID},
		{"system", resident.Persona, "resident_revision", "resident_definition", &personaID},
		{"system", resident.MemoryPolicy, "resident_revision", "resident_definition", &memoryID},
		{"system", runtimeJSON.String(), "runtime_projection", "runtime_projection", nil},
	}
	for index := range contexts {
		claimID := contexts[index].ClaimID
		inputs = append(inputs, rawInput{"system", contexts[index].Statement, "claim", "memory_recall", &claimID})
	}
	inputBytes := 0
	for index, item := range inputs {
		inputBytes += len([]byte(item.text))
		content, err := a.newContent(residentID, "generation_input", []byte(item.text), "independent")
		if err != nil {
			return autonomousAssembly{}, err
		}
		inputID, err := a.ids.New()
		if err != nil {
			return autonomousAssembly{}, err
		}
		assembly.Prepare.Generation.Inputs = append(assembly.Prepare.Generation.Inputs, domain.GenerationInput{
			ID: inputID, Ordinal: int64(index), Role: item.role, SourceType: item.sourceType,
			SourceID: item.sourceID, InclusionMode: item.mode, Content: content,
		})
		assembly.Contents = append(assembly.Contents, content)
	}
	if inputBytes > a.maxInputBytes {
		return autonomousAssembly{}, fmt.Errorf("app: mandatory autonomous inputs exceed %d-byte budget", a.maxInputBytes)
	}
	for index, claim := range contexts {
		usageID, err := a.ids.New()
		if err != nil {
			return autonomousAssembly{}, err
		}
		ordinal, _ := canonical.NewOrdinal(int64(index))
		assembly.Prepare.Usages = append(assembly.Prepare.Usages, domain.AutonomousClaimUsage{
			ID: usageID, ClaimID: claim.ClaimID, Ordinal: ordinal,
		})
	}
	return assembly, nil
}

func (a *Application) validateAutonomousEnvelope(
	prepared domain.PreparedGeneration,
	trigger autonomy.Trigger,
) error {
	if prepared.Provider != a.provider || prepared.SessionPolicyID != nil || prepared.RecallRunID != nil ||
		(prepared.Purpose != domain.GenerationPurposeSelfTalk && prepared.Purpose != domain.GenerationPurposeOutboundInitiative) {
		return fmt.Errorf("%w: autonomous generation identity mismatch", ErrGenerationEnvelopeUnsupported)
	}
	if prepared.Purpose == domain.GenerationPurposeSelfTalk && !trigger.Kind.IsSelfTalk() ||
		prepared.Purpose == domain.GenerationPurposeOutboundInitiative && !trigger.Kind.IsInitiative() {
		return fmt.Errorf("%w: autonomous purpose/trigger mismatch", ErrGenerationEnvelopeUnsupported)
	}
	wantKey, err := trigger.IdempotencyKey(string(a.autonomyPolicy.Version), prepared.ResidentID)
	if err != nil || prepared.IdempotencyKey != wantKey {
		return fmt.Errorf("%w: autonomous trigger key mismatch", ErrGenerationEnvelopeUnsupported)
	}
	if err := domain.ValidateGeneratorParamsForPurpose(prepared.Purpose, prepared.GeneratorParams); err != nil ||
		prepared.GeneratorParams.Streaming {
		return fmt.Errorf("%w: autonomous generator params mismatch", ErrGenerationEnvelopeUnsupported)
	}
	if err := validatePreparedGenerationVersions(prepared); err != nil {
		return err
	}
	runtimeCount := 0
	for _, input := range prepared.Inputs {
		switch input.SourceType {
		case "resident_revision":
		case "runtime_projection":
			runtimeCount++
			persistedTrigger, persistedPolicy, _, evidence, err := domain.ParseAutonomousRuntimeProjection(input.Content.Bytes)
			if err != nil {
				return fmt.Errorf("%w: %v", ErrGenerationEnvelopeUnsupported, err)
			}
			wantIdentity, _ := trigger.StableIdentity()
			gotIdentity, _ := persistedTrigger.StableIdentity()
			if gotIdentity != wantIdentity || (trigger.Kind.IsInitiative() != (evidence != nil)) {
				return fmt.Errorf("%w: autonomous runtime projection mismatch", ErrGenerationEnvelopeUnsupported)
			}
			if a.autonomyPolicy == nil || persistedPolicy.Version != a.autonomyPolicy.Version {
				return fmt.Errorf("%w: frozen autonomy policy version is unsupported", ErrGenerationEnvelopeUnsupported)
			}
		case "claim":
			if input.SourceID == nil || input.InclusionMode != "memory_recall" {
				return fmt.Errorf("%w: invalid autonomous claim input", ErrGenerationEnvelopeUnsupported)
			}
		default:
			return fmt.Errorf("%w: unsupported autonomous input source %q", ErrGenerationEnvelopeUnsupported, input.SourceType)
		}
	}
	if runtimeCount != 1 {
		return fmt.Errorf("%w: autonomous runtime projection count mismatch", ErrGenerationEnvelopeUnsupported)
	}
	return nil
}

func (a *Application) callAndLandAutonomous(
	ctx context.Context,
	prepared domain.PreparedGeneration,
	trigger autonomy.Trigger,
) error {
	return a.callAndLandAutonomousAtEpoch(
		ctx, prepared, trigger, a.captureForegroundEpoch(prepared.ResidentID),
	)
}

func (a *Application) callAndLandAutonomousAtEpoch(
	ctx context.Context,
	prepared domain.PreparedGeneration,
	trigger autonomy.Trigger,
	foregroundEpoch uint64,
) error {
	request := generation.Request{
		GenerationRunID: prepared.RunID.String(), Purpose: string(prepared.Purpose), Model: prepared.Model,
		Streaming: false, MaxOutputBytes: int(prepared.GeneratorParams.MaxOutputBytes),
		Messages: make([]generation.Message, 0, len(prepared.Inputs)),
	}
	var evidence *domain.AutonomousProjectionEvidence
	var frozenPolicy autonomy.Policy
	var frozenMaxStaleness time.Duration
	for _, input := range prepared.Inputs {
		request.Messages = append(request.Messages, generation.Message{Role: generation.Role(input.Role), Text: string(input.Content.Bytes)})
		if input.SourceType == "runtime_projection" {
			_, frozenPolicy, frozenMaxStaleness, evidence, _ = domain.ParseAutonomousRuntimeProjection(input.Content.Bytes)
		}
	}
	providerCtx, finish, registered := a.beginBackgroundCallAtEpoch(
		ctx, prepared.ResidentID, foregroundEpoch, true,
	)
	if !registered {
		if err := a.recordAutonomousForegroundPreempted(context.WithoutCancel(ctx), prepared); err != nil {
			return err
		}
		return errAutonomyForegroundPreempted
	}
	lease := backgroundCallLease{
		Context: providerCtx, residentID: prepared.ResidentID, epoch: foregroundEpoch,
		finish: finish, registered: true,
	}
	defer lease.finish()
	started := time.Now()
	result, providerErr := a.generator.Stream(providerCtx, request, func(generation.Delta) {})
	latency := time.Since(started).Microseconds()
	cause := context.Cause(providerCtx)
	if errors.Is(cause, errForegroundPreempted) || a.foregroundAdvanced(prepared.ResidentID, foregroundEpoch) {
		if err := a.recordAutonomousForegroundPreempted(context.WithoutCancel(ctx), prepared); err != nil {
			return err
		}
		return errAutonomyForegroundPreempted
	}
	if providerErr != nil {
		code := generation.OutcomeErrorCodeFromError(providerErr)
		state := "failed"
		if ctx.Err() != nil {
			code = generation.RuntimeInterruptedErrorCode()
			state = "cancelled"
		}
		if err := a.recordAutonomousFailure(context.WithoutCancel(ctx), prepared, state, code); err != nil {
			return err
		}
		return nil
	}
	if !a.foregroundCleanAtEpoch(prepared.ResidentID, foregroundEpoch) {
		if err := a.recordAutonomousForegroundPreempted(context.WithoutCancel(ctx), prepared); err != nil {
			return err
		}
		return errAutonomyForegroundPreempted
	}
	resident, err := a.repository.Resident(ctx, prepared.ResidentID)
	if err != nil {
		return a.recordAutonomousLandingOrPreemption(ctx, prepared, foregroundEpoch, err)
	}
	output, err := a.newContent(prepared.ResidentID, "generation_output", []byte(result.Text), "independent")
	if err != nil {
		return a.recordAutonomousLandingOrPreemption(ctx, prepared, foregroundEpoch, err)
	}
	ids, err := a.allocateIDs(2)
	if err != nil {
		return a.recordAutonomousLandingOrPreemption(ctx, prepared, foregroundEpoch, err)
	}
	command := domain.LandAutonomousEventCommand(domain.LandAutonomousEvent{
		Attempt: domain.Attempt{RunID: prepared.RunID, ResidentID: prepared.ResidentID,
			AttemptNo: prepared.AttemptNo, OutcomeID: ids[0], MaxAttempts: a.maxAttempts},
		EventID: ids[1], ResidentPrincipalID: resident.ResidentPrincipalID, OwnerPrincipalID: resident.OwnerPrincipalID,
		Output: output, OccurredAt: canonical.InstantFromTime(a.autonomyClock.Now().Wall), OccurredTZ: a.timezone,
		PromptTokens: result.PromptTokens, CompletionTokens: result.CompletionTokens, LatencyMicros: latency,
		Trigger: trigger, Policy: frozenPolicy, ProjectionMaxStaleness: frozenMaxStaleness,
		ProjectionEvidence: evidence,
	})
	writerResult, err := a.submitBackgroundLandingWithContent(ctx, lease, command, []domain.Content{output})
	if err != nil {
		return a.recordAutonomousLandingOrPreemption(ctx, prepared, foregroundEpoch, err)
	}
	event, ok := writerResult.Value.(domain.Event)
	if !ok {
		return errors.New("app: autonomous landing returned an unexpected value")
	}
	a.observeAutonomyEvent(event)
	if event.Type == "outbound_initiative" {
		a.withObserver(func(observer Observer) { observer.ResidentCommitted(event) })
	}
	return nil
}

func (a *Application) recordAutonomousForegroundPreempted(
	ctx context.Context,
	prepared domain.PreparedGeneration,
) error {
	latest, err := a.repository.Generation(context.WithoutCancel(ctx), prepared.RunID)
	if err != nil {
		return err
	}
	if latest.State != domain.WorkRunning {
		return nil
	}
	err = a.recordAutonomousFailure(ctx, prepared, "cancelled",
		generation.MustOutcomeErrorCode(generation.ErrorForegroundPreempted, 0))
	if err == nil {
		return nil
	}
	latest, readErr := a.repository.Generation(context.WithoutCancel(ctx), prepared.RunID)
	if readErr == nil && latest.State != domain.WorkRunning {
		return nil
	}
	return err
}

func (a *Application) recordAutonomousLandingOrPreemption(
	ctx context.Context,
	prepared domain.PreparedGeneration,
	foregroundEpoch uint64,
	cause error,
) error {
	// A poisoned Writer makes the landing result ambiguous: the event/outcome
	// may already be durable even when the caller also observed a typed
	// revalidation error. Never issue a follow-up cancellation or preemption
	// write; startup recovery reloads the durable head and reconciles the
	// still-running view when the landing did not commit.
	if errors.Is(cause, canonical.ErrWriterPoisoned) {
		return cause
	}
	if errors.Is(cause, domain.ErrClaimSourceIneligible) {
		if err := a.cancelAutonomousForErasedSource(context.WithoutCancel(ctx), prepared); err != nil {
			return errors.Join(cause, err)
		}
		return cause
	}
	if !a.foregroundCleanAtEpoch(prepared.ResidentID, foregroundEpoch) {
		if err := a.recordAutonomousForegroundPreempted(context.WithoutCancel(ctx), prepared); err != nil {
			return errors.Join(cause, err)
		}
		return errAutonomyForegroundPreempted
	}
	return a.recordAutonomousLandingFailure(ctx, prepared, cause)
}

func (a *Application) cancelAutonomousForErasedSource(
	ctx context.Context,
	prepared domain.PreparedGeneration,
) error {
	ids, err := a.allocateIDs(2)
	if err != nil {
		return err
	}
	code := generation.MustOutcomeErrorCode(generation.ErrorSourceContentErased, 0)
	_, err = a.submit(ctx, domain.CancelAutonomousGenerationCommand(domain.CancelAutonomousGeneration{
		RunID: prepared.RunID, ResidentID: prepared.ResidentID,
		AttemptNo:        prepared.AttemptNo,
		RunningOutcomeID: ids[0], CancelledOutcomeID: ids[1], ErrorClass: code.String(),
	}))
	if err != nil && !errors.Is(err, canonical.ErrNoMutation) {
		return fmt.Errorf("app: cancel autonomous work with erased claim source: %w", err)
	}
	return nil
}

func (a *Application) recordAutonomousFailure(
	ctx context.Context,
	prepared domain.PreparedGeneration,
	state string,
	code generation.OutcomeErrorCode,
) error {
	_, err := a.recordBackgroundAttemptFailure(ctx, prepared, state, code)
	return err
}

func (a *Application) recordAutonomousLandingFailure(
	ctx context.Context,
	prepared domain.PreparedGeneration,
	cause error,
) error {
	if errors.Is(cause, canonical.ErrWriterPoisoned) {
		return cause
	}
	code := generation.MustOutcomeErrorCode(generation.ErrorLandingFailure, 0)
	if ctx.Err() != nil {
		code = generation.RuntimeInterruptedErrorCode()
	}
	if err := a.recordAutonomousFailure(context.WithoutCancel(ctx), prepared, "failed", code); err != nil {
		return errors.Join(cause, err)
	}
	return cause
}

func (a *Application) rejectAutonomousEnvelope(
	ctx context.Context,
	work domain.AutonomousWork,
	cause error,
) error {
	if cause == nil || errors.Is(cause, context.Canceled) || errors.Is(cause, context.DeadlineExceeded) {
		return cause
	}
	if work.RunID == nil {
		return cause
	}
	_, lifecycleWon, err := a.rejectGenerationEnvelopeAtResidentBoundary(ctx, work.ResidentID, *work.RunID)
	if err != nil {
		return errors.Join(cause, err)
	}
	if lifecycleWon {
		return nil
	}
	return cause
}
