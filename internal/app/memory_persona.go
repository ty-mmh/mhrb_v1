package app

import (
	"context"
	"errors"
	"fmt"
	"time"

	"mahoroba.local/mahoroba/internal/canonical"
	"mahoroba.local/mahoroba/internal/domain"
	"mahoroba.local/mahoroba/internal/generation"
	"mahoroba.local/mahoroba/internal/memory"
)

type personaRevisionAssembly struct {
	Prepare  domain.PrepareGeneration
	Contents []domain.Content
}

var errPersonaForegroundPreempted = errors.New("app: persona revision yielded to foreground dialogue")

// ProposeMemoryPersona executes the same durable background path used by the
// safety scanner. The Admin CLI uses it under the offline host lock.
func (a *Application) ProposeMemoryPersona(
	ctx context.Context,
	residentID canonical.ID,
) (domain.MemoryPersonaProposalResult, error) {
	if err := a.ensureAccepting(); err != nil {
		return domain.MemoryPersonaProposalResult{}, err
	}
	lock := a.residentLock(residentID)
	lock.Lock()
	defer lock.Unlock()
	unselected, err := a.residentActiveButUnselected(ctx, residentID)
	if err != nil {
		return domain.MemoryPersonaProposalResult{}, err
	}
	if unselected {
		return domain.MemoryPersonaProposalResult{BlockingReason: "resident_unselected"}, nil
	}
	if !a.memoryExtractionScansAreComplete(residentID) {
		return domain.MemoryPersonaProposalResult{BlockingReason: "mandatory_memory_scan_incomplete"}, nil
	}
	return a.processPersonaRevisionQueue(ctx, residentID)
}

func (a *Application) processPersonaRevisionQueue(
	ctx context.Context,
	residentID canonical.ID,
) (domain.MemoryPersonaProposalResult, error) {
	repository, ok := a.repository.(domain.PersonaRevisionWorkRepository)
	if !ok {
		return domain.MemoryPersonaProposalResult{}, errors.New("app: repository lacks persona revision work capability")
	}
	var final domain.MemoryPersonaProposalResult
	for {
		work, err := repository.DiscoverPersonaRevisionWork(ctx, residentID, a.maxAttempts)
		if err != nil {
			return final, err
		}
		if work == nil {
			if !final.Changed && final.BlockingReason == "" {
				final.BlockingReason = "no_eligible_settled_transition"
			}
			return final, nil
		}
		result, err := a.processPersonaRevisionWork(ctx, *work)
		if err != nil {
			if errors.Is(err, errPersonaForegroundPreempted) {
				if result.RunID != nil {
					final = result
				}
				return final, nil
			}
			return final, err
		}
		if result.RunID != nil {
			final = result
		}
		if result.Landing != nil {
			return result, nil
		}
	}
}

func (a *Application) processPersonaRevisionWork(
	ctx context.Context,
	work domain.PersonaRevisionWork,
) (domain.MemoryPersonaProposalResult, error) {
	if a.generator == nil {
		return domain.MemoryPersonaProposalResult{}, errors.New("app: generator is not configured for persona proposal")
	}
	var prepared domain.PreparedGeneration
	var admittedLease *backgroundCallLease
	var err error
	switch work.State {
	case domain.WorkPending:
		assembly, err := a.assemblePersonaRevision(ctx, work)
		if err != nil {
			return domain.MemoryPersonaProposalResult{}, err
		}
		expected := a.captureForegroundEpoch(work.ResidentID)
		_, lease, submitErr := a.submitBackgroundMutationAndBeginCall(
			ctx, expected, true,
			domain.PrepareGenerationCommand(assembly.Prepare), assembly.Contents,
		)
		if errors.Is(submitErr, errForegroundPreempted) {
			return domain.MemoryPersonaProposalResult{}, errPersonaForegroundPreempted
		}
		if submitErr != nil {
			return domain.MemoryPersonaProposalResult{}, fmt.Errorf("app: prepare persona revision: %w", submitErr)
		}
		admittedLease = &lease
		preparedRunID := assembly.Prepare.RunID
		work.RunID = &preparedRunID
		prepared, err = a.repository.Generation(ctx, assembly.Prepare.RunID)
	case domain.WorkRunning:
		if work.RunID == nil {
			return domain.MemoryPersonaProposalResult{}, errors.New("app: running persona work has no run")
		}
		prepared, err = a.repository.Generation(ctx, *work.RunID)
	case domain.WorkRetryPending:
		if work.RunID == nil {
			return domain.MemoryPersonaProposalResult{}, errors.New("app: retrying persona work has no run")
		}
		prepared, err = a.repository.Generation(ctx, *work.RunID)
		if err == nil {
			err = a.validatePersonaRevisionEnvelope(prepared, work)
		}
		if err != nil {
			return domain.MemoryPersonaProposalResult{}, a.rejectPersonaRevisionEnvelope(ctx, work, a.unsupportedEnvelopeError(err))
		}
		if !work.ForegroundPreempted && work.RetryCount > 0 && int(work.RetryCount) <= len(a.retryBackoff) {
			timer := time.NewTimer(a.retryBackoff[work.RetryCount-1])
			select {
			case <-ctx.Done():
				timer.Stop()
				return domain.MemoryPersonaProposalResult{}, ctx.Err()
			case <-timer.C:
			}
		}
		if err == nil {
			outcomeID, idErr := a.ids.New()
			if idErr != nil {
				return domain.MemoryPersonaProposalResult{}, idErr
			}
			expected := a.captureForegroundEpoch(work.ResidentID)
			_, lease, submitErr := a.submitBackgroundMutationAndBeginCall(
				ctx, expected, true,
				domain.StartAttemptCommand(domain.Attempt{
					RunID: *work.RunID, ResidentID: work.ResidentID,
					AttemptNo: work.AttemptNo + 1, OutcomeID: outcomeID,
				}), nil)
			if errors.Is(submitErr, errForegroundPreempted) {
				return domain.MemoryPersonaProposalResult{}, errPersonaForegroundPreempted
			}
			if submitErr != nil {
				err = submitErr
			} else {
				admittedLease = &lease
				prepared, err = a.repository.Generation(ctx, *work.RunID)
			}
		}
	default:
		return domain.MemoryPersonaProposalResult{}, fmt.Errorf("app: unsupported persona work state %q", work.State)
	}
	if err != nil {
		if admittedLease != nil && admittedLease.registered {
			admittedLease.finish()
		}
		return domain.MemoryPersonaProposalResult{}, a.rejectPersonaRevisionEnvelope(ctx, work, a.unsupportedEnvelopeError(err))
	}
	if prepared.State != domain.WorkRunning {
		if admittedLease != nil && admittedLease.registered {
			admittedLease.finish()
		}
		return domain.MemoryPersonaProposalResult{}, nil
	}
	if err := a.validatePersonaRevisionEnvelope(prepared, work); err != nil {
		if admittedLease != nil && admittedLease.registered {
			admittedLease.finish()
		}
		return domain.MemoryPersonaProposalResult{}, a.rejectPersonaRevisionEnvelope(ctx, work, err)
	}
	return a.callAndLandPersonaRevisionWithAdmission(
		ctx, prepared, work.TriggerStageTransitionID, admittedLease,
	)
}

func (a *Application) assemblePersonaRevision(
	ctx context.Context,
	work domain.PersonaRevisionWork,
) (personaRevisionAssembly, error) {
	resident, err := a.repository.Resident(ctx, work.ResidentID)
	if err != nil {
		return personaRevisionAssembly{}, err
	}
	if resident.Status != "active" || resident.PersonaRevisionID != work.CurrentPersonaRevisionID ||
		resident.MemoryPolicyRevisionID != work.PolicyRevisionID {
		return personaRevisionAssembly{}, errors.New("app: persona proposal inputs changed before prepare")
	}
	contract, err := a.personaRevisionSchemaContract(a.structuredOutputMode)
	if err != nil {
		return personaRevisionAssembly{}, err
	}
	if err := contract.Validate(a.generatorCapabilities()); err != nil {
		return personaRevisionAssembly{}, err
	}
	_, params, err := domain.NewStructuredGeneratorParams(
		false, canonical.ByteSize(a.maxOutputBytes), contract.Mode, contract.Version, contract.Hash,
	)
	if err != nil {
		return personaRevisionAssembly{}, err
	}
	dropped, err := canonical.MarshalCanonical(struct {
		MemoryRecall string `json:"memory_recall"`
	}{MemoryRecall: "not_applicable"})
	if err != nil {
		return personaRevisionAssembly{}, err
	}
	ids, err := a.allocateIDs(2)
	if err != nil {
		return personaRevisionAssembly{}, err
	}
	now := canonical.InstantFromTime(a.clock.Now())
	assembly := personaRevisionAssembly{Prepare: domain.PrepareGeneration{
		RunID: ids[0], ResidentID: resident.ResidentID, Purpose: domain.GenerationPurposePersonaRevision,
		IdempotencyKey: domain.PersonaRevisionObligation(work.TriggerStageTransitionID),
		Provider:       a.provider, Model: a.model, PipelineVersionID: work.PipelineVersionID,
		PrinciplesRevisionID: resident.PrinciplesRevisionID, PersonaRevisionID: work.CurrentPersonaRevisionID,
		MemoryPolicyRevisionID: work.PolicyRevisionID, AsOf: now, AsOfTZ: a.timezone,
		DroppedInputSummary: dropped, GeneratorParams: params, RunningOutcomeID: ids[1],
	}}
	if err := pinGenerationVersions(&assembly.Prepare); err != nil {
		return personaRevisionAssembly{}, err
	}
	type source struct {
		text, sourceType, inclusion string
		id                          canonical.ID
	}
	sources := []source{
		{text: work.CurrentPersona, sourceType: "resident_revision", inclusion: "resident_definition", id: work.CurrentPersonaRevisionID},
		{text: work.PolicyContent, sourceType: "resident_revision", inclusion: "resident_definition", id: work.PolicyRevisionID},
	}
	for _, claim := range work.Claims {
		sources = append(sources, source{text: claim.Statement, sourceType: "claim", inclusion: "memory_recall", id: claim.ClaimID})
	}
	total := 0
	for _, item := range sources {
		total += len([]byte(item.text))
	}
	if total > a.maxInputBytes {
		return personaRevisionAssembly{}, fmt.Errorf("app: persona inputs exceed %d-byte budget", a.maxInputBytes)
	}
	for index, item := range sources {
		content, err := a.newContent(resident.ResidentID, "generation_input", []byte(item.text), "independent")
		if err != nil {
			return personaRevisionAssembly{}, err
		}
		inputID, err := a.ids.New()
		if err != nil {
			return personaRevisionAssembly{}, err
		}
		sourceID := item.id
		assembly.Prepare.Inputs = append(assembly.Prepare.Inputs, domain.GenerationInput{
			ID: inputID, Ordinal: int64(index), Role: string(generation.RoleUser),
			SourceType: item.sourceType, SourceID: &sourceID, InclusionMode: item.inclusion, Content: content,
		})
		assembly.Contents = append(assembly.Contents, content)
	}
	return assembly, nil
}

func (a *Application) personaRevisionSchemaContract(mode generation.StructuredOutputMode) (generation.SchemaContract, error) {
	schema, err := memory.PersonaRevisionJSONSchema()
	if err != nil {
		return generation.SchemaContract{}, err
	}
	return generation.NewSchemaContract("persona_revision", memory.PersonaOutputSchemaVersionV1, mode, schema)
}

func (a *Application) validatePersonaRevisionEnvelope(
	prepared domain.PreparedGeneration,
	work domain.PersonaRevisionWork,
) error {
	if prepared.Provider != a.provider || prepared.Purpose != domain.GenerationPurposePersonaRevision ||
		prepared.SessionPolicyID != nil || prepared.RecallRunID != nil || prepared.ResidentID != work.ResidentID ||
		prepared.IdempotencyKey != domain.PersonaRevisionObligation(work.TriggerStageTransitionID) ||
		prepared.PipelineVersionID != work.PipelineVersionID ||
		prepared.PersonaRevisionID != work.CurrentPersonaRevisionID ||
		prepared.MemoryPolicyRevisionID != work.PolicyRevisionID {
		return fmt.Errorf("%w: invalid persona generation identity", ErrGenerationEnvelopeUnsupported)
	}
	if err := domain.ValidateGeneratorParamsForPurpose(prepared.Purpose, prepared.GeneratorParams); err != nil {
		return fmt.Errorf("%w: %v", ErrGenerationEnvelopeUnsupported, err)
	}
	if err := validatePreparedGenerationVersions(prepared); err != nil {
		return fmt.Errorf("%w: %v", ErrGenerationEnvelopeUnsupported, err)
	}
	if prepared.GeneratorParams.Streaming || prepared.GeneratorParams.SchemaHash == nil {
		return fmt.Errorf("%w: invalid persona generator parameters", ErrGenerationEnvelopeUnsupported)
	}
	contract, err := a.personaRevisionSchemaContract(prepared.GeneratorParams.StructuredOutputMode)
	if err != nil {
		return err
	}
	if contract.Version != prepared.GeneratorParams.SchemaVersion || contract.Hash != *prepared.GeneratorParams.SchemaHash {
		return fmt.Errorf("%w: persona schema version/hash mismatch", ErrGenerationEnvelopeUnsupported)
	}
	if err := contract.Validate(a.generatorCapabilities()); err != nil {
		return fmt.Errorf("%w: %v", ErrGenerationEnvelopeUnsupported, err)
	}
	expectedCount := 2 + len(work.Claims)
	if len(work.Claims) < 2 || len(work.Claims) > 8 || len(prepared.Inputs) != expectedCount {
		return fmt.Errorf("%w: invalid persona input count", ErrGenerationEnvelopeUnsupported)
	}
	type expectedInput struct {
		text, sourceType, inclusion string
		id                          canonical.ID
	}
	expected := []expectedInput{
		{text: work.CurrentPersona, sourceType: "resident_revision", inclusion: "resident_definition", id: work.CurrentPersonaRevisionID},
		{text: work.PolicyContent, sourceType: "resident_revision", inclusion: "resident_definition", id: work.PolicyRevisionID},
	}
	for _, claim := range work.Claims {
		expected = append(expected, expectedInput{
			text: claim.Statement, sourceType: "claim", inclusion: "memory_recall", id: claim.ClaimID,
		})
	}
	for index, input := range prepared.Inputs {
		want := expected[index]
		if input.Ordinal != int64(index) || input.Role != string(generation.RoleUser) ||
			input.SourceType != want.sourceType || input.InclusionMode != want.inclusion ||
			input.SourceID == nil || *input.SourceID != want.id || string(input.Content.Bytes) != want.text {
			return fmt.Errorf("%w: persona input %d mismatch", ErrGenerationEnvelopeUnsupported, index)
		}
	}
	return nil
}

func (a *Application) callAndLandPersonaRevision(
	ctx context.Context,
	prepared domain.PreparedGeneration,
	triggerID canonical.ID,
) (domain.MemoryPersonaProposalResult, error) {
	return a.callAndLandPersonaRevisionWithAdmission(ctx, prepared, triggerID, nil)
}

func (a *Application) callAndLandPersonaRevisionWithAdmission(
	ctx context.Context,
	prepared domain.PreparedGeneration,
	triggerID canonical.ID,
	admittedLease *backgroundCallLease,
) (domain.MemoryPersonaProposalResult, error) {
	contract, err := a.personaRevisionSchemaContract(prepared.GeneratorParams.StructuredOutputMode)
	if err != nil {
		return domain.MemoryPersonaProposalResult{}, err
	}
	request := generation.Request{
		GenerationRunID: prepared.RunID.String(), Purpose: string(prepared.Purpose), Model: prepared.Model,
		Streaming: false, MaxOutputBytes: int(prepared.GeneratorParams.MaxOutputBytes), StructuredOutput: &contract,
		Messages: make([]generation.Message, 0, len(prepared.Inputs)),
	}
	for _, input := range prepared.Inputs {
		request.Messages = append(request.Messages, generation.Message{Role: generation.Role(input.Role), Text: string(input.Content.Bytes)})
	}
	lease := backgroundCallLease{}
	if admittedLease != nil {
		lease = *admittedLease
	} else {
		lease = a.beginBackgroundCall(ctx, prepared.ResidentID)
	}
	if !lease.registered {
		return a.recordPersonaForegroundPreempted(ctx, prepared)
	}
	defer lease.finish()
	started := time.Now()
	providerResult, providerErr := a.generator.Stream(lease.Context, request, func(generation.Delta) {})
	latency := time.Since(started).Microseconds()
	if a.backgroundCallPreempted(context.WithoutCancel(ctx), lease) {
		return a.recordPersonaForegroundPreempted(ctx, prepared)
	}
	if providerErr != nil {
		code := generation.OutcomeErrorCodeFromError(providerErr)
		state := "failed"
		if ctx.Err() != nil {
			code = generation.RuntimeInterruptedErrorCode()
			state = "cancelled"
		}
		if err := a.recordPersonaFailure(context.WithoutCancel(ctx), prepared, state, code); err != nil {
			return domain.MemoryPersonaProposalResult{}, err
		}
		result := domain.MemoryPersonaProposalResult{RunID: &prepared.RunID, AttemptNo: prepared.AttemptNo}
		return result, nil
	}
	parsed, canonicalOutput, err := memory.ParsePersonaRevisionOutput([]byte(providerResult.Text))
	if err != nil {
		code := generation.MustOutcomeErrorCode(generation.ErrorInvalidResponse, 0)
		if recordErr := a.recordPersonaFailure(ctx, prepared, "failed", code); recordErr != nil {
			return domain.MemoryPersonaProposalResult{}, errors.Join(err, recordErr)
		}
		return domain.MemoryPersonaProposalResult{RunID: &prepared.RunID, AttemptNo: prepared.AttemptNo}, nil
	}
	output, err := a.newContent(prepared.ResidentID, "generation_output", canonicalOutput.Bytes(), "independent")
	if err != nil {
		return domain.MemoryPersonaProposalResult{}, err
	}
	personaContent, err := a.newContent(prepared.ResidentID, "persona_text", []byte(parsed.Persona), "resident_only")
	if err != nil {
		return domain.MemoryPersonaProposalResult{}, err
	}
	ids, err := a.allocateIDs(3)
	if err != nil {
		return domain.MemoryPersonaProposalResult{}, err
	}
	command := domain.LandPersonaRevisionCommand(domain.LandPersonaRevision{
		Attempt:                  domain.Attempt{RunID: prepared.RunID, ResidentID: prepared.ResidentID, AttemptNo: prepared.AttemptNo, OutcomeID: ids[0]},
		TriggerStageTransitionID: triggerID, PipelineVersionID: prepared.PipelineVersionID,
		MemoryPolicyRevisionID: prepared.MemoryPolicyRevisionID, ParentPersonaRevisionID: prepared.PersonaRevisionID,
		PersonaRevisionID: ids[1], PersonaActivationID: ids[2], Output: output, PersonaContent: personaContent,
		PromptTokens: providerResult.PromptTokens, CompletionTokens: providerResult.CompletionTokens, LatencyMicros: latency,
	})
	result, err := a.submitBackgroundLandingWithContent(
		ctx, lease, command, []domain.Content{output, personaContent},
	)
	if errors.Is(err, errForegroundPreempted) {
		return a.recordPersonaForegroundPreempted(ctx, prepared)
	}
	if err != nil {
		// A poisoned Writer may have committed the landing without returning an
		// acknowledgement. Do not issue a second canonical write until startup
		// recovery has reloaded the durable head.
		if errors.Is(err, canonical.ErrWriterPoisoned) {
			return domain.MemoryPersonaProposalResult{}, err
		}
		state := "failed"
		code := generation.MustOutcomeErrorCode(generation.ErrorLandingFailure, 0)
		if errors.Is(err, domain.ErrClaimSourceIneligible) {
			state = "cancelled"
			code = generation.MustOutcomeErrorCode(generation.ErrorSourceContentErased, 0)
		}
		if recordErr := a.recordPersonaFailure(context.WithoutCancel(ctx), prepared, state, code); recordErr != nil {
			return domain.MemoryPersonaProposalResult{}, errors.Join(err, recordErr)
		}
		return domain.MemoryPersonaProposalResult{}, err
	}
	landing, ok := result.Value.(domain.PersonaRevisionLandingResult)
	if !ok {
		return domain.MemoryPersonaProposalResult{}, errors.New("app: persona landing returned unexpected result")
	}
	return domain.MemoryPersonaProposalResult{
		Changed: true, RunID: &prepared.RunID, AttemptNo: prepared.AttemptNo, Landing: &landing,
	}, nil
}

func (a *Application) recordPersonaForegroundPreempted(
	ctx context.Context,
	prepared domain.PreparedGeneration,
) (domain.MemoryPersonaProposalResult, error) {
	latest, err := a.repository.Generation(context.WithoutCancel(ctx), prepared.RunID)
	if err != nil {
		return domain.MemoryPersonaProposalResult{}, err
	}
	if latest.State != domain.WorkRunning {
		return domain.MemoryPersonaProposalResult{
			RunID: &prepared.RunID, AttemptNo: prepared.AttemptNo,
		}, errPersonaForegroundPreempted
	}
	code := generation.MustOutcomeErrorCode(generation.ErrorForegroundPreempted, 0)
	if err := a.recordPersonaFailure(context.WithoutCancel(ctx), prepared, "cancelled", code); err != nil {
		latest, readErr := a.repository.Generation(context.WithoutCancel(ctx), prepared.RunID)
		if readErr == nil && latest.State != domain.WorkRunning {
			return domain.MemoryPersonaProposalResult{
				RunID: &prepared.RunID, AttemptNo: prepared.AttemptNo,
			}, errPersonaForegroundPreempted
		}
		return domain.MemoryPersonaProposalResult{}, err
	}
	return domain.MemoryPersonaProposalResult{
		RunID: &prepared.RunID, AttemptNo: prepared.AttemptNo,
	}, errPersonaForegroundPreempted
}

func (a *Application) recordPersonaFailure(
	ctx context.Context,
	prepared domain.PreparedGeneration,
	state string,
	code generation.OutcomeErrorCode,
) error {
	_, err := a.recordPersonaFailureOutcome(ctx, prepared, state, code)
	return err
}

func (a *Application) recordPersonaFailureOutcome(
	ctx context.Context,
	prepared domain.PreparedGeneration,
	state string,
	code generation.OutcomeErrorCode,
) (bool, error) {
	return a.recordBackgroundAttemptFailure(ctx, prepared, state, code)
}

func (a *Application) rejectPersonaRevisionEnvelope(
	ctx context.Context,
	work domain.PersonaRevisionWork,
	cause error,
) error {
	if cause == nil || errors.Is(cause, context.Canceled) || errors.Is(cause, context.DeadlineExceeded) {
		return cause
	}
	if work.RunID == nil || work.RunID.IsZero() {
		return cause
	}
	_, lifecycleWon, err := a.rejectGenerationEnvelopeAtResidentBoundary(ctx, work.ResidentID, *work.RunID)
	if err != nil {
		return errors.Join(cause, fmt.Errorf("app: terminalize unsupported persona generation envelope: %w", err))
	}
	if lifecycleWon {
		return nil
	}
	return nil
}
