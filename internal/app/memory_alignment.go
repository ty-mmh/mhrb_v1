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

var errMemoryAlignmentForegroundPreempted = errors.New("app: memory alignment yielded to foreground dialogue")

type memoryAlignmentAssembly struct {
	Prepare  domain.PrepareGeneration
	Contents []domain.Content
}

func (a *Application) processMemoryAlignmentQueue(ctx context.Context, residentID canonical.ID) error {
	repository, ok := a.repository.(domain.MemoryAlignmentWorkRepository)
	if !ok {
		return errors.New("app: repository lacks memory alignment work capability")
	}
	for {
		work, err := repository.DiscoverMemoryAlignmentWork(
			ctx, residentID, domain.MaximumMemoryAlignmentPairs, a.maxAttempts,
		)
		if err != nil {
			return err
		}
		if work == nil {
			_, err := a.processPersonaRevisionQueue(ctx, residentID)
			return err
		}
		if a.generator == nil {
			return errors.New("app: generator is not configured for memory alignment")
		}
		if err := a.processMemoryAlignmentWork(ctx, *work); err != nil {
			if errors.Is(err, errMemoryAlignmentForegroundPreempted) {
				return nil
			}
			return err
		}
	}
}

func (a *Application) processMemoryAlignmentWork(ctx context.Context, work domain.MemoryAlignmentWork) error {
	var prepared domain.PreparedGeneration
	var admittedLease *backgroundCallLease
	var err error
	switch work.State {
	case domain.WorkPending:
		assembly, err := a.assembleMemoryAlignment(ctx, work)
		if err != nil {
			return err
		}
		expected := a.captureForegroundEpoch(work.Resident.ResidentID)
		_, lease, submitErr := a.submitBackgroundMutationAndBeginCall(
			ctx, expected, true,
			domain.PrepareGenerationCommand(assembly.Prepare), assembly.Contents,
		)
		if errors.Is(submitErr, errForegroundPreempted) {
			return errMemoryAlignmentForegroundPreempted
		}
		if submitErr != nil {
			return fmt.Errorf("app: prepare memory alignment: %w", submitErr)
		}
		admittedLease = &lease
		preparedRunID := assembly.Prepare.RunID
		work.RunID = &preparedRunID
		prepared, err = a.repository.Generation(ctx, assembly.Prepare.RunID)
	case domain.WorkRunning:
		if work.RunID == nil {
			return errors.New("app: running memory alignment has no run")
		}
		prepared, err = a.repository.Generation(ctx, *work.RunID)
	case domain.WorkRetryPending:
		if work.RunID == nil {
			return errors.New("app: retrying memory alignment has no run")
		}
		prepared, err = a.repository.Generation(ctx, *work.RunID)
		if err == nil {
			err = a.validateMemoryAlignmentEnvelope(prepared, work)
		}
		if err != nil {
			return a.rejectMemoryAlignmentEnvelope(ctx, work, err)
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
		expected := a.captureForegroundEpoch(work.Resident.ResidentID)
		_, lease, submitErr := a.submitBackgroundMutationAndBeginCall(
			ctx, expected, true,
			domain.StartAttemptCommand(domain.Attempt{
				RunID: *work.RunID, ResidentID: work.Resident.ResidentID,
				AttemptNo: work.AttemptNo + 1, OutcomeID: outcomeID,
			}), nil,
		)
		if errors.Is(submitErr, errForegroundPreempted) {
			return errMemoryAlignmentForegroundPreempted
		}
		if submitErr != nil {
			return fmt.Errorf("app: start memory alignment retry: %w", submitErr)
		}
		admittedLease = &lease
		prepared, err = a.repository.Generation(ctx, *work.RunID)
	default:
		return fmt.Errorf("app: unsupported memory alignment work state %q", work.State)
	}
	if err != nil {
		if admittedLease != nil && admittedLease.registered {
			admittedLease.finish()
		}
		return a.rejectMemoryAlignmentEnvelope(ctx, work, err)
	}
	if prepared.State != domain.WorkRunning {
		if admittedLease != nil && admittedLease.registered {
			admittedLease.finish()
		}
		return nil
	}
	if err := a.validateMemoryAlignmentEnvelope(prepared, work); err != nil {
		if admittedLease != nil && admittedLease.registered {
			admittedLease.finish()
		}
		return a.rejectMemoryAlignmentEnvelope(ctx, work, err)
	}
	return a.callAndLandMemoryAlignmentWithAdmission(ctx, prepared, work, admittedLease)
}

func (a *Application) assembleMemoryAlignment(
	ctx context.Context,
	work domain.MemoryAlignmentWork,
) (memoryAlignmentAssembly, error) {
	resident, err := a.repository.Resident(ctx, work.Resident.ResidentID)
	if err != nil {
		return memoryAlignmentAssembly{}, err
	}
	if resident.Status != "active" || resident.PersonaRevisionID != work.Resident.PersonaRevisionID ||
		resident.MemoryPolicyRevisionID != work.Resident.MemoryPolicyRevisionID {
		return memoryAlignmentAssembly{}, errors.New("app: memory alignment revisions changed before prepare")
	}
	contract, err := a.memoryAlignmentSchemaContract(a.structuredOutputMode)
	if err != nil {
		return memoryAlignmentAssembly{}, err
	}
	if err := contract.Validate(a.generatorCapabilities()); err != nil {
		return memoryAlignmentAssembly{}, err
	}
	_, params, err := domain.NewStructuredGeneratorParams(
		false, canonical.ByteSize(a.maxOutputBytes), contract.Mode, contract.Version, contract.Hash,
	)
	if err != nil {
		return memoryAlignmentAssembly{}, err
	}
	dropped, err := canonical.MarshalCanonical(struct {
		MemoryRecall string `json:"memory_recall"`
	}{MemoryRecall: "not_applicable"})
	if err != nil {
		return memoryAlignmentAssembly{}, err
	}
	ids, err := a.allocateIDs(2)
	if err != nil {
		return memoryAlignmentAssembly{}, err
	}
	now := canonical.InstantFromTime(a.clock.Now())
	assembly := memoryAlignmentAssembly{Prepare: domain.PrepareGeneration{
		RunID: ids[0], ResidentID: resident.ResidentID, Purpose: domain.GenerationPurposeMemoryAlignment,
		IdempotencyKey: work.IdempotencyKey, Provider: a.provider, Model: a.model,
		PipelineVersionID:    work.AlignmentPipelineVersionID,
		PrinciplesRevisionID: resident.PrinciplesRevisionID, PersonaRevisionID: resident.PersonaRevisionID,
		MemoryPolicyRevisionID: resident.MemoryPolicyRevisionID,
		AsOf:                   now, AsOfTZ: a.timezone, DroppedInputSummary: dropped,
		GeneratorParams: params, RunningOutcomeID: ids[1],
	}}
	if err := pinGenerationVersions(&assembly.Prepare); err != nil {
		return memoryAlignmentAssembly{}, err
	}
	type source struct {
		text, sourceType, inclusion string
		id                          canonical.ID
		role                        generation.Role
	}
	sources := []source{
		{resident.Persona, "resident_revision", "resident_definition", resident.PersonaRevisionID, generation.RoleSystem},
		{work.PolicyContent, "resident_revision", "resident_definition", resident.MemoryPolicyRevisionID, generation.RoleSystem},
		{work.Direct.Statement, "claim", "memory_recall", work.Direct.ClaimID, generation.RoleUser},
		{work.Meta.Statement, "claim", "memory_recall", work.Meta.ClaimID, generation.RoleUser},
	}
	total := 0
	for _, item := range sources {
		total += len([]byte(item.text))
	}
	if total > a.maxInputBytes {
		return memoryAlignmentAssembly{}, fmt.Errorf("app: memory alignment inputs exceed %d-byte budget", a.maxInputBytes)
	}
	for index, item := range sources {
		content, err := a.newContent(resident.ResidentID, "generation_input", []byte(item.text), "independent")
		if err != nil {
			return memoryAlignmentAssembly{}, err
		}
		inputID, err := a.ids.New()
		if err != nil {
			return memoryAlignmentAssembly{}, err
		}
		sourceID := item.id
		assembly.Prepare.Inputs = append(assembly.Prepare.Inputs, domain.GenerationInput{
			ID: inputID, Ordinal: int64(index), Role: string(item.role), SourceType: item.sourceType,
			SourceID: &sourceID, InclusionMode: item.inclusion, Content: content,
		})
		assembly.Contents = append(assembly.Contents, content)
	}
	return assembly, nil
}

func (a *Application) memoryAlignmentSchemaContract(mode generation.StructuredOutputMode) (generation.SchemaContract, error) {
	schema, err := memory.AlignmentJSONSchema()
	if err != nil {
		return generation.SchemaContract{}, err
	}
	return generation.NewSchemaContract(
		"memory_alignment", memory.AlignmentOutputVersionV1, mode, schema,
	)
}

func (a *Application) validateMemoryAlignmentEnvelope(
	prepared domain.PreparedGeneration,
	work domain.MemoryAlignmentWork,
) error {
	if prepared.Provider != a.provider || prepared.Purpose != domain.GenerationPurposeMemoryAlignment || prepared.SessionPolicyID != nil {
		return fmt.Errorf("%w: invalid memory alignment generation identity", ErrGenerationEnvelopeUnsupported)
	}
	identity, err := domain.ParseMemoryAlignmentObligation(prepared.IdempotencyKey)
	if err != nil {
		return fmt.Errorf("%w: %v", ErrGenerationEnvelopeUnsupported, err)
	}
	if prepared.IdempotencyKey != work.IdempotencyKey ||
		identity.DirectClaimID != work.Direct.ClaimID || identity.MetaClaimID != work.Meta.ClaimID ||
		identity.DirectEvidenceID != work.Direct.LatestEvidenceID || identity.MetaEvidenceID != work.Meta.LatestEvidenceID {
		return fmt.Errorf("%w: memory alignment work identity mismatch", ErrGenerationEnvelopeUnsupported)
	}
	if (identity.Replacement == nil) != (work.Replacement == nil) {
		return fmt.Errorf("%w: memory alignment replacement presence mismatch", ErrGenerationEnvelopeUnsupported)
	}
	if identity.Replacement != nil &&
		(identity.Replacement.OldClaimID != work.Replacement.OldClaimID ||
			identity.Replacement.IntentRelationID != work.Replacement.IntentRelationID) {
		return fmt.Errorf("%w: memory alignment replacement identity mismatch", ErrGenerationEnvelopeUnsupported)
	}
	if len(prepared.Inputs) != domain.MemoryAlignmentInputCount {
		return fmt.Errorf("%w: invalid memory alignment input count", ErrGenerationEnvelopeUnsupported)
	}
	if err := domain.ValidateGeneratorParamsForPurpose(prepared.Purpose, prepared.GeneratorParams); err != nil {
		return fmt.Errorf("%w: %v", ErrGenerationEnvelopeUnsupported, err)
	}
	if err := validatePreparedGenerationVersions(prepared); err != nil {
		return fmt.Errorf("%w: %v", ErrGenerationEnvelopeUnsupported, err)
	}
	if prepared.GeneratorParams.Streaming || prepared.GeneratorParams.SchemaHash == nil {
		return fmt.Errorf("%w: invalid memory alignment generator parameters", ErrGenerationEnvelopeUnsupported)
	}
	contract, err := a.memoryAlignmentSchemaContract(prepared.GeneratorParams.StructuredOutputMode)
	if err != nil {
		return err
	}
	if contract.Version != prepared.GeneratorParams.SchemaVersion || contract.Hash != *prepared.GeneratorParams.SchemaHash {
		return fmt.Errorf("%w: memory alignment schema version/hash mismatch", ErrGenerationEnvelopeUnsupported)
	}
	return contract.Validate(a.generatorCapabilities())
}

func (a *Application) callAndLandMemoryAlignment(
	ctx context.Context,
	prepared domain.PreparedGeneration,
	work domain.MemoryAlignmentWork,
) error {
	return a.callAndLandMemoryAlignmentWithAdmission(ctx, prepared, work, nil)
}

func (a *Application) callAndLandMemoryAlignmentWithAdmission(
	ctx context.Context,
	prepared domain.PreparedGeneration,
	work domain.MemoryAlignmentWork,
	admittedLease *backgroundCallLease,
) error {
	identity, err := domain.ParseMemoryAlignmentObligation(prepared.IdempotencyKey)
	if err != nil {
		return err
	}
	contract, err := a.memoryAlignmentSchemaContract(prepared.GeneratorParams.StructuredOutputMode)
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
	lease := backgroundCallLease{}
	if admittedLease != nil {
		lease = *admittedLease
	} else {
		lease = a.beginBackgroundCall(ctx, prepared.ResidentID)
	}
	if !lease.registered {
		return a.recordMemoryAlignmentForegroundPreempted(ctx, prepared)
	}
	defer lease.finish()
	started := time.Now()
	providerResult, providerErr := a.generator.Stream(lease.Context, request, func(generation.Delta) {})
	latency := time.Since(started).Microseconds()
	if a.backgroundCallPreempted(context.WithoutCancel(ctx), lease) {
		return a.recordMemoryAlignmentForegroundPreempted(ctx, prepared)
	}
	if providerErr != nil {
		code := generation.OutcomeErrorCodeFromError(providerErr)
		state := "failed"
		if ctx.Err() != nil {
			code = generation.RuntimeInterruptedErrorCode()
			state = "cancelled"
		}
		if err := a.recordMemoryAlignmentFailure(context.WithoutCancel(ctx), prepared, state, code); err != nil {
			return err
		}
		return nil
	}
	_, canonicalOutput, err := memory.ParseAlignmentOutput([]byte(providerResult.Text))
	if err != nil {
		code := generation.MustOutcomeErrorCode(generation.ErrorInvalidResponse, 0)
		return a.recordMemoryAlignmentFailure(ctx, prepared, "failed", code)
	}
	output, err := a.newContent(prepared.ResidentID, "generation_output", canonicalOutput.Bytes(), "independent")
	if err != nil {
		return err
	}
	idCount := 3
	if work.Replacement != nil {
		idCount += 2
	}
	ids, err := a.allocateIDs(idCount)
	if err != nil {
		return err
	}
	var replacement *domain.MemoryAlignmentReplacement
	if work.Replacement != nil {
		replacement = &domain.MemoryAlignmentReplacement{
			OldClaimID: work.Replacement.OldClaimID, IntentRelationID: work.Replacement.IntentRelationID,
			RelationID: ids[3], OldStatusTransitionID: ids[4],
			StatusPipelineVersionID: work.Replacement.StatusPipelineVersionID,
		}
	}
	_, err = a.submitBackgroundLandingWithContent(ctx, lease, domain.LandMemoryAlignmentCommand(domain.LandMemoryAlignment{
		Attempt:       domain.Attempt{RunID: prepared.RunID, ResidentID: prepared.ResidentID, AttemptNo: prepared.AttemptNo, OutcomeID: ids[0]},
		DirectClaimID: identity.DirectClaimID, MetaClaimID: identity.MetaClaimID,
		DirectEvidenceID: identity.DirectEvidenceID, MetaEvidenceID: identity.MetaEvidenceID,
		AlignmentPipelineVersionID: prepared.PipelineVersionID, MaturationPipelineVersionID: work.MaturationPipelineVersionID,
		MemoryPolicyRevisionID: prepared.MemoryPolicyRevisionID,
		StageTransitionID:      ids[1], StageTransitionDependencyID: ids[2], Output: output,
		PromptTokens: providerResult.PromptTokens, CompletionTokens: providerResult.CompletionTokens, LatencyMicros: latency,
		Replacement: replacement,
	}), []domain.Content{output})
	if errors.Is(err, errForegroundPreempted) {
		return a.recordMemoryAlignmentForegroundPreempted(ctx, prepared)
	}
	if err != nil {
		// The landing may already be durable when Writer acknowledgement fails.
		// Leave its running attempt to recovery instead of issuing a second write.
		if errors.Is(err, canonical.ErrWriterPoisoned) {
			return err
		}
		state := "failed"
		code := generation.MustOutcomeErrorCode(generation.ErrorLandingFailure, 0)
		if errors.Is(err, domain.ErrClaimSourceIneligible) {
			state = "cancelled"
			code = generation.MustOutcomeErrorCode(generation.ErrorSourceContentErased, 0)
		}
		if recordErr := a.recordMemoryAlignmentFailure(context.WithoutCancel(ctx), prepared, state, code); recordErr != nil {
			return errors.Join(err, recordErr)
		}
		return err
	}
	return nil
}

func (a *Application) recordMemoryAlignmentForegroundPreempted(
	ctx context.Context,
	prepared domain.PreparedGeneration,
) error {
	latest, err := a.repository.Generation(context.WithoutCancel(ctx), prepared.RunID)
	if err != nil {
		return err
	}
	if latest.State != domain.WorkRunning {
		return errMemoryAlignmentForegroundPreempted
	}
	code := generation.MustOutcomeErrorCode(generation.ErrorForegroundPreempted, 0)
	if err := a.recordMemoryAlignmentFailure(context.WithoutCancel(ctx), prepared, "cancelled", code); err != nil {
		latest, readErr := a.repository.Generation(context.WithoutCancel(ctx), prepared.RunID)
		if readErr == nil && latest.State != domain.WorkRunning {
			return errMemoryAlignmentForegroundPreempted
		}
		return err
	}
	return errMemoryAlignmentForegroundPreempted
}

func (a *Application) recordMemoryAlignmentFailure(
	ctx context.Context,
	prepared domain.PreparedGeneration,
	state string,
	code generation.OutcomeErrorCode,
) error {
	_, err := a.recordBackgroundAttemptFailure(ctx, prepared, state, code)
	return err
}

func (a *Application) rejectMemoryAlignmentEnvelope(
	ctx context.Context,
	work domain.MemoryAlignmentWork,
	cause error,
) error {
	if cause == nil || errors.Is(cause, context.Canceled) || errors.Is(cause, context.DeadlineExceeded) {
		return cause
	}
	if work.RunID == nil {
		return cause
	}
	_, lifecycleWon, err := a.rejectGenerationEnvelopeAtResidentBoundary(
		ctx, work.Resident.ResidentID, *work.RunID,
	)
	if err != nil {
		return errors.Join(cause, err)
	}
	if lifecycleWon {
		return nil
	}
	return nil
}
