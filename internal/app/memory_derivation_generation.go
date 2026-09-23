package app

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"mahoroba.local/mahoroba/internal/canonical"
	"mahoroba.local/mahoroba/internal/domain"
	"mahoroba.local/mahoroba/internal/generation"
	"mahoroba.local/mahoroba/internal/memory"
)

// DeriveMemoryClaim runs the Admin-reviewed abstraction or differentiation
// pipeline. Only source claim IDs are caller-controlled; the model output is
// closed to statement and temporal kind, and the Writer derives every
// identity, scope, relation, policy and inherited-evidence field.
func (a *Application) DeriveMemoryClaim(
	ctx context.Context,
	residentID canonical.ID,
	purpose domain.GenerationPurpose,
	sourceIDs []canonical.ID,
) (domain.MemoryDerivationResult, error) {
	if err := a.ensureAccepting(); err != nil {
		return domain.MemoryDerivationResult{}, err
	}
	if purpose != domain.GenerationPurposeMemoryAbstraction && purpose != domain.GenerationPurposeMemoryDifferentiation {
		return domain.MemoryDerivationResult{}, errors.New("app: unsupported memory derivation purpose")
	}
	if a.generator == nil {
		return domain.MemoryDerivationResult{}, errors.New("app: generator is not configured for memory derivation")
	}
	repository, ok := a.repository.(domain.MemoryDerivationRepository)
	if !ok {
		return domain.MemoryDerivationResult{}, errors.New("app: repository lacks memory derivation preparation")
	}
	lock := a.residentLock(residentID)
	lock.Lock()
	defer lock.Unlock()
	unselected, err := a.residentActiveButUnselected(ctx, residentID)
	if err != nil {
		return domain.MemoryDerivationResult{}, err
	}
	if unselected {
		return domain.MemoryDerivationResult{}, nil
	}
	if !a.memoryExtractionScansAreComplete(residentID) {
		return domain.MemoryDerivationResult{}, nil
	}
	preparation, err := repository.PrepareMemoryDerivation(ctx, residentID, purpose, sourceIDs)
	if err != nil {
		return domain.MemoryDerivationResult{}, err
	}
	contract, err := a.derivedClaimSchemaContract(purpose, a.structuredOutputMode)
	if err != nil {
		return domain.MemoryDerivationResult{}, err
	}
	if err := contract.Validate(a.generatorCapabilities()); err != nil {
		return domain.MemoryDerivationResult{}, err
	}
	_, params, err := domain.NewStructuredGeneratorParams(
		false, canonical.ByteSize(a.maxOutputBytes), contract.Mode, contract.Version, contract.Hash,
	)
	if err != nil {
		return domain.MemoryDerivationResult{}, err
	}
	dropped, err := canonical.MarshalCanonical(struct {
		MemoryRecall string `json:"memory_recall"`
	}{MemoryRecall: "not_applicable"})
	if err != nil {
		return domain.MemoryDerivationResult{}, err
	}
	ids, err := a.allocateIDs(3)
	if err != nil {
		return domain.MemoryDerivationResult{}, err
	}
	requestID := ids[0]
	now := canonical.InstantFromTime(a.clock.Now())
	prepare := domain.PrepareGeneration{
		RunID: ids[1], ResidentID: residentID, Purpose: purpose,
		IdempotencyKey: string(purpose) + ":v1:" + requestID.String(),
		Provider:       a.provider, Model: a.model, PipelineVersionID: preparation.PipelineVersionID,
		PrinciplesRevisionID:   preparation.Resident.PrinciplesRevisionID,
		PersonaRevisionID:      preparation.Resident.PersonaRevisionID,
		MemoryPolicyRevisionID: preparation.Resident.MemoryPolicyRevisionID,
		AsOf:                   now, AsOfTZ: a.timezone, DroppedInputSummary: dropped,
		GeneratorParams: params, RunningOutcomeID: ids[2],
	}
	if err := pinGenerationVersions(&prepare); err != nil {
		return domain.MemoryDerivationResult{}, err
	}
	type inputSource struct {
		text, sourceType, inclusion string
		id                          canonical.ID
	}
	inputs := []inputSource{
		{preparation.Resident.Persona, "resident_revision", "resident_definition", preparation.Resident.PersonaRevisionID},
		{preparation.PolicyContent, "resident_revision", "resident_definition", preparation.Resident.MemoryPolicyRevisionID},
	}
	for _, source := range preparation.Sources {
		inputs = append(inputs, inputSource{source.Statement, "claim", "memory_recall", source.ClaimID})
	}
	total := 0
	contents := make([]domain.Content, 0, len(inputs))
	for index, item := range inputs {
		total += len([]byte(item.text))
		if total > a.maxInputBytes {
			return domain.MemoryDerivationResult{}, fmt.Errorf("app: derivation inputs exceed %d-byte budget", a.maxInputBytes)
		}
		content, err := a.newContent(residentID, "generation_input", []byte(item.text), "independent")
		if err != nil {
			return domain.MemoryDerivationResult{}, err
		}
		inputID, err := a.ids.New()
		if err != nil {
			return domain.MemoryDerivationResult{}, err
		}
		sourceID := item.id
		prepare.Inputs = append(prepare.Inputs, domain.GenerationInput{
			ID: inputID, Ordinal: int64(index), Role: string(generation.RoleUser),
			SourceType: item.sourceType, SourceID: &sourceID, InclusionMode: item.inclusion, Content: content,
		})
		contents = append(contents, content)
	}
	expected := a.captureForegroundEpoch(residentID)
	_, lease, err := a.submitBackgroundMutationAndBeginCall(
		ctx, expected, true, domain.PrepareGenerationCommand(prepare), contents,
	)
	if errors.Is(err, errForegroundPreempted) {
		return domain.MemoryDerivationResult{}, nil
	}
	if err != nil {
		return domain.MemoryDerivationResult{}, fmt.Errorf("app: prepare memory derivation: %w", err)
	}
	admittedLease := &lease
	prepared, err := a.repository.Generation(ctx, prepare.RunID)
	if err != nil {
		lease.finish()
		return domain.MemoryDerivationResult{}, a.rejectMemoryDerivationEnvelope(
			ctx, residentID, prepare.RunID, a.unsupportedEnvelopeError(err),
		)
	}
	if err := a.validateMemoryDerivationEnvelope(prepared, purpose, preparation); err != nil {
		lease.finish()
		return domain.MemoryDerivationResult{}, a.rejectMemoryDerivationEnvelope(ctx, residentID, prepare.RunID, err)
	}
	for {
		result, retry, err := a.callAndLandMemoryDerivationWithAdmission(
			ctx, prepared, preparation, admittedLease,
		)
		admittedLease = nil
		if err == nil || !retry || prepared.AttemptNo >= int64(a.maxAttempts) {
			return result, err
		}
		// Reload and validate the immutable envelope while the durable run is
		// still retry-pending. RejectGenerationEnvelope can then make the
		// retry transition and terminal rejection atomically.
		prepared, err = a.repository.Generation(ctx, prepared.RunID)
		if err != nil {
			return domain.MemoryDerivationResult{}, a.rejectMemoryDerivationEnvelope(
				ctx, residentID, prepare.RunID, a.unsupportedEnvelopeError(err),
			)
		}
		if err := a.validateMemoryDerivationEnvelope(prepared, purpose, preparation); err != nil {
			return domain.MemoryDerivationResult{}, a.rejectMemoryDerivationEnvelope(ctx, residentID, prepare.RunID, err)
		}
		if int(prepared.AttemptNo) <= len(a.retryBackoff) {
			timer := time.NewTimer(a.retryBackoff[prepared.AttemptNo-1])
			select {
			case <-ctx.Done():
				timer.Stop()
				return domain.MemoryDerivationResult{}, ctx.Err()
			case <-timer.C:
			}
		}
		outcomeID, err := a.ids.New()
		if err != nil {
			return domain.MemoryDerivationResult{}, err
		}
		expected := a.captureForegroundEpoch(residentID)
		_, retryLease, err := a.submitBackgroundMutationAndBeginCall(
			ctx, expected, true, domain.StartAttemptCommand(domain.Attempt{
				RunID: prepared.RunID, ResidentID: residentID,
				AttemptNo: prepared.AttemptNo + 1, OutcomeID: outcomeID,
			}), nil)
		if errors.Is(err, errForegroundPreempted) {
			return domain.MemoryDerivationResult{}, nil
		}
		if err != nil {
			return domain.MemoryDerivationResult{}, err
		}
		admittedLease = &retryLease
		prepared, err = a.repository.Generation(ctx, prepared.RunID)
		if err != nil {
			retryLease.finish()
			return domain.MemoryDerivationResult{}, a.rejectMemoryDerivationEnvelope(
				ctx, residentID, prepare.RunID, a.unsupportedEnvelopeError(err),
			)
		}
		if err := a.validateMemoryDerivationEnvelope(prepared, purpose, preparation); err != nil {
			retryLease.finish()
			return domain.MemoryDerivationResult{}, a.rejectMemoryDerivationEnvelope(ctx, residentID, prepare.RunID, err)
		}
	}
}

func (a *Application) derivedClaimSchemaContract(
	purpose domain.GenerationPurpose,
	mode generation.StructuredOutputMode,
) (generation.SchemaContract, error) {
	if purpose != domain.GenerationPurposeMemoryAbstraction && purpose != domain.GenerationPurposeMemoryDifferentiation {
		return generation.SchemaContract{}, errors.New("app: invalid derived schema purpose")
	}
	schema, err := memory.DerivedClaimJSONSchema()
	if err != nil {
		return generation.SchemaContract{}, err
	}
	return generation.NewSchemaContract(string(purpose), memory.DerivedClaimOutputSchemaVersionV1, mode, schema)
}

func (a *Application) callAndLandMemoryDerivation(
	ctx context.Context,
	prepared domain.PreparedGeneration,
	preparation domain.DerivedClaimPreparation,
) (domain.MemoryDerivationResult, bool, error) {
	return a.callAndLandMemoryDerivationWithAdmission(ctx, prepared, preparation, nil)
}

func (a *Application) callAndLandMemoryDerivationWithAdmission(
	ctx context.Context,
	prepared domain.PreparedGeneration,
	preparation domain.DerivedClaimPreparation,
	admittedLease *backgroundCallLease,
) (domain.MemoryDerivationResult, bool, error) {
	contract, err := a.derivedClaimSchemaContract(prepared.Purpose, prepared.GeneratorParams.StructuredOutputMode)
	if err != nil {
		return domain.MemoryDerivationResult{}, false, err
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
		return a.recordMemoryDerivationForegroundPreempted(ctx, prepared)
	}
	defer lease.finish()
	started := time.Now()
	providerResult, providerErr := a.generator.Stream(lease.Context, request, func(generation.Delta) {})
	latency := time.Since(started).Microseconds()
	if a.backgroundCallPreempted(context.WithoutCancel(ctx), lease) {
		return a.recordMemoryDerivationForegroundPreempted(ctx, prepared)
	}
	if providerErr != nil {
		code := generation.OutcomeErrorCodeFromError(providerErr)
		state := "failed"
		if ctx.Err() != nil {
			code = generation.RuntimeInterruptedErrorCode()
			state = "cancelled"
		}
		lifecycleTerminal, err := a.recordPersonaFailureOutcome(
			context.WithoutCancel(ctx), prepared, state, code,
		)
		if err != nil {
			return domain.MemoryDerivationResult{}, false, err
		}
		if lifecycleTerminal {
			return domain.MemoryDerivationResult{}, false, nil
		}
		return domain.MemoryDerivationResult{}, code.Retryable(), providerErr
	}
	parsed, canonicalOutput, err := memory.ParseDerivedClaimOutput([]byte(providerResult.Text))
	if err != nil {
		code := generation.MustOutcomeErrorCode(generation.ErrorInvalidResponse, 0)
		lifecycleTerminal, recordErr := a.recordPersonaFailureOutcome(ctx, prepared, "failed", code)
		if recordErr != nil {
			return domain.MemoryDerivationResult{}, false, errors.Join(err, recordErr)
		}
		if lifecycleTerminal {
			return domain.MemoryDerivationResult{}, false, nil
		}
		return domain.MemoryDerivationResult{}, false, err
	}
	output, err := a.newContent(prepared.ResidentID, "generation_output", canonicalOutput.Bytes(), "independent")
	if err != nil {
		return domain.MemoryDerivationResult{}, false, err
	}
	statement, err := a.newContent(prepared.ResidentID, "claim_statement", []byte(parsed.Statement), "independent")
	if err != nil {
		return domain.MemoryDerivationResult{}, false, err
	}
	baseIDs, err := a.allocateIDs(4)
	if err != nil {
		return domain.MemoryDerivationResult{}, false, err
	}
	landing := domain.LandDerivedClaim{
		Attempt:          domain.Attempt{RunID: prepared.RunID, ResidentID: prepared.ResidentID, AttemptNo: prepared.AttemptNo, OutcomeID: baseIDs[0]},
		OwnerPrincipalID: preparation.Resident.OwnerPrincipalID, ClaimID: baseIDs[1], TemporalKind: parsed.TemporalKind,
		Statement: statement, Output: output, PipelineVersionID: prepared.PipelineVersionID,
		MemoryPolicyRevisionID: prepared.MemoryPolicyRevisionID, InitialStageID: baseIDs[2],
		InitialViewScopeID: baseIDs[3], PromptTokens: providerResult.PromptTokens,
		CompletionTokens: providerResult.CompletionTokens, LatencyMicros: latency,
	}
	for _, source := range preparation.Sources {
		relationID, err := a.ids.New()
		if err != nil {
			return domain.MemoryDerivationResult{}, false, err
		}
		landing.Sources = append(landing.Sources, domain.DerivedClaimSource{ClaimID: source.ClaimID, RelationID: relationID})
		for _, sourceEvidenceID := range source.EvidenceIDs {
			evidenceID, err := a.ids.New()
			if err != nil {
				return domain.MemoryDerivationResult{}, false, err
			}
			landing.Evidence = append(landing.Evidence, domain.DerivedClaimEvidence{
				EvidenceID: evidenceID, SourceClaimID: source.ClaimID, SourceEvidenceID: sourceEvidenceID,
			})
		}
	}
	var landed domain.DerivedClaimLandingResult
	if prepared.Purpose == domain.GenerationPurposeMemoryAbstraction {
		landed, err = a.landMemoryDerivedClaim(ctx, landing, domain.LandClaimAbstractionCommand, &lease)
	} else {
		landed, err = a.landMemoryDerivedClaim(ctx, landing, domain.LandClaimDifferentiationCommand, &lease)
	}
	if errors.Is(err, errForegroundPreempted) {
		return a.recordMemoryDerivationForegroundPreempted(ctx, prepared)
	}
	if err != nil {
		// A poisoned Writer has an ambiguous commit result. A follow-up failure
		// write could create an illegal second terminal outcome, so recovery owns it.
		if errors.Is(err, canonical.ErrWriterPoisoned) {
			return domain.MemoryDerivationResult{}, false, err
		}
		state := "failed"
		code := generation.MustOutcomeErrorCode(generation.ErrorLandingFailure, 0)
		if errors.Is(err, domain.ErrClaimSourceIneligible) {
			state = "cancelled"
			code = generation.MustOutcomeErrorCode(generation.ErrorSourceContentErased, 0)
		}
		if recordErr := a.recordPersonaFailure(context.WithoutCancel(ctx), prepared, state, code); recordErr != nil {
			return domain.MemoryDerivationResult{}, false, errors.Join(err, recordErr)
		}
		return domain.MemoryDerivationResult{}, false, err
	}
	return domain.MemoryDerivationResult{RunID: prepared.RunID, Landing: landed}, false, nil
}

func (a *Application) recordMemoryDerivationForegroundPreempted(
	ctx context.Context,
	prepared domain.PreparedGeneration,
) (domain.MemoryDerivationResult, bool, error) {
	latest, err := a.repository.Generation(context.WithoutCancel(ctx), prepared.RunID)
	if err != nil {
		return domain.MemoryDerivationResult{}, false, err
	}
	if latest.State != domain.WorkRunning {
		return domain.MemoryDerivationResult{}, false, nil
	}
	code := generation.MustOutcomeErrorCode(generation.ErrorForegroundPreempted, 0)
	lifecycleTerminal, err := a.recordPersonaFailureOutcome(
		context.WithoutCancel(ctx), prepared, "cancelled", code,
	)
	if err != nil {
		latest, readErr := a.repository.Generation(context.WithoutCancel(ctx), prepared.RunID)
		if readErr == nil && latest.State != domain.WorkRunning {
			return domain.MemoryDerivationResult{}, false, nil
		}
		return domain.MemoryDerivationResult{}, false, err
	}
	if lifecycleTerminal {
		return domain.MemoryDerivationResult{}, false, nil
	}
	return domain.MemoryDerivationResult{}, false, errForegroundPreempted
}

func (a *Application) validateMemoryDerivationEnvelope(
	prepared domain.PreparedGeneration,
	expectedPurpose domain.GenerationPurpose,
	preparation domain.DerivedClaimPreparation,
) error {
	if expectedPurpose != domain.GenerationPurposeMemoryAbstraction &&
		expectedPurpose != domain.GenerationPurposeMemoryDifferentiation {
		return fmt.Errorf("%w: unsupported derived generation purpose %q", ErrGenerationEnvelopeUnsupported, expectedPurpose)
	}
	resident := preparation.Resident
	if prepared.Provider != a.provider || prepared.Purpose != expectedPurpose || prepared.SessionPolicyID != nil ||
		prepared.RecallRunID != nil || prepared.ResidentID != resident.ResidentID ||
		prepared.PipelineVersionID != preparation.PipelineVersionID ||
		prepared.PrinciplesRevisionID != resident.PrinciplesRevisionID ||
		prepared.PersonaRevisionID != resident.PersonaRevisionID ||
		prepared.MemoryPolicyRevisionID != resident.MemoryPolicyRevisionID {
		return fmt.Errorf("%w: derived generation identity mismatch", ErrGenerationEnvelopeUnsupported)
	}
	if err := domain.ValidateGeneratorParamsForPurpose(prepared.Purpose, prepared.GeneratorParams); err != nil {
		return fmt.Errorf("%w: %v", ErrGenerationEnvelopeUnsupported, err)
	}
	if err := validatePreparedGenerationVersions(prepared); err != nil {
		return fmt.Errorf("%w: %v", ErrGenerationEnvelopeUnsupported, err)
	}
	if prepared.GeneratorParams.Streaming || prepared.GeneratorParams.SchemaHash == nil {
		return fmt.Errorf("%w: invalid derived generator parameters", ErrGenerationEnvelopeUnsupported)
	}
	contract, err := a.derivedClaimSchemaContract(prepared.Purpose, prepared.GeneratorParams.StructuredOutputMode)
	if err != nil {
		return fmt.Errorf("%w: %v", ErrGenerationEnvelopeUnsupported, err)
	}
	if prepared.GeneratorParams.SchemaVersion != contract.Version ||
		*prepared.GeneratorParams.SchemaHash != contract.Hash {
		return fmt.Errorf("%w: derived schema version/hash mismatch", ErrGenerationEnvelopeUnsupported)
	}
	if err := contract.Validate(a.generatorCapabilities()); err != nil {
		return fmt.Errorf("%w: %v", ErrGenerationEnvelopeUnsupported, err)
	}
	requestPrefix := string(expectedPurpose) + ":v1:"
	requestRaw := strings.TrimPrefix(prepared.IdempotencyKey, requestPrefix)
	if requestRaw == prepared.IdempotencyKey {
		return fmt.Errorf("%w: invalid derived generation obligation", ErrGenerationEnvelopeUnsupported)
	}
	requestID, err := canonical.ParseID(requestRaw)
	if err != nil || requestID.IsZero() {
		return fmt.Errorf("%w: invalid derived generation obligation", ErrGenerationEnvelopeUnsupported)
	}
	type expectedInput struct {
		text, sourceType, inclusion string
		id                          canonical.ID
	}
	expected := []expectedInput{
		{text: resident.Persona, sourceType: "resident_revision", inclusion: "resident_definition", id: resident.PersonaRevisionID},
		{text: preparation.PolicyContent, sourceType: "resident_revision", inclusion: "resident_definition", id: resident.MemoryPolicyRevisionID},
	}
	for _, source := range preparation.Sources {
		expected = append(expected, expectedInput{
			text: source.Statement, sourceType: "claim", inclusion: "memory_recall", id: source.ClaimID,
		})
	}
	if len(prepared.Inputs) != len(expected) {
		return fmt.Errorf("%w: invalid derived generation input count", ErrGenerationEnvelopeUnsupported)
	}
	for index, input := range prepared.Inputs {
		want := expected[index]
		if input.Ordinal != int64(index) || input.Role != string(generation.RoleUser) ||
			input.SourceType != want.sourceType || input.InclusionMode != want.inclusion ||
			input.SourceID == nil || *input.SourceID != want.id || string(input.Content.Bytes) != want.text {
			return fmt.Errorf("%w: derived generation input %d mismatch", ErrGenerationEnvelopeUnsupported, index)
		}
	}
	return nil
}

func (a *Application) rejectMemoryDerivationEnvelope(
	ctx context.Context,
	residentID, runID canonical.ID,
	cause error,
) error {
	if cause == nil || errors.Is(cause, context.Canceled) || errors.Is(cause, context.DeadlineExceeded) {
		return cause
	}
	if runID.IsZero() {
		return cause
	}
	_, lifecycleWon, err := a.rejectGenerationEnvelopeAtResidentBoundary(ctx, residentID, runID)
	if err != nil {
		return errors.Join(cause, fmt.Errorf("app: terminalize unsupported derivation generation envelope: %w", err))
	}
	if lifecycleWon {
		return nil
	}
	return cause
}
