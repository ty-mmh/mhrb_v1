package domain

import (
	"context"
	"fmt"
	"strings"

	"mahoroba.local/mahoroba/internal/canonical"
)

func MemoryExtractionObligation(eventID canonical.ID) string {
	return "memory_extract:v1:" + eventID.String()
}

func MemoryReextractionObligation(eventID, requestID canonical.ID) string {
	return "memory_reextract:v1:" + eventID.String() + ":" + requestID.String()
}

type MemoryExtractionMode string

const (
	MemoryExtractionMandatory MemoryExtractionMode = "mandatory"
	MemoryExtractionReextract MemoryExtractionMode = "reextract"
)

type MemoryExtractionRequest struct {
	Mode          MemoryExtractionMode
	SourceEventID canonical.ID
	RequestID     *canonical.ID
}

// ParseMemoryExtractionObligation rejects prefixes, suffixes, and malformed
// ULIDs. Writer adapters use it to keep mandatory discovery and Admin
// re-extraction in separate idempotency domains.
func ParseMemoryExtractionObligation(value string) (MemoryExtractionRequest, error) {
	if raw, ok := strings.CutPrefix(value, "memory_extract:v1:"); ok {
		eventID, err := canonical.ParseID(raw)
		if err != nil || raw != eventID.String() {
			return MemoryExtractionRequest{}, fmt.Errorf("domain: invalid mandatory memory extraction key")
		}
		return MemoryExtractionRequest{Mode: MemoryExtractionMandatory, SourceEventID: eventID}, nil
	}
	if raw, ok := strings.CutPrefix(value, "memory_reextract:v1:"); ok {
		eventRaw, requestRaw, found := strings.Cut(raw, ":")
		if !found || strings.Contains(requestRaw, ":") {
			return MemoryExtractionRequest{}, fmt.Errorf("domain: invalid memory re-extraction key")
		}
		eventID, eventErr := canonical.ParseID(eventRaw)
		requestID, requestErr := canonical.ParseID(requestRaw)
		if eventErr != nil || requestErr != nil || eventRaw != eventID.String() || requestRaw != requestID.String() {
			return MemoryExtractionRequest{}, fmt.Errorf("domain: invalid memory re-extraction key")
		}
		return MemoryExtractionRequest{
			Mode: MemoryExtractionReextract, SourceEventID: eventID, RequestID: &requestID,
		}, nil
	}
	return MemoryExtractionRequest{}, fmt.Errorf("domain: unknown memory extraction key")
}

type ExtractedClaimLanding struct {
	ClaimID                   canonical.ID
	EvidenceID                canonical.ID
	InitialStageID            canonical.ID
	InitialViewScopeID        canonical.ID
	SedimentStageTransitionID canonical.ID
	SettledStageTransitionID  canonical.ID
	Statement                 Content
}

type LandMemoryExtraction struct {
	Attempt
	SourceEventID               canonical.ID
	PipelineVersionID           canonical.ID
	MaturationPipelineVersionID canonical.ID
	MemoryPolicyRevisionID      canonical.ID
	Output                      Content
	Claims                      []ExtractedClaimLanding
	PromptTokens                *int64
	CompletionTokens            *int64
	LatencyMicros               int64
}

type MemoryExtractionLandingResult struct {
	ClaimIDs []canonical.ID
}

type CancelMemoryExtraction struct {
	Generation         PrepareGeneration
	SourceEventID      canonical.ID
	CancelledOutcomeID canonical.ID
	ErrorClass         string
}

type MemoryExtractionCancellationResult struct {
	RunID     canonical.ID
	AttemptNo int64
}

type PrepareMemoryReextraction struct {
	Generation    PrepareGeneration
	SourceEventID canonical.ID
	RequestID     canonical.ID
}

type MemoryReextractionResult struct {
	RunID     canonical.ID
	AttemptNo int64
	State     WorkState
	Changed   bool
}

type FailMemoryExtraction struct {
	Attempt
	State       string
	ErrorClass  string
	ErrorDetail *Content
}

type memoryExtractionMutator interface {
	LandMemoryExtraction(context.Context, LandMemoryExtraction) (MemoryExtractionLandingResult, error)
}

type memoryExtractionCanceller interface {
	CancelMemoryExtraction(context.Context, CancelMemoryExtraction) (MemoryExtractionCancellationResult, error)
}

type memoryExtractionFailureMutator interface {
	FailMemoryExtraction(context.Context, FailMemoryExtraction) error
}

type memoryReextractionPreparer interface {
	PrepareMemoryReextraction(context.Context, PrepareMemoryReextraction) (MemoryReextractionResult, error)
}

func PrepareMemoryReextractionCommand(value PrepareMemoryReextraction) canonical.Command {
	envelope := value.Generation
	scope, _ := canonical.ResidentScope(envelope.ResidentID)
	return command{
		name: "PrepareMemoryReextraction", scope: scope,
		validate: func() error {
			if err := value.SourceEventID.Validate(); err != nil {
				return err
			}
			if err := value.RequestID.Validate(); err != nil {
				return err
			}
			request, err := ParseMemoryExtractionObligation(envelope.IdempotencyKey)
			if err != nil || request.Mode != MemoryExtractionReextract || request.RequestID == nil ||
				request.SourceEventID != value.SourceEventID || *request.RequestID != value.RequestID {
				return fmt.Errorf("domain: re-extraction envelope/key mismatch")
			}
			if envelope.Purpose.Effective() != GenerationPurposeMemoryExtraction || envelope.SessionPolicyID != nil ||
				len(envelope.Inputs) != 4 {
				return fmt.Errorf("domain: invalid memory re-extraction envelope")
			}
			if err := PrepareGenerationCommand(envelope).Validate(); err != nil {
				return err
			}
			current := envelope.Inputs[3]
			if current.SourceID == nil || *current.SourceID != value.SourceEventID ||
				current.SourceType != "event" || current.InclusionMode != "current_input" {
				return fmt.Errorf("domain: re-extraction source input mismatch")
			}
			return nil
		},
		execute: func(ctx context.Context, store MutationStore) (any, error) {
			mutator, ok := store.(memoryReextractionPreparer)
			if !ok {
				return nil, fmt.Errorf("domain: canonical UoW lacks memory re-extraction preparation capability")
			}
			return mutator.PrepareMemoryReextraction(ctx, value)
		},
	}
}

func LandMemoryExtractionCommand(value LandMemoryExtraction) canonical.Command {
	scope, _ := canonical.ResidentScope(value.ResidentID)
	return command{
		name: "LandMemoryExtraction", scope: scope,
		validate: func() error {
			if value.AttemptNo < 1 || value.LatencyMicros < 0 {
				return fmt.Errorf("domain: invalid memory extraction attempt")
			}
			for _, id := range []canonical.ID{
				value.RunID, value.ResidentID, value.OutcomeID, value.SourceEventID,
				value.PipelineVersionID, value.MaturationPipelineVersionID, value.MemoryPolicyRevisionID,
			} {
				if err := id.Validate(); err != nil {
					return err
				}
			}
			if value.Output.ResidentID != value.ResidentID || value.Output.Class != "generation_output" {
				return fmt.Errorf("domain: invalid memory extraction output content")
			}
			if err := value.Output.Validate(); err != nil {
				return err
			}
			seen := make(map[canonical.ID]struct{}, len(value.Claims)*7)
			for _, claim := range value.Claims {
				for _, id := range []canonical.ID{
					claim.ClaimID, claim.EvidenceID, claim.InitialStageID,
					claim.InitialViewScopeID, claim.SedimentStageTransitionID,
					claim.SettledStageTransitionID, claim.Statement.ID,
				} {
					if err := id.Validate(); err != nil {
						return err
					}
					if _, duplicate := seen[id]; duplicate {
						return fmt.Errorf("domain: duplicate memory landing ID")
					}
					seen[id] = struct{}{}
				}
				if claim.SedimentStageTransitionID.String() >= claim.SettledStageTransitionID.String() {
					return fmt.Errorf("domain: sediment transition ID must sort before settled transition ID")
				}
				if claim.Statement.ResidentID != value.ResidentID || claim.Statement.Class != "claim_statement" {
					return fmt.Errorf("domain: invalid extracted claim statement content")
				}
				if err := claim.Statement.Validate(); err != nil {
					return err
				}
			}
			return nil
		},
		execute: func(ctx context.Context, store MutationStore) (any, error) {
			mutator, ok := store.(memoryExtractionMutator)
			if !ok {
				return nil, fmt.Errorf("domain: canonical UoW lacks memory extraction landing capability")
			}
			return mutator.LandMemoryExtraction(ctx, value)
		},
	}
}

func CancelMemoryExtractionCommand(value CancelMemoryExtraction) canonical.Command {
	envelope := value.Generation
	scope, _ := canonical.ResidentScope(envelope.ResidentID)
	return command{
		name: "CancelMemoryExtraction", scope: scope,
		validate: func() error {
			request, err := ParseMemoryExtractionObligation(envelope.IdempotencyKey)
			if err != nil || request.SourceEventID != value.SourceEventID ||
				envelope.Purpose.Effective() != GenerationPurposeMemoryExtraction ||
				envelope.SessionPolicyID != nil || len(envelope.Inputs) != 0 || value.ErrorClass == "" {
				return fmt.Errorf("domain: incomplete memory extraction cancellation envelope")
			}
			for _, id := range []canonical.ID{
				envelope.RunID, envelope.ResidentID, envelope.PipelineVersionID,
				envelope.PrinciplesRevisionID, envelope.PersonaRevisionID,
				envelope.MemoryPolicyRevisionID, envelope.RunningOutcomeID,
				value.SourceEventID, value.CancelledOutcomeID,
			} {
				if err := id.Validate(); err != nil {
					return err
				}
			}
			params, _, err := ParseGeneratorParams(envelope.GeneratorParams.Bytes())
			if err != nil {
				return err
			}
			if err := ValidateNewGeneratorParamsForPurpose(GenerationPurposeMemoryExtraction, params); err != nil {
				return err
			}
			return ValidateGenerationVersions(GenerationPurposeMemoryExtraction, GenerationVersionContract{
				PromptTemplateVersion:  envelope.PromptTemplateVersion,
				ContextPolicyVersion:   envelope.ContextPolicyVersion,
				MemoryRenderingVersion: envelope.MemoryRenderingVersion,
			})
		},
		execute: func(ctx context.Context, store MutationStore) (any, error) {
			mutator, ok := store.(memoryExtractionCanceller)
			if !ok {
				return nil, fmt.Errorf("domain: canonical UoW lacks memory extraction cancellation capability")
			}
			return mutator.CancelMemoryExtraction(ctx, value)
		},
	}
}

func FailMemoryExtractionCommand(value FailMemoryExtraction) canonical.Command {
	scope, _ := canonical.ResidentScope(value.ResidentID)
	return command{
		name: "FailMemoryExtraction", scope: scope,
		validate: func() error {
			for _, id := range []canonical.ID{value.RunID, value.ResidentID, value.OutcomeID} {
				if err := id.Validate(); err != nil {
					return err
				}
			}
			if value.AttemptNo < 1 || (value.State != "failed" && value.State != "cancelled") || value.ErrorClass == "" {
				return fmt.Errorf("domain: invalid memory extraction failure")
			}
			if value.ErrorDetail != nil {
				if value.ErrorDetail.ResidentID != value.ResidentID || value.ErrorDetail.Class != "error_detail" {
					return fmt.Errorf("domain: invalid memory extraction error detail")
				}
				if err := value.ErrorDetail.Validate(); err != nil {
					return err
				}
			}
			return nil
		},
		execute: func(ctx context.Context, store MutationStore) (any, error) {
			mutator, ok := store.(memoryExtractionFailureMutator)
			if !ok {
				return nil, fmt.Errorf("domain: canonical UoW lacks memory extraction failure capability")
			}
			return nil, mutator.FailMemoryExtraction(ctx, value)
		},
	}
}
