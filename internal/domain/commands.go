package domain

import (
	"context"
	"fmt"

	"mahoroba.local/mahoroba/internal/canonical"
	generationpkg "mahoroba.local/mahoroba/internal/generation"
)

type command struct {
	name     string
	scope    canonical.Scope
	validate func() error
	execute  func(context.Context, MutationStore) (any, error)
}

func (c command) Name() string           { return c.name }
func (c command) Scope() canonical.Scope { return c.scope }
func (c command) Validate() error        { return c.validate() }
func (c command) Execute(ctx context.Context, uow canonical.CanonicalUoW) (any, error) {
	store, ok := uow.(MutationStore)
	if !ok {
		return nil, fmt.Errorf("domain: canonical UoW lacks command capability for %s", c.name)
	}
	return c.execute(ctx, store)
}

func GlobalBootstrapCommand(value GlobalBootstrap) canonical.Command {
	return command{
		name: "GlobalBootstrap", scope: canonical.GlobalScope(),
		validate: func() error {
			if value.OwnerDisplayName == "" || value.PipelineDefinition.IsZero() || value.SessionDefinition.IsZero() {
				return fmt.Errorf("domain: incomplete global bootstrap")
			}
			for _, id := range []canonical.ID{value.SystemPrincipalID, value.OwnerPrincipalID, value.PipelineVersionID, value.SessionPolicyID} {
				if err := id.Validate(); err != nil {
					return err
				}
			}
			return ValidateExactDialoguePipelineDefinition(PipelineVersionDefinition{
				ID: value.PipelineVersionID, Kind: "dialogue", VersionKey: DialoguePipelineVersionV4,
				Definition: value.PipelineDefinition,
			})
		},
		execute: func(ctx context.Context, store MutationStore) (any, error) {
			return nil, store.InsertGlobalBootstrap(ctx, value)
		},
	}
}

func DraftResidentCommand(value DraftResident) canonical.Command {
	scope, _ := canonical.ResidentScope(value.ResidentID)
	return command{
		name: "CreateDraftResident", scope: scope,
		validate: func() error {
			if value.Name == "" || value.SeedKey == "" {
				return fmt.Errorf("domain: resident name and seed key are required")
			}
			for _, id := range []canonical.ID{value.ResidentID, value.ResidentPrincipalID, value.OwnerPrincipalID, value.PrinciplesRevisionID, value.StatusTransitionID} {
				if err := id.Validate(); err != nil {
					return err
				}
			}
			return value.PrinciplesContent.Validate()
		},
		execute: func(ctx context.Context, store MutationStore) (any, error) {
			return value.ResidentID, store.InsertDraftResident(ctx, value)
		},
	}
}

func ApprovePrinciplesCommand(value ApprovePrinciples) canonical.Command {
	scope, _ := canonical.ResidentScope(value.ResidentID)
	return command{
		name: "ApprovePrinciples", scope: scope,
		validate: func() error {
			for _, id := range []canonical.ID{value.ResidentID, value.RevisionID, value.OwnerPrincipalID, value.ApprovalID, value.ActivationID} {
				if err := id.Validate(); err != nil {
					return err
				}
			}
			return nil
		},
		execute: func(ctx context.Context, store MutationStore) (any, error) {
			return nil, store.ApproveAndActivatePrinciples(ctx, value)
		},
	}
}

func FinalizeResidentCommand(value FinalizeResident) canonical.Command {
	scope, _ := canonical.ResidentScope(value.ResidentID)
	return command{
		name: "FinalizeBootstrap", scope: scope,
		validate: func() error {
			for _, id := range []canonical.ID{value.ResidentID, value.OwnerPrincipalID, value.PersonaRevisionID, value.PersonaActivationID, value.MemoryRevisionID, value.MemoryActivationID, value.StatusTransitionID} {
				if err := id.Validate(); err != nil {
					return err
				}
			}
			if err := value.PersonaContent.Validate(); err != nil {
				return err
			}
			return value.MemoryContent.Validate()
		},
		execute: func(ctx context.Context, store MutationStore) (any, error) {
			return nil, store.FinalizeResident(ctx, value)
		},
	}
}

func ArchiveResidentCommand(value ArchiveResident) canonical.Command {
	scope, _ := canonical.ResidentScope(value.ResidentID)
	return command{
		name: "ArchiveResident", scope: scope,
		validate: func() error {
			for _, id := range []canonical.ID{value.ResidentID, value.OwnerPrincipalID, value.StatusTransitionID} {
				if err := id.Validate(); err != nil {
					return err
				}
			}
			return nil
		},
		execute: func(ctx context.Context, store MutationStore) (any, error) {
			return nil, store.ArchiveResident(ctx, value)
		},
	}
}

func IngressUserMessageCommand(value IngressUserMessage) canonical.Command {
	scope, _ := canonical.ResidentScope(value.ResidentID)
	return command{
		name: "IngressUserMessage", scope: scope,
		validate: func() error {
			for _, id := range []canonical.ID{value.EventID, value.ResidentID, value.OwnerPrincipalID, value.ResidentPrincipalID} {
				if err := id.Validate(); err != nil {
					return err
				}
			}
			if value.OccurredTZ == "" {
				return fmt.Errorf("domain: occurred timezone is required")
			}
			return value.Content.Validate()
		},
		execute: func(ctx context.Context, store MutationStore) (any, error) {
			return store.IngressUserMessage(ctx, value)
		},
	}
}

func PrepareGenerationCommand(value PrepareGeneration) canonical.Command {
	scope, _ := canonical.ResidentScope(value.ResidentID)
	return command{
		name: "PrepareGeneration", scope: scope,
		validate: func() error {
			purpose := value.Purpose.Effective()
			if purpose == GenerationPurposeDialogue {
				return fmt.Errorf("domain: dialogue generation requires PrepareDialogue")
			}
			if purpose == GenerationPurposeSelfTalk || purpose == GenerationPurposeOutboundInitiative {
				return fmt.Errorf("domain: autonomous generation requires PrepareAutonomousGeneration")
			}
			if value.IdempotencyKey == "" || value.Provider == "" || value.Model == "" ||
				value.GeneratorParams.IsZero() || len(value.Inputs) == 0 {
				return fmt.Errorf("domain: incomplete generation envelope")
			}
			params, _, err := ParseGeneratorParams(value.GeneratorParams.Bytes())
			if err != nil {
				return err
			}
			if err := ValidateNewGeneratorParamsForPurpose(value.Purpose, params); err != nil {
				return err
			}
			if err := ValidateGenerationVersions(value.Purpose, GenerationVersionContract{
				PromptTemplateVersion:  value.PromptTemplateVersion,
				ContextPolicyVersion:   value.ContextPolicyVersion,
				MemoryRenderingVersion: value.MemoryRenderingVersion,
			}); err != nil {
				return err
			}
			if err := validateGenerationSessionPolicy(value.Purpose, value.SessionPolicyID); err != nil {
				return err
			}
			for _, id := range []canonical.ID{value.RunID, value.ResidentID, value.PipelineVersionID, value.PrinciplesRevisionID, value.PersonaRevisionID, value.MemoryPolicyRevisionID, value.RunningOutcomeID} {
				if err := id.Validate(); err != nil {
					return err
				}
			}
			if value.RecallRunID != nil {
				if err := value.RecallRunID.Validate(); err != nil {
					return err
				}
				if value.Purpose.Effective() != GenerationPurposeDialogue {
					return fmt.Errorf("domain: only dialogue generation may link a Recall run")
				}
			}
			for index, input := range value.Inputs {
				if input.Ordinal != int64(index) {
					return fmt.Errorf("domain: generation input ordinals must be contiguous")
				}
				if err := input.ID.Validate(); err != nil {
					return err
				}
				if err := input.Content.Validate(); err != nil {
					return err
				}
			}
			return nil
		},
		execute: func(ctx context.Context, store MutationStore) (any, error) {
			return value.RunID, store.PrepareGeneration(ctx, value)
		},
	}
}

func validateGenerationSessionPolicy(purpose GenerationPurpose, policyID *canonical.ID) error {
	if err := purpose.Validate(); err != nil {
		return err
	}
	if purpose.Effective() == GenerationPurposeDialogue && policyID == nil {
		return fmt.Errorf("domain: dialogue generation requires a sessionization policy")
	}
	if policyID != nil {
		return policyID.Validate()
	}
	return nil
}

func CancelDialogueCommand(value CancelDialogue) canonical.Command {
	generation := value.Generation
	scope, _ := canonical.ResidentScope(generation.ResidentID)
	return command{
		name: "CancelDialogue", scope: scope,
		validate: func() error {
			if generation.IdempotencyKey != DialogueObligation(value.SourceEventID) ||
				generation.Provider == "" || generation.Model == "" || len(generation.Inputs) != 0 ||
				generation.DroppedInputSummary.String() == "" || generation.GeneratorParams.String() == "" ||
				value.ErrorClass == "" {
				return fmt.Errorf("domain: incomplete dialogue cancellation envelope")
			}
			attemptBound := value.ExpectedRunID != nil || value.ExpectedAttemptNo != nil
			if (value.ExpectedRunID == nil) != (value.ExpectedAttemptNo == nil) {
				return fmt.Errorf("domain: dialogue cancellation run and attempt expectations must be supplied together")
			}
			if attemptBound {
				code, err := generationpkg.ParseOutcomeErrorCode(value.ErrorClass)
				if err != nil || code.Class() != generationpkg.ErrorSourceContentErased {
					return fmt.Errorf("domain: only erased-source dialogue cancellation may be attempt-bound")
				}
				if err := value.ExpectedRunID.Validate(); err != nil {
					return err
				}
				if *value.ExpectedAttemptNo < 1 {
					return fmt.Errorf("domain: expected dialogue cancellation attempt must be positive")
				}
			}
			if generation.Purpose.Effective() != GenerationPurposeDialogue {
				return fmt.Errorf("domain: dialogue cancellation requires dialogue purpose")
			}
			params, _, err := ParseGeneratorParams(generation.GeneratorParams.Bytes())
			if err != nil {
				return err
			}
			if err := ValidateNewGeneratorParamsForPurpose(GenerationPurposeDialogue, params); err != nil {
				return err
			}
			if err := ValidateDialogueCancellationVersionContract(GenerationVersionContract{
				PromptTemplateVersion:  generation.PromptTemplateVersion,
				ContextPolicyVersion:   generation.ContextPolicyVersion,
				MemoryRenderingVersion: generation.MemoryRenderingVersion,
			}); err != nil {
				return err
			}
			if err := validateGenerationSessionPolicy(GenerationPurposeDialogue, generation.SessionPolicyID); err != nil {
				return err
			}
			for _, id := range []canonical.ID{
				generation.RunID, generation.ResidentID, value.SourceEventID,
				generation.PipelineVersionID,
				generation.PrinciplesRevisionID, generation.PersonaRevisionID,
				generation.MemoryPolicyRevisionID, generation.RunningOutcomeID,
				value.CancelledOutcomeID,
			} {
				if err := id.Validate(); err != nil {
					return err
				}
			}
			return nil
		},
		execute: func(ctx context.Context, store MutationStore) (any, error) {
			return store.CancelDialogue(ctx, value)
		},
	}
}

func StartAttemptCommand(value Attempt) canonical.Command {
	return attemptCommand("StartGenerationAttempt", value, func(ctx context.Context, store MutationStore) (any, error) {
		return nil, store.StartAttempt(ctx, value)
	})
}

func FailAttemptCommand(value FailAttempt) canonical.Command {
	return attemptCommand("FailGenerationAttempt", value.Attempt, func(ctx context.Context, store MutationStore) (any, error) {
		if value.State != "failed" && value.State != "cancelled" {
			return nil, fmt.Errorf("domain: invalid failure state")
		}
		if value.ErrorClass == "" {
			return nil, fmt.Errorf("domain: failure error class is required")
		}
		return nil, store.FailAttempt(ctx, value)
	})
}

func RejectGenerationEnvelopeCommand(value RejectGenerationEnvelope) canonical.Command {
	scope, _ := canonical.ResidentScope(value.ResidentID)
	return command{
		name: "RejectGenerationEnvelope", scope: scope,
		validate: func() error {
			for _, id := range []canonical.ID{value.RunID, value.ResidentID, value.RunningOutcomeID, value.RejectedOutcomeID} {
				if err := id.Validate(); err != nil {
					return err
				}
			}
			if value.RunningOutcomeID == value.RejectedOutcomeID {
				return fmt.Errorf("domain: generation rejection outcome IDs must be distinct")
			}
			code, err := generationpkg.ParseOutcomeErrorCode(value.ErrorClass)
			if err != nil || code.Class() != generationpkg.ErrorProviderUnsupported {
				return fmt.Errorf("domain: invalid generation rejection error class")
			}
			return nil
		},
		execute: func(ctx context.Context, store MutationStore) (any, error) {
			return store.RejectGenerationEnvelope(ctx, value)
		},
	}
}

func LandDialogueCommand(value LandDialogue) canonical.Command {
	scope, _ := canonical.ResidentScope(value.ResidentID)
	return command{
		name: "LandDialogue", scope: scope,
		validate: func() error {
			if value.AttemptNo < 1 {
				return fmt.Errorf("domain: invalid attempt number")
			}
			if value.Attempt.ResidentID != value.ResidentID {
				return fmt.Errorf("domain: attempt resident mismatch")
			}
			for _, id := range []canonical.ID{value.RunID, value.OutcomeID, value.EventID, value.ResidentID, value.ResidentPrincipalID, value.OwnerPrincipalID} {
				if err := id.Validate(); err != nil {
					return err
				}
			}
			return value.Output.Validate()
		},
		execute: func(ctx context.Context, store MutationStore) (any, error) { return store.LandDialogue(ctx, value) },
	}
}

func attemptCommand(name string, value Attempt, execute func(context.Context, MutationStore) (any, error)) canonical.Command {
	scope, _ := canonical.ResidentScope(value.ResidentID)
	return command{
		name: name, scope: scope,
		validate: func() error {
			if err := value.RunID.Validate(); err != nil {
				return err
			}
			if err := value.ResidentID.Validate(); err != nil {
				return err
			}
			if err := value.OutcomeID.Validate(); err != nil {
				return err
			}
			if value.AttemptNo < 1 {
				return fmt.Errorf("domain: invalid attempt number")
			}
			return nil
		}, execute: execute,
	}
}
