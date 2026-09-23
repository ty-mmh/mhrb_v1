package domain

import (
	"context"
	"fmt"

	"mahoroba.local/mahoroba/internal/canonical"
)

func PersonaRevisionObligation(stageTransitionID canonical.ID) string {
	return "persona_revision:v1:" + stageTransitionID.String()
}

type PersonaSourceClaim struct {
	ClaimID   canonical.ID
	Statement string
}

type PersonaRevisionWork struct {
	ResidentID               canonical.ID
	TriggerStageTransitionID canonical.ID
	PolicyRevisionID         canonical.ID
	PolicyContent            string
	PipelineVersionID        canonical.ID
	CurrentPersonaRevisionID canonical.ID
	CurrentPersona           string
	LastActivationAt         *canonical.Instant
	Claims                   []PersonaSourceClaim
	RunID                    *canonical.ID
	AttemptNo                int64
	RetryCount               int64
	State                    WorkState
	ForegroundPreempted      bool
}

type PersonaRevisionWorkRepository interface {
	DiscoverPersonaRevisionWork(context.Context, canonical.ID, int) (*PersonaRevisionWork, error)
}

type LandPersonaRevision struct {
	Attempt
	TriggerStageTransitionID canonical.ID
	PipelineVersionID        canonical.ID
	MemoryPolicyRevisionID   canonical.ID
	ParentPersonaRevisionID  canonical.ID
	PersonaRevisionID        canonical.ID
	PersonaActivationID      canonical.ID
	Output                   Content
	PersonaContent           Content
	PromptTokens             *int64
	CompletionTokens         *int64
	LatencyMicros            int64
}

type PersonaRevisionLandingResult struct {
	RevisionID      canonical.ID  `json:"revision_id"`
	ActivationID    *canonical.ID `json:"activation_id,omitempty"`
	AutoActivated   bool          `json:"auto_activated"`
	BlockingReasons []string      `json:"blocking_reasons,omitempty"`
}

type memoryPersonaMutator interface {
	LandPersonaRevision(context.Context, LandPersonaRevision) (PersonaRevisionLandingResult, error)
}

func LandPersonaRevisionCommand(value LandPersonaRevision) canonical.Command {
	scope, _ := canonical.ResidentScope(value.ResidentID)
	return command{
		name: "LandPersonaRevision", scope: scope,
		validate: func() error {
			if value.AttemptNo < 1 || value.LatencyMicros < 0 {
				return fmt.Errorf("domain: invalid persona revision attempt")
			}
			for _, id := range []canonical.ID{
				value.RunID, value.ResidentID, value.OutcomeID, value.TriggerStageTransitionID,
				value.PipelineVersionID, value.MemoryPolicyRevisionID, value.ParentPersonaRevisionID,
				value.PersonaRevisionID, value.PersonaActivationID,
			} {
				if err := id.Validate(); err != nil {
					return err
				}
			}
			if value.Output.ResidentID != value.ResidentID || value.Output.Class != "generation_output" {
				return fmt.Errorf("domain: invalid persona generation output")
			}
			if value.PersonaContent.ResidentID != value.ResidentID || value.PersonaContent.Class != "persona_text" {
				return fmt.Errorf("domain: invalid persona revision content")
			}
			if err := value.Output.Validate(); err != nil {
				return err
			}
			return value.PersonaContent.Validate()
		},
		execute: func(ctx context.Context, store MutationStore) (any, error) {
			mutator, ok := store.(memoryPersonaMutator)
			if !ok {
				return nil, fmt.Errorf("domain: canonical UoW lacks persona revision landing capability")
			}
			return mutator.LandPersonaRevision(ctx, value)
		},
	}
}

type MemoryPersonaProposalResult struct {
	Changed        bool                          `json:"changed"`
	RunID          *canonical.ID                 `json:"run_id,omitempty"`
	AttemptNo      int64                         `json:"attempt_no,omitempty"`
	Landing        *PersonaRevisionLandingResult `json:"landing,omitempty"`
	BlockingReason string                        `json:"blocking_reason,omitempty"`
}
