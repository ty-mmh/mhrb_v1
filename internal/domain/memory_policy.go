package domain

import (
	"context"
	"fmt"

	"mahoroba.local/mahoroba/internal/canonical"
	"mahoroba.local/mahoroba/internal/memory"
)

type ActivateMemoryPolicy struct {
	ResidentID                    canonical.ID
	OwnerPrincipalID              canonical.ID
	RevisionID                    canonical.ID
	ActivationID                  canonical.ID
	Content                       Content
	ExpectedFrom                  memory.PolicyVersion
	AcknowledgeRecallEnable       bool
	AcknowledgeSelfTalkExtraction bool
}

type MemoryPolicyActivationResult struct {
	RevisionID            canonical.ID         `json:"revision_id"`
	ActivationID          canonical.ID         `json:"activation_id"`
	Changed               bool                 `json:"changed"`
	PreviousPolicyVersion memory.PolicyVersion `json:"previous_policy_version"`
	PolicyVersion         memory.PolicyVersion `json:"policy_version"`
	RenderingVersion      string               `json:"rendering_version"`
}

type memoryPolicyMutator interface {
	ActivateMemoryPolicy(context.Context, ActivateMemoryPolicy) (MemoryPolicyActivationResult, error)
}

func ActivateMemoryPolicyCommand(value ActivateMemoryPolicy) canonical.Command {
	scope, _ := canonical.ResidentScope(value.ResidentID)
	return command{
		name: "ActivateMemoryPolicy", scope: scope,
		validate: func() error {
			for _, id := range []canonical.ID{
				value.ResidentID, value.OwnerPrincipalID, value.RevisionID, value.ActivationID,
			} {
				if err := id.Validate(); err != nil {
					return err
				}
			}
			if value.Content.ResidentID != value.ResidentID ||
				value.Content.Class != "memory_policy_text" ||
				value.Content.ErasurePolicy != "resident_only" {
				return fmt.Errorf("domain: invalid memory policy content")
			}
			if err := value.ExpectedFrom.Validate(); err != nil {
				return fmt.Errorf("domain: memory policy expected-from is required: %w", err)
			}
			return value.Content.Validate()
		},
		execute: func(ctx context.Context, store MutationStore) (any, error) {
			mutator, ok := store.(memoryPolicyMutator)
			if !ok {
				return nil, fmt.Errorf("domain: canonical UoW lacks memory policy activation capability")
			}
			return mutator.ActivateMemoryPolicy(ctx, value)
		},
	}
}
