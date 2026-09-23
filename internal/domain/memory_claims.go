package domain

import (
	"context"
	"fmt"

	"mahoroba.local/mahoroba/internal/canonical"
	"mahoroba.local/mahoroba/internal/memory"
)

type AddClaimEvidence struct {
	ResidentID                  canonical.ID
	ClaimID                     canonical.ID
	EvidenceID                  canonical.ID
	SourceEventID               canonical.ID
	SourceEvidenceID            *canonical.ID
	Polarity                    memory.EvidencePolarity
	Grade                       memory.EvidenceGrade
	Derivation                  memory.EvidenceDerivation
	Reason                      memory.EvidenceReason
	MemoryPolicyRevisionID      canonical.ID
	CreatedByRunID              canonical.ID
	MaturationPipelineVersionID canonical.ID
	SedimentStageTransitionID   canonical.ID
	SettledStageTransitionID    canonical.ID
}

type AddClaimEvidenceResult struct {
	EvidenceID       canonical.ID
	FinalStage       memory.ClaimStage
	StageTransitions []canonical.ID
}

type claimEvidenceMutator interface {
	AddClaimEvidence(context.Context, AddClaimEvidence) (AddClaimEvidenceResult, error)
}

func AddClaimEvidenceCommand(value AddClaimEvidence) canonical.Command {
	scope, _ := canonical.ResidentScope(value.ResidentID)
	return command{
		name: "AddClaimEvidence", scope: scope,
		validate: func() error { return validateAddClaimEvidence(value) },
		execute: func(ctx context.Context, store MutationStore) (any, error) {
			mutator, ok := store.(claimEvidenceMutator)
			if !ok {
				return nil, fmt.Errorf("domain: canonical UoW lacks claim evidence capability")
			}
			return mutator.AddClaimEvidence(ctx, value)
		},
	}
}

func validateAddClaimEvidence(value AddClaimEvidence) error {
	for _, id := range []canonical.ID{
		value.ResidentID, value.ClaimID, value.EvidenceID, value.SourceEventID,
		value.MemoryPolicyRevisionID, value.CreatedByRunID, value.MaturationPipelineVersionID,
		value.SedimentStageTransitionID, value.SettledStageTransitionID,
	} {
		if err := id.Validate(); err != nil {
			return err
		}
	}
	if value.EvidenceID == value.SourceEventID ||
		value.SedimentStageTransitionID == value.SettledStageTransitionID {
		return fmt.Errorf("domain: claim evidence identities must be distinct")
	}
	// Projection replay orders same-commit transitions by ID. Requiring this
	// order makes floating -> sediment -> settled deterministic even when both
	// transitions are appended by one Canonical UoW.
	if value.SedimentStageTransitionID.String() >= value.SettledStageTransitionID.String() {
		return fmt.Errorf("domain: sediment transition ID must sort before settled transition ID")
	}
	if err := value.Polarity.Validate(); err != nil {
		return err
	}
	if err := value.Grade.Validate(); err != nil {
		return err
	}
	if err := value.Derivation.Validate(); err != nil {
		return err
	}
	if err := value.Reason.Validate(); err != nil {
		return err
	}
	if value.SourceEvidenceID != nil {
		if err := value.SourceEvidenceID.Validate(); err != nil {
			return err
		}
		if *value.SourceEvidenceID == value.EvidenceID {
			return fmt.Errorf("domain: evidence cannot inherit from itself")
		}
	}
	if (value.Derivation == memory.DerivationExtracted) != (value.SourceEvidenceID == nil) {
		return fmt.Errorf("domain: claim evidence derivation provenance is inconsistent")
	}
	return nil
}

type HumanClaimStatusDecision struct {
	ResidentID         canonical.ID
	ClaimID            canonical.ID
	StatusTransitionID canonical.ID
	OwnerPrincipalID   canonical.ID
	ToStatus           memory.ClaimStatus
	Reason             memory.HumanDecisionReason
	ReasonContentID    *canonical.ID
}

type ClaimStatusDecisionResult struct {
	TransitionID canonical.ID
	FromStatus   memory.ClaimStatus
	ToStatus     memory.ClaimStatus
}

type humanClaimStatusMutator interface {
	HumanClaimStatusDecision(context.Context, HumanClaimStatusDecision) (ClaimStatusDecisionResult, error)
}

func HumanClaimStatusDecisionCommand(value HumanClaimStatusDecision) canonical.Command {
	scope, _ := canonical.ResidentScope(value.ResidentID)
	return command{
		name: "HumanClaimStatusDecision", scope: scope,
		validate: func() error {
			for _, id := range []canonical.ID{value.ResidentID, value.ClaimID, value.StatusTransitionID, value.OwnerPrincipalID} {
				if err := id.Validate(); err != nil {
					return err
				}
			}
			if value.ReasonContentID != nil {
				if err := value.ReasonContentID.Validate(); err != nil {
					return err
				}
			}
			if err := value.ToStatus.Validate(); err != nil {
				return err
			}
			if err := value.Reason.Validate(); err != nil {
				return err
			}
			if !humanReasonMatchesStatus(value.Reason, value.ToStatus) {
				return fmt.Errorf("domain: human claim status reason does not match target status")
			}
			return nil
		},
		execute: func(ctx context.Context, store MutationStore) (any, error) {
			mutator, ok := store.(humanClaimStatusMutator)
			if !ok {
				return nil, fmt.Errorf("domain: canonical UoW lacks human claim status capability")
			}
			return mutator.HumanClaimStatusDecision(ctx, value)
		},
	}
}

type ClaimStatusTriggerKind string

const (
	ClaimTriggerEvidence         ClaimStatusTriggerKind = "claim_evidence"
	ClaimTriggerRelation         ClaimStatusTriggerKind = "claim_relation"
	ClaimTriggerIntegrityFinding ClaimStatusTriggerKind = "integrity_finding"
)

func (value ClaimStatusTriggerKind) Validate() error {
	switch value {
	case ClaimTriggerEvidence, ClaimTriggerRelation, ClaimTriggerIntegrityFinding:
		return nil
	default:
		return fmt.Errorf("domain: invalid claim status trigger kind %q", value)
	}
}

type AutomaticClaimStatusDecision struct {
	ResidentID              canonical.ID
	ClaimID                 canonical.ID
	StatusTransitionID      canonical.ID
	ToStatus                memory.ClaimStatus
	Reason                  memory.AutomaticDecisionReason
	TriggerKind             ClaimStatusTriggerKind
	TriggerID               canonical.ID
	StatusPipelineVersionID canonical.ID
	MemoryPolicyRevisionID  *canonical.ID
}

type automaticClaimStatusMutator interface {
	AutomaticClaimStatusDecision(context.Context, AutomaticClaimStatusDecision) (ClaimStatusDecisionResult, error)
}

func AutomaticClaimStatusDecisionCommand(value AutomaticClaimStatusDecision) canonical.Command {
	scope, _ := canonical.ResidentScope(value.ResidentID)
	return command{
		name: "AutomaticClaimStatusDecision", scope: scope,
		validate: func() error { return validateAutomaticClaimStatusDecision(value) },
		execute: func(ctx context.Context, store MutationStore) (any, error) {
			mutator, ok := store.(automaticClaimStatusMutator)
			if !ok {
				return nil, fmt.Errorf("domain: canonical UoW lacks automatic claim status capability")
			}
			return mutator.AutomaticClaimStatusDecision(ctx, value)
		},
	}
}

func validateAutomaticClaimStatusDecision(value AutomaticClaimStatusDecision) error {
	for _, id := range []canonical.ID{
		value.ResidentID, value.ClaimID, value.StatusTransitionID,
		value.TriggerID, value.StatusPipelineVersionID,
	} {
		if err := id.Validate(); err != nil {
			return err
		}
	}
	if err := value.ToStatus.Validate(); err != nil {
		return err
	}
	if err := value.Reason.Validate(); err != nil {
		return err
	}
	if err := value.TriggerKind.Validate(); err != nil {
		return err
	}
	wantStatus, wantTrigger := automaticReasonContract(value.Reason)
	if value.ToStatus != wantStatus || value.TriggerKind != wantTrigger {
		return fmt.Errorf("domain: automatic reason, trigger, and target status do not match")
	}
	policyRequired := value.Reason != memory.AutomaticReasonStructuralQuarantine
	if policyRequired != (value.MemoryPolicyRevisionID != nil) {
		return fmt.Errorf("domain: automatic semantic decisions require exactly one memory policy revision")
	}
	if value.MemoryPolicyRevisionID != nil {
		if err := value.MemoryPolicyRevisionID.Validate(); err != nil {
			return err
		}
	}
	return nil
}

func humanReasonMatchesStatus(reason memory.HumanDecisionReason, status memory.ClaimStatus) bool {
	switch reason {
	case memory.HumanReasonInvalidation:
		return status == memory.StatusInvalidated
	case memory.HumanReasonSupersession:
		return status == memory.StatusSuperseded
	case memory.HumanReasonQuarantine:
		return status == memory.StatusQuarantined
	case memory.HumanReasonReactivation:
		return status == memory.StatusActive
	default:
		return false
	}
}

func automaticReasonContract(reason memory.AutomaticDecisionReason) (memory.ClaimStatus, ClaimStatusTriggerKind) {
	switch reason {
	case memory.AutomaticReasonExplicitCorrection:
		return memory.StatusInvalidated, ClaimTriggerEvidence
	case memory.AutomaticReasonExplicitSupersession:
		return memory.StatusSuperseded, ClaimTriggerRelation
	case memory.AutomaticReasonStructuralQuarantine:
		return memory.StatusQuarantined, ClaimTriggerIntegrityFinding
	default:
		return "", ""
	}
}
