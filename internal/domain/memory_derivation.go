package domain

import (
	"context"
	"fmt"

	"mahoroba.local/mahoroba/internal/canonical"
	"mahoroba.local/mahoroba/internal/memory"
)

const (
	MinimumAbstractionSources = 2
	MaximumAbstractionSources = 8
	MaximumInheritedEvidence  = 8
)

// DerivedClaimSource identifies one Canonical source claim and the relation
// ID that will connect the new claim to it. Both supported operations use the
// direction new -> source: abstracts for abstraction and split_from for
// differentiation.
type DerivedClaimSource struct {
	ClaimID    canonical.ID
	RelationID canonical.ID
}

// DerivedClaimEvidence supplies caller-allocated identity for one inherited
// row and points only at Canonical claim_evidence. The Writer resolves the
// underlying event from SourceEvidenceID; callers cannot substitute Recall or
// claim_usage provenance for evidence.
type DerivedClaimEvidence struct {
	EvidenceID       canonical.ID
	SourceClaimID    canonical.ID
	SourceEvidenceID canonical.ID
}

// LandDerivedClaim is the caller-validated success landing envelope shared by
// Admin abstraction and differentiation. Statement and Output are complete
// Content values so blob staging can occur before the Canonical transaction.
type LandDerivedClaim struct {
	Attempt
	OwnerPrincipalID       canonical.ID
	ClaimID                canonical.ID
	TemporalKind           memory.TemporalKind
	Statement              Content
	Output                 Content
	PipelineVersionID      canonical.ID
	MemoryPolicyRevisionID canonical.ID
	InitialStageID         canonical.ID
	InitialViewScopeID     canonical.ID
	Sources                []DerivedClaimSource
	Evidence               []DerivedClaimEvidence
	PromptTokens           *int64
	CompletionTokens       *int64
	LatencyMicros          int64
}

type DerivedClaimLandingResult struct {
	ClaimID            canonical.ID
	EvidenceIDs        []canonical.ID
	RelationIDs        []canonical.ID
	InitialStageID     canonical.ID
	InitialViewScopeID canonical.ID
	InitialViewScope   memory.ViewScope
}

type claimAbstractionMutator interface {
	LandClaimAbstraction(context.Context, LandDerivedClaim) (DerivedClaimLandingResult, error)
}

type claimDifferentiationMutator interface {
	LandClaimDifferentiation(context.Context, LandDerivedClaim) (DerivedClaimLandingResult, error)
}

func LandClaimAbstractionCommand(value LandDerivedClaim) canonical.Command {
	return derivedClaimCommand("LandClaimAbstraction", value, memory.RelationAbstracts)
}

func LandClaimDifferentiationCommand(value LandDerivedClaim) canonical.Command {
	return derivedClaimCommand("LandClaimDifferentiation", value, memory.RelationSplitFrom)
}

func derivedClaimCommand(name string, value LandDerivedClaim, relation memory.RelationType) canonical.Command {
	scope, _ := canonical.ResidentScope(value.ResidentID)
	return command{
		name: name, scope: scope,
		validate: func() error { return validateDerivedClaim(value, relation) },
		execute: func(ctx context.Context, store MutationStore) (any, error) {
			switch relation {
			case memory.RelationAbstracts:
				mutator, ok := store.(claimAbstractionMutator)
				if !ok {
					return nil, fmt.Errorf("domain: canonical UoW lacks claim abstraction capability")
				}
				return mutator.LandClaimAbstraction(ctx, value)
			case memory.RelationSplitFrom:
				mutator, ok := store.(claimDifferentiationMutator)
				if !ok {
					return nil, fmt.Errorf("domain: canonical UoW lacks claim differentiation capability")
				}
				return mutator.LandClaimDifferentiation(ctx, value)
			default:
				return nil, fmt.Errorf("domain: unsupported derived claim relation %q", relation)
			}
		},
	}
}

func validateDerivedClaim(value LandDerivedClaim, relation memory.RelationType) error {
	if value.AttemptNo < 1 || value.LatencyMicros < 0 {
		return fmt.Errorf("domain: derived claim landing requires a positive attempt")
	}
	ids := []canonical.ID{
		value.RunID, value.ResidentID, value.OutcomeID, value.OwnerPrincipalID,
		value.ClaimID, value.PipelineVersionID, value.MemoryPolicyRevisionID,
		value.InitialStageID, value.InitialViewScopeID,
	}
	seen := make(map[canonical.ID]struct{}, len(ids)+len(value.Sources)*2+len(value.Evidence)*2)
	for _, id := range ids {
		if err := id.Validate(); err != nil {
			return err
		}
		if _, duplicate := seen[id]; duplicate {
			return fmt.Errorf("domain: duplicate derived claim identity %s", id)
		}
		seen[id] = struct{}{}
	}
	if err := value.TemporalKind.Validate(); err != nil {
		return err
	}
	if value.Statement.ResidentID != value.ResidentID || value.Statement.Class != "claim_statement" ||
		value.Statement.ErasurePolicy != "independent" {
		return fmt.Errorf("domain: invalid derived claim statement content")
	}
	if err := value.Statement.Validate(); err != nil {
		return err
	}
	normalized, err := memory.NormalizeStatementV1(string(value.Statement.Bytes))
	if err != nil || normalized == "" {
		return fmt.Errorf("domain: derived claim statement normalizes to empty")
	}
	if value.Output.ResidentID != value.ResidentID || value.Output.Class != "generation_output" ||
		value.Output.ErasurePolicy != "independent" || len(value.Output.Bytes) == 0 {
		return fmt.Errorf("domain: invalid derived claim output content")
	}
	if err := value.Output.Validate(); err != nil {
		return err
	}
	for _, contentID := range []canonical.ID{value.Statement.ID, value.Output.ID} {
		if _, duplicate := seen[contentID]; duplicate {
			return fmt.Errorf("domain: duplicate derived claim identity %s", contentID)
		}
		seen[contentID] = struct{}{}
	}

	switch relation {
	case memory.RelationAbstracts:
		if len(value.Sources) < MinimumAbstractionSources || len(value.Sources) > MaximumAbstractionSources {
			return fmt.Errorf("domain: abstraction requires %d..%d source claims", MinimumAbstractionSources, MaximumAbstractionSources)
		}
	case memory.RelationSplitFrom:
		if len(value.Sources) != 1 {
			return fmt.Errorf("domain: differentiation requires exactly one source claim")
		}
	default:
		return fmt.Errorf("domain: unsupported derived claim relation %q", relation)
	}

	sourceIDs := make(map[canonical.ID]struct{}, len(value.Sources))
	for _, source := range value.Sources {
		for _, id := range []canonical.ID{source.ClaimID, source.RelationID} {
			if err := id.Validate(); err != nil {
				return err
			}
			if _, duplicate := seen[id]; duplicate {
				return fmt.Errorf("domain: duplicate derived claim identity %s", id)
			}
			seen[id] = struct{}{}
		}
		if source.ClaimID == value.ClaimID {
			return fmt.Errorf("domain: derived claim cannot source itself")
		}
		sourceIDs[source.ClaimID] = struct{}{}
	}
	if len(value.Evidence) == 0 || len(value.Evidence) > MaximumInheritedEvidence {
		return fmt.Errorf("domain: derived claim requires 1..%d inherited evidence rows", MaximumInheritedEvidence)
	}
	perSource := make(map[canonical.ID]int)
	for _, inherited := range value.Evidence {
		for _, id := range []canonical.ID{inherited.EvidenceID, inherited.SourceEvidenceID} {
			if err := id.Validate(); err != nil {
				return err
			}
			if _, duplicate := seen[id]; duplicate {
				return fmt.Errorf("domain: duplicate derived claim identity %s", id)
			}
			seen[id] = struct{}{}
		}
		if _, declared := sourceIDs[inherited.SourceClaimID]; !declared {
			return fmt.Errorf("domain: inherited evidence source claim %s is not declared", inherited.SourceClaimID)
		}
		perSource[inherited.SourceClaimID]++
		if perSource[inherited.SourceClaimID] > 2 {
			return fmt.Errorf("domain: source claim %s exceeds inherited evidence limit", inherited.SourceClaimID)
		}
	}
	return nil
}
