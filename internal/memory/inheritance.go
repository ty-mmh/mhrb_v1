package memory

import (
	"fmt"

	"mahoroba.local/mahoroba/internal/canonical"
)

type InheritedEvidenceCandidate struct {
	TargetClaimID    canonical.ID
	SourceClaimID    canonical.ID
	SourceEvidenceID canonical.ID
	SourceEventID    canonical.ID
	Relation         RelationType
}

// ValidateInheritanceBatch applies the v0 laundering and fan-out limits before
// a landing UoW is built. Every inherited row still carries its direct source
// event ID; callers must additionally verify that it equals the source
// evidence's own event ID when loading Canonical provenance.
func ValidateInheritanceBatch(policy Policy, candidates []InheritedEvidenceCandidate) error {
	if err := policy.RequireEnabled(); err != nil {
		return err
	}
	type claimPair struct {
		target canonical.ID
		source canonical.ID
	}
	perTarget := make(map[canonical.ID]int64)
	perSource := make(map[claimPair]int64)
	underlying := make(map[canonical.ID]map[canonical.ID]struct{})
	for index, candidate := range candidates {
		identifiers := []struct {
			label string
			value canonical.ID
		}{
			{label: "target claim", value: candidate.TargetClaimID},
			{label: "source claim", value: candidate.SourceClaimID},
			{label: "source evidence", value: candidate.SourceEvidenceID},
			{label: "source event", value: candidate.SourceEventID},
		}
		for _, identifier := range identifiers {
			if err := identifier.value.Validate(); err != nil {
				return fmt.Errorf("%w: inherited candidate %d %s: %v", ErrInvalidEvidence, index, identifier.label, err)
			}
		}
		if candidate.TargetClaimID == candidate.SourceClaimID {
			return fmt.Errorf("%w: inherited candidate %d is self-derived", ErrInvalidEvidence, index)
		}
		if err := candidate.Relation.Validate(); err != nil {
			return fmt.Errorf("%w: inherited candidate %d: %v", ErrInvalidEvidence, index, err)
		}
		if candidate.Relation != RelationAbstracts && candidate.Relation != RelationSplitFrom {
			return fmt.Errorf("%w: relation %q cannot inherit evidence", ErrInvalidEvidence, candidate.Relation)
		}
		perTarget[candidate.TargetClaimID]++
		if perTarget[candidate.TargetClaimID] > policy.Inheritance.MaxPerTarget {
			return fmt.Errorf("%w: target claim exceeds inherited evidence limit", ErrInvalidEvidence)
		}
		pair := claimPair{target: candidate.TargetClaimID, source: candidate.SourceClaimID}
		perSource[pair]++
		if perSource[pair] > policy.Inheritance.MaxPerSourceClaim {
			return fmt.Errorf("%w: source claim exceeds per-target inheritance limit", ErrInvalidEvidence)
		}
		if policy.Inheritance.DedupeUnderlyingEvent {
			if underlying[candidate.TargetClaimID] == nil {
				underlying[candidate.TargetClaimID] = make(map[canonical.ID]struct{})
			}
			if _, duplicate := underlying[candidate.TargetClaimID][candidate.SourceEventID]; duplicate {
				return fmt.Errorf("%w: duplicate underlying event for target claim", ErrInvalidEvidence)
			}
			underlying[candidate.TargetClaimID][candidate.SourceEventID] = struct{}{}
		}
	}
	return nil
}
