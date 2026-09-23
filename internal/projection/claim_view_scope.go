package projection

import (
	"fmt"
	"slices"
	"strings"

	"mahoroba.local/mahoroba/internal/canonical"
)

func ClaimViewScopeCurrentDefinition() Definition {
	return Definition{Name: ClaimViewScopeCurrentName, Version: "claim-view-scope-current-v1"}
}

type ClaimViewScope string

const (
	ClaimViewScopeResidentUI ClaimViewScope = "resident_ui"
	ClaimViewScopeAdminOnly  ClaimViewScope = "admin_only"
)

func (value ClaimViewScope) Validate() error {
	switch value {
	case ClaimViewScopeResidentUI, ClaimViewScopeAdminOnly:
		return nil
	default:
		return fmt.Errorf("projection: invalid claim view scope %q", value)
	}
}

type ClaimViewScopeAssertion struct {
	AssertionID canonical.ID
	ClaimID     canonical.ID
	CommitSeq   canonical.CommitSeq
	RecordedAt  canonical.Instant
	ViewScope   ClaimViewScope
}

type ClaimViewScopeState struct {
	ClaimID           canonical.ID
	ViewScope         ClaimViewScope
	SourceAssertionID canonical.ID
}

// ReplayClaimViewScopes replays the last assertion in Canonical commit order.
// It is commit-sensitive, not wall-clock-sensitive: recorded_at is retained
// for provenance but does not gate whether an already captured assertion is
// current. Missing initial assertions fail closed rather than exposing a claim
// under an implicit default scope.
func ReplayClaimViewScopes(through canonical.CommitSeq, claims []ClaimSeed, assertions []ClaimViewScopeAssertion) ([]ClaimViewScopeState, error) {
	if err := through.Validate(); err != nil {
		return nil, err
	}
	capturedClaims := make(map[canonical.ID]struct{}, len(claims))
	orderedClaims := make([]canonical.ID, 0, len(claims))
	for _, claim := range claims {
		if err := validateClaimSeed(claim); err != nil {
			return nil, err
		}
		if claim.CommitSeq > through {
			continue
		}
		if _, duplicate := capturedClaims[claim.ClaimID]; duplicate {
			return nil, fmt.Errorf("projection: duplicate claim seed %s", claim.ClaimID)
		}
		capturedClaims[claim.ClaimID] = struct{}{}
		orderedClaims = append(orderedClaims, claim.ClaimID)
	}
	slices.SortFunc(orderedClaims, func(left, right canonical.ID) int {
		return strings.Compare(left.String(), right.String())
	})

	current := make(map[canonical.ID]ClaimViewScopeAssertion, len(capturedClaims))
	for _, assertion := range assertions {
		if err := assertion.AssertionID.Validate(); err != nil {
			return nil, fmt.Errorf("projection: invalid view-scope assertion id: %w", err)
		}
		if err := assertion.ClaimID.Validate(); err != nil {
			return nil, fmt.Errorf("projection: invalid view-scope claim id: %w", err)
		}
		if err := assertion.CommitSeq.Validate(); err != nil {
			return nil, err
		}
		if err := assertion.ViewScope.Validate(); err != nil {
			return nil, err
		}
		if assertion.CommitSeq > through {
			continue
		}
		if _, exists := capturedClaims[assertion.ClaimID]; !exists {
			return nil, fmt.Errorf("projection: view-scope assertion %s references uncaptured claim %s", assertion.AssertionID, assertion.ClaimID)
		}
		previous, exists := current[assertion.ClaimID]
		if !exists || compareCommitAndID(previous.CommitSeq, previous.AssertionID, assertion.CommitSeq, assertion.AssertionID) < 0 {
			current[assertion.ClaimID] = assertion
		}
	}

	result := make([]ClaimViewScopeState, 0, len(orderedClaims))
	for _, claimID := range orderedClaims {
		assertion, exists := current[claimID]
		if !exists {
			return nil, fmt.Errorf("projection: claim %s has no captured view-scope assertion", claimID)
		}
		result = append(result, ClaimViewScopeState{
			ClaimID: claimID, ViewScope: assertion.ViewScope, SourceAssertionID: assertion.AssertionID,
		})
	}
	return result, nil
}
