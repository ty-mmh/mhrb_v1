package memory

import (
	"testing"

	"mahoroba.local/mahoroba/internal/canonical"
)

func TestM5I90CandidateUsageDoesNotIncreaseSalience(t *testing.T) {
	policy := DefaultPolicyV2()
	contribution, err := SalienceContribution(policy, UsageCandidate)
	if err != nil {
		t.Fatal(err)
	}
	if contribution.Millionths() != 0 {
		t.Fatalf("candidate contribution = %d, want 0", contribution.Millionths())
	}
	asOf := canonical.Instant(1_700_000_000_000_000)
	salience, err := CalculateSalience(policy, []UsageRecord{{
		Type: UsageCandidate, RecordedAt: asOf,
	}}, asOf)
	if err != nil {
		t.Fatal(err)
	}
	if salience.Millionths() != 0 {
		t.Fatalf("candidate-only salience = %d, want 0", salience.Millionths())
	}
}
