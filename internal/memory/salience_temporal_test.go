package memory

import (
	"errors"
	"testing"
	"time"

	"mahoroba.local/mahoroba/internal/canonical"
)

func TestSalienceContributionsDecayAtCompletedIntervals(t *testing.T) {
	policy := DefaultPolicyV2()
	for usage, want := range map[UsageType]int64{
		UsageCandidate: 0, UsageSelected: 50_000,
		UsagePromptIncluded: 250_000, UsageExplicitlyReferenced: 500_000,
	} {
		got, err := SalienceContribution(policy, usage)
		if err != nil {
			t.Fatalf("SalienceContribution(%q): %v", usage, err)
		}
		if got.Millionths() != want {
			t.Errorf("SalienceContribution(%q) = %d, want %d", usage, got.Millionths(), want)
		}
	}

	halfLife := (30 * 24 * time.Hour).Microseconds()
	asOf := canonical.Instant(10 * halfLife)
	justBefore := canonical.Instant(int64(asOf) - halfLife + 1)
	atBoundary := canonical.Instant(int64(asOf) - halfLife)
	result, err := AggregateSalience(policy, []SaliencePoint{
		{Contribution: mustWeight(t, 250_000), RecordedAt: justBefore},
		{Contribution: mustWeight(t, 250_000), RecordedAt: atBoundary},
	}, asOf)
	if err != nil {
		t.Fatalf("AggregateSalience: %v", err)
	}
	if got := result.Millionths(); got != 375_000 {
		t.Fatalf("salience = %d, want 375000", got)
	}

	result, err = CalculateSalience(policy, []UsageRecord{
		{Type: UsageExplicitlyReferenced, RecordedAt: asOf},
		{Type: UsageExplicitlyReferenced, RecordedAt: asOf},
		{Type: UsagePromptIncluded, RecordedAt: asOf},
	}, asOf)
	if err != nil {
		t.Fatalf("CalculateSalience(cap): %v", err)
	}
	if result.Millionths() != 1_000_000 {
		t.Fatalf("capped salience = %d, want 1000000", result.Millionths())
	}
	if _, err := AggregateSalience(policy, []SaliencePoint{{Contribution: mustWeight(t, 1), RecordedAt: asOf + 1}}, asOf); !errors.Is(err, ErrInvalidSalience) {
		t.Fatalf("future usage error = %v, want ErrInvalidSalience", err)
	}
}

func TestTemporalBoundariesAreInclusiveAndDeterministic(t *testing.T) {
	policy := DefaultPolicyV2()
	anchor := canonical.Instant(1_000_000)
	volatileBoundary := canonical.Instant(int64(anchor) + (30 * 24 * time.Hour).Microseconds())
	episodicBoundary := canonical.Instant(int64(anchor) + (24 * time.Hour).Microseconds())

	tests := []struct {
		name   string
		input  TemporalInput
		want   TemporalRelation
		factor int64
	}{
		{"volatile just before", TemporalInput{Kind: TemporalVolatile, AnchorAt: anchor, AsOf: volatileBoundary - 1}, RelationCurrent, 1_000_000},
		{"volatile at boundary", TemporalInput{Kind: TemporalVolatile, AnchorAt: anchor, AsOf: volatileBoundary}, RelationStaleUnknown, 400_000},
		{"episodic just before", TemporalInput{Kind: TemporalEpisodic, AnchorAt: anchor, AsOf: episodicBoundary - 1}, RelationCurrent, 1_000_000},
		{"episodic at boundary", TemporalInput{Kind: TemporalEpisodic, AnchorAt: anchor, AsOf: episodicBoundary}, RelationPast, 600_000},
		{"anchor in future", TemporalInput{Kind: TemporalStable, AnchorAt: anchor, AsOf: anchor - 1}, RelationFuture, 250_000},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got, err := EvaluateTemporal(policy, test.input)
			if err != nil {
				t.Fatalf("EvaluateTemporal: %v", err)
			}
			if got.Relation != test.want || got.Currentness.Millionths() != test.factor {
				t.Fatalf("state = %+v, want relation %q factor %d", got, test.want, test.factor)
			}
		})
	}

	validFrom := canonical.Instant(anchor + 10)
	validTo := canonical.Instant(anchor + 20)
	for _, test := range []struct {
		asOf canonical.Instant
		want TemporalRelation
	}{
		{validFrom - 1, RelationFuture},
		{validFrom, RelationCurrent},
		{validTo - 1, RelationCurrent},
		{validTo, RelationPast},
	} {
		got, err := EvaluateTemporal(policy, TemporalInput{
			Kind: TemporalStable, AnchorAt: anchor, ValidFrom: &validFrom, ValidTo: &validTo, AsOf: test.asOf,
		})
		if err != nil || got.Relation != test.want {
			t.Fatalf("bounded as_of %d = %+v, %v; want %q", test.asOf, got, err, test.want)
		}
	}

	badFrom, badTo := canonical.Instant(2), canonical.Instant(1)
	if _, err := EvaluateTemporal(policy, TemporalInput{Kind: TemporalStable, ValidFrom: &badFrom, ValidTo: &badTo}); !errors.Is(err, ErrInvalidTemporal) {
		t.Fatalf("reversed bounds error = %v, want ErrInvalidTemporal", err)
	}
}
