package domain

import (
	"testing"

	"mahoroba.local/mahoroba/internal/canonical"
)

func TestRecallCandidateSnapshotValidatesStructuredRenderingState(t *testing.T) {
	confirmed := canonical.Instant(1_725_081_234_567_890)
	candidate := RecallCandidateSnapshot{
		ClaimID:              mustParseID(t, "01ARZ3NDEKTSV4RRFFQ69G5FE0"),
		Statement:            "structured memory",
		ContextCompatibility: canonical.Ratio(1_000_000),
		Salience:             canonical.Ratio(500_000),
		Confidence:           canonical.Ratio(750_000),
		Currentness:          canonical.Ratio(400_000),
		LastConfirmed:        &confirmed,
		Status:               "active",
		Stage:                "floating",
		TemporalRelation:     "stale_unknown",
		SourceEventIDs: []canonical.ID{
			mustParseID(t, "01ARZ3NDEKTSV4RRFFQ69G5FE1"),
		},
	}
	if err := candidate.Validate(); err != nil {
		t.Fatalf("Validate: %v", err)
	}
	candidate.Currentness = canonical.Ratio(canonical.FixedPointScale + 1)
	if err := candidate.Validate(); err == nil {
		t.Fatal("out-of-range currentness passed validation")
	}
}
