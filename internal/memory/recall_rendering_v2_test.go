package memory

import (
	"errors"
	"fmt"
	"testing"

	"mahoroba.local/mahoroba/internal/canonical"
)

func TestMemoryRenderingV1GoldenBytesRemainUnchanged(t *testing.T) {
	policy := DefaultPolicyV2()
	candidate := recallCandidate(t, 0)
	candidate.Currentness = mustRatio(t, 321_000)
	confirmed := canonical.Instant(1_725_081_234_567_890)
	candidate.LastConfirmed = &confirmed

	got, err := RenderClaim(policy, candidate)
	if err != nil {
		t.Fatal(err)
	}
	want := fmt.Sprintf(
		`{"statement":"claim %s","temporal_relation":"current","version":"memory-rendering-v1"}`,
		candidate.ClaimID,
	)
	if got != want {
		t.Fatalf("v1 render = %s\nwant      = %s", got, want)
	}
}

func TestMemoryRenderingV2ExactJCSAndCertaintyBands(t *testing.T) {
	policy := DefaultPolicyV4()
	tests := []struct {
		confidence int64
		certainty  string
	}{
		{confidence: 0, certainty: "low"},
		{confidence: 499_999, certainty: "low"},
		{confidence: 500_000, certainty: "medium"},
		{confidence: 749_999, certainty: "medium"},
		{confidence: 750_000, certainty: "high"},
		{confidence: 1_000_000, certainty: "high"},
	}
	for _, test := range tests {
		t.Run(fmt.Sprintf("%d", test.confidence), func(t *testing.T) {
			candidate := recallCandidate(t, 1)
			candidate.Confidence = mustRatio(t, test.confidence)
			candidate.Currentness = mustRatio(t, 400_000)

			got, err := RenderClaim(policy, candidate)
			if err != nil {
				t.Fatal(err)
			}
			want := fmt.Sprintf(
				`{"certainty":"%s","currentness":"400000","last_confirmed":null,"statement":"claim %s","temporal_relation":"current","version":"memory-rendering-v2"}`,
				test.certainty, candidate.ClaimID,
			)
			if got != want {
				t.Fatalf("v2 render = %s\nwant      = %s", got, want)
			}
		})
	}
}

func TestMemoryRenderingV2CurrentnessLastConfirmedAndTemporalRelations(t *testing.T) {
	policy := DefaultPolicyV4()
	tests := []struct {
		name        string
		currentness int64
		relation    TemporalRelation
		confirmed   *canonical.Instant
		wantConfirm string
	}{
		{name: "future zero", currentness: 0, relation: RelationFuture, wantConfirm: "null"},
		{name: "current maximum", currentness: 1_000_000, relation: RelationCurrent, wantConfirm: "null"},
		{name: "past confirmed", currentness: 600_000, relation: RelationPast,
			confirmed: instantPointer(1_725_081_234_567_890), wantConfirm: `"1725081234567890"`},
		{name: "stale confirmed", currentness: 400_000, relation: RelationStaleUnknown,
			confirmed: instantPointer(0), wantConfirm: `"0"`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			candidate := recallCandidate(t, 2)
			candidate.Confidence = mustRatio(t, 750_000)
			candidate.Currentness = mustRatio(t, test.currentness)
			candidate.TemporalRelation = test.relation
			candidate.LastConfirmed = test.confirmed

			got, err := RenderClaim(policy, candidate)
			if err != nil {
				t.Fatal(err)
			}
			want := fmt.Sprintf(
				`{"certainty":"high","currentness":"%d","last_confirmed":%s,"statement":"claim %s","temporal_relation":"%s","version":"memory-rendering-v2"}`,
				test.currentness, test.wantConfirm, candidate.ClaimID, test.relation,
			)
			if got != want {
				t.Fatalf("v2 render = %s\nwant      = %s", got, want)
			}
		})
	}
}

func TestMemoryRenderingV2DoesNotChangeRecallSelection(t *testing.T) {
	policy := DefaultPolicyV4()
	first := recallCandidate(t, 3)
	first.Currentness = mustRatio(t, 0)
	first.LastConfirmed = instantPointer(1)
	second := first
	second.Currentness = mustRatio(t, 1_000_000)
	second.LastConfirmed = instantPointer(9_999_999)

	left, err := ScoreRecall(policy, first, 0)
	if err != nil {
		t.Fatal(err)
	}
	right, err := ScoreRecall(policy, second, 0)
	if err != nil {
		t.Fatal(err)
	}
	if left.Score != right.Score || left.TemporalFactor != right.TemporalFactor || left.Eligible != right.Eligible {
		t.Fatalf("structured rendering fields changed selection: left=%+v right=%+v", left, right)
	}
}

func TestMemoryRenderingV2RejectsInvalidDisplayRatios(t *testing.T) {
	policy := DefaultPolicyV4()
	tests := []struct {
		name   string
		mutate func(*RecallCandidate)
	}{
		{name: "confidence", mutate: func(candidate *RecallCandidate) {
			candidate.Confidence = canonical.Ratio(canonical.FixedPointScale + 1)
		}},
		{name: "currentness", mutate: func(candidate *RecallCandidate) {
			candidate.Currentness = canonical.Ratio(canonical.FixedPointScale + 1)
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			candidate := recallCandidate(t, 4)
			test.mutate(&candidate)
			if _, err := RenderClaim(policy, candidate); !errors.Is(err, ErrInvalidRecall) {
				t.Fatalf("RenderClaim error = %v, want ErrInvalidRecall", err)
			}
		})
	}
}

func instantPointer(value canonical.Instant) *canonical.Instant {
	copyValue := value
	return &copyValue
}
