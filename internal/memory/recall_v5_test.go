package memory

import (
	"math"
	"testing"

	"mahoroba.local/mahoroba/internal/canonical"
)

func TestRecallV5JapaneseQueryAndEmptyInput(t *testing.T) {
	for _, test := range []struct {
		query, statement string
		want             canonical.Ratio
	}{
		{"東京の天気", "東京の天気", 1_000_000},
		{"東京", "大阪", 0},
		{"", "東京", 0},
		{"！？", "東京", 0},
		{"猫", "猫", 1_000_000},
		{"猫", "猫が好き", 400_000},
		{"猫", "犬が好き", 0},
		{"TEA", "tea", 1_000_000},
	} {
		query := NewRecallQueryV5(test.query)
		for range 3 {
			if got := query.Compatibility(test.statement); got != test.want {
				t.Fatalf("%q / %q = %d, want %d", test.query, test.statement, got, test.want)
			}
		}
	}
	if score := NewRecallQueryV5("東京の天気").Compatibility("東京は晴れ"); score <= 0 || score >= 1_000_000 {
		t.Fatalf("partial Japanese match = %d", score)
	}
}

func TestRecallV5OldSalienceCannotBeatRelevantMemoryAndTiesPreferNewer(t *testing.T) {
	old, fresh := recallCandidate(t, 0), recallCandidate(t, 1)
	old.ContextCompatibility, old.Salience = 0, 1_000_000
	fresh.ContextCompatibility, fresh.Salience, fresh.Confidence = 1_000_000, 0, 375_000
	selection, err := SelectRecall(DefaultPolicyV5(), []RecallCandidate{old, fresh})
	if err != nil || len(selection.Selected) != 1 || selection.Selected[0].Candidate.ClaimID != fresh.ClaimID {
		t.Fatalf("unrelated reinforced claim selected: %+v / %v", selection, err)
	}
	old.ContextCompatibility, old.Confidence = fresh.ContextCompatibility, fresh.Confidence
	selection, err = SelectRecall(DefaultPolicyV5(), []RecallCandidate{old, fresh})
	if err != nil || selection.Selected[0].Candidate.ClaimID != fresh.ClaimID {
		t.Fatalf("V5 tie did not prefer newer claim: %+v / %v", selection, err)
	}
	old.Salience = 0
	legacy, err := SelectRecall(DefaultPolicyV4(), []RecallCandidate{old, fresh})
	if err != nil || legacy.Selected[0].Candidate.ClaimID != old.ClaimID {
		t.Fatalf("V4 tie behavior changed: %+v / %v", legacy, err)
	}
}

func TestMemoryPolicyV5RoundTripAndSelfReinforcementDisabled(t *testing.T) {
	policy := DefaultPolicyV5()
	encoded, err := policy.CanonicalJSON()
	if err != nil {
		t.Fatal(err)
	}
	parsed, reencoded, err := ParsePolicy(encoded.Bytes())
	if err != nil || parsed.Version != PolicyVersionV5 || encoded.String() != reencoded.String() {
		t.Fatalf("V5 round trip: %+v / %v", parsed, err)
	}
	for _, usage := range []UsageType{UsageCandidate, UsageSelected, UsagePromptIncluded} {
		value, err := SalienceContribution(policy, usage)
		if err != nil || value != 0 {
			t.Fatalf("Recall usage %s contribution=%d err=%v", usage, value, err)
		}
	}
	if policy.RenderingVersion != RenderingVersionV2 {
		t.Fatal("V5 changed renderer")
	}
}

func TestConfidenceV5EvidenceAmountContradictionAndLegacyReplay(t *testing.T) {
	for _, test := range []struct {
		name                string
		support, contradict int64
		want                int64
	}{
		{"none", 0, 0, 0},
		{"single inferred", 600_000, 0, 375_000},
		{"single stated", 1_000_000, 0, 500_000},
		{"two stated", 2_000_000, 0, 666_666},
		{"settled confidence boundary", 3_000_000, 0, 750_000},
		{"contradict only", 0, 1_000_000, 0},
		{"contradiction", 3_000_000, 1_000_000, 600_000},
	} {
		t.Run(test.name, func(t *testing.T) {
			points := []EvaluatedEvidence{}
			for index, value := range []int64{test.support, test.contradict} {
				if value == 0 {
					continue
				}
				polarity := PolaritySupport
				if index == 1 {
					polarity = PolarityContradict
				}
				points = append(points, EvaluatedEvidence{EvidencePoint: EvidencePoint{SourceEventID: recallCandidate(t, index).ClaimID, Polarity: polarity}, EffectiveWeight: canonical.Weight(value)})
			}
			got, err := AggregateEvidence(DefaultPolicyV5(), points)
			if err != nil || got.Confidence.Millionths() != test.want {
				t.Fatalf("V5 confidence=%d want=%d err=%v", got.Confidence, test.want, err)
			}
			if test.support > 0 && test.contradict == 0 {
				old, err := AggregateEvidence(DefaultPolicyV4(), points)
				if err != nil || old.Confidence != 1_000_000 {
					t.Fatalf("legacy confidence changed: %+v / %v", old, err)
				}
			}
		})
	}
	point := EvaluatedEvidence{EvidencePoint: EvidencePoint{SourceEventID: recallCandidate(t, 0).ClaimID, Polarity: PolaritySupport}, EffectiveWeight: canonical.Weight(math.MaxInt64)}
	if _, err := AggregateEvidence(DefaultPolicyV5(), []EvaluatedEvidence{point}); err == nil {
		t.Fatal("confidence denominator overflow accepted")
	}
}
