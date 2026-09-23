package memory

import (
	"errors"
	"strings"
	"testing"
	"time"

	"mahoroba.local/mahoroba/internal/canonical"
)

func recallCandidate(t *testing.T, index int) RecallCandidate {
	t.Helper()
	return RecallCandidate{
		ClaimID: testID(t, index), Statement: "claim " + testID(t, index).String(),
		ContextCompatibility: mustRatio(t, 1_000_000), Salience: mustRatio(t, 1_000_000),
		Confidence: mustRatio(t, 1_000_000), Status: StatusActive, Stage: StageSettled,
		TemporalRelation: RelationCurrent, SourceEventIDs: []canonical.ID{testID(t, index+1)},
	}
}

func TestRecallScoringUsesActiveStateAndFixedPointTemporalFactors(t *testing.T) {
	policy := DefaultPolicyV2()
	candidate := recallCandidate(t, 0)
	scored, err := ScoreRecall(policy, candidate, 0)
	if err != nil {
		t.Fatalf("ScoreRecall(settled): %v", err)
	}
	if !scored.Eligible || scored.Score.Millionths() != 1_000_000 {
		t.Fatalf("settled score = %+v", scored)
	}

	candidate.Stage = StageFloating
	scored, err = ScoreRecall(policy, candidate, 0)
	if err != nil {
		t.Fatalf("ScoreRecall(floating): %v", err)
	}
	if scored.StateFactor.Millionths() != 1_000_000 || scored.Score.Millionths() != 1_000_000 {
		t.Fatalf("floating score = state %d score %d", scored.StateFactor.Millionths(), scored.Score.Millionths())
	}

	candidate.Status = StatusInvalidated
	scored, err = ScoreRecall(policy, candidate, 0)
	if err != nil {
		t.Fatalf("ScoreRecall(inactive): %v", err)
	}
	if scored.Eligible || scored.StateFactor.Millionths() != 0 || scored.Score.Millionths() != 0 {
		t.Fatalf("inactive score = %+v", scored)
	}
}

func TestRecallSelectionStableTieBreakAndLimits(t *testing.T) {
	policy := DefaultPolicyV2()
	first, second := recallCandidate(t, 1), recallCandidate(t, 0)
	selection, err := SelectRecall(policy, []RecallCandidate{first, second})
	if err != nil {
		t.Fatalf("SelectRecall: %v", err)
	}
	if selection.Selected[0].Candidate.ClaimID != second.ClaimID || selection.Selected[1].Candidate.ClaimID != first.ClaimID {
		t.Fatalf("tie order = %s, %s", selection.Selected[0].Candidate.ClaimID, selection.Selected[1].Candidate.ClaimID)
	}
	if selection.Candidates[0].CandidateOrdinal.Int64() != 0 || selection.Candidates[1].CandidateOrdinal.Int64() != 1 {
		t.Fatal("audit candidate ordinals did not preserve adapter order")
	}
	if _, err := SelectRecall(policy, []RecallCandidate{first, first}); !errors.Is(err, ErrInvalidRecall) {
		t.Fatalf("duplicate candidate error = %v, want ErrInvalidRecall", err)
	}
}

func TestRecallPlanProvenanceRenderingCountAndBudget(t *testing.T) {
	policy := DefaultPolicyV2()
	candidates := make([]RecallCandidate, 5)
	for index := range candidates {
		candidates[index] = recallCandidate(t, index)
	}
	candidates[0].Stage = StageFloating
	candidates[0].Abstract = false
	candidates[1].TemporalRelation = RelationStaleUnknown
	selection, err := SelectRecall(policy, candidates)
	if err != nil {
		t.Fatalf("SelectRecall: %v", err)
	}
	plan, err := PlanRecall(policy, selection, candidates[0].SourceEventIDs)
	if err != nil {
		t.Fatalf("PlanRecall: %v", err)
	}
	if len(plan.Decisions) != 5 || len(plan.Prompt) != 4 {
		t.Fatalf("plan sizes = decisions %d prompt %d", len(plan.Decisions), len(plan.Prompt))
	}
	if plan.Decisions[0].ExclusionReason != ExclusionProvenanceDuplicate || plan.Decisions[0].PromptIncluded {
		t.Fatalf("floating duplicate decision = %+v", plan.Decisions[0])
	}
	if !strings.Contains(plan.Decisions[1].RenderedText, `"temporal_relation":"stale_unknown"`) {
		t.Fatalf("stale rendering = %q", plan.Decisions[1].RenderedText)
	}
	var actualBytes int
	for index, decision := range plan.Prompt {
		if !decision.PromptIncluded || decision.PromptOrdinal == nil || decision.PromptOrdinal.Int64() != int64(index) {
			t.Fatalf("prompt decision %d = %+v", index, decision)
		}
		actualBytes += len([]byte(decision.RenderedText))
	}
	if plan.RenderedBytes.Int64() != int64(actualBytes) {
		t.Fatalf("rendered bytes = %d, actual %d", plan.RenderedBytes.Int64(), actualBytes)
	}

	oversized := recallCandidate(t, 6)
	oversized.Statement = strings.Repeat("x", int(policy.Recall.ByteBudget))
	oversizedSelection, err := SelectRecall(policy, []RecallCandidate{oversized})
	if err != nil {
		t.Fatalf("SelectRecall(oversized): %v", err)
	}
	budgetPlan, err := PlanRecall(policy, oversizedSelection, nil)
	if err != nil {
		t.Fatalf("PlanRecall(oversized): %v", err)
	}
	if len(budgetPlan.Prompt) != 0 || budgetPlan.Decisions[0].ExclusionReason != ExclusionTokenBudget {
		t.Fatalf("budget decision = %+v", budgetPlan.Decisions[0])
	}

	duplicateSelection := selection
	duplicateSelection.Selected = []ScoredRecallCandidate{selection.Selected[0], selection.Selected[0]}
	if _, err := PlanRecall(policy, duplicateSelection, nil); !errors.Is(err, ErrInvalidRecall) {
		t.Fatalf("duplicate selected error = %v, want ErrInvalidRecall", err)
	}
}

func TestRecallPlanExcludesAnySourceOverlapRegardlessOfStageOrAbstractness(t *testing.T) {
	policy := DefaultPolicyV2()
	firstSource := testID(t, 8)
	secondSource := testID(t, 9)
	tests := []struct {
		name     string
		stage    ClaimStage
		abstract bool
	}{
		{name: "floating concrete", stage: StageFloating},
		{name: "sediment concrete", stage: StageSediment},
		{name: "settled concrete", stage: StageSettled},
		{name: "settled abstract", stage: StageSettled, abstract: true},
	}
	for index, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			candidate := recallCandidate(t, index)
			candidate.Stage = test.stage
			candidate.Abstract = test.abstract
			candidate.SourceEventIDs = []canonical.ID{firstSource, secondSource}
			selection, err := SelectRecall(policy, []RecallCandidate{candidate})
			if err != nil {
				t.Fatalf("SelectRecall: %v", err)
			}
			plan, err := PlanRecall(policy, selection, []canonical.ID{secondSource})
			if err != nil {
				t.Fatalf("PlanRecall(partial overlap): %v", err)
			}
			if len(plan.Decisions) != 1 || !plan.Decisions[0].Selected ||
				plan.Decisions[0].PromptIncluded ||
				plan.Decisions[0].ExclusionReason != ExclusionProvenanceDuplicate ||
				len(plan.Prompt) != 0 {
				t.Fatalf("partial-overlap plan = %+v", plan)
			}
		})
	}

	candidate := recallCandidate(t, 4)
	candidate.Stage = StageSettled
	candidate.Abstract = true
	candidate.SourceEventIDs = []canonical.ID{firstSource, secondSource}
	selection, err := SelectRecall(policy, []RecallCandidate{candidate})
	if err != nil {
		t.Fatalf("SelectRecall(no overlap): %v", err)
	}
	plan, err := PlanRecall(policy, selection, []canonical.ID{testID(t, 10)})
	if err != nil {
		t.Fatalf("PlanRecall(no overlap): %v", err)
	}
	if len(plan.Prompt) != 1 || !plan.Decisions[0].PromptIncluded || plan.Decisions[0].ExclusionReason != "" {
		t.Fatalf("non-overlap plan = %+v", plan)
	}
}

func TestRecallExclusionReasonVersionBoundaries(t *testing.T) {
	if ExclusionProvenanceDuplicate != ExclusionProvenanceDuplicateV1 {
		t.Fatal("production provenance reason alias no longer targets context v1")
	}
	if ExclusionProvenanceDuplicateV1 != "source_event_already_in_context" {
		t.Fatalf("v1 provenance reason = %q", ExclusionProvenanceDuplicateV1)
	}
	if ExclusionProvenanceDuplicateV2 != "provenance_duplicate" {
		t.Fatalf("v2 provenance reason = %q", ExclusionProvenanceDuplicateV2)
	}
	tests := []struct {
		name   string
		reason RecallExclusionReason
		v1     bool
		v2     bool
	}{
		{name: "v1 provenance", reason: ExclusionProvenanceDuplicateV1, v1: true},
		{name: "v2 provenance", reason: ExclusionProvenanceDuplicateV2, v2: true},
		{name: "token budget", reason: ExclusionTokenBudget, v1: true, v2: true},
		{name: "policy filter", reason: ExclusionPolicyFilter, v1: true, v2: true},
		{name: "unknown", reason: RecallExclusionReason("unknown")},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if got := test.reason.ValidateV1() == nil; got != test.v1 {
				t.Fatalf("ValidateV1 success = %v, want %v", got, test.v1)
			}
			if got := test.reason.ValidateV2() == nil; got != test.v2 {
				t.Fatalf("ValidateV2 success = %v, want %v", got, test.v2)
			}
			if got := test.reason.Validate() == nil; got != test.v1 {
				t.Fatalf("production Validate success = %v, want v1 result %v", got, test.v1)
			}
		})
	}
}

func TestRecallPlanV2LimitsProvenanceDeduplication(t *testing.T) {
	policy := DefaultPolicyV2()
	firstSource := testID(t, 10)
	secondSource := testID(t, 11)
	tests := []struct {
		name         string
		stage        ClaimStage
		abstract     bool
		included     []canonical.ID
		wantExcluded bool
	}{
		{
			name: "floating concrete all sources", stage: StageFloating,
			included: []canonical.ID{firstSource, secondSource}, wantExcluded: true,
		},
		{
			name: "floating concrete partial overlap", stage: StageFloating,
			included: []canonical.ID{secondSource},
		},
		{
			name: "floating concrete no overlap", stage: StageFloating,
			included: []canonical.ID{testID(t, 9)},
		},
		{
			name: "sediment concrete all sources", stage: StageSediment,
			included: []canonical.ID{firstSource, secondSource},
		},
		{
			name: "settled concrete all sources", stage: StageSettled,
			included: []canonical.ID{firstSource, secondSource},
		},
		{
			name: "floating abstract all sources", stage: StageFloating, abstract: true,
			included: []canonical.ID{firstSource, secondSource},
		},
	}
	for index, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			candidate := recallCandidate(t, index)
			candidate.Stage = test.stage
			candidate.Abstract = test.abstract
			candidate.SourceEventIDs = []canonical.ID{firstSource, secondSource}
			selection, err := SelectRecall(policy, []RecallCandidate{candidate})
			if err != nil {
				t.Fatalf("SelectRecall: %v", err)
			}
			plan, err := PlanRecallV2(policy, selection, test.included)
			if err != nil {
				t.Fatalf("PlanRecallV2: %v", err)
			}
			if len(plan.Decisions) != 1 {
				t.Fatalf("decisions = %d, want 1", len(plan.Decisions))
			}
			decision := plan.Decisions[0]
			if !decision.Selected || decision.Candidate.CandidateOrdinal.Int64() != 0 ||
				decision.SelectedOrdinal == nil || decision.SelectedOrdinal.Int64() != 0 {
				t.Fatalf("candidate/selected audit state = %+v", decision)
			}
			if test.wantExcluded {
				if decision.PromptIncluded || decision.PromptOrdinal != nil ||
					decision.ExclusionReason != ExclusionProvenanceDuplicateV2 || len(plan.Prompt) != 0 {
					t.Fatalf("deduplicated decision = %+v, prompt = %+v", decision, plan.Prompt)
				}
				return
			}
			if !decision.PromptIncluded || decision.PromptOrdinal == nil ||
				decision.PromptOrdinal.Int64() != 0 || decision.ExclusionReason != "" || len(plan.Prompt) != 1 {
				t.Fatalf("included decision = %+v, prompt = %+v", decision, plan.Prompt)
			}
		})
	}
}

func TestRecallPlanV2RejectsInvalidSourceProvenance(t *testing.T) {
	policy := DefaultPolicyV2()
	source := testID(t, 10)
	tests := []struct {
		name     string
		sources  []canonical.ID
		selected bool
	}{
		{name: "empty selected candidate", selected: true},
		{name: "duplicate source", sources: []canonical.ID{source, source}, selected: true},
		{name: "invalid source", sources: []canonical.ID{{}}, selected: true},
		{name: "empty non-selected candidate"},
	}
	for index, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			candidate := recallCandidate(t, index)
			candidate.SourceEventIDs = test.sources
			if !test.selected {
				candidate.Status = StatusInvalidated
			}
			selection, err := SelectRecall(policy, []RecallCandidate{candidate})
			if err != nil {
				t.Fatalf("SelectRecall: %v", err)
			}
			if _, err := PlanRecallV2(policy, selection, nil); !errors.Is(err, ErrInvalidRecall) {
				t.Fatalf("PlanRecallV2 error = %v, want ErrInvalidRecall", err)
			}
		})
	}
}

func TestCOV3RecallPlanV2ReevaluatesDedupAfterContextBudgetDropsSource(t *testing.T) {
	policy := DefaultPolicyV2()
	source := testID(t, 10)
	candidate := recallCandidate(t, 0)
	candidate.Stage = StageFloating
	candidate.Abstract = false
	candidate.SourceEventIDs = []canonical.ID{source}
	selection, err := SelectRecall(policy, []RecallCandidate{candidate})
	if err != nil {
		t.Fatal(err)
	}

	withSource, err := PlanRecallV2(policy, selection, []canonical.ID{source})
	if err != nil {
		t.Fatal(err)
	}
	if len(withSource.Decisions) != 1 || withSource.Decisions[0].ExclusionReason != ExclusionProvenanceDuplicateV2 {
		t.Fatalf("initial dedup plan = %+v", withSource)
	}

	withoutSource, err := PlanRecallV2(policy, selection, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(withoutSource.Prompt) != 1 || !withoutSource.Decisions[0].PromptIncluded ||
		withoutSource.Decisions[0].Candidate.CandidateOrdinal != withSource.Decisions[0].Candidate.CandidateOrdinal ||
		*withoutSource.Decisions[0].SelectedOrdinal != *withSource.Decisions[0].SelectedOrdinal {
		t.Fatalf("replanned context result = %+v", withoutSource)
	}

}

func TestPersonaThresholdUsesInclusiveLimitsAndCooldownBoundary(t *testing.T) {
	policy := DefaultPolicyV2()
	asOf := canonical.Instant((48 * time.Hour).Microseconds())
	last := canonical.Instant(int64(asOf) - (24 * time.Hour).Microseconds())
	input := PersonaThresholdInput{
		ClaimCount: mustCount(t, 2), ChangedBytes: mustByteSize(t, 256),
		ChangedRatio: mustRatio(t, 100_000), ChangedLines: mustCount(t, 3),
		TotalBytes: mustByteSize(t, 8192), AsOf: asOf, LastActivationAt: &last,
	}
	decision, err := EvaluatePersonaThreshold(policy, input)
	if err != nil {
		t.Fatalf("EvaluatePersonaThreshold(boundary): %v", err)
	}
	if !decision.Eligible || len(decision.BlockingReasons) != 0 {
		t.Fatalf("boundary decision = %+v", decision)
	}

	input.ClaimCount = mustCount(t, 1)
	input.ChangedBytes = mustByteSize(t, 257)
	input.ChangedRatio = mustRatio(t, 100_001)
	input.ChangedLines = mustCount(t, 4)
	input.TotalBytes = mustByteSize(t, 8193)
	last = canonical.Instant(int64(asOf) - (24 * time.Hour).Microseconds() + 1)
	input.LastActivationAt = &last
	decision, err = EvaluatePersonaThreshold(policy, input)
	if err != nil {
		t.Fatalf("EvaluatePersonaThreshold(blocked): %v", err)
	}
	for _, want := range []PersonaBlockingReason{
		PersonaTooFewClaims, PersonaChangedBytes, PersonaChangedRatio,
		PersonaChangedLines, PersonaTotalBytes, PersonaActivationCooldown,
	} {
		found := false
		for _, got := range decision.BlockingReasons {
			found = found || got == want
		}
		if !found {
			t.Errorf("missing blocking reason %q in %v", want, decision.BlockingReasons)
		}
	}

	ratio, err := QuantizeChangedRatio(mustByteSize(t, 1), mustByteSize(t, 3))
	if err != nil || ratio.Millionths() != 333_333 {
		t.Fatalf("changed ratio = %d, %v; want 333333", ratio.Millionths(), err)
	}
	if _, err := QuantizeChangedRatio(mustByteSize(t, 2), mustByteSize(t, 1)); !errors.Is(err, ErrInvalidPersona) {
		t.Fatalf("overflow ratio error = %v, want ErrInvalidPersona", err)
	}
}
