package memory

import (
	"bytes"
	"encoding/json"
	"fmt"
	"sort"
	"unicode/utf8"

	"mahoroba.local/mahoroba/internal/canonical"
)

type RecallCandidate struct {
	ClaimID              canonical.ID
	Statement            string
	ContextCompatibility canonical.Ratio
	Salience             canonical.Ratio
	Confidence           canonical.Ratio
	Currentness          canonical.Ratio
	LastConfirmed        *canonical.Instant
	Status               ClaimStatus
	Stage                ClaimStage
	TemporalRelation     TemporalRelation
	SourceEventIDs       []canonical.ID
	Abstract             bool
}

type ScoredRecallCandidate struct {
	Candidate        RecallCandidate
	CandidateOrdinal canonical.Ordinal
	Score            canonical.Ratio
	StateFactor      canonical.Ratio
	TemporalFactor   canonical.Ratio
	Eligible         bool
}

type RecallSelection struct {
	Candidates []ScoredRecallCandidate
	Selected   []ScoredRecallCandidate
}

type RecallDecision struct {
	Candidate       ScoredRecallCandidate
	Selected        bool
	SelectedOrdinal *canonical.Ordinal
	PromptIncluded  bool
	PromptOrdinal   *canonical.Ordinal
	RenderedText    string
	ExclusionReason RecallExclusionReason
}

type RecallPlan struct {
	Decisions     []RecallDecision
	Prompt        []RecallDecision
	RenderedBytes canonical.ByteSize
}

// ScoreRecall applies the fixed v0 weighted sum. Non-active claims are valid
// audit candidates but are never selection-eligible.
func ScoreRecall(policy Policy, candidate RecallCandidate, ordinal canonical.Ordinal) (ScoredRecallCandidate, error) {
	if err := policy.RequireEnabled(); err != nil {
		return ScoredRecallCandidate{}, err
	}
	if err := candidate.ClaimID.Validate(); err != nil {
		return ScoredRecallCandidate{}, fmt.Errorf("%w: claim ID: %v", ErrInvalidRecall, err)
	}
	if !utf8.ValidString(candidate.Statement) || candidate.Statement == "" {
		return ScoredRecallCandidate{}, fmt.Errorf("%w: statement must be non-empty UTF-8", ErrInvalidRecall)
	}
	if err := candidate.ContextCompatibility.Validate(); err != nil {
		return ScoredRecallCandidate{}, fmt.Errorf("%w: context compatibility: %v", ErrInvalidRecall, err)
	}
	if err := candidate.Salience.Validate(); err != nil {
		return ScoredRecallCandidate{}, fmt.Errorf("%w: salience: %v", ErrInvalidRecall, err)
	}
	if err := candidate.Confidence.Validate(); err != nil {
		return ScoredRecallCandidate{}, fmt.Errorf("%w: confidence: %v", ErrInvalidRecall, err)
	}
	if err := candidate.Currentness.Validate(); err != nil {
		return ScoredRecallCandidate{}, fmt.Errorf("%w: currentness: %v", ErrInvalidRecall, err)
	}
	if err := candidate.Status.Validate(); err != nil {
		return ScoredRecallCandidate{}, fmt.Errorf("%w: %v", ErrInvalidRecall, err)
	}
	if err := candidate.Stage.Validate(); err != nil {
		return ScoredRecallCandidate{}, fmt.Errorf("%w: %v", ErrInvalidRecall, err)
	}
	if err := candidate.TemporalRelation.Validate(); err != nil {
		return ScoredRecallCandidate{}, fmt.Errorf("%w: %v", ErrInvalidRecall, err)
	}
	if err := ordinal.Validate(); err != nil {
		return ScoredRecallCandidate{}, fmt.Errorf("%w: candidate ordinal: %v", ErrInvalidRecall, err)
	}

	// M5 selection admits only active claims. The fixed policy does not assign
	// an additional ranking preference to floating/sediment/settled, so the
	// state axis is one for an eligible claim and zero otherwise.
	stateFactor, _ := canonical.NewRatio(canonical.FixedPointScale)
	temporalFactor, err := CurrentnessForRelation(policy, candidate.TemporalRelation)
	if err != nil {
		return ScoredRecallCandidate{}, err
	}
	result := ScoredRecallCandidate{
		Candidate: candidate, CandidateOrdinal: ordinal,
		StateFactor: stateFactor, TemporalFactor: temporalFactor,
		Eligible: candidate.Status == StatusActive,
	}
	if !result.Eligible {
		result.StateFactor, _ = canonical.NewRatio(0)
		result.Score, _ = canonical.NewRatio(0)
		return result, nil
	}
	score, err := weightedRatioFloor(
		policy.Recall.Weights.Context, candidate.ContextCompatibility.Millionths(),
		policy.Recall.Weights.Salience, candidate.Salience.Millionths(),
		policy.Recall.Weights.Confidence, candidate.Confidence.Millionths(),
		policy.Recall.Weights.State, stateFactor.Millionths(),
		policy.Recall.Weights.Temporal, temporalFactor.Millionths(),
	)
	if err != nil {
		return ScoredRecallCandidate{}, fmt.Errorf("%w: score: %v", ErrInvalidRecall, err)
	}
	result.Score = score
	return result, nil
}

// SelectRecall records every adapter candidate in its original order and
// chooses at most selected_limit active candidates by score, with claim ID as
// the stable tie-breaker.
func SelectRecall(policy Policy, candidates []RecallCandidate) (RecallSelection, error) {
	if err := policy.RequireEnabled(); err != nil {
		return RecallSelection{}, err
	}
	if int64(len(candidates)) > policy.Recall.CandidateLimit {
		return RecallSelection{}, fmt.Errorf("%w: %d candidates exceeds limit %d", ErrInvalidRecall, len(candidates), policy.Recall.CandidateLimit)
	}
	result := RecallSelection{Candidates: make([]ScoredRecallCandidate, 0, len(candidates))}
	seen := make(map[canonical.ID]struct{}, len(candidates))
	for index, candidate := range candidates {
		if _, duplicate := seen[candidate.ClaimID]; duplicate {
			return RecallSelection{}, fmt.Errorf("%w: duplicate candidate claim %s", ErrInvalidRecall, candidate.ClaimID)
		}
		seen[candidate.ClaimID] = struct{}{}
		ordinal, _ := canonical.NewOrdinal(int64(index))
		scored, err := ScoreRecall(policy, candidate, ordinal)
		if err != nil {
			return RecallSelection{}, err
		}
		result.Candidates = append(result.Candidates, scored)
		if scored.Eligible {
			result.Selected = append(result.Selected, scored)
		}
	}
	sort.SliceStable(result.Selected, func(left, right int) bool {
		if result.Selected[left].Score != result.Selected[right].Score {
			return result.Selected[left].Score > result.Selected[right].Score
		}
		return result.Selected[left].Candidate.ClaimID.String() < result.Selected[right].Candidate.ClaimID.String()
	})
	if int64(len(result.Selected)) > policy.Recall.SelectedLimit {
		result.Selected = result.Selected[:policy.Recall.SelectedLimit]
	}
	return result, nil
}

// PlanRecall performs provenance deduplication, rendering, prompt count, and
// byte-budget enforcement after selection. Selected-but-excluded claims remain
// explicit decisions and therefore cannot be confused with non-selection.
func PlanRecall(policy Policy, selection RecallSelection, includedEventIDs []canonical.ID) (RecallPlan, error) {
	if err := policy.RequireEnabled(); err != nil {
		return RecallPlan{}, err
	}
	included := make(map[canonical.ID]struct{}, len(includedEventIDs))
	for _, id := range includedEventIDs {
		if err := id.Validate(); err != nil {
			return RecallPlan{}, fmt.Errorf("%w: included event ID: %v", ErrInvalidRecall, err)
		}
		included[id] = struct{}{}
	}
	plan := RecallPlan{Decisions: make([]RecallDecision, len(selection.Candidates))}
	decisionIndex := make(map[canonical.ID]int, len(selection.Candidates))
	for index, candidate := range selection.Candidates {
		if _, duplicate := decisionIndex[candidate.Candidate.ClaimID]; duplicate {
			return RecallPlan{}, fmt.Errorf("%w: duplicate audited candidate %s", ErrInvalidRecall, candidate.Candidate.ClaimID)
		}
		decisionIndex[candidate.Candidate.ClaimID] = index
		plan.Decisions[index] = RecallDecision{Candidate: candidate}
	}
	selectedClaims := make(map[canonical.ID]struct{}, len(selection.Selected))
	for rank, candidate := range selection.Selected {
		if _, duplicate := selectedClaims[candidate.Candidate.ClaimID]; duplicate {
			return RecallPlan{}, fmt.Errorf("%w: duplicate selected claim %s", ErrInvalidRecall, candidate.Candidate.ClaimID)
		}
		selectedClaims[candidate.Candidate.ClaimID] = struct{}{}
		index, present := decisionIndex[candidate.Candidate.ClaimID]
		if !present {
			return RecallPlan{}, fmt.Errorf("%w: selected claim %s is absent from candidates", ErrInvalidRecall, candidate.Candidate.ClaimID)
		}
		decision := plan.Decisions[index]
		decision.Selected = true
		selectedOrdinal, _ := canonical.NewOrdinal(int64(rank))
		decision.SelectedOrdinal = &selectedOrdinal
		if len(candidate.Candidate.SourceEventIDs) == 0 {
			return RecallPlan{}, fmt.Errorf("%w: selected claim %s has no source provenance", ErrInvalidRecall, candidate.Candidate.ClaimID)
		}
		sourceAlreadyIncluded := false
		sources := make(map[canonical.ID]struct{}, len(candidate.Candidate.SourceEventIDs))
		for _, sourceID := range candidate.Candidate.SourceEventIDs {
			if err := sourceID.Validate(); err != nil {
				return RecallPlan{}, fmt.Errorf("%w: source event ID: %v", ErrInvalidRecall, err)
			}
			if _, duplicate := sources[sourceID]; duplicate {
				return RecallPlan{}, fmt.Errorf("%w: duplicate source event %s", ErrInvalidRecall, sourceID)
			}
			sources[sourceID] = struct{}{}
			if _, present := included[sourceID]; present {
				sourceAlreadyIncluded = true
			}
		}
		// Any source event already supplied by Live Context or Backfill makes the
		// whole selected claim redundant for this prompt. This provenance rule is
		// independent of maturation stage, derivation/abstractness, and whether
		// the overlap covers all or only part of the claim's evidence.
		if sourceAlreadyIncluded {
			decision.ExclusionReason = ExclusionProvenanceDuplicate
			plan.Decisions[index] = decision
			continue
		}
		if int64(len(plan.Prompt)) >= policy.Recall.PromptIncludedLimit {
			decision.ExclusionReason = ExclusionPolicyFilter
			plan.Decisions[index] = decision
			continue
		}
		rendered, err := RenderClaim(policy, candidate.Candidate)
		if err != nil {
			return RecallPlan{}, err
		}
		prospective := int64(plan.RenderedBytes) + int64(len([]byte(rendered)))
		if prospective > policy.Recall.ByteBudget {
			decision.ExclusionReason = ExclusionTokenBudget
			plan.Decisions[index] = decision
			continue
		}
		promptOrdinal, _ := canonical.NewOrdinal(int64(len(plan.Prompt)))
		decision.PromptIncluded = true
		decision.PromptOrdinal = &promptOrdinal
		decision.RenderedText = rendered
		plan.Decisions[index] = decision
		plan.Prompt = append(plan.Prompt, decision)
		plan.RenderedBytes = canonical.ByteSize(prospective)
	}
	return plan, nil
}

// PlanRecallV2 applies the dialogue-context-v2 provenance rule while
// preserving the context-v1 planner for historical read/retry compatibility.
func PlanRecallV2(policy Policy, selection RecallSelection, includedEventIDs []canonical.ID) (RecallPlan, error) {
	if err := policy.RequireEnabled(); err != nil {
		return RecallPlan{}, err
	}
	included := make(map[canonical.ID]struct{}, len(includedEventIDs))
	for _, id := range includedEventIDs {
		if err := id.Validate(); err != nil {
			return RecallPlan{}, fmt.Errorf("%w: included event ID: %v", ErrInvalidRecall, err)
		}
		included[id] = struct{}{}
	}
	plan := RecallPlan{Decisions: make([]RecallDecision, len(selection.Candidates))}
	decisionIndex := make(map[canonical.ID]int, len(selection.Candidates))
	allSourcesIncluded := make(map[canonical.ID]bool, len(selection.Candidates))
	for index, candidate := range selection.Candidates {
		claimID := candidate.Candidate.ClaimID
		if _, duplicate := decisionIndex[claimID]; duplicate {
			return RecallPlan{}, fmt.Errorf("%w: duplicate audited candidate %s", ErrInvalidRecall, claimID)
		}
		covered, err := recallSourcesAllIncluded(candidate.Candidate, included)
		if err != nil {
			return RecallPlan{}, err
		}
		decisionIndex[claimID] = index
		allSourcesIncluded[claimID] = covered
		plan.Decisions[index] = RecallDecision{Candidate: candidate}
	}
	selectedClaims := make(map[canonical.ID]struct{}, len(selection.Selected))
	for rank, selected := range selection.Selected {
		claimID := selected.Candidate.ClaimID
		if _, duplicate := selectedClaims[claimID]; duplicate {
			return RecallPlan{}, fmt.Errorf("%w: duplicate selected claim %s", ErrInvalidRecall, claimID)
		}
		selectedClaims[claimID] = struct{}{}
		index, present := decisionIndex[claimID]
		if !present {
			return RecallPlan{}, fmt.Errorf("%w: selected claim %s is absent from candidates", ErrInvalidRecall, claimID)
		}
		decision := plan.Decisions[index]
		candidate := decision.Candidate.Candidate
		decision.Selected = true
		selectedOrdinal, _ := canonical.NewOrdinal(int64(rank))
		decision.SelectedOrdinal = &selectedOrdinal
		if candidate.Stage == StageFloating && !candidate.Abstract && allSourcesIncluded[claimID] {
			decision.ExclusionReason = ExclusionProvenanceDuplicateV2
			plan.Decisions[index] = decision
			continue
		}
		if int64(len(plan.Prompt)) >= policy.Recall.PromptIncludedLimit {
			decision.ExclusionReason = ExclusionPolicyFilter
			plan.Decisions[index] = decision
			continue
		}
		rendered, err := RenderClaim(policy, candidate)
		if err != nil {
			return RecallPlan{}, err
		}
		prospective := int64(plan.RenderedBytes) + int64(len([]byte(rendered)))
		if prospective > policy.Recall.ByteBudget {
			decision.ExclusionReason = ExclusionTokenBudget
			plan.Decisions[index] = decision
			continue
		}
		promptOrdinal, _ := canonical.NewOrdinal(int64(len(plan.Prompt)))
		decision.PromptIncluded = true
		decision.PromptOrdinal = &promptOrdinal
		decision.RenderedText = rendered
		plan.Decisions[index] = decision
		plan.Prompt = append(plan.Prompt, decision)
		plan.RenderedBytes = canonical.ByteSize(prospective)
	}
	return plan, nil
}

func recallSourcesAllIncluded(candidate RecallCandidate, included map[canonical.ID]struct{}) (bool, error) {
	if len(candidate.SourceEventIDs) == 0 {
		return false, fmt.Errorf("%w: claim %s has no source provenance", ErrInvalidRecall, candidate.ClaimID)
	}
	allIncluded := true
	sources := make(map[canonical.ID]struct{}, len(candidate.SourceEventIDs))
	for _, sourceID := range candidate.SourceEventIDs {
		if err := sourceID.Validate(); err != nil {
			return false, fmt.Errorf("%w: source event ID: %v", ErrInvalidRecall, err)
		}
		if _, duplicate := sources[sourceID]; duplicate {
			return false, fmt.Errorf("%w: duplicate source event %s", ErrInvalidRecall, sourceID)
		}
		sources[sourceID] = struct{}{}
		if _, present := included[sourceID]; !present {
			allIncluded = false
		}
	}
	return allIncluded, nil
}

func RenderClaim(policy Policy, candidate RecallCandidate) (string, error) {
	if err := policy.RequireEnabled(); err != nil {
		return "", err
	}
	if !utf8.ValidString(candidate.Statement) || candidate.Statement == "" {
		return "", fmt.Errorf("%w: statement must be non-empty UTF-8", ErrInvalidRecall)
	}
	if err := candidate.TemporalRelation.Validate(); err != nil {
		return "", fmt.Errorf("%w: %v", ErrInvalidRecall, err)
	}
	var wire any
	switch policy.RenderingVersion {
	case RenderingVersionV1:
		// The historical v1 wire intentionally ignores the structured v2
		// fields so persisted v1 bytes remain stable.
		wire = struct {
			Statement        string           `json:"statement"`
			TemporalRelation TemporalRelation `json:"temporal_relation"`
			Version          string           `json:"version"`
		}{
			Statement: candidate.Statement, TemporalRelation: candidate.TemporalRelation,
			Version: policy.RenderingVersion,
		}
	case RenderingVersionV2:
		if err := candidate.Confidence.Validate(); err != nil {
			return "", fmt.Errorf("%w: confidence: %v", ErrInvalidRecall, err)
		}
		if err := candidate.Currentness.Validate(); err != nil {
			return "", fmt.Errorf("%w: currentness: %v", ErrInvalidRecall, err)
		}
		wire = struct {
			Certainty        string             `json:"certainty"`
			Currentness      canonical.Ratio    `json:"currentness"`
			LastConfirmed    *canonical.Instant `json:"last_confirmed"`
			Statement        string             `json:"statement"`
			TemporalRelation TemporalRelation   `json:"temporal_relation"`
			Version          string             `json:"version"`
		}{
			Certainty: certaintyForConfidence(candidate.Confidence), Currentness: candidate.Currentness,
			LastConfirmed: candidate.LastConfirmed, Statement: candidate.Statement,
			TemporalRelation: candidate.TemporalRelation, Version: policy.RenderingVersion,
		}
	default:
		return "", fmt.Errorf("%w: unsupported rendering version %q", ErrInvalidRecall, policy.RenderingVersion)
	}
	raw, err := json.Marshal(wire)
	if err != nil {
		return "", fmt.Errorf("%w: render: %v", ErrInvalidRecall, err)
	}
	canonicalText, err := canonical.CanonicalizeRFC8785(raw)
	if err != nil {
		return "", fmt.Errorf("%w: canonical render: %v", ErrInvalidRecall, err)
	}
	return string(bytes.Clone(canonicalText.Bytes())), nil
}

func certaintyForConfidence(confidence canonical.Ratio) string {
	switch value := confidence.Millionths(); {
	case value < 500_000:
		return "low"
	case value < 750_000:
		return "medium"
	default:
		return "high"
	}
}
