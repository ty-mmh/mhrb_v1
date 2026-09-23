package projection

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"mahoroba.local/mahoroba/internal/canonical"
)

// legacyM4ZeroWatermarkMetadataDefinition preserves only the historical M4
// zero-dependency watermark fixture. Keeping it in a _test.go file prevents
// production registries from accepting it as the claim_states definition.
func legacyM4ZeroWatermarkMetadataDefinition() Definition {
	return Definition{Name: ClaimStatesName, Version: "claim-states-v1", TimeSensitive: true}
}

func TestM5ClaimStatesProductionDefinitionDeclaresExactMemoryPolicyDependency(t *testing.T) {
	definition := ClaimStatesDefinition()
	if definition.Name != ClaimStatesName || definition.Version != "claim-states-v1" || !definition.TimeSensitive {
		t.Fatalf("claim_states definition = %+v", definition)
	}
	if fmt.Sprint(definition.Dependencies) != fmt.Sprint([]DependencyKind{MemoryPolicyDependency}) ||
		fmt.Sprint(definition.RebuildOnActivation) != fmt.Sprint([]DependencyKind{MemoryPolicyDependency}) {
		t.Fatalf("claim_states dependency contract = %+v", definition)
	}
	fixture := legacyM4ZeroWatermarkMetadataDefinition()
	if fixture.Name != definition.Name || fixture.Version != definition.Version || fixture.TimeSensitive != definition.TimeSensitive ||
		len(fixture.Dependencies) != 0 || len(fixture.RebuildOnActivation) != 0 {
		t.Fatal("M4 fixture compatibility definition diverged from production")
	}
}

func TestClaimReplayDefaultsTransitionlessClaimToActive(t *testing.T) {
	input, evaluator := claimReplayFixture(t)
	input.StatusTransitions = nil
	input.StageTransitions = nil
	states, err := ReplayClaimStates(context.Background(), input, evaluator)
	if err != nil {
		t.Fatal(err)
	}
	if len(states) != 1 || states[0].Status != ClaimStatusActive || states[0].Stage != ClaimStageFloating {
		t.Fatalf("claim states = %+v", states)
	}
}

func TestClaimReplayUsesPointwiseThenAggregateWithZeroDependencies(t *testing.T) {
	input, _ := claimReplayFixture(t)
	if definition := legacyM4ZeroWatermarkMetadataDefinition(); len(definition.Dependencies) != 0 {
		t.Fatalf("M4 fixture dependencies = %+v, want zero", definition.Dependencies)
	}
	var captured ClaimEvaluationInput
	evaluator := ClaimStateEvaluatorFunc(func(_ context.Context, value ClaimEvaluationInput) (ClaimStateMetrics, error) {
		captured = value
		return validClaimMetrics(), nil
	})
	states, err := ReplayClaimStates(context.Background(), input, evaluator)
	if err != nil {
		t.Fatal(err)
	}
	if captured.ActivePolicyRevisionID != input.ActivePolicyRevisionID {
		t.Fatalf("aggregate policy = %s, want %s", captured.ActivePolicyRevisionID, input.ActivePolicyRevisionID)
	}
	if len(captured.Evidence) != 2 || captured.Evidence[0].CommitSeq > captured.Evidence[1].CommitSeq {
		t.Fatalf("typed evidence order = %+v", captured.Evidence)
	}
	if captured.Evidence[0].PolicyRevisionID == captured.ActivePolicyRevisionID || captured.Evidence[1].PolicyRevisionID == captured.ActivePolicyRevisionID {
		t.Fatal("recorded pointwise policies were replaced by aggregate policy")
	}
	if len(states) != 1 || states[0].EvidenceCount != 2 || states[0].TemporalRelation != ClaimTemporalCurrent {
		t.Fatalf("claim states = %+v", states)
	}
}

func TestClaimAggregateUsesPolicyActiveAtExplicitAsOf(t *testing.T) {
	input, _ := claimReplayFixture(t)
	explicitAsOf := input.AsOf
	var captured ClaimEvaluationInput
	evaluator := ClaimStateEvaluatorFunc(func(_ context.Context, value ClaimEvaluationInput) (ClaimStateMetrics, error) {
		captured = value
		return validClaimMetrics(), nil
	})
	if _, err := ReplayClaimStates(context.Background(), input, evaluator); err != nil {
		t.Fatal(err)
	}
	if captured.AsOf != explicitAsOf || captured.ActivePolicyRevisionID != input.ActivePolicyRevisionID {
		t.Fatalf("aggregate target = as_of %d policy %s, want %d/%s",
			captured.AsOf, captured.ActivePolicyRevisionID, explicitAsOf, input.ActivePolicyRevisionID)
	}
}

func TestM5ClaimReplayIgnoresRowsBeyondCapturedHeadOrAsOf(t *testing.T) {
	input, _ := claimReplayFixture(t)
	input.Evidence = append(input.Evidence,
		claimEvidenceFixture(t, "00000000000000000000000021", input.Claims[0].ClaimID, 11, 20, input.ActivePolicyRevisionID),
		claimEvidenceFixture(t, "00000000000000000000000022", input.Claims[0].ClaimID, 9, 301, input.ActivePolicyRevisionID),
	)
	var evidenceCount int
	evaluator := ClaimStateEvaluatorFunc(func(_ context.Context, value ClaimEvaluationInput) (ClaimStateMetrics, error) {
		evidenceCount = len(value.Evidence)
		return validClaimMetrics(), nil
	})
	states, err := ReplayClaimStates(context.Background(), input, evaluator)
	if err != nil {
		t.Fatal(err)
	}
	if evidenceCount != 2 || len(states) != 1 || states[0].EvidenceCount != 2 {
		t.Fatalf("captured evidence = %d, states = %+v", evidenceCount, states)
	}
}

func TestM5ClaimReplayRejectsAsOfRegression(t *testing.T) {
	input, evaluator := claimReplayFixture(t)
	previous := canonical.Instant(301)
	input.PreviousAsOf = &previous
	if _, err := ReplayClaimStates(context.Background(), input, evaluator); !errors.Is(err, ErrAsOfRegression) {
		t.Fatalf("claim replay regression error = %v", err)
	}
}

func TestM5ClaimReplayReplaysStageAndStatusTransitions(t *testing.T) {
	input, evaluator := claimReplayFixture(t)
	claimID := input.Claims[0].ClaimID
	input.StageTransitions = []ClaimTransition{
		{TransitionID: projectionFixtureID(t, "00000000000000000000000023"), ClaimID: claimID, CommitSeq: projectionFixtureCommit(t, 1), RecordedAt: 10, From: "", To: "floating"},
		{TransitionID: projectionFixtureID(t, "00000000000000000000000024"), ClaimID: claimID, CommitSeq: projectionFixtureCommit(t, 4), RecordedAt: 40, From: "floating", To: "sediment"},
		{TransitionID: projectionFixtureID(t, "00000000000000000000000025"), ClaimID: claimID, CommitSeq: projectionFixtureCommit(t, 5), RecordedAt: 50, From: "sediment", To: "settled"},
	}
	input.StatusTransitions = []ClaimTransition{
		{TransitionID: projectionFixtureID(t, "00000000000000000000000026"), ClaimID: claimID, CommitSeq: projectionFixtureCommit(t, 6), RecordedAt: 60, From: "active", To: "superseded"},
	}
	states, err := ReplayClaimStates(context.Background(), input, evaluator)
	if err != nil {
		t.Fatal(err)
	}
	if len(states) != 1 || states[0].Stage != ClaimStageSettled || states[0].Status != ClaimStatusSuperseded {
		t.Fatalf("replayed claim transitions = %+v", states)
	}
}

func TestM5ClaimReplayRejectsUnconfiguredEvaluatorAndInvalidMetrics(t *testing.T) {
	input, _ := claimReplayFixture(t)
	if _, err := ReplayClaimStates(context.Background(), input, nil); err == nil {
		t.Fatal("nil evaluator unexpectedly accepted")
	}
	invalid := ClaimStateEvaluatorFunc(func(context.Context, ClaimEvaluationInput) (ClaimStateMetrics, error) {
		return ClaimStateMetrics{TemporalRelation: "not-a-relation"}, nil
	})
	if _, err := ReplayClaimStates(context.Background(), input, invalid); err == nil {
		t.Fatal("invalid evaluator metrics unexpectedly accepted")
	}
}

func claimReplayFixture(t *testing.T) (ClaimReplayInput, ClaimStateEvaluator) {
	t.Helper()
	claimID := projectionFixtureID(t, "00000000000000000000000011")
	pointPolicyOne := projectionFixtureID(t, "00000000000000000000000012")
	pointPolicyTwo := projectionFixtureID(t, "00000000000000000000000013")
	activePolicy := projectionFixtureID(t, "00000000000000000000000014")
	recallRunID := projectionFixtureID(t, "00000000000000000000000018")
	lastReference := canonical.Instant(50)
	return ClaimReplayInput{
			ThroughCommitSeq: projectionFixtureCommit(t, 10), AsOf: 300, ActivePolicyRevisionID: activePolicy,
			Claims: []ClaimSeed{{ClaimID: claimID, CommitSeq: projectionFixtureCommit(t, 1), RecordedAt: 10, TemporalKind: ClaimTemporalStable}},
			Evidence: []ClaimEvidence{
				claimEvidenceFixture(t, "00000000000000000000000016", claimID, 3, 30, pointPolicyTwo),
				claimEvidenceFixture(t, "00000000000000000000000015", claimID, 2, 20, pointPolicyOne),
			},
			Usages: []ClaimUsage{{
				UsageID: projectionFixtureID(t, "00000000000000000000000017"), ClaimID: claimID,
				RecallRunID: &recallRunID, CommitSeq: projectionFixtureCommit(t, 4), RecordedAt: lastReference,
				PolicyRevisionID: pointPolicyTwo, UsageType: ClaimUsagePromptIncluded,
			}},
		}, ClaimStateEvaluatorFunc(func(_ context.Context, input ClaimEvaluationInput) (ClaimStateMetrics, error) {
			metrics := validClaimMetrics()
			metrics.LastReferencedAt = &lastReference
			return metrics, nil
		})
}

func claimEvidenceFixture(t *testing.T, rawID string, claimID canonical.ID, commit int64, recordedAt canonical.Instant, policyID canonical.ID) ClaimEvidence {
	t.Helper()
	return ClaimEvidence{
		EvidenceID: projectionFixtureID(t, rawID), ClaimID: claimID, SourceEventID: claimID,
		CommitSeq:  projectionFixtureCommit(t, commit),
		RecordedAt: recordedAt, PolicyRevisionID: policyID, Polarity: ClaimEvidenceSupport,
		SourceEventType: ClaimSourceUserMessage, Grade: ClaimEvidenceStated, TrustLevel: ClaimTrustTrusted,
		Derivation: ClaimEvidenceExtracted, ReasonCode: "source_stated", ActorIsSubject: true,
		Weight: canonical.Weight(canonical.FixedPointScale),
	}
}

func validClaimMetrics() ClaimStateMetrics {
	return ClaimStateMetrics{
		Salience: 1.25, Confidence: canonical.Ratio(800_000), Currentness: canonical.Ratio(900_000),
		TemporalRelation: ClaimTemporalCurrent,
	}
}

func projectionFixtureID(t *testing.T, raw string) canonical.ID {
	t.Helper()
	id, err := canonical.ParseID(raw)
	if err != nil {
		t.Fatal(err)
	}
	return id
}

func projectionFixtureCommit(t *testing.T, raw int64) canonical.CommitSeq {
	t.Helper()
	value, err := canonical.NewCommitSeq(raw)
	if err != nil {
		t.Fatal(err)
	}
	return value
}
