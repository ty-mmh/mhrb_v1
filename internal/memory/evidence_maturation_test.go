package memory

import (
	"errors"
	"strings"
	"testing"
)

func baseEvidence(t *testing.T, index int) EvidencePoint {
	t.Helper()
	return EvidencePoint{
		SourceEventID: testID(t, index), EventType: EventUserMessage,
		Polarity: PolaritySupport, Grade: GradeStated, Trust: TrustTrusted,
		Derivation: DerivationExtracted, Reason: EvidenceReasonSourceStated,
	}
}

func TestEvidenceFixedPointWeightsAndProvenance(t *testing.T) {
	policy := DefaultPolicyV2()

	stated, err := EvaluateEvidence(policy, baseEvidence(t, 0))
	if err != nil {
		t.Fatalf("stated evidence: %v", err)
	}
	if got := stated.EffectiveWeight.Millionths(); got != 1_000_000 {
		t.Fatalf("stated weight = %d, want 1000000", got)
	}

	untrusted := baseEvidence(t, 1)
	untrusted.Grade = GradeInferred
	untrusted.Reason = EvidenceReasonSourceInferred
	untrusted.Trust = TrustUntrusted
	evaluated, err := EvaluateEvidence(policy, untrusted)
	if err != nil {
		t.Fatalf("untrusted inferred evidence: %v", err)
	}
	if got := evaluated.EffectiveWeight.Millionths(); got != 150_000 {
		t.Fatalf("untrusted inferred weight = %d, want 150000", got)
	}

	sourceEvidence := testID(t, 7)
	inherited := baseEvidence(t, 2)
	inherited.Derivation = DerivationInherited
	inherited.SourceEvidenceID = &sourceEvidence
	inherited.InheritanceDepth = 1
	inherited.Reason = EvidenceReasonInheritedAbstraction
	evaluated, err = EvaluateEvidence(policy, inherited)
	if err != nil {
		t.Fatalf("inherited evidence: %v", err)
	}
	if got := evaluated.EffectiveWeight.Millionths(); got != 500_000 {
		t.Fatalf("inherited weight = %d, want 500000", got)
	}

	selfTalk := baseEvidence(t, 3)
	selfTalk.EventType = EventSelfTalk
	selfTalk.Reason = EvidenceReasonSourceInferred
	evaluated, err = EvaluateEvidence(policy, selfTalk)
	if err != nil {
		t.Fatalf("self-talk evidence: %v", err)
	}
	if evaluated.EffectiveGrade != GradeInferred || evaluated.EffectiveWeight.Millionths() != 600_000 {
		t.Fatalf("self-talk = grade %q weight %d", evaluated.EffectiveGrade, evaluated.EffectiveWeight.Millionths())
	}
}

func TestEvidenceRejectsForbiddenAndUnlaunderedInputs(t *testing.T) {
	policy := DefaultPolicyV2()
	tests := map[string]func(*EvidencePoint){
		"forbidden event":     func(point *EvidencePoint) { point.EventType = EventResidentMessage },
		"observed grade":      func(point *EvidencePoint) { point.Grade = GradeObserved },
		"wrong stated reason": func(point *EvidencePoint) { point.Reason = EvidenceReasonSourceInferred },
		"inherited without source": func(point *EvidencePoint) {
			point.Derivation = DerivationInherited
			point.InheritanceDepth = 1
			point.Reason = EvidenceReasonInheritedSplit
		},
		"too-deep inheritance": func(point *EvidencePoint) {
			source := testID(t, 11)
			point.Derivation = DerivationInherited
			point.SourceEvidenceID = &source
			point.InheritanceDepth = 2
			point.Reason = EvidenceReasonInheritedSplit
		},
	}
	for name, mutate := range tests {
		t.Run(name, func(t *testing.T) {
			point := baseEvidence(t, 0)
			mutate(&point)
			if _, err := EvaluateEvidence(policy, point); !errors.Is(err, ErrInvalidEvidence) {
				t.Fatalf("error = %v, want ErrInvalidEvidence", err)
			}
		})
	}
}

func TestM6I39OutboundInitiativeCannotBecomeClaimEvidence(t *testing.T) {
	point := baseEvidence(t, 0)
	point.EventType = EventOutboundInitiative
	if _, err := EvaluateEvidence(DefaultPolicyV3(), point); !errors.Is(err, ErrInvalidEvidence) {
		t.Fatalf("outbound initiative evidence error = %v, want ErrInvalidEvidence", err)
	}
}

func TestM6I40SelfTalkOnlySupportCannotSettleClaim(t *testing.T) {
	policy := DefaultPolicyV3()
	points := make([]EvidencePoint, 4)
	for index := range points {
		points[index] = baseEvidence(t, index)
		points[index].EventType = EventSelfTalk
		points[index].Grade = GradeInferred
		points[index].Reason = EvidenceReasonSourceInferred
	}
	aggregate, err := EvaluateAndAggregateEvidence(policy, points)
	if err != nil {
		t.Fatal(err)
	}
	if aggregate.SupportWeight.Millionths() < policy.Maturation.SettledSupportWeight ||
		aggregate.DistinctSupportEvents.Int64() < policy.Maturation.SettledDistinctEvents {
		t.Fatalf("fixture does not reach numeric settled thresholds: %+v", aggregate)
	}
	decision, err := EvaluateMaturation(policy, MaturationInput{
		CurrentStage: StageSediment, Kind: ClaimKindDirect, Evidence: aggregate,
	})
	if err != nil {
		t.Fatal(err)
	}
	if decision.Advance || decision.BlockingReason != "settled_provenance_gate" || aggregate.HasExternalSupport {
		t.Fatalf("self-talk-only settlement = %+v aggregate=%+v", decision, aggregate)
	}
}

func TestEvidenceAggregateConfidenceAndDedupe(t *testing.T) {
	policy := DefaultPolicyV2()
	first := baseEvidence(t, 0)
	first.ActorIsSubject = true
	second := baseEvidence(t, 1)
	second.ActorIsPerspective = true
	contradict := baseEvidence(t, 2)
	contradict.Polarity = PolarityContradict
	aggregate, err := EvaluateAndAggregateEvidence(policy, []EvidencePoint{first, second, contradict})
	if err != nil {
		t.Fatalf("EvaluateAndAggregateEvidence: %v", err)
	}
	if aggregate.SupportWeight.Millionths() != 2_000_000 || aggregate.ContradictWeight.Millionths() != 1_000_000 {
		t.Fatalf("weights = support %d contradict %d", aggregate.SupportWeight.Millionths(), aggregate.ContradictWeight.Millionths())
	}
	if aggregate.Confidence.Millionths() != 666_666 {
		t.Fatalf("confidence = %d, want floor 666666", aggregate.Confidence.Millionths())
	}
	if aggregate.DistinctSupportEvents.Int64() != 2 || !aggregate.HasExternalSupport || !aggregate.HasSubjectUserSupport || !aggregate.HasPerspectiveUserSupport {
		t.Fatalf("aggregate provenance flags = %+v", aggregate)
	}

	lower := EvaluatedEvidence{EvidencePoint: first, EffectiveGrade: GradeInferred, EffectiveWeight: mustWeight(t, 600_000)}
	higher := EvaluatedEvidence{EvidencePoint: first, EffectiveGrade: GradeStated, EffectiveWeight: mustWeight(t, 1_000_000)}
	deduped, err := AggregateEvidence(policy, []EvaluatedEvidence{lower, higher})
	if err != nil {
		t.Fatalf("AggregateEvidence(deduped): %v", err)
	}
	if deduped.SupportWeight.Millionths() != 1_000_000 || deduped.DistinctSupportEvents.Int64() != 1 {
		t.Fatalf("dedupe aggregate = %+v", deduped)
	}
}

func TestMaturationThresholdsAndProvenanceGates(t *testing.T) {
	policy := DefaultPolicyV2()
	base := EvidenceAggregate{
		SupportWeight: mustWeight(t, 1_200_000), ContradictWeight: mustWeight(t, 0),
		Confidence: mustRatio(t, 1_000_000), DistinctSupportEvents: mustCount(t, 2), HasTrustedSupport: true,
	}
	decision, err := EvaluateMaturation(policy, MaturationInput{CurrentStage: StageFloating, Kind: ClaimKindOther, Evidence: base})
	if err != nil {
		t.Fatalf("floating maturation: %v", err)
	}
	if !decision.Advance || decision.To != StageSediment || decision.Reason != StageReasonMaturationThreshold {
		t.Fatalf("floating decision = %+v", decision)
	}

	settledEvidence := base
	settledEvidence.SupportWeight = mustWeight(t, 2_000_000)
	settledEvidence.DistinctSupportEvents = mustCount(t, 3)
	settledEvidence.Confidence = mustRatio(t, 750_000)
	decision, err = EvaluateMaturation(policy, MaturationInput{CurrentStage: StageSediment, Kind: ClaimKindOther, Evidence: settledEvidence})
	if err != nil {
		t.Fatalf("other maturation: %v", err)
	}
	if decision.Advance || decision.BlockingReason != "settled_provenance_gate" {
		t.Fatalf("subject-less other decision = %+v", decision)
	}
	settledEvidence.HasSubjectUserSupport = true
	decision, err = EvaluateMaturation(policy, MaturationInput{CurrentStage: StageSediment, Kind: ClaimKindOther, Evidence: settledEvidence})
	if err != nil || !decision.Advance || decision.To != StageSettled {
		t.Fatalf("subject-backed other decision = %+v, err %v", decision, err)
	}

	settledEvidence.HasSubjectUserSupport = false
	alignment := &AlignmentGate{
		Kind: ClaimKindMeta, Status: StatusActive, Stage: StageSediment,
		Confidence: mustRatio(t, 800_000), HasExternalSupport: true, HasPerspectiveUserSupport: true,
	}
	decision, err = EvaluateMaturation(policy, MaturationInput{
		CurrentStage: StageSediment, Kind: ClaimKindDirect, Evidence: settledEvidence, Alignment: alignment,
	})
	if err != nil || !decision.Advance || decision.Reason != StageReasonExternalAlignment || !decision.Metrics.AlignmentSatisfied {
		t.Fatalf("aligned direct decision = %+v, err %v", decision, err)
	}
	alignment.Status = StatusQuarantined
	decision, err = EvaluateMaturation(policy, MaturationInput{
		CurrentStage: StageSediment, Kind: ClaimKindDirect, Evidence: settledEvidence, Alignment: alignment,
	})
	if err != nil || decision.Advance {
		t.Fatalf("inactive alignment decision = %+v, err %v", decision, err)
	}

	metricsJSON, err := decision.Metrics.CanonicalJSON()
	if err != nil {
		t.Fatalf("metrics CanonicalJSON: %v", err)
	}
	if !strings.Contains(string(metricsJSON.Bytes()), `"confidence":"750000"`) {
		t.Fatalf("metrics lack quantized confidence: %s", metricsJSON.Bytes())
	}
}

func TestM5I13SettledRejectsUntrustedOnlyEvidence(t *testing.T) {
	policy := DefaultPolicyV2()
	evidence := EvidenceAggregate{
		SupportWeight: mustWeight(t, 2_000_000), ContradictWeight: mustWeight(t, 0),
		Confidence: mustRatio(t, 1_000_000), DistinctSupportEvents: mustCount(t, 8),
		HasExternalSupport: true, HasSubjectUserSupport: true,
	}
	decision, err := EvaluateMaturation(policy, MaturationInput{
		CurrentStage: StageSediment, Kind: ClaimKindOther, Evidence: evidence,
	})
	if err != nil {
		t.Fatalf("EvaluateMaturation: %v", err)
	}
	if decision.Advance || decision.BlockingReason != "trusted_support_required" {
		t.Fatalf("untrusted-only maturation decision = %+v", decision)
	}
}

func TestInitialAndDerivedScopes(t *testing.T) {
	policy := DefaultPolicyV2()
	if scope, err := InitialScopeForEvent(policy, EventUserMessage); err != nil || scope != ScopeResidentUI {
		t.Fatalf("user scope = %q, %v", scope, err)
	}
	if scope, err := InitialScopeForEvent(policy, EventSelfTalk); err != nil || scope != ScopeAdminOnly {
		t.Fatalf("self-talk scope = %q, %v", scope, err)
	}
	if scope, err := NarrowestSourceScope(policy, []ViewScope{ScopeResidentUI, ScopeAdminOnly}); err != nil || scope != ScopeAdminOnly {
		t.Fatalf("derived scope = %q, %v", scope, err)
	}
	if _, err := NarrowestSourceScope(policy, nil); !errors.Is(err, ErrInvalidEvidence) {
		t.Fatalf("empty source scope error = %v", err)
	}
}
