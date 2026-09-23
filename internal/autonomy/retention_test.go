package autonomy

import (
	"testing"
	"time"

	"mahoroba.local/mahoroba/internal/canonical"
)

func TestM6RetentionCandidateAtExactDuration(t *testing.T) {
	policy := DefaultPolicy("UTC").Retention
	policy.Mode = RetentionCandidateAfter
	now := time.Date(2026, 8, 21, 12, 0, 0, 0, time.UTC)
	source := retentionSource(t, now.Add(-policy.Duration))
	candidates, err := Candidates(policy, now, RetentionCapture{Sources: []RetentionSource{source}})
	if err != nil {
		t.Fatal(err)
	}
	if len(candidates) != 1 || candidates[0].Age != policy.Duration {
		t.Fatalf("candidates=%#v", candidates)
	}
	if candidates[0].AgeMicroseconds != policy.Duration.Microseconds() {
		t.Fatalf("age_microseconds=%d", candidates[0].AgeMicroseconds)
	}
}

func TestM6RetentionRejectsFutureErasedAndResidentOnlyContent(t *testing.T) {
	policy := DefaultPolicy("UTC").Retention
	policy.Mode = RetentionCandidateAfter
	now := time.Date(2026, 8, 21, 12, 0, 0, 0, time.UTC)
	future := retentionSource(t, now.Add(time.Hour))
	erased := retentionSource(t, now.Add(-policy.Duration))
	erased.EventID = testID(t, "01K00000000000000000000014")
	erased.ContentPresent = false
	residentOnly := retentionSource(t, now.Add(-policy.Duration))
	residentOnly.EventID = testID(t, "01K00000000000000000000012")
	residentOnly.ErasurePolicy = "resident_only"
	candidates, err := Candidates(policy, now, RetentionCapture{Sources: []RetentionSource{future, erased, residentOnly}})
	if err != nil {
		t.Fatal(err)
	}
	if len(candidates) != 1 || len(candidates[0].BlockingReasons) != 1 ||
		candidates[0].BlockingReasons[0] != ImpactContentPolicyRequiresResidentErase {
		t.Fatalf("candidates=%#v", candidates)
	}
}

func TestM6RetentionReferenceReasonsAreDeterministic(t *testing.T) {
	policy := DefaultPolicy("UTC").Retention
	policy.Mode = RetentionCandidateAfter
	now := time.Date(2026, 8, 21, 12, 0, 0, 0, time.UTC)
	source := retentionSource(t, now.Add(-policy.Duration))
	source.References = ReferenceImpact{EvidenceReferences: 2, ClaimReferences: 1, GenerationInputReferences: 3, GenerationOutputReferences: 1}
	candidates, err := Candidates(policy, now, RetentionCapture{Sources: []RetentionSource{source}})
	if err != nil {
		t.Fatal(err)
	}
	want := []RetentionImpactReason{
		ImpactClaimProvenanceReference, ImpactGenerationInputReference, ImpactGenerationOutputReference,
	}
	if len(candidates) != 1 || len(candidates[0].BlockingReasons) != len(want) {
		t.Fatalf("candidates=%#v", candidates)
	}
	for index := range want {
		if candidates[0].BlockingReasons[index] != want[index] {
			t.Fatalf("reasons=%v", candidates[0].BlockingReasons)
		}
	}
}

func TestM6RetentionPureCandidateCalculationDoesNotMutateSource(t *testing.T) {
	policy := DefaultPolicy("UTC").Retention
	policy.Mode = RetentionCandidateAfter
	now := time.Date(2026, 8, 21, 12, 0, 0, 0, time.UTC)
	source := retentionSource(t, now.Add(-policy.Duration))
	source.References.GenerationOutputReferences = 1
	original := source
	candidates, err := Candidates(policy, now, RetentionCapture{Sources: []RetentionSource{source}})
	if err != nil {
		t.Fatal(err)
	}
	if len(candidates) != 1 || source != original || !source.ContentPresent {
		t.Fatalf("candidate calculation mutated its Canonical source: candidates=%#v source=%#v", candidates, source)
	}
	if len(candidates[0].BlockingReasons) != 1 || candidates[0].BlockingReasons[0] != ImpactGenerationOutputReference {
		t.Fatalf("generation output impact=%v", candidates[0].BlockingReasons)
	}
}

func TestM7RTI24RetentionCandidateModeNeverErasesContent(t *testing.T) {
	now := time.Date(2026, 8, 24, 0, 0, 0, 0, time.UTC)
	source := retentionSource(t, now.Add(-31*24*time.Hour))
	original := source

	disabled := DefaultPolicy("UTC").Retention
	if candidates, err := Candidates(disabled, now, RetentionCapture{Sources: []RetentionSource{source}}); err != nil || len(candidates) != 0 {
		t.Fatalf("disabled retention produced work: %#v, %v", candidates, err)
	}
	candidateOnly := disabled
	candidateOnly.Mode = RetentionCandidateAfter
	candidates, err := Candidates(candidateOnly, now, RetentionCapture{Sources: []RetentionSource{source}})
	if err != nil || len(candidates) != 1 || candidates[0].ContentID != source.ContentID {
		t.Fatalf("candidate mode result = %#v, %v", candidates, err)
	}
	if source != original || !source.ContentPresent {
		t.Fatalf("candidate derivation mutated its Canonical snapshot: before=%#v after=%#v", original, source)
	}
}

func retentionSource(t *testing.T, at time.Time) RetentionSource {
	t.Helper()
	return RetentionSource{
		ResidentID: testID(t, "01K00000000000000000000010"),
		EventID:    testID(t, "01K00000000000000000000011"),
		ContentID:  testID(t, "01K00000000000000000000013"), EventType: EventSelfTalk,
		RecordedAt: canonical.InstantFromTime(at), ContentPresent: true, ErasurePolicy: "independent",
	}
}
