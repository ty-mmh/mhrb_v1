package integrity

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"mahoroba.local/mahoroba/internal/canonical"
)

func TestM7IntegrityFindingFingerprintV1GoldenAndKindMatrix(t *testing.T) {
	residentID := integrityTestID(t, 1)
	targetID := integrityTestID(t, 2)
	sourceID := integrityTestID(t, 3)
	claimID := targetID
	golden := CandidateInput{
		ResidentID: residentID, ClaimID: &claimID,
		Kind: FindingRequiredProvenanceErased, RuleCode: RuleClaimStatementErased,
		TargetKind: TargetClaim, TargetID: targetID, TargetField: "statement_content_id",
		SourceContentErasureEventID: &sourceID,
		OccurredAt:                  100, OccurredTZ: canonical.MustTimezone("UTC"),
	}
	fingerprint, err := FingerprintV1(golden)
	if err != nil {
		t.Fatal(err)
	}
	if got, want := fingerprint.Hex(), "1c71ff30710ddceee3f6985bddba3592c916b6c009a5e1a9bc3b00c02e4df765"; got != want {
		t.Fatalf("fingerprint = %s, want pinned v1 vector %s", got, want)
	}
	otherTarget := golden
	otherTargetID := integrityTestID(t, 4)
	otherTarget.TargetID, otherTarget.ClaimID = otherTargetID, &otherTargetID
	otherTargetFingerprint, err := FingerprintV1(otherTarget)
	if err != nil {
		t.Fatal(err)
	}
	otherSource := golden
	otherSourceID := integrityTestID(t, 5)
	otherSource.SourceContentErasureEventID = &otherSourceID
	otherSourceFingerprint, err := FingerprintV1(otherSource)
	if err != nil {
		t.Fatal(err)
	}
	if fingerprint == otherTargetFingerprint || fingerprint == otherSourceFingerprint || otherTargetFingerprint == otherSourceFingerprint {
		t.Fatal("target or source erasure identity did not partition v1 fingerprints")
	}

	tests := []struct {
		name        string
		input       CandidateInput
		wantKind    FindingKind
		wantInvalid bool
	}{
		{
			name: "required provenance erased", input: golden,
			wantKind: FindingRequiredProvenanceErased,
		},
		{
			name: "provenance unresolvable",
			input: CandidateInput{
				ResidentID: residentID, Kind: FindingProvenanceUnresolvable,
				RuleCode: RuleGenerationInputSourceMissing, TargetKind: TargetGenerationInput,
				TargetID: targetID, TargetField: "source", OccurredTZ: canonical.MustTimezone("UTC"),
			},
			wantKind: FindingProvenanceUnresolvable,
		},
		{
			name: "canonical invariant",
			input: CandidateInput{
				ResidentID: residentID, Kind: FindingCanonicalInvariant,
				RuleCode: RuleActiveRequiredRevisionErased, TargetKind: TargetResidentRevision,
				TargetID: targetID, TargetField: "content_id", SourceContentErasureEventID: &sourceID,
				OccurredTZ: canonical.MustTimezone("UTC"),
			},
			wantKind: FindingCanonicalInvariant,
		},
		{
			name: "rule-kind mismatch",
			input: CandidateInput{
				ResidentID: residentID, Kind: FindingCanonicalInvariant,
				RuleCode: RuleGenerationInputSourceMissing, TargetKind: TargetGenerationInput,
				TargetID: targetID, TargetField: "source", OccurredTZ: canonical.MustTimezone("UTC"),
			},
			wantInvalid: true,
		},
		{
			name: "claim target mismatch",
			input: CandidateInput{
				ResidentID: residentID, ClaimID: &residentID,
				Kind: FindingRequiredProvenanceErased, RuleCode: RuleClaimStatementErased,
				TargetKind: TargetClaim, TargetID: targetID, TargetField: "statement_content_id",
				SourceContentErasureEventID: &sourceID, OccurredTZ: canonical.MustTimezone("UTC"),
			},
			wantInvalid: true,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			candidate, err := NewCandidate(test.input)
			if test.wantInvalid {
				if !errors.Is(err, ErrInvalidCandidate) {
					t.Fatalf("NewCandidate() error = %v, want ErrInvalidCandidate", err)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if candidate.Kind != test.wantKind {
				t.Fatalf("kind = %s, want %s", candidate.Kind, test.wantKind)
			}
		})
	}
}

func TestM7IntegrityScannerUsesCapturedReadOnlyCandidateTuple(t *testing.T) {
	residentID := integrityTestID(t, 10)
	claimID := integrityTestID(t, 11)
	sourceID := integrityTestID(t, 12)
	input := CandidateInput{
		ResidentID: residentID, ClaimID: &claimID,
		Kind: FindingRequiredProvenanceErased, RuleCode: RuleClaimStatementErased,
		TargetKind: TargetClaim, TargetID: claimID, TargetField: "statement_content_id",
		SourceContentErasureEventID: &sourceID,
		OccurredAt:                  12, OccurredTZ: canonical.MustTimezone("Asia/Tokyo"),
	}
	seq, err := canonical.NewCommitSeq(9)
	if err != nil {
		t.Fatal(err)
	}
	source := &integrityScanFake{snapshot: ScanSnapshot{
		CapturedHead: canonical.Head{Exists: true, CommitSeq: seq, CommittedAt: 99},
		Candidates:   []CandidateInput{input, input},
	}}
	result, err := NewScanner(source).Scan(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if source.calls != 1 || result.CapturedHead.CommitSeq != seq || len(result.Candidates) != 1 {
		t.Fatalf("scan result = %+v, source calls=%d", result, source.calls)
	}
	if err := result.Candidates[0].Validate(); err != nil {
		t.Fatalf("scanner returned invalid candidate: %v", err)
	}

	// Changing excluded occurrence metadata preserves the logical identity.
	changed := input
	changed.OccurredAt++
	changed.OccurredTZ = canonical.MustTimezone("UTC")
	left, _ := NewCandidate(input)
	right, _ := NewCandidate(changed)
	if !SemanticEqual(left, right) {
		t.Fatal("occurrence metadata changed finding identity")
	}
}

func TestM7RecordFindingsStructuralQuarantinePlanContract(t *testing.T) {
	residentID := integrityTestID(t, 30)
	claimID := integrityTestID(t, 31)
	sourceID := integrityTestID(t, 32)
	findingID := integrityTestID(t, 33)
	transitionID := integrityTestID(t, 34)
	integrityPipelineID := integrityTestID(t, 35)
	memoryStatusID := integrityTestID(t, 36)
	claimCopy, sourceCopy := claimID, sourceID
	candidate, err := NewCandidate(CandidateInput{
		ResidentID: residentID, ClaimID: &claimCopy,
		Kind: FindingRequiredProvenanceErased, RuleCode: RuleClaimStatementErased,
		TargetKind: TargetClaim, TargetID: claimID, TargetField: "statement_content_id",
		SourceContentErasureEventID: &sourceCopy, OccurredTZ: canonical.MustTimezone("UTC"),
	})
	if err != nil {
		t.Fatal(err)
	}
	valid := RecordFindings{
		ResidentID: residentID, PipelineVersionID: integrityPipelineID,
		MemoryStatusPipelineVersionID: &memoryStatusID,
		Findings: []PlannedFinding{{
			FindingID: findingID, Candidate: candidate, QuarantineTransitionID: &transitionID,
		}},
	}
	if err := valid.Validate(); err != nil {
		t.Fatalf("valid structural plan = %v", err)
	}
	qualifying, err := NewCandidate(CandidateInput{
		ResidentID: residentID, ClaimID: &claimCopy,
		Kind: FindingRequiredProvenanceErased, RuleCode: RuleClaimQualifyingSupportErased,
		TargetKind: TargetClaim, TargetID: claimID, TargetField: "qualifying_support",
		SourceContentErasureEventID: &sourceCopy, OccurredTZ: canonical.MustTimezone("UTC"),
	})
	if err != nil {
		t.Fatal(err)
	}
	multiRule := valid
	multiRule.Findings = append(append([]PlannedFinding(nil), valid.Findings...), PlannedFinding{
		FindingID: integrityTestID(t, 37), Candidate: qualifying,
		QuarantineTransitionID: &transitionID,
	})
	if err := multiRule.Validate(); err != nil {
		t.Fatalf("same-claim structural findings sharing one transition = %v", err)
	}
	tests := []struct {
		name   string
		mutate func(*RecordFindings)
	}{
		{name: "missing transition ID", mutate: func(value *RecordFindings) {
			value.Findings[0].QuarantineTransitionID = nil
		}},
		{name: "missing memory-status dependency", mutate: func(value *RecordFindings) {
			value.MemoryStatusPipelineVersionID = nil
		}},
		{name: "same claim uses different transitions", mutate: func(value *RecordFindings) {
			otherFinding, otherTransition := integrityTestID(t, 38), integrityTestID(t, 39)
			value.Findings = append(value.Findings, PlannedFinding{
				FindingID: otherFinding, Candidate: qualifying, QuarantineTransitionID: &otherTransition,
			})
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			value := valid
			value.Findings = append([]PlannedFinding(nil), valid.Findings...)
			test.mutate(&value)
			if err := value.Validate(); !errors.Is(err, ErrInvalidCandidate) {
				t.Fatalf("Validate() error = %v, want ErrInvalidCandidate", err)
			}
		})
	}
}

type integrityScanFake struct {
	snapshot ScanSnapshot
	calls    int
}

func (source *integrityScanFake) CaptureIntegrityCandidates(context.Context) (ScanSnapshot, error) {
	source.calls++
	return source.snapshot, nil
}

func integrityTestID(t *testing.T, value int) canonical.ID {
	t.Helper()
	id, err := canonical.ParseID(fmt.Sprintf("%026d", value))
	if err != nil {
		t.Fatal(err)
	}
	return id
}
