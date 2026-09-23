package domain

import (
	"bytes"
	"testing"
	"time"

	"mahoroba.local/mahoroba/internal/canonical"
	"mahoroba.local/mahoroba/internal/memory"
)

func TestDerivedClaimCommandsFixPurposeSpecificSourceCardinality(t *testing.T) {
	value := derivedClaimDomainFixture(t, 2)
	if err := LandClaimAbstractionCommand(value).Validate(); err != nil {
		t.Fatalf("valid abstraction: %v", err)
	}
	if err := LandClaimDifferentiationCommand(value).Validate(); err == nil {
		t.Fatal("differentiation accepted two source claims")
	}
	value = derivedClaimDomainFixture(t, 1)
	if err := LandClaimDifferentiationCommand(value).Validate(); err != nil {
		t.Fatalf("valid differentiation: %v", err)
	}
	if err := LandClaimAbstractionCommand(value).Validate(); err == nil {
		t.Fatal("abstraction accepted one source claim")
	}
}

func TestM5RTI13MandatoryObligationKeyIsStableAcrossPolicyAndPipelineChanges(t *testing.T) {
	value := derivedClaimDomainFixture(t, 2)
	eventID := value.Evidence[0].SourceEvidenceID
	want := MemoryExtractionObligation(eventID)
	value.PipelineVersionID = derivedClaimDomainID(t, 90)
	value.MemoryPolicyRevisionID = derivedClaimDomainID(t, 91)
	_ = LandClaimAbstractionCommand(value)
	if got := MemoryExtractionObligation(eventID); got != want {
		t.Fatalf("mandatory obligation changed with derived pipeline/policy: %q, want %q", got, want)
	}
}

func TestRTI13MandatoryAndAdminReextractionKeysAreStableAndDisjoint(t *testing.T) {
	eventID := derivedClaimDomainID(t, 70)
	requestID := derivedClaimDomainID(t, 71)
	wantMandatory := "memory_extract:v1:" + eventID.String()
	if got := MemoryExtractionObligation(eventID); got != wantMandatory {
		t.Fatalf("mandatory obligation = %q, want %q", got, wantMandatory)
	}
	reextract := MemoryReextractionObligation(eventID, requestID)
	if reextract == wantMandatory {
		t.Fatal("Admin re-extraction collided with mandatory obligation")
	}
	parsed, err := ParseMemoryExtractionObligation(reextract)
	if err != nil || parsed.Mode != MemoryExtractionReextract || parsed.SourceEventID != eventID ||
		parsed.RequestID == nil || *parsed.RequestID != requestID {
		t.Fatalf("parsed re-extraction key = %+v, %v", parsed, err)
	}
	for _, invalid := range []string{
		reextract + ":suffix",
		"memory_reextract:v1:" + eventID.String(),
		"memory_extract:v1:" + eventID.String() + ":" + requestID.String(),
	} {
		if _, err := ParseMemoryExtractionObligation(invalid); err == nil {
			t.Fatalf("invalid extraction key accepted: %q", invalid)
		}
	}
}

func derivedClaimDomainFixture(t *testing.T, sourceCount int) LandDerivedClaim {
	t.Helper()
	residentID := derivedClaimDomainID(t, 1)
	content := func(index int, class string, raw []byte) Content {
		salt, err := canonical.ContentSaltFromBytes(bytes.Repeat([]byte{byte(index)}, canonical.ContentCommitmentSaltLen))
		if err != nil {
			t.Fatal(err)
		}
		commitment, err := canonical.CommitContent(class, salt, raw)
		if err != nil {
			t.Fatal(err)
		}
		return Content{
			ID: derivedClaimDomainID(t, index), ResidentID: residentID, Class: class,
			Bytes: raw, BlobHash: canonical.HashBlob(raw), Commitment: commitment,
			CommitmentSalt: salt, ErasurePolicy: "independent",
		}
	}
	value := LandDerivedClaim{
		Attempt:          Attempt{RunID: derivedClaimDomainID(t, 2), ResidentID: residentID, AttemptNo: 1, OutcomeID: derivedClaimDomainID(t, 3)},
		OwnerPrincipalID: derivedClaimDomainID(t, 4), ClaimID: derivedClaimDomainID(t, 5),
		TemporalKind: memory.TemporalStable, Statement: content(6, "claim_statement", []byte("derived statement")),
		Output:            content(7, "generation_output", []byte(`{"version":"derived-v1"}`)),
		PipelineVersionID: derivedClaimDomainID(t, 8), MemoryPolicyRevisionID: derivedClaimDomainID(t, 9),
		InitialStageID: derivedClaimDomainID(t, 10), InitialViewScopeID: derivedClaimDomainID(t, 11),
	}
	for index := range sourceCount {
		sourceID := derivedClaimDomainID(t, 20+index)
		value.Sources = append(value.Sources, DerivedClaimSource{
			ClaimID: sourceID, RelationID: derivedClaimDomainID(t, 30+index),
		})
		value.Evidence = append(value.Evidence, DerivedClaimEvidence{
			EvidenceID: derivedClaimDomainID(t, 40+index), SourceClaimID: sourceID,
			SourceEvidenceID: derivedClaimDomainID(t, 50+index),
		})
	}
	return value
}

func derivedClaimDomainID(t *testing.T, index int) canonical.ID {
	t.Helper()
	clock := &derivedClaimDomainClock{now: time.Unix(1_700_000_000+int64(index), 0).UTC()}
	generator, err := canonical.NewIDGenerator(clock, bytes.NewReader(bytes.Repeat([]byte{byte(index + 1)}, 32)))
	if err != nil {
		t.Fatal(err)
	}
	id, err := generator.New()
	if err != nil {
		t.Fatal(err)
	}
	return id
}

type derivedClaimDomainClock struct{ now time.Time }

func (clock *derivedClaimDomainClock) Now() time.Time { return clock.now }
