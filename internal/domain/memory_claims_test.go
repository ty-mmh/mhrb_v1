package domain

import (
	"fmt"
	"testing"

	"mahoroba.local/mahoroba/internal/canonical"
	"mahoroba.local/mahoroba/internal/memory"
)

func claimCommandID(t *testing.T, value int) canonical.ID {
	t.Helper()
	return mustParseID(t, fmt.Sprintf("%026d", value))
}

func automaticClaimDecisionFixture(t *testing.T) AutomaticClaimStatusDecision {
	t.Helper()
	policy := claimCommandID(t, 6)
	return AutomaticClaimStatusDecision{
		ResidentID: claimCommandID(t, 1), ClaimID: claimCommandID(t, 2),
		StatusTransitionID: claimCommandID(t, 3), ToStatus: memory.StatusInvalidated,
		Reason: memory.AutomaticReasonExplicitCorrection, TriggerKind: ClaimTriggerEvidence,
		TriggerID: claimCommandID(t, 4), StatusPipelineVersionID: claimCommandID(t, 5),
		MemoryPolicyRevisionID: &policy,
	}
}

func addEvidenceCommandFixture(t *testing.T) AddClaimEvidence {
	t.Helper()
	return AddClaimEvidence{
		ResidentID: claimCommandID(t, 10), ClaimID: claimCommandID(t, 11),
		EvidenceID: claimCommandID(t, 12), SourceEventID: claimCommandID(t, 13),
		Polarity: memory.PolaritySupport, Grade: memory.GradeStated,
		Derivation: memory.DerivationExtracted, Reason: memory.EvidenceReasonSourceStated,
		MemoryPolicyRevisionID: claimCommandID(t, 14), CreatedByRunID: claimCommandID(t, 15),
		MaturationPipelineVersionID: claimCommandID(t, 16),
		SedimentStageTransitionID:   claimCommandID(t, 17), SettledStageTransitionID: claimCommandID(t, 18),
	}
}

func TestClaimEvidenceCommandRequiresAnEvidenceIdentity(t *testing.T) {
	value := addEvidenceCommandFixture(t)
	value.EvidenceID = canonical.ID{}
	if err := AddClaimEvidenceCommand(value).Validate(); err == nil {
		t.Fatal("claim evidence command accepted a missing evidence identity")
	}
}

func TestM5I16PolicyDependentDecisionRequiresMemoryPolicyRevision(t *testing.T) {
	value := addEvidenceCommandFixture(t)
	value.MemoryPolicyRevisionID = canonical.ID{}
	if err := AddClaimEvidenceCommand(value).Validate(); err == nil {
		t.Fatal("policy-dependent evidence decision accepted no memory policy revision")
	}
}

func TestHumanStatusDecisionRequiresTypedTransitionReason(t *testing.T) {
	value := HumanClaimStatusDecision{
		ResidentID: claimCommandID(t, 20), ClaimID: claimCommandID(t, 21),
		StatusTransitionID: claimCommandID(t, 22), OwnerPrincipalID: claimCommandID(t, 23),
		ToStatus: memory.StatusSuperseded, Reason: memory.HumanReasonInvalidation,
	}
	if err := HumanClaimStatusDecisionCommand(value).Validate(); err == nil {
		t.Fatal("human status decision accepted reason/target mismatch")
	}
}

func TestM5I49AutomaticStatusRejectsUnknownReasonClass(t *testing.T) {
	value := automaticClaimDecisionFixture(t)
	value.Reason = memory.AutomaticDecisionReason("model_discretion")
	if err := AutomaticClaimStatusDecisionCommand(value).Validate(); err == nil {
		t.Fatal("unknown automatic status reason was accepted")
	}
}

func TestM5I50AutomaticStatusRequiresTriggerPipelineAndGateMetrics(t *testing.T) {
	value := automaticClaimDecisionFixture(t)
	value.MemoryPolicyRevisionID = nil
	if err := AutomaticClaimStatusDecisionCommand(value).Validate(); err == nil {
		t.Fatal("semantic automatic decision without policy was accepted")
	}
}

func TestM5I52QuarantinedRecoveryRequiresHumanDecision(t *testing.T) {
	value := automaticClaimDecisionFixture(t)
	value.ToStatus = memory.StatusActive
	if err := AutomaticClaimStatusDecisionCommand(value).Validate(); err == nil {
		t.Fatal("automatic transition to active was accepted")
	}
}

func TestM5I68AutomaticReasonTriggerStatusMatrix(t *testing.T) {
	value := automaticClaimDecisionFixture(t)
	value.TriggerKind = ClaimTriggerRelation
	if err := AutomaticClaimStatusDecisionCommand(value).Validate(); err == nil {
		t.Fatal("automatic correction with relation trigger was accepted")
	}
	value = automaticClaimDecisionFixture(t)
	value.ToStatus = memory.StatusSuperseded
	if err := AutomaticClaimStatusDecisionCommand(value).Validate(); err == nil {
		t.Fatal("automatic correction to superseded was accepted")
	}
}

func TestM5I69StructuralQuarantineRequiresFindingTrigger(t *testing.T) {
	value := automaticClaimDecisionFixture(t)
	value.Reason = memory.AutomaticReasonStructuralQuarantine
	value.ToStatus = memory.StatusQuarantined
	value.TriggerKind = ClaimStatusTriggerKind("event")
	value.MemoryPolicyRevisionID = nil
	if err := AutomaticClaimStatusDecisionCommand(value).Validate(); err == nil {
		t.Fatal("structural quarantine with an event trigger was accepted")
	}
}
