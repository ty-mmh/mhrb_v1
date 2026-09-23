package app

import (
	"context"
	"testing"

	"mahoroba.local/mahoroba/internal/domain"
	"mahoroba.local/mahoroba/internal/memory"
)

func TestM5AdminProvenanceTraversesClaimEvidenceEventRunPolicyAndPipeline(t *testing.T) {
	ctx := context.Background()
	extraction := `{"claims":[{"grade":"stated","perspective":"source_actor","source_quote":"blue bicycle","statement":"The owner has a blue bicycle","subject":"source_actor","temporal_kind":"stable"}],"version":"memory-extraction-output-v1"}`
	fixture := newApplicationFixture(t, &scriptedGenerator{steps: []generatorStep{
		{text: "dialogue response"}, {text: extraction},
	}}, 2)
	if _, err := fixture.application.activateMemoryPolicyV4ForTest(ctx, fixture.residentID); err != nil {
		t.Fatal(err)
	}
	if _, err := fixture.application.Ingress(ctx, "I own a blue bicycle"); err != nil {
		t.Fatal(err)
	}
	if err := fixture.application.ProcessResident(ctx, fixture.residentID); err != nil {
		t.Fatal(err)
	}

	claims, err := fixture.application.ListMemoryClaims(ctx, domain.MemoryClaimFilter{
		ResidentID: fixture.residentID,
		Stage:      memory.StageFloating,
		Status:     memory.StatusActive,
		Scope:      memory.ScopeResidentUI,
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(claims) != 1 || claims[0].Statement != "[redacted]" ||
		claims[0].CreatedByPurpose != domain.GenerationPurposeMemoryExtraction {
		t.Fatalf("claim list = %+v", claims)
	}

	provenance, err := fixture.application.MemoryClaimProvenance(ctx, fixture.residentID, claims[0].ClaimID)
	if err != nil {
		t.Fatal(err)
	}
	if len(provenance.Evidence) != 1 || provenance.Evidence[0].SourceEventContent != "I own a blue bicycle" ||
		provenance.Evidence[0].CreatedByRunID != claims[0].CreatedByRunID ||
		provenance.Evidence[0].PipelineVersionID != claims[0].CreatedByPipelineVersionID ||
		provenance.Evidence[0].MemoryPolicyRevisionID != claims[0].CreatedByMemoryPolicyRevisionID {
		t.Fatalf("evidence provenance = %+v; claim = %+v", provenance.Evidence, claims[0])
	}
	if len(provenance.StageTransitions) != 1 || provenance.StageTransitions[0].ToStage != memory.StageFloating {
		t.Fatalf("stage provenance = %+v", provenance.StageTransitions)
	}
	if len(provenance.StatusTransitions) != 0 || len(provenance.Relations) != 0 || len(provenance.Usages) != 0 {
		t.Fatalf("unexpected provenance tails = %+v", provenance)
	}

	changed, err := fixture.application.SetMemoryClaimScope(
		ctx, fixture.residentID, claims[0].ClaimID, memory.ScopeAdminOnly,
	)
	if err != nil {
		t.Fatal(err)
	}
	if !changed.Changed || changed.Scope != memory.ScopeAdminOnly {
		t.Fatalf("scope change = %+v", changed)
	}
	repeated, err := fixture.application.SetMemoryClaimScope(
		ctx, fixture.residentID, claims[0].ClaimID, memory.ScopeAdminOnly,
	)
	if err != nil {
		t.Fatal(err)
	}
	if repeated.Changed || repeated.AssertionID != changed.AssertionID {
		t.Fatalf("idempotent scope change = %+v, first = %+v", repeated, changed)
	}
	adminOnly, err := fixture.application.ListMemoryClaims(ctx, domain.MemoryClaimFilter{
		ResidentID: fixture.residentID, Scope: memory.ScopeAdminOnly,
	})
	if err != nil || len(adminOnly) != 1 {
		t.Fatalf("admin-only claim list = %+v, error=%v", adminOnly, err)
	}
	quarantined, err := fixture.application.SetMemoryClaimStatus(
		ctx, fixture.residentID, claims[0].ClaimID,
		memory.StatusQuarantined, memory.HumanReasonQuarantine,
	)
	if err != nil || quarantined.FromStatus != memory.StatusActive || quarantined.ToStatus != memory.StatusQuarantined {
		t.Fatalf("human quarantine = %+v, error=%v", quarantined, err)
	}
	reactivated, err := fixture.application.SetMemoryClaimStatus(
		ctx, fixture.residentID, claims[0].ClaimID,
		memory.StatusActive, memory.HumanReasonReactivation,
	)
	if err != nil || reactivated.FromStatus != memory.StatusQuarantined || reactivated.ToStatus != memory.StatusActive {
		t.Fatalf("human reactivation = %+v, error=%v", reactivated, err)
	}
	updated, err := fixture.application.MemoryClaimProvenance(ctx, fixture.residentID, claims[0].ClaimID)
	if err != nil || len(updated.StatusTransitions) != 2 || updated.Claim.Status != memory.StatusActive {
		t.Fatalf("status provenance = %+v, error=%v", updated, err)
	}
}
