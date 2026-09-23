package app

import (
	"context"
	"testing"

	"mahoroba.local/mahoroba/internal/memory"
)

func TestMemoryPolicyV5ExplicitMigrationPreservesV4EvidenceAndHistory(t *testing.T) {
	fixture := newRecallReadyFixture(t)
	ctx := context.Background()
	before := readRecallClaimMutationSnapshot(t, fixture)
	oldResident, err := fixture.repository.Resident(ctx, fixture.residentID)
	if err != nil {
		t.Fatal(err)
	}
	activation, err := fixture.application.ActivateMemoryPolicyV5(ctx, ActivateMemoryPolicyV5Options{ResidentID: fixture.residentID, ExpectedFrom: memory.PolicyVersionV4})
	if err != nil {
		t.Fatal(err)
	}
	if !activation.Changed || activation.PolicyVersion != memory.PolicyVersionV5 || activation.PreviousPolicyVersion != memory.PolicyVersionV4 {
		t.Fatalf("activation=%+v", activation)
	}
	reconcileRecallResident(t, fixture.applicationFixture)
	after := readRecallClaimMutationSnapshot(t, fixture)
	if after.Evidence != before.Evidence || after.Stages != before.Stages || after.ProjectedStage != before.ProjectedStage {
		t.Fatalf("historical evidence/stages changed: before=%+v after=%+v", before, after)
	}
	if after.Confidence >= before.Confidence || after.Confidence == 0 {
		t.Fatalf("v5 projection did not account for evidence amount: before=%d after=%d", before.Confidence, after.Confidence)
	}
	// The captured V4 policy remains parseable and byte-identical; no migration
	// of existing policy/content rows is performed by activation.
	oldPolicy, canonicalOld, err := memory.ParsePolicy([]byte(oldResident.MemoryPolicy))
	if err != nil || oldPolicy.Version != memory.PolicyVersionV4 || canonicalOld.String() != oldResident.MemoryPolicy {
		t.Fatalf("old policy changed: %v", err)
	}
	retry, err := fixture.application.ActivateMemoryPolicyV5(ctx, ActivateMemoryPolicyV5Options{ResidentID: fixture.residentID, ExpectedFrom: memory.PolicyVersionV5})
	if err != nil || retry.Changed || retry.RevisionID != activation.RevisionID {
		t.Fatalf("idempotent v5=%+v err=%v", retry, err)
	}
	if _, err := fixture.application.ActivateMemoryPolicyV4(ctx, ActivateMemoryPolicyV4Options{ResidentID: fixture.residentID, ExpectedFrom: memory.PolicyVersionV5}); err == nil {
		t.Fatal("v5 downgrade allowed")
	}
	_, runID := ingressRecallDialogue(t, fixture, "Resident likes tea")
	if got := recallInputClaims(t, fixture, runID); len(got) != 1 || got[0] != fixture.claimID {
		t.Fatalf("v5 relevant Recall=%v", got)
	}
}

func TestMemoryPolicyV5RequiresAcknowledgementsFromV1AndRejectsStaleFrom(t *testing.T) {
	fixture := newApplicationFixture(t, &scriptedGenerator{}, 1)
	ctx := context.Background()
	before := countMemoryPolicyRevisionsForTest(t, fixture)
	for _, options := range []ActivateMemoryPolicyV5Options{
		{ResidentID: fixture.residentID, ExpectedFrom: memory.PolicyVersionV1},
		{ResidentID: fixture.residentID, ExpectedFrom: memory.PolicyVersionV4},
	} {
		if _, err := fixture.application.ActivateMemoryPolicyV5(ctx, options); err == nil {
			t.Fatalf("invalid activation accepted: %+v", options)
		}
	}
	if after := countMemoryPolicyRevisionsForTest(t, fixture); after != before {
		t.Fatal("rejected activation wrote a revision")
	}
	result, err := fixture.application.ActivateMemoryPolicyV5(ctx, ActivateMemoryPolicyV5Options{ResidentID: fixture.residentID, ExpectedFrom: memory.PolicyVersionV1, AcknowledgeRecallEnable: true, AcknowledgeSelfTalkExtraction: true})
	if err != nil || !result.Changed || result.PolicyVersion != memory.PolicyVersionV5 {
		t.Fatalf("v1-to-v5=%+v err=%v", result, err)
	}
}
