package app

import (
	"context"
	"strings"
	"testing"

	"mahoroba.local/mahoroba/internal/memory"
)

func TestM5I21MemoryPolicyChangesRequireExplicitOwnerAdminActivation(t *testing.T) {
	fixture := newApplicationFixture(t, &scriptedGenerator{}, 2)
	ctx := context.Background()

	before, err := fixture.repository.Resident(ctx, fixture.residentID)
	if err != nil {
		t.Fatal(err)
	}
	legacy, _, err := memory.ParsePolicy([]byte(before.MemoryPolicy))
	if err != nil {
		t.Fatalf("parse bootstrap memory policy %q: %v", before.MemoryPolicy, err)
	}
	if legacy.Version != memory.PolicyVersionV1 || legacy.MemoryRecallEnabled {
		t.Fatalf("bootstrap memory policy = %+v, want disabled v1", legacy)
	}

	activated, err := fixture.application.ActivateMemoryPolicyV4(ctx, ActivateMemoryPolicyV4Options{
		ResidentID: fixture.residentID, ExpectedFrom: memory.PolicyVersionV1,
		AcknowledgeRecallEnable: true, AcknowledgeSelfTalkExtraction: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	if !activated.Changed || activated.RevisionID.IsZero() || activated.ActivationID.IsZero() {
		t.Fatalf("activation result = %+v", activated)
	}
	after, err := fixture.repository.Resident(ctx, fixture.residentID)
	if err != nil {
		t.Fatal(err)
	}
	policy, _, err := memory.ParsePolicy([]byte(after.MemoryPolicy))
	if err != nil {
		t.Fatal(err)
	}
	if policy.Version != memory.PolicyVersionV4 || policy.RenderingVersion != memory.RenderingVersionV2 {
		t.Fatalf("active policy is not fixed COV-6 v4: %+v", policy)
	}

	retry, err := fixture.application.ActivateMemoryPolicyV4(ctx, ActivateMemoryPolicyV4Options{
		ResidentID: fixture.residentID, ExpectedFrom: memory.PolicyVersionV4,
	})
	if err != nil {
		t.Fatal(err)
	}
	if retry.Changed || retry.RevisionID != activated.RevisionID {
		t.Fatalf("idempotent activation = %+v, first = %+v", retry, activated)
	}
	if activated.PreviousPolicyVersion != memory.PolicyVersionV1 ||
		activated.PolicyVersion != memory.PolicyVersionV4 || activated.RenderingVersion != memory.RenderingVersionV2 {
		t.Fatalf("activation version result = %+v", activated)
	}
	if _, err := fixture.application.ActivateMemoryPolicyV0(ctx, fixture.residentID); err == nil ||
		!strings.Contains(err.Error(), "use ActivateMemoryPolicyV4") {
		t.Fatalf("legacy activation did not reject downgrade: %v", err)
	}
}

func TestCOV6MemoryPolicyV4WriterRejectsMissingAckAndStaleExpectedFrom(t *testing.T) {
	fixture := newApplicationFixture(t, &scriptedGenerator{}, 2)
	ctx := context.Background()
	resident, err := fixture.repository.Resident(ctx, fixture.residentID)
	if err != nil {
		t.Fatal(err)
	}
	before := countMemoryPolicyRevisionsForTest(t, fixture)
	for _, transition := range []memoryPolicyActivationTransition{
		{expectedFrom: memory.PolicyVersionV1},
		{expectedFrom: memory.PolicyVersionV2, acknowledgeRecallEnable: true, acknowledgeSelfTalkExtraction: true},
	} {
		if _, err := fixture.application.activateMemoryPolicy(
			ctx, resident, memory.DefaultPolicyV4(), transition,
		); err == nil {
			t.Fatalf("writer accepted invalid transition %+v", transition)
		}
		if after := countMemoryPolicyRevisionsForTest(t, fixture); after != before {
			t.Fatalf("rejected transition changed policy revisions from %d to %d", before, after)
		}
	}
}

func countMemoryPolicyRevisionsForTest(t *testing.T, fixture applicationFixture) int {
	t.Helper()
	var count int
	if err := fixture.store.Reader().QueryRow(`SELECT COUNT(*) FROM resident_revisions
		WHERE resident_id = ? AND revision_class = 'memory_policy'`, fixture.residentID.String()).Scan(&count); err != nil {
		t.Fatal(err)
	}
	return count
}
