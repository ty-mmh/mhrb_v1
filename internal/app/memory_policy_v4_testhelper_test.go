package app

import (
	"context"
	"fmt"
	"testing"

	"mahoroba.local/mahoroba/internal/canonical"
	"mahoroba.local/mahoroba/internal/domain"
	"mahoroba.local/mahoroba/internal/memory"
)

// activateMemoryPolicyV4ForTest is deliberately test-only.  Production's V2
// and V3 activation endpoints are frozen lost-response retries; current test
// fixtures must therefore enter the enabled capability set through the same
// explicit V4 boundary as an owner would.
func (a *Application) activateMemoryPolicyV4ForTest(
	ctx context.Context,
	residentID canonical.ID,
) (domain.MemoryPolicyActivationResult, error) {
	resident, err := a.repository.Resident(ctx, residentID)
	if err != nil {
		return domain.MemoryPolicyActivationResult{}, err
	}
	policy, _, err := memory.ParsePolicy([]byte(resident.MemoryPolicy))
	if err != nil {
		return domain.MemoryPolicyActivationResult{}, fmt.Errorf("test: parse active memory policy: %w", err)
	}
	return a.ActivateMemoryPolicyV4(ctx, ActivateMemoryPolicyV4Options{
		ResidentID:                    residentID,
		ExpectedFrom:                  policy.Version,
		AcknowledgeRecallEnable:       policy.Version == memory.PolicyVersionV1,
		AcknowledgeSelfTalkExtraction: policy.Version == memory.PolicyVersionV1 || policy.Version == memory.PolicyVersionV2,
	})
}

func activateMemoryPolicyV4Fixture(t *testing.T, fixture applicationFixture) domain.MemoryPolicyActivationResult {
	t.Helper()
	result, err := fixture.application.activateMemoryPolicyV4ForTest(context.Background(), fixture.residentID)
	if err != nil {
		t.Fatal(err)
	}
	return result
}

// drainForegroundToOptionalForTest completes the two bounded clean scans that
// follow a sequence of fair dialogue/memory handoffs.  It models normal worker
// turns; it deliberately does not bypass the COVR-01 foreground epoch gate.
func drainForegroundToOptionalForTest(t *testing.T, fixture applicationFixture) {
	t.Helper()
	for turn := 0; turn < 2; turn++ {
		if err := fixture.application.ProcessResident(context.Background(), fixture.residentID); err != nil {
			t.Fatalf("clean resident drain %d: %v", turn, err)
		}
	}
}
