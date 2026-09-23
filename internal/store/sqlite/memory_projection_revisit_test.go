package sqlite

import (
	"context"
	"testing"

	"mahoroba.local/mahoroba/internal/canonical"
	"mahoroba.local/mahoroba/internal/projection"
)

func TestM5I102MemoryPolicyActivationForcesProductionClaimStatesFullRebuild(t *testing.T) {
	fixture, closeFixture := newSemanticFixture(t)
	defer closeFixture()
	ctx := context.Background()
	residentID := projectionTestID(t, fixture.resident["A"])
	activationCommitID := fixture.ids.new()
	mustExec(t, fixture.db, `INSERT INTO canonical_commits(
		canonical_commit_id, commit_seq, resident_id, committed_at, committed_tz
	) VALUES (?, 4, ?, ?, ?)`, activationCommitID, residentID.String(), semanticTime+4, semanticTZ)
	mustExec(t, fixture.db, `INSERT INTO resident_revision_activations(
		activation_id, canonical_commit_id, resident_id, revision_id, actor_principal_id,
		approval_id, reason_code, reason_content_id, recorded_at, recorded_tz
	) VALUES (?, ?, ?, ?, ?, NULL, 'activate-memory-policy', NULL, ?, ?)`, fixture.ids.new(), activationCommitID,
		residentID.String(), fixture.revision["A"]["memory_policy"], fixture.principal["human"], semanticTime+4, semanticTZ)

	repository := &ProjectionRepository{store: &Store{reader: fixture.db}}
	after, through := mustProjectionCommitSeq(t, 2), mustProjectionCommitSeq(t, 4)
	activated, err := repository.DependencyActivated(ctx, projection.ActivationRequest{
		Definition: projection.ClaimStatesDefinition(), Kind: projection.MemoryPolicyDependency,
		ResidentID: residentID, After: after, Through: through,
	})
	if err != nil || !activated {
		t.Fatalf("production activation scan=%v error=%v", activated, err)
	}

	definition := projection.ClaimStatesDefinition()
	policyID := projectionTestID(t, fixture.revision["A"]["memory_policy"])
	stored := projection.Watermark{
		ProjectionName: definition.Name, ResidentID: residentID, ProjectionVersion: definition.Version,
		SourceCommitSeq: after, AsOf: 100, AsOfTZ: canonical.MustTimezone("UTC"),
		Dependencies: []projection.Dependency{{Kind: projection.MemoryPolicyDependency, VersionID: policyID}},
	}
	plan, err := projection.PlanUpdate(projection.PlanInput{
		Definition: definition, ResidentID: residentID,
		Target: projection.Target{
			Head: canonical.Head{Exists: true, CommitSeq: through, CommittedAt: 104},
			AsOf: 200, AsOfTZ: canonical.MustTimezone("UTC"),
		},
		Stored: &stored, DesiredDependencies: stored.Dependencies,
		ActivationDetected: activated,
	})
	if err != nil {
		t.Fatal(err)
	}
	if plan.Kind != projection.FullRebuild || plan.Reason != projection.ReasonDependencyActivation {
		t.Fatalf("production activation plan=%+v", plan)
	}
}
