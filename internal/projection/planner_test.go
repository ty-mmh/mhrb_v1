package projection

import (
	"errors"
	"testing"

	"mahoroba.local/mahoroba/internal/canonical"
)

var (
	projectionTestResident = mustProjectionTestID("01ARZ3NDEKTSV4RRFFQ69G5FAV")
	projectionTestPolicy   = mustProjectionTestID("01ARZ3NDEKTSV4RRFFQ69G5FAW")
	projectionTestPolicy2  = mustProjectionTestID("01ARZ3NDEKTSV4RRFFQ69G5FAX")
	projectionTestTZ       = canonical.MustTimezone("UTC")
)

func mustProjectionTestID(value string) canonical.ID {
	id, err := canonical.ParseID(value)
	if err != nil {
		panic(err)
	}
	return id
}

func projectionTestTarget(seq int64, asOf canonical.Instant) Target {
	return Target{
		Head: canonical.Head{Exists: true, CommitSeq: canonical.CommitSeq(seq), CommittedAt: asOf - 1},
		AsOf: asOf, AsOfTZ: projectionTestTZ,
	}
}

func projectionTestWatermark(definition Definition, seq int64, asOf canonical.Instant, dependencies ...Dependency) Watermark {
	return Watermark{
		ProjectionName: definition.Name, ResidentID: projectionTestResident,
		ProjectionVersion: definition.Version, SourceCommitSeq: canonical.CommitSeq(seq),
		AsOf: asOf, AsOfTZ: projectionTestTZ, Dependencies: dependencies,
	}
}

func TestM4I74MissingWatermarkMeansFullBuildWithoutSentinel(t *testing.T) {
	definition := Definition{Name: ResidentCurrentStatusName, Version: "resident-current-status-v1"}
	plan, err := PlanUpdate(PlanInput{
		Definition: definition, ResidentID: projectionTestResident, Target: projectionTestTarget(10, 200),
	})
	if err != nil {
		t.Fatalf("PlanUpdate: %v", err)
	}
	if plan.Kind != FullBuild || plan.Reason != ReasonUnbuilt || !plan.NeedCommitCatchUp {
		t.Fatalf("plan = %+v, want full build from captured head", plan)
	}
}

func TestM4I100ProjectionVersionMismatchForcesFullRebuild(t *testing.T) {
	definition := Definition{Name: ResidentCurrentStatusName, Version: "resident-current-status-v2"}
	stored := projectionTestWatermark(definition, 10, 200)
	stored.ProjectionVersion = "resident-current-status-v1"
	plan, err := PlanUpdate(PlanInput{
		Definition: definition, ResidentID: projectionTestResident, Target: projectionTestTarget(10, 200), Stored: &stored,
	})
	if err != nil {
		t.Fatalf("PlanUpdate: %v", err)
	}
	if plan.Kind != FullRebuild || plan.Reason != ReasonVersionMismatch {
		t.Fatalf("plan = %+v, want version full rebuild", plan)
	}
}

func TestM4I92DependencySetComparisonIsCompleteAndOrderIndependent(t *testing.T) {
	definition := Definition{
		Name: ClaimStatesName, Version: "claim-states-v1", TimeSensitive: true,
		Dependencies: []DependencyKind{MemoryPolicyDependency, SessionizationPolicyDependency},
	}
	memory := Dependency{Kind: MemoryPolicyDependency, VersionID: projectionTestPolicy}
	session := Dependency{Kind: SessionizationPolicyDependency, VersionID: projectionTestPolicy2}
	stored := projectionTestWatermark(definition, 10, 200, memory, session)
	plan, err := PlanUpdate(PlanInput{
		Definition: definition, ResidentID: projectionTestResident, Target: projectionTestTarget(10, 200),
		Stored: &stored, DesiredDependencies: []Dependency{session, memory},
	})
	if err != nil {
		t.Fatalf("PlanUpdate equal sets: %v", err)
	}
	if plan.Kind != UpToDate {
		t.Fatalf("equal reordered set plan = %+v", plan)
	}

	plan, err = PlanUpdate(PlanInput{
		Definition: definition, ResidentID: projectionTestResident, Target: projectionTestTarget(10, 200),
		Stored: &stored, DesiredDependencies: []Dependency{
			{Kind: MemoryPolicyDependency, VersionID: projectionTestPolicy2}, session,
		},
	})
	if err != nil {
		t.Fatalf("PlanUpdate mismatched sets: %v", err)
	}
	if plan.Kind != FullRebuild || plan.Reason != ReasonDependencyMismatch {
		t.Fatalf("mismatched set plan = %+v", plan)
	}

	for _, test := range []struct {
		name    string
		stored  []Dependency
		desired []Dependency
		wantErr error
	}{
		{name: "stored missing member", stored: []Dependency{memory}, desired: []Dependency{memory, session}},
		{name: "stored extra member", stored: []Dependency{memory, session, {Kind: MemoryPolicyDependency, VersionID: projectionTestPolicy2}}, desired: []Dependency{memory, session}},
		{name: "unresolved desired member", stored: []Dependency{memory, session}, desired: []Dependency{memory}, wantErr: ErrUnresolvedDependency},
		{name: "unknown desired kind", stored: []Dependency{memory, session}, desired: []Dependency{memory, session, {Kind: "latest_policy", VersionID: projectionTestPolicy2}}, wantErr: ErrUnknownDependency},
	} {
		t.Run(test.name, func(t *testing.T) {
			candidate := projectionTestWatermark(definition, 10, 200, test.stored...)
			plan, err := PlanUpdate(PlanInput{
				Definition: definition, ResidentID: projectionTestResident, Target: projectionTestTarget(10, 200),
				Stored: &candidate, DesiredDependencies: test.desired,
			})
			if test.wantErr != nil {
				if !errors.Is(err, test.wantErr) {
					t.Fatalf("error = %v, want %v", err, test.wantErr)
				}
				return
			}
			if err != nil || plan.Kind != FullRebuild || plan.Reason != ReasonDependencyMismatch {
				t.Fatalf("plan/error = %+v / %v", plan, err)
			}
		})
	}
}

func TestM4DependencyResolutionFailsClosed(t *testing.T) {
	definition := Definition{
		Name: ClaimStatesName, Version: "claim-states-v1",
		Dependencies: []DependencyKind{MemoryPolicyDependency},
	}
	tests := []struct {
		name         string
		dependencies []Dependency
		want         error
	}{
		{name: "missing", want: ErrUnresolvedDependency},
		{name: "unknown", dependencies: []Dependency{{Kind: "latest_policy", VersionID: projectionTestPolicy}}, want: ErrUnknownDependency},
		{name: "undeclared", dependencies: []Dependency{{Kind: SessionizationPolicyDependency, VersionID: projectionTestPolicy}}, want: nil},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, err := PlanUpdate(PlanInput{
				Definition: definition, ResidentID: projectionTestResident, Target: projectionTestTarget(10, 200),
				DesiredDependencies: test.dependencies,
			})
			if err == nil {
				t.Fatal("PlanUpdate succeeded, want fail closed")
			}
			if test.want != nil && !errors.Is(err, test.want) {
				t.Fatalf("error = %v, want %v", err, test.want)
			}
		})
	}
}

func TestCOV4ClaimStatesStoredDependencyUpgradeAndCorruptionBoundary(t *testing.T) {
	definition := ClaimStatesDefinition()
	desired := []Dependency{{Kind: MemoryPolicyDependency, VersionID: projectionTestPolicy}}
	target := projectionTestTarget(10, 200)

	legacy := projectionTestWatermark(definition, 5, 100)
	plan, err := PlanUpdate(PlanInput{
		Definition: definition, ResidentID: projectionTestResident, Target: target,
		Stored: &legacy, DesiredDependencies: desired,
	})
	if err != nil || plan.Kind != FullRebuild || plan.Reason != ReasonDependencyMismatch {
		t.Fatalf("legacy zero dependency plan/error = %+v / %v", plan, err)
	}

	corrupt := projectionTestWatermark(definition, 5, 100,
		desired[0],
		Dependency{Kind: MemoryPolicyDependency, VersionID: projectionTestPolicy2},
	)
	_, err = PlanUpdate(PlanInput{
		Definition: definition, ResidentID: projectionTestResident, Target: target,
		Stored: &corrupt, DesiredDependencies: desired,
	})
	if !errors.Is(err, ErrInvalidWatermarkMetadata) {
		t.Fatalf("two stored dependencies error = %v, want ErrInvalidWatermarkMetadata", err)
	}
}

func TestCOV4ClaimStatesValidDependencyMismatchAndTargetAdvanceRemainDistinct(t *testing.T) {
	definition := ClaimStatesDefinition()
	storedDependency := Dependency{Kind: MemoryPolicyDependency, VersionID: projectionTestPolicy}
	stored := projectionTestWatermark(definition, 5, 100, storedDependency)
	target := projectionTestTarget(10, 200)

	plan, err := PlanUpdate(PlanInput{
		Definition: definition, ResidentID: projectionTestResident, Target: target, Stored: &stored,
		DesiredDependencies: []Dependency{{Kind: MemoryPolicyDependency, VersionID: projectionTestPolicy2}},
	})
	if err != nil || plan.Kind != FullRebuild || plan.Reason != ReasonDependencyMismatch {
		t.Fatalf("valid A to B mismatch plan/error = %+v / %v", plan, err)
	}

	plan, err = PlanUpdate(PlanInput{
		Definition: definition, ResidentID: projectionTestResident, Target: target, Stored: &stored,
		DesiredDependencies: []Dependency{storedDependency},
	})
	if err != nil || plan.Kind != Update || plan.Reason != ReasonCursorAndAsOfAdvance ||
		!plan.NeedCommitCatchUp || !plan.NeedAsOfReEvaluation {
		t.Fatalf("same dependency target advance plan/error = %+v / %v", plan, err)
	}
}

func TestM4I102ClaimPolicyActivationAfterWatermarkForcesFullRebuild(t *testing.T) {
	definition := Definition{
		Name: ClaimStatesName, Version: "claim-states-v1", TimeSensitive: true,
		RebuildOnActivation: []DependencyKind{MemoryPolicyDependency},
	}
	stored := projectionTestWatermark(definition, 5, 100)
	plan, err := PlanUpdate(PlanInput{
		Definition: definition, ResidentID: projectionTestResident, Target: projectionTestTarget(10, 200),
		Stored: &stored, ActivationDetected: true,
	})
	if err != nil {
		t.Fatalf("PlanUpdate: %v", err)
	}
	if plan.Kind != FullRebuild || plan.Reason != ReasonDependencyActivation {
		t.Fatalf("plan = %+v, want activation rebuild", plan)
	}
}

func TestM4RTI29CommitCatchUpAndAsOfRefreshRemainIndependentComponents(t *testing.T) {
	definition := Definition{Name: RuntimeStatesName, Version: "runtime-states-v1", TimeSensitive: true}
	tests := []struct {
		name                   string
		storedSeq, targetSeq   int64
		storedAsOf, targetAsOf canonical.Instant
		kind                   UpdateKind
		needCommit, needAsOf   bool
	}{
		{name: "both", storedSeq: 5, targetSeq: 10, storedAsOf: 100, targetAsOf: 200, kind: Update, needCommit: true, needAsOf: true},
		{name: "commit_only", storedSeq: 5, targetSeq: 10, storedAsOf: 200, targetAsOf: 200, kind: Update, needCommit: true},
		{name: "as_of_only", storedSeq: 10, targetSeq: 10, storedAsOf: 100, targetAsOf: 200, kind: Update, needAsOf: true},
		{name: "current", storedSeq: 10, targetSeq: 10, storedAsOf: 200, targetAsOf: 200, kind: UpToDate},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			stored := projectionTestWatermark(definition, test.storedSeq, test.storedAsOf)
			plan, err := PlanUpdate(PlanInput{
				Definition: definition, ResidentID: projectionTestResident,
				Target: projectionTestTarget(test.targetSeq, test.targetAsOf), Stored: &stored,
			})
			if err != nil {
				t.Fatalf("PlanUpdate: %v", err)
			}
			if plan.Kind != test.kind || plan.NeedCommitCatchUp != test.needCommit || plan.NeedAsOfReEvaluation != test.needAsOf {
				t.Fatalf("plan = %+v", plan)
			}
		})
	}
}

func TestM4RTI29WatermarkAdvancesOnlyComponentsActuallyEvaluated(t *testing.T) {
	definition := Definition{Name: RuntimeStatesName, Version: "runtime-states-v1", TimeSensitive: true}
	stored := projectionTestWatermark(definition, 5, 100)
	target := projectionTestTarget(10, 200)

	commitOnly := UpdatePlan{Kind: Update, Reason: ReasonCursorAdvance, NeedCommitCatchUp: true}
	watermark := targetWatermark(PlanInput{
		Definition: definition, ResidentID: projectionTestResident, Target: target, Stored: &stored,
	}, commitOnly)
	if watermark.SourceCommitSeq != 10 || watermark.AsOf != 100 {
		t.Fatalf("commit-only watermark = %+v", watermark)
	}

	asOfOnly := UpdatePlan{Kind: Update, Reason: ReasonAsOfAdvance, NeedAsOfReEvaluation: true}
	watermark = targetWatermark(PlanInput{
		Definition: definition, ResidentID: projectionTestResident, Target: target, Stored: &stored,
	}, asOfOnly)
	if watermark.SourceCommitSeq != 5 || watermark.AsOf != 200 {
		t.Fatalf("as-of-only watermark = %+v", watermark)
	}
}

func TestM4I103PersistentAsOfRegressionFailsClosed(t *testing.T) {
	definition := Definition{Name: ResidentCurrentStatusName, Version: "resident-current-status-v1"}
	stored := projectionTestWatermark(definition, 5, 200)
	_, err := PlanUpdate(PlanInput{
		Definition: definition, ResidentID: projectionTestResident,
		Target: projectionTestTarget(10, 199), Stored: &stored,
	})
	if !errors.Is(err, ErrAsOfRegression) {
		t.Fatalf("error = %v, want ErrAsOfRegression", err)
	}
}

func TestM4CanonicalHeadRegressionFailsClosed(t *testing.T) {
	definition := Definition{Name: ResidentCurrentStatusName, Version: "resident-current-status-v1"}
	stored := projectionTestWatermark(definition, 10, 100)
	_, err := PlanUpdate(PlanInput{
		Definition: definition, ResidentID: projectionTestResident,
		Target: projectionTestTarget(9, 200), Stored: &stored,
	})
	if !errors.Is(err, ErrSourceRegression) {
		t.Fatalf("error = %v, want ErrSourceRegression", err)
	}
}

func TestM4UndeclaredProjectionFailsClosed(t *testing.T) {
	registry, err := NewRegistry(Definition{Name: RuntimeStatesName, Version: "runtime-states-v1"})
	if err != nil {
		t.Fatal(err)
	}
	_, err = registry.Definition("content_references")
	if !errors.Is(err, ErrUndeclaredProjection) {
		t.Fatalf("error = %v, want ErrUndeclaredProjection", err)
	}
}
