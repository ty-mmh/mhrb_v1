package projection

import (
	"context"
	"errors"
	"reflect"
	"sync"
	"testing"
	"time"

	"mahoroba.local/mahoroba/internal/canonical"
	"mahoroba.local/mahoroba/internal/operationalmetrics"
)

type projectionTestClock struct {
	mu  sync.Mutex
	now time.Time
}

func (clock *projectionTestClock) Now() time.Time {
	clock.mu.Lock()
	defer clock.mu.Unlock()
	return clock.now
}

func (clock *projectionTestClock) Set(now time.Time) {
	clock.mu.Lock()
	clock.now = now
	clock.mu.Unlock()
}

type projectionFakeSource struct {
	mu sync.Mutex

	head                  canonical.Head
	residents             []canonical.ID
	dependencies          map[Name][]Dependency
	activation            bool
	evaluateErr           error
	advanceHeadOnEvaluate bool

	residentTargets []Target
	evaluations     []EvaluationRequest
	activationCalls int
	evaluated       chan struct{}
}

func (source *projectionFakeSource) Head(context.Context) (canonical.Head, error) {
	source.mu.Lock()
	defer source.mu.Unlock()
	return source.head, nil
}

func (source *projectionFakeSource) Residents(_ context.Context, target Target) ([]canonical.ID, error) {
	source.mu.Lock()
	defer source.mu.Unlock()
	source.residentTargets = append(source.residentTargets, target)
	return append([]canonical.ID(nil), source.residents...), nil
}

func (source *projectionFakeSource) ResolveDependencies(_ context.Context, request DependencyRequest) ([]Dependency, error) {
	source.mu.Lock()
	defer source.mu.Unlock()
	return append([]Dependency(nil), source.dependencies[request.Definition.Name]...), nil
}

func (source *projectionFakeSource) DependencyActivated(context.Context, ActivationRequest) (bool, error) {
	source.mu.Lock()
	defer source.mu.Unlock()
	source.activationCalls++
	return source.activation, nil
}

func (source *projectionFakeSource) Evaluate(_ context.Context, request EvaluationRequest) (Evaluation, error) {
	source.mu.Lock()
	defer source.mu.Unlock()
	source.evaluations = append(source.evaluations, request)
	if source.advanceHeadOnEvaluate {
		source.head.CommitSeq++
		source.head.CommittedAt++
		source.advanceHeadOnEvaluate = false
	}
	if source.evaluated != nil {
		select {
		case source.evaluated <- struct{}{}:
		default:
		}
	}
	return Evaluation{Value: request.Plan}, source.evaluateErr
}

func TestM7ProjectionPreflightUsesOneCapturedHeadForRequiredSet(t *testing.T) {
	definitions := []Definition{
		{Name: ResidentCurrentStatusName, Version: "resident-current-status-v1"},
		{Name: RuntimeStatesName, Version: "runtime-states-v1", TimeSensitive: true},
	}
	source, store, clock := projectionCoordinatorFixtures(definitions[0])
	registry, err := NewRegistry(definitions...)
	if err != nil {
		t.Fatal(err)
	}
	coordinator, err := NewCoordinator(CoordinatorOptions{
		Registry: registry, Source: source, Store: store, Clock: clock, Timezone: projectionTestTZ,
		ScanInterval: time.Hour, AsOfRefreshInterval: time.Hour,
		RebuildRetryInterval: time.Minute, MaxStaleness: 5 * time.Minute,
	})
	if err != nil {
		t.Fatal(err)
	}
	result, err := coordinator.PreflightResident(context.Background(), projectionTestResident,
		[]Name{RuntimeStatesName, ResidentCurrentStatusName})
	if err != nil {
		t.Fatalf("PreflightResident: %v", err)
	}
	if result.Target.Head.CommitSeq != 10 || result.Target.AsOf != 200 ||
		!reflect.DeepEqual(result.Names, []Name{ResidentCurrentStatusName, RuntimeStatesName}) {
		t.Fatalf("preflight result = %+v", result)
	}
	source.mu.Lock()
	if len(source.evaluations) != 2 {
		source.mu.Unlock()
		t.Fatalf("evaluations = %d, want 2", len(source.evaluations))
	}
	for _, evaluation := range source.evaluations {
		if evaluation.Target != result.Target {
			source.mu.Unlock()
			t.Fatalf("evaluation target = %+v, want %+v", evaluation.Target, result.Target)
		}
	}
	source.mu.Unlock()
	for _, name := range result.Names {
		watermark, exists, err := store.Watermark(context.Background(), name, projectionTestResident)
		if err != nil || !exists || watermark.SourceCommitSeq != 10 {
			t.Fatalf("watermark %s = %+v/%v/%v", name, watermark, exists, err)
		}
	}
}

func TestM7ProjectionPreflightRejectsCanonicalHeadRace(t *testing.T) {
	definition := Definition{Name: ResidentCurrentStatusName, Version: "resident-current-status-v1"}
	source, store, clock := projectionCoordinatorFixtures(definition)
	source.advanceHeadOnEvaluate = true
	coordinator := newProjectionTestCoordinator(t, definition, source, store, clock)
	result, err := coordinator.PreflightResident(context.Background(), projectionTestResident, []Name{definition.Name})
	if !errors.Is(err, ErrPreflightHeadChanged) {
		t.Fatalf("PreflightResident = %+v, %v, want ErrPreflightHeadChanged", result, err)
	}
	watermark, exists, watermarkErr := store.Watermark(context.Background(), definition.Name, projectionTestResident)
	if watermarkErr != nil || !exists || watermark.SourceCommitSeq != result.Target.Head.CommitSeq {
		t.Fatalf("bounded derived watermark = %+v/%v/%v", watermark, exists, watermarkErr)
	}
}

func TestM7ContentReferencesPreflightUsesOneHeadForAllResidents(t *testing.T) {
	definition := ContentReferencesDefinition()
	source, store, clock := projectionCoordinatorFixtures(definition)
	second, err := canonical.ParseID("01J00000000000000000000009")
	if err != nil {
		t.Fatal(err)
	}
	source.residents = []canonical.ID{projectionTestResident, second}
	coordinator := newProjectionTestCoordinator(t, definition, source, store, clock)
	results, err := coordinator.PreflightContentReferences(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(results) != 2 || results[0].ResidentID != projectionTestResident || results[1].ResidentID != second {
		t.Fatalf("all-resident results = %+v", results)
	}
	source.mu.Lock()
	defer source.mu.Unlock()
	if len(source.residentTargets) != 1 || len(source.evaluations) != 2 {
		t.Fatalf("resident/evaluation calls = %d/%d", len(source.residentTargets), len(source.evaluations))
	}
	for _, evaluation := range source.evaluations {
		if evaluation.Target != results[0].Target || evaluation.Definition.Name != definition.Name ||
			evaluation.Definition.Version != definition.Version || len(evaluation.Definition.Dependencies) != 0 {
			t.Fatalf("evaluation not bound to shared target/definition: %+v", evaluation)
		}
	}
}

func TestM7ProjectionCoordinatorReadyReportsStartupFailure(t *testing.T) {
	definition := Definition{Name: ResidentCurrentStatusName, Version: "resident-current-status-v1"}
	source, store, clock := projectionCoordinatorFixtures(definition)
	want := errors.New("injected startup evaluator failure")
	source.evaluateErr = want
	coordinator := newProjectionTestCoordinator(t, definition, source, store, clock)
	if err := coordinator.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := coordinator.WaitReady(context.Background()); !errors.Is(err, want) {
		t.Fatalf("WaitReady = %v, want startup failure", err)
	}
	coordinator.Stop()
	waitCtx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := coordinator.Wait(waitCtx); err != nil {
		t.Fatal(err)
	}
}

type projectionFakeStore struct {
	mu sync.Mutex

	watermarks  map[targetKey]Watermark
	applies     []ApplyRequest
	casFailures int
	applyErr    error
	applyErrors map[targetKey]error
}

func (store *projectionFakeStore) Watermark(_ context.Context, name Name, residentID canonical.ID) (Watermark, bool, error) {
	store.mu.Lock()
	defer store.mu.Unlock()
	watermark, exists := store.watermarks[targetKey{name: name, residentID: residentID}]
	return watermark.clone(), exists, nil
}

func (store *projectionFakeStore) Apply(_ context.Context, request ApplyRequest) error {
	store.mu.Lock()
	defer store.mu.Unlock()
	store.applies = append(store.applies, request)
	key := targetKey{name: request.Definition.Name, residentID: request.ResidentID}
	if store.casFailures > 0 {
		store.casFailures--
		competing := request.Watermark.clone()
		if request.Observed != nil {
			competing.SourceCommitSeq = request.Observed.SourceCommitSeq + 1
		}
		store.watermarks[key] = competing
		return ErrCASConflict
	}
	if store.applyErr != nil {
		return store.applyErr
	}
	if err := store.applyErrors[key]; err != nil {
		return err
	}
	store.watermarks[key] = request.Watermark.clone()
	return nil
}

func (store *projectionFakeStore) Drop(_ context.Context, request DropRequest) error {
	store.mu.Lock()
	defer store.mu.Unlock()
	delete(store.watermarks, targetKey{name: request.Definition.Name, residentID: request.ResidentID})
	return nil
}

func newProjectionTestCoordinator(t *testing.T, definition Definition, source *projectionFakeSource, store *projectionFakeStore, clock *projectionTestClock) *Coordinator {
	t.Helper()
	registry, err := NewRegistry(definition)
	if err != nil {
		t.Fatalf("NewRegistry: %v", err)
	}
	coordinator, err := NewCoordinator(CoordinatorOptions{
		Registry: registry, Source: source, Store: store, Clock: clock, Timezone: projectionTestTZ,
		ScanInterval: time.Hour, AsOfRefreshInterval: time.Hour,
		RebuildRetryInterval: time.Minute, MaxStaleness: 5 * time.Minute,
		NotificationCapacity: 1,
	})
	if err != nil {
		t.Fatalf("NewCoordinator: %v", err)
	}
	return coordinator
}

func projectionCoordinatorFixtures(definition Definition) (*projectionFakeSource, *projectionFakeStore, *projectionTestClock) {
	clock := &projectionTestClock{now: time.UnixMicro(200).UTC()}
	source := &projectionFakeSource{
		head:      canonical.Head{Exists: true, CommitSeq: 10, CommittedAt: 150},
		residents: []canonical.ID{projectionTestResident}, dependencies: make(map[Name][]Dependency),
	}
	store := &projectionFakeStore{watermarks: make(map[targetKey]Watermark)}
	return source, store, clock
}

func TestM7ProjectionOperationalObserverRecordsLagRebuildAndFailure(t *testing.T) {
	definition := Definition{Name: ResidentCurrentStatusName, Version: "resident-current-status-v1"}
	source, store, clock := projectionCoordinatorFixtures(definition)
	metrics := operationalmetrics.New()
	registry, err := NewRegistry(definition)
	if err != nil {
		t.Fatal(err)
	}
	coordinator, err := NewCoordinator(CoordinatorOptions{
		Registry: registry, Source: source, Store: store, Clock: clock, Timezone: projectionTestTZ,
		ScanInterval: time.Hour, AsOfRefreshInterval: time.Hour,
		RebuildRetryInterval: time.Minute, MaxStaleness: 5 * time.Minute,
		Observer: metrics,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := coordinator.ReconcileResident(context.Background(), projectionTestResident); err != nil {
		t.Fatal(err)
	}
	snapshot := metrics.Snapshot().Projection
	if snapshot.ObservedTotal != 1 || snapshot.CurrentCommitLag != 10 || snapshot.MaximumCommitLag != 10 ||
		snapshot.RebuildTotal != 1 || snapshot.FailureTotal != 0 {
		t.Fatalf("projection metrics after build = %+v", snapshot)
	}

	source.head.CommitSeq = 12
	store.applyErr = errors.New("closed projection failure")
	if err := coordinator.ReconcileResident(context.Background(), projectionTestResident); err == nil {
		t.Fatal("reconcile unexpectedly succeeded")
	}
	snapshot = metrics.Snapshot().Projection
	if snapshot.ObservedTotal != 2 || snapshot.CurrentCommitLag != 2 || snapshot.MaximumCommitLag != 10 ||
		snapshot.RebuildTotal != 1 || snapshot.FailureTotal != 1 {
		t.Fatalf("projection metrics after failure = %+v", snapshot)
	}
}

func TestM4I75CoordinatorBoundsResidentAndEvaluationReadsToCapturedHead(t *testing.T) {
	definition := Definition{Name: ResidentCurrentStatusName, Version: "resident-current-status-v1"}
	source, store, clock := projectionCoordinatorFixtures(definition)
	coordinator := newProjectionTestCoordinator(t, definition, source, store, clock)
	if err := coordinator.Reconcile(context.Background()); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	source.mu.Lock()
	defer source.mu.Unlock()
	if len(source.residentTargets) != 1 || source.residentTargets[0].Head.CommitSeq != 10 {
		t.Fatalf("resident targets = %+v", source.residentTargets)
	}
	if len(source.evaluations) != 1 {
		t.Fatalf("evaluations = %d, want 1", len(source.evaluations))
	}
	request := source.evaluations[0]
	if request.Target.Head.CommitSeq != 10 || request.Target.AsOf != 200 {
		t.Fatalf("captured evaluation target = %+v", request.Target)
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	if len(store.applies) != 1 || store.applies[0].Watermark.SourceCommitSeq != 10 {
		t.Fatalf("applies = %+v", store.applies)
	}
}

func TestM4RTI29CoordinatorAppliesCombinedPlanOnce(t *testing.T) {
	definition := Definition{Name: RuntimeStatesName, Version: "runtime-states-v1", TimeSensitive: true}
	source, store, clock := projectionCoordinatorFixtures(definition)
	store.watermarks[targetKey{name: definition.Name, residentID: projectionTestResident}] = projectionTestWatermark(definition, 5, 100)
	coordinator := newProjectionTestCoordinator(t, definition, source, store, clock)
	if err := coordinator.ReconcileResident(context.Background(), projectionTestResident); err != nil {
		t.Fatalf("ReconcileResident: %v", err)
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	if len(store.applies) != 1 {
		t.Fatalf("apply count = %d, want one transaction request", len(store.applies))
	}
	apply := store.applies[0]
	if !apply.Plan.NeedCommitCatchUp || !apply.Plan.NeedAsOfReEvaluation {
		t.Fatalf("plan = %+v, want both components", apply.Plan)
	}
	if apply.Watermark.SourceCommitSeq != 10 || apply.Watermark.AsOf != 200 {
		t.Fatalf("watermark = %+v", apply.Watermark)
	}
}

func TestCOV4CoordinatorUpgradesLegacyClaimStatesWatermarkToExactDependency(t *testing.T) {
	definition := ClaimStatesDefinition()
	source, store, clock := projectionCoordinatorFixtures(definition)
	desired := Dependency{Kind: MemoryPolicyDependency, VersionID: projectionTestPolicy}
	source.dependencies[definition.Name] = []Dependency{desired}
	store.watermarks[targetKey{name: definition.Name, residentID: projectionTestResident}] =
		projectionTestWatermark(definition, 5, 100)

	coordinator := newProjectionTestCoordinator(t, definition, source, store, clock)
	if err := coordinator.ReconcileResident(context.Background(), projectionTestResident); err != nil {
		t.Fatalf("ReconcileResident: %v", err)
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	if len(store.applies) != 1 || store.applies[0].Plan.Kind != FullRebuild ||
		store.applies[0].Plan.Reason != ReasonDependencyMismatch {
		t.Fatalf("legacy upgrade applies = %+v", store.applies)
	}
	watermark := store.watermarks[targetKey{name: definition.Name, residentID: projectionTestResident}]
	if len(watermark.Dependencies) != 1 || watermark.Dependencies[0] != desired {
		t.Fatalf("upgraded watermark dependencies = %+v", watermark.Dependencies)
	}
}

func TestM4WatermarkCASConflictRollsBackAndReplans(t *testing.T) {
	definition := Definition{Name: ResidentCurrentStatusName, Version: "resident-current-status-v1"}
	source, store, clock := projectionCoordinatorFixtures(definition)
	store.watermarks[targetKey{name: definition.Name, residentID: projectionTestResident}] = projectionTestWatermark(definition, 5, 100)
	store.casFailures = 1
	coordinator := newProjectionTestCoordinator(t, definition, source, store, clock)
	if err := coordinator.ReconcileResident(context.Background(), projectionTestResident); err != nil {
		t.Fatalf("ReconcileResident: %v", err)
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	if len(store.applies) != 2 {
		t.Fatalf("apply count = %d, want CAS retry", len(store.applies))
	}
	if store.applies[0].Observed.SourceCommitSeq != 5 || store.applies[1].Observed.SourceCommitSeq != 6 {
		t.Fatalf("observed cursors = %d, %d", store.applies[0].Observed.SourceCommitSeq, store.applies[1].Observed.SourceCommitSeq)
	}
}

func TestM4DecisionOrderSkipsActivationScanWhenVersionAlreadyRequiresRebuild(t *testing.T) {
	definition := Definition{
		Name: ClaimStatesName, Version: "claim-states-v2", TimeSensitive: true,
		RebuildOnActivation: []DependencyKind{MemoryPolicyDependency},
	}
	source, store, clock := projectionCoordinatorFixtures(definition)
	stored := projectionTestWatermark(definition, 5, 100)
	stored.ProjectionVersion = "claim-states-v1"
	store.watermarks[targetKey{name: definition.Name, residentID: projectionTestResident}] = stored
	source.activation = true
	coordinator := newProjectionTestCoordinator(t, definition, source, store, clock)
	if err := coordinator.ReconcileResident(context.Background(), projectionTestResident); err != nil {
		t.Fatalf("ReconcileResident: %v", err)
	}
	source.mu.Lock()
	defer source.mu.Unlock()
	if source.activationCalls != 0 {
		t.Fatalf("activation calls = %d, want decision-order short circuit", source.activationCalls)
	}
}

func TestM4CoordinatorRebuildRejectsUnregisteredProjection(t *testing.T) {
	definition := Definition{Name: ResidentCurrentStatusName, Version: "resident-current-status-v1"}
	source, store, clock := projectionCoordinatorFixtures(definition)
	coordinator := newProjectionTestCoordinator(t, definition, source, store, clock)
	err := coordinator.Rebuild(context.Background(), projectionTestResident, "content_references")
	if !errors.Is(err, ErrUndeclaredProjection) {
		t.Fatalf("error = %v, want ErrUndeclaredProjection", err)
	}
}

func TestM4CoordinatorNotificationIsNonBlockingAndLossTolerant(t *testing.T) {
	definition := Definition{Name: ResidentCurrentStatusName, Version: "resident-current-status-v1"}
	source, store, clock := projectionCoordinatorFixtures(definition)
	coordinator := newProjectionTestCoordinator(t, definition, source, store, clock)
	metadata := canonical.CommitMetadata{
		CommitID: projectionTestPolicy, CommitSeq: 1, Scope: canonical.GlobalScope(),
		CommittedAt: 100, CommittedTZ: projectionTestTZ,
	}
	if !coordinator.NotifyCommit(metadata) {
		t.Fatal("first notification was not accepted")
	}
	if coordinator.NotifyCommit(metadata) {
		t.Fatal("second notification was accepted despite a full bounded queue")
	}
}

func TestM4CoordinatorStartupReconcileIsAsyncAndShutdownJoins(t *testing.T) {
	definition := Definition{Name: ResidentCurrentStatusName, Version: "resident-current-status-v1"}
	source, store, clock := projectionCoordinatorFixtures(definition)
	source.evaluated = make(chan struct{}, 1)
	coordinator := newProjectionTestCoordinator(t, definition, source, store, clock)
	if err := coordinator.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	select {
	case <-source.evaluated:
	case <-time.After(2 * time.Second):
		t.Fatal("startup reconcile did not run")
	}
	coordinator.Stop()
	waitCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := coordinator.Wait(waitCtx); err != nil {
		t.Fatalf("Wait: %v", err)
	}
}

func TestM4ProjectionStatusReportsLagStalenessAndBlockingFailure(t *testing.T) {
	definition := Definition{Name: ResidentCurrentStatusName, Version: "resident-current-status-v1"}
	source, store, clock := projectionCoordinatorFixtures(definition)
	store.watermarks[targetKey{name: definition.Name, residentID: projectionTestResident}] = projectionTestWatermark(definition, 5, canonical.Instant(time.UnixMicro(200).Add(-10*time.Minute).UnixMicro()))
	store.applyErr = errors.New("projection body write failed")
	coordinator := newProjectionTestCoordinator(t, definition, source, store, clock)
	if err := coordinator.ReconcileResident(context.Background(), projectionTestResident); err == nil {
		t.Fatal("ReconcileResident succeeded, want injected failure")
	}
	statuses, err := coordinator.Status(context.Background(), StatusFilter{ResidentID: &projectionTestResident})
	if err != nil {
		t.Fatalf("Status: %v", err)
	}
	if len(statuses) != 1 {
		t.Fatalf("status count = %d", len(statuses))
	}
	status := statuses[0]
	if status.CommitLag != 5 || status.UpToDate || !status.Stale || status.BlockingReason == "" {
		t.Fatalf("status = %+v", status)
	}
}

func TestSchedulingIntervalsDoNotChangeProjectionResult(t *testing.T) {
	definition := Definition{Name: RuntimeStatesName, Version: "runtime-states-v1", TimeSensitive: true}
	run := func(override ScheduleOverride) ApplyRequest {
		source, store, clock := projectionCoordinatorFixtures(definition)
		registry, err := NewRegistry(definition)
		if err != nil {
			t.Fatal(err)
		}
		coordinator, err := NewCoordinator(CoordinatorOptions{
			Registry: registry, Source: source, Store: store, Clock: clock, Timezone: projectionTestTZ,
			ScanInterval: time.Hour, AsOfRefreshInterval: time.Hour, RebuildRetryInterval: time.Hour,
			MaxStaleness: time.Hour, Overrides: map[Name]ScheduleOverride{definition.Name: override},
		})
		if err != nil {
			t.Fatal(err)
		}
		if err := coordinator.ReconcileResident(context.Background(), projectionTestResident); err != nil {
			t.Fatal(err)
		}
		return store.applies[0]
	}
	fast := run(ScheduleOverride{
		ScanInterval: time.Millisecond, AsOfRefreshInterval: 2 * time.Millisecond,
		RebuildRetryInterval: 3 * time.Millisecond, MaxStaleness: 4 * time.Millisecond,
	})
	slow := run(ScheduleOverride{
		ScanInterval: 24 * time.Hour, AsOfRefreshInterval: 48 * time.Hour,
		RebuildRetryInterval: 72 * time.Hour, MaxStaleness: 96 * time.Hour,
	})
	if !reflect.DeepEqual(fast.Plan, slow.Plan) || !reflect.DeepEqual(fast.Evaluation, slow.Evaluation) || !watermarksEqualForTest(fast.Watermark, slow.Watermark) {
		t.Fatalf("scheduling changed meaning:\nfast=%+v\nslow=%+v", fast, slow)
	}
}

func TestCoordinatorScanDoesNotBypassAsOfRefreshSchedule(t *testing.T) {
	definition := Definition{Name: RuntimeStatesName, Version: "runtime-states-v1", TimeSensitive: true}
	source, store, clock := projectionCoordinatorFixtures(definition)
	coordinator := newProjectionTestCoordinator(t, definition, source, store, clock)
	if err := coordinator.ReconcileResident(context.Background(), projectionTestResident); err != nil {
		t.Fatal(err)
	}
	clock.Set(time.UnixMicro(300).UTC())
	name := definition.Name
	if err := coordinator.reconcile(context.Background(), &projectionTestResident, false, false, &name, false); err != nil {
		t.Fatal(err)
	}
	store.mu.Lock()
	if len(store.applies) != 1 || store.applies[0].Watermark.AsOf != 200 {
		store.mu.Unlock()
		t.Fatalf("scan advanced as_of: %+v", store.applies)
	}
	store.mu.Unlock()
	if err := coordinator.reconcile(context.Background(), &projectionTestResident, true, false, &name, true); err != nil {
		t.Fatal(err)
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	if len(store.applies) != 2 || store.applies[1].Watermark.AsOf != 300 {
		t.Fatalf("as-of refresh did not advance cursor: %+v", store.applies)
	}
}

func TestCoordinatorRetriesAtFailureRelativeDeadline(t *testing.T) {
	definition := Definition{Name: ResidentCurrentStatusName, Version: "resident-current-status-v1"}
	source, store, clock := projectionCoordinatorFixtures(definition)
	source.evaluateErr = errors.New("injected evaluation failure")
	source.evaluated = make(chan struct{}, 4)
	registry, err := NewRegistry(definition)
	if err != nil {
		t.Fatal(err)
	}
	const retryInterval = 40 * time.Millisecond
	coordinator, err := NewCoordinator(CoordinatorOptions{
		Registry: registry, Source: source, Store: store, Clock: clock, Timezone: projectionTestTZ,
		ScanInterval: time.Hour, AsOfRefreshInterval: time.Hour,
		RebuildRetryInterval: retryInterval, MaxStaleness: time.Hour,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := coordinator.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		coordinator.Stop()
		_ = coordinator.Wait(context.Background())
	})
	select {
	case <-source.evaluated:
	case <-time.After(time.Second):
		t.Fatal("startup evaluation did not run")
	}
	source.mu.Lock()
	source.evaluateErr = nil
	source.mu.Unlock()
	started := time.Now()
	select {
	case <-source.evaluated:
	case <-time.After(500 * time.Millisecond):
		t.Fatal("failure-relative retry did not run")
	}
	if elapsed := time.Since(started); elapsed < retryInterval/2 {
		t.Fatalf("retry ran before its failure-relative delay: %v", elapsed)
	}
}

func TestResidentSerializationCapturesTargetAfterWaiting(t *testing.T) {
	definition := Definition{Name: ResidentCurrentStatusName, Version: "resident-current-status-v1"}
	source, store, clock := projectionCoordinatorFixtures(definition)
	coordinator := newProjectionTestCoordinator(t, definition, source, store, clock)
	lock := coordinator.residentLock(projectionTestResident)
	lock.Lock()
	result := make(chan error, 1)
	go func() { result <- coordinator.ReconcileResident(context.Background(), projectionTestResident) }()
	time.Sleep(20 * time.Millisecond)
	source.mu.Lock()
	source.head.CommitSeq = 11
	source.mu.Unlock()
	store.mu.Lock()
	store.watermarks[targetKey{name: definition.Name, residentID: projectionTestResident}] = projectionTestWatermark(definition, 11, 100)
	store.mu.Unlock()
	lock.Unlock()
	if err := <-result; err != nil {
		t.Fatalf("waiting reconcile used an obsolete target: %v", err)
	}
}

func TestRequireCurrentFailsClosedForProjectionLag(t *testing.T) {
	definition := Definition{Name: ResidentCurrentStatusName, Version: "resident-current-status-v1"}
	source, store, clock := projectionCoordinatorFixtures(definition)
	store.watermarks[targetKey{name: definition.Name, residentID: projectionTestResident}] = projectionTestWatermark(definition, 10, 100)
	coordinator := newProjectionTestCoordinator(t, definition, source, store, clock)
	if err := coordinator.RequireCurrent(context.Background(), projectionTestResident, definition.Name); err != nil {
		t.Fatalf("current Projection rejected: %v", err)
	}
	source.mu.Lock()
	source.head.CommitSeq = 11
	source.mu.Unlock()
	if err := coordinator.RequireCurrent(context.Background(), projectionTestResident, definition.Name); !errors.Is(err, ErrProjectionNotCurrent) {
		t.Fatalf("lagging Projection error = %v, want fail closed", err)
	}
}

func watermarksEqualForTest(left, right Watermark) bool {
	equalDependencies, err := DependencySetEqual(left.Dependencies, right.Dependencies)
	return err == nil && equalDependencies && left.ProjectionName == right.ProjectionName &&
		left.ResidentID == right.ResidentID && left.ProjectionVersion == right.ProjectionVersion &&
		left.SourceCommitSeq == right.SourceCommitSeq && left.AsOf == right.AsOf && left.AsOfTZ == right.AsOfTZ
}

func TestCOV56CoordinatorBuildsOneMinimumIntervalSameTargetCohort(t *testing.T) {
	definitions := projectionSameTargetTestDefinitions()
	source, store, clock := projectionCoordinatorFixtures(definitions[0])
	registry, err := NewRegistry(definitions[1], definitions[0])
	if err != nil {
		t.Fatal(err)
	}
	coordinator, err := NewCoordinator(CoordinatorOptions{
		Registry: registry, Source: source, Store: store, Clock: clock, Timezone: projectionTestTZ,
		ScanInterval: time.Hour, AsOfRefreshInterval: time.Hour,
		RebuildRetryInterval: time.Minute, MaxStaleness: time.Hour,
		Overrides: map[Name]ScheduleOverride{
			ClaimStatesName:   {ScanInterval: 80 * time.Millisecond, AsOfRefreshInterval: 20 * time.Millisecond},
			RuntimeStatesName: {ScanInterval: 30 * time.Millisecond, AsOfRefreshInterval: 50 * time.Millisecond},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	units := coordinator.scheduleUnits()
	if len(units) != 1 {
		t.Fatalf("schedule units = %d, want one cohort", len(units))
	}
	unit := units[0]
	if len(unit.definitions) != 2 || unit.definitions[0].Name != ClaimStatesName || unit.definitions[1].Name != RuntimeStatesName {
		t.Fatalf("cohort definitions = %+v", unit.definitions)
	}
	if unit.scanInterval != 30*time.Millisecond || unit.asOfRefreshInterval != 20*time.Millisecond {
		t.Fatalf("cohort intervals = %v/%v", unit.scanInterval, unit.asOfRefreshInterval)
	}
}

func TestCOV56CoordinatorRejectsNonTimeSensitiveSameTargetCohort(t *testing.T) {
	definitions := projectionSameTargetTestDefinitions()
	definitions[1].TimeSensitive = false
	source, store, clock := projectionCoordinatorFixtures(definitions[0])
	registry, err := NewRegistry(definitions...)
	if err != nil {
		t.Fatal(err)
	}
	_, err = NewCoordinator(CoordinatorOptions{
		Registry: registry, Source: source, Store: store, Clock: clock, Timezone: projectionTestTZ,
		ScanInterval: time.Hour, AsOfRefreshInterval: time.Hour,
		RebuildRetryInterval: time.Minute, MaxStaleness: time.Hour,
	})
	if err == nil {
		t.Fatal("NewCoordinator accepted a non-time-sensitive same-Target cohort")
	}
}

func TestCOV56CoordinatorKeepsSingleRegisteredMemberCompatible(t *testing.T) {
	definition := Definition{Name: ClaimStatesName, Version: "claim-states-test-v1"}
	source, store, clock := projectionCoordinatorFixtures(definition)
	coordinator := newProjectionTestCoordinator(t, definition, source, store, clock)
	units := coordinator.scheduleUnits()
	if len(units) != 1 || len(units[0].definitions) != 1 || !reflect.DeepEqual(units[0].definitions[0], definition) {
		t.Fatalf("singleton schedule = %+v", units)
	}
	if err := coordinator.ReconcileResident(context.Background(), projectionTestResident); err != nil {
		t.Fatal(err)
	}
	if _, exists, err := store.Watermark(context.Background(), definition.Name, projectionTestResident); err != nil || !exists {
		t.Fatalf("singleton watermark exists/error = %v/%v", exists, err)
	}
}

func TestCOV56CoordinatorManualMemberRebuildUsesOneExactCohortTarget(t *testing.T) {
	for _, requested := range []Name{ClaimStatesName, RuntimeStatesName} {
		t.Run(string(requested), func(t *testing.T) {
			definitions := projectionSameTargetTestDefinitions()
			source, store, clock := projectionCoordinatorFixtures(definitions[0])
			registry, err := NewRegistry(definitions...)
			if err != nil {
				t.Fatal(err)
			}
			coordinator, err := NewCoordinator(CoordinatorOptions{
				Registry: registry, Source: source, Store: store, Clock: clock, Timezone: projectionTestTZ,
				ScanInterval: time.Hour, AsOfRefreshInterval: time.Hour,
				RebuildRetryInterval: time.Minute, MaxStaleness: time.Hour,
			})
			if err != nil {
				t.Fatal(err)
			}

			if err := coordinator.Rebuild(context.Background(), projectionTestResident, requested); err != nil {
				t.Fatal(err)
			}
			source.mu.Lock()
			if len(source.evaluations) != 2 ||
				source.evaluations[0].Definition.Name != ClaimStatesName ||
				source.evaluations[1].Definition.Name != RuntimeStatesName ||
				source.evaluations[0].Target != source.evaluations[1].Target {
				source.mu.Unlock()
				t.Fatalf("manual rebuild evaluations = %+v, want one ordered exact-Target pair", source.evaluations)
			}
			expectedHead := source.head
			source.mu.Unlock()

			waitForProjectionCohortTarget(t, store, []canonical.ID{projectionTestResident}, 10, 200)
			if err := coordinator.RequireSameTargetCohort(context.Background(), projectionTestResident, expectedHead); err != nil {
				t.Fatalf("RequireSameTargetCohort: %v", err)
			}

			store.mu.Lock()
			runtimeKey := targetKey{name: RuntimeStatesName, residentID: projectionTestResident}
			runtime := store.watermarks[runtimeKey]
			runtime.AsOf++
			store.watermarks[runtimeKey] = runtime
			store.mu.Unlock()
			if err := coordinator.RequireSameTargetCohort(context.Background(), projectionTestResident, expectedHead); !errors.Is(err, ErrProjectionNotCurrent) {
				t.Fatalf("skewed cohort verification = %v, want ErrProjectionNotCurrent", err)
			}

			store.mu.Lock()
			delete(store.watermarks, runtimeKey)
			store.mu.Unlock()
			if err := coordinator.RequireSameTargetCohort(context.Background(), projectionTestResident, expectedHead); !errors.Is(err, ErrProjectionNotCurrent) {
				t.Fatalf("missing cohort verification = %v, want ErrProjectionNotCurrent", err)
			}
		})
	}
}

func TestCOV56CoordinatorPeriodicCohortKeepsExactTargetAcrossResidentsAndCycles(t *testing.T) {
	definitions := projectionSameTargetTestDefinitions()
	source, store, clock := projectionCoordinatorFixtures(definitions[0])
	second, err := canonical.ParseID("01J00000000000000000000009")
	if err != nil {
		t.Fatal(err)
	}
	source.residents = []canonical.ID{second, projectionTestResident}
	registry, err := NewRegistry(definitions...)
	if err != nil {
		t.Fatal(err)
	}
	coordinator, err := NewCoordinator(CoordinatorOptions{
		Registry: registry, Source: source, Store: store, Clock: clock, Timezone: projectionTestTZ,
		ScanInterval: time.Hour, AsOfRefreshInterval: 15 * time.Millisecond,
		RebuildRetryInterval: time.Minute, MaxStaleness: time.Hour,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := coordinator.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := coordinator.WaitReady(context.Background()); err != nil {
		t.Fatal(err)
	}
	for _, asOf := range []canonical.Instant{300, 400} {
		clock.Set(time.UnixMicro(int64(asOf)).UTC())
		waitForProjectionCohortTarget(t, store, []canonical.ID{projectionTestResident, second}, 10, asOf)
	}
	coordinator.Stop()
	waitCtx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := coordinator.Wait(waitCtx); err != nil {
		t.Fatal(err)
	}

	source.mu.Lock()
	defer source.mu.Unlock()
	if len(source.residentTargets) < 3 {
		t.Fatalf("resident enumerations = %d, want startup plus two refresh cycles", len(source.residentTargets))
	}
	if len(source.evaluations)%2 != 0 {
		t.Fatalf("evaluation count = %d, want complete pairs", len(source.evaluations))
	}
	for index := 0; index < len(source.evaluations); index += 2 {
		claim := source.evaluations[index]
		runtime := source.evaluations[index+1]
		if claim.Definition.Name != ClaimStatesName || runtime.Definition.Name != RuntimeStatesName ||
			claim.ResidentID != runtime.ResidentID || claim.Target != runtime.Target {
			t.Fatalf("evaluation pair %d does not share one Target: %+v / %+v", index/2, claim, runtime)
		}
	}
}

func TestCOV56CoordinatorCohortScanAdvancesHeadWithoutAdvancingAsOf(t *testing.T) {
	definitions := projectionSameTargetTestDefinitions()
	source, store, clock := projectionCoordinatorFixtures(definitions[0])
	registry, err := NewRegistry(definitions...)
	if err != nil {
		t.Fatal(err)
	}
	coordinator, err := NewCoordinator(CoordinatorOptions{
		Registry: registry, Source: source, Store: store, Clock: clock, Timezone: projectionTestTZ,
		ScanInterval: time.Hour, AsOfRefreshInterval: time.Hour,
		RebuildRetryInterval: time.Minute, MaxStaleness: time.Hour,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := coordinator.ReconcileResident(context.Background(), projectionTestResident); err != nil {
		t.Fatal(err)
	}
	clock.Set(time.UnixMicro(300).UTC())
	source.mu.Lock()
	source.head.CommitSeq = 11
	source.head.CommittedAt = 250
	source.mu.Unlock()
	name := RuntimeStatesName
	if err := coordinator.reconcile(context.Background(), &projectionTestResident, false, true, &name, false); err != nil {
		t.Fatal(err)
	}
	waitForProjectionCohortTarget(t, store, []canonical.ID{projectionTestResident}, 11, 200)
}

func TestCOV56CoordinatorCohortScanHealsPreExistingTargetSkewWithoutRegression(t *testing.T) {
	tokyo := canonical.MustTimezone("Asia/Tokyo")
	newYork := canonical.MustTimezone("America/New_York")
	tests := []struct {
		name           string
		capturedAsOf   canonical.Instant
		claimAsOf      canonical.Instant
		claimTZ        canonical.Timezone
		runtimeAsOf    canonical.Instant
		runtimeTZ      canonical.Timezone
		wantSharedAsOf canonical.Instant
	}{
		{
			name: "stored as-of and timezone skew", capturedAsOf: 200,
			claimAsOf: 350, claimTZ: tokyo, runtimeAsOf: 300, runtimeTZ: projectionTestTZ,
			wantSharedAsOf: 350,
		},
		{
			name: "timezone-only skew at the stored maximum", capturedAsOf: 200,
			claimAsOf: 350, claimTZ: tokyo, runtimeAsOf: 350, runtimeTZ: newYork,
			wantSharedAsOf: 350,
		},
		{
			name: "captured target is the maximum", capturedAsOf: 400,
			claimAsOf: 350, claimTZ: tokyo, runtimeAsOf: 300, runtimeTZ: newYork,
			wantSharedAsOf: 400,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			definitions := projectionSameTargetTestDefinitions()
			source, store, clock := projectionCoordinatorFixtures(definitions[0])
			clock.Set(time.UnixMicro(int64(test.capturedAsOf)).UTC())
			claim := projectionTestWatermark(definitions[0], 10, test.claimAsOf)
			claim.AsOfTZ = test.claimTZ
			runtime := projectionTestWatermark(definitions[1], 10, test.runtimeAsOf)
			runtime.AsOfTZ = test.runtimeTZ
			store.watermarks[targetKey{name: ClaimStatesName, residentID: projectionTestResident}] = claim
			store.watermarks[targetKey{name: RuntimeStatesName, residentID: projectionTestResident}] = runtime
			registry, err := NewRegistry(definitions...)
			if err != nil {
				t.Fatal(err)
			}
			coordinator, err := NewCoordinator(CoordinatorOptions{
				Registry: registry, Source: source, Store: store, Clock: clock, Timezone: projectionTestTZ,
				ScanInterval: time.Hour, AsOfRefreshInterval: time.Hour,
				RebuildRetryInterval: time.Minute, MaxStaleness: time.Hour,
			})
			if err != nil {
				t.Fatal(err)
			}

			name := ClaimStatesName
			if err := coordinator.reconcile(context.Background(), &projectionTestResident, false, false, &name, false); err != nil {
				t.Fatal(err)
			}
			if test.wantSharedAsOf < test.claimAsOf || test.wantSharedAsOf < test.runtimeAsOf {
				t.Fatalf("test expected a regressing target: want=%d claim=%d runtime=%d", test.wantSharedAsOf, test.claimAsOf, test.runtimeAsOf)
			}
			waitForProjectionCohortTarget(t, store, []canonical.ID{projectionTestResident}, 10, test.wantSharedAsOf)

			source.mu.Lock()
			defer source.mu.Unlock()
			if len(source.evaluations) != 2 {
				t.Fatalf("evaluations = %d, want both skewed cohort members reevaluated", len(source.evaluations))
			}
			wantTarget := Target{
				Head: source.head, AsOf: test.wantSharedAsOf, AsOfTZ: projectionTestTZ,
			}
			if source.evaluations[0].Definition.Name != ClaimStatesName || source.evaluations[1].Definition.Name != RuntimeStatesName ||
				source.evaluations[0].Target != wantTarget || source.evaluations[1].Target != wantTarget {
				t.Fatalf("skew recovery targets = %+v / %+v, want %+v", source.evaluations[0], source.evaluations[1], wantTarget)
			}
		})
	}
}

func TestCOV56CoordinatorPartialFirstBuildScanRetryPreservesSharedAsOf(t *testing.T) {
	definitions := projectionSameTargetTestDefinitions()
	source, store, clock := projectionCoordinatorFixtures(definitions[0])
	source.residents = nil
	store.applyErrors = map[targetKey]error{
		{name: RuntimeStatesName, residentID: projectionTestResident}: errors.New("injected runtime first-build failure"),
	}
	registry, err := NewRegistry(definitions...)
	if err != nil {
		t.Fatal(err)
	}
	coordinator, err := NewCoordinator(CoordinatorOptions{
		Registry: registry, Source: source, Store: store, Clock: clock, Timezone: projectionTestTZ,
		ScanInterval: time.Hour, AsOfRefreshInterval: time.Hour,
		RebuildRetryInterval: 40 * time.Millisecond, MaxStaleness: time.Hour,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := coordinator.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		coordinator.Stop()
		_ = coordinator.Wait(context.Background())
	})
	if err := coordinator.WaitReady(context.Background()); err != nil {
		t.Fatal(err)
	}

	name := ClaimStatesName
	if err := coordinator.reconcile(context.Background(), &projectionTestResident, false, true, &name, false); err == nil {
		t.Fatal("first scan unexpectedly succeeded despite injected runtime failure")
	}
	claim, exists, err := store.Watermark(context.Background(), ClaimStatesName, projectionTestResident)
	if err != nil || !exists || claim.AsOf != 200 {
		t.Fatalf("partial first-build claim watermark = %+v/%v/%v", claim, exists, err)
	}
	coordinator.failuresMu.RLock()
	runtimeFailure := coordinator.failures[targetKey{name: RuntimeStatesName, residentID: projectionTestResident}]
	coordinator.failuresMu.RUnlock()
	if runtimeFailure.refreshAsOf {
		t.Fatal("scan failure retry was changed into an as-of refresh")
	}

	clock.Set(time.UnixMicro(300).UTC())
	store.mu.Lock()
	delete(store.applyErrors, targetKey{name: RuntimeStatesName, residentID: projectionTestResident})
	store.mu.Unlock()
	waitForProjectionCohortTarget(t, store, []canonical.ID{projectionTestResident}, 10, 200)
}

func TestCOV56CoordinatorCohortRetrySuppressesPartialAdvanceAndReconverges(t *testing.T) {
	definitions := projectionSameTargetTestDefinitions()
	source, store, clock := projectionCoordinatorFixtures(definitions[0])
	store.applyErrors = map[targetKey]error{
		{name: RuntimeStatesName, residentID: projectionTestResident}: errors.New("injected runtime apply failure"),
	}
	registry, err := NewRegistry(definitions...)
	if err != nil {
		t.Fatal(err)
	}
	coordinator, err := NewCoordinator(CoordinatorOptions{
		Registry: registry, Source: source, Store: store, Clock: clock, Timezone: projectionTestTZ,
		ScanInterval: time.Hour, AsOfRefreshInterval: time.Hour,
		RebuildRetryInterval: 40 * time.Millisecond, MaxStaleness: time.Hour,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := coordinator.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		coordinator.Stop()
		_ = coordinator.Wait(context.Background())
	})
	if err := coordinator.WaitReady(context.Background()); err == nil {
		t.Fatal("startup unexpectedly succeeded despite injected runtime failure")
	}

	clock.Set(time.UnixMicro(300).UTC())
	source.mu.Lock()
	source.head.CommitSeq = 11
	source.head.CommittedAt = 250
	source.mu.Unlock()
	name := ClaimStatesName
	if err := coordinator.reconcile(context.Background(), &projectionTestResident, false, true, &name, true); err != nil {
		t.Fatal(err)
	}
	claim, exists, err := store.Watermark(context.Background(), ClaimStatesName, projectionTestResident)
	if err != nil || !exists || claim.SourceCommitSeq != 10 || claim.AsOf != 200 {
		t.Fatalf("successful member advanced while cohort retry was pending: %+v/%v/%v", claim, exists, err)
	}

	store.mu.Lock()
	delete(store.applyErrors, targetKey{name: RuntimeStatesName, residentID: projectionTestResident})
	store.mu.Unlock()
	waitForProjectionCohortTarget(t, store, []canonical.ID{projectionTestResident}, 11, 300)
}

func projectionSameTargetTestDefinitions() []Definition {
	return []Definition{
		{Name: ClaimStatesName, Version: "claim-states-test-v1", TimeSensitive: true},
		{Name: RuntimeStatesName, Version: "runtime-states-test-v2", TimeSensitive: true},
	}
}

func waitForProjectionCohortTarget(t *testing.T, store *projectionFakeStore, residents []canonical.ID, commitSeq canonical.CommitSeq, asOf canonical.Instant) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for {
		matched := true
		for _, residentID := range residents {
			for _, name := range []Name{ClaimStatesName, RuntimeStatesName} {
				watermark, exists, err := store.Watermark(context.Background(), name, residentID)
				if err != nil || !exists || watermark.SourceCommitSeq != commitSeq || watermark.AsOf != asOf || watermark.AsOfTZ != projectionTestTZ {
					matched = false
				}
			}
		}
		if matched {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("cohort did not reach exact Target commit=%d as_of=%d", commitSeq, asOf)
		}
		time.Sleep(5 * time.Millisecond)
	}
}
