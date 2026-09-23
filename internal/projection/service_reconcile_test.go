package projection

import (
	"context"
	"errors"
	"reflect"
	"sync"
	"testing"
	"time"

	"mahoroba.local/mahoroba/internal/canonical"
)

type serviceReconcileCountingSource struct {
	*projectionFakeSource

	headMu    sync.Mutex
	headCalls int
}

func (source *serviceReconcileCountingSource) Head(ctx context.Context) (canonical.Head, error) {
	source.headMu.Lock()
	source.headCalls++
	source.headMu.Unlock()
	return source.projectionFakeSource.Head(ctx)
}

func (source *serviceReconcileCountingSource) HeadCalls() int {
	source.headMu.Lock()
	defer source.headMu.Unlock()
	return source.headCalls
}

func TestCOV1ServiceRequiredReconcileUsesOneTargetAndExactClosedSet(t *testing.T) {
	coordinator, source, store, _, definitions := newServiceReconcileTestCoordinator(t)
	desired := source.dependencies[ClaimStatesName]
	for _, definition := range definitions {
		dependencies := []Dependency(nil)
		if definition.Name == ClaimStatesName {
			dependencies = desired
		}
		store.watermarks[targetKey{name: definition.Name, residentID: projectionTestResident}] =
			projectionTestWatermark(definition, 5, 100, dependencies...)
	}

	target, err := coordinator.ReconcileServiceRequiredResident(context.Background(), projectionTestResident)
	if err != nil {
		t.Fatalf("ReconcileServiceRequiredResident: %v", err)
	}
	if target.Head.CommitSeq != 10 || target.AsOf != 200 || target.AsOfTZ != projectionTestTZ {
		t.Fatalf("captured Target = %+v", target)
	}
	if got := source.HeadCalls(); got != 1 {
		t.Fatalf("Head calls = %d, want exactly one", got)
	}

	source.mu.Lock()
	evaluations := append([]EvaluationRequest(nil), source.evaluations...)
	source.mu.Unlock()
	if len(evaluations) != len(ServiceRequiredNames()) {
		t.Fatalf("evaluations = %d, want %d", len(evaluations), len(ServiceRequiredNames()))
	}
	gotNames := make([]Name, 0, len(evaluations))
	for _, evaluation := range evaluations {
		gotNames = append(gotNames, evaluation.Definition.Name)
		if evaluation.Target != target {
			t.Fatalf("%s evaluation Target = %+v, want %+v", evaluation.Definition.Name, evaluation.Target, target)
		}
	}
	if !reflect.DeepEqual(gotNames, ServiceRequiredNames()) {
		t.Fatalf("evaluated names = %v, want %v", gotNames, ServiceRequiredNames())
	}

	serviceRequired := make(map[Name]Definition, len(definitions))
	for _, definition := range definitions {
		serviceRequired[definition.Name] = definition
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	if len(store.applies) != len(ServiceRequiredNames()) {
		t.Fatalf("apply count = %d, want %d", len(store.applies), len(ServiceRequiredNames()))
	}
	for _, name := range ServiceRequiredNames() {
		definition := serviceRequired[name]
		watermark := store.watermarks[targetKey{name: name, residentID: projectionTestResident}]
		if watermark.SourceCommitSeq != target.Head.CommitSeq {
			t.Fatalf("%s source cursor = %s, want %s", name, watermark.SourceCommitSeq, target.Head.CommitSeq)
		}
		wantAsOf := canonical.Instant(100)
		if definition.TimeSensitive {
			wantAsOf = target.AsOf
		}
		if watermark.AsOf != wantAsOf {
			t.Fatalf("%s as_of = %s, want %s", name, watermark.AsOf, wantAsOf)
		}
	}
	content := ContentReferencesDefinition()
	watermark := store.watermarks[targetKey{name: content.Name, residentID: projectionTestResident}]
	if watermark.SourceCommitSeq != 5 || watermark.AsOf != 100 {
		t.Fatalf("content_references was reconciled: %+v", watermark)
	}
}

func TestCOV1ServiceRequiredReconcileCapturesAfterResidentLock(t *testing.T) {
	coordinator, source, _, clock, _ := newServiceReconcileTestCoordinator(t)
	lock := coordinator.residentLock(projectionTestResident)
	lock.Lock()
	locked := true
	t.Cleanup(func() {
		if locked {
			lock.Unlock()
		}
	})

	type reconcileResult struct {
		target Target
		err    error
	}
	started := make(chan struct{})
	finished := make(chan reconcileResult, 1)
	go func() {
		close(started)
		target, err := coordinator.ReconcileServiceRequiredResident(context.Background(), projectionTestResident)
		finished <- reconcileResult{target: target, err: err}
	}()
	<-started
	select {
	case result := <-finished:
		t.Fatalf("reconcile bypassed resident lock: %+v/%v", result.target, result.err)
	case <-time.After(20 * time.Millisecond):
	}

	source.mu.Lock()
	source.head = canonical.Head{Exists: true, CommitSeq: 11, CommittedAt: 250}
	source.mu.Unlock()
	clock.Set(time.UnixMicro(300).UTC())
	lock.Unlock()
	locked = false

	result := <-finished
	if result.err != nil {
		t.Fatalf("ReconcileServiceRequiredResident: %v", result.err)
	}
	if result.target.Head.CommitSeq != 11 || result.target.AsOf != 300 {
		t.Fatalf("Target captured before resident lock = %+v", result.target)
	}
}

func TestCOV1ServiceRequiredReconcileReturnsCapturedTargetOnUpdateFailure(t *testing.T) {
	coordinator, source, store, _, _ := newServiceReconcileTestCoordinator(t)
	want := errors.New("injected service-required update failure")
	store.applyErr = want

	target, err := coordinator.ReconcileServiceRequiredResident(context.Background(), projectionTestResident)
	if !errors.Is(err, want) {
		t.Fatalf("error = %v, want injected failure", err)
	}
	if target.Head.CommitSeq != 10 || target.AsOf != 200 || target.AsOfTZ != projectionTestTZ {
		t.Fatalf("captured Target was lost on update failure: %+v", target)
	}
	if got := source.HeadCalls(); got != 1 {
		t.Fatalf("Head calls = %d, want exactly one", got)
	}
}

func TestCOV1ServiceRequiredReconcileNeverReturnsAsOfBeforeCapturedHead(t *testing.T) {
	coordinator, _, _, clock, _ := newServiceReconcileTestCoordinator(t)
	clock.Set(time.UnixMicro(50).UTC())

	target, err := coordinator.ReconcileServiceRequiredResident(context.Background(), projectionTestResident)
	if err != nil {
		t.Fatal(err)
	}
	if target.AsOf != target.Head.CommittedAt {
		t.Fatalf("Assembly Target as_of = %s, head committed_at = %s", target.AsOf, target.Head.CommittedAt)
	}
}

func newServiceReconcileTestCoordinator(
	t *testing.T,
) (*Coordinator, *serviceReconcileCountingSource, *projectionFakeStore, *projectionTestClock, []Definition) {
	t.Helper()
	definitions := []Definition{
		{Name: ResidentCurrentStatusName, Version: "resident-current-status-v1"},
		{Name: ResidentCurrentRevisionName, Version: "resident-current-revision-v1"},
		{Name: RuntimeStatesName, Version: "runtime-states-v1", TimeSensitive: true},
		ClaimStatesDefinition(),
		ClaimViewScopeCurrentDefinition(),
		ContentReferencesDefinition(),
	}
	base, store, clock := projectionCoordinatorFixtures(definitions[0])
	base.dependencies[ClaimStatesName] = []Dependency{{
		Kind: MemoryPolicyDependency, VersionID: projectionTestPolicy,
	}}
	source := &serviceReconcileCountingSource{projectionFakeSource: base}
	registry, err := NewRegistry(definitions...)
	if err != nil {
		t.Fatalf("NewRegistry: %v", err)
	}
	coordinator, err := NewCoordinator(CoordinatorOptions{
		Registry: registry, Source: source, Store: store, Clock: clock, Timezone: projectionTestTZ,
		ScanInterval: time.Hour, AsOfRefreshInterval: time.Hour,
		RebuildRetryInterval: time.Minute, MaxStaleness: 5 * time.Minute,
	})
	if err != nil {
		t.Fatalf("NewCoordinator: %v", err)
	}
	return coordinator, source, store, clock, definitions
}
