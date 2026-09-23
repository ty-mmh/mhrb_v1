package app

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"

	"mahoroba.local/mahoroba/internal/canonical"
	"mahoroba.local/mahoroba/internal/domain"
	"mahoroba.local/mahoroba/internal/projection"
	store "mahoroba.local/mahoroba/internal/store/sqlite"
)

var _ dialogueProjectionReconciler = (*projection.Coordinator)(nil)

func TestCOV1CommittedIngressSurvivesNotificationAndPrepareHintFailure(t *testing.T) {
	ctx := context.Background()
	generator := &scriptedGenerator{}
	fixture := newApplicationFixture(t, generator, 2)
	failing := &cov1FailingAssemblyReconciler{Called: make(chan struct{}, 1)}
	fixture.application.commitNotifier = failing
	fixture.application.dialogueReconciler = failing
	if err := fixture.application.Prepare(ctx); err != nil {
		t.Fatal(err)
	}
	if err := fixture.application.StartWorkers(nil); err != nil {
		t.Fatal(err)
	}
	defer func() {
		fixture.application.Stop()
		waitCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := fixture.application.Wait(waitCtx); err != nil {
			t.Errorf("wait application: %v", err)
		}
	}()

	event, err := fixture.application.Ingress(ctx, "accepted before failed Prepare hint")
	if err != nil {
		t.Fatalf("Ingress returned post-Commit failure: %v", err)
	}
	select {
	case <-failing.Called:
	case <-time.After(5 * time.Second):
		t.Fatal("best-effort Prepare hint did not run")
	}
	if failing.Notifications != 1 {
		t.Fatalf("notification calls = %d, want 1 failed best-effort notification", failing.Notifications)
	}
	if got := generator.CallCount(); got != 0 {
		t.Fatalf("provider calls after Prepare failure = %d, want 0", got)
	}
	var events, runs int
	if err := fixture.store.Reader().QueryRowContext(ctx, `SELECT COUNT(*) FROM events WHERE event_id = ?`,
		event.ID.String()).Scan(&events); err != nil {
		t.Fatal(err)
	}
	if err := fixture.store.Reader().QueryRowContext(ctx, `SELECT COUNT(*) FROM generation_runs
		WHERE resident_id = ? AND idempotency_key = ?`, fixture.residentID.String(),
		domain.DialogueObligation(event.ID)).Scan(&runs); err != nil {
		t.Fatal(err)
	}
	if events != 1 || runs != 0 {
		t.Fatalf("post-failure durable state = events %d / runs %d, want 1 / 0", events, runs)
	}
}

func TestCOV1ProductionPendingPathCommitsPrepareBeforeProvider(t *testing.T) {
	ctx := context.Background()
	providerSawRunning := false
	var fixture applicationFixture
	generator := &scriptedGenerator{steps: []generatorStep{{
		text: "reply after Commit B",
		beforeReturn: func() {
			var running, inputs int
			if err := fixture.store.Reader().QueryRowContext(ctx, `SELECT COUNT(*)
				FROM generation_run_outcomes outcome
				JOIN generation_runs run ON run.generation_run_id = outcome.generation_run_id
				WHERE run.resident_id = ? AND run.purpose = 'dialogue' AND outcome.state = 'running'`,
				fixture.residentID.String()).Scan(&running); err != nil {
				t.Error(err)
				return
			}
			if err := fixture.store.Reader().QueryRowContext(ctx, `SELECT COUNT(*)
				FROM generation_run_inputs input JOIN generation_runs run
				ON run.generation_run_id = input.generation_run_id
				WHERE run.resident_id = ? AND run.purpose = 'dialogue'`,
				fixture.residentID.String()).Scan(&inputs); err != nil {
				t.Error(err)
				return
			}
			providerSawRunning = running == 1 && inputs > 0
		},
	}}}
	fixture = newApplicationFixture(t, generator, 2)
	coordinator := newCOV1ProjectionCoordinator(t, fixture)
	fixture.application.commitNotifier = coordinator
	fixture.application.dialogueReconciler = coordinator
	fixture.clock.Advance(time.Second)

	event, err := fixture.application.Ingress(ctx, "prepare before provider")
	if err != nil {
		t.Fatal(err)
	}
	if got := generator.CallCount(); got != 0 {
		t.Fatalf("provider calls during Commit A = %d, want 0", got)
	}
	var runsAfterIngress int
	if err := fixture.store.Reader().QueryRowContext(ctx, `SELECT COUNT(*) FROM generation_runs
		WHERE resident_id = ? AND idempotency_key = ?`, fixture.residentID.String(),
		domain.DialogueObligation(event.ID)).Scan(&runsAfterIngress); err != nil {
		t.Fatal(err)
	}
	if runsAfterIngress != 0 {
		t.Fatalf("generation runs after Commit A = %d, want 0", runsAfterIngress)
	}

	if err := fixture.application.ProcessResident(ctx, fixture.residentID); err != nil {
		t.Fatal(err)
	}
	if !providerSawRunning {
		t.Fatal("provider began before the durable running outcome and frozen inputs were readable")
	}
	if got := generator.CallCount(); got != 1 {
		t.Fatalf("provider calls after ProcessResident = %d, want 1", got)
	}
	var pipelineKey, promptVersion, contextVersion, renderingVersion string
	if err := fixture.store.Reader().QueryRowContext(ctx, `SELECT pipeline.version_key,
		run.prompt_template_version, run.context_policy_version, run.memory_rendering_version
		FROM generation_runs run JOIN pipeline_versions pipeline
		ON pipeline.pipeline_version_id = run.pipeline_version_id
		WHERE run.resident_id = ? AND run.idempotency_key = ?`, fixture.residentID.String(),
		domain.DialogueObligation(event.ID)).Scan(
		&pipelineKey, &promptVersion, &contextVersion, &renderingVersion,
	); err != nil {
		t.Fatal(err)
	}
	current := domain.CurrentDialogueNormalExecutionContract()
	if pipelineKey != current.PipelineVersionKey || promptVersion != current.PromptTemplateVersion ||
		contextVersion != current.ContextPolicyVersion || renderingVersion != current.MemoryRenderingVersion {
		t.Fatalf("prepared tuple = %s/%s/%s/%s, want %+v",
			pipelineKey, promptVersion, contextVersion, renderingVersion, current)
	}
}

func newCOV1ProjectionCoordinator(t *testing.T, fixture applicationFixture) *projection.Coordinator {
	t.Helper()
	registry, err := store.ActiveProjectionRegistry()
	if err != nil {
		t.Fatal(err)
	}
	surface := fixture.store.Projection()
	coordinator, err := projection.NewCoordinator(projection.CoordinatorOptions{
		Registry: registry, Source: surface, Store: surface,
		Clock: fixture.clock, Timezone: canonical.MustTimezone("UTC"),
		ScanInterval: time.Hour, AsOfRefreshInterval: time.Hour,
		RebuildRetryInterval: time.Hour, MaxStaleness: 5 * time.Minute,
	})
	if err != nil {
		t.Fatal(err)
	}
	return coordinator
}

func TestCOV1IngressCommitsOnlyUserEventAndRejectsExplicitReferencesBeforeCommit(t *testing.T) {
	ctx := context.Background()
	fixture := newApplicationFixture(t, &scriptedGenerator{}, 2)
	var commitsBefore int
	if err := fixture.store.Reader().QueryRowContext(ctx, `SELECT COUNT(*) FROM canonical_commits`).Scan(&commitsBefore); err != nil {
		t.Fatal(err)
	}

	event, err := fixture.application.Ingress(ctx, "Commit A only")
	if err != nil {
		t.Fatal(err)
	}
	var commitsAfter, runs, recallRuns int
	if err := fixture.store.Reader().QueryRowContext(ctx, `SELECT COUNT(*) FROM canonical_commits`).Scan(&commitsAfter); err != nil {
		t.Fatal(err)
	}
	if err := fixture.store.Reader().QueryRowContext(ctx, `SELECT COUNT(*) FROM generation_runs`).Scan(&runs); err != nil {
		t.Fatal(err)
	}
	if err := fixture.store.Reader().QueryRowContext(ctx, `SELECT COUNT(*) FROM recall_runs`).Scan(&recallRuns); err != nil {
		t.Fatal(err)
	}
	if commitsAfter != commitsBefore+1 || runs != 0 || recallRuns != 0 {
		t.Fatalf("Commit A state = commits %d->%d, generation runs %d, Recall runs %d",
			commitsBefore, commitsAfter, runs, recallRuns)
	}
	work, err := discoverDialogueWorkForTest(ctx, fixture.repository, fixture.residentID, 10, 2)
	if err != nil {
		t.Fatal(err)
	}
	if len(work) != 1 || work[0].UserEvent.ID != event.ID || work[0].State != domain.WorkPending {
		t.Fatalf("Commit A work = %+v, want one pending obligation", work)
	}

	if _, err := fixture.application.IngressWithMetadata(ctx, domain.IngressRequest{
		RawText: "durable reference is not available", ExplicitEventIDs: []canonical.ID{event.ID},
	}); !errors.Is(err, domain.ErrInvalidEventReference) {
		t.Fatalf("explicit-reference ingress error = %v", err)
	}
	var commitsAfterReject int
	if err := fixture.store.Reader().QueryRowContext(ctx, `SELECT COUNT(*) FROM canonical_commits`).Scan(&commitsAfterReject); err != nil {
		t.Fatal(err)
	}
	if commitsAfterReject != commitsAfter {
		t.Fatalf("explicit-reference rejection created Commit %d, want unchanged %d", commitsAfterReject, commitsAfter)
	}
}

func TestCOV1CommitAOnlyRestartConvergesToExactlyOneRunAndResidentMessage(t *testing.T) {
	ctx := context.Background()
	fixture := newApplicationFixture(t, &scriptedGenerator{}, 2)
	event, err := fixture.application.Ingress(ctx, "survive before Commit B")
	if err != nil {
		t.Fatal(err)
	}
	var runsBeforeRestart int
	if err := fixture.store.Reader().QueryRowContext(ctx, `SELECT COUNT(*) FROM generation_runs
		WHERE resident_id = ? AND idempotency_key = ?`, fixture.residentID.String(),
		domain.DialogueObligation(event.ID)).Scan(&runsBeforeRestart); err != nil {
		t.Fatal(err)
	}
	if runsBeforeRestart != 0 {
		t.Fatalf("generation runs before restart = %d, want Commit A only", runsBeforeRestart)
	}
	if err := fixture.writer.Close(ctx); err != nil {
		t.Fatal(err)
	}
	if err := fixture.store.Close(); err != nil {
		t.Fatal(err)
	}

	reopened, err := store.Open(ctx, fixture.store.Path())
	if err != nil {
		t.Fatal(err)
	}
	reopenedIDs := canonical.NewSecureIDGenerator()
	reopenedWriter, err := canonical.OpenWriter(ctx, canonical.WriterOptions{
		Backend: reopened.Canonical(), IDs: reopenedIDs, Clock: fixture.clock,
		Timezone: canonical.MustTimezone("UTC"), QueueCapacity: 32,
	})
	if err != nil {
		_ = reopened.Close()
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = reopenedWriter.Close(context.Background())
		_ = reopened.Close()
	})
	reopenedFixture := applicationFixture{store: reopened, clock: fixture.clock}
	coordinator := newCOV1ProjectionCoordinator(t, reopenedFixture)
	generator := &scriptedGenerator{steps: []generatorStep{{text: "reply after restart"}}}
	restarted, err := New(Options{
		Writer: reopenedWriter, CommitNotifier: coordinator, Repository: reopened.Canonical(),
		IDs: reopenedIDs, Clock: fixture.clock, Timezone: canonical.MustTimezone("UTC"),
		Blobs: fixture.blobs, Generator: generator, Provider: "test", Model: "test-model",
		MaxAttempts: 2, RetryBackoff: []time.Duration{0},
		MaxInputBytes: 64 << 10, MaxOutputBytes: 64 << 10, SafetyScanInterval: time.Hour,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := restarted.ProcessResident(ctx, fixture.residentID); err != nil {
		t.Fatal(err)
	}
	if err := restarted.ProcessResident(ctx, fixture.residentID); err != nil {
		t.Fatalf("idempotent second processing: %v", err)
	}

	if got := generator.CallCount(); got != 1 {
		t.Fatalf("provider calls after restart convergence = %d, want 1", got)
	}
	var runs, residentMessages int
	if err := reopened.Reader().QueryRowContext(ctx, `SELECT COUNT(*) FROM generation_runs
		WHERE resident_id = ? AND idempotency_key = ?`, fixture.residentID.String(),
		domain.DialogueObligation(event.ID)).Scan(&runs); err != nil {
		t.Fatal(err)
	}
	if err := reopened.Reader().QueryRowContext(ctx, `SELECT COUNT(*) FROM events
		WHERE resident_id = ? AND event_type = 'resident_message'`, fixture.residentID.String()).Scan(&residentMessages); err != nil {
		t.Fatal(err)
	}
	if runs != 1 || residentMessages != 1 {
		t.Fatalf("restart convergence = runs %d / resident messages %d, want 1 / 1", runs, residentMessages)
	}
}

func TestCOV1PendingAssemblyRetriesTargetsWithOneFixedIdentityPoolBeforeProvider(t *testing.T) {
	ids := canonical.NewSecureIDGenerator()
	residentID, err := ids.New()
	if err != nil {
		t.Fatal(err)
	}
	eventID, err := ids.New()
	if err != nil {
		t.Fatal(err)
	}
	resident := domain.ResidentSnapshot{ResidentID: residentID, Status: "active"}
	repository := &cov1FailingAssemblyRepository{
		Snapshot: resident,
		Err:      domain.ErrDialogueAssemblyTargetChanged,
	}
	reconciler := &cov1CountingAssemblyReconciler{}
	generator := &scriptedGenerator{}
	application := &Application{
		repository:         repository,
		commitNotifier:     reconciler,
		dialogueReconciler: reconciler,
		ids:                ids,
		generator:          generator,
		provider:           "test",
		model:              "test-model",
		maxInputBytes:      64 << 10,
		maxOutputBytes:     64 << 10,
	}

	err = application.processWork(context.Background(), residentID, domain.DialogueWork{
		UserEvent: domain.Event{ID: eventID, ResidentID: residentID, Type: "user_message"},
		State:     domain.WorkPending,
	})
	if err == nil || !strings.Contains(err.Error(), "did not stabilize after 3 attempts") {
		t.Fatalf("processWork error = %v, want bounded Assembly retry failure", err)
	}
	if reconciler.Calls != dialogueAssemblyAttemptLimit || len(repository.Requests) != dialogueAssemblyAttemptLimit {
		t.Fatalf("reconcile/Assembly calls = %d/%d, want %d/%d",
			reconciler.Calls, len(repository.Requests), dialogueAssemblyAttemptLimit, dialogueAssemblyAttemptLimit)
	}
	if got := generator.CallCount(); got != 0 {
		t.Fatalf("provider calls before Prepare commit = %d, want 0", got)
	}

	first := repository.Requests[0]
	for index, request := range repository.Requests {
		if request.Target.Head.CommitSeq.Int64() != int64(index+1) {
			t.Fatalf("Assembly request %d Target = %+v", index, request.Target)
		}
		if request.SourceEventID != first.SourceEventID || request.ResidentID != first.ResidentID ||
			request.RunID != first.RunID || request.RunningOutcomeID != first.RunningOutcomeID ||
			request.RecallRunID != first.RecallRunID || request.GeneratorParams.String() != first.GeneratorParams.String() ||
			!reflect.DeepEqual(request.InputIDs, first.InputIDs) ||
			!reflect.DeepEqual(request.InputContentIDs, first.InputContentIDs) ||
			!reflect.DeepEqual(request.InputContentSalts, first.InputContentSalts) ||
			!reflect.DeepEqual(request.RecallUsageIDs, first.RecallUsageIDs) {
			t.Fatalf("Assembly request %d did not preserve its fixed identity/salt pool", index)
		}
	}
}

func TestCOV1PendingAssemblyBoundsProjectionReconcileFailureBeforeProvider(t *testing.T) {
	ids := canonical.NewSecureIDGenerator()
	residentID, err := ids.New()
	if err != nil {
		t.Fatal(err)
	}
	eventID, err := ids.New()
	if err != nil {
		t.Fatal(err)
	}
	repository := &cov1FailingAssemblyRepository{
		Snapshot: domain.ResidentSnapshot{ResidentID: residentID, Status: "active"},
	}
	reconciler := &cov1CountingAssemblyReconciler{Err: errors.New("injected Projection outage")}
	generator := &scriptedGenerator{}
	application := &Application{
		repository:         repository,
		commitNotifier:     reconciler,
		dialogueReconciler: reconciler,
		ids:                ids,
		generator:          generator,
		provider:           "test",
		model:              "test-model",
		maxInputBytes:      64 << 10,
		maxOutputBytes:     64 << 10,
	}

	err = application.processWork(context.Background(), residentID, domain.DialogueWork{
		UserEvent: domain.Event{ID: eventID, ResidentID: residentID, Type: "user_message"},
		State:     domain.WorkPending,
	})
	if err == nil || !strings.Contains(err.Error(), "did not stabilize after 3 attempts") {
		t.Fatalf("processWork error = %v, want bounded Projection reconcile failure", err)
	}
	if reconciler.Calls != dialogueAssemblyAttemptLimit || len(repository.Requests) != 0 {
		t.Fatalf("reconcile/Assembly calls = %d/%d, want %d/0",
			reconciler.Calls, len(repository.Requests), dialogueAssemblyAttemptLimit)
	}
	if got := generator.CallCount(); got != 0 {
		t.Fatalf("provider calls before successful reconcile/Prepare = %d, want 0", got)
	}
}

type cov1FailingAssemblyRepository struct {
	domain.Repository
	Snapshot domain.ResidentSnapshot
	Err      error
	Requests []domain.DialogueAssemblyRequest
}

func (repository *cov1FailingAssemblyRepository) ActiveResident(context.Context) (domain.ResidentSnapshot, error) {
	return repository.Snapshot, nil
}

func (repository *cov1FailingAssemblyRepository) AssembleDialogue(
	_ context.Context,
	request domain.DialogueAssemblyRequest,
) (domain.DialogueAssemblyResult, error) {
	copyRequest := request
	copyRequest.InputIDs = append([]canonical.ID(nil), request.InputIDs...)
	copyRequest.InputContentIDs = append([]canonical.ID(nil), request.InputContentIDs...)
	copyRequest.InputContentSalts = append([]canonical.ContentSalt(nil), request.InputContentSalts...)
	copyRequest.RecallUsageIDs = append([]canonical.ID(nil), request.RecallUsageIDs...)
	repository.Requests = append(repository.Requests, copyRequest)
	return domain.DialogueAssemblyResult{}, repository.Err
}

type cov1CountingAssemblyReconciler struct {
	Calls int
	Err   error
}

func (reconciler *cov1CountingAssemblyReconciler) NotifyCommit(canonical.CommitMetadata) bool {
	return true
}

type cov1FailingAssemblyReconciler struct {
	Called        chan struct{}
	Notifications int
}

func (reconciler *cov1FailingAssemblyReconciler) NotifyCommit(canonical.CommitMetadata) bool {
	reconciler.Notifications++
	return false
}

func (reconciler *cov1FailingAssemblyReconciler) ReconcileDialogueAssembly(
	context.Context,
	canonical.ID,
) (canonical.Head, canonical.Instant, canonical.Timezone, error) {
	select {
	case reconciler.Called <- struct{}{}:
	default:
	}
	return canonical.Head{}, 0, "", errors.New("injected Prepare hint failure")
}

func (reconciler *cov1CountingAssemblyReconciler) ReconcileDialogueAssembly(
	context.Context,
	canonical.ID,
) (canonical.Head, canonical.Instant, canonical.Timezone, error) {
	reconciler.Calls++
	instant := canonical.Instant(100 + reconciler.Calls)
	return canonical.Head{
		Exists: true, CommitSeq: canonical.CommitSeq(reconciler.Calls), CommittedAt: instant,
	}, instant, canonical.MustTimezone("UTC"), reconciler.Err
}
