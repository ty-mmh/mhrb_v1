package app

import (
	"context"
	"database/sql"
	"errors"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"mahoroba.local/mahoroba/internal/blob"
	"mahoroba.local/mahoroba/internal/canonical"
	"mahoroba.local/mahoroba/internal/domain"
	"mahoroba.local/mahoroba/internal/generation"
	"mahoroba.local/mahoroba/internal/store/sqlite"
)

func TestLandingFailureIsDurableAndRetryableAfterRestart(t *testing.T) {
	ctx := context.Background()
	fixture := newApplicationFixture(t, &scriptedGenerator{steps: []generatorStep{{text: "first provider result"}}}, 3)
	event, err := fixture.application.Ingress(ctx, "land this after restart")
	if err != nil {
		t.Fatal(err)
	}
	_ = dialogueRunIDForEventForTest(t, fixture, event)
	work, err := discoverDialogueWorkForTest(ctx, fixture.repository, fixture.residentID, 10, 3)
	if err != nil {
		t.Fatal(err)
	}
	if len(work) != 1 || work[0].RunID == nil || work[0].State != domain.WorkRunning {
		t.Fatalf("initial work = %+v, want one running generation", work)
	}
	runID := *work[0].RunID

	failureDB, err := sql.Open("sqlite", fixture.store.Path())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := failureDB.ExecContext(ctx, `CREATE TRIGGER test_reject_resident_message
		BEFORE INSERT ON events WHEN NEW.event_type = 'resident_message'
		BEGIN SELECT RAISE(ABORT, 'injected landing failure'); END`); err != nil {
		_ = failureDB.Close()
		t.Fatal(err)
	}
	landingErr := fixture.application.processPreparedRun(ctx, fixture.residentID, runID)
	if landingErr == nil || !strings.Contains(landingErr.Error(), "land dialogue after provider success") {
		t.Fatalf("landing error = %v, want durable local landing failure", landingErr)
	}
	if _, err := failureDB.ExecContext(ctx, `DROP TRIGGER test_reject_resident_message`); err != nil {
		_ = failureDB.Close()
		t.Fatal(err)
	}
	if err := failureDB.Close(); err != nil {
		t.Fatal(err)
	}

	var state, code string
	if err := fixture.store.Reader().QueryRowContext(ctx, `SELECT state, error_class
		FROM generation_run_outcomes WHERE generation_run_id = ?
		ORDER BY outcome_id DESC LIMIT 1`, runID.String()).Scan(&state, &code); err != nil {
		t.Fatal(err)
	}
	if state != "failed" || code != string(generation.ErrorLandingFailure) {
		t.Fatalf("landing outcome = state=%q code=%q, want failed/landing_failure", state, code)
	}
	work, err = discoverDialogueWorkForTest(ctx, fixture.repository, fixture.residentID, 10, 3)
	if err != nil {
		t.Fatal(err)
	}
	if len(work) != 1 || work[0].State != domain.WorkRetryPending || work[0].AttemptNo != 1 {
		t.Fatalf("work after landing failure = %+v, want retry_pending attempt 1", work)
	}

	fixture.application.Stop()
	if err := fixture.application.Wait(ctx); err != nil {
		t.Fatal(err)
	}
	if err := fixture.writer.Close(ctx); err != nil {
		t.Fatal(err)
	}
	if err := fixture.store.Close(); err != nil {
		t.Fatal(err)
	}

	reopenedStore, err := sqlite.Open(ctx, filepath.Join(fixture.root, "mahoroba.db"))
	if err != nil {
		t.Fatal(err)
	}
	reopenedIDs := canonical.NewSecureIDGenerator()
	reopenedWriter, err := canonical.OpenWriter(ctx, canonical.WriterOptions{
		Backend: reopenedStore.Canonical(), IDs: reopenedIDs, Clock: fixture.clock,
		Timezone: canonical.MustTimezone("UTC"), QueueCapacity: 32,
	})
	if err != nil {
		_ = reopenedStore.Close()
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = reopenedWriter.Close(context.Background())
		_ = reopenedStore.Close()
	})
	reopenedBlobs, err := blob.NewFileStore(filepath.Join(fixture.root, "blobs"))
	if err != nil {
		t.Fatal(err)
	}
	restartGenerator := &scriptedGenerator{steps: []generatorStep{{text: "landed after restart"}}}
	restarted, err := New(Options{
		Writer: reopenedWriter, Repository: reopenedStore.Canonical(), IDs: reopenedIDs, Clock: fixture.clock,
		Timezone: canonical.MustTimezone("UTC"), Blobs: reopenedBlobs, Generator: restartGenerator,
		Provider: "test", Model: "test-model", MaxAttempts: 3,
		RetryBackoff: []time.Duration{0, 0}, MaxInputBytes: 64 << 10, MaxOutputBytes: 64 << 10,
		SafetyScanInterval: time.Hour,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := restarted.Recover(ctx); err != nil {
		t.Fatal(err)
	}
	if err := restarted.ProcessResident(ctx, fixture.residentID); err != nil {
		t.Fatal(err)
	}
	if restartGenerator.CallCount() != 1 {
		t.Fatalf("provider calls after restart = %d, want 1", restartGenerator.CallCount())
	}
	history, err := reopenedStore.Canonical().History(ctx, fixture.residentID, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(history) != 2 || history[1].Content != "landed after restart" {
		t.Fatalf("history after landing recovery = %+v", history)
	}
}

func TestFormerOperationalResidentIsCancelledWithoutProviderCall(t *testing.T) {
	ctx := context.Background()
	generator := &scriptedGenerator{}
	fixture := newApplicationFixture(t, generator, 3)
	event, err := fixture.application.Ingress(ctx, "belongs to the former selection")
	if err != nil {
		t.Fatal(err)
	}
	_ = dialogueRunIDForEventForTest(t, fixture, event)
	work, err := discoverDialogueWorkForTest(ctx, fixture.repository, fixture.residentID, 10, 3)
	if err != nil {
		t.Fatal(err)
	}
	if len(work) != 1 || work[0].RunID == nil {
		t.Fatalf("initial work = %+v", work)
	}
	runID := *work[0].RunID

	state, err := fixture.application.BootstrapInit(ctx, BootstrapInput{
		OwnerName: "Owner", Name: "Second", SeedKey: "resident-2", Principles: "be helpful",
	})
	if err != nil {
		t.Fatal(err)
	}
	var second canonical.ID
	for _, resident := range state.Residents {
		if resident.SeedKey == "resident-2" {
			second = resident.ResidentID
		}
	}
	if second.IsZero() {
		t.Fatal("second resident was not created")
	}
	if err := fixture.application.ApprovePrinciples(ctx, second); err != nil {
		t.Fatal(err)
	}
	if err := fixture.application.FinalizeBootstrap(ctx, second, "friendly",
		`{"mandatory_event_types":[],"memory_recall_enabled":false,"version":"memory-policy-v1"}`); err != nil {
		t.Fatal(err)
	}
	if err := fixture.application.SelectResident(ctx, second); err != nil {
		t.Fatal(err)
	}

	// The direct post-commit task must re-check Operational selection after it
	// acquires the resident lock, not call the provider for the old selection.
	if err := fixture.application.processPreparedRun(ctx, fixture.residentID, runID); err != nil {
		t.Fatal(err)
	}
	if generator.CallCount() != 0 {
		t.Fatalf("provider calls for former selection = %d, want 0", generator.CallCount())
	}
	// Startup recovery first closes the pre-crash running attempt, then its
	// unavailable-resident scan terminalizes the still-mandatory retry without
	// relying on the resident ever becoming Canonically archived.
	if err := fixture.application.Start(ctx); err != nil {
		t.Fatal(err)
	}
	work, err = discoverDialogueWorkForTest(ctx, fixture.repository, fixture.residentID, 10, 3)
	if err != nil {
		t.Fatal(err)
	}
	if len(work) != 1 || work[0].State != domain.WorkTerminalFailed || work[0].AttemptNo != 2 {
		t.Fatalf("former selection work = %+v, want startup terminal cancellation on attempt 2", work)
	}
	var code string
	if err := fixture.store.Reader().QueryRowContext(ctx, `SELECT error_class FROM generation_run_outcomes
		WHERE generation_run_id = ? ORDER BY outcome_id DESC LIMIT 1`, runID.String()).Scan(&code); err != nil {
		t.Fatal(err)
	}
	if code != string(generation.ErrorResidentUnselected) {
		t.Fatalf("former selection cancellation = %q, want %q", code, generation.ErrorResidentUnselected)
	}
	var outcomeCount int
	if err := fixture.store.Reader().QueryRowContext(ctx, `SELECT COUNT(*) FROM generation_run_outcomes
		WHERE generation_run_id = ?`, runID.String()).Scan(&outcomeCount); err != nil {
		t.Fatal(err)
	}
	fixture.application.Stop()
	if err := fixture.application.Wait(ctx); err != nil {
		t.Fatal(err)
	}
	if err := fixture.application.cancelUnavailableResidentWork(ctx); err != nil {
		t.Fatal(err)
	}
	var secondCount int
	if err := fixture.store.Reader().QueryRowContext(ctx, `SELECT COUNT(*) FROM generation_run_outcomes
		WHERE generation_run_id = ?`, runID.String()).Scan(&secondCount); err != nil {
		t.Fatal(err)
	}
	if secondCount != outcomeCount {
		t.Fatalf("idempotent cancellation outcomes = %d, want %d", secondCount, outcomeCount)
	}
}

func TestApplicationStopWaitPersistsInterruptionBeforePersistenceClose(t *testing.T) {
	generator := newCancellationGenerator()
	fixture := newApplicationFixture(t, generator, 3)
	if err := fixture.application.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	if _, err := fixture.application.Ingress(context.Background(), "interrupt this provider call"); err != nil {
		t.Fatal(err)
	}
	select {
	case <-generator.started:
	case <-time.After(5 * time.Second):
		t.Fatal("provider did not start")
	}
	fixture.application.Stop()
	waitCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := fixture.application.Wait(waitCtx); err != nil {
		t.Fatal(err)
	}
	select {
	case <-generator.returned:
	default:
		t.Fatal("Wait returned before the generation goroutine")
	}
	if _, err := fixture.application.Ingress(context.Background(), "must be rejected"); !errors.Is(err, ErrApplicationStopping) {
		t.Fatalf("post-stop ingress error = %v, want ErrApplicationStopping", err)
	}
	running, err := fixture.repository.RunningAttempts(context.Background(), 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(running) != 0 {
		t.Fatalf("running attempts after Wait = %+v", running)
	}
	var state, code string
	if err := fixture.store.Reader().QueryRowContext(context.Background(), `SELECT state, error_class
		FROM generation_run_outcomes ORDER BY outcome_id DESC LIMIT 1`).Scan(&state, &code); err != nil {
		t.Fatal(err)
	}
	if state != "failed" || code != generation.RuntimeInterruptedErrorCode().String() {
		t.Fatalf("shutdown outcome = state=%q code=%q", state, code)
	}
	if err := fixture.writer.Close(waitCtx); err != nil {
		t.Fatal(err)
	}
	if err := fixture.store.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestIngressRejectsCancelledOwnedRunContextBeforeExplicitStop(t *testing.T) {
	fixture := newApplicationFixture(t, &scriptedGenerator{}, 2)
	runCtx, cancelRun := context.WithCancel(context.Background())
	if err := fixture.application.Start(runCtx); err != nil {
		t.Fatal(err)
	}
	cancelRun()
	if _, err := fixture.application.Ingress(context.Background(), "too late"); !errors.Is(err, ErrApplicationStopping) {
		t.Fatalf("ingress after run cancellation = %v, want ErrApplicationStopping", err)
	}
	fixture.application.Stop()
	waitCtx, cancelWait := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancelWait()
	if err := fixture.application.Wait(waitCtx); err != nil {
		t.Fatal(err)
	}
}

type cancellationGenerator struct {
	started  chan struct{}
	returned chan struct{}
	once     sync.Once
}

func newCancellationGenerator() *cancellationGenerator {
	return &cancellationGenerator{started: make(chan struct{}), returned: make(chan struct{})}
}

func (generator *cancellationGenerator) Stream(ctx context.Context, _ generation.Request, _ generation.DeltaSink) (generation.Result, error) {
	generator.once.Do(func() { close(generator.started) })
	<-ctx.Done()
	close(generator.returned)
	return generation.Result{}, &generation.ProviderError{Class: generation.ErrorCancelled, Cause: ctx.Err()}
}

var _ generation.Generator = (*cancellationGenerator)(nil)
