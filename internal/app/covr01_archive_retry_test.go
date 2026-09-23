package app

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"mahoroba.local/mahoroba/internal/canonical"
	"mahoroba.local/mahoroba/internal/domain"
	"mahoroba.local/mahoroba/internal/generation"
	"mahoroba.local/mahoroba/internal/operationalmetrics"
	"mahoroba.local/mahoroba/internal/store/sqlite"
)

type covr01ArchiveCleanupFailOnceRepository struct {
	*sqlite.CanonicalRepository

	mu      sync.Mutex
	failure error
	calls   int
}

type covr01AmbiguousCommitBackend struct {
	delegate canonical.Backend

	mu      sync.Mutex
	failure error
}

func (backend *covr01AmbiguousCommitBackend) LoadHead(ctx context.Context) (canonical.Head, error) {
	return backend.delegate.LoadHead(ctx)
}

func (backend *covr01AmbiguousCommitBackend) Begin(
	ctx context.Context,
	metadata canonical.CommitMetadata,
) (canonical.CanonicalUoW, error) {
	uow, err := backend.delegate.Begin(ctx, metadata)
	if err != nil {
		return nil, err
	}
	mutations, ok := uow.(domain.MutationStore)
	if !ok {
		_ = uow.Rollback(context.WithoutCancel(ctx))
		return nil, errors.New("test backend UoW lacks domain mutation capability")
	}
	return &covr01AmbiguousCommitUoW{
		CanonicalUoW:  uow,
		MutationStore: mutations,
		backend:       backend,
	}, nil
}

type covr01AmbiguousCommitUoW struct {
	canonical.CanonicalUoW
	domain.MutationStore
	backend *covr01AmbiguousCommitBackend
}

func (uow *covr01AmbiguousCommitUoW) Commit(ctx context.Context) error {
	if err := uow.CanonicalUoW.Commit(ctx); err != nil {
		return err
	}
	uow.backend.mu.Lock()
	failure := uow.backend.failure
	uow.backend.failure = nil
	uow.backend.mu.Unlock()
	return failure
}

func (repository *covr01ArchiveCleanupFailOnceRepository) RunningAttemptsForResident(
	ctx context.Context,
	residentID canonical.ID,
	limit int,
) ([]domain.RunningAttempt, error) {
	repository.mu.Lock()
	repository.calls++
	failure := repository.failure
	repository.failure = nil
	repository.mu.Unlock()
	if failure != nil {
		return nil, failure
	}
	return repository.CanonicalRepository.RunningAttemptsForResident(ctx, residentID, limit)
}

func (repository *covr01ArchiveCleanupFailOnceRepository) Calls() int {
	repository.mu.Lock()
	defer repository.mu.Unlock()
	return repository.calls
}

func TestCOVR01ArchiveRetryResumesCleanupWithoutRepeatingStatusMutation(t *testing.T) {
	ctx := context.Background()
	fixture := newApplicationFixture(t, &scriptedGenerator{}, 2)
	injected := errors.New("injected archived cleanup discovery failure")
	repository := &covr01ArchiveCleanupFailOnceRepository{
		CanonicalRepository: fixture.repository,
		failure:             injected,
	}
	fixture.application.repository = repository

	if err := fixture.application.ArchiveResident(ctx, fixture.residentID); !errors.Is(err, injected) {
		t.Fatalf("first ArchiveResident error = %v, want injected cleanup failure", err)
	}
	resident, err := fixture.repository.Resident(ctx, fixture.residentID)
	if err != nil {
		t.Fatal(err)
	}
	if resident.Status != "archived" {
		t.Fatalf("resident status after failed cleanup = %q, want archived", resident.Status)
	}

	if err := fixture.application.ArchiveResident(ctx, fixture.residentID); err != nil {
		t.Fatalf("retry ArchiveResident should resume cleanup: %v", err)
	}
	if calls := repository.Calls(); calls != 2 {
		t.Fatalf("resident running-attempt discovery calls = %d, want failed call plus retry", calls)
	}
	var transitions int
	if err := fixture.store.Reader().QueryRowContext(ctx, `SELECT COUNT(*)
		FROM resident_status_transitions
		WHERE resident_id = ? AND from_status = 'active' AND to_status = 'archived'`,
		fixture.residentID.String(),
	).Scan(&transitions); err != nil {
		t.Fatal(err)
	}
	if transitions != 1 {
		t.Fatalf("active-to-archived transitions = %d, want exactly one", transitions)
	}
}

func TestCOVR01AmbiguousArchiveCommitStillRunsCleanupAndRetrySkipsMutation(t *testing.T) {
	ctx := context.Background()
	fixture := newApplicationFixture(t, &scriptedGenerator{}, 2)
	prepareRunningRecoveryAttemptsForTest(t, fixture, 1)
	running, err := fixture.repository.RunningAttemptsForResident(ctx, fixture.residentID, 2)
	if err != nil {
		t.Fatal(err)
	}
	if len(running) != 1 {
		t.Fatalf("running attempts before ambiguous archive = %d, want 1", len(running))
	}
	runID := running[0].RunID
	if err := fixture.writer.Close(ctx); err != nil {
		t.Fatalf("close bootstrap writer: %v", err)
	}
	injected := errors.New("injected ambiguous archive commit acknowledgement")
	backend := &covr01AmbiguousCommitBackend{delegate: fixture.repository, failure: injected}
	writer, err := canonical.OpenWriter(ctx, canonical.WriterOptions{
		Backend: backend, IDs: fixture.application.ids, Clock: fixture.clock,
		Timezone: fixture.application.timezone, QueueCapacity: 32,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := writer.Close(context.Background()); err != nil {
			t.Errorf("close ambiguous writer: %v", err)
		}
	})
	repository := &covr01ArchiveCleanupFailOnceRepository{CanonicalRepository: fixture.repository}
	fixture.application.writer = writer
	fixture.application.repository = repository

	archiveErr := fixture.application.ArchiveResident(ctx, fixture.residentID)
	if !errors.Is(archiveErr, injected) || !errors.Is(archiveErr, canonical.ErrWriterPoisoned) {
		t.Fatalf("ambiguous ArchiveResident error = %v, want acknowledgement and poisoned cleanup failures", archiveErr)
	}
	if calls := repository.Calls(); calls != 1 {
		t.Fatalf("cleanup discovery calls after ambiguous commit = %d, want 1", calls)
	}
	resident, err := fixture.repository.Resident(ctx, fixture.residentID)
	if err != nil {
		t.Fatal(err)
	}
	if resident.Status != "archived" {
		t.Fatalf("resident status after ambiguous commit = %q, want archived", resident.Status)
	}

	// The ambiguous acknowledgement poisons this Writer. Archive retry still
	// skips the already-durable status mutation, but write-required cleanup must
	// fail closed until a healthy Writer reloads the Canonical head.
	if err := fixture.application.ArchiveResident(ctx, fixture.residentID); !errors.Is(err, canonical.ErrWriterPoisoned) {
		t.Fatalf("same-writer retry error = %v, want poisoned cleanup failure", err)
	}
	if calls := repository.Calls(); calls != 2 {
		t.Fatalf("cleanup discovery calls after retry = %d, want 2", calls)
	}
	if err := writer.Close(ctx); err != nil {
		t.Fatalf("close poisoned writer: %v", err)
	}
	recoveredWriter, err := canonical.OpenWriter(ctx, canonical.WriterOptions{
		Backend: fixture.repository, IDs: fixture.application.ids, Clock: fixture.clock,
		Timezone: fixture.application.timezone, QueueCapacity: 32,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := recoveredWriter.Close(context.Background()); err != nil {
			t.Errorf("close recovered writer: %v", err)
		}
	})
	restarted, err := New(Options{
		Writer: recoveredWriter, Repository: repository, IDs: fixture.application.ids,
		Clock: fixture.clock, Timezone: fixture.application.timezone, Blobs: fixture.blobs,
		Generator: &scriptedGenerator{}, Provider: "test", Model: "test-model",
		MaxAttempts: 2, RetryBackoff: []time.Duration{0}, MaxInputBytes: 64 << 10,
		MaxOutputBytes: 64 << 10, SafetyScanInterval: time.Hour,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := restarted.ArchiveResident(ctx, fixture.residentID); err != nil {
		t.Fatalf("retry after healthy Writer reload: %v", err)
	}
	if calls := repository.Calls(); calls < 4 {
		t.Fatalf("cleanup discovery calls after healthy retry = %d, want running cancellation plus empty verification", calls)
	}
	var state, errorClass string
	if err := fixture.store.Reader().QueryRowContext(ctx, `SELECT state, COALESCE(error_class, '')
		FROM generation_run_outcomes WHERE generation_run_id = ?
		ORDER BY outcome_id DESC LIMIT 1`, runID.String()).Scan(&state, &errorClass); err != nil {
		t.Fatal(err)
	}
	if state != "cancelled" || errorClass != string(generation.ErrorResidentInactive) {
		t.Fatalf("recovered cleanup outcome = %s/%s, want cancelled/resident_inactive", state, errorClass)
	}
	var transitions int
	if err := fixture.store.Reader().QueryRowContext(ctx, `SELECT COUNT(*)
		FROM resident_status_transitions
		WHERE resident_id = ? AND from_status = 'active' AND to_status = 'archived'`,
		fixture.residentID.String(),
	).Scan(&transitions); err != nil {
		t.Fatal(err)
	}
	if transitions != 1 {
		t.Fatalf("active-to-archived transitions = %d, want exactly one", transitions)
	}
}

func TestCOVR01DialogueProviderFailureObserverArchiveUnwindsWithoutSecondOutcome(t *testing.T) {
	ctx := context.Background()
	providerErr := &generation.ProviderError{
		Class: generation.ErrorHTTP, Retryable: true, StatusCode: 503, Detail: "test provider failure",
	}
	generator := &scriptedGenerator{steps: []generatorStep{{err: providerErr}}}
	fixture := newApplicationFixture(t, generator, 2)
	source, err := fixture.application.Ingress(ctx, "archive after dialogue provider failure")
	if err != nil {
		t.Fatal(err)
	}
	observer := &covr01ArchiveOnProviderFinishedObserver{
		application: fixture.application,
		residentID:  fixture.residentID,
		key:         domain.DialogueObligation(source.ID),
		reader:      fixture.store.Reader(),
		observed:    make(chan covr01ArchiveProviderObservation, 1),
	}
	fixture.application.operationalObserver = observer
	fixture.application.generator = observedGenerator{
		delegate: generator, observer: observer, providerIdentity: "test",
	}

	done := make(chan error, 1)
	go func() {
		done <- fixture.application.ProcessResident(ctx, fixture.residentID)
	}()

	var observation covr01ArchiveProviderObservation
	select {
	case observation = <-observer.observed:
	case err := <-done:
		t.Fatalf("ProcessResident returned before ProviderFinished archive observation: %v", err)
	case <-time.After(5 * time.Second):
		t.Fatal("ProviderFinished callback deadlocked while archiving failed dialogue")
	}
	if observation.terminal != operationalmetrics.ProviderRetryableFailure {
		t.Fatalf("provider terminal = %v, want retryable failure", observation.terminal)
	}
	if observation.archiveErr != nil {
		t.Fatalf("ArchiveResident from ProviderFinished: %v", observation.archiveErr)
	}
	if observation.queryErr != nil {
		t.Fatalf("query immediately after ArchiveResident returned: %v", observation.queryErr)
	}
	if observation.runID == "" || observation.attemptNo != 1 || observation.state != "cancelled" ||
		observation.errorClass != string(generation.ErrorResidentInactive) || observation.latestRunning != 0 {
		t.Fatalf("immediate archived dialogue outcome = run:%s attempt:%d state:%s class:%s latest-running:%d",
			observation.runID, observation.attemptNo, observation.state,
			observation.errorClass, observation.latestRunning)
	}
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("ProcessResident after synchronous archive: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("ProcessResident did not unwind after ProviderFinished archive returned")
	}
	if calls := generator.CallCount(); calls != 1 {
		t.Fatalf("dialogue provider calls = %d, want 1", calls)
	}
}
