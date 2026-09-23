package app

import (
	"context"
	"errors"
	"sync"
	"testing"

	"mahoroba.local/mahoroba/internal/canonical"
	"mahoroba.local/mahoroba/internal/domain"
	"mahoroba.local/mahoroba/internal/operationalmetrics"
)

type crashRetryRecoveryRepository struct {
	mu      sync.Mutex
	running bool
	attempt domain.RunningAttempt
}

func (repository *crashRetryRecoveryRepository) RunningAttempts(context.Context, int) ([]domain.RunningAttempt, error) {
	repository.mu.Lock()
	defer repository.mu.Unlock()
	if !repository.running {
		return nil, nil
	}
	return []domain.RunningAttempt{repository.attempt}, nil
}

func TestM7RecoveryTerminalizerCrashRetryDoesNotAppendSecondTerminalOutcome(t *testing.T) {
	repository := &crashRetryRecoveryRepository{
		running: true,
		attempt: domain.RunningAttempt{
			RunID:      mustRecoveryTestID(t, "01J00000000000000000000031"),
			ResidentID: mustRecoveryTestID(t, "01J00000000000000000000032"),
			AttemptNo:  1,
		},
	}
	committedThenCrashed := errors.New("injected crash after durable commit")
	var submitCalls int
	terminalizer, err := NewRecoveryTerminalizer(RecoveryTerminalizerOptions{
		Repository: repository,
		IDs:        canonical.NewSecureIDGenerator(),
		Submit: func(_ context.Context, command canonical.Command) (canonical.CommandResult, error) {
			if command == nil || command.Name() != "FailGenerationAttempt" {
				t.Fatalf("recovery command = %#v", command)
			}
			submitCalls++
			repository.mu.Lock()
			repository.running = false // model the Canonical commit becoming durable
			repository.mu.Unlock()
			return canonical.CommandResult{}, committedThenCrashed
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := terminalizer.Terminalize(context.Background()); !errors.Is(err, committedThenCrashed) {
		t.Fatalf("first terminalization = %v, want injected crash", err)
	}
	result, err := terminalizer.Terminalize(context.Background())
	if err != nil {
		t.Fatalf("retry terminalization: %v", err)
	}
	if submitCalls != 1 || result.TerminalizedAttempts != 0 || len(result.CanonicalCommits) != 0 {
		t.Fatalf("retry calls/result = %d/%+v, want no duplicate outcome", submitCalls, result)
	}
}

func mustRecoveryTestID(t *testing.T, raw string) canonical.ID {
	t.Helper()
	id, err := canonical.ParseID(raw)
	if err != nil {
		t.Fatal(err)
	}
	return id
}

type mandatoryObserverRepository struct {
	works []domain.MandatoryRecoveryWork
}

func (*mandatoryObserverRepository) RunningAttempts(context.Context, int) ([]domain.RunningAttempt, error) {
	return nil, nil
}

func (repository *mandatoryObserverRepository) DiscoverMandatoryRecoveryWork(
	context.Context, *domain.MandatoryRecoveryCursor, int,
) ([]domain.MandatoryRecoveryWork, *domain.MandatoryRecoveryCursor, error) {
	return append([]domain.MandatoryRecoveryWork(nil), repository.works...), nil, nil
}

func (*mandatoryObserverRepository) ResolveMandatoryCancellationEnvelope(
	context.Context, domain.MandatoryRecoveryWork,
) (domain.CancellationEnvelopeResolution, error) {
	return domain.CancellationEnvelopeResolution{}, errors.New("not used by preflight")
}

func TestM7MandatoryRecoveryObserverPublishesClosedStateCounts(t *testing.T) {
	repository := &mandatoryObserverRepository{works: []domain.MandatoryRecoveryWork{
		{Kind: domain.MandatoryRecoveryDialogue, State: domain.WorkPending},
		{Kind: domain.MandatoryRecoveryDialogue, State: domain.WorkRunning, RunID: recoveryIDPointer(t, "01J00000000000000000000041"), AttemptNo: 1},
		{Kind: domain.MandatoryRecoveryDialogue, State: domain.WorkRetryPending, RunID: recoveryIDPointer(t, "01J00000000000000000000042"), AttemptNo: 2},
		{Kind: domain.MandatoryRecoveryMemoryExtraction, State: domain.WorkPending},
		{Kind: domain.MandatoryRecoveryMemoryExtraction, State: domain.WorkRunning, RunID: recoveryIDPointer(t, "01J00000000000000000000043"), AttemptNo: 3},
		{Kind: domain.MandatoryRecoveryMemoryExtraction, State: domain.WorkRetryPending, RunID: recoveryIDPointer(t, "01J00000000000000000000044"), AttemptNo: 4},
	}}
	metrics := operationalmetrics.New()
	terminalizer, err := NewRecoveryTerminalizer(RecoveryTerminalizerOptions{
		Repository: repository, MandatoryRepository: repository, EnvelopeResolver: repository,
		IDs: canonical.NewSecureIDGenerator(), Submit: func(context.Context, canonical.Command) (canonical.CommandResult, error) {
			return canonical.CommandResult{}, nil
		}, Observer: metrics,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := terminalizer.PreflightMandatory(context.Background()); err != nil {
		t.Fatal(err)
	}
	want := operationalmetrics.MandatoryWorkCounts{
		DialoguePending: 1, DialogueRunning: 1, DialogueRetryPending: 1,
		MemoryPending: 1, MemoryRunning: 1, MemoryRetryPending: 1,
	}
	if got := metrics.Snapshot().Mandatory; got != want {
		t.Fatalf("mandatory metrics = %+v, want %+v", got, want)
	}
}

func TestM7ApplicationPropagatesOperationalObserverToRecoveryTerminalizer(t *testing.T) {
	metrics := operationalmetrics.New()
	application := &Application{
		repository:          &contextRepository{},
		ids:                 canonical.NewSecureIDGenerator(),
		operationalObserver: metrics,
	}
	terminalizer, err := application.newRecoveryTerminalizer()
	if err != nil {
		t.Fatal(err)
	}
	if terminalizer.observer != metrics {
		t.Fatal("Application did not propagate its operational observer to recovery")
	}
}

func recoveryIDPointer(t *testing.T, raw string) *canonical.ID {
	id := mustRecoveryTestID(t, raw)
	return &id
}
