package app

import (
	"context"
	"sync"
	"testing"
	"time"

	"mahoroba.local/mahoroba/internal/canonical"
	"mahoroba.local/mahoroba/internal/domain"
	"mahoroba.local/mahoroba/internal/generation"
)

func TestCOVR01AdminReextractCancelsErasedRetryWhileResidentUnselectedWithoutProvider(t *testing.T) {
	ctx := context.Background()
	fixture := newApplicationFixture(t, &scriptedGenerator{steps: []generatorStep{{text: "dialogue"}}}, 2)
	event, err := fixture.application.Ingress(ctx, "erase retry while its resident is unselected")
	if err != nil {
		t.Fatal(err)
	}
	if err := fixture.application.ProcessResident(ctx, fixture.residentID); err != nil {
		t.Fatal(err)
	}
	if _, err := fixture.application.activateMemoryPolicyV4ForTest(ctx, fixture.residentID); err != nil {
		t.Fatal(err)
	}

	reextractGenerator := &scriptedGenerator{steps: []generatorStep{{
		err: &generation.ProviderError{Class: generation.ErrorTimeout, Detail: "retryable extraction timeout"},
	}}}
	fixture.application.generator = reextractGenerator
	requestID := memoryExtractionTestID(t, fixture)
	first, err := fixture.application.ReextractMemoryEvent(ctx, fixture.residentID, event.ID, requestID)
	if err != nil {
		t.Fatal(err)
	}
	if first.State != domain.WorkRetryPending || first.AttemptNo != 1 || reextractGenerator.CallCount() != 1 {
		t.Fatalf("retryable first extraction = %+v calls=%d", first, reextractGenerator.CallCount())
	}

	eraseEventContentForTest(t, fixture.store.Path(), event.ContentID)
	selected := createCOVR01ActiveResident(t, fixture, "covr01-erased-reextract-crossing")
	if err := fixture.application.SelectResident(ctx, selected); err != nil {
		t.Fatal(err)
	}
	second, err := fixture.application.ReextractMemoryEvent(ctx, fixture.residentID, event.ID, requestID)
	if err != nil {
		t.Fatal(err)
	}
	if second.RunID != first.RunID || second.State != domain.WorkTerminalFailed || second.AttemptNo != 2 || second.Changed {
		t.Fatalf("unselected erased retry cancellation = %+v, first=%+v", second, first)
	}
	if reextractGenerator.CallCount() != 1 {
		t.Fatalf("unselected erased retry called provider %d times, want 1 total", reextractGenerator.CallCount())
	}

	var state, errorClass string
	var attemptNo int64
	if err := fixture.store.Reader().QueryRow(`SELECT state, COALESCE(error_class, ''), attempt_no
		FROM generation_run_outcomes WHERE generation_run_id = ?
		ORDER BY outcome_id DESC LIMIT 1`, second.RunID.String()).Scan(&state, &errorClass, &attemptNo); err != nil {
		t.Fatal(err)
	}
	if state != "cancelled" || errorClass != "source_content_erased" || attemptNo != 2 {
		t.Fatalf("unselected erased retry outcome = %s/%s attempt %d", state, errorClass, attemptNo)
	}
}

type covr01ArchiveReentryObserver struct {
	application *Application
	residentID  canonical.ID
	once        sync.Once
	entered     chan struct{}
	returned    chan error
}

func (*covr01ArchiveReentryObserver) UserCommitted(domain.Event) {}

func (observer *covr01ArchiveReentryObserver) GenerationStarted(canonical.ID, canonical.ID, int64) {
	observer.once.Do(func() {
		close(observer.entered)
		observer.returned <- observer.application.ArchiveResident(context.Background(), observer.residentID)
	})
}

func (*covr01ArchiveReentryObserver) GenerationDelta(canonical.ID, canonical.ID, string) {}
func (*covr01ArchiveReentryObserver) ResidentCommitted(domain.Event)                     {}
func (*covr01ArchiveReentryObserver) GenerationFailed(canonical.ID, canonical.ID, int64, string, bool) {
}

func TestCOVR01ObserverArchiveReentryDoesNotDeadlockResidentTurn(t *testing.T) {
	ctx := context.Background()
	generator := &scriptedGenerator{steps: []generatorStep{{text: "must not land after archive"}}}
	fixture := newApplicationFixture(t, generator, 1)
	if _, err := fixture.application.Ingress(ctx, "archive synchronously from GenerationStarted"); err != nil {
		t.Fatal(err)
	}
	observer := &covr01ArchiveReentryObserver{
		application: fixture.application,
		residentID:  fixture.residentID,
		entered:     make(chan struct{}),
		returned:    make(chan error, 1),
	}
	fixture.application.SetObserver(observer)

	processDone := make(chan error, 1)
	go func() { processDone <- fixture.application.ProcessResident(ctx, fixture.residentID) }()
	select {
	case <-observer.entered:
	case <-time.After(5 * time.Second):
		t.Fatal("resident turn did not reach the synchronous Observer archive callback")
	}
	select {
	case err := <-observer.returned:
		if err != nil {
			t.Fatalf("synchronous Observer archive: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("ArchiveResident deadlocked while re-entered from GenerationStarted")
	}
	select {
	case <-processDone:
		// The crossing may return a landing error after the archive commits. This
		// assertion is deliberately about bounded completion of the resident turn.
	case <-time.After(5 * time.Second):
		t.Fatal("ProcessResident did not complete after synchronous Observer archive returned")
	}

	resident, err := fixture.repository.Resident(ctx, fixture.residentID)
	if err != nil {
		t.Fatal(err)
	}
	if resident.Status != "archived" || generator.CallCount() != 0 {
		t.Fatalf("archive crossing status/calls = %s/%d, want archived/0", resident.Status, generator.CallCount())
	}
	var state, errorClass string
	var attemptNo int64
	if err := fixture.store.Reader().QueryRow(`SELECT outcome.state, COALESCE(outcome.error_class, ''), outcome.attempt_no
		FROM generation_runs run
		JOIN generation_run_outcomes outcome ON outcome.generation_run_id = run.generation_run_id
		WHERE run.resident_id = ? AND run.idempotency_key LIKE 'dialogue:v1:%'
		ORDER BY outcome.outcome_id DESC LIMIT 1`, fixture.residentID.String()).Scan(
		&state, &errorClass, &attemptNo,
	); err != nil {
		t.Fatal(err)
	}
	if state != "cancelled" || errorClass != "resident_inactive" || attemptNo != 1 {
		t.Fatalf("archive crossing dialogue outcome = %s/%s attempt %d", state, errorClass, attemptNo)
	}
}
