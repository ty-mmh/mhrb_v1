package app

import (
	"context"
	"database/sql"
	"errors"
	"sync"
	"testing"
	"time"

	"mahoroba.local/mahoroba/internal/canonical"
	"mahoroba.local/mahoroba/internal/domain"
	"mahoroba.local/mahoroba/internal/generation"
	"mahoroba.local/mahoroba/internal/operationalmetrics"
)

type covr01PrepareBoundaryBarrier struct {
	entered     chan struct{}
	release     chan struct{}
	enterOnce   sync.Once
	releaseOnce sync.Once
}

func newCOVR01PrepareBoundaryBarrier() *covr01PrepareBoundaryBarrier {
	return &covr01PrepareBoundaryBarrier{
		entered: make(chan struct{}),
		release: make(chan struct{}),
	}
}

func (barrier *covr01PrepareBoundaryBarrier) wait() {
	barrier.enterOnce.Do(func() {
		close(barrier.entered)
		<-barrier.release
	})
}

func (barrier *covr01PrepareBoundaryBarrier) releaseNow() {
	barrier.releaseOnce.Do(func() { close(barrier.release) })
}

func TestCOVR01NormalPendingMemorySelectionCrossingDoesNotPrepareCancelOrCallProvider(t *testing.T) {
	ctx := context.Background()
	generator := &alignmentLostSignalGenerator{}
	fixture := newApplicationFixture(t, generator, 1)
	if _, err := fixture.application.activateMemoryPolicyV4ForTest(ctx, fixture.residentID); err != nil {
		t.Fatal(err)
	}
	source, err := fixture.application.Ingress(ctx, "I am calm at the prepare boundary")
	if err != nil {
		t.Fatal(err)
	}
	if err := processForegroundDialogueUnderResidentLock(ctx, fixture, source); err != nil {
		t.Fatal(err)
	}
	works, err := discoverMemoryExtractionWorkForTest(ctx, fixture.repository, fixture.residentID, 16, 1)
	if err != nil || len(works) != 1 || works[0].State != domain.WorkPending {
		t.Fatalf("pending normal memory work = %+v, %v", works, err)
	}

	selected := createCOVR01ActiveResident(t, fixture, "covr01-normal-prepare-crossing")
	seedForegroundCleanProofForTest(t, fixture)
	barrier := newCOVR01PrepareBoundaryBarrier()
	defer barrier.releaseNow()
	fixture.application.backgroundRegistrationHook = barrier.wait

	backgroundDone := make(chan error, 1)
	go func() {
		lock := fixture.application.residentLock(fixture.residentID)
		lock.Lock()
		defer lock.Unlock()
		backgroundDone <- fixture.application.processQueuedMemoryExtraction(
			ctx, fixture.residentID, works[0],
		)
	}()
	select {
	case <-barrier.entered:
		// The hook is reached only after processQueuedMemoryExtraction accepted
		// the old resident as selected and assembled the pending Prepare.
	case err := <-backgroundDone:
		t.Fatalf("normal memory stopped before the Prepare boundary: %v", err)
	case <-time.After(5 * time.Second):
		t.Fatal("normal memory did not reach the Prepare boundary")
	}

	selectionDone := make(chan error, 1)
	go func() { selectionDone <- fixture.application.SelectResident(ctx, selected) }()
	select {
	case err := <-selectionDone:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("selection did not cross the paused normal-memory Prepare boundary")
	}
	barrier.releaseNow()
	select {
	case err := <-backgroundDone:
		if err != nil {
			t.Fatalf("selection-rejected normal memory: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("normal memory did not stop after the selection crossing")
	}
	fixture.application.backgroundRegistrationHook = nil

	if calls := generator.ExtractionCalls(); calls != 0 {
		t.Fatalf("normal-memory provider calls = %d, want 0", calls)
	}
	assertCOVR01NoPreparedOrCancelledRun(t, fixture, works[0].IdempotencyKey)
}

func TestCOVR01AdminReextractSelectionCrossingDoesNotPrepareCancelOrCallProvider(t *testing.T) {
	ctx := context.Background()
	generator := &alignmentLostSignalGenerator{}
	fixture := newApplicationFixture(t, generator, 2)
	source, err := fixture.application.Ingress(ctx, "remember the Admin prepare boundary")
	if err != nil {
		t.Fatal(err)
	}
	if err := processForegroundDialogueUnderResidentLock(ctx, fixture, source); err != nil {
		t.Fatal(err)
	}
	if _, err := fixture.application.activateMemoryPolicyV4ForTest(ctx, fixture.residentID); err != nil {
		t.Fatal(err)
	}
	selected := createCOVR01ActiveResident(t, fixture, "covr01-admin-prepare-crossing")
	seedForegroundCleanProofForTest(t, fixture)
	requestID := memoryExtractionTestID(t, fixture)
	key := domain.MemoryReextractionObligation(source.ID, requestID)

	barrier := newCOVR01PrepareBoundaryBarrier()
	defer barrier.releaseNow()
	fixture.application.backgroundRegistrationHook = barrier.wait
	type reextractResponse struct {
		result domain.MemoryReextractionResult
		err    error
	}
	reextractDone := make(chan reextractResponse, 1)
	go func() {
		result, err := fixture.application.ReextractMemoryEvent(
			ctx, fixture.residentID, source.ID, requestID,
		)
		reextractDone <- reextractResponse{result: result, err: err}
	}()
	select {
	case <-barrier.entered:
		// ReextractMemoryEvent has already accepted the old resident as selected;
		// the Canonical Prepare and provider lease have not started yet.
	case response := <-reextractDone:
		t.Fatalf("Admin re-extraction stopped before the Prepare boundary: %+v", response)
	case <-time.After(5 * time.Second):
		t.Fatal("Admin re-extraction did not reach the Prepare boundary")
	}

	selectionDone := make(chan error, 1)
	go func() { selectionDone <- fixture.application.SelectResident(ctx, selected) }()
	select {
	case err := <-selectionDone:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("selection did not cross the paused Admin re-extraction Prepare boundary")
	}
	barrier.releaseNow()
	var response reextractResponse
	select {
	case response = <-reextractDone:
	case <-time.After(5 * time.Second):
		t.Fatal("Admin re-extraction did not stop after the selection crossing")
	}
	fixture.application.backgroundRegistrationHook = nil
	if response.err != nil {
		t.Fatal(response.err)
	}
	if !response.result.RunID.IsZero() || response.result.AttemptNo != 0 ||
		response.result.State != "" || response.result.Changed {
		t.Fatalf("selection-rejected Admin re-extraction = %+v, want zero result", response.result)
	}
	if calls := generator.ExtractionCalls(); calls != 0 {
		t.Fatalf("Admin re-extraction provider calls = %d, want 0", calls)
	}
	assertCOVR01NoPreparedOrCancelledRun(t, fixture, key)
}

func TestCOVR01AlignmentPrepareSelectionCrossingDoesNotPrepareCancelOrCallProvider(t *testing.T) {
	ctx := context.Background()
	fixture, generator, work := covr01PendingAlignmentWork(t)
	selected := createCOVR01ActiveResident(t, fixture, "covr01-alignment-prepare-crossing")
	seedForegroundCleanProofForTest(t, fixture)

	barrier := newCOVR01PrepareBoundaryBarrier()
	defer barrier.releaseNow()
	fixture.application.backgroundRegistrationHook = barrier.wait
	backgroundDone := make(chan error, 1)
	go func() {
		lock := fixture.application.residentLock(fixture.residentID)
		lock.Lock()
		defer lock.Unlock()
		backgroundDone <- fixture.application.processMemoryAlignmentWork(ctx, work)
	}()
	select {
	case <-barrier.entered:
		// Alignment assembly is complete, but the atomic Prepare+lease mutation
		// has not entered its resident landing boundary.
	case err := <-backgroundDone:
		t.Fatalf("alignment stopped before the Prepare boundary: %v", err)
	case <-time.After(5 * time.Second):
		t.Fatal("alignment did not reach the Prepare boundary")
	}

	selectionDone := make(chan error, 1)
	go func() { selectionDone <- fixture.application.SelectResident(ctx, selected) }()
	select {
	case err := <-selectionDone:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("selection did not cross the paused alignment Prepare boundary")
	}
	barrier.releaseNow()
	select {
	case err := <-backgroundDone:
		if !errors.Is(err, errMemoryAlignmentForegroundPreempted) {
			t.Fatalf("selection-rejected alignment = %v, want foreground preemption", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("alignment did not stop after the selection crossing")
	}
	fixture.application.backgroundRegistrationHook = nil

	if calls := generator.AlignmentCalls(); calls != 0 {
		t.Fatalf("alignment provider calls = %d, want 0", calls)
	}
	assertCOVR01NoPreparedOrCancelledRun(t, fixture, work.IdempotencyKey)
}

type covr01RetryAlignmentGenerator struct {
	base alignmentLostSignalGenerator
}

func (generator *covr01RetryAlignmentGenerator) Stream(
	ctx context.Context,
	request generation.Request,
	sink generation.DeltaSink,
) (generation.Result, error) {
	if request.Purpose == string(domain.GenerationPurposeMemoryAlignment) {
		generator.base.mu.Lock()
		generator.base.alignmentCalls++
		generator.base.mu.Unlock()
		return generation.Result{}, &generation.ProviderError{
			Class: generation.ErrorTimeout, Detail: "retryable alignment timeout",
		}
	}
	return generator.base.Stream(ctx, request, sink)
}

func (generator *covr01RetryAlignmentGenerator) AlignmentCalls() int {
	return generator.base.AlignmentCalls()
}

func TestCOVR01AlignmentRetrySelectionCrossingDoesNotStartAttemptCancelOrCallProvider(t *testing.T) {
	ctx := context.Background()
	generator := &covr01RetryAlignmentGenerator{}
	fixture, work := covr01PendingAlignmentWorkWithGenerator(t, generator, 2)
	seedForegroundCleanProofForTest(t, fixture)
	lock := fixture.application.residentLock(fixture.residentID)
	lock.Lock()
	err := fixture.application.processMemoryAlignmentWork(ctx, work)
	lock.Unlock()
	if err != nil {
		t.Fatal(err)
	}
	if calls := generator.AlignmentCalls(); calls != 1 {
		t.Fatalf("initial alignment provider calls = %d, want 1", calls)
	}
	retry, err := fixture.repository.DiscoverMemoryAlignmentWork(
		ctx, fixture.residentID, domain.MaximumMemoryAlignmentPairs, 2,
	)
	if err != nil || retry == nil || retry.State != domain.WorkRetryPending ||
		retry.RunID == nil || retry.AttemptNo != 1 {
		t.Fatalf("retry-pending alignment work = %+v, %v", retry, err)
	}
	beforeRuns, beforeOutcomes, beforeCancellations := covr01GenerationCounts(
		t, fixture, retry.IdempotencyKey,
	)
	if beforeRuns != 1 || beforeOutcomes == 0 || beforeCancellations != 0 {
		t.Fatalf("initial retry alignment counts = runs:%d outcomes:%d cancellations:%d",
			beforeRuns, beforeOutcomes, beforeCancellations)
	}

	selected := createCOVR01ActiveResident(t, fixture, "covr01-alignment-retry-crossing")
	seedForegroundCleanProofForTest(t, fixture)
	barrier := newCOVR01PrepareBoundaryBarrier()
	defer barrier.releaseNow()
	fixture.application.backgroundRegistrationHook = barrier.wait
	backgroundDone := make(chan error, 1)
	go func() {
		residentLock := fixture.application.residentLock(fixture.residentID)
		residentLock.Lock()
		defer residentLock.Unlock()
		backgroundDone <- fixture.application.processMemoryAlignmentWork(ctx, *retry)
	}()
	select {
	case <-barrier.entered:
	case err := <-backgroundDone:
		t.Fatalf("alignment retry stopped before the StartAttempt boundary: %v", err)
	case <-time.After(5 * time.Second):
		t.Fatal("alignment retry did not reach the StartAttempt boundary")
	}

	selectionDone := make(chan error, 1)
	go func() { selectionDone <- fixture.application.SelectResident(ctx, selected) }()
	select {
	case err := <-selectionDone:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("selection did not cross the paused alignment StartAttempt boundary")
	}
	barrier.releaseNow()
	select {
	case err := <-backgroundDone:
		if !errors.Is(err, errMemoryAlignmentForegroundPreempted) {
			t.Fatalf("selection-rejected alignment retry = %v, want foreground preemption", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("alignment retry did not stop after the selection crossing")
	}
	fixture.application.backgroundRegistrationHook = nil

	if calls := generator.AlignmentCalls(); calls != 1 {
		t.Fatalf("alignment provider calls after rejected retry = %d, want 1", calls)
	}
	afterRuns, afterOutcomes, afterCancellations := covr01GenerationCounts(
		t, fixture, retry.IdempotencyKey,
	)
	if afterRuns != beforeRuns || afterOutcomes != beforeOutcomes ||
		afterCancellations != beforeCancellations {
		t.Fatalf("alignment retry crossing counts before=%d/%d/%d after=%d/%d/%d",
			beforeRuns, beforeOutcomes, beforeCancellations,
			afterRuns, afterOutcomes, afterCancellations)
	}
	var attemptNo int64
	var state, errorClass string
	if err := fixture.store.Reader().QueryRow(`SELECT outcome.attempt_no, outcome.state,
		COALESCE(outcome.error_class, '')
		FROM generation_runs run
		JOIN generation_run_outcomes outcome ON outcome.generation_run_id = run.generation_run_id
		WHERE run.resident_id = ? AND run.idempotency_key = ?
		ORDER BY outcome.outcome_id DESC LIMIT 1`, fixture.residentID.String(),
		retry.IdempotencyKey).Scan(&attemptNo, &state, &errorClass); err != nil {
		t.Fatal(err)
	}
	if attemptNo != 1 || state != "failed" || errorClass != string(generation.ErrorTimeout) {
		t.Fatalf("alignment retry latest outcome = %d/%s/%s, want 1/failed/timeout",
			attemptNo, state, errorClass)
	}
}

func covr01PendingAlignmentWork(
	t *testing.T,
) (applicationFixture, *alignmentLostSignalGenerator, domain.MemoryAlignmentWork) {
	t.Helper()
	generator := &alignmentLostSignalGenerator{}
	fixture, work := covr01PendingAlignmentWorkWithGenerator(t, generator, 1)
	return fixture, generator, work
}

func covr01PendingAlignmentWorkWithGenerator(
	t *testing.T,
	generator generation.Generator,
	maxAttempts int,
) (applicationFixture, domain.MemoryAlignmentWork) {
	t.Helper()
	ctx := context.Background()
	fixture := newApplicationFixture(t, generator, maxAttempts)
	if _, err := fixture.application.activateMemoryPolicyV4ForTest(ctx, fixture.residentID); err != nil {
		t.Fatal(err)
	}
	for _, message := range []string{
		"I am calm", "I am calm", "I am calm", "I see calm", "I see calm",
	} {
		event, err := fixture.application.Ingress(ctx, message)
		if err != nil {
			t.Fatal(err)
		}
		if err := processForegroundDialogueUnderResidentLock(ctx, fixture, event); err != nil {
			t.Fatal(err)
		}
		extractions, err := discoverMemoryExtractionWorkForTest(
			ctx, fixture.repository, fixture.residentID, 32, 1,
		)
		if err != nil {
			t.Fatal(err)
		}
		var extraction *domain.MemoryExtractionWork
		for index := range extractions {
			if extractions[index].SourceEvent.ID == event.ID {
				extraction = &extractions[index]
				break
			}
		}
		if extraction == nil {
			t.Fatalf("memory extraction for alignment source %s was not discovered", event.ID)
		}
		seedForegroundCleanProofForTest(t, fixture)
		if err := fixture.application.processMemoryExtractionWork(ctx, *extraction); err != nil {
			t.Fatal(err)
		}
	}
	work, err := fixture.repository.DiscoverMemoryAlignmentWork(
		ctx, fixture.residentID, domain.MaximumMemoryAlignmentPairs, maxAttempts,
	)
	if err != nil || work == nil || work.State != domain.WorkPending {
		t.Fatalf("pending alignment work = %+v, %v", work, err)
	}
	return fixture, *work
}

type covr01ArchiveProviderObservation struct {
	terminal      operationalmetrics.ProviderTerminal
	archiveErr    error
	queryErr      error
	runID         string
	attemptNo     int64
	state         string
	errorClass    string
	latestRunning int
}

type covr01ArchiveOnProviderFinishedObserver struct {
	application *Application
	residentID  canonical.ID
	key         string
	reader      *sql.DB
	once        sync.Once
	observed    chan covr01ArchiveProviderObservation
}

func (observer *covr01ArchiveOnProviderFinishedObserver) ProviderFinished(
	terminal operationalmetrics.ProviderTerminal,
	_ time.Duration,
) {
	observer.once.Do(func() {
		observation := covr01ArchiveProviderObservation{terminal: terminal}
		observation.archiveErr = observer.application.ArchiveResident(
			context.Background(), observer.residentID,
		)
		// This query deliberately remains inside the synchronous callback. It
		// proves ArchiveResident does not merely schedule cleanup: immediately
		// after it returns, the in-flight Admin run is durably terminal.
		observation.queryErr = observer.reader.QueryRow(`SELECT run.generation_run_id,
			outcome.attempt_no, outcome.state, COALESCE(outcome.error_class, '')
			FROM generation_runs run
			JOIN generation_run_outcomes outcome
			  ON outcome.generation_run_id = run.generation_run_id
			WHERE run.resident_id = ? AND run.idempotency_key = ?
			ORDER BY outcome.outcome_id DESC LIMIT 1`,
			observer.residentID.String(), observer.key,
		).Scan(
			&observation.runID, &observation.attemptNo,
			&observation.state, &observation.errorClass,
		)
		if observation.queryErr == nil {
			observation.queryErr = observer.reader.QueryRow(`SELECT COUNT(*)
				FROM generation_run_outcomes outcome
				WHERE outcome.generation_run_id = ? AND outcome.state = 'running'
				  AND NOT EXISTS (
					SELECT 1 FROM generation_run_outcomes newer
					WHERE newer.generation_run_id = outcome.generation_run_id
					  AND newer.outcome_id > outcome.outcome_id
				  )`, observation.runID).Scan(&observation.latestRunning)
		}
		observer.observed <- observation
	})
}

func (*covr01ArchiveOnProviderFinishedObserver) SetMandatoryWorkCounts(
	operationalmetrics.MandatoryWorkCounts,
) {
}

func (*covr01ArchiveOnProviderFinishedObserver) MandatoryWorkerFailed(
	operationalmetrics.MandatoryWorkerFailure,
) {
}

func TestCOVR01AdminReextractProviderObserverArchiveReturnsAfterDurableTerminalization(t *testing.T) {
	ctx := context.Background()
	generator := &alignmentLostSignalGenerator{}
	fixture := newApplicationFixture(t, generator, 2)
	source, err := fixture.application.Ingress(ctx, "I am calm during observer archive")
	if err != nil {
		t.Fatal(err)
	}
	if err := processForegroundDialogueUnderResidentLock(ctx, fixture, source); err != nil {
		t.Fatal(err)
	}
	if _, err := fixture.application.activateMemoryPolicyV4ForTest(ctx, fixture.residentID); err != nil {
		t.Fatal(err)
	}
	seedForegroundCleanProofForTest(t, fixture)
	requestID := memoryExtractionTestID(t, fixture)
	key := domain.MemoryReextractionObligation(source.ID, requestID)
	observer := &covr01ArchiveOnProviderFinishedObserver{
		application: fixture.application,
		residentID:  fixture.residentID,
		key:         key,
		reader:      fixture.store.Reader(),
		observed:    make(chan covr01ArchiveProviderObservation, 1),
	}
	fixture.application.operationalObserver = observer
	fixture.application.generator = observedGenerator{
		delegate: generator, observer: observer, providerIdentity: "test",
	}

	type reextractResponse struct {
		result domain.MemoryReextractionResult
		err    error
	}
	reextractDone := make(chan reextractResponse, 1)
	go func() {
		result, err := fixture.application.ReextractMemoryEvent(
			ctx, fixture.residentID, source.ID, requestID,
		)
		reextractDone <- reextractResponse{result: result, err: err}
	}()

	var observation covr01ArchiveProviderObservation
	select {
	case observation = <-observer.observed:
	case response := <-reextractDone:
		t.Fatalf("Admin re-extraction returned before ProviderFinished archive observation: %+v", response)
	case <-time.After(5 * time.Second):
		t.Fatal("ProviderFinished callback deadlocked while synchronously archiving the resident")
	}
	if observation.terminal != operationalmetrics.ProviderSucceeded {
		t.Fatalf("provider terminal = %v, want succeeded", observation.terminal)
	}
	if observation.archiveErr != nil {
		t.Fatalf("ArchiveResident from ProviderFinished: %v", observation.archiveErr)
	}
	if observation.queryErr != nil {
		t.Fatalf("query immediately after ArchiveResident returned: %v", observation.queryErr)
	}
	if observation.runID == "" || observation.attemptNo != 1 || observation.state != "cancelled" ||
		observation.errorClass != string(generation.ErrorResidentInactive) || observation.latestRunning != 0 {
		t.Fatalf("immediate archived Admin outcome = run:%s attempt:%d state:%s class:%s latest-running:%d",
			observation.runID, observation.attemptNo, observation.state,
			observation.errorClass, observation.latestRunning)
	}
	select {
	case response := <-reextractDone:
		if response.err != nil {
			t.Fatalf("Admin re-extraction after synchronous archive: %v", response.err)
		}
		if response.result.RunID.String() != observation.runID ||
			response.result.AttemptNo != observation.attemptNo ||
			response.result.State != domain.WorkTerminalFailed || !response.result.Changed {
			t.Fatalf("Admin re-extraction final cancelled result = %+v, want run %s attempt %d terminal_failed changed",
				response.result, observation.runID, observation.attemptNo)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Admin re-extraction did not unwind after ProviderFinished archive returned")
	}
	if calls := generator.ExtractionCalls(); calls != 1 {
		t.Fatalf("Admin re-extraction provider calls = %d, want 1", calls)
	}
	resident, err := fixture.application.Resident(ctx, fixture.residentID)
	if err != nil {
		t.Fatal(err)
	}
	if resident.Status != "archived" {
		t.Fatalf("resident status = %q, want archived", resident.Status)
	}
}

type covr01OptionalArchiveObservation struct {
	terminal      operationalmetrics.ProviderTerminal
	archiveErr    error
	queryErr      error
	runID         string
	attemptNo     int64
	state         string
	errorClass    string
	latestRunning int
}

type covr01OptionalArchiveObserver struct {
	application *Application
	residentID  canonical.ID
	purpose     domain.GenerationPurpose
	reader      *sql.DB
	once        sync.Once
	observed    chan covr01OptionalArchiveObservation
}

func (observer *covr01OptionalArchiveObserver) ProviderFinished(
	terminal operationalmetrics.ProviderTerminal,
	_ time.Duration,
) {
	observer.once.Do(func() {
		observation := covr01OptionalArchiveObservation{terminal: terminal}
		observation.queryErr = observer.reader.QueryRow(`SELECT run.generation_run_id,
			outcome.attempt_no
			FROM generation_runs run
			JOIN generation_run_outcomes outcome
			  ON outcome.generation_run_id = run.generation_run_id
			WHERE run.resident_id = ? AND run.purpose = ? AND outcome.state = 'running'
			  AND NOT EXISTS (
				SELECT 1 FROM generation_run_outcomes newer
				WHERE newer.generation_run_id = outcome.generation_run_id
				  AND newer.outcome_id > outcome.outcome_id
			  )
			ORDER BY outcome.outcome_id DESC LIMIT 1`,
			observer.residentID.String(), string(observer.purpose),
		).Scan(&observation.runID, &observation.attemptNo)
		if observation.queryErr == nil {
			observation.archiveErr = observer.application.ArchiveResident(
				context.Background(), observer.residentID,
			)
			observation.queryErr = observer.reader.QueryRow(`SELECT outcome.state,
				COALESCE(outcome.error_class, '')
				FROM generation_run_outcomes outcome
				WHERE outcome.generation_run_id = ?
				ORDER BY outcome.outcome_id DESC LIMIT 1`, observation.runID,
			).Scan(&observation.state, &observation.errorClass)
		}
		if observation.queryErr == nil {
			observation.queryErr = observer.reader.QueryRow(`SELECT COUNT(*)
				FROM generation_run_outcomes outcome
				WHERE outcome.generation_run_id = ? AND outcome.state = 'running'
				  AND NOT EXISTS (
					SELECT 1 FROM generation_run_outcomes newer
					WHERE newer.generation_run_id = outcome.generation_run_id
					  AND newer.outcome_id > outcome.outcome_id
				  )`, observation.runID).Scan(&observation.latestRunning)
		}
		observer.observed <- observation
	})
}

func (*covr01OptionalArchiveObserver) SetMandatoryWorkCounts(
	operationalmetrics.MandatoryWorkCounts,
) {
}

func (*covr01OptionalArchiveObserver) MandatoryWorkerFailed(
	operationalmetrics.MandatoryWorkerFailure,
) {
}

type covr01OptionalArchiveCase struct {
	fixture   applicationFixture
	generator *scriptedGenerator
	run       func(context.Context) error
}

func TestCOVR01OptionalProviderObserverArchiveReturnsAfterExactRunTerminalization(t *testing.T) {
	tests := []struct {
		name    string
		purpose domain.GenerationPurpose
		build   func(*testing.T) covr01OptionalArchiveCase
	}{
		{
			name:    "persona",
			purpose: domain.GenerationPurposePersonaRevision,
			build: func(t *testing.T) covr01OptionalArchiveCase {
				fixture, _ := personaWorkForTerminalization(t, 2)
				fixture.application.setMemoryExtractionScansComplete(fixture.residentID, true)
				generator := &scriptedGenerator{steps: []generatorStep{{
					text: `{"contradiction":false,"persona":"friendly, careful, and grounded"}`,
				}}}
				return covr01OptionalArchiveCase{
					fixture: fixture, generator: generator,
					run: func(ctx context.Context) error {
						_, err := fixture.application.ProposeMemoryPersona(ctx, fixture.residentID)
						return err
					},
				}
			},
		},
		{
			name:    "alignment",
			purpose: domain.GenerationPurposeMemoryAlignment,
			build: func(t *testing.T) covr01OptionalArchiveCase {
				fixture, _, _ := covr01PendingAlignmentWork(t)
				seedForegroundCleanProofForTest(t, fixture)
				generator := &scriptedGenerator{steps: []generatorStep{{
					text: `{"aligned":true,"confidence":"800000","version":"memory-alignment-output-v1"}`,
				}}}
				return covr01OptionalArchiveCase{
					fixture: fixture, generator: generator,
					run: func(ctx context.Context) error {
						lock := fixture.application.residentLock(fixture.residentID)
						lock.Lock()
						defer lock.Unlock()
						return fixture.application.processMemoryAlignmentQueue(ctx, fixture.residentID)
					},
				}
			},
		},
		{
			name:    "abstraction",
			purpose: domain.GenerationPurposeMemoryAbstraction,
			build: func(t *testing.T) covr01OptionalArchiveCase {
				fixture, sources, _ := derivationSourcesForTerminalization(t, generatorStep{})
				fixture.application.setMemoryExtractionScansComplete(fixture.residentID, true)
				generator := &scriptedGenerator{steps: []generatorStep{{
					text: `{"statement":"The owner enjoys quiet hobbies.","temporal_kind":"stable","version":"memory-derived-output-v1"}`,
				}}}
				return covr01OptionalArchiveCase{
					fixture: fixture, generator: generator,
					run: func(ctx context.Context) error {
						_, err := fixture.application.DeriveMemoryClaim(
							ctx, fixture.residentID,
							domain.GenerationPurposeMemoryAbstraction, sources,
						)
						return err
					},
				}
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			ctx := context.Background()
			optional := test.build(t)
			observer := &covr01OptionalArchiveObserver{
				application: optional.fixture.application,
				residentID:  optional.fixture.residentID,
				purpose:     test.purpose,
				reader:      optional.fixture.store.Reader(),
				observed:    make(chan covr01OptionalArchiveObservation, 1),
			}
			optional.fixture.application.operationalObserver = observer
			optional.fixture.application.generator = observedGenerator{
				delegate: optional.generator, observer: observer, providerIdentity: "test",
			}

			operationDone := make(chan error, 1)
			go func() { operationDone <- optional.run(ctx) }()
			var observation covr01OptionalArchiveObservation
			select {
			case observation = <-observer.observed:
			case err := <-operationDone:
				t.Fatalf("optional operation returned before ProviderFinished archive: %v", err)
			case <-time.After(5 * time.Second):
				t.Fatal("ProviderFinished callback deadlocked while archiving optional run")
			}
			if observation.terminal != operationalmetrics.ProviderSucceeded {
				t.Fatalf("provider terminal = %v, want succeeded", observation.terminal)
			}
			if observation.archiveErr != nil {
				t.Fatalf("ArchiveResident from ProviderFinished: %v", observation.archiveErr)
			}
			if observation.queryErr != nil {
				t.Fatalf("exact optional run query around ArchiveResident: %v", observation.queryErr)
			}
			if observation.runID == "" || observation.attemptNo != 1 ||
				observation.state != "cancelled" ||
				observation.errorClass != string(generation.ErrorResidentInactive) ||
				observation.latestRunning != 0 {
				t.Fatalf("optional archived outcome = run:%s attempt:%d state:%s class:%s latest-running:%d",
					observation.runID, observation.attemptNo, observation.state,
					observation.errorClass, observation.latestRunning)
			}
			select {
			case err := <-operationDone:
				if err != nil {
					t.Fatalf("optional operation after synchronous ArchiveResident: %v", err)
				}
			case <-time.After(5 * time.Second):
				t.Fatal("optional operation did not unwind after ProviderFinished archive")
			}
			if calls := optional.generator.CallCount(); calls != 1 {
				t.Fatalf("optional provider calls = %d, want 1", calls)
			}
			resident, err := optional.fixture.application.Resident(ctx, optional.fixture.residentID)
			if err != nil {
				t.Fatal(err)
			}
			if resident.Status != "archived" {
				t.Fatalf("resident status = %q, want archived", resident.Status)
			}
		})
	}
}

type covr01LockedObserverArchiveResult struct {
	archiveErr       error
	preconditionErr  error
	startedRunState  string
	secondPendingRun int
}

type covr01LockedArchiveObserver struct {
	mu                    sync.Mutex
	once                  sync.Once
	application           *Application
	residentID            canonical.ID
	secondEventID         canonical.ID
	reader                *sql.DB
	entered               chan struct{}
	archiveReturned       chan covr01LockedObserverArchiveResult
	generationFailedCalls int
}

func (*covr01LockedArchiveObserver) UserCommitted(domain.Event) {}

func (observer *covr01LockedArchiveObserver) GenerationStarted(
	_ canonical.ID,
	runID canonical.ID,
	_ int64,
) {
	observer.mu.Lock()
	defer observer.mu.Unlock()
	observer.once.Do(func() {
		close(observer.entered)
		result := covr01LockedObserverArchiveResult{}
		result.preconditionErr = observer.reader.QueryRow(`SELECT state
			FROM generation_run_outcomes WHERE generation_run_id = ?
			ORDER BY outcome_id DESC LIMIT 1`, runID.String()).Scan(&result.startedRunState)
		if result.preconditionErr == nil {
			result.preconditionErr = observer.reader.QueryRow(`SELECT COUNT(*)
				FROM generation_runs WHERE resident_id = ? AND idempotency_key = ?`,
				observer.residentID.String(), domain.DialogueObligation(observer.secondEventID),
			).Scan(&result.secondPendingRun)
		}
		result.archiveErr = observer.application.ArchiveResident(
			context.Background(), observer.residentID,
		)
		observer.archiveReturned <- result
	})
}

func (*covr01LockedArchiveObserver) GenerationDelta(canonical.ID, canonical.ID, string) {}
func (*covr01LockedArchiveObserver) ResidentCommitted(domain.Event)                     {}

func (observer *covr01LockedArchiveObserver) GenerationFailed(
	canonical.ID,
	canonical.ID,
	int64,
	string,
	bool,
) {
	// This deliberately uses the same non-reentrant mutex held by
	// GenerationStarted. Archive cleanup must not call back here synchronously
	// on the reentrant ArchiveResident stack.
	observer.mu.Lock()
	observer.generationFailedCalls++
	observer.mu.Unlock()
}

func TestCOVR01LockedObserverArchiveReentryDoesNotSynchronouslyNotifyGenerationFailure(t *testing.T) {
	ctx := context.Background()
	generator := &scriptedGenerator{steps: []generatorStep{{text: "provider must not run after archive"}}}
	fixture := newApplicationFixture(t, generator, 1)
	first, err := fixture.application.Ingress(ctx, "first dialogue enters GenerationStarted")
	if err != nil {
		t.Fatal(err)
	}
	second, err := fixture.application.Ingress(ctx, "second dialogue remains pending during archive")
	if err != nil {
		t.Fatal(err)
	}
	observer := &covr01LockedArchiveObserver{
		application:     fixture.application,
		residentID:      fixture.residentID,
		secondEventID:   second.ID,
		reader:          fixture.store.Reader(),
		entered:         make(chan struct{}),
		archiveReturned: make(chan covr01LockedObserverArchiveResult, 1),
	}
	fixture.application.SetObserver(observer)

	processDone := make(chan error, 1)
	go func() { processDone <- fixture.application.ProcessResident(ctx, fixture.residentID) }()
	select {
	case <-observer.entered:
	case err := <-processDone:
		t.Fatalf("resident turn stopped before GenerationStarted archive callback: %v", err)
	case <-time.After(5 * time.Second):
		t.Fatal("resident turn did not reach GenerationStarted archive callback")
	}
	var archiveResult covr01LockedObserverArchiveResult
	select {
	case archiveResult = <-observer.archiveReturned:
	case <-time.After(5 * time.Second):
		t.Fatal("ArchiveResident deadlocked by synchronous GenerationFailed on the locked Observer")
	}
	if archiveResult.preconditionErr != nil {
		t.Fatalf("observer archive precondition query: %v", archiveResult.preconditionErr)
	}
	if archiveResult.startedRunState != "running" || archiveResult.secondPendingRun != 0 {
		t.Fatalf("observer archive precondition = first:%s second-runs:%d, want running/0",
			archiveResult.startedRunState, archiveResult.secondPendingRun)
	}
	if archiveResult.archiveErr != nil {
		t.Fatalf("ArchiveResident from locked GenerationStarted Observer: %v", archiveResult.archiveErr)
	}
	select {
	case err := <-processDone:
		if err != nil {
			t.Fatalf("ProcessResident after Observer archive: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("ProcessResident did not complete after locked Observer archive returned")
	}
	if calls := generator.CallCount(); calls != 0 {
		t.Fatalf("dialogue provider calls after GenerationStarted archive = %d, want 0", calls)
	}
	assertCOVR01DialogueResidentInactive(t, fixture, first.ID, 1)
	assertCOVR01DialogueResidentInactive(t, fixture, second.ID, 0)
	resident, err := fixture.application.Resident(ctx, fixture.residentID)
	if err != nil {
		t.Fatal(err)
	}
	if resident.Status != "archived" {
		t.Fatalf("resident status = %q, want archived", resident.Status)
	}
}

func assertCOVR01DialogueResidentInactive(
	t *testing.T,
	fixture applicationFixture,
	eventID canonical.ID,
	wantAttempt int64,
) {
	t.Helper()
	var state, errorClass string
	var attemptNo int64
	var latestRunning int
	err := fixture.store.Reader().QueryRow(`SELECT latest.state,
		COALESCE(latest.error_class, ''), latest.attempt_no,
		(SELECT COUNT(*) FROM generation_run_outcomes running
		 WHERE running.generation_run_id = run.generation_run_id
		   AND running.state = 'running'
		   AND NOT EXISTS (
			SELECT 1 FROM generation_run_outcomes newer
			WHERE newer.generation_run_id = running.generation_run_id
			  AND newer.outcome_id > running.outcome_id
		   ))
		FROM generation_runs run
		JOIN generation_run_outcomes latest ON latest.generation_run_id = run.generation_run_id
		WHERE run.resident_id = ? AND run.idempotency_key = ?
		ORDER BY latest.outcome_id DESC LIMIT 1`,
		fixture.residentID.String(), domain.DialogueObligation(eventID),
	).Scan(&state, &errorClass, &attemptNo, &latestRunning)
	if err != nil {
		t.Fatal(err)
	}
	if state != "cancelled" || errorClass != string(generation.ErrorResidentInactive) ||
		attemptNo != wantAttempt || latestRunning != 0 {
		t.Fatalf("dialogue %s archive outcome = %s/%s attempt:%d latest-running:%d, want attempt:%d",
			eventID, state, errorClass, attemptNo, latestRunning, wantAttempt)
	}
}

func TestCOVR01PersonaRequiresCompleteMandatoryMemoryScanBeforePrepare(t *testing.T) {
	ctx := context.Background()
	fixture, work := personaWorkForTerminalization(t, 2)
	generator, ok := fixture.application.generator.(*scriptedGenerator)
	if !ok {
		t.Fatalf("persona fixture generator = %T, want *scriptedGenerator", fixture.application.generator)
	}
	assertCOVR01ResidentSelected(t, fixture)
	fixture.application.setMemoryExtractionScansComplete(fixture.residentID, false)
	before := generator.CallCount()

	result, err := fixture.application.ProposeMemoryPersona(ctx, fixture.residentID)
	if err != nil {
		t.Fatal(err)
	}
	if result.Changed || result.RunID != nil || result.Landing != nil ||
		result.BlockingReason != "mandatory_memory_scan_incomplete" {
		t.Fatalf("scan-incomplete persona result = %+v", result)
	}
	if calls := generator.CallCount(); calls != before {
		t.Fatalf("scan-incomplete persona provider calls changed from %d to %d", before, calls)
	}
	assertCOVR01NoPreparedOrCancelledRun(
		t, fixture, domain.PersonaRevisionObligation(work.TriggerStageTransitionID),
	)
}

func TestCOVR01DerivationRequiresCompleteMandatoryMemoryScanBeforePrepare(t *testing.T) {
	ctx := context.Background()
	fixture, sources, generator := derivationSourcesForTerminalization(t, generatorStep{})
	assertCOVR01ResidentSelected(t, fixture)
	fixture.application.setMemoryExtractionScansComplete(fixture.residentID, false)
	before := generator.CallCount()

	result, err := fixture.application.DeriveMemoryClaim(
		ctx, fixture.residentID, domain.GenerationPurposeMemoryAbstraction, sources,
	)
	if err != nil {
		t.Fatal(err)
	}
	if !result.RunID.IsZero() || !result.Landing.ClaimID.IsZero() || len(result.Landing.RelationIDs) != 0 {
		t.Fatalf("scan-incomplete derivation result = %+v, want zero result", result)
	}
	if calls := generator.CallCount(); calls != before {
		t.Fatalf("scan-incomplete derivation provider calls changed from %d to %d", before, calls)
	}
	var runs int
	if err := fixture.store.Reader().QueryRow(`SELECT COUNT(*) FROM generation_runs
		WHERE resident_id = ? AND purpose = ?`, fixture.residentID.String(),
		string(domain.GenerationPurposeMemoryAbstraction)).Scan(&runs); err != nil {
		t.Fatal(err)
	}
	if runs != 0 {
		t.Fatalf("scan-incomplete derivation prepared runs = %d, want 0", runs)
	}
}

func assertCOVR01ResidentSelected(t *testing.T, fixture applicationFixture) {
	t.Helper()
	selected, err := fixture.application.ActiveResident(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if selected.Status != "active" || selected.ResidentID != fixture.residentID {
		t.Fatalf("selected resident = %+v, want active %s", selected, fixture.residentID)
	}
}

func covr01GenerationCounts(
	t *testing.T,
	fixture applicationFixture,
	key string,
) (runs, outcomes, cancellations int) {
	t.Helper()
	if err := fixture.store.Reader().QueryRow(`SELECT COUNT(*) FROM generation_runs
		WHERE resident_id = ? AND idempotency_key = ?`,
		fixture.residentID.String(), key).Scan(&runs); err != nil {
		t.Fatal(err)
	}
	if err := fixture.store.Reader().QueryRow(`SELECT COUNT(*)
		FROM generation_run_outcomes outcome
		JOIN generation_runs run ON run.generation_run_id = outcome.generation_run_id
		WHERE run.resident_id = ? AND run.idempotency_key = ?`,
		fixture.residentID.String(), key).Scan(&outcomes); err != nil {
		t.Fatal(err)
	}
	if err := fixture.store.Reader().QueryRow(`SELECT COUNT(*)
		FROM generation_run_outcomes outcome
		JOIN generation_runs run ON run.generation_run_id = outcome.generation_run_id
		WHERE run.resident_id = ? AND run.idempotency_key = ? AND outcome.state = 'cancelled'`,
		fixture.residentID.String(), key).Scan(&cancellations); err != nil {
		t.Fatal(err)
	}
	return runs, outcomes, cancellations
}

func assertCOVR01NoPreparedOrCancelledRun(t *testing.T, fixture applicationFixture, key string) {
	t.Helper()
	runs, attempts, cancellations := covr01GenerationCounts(t, fixture, key)
	if runs != 0 {
		t.Fatalf("generation runs for %q = %d, want 0", key, runs)
	}
	if attempts != 0 {
		t.Fatalf("generation attempts for %q = %d, want 0", key, attempts)
	}
	if cancellations != 0 {
		t.Fatalf("typed cancellations for %q = %d, want 0", key, cancellations)
	}
}
