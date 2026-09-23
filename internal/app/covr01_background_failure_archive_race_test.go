package app

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"mahoroba.local/mahoroba/internal/canonical"
	"mahoroba.local/mahoroba/internal/domain"
	"mahoroba.local/mahoroba/internal/generation"
	"mahoroba.local/mahoroba/internal/store/sqlite"
)

type covr01OptionalFailureReloadBarrierRepository struct {
	*sqlite.CanonicalRepository
	mu          sync.Mutex
	armed       bool
	runID       canonical.ID
	entered     chan struct{}
	release     chan struct{}
	enterOnce   sync.Once
	releaseOnce sync.Once
}

type covr01ResidentStatusBarrierRepository struct {
	*sqlite.CanonicalRepository
	mu          sync.Mutex
	armed       bool
	entered     chan struct{}
	release     chan struct{}
	enterOnce   sync.Once
	releaseOnce sync.Once
}

type covr01AmbiguousMemoryFailureBackend struct {
	*covr01AmbiguousCommitBackend
}

type covr01MemoryExtractionFailureMutator interface {
	FailMemoryExtraction(context.Context, domain.FailMemoryExtraction) error
}

type covr01AmbiguousMemoryFailureUoW struct {
	canonical.CanonicalUoW
	domain.MutationStore
	memoryFailures covr01MemoryExtractionFailureMutator
	backend        *covr01AmbiguousCommitBackend
}

func (backend *covr01AmbiguousMemoryFailureBackend) Begin(
	ctx context.Context,
	metadata canonical.CommitMetadata,
) (canonical.CanonicalUoW, error) {
	uow, err := backend.delegate.Begin(ctx, metadata)
	if err != nil {
		return nil, err
	}
	mutations, mutationsOK := uow.(domain.MutationStore)
	memoryFailures, memoryOK := uow.(covr01MemoryExtractionFailureMutator)
	if !mutationsOK || !memoryOK {
		_ = uow.Rollback(context.WithoutCancel(ctx))
		return nil, errors.New("test backend UoW lacks memory failure capability")
	}
	return &covr01AmbiguousMemoryFailureUoW{
		CanonicalUoW: uow, MutationStore: mutations,
		memoryFailures: memoryFailures, backend: backend.covr01AmbiguousCommitBackend,
	}, nil
}

func (uow *covr01AmbiguousMemoryFailureUoW) FailMemoryExtraction(
	ctx context.Context,
	failure domain.FailMemoryExtraction,
) error {
	return uow.memoryFailures.FailMemoryExtraction(ctx, failure)
}

func (uow *covr01AmbiguousMemoryFailureUoW) Commit(ctx context.Context) error {
	if err := uow.CanonicalUoW.Commit(ctx); err != nil {
		return err
	}
	uow.backend.mu.Lock()
	failure := uow.backend.failure
	uow.backend.failure = nil
	uow.backend.mu.Unlock()
	return failure
}

func newCOVR01OptionalFailureReloadBarrierRepository(
	repository *sqlite.CanonicalRepository,
) *covr01OptionalFailureReloadBarrierRepository {
	return &covr01OptionalFailureReloadBarrierRepository{
		CanonicalRepository: repository,
		entered:             make(chan struct{}),
		release:             make(chan struct{}),
	}
}

func newCOVR01ResidentStatusBarrierRepository(
	repository *sqlite.CanonicalRepository,
) *covr01ResidentStatusBarrierRepository {
	return &covr01ResidentStatusBarrierRepository{
		CanonicalRepository: repository,
		entered:             make(chan struct{}),
		release:             make(chan struct{}),
	}
}

func (repository *covr01ResidentStatusBarrierRepository) arm() {
	repository.mu.Lock()
	repository.armed = true
	repository.mu.Unlock()
}

func (repository *covr01ResidentStatusBarrierRepository) Resident(
	ctx context.Context,
	residentID canonical.ID,
) (domain.ResidentSnapshot, error) {
	resident, err := repository.CanonicalRepository.Resident(ctx, residentID)
	repository.mu.Lock()
	block := repository.armed
	if block {
		repository.armed = false
	}
	repository.mu.Unlock()
	if block {
		repository.enterOnce.Do(func() { close(repository.entered) })
		<-repository.release
	}
	return resident, err
}

func (repository *covr01ResidentStatusBarrierRepository) releaseStatus() {
	repository.releaseOnce.Do(func() { close(repository.release) })
}

func (repository *covr01OptionalFailureReloadBarrierRepository) arm() {
	repository.mu.Lock()
	repository.armed = true
	repository.mu.Unlock()
}

func (repository *covr01OptionalFailureReloadBarrierRepository) Generation(
	ctx context.Context,
	runID canonical.ID,
) (domain.PreparedGeneration, error) {
	// Capture the running state before blocking. The recorder now owns the
	// resident landing lock, so a concurrent Archive must wait until the generic
	// failure commit has established the failure-first linearization order.
	prepared, err := repository.CanonicalRepository.Generation(ctx, runID)
	repository.mu.Lock()
	block := repository.armed
	if block {
		repository.armed = false
		repository.runID = runID
	}
	repository.mu.Unlock()
	if block {
		repository.enterOnce.Do(func() { close(repository.entered) })
		<-repository.release
	}
	return prepared, err
}

func (repository *covr01OptionalFailureReloadBarrierRepository) releaseReload() {
	repository.releaseOnce.Do(func() { close(repository.release) })
}

func (repository *covr01OptionalFailureReloadBarrierRepository) blockedRunID() canonical.ID {
	repository.mu.Lock()
	defer repository.mu.Unlock()
	return repository.runID
}

type covr01ProviderFailureBarrier struct {
	failure     error
	entered     chan struct{}
	release     chan struct{}
	enterOnce   sync.Once
	releaseOnce sync.Once
}

func newCOVR01ProviderFailureBarrier() *covr01ProviderFailureBarrier {
	return &covr01ProviderFailureBarrier{
		failure: &generation.ProviderError{
			Class: generation.ErrorHTTP, StatusCode: 401, Detail: "injected optional provider failure",
		},
		entered: make(chan struct{}),
		release: make(chan struct{}),
	}
}

func (barrier *covr01ProviderFailureBarrier) step() generatorStep {
	return generatorStep{
		err: barrier.failure,
		beforeReturn: func() {
			barrier.enterOnce.Do(func() { close(barrier.entered) })
			<-barrier.release
		},
	}
}

func (barrier *covr01ProviderFailureBarrier) releaseProvider() {
	barrier.releaseOnce.Do(func() { close(barrier.release) })
}

func TestCOVR01OptionalRawProviderFailureLinearizesBeforeArchive(t *testing.T) {
	t.Run("persona", func(t *testing.T) {
		fixture, _ := personaWorkForTerminalization(t, 2)
		fixture.application.setMemoryExtractionScansComplete(fixture.residentID, true)
		assertCOVR01OptionalProviderFailureLinearizesBeforeArchive(t, fixture, false, func(ctx context.Context) error {
			_, err := fixture.application.ProposeMemoryPersona(ctx, fixture.residentID)
			return err
		})
	})

	t.Run("alignment", func(t *testing.T) {
		fixture, _, _ := covr01PendingAlignmentWork(t)
		seedForegroundCleanProofForTest(t, fixture)
		assertCOVR01OptionalProviderFailureLinearizesBeforeArchive(t, fixture, false, func(ctx context.Context) error {
			lock := fixture.application.residentLock(fixture.residentID)
			lock.Lock()
			defer lock.Unlock()
			return fixture.application.processMemoryAlignmentQueue(ctx, fixture.residentID)
		})
	})

	t.Run("derivation", func(t *testing.T) {
		fixture, sources, _ := derivationSourcesForTerminalization(t, generatorStep{})
		fixture.application.setMemoryExtractionScansComplete(fixture.residentID, true)
		assertCOVR01OptionalProviderFailureLinearizesBeforeArchive(t, fixture, true, func(ctx context.Context) error {
			_, err := fixture.application.DeriveMemoryClaim(
				ctx, fixture.residentID, domain.GenerationPurposeMemoryAbstraction, sources,
			)
			return err
		})
	})

	t.Run("autonomy", func(t *testing.T) {
		fixture, _, trigger := initiativeReadyForRaceTest(t)
		ctx := context.Background()
		assembly, err := fixture.application.assembleAutonomous(
			ctx, fixture.repository, fixture.residentID, trigger,
		)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := fixture.application.submitWithContent(
			ctx, domain.PrepareAutonomousGenerationCommand(assembly.Prepare), assembly.Contents,
		); err != nil {
			t.Fatal(err)
		}
		prepared, err := fixture.repository.Generation(ctx, assembly.Prepare.Generation.RunID)
		if err != nil {
			t.Fatal(err)
		}
		seedForegroundCleanProofForTest(t, fixture)
		assertCOVR01OptionalProviderFailureLinearizesBeforeArchive(t, fixture, false, func(ctx context.Context) error {
			return fixture.application.callAndLandAutonomous(ctx, prepared, trigger)
		})
	})
}

func assertCOVR01OptionalProviderFailureLinearizesBeforeArchive(
	t *testing.T,
	fixture applicationFixture,
	wantOperationError bool,
	run func(context.Context) error,
) {
	t.Helper()
	ctx := context.Background()
	repository := newCOVR01OptionalFailureReloadBarrierRepository(fixture.repository)
	fixture.application.repository = repository
	failure := newCOVR01ProviderFailureBarrier()
	provider := &scriptedGenerator{steps: []generatorStep{failure.step()}}
	fixture.application.generator = provider
	t.Cleanup(repository.releaseReload)
	t.Cleanup(failure.releaseProvider)

	operationDone := make(chan error, 1)
	go func() { operationDone <- run(ctx) }()
	waitCOVR01OptionalFailureBoundary(t, failure.entered, "provider")
	repository.arm()
	failure.releaseProvider()
	waitCOVR01OptionalFailureBoundary(t, repository.entered, "failure recorder")

	archiveDone := make(chan error, 1)
	go func() { archiveDone <- fixture.application.ArchiveResident(ctx, fixture.residentID) }()
	waitCOVR01ArchiveAdmissionFence(t, fixture.application, fixture.residentID)
	repository.releaseReload()

	select {
	case err := <-operationDone:
		if wantOperationError && err == nil {
			t.Fatal("optional operation error = nil, want durable provider failure")
		}
		if !wantOperationError && err != nil {
			t.Fatalf("optional operation after durable provider failure: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("optional failure recorder did not finish after releasing its exact-run reload")
	}

	select {
	case err := <-archiveDone:
		if err != nil {
			t.Fatalf("ArchiveResident after failure-first linearization: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("ArchiveResident did not finish after failure recorder released the landing lock")
	}
	if calls := provider.CallCount(); calls != 1 {
		t.Fatalf("optional provider calls = %d, want 1", calls)
	}
	runID := repository.blockedRunID()
	if runID.IsZero() {
		t.Fatal("failure recorder did not expose its exact run")
	}
	var state, errorClass string
	if err := fixture.store.Reader().QueryRow(`SELECT state, COALESCE(error_class, '')
		FROM generation_run_outcomes WHERE generation_run_id = ?
		ORDER BY outcome_id DESC LIMIT 1`, runID.String()).Scan(&state, &errorClass); err != nil {
		t.Fatal(err)
	}
	wantCode := generation.OutcomeErrorCodeFromError(failure.failure).String()
	if state != "failed" || errorClass != wantCode {
		t.Fatalf("failure-first optional outcome = %s/%s, want failed/%s", state, errorClass, wantCode)
	}
	attempts, err := repository.RunningAttemptsForResident(ctx, fixture.residentID, 1)
	if err != nil {
		t.Fatal(err)
	}
	if len(attempts) != 0 {
		t.Fatalf("running attempts after Archive = %+v, want none", attempts)
	}
}

func TestCOVR01DialogueFailureLinearizesWithArchive(t *testing.T) {
	code := generation.MustOutcomeErrorCode(generation.ErrorHTTP, 503)

	t.Run("archive first", func(t *testing.T) {
		fixture, prepared := prepareCOVR01RunningDialogue(t)
		ctx := context.Background()
		injected := errors.New("injected cleanup pause after archive status commit")
		repository := &covr01ArchiveCleanupFailOnceRepository{
			CanonicalRepository: fixture.repository,
			failure:             injected,
		}
		fixture.application.repository = repository

		if err := fixture.application.ArchiveResident(ctx, fixture.residentID); !errors.Is(err, injected) {
			t.Fatalf("ArchiveResident status-only boundary = %v, want injected cleanup failure", err)
		}
		assertCOVR01FailureOutcome(t, fixture, prepared.RunID, "running", "")
		alreadyTerminal, err := fixture.application.recordOutcome(ctx, prepared, code)
		if err != nil || !alreadyTerminal {
			t.Fatalf("dialogue recorder after archive status = terminal:%v err:%v, want true/nil", alreadyTerminal, err)
		}
		assertCOVR01FailureOutcome(t, fixture, prepared.RunID, "running", "")
		if err := fixture.application.ArchiveResident(ctx, fixture.residentID); err != nil {
			t.Fatalf("ArchiveResident cleanup retry: %v", err)
		}
		assertCOVR01FailureOutcome(
			t, fixture, prepared.RunID, "cancelled", string(generation.ErrorResidentInactive),
		)
	})

	t.Run("failure first", func(t *testing.T) {
		fixture, prepared := prepareCOVR01RunningDialogue(t)
		ctx := context.Background()
		repository := newCOVR01OptionalFailureReloadBarrierRepository(fixture.repository)
		fixture.application.repository = repository
		t.Cleanup(repository.releaseReload)
		repository.arm()

		type recordResult struct {
			alreadyTerminal bool
			err             error
		}
		recorded := make(chan recordResult, 1)
		go func() {
			alreadyTerminal, err := fixture.application.recordOutcome(ctx, prepared, code)
			recorded <- recordResult{alreadyTerminal: alreadyTerminal, err: err}
		}()
		waitCOVR01OptionalFailureBoundary(t, repository.entered, "dialogue failure recorder")
		archiveDone := make(chan error, 1)
		go func() { archiveDone <- fixture.application.ArchiveResident(ctx, fixture.residentID) }()
		waitCOVR01ArchiveAdmissionFence(t, fixture.application, fixture.residentID)
		repository.releaseReload()

		select {
		case result := <-recorded:
			if result.err != nil || result.alreadyTerminal {
				t.Fatalf("failure-first dialogue recorder = terminal:%v err:%v, want false/nil",
					result.alreadyTerminal, result.err)
			}
		case <-time.After(5 * time.Second):
			t.Fatal("failure-first dialogue recorder did not release the landing lock")
		}
		select {
		case err := <-archiveDone:
			if err != nil {
				t.Fatalf("ArchiveResident after dialogue failure: %v", err)
			}
		case <-time.After(5 * time.Second):
			t.Fatal("ArchiveResident did not finish after dialogue failure commit")
		}
		assertCOVR01FailureOutcome(t, fixture, prepared.RunID, "failed", code.String())
	})
}

func TestCOVR01BackgroundFailureAlreadyArchivedBeforeReloadIsIdempotent(t *testing.T) {
	fixture, work := personaWorkForTerminalization(t, 2)
	runID := preparePersonaRevisionRun(t, fixture, work)
	prepared, err := fixture.repository.Generation(context.Background(), runID)
	if err != nil {
		t.Fatal(err)
	}
	injected := errors.New("injected cleanup pause before background failure reload")
	repository := &covr01ArchiveCleanupFailOnceRepository{
		CanonicalRepository: fixture.repository,
		failure:             injected,
	}
	fixture.application.repository = repository
	if err := fixture.application.ArchiveResident(context.Background(), fixture.residentID); !errors.Is(err, injected) {
		t.Fatalf("ArchiveResident status-only boundary = %v, want injected cleanup failure", err)
	}
	assertCOVR01FailureOutcome(t, fixture, runID, "running", "")
	code := generation.MustOutcomeErrorCode(generation.ErrorInvalidResponse, 0)
	lifecycleWon, err := fixture.application.recordBackgroundAttemptFailure(
		context.Background(), prepared, "failed", code,
	)
	if err != nil || !lifecycleWon {
		t.Fatalf("background recorder after Archive status = lifecycle:%v err:%v, want true/nil", lifecycleWon, err)
	}
	assertCOVR01FailureOutcome(t, fixture, runID, "running", "")
	if err := fixture.application.ArchiveResident(context.Background(), fixture.residentID); err != nil {
		t.Fatalf("ArchiveResident cleanup retry: %v", err)
	}
	assertCOVR01FailureOutcome(
		t, fixture, runID, "cancelled", string(generation.ErrorResidentInactive),
	)
}

func TestCOVR01FailureRecordersPreserveAmbiguousWriterPoison(t *testing.T) {
	code := generation.MustOutcomeErrorCode(generation.ErrorHTTP, 503)

	t.Run("dialogue", func(t *testing.T) {
		fixture, prepared := prepareCOVR01RunningDialogue(t)
		injected := errors.New("injected dialogue failure acknowledgement loss")
		installCOVR01FailurePoisonWriter(t, fixture, injected)

		alreadyTerminal, err := fixture.application.recordOutcome(context.Background(), prepared, code)
		if alreadyTerminal || !errors.Is(err, canonical.ErrWriterPoisoned) || !errors.Is(err, injected) {
			t.Fatalf("ambiguous dialogue failure = terminal:%v err:%v, want false + poison/injected",
				alreadyTerminal, err)
		}
		assertCOVR01FailureOutcome(t, fixture, prepared.RunID, "failed", code.String())
	})

	t.Run("background", func(t *testing.T) {
		fixture, work := personaWorkForTerminalization(t, 2)
		runID := preparePersonaRevisionRun(t, fixture, work)
		prepared, err := fixture.repository.Generation(context.Background(), runID)
		if err != nil {
			t.Fatal(err)
		}
		injected := errors.New("injected background failure acknowledgement loss")
		installCOVR01FailurePoisonWriter(t, fixture, injected)

		lifecycleWon, err := fixture.application.recordBackgroundAttemptFailure(
			context.Background(), prepared, "failed", code,
		)
		if lifecycleWon || !errors.Is(err, canonical.ErrWriterPoisoned) || !errors.Is(err, injected) {
			t.Fatalf("ambiguous background failure = lifecycle:%v err:%v, want false + poison/injected",
				lifecycleWon, err)
		}
		assertCOVR01FailureOutcome(t, fixture, runID, "failed", code.String())
	})

	t.Run("memory extraction with detail", func(t *testing.T) {
		ctx := context.Background()
		fixture := newApplicationFixture(t, &scriptedGenerator{steps: []generatorStep{{text: "dialogue"}}}, 2)
		if _, err := fixture.application.activateMemoryPolicyV4ForTest(ctx, fixture.residentID); err != nil {
			t.Fatal(err)
		}
		source, err := fixture.application.Ingress(ctx, "memory failure writer poison")
		if err != nil {
			t.Fatal(err)
		}
		if err := processForegroundDialogueUnderResidentLock(ctx, fixture, source); err != nil {
			t.Fatal(err)
		}
		works, err := discoverMemoryExtractionWorkForTest(ctx, fixture.repository, fixture.residentID, 16, 2)
		if err != nil || len(works) != 1 || works[0].State != domain.WorkPending {
			t.Fatalf("pending memory extraction = %+v err=%v", works, err)
		}
		assembly, err := fixture.application.assembleMemoryExtraction(ctx, works[0])
		if err != nil {
			t.Fatal(err)
		}
		if _, err := fixture.application.submitWithContent(
			ctx, domain.PrepareGenerationCommand(assembly.Prepare), assembly.Contents,
		); err != nil {
			t.Fatal(err)
		}
		prepared, err := fixture.repository.Generation(ctx, assembly.Prepare.RunID)
		if err != nil {
			t.Fatal(err)
		}
		detail, err := fixture.application.newContent(
			fixture.residentID, "error_detail", []byte("ambiguous invalid response"), "independent",
		)
		if err != nil {
			t.Fatal(err)
		}
		injected := errors.New("injected memory failure acknowledgement loss")
		installCOVR01MemoryFailurePoisonWriter(t, fixture, injected)
		memoryCode := generation.MustOutcomeErrorCode(generation.ErrorInvalidResponse, 0)

		err = fixture.application.recordMemoryExtractionFailure(ctx, prepared, memoryCode, &detail)
		if !errors.Is(err, canonical.ErrWriterPoisoned) || !errors.Is(err, injected) {
			t.Fatalf("ambiguous memory failure = %v, want poison/injected", err)
		}
		assertCOVR01FailureOutcome(t, fixture, prepared.RunID, "failed", memoryCode.String())
	})
}

func TestCOVR01UnsupportedEnvelopeRejectionPreservesAmbiguousWriterPoison(t *testing.T) {
	fixture, prepared := prepareCOVR01RunningDialogue(t)
	injected := errors.New("injected rejection acknowledgement loss")
	installCOVR01FailurePoisonWriter(t, fixture, injected)
	cause := errors.New("injected unsupported envelope")
	runID := prepared.RunID

	err := fixture.application.rejectUnsupportedGeneration(
		context.Background(), fixture.residentID,
		domain.DialogueWork{RunID: &runID, State: domain.WorkRunning, AttemptNo: prepared.AttemptNo},
		cause,
	)
	if !errors.Is(err, cause) || !errors.Is(err, canonical.ErrWriterPoisoned) || !errors.Is(err, injected) {
		t.Fatalf("ambiguous rejection = %v, want cause + poison/injected", err)
	}
	assertCOVR01FailureOutcome(
		t, fixture, prepared.RunID, "failed",
		generation.MustOutcomeErrorCode(generation.ErrorProviderUnsupported, 0).String(),
	)
}

func TestCOVR01FailureRecordersDoNotSuppressActiveTerminalConflict(t *testing.T) {
	firstCode := generation.MustOutcomeErrorCode(generation.ErrorInvalidResponse, 0)
	secondCode := generation.MustOutcomeErrorCode(generation.ErrorProviderUnsupported, 0)

	t.Run("dialogue", func(t *testing.T) {
		fixture, prepared := prepareCOVR01RunningDialogue(t)
		if alreadyTerminal, err := fixture.application.recordOutcome(
			context.Background(), prepared, firstCode,
		); err != nil || alreadyTerminal {
			t.Fatalf("first dialogue failure = terminal:%v err:%v", alreadyTerminal, err)
		}
		before := covr01GenerationOutcomeCount(t, fixture, prepared.RunID)
		alreadyTerminal, err := fixture.application.recordOutcome(
			context.Background(), prepared, secondCode,
		)
		if err == nil || alreadyTerminal {
			t.Fatalf("conflicting dialogue failure = terminal:%v err:%v, want false/error", alreadyTerminal, err)
		}
		if after := covr01GenerationOutcomeCount(t, fixture, prepared.RunID); after != before {
			t.Fatalf("conflicting dialogue failure outcome count = %d, want %d", after, before)
		}
		assertCOVR01FailureOutcome(t, fixture, prepared.RunID, "failed", firstCode.String())
	})

	t.Run("generic background", func(t *testing.T) {
		fixture, work := personaWorkForTerminalization(t, 2)
		runID := preparePersonaRevisionRun(t, fixture, work)
		prepared, err := fixture.repository.Generation(context.Background(), runID)
		if err != nil {
			t.Fatal(err)
		}
		if lifecycleWon, err := fixture.application.recordBackgroundAttemptFailure(
			context.Background(), prepared, "failed", firstCode,
		); err != nil || lifecycleWon {
			t.Fatalf("first background failure = lifecycle:%v err:%v", lifecycleWon, err)
		}
		before := covr01GenerationOutcomeCount(t, fixture, runID)
		lifecycleWon, err := fixture.application.recordBackgroundAttemptFailure(
			context.Background(), prepared, "failed", secondCode,
		)
		if err == nil || lifecycleWon {
			t.Fatalf("conflicting background failure = lifecycle:%v err:%v, want false/error", lifecycleWon, err)
		}
		if after := covr01GenerationOutcomeCount(t, fixture, runID); after != before {
			t.Fatalf("conflicting background failure outcome count = %d, want %d", after, before)
		}
		assertCOVR01FailureOutcome(t, fixture, runID, "failed", firstCode.String())
	})

	t.Run("memory extraction", func(t *testing.T) {
		fixture, prepared := prepareCOVR01RunningMemoryExtraction(t)
		if err := fixture.application.recordMemoryExtractionFailure(
			context.Background(), prepared, firstCode, nil,
		); err != nil {
			t.Fatal(err)
		}
		before := covr01GenerationOutcomeCount(t, fixture, prepared.RunID)
		err := fixture.application.recordMemoryExtractionFailure(
			context.Background(), prepared, secondCode, nil,
		)
		if err == nil {
			t.Fatal("conflicting memory extraction failure = nil, want error")
		}
		if after := covr01GenerationOutcomeCount(t, fixture, prepared.RunID); after != before {
			t.Fatalf("conflicting memory failure outcome count = %d, want %d", after, before)
		}
		assertCOVR01FailureOutcome(t, fixture, prepared.RunID, "failed", firstCode.String())
	})
}

func TestCOVR01UnsupportedEnvelopeDoesNotCrossArchivedStatusBoundary(t *testing.T) {
	t.Run("archive first", func(t *testing.T) {
		fixture, prepared := prepareCOVR01RunningDialogue(t)
		ctx := context.Background()
		injected := errors.New("injected cleanup pause before unsupported-envelope rejection")
		repository := &covr01ArchiveCleanupFailOnceRepository{
			CanonicalRepository: fixture.repository,
			failure:             injected,
		}
		fixture.application.repository = repository
		if err := fixture.application.ArchiveResident(ctx, fixture.residentID); !errors.Is(err, injected) {
			t.Fatalf("ArchiveResident status-only boundary = %v, want injected cleanup failure", err)
		}
		assertCOVR01FailureOutcome(t, fixture, prepared.RunID, "running", "")
		runID := prepared.RunID
		if err := fixture.application.rejectUnsupportedGeneration(ctx, fixture.residentID, domain.DialogueWork{
			RunID: &runID, State: domain.WorkRunning, AttemptNo: prepared.AttemptNo,
		}, errors.New("injected unsupported envelope")); err != nil {
			t.Fatalf("unsupported-envelope recorder after Archive status: %v", err)
		}
		assertCOVR01FailureOutcome(t, fixture, prepared.RunID, "running", "")
		if err := fixture.application.ArchiveResident(ctx, fixture.residentID); err != nil {
			t.Fatalf("ArchiveResident cleanup retry: %v", err)
		}
		assertCOVR01FailureOutcome(
			t, fixture, prepared.RunID, "cancelled", string(generation.ErrorResidentInactive),
		)
	})

	t.Run("rejection first", func(t *testing.T) {
		fixture, prepared := prepareCOVR01RunningDialogue(t)
		ctx := context.Background()
		repository := newCOVR01ResidentStatusBarrierRepository(fixture.repository)
		fixture.application.repository = repository
		t.Cleanup(repository.releaseStatus)
		repository.arm()
		cause := errors.New("injected active unsupported envelope")
		runID := prepared.RunID
		rejected := make(chan error, 1)
		go func() {
			rejected <- fixture.application.rejectUnsupportedGeneration(ctx, fixture.residentID, domain.DialogueWork{
				RunID: &runID, State: domain.WorkRunning, AttemptNo: prepared.AttemptNo,
			}, cause)
		}()
		waitCOVR01OptionalFailureBoundary(t, repository.entered, "unsupported-envelope resident status")
		archiveDone := make(chan error, 1)
		go func() { archiveDone <- fixture.application.ArchiveResident(ctx, fixture.residentID) }()
		waitCOVR01ArchiveAdmissionFence(t, fixture.application, fixture.residentID)
		repository.releaseStatus()

		select {
		case err := <-rejected:
			if !errors.Is(err, cause) {
				t.Fatalf("active unsupported-envelope result = %v, want original cause", err)
			}
		case <-time.After(5 * time.Second):
			t.Fatal("unsupported-envelope rejection did not release the landing lock")
		}
		select {
		case err := <-archiveDone:
			if err != nil {
				t.Fatalf("ArchiveResident after envelope rejection: %v", err)
			}
		case <-time.After(5 * time.Second):
			t.Fatal("ArchiveResident did not finish after envelope rejection")
		}
		providerUnsupported := generation.MustOutcomeErrorCode(generation.ErrorProviderUnsupported, 0).String()
		assertCOVR01FailureOutcome(t, fixture, prepared.RunID, "failed", providerUnsupported)
	})
}

func TestCOVR01CancellationDoesNotRejectGenerationEnvelope(t *testing.T) {
	type rejectCase struct {
		name   string
		reject func(context.Context, *Application, canonical.ID, canonical.ID, error) error
	}
	tests := []rejectCase{
		{
			name: "memory extraction",
			reject: func(ctx context.Context, application *Application, residentID, runID canonical.ID, cause error) error {
				return application.rejectMemoryExtractionEnvelope(ctx, domain.MemoryExtractionWork{
					SourceEvent: domain.Event{ResidentID: residentID}, RunID: &runID,
				}, cause)
			},
		},
		{
			name: "memory alignment",
			reject: func(ctx context.Context, application *Application, residentID, runID canonical.ID, cause error) error {
				return application.rejectMemoryAlignmentEnvelope(ctx, domain.MemoryAlignmentWork{
					Resident: domain.ResidentSnapshot{ResidentID: residentID}, RunID: &runID,
				}, cause)
			},
		},
		{
			name: "autonomy",
			reject: func(ctx context.Context, application *Application, residentID, runID canonical.ID, cause error) error {
				return application.rejectAutonomousEnvelope(ctx, domain.AutonomousWork{
					ResidentID: residentID, RunID: &runID,
				}, cause)
			},
		},
	}
	causes := []struct {
		name string
		err  error
	}{
		{name: "cancelled", err: context.Canceled},
		{name: "deadline", err: context.DeadlineExceeded},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			for _, cause := range causes {
				t.Run(cause.name, func(t *testing.T) {
					fixture, prepared := prepareCOVR01RunningDialogue(t)
					err := test.reject(
						context.Background(), fixture.application, fixture.residentID, prepared.RunID, cause.err,
					)
					if !errors.Is(err, cause.err) {
						t.Fatalf("cancellation result = %v, want %v", err, cause.err)
					}
					assertCOVR01FailureOutcome(t, fixture, prepared.RunID, "running", "")
				})
			}
		})
	}
}

func TestCOVR01UnsupportedEnvelopeRejectionRequiresExactTerminalReplay(t *testing.T) {
	t.Run("same rejection replay", func(t *testing.T) {
		fixture, prepared := prepareCOVR01RunningDialogue(t)
		ctx := context.Background()
		if _, lifecycleWon, err := fixture.application.rejectGenerationEnvelopeAtResidentBoundary(
			ctx, fixture.residentID, prepared.RunID,
		); err != nil || lifecycleWon {
			t.Fatalf("initial rejection = lifecycle:%v err:%v", lifecycleWon, err)
		}
		before := covr01GenerationOutcomeCount(t, fixture, prepared.RunID)
		if _, lifecycleWon, err := fixture.application.rejectGenerationEnvelopeAtResidentBoundary(
			ctx, fixture.residentID, prepared.RunID,
		); err != nil || lifecycleWon {
			t.Fatalf("exact rejection replay = lifecycle:%v err:%v", lifecycleWon, err)
		}
		if after := covr01GenerationOutcomeCount(t, fixture, prepared.RunID); after != before {
			t.Fatalf("exact rejection replay outcome count = %d, want %d", after, before)
		}
		assertCOVR01FailureOutcome(
			t, fixture, prepared.RunID, "failed",
			generation.MustOutcomeErrorCode(generation.ErrorProviderUnsupported, 0).String(),
		)
	})

	tests := []struct {
		name        string
		terminalize func(*testing.T, applicationFixture, domain.PreparedGeneration)
		wantState   string
		wantCode    string
	}{
		{
			name: "different nonretryable failure",
			terminalize: func(t *testing.T, fixture applicationFixture, prepared domain.PreparedGeneration) {
				code := generation.MustOutcomeErrorCode(generation.ErrorInvalidResponse, 0)
				if _, err := fixture.application.recordOutcome(context.Background(), prepared, code); err != nil {
					t.Fatal(err)
				}
			},
			wantState: "failed",
			wantCode:  generation.MustOutcomeErrorCode(generation.ErrorInvalidResponse, 0).String(),
		},
		{
			name: "different cancellation",
			terminalize: func(t *testing.T, fixture applicationFixture, prepared domain.PreparedGeneration) {
				code := generation.MustOutcomeErrorCode(generation.ErrorResidentUnselected, 0)
				if lifecycleWon, err := fixture.application.recordBackgroundAttemptFailure(
					context.Background(), prepared, "cancelled", code,
				); err != nil || lifecycleWon {
					t.Fatalf("terminalize cancellation = lifecycle:%v err:%v", lifecycleWon, err)
				}
			},
			wantState: "cancelled",
			wantCode:  generation.MustOutcomeErrorCode(generation.ErrorResidentUnselected, 0).String(),
		},
		{
			name: "succeeded",
			terminalize: func(t *testing.T, fixture applicationFixture, prepared domain.PreparedGeneration) {
				fixture.application.generator = &scriptedGenerator{steps: []generatorStep{{text: "terminal winner"}}}
				if err := fixture.application.callAndLand(context.Background(), prepared); err != nil {
					t.Fatal(err)
				}
			},
			wantState: "succeeded",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			fixture, prepared := prepareCOVR01RunningDialogue(t)
			test.terminalize(t, fixture, prepared)
			before := covr01GenerationOutcomeCount(t, fixture, prepared.RunID)
			_, lifecycleWon, err := fixture.application.rejectGenerationEnvelopeAtResidentBoundary(
				context.Background(), fixture.residentID, prepared.RunID,
			)
			if err == nil || lifecycleWon || !strings.Contains(err.Error(), "conflicts with existing") {
				t.Fatalf("conflicting rejection = lifecycle:%v err:%v, want explicit conflict", lifecycleWon, err)
			}
			if after := covr01GenerationOutcomeCount(t, fixture, prepared.RunID); after != before {
				t.Fatalf("conflicting rejection outcome count = %d, want %d", after, before)
			}
			assertCOVR01FailureOutcome(t, fixture, prepared.RunID, test.wantState, test.wantCode)
		})
	}
}

func prepareCOVR01RunningDialogue(t *testing.T) (applicationFixture, domain.PreparedGeneration) {
	t.Helper()
	fixture := newApplicationFixture(t, &scriptedGenerator{}, 1)
	event := ingressPendingForTest(t, fixture, "COVR dialogue failure linearization")
	prepareDialogueRunForTest(t, fixture, event)
	runID := dialogueRunIDForEventForTest(t, fixture, event)
	prepared, err := fixture.repository.Generation(context.Background(), runID)
	if err != nil {
		t.Fatal(err)
	}
	if prepared.State != domain.WorkRunning || prepared.AttemptNo != 1 {
		t.Fatalf("prepared dialogue = %+v, want running attempt 1", prepared)
	}
	return fixture, prepared
}

func prepareCOVR01RunningMemoryExtraction(t *testing.T) (applicationFixture, domain.PreparedGeneration) {
	t.Helper()
	ctx := context.Background()
	fixture := newApplicationFixture(t, &scriptedGenerator{steps: []generatorStep{{text: "dialogue"}}}, 2)
	if _, err := fixture.application.activateMemoryPolicyV4ForTest(ctx, fixture.residentID); err != nil {
		t.Fatal(err)
	}
	source, err := fixture.application.Ingress(ctx, "COVR running memory extraction")
	if err != nil {
		t.Fatal(err)
	}
	if err := processForegroundDialogueUnderResidentLock(ctx, fixture, source); err != nil {
		t.Fatal(err)
	}
	works, err := discoverMemoryExtractionWorkForTest(ctx, fixture.repository, fixture.residentID, 16, 2)
	if err != nil || len(works) != 1 || works[0].State != domain.WorkPending {
		t.Fatalf("pending memory extraction = %+v err=%v", works, err)
	}
	assembly, err := fixture.application.assembleMemoryExtraction(ctx, works[0])
	if err != nil {
		t.Fatal(err)
	}
	if _, err := fixture.application.submitWithContent(
		ctx, domain.PrepareGenerationCommand(assembly.Prepare), assembly.Contents,
	); err != nil {
		t.Fatal(err)
	}
	prepared, err := fixture.repository.Generation(ctx, assembly.Prepare.RunID)
	if err != nil || prepared.State != domain.WorkRunning {
		t.Fatalf("prepared memory extraction = %+v err=%v", prepared, err)
	}
	return fixture, prepared
}

func installCOVR01FailurePoisonWriter(t *testing.T, fixture applicationFixture, injected error) {
	t.Helper()
	ctx := context.Background()
	if err := fixture.writer.Close(ctx); err != nil {
		t.Fatalf("close fixture writer before poison test: %v", err)
	}
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
			t.Errorf("close failure poison writer: %v", err)
		}
	})
	fixture.application.writer = writer
}

func installCOVR01MemoryFailurePoisonWriter(t *testing.T, fixture applicationFixture, injected error) {
	t.Helper()
	ctx := context.Background()
	if err := fixture.writer.Close(ctx); err != nil {
		t.Fatalf("close fixture writer before memory poison test: %v", err)
	}
	base := &covr01AmbiguousCommitBackend{delegate: fixture.repository, failure: injected}
	writer, err := canonical.OpenWriter(ctx, canonical.WriterOptions{
		Backend: &covr01AmbiguousMemoryFailureBackend{covr01AmbiguousCommitBackend: base},
		IDs:     fixture.application.ids, Clock: fixture.clock,
		Timezone: fixture.application.timezone, QueueCapacity: 32,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := writer.Close(context.Background()); err != nil {
			t.Errorf("close memory failure poison writer: %v", err)
		}
	})
	fixture.application.writer = writer
}

func assertCOVR01FailureOutcome(
	t *testing.T,
	fixture applicationFixture,
	runID canonical.ID,
	wantState string,
	wantCode string,
) {
	t.Helper()
	var state, errorClass string
	if err := fixture.store.Reader().QueryRow(`SELECT state, COALESCE(error_class, '')
		FROM generation_run_outcomes WHERE generation_run_id = ?
		ORDER BY outcome_id DESC LIMIT 1`, runID.String()).Scan(&state, &errorClass); err != nil {
		t.Fatal(err)
	}
	if state != wantState || errorClass != wantCode {
		t.Fatalf("generation outcome = %s/%s, want %s/%s",
			state, errorClass, wantState, wantCode)
	}
}

func covr01GenerationOutcomeCount(t *testing.T, fixture applicationFixture, runID canonical.ID) int {
	t.Helper()
	var count int
	if err := fixture.store.Reader().QueryRow(
		`SELECT COUNT(*) FROM generation_run_outcomes WHERE generation_run_id = ?`, runID.String(),
	).Scan(&count); err != nil {
		t.Fatal(err)
	}
	return count
}

func assertCOVR01AttemptOutcome(
	t *testing.T,
	fixture applicationFixture,
	runID canonical.ID,
	attemptNo int,
	wantState string,
	wantCode string,
) {
	t.Helper()
	var state, errorClass string
	if err := fixture.store.Reader().QueryRow(`SELECT state, COALESCE(error_class, '')
		FROM generation_run_outcomes WHERE generation_run_id = ? AND attempt_no = ?
		ORDER BY outcome_id DESC LIMIT 1`, runID.String(), attemptNo).Scan(&state, &errorClass); err != nil {
		t.Fatal(err)
	}
	if state != wantState || errorClass != wantCode {
		t.Fatalf("generation attempt %d outcome = %s/%s, want %s/%s",
			attemptNo, state, errorClass, wantState, wantCode)
	}
}

func waitCOVR01OptionalFailureBoundary(t *testing.T, ready <-chan struct{}, label string) {
	t.Helper()
	select {
	case <-ready:
	case <-time.After(5 * time.Second):
		t.Fatalf("timed out waiting for %s boundary", label)
	}
}

func waitCOVR01ArchiveAdmissionFence(t *testing.T, application *Application, residentID canonical.ID) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		application.backgroundMu.Lock()
		blocked := application.backgroundBlocked[residentID]
		application.backgroundMu.Unlock()
		if blocked {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("ArchiveResident did not close provider admission before the recorder barrier was released")
}
