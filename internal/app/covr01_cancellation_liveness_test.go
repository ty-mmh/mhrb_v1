package app

import (
	"context"
	"sync"
	"testing"
	"time"

	"mahoroba.local/mahoroba/internal/canonical"
	"mahoroba.local/mahoroba/internal/domain"
	"mahoroba.local/mahoroba/internal/generation"
	"mahoroba.local/mahoroba/internal/store/sqlite"
)

type covr01CancellationDiscoveryBarrierRepository struct {
	*sqlite.CanonicalRepository
	reextraction bool
	entered      chan struct{}
	release      chan struct{}
	enterOnce    sync.Once
	releaseOnce  sync.Once
}

// covr01CancellationArchiveBarrierRepository blocks only the first typed
// cancellation discovery. ArchiveResident must remain free to run its own
// authoritative discovery and terminalization while the stale queue item is
// held immediately after classification.
type covr01CancellationArchiveBarrierRepository struct {
	*sqlite.CanonicalRepository
	reextraction bool
	mu           sync.Mutex
	blocked      bool
	entered      chan struct{}
	release      chan struct{}
	enterOnce    sync.Once
	releaseOnce  sync.Once
}

type covr01MemoryFailureReloadBarrierRepository struct {
	*sqlite.CanonicalRepository
	mu          sync.Mutex
	armed       bool
	entered     chan struct{}
	release     chan struct{}
	enterOnce   sync.Once
	releaseOnce sync.Once
}

func newCOVR01MemoryFailureReloadBarrierRepository(
	repository *sqlite.CanonicalRepository,
) *covr01MemoryFailureReloadBarrierRepository {
	return &covr01MemoryFailureReloadBarrierRepository{
		CanonicalRepository: repository,
		entered:             make(chan struct{}),
		release:             make(chan struct{}),
	}
}

func (repository *covr01MemoryFailureReloadBarrierRepository) arm() {
	repository.mu.Lock()
	repository.armed = true
	repository.mu.Unlock()
}

func (repository *covr01MemoryFailureReloadBarrierRepository) Generation(
	ctx context.Context,
	runID canonical.ID,
) (domain.PreparedGeneration, error) {
	prepared, err := repository.CanonicalRepository.Generation(ctx, runID)
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
	return prepared, err
}

func (repository *covr01MemoryFailureReloadBarrierRepository) releaseReload() {
	repository.releaseOnce.Do(func() { close(repository.release) })
}

func newCOVR01CancellationDiscoveryBarrierRepository(
	repository *sqlite.CanonicalRepository,
	reextraction bool,
) *covr01CancellationDiscoveryBarrierRepository {
	return &covr01CancellationDiscoveryBarrierRepository{
		CanonicalRepository: repository,
		reextraction:        reextraction,
		entered:             make(chan struct{}),
		release:             make(chan struct{}),
	}
}

func newCOVR01CancellationArchiveBarrierRepository(
	repository *sqlite.CanonicalRepository,
	reextraction bool,
) *covr01CancellationArchiveBarrierRepository {
	return &covr01CancellationArchiveBarrierRepository{
		CanonicalRepository: repository,
		reextraction:        reextraction,
		entered:             make(chan struct{}),
		release:             make(chan struct{}),
	}
}

func (repository *covr01CancellationArchiveBarrierRepository) DiscoverMemoryExtractionWork(
	ctx context.Context,
	residentID canonical.ID,
	request domain.MemoryDiscoveryRequest,
) (domain.MemoryDiscoveryResult, error) {
	result, err := repository.CanonicalRepository.DiscoverMemoryExtractionWork(ctx, residentID, request)
	if err == nil && !repository.reextraction && result.Work != nil && result.Work.CancellationCode != "" {
		repository.waitFirst()
	}
	return result, err
}

func (repository *covr01CancellationArchiveBarrierRepository) DiscoverMemoryReextractionWork(
	ctx context.Context,
	residentID canonical.ID,
	request domain.MemoryReextractionDiscoveryRequest,
) (domain.MemoryReextractionDiscoveryResult, error) {
	result, err := repository.CanonicalRepository.DiscoverMemoryReextractionWork(ctx, residentID, request)
	if err == nil && repository.reextraction && result.Work != nil && result.Work.CancellationCode != "" {
		repository.waitFirst()
	}
	return result, err
}

func (repository *covr01CancellationArchiveBarrierRepository) waitFirst() {
	repository.mu.Lock()
	first := !repository.blocked
	if first {
		repository.blocked = true
	}
	repository.mu.Unlock()
	if !first {
		return
	}
	repository.enterOnce.Do(func() { close(repository.entered) })
	<-repository.release
}

func (repository *covr01CancellationArchiveBarrierRepository) releaseDiscovery() {
	repository.releaseOnce.Do(func() { close(repository.release) })
}

func (repository *covr01CancellationDiscoveryBarrierRepository) DiscoverMemoryExtractionWork(
	ctx context.Context,
	residentID canonical.ID,
	request domain.MemoryDiscoveryRequest,
) (domain.MemoryDiscoveryResult, error) {
	result, err := repository.CanonicalRepository.DiscoverMemoryExtractionWork(ctx, residentID, request)
	if err == nil && !repository.reextraction && result.Work != nil && result.Work.CancellationCode != "" {
		repository.wait()
	}
	return result, err
}

func (repository *covr01CancellationDiscoveryBarrierRepository) DiscoverMemoryReextractionWork(
	ctx context.Context,
	residentID canonical.ID,
	request domain.MemoryReextractionDiscoveryRequest,
) (domain.MemoryReextractionDiscoveryResult, error) {
	result, err := repository.CanonicalRepository.DiscoverMemoryReextractionWork(ctx, residentID, request)
	if err == nil && repository.reextraction && result.Work != nil && result.Work.CancellationCode != "" {
		repository.wait()
	}
	return result, err
}

func (repository *covr01CancellationDiscoveryBarrierRepository) wait() {
	repository.enterOnce.Do(func() {
		close(repository.entered)
		<-repository.release
	})
}

func (repository *covr01CancellationDiscoveryBarrierRepository) releaseDiscovery() {
	repository.releaseOnce.Do(func() { close(repository.release) })
}

func TestCOVR01FairTypedCancellationSurvivesIngressBeforeCursorPublish(t *testing.T) {
	fixture, source, work, provider := prepareCOVR01ErasedMandatoryCancellation(t)
	ctx := context.Background()
	barrier := newCOVR01CancellationDiscoveryBarrierRepository(fixture.repository, false)
	fixture.application.repository = barrier
	t.Cleanup(barrier.releaseDiscovery)

	type fairResult struct {
		worked bool
		err    error
	}
	done := make(chan fairResult, 1)
	go func() {
		lock := fixture.application.residentLock(fixture.residentID)
		lock.Lock()
		defer lock.Unlock()
		worked, err := fixture.application.processFairMandatoryMemory(ctx, fixture.residentID, source.Seq)
		done <- fairResult{worked: worked, err: err}
	}()
	waitCOVR01CancellationDiscovery(t, barrier.entered, "fair")
	commitCOVR01ContinuousIngress(t, fixture, 3, "fair cancellation crossing")
	barrier.releaseDiscovery()

	select {
	case result := <-done:
		if result.err != nil || !result.worked {
			t.Fatalf("fair typed cancellation = worked:%v err:%v, want true/nil", result.worked, result.err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("fair typed cancellation did not finish after foreground advance")
	}
	if calls := provider.CallCount(); calls != 0 {
		t.Fatalf("fair typed cancellation provider calls = %d, want 0", calls)
	}
	assertCOVR01QueuedCancellationIdempotent(t, fixture, work, domain.MemoryExtractionObligation(source.ID), canonical.ID{}, 0)
}

func TestCOVR01NormalTypedCancellationSurvivesIngressAfterDiscovery(t *testing.T) {
	fixture, source, work, provider := prepareCOVR01ErasedMandatoryCancellation(t)
	ctx := context.Background()
	barrier := newCOVR01CancellationDiscoveryBarrierRepository(fixture.repository, false)
	fixture.application.repository = barrier
	t.Cleanup(barrier.releaseDiscovery)

	done := make(chan error, 1)
	go func() {
		lock := fixture.application.residentLock(fixture.residentID)
		lock.Lock()
		defer lock.Unlock()
		done <- fixture.application.processMemoryExtractionQueue(ctx, fixture.residentID)
	}()
	waitCOVR01CancellationDiscovery(t, barrier.entered, "normal")
	commitCOVR01ContinuousIngress(t, fixture, 3, "normal cancellation crossing")
	barrier.releaseDiscovery()

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("normal typed cancellation: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("normal typed cancellation did not finish after foreground advance")
	}
	if calls := provider.CallCount(); calls != 0 {
		t.Fatalf("normal typed cancellation provider calls = %d, want 0", calls)
	}
	assertCOVR01QueuedCancellationIdempotent(t, fixture, work, domain.MemoryExtractionObligation(source.ID), canonical.ID{}, 0)
}

func TestCOVR01ReextractionTypedCancellationSurvivesSelectionCursorInvalidation(t *testing.T) {
	ctx := context.Background()
	fixture := newApplicationFixture(t, &scriptedGenerator{steps: []generatorStep{{text: "dialogue"}}}, 2)
	source, err := fixture.application.Ingress(ctx, "source for re-extraction cancellation crossing")
	if err != nil {
		t.Fatal(err)
	}
	if err := fixture.application.ProcessResident(ctx, fixture.residentID); err != nil {
		t.Fatal(err)
	}
	if _, err := fixture.application.activateMemoryPolicyV4ForTest(ctx, fixture.residentID); err != nil {
		t.Fatal(err)
	}

	retryProvider := &scriptedGenerator{steps: []generatorStep{{
		err: &generation.ProviderError{Class: generation.ErrorTimeout, Detail: "retry before erased cancellation"},
	}}}
	fixture.application.generator = retryProvider
	requestID := memoryExtractionTestID(t, fixture)
	first, err := fixture.application.ReextractMemoryEvent(ctx, fixture.residentID, source.ID, requestID)
	if err != nil {
		t.Fatal(err)
	}
	if first.State != domain.WorkRetryPending || first.AttemptNo != 1 || retryProvider.CallCount() != 1 {
		t.Fatalf("prepared re-extraction retry = %+v calls=%d", first, retryProvider.CallCount())
	}
	eraseEventContentForTest(t, fixture.store.Path(), source.ContentID)
	work, exists, err := fixture.repository.MemoryReextractionWork(
		ctx, fixture.residentID, source.ID, requestID, fixture.application.maxAttempts,
	)
	if err != nil || !exists || work.CancellationCode != string(generation.ErrorSourceContentErased) {
		t.Fatalf("erased re-extraction classification = %+v exists=%v err=%v", work, exists, err)
	}

	provider := &scriptedGenerator{}
	fixture.application.generator = provider
	selected := createCOVR01ActiveResident(t, fixture, "covr01-cancellation-liveness-selection")
	epoch := fixture.application.captureForegroundEpoch(fixture.residentID)
	fixture.application.memoryStateMu.Lock()
	fixture.application.memoryReextractionCycles[fixture.residentID] = true
	fixture.application.memoryReextractionCycleEpochs[fixture.residentID] = epoch
	fixture.application.memoryStateMu.Unlock()

	barrier := newCOVR01CancellationDiscoveryBarrierRepository(fixture.repository, true)
	fixture.application.repository = barrier
	t.Cleanup(barrier.releaseDiscovery)
	done := make(chan error, 1)
	go func() {
		lock := fixture.application.residentLock(fixture.residentID)
		lock.Lock()
		defer lock.Unlock()
		done <- fixture.application.processMemoryExtractionQueue(ctx, fixture.residentID)
	}()
	waitCOVR01CancellationDiscovery(t, barrier.entered, "re-extraction")
	if err := fixture.application.SelectResident(ctx, selected); err != nil {
		t.Fatal(err)
	}
	barrier.releaseDiscovery()

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("re-extraction typed cancellation: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("re-extraction typed cancellation did not finish after selection invalidated its cursor")
	}
	if calls := provider.CallCount(); calls != 0 {
		t.Fatalf("re-extraction typed cancellation provider calls = %d, want 0", calls)
	}
	assertCOVR01QueuedCancellationIdempotent(
		t, fixture, work, domain.MemoryReextractionObligation(source.ID, requestID), first.RunID, 2,
	)
}

func TestCOVR01TypedCancellationClassifiedBeforeArchiveConvergesIdempotently(t *testing.T) {
	t.Run("runless mandatory", func(t *testing.T) {
		fixture, source, work, provider := prepareCOVR01ErasedMandatoryCancellation(t)
		ctx := context.Background()
		barrier := newCOVR01CancellationArchiveBarrierRepository(fixture.repository, false)
		fixture.application.repository = barrier
		t.Cleanup(barrier.releaseDiscovery)

		done := make(chan error, 1)
		go func() {
			lock := fixture.application.residentLock(fixture.residentID)
			lock.Lock()
			defer lock.Unlock()
			done <- fixture.application.processMemoryExtractionQueue(ctx, fixture.residentID)
		}()
		waitCOVR01CancellationDiscovery(t, barrier.entered, "runless archive crossing")
		archiveCOVR01BeforeCancellationRelease(t, fixture)
		assertCOVR01QueuedCancellationOutcome(
			t, fixture, domain.MemoryExtractionObligation(source.ID), canonical.ID{}, 0, 1,
		)
		barrier.releaseDiscovery()
		waitCOVR01CancellationWorker(t, done, "runless typed cancellation after ArchiveResident")
		if calls := provider.CallCount(); calls != 0 {
			t.Fatalf("runless typed cancellation provider calls = %d, want 0", calls)
		}
		assertCOVR01QueuedCancellationIdempotent(
			t, fixture, work, domain.MemoryExtractionObligation(source.ID), canonical.ID{}, 0,
		)
	})

	t.Run("existing reextraction run", func(t *testing.T) {
		ctx := context.Background()
		fixture := newApplicationFixture(t, &scriptedGenerator{steps: []generatorStep{{text: "dialogue"}}}, 2)
		source, err := fixture.application.Ingress(ctx, "source for re-extraction archive crossing")
		if err != nil {
			t.Fatal(err)
		}
		if err := fixture.application.ProcessResident(ctx, fixture.residentID); err != nil {
			t.Fatal(err)
		}
		if _, err := fixture.application.activateMemoryPolicyV4ForTest(ctx, fixture.residentID); err != nil {
			t.Fatal(err)
		}

		retryProvider := &scriptedGenerator{steps: []generatorStep{{
			err: &generation.ProviderError{Class: generation.ErrorTimeout, Detail: "retry before archive cancellation"},
		}}}
		fixture.application.generator = retryProvider
		requestID := memoryExtractionTestID(t, fixture)
		first, err := fixture.application.ReextractMemoryEvent(ctx, fixture.residentID, source.ID, requestID)
		if err != nil {
			t.Fatal(err)
		}
		if first.State != domain.WorkRetryPending || first.AttemptNo != 1 || retryProvider.CallCount() != 1 {
			t.Fatalf("prepared re-extraction retry = %+v calls=%d", first, retryProvider.CallCount())
		}
		eraseEventContentForTest(t, fixture.store.Path(), source.ContentID)
		work, exists, err := fixture.repository.MemoryReextractionWork(
			ctx, fixture.residentID, source.ID, requestID, fixture.application.maxAttempts,
		)
		if err != nil || !exists || work.CancellationCode != string(generation.ErrorSourceContentErased) ||
			work.RunID == nil || *work.RunID != first.RunID {
			t.Fatalf("erased re-extraction classification = %+v exists=%v err=%v", work, exists, err)
		}

		provider := &scriptedGenerator{}
		fixture.application.generator = provider
		epoch := fixture.application.captureForegroundEpoch(fixture.residentID)
		fixture.application.memoryStateMu.Lock()
		fixture.application.memoryReextractionCycles[fixture.residentID] = true
		fixture.application.memoryReextractionCycleEpochs[fixture.residentID] = epoch
		fixture.application.memoryStateMu.Unlock()

		barrier := newCOVR01CancellationArchiveBarrierRepository(fixture.repository, true)
		fixture.application.repository = barrier
		t.Cleanup(barrier.releaseDiscovery)
		done := make(chan error, 1)
		go func() {
			lock := fixture.application.residentLock(fixture.residentID)
			lock.Lock()
			defer lock.Unlock()
			done <- fixture.application.processMemoryExtractionQueue(ctx, fixture.residentID)
		}()
		waitCOVR01CancellationDiscovery(t, barrier.entered, "re-extraction archive crossing")
		archiveCOVR01BeforeCancellationRelease(t, fixture)
		assertCOVR01QueuedCancellationOutcome(
			t, fixture, domain.MemoryReextractionObligation(source.ID, requestID), first.RunID, 2, 1,
		)
		barrier.releaseDiscovery()
		waitCOVR01CancellationWorker(t, done, "re-extraction typed cancellation after ArchiveResident")
		if calls := provider.CallCount(); calls != 0 {
			t.Fatalf("re-extraction typed cancellation provider calls = %d, want 0", calls)
		}
		assertCOVR01QueuedCancellationIdempotent(
			t, fixture, work, domain.MemoryReextractionObligation(source.ID, requestID), first.RunID, 2,
		)
	})
}

func TestCOVR01MemoryFailureLinearizesBeforeArchive(t *testing.T) {
	for _, test := range []struct {
		name string
		step generatorStep
	}{
		{
			name: "provider error without detail",
			step: generatorStep{err: &generation.ProviderError{
				Class: generation.ErrorTimeout, Detail: "failure wins before archive",
			}},
		},
		{name: "invalid response with detail", step: generatorStep{text: "not-json"}},
	} {
		t.Run(test.name, func(t *testing.T) {
			ctx := context.Background()
			fixture := newApplicationFixture(t, &scriptedGenerator{steps: []generatorStep{{text: "dialogue"}}}, 2)
			if _, err := fixture.application.activateMemoryPolicyV4ForTest(ctx, fixture.residentID); err != nil {
				t.Fatal(err)
			}
			source, err := fixture.application.Ingress(ctx, "archive between memory failure reload and write")
			if err != nil {
				t.Fatal(err)
			}
			if err := processForegroundDialogueUnderResidentLock(ctx, fixture, source); err != nil {
				t.Fatal(err)
			}
			works, err := discoverMemoryExtractionWorkForTest(ctx, fixture.repository, fixture.residentID, 16, 2)
			if err != nil {
				t.Fatal(err)
			}
			var work *domain.MemoryExtractionWork
			for index := range works {
				if works[index].SourceEvent.ID == source.ID {
					work = &works[index]
					break
				}
			}
			if work == nil || work.State != domain.WorkPending {
				t.Fatalf("pending extraction work = %+v", work)
			}
			seedForegroundCleanProofForTest(t, fixture)

			barrier := newCOVR01MemoryFailureReloadBarrierRepository(fixture.repository)
			fixture.application.repository = barrier
			t.Cleanup(barrier.releaseReload)
			test.step.beforeReturn = barrier.arm
			provider := &scriptedGenerator{steps: []generatorStep{test.step}}
			fixture.application.generator = provider
			done := make(chan error, 1)
			go func() {
				lock := fixture.application.residentLock(fixture.residentID)
				lock.Lock()
				defer lock.Unlock()
				done <- fixture.application.processMemoryExtractionWork(ctx, *work)
			}()

			waitCOVR01CancellationDiscovery(t, barrier.entered, "memory failure reload under landing lock")
			// Retryable failures can cause Archive cleanup to synthesize a later
			// resident_inactive attempt for the same obligation. Pin the exact run
			// whose failure recorder currently owns the landing boundary.
			runID := generationRunIDForKey(t, fixture, work.IdempotencyKey)
			archiveDone := make(chan error, 1)
			go func() { archiveDone <- fixture.application.ArchiveResident(ctx, fixture.residentID) }()
			waitCOVR01ArchiveAdmissionFence(t, fixture.application, fixture.residentID)
			barrier.releaseReload()
			select {
			case err := <-done:
				if err != nil {
					t.Fatalf("memory failure before ArchiveResident: %v", err)
				}
			case <-time.After(5 * time.Second):
				t.Fatal("memory failure recorder did not release the landing lock")
			}
			select {
			case err := <-archiveDone:
				if err != nil {
					t.Fatalf("ArchiveResident after memory failure: %v", err)
				}
			case <-time.After(5 * time.Second):
				t.Fatal("ArchiveResident did not finish after memory failure commit")
			}
			if calls := provider.CallCount(); calls != 1 {
				t.Fatalf("memory failure provider calls = %d, want 1", calls)
			}
			wantCode := generation.MustOutcomeErrorCode(generation.ErrorInvalidResponse, 0).String()
			if test.step.err != nil {
				wantCode = generation.OutcomeErrorCodeFromError(test.step.err).String()
			}
			assertCOVR01AttemptOutcome(t, fixture, runID, 1, "failed", wantCode)
			attempts, err := fixture.repository.RunningAttemptsForResident(ctx, fixture.residentID, 1)
			if err != nil {
				t.Fatal(err)
			}
			if len(attempts) != 0 {
				t.Fatalf("running memory attempts after Archive = %+v, want none", attempts)
			}
		})
	}
}

func TestCOVR01MemoryFailureAlreadyArchivedBeforeReloadIsIdempotent(t *testing.T) {
	for _, withDetail := range []bool{false, true} {
		name := "without detail"
		if withDetail {
			name = "with detail"
		}
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			fixture := newApplicationFixture(t, &scriptedGenerator{steps: []generatorStep{{text: "dialogue"}}}, 2)
			if _, err := fixture.application.activateMemoryPolicyV4ForTest(ctx, fixture.residentID); err != nil {
				t.Fatal(err)
			}
			source, err := fixture.application.Ingress(ctx, "archive before memory failure reload")
			if err != nil {
				t.Fatal(err)
			}
			if err := processForegroundDialogueUnderResidentLock(ctx, fixture, source); err != nil {
				t.Fatal(err)
			}
			works, err := discoverMemoryExtractionWorkForTest(ctx, fixture.repository, fixture.residentID, 16, 2)
			if err != nil || len(works) != 1 || works[0].State != domain.WorkPending {
				t.Fatalf("pending extraction work = %+v err=%v", works, err)
			}
			work := works[0]
			assembly, err := fixture.application.assembleMemoryExtraction(ctx, work)
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
				t.Fatalf("prepared extraction = %+v err=%v", prepared, err)
			}
			if err := fixture.application.ArchiveResident(ctx, fixture.residentID); err != nil {
				t.Fatal(err)
			}
			var detail *domain.Content
			if withDetail {
				value, err := fixture.application.newContent(
					fixture.residentID, "error_detail", []byte("must not be committed"), "independent",
				)
				if err != nil {
					t.Fatal(err)
				}
				detail = &value
			}
			code := generation.MustOutcomeErrorCode(generation.ErrorInvalidResponse, 0)
			if err := fixture.application.recordMemoryExtractionFailure(ctx, prepared, code, detail); err != nil {
				t.Fatalf("already-archived memory failure: %v", err)
			}
			assertCOVR01ArchivedMemoryFailureOutcome(t, fixture, work.IdempotencyKey)
		})
	}
}

func prepareCOVR01ErasedMandatoryCancellation(
	t *testing.T,
) (applicationFixture, domain.Event, domain.MemoryExtractionWork, *scriptedGenerator) {
	t.Helper()
	ctx := context.Background()
	fixture := newApplicationFixture(t, &scriptedGenerator{steps: []generatorStep{{text: "dialogue"}}}, 1)
	if _, err := fixture.application.activateMemoryPolicyV4ForTest(ctx, fixture.residentID); err != nil {
		t.Fatal(err)
	}
	source, err := fixture.application.Ingress(ctx, "source erased before mandatory cancellation")
	if err != nil {
		t.Fatal(err)
	}
	if err := processForegroundDialogueUnderResidentLock(ctx, fixture, source); err != nil {
		t.Fatal(err)
	}
	eraseEventContentForTest(t, fixture.store.Path(), source.ContentID)
	works, err := discoverMemoryExtractionWorkForTest(ctx, fixture.repository, fixture.residentID, 16, 1)
	if err != nil {
		t.Fatal(err)
	}
	var work *domain.MemoryExtractionWork
	for index := range works {
		if works[index].SourceEvent.ID == source.ID {
			work = &works[index]
			break
		}
	}
	if work == nil || work.CancellationCode != string(generation.ErrorSourceContentErased) || work.RunID != nil {
		t.Fatalf("erased mandatory classification = %+v", work)
	}
	provider := &scriptedGenerator{}
	fixture.application.generator = provider
	return fixture, source, *work, provider
}

func commitCOVR01ContinuousIngress(t *testing.T, fixture applicationFixture, count int, prefix string) {
	t.Helper()
	for index := 0; index < count; index++ {
		if _, err := fixture.application.Ingress(context.Background(), prefix); err != nil {
			t.Fatal(err)
		}
	}
}

func waitCOVR01CancellationDiscovery(t *testing.T, entered <-chan struct{}, queue string) {
	t.Helper()
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatalf("%s queue did not discover typed cancellation", queue)
	}
}

func archiveCOVR01BeforeCancellationRelease(t *testing.T, fixture applicationFixture) {
	t.Helper()
	done := make(chan error, 1)
	go func() {
		done <- fixture.application.ArchiveResident(context.Background(), fixture.residentID)
	}()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("ArchiveResident before stale typed cancellation: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("ArchiveResident deadlocked behind stale typed cancellation")
	}
}

func waitCOVR01CancellationWorker(t *testing.T, done <-chan error, operation string) {
	t.Helper()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("%s: %v", operation, err)
		}
	case <-time.After(5 * time.Second):
		t.Fatalf("%s did not finish", operation)
	}
}

func assertCOVR01QueuedCancellationIdempotent(
	t *testing.T,
	fixture applicationFixture,
	work domain.MemoryExtractionWork,
	key string,
	wantRunID canonical.ID,
	wantAttempt int64,
) {
	t.Helper()
	ctx := context.Background()
	assertCOVR01QueuedCancellationOutcome(t, fixture, key, wantRunID, wantAttempt, 1)
	if err := fixture.application.cancelQueuedMemoryExtraction(ctx, work); err != nil {
		t.Fatalf("idempotent typed cancellation replay: %v", err)
	}
	assertCOVR01QueuedCancellationOutcome(t, fixture, key, wantRunID, wantAttempt, 1)
}

func assertCOVR01QueuedCancellationOutcome(
	t *testing.T,
	fixture applicationFixture,
	key string,
	wantRunID canonical.ID,
	wantAttempt int64,
	wantCancelled int,
) {
	t.Helper()
	var runRaw, state, errorClass string
	var attempt int64
	if err := fixture.store.Reader().QueryRow(`SELECT run.generation_run_id, outcome.state,
		COALESCE(outcome.error_class, ''), outcome.attempt_no
		FROM generation_runs run
		JOIN generation_run_outcomes outcome ON outcome.generation_run_id = run.generation_run_id
		WHERE run.resident_id = ? AND run.idempotency_key = ?
		ORDER BY outcome.outcome_id DESC LIMIT 1`, fixture.residentID.String(), key).Scan(
		&runRaw, &state, &errorClass, &attempt,
	); err != nil {
		t.Fatal(err)
	}
	if !wantRunID.IsZero() && runRaw != wantRunID.String() {
		t.Fatalf("typed cancellation run = %s, want %s", runRaw, wantRunID)
	}
	if state != "cancelled" || errorClass != string(generation.ErrorSourceContentErased) || attempt != wantAttempt {
		t.Fatalf("typed cancellation outcome = %s/%s attempt %d", state, errorClass, attempt)
	}
	var cancelled int
	if err := fixture.store.Reader().QueryRow(`SELECT COUNT(*) FROM generation_run_outcomes outcome
		JOIN generation_runs run ON run.generation_run_id = outcome.generation_run_id
		WHERE run.resident_id = ? AND run.idempotency_key = ?
		AND outcome.state = 'cancelled' AND outcome.error_class = ?`,
		fixture.residentID.String(), key, string(generation.ErrorSourceContentErased),
	).Scan(&cancelled); err != nil {
		t.Fatal(err)
	}
	if cancelled != wantCancelled {
		t.Fatalf("typed cancellation terminal rows = %d, want %d", cancelled, wantCancelled)
	}
}

func assertCOVR01ArchivedMemoryFailureOutcome(t *testing.T, fixture applicationFixture, key string) {
	t.Helper()
	var state, errorClass string
	var attempt, failed int64
	if err := fixture.store.Reader().QueryRow(`SELECT outcome.state, COALESCE(outcome.error_class, ''),
		outcome.attempt_no,
		(SELECT COUNT(*) FROM generation_run_outcomes other
		 WHERE other.generation_run_id = run.generation_run_id AND other.state = 'failed')
		FROM generation_runs run
		JOIN generation_run_outcomes outcome ON outcome.generation_run_id = run.generation_run_id
		WHERE run.resident_id = ? AND run.idempotency_key = ?
		ORDER BY outcome.outcome_id DESC LIMIT 1`, fixture.residentID.String(), key).Scan(
		&state, &errorClass, &attempt, &failed,
	); err != nil {
		t.Fatal(err)
	}
	if state != "cancelled" || errorClass != string(generation.ErrorResidentInactive) || attempt != 1 || failed != 0 {
		t.Fatalf("archived memory failure outcome = %s/%s attempt %d failed-rows %d",
			state, errorClass, attempt, failed)
	}
}
