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
	"mahoroba.local/mahoroba/internal/store/sqlite"
)

type backgroundRegistrationBarrier struct {
	entered chan struct{}
	release chan struct{}
	once    sync.Once
}

func newBackgroundRegistrationBarrier() *backgroundRegistrationBarrier {
	return &backgroundRegistrationBarrier{entered: make(chan struct{}), release: make(chan struct{})}
}

func (barrier *backgroundRegistrationBarrier) wait() {
	barrier.once.Do(func() {
		close(barrier.entered)
		<-barrier.release
	})
}

type fairMemoryDiscoveryBarrierRepository struct {
	*sqlite.CanonicalRepository
	entered chan struct{}
	release chan struct{}
	once    sync.Once
}

func (repository *fairMemoryDiscoveryBarrierRepository) DiscoverMemoryExtractionWork(
	ctx context.Context,
	residentID canonical.ID,
	request domain.MemoryDiscoveryRequest,
) (domain.MemoryDiscoveryResult, error) {
	result, err := repository.CanonicalRepository.DiscoverMemoryExtractionWork(ctx, residentID, request)
	if err != nil {
		return domain.MemoryDiscoveryResult{}, err
	}
	repository.once.Do(func() {
		close(repository.entered)
		<-repository.release
	})
	return result, nil
}

func TestM6MemoryBackgroundEpochClosesLostSignalForExtractionAlignmentAndPersona(t *testing.T) {
	for _, purpose := range []domain.GenerationPurpose{
		domain.GenerationPurposeMemoryExtraction,
		domain.GenerationPurposeMemoryAlignment,
		domain.GenerationPurposePersonaRevision,
	} {
		t.Run(string(purpose), func(t *testing.T) {
			ctx := context.Background()
			generator := &scriptedGenerator{steps: []generatorStep{{text: "foreground reply"}}}
			fixture := newApplicationFixture(t, generator, 1)
			cleanEpoch := fixture.application.captureForegroundEpoch(fixture.residentID)
			if !fixture.application.recordDialogueCleanAtEpoch(fixture.residentID, cleanEpoch) {
				t.Fatal("failed to seed completed clean-cycle proof")
			}

			leaseResult := make(chan backgroundCallLease, 1)
			providerCalls := make(chan struct{}, 1)
			registrationReady := make(chan uint64, 1)
			releaseRegistration := make(chan struct{})
			go func() {
				lock := fixture.application.residentLock(fixture.residentID)
				lock.Lock()
				defer lock.Unlock()
				epoch := fixture.application.captureForegroundEpoch(fixture.residentID)
				registrationReady <- epoch
				<-releaseRegistration
				providerCtx, finish, registered := fixture.application.beginBackgroundCallAtEpoch(
					ctx, fixture.residentID, epoch, true,
				)
				lease := backgroundCallLease{
					Context: providerCtx, residentID: fixture.residentID, epoch: epoch,
					finish: finish, registered: registered,
				}
				if lease.registered {
					providerCalls <- struct{}{}
					lease.finish()
				}
				leaseResult <- lease
			}()

			select {
			case captured := <-registrationReady:
				if captured != cleanEpoch {
					t.Fatalf("captured epoch = %d, want %d", captured, cleanEpoch)
				}
			case <-time.After(5 * time.Second):
				t.Fatal("background did not reach the registration barrier")
			}
			foreground, err := fixture.application.Ingress(ctx, "foreground committed in registration gap")
			if err != nil {
				t.Fatal(err)
			}
			foregroundDone := make(chan error, 1)
			go func() {
				lock := fixture.application.residentLock(fixture.residentID)
				lock.Lock()
				defer lock.Unlock()
				works, err := discoverDialogueWorkForTest(ctx, fixture.repository, fixture.residentID, 8, 1)
				if err != nil {
					foregroundDone <- err
					return
				}
				for index := range works {
					if works[index].UserEvent.ID == foreground.ID {
						foregroundDone <- fixture.application.processWork(ctx, fixture.residentID, works[index])
						return
					}
				}
				foregroundDone <- errors.New("foreground dialogue work was not discovered")
			}()
			select {
			case err := <-foregroundDone:
				t.Fatalf("foreground acquired resident lock before background yielded: %v", err)
			default:
			}

			close(releaseRegistration)
			var lease backgroundCallLease
			select {
			case lease = <-leaseResult:
			case <-time.After(5 * time.Second):
				t.Fatal("background registration did not reject the advanced foreground epoch")
			}
			if lease.registered || !errors.Is(context.Cause(lease.Context), errForegroundPreempted) {
				t.Fatalf("lost-signal lease = registered:%v cause:%v", lease.registered, context.Cause(lease.Context))
			}
			select {
			case <-providerCalls:
				t.Fatalf("%s provider ran after foreground committed", purpose)
			default:
			}
			select {
			case err := <-foregroundDone:
				if err != nil {
					t.Fatal(err)
				}
			case <-time.After(5 * time.Second):
				t.Fatal("foreground did not acquire the resident lock after background preemption")
			}
			requests := generator.Requests()
			if len(requests) != 1 || requests[0].Purpose != string(domain.GenerationPurposeDialogue) {
				t.Fatalf("provider requests after lost signal = %+v", requests)
			}
		})
	}
}

type alignmentLostSignalGenerator struct {
	mu              sync.Mutex
	extractionCalls int
	alignmentCalls  int
	personaCalls    int
}

func (generator *alignmentLostSignalGenerator) Stream(
	_ context.Context,
	request generation.Request,
	_ generation.DeltaSink,
) (generation.Result, error) {
	generator.mu.Lock()
	defer generator.mu.Unlock()
	switch request.Purpose {
	case string(domain.GenerationPurposeDialogue):
		return generation.Result{Text: "dialogue reply"}, nil
	case string(domain.GenerationPurposeMemoryExtraction):
		generator.extractionCalls++
		if generator.extractionCalls <= 3 {
			return generation.Result{Text: `{"claims":[{"grade":"stated","perspective":"resident","source_quote":"I am calm","statement":"The resident is calm.","subject":"resident","temporal_kind":"stable"}],"version":"memory-extraction-output-v1"}`}, nil
		}
		return generation.Result{Text: `{"claims":[{"grade":"stated","perspective":"source_actor","source_quote":"I see calm","statement":"The resident appears calm to the owner.","subject":"resident","temporal_kind":"stable"}],"version":"memory-extraction-output-v1"}`}, nil
	case string(domain.GenerationPurposeMemoryAlignment):
		generator.alignmentCalls++
		return generation.Result{Text: `{"aligned":true,"confidence":"800000","version":"memory-alignment-output-v1"}`}, nil
	case string(domain.GenerationPurposePersonaRevision):
		generator.personaCalls++
		return generation.Result{Text: `{"contradiction":false,"persona":"friendly"}`}, nil
	default:
		return generation.Result{}, errors.New("unexpected generation purpose")
	}
}

func (generator *alignmentLostSignalGenerator) ExtractionCalls() int {
	generator.mu.Lock()
	defer generator.mu.Unlock()
	return generator.extractionCalls
}

func (generator *alignmentLostSignalGenerator) AlignmentCalls() int {
	generator.mu.Lock()
	defer generator.mu.Unlock()
	return generator.alignmentCalls
}

func (generator *alignmentLostSignalGenerator) PersonaCalls() int {
	generator.mu.Lock()
	defer generator.mu.Unlock()
	return generator.personaCalls
}

func processForegroundDialogueUnderResidentLock(
	ctx context.Context,
	fixture applicationFixture,
	event domain.Event,
) error {
	lock := fixture.application.residentLock(fixture.residentID)
	lock.Lock()
	defer lock.Unlock()
	works, err := discoverDialogueWorkForTest(ctx, fixture.repository, fixture.residentID, 16, 1)
	if err != nil {
		return err
	}
	for index := range works {
		if works[index].UserEvent.ID == event.ID {
			return fixture.application.processWork(ctx, fixture.residentID, works[index])
		}
	}
	return errors.New("foreground dialogue work was not discovered")
}

func TestM6ExtractionLostForegroundSignalPersistsPreemptionWithoutRetryBudget(t *testing.T) {
	ctx := context.Background()
	generator := &alignmentLostSignalGenerator{}
	fixture := newApplicationFixture(t, generator, 1)
	if _, err := fixture.application.activateMemoryPolicyV4ForTest(ctx, fixture.residentID); err != nil {
		t.Fatal(err)
	}
	source, err := fixture.application.Ingress(ctx, "I am calm")
	if err != nil {
		t.Fatal(err)
	}
	if err := processForegroundDialogueUnderResidentLock(ctx, fixture, source); err != nil {
		t.Fatal(err)
	}
	works, err := discoverMemoryExtractionWorkForTest(ctx, fixture.repository, fixture.residentID, 16, 1)
	if err != nil || len(works) != 1 {
		t.Fatalf("extraction work = %+v, %v", works, err)
	}
	seedForegroundCleanProofForTest(t, fixture)
	barrier := newBackgroundRegistrationBarrier()
	fixture.application.backgroundRegistrationHook = barrier.wait
	backgroundDone := make(chan error, 1)
	go func() {
		lock := fixture.application.residentLock(fixture.residentID)
		lock.Lock()
		defer lock.Unlock()
		backgroundDone <- fixture.application.processMemoryExtractionWork(ctx, works[0])
	}()
	select {
	case <-barrier.entered:
	case err := <-backgroundDone:
		t.Fatalf("extraction stopped before provider registration: %v", err)
	case <-time.After(5 * time.Second):
		t.Fatal("extraction did not reach the provider-registration barrier")
	}
	foreground, err := fixture.application.Ingress(ctx, "foreground wins extraction registration")
	if err != nil {
		t.Fatal(err)
	}
	foregroundDone := make(chan error, 1)
	go func() { foregroundDone <- processForegroundDialogueUnderResidentLock(ctx, fixture, foreground) }()
	close(barrier.release)
	select {
	case err := <-backgroundDone:
		if !errors.Is(err, errMemoryForegroundPreempted) {
			t.Fatalf("extraction lost-signal result = %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("preempted extraction did not yield the resident lock")
	}
	select {
	case err := <-foregroundDone:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("foreground did not run after extraction preemption")
	}
	if calls := generator.ExtractionCalls(); calls != 0 {
		t.Fatalf("extraction provider calls after lost signal = %d, want 0", calls)
	}
	assertNoMemoryBackgroundRun(t, fixture, works[0].IdempotencyKey)
	retries, err := discoverMemoryExtractionWorkForTest(ctx, fixture.repository, fixture.residentID, 16, 1)
	if err != nil {
		t.Fatal(err)
	}
	var retry *domain.MemoryExtractionWork
	for index := range retries {
		if retries[index].IdempotencyKey == works[0].IdempotencyKey {
			retry = &retries[index]
			break
		}
	}
	if retry == nil || retry.State != domain.WorkPending || retry.RunID != nil ||
		retry.RetryCount != 0 || retry.ForegroundPreempted {
		t.Fatalf("extraction retry classification = %+v", retry)
	}
}

func TestCOVR01SelectionFenceRejectsMemoryLandingAlreadyPastProvider(t *testing.T) {
	ctx := context.Background()
	generator := &alignmentLostSignalGenerator{}
	fixture := newApplicationFixture(t, generator, 1)
	if _, err := fixture.application.activateMemoryPolicyV4ForTest(ctx, fixture.residentID); err != nil {
		t.Fatal(err)
	}
	source, err := fixture.application.Ingress(ctx, "I am calm")
	if err != nil {
		t.Fatal(err)
	}
	if err := processForegroundDialogueUnderResidentLock(ctx, fixture, source); err != nil {
		t.Fatal(err)
	}
	works, err := discoverMemoryExtractionWorkForTest(ctx, fixture.repository, fixture.residentID, 16, 1)
	if err != nil || len(works) != 1 {
		t.Fatalf("extraction work = %+v, %v", works, err)
	}
	seedForegroundCleanProofForTest(t, fixture)
	newResident := createCOVR01ActiveResident(t, fixture, "covr01-selection-final-landing")

	barrier := newBackgroundRegistrationBarrier()
	fixture.application.backgroundLandingHook = barrier.wait
	backgroundDone := make(chan error, 1)
	go func() {
		lock := fixture.application.residentLock(fixture.residentID)
		lock.Lock()
		defer lock.Unlock()
		backgroundDone <- fixture.application.processMemoryExtractionWork(ctx, works[0])
	}()
	select {
	case <-barrier.entered:
	case err := <-backgroundDone:
		t.Fatalf("memory extraction stopped before its final landing barrier: %v", err)
	case <-time.After(5 * time.Second):
		t.Fatal("memory extraction did not pass its provider before the landing barrier")
	}

	selectionDone := make(chan error, 1)
	go func() { selectionDone <- fixture.application.SelectResident(ctx, newResident) }()
	fenceDeadline := time.Now().Add(5 * time.Second)
	for {
		fixture.application.backgroundMu.Lock()
		blocked := fixture.application.backgroundBlocked[fixture.residentID]
		fixture.application.backgroundMu.Unlock()
		if blocked {
			break
		}
		if time.Now().After(fenceDeadline) {
			t.Fatal("selection did not close old-resident landing admission")
		}
		time.Sleep(time.Millisecond)
	}
	close(barrier.release)

	select {
	case err := <-backgroundDone:
		if !errors.Is(err, errMemoryForegroundPreempted) {
			t.Fatalf("memory landing after selection fence = %v, want foreground preemption", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("memory landing did not leave the selection boundary")
	}
	select {
	case err := <-selectionDone:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("selection did not complete after the memory landing boundary")
	}
	assertMemoryBackgroundPreemptionOutcome(t, fixture, works[0].IdempotencyKey)
}

func TestFairRunningExtractionPersistsPreemptionWhenIngressFollowsDiscovery(t *testing.T) {
	ctx := context.Background()
	generator := &alignmentLostSignalGenerator{}
	fixture := newApplicationFixture(t, generator, 1)
	if _, err := fixture.application.activateMemoryPolicyV4ForTest(ctx, fixture.residentID); err != nil {
		t.Fatal(err)
	}
	source, err := fixture.application.Ingress(ctx, "running fair extraction")
	if err != nil {
		t.Fatal(err)
	}
	if err := processForegroundDialogueUnderResidentLock(ctx, fixture, source); err != nil {
		t.Fatal(err)
	}
	works, err := discoverMemoryExtractionWorkForTest(ctx, fixture.repository, fixture.residentID, 16, 1)
	if err != nil || len(works) != 1 || works[0].State != domain.WorkPending {
		t.Fatalf("pending fair extraction = %+v, %v", works, err)
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
	running, err := discoverMemoryExtractionWorkForTest(ctx, fixture.repository, fixture.residentID, 16, 1)
	if err != nil || len(running) != 1 || running[0].State != domain.WorkRunning {
		t.Fatalf("running fair extraction = %+v, %v", running, err)
	}

	barrier := &fairMemoryDiscoveryBarrierRepository{
		CanonicalRepository: fixture.repository,
		entered:             make(chan struct{}),
		release:             make(chan struct{}),
	}
	fixture.application.repository = barrier
	backgroundDone := make(chan error, 1)
	go func() {
		lock := fixture.application.residentLock(fixture.residentID)
		lock.Lock()
		defer lock.Unlock()
		_, err := fixture.application.processFairMandatoryMemory(ctx, fixture.residentID, source.Seq)
		backgroundDone <- err
	}()
	select {
	case <-barrier.entered:
	case err := <-backgroundDone:
		t.Fatalf("fair extraction stopped before discovery barrier: %v", err)
	case <-time.After(5 * time.Second):
		t.Fatal("fair extraction did not reach discovery barrier")
	}
	if _, err := fixture.application.Ingress(ctx, "foreground after fair discovery"); err != nil {
		t.Fatal(err)
	}
	close(barrier.release)
	select {
	case err := <-backgroundDone:
		if !errors.Is(err, errMemoryForegroundPreempted) {
			t.Fatalf("fair running preemption result = %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("fair running extraction did not persist preemption")
	}
	if calls := generator.ExtractionCalls(); calls != 0 {
		t.Fatalf("fair extraction provider calls = %d, want 0", calls)
	}
	assertMemoryBackgroundPreemptionOutcome(t, fixture, running[0].IdempotencyKey)
}

func TestM6PersonaLostForegroundSignalPersistsPreemptionWithoutRetryBudget(t *testing.T) {
	ctx := context.Background()
	fixture, work := personaWorkForTerminalization(t, 1)
	generator := &alignmentLostSignalGenerator{}
	fixture.application.generator = generator
	seedForegroundCleanProofForTest(t, fixture)
	barrier := newBackgroundRegistrationBarrier()
	fixture.application.backgroundRegistrationHook = barrier.wait
	backgroundDone := make(chan error, 1)
	go func() {
		lock := fixture.application.residentLock(fixture.residentID)
		lock.Lock()
		defer lock.Unlock()
		_, err := fixture.application.processPersonaRevisionWork(ctx, work)
		backgroundDone <- err
	}()
	select {
	case <-barrier.entered:
	case <-time.After(5 * time.Second):
		t.Fatal("persona did not reach the provider-registration barrier")
	}
	foreground, err := fixture.application.Ingress(ctx, "foreground wins persona registration")
	if err != nil {
		t.Fatal(err)
	}
	foregroundDone := make(chan error, 1)
	go func() { foregroundDone <- processForegroundDialogueUnderResidentLock(ctx, fixture, foreground) }()
	close(barrier.release)
	select {
	case err := <-backgroundDone:
		if !errors.Is(err, errPersonaForegroundPreempted) {
			t.Fatalf("persona lost-signal result = %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("preempted persona did not yield the resident lock")
	}
	select {
	case err := <-foregroundDone:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("foreground did not run after persona preemption")
	}
	if calls := generator.PersonaCalls(); calls != 0 {
		t.Fatalf("persona provider calls after lost signal = %d, want 0", calls)
	}
	key := domain.PersonaRevisionObligation(work.TriggerStageTransitionID)
	assertNoMemoryBackgroundRun(t, fixture, key)
	retry, err := fixture.repository.DiscoverPersonaRevisionWork(ctx, fixture.residentID, 1)
	if err != nil || retry == nil {
		t.Fatalf("persona retry work = %+v, %v", retry, err)
	}
	if retry.State != domain.WorkPending || retry.RunID != nil ||
		retry.RetryCount != 0 || retry.ForegroundPreempted {
		t.Fatalf("persona retry classification = %+v", retry)
	}
}

func assertNoMemoryBackgroundRun(t *testing.T, fixture applicationFixture, key string) {
	t.Helper()
	var runs int
	if err := fixture.store.Reader().QueryRow(`SELECT COUNT(*) FROM generation_runs
		WHERE resident_id = ? AND idempotency_key = ?`, fixture.residentID.String(), key).Scan(&runs); err != nil {
		t.Fatal(err)
	}
	if runs != 0 {
		t.Fatalf("background run count after pre-Prepare boundary = %d, want 0", runs)
	}
}

func assertMemoryBackgroundPreemptionOutcome(t *testing.T, fixture applicationFixture, key string) {
	t.Helper()
	var attempt int64
	var state, code string
	if err := fixture.store.Reader().QueryRow(`SELECT outcome.attempt_no, outcome.state, outcome.error_class
		FROM generation_runs run JOIN generation_run_outcomes outcome ON outcome.generation_run_id = run.generation_run_id
		WHERE run.resident_id = ? AND run.idempotency_key = ? ORDER BY outcome.outcome_id DESC LIMIT 1`,
		fixture.residentID.String(), key).Scan(&attempt, &state, &code); err != nil {
		t.Fatal(err)
	}
	if attempt != 1 || state != "cancelled" || code != string(generation.ErrorForegroundPreempted) {
		t.Fatalf("background preemption outcome = %d/%s/%s", attempt, state, code)
	}
}

func TestM6AlignmentLostForegroundSignalPersistsPreemptionWithoutRetryBudget(t *testing.T) {
	ctx := context.Background()
	generator := &alignmentLostSignalGenerator{}
	fixture := newApplicationFixture(t, generator, 1)
	if _, err := fixture.application.activateMemoryPolicyV4ForTest(ctx, fixture.residentID); err != nil {
		t.Fatal(err)
	}
	for _, message := range []string{"I am calm", "I am calm", "I am calm", "I see calm", "I see calm"} {
		event, err := fixture.application.Ingress(ctx, message)
		if err != nil {
			t.Fatal(err)
		}
		dialogue, err := discoverDialogueWorkForTest(ctx, fixture.repository, fixture.residentID, 16, 1)
		if err != nil {
			t.Fatal(err)
		}
		processed := false
		for index := range dialogue {
			if dialogue[index].UserEvent.ID == event.ID {
				if err := fixture.application.processWork(ctx, fixture.residentID, dialogue[index]); err != nil {
					t.Fatal(err)
				}
				processed = true
				break
			}
		}
		if !processed {
			t.Fatal("dialogue work was not discovered")
		}
		extractions, err := discoverMemoryExtractionWorkForTest(ctx, fixture.repository, fixture.residentID, 32, 1)
		if err != nil {
			t.Fatal(err)
		}
		processed = false
		for index := range extractions {
			if extractions[index].SourceEvent.ID == event.ID {
				seedForegroundCleanProofForTest(t, fixture)
				if err := fixture.application.processMemoryExtractionWork(ctx, extractions[index]); err != nil {
					t.Fatal(err)
				}
				processed = true
				break
			}
		}
		if !processed {
			t.Fatal("memory extraction work was not discovered")
		}
	}

	work, err := fixture.repository.DiscoverMemoryAlignmentWork(
		ctx, fixture.residentID, domain.MaximumMemoryAlignmentPairs, 1,
	)
	if err != nil || work == nil {
		t.Fatalf("alignment work = %+v, %v", work, err)
	}
	seedForegroundCleanProofForTest(t, fixture)
	barrier := newBackgroundRegistrationBarrier()
	fixture.application.backgroundRegistrationHook = barrier.wait
	backgroundDone := make(chan error, 1)
	go func() {
		lock := fixture.application.residentLock(fixture.residentID)
		lock.Lock()
		defer lock.Unlock()
		backgroundDone <- fixture.application.processMemoryAlignmentWork(ctx, *work)
	}()
	select {
	case <-barrier.entered:
	case <-time.After(5 * time.Second):
		t.Fatal("alignment did not reach the provider-registration barrier")
	}
	foreground, err := fixture.application.Ingress(ctx, "foreground wins alignment registration")
	if err != nil {
		t.Fatal(err)
	}
	foregroundDone := make(chan error, 1)
	go func() {
		lock := fixture.application.residentLock(fixture.residentID)
		lock.Lock()
		defer lock.Unlock()
		works, err := discoverDialogueWorkForTest(ctx, fixture.repository, fixture.residentID, 16, 1)
		if err != nil {
			foregroundDone <- err
			return
		}
		for index := range works {
			if works[index].UserEvent.ID == foreground.ID {
				foregroundDone <- fixture.application.processWork(ctx, fixture.residentID, works[index])
				return
			}
		}
		foregroundDone <- errors.New("foreground dialogue work was not discovered")
	}()
	close(barrier.release)
	select {
	case err := <-backgroundDone:
		if !errors.Is(err, errMemoryAlignmentForegroundPreempted) {
			t.Fatalf("alignment lost-signal result = %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("preempted alignment did not yield the resident lock")
	}
	select {
	case err := <-foregroundDone:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("foreground did not run after alignment preemption")
	}
	if calls := generator.AlignmentCalls(); calls != 0 {
		t.Fatalf("alignment provider calls after lost signal = %d, want 0", calls)
	}
	assertNoMemoryBackgroundRun(t, fixture, work.IdempotencyKey)
	retry, err := fixture.repository.DiscoverMemoryAlignmentWork(
		ctx, fixture.residentID, domain.MaximumMemoryAlignmentPairs, 1,
	)
	if err != nil || retry == nil {
		t.Fatalf("alignment retry work = %+v, %v", retry, err)
	}
	if retry.State != domain.WorkPending || retry.RunID != nil ||
		retry.RetryCount != 0 || retry.ForegroundPreempted {
		t.Fatalf("alignment retry classification = %+v", retry)
	}
}

func seedForegroundCleanProofForTest(t *testing.T, fixture applicationFixture) {
	t.Helper()
	epoch := fixture.application.captureForegroundEpoch(fixture.residentID)
	if !fixture.application.recordDialogueCleanAtEpoch(fixture.residentID, epoch) {
		t.Fatal("failed to seed foreground clean proof")
	}
}
