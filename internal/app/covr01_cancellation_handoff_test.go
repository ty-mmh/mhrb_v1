package app

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"testing"
	"time"

	"mahoroba.local/mahoroba/internal/canonical"
	"mahoroba.local/mahoroba/internal/domain"
)

const covr01EmptyExtraction = `{"claims":[],"version":"memory-extraction-output-v1"}`

type covr01SelectionBarrierRepository struct {
	domain.Repository
	entered chan struct{}
	release chan struct{}
}

func (repository *covr01SelectionBarrierRepository) SelectActiveResident(
	ctx context.Context,
	residentID canonical.ID,
	at canonical.Instant,
	timezone canonical.Timezone,
) error {
	close(repository.entered)
	<-repository.release
	return repository.Repository.SelectActiveResident(ctx, residentID, at, timezone)
}

func TestCOVR01ErasedDialogueCancellationHandsOffMemoryBeforeContinuousDialogue(t *testing.T) {
	ctx := context.Background()
	generator := &scriptedGenerator{steps: []generatorStep{
		{text: "newer dialogue one"}, {text: covr01EmptyExtraction},
		{text: "newer dialogue two"}, {text: covr01EmptyExtraction},
	}}
	fixture := newApplicationFixture(t, generator, 1)
	if _, err := fixture.application.activateMemoryPolicyV4ForTest(ctx, fixture.residentID); err != nil {
		t.Fatal(err)
	}

	erased := ingressPendingForTest(t, fixture, "erase before dialogue cancellation")
	newerOne := ingressPendingForTest(t, fixture, "newer dialogue one")
	_ = ingressPendingForTest(t, fixture, "newer dialogue two")
	eraseEventContentForTest(t, fixture.store.Path(), erased.ContentID)

	if err := fixture.application.ProcessResident(ctx, fixture.residentID); err != nil {
		t.Fatal(err)
	}

	dialogueCommit := assertCOVR01CancelledRun(
		t, fixture, domain.DialogueObligation(erased.ID), "dialogue", "source_content_erased",
	)
	memoryCommit := assertCOVR01CancelledRun(
		t, fixture, domain.MemoryExtractionObligation(erased.ID), "memory_extraction", "source_content_erased",
	)
	if runs := covr01RunCount(t, fixture, domain.DialogueObligation(newerOne.ID)); runs != 0 {
		t.Fatalf("newer dialogue runs after cancellation handoff = %d, want 0", runs)
	}
	if generator.CallCount() != 0 {
		t.Fatalf("provider calls in cancellation handoff turn = %d, want 0", generator.CallCount())
	}

	// Each later turn may process one dialogue and its one fair-memory quantum.
	for pass := 0; pass < 2; pass++ {
		if err := fixture.application.ProcessResident(ctx, fixture.residentID); err != nil {
			t.Fatal(err)
		}
	}
	newerDialogueCommit := covr01RunCommitSeq(t, fixture, domain.DialogueObligation(newerOne.ID))
	if !(dialogueCommit < memoryCommit && memoryCommit < newerDialogueCommit) {
		t.Fatalf(
			"durable handoff order dialogue-cancel/memory-cancel/next-dialogue = %d/%d/%d",
			dialogueCommit, memoryCommit, newerDialogueCommit,
		)
	}

	requests := generator.Requests()
	wantPurposes := []string{"dialogue", "memory_extraction", "dialogue", "memory_extraction"}
	if len(requests) != len(wantPurposes) {
		t.Fatalf("provider requests = %d, want %d", len(requests), len(wantPurposes))
	}
	for index, want := range wantPurposes {
		if requests[index].Purpose != want {
			t.Fatalf("provider request[%d].purpose = %q, want %q", index, requests[index].Purpose, want)
		}
	}

	beforeDialogue := covr01OutcomeCount(t, fixture, domain.DialogueObligation(erased.ID))
	beforeMemory := covr01OutcomeCount(t, fixture, domain.MemoryExtractionObligation(erased.ID))
	if err := fixture.application.ProcessResident(ctx, fixture.residentID); err != nil {
		t.Fatal(err)
	}
	if after := covr01OutcomeCount(t, fixture, domain.DialogueObligation(erased.ID)); after != beforeDialogue {
		t.Fatalf("dialogue cancellation outcomes after replay = %d, want %d", after, beforeDialogue)
	}
	if after := covr01OutcomeCount(t, fixture, domain.MemoryExtractionObligation(erased.ID)); after != beforeMemory {
		t.Fatalf("memory cancellation outcomes after replay = %d, want %d", after, beforeMemory)
	}
}

func TestCOVR0102DialogueBacklogAllowsOneFairMemoryProviderQuantumPerResidentTurn(t *testing.T) {
	ctx := context.Background()
	generator := &scriptedGenerator{steps: []generatorStep{
		{text: "first dialogue"}, {text: covr01EmptyExtraction},
		{text: "second dialogue"}, {text: covr01EmptyExtraction},
	}}
	fixture := newApplicationFixture(t, generator, 1)
	if _, err := fixture.application.activateMemoryPolicyV4ForTest(ctx, fixture.residentID); err != nil {
		t.Fatal(err)
	}
	first := ingressPendingForTest(t, fixture, "first queued dialogue")
	second := ingressPendingForTest(t, fixture, "second queued dialogue")

	if err := fixture.application.ProcessResident(ctx, fixture.residentID); err != nil {
		t.Fatal(err)
	}
	requests := generator.Requests()
	if len(requests) != 2 || requests[0].Purpose != "dialogue" || requests[1].Purpose != "memory_extraction" {
		t.Fatalf("first resident turn provider requests = %+v, want dialogue then one memory_extraction", requests)
	}
	if state := covr01LatestState(t, fixture, domain.MemoryExtractionObligation(first.ID)); state != "succeeded" {
		t.Fatalf("first fair-memory state = %q, want succeeded", state)
	}
	if runs := covr01RunCount(t, fixture, domain.DialogueObligation(second.ID)); runs != 0 {
		t.Fatalf("second dialogue runs after first resident turn = %d, want 0", runs)
	}
	if runs := covr01RunCount(t, fixture, domain.MemoryExtractionObligation(second.ID)); runs != 0 {
		t.Fatalf("second fair-memory runs after first resident turn = %d, want 0", runs)
	}

	if err := fixture.application.ProcessResident(ctx, fixture.residentID); err != nil {
		t.Fatal(err)
	}
	requests = generator.Requests()
	if len(requests) != 4 || requests[2].Purpose != "dialogue" || requests[3].Purpose != "memory_extraction" {
		t.Fatalf("second resident turn provider requests = %+v, want one additional dialogue/memory pair", requests)
	}
	if state := covr01LatestState(t, fixture, domain.MemoryExtractionObligation(second.ID)); state != "succeeded" {
		t.Fatalf("second fair-memory state = %q, want succeeded", state)
	}
}

func TestCOVR01InactiveDialogueCancellationHandsOffMemoryExactlyOnceWithoutProvider(t *testing.T) {
	ctx := context.Background()
	generator := &scriptedGenerator{}
	fixture := newApplicationFixture(t, generator, 1)
	if _, err := fixture.application.activateMemoryPolicyV4ForTest(ctx, fixture.residentID); err != nil {
		t.Fatal(err)
	}
	event := ingressPendingForTest(t, fixture, "archive before mandatory work")

	if err := fixture.application.ArchiveResident(ctx, fixture.residentID); err != nil {
		t.Fatal(err)
	}
	if generator.CallCount() != 0 {
		t.Fatalf("inactive resident provider calls = %d, want 0", generator.CallCount())
	}
	dialogueCommit := assertCOVR01CancelledRun(
		t, fixture, domain.DialogueObligation(event.ID), "dialogue", "resident_inactive",
	)
	memoryCommit := assertCOVR01CancelledRun(
		t, fixture, domain.MemoryExtractionObligation(event.ID), "memory_extraction", "resident_inactive",
	)
	if dialogueCommit >= memoryCommit {
		t.Fatalf("inactive cancellation commit order dialogue/memory = %d/%d", dialogueCommit, memoryCommit)
	}

	beforeDialogue := covr01OutcomeCount(t, fixture, domain.DialogueObligation(event.ID))
	beforeMemory := covr01OutcomeCount(t, fixture, domain.MemoryExtractionObligation(event.ID))
	if err := fixture.application.ProcessResident(ctx, fixture.residentID); err != nil {
		t.Fatal(err)
	}
	if after := covr01OutcomeCount(t, fixture, domain.DialogueObligation(event.ID)); after != beforeDialogue {
		t.Fatalf("inactive dialogue cancellation outcomes after replay = %d, want %d", after, beforeDialogue)
	}
	if after := covr01OutcomeCount(t, fixture, domain.MemoryExtractionObligation(event.ID)); after != beforeMemory {
		t.Fatalf("inactive memory cancellation outcomes after replay = %d, want %d", after, beforeMemory)
	}
}

func TestCOVR01UnselectedMemoryRemainsPendingUntilResidentIsReselected(t *testing.T) {
	ctx := context.Background()
	generator := &scriptedGenerator{steps: []generatorStep{{text: covr01EmptyExtraction}}}
	fixture := newApplicationFixture(t, generator, 1)
	if _, err := fixture.application.activateMemoryPolicyV4ForTest(ctx, fixture.residentID); err != nil {
		t.Fatal(err)
	}
	event := ingressPendingForTest(t, fixture, "retain memory while unselected")
	selected := createCOVR01ActiveResident(t, fixture, "covr01-selected-2")
	if err := fixture.application.SelectResident(ctx, selected); err != nil {
		t.Fatal(err)
	}

	// The first pass terminalizes dialogue as resident_unselected and yields at
	// its fair-memory boundary. A second pass exercises the ordinary queue: it
	// may advance only Operational cursor state, never provider/cancellation.
	for pass := 0; pass < 2; pass++ {
		if err := fixture.application.ProcessResident(ctx, fixture.residentID); err != nil {
			t.Fatal(err)
		}
	}
	assertCOVR01CancelledRun(
		t, fixture, domain.DialogueObligation(event.ID), "dialogue", "resident_unselected",
	)
	if generator.CallCount() != 0 {
		t.Fatalf("unselected memory provider calls = %d, want 0", generator.CallCount())
	}
	if runs := covr01RunCount(t, fixture, domain.MemoryExtractionObligation(event.ID)); runs != 0 {
		t.Fatalf("unselected memory runs = %d, want 0", runs)
	}

	if err := fixture.application.SelectResident(ctx, fixture.residentID); err != nil {
		t.Fatal(err)
	}
	pumpCOVR01ResidentUntil(t, fixture, 8, func() bool {
		return covr01LatestState(t, fixture, domain.MemoryExtractionObligation(event.ID)) == "succeeded"
	})
	if generator.CallCount() != 1 {
		t.Fatalf("reselected memory provider calls = %d, want 1", generator.CallCount())
	}
	requests := generator.Requests()
	if len(requests) != 1 || requests[0].Purpose != "memory_extraction" {
		t.Fatalf("reselected provider requests = %+v, want one memory_extraction", requests)
	}
	if outcomes := covr01OutcomeCount(t, fixture, domain.DialogueObligation(event.ID)); outcomes != 1 {
		t.Fatalf("reselected dialogue cancellation outcomes = %d, want unchanged 1", outcomes)
	}
}

func TestCOVR01SelectionInvalidatesOldAndNewResidentMemoryState(t *testing.T) {
	ctx := context.Background()
	fixture := newApplicationFixture(t, &scriptedGenerator{}, 1)
	newResident := createCOVR01ActiveResident(t, fixture, "covr01-epoch-2")

	oldEpoch := fixture.application.captureForegroundEpoch(fixture.residentID)
	newEpoch := fixture.application.captureForegroundEpoch(newResident)
	oldLease := fixture.application.beginFairBackgroundCallAtEpoch(ctx, fixture.residentID, oldEpoch)
	newLease := fixture.application.beginFairBackgroundCallAtEpoch(ctx, newResident, newEpoch)
	defer oldLease.finish()
	defer newLease.finish()
	if !oldLease.registered || !newLease.registered {
		t.Fatal("test could not register both pre-selection fair-memory leases")
	}

	fixture.application.memoryStateMu.Lock()
	for _, residentID := range []canonical.ID{fixture.residentID, newResident} {
		fixture.application.memoryDiscoveryCursors[residentID] = &domain.MemoryDiscoveryCursor{}
		fixture.application.normalMemoryDiscoveryCursors[residentID] = &domain.MemoryDiscoveryCursor{}
		fixture.application.memoryNormalCycleEpochs[residentID] = 99
		fixture.application.memoryReextractionCursors[residentID] = &domain.MemoryReextractionDiscoveryCursor{}
		fixture.application.memoryReextractionCycles[residentID] = true
		fixture.application.memoryReextractionCycleEpochs[residentID] = 99
		fixture.application.memoryReextractionRescanPending[residentID] = true
		fixture.application.memoryNormalCyclesDirty[residentID] = true
		fixture.application.memoryReextractionCyclesDirty[residentID] = true
		fixture.application.memoryNormalCompleteEpochs[residentID] = 99
		fixture.application.memoryExtractionScansComplete[residentID] = true
	}
	fixture.application.memoryStateMu.Unlock()

	if err := fixture.application.SelectResident(ctx, newResident); err != nil {
		t.Fatal(err)
	}
	for label, lease := range map[string]backgroundCallLease{"old": oldLease, "new": newLease} {
		if !errors.Is(context.Cause(lease.Context), errForegroundPreempted) {
			t.Fatalf("%s resident lease cause = %v, want foreground preemption", label, context.Cause(lease.Context))
		}
	}
	if got := fixture.application.captureForegroundEpoch(fixture.residentID); got != oldEpoch+1 {
		t.Fatalf("old resident epoch = %d, want %d", got, oldEpoch+1)
	}
	if got := fixture.application.captureForegroundEpoch(newResident); got != newEpoch+1 {
		t.Fatalf("new resident epoch = %d, want %d", got, newEpoch+1)
	}

	fixture.application.memoryStateMu.Lock()
	defer fixture.application.memoryStateMu.Unlock()
	for _, residentID := range []canonical.ID{fixture.residentID, newResident} {
		if fixture.application.memoryDiscoveryCursors[residentID] != nil ||
			fixture.application.normalMemoryDiscoveryCursors[residentID] != nil ||
			fixture.application.memoryReextractionCursors[residentID] != nil ||
			fixture.application.memoryReextractionCycles[residentID] ||
			fixture.application.memoryReextractionRescanPending[residentID] ||
			fixture.application.memoryNormalCyclesDirty[residentID] ||
			fixture.application.memoryReextractionCyclesDirty[residentID] ||
			fixture.application.memoryExtractionScansComplete[residentID] {
			t.Fatalf("selection retained Operational memory state for resident %s", residentID)
		}
		if _, ok := fixture.application.memoryNormalCompleteEpochs[residentID]; ok {
			t.Fatalf("selection retained normal completion epoch for resident %s", residentID)
		}
		if _, ok := fixture.application.memoryNormalCycleEpochs[residentID]; ok {
			t.Fatalf("selection retained normal cycle epoch for resident %s", residentID)
		}
		if _, ok := fixture.application.memoryReextractionCycleEpochs[residentID]; ok {
			t.Fatalf("selection retained re-extraction cycle epoch for resident %s", residentID)
		}
	}
}

func TestCOVR01SelectionCommitBlocksConcurrentBackgroundRegistration(t *testing.T) {
	ctx := context.Background()
	fixture := newApplicationFixture(t, &scriptedGenerator{}, 1)
	newResident := createCOVR01ActiveResident(t, fixture, "covr01-selection-registration")
	expected := fixture.application.captureForegroundEpoch(newResident)
	if !fixture.application.recordDialogueCleanAtEpoch(newResident, expected) {
		t.Fatal("failed to seed target resident clean proof")
	}

	barrier := &covr01SelectionBarrierRepository{
		Repository: fixture.application.repository,
		entered:    make(chan struct{}),
		release:    make(chan struct{}),
	}
	fixture.application.repository = barrier
	selectionDone := make(chan error, 1)
	go func() { selectionDone <- fixture.application.SelectResident(ctx, newResident) }()
	select {
	case <-barrier.entered:
	case <-time.After(5 * time.Second):
		t.Fatal("selection did not reach its durable transaction barrier")
	}

	registrationEntered := make(chan struct{})
	fixture.application.backgroundRegistrationHook = func() { close(registrationEntered) }
	leaseDone := make(chan backgroundCallLease, 1)
	go func() { leaseDone <- fixture.application.beginFairBackgroundCallAtEpoch(ctx, newResident, expected) }()
	select {
	case <-registrationEntered:
	case <-time.After(5 * time.Second):
		t.Fatal("background registration did not reach the selection boundary")
	}
	select {
	case lease := <-leaseDone:
		if lease.registered {
			lease.finish()
			t.Fatal("background registration crossed an in-flight selection transaction")
		}
		if !errors.Is(context.Cause(lease.Context), errForegroundPreempted) {
			t.Fatalf("rejected registration cause = %v, want foreground preemption", context.Cause(lease.Context))
		}
	case <-time.After(5 * time.Second):
		t.Fatal("background registration was not rejected by the in-flight selection fence")
	}

	close(barrier.release)
	select {
	case err := <-selectionDone:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("selection did not complete after releasing its transaction barrier")
	}
}

func TestCOVR01SelectionWaitsForOldResidentLandingBoundary(t *testing.T) {
	ctx := context.Background()
	fixture := newApplicationFixture(t, &scriptedGenerator{}, 1)
	newResident := createCOVR01ActiveResident(t, fixture, "covr01-selection-landing")
	barrier := &covr01SelectionBarrierRepository{
		Repository: fixture.application.repository,
		entered:    make(chan struct{}),
		release:    make(chan struct{}),
	}
	fixture.application.repository = barrier

	oldResidentLanding := fixture.application.backgroundLandingLock(fixture.residentID)
	oldResidentLanding.Lock()
	locked := true
	released := false
	defer func() {
		if locked {
			oldResidentLanding.Unlock()
		}
		if !released {
			close(barrier.release)
		}
	}()

	selectionDone := make(chan error, 1)
	go func() { selectionDone <- fixture.application.SelectResident(ctx, newResident) }()
	select {
	case <-barrier.entered:
		t.Fatal("selection transaction started before the old resident landing boundary drained")
	case <-time.After(100 * time.Millisecond):
	}

	oldResidentLanding.Unlock()
	locked = false
	select {
	case <-barrier.entered:
	case <-time.After(5 * time.Second):
		t.Fatal("selection transaction did not start after the old resident landing boundary drained")
	}
	close(barrier.release)
	released = true
	select {
	case err := <-selectionDone:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("selection did not complete after the landing boundary drained")
	}
}

func TestCOVR01ArchiveCommitRejectsConcurrentBackgroundRegistration(t *testing.T) {
	ctx := context.Background()
	fixture := newApplicationFixture(t, &scriptedGenerator{}, 1)
	seedForegroundCleanProofForTest(t, fixture)
	barrier := newBackgroundRegistrationBarrier()
	fixture.application.backgroundRegistrationHook = barrier.wait

	leaseDone := make(chan backgroundCallLease, 1)
	go func() { leaseDone <- fixture.application.beginBackgroundCall(ctx, fixture.residentID) }()
	select {
	case <-barrier.entered:
	case <-time.After(5 * time.Second):
		t.Fatal("background registration did not reach the archive boundary")
	}

	archiveDone := make(chan error, 1)
	go func() { archiveDone <- fixture.application.ArchiveResident(ctx, fixture.residentID) }()
	select {
	case err := <-archiveDone:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("archive did not complete while the pre-registration call was paused")
	}
	close(barrier.release)

	select {
	case lease := <-leaseDone:
		if lease.registered {
			lease.finish()
			t.Fatal("background call registered after the resident archive committed")
		}
		if !errors.Is(context.Cause(lease.Context), errForegroundPreempted) {
			t.Fatalf("archive-rejected registration cause = %v, want foreground preemption", context.Cause(lease.Context))
		}
	case <-time.After(5 * time.Second):
		t.Fatal("background registration remained blocked after archive")
	}
}

func createCOVR01ActiveResident(t *testing.T, fixture applicationFixture, seed string) canonical.ID {
	t.Helper()
	ctx := context.Background()
	state, err := fixture.application.BootstrapInit(ctx, BootstrapInput{
		OwnerName: "Owner", Name: "COVR-01 " + seed, SeedKey: seed, Principles: "preserve mandatory work",
	})
	if err != nil {
		t.Fatal(err)
	}
	var residentID canonical.ID
	for _, resident := range state.Residents {
		if resident.SeedKey == seed {
			residentID = resident.ResidentID
			break
		}
	}
	if residentID.IsZero() {
		t.Fatalf("COVR-01 resident %q was not created", seed)
	}
	if err := fixture.application.ApprovePrinciples(ctx, residentID); err != nil {
		t.Fatal(err)
	}
	if err := fixture.application.FinalizeBootstrap(
		ctx, residentID, "patient",
		`{"mandatory_event_types":[],"memory_recall_enabled":false,"version":"memory-policy-v1"}`,
	); err != nil {
		t.Fatal(err)
	}
	return residentID
}

func pumpCOVR01ResidentUntil(
	t *testing.T,
	fixture applicationFixture,
	limit int,
	done func() bool,
) {
	t.Helper()
	ctx := context.Background()
	for pass := 0; pass < limit; pass++ {
		if done() {
			return
		}
		if err := fixture.application.ProcessResident(ctx, fixture.residentID); err != nil {
			t.Fatal(err)
		}
	}
	if !done() {
		t.Fatalf("resident did not reach the requested durable state in %d bounded passes", limit)
	}
}

func assertCOVR01CancelledRun(
	t *testing.T,
	fixture applicationFixture,
	key, wantPurpose, wantCode string,
) int64 {
	t.Helper()
	var purpose, state, code string
	var commitSeq int64
	var outcomes int
	err := fixture.store.Reader().QueryRow(`SELECT run.purpose, run_commit.commit_seq,
		latest.state, COALESCE(latest.error_class, ''),
		(SELECT COUNT(*) FROM generation_run_outcomes counted
		 WHERE counted.generation_run_id = run.generation_run_id)
		FROM generation_runs run
		JOIN canonical_commits run_commit ON run_commit.canonical_commit_id = run.canonical_commit_id
		JOIN generation_run_outcomes latest ON latest.generation_run_id = run.generation_run_id
		JOIN canonical_commits outcome_commit ON outcome_commit.canonical_commit_id = latest.canonical_commit_id
		WHERE run.resident_id = ? AND run.idempotency_key = ?
		ORDER BY outcome_commit.commit_seq DESC LIMIT 1`, fixture.residentID.String(), key).Scan(
		&purpose, &commitSeq, &state, &code, &outcomes,
	)
	if err != nil {
		t.Fatal(err)
	}
	if purpose != wantPurpose || state != "cancelled" || code != wantCode || outcomes != 1 {
		t.Fatalf(
			"cancelled run %q = purpose=%q state=%q code=%q outcomes=%d, want %q/cancelled/%q/1",
			key, purpose, state, code, outcomes, wantPurpose, wantCode,
		)
	}
	return commitSeq
}

func covr01RunCommitSeq(t *testing.T, fixture applicationFixture, key string) int64 {
	t.Helper()
	var commitSeq int64
	if err := fixture.store.Reader().QueryRow(`SELECT canonical_commit.commit_seq
		FROM generation_runs run
		JOIN canonical_commits canonical_commit ON canonical_commit.canonical_commit_id = run.canonical_commit_id
		WHERE run.resident_id = ? AND run.idempotency_key = ?`, fixture.residentID.String(), key).Scan(&commitSeq); err != nil {
		t.Fatal(err)
	}
	return commitSeq
}

func covr01RunCount(t *testing.T, fixture applicationFixture, key string) int {
	t.Helper()
	var count int
	if err := fixture.store.Reader().QueryRow(`SELECT COUNT(*) FROM generation_runs
		WHERE resident_id = ? AND idempotency_key = ?`, fixture.residentID.String(), key).Scan(&count); err != nil {
		t.Fatal(err)
	}
	return count
}

func covr01OutcomeCount(t *testing.T, fixture applicationFixture, key string) int {
	t.Helper()
	var count int
	if err := fixture.store.Reader().QueryRow(`SELECT COUNT(*)
		FROM generation_runs run
		JOIN generation_run_outcomes outcome ON outcome.generation_run_id = run.generation_run_id
		WHERE run.resident_id = ? AND run.idempotency_key = ?`, fixture.residentID.String(), key).Scan(&count); err != nil {
		t.Fatal(err)
	}
	return count
}

func covr01LatestState(t *testing.T, fixture applicationFixture, key string) string {
	t.Helper()
	var state string
	err := fixture.store.Reader().QueryRow(`SELECT outcome.state
		FROM generation_runs run
		JOIN generation_run_outcomes outcome ON outcome.generation_run_id = run.generation_run_id
		JOIN canonical_commits canonical_commit ON canonical_commit.canonical_commit_id = outcome.canonical_commit_id
		WHERE run.resident_id = ? AND run.idempotency_key = ?
		ORDER BY canonical_commit.commit_seq DESC LIMIT 1`, fixture.residentID.String(), key).Scan(&state)
	if errors.Is(err, sql.ErrNoRows) {
		return ""
	}
	if err != nil {
		t.Fatal(fmt.Errorf("read latest state for %q: %w", key, err))
	}
	return state
}
