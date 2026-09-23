package app

import (
	"context"
	"errors"
	"sync"
	"testing"

	"mahoroba.local/mahoroba/internal/autonomy"
	"mahoroba.local/mahoroba/internal/canonical"
	"mahoroba.local/mahoroba/internal/domain"
)

func TestForegroundCleanProofLifecycleAndNormalRegistration(t *testing.T) {
	residentID := foregroundCleanTestID(t)
	application := &Application{
		background:         make(map[canonical.ID]*backgroundCall),
		foregroundEpoch:    make(map[canonical.ID]uint64),
		dialogueCleanEpoch: make(map[canonical.ID]uint64),
	}

	if application.currentForegroundClean(residentID) {
		t.Fatal("fresh process reported foreground clean without a completed cycle")
	}
	lease := application.beginBackgroundCall(context.Background(), residentID)
	if lease.registered || !errors.Is(context.Cause(lease.Context), errForegroundPreempted) {
		t.Fatalf("background without proof = registered:%v cause:%v", lease.registered, context.Cause(lease.Context))
	}

	epoch := application.captureForegroundEpoch(residentID)
	if !application.recordDialogueCleanAtEpoch(residentID, epoch) {
		t.Fatal("stable dialogue cycle did not publish a clean proof")
	}
	lease = application.beginBackgroundCall(context.Background(), residentID)
	if !lease.registered {
		t.Fatalf("background with matching proof was rejected: %v", context.Cause(lease.Context))
	}
	application.preemptBackground(residentID)
	if !errors.Is(context.Cause(lease.Context), errForegroundPreempted) {
		t.Fatalf("registered background cause after ingress = %v", context.Cause(lease.Context))
	}
	lease.finish()

	if application.currentForegroundClean(residentID) {
		t.Fatal("foreground ingress did not invalidate the prior clean proof")
	}
	if application.recordDialogueCleanAtEpoch(residentID, epoch) {
		t.Fatal("stale dialogue cycle published a clean proof")
	}
	lease = application.beginBackgroundCall(context.Background(), residentID)
	if lease.registered {
		lease.finish()
		t.Fatal("background registered against an invalidated proof")
	}

	current := application.captureForegroundEpoch(residentID)
	if !application.recordDialogueCleanAtEpoch(residentID, current) {
		t.Fatal("new stable cycle did not publish the current clean proof")
	}
	lease = application.beginBackgroundCall(context.Background(), residentID)
	if !lease.registered {
		t.Fatalf("background after renewed proof was rejected: %v", context.Cause(lease.Context))
	}
	lease.finish()

	restarted := &Application{
		background:         make(map[canonical.ID]*backgroundCall),
		foregroundEpoch:    make(map[canonical.ID]uint64),
		dialogueCleanEpoch: make(map[canonical.ID]uint64),
	}
	if restarted.currentForegroundClean(residentID) {
		t.Fatal("restart inherited a process-local clean proof")
	}
}

func TestDialogueScanCycleKeepsOriginalEpochAcrossIngress(t *testing.T) {
	residentID := foregroundCleanTestID(t)
	application := &Application{
		background:         make(map[canonical.ID]*backgroundCall),
		foregroundEpoch:    make(map[canonical.ID]uint64),
		dialogueCleanEpoch: make(map[canonical.ID]uint64),
		dialogueScanCycles: make(map[canonical.ID]dialogueScanCycle),
	}

	cycle := application.startOrResumeDialogueScanCycle(residentID)
	beforeSeq, err := canonical.NewSeq(42)
	if err != nil {
		t.Fatal(err)
	}
	cycle.Cursor = &domain.DialogueDiscoveryCursor{BeforeSeq: beforeSeq}
	application.saveDialogueScanCycle(residentID, cycle)
	application.preemptBackground(residentID)

	resumed := application.startOrResumeDialogueScanCycle(residentID)
	if resumed.Epoch != cycle.Epoch || resumed.Cursor == nil || resumed.Cursor.BeforeSeq != beforeSeq {
		t.Fatalf("resumed cycle = %+v, want epoch %d / cursor %d", resumed, cycle.Epoch, beforeSeq)
	}
	application.finishDialogueScanCycle(residentID, resumed.Epoch)
	if application.recordDialogueCleanAtEpoch(residentID, resumed.Epoch) {
		t.Fatal("cycle spanning foreground ingress published a clean proof")
	}

	fresh := application.startOrResumeDialogueScanCycle(residentID)
	if fresh.Epoch == cycle.Epoch || fresh.Cursor != nil {
		t.Fatalf("fresh cycle = %+v, old epoch = %d", fresh, cycle.Epoch)
	}
}

func TestBeginFairBackgroundCallAtEpochRejectsIngressBetweenCheckAndRegistration(t *testing.T) {
	residentID := foregroundCleanTestID(t)
	application := &Application{
		background:      make(map[canonical.ID]*backgroundCall),
		foregroundEpoch: make(map[canonical.ID]uint64),
	}
	expected := application.captureForegroundEpoch(residentID)
	if application.foregroundAdvanced(residentID, expected) {
		t.Fatal("captured epoch was unexpectedly stale")
	}

	// This is the former check-register window: ingress commits after the
	// caller's check but before fair registration.
	application.preemptBackground(residentID)
	lease := application.beginFairBackgroundCallAtEpoch(context.Background(), residentID, expected)
	if lease.registered || !errors.Is(context.Cause(lease.Context), errForegroundPreempted) {
		t.Fatalf("stale fair lease = registered:%v cause:%v", lease.registered, context.Cause(lease.Context))
	}
}

type wrapMemoryDiscoveryRepository struct {
	domain.Repository
	requests []domain.MemoryDiscoveryRequest
	work     domain.MemoryExtractionWork
}

func (repository *wrapMemoryDiscoveryRepository) Resident(
	context.Context,
	canonical.ID,
) (domain.ResidentSnapshot, error) {
	return domain.ResidentSnapshot{
		ResidentID: repository.work.SourceEvent.ResidentID,
		Status:     "active",
	}, nil
}

func (repository *wrapMemoryDiscoveryRepository) ActiveResident(context.Context) (domain.ResidentSnapshot, error) {
	return domain.ResidentSnapshot{
		ResidentID: repository.work.SourceEvent.ResidentID,
		Status:     "active",
	}, nil
}

func (repository *wrapMemoryDiscoveryRepository) DiscoverMemoryExtractionWork(
	_ context.Context,
	_ canonical.ID,
	request domain.MemoryDiscoveryRequest,
) (domain.MemoryDiscoveryResult, error) {
	repository.requests = append(repository.requests, request)
	if len(repository.requests) == 1 {
		return domain.MemoryDiscoveryResult{CycleComplete: true}, nil
	}
	return domain.MemoryDiscoveryResult{Work: &repository.work}, nil
}

func TestFairMemoryDiscoveryWrapsOnceBeforeYielding(t *testing.T) {
	residentID := foregroundCleanTestID(t)
	throughSeq, err := canonical.NewSeq(7)
	if err != nil {
		t.Fatal(err)
	}
	afterSeq := throughSeq
	repository := &wrapMemoryDiscoveryRepository{work: domain.MemoryExtractionWork{
		SourceEvent:    domain.Event{ID: residentID, ResidentID: residentID, Seq: throughSeq},
		IdempotencyKey: domain.MemoryExtractionObligation(residentID),
		State:          domain.WorkPending,
	}}
	application := &Application{
		repository:      repository,
		maxAttempts:     1,
		foregroundEpoch: make(map[canonical.ID]uint64),
		memoryDiscoveryCursors: map[canonical.ID]*domain.MemoryDiscoveryCursor{
			residentID: {AfterSeq: &afterSeq, CycleThroughSeq: throughSeq},
		},
	}

	_, err = application.processFairMandatoryMemory(context.Background(), residentID, throughSeq)
	if err == nil {
		t.Fatal("wrapped eligible work was not processed")
	}
	if len(repository.requests) != 2 {
		t.Fatalf("memory discovery calls = %d, want 2", len(repository.requests))
	}
	if repository.requests[0].Cursor == nil || repository.requests[0].Cursor.AfterSeq == nil ||
		*repository.requests[0].Cursor.AfterSeq != afterSeq {
		t.Fatalf("first cursor = %+v, want completed cursor after %d", repository.requests[0].Cursor, afterSeq)
	}
	if repository.requests[1].Cursor != nil {
		t.Fatalf("wrapped cursor = %+v, want nil", repository.requests[1].Cursor)
	}
	for index, request := range repository.requests {
		if request.ThroughSeq == nil || *request.ThroughSeq != throughSeq {
			t.Fatalf("request %d through seq = %v, want %d", index, request.ThroughSeq, throughSeq)
		}
	}
	if cursor := application.memoryDiscoveryCursor(residentID); cursor != nil {
		t.Fatalf("completed cursor survived wrap = %+v", cursor)
	}
}

type incompleteDialogueGateRepository struct {
	domain.Repository
	activeResidentCalls int
}

func (repository *incompleteDialogueGateRepository) DiscoverDialogueWork(
	context.Context,
	canonical.ID,
	domain.DialogueDiscoveryRequest,
) (domain.DialogueDiscoveryResult, error) {
	beforeSeq, err := canonical.NewSeq(1)
	if err != nil {
		return domain.DialogueDiscoveryResult{}, err
	}
	return domain.DialogueDiscoveryResult{
		NextCursor:      &domain.DialogueDiscoveryCursor{BeforeSeq: beforeSeq},
		BudgetExhausted: true,
	}, nil
}

func (repository *incompleteDialogueGateRepository) ActiveResident(context.Context) (domain.ResidentSnapshot, error) {
	repository.activeResidentCalls++
	return domain.ResidentSnapshot{}, errors.New("autonomy should not read the active resident before foreground is clean")
}

type incompleteDialogueGateSource struct{}

func (incompleteDialogueGateSource) AutonomySnapshot(
	context.Context,
	autonomy.SnapshotRequest,
) (autonomy.Snapshot, error) {
	return autonomy.Snapshot{}, nil
}

func (incompleteDialogueGateSource) RetentionSources(
	context.Context,
	autonomy.RetentionRequest,
) (autonomy.RetentionCapture, error) {
	return autonomy.RetentionCapture{}, nil
}

func TestAutonomyTurnStopsWhenDialogueCycleIsIncomplete(t *testing.T) {
	residentID := foregroundCleanTestID(t)
	repository := &incompleteDialogueGateRepository{}
	policy := autonomy.Policy{}
	policy.SelfTalk.Enabled = true
	application := &Application{
		repository:         repository,
		maxAttempts:        1,
		work:               make(map[canonical.ID]*sync.Mutex),
		background:         make(map[canonical.ID]*backgroundCall),
		foregroundEpoch:    make(map[canonical.ID]uint64),
		dialogueCleanEpoch: make(map[canonical.ID]uint64),
		dialogueScanCycles: make(map[canonical.ID]dialogueScanCycle),
		autonomyPolicy:     &policy,
		autonomySource:     incompleteDialogueGateSource{},
		autonomyClock:      autonomy.NewSystemSchedulerClock(),
	}

	application.processAutonomyTurn(context.Background(), residentID)
	if repository.activeResidentCalls != 0 {
		t.Fatalf("autonomy active-resident reads = %d, want 0", repository.activeResidentCalls)
	}
	if application.currentForegroundClean(residentID) {
		t.Fatal("incomplete dialogue cycle unexpectedly published a clean proof")
	}
}

func foregroundCleanTestID(t *testing.T) canonical.ID {
	t.Helper()
	id, err := canonical.ParseID("01HF7YAT00A9954MJJA9954M80")
	if err != nil {
		t.Fatal(err)
	}
	return id
}
