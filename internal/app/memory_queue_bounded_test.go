package app

import (
	"context"
	"sync"
	"testing"
	"time"

	"mahoroba.local/mahoroba/internal/autonomy"
	"mahoroba.local/mahoroba/internal/canonical"
	"mahoroba.local/mahoroba/internal/domain"
)

type boundedMemoryQueueRepository struct {
	domain.Repository

	mu                   sync.Mutex
	normalCalls          int
	reextractionCalls    int
	activeResidentCalls  int
	residentID           canonical.ID
	selectedID           canonical.ID
	throughSeq           canonical.Seq
	normalResults        []domain.MemoryDiscoveryResult
	reextractionResults  []domain.MemoryReextractionDiscoveryResult
	normalRequests       []domain.MemoryDiscoveryRequest
	reextractionRequests []domain.MemoryReextractionDiscoveryRequest
}

func (repository *boundedMemoryQueueRepository) DiscoverDialogueWork(
	context.Context,
	canonical.ID,
	domain.DialogueDiscoveryRequest,
) (domain.DialogueDiscoveryResult, error) {
	return domain.DialogueDiscoveryResult{CycleComplete: true}, nil
}

func (repository *boundedMemoryQueueRepository) DiscoverMemoryExtractionWork(
	_ context.Context,
	_ canonical.ID,
	request domain.MemoryDiscoveryRequest,
) (domain.MemoryDiscoveryResult, error) {
	repository.mu.Lock()
	repository.normalRequests = append(repository.normalRequests, request)
	repository.normalCalls++
	call := repository.normalCalls
	if call <= len(repository.normalResults) {
		result := repository.normalResults[call-1]
		repository.mu.Unlock()
		return result, nil
	}
	repository.mu.Unlock()
	after := repository.throughSeq
	return domain.MemoryDiscoveryResult{
		NextCursor: &domain.MemoryDiscoveryCursor{
			AfterSeq: &after, CycleThroughSeq: repository.throughSeq,
		},
		CandidatesScanned: 1,
		PageQueries:       1,
		BudgetExhausted:   true,
	}, nil
}

func (repository *boundedMemoryQueueRepository) DiscoverMemoryReextractionWork(
	_ context.Context,
	_ canonical.ID,
	request domain.MemoryReextractionDiscoveryRequest,
) (domain.MemoryReextractionDiscoveryResult, error) {
	repository.mu.Lock()
	repository.reextractionRequests = append(repository.reextractionRequests, request)
	repository.reextractionCalls++
	call := repository.reextractionCalls
	if call <= len(repository.reextractionResults) {
		result := repository.reextractionResults[call-1]
		repository.mu.Unlock()
		return result, nil
	}
	repository.mu.Unlock()
	return domain.MemoryReextractionDiscoveryResult{CycleComplete: true}, nil
}

func (*boundedMemoryQueueRepository) MemoryReextractionWork(
	context.Context,
	canonical.ID,
	canonical.ID,
	canonical.ID,
	int,
) (domain.MemoryExtractionWork, bool, error) {
	return domain.MemoryExtractionWork{}, false, nil
}

func (*boundedMemoryQueueRepository) PipelineVersion(
	context.Context,
	string,
	string,
) (domain.PipelineVersionDefinition, error) {
	return domain.PipelineVersionDefinition{}, nil
}

func (repository *boundedMemoryQueueRepository) Resident(
	context.Context,
	canonical.ID,
) (domain.ResidentSnapshot, error) {
	return domain.ResidentSnapshot{ResidentID: repository.residentID, Status: "active"}, nil
}

func (repository *boundedMemoryQueueRepository) ActiveResident(context.Context) (domain.ResidentSnapshot, error) {
	repository.mu.Lock()
	repository.activeResidentCalls++
	selectedID := repository.selectedID
	if selectedID.IsZero() {
		selectedID = repository.residentID
	}
	repository.mu.Unlock()
	return domain.ResidentSnapshot{ResidentID: selectedID, Status: "active"}, nil
}

func (repository *boundedMemoryQueueRepository) counts() (normal, reextraction, active int) {
	repository.mu.Lock()
	defer repository.mu.Unlock()
	return repository.normalCalls, repository.reextractionCalls, repository.activeResidentCalls
}

func (repository *boundedMemoryQueueRepository) requests() (
	[]domain.MemoryDiscoveryRequest,
	[]domain.MemoryReextractionDiscoveryRequest,
) {
	repository.mu.Lock()
	defer repository.mu.Unlock()
	normal := append([]domain.MemoryDiscoveryRequest(nil), repository.normalRequests...)
	reextraction := append([]domain.MemoryReextractionDiscoveryRequest(nil), repository.reextractionRequests...)
	return normal, reextraction
}

func TestCOVR02MemoryBudgetExhaustionYieldsResidentLockAndSuppressesOptional(t *testing.T) {
	residentID := foregroundCleanTestID(t)
	throughSeq, err := canonical.NewSeq(1)
	if err != nil {
		t.Fatal(err)
	}
	repository := &boundedMemoryQueueRepository{residentID: residentID, throughSeq: throughSeq}
	policy := autonomy.Policy{}
	policy.SelfTalk.Enabled = true
	application := &Application{
		repository:                    repository,
		maxAttempts:                   1,
		work:                          make(map[canonical.ID]*sync.Mutex),
		background:                    make(map[canonical.ID]*backgroundCall),
		foregroundEpoch:               make(map[canonical.ID]uint64),
		dialogueCleanEpoch:            make(map[canonical.ID]uint64),
		dialogueScanCycles:            make(map[canonical.ID]dialogueScanCycle),
		memoryDiscoveryCursors:        make(map[canonical.ID]*domain.MemoryDiscoveryCursor),
		normalMemoryDiscoveryCursors:  make(map[canonical.ID]*domain.MemoryDiscoveryCursor),
		memoryReextractionCursors:     make(map[canonical.ID]*domain.MemoryReextractionDiscoveryCursor),
		memoryReextractionCycles:      make(map[canonical.ID]bool),
		memoryNormalCyclesDirty:       make(map[canonical.ID]bool),
		memoryReextractionCyclesDirty: make(map[canonical.ID]bool),
		memoryNormalCompleteEpochs:    make(map[canonical.ID]uint64),
		memoryExtractionScansComplete: make(map[canonical.ID]bool),
		autonomyPolicy:                &policy,
		autonomySource:                incompleteDialogueGateSource{},
		autonomyClock:                 autonomy.NewSystemSchedulerClock(),
	}

	done := make(chan error, 1)
	go func() { done <- application.ProcessResident(context.Background(), residentID) }()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("bounded memory pass did not yield the resident lock")
	}
	if normal, reextraction, active := repository.counts(); normal != 1 || reextraction != 0 || active != 0 {
		t.Fatalf("first bounded turn calls = normal:%d reextraction:%d active:%d, want 1/0/0", normal, reextraction, active)
	}
	if !application.currentForegroundClean(residentID) {
		t.Fatal("complete dialogue scan did not publish its clean proof")
	}
	if application.memoryExtractionScansAreComplete(residentID) {
		t.Fatal("budget-exhausted memory scan was marked complete")
	}

	lockAcquired := make(chan struct{})
	go func() {
		lock := application.residentLock(residentID)
		lock.Lock()
		close(lockAcquired)
		lock.Unlock()
	}()
	select {
	case <-lockAcquired:
	case <-time.After(time.Second):
		t.Fatal("resident lock remained held after bounded memory pass")
	}

	application.processAutonomyTurn(context.Background(), residentID)
	if normal, reextraction, active := repository.counts(); normal != 2 || reextraction != 0 || active != 0 {
		t.Fatalf("autonomy gate calls = normal:%d reextraction:%d active:%d, want second scan and no optional admission", normal, reextraction, active)
	}
}

func TestCOVR02NormalAndReextractionCursorsAdvanceIndependentlyOneWorkPerTurn(t *testing.T) {
	residentID := foregroundCleanTestID(t)
	selectedID, err := canonical.ParseID("01HF7YAT00A9954MJJA9954M81")
	if err != nil {
		t.Fatal(err)
	}
	throughSeq, err := canonical.NewSeq(7)
	if err != nil {
		t.Fatal(err)
	}
	normalAfter := throughSeq
	normalCursor := &domain.MemoryDiscoveryCursor{AfterSeq: &normalAfter, CycleThroughSeq: throughSeq}
	reextractionCommitSeq := canonical.CommitSeq(1)
	reextractionAfterRunID := residentID
	reextractionCursor := &domain.MemoryReextractionDiscoveryCursor{
		CycleThroughCommitSeq: reextractionCommitSeq,
		ActiveCommit: &domain.MemoryReextractionCommitCursor{
			CommitID: residentID, CommitSeq: reextractionCommitSeq, AfterRunID: &reextractionAfterRunID,
		},
	}
	normalWork := domain.MemoryExtractionWork{
		SourceEvent: domain.Event{ID: residentID, ResidentID: residentID, Seq: throughSeq},
		State:       domain.WorkPending,
	}
	reextractionWork := normalWork
	repository := &boundedMemoryQueueRepository{
		residentID: residentID, selectedID: selectedID, throughSeq: throughSeq,
		normalResults: []domain.MemoryDiscoveryResult{
			{Work: &normalWork, NextCursor: normalCursor, CandidatesScanned: 1, PageQueries: 1},
			{CycleComplete: true},
			{CycleComplete: true},
		},
		reextractionResults: []domain.MemoryReextractionDiscoveryResult{
			{Work: &reextractionWork, NextCursor: reextractionCursor, CandidatesScanned: 1, PageQueries: 1},
		},
	}
	application := &Application{
		repository:                    repository,
		maxAttempts:                   1,
		background:                    make(map[canonical.ID]*backgroundCall),
		foregroundEpoch:               make(map[canonical.ID]uint64),
		memoryDiscoveryCursors:        make(map[canonical.ID]*domain.MemoryDiscoveryCursor),
		normalMemoryDiscoveryCursors:  make(map[canonical.ID]*domain.MemoryDiscoveryCursor),
		memoryReextractionCursors:     make(map[canonical.ID]*domain.MemoryReextractionDiscoveryCursor),
		memoryReextractionCycles:      make(map[canonical.ID]bool),
		memoryNormalCyclesDirty:       make(map[canonical.ID]bool),
		memoryReextractionCyclesDirty: make(map[canonical.ID]bool),
		memoryNormalCompleteEpochs:    make(map[canonical.ID]uint64),
		memoryExtractionScansComplete: make(map[canonical.ID]bool),
	}

	if err := application.processMemoryExtractionQueue(context.Background(), residentID); err != nil {
		t.Fatal(err)
	}
	if normal, reextraction, active := repository.counts(); normal != 1 || reextraction != 0 || active != 1 {
		t.Fatalf("normal quantum calls = %d/%d/%d, want 1/0/1", normal, reextraction, active)
	}
	if got := application.normalMemoryDiscoveryCursor(residentID); got == nil || got.AfterSeq == nil || *got.AfterSeq != throughSeq {
		t.Fatalf("normal cursor after first quantum = %+v", got)
	}
	if got := application.memoryReextractionCursor(residentID); got != nil {
		t.Fatalf("re-extraction cursor advanced during normal quantum: %+v", got)
	}

	if err := application.processMemoryExtractionQueue(context.Background(), residentID); err != nil {
		t.Fatal(err)
	}
	if normal, reextraction, active := repository.counts(); normal != 2 || reextraction != 0 || active != 1 {
		t.Fatalf("dirty normal completion calls = %d/%d/%d, want 2/0/1", normal, reextraction, active)
	}
	if got := application.normalMemoryDiscoveryCursor(residentID); got == nil || got.AfterSeq != nil || got.CycleThroughSeq != throughSeq {
		t.Fatalf("dirty normal verification cursor = %+v, want fixed ceiling %s from cycle start", got, throughSeq)
	}
	if got := application.memoryReextractionCursor(residentID); got != nil {
		t.Fatalf("re-extraction cursor advanced before normal verification: %+v", got)
	}
	if application.memoryExtractionScansAreComplete(residentID) {
		t.Fatal("one-work re-extraction quantum was marked globally complete")
	}
	if application.memoryReextractionCycleActive(residentID) {
		t.Fatal("dirty normal cycle entered re-extraction without verification")
	}

	if err := application.processMemoryExtractionQueue(context.Background(), residentID); err != nil {
		t.Fatal(err)
	}
	normalRequests, reextractionRequests := repository.requests()
	if len(normalRequests) < 3 || normalRequests[2].Cursor == nil ||
		normalRequests[2].Cursor.AfterSeq != nil || normalRequests[2].Cursor.CycleThroughSeq != throughSeq {
		t.Fatalf("normal verification request recaptured its ceiling: %+v", normalRequests)
	}
	if normal, reextraction, active := repository.counts(); normal != 3 || reextraction != 1 || active != 2 {
		t.Fatalf("verified normal/re-extraction work calls = %d/%d/%d, want 3/1/2", normal, reextraction, active)
	}
	if got := application.memoryReextractionCursor(residentID); got == nil || got.ActiveCommit == nil ||
		got.ActiveCommit.AfterRunID == nil || *got.ActiveCommit.AfterRunID != residentID {
		t.Fatalf("re-extraction cursor after work quantum = %+v", got)
	}
	if !application.memoryReextractionCycleActive(residentID) {
		t.Fatal("re-extraction phase was not retained across the one-work yield")
	}

	if err := application.processMemoryExtractionQueue(context.Background(), residentID); err != nil {
		t.Fatal(err)
	}
	if normal, reextraction, active := repository.counts(); normal != 3 || reextraction != 2 || active != 2 {
		t.Fatalf("dirty re-extraction completion calls = %d/%d/%d, want 3/2/2", normal, reextraction, active)
	}
	if !application.memoryReextractionCycleActive(residentID) {
		t.Fatal("dirty re-extraction cycle did not retain its phase for verification")
	}
	if got := application.memoryReextractionCursor(residentID); got == nil || got.ActiveCommit != nil ||
		got.CompletedThroughCommitSeq != nil || got.CycleThroughCommitSeq != reextractionCommitSeq {
		t.Fatalf("dirty re-extraction verification cursor = %+v, want fixed cycle bound %d", got, reextractionCommitSeq)
	}
	if application.memoryExtractionScansAreComplete(residentID) {
		t.Fatal("dirty re-extraction cycle published completeness")
	}

	if err := application.processMemoryExtractionQueue(context.Background(), residentID); err != nil {
		t.Fatal(err)
	}
	_, reextractionRequests = repository.requests()
	if len(reextractionRequests) < 3 || reextractionRequests[2].Cursor == nil ||
		reextractionRequests[2].Cursor.ActiveCommit != nil ||
		reextractionRequests[2].Cursor.CompletedThroughCommitSeq != nil ||
		reextractionRequests[2].Cursor.CycleThroughCommitSeq != reextractionCommitSeq {
		t.Fatalf("re-extraction verification request recaptured its ceiling: %+v", reextractionRequests)
	}
	if normal, reextraction, active := repository.counts(); normal != 3 || reextraction != 3 || active != 3 {
		t.Fatalf("verified re-extraction completion calls = %d/%d/%d, want 3/3/3", normal, reextraction, active)
	}
	if application.memoryReextractionCycleActive(residentID) {
		t.Fatal("verified re-extraction phase survived")
	}
	if !application.memoryExtractionScansAreComplete(residentID) {
		t.Fatal("both verified discovery phases did not publish completeness")
	}
}

func TestCOVR02ReextractionCursorCloneOwnsNestedProgress(t *testing.T) {
	residentID := foregroundCleanTestID(t)
	completed := canonical.CommitSeq(3)
	afterRunID := residentID
	cursor := &domain.MemoryReextractionDiscoveryCursor{
		CycleThroughCommitSeq:     canonical.CommitSeq(5),
		CompletedThroughCommitSeq: &completed,
		ActiveCommit: &domain.MemoryReextractionCommitCursor{
			CommitID: residentID, CommitSeq: canonical.CommitSeq(4), AfterRunID: &afterRunID,
		},
	}

	cloned := cloneMemoryReextractionDiscoveryCursor(cursor)
	if cloned == nil || cloned.CompletedThroughCommitSeq == nil || cloned.ActiveCommit == nil ||
		cloned.ActiveCommit.AfterRunID == nil || cloned == cursor ||
		cloned.CompletedThroughCommitSeq == cursor.CompletedThroughCommitSeq ||
		cloned.ActiveCommit == cursor.ActiveCommit ||
		cloned.ActiveCommit.AfterRunID == cursor.ActiveCommit.AfterRunID {
		t.Fatalf("re-extraction cursor clone retained source pointers: source=%+v clone=%+v", cursor, cloned)
	}
	if cloned.CycleThroughCommitSeq != cursor.CycleThroughCommitSeq ||
		*cloned.CompletedThroughCommitSeq != completed ||
		cloned.ActiveCommit.CommitID != residentID ||
		cloned.ActiveCommit.CommitSeq != canonical.CommitSeq(4) ||
		*cloned.ActiveCommit.AfterRunID != afterRunID {
		t.Fatalf("re-extraction cursor clone changed progress: source=%+v clone=%+v", cursor, cloned)
	}
}

func TestCOVR02ArchivedReextractionDrainRejectsRepeatedCursor(t *testing.T) {
	residentID := foregroundCleanTestID(t)
	completed := canonical.CommitSeq(1)
	cursor := &domain.MemoryReextractionDiscoveryCursor{
		CycleThroughCommitSeq:     canonical.CommitSeq(2),
		CompletedThroughCommitSeq: &completed,
	}
	repository := &boundedMemoryQueueRepository{
		residentID: residentID,
		reextractionResults: []domain.MemoryReextractionDiscoveryResult{
			{NextCursor: cursor, BudgetExhausted: true},
			{NextCursor: cloneMemoryReextractionDiscoveryCursor(cursor), BudgetExhausted: true},
		},
	}
	application := &Application{repository: repository, maxAttempts: 1}

	if err := application.cancelArchivedResidentMemoryReextractions(
		context.Background(), residentID,
	); err == nil {
		t.Fatal("archive re-extraction drain accepted a repeated cursor")
	}
	if _, reextraction, _ := repository.counts(); reextraction != 2 {
		t.Fatalf("archive re-extraction discovery calls = %d, want 2", reextraction)
	}
}

func TestCOVR02ForegroundAdvanceDuringReextractionRequiresFreshNormalCompletion(t *testing.T) {
	residentID := foregroundCleanTestID(t)
	throughSeq, err := canonical.NewSeq(5)
	if err != nil {
		t.Fatal(err)
	}
	repository := &boundedMemoryQueueRepository{
		residentID: residentID,
		throughSeq: throughSeq,
		reextractionResults: []domain.MemoryReextractionDiscoveryResult{
			{CycleComplete: true},
		},
	}
	application := &Application{
		repository:                    repository,
		maxAttempts:                   1,
		background:                    make(map[canonical.ID]*backgroundCall),
		foregroundEpoch:               map[canonical.ID]uint64{residentID: 1},
		memoryDiscoveryCursors:        make(map[canonical.ID]*domain.MemoryDiscoveryCursor),
		normalMemoryDiscoveryCursors:  make(map[canonical.ID]*domain.MemoryDiscoveryCursor),
		memoryReextractionCursors:     make(map[canonical.ID]*domain.MemoryReextractionDiscoveryCursor),
		memoryReextractionCycles:      map[canonical.ID]bool{residentID: true},
		memoryReextractionCycleEpochs: map[canonical.ID]uint64{residentID: 0},
		memoryNormalCyclesDirty:       make(map[canonical.ID]bool),
		memoryReextractionCyclesDirty: make(map[canonical.ID]bool),
		memoryNormalCompleteEpochs:    map[canonical.ID]uint64{residentID: 0},
		memoryExtractionScansComplete: make(map[canonical.ID]bool),
	}

	if err := application.processMemoryExtractionQueue(context.Background(), residentID); err != nil {
		t.Fatal(err)
	}
	if normal, reextraction, active := repository.counts(); normal != 0 || reextraction != 1 || active != 0 {
		t.Fatalf("stale normal proof calls = %d/%d/%d, want 0/1/0", normal, reextraction, active)
	}
	if application.memoryExtractionScansAreComplete(residentID) {
		t.Fatal("re-extraction completion reused a normal proof from an older foreground epoch")
	}
	if application.memoryReextractionCycleActive(residentID) {
		t.Fatal("completed re-extraction phase was not released for a fresh normal cycle")
	}

	if err := application.processMemoryExtractionQueue(context.Background(), residentID); err != nil {
		t.Fatal(err)
	}
	if normal, reextraction, active := repository.counts(); normal != 1 || reextraction != 1 || active != 0 {
		t.Fatalf("fresh normal cycle calls = %d/%d/%d, want 1/1/0", normal, reextraction, active)
	}
}

func TestCOVR02ForegroundAdvanceDuringNormalCycleRequiresFreshCurrentEpochCycle(t *testing.T) {
	residentID := foregroundCleanTestID(t)
	oldThrough, err := canonical.NewSeq(5)
	if err != nil {
		t.Fatal(err)
	}
	oldAfter := oldThrough
	newThrough, err := canonical.NewSeq(9)
	if err != nil {
		t.Fatal(err)
	}
	newAfter := newThrough
	repository := &boundedMemoryQueueRepository{
		residentID: residentID,
		throughSeq: newThrough,
		normalResults: []domain.MemoryDiscoveryResult{
			{CycleComplete: true},
			{
				NextCursor:        &domain.MemoryDiscoveryCursor{AfterSeq: &newAfter, CycleThroughSeq: newThrough},
				CandidatesScanned: 1, PageQueries: 1, BudgetExhausted: true,
			},
		},
	}
	application := &Application{
		repository:                    repository,
		maxAttempts:                   1,
		background:                    make(map[canonical.ID]*backgroundCall),
		foregroundEpoch:               map[canonical.ID]uint64{residentID: 1},
		memoryDiscoveryCursors:        make(map[canonical.ID]*domain.MemoryDiscoveryCursor),
		normalMemoryDiscoveryCursors:  map[canonical.ID]*domain.MemoryDiscoveryCursor{residentID: {AfterSeq: &oldAfter, CycleThroughSeq: oldThrough}},
		memoryNormalCycleEpochs:       map[canonical.ID]uint64{residentID: 0},
		memoryReextractionCursors:     make(map[canonical.ID]*domain.MemoryReextractionDiscoveryCursor),
		memoryReextractionCycles:      make(map[canonical.ID]bool),
		memoryReextractionCycleEpochs: make(map[canonical.ID]uint64),
		memoryNormalCyclesDirty:       make(map[canonical.ID]bool),
		memoryReextractionCyclesDirty: make(map[canonical.ID]bool),
		memoryNormalCompleteEpochs:    make(map[canonical.ID]uint64),
		memoryExtractionScansComplete: make(map[canonical.ID]bool),
	}

	if err := application.processMemoryExtractionQueue(context.Background(), residentID); err != nil {
		t.Fatal(err)
	}
	if normal, reextraction, _ := repository.counts(); normal != 1 || reextraction != 0 {
		t.Fatalf("stale normal completion calls = %d/%d, want 1/0", normal, reextraction)
	}
	if application.memoryExtractionScansAreComplete(residentID) || application.memoryReextractionCycleActive(residentID) {
		t.Fatal("old-epoch normal cycle published current completeness")
	}
	if got := application.normalMemoryDiscoveryCursor(residentID); got != nil {
		t.Fatalf("old-epoch normal cursor survived completed cycle: %+v", got)
	}

	if err := application.processMemoryExtractionQueue(context.Background(), residentID); err != nil {
		t.Fatal(err)
	}
	normalRequests, _ := repository.requests()
	if len(normalRequests) != 2 || normalRequests[1].Cursor != nil {
		t.Fatalf("fresh current-epoch normal cycle did not recapture its ceiling: %+v", normalRequests)
	}
	if got := application.normalMemoryDiscoveryCursor(residentID); got == nil || got.CycleThroughSeq != newThrough {
		t.Fatalf("fresh current-epoch cursor = %+v, want ceiling %s", got, newThrough)
	}
	if application.memoryExtractionScansAreComplete(residentID) {
		t.Fatal("incomplete fresh current-epoch cycle published completeness")
	}
}

func TestCOVR02AutonomyRechecksMemoryCompletenessAfterResidentLock(t *testing.T) {
	residentID := foregroundCleanTestID(t)
	throughSeq, err := canonical.NewSeq(1)
	if err != nil {
		t.Fatal(err)
	}
	repository := &boundedMemoryQueueRepository{residentID: residentID, throughSeq: throughSeq}
	policy := autonomy.Policy{}
	policy.SelfTalk.Enabled = true
	application := &Application{
		repository:                    repository,
		maxAttempts:                   1,
		work:                          make(map[canonical.ID]*sync.Mutex),
		background:                    make(map[canonical.ID]*backgroundCall),
		foregroundEpoch:               make(map[canonical.ID]uint64),
		dialogueCleanEpoch:            map[canonical.ID]uint64{residentID: 0},
		memoryExtractionScansComplete: map[canonical.ID]bool{residentID: true},
		autonomyPolicy:                &policy,
		autonomySource:                incompleteDialogueGateSource{},
		autonomyClock:                 autonomy.NewSystemSchedulerClock(),
	}

	lock := application.residentLock(residentID)
	lock.Lock()
	done := make(chan error, 1)
	go func() {
		done <- application.processAutonomyResidentAfterMandatory(context.Background(), residentID)
	}()
	deadline := time.Now().Add(time.Second)
	for {
		_, _, active := repository.counts()
		if active == 1 {
			break
		}
		if time.Now().After(deadline) {
			lock.Unlock()
			t.Fatal("autonomy turn did not reach the pre-lock selection check")
		}
		time.Sleep(time.Millisecond)
	}
	application.setMemoryExtractionScansComplete(residentID, false)
	lock.Unlock()

	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("autonomy turn did not return after resident lock release")
	}
	if _, _, active := repository.counts(); active != 1 {
		t.Fatalf("optional admission continued after memory proof invalidation: active reads=%d, want 1", active)
	}
}
