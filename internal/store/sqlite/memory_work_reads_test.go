package sqlite

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"mahoroba.local/mahoroba/internal/canonical"
	"mahoroba.local/mahoroba/internal/domain"
	"mahoroba.local/mahoroba/internal/memory"
)

func TestMemoryDiscoveryAdvancesAscendingWithinFixedCycle(t *testing.T) {
	fixture, repository, residentID, closeFixture := newBoundedMemoryDiscoveryFixture(t)
	defer closeFixture()

	insertMemoryDiscoveryDialogueCommitB(t, fixture, fixture.event["A"])
	insertTerminalMemoryDiscoveryRun(t, fixture, fixture.event["A"])
	for seq := int64(2); seq <= 4; seq++ {
		eventID := insertMemoryDiscoveryEvent(t, fixture, seq, "user_message")
		insertMemoryDiscoveryDialogueCommitB(t, fixture, eventID)
		if seq == 2 {
			insertTerminalMemoryDiscoveryRun(t, fixture, eventID)
		}
	}

	request := domain.MemoryDiscoveryRequest{
		ThroughSeq:  memoryDiscoveryThrough(4),
		MaxAttempts: 1,
		Budget: domain.MemoryDiscoveryBudget{
			PageSize: 2, Candidates: 2, Pages: 1, Elapsed: time.Second,
		},
	}
	first, err := repository.DiscoverMemoryExtractionWork(context.Background(), residentID, request)
	if err != nil {
		t.Fatal(err)
	}
	assertMemoryDiscoveryResult(t, first, memoryDiscoveryExpectation{
		nextAfter: 2, cycleThrough: 4, scanned: 2, pages: 1, budgetExhausted: true,
	})

	request.Cursor = first.NextCursor
	second, err := repository.DiscoverMemoryExtractionWork(context.Background(), residentID, request)
	if err != nil {
		t.Fatal(err)
	}
	assertMemoryDiscoveryResult(t, second, memoryDiscoveryExpectation{
		workSeq: 3, nextAfter: 3, cycleThrough: 4, scanned: 1, pages: 1,
	})

	// Losing the Operational cursor repeats immutable classification from the
	// oldest unfinished obligation instead of skipping it.
	lostCursorRequest := request
	lostCursorRequest.Cursor = nil
	lostCursorRequest.Budget = domain.MemoryDiscoveryBudget{
		PageSize: 4, Candidates: 4, Pages: 1, Elapsed: time.Second,
	}
	lost, err := repository.DiscoverMemoryExtractionWork(context.Background(), residentID, lostCursorRequest)
	if err != nil {
		t.Fatal(err)
	}
	if lost.Work == nil || lost.Work.SourceEvent.Seq.Int64() != 3 {
		t.Fatalf("cursor-loss work = %+v, want oldest unfinished seq 3", lost.Work)
	}

	// New ingress does not extend the in-progress cycle ceiling.
	event5 := insertMemoryDiscoveryEvent(t, fixture, 5, "user_message")
	insertMemoryDiscoveryDialogueCommitB(t, fixture, event5)
	request.Cursor = second.NextCursor
	third, err := repository.DiscoverMemoryExtractionWork(context.Background(), residentID, request)
	if err != nil {
		t.Fatal(err)
	}
	assertMemoryDiscoveryResult(t, third, memoryDiscoveryExpectation{
		workSeq: 4, nextAfter: 4, cycleThrough: 4, scanned: 1, pages: 1,
	})

	insertTerminalMemoryDiscoveryRun(t, fixture, fixture.event["seq-3"])
	insertTerminalMemoryDiscoveryRun(t, fixture, fixture.event["seq-4"])
	request.Cursor = third.NextCursor
	complete, err := repository.DiscoverMemoryExtractionWork(context.Background(), residentID, request)
	if err != nil {
		t.Fatal(err)
	}
	assertMemoryDiscoveryResult(t, complete, memoryDiscoveryExpectation{pages: 1, cycleComplete: true})

	request.Cursor = nil
	request.ThroughSeq = memoryDiscoveryThrough(5)
	request.Budget = domain.MemoryDiscoveryBudget{PageSize: 5, Candidates: 5, Pages: 1, Elapsed: time.Second}
	nextCycle, err := repository.DiscoverMemoryExtractionWork(context.Background(), residentID, request)
	if err != nil {
		t.Fatal(err)
	}
	assertMemoryDiscoveryResult(t, nextCycle, memoryDiscoveryExpectation{
		workSeq: 5, nextAfter: 5, cycleThrough: 5, scanned: 5, pages: 1,
	})
}

func TestMemoryDiscoveryDefersUserMessageUntilDialogueCommitB(t *testing.T) {
	fixture, repository, residentID, closeFixture := newBoundedMemoryDiscoveryFixture(t)
	defer closeFixture()

	request := domain.MemoryDiscoveryRequest{
		ThroughSeq: memoryDiscoveryThrough(1), MaxAttempts: 1,
		Budget: domain.MemoryDiscoveryBudget{PageSize: 1, Candidates: 1, Pages: 1, Elapsed: time.Second},
	}
	deferred, err := repository.DiscoverMemoryExtractionWork(context.Background(), residentID, request)
	if err != nil {
		t.Fatal(err)
	}
	assertMemoryDiscoveryResult(t, deferred, memoryDiscoveryExpectation{
		scanned: 1, pages: 1, dialogueDeferred: 1, cycleComplete: true,
	})

	insertMemoryDiscoveryDialogueCommitB(t, fixture, fixture.event["A"])
	ready, err := repository.DiscoverMemoryExtractionWork(context.Background(), residentID, request)
	if err != nil {
		t.Fatal(err)
	}
	assertMemoryDiscoveryResult(t, ready, memoryDiscoveryExpectation{
		workSeq: 1, nextAfter: 1, cycleThrough: 1, scanned: 1, pages: 1,
	})
	if ready.Work.State != domain.WorkPending {
		t.Fatalf("ready work state = %q, want pending", ready.Work.State)
	}
}

func TestMemoryDiscoveryLowerThroughSeqRestartsHigherCycle(t *testing.T) {
	fixture, repository, residentID, closeFixture := newBoundedMemoryDiscoveryFixture(t)
	defer closeFixture()
	insertMemoryDiscoveryDialogueCommitB(t, fixture, fixture.event["A"])

	after := canonical.Seq(9)
	result, err := repository.DiscoverMemoryExtractionWork(
		context.Background(), residentID, domain.MemoryDiscoveryRequest{
			Cursor:      &domain.MemoryDiscoveryCursor{AfterSeq: &after, CycleThroughSeq: canonical.Seq(10)},
			ThroughSeq:  memoryDiscoveryThrough(1),
			MaxAttempts: 1,
			Budget: domain.MemoryDiscoveryBudget{
				PageSize: 1, Candidates: 1, Pages: 1, Elapsed: time.Second,
			},
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	assertMemoryDiscoveryResult(t, result, memoryDiscoveryExpectation{
		workSeq: 1, nextAfter: 1, cycleThrough: 1, scanned: 1, pages: 1,
	})
}

func TestMemoryDiscoverySelfTalkDoesNotRequireDialogueCommitB(t *testing.T) {
	fixture, repository, residentID, closeFixture := newBoundedMemoryDiscoveryFixture(t)
	defer closeFixture()

	commitID := activateMemoryDiscoveryPolicyV3(t, fixture)
	selfTalkID := insertMemoryDiscoveryEventAtCommit(t, fixture, 2, "self_talk", commitID)
	request := domain.MemoryDiscoveryRequest{
		ThroughSeq: memoryDiscoveryThrough(2), MaxAttempts: 1,
		Budget: domain.MemoryDiscoveryBudget{PageSize: 2, Candidates: 2, Pages: 1, Elapsed: time.Second},
	}
	result, err := repository.DiscoverMemoryExtractionWork(context.Background(), residentID, request)
	if err != nil {
		t.Fatal(err)
	}
	assertMemoryDiscoveryResult(t, result, memoryDiscoveryExpectation{
		workSeq: 2, nextAfter: 2, cycleThrough: 2, scanned: 2, pages: 1, dialogueDeferred: 1,
	})
	if result.Work.SourceEvent.ID.String() != selfTalkID {
		t.Fatalf("self-talk work source = %s, want %s", result.Work.SourceEvent.ID, selfTalkID)
	}
}

func TestMemoryDiscoveryBoundsLongTerminalHistory(t *testing.T) {
	fixture, repository, residentID, closeFixture := newBoundedMemoryDiscoveryFixture(t)
	defer closeFixture()

	insertMemoryDiscoveryDialogueCommitB(t, fixture, fixture.event["A"])
	insertTerminalMemoryDiscoveryRun(t, fixture, fixture.event["A"])
	const through = int64(20)
	for seq := int64(2); seq <= through; seq++ {
		eventID := insertMemoryDiscoveryEvent(t, fixture, seq, "user_message")
		insertMemoryDiscoveryDialogueCommitB(t, fixture, eventID)
		insertTerminalMemoryDiscoveryRun(t, fixture, eventID)
	}

	result, err := repository.DiscoverMemoryExtractionWork(
		context.Background(), residentID, domain.MemoryDiscoveryRequest{
			ThroughSeq: memoryDiscoveryThrough(through), MaxAttempts: 1,
			Budget: domain.MemoryDiscoveryBudget{PageSize: 2, Candidates: 4, Pages: 2, Elapsed: time.Second},
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	assertMemoryDiscoveryResult(t, result, memoryDiscoveryExpectation{
		nextAfter: 4, cycleThrough: through, scanned: 4, pages: 2, budgetExhausted: true,
	})
}

func TestMemoryDiscoveryCallerCancellationDoesNotAdvanceCursor(t *testing.T) {
	_, repository, residentID, closeFixture := newBoundedMemoryDiscoveryFixture(t)
	defer closeFixture()
	after := canonical.Seq(1)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	result, err := repository.DiscoverMemoryExtractionWork(
		ctx, residentID, domain.MemoryDiscoveryRequest{
			Cursor:      &domain.MemoryDiscoveryCursor{AfterSeq: &after, CycleThroughSeq: canonical.Seq(2)},
			ThroughSeq:  memoryDiscoveryThrough(2),
			MaxAttempts: 1,
			Budget: domain.MemoryDiscoveryBudget{
				PageSize: 1, Candidates: 1, Pages: 1, Elapsed: time.Second,
			},
		},
	)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("discovery error = %v, want context.Canceled", err)
	}
	if result != (domain.MemoryDiscoveryResult{}) {
		t.Fatalf("cancelled discovery advanced progress: %+v", result)
	}
}

func TestMemoryDiscoveryRepositoryErrorDoesNotAdvanceCursor(t *testing.T) {
	_, repository, residentID, closeFixture := newBoundedMemoryDiscoveryFixture(t)
	closeFixture()
	after := canonical.Seq(1)
	result, err := repository.DiscoverMemoryExtractionWork(
		context.Background(), residentID, domain.MemoryDiscoveryRequest{
			Cursor:      &domain.MemoryDiscoveryCursor{AfterSeq: &after, CycleThroughSeq: canonical.Seq(2)},
			ThroughSeq:  memoryDiscoveryThrough(2),
			MaxAttempts: 1,
			Budget: domain.MemoryDiscoveryBudget{
				PageSize: 1, Candidates: 1, Pages: 1, Elapsed: time.Second,
			},
		},
	)
	if err == nil {
		t.Fatal("closed repository discovery unexpectedly succeeded")
	}
	if result != (domain.MemoryDiscoveryResult{}) {
		t.Fatalf("repository error advanced progress: %+v", result)
	}
}

func TestMemoryDiscoveryInternalDeadlineReturnsPriorCursorProgress(t *testing.T) {
	initialAfter := canonical.Seq(1)
	initialCursor := &domain.MemoryDiscoveryCursor{
		AfterSeq: &initialAfter, CycleThroughSeq: canonical.Seq(5),
	}
	after := canonical.Seq(3)
	cursor := &domain.MemoryDiscoveryCursor{AfterSeq: &after, CycleThroughSeq: canonical.Seq(5)}
	scanCtx, cancel := context.WithDeadline(context.Background(), time.Now().Add(-time.Second))
	defer cancel()
	result, err := boundedMemoryDiscoveryFailure(
		context.Background(), scanCtx,
		domain.MemoryDiscoveryResult{CandidatesScanned: 3, PageQueries: 1},
		initialCursor, cursor, context.DeadlineExceeded,
	)
	if err != nil {
		t.Fatal(err)
	}
	if !result.BudgetExhausted || result.NextCursor == nil || result.NextCursor.AfterSeq == nil ||
		result.NextCursor.AfterSeq.Int64() != 3 || result.NextCursor.CycleThroughSeq.Int64() != 5 ||
		memoryDiscoveryCursorsEqual(initialCursor, result.NextCursor) ||
		result.CandidatesScanned != 3 || result.PageQueries != 1 {
		t.Fatalf("internal deadline progress = %+v", result)
	}
}

func TestMemoryDiscoveryInternalDeadlineAfterCeilingReturnsCursorProgress(t *testing.T) {
	cursor := &domain.MemoryDiscoveryCursor{CycleThroughSeq: canonical.Seq(5)}
	scanCtx, cancel := context.WithDeadline(context.Background(), time.Now().Add(-time.Second))
	defer cancel()
	result, err := boundedMemoryDiscoveryFailure(
		context.Background(), scanCtx, domain.MemoryDiscoveryResult{},
		nil, cursor, context.DeadlineExceeded,
	)
	if err != nil {
		t.Fatal(err)
	}
	if !result.BudgetExhausted || result.NextCursor == nil ||
		result.NextCursor.CycleThroughSeq != canonical.Seq(5) || result.NextCursor.AfterSeq != nil ||
		memoryDiscoveryCursorsEqual(nil, result.NextCursor) ||
		result.CandidatesScanned != 0 || result.PageQueries != 0 {
		t.Fatalf("deadline after ceiling progress = %+v", result)
	}
}

func TestMemoryDiscoveryInternalDeadlineBeforeCursorIsAnError(t *testing.T) {
	scanCtx, cancel := context.WithDeadline(context.Background(), time.Now().Add(-time.Second))
	defer cancel()
	result, err := boundedMemoryDiscoveryFailure(
		context.Background(), scanCtx, domain.MemoryDiscoveryResult{}, nil, nil,
		context.DeadlineExceeded,
	)
	if err == nil {
		t.Fatal("deadline before fixed cursor unexpectedly succeeded")
	}
	if result != (domain.MemoryDiscoveryResult{}) {
		t.Fatalf("deadline before fixed cursor advanced progress: %+v", result)
	}
}

func TestMemoryDiscoveryInternalDeadlineWithoutCursorAdvanceIsAnError(t *testing.T) {
	after := canonical.Seq(3)
	cursor := &domain.MemoryDiscoveryCursor{
		AfterSeq: &after, CycleThroughSeq: canonical.Seq(5),
	}
	scanCtx, cancel := context.WithDeadline(context.Background(), time.Now().Add(-time.Second))
	defer cancel()
	result, err := boundedMemoryDiscoveryFailure(
		context.Background(), scanCtx, domain.MemoryDiscoveryResult{},
		cursor, cloneMemoryDiscoveryCursor(cursor), context.DeadlineExceeded,
	)
	if err == nil {
		t.Fatal("deadline without cursor advance unexpectedly succeeded")
	}
	if result != (domain.MemoryDiscoveryResult{}) {
		t.Fatalf("deadline without cursor advance returned progress: %+v", result)
	}
}

func TestMemoryDiscoveryPreemptedWorkDoesNotHeadOfLineBlockLaterObligation(t *testing.T) {
	fixture, repository, residentID, closeFixture := newBoundedMemoryDiscoveryFixture(t)
	defer closeFixture()

	insertMemoryDiscoveryDialogueCommitB(t, fixture, fixture.event["A"])
	insertTerminalMemoryDiscoveryRun(t, fixture, fixture.event["A"])
	event2 := insertMemoryDiscoveryEvent(t, fixture, 2, "user_message")
	insertMemoryDiscoveryDialogueCommitB(t, fixture, event2)
	insertPreemptedMemoryDiscoveryRun(t, fixture, event2)
	event3 := insertMemoryDiscoveryEvent(t, fixture, 3, "user_message")
	insertMemoryDiscoveryDialogueCommitB(t, fixture, event3)

	request := domain.MemoryDiscoveryRequest{
		ThroughSeq: memoryDiscoveryThrough(3), MaxAttempts: 1,
		Budget: domain.MemoryDiscoveryBudget{PageSize: 3, Candidates: 3, Pages: 1, Elapsed: time.Second},
	}
	old, err := repository.DiscoverMemoryExtractionWork(context.Background(), residentID, request)
	if err != nil {
		t.Fatal(err)
	}
	assertMemoryDiscoveryResult(t, old, memoryDiscoveryExpectation{
		workSeq: 2, nextAfter: 2, cycleThrough: 3, scanned: 2, pages: 1,
	})
	if !old.Work.ForegroundPreempted || old.Work.State != domain.WorkRetryPending {
		t.Fatalf("old work = %+v, want foreground-preempted retry", old.Work)
	}

	request.Cursor = old.NextCursor
	later, err := repository.DiscoverMemoryExtractionWork(context.Background(), residentID, request)
	if err != nil {
		t.Fatal(err)
	}
	assertMemoryDiscoveryResult(t, later, memoryDiscoveryExpectation{
		workSeq: 3, nextAfter: 3, cycleThrough: 3, scanned: 1, pages: 1,
	})
}

func TestMemoryDiscoveryCapturesFixedCeilingWhenThroughSeqIsOmitted(t *testing.T) {
	fixture, repository, residentID, closeFixture := newBoundedMemoryDiscoveryFixture(t)
	defer closeFixture()
	insertMemoryDiscoveryDialogueCommitB(t, fixture, fixture.event["A"])

	request := domain.MemoryDiscoveryRequest{
		MaxAttempts: 1,
		Budget: domain.MemoryDiscoveryBudget{
			PageSize: 1, Candidates: 1, Pages: 1, Elapsed: time.Second,
		},
	}
	first, err := repository.DiscoverMemoryExtractionWork(context.Background(), residentID, request)
	if err != nil {
		t.Fatal(err)
	}
	assertMemoryDiscoveryResult(t, first, memoryDiscoveryExpectation{
		workSeq: 1, nextAfter: 1, cycleThrough: 1, scanned: 1, pages: 1,
	})

	event2 := insertMemoryDiscoveryEvent(t, fixture, 2, "user_message")
	insertMemoryDiscoveryDialogueCommitB(t, fixture, event2)
	insertTerminalMemoryDiscoveryRun(t, fixture, fixture.event["A"])
	request.Cursor = first.NextCursor
	complete, err := repository.DiscoverMemoryExtractionWork(context.Background(), residentID, request)
	if err != nil {
		t.Fatal(err)
	}
	assertMemoryDiscoveryResult(t, complete, memoryDiscoveryExpectation{pages: 1, cycleComplete: true})

	request.Cursor = nil
	request.Budget = domain.MemoryDiscoveryBudget{
		PageSize: 2, Candidates: 2, Pages: 1, Elapsed: time.Second,
	}
	nextCycle, err := repository.DiscoverMemoryExtractionWork(context.Background(), residentID, request)
	if err != nil {
		t.Fatal(err)
	}
	assertMemoryDiscoveryResult(t, nextCycle, memoryDiscoveryExpectation{
		workSeq: 2, nextAfter: 2, cycleThrough: 2, scanned: 2, pages: 1,
	})
}

func TestCOVR02MemoryDiscoveryUsesTypeIndexedUnionMerge(t *testing.T) {
	fixture, _, residentID, closeFixture := newBoundedMemoryDiscoveryFixture(t)
	defer closeFixture()

	plan := explainMemoryDiscoveryQueryPlan(t, fixture,
		memoryEvidenceEventAscendingQuery,
		residentID.String(), int64(0), int64(100),
		residentID.String(), int64(0), int64(100), 128,
	)
	if !strings.Contains(plan, "MERGE (UNION ALL)") {
		t.Fatalf("memory evidence plan does not merge the two ordered branches:\n%s", plan)
	}
	if count := strings.Count(plan, "idx_events_resident_type_seq"); count != 2 {
		t.Fatalf("memory evidence plan uses the type/seq index %d times, want 2:\n%s", count, plan)
	}
	for _, forbidden := range []string{"USE TEMP B-TREE", "AUTOMATIC", "sqlite_autoindex_events"} {
		if strings.Contains(plan, forbidden) {
			t.Fatalf("memory evidence plan contains %q:\n%s", forbidden, plan)
		}
	}
}

func TestCOVR02MemoryReextractionUsesBoundedPhysicalPlans(t *testing.T) {
	fixture, _, residentID, closeFixture := newBoundedMemoryDiscoveryFixture(t)
	defer closeFixture()
	commitID := fixture.commit["A"]
	runID := fixture.run["A"]

	plans := map[string]string{
		"ceiling": explainMemoryDiscoveryQueryPlan(t, fixture,
			memoryReextractionCycleCeilingQuery, residentID.String()),
		"commit page": explainMemoryDiscoveryQueryPlan(t, fixture,
			memoryReextractionCommitPageQuery, residentID.String(), int64(0), int64(100), 128),
		"initial run page": explainMemoryDiscoveryQueryPlan(t, fixture,
			memoryReextractionInitialRunPageQuery, commitID, residentID.String(), 129),
		"continuation run page": explainMemoryDiscoveryQueryPlan(t, fixture,
			memoryReextractionContinuationRunPageQuery, commitID, residentID.String(), runID, 129),
	}
	if !strings.Contains(plans["ceiling"], "idx_canonical_commits_resident_seq") {
		t.Fatalf("ceiling plan misses resident/commit-seq index:\n%s", plans["ceiling"])
	}
	if !strings.Contains(plans["commit page"], "idx_canonical_commits_resident_seq") ||
		!strings.Contains(plans["commit page"], "idx_generation_runs_commit_resident_purpose_run") {
		t.Fatalf("commit page plan misses a bounded index:\n%s", plans["commit page"])
	}
	for name, plan := range plans {
		if name != "ceiling" && name != "commit page" &&
			!strings.Contains(plan, "idx_generation_runs_commit_resident_purpose_run") {
			t.Fatalf("%s plan misses the run keyset index:\n%s", name, plan)
		}
		for _, forbidden := range []string{"USE TEMP B-TREE", "AUTOMATIC", "SCAN run"} {
			if strings.Contains(plan, forbidden) {
				t.Fatalf("%s plan contains %q:\n%s", name, forbidden, plan)
			}
		}
	}
}

func TestCOVR02MemoryDiscoverySkipsFiftyThousandIrrelevantEventsWithinCandidateBudget(t *testing.T) {
	fixture, repository, residentID, closeFixture := newBoundedMemoryDiscoveryFixture(t)
	defer closeFixture()

	const irrelevantEvents = 50_001
	insertCOVR02IrrelevantResidentMessages(t, fixture, irrelevantEvents)
	targetSeq := int64(irrelevantEvents + 2)
	targetEvent := insertMemoryDiscoveryEvent(t, fixture, targetSeq, "user_message")
	insertMemoryDiscoveryDialogueCommitB(t, fixture, targetEvent)

	after := canonical.Seq(1)
	through := canonical.Seq(targetSeq)
	result, err := repository.DiscoverMemoryExtractionWork(
		context.Background(), residentID, domain.MemoryDiscoveryRequest{
			Cursor:      &domain.MemoryDiscoveryCursor{AfterSeq: &after, CycleThroughSeq: through},
			MaxAttempts: 1,
			Budget: domain.MemoryDiscoveryBudget{
				PageSize: 1, Candidates: 1, Pages: 1, Elapsed: 2 * time.Second,
			},
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	assertMemoryDiscoveryResult(t, result, memoryDiscoveryExpectation{
		workSeq: targetSeq, nextAfter: targetSeq, cycleThrough: targetSeq,
		scanned: 1, pages: 1,
	})
}

func TestMemoryReextractionDiscoveryBoundsSparseHistoryAndFixesCycleCeiling(t *testing.T) {
	fixture, repository, residentID, closeFixture := newBoundedMemoryDiscoveryFixture(t)
	defer closeFixture()

	for offset := int64(1); offset <= 3; offset++ {
		eventID := mustParseSnapshotID(fixture.ids.new())
		insertMemoryReextractionDiscoveryRun(
			t, fixture, semanticTime+offset, domain.MemoryExtractionObligation(eventID), false,
		)
	}
	oldRequestID := mustParseSnapshotID(fixture.ids.new())
	oldKey := domain.MemoryReextractionObligation(mustParseSnapshotID(fixture.event["A"]), oldRequestID)
	oldRunRaw := insertMemoryReextractionDiscoveryRun(t, fixture, semanticTime+4, oldKey, false)

	request := domain.MemoryReextractionDiscoveryRequest{
		MaxAttempts: 1,
		Budget: domain.MemoryDiscoveryBudget{
			PageSize: 2, Candidates: 2, Pages: 1, Elapsed: time.Second,
		},
	}
	first, err := repository.DiscoverMemoryReextractionWork(context.Background(), residentID, request)
	if err != nil {
		t.Fatal(err)
	}
	assertMemoryReextractionDiscoveryResult(t, first, memoryReextractionDiscoveryExpectation{
		hasCursor: true, activeCommit: 2, cycleThrough: 2,
		scanned: 1, pages: 1, budgetExhausted: true,
	})

	newRequestID := mustParseSnapshotID(fixture.ids.new())
	newKey := domain.MemoryReextractionObligation(mustParseSnapshotID(fixture.event["A"]), newRequestID)
	newCommit := insertMemoryDiscoveryCommit(t, fixture, 4)
	insertMemoryReextractionDiscoveryRunAtCommit(
		t, fixture, newCommit, semanticTime+2, newKey, false,
	)

	var oldWork domain.MemoryReextractionDiscoveryResult
	prior := first.NextCursor
	for pass := 0; pass < 32; pass++ {
		request.Cursor = prior
		result, err := repository.DiscoverMemoryReextractionWork(context.Background(), residentID, request)
		if err != nil {
			t.Fatal(err)
		}
		if result.Work != nil {
			oldWork = result
			break
		}
		if !result.BudgetExhausted || result.NextCursor == nil ||
			memoryReextractionCursorsEqual(prior, result.NextCursor) {
			t.Fatalf("pass %d did not make bounded cursor progress: %+v", pass, result)
		}
		if result.NextCursor.CycleThroughCommitSeq != canonical.CommitSeq(2) ||
			result.CandidatesScanned < 1 || result.CandidatesScanned > 2 || result.PageQueries != 1 {
			t.Fatalf("pass %d exceeded fixed sparse-history budget: %+v", pass, result)
		}
		prior = result.NextCursor
	}
	assertMemoryReextractionDiscoveryResult(t, oldWork, memoryReextractionDiscoveryExpectation{
		workKey: oldKey, hasCursor: true, completedCommit: 2, cycleThrough: 2,
		scanned: 1, pages: 1,
	})
	if oldWork.NextCursor.CycleThroughCommitSeq != canonical.CommitSeq(2) {
		t.Fatal("new ingress extended an in-progress re-extraction cycle")
	}

	insertMemoryDiscoveryOutcome(t, fixture, oldRunRaw, "failed", "provider_timeout")
	request.Cursor = oldWork.NextCursor
	complete, err := repository.DiscoverMemoryReextractionWork(context.Background(), residentID, request)
	if err != nil {
		t.Fatal(err)
	}
	assertMemoryReextractionDiscoveryResult(t, complete, memoryReextractionDiscoveryExpectation{
		pages: 1, cycleComplete: true,
	})

	request.Cursor = nil
	prior = nil
	foundNew := false
	for pass := 0; pass < 64; pass++ {
		result, err := repository.DiscoverMemoryReextractionWork(context.Background(), residentID, request)
		if err != nil {
			t.Fatal(err)
		}
		if result.Work != nil {
			assertMemoryReextractionDiscoveryResult(t, result, memoryReextractionDiscoveryExpectation{
				workKey: newKey, hasCursor: true, completedCommit: 4, cycleThrough: 4,
				scanned: 1, pages: 1,
			})
			foundNew = true
			break
		}
		if result.CycleComplete || !result.BudgetExhausted || result.NextCursor == nil ||
			memoryReextractionCursorsEqual(prior, result.NextCursor) {
			t.Fatalf("new-cycle pass %d did not make bounded cursor progress: %+v", pass, result)
		}
		if result.NextCursor.CycleThroughCommitSeq != canonical.CommitSeq(4) ||
			result.CandidatesScanned < 1 || result.CandidatesScanned > 2 || result.PageQueries != 1 {
			t.Fatalf("new-cycle pass %d exceeded fixed sparse-history budget: %+v", pass, result)
		}
		prior = result.NextCursor
		request.Cursor = result.NextCursor
	}
	if !foundNew {
		t.Fatal("new fixed-ceiling cycle did not reach the new re-extraction work")
	}
}

func TestMemoryReextractionDiscoveryResumesAcrossEqualCommitRunIDs(t *testing.T) {
	fixture, repository, residentID, closeFixture := newBoundedMemoryDiscoveryFixture(t)
	defer closeFixture()
	mandatoryRunRaw := insertMemoryReextractionDiscoveryRun(
		t, fixture, semanticTime+20,
		domain.MemoryExtractionObligation(mustParseSnapshotID(fixture.ids.new())), false,
	)
	requestID := mustParseSnapshotID(fixture.ids.new())
	reextractKey := domain.MemoryReextractionObligation(mustParseSnapshotID(fixture.event["A"]), requestID)
	reextractRunRaw := insertMemoryReextractionDiscoveryRun(t, fixture, semanticTime+10, reextractKey, false)
	mandatoryRunID, reextractRunID := mustParseSnapshotID(mandatoryRunRaw), mustParseSnapshotID(reextractRunRaw)
	result, err := repository.DiscoverMemoryReextractionWork(
		context.Background(), residentID, domain.MemoryReextractionDiscoveryRequest{
			Cursor: &domain.MemoryReextractionDiscoveryCursor{
				CycleThroughCommitSeq: canonical.CommitSeq(2),
				ActiveCommit: &domain.MemoryReextractionCommitCursor{
					CommitID:   mustParseSnapshotID(fixture.commit["A"]),
					CommitSeq:  canonical.CommitSeq(2),
					AfterRunID: &mandatoryRunID,
				},
			},
			MaxAttempts: 1,
			Budget: domain.MemoryDiscoveryBudget{
				PageSize: 1, Candidates: 1, Pages: 1, Elapsed: time.Second,
			},
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	assertMemoryReextractionDiscoveryResult(t, result, memoryReextractionDiscoveryExpectation{
		workKey: reextractKey, hasCursor: true, completedCommit: 2, cycleThrough: 2,
		scanned: 1, pages: 1,
	})
	if mandatoryRunID.String() >= reextractRunID.String() {
		t.Fatalf("equal-commit fixture run order = %s then %s", mandatoryRunID, reextractRunID)
	}
}

func TestCOVR02MemoryReextractionLookaheadCompletesTerminalRunPageWithoutEmptyQuery(t *testing.T) {
	fixture, repository, residentID, closeFixture := newBoundedMemoryDiscoveryFixture(t)
	defer closeFixture()
	commitIDRaw := insertMemoryDiscoveryCommit(t, fixture, 4)
	commitID := mustParseSnapshotID(commitIDRaw)
	for offset := int64(0); offset < 3; offset++ {
		key := domain.MemoryReextractionObligation(
			mustParseSnapshotID(fixture.event["A"]), mustParseSnapshotID(fixture.ids.new()),
		)
		insertMemoryReextractionDiscoveryRunAtCommit(
			t, fixture, commitIDRaw, semanticTime+offset, key, true,
		)
	}
	completed := canonical.CommitSeq(2)
	request := domain.MemoryReextractionDiscoveryRequest{
		Cursor: &domain.MemoryReextractionDiscoveryCursor{
			CycleThroughCommitSeq:     canonical.CommitSeq(4),
			CompletedThroughCommitSeq: &completed,
			ActiveCommit: &domain.MemoryReextractionCommitCursor{
				CommitID: commitID, CommitSeq: canonical.CommitSeq(4),
			},
		},
		MaxAttempts: 1,
		Budget: domain.MemoryDiscoveryBudget{
			PageSize: 2, Candidates: 2, Pages: 1, Elapsed: time.Second,
		},
	}
	first, err := repository.DiscoverMemoryReextractionWork(context.Background(), residentID, request)
	if err != nil {
		t.Fatal(err)
	}
	if !first.BudgetExhausted || first.CandidatesScanned != 2 || first.PageQueries != 1 ||
		first.NextCursor == nil || first.NextCursor.ActiveCommit == nil ||
		first.NextCursor.ActiveCommit.AfterRunID == nil {
		t.Fatalf("first terminal run page = %+v", first)
	}

	request.Cursor = first.NextCursor
	second, err := repository.DiscoverMemoryReextractionWork(context.Background(), residentID, request)
	if err != nil {
		t.Fatal(err)
	}
	assertMemoryReextractionDiscoveryResult(t, second, memoryReextractionDiscoveryExpectation{
		hasCursor: true, completedCommit: 4, cycleThrough: 4,
		scanned: 1, pages: 1, budgetExhausted: true,
	})
}

func TestCOVR02MemoryReextractionSparseEmptyCommitsAndOversizedRunPageProgress(t *testing.T) {
	fixture, repository, residentID, closeFixture := newBoundedMemoryDiscoveryFixture(t)
	defer closeFixture()

	for commitSeq := int64(4); commitSeq <= 13; commitSeq++ {
		insertMemoryDiscoveryCommit(t, fixture, commitSeq)
	}
	activeCommit := insertMemoryDiscoveryCommit(t, fixture, 14)
	for offset := int64(0); offset < 6; offset++ {
		key := domain.MemoryReextractionObligation(
			mustParseSnapshotID(fixture.event["A"]), mustParseSnapshotID(fixture.ids.new()),
		)
		insertMemoryReextractionDiscoveryRunAtCommit(
			t, fixture, activeCommit, semanticTime+offset, key, true,
		)
	}
	targetKey := domain.MemoryReextractionObligation(
		mustParseSnapshotID(fixture.event["A"]), mustParseSnapshotID(fixture.ids.new()),
	)
	insertMemoryReextractionDiscoveryRunAtCommit(
		t, fixture, activeCommit, semanticTime+10, targetKey, false,
	)

	completed := canonical.CommitSeq(2)
	request := domain.MemoryReextractionDiscoveryRequest{
		Cursor: &domain.MemoryReextractionDiscoveryCursor{
			CycleThroughCommitSeq: canonical.CommitSeq(14), CompletedThroughCommitSeq: &completed,
		},
		MaxAttempts: 1,
		Budget: domain.MemoryDiscoveryBudget{
			PageSize: 2, Candidates: 2, Pages: 1, Elapsed: time.Second,
		},
	}
	prior := request.Cursor
	totalPositions := 0
	passes := 0
	found := false
	for ; passes < 20; passes++ {
		result, err := repository.DiscoverMemoryReextractionWork(context.Background(), residentID, request)
		if err != nil {
			t.Fatal(err)
		}
		totalPositions += result.CandidatesScanned
		if result.Work != nil {
			assertMemoryReextractionDiscoveryResult(t, result, memoryReextractionDiscoveryExpectation{
				workKey: targetKey, hasCursor: true, completedCommit: 14, cycleThrough: 14,
				scanned: 1, pages: 1,
			})
			found = true
			passes++
			break
		}
		if result.CycleComplete || !result.BudgetExhausted || result.NextCursor == nil ||
			memoryReextractionCursorsEqual(prior, result.NextCursor) {
			t.Fatalf("sparse pass %d did not advance: %+v", passes, result)
		}
		if result.NextCursor.CycleThroughCommitSeq != canonical.CommitSeq(14) ||
			result.CandidatesScanned < 1 || result.CandidatesScanned > 2 || result.PageQueries != 1 {
			t.Fatalf("sparse pass %d exceeded its physical budget: %+v", passes, result)
		}
		prior = result.NextCursor
		request.Cursor = result.NextCursor
	}
	if !found || totalPositions != 18 || passes != 10 {
		t.Fatalf("sparse discovery completion = found:%t positions:%d passes:%d, want true/18/10",
			found, totalPositions, passes)
	}
}

func TestMemoryReextractionDiscoveryCancellationAndRepositoryErrorsDoNotAdvance(t *testing.T) {
	t.Run("caller cancellation", func(t *testing.T) {
		_, repository, residentID, closeFixture := newBoundedMemoryDiscoveryFixture(t)
		defer closeFixture()
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		result, err := repository.DiscoverMemoryReextractionWork(ctx, residentID,
			domain.MemoryReextractionDiscoveryRequest{
				MaxAttempts: 1,
				Budget: domain.MemoryDiscoveryBudget{
					PageSize: 1, Candidates: 1, Pages: 1, Elapsed: time.Second,
				},
			})
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("discovery error = %v, want context.Canceled", err)
		}
		if result != (domain.MemoryReextractionDiscoveryResult{}) {
			t.Fatalf("caller cancellation advanced progress: %+v", result)
		}
	})

	t.Run("repository error", func(t *testing.T) {
		_, repository, residentID, closeFixture := newBoundedMemoryDiscoveryFixture(t)
		closeFixture()
		result, err := repository.DiscoverMemoryReextractionWork(context.Background(), residentID,
			domain.MemoryReextractionDiscoveryRequest{
				MaxAttempts: 1,
				Budget: domain.MemoryDiscoveryBudget{
					PageSize: 1, Candidates: 1, Pages: 1, Elapsed: time.Second,
				},
			})
		if err == nil {
			t.Fatal("closed repository discovery unexpectedly succeeded")
		}
		if result != (domain.MemoryReextractionDiscoveryResult{}) {
			t.Fatalf("repository error advanced progress: %+v", result)
		}
	})
}

func TestMemoryReextractionInternalDeadlineReturnsPriorCursorProgress(t *testing.T) {
	runID := mustParseSnapshotID("00000000000000000000000001")
	initialCursor := &domain.MemoryReextractionDiscoveryCursor{
		CycleThroughCommitSeq: canonical.CommitSeq(10),
		ActiveCommit: &domain.MemoryReextractionCommitCursor{
			CommitID: runID, CommitSeq: canonical.CommitSeq(10),
		},
	}
	cursor := &domain.MemoryReextractionDiscoveryCursor{
		CycleThroughCommitSeq: canonical.CommitSeq(10),
		ActiveCommit: &domain.MemoryReextractionCommitCursor{
			CommitID: runID, CommitSeq: canonical.CommitSeq(10), AfterRunID: &runID,
		},
	}
	scanCtx, cancel := context.WithDeadline(context.Background(), time.Now().Add(-time.Second))
	defer cancel()
	result, err := boundedMemoryReextractionDiscoveryFailure(
		context.Background(), scanCtx,
		domain.MemoryReextractionDiscoveryResult{CandidatesScanned: 3, PageQueries: 1},
		initialCursor, cursor, context.DeadlineExceeded,
	)
	if err != nil {
		t.Fatal(err)
	}
	if !result.BudgetExhausted || result.NextCursor == nil || result.NextCursor.ActiveCommit == nil ||
		result.NextCursor.ActiveCommit.AfterRunID == nil ||
		*result.NextCursor.ActiveCommit.AfterRunID != runID ||
		result.CandidatesScanned != 3 || result.PageQueries != 1 {
		t.Fatalf("internal deadline progress = %+v", result)
	}
}

func TestMemoryReextractionInternalDeadlineAfterCeilingReturnsCursorProgress(t *testing.T) {
	cursor := &domain.MemoryReextractionDiscoveryCursor{
		CycleThroughCommitSeq: canonical.CommitSeq(10),
	}
	scanCtx, cancel := context.WithDeadline(context.Background(), time.Now().Add(-time.Second))
	defer cancel()
	result, err := boundedMemoryReextractionDiscoveryFailure(
		context.Background(), scanCtx, domain.MemoryReextractionDiscoveryResult{},
		nil, cursor, context.DeadlineExceeded,
	)
	if err != nil {
		t.Fatal(err)
	}
	if !result.BudgetExhausted || result.NextCursor == nil ||
		result.NextCursor.CycleThroughCommitSeq != canonical.CommitSeq(10) ||
		result.NextCursor.CompletedThroughCommitSeq != nil || result.NextCursor.ActiveCommit != nil ||
		result.CandidatesScanned != 0 || result.PageQueries != 0 {
		t.Fatalf("deadline after ceiling progress = %+v", result)
	}
}

func TestMemoryReextractionInternalDeadlineBeforeCursorIsAnError(t *testing.T) {
	scanCtx, cancel := context.WithDeadline(context.Background(), time.Now().Add(-time.Second))
	defer cancel()
	result, err := boundedMemoryReextractionDiscoveryFailure(
		context.Background(), scanCtx, domain.MemoryReextractionDiscoveryResult{}, nil, nil,
		context.DeadlineExceeded,
	)
	if err == nil {
		t.Fatal("deadline before fixed cursor unexpectedly succeeded")
	}
	if result != (domain.MemoryReextractionDiscoveryResult{}) {
		t.Fatalf("deadline before fixed cursor advanced progress: %+v", result)
	}
}

func TestMemoryReextractionInternalDeadlineWithoutCursorAdvanceIsAnError(t *testing.T) {
	commitID := mustParseSnapshotID("00000000000000000000000001")
	cursor := &domain.MemoryReextractionDiscoveryCursor{
		CycleThroughCommitSeq: canonical.CommitSeq(10),
		ActiveCommit: &domain.MemoryReextractionCommitCursor{
			CommitID: commitID, CommitSeq: canonical.CommitSeq(10),
		},
	}
	scanCtx, cancel := context.WithDeadline(context.Background(), time.Now().Add(-time.Second))
	defer cancel()
	result, err := boundedMemoryReextractionDiscoveryFailure(
		context.Background(), scanCtx, domain.MemoryReextractionDiscoveryResult{},
		cursor, cloneMemoryReextractionDiscoveryCursor(cursor), context.DeadlineExceeded,
	)
	if err == nil {
		t.Fatal("deadline without cursor advance unexpectedly succeeded")
	}
	if result != (domain.MemoryReextractionDiscoveryResult{}) {
		t.Fatalf("deadline without cursor advance returned progress: %+v", result)
	}
}

func TestMemoryReextractionDiscoveryOrdersByCanonicalCommitDespiteRequestedAtRegression(t *testing.T) {
	fixture, repository, residentID, closeFixture := newBoundedMemoryDiscoveryFixture(t)
	defer closeFixture()
	earlyCommit := insertMemoryDiscoveryCommit(t, fixture, 4)
	lateCommit := insertMemoryDiscoveryCommit(t, fixture, 5)
	insertMemoryReextractionDiscoveryRunAtCommit(
		t, fixture, earlyCommit, semanticTime+100,
		domain.MemoryExtractionObligation(mustParseSnapshotID(fixture.ids.new())), false,
	)
	requestID := mustParseSnapshotID(fixture.ids.new())
	reextractKey := domain.MemoryReextractionObligation(mustParseSnapshotID(fixture.event["A"]), requestID)
	insertMemoryReextractionDiscoveryRunAtCommit(
		t, fixture, lateCommit, semanticTime-100, reextractKey, false,
	)
	request := domain.MemoryReextractionDiscoveryRequest{
		MaxAttempts: 1,
		Budget: domain.MemoryDiscoveryBudget{
			PageSize: 2, Candidates: 2, Pages: 1, Elapsed: time.Second,
		},
	}
	var prior *domain.MemoryReextractionDiscoveryCursor
	sawCompletedEarlyCommit := false
	foundLateWork := false
	for pass := 0; pass < 64; pass++ {
		request.Cursor = prior
		result, err := repository.DiscoverMemoryReextractionWork(context.Background(), residentID, request)
		if err != nil {
			t.Fatal(err)
		}
		if result.NextCursor != nil && result.NextCursor.CompletedThroughCommitSeq != nil &&
			*result.NextCursor.CompletedThroughCommitSeq >= canonical.CommitSeq(4) {
			sawCompletedEarlyCommit = true
		}
		if result.Work != nil {
			assertMemoryReextractionDiscoveryResult(t, result, memoryReextractionDiscoveryExpectation{
				workKey: reextractKey, hasCursor: true, completedCommit: 5, cycleThrough: 5,
				scanned: 1, pages: 1,
			})
			foundLateWork = true
			break
		}
		if result.CycleComplete || !result.BudgetExhausted || result.NextCursor == nil ||
			memoryReextractionCursorsEqual(prior, result.NextCursor) {
			t.Fatalf("commit-order pass %d did not make bounded cursor progress: %+v", pass, result)
		}
		if result.NextCursor.CycleThroughCommitSeq != canonical.CommitSeq(5) ||
			result.CandidatesScanned < 1 || result.CandidatesScanned > 2 || result.PageQueries != 1 {
			t.Fatalf("commit-order pass %d exceeded budget: %+v", pass, result)
		}
		prior = result.NextCursor
	}
	if !foundLateWork || !sawCompletedEarlyCommit {
		t.Fatalf("canonical commit order was not observed: found late=%t completed early=%t",
			foundLateWork, sawCompletedEarlyCommit)
	}
}

type memoryReextractionDiscoveryExpectation struct {
	workKey                        string
	hasCursor                      bool
	completedCommit, activeCommit  int64
	afterRun                       canonical.ID
	cycleThrough                   int64
	scanned, pages                 int
	cycleComplete, budgetExhausted bool
}

func explainMemoryDiscoveryQueryPlan(
	t *testing.T,
	fixture *semanticFixture,
	query string,
	args ...any,
) string {
	t.Helper()
	rows, err := fixture.db.Query("EXPLAIN QUERY PLAN "+query, args...)
	if err != nil {
		t.Fatalf("explain query plan: %v\n%s", err, query)
	}
	defer rows.Close()
	var details []string
	for rows.Next() {
		var id, parent, unused int
		var detail string
		if err := rows.Scan(&id, &parent, &unused, &detail); err != nil {
			t.Fatal(err)
		}
		details = append(details, fmt.Sprintf("%d/%d/%d %s", id, parent, unused, detail))
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	return strings.Join(details, "\n")
}

func assertMemoryReextractionDiscoveryResult(
	t *testing.T,
	result domain.MemoryReextractionDiscoveryResult,
	want memoryReextractionDiscoveryExpectation,
) {
	t.Helper()
	if want.workKey == "" {
		if result.Work != nil {
			t.Fatalf("work = %+v, want nil", result.Work)
		}
	} else if result.Work == nil || result.Work.IdempotencyKey != want.workKey {
		t.Fatalf("work = %+v, want key %q", result.Work, want.workKey)
	}
	if !want.hasCursor {
		if result.NextCursor != nil {
			t.Fatalf("next cursor = %+v, want nil", result.NextCursor)
		}
	} else if result.NextCursor == nil {
		t.Fatal("next cursor = nil, want bounded progress")
	} else {
		if result.NextCursor.CycleThroughCommitSeq.Int64() != want.cycleThrough {
			t.Fatalf("next cursor cycle = %+v, want %d", result.NextCursor, want.cycleThrough)
		}
		if want.completedCommit == 0 {
			if result.NextCursor.CompletedThroughCommitSeq != nil {
				t.Fatalf("completed progress = %+v, want nil", result.NextCursor.CompletedThroughCommitSeq)
			}
		} else if result.NextCursor.CompletedThroughCommitSeq == nil ||
			result.NextCursor.CompletedThroughCommitSeq.Int64() != want.completedCommit {
			t.Fatalf("completed progress = %+v, want %d", result.NextCursor, want.completedCommit)
		}
		if want.activeCommit == 0 {
			if result.NextCursor.ActiveCommit != nil {
				t.Fatalf("active commit = %+v, want nil", result.NextCursor.ActiveCommit)
			}
		} else if result.NextCursor.ActiveCommit == nil ||
			result.NextCursor.ActiveCommit.CommitSeq.Int64() != want.activeCommit {
			t.Fatalf("active commit = %+v, want %d", result.NextCursor, want.activeCommit)
		} else if want.afterRun == (canonical.ID{}) {
			if result.NextCursor.ActiveCommit.AfterRunID != nil {
				t.Fatalf("active run progress = %+v, want nil", result.NextCursor.ActiveCommit)
			}
		} else if result.NextCursor.ActiveCommit.AfterRunID == nil ||
			*result.NextCursor.ActiveCommit.AfterRunID != want.afterRun {
			t.Fatalf("active run progress = %+v, want %s", result.NextCursor.ActiveCommit, want.afterRun)
		}
	}
	if result.CandidatesScanned != want.scanned || result.PageQueries != want.pages ||
		result.CycleComplete != want.cycleComplete || result.BudgetExhausted != want.budgetExhausted {
		t.Fatalf("re-extraction discovery metadata = %+v, want %+v", result, want)
	}
}

type memoryDiscoveryExpectation struct {
	workSeq, nextAfter, cycleThrough int64
	scanned, pages                   int
	dialogueDeferred                 int
	cycleComplete, budgetExhausted   bool
}

func memoryDiscoveryThrough(value int64) *canonical.Seq {
	through := canonical.Seq(value)
	return &through
}

func assertMemoryDiscoveryResult(t *testing.T, result domain.MemoryDiscoveryResult, want memoryDiscoveryExpectation) {
	t.Helper()
	if want.workSeq == 0 {
		if result.Work != nil {
			t.Fatalf("work = %+v, want nil", result.Work)
		}
	} else if result.Work == nil || result.Work.SourceEvent.Seq.Int64() != want.workSeq {
		t.Fatalf("work = %+v, want seq %d", result.Work, want.workSeq)
	}
	if want.nextAfter == 0 {
		if result.NextCursor != nil {
			t.Fatalf("next cursor = %+v, want nil", result.NextCursor)
		}
	} else {
		if result.NextCursor == nil || result.NextCursor.AfterSeq == nil ||
			result.NextCursor.AfterSeq.Int64() != want.nextAfter ||
			result.NextCursor.CycleThroughSeq.Int64() != want.cycleThrough {
			t.Fatalf("next cursor = %+v, want after/cycle %d/%d", result.NextCursor, want.nextAfter, want.cycleThrough)
		}
	}
	if result.CandidatesScanned != want.scanned || result.PageQueries != want.pages ||
		result.DialogueDeferred != want.dialogueDeferred || result.CycleComplete != want.cycleComplete ||
		result.BudgetExhausted != want.budgetExhausted {
		t.Fatalf("discovery metadata = %+v, want %+v", result, want)
	}
}

func newBoundedMemoryDiscoveryFixture(
	t *testing.T,
) (*semanticFixture, *CanonicalRepository, canonical.ID, func()) {
	t.Helper()
	fixture, closeFixture := newSemanticFixture(t)
	repository := dialogueDiscoveryRepository(t, fixture)
	mustExec(t, fixture.db, `INSERT INTO resident_revision_activations(
		activation_id, canonical_commit_id, resident_id, revision_id, actor_principal_id,
		approval_id, reason_code, reason_content_id, recorded_at, recorded_tz
	) VALUES (?, ?, ?, ?, ?, NULL, 'memory_discovery_fixture', NULL, ?, ?)`,
		fixture.ids.new(), fixture.commit["A"], fixture.resident["A"],
		fixture.revision["A"]["memory_policy"], fixture.principal["human"], semanticTime, semanticTZ)
	dialoguePipelineID := fixture.ids.new()
	mustExec(t, fixture.db, `INSERT INTO pipeline_versions(
		pipeline_version_id, canonical_commit_id, pipeline_kind, version_key, definition, recorded_at, recorded_tz
	) VALUES (?, ?, 'dialogue', ?, '{}', ?, ?)`, dialoguePipelineID, fixture.commit["global"],
		domain.DialoguePipelineVersionV1, semanticTime, semanticTZ)
	mustExec(t, fixture.db, `INSERT INTO sessionization_policy_versions(
		sessionization_policy_version_id, canonical_commit_id, version_key, definition, recorded_at, recorded_tz
	) VALUES (?, ?, ?, '{}', ?, ?)`, fixture.ids.new(), fixture.commit["global"],
		domain.SessionPolicyVersion, semanticTime, semanticTZ)
	return fixture, repository, mustParseSnapshotID(fixture.resident["A"]), closeFixture
}

const covr02FiveDigitNumbersCTE = `WITH digits(value) AS (
	VALUES (0), (1), (2), (3), (4), (5), (6), (7), (8), (9)
), numbers(value) AS (
	SELECT ones.value + 10*tens.value + 100*hundreds.value +
		1000*thousands.value + 10000*ten_thousands.value
	FROM digits ones
	CROSS JOIN digits tens
	CROSS JOIN digits hundreds
	CROSS JOIN digits thousands
	CROSS JOIN digits ten_thousands
) `

func insertCOVR02IrrelevantResidentMessages(t *testing.T, fixture *semanticFixture, count int) {
	t.Helper()
	if count < 1 || count > 100_000 {
		t.Fatalf("invalid COVR-02 sparse event count %d", count)
	}
	var initialPreviousHash []byte
	if err := fixture.db.QueryRow(`SELECT event_hash FROM events
		WHERE resident_id = ? AND seq = 1`, fixture.resident["A"]).Scan(&initialPreviousHash); err != nil {
		t.Fatal(err)
	}

	tx, err := fixture.db.Begin()
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	mustExec(t, tx, covr02FiveDigitNumbersCTE+`INSERT INTO generation_runs
		SELECT printf('%026d', 1000000 + numbers.value),
			source.canonical_commit_id, source.resident_id, source.purpose,
			'covr02-irrelevant:' || numbers.value,
			source.provider, source.model, source.model_version,
			source.prompt_template_version, source.pipeline_version_id,
			source.context_policy_version, source.sessionization_policy_version_id,
			source.memory_rendering_version, source.principles_revision_id,
			source.persona_revision_id, source.memory_policy_revision_id,
			source.recall_run_id, source.temperature, source.top_p, source.max_tokens,
			source.seed, source.generator_params, source.as_of, source.as_of_tz,
			source.budget_exceeded, source.dropped_input_summary,
			source.requested_at, source.requested_tz
		FROM numbers
		JOIN generation_runs source ON source.generation_run_id = ?
		WHERE numbers.value < ?`, fixture.run["A"], count)
	mustExec(t, tx, covr02FiveDigitNumbersCTE+`INSERT INTO events(
		event_id, canonical_commit_id, resident_id, seq, event_type, visibility,
		delivery_screen, delivery_audio, ingress, trust_level, actor_principal_id,
		target_principal_id, generation_run_id, occurred_at, occurred_tz,
		recorded_at, recorded_tz, content_id, payload_commitment, prev_event_hash,
		event_hash, event_hash_algorithm, event_hash_domain, canonicalization_version
	) SELECT
		printf('%026d', 2000000 + numbers.value), ?, ?, numbers.value + 2,
		'resident_message', 'conversation', 1, 0, 'resident_runtime', 'trusted', ?, ?,
		printf('%026d', 1000000 + numbers.value), ? + numbers.value, ?,
		? + numbers.value, ?, ?,
		CAST(printf('%032d', 4000000 + numbers.value) AS BLOB),
		CASE WHEN numbers.value = 0 THEN ?
			ELSE CAST(printf('%032d', 3000000 + numbers.value - 1) AS BLOB) END,
		CAST(printf('%032d', 3000000 + numbers.value) AS BLOB),
		'sha256', 'mahoroba:event-hash:v1', 'mahoroba-jcs-v1'
	FROM numbers WHERE numbers.value < ? ORDER BY numbers.value`,
		fixture.commit["A"], fixture.resident["A"],
		fixture.principal["A"], fixture.principal["human"],
		semanticTime, semanticTZ, semanticTime, semanticTZ, fixture.content["A"]["event"],
		initialPreviousHash, count)
	if err := tx.Commit(); err != nil {
		t.Fatalf("commit COVR-02 sparse events: %v", err)
	}
}

func insertMemoryDiscoveryEvent(t *testing.T, fixture *semanticFixture, seq int64, eventType string) string {
	return insertMemoryDiscoveryEventAtCommit(t, fixture, seq, eventType, fixture.commit["A"])
}

func insertMemoryDiscoveryEventAtCommit(
	t *testing.T,
	fixture *semanticFixture,
	seq int64,
	eventType string,
	commitID string,
) string {
	t.Helper()
	var previousHash []byte
	if err := fixture.db.QueryRow(
		`SELECT event_hash FROM events WHERE resident_id = ? AND seq = ?`, fixture.resident["A"], seq-1,
	).Scan(&previousHash); err != nil {
		t.Fatalf("resolve previous event hash for seq %d: %v", seq, err)
	}
	eventID := fixture.ids.new()
	contentID := fixture.addContent(t, "A", "event_payload", fmt.Sprintf("memory-discovery-%d", seq), "independent")
	visibility, deliveryScreen, ingress := "conversation", 1, "local_ui"
	actorID := fixture.principal["human"]
	var generationRunID any
	if eventType == "self_talk" {
		visibility, deliveryScreen, ingress = "internal", 0, "resident_runtime"
		actorID = fixture.principal["A"]
		generationRunID = insertMemoryDiscoverySelfTalkSourceRun(t, fixture, commitID, eventID)
	}
	mustExec(t, fixture.db, "INSERT INTO events VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
		eventID, commitID, fixture.resident["A"], seq, eventType, visibility, deliveryScreen, 0,
		ingress, "trusted", actorID, fixture.principal["A"], generationRunID,
		semanticTime, semanticTZ, semanticTime, semanticTZ, contentID,
		semanticDigest(fmt.Sprintf("memory-discovery-payload-%d", seq)), previousHash,
		semanticDigest(fmt.Sprintf("memory-discovery-event-%d", seq)), "sha256",
		"mahoroba:event-hash:v1", "mahoroba-jcs-v1")
	fixture.event[fmt.Sprintf("seq-%d", seq)] = eventID
	return eventID
}

func insertMemoryDiscoverySelfTalkSourceRun(
	t *testing.T,
	fixture *semanticFixture,
	commitID string,
	eventID string,
) string {
	t.Helper()
	versions, err := domain.GenerationVersionsForPurpose(domain.GenerationPurposeSelfTalk)
	if err != nil {
		t.Fatal(err)
	}
	var memoryPolicyRevisionID string
	if err := fixture.db.QueryRow(`SELECT revision_id FROM resident_revisions
		WHERE canonical_commit_id = ? AND resident_id = ? AND revision_class = 'memory_policy'`,
		commitID, fixture.resident["A"]).Scan(&memoryPolicyRevisionID); err != nil {
		t.Fatalf("resolve self-talk source memory policy: %v", err)
	}
	runID := fixture.ids.new()
	mustExec(t, fixture.db, "INSERT INTO generation_runs VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
		runID, commitID, fixture.resident["A"], string(domain.GenerationPurposeSelfTalk),
		"self-talk-source:"+eventID, "test", "model", nil,
		versions.PromptTemplateVersion, fixture.pipeline, versions.ContextPolicyVersion, fixture.session,
		versions.MemoryRenderingVersion, fixture.revision["A"]["principles"], fixture.revision["A"]["persona"],
		memoryPolicyRevisionID, nil, nil, nil, nil, nil, "{}",
		semanticTime, semanticTZ, 0, "{}", semanticTime, semanticTZ)
	return runID
}

func activateMemoryDiscoveryPolicyV3(t *testing.T, fixture *semanticFixture) string {
	t.Helper()
	commitID := fixture.ids.new()
	mustExec(t, fixture.db, `INSERT INTO canonical_commits(
		canonical_commit_id, commit_seq, resident_id, committed_at, committed_tz
	) VALUES (?, 4, ?, ?, ?)`, commitID, fixture.resident["A"], semanticTime+4, semanticTZ)
	encoded, err := memory.DefaultPolicyV3().CanonicalJSON()
	if err != nil {
		t.Fatal(err)
	}
	contentID := fixture.addContent(t, "A", "memory_policy_text", encoded.String(), "resident_only")
	revisionID := fixture.ids.new()
	mustExec(t, fixture.db, `INSERT INTO resident_revisions(
		revision_id, canonical_commit_id, resident_id, revision_class, content_id,
		parent_revision_id, created_by_run_id, reason_content_id, recorded_at, recorded_tz
	) VALUES (?, ?, ?, 'memory_policy', ?, ?, NULL, NULL, ?, ?)`,
		revisionID, commitID, fixture.resident["A"], contentID, fixture.revision["A"]["memory_policy"],
		semanticTime+4, semanticTZ)
	mustExec(t, fixture.db, `INSERT INTO resident_revision_activations(
		activation_id, canonical_commit_id, resident_id, revision_id, actor_principal_id,
		approval_id, reason_code, reason_content_id, recorded_at, recorded_tz
	) VALUES (?, ?, ?, ?, ?, NULL, 'memory_discovery_v3', NULL, ?, ?)`,
		fixture.ids.new(), commitID, fixture.resident["A"], revisionID, fixture.principal["human"],
		semanticTime+4, semanticTZ)
	return commitID
}

func insertMemoryDiscoveryDialogueCommitB(t *testing.T, fixture *semanticFixture, eventID string) {
	t.Helper()
	mustExec(t, fixture.db, "INSERT INTO generation_runs VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
		fixture.ids.new(), fixture.commit["A"], fixture.resident["A"], "dialogue",
		domain.DialogueObligation(mustParseSnapshotID(eventID)), "test", "model", nil,
		"prompt-v1", fixture.pipeline, "context-v1", fixture.session, "render-v1",
		fixture.revision["A"]["principles"], fixture.revision["A"]["persona"],
		fixture.revision["A"]["memory_policy"], nil, nil, nil, nil, nil, "{}",
		semanticTime, semanticTZ, 0, "{}", semanticTime, semanticTZ)
}

func insertTerminalMemoryDiscoveryRun(t *testing.T, fixture *semanticFixture, eventID string) {
	t.Helper()
	runID := fixture.ids.new()
	mustExec(t, fixture.db, "INSERT INTO generation_runs VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
		runID, fixture.commit["A"], fixture.resident["A"], "memory_extraction",
		domain.MemoryExtractionObligation(mustParseSnapshotID(eventID)), "test", "model", nil,
		"prompt-v1", fixture.pipeline, "context-v1", fixture.session, "render-v1",
		fixture.revision["A"]["principles"], fixture.revision["A"]["persona"],
		fixture.revision["A"]["memory_policy"], nil, nil, nil, nil, nil, "{}",
		semanticTime, semanticTZ, 0, "{}", semanticTime, semanticTZ)
	insertMemoryDiscoveryRunInput(t, fixture, runID, eventID)
	insertMemoryDiscoveryOutcome(t, fixture, runID, "running", nil)
	insertMemoryDiscoveryOutcome(t, fixture, runID, "failed", "provider_timeout")
}

func insertPreemptedMemoryDiscoveryRun(t *testing.T, fixture *semanticFixture, eventID string) {
	t.Helper()
	runID := fixture.ids.new()
	mustExec(t, fixture.db, "INSERT INTO generation_runs VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
		runID, fixture.commit["A"], fixture.resident["A"], "memory_extraction",
		domain.MemoryExtractionObligation(mustParseSnapshotID(eventID)), "test", "model", nil,
		"prompt-v1", fixture.pipeline, "context-v1", fixture.session, "render-v1",
		fixture.revision["A"]["principles"], fixture.revision["A"]["persona"],
		fixture.revision["A"]["memory_policy"], nil, nil, nil, nil, nil, "{}",
		semanticTime, semanticTZ, 0, "{}", semanticTime, semanticTZ)
	insertMemoryDiscoveryRunInput(t, fixture, runID, eventID)
	insertMemoryDiscoveryOutcome(t, fixture, runID, "running", nil)
	insertMemoryDiscoveryOutcome(t, fixture, runID, "cancelled", "foreground_preempted")
}

func insertMemoryReextractionDiscoveryRun(
	t *testing.T,
	fixture *semanticFixture,
	requestedAt int64,
	idempotencyKey string,
	terminal bool,
) string {
	return insertMemoryReextractionDiscoveryRunAtCommit(
		t, fixture, fixture.commit["A"], requestedAt, idempotencyKey, terminal,
	)
}

func insertMemoryReextractionDiscoveryRunAtCommit(
	t *testing.T,
	fixture *semanticFixture,
	commitID string,
	requestedAt int64,
	idempotencyKey string,
	terminal bool,
) string {
	t.Helper()
	runID := fixture.ids.new()
	mustExec(t, fixture.db, "INSERT INTO generation_runs VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
		runID, commitID, fixture.resident["A"], "memory_extraction",
		idempotencyKey, "test", "model", nil,
		"prompt-v1", fixture.pipeline, "context-v1", fixture.session, "render-v1",
		fixture.revision["A"]["principles"], fixture.revision["A"]["persona"],
		fixture.revision["A"]["memory_policy"], nil, nil, nil, nil, nil, "{}",
		semanticTime, semanticTZ, 0, "{}", requestedAt, semanticTZ)
	insertMemoryDiscoveryRunInputAtCommit(t, fixture, commitID, runID, fixture.event["A"])
	insertMemoryDiscoveryOutcomeAtCommit(t, fixture, commitID, runID, "running", nil)
	if terminal {
		insertMemoryDiscoveryOutcomeAtCommit(t, fixture, commitID, runID, "failed", "provider_timeout")
	}
	return runID
}

func insertMemoryDiscoveryRunInput(t *testing.T, fixture *semanticFixture, runID, eventID string) {
	insertMemoryDiscoveryRunInputAtCommit(t, fixture, fixture.commit["A"], runID, eventID)
}

func insertMemoryDiscoveryRunInputAtCommit(
	t *testing.T,
	fixture *semanticFixture,
	commitID string,
	runID string,
	eventID string,
) {
	t.Helper()
	var contentID string
	if err := fixture.db.QueryRow(`SELECT content_id FROM events WHERE event_id = ?`, eventID).Scan(&contentID); err != nil {
		t.Fatalf("resolve memory discovery input content: %v", err)
	}
	mustExec(t, fixture.db, `INSERT INTO generation_run_inputs(
		generation_run_input_id, canonical_commit_id, generation_run_id, ordinal,
		role, source_type, source_id, inclusion_mode, content_id, recorded_at, recorded_tz
	) VALUES (?, ?, ?, 0, 'user', 'event', ?, 'current_input', ?, ?, ?)`,
		fixture.ids.new(), commitID, runID, eventID, contentID, semanticTime, semanticTZ)
}

func insertMemoryDiscoveryOutcome(t *testing.T, fixture *semanticFixture, runID, state string, errorClass any) {
	insertMemoryDiscoveryOutcomeAtCommit(t, fixture, fixture.commit["A"], runID, state, errorClass)
}

func insertMemoryDiscoveryOutcomeAtCommit(
	t *testing.T,
	fixture *semanticFixture,
	commitID string,
	runID string,
	state string,
	errorClass any,
) {
	t.Helper()
	mustExec(t, fixture.db, `INSERT INTO generation_run_outcomes(
		outcome_id, canonical_commit_id, generation_run_id, attempt_no, state,
		output_content_id, prompt_tokens, completion_tokens, latency, estimated_cost,
		error_class, error_detail_content_id, recorded_at, recorded_tz
	) VALUES (?, ?, ?, 1, ?, NULL, NULL, NULL, NULL, NULL, ?, NULL, ?, ?)`,
		fixture.ids.new(), commitID, runID, state, errorClass, semanticTime, semanticTZ)
}

func insertMemoryDiscoveryCommit(t *testing.T, fixture *semanticFixture, commitSeq int64) string {
	t.Helper()
	commitID := fixture.ids.new()
	mustExec(t, fixture.db, `INSERT INTO canonical_commits(
		canonical_commit_id, commit_seq, resident_id, committed_at, committed_tz
	) VALUES (?, ?, ?, ?, ?)`, commitID, commitSeq, fixture.resident["A"], semanticTime+commitSeq, semanticTZ)
	return commitID
}
