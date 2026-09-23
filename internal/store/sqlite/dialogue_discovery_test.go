package sqlite

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"mahoroba.local/mahoroba/internal/canonical"
	"mahoroba.local/mahoroba/internal/domain"
)

func TestDialogueDiscoveryAdvancesBoundedPagesAndHoldsActionableBoundary(t *testing.T) {
	fixture, closeFixture := newSemanticFixture(t)
	defer closeFixture()
	repository := dialogueDiscoveryRepository(t, fixture)

	for seq := int64(2); seq <= 7; seq++ {
		eventID, contentID := insertDialogueDiscoveryEvent(t, fixture, seq)
		insertTerminalDialogueDiscoveryRun(t, fixture, eventID, contentID)
	}

	request := domain.DialogueDiscoveryRequest{
		MaxAttempts: 1,
		Budget: domain.DialogueDiscoveryBudget{
			RecentCandidates: 2,
			OlderPageSize:    2,
			OlderCandidates:  4,
			OlderPages:       2,
			Elapsed:          time.Second,
		},
	}
	residentID := mustParseSnapshotID(fixture.resident["A"])
	first, err := repository.DiscoverDialogueWork(context.Background(), residentID, request)
	if err != nil {
		t.Fatal(err)
	}
	assertDialogueDiscoveryResult(t, first, dialogueDiscoveryExpectation{
		seqs: []int64{6, 7}, nextBeforeSeq: 2, recentScanned: 2,
		olderScanned: 4, olderPageQueries: 2, budgetExhausted: true,
	})

	request.Cursor = first.NextCursor
	second, err := repository.DiscoverDialogueWork(context.Background(), residentID, request)
	if err != nil {
		t.Fatal(err)
	}
	assertDialogueDiscoveryResult(t, second, dialogueDiscoveryExpectation{
		seqs: []int64{1, 6, 7}, nextBeforeSeq: 2, recentScanned: 2,
		olderScanned: 1, olderPageQueries: 1, actionablePage: true,
	})
	if second.Work[0].State != domain.WorkPending {
		t.Fatalf("oldest actionable state = %q, want pending", second.Work[0].State)
	}

	repeated, err := repository.DiscoverDialogueWork(context.Background(), residentID, request)
	if err != nil {
		t.Fatal(err)
	}
	assertDialogueDiscoveryResult(t, repeated, dialogueDiscoveryExpectation{
		seqs: []int64{1, 6, 7}, nextBeforeSeq: 2, recentScanned: 2,
		olderScanned: 1, olderPageQueries: 1, actionablePage: true,
	})

	insertTerminalDialogueDiscoveryRun(
		t, fixture, fixture.event["A"], fixture.content["A"]["event"],
	)
	complete, err := repository.DiscoverDialogueWork(context.Background(), residentID, request)
	if err != nil {
		t.Fatal(err)
	}
	assertDialogueDiscoveryResult(t, complete, dialogueDiscoveryExpectation{
		seqs: []int64{6, 7}, recentScanned: 2, olderScanned: 1,
		olderPageQueries: 1, cycleComplete: true,
	})
}

func TestDialogueDiscoveryDoesNotReturnAdvancedCursorOnMalformedHistoryOrError(t *testing.T) {
	t.Run("malformed history", func(t *testing.T) {
		fixture, closeFixture := newSemanticFixture(t)
		defer closeFixture()
		repository := dialogueDiscoveryRepository(t, fixture)

		recentEventID, recentContentID := insertDialogueDiscoveryEvent(t, fixture, 2)
		insertTerminalDialogueDiscoveryRun(t, fixture, recentEventID, recentContentID)
		malformedRunID := insertDialogueDiscoveryRun(
			t, fixture, fixture.event["A"], fixture.content["A"]["event"],
		)
		insertDialogueDiscoveryOutcome(t, fixture, malformedRunID, 1, "failed")

		request := domain.DialogueDiscoveryRequest{
			Cursor:      &domain.DialogueDiscoveryCursor{BeforeSeq: canonical.Seq(2)},
			MaxAttempts: 1,
			Budget: domain.DialogueDiscoveryBudget{
				RecentCandidates: 1,
				OlderPageSize:    1,
				OlderCandidates:  1,
				OlderPages:       1,
				Elapsed:          time.Second,
			},
		}
		result, err := repository.DiscoverDialogueWork(
			context.Background(), mustParseSnapshotID(fixture.resident["A"]), request,
		)
		if !errors.Is(err, ErrInvalidGenerationOutcomeHistory) {
			t.Fatalf("discovery error = %v, want ErrInvalidGenerationOutcomeHistory", err)
		}
		assertEmptyDialogueDiscoveryProgress(t, result)
	})

	t.Run("cancelled query", func(t *testing.T) {
		fixture, closeFixture := newSemanticFixture(t)
		defer closeFixture()
		repository := dialogueDiscoveryRepository(t, fixture)
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		result, err := repository.DiscoverDialogueWork(
			ctx,
			mustParseSnapshotID(fixture.resident["A"]),
			domain.DialogueDiscoveryRequest{
				Cursor:      &domain.DialogueDiscoveryCursor{BeforeSeq: canonical.Seq(2)},
				MaxAttempts: 1,
				Budget: domain.DialogueDiscoveryBudget{
					RecentCandidates: 1,
					OlderPageSize:    1,
					OlderCandidates:  1,
					OlderPages:       1,
					Elapsed:          time.Second,
				},
			},
		)
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("discovery error = %v, want context.Canceled", err)
		}
		assertEmptyDialogueDiscoveryProgress(t, result)
	})
}

type dialogueDiscoveryExpectation struct {
	seqs                          []int64
	nextBeforeSeq                 int64
	recentScanned, olderScanned   int
	olderPageQueries              int
	cycleComplete, actionablePage bool
	budgetExhausted               bool
}

func assertDialogueDiscoveryResult(t *testing.T, result domain.DialogueDiscoveryResult, want dialogueDiscoveryExpectation) {
	t.Helper()
	if len(result.Work) != len(want.seqs) {
		t.Fatalf("work count = %d, want %d: %+v", len(result.Work), len(want.seqs), result)
	}
	for index, wantSeq := range want.seqs {
		if got := result.Work[index].UserEvent.Seq.Int64(); got != wantSeq {
			t.Fatalf("work[%d] seq = %d, want %d", index, got, wantSeq)
		}
	}
	if want.nextBeforeSeq == 0 {
		if result.NextCursor != nil {
			t.Fatalf("next cursor = %+v, want nil", result.NextCursor)
		}
	} else if result.NextCursor == nil || result.NextCursor.BeforeSeq.Int64() != want.nextBeforeSeq {
		t.Fatalf("next cursor = %+v, want before seq %d", result.NextCursor, want.nextBeforeSeq)
	}
	if result.RecentScanned != want.recentScanned || result.OlderScanned != want.olderScanned ||
		result.OlderPageQueries != want.olderPageQueries || result.CycleComplete != want.cycleComplete ||
		result.ActionablePage != want.actionablePage || result.BudgetExhausted != want.budgetExhausted {
		t.Fatalf("discovery metadata = %+v, want %+v", result, want)
	}
}

func assertEmptyDialogueDiscoveryProgress(t *testing.T, result domain.DialogueDiscoveryResult) {
	t.Helper()
	if result.NextCursor != nil || len(result.Work) != 0 || result.RecentScanned != 0 ||
		result.OlderScanned != 0 || result.OlderPageQueries != 0 || result.CycleComplete ||
		result.ActionablePage || result.BudgetExhausted {
		t.Fatalf("error returned advanced dialogue discovery progress: %+v", result)
	}
}

func dialogueDiscoveryRepository(t *testing.T, fixture *semanticFixture) *CanonicalRepository {
	t.Helper()
	mustExec(t, fixture.db, `INSERT INTO resident_status_transitions(
		resident_status_transition_id, canonical_commit_id, resident_id,
		from_status, to_status, actor_principal_id, reason_code,
		reason_content_id, occurred_at, occurred_tz, recorded_at, recorded_tz
	) VALUES (?, ?, ?, NULL, 'active', ?, 'test_fixture', NULL, ?, ?, ?, ?)`,
		fixture.ids.new(), fixture.commit["A"], fixture.resident["A"], fixture.principal["human"],
		semanticTime, semanticTZ, semanticTime, semanticTZ)
	mustExec(t, fixture.db, `INSERT INTO runtime_config(
		singleton_id, active_resident_id, desired_sessionization_policy_version_id,
		updated_at, updated_tz
	) VALUES (1, ?, NULL, ?, ?)`, fixture.resident["A"], semanticTime, semanticTZ)
	return &CanonicalRepository{store: &Store{reader: fixture.db}}
}

func insertDialogueDiscoveryEvent(t *testing.T, fixture *semanticFixture, seq int64) (string, string) {
	t.Helper()
	var previousHash []byte
	if err := fixture.db.QueryRow(
		`SELECT event_hash FROM events WHERE resident_id = ? AND seq = ?`,
		fixture.resident["A"], seq-1,
	).Scan(&previousHash); err != nil {
		t.Fatalf("resolve previous event hash for seq %d: %v", seq, err)
	}
	eventID := fixture.ids.new()
	contentID := fixture.addContent(
		t, "A", "event_payload", fmt.Sprintf("dialogue-discovery-%d", seq), "independent",
	)
	mustExec(t, fixture.db, "INSERT INTO events VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
		eventID, fixture.commit["A"], fixture.resident["A"], seq, "user_message", "conversation", 1, 0,
		"local_ui", "trusted", fixture.principal["human"], fixture.principal["A"], nil,
		semanticTime, semanticTZ, semanticTime, semanticTZ, contentID,
		semanticDigest(fmt.Sprintf("dialogue-discovery-payload-%d", seq)), previousHash,
		semanticDigest(fmt.Sprintf("dialogue-discovery-event-%d", seq)), "sha256",
		"mahoroba:event-hash:v1", "mahoroba-jcs-v1")
	return eventID, contentID
}

func insertDialogueDiscoveryRun(t *testing.T, fixture *semanticFixture, eventID, contentID string) string {
	t.Helper()
	runID := fixture.ids.new()
	mustExec(t, fixture.db, "INSERT INTO generation_runs VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
		runID, fixture.commit["A"], fixture.resident["A"], "dialogue",
		domain.DialogueObligation(mustParseSnapshotID(eventID)), "test", "model", nil,
		"prompt-v1", fixture.pipeline, "context-v1", fixture.session, "render-v1",
		fixture.revision["A"]["principles"], fixture.revision["A"]["persona"],
		fixture.revision["A"]["memory_policy"], nil, nil, nil, nil, nil, "{}",
		semanticTime, semanticTZ, 0, "{}", semanticTime, semanticTZ)
	mustExec(t, fixture.db, `INSERT INTO generation_run_inputs(
		generation_run_input_id, canonical_commit_id, generation_run_id, ordinal,
		role, source_type, source_id, inclusion_mode, content_id, recorded_at, recorded_tz
	) VALUES (?, ?, ?, 0, 'user', 'event', ?, 'current_input', ?, ?, ?)`,
		fixture.ids.new(), fixture.commit["A"], runID, eventID, contentID, semanticTime, semanticTZ)
	return runID
}

func insertTerminalDialogueDiscoveryRun(t *testing.T, fixture *semanticFixture, eventID, contentID string) {
	t.Helper()
	runID := insertDialogueDiscoveryRun(t, fixture, eventID, contentID)
	insertDialogueDiscoveryOutcome(t, fixture, runID, 1, "running")
	insertDialogueDiscoveryOutcome(t, fixture, runID, 1, "failed")
}

func insertDialogueDiscoveryOutcome(t *testing.T, fixture *semanticFixture, runID string, attempt int, state string) {
	t.Helper()
	var errorClass any
	if state != "running" {
		errorClass = "provider_timeout"
	}
	mustExec(t, fixture.db, `INSERT INTO generation_run_outcomes(
		outcome_id, canonical_commit_id, generation_run_id, attempt_no, state,
		output_content_id, prompt_tokens, completion_tokens, latency, estimated_cost,
		error_class, error_detail_content_id, recorded_at, recorded_tz
	) VALUES (?, ?, ?, ?, ?, NULL, NULL, NULL, NULL, NULL, ?, NULL, ?, ?)`,
		fixture.ids.new(), fixture.commit["A"], runID, attempt, state, errorClass, semanticTime, semanticTZ)
}
