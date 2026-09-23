package app

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"mahoroba.local/mahoroba/internal/canonical"
	"mahoroba.local/mahoroba/internal/domain"
)

func TestDialogueAssemblyLiveContextAllowsOnlyDialogueEvents(t *testing.T) {
	ctx := context.Background()
	fixture := newApplicationFixture(t, &scriptedGenerator{}, 2)
	var dialogue []domain.Event
	var prohibited []canonical.ID
	for index := 0; index < initialLiveEventLimit+4; index++ {
		event, err := fixture.application.IngressWithMetadata(ctx, domain.IngressRequest{
			RawText: fmt.Sprintf("prior user %02d", index),
		})
		if err != nil {
			t.Fatal(err)
		}
		dialogue = append(dialogue, event)
		eventType := "self_talk"
		if index%2 == 1 {
			eventType = "outbound_initiative"
		}
		prohibited = append(prohibited, insertRuntimeEventForLiveContextTest(
			t, fixture, eventType, dialogueRunIDForEventForTest(t, fixture, event),
		))
	}

	current, err := fixture.application.IngressWithMetadata(ctx, domain.IngressRequest{RawText: "current user"})
	if err != nil {
		t.Fatal(err)
	}
	runID := dialogueRunIDForEventForTest(t, fixture, current)
	rows, err := fixture.store.Reader().QueryContext(ctx, `SELECT source_id
		FROM generation_run_inputs
		WHERE generation_run_id = ? AND inclusion_mode = 'live_context'
		ORDER BY ordinal`, runID.String())
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var live []string
	for rows.Next() {
		var source string
		if err := rows.Scan(&source); err != nil {
			t.Fatal(err)
		}
		live = append(live, source)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	history, err := fixture.repository.History(ctx, fixture.residentID, 128)
	if err != nil {
		t.Fatal(err)
	}
	resident, err := fixture.repository.Resident(ctx, fixture.residentID)
	if err != nil {
		t.Fatal(err)
	}
	session := CalculateActivitySession(
		history,
		current.RecordedAt,
		time.Duration(resident.SessionIdleGap.Microseconds())*time.Microsecond,
		initialLiveEventLimit,
	)
	want := make([]string, 0, len(session))
	for _, event := range session {
		if event.ID != current.ID && !event.ContentErased {
			want = append(want, event.ID.String())
		}
	}
	if strings.Join(live, ",") != strings.Join(want, ",") {
		t.Fatalf("atomic live-context sources = %v, want shared calculator result %v; prohibited count=%d", live, want, len(prohibited))
	}
}

func TestDialogueAssemblyErasedRecentEventDoesNotDisplacePresentLiveContext(t *testing.T) {
	ctx := context.Background()
	fixture := newApplicationFixture(t, &scriptedGenerator{}, 2)
	prior := make([]domain.Event, 0, initialLiveEventLimit+2)
	for index := 0; index < initialLiveEventLimit+2; index++ {
		event, err := fixture.application.Ingress(ctx, fmt.Sprintf("present history %02d", index))
		if err != nil {
			t.Fatal(err)
		}
		prior = append(prior, event)
	}
	erased := prior[len(prior)-2]
	eraseEventContentForTest(t, fixture.store.Path(), erased.ContentID)
	current, err := fixture.application.Ingress(ctx, "current after erasure")
	if err != nil {
		t.Fatal(err)
	}
	runID := dialogueRunIDForEventForTest(t, fixture, current)
	rows, err := fixture.store.Reader().QueryContext(ctx, `SELECT source_id
		FROM generation_run_inputs
		WHERE generation_run_id = ? AND inclusion_mode = 'live_context'
		ORDER BY ordinal`, runID.String())
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var got []string
	for rows.Next() {
		var source string
		if err := rows.Scan(&source); err != nil {
			t.Fatal(err)
		}
		got = append(got, source)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	var present []string
	for _, event := range prior {
		if event.ID != erased.ID {
			present = append(present, event.ID.String())
		}
	}
	want := present[len(present)-(initialLiveEventLimit-1):]
	if strings.Join(got, ",") != strings.Join(want, ",") {
		t.Fatalf("live context after erasure = %v, want older present row backfilled %v", got, want)
	}
}

func TestIngressMessageAndPreparedDialogueUseSeparateCanonicalCommits(t *testing.T) {
	ctx := context.Background()
	fixture := newApplicationFixture(t, &scriptedGenerator{}, 2)
	first, err := fixture.application.IngressWithMetadata(ctx, domain.IngressRequest{RawText: "first"})
	if err != nil {
		t.Fatal(err)
	}
	_, err = fixture.application.IngressWithMetadata(ctx, domain.IngressRequest{RawText: "second"})
	if err != nil {
		t.Fatal(err)
	}
	rawText := "  literal event:" + first.ID.String() + "\r\nremains byte-for-byte  \n"
	current, err := fixture.application.IngressWithMetadata(ctx, domain.IngressRequest{
		RawText: rawText,
	})
	if err != nil {
		t.Fatal(err)
	}
	_ = dialogueRunIDForEventForTest(t, fixture, current)

	var eventCommit, runCommit, outcomeCommit string
	var recordedAt, asOf int64
	var runID string
	err = fixture.store.Reader().QueryRowContext(ctx, `SELECT e.canonical_commit_id, e.recorded_at,
		gr.generation_run_id, gr.canonical_commit_id, gr.as_of, o.canonical_commit_id
		FROM events e
		JOIN generation_runs gr ON gr.idempotency_key = 'dialogue:v1:' || e.event_id
		JOIN generation_run_outcomes o ON o.generation_run_id = gr.generation_run_id
		WHERE e.event_id = ? AND o.attempt_no = 1 AND o.state = 'running'`, current.ID.String()).Scan(
		&eventCommit, &recordedAt, &runID, &runCommit, &asOf, &outcomeCommit)
	if err != nil {
		t.Fatal(err)
	}
	if eventCommit == runCommit || runCommit != outcomeCommit {
		t.Fatalf("two-commit IDs event=%s run=%s outcome=%s", eventCommit, runCommit, outcomeCommit)
	}
	if asOf != recordedAt || canonical.Instant(asOf) != current.RecordedAt {
		t.Fatalf("as_of=%d recorded_at=%d event.RecordedAt=%d", asOf, recordedAt, current.RecordedAt)
	}
	var inputCount, distinctInputCommits int
	if err := fixture.store.Reader().QueryRowContext(ctx, `SELECT COUNT(*), COUNT(DISTINCT canonical_commit_id)
		FROM generation_run_inputs WHERE generation_run_id = ?`, runID).Scan(&inputCount, &distinctInputCommits); err != nil {
		t.Fatal(err)
	}
	if inputCount < 7 || distinctInputCommits != 1 {
		t.Fatalf("input count=%d distinct commits=%d", inputCount, distinctInputCommits)
	}
	var inputCommit string
	if err := fixture.store.Reader().QueryRowContext(ctx, `SELECT canonical_commit_id FROM generation_run_inputs
		WHERE generation_run_id = ? ORDER BY ordinal LIMIT 1`, runID).Scan(&inputCommit); err != nil {
		t.Fatal(err)
	}
	if inputCommit != runCommit {
		t.Fatalf("input commit=%s run commit=%s", inputCommit, runCommit)
	}
	loaded, err := fixture.repository.Event(ctx, fixture.residentID, current.ID)
	if err != nil {
		t.Fatal(err)
	}
	if loaded.Content != rawText {
		t.Fatalf("RawText=%q want exact %q", loaded.Content, rawText)
	}
}

func TestIngressDoesNotInferBackfillFromRawText(t *testing.T) {
	ctx := context.Background()
	fixture := newApplicationFixture(t, &scriptedGenerator{}, 2)
	old, err := fixture.application.Ingress(ctx, "old")
	if err != nil {
		t.Fatal(err)
	}
	current, err := fixture.application.Ingress(ctx, "literal event:"+old.ID.String())
	if err != nil {
		t.Fatal(err)
	}
	var count int
	if err := fixture.store.Reader().QueryRowContext(ctx, `SELECT COUNT(*) FROM generation_run_inputs i
		JOIN generation_runs r ON r.generation_run_id = i.generation_run_id
		WHERE r.idempotency_key = ? AND i.inclusion_mode = 'context_backfill'`,
		domain.DialogueObligation(current.ID)).Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 0 {
		t.Fatalf("inferred context_backfill rows=%d, want 0", count)
	}
}

func TestCOV5NormalUIContinuationBackfillsImmediatelyPreviousExchange(t *testing.T) {
	tests := []struct {
		name        string
		text        string
		wantMarkers string
	}{
		{
			name: "Japanese", text: "前回の続きを再開しよう",
			wantMarkers: `["continuation_request","prior_context_reference"]`,
		},
		{
			name: "English", text: "Pick up where we left off",
			wantMarkers: `["continuation_request","prior_context_reference"]`,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			ctx := context.Background()
			generator := &scriptedGenerator{steps: []generatorStep{
				{text: "prior resident reply"}, {text: "current resident reply"},
			}}
			fixture := newApplicationFixture(t, generator, 1)
			priorUser, err := fixture.application.Ingress(ctx, "prior user message")
			if err != nil {
				t.Fatal(err)
			}
			if err := fixture.application.ProcessResident(ctx, fixture.residentID); err != nil {
				t.Fatalf("complete prior exchange: %v", err)
			}
			priorRunID := dialogueRunIDForEventForTest(t, fixture, priorUser)
			var priorResidentRaw string
			if err := fixture.store.Reader().QueryRowContext(ctx, `SELECT event_id FROM events
				WHERE resident_id = ? AND generation_run_id = ? AND event_type = 'resident_message'`,
				fixture.residentID.String(), priorRunID.String()).Scan(&priorResidentRaw); err != nil {
				t.Fatal(err)
			}
			fixture.clock.Advance(31 * time.Minute)
			current, err := fixture.application.Ingress(ctx, test.text)
			if err != nil {
				t.Fatal(err)
			}
			if err := fixture.application.ProcessResident(ctx, fixture.residentID); err != nil {
				t.Fatalf("complete current exchange: %v", err)
			}
			currentRunID := dialogueRunIDForEventForTest(t, fixture, current)
			rows, err := fixture.store.Reader().QueryContext(ctx, `SELECT input.source_id, input.role,
				event.event_type, event.visibility
				FROM generation_run_inputs input
				JOIN events event ON event.event_id = input.source_id
				WHERE input.generation_run_id = ? AND input.inclusion_mode = 'context_backfill'
				ORDER BY input.ordinal`, currentRunID.String())
			if err != nil {
				t.Fatal(err)
			}
			defer rows.Close()
			type backfillRow struct{ source, role, eventType, visibility string }
			var backfill []backfillRow
			for rows.Next() {
				var row backfillRow
				if err := rows.Scan(&row.source, &row.role, &row.eventType, &row.visibility); err != nil {
					t.Fatal(err)
				}
				backfill = append(backfill, row)
			}
			if err := rows.Err(); err != nil {
				t.Fatal(err)
			}
			want := []backfillRow{
				{source: priorUser.ID.String(), role: "user", eventType: "user_message", visibility: "conversation"},
				{source: priorResidentRaw, role: "assistant", eventType: "resident_message", visibility: "conversation"},
			}
			if fmt.Sprint(backfill) != fmt.Sprint(want) {
				t.Fatalf("automatic Backfill = %+v, want %+v", backfill, want)
			}
			var markers string
			if err := fixture.store.Reader().QueryRowContext(ctx, `SELECT unresolved_reference_markers
				FROM runtime_states WHERE resident_id = ?`, fixture.residentID.String()).Scan(&markers); err != nil {
				t.Fatal(err)
			}
			if markers != test.wantMarkers {
				t.Fatalf("runtime markers = %s, want %s", markers, test.wantMarkers)
			}
			var prohibited int
			if err := fixture.store.Reader().QueryRowContext(ctx, `SELECT count(*)
				FROM generation_run_inputs input JOIN events event ON event.event_id = input.source_id
				WHERE input.generation_run_id = ? AND input.inclusion_mode = 'context_backfill'
				  AND (event.event_type = 'self_talk' OR event.visibility <> 'conversation')`,
				currentRunID.String()).Scan(&prohibited); err != nil {
				t.Fatal(err)
			}
			if prohibited != 0 {
				t.Fatalf("automatic Backfill included %d self-talk/internal events", prohibited)
			}
		})
	}
}

func TestCOV5AutomaticBackfillIgnoresLaterSelfTalk(t *testing.T) {
	ctx := context.Background()
	fixture := landedSelfTalkFixture(t)
	fixture.clock.Advance(31 * time.Minute)

	current, err := fixture.application.Ingress(ctx, "Pick up where we left off")
	if err != nil {
		t.Fatal(err)
	}
	runID := dialogueRunIDForEventForTest(t, fixture, current)

	rows, err := fixture.store.Reader().QueryContext(ctx, `SELECT event.event_type
		FROM generation_run_inputs input
		JOIN events event ON event.event_id = input.source_id
		WHERE input.generation_run_id = ? AND input.inclusion_mode = 'context_backfill'
		ORDER BY input.ordinal`, runID.String())
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var eventTypes []string
	for rows.Next() {
		var eventType string
		if err := rows.Scan(&eventType); err != nil {
			t.Fatal(err)
		}
		eventTypes = append(eventTypes, eventType)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	if got, want := strings.Join(eventTypes, ","), "user_message,resident_message"; got != want {
		t.Fatalf("Backfill event types after self-talk = %q, want %q", got, want)
	}
	var selfTalkEvents int
	if err := fixture.store.Reader().QueryRowContext(ctx, `SELECT COUNT(*) FROM events
		WHERE resident_id = ? AND event_type = 'self_talk'`, fixture.residentID.String()).Scan(&selfTalkEvents); err != nil {
		t.Fatal(err)
	}
	if selfTalkEvents == 0 {
		t.Fatal("fixture did not land self-talk before automatic Backfill")
	}
}

func TestCOV5ContinuationInsideCurrentActivitySessionDoesNotBackfill(t *testing.T) {
	ctx := context.Background()
	fixture := newApplicationFixture(t, &scriptedGenerator{steps: []generatorStep{
		{text: "prior resident reply"}, {text: "current resident reply"},
	}}, 1)
	if _, err := fixture.application.Ingress(ctx, "prior user message"); err != nil {
		t.Fatal(err)
	}
	if err := fixture.application.ProcessResident(ctx, fixture.residentID); err != nil {
		t.Fatalf("complete prior exchange: %v", err)
	}
	current, err := fixture.application.Ingress(ctx, "前回の続きを再開しよう")
	if err != nil {
		t.Fatal(err)
	}
	if err := fixture.application.ProcessResident(ctx, fixture.residentID); err != nil {
		t.Fatalf("complete current exchange: %v", err)
	}
	currentRunID := dialogueRunIDForEventForTest(t, fixture, current)
	var backfill, live int
	if err := fixture.store.Reader().QueryRowContext(ctx, `SELECT
		coalesce(sum(inclusion_mode = 'context_backfill'), 0),
		coalesce(sum(inclusion_mode = 'live_context'), 0)
		FROM generation_run_inputs WHERE generation_run_id = ?`, currentRunID.String()).Scan(&backfill, &live); err != nil {
		t.Fatal(err)
	}
	if backfill != 0 || live < 2 {
		t.Fatalf("same-session context backfill=%d live=%d, want 0 and prior exchange", backfill, live)
	}
}

func TestCOV5ContinuationBackfillsOnlyTheImmediatelyPreviousUnansweredUser(t *testing.T) {
	ctx := context.Background()
	fixture := newApplicationFixture(t, &scriptedGenerator{steps: []generatorStep{
		{err: errors.New("terminal prior failure")}, {text: "current resident reply"},
	}}, 1)
	prior, err := fixture.application.Ingress(ctx, "unanswered prior user")
	if err != nil {
		t.Fatal(err)
	}
	if err := fixture.application.ProcessResident(ctx, fixture.residentID); err != nil {
		t.Fatalf("terminalize prior dialogue: %v", err)
	}
	fixture.clock.Advance(31 * time.Minute)
	current, err := fixture.application.Ingress(ctx, "Pick up where we left off")
	if err != nil {
		t.Fatal(err)
	}
	if err := fixture.application.ProcessResident(ctx, fixture.residentID); err != nil {
		t.Fatalf("complete current exchange: %v", err)
	}
	currentRunID := dialogueRunIDForEventForTest(t, fixture, current)
	rows, err := fixture.store.Reader().QueryContext(ctx, `SELECT input.source_id, input.role, event.event_type
		FROM generation_run_inputs input
		JOIN events event ON event.event_id = input.source_id
		WHERE input.generation_run_id = ? AND input.inclusion_mode = 'context_backfill'
		ORDER BY input.ordinal`, currentRunID.String())
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	type backfillRow struct{ source, role, eventType string }
	var got []backfillRow
	for rows.Next() {
		var row backfillRow
		if err := rows.Scan(&row.source, &row.role, &row.eventType); err != nil {
			t.Fatal(err)
		}
		got = append(got, row)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	want := []backfillRow{{source: prior.ID.String(), role: "user", eventType: "user_message"}}
	if fmt.Sprint(got) != fmt.Sprint(want) {
		t.Fatalf("unanswered-user Backfill = %+v, want %+v", got, want)
	}
}

func TestCOV5CommitAOnlyRestartReconstructsAutomaticBackfillFromSourceBytes(t *testing.T) {
	ctx := context.Background()
	fixture := newApplicationFixture(t, &scriptedGenerator{steps: []generatorStep{{text: "prior reply"}}}, 1)
	prior, err := fixture.application.Ingress(ctx, "prior user")
	if err != nil {
		t.Fatal(err)
	}
	if err := fixture.application.ProcessResident(ctx, fixture.residentID); err != nil {
		t.Fatalf("complete prior exchange: %v", err)
	}
	priorRunID := dialogueRunIDForEventForTest(t, fixture, prior)
	var priorResident string
	if err := fixture.store.Reader().QueryRowContext(ctx, `SELECT event_id FROM events
		WHERE resident_id = ? AND generation_run_id = ? AND event_type = 'resident_message'`,
		fixture.residentID.String(), priorRunID.String()).Scan(&priorResident); err != nil {
		t.Fatal(err)
	}

	fixture.clock.Advance(31 * time.Minute)
	current, err := fixture.application.Ingress(ctx, "前回の続きを再開しよう")
	if err != nil {
		t.Fatal(err)
	}
	var runsBeforeRestart int
	if err := fixture.store.Reader().QueryRowContext(ctx, `SELECT count(*) FROM generation_runs
		WHERE resident_id = ? AND idempotency_key = ?`, fixture.residentID.String(),
		domain.DialogueObligation(current.ID)).Scan(&runsBeforeRestart); err != nil {
		t.Fatal(err)
	}
	if runsBeforeRestart != 0 {
		t.Fatalf("generation runs before restart = %d, want Commit A only", runsBeforeRestart)
	}

	restartedGenerator := &scriptedGenerator{steps: []generatorStep{{text: "current reply"}}}
	restarted, _ := restartApplicationForEnvelopeTest(
		t, fixture, restartedGenerator, "test", "test-model", 64<<10,
	)
	if err := restarted.Recover(ctx); err != nil {
		t.Fatal(err)
	}
	if err := restarted.ProcessResident(ctx, fixture.residentID); err != nil {
		t.Fatalf("process restarted continuation: %v", err)
	}

	database := openApplicationDatabase(t, fixture.store.Path())
	defer database.Close()
	rows, err := database.Query(`SELECT input.source_id FROM generation_run_inputs input
		JOIN generation_runs run ON run.generation_run_id = input.generation_run_id
		WHERE run.resident_id = ? AND run.idempotency_key = ?
		  AND input.inclusion_mode = 'context_backfill' ORDER BY input.ordinal`,
		fixture.residentID.String(), domain.DialogueObligation(current.ID))
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var got []string
	for rows.Next() {
		var source string
		if err := rows.Scan(&source); err != nil {
			t.Fatal(err)
		}
		got = append(got, source)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	want := []string{prior.ID.String(), priorResident}
	if fmt.Sprint(got) != fmt.Sprint(want) {
		t.Fatalf("restart Backfill sources = %v, want %v", got, want)
	}
}

func TestCOV5ErasedPreviousExchangeDoesNotFallBackToOlderConversation(t *testing.T) {
	ctx := context.Background()
	fixture := newApplicationFixture(t, &scriptedGenerator{steps: []generatorStep{
		{text: "prior resident reply"}, {text: "current resident reply"},
	}}, 1)
	prior, err := fixture.application.Ingress(ctx, "prior user message")
	if err != nil {
		t.Fatal(err)
	}
	if err := fixture.application.ProcessResident(ctx, fixture.residentID); err != nil {
		t.Fatalf("complete prior exchange: %v", err)
	}
	priorRunID := dialogueRunIDForEventForTest(t, fixture, prior)
	var residentContentRaw string
	if err := fixture.store.Reader().QueryRowContext(ctx, `SELECT content_id FROM events
		WHERE resident_id = ? AND generation_run_id = ? AND event_type = 'resident_message'`,
		fixture.residentID.String(), priorRunID.String()).Scan(&residentContentRaw); err != nil {
		t.Fatal(err)
	}
	residentContentID, err := canonical.ParseID(residentContentRaw)
	if err != nil {
		t.Fatal(err)
	}
	eraseEventContentForTest(t, fixture.store.Path(), residentContentID)
	fixture.clock.Advance(31 * time.Minute)
	current, err := fixture.application.Ingress(ctx, "前回の続きを再開しよう")
	if err != nil {
		t.Fatal(err)
	}
	if err := fixture.application.ProcessResident(ctx, fixture.residentID); err != nil {
		t.Fatalf("complete current exchange: %v", err)
	}
	currentRunID := dialogueRunIDForEventForTest(t, fixture, current)
	var count int
	if err := fixture.store.Reader().QueryRowContext(ctx, `SELECT count(*) FROM generation_run_inputs
		WHERE generation_run_id = ? AND inclusion_mode = 'context_backfill'`, currentRunID.String()).Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 0 {
		t.Fatalf("erased latest exchange produced %d Backfill rows, want 0", count)
	}
}

func TestCOV5InitiativeBackfillSuppressesAutomaticPreviousExchange(t *testing.T) {
	ctx := context.Background()
	fixture := newApplicationFixture(t, &scriptedGenerator{steps: []generatorStep{
		{text: "prior resident reply"}, {err: errors.New("terminal test failure")}, {text: "current resident reply"},
	}}, 1)
	if _, err := fixture.application.Ingress(ctx, "prior user message"); err != nil {
		t.Fatal(err)
	}
	if err := fixture.application.ProcessResident(ctx, fixture.residentID); err != nil {
		t.Fatalf("complete prior exchange: %v", err)
	}
	failed, err := fixture.application.Ingress(ctx, "failed user message")
	if err != nil {
		t.Fatal(err)
	}
	if err := fixture.application.ProcessResident(ctx, fixture.residentID); err != nil {
		t.Fatalf("complete failed exchange: %v", err)
	}
	failedRunID := dialogueRunIDForEventForTest(t, fixture, failed)
	initiativeID := insertRuntimeEventForLiveContextTest(t, fixture, "outbound_initiative", failedRunID)
	fixture.clock.Advance(31 * time.Minute)
	current, err := fixture.application.Ingress(ctx, "Pick up where we left off")
	if err != nil {
		t.Fatal(err)
	}
	if err := fixture.application.ProcessResident(ctx, fixture.residentID); err != nil {
		t.Fatalf("complete current exchange: %v", err)
	}
	currentRunID := dialogueRunIDForEventForTest(t, fixture, current)
	rows, err := fixture.store.Reader().QueryContext(ctx, `SELECT source_id FROM generation_run_inputs
		WHERE generation_run_id = ? AND inclusion_mode = 'context_backfill' ORDER BY ordinal`, currentRunID.String())
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var sources []string
	for rows.Next() {
		var source string
		if err := rows.Scan(&source); err != nil {
			t.Fatal(err)
		}
		sources = append(sources, source)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	if fmt.Sprint(sources) != fmt.Sprint([]string{initiativeID.String()}) {
		t.Fatalf("initiative-suppressed Backfill sources = %v, want only %s", sources, initiativeID)
	}
}

func TestIngressExplicitReferencesAreRejectedBeforeCommitA(t *testing.T) {
	ctx := context.Background()
	fixture := newApplicationFixture(t, &scriptedGenerator{}, 2)
	unknown := mustIntegrationID(t, "01ARZ3NDEKTSV4RRFFQ69G5FAV")
	before := dialogueIngressCounts(t, fixture)
	if _, err := fixture.application.IngressWithMetadata(ctx, domain.IngressRequest{
		RawText: "unknown reference", ExplicitEventIDs: []canonical.ID{unknown},
	}); !errors.Is(err, domain.ErrInvalidEventReference) ||
		!strings.Contains(err.Error(), "durable Context Reference") {
		t.Fatalf("unknown reference error=%v", err)
	}
	if after := dialogueIngressCounts(t, fixture); after != before {
		t.Fatalf("rows after rollback=%+v before=%+v", after, before)
	}
	if _, err := fixture.application.IngressWithMetadata(ctx, domain.IngressRequest{
		RawText: "duplicates", ExplicitEventIDs: []canonical.ID{unknown, unknown},
	}); !errors.Is(err, domain.ErrInvalidEventReference) ||
		!strings.Contains(err.Error(), "durable Context Reference") {
		t.Fatalf("duplicate reference error=%v", err)
	}
	if after := dialogueIngressCounts(t, fixture); after != before {
		t.Fatalf("rows after duplicate rejection=%+v before=%+v", after, before)
	}
}

func TestIngressRejectsExplicitReferencesBeforeResidentOrAvailabilityLookup(t *testing.T) {
	ctx := context.Background()
	fixture := newApplicationFixture(t, &scriptedGenerator{}, 2)
	firstResident := fixture.residentID
	state, err := fixture.application.BootstrapInit(ctx, BootstrapInput{
		OwnerName: "Owner", Name: "Other", SeedKey: "other-v1", Principles: "stay scoped",
	})
	if err != nil {
		t.Fatal(err)
	}
	var other canonical.ID
	for _, resident := range state.Residents {
		if resident.Name == "Other" {
			other = resident.ResidentID
		}
	}
	if other.IsZero() {
		t.Fatal("other resident was not created")
	}
	if err := fixture.application.ApprovePrinciples(ctx, other); err != nil {
		t.Fatal(err)
	}
	if err := fixture.application.FinalizeBootstrap(ctx, other, "other persona",
		`{"mandatory_event_types":[],"memory_recall_enabled":false,"version":"memory-policy-v1"}`); err != nil {
		t.Fatal(err)
	}
	if err := fixture.application.SelectResident(ctx, other); err != nil {
		t.Fatal(err)
	}
	foreign, err := fixture.application.Ingress(ctx, "foreign resident event")
	if err != nil {
		t.Fatal(err)
	}
	if err := fixture.application.SelectResident(ctx, firstResident); err != nil {
		t.Fatal(err)
	}
	before := dialogueIngressCounts(t, fixture)
	if _, err := fixture.application.IngressWithMetadata(ctx, domain.IngressRequest{
		RawText: "cross resident", ExplicitEventIDs: []canonical.ID{foreign.ID},
	}); !errors.Is(err, domain.ErrInvalidEventReference) || !strings.Contains(err.Error(), "durable Context Reference") {
		t.Fatalf("cross-resident reference error=%v", err)
	}
	if after := dialogueIngressCounts(t, fixture); after != before {
		t.Fatalf("rows after cross-resident rollback=%+v before=%+v", after, before)
	}

	local, err := fixture.application.Ingress(ctx, "erase before explicit use")
	if err != nil {
		t.Fatal(err)
	}
	eraseEventContentForTest(t, fixture.store.Path(), local.ContentID)
	before = dialogueIngressCounts(t, fixture)
	if _, err := fixture.application.IngressWithMetadata(ctx, domain.IngressRequest{
		RawText: "erased reference", ExplicitEventIDs: []canonical.ID{local.ID},
	}); !errors.Is(err, domain.ErrInvalidEventReference) || !strings.Contains(err.Error(), "durable Context Reference") {
		t.Fatalf("erased reference error=%v", err)
	}
	if after := dialogueIngressCounts(t, fixture); after != before {
		t.Fatalf("rows after erased-reference rollback=%+v before=%+v", after, before)
	}
}

func TestExplicitBackfillBudgetIsDeferredToReadAssembly(t *testing.T) {
	ctx := context.Background()
	fixture := newApplicationFixture(t, &scriptedGenerator{}, 2)
	old, err := fixture.application.Ingress(ctx, strings.Repeat("x", 64))
	if err != nil {
		t.Fatal(err)
	}
	before := dialogueIngressCounts(t, fixture)
	// App/HTTP cannot create explicit Backfill until durable Context References
	// exist. Context-v2 Backfill selection, budget, and re-evaluation are proved
	// by the read Assembly/SQLite tests where the final candidate set is fixed.
	_, err = fixture.application.IngressWithMetadata(ctx, domain.IngressRequest{
		RawText: "current", ExplicitEventIDs: []canonical.ID{old.ID},
	})
	if !errors.Is(err, domain.ErrInvalidEventReference) {
		t.Fatalf("explicit Backfill error = %v", err)
	}
	if after := dialogueIngressCounts(t, fixture); after != before {
		t.Fatalf("explicit Backfill rejection mutated Canonical rows: before=%+v after=%+v", before, after)
	}
}

func TestExplicitBackfillCreatesNoCrashRecoveryStateBeforeCommitA(t *testing.T) {
	ctx := context.Background()
	fixture := newApplicationFixture(t, &scriptedGenerator{}, 2)
	old, err := fixture.application.Ingress(ctx, "old source")
	if err != nil {
		t.Fatal(err)
	}
	before := dialogueIngressCounts(t, fixture)
	_, err = fixture.application.IngressWithMetadata(ctx, domain.IngressRequest{
		RawText: "current after crash", ExplicitEventIDs: []canonical.ID{old.ID},
	})
	if !errors.Is(err, domain.ErrInvalidEventReference) {
		t.Fatalf("explicit Backfill error = %v", err)
	}
	if after := dialogueIngressCounts(t, fixture); after != before {
		t.Fatalf("pre-Commit-A rejection left recovery state: before=%+v after=%+v", before, after)
	}
}

type ingressCounts struct {
	commits, events, runs, inputs, outcomes int
}

func dialogueIngressCounts(t *testing.T, fixture applicationFixture) ingressCounts {
	t.Helper()
	var result ingressCounts
	for _, item := range []struct {
		destination *int
		query       string
	}{
		{&result.commits, `SELECT COUNT(*) FROM canonical_commits`},
		{&result.events, `SELECT COUNT(*) FROM events`},
		{&result.runs, `SELECT COUNT(*) FROM generation_runs`},
		{&result.inputs, `SELECT COUNT(*) FROM generation_run_inputs`},
		{&result.outcomes, `SELECT COUNT(*) FROM generation_run_outcomes`},
	} {
		if err := fixture.store.Reader().QueryRow(item.query).Scan(item.destination); err != nil {
			t.Fatal(err)
		}
	}
	return result
}

// insertRuntimeEventForLiveContextTest seeds event kinds that are specified by
// the canonical schema but do not yet have an M3 application command. The row
// passes the real SQLite constraints and lets the ingress query prove that
// future runtime event kinds are not automatically exposed to dialogue context.
func insertRuntimeEventForLiveContextTest(t *testing.T, fixture applicationFixture, eventType string, runID canonical.ID) canonical.ID {
	t.Helper()
	visibility, deliveryScreen := "conversation", 1
	if eventType == "self_talk" {
		visibility, deliveryScreen = "internal", 0
	} else if eventType != "outbound_initiative" {
		t.Fatalf("unsupported synthetic runtime event type %q", eventType)
	}
	eventID, err := fixture.application.ids.New()
	if err != nil {
		t.Fatal(err)
	}
	resident, err := fixture.repository.Resident(context.Background(), fixture.residentID)
	if err != nil {
		t.Fatal(err)
	}
	database, err := sql.Open("sqlite", fixture.store.Path())
	if err != nil {
		t.Fatal(err)
	}
	database.SetMaxOpenConns(1)
	defer database.Close()
	if _, err := database.Exec(`PRAGMA foreign_keys = ON`); err != nil {
		t.Fatal(err)
	}
	if _, err := database.Exec(`PRAGMA busy_timeout = 5000`); err != nil {
		t.Fatal(err)
	}

	var commitID, occurredTZ, recordedTZ, contentID string
	var seq, occurredAt, recordedAt int64
	var payloadCommitment, previousHash []byte
	err = database.QueryRow(`SELECT canonical_commit_id, seq, occurred_at, occurred_tz,
		recorded_at, recorded_tz, content_id, payload_commitment, event_hash
		FROM events WHERE resident_id = ? ORDER BY seq DESC LIMIT 1`, fixture.residentID.String()).Scan(
		&commitID, &seq, &occurredAt, &occurredTZ, &recordedAt, &recordedTZ,
		&contentID, &payloadCommitment, &previousHash,
	)
	if err != nil {
		t.Fatal(err)
	}
	eventHash := canonical.HashBlob([]byte("live-context-negative:" + eventType + ":" + eventID.String()))
	_, err = database.Exec(`INSERT INTO events(
		event_id, canonical_commit_id, resident_id, seq, event_type, visibility,
		delivery_screen, delivery_audio, ingress, trust_level, actor_principal_id,
		target_principal_id, generation_run_id, occurred_at, occurred_tz, recorded_at,
		recorded_tz, content_id, payload_commitment, prev_event_hash, event_hash,
		event_hash_algorithm, event_hash_domain, canonicalization_version
	) VALUES (?, ?, ?, ?, ?, ?, ?, 0, 'resident_runtime', 'trusted', ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
		'sha256', 'mahoroba:event-hash:v1', 'mahoroba-jcs-v1')`,
		eventID.String(), commitID, fixture.residentID.String(), seq+1, eventType, visibility,
		deliveryScreen, resident.ResidentPrincipalID.String(), resident.OwnerPrincipalID.String(), runID.String(),
		occurredAt, occurredTZ, recordedAt, recordedTZ, contentID, payloadCommitment, previousHash, eventHash.Bytes(),
	)
	if err != nil {
		t.Fatal(err)
	}
	return eventID
}

func mustIntegrationID(t *testing.T, raw string) canonical.ID {
	t.Helper()
	id, err := canonical.ParseID(raw)
	if err != nil {
		t.Fatal(err)
	}
	return id
}
