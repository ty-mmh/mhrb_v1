package app

import (
	"context"
	"fmt"
	"reflect"
	"testing"
	"time"

	"mahoroba.local/mahoroba/internal/domain"
)

func TestSessionContinuityCarriesOrdinaryConversationWithinSevenEvents(t *testing.T) {
	for _, gap := range []time.Duration{30 * time.Minute, 31 * time.Minute, 14 * 24 * time.Hour} {
		t.Run(gap.String(), func(t *testing.T) {
			ctx := context.Background()
			generator := &scriptedGenerator{steps: []generatorStep{
				{text: "old answer"}, {text: "answer 1"}, {text: "answer 2"}, {text: "answer 3"}, {text: "answer 4"},
			}}
			fixture := newApplicationFixture(t, generator, 1)
			prior, err := fixture.application.Ingress(ctx, "old question")
			if err != nil {
				t.Fatal(err)
			}
			if err := fixture.application.ProcessResident(ctx, fixture.residentID); err != nil {
				t.Fatal(err)
			}
			fixture.clock.Advance(gap)
			var first domain.Event
			for turn := 1; turn <= 4; turn++ {
				current, err := fixture.application.Ingress(ctx, fmt.Sprintf("question %d", turn))
				if err != nil {
					t.Fatal(err)
				}
				if turn == 1 {
					first = current
				}
				if err := fixture.application.ProcessResident(ctx, fixture.residentID); err != nil {
					t.Fatal(err)
				}
				runID := dialogueRunIDForEventForTest(t, fixture, current)
				var events, distinct, currentCount, backfill int
				if err := fixture.store.Reader().QueryRow(`SELECT count(*), count(DISTINCT source_id),
				 sum(inclusion_mode = 'current_input'), sum(inclusion_mode = 'context_backfill')
				 FROM generation_run_inputs WHERE generation_run_id = ? AND source_type = 'event'`, runID.String()).
					Scan(&events, &distinct, &currentCount, &backfill); err != nil {
					t.Fatal(err)
				}
				wantBackfill := 2
				if turn == 4 {
					wantBackfill = 0
				}
				if events > domain.DialogueLiveEventLimit || events != distinct || currentCount != 1 || backfill != wantBackfill {
					t.Fatalf("turn %d: events=%d distinct=%d current=%d backfill=%d", turn, events, distinct, currentCount, backfill)
				}
				request := generator.Requests()[turn]
				var conversation []string
				for _, message := range request.Messages {
					if message.Role == "user" || message.Role == "assistant" {
						conversation = append(conversation, message.Text)
					}
				}
				if turn <= 3 && (len(conversation) < 3 || !reflect.DeepEqual(conversation[:2], []string{"old question", "old answer"})) {
					t.Fatalf("turn %d provider input lost the prior exchange: %v", turn, conversation)
				}
				var contextVersion, sessionDefinition string
				if err := fixture.store.Reader().QueryRow(`SELECT run.context_policy_version, policy.definition
				 FROM generation_runs run JOIN sessionization_policy_versions policy
				 ON policy.sessionization_policy_version_id = run.sessionization_policy_version_id
				 WHERE run.generation_run_id = ?`, runID.String()).Scan(&contextVersion, &sessionDefinition); err != nil {
					t.Fatal(err)
				}
				if contextVersion != domain.DialogueContextPolicyVersionV4 || sessionDefinition != `{"idle_gap_microseconds":"1800000000","version":"sessionization-v1"}` {
					t.Fatalf("unexpected context/session contracts: %s / %s", contextVersion, sessionDefinition)
				}
				fixture.clock.Advance(time.Minute)
			}
			// Actual activity statistics still start at the first new user message.
			history, err := fixture.repository.History(ctx, fixture.residentID, 128)
			if err != nil {
				t.Fatal(err)
			}
			session := CalculateActivitySession(history, first.RecordedAt, 30*time.Minute, domain.DialogueLiveEventLimit)
			for _, event := range session {
				if event.ID == prior.ID {
					t.Fatal("context continuity changed the activity session boundary")
				}
			}
		})
	}
}

func TestSessionContinuityDoesNotSkipErasedLatestExchange(t *testing.T) {
	ctx := context.Background()
	for _, eraseUser := range []bool{true, false} {
		t.Run(fmt.Sprintf("user=%t", eraseUser), func(t *testing.T) {
			fixture := newApplicationFixture(t, &scriptedGenerator{steps: []generatorStep{
				{text: "old intact answer"}, {text: "latest answer"}, {text: "fresh answer"},
			}}, 1)
			for _, text := range []string{"old intact question", "latest question"} {
				event, err := fixture.application.Ingress(ctx, text)
				if err != nil {
					t.Fatal(err)
				}
				if err := fixture.application.ProcessResident(ctx, fixture.residentID); err != nil {
					t.Fatal(err)
				}
				if text == "latest question" {
					if eraseUser {
						eraseEventContentForTest(t, fixture.store.Path(), event.ContentID)
					} else {
						history, err := fixture.repository.History(ctx, fixture.residentID, 16)
						if err != nil {
							t.Fatal(err)
						}
						for _, reply := range history {
							if reply.Type == "resident_message" && reply.Content == "latest answer" {
								eraseEventContentForTest(t, fixture.store.Path(), reply.ContentID)
							}
						}
					}
				}
				fixture.clock.Advance(time.Minute)
			}
			fixture.clock.Advance(31 * time.Minute)
			current, err := fixture.application.Ingress(ctx, "ordinary fresh question")
			if err != nil {
				t.Fatal(err)
			}
			runID := dialogueRunIDForEventForTest(t, fixture, current)
			var count int
			if err := fixture.store.Reader().QueryRow(`SELECT count(*) FROM generation_run_inputs
			 WHERE generation_run_id = ? AND inclusion_mode = 'context_backfill'`, runID.String()).Scan(&count); err != nil {
				t.Fatal(err)
			}
			if count != 0 {
				t.Fatalf("erased newest exchange backfilled %d events", count)
			}
		})
	}
}
