package app

import (
	"reflect"
	"testing"
	"time"

	"mahoroba.local/mahoroba/internal/canonical"
	"mahoroba.local/mahoroba/internal/domain"
)

func TestCalculateActivitySessionOnlyUserMessagesExtendSession(t *testing.T) {
	base := canonical.Instant(1_700_000_000_000_000)
	atMinute := func(minute int64) canonical.Instant {
		return base + canonical.Instant(time.Duration(minute)*time.Minute/time.Microsecond)
	}
	event := func(eventType string, minute int64) domain.Event {
		return domain.Event{Type: eventType, RecordedAt: atMinute(minute)}
	}

	t.Run("resident activity cannot bridge a user idle gap", func(t *testing.T) {
		events := []domain.Event{
			event("user_message", 0), event("resident_message", 20),
			event("resident_message", 35), event("user_message", 40),
		}
		got := CalculateActivitySession(events, atMinute(40), 30*time.Minute, 16)
		if len(got) != 1 || got[0].RecordedAt != atMinute(40) {
			t.Fatalf("session = %+v, want only the current user event", got)
		}
	})

	t.Run("resident events between in-window user messages are retained", func(t *testing.T) {
		events := []domain.Event{
			event("user_message", 0), event("resident_message", 20), event("user_message", 30),
		}
		got := CalculateActivitySession(events, atMinute(30), 30*time.Minute, 16)
		want := events[2:]
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("session = %+v, want exact-threshold boundary %+v", got, want)
		}
	})

	t.Run("a gap below the canonical threshold remains in one session", func(t *testing.T) {
		events := []domain.Event{
			event("user_message", 0), event("resident_message", 20), event("user_message", 29),
		}
		got := CalculateActivitySession(events, atMinute(29), 30*time.Minute, 16)
		if !reflect.DeepEqual(got, events) {
			t.Fatalf("session = %+v, want %+v", got, events)
		}
	})
}

func TestCalculateActivitySessionHonorsAsOfAndBoundedTail(t *testing.T) {
	events := []domain.Event{
		{Type: "user_message", RecordedAt: 1}, {Type: "resident_message", RecordedAt: 2},
		{Type: "user_message", RecordedAt: 3}, {Type: "user_message", RecordedAt: 4},
	}
	got := CalculateActivitySession(events, 3, time.Hour, 2)
	if want := events[1:3]; !reflect.DeepEqual(got, want) {
		t.Fatalf("session = %+v, want %+v", got, want)
	}
}

func TestCalculateActivitySessionExcludesNonDialogueEventsBeforeBounding(t *testing.T) {
	prior := domain.Event{Type: "user_message", RecordedAt: 1, Content: "prior"}
	reply := domain.Event{Type: "resident_message", RecordedAt: 2, Content: "reply"}
	events := []domain.Event{prior, reply}
	for index := 0; index < domain.DialogueLiveEventLimit*2; index++ {
		eventType := "self_talk"
		if index%2 == 1 {
			eventType = "outbound_initiative"
		}
		events = append(events, domain.Event{
			Type: eventType, RecordedAt: canonical.Instant(3 + index),
			Content: "must not consume the live window",
		})
	}
	current := domain.Event{Type: "user_message", RecordedAt: canonical.Instant(3 + len(events)), Content: "current"}
	events = append(events, current)

	got := CalculateActivitySession(events, current.RecordedAt, time.Hour, domain.DialogueLiveEventLimit)
	want := []domain.Event{prior, reply, current}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("bounded dialogue session = %+v, want %+v", got, want)
	}
}
