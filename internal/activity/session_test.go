package activity

import (
	"reflect"
	"testing"
	"time"

	"mahoroba.local/mahoroba/internal/canonical"
	"mahoroba.local/mahoroba/internal/domain"
)

func TestActivitySessionThresholdBoundary(t *testing.T) {
	events := []domain.Event{
		activityEvent(1, "user_message", 0),
		activityEvent(2, "resident_message", 20),
		activityEvent(3, "user_message", 30),
	}
	session, err := CalculateEvents(events, activityAtMinute(30), activityGap(30*time.Minute), 16)
	if err != nil {
		t.Fatal(err)
	}
	if want := events[2:]; !reflect.DeepEqual(session.Events, want) {
		t.Fatalf("session = %+v, want exact-threshold split %+v", session.Events, want)
	}
	if !session.Open {
		t.Fatal("session at latest user message is closed")
	}

	session, err = CalculateEvents(events, activityAtMinute(60), activityGap(30*time.Minute), 16)
	if err != nil {
		t.Fatal(err)
	}
	if session.Open {
		t.Fatal("session at exact idle threshold is open")
	}
}

func TestActivitySessionClockRollbackIsDeterministic(t *testing.T) {
	events := []domain.Event{
		activityEvent(1, "user_message", 20),
		activityEvent(2, "resident_message", 10),
		activityEvent(3, "user_message", 5),
	}
	first, err := CalculateEvents(events, activityAtMinute(20), activityGap(15*time.Minute), 16)
	if err != nil {
		t.Fatal(err)
	}
	second, err := CalculateEvents(events, activityAtMinute(20), activityGap(15*time.Minute), 16)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(first, second) {
		t.Fatalf("clock-rollback result changed: first=%+v second=%+v", first, second)
	}
	if !reflect.DeepEqual(first.Events, events) {
		t.Fatalf("clock rollback split canonical sequence: got=%+v want=%+v", first.Events, events)
	}
}

func TestActivitySessionResidentOriginDoesNotExtend(t *testing.T) {
	events := []domain.Event{
		activityEvent(1, "user_message", 0),
		activityEvent(2, "resident_message", 20),
		activityEvent(3, "resident_message", 35),
		activityEvent(4, "user_message", 40),
	}
	session, err := CalculateEvents(events, activityAtMinute(40), activityGap(30*time.Minute), 16)
	if err != nil {
		t.Fatal(err)
	}
	if want := events[3:]; !reflect.DeepEqual(session.Events, want) {
		t.Fatalf("resident activity bridged user idle gap: got=%+v want=%+v", session.Events, want)
	}
}

func TestActivitySessionInactiveLifecycleIsHardBoundary(t *testing.T) {
	events := []Event{
		{CommitSeq: 3, Value: activityEvent(1, "user_message", 1)},
		{CommitSeq: 4, Value: activityEvent(2, "resident_message", 2)},
	}
	input := Input{
		Events: events,
		StatusTransitions: []StatusTransition{
			{CommitSeq: 1, RecordedAt: activityAtMinute(0), Status: "draft"},
			{CommitSeq: 2, RecordedAt: activityAtMinute(0), Status: "active"},
			{CommitSeq: 5, RecordedAt: activityAtMinute(3), Status: "archived"},
		},
		ThroughCommit: 5,
		AsOf:          activityAtMinute(4),
		IdleGap:       activityGap(30 * time.Minute),
		MaxEvents:     16,
	}
	session, err := Calculate(input)
	if err != nil {
		t.Fatal(err)
	}
	if session.ResidentActive || session.Open || len(session.Events) != 0 {
		t.Fatalf("inactive lifecycle leaked an activity session: %+v", session)
	}

	// The captured target is part of the pure input. Evaluating before the
	// archive observes the same prior events as the online ingress path.
	input.ThroughCommit = 4
	input.AsOf = activityAtMinute(2)
	session, err = Calculate(input)
	if err != nil {
		t.Fatal(err)
	}
	if !session.ResidentActive || !reflect.DeepEqual(session.Events, []domain.Event{events[0].Value, events[1].Value}) {
		t.Fatalf("pre-archive session = %+v", session)
	}
}

func TestActivitySessionRebuildUsesCanonicalLifecyclePolicyAndAsOf(t *testing.T) {
	input := Input{
		Events: []Event{
			{CommitSeq: 3, Value: activityEvent(1, "user_message", 10)},
			{CommitSeq: 4, Value: activityEvent(2, "resident_message", 20)},
		},
		StatusTransitions: []StatusTransition{
			{CommitSeq: 1, RecordedAt: activityAtMinute(0), Status: "draft"},
			{CommitSeq: 2, RecordedAt: activityAtMinute(5), Status: "active"},
			{CommitSeq: 5, RecordedAt: activityAtMinute(40), Status: "archived"},
		},
		ThroughCommit: 4, AsOf: activityAtMinute(40), IdleGap: activityGap(30 * time.Minute), MaxEvents: 16,
	}
	first, err := Calculate(input)
	if err != nil {
		t.Fatal(err)
	}
	second, err := Calculate(input)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(first, second) || !first.ResidentActive || first.Open || len(first.Events) != 2 {
		t.Fatalf("deterministic captured rebuild = %+v / %+v", first, second)
	}
	input.ThroughCommit = 5
	archived, err := Calculate(input)
	if err != nil {
		t.Fatal(err)
	}
	if archived.ResidentActive || len(archived.Events) != 0 {
		t.Fatalf("archive boundary leaked session: %+v", archived)
	}
}

func TestActivitySessionResidentOriginDoesNotStartOrExtend(t *testing.T) {
	residentOnly, err := CalculateEvents([]domain.Event{
		activityEvent(1, "resident_message", 1),
		activityEvent(2, "self_talk", 2),
		activityEvent(3, "outbound_initiative", 3),
	}, activityAtMinute(3), activityGap(30*time.Minute), 16)
	if err != nil {
		t.Fatal(err)
	}
	if residentOnly.Open || len(residentOnly.Events) != 0 {
		t.Fatalf("resident-origin events started a session: %+v", residentOnly)
	}
	withUser, err := CalculateEvents([]domain.Event{
		activityEvent(1, "user_message", 0),
		activityEvent(2, "resident_message", 29),
		activityEvent(3, "self_talk", 30),
		activityEvent(4, "outbound_initiative", 59),
	}, activityAtMinute(60), activityGap(30*time.Minute), 16)
	if err != nil {
		t.Fatal(err)
	}
	if withUser.Open || withUser.LastUserAt != activityAtMinute(0) {
		t.Fatalf("resident-origin activity extended the session: %+v", withUser)
	}
	if !reflect.DeepEqual(withUser.Events, []domain.Event{
		activityEvent(1, "user_message", 0),
		activityEvent(2, "resident_message", 29),
	}) {
		t.Fatalf("resident-origin session members = %+v", withUser.Events)
	}
}

func TestM6I61AutonomyEventsDoNotStartOrExtendActivitySession(t *testing.T) {
	result, err := CalculateEvents([]domain.Event{
		activityEvent(1, "user_message", 0),
		activityEvent(2, "self_talk", 20),
		activityEvent(3, "outbound_initiative", 29),
	}, activityAtMinute(31), activityGap(30*time.Minute), 16)
	if err != nil {
		t.Fatal(err)
	}
	if result.Open || result.LastUserAt != activityAtMinute(0) || len(result.Events) != 1 ||
		result.Events[0].Type != "user_message" {
		t.Fatalf("autonomy events changed user-anchored activity = %+v", result)
	}
	residentOnly, err := CalculateEvents([]domain.Event{
		activityEvent(1, "self_talk", 1), activityEvent(2, "outbound_initiative", 2),
	}, activityAtMinute(2), activityGap(30*time.Minute), 16)
	if err != nil {
		t.Fatal(err)
	}
	if residentOnly.Open || len(residentOnly.Events) != 0 {
		t.Fatalf("autonomy events started activity = %+v", residentOnly)
	}
}

func activityEvent(seq int64, eventType string, minute int64) domain.Event {
	return domain.Event{
		Seq:        canonical.Seq(seq),
		Type:       eventType,
		RecordedAt: activityAtMinute(minute),
	}
}

func activityAtMinute(minute int64) canonical.Instant {
	return canonical.Instant(1_700_000_000_000_000 + time.Duration(minute)*time.Minute/time.Microsecond)
}

func activityGap(value time.Duration) canonical.Duration {
	return canonical.Duration(value / time.Microsecond)
}
