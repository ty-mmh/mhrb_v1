// Package activity reconstructs Activity Sessions from Canonical inputs.
package activity

import (
	"fmt"
	"sort"

	"mahoroba.local/mahoroba/internal/canonical"
	"mahoroba.local/mahoroba/internal/domain"
)

// Event is an event together with its store-global Canonical ordering cursor.
// CommitSeq may be zero for callers that only have resident-local event order.
type Event struct {
	CommitSeq canonical.CommitSeq
	Value     domain.Event
}

// StatusTransition is the lifecycle state established by a Canonical Commit.
// Activity is possible only after the most recent transition to active and
// before any later transition away from active.
type StatusTransition struct {
	CommitSeq  canonical.CommitSeq
	RecordedAt canonical.Instant
	Status     string
}

// Input is the complete, immutable input to Activity Session reconstruction.
// ThroughCommit is optional for ad-hoc callers, but Projection and store
// callers set it to their captured Canonical target.
type Input struct {
	Events            []Event
	StatusTransitions []StatusTransition
	InitialStatus     string
	ThroughCommit     canonical.CommitSeq
	AsOf              canonical.Instant
	IdleGap           canonical.Duration
	MaxEvents         int
}

// Session describes the latest Activity Session at AsOf. Events contains a
// bounded Canonical-order tail from the user-established session boundary;
// later resident messages remain members but never affect LastUserAt or Open.
type Session struct {
	Events         []domain.Event
	StartSeq       canonical.Seq
	StartedAt      canonical.Instant
	LastUserAt     canonical.Instant
	Open           bool
	ResidentActive bool
}

// Calculate deterministically reconstructs the latest Activity Session. Event
// order is Canonical order (CommitSeq, then resident Seq); recorded_at is used
// only for the policy gap comparison. Consequently, a host-clock rollback can
// make a gap look shorter, but repeated evaluation of the same Canonical input
// always produces the same boundary.
func Calculate(input Input) (Session, error) {
	if input.IdleGap.Microseconds() <= 0 {
		return Session{}, fmt.Errorf("activity: idle gap must be positive")
	}
	if input.MaxEvents < 1 {
		return Session{}, fmt.Errorf("activity: max events must be positive")
	}

	active, activeBoundary, err := lifecycleAt(input)
	if err != nil {
		return Session{}, err
	}
	if !active {
		return Session{}, nil
	}

	events := append([]Event(nil), input.Events...)
	sort.SliceStable(events, func(left, right int) bool {
		if events[left].CommitSeq > 0 && events[right].CommitSeq > 0 && events[left].CommitSeq != events[right].CommitSeq {
			return events[left].CommitSeq < events[right].CommitSeq
		}
		leftSeq, rightSeq := events[left].Value.Seq, events[right].Value.Seq
		if leftSeq > 0 && rightSeq > 0 && leftSeq != rightSeq {
			return leftSeq < rightSeq
		}
		return false
	})

	eligible := make([]Event, 0, len(events))
	for _, event := range events {
		if input.ThroughCommit > 0 && event.CommitSeq > input.ThroughCommit {
			continue
		}
		if event.Value.RecordedAt > input.AsOf || !afterBoundary(event, activeBoundary) {
			continue
		}
		if event.Value.Type != "user_message" && event.Value.Type != "resident_message" {
			continue
		}
		eligible = append(eligible, event)
	}
	lastUser := -1
	for index := len(eligible) - 1; index >= 0; index-- {
		if eligible[index].Value.Type == "user_message" {
			lastUser = index
			break
		}
	}
	if lastUser < 0 {
		return Session{ResidentActive: true}, nil
	}

	start := lastUser
	newerUserTime := eligible[lastUser].Value.RecordedAt
	for index := lastUser - 1; index >= 0; index-- {
		if eligible[index].Value.Type != "user_message" {
			continue
		}
		olderUserTime := eligible[index].Value.RecordedAt
		if gapAtLeast(newerUserTime, olderUserTime, input.IdleGap.Microseconds()) {
			break
		}
		start = index
		newerUserTime = olderUserTime
	}

	// Resident-origin events do not move the boundary, but they remain members
	// of the session after its last user message.
	selected := eligible[start:]
	if len(selected) > input.MaxEvents {
		selected = selected[len(selected)-input.MaxEvents:]
	}
	result := Session{
		Events:         make([]domain.Event, 0, len(selected)),
		StartSeq:       eligible[start].Value.Seq,
		StartedAt:      eligible[start].Value.RecordedAt,
		LastUserAt:     eligible[lastUser].Value.RecordedAt,
		ResidentActive: true,
	}
	for _, event := range selected {
		result.Events = append(result.Events, event.Value)
	}
	result.Open = !gapAtLeast(input.AsOf, result.LastUserAt, input.IdleGap.Microseconds())
	return result, nil
}

// CalculateEvents is the compatibility entry point for callers that already
// established that the resident is active and only have resident-local event
// order available.
func CalculateEvents(events []domain.Event, asOf canonical.Instant, idleGap canonical.Duration, maxEvents int) (Session, error) {
	records := make([]Event, 0, len(events))
	for _, event := range events {
		records = append(records, Event{Value: event})
	}
	return Calculate(Input{
		Events:        records,
		InitialStatus: "active",
		AsOf:          asOf,
		IdleGap:       idleGap,
		MaxEvents:     maxEvents,
	})
}

type lifecycleBoundary struct {
	present    bool
	commitSeq  canonical.CommitSeq
	recordedAt canonical.Instant
}

func lifecycleAt(input Input) (bool, lifecycleBoundary, error) {
	if input.InitialStatus != "" && !knownStatus(input.InitialStatus) {
		return false, lifecycleBoundary{}, fmt.Errorf("activity: unknown initial resident status %q", input.InitialStatus)
	}
	transitions := append([]StatusTransition(nil), input.StatusTransitions...)
	sort.SliceStable(transitions, func(left, right int) bool {
		if transitions[left].CommitSeq > 0 && transitions[right].CommitSeq > 0 && transitions[left].CommitSeq != transitions[right].CommitSeq {
			return transitions[left].CommitSeq < transitions[right].CommitSeq
		}
		return transitions[left].RecordedAt < transitions[right].RecordedAt
	})
	status := input.InitialStatus
	var boundary lifecycleBoundary
	for _, transition := range transitions {
		if !knownStatus(transition.Status) {
			return false, lifecycleBoundary{}, fmt.Errorf("activity: unknown resident status %q", transition.Status)
		}
		if input.ThroughCommit > 0 && transition.CommitSeq > input.ThroughCommit {
			continue
		}
		if transition.RecordedAt > input.AsOf {
			continue
		}
		status = transition.Status
		if status == "active" {
			boundary = lifecycleBoundary{present: true, commitSeq: transition.CommitSeq, recordedAt: transition.RecordedAt}
		} else {
			boundary = lifecycleBoundary{}
		}
	}
	return status == "active", boundary, nil
}

func knownStatus(status string) bool {
	switch status {
	case "draft", "active", "archived", "erased":
		return true
	default:
		return false
	}
}

func afterBoundary(event Event, boundary lifecycleBoundary) bool {
	if !boundary.present {
		return true
	}
	if boundary.commitSeq > 0 && event.CommitSeq > 0 {
		return event.CommitSeq > boundary.commitSeq
	}
	return event.Value.RecordedAt >= boundary.recordedAt
}

func gapAtLeast(newer, older canonical.Instant, idleMicros int64) bool {
	if newer < older {
		return false
	}
	return uint64(newer)-uint64(older) >= uint64(idleMicros)
}
