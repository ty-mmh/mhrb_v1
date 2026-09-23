package app

import (
	"time"

	"mahoroba.local/mahoroba/internal/activity"
	"mahoroba.local/mahoroba/internal/canonical"
	"mahoroba.local/mahoroba/internal/domain"
)

const initialLiveEventLimit = domain.DialogueLiveEventLimit

// CalculateActivitySession is a deterministic, projection-free activity
// session calculator. Only user_message extends the session; resident events
// between user messages are included but cannot keep a session alive by
// themselves.
func CalculateActivitySession(
	events []domain.Event,
	asOf canonical.Instant,
	idleGap time.Duration,
	maxEvents int,
) []domain.Event {
	if idleGap <= 0 || maxEvents < 1 {
		return nil
	}
	session, err := activity.CalculateEvents(
		events,
		asOf,
		canonical.Duration(idleGap/time.Microsecond),
		maxEvents,
	)
	if err != nil {
		return nil
	}
	return session.Events
}
