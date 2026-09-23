package app

import (
	"bytes"
	"context"
	"errors"
	"testing"
	"time"

	"mahoroba.local/mahoroba/internal/canonical"
	"mahoroba.local/mahoroba/internal/domain"
)

// These small fakes keep projection-free unit tests independent of the
// production dialogue assembly path. The old context test fixture used the
// pre-COVR discovery signature; retain the fixture with the typed bounded
// request so tests exercise the current Repository contract.
type contextIDs struct {
	generator *canonical.IDGenerator
}

func newContextTestIDs(t *testing.T) *contextIDs {
	t.Helper()
	clock := fixedClock{now: time.Unix(1_700_000_000, 0)}
	generator, err := canonical.NewIDGenerator(clock, bytes.NewReader(bytes.Repeat([]byte{0x41}, 32)))
	if err != nil {
		t.Fatal(err)
	}
	return &contextIDs{generator: generator}
}

func (ids *contextIDs) next(t *testing.T) canonical.ID {
	t.Helper()
	id, err := ids.generator.New()
	if err != nil {
		t.Fatal(err)
	}
	return id
}

type fixedClock struct{ now time.Time }

func (clock fixedClock) Now() time.Time { return clock.now }

func contextEvent(id, residentID canonical.ID, seq int64, eventType string, minute int64, content string) domain.Event {
	return domain.Event{
		ID: id, ResidentID: residentID, Seq: canonical.Seq(seq), Type: eventType,
		RecordedAt: canonical.Instant(1_700_000_000_000_000 + time.Duration(minute)*time.Minute/time.Microsecond),
		RecordedTZ: canonical.MustTimezone("UTC"), Content: content,
	}
}

func contextApplication(t *testing.T, repository domain.Repository, maxInputBytes int) *Application {
	t.Helper()
	ids := newContextTestIDs(t)
	return &Application{
		repository: repository, ids: ids.generator, provider: "test", model: "test-model",
		maxInputBytes: maxInputBytes, maxOutputBytes: 4096,
	}
}

type contextRepository struct {
	resident domain.ResidentSnapshot
	history  []domain.Event
	events   map[canonical.ID]domain.Event
}

func (repository *contextRepository) BootstrapSnapshot(context.Context) (domain.BootstrapState, error) {
	return domain.BootstrapState{}, errors.New("unexpected BootstrapSnapshot call")
}

func (repository *contextRepository) ListResidents(context.Context) ([]domain.ResidentSnapshot, error) {
	return nil, errors.New("unexpected ListResidents call")
}

func (repository *contextRepository) Resident(context.Context, canonical.ID) (domain.ResidentSnapshot, error) {
	return repository.resident, nil
}

func (repository *contextRepository) SelectActiveResident(context.Context, canonical.ID, canonical.Instant, canonical.Timezone) error {
	return errors.New("unexpected SelectActiveResident call")
}

func (repository *contextRepository) ActiveResident(context.Context) (domain.ResidentSnapshot, error) {
	return repository.resident, nil
}

func (repository *contextRepository) History(context.Context, canonical.ID, int) ([]domain.Event, error) {
	return append([]domain.Event(nil), repository.history...), nil
}

func (repository *contextRepository) Event(_ context.Context, _ canonical.ID, eventID canonical.ID) (domain.Event, error) {
	event, ok := repository.events[eventID]
	if !ok {
		return domain.Event{}, errors.New("event not found")
	}
	return event, nil
}

func (*contextRepository) DiscoverDialogueWork(context.Context, canonical.ID, domain.DialogueDiscoveryRequest) (domain.DialogueDiscoveryResult, error) {
	return domain.DialogueDiscoveryResult{}, errors.New("unexpected DiscoverDialogueWork call")
}

func (*contextRepository) RunningAttempts(context.Context, int) ([]domain.RunningAttempt, error) {
	return nil, errors.New("unexpected RunningAttempts call")
}

func (*contextRepository) Generation(context.Context, canonical.ID) (domain.PreparedGeneration, error) {
	return domain.PreparedGeneration{}, errors.New("unexpected Generation call")
}

var _ domain.Repository = (*contextRepository)(nil)
