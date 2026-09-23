package app

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"mahoroba.local/mahoroba/internal/canonical"
	"mahoroba.local/mahoroba/internal/projection"
)

type rejectingCommitNotifier struct {
	mu      sync.Mutex
	commits []canonical.CommitMetadata
}

type failingProjectionStore struct {
	projection.Store
}

func (store failingProjectionStore) Apply(context.Context, projection.ApplyRequest) error {
	return errors.New("injected post-commit Projection failure")
}

func (notifier *rejectingCommitNotifier) NotifyCommit(metadata canonical.CommitMetadata) bool {
	notifier.mu.Lock()
	notifier.commits = append(notifier.commits, metadata)
	notifier.mu.Unlock()
	return false
}

func TestProjectionNotificationFailureDoesNotAffectCanonicalSuccess(t *testing.T) {
	fixture := newApplicationFixture(t, &scriptedGenerator{}, 1)
	notifier := &rejectingCommitNotifier{}
	fixture.application.commitNotifier = notifier

	if err := fixture.application.ArchiveResident(context.Background(), fixture.residentID); err != nil {
		t.Fatal(err)
	}
	notifier.mu.Lock()
	commitCount := len(notifier.commits)
	notifier.mu.Unlock()
	if commitCount == 0 {
		t.Fatal("post-commit notifier was not called")
	}
	resident, err := fixture.store.Canonical().Resident(context.Background(), fixture.residentID)
	if err != nil || resident.Status != "archived" {
		t.Fatalf("durable resident after Projection notification failure = %+v, %v", resident, err)
	}
}

func TestCanonicalCommitSurvivesProjectionUpdateFailure(t *testing.T) {
	fixture := newApplicationFixture(t, &scriptedGenerator{}, 1)
	registry := m4ProjectionRegistry(t)
	surface := fixture.store.Projection()
	projectionErrors := make(chan error, 1)
	coordinator, err := projection.NewCoordinator(projection.CoordinatorOptions{
		Registry: registry, Source: surface, Store: failingProjectionStore{Store: surface},
		Clock: fixture.clock, Timezone: canonical.MustTimezone("UTC"),
		ScanInterval: time.Hour, AsOfRefreshInterval: time.Hour, RebuildRetryInterval: time.Hour,
		MaxStaleness: time.Hour, OnError: func(err error) {
			select {
			case projectionErrors <- err:
			default:
			}
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	fixture.application.commitNotifier = coordinator
	if err := fixture.application.ArchiveResident(context.Background(), fixture.residentID); err != nil {
		t.Fatal(err)
	}
	resident, err := fixture.store.Canonical().Resident(context.Background(), fixture.residentID)
	if err != nil || resident.Status != "archived" {
		t.Fatalf("Canonical archive = %+v, %v", resident, err)
	}
	if err := coordinator.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	select {
	case <-projectionErrors:
	case <-time.After(2 * time.Second):
		t.Fatal("Projection failure was not observed")
	}
	coordinator.Stop()
	waitCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := coordinator.Wait(waitCtx); err != nil {
		t.Fatal(err)
	}
}
