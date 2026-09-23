package canonical_test

import (
	"bytes"
	"context"
	"errors"
	"testing"
	"time"

	"mahoroba.local/mahoroba/internal/canonical"
	"mahoroba.local/mahoroba/internal/operationalmetrics"
	"mahoroba.local/mahoroba/internal/testsupport"
)

func TestM7WriterOperationalObserverTracksExactLifecycle(t *testing.T) {
	clock := testsupport.NewManualClock(time.Unix(1_700_000_000, 0))
	ids, err := canonical.NewIDGenerator(clock, bytes.NewReader(bytes.Repeat([]byte{0x39}, 30)))
	if err != nil {
		t.Fatal(err)
	}
	metrics := operationalmetrics.New()
	writer, err := canonical.OpenWriter(context.Background(), canonical.WriterOptions{
		Backend: testsupport.NewMemoryCanonicalBackend(), IDs: ids, Clock: clock,
		Timezone: canonical.MustTimezone("UTC"), QueueCapacity: 2, Observer: metrics,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = writer.Close(context.Background()) })

	if _, err := writer.Submit(context.Background(), command{name: "commit", scope: canonical.GlobalScope()}); err != nil {
		t.Fatal(err)
	}
	if _, err := writer.Submit(context.Background(), command{
		name: "no-mutation", scope: canonical.GlobalScope(),
		execute: func(context.Context, canonical.CanonicalUoW) (any, error) { return nil, canonical.ErrNoMutation },
	}); err != nil {
		t.Fatal(err)
	}
	wantFailure := errors.New("closed failure without raw metrics label")
	if _, err := writer.Submit(context.Background(), command{
		name: "failure", scope: canonical.GlobalScope(),
		execute: func(context.Context, canonical.CanonicalUoW) (any, error) { return nil, wantFailure },
	}); !errors.Is(err, wantFailure) {
		t.Fatalf("failure = %v", err)
	}

	snapshot := metrics.Snapshot().Writer
	if snapshot.AcceptedTotal != 3 || snapshot.AcceptedCurrent != 0 || snapshot.QueuedCurrent != 0 ||
		snapshot.InFlightCurrent != 0 || snapshot.CommittedTotal != 1 || snapshot.NoMutationTotal != 1 || snapshot.FailedTotal != 1 {
		t.Fatalf("writer metrics = %+v", snapshot)
	}
}
