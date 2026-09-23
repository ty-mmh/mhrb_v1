package sqlite

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestProjectionPriorityGateYieldsToWaitingCanonicalWrite(t *testing.T) {
	gate := newWritePriorityGate()
	firstRelease, err := gate.acquireProjection(context.Background())
	if err != nil {
		t.Fatal(err)
	}

	projectionAcquired := make(chan func(), 1)
	go func() {
		release, acquireErr := gate.acquireProjection(context.Background())
		if acquireErr == nil {
			projectionAcquired <- release
		}
	}()

	canonicalAcquired := make(chan func(), 1)
	go func() {
		release, acquireErr := gate.acquireHigh(context.Background())
		if acquireErr == nil {
			canonicalAcquired <- release
		}
	}()

	deadline := time.Now().Add(time.Second)
	for {
		gate.mu.Lock()
		waiting := gate.highWaiting
		gate.mu.Unlock()
		if waiting == 1 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("Canonical writer did not enter the priority queue")
		}
		time.Sleep(time.Millisecond)
	}

	firstRelease()
	var canonicalRelease func()
	select {
	case canonicalRelease = <-canonicalAcquired:
	case <-projectionAcquired:
		t.Fatal("Projection write started while a Canonical write was waiting")
	case <-time.After(time.Second):
		t.Fatal("Canonical write did not acquire the gate")
	}
	canonicalRelease()

	select {
	case projectionRelease := <-projectionAcquired:
		projectionRelease()
	case <-time.After(time.Second):
		t.Fatal("Projection write did not resume after Canonical release")
	}
}

func TestProjectionPriorityGateHonorsCancellation(t *testing.T) {
	gate := newWritePriorityGate()
	release, err := gate.acquireHigh(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer release()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := gate.acquireProjection(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("projection acquire error = %v, want context.Canceled", err)
	}
}
