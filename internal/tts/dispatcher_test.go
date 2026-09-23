package tts

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"mahoroba.local/mahoroba/internal/canonical"
	"mahoroba.local/mahoroba/internal/domain"
)

type testDemand struct{ enabled atomic.Bool }

func (demand *testDemand) HasAudioSubscribers() bool { return demand.enabled.Load() }

type testStartGate struct{ open chan struct{} }

func (gate *testStartGate) Wait(ctx context.Context) error {
	select {
	case <-gate.open:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

type testAdapter struct {
	calls   atomic.Int64
	started chan Request
	release <-chan struct{}
	err     error
}

type contextAdapter struct {
	started  chan struct{}
	finished chan error
}

func (adapter *contextAdapter) Synthesize(ctx context.Context, _ Request) (Audio, error) {
	close(adapter.started)
	<-ctx.Done()
	adapter.finished <- ctx.Err()
	return Audio{}, ctx.Err()
}

func (adapter *testAdapter) Synthesize(ctx context.Context, request Request) (Audio, error) {
	adapter.calls.Add(1)
	if adapter.started != nil {
		adapter.started <- request
	}
	if adapter.release != nil {
		select {
		case <-adapter.release:
		case <-ctx.Done():
			return Audio{}, ctx.Err()
		}
	}
	if adapter.err != nil {
		return Audio{}, adapter.err
	}
	return Audio{MIMEType: MIMETypeWAV, Data: validWAV()}, nil
}

type testSink struct {
	mu         sync.Mutex
	deliveries []Delivery
	ready      chan struct{}
	err        error
}

func (sink *testSink) PublishAudio(delivery Delivery) error {
	sink.mu.Lock()
	sink.deliveries = append(sink.deliveries, delivery)
	sink.mu.Unlock()
	if sink.ready != nil {
		select {
		case sink.ready <- struct{}{}:
		default:
		}
	}
	return sink.err
}

func committedEvent(t *testing.T, eventID string) domain.Event {
	return committedEventWithSeq(t, eventID, 1, "01ARZ3NDEKTSV4RRFFQ69G5FAV")
}

func committedEventWithSeq(t *testing.T, eventID string, seqValue int64, residentID string) domain.Event {
	t.Helper()
	seq, err := canonical.NewSeq(seqValue)
	if err != nil {
		t.Fatal(err)
	}
	runID := testCanonicalID(t, "01ARZ3NDEKTSV4RRFFQ69G5FAX")
	return domain.Event{
		ID: testCanonicalID(t, eventID), ResidentID: testCanonicalID(t, residentID),
		Seq: seq, Type: "resident_message", GenerationRunID: &runID, Content: "committed response",
	}
}

func newTestDispatcher(t *testing.T, adapter Adapter, demand Demand, sink Sink, capacity int) *Dispatcher {
	t.Helper()
	dispatcher, err := NewDispatcher(DispatcherOptions{
		Adapter: adapter, Demand: demand, Sink: sink, RequestTimeout: time.Second,
		MaxConcurrency: 1, QueueCapacity: capacity, MaxAudioBytes: 1024,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := dispatcher.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		dispatcher.Stop()
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		if err := dispatcher.Wait(ctx); err != nil {
			t.Errorf("wait dispatcher: %v", err)
		}
	})
	return dispatcher
}

func TestDispatcherRequiresDemandAndDeduplicatesBeforeDemand(t *testing.T) {
	demand := &testDemand{}
	adapter := &testAdapter{}
	sink := &testSink{}
	dispatcher := newTestDispatcher(t, adapter, demand, sink, 2)
	event := committedEvent(t, "01ARZ3NDEKTSV4RRFFQ69G5FAW")
	if dispatcher.EnqueueCommitted(event) {
		t.Fatal("event was queued without an audio subscriber")
	}
	demand.enabled.Store(true)
	if dispatcher.EnqueueCommitted(event) {
		t.Fatal("duplicate event was queued after demand appeared")
	}
	time.Sleep(10 * time.Millisecond)
	if adapter.calls.Load() != 0 {
		t.Fatalf("adapter calls = %d", adapter.calls.Load())
	}
}

func TestDispatcherCannotStartAfterStop(t *testing.T) {
	dispatcher, err := NewDispatcher(DispatcherOptions{
		Adapter: &testAdapter{}, Demand: &testDemand{}, Sink: &testSink{}, RequestTimeout: time.Second,
		MaxConcurrency: 1, QueueCapacity: 1, MaxAudioBytes: 1024,
	})
	if err != nil {
		t.Fatal(err)
	}
	dispatcher.Stop()
	if err := dispatcher.Start(context.Background()); err == nil {
		t.Fatal("Start succeeded after Stop")
	}
}

func TestDispatcherPublishesOnceForCommittedResidentEvents(t *testing.T) {
	demand := &testDemand{}
	demand.enabled.Store(true)
	adapter := &testAdapter{}
	sink := &testSink{ready: make(chan struct{}, 1)}
	dispatcher := newTestDispatcher(t, adapter, demand, sink, 2)
	event := committedEvent(t, "01ARZ3NDEKTSV4RRFFQ69G5FAW")
	if !dispatcher.EnqueueCommitted(event) || dispatcher.EnqueueCommitted(event) {
		t.Fatal("event was not accepted exactly once")
	}
	select {
	case <-sink.ready:
	case <-time.After(time.Second):
		t.Fatal("audio was not published")
	}
	sink.mu.Lock()
	delivery := sink.deliveries[0]
	sink.mu.Unlock()
	if delivery.SourceEventID != event.ID || delivery.GenerationRunID != *event.GenerationRunID {
		t.Fatalf("delivery = %#v", delivery)
	}
	if adapter.calls.Load() != 1 {
		t.Fatalf("adapter calls = %d", adapter.calls.Load())
	}
}

func TestM7DispatcherDoesNotCallProviderBeforeRuntimeStartGate(t *testing.T) {
	demand := &testDemand{}
	demand.enabled.Store(true)
	adapter := &testAdapter{}
	sink := &testSink{ready: make(chan struct{}, 1)}
	dispatcher, err := NewDispatcher(DispatcherOptions{
		Adapter: adapter, Demand: demand, Sink: sink, RequestTimeout: time.Second,
		MaxConcurrency: 1, QueueCapacity: 1, MaxAudioBytes: 1024,
	})
	if err != nil {
		t.Fatal(err)
	}
	gate := &testStartGate{open: make(chan struct{})}
	if err := dispatcher.StartGated(context.Background(), gate); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		dispatcher.Stop()
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		if err := dispatcher.Wait(ctx); err != nil {
			t.Errorf("wait dispatcher: %v", err)
		}
	})
	if !dispatcher.EnqueueCommitted(committedEvent(t, "01ARZ3NDEKTSV4RRFFQ69G5FAW")) {
		t.Fatal("gated dispatcher did not accept the bounded request")
	}
	time.Sleep(20 * time.Millisecond)
	if adapter.calls.Load() != 0 {
		t.Fatalf("provider calls before gate = %d", adapter.calls.Load())
	}
	close(gate.open)
	select {
	case <-sink.ready:
	case <-time.After(time.Second):
		t.Fatal("gated provider did not start after activation")
	}
}

func TestDispatcherDeduplicatesByResidentSeqAndKeepsRecentSetBounded(t *testing.T) {
	demand := &testDemand{}
	demand.enabled.Store(true)
	dispatcher := newTestDispatcher(t, &testAdapter{}, demand, &testSink{}, maxRecentSeenEvents+1)
	residentID := "01ARZ3NDEKTSV4RRFFQ69G5FAV"
	events := make([]domain.Event, 0, maxRecentSeenEvents+1)
	for seq := int64(1); seq <= maxRecentSeenEvents+1; seq++ {
		event := committedEventWithSeq(t, fmt.Sprintf("01ARZ3NDEKTSV4RRFFQ69G5F%02X", seq), seq, residentID)
		if !dispatcher.EnqueueCommitted(event) {
			t.Fatalf("event seq %d was not accepted", seq)
		}
		events = append(events, event)
	}

	dispatcher.mu.Lock()
	state := dispatcher.seen[events[0].ResidentID]
	recentLength := len(state.recent)
	orderLength := len(state.order)
	highWater := state.highWater
	dispatcher.mu.Unlock()
	if highWater != events[len(events)-1].Seq {
		t.Fatalf("high-water = %s, want %s", highWater, events[len(events)-1].Seq)
	}
	if recentLength > maxRecentSeenEvents || orderLength > maxRecentSeenEvents {
		t.Fatalf("recent state is unbounded: map=%d order=%d", recentLength, orderLength)
	}
	if dispatcher.EnqueueCommitted(events[0]) {
		t.Fatal("old event was accepted after the resident high-water advanced")
	}
}

func TestDispatcherRejectsOutOfOrderSeqAndTracksResidentsIndependently(t *testing.T) {
	demand := &testDemand{}
	demand.enabled.Store(true)
	dispatcher := newTestDispatcher(t, &testAdapter{}, demand, &testSink{}, 4)
	first := committedEventWithSeq(t, "01ARZ3NDEKTSV4RRFFQ69G5FAY", 3, "01ARZ3NDEKTSV4RRFFQ69G5FAV")
	older := committedEventWithSeq(t, "01ARZ3NDEKTSV4RRFFQ69G5FAZ", 2, "01ARZ3NDEKTSV4RRFFQ69G5FAV")
	otherResident := committedEventWithSeq(t, "01ARZ3NDEKTSV4RRFFQ69G5FB0", 1, "01ARZ3NDEKTSV4RRFFQ69G5FBW")
	if !dispatcher.EnqueueCommitted(first) {
		t.Fatal("first event was not accepted")
	}
	if dispatcher.EnqueueCommitted(older) {
		t.Fatal("out-of-order event was accepted")
	}
	if dispatcher.EnqueueCommitted(first) {
		t.Fatal("duplicate event was accepted")
	}
	if !dispatcher.EnqueueCommitted(otherResident) {
		t.Fatal("event for another resident was not accepted")
	}
	dispatcher.mu.Lock()
	residentCount := len(dispatcher.seen)
	dispatcher.mu.Unlock()
	if residentCount != 2 {
		t.Fatalf("tracked resident count = %d, want 2", residentCount)
	}
}

func TestDispatcherRejectsSelfTalkAndProvisionalShapes(t *testing.T) {
	demand := &testDemand{}
	demand.enabled.Store(true)
	adapter := &testAdapter{}
	dispatcher := newTestDispatcher(t, adapter, demand, &testSink{}, 2)
	event := committedEvent(t, "01ARZ3NDEKTSV4RRFFQ69G5FAW")
	event.Type = "self_talk"
	if dispatcher.EnqueueCommitted(event) {
		t.Fatal("self-talk was queued for TTS")
	}
	event = committedEvent(t, "01ARZ3NDEKTSV4RRFFQ69G5FAY")
	event.GenerationRunID = nil
	if dispatcher.EnqueueCommitted(event) {
		t.Fatal("event without a durable generation run was queued")
	}
	if adapter.calls.Load() != 0 {
		t.Fatalf("adapter calls = %d", adapter.calls.Load())
	}
}

func TestDispatcherQueueIsBoundedAndNonBlocking(t *testing.T) {
	demand := &testDemand{}
	demand.enabled.Store(true)
	release := make(chan struct{})
	started := make(chan Request, 1)
	adapter := &testAdapter{started: started, release: release}
	dispatcher := newTestDispatcher(t, adapter, demand, &testSink{}, 1)
	if !dispatcher.EnqueueCommitted(committedEventWithSeq(t, "01ARZ3NDEKTSV4RRFFQ69G5FAW", 1, "01ARZ3NDEKTSV4RRFFQ69G5FAV")) {
		t.Fatal("first event was not queued")
	}
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("first synthesis did not start")
	}
	if !dispatcher.EnqueueCommitted(committedEventWithSeq(t, "01ARZ3NDEKTSV4RRFFQ69G5FAY", 2, "01ARZ3NDEKTSV4RRFFQ69G5FAV")) {
		t.Fatal("second event did not occupy the bounded queue")
	}
	startedAt := time.Now()
	if dispatcher.EnqueueCommitted(committedEventWithSeq(t, "01ARZ3NDEKTSV4RRFFQ69G5FAZ", 3, "01ARZ3NDEKTSV4RRFFQ69G5FAV")) {
		t.Fatal("third event was accepted into a full queue")
	}
	if time.Since(startedAt) > 100*time.Millisecond {
		t.Fatal("full queue blocked producer")
	}
	close(release)
}

func TestDispatcherTimeoutAndAdapterFailureAreNonFatal(t *testing.T) {
	demand := &testDemand{}
	demand.enabled.Store(true)
	adapter := &testAdapter{err: errors.New("provider failure")}
	sink := &testSink{ready: make(chan struct{}, 1)}
	dispatcher := newTestDispatcher(t, adapter, demand, sink, 1)
	if !dispatcher.EnqueueCommitted(committedEvent(t, "01ARZ3NDEKTSV4RRFFQ69G5FAW")) {
		t.Fatal("event was not queued")
	}
	deadline := time.Now().Add(time.Second)
	for adapter.calls.Load() == 0 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if adapter.calls.Load() != 1 {
		t.Fatal("adapter was not called")
	}
	select {
	case <-sink.ready:
		t.Fatal("failed audio reached sink")
	default:
	}
}

func TestDispatcherAppliesTimeoutAndShutdownCancelsInflight(t *testing.T) {
	for _, test := range []struct {
		name    string
		timeout time.Duration
		stop    bool
		want    error
	}{
		{name: "timeout", timeout: 20 * time.Millisecond, want: context.DeadlineExceeded},
		{name: "shutdown", timeout: time.Hour, stop: true, want: context.Canceled},
	} {
		t.Run(test.name, func(t *testing.T) {
			adapter := &contextAdapter{started: make(chan struct{}), finished: make(chan error, 1)}
			demand := &testDemand{}
			demand.enabled.Store(true)
			sink := &testSink{ready: make(chan struct{}, 1)}
			dispatcher, err := NewDispatcher(DispatcherOptions{
				Adapter: adapter, Demand: demand, Sink: sink, RequestTimeout: test.timeout,
				MaxConcurrency: 1, QueueCapacity: 1, MaxAudioBytes: 1024,
			})
			if err != nil {
				t.Fatal(err)
			}
			if err := dispatcher.Start(context.Background()); err != nil {
				t.Fatal(err)
			}
			if !dispatcher.EnqueueCommitted(committedEvent(t, "01ARZ3NDEKTSV4RRFFQ69G5FAW")) {
				t.Fatal("event was not queued")
			}
			select {
			case <-adapter.started:
			case <-time.After(time.Second):
				t.Fatal("synthesis did not start")
			}
			if test.stop {
				dispatcher.Stop()
			}
			select {
			case got := <-adapter.finished:
				if !errors.Is(got, test.want) {
					t.Fatalf("adapter context error = %v, want %v", got, test.want)
				}
			case <-time.After(time.Second):
				t.Fatal("inflight synthesis was not canceled")
			}
			dispatcher.Stop()
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			if err := dispatcher.Wait(ctx); err != nil {
				t.Fatal(err)
			}
			select {
			case <-sink.ready:
				t.Fatal("canceled synthesis reached sink")
			default:
			}
		})
	}
}
