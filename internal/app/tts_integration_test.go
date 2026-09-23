package app

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"mahoroba.local/mahoroba/internal/canonical"
	"mahoroba.local/mahoroba/internal/domain"
	"mahoroba.local/mahoroba/internal/generation"
	"mahoroba.local/mahoroba/internal/tts"
)

type alwaysAudioDemand struct{}

func (alwaysAudioDemand) HasAudioSubscribers() bool { return true }

type rejectingAudioSink struct{}

func (rejectingAudioSink) PublishAudio(tts.Delivery) error {
	return errors.New("audio must not be published after provider failure")
}

type failingTTSAdapter struct {
	calls chan tts.Request
}

func (adapter *failingTTSAdapter) Synthesize(_ context.Context, request tts.Request) (tts.Audio, error) {
	adapter.calls <- request
	return tts.Audio{}, errors.New("synthetic TTS failure")
}

type ttsObserver struct {
	dispatcher *tts.Dispatcher
	deltas     atomic.Int64
}

func (*ttsObserver) UserCommitted(domain.Event)                          {}
func (*ttsObserver) GenerationStarted(canonical.ID, canonical.ID, int64) {}
func (observer *ttsObserver) GenerationDelta(canonical.ID, canonical.ID, string) {
	observer.deltas.Add(1)
}
func (observer *ttsObserver) ResidentCommitted(event domain.Event) {
	observer.dispatcher.EnqueueCommitted(event)
}
func (*ttsObserver) GenerationFailed(canonical.ID, canonical.ID, int64, string, bool) {}

func newFailingTTSObserver(t *testing.T) (*ttsObserver, *failingTTSAdapter) {
	t.Helper()
	adapter := &failingTTSAdapter{calls: make(chan tts.Request, 4)}
	dispatcher, err := tts.NewDispatcher(tts.DispatcherOptions{
		Adapter: adapter, Demand: alwaysAudioDemand{}, Sink: rejectingAudioSink{},
		RequestTimeout: time.Second, MaxConcurrency: 1, QueueCapacity: 2, MaxAudioBytes: 1024,
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
			t.Errorf("wait TTS dispatcher: %v", err)
		}
	})
	return &ttsObserver{dispatcher: dispatcher}, adapter
}

func TestM6RTI27TTSFailureCannotRollbackCommittedTextEvent(t *testing.T) {
	ctx := context.Background()
	generator := &scriptedGenerator{steps: []generatorStep{{text: "durable response", deltas: []string{"durable "}}}}
	fixture := newApplicationFixture(t, generator, 2)
	observer, adapter := newFailingTTSObserver(t)
	fixture.application.SetObserver(observer)
	if _, err := fixture.application.Ingress(ctx, "speak after commit"); err != nil {
		t.Fatal(err)
	}
	if err := fixture.application.ProcessResident(ctx, fixture.residentID); err != nil {
		t.Fatal(err)
	}
	select {
	case request := <-adapter.calls:
		if request.Text != "durable response" || request.SourceEventType != "resident_message" {
			t.Fatalf("TTS request = %#v", request)
		}
	case <-time.After(time.Second):
		t.Fatal("post-commit TTS adapter was not called")
	}
	history, err := fixture.repository.History(ctx, fixture.residentID, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(history) != 2 || history[1].Type != "resident_message" || history[1].Content != "durable response" || history[1].ContentErased {
		t.Fatalf("Canonical history after TTS failure = %#v", history)
	}
}

type provisionalBlockingGenerator struct {
	deltaSent chan struct{}
	release   chan struct{}
}

func (generator *provisionalBlockingGenerator) Stream(ctx context.Context, _ generation.Request, sink generation.DeltaSink) (generation.Result, error) {
	sink(generation.Delta{Text: "provisional only"})
	close(generator.deltaSent)
	select {
	case <-generator.release:
		return generation.Result{Text: "committed output", FinishReason: "stop", ModelVersion: "test-model-v1"}, nil
	case <-ctx.Done():
		return generation.Result{}, ctx.Err()
	}
}

func TestM6RTI11ProvisionalOutputNeverReachesTTS(t *testing.T) {
	ctx := context.Background()
	generator := &provisionalBlockingGenerator{deltaSent: make(chan struct{}), release: make(chan struct{})}
	fixture := newApplicationFixture(t, generator, 2)
	observer, adapter := newFailingTTSObserver(t)
	fixture.application.SetObserver(observer)
	if _, err := fixture.application.Ingress(ctx, "hold the provider"); err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- fixture.application.ProcessResident(ctx, fixture.residentID) }()
	select {
	case <-generator.deltaSent:
	case <-time.After(time.Second):
		t.Fatal("provider did not emit provisional output")
	}
	if observer.deltas.Load() != 1 {
		t.Fatalf("provisional delta count = %d", observer.deltas.Load())
	}
	select {
	case request := <-adapter.calls:
		t.Fatalf("provisional output reached TTS: %#v", request)
	default:
	}
	close(generator.release)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	select {
	case request := <-adapter.calls:
		if request.Text != "committed output" {
			t.Fatalf("TTS request text = %q", request.Text)
		}
	case <-time.After(time.Second):
		t.Fatal("committed output did not reach TTS")
	}
}
