package tts

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"sync"
	"time"

	"mahoroba.local/mahoroba/internal/canonical"
	"mahoroba.local/mahoroba/internal/domain"
)

type DispatcherOptions struct {
	Adapter        Adapter
	Demand         Demand
	Sink           Sink
	RequestTimeout time.Duration
	MaxConcurrency int
	QueueCapacity  int
	MaxAudioBytes  int
	Logger         *slog.Logger
}

// StartGate is the narrow activation barrier shared with HTTP and the other
// provider-capable runtime workers. The TTS package intentionally depends on
// the behavior, not on a host-specific gate implementation.
type StartGate interface {
	Wait(context.Context) error
}

const maxRecentSeenEvents = 64

type residentDedupState struct {
	highWater canonical.Seq
	recent    map[canonical.ID]struct{}
	order     []canonical.ID
}

func (state *residentDedupState) observe(eventID canonical.ID, seq canonical.Seq) bool {
	if _, duplicate := state.recent[eventID]; duplicate || seq <= state.highWater {
		return true
	}
	state.highWater = seq
	if len(state.order) == maxRecentSeenEvents {
		delete(state.recent, state.order[0])
		state.order = state.order[1:]
	}
	state.recent[eventID] = struct{}{}
	state.order = append(state.order, eventID)
	return false
}

type Dispatcher struct {
	adapter     Adapter
	demand      Demand
	sink        Sink
	timeout     time.Duration
	maximum     int
	logger      *slog.Logger
	queue       chan Request
	concurrency int

	mu       sync.Mutex
	seen     map[canonical.ID]*residentDedupState
	started  bool
	stopping bool
	cancel   context.CancelFunc
	done     chan struct{}
	wait     sync.WaitGroup
}

func NewDispatcher(options DispatcherOptions) (*Dispatcher, error) {
	if options.Adapter == nil || options.Demand == nil || options.Sink == nil ||
		options.RequestTimeout <= 0 || options.MaxConcurrency < 1 || options.QueueCapacity < 1 {
		return nil, &Error{Class: ErrorInvalidRequest}
	}
	if err := validateMaximum(options.MaxAudioBytes); err != nil {
		return nil, err
	}
	logger := options.Logger
	if logger == nil {
		logger = slog.New(slog.NewTextHandler(io.Discard, nil))
	}
	return &Dispatcher{
		adapter: options.Adapter, demand: options.Demand, sink: options.Sink,
		timeout: options.RequestTimeout, maximum: options.MaxAudioBytes, logger: logger,
		queue: make(chan Request, options.QueueCapacity), concurrency: options.MaxConcurrency,
		seen: make(map[canonical.ID]*residentDedupState), done: make(chan struct{}),
	}, nil
}

func (dispatcher *Dispatcher) Start(parent context.Context) error {
	return dispatcher.start(parent, nil)
}

// StartGated starts lifecycle ownership immediately, but every dispatcher
// worker waits on the supplied one-shot runtime gate before it can consume a
// request or call the provider.
func (dispatcher *Dispatcher) StartGated(parent context.Context, gate StartGate) error {
	if gate == nil {
		return errors.New("tts: start gate is required")
	}
	return dispatcher.start(parent, gate)
}

func (dispatcher *Dispatcher) start(parent context.Context, gate StartGate) error {
	if parent == nil {
		parent = context.Background()
	}
	dispatcher.mu.Lock()
	defer dispatcher.mu.Unlock()
	if dispatcher.started {
		return errors.New("tts: dispatcher already started")
	}
	if dispatcher.stopping {
		return errors.New("tts: dispatcher is stopped")
	}
	dispatcher.started = true
	runCtx, cancel := context.WithCancel(parent)
	dispatcher.cancel = cancel
	for range dispatcher.concurrency {
		dispatcher.wait.Add(1)
		go dispatcher.gatedWorker(runCtx, gate)
	}
	go func() {
		dispatcher.wait.Wait()
		close(dispatcher.done)
	}()
	return nil
}

func (dispatcher *Dispatcher) gatedWorker(runCtx context.Context, gate StartGate) {
	defer dispatcher.wait.Done()
	if gate != nil {
		if err := gate.Wait(runCtx); err != nil {
			return
		}
	}
	dispatcher.worker(runCtx)
}

// EnqueueCommitted accepts only a fully identified, persisted resident event.
// Every valid event ID is marked seen before demand/queue checks, so a dropped
// event can never be synthesized later through a duplicate callback.
func (dispatcher *Dispatcher) EnqueueCommitted(event domain.Event) bool {
	request, err := requestFromCommitted(event)
	if err != nil {
		return false
	}
	dispatcher.mu.Lock()
	state := dispatcher.seen[event.ResidentID]
	if state == nil {
		state = &residentDedupState{recent: make(map[canonical.ID]struct{})}
		dispatcher.seen[event.ResidentID] = state
	}
	if state.observe(event.ID, event.Seq) {
		dispatcher.mu.Unlock()
		return false
	}
	accepting := dispatcher.started && !dispatcher.stopping
	dispatcher.mu.Unlock()
	if !accepting || !dispatcher.demand.HasAudioSubscribers() {
		return false
	}
	select {
	case dispatcher.queue <- request:
		return true
	default:
		dispatcher.logger.Warn("TTS request dropped", "resident_id", request.ResidentID.String(),
			"event_id", request.SourceEventID.String(), "event_type", request.SourceEventType,
			"error_class", "queue_full")
		return false
	}
}

func requestFromCommitted(event domain.Event) (Request, error) {
	if event.ContentErased || event.GenerationRunID == nil || event.Seq.Validate() != nil {
		return Request{}, &Error{Class: ErrorInvalidRequest}
	}
	request := Request{
		ResidentID: event.ResidentID, SourceEventID: event.ID,
		GenerationRunID: *event.GenerationRunID, SourceEventType: event.Type, Text: event.Content,
	}
	if err := request.Validate(); err != nil {
		return Request{}, err
	}
	return request, nil
}

func (dispatcher *Dispatcher) worker(runCtx context.Context) {
	for {
		if runCtx.Err() != nil {
			return
		}
		select {
		case <-runCtx.Done():
			return
		case request := <-dispatcher.queue:
			if runCtx.Err() != nil {
				return
			}
			dispatcher.synthesize(runCtx, request)
		}
	}
}

func (dispatcher *Dispatcher) synthesize(runCtx context.Context, request Request) {
	started := time.Now()
	requestCtx, cancel := context.WithTimeout(runCtx, dispatcher.timeout)
	audio, err := dispatcher.adapter.Synthesize(requestCtx, request)
	cancel()
	latencyMicros := time.Since(started).Microseconds()
	if err == nil {
		err = audio.Validate(dispatcher.maximum)
	}
	if err == nil {
		err = dispatcher.sink.PublishAudio(Delivery{
			ResidentID: request.ResidentID, SourceEventID: request.SourceEventID,
			GenerationRunID: request.GenerationRunID,
			Audio:           Audio{MIMEType: audio.MIMEType, Data: append([]byte(nil), audio.Data...)},
		})
		if err != nil {
			err = &Error{Class: ErrorDelivery}
		}
	}
	if err != nil {
		dispatcher.logger.Warn("TTS request failed", "resident_id", request.ResidentID.String(),
			"event_id", request.SourceEventID.String(), "event_type", request.SourceEventType,
			"latency_micros", latencyMicros, "error_class", ErrorClassOf(err))
	}
}

func (dispatcher *Dispatcher) Stop() {
	if dispatcher == nil {
		return
	}
	dispatcher.mu.Lock()
	if !dispatcher.stopping {
		dispatcher.stopping = true
		if dispatcher.cancel != nil {
			dispatcher.cancel()
		}
	}
	dispatcher.mu.Unlock()
}

func (dispatcher *Dispatcher) Wait(ctx context.Context) error {
	if dispatcher == nil {
		return nil
	}
	dispatcher.mu.Lock()
	started := dispatcher.started
	dispatcher.mu.Unlock()
	if !started {
		return nil
	}
	if ctx == nil {
		ctx = context.Background()
	}
	select {
	case <-dispatcher.done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
