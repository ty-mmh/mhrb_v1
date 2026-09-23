package httpui

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"time"
	"unicode/utf8"

	"mahoroba.local/mahoroba/internal/canonical"
	"mahoroba.local/mahoroba/internal/domain"
	"mahoroba.local/mahoroba/internal/tts"
)

// StreamEventType is the wire-level SSE event name. The hub is deliberately
// ephemeral: durable truth remains in the canonical event ledger.
type StreamEventType string

const (
	EventProvisional StreamEventType = "provisional"
	EventCommitted   StreamEventType = "committed"
	EventError       StreamEventType = "error"
	EventStatus      StreamEventType = "status"
	EventAudio       StreamEventType = "audio"
)

// StreamEvent is sent as JSON in an SSE data field. Sequence is process-local
// and only helps a connected client deduplicate notifications; it is not a
// canonical cursor and is intentionally reset on restart.
type StreamEvent struct {
	Sequence        uint64          `json:"sequence"`
	Type            StreamEventType `json:"type"`
	ResidentID      string          `json:"resident_id,omitempty"`
	EventID         string          `json:"event_id,omitempty"`
	GenerationRunID string          `json:"generation_run_id,omitempty"`
	EventType       string          `json:"event_type,omitempty"`
	Text            string          `json:"text,omitempty"`
	Status          string          `json:"status,omitempty"`
	ErrorClass      string          `json:"error_class,omitempty"`
	Message         string          `json:"message,omitempty"`
	MIMEType        string          `json:"mime_type,omitempty"`
	AudioBase64     string          `json:"audio_base64,omitempty"`
	EmittedAt       time.Time       `json:"emitted_at"`
}

func (event StreamEvent) validate() error {
	switch event.Type {
	case EventProvisional, EventCommitted, EventError, EventStatus, EventAudio:
	default:
		return fmt.Errorf("httpui: invalid stream event type %q", event.Type)
	}
	for name, value := range map[string]string{
		"resident_id":       event.ResidentID,
		"event_id":          event.EventID,
		"generation_run_id": event.GenerationRunID,
		"event_type":        event.EventType,
		"text":              event.Text,
		"status":            event.Status,
		"error_class":       event.ErrorClass,
		"message":           event.Message,
		"mime_type":         event.MIMEType,
		"audio_base64":      event.AudioBase64,
	} {
		if !utf8.ValidString(value) {
			return fmt.Errorf("httpui: %s is not UTF-8", name)
		}
	}
	if event.Type == EventAudio {
		if event.EventType != "" || event.Text != "" || event.Status != "" || event.ErrorClass != "" || event.Message != "" {
			return errors.New("httpui: audio event contains non-audio fields")
		}
		for _, value := range []string{event.ResidentID, event.EventID, event.GenerationRunID} {
			if _, err := canonical.ParseID(value); err != nil {
				return fmt.Errorf("httpui: invalid audio identity: %w", err)
			}
		}
		if event.MIMEType != tts.MIMETypeWAV {
			return errors.New("httpui: audio event must contain audio/wav")
		}
		if _, err := tts.DecodeBase64WAV(event.AudioBase64, tts.HardMaxAudioBytes); err != nil {
			return fmt.Errorf("httpui: invalid audio payload: %w", err)
		}
	} else {
		if event.MIMEType != "" || event.AudioBase64 != "" {
			return errors.New("httpui: non-audio event contains audio fields")
		}
		if event.Type == EventCommitted {
			if !conversationVisibleEventType(event.EventType) {
				return fmt.Errorf("httpui: event type %q is not conversation-visible", event.EventType)
			}
			for _, value := range []string{event.ResidentID, event.EventID} {
				if _, err := canonical.ParseID(value); err != nil {
					return fmt.Errorf("httpui: invalid committed event identity: %w", err)
				}
			}
			if event.EventType != "user_message" {
				if _, err := canonical.ParseID(event.GenerationRunID); err != nil {
					return fmt.Errorf("httpui: generated event requires a run identity: %w", err)
				}
			}
		}
	}
	return nil
}

func (event StreamEvent) json() ([]byte, error) {
	// Only Hub subscriptions can produce an event consumed by the handler, and
	// Hub validates every public publication before enqueueing it. Avoid decoding
	// and validating a potentially 8 MiB WAV a second time for every subscriber.
	return json.Marshal(event)
}

func conversationVisibleEventType(eventType string) bool {
	switch eventType {
	case "user_message", "resident_message", "outbound_initiative":
		return true
	default:
		return false
	}
}

// Hub fans transient progress notifications out to connected browsers. Each
// subscriber has a bounded buffer. A slow browser loses its oldest transient
// notification and can always recover by reloading canonical history.
type Hub struct {
	mu          sync.Mutex
	nextID      uint64
	nextSubID   uint64
	bufferSize  int
	closed      bool
	subscribers map[uint64]hubSubscriber
}

type hubSubscriber struct {
	events chan StreamEvent
	audio  bool
}

type SubscriberCapabilities struct {
	Audio bool
}

const audioSubscriberBufferSize = 2

func NewHub(bufferSize int) *Hub {
	if bufferSize < 1 {
		bufferSize = 64
	}
	return &Hub{
		bufferSize:  bufferSize,
		subscribers: make(map[uint64]hubSubscriber),
	}
}

// Publish validates an event and broadcasts it without blocking a producer.
func (hub *Hub) Publish(event StreamEvent) error {
	if hub == nil {
		return errors.New("httpui: nil event hub")
	}
	if err := event.validate(); err != nil {
		return err
	}
	return hub.publishValidated(event)
}

// publishValidated is reserved for values assembled from an already-validated
// typed input, such as tts.Delivery. All general callers must use Publish.
func (hub *Hub) publishValidated(event StreamEvent) error {
	if hub == nil {
		return errors.New("httpui: nil event hub")
	}
	hub.mu.Lock()
	defer hub.mu.Unlock()
	if hub.closed {
		return errors.New("httpui: event hub is closed")
	}
	hub.nextID++
	event.Sequence = hub.nextID
	if event.EmittedAt.IsZero() {
		event.EmittedAt = time.Now().UTC()
	} else {
		event.EmittedAt = event.EmittedAt.UTC()
	}
	for _, subscriber := range hub.subscribers {
		if event.Type == EventAudio && !subscriber.audio {
			continue
		}
		select {
		case subscriber.events <- event:
		default:
			// Provisional output is disposable. Retain the freshest notification
			// so a committed or error event is not trapped behind stale chunks.
			select {
			case <-subscriber.events:
			default:
			}
			select {
			case subscriber.events <- event:
			default:
			}
		}
	}
	return nil
}

// PublishProvisional emits one provider text delta. Consumers concatenate
// deltas for a run and discard the provisional text when a committed event
// arrives.
func (hub *Hub) PublishProvisional(purpose domain.GenerationPurpose, residentID, runID canonical.ID, text string) error {
	if purpose.Effective() != domain.GenerationPurposeDialogue {
		return fmt.Errorf("httpui: generation purpose %q cannot publish provisional dialogue output", purpose)
	}
	if err := residentID.Validate(); err != nil {
		return fmt.Errorf("httpui: invalid resident ID: %w", err)
	}
	if err := runID.Validate(); err != nil {
		return fmt.Errorf("httpui: invalid generation run ID: %w", err)
	}
	return hub.Publish(StreamEvent{
		Type:            EventProvisional,
		ResidentID:      residentID.String(),
		GenerationRunID: runID.String(),
		Text:            text,
	})
}

func (hub *Hub) PublishCommitted(event domain.Event) error {
	if err := event.ResidentID.Validate(); err != nil {
		return fmt.Errorf("httpui: invalid resident ID: %w", err)
	}
	if err := event.ID.Validate(); err != nil {
		return fmt.Errorf("httpui: invalid event ID: %w", err)
	}
	if !conversationVisibleEventType(event.Type) {
		return fmt.Errorf("httpui: event type %q is not conversation-visible", event.Type)
	}
	streamEvent := StreamEvent{
		Type:       EventCommitted,
		ResidentID: event.ResidentID.String(),
		EventID:    event.ID.String(),
		EventType:  event.Type,
		Text:       event.Content,
	}
	if event.GenerationRunID != nil {
		if err := event.GenerationRunID.Validate(); err != nil {
			return fmt.Errorf("httpui: invalid generation run ID: %w", err)
		}
		streamEvent.GenerationRunID = event.GenerationRunID.String()
	}
	return hub.Publish(streamEvent)
}

func (hub *Hub) PublishAudio(delivery tts.Delivery) error {
	if err := delivery.Validate(tts.HardMaxAudioBytes); err != nil {
		return fmt.Errorf("httpui: invalid audio delivery: %w", err)
	}
	return hub.publishValidated(StreamEvent{
		Type: EventAudio, ResidentID: delivery.ResidentID.String(),
		EventID: delivery.SourceEventID.String(), GenerationRunID: delivery.GenerationRunID.String(),
		MIMEType:    delivery.Audio.MIMEType,
		AudioBase64: base64.StdEncoding.EncodeToString(delivery.Audio.Data),
	})
}

func (hub *Hub) PublishError(residentID canonical.ID, runID *canonical.ID, errorClass, message string) error {
	if err := residentID.Validate(); err != nil {
		return fmt.Errorf("httpui: invalid resident ID: %w", err)
	}
	event := StreamEvent{
		Type:       EventError,
		ResidentID: residentID.String(),
		ErrorClass: errorClass,
		Message:    message,
	}
	if runID != nil {
		if err := runID.Validate(); err != nil {
			return fmt.Errorf("httpui: invalid generation run ID: %w", err)
		}
		event.GenerationRunID = runID.String()
	}
	return hub.Publish(event)
}

func (hub *Hub) PublishStatus(residentID canonical.ID, runID *canonical.ID, status, message string) error {
	if err := residentID.Validate(); err != nil {
		return fmt.Errorf("httpui: invalid resident ID: %w", err)
	}
	event := StreamEvent{
		Type:       EventStatus,
		ResidentID: residentID.String(),
		Status:     status,
		Message:    message,
	}
	if runID != nil {
		if err := runID.Validate(); err != nil {
			return fmt.Errorf("httpui: invalid generation run ID: %w", err)
		}
		event.GenerationRunID = runID.String()
	}
	return hub.Publish(event)
}

// Subscription owns a single hub channel. Close is safe to call repeatedly.
type Subscription struct {
	Events <-chan StreamEvent
	once   sync.Once
	close  func()
	done   chan struct{}
}

func (subscription *Subscription) Close() {
	if subscription == nil {
		return
	}
	subscription.once.Do(subscription.close)
}

func (hub *Hub) Subscribe(ctx context.Context) *Subscription {
	return hub.SubscribeWithCapabilities(ctx, SubscriberCapabilities{})
}

func (hub *Hub) SubscribeWithCapabilities(ctx context.Context, capabilities SubscriberCapabilities) *Subscription {
	if ctx == nil {
		ctx = context.Background()
	}
	hub.mu.Lock()
	hub.nextSubID++
	id := hub.nextSubID
	bufferSize := hub.bufferSize
	if capabilities.Audio && bufferSize > audioSubscriberBufferSize {
		// Encoded WAV events may each be roughly 10.7 MiB at the hard audio
		// limit. Keep a slow audio connection tightly bounded; all text remains
		// recoverable from Canonical history and transient audio may be dropped.
		bufferSize = audioSubscriberBufferSize
	}
	channel := make(chan StreamEvent, bufferSize)
	if hub.closed {
		close(channel)
	} else {
		hub.subscribers[id] = hubSubscriber{events: channel, audio: capabilities.Audio}
	}
	hub.mu.Unlock()

	subscription := &Subscription{Events: channel, done: make(chan struct{})}
	subscription.close = func() {
		hub.mu.Lock()
		if existing, ok := hub.subscribers[id]; ok {
			delete(hub.subscribers, id)
			close(existing.events)
		}
		hub.mu.Unlock()
		close(subscription.done)
	}
	go func() {
		select {
		case <-ctx.Done():
			subscription.Close()
		case <-subscription.done:
		}
	}()
	return subscription
}

func (hub *Hub) HasAudioSubscribers() bool {
	if hub == nil {
		return false
	}
	hub.mu.Lock()
	defer hub.mu.Unlock()
	if hub.closed {
		return false
	}
	for _, subscriber := range hub.subscribers {
		if subscriber.audio {
			return true
		}
	}
	return false
}

func (hub *Hub) Close() {
	if hub == nil {
		return
	}
	hub.mu.Lock()
	defer hub.mu.Unlock()
	if hub.closed {
		return
	}
	hub.closed = true
	for id, subscriber := range hub.subscribers {
		delete(hub.subscribers, id)
		close(subscriber.events)
	}
}

var _ tts.Demand = (*Hub)(nil)
var _ tts.Sink = (*Hub)(nil)
