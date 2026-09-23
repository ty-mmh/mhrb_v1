package httpui

import (
	"context"
	"encoding/base64"
	"testing"
	"time"

	"mahoroba.local/mahoroba/internal/canonical"
	"mahoroba.local/mahoroba/internal/domain"
	"mahoroba.local/mahoroba/internal/tts"
)

func hubTestID(t *testing.T, value string) canonical.ID {
	t.Helper()
	id, err := canonical.ParseID(value)
	if err != nil {
		t.Fatal(err)
	}
	return id
}

func hubTestWAV() []byte {
	return []byte{'R', 'I', 'F', 'F', 4, 0, 0, 0, 'W', 'A', 'V', 'E'}
}

func TestHubDropsOldestNotificationForSlowSubscriber(t *testing.T) {
	hub := NewHub(2)
	subscription := hub.Subscribe(context.Background())
	defer subscription.Close()

	for _, message := range []string{"one", "two", "three"} {
		if err := hub.Publish(StreamEvent{Type: EventStatus, Message: message}); err != nil {
			t.Fatal(err)
		}
	}
	first := <-subscription.Events
	second := <-subscription.Events
	if first.Message != "two" || first.Sequence != 2 {
		t.Fatalf("first retained event = %#v", first)
	}
	if second.Message != "three" || second.Sequence != 3 {
		t.Fatalf("second retained event = %#v", second)
	}
	if first.EmittedAt.Location() != time.UTC {
		t.Fatalf("emitted location = %v, want UTC", first.EmittedAt.Location())
	}
}

func TestHubRejectsInvalidEvent(t *testing.T) {
	hub := NewHub(1)
	if err := hub.Publish(StreamEvent{Type: "invented"}); err == nil {
		t.Fatal("Publish accepted an unknown event type")
	}
	if err := hub.Publish(StreamEvent{Type: EventProvisional, Text: string([]byte{0xff})}); err == nil {
		t.Fatal("Publish accepted invalid UTF-8")
	}
}

func TestHubCloseClosesSubscribers(t *testing.T) {
	hub := NewHub(1)
	subscription := hub.Subscribe(context.Background())
	hub.Close()
	if _, open := <-subscription.Events; open {
		t.Fatal("subscriber channel remained open")
	}
	if err := hub.Publish(StreamEvent{Type: EventStatus}); err == nil {
		t.Fatal("Publish succeeded after Close")
	}
	subscription.Close()
}

func TestHubAudioReachesOnlyCapableSubscribers(t *testing.T) {
	hub := NewHub(128)
	standard := hub.Subscribe(context.Background())
	audio := hub.SubscribeWithCapabilities(context.Background(), SubscriberCapabilities{Audio: true})
	defer standard.Close()
	defer audio.Close()
	if !hub.HasAudioSubscribers() {
		t.Fatal("audio demand was not visible")
	}
	if got := cap(audio.Events); got != audioSubscriberBufferSize {
		t.Fatalf("audio subscriber buffer = %d, want %d", got, audioSubscriberBufferSize)
	}
	if got := cap(standard.Events); got != 128 {
		t.Fatalf("standard subscriber buffer = %d, want 128", got)
	}
	residentID := hubTestID(t, "01ARZ3NDEKTSV4RRFFQ69G5FAV")
	eventID := hubTestID(t, "01ARZ3NDEKTSV4RRFFQ69G5FAW")
	runID := hubTestID(t, "01ARZ3NDEKTSV4RRFFQ69G5FAX")
	if err := hub.PublishAudio(tts.Delivery{
		ResidentID: residentID, SourceEventID: eventID, GenerationRunID: runID,
		Audio: tts.Audio{MIMEType: tts.MIMETypeWAV, Data: hubTestWAV()},
	}); err != nil {
		t.Fatal(err)
	}
	if err := hub.PublishStatus(residentID, nil, "ready", "ok"); err != nil {
		t.Fatal(err)
	}
	if got := <-standard.Events; got.Type != EventStatus {
		t.Fatalf("standard subscriber received %q", got.Type)
	}
	if got := <-audio.Events; got.Type != EventAudio || got.EventID != eventID.String() || got.GenerationRunID != runID.String() {
		t.Fatalf("audio event = %#v", got)
	}
	if got := <-audio.Events; got.Type != EventStatus {
		t.Fatalf("capable subscriber status = %#v", got)
	}
	audio.Close()
	if hub.HasAudioSubscribers() {
		t.Fatal("closed subscription still counted as audio demand")
	}
}

func TestHubRejectsSelfTalkAndInvalidAudio(t *testing.T) {
	hub := NewHub(2)
	residentID := hubTestID(t, "01ARZ3NDEKTSV4RRFFQ69G5FAV")
	eventID := hubTestID(t, "01ARZ3NDEKTSV4RRFFQ69G5FAW")
	runID := hubTestID(t, "01ARZ3NDEKTSV4RRFFQ69G5FAX")
	if err := hub.PublishProvisional(domain.GenerationPurposeSelfTalk, residentID, runID, "private"); err == nil {
		t.Fatal("non-dialogue provisional output was published")
	}
	if err := hub.PublishProvisional(domain.GenerationPurposeDialogue, residentID, runID, "public"); err != nil {
		t.Fatalf("dialogue provisional output rejected: %v", err)
	}
	if err := hub.PublishCommitted(domain.Event{
		ID: eventID, ResidentID: residentID, Type: "self_talk", GenerationRunID: &runID, Content: "private",
	}); err == nil {
		t.Fatal("self-talk was published")
	}
	if err := hub.PublishCommitted(domain.Event{
		ID: eventID, ResidentID: residentID, Type: "outbound_initiative", GenerationRunID: &runID, Content: "hello",
	}); err != nil {
		t.Fatalf("outbound initiative rejected: %v", err)
	}
	for _, event := range []StreamEvent{
		{Type: EventAudio, ResidentID: residentID.String(), EventID: eventID.String(), GenerationRunID: runID.String(), MIMEType: "audio/mpeg", AudioBase64: base64.StdEncoding.EncodeToString(hubTestWAV())},
		{Type: EventAudio, ResidentID: residentID.String(), EventID: eventID.String(), GenerationRunID: runID.String(), MIMEType: tts.MIMETypeWAV, AudioBase64: "%%%"},
		{Type: EventAudio, ResidentID: residentID.String(), EventID: eventID.String(), GenerationRunID: runID.String(), Text: "not audio", MIMEType: tts.MIMETypeWAV, AudioBase64: base64.StdEncoding.EncodeToString(hubTestWAV())},
		{Type: EventCommitted, ResidentID: residentID.String(), EventID: eventID.String(), EventType: "self_talk", Text: "private"},
		{Type: EventStatus, MIMEType: tts.MIMETypeWAV, AudioBase64: base64.StdEncoding.EncodeToString(hubTestWAV())},
	} {
		if err := hub.Publish(event); err == nil {
			t.Fatalf("invalid event accepted: %#v", event)
		}
	}
}

func TestHubStatusCarriesGenerationIdentity(t *testing.T) {
	hub := NewHub(2)
	subscription := hub.Subscribe(context.Background())
	defer subscription.Close()
	residentID := hubTestID(t, "01ARZ3NDEKTSV4RRFFQ69G5FAV")
	runID := hubTestID(t, "01ARZ3NDEKTSV4RRFFQ69G5FAX")
	if err := hub.PublishStatus(residentID, &runID, "generating", "Generating response"); err != nil {
		t.Fatal(err)
	}
	if got := <-subscription.Events; got.Type != EventStatus || got.ResidentID != residentID.String() ||
		got.GenerationRunID != runID.String() || got.Status != "generating" {
		t.Fatalf("generation status = %#v", got)
	}
	if err := hub.PublishStatus(residentID, nil, "ready", ""); err != nil {
		t.Fatal(err)
	}
	if got := <-subscription.Events; got.GenerationRunID != "" {
		t.Fatalf("general status has generation identity: %#v", got)
	}
	var invalidRun canonical.ID
	if err := hub.PublishStatus(residentID, &invalidRun, "generating", "Generating response"); err == nil {
		t.Fatal("status accepted an invalid generation identity")
	}
	select {
	case got := <-subscription.Events:
		t.Fatalf("invalid status was published: %#v", got)
	default:
	}
}
