package main

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"mahoroba.local/mahoroba/internal/canonical"
	"mahoroba.local/mahoroba/internal/config"
	"mahoroba.local/mahoroba/internal/domain"
	"mahoroba.local/mahoroba/internal/httpui"
)

func ttsRuntimeID(t *testing.T, value string) canonical.ID {
	t.Helper()
	id, err := canonical.ParseID(value)
	if err != nil {
		t.Fatal(err)
	}
	return id
}

func TestServeTTSRuntimeDisabledHasNoRuntime(t *testing.T) {
	runtime, err := newServeTTSRuntime(config.AutonomyTTS{}, httpui.NewHub(1), nil, nil)
	if err != nil || runtime != nil {
		t.Fatalf("runtime=%#v err=%v", runtime, err)
	}
}

func TestServeTTSRuntimeConvertsOpenAIConfigAndPublishesAfterDemand(t *testing.T) {
	wave := []byte{'R', 'I', 'F', 'F', 4, 0, 0, 0, 'W', 'A', 'V', 'E'}
	server := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		if request.Header.Get("Authorization") != "Bearer secret" {
			t.Errorf("Authorization = %q", request.Header.Get("Authorization"))
		}
		response.Header().Set("Content-Type", "audio/wav")
		_, _ = response.Write(wave)
	}))
	defer server.Close()
	hub := httpui.NewHub(4)
	subscription := hub.SubscribeWithCapabilities(context.Background(), httpui.SubscriberCapabilities{Audio: true})
	defer subscription.Close()
	runtime, err := newServeTTSRuntime(config.AutonomyTTS{
		Enabled: true, Provider: "openai_speech", RequestTimeout: time.Second,
		MaxConcurrency: 1, QueueCapacity: 1, MaxAudioBytes: 1024,
		OpenAISpeech: config.OpenAISpeech{BaseURL: server.URL, APIKey: "secret", Model: "model", Voice: "voice"},
	}, hub, nil, server.Client())
	if err != nil {
		t.Fatal(err)
	}
	if err := runtime.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	defer func() {
		runtime.Stop()
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		if err := runtime.Wait(ctx); err != nil {
			t.Errorf("wait runtime: %v", err)
		}
	}()
	seq, _ := canonical.NewSeq(1)
	runID := ttsRuntimeID(t, "01ARZ3NDEKTSV4RRFFQ69G5FAX")
	event := domain.Event{
		ID:         ttsRuntimeID(t, "01ARZ3NDEKTSV4RRFFQ69G5FAW"),
		ResidentID: ttsRuntimeID(t, "01ARZ3NDEKTSV4RRFFQ69G5FAV"),
		Seq:        seq, Type: "resident_message", GenerationRunID: &runID, Content: "speak this",
	}
	if !runtime.EnqueueCommitted(event) {
		t.Fatal("committed event was not queued")
	}
	select {
	case published := <-subscription.Events:
		if published.Type != httpui.EventAudio || published.EventID != event.ID.String() {
			t.Fatalf("published event = %#v", published)
		}
	case <-time.After(time.Second):
		t.Fatal("audio was not published")
	}
}
