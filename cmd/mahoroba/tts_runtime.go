package main

import (
	"context"
	"errors"
	"log/slog"
	"net/http"

	"mahoroba.local/mahoroba/internal/config"
	"mahoroba.local/mahoroba/internal/domain"
	"mahoroba.local/mahoroba/internal/httpui"
	"mahoroba.local/mahoroba/internal/tts"
)

// serveTTSRuntime owns only ephemeral post-commit work. It has no access to
// the Canonical Writer, repository, or blob store.
type serveTTSRuntime struct {
	dispatcher *tts.Dispatcher
}

func newServeTTSRuntime(settings config.AutonomyTTS, hub *httpui.Hub, logger *slog.Logger, client *http.Client) (*serveTTSRuntime, error) {
	if !settings.Enabled {
		return nil, nil
	}
	if hub == nil {
		return nil, errors.New("mahoroba: TTS requires an event hub")
	}
	var adapter tts.Adapter
	var err error
	switch settings.Provider {
	case "openai_speech":
		adapter, err = tts.NewOpenAISpeech(tts.OpenAISpeechOptions{
			BaseURL: settings.OpenAISpeech.BaseURL, APIKey: settings.OpenAISpeech.APIKey,
			Model: settings.OpenAISpeech.Model, Voice: settings.OpenAISpeech.Voice,
			MaxAudioBytes: settings.MaxAudioBytes, Client: client,
		})
	case "aivis_speech":
		if settings.AivisSpeech.StyleID == nil {
			return nil, errors.New("mahoroba: AivisSpeech requires an explicit style ID")
		}
		adapter, err = tts.NewAivisSpeech(tts.AivisSpeechOptions{
			BaseURL: settings.AivisSpeech.BaseURL, StyleID: *settings.AivisSpeech.StyleID,
			MaxAudioBytes: settings.MaxAudioBytes, Client: client,
		})
	default:
		return nil, errors.New("mahoroba: unsupported TTS provider")
	}
	if err != nil {
		return nil, err
	}
	dispatcher, err := tts.NewDispatcher(tts.DispatcherOptions{
		Adapter: adapter, Demand: hub, Sink: hub, RequestTimeout: settings.RequestTimeout,
		MaxConcurrency: settings.MaxConcurrency, QueueCapacity: settings.QueueCapacity,
		MaxAudioBytes: settings.MaxAudioBytes, Logger: logger,
	})
	if err != nil {
		return nil, err
	}
	return &serveTTSRuntime{dispatcher: dispatcher}, nil
}

func (runtime *serveTTSRuntime) Start(ctx context.Context) error {
	if runtime == nil {
		return nil
	}
	return runtime.dispatcher.Start(ctx)
}

func (runtime *serveTTSRuntime) StartGated(ctx context.Context, gate tts.StartGate) error {
	if runtime == nil {
		return nil
	}
	return runtime.dispatcher.StartGated(ctx, gate)
}

func (runtime *serveTTSRuntime) EnqueueCommitted(event domain.Event) bool {
	return runtime != nil && runtime.dispatcher.EnqueueCommitted(event)
}

func (runtime *serveTTSRuntime) Stop() {
	if runtime != nil {
		runtime.dispatcher.Stop()
	}
}

func (runtime *serveTTSRuntime) Wait(ctx context.Context) error {
	if runtime == nil {
		return nil
	}
	return runtime.dispatcher.Wait(ctx)
}
