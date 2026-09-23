package tts

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func TestOpenAISpeechUsesStrictWAVRequest(t *testing.T) {
	request := validRequest(t)
	server := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, httpRequest *http.Request) {
		if httpRequest.Method != http.MethodPost || httpRequest.URL.Path != "/v1/audio/speech" {
			t.Errorf("request = %s %s", httpRequest.Method, httpRequest.URL.Path)
		}
		if got := httpRequest.Header.Get("Authorization"); got != "Bearer tts-secret" {
			t.Errorf("Authorization = %q", got)
		}
		if got := httpRequest.Header.Get("Accept"); got != MIMETypeWAV {
			t.Errorf("Accept = %q", got)
		}
		var body struct {
			Model          string `json:"model"`
			Input          string `json:"input"`
			Voice          string `json:"voice"`
			ResponseFormat string `json:"response_format"`
			Speed          int    `json:"speed"`
		}
		decoder := json.NewDecoder(httpRequest.Body)
		decoder.DisallowUnknownFields()
		if err := decoder.Decode(&body); err != nil {
			t.Errorf("decode request: %v", err)
		}
		if body.Model != "speech-model" || body.Input != request.Text || body.Voice != "voice" || body.ResponseFormat != "wav" || body.Speed != 1 {
			t.Errorf("body = %#v", body)
		}
		response.Header().Set("Content-Type", MIMETypeWAV)
		_, _ = response.Write(validWAV())
	}))
	defer server.Close()

	adapter, err := NewOpenAISpeech(OpenAISpeechOptions{
		BaseURL: server.URL + "/v1", APIKey: "tts-secret", Model: "speech-model", Voice: "voice",
		MaxAudioBytes: 1024, Client: server.Client(),
	})
	if err != nil {
		t.Fatal(err)
	}
	audio, err := adapter.Synthesize(context.Background(), request)
	if err != nil || string(audio.Data) != string(validWAV()) {
		t.Fatalf("audio=%q err=%v", audio.Data, err)
	}
}

func TestOpenAISpeechRejectsRedirectsAndDoesNotExposeSecrets(t *testing.T) {
	var redirected atomic.Int64
	server := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		if request.URL.Path == "/redirected" {
			redirected.Add(1)
			return
		}
		http.Redirect(response, request, "/redirected", http.StatusFound)
	}))
	defer server.Close()
	adapter, err := NewOpenAISpeech(OpenAISpeechOptions{
		BaseURL: server.URL, APIKey: "never-log-this", Model: "model", Voice: "voice",
		MaxAudioBytes: 1024, Client: server.Client(),
	})
	if err != nil {
		t.Fatal(err)
	}
	request := validRequest(t)
	request.Text = "private resident text"
	_, err = adapter.Synthesize(context.Background(), request)
	if ErrorClassOf(err) != ErrorHTTP || redirected.Load() != 0 {
		t.Fatalf("error=%v redirected=%d", err, redirected.Load())
	}
	if strings.Contains(err.Error(), request.Text) || strings.Contains(err.Error(), "never-log-this") {
		t.Fatalf("error exposed private material: %v", err)
	}
}

func TestOpenAISpeechRejectsInvalidMediaWAVAndSize(t *testing.T) {
	for _, test := range []struct {
		name      string
		mediaType string
		body      []byte
		maximum   int
		want      ErrorClass
	}{
		{name: "media", mediaType: "audio/mpeg", body: validWAV(), maximum: 1024, want: ErrorInvalidMedia},
		{name: "wave", mediaType: MIMETypeWAV, body: []byte("not-wave-data"), maximum: 1024, want: ErrorInvalidWAV},
		{name: "size", mediaType: MIMETypeWAV, body: append(validWAV(), make([]byte, 32)...), maximum: 12, want: ErrorOutputTooLarge},
	} {
		t.Run(test.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, _ *http.Request) {
				response.Header().Set("Content-Type", test.mediaType)
				_, _ = response.Write(test.body)
			}))
			defer server.Close()
			adapter, err := NewOpenAISpeech(OpenAISpeechOptions{
				BaseURL: server.URL, APIKey: "key", Model: "model", Voice: "voice",
				MaxAudioBytes: test.maximum, Client: server.Client(),
			})
			if err != nil {
				t.Fatal(err)
			}
			_, err = adapter.Synthesize(context.Background(), validRequest(t))
			if ErrorClassOf(err) != test.want {
				t.Fatalf("error=%v, want class %s", err, test.want)
			}
		})
	}
}

func TestOpenAISpeechDoesNotReadUnboundedHTTPError(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, _ *http.Request) {
		response.WriteHeader(http.StatusBadGateway)
		_, _ = io.WriteString(response, strings.Repeat("private", 4096))
	}))
	defer server.Close()
	adapter, err := NewOpenAISpeech(OpenAISpeechOptions{
		BaseURL: server.URL, APIKey: "key", Model: "model", Voice: "voice", MaxAudioBytes: 1024,
	})
	if err != nil {
		t.Fatal(err)
	}
	_, err = adapter.Synthesize(context.Background(), validRequest(t))
	if ErrorClassOf(err) != ErrorHTTP || strings.Contains(err.Error(), "private") {
		t.Fatalf("error = %v", err)
	}
}

func TestOpenAISpeechClassifiesRequestTimeout(t *testing.T) {
	release := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(_ http.ResponseWriter, _ *http.Request) {
		<-release
	}))
	defer func() {
		close(release)
		server.Close()
	}()
	adapter, err := NewOpenAISpeech(OpenAISpeechOptions{
		BaseURL: server.URL, APIKey: "key", Model: "model", Voice: "voice",
		MaxAudioBytes: 1024, Client: server.Client(),
	})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	_, err = adapter.Synthesize(ctx, validRequest(t))
	if ErrorClassOf(err) != ErrorTimeout {
		t.Fatalf("error = %v, want timeout", err)
	}
}
