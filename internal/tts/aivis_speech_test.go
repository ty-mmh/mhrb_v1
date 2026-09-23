package tts

import (
	"bytes"
	"context"
	"io"
	"math"
	"net/http"
	"net/http/httptest"
	"strconv"
	"testing"
)

func TestAivisSpeechPassesAudioQueryBytesUnchanged(t *testing.T) {
	request := validRequest(t)
	queryJSON := []byte("{\n  \"accent_phrases\": [], \"kana\": null\n}\n")
	styleID := int64(-2147483648)
	server := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, httpRequest *http.Request) {
		if httpRequest.Method != http.MethodPost || httpRequest.URL.Query().Get("speaker") != strconv.FormatInt(styleID, 10) {
			t.Errorf("request = %s %s", httpRequest.Method, httpRequest.URL.String())
		}
		switch httpRequest.URL.Path {
		case "/audio_query":
			if httpRequest.URL.Query().Get("text") != request.Text {
				t.Errorf("text = %q", httpRequest.URL.Query().Get("text"))
			}
			body, _ := io.ReadAll(httpRequest.Body)
			if len(body) != 0 {
				t.Errorf("audio_query body = %q", body)
			}
			response.Header().Set("Content-Type", "application/json; charset=utf-8")
			_, _ = response.Write(queryJSON)
		case "/synthesis":
			body, _ := io.ReadAll(httpRequest.Body)
			if !bytes.Equal(body, queryJSON) {
				t.Errorf("synthesis body changed:\n%q\nwant:\n%q", body, queryJSON)
			}
			if httpRequest.Header.Get("Content-Type") != "application/json" {
				t.Errorf("Content-Type = %q", httpRequest.Header.Get("Content-Type"))
			}
			response.Header().Set("Content-Type", MIMETypeWAV)
			_, _ = response.Write(validWAV())
		default:
			http.NotFound(response, httpRequest)
		}
	}))
	defer server.Close()
	adapter, err := NewAivisSpeech(AivisSpeechOptions{
		BaseURL: server.URL, StyleID: styleID, MaxAudioBytes: 1024, Client: server.Client(),
	})
	if err != nil {
		t.Fatal(err)
	}
	audio, err := adapter.Synthesize(context.Background(), request)
	if err != nil || string(audio.Data) != string(validWAV()) {
		t.Fatalf("audio=%q err=%v", audio.Data, err)
	}
}

func TestAivisSpeechAcceptsSignedInt32StyleIDsOnly(t *testing.T) {
	for _, styleID := range []int64{math.MinInt32, 0, math.MaxInt32} {
		if _, err := NewAivisSpeech(AivisSpeechOptions{BaseURL: "http://127.0.0.1:10101", StyleID: styleID, MaxAudioBytes: 1024}); err != nil {
			t.Fatalf("style ID %d rejected: %v", styleID, err)
		}
	}
	for _, styleID := range []int64{math.MinInt32 - 1, math.MaxInt32 + 1} {
		if _, err := NewAivisSpeech(AivisSpeechOptions{BaseURL: "http://127.0.0.1:10101", StyleID: styleID, MaxAudioBytes: 1024}); ErrorClassOf(err) != ErrorInvalidRequest {
			t.Fatalf("style ID %d error = %v", styleID, err)
		}
	}
}

func TestAivisSpeechRejectsRemoteAndMalformedQueryResponses(t *testing.T) {
	if _, err := NewAivisSpeech(AivisSpeechOptions{BaseURL: "https://example.com", MaxAudioBytes: 1024}); ErrorClassOf(err) != ErrorInvalidRequest {
		t.Fatalf("remote Aivis URL error = %v", err)
	}
	for _, test := range []struct {
		name, mediaType string
		body            []byte
		want            ErrorClass
	}{
		{name: "media", mediaType: "text/plain", body: []byte("{}"), want: ErrorInvalidMedia},
		{name: "json", mediaType: "application/json", body: []byte("{"), want: ErrorInvalidResponse},
		{name: "size", mediaType: "application/json", body: bytes.Repeat([]byte(" "), defaultMaxAivisQueryBytes+1), want: ErrorOutputTooLarge},
	} {
		t.Run(test.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, _ *http.Request) {
				response.Header().Set("Content-Type", test.mediaType)
				_, _ = response.Write(test.body)
			}))
			defer server.Close()
			adapter, err := NewAivisSpeech(AivisSpeechOptions{BaseURL: server.URL, StyleID: 0, MaxAudioBytes: 1024, Client: server.Client()})
			if err != nil {
				t.Fatal(err)
			}
			_, err = adapter.Synthesize(context.Background(), validRequest(t))
			if ErrorClassOf(err) != test.want {
				t.Fatalf("error = %v, want %s", err, test.want)
			}
		})
	}
}
