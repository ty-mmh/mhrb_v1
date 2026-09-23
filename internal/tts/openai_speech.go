package tts

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
)

type OpenAISpeechOptions struct {
	BaseURL       string
	APIKey        string
	Model         string
	Voice         string
	MaxAudioBytes int
	Client        *http.Client
}

type OpenAISpeech struct {
	endpoint string
	apiKey   string
	model    string
	voice    string
	maximum  int
	client   *http.Client
}

func NewOpenAISpeech(options OpenAISpeechOptions) (*OpenAISpeech, error) {
	baseURL, err := validateBaseURL(options.BaseURL, false)
	if err != nil || options.APIKey == "" || options.Model == "" || options.Voice == "" {
		return nil, &Error{Class: ErrorInvalidRequest}
	}
	if err := validateMaximum(options.MaxAudioBytes); err != nil {
		return nil, err
	}
	return &OpenAISpeech{
		endpoint: baseURL + "/audio/speech", apiKey: options.APIKey,
		model: options.Model, voice: options.Voice, maximum: options.MaxAudioBytes,
		client: noRedirectClient(options.Client),
	}, nil
}

func (adapter *OpenAISpeech) Synthesize(ctx context.Context, request Request) (Audio, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := request.Validate(); err != nil {
		return Audio{}, err
	}
	body, err := json.Marshal(struct {
		Model          string `json:"model"`
		Input          string `json:"input"`
		Voice          string `json:"voice"`
		ResponseFormat string `json:"response_format"`
		Speed          int    `json:"speed"`
	}{
		Model: adapter.model, Input: request.Text, Voice: adapter.voice,
		ResponseFormat: "wav", Speed: 1,
	})
	if err != nil {
		return Audio{}, &Error{Class: ErrorInvalidRequest}
	}
	httpRequest, err := http.NewRequestWithContext(ctx, http.MethodPost, adapter.endpoint, bytes.NewReader(body))
	if err != nil {
		return Audio{}, &Error{Class: ErrorInvalidRequest}
	}
	httpRequest.Header.Set("Authorization", "Bearer "+adapter.apiKey)
	httpRequest.Header.Set("Content-Type", "application/json")
	httpRequest.Header.Set("Accept", MIMETypeWAV)
	httpRequest.Header.Set("User-Agent", "mahoroba/alpha")
	response, err := adapter.client.Do(httpRequest)
	if err != nil {
		return Audio{}, classifyTransport(ctx.Err(), err)
	}
	defer response.Body.Close()
	if response.StatusCode < http.StatusOK || response.StatusCode >= http.StatusMultipleChoices {
		return Audio{}, classifyHTTPError(response)
	}
	return readWAV(response, adapter.maximum)
}

var _ Adapter = (*OpenAISpeech)(nil)
