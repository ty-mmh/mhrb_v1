package tts

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"math"
	"net/http"
	"net/url"
	"strconv"
)

const defaultMaxAivisQueryBytes = 1 << 20

type AivisSpeechOptions struct {
	BaseURL       string
	StyleID       int64
	MaxAudioBytes int
	MaxQueryBytes int
	Client        *http.Client
}

type AivisSpeech struct {
	audioQueryEndpoint string
	synthesisEndpoint  string
	styleID            string
	maximum            int
	maxQueryBytes      int
	client             *http.Client
}

func NewAivisSpeech(options AivisSpeechOptions) (*AivisSpeech, error) {
	baseURL, err := validateBaseURL(options.BaseURL, true)
	if err != nil || options.StyleID < math.MinInt32 || options.StyleID > math.MaxInt32 {
		return nil, &Error{Class: ErrorInvalidRequest}
	}
	if err := validateMaximum(options.MaxAudioBytes); err != nil {
		return nil, err
	}
	maxQueryBytes := options.MaxQueryBytes
	if maxQueryBytes == 0 {
		maxQueryBytes = defaultMaxAivisQueryBytes
	}
	if maxQueryBytes < 1 || maxQueryBytes > defaultMaxAivisQueryBytes {
		return nil, &Error{Class: ErrorInvalidRequest}
	}
	return &AivisSpeech{
		audioQueryEndpoint: baseURL + "/audio_query",
		synthesisEndpoint:  baseURL + "/synthesis",
		styleID:            strconv.FormatInt(options.StyleID, 10), maximum: options.MaxAudioBytes,
		maxQueryBytes: maxQueryBytes, client: noRedirectClient(options.Client),
	}, nil
}

func (adapter *AivisSpeech) Synthesize(ctx context.Context, request Request) (Audio, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := request.Validate(); err != nil {
		return Audio{}, err
	}
	queryURL, err := url.Parse(adapter.audioQueryEndpoint)
	if err != nil {
		return Audio{}, &Error{Class: ErrorInvalidRequest}
	}
	values := queryURL.Query()
	values.Set("text", request.Text)
	values.Set("speaker", adapter.styleID)
	queryURL.RawQuery = values.Encode()
	queryRequest, err := http.NewRequestWithContext(ctx, http.MethodPost, queryURL.String(), http.NoBody)
	if err != nil {
		return Audio{}, &Error{Class: ErrorInvalidRequest}
	}
	queryRequest.Header.Set("Accept", "application/json")
	queryRequest.Header.Set("User-Agent", "mahoroba/alpha")
	queryResponse, err := adapter.client.Do(queryRequest)
	if err != nil {
		return Audio{}, classifyTransport(ctx.Err(), err)
	}
	queryBody, err := adapter.readAudioQuery(queryResponse)
	if err != nil {
		return Audio{}, err
	}

	synthesisURL, err := url.Parse(adapter.synthesisEndpoint)
	if err != nil {
		return Audio{}, &Error{Class: ErrorInvalidRequest}
	}
	values = synthesisURL.Query()
	values.Set("speaker", adapter.styleID)
	synthesisURL.RawQuery = values.Encode()
	synthesisRequest, err := http.NewRequestWithContext(ctx, http.MethodPost, synthesisURL.String(), bytes.NewReader(queryBody))
	if err != nil {
		return Audio{}, &Error{Class: ErrorInvalidRequest}
	}
	synthesisRequest.Header.Set("Content-Type", "application/json")
	synthesisRequest.Header.Set("Accept", MIMETypeWAV)
	synthesisRequest.Header.Set("User-Agent", "mahoroba/alpha")
	response, err := adapter.client.Do(synthesisRequest)
	if err != nil {
		return Audio{}, classifyTransport(ctx.Err(), err)
	}
	defer response.Body.Close()
	if response.StatusCode < http.StatusOK || response.StatusCode >= http.StatusMultipleChoices {
		return Audio{}, classifyHTTPError(response)
	}
	return readWAV(response, adapter.maximum)
}

func (adapter *AivisSpeech) readAudioQuery(response *http.Response) ([]byte, error) {
	defer response.Body.Close()
	if response.StatusCode < http.StatusOK || response.StatusCode >= http.StatusMultipleChoices {
		return nil, classifyHTTPError(response)
	}
	mediaType, err := responseMediaType(response)
	if err != nil || mediaType != "application/json" {
		return nil, &Error{Class: ErrorInvalidMedia}
	}
	body, err := io.ReadAll(io.LimitReader(response.Body, int64(adapter.maxQueryBytes)+1))
	if err != nil {
		return nil, &Error{Class: ErrorTransport}
	}
	if len(body) > adapter.maxQueryBytes {
		return nil, &Error{Class: ErrorOutputTooLarge}
	}
	if len(body) == 0 || !json.Valid(body) {
		return nil, &Error{Class: ErrorInvalidResponse}
	}
	return body, nil
}

var _ Adapter = (*AivisSpeech)(nil)
