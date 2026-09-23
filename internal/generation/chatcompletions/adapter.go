package chatcompletions

import (
	"bufio"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"
	"unicode/utf8"

	"mahoroba.local/mahoroba/internal/generation"
)

type Adapter struct {
	endpoint     string
	identity     string
	apiKey       string
	client       *http.Client
	timeout      time.Duration
	sem          chan struct{}
	capabilities generation.Capabilities
}

const maxSSEEventBytes = 1 << 20

const maxJSONEnvelopeBytes = 1 << 20

var errSSELineTooLong = errors.New("provider SSE line exceeds limit")

func New(baseURL, apiKey string, timeout time.Duration, maxConcurrency int, client *http.Client) (*Adapter, error) {
	return NewWithCapabilities(baseURL, apiKey, timeout, maxConcurrency, client, generation.Capabilities{})
}

func NewWithCapabilities(
	baseURL, apiKey string,
	timeout time.Duration,
	maxConcurrency int,
	client *http.Client,
	capabilities generation.Capabilities,
) (*Adapter, error) {
	if baseURL == "" || timeout <= 0 || maxConcurrency < 1 {
		return nil, errors.New("chat-completions adapter requires base URL, positive timeout, and positive concurrency")
	}
	if client == nil {
		client = &http.Client{}
	}
	endpoint := strings.TrimRight(baseURL, "/") + "/chat/completions"
	return &Adapter{
		endpoint:     endpoint,
		identity:     providerIdentity(endpoint),
		apiKey:       apiKey,
		client:       client,
		timeout:      timeout,
		sem:          make(chan struct{}, maxConcurrency),
		capabilities: capabilities,
	}, nil
}

func (a *Adapter) ProviderIdentity() string              { return a.identity }
func (a *Adapter) Capabilities() generation.Capabilities { return a.capabilities }

func providerIdentity(endpoint string) string {
	// The endpoint is the complete provider routing input used by this adapter.
	// Hash it with a protocol/version domain separator so Canonical state never
	// stores credentials or accidentally conflates a future adapter contract.
	digest := sha256.Sum256([]byte("mahoroba:chat-completions:v1\x00" + endpoint))
	return fmt.Sprintf("chat-completions@sha256:%x", digest)
}

type requestBody struct {
	Model          string              `json:"model"`
	Messages       []wireMessage       `json:"messages"`
	Stream         bool                `json:"stream"`
	ResponseFormat *wireResponseFormat `json:"response_format,omitempty"`
}

type wireMessage struct {
	Role    generation.Role `json:"role"`
	Content string          `json:"content"`
}

type wireResponseFormat struct {
	Type       string         `json:"type"`
	JSONSchema wireJSONSchema `json:"json_schema"`
}

type wireJSONSchema struct {
	Name   string          `json:"name"`
	Strict bool            `json:"strict"`
	Schema json.RawMessage `json:"schema"`
}

type streamChunk struct {
	Model   string         `json:"model"`
	Choices []streamChoice `json:"choices"`
	Usage   *struct {
		PromptTokens     int64 `json:"prompt_tokens"`
		CompletionTokens int64 `json:"completion_tokens"`
	} `json:"usage"`
}

type streamChoice struct {
	Index *int `json:"index"`
	Delta struct {
		Content json.RawMessage `json:"content"`
	} `json:"delta"`
	FinishReason *string `json:"finish_reason"`
}

type completionResponse struct {
	Model   string `json:"model"`
	Choices []struct {
		Message struct {
			Content json.RawMessage `json:"content"`
		} `json:"message"`
		FinishReason *string `json:"finish_reason"`
	} `json:"choices"`
	Usage *struct {
		PromptTokens     int64 `json:"prompt_tokens"`
		CompletionTokens int64 `json:"completion_tokens"`
	} `json:"usage"`
}

func (a *Adapter) Stream(parent context.Context, req generation.Request, sink generation.DeltaSink) (generation.Result, error) {
	if req.Model == "" || len(req.Messages) == 0 || req.MaxOutputBytes < 1 {
		return generation.Result{}, invalid("model, messages, and a positive output limit are required", nil)
	}
	for _, message := range req.Messages {
		if message.Role != generation.RoleSystem && message.Role != generation.RoleUser && message.Role != generation.RoleAssistant {
			return generation.Result{}, invalid("unsupported message role", nil)
		}
		if !utf8.ValidString(message.Text) {
			return generation.Result{}, invalid("message contains invalid UTF-8", nil)
		}
	}
	if req.StructuredOutput != nil {
		if err := req.StructuredOutput.Validate(a.capabilities); err != nil {
			return generation.Result{}, invalid("invalid structured-output contract", err)
		}
	}

	select {
	case a.sem <- struct{}{}:
		defer func() { <-a.sem }()
	case <-parent.Done():
		return generation.Result{}, classifyContext(parent.Err())
	}

	ctx, cancel := context.WithTimeout(parent, a.timeout)
	defer cancel()

	messageCount := len(req.Messages)
	if req.StructuredOutput != nil && req.StructuredOutput.Mode == generation.StructuredOutputPrompt {
		messageCount++
	}
	wire := requestBody{Model: req.Model, Stream: req.Streaming, Messages: make([]wireMessage, 0, messageCount)}
	for _, message := range req.Messages {
		wire.Messages = append(wire.Messages, wireMessage{Role: message.Role, Content: message.Text})
	}
	if req.StructuredOutput != nil {
		schema := req.StructuredOutput
		switch schema.Mode {
		case generation.StructuredOutputPrompt:
			wire.Messages = append(wire.Messages, wireMessage{Role: generation.RoleSystem, Content: schema.PromptInstruction()})
		case generation.StructuredOutputJSONSchema:
			wire.ResponseFormat = &wireResponseFormat{
				Type: "json_schema",
				JSONSchema: wireJSONSchema{
					Name: schema.Name, Strict: true, Schema: json.RawMessage(schema.Schema.Bytes()),
				},
			}
		}
	}
	payload, err := json.Marshal(wire)
	if err != nil {
		return generation.Result{}, invalid("encode request", err)
	}
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, a.endpoint, bytes.NewReader(payload))
	if err != nil {
		return generation.Result{}, invalid("construct request", err)
	}
	httpReq.Header.Set("Content-Type", "application/json")
	if req.Streaming {
		httpReq.Header.Set("Accept", "text/event-stream")
	} else {
		httpReq.Header.Set("Accept", "application/json")
	}
	httpReq.Header.Set("User-Agent", "mahoroba/alpha")
	if a.apiKey != "" {
		httpReq.Header.Set("Authorization", "Bearer "+a.apiKey)
	}
	response, err := a.client.Do(httpReq)
	if err != nil {
		if ctx.Err() != nil {
			return generation.Result{}, classifyContext(ctx.Err())
		}
		return generation.Result{}, &generation.ProviderError{Class: generation.ErrorTransport, Retryable: true, Detail: "provider request failed", Cause: err}
	}
	defer response.Body.Close()
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		return generation.Result{}, &generation.ProviderError{
			Class: generation.ErrorHTTP, Retryable: generation.RetryableHTTPStatus(response.StatusCode), StatusCode: response.StatusCode,
		}
	}
	if req.Streaming {
		return consumeStream(response.Body, req.MaxOutputBytes, sink)
	}
	return consumeResponse(response.Body, req.MaxOutputBytes, sink)
}

func consumeResponse(body io.Reader, maxOutputBytes int, sink generation.DeltaSink) (generation.Result, error) {
	wireLimit := maxJSONWireBytes(maxOutputBytes)
	payload, err := io.ReadAll(io.LimitReader(body, wireLimit+1))
	if err != nil {
		return generation.Result{}, &generation.ProviderError{
			Class: generation.ErrorTransport, Retryable: true, Detail: "read provider response", Cause: err,
		}
	}
	if int64(len(payload)) > wireLimit {
		return generation.Result{}, invalid("provider JSON response exceeds limit", nil)
	}
	if !utf8.Valid(payload) {
		return generation.Result{}, invalid("provider JSON response contains invalid UTF-8", nil)
	}
	var response completionResponse
	if err := json.Unmarshal(payload, &response); err != nil {
		return generation.Result{}, invalid("malformed provider JSON", err)
	}
	if len(response.Choices) != 1 {
		return generation.Result{}, invalid("provider response must contain exactly one choice", nil)
	}
	choice := response.Choices[0]
	if choice.FinishReason == nil {
		return generation.Result{}, invalid("provider response is missing finish reason", nil)
	}
	if *choice.FinishReason != "stop" && *choice.FinishReason != "length" {
		return generation.Result{}, invalid("unsupported finish reason: "+*choice.FinishReason, nil)
	}
	if len(choice.Message.Content) == 0 || bytes.Equal(choice.Message.Content, []byte("null")) {
		return generation.Result{}, invalid("provider returned empty text", nil)
	}
	var output string
	if err := json.Unmarshal(choice.Message.Content, &output); err != nil {
		return generation.Result{}, invalid("message.content is not text", err)
	}
	if output == "" {
		return generation.Result{}, invalid("provider returned empty text", nil)
	}
	if !utf8.ValidString(output) || len(output) > maxOutputBytes {
		return generation.Result{}, invalid("provider output is invalid or exceeds limit", nil)
	}
	result := generation.Result{
		Text: output, FinishReason: *choice.FinishReason, ModelVersion: response.Model,
	}
	if response.Usage != nil {
		prompt, completion := response.Usage.PromptTokens, response.Usage.CompletionTokens
		result.PromptTokens, result.CompletionTokens = &prompt, &completion
	}
	if sink != nil {
		sink(generation.Delta{Text: output})
	}
	return result, nil
}

func maxJSONWireBytes(maxOutputBytes int) int64 {
	// A JSON string may encode one output byte as a six-byte Unicode escape.
	// Reserve a bounded envelope for provider metadata while allowing any valid
	// representation of an output that satisfies the captured output limit.
	const maxInt64 = int64(^uint64(0) >> 1)
	outputLimit := int64(maxOutputBytes)
	if outputLimit > (maxInt64-maxJSONEnvelopeBytes-1)/6 {
		return maxInt64 - 1
	}
	return outputLimit*6 + maxJSONEnvelopeBytes
}

func consumeStream(body io.Reader, maxOutputBytes int, sink generation.DeltaSink) (generation.Result, error) {
	reader := bufio.NewReaderSize(body, 32<<10)
	var result generation.Result
	var output strings.Builder
	var dataLines []string
	dataBytes := 0
	done := false
	finishSeen := false
	for {
		line, err := readBoundedSSELine(reader, maxSSEEventBytes)
		if errors.Is(err, errSSELineTooLong) {
			return generation.Result{}, invalid(errSSELineTooLong.Error(), nil)
		}
		line = strings.TrimSuffix(strings.TrimSuffix(line, "\n"), "\r")
		if line == "" {
			if len(dataLines) > 0 {
				payload := strings.Join(dataLines, "\n")
				dataLines = dataLines[:0]
				dataBytes = 0
				if payload == "[DONE]" {
					done = true
					break
				}
				var chunk streamChunk
				if decodeErr := json.Unmarshal([]byte(payload), &chunk); decodeErr != nil {
					return generation.Result{}, invalid("malformed provider SSE JSON", decodeErr)
				}
				if chunk.Model != "" {
					result.ModelVersion = chunk.Model
				}
				if chunk.Usage != nil {
					prompt, completion := chunk.Usage.PromptTokens, chunk.Usage.CompletionTokens
					result.PromptTokens, result.CompletionTokens = &prompt, &completion
				}
				if len(chunk.Choices) == 0 {
					if !finishSeen || chunk.Usage == nil {
						return generation.Result{}, invalid("provider stream chunk must contain exactly one choice", nil)
					}
				} else {
					if finishSeen {
						if len(chunk.Choices) == 1 && chunk.Choices[0].FinishReason != nil {
							return generation.Result{}, invalid("provider stream contains duplicate finish reason", nil)
						}
						return generation.Result{}, invalid("provider stream emitted choice after finish", nil)
					}
					if len(chunk.Choices) != 1 {
						return generation.Result{}, invalid("provider stream chunk must contain exactly one choice", nil)
					}
					choice := chunk.Choices[0]
					if choice.Index == nil {
						return generation.Result{}, invalid("provider stream choice is missing index", nil)
					}
					if *choice.Index != 0 {
						return generation.Result{}, invalid("provider stream choice index must be zero", nil)
					}
					if len(choice.Delta.Content) > 0 && !bytes.Equal(choice.Delta.Content, []byte("null")) {
						var delta string
						if decodeErr := json.Unmarshal(choice.Delta.Content, &delta); decodeErr != nil {
							return generation.Result{}, invalid("delta.content is not text", decodeErr)
						}
						if !utf8.ValidString(delta) || output.Len()+len(delta) > maxOutputBytes {
							return generation.Result{}, invalid("provider output is invalid or exceeds limit", nil)
						}
						output.WriteString(delta)
						if sink != nil && delta != "" {
							sink(generation.Delta{Text: delta})
						}
					}
					if choice.FinishReason != nil {
						result.FinishReason = *choice.FinishReason
						finishSeen = true
					}
				}
			}
		} else if strings.HasPrefix(line, "data:") {
			data := strings.TrimSpace(strings.TrimPrefix(line, "data:"))
			additional := len(data)
			if len(dataLines) > 0 {
				additional++
			}
			if dataBytes > maxSSEEventBytes-additional {
				return generation.Result{}, invalid("provider SSE event exceeds limit", nil)
			}
			dataBytes += additional
			dataLines = append(dataLines, data)
		}
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			return generation.Result{}, &generation.ProviderError{Class: generation.ErrorTransport, Retryable: true, Detail: "read provider stream", Cause: err}
		}
	}
	if !done || !finishSeen {
		return generation.Result{}, invalid("provider stream ended without DONE and finish reason", nil)
	}
	if result.FinishReason != "stop" && result.FinishReason != "length" {
		return generation.Result{}, invalid("unsupported finish reason: "+result.FinishReason, nil)
	}
	if output.Len() == 0 {
		return generation.Result{}, invalid("provider returned empty text", nil)
	}
	result.Text = output.String()
	return result, nil
}

func readBoundedSSELine(reader *bufio.Reader, maximum int) (string, error) {
	line := make([]byte, 0, min(reader.Size(), maximum))
	for {
		fragment, err := reader.ReadSlice('\n')
		if len(line)+len(fragment) > maximum {
			return "", errSSELineTooLong
		}
		line = append(line, fragment...)
		if !errors.Is(err, bufio.ErrBufferFull) {
			return string(line), err
		}
	}
}

func invalid(detail string, cause error) error {
	return &generation.ProviderError{Class: generation.ErrorInvalidResponse, Detail: detail, Cause: cause}
}

func classifyContext(err error) error {
	if errors.Is(err, context.DeadlineExceeded) {
		return &generation.ProviderError{Class: generation.ErrorTimeout, Retryable: true, Detail: "provider request timed out", Cause: err}
	}
	return &generation.ProviderError{Class: generation.ErrorCancelled, Detail: "provider request cancelled", Cause: err}
}

func ErrorInfo(err error) (class generation.ErrorClass, retryable bool) {
	var providerErr *generation.ProviderError
	if errors.As(err, &providerErr) {
		return providerErr.Class, providerErr.Retryable
	}
	return generation.ErrorTransport, false
}

var _ generation.Generator = (*Adapter)(nil)
var _ generation.CapabilityProvider = (*Adapter)(nil)

func (a *Adapter) String() string { return fmt.Sprintf("chat-completions(%s)", a.endpoint) }
