package chatcompletions

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"mahoroba.local/mahoroba/internal/canonical"
	"mahoroba.local/mahoroba/internal/generation"
)

func TestStreamText(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/chat/completions" || r.Header.Get("Authorization") != "Bearer secret" {
			t.Fatalf("unexpected request %s auth=%q", r.URL.Path, r.Header.Get("Authorization"))
		}
		w.Header().Set("Content-Type", "text/event-stream")
		flusher := w.(http.Flusher)
		for _, event := range []string{
			`data: {"model":"local-model","choices":[{"index":0,"delta":{"content":"こん"},"finish_reason":null}]}` + "\n\n",
			`data: {"choices":[{"index":0,"delta":{"content":"にちは"},"finish_reason":"stop"}],"usage":{"prompt_tokens":3,"completion_tokens":2}}` + "\n\n",
			"data: [DONE]\n\n",
		} {
			for _, fragment := range []string{event[:len(event)/2], event[len(event)/2:]} {
				_, _ = fmt.Fprint(w, fragment)
				flusher.Flush()
			}
		}
	}))
	defer server.Close()
	adapter, err := New(server.URL+"/v1", "secret", time.Second, 1, server.Client())
	if err != nil {
		t.Fatal(err)
	}
	var deltas strings.Builder
	result, err := adapter.Stream(context.Background(), generation.Request{
		Model: "local-model", Streaming: true, MaxOutputBytes: 1024,
		Messages: []generation.Message{{Role: generation.RoleUser, Text: "hello"}},
	}, func(delta generation.Delta) { deltas.WriteString(delta.Text) })
	if err != nil {
		t.Fatal(err)
	}
	if result.Text != "こんにちは" || deltas.String() != result.Text || result.FinishReason != "stop" {
		t.Fatalf("unexpected result %#v deltas=%q", result, deltas.String())
	}
	if result.PromptTokens == nil || *result.PromptTokens != 3 {
		t.Fatalf("usage missing: %#v", result)
	}
}

func TestProviderIdentityPinsEndpointWithoutCredentials(t *testing.T) {
	first, err := New("https://api.example.test/v1/", "first-secret", time.Second, 1, nil)
	if err != nil {
		t.Fatal(err)
	}
	sameRoute, err := New("https://api.example.test/v1", "different-secret", 2*time.Second, 7, nil)
	if err != nil {
		t.Fatal(err)
	}
	differentRoute, err := New("https://other.example.test/v1", "first-secret", time.Second, 1, nil)
	if err != nil {
		t.Fatal(err)
	}
	if first.ProviderIdentity() != sameRoute.ProviderIdentity() {
		t.Fatal("non-routing configuration changed the provider identity")
	}
	if first.ProviderIdentity() == differentRoute.ProviderIdentity() {
		t.Fatal("different provider endpoints share an identity")
	}
	if strings.Contains(first.ProviderIdentity(), "secret") || strings.Contains(first.ProviderIdentity(), "api.example") {
		t.Fatalf("provider identity leaks endpoint or credentials: %q", first.ProviderIdentity())
	}
}

func TestRequestUsesRecordedStreamingMode(t *testing.T) {
	for _, streaming := range []bool{true, false} {
		t.Run(fmt.Sprint(streaming), func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
				payload, err := io.ReadAll(request.Body)
				if err != nil {
					t.Fatal(err)
				}
				want := fmt.Sprintf(`{"model":"m","messages":[{"role":"user","content":"x"}],"stream":%t}`, streaming)
				if string(payload) != want {
					t.Fatalf("legacy dialogue payload=%s want=%s", payload, want)
				}
				var body requestBody
				if err := json.Unmarshal(payload, &body); err != nil {
					t.Fatal(err)
				}
				if body.Stream != streaming {
					t.Fatalf("wire stream=%v want=%v", body.Stream, streaming)
				}
				if body.ResponseFormat != nil || len(body.Messages) != 1 {
					t.Fatalf("legacy dialogue request gained structured-output fields: %+v", body)
				}
				if streaming {
					_, _ = fmt.Fprint(w, `data: {"choices":[{"index":0,"delta":{"content":"x"},"finish_reason":"stop"}]}`+"\n\ndata: [DONE]\n\n")
					return
				}
				_, _ = fmt.Fprint(w, `{"choices":[{"message":{"role":"assistant","content":"x"},"finish_reason":"stop"}]}`)
			}))
			defer server.Close()
			adapter, err := New(server.URL, "", time.Second, 1, server.Client())
			if err != nil {
				t.Fatal(err)
			}
			if _, err := adapter.Stream(context.Background(), generation.Request{
				Model: "m", Streaming: streaming, MaxOutputBytes: 10,
				Messages: []generation.Message{{Role: generation.RoleUser, Text: "x"}},
			}, nil); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestNonStreamingJSONText(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		if request.Header.Get("Accept") != "application/json" {
			t.Fatalf("Accept=%q", request.Header.Get("Accept"))
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = fmt.Fprint(w, `{
			"model":"local-model-2026-08",
			"choices":[{"message":{"role":"assistant","content":"こんにちは"},"finish_reason":"stop"}],
			"usage":{"prompt_tokens":7,"completion_tokens":4}
		}`)
	}))
	defer server.Close()
	adapter, err := New(server.URL, "", time.Second, 1, server.Client())
	if err != nil {
		t.Fatal(err)
	}
	var deltas strings.Builder
	result, err := adapter.Stream(context.Background(), generation.Request{
		Model: "m", Streaming: false, MaxOutputBytes: 1024,
		Messages: []generation.Message{{Role: generation.RoleUser, Text: "hello"}},
	}, func(delta generation.Delta) { deltas.WriteString(delta.Text) })
	if err != nil {
		t.Fatal(err)
	}
	if result.Text != "こんにちは" || deltas.String() != result.Text || result.ModelVersion != "local-model-2026-08" || result.FinishReason != "stop" {
		t.Fatalf("unexpected result %#v deltas=%q", result, deltas.String())
	}
	if result.PromptTokens == nil || *result.PromptTokens != 7 || result.CompletionTokens == nil || *result.CompletionTokens != 4 {
		t.Fatalf("usage missing: %#v", result)
	}
}

func TestRejectsInvalidNonStreamingResponse(t *testing.T) {
	tests := map[string]string{
		"malformed":       `{`,
		"missing-choice":  `{"model":"m"}`,
		"multiple-choice": `{"choices":[{"message":{"content":"a"},"finish_reason":"stop"},{"message":{"content":"b"},"finish_reason":"stop"}]}`,
		"non-text":        `{"choices":[{"message":{"content":{"text":"x"}},"finish_reason":"stop"}]}`,
		"tool-call":       `{"choices":[{"message":{"content":null},"finish_reason":"tool_calls"}]}`,
	}
	for name, response := range tests {
		t.Run(name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				_, _ = fmt.Fprint(w, response)
			}))
			defer server.Close()
			adapter, err := New(server.URL, "", time.Second, 1, server.Client())
			if err != nil {
				t.Fatal(err)
			}
			_, err = adapter.Stream(context.Background(), generation.Request{
				Model: "m", Streaming: false, MaxOutputBytes: 10,
				Messages: []generation.Message{{Role: generation.RoleUser, Text: "x"}},
			}, nil)
			var providerErr *generation.ProviderError
			if !errors.As(err, &providerErr) || providerErr.Class != generation.ErrorInvalidResponse || providerErr.Retryable {
				t.Fatalf("expected terminal invalid response, got %#v", err)
			}
		})
	}
}

func TestJSONSchemaModeUsesNativeResponseFormatWithoutPromptMutation(t *testing.T) {
	contract := schemaContract(t, generation.StructuredOutputJSONSchema)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		var body requestBody
		if err := json.NewDecoder(request.Body).Decode(&body); err != nil {
			t.Fatal(err)
		}
		if body.ResponseFormat == nil || body.ResponseFormat.Type != "json_schema" ||
			body.ResponseFormat.JSONSchema.Name != contract.Name || !body.ResponseFormat.JSONSchema.Strict ||
			string(body.ResponseFormat.JSONSchema.Schema) != contract.Schema.String() {
			t.Fatalf("native response format = %+v", body.ResponseFormat)
		}
		if len(body.Messages) != 1 || body.Messages[0].Content != "extract" {
			t.Fatalf("native schema mode changed messages: %+v", body.Messages)
		}
		_, _ = fmt.Fprint(w, `{"choices":[{"message":{"role":"assistant","content":"{}"},"finish_reason":"stop"}]}`)
	}))
	defer server.Close()
	adapter, err := NewWithCapabilities(server.URL, "", time.Second, 1, server.Client(), generation.Capabilities{SupportsJSONSchema: true})
	if err != nil {
		t.Fatal(err)
	}
	if !adapter.Capabilities().SupportsJSONSchema {
		t.Fatal("adapter did not expose configured capability")
	}
	result, err := adapter.Stream(context.Background(), generation.Request{
		Purpose: "memory_extraction", Model: "m", Streaming: false, MaxOutputBytes: 1024,
		Messages: []generation.Message{{Role: generation.RoleUser, Text: "extract"}}, StructuredOutput: &contract,
	}, nil)
	if err != nil || result.Text != "{}" {
		t.Fatalf("native schema result=%+v err=%v", result, err)
	}
}

func TestPromptModeAddsDeterministicSchemaInstructionWithoutResponseFormat(t *testing.T) {
	contract := schemaContract(t, generation.StructuredOutputPrompt)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		var body requestBody
		if err := json.NewDecoder(request.Body).Decode(&body); err != nil {
			t.Fatal(err)
		}
		if body.ResponseFormat != nil {
			t.Fatalf("prompt fallback sent response_format: %+v", body.ResponseFormat)
		}
		if len(body.Messages) != 2 || body.Messages[0].Content != "extract" ||
			body.Messages[1].Role != generation.RoleSystem || body.Messages[1].Content != contract.PromptInstruction() {
			t.Fatalf("prompt-mode messages = %+v", body.Messages)
		}
		_, _ = fmt.Fprint(w, `{"choices":[{"message":{"role":"assistant","content":"{}"},"finish_reason":"stop"}]}`)
	}))
	defer server.Close()
	adapter, err := New(server.URL, "", time.Second, 1, server.Client())
	if err != nil {
		t.Fatal(err)
	}
	_, err = adapter.Stream(context.Background(), generation.Request{
		Purpose: "memory_extraction", Model: "m", Streaming: false, MaxOutputBytes: 1024,
		Messages: []generation.Message{{Role: generation.RoleUser, Text: "extract"}}, StructuredOutput: &contract,
	}, nil)
	if err != nil {
		t.Fatal(err)
	}
}

func TestJSONSchemaModeFailsClosedWhenProviderCapabilityIsAbsent(t *testing.T) {
	contract := schemaContract(t, generation.StructuredOutputJSONSchema)
	adapter, err := New("https://api.example.test/v1", "", time.Second, 1, nil)
	if err != nil {
		t.Fatal(err)
	}
	_, err = adapter.Stream(context.Background(), generation.Request{
		Model: "m", Streaming: true, MaxOutputBytes: 1024,
		Messages: []generation.Message{{Role: generation.RoleUser, Text: "extract"}}, StructuredOutput: &contract,
	}, nil)
	var providerErr *generation.ProviderError
	if !errors.As(err, &providerErr) || providerErr.Class != generation.ErrorInvalidResponse ||
		!strings.Contains(err.Error(), "structured-output contract") {
		t.Fatalf("missing capability error = %v", err)
	}
}

func schemaContract(t *testing.T, mode generation.StructuredOutputMode) generation.SchemaContract {
	t.Helper()
	schema, err := canonical.CanonicalizeRFC8785([]byte(`{
		"type":"object",
		"properties":{"claims":{"type":"array"}},
		"required":["claims"],
		"additionalProperties":false
	}`))
	if err != nil {
		t.Fatal(err)
	}
	contract, err := generation.NewSchemaContract("memory_extraction", "memory-extraction-output-v1", mode, schema)
	if err != nil {
		t.Fatal(err)
	}
	return contract
}

func TestHTTPClassification(t *testing.T) {
	for _, tc := range []struct {
		code      int
		retryable bool
	}{{400, false}, {429, true}, {503, true}} {
		t.Run(fmt.Sprint(tc.code), func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				http.Error(w, "prompt=resident secret=do-not-log", tc.code)
			}))
			defer server.Close()
			adapter, _ := New(server.URL, "", time.Second, 1, server.Client())
			_, err := adapter.Stream(context.Background(), generation.Request{Model: "m", Streaming: true, MaxOutputBytes: 10, Messages: []generation.Message{{Role: generation.RoleUser, Text: "x"}}}, nil)
			var providerErr *generation.ProviderError
			if !errors.As(err, &providerErr) || providerErr.Retryable != tc.retryable || providerErr.StatusCode != tc.code {
				t.Fatalf("unexpected error: %#v", err)
			}
			if providerErr.Detail != "" || strings.Contains(err.Error(), "resident") || strings.Contains(err.Error(), "do-not-log") {
				t.Fatalf("provider response body leaked through error: detail=%q error=%q", providerErr.Detail, err)
			}
		})
	}
}

func TestM7ProviderBodyDoesNotEnterOutcomeClassification(t *testing.T) {
	const secret = "provider-body-secret-do-not-persist"
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, secret, http.StatusBadGateway)
	}))
	defer server.Close()
	adapter, err := New(server.URL, "", time.Second, 1, server.Client())
	if err != nil {
		t.Fatal(err)
	}
	_, err = adapter.Stream(context.Background(), generation.Request{
		Model: "m", Streaming: true, MaxOutputBytes: 10,
		Messages: []generation.Message{{Role: generation.RoleUser, Text: "x"}},
	}, nil)
	if err == nil || strings.Contains(err.Error(), secret) {
		t.Fatalf("provider body entered returned error: %v", err)
	}
	var providerErr *generation.ProviderError
	if !errors.As(err, &providerErr) || providerErr.Detail != "" {
		t.Fatalf("provider error retained body detail: %#v", providerErr)
	}
	if code := generation.OutcomeErrorCodeFromError(err); code.String() != "provider_http:502" {
		t.Fatalf("outcome classification = %q, want provider_http:502", code)
	}
}

func TestRejectsMalformedOrIncompleteStream(t *testing.T) {
	for name, stream := range map[string]string{
		"malformed":       "data: not-json\n\n",
		"incomplete":      `data: {"choices":[{"index":0,"delta":{"content":"x"},"finish_reason":"stop"}]}` + "\n\n",
		"tool-call":       `data: {"choices":[{"index":0,"delta":{},"finish_reason":"tool_calls"}]}` + "\n\ndata: [DONE]\n\n",
		"multiple-choice": `data: {"choices":[{"index":0,"delta":{"content":"a"},"finish_reason":null},{"index":1,"delta":{"content":"b"},"finish_reason":null}]}\n\n` + "data: [DONE]\n\n",
		"missing-index":   `data: {"choices":[{"delta":{"content":"x"},"finish_reason":"stop"}]}\n\n` + "data: [DONE]\n\n",
		"unknown-index":   `data: {"choices":[{"index":1,"delta":{"content":"x"},"finish_reason":"stop"}]}\n\n` + "data: [DONE]\n\n",
		"finish-then-delta": `data: {"choices":[{"index":0,"delta":{},"finish_reason":"stop"}]}\n\n` +
			`data: {"choices":[{"index":0,"delta":{"content":"late"},"finish_reason":null}]}\n\n` + "data: [DONE]\n\n",
		"duplicate-finish": `data: {"choices":[{"index":0,"delta":{},"finish_reason":"stop"}]}\n\n` +
			`data: {"choices":[{"index":0,"delta":{},"finish_reason":"stop"}]}\n\n` + "data: [DONE]\n\n",
	} {
		t.Run(name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { _, _ = fmt.Fprint(w, stream) }))
			defer server.Close()
			adapter, _ := New(server.URL, "", time.Second, 1, server.Client())
			_, err := adapter.Stream(context.Background(), generation.Request{Model: "m", Streaming: true, MaxOutputBytes: 10, Messages: []generation.Message{{Role: generation.RoleUser, Text: "x"}}}, nil)
			var providerErr *generation.ProviderError
			if !errors.As(err, &providerErr) || providerErr.Class != generation.ErrorInvalidResponse {
				t.Fatalf("expected invalid response, got %v", err)
			}
		})
	}
}

func TestAllowsUsageOnlyChunkAfterFinish(t *testing.T) {
	result, err := consumeStream(strings.NewReader(
		`data: {"choices":[{"index":0,"delta":{"content":"x"},"finish_reason":"stop"}]}`+"\n\n"+
			`data: {"choices":[],"usage":{"prompt_tokens":1,"completion_tokens":1}}`+"\n\n"+
			"data: [DONE]\n\n",
	), 10, nil)
	if err != nil {
		t.Fatal(err)
	}
	if result.Text != "x" || result.FinishReason != "stop" || result.PromptTokens == nil || *result.PromptTokens != 1 {
		t.Fatalf("unexpected result: %#v", result)
	}
}

func TestRejectsOversizedSSELineAndEvent(t *testing.T) {
	tests := []struct {
		name string
		body string
		want string
	}{
		{name: "line", body: strings.Repeat("x", maxSSEEventBytes+1) + "\n", want: "line exceeds limit"},
		{name: "event", body: "data:" + strings.Repeat("x", maxSSEEventBytes/2) + "\n" +
			"data:" + strings.Repeat("y", maxSSEEventBytes/2) + "\n\n", want: "event exceeds limit"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, err := consumeStream(strings.NewReader(test.body), 1024, nil)
			if err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("consumeStream error = %v, want %q", err, test.want)
			}
		})
	}
}

func TestTimeout(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		select {
		case <-time.After(time.Second):
			_, _ = fmt.Fprint(w, "data: [DONE]\n\n")
		}
	}))
	defer server.Close()
	adapter, _ := New(server.URL, "", 10*time.Millisecond, 1, server.Client())
	_, err := adapter.Stream(context.Background(), generation.Request{Model: "m", MaxOutputBytes: 10, Messages: []generation.Message{{Role: generation.RoleUser, Text: "x"}}}, nil)
	var providerErr *generation.ProviderError
	if !errors.As(err, &providerErr) || providerErr.Class != generation.ErrorTimeout || !providerErr.Retryable {
		t.Fatalf("expected retryable timeout, got %v", err)
	}
}
