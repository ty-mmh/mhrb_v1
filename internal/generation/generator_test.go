package generation

import (
	"strings"
	"testing"

	"mahoroba.local/mahoroba/internal/canonical"
)

func TestSchemaContractPinsCanonicalSchemaAndCapability(t *testing.T) {
	schema, err := canonical.CanonicalizeRFC8785([]byte(`{ "type": "object", "properties": {"value": {"type": "string"}}, "additionalProperties": false }`))
	if err != nil {
		t.Fatal(err)
	}
	contract, err := NewSchemaContract("memory_extraction", "memory-extraction-output-v1", StructuredOutputJSONSchema, schema)
	if err != nil {
		t.Fatal(err)
	}
	if contract.Hash != canonical.HashBlob(schema.Bytes()) {
		t.Fatal("schema contract did not pin canonical bytes")
	}
	if err := contract.Validate(Capabilities{SupportsJSONSchema: true}); err != nil {
		t.Fatal(err)
	}
	if err := contract.Validate(Capabilities{}); err == nil {
		t.Fatal("JSON Schema mode ignored provider capability")
	}

	contract.Mode = StructuredOutputPrompt
	if err := contract.Validate(Capabilities{}); err != nil {
		t.Fatalf("prompt fallback depends on provider capability: %v", err)
	}
	instruction := contract.PromptInstruction()
	if !strings.Contains(instruction, contract.Version) || !strings.Contains(instruction, contract.Hash.Hex()) ||
		!strings.Contains(instruction, contract.Schema.String()) {
		t.Fatalf("prompt instruction does not pin schema contract: %q", instruction)
	}
}

func TestSchemaContractRejectsHashDriftAndInvalidShape(t *testing.T) {
	object, err := canonical.CanonicalizeRFC8785([]byte(`{"type":"object"}`))
	if err != nil {
		t.Fatal(err)
	}
	contract, err := NewSchemaContract("memory_extraction", "v1", StructuredOutputPrompt, object)
	if err != nil {
		t.Fatal(err)
	}
	contract.Hash = canonical.HashBlob([]byte("different"))
	if err := contract.Validate(Capabilities{}); err == nil {
		t.Fatal("schema hash drift was accepted")
	}

	array, err := canonical.CanonicalizeRFC8785([]byte(`[]`))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := NewSchemaContract("memory_extraction", "v1", StructuredOutputPrompt, array); err == nil {
		t.Fatal("non-object JSON Schema root was accepted")
	}
	if _, err := NewSchemaContract("bad name", "v1", StructuredOutputPrompt, object); err == nil {
		t.Fatal("provider-unsafe schema name was accepted")
	}
}

func TestOutcomeErrorCodeRetryPolicy(t *testing.T) {
	tests := []struct {
		name      string
		err       error
		wantCode  string
		retryable bool
	}{
		{
			name:     "rate limit",
			err:      &ProviderError{Class: ErrorHTTP, StatusCode: 429},
			wantCode: "provider_http:429", retryable: true,
		},
		{
			name:     "server failure",
			err:      &ProviderError{Class: ErrorHTTP, StatusCode: 503},
			wantCode: "provider_http:503", retryable: true,
		},
		{
			name:     "unauthorized even when adapter flag disagrees",
			err:      &ProviderError{Class: ErrorHTTP, StatusCode: 401, Retryable: true},
			wantCode: "provider_http:401", retryable: false,
		},
		{
			name:     "invalid response",
			err:      &ProviderError{Class: ErrorInvalidResponse},
			wantCode: string(ErrorInvalidResponse), retryable: false,
		},
		{
			name:     "timeout",
			err:      &ProviderError{Class: ErrorTimeout},
			wantCode: string(ErrorTimeout), retryable: true,
		},
		{
			name:     "cancelled",
			err:      &ProviderError{Class: ErrorCancelled, Retryable: true},
			wantCode: string(ErrorCancelled), retryable: false,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			code := OutcomeErrorCodeFromError(test.err)
			if code.String() != test.wantCode || code.Retryable() != test.retryable {
				t.Fatalf("code = %q retryable=%v, want %q retryable=%v", code, code.Retryable(), test.wantCode, test.retryable)
			}
			restored, err := ParseOutcomeErrorCode(code.String())
			if err != nil {
				t.Fatal(err)
			}
			if restored.String() != code.String() || restored.Retryable() != code.Retryable() {
				t.Fatalf("restored code = %q retryable=%v, want %q retryable=%v", restored, restored.Retryable(), code, code.Retryable())
			}
		})
	}

	interrupted := RuntimeInterruptedErrorCode()
	if !interrupted.Retryable() {
		t.Fatal("runtime interruption must remain retryable after restart")
	}
	landing := MustOutcomeErrorCode(ErrorLandingFailure, 0)
	if !landing.Retryable() {
		t.Fatal("landing failure must remain retryable after restart")
	}
	for _, class := range []ErrorClass{ErrorSourceContentErased, ErrorResidentInactive, ErrorResidentUnselected, ErrorProviderUnsupported} {
		code := MustOutcomeErrorCode(class, 0)
		if code.Retryable() {
			t.Errorf("cancellation code %q must be terminal", code)
		}
	}
}

func TestParseOutcomeErrorCodeRejectsNonCanonicalHTTPStatus(t *testing.T) {
	for _, raw := range []string{"", "provider_http", "provider_http:", "provider_http:0429", "provider_http:0", "provider_http:429:extra", "unknown"} {
		if _, err := ParseOutcomeErrorCode(raw); err == nil {
			t.Errorf("ParseOutcomeErrorCode(%q) unexpectedly succeeded", raw)
		}
	}
}

func TestProviderErrorHTTPRenderingOmitsEmptyDetail(t *testing.T) {
	err := &ProviderError{Class: ErrorHTTP, StatusCode: 503}
	if got, want := err.Error(), "provider_http (HTTP 503)"; got != want {
		t.Fatalf("ProviderError.Error() = %q, want %q", got, want)
	}
}
