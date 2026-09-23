package generation

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"

	"mahoroba.local/mahoroba/internal/canonical"
)

type Role string

const (
	RoleSystem    Role = "system"
	RoleUser      Role = "user"
	RoleAssistant Role = "assistant"
)

type Message struct {
	Role Role
	Text string
}

type StructuredOutputMode string

const (
	StructuredOutputPrompt     StructuredOutputMode = "prompt"
	StructuredOutputJSONSchema StructuredOutputMode = "json_schema"
)

func (mode StructuredOutputMode) Validate() error {
	switch mode {
	case StructuredOutputPrompt, StructuredOutputJSONSchema:
		return nil
	default:
		return fmt.Errorf("generation: unsupported resolved structured-output mode %q", mode)
	}
}

// Capabilities describes provider behavior that is relevant while serving a
// frozen request. It is deliberately not persisted as a substitute for the
// resolved mode in generator_params.
type Capabilities struct {
	SupportsJSONSchema bool
}

// CapabilityProvider is optional so existing generators remain source
// compatible. Coordinators can inspect it before preparing structured work.
type CapabilityProvider interface {
	Capabilities() Capabilities
}

// SchemaContract is the executable schema selected by a versioned registry.
// Version and Hash are persisted in generator_params; Schema is re-resolved
// and checked against that hash before every provider attempt.
type SchemaContract struct {
	Name    string
	Version string
	Hash    canonical.Digest
	Schema  canonical.CanonicalJSON
	Mode    StructuredOutputMode
}

func NewSchemaContract(name, version string, mode StructuredOutputMode, schema canonical.CanonicalJSON) (SchemaContract, error) {
	contract := SchemaContract{
		Name: name, Version: version, Hash: canonical.HashBlob(schema.Bytes()), Schema: schema, Mode: mode,
	}
	if err := contract.Validate(Capabilities{SupportsJSONSchema: true}); err != nil {
		return SchemaContract{}, err
	}
	return contract, nil
}

func (contract SchemaContract) Validate(capabilities Capabilities) error {
	if !validSchemaName(contract.Name) {
		return errors.New("generation: structured-output schema name must contain 1-64 ASCII letters, digits, underscores, or hyphens")
	}
	if contract.Version == "" || strings.TrimSpace(contract.Version) != contract.Version {
		return errors.New("generation: structured-output schema version is required without surrounding whitespace")
	}
	if err := contract.Mode.Validate(); err != nil {
		return err
	}
	if contract.Schema.IsZero() {
		return errors.New("generation: structured-output schema is required")
	}
	var root map[string]json.RawMessage
	if err := json.Unmarshal(contract.Schema.Bytes(), &root); err != nil || root == nil {
		return errors.New("generation: structured-output schema root must be a JSON object")
	}
	if actual := canonical.HashBlob(contract.Schema.Bytes()); actual != contract.Hash {
		return fmt.Errorf("generation: structured-output schema hash mismatch: got %s want %s", actual, contract.Hash)
	}
	if contract.Mode == StructuredOutputJSONSchema && !capabilities.SupportsJSONSchema {
		return errors.New("generation: provider does not support required JSON Schema response format")
	}
	return nil
}

// PromptInstruction is deterministic for a given schema contract. Prompt
// mode adds it as the final system message; JSON-Schema mode never adds it.
func (contract SchemaContract) PromptInstruction() string {
	return "Return exactly one JSON value that validates against schema " + contract.Version +
		" (sha256:" + contract.Hash.Hex() + "). Do not use Markdown fences or add prose.\nJSON Schema:\n" + contract.Schema.String()
}

func validSchemaName(value string) bool {
	if len(value) < 1 || len(value) > 64 {
		return false
	}
	for _, char := range value {
		if (char >= 'a' && char <= 'z') || (char >= 'A' && char <= 'Z') ||
			(char >= '0' && char <= '9') || char == '_' || char == '-' {
			continue
		}
		return false
	}
	return true
}

type Request struct {
	GenerationRunID  string
	Purpose          string
	Model            string
	Messages         []Message
	Streaming        bool
	MaxOutputBytes   int
	StructuredOutput *SchemaContract
}

type Delta struct {
	Text string
}

type Result struct {
	Text             string
	FinishReason     string
	ModelVersion     string
	PromptTokens     *int64
	CompletionTokens *int64
}

type DeltaSink func(Delta)

type Generator interface {
	Stream(context.Context, Request, DeltaSink) (Result, error)
}

// IdentifiedGenerator exposes the non-secret identity of the provider route
// that will serve a request. Implementations should include every routing
// attribute that could send a recovered request to a different provider, but
// must never include credentials.
type IdentifiedGenerator interface {
	Generator
	ProviderIdentity() string
}

type ErrorClass string

const (
	ErrorTransport           ErrorClass = "provider_transport"
	ErrorTimeout             ErrorClass = "provider_timeout"
	ErrorCancelled           ErrorClass = "provider_cancelled"
	ErrorHTTP                ErrorClass = "provider_http"
	ErrorInvalidResponse     ErrorClass = "provider_invalid_response"
	ErrorUnknown             ErrorClass = "provider_failure"
	ErrorLandingFailure      ErrorClass = "landing_failure"
	ErrorRuntimeInterrupted  ErrorClass = "runtime_interrupted"
	ErrorForegroundPreempted ErrorClass = "foreground_preempted"
	ErrorSourceContentErased ErrorClass = "source_content_erased"
	ErrorResidentInactive    ErrorClass = "resident_inactive"
	ErrorResidentUnselected  ErrorClass = "resident_unselected"
	ErrorProviderUnsupported ErrorClass = "provider_unsupported"
)

// OutcomeErrorCode is the stable value persisted in
// generation_run_outcomes.error_class. HTTP failures include the status code
// (for example, provider_http:429), so retry eligibility can be re-derived
// from Canonical state after restart or a policy update.
type OutcomeErrorCode struct {
	class      ErrorClass
	httpStatus int
}

func NewOutcomeErrorCode(class ErrorClass, httpStatus int) (OutcomeErrorCode, error) {
	switch class {
	case ErrorTransport, ErrorTimeout, ErrorCancelled, ErrorInvalidResponse, ErrorUnknown,
		ErrorLandingFailure, ErrorRuntimeInterrupted, ErrorForegroundPreempted, ErrorSourceContentErased, ErrorResidentInactive,
		ErrorResidentUnselected, ErrorProviderUnsupported:
		if httpStatus != 0 {
			return OutcomeErrorCode{}, fmt.Errorf("generation: HTTP status is only valid for %s", ErrorHTTP)
		}
	case ErrorHTTP:
		if httpStatus < 100 || httpStatus > 999 {
			return OutcomeErrorCode{}, fmt.Errorf("generation: invalid HTTP status %d", httpStatus)
		}
	default:
		return OutcomeErrorCode{}, fmt.Errorf("generation: unknown outcome error class %q", class)
	}
	return OutcomeErrorCode{class: class, httpStatus: httpStatus}, nil
}

func MustOutcomeErrorCode(class ErrorClass, httpStatus int) OutcomeErrorCode {
	code, err := NewOutcomeErrorCode(class, httpStatus)
	if err != nil {
		panic(err)
	}
	return code
}

func ParseOutcomeErrorCode(raw string) (OutcomeErrorCode, error) {
	if raw == "" {
		return OutcomeErrorCode{}, errors.New("generation: empty outcome error code")
	}
	classRaw, statusRaw, hasStatus := strings.Cut(raw, ":")
	class := ErrorClass(classRaw)
	// provider_transient was emitted by the pre-M3 test/provider boundary. Keep
	// it readable as the canonical transport class during recovery, while new
	// writes continue to use provider_transport.
	if class == ErrorClass("provider_transient") {
		class = ErrorTransport
	}
	if !hasStatus {
		return NewOutcomeErrorCode(class, 0)
	}
	if class != ErrorHTTP || statusRaw == "" || strings.Contains(statusRaw, ":") {
		return OutcomeErrorCode{}, fmt.Errorf("generation: invalid outcome error code %q", raw)
	}
	status, err := strconv.Atoi(statusRaw)
	if err != nil || status == 0 || strconv.Itoa(status) != statusRaw {
		return OutcomeErrorCode{}, fmt.Errorf("generation: invalid outcome error code %q", raw)
	}
	return NewOutcomeErrorCode(class, status)
}

func OutcomeErrorCodeFromError(err error) OutcomeErrorCode {
	var providerErr *ProviderError
	if errors.As(err, &providerErr) {
		code, codeErr := NewOutcomeErrorCode(providerErr.Class, providerErr.StatusCode)
		if codeErr == nil {
			return code
		}
	}
	return MustOutcomeErrorCode(ErrorUnknown, 0)
}

func RuntimeInterruptedErrorCode() OutcomeErrorCode {
	return MustOutcomeErrorCode(ErrorRuntimeInterrupted, 0)
}

func (code OutcomeErrorCode) Class() ErrorClass { return code.class }
func (code OutcomeErrorCode) HTTPStatus() int   { return code.httpStatus }

func (code OutcomeErrorCode) String() string {
	if code.class == ErrorHTTP {
		return string(code.class) + ":" + strconv.Itoa(code.httpStatus)
	}
	return string(code.class)
}

// Retryable is the initial M3 retry policy. It is intentionally derived from
// the stable persisted code rather than stored as a frozen boolean.
func (code OutcomeErrorCode) Retryable() bool {
	switch code.class {
	case ErrorTransport, ErrorTimeout, ErrorLandingFailure, ErrorRuntimeInterrupted, ErrorForegroundPreempted:
		return true
	case ErrorHTTP:
		return RetryableHTTPStatus(code.httpStatus)
	default:
		return false
	}
}

func RetryableHTTPStatus(status int) bool {
	return status == 408 || status == 409 || status == 425 || status == 429 || status >= 500
}

type ProviderError struct {
	Class      ErrorClass
	Retryable  bool
	StatusCode int
	Detail     string
	Cause      error
}

func (e *ProviderError) Error() string {
	if e.StatusCode != 0 {
		if e.Detail == "" {
			return fmt.Sprintf("%s (HTTP %d)", e.Class, e.StatusCode)
		}
		return fmt.Sprintf("%s (HTTP %d): %s", e.Class, e.StatusCode, e.Detail)
	}
	if e.Detail != "" {
		return fmt.Sprintf("%s: %s", e.Class, e.Detail)
	}
	return string(e.Class)
}

func (e *ProviderError) Unwrap() error { return e.Cause }
