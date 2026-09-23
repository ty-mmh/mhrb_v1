package memory

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"unicode/utf8"

	"mahoroba.local/mahoroba/internal/canonical"
)

const (
	DerivedClaimOutputVersionV1       = "memory-derived-output-v1"
	DerivedClaimOutputSchemaVersionV1 = "memory-derived-output-schema-v1"
)

type DerivedClaimOutput struct {
	Statement    string       `json:"statement"`
	TemporalKind TemporalKind `json:"temporal_kind"`
	Version      string       `json:"version"`
}

func DerivedClaimJSONSchema() (canonical.CanonicalJSON, error) {
	const schema = `{
		"$schema":"https://json-schema.org/draft/2020-12/schema",
		"type":"object",
		"additionalProperties":false,
		"required":["statement","temporal_kind","version"],
		"properties":{
			"statement":{"type":"string","minLength":1,"maxLength":2048},
			"temporal_kind":{"type":"string","enum":["stable","volatile","episodic"]},
			"version":{"type":"string","const":"memory-derived-output-v1"}
		}
	}`
	return canonical.CanonicalizeRFC8785([]byte(schema))
}

func ParseDerivedClaimOutput(input []byte) (DerivedClaimOutput, canonical.CanonicalJSON, error) {
	encoded, err := canonical.CanonicalizeRFC8785(input)
	if err != nil {
		return DerivedClaimOutput{}, canonical.CanonicalJSON{}, fmt.Errorf("%w: malformed derived output: %v", ErrInvalidExtraction, err)
	}
	decoder := json.NewDecoder(bytes.NewReader(encoded.Bytes()))
	decoder.DisallowUnknownFields()
	var output DerivedClaimOutput
	if err := decoder.Decode(&output); err != nil {
		return DerivedClaimOutput{}, canonical.CanonicalJSON{}, fmt.Errorf("%w: decode derived output: %v", ErrInvalidExtraction, err)
	}
	var trailing any
	if err := decoder.Decode(&trailing); err != io.EOF {
		return DerivedClaimOutput{}, canonical.CanonicalJSON{}, fmt.Errorf("%w: trailing derived output", ErrInvalidExtraction)
	}
	if output.Version != DerivedClaimOutputVersionV1 {
		return DerivedClaimOutput{}, canonical.CanonicalJSON{}, fmt.Errorf("%w: unsupported derived output version", ErrInvalidExtraction)
	}
	if !utf8.ValidString(output.Statement) || output.Statement == "" || len([]byte(output.Statement)) > MaximumStatementBytes {
		return DerivedClaimOutput{}, canonical.CanonicalJSON{}, fmt.Errorf("%w: invalid derived statement", ErrInvalidExtraction)
	}
	if normalized, err := NormalizeStatementV1(output.Statement); err != nil || normalized == "" {
		return DerivedClaimOutput{}, canonical.CanonicalJSON{}, fmt.Errorf("%w: derived statement normalizes to empty", ErrInvalidExtraction)
	}
	if err := output.TemporalKind.Validate(); err != nil {
		return DerivedClaimOutput{}, canonical.CanonicalJSON{}, fmt.Errorf("%w: %v", ErrInvalidExtraction, err)
	}
	want, err := canonical.MarshalCanonical(output)
	if err != nil || !bytes.Equal(want.Bytes(), encoded.Bytes()) {
		return DerivedClaimOutput{}, canonical.CanonicalJSON{}, fmt.Errorf("%w: derived output is missing required fields", ErrInvalidExtraction)
	}
	return output, encoded, nil
}
