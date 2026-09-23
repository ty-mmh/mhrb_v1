package domain

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"strings"

	"mahoroba.local/mahoroba/internal/canonical"
	"mahoroba.local/mahoroba/internal/generation"
)

const GeneratorParamsV2 = "generator-params-v2"

// GeneratorParams represents the legacy two-field dialogue envelope, the
// versioned three-field dialogue envelope, or the complete version-2
// structured-output envelope. Logical byte sizes use Mahoroba's canonical
// string encoding.
//
// Keep this type closed: accepting an unknown field and silently discarding it
// would make a recovered request differ from the request that was committed.
type GeneratorParams struct {
	Version              string                          `json:"version,omitempty"`
	Streaming            bool                            `json:"streaming"`
	MaxOutputBytes       canonical.ByteSize              `json:"max_output_bytes"`
	StructuredOutputMode generation.StructuredOutputMode `json:"structured_output_mode,omitempty"`
	SchemaVersion        string                          `json:"schema_version,omitempty"`
	SchemaHash           *canonical.Digest               `json:"schema_hash,omitempty"`
}

func (params GeneratorParams) IsLegacy() bool { return params.Version == "" }
func (params GeneratorParams) IsV2() bool     { return params.Version == GeneratorParamsV2 }
func (params GeneratorParams) IsStructured() bool {
	return params.StructuredOutputMode != "" && params.SchemaVersion != "" && params.SchemaHash != nil
}

// NewUnstructuredGeneratorParams builds the v2 envelope for new dialogue
// runs. Legacy two-field params remain readable but are never written by M5.
func NewUnstructuredGeneratorParams(
	streaming bool,
	maxOutputBytes canonical.ByteSize,
) (GeneratorParams, canonical.CanonicalJSON, error) {
	params := GeneratorParams{
		Version: GeneratorParamsV2, Streaming: streaming, MaxOutputBytes: maxOutputBytes,
	}
	if err := params.validate(); err != nil {
		return GeneratorParams{}, canonical.CanonicalJSON{}, err
	}
	encoded, err := canonical.MarshalCanonical(params)
	if err != nil {
		return GeneratorParams{}, canonical.CanonicalJSON{}, fmt.Errorf("domain: encode generator_params: %w", err)
	}
	return params, encoded, nil
}

// NewStructuredGeneratorParams builds the accepted v2 representation for a
// non-dialogue purpose. Mode must already be resolved: public config values
// such as auto/required never enter Canonical generator_params.
func NewStructuredGeneratorParams(
	streaming bool,
	maxOutputBytes canonical.ByteSize,
	mode generation.StructuredOutputMode,
	schemaVersion string,
	schemaHash canonical.Digest,
) (GeneratorParams, canonical.CanonicalJSON, error) {
	hash := schemaHash
	params := GeneratorParams{
		Version: GeneratorParamsV2, Streaming: streaming, MaxOutputBytes: maxOutputBytes,
		StructuredOutputMode: mode, SchemaVersion: schemaVersion, SchemaHash: &hash,
	}
	if err := params.validate(); err != nil {
		return GeneratorParams{}, canonical.CanonicalJSON{}, err
	}
	encoded, err := canonical.MarshalCanonical(params)
	if err != nil {
		return GeneratorParams{}, canonical.CanonicalJSON{}, fmt.Errorf("domain: encode generator_params: %w", err)
	}
	return params, encoded, nil
}

// ParseGeneratorParams accepts only the exact canonical representation of the
// currently supported generator parameter envelope. It rejects unknown,
// missing, duplicate, non-canonical, and non-positive values.
func ParseGeneratorParams(input []byte) (GeneratorParams, canonical.CanonicalJSON, error) {
	encoded, err := canonical.ParseCanonicalJSON(input)
	if err != nil {
		return GeneratorParams{}, canonical.CanonicalJSON{}, fmt.Errorf("domain: generator_params is not canonical: %w", err)
	}

	decoder := json.NewDecoder(bytes.NewReader(encoded.Bytes()))
	decoder.DisallowUnknownFields()
	var params GeneratorParams
	if err := decoder.Decode(&params); err != nil {
		return GeneratorParams{}, canonical.CanonicalJSON{}, fmt.Errorf("domain: invalid generator_params: %w", err)
	}
	if err := requireJSONEOF(decoder); err != nil {
		return GeneratorParams{}, canonical.CanonicalJSON{}, err
	}
	if err := params.validate(); err != nil {
		return GeneratorParams{}, canonical.CanonicalJSON{}, err
	}

	// Re-encoding is both a required-field check and a duplicate-key check: the
	// exact supported object has one occurrence of each field and nothing else.
	want, err := canonical.MarshalCanonical(params)
	if err != nil {
		return GeneratorParams{}, canonical.CanonicalJSON{}, fmt.Errorf("domain: encode generator_params: %w", err)
	}
	if !bytes.Equal(want.Bytes(), encoded.Bytes()) {
		if params.IsLegacy() {
			return GeneratorParams{}, canonical.CanonicalJSON{}, errorsGeneratorParams("legacy object must contain exactly streaming and max_output_bytes")
		}
		if params.IsStructured() {
			return GeneratorParams{}, canonical.CanonicalJSON{}, errorsGeneratorParams("structured v2 object must contain exactly version, streaming, max_output_bytes, structured_output_mode, schema_version, and schema_hash")
		}
		return GeneratorParams{}, canonical.CanonicalJSON{}, errorsGeneratorParams("unstructured v2 object must contain exactly version, streaming, and max_output_bytes")
	}
	return params, encoded, nil
}

func (params GeneratorParams) validate() error {
	if params.MaxOutputBytes <= 0 {
		return errorsGeneratorParams("max_output_bytes must be positive")
	}
	if params.IsLegacy() {
		if params.StructuredOutputMode != "" || params.SchemaVersion != "" || params.SchemaHash != nil {
			return errorsGeneratorParams("legacy dialogue params cannot contain structured-output fields")
		}
		return nil
	}
	if !params.IsV2() {
		return errorsGeneratorParams("unsupported version")
	}
	structuredFields := 0
	if params.StructuredOutputMode != "" {
		structuredFields++
	}
	if params.SchemaVersion != "" {
		structuredFields++
	}
	if params.SchemaHash != nil {
		structuredFields++
	}
	if structuredFields == 0 {
		return nil
	}
	if structuredFields != 3 {
		return errorsGeneratorParams("structured-output fields must be all present or all absent")
	}
	if err := params.StructuredOutputMode.Validate(); err != nil {
		return errorsGeneratorParams(err.Error())
	}
	if strings.TrimSpace(params.SchemaVersion) != params.SchemaVersion {
		return errorsGeneratorParams("schema_version is required without surrounding whitespace")
	}
	if *params.SchemaHash == (canonical.Digest{}) {
		return errorsGeneratorParams("schema_hash must be a non-zero SHA-256 digest")
	}
	return nil
}

func ValidateGeneratorParamsForPurpose(purpose GenerationPurpose, params GeneratorParams) error {
	if err := purpose.Validate(); err != nil {
		return err
	}
	if purpose.RequiresStructuredOutput() {
		if !params.IsV2() || !params.IsStructured() {
			return errorsGeneratorParams("structured generation purpose requires v2 params")
		}
		return nil
	}
	if params.IsLegacy() {
		if purpose.Effective() == GenerationPurposeDialogue {
			return nil
		}
		return errorsGeneratorParams("legacy params are valid only for dialogue compatibility")
	}
	if !params.IsV2() || params.IsStructured() {
		if purpose.Effective() == GenerationPurposeDialogue {
			return errorsGeneratorParams("dialogue purpose requires unstructured params")
		}
		return errorsGeneratorParams("unstructured generation purpose requires unstructured v2 params")
	}
	return nil
}

// ValidateNewGeneratorParamsForPurpose is the Writer boundary. Compatibility
// parsing accepts legacy dialogue params so existing runs can retry; every new
// run must carry an explicit versioned v2 envelope.
func ValidateNewGeneratorParamsForPurpose(purpose GenerationPurpose, params GeneratorParams) error {
	if err := ValidateGeneratorParamsForPurpose(purpose, params); err != nil {
		return err
	}
	if params.IsLegacy() {
		return errorsGeneratorParams("new generation runs require versioned v2 params")
	}
	return nil
}

func requireJSONEOF(decoder *json.Decoder) error {
	var trailing any
	if err := decoder.Decode(&trailing); err != io.EOF {
		if err == nil {
			return errorsGeneratorParams("contains a trailing JSON value")
		}
		return fmt.Errorf("domain: invalid generator_params trailing data: %w", err)
	}
	return nil
}

func errorsGeneratorParams(detail string) error {
	return fmt.Errorf("domain: invalid generator_params: %s", detail)
}
