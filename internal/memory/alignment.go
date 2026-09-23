package memory

import (
	"bytes"
	"encoding/json"
	"fmt"

	"mahoroba.local/mahoroba/internal/canonical"
)

const AlignmentOutputVersionV1 = "memory-alignment-output-v1"

// AlignmentOutput is intentionally only a verdict over the immutable claim
// pair selected by the Writer. The provider cannot name claims, kinds,
// principals, policies, or pipelines.
type AlignmentOutput struct {
	Version    string          `json:"version"`
	Aligned    bool            `json:"aligned"`
	Confidence canonical.Ratio `json:"confidence"`
}

func AlignmentJSONSchema() (canonical.CanonicalJSON, error) {
	const schema = `{
		"type":"object",
		"additionalProperties":false,
		"required":["version","aligned","confidence"],
		"properties":{
			"version":{"type":"string","enum":["memory-alignment-output-v1"]},
			"aligned":{"type":"boolean"},
			"confidence":{"type":"string","pattern":"^(0|[1-9][0-9]{0,5}|1000000)$"}
		}
	}`
	result, err := canonical.CanonicalizeRFC8785([]byte(schema))
	if err != nil {
		return canonical.CanonicalJSON{}, fmt.Errorf("%w: schema: %v", ErrInvalidAlignment, err)
	}
	return result, nil
}

func ParseAlignmentOutput(input []byte) (AlignmentOutput, canonical.CanonicalJSON, error) {
	encoded, err := canonical.CanonicalizeRFC8785(input)
	if err != nil {
		return AlignmentOutput{}, canonical.CanonicalJSON{}, fmt.Errorf("%w: malformed JSON: %v", ErrInvalidAlignment, err)
	}
	var output AlignmentOutput
	if err := decodeClosed(encoded.Bytes(), &output); err != nil {
		return AlignmentOutput{}, canonical.CanonicalJSON{}, fmt.Errorf("%w: %v", ErrInvalidAlignment, err)
	}
	if output.Version != AlignmentOutputVersionV1 {
		return AlignmentOutput{}, canonical.CanonicalJSON{}, fmt.Errorf("%w: unsupported version %q", ErrInvalidAlignment, output.Version)
	}
	if err := output.Confidence.Validate(); err != nil {
		return AlignmentOutput{}, canonical.CanonicalJSON{}, fmt.Errorf("%w: confidence: %v", ErrInvalidAlignment, err)
	}
	want, err := json.Marshal(output)
	if err != nil {
		return AlignmentOutput{}, canonical.CanonicalJSON{}, fmt.Errorf("%w: re-encode: %v", ErrInvalidAlignment, err)
	}
	canonicalWant, err := canonical.CanonicalizeRFC8785(want)
	if err != nil || !bytes.Equal(encoded.Bytes(), canonicalWant.Bytes()) {
		return AlignmentOutput{}, canonical.CanonicalJSON{}, fmt.Errorf("%w: object must contain exactly version, aligned, and confidence", ErrInvalidAlignment)
	}
	return output, encoded, nil
}
