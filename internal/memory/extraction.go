package memory

import (
	"bytes"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"strings"
	"unicode/utf8"

	"mahoroba.local/mahoroba/internal/canonical"
)

const (
	ExtractionOutputVersionV1       = "memory-extraction-output-v1"
	ExtractionOutputSchemaVersionV1 = "memory-extraction-output-schema-v1"
	MaximumExtractionClaims         = 8
	MaximumStatementBytes           = 2048
)

type PrincipalSelector string

const (
	SelectorResident    PrincipalSelector = "resident"
	SelectorSourceActor PrincipalSelector = "source_actor"
)

func (value PrincipalSelector) Validate() error {
	switch value {
	case SelectorResident, SelectorSourceActor:
		return nil
	default:
		return enumError("principal selector", string(value))
	}
}

type ExtractionClaim struct {
	Statement    string            `json:"statement"`
	Subject      PrincipalSelector `json:"subject"`
	Perspective  PrincipalSelector `json:"perspective"`
	TemporalKind TemporalKind      `json:"temporal_kind"`
	Grade        EvidenceGrade     `json:"grade"`
	SourceQuote  string            `json:"source_quote"`
}

type ExtractionOutput struct {
	Version string            `json:"version"`
	Claims  []ExtractionClaim `json:"claims"`
}

type DerivedIdentity [sha256.Size]byte

// ExtractionJSONSchema returns the immutable provider-facing schema for v1
// extraction. JSON Schema measures maxLength in Unicode code points, so the
// 2048 UTF-8 byte limit and the grade/source_quote correlation intentionally
// remain authoritative parser rules.
func ExtractionJSONSchema() (canonical.CanonicalJSON, error) {
	const schema = `{
		"type":"object",
		"additionalProperties":false,
		"required":["version","claims"],
		"properties":{
			"version":{"type":"string","enum":["memory-extraction-output-v1"]},
			"claims":{
				"type":"array","maxItems":8,
				"items":{
					"type":"object",
					"additionalProperties":false,
					"required":["statement","subject","perspective","temporal_kind","grade","source_quote"],
					"properties":{
						"statement":{"type":"string","minLength":1},
						"subject":{"type":"string","enum":["resident","source_actor"]},
						"perspective":{"type":"string","enum":["resident","source_actor"]},
						"temporal_kind":{"type":"string","enum":["stable","volatile","episodic"]},
						"grade":{"type":"string","enum":["stated","inferred"]},
						"source_quote":{"type":"string"}
					}
				}
			}
		}
	}`
	encoded, err := canonical.CanonicalizeRFC8785([]byte(schema))
	if err != nil {
		return canonical.CanonicalJSON{}, fmt.Errorf("%w: canonical extraction schema: %v", ErrInvalidExtraction, err)
	}
	return encoded, nil
}

// ParseExtractionOutput accepts ordinary provider JSON, rejects duplicate and
// unknown fields, and returns a JCS copy for durable output validation. Raw
// statement bytes are preserved; normalization is used only for identity.
func ParseExtractionOutput(input []byte, sourceText string) (ExtractionOutput, canonical.CanonicalJSON, error) {
	if !utf8.ValidString(sourceText) {
		return ExtractionOutput{}, canonical.CanonicalJSON{}, fmt.Errorf("%w: source text is not UTF-8", ErrInvalidExtraction)
	}
	encoded, err := canonical.CanonicalizeRFC8785(input)
	if err != nil {
		return ExtractionOutput{}, canonical.CanonicalJSON{}, fmt.Errorf("%w: malformed JSON: %v", ErrInvalidExtraction, err)
	}
	var output ExtractionOutput
	if err := decodeClosed(encoded.Bytes(), &output); err != nil {
		return ExtractionOutput{}, canonical.CanonicalJSON{}, fmt.Errorf("%w: %v", ErrInvalidExtraction, err)
	}
	if output.Version != ExtractionOutputVersionV1 {
		return ExtractionOutput{}, canonical.CanonicalJSON{}, fmt.Errorf("%w: unsupported version %q", ErrInvalidExtraction, output.Version)
	}
	if output.Claims == nil {
		return ExtractionOutput{}, canonical.CanonicalJSON{}, fmt.Errorf("%w: claims must be an array", ErrInvalidExtraction)
	}
	if len(output.Claims) > MaximumExtractionClaims {
		return ExtractionOutput{}, canonical.CanonicalJSON{}, fmt.Errorf("%w: %d claims exceeds limit %d", ErrInvalidExtraction, len(output.Claims), MaximumExtractionClaims)
	}

	// Comparing the closed decoded value with the canonical input catches every
	// missing required key. Unknown and duplicate keys were rejected earlier.
	raw, err := json.Marshal(output)
	if err != nil {
		return ExtractionOutput{}, canonical.CanonicalJSON{}, fmt.Errorf("%w: re-encode: %v", ErrInvalidExtraction, err)
	}
	want, err := canonical.CanonicalizeRFC8785(raw)
	if err != nil {
		return ExtractionOutput{}, canonical.CanonicalJSON{}, fmt.Errorf("%w: canonical re-encode: %v", ErrInvalidExtraction, err)
	}
	if !bytes.Equal(encoded.Bytes(), want.Bytes()) {
		return ExtractionOutput{}, canonical.CanonicalJSON{}, fmt.Errorf("%w: object is missing required fields", ErrInvalidExtraction)
	}

	identities := make(map[DerivedIdentity]int, len(output.Claims))
	for index, claim := range output.Claims {
		if err := ValidateExtractionClaim(claim, sourceText); err != nil {
			return ExtractionOutput{}, canonical.CanonicalJSON{}, fmt.Errorf("%w: claim %d: %v", ErrInvalidExtraction, index, err)
		}
		identity, err := DerivedClaimIdentity(claim)
		if err != nil {
			return ExtractionOutput{}, canonical.CanonicalJSON{}, fmt.Errorf("%w: claim %d identity: %v", ErrInvalidExtraction, index, err)
		}
		if previous, duplicate := identities[identity]; duplicate {
			return ExtractionOutput{}, canonical.CanonicalJSON{}, fmt.Errorf("%w: claims %d and %d have duplicate derived identity", ErrInvalidExtraction, previous, index)
		}
		identities[identity] = index
	}
	return output, encoded, nil
}

func ValidateExtractionClaim(claim ExtractionClaim, sourceText string) error {
	if !utf8.ValidString(claim.Statement) || claim.Statement == "" {
		return fmt.Errorf("statement must be non-empty UTF-8")
	}
	if len([]byte(claim.Statement)) > MaximumStatementBytes {
		return fmt.Errorf("statement exceeds %d UTF-8 bytes", MaximumStatementBytes)
	}
	normalized, err := NormalizeStatementV1(claim.Statement)
	if err != nil || normalized == "" {
		return fmt.Errorf("statement normalizes to empty text")
	}
	if err := claim.Subject.Validate(); err != nil {
		return err
	}
	if err := claim.Perspective.Validate(); err != nil {
		return err
	}
	if err := claim.TemporalKind.Validate(); err != nil {
		return err
	}
	if claim.Grade != GradeStated && claim.Grade != GradeInferred {
		return fmt.Errorf("grade must be stated or inferred")
	}
	if !utf8.ValidString(claim.SourceQuote) {
		return fmt.Errorf("source_quote is not UTF-8")
	}
	switch claim.Grade {
	case GradeStated:
		if claim.SourceQuote == "" {
			return fmt.Errorf("stated claim requires a non-empty source_quote")
		}
		if !strings.Contains(sourceText, claim.SourceQuote) {
			return fmt.Errorf("stated source_quote is not an exact source substring")
		}
	case GradeInferred:
		if claim.SourceQuote != "" {
			return fmt.Errorf("inferred claim requires an empty source_quote")
		}
	}
	return nil
}

// NormalizeStatementV1 collapses all Unicode whitespace to one ASCII space,
// trims the ends as a consequence of strings.Fields, and applies Unicode
// lower-casing. It deliberately performs no NFC/NFKC, punctuation removal, or
// stemming.
func NormalizeStatementV1(statement string) (string, error) {
	if !utf8.ValidString(statement) {
		return "", fmt.Errorf("memory: statement is not UTF-8")
	}
	return strings.ToLower(strings.Join(strings.Fields(statement), " ")), nil
}

func DerivedClaimIdentity(claim ExtractionClaim) (DerivedIdentity, error) {
	normalized, err := NormalizeStatementV1(claim.Statement)
	if err != nil {
		return DerivedIdentity{}, err
	}
	if normalized == "" {
		return DerivedIdentity{}, fmt.Errorf("memory: normalized statement is empty")
	}
	if err := claim.Subject.Validate(); err != nil {
		return DerivedIdentity{}, err
	}
	if err := claim.Perspective.Validate(); err != nil {
		return DerivedIdentity{}, err
	}
	if err := claim.TemporalKind.Validate(); err != nil {
		return DerivedIdentity{}, err
	}
	wire := struct {
		NormalizationVersion string            `json:"normalization_version"`
		NormalizedStatement  string            `json:"normalized_statement"`
		Subject              PrincipalSelector `json:"subject"`
		Perspective          PrincipalSelector `json:"perspective"`
		TemporalKind         TemporalKind      `json:"temporal_kind"`
	}{
		NormalizationVersion: NormalizationVersionV1, NormalizedStatement: normalized,
		Subject: claim.Subject, Perspective: claim.Perspective, TemporalKind: claim.TemporalKind,
	}
	raw, err := json.Marshal(wire)
	if err != nil {
		return DerivedIdentity{}, err
	}
	canonicalWire, err := canonical.CanonicalizeRFC8785(raw)
	if err != nil {
		return DerivedIdentity{}, err
	}
	digest := sha256.New()
	digest.Write([]byte("mahoroba:memory-derived-identity:v1\x00"))
	digest.Write(canonicalWire.Bytes())
	var identity DerivedIdentity
	copy(identity[:], digest.Sum(nil))
	return identity, nil
}
