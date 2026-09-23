package memory

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"strings"
	"time"
	"unicode/utf8"

	"mahoroba.local/mahoroba/internal/canonical"
)

const PersonaOutputSchemaVersionV1 = "persona-revision-output-v1"

// PersonaRevisionOutput is deliberately smaller than the generation
// envelope. Principles are neither an input nor an output of this pipeline.
// Contradiction is an explicit fail-safe signal: a proposal is still retained
// as a draft revision, but can never be activated automatically.
type PersonaRevisionOutput struct {
	Persona       string `json:"persona"`
	Contradiction bool   `json:"contradiction"`
}

type PersonaEditMetrics struct {
	ChangedBytes canonical.ByteSize
	ChangedRatio canonical.Ratio
	ChangedLines canonical.Count
	TotalBytes   canonical.ByteSize
}

func PersonaRevisionJSONSchema() (canonical.CanonicalJSON, error) {
	const schema = `{
		"$schema":"https://json-schema.org/draft/2020-12/schema",
		"type":"object",
		"additionalProperties":false,
		"required":["persona","contradiction"],
		"properties":{
			"persona":{"type":"string","minLength":1,"maxLength":8192},
			"contradiction":{"type":"boolean"}
		}
	}`
	return canonical.CanonicalizeRFC8785([]byte(schema))
}

func ParsePersonaRevisionOutput(input []byte) (PersonaRevisionOutput, canonical.CanonicalJSON, error) {
	encoded, err := canonical.CanonicalizeRFC8785(input)
	if err != nil {
		return PersonaRevisionOutput{}, canonical.CanonicalJSON{}, fmt.Errorf("%w: output is not canonical JSON: %v", ErrInvalidPersona, err)
	}
	decoder := json.NewDecoder(bytes.NewReader(encoded.Bytes()))
	decoder.DisallowUnknownFields()
	var output PersonaRevisionOutput
	if err := decoder.Decode(&output); err != nil {
		return PersonaRevisionOutput{}, canonical.CanonicalJSON{}, fmt.Errorf("%w: decode output: %v", ErrInvalidPersona, err)
	}
	var trailing any
	if err := decoder.Decode(&trailing); err != io.EOF {
		return PersonaRevisionOutput{}, canonical.CanonicalJSON{}, fmt.Errorf("%w: trailing JSON value", ErrInvalidPersona)
	}
	if output.Persona == "" || !utf8.ValidString(output.Persona) {
		return PersonaRevisionOutput{}, canonical.CanonicalJSON{}, fmt.Errorf("%w: persona must be non-empty UTF-8", ErrInvalidPersona)
	}
	want, err := canonical.MarshalCanonical(output)
	if err != nil || !bytes.Equal(want.Bytes(), encoded.Bytes()) {
		return PersonaRevisionOutput{}, canonical.CanonicalJSON{}, fmt.Errorf("%w: output must contain exactly persona and contradiction", ErrInvalidPersona)
	}
	return output, encoded, nil
}

// MeasurePersonaEdit computes Levenshtein distance over UTF-8 bytes. The M5
// policy intentionally defines a byte budget, so Unicode normalization and
// rune-based distance would silently change the contract.
func MeasurePersonaEdit(previous, proposed string) (PersonaEditMetrics, error) {
	oldBytes, newBytes := []byte(previous), []byte(proposed)
	if !utf8.ValidString(previous) || !utf8.ValidString(proposed) {
		return PersonaEditMetrics{}, fmt.Errorf("%w: persona is not valid UTF-8", ErrInvalidPersona)
	}
	distance := levenshteinBytes(oldBytes, newBytes)
	changed, err := canonical.NewByteSize(int64(distance))
	if err != nil {
		return PersonaEditMetrics{}, err
	}
	total, err := canonical.NewByteSize(int64(len(newBytes)))
	if err != nil {
		return PersonaEditMetrics{}, err
	}
	denominator, err := canonical.NewByteSize(int64(len(oldBytes)))
	if err != nil {
		return PersonaEditMetrics{}, err
	}
	var ratio canonical.Ratio
	if len(oldBytes) == 0 {
		if distance == 0 {
			ratio, err = canonical.NewRatio(0)
		} else {
			ratio, err = canonical.NewRatio(1_000_000)
		}
	} else if distance >= len(oldBytes) {
		ratio, err = canonical.NewRatio(1_000_000)
	} else {
		ratio, err = QuantizeChangedRatio(changed, denominator)
	}
	if err != nil {
		return PersonaEditMetrics{}, err
	}
	lines, err := canonical.NewCount(int64(changedLineCount(previous, proposed)))
	if err != nil {
		return PersonaEditMetrics{}, err
	}
	return PersonaEditMetrics{ChangedBytes: changed, ChangedRatio: ratio, ChangedLines: lines, TotalBytes: total}, nil
}

func levenshteinBytes(left, right []byte) int {
	if len(left) > len(right) {
		left, right = right, left
	}
	previous := make([]int, len(left)+1)
	for index := range previous {
		previous[index] = index
	}
	for rightIndex, rightByte := range right {
		current := make([]int, len(left)+1)
		current[0] = rightIndex + 1
		for leftIndex, leftByte := range left {
			cost := 0
			if leftByte != rightByte {
				cost = 1
			}
			deletion := previous[leftIndex+1] + 1
			insertion := current[leftIndex] + 1
			substitution := previous[leftIndex] + cost
			current[leftIndex+1] = min(deletion, insertion, substitution)
		}
		previous = current
	}
	return previous[len(left)]
}

func changedLineCount(previous, proposed string) int {
	oldLines, newLines := strings.Split(previous, "\n"), strings.Split(proposed, "\n")
	count := 0
	limit := max(len(oldLines), len(newLines))
	for index := 0; index < limit; index++ {
		if index >= len(oldLines) || index >= len(newLines) || oldLines[index] != newLines[index] {
			count++
		}
	}
	return count
}

type PersonaBlockingReason string

const (
	PersonaNoChange           PersonaBlockingReason = "no_change"
	PersonaTooFewClaims       PersonaBlockingReason = "too_few_claims"
	PersonaTooManyClaims      PersonaBlockingReason = "too_many_claims"
	PersonaChangedBytes       PersonaBlockingReason = "changed_bytes_limit"
	PersonaChangedRatio       PersonaBlockingReason = "changed_ratio_limit"
	PersonaChangedLines       PersonaBlockingReason = "changed_lines_limit"
	PersonaTotalBytes         PersonaBlockingReason = "total_bytes_limit"
	PersonaActivationCooldown PersonaBlockingReason = "activation_cooldown"
	PersonaContradiction      PersonaBlockingReason = "contradiction"
)

type PersonaThresholdInput struct {
	ClaimCount       canonical.Count
	ChangedBytes     canonical.ByteSize
	ChangedRatio     canonical.Ratio
	ChangedLines     canonical.Count
	TotalBytes       canonical.ByteSize
	AsOf             canonical.Instant
	LastActivationAt *canonical.Instant
}

type PersonaThresholdDecision struct {
	Eligible        bool
	BlockingReasons []PersonaBlockingReason
}

func EvaluatePersonaThreshold(policy Policy, input PersonaThresholdInput) (PersonaThresholdDecision, error) {
	if err := policy.RequireEnabled(); err != nil {
		return PersonaThresholdDecision{}, err
	}
	if err := input.ClaimCount.Validate(); err != nil {
		return PersonaThresholdDecision{}, fmt.Errorf("%w: claim count: %v", ErrInvalidPersona, err)
	}
	if err := input.ChangedBytes.Validate(); err != nil {
		return PersonaThresholdDecision{}, fmt.Errorf("%w: changed bytes: %v", ErrInvalidPersona, err)
	}
	if err := input.ChangedRatio.Validate(); err != nil {
		return PersonaThresholdDecision{}, fmt.Errorf("%w: changed ratio: %v", ErrInvalidPersona, err)
	}
	if err := input.ChangedLines.Validate(); err != nil {
		return PersonaThresholdDecision{}, fmt.Errorf("%w: changed lines: %v", ErrInvalidPersona, err)
	}
	if err := input.TotalBytes.Validate(); err != nil {
		return PersonaThresholdDecision{}, fmt.Errorf("%w: total bytes: %v", ErrInvalidPersona, err)
	}
	decision := PersonaThresholdDecision{}
	if input.ChangedBytes == 0 && input.ChangedLines == 0 {
		decision.BlockingReasons = append(decision.BlockingReasons, PersonaNoChange)
	}
	if input.ClaimCount.Int64() < policy.Persona.MinimumClaims {
		decision.BlockingReasons = append(decision.BlockingReasons, PersonaTooFewClaims)
	}
	if input.ClaimCount.Int64() > policy.Persona.MaximumClaims {
		decision.BlockingReasons = append(decision.BlockingReasons, PersonaTooManyClaims)
	}
	if input.ChangedBytes.Int64() > policy.Persona.MaximumChangedBytes {
		decision.BlockingReasons = append(decision.BlockingReasons, PersonaChangedBytes)
	}
	if input.ChangedRatio.Millionths() > policy.Persona.MaximumChangedRatio {
		decision.BlockingReasons = append(decision.BlockingReasons, PersonaChangedRatio)
	}
	if input.ChangedLines.Int64() > policy.Persona.MaximumChangedLines {
		decision.BlockingReasons = append(decision.BlockingReasons, PersonaChangedLines)
	}
	if input.TotalBytes.Int64() > policy.Persona.MaximumTotalBytes {
		decision.BlockingReasons = append(decision.BlockingReasons, PersonaTotalBytes)
	}
	if input.LastActivationAt != nil {
		if *input.LastActivationAt > input.AsOf {
			return PersonaThresholdDecision{}, fmt.Errorf("%w: activation is after as_of", ErrInvalidPersona)
		}
		elapsed, err := instantDifference(input.AsOf, *input.LastActivationAt)
		if err != nil {
			return PersonaThresholdDecision{}, fmt.Errorf("%w: activation interval: %v", ErrInvalidPersona, err)
		}
		cooldown, err := durationMicroseconds(policy.Persona.ActivationCooldownHours, time.Hour)
		if err != nil || cooldown <= 0 {
			return PersonaThresholdDecision{}, fmt.Errorf("%w: invalid activation cooldown", ErrInvalidPersona)
		}
		if elapsed < cooldown {
			decision.BlockingReasons = append(decision.BlockingReasons, PersonaActivationCooldown)
		}
	}
	decision.Eligible = len(decision.BlockingReasons) == 0
	return decision, nil
}

func QuantizeChangedRatio(changedBytes, totalBytes canonical.ByteSize) (canonical.Ratio, error) {
	if err := changedBytes.Validate(); err != nil {
		return 0, fmt.Errorf("%w: changed bytes: %v", ErrInvalidPersona, err)
	}
	if err := totalBytes.Validate(); err != nil {
		return 0, fmt.Errorf("%w: total bytes: %v", ErrInvalidPersona, err)
	}
	if changedBytes > totalBytes {
		return 0, fmt.Errorf("%w: changed bytes exceed total bytes", ErrInvalidPersona)
	}
	if totalBytes == 0 {
		if changedBytes == 0 {
			return canonical.NewRatio(0)
		}
		return 0, fmt.Errorf("%w: nonzero change with zero total bytes", ErrInvalidPersona)
	}
	return ratioOf(changedBytes.Int64(), totalBytes.Int64())
}
