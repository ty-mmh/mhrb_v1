// Package surfaceref implements the closed, deterministic surface-reference
// detector used by runtime-states-v2 and dialogue-context-v3.
package surfaceref

import (
	"errors"
	"strings"
	"unicode"
	"unicode/utf8"

	"mahoroba.local/mahoroba/internal/canonical"
)

const Version = "surface-reference-v1"

type Marker string

const (
	ContinuationRequest    Marker = "continuation_request"
	DemonstrativeReference Marker = "demonstrative_reference"
	PriorContextReference  Marker = "prior_context_reference"
)

var ErrInvalidUTF8 = errors.New("surface reference: input is not valid UTF-8")

// Detect returns the closed marker set in ASCII lexical order. It intentionally
// examines only utterance prefixes; it does not perform semantic inference,
// normalization beyond the v1 rules, or explicit event-reference parsing.
func Detect(input []byte) ([]Marker, error) {
	value, err := normalize(input)
	if err != nil {
		return nil, err
	}
	markers := make(map[Marker]struct{}, 3)

	if detectsJapaneseContinuation(value) || detectsEnglishContinuation(value) {
		markers[ContinuationRequest] = struct{}{}
	}
	if detectsJapaneseDemonstrative(value) || detectsEnglishDemonstrative(value) {
		markers[DemonstrativeReference] = struct{}{}
	}
	if detectsJapanesePrior(value) || detectsEnglishPrior(value) {
		markers[PriorContextReference] = struct{}{}
	}
	// These phrases are closed combinations whose continuation wording itself
	// carries a prior-conversation reference.
	if hasEnglishPrefix(value, "pick up where we left off") ||
		hasJapanesePrefix(value, "前回の続き", japaneseContinuationSuffixes) ||
		hasJapanesePrefix(value, "さっきの続き", japaneseContinuationSuffixes) ||
		hasJapanesePrefix(value, "先ほどの続き", japaneseContinuationSuffixes) {
		markers[ContinuationRequest] = struct{}{}
		markers[PriorContextReference] = struct{}{}
	}

	ordered := make([]Marker, 0, len(markers))
	for _, marker := range []Marker{ContinuationRequest, DemonstrativeReference, PriorContextReference} {
		if _, present := markers[marker]; present {
			ordered = append(ordered, marker)
		}
	}
	return ordered, nil
}

// CanonicalJSON serializes a detector result without permitting unknown,
// duplicate, or non-canonical marker ordering.
func CanonicalJSON(markers []Marker) (canonical.CanonicalJSON, error) {
	previous := Marker("")
	values := make([]string, 0, len(markers))
	for _, marker := range markers {
		if marker != ContinuationRequest && marker != DemonstrativeReference && marker != PriorContextReference {
			return canonical.CanonicalJSON{}, errors.New("surface reference: unknown marker")
		}
		if previous != "" && previous >= marker {
			return canonical.CanonicalJSON{}, errors.New("surface reference: markers are not strictly ordered")
		}
		previous = marker
		values = append(values, string(marker))
	}
	return canonical.MarshalCanonical(values)
}

func normalize(input []byte) (string, error) {
	if !utf8.Valid(input) {
		return "", ErrInvalidUTF8
	}
	var builder strings.Builder
	builder.Grow(len(input))
	spacePending := false
	for _, character := range string(input) {
		if unicode.IsSpace(character) {
			if builder.Len() > 0 {
				spacePending = true
			}
			continue
		}
		if spacePending {
			builder.WriteByte(' ')
			spacePending = false
		}
		if character >= 'A' && character <= 'Z' {
			character += 'a' - 'A'
		}
		builder.WriteRune(character)
	}
	value := strings.TrimRightFunc(builder.String(), isTrailingPunctuation)
	return strings.TrimSpace(value), nil
}

func isTrailingPunctuation(character rune) bool {
	return strings.ContainsRune(".,!?;:、。！？；：，．…", character)
}

var japaneseContinuationSuffixes = []string{
	"を", "から", "について", "お願い", "お願いします", "に戻", "ください", "話", "進め", "やって", "して", "しよう", "しましょう", "します",
}

var japanesePriorSuffixes = []string{
	"を", "について", "で", "の", "から", "お願い", "お願いします", "続け", "話", "まで", "に戻",
}

var japaneseDemonstrativeSuffixes = []string{
	"を", "について", "の話", "の件", "お願い", "お願いします", "続けて", "に戻",
}

func detectsJapaneseContinuation(value string) bool {
	for _, prefix := range []string{"続き", "続けて", "再開"} {
		if hasJapanesePrefix(value, prefix, japaneseContinuationSuffixes) {
			return true
		}
	}
	return false
}

func detectsJapanesePrior(value string) bool {
	for _, prefix := range []string{"前回", "さっき", "先ほど"} {
		if hasJapanesePrefix(value, prefix, japanesePriorSuffixes) {
			return true
		}
	}
	return false
}

func detectsJapaneseDemonstrative(value string) bool {
	for _, prefix := range []string{
		"それ", "あれ", "その件", "その話", "その計画", "そのトピック", "その問題",
		"あの件", "あの話", "あの計画", "あのトピック", "あの問題",
	} {
		if hasJapanesePrefix(value, prefix, japaneseDemonstrativeSuffixes) {
			return true
		}
	}
	return false
}

func detectsEnglishContinuation(value string) bool {
	for _, prefix := range []string{"continue", "resume", "pick up where we left off"} {
		if hasEnglishPrefix(value, prefix) {
			return true
		}
	}
	return false
}

func detectsEnglishPrior(value string) bool {
	for _, prefix := range []string{
		"previous conversation", "the previous conversation", "our previous conversation",
		"last conversation", "the last conversation", "our last conversation",
		"earlier conversation", "the earlier conversation", "where we left off",
		"what we discussed before", "pick up where we left off",
	} {
		if hasEnglishPrefix(value, prefix) {
			return true
		}
	}
	return false
}

func detectsEnglishDemonstrative(value string) bool {
	for _, prefix := range []string{"that topic", "that issue", "that plan"} {
		if hasEnglishPrefix(value, prefix) {
			return true
		}
	}
	return false
}

func hasEnglishPrefix(value, prefix string) bool {
	if value == prefix || strings.HasPrefix(value, prefix+" ") {
		return true
	}
	if !strings.HasPrefix(value, prefix) {
		return false
	}
	remainder := strings.TrimPrefix(value, prefix)
	character, _ := utf8.DecodeRuneInString(remainder)
	return isTrailingPunctuation(character)
}

func hasJapanesePrefix(value, prefix string, allowedSuffixes []string) bool {
	if value == prefix {
		return true
	}
	if !strings.HasPrefix(value, prefix) {
		return false
	}
	remainder := strings.TrimPrefix(value, prefix)
	for _, suffix := range allowedSuffixes {
		if strings.HasPrefix(remainder, suffix) {
			return true
		}
	}
	return false
}
