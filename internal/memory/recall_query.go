package memory

import (
	"unicode"
	"unicode/utf8"

	"mahoroba.local/mahoroba/internal/canonical"
)

// RecallQueryV5 holds only transient, deterministic character bigrams. Neither
// the query nor this set is serialized; Recall's existing content reference is
// sufficient for target-bound Writer revalidation and content erasure.
type RecallQueryV5 map[string]struct{}

func NewRecallQueryV5(query string) RecallQueryV5 {
	return recallBigrams(query)
}

// Compatibility is the set Dice coefficient, rounded down to millionths.
// Empty or punctuation-only queries deliberately recall nothing. A one-rune
// text uses a singleton, so short Japanese queries also have defined behavior.
func (query RecallQueryV5) Compatibility(statement string) canonical.Ratio {
	other := recallBigrams(statement)
	if len(query) == 1 {
		for token := range query {
			if utf8.RuneCountInString(token) == 1 {
				// A one-character Japanese topic still matches a longer claim.
				other = make(RecallQueryV5)
				for _, r := range recallRunes(statement) {
					other[string(r)] = struct{}{}
				}
			}
		}
	}
	if len(query) == 0 || len(other) == 0 {
		return 0
	}
	common := int64(0)
	for token := range query {
		if _, present := other[token]; present {
			common++
		}
	}
	return canonical.Ratio(2 * common * canonical.FixedPointScale / int64(len(query)+len(other)))
}

func recallBigrams(text string) RecallQueryV5 {
	runes := recallRunes(text)
	result := make(RecallQueryV5)
	if len(runes) == 1 {
		result[string(runes)] = struct{}{}
	}
	for index := 1; index < len(runes); index++ {
		result[string(runes[index-1:index+1])] = struct{}{}
	}
	return result
}

func recallRunes(text string) []rune {
	runes := make([]rune, 0, len(text))
	for _, r := range text {
		if unicode.IsLetter(r) || unicode.IsDigit(r) {
			runes = append(runes, unicode.ToLower(r))
		}
	}
	return runes
}
