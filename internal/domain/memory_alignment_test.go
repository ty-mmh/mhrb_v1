package domain

import (
	"strings"
	"testing"

	"mahoroba.local/mahoroba/internal/canonical"
)

func TestMemoryAlignmentObligationRoundTrip(t *testing.T) {
	ids := make([]canonical.ID, 6)
	for index, raw := range []string{
		"01ARZ3NDEKTSV4RRFFQ69G5FAV", "01ARZ3NDEKTSV4RRFFQ69G5FAW",
		"01ARZ3NDEKTSV4RRFFQ69G5FAX", "01ARZ3NDEKTSV4RRFFQ69G5FAY",
		"01ARZ3NDEKTSV4RRFFQ69G5FAZ", "01ARZ3NDEKTSV4RRFFQ69G5FB0",
	} {
		var err error
		ids[index], err = canonical.ParseID(raw)
		if err != nil {
			t.Fatal(err)
		}
	}
	key := MemoryAlignmentObligation(ids[0], ids[1], ids[2], ids[3])
	parsed, err := ParseMemoryAlignmentObligation(key)
	if err != nil {
		t.Fatal(err)
	}
	if parsed.DirectClaimID != ids[0] || parsed.MetaClaimID != ids[1] ||
		parsed.DirectEvidenceID != ids[2] || parsed.MetaEvidenceID != ids[3] || parsed.Replacement != nil {
		t.Fatalf("identity = %+v", parsed)
	}
	replacementKey := MemoryAlignmentReplacementObligation(ids[0], ids[1], ids[2], ids[3], ids[4], ids[5])
	replacement, err := ParseMemoryAlignmentObligation(replacementKey)
	if err != nil {
		t.Fatal(err)
	}
	if replacement.Replacement == nil || replacement.Replacement.OldClaimID != ids[4] ||
		replacement.Replacement.IntentRelationID != ids[5] {
		t.Fatalf("replacement identity = %+v", replacement)
	}
	for _, invalid := range []string{"", strings.Replace(key, "v1", "v2", 1), key + ":extra",
		replacementKey + ":extra", strings.Replace(replacementKey, ids[4].String(), ids[0].String(), 1)} {
		if _, err := ParseMemoryAlignmentObligation(invalid); err == nil {
			t.Fatalf("invalid key accepted: %q", invalid)
		}
	}
}
