package domain

import (
	"bytes"
	"testing"

	"mahoroba.local/mahoroba/internal/canonical"
	"mahoroba.local/mahoroba/internal/memory"
)

func TestM5I84MemoryDecisionCanonicalJSONUsesQuantizedIntegers(t *testing.T) {
	head, err := canonical.NewCommitSeq(42)
	if err != nil {
		t.Fatal(err)
	}
	compatibility, err := canonical.NewRatio(875_000)
	if err != nil {
		t.Fatal(err)
	}
	query, constraints, err := NewRecallDecisionJSON(head, memory.DefaultPolicyV2(), compatibility)
	if err != nil {
		t.Fatal(err)
	}
	wantQuery := `{"candidate_limit":"64","context_compatibility":"875000","projection_head":"42","scoring_version":"memory-recall-v1"}`
	wantConstraints := `{"byte_budget":"8192","prompt_included_limit":"4","selected_limit":"8","view_scope":"resident_ui"}`
	if query.String() != wantQuery {
		t.Fatalf("query conditions = %s, want %s", query.String(), wantQuery)
	}
	if constraints.String() != wantConstraints {
		t.Fatalf("context constraints = %s, want %s", constraints.String(), wantConstraints)
	}
	for name, encoded := range map[string][]byte{
		"query": query.Bytes(), "constraints": constraints.Bytes(),
	} {
		if bytes.Contains(encoded, []byte(`"score"`)) {
			t.Fatalf("%s contains a forbidden score: %s", name, encoded)
		}
		if _, err := canonical.ParseCanonicalJSON(encoded); err != nil {
			t.Fatalf("%s is not exact Canonical JSON: %v", name, err)
		}
	}
}

func TestCOV3DialogueRecallExclusionVocabularyIsContextVersioned(t *testing.T) {
	makeRecall := func(reason memory.RecallExclusionReason) DialogueRecall {
		value := validPrepareDialogue(t)
		recall := *value.Recall
		claimID := mustParseID(t, "01ARZ3NDEKTSV4RRFFQ69G5FE0")
		recall.Usages = []RecallUsage{
			{ID: mustParseID(t, "01ARZ3NDEKTSV4RRFFQ69G5FE1"), ClaimID: claimID, Type: memory.UsageCandidate, Ordinal: 0},
			{ID: mustParseID(t, "01ARZ3NDEKTSV4RRFFQ69G5FE2"), ClaimID: claimID, Type: memory.UsageSelected, Ordinal: 0, ExclusionReason: reason},
		}
		return recall
	}

	legacy := makeRecall(memory.ExclusionProvenanceDuplicateV1)
	if err := legacy.Validate(); err != nil {
		t.Fatalf("legacy validation: %v", err)
	}
	if err := legacy.ValidateForContext(DialogueContextPolicyVersionV2); err == nil {
		t.Fatal("context-v2 accepted the context-v1 exclusion reason")
	}

	current := makeRecall(memory.ExclusionProvenanceDuplicateV2)
	if err := current.ValidateForContext(DialogueContextPolicyVersionV2); err != nil {
		t.Fatalf("context-v2 validation: %v", err)
	}
	if err := current.ValidateForContext(DialogueContextPolicyVersionV3); err != nil {
		t.Fatalf("context-v3 validation: %v", err)
	}
	if err := current.Validate(); err == nil {
		t.Fatal("legacy validation accepted the context-v2 exclusion reason")
	}
}
