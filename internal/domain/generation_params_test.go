package domain

import (
	"testing"

	"mahoroba.local/mahoroba/internal/canonical"
	"mahoroba.local/mahoroba/internal/generation"
)

func TestParseGeneratorParamsRequiresExactClosedCanonicalEnvelope(t *testing.T) {
	valid, err := canonical.MarshalCanonical(GeneratorParams{
		Streaming: true, MaxOutputBytes: canonical.ByteSize(65536),
	})
	if err != nil {
		t.Fatal(err)
	}
	const legacyBytes = `{"max_output_bytes":"65536","streaming":true}`
	if valid.String() != legacyBytes {
		t.Fatalf("legacy params bytes=%s want=%s", valid.String(), legacyBytes)
	}
	params, encoded, err := ParseGeneratorParams(valid.Bytes())
	if err != nil {
		t.Fatal(err)
	}
	if !params.Streaming || params.MaxOutputBytes != 65536 || encoded.String() != valid.String() {
		t.Fatalf("parsed params=%+v encoded=%s", params, encoded.String())
	}

	for name, input := range map[string]string{
		"non-canonical":   `{ "max_output_bytes": "65536", "streaming": true }`,
		"unknown field":   `{"future":true,"max_output_bytes":"65536","streaming":true}`,
		"missing field":   `{"streaming":true}`,
		"duplicate field": `{"max_output_bytes":"65536","streaming":true,"streaming":true}`,
		"wrong type":      `{"max_output_bytes":65536,"streaming":true}`,
		"zero limit":      `{"max_output_bytes":"0","streaming":true}`,
	} {
		t.Run(name, func(t *testing.T) {
			if _, _, err := ParseGeneratorParams([]byte(input)); err == nil {
				t.Fatalf("ParseGeneratorParams(%s) unexpectedly succeeded", input)
			}
		})
	}
}

func TestParseGeneratorParamsV2PinsResolvedModeSchemaVersionAndHash(t *testing.T) {
	schema, err := canonical.CanonicalizeRFC8785([]byte(`{"type":"object","additionalProperties":false}`))
	if err != nil {
		t.Fatal(err)
	}
	hash := canonical.HashBlob(schema.Bytes())
	params, encoded, err := NewStructuredGeneratorParams(
		false, canonical.ByteSize(4096), generation.StructuredOutputJSONSchema,
		"memory-extraction-output-v1", hash,
	)
	if err != nil {
		t.Fatal(err)
	}
	parsed, canonicalParams, err := ParseGeneratorParams(encoded.Bytes())
	if err != nil {
		t.Fatal(err)
	}
	if !parsed.IsV2() || parsed.Streaming || parsed.MaxOutputBytes != 4096 ||
		parsed.StructuredOutputMode != generation.StructuredOutputJSONSchema ||
		parsed.SchemaVersion != "memory-extraction-output-v1" || parsed.SchemaHash == nil || *parsed.SchemaHash != hash {
		t.Fatalf("parsed v2 params = %+v", parsed)
	}
	if canonicalParams.String() != encoded.String() || params.SchemaHash == parsed.SchemaHash {
		t.Fatalf("canonical params=%s original=%s or hash pointer was aliased", canonicalParams, encoded)
	}
}

func TestParseGeneratorParamsDialogueV2IsVersionedAndUnstructured(t *testing.T) {
	params, encoded, err := NewUnstructuredGeneratorParams(true, canonical.ByteSize(4096))
	if err != nil {
		t.Fatal(err)
	}
	if encoded.String() != `{"max_output_bytes":"4096","streaming":true,"version":"generator-params-v2"}` {
		t.Fatalf("dialogue v2 bytes = %s", encoded.String())
	}
	parsed, _, err := ParseGeneratorParams(encoded.Bytes())
	if err != nil {
		t.Fatal(err)
	}
	if !parsed.IsV2() || parsed.IsStructured() || !parsed.Streaming || parsed.MaxOutputBytes != 4096 || params.IsLegacy() {
		t.Fatalf("dialogue params = %+v", parsed)
	}
}

func TestParseGeneratorParamsV2RejectsUnresolvedOrIncompleteContract(t *testing.T) {
	validHash := canonical.HashBlob([]byte("schema"))
	zeroHash := canonical.Digest{}
	for name, params := range map[string]GeneratorParams{
		"unsupported version": {
			Version: "future", Streaming: true, MaxOutputBytes: 1,
			StructuredOutputMode: generation.StructuredOutputPrompt, SchemaVersion: "v1", SchemaHash: &validHash,
		},
		"public auto not resolved": {
			Version: GeneratorParamsV2, Streaming: true, MaxOutputBytes: 1,
			StructuredOutputMode: generation.StructuredOutputMode("auto"), SchemaVersion: "v1", SchemaHash: &validHash,
		},
		"public required not resolved": {
			Version: GeneratorParamsV2, Streaming: true, MaxOutputBytes: 1,
			StructuredOutputMode: generation.StructuredOutputMode("required"), SchemaVersion: "v1", SchemaHash: &validHash,
		},
		"missing schema version": {
			Version: GeneratorParamsV2, Streaming: true, MaxOutputBytes: 1,
			StructuredOutputMode: generation.StructuredOutputPrompt, SchemaHash: &validHash,
		},
		"zero schema hash": {
			Version: GeneratorParamsV2, Streaming: true, MaxOutputBytes: 1,
			StructuredOutputMode: generation.StructuredOutputPrompt, SchemaVersion: "v1", SchemaHash: &zeroHash,
		},
		"legacy with structured fields": {
			Streaming: true, MaxOutputBytes: 1,
			StructuredOutputMode: generation.StructuredOutputPrompt, SchemaVersion: "v1", SchemaHash: &validHash,
		},
	} {
		t.Run(name, func(t *testing.T) {
			encoded, err := canonical.MarshalCanonical(params)
			if err != nil {
				t.Fatal(err)
			}
			if _, _, err := ParseGeneratorParams(encoded.Bytes()); err == nil {
				t.Fatalf("params %+v unexpectedly succeeded", params)
			}
		})
	}
}

func TestGeneratorParamsPurposeCompatibility(t *testing.T) {
	legacy := GeneratorParams{Streaming: true, MaxOutputBytes: 1024}
	hash := canonical.HashBlob([]byte("schema"))
	structured, _, err := NewStructuredGeneratorParams(true, 1024, generation.StructuredOutputPrompt, "schema-v1", hash)
	if err != nil {
		t.Fatal(err)
	}
	dialogueV2, _, err := NewUnstructuredGeneratorParams(true, 1024)
	if err != nil {
		t.Fatal(err)
	}
	if err := ValidateGeneratorParamsForPurpose("", legacy); err != nil {
		t.Fatalf("zero-value legacy dialogue purpose: %v", err)
	}
	if err := ValidateNewGeneratorParamsForPurpose(GenerationPurposeDialogue, legacy); err == nil {
		t.Fatal("new dialogue accepted legacy params")
	}
	if err := ValidateGeneratorParamsForPurpose(GenerationPurposeDialogue, structured); err == nil {
		t.Fatal("dialogue accepted v2 structured params")
	}
	if err := ValidateNewGeneratorParamsForPurpose(GenerationPurposeDialogue, dialogueV2); err != nil {
		t.Fatalf("new dialogue rejected unstructured v2: %v", err)
	}
	for _, purpose := range []GenerationPurpose{
		GenerationPurposeMemoryExtraction, GenerationPurposeMemoryAlignment,
		GenerationPurposeMemoryAbstraction, GenerationPurposeMemoryDifferentiation,
		GenerationPurposePersonaRevision,
	} {
		if err := ValidateGeneratorParamsForPurpose(purpose, structured); err != nil {
			t.Errorf("purpose %q rejected v2 params: %v", purpose, err)
		}
		if err := ValidateGeneratorParamsForPurpose(purpose, legacy); err == nil {
			t.Errorf("purpose %q accepted legacy params", purpose)
		}
		if err := ValidateGeneratorParamsForPurpose(purpose, dialogueV2); err == nil {
			t.Errorf("purpose %q accepted unstructured v2 params", purpose)
		}
	}
	if err := ValidateGeneratorParamsForPurpose("unknown", structured); err == nil {
		t.Fatal("unknown purpose was accepted")
	}
}

func TestGenerationPurposeWireValues(t *testing.T) {
	want := []GenerationPurpose{
		"dialogue", "memory_extraction", "memory_alignment",
		"memory_abstraction", "memory_differentiation", "persona_revision",
	}
	got := []GenerationPurpose{
		GenerationPurposeDialogue, GenerationPurposeMemoryExtraction, GenerationPurposeMemoryAlignment,
		GenerationPurposeMemoryAbstraction, GenerationPurposeMemoryDifferentiation, GenerationPurposePersonaRevision,
	}
	for index := range want {
		if got[index] != want[index] {
			t.Fatalf("purpose[%d]=%q want=%q", index, got[index], want[index])
		}
	}
}

func TestGenerationSessionPolicyContract(t *testing.T) {
	policyID, err := canonical.ParseID("01ARZ3NDEKTSV4RRFFQ69G5FAV")
	if err != nil {
		t.Fatal(err)
	}
	if err := validateGenerationSessionPolicy(GenerationPurposeDialogue, &policyID); err != nil {
		t.Fatalf("dialogue rejected a valid sessionization policy: %v", err)
	}
	if err := validateGenerationSessionPolicy("", nil); err == nil {
		t.Fatal("legacy dialogue accepted a missing sessionization policy")
	}
	if err := validateGenerationSessionPolicy(GenerationPurposeMemoryExtraction, nil); err != nil {
		t.Fatalf("non-dialogue purpose required a sessionization policy: %v", err)
	}
	zero := canonical.ID{}
	if err := validateGenerationSessionPolicy(GenerationPurposeMemoryAlignment, &zero); err == nil {
		t.Fatal("non-dialogue purpose accepted an invalid optional sessionization policy")
	}
	if err := validateGenerationSessionPolicy("unknown", nil); err == nil {
		t.Fatal("unknown purpose bypassed the sessionization policy contract")
	}
}
