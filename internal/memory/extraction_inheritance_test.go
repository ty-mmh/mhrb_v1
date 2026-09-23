package memory

import (
	"bytes"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"unicode/utf8"

	"mahoroba.local/mahoroba/internal/canonical"
)

func TestExtractionJSONSchemaIsCanonicalAndClosed(t *testing.T) {
	if ExtractionOutputSchemaVersionV1 != "memory-extraction-output-schema-v1" {
		t.Fatalf("schema version = %q", ExtractionOutputSchemaVersionV1)
	}
	schema, err := ExtractionJSONSchema()
	if err != nil {
		t.Fatalf("ExtractionJSONSchema: %v", err)
	}
	parsed, err := canonical.ParseCanonicalJSON(schema.Bytes())
	if err != nil {
		t.Fatalf("schema is not canonical: %v", err)
	}
	if !bytes.Equal(parsed.Bytes(), schema.Bytes()) {
		t.Fatal("canonical parser changed schema bytes")
	}

	var root map[string]any
	if err := json.Unmarshal(schema.Bytes(), &root); err != nil {
		t.Fatalf("decode schema: %v", err)
	}
	if root["additionalProperties"] != false {
		t.Fatal("root schema is not closed")
	}
	properties := root["properties"].(map[string]any)
	claims := properties["claims"].(map[string]any)
	if claims["maxItems"] != float64(MaximumExtractionClaims) {
		t.Fatalf("claims maxItems = %v", claims["maxItems"])
	}
	item := claims["items"].(map[string]any)
	if item["additionalProperties"] != false || len(item["required"].([]any)) != 6 {
		t.Fatal("claim schema is not closed and fully required")
	}
	statement := item["properties"].(map[string]any)["statement"].(map[string]any)
	if _, present := statement["maxLength"]; present {
		t.Fatal("schema incorrectly represents UTF-8 byte limit as maxLength")
	}
	text := schema.String()
	for _, exactEnum := range []string{
		`"enum":["resident","source_actor"]`,
		`"enum":["stable","volatile","episodic"]`,
		`"enum":["stated","inferred"]`,
	} {
		if !strings.Contains(text, exactEnum) {
			t.Errorf("schema missing exact enum %s: %s", exactEnum, text)
		}
	}
}

func TestExtractionStrictParserPreservesRawTextAndReturnsJCS(t *testing.T) {
	source := "The resident likes green tea."
	input := []byte(`{
		"claims":[{"source_quote":"likes green tea","grade":"stated","temporal_kind":"stable","perspective":"source_actor","subject":"resident","statement":"  Likes\tGREEN\nTea.  "}],
		"version":"memory-extraction-output-v1"
	}`)
	output, encoded, err := ParseExtractionOutput(input, source)
	if err != nil {
		t.Fatalf("ParseExtractionOutput: %v", err)
	}
	if output.Claims[0].Statement != "  Likes\tGREEN\nTea.  " {
		t.Fatalf("raw statement changed: %q", output.Claims[0].Statement)
	}
	if got := string(encoded.Bytes()); strings.Contains(got, "\n") || !strings.HasPrefix(got, `{"claims":`) {
		t.Fatalf("output is not compact JCS: %q", got)
	}
	normalized, err := NormalizeStatementV1(" \u2003HELLO\tWorld\n ")
	if err != nil || normalized != "hello world" {
		t.Fatalf("NormalizeStatementV1 = %q, %v", normalized, err)
	}
	if normalized, err := NormalizeStatementV1("Ｃａｆｅ\u0301!"); err != nil || normalized != "ｃａｆｅ\u0301!" {
		t.Fatalf("normalization unexpectedly performs compatibility/canonical normalization: %q, %v", normalized, err)
	}
}

func TestExtractionSchemaAndQuoteRulesFailClosed(t *testing.T) {
	valid := `{"version":"memory-extraction-output-v1","claims":[{"statement":"likes tea","subject":"resident","perspective":"source_actor","temporal_kind":"stable","grade":"stated","source_quote":"likes tea"}]}`
	tests := map[string]string{
		"unknown output key":   strings.Replace(valid, `"version":`, `"unknown":0,"version":`, 1),
		"unknown claim key":    strings.Replace(valid, `"statement":`, `"unknown":0,"statement":`, 1),
		"missing claim key":    strings.Replace(valid, `,"source_quote":"likes tea"`, "", 1),
		"duplicate claim key":  strings.Replace(valid, `"statement":"likes tea"`, `"statement":"likes tea","statement":"likes tea"`, 1),
		"stated empty quote":   strings.Replace(valid, `"source_quote":"likes tea"`, `"source_quote":""`, 1),
		"stated inexact quote": strings.Replace(valid, `"source_quote":"likes tea"`, `"source_quote":"Likes tea"`, 1),
		"inferred with quote":  strings.Replace(valid, `"grade":"stated"`, `"grade":"inferred"`, 1),
		"unknown selector":     strings.Replace(valid, `"subject":"resident"`, `"subject":"everyone"`, 1),
		"unknown temporal":     strings.Replace(valid, `"temporal_kind":"stable"`, `"temporal_kind":"forever"`, 1),
		"unknown version":      strings.Replace(valid, ExtractionOutputVersionV1, "memory-extraction-output-v9", 1),
	}
	for name, input := range tests {
		t.Run(name, func(t *testing.T) {
			if _, _, err := ParseExtractionOutput([]byte(input), "likes tea"); !errors.Is(err, ErrInvalidExtraction) {
				t.Fatalf("error = %v, want ErrInvalidExtraction", err)
			}
		})
	}

	claims := make([]ExtractionClaim, MaximumExtractionClaims+1)
	for index := range claims {
		claims[index] = ExtractionClaim{
			Statement: strings.Repeat("x", index+1), Subject: SelectorResident,
			Perspective: SelectorSourceActor, TemporalKind: TemporalStable,
			Grade: GradeInferred, SourceQuote: "",
		}
	}
	raw, err := json.Marshal(ExtractionOutput{Version: ExtractionOutputVersionV1, Claims: claims})
	if err != nil {
		t.Fatalf("Marshal oversized output: %v", err)
	}
	if _, _, err := ParseExtractionOutput(raw, "source"); !errors.Is(err, ErrInvalidExtraction) {
		t.Fatalf("oversized batch error = %v, want ErrInvalidExtraction", err)
	}

	tooLong := strings.Repeat("界", MaximumStatementBytes/utf8.RuneLen('界')+1)
	if len([]byte(tooLong)) <= MaximumStatementBytes {
		t.Fatal("test statement is not over byte limit")
	}
	claim := ExtractionClaim{Statement: tooLong, Subject: SelectorResident, Perspective: SelectorSourceActor, TemporalKind: TemporalStable, Grade: GradeInferred}
	if err := ValidateExtractionClaim(claim, ""); err == nil {
		t.Fatal("oversized UTF-8 statement was accepted")
	}
}

func TestExtractionDerivedIdentityRejectsNormalizedBatchDuplicates(t *testing.T) {
	output := ExtractionOutput{Version: ExtractionOutputVersionV1, Claims: []ExtractionClaim{
		{Statement: "Hello\u2003WORLD", Subject: SelectorResident, Perspective: SelectorSourceActor, TemporalKind: TemporalStable, Grade: GradeInferred},
		{Statement: "  hello world ", Subject: SelectorResident, Perspective: SelectorSourceActor, TemporalKind: TemporalStable, Grade: GradeInferred},
	}}
	raw, err := json.Marshal(output)
	if err != nil {
		t.Fatalf("Marshal duplicate output: %v", err)
	}
	if _, _, err := ParseExtractionOutput(raw, "source"); !errors.Is(err, ErrInvalidExtraction) {
		t.Fatalf("duplicate identity error = %v, want ErrInvalidExtraction", err)
	}

	output.Claims[1].TemporalKind = TemporalVolatile
	raw, err = json.Marshal(output)
	if err != nil {
		t.Fatalf("Marshal distinct output: %v", err)
	}
	if _, _, err := ParseExtractionOutput(raw, "source"); err != nil {
		t.Fatalf("identity did not include temporal kind: %v", err)
	}

	left, err := DerivedClaimIdentity(output.Claims[0])
	if err != nil {
		t.Fatalf("first identity: %v", err)
	}
	output.Claims[1].TemporalKind = output.Claims[0].TemporalKind
	right, err := DerivedClaimIdentity(output.Claims[1])
	if err != nil {
		t.Fatalf("second identity: %v", err)
	}
	if left != right {
		t.Fatal("normalization-equivalent identity differs")
	}
}

func TestInheritanceBatchEnforcesRelationsFanoutAndEventDedupe(t *testing.T) {
	policy := DefaultPolicyV2()
	target, source := testID(t, 0), testID(t, 1)
	candidate := func(index int) InheritedEvidenceCandidate {
		return InheritedEvidenceCandidate{
			TargetClaimID: target, SourceClaimID: source,
			SourceEvidenceID: testID(t, index+2), SourceEventID: testID(t, index+5),
			Relation: RelationAbstracts,
		}
	}
	if err := ValidateInheritanceBatch(policy, []InheritedEvidenceCandidate{candidate(0), candidate(1)}); err != nil {
		t.Fatalf("valid inheritance batch: %v", err)
	}
	third := candidate(2)
	if err := ValidateInheritanceBatch(policy, []InheritedEvidenceCandidate{candidate(0), candidate(1), third}); !errors.Is(err, ErrInvalidEvidence) {
		t.Fatalf("source fanout error = %v, want ErrInvalidEvidence", err)
	}
	badRelation := candidate(0)
	badRelation.Relation = RelationSupersedes
	if err := ValidateInheritanceBatch(policy, []InheritedEvidenceCandidate{badRelation}); !errors.Is(err, ErrInvalidEvidence) {
		t.Fatalf("non-inheriting relation error = %v, want ErrInvalidEvidence", err)
	}
	duplicateEvent := candidate(1)
	duplicateEvent.SourceClaimID = testID(t, 9)
	duplicateEvent.SourceEventID = candidate(0).SourceEventID
	if err := ValidateInheritanceBatch(policy, []InheritedEvidenceCandidate{candidate(0), duplicateEvent}); !errors.Is(err, ErrInvalidEvidence) {
		t.Fatalf("underlying event dedupe error = %v, want ErrInvalidEvidence", err)
	}
}
