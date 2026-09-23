package memory

import "testing"

func TestDerivedClaimOutputSchemaIsClosedAndModelCannotChooseCanonicalIdentity(t *testing.T) {
	schema, err := DerivedClaimJSONSchema()
	if err != nil {
		t.Fatal(err)
	}
	for _, forbidden := range []string{"\"claim_id\"", "\"principal_id\"", "\"policy_id\"", "\"scope\"", "\"weight\"", "\"kind\":"} {
		if containsASCII(schema.String(), forbidden) {
			t.Fatalf("schema exposes forbidden model field %q: %s", forbidden, schema.String())
		}
	}
	valid := []byte(`{"statement":"A broader stable preference.","temporal_kind":"stable","version":"memory-derived-output-v1"}`)
	output, _, err := ParseDerivedClaimOutput(valid)
	if err != nil || output.TemporalKind != TemporalStable {
		t.Fatalf("ParseDerivedClaimOutput(valid) = %+v, %v", output, err)
	}
	if _, _, err := ParseDerivedClaimOutput([]byte(`{"kind":"direct","statement":"x","temporal_kind":"stable","version":"memory-derived-output-v1"}`)); err == nil {
		t.Fatal("model-selected kind was accepted")
	}
}
