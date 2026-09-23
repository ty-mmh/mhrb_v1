package memory

import "testing"

func TestAlignmentOutputStrictContract(t *testing.T) {
	parsed, encoded, err := ParseAlignmentOutput([]byte(`{"confidence":"800000","version":"memory-alignment-output-v1","aligned":true}`))
	if err != nil {
		t.Fatal(err)
	}
	if !parsed.Aligned || parsed.Confidence.Millionths() != 800_000 || encoded.String() != `{"aligned":true,"confidence":"800000","version":"memory-alignment-output-v1"}` {
		t.Fatalf("parsed alignment = %+v, %s", parsed, encoded.String())
	}
	for _, invalid := range []string{
		`{"version":"memory-alignment-output-v1","aligned":true,"confidence":"1000001"}`,
		`{"version":"memory-alignment-output-v1","aligned":true,"confidence":"800000","claim_id":"01ARZ3NDEKTSV4RRFFQ69G5FAV"}`,
		`{"version":"memory-alignment-output-v2","aligned":true,"confidence":"800000"}`,
		`{"version":"memory-alignment-output-v1","aligned":true}`,
	} {
		if _, _, err := ParseAlignmentOutput([]byte(invalid)); err == nil {
			t.Fatalf("invalid output accepted: %s", invalid)
		}
	}
	if _, err := AlignmentJSONSchema(); err != nil {
		t.Fatal(err)
	}
}
