package memory

import (
	"errors"
	"testing"
)

func TestPersonaRevisionOutputIsStrictCanonicalJSON(t *testing.T) {
	valid := []byte(`{"contradiction":false,"persona":"steady and curious"}`)
	output, encoded, err := ParsePersonaRevisionOutput(valid)
	if err != nil {
		t.Fatalf("ParsePersonaRevisionOutput(valid): %v", err)
	}
	if output.Persona != "steady and curious" || output.Contradiction || string(encoded.Bytes()) != string(valid) {
		t.Fatalf("output = %+v encoded=%s", output, encoded.String())
	}
	for _, invalid := range [][]byte{
		[]byte(`{"persona":"x","contradiction":false,"extra":1}`),
		[]byte(`{"persona":"x"}`),
		[]byte(`{"contradiction":false,"persona":""}`),
	} {
		if _, _, err := ParsePersonaRevisionOutput(invalid); !errors.Is(err, ErrInvalidPersona) {
			t.Fatalf("ParsePersonaRevisionOutput(%s) error = %v, want ErrInvalidPersona", invalid, err)
		}
	}
}

func TestPersonaEditMetricsUseUTF8BytesAndOldSizeRatio(t *testing.T) {
	metrics, err := MeasurePersonaEdit("ab日本\nline", "ac日本\nline")
	if err != nil {
		t.Fatal(err)
	}
	if metrics.ChangedBytes.Int64() != 1 || metrics.ChangedLines.Int64() != 1 || metrics.TotalBytes.Int64() != 13 {
		t.Fatalf("metrics = %+v", metrics)
	}
	if metrics.ChangedRatio.Millionths() != 76_923 {
		t.Fatalf("ratio = %d, want 76923", metrics.ChangedRatio.Millionths())
	}

	metrics, err = MeasurePersonaEdit("a", "abcdefgh")
	if err != nil {
		t.Fatal(err)
	}
	if metrics.ChangedRatio.Millionths() != 1_000_000 {
		t.Fatalf("capped ratio = %d", metrics.ChangedRatio.Millionths())
	}
}

func TestPersonaRevisionSchemaExcludesPrinciples(t *testing.T) {
	schema, err := PersonaRevisionJSONSchema()
	if err != nil {
		t.Fatal(err)
	}
	if got := schema.String(); got == "" || containsASCII(got, "principles") {
		t.Fatalf("schema = %s", got)
	}
}

func containsASCII(value, needle string) bool {
	for index := 0; index+len(needle) <= len(value); index++ {
		if value[index:index+len(needle)] == needle {
			return true
		}
	}
	return false
}
