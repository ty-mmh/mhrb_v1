package canonical

import (
	"encoding/json"
	"errors"
	"math"
	"testing"
)

func TestCanonicalizeRFC8785OrdersAndCompacts(t *testing.T) {
	canonical, err := CanonicalizeRFC8785([]byte(" { \"b\" : 2, \"a\" : 1 } "))
	if err != nil {
		t.Fatal(err)
	}
	if got, want := canonical.String(), `{"a":1,"b":2}`; got != want {
		t.Fatalf("canonical = %s, want %s", got, want)
	}
}

func TestMarshalCanonicalMapsLogicalIntegers(t *testing.T) {
	seq, _ := NewSeq(42)
	weight, _ := NewWeight(750_000)
	canonical, err := MarshalCanonical(map[string]any{
		"weight": weight,
		"seq":    seq,
	})
	if err != nil {
		t.Fatal(err)
	}
	if got, want := canonical.String(), `{"seq":"42","weight":"750000"}`; got != want {
		t.Fatalf("canonical = %s, want %s", got, want)
	}
	if _, err := MarshalCanonical(map[string]any{"seq": 42}); !errors.Is(err, ErrJSONNumberForbidden) {
		t.Fatalf("raw integer error = %v", err)
	}
	if _, err := MarshalCanonical(map[string]any{"score": math.NaN()}); !errors.Is(err, ErrInvalidCanonicalJSON) {
		t.Fatalf("NaN error = %v", err)
	}
	binary, err := MarshalCanonical(map[string]any{"bytes": NewBinary([]byte{0x00, 0xab, 0xff})})
	if err != nil {
		t.Fatal(err)
	}
	if got, want := binary.String(), `{"bytes":"00abff"}`; got != want {
		t.Fatalf("binary = %s, want %s", got, want)
	}
	if _, err := MarshalCanonical(map[string]any{"bytes": []byte{1}}); !errors.Is(err, ErrInvalidCanonicalJSON) {
		t.Fatalf("raw []byte error = %v", err)
	}
	if _, err := MarshalCanonical(map[string]any{"text": string([]byte{0xff})}); !errors.Is(err, ErrInvalidCanonicalJSON) {
		t.Fatalf("invalid Go UTF-8 error = %v", err)
	}
}

func TestCanonicalJSONRejectsAmbiguousOrInvalidInput(t *testing.T) {
	tests := []struct {
		name  string
		input []byte
		want  error
	}{
		{"duplicate", []byte(`{"a":1,"a":2}`), ErrDuplicateJSONKey},
		{"escaped duplicate", []byte(`{"a":1,"\u0061":2}`), ErrDuplicateJSONKey},
		{"invalid utf8", []byte{'{', '"', 'x', '"', ':', '"', 0xff, '"', '}'}, ErrInvalidCanonicalJSON},
		{"unpaired high surrogate", []byte(`{"x":"\ud800"}`), ErrInvalidCanonicalJSON},
		{"unpaired low surrogate", []byte(`{"x":"\udc00"}`), ErrInvalidCanonicalJSON},
		{"nonfinite", []byte(`{"x":NaN}`), ErrInvalidCanonicalJSON},
		{"number overflow", []byte(`{"x":1e999}`), ErrInvalidCanonicalJSON},
		{"trailing", []byte(`{} {}`), ErrInvalidCanonicalJSON},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if _, err := CanonicalizeRFC8785(test.input); !errors.Is(err, test.want) {
				t.Fatalf("error = %v, want %v", err, test.want)
			}
		})
	}
}

func TestCanonicalJSONAcceptsPairedSurrogate(t *testing.T) {
	value, err := CanonicalizeRFC8785([]byte(`{"x":"\ud83d\ude00"}`))
	if err != nil {
		t.Fatal(err)
	}
	if value.String() != `{"x":"😀"}` {
		t.Fatalf("paired surrogate canonicalized to %q", value.String())
	}
}

func TestParseCanonicalJSONRequiresExactBytesAndCopies(t *testing.T) {
	if _, err := ParseCanonicalJSON([]byte(`{ "a": 1 }`)); !errors.Is(err, ErrInvalidCanonicalJSON) {
		t.Fatalf("noncanonical parse error = %v", err)
	}
	var decoded CanonicalJSON
	if err := json.Unmarshal([]byte(`{ "a": 1 }`), &decoded); !errors.Is(err, ErrInvalidCanonicalJSON) {
		t.Fatalf("noncanonical UnmarshalJSON error = %v", err)
	}
	input := []byte(`{"a":1}`)
	value, err := ParseCanonicalJSON(input)
	if err != nil {
		t.Fatal(err)
	}
	input[2] = 'z'
	copyBytes := value.Bytes()
	copyBytes[2] = 'z'
	if value.String() != `{"a":1}` {
		t.Fatalf("CanonicalJSON backing bytes escaped: %s", value.String())
	}
}

func TestMarshalCanonicalRejectsCyclicGoValues(t *testing.T) {
	value := map[string]any{}
	value["self"] = value
	if _, err := MarshalCanonical(value); !errors.Is(err, ErrInvalidCanonicalJSON) {
		t.Fatalf("cyclic map error = %v", err)
	}
}
