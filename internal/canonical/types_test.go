package canonical

import (
	"bytes"
	"encoding/json"
	"errors"
	"testing"
	"time"
)

func TestLogicalTypesValidateAndEncodeDecimalStrings(t *testing.T) {
	seq, _ := NewSeq(42)
	ratio, _ := NewRatio(750_000)
	ordinal, _ := NewOrdinal(0)
	value := struct {
		Seq     Seq     `json:"seq"`
		Ratio   Ratio   `json:"ratio"`
		Ordinal Ordinal `json:"ordinal"`
	}{seq, ratio, ordinal}
	encoded, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	if got, want := string(encoded), `{"seq":"42","ratio":"750000","ordinal":"0"}`; got != want {
		t.Fatalf("encoded = %s, want %s", got, want)
	}

	if _, err := NewSeq(0); !errors.Is(err, ErrInvalidLogicalValue) {
		t.Fatalf("NewSeq(0) error = %v", err)
	}
	if _, err := NewRatio(FixedPointScale + 1); !errors.Is(err, ErrInvalidLogicalValue) {
		t.Fatalf("NewRatio overflow error = %v", err)
	}
	if _, err := NewWeight(-1); !errors.Is(err, ErrInvalidLogicalValue) {
		t.Fatalf("NewWeight(-1) error = %v", err)
	}
	if _, err := DurationFromTime(-time.Nanosecond); !errors.Is(err, ErrInvalidLogicalValue) {
		t.Fatalf("DurationFromTime(-1ns) error = %v", err)
	}
}

func TestLogicalIntegerJSONRoundTripAndStrictGrammar(t *testing.T) {
	type logicalValues struct {
		Instant Instant   `json:"instant"`
		Seq     Seq       `json:"seq"`
		Commit  CommitSeq `json:"commit"`
		Ratio   Ratio     `json:"ratio"`
		Weight  Weight    `json:"weight"`
	}
	original := logicalValues{
		Instant: Instant(-123),
		Seq:     Seq(7),
		Commit:  CommitSeq(9),
		Ratio:   Ratio(500_000),
		Weight:  Weight(1_250_000),
	}
	encoded, err := json.Marshal(original)
	if err != nil {
		t.Fatal(err)
	}
	var decoded logicalValues
	if err := json.Unmarshal(encoded, &decoded); err != nil {
		t.Fatal(err)
	}
	if decoded != original {
		t.Fatalf("round trip = %+v, want %+v", decoded, original)
	}

	for _, input := range []string{`0`, `"+1"`, `"01"`, `"-0"`, `" 1"`, `"1.0"`} {
		var seq Seq
		if err := json.Unmarshal([]byte(input), &seq); !errors.Is(err, ErrInvalidLogicalValue) {
			t.Errorf("Seq.UnmarshalJSON(%s) error = %v", input, err)
		}
	}
	var ratio Ratio
	if err := json.Unmarshal([]byte(`"1000001"`), &ratio); !errors.Is(err, ErrInvalidLogicalValue) {
		t.Fatalf("Ratio overflow error = %v", err)
	}
}

func TestInstantAndRawTextPreserveMeaning(t *testing.T) {
	timestamp := time.Unix(1_700_000_000, 123_456_000)
	instant := InstantFromTime(timestamp)
	if instant.UnixMicro() != 1_700_000_000_123_456 {
		t.Fatalf("UnixMicro = %d", instant.UnixMicro())
	}
	raw := RawText("  A\r\nＢ\n")
	if !bytes.Equal(raw.Bytes(), []byte("  A\r\nＢ\n")) {
		t.Fatalf("RawText was normalized: %q", raw.Bytes())
	}
	encoded, err := json.Marshal(raw)
	if err != nil {
		t.Fatal(err)
	}
	var decoded RawText
	if err := json.Unmarshal(encoded, &decoded); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(decoded.Bytes(), raw.Bytes()) {
		t.Fatalf("RawText JSON round trip changed bytes: %q", decoded.Bytes())
	}
	if err := json.Unmarshal([]byte(`"\ud800"`), &decoded); !errors.Is(err, ErrInvalidCanonicalJSON) {
		t.Fatalf("RawText unpaired surrogate error = %v", err)
	}
}

func TestTimezoneRequiresStableIANAName(t *testing.T) {
	for _, value := range []string{"UTC", "Asia/Tokyo", "America/New_York"} {
		if _, err := ParseTimezone(value); err != nil {
			t.Errorf("ParseTimezone(%q): %v", value, err)
		}
	}
	for _, value := range []string{"", "Local", " UTC", "Not/A_Zone"} {
		if _, err := ParseTimezone(value); !errors.Is(err, ErrInvalidTimezone) {
			t.Errorf("ParseTimezone(%q) error = %v", value, err)
		}
	}
}
