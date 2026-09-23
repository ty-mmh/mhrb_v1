package canonical

import (
	"bytes"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"
)

type fixedClock struct{ now time.Time }

func (clock *fixedClock) Now() time.Time { return clock.now }

func TestParseIDStrictCanonicalForm(t *testing.T) {
	const known = "01ARZ3NDEKTSV4RRFFQ69G5FAV"
	id, err := ParseID(known)
	if err != nil {
		t.Fatalf("ParseID: %v", err)
	}
	if got := id.String(); got != known {
		t.Fatalf("round trip = %q, want %q", got, known)
	}
	if got := id.TimeMillis(); got != 1469922850259 {
		t.Fatalf("timestamp = %d", got)
	}

	invalid := []string{
		strings.ToLower(known),
		"00000000000000000000000000",
		"81ARZ3NDEKTSV4RRFFQ69G5FAV",
		"01ARZ3NDEKTSV4RRFFQ69G5FAI",
		known[:25],
	}
	for _, value := range invalid {
		if _, err := ParseID(value); err == nil {
			t.Errorf("ParseID(%q) succeeded", value)
		}
	}
}

func TestIDGeneratorMonotonicWithClockRegression(t *testing.T) {
	clock := &fixedClock{now: time.UnixMilli(1_700_000_000_123)}
	gen, err := NewIDGenerator(clock, bytes.NewReader(bytes.Repeat([]byte{0x42}, 10)))
	if err != nil {
		t.Fatal(err)
	}
	first, err := gen.New()
	if err != nil {
		t.Fatal(err)
	}
	second, err := gen.New()
	if err != nil {
		t.Fatal(err)
	}
	clock.now = clock.now.Add(-time.Hour)
	third, err := gen.New()
	if err != nil {
		t.Fatal(err)
	}
	if !(first.String() < second.String() && second.String() < third.String()) {
		t.Fatalf("IDs are not monotonic: %s %s %s", first, second, third)
	}
	if first.TimeMillis() != second.TimeMillis() || second.TimeMillis() != third.TimeMillis() {
		t.Fatal("timestamp was not clamped during same/regressed clock")
	}
}

func TestIDGeneratorEntropyOverflow(t *testing.T) {
	clock := &fixedClock{now: time.UnixMilli(1)}
	gen, err := NewIDGenerator(clock, bytes.NewReader(bytes.Repeat([]byte{0xff}, 10)))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := gen.New(); err != nil {
		t.Fatal(err)
	}
	if _, err := gen.New(); !errors.Is(err, ErrULIDEntropyOverflow) {
		t.Fatalf("second New error = %v", err)
	}
}

func TestIDJSONRejectsZero(t *testing.T) {
	if _, err := json.Marshal(ID{}); !errors.Is(err, ErrZeroID) {
		t.Fatalf("Marshal zero ID error = %v", err)
	}
}
