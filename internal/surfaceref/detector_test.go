package surfaceref

import (
	"errors"
	"reflect"
	"testing"
)

func TestDetectSurfaceReferenceV1PositivePrefixes(t *testing.T) {
	tests := []struct {
		name  string
		input string
		want  []Marker
	}{
		{name: "Japanese continuation", input: "続けてください。", want: []Marker{ContinuationRequest}},
		{name: "Japanese prior continuation", input: "前回の続きを再開しよう！", want: []Marker{ContinuationRequest, PriorContextReference}},
		{name: "Japanese prior", input: "先ほどの話について", want: []Marker{PriorContextReference}},
		{name: "Japanese demonstrative", input: "その件を進めよう", want: []Marker{DemonstrativeReference}},
		{name: "Japanese standalone demonstrative", input: "それをお願い", want: []Marker{DemonstrativeReference}},
		{name: "English continuation", input: "\tRESUME\u2003the work!!!", want: []Marker{ContinuationRequest}},
		{name: "English combined", input: "Pick up\nwhere we left off.", want: []Marker{ContinuationRequest, PriorContextReference}},
		{name: "English prior", input: "Our previous conversation about tea", want: []Marker{PriorContextReference}},
		{name: "English demonstrative", input: "That Plan: continue it", want: []Marker{DemonstrativeReference}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got, err := Detect([]byte(test.input))
			if err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(got, test.want) {
				t.Fatalf("Detect(%q) = %v, want %v", test.input, got, test.want)
			}
		})
	}
}

func TestDetectSurfaceReferenceV1ConservativeNegatives(t *testing.T) {
	for _, input := range []string{
		"これ", "この計画", "this", "it", "that", "それぞれ確認する",
		"それでも進める", "それから出発する",
		"『前回の続き』という題名", "本文の途中で continue と書く", "please continue",
		"ええと、前回の続き", "event:01ARZ3NDEKTSV4RRFFQ69G5FAV", "go on vacation",
		"continuation is a noun", "resumeable text", "その件名を入力する",
	} {
		t.Run(input, func(t *testing.T) {
			got, err := Detect([]byte(input))
			if err != nil {
				t.Fatal(err)
			}
			if len(got) != 0 {
				t.Fatalf("Detect(%q) = %v, want no markers", input, got)
			}
		})
	}
}

func TestDetectSurfaceReferenceV1RejectsInvalidUTF8(t *testing.T) {
	if _, err := Detect([]byte{0xff, 'x'}); !errors.Is(err, ErrInvalidUTF8) {
		t.Fatalf("invalid UTF-8 error = %v", err)
	}
}

func TestSurfaceReferenceMarkersCanonicalJSON(t *testing.T) {
	markers, err := Detect([]byte("前回の続きを that topic"))
	if err != nil {
		t.Fatal(err)
	}
	encoded, err := CanonicalJSON(markers)
	if err != nil {
		t.Fatal(err)
	}
	if got, want := encoded.String(), `["continuation_request","prior_context_reference"]`; got != want {
		t.Fatalf("markers JSON = %s, want %s", got, want)
	}
	empty, err := CanonicalJSON(nil)
	if err != nil {
		t.Fatal(err)
	}
	if empty.String() != "[]" {
		t.Fatalf("empty markers JSON = %s", empty.String())
	}
	if _, err := CanonicalJSON([]Marker{PriorContextReference, ContinuationRequest}); err == nil {
		t.Fatal("non-canonical marker order unexpectedly accepted")
	}
}
