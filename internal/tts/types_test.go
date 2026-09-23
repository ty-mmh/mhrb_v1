package tts

import (
	"encoding/base64"
	"strings"
	"testing"

	"mahoroba.local/mahoroba/internal/canonical"
)

func testCanonicalID(t *testing.T, value string) canonical.ID {
	t.Helper()
	id, err := canonical.ParseID(value)
	if err != nil {
		t.Fatal(err)
	}
	return id
}

func validRequest(t *testing.T) Request {
	t.Helper()
	return Request{
		ResidentID:      testCanonicalID(t, "01ARZ3NDEKTSV4RRFFQ69G5FAV"),
		SourceEventID:   testCanonicalID(t, "01ARZ3NDEKTSV4RRFFQ69G5FAW"),
		GenerationRunID: testCanonicalID(t, "01ARZ3NDEKTSV4RRFFQ69G5FAX"),
		SourceEventType: "resident_message", Text: "hello",
	}
}

func validWAV() []byte {
	return []byte{'R', 'I', 'F', 'F', 4, 0, 0, 0, 'W', 'A', 'V', 'E'}
}

func TestRequestEnforcesCommittedEventTypesAndCodePointLimit(t *testing.T) {
	request := validRequest(t)
	request.Text = strings.Repeat("界", MaxInputCodePoints)
	if err := request.Validate(); err != nil {
		t.Fatalf("4096 code points rejected: %v", err)
	}
	request.Text += "界"
	if err := request.Validate(); ErrorClassOf(err) != ErrorInvalidRequest {
		t.Fatalf("4097 code point error = %v", err)
	}
	request = validRequest(t)
	request.SourceEventType = "outbound_initiative"
	if err := request.Validate(); err != nil {
		t.Fatalf("outbound initiative rejected: %v", err)
	}
	for _, eventType := range []string{"self_talk", "user_message", ""} {
		request.SourceEventType = eventType
		if err := request.Validate(); ErrorClassOf(err) != ErrorInvalidRequest {
			t.Fatalf("event type %q error = %v", eventType, err)
		}
	}
}

func TestAudioValidationAndBase64AreBounded(t *testing.T) {
	audio := Audio{MIMEType: MIMETypeWAV, Data: validWAV()}
	if err := audio.Validate(len(audio.Data)); err != nil {
		t.Fatal(err)
	}
	if err := (Audio{MIMEType: "audio/mpeg", Data: validWAV()}).Validate(1024); ErrorClassOf(err) != ErrorInvalidMedia {
		t.Fatalf("media error = %v", err)
	}
	if err := (Audio{MIMEType: MIMETypeWAV, Data: []byte("not a wave")}).Validate(1024); ErrorClassOf(err) != ErrorInvalidWAV {
		t.Fatalf("WAV error = %v", err)
	}
	encoded := base64.StdEncoding.EncodeToString(validWAV())
	decoded, err := DecodeBase64WAV(encoded, len(validWAV()))
	if err != nil || string(decoded.Data) != string(validWAV()) {
		t.Fatalf("decoded=%q err=%v", decoded.Data, err)
	}
	if _, err := DecodeBase64WAV(encoded, len(validWAV())-1); ErrorClassOf(err) != ErrorOutputTooLarge {
		t.Fatalf("base64 bound error = %v", err)
	}
}
