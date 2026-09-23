// Package tts provides non-persistent, post-commit voice synthesis.
//
// Audio is deliberately ephemeral: adapters and dispatchers in this package
// never write Canonical state and a failure can never change a committed text
// event.
package tts

import (
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"unicode/utf8"

	"mahoroba.local/mahoroba/internal/canonical"
)

const (
	MaxInputCodePoints = 4096
	HardMaxAudioBytes  = 8 << 20
	MIMETypeWAV        = "audio/wav"
)

type ErrorClass string

const (
	ErrorInvalidRequest  ErrorClass = "invalid_request"
	ErrorTransport       ErrorClass = "transport"
	ErrorTimeout         ErrorClass = "timeout"
	ErrorHTTP            ErrorClass = "http"
	ErrorInvalidMedia    ErrorClass = "invalid_media_type"
	ErrorOutputTooLarge  ErrorClass = "output_too_large"
	ErrorInvalidWAV      ErrorClass = "invalid_wav"
	ErrorInvalidResponse ErrorClass = "invalid_response"
	ErrorDelivery        ErrorClass = "delivery"
)

// Error intentionally contains no provider response body, request text, URL,
// or secret. It is safe to classify and log without exposing resident data.
type Error struct {
	Class      ErrorClass
	StatusCode int
}

func (err *Error) Error() string {
	if err == nil {
		return "tts: unknown error"
	}
	if err.StatusCode != 0 {
		return fmt.Sprintf("tts: %s (HTTP %d)", err.Class, err.StatusCode)
	}
	return "tts: " + string(err.Class)
}

func ErrorClassOf(err error) ErrorClass {
	if err == nil {
		return ""
	}
	var classified *Error
	if errors.As(err, &classified) {
		return classified.Class
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return ErrorTimeout
	}
	return ErrorTransport
}

type Request struct {
	ResidentID      canonical.ID
	SourceEventID   canonical.ID
	GenerationRunID canonical.ID
	SourceEventType string
	Text            string
}

func (request Request) Validate() error {
	if err := request.ResidentID.Validate(); err != nil {
		return &Error{Class: ErrorInvalidRequest}
	}
	if err := request.SourceEventID.Validate(); err != nil {
		return &Error{Class: ErrorInvalidRequest}
	}
	if err := request.GenerationRunID.Validate(); err != nil {
		return &Error{Class: ErrorInvalidRequest}
	}
	if request.SourceEventType != "resident_message" && request.SourceEventType != "outbound_initiative" {
		return &Error{Class: ErrorInvalidRequest}
	}
	if request.Text == "" || !utf8.ValidString(request.Text) || utf8.RuneCountInString(request.Text) > MaxInputCodePoints {
		return &Error{Class: ErrorInvalidRequest}
	}
	return nil
}

type Audio struct {
	MIMEType string
	Data     []byte
}

func (audio Audio) Validate(maximum int) error {
	if maximum < 1 || maximum > HardMaxAudioBytes {
		return &Error{Class: ErrorInvalidRequest}
	}
	if audio.MIMEType != MIMETypeWAV {
		return &Error{Class: ErrorInvalidMedia}
	}
	if len(audio.Data) > maximum {
		return &Error{Class: ErrorOutputTooLarge}
	}
	if len(audio.Data) < 12 || string(audio.Data[:4]) != "RIFF" || string(audio.Data[8:12]) != "WAVE" {
		return &Error{Class: ErrorInvalidWAV}
	}
	return nil
}

type Delivery struct {
	ResidentID      canonical.ID
	SourceEventID   canonical.ID
	GenerationRunID canonical.ID
	Audio           Audio
}

func (delivery Delivery) Validate(maximum int) error {
	if err := delivery.ResidentID.Validate(); err != nil {
		return &Error{Class: ErrorInvalidRequest}
	}
	if err := delivery.SourceEventID.Validate(); err != nil {
		return &Error{Class: ErrorInvalidRequest}
	}
	if err := delivery.GenerationRunID.Validate(); err != nil {
		return &Error{Class: ErrorInvalidRequest}
	}
	return delivery.Audio.Validate(maximum)
}

func DecodeBase64WAV(encoded string, maximum int) (Audio, error) {
	if encoded == "" || maximum < 1 || maximum > HardMaxAudioBytes {
		return Audio{}, &Error{Class: ErrorInvalidRequest}
	}
	if base64.StdEncoding.DecodedLen(len(encoded)) > maximum {
		return Audio{}, &Error{Class: ErrorOutputTooLarge}
	}
	decoded, err := base64.StdEncoding.DecodeString(encoded)
	if err != nil {
		return Audio{}, &Error{Class: ErrorInvalidResponse}
	}
	audio := Audio{MIMEType: MIMETypeWAV, Data: decoded}
	if err := audio.Validate(maximum); err != nil {
		return Audio{}, err
	}
	return audio, nil
}

type Adapter interface {
	Synthesize(context.Context, Request) (Audio, error)
}

type Demand interface {
	HasAudioSubscribers() bool
}

type Sink interface {
	PublishAudio(Delivery) error
}
