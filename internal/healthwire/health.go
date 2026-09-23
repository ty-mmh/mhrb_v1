// Package healthwire owns the exact loopback HTTP readiness representation.
//
// The representation is deliberately smaller than diagnostics: it exposes one
// closed readiness reason, a server-side observation time, and no identifiers
// or implementation errors.
package healthwire

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"regexp"

	"mahoroba.local/mahoroba/internal/readiness"
)

const FormatVersion = "mahoroba-health-v1"

var decimalPattern = regexp.MustCompile(`^(0|[1-9][0-9]*)$`)

// Response fields are declared in JCS lexical-key order. All possible string
// values are closed ASCII tokens, so encoding/json produces the exact JCS form.
type Response struct {
	CheckedAtUnixMicros string               `json:"checked_at_unix_micros"`
	FormatVersion       string               `json:"format_version"`
	Ready               bool                 `json:"ready"`
	ReasonCode          readiness.ReasonCode `json:"reason_code"`
}

func New(result readiness.Result, checkedAtUnixMicros string) (Response, error) {
	if len(result.ReasonCodes) == 0 {
		return Response{}, errors.New("health wire: readiness result has no reason")
	}
	response := Response{
		CheckedAtUnixMicros: checkedAtUnixMicros,
		FormatVersion:       FormatVersion,
		Ready:               result.Ready,
		ReasonCode:          result.ReasonCodes[0],
	}
	if err := response.Validate(); err != nil {
		return Response{}, err
	}
	return response, nil
}

func (response Response) Validate() error {
	if response.FormatVersion != FormatVersion {
		return errors.New("health wire: unsupported format version")
	}
	if !decimalPattern.MatchString(response.CheckedAtUnixMicros) {
		return errors.New("health wire: checked time is not a canonical decimal string")
	}
	if response.Ready {
		if response.ReasonCode != readiness.ReasonReady {
			return errors.New("health wire: ready response requires reason=ready")
		}
		return nil
	}
	if response.ReasonCode == readiness.ReasonReady || !closedNotReadyReason(response.ReasonCode) {
		return errors.New("health wire: not-ready response has an unknown reason")
	}
	return nil
}

func closedNotReadyReason(reason readiness.ReasonCode) bool {
	for _, candidate := range readiness.NotReadyReasonOrder() {
		if reason == candidate {
			return true
		}
	}
	return false
}

// MarshalExact returns one JCS object followed by exactly one LF.
func MarshalExact(response Response) ([]byte, error) {
	if err := response.Validate(); err != nil {
		return nil, err
	}
	encoded, err := json.Marshal(response)
	if err != nil {
		return nil, fmt.Errorf("health wire: marshal: %w", err)
	}
	return append(encoded, '\n'), nil
}

// ParseExact accepts only the byte representation emitted by MarshalExact.
// The byte comparison also rejects duplicate/unknown fields, alternate key
// order, insignificant whitespace, non-canonical escaping, and extra values.
func ParseExact(data []byte) (Response, error) {
	var response Response
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&response); err != nil {
		return Response{}, fmt.Errorf("health wire: decode: %w", err)
	}
	if err := response.Validate(); err != nil {
		return Response{}, err
	}
	exact, err := MarshalExact(response)
	if err != nil {
		return Response{}, err
	}
	if !bytes.Equal(data, exact) {
		return Response{}, errors.New("health wire: response is not the exact canonical representation")
	}
	return response, nil
}
