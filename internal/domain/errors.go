package domain

import "errors"

// ErrInvalidEventReference identifies client-supplied explicit event metadata
// that cannot be used for the selected resident and current ingress snapshot.
// Callers may map it to a client error without exposing Canonical state details.
var ErrInvalidEventReference = errors.New("domain: invalid explicit event reference")
