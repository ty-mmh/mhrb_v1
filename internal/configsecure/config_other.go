//go:build !linux

package configsecure

import "errors"

const MaxBytes = 1 << 20

var ErrInvalid = errors.New("secure configuration unsupported platform")

func Read(string) ([]byte, error) { return nil, ErrInvalid }
