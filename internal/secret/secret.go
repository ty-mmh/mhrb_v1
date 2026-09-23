// Package secret implements the closed M7 provider/TTS secret boundary. It
// never returns a path or secret byte in an error.
package secret

import (
	"bytes"
	"errors"
	"fmt"
)

type Name string

const (
	ProviderAPIKey Name = "provider_api_key"
	TTSAPIKey      Name = "tts_api_key"
)

var (
	ErrSourceConflict     = errors.New("secret source conflict")
	ErrFileInvalid        = errors.New("secret file invalid")
	ErrFileUnsupported    = errors.New("secret file unsupported platform")
	ErrDirectValueInvalid = errors.New("secret direct value invalid")
)

// Load gives no precedence to direct and file sources. Direct values preserve
// the existing local environment contract; file values use the stricter
// Linux/OCI handle-bound loader.
func Load(name Name, directValue, filePath string) ([]byte, error) {
	if err := name.Validate(); err != nil {
		return nil, err
	}
	if directValue != "" && filePath != "" {
		return nil, namedError(ErrSourceConflict, name)
	}
	if filePath != "" {
		return loadFilePlatform(name, filePath)
	}
	if directValue == "" {
		return nil, nil
	}
	result := []byte(directValue)
	if bytes.ContainsAny(result, "\x00\r\n") {
		Zero(result)
		return nil, namedError(ErrDirectValueInvalid, name)
	}
	return result, nil
}

func (name Name) Validate() error {
	switch name {
	case ProviderAPIKey, TTSAPIKey:
		return nil
	default:
		return errors.New("secret name is not supported")
	}
}

// Zero best-effort clears a caller-owned secret buffer after provider setup.
func Zero(value []byte) {
	for index := range value {
		value[index] = 0
	}
}

func namedError(kind error, name Name) error {
	return fmt.Errorf("%w: %s", kind, name)
}
