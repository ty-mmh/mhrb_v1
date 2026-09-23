package canonical

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"reflect"
	"unicode/utf8"

	"github.com/gowebpki/jcs"
)

const CanonicalizationVersion = "mahoroba-jcs-v1"

// CanonicalJSON contains RFC 8785 bytes. The byte slice is private so an
// invalid or non-canonical representation cannot be constructed accidentally.
type CanonicalJSON struct {
	raw []byte
}

// CanonicalizeRFC8785 validates an arbitrary RFC 8785 JSON value and applies
// JCS. This low-level function permits JSON numbers for RFC test vectors and
// external schema validation. Canonical Mahoroba records should normally use
// MarshalCanonical, which rejects untyped numbers and maps logical integer
// wrappers to decimal strings.
func CanonicalizeRFC8785(input []byte) (CanonicalJSON, error) {
	if err := validateJSON(input, false); err != nil {
		return CanonicalJSON{}, err
	}
	transformed, err := jcs.Transform(input)
	if err != nil {
		return CanonicalJSON{}, fmt.Errorf("%w: JCS transform: %v", ErrInvalidCanonicalJSON, err)
	}
	if !utf8.Valid(transformed) {
		return CanonicalJSON{}, fmt.Errorf("%w: JCS output is not UTF-8", ErrInvalidCanonicalJSON)
	}
	return CanonicalJSON{raw: append([]byte(nil), transformed...)}, nil
}

// MarshalCanonical marshals a typed Mahoroba value and canonicalizes it.
// Mahoroba logical integer wrappers implement decimal-string JSON encoding.
// Any remaining JSON number is rejected to prevent an untyped integer or Score
// from entering a Canonical record.
func MarshalCanonical(value any) (CanonicalJSON, error) {
	if err := validateGoValue(reflect.ValueOf(value), make(map[visit]struct{})); err != nil {
		return CanonicalJSON{}, err
	}
	encoded, err := json.Marshal(value)
	if err != nil {
		return CanonicalJSON{}, fmt.Errorf("%w: marshal: %v", ErrInvalidCanonicalJSON, err)
	}
	if err := validateJSON(encoded, true); err != nil {
		return CanonicalJSON{}, err
	}
	transformed, err := jcs.Transform(encoded)
	if err != nil {
		return CanonicalJSON{}, fmt.Errorf("%w: JCS transform: %v", ErrInvalidCanonicalJSON, err)
	}
	return CanonicalJSON{raw: append([]byte(nil), transformed...)}, nil
}

var jsonMarshalerType = reflect.TypeOf((*json.Marshaler)(nil)).Elem()

type visit struct {
	typeName reflect.Type
	pointer  uintptr
}

func validateGoValue(value reflect.Value, seen map[visit]struct{}) error {
	if !value.IsValid() {
		return nil
	}
	if value.Kind() == reflect.Interface {
		if value.IsNil() {
			return nil
		}
		return validateGoValue(value.Elem(), seen)
	}
	if value.Kind() == reflect.Pointer {
		if value.IsNil() {
			return nil
		}
		key := visit{typeName: value.Type(), pointer: uintptr(value.UnsafePointer())}
		if _, exists := seen[key]; exists {
			return fmt.Errorf("%w: cyclic Go value", ErrInvalidCanonicalJSON)
		}
		seen[key] = struct{}{}
		defer delete(seen, key)
		if value.Type().Implements(jsonMarshalerType) {
			return nil
		}
		return validateGoValue(value.Elem(), seen)
	}
	if value.CanInterface() && value.Type().Implements(jsonMarshalerType) {
		return nil
	}

	switch value.Kind() {
	case reflect.String:
		if !utf8.ValidString(value.String()) {
			return fmt.Errorf("%w: Go string is not UTF-8", ErrInvalidCanonicalJSON)
		}
	case reflect.Map:
		if value.IsNil() {
			return nil
		}
		key := visit{typeName: value.Type(), pointer: uintptr(value.UnsafePointer())}
		if _, exists := seen[key]; exists {
			return fmt.Errorf("%w: cyclic Go map", ErrInvalidCanonicalJSON)
		}
		seen[key] = struct{}{}
		defer delete(seen, key)
		iterator := value.MapRange()
		for iterator.Next() {
			if err := validateGoValue(iterator.Key(), seen); err != nil {
				return err
			}
			if err := validateGoValue(iterator.Value(), seen); err != nil {
				return err
			}
		}
	case reflect.Slice:
		if value.Type().Elem().Kind() == reflect.Uint8 {
			return fmt.Errorf("%w: []byte uses base64 JSON; wrap binary data with canonical.Binary", ErrInvalidCanonicalJSON)
		}
		if value.IsNil() {
			return nil
		}
		key := visit{typeName: value.Type(), pointer: value.Pointer()}
		if _, exists := seen[key]; exists {
			return fmt.Errorf("%w: cyclic Go slice", ErrInvalidCanonicalJSON)
		}
		seen[key] = struct{}{}
		defer delete(seen, key)
		for i := 0; i < value.Len(); i++ {
			if err := validateGoValue(value.Index(i), seen); err != nil {
				return err
			}
		}
	case reflect.Array:
		for i := 0; i < value.Len(); i++ {
			if err := validateGoValue(value.Index(i), seen); err != nil {
				return err
			}
		}
	case reflect.Struct:
		for i := 0; i < value.NumField(); i++ {
			field := value.Type().Field(i)
			if field.PkgPath != "" { // unexported
				continue
			}
			if err := validateGoValue(value.Field(i), seen); err != nil {
				return err
			}
		}
	}
	return nil
}

// ParseCanonicalJSON accepts only an already-canonical byte representation.
func ParseCanonicalJSON(input []byte) (CanonicalJSON, error) {
	canonical, err := CanonicalizeRFC8785(input)
	if err != nil {
		return CanonicalJSON{}, err
	}
	if !bytes.Equal(input, canonical.raw) {
		return CanonicalJSON{}, fmt.Errorf("%w: input is valid JSON but is not canonical", ErrInvalidCanonicalJSON)
	}
	return canonical, nil
}

func (value CanonicalJSON) Bytes() []byte {
	return append([]byte(nil), value.raw...)
}

func (value CanonicalJSON) String() string { return string(value.raw) }
func (value CanonicalJSON) IsZero() bool   { return len(value.raw) == 0 }

func (value CanonicalJSON) MarshalJSON() ([]byte, error) {
	if value.IsZero() {
		return nil, fmt.Errorf("%w: empty CanonicalJSON", ErrInvalidCanonicalJSON)
	}
	if _, err := ParseCanonicalJSON(value.raw); err != nil {
		return nil, err
	}
	return value.Bytes(), nil
}

func (value *CanonicalJSON) UnmarshalJSON(input []byte) error {
	canonical, err := ParseCanonicalJSON(input)
	if err != nil {
		return err
	}
	*value = canonical
	return nil
}

func validateJSON(input []byte, rejectNumbers bool) error {
	if len(input) == 0 {
		return fmt.Errorf("%w: empty input", ErrInvalidCanonicalJSON)
	}
	if !utf8.Valid(input) {
		return fmt.Errorf("%w: input is not UTF-8", ErrInvalidCanonicalJSON)
	}
	if err := validateUnicodeEscapes(input); err != nil {
		return err
	}

	decoder := json.NewDecoder(bytes.NewReader(input))
	decoder.UseNumber()
	if err := consumeJSONValue(decoder, rejectNumbers); err != nil {
		return err
	}
	if _, err := decoder.Token(); !errors.Is(err, io.EOF) {
		if err == nil {
			return fmt.Errorf("%w: trailing JSON value", ErrInvalidCanonicalJSON)
		}
		return fmt.Errorf("%w: trailing data: %v", ErrInvalidCanonicalJSON, err)
	}
	return nil
}

// encoding/json replaces lone UTF-16 surrogate escapes with U+FFFD. RFC 8785
// instead requires invalid Unicode data to fail, so check escape pairing before
// handing the document to encoding/json or the JCS implementation.
func validateUnicodeEscapes(input []byte) error {
	inString := false
	for index := 0; index < len(input); index++ {
		switch input[index] {
		case '"':
			inString = !inString
		case '\\':
			if !inString {
				continue
			}
			index++
			if index >= len(input) {
				return fmt.Errorf("%w: truncated string escape", ErrInvalidCanonicalJSON)
			}
			if input[index] != 'u' {
				continue
			}
			codePoint, ok := decodeHexQuad(input, index+1)
			if !ok {
				return fmt.Errorf("%w: malformed Unicode escape", ErrInvalidCanonicalJSON)
			}
			index += 4
			switch {
			case codePoint >= 0xd800 && codePoint <= 0xdbff:
				if index+6 >= len(input) || input[index+1] != '\\' || input[index+2] != 'u' {
					return fmt.Errorf("%w: unpaired high surrogate", ErrInvalidCanonicalJSON)
				}
				low, valid := decodeHexQuad(input, index+3)
				if !valid || low < 0xdc00 || low > 0xdfff {
					return fmt.Errorf("%w: unpaired high surrogate", ErrInvalidCanonicalJSON)
				}
				index += 6
			case codePoint >= 0xdc00 && codePoint <= 0xdfff:
				return fmt.Errorf("%w: unpaired low surrogate", ErrInvalidCanonicalJSON)
			}
		}
	}
	return nil
}

func decodeHexQuad(input []byte, start int) (uint16, bool) {
	if start < 0 || start+4 > len(input) {
		return 0, false
	}
	var value uint16
	for index := start; index < start+4; index++ {
		digit := input[index]
		var decoded byte
		switch {
		case digit >= '0' && digit <= '9':
			decoded = digit - '0'
		case digit >= 'a' && digit <= 'f':
			decoded = digit - 'a' + 10
		case digit >= 'A' && digit <= 'F':
			decoded = digit - 'A' + 10
		default:
			return 0, false
		}
		value = value<<4 | uint16(decoded)
	}
	return value, true
}

func consumeJSONValue(decoder *json.Decoder, rejectNumbers bool) error {
	token, err := decoder.Token()
	if err != nil {
		return fmt.Errorf("%w: %v", ErrInvalidCanonicalJSON, err)
	}

	switch value := token.(type) {
	case json.Delim:
		switch value {
		case '{':
			seen := make(map[string]struct{})
			for decoder.More() {
				keyToken, err := decoder.Token()
				if err != nil {
					return fmt.Errorf("%w: object key: %v", ErrInvalidCanonicalJSON, err)
				}
				key, ok := keyToken.(string)
				if !ok {
					return fmt.Errorf("%w: non-string object key", ErrInvalidCanonicalJSON)
				}
				if _, duplicate := seen[key]; duplicate {
					return fmt.Errorf("%w: %q", ErrDuplicateJSONKey, key)
				}
				seen[key] = struct{}{}
				if err := consumeJSONValue(decoder, rejectNumbers); err != nil {
					return err
				}
			}
			end, err := decoder.Token()
			if err != nil || end != json.Delim('}') {
				return fmt.Errorf("%w: unterminated object", ErrInvalidCanonicalJSON)
			}
		case '[':
			for decoder.More() {
				if err := consumeJSONValue(decoder, rejectNumbers); err != nil {
					return err
				}
			}
			end, err := decoder.Token()
			if err != nil || end != json.Delim(']') {
				return fmt.Errorf("%w: unterminated array", ErrInvalidCanonicalJSON)
			}
		default:
			return fmt.Errorf("%w: unexpected delimiter %q", ErrInvalidCanonicalJSON, value)
		}
	case json.Number:
		if rejectNumbers {
			return fmt.Errorf("%w: %s", ErrJSONNumberForbidden, value)
		}
	case string, bool, nil:
		return nil
	default:
		return fmt.Errorf("%w: unsupported token %T", ErrInvalidCanonicalJSON, value)
	}
	return nil
}
