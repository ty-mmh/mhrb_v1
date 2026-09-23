package canonical

import (
	"bytes"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"
)

const FixedPointScale int64 = 1_000_000

// Instant is a Unix epoch timestamp expressed in microseconds.
type Instant int64

func InstantFromTime(t time.Time) Instant { return Instant(t.UnixMicro()) }
func (v Instant) UnixMicro() int64        { return int64(v) }
func (v Instant) Time() time.Time         { return time.UnixMicro(int64(v)).UTC() }
func (v Instant) String() string          { return strconv.FormatInt(int64(v), 10) }
func (v Instant) MarshalJSON() ([]byte, error) {
	return marshalLogicalInteger(int64(v), true)
}
func (v *Instant) UnmarshalJSON(input []byte) error {
	parsed, err := unmarshalLogicalInteger(input, "Instant", minInt64, maxInt64)
	if err != nil {
		return err
	}
	*v = Instant(parsed)
	return nil
}

// Timezone is an IANA timezone identifier. UTC is accepted as the canonical
// fixed-offset zone; Local is intentionally rejected because it is host
// dependent.
type Timezone string

func ParseTimezone(value string) (Timezone, error) {
	if value == "" || value == "Local" || strings.TrimSpace(value) != value {
		return "", fmt.Errorf("%w: %q", ErrInvalidTimezone, value)
	}
	if _, err := time.LoadLocation(value); err != nil {
		return "", fmt.Errorf("%w: %q: %v", ErrInvalidTimezone, value, err)
	}
	return Timezone(value), nil
}

func MustTimezone(value string) Timezone {
	tz, err := ParseTimezone(value)
	if err != nil {
		panic(err)
	}
	return tz
}

func (v Timezone) String() string { return string(v) }
func (v Timezone) Validate() error {
	_, err := ParseTimezone(string(v))
	return err
}
func (v Timezone) MarshalJSON() ([]byte, error) {
	if err := v.Validate(); err != nil {
		return nil, err
	}
	return json.Marshal(string(v))
}
func (v *Timezone) UnmarshalJSON(input []byte) error {
	if !utf8.Valid(input) {
		return fmt.Errorf("%w: timezone JSON is not UTF-8", ErrInvalidTimezone)
	}
	if err := validateUnicodeEscapes(input); err != nil {
		return fmt.Errorf("%w: %v", ErrInvalidTimezone, err)
	}
	var value string
	if err := json.Unmarshal(input, &value); err != nil {
		return fmt.Errorf("%w: %v", ErrInvalidTimezone, err)
	}
	parsed, err := ParseTimezone(value)
	if err != nil {
		return err
	}
	*v = parsed
	return nil
}

// Seq is the resident-local canonical event order. It is positive; gaps are
// permitted and its value must never be inferred from an ID timestamp.
type Seq int64

func NewSeq(value int64) (Seq, error) {
	if value <= 0 {
		return 0, logicalRangeError("Seq", value, "> 0")
	}
	return Seq(value), nil
}
func (v Seq) Int64() int64   { return int64(v) }
func (v Seq) String() string { return strconv.FormatInt(int64(v), 10) }
func (v Seq) Validate() error {
	_, err := NewSeq(int64(v))
	return err
}
func (v Seq) MarshalJSON() ([]byte, error) {
	return marshalPositive("Seq", int64(v))
}
func (v *Seq) UnmarshalJSON(input []byte) error {
	parsed, err := unmarshalLogicalInteger(input, "Seq", 1, maxInt64)
	if err != nil {
		return err
	}
	*v = Seq(parsed)
	return nil
}

// CommitSeq is the store-global Canonical Commit cursor.
type CommitSeq int64

func NewCommitSeq(value int64) (CommitSeq, error) {
	if value <= 0 {
		return 0, logicalRangeError("CommitSeq", value, "> 0")
	}
	return CommitSeq(value), nil
}
func (v CommitSeq) Int64() int64   { return int64(v) }
func (v CommitSeq) String() string { return strconv.FormatInt(int64(v), 10) }
func (v CommitSeq) Validate() error {
	_, err := NewCommitSeq(int64(v))
	return err
}
func (v CommitSeq) MarshalJSON() ([]byte, error) {
	return marshalPositive("CommitSeq", int64(v))
}
func (v *CommitSeq) UnmarshalJSON(input []byte) error {
	parsed, err := unmarshalLogicalInteger(input, "CommitSeq", 1, maxInt64)
	if err != nil {
		return err
	}
	*v = CommitSeq(parsed)
	return nil
}

type Ordinal int64

func NewOrdinal(value int64) (Ordinal, error) {
	if value < 0 {
		return 0, logicalRangeError("Ordinal", value, ">= 0")
	}
	return Ordinal(value), nil
}
func (v Ordinal) Int64() int64   { return int64(v) }
func (v Ordinal) String() string { return strconv.FormatInt(int64(v), 10) }
func (v Ordinal) Validate() error {
	_, err := NewOrdinal(int64(v))
	return err
}
func (v Ordinal) MarshalJSON() ([]byte, error) {
	return marshalNonNegative("Ordinal", int64(v))
}
func (v *Ordinal) UnmarshalJSON(input []byte) error {
	parsed, err := unmarshalLogicalInteger(input, "Ordinal", 0, maxInt64)
	if err != nil {
		return err
	}
	*v = Ordinal(parsed)
	return nil
}

type Count int64

func NewCount(value int64) (Count, error) {
	if value < 0 {
		return 0, logicalRangeError("Count", value, ">= 0")
	}
	return Count(value), nil
}
func (v Count) Int64() int64   { return int64(v) }
func (v Count) String() string { return strconv.FormatInt(int64(v), 10) }
func (v Count) Validate() error {
	_, err := NewCount(int64(v))
	return err
}
func (v Count) MarshalJSON() ([]byte, error) {
	return marshalNonNegative("Count", int64(v))
}
func (v *Count) UnmarshalJSON(input []byte) error {
	parsed, err := unmarshalLogicalInteger(input, "Count", 0, maxInt64)
	if err != nil {
		return err
	}
	*v = Count(parsed)
	return nil
}

type ByteSize int64

func NewByteSize(value int64) (ByteSize, error) {
	if value < 0 {
		return 0, logicalRangeError("ByteSize", value, ">= 0")
	}
	return ByteSize(value), nil
}
func (v ByteSize) Int64() int64   { return int64(v) }
func (v ByteSize) String() string { return strconv.FormatInt(int64(v), 10) }
func (v ByteSize) Validate() error {
	_, err := NewByteSize(int64(v))
	return err
}
func (v ByteSize) MarshalJSON() ([]byte, error) {
	return marshalNonNegative("ByteSize", int64(v))
}
func (v *ByteSize) UnmarshalJSON(input []byte) error {
	parsed, err := unmarshalLogicalInteger(input, "ByteSize", 0, maxInt64)
	if err != nil {
		return err
	}
	*v = ByteSize(parsed)
	return nil
}

type Duration int64

func NewDuration(microseconds int64) (Duration, error) {
	if microseconds < 0 {
		return 0, logicalRangeError("Duration", microseconds, ">= 0")
	}
	return Duration(microseconds), nil
}
func DurationFromTime(value time.Duration) (Duration, error) {
	if value < 0 {
		return 0, fmt.Errorf("%w: Duration=%s, want >= 0", ErrInvalidLogicalValue, value)
	}
	return NewDuration(value.Microseconds())
}
func (v Duration) Microseconds() int64 { return int64(v) }
func (v Duration) String() string      { return strconv.FormatInt(int64(v), 10) }
func (v Duration) Validate() error {
	_, err := NewDuration(int64(v))
	return err
}
func (v Duration) MarshalJSON() ([]byte, error) {
	return marshalNonNegative("Duration", int64(v))
}
func (v *Duration) UnmarshalJSON(input []byte) error {
	parsed, err := unmarshalLogicalInteger(input, "Duration", 0, maxInt64)
	if err != nil {
		return err
	}
	*v = Duration(parsed)
	return nil
}

type TokenCount int64

func NewTokenCount(value int64) (TokenCount, error) {
	if value < 0 {
		return 0, logicalRangeError("TokenCount", value, ">= 0")
	}
	return TokenCount(value), nil
}
func (v TokenCount) Int64() int64   { return int64(v) }
func (v TokenCount) String() string { return strconv.FormatInt(int64(v), 10) }
func (v TokenCount) Validate() error {
	_, err := NewTokenCount(int64(v))
	return err
}
func (v TokenCount) MarshalJSON() ([]byte, error) {
	return marshalNonNegative("TokenCount", int64(v))
}
func (v *TokenCount) UnmarshalJSON(input []byte) error {
	parsed, err := unmarshalLogicalInteger(input, "TokenCount", 0, maxInt64)
	if err != nil {
		return err
	}
	*v = TokenCount(parsed)
	return nil
}

// Ratio is a [0,1] value represented at one-millionth precision.
type Ratio int64

func NewRatio(value int64) (Ratio, error) {
	if value < 0 || value > FixedPointScale {
		return 0, logicalRangeError("Ratio", value, "0..1000000")
	}
	return Ratio(value), nil
}
func (v Ratio) Millionths() int64 { return int64(v) }
func (v Ratio) String() string    { return strconv.FormatInt(int64(v), 10) }
func (v Ratio) Validate() error {
	_, err := NewRatio(int64(v))
	return err
}
func (v Ratio) MarshalJSON() ([]byte, error) {
	if _, err := NewRatio(int64(v)); err != nil {
		return nil, err
	}
	return marshalLogicalInteger(int64(v), true)
}
func (v *Ratio) UnmarshalJSON(input []byte) error {
	parsed, err := unmarshalLogicalInteger(input, "Ratio", 0, FixedPointScale)
	if err != nil {
		return err
	}
	*v = Ratio(parsed)
	return nil
}

// Weight is a non-negative one-millionth fixed-point value. Values above one
// are allowed because evidence aggregation may intentionally use them.
type Weight int64

func NewWeight(value int64) (Weight, error) {
	if value < 0 {
		return 0, logicalRangeError("Weight", value, ">= 0")
	}
	return Weight(value), nil
}
func (v Weight) Millionths() int64 { return int64(v) }
func (v Weight) String() string    { return strconv.FormatInt(int64(v), 10) }
func (v Weight) Validate() error {
	_, err := NewWeight(int64(v))
	return err
}
func (v Weight) MarshalJSON() ([]byte, error) {
	return marshalNonNegative("Weight", int64(v))
}
func (v *Weight) UnmarshalJSON(input []byte) error {
	parsed, err := unmarshalLogicalInteger(input, "Weight", 0, maxInt64)
	if err != nil {
		return err
	}
	*v = Weight(parsed)
	return nil
}

type Money int64

func NewMoney(microUnits int64) (Money, error) {
	if microUnits < 0 {
		return 0, logicalRangeError("Money", microUnits, ">= 0")
	}
	return Money(microUnits), nil
}
func (v Money) MicroUnits() int64 { return int64(v) }
func (v Money) String() string    { return strconv.FormatInt(int64(v), 10) }
func (v Money) Validate() error {
	_, err := NewMoney(int64(v))
	return err
}
func (v Money) MarshalJSON() ([]byte, error) {
	return marshalNonNegative("Money", int64(v))
}
func (v *Money) UnmarshalJSON(input []byte) error {
	parsed, err := unmarshalLogicalInteger(input, "Money", 0, maxInt64)
	if err != nil {
		return err
	}
	*v = Money(parsed)
	return nil
}

// RawText preserves the original UTF-8 bytes. No normalization is performed.
type RawText string

func (v RawText) Bytes() []byte { return []byte(string(v)) }
func (v RawText) MarshalJSON() ([]byte, error) {
	if !utf8.ValidString(string(v)) {
		return nil, fmt.Errorf("%w: RawText is not UTF-8", ErrInvalidCanonicalJSON)
	}
	return json.Marshal(string(v))
}
func (v *RawText) UnmarshalJSON(input []byte) error {
	if !utf8.Valid(input) {
		return fmt.Errorf("%w: RawText JSON is not UTF-8", ErrInvalidCanonicalJSON)
	}
	if err := validateUnicodeEscapes(input); err != nil {
		return err
	}
	var value string
	if err := json.Unmarshal(input, &value); err != nil {
		return fmt.Errorf("%w: RawText: %v", ErrInvalidCanonicalJSON, err)
	}
	if !utf8.ValidString(value) {
		return fmt.Errorf("%w: RawText is not UTF-8", ErrInvalidCanonicalJSON)
	}
	*v = RawText(value)
	return nil
}

const (
	minInt64 = -1 << 63
	maxInt64 = 1<<63 - 1
)

func logicalRangeError(name string, value int64, want string) error {
	return fmt.Errorf("%w: %s=%d, want %s", ErrInvalidLogicalValue, name, value, want)
}

func marshalPositive(name string, value int64) ([]byte, error) {
	if value <= 0 {
		return nil, logicalRangeError(name, value, "> 0")
	}
	return marshalLogicalInteger(value, true)
}

func marshalNonNegative(name string, value int64) ([]byte, error) {
	if value < 0 {
		return nil, logicalRangeError(name, value, ">= 0")
	}
	return marshalLogicalInteger(value, true)
}

func marshalLogicalInteger(value int64, quoted bool) ([]byte, error) {
	decimal := strconv.FormatInt(value, 10)
	if !quoted {
		return []byte(decimal), nil
	}
	var out bytes.Buffer
	out.WriteByte('"')
	out.WriteString(decimal)
	out.WriteByte('"')
	return out.Bytes(), nil
}

func unmarshalLogicalInteger(input []byte, name string, minimum, maximum int64) (int64, error) {
	if !utf8.Valid(input) {
		return 0, fmt.Errorf("%w: %s JSON is not UTF-8", ErrInvalidLogicalValue, name)
	}
	var decimal string
	if err := json.Unmarshal(input, &decimal); err != nil {
		return 0, fmt.Errorf("%w: %s must be a decimal JSON string: %v", ErrInvalidLogicalValue, name, err)
	}
	if !isCanonicalDecimal(decimal) {
		return 0, fmt.Errorf("%w: %s=%q is not a canonical decimal string", ErrInvalidLogicalValue, name, decimal)
	}
	parsed, err := strconv.ParseInt(decimal, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("%w: %s=%q: %v", ErrInvalidLogicalValue, name, decimal, err)
	}
	if parsed < minimum || parsed > maximum {
		return 0, logicalRangeError(name, parsed, fmt.Sprintf("%d..%d", minimum, maximum))
	}
	return parsed, nil
}

func isCanonicalDecimal(value string) bool {
	if value == "0" {
		return true
	}
	if value == "" {
		return false
	}
	start := 0
	if value[0] == '-' {
		if len(value) == 1 {
			return false
		}
		start = 1
	}
	if value[start] < '1' || value[start] > '9' {
		return false
	}
	for i := start + 1; i < len(value); i++ {
		if value[i] < '0' || value[i] > '9' {
			return false
		}
	}
	return true
}
