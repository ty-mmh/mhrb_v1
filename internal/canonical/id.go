package canonical

import (
	"crypto/rand"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"sync"
	"time"

	"github.com/oklog/ulid/v2"
)

const (
	ulidTextLength = 26
	ulidTimeMax    = uint64(1<<48 - 1)
)

const crockford = "0123456789ABCDEFGHJKMNPQRSTVWXYZ"

var crockfordDecode = func() [256]byte {
	var table [256]byte
	for i := range table {
		table[i] = 0xff
	}
	for i := 0; i < len(crockford); i++ {
		table[crockford[i]] = byte(i)
	}
	return table
}()

// ID is a non-zero 128-bit ULID with a strict uppercase canonical text form.
type ID [16]byte

func ParseID(value string) (ID, error) {
	if len(value) != ulidTextLength {
		return ID{}, fmt.Errorf("%w: length=%d", ErrInvalidID, len(value))
	}
	if first := crockfordDecode[value[0]]; first == 0xff || first > 7 {
		return ID{}, fmt.Errorf("%w: invalid or overflowing first character", ErrInvalidID)
	}

	for i := 0; i < len(value); i++ {
		ch := value[i]
		if crockfordDecode[ch] == 0xff {
			return ID{}, fmt.Errorf("%w: character %q at offset %d", ErrInvalidID, ch, i)
		}
	}
	parsed, err := ulid.ParseStrict(value)
	if err != nil {
		return ID{}, fmt.Errorf("%w: %v", ErrInvalidID, err)
	}
	id := ID(parsed)
	if id.IsZero() {
		return ID{}, ErrZeroID
	}
	return id, nil
}

func IDFromBytes(value [16]byte) (ID, error) {
	id := ID(value)
	if id.IsZero() {
		return ID{}, ErrZeroID
	}
	return id, nil
}

func (id ID) IsZero() bool { return id == ID{} }
func (id ID) Validate() error {
	if id.IsZero() {
		return ErrZeroID
	}
	return nil
}
func (id ID) Bytes() [16]byte { return [16]byte(id) }

func (id ID) String() string {
	return ulid.ULID(id).String()
}

func (id ID) TimeMillis() uint64 {
	return ulid.ULID(id).Time()
}

func (id ID) MarshalText() ([]byte, error) {
	if err := id.Validate(); err != nil {
		return nil, err
	}
	return []byte(id.String()), nil
}

func (id *ID) UnmarshalText(text []byte) error {
	parsed, err := ParseID(string(text))
	if err != nil {
		return err
	}
	*id = parsed
	return nil
}

func (id ID) MarshalJSON() ([]byte, error) {
	if err := id.Validate(); err != nil {
		return nil, err
	}
	return json.Marshal(id.String())
}

func (id *ID) UnmarshalJSON(data []byte) error {
	var text string
	if err := json.Unmarshal(data, &text); err != nil {
		return fmt.Errorf("%w: %v", ErrInvalidID, err)
	}
	return id.UnmarshalText([]byte(text))
}

// Clock is injectable so ID generation and ledger time can be tested without
// coupling either value to an ID's semantic order.
type Clock interface {
	Now() time.Time
}

type SystemClock struct{}

func (SystemClock) Now() time.Time { return time.Now() }

// IDGenerator produces CSPRNG-backed monotonic ULIDs. When the wall clock
// stalls or moves backwards, the timestamp is clamped and the entropy field is
// incremented; the timestamp remains metadata only.
type IDGenerator struct {
	mu          sync.Mutex
	clock       Clock
	entropy     io.Reader
	initialized bool
	lastMillis  uint64
}

func NewIDGenerator(clock Clock, entropy io.Reader) (*IDGenerator, error) {
	if clock == nil {
		return nil, fmt.Errorf("%w: nil clock", ErrInvalidLogicalValue)
	}
	if entropy == nil {
		return nil, fmt.Errorf("%w: nil entropy", ErrInvalidLogicalValue)
	}
	// inc=1 gives deterministic +1 entropy for a repeated/clamped millisecond
	// and consumes fresh CSPRNG bytes only when the millisecond advances. The
	// oklog API treats inc=0 as MaxUint32, which would unexpectedly require
	// additional entropy on every same-millisecond ID.
	return &IDGenerator{clock: clock, entropy: ulid.Monotonic(entropy, 1)}, nil
}

func NewSecureIDGenerator() *IDGenerator {
	gen, err := NewIDGenerator(SystemClock{}, rand.Reader)
	if err != nil {
		panic(err)
	}
	return gen
}

func (g *IDGenerator) New() (ID, error) {
	g.mu.Lock()
	defer g.mu.Unlock()

	nowMillis := g.clock.Now().UnixMilli()
	if nowMillis < 0 || uint64(nowMillis) > ulidTimeMax {
		return ID{}, ErrULIDTimeOutOfRange
	}
	millis := uint64(nowMillis)

	if g.initialized && millis <= g.lastMillis {
		millis = g.lastMillis
	}
	generated, err := ulid.New(millis, g.entropy)
	if err != nil {
		if errors.Is(err, ulid.ErrMonotonicOverflow) {
			return ID{}, ErrULIDEntropyOverflow
		}
		return ID{}, fmt.Errorf("canonical: generate ULID: %w", err)
	}
	id := ID(generated)
	if id.IsZero() {
		return ID{}, ErrZeroID
	}

	g.initialized = true
	g.lastMillis = millis
	return id, nil
}
