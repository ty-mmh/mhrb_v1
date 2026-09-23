package canonical

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"strings"
	"unicode/utf8"
)

const (
	HashAlgorithm            = "sha256"
	ContentCommitmentDomain  = "mahoroba:content-commitment:v1"
	EventHashDomain          = "mahoroba:event-hash:v1"
	ContentCommitmentSaltLen = 32
)

// Digest is a raw SHA-256 digest. JSON and textual forms are strict lowercase
// hexadecimal while database adapters should use Bytes.
type Digest [sha256.Size]byte

// Binary is an explicit canonical binary value. Unlike []byte's default JSON
// base64 encoding, Binary is always represented as lowercase hexadecimal.
type Binary []byte

func NewBinary(value []byte) Binary { return append(Binary(nil), value...) }
func (value Binary) Bytes() []byte  { return append([]byte(nil), value...) }
func (value Binary) MarshalJSON() ([]byte, error) {
	return json.Marshal(hex.EncodeToString(value))
}
func (value *Binary) UnmarshalJSON(input []byte) error {
	var text string
	if err := json.Unmarshal(input, &text); err != nil {
		return fmt.Errorf("canonical: invalid binary JSON: %w", err)
	}
	if len(text)%2 != 0 || text != strings.ToLower(text) {
		return fmt.Errorf("canonical: binary must be even-length lowercase hexadecimal")
	}
	decoded, err := hex.DecodeString(text)
	if err != nil {
		return fmt.Errorf("canonical: invalid binary hexadecimal: %w", err)
	}
	*value = append((*value)[:0], decoded...)
	return nil
}

func ParseDigestHex(value string) (Digest, error) {
	if len(value) != sha256.Size*2 || value != strings.ToLower(value) {
		return Digest{}, fmt.Errorf("%w: expected 64 lowercase hexadecimal characters", ErrInvalidDigest)
	}
	decoded, err := hex.DecodeString(value)
	if err != nil {
		return Digest{}, fmt.Errorf("%w: %v", ErrInvalidDigest, err)
	}
	var digest Digest
	copy(digest[:], decoded)
	return digest, nil
}

func DigestFromBytes(value []byte) (Digest, error) {
	if len(value) != sha256.Size {
		return Digest{}, fmt.Errorf("%w: length=%d", ErrInvalidDigest, len(value))
	}
	var digest Digest
	copy(digest[:], value)
	return digest, nil
}

func (digest Digest) Bytes() []byte           { return append([]byte(nil), digest[:]...) }
func (digest Digest) Hex() string             { return hex.EncodeToString(digest[:]) }
func (digest Digest) String() string          { return digest.Hex() }
func (digest Digest) Equal(other Digest) bool { return digest == other }

func (digest Digest) MarshalText() ([]byte, error) { return []byte(digest.Hex()), nil }
func (digest *Digest) UnmarshalText(text []byte) error {
	parsed, err := ParseDigestHex(string(text))
	if err != nil {
		return err
	}
	*digest = parsed
	return nil
}
func (digest Digest) MarshalJSON() ([]byte, error) { return json.Marshal(digest.Hex()) }
func (digest *Digest) UnmarshalJSON(input []byte) error {
	var text string
	if err := json.Unmarshal(input, &text); err != nil {
		return fmt.Errorf("%w: %v", ErrInvalidDigest, err)
	}
	return digest.UnmarshalText([]byte(text))
}

type ContentSalt [ContentCommitmentSaltLen]byte

func NewContentSalt(entropy io.Reader) (ContentSalt, error) {
	if entropy == nil {
		return ContentSalt{}, fmt.Errorf("canonical: nil commitment entropy")
	}
	var salt ContentSalt
	if _, err := io.ReadFull(entropy, salt[:]); err != nil {
		return ContentSalt{}, fmt.Errorf("canonical: read commitment salt: %w", err)
	}
	return salt, nil
}

func ContentSaltFromBytes(value []byte) (ContentSalt, error) {
	if len(value) != ContentCommitmentSaltLen {
		return ContentSalt{}, fmt.Errorf("canonical: invalid commitment salt length %d", len(value))
	}
	var salt ContentSalt
	copy(salt[:], value)
	return salt, nil
}

func (salt ContentSalt) Bytes() []byte { return append([]byte(nil), salt[:]...) }
func (salt ContentSalt) MarshalJSON() ([]byte, error) {
	return json.Marshal(hex.EncodeToString(salt[:]))
}
func (salt *ContentSalt) UnmarshalJSON(input []byte) error {
	var text string
	if err := json.Unmarshal(input, &text); err != nil {
		return fmt.Errorf("canonical: invalid commitment salt JSON: %w", err)
	}
	if len(text) != ContentCommitmentSaltLen*2 || text != strings.ToLower(text) {
		return fmt.Errorf("canonical: commitment salt must be 64 lowercase hexadecimal characters")
	}
	decoded, err := hex.DecodeString(text)
	if err != nil {
		return fmt.Errorf("canonical: invalid commitment salt hexadecimal: %w", err)
	}
	copy(salt[:], decoded)
	return nil
}

// HashBlob hashes uncompressed logical content bytes without a domain prefix,
// as specified for resident-scoped deduplication.
func HashBlob(logicalContent []byte) Digest {
	return Digest(sha256.Sum256(logicalContent))
}

func HashBlobReader(reader io.Reader) (Digest, int64, error) {
	if reader == nil {
		return Digest{}, 0, fmt.Errorf("canonical: nil blob reader")
	}
	hasher := sha256.New()
	written, err := io.Copy(hasher, reader)
	if err != nil {
		return Digest{}, written, err
	}
	return digestFromBytesUnchecked(hasher.Sum(nil)), written, nil
}

func CommitContent(contentClass string, salt ContentSalt, logicalContent []byte) (Digest, error) {
	if contentClass == "" || strings.ContainsRune(contentClass, '\x00') || !utf8.ValidString(contentClass) {
		return Digest{}, fmt.Errorf("canonical: content class must be non-empty UTF-8 and contain no NUL")
	}
	hasher := sha256.New()
	_, _ = io.WriteString(hasher, ContentCommitmentDomain)
	_, _ = hasher.Write([]byte{0})
	_, _ = io.WriteString(hasher, contentClass)
	_, _ = hasher.Write([]byte{0})
	_, _ = hasher.Write(salt[:])
	_, _ = hasher.Write(logicalContent)
	return digestFromBytesUnchecked(hasher.Sum(nil)), nil
}

func VerifyContentCommitment(contentClass string, salt ContentSalt, logicalContent []byte, expected Digest) error {
	actual, err := CommitContent(contentClass, salt, logicalContent)
	if err != nil {
		return err
	}
	if actual != expected {
		return fmt.Errorf("%w: content commitment mismatch", ErrLedgerViolation)
	}
	return nil
}

func HashEvent(envelope CanonicalJSON) (Digest, error) {
	if envelope.IsZero() {
		return Digest{}, fmt.Errorf("%w: empty event envelope", ErrInvalidCanonicalJSON)
	}
	hasher := sha256.New()
	_, _ = io.WriteString(hasher, EventHashDomain)
	_, _ = hasher.Write([]byte{0})
	_, _ = hasher.Write(envelope.raw)
	return digestFromBytesUnchecked(hasher.Sum(nil)), nil
}

func digestFromBytesUnchecked(value []byte) Digest {
	var digest Digest
	copy(digest[:], value)
	return digest
}
