package canonical

import (
	"bytes"
	"encoding/json"
	"errors"
	"testing"
)

func TestHashGoldenVectors(t *testing.T) {
	content := []byte("hello\n")
	if got, want := HashBlob(content).Hex(), "5891b5b522d5df086d0ff0b110fbd9d21bb4fc7163af34d08286a2e846f6be03"; got != want {
		t.Fatalf("blob hash = %s, want %s", got, want)
	}

	saltBytes := make([]byte, ContentCommitmentSaltLen)
	for i := range saltBytes {
		saltBytes[i] = byte(i)
	}
	salt, err := ContentSaltFromBytes(saltBytes)
	if err != nil {
		t.Fatal(err)
	}
	commitment, err := CommitContent("event_payload", salt, content)
	if err != nil {
		t.Fatal(err)
	}
	if got, want := commitment.Hex(), "3059cb750c9791d906a08dfd24f9c2c9fefdfacfc36899142b8e8f635ec3e623"; got != want {
		t.Fatalf("commitment = %s, want %s", got, want)
	}

	envelope, err := ParseCanonicalJSON([]byte(`{"seq":"1"}`))
	if err != nil {
		t.Fatal(err)
	}
	eventHash, err := HashEvent(envelope)
	if err != nil {
		t.Fatal(err)
	}
	if got, want := eventHash.Hex(), "b8a877ed4fd34497e5853bcb58ee52946f222d9aa928c43a1de15caa8cd7f665"; got != want {
		t.Fatalf("event hash = %s, want %s", got, want)
	}
}

func TestCommitmentVerificationAndStrictDigestHex(t *testing.T) {
	salt, err := NewContentSalt(bytes.NewReader(bytes.Repeat([]byte{1}, ContentCommitmentSaltLen)))
	if err != nil {
		t.Fatal(err)
	}
	expected, err := CommitContent("event_payload", salt, []byte("payload"))
	if err != nil {
		t.Fatal(err)
	}
	if err := VerifyContentCommitment("event_payload", salt, []byte("payload"), expected); err != nil {
		t.Fatal(err)
	}
	if err := VerifyContentCommitment("event_payload", salt, []byte("changed"), expected); !errors.Is(err, ErrLedgerViolation) {
		t.Fatalf("changed content error = %v", err)
	}
	if _, err := ParseDigestHex(expected.Hex()); err != nil {
		t.Fatal(err)
	}
	if _, err := ParseDigestHex("ABC" + expected.Hex()[3:]); !errors.Is(err, ErrInvalidDigest) {
		t.Fatalf("uppercase digest error = %v", err)
	}
}

func TestCanonicalBinaryAndSaltJSONUseLowercaseHex(t *testing.T) {
	binary := NewBinary([]byte{0x00, 0xab, 0xff})
	encoded, err := json.Marshal(binary)
	if err != nil {
		t.Fatal(err)
	}
	if string(encoded) != `"00abff"` {
		t.Fatalf("binary JSON = %s", encoded)
	}
	var decoded Binary
	if err := json.Unmarshal(encoded, &decoded); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(decoded, binary) {
		t.Fatalf("binary round trip = %x", decoded)
	}
	if err := json.Unmarshal([]byte(`"00AB"`), &decoded); err == nil {
		t.Fatal("uppercase binary was accepted")
	}

	salt, err := ContentSaltFromBytes(bytes.Repeat([]byte{0x0a}, ContentCommitmentSaltLen))
	if err != nil {
		t.Fatal(err)
	}
	encoded, err = json.Marshal(salt)
	if err != nil {
		t.Fatal(err)
	}
	var decodedSalt ContentSalt
	if err := json.Unmarshal(encoded, &decodedSalt); err != nil {
		t.Fatal(err)
	}
	if decodedSalt != salt {
		t.Fatal("commitment salt JSON did not round trip")
	}
}
