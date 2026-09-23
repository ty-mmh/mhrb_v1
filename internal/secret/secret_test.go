package secret

import (
	"errors"
	"strings"
	"testing"
)

func TestM7SecretSourcesHaveNoPrecedenceAndErrorsAreSanitized(t *testing.T) {
	value, err := Load(ProviderAPIKey, "super-secret", "/must/not/appear")
	if value != nil || !errors.Is(err, ErrSourceConflict) {
		t.Fatalf("source conflict = %q, %v", value, err)
	}
	if strings.Contains(err.Error(), "super-secret") || strings.Contains(err.Error(), "/must/not/appear") {
		t.Fatalf("secret error leaked source: %v", err)
	}
	if _, err := Load(Name("attacker\nvalue"), "x", ""); err == nil || strings.Contains(err.Error(), "attacker") {
		t.Fatalf("unclosed secret name accepted or reflected: %v", err)
	}
}

func TestM7DirectSecretIsCopiedValidatedAndZeroable(t *testing.T) {
	value, err := Load(TTSAPIKey, "token", "")
	if err != nil || string(value) != "token" {
		t.Fatalf("direct secret = %q, %v", value, err)
	}
	Zero(value)
	for _, item := range value {
		if item != 0 {
			t.Fatalf("secret buffer was not cleared: %v", value)
		}
	}
	if _, err := Load(TTSAPIKey, "bad\nvalue", ""); !errors.Is(err, ErrDirectValueInvalid) {
		t.Fatalf("multiline direct secret accepted: %v", err)
	}
}
