//go:build !linux

package secret

import (
	"errors"
	"testing"
)

func TestM7SecretFileIsUnsupportedOutsideLinux(t *testing.T) {
	if _, err := Load(ProviderAPIKey, "", `C:\secrets\provider`); !errors.Is(err, ErrFileUnsupported) {
		t.Fatalf("file secret did not fail closed: %v", err)
	}
}
