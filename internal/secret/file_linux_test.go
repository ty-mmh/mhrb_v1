//go:build linux

package secret

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
)

func TestM7LinuxSecretFileRequiresOwnerModeAndNormalizesOneNewline(t *testing.T) {
	path := filepath.Join(t.TempDir(), "provider_api_key")
	if err := os.WriteFile(path, []byte("token\r\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	value, err := Load(ProviderAPIKey, "", path)
	if err != nil || string(value) != "token" {
		t.Fatalf("secret file = %q, %v", value, err)
	}
	Zero(value)
	if err := os.Chmod(path, 0o640); err != nil {
		t.Fatal(err)
	}
	if _, err := Load(ProviderAPIKey, "", path); !errors.Is(err, ErrFileInvalid) {
		t.Fatalf("group-readable secret accepted: %v", err)
	}
}

func TestM7LinuxSecretFileRejectsSymlinkAndResidualNewline(t *testing.T) {
	directory := t.TempDir()
	target := filepath.Join(directory, "target")
	if err := os.WriteFile(target, []byte("one\ntwo\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := Load(TTSAPIKey, "", target); !errors.Is(err, ErrFileInvalid) {
		t.Fatalf("residual newline accepted: %v", err)
	}
	link := filepath.Join(directory, "link")
	if err := os.Symlink(target, link); err != nil {
		t.Skipf("symlink unavailable: %v", err)
	}
	if _, err := Load(TTSAPIKey, "", link); !errors.Is(err, ErrFileInvalid) {
		t.Fatalf("secret symlink accepted: %v", err)
	}
}
