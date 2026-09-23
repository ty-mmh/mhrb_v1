//go:build linux

package configsecure

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
)

func TestM7OCIConfigRequiresExactOwnerModeRegularHandle(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config.toml")
	if err := os.WriteFile(path, []byte("timezone = \"UTC\"\n"), 0o400); err != nil {
		t.Fatal(err)
	}
	if _, err := Read(path); err != nil {
		t.Fatalf("owner 0400 config rejected: %v", err)
	}
	if err := os.Chmod(path, 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := Read(path); !errors.Is(err, ErrInvalid) {
		t.Fatalf("writable config accepted: %v", err)
	}
}

func TestM7OCIConfigRejectsSymlinkAndOversize(t *testing.T) {
	directory := t.TempDir()
	target := filepath.Join(directory, "target.toml")
	if err := os.WriteFile(target, []byte("x"), 0o400); err != nil {
		t.Fatal(err)
	}
	link := filepath.Join(directory, "link.toml")
	if err := os.Symlink(target, link); err != nil {
		t.Fatal(err)
	}
	if _, err := Read(link); !errors.Is(err, ErrInvalid) {
		t.Fatalf("symlink config accepted: %v", err)
	}
	large := filepath.Join(directory, "large.toml")
	file, err := os.OpenFile(large, os.O_CREATE|os.O_WRONLY, 0o400)
	if err != nil {
		t.Fatal(err)
	}
	if err := file.Truncate(MaxBytes + 1); err != nil {
		t.Fatal(err)
	}
	_ = file.Close()
	if _, err := Read(large); !errors.Is(err, ErrInvalid) {
		t.Fatalf("oversized config accepted: %v", err)
	}
}
