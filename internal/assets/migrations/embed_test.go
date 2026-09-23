package migrations

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"testing"
)

func TestEmbeddedBaselineMatchesDocsManifestAndBytes(t *testing.T) {
	if err := Validate(); err != nil {
		t.Fatal(err)
	}
	root := docsBaselineRoot(t)
	manifestPath := filepath.Join(root, "docs", "database", "sqlite", "v0.1.3", "source-manifest.json")
	manifestBytes, err := os.ReadFile(manifestPath)
	if err != nil {
		t.Fatal(err)
	}
	var manifest struct {
		Version string `json:"version"`
		Files   []struct {
			Path   string `json:"path"`
			Bytes  int    `json:"bytes"`
			SHA256 string `json:"sha256"`
		} `json:"files"`
	}
	if err := json.Unmarshal(manifestBytes, &manifest); err != nil {
		t.Fatal(err)
	}
	if manifest.Version != "v0.1.3" {
		t.Fatalf("manifest version = %q, want v0.1.3", manifest.Version)
	}
	byPath := make(map[string]struct {
		Bytes int
		Hash  string
	})
	for _, file := range manifest.Files {
		byPath[filepath.ToSlash(file.Path)] = struct {
			Bytes int
			Hash  string
		}{Bytes: file.Bytes, Hash: file.SHA256}
	}

	descriptors := Descriptors()
	if len(descriptors) != 13 {
		t.Fatalf("migration descriptors = %d, want 13", len(descriptors))
	}
	for index, descriptor := range descriptors {
		wantSequence := index + 1
		if !strings.HasPrefix(descriptor.Name, formatSequence(wantSequence)+"_") {
			t.Errorf("migration %d name = %q", wantSequence, descriptor.Name)
		}
		manifestEntry, ok := byPath["docs/database/sqlite/v0.1.3/migrations/"+descriptor.Name]
		if !ok {
			t.Errorf("manifest missing %s", descriptor.Name)
			continue
		}
		if manifestEntry.Bytes != descriptor.Bytes || manifestEntry.Hash != descriptor.SHA256 {
			t.Errorf("manifest identity differs for %s", descriptor.Name)
		}
		embedded, err := Files.ReadFile(descriptor.Name)
		if err != nil {
			t.Fatal(err)
		}
		docPath := filepath.Join(root, "docs", "database", "sqlite", "v0.1.3", "migrations", descriptor.Name)
		docBytes, err := os.ReadFile(docPath)
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(embedded, docBytes) {
			t.Errorf("embedded migration %s is not byte-identical to docs", descriptor.Name)
		}
		digest := sha256.Sum256(docBytes)
		if got := hex.EncodeToString(digest[:]); got != descriptor.SHA256 {
			t.Errorf("docs migration %s sha256 = %s, want %s", descriptor.Name, got, descriptor.SHA256)
		}
	}

	names, err := filepath.Glob(filepath.Join(root, "docs", "database", "sqlite", "v0.1.3", "migrations", "*.sql"))
	if err != nil {
		t.Fatal(err)
	}
	sort.Strings(names)
	if len(names) != len(descriptors) {
		t.Fatalf("docs migration files = %d, want %d", len(names), len(descriptors))
	}
}

func TestDocsBundleMatchesSourceManifest(t *testing.T) {
	root := docsBaselineRoot(t)
	bundleRoot := filepath.Join(root, "docs", "database", "sqlite", "v0.1.3")
	manifestBytes, err := os.ReadFile(filepath.Join(bundleRoot, "source-manifest.json"))
	if err != nil {
		t.Fatal(err)
	}
	var manifest struct {
		Files []struct {
			Path   string `json:"path"`
			Bytes  int64  `json:"bytes"`
			SHA256 string `json:"sha256"`
		} `json:"files"`
	}
	if err := json.Unmarshal(manifestBytes, &manifest); err != nil {
		t.Fatal(err)
	}
	if len(manifest.Files) != 37 {
		t.Fatalf("manifest files = %d, want 37", len(manifest.Files))
	}

	seen := make(map[string]struct{}, len(manifest.Files))
	for _, entry := range manifest.Files {
		if _, duplicate := seen[entry.Path]; duplicate {
			t.Fatalf("manifest repeats path %q", entry.Path)
		}
		seen[entry.Path] = struct{}{}
		artifactPath, err := relocatedArtifactPath(root, bundleRoot, entry.Path)
		if err != nil {
			t.Fatalf("manifest path %q: %v", entry.Path, err)
		}
		body, err := os.ReadFile(artifactPath)
		if err != nil {
			t.Errorf("read manifest artifact %q at %s: %v", entry.Path, artifactPath, err)
			continue
		}
		digest := sha256.Sum256(body)
		if int64(len(body)) != entry.Bytes || hex.EncodeToString(digest[:]) != entry.SHA256 {
			t.Errorf("manifest identity mismatch for %q", entry.Path)
		}
	}
}

func relocatedArtifactPath(root, bundleRoot, manifestPath string) (string, error) {
	clean := filepath.Clean(filepath.FromSlash(manifestPath))
	if clean == "." || filepath.IsAbs(clean) || clean == ".." || strings.HasPrefix(clean, ".."+string(filepath.Separator)) {
		return "", fmt.Errorf("unsafe relative path")
	}
	return filepath.Join(root, clean), nil
}

func docsBaselineRoot(t *testing.T) string {
	t.Helper()
	_, file, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("runtime.Caller failed")
	}
	return filepath.Clean(filepath.Join(filepath.Dir(file), "..", "..", "..", "testdata", "docs-baseline"))
}

func formatSequence(sequence int) string {
	return fmt.Sprintf("%05d", sequence)
}
