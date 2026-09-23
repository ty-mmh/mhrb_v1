package exportjsonlservice

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"runtime"
	"testing"

	"mahoroba.local/mahoroba/internal/blob"
	"mahoroba.local/mahoroba/internal/fssecure"
	"mahoroba.local/mahoroba/internal/hostlock"
	storesqlite "mahoroba.local/mahoroba/internal/store/sqlite"
)

func TestM7JSONLExportProductionPipelinePublishesDeterministicSingleFile(t *testing.T) {
	ctx := context.Background()
	source := filepath.Join(t.TempDir(), "source")
	sourceLock, err := hostlock.Acquire(source)
	if err != nil {
		skipJSONLWindowsSandbox(t, err)
		t.Fatal(err)
	}
	if err := sourceLock.Close(); err != nil {
		t.Fatal(err)
	}
	policy, err := fssecure.CurrentSecurityPolicy()
	if err != nil {
		t.Fatal(err)
	}
	sourceRoot, err := fssecure.OpenRoot(source, policy)
	if err != nil {
		skipJSONLWindowsSandbox(t, err)
		t.Fatal(err)
	}
	databaseHandle, err := sourceRoot.CreateRegular("mahoroba.db")
	if err != nil {
		_ = sourceRoot.Close()
		skipJSONLWindowsSandbox(t, err)
		t.Fatal(err)
	}
	if err := errors.Join(databaseHandle.Close(), sourceRoot.Close()); err != nil {
		t.Fatal(err)
	}
	database, err := storesqlite.Open(ctx, filepath.Join(source, "mahoroba.db"))
	if err != nil {
		t.Fatal(err)
	}
	if err := database.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := blob.NewFileStore(filepath.Join(source, "blobs")); err != nil {
		if runtime.GOOS == "windows" && os.Getenv("CI") == "" && errors.Is(err, os.ErrPermission) {
			// The Codex desktop capability SID intentionally makes the sandbox
			// DACL non-production-exact. Exercise the exact core/CLI tests here;
			// non-sandbox Windows Hosted runs this filesystem path without skip.
			t.Skip("desktop sandbox adds a capability SID to the protected ACL")
		}
		t.Fatal(err)
	}
	parent := filepath.Join(t.TempDir(), "artifacts")
	managedParent, err := fssecure.OpenOrCreateRoot(parent, policy)
	if err != nil {
		skipJSONLWindowsSandbox(t, err)
		t.Fatal(err)
	}
	if err := managedParent.Close(); err != nil {
		t.Fatal(err)
	}
	firstPath, secondPath := filepath.Join(parent, "first.jsonl"), filepath.Join(parent, "second.jsonl")
	request := func(output string) Request {
		return Request{SourceDataDir: source, DatabaseFilename: "mahoroba.db", Output: output}
	}
	first, err := Create(ctx, request(firstPath))
	if err != nil {
		if runtime.GOOS == "windows" && os.Getenv("CI") == "" && errors.Is(err, os.ErrPermission) {
			t.Skip("desktop sandbox adds a capability SID to the protected ACL")
		}
		t.Fatal(err)
	}
	second, err := Create(ctx, request(secondPath))
	if err != nil {
		t.Fatal(err)
	}
	firstBytes, err := os.ReadFile(firstPath)
	if err != nil {
		t.Fatal(err)
	}
	secondBytes, err := os.ReadFile(secondPath)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(firstBytes, secondBytes) || first.RecordCount != 1 || second.RecordCount != 1 ||
		first.ByteCount != int64(len(firstBytes)) || second.ByteCount != int64(len(secondBytes)) {
		t.Fatalf("determinism/results: first=%#v second=%#v equal=%v", first, second, bytes.Equal(firstBytes, secondBytes))
	}
	if _, err := Create(ctx, request(firstPath)); !errors.Is(err, ErrArtifactTargetExists) {
		t.Fatalf("existing target error = %v", err)
	}
	entries, err := os.ReadDir(parent)
	if err != nil {
		t.Fatal(err)
	}
	for _, entry := range entries {
		if filepath.Ext(entry.Name()) == ".jsonl" || filepath.Ext(entry.Name()) == ".lock" {
			continue
		}
		if bytes.Contains([]byte(entry.Name()), []byte("publish-pending")) || bytes.Contains([]byte(entry.Name()), []byte(".staging.")) {
			t.Fatalf("successful publication left recovery evidence %q", entry.Name())
		}
	}

	// Descriptor-backed offline readers cannot resolve SQLite WAL sidecars.
	// A non-empty sidecar must therefore fail closed before any output staging
	// or publication rather than exporting an incomplete main-file snapshot.
	sourceRoot, err = fssecure.OpenRoot(source, policy)
	if err != nil {
		t.Fatal(err)
	}
	wal, err := sourceRoot.OpenOrCreateRegular("mahoroba.db-wal")
	if err != nil {
		_ = sourceRoot.Close()
		t.Fatal(err)
	}
	if _, err := wal.File().Write([]byte("active-wal")); err != nil {
		t.Fatal(err)
	}
	if err := wal.Seal(); err != nil {
		t.Fatal(err)
	}
	if err := errors.Join(wal.Close(), sourceRoot.Close()); err != nil {
		t.Fatal(err)
	}
	hotOutput := filepath.Join(parent, "hot-wal.jsonl")
	if _, err := Create(ctx, request(hotOutput)); !errors.Is(err, ErrSourceUnavailable) {
		t.Fatalf("hot WAL export error=%v", err)
	}
	if _, err := os.Lstat(hotOutput); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("hot WAL export published output: %v", err)
	}
}

func skipJSONLWindowsSandbox(t *testing.T, err error) {
	t.Helper()
	if runtime.GOOS == "windows" && os.Getenv("CI") == "" && errors.Is(err, os.ErrPermission) {
		t.Skip("desktop sandbox adds a capability SID to the protected ACL")
	}
}
