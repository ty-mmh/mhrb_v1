//go:build linux

package backup

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"golang.org/x/sys/unix"
	"mahoroba.local/mahoroba/internal/fssecure"
	storesqlite "mahoroba.local/mahoroba/internal/store/sqlite"
)

func TestM7BackupSealedDatabaseSnapshotRejectsInPlaceWriteABA(t *testing.T) {
	fixture := newBackupFixture(t)
	output := filepath.Join(fixture.parent, "sealed-database-snapshot")
	if _, err := Create(context.Background(), fixture.request(output, false)); err != nil {
		t.Fatal(err)
	}
	manifest, err := decodeManifest(mustReadFile(t, filepath.Join(output, "manifest.json")))
	if err != nil {
		t.Fatal(err)
	}
	policy, err := fssecure.CurrentSecurityPolicy()
	if err != nil {
		t.Fatal(err)
	}
	root, err := fssecure.OpenRootReadOnly(output, policy)
	if err != nil {
		t.Fatal(err)
	}
	defer root.Close()
	bound, err := openBoundBundleFile(root, DatabaseBundlePath)
	if err != nil {
		t.Fatal(err)
	}
	defer bound.Close()
	snapshot, owned, _, err := captureVerifiedDatabase(context.Background(), bound.handle, manifest.DatabaseFile)
	if err != nil {
		t.Fatal(err)
	}
	if !owned {
		t.Fatal("Linux database snapshot did not return owned anonymous authority")
	}
	defer snapshot.Close()
	if _, err := snapshot.WriteAt([]byte("evil"), 0); !errors.Is(err, unix.EPERM) {
		t.Fatalf("sealed database snapshot write = %v, want EPERM", err)
	}

	databasePath := filepath.Join(output, filepath.FromSlash(DatabaseBundlePath))
	writer, err := os.OpenFile(databasePath, os.O_WRONLY, 0)
	if err != nil {
		t.Fatalf("Linux fixture could not model an in-place writer: %v", err)
	}
	if _, err := writer.WriteAt([]byte("evil"), 0); err != nil {
		_ = writer.Close()
		t.Fatal(err)
	}
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}
	inspection, err := storesqlite.OpenImmutableDescriptorInspection(context.Background(), snapshot, filepath.Join(output, "missing-namespace-fallback.db"))
	if err != nil {
		t.Fatalf("sealed snapshot followed in-place source mutation: %v", err)
	}
	if err := inspection.Close(); err != nil {
		t.Fatal(err)
	}
	if source, err := storesqlite.OpenImmutableInspection(context.Background(), databasePath); err == nil {
		_ = source.Close()
		t.Fatal("corrupt in-place source unexpectedly passed SQLite inspection")
	}
}
