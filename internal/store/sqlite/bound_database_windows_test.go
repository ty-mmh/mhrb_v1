//go:build windows

package sqlite

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
)

func TestM7ReadOnlyBoundDatabaseRequiresClosedWritableStoreWindows(t *testing.T) {
	dataDir, databasePath := newBoundDatabaseFixture(t)
	live, err := Open(context.Background(), databasePath)
	if err != nil {
		t.Fatal(err)
	}

	boundary, openErr := OpenReadOnlyBoundDatabase(
		context.Background(), dataDir, filepath.Base(databasePath),
	)
	if boundary != nil {
		_ = boundary.Close()
	}
	if !errors.Is(openErr, ErrUnsafeDatabaseBoundary) {
		_ = live.Close()
		t.Fatalf("offline read boundary with live writer error = %v, want ErrUnsafeDatabaseBoundary", openErr)
	}
	if err := live.Close(); err != nil {
		t.Fatal(err)
	}

	boundary, err = OpenReadOnlyBoundDatabase(
		context.Background(), dataDir, filepath.Base(databasePath),
	)
	if err != nil {
		t.Fatalf("offline read boundary after writer close: %v", err)
	}
	if err := boundary.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestM7StoreCloseReleasesMigratedSQLiteHandlesWindows(t *testing.T) {
	databasePath := filepath.Join(t.TempDir(), "canonical.db")
	live, err := Open(context.Background(), databasePath)
	if err != nil {
		t.Fatal(err)
	}
	if err := live.Close(); err != nil {
		t.Fatal(err)
	}

	// modernc SQLite opens the database without FILE_SHARE_DELETE. A rename
	// therefore proves that every native connection created by the complete
	// Goose migration and schema-gate path was synchronously released. Do not
	// weaken this to a GC/retry assertion: the offline read boundary must be
	// usable immediately after Store.Close returns.
	moved := databasePath + ".moved"
	if err := os.Rename(databasePath, moved); err != nil {
		t.Fatalf("rename migrated database after Store.Close: %v", err)
	}
	if err := os.Rename(moved, databasePath); err != nil {
		t.Fatalf("restore migrated database name: %v", err)
	}
}

func TestM7WritableBoundDatabaseCoexistsWithSQLiteAndDeniesDeleteWindows(t *testing.T) {
	dataDir, databasePath := newBoundDatabaseFixture(t)
	live, err := Open(context.Background(), databasePath)
	if err != nil {
		t.Fatal(err)
	}

	boundary, err := OpenWritableBoundDatabase(dataDir, filepath.Base(databasePath))
	if err != nil {
		_ = live.Close()
		t.Fatalf("writable identity guard did not share with SQLite: %v", err)
	}
	defer boundary.Close()

	connection, err := live.writer.Conn(context.Background())
	if err != nil {
		_ = live.Close()
		t.Fatal(err)
	}
	if _, err := connection.ExecContext(context.Background(), "BEGIN IMMEDIATE"); err != nil {
		_ = connection.Close()
		_ = live.Close()
		t.Fatalf("begin SQLite write with retained identity guard: %v", err)
	}
	if _, err := connection.ExecContext(context.Background(), "CREATE TABLE boundary_write_probe(value INTEGER)"); err != nil {
		_, _ = connection.ExecContext(context.Background(), "ROLLBACK")
		_ = connection.Close()
		_ = live.Close()
		t.Fatalf("write SQLite WAL with retained identity guard: %v", err)
	}
	if err := boundary.Verify(); err != nil {
		_, _ = connection.ExecContext(context.Background(), "ROLLBACK")
		_ = connection.Close()
		_ = live.Close()
		t.Fatalf("verify mutable database binding during SQLite write: %v", err)
	}
	if _, err := connection.ExecContext(context.Background(), "ROLLBACK"); err != nil {
		_ = connection.Close()
		_ = live.Close()
		t.Fatal(err)
	}
	if err := connection.Close(); err != nil {
		_ = live.Close()
		t.Fatal(err)
	}

	moved := databasePath + ".moved"
	if err := os.Rename(databasePath, moved); err == nil {
		_ = live.Close()
		_ = os.Rename(moved, databasePath)
		t.Fatal("database rename succeeded while writable SQLite session denied delete sharing")
	}
	if err := boundary.Verify(); err != nil {
		t.Fatalf("blocked rename invalidated writable boundary: %v", err)
	}
	if err := live.Close(); err != nil {
		t.Fatal(err)
	}
	if err := boundary.Verify(); err != nil {
		t.Fatalf("writable boundary changed while sealing Store: %v", err)
	}
}
