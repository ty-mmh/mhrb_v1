package sqlite

import (
	"context"
	"os"
	"path/filepath"
	"testing"
)

func TestOpenDiagnosticInspectionDoesNotCreateOrGateDatabase(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "mahoroba.db")
	if _, err := OpenDiagnosticInspection(ctx, path); err == nil {
		t.Fatal("absent database was opened")
	}
	if _, err := os.Lstat(path); !os.IsNotExist(err) {
		t.Fatalf("absent database was created: %v", err)
	}

	body := []byte("not a sqlite database\n")
	if err := os.WriteFile(path, body, 0o600); err != nil {
		t.Fatal(err)
	}
	inspection, err := OpenDiagnosticInspection(ctx, path)
	if err != nil {
		t.Fatalf("corrupt diagnostic open = %v", err)
	}
	var value int
	if err := inspection.QueryRowContext(ctx, `SELECT COUNT(*) FROM sqlite_schema`).Scan(&value); err == nil {
		t.Fatal("corrupt database query unexpectedly succeeded")
	}
	if err := inspection.Close(); err != nil {
		t.Fatal(err)
	}
	after, err := os.ReadFile(path)
	if err != nil || string(after) != string(body) {
		t.Fatalf("diagnostic open mutated corrupt DB: %q, %v", after, err)
	}
	for _, suffix := range []string{"-wal", "-shm", "-journal"} {
		if _, err := os.Lstat(path + suffix); !os.IsNotExist(err) {
			t.Fatalf("diagnostic open created %s: %v", suffix, err)
		}
	}
}

func TestOpenDiagnosticInspectionIsQueryOnly(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "mahoroba.db")
	options, err := normalizeOptions(DefaultOptions())
	if err != nil {
		t.Fatal(err)
	}
	writer, err := openWriter(ctx, path, options)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := writer.ExecContext(ctx, `CREATE TABLE schema_migrations(
		version_id INTEGER NOT NULL, is_applied INTEGER NOT NULL, tstamp TEXT NOT NULL)`); err != nil {
		t.Fatal(err)
	}
	if _, err := writer.ExecContext(ctx, `INSERT INTO schema_migrations VALUES(?,1,'now')`, DiagnosticExpectedSchemaVersion()); err != nil {
		t.Fatal(err)
	}
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}
	inspection, err := OpenDiagnosticInspection(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	defer inspection.Close()
	rows, err := inspection.QueryContext(ctx, `INSERT INTO schema_migrations(version_id,is_applied,tstamp) VALUES(999,1,0)`)
	if rows != nil {
		_ = rows.Close()
	}
	if err == nil {
		t.Fatal("query-only diagnostic surface accepted INSERT")
	}
	var version int64
	if err := inspection.QueryRowContext(ctx, `SELECT MAX(version_id) FROM schema_migrations WHERE is_applied=1`).Scan(&version); err != nil {
		t.Fatal(err)
	}
	if version != DiagnosticExpectedSchemaVersion() {
		t.Fatalf("schema version = %d", version)
	}
}
