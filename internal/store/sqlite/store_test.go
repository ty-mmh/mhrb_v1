package sqlite

import (
	"context"
	"database/sql"
	"io/fs"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"testing"
	"testing/fstest"

	"mahoroba.local/mahoroba/internal/assets/migrations"
)

func TestBuildDSNUsesValidWindowsFileURI(t *testing.T) {
	if runtime.GOOS != "windows" {
		t.Skip("Windows drive-letter URI regression")
	}
	dsn := buildDSN(`C:\Toy Box\mahoroba.db`, false, DefaultOptions())
	if !strings.HasPrefix(dsn, "file:///C:/Toy%20Box/mahoroba.db?") {
		t.Fatalf("invalid Windows SQLite URI: %s", dsn)
	}
}

func TestOpenAppliesBaselineAndConnectionProfiles(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "mahoroba.db")
	options := DefaultOptions()
	options.ReaderConnections = 2
	store, err := OpenWithOptions(ctx, path, options)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	report := store.SchemaReport()
	if report.SchemaVersion != 13 || report.ApplicationTables != 39 || report.StrictTables != 39 ||
		report.NamedIndexes != 59 || report.Triggers != 82 || report.WithoutRowIDTables != 5 ||
		report.ResidentScopeTriggers != 21 || report.QuickCheck != "ok" || report.ForeignKeyViolations != 0 {
		t.Fatalf("unexpected schema report: %+v", report)
	}
	if store.writer.Stats().MaxOpenConnections != 1 {
		t.Fatalf("writer MaxOpenConnections = %d, want 1", store.writer.Stats().MaxOpenConnections)
	}
	var autoVacuum int
	if err := store.writer.QueryRowContext(ctx, "PRAGMA auto_vacuum").Scan(&autoVacuum); err != nil {
		t.Fatal(err)
	}
	if autoVacuum != 2 {
		t.Fatalf("auto_vacuum = %d, want INCREMENTAL (2)", autoVacuum)
	}
	if _, err := store.Reader().ExecContext(ctx, "CREATE TABLE forbidden_reader_write(id INTEGER)"); err == nil {
		t.Fatal("query-only reader unexpectedly accepted DDL")
	}
}

func TestOpenInspectionRequiresExistingBaselineAndIsQueryOnly(t *testing.T) {
	ctx := context.Background()
	directory := t.TempDir()
	path := filepath.Join(directory, "inspection.db")
	if _, err := OpenInspection(ctx, path); err == nil {
		t.Fatal("OpenInspection created an absent database")
	}
	if _, err := os.Stat(path); !os.IsNotExist(err) {
		t.Fatalf("absent database stat after inspection = %v", err)
	}

	database, err := Open(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	if err := database.Close(); err != nil {
		t.Fatal(err)
	}
	inspection, err := OpenInspection(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	defer inspection.Close()
	if inspection.SchemaReport().SchemaVersion != 13 {
		t.Fatalf("inspection schema version = %d", inspection.SchemaReport().SchemaVersion)
	}
	if inspection.store.writer != nil {
		t.Fatal("inspection unexpectedly owns a writer pool")
	}
	if _, err := inspection.store.reader.ExecContext(ctx, "CREATE TABLE forbidden_inspection_write(id INTEGER)"); err == nil {
		t.Fatal("inspection connection accepted a write")
	}
}

func TestDialogueActionableDiscoveryUsesCandidateIndexedPlan(t *testing.T) {
	ctx := context.Background()
	store, err := Open(ctx, filepath.Join(t.TempDir(), "dialogue-plan.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	rows, err := store.reader.QueryContext(ctx, "EXPLAIN QUERY PLAN "+dialogueOlderCandidatesQuery,
		"01ARZ3NDEKTSV4RRFFQ69G5FAV", int64(1_000_000), 128)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var details []string
	for rows.Next() {
		var id, parent, unused int
		var detail string
		if err := rows.Scan(&id, &parent, &unused, &detail); err != nil {
			t.Fatal(err)
		}
		details = append(details, detail)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	plan := strings.Join(details, "\n")
	for _, index := range []string{"idx_events_resident_type_seq"} {
		if !strings.Contains(plan, index) {
			t.Fatalf("dialogue discovery plan does not use %s:\n%s", index, plan)
		}
	}
	if strings.Contains(plan, "generation_run_outcomes") {
		t.Fatalf("dialogue candidate plan reaches outcome history before reducer classification:\n%s", plan)
	}
}

func TestOpenRejectsFutureSchemaVersion(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "future.db")
	store, err := Open(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.writer.ExecContext(ctx,
		"UPDATE schema_migrations SET version_id=? WHERE version_id=? AND is_applied=1",
		migrations.BaselineVersion+1, migrations.BaselineVersion,
	); err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	_, err = Open(ctx, path)
	if err == nil || !strings.Contains(err.Error(), "newer than supported") {
		t.Fatalf("expected future-version failure, got %v", err)
	}
}

func TestOpenRejectsSameNameAndCountSchemaDefinitionTamper(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "tampered.db")
	store, err := Open(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.writer.ExecContext(ctx, `
DROP TRIGGER trg_events_no_update;
CREATE TRIGGER trg_events_no_update
BEFORE UPDATE ON events
BEGIN
    SELECT 1;
END`); err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}

	_, err = Open(ctx, path)
	if err == nil || !strings.Contains(err.Error(), "application schema fingerprint") {
		t.Fatalf("expected schema-definition fingerprint failure, got %v", err)
	}
}

func TestRealGooseReversibleUpDownUp(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "roundtrip.db")
	options := DefaultOptions()
	writer, err := openWriter(ctx, path, options)
	if err != nil {
		t.Fatal(err)
	}
	defer writer.Close()
	if err := initializeEmptyDatabase(ctx, writer); err != nil {
		t.Fatal(err)
	}
	reversible := migrationFilesThrough(t, 10)
	if err := migrateUpFS(ctx, writer, reversible); err != nil {
		t.Fatal(err)
	}
	if version, err := currentVersion(ctx, writer); err != nil || version != 10 {
		t.Fatalf("version after reversible Up = %d, err=%v", version, err)
	}
	if err := migrateDownAllForTest(ctx, writer, migrations.Files); err != nil {
		t.Fatal(err)
	}
	version, err := currentVersion(ctx, writer)
	if err != nil {
		t.Fatal(err)
	}
	if version != 0 {
		t.Fatalf("version after all Down migrations = %d, want 0", version)
	}
	var residual int
	if err := writer.QueryRowContext(ctx,
		"SELECT COUNT(*) FROM sqlite_schema WHERE name NOT LIKE 'sqlite_%' AND name <> ? AND tbl_name <> ?",
		migrations.GooseTableName, migrations.GooseTableName,
	).Scan(&residual); err != nil {
		t.Fatal(err)
	}
	if residual != 0 {
		t.Fatalf("objects remaining after all Down migrations = %d", residual)
	}
	if err := migrateUpFS(ctx, writer, reversible); err != nil {
		t.Fatal(err)
	}
	if version, err := currentVersion(ctx, writer); err != nil || version != 10 {
		t.Fatalf("version after reversible Up/Down/Up = %d, err=%v", version, err)
	}
}

func TestGooseMigrationFailureRollsBackFailedVersion(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "rollback.db")
	writer, err := openWriter(ctx, path, DefaultOptions())
	if err != nil {
		t.Fatal(err)
	}
	defer writer.Close()
	if err := initializeEmptyDatabase(ctx, writer); err != nil {
		t.Fatal(err)
	}
	broken := fstest.MapFS{
		"00001_anchor.sql": &fstest.MapFile{Data: []byte("-- +goose Up\nCREATE TABLE rollback_anchor(id INTEGER PRIMARY KEY) STRICT;\n-- +goose Down\nDROP TABLE rollback_anchor;\n")},
		"00002_broken.sql": &fstest.MapFile{Data: []byte("-- +goose Up\nCREATE TABLE should_rollback(id INTEGER PRIMARY KEY) STRICT;\nTHIS IS NOT SQL;\n-- +goose Down\nDROP TABLE should_rollback;\n")},
	}
	if err := migrateUpFS(ctx, writer, broken); err == nil {
		t.Fatal("broken migration unexpectedly succeeded")
	}
	anchor, err := tableExists(ctx, writer, "rollback_anchor")
	if err != nil {
		t.Fatal(err)
	}
	rolledBack, err := tableExists(ctx, writer, "should_rollback")
	if err != nil {
		t.Fatal(err)
	}
	if !anchor || rolledBack {
		t.Fatalf("transaction result anchor=%v should_rollback=%v", anchor, rolledBack)
	}
	version, err := currentVersion(ctx, writer)
	if err != nil {
		t.Fatal(err)
	}
	if version != 1 {
		t.Fatalf("version after failed migration = %d, want 1", version)
	}
}

func TestGooseSchemaExactlyMatchesCurrentV13Reference(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "migrated.db")
	store, err := Open(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	reference := openReferenceSchema(t)
	defer reference.Close()
	got := normalizedSchema(t, ctx, store.writer)
	want := normalizedSchema(t, ctx, reference)
	if strings.Join(got, "\n") != strings.Join(want, "\n") {
		t.Fatalf("current v13 schema differs from the generated reference artifact")
	}
	gotNames := make(map[string]struct{}, len(got))
	for _, object := range got {
		fields := strings.SplitN(object, "|", 4)
		gotNames[strings.Join(fields[:3], "|")] = struct{}{}
	}
	for _, required := range []string{
		"table|claim_statement_erasure_events|claim_statement_erasure_events",
		"index|uq_claim_stage_transitions_entity_commit|claim_stage_transitions",
		"index|uq_claim_status_transitions_entity_commit|claim_status_transitions",
		"index|idx_generation_runs_commit_resident_purpose_run|generation_runs",
		"trigger|trg_resident_revision_activations_identity_contract|resident_revision_activations",
	} {
		if _, ok := gotNames[required]; !ok {
			t.Errorf("M7 schema object is missing: %s", required)
		}
	}
}

func TestResidentScopeTriggersAreFailClosed(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "scope.db")
	store, err := Open(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	rows, err := store.writer.QueryContext(ctx,
		"SELECT name, sql FROM sqlite_schema WHERE type='trigger' AND (name LIKE '%_resident_scope' OR name = 'trg_residents_content_scope') ORDER BY name",
	)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	count := 0
	for rows.Next() {
		var name, body string
		if err := rows.Scan(&name, &body); err != nil {
			t.Fatal(err)
		}
		count++
		if !strings.Contains(strings.ToUpper(body), "NOT EXISTS") {
			t.Errorf("scope trigger %s is not fail-closed with NOT EXISTS", name)
		}
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	if count != 21 {
		t.Fatalf("resident-scope trigger count = %d, want 21", count)
	}
}

func openReferenceSchema(t *testing.T) *sql.DB {
	t.Helper()
	root := storeDocsBaselineRoot(t)
	body, err := os.ReadFile(filepath.Join(root, "docs", "database", "sqlite", "v0.1.3", "schema_v13_v0.1.3.sql"))
	if err != nil {
		t.Fatal(err)
	}
	db, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(string(body)); err != nil {
		db.Close()
		t.Fatal(err)
	}
	return db
}

func normalizedSchema(t *testing.T, ctx context.Context, db *sql.DB) []string {
	t.Helper()
	rows, err := db.QueryContext(ctx, `
SELECT type, name, tbl_name, sql
FROM sqlite_schema
WHERE name NOT LIKE 'sqlite_%'
  AND name <> ?
  AND tbl_name <> ?
ORDER BY type, name`, migrations.GooseTableName, migrations.GooseTableName)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var result []string
	for rows.Next() {
		var objectType, name, table string
		var sqlText sql.NullString
		if err := rows.Scan(&objectType, &name, &table, &sqlText); err != nil {
			t.Fatal(err)
		}
		normalizedSQL := strings.Join(strings.Fields(sqlText.String), " ")
		result = append(result, strings.Join([]string{objectType, name, table, normalizedSQL}, "|"))
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	sort.Strings(result)
	return result
}

func storeDocsBaselineRoot(t *testing.T) string {
	t.Helper()
	_, file, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("runtime.Caller failed")
	}
	return filepath.Clean(filepath.Join(filepath.Dir(file), "..", "..", "..", "testdata", "docs-baseline"))
}

var _ fs.FS = migrations.Files
