package migrations

import (
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"

	_ "modernc.org/sqlite"
)

func TestM7GeneratedSchemaArtifactsAreCurrent(t *testing.T) {
	root := docsBaselineRoot(t)
	for _, tc := range []struct {
		head int
		name string
	}{
		{head: 10, name: "schema_v10_v0.1.3.sql"},
		{head: 11, name: "schema_v11_v0.1.3.sql"},
		{head: 12, name: "schema_v12_v0.1.3.sql"},
		{head: 13, name: "schema_v13_v0.1.3.sql"},
	} {
		body, err := os.ReadFile(filepath.Join(root, "docs", "database", "sqlite", "v0.1.3", tc.name))
		if err != nil {
			t.Fatal(err)
		}
		want := generatedSchemaBytes(t, tc.head)
		if !bytes.Equal(body, want) {
			t.Fatalf("%s is not generated from embedded migrations", tc.name)
		}
		if tc.head == 10 {
			digest := sha256.Sum256(body)
			if got := hex.EncodeToString(digest[:]); got != "9e5ab0322f8049816678554bd3529d4a7b33f43397b25f683a07b16de0ba2671" {
				t.Fatalf("v10 artifact SHA-256 = %s", got)
			}
		}
		if tc.head == 11 {
			digest := sha256.Sum256(body)
			if got := hex.EncodeToString(digest[:]); got != "d7b1fe5c0e4560dc83d4bdcac7b0b6a57ca0866df05e438937cc8986678f373e" {
				t.Fatalf("v11 artifact SHA-256 = %s", got)
			}
		}
		if tc.head == 12 {
			digest := sha256.Sum256(body)
			if got := hex.EncodeToString(digest[:]); got != "d1947ff92c92927255d1a06a8872f4a636faa62ec319724b7b0f90de920351bc" {
				t.Fatalf("v12 artifact SHA-256 = %s", got)
			}
		}
	}
}

func TestM7V10ReferenceSchemaExactlyMatchesMigrationsThrough10(t *testing.T) {
	assertReferenceSchemaMatchesMigrations(t, 10, "schema_v10_v0.1.3.sql")
}

func TestM7V11ReferenceSchemaExactlyMatchesMigrationsThrough11(t *testing.T) {
	assertReferenceSchemaMatchesMigrations(t, 11, "schema_v11_v0.1.3.sql")
}

func TestM7V12ReferenceSchemaExactlyMatchesAllMigrations(t *testing.T) {
	assertReferenceSchemaMatchesMigrations(t, 12, "schema_v12_v0.1.3.sql")
}

func TestCOVR02V13ReferenceSchemaExactlyMatchesAllMigrations(t *testing.T) {
	assertReferenceSchemaMatchesMigrations(t, 13, "schema_v13_v0.1.3.sql")
}

func generatedSchemaBytes(t *testing.T, head int) []byte {
	t.Helper()
	var builder strings.Builder
	for index, descriptor := range Descriptors() {
		if index >= head {
			break
		}
		body, err := Files.ReadFile(descriptor.Name)
		if err != nil {
			t.Fatal(err)
		}
		text := string(body)
		up := strings.SplitN(strings.SplitN(text, "-- +goose Up", 2)[1], "-- +goose Down", 2)[0]
		builder.WriteString("-- ")
		builder.WriteString(descriptor.Name)
		builder.WriteString(strings.TrimRight(up, "\n"))
		builder.WriteString("\n\n")
	}
	return []byte(strings.TrimRight(builder.String(), "\n") + "\n")
}

func assertReferenceSchemaMatchesMigrations(t *testing.T, head int, artifact string) {
	t.Helper()
	ctx := context.Background()
	root := docsBaselineRoot(t)
	artifactBody, err := os.ReadFile(filepath.Join(root, "docs", "database", "sqlite", "v0.1.3", artifact))
	if err != nil {
		t.Fatal(err)
	}
	artifactDB := openSchemaArtifact(t, string(artifactBody))
	defer artifactDB.Close()
	migrationDB := openSchemaArtifact(t, "")
	defer migrationDB.Close()
	for index, descriptor := range Descriptors() {
		if index >= head {
			break
		}
		body, err := Files.ReadFile(descriptor.Name)
		if err != nil {
			t.Fatal(err)
		}
		up := strings.SplitN(strings.SplitN(string(body), "-- +goose Up", 2)[1], "-- +goose Down", 2)[0]
		if _, err := migrationDB.ExecContext(ctx, up); err != nil {
			t.Fatalf("apply %s: %v", descriptor.Name, err)
		}
	}
	want := schemaSnapshot(t, migrationDB)
	got := schemaSnapshot(t, artifactDB)
	if !bytes.Equal(want, got) {
		t.Fatalf("%s differs from migrations through %d\nwant:\n%s\ngot:\n%s", artifact, head, want, got)
	}
}

func openSchemaArtifact(t *testing.T, schema string) *sql.DB {
	t.Helper()
	db, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	if schema != "" {
		if _, err := db.Exec(schema); err != nil {
			db.Close()
			t.Fatal(err)
		}
	}
	return db
}

func schemaSnapshot(t *testing.T, db *sql.DB) []byte {
	t.Helper()
	var lines []string
	rows, err := db.Query(`SELECT type, name, tbl_name, COALESCE(sql, '') FROM sqlite_schema WHERE name NOT LIKE 'sqlite_%' ORDER BY type, name, tbl_name`)
	if err != nil {
		t.Fatal(err)
	}
	for rows.Next() {
		var typ, name, table, definition string
		if err := rows.Scan(&typ, &name, &table, &definition); err != nil {
			rows.Close()
			t.Fatal(err)
		}
		lines = append(lines, fmt.Sprintf("schema|%s|%s|%s|%s", typ, name, table, normalizeSchemaSQL(definition)))
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		t.Fatal(err)
	}
	rows.Close()

	var tables []string
	tableRows, err := db.Query(`SELECT name FROM sqlite_schema WHERE type='table' AND name NOT LIKE 'sqlite_%' ORDER BY name`)
	if err != nil {
		t.Fatal(err)
	}
	for tableRows.Next() {
		var table string
		if err := tableRows.Scan(&table); err != nil {
			tableRows.Close()
			t.Fatal(err)
		}
		tables = append(tables, table)
	}
	tableRows.Close()
	for _, tableName := range tables {
		quoted := strings.ReplaceAll(tableName, "'", "''")
		info, err := db.Query("PRAGMA table_xinfo('" + quoted + "')")
		if err != nil {
			t.Fatal(err)
		}
		for info.Next() {
			var cid, notNull, pk, hidden int
			var name, typ string
			var defaultValue sql.NullString
			if err := info.Scan(&cid, &name, &typ, &notNull, &defaultValue, &pk, &hidden); err != nil {
				info.Close()
				t.Fatal(err)
			}
			lines = append(lines, fmt.Sprintf("xinfo|%s|%d|%s|%s|%d|%s|%d|%d|%t", tableName, cid, name, typ, notNull, defaultValue.String, pk, hidden, defaultValue.Valid))
		}
		info.Close()
		foreignKeys, err := db.Query("PRAGMA foreign_key_list('" + quoted + "')")
		if err != nil {
			t.Fatal(err)
		}
		for foreignKeys.Next() {
			var id, sequence int
			var parent, from, to, onUpdate, onDelete, match string
			if err := foreignKeys.Scan(&id, &sequence, &parent, &from, &to, &onUpdate, &onDelete, &match); err != nil {
				foreignKeys.Close()
				t.Fatal(err)
			}
			lines = append(lines, fmt.Sprintf("fk|%s|%d|%d|%s|%s|%s|%s|%s|%s", tableName, id, sequence, parent, from, to, onUpdate, onDelete, match))
		}
		foreignKeys.Close()
	}
	sort.Strings(lines)
	return []byte(strings.Join(lines, "\n") + "\n")
}

func normalizeSchemaSQL(value string) string {
	return strings.Join(strings.Fields(value), " ")
}
