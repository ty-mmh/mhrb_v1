package sqlite

import (
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"fmt"
	"sort"
	"strings"
	"testing"
)

// This value is intentionally pinned rather than derived from the fixture
// builder. A test fixture that changes migrations 1-10 cannot silently become
// the new definition of v10.
const genuineV10SchemaFingerprint = "cbd561ad1d7827527eca0e413c53875ca12a19cc7d0ede9a8623cd759226cfae"

func v10SchemaFingerprint(t *testing.T, db *sql.DB) string {
	t.Helper()
	lines := make([]string, 0, 256)
	rows, err := db.Query(`SELECT type, name, tbl_name, COALESCE(sql, '') FROM sqlite_schema ORDER BY type, name, tbl_name`)
	if err != nil {
		t.Fatal(err)
	}
	for rows.Next() {
		var typ, name, tableName, sqlText string
		if err := rows.Scan(&typ, &name, &tableName, &sqlText); err != nil {
			rows.Close()
			t.Fatal(err)
		}
		lines = append(lines, fmt.Sprintf("schema|%s|%s|%s|%s", typ, name, tableName, normalizeSQL(sqlText)))
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		t.Fatal(err)
	}
	rows.Close()

	var tables []string
	tableRows, err := db.Query(`SELECT name FROM sqlite_schema WHERE type = 'table' ORDER BY name`)
	if err != nil {
		t.Fatal(err)
	}
	for tableRows.Next() {
		var name string
		if err := tableRows.Scan(&name); err != nil {
			tableRows.Close()
			t.Fatal(err)
		}
		tables = append(tables, name)
	}
	if err := tableRows.Err(); err != nil {
		tableRows.Close()
		t.Fatal(err)
	}
	tableRows.Close()
	sort.Strings(tables)
	for _, table := range tables {
		quoted := strings.ReplaceAll(table, "'", "''")
		info, err := db.Query("PRAGMA table_xinfo('" + quoted + "')")
		if err != nil {
			t.Fatal(err)
		}
		for info.Next() {
			var cid, notNull, pk, hidden int
			var name, typ string
			var dflt sql.NullString
			if err := info.Scan(&cid, &name, &typ, &notNull, &dflt, &pk, &hidden); err != nil {
				info.Close()
				t.Fatal(err)
			}
			lines = append(lines, fmt.Sprintf("xinfo|%s|%d|%s|%s|%d|%s|%d|%d|%t", table, cid, name, typ, notNull, dflt.String, pk, hidden, dflt.Valid))
		}
		if err := info.Err(); err != nil {
			info.Close()
			t.Fatal(err)
		}
		info.Close()

		foreignKeys, err := db.Query("PRAGMA foreign_key_list('" + quoted + "')")
		if err != nil {
			t.Fatal(err)
		}
		for foreignKeys.Next() {
			var id, seq int
			var parent, from, to, onUpdate, onDelete, match string
			if err := foreignKeys.Scan(&id, &seq, &parent, &from, &to, &onUpdate, &onDelete, &match); err != nil {
				foreignKeys.Close()
				t.Fatal(err)
			}
			lines = append(lines, fmt.Sprintf("fk|%s|%d|%d|%s|%s|%s|%s|%s|%s", table, id, seq, parent, from, to, onUpdate, onDelete, match))
		}
		if err := foreignKeys.Err(); err != nil {
			foreignKeys.Close()
			t.Fatal(err)
		}
		foreignKeys.Close()
	}
	sort.Strings(lines)
	digest := sha256.Sum256([]byte(strings.Join(lines, "\n") + "\n"))
	return hex.EncodeToString(digest[:])
}

func normalizeSQL(value string) string {
	return strings.Join(strings.Fields(value), " ")
}
