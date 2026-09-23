package sqlite

import (
	"context"
	"database/sql"
	"fmt"
	"slices"
	"strings"

	"mahoroba.local/mahoroba/internal/assets/migrations"
)

// DiagnosticExpectedSchemaVersion and DiagnosticExpectedSchemaFingerprint
// expose the compiled schema contract without granting migration authority.
func DiagnosticExpectedSchemaVersion() int64 { return migrations.BaselineVersion }

func DiagnosticExpectedSchemaFingerprint() string { return expectedApplicationSchemaFingerprint }

func (inspection *DiagnosticInspection) ApplicationSchemaFingerprint(ctx context.Context) (string, error) {
	if inspection == nil || inspection.store == nil || inspection.store.reader == nil {
		return "", fmt.Errorf("sqlite: diagnostic reader is closed")
	}
	return applicationSchemaFingerprint(ctx, inspection.store.reader)
}

// HasColumns checks table shape before a section issues its semantic query.
// A missing table/column is not an operational SQL error; callers map false
// to the closed table_unavailable diagnostic state.
func (inspection *DiagnosticInspection) HasColumns(
	ctx context.Context,
	table string,
	columns ...string,
) (bool, error) {
	if inspection == nil || inspection.store == nil || inspection.store.reader == nil {
		return false, fmt.Errorf("sqlite: diagnostic reader is closed")
	}
	if table == "" || strings.ContainsAny(table, "\x00'\".;/\\") {
		return false, fmt.Errorf("sqlite: invalid diagnostic table name")
	}
	rows, err := inspection.store.reader.QueryContext(ctx, "PRAGMA table_info('"+table+"')")
	if err != nil {
		return false, err
	}
	defer rows.Close()
	found := make(map[string]struct{}, len(columns))
	for rows.Next() {
		var cid int
		var name, dataType string
		var notNull, primaryKey int
		var defaultValue sql.NullString
		if err := rows.Scan(&cid, &name, &dataType, &notNull, &defaultValue, &primaryKey); err != nil {
			return false, err
		}
		if slices.Contains(columns, name) {
			found[name] = struct{}{}
		}
	}
	if err := rows.Err(); err != nil {
		return false, err
	}
	if len(found) != len(columns) {
		return false, nil
	}
	return true, nil
}
