package sqlite

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"fmt"
	"io/fs"
	"strings"
	"sync"
	"testing/fstest"

	"github.com/pressly/goose/v3"

	"mahoroba.local/mahoroba/internal/assets/migrations"
)

// gooseLegacyMu protects Goose's legacy package-level base FS, dialect, and
// table name. Keeping the legacy surface behind this lock makes migration
// operations deterministic until the pinned Goose provider API is adopted.
var gooseLegacyMu sync.Mutex

// ErrForwardOnlyMigration is returned before Goose executes any Down SQL for
// a migration whose data-loss semantics cannot be reversed safely.
var ErrForwardOnlyMigration = errors.New("sqlite: migration is forward-only")

func migrateUp(ctx context.Context, db *sql.DB) error {
	if err := migrations.Validate(); err != nil {
		return fmt.Errorf("validate embedded migration baseline: %w", err)
	}
	versionBefore, err := currentVersion(ctx, db)
	if err != nil {
		return err
	}
	if err := rejectLegacyClaimIdentityRepair(ctx, db); err != nil {
		return err
	}
	if err := rejectM7AssertionIdentityDuplicates(ctx, db, versionBefore); err != nil {
		return err
	}
	if versionBefore == 10 {
		// SQLite cannot rebuild a referenced table inside a transaction while
		// foreign_keys=ON. The Goose transaction still makes the migration
		// atomic; references are checked again when the connection is restored.
		if _, err := db.ExecContext(ctx, "PRAGMA foreign_keys=OFF"); err != nil {
			return fmt.Errorf("disable foreign-key checks for v10 rebuild: %w", err)
		}
		defer func() { _, _ = db.ExecContext(ctx, "PRAGMA foreign_keys=ON") }()
	}
	if err := migrateUpFS(ctx, db, migrations.Files); err != nil {
		return err
	}
	version, err := currentVersion(ctx, db)
	if err != nil {
		return err
	}
	if version != migrations.BaselineVersion {
		return fmt.Errorf("schema version = %d, require exactly %d", version, migrations.BaselineVersion)
	}
	return nil
}

// rejectM7AssertionIdentityDuplicates keeps the v12 uniqueness migration from
// choosing an arbitrary historical assertion when an older database already
// contains duplicate entity/commit identities. The migration must stay a
// schema transition; repairing Canonical history requires an explicit static
// design decision.
func rejectM7AssertionIdentityDuplicates(ctx context.Context, db *sql.DB, version int64) error {
	if version < 6 || version >= 12 {
		return nil
	}
	var duplicateValidity, duplicateViewScope bool
	err := db.QueryRowContext(ctx, `
		SELECT
			EXISTS(
				SELECT 1
				  FROM claim_validity_assertions
				 GROUP BY claim_id, canonical_commit_id
				HAVING COUNT(*) > 1
			),
			EXISTS(
				SELECT 1
				  FROM claim_view_scope_assertions
				 GROUP BY claim_id, canonical_commit_id
				HAVING COUNT(*) > 1
			)`).Scan(&duplicateValidity, &duplicateViewScope)
	if err != nil {
		return fmt.Errorf("inspect M7 assertion identities: %w", err)
	}
	if duplicateValidity {
		return fmt.Errorf("m7_claim_validity_assertion_identity_duplicates_require_static_repair")
	}
	if duplicateViewScope {
		return fmt.Errorf("m7_claim_view_scope_assertion_identity_duplicates_require_static_repair")
	}
	return nil
}

// rejectLegacyClaimIdentityRepair is intentionally a preflight rather than a
// data-fixing migration. A v10 database can contain erased statement content
// whose unsalted claim hash has no historical erasure event. Inventing that
// event during an upgrade would destroy the audit meaning of the new v11
// contract, so the complete upgrade remains at v10 and asks for static repair.
func rejectLegacyClaimIdentityRepair(ctx context.Context, db *sql.DB) error {
	version, err := currentVersion(ctx, db)
	if err != nil || version != 10 {
		return err
	}
	var invalid bool
	err = db.QueryRowContext(ctx, `
		SELECT EXISTS(
			SELECT 1
			FROM claims claim
			JOIN content_objects content ON content.content_id = claim.statement_content_id
			WHERE content.erasure_state = 'erased'
			  AND claim.statement_hash IS NOT NULL
		)`).Scan(&invalid)
	if err != nil {
		return fmt.Errorf("inspect legacy claim identity state: %w", err)
	}
	if invalid {
		return fmt.Errorf("legacy_claim_identity_requires_static_repair")
	}
	return nil
}

func migrateUpFS(ctx context.Context, db *sql.DB, migrationFS fs.FS) error {
	gooseLegacyMu.Lock()
	defer gooseLegacyMu.Unlock()
	if err := configureGoose(migrationFS); err != nil {
		return err
	}
	if err := goose.UpContext(ctx, db, "."); err != nil {
		return fmt.Errorf("goose up: %w", err)
	}
	return nil
}

// migrateDownAllForTest is deliberately unexported: product code must use
// forward-only migrations once a database can contain resident data.
func migrateDownAllForTest(ctx context.Context, db *sql.DB, migrationFS fs.FS) error {
	gooseLegacyMu.Lock()
	defer gooseLegacyMu.Unlock()
	if err := configureGoose(migrationFS); err != nil {
		return err
	}
	for {
		version, err := goose.GetDBVersion(db)
		if err != nil {
			return fmt.Errorf("read Goose version before down: %w", err)
		}
		if version == 0 {
			return nil
		}
		if migrations.IsForwardOnlyVersion(version) {
			return fmt.Errorf("%w: version %d", ErrForwardOnlyMigration, version)
		}
		if err := goose.DownContext(ctx, db, "."); err != nil {
			return fmt.Errorf("goose down from version %d: %w", version, err)
		}
	}
}

func migrateDownOneForTest(ctx context.Context, db *sql.DB, migrationFS fs.FS) error {
	gooseLegacyMu.Lock()
	defer gooseLegacyMu.Unlock()
	version, err := currentVersion(ctx, db)
	if err != nil {
		return err
	}
	if migrations.IsForwardOnlyVersion(version) {
		return fmt.Errorf("%w: version %d", ErrForwardOnlyMigration, version)
	}
	if err := configureGoose(migrationFS); err != nil {
		return err
	}
	if err := goose.DownContext(ctx, db, "."); err != nil {
		return fmt.Errorf("goose down: %w", err)
	}
	return nil
}

func configureGoose(migrationFS fs.FS) error {
	compatibleFS, err := gooseCompatibleFS(migrationFS)
	if err != nil {
		return err
	}
	goose.SetBaseFS(compatibleFS)
	goose.SetTableName(migrations.GooseTableName)
	if err := goose.SetDialect("sqlite3"); err != nil {
		return fmt.Errorf("set Goose sqlite dialect: %w", err)
	}
	return nil
}

// gooseCompatibleFS keeps the hash-pinned source bytes immutable while adding
// parser-only annotations to trigger migrations in memory. SQLite trigger
// bodies contain semicolons; Goose otherwise splits them into incomplete SQL.
// StatementBegin/End are comments and do not change the executed DDL meaning.
func gooseCompatibleFS(source fs.FS) (fs.FS, error) {
	result := make(fstest.MapFS)
	err := fs.WalkDir(source, ".", func(name string, entry fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if entry.IsDir() || !strings.HasSuffix(strings.ToLower(name), ".sql") {
			return nil
		}
		data, err := fs.ReadFile(source, name)
		if err != nil {
			return err
		}
		data = annotateGooseTriggerSections(data, name)
		result[name] = &fstest.MapFile{Data: data}
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("prepare Goose migration view: %w", err)
	}
	return result, nil
}

func annotateGooseTriggerSections(data []byte, name string) []byte {
	const up = "-- +goose Up\n"
	const down = "-- +goose Down\n"
	upIndex := bytes.Index(data, []byte(up))
	downIndex := bytes.Index(data, []byte(down))
	if upIndex < 0 || downIndex < 0 || downIndex < upIndex {
		return data
	}
	annotate := func(section []byte) []byte {
		if !bytes.Contains(section, []byte("CREATE TRIGGER")) || bytes.Contains(section, []byte("-- +goose StatementBegin")) {
			return section
		}
		wrapped := make([]byte, 0, len(section)+64)
		wrapped = append(wrapped, []byte("-- +goose StatementBegin\n")...)
		wrapped = append(wrapped, section...)
		wrapped = append(wrapped, []byte("\n-- +goose StatementEnd\n")...)
		return wrapped
	}
	upEnd := upIndex + len(up)
	downEnd := downIndex + len(down)
	result := make([]byte, 0, len(data)+128)
	result = append(result, data[:upEnd]...)
	result = append(result, annotate(data[upEnd:downIndex])...)
	result = append(result, data[downIndex:downEnd]...)
	result = append(result, annotate(data[downEnd:])...)
	_ = name
	return result
}

func currentVersion(ctx context.Context, db *sql.DB) (int64, error) {
	exists, err := tableExists(ctx, db, migrations.GooseTableName)
	if err != nil {
		return 0, err
	}
	if !exists {
		return 0, nil
	}
	query := "SELECT version_id, is_applied FROM " + migrations.GooseTableName + " ORDER BY id DESC"
	rows, err := db.QueryContext(ctx, query)
	if err != nil {
		return 0, fmt.Errorf("read schema version history: %w", err)
	}
	defer rows.Close()
	seen := make(map[int64]struct{})
	for rows.Next() {
		var version int64
		var applied bool
		if err := rows.Scan(&version, &applied); err != nil {
			return 0, fmt.Errorf("scan schema version history: %w", err)
		}
		if _, alreadySeen := seen[version]; alreadySeen {
			continue
		}
		seen[version] = struct{}{}
		if applied {
			return version, nil
		}
	}
	if err := rows.Err(); err != nil {
		return 0, fmt.Errorf("iterate schema version history: %w", err)
	}
	return 0, nil
}

func rejectUnsupportedFutureVersion(ctx context.Context, db *sql.DB) error {
	version, err := currentVersion(ctx, db)
	if err != nil {
		return err
	}
	if version > migrations.BaselineVersion {
		return fmt.Errorf("database schema version %d is newer than supported version %d", version, migrations.BaselineVersion)
	}
	return nil
}

func tableExists(ctx context.Context, db *sql.DB, name string) (bool, error) {
	var exists bool
	err := db.QueryRowContext(ctx,
		"SELECT EXISTS(SELECT 1 FROM sqlite_schema WHERE type = 'table' AND name = ?)", name,
	).Scan(&exists)
	if err != nil {
		return false, fmt.Errorf("inspect sqlite_schema for table %q: %w", name, err)
	}
	return exists, nil
}
