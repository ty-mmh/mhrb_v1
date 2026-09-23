package sqlite

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
)

type pragmaQuerier interface {
	QueryRowContext(context.Context, string, ...any) *sql.Row
}

func initializeEmptyDatabase(ctx context.Context, db *sql.DB) error {
	var tableCount int
	if err := db.QueryRowContext(ctx,
		"SELECT COUNT(*) FROM sqlite_schema WHERE type = 'table' AND name NOT LIKE 'sqlite_%'",
	).Scan(&tableCount); err != nil {
		return fmt.Errorf("inspect database before initialization: %w", err)
	}
	if tableCount != 0 {
		return nil
	}
	if _, err := db.ExecContext(ctx, "PRAGMA auto_vacuum = INCREMENTAL"); err != nil {
		return fmt.Errorf("enable incremental auto_vacuum before migrations: %w", err)
	}
	// Changing an existing database header from NONE to INCREMENTAL becomes
	// durable only after VACUUM. The file is still schema-empty here, before
	// Goose creates its metadata table, so this is the only safe opportunity.
	if _, err := db.ExecContext(ctx, "VACUUM"); err != nil {
		return fmt.Errorf("persist incremental auto_vacuum before migrations: %w", err)
	}
	var mode int
	if err := db.QueryRowContext(ctx, "PRAGMA auto_vacuum").Scan(&mode); err != nil {
		return fmt.Errorf("verify auto_vacuum: %w", err)
	}
	if mode != 2 {
		return fmt.Errorf("auto_vacuum = %d, want INCREMENTAL (2)", mode)
	}
	return nil
}

func verifyWriterPragmas(ctx context.Context, db *sql.DB, options Options) error {
	connection, err := db.Conn(ctx)
	if err != nil {
		return fmt.Errorf("acquire SQLite writer connection for PRAGMA verification: %w", err)
	}
	defer connection.Close()
	if err := expectIntPragma(ctx, connection, "foreign_keys", 1); err != nil {
		return fmt.Errorf("writer %w", err)
	}
	if err := expectTextPragma(ctx, connection, "journal_mode", "wal"); err != nil {
		return fmt.Errorf("writer %w", err)
	}
	if err := expectIntPragma(ctx, connection, "synchronous", 2); err != nil {
		return fmt.Errorf("writer %w", err)
	}
	if err := expectIntPragma(ctx, connection, "busy_timeout", int(options.BusyTimeout.Milliseconds())); err != nil {
		return fmt.Errorf("writer %w", err)
	}
	if err := expectIntPragma(ctx, connection, "trusted_schema", 0); err != nil {
		return fmt.Errorf("writer %w", err)
	}
	if err := expectIntPragma(ctx, connection, "secure_delete", 1); err != nil {
		return fmt.Errorf("writer %w", err)
	}
	return nil
}

func verifyReaderPragmas(ctx context.Context, db *sql.DB, options Options, connectionCount int) error {
	connections := make([]*sql.Conn, 0, connectionCount)
	defer func() {
		for _, connection := range connections {
			_ = connection.Close()
		}
	}()
	for i := 0; i < connectionCount; i++ {
		connection, err := db.Conn(ctx)
		if err != nil {
			return fmt.Errorf("acquire SQLite reader connection %d for PRAGMA verification: %w", i+1, err)
		}
		connections = append(connections, connection)
		if err := expectIntPragma(ctx, connection, "foreign_keys", 1); err != nil {
			return fmt.Errorf("reader connection %d: %w", i+1, err)
		}
		if err := expectIntPragma(ctx, connection, "query_only", 1); err != nil {
			return fmt.Errorf("reader connection %d: %w", i+1, err)
		}
		if err := expectIntPragma(ctx, connection, "busy_timeout", int(options.BusyTimeout.Milliseconds())); err != nil {
			return fmt.Errorf("reader connection %d: %w", i+1, err)
		}
		if err := expectIntPragma(ctx, connection, "trusted_schema", 0); err != nil {
			return fmt.Errorf("reader connection %d: %w", i+1, err)
		}
	}
	return nil
}

func expectIntPragma(ctx context.Context, queryer pragmaQuerier, name string, want int) error {
	var got int
	if err := queryer.QueryRowContext(ctx, "PRAGMA "+name).Scan(&got); err != nil {
		return fmt.Errorf("read PRAGMA %s: %w", name, err)
	}
	if got != want {
		return fmt.Errorf("PRAGMA %s = %d, want %d", name, got, want)
	}
	return nil
}

func expectTextPragma(ctx context.Context, queryer pragmaQuerier, name, want string) error {
	var got string
	if err := queryer.QueryRowContext(ctx, "PRAGMA "+name).Scan(&got); err != nil {
		return fmt.Errorf("read PRAGMA %s: %w", name, err)
	}
	if !strings.EqualFold(got, want) {
		return fmt.Errorf("PRAGMA %s = %q, want %q", name, got, want)
	}
	return nil
}
