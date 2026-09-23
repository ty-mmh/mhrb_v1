package sqlite

import (
	"context"
	"errors"
	"fmt"

	moderncsqlite "modernc.org/sqlite"
)

type onlineBackuper interface {
	NewBackup(string) (*moderncsqlite.Backup, error)
}

// SnapshotTo copies the Inspection's stable SQLite view with SQLite's online
// backup API. "Online" names the SQLite consistency primitive; the supported
// M7 producer still owns an offline OpenReadOnlyBoundDatabase session and must
// not coexist with a writable source Store. It never reopens the source by
// pathname and never migrates it. destination must be a private, pre-created
// regular file owned by the caller.
func (inspection *Inspection) SnapshotTo(ctx context.Context, destination string) error {
	if inspection == nil || inspection.store == nil || inspection.store.reader == nil {
		return errors.New("sqlite: backup snapshot inspection is required")
	}
	if ctx == nil {
		return errors.New("sqlite: nil backup snapshot context")
	}
	connection, err := inspection.store.reader.Conn(ctx)
	if err != nil {
		return fmt.Errorf("sqlite: reserve backup source connection: %w", err)
	}
	defer connection.Close()

	err = connection.Raw(func(raw any) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		backuper, ok := raw.(onlineBackuper)
		if !ok {
			return errors.New("sqlite: driver does not provide the required online backup primitive")
		}
		backup, err := backuper.NewBackup(destination)
		if err != nil {
			return fmt.Errorf("sqlite: initialize online backup: %w", err)
		}
		_, stepErr := backup.Step(-1)
		finishErr := backup.Finish()
		if stepErr != nil || finishErr != nil {
			return fmt.Errorf("sqlite: copy online backup: %w", errors.Join(stepErr, finishErr))
		}
		return ctx.Err()
	})
	if err != nil {
		return err
	}
	return nil
}
