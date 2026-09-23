package service

import (
	"fmt"

	storesqlite "mahoroba.local/mahoroba/internal/store/sqlite"
)

// ApplyBoundary wraps the shared protected writable database boundary in the
// erasure service's public closed-source error contract.
type ApplyBoundary struct{ database *storesqlite.BoundDatabase }

func OpenApplyBoundary(dataDir, databaseFilename string) (*ApplyBoundary, error) {
	database, err := storesqlite.OpenWritableBoundDatabase(dataDir, databaseFilename)
	if err != nil {
		return nil, fmt.Errorf("%w: open protected apply database: %v", ErrSourceUnavailable, err)
	}
	return &ApplyBoundary{database: database}, nil
}

func (boundary *ApplyBoundary) DataDir() string {
	if boundary == nil || boundary.database == nil {
		return ""
	}
	return boundary.database.DataDir()
}

func (boundary *ApplyBoundary) Verify() error {
	if boundary == nil || boundary.database == nil {
		return fmt.Errorf("%w: incomplete apply boundary", ErrSourceUnavailable)
	}
	if err := boundary.database.Verify(); err != nil {
		return fmt.Errorf("%w: apply database identity changed: %v", ErrSourceUnavailable, err)
	}
	return nil
}

func (boundary *ApplyBoundary) Close() error {
	if boundary == nil || boundary.database == nil {
		return nil
	}
	return boundary.database.Close()
}
