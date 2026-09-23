package sqlite

import (
	"context"
	"fmt"

	"mahoroba.local/mahoroba/internal/canonical"
)

// BootstrapInitialized is the version-independent startup probe. It must stay
// usable before the current dialogue pipeline row has been registered.
func (r *CanonicalRepository) BootstrapInitialized(ctx context.Context) (bool, error) {
	var count int64
	if err := r.store.reader.QueryRowContext(ctx, `SELECT count(*) FROM principals`).Scan(&count); err != nil {
		return false, fmt.Errorf("sqlite: probe bootstrap initialization: %w", err)
	}
	return count > 0, nil
}

// ListResidentIDs deliberately avoids loading current pipeline or revision
// state. Startup integrity verification uses it before version registration.
func (r *CanonicalRepository) ListResidentIDs(ctx context.Context) ([]canonical.ID, error) {
	rows, err := r.store.reader.QueryContext(ctx, `SELECT resident_id FROM residents ORDER BY created_at, resident_id`)
	if err != nil {
		return nil, fmt.Errorf("sqlite: list resident IDs: %w", err)
	}
	defer rows.Close()

	var ids []canonical.ID
	for rows.Next() {
		var raw string
		if err := rows.Scan(&raw); err != nil {
			return nil, fmt.Errorf("sqlite: scan resident ID: %w", err)
		}
		id, err := canonical.ParseID(raw)
		if err != nil {
			return nil, fmt.Errorf("sqlite: parse resident ID: %w", err)
		}
		ids = append(ids, id)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("sqlite: list resident IDs: %w", err)
	}
	return ids, nil
}

// ResidentExists is the version-independent resident existence probe used by
// read-only verification and pre-registration command preflight. It must not
// hydrate a ResidentSnapshot because that would require the current dialogue
// pipeline row to exist already.
func (r *CanonicalRepository) ResidentExists(ctx context.Context, residentID canonical.ID) (bool, error) {
	if err := residentID.Validate(); err != nil {
		return false, err
	}
	var exists bool
	if err := r.store.reader.QueryRowContext(ctx,
		`SELECT EXISTS(SELECT 1 FROM residents WHERE resident_id = ?)`, residentID.String(),
	).Scan(&exists); err != nil {
		return false, fmt.Errorf("sqlite: probe resident existence: %w", err)
	}
	return exists, nil
}
