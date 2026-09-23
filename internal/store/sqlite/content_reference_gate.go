package sqlite

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"

	"mahoroba.local/mahoroba/internal/canonical"
	"mahoroba.local/mahoroba/internal/contentref"
	"mahoroba.local/mahoroba/internal/projection"
)

type contentReferenceGateQuerier interface {
	QueryRowContext(context.Context, string, ...any) *sql.Row
}

// RequireCurrentContentReferences verifies the all-resident publication gate
// from one read snapshot. It never repairs or mutates a source database.
func (inspection *Inspection) RequireCurrentContentReferences(ctx context.Context) error {
	if inspection == nil || inspection.store == nil {
		return errors.New("sqlite: content references inspection is required")
	}
	return inspection.store.requireCurrentContentReferences(ctx)
}

// RequireCurrentContentReferences is the writable-store form used only after
// restore has synchronously rebuilt the Projection and before publication.
func (store *Store) RequireCurrentContentReferences(ctx context.Context) error {
	if store == nil {
		return errors.New("sqlite: content references store is required")
	}
	return store.requireCurrentContentReferences(ctx)
}

func (store *Store) requireCurrentContentReferences(ctx context.Context) (_ error) {
	if ctx == nil || store.reader == nil {
		return errors.New("sqlite: content references gate requires a reader and context")
	}
	tx, err := store.reader.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return err
	}
	defer tx.Rollback()
	var rawHead sql.NullInt64
	if err := tx.QueryRowContext(ctx, `SELECT MAX(commit_seq) FROM canonical_commits`).Scan(&rawHead); err != nil {
		return err
	}
	rows, err := tx.QueryContext(ctx, `SELECT resident_id FROM residents ORDER BY resident_id`)
	if err != nil {
		return err
	}
	var residents []canonical.ID
	for rows.Next() {
		var raw string
		if err := rows.Scan(&raw); err != nil {
			_ = rows.Close()
			return err
		}
		residentID, err := canonical.ParseID(raw)
		if err != nil {
			_ = rows.Close()
			return err
		}
		residents = append(residents, residentID)
	}
	if err := rows.Err(); err != nil {
		_ = rows.Close()
		return err
	}
	if err := rows.Close(); err != nil {
		return err
	}
	var orphanResidentRows int64
	if err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM content_references reference
		LEFT JOIN residents resident ON resident.resident_id = reference.resident_id
		WHERE resident.resident_id IS NULL`).Scan(&orphanResidentRows); err != nil {
		return fmt.Errorf("%w: inspect content references resident scope", projection.ErrProjectionNotCurrent)
	}
	if orphanResidentRows != 0 {
		return fmt.Errorf("%w: content references contain an unknown resident scope", projection.ErrProjectionNotCurrent)
	}
	var orphanWatermarks int64
	if err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM projection_watermarks watermark
		LEFT JOIN residents resident ON resident.resident_id = watermark.resident_id
		WHERE watermark.projection_name = ? AND resident.resident_id IS NULL`,
		projection.ContentReferencesName).Scan(&orphanWatermarks); err != nil {
		return fmt.Errorf("%w: inspect content references watermark scope", projection.ErrProjectionNotCurrent)
	}
	if orphanWatermarks != 0 {
		return fmt.Errorf("%w: content references contain an unknown resident watermark", projection.ErrProjectionNotCurrent)
	}
	if len(residents) == 0 {
		// Global Canonical commits (for example integrity pipeline version
		// registration during empty-store restore) do not create a resident
		// reachability domain. With both orphan checks above at zero, the exact
		// all-resident content-reference projection is therefore the empty set.
		return tx.Commit()
	}
	if !rawHead.Valid {
		return fmt.Errorf("%w: residents exist without Canonical head", projection.ErrProjectionNotCurrent)
	}
	head, err := canonical.NewCommitSeq(rawHead.Int64)
	if err != nil {
		return fmt.Errorf("%w: invalid Canonical head", projection.ErrProjectionNotCurrent)
	}
	definition := projection.ContentReferencesDefinition()
	for _, residentID := range residents {
		watermark, exists, err := readProjectionWatermark(ctx, tx, definition.Name, residentID)
		if err != nil {
			return fmt.Errorf("%w: read content references watermark", projection.ErrProjectionNotCurrent)
		}
		if !exists || watermark.ProjectionName != definition.Name || watermark.ResidentID != residentID ||
			watermark.ProjectionVersion != definition.Version || watermark.SourceCommitSeq != head || len(watermark.Dependencies) != 0 {
			return fmt.Errorf("%w: content references watermark identity/version/head/dependencies differ", projection.ErrProjectionNotCurrent)
		}
		if err := requireContentReferenceBody(ctx, tx, residentID); err != nil {
			return err
		}
	}
	return tx.Commit()
}

func requireContentReferenceBody(ctx context.Context, query contentReferenceGateQuerier, residentID canonical.ID) error {
	var invalidRows int64
	if err := query.QueryRowContext(ctx, `SELECT COUNT(*) FROM content_references reference
		LEFT JOIN content_objects content ON content.content_id = reference.content_id
		WHERE reference.resident_id = ? AND (
		  content.content_id IS NULL OR content.owner_resident_id != reference.resident_id OR
		  (content.erasure_state = 'present' AND (
		    reference.dedupe_scope_id IS NULL OR reference.blob_hash_algorithm IS NULL OR reference.blob_hash IS NULL OR
		    reference.dedupe_scope_id != content.owner_resident_id OR
		    reference.blob_hash_algorithm != content.blob_hash_algorithm OR reference.blob_hash != content.blob_hash
		  )) OR
		  (content.erasure_state = 'erased' AND (
		    reference.dedupe_scope_id IS NOT NULL OR reference.blob_hash_algorithm IS NOT NULL OR reference.blob_hash IS NOT NULL
		  ))
		)`, residentID.String()).Scan(&invalidRows); err != nil {
		return fmt.Errorf("%w: inspect content references body", projection.ErrProjectionNotCurrent)
	}
	if invalidRows != 0 {
		return fmt.Errorf("%w: content references body has invalid locator bindings", projection.ErrProjectionNotCurrent)
	}
	if err := requireContentReferenceSelfRows(ctx, query, residentID); err != nil {
		return err
	}
	return requireExactContentReferenceBody(ctx, query, residentID)
}

// requireExactContentReferenceBody compares the persisted Projection with the
// complete code-owned direct-reference catalog in both directions. Locator
// validity and content_object self rows alone are insufficient: a forged
// current watermark must not make an omitted non-self reference (or an extra
// invented reference) authoritative GC evidence.
func requireExactContentReferenceBody(ctx context.Context, query contentReferenceGateQuerier, residentID canonical.ID) error {
	selects := make([]string, 0, len(contentref.DirectDescriptors()))
	args := make([]any, 0, len(contentref.DirectDescriptors())*3)
	for _, descriptor := range contentref.DirectDescriptors() {
		selects = append(selects, fmt.Sprintf(`SELECT
			ref.%s AS content_id,
			co.owner_resident_id AS resident_id,
			? AS referrer_kind,
			ref.%s AS referrer_id,
			? AS referrer_field,
			CASE WHEN co.erasure_state = 'present' THEN co.owner_resident_id ELSE NULL END AS dedupe_scope_id,
			CASE WHEN co.erasure_state = 'present' THEN co.blob_hash_algorithm ELSE NULL END AS blob_hash_algorithm,
			CASE WHEN co.erasure_state = 'present' THEN co.blob_hash ELSE NULL END AS blob_hash
		FROM %s ref
		JOIN content_objects co ON co.content_id = ref.%s
		WHERE ref.%s IS NOT NULL AND co.owner_resident_id = ?`,
			descriptor.ContentField, descriptor.PrimaryKey, descriptor.Table,
			descriptor.ContentField, descriptor.ContentField))
		args = append(args, descriptor.ReferrerKind, descriptor.ReferrerField, residentID.String())
	}
	expected := strings.Join(selects, " UNION ALL ")
	keyJoin := `actual.content_id = expected.content_id
		AND actual.resident_id = expected.resident_id
		AND actual.referrer_kind = expected.referrer_kind
		AND actual.referrer_id = expected.referrer_id
		AND actual.referrer_field = expected.referrer_field`

	var missingOrMismatched int64
	missingQuery := `WITH expected AS (` + expected + `)
		SELECT COUNT(*) FROM expected
		LEFT JOIN content_references actual ON ` + keyJoin + `
		WHERE actual.content_id IS NULL
		   OR NOT (actual.dedupe_scope_id IS expected.dedupe_scope_id
		       AND actual.blob_hash_algorithm IS expected.blob_hash_algorithm
		       AND actual.blob_hash IS expected.blob_hash)`
	if err := query.QueryRowContext(ctx, missingQuery, args...).Scan(&missingOrMismatched); err != nil {
		return fmt.Errorf("%w: compare authoritative content references", projection.ErrProjectionNotCurrent)
	}
	if missingOrMismatched != 0 {
		return fmt.Errorf("%w: content references omit or alter authoritative rows", projection.ErrProjectionNotCurrent)
	}

	var extra int64
	extraQuery := `WITH expected AS (` + expected + `)
		SELECT COUNT(*) FROM content_references actual
		LEFT JOIN expected ON ` + keyJoin + `
		WHERE actual.resident_id = ? AND expected.content_id IS NULL`
	extraArgs := append(append([]any(nil), args...), residentID.String())
	if err := query.QueryRowContext(ctx, extraQuery, extraArgs...).Scan(&extra); err != nil {
		return fmt.Errorf("%w: compare reverse content references", projection.ErrProjectionNotCurrent)
	}
	if extra != 0 {
		return fmt.Errorf("%w: content references contain non-authoritative rows", projection.ErrProjectionNotCurrent)
	}
	return nil
}

// requireContentReferenceSelfRows enforces the exact bidirectional locator
// bridge used by GC. A current watermark without this body invariant is not
// current reachability evidence.
func requireContentReferenceSelfRows(ctx context.Context, query contentReferenceGateQuerier, residentID canonical.ID) error {
	var missingOrMismatched int64
	if err := query.QueryRowContext(ctx, `SELECT COUNT(*) FROM content_objects content
		LEFT JOIN content_references reference
		  ON reference.content_id = content.content_id
		 AND reference.referrer_kind = 'content_object'
		 AND reference.referrer_id = content.content_id
		 AND reference.referrer_field = 'blob'
		WHERE content.owner_resident_id = ? AND (
		  reference.content_id IS NULL OR reference.resident_id != content.owner_resident_id OR
		  (content.erasure_state = 'present' AND (
		    reference.dedupe_scope_id IS NULL OR reference.blob_hash_algorithm IS NULL OR reference.blob_hash IS NULL OR
		    reference.dedupe_scope_id != content.owner_resident_id OR
		    reference.blob_hash_algorithm != content.blob_hash_algorithm OR reference.blob_hash != content.blob_hash
		  )) OR
		  (content.erasure_state = 'erased' AND (
		    reference.dedupe_scope_id IS NOT NULL OR reference.blob_hash_algorithm IS NOT NULL OR reference.blob_hash IS NOT NULL
		  ))
		)`, residentID.String()).Scan(&missingOrMismatched); err != nil {
		return fmt.Errorf("%w: inspect content references self rows", projection.ErrProjectionNotCurrent)
	}
	if missingOrMismatched != 0 {
		return fmt.Errorf("%w: content references self rows are incomplete", projection.ErrProjectionNotCurrent)
	}

	var extraOrMismatched int64
	if err := query.QueryRowContext(ctx, `SELECT COUNT(*) FROM content_references reference
		LEFT JOIN content_objects content ON content.content_id = reference.content_id
		WHERE reference.resident_id = ? AND reference.referrer_kind = 'content_object' AND (
		  reference.referrer_field != 'blob' OR reference.referrer_id != reference.content_id OR
		  content.content_id IS NULL OR content.owner_resident_id != reference.resident_id OR
		  (content.erasure_state = 'present' AND (
		    reference.dedupe_scope_id IS NULL OR reference.blob_hash_algorithm IS NULL OR reference.blob_hash IS NULL OR
		    reference.dedupe_scope_id != content.owner_resident_id OR
		    reference.blob_hash_algorithm != content.blob_hash_algorithm OR reference.blob_hash != content.blob_hash
		  )) OR
		  (content.erasure_state = 'erased' AND (
		    reference.dedupe_scope_id IS NOT NULL OR reference.blob_hash_algorithm IS NOT NULL OR reference.blob_hash IS NOT NULL
		  ))
		)`, residentID.String()).Scan(&extraOrMismatched); err != nil {
		return fmt.Errorf("%w: inspect reverse content references self rows", projection.ErrProjectionNotCurrent)
	}
	if extraOrMismatched != 0 {
		return fmt.Errorf("%w: content references contain extra or mismatched self rows", projection.ErrProjectionNotCurrent)
	}
	return nil
}
