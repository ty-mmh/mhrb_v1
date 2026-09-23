package sqlite

import (
	"context"
	"database/sql"
	"fmt"

	"mahoroba.local/mahoroba/internal/canonical"
	"mahoroba.local/mahoroba/internal/integrity"
)

// MinimumChecker returns the read-only fatal-invariant gate over Store's
// query-only connection pool.
func (s *Store) MinimumChecker() *integrity.MinimumChecker {
	return newSQLiteMinimumChecker(s.reader)
}

// MinimumCheckerWithBlobObjects includes the mandatory dual-copy verification
// used by runtime, backup, and restore publication gates.
func (s *Store) MinimumCheckerWithBlobObjects(objects integrity.BlobObjectReader) *integrity.MinimumChecker {
	return integrity.NewMinimumChecker(&minimumCheckSource{reader: s.reader}, integrity.WithBlobObjects(objects))
}

// MinimumChecker exposes the same read-only gate to offline verification and
// restore validation without making any writer surface reachable.
func (inspection *Inspection) MinimumChecker() *integrity.MinimumChecker {
	return newSQLiteMinimumChecker(inspection.store.reader)
}

// MinimumCheckerWithBlobObjects is the offline source/restore publication
// gate. Inspection remains query-only while the caller supplies the separate,
// handle-bound filesystem copy authority.
func (inspection *Inspection) MinimumCheckerWithBlobObjects(objects integrity.BlobObjectReader) *integrity.MinimumChecker {
	return integrity.NewMinimumChecker(&minimumCheckSource{reader: inspection.store.reader}, integrity.WithBlobObjects(objects))
}

// MinimumChecker exposes the same mutation-free fatal gate to the ungated
// diagnostics reader. Source/query failures remain caller-classified section
// failures; a completed fatal check never writes findings.
func (inspection *DiagnosticInspection) MinimumChecker() *integrity.MinimumChecker {
	if inspection == nil || inspection.store == nil {
		return integrity.NewMinimumChecker(nil)
	}
	return newSQLiteMinimumChecker(inspection.store.reader)
}

func newSQLiteMinimumChecker(reader *sql.DB) *integrity.MinimumChecker {
	return integrity.NewMinimumChecker(&minimumCheckSource{reader: reader})
}

type minimumCheckSource struct {
	reader *sql.DB
}

func (source *minimumCheckSource) LoadClaimStatementAliases(
	ctx context.Context,
) (_ integrity.ClaimStatementAliasSnapshot, resultErr error) {
	if source == nil || source.reader == nil {
		return integrity.ClaimStatementAliasSnapshot{}, fmt.Errorf("sqlite: MinimumCheck reader is required")
	}
	tx, err := source.reader.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return integrity.ClaimStatementAliasSnapshot{}, fmt.Errorf("sqlite: begin MinimumCheck snapshot: %w", err)
	}
	defer func() {
		if resultErr != nil {
			_ = tx.Rollback()
		}
	}()

	contents, err := loadClaimStatementContents(ctx, tx)
	if err != nil {
		return integrity.ClaimStatementAliasSnapshot{}, err
	}
	if err := loadStatementContentErasureEvents(ctx, tx, contents); err != nil {
		return integrity.ClaimStatementAliasSnapshot{}, err
	}
	pairs, err := loadClaimStatementErasureEvents(ctx, tx)
	if err != nil {
		return integrity.ClaimStatementAliasSnapshot{}, err
	}
	if err := tx.Commit(); err != nil {
		return integrity.ClaimStatementAliasSnapshot{}, fmt.Errorf("sqlite: finish MinimumCheck snapshot: %w", err)
	}
	return integrity.ClaimStatementAliasSnapshot{
		Contents:      contents,
		ErasureEvents: pairs,
	}, nil
}

func loadClaimStatementContents(
	ctx context.Context,
	tx *sql.Tx,
) ([]integrity.ClaimStatementContent, error) {
	rows, err := tx.QueryContext(ctx, `SELECT
		content.content_id,
		content.owner_resident_id,
		content.erasure_state,
		claim.claim_id,
		claim.owner_resident_id,
		claim.statement_hash IS NOT NULL
	FROM claims claim
	JOIN content_objects content ON content.content_id = claim.statement_content_id
	ORDER BY content.content_id, claim.claim_id`)
	if err != nil {
		return nil, fmt.Errorf("sqlite: load MinimumCheck claim aliases: %w", err)
	}
	defer rows.Close()

	contents := make([]integrity.ClaimStatementContent, 0)
	contentIndexes := make(map[string]int)
	for rows.Next() {
		var contentID, contentResidentID, state, claimID, claimResidentID string
		var statementHashIsSet int
		if err := rows.Scan(
			&contentID,
			&contentResidentID,
			&state,
			&claimID,
			&claimResidentID,
			&statementHashIsSet,
		); err != nil {
			return nil, fmt.Errorf("sqlite: scan MinimumCheck claim alias: %w", err)
		}
		index, exists := contentIndexes[contentID]
		if !exists {
			index = len(contents)
			contentIndexes[contentID] = index
			contents = append(contents, integrity.ClaimStatementContent{
				ContentID:       contentID,
				OwnerResidentID: contentResidentID,
				ErasureState:    state,
			})
		}
		contents[index].Claims = append(contents[index].Claims, integrity.ClaimStatementAlias{
			ClaimID:            claimID,
			OwnerResidentID:    claimResidentID,
			StatementHashIsSet: statementHashIsSet != 0,
		})
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("sqlite: iterate MinimumCheck claim aliases: %w", err)
	}
	return contents, nil
}

func loadStatementContentErasureEvents(
	ctx context.Context,
	tx *sql.Tx,
	contents []integrity.ClaimStatementContent,
) error {
	rows, err := tx.QueryContext(ctx, `SELECT
		event.content_erasure_event_id,
		event.content_id,
		event.canonical_commit_id,
		commit_row.resident_id,
		event.erasure_scope,
		event.recorded_at,
		event.recorded_tz,
		commit_row.committed_at,
		commit_row.committed_tz
	FROM content_erasure_events event
	LEFT JOIN canonical_commits commit_row
	  ON commit_row.canonical_commit_id = event.canonical_commit_id
	WHERE EXISTS (
		SELECT 1 FROM claims claim WHERE claim.statement_content_id = event.content_id
	)
	ORDER BY event.content_id, event.content_erasure_event_id`)
	if err != nil {
		return fmt.Errorf("sqlite: load MinimumCheck statement content erasure events: %w", err)
	}
	defer rows.Close()

	contentIndexes := make(map[string]int, len(contents))
	for index := range contents {
		contentIndexes[contents[index].ContentID] = index
	}
	for rows.Next() {
		var event integrity.ContentErasureEvent
		var commitResident sql.NullString
		var commitRecordedAt sql.NullInt64
		var commitRecordedTZ sql.NullString
		if err := rows.Scan(
			&event.EventID,
			&event.ContentID,
			&event.CanonicalCommitID,
			&commitResident,
			&event.ErasureScope,
			&event.RecordedAt,
			&event.RecordedTZ,
			&commitRecordedAt,
			&commitRecordedTZ,
		); err != nil {
			return fmt.Errorf("sqlite: scan MinimumCheck statement content erasure event: %w", err)
		}
		event.CommitResidentID = commitResident.String
		event.CommitRecordedAt = commitRecordedAt.Int64
		event.CommitRecordedTZ = commitRecordedTZ.String
		index, exists := contentIndexes[event.ContentID]
		if !exists {
			// EXISTS above makes this impossible for a stable snapshot. Preserve
			// it as a source failure instead of silently dropping an event.
			return fmt.Errorf("sqlite: statement content erasure event %s has no loaded alias group", event.EventID)
		}
		contents[index].ErasureEvents = append(contents[index].ErasureEvents, event)
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("sqlite: iterate MinimumCheck statement content erasure events: %w", err)
	}
	return nil
}

func loadClaimStatementErasureEvents(
	ctx context.Context,
	tx *sql.Tx,
) ([]integrity.ClaimStatementErasureEvent, error) {
	rows, err := tx.QueryContext(ctx, `SELECT
		claim_statement_erasure_event_id,
		claim_id,
		resident_id,
		canonical_commit_id,
		content_erasure_event_id,
		recorded_at,
		recorded_tz
	FROM claim_statement_erasure_events
	ORDER BY claim_statement_erasure_event_id`)
	if err != nil {
		return nil, fmt.Errorf("sqlite: load MinimumCheck claim statement erasure events: %w", err)
	}
	defer rows.Close()

	var events []integrity.ClaimStatementErasureEvent
	for rows.Next() {
		var event integrity.ClaimStatementErasureEvent
		if err := rows.Scan(
			&event.EventID,
			&event.ClaimID,
			&event.ResidentID,
			&event.CanonicalCommitID,
			&event.ContentErasureEventID,
			&event.RecordedAt,
			&event.RecordedTZ,
		); err != nil {
			return nil, fmt.Errorf("sqlite: scan MinimumCheck claim statement erasure event: %w", err)
		}
		events = append(events, event)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("sqlite: iterate MinimumCheck claim statement erasure events: %w", err)
	}
	return events, nil
}

var _ integrity.MinimumSource = (*minimumCheckSource)(nil)

func (source *minimumCheckSource) LoadPresentBlobCopies(ctx context.Context) ([]integrity.PresentBlobCopy, error) {
	if source == nil || source.reader == nil {
		return nil, fmt.Errorf("sqlite: MinimumCheck reader is required")
	}
	rows, err := source.reader.QueryContext(ctx, `SELECT
		content.content_id,
		content.owner_resident_id,
		content.blob_hash,
		blob.blob_hash IS NOT NULL,
		blob.content,
		blob.byte_size
	FROM content_objects content
	LEFT JOIN blobs blob
	  ON blob.dedupe_scope_id = content.owner_resident_id
	 AND blob.hash_algorithm = content.blob_hash_algorithm
	 AND blob.blob_hash = content.blob_hash
	WHERE content.erasure_state = 'present'
	ORDER BY content.owner_resident_id, content.content_id`)
	if err != nil {
		return nil, fmt.Errorf("sqlite: load MinimumCheck present blobs: %w", err)
	}
	defer rows.Close()
	var result []integrity.PresentBlobCopy
	for rows.Next() {
		var item integrity.PresentBlobCopy
		var residentRaw string
		var hashRaw, body []byte
		var found int
		var declaredSize sql.NullInt64
		if err := rows.Scan(&item.ContentID, &residentRaw, &hashRaw, &found, &body, &declaredSize); err != nil {
			return nil, fmt.Errorf("sqlite: scan MinimumCheck present blob: %w", err)
		}
		item.ResidentID, err = canonical.ParseID(residentRaw)
		if err != nil {
			return nil, fmt.Errorf("sqlite: parse MinimumCheck blob resident: %w", err)
		}
		item.ExpectedHash, err = canonical.DigestFromBytes(hashRaw)
		if err != nil {
			return nil, fmt.Errorf("sqlite: parse MinimumCheck blob identity: %w", err)
		}
		item.BlobFound = found != 0
		if item.BlobFound {
			if !declaredSize.Valid {
				return nil, fmt.Errorf("sqlite: present blob has no byte size")
			}
			item.DeclaredSize = declaredSize.Int64
			item.DatabaseBlob = append([]byte(nil), body...)
		}
		result = append(result, item)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("sqlite: iterate MinimumCheck present blobs: %w", err)
	}
	return result, nil
}

var _ integrity.PresentBlobSource = (*minimumCheckSource)(nil)
