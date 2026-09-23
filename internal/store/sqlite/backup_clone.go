package sqlite

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"strings"

	"mahoroba.local/mahoroba/internal/canonical"
	"mahoroba.local/mahoroba/internal/projection"
	"mahoroba.local/mahoroba/internal/readiness"
)

type BackupCloneOptions struct {
	IncludeProjections bool
}

type BackupBlob struct {
	ResidentID canonical.ID
	Digest     canonical.Digest
	ByteSize   int64
	Content    []byte
}

type BackupCloneMetadata struct {
	SchemaReport          SchemaReport
	Head                  readiness.Head
	ActiveResidentID      *canonical.ID
	SessionPolicyID       *canonical.ID
	SelectionSource       string
	ServiceReady          bool
	RequiredActions       map[string][]canonical.ID
	ProjectionDefinitions []projection.Definition
	Blobs                 []BackupBlob
}

// PrepareBackupClone removes runtime-delivery state and optional derived state
// only from an already-created private snapshot, then compacts and closes it.
// It never opens or mutates the live source database.
func PrepareBackupClone(ctx context.Context, path string, options BackupCloneOptions) (_ BackupCloneMetadata, resultErr error) {
	if ctx == nil {
		return BackupCloneMetadata{}, errors.New("sqlite: nil backup clone context")
	}
	normalized, err := normalizeLocalPath(path)
	if err != nil {
		return BackupCloneMetadata{}, err
	}
	db, err := openWriter(ctx, normalized, DefaultOptions())
	if err != nil {
		return BackupCloneMetadata{}, fmt.Errorf("sqlite: open backup clone: %w", err)
	}
	defer func() {
		if db != nil {
			resultErr = errors.Join(resultErr, db.Close())
		}
	}()
	if _, err := ValidateSchema(ctx, db); err != nil {
		return BackupCloneMetadata{}, fmt.Errorf("sqlite: backup clone schema gate: %w", err)
	}

	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return BackupCloneMetadata{}, fmt.Errorf("sqlite: begin backup clone preparation: %w", err)
	}
	committed := false
	defer func() {
		if !committed {
			_ = tx.Rollback()
		}
	}()

	selectionSource, err := resolveBackupCloneSelection(ctx, tx)
	if err != nil {
		return BackupCloneMetadata{}, err
	}
	if _, err := tx.ExecContext(ctx, "DELETE FROM analytics_outbox"); err != nil {
		return BackupCloneMetadata{}, fmt.Errorf("sqlite: exclude analytics outbox: %w", err)
	}
	if _, err := tx.ExecContext(ctx, "DELETE FROM search_outbox"); err != nil {
		return BackupCloneMetadata{}, fmt.Errorf("sqlite: exclude search outbox: %w", err)
	}
	if err := pruneBackupProjections(ctx, tx, options.IncludeProjections); err != nil {
		return BackupCloneMetadata{}, err
	}
	if _, err := tx.ExecContext(ctx, `DELETE FROM blobs
		WHERE NOT EXISTS (
			SELECT 1 FROM content_objects content
			WHERE content.erasure_state = 'present'
			  AND content.owner_resident_id = blobs.dedupe_scope_id
			  AND content.blob_hash_algorithm = blobs.hash_algorithm
			  AND content.blob_hash = blobs.blob_hash
		)`); err != nil {
		return BackupCloneMetadata{}, fmt.Errorf("sqlite: prune unreferenced backup blobs: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return BackupCloneMetadata{}, fmt.Errorf("sqlite: commit backup clone preparation: %w", err)
	}
	committed = true

	if _, err := db.ExecContext(ctx, "VACUUM"); err != nil {
		return BackupCloneMetadata{}, fmt.Errorf("sqlite: compact backup clone: %w", err)
	}
	if _, err := db.ExecContext(ctx, "PRAGMA wal_checkpoint(TRUNCATE)"); err != nil {
		return BackupCloneMetadata{}, fmt.Errorf("sqlite: checkpoint backup clone: %w", err)
	}
	var journalMode string
	if err := db.QueryRowContext(ctx, "PRAGMA journal_mode=DELETE").Scan(&journalMode); err != nil {
		return BackupCloneMetadata{}, fmt.Errorf("sqlite: seal backup clone journal mode: %w", err)
	}
	if !strings.EqualFold(journalMode, "delete") {
		return BackupCloneMetadata{}, fmt.Errorf("sqlite: sealed backup clone journal mode = %q, want delete", journalMode)
	}
	if err := db.Close(); err != nil {
		return BackupCloneMetadata{}, fmt.Errorf("sqlite: close prepared backup clone: %w", err)
	}
	db = nil
	for _, suffix := range []string{"-wal", "-shm"} {
		if _, err := os.Lstat(normalized + suffix); err == nil {
			return BackupCloneMetadata{}, fmt.Errorf("sqlite: prepared backup clone retains %s", suffix)
		} else if !errors.Is(err, os.ErrNotExist) {
			return BackupCloneMetadata{}, fmt.Errorf("sqlite: inspect backup clone sidecar: %w", err)
		}
	}
	return InspectBackupClone(ctx, normalized, options.IncludeProjections, selectionSource)
}

func resolveBackupCloneSelection(ctx context.Context, tx *sql.Tx) (string, error) {
	residentID, err := captureReadinessSelection(ctx, tx)
	if err != nil {
		return "", err
	}
	if residentID == nil {
		return "unresolved", nil
	}
	var configured sql.NullString
	if err := tx.QueryRowContext(ctx, `SELECT desired_sessionization_policy_version_id
		FROM runtime_config WHERE singleton_id = 1 AND active_resident_id = ?`, residentID.String()).Scan(&configured); err != nil {
		return "", fmt.Errorf("sqlite: inspect backup runtime selection: %w", err)
	}
	if configured.Valid {
		policy, err := loadValidReadinessSessionPolicy(ctx, tx, configured.String)
		if err != nil {
			return "", err
		}
		if policy == nil {
			return "unresolved", nil
		}
		return "runtime_config", nil
	}
	policy, err := resolveReadinessSessionPolicy(ctx, tx, *residentID)
	if err != nil {
		return "", err
	}
	if policy == nil {
		return "unresolved", nil
	}
	if _, err := tx.ExecContext(ctx, `UPDATE runtime_config
		SET desired_sessionization_policy_version_id = ?
		WHERE singleton_id = 1 AND active_resident_id = ?
		  AND desired_sessionization_policy_version_id IS NULL`, policy.String(), residentID.String()); err != nil {
		return "", fmt.Errorf("sqlite: persist backup-only session selection: %w", err)
	}
	return "projection_watermark", nil
}

func pruneBackupProjections(ctx context.Context, tx *sql.Tx, include bool) error {
	definitions := activeProjectionDefinitions
	if !include {
		for _, table := range []string{
			"projection_watermark_dependencies", "projection_watermarks", "content_references",
			"claim_view_scope_current", "claim_states", "runtime_states", "resident_current_revision", "resident_current_status",
		} {
			if _, err := tx.ExecContext(ctx, "DELETE FROM "+table); err != nil {
				return fmt.Errorf("sqlite: exclude backup projection %s: %w", table, err)
			}
		}
		return nil
	}
	names := make([]string, 0, len(definitions))
	for _, definition := range definitions {
		names = append(names, string(definition.Name))
	}
	placeholders := strings.TrimSuffix(strings.Repeat("?,", len(names)), ",")
	arguments := make([]any, len(names))
	for index := range names {
		arguments[index] = names[index]
	}
	if _, err := tx.ExecContext(ctx, "DELETE FROM projection_watermark_dependencies WHERE projection_name NOT IN ("+placeholders+")", arguments...); err != nil {
		return fmt.Errorf("sqlite: prune non-production projection dependencies: %w", err)
	}
	if _, err := tx.ExecContext(ctx, "DELETE FROM projection_watermarks WHERE projection_name NOT IN ("+placeholders+")", arguments...); err != nil {
		return fmt.Errorf("sqlite: prune non-production projection watermarks: %w", err)
	}
	return nil
}

// InspectBackupClone validates one closed clone and captures all manifest and
// filesystem-copy inputs from a read-only connection.
func InspectBackupClone(ctx context.Context, path string, includeProjections bool, selectionSource string) (_ BackupCloneMetadata, resultErr error) {
	inspection, err := OpenImmutableInspection(ctx, path)
	if err != nil {
		return BackupCloneMetadata{}, err
	}
	defer func() { resultErr = errors.Join(resultErr, inspection.Close()) }()
	return InspectBackupCloneInspection(ctx, inspection, includeProjections, selectionSource)
}

// InspectBackupCloneInspection captures manifest and filesystem-copy inputs
// through an already-open immutable inspection. The caller retains ownership
// of inspection, allowing metadata and MinimumCheck to consume one bound
// snapshot authority without a namespace reopen.
func InspectBackupCloneInspection(ctx context.Context, inspection *Inspection, includeProjections bool, selectionSource string) (_ BackupCloneMetadata, err error) {
	if ctx == nil {
		return BackupCloneMetadata{}, errors.New("sqlite: nil backup clone inspection context")
	}
	if inspection == nil || inspection.store == nil || inspection.store.reader == nil {
		return BackupCloneMetadata{}, errors.New("sqlite: immutable backup clone inspection is required")
	}
	reader := inspection.store.reader
	result := BackupCloneMetadata{SchemaReport: inspection.SchemaReport(), SelectionSource: selectionSource, RequiredActions: make(map[string][]canonical.ID)}
	result.Head, err = captureBackupHead(ctx, reader)
	if err != nil {
		return BackupCloneMetadata{}, err
	}
	snapshot, err := captureBackupReadiness(ctx, inspection)
	if err != nil {
		return BackupCloneMetadata{}, err
	}
	result.ActiveResidentID = cloneBackupID(snapshot.ActiveResidentID)
	result.SessionPolicyID = cloneBackupID(snapshot.SessionPolicyID)
	if result.ActiveResidentID == nil {
		result.RequiredActions["select_active_resident"] = []canonical.ID{}
	} else if result.SessionPolicyID == nil {
		result.RequiredActions["select_sessionization_policy"] = []canonical.ID{*result.ActiveResidentID}
	}
	if len(snapshot.BlockingPredicates) != 0 {
		result.RequiredActions["repair_integrity"] = []canonical.ID{}
	}
	projectionCurrent := false
	if result.ActiveResidentID != nil && includeProjections {
		projectionCurrent, err = backupProjectionsCurrent(ctx, reader, snapshot)
		if err != nil {
			return BackupCloneMetadata{}, err
		}
	}
	if result.ActiveResidentID != nil && !projectionCurrent {
		result.RequiredActions["rebuild_projection"] = []canonical.ID{*result.ActiveResidentID}
	}
	result.ServiceReady = result.Head.Exists && result.ActiveResidentID != nil && snapshot.ActiveResidentStatus == "active" &&
		result.SessionPolicyID != nil && len(snapshot.BlockingPredicates) == 0 && projectionCurrent
	if includeProjections {
		result.ProjectionDefinitions = append([]projection.Definition(nil), activeProjectionDefinitions...)
	}
	result.Blobs, err = loadBackupBlobs(ctx, reader)
	if err != nil {
		return BackupCloneMetadata{}, err
	}
	return result, nil
}

func captureBackupReadiness(ctx context.Context, inspection *Inspection) (readiness.Snapshot, error) {
	source, ok := inspection.ServiceReadinessSource().(*serviceReadinessSource)
	if !ok {
		return readiness.Snapshot{}, errors.New("sqlite: internal readiness source differs")
	}
	return source.CaptureServiceReadiness(ctx)
}

func captureBackupHead(ctx context.Context, db *sql.DB) (readiness.Head, error) {
	var head readiness.Head
	var id string
	var sequence, committedAt int64
	var timezone string
	err := db.QueryRowContext(ctx, `SELECT canonical_commit_id, commit_seq, committed_at, committed_tz
		FROM canonical_commits ORDER BY commit_seq DESC LIMIT 1`).Scan(&id, &sequence, &committedAt, &timezone)
	if errors.Is(err, sql.ErrNoRows) {
		return head, nil
	}
	if err != nil {
		return head, fmt.Errorf("sqlite: capture backup head: %w", err)
	}
	head.CommitID, err = canonical.ParseID(id)
	if err != nil {
		return readiness.Head{}, err
	}
	head.CommitSeq, err = canonical.NewCommitSeq(sequence)
	if err != nil {
		return readiness.Head{}, err
	}
	head.CommittedAt = canonical.Instant(committedAt)
	head.CommittedTZ, err = canonical.ParseTimezone(timezone)
	if err != nil {
		return readiness.Head{}, err
	}
	head.Exists = true
	return head, head.Validate()
}

func backupProjectionsCurrent(ctx context.Context, db *sql.DB, snapshot readiness.Snapshot) (bool, error) {
	if snapshot.ActiveResidentID == nil || !snapshot.CapturedHead.Exists {
		return false, nil
	}
	for _, definition := range activeProjectionDefinitions {
		var version string
		var sequence int64
		err := db.QueryRowContext(ctx, `SELECT projection_version, source_commit_seq
			FROM projection_watermarks WHERE projection_name = ? AND resident_id = ?`,
			definition.Name, snapshot.ActiveResidentID.String()).Scan(&version, &sequence)
		if errors.Is(err, sql.ErrNoRows) {
			return false, nil
		}
		if err != nil {
			return false, fmt.Errorf("sqlite: inspect backup projection currentness: %w", err)
		}
		if version != string(definition.Version) || sequence != snapshot.CapturedHead.CommitSeq.Int64() {
			return false, nil
		}
	}
	return true, nil
}

func loadBackupBlobs(ctx context.Context, db *sql.DB) ([]BackupBlob, error) {
	rows, err := db.QueryContext(ctx, `SELECT dedupe_scope_id, blob_hash, byte_size, content
		FROM blobs ORDER BY dedupe_scope_id, blob_hash`)
	if err != nil {
		return nil, fmt.Errorf("sqlite: enumerate backup blobs: %w", err)
	}
	defer rows.Close()
	var result []BackupBlob
	for rows.Next() {
		var rawResident string
		var rawDigest, content []byte
		var size int64
		if err := rows.Scan(&rawResident, &rawDigest, &size, &content); err != nil {
			return nil, fmt.Errorf("sqlite: scan backup blob: %w", err)
		}
		residentID, err := canonical.ParseID(rawResident)
		if err != nil {
			return nil, err
		}
		digest, err := canonical.DigestFromBytes(rawDigest)
		if err != nil {
			return nil, err
		}
		if size != int64(len(content)) || canonical.HashBlob(content) != digest {
			return nil, errors.New("sqlite: backup blob bytes differ from declared identity")
		}
		result = append(result, BackupBlob{ResidentID: residentID, Digest: digest, ByteSize: size, Content: append([]byte(nil), content...)})
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("sqlite: iterate backup blobs: %w", err)
	}
	return result, nil
}

func cloneBackupID(value *canonical.ID) *canonical.ID {
	if value == nil {
		return nil
	}
	copy := *value
	return &copy
}
