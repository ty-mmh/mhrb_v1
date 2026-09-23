package sqlite

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"fmt"
	"slices"
	"strings"

	"mahoroba.local/mahoroba/internal/blob"
	"mahoroba.local/mahoroba/internal/blobgc"
	"mahoroba.local/mahoroba/internal/canonical"
)

// BlobGCRepository exposes only the snapshot and Operational transaction
// primitives required by physical GC.
type BlobGCRepository struct{ store *Store }

func (store *Store) BlobGC() *BlobGCRepository { return &BlobGCRepository{store: store} }

// BlobGC exposes the same captured snapshot through an inspection handle.
// BeginCandidate and Maintenance remain unavailable because Inspection has no
// writer; this is the production default dry-run boundary.
func (inspection *Inspection) BlobGC() *BlobGCRepository {
	if inspection == nil {
		return &BlobGCRepository{}
	}
	return &BlobGCRepository{store: inspection.store}
}

func (repository *BlobGCRepository) Capture(
	ctx context.Context,
	residentID canonical.ID,
	filesystem []blob.FinalObject,
) (_ blobgc.Snapshot, resultErr error) {
	var result blobgc.Snapshot
	if repository == nil || repository.store == nil || repository.store.reader == nil {
		return result, fmt.Errorf("%w: SQLite repository is unavailable", blobgc.ErrSourceUnavailable)
	}
	if ctx == nil {
		return result, fmt.Errorf("%w: nil snapshot context", blobgc.ErrSourceUnavailable)
	}
	if err := residentID.Validate(); err != nil {
		return result, err
	}
	tx, err := repository.store.reader.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return result, fmt.Errorf("%w: begin read snapshot: %v", blobgc.ErrSourceUnavailable, err)
	}
	defer func() {
		if rollbackErr := tx.Rollback(); rollbackErr != nil && !errors.Is(rollbackErr, sql.ErrTxDone) {
			resultErr = errors.Join(resultErr, rollbackErr)
		}
	}()

	head, err := captureBlobGCHead(ctx, tx)
	if err != nil {
		return result, err
	}
	if !head.Exists {
		return result, fmt.Errorf("%w: empty Canonical head", blobgc.ErrProjectionNotCurrent)
	}
	if err := requireBlobGCWatermark(ctx, tx, residentID, head); err != nil {
		return result, err
	}
	states := make(map[canonical.Digest]*blobgc.LocatorState)
	rows, err := tx.QueryContext(ctx, `SELECT hash_algorithm, blob_hash, content, byte_size
		FROM blobs WHERE dedupe_scope_id = ? ORDER BY hash_algorithm, blob_hash`, residentID.String())
	if err != nil {
		return result, fmt.Errorf("%w: read SQLite blobs: %v", blobgc.ErrSourceUnavailable, err)
	}
	for rows.Next() {
		var algorithm string
		var rawDigest, content []byte
		var storedSize int64
		if err := rows.Scan(&algorithm, &rawDigest, &content, &storedSize); err != nil {
			_ = rows.Close()
			return result, fmt.Errorf("%w: scan SQLite blob: %v", blobgc.ErrSourceUnavailable, err)
		}
		digest, err := canonical.DigestFromBytes(rawDigest)
		if err != nil || algorithm != canonical.HashAlgorithm {
			_ = rows.Close()
			return result, fmt.Errorf("%w: invalid SQLite blob locator", blobgc.ErrContentIntegrity)
		}
		actual, actualSize, err := canonical.HashBlobReader(bytes.NewReader(content))
		byteSize, sizeErr := canonical.NewByteSize(storedSize)
		if err != nil || sizeErr != nil || actual != digest || actualSize != storedSize {
			_ = rows.Close()
			return result, fmt.Errorf("%w: SQLite blob hash or size mismatch", blobgc.ErrContentIntegrity)
		}
		if _, duplicate := states[digest]; duplicate {
			_ = rows.Close()
			return result, fmt.Errorf("%w: duplicate SQLite locator", blobgc.ErrContentIntegrity)
		}
		states[digest] = &blobgc.LocatorState{
			ResidentID: residentID, HashAlgorithm: algorithm, Digest: digest,
			SQLitePresent: true, SQLiteByteSize: &byteSize,
		}
	}
	if err := rows.Err(); err != nil {
		_ = rows.Close()
		return result, fmt.Errorf("%w: iterate SQLite blobs: %v", blobgc.ErrSourceUnavailable, err)
	}
	if err := rows.Close(); err != nil {
		return result, fmt.Errorf("%w: close SQLite blob rows: %v", blobgc.ErrSourceUnavailable, err)
	}
	for _, object := range filesystem {
		if object.ResidentID() != residentID || object.HashAlgorithm() != canonical.HashAlgorithm {
			return result, fmt.Errorf("%w: cross-scope filesystem locator", blobgc.ErrContentIntegrity)
		}
		if _, exists := states[object.Digest()]; !exists {
			states[object.Digest()] = &blobgc.LocatorState{
				ResidentID: residentID, HashAlgorithm: canonical.HashAlgorithm, Digest: object.Digest(),
			}
		}
	}
	if err := attachBlobGCReferenceCounts(ctx, tx, residentID, states); err != nil {
		return result, err
	}
	locators := make([]blobgc.LocatorState, 0, len(states))
	for _, state := range states {
		locators = append(locators, *state)
	}
	slices.SortFunc(locators, func(left, right blobgc.LocatorState) int {
		return strings.Compare(left.Digest.Hex(), right.Digest.Hex())
	})
	result = blobgc.Snapshot{
		CapturedHead: head, ProjectionName: blobgc.ProjectionName,
		ProjectionVersion:  blobgc.ProjectionVersion,
		DependencyVersions: []blobgc.DependencyVersion{}, Locators: locators,
	}
	if err := tx.Commit(); err != nil {
		return blobgc.Snapshot{}, fmt.Errorf("%w: close read snapshot: %v", blobgc.ErrSourceUnavailable, err)
	}
	return result, nil
}

type blobGCQuerier interface {
	QueryRowContext(context.Context, string, ...any) *sql.Row
	QueryContext(context.Context, string, ...any) (*sql.Rows, error)
}

func captureBlobGCHead(ctx context.Context, query blobGCQuerier) (blobgc.CapturedHead, error) {
	var rawID, rawTimezone string
	var rawSeq, rawTime int64
	err := query.QueryRowContext(ctx, `SELECT canonical_commit_id, commit_seq, committed_at, committed_tz
		FROM canonical_commits ORDER BY commit_seq DESC LIMIT 1`).Scan(&rawID, &rawSeq, &rawTime, &rawTimezone)
	if errors.Is(err, sql.ErrNoRows) {
		return blobgc.CapturedHead{}, nil
	}
	if err != nil {
		return blobgc.CapturedHead{}, fmt.Errorf("%w: capture Canonical head: %v", blobgc.ErrSourceUnavailable, err)
	}
	commitID, err := canonical.ParseID(rawID)
	if err != nil {
		return blobgc.CapturedHead{}, fmt.Errorf("%w: invalid head identity", blobgc.ErrContentIntegrity)
	}
	commitSeq, err := canonical.NewCommitSeq(rawSeq)
	if err != nil {
		return blobgc.CapturedHead{}, fmt.Errorf("%w: invalid head sequence", blobgc.ErrContentIntegrity)
	}
	timezone, err := canonical.ParseTimezone(rawTimezone)
	if err != nil {
		return blobgc.CapturedHead{}, fmt.Errorf("%w: invalid head timezone", blobgc.ErrContentIntegrity)
	}
	committedAt := canonical.Instant(rawTime)
	return blobgc.CapturedHead{
		Exists: true, CommitID: &commitID, CommitSeq: &commitSeq,
		CommittedAt: &committedAt, CommittedTZ: &timezone,
	}, nil
}

func requireBlobGCWatermark(ctx context.Context, query blobGCQuerier, residentID canonical.ID, head blobgc.CapturedHead) error {
	var version string
	var sourceSeq int64
	err := query.QueryRowContext(ctx, `SELECT projection_version, source_commit_seq
		FROM projection_watermarks WHERE projection_name = ? AND resident_id = ?`,
		blobgc.ProjectionName, residentID.String()).Scan(&version, &sourceSeq)
	if errors.Is(err, sql.ErrNoRows) {
		return fmt.Errorf("%w: watermark is missing", blobgc.ErrProjectionNotCurrent)
	}
	if err != nil {
		return fmt.Errorf("%w: read watermark: %v", blobgc.ErrProjectionNotCurrent, err)
	}
	if version != blobgc.ProjectionVersion || head.CommitSeq == nil || sourceSeq != head.CommitSeq.Int64() {
		return fmt.Errorf("%w: watermark version or head differs", blobgc.ErrProjectionNotCurrent)
	}
	var dependencyCount int64
	if err := query.QueryRowContext(ctx, `SELECT COUNT(*) FROM projection_watermark_dependencies
		WHERE projection_name = ? AND resident_id = ?`, blobgc.ProjectionName, residentID.String()).Scan(&dependencyCount); err != nil {
		return fmt.Errorf("%w: read watermark dependencies: %v", blobgc.ErrProjectionNotCurrent, err)
	}
	if dependencyCount != 0 {
		return fmt.Errorf("%w: v1 dependency set is not empty", blobgc.ErrProjectionNotCurrent)
	}
	if err := requireContentReferenceBody(ctx, query, residentID); err != nil {
		return fmt.Errorf("%w: Projection body differs", blobgc.ErrProjectionNotCurrent)
	}
	return nil
}

func attachBlobGCReferenceCounts(
	ctx context.Context,
	query blobGCQuerier,
	residentID canonical.ID,
	states map[canonical.Digest]*blobgc.LocatorState,
) error {
	for _, descriptor := range []struct {
		query string
		apply func(*blobgc.LocatorState, int64)
	}{
		{`SELECT blob_hash, COUNT(*) FROM content_objects
			WHERE owner_resident_id = ? AND erasure_state = 'present' AND blob_hash_algorithm = 'sha256'
			GROUP BY blob_hash ORDER BY blob_hash`, func(state *blobgc.LocatorState, count int64) { state.AuthoritativeReferences = count }},
		{`SELECT blob_hash, COUNT(*) FROM content_references
			WHERE resident_id = ? AND dedupe_scope_id = ? AND blob_hash_algorithm = 'sha256' AND blob_hash IS NOT NULL
			GROUP BY blob_hash ORDER BY blob_hash`, func(state *blobgc.LocatorState, count int64) { state.ProjectionReferences = count }},
	} {
		args := []any{residentID.String()}
		if strings.Contains(descriptor.query, "dedupe_scope_id = ?") {
			args = append(args, residentID.String())
		}
		rows, err := query.QueryContext(ctx, descriptor.query, args...)
		if err != nil {
			return fmt.Errorf("%w: read reference counts: %v", blobgc.ErrSourceUnavailable, err)
		}
		for rows.Next() {
			var raw []byte
			var count int64
			if err := rows.Scan(&raw, &count); err != nil {
				_ = rows.Close()
				return fmt.Errorf("%w: scan reference count: %v", blobgc.ErrSourceUnavailable, err)
			}
			digest, err := canonical.DigestFromBytes(raw)
			if err != nil || count <= 0 {
				_ = rows.Close()
				return fmt.Errorf("%w: invalid reference locator/count", blobgc.ErrContentIntegrity)
			}
			if state := states[digest]; state != nil {
				descriptor.apply(state, count)
			}
		}
		if err := rows.Err(); err != nil {
			_ = rows.Close()
			return fmt.Errorf("%w: iterate reference counts: %v", blobgc.ErrSourceUnavailable, err)
		}
		if err := rows.Close(); err != nil {
			return fmt.Errorf("%w: close reference counts: %v", blobgc.ErrSourceUnavailable, err)
		}
	}
	return nil
}

type blobGCCandidateTx struct {
	tx        *sql.Tx
	release   func()
	candidate blobgc.Candidate
	closed    bool
}

func (repository *BlobGCRepository) BeginCandidate(
	ctx context.Context,
	candidate blobgc.Candidate,
	expectedHead blobgc.CapturedHead,
) (_ blobgc.CandidateTransaction, returnErr error) {
	if repository == nil || repository.store == nil || repository.store.writer == nil {
		return nil, fmt.Errorf("%w: SQLite repository is unavailable", blobgc.ErrSourceUnavailable)
	}
	if err := candidate.Validate(); err != nil {
		return nil, err
	}
	release, err := repository.store.writes.acquireHigh(ctx)
	if err != nil {
		return nil, err
	}
	tx, err := repository.store.writer.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelSerializable})
	if err != nil {
		release()
		return nil, err
	}
	fail := func(err error) (blobgc.CandidateTransaction, error) {
		return nil, errors.Join(err, tx.Rollback())
	}
	head, err := captureBlobGCHead(ctx, tx)
	if err != nil || !head.Equal(expectedHead) {
		txn, failure := fail(fmt.Errorf("%w: Canonical head changed", blobgc.ErrPlanStale))
		release()
		return txn, errors.Join(failure, err)
	}
	if err := requireBlobGCWatermark(ctx, tx, candidate.ResidentID, head); err != nil {
		txn, failure := fail(err)
		release()
		return txn, failure
	}
	authoritative, projected, err := blobGCZeroCounts(ctx, tx, candidate)
	if err != nil || authoritative != 0 || projected != 0 {
		txn, failure := fail(fmt.Errorf("%w: locator is referenced", blobgc.ErrPlanStale))
		release()
		return txn, errors.Join(failure, err)
	}
	var content []byte
	var byteSize int64
	err = tx.QueryRowContext(ctx, `SELECT content, byte_size FROM blobs
		WHERE dedupe_scope_id = ? AND hash_algorithm = ? AND blob_hash = ?`,
		candidate.ResidentID.String(), candidate.HashAlgorithm, candidate.Digest().Bytes()).Scan(&content, &byteSize)
	if candidate.SQLitePresent {
		if err != nil {
			txn, failure := fail(fmt.Errorf("%w: SQLite copy changed", blobgc.ErrPlanStale))
			release()
			return txn, errors.Join(failure, err)
		}
		actual, actualSize, hashErr := canonical.HashBlobReader(bytes.NewReader(content))
		if hashErr != nil || actual != candidate.Digest() || candidate.SQLiteByteSize == nil ||
			actualSize != byteSize || byteSize != candidate.SQLiteByteSize.Int64() {
			txn, failure := fail(fmt.Errorf("%w: SQLite content changed", blobgc.ErrContentIntegrity))
			release()
			return txn, errors.Join(failure, hashErr)
		}
	} else if !errors.Is(err, sql.ErrNoRows) {
		txn, failure := fail(fmt.Errorf("%w: SQLite presence changed", blobgc.ErrPlanStale))
		release()
		return txn, errors.Join(failure, err)
	}
	return &blobGCCandidateTx{tx: tx, release: release, candidate: candidate}, nil
}

func blobGCZeroCounts(ctx context.Context, query blobGCQuerier, candidate blobgc.Candidate) (int64, int64, error) {
	var authoritative, projected int64
	if err := query.QueryRowContext(ctx, `SELECT COUNT(*) FROM content_objects
		WHERE owner_resident_id = ? AND erasure_state = 'present' AND blob_hash_algorithm = ? AND blob_hash = ?`,
		candidate.ResidentID.String(), candidate.HashAlgorithm, candidate.Digest().Bytes()).Scan(&authoritative); err != nil {
		return 0, 0, err
	}
	if err := query.QueryRowContext(ctx, `SELECT COUNT(*) FROM content_references
		WHERE resident_id = ? AND dedupe_scope_id = ? AND blob_hash_algorithm = ? AND blob_hash = ?`,
		candidate.ResidentID.String(), candidate.ResidentID.String(), candidate.HashAlgorithm, candidate.Digest().Bytes()).Scan(&projected); err != nil {
		return 0, 0, err
	}
	return authoritative, projected, nil
}

func (transaction *blobGCCandidateTx) DeleteSQLite(ctx context.Context) (bool, error) {
	if transaction == nil || transaction.tx == nil || transaction.closed {
		return false, errors.New("sqlite: blob GC transaction is closed")
	}
	result, err := transaction.tx.ExecContext(ctx, `DELETE FROM blobs
		WHERE dedupe_scope_id = ? AND hash_algorithm = ? AND blob_hash = ?`,
		transaction.candidate.ResidentID.String(), transaction.candidate.HashAlgorithm,
		transaction.candidate.Digest().Bytes())
	if err != nil {
		return false, err
	}
	count, err := result.RowsAffected()
	if err != nil {
		return false, err
	}
	want := int64(0)
	if transaction.candidate.SQLitePresent {
		want = 1
	}
	if count != want {
		return false, blobgc.ErrPlanStale
	}
	return count == 1, nil
}

func (transaction *blobGCCandidateTx) Commit(context.Context) error {
	if transaction == nil || transaction.closed {
		return errors.New("sqlite: blob GC transaction is closed")
	}
	transaction.closed = true
	err := transaction.tx.Commit()
	transaction.release()
	return err
}

func (transaction *blobGCCandidateTx) Rollback(context.Context) error {
	if transaction == nil || transaction.closed {
		return nil
	}
	transaction.closed = true
	err := transaction.tx.Rollback()
	transaction.release()
	if errors.Is(err, sql.ErrTxDone) {
		return nil
	}
	return err
}

func (repository *BlobGCRepository) Maintenance(ctx context.Context) error {
	if repository == nil || repository.store == nil || repository.store.writer == nil {
		return blobgc.ErrSourceUnavailable
	}
	release, err := repository.store.writes.acquireHigh(ctx)
	if err != nil {
		return err
	}
	defer release()
	_, checkpointErr := repository.store.writer.ExecContext(ctx, `PRAGMA wal_checkpoint(TRUNCATE)`)
	_, vacuumErr := repository.store.writer.ExecContext(ctx, `PRAGMA incremental_vacuum`)
	return errors.Join(checkpointErr, vacuumErr)
}

var _ blobgc.Repository = (*BlobGCRepository)(nil)
var _ blobgc.CandidateTransaction = (*blobGCCandidateTx)(nil)
