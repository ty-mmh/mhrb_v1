// Package diagnostics implements the offline, read-only M7 diagnostics
// snapshot. It deliberately returns a complete closed result even when the
// target or database is absent or individual queries fail.
package diagnostics

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"

	"mahoroba.local/mahoroba/internal/blob"
	"mahoroba.local/mahoroba/internal/canonical"
	"mahoroba.local/mahoroba/internal/cliresult"
	"mahoroba.local/mahoroba/internal/descriptorpath"
	"mahoroba.local/mahoroba/internal/domain"
	"mahoroba.local/mahoroba/internal/durablepublish"
	"mahoroba.local/mahoroba/internal/fssecure"
	"mahoroba.local/mahoroba/internal/hostlock"
	"mahoroba.local/mahoroba/internal/integrity"
	"mahoroba.local/mahoroba/internal/namespacelock"
	"mahoroba.local/mahoroba/internal/projection"
	"mahoroba.local/mahoroba/internal/readiness"
	"mahoroba.local/mahoroba/internal/restore"
	storesqlite "mahoroba.local/mahoroba/internal/store/sqlite"
)

const (
	maximumDiagnosticWork = 100_000
	maximumMarkerBytes    = 16 << 10
)

var (
	errorTableUnavailable = "table_unavailable"
	errorQueryFailed      = "query_failed"
	errorIntegrity        = "integrity_violation"
	errorMarkerUnreadable = "marker_unreadable"
)

type Request struct {
	DataDir          string
	DatabaseFilename string
	BlobRoot         string
	ResidentID       *canonical.ID
	MaxAttempts      int
	Timezone         canonical.Timezone
	Projection       ProjectionSchedule
}

type ProjectionSchedule struct {
	Clock                canonical.Clock
	ScanInterval         time.Duration
	AsOfRefreshInterval  time.Duration
	RebuildRetryInterval time.Duration
	MaxStaleness         time.Duration
	Overrides            map[projection.Name]projection.ScheduleOverride
}

// Inspect captures one sanitized snapshot. Operational inspection failures
// are represented inside the 13 sections; errors are reserved for malformed
// caller input or failure to establish the namespace/host-lock protocol.
func Inspect(ctx context.Context, request Request) (*cliresult.DiagnosticsResult, error) {
	if ctx == nil {
		return nil, errors.New("diagnostics: nil context")
	}
	if !filepath.IsAbs(request.DataDir) || filepath.Clean(request.DataDir) != request.DataDir {
		return nil, errors.New("diagnostics: data directory must be canonical absolute")
	}
	if request.DatabaseFilename == "" || filepath.Base(request.DatabaseFilename) != request.DatabaseFilename {
		return nil, errors.New("diagnostics: database filename is invalid")
	}
	if request.BlobRoot == "" {
		request.BlobRoot = filepath.Join(request.DataDir, "blobs")
	}
	if !filepath.IsAbs(request.BlobRoot) || filepath.Clean(request.BlobRoot) != request.BlobRoot {
		return nil, errors.New("diagnostics: blob root must be canonical absolute")
	}
	if request.ResidentID != nil {
		if err := request.ResidentID.Validate(); err != nil {
			return nil, errors.New("diagnostics: resident scope is invalid")
		}
	}
	if request.MaxAttempts < 1 {
		return nil, errors.New("diagnostics: max attempts must be positive")
	}
	if request.Timezone == "" {
		request.Timezone = canonical.MustTimezone("UTC")
	}
	if err := request.Timezone.Validate(); err != nil {
		return nil, errors.New("diagnostics: timezone is invalid")
	}
	request.Projection = normalizeProjectionSchedule(request.Projection)

	result := unknownResult()
	namespace, err := namespacelock.AcquireExistingParent(request.DataDir)
	if err != nil {
		return nil, fmt.Errorf("diagnostics: acquire target namespace: %w", err)
	}
	defer namespace.Close()
	siblings, siblingErr := namespace.InspectDiagnosticSiblings(maximumMarkerBytes)
	exists, existsErr := namespace.TargetExists()
	if existsErr != nil {
		return nil, fmt.Errorf("diagnostics: inspect target: %w", existsErr)
	}
	if !exists {
		setPublishSection(result, inspectPublishState(ctx, request.DataDir, false, nil, siblings, siblingErr))
		deriveOverall(result)
		return result, nil
	}

	target, err := namespace.OpenExistingTarget()
	if err != nil {
		return nil, fmt.Errorf("diagnostics: bind target: %w", err)
	}
	defer target.Close()
	dataLock, err := hostlock.AcquireBoundTarget(target)
	if err != nil {
		return nil, fmt.Errorf("diagnostics: acquire host lock: %w", err)
	}
	defer dataLock.Close()
	internal := inspectTargetMarkers(target)
	setPublishSection(result, inspectPublishState(ctx, request.DataDir, true, internal, siblings, siblingErr))
	if err := namespace.VerifyBoundTarget(target); err != nil {
		return nil, fmt.Errorf("diagnostics: target namespace changed before database observation: %w", err)
	}
	database, err := target.OpenRegularRead(request.DatabaseFilename)
	if err != nil {
		deriveOverall(result)
		return result, nil
	}
	defer database.Close()
	databasePath, err := descriptorpath.ReadOnly(database.File(), filepath.Join(request.DataDir, request.DatabaseFilename))
	if err != nil {
		return nil, fmt.Errorf("diagnostics: bind database descriptor: %w", err)
	}
	inspection, err := storesqlite.OpenBoundDiagnosticInspection(ctx, databasePath)
	if err != nil {
		deriveOverall(result)
		return result, nil
	}
	defer inspection.Close()
	minimumErr := inspection.MinimumChecker().Check(ctx)
	populateDatabaseSections(ctx, result, inspection, request)
	if errors.Is(minimumErr, integrity.ErrFatal) {
		applyMinimumFatal(result)
	}
	if err := database.VerifyBound(); err != nil {
		return nil, fmt.Errorf("diagnostics: database changed during observation: %w", err)
	}
	if err := namespace.VerifyBoundTarget(target); err != nil {
		return nil, fmt.Errorf("diagnostics: target namespace changed during observation: %w", err)
	}
	deriveOverall(result)
	return result, nil
}

func applyMinimumFatal(result *cliresult.DiagnosticsResult) {
	content, ok := result.Sections[5].Details.(*cliresult.ContentBlobIntegrityDetails)
	if !ok || content == nil {
		content = &cliresult.ContentBlobIntegrityDetails{}
	}
	setSection(result, 5, cliresult.DiagnosticStatusError, content, errorIntegrity)
	setSection(result, 7, cliresult.DiagnosticStatusError,
		&cliresult.ServiceReadinessDetails{ReasonCodes: []string{}}, errorIntegrity)
}

func unknownResult() *cliresult.DiagnosticsResult {
	version := strconv.FormatInt(storesqlite.DiagnosticExpectedSchemaVersion(), 10)
	sections := []cliresult.DiagnosticSection{
		{SectionCode: "schema", Status: cliresult.DiagnosticStatusUnknown, Details: &cliresult.SchemaDetails{ExpectedVersion: version, ErrorCode: &errorTableUnavailable}},
		{SectionCode: "sqlite_quick_check", Status: cliresult.DiagnosticStatusUnknown, Details: &cliresult.QuickCheckDetails{Result: "unavailable", ErrorCode: &errorTableUnavailable}},
		{SectionCode: "foreign_keys", Status: cliresult.DiagnosticStatusUnknown, Details: &cliresult.ForeignKeysDetails{ErrorCode: &errorTableUnavailable}},
		{SectionCode: "canonical_head", Status: cliresult.DiagnosticStatusUnknown, Details: &cliresult.CanonicalHeadDetails{ErrorCode: &errorTableUnavailable}},
		{SectionCode: "resident_ledger", Status: cliresult.DiagnosticStatusUnknown, Details: &cliresult.ResidentLedgerDetails{ErrorCode: &errorTableUnavailable}},
		{SectionCode: "content_blob_integrity", Status: cliresult.DiagnosticStatusUnknown, Details: &cliresult.ContentBlobIntegrityDetails{ErrorCode: &errorTableUnavailable}},
		{SectionCode: "runtime_selection", Status: cliresult.DiagnosticStatusUnknown, Details: &cliresult.RuntimeSelectionDetails{ErrorCode: &errorTableUnavailable}},
		{SectionCode: "service_readiness", Status: cliresult.DiagnosticStatusUnknown, Details: &cliresult.ServiceReadinessDetails{ReasonCodes: []string{}, ErrorCode: &errorTableUnavailable}},
		{SectionCode: "projections", Status: cliresult.DiagnosticStatusUnknown, Details: &cliresult.ProjectionsDetails{Entries: []cliresult.ProjectionDiagnosticEntry{}, ErrorCode: &errorTableUnavailable}},
		{SectionCode: "running_attempts", Status: cliresult.DiagnosticStatusUnknown, Details: &cliresult.RunningAttemptsDetails{RunIDs: []string{}, ResidentIDs: []string{}, ErrorCode: &errorTableUnavailable}},
		{SectionCode: "mandatory_work", Status: cliresult.DiagnosticStatusUnknown, Details: &cliresult.MandatoryWorkDetails{ResidentIDs: []string{}, DialogueUnpreparedEventIDs: []string{}, DialogueScanComplete: false, MemoryScanComplete: false, ErrorCode: &errorTableUnavailable}},
		{SectionCode: "blob_gc", Status: cliresult.DiagnosticStatusUnknown, Details: &cliresult.BlobGCDetails{ErrorCode: &errorTableUnavailable}},
		{SectionCode: "publish_state", Status: cliresult.DiagnosticStatusOK, Details: &cliresult.PublishStateDetails{RestoreStaging: []cliresult.RestoreStagingDiagnostic{}, PublishPending: []cliresult.PublishPendingDiagnostic{}}},
	}
	return &cliresult.DiagnosticsResult{OverallState: cliresult.DiagnosticStatusUnknown, Sections: sections}
}

func populateDatabaseSections(
	ctx context.Context,
	result *cliresult.DiagnosticsResult,
	inspection *storesqlite.DiagnosticInspection,
	request Request,
) {
	inspectSchema(ctx, result, inspection)
	inspectQuickCheck(ctx, result, inspection)
	inspectForeignKeys(ctx, result, inspection)
	head := inspectCanonicalHead(ctx, result, inspection)
	residents := inspectResidentLedger(ctx, result, inspection, request.ResidentID)
	content := inspectContentBlobs(ctx, result, inspection, request, residents)
	snapshot, snapshotOK := inspectRuntimeSelection(ctx, result, inspection)
	projectionCurrent := inspectProjections(ctx, result, inspection, residents, head, request)
	inspectServiceReadiness(ctx, result, inspection, snapshot, snapshotOK, projectionCurrent.service)
	inspectRunningAttempts(ctx, result, inspection, request.ResidentID)
	inspectMandatoryWork(ctx, result, inspection, residents, request.MaxAttempts)
	inspectBlobGC(ctx, result, inspection, request.ResidentID, content, projectionCurrent.contentReferences)
}

func normalizeProjectionSchedule(schedule ProjectionSchedule) ProjectionSchedule {
	if schedule.Clock == nil {
		schedule.Clock = canonical.SystemClock{}
	}
	if schedule.ScanInterval <= 0 {
		schedule.ScanInterval = 30 * time.Second
	}
	if schedule.AsOfRefreshInterval <= 0 {
		schedule.AsOfRefreshInterval = time.Minute
	}
	if schedule.RebuildRetryInterval <= 0 {
		schedule.RebuildRetryInterval = 30 * time.Second
	}
	if schedule.MaxStaleness <= 0 {
		schedule.MaxStaleness = 5 * time.Minute
	}
	if schedule.Overrides == nil {
		schedule.Overrides = map[projection.Name]projection.ScheduleOverride{}
	}
	return schedule
}

func inspectSchema(ctx context.Context, result *cliresult.DiagnosticsResult, inspection *storesqlite.DiagnosticInspection) {
	details := &cliresult.SchemaDetails{ExpectedVersion: strconv.FormatInt(storesqlite.DiagnosticExpectedSchemaVersion(), 10)}
	if !requireColumns(ctx, inspection, "schema_migrations", "version_id", "is_applied") {
		setSection(result, 0, cliresult.DiagnosticStatusUnknown, details, errorTableUnavailable)
		return
	}
	var version sql.NullInt64
	if err := inspection.QueryRowContext(ctx, `SELECT MAX(version_id) FROM schema_migrations WHERE is_applied = 1`).Scan(&version); err != nil {
		setSection(result, 0, cliresult.DiagnosticStatusError, details, errorQueryFailed)
		return
	}
	actual := "0"
	if version.Valid {
		actual = strconv.FormatInt(version.Int64, 10)
	}
	details.ActualVersion = &actual
	fingerprint, err := inspection.ApplicationSchemaFingerprint(ctx)
	if err != nil {
		setSection(result, 0, cliresult.DiagnosticStatusError, details, errorQueryFailed)
		return
	}
	fingerprint = "sha256:" + fingerprint
	details.ActualFingerprint = &fingerprint
	exact := actual == details.ExpectedVersion && fingerprint == "sha256:"+storesqlite.DiagnosticExpectedSchemaFingerprint()
	details.Exact = &exact
	if exact {
		setSection(result, 0, cliresult.DiagnosticStatusOK, details, "")
	} else {
		setSection(result, 0, cliresult.DiagnosticStatusError, details, errorIntegrity)
	}
}

func inspectQuickCheck(ctx context.Context, result *cliresult.DiagnosticsResult, inspection *storesqlite.DiagnosticInspection) {
	details := &cliresult.QuickCheckDetails{Result: "unavailable"}
	rows, err := inspection.QueryContext(ctx, `PRAGMA quick_check`)
	if err != nil {
		setSection(result, 1, cliresult.DiagnosticStatusError, details, errorQueryFailed)
		return
	}
	defer rows.Close()
	clean := true
	seen := false
	for rows.Next() {
		var value string
		if err := rows.Scan(&value); err != nil {
			setSection(result, 1, cliresult.DiagnosticStatusError, details, errorQueryFailed)
			return
		}
		seen = true
		clean = clean && value == "ok"
	}
	if rows.Err() != nil {
		setSection(result, 1, cliresult.DiagnosticStatusError, details, errorQueryFailed)
		return
	}
	if clean && seen {
		details.Result = "ok"
		setSection(result, 1, cliresult.DiagnosticStatusOK, details, "")
	} else {
		details.Result = "corrupt"
		setSection(result, 1, cliresult.DiagnosticStatusError, details, errorIntegrity)
	}
}

func inspectForeignKeys(ctx context.Context, result *cliresult.DiagnosticsResult, inspection *storesqlite.DiagnosticInspection) {
	details := &cliresult.ForeignKeysDetails{}
	rows, err := inspection.QueryContext(ctx, `PRAGMA foreign_key_check`)
	if err != nil {
		setSection(result, 2, cliresult.DiagnosticStatusError, details, errorQueryFailed)
		return
	}
	defer rows.Close()
	var count uint64
	for rows.Next() {
		var table, parent string
		// PRAGMA foreign_key_check's second output column is diagnostic-only.
		// It is consumed as an opaque entry token and is never used to identify,
		// reopen, order, or mutate an application row.
		var entryToken sql.NullInt64
		var foreignKeyID int64
		if err := rows.Scan(&table, &entryToken, &parent, &foreignKeyID); err != nil {
			setSection(result, 2, cliresult.DiagnosticStatusError, details, errorQueryFailed)
			return
		}
		count++
	}
	if rows.Err() != nil {
		setSection(result, 2, cliresult.DiagnosticStatusError, details, errorQueryFailed)
		return
	}
	value := strconv.FormatUint(count, 10)
	details.ViolationCount = &value
	if count == 0 {
		setSection(result, 2, cliresult.DiagnosticStatusOK, details, "")
	} else {
		setSection(result, 2, cliresult.DiagnosticStatusError, details, errorIntegrity)
	}
}

func inspectCanonicalHead(
	ctx context.Context,
	result *cliresult.DiagnosticsResult,
	inspection *storesqlite.DiagnosticInspection,
) *cliresult.Head {
	details := &cliresult.CanonicalHeadDetails{}
	if !requireColumns(ctx, inspection, "canonical_commits", "canonical_commit_id", "commit_seq", "committed_at", "committed_tz") {
		setSection(result, 3, cliresult.DiagnosticStatusUnknown, details, errorTableUnavailable)
		return nil
	}
	var rawID, timezone string
	var seq, at int64
	err := inspection.QueryRowContext(ctx, `SELECT canonical_commit_id, commit_seq, committed_at, committed_tz
		FROM canonical_commits ORDER BY commit_seq DESC LIMIT 1`).Scan(&rawID, &seq, &at, &timezone)
	if errors.Is(err, sql.ErrNoRows) {
		head := &cliresult.Head{Exists: false}
		valid := true
		details.Head, details.LedgerValid = head, &valid
		result.CapturedHead = head
		setSection(result, 3, cliresult.DiagnosticStatusOK, details, "")
		return head
	}
	if err != nil {
		setSection(result, 3, cliresult.DiagnosticStatusError, details, errorQueryFailed)
		return nil
	}
	id, idErr := canonical.ParseID(rawID)
	commitSeq, seqErr := canonical.NewCommitSeq(seq)
	tz, tzErr := canonical.ParseTimezone(timezone)
	if idErr != nil || seqErr != nil || tzErr != nil {
		valid := false
		details.LedgerValid = &valid
		setSection(result, 3, cliresult.DiagnosticStatusError, details, errorIntegrity)
		return nil
	}
	var count, minSeq, maxSeq int64
	if err := inspection.QueryRowContext(ctx, `SELECT COUNT(*), MIN(commit_seq), MAX(commit_seq) FROM canonical_commits`).Scan(&count, &minSeq, &maxSeq); err != nil {
		setSection(result, 3, cliresult.DiagnosticStatusError, details, errorQueryFailed)
		return nil
	}
	valid := count == maxSeq && minSeq == 1 && maxSeq == commitSeq.Int64()
	commit := id.String()
	sequence := commitSeq.String()
	timestamp := strconv.FormatInt(at, 10)
	timezone = tz.String()
	head := &cliresult.Head{Exists: true, CommitID: &commit, CommitSeq: &sequence, CommittedAt: &timestamp, CommittedTZ: &timezone}
	details.Head, details.LedgerValid = head, &valid
	result.CapturedHead = head
	if valid {
		setSection(result, 3, cliresult.DiagnosticStatusOK, details, "")
	} else {
		setSection(result, 3, cliresult.DiagnosticStatusError, details, errorIntegrity)
	}
	return head
}

func inspectResidentLedger(
	ctx context.Context,
	result *cliresult.DiagnosticsResult,
	inspection *storesqlite.DiagnosticInspection,
	filter *canonical.ID,
) []canonical.ID {
	details := &cliresult.ResidentLedgerDetails{}
	if !requireColumns(ctx, inspection, "residents", "resident_id", "parent_resident_id") ||
		!requireColumns(ctx, inspection, "resident_status_transitions", "resident_id", "to_status") {
		setSection(result, 4, cliresult.DiagnosticStatusUnknown, details, errorTableUnavailable)
		return nil
	}
	query := `SELECT r.resident_id, r.parent_resident_id,
		(SELECT COUNT(*) FROM resident_status_transitions t WHERE t.resident_id = r.resident_id)
		FROM residents r`
	args := []any{}
	if filter != nil {
		query += ` WHERE r.resident_id = ?`
		args = append(args, filter.String())
	}
	query += ` ORDER BY r.resident_id`
	rows, err := inspection.QueryContext(ctx, query, args...)
	if err != nil {
		setSection(result, 4, cliresult.DiagnosticStatusError, details, errorQueryFailed)
		return nil
	}
	defer rows.Close()
	residents := make([]canonical.ID, 0)
	invalid := uint64(0)
	for rows.Next() {
		var raw string
		var parent sql.NullString
		var transitions int64
		if err := rows.Scan(&raw, &parent, &transitions); err != nil {
			setSection(result, 4, cliresult.DiagnosticStatusError, details, errorQueryFailed)
			return nil
		}
		id, err := canonical.ParseID(raw)
		if err != nil {
			invalid++
			continue
		}
		residents = append(residents, id)
		// Branches are absent from M7's production surface; any parent link is
		// therefore a ledger violation rather than a supported topology.
		if parent.Valid || transitions == 0 {
			invalid++
		}
	}
	if rows.Err() != nil {
		setSection(result, 4, cliresult.DiagnosticStatusError, details, errorQueryFailed)
		return nil
	}
	repository := inspection.Canonical()
	verifier := canonical.LedgerVerifier{EnvelopeValidator: repository}
	for _, residentID := range residents {
		if _, err := verifier.Verify(ctx, repository, residentID); err != nil {
			if errors.Is(err, canonical.ErrLedgerViolation) {
				invalid++
				continue
			}
			setSection(result, 4, cliresult.DiagnosticStatusError, details, errorQueryFailed)
			return nil
		}
	}
	residentCount := strconv.Itoa(len(residents))
	invalidCount := strconv.FormatUint(invalid, 10)
	details.ResidentCount, details.InvalidResidentCount = &residentCount, &invalidCount
	if invalid == 0 {
		setSection(result, 4, cliresult.DiagnosticStatusOK, details, "")
	} else {
		setSection(result, 4, cliresult.DiagnosticStatusError, details, errorIntegrity)
	}
	return residents
}

type contentObservation struct {
	filesystemOrphans uint64
	sqliteOrphans     uint64
	sqliteOrphanBytes uint64
}

func inspectContentBlobs(
	ctx context.Context,
	result *cliresult.DiagnosticsResult,
	inspection *storesqlite.DiagnosticInspection,
	request Request,
	residents []canonical.ID,
) contentObservation {
	var observation contentObservation
	details := &cliresult.ContentBlobIntegrityDetails{}
	if !requireColumns(ctx, inspection, "content_objects", "owner_resident_id", "blob_hash", "erasure_state") ||
		!requireColumns(ctx, inspection, "blobs", "dedupe_scope_id", "blob_hash", "content", "byte_size") {
		setSection(result, 5, cliresult.DiagnosticStatusUnknown, details, errorTableUnavailable)
		return observation
	}
	query := `SELECT c.owner_resident_id, c.erasure_state, c.blob_hash,
		b.blob_hash, b.byte_size, b.content
		FROM content_objects c
		LEFT JOIN blobs b ON b.dedupe_scope_id = c.owner_resident_id
		 AND b.hash_algorithm = c.blob_hash_algorithm AND b.blob_hash = c.blob_hash`
	args := []any{}
	if request.ResidentID != nil {
		query += ` WHERE c.owner_resident_id = ?`
		args = append(args, request.ResidentID.String())
	}
	query += ` ORDER BY c.owner_resident_id, c.content_id`
	rows, err := inspection.QueryContext(ctx, query, args...)
	if err != nil {
		setSection(result, 5, cliresult.DiagnosticStatusError, details, errorQueryFailed)
		return observation
	}
	type locator struct {
		resident canonical.ID
		digest   canonical.Digest
		size     int64
	}
	presentLocators := make(map[string]locator)
	var present, erased, missingSQLite, missingFilesystem, mismatch uint64
	for rows.Next() {
		var residentRaw, state string
		var contentHash, sqliteHash, content []byte
		var byteSize sql.NullInt64
		if err := rows.Scan(&residentRaw, &state, &contentHash, &sqliteHash, &byteSize, &content); err != nil {
			_ = rows.Close()
			setSection(result, 5, cliresult.DiagnosticStatusError, details, errorQueryFailed)
			return observation
		}
		if state == "erased" {
			erased++
			if contentHash != nil {
				mismatch++
			}
			continue
		}
		present++
		resident, residentErr := canonical.ParseID(residentRaw)
		digest, digestErr := canonical.DigestFromBytes(contentHash)
		if residentErr != nil || digestErr != nil {
			mismatch++
			continue
		}
		if sqliteHash == nil || !byteSize.Valid || content == nil {
			missingSQLite++
			continue
		}
		if !bytes.Equal(sqliteHash, contentHash) || int64(len(content)) != byteSize.Int64 || canonical.HashBlob(content) != digest {
			mismatch++
		}
		presentLocators[resident.String()+"\x00"+digest.Hex()] = locator{resident: resident, digest: digest, size: byteSize.Int64}
	}
	if rows.Err() != nil || rows.Close() != nil {
		setSection(result, 5, cliresult.DiagnosticStatusError, details, errorQueryFailed)
		return observation
	}

	objects, openErr := blob.OpenFileStoreReadOnly(request.BlobRoot)
	if openErr != nil {
		if errors.Is(openErr, os.ErrNotExist) {
			missingFilesystem = uint64(len(presentLocators))
		} else {
			setSection(result, 5, cliresult.DiagnosticStatusError, details, errorIntegrity)
			return observation
		}
	} else {
		for key, locator := range presentLocators {
			body, readErr := objects.Read(ctx, locator.resident, locator.digest)
			if errors.Is(readErr, blob.ErrObjectNotFound) {
				missingFilesystem++
				continue
			}
			if readErr != nil {
				mismatch++
				continue
			}
			if int64(len(body)) != locator.size {
				mismatch++
			}
			delete(presentLocators, key)
		}
		for _, residentID := range residents {
			finals, walkErr := objects.WalkFinal(ctx, residentID)
			if walkErr != nil {
				setSection(result, 5, cliresult.DiagnosticStatusError, details, errorIntegrity)
				return observation
			}
			for _, final := range finals {
				var count int64
				if err := inspection.QueryRowContext(ctx, `SELECT COUNT(*) FROM content_objects
					WHERE owner_resident_id = ? AND erasure_state = 'present' AND blob_hash = ?`,
					final.ResidentID().String(), final.Digest().Bytes()).Scan(&count); err != nil {
					setSection(result, 5, cliresult.DiagnosticStatusError, details, errorQueryFailed)
					return observation
				}
				if count == 0 {
					observation.filesystemOrphans++
				}
			}
		}
	}
	values := []*string{
		decimal(present), decimal(erased), decimal(missingSQLite), decimal(missingFilesystem),
		decimal(mismatch), decimal(observation.filesystemOrphans),
	}
	details.PresentCount, details.ErasedCount, details.MissingSQLiteBlobCount = values[0], values[1], values[2]
	details.MissingFilesystemObjectCount, details.ByteMismatchCount, details.FilesystemOrphanCount = values[3], values[4], values[5]
	if missingSQLite+missingFilesystem+mismatch+observation.filesystemOrphans == 0 {
		setSection(result, 5, cliresult.DiagnosticStatusOK, details, "")
	} else {
		setSection(result, 5, cliresult.DiagnosticStatusError, details, errorIntegrity)
	}
	return observation
}

func inspectRuntimeSelection(
	ctx context.Context,
	result *cliresult.DiagnosticsResult,
	inspection *storesqlite.DiagnosticInspection,
) (readiness.Snapshot, bool) {
	details := &cliresult.RuntimeSelectionDetails{}
	if !requireColumns(ctx, inspection, "runtime_config", "active_resident_id", "desired_sessionization_policy_version_id") ||
		!requireColumns(ctx, inspection, "residents", "resident_id") {
		setSection(result, 6, cliresult.DiagnosticStatusUnknown, details, errorTableUnavailable)
		return readiness.Snapshot{}, false
	}
	snapshot, err := inspection.ServiceReadinessSource().CaptureServiceReadiness(ctx)
	if err != nil {
		setSection(result, 6, cliresult.DiagnosticStatusError, details, errorQueryFailed)
		return readiness.Snapshot{}, false
	}
	activeResolvable := snapshot.ActiveResidentID != nil && snapshot.ActiveResidentStatus != ""
	sessionResolvable := snapshot.SessionPolicyID != nil
	details.ActiveResidentResolvable = &activeResolvable
	details.SessionPolicyResolvable = &sessionResolvable
	if snapshot.ActiveResidentID != nil {
		value := snapshot.ActiveResidentID.String()
		details.ActiveResidentID = &value
	}
	if snapshot.SessionPolicyID != nil {
		value := snapshot.SessionPolicyID.String()
		details.DesiredSessionPolicyID = &value
	}
	if activeResolvable && sessionResolvable {
		setSection(result, 6, cliresult.DiagnosticStatusOK, details, "")
	} else {
		setSection(result, 6, cliresult.DiagnosticStatusWarn, details, "")
	}
	return snapshot, true
}

type projectionObservation struct {
	service           map[string]bool
	contentReferences map[string]bool
}

type diagnosticProjectionStore struct {
	*storesqlite.ProjectionInspection
}

func (diagnosticProjectionStore) Apply(context.Context, projection.ApplyRequest) error {
	return errors.New("diagnostics: projection store is query-only")
}

func (diagnosticProjectionStore) Drop(context.Context, projection.DropRequest) error {
	return errors.New("diagnostics: projection store is query-only")
}

func inspectProjections(
	ctx context.Context,
	result *cliresult.DiagnosticsResult,
	inspection *storesqlite.DiagnosticInspection,
	residents []canonical.ID,
	_ *cliresult.Head,
	request Request,
) projectionObservation {
	details := &cliresult.ProjectionsDetails{Entries: []cliresult.ProjectionDiagnosticEntry{}}
	observed := projectionObservation{
		service: make(map[string]bool, len(residents)), contentReferences: make(map[string]bool, len(residents)),
	}
	if !requireColumns(ctx, inspection, "projection_watermarks", "projection_name", "resident_id", "projection_version", "source_commit_seq", "as_of", "as_of_tz") ||
		!requireColumns(ctx, inspection, "projection_watermark_dependencies", "projection_name", "resident_id", "dependency_kind", "dependency_version_id") {
		setSection(result, 8, cliresult.DiagnosticStatusUnknown, details, errorTableUnavailable)
		return observed
	}
	registry, err := storesqlite.ActiveProjectionRegistry()
	if err != nil {
		setSection(result, 8, cliresult.DiagnosticStatusError, details, errorIntegrity)
		return observed
	}
	surface := inspection.Projection()
	coordinator, err := projection.NewCoordinator(projection.CoordinatorOptions{
		Registry: registry, Source: surface, Store: diagnosticProjectionStore{surface},
		Clock: request.Projection.Clock, Timezone: request.Timezone,
		ScanInterval: request.Projection.ScanInterval, AsOfRefreshInterval: request.Projection.AsOfRefreshInterval,
		RebuildRetryInterval: request.Projection.RebuildRetryInterval, MaxStaleness: request.Projection.MaxStaleness,
		Overrides: request.Projection.Overrides,
	})
	if err != nil {
		setSection(result, 8, cliresult.DiagnosticStatusError, details, errorIntegrity)
		return observed
	}
	filter := projection.StatusFilter{}
	if request.ResidentID != nil {
		filter.ResidentID = request.ResidentID
	}
	statuses, err := coordinator.Status(ctx, filter)
	if err != nil {
		setSection(result, 8, cliresult.DiagnosticStatusError, details, errorQueryFailed)
		return observed
	}
	serviceRequired := make(map[projection.Name]struct{})
	for _, name := range projection.ServiceRequiredNames() {
		serviceRequired[name] = struct{}{}
	}
	for _, residentID := range residents {
		observed.service[residentID.String()] = true
		observed.contentReferences[residentID.String()] = true
	}
	memoryTargetsCurrent := make(map[string]bool, len(residents))
	for _, residentID := range residents {
		memoryTargetsCurrent[residentID.String()] = exactMemoryProjectionTarget(statuses, residentID)
	}
	allCurrent := true
	for _, status := range statuses {
		current := status.Built && status.UpToDate && !status.Stale && status.BlockingReason == ""
		if (status.Name == projection.ClaimStatesName || status.Name == projection.RuntimeStatesName) &&
			!memoryTargetsCurrent[status.ResidentID.String()] {
			current = false
		}
		lag := status.CommitLag
		if lag < 0 {
			lag = 0
			current = false
		}
		entry := cliresult.ProjectionDiagnosticEntry{
			ResidentID: status.ResidentID.String(), Name: string(status.Name), Version: string(status.Version),
			Current: current, Rebuilding: false, LagCommits: decimalInt(lag),
		}
		if status.Watermark != nil {
			var commitID, timezone string
			var committedAt int64
			sourceSeq := status.Watermark.SourceCommitSeq.Int64()
			if err := inspection.QueryRowContext(ctx, `SELECT canonical_commit_id, committed_at, committed_tz
				FROM canonical_commits WHERE commit_seq = ?`, sourceSeq).Scan(&commitID, &committedAt, &timezone); err == nil {
				sequence := strconv.FormatInt(sourceSeq, 10)
				at := strconv.FormatInt(committedAt, 10)
				entry.WatermarkHead = &cliresult.Head{Exists: true, CommitID: &commitID, CommitSeq: &sequence, CommittedAt: &at, CommittedTZ: &timezone}
			} else {
				entry.Current = false
				current = false
			}
		}
		if _, required := serviceRequired[status.Name]; required {
			observed.service[status.ResidentID.String()] = observed.service[status.ResidentID.String()] && current
		}
		if status.Name == projection.ContentReferencesName {
			observed.contentReferences[status.ResidentID.String()] = current
		}
		allCurrent = allCurrent && current
		details.Entries = append(details.Entries, entry)
	}
	for _, residentID := range residents {
		key := residentID.String()
		if !memoryTargetsCurrent[key] {
			observed.service[key] = false
			allCurrent = false
		}
	}
	sort.Slice(details.Entries, func(i, j int) bool {
		left, right := details.Entries[i], details.Entries[j]
		return left.ResidentID+"\x00"+left.Name+"\x00"+left.Version < right.ResidentID+"\x00"+right.Name+"\x00"+right.Version
	})
	if allCurrent {
		setSection(result, 8, cliresult.DiagnosticStatusOK, details, "")
	} else {
		setSection(result, 8, cliresult.DiagnosticStatusWarn, details, "")
	}
	return observed
}

func exactMemoryProjectionTarget(statuses []projection.Status, residentID canonical.ID) bool {
	var claim, runtime *projection.Watermark
	claimCount, runtimeCount := 0, 0
	for _, status := range statuses {
		if status.ResidentID != residentID {
			continue
		}
		switch status.Name {
		case projection.ClaimStatesName:
			claimCount++
			claim = status.Watermark
		case projection.RuntimeStatesName:
			runtimeCount++
			runtime = status.Watermark
		}
	}
	return claimCount == 1 && runtimeCount == 1 && claim != nil && runtime != nil &&
		claim.SourceCommitSeq == runtime.SourceCommitSeq && claim.AsOf == runtime.AsOf && claim.AsOfTZ == runtime.AsOfTZ
}

func inspectServiceReadiness(
	ctx context.Context,
	result *cliresult.DiagnosticsResult,
	inspection *storesqlite.DiagnosticInspection,
	snapshot readiness.Snapshot,
	snapshotOK bool,
	projectionCurrent map[string]bool,
) {
	details := &cliresult.ServiceReadinessDetails{ReasonCodes: []string{}}
	if !snapshotOK {
		setSection(result, 7, cliresult.DiagnosticStatusUnknown, details, errorTableUnavailable)
		return
	}
	evaluated, err := readiness.EvaluateServiceReadiness(ctx, inspection.ServiceReadinessSource(), readiness.Request{
		StartupComplete: true,
		Projection: readiness.ProjectionCheckFunc(func(_ context.Context, requirement readiness.ProjectionRequirement) (bool, error) {
			return projectionCurrent[requirement.ResidentID.String()], nil
		}),
	})
	if err != nil || evaluated.CapturedHead != snapshot.CapturedHead {
		setSection(result, 7, cliresult.DiagnosticStatusError, details, errorQueryFailed)
		return
	}
	head := readinessHead(evaluated.CapturedHead)
	details.EvaluatedHead = &head
	details.Ready = boolPointer(evaluated.Ready)
	for _, reason := range evaluated.ReasonCodes {
		details.ReasonCodes = append(details.ReasonCodes, string(reason))
	}
	if evaluated.Ready {
		setSection(result, 7, cliresult.DiagnosticStatusOK, details, "")
	} else {
		setSection(result, 7, cliresult.DiagnosticStatusWarn, details, "")
	}
}

func inspectRunningAttempts(
	ctx context.Context,
	result *cliresult.DiagnosticsResult,
	inspection *storesqlite.DiagnosticInspection,
	residentFilter *canonical.ID,
) {
	details := &cliresult.RunningAttemptsDetails{RunIDs: []string{}, ResidentIDs: []string{}}
	if !requireColumns(ctx, inspection, "generation_runs", "generation_run_id", "resident_id", "purpose") {
		setSection(result, 9, cliresult.DiagnosticStatusUnknown, details, errorTableUnavailable)
		return
	}
	attempts, err := inspection.Canonical().RunningAttempts(ctx, maximumDiagnosticWork)
	if err != nil {
		setSection(result, 9, cliresult.DiagnosticStatusError, details, errorIntegrity)
		return
	}
	residentSet := make(map[string]struct{})
	for _, attempt := range attempts {
		if residentFilter != nil && attempt.ResidentID != *residentFilter {
			continue
		}
		details.RunIDs = append(details.RunIDs, attempt.RunID.String())
		residentSet[attempt.ResidentID.String()] = struct{}{}
	}
	sort.Strings(details.RunIDs)
	for id := range residentSet {
		details.ResidentIDs = append(details.ResidentIDs, id)
	}
	sort.Strings(details.ResidentIDs)
	count := strconv.Itoa(len(details.RunIDs))
	details.Count = &count
	if len(details.RunIDs) == 0 {
		setSection(result, 9, cliresult.DiagnosticStatusOK, details, "")
	} else {
		setSection(result, 9, cliresult.DiagnosticStatusWarn, details, "")
	}
}

func inspectMandatoryWork(
	ctx context.Context,
	result *cliresult.DiagnosticsResult,
	inspection *storesqlite.DiagnosticInspection,
	residents []canonical.ID,
	maxAttempts int,
) {
	details := &cliresult.MandatoryWorkDetails{
		ResidentIDs: []string{}, DialogueUnpreparedEventIDs: []string{},
		DialogueScanComplete: true, MemoryScanComplete: true,
	}
	if !requireColumns(ctx, inspection, "events", "event_id", "resident_id", "event_type") ||
		!requireColumns(ctx, inspection, "generation_runs", "generation_run_id", "resident_id", "idempotency_key") {
		details.DialogueScanComplete = false
		details.MemoryScanComplete = false
		setSection(result, 10, cliresult.DiagnosticStatusUnknown, details, errorTableUnavailable)
		return
	}
	counts := map[string]uint64{}
	residentSet := make(map[string]struct{})
	dialogueScanned := 0
	memoryScanned := 0
	details.DialogueScannedCandidates = decimal(0)
	details.MemoryScannedCandidates = decimal(0)
	for _, residentID := range residents {
		var dialogue []domain.DialogueWork
		if dialogueScanned >= maximumDiagnosticWork {
			details.DialogueScanComplete = false
		} else {
			var scanned int
			var complete bool
			var unprepared []string
			var err error
			dialogue, scanned, complete, unprepared, err = discoverDialogueWorkDiagnostic(
				ctx, inspection.Canonical(), residentID, maxAttempts, maximumDiagnosticWork-dialogueScanned,
			)
			dialogueScanned += scanned
			details.DialogueScannedCandidates = decimal(uint64(dialogueScanned))
			details.DialogueUnpreparedEventIDs = append(details.DialogueUnpreparedEventIDs, unprepared...)
			if err != nil {
				details.DialogueScanComplete = false
				details.MemoryScanComplete = false
				setSection(result, 10, cliresult.DiagnosticStatusUnknown, details, errorQueryFailed)
				return
			}
			if !complete {
				details.DialogueScanComplete = false
			}
		}

		var memory []domain.MemoryExtractionWork
		if memoryScanned >= maximumDiagnosticWork {
			details.MemoryScanComplete = false
		} else {
			var scanned int
			var complete bool
			var err error
			memory, scanned, complete, err = discoverMemoryWorkDiagnostic(
				ctx, inspection.Canonical(), residentID, maxAttempts, maximumDiagnosticWork-memoryScanned,
			)
			memoryScanned += scanned
			details.MemoryScannedCandidates = decimal(uint64(memoryScanned))
			if err != nil {
				details.MemoryScanComplete = false
				setSection(result, 10, cliresult.DiagnosticStatusUnknown, details, errorQueryFailed)
				return
			}
			if !complete {
				details.MemoryScanComplete = false
			}
		}
		for _, work := range dialogue {
			if incrementWorkCount(counts, "dialogue", work.State) {
				residentSet[residentID.String()] = struct{}{}
			}
		}
		for _, work := range memory {
			if incrementWorkCount(counts, "memory", work.State) {
				residentSet[residentID.String()] = struct{}{}
			}
		}
	}
	sort.Strings(details.DialogueUnpreparedEventIDs)
	if len(details.DialogueUnpreparedEventIDs) > 128 {
		details.DialogueUnpreparedEventIDs = details.DialogueUnpreparedEventIDs[:128]
		details.DialogueUnpreparedTruncated = true
	}
	details.DialoguePending = decimal(counts["dialogue_pending"])
	details.DialogueRunning = decimal(counts["dialogue_running"])
	details.DialogueRetryPending = decimal(counts["dialogue_retry_pending"])
	details.MemoryPending = decimal(counts["memory_pending"])
	details.MemoryRunning = decimal(counts["memory_running"])
	details.MemoryRetryPending = decimal(counts["memory_retry_pending"])
	for id := range residentSet {
		details.ResidentIDs = append(details.ResidentIDs, id)
	}
	sort.Strings(details.ResidentIDs)
	if len(details.ResidentIDs) == 0 && details.DialogueScanComplete && details.MemoryScanComplete {
		setSection(result, 10, cliresult.DiagnosticStatusOK, details, "")
	} else {
		setSection(result, 10, cliresult.DiagnosticStatusWarn, details, "")
	}
}

func discoverDialogueWorkDiagnostic(
	ctx context.Context,
	repository interface {
		DiscoverDialogueWork(context.Context, canonical.ID, domain.DialogueDiscoveryRequest) (domain.DialogueDiscoveryResult, error)
	},
	residentID canonical.ID,
	maxAttempts int,
	candidateLimit int,
) ([]domain.DialogueWork, int, bool, []string, error) {
	if candidateLimit < 1 {
		return nil, 0, false, nil, nil
	}
	cursor := (*domain.DialogueDiscoveryCursor)(nil)
	seen := make(map[canonical.ID]domain.DialogueWork)
	var scanned int
	for scanned < candidateLimit {
		budget, ok := diagnosticDialogueBudget(candidateLimit - scanned)
		if !ok {
			return dialogueDiagnosticResult(seen, scanned, false)
		}
		request := domain.DialogueDiscoveryRequest{Cursor: cursor, MaxAttempts: maxAttempts, Budget: budget}
		page, err := repository.DiscoverDialogueWork(ctx, residentID, request)
		if err != nil {
			return nil, scanned, false, nil, err
		}
		pageScanned := page.RecentScanned + page.OlderScanned
		if pageScanned < 0 || pageScanned > candidateLimit-scanned {
			return nil, scanned, false, nil, errors.New("diagnostics: dialogue discovery exceeded its candidate budget")
		}
		scanned += pageScanned
		for _, work := range page.Work {
			seen[work.UserEvent.ID] = work
		}
		if page.CycleComplete {
			return dialogueDiagnosticResult(seen, scanned, true)
		}
		if page.NextCursor == nil || pageScanned == 0 {
			return dialogueDiagnosticResult(seen, scanned, false)
		}
		cursor = page.NextCursor
		if page.ActionablePage {
			// Production discovery deliberately holds NextCursor at the
			// beginning of an actionable page so the scheduler can process
			// every sibling before advancing. Diagnostics is read-only, so it
			// advances below the oldest actionable event it just inventoried.
			boundary := page.NextCursor.BeforeSeq
			var oldest *canonical.Seq
			for _, work := range page.Work {
				if work.UserEvent.Seq >= boundary {
					continue
				}
				candidate := work.UserEvent.Seq
				if oldest == nil || candidate < *oldest {
					copy := candidate
					oldest = &copy
				}
			}
			if oldest == nil {
				return dialogueDiagnosticResult(seen, scanned, false)
			}
			cursor = &domain.DialogueDiscoveryCursor{BeforeSeq: *oldest}
		}
	}
	return dialogueDiagnosticResult(seen, scanned, false)
}

func diagnosticDialogueBudget(remaining int) (domain.DialogueDiscoveryBudget, bool) {
	if remaining < 2 {
		return domain.DialogueDiscoveryBudget{}, false
	}
	production := domain.ProductionDialogueDiscoveryBudget()
	recent := min(production.RecentCandidates, remaining/2)
	olderCandidates := min(production.OlderCandidates, remaining-recent)
	olderPageSize := min(production.OlderPageSize, olderCandidates)
	olderPages := (olderCandidates + olderPageSize - 1) / olderPageSize
	return domain.DialogueDiscoveryBudget{
		RecentCandidates: recent, OlderPageSize: olderPageSize,
		OlderCandidates: olderCandidates, OlderPages: olderPages, Elapsed: production.Elapsed,
	}, true
}

func dialogueDiagnosticResult(
	seen map[canonical.ID]domain.DialogueWork,
	scanned int,
	complete bool,
) ([]domain.DialogueWork, int, bool, []string, error) {
	works := make([]domain.DialogueWork, 0, len(seen))
	for _, work := range seen {
		works = append(works, work)
	}
	sort.Slice(works, func(i, j int) bool { return works[i].UserEvent.ID.String() < works[j].UserEvent.ID.String() })
	var unprepared []string
	for _, work := range works {
		if work.RunID == nil && work.CancellationCode == "" && work.State == domain.WorkPending {
			unprepared = append(unprepared, work.UserEvent.ID.String())
		}
	}
	sort.Strings(unprepared)
	return works, scanned, complete, unprepared, nil
}

func discoverMemoryWorkDiagnostic(
	ctx context.Context,
	repository memoryDiagnosticRepository,
	residentID canonical.ID,
	maxAttempts int,
	candidateLimit int,
) ([]domain.MemoryExtractionWork, int, bool, error) {
	if candidateLimit < 1 {
		return nil, 0, false, nil
	}
	seen := make(map[string]domain.MemoryExtractionWork)
	normalScanned, normalComplete, err := discoverNormalMemoryWorkDiagnostic(
		ctx, repository, residentID, maxAttempts, candidateLimit, seen,
	)
	if err != nil {
		return nil, normalScanned, false, err
	}
	if normalScanned >= candidateLimit {
		return memoryDiagnosticResult(seen, normalScanned, false)
	}
	reextractionScanned, reextractionComplete, err := discoverMemoryReextractionWorkDiagnostic(
		ctx, repository, residentID, maxAttempts, candidateLimit-normalScanned, seen,
	)
	totalScanned := normalScanned + reextractionScanned
	if err != nil {
		return nil, totalScanned, false, err
	}
	return memoryDiagnosticResult(seen, totalScanned, normalComplete && reextractionComplete)
}

type memoryDiagnosticRepository interface {
	DiscoverMemoryExtractionWork(
		context.Context,
		canonical.ID,
		domain.MemoryDiscoveryRequest,
	) (domain.MemoryDiscoveryResult, error)
	DiscoverMemoryReextractionWork(
		context.Context,
		canonical.ID,
		domain.MemoryReextractionDiscoveryRequest,
	) (domain.MemoryReextractionDiscoveryResult, error)
}

func discoverNormalMemoryWorkDiagnostic(
	ctx context.Context,
	repository memoryDiagnosticRepository,
	residentID canonical.ID,
	maxAttempts int,
	candidateLimit int,
	seen map[string]domain.MemoryExtractionWork,
) (int, bool, error) {
	cursor := (*domain.MemoryDiscoveryCursor)(nil)
	var scanned int
	for scanned < candidateLimit {
		page, err := repository.DiscoverMemoryExtractionWork(ctx, residentID, domain.MemoryDiscoveryRequest{
			Cursor: cursor, MaxAttempts: maxAttempts, Budget: diagnosticMemoryBudget(candidateLimit - scanned),
		})
		if err != nil {
			return scanned, false, err
		}
		if page.CandidatesScanned < 0 || page.CandidatesScanned > candidateLimit-scanned {
			return scanned, false, errors.New("diagnostics: memory discovery exceeded its candidate budget")
		}
		scanned += page.CandidatesScanned
		if page.Work != nil {
			seen[page.Work.IdempotencyKey] = *page.Work
		}
		if page.CycleComplete {
			return scanned, true, nil
		}
		if page.NextCursor == nil || page.CandidatesScanned == 0 {
			return scanned, false, nil
		}
		cursor = page.NextCursor
	}
	return scanned, false, nil
}

func discoverMemoryReextractionWorkDiagnostic(
	ctx context.Context,
	repository memoryDiagnosticRepository,
	residentID canonical.ID,
	maxAttempts int,
	candidateLimit int,
	seen map[string]domain.MemoryExtractionWork,
) (int, bool, error) {
	if candidateLimit < 1 {
		return 0, false, nil
	}
	cursor := (*domain.MemoryReextractionDiscoveryCursor)(nil)
	var scanned int
	for scanned < candidateLimit {
		page, err := repository.DiscoverMemoryReextractionWork(
			ctx,
			residentID,
			domain.MemoryReextractionDiscoveryRequest{
				Cursor: cursor, MaxAttempts: maxAttempts, Budget: diagnosticMemoryBudget(candidateLimit - scanned),
			},
		)
		if err != nil {
			return scanned, false, err
		}
		if page.CandidatesScanned < 0 || page.CandidatesScanned > candidateLimit-scanned {
			return scanned, false, errors.New("diagnostics: memory re-extraction discovery exceeded its candidate budget")
		}
		scanned += page.CandidatesScanned
		if page.Work != nil {
			seen[page.Work.IdempotencyKey] = *page.Work
		}
		if page.CycleComplete {
			return scanned, true, nil
		}
		if page.NextCursor == nil || (page.CandidatesScanned == 0 &&
			memoryReextractionDiscoveryCursorsEqual(cursor, page.NextCursor)) {
			return scanned, false, nil
		}
		cursor = page.NextCursor
	}
	return scanned, false, nil
}

func memoryReextractionDiscoveryCursorsEqual(
	left *domain.MemoryReextractionDiscoveryCursor,
	right *domain.MemoryReextractionDiscoveryCursor,
) bool {
	if left == nil || right == nil {
		return left == right
	}
	if left.CycleThroughCommitSeq != right.CycleThroughCommitSeq ||
		(left.CompletedThroughCommitSeq == nil) != (right.CompletedThroughCommitSeq == nil) ||
		(left.ActiveCommit == nil) != (right.ActiveCommit == nil) {
		return false
	}
	if left.CompletedThroughCommitSeq != nil &&
		*left.CompletedThroughCommitSeq != *right.CompletedThroughCommitSeq {
		return false
	}
	if left.ActiveCommit == nil {
		return true
	}
	if left.ActiveCommit.CommitID != right.ActiveCommit.CommitID ||
		left.ActiveCommit.CommitSeq != right.ActiveCommit.CommitSeq ||
		(left.ActiveCommit.AfterRunID == nil) != (right.ActiveCommit.AfterRunID == nil) {
		return false
	}
	return left.ActiveCommit.AfterRunID == nil ||
		*left.ActiveCommit.AfterRunID == *right.ActiveCommit.AfterRunID
}

func diagnosticMemoryBudget(remaining int) domain.MemoryDiscoveryBudget {
	production := domain.ProductionMemoryDiscoveryBudget()
	candidates := min(production.Candidates, remaining)
	pageSize := min(production.PageSize, candidates)
	pages := (candidates + pageSize - 1) / pageSize
	return domain.MemoryDiscoveryBudget{
		PageSize: pageSize, Candidates: candidates, Pages: pages, Elapsed: production.Elapsed,
	}
}

func memoryDiagnosticResult(
	seen map[string]domain.MemoryExtractionWork,
	scanned int,
	complete bool,
) ([]domain.MemoryExtractionWork, int, bool, error) {
	works := make([]domain.MemoryExtractionWork, 0, len(seen))
	for _, work := range seen {
		works = append(works, work)
	}
	sort.Slice(works, func(i, j int) bool { return works[i].IdempotencyKey < works[j].IdempotencyKey })
	return works, scanned, complete, nil
}

func incrementWorkCount(counts map[string]uint64, prefix string, state domain.WorkState) bool {
	switch state {
	case domain.WorkPending:
		counts[prefix+"_pending"]++
	case domain.WorkRunning:
		counts[prefix+"_running"]++
	case domain.WorkRetryPending:
		counts[prefix+"_retry_pending"]++
	default:
		return false
	}
	return true
}

func inspectBlobGC(
	ctx context.Context,
	result *cliresult.DiagnosticsResult,
	inspection *storesqlite.DiagnosticInspection,
	residentFilter *canonical.ID,
	content contentObservation,
	projectionCurrent map[string]bool,
) {
	details := &cliresult.BlobGCDetails{}
	if !requireColumns(ctx, inspection, "blobs", "dedupe_scope_id", "blob_hash", "byte_size") ||
		!requireColumns(ctx, inspection, "content_objects", "owner_resident_id", "blob_hash") {
		setSection(result, 11, cliresult.DiagnosticStatusUnknown, details, errorTableUnavailable)
		return
	}
	query := `SELECT COUNT(*), COALESCE(SUM(b.byte_size), 0) FROM blobs b
		WHERE NOT EXISTS (SELECT 1 FROM content_objects c
		 WHERE c.owner_resident_id = b.dedupe_scope_id AND c.blob_hash = b.blob_hash)`
	args := []any{}
	if residentFilter != nil {
		query += ` AND b.dedupe_scope_id = ?`
		args = append(args, residentFilter.String())
	}
	var count, size int64
	if err := inspection.QueryRowContext(ctx, query, args...).Scan(&count, &size); err != nil || count < 0 || size < 0 {
		setSection(result, 11, cliresult.DiagnosticStatusError, details, errorQueryFailed)
		return
	}
	content.sqliteOrphans = uint64(count)
	content.sqliteOrphanBytes = uint64(size)
	allCurrent := true
	if residentFilter != nil {
		allCurrent = projectionCurrent[residentFilter.String()]
	} else {
		for _, current := range projectionCurrent {
			allCurrent = allCurrent && current
		}
	}
	eligibleCount, eligibleBytes, blocked := uint64(count), uint64(size), uint64(0)
	if !allCurrent {
		eligibleCount, eligibleBytes, blocked = 0, 0, uint64(count)
	}
	details.EligibleCount = decimal(eligibleCount)
	details.EligibleBytes = decimal(eligibleBytes)
	details.BlockedCount = decimal(blocked)
	details.FilesystemOrphanCount = decimal(content.filesystemOrphans)
	details.SQLiteOrphanCount = decimal(content.sqliteOrphans)
	if eligibleCount+blocked+content.filesystemOrphans+content.sqliteOrphans == 0 {
		setSection(result, 11, cliresult.DiagnosticStatusOK, details, "")
	} else {
		setSection(result, 11, cliresult.DiagnosticStatusWarn, details, "")
	}
}

type targetMarkerObservation struct {
	restoreBody    []byte
	restorePresent bool
	restoreErr     bool
	publishBody    []byte
	publishPresent bool
	publishErr     bool
}

func inspectTargetMarkers(target *namespacelock.Target) *targetMarkerObservation {
	result := &targetMarkerObservation{}
	var err error
	result.restoreBody, result.restorePresent, err = target.ReadDiagnosticMarker(restore.RestoreStagingMarker, maximumMarkerBytes)
	result.restoreErr = err != nil
	result.publishBody, result.publishPresent, err = target.ReadDiagnosticMarker(durablepublish.MarkerName, maximumMarkerBytes)
	result.publishErr = err != nil
	return result
}

func inspectPublishState(
	ctx context.Context,
	targetPath string,
	targetExists bool,
	internal *targetMarkerObservation,
	siblings []namespacelock.DiagnosticMarkerObservation,
	siblingErr error,
) cliresult.DiagnosticSection {
	details := &cliresult.PublishStateDetails{RestoreStaging: []cliresult.RestoreStagingDiagnostic{}, PublishPending: []cliresult.PublishPendingDiagnostic{}}
	targetBase := filepath.Base(targetPath)
	status := cliresult.DiagnosticStatusOK
	errorCode := ""
	if siblingErr != nil {
		status, errorCode = cliresult.DiagnosticStatusError, errorMarkerUnreadable
	}
	restoreByName := make(map[string]restore.StagingMarker)
	for _, observation := range siblings {
		switch observation.Kind {
		case namespacelock.DiagnosticRestoreStaging:
			markerID := markerIDFromSuffix(observation.Basename, "."+targetBase+".restore-staging.")
			entry := cliresult.RestoreStagingDiagnostic{MarkerID: markerID, TargetBasename: targetBase, State: "unreadable"}
			if observation.Readable {
				marker, err := restore.ParseStagingMarker(observation.Marker)
				if err == nil && marker.TargetName == targetBase && marker.StagingName == observation.Basename {
					id := marker.RestoreID.String()
					entry.MarkerID, entry.State = &id, "prepared"
					restoreByName[observation.Basename] = marker
				} else if err == nil {
					id := marker.RestoreID.String()
					entry.MarkerID, entry.State = &id, "identity_mismatch"
				}
			}
			details.RestoreStaging = append(details.RestoreStaging, entry)
			updatePublishSeverity(entry.State, &status, &errorCode)
		case namespacelock.DiagnosticPublishPending:
			entry := publishDiagnosticFromObservation(observation, targetBase)
			if entry.State != "unreadable" && entry.PublishID != nil {
				marker, _ := durablepublish.ParseMarker(observation.Marker)
				if marker.TargetBasename != targetBase || observation.Basename != mustSiblingName(marker) {
					entry.State = "identity_mismatch"
				} else if targetExists {
					entry.State = targetDigestState(ctx, targetPath, marker)
				} else if marker.Variant == durablepublish.VariantDirectory {
					if _, exists := restoreByName[marker.StagingBasename]; exists {
						entry.State = "prepared"
					} else {
						entry.State = "staging_missing"
					}
				}
			}
			details.PublishPending = append(details.PublishPending, entry)
			updatePublishSeverity(entry.State, &status, &errorCode)
		}
	}
	if targetExists && internal != nil {
		if internal.restorePresent {
			entry := cliresult.RestoreStagingDiagnostic{TargetBasename: targetBase, State: "unreadable"}
			if !internal.restoreErr {
				if marker, err := restore.ParseStagingMarker(internal.restoreBody); err == nil {
					id := marker.RestoreID.String()
					entry.MarkerID, entry.State = &id, "reserved_marker_in_target"
				}
			}
			details.RestoreStaging = append(details.RestoreStaging, entry)
			updatePublishSeverity(entry.State, &status, &errorCode)
		}
		if internal.publishPresent {
			entry := cliresult.PublishPendingDiagnostic{TargetBasename: targetBase, State: "unreadable"}
			if !internal.publishErr {
				if marker, err := durablepublish.ParseMarker(internal.publishBody); err == nil {
					entry = publishDiagnostic(marker, "target_marker_only")
					if marker.TargetBasename != targetBase {
						entry.State = "identity_mismatch"
					}
				}
			}
			details.PublishPending = append(details.PublishPending, entry)
			updatePublishSeverity(entry.State, &status, &errorCode)
		}
	}
	dedupeAndSortPublish(details)
	if status == cliresult.DiagnosticStatusError {
		details.ErrorCode = &errorCode
	}
	return cliresult.DiagnosticSection{SectionCode: "publish_state", Status: status, Details: details}
}

func publishDiagnosticFromObservation(observation namespacelock.DiagnosticMarkerObservation, target string) cliresult.PublishPendingDiagnostic {
	entry := cliresult.PublishPendingDiagnostic{
		PublishID:      markerIDFromSuffix(observation.Basename, "."+target+".publish-pending."),
		TargetBasename: target, State: "unreadable",
	}
	if !observation.Readable {
		return entry
	}
	marker, err := durablepublish.ParseMarker(observation.Marker)
	if err != nil {
		return entry
	}
	return publishDiagnostic(marker, "prepared")
}

func publishDiagnostic(marker durablepublish.Marker, state string) cliresult.PublishPendingDiagnostic {
	id := marker.PublishID.String()
	variant := string(marker.Variant)
	command := cliresult.CommandID(marker.ProducerCommand)
	return cliresult.PublishPendingDiagnostic{
		PublishID: &id, Variant: &variant, ProducerCommand: &command,
		TargetBasename: marker.TargetBasename, State: state,
	}
}

func targetDigestState(ctx context.Context, targetPath string, marker durablepublish.Marker) string {
	if marker.Variant != durablepublish.VariantDirectory {
		return "identity_mismatch"
	}
	policy, err := fssecure.CurrentSecurityPolicy()
	if err != nil {
		return "identity_mismatch"
	}
	root, err := fssecure.OpenRootReadOnly(targetPath, policy)
	if err != nil {
		return "identity_mismatch"
	}
	defer root.Close()
	reserved := []string{}
	if marker.ProducerCommand == durablepublish.CommandBackupRestore {
		reserved = append(reserved, restore.RestoreStagingMarker)
	}
	payload, err := durablepublish.DirectoryPayloadDigest(ctx, root, reserved...)
	if err != nil {
		return "identity_mismatch"
	}
	if marker.PayloadSHA256 == "sha256:"+payload.SHA256.Hex() {
		return "target_visible_digest_match"
	}
	return "target_visible_digest_mismatch"
}

func updatePublishSeverity(state string, status *string, errorCode *string) {
	switch state {
	case "unreadable":
		*status, *errorCode = cliresult.DiagnosticStatusError, errorMarkerUnreadable
	case "identity_mismatch", "target_visible_digest_mismatch", "reserved_marker_in_target":
		if *errorCode != errorMarkerUnreadable {
			*status, *errorCode = cliresult.DiagnosticStatusError, errorIntegrity
		}
	case "prepared", "staging_missing", "target_marker_only":
		if *status == cliresult.DiagnosticStatusOK {
			*status = cliresult.DiagnosticStatusWarn
		}
	}
}

func markerIDFromSuffix(name, prefix string) *string {
	if !strings.HasPrefix(name, prefix) {
		return nil
	}
	id, err := canonical.ParseID(strings.TrimPrefix(name, prefix))
	if err != nil {
		return nil
	}
	value := id.String()
	return &value
}

func mustSiblingName(marker durablepublish.Marker) string {
	name, err := durablepublish.SiblingMarkerBasename(marker.TargetBasename, marker.PublishID)
	if err != nil {
		return ""
	}
	return name
}

func dedupeAndSortPublish(details *cliresult.PublishStateDetails) {
	restoreSeen := make(map[string]cliresult.RestoreStagingDiagnostic)
	for _, marker := range details.RestoreStaging {
		key := marker.TargetBasename + "\x00" + nullable(marker.MarkerID)
		if previous, exists := restoreSeen[key]; !exists || previous.State == "prepared" && marker.State != "prepared" {
			restoreSeen[key] = marker
		}
	}
	details.RestoreStaging = details.RestoreStaging[:0]
	for _, marker := range restoreSeen {
		details.RestoreStaging = append(details.RestoreStaging, marker)
	}
	sort.Slice(details.RestoreStaging, func(i, j int) bool {
		left, right := details.RestoreStaging[i], details.RestoreStaging[j]
		return left.TargetBasename+"\x00"+nullable(left.MarkerID) < right.TargetBasename+"\x00"+nullable(right.MarkerID)
	})
	publishSeen := make(map[string]cliresult.PublishPendingDiagnostic)
	for _, marker := range details.PublishPending {
		key := marker.TargetBasename + "\x00" + nullable(marker.PublishID)
		if previous, exists := publishSeen[key]; !exists || previous.State == "prepared" && marker.State != "prepared" {
			publishSeen[key] = marker
		}
	}
	details.PublishPending = details.PublishPending[:0]
	for _, marker := range publishSeen {
		details.PublishPending = append(details.PublishPending, marker)
	}
	sort.Slice(details.PublishPending, func(i, j int) bool {
		left, right := details.PublishPending[i], details.PublishPending[j]
		return left.TargetBasename+"\x00"+nullable(left.PublishID) < right.TargetBasename+"\x00"+nullable(right.PublishID)
	})
}

func nullable(value *string) string {
	if value == nil {
		return ""
	}
	return *value
}

func setPublishSection(result *cliresult.DiagnosticsResult, section cliresult.DiagnosticSection) {
	result.Sections[12] = section
}

func requireColumns(ctx context.Context, inspection *storesqlite.DiagnosticInspection, table string, columns ...string) bool {
	ok, err := inspection.HasColumns(ctx, table, columns...)
	return err == nil && ok
}

func setSection(result *cliresult.DiagnosticsResult, index int, status string, details cliresult.DiagnosticDetails, errorCode string) {
	result.Sections[index].Status = status
	result.Sections[index].Details = details
	setDiagnosticError(details, errorCode)
}

func setDiagnosticError(details cliresult.DiagnosticDetails, code string) {
	var value *string
	if code != "" {
		copy := code
		value = &copy
	}
	switch typed := details.(type) {
	case *cliresult.SchemaDetails:
		typed.ErrorCode = value
	case *cliresult.QuickCheckDetails:
		typed.ErrorCode = value
	case *cliresult.ForeignKeysDetails:
		typed.ErrorCode = value
	case *cliresult.CanonicalHeadDetails:
		typed.ErrorCode = value
	case *cliresult.ResidentLedgerDetails:
		typed.ErrorCode = value
	case *cliresult.ContentBlobIntegrityDetails:
		typed.ErrorCode = value
	case *cliresult.RuntimeSelectionDetails:
		typed.ErrorCode = value
	case *cliresult.ServiceReadinessDetails:
		typed.ErrorCode = value
	case *cliresult.ProjectionsDetails:
		typed.ErrorCode = value
	case *cliresult.RunningAttemptsDetails:
		typed.ErrorCode = value
	case *cliresult.MandatoryWorkDetails:
		typed.ErrorCode = value
	case *cliresult.BlobGCDetails:
		typed.ErrorCode = value
	}
}

func deriveOverall(result *cliresult.DiagnosticsResult) {
	result.OverallState = cliresult.DiagnosticStatusOK
	severity := 0
	for _, section := range result.Sections {
		value := map[string]int{
			cliresult.DiagnosticStatusOK: 0, cliresult.DiagnosticStatusUnknown: 1,
			cliresult.DiagnosticStatusWarn: 2, cliresult.DiagnosticStatusError: 3,
		}[section.Status]
		if value > severity {
			severity, result.OverallState = value, section.Status
		}
	}
}

func readinessHead(head readiness.Head) cliresult.Head {
	if !head.Exists {
		return cliresult.Head{Exists: false}
	}
	id := head.CommitID.String()
	seq := head.CommitSeq.String()
	at := strconv.FormatInt(head.CommittedAt.UnixMicro(), 10)
	tz := head.CommittedTZ.String()
	return cliresult.Head{Exists: true, CommitID: &id, CommitSeq: &seq, CommittedAt: &at, CommittedTZ: &tz}
}

func decimal(value uint64) *string {
	text := strconv.FormatUint(value, 10)
	return &text
}

func decimalInt(value int64) *string {
	text := strconv.FormatInt(value, 10)
	return &text
}

func boolPointer(value bool) *bool { return &value }

// RenderText is the sole human-readable M7 success renderer. It consumes only
// validated closed diagnostics fields and therefore cannot print a raw path,
// content value, digest, or underlying error string.
func RenderText(writer io.Writer, result *cliresult.DiagnosticsResult) error {
	if writer == nil || result == nil {
		return errors.New("diagnostics: text renderer requires output and result")
	}
	if err := cliresult.NewSuccess(cliresult.CommandAdminDiagnostics, result).Validate(); err != nil {
		return err
	}
	var output strings.Builder
	output.WriteString("overall_state: ")
	output.WriteString(result.OverallState)
	output.WriteByte('\n')
	if result.CapturedHead == nil {
		output.WriteString("captured_head: unavailable\n")
	} else if !result.CapturedHead.Exists {
		output.WriteString("captured_head: empty\n")
	} else {
		output.WriteString("captured_head: ")
		output.WriteString(*result.CapturedHead.CommitID)
		output.WriteByte('@')
		output.WriteString(*result.CapturedHead.CommitSeq)
		output.WriteByte('\n')
	}
	for index, section := range result.Sections {
		output.WriteString(strconv.FormatInt(int64((index+1)*10), 10))
		output.WriteByte(' ')
		output.WriteString(section.SectionCode)
		output.WriteByte(' ')
		output.WriteString(section.Status)
		output.WriteByte('\n')
	}
	_, err := io.WriteString(writer, output.String())
	return err
}
