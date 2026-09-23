package diagnostics

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	"mahoroba.local/mahoroba/internal/app"
	"mahoroba.local/mahoroba/internal/blob"
	"mahoroba.local/mahoroba/internal/canonical"
	"mahoroba.local/mahoroba/internal/cliresult"
	"mahoroba.local/mahoroba/internal/domain"
	"mahoroba.local/mahoroba/internal/fssecure"
	"mahoroba.local/mahoroba/internal/generation"
	"mahoroba.local/mahoroba/internal/hostlock"
	"mahoroba.local/mahoroba/internal/memory"
	"mahoroba.local/mahoroba/internal/namespacelock"
	"mahoroba.local/mahoroba/internal/projection"
	"mahoroba.local/mahoroba/internal/restore"
	storesqlite "mahoroba.local/mahoroba/internal/store/sqlite"
)

func TestDiagnosticsAbsentTargetReturnsExactThirteenSections(t *testing.T) {
	parent := t.TempDir()
	target := filepath.Join(parent, "absent")
	result, err := Inspect(context.Background(), Request{
		DataDir: target, DatabaseFilename: "mahoroba.db", BlobRoot: filepath.Join(target, "blobs"), MaxAttempts: 3,
	})
	skipDiagnosticWindowsSandbox(t, err)
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Sections) != 13 || result.CapturedHead != nil || result.OverallState != cliresult.DiagnosticStatusUnknown {
		t.Fatalf("absent result = %+v", result)
	}
	if result.Sections[12].Status != cliresult.DiagnosticStatusOK {
		t.Fatalf("publish section = %+v", result.Sections[12])
	}
	if err := cliresult.NewSuccess(cliresult.CommandAdminDiagnostics, result).Validate(); err != nil {
		t.Fatalf("closed result = %v", err)
	}
	if _, err := os.Lstat(target); !os.IsNotExist(err) {
		t.Fatalf("diagnostics created absent target: %v", err)
	}
}

func TestDiagnosticsInvalidDatabaseIsSanitizedAndSectionIsolated(t *testing.T) {
	target := managedDiagnosticTarget(t)
	secret := "RAW-CONTENT-SHOULD-NEVER-APPEAR"
	databasePath := filepath.Join(target, "mahoroba.db")
	if err := os.WriteFile(databasePath, []byte(secret), 0o600); err != nil {
		t.Fatal(err)
	}
	result, err := Inspect(context.Background(), Request{
		DataDir: target, DatabaseFilename: "mahoroba.db", BlobRoot: filepath.Join(target, "blobs"), MaxAttempts: 3,
	})
	skipDiagnosticWindowsSandbox(t, err)
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Sections) != 13 || result.Sections[1].Status != cliresult.DiagnosticStatusError {
		t.Fatalf("invalid DB result = %+v", result)
	}
	mandatoryDetails := result.Sections[10].Details.(*cliresult.MandatoryWorkDetails)
	if mandatoryDetails.DialogueScanComplete || mandatoryDetails.MemoryScanComplete {
		t.Fatalf("unavailable mandatory scan reported completion: %+v", mandatoryDetails)
	}
	var stdout, stderr bytes.Buffer
	if _, err := cliresult.Render(&stdout, &stderr, cliresult.NewSuccess(cliresult.CommandAdminDiagnostics, result)); err != nil {
		t.Fatal(err)
	}
	output := stdout.String() + stderr.String()
	for _, forbidden := range []string{secret, target, databasePath, "database disk image is malformed"} {
		if strings.Contains(output, forbidden) {
			t.Fatalf("diagnostics leaked %q in %s", forbidden, output)
		}
	}
	after, err := os.ReadFile(databasePath)
	if err != nil || string(after) != secret {
		t.Fatalf("invalid database changed: %q, %v", after, err)
	}
}

func TestDiagnosticsCleanEmptyDatabaseUsesClosedReadinessReasons(t *testing.T) {
	target := managedDiagnosticTarget(t)
	store, err := storesqlite.Open(context.Background(), filepath.Join(target, "mahoroba.db"))
	if err == nil {
		if err := store.Close(); err != nil {
			t.Fatal(err)
		}
	} else if _, statErr := os.Stat(filepath.Join(target, "mahoroba.db")); statErr != nil {
		// A concurrently updated schema trust anchor can make Open's final exact
		// gate fail after the migration transaction has durably built the DB.
		// The ungated diagnostic opener must still inspect that existing file.
		t.Fatalf("create migrated diagnostic fixture: %v (stat %v)", err, statErr)
	}
	result, err := Inspect(context.Background(), Request{
		DataDir: target, DatabaseFilename: "mahoroba.db", BlobRoot: filepath.Join(target, "blobs"), MaxAttempts: 3,
	})
	skipDiagnosticWindowsSandbox(t, err)
	if err != nil {
		t.Fatal(err)
	}
	if err := cliresult.NewSuccess(cliresult.CommandAdminDiagnostics, result).Validate(); err != nil {
		t.Fatal(err)
	}
	readinessDetails := result.Sections[7].Details.(*cliresult.ServiceReadinessDetails)
	mandatorySection := result.Sections[10]
	mandatoryDetails := mandatorySection.Details.(*cliresult.MandatoryWorkDetails)
	if result.Sections[3].Status != cliresult.DiagnosticStatusOK ||
		result.CapturedHead == nil || result.CapturedHead.Exists ||
		len(readinessDetails.ReasonCodes) != 1 || readinessDetails.ReasonCodes[0] != cliresult.HealthReasonResidentUnselected {
		t.Fatalf("clean diagnostics = %+v", result)
	}
	if mandatorySection.Status != cliresult.DiagnosticStatusOK ||
		!mandatoryDetails.DialogueScanComplete || !mandatoryDetails.MemoryScanComplete ||
		mandatoryDetails.DialogueScannedCandidates == nil || *mandatoryDetails.DialogueScannedCandidates != "0" ||
		mandatoryDetails.MemoryScannedCandidates == nil || *mandatoryDetails.MemoryScannedCandidates != "0" {
		t.Fatalf("clean mandatory-work diagnostics = %+v", mandatorySection)
	}
}

func TestCOV56DiagnosticsRequireExactMemoryProjectionTarget(t *testing.T) {
	residentID, err := canonical.ParseID("01ARZ3NDEKTSV4RRFFQ69G5FAV")
	if err != nil {
		t.Fatal(err)
	}
	otherResidentID, err := canonical.ParseID("01ARZ3NDEKTSV4RRFFQ69G5FAW")
	if err != nil {
		t.Fatal(err)
	}
	base := diagnosticMemoryTargetStatuses(residentID)
	base = append(base, diagnosticMemoryTargetStatuses(otherResidentID)...)
	base[0], base[1] = base[1], base[0]
	if !exactMemoryProjectionTarget(base, residentID) {
		t.Fatal("reordered exact target was rejected")
	}

	tests := []struct {
		name   string
		mutate func([]projection.Status) []projection.Status
	}{
		{name: "four millisecond as-of skew", mutate: func(values []projection.Status) []projection.Status {
			diagnosticStatus(t, values, residentID, projection.RuntimeStatesName).Watermark.AsOf += canonical.Instant(4_000)
			return values
		}},
		{name: "timezone mismatch", mutate: func(values []projection.Status) []projection.Status {
			diagnosticStatus(t, values, residentID, projection.RuntimeStatesName).Watermark.AsOfTZ = canonical.MustTimezone("Asia/Tokyo")
			return values
		}},
		{name: "source sequence mismatch", mutate: func(values []projection.Status) []projection.Status {
			diagnosticStatus(t, values, residentID, projection.RuntimeStatesName).Watermark.SourceCommitSeq = 8
			return values
		}},
		{name: "nil claim watermark", mutate: func(values []projection.Status) []projection.Status {
			diagnosticStatus(t, values, residentID, projection.ClaimStatesName).Watermark = nil
			return values
		}},
		{name: "missing runtime status", mutate: func(values []projection.Status) []projection.Status {
			for index := range values {
				if values[index].ResidentID == residentID && values[index].Name == projection.RuntimeStatesName {
					return append(values[:index], values[index+1:]...)
				}
			}
			return values
		}},
		{name: "duplicate claim status", mutate: func(values []projection.Status) []projection.Status {
			duplicate := *diagnosticStatus(t, values, residentID, projection.ClaimStatesName)
			watermark := *duplicate.Watermark
			duplicate.Watermark = &watermark
			return append(values, duplicate)
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if exactMemoryProjectionTarget(test.mutate(cloneDiagnosticStatuses(base)), residentID) {
				t.Fatal("inexact target was accepted")
			}
		})
	}
}

func diagnosticMemoryTargetStatuses(residentID canonical.ID) []projection.Status {
	statuses := make([]projection.Status, 0, 2)
	for _, name := range []projection.Name{projection.ClaimStatesName, projection.RuntimeStatesName} {
		watermark := projection.Watermark{
			ProjectionName: name, ResidentID: residentID, ProjectionVersion: "v1", SourceCommitSeq: 7,
			AsOf: canonical.Instant(1_700_000_000_000_000), AsOfTZ: canonical.MustTimezone("UTC"),
		}
		statuses = append(statuses, projection.Status{
			ResidentID: residentID, Name: name, Built: true, UpToDate: true, Watermark: &watermark,
		})
	}
	return statuses
}

func cloneDiagnosticStatuses(values []projection.Status) []projection.Status {
	cloned := append([]projection.Status(nil), values...)
	for index := range cloned {
		if cloned[index].Watermark != nil {
			watermark := *cloned[index].Watermark
			cloned[index].Watermark = &watermark
		}
	}
	return cloned
}

func diagnosticStatus(
	t *testing.T,
	statuses []projection.Status,
	residentID canonical.ID,
	name projection.Name,
) *projection.Status {
	t.Helper()
	for index := range statuses {
		if statuses[index].ResidentID == residentID && statuses[index].Name == name {
			return &statuses[index]
		}
	}
	t.Fatalf("status %s/%s not found", residentID, name)
	return nil
}

func TestCOVR02MemoryDiagnosticsPaginateWithinInvocationCandidateCap(t *testing.T) {
	residentID, err := canonical.ParseID("01ARZ3NDEKTSV4RRFFQ69G5FAV")
	if err != nil {
		t.Fatal(err)
	}
	firstEventID, _ := canonical.ParseID("01ARZ3NDEKTSV4RRFFQ69G5FAW")
	secondEventID, _ := canonical.ParseID("01ARZ3NDEKTSV4RRFFQ69G5FAX")
	after := canonical.Seq(2)
	through := canonical.Seq(4)
	repository := &memoryDiagnosticRepositoryStub{pages: []domain.MemoryDiscoveryResult{
		{
			Work: &domain.MemoryExtractionWork{
				SourceEvent: domain.Event{ID: secondEventID}, IdempotencyKey: "memory/z", State: domain.WorkPending,
			},
			NextCursor:        &domain.MemoryDiscoveryCursor{AfterSeq: &after, CycleThroughSeq: through},
			CandidatesScanned: 2,
		},
		{
			Work: &domain.MemoryExtractionWork{
				SourceEvent: domain.Event{ID: firstEventID}, IdempotencyKey: "memory/a", State: domain.WorkRetryPending,
			},
			CandidatesScanned: 1, CycleComplete: true,
		},
	}}

	work, scanned, complete, err := discoverMemoryWorkDiagnostic(
		context.Background(), repository, residentID, 3, 10,
	)
	if err != nil {
		t.Fatal(err)
	}
	if !complete || scanned != 3 || len(work) != 2 ||
		work[0].IdempotencyKey != "memory/a" || work[1].IdempotencyKey != "memory/z" {
		t.Fatalf("memory diagnostic result work=%+v scanned=%d complete=%t", work, scanned, complete)
	}
	if len(repository.requests) != 2 || repository.requests[0].Cursor != nil ||
		repository.requests[1].Cursor == nil || repository.requests[1].Cursor.AfterSeq == nil ||
		*repository.requests[1].Cursor.AfterSeq != after {
		t.Fatalf("memory diagnostic cursors = %+v", repository.requests)
	}
	if repository.requests[0].Budget.Candidates != 10 || repository.requests[1].Budget.Candidates != 8 {
		t.Fatalf("memory diagnostic budgets = %+v", repository.requests)
	}
	if len(repository.reextractionRequests) != 1 || repository.reextractionRequests[0].Cursor != nil ||
		repository.reextractionRequests[0].Budget.Candidates != 7 {
		t.Fatalf("memory re-extraction diagnostic requests = %+v", repository.reextractionRequests)
	}
}

func TestCOVR02MemoryDiagnosticsReportIncompleteAtCandidateCap(t *testing.T) {
	residentID, _ := canonical.ParseID("01ARZ3NDEKTSV4RRFFQ69G5FAV")
	eventID, _ := canonical.ParseID("01ARZ3NDEKTSV4RRFFQ69G5FAW")
	after := canonical.Seq(2)
	repository := &memoryDiagnosticRepositoryStub{pages: []domain.MemoryDiscoveryResult{{
		Work: &domain.MemoryExtractionWork{
			SourceEvent: domain.Event{ID: eventID}, IdempotencyKey: "memory/a", State: domain.WorkPending,
		},
		NextCursor:        &domain.MemoryDiscoveryCursor{AfterSeq: &after, CycleThroughSeq: after},
		CandidatesScanned: 2, BudgetExhausted: true,
	}}}

	work, scanned, complete, err := discoverMemoryWorkDiagnostic(
		context.Background(), repository, residentID, 3, 2,
	)
	if err != nil {
		t.Fatal(err)
	}
	if complete || scanned != 2 || len(work) != 1 || len(repository.requests) != 1 {
		t.Fatalf("capped memory diagnostic result work=%+v scanned=%d complete=%t requests=%d",
			work, scanned, complete, len(repository.requests))
	}
	if repository.requests[0].Budget.Candidates != 2 || repository.requests[0].Budget.PageSize != 2 ||
		repository.requests[0].Budget.Pages != 1 {
		t.Fatalf("capped memory diagnostic budget = %+v", repository.requests[0].Budget)
	}
}

func TestCOVR02MemoryDiagnosticsReturnQueryFailureWithoutCursorProgress(t *testing.T) {
	residentID, _ := canonical.ParseID("01ARZ3NDEKTSV4RRFFQ69G5FAV")
	repository := &memoryDiagnosticRepositoryStub{err: errors.New("query failed")}

	work, scanned, complete, err := discoverMemoryWorkDiagnostic(
		context.Background(), repository, residentID, 3, 10,
	)
	if err == nil || complete || scanned != 0 || work != nil {
		t.Fatalf("failed memory diagnostic result work=%+v scanned=%d complete=%t err=%v",
			work, scanned, complete, err)
	}
}

func TestCOVR02MemoryDiagnosticsIncludeRetryPendingReextraction(t *testing.T) {
	residentID, _ := canonical.ParseID("01ARZ3NDEKTSV4RRFFQ69G5FAV")
	eventID, _ := canonical.ParseID("01ARZ3NDEKTSV4RRFFQ69G5FAW")
	runID, _ := canonical.ParseID("01ARZ3NDEKTSV4RRFFQ69G5FAX")
	commitSeq := canonical.CommitSeq(42)
	position := domain.MemoryReextractionCommitCursor{
		CommitID: eventID, CommitSeq: commitSeq, AfterRunID: &runID,
	}
	repository := &memoryDiagnosticRepositoryStub{
		pages: []domain.MemoryDiscoveryResult{{CandidatesScanned: 2, CycleComplete: true}},
		reextractionPages: []domain.MemoryReextractionDiscoveryResult{
			{
				Work: &domain.MemoryExtractionWork{
					SourceEvent:    domain.Event{ID: eventID},
					IdempotencyKey: "memory_reextract:v1:" + eventID.String() + ":01ARZ3NDEKTSV4RRFFQ69G5FAY",
					State:          domain.WorkRetryPending,
				},
				CandidatesScanned: 1,
				NextCursor: &domain.MemoryReextractionDiscoveryCursor{
					CycleThroughCommitSeq: commitSeq, ActiveCommit: &position,
				},
			},
			{CycleComplete: true},
		},
	}

	work, scanned, complete, err := discoverMemoryWorkDiagnostic(
		context.Background(), repository, residentID, 3, 10,
	)
	if err != nil {
		t.Fatal(err)
	}
	if !complete || scanned != 3 || len(work) != 1 || work[0].State != domain.WorkRetryPending {
		t.Fatalf("re-extraction diagnostic result work=%+v scanned=%d complete=%t", work, scanned, complete)
	}
	counts := make(map[string]uint64)
	if !incrementWorkCount(counts, "memory", work[0].State) || counts["memory_retry_pending"] != 1 {
		t.Fatalf("re-extraction retry_pending was not folded into mandatory memory counts: %+v", counts)
	}
	if len(repository.reextractionRequests) != 2 || repository.reextractionRequests[0].Cursor != nil ||
		repository.reextractionRequests[0].Budget.Candidates != 8 ||
		repository.reextractionRequests[1].Cursor == nil ||
		repository.reextractionRequests[1].Cursor.ActiveCommit == nil ||
		repository.reextractionRequests[1].Cursor.ActiveCommit.CommitID != eventID ||
		repository.reextractionRequests[1].Cursor.ActiveCommit.CommitSeq != commitSeq ||
		repository.reextractionRequests[1].Cursor.ActiveCommit.AfterRunID == nil ||
		*repository.reextractionRequests[1].Cursor.ActiveCommit.AfterRunID != runID ||
		repository.reextractionRequests[1].Cursor.CycleThroughCommitSeq != commitSeq ||
		repository.reextractionRequests[1].Budget.Candidates != 7 {
		t.Fatalf("re-extraction diagnostic request = %+v", repository.reextractionRequests)
	}
}

func TestCOVR02MemoryDiagnosticsShareCandidateCapWithReextraction(t *testing.T) {
	residentID, _ := canonical.ParseID("01ARZ3NDEKTSV4RRFFQ69G5FAV")
	runID, _ := canonical.ParseID("01ARZ3NDEKTSV4RRFFQ69G5FAW")
	commitSeq := canonical.CommitSeq(42)
	position := domain.MemoryReextractionCommitCursor{
		CommitID: residentID, CommitSeq: commitSeq, AfterRunID: &runID,
	}
	repository := &memoryDiagnosticRepositoryStub{
		pages: []domain.MemoryDiscoveryResult{{CandidatesScanned: 2, CycleComplete: true}},
		reextractionPages: []domain.MemoryReextractionDiscoveryResult{{
			CandidatesScanned: 3,
			BudgetExhausted:   true,
			NextCursor: &domain.MemoryReextractionDiscoveryCursor{
				CycleThroughCommitSeq: commitSeq, ActiveCommit: &position,
			},
		}},
	}

	work, scanned, complete, err := discoverMemoryWorkDiagnostic(
		context.Background(), repository, residentID, 3, 5,
	)
	if err != nil {
		t.Fatal(err)
	}
	if complete || scanned != 5 || len(work) != 0 || len(repository.reextractionRequests) != 1 {
		t.Fatalf("shared-cap diagnostic result work=%+v scanned=%d complete=%t requests=%d",
			work, scanned, complete, len(repository.reextractionRequests))
	}
	budget := repository.reextractionRequests[0].Budget
	if budget.Candidates != 3 || budget.PageSize != 3 || budget.Pages != 1 {
		t.Fatalf("shared re-extraction budget = %+v", budget)
	}
}

func TestCOVR02MemoryDiagnosticsContinueAfterCeilingOnlyProgress(t *testing.T) {
	residentID, _ := canonical.ParseID("01ARZ3NDEKTSV4RRFFQ69G5FAV")
	commitSeq := canonical.CommitSeq(42)
	repository := &memoryDiagnosticRepositoryStub{
		pages: []domain.MemoryDiscoveryResult{{CycleComplete: true}},
		reextractionPages: []domain.MemoryReextractionDiscoveryResult{
			{
				NextCursor: &domain.MemoryReextractionDiscoveryCursor{
					CycleThroughCommitSeq: commitSeq,
				},
				BudgetExhausted: true,
			},
			{CandidatesScanned: 1, CycleComplete: true},
		},
	}

	work, scanned, complete, err := discoverMemoryWorkDiagnostic(
		context.Background(), repository, residentID, 3, 4,
	)
	if err != nil {
		t.Fatal(err)
	}
	if !complete || scanned != 1 || len(work) != 0 || len(repository.reextractionRequests) != 2 {
		t.Fatalf("ceiling-only diagnostic result work=%+v scanned=%d complete=%t requests=%d",
			work, scanned, complete, len(repository.reextractionRequests))
	}
	if repository.reextractionRequests[1].Cursor == nil ||
		repository.reextractionRequests[1].Cursor.CycleThroughCommitSeq != commitSeq ||
		repository.reextractionRequests[1].Budget.Candidates != 4 {
		t.Fatalf("ceiling-only diagnostic continuation = %+v", repository.reextractionRequests)
	}
}

func TestCOVR02MemoryDiagnosticsReturnReextractionQueryFailure(t *testing.T) {
	residentID, _ := canonical.ParseID("01ARZ3NDEKTSV4RRFFQ69G5FAV")
	repository := &memoryDiagnosticRepositoryStub{
		pages:           []domain.MemoryDiscoveryResult{{CandidatesScanned: 2, CycleComplete: true}},
		reextractionErr: errors.New("re-extraction query failed"),
	}

	work, scanned, complete, err := discoverMemoryWorkDiagnostic(
		context.Background(), repository, residentID, 3, 10,
	)
	if err == nil || complete || scanned != 2 || work != nil || len(repository.reextractionRequests) != 1 {
		t.Fatalf("failed re-extraction diagnostic result work=%+v scanned=%d complete=%t err=%v requests=%d",
			work, scanned, complete, err, len(repository.reextractionRequests))
	}
}

type memoryDiagnosticRepositoryStub struct {
	pages                []domain.MemoryDiscoveryResult
	err                  error
	requests             []domain.MemoryDiscoveryRequest
	reextractionPages    []domain.MemoryReextractionDiscoveryResult
	reextractionErr      error
	reextractionRequests []domain.MemoryReextractionDiscoveryRequest
}

func (repository *memoryDiagnosticRepositoryStub) DiscoverMemoryExtractionWork(
	_ context.Context,
	_ canonical.ID,
	request domain.MemoryDiscoveryRequest,
) (domain.MemoryDiscoveryResult, error) {
	repository.requests = append(repository.requests, request)
	if repository.err != nil {
		return domain.MemoryDiscoveryResult{}, repository.err
	}
	index := len(repository.requests) - 1
	if index >= len(repository.pages) {
		return domain.MemoryDiscoveryResult{}, errors.New("unexpected memory diagnostic request")
	}
	return repository.pages[index], nil
}

func (repository *memoryDiagnosticRepositoryStub) DiscoverMemoryReextractionWork(
	_ context.Context,
	_ canonical.ID,
	request domain.MemoryReextractionDiscoveryRequest,
) (domain.MemoryReextractionDiscoveryResult, error) {
	repository.reextractionRequests = append(repository.reextractionRequests, request)
	if repository.reextractionErr != nil {
		return domain.MemoryReextractionDiscoveryResult{}, repository.reextractionErr
	}
	index := len(repository.reextractionRequests) - 1
	if index >= len(repository.reextractionPages) {
		return domain.MemoryReextractionDiscoveryResult{CycleComplete: true}, nil
	}
	return repository.reextractionPages[index], nil
}

func TestM7DiagnosticsHalfErasedAliasIsFatalReadOnlyAndRedacted(t *testing.T) {
	dataDir := managedDiagnosticTarget(t)
	databasePath := filepath.Join(dataDir, "mahoroba.db")
	statement := "DIAGNOSTIC-RAW-ALIAS-CONTENT-MUST-NOT-LEAK"
	makeDiagnosticClaimFixture(t, dataDir, databasePath, statement)
	makeDiagnosticHalfErasedAlias(t, databasePath)
	beforeCommits, beforeFindings := diagnosticMutationCounts(t, databasePath)

	result, err := Inspect(context.Background(), Request{
		DataDir: dataDir, DatabaseFilename: "mahoroba.db", BlobRoot: filepath.Join(dataDir, "blobs"), MaxAttempts: 3,
	})
	if err != nil {
		t.Fatal(err)
	}
	content := result.Sections[5]
	contentDetails := content.Details.(*cliresult.ContentBlobIntegrityDetails)
	readinessSection := result.Sections[7]
	readinessDetails := readinessSection.Details.(*cliresult.ServiceReadinessDetails)
	if result.OverallState != cliresult.DiagnosticStatusError ||
		content.Status != cliresult.DiagnosticStatusError || contentDetails.ErrorCode == nil || *contentDetails.ErrorCode != errorIntegrity ||
		readinessSection.Status != cliresult.DiagnosticStatusError || readinessDetails.ErrorCode == nil || *readinessDetails.ErrorCode != errorIntegrity ||
		readinessDetails.Ready != nil || len(readinessDetails.ReasonCodes) != 0 {
		t.Fatalf("half-erased alias diagnostics = %+v", result)
	}
	if err := cliresult.NewSuccess(cliresult.CommandAdminDiagnostics, result).Validate(); err != nil {
		t.Fatal(err)
	}
	var stdout, stderr bytes.Buffer
	if _, err := cliresult.Render(&stdout, &stderr, cliresult.NewSuccess(cliresult.CommandAdminDiagnostics, result)); err != nil {
		t.Fatal(err)
	}
	combined := stdout.String() + stderr.String()
	for _, forbidden := range []string{statement, dataDir, databasePath} {
		if strings.Contains(combined, forbidden) {
			t.Fatalf("fatal alias diagnostics leaked %q", forbidden)
		}
	}
	afterCommits, afterFindings := diagnosticMutationCounts(t, databasePath)
	if afterCommits != beforeCommits || afterFindings != beforeFindings {
		t.Fatalf("diagnostics mutated fatal alias DB: before=%d/%d after=%d/%d",
			beforeCommits, beforeFindings, afterCommits, afterFindings)
	}
}

func managedDiagnosticTarget(t *testing.T) string {
	t.Helper()
	dataDir := filepath.Join(t.TempDir(), "data")
	lock, err := hostlock.Acquire(dataDir)
	skipDiagnosticWindowsSandbox(t, err)
	if err != nil {
		t.Fatal(err)
	}
	if err := lock.Close(); err != nil {
		t.Fatal(err)
	}
	policy, err := fssecure.CurrentSecurityPolicy()
	if err != nil {
		t.Fatal(err)
	}
	root, err := fssecure.OpenRoot(dataDir, policy)
	if err != nil {
		t.Fatal(err)
	}
	database, err := root.CreateRegular("mahoroba.db")
	if err != nil {
		_ = root.Close()
		t.Fatal(err)
	}
	if err := errors.Join(database.Close(), root.Close()); err != nil {
		t.Fatal(err)
	}
	return dataDir
}

type diagnosticClaimGenerator struct{ statement string }

func (generator diagnosticClaimGenerator) Stream(
	_ context.Context,
	request generation.Request,
	_ generation.DeltaSink,
) (generation.Result, error) {
	if request.Purpose == string(domain.GenerationPurposeMemoryExtraction) {
		return generation.Result{Text: `{"claims":[{"grade":"stated","perspective":"source_actor","source_quote":"alias","statement":"` + generator.statement + `","subject":"source_actor","temporal_kind":"stable"}],"version":"memory-extraction-output-v1"}`}, nil
	}
	return generation.Result{Text: "dialogue response"}, nil
}

func makeDiagnosticClaimFixture(t *testing.T, dataDir, databasePath, statement string) {
	t.Helper()
	ctx := context.Background()
	store, err := storesqlite.Open(ctx, databasePath)
	if err != nil {
		t.Fatal(err)
	}
	objects, err := blob.NewFileStore(filepath.Join(dataDir, "blobs"))
	if err != nil {
		_ = store.Close()
		t.Fatal(err)
	}
	ids := canonical.NewSecureIDGenerator()
	writer, err := canonical.OpenWriter(ctx, canonical.WriterOptions{
		Backend: store.Canonical(), IDs: ids, Clock: canonical.SystemClock{},
		Timezone: canonical.MustTimezone("UTC"), QueueCapacity: 16,
	})
	if err != nil {
		_ = store.Close()
		t.Fatal(err)
	}
	registry, err := storesqlite.ActiveProjectionRegistry()
	if err != nil {
		_ = writer.Close(ctx)
		_ = store.Close()
		t.Fatal(err)
	}
	projectionStore := store.Projection()
	coordinator, err := projection.NewCoordinator(projection.CoordinatorOptions{
		Registry: registry, Source: projectionStore, Store: projectionStore,
		Clock: canonical.SystemClock{}, Timezone: canonical.MustTimezone("UTC"),
		ScanInterval: time.Hour, AsOfRefreshInterval: time.Hour,
		RebuildRetryInterval: time.Minute, MaxStaleness: time.Hour,
	})
	if err != nil {
		_ = writer.Close(ctx)
		_ = store.Close()
		t.Fatal(err)
	}
	application, err := app.New(app.Options{
		Writer: writer, CommitNotifier: coordinator, Repository: store.Canonical(), IDs: ids, Clock: canonical.SystemClock{},
		Timezone: canonical.MustTimezone("UTC"), Blobs: objects,
		Generator: diagnosticClaimGenerator{statement: statement}, Provider: "test", Model: "test-model",
		MaxAttempts: 1, RetryBackoff: []time.Duration{}, MaxInputBytes: 4096, MaxOutputBytes: 4096,
		SafetyScanInterval: time.Hour,
	})
	if err != nil {
		_ = writer.Close(ctx)
		_ = store.Close()
		t.Fatal(err)
	}
	state, err := application.BootstrapInit(ctx, app.BootstrapInput{
		OwnerName: "owner", Name: "resident", SeedKey: "diagnostic-alias", Principles: "principles",
	})
	if err != nil || len(state.Residents) != 1 {
		_ = writer.Close(ctx)
		_ = store.Close()
		t.Fatalf("bootstrap diagnostic alias fixture: residents=%d err=%v", len(state.Residents), err)
	}
	residentID := state.Residents[0].ResidentID
	if err := application.ApprovePrinciples(ctx, residentID); err != nil {
		t.Fatal(err)
	}
	if err := application.FinalizeBootstrap(ctx, residentID, "persona",
		`{"mandatory_event_types":[],"memory_recall_enabled":false,"version":"memory-policy-v1"}`); err != nil {
		t.Fatal(err)
	}
	if err := application.SelectResident(ctx, residentID); err != nil {
		t.Fatal(err)
	}
	if _, err := application.ActivateMemoryPolicyV4(ctx, app.ActivateMemoryPolicyV4Options{
		ResidentID: residentID, ExpectedFrom: memory.PolicyVersionV1,
		AcknowledgeRecallEnable: true, AcknowledgeSelfTalkExtraction: true,
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := application.Ingress(ctx, "alias source"); err != nil {
		t.Fatal(err)
	}
	if err := application.ProcessResident(ctx, residentID); err != nil {
		t.Fatal(err)
	}
	if err := errors.Join(writer.Close(ctx), store.Close()); err != nil {
		t.Fatal(err)
	}
}

func makeDiagnosticHalfErasedAlias(t *testing.T, databasePath string) {
	t.Helper()
	database, err := sql.Open("sqlite", databasePath)
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	tx, err := database.Begin()
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	var sourceClaim, triggerSQL string
	if err := tx.QueryRow(`SELECT claim_id FROM claims ORDER BY claim_id LIMIT 1`).Scan(&sourceClaim); err != nil {
		t.Fatal(err)
	}
	aliasID, err := canonical.NewSecureIDGenerator().New()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec(`INSERT INTO claims
		SELECT ?, canonical_commit_id, owner_resident_id, subject_principal_id,
		perspective_principal_id, kind, temporal_kind, statement_content_id,
		statement_hash, statement_hash_algorithm, statement_normalization_version,
		created_by_run_id, recorded_at, recorded_tz
		FROM claims WHERE claim_id = ?`, aliasID.String(), sourceClaim); err != nil {
		t.Fatal(err)
	}
	if err := tx.QueryRow(`SELECT sql FROM sqlite_schema
		WHERE type = 'trigger' AND name = 'trg_claims_update_contract'`).Scan(&triggerSQL); err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec(`DROP TRIGGER trg_claims_update_contract`); err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec(`UPDATE claims SET statement_hash = NULL WHERE claim_id = ?`, aliasID.String()); err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec(triggerSQL); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
}

func diagnosticMutationCounts(t *testing.T, databasePath string) (commits, findings int) {
	t.Helper()
	database, err := sql.Open("sqlite", databasePath)
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	if err := database.QueryRow(`SELECT
		(SELECT COUNT(*) FROM canonical_commits),
		(SELECT COUNT(*) FROM integrity_findings)`).Scan(&commits, &findings); err != nil {
		t.Fatal(err)
	}
	return commits, findings
}

func TestDiagnosticsReportsAuthenticatedAbsentRestoreStaging(t *testing.T) {
	parent := t.TempDir()
	target := filepath.Join(parent, "restored")
	restoreID, err := canonical.ParseID("01ARZ3NDEKTSV4RRFFQ69G5FAV")
	if err != nil {
		t.Fatal(err)
	}
	stagingName := ".restored.restore-staging." + restoreID.String()
	stagingPath := filepath.Join(parent, stagingName)
	lock, err := namespacelock.AcquireExistingParent(stagingPath)
	skipDiagnosticWindowsSandbox(t, err)
	if err != nil {
		t.Fatal(err)
	}
	staging, err := lock.OpenOrCreateTarget(0o700)
	if err != nil {
		_ = lock.Close()
		t.Fatal(err)
	}
	defer staging.Close()
	defer lock.Close()
	marker := struct {
		FormatVersion  string       `json:"format_version"`
		ManifestSHA256 string       `json:"manifest_sha256"`
		RestoreID      canonical.ID `json:"restore_id"`
		StagingName    string       `json:"staging_basename"`
		TargetName     string       `json:"target_basename"`
	}{restore.RestoreStagingFormat, "sha256:" + strings.Repeat("0", 64), restoreID, stagingName, "restored"}
	encoded, err := canonical.MarshalCanonical(marker)
	if err != nil {
		t.Fatal(err)
	}
	handle, err := staging.OpenOrCreateRegular(restore.RestoreStagingMarker, 0o600)
	if err != nil {
		_ = staging.Close()
		_ = lock.Close()
		if runtime.GOOS == "windows" && os.Getenv("CI") == "" && errors.Is(err, os.ErrPermission) {
			t.Skipf("protected marker creation unavailable in Windows test sandbox: %v", err)
		}
		t.Fatal(err)
	}
	if _, err := handle.Write(append(encoded.Bytes(), '\n')); err != nil {
		t.Fatal(err)
	}
	if err := errors.Join(handle.Sync(), handle.Close(), staging.Close(), lock.Close()); err != nil {
		t.Fatal(err)
	}

	result, err := Inspect(context.Background(), Request{
		DataDir: target, DatabaseFilename: "mahoroba.db", BlobRoot: filepath.Join(target, "blobs"), MaxAttempts: 3,
	})
	if err != nil {
		t.Fatal(err)
	}
	publish := result.Sections[12]
	details := publish.Details.(*cliresult.PublishStateDetails)
	if publish.Status != cliresult.DiagnosticStatusWarn || len(details.RestoreStaging) != 1 ||
		details.RestoreStaging[0].State != "prepared" || details.RestoreStaging[0].MarkerID == nil ||
		*details.RestoreStaging[0].MarkerID != restoreID.String() {
		t.Fatalf("publish state = %+v", publish)
	}
}

func TestDiagnosticsTextGoldenAndConcurrentObservers(t *testing.T) {
	const workers = 8
	results := make(chan *cliresult.DiagnosticsResult, workers)
	errorsChannel := make(chan error, workers)
	var wait sync.WaitGroup
	for index := 0; index < workers; index++ {
		wait.Add(1)
		go func() {
			defer wait.Done()
			parent := t.TempDir()
			target := filepath.Join(parent, "absent")
			result, err := Inspect(context.Background(), Request{
				DataDir: target, DatabaseFilename: "mahoroba.db", BlobRoot: filepath.Join(target, "blobs"), MaxAttempts: 3,
			})
			if err != nil {
				errorsChannel <- err
				return
			}
			results <- result
		}()
	}
	wait.Wait()
	close(errorsChannel)
	close(results)
	for err := range errorsChannel {
		skipDiagnosticWindowsSandbox(t, err)
		t.Fatal(err)
	}
	var first *cliresult.DiagnosticsResult
	for result := range results {
		if first == nil {
			first = result
		}
	}
	if first == nil {
		t.Fatal("no result")
	}
	var output bytes.Buffer
	if err := RenderText(&output, first); err != nil {
		t.Fatal(err)
	}
	want := "overall_state: unknown\n" +
		"captured_head: unavailable\n" +
		"10 schema unknown\n20 sqlite_quick_check unknown\n30 foreign_keys unknown\n" +
		"40 canonical_head unknown\n50 resident_ledger unknown\n60 content_blob_integrity unknown\n" +
		"70 runtime_selection unknown\n80 service_readiness unknown\n90 projections unknown\n" +
		"100 running_attempts unknown\n110 mandatory_work unknown\n120 blob_gc unknown\n130 publish_state ok\n"
	if output.String() != want {
		t.Fatalf("text output:\n%s\nwant:\n%s", output.String(), want)
	}
	var decoded map[string]any
	if err := json.Unmarshal([]byte(`{"safe":true}`), &decoded); err != nil || decoded["safe"] != true {
		t.Fatal("test JSON setup failed")
	}
}

func skipDiagnosticWindowsSandbox(t *testing.T, err error) {
	t.Helper()
	if runtime.GOOS == "windows" && os.Getenv("CI") == "" && errors.Is(err, os.ErrPermission) {
		t.Skipf("Windows desktop capability sandbox cannot exercise protected ACL: %v", err)
	}
}
