package sqlite

import (
	"context"
	"database/sql"
	"sync"
	"testing"

	"mahoroba.local/mahoroba/internal/canonical"
	"mahoroba.local/mahoroba/internal/integrityrun"
)

// TestM7IntegrityRunConcurrentAndReopenKeepsExactlyOneFinding closes the
// idempotency boundary with the real SQLite scanner, Canonical backend, and
// serialized Writer. In particular, it does not replace the database UNIQUE
// constraint or Writer revalidation with an in-memory fake.
func TestM7IntegrityRunConcurrentAndReopenKeepsExactlyOneFinding(t *testing.T) {
	ctx := context.Background()
	evidence := performM7ClaimErasure(t)
	restoreIntegrityEvidenceSchemaTriggers(t, evidence.DB)
	databasePath := sqliteDatabasePath(t, evidence.DB)

	liveStore := &Store{
		path: databasePath, writer: evidence.DB, reader: evidence.DB,
		writes: newWritePriorityGate(),
	}
	writer := openIntegrityEvidenceWriter(t, ctx, liveStore)
	services := make([]*integrityrun.Service, 2)
	for index := range services {
		service, err := integrityrun.New(integrityrun.Options{
			Writer: writer, Repository: liveStore.Canonical(),
			Scanner: liveStore.IntegrityScanner(), IDs: canonical.NewSecureIDGenerator(),
		})
		if err != nil {
			t.Fatal(err)
		}
		services[index] = service
	}

	start := make(chan struct{})
	errorsByRun := make(chan error, len(services))
	var ready sync.WaitGroup
	ready.Add(len(services))
	for _, service := range services {
		go func(service *integrityrun.Service) {
			ready.Done()
			<-start
			_, err := service.Run(ctx, nil)
			errorsByRun <- err
		}(service)
	}
	ready.Wait()
	close(start)
	for range services {
		if err := <-errorsByRun; err != nil {
			t.Fatalf("concurrent integrity Run: %v", err)
		}
	}
	assertExactIntegrityFingerprintCardinality(t, evidence.DB, 1)

	if err := writer.Close(ctx); err != nil {
		t.Fatal(err)
	}
	if err := evidence.DB.Close(); err != nil {
		t.Fatal(err)
	}

	reopened, err := Open(ctx, databasePath)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	reopenedWriter := openIntegrityEvidenceWriter(t, ctx, reopened)
	defer reopenedWriter.Close(ctx)
	reopenedService, err := integrityrun.New(integrityrun.Options{
		Writer: reopenedWriter, Repository: reopened.Canonical(),
		Scanner: reopened.IntegrityScanner(), IDs: canonical.NewSecureIDGenerator(),
	})
	if err != nil {
		t.Fatal(err)
	}
	result, err := reopenedService.Run(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	if result.CreatedFindings != 0 || result.ExistingFindings != 1 {
		t.Fatalf("reopened Run findings = created %d existing %d, want 0/1",
			result.CreatedFindings, result.ExistingFindings)
	}
	assertExactIntegrityFingerprintCardinality(t, reopened.Reader(), 1)
}

func restoreIntegrityEvidenceSchemaTriggers(t *testing.T, database *sql.DB) {
	t.Helper()
	for _, statement := range []string{
		`CREATE TRIGGER trg_content_erasure_events_no_delete BEFORE DELETE ON content_erasure_events BEGIN SELECT RAISE(ABORT, 'content_erasure_events is append-only'); END`,
		`CREATE TRIGGER trg_integrity_findings_no_delete BEFORE DELETE ON integrity_findings BEGIN SELECT RAISE(ABORT, 'integrity_findings is append-only'); END`,
		`CREATE TRIGGER trg_sessionization_policy_versions_no_update BEFORE UPDATE ON sessionization_policy_versions BEGIN SELECT RAISE(ABORT, 'sessionization_policy_versions is append-only'); END`,
	} {
		if _, err := database.Exec(statement); err != nil {
			t.Fatal(err)
		}
	}
}

func openIntegrityEvidenceWriter(t *testing.T, ctx context.Context, store *Store) *canonical.Writer {
	t.Helper()
	writer, err := canonical.OpenWriter(ctx, canonical.WriterOptions{
		Backend: store.Canonical(), IDs: canonical.NewSecureIDGenerator(),
		Clock: canonical.SystemClock{}, Timezone: canonical.MustTimezone("UTC"),
		QueueCapacity: 16,
	})
	if err != nil {
		t.Fatal(err)
	}
	return writer
}

func sqliteDatabasePath(t *testing.T, database *sql.DB) string {
	t.Helper()
	rows, err := database.Query(`PRAGMA database_list`)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	for rows.Next() {
		var sequence int
		var name, path string
		if err := rows.Scan(&sequence, &name, &path); err != nil {
			t.Fatal(err)
		}
		if name == "main" && path != "" {
			return path
		}
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	t.Fatal("SQLite main database path is unavailable")
	return ""
}

func assertExactIntegrityFingerprintCardinality(t *testing.T, database *sql.DB, want int) {
	t.Helper()
	var rows, fingerprints int
	if err := database.QueryRow(`SELECT COUNT(*), COUNT(DISTINCT finding_fingerprint)
		FROM integrity_findings WHERE rule_code = 'claim_statement_erased'`).Scan(
		&rows, &fingerprints,
	); err != nil {
		t.Fatal(err)
	}
	if rows != want || fingerprints != want {
		t.Fatalf("claim-statement finding rows/fingerprints = %d/%d, want %d/%d",
			rows, fingerprints, want, want)
	}
}
