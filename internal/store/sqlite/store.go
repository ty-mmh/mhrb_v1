// Package sqlite opens and validates the local SQLite canonical store.
package sqlite

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"time"

	"mahoroba.local/mahoroba/internal/readiness"

	_ "modernc.org/sqlite"
)

const (
	defaultBusyTimeout       = 5 * time.Second
	defaultReaderConnections = 4
)

// Options controls connection-pool mechanics without changing schema meaning.
type Options struct {
	BusyTimeout       time.Duration
	ReaderConnections int
}

// DefaultOptions returns the fixed Phase-0 opening profile.
func DefaultOptions() Options {
	return Options{
		BusyTimeout:       defaultBusyTimeout,
		ReaderConnections: defaultReaderConnections,
	}
}

// Store owns physically separate writer and reader pools for one local DB.
type Store struct {
	path              string
	writer            *sql.DB
	reader            *sql.DB
	report            SchemaReport
	writes            *writePriorityGate
	canonicalBoundary interface{ Verify() error }
	closeOnce         sync.Once
	closeErr          error
}

// BindCanonicalBoundary attaches the retained writable root/database identity
// to every subsequent writable Store transaction, including Canonical and
// Projection UoWs. It must be called once, before a Canonical Writer or
// Projection coordinator is opened. The boundary itself remains caller-owned
// and must outlive Store.
func (s *Store) BindCanonicalBoundary(boundary *BoundDatabase) error {
	if s == nil || s.writer == nil || boundary == nil || !boundary.writable {
		return ErrUnsafeDatabaseBoundary
	}
	want := filepath.Clean(filepath.Join(boundary.dataDir, boundary.databaseFilename))
	if filepath.Clean(s.path) != want {
		return fmt.Errorf("%w: store path differs from retained database", ErrUnsafeDatabaseBoundary)
	}
	if s.canonicalBoundary != nil {
		return fmt.Errorf("%w: Canonical boundary is already bound", ErrUnsafeDatabaseBoundary)
	}
	if err := boundary.Verify(); err != nil {
		return err
	}
	s.canonicalBoundary = boundary
	return nil
}

func (s *Store) verifyWritableBoundary(stage string) error {
	if s == nil || s.canonicalBoundary == nil {
		return nil
	}
	if err := s.canonicalBoundary.Verify(); err != nil {
		return fmt.Errorf("sqlite: %s: %w", stage, err)
	}
	return nil
}

// Inspection is the read-only database surface used by verification tools.
// It never initializes or migrates a database and owns no writer connection.
type Inspection struct {
	store     *Store
	cleanup   func() error
	closeOnce sync.Once
	closeErr  error
}

// DiagnosticInspection is the deliberately ungated, query-only surface used
// by offline diagnostics.  Unlike Inspection it can be returned for an older,
// incomplete, or corrupt database so each diagnostic section can report its
// own closed failure state.  It never owns a writer connection.
type DiagnosticInspection struct {
	store *Store
}

// Open applies the embedded baseline migrations and fails closed unless the
// resulting database exactly matches the v0.1.3 schema contract.
func Open(ctx context.Context, path string) (*Store, error) {
	return OpenWithOptions(ctx, path, DefaultOptions())
}

// OpenInspection opens an existing, fully migrated database through the
// query-only connection profile and runs the exact schema gate. Verification
// commands use this path so inspecting a database cannot trigger Canonical or
// generation recovery and cannot create or migrate an absent database.
func OpenInspection(ctx context.Context, path string) (*Inspection, error) {
	return openInspection(ctx, path, false)
}

// OpenBoundInspection opens the query-only schema-gated surface through a
// descriptor path supplied by a caller which retains and verifies the owning
// no-follow handle for the complete inspection lifetime. On Linux this is a
// /proc/self/fd path; on Windows descriptorpath has already proved that the
// fallback namespace path is the retained, write/delete-sharing-denied file.
func OpenBoundInspection(ctx context.Context, descriptorPath string) (*Inspection, error) {
	return openInspection(ctx, descriptorPath, false)
}

// OpenImmutableInspection is the closed-artifact form used only after every
// SQLite writer has closed and the main database file has been durably
// checkpointed. immutable=1 prevents a read-only verification pass from
// creating WAL shared-memory sidecars inside a sealed artifact payload.
func OpenImmutableInspection(ctx context.Context, path string) (*Inspection, error) {
	return openInspection(ctx, path, true)
}

// OpenDiagnosticInspection opens an existing database without running
// migrations, the schema gate, quick_check, or foreign_key_check. Connection
// establishment is intentionally lazy: a file whose SQLite header is corrupt
// still yields an inspection handle and individual queries report the damage.
// The DSN is mode=ro/query_only and therefore cannot create an absent DB or
// execute a mutation through this surface.
func OpenDiagnosticInspection(ctx context.Context, path string) (*DiagnosticInspection, error) {
	return openDiagnosticInspection(ctx, path, false)
}

// OpenBoundDiagnosticInspection is the descriptor-path form used when the
// caller already owns and retains a no-follow, no-write/no-delete-sharing file
// handle. On Linux that path is /proc/self/fd/N and is intentionally a
// symlink; authority comes from the retained descriptor rather than Lstat of
// a mutable namespace name.
func OpenBoundDiagnosticInspection(ctx context.Context, path string) (*DiagnosticInspection, error) {
	return openDiagnosticInspection(ctx, path, true)
}

func openDiagnosticInspection(ctx context.Context, path string, descriptorBound bool) (*DiagnosticInspection, error) {
	if ctx == nil {
		return nil, errors.New("open diagnostic SQLite reader: nil context")
	}
	options, err := normalizeOptions(DefaultOptions())
	if err != nil {
		return nil, err
	}
	absPath, err := normalizeLocalPath(path)
	if err != nil {
		return nil, err
	}
	if !descriptorBound {
		info, err := os.Lstat(absPath)
		if err != nil {
			return nil, fmt.Errorf("open diagnostic SQLite file: %w", err)
		}
		if !info.Mode().IsRegular() || info.Mode()&os.ModeSymlink != 0 {
			return nil, errors.New("open diagnostic SQLite file: existing regular file required")
		}
	}
	dsn := buildDSN(absPath, true, options)
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		return nil, fmt.Errorf("open diagnostic SQLite reader: %w", err)
	}
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	db.SetConnMaxIdleTime(0)
	db.SetConnMaxLifetime(0)
	return &DiagnosticInspection{store: &Store{path: absPath, reader: db}}, nil
}

func (inspection *DiagnosticInspection) Close() error {
	if inspection == nil || inspection.store == nil {
		return nil
	}
	return inspection.store.Close()
}

// QueryContext and QueryRowContext expose only the query-only reader.  They
// are intentionally the complete SQL surface available to diagnostics.
func (inspection *DiagnosticInspection) QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error) {
	if inspection == nil || inspection.store == nil || inspection.store.reader == nil {
		return nil, errors.New("sqlite: diagnostic reader is closed")
	}
	return inspection.store.reader.QueryContext(ctx, query, args...)
}

func (inspection *DiagnosticInspection) QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row {
	return inspection.store.reader.QueryRowContext(ctx, query, args...)
}

func (inspection *DiagnosticInspection) Canonical() *InspectionRepository {
	return &InspectionRepository{repository: inspection.store.Canonical()}
}

func (inspection *DiagnosticInspection) Projection() *ProjectionInspection {
	return &ProjectionInspection{repository: &ProjectionRepository{
		store: inspection.store, transactionTimeout: defaultProjectionTransactionTimeout,
		claimStateEvaluator: newSQLiteClaimStateEvaluator(inspection.store),
	}}
}

func (inspection *DiagnosticInspection) ServiceReadinessSource() readiness.Source {
	return &serviceReadinessSource{reader: inspection.store.reader}
}

func openInspection(ctx context.Context, path string, immutable bool) (*Inspection, error) {
	options, err := normalizeOptions(DefaultOptions())
	if err != nil {
		return nil, err
	}
	absPath, err := normalizeLocalPath(path)
	if err != nil {
		return nil, err
	}
	reader, err := openReaderMode(ctx, absPath, options, immutable)
	if err != nil {
		return nil, err
	}
	return validatedInspection(ctx, absPath, reader, nil)
}

func validatedInspection(ctx context.Context, path string, reader *sql.DB, cleanup func() error) (*Inspection, error) {
	report, err := ValidateSchema(ctx, reader)
	if err != nil {
		return nil, errors.Join(fmt.Errorf("schema gate: %w", err), reader.Close(), closeInspectionCleanup(cleanup))
	}
	return &Inspection{store: &Store{path: path, reader: reader, report: report}, cleanup: cleanup}, nil
}

func (inspection *Inspection) SchemaReport() SchemaReport {
	return inspection.store.SchemaReport()
}

func (inspection *Inspection) Close() error {
	if inspection == nil {
		return nil
	}
	inspection.closeOnce.Do(func() {
		var storeErr error
		if inspection.store != nil {
			storeErr = inspection.store.Close()
		}
		inspection.closeErr = errors.Join(storeErr, closeInspectionCleanup(inspection.cleanup))
	})
	return inspection.closeErr
}

func closeInspectionCleanup(cleanup func() error) error {
	if cleanup == nil {
		return nil
	}
	return cleanup()
}

// OpenWithOptions is Open with explicit pool mechanics for tests and hosts.
func OpenWithOptions(ctx context.Context, path string, options Options) (_ *Store, resultErr error) {
	options, err := normalizeOptions(options)
	if err != nil {
		return nil, err
	}
	absPath, err := normalizeLocalPath(path)
	if err != nil {
		return nil, err
	}

	writer, err := openWriter(ctx, absPath, options)
	if err != nil {
		return nil, err
	}
	defer func() {
		if resultErr != nil {
			_ = writer.Close()
		}
	}()

	if err := initializeEmptyDatabase(ctx, writer); err != nil {
		return nil, err
	}
	if err := rejectUnsupportedFutureVersion(ctx, writer); err != nil {
		return nil, err
	}
	if err := migrateUp(ctx, writer); err != nil {
		return nil, err
	}
	report, err := ValidateSchema(ctx, writer)
	if err != nil {
		return nil, fmt.Errorf("schema gate: %w", err)
	}

	reader, err := openReader(ctx, absPath, options)
	if err != nil {
		return nil, err
	}
	defer func() {
		if resultErr != nil {
			_ = reader.Close()
		}
	}()

	return &Store{path: absPath, writer: writer, reader: reader, report: report, writes: newWritePriorityGate()}, nil
}

// Reader returns the query-only pool. Canonical mutation must never use it.
func (s *Store) Reader() *sql.DB { return s.reader }

// Path returns the normalized absolute local database path.
func (s *Store) Path() string { return s.path }

// SchemaReport returns the startup schema-gate evidence.
func (s *Store) SchemaReport() SchemaReport { return s.report }

// Close seals a writable WAL database into one closed main-file snapshot,
// then closes both pools. The seal is required before an offline M7 reader can
// retain a handle which denies write/delete sharing on Windows. It also makes
// shutdown independent of driver-owned WAL handles whose final release would
// otherwise be deferred until garbage collection.
func (s *Store) Close() error {
	if s == nil {
		return nil
	}
	s.closeOnce.Do(func() {
		var readerErr, checkpointErr, journalErr, writerErr error
		if s.reader != nil && s.reader != s.writer {
			readerErr = s.reader.Close()
		}
		if s.writer != nil {
			if s.path != "" {
				checkpointCtx, cancelCheckpoint := context.WithTimeout(context.Background(), defaultBusyTimeout)
				var busy, logFrames, checkpointedFrames int
				if err := s.writer.QueryRowContext(checkpointCtx, "PRAGMA wal_checkpoint(TRUNCATE)").Scan(
					&busy, &logFrames, &checkpointedFrames,
				); err != nil {
					checkpointErr = fmt.Errorf("seal writable database WAL: %w", err)
				} else if busy != 0 || logFrames != checkpointedFrames {
					checkpointErr = fmt.Errorf(
						"seal writable database WAL: incomplete checkpoint (busy=%d log=%d checkpointed=%d)",
						busy, logFrames, checkpointedFrames,
					)
				}
				cancelCheckpoint()

				// Checkpointing and changing journal mode are distinct blocking
				// primitives. Give each the configured operation bound so a slow
				// but successful checkpoint cannot hand an already-expired context
				// to the journal transition.
				journalCtx, cancelJournal := context.WithTimeout(context.Background(), defaultBusyTimeout)
				var journalMode string
				if err := s.writer.QueryRowContext(journalCtx, "PRAGMA journal_mode=DELETE").Scan(&journalMode); err != nil {
					journalErr = fmt.Errorf("seal writable database journal mode: %w", err)
				} else if !strings.EqualFold(journalMode, "delete") {
					journalErr = fmt.Errorf("seal writable database journal mode = %q, want delete", journalMode)
				}
				cancelJournal()
			}
			writerErr = s.writer.Close()
		} else if s.reader != nil {
			readerErr = s.reader.Close()
		}
		s.closeErr = errors.Join(readerErr, checkpointErr, journalErr, writerErr)
	})
	return s.closeErr
}

func normalizeOptions(options Options) (Options, error) {
	if options.BusyTimeout == 0 {
		options.BusyTimeout = defaultBusyTimeout
	}
	if options.ReaderConnections == 0 {
		options.ReaderConnections = defaultReaderConnections
	}
	if options.BusyTimeout < 0 || options.BusyTimeout%time.Millisecond != 0 {
		return Options{}, fmt.Errorf("busy timeout must be a non-negative whole number of milliseconds")
	}
	if options.ReaderConnections < 1 {
		return Options{}, fmt.Errorf("reader connections must be positive")
	}
	return options, nil
}

func normalizeLocalPath(path string) (string, error) {
	if strings.TrimSpace(path) == "" {
		return "", fmt.Errorf("SQLite path is required")
	}
	if strings.Contains(path, "://") || strings.HasPrefix(strings.ToLower(path), "file:") {
		return "", fmt.Errorf("SQLite path must be a local filesystem path, not a URI")
	}
	if strings.HasPrefix(path, `\\`) {
		return "", fmt.Errorf("UNC/network SQLite paths are unsupported")
	}
	absPath, err := filepath.Abs(path)
	if err != nil {
		return "", fmt.Errorf("resolve SQLite path: %w", err)
	}
	parent := filepath.Dir(absPath)
	info, err := os.Stat(parent)
	if err != nil {
		return "", fmt.Errorf("stat SQLite parent directory %q: %w", parent, err)
	}
	if !info.IsDir() {
		return "", fmt.Errorf("SQLite parent %q is not a directory", parent)
	}
	return filepath.Clean(absPath), nil
}

func openWriter(ctx context.Context, path string, options Options) (*sql.DB, error) {
	dsn := buildDSN(path, false, options)
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		return nil, fmt.Errorf("open SQLite writer: %w", err)
	}
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	db.SetConnMaxIdleTime(0)
	db.SetConnMaxLifetime(0)
	if err := db.PingContext(ctx); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("ping SQLite writer: %w", err)
	}
	if err := verifyWriterPragmas(ctx, db, options); err != nil {
		_ = db.Close()
		return nil, err
	}
	return db, nil
}

func openReader(ctx context.Context, path string, options Options) (*sql.DB, error) {
	return openReaderMode(ctx, path, options, false)
}

func openReaderMode(ctx context.Context, path string, options Options, immutable bool) (*sql.DB, error) {
	dsn := buildDSN(path, true, options)
	if immutable {
		parsed, err := url.Parse(dsn)
		if err != nil {
			return nil, fmt.Errorf("parse immutable SQLite reader DSN: %w", err)
		}
		query := parsed.Query()
		query.Set("immutable", "1")
		parsed.RawQuery = query.Encode()
		dsn = parsed.String()
	}
	return openReaderDSN(ctx, dsn, options)
}

func openReaderDSN(ctx context.Context, dsn string, options Options) (*sql.DB, error) {
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		return nil, fmt.Errorf("open SQLite reader: %w", err)
	}
	db.SetMaxOpenConns(options.ReaderConnections)
	db.SetMaxIdleConns(options.ReaderConnections)
	if err := db.PingContext(ctx); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("ping SQLite reader: %w", err)
	}
	if err := verifyReaderPragmas(ctx, db, options, options.ReaderConnections); err != nil {
		_ = db.Close()
		return nil, err
	}
	return db, nil
}

func buildDSN(path string, readOnly bool, options Options) string {
	uriPath := filepath.ToSlash(path)
	if runtime.GOOS == "windows" && !strings.HasPrefix(uriPath, "/") {
		// A drive-qualified path must be represented as file:///C:/... .
		// Without the leading slash url.URL serializes C: as an authority,
		// which modernc SQLite correctly rejects.
		uriPath = "/" + uriPath
	}
	pathURL := &url.URL{Scheme: "file", Path: uriPath}
	query := pathURL.Query()
	if readOnly {
		query.Set("mode", "ro")
		query.Add("_pragma", "foreign_keys(1)")
		query.Add("_pragma", "trusted_schema(OFF)")
		query.Add("_pragma", "query_only(1)")
		query.Add("_pragma", "busy_timeout("+strconv.FormatInt(options.BusyTimeout.Milliseconds(), 10)+")")
	} else {
		query.Set("mode", "rwc")
		query.Add("_pragma", "foreign_keys(1)")
		query.Add("_pragma", "journal_mode(WAL)")
		query.Add("_pragma", "synchronous(FULL)")
		query.Add("_pragma", "busy_timeout("+strconv.FormatInt(options.BusyTimeout.Milliseconds(), 10)+")")
		query.Add("_pragma", "trusted_schema(OFF)")
		query.Add("_pragma", "secure_delete(ON)")
		query.Set("_txlock", "immediate")
		query.Set("_dqs", "0")
	}
	pathURL.RawQuery = query.Encode()
	return pathURL.String()
}
