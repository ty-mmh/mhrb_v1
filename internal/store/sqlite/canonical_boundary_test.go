package sqlite

import (
	"context"
	"crypto/sha256"
	"errors"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"testing"

	"mahoroba.local/mahoroba/internal/canonical"
	"mahoroba.local/mahoroba/internal/hostlock"
)

var errCanonicalBoundaryChanged = errors.New("test Canonical database boundary changed")

type canonicalBoundaryProbe struct {
	calls  int
	failAt int
	hook   func(int) error
}

func (probe *canonicalBoundaryProbe) Verify() error {
	probe.calls++
	if probe.hook != nil {
		if err := probe.hook(probe.calls); err != nil {
			return err
		}
	}
	if probe.calls == probe.failAt {
		return errCanonicalBoundaryChanged
	}
	return nil
}

func TestM7CanonicalPostCommitBoundaryVerificationRetainsWriteGate(t *testing.T) {
	database, err := Open(context.Background(), filepath.Join(t.TempDir(), "mahoroba.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	postCommitObserved := false
	probe := &canonicalBoundaryProbe{hook: func(call int) error {
		if call != 4 {
			return nil
		}
		database.writes.mu.Lock()
		defer database.writes.mu.Unlock()
		postCommitObserved = true
		if !database.writes.active {
			return errors.New("Canonical write gate released before post-commit boundary verification")
		}
		return nil
	}}
	database.canonicalBoundary = probe
	commitID, err := canonical.NewSecureIDGenerator().New()
	if err != nil {
		t.Fatal(err)
	}
	seq, err := canonical.NewCommitSeq(1)
	if err != nil {
		t.Fatal(err)
	}
	uow, err := database.Canonical().Begin(context.Background(), canonical.CommitMetadata{
		CommitID: commitID, CommitSeq: seq, Scope: canonical.GlobalScope(),
		CommittedAt: 1, CommittedTZ: canonical.MustTimezone("UTC"),
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := uow.Commit(context.Background()); err != nil {
		t.Fatal(err)
	}
	if !postCommitObserved || probe.calls != 4 {
		t.Fatalf("post-commit boundary observation=%v calls=%d", postCommitObserved, probe.calls)
	}
	database.writes.mu.Lock()
	active := database.writes.active
	database.writes.mu.Unlock()
	if active {
		t.Fatal("Canonical write gate remained active after post-commit verification")
	}
}

func TestM7CanonicalUoWVerifiesWritableDatabaseBoundaryAtOpenPrecommitAndPostcommit(t *testing.T) {
	for _, testCase := range []struct {
		name          string
		failAt        int
		wantCommitted int
	}{
		{name: "after_transaction_open", failAt: 2, wantCommitted: 0},
		{name: "pre_commit", failAt: 3, wantCommitted: 0},
		{name: "post_commit", failAt: 4, wantCommitted: 1},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			database, err := Open(context.Background(), filepath.Join(t.TempDir(), "mahoroba.db"))
			if err != nil {
				t.Fatal(err)
			}
			defer database.Close()
			probe := &canonicalBoundaryProbe{failAt: testCase.failAt}
			database.canonicalBoundary = probe

			commitID, err := canonical.NewSecureIDGenerator().New()
			if err != nil {
				t.Fatal(err)
			}
			seq, err := canonical.NewCommitSeq(1)
			if err != nil {
				t.Fatal(err)
			}
			metadata := canonical.CommitMetadata{
				CommitID: commitID, CommitSeq: seq, Scope: canonical.GlobalScope(),
				CommittedAt: 1, CommittedTZ: canonical.MustTimezone("UTC"),
			}
			uow, beginErr := database.Canonical().Begin(context.Background(), metadata)
			operationErr := beginErr
			if beginErr == nil {
				operationErr = uow.Commit(context.Background())
			}
			if !errors.Is(operationErr, errCanonicalBoundaryChanged) {
				t.Fatalf("boundary failure = %v", operationErr)
			}
			if probe.calls != testCase.failAt {
				t.Fatalf("boundary calls = %d, want %d", probe.calls, testCase.failAt)
			}
			var committed int
			if err := database.reader.QueryRow(`SELECT COUNT(*) FROM canonical_commits`).Scan(&committed); err != nil {
				t.Fatal(err)
			}
			if committed != testCase.wantCommitted {
				t.Fatalf("committed rows = %d, want %d", committed, testCase.wantCommitted)
			}
		})
	}
}

func TestM7CanonicalUoWRejectsDataRootNamespaceReplacementAfterHostLock(t *testing.T) {
	dataDir, databasePath := newBoundDatabaseFixture(t)
	firstLock, err := hostlock.Acquire(dataDir)
	if err != nil {
		t.Fatal(err)
	}
	defer firstLock.Close()
	boundary, err := OpenWritableBoundDatabase(dataDir, filepath.Base(databasePath))
	if err != nil {
		t.Fatal(err)
	}
	defer boundary.Close()
	database, err := Open(context.Background(), databasePath)
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	if err := database.BindCanonicalBoundary(boundary); err != nil {
		t.Fatal(err)
	}
	var beforeDigest [sha256.Size]byte
	var beforeSize int64
	if runtime.GOOS != "windows" {
		// The Windows identity guard intentionally has no content-read access;
		// denying the rename is already the stronger kernel-enforced result.
		beforeDigest, beforeSize = hashRetainedDatabaseBytes(t, boundary.database.File())
	}

	detached := dataDir + ".detached"
	if err := os.Rename(dataDir, detached); err != nil {
		if runtime.GOOS != "windows" {
			t.Fatalf("rename bound data root: %v", err)
		}
		// Denying the rename while live handles are retained is stronger than
		// detecting the replacement at the next UoW boundary.
		if err := boundary.Verify(); err != nil {
			t.Fatalf("blocked Windows rename invalidated boundary: %v", err)
		}
		return
	}
	if runtime.GOOS == "windows" {
		t.Fatal("Windows data-root rename succeeded while delete sharing was denied")
	}
	secondLock, err := hostlock.Acquire(dataDir)
	if err != nil {
		t.Fatalf("acquire replacement-root host lock: %v", err)
	}
	defer secondLock.Close()

	commitID, err := canonical.NewSecureIDGenerator().New()
	if err != nil {
		t.Fatal(err)
	}
	seq, err := canonical.NewCommitSeq(1)
	if err != nil {
		t.Fatal(err)
	}
	_, err = database.Canonical().Begin(context.Background(), canonical.CommitMetadata{
		CommitID: commitID, CommitSeq: seq, Scope: canonical.GlobalScope(),
		CommittedAt: 1, CommittedTZ: canonical.MustTimezone("UTC"),
	})
	if !errors.Is(err, ErrUnsafeDatabaseBoundary) {
		t.Fatalf("Canonical Begin after data-root replacement = %v, want ErrUnsafeDatabaseBoundary", err)
	}
	// Once the namespace boundary has failed closed, the SQLite VFS may no
	// longer resolve sidecars through its original path. Prove no mutation
	// through the retained database authority instead of reusing that unsafe
	// namespace reader.
	afterDigest, afterSize := hashRetainedDatabaseBytes(t, boundary.database.File())
	if beforeDigest != afterDigest || beforeSize != afterSize {
		t.Fatal("rejected data-root replacement changed retained database bytes")
	}
}

func hashRetainedDatabaseBytes(t *testing.T, file *os.File) ([sha256.Size]byte, int64) {
	t.Helper()
	if file == nil {
		t.Fatal("retained database handle is unavailable")
	}
	info, err := file.Stat()
	if err != nil {
		t.Fatalf("stat retained database: %v", err)
	}
	hasher := sha256.New()
	count, err := io.Copy(hasher, io.NewSectionReader(file, 0, info.Size()))
	if err != nil {
		t.Fatalf("hash retained database: %v", err)
	}
	var result [sha256.Size]byte
	copy(result[:], hasher.Sum(nil))
	return result, count
}

func TestM7CanonicalWriterHeadReadIsBoundBeforeAndAfter(t *testing.T) {
	for _, failAt := range []int{1, 2} {
		t.Run(string(rune('0'+failAt)), func(t *testing.T) {
			database, err := Open(context.Background(), filepath.Join(t.TempDir(), "mahoroba.db"))
			if err != nil {
				t.Fatal(err)
			}
			defer database.Close()
			probe := &canonicalBoundaryProbe{failAt: failAt}
			database.canonicalBoundary = probe
			writer, err := canonical.OpenWriter(context.Background(), canonical.WriterOptions{
				Backend: database.Canonical(), IDs: canonical.NewSecureIDGenerator(),
				Clock: canonical.SystemClock{}, Timezone: canonical.MustTimezone("UTC"), QueueCapacity: 1,
			})
			if writer != nil {
				_ = writer.Close(context.Background())
			}
			if !errors.Is(err, errCanonicalBoundaryChanged) {
				t.Fatalf("OpenWriter boundary failure = %v", err)
			}
			if probe.calls != failAt {
				t.Fatalf("boundary calls = %d, want %d", probe.calls, failAt)
			}
		})
	}
}

type canonicalBoundaryNoopCommand struct{}

func (canonicalBoundaryNoopCommand) Name() string           { return "CanonicalBoundaryNoop" }
func (canonicalBoundaryNoopCommand) Scope() canonical.Scope { return canonical.GlobalScope() }
func (canonicalBoundaryNoopCommand) Validate() error        { return nil }
func (canonicalBoundaryNoopCommand) Execute(context.Context, canonical.CanonicalUoW) (any, error) {
	return nil, nil
}

func TestM7CanonicalWriterPoisonsOnPostCommitBoundaryFailure(t *testing.T) {
	database, err := Open(context.Background(), filepath.Join(t.TempDir(), "mahoroba.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	// LoadHead uses calls 1/2; Begin uses 3/4; Commit uses 5/6.
	probe := &canonicalBoundaryProbe{failAt: 6}
	database.canonicalBoundary = probe
	writer, err := canonical.OpenWriter(context.Background(), canonical.WriterOptions{
		Backend: database.Canonical(), IDs: canonical.NewSecureIDGenerator(),
		Clock: canonical.SystemClock{}, Timezone: canonical.MustTimezone("UTC"), QueueCapacity: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer writer.Close(context.Background())
	_, err = writer.Submit(context.Background(), canonicalBoundaryNoopCommand{})
	if !errors.Is(err, canonical.ErrWriterPoisoned) || !errors.Is(err, errCanonicalBoundaryChanged) {
		t.Fatalf("post-commit boundary failure = %v, want poisoned ambiguous outcome", err)
	}
	var committed int
	if err := database.reader.QueryRow(`SELECT COUNT(*) FROM canonical_commits`).Scan(&committed); err != nil {
		t.Fatal(err)
	}
	if committed != 1 {
		t.Fatalf("post-commit boundary failure persisted commits = %d, want 1", committed)
	}
	calls := probe.calls
	if _, err := writer.Submit(context.Background(), canonicalBoundaryNoopCommand{}); !errors.Is(err, canonical.ErrWriterPoisoned) {
		t.Fatalf("second submission after boundary poison = %v", err)
	}
	if probe.calls != calls {
		t.Fatalf("poisoned writer touched database boundary again: %d -> %d", calls, probe.calls)
	}
}
