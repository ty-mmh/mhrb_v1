package sqlite

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"

	"mahoroba.local/mahoroba/internal/descriptorpath"
	"mahoroba.local/mahoroba/internal/fssecure"
)

// ErrUnsafeDatabaseBoundary identifies a database namespace, identity, kind,
// or exact owner/mode/ACL failure. Callers wrap it in their public operation's
// closed source error.
var ErrUnsafeDatabaseBoundary = errors.New("sqlite: unsafe database boundary")

// BoundDatabase retains the exact protected root and root-relative Canonical
// database identity. A read boundary also owns the descriptor-backed
// Inspection. A writable boundary permits cooperating SQLite content writes
// while retaining identity and parent-relative namespace authority.
type BoundDatabase struct {
	dataDir          string
	databaseFilename string
	root             *fssecure.Directory
	database         *fssecure.Handle
	inspection       *Inspection
	writable         bool
	ownsRoot         bool
	once             sync.Once
	closeErr         error
}

// boundDatabaseTestHook is nil in production. Package tests use the named
// points to force a namespace swap between the retained root and database
// opens without widening the public API.
var boundDatabaseTestHook func(string)

// OpenReadOnlyBoundDatabase opens an exact root-relative no-follow database
// handle, exposes that retained descriptor to SQLite, and keeps both handles
// alive for the complete query-only inspection lifetime. This is an offline
// boundary: on Windows any pre-existing writable database handle must make the
// open fail with a sharing violation. Callers must close their writable Store
// after acquiring the host lock instead of weakening the retained handle's
// write/delete-sharing denial.
func OpenReadOnlyBoundDatabase(
	ctx context.Context,
	dataDir, databaseFilename string,
) (*BoundDatabase, error) {
	if ctx == nil {
		return nil, fmt.Errorf("%w: nil read context", ErrUnsafeDatabaseBoundary)
	}
	return openBoundDatabase(ctx, dataDir, databaseFilename, false)
}

// OpenReadOnlyBoundDatabaseFromRoot borrows an already-retained protected
// root and opens the Canonical database relative to that handle. The caller
// must keep root open until the returned boundary closes. DurablePublish uses
// this form so Windows never needs to drop publication authority merely to
// acquire a second root handle with a conflicting share contract.
func OpenReadOnlyBoundDatabaseFromRoot(
	ctx context.Context,
	root *fssecure.Directory,
	databaseFilename string,
) (*BoundDatabase, error) {
	if ctx == nil {
		return nil, fmt.Errorf("%w: nil read context", ErrUnsafeDatabaseBoundary)
	}
	if root == nil {
		return nil, fmt.Errorf("%w: nil retained root", ErrUnsafeDatabaseBoundary)
	}
	dataDir := root.Path()
	if err := validateBoundDatabaseSource(dataDir, databaseFilename); err != nil {
		return nil, err
	}
	return finishOpenBoundDatabase(ctx, dataDir, databaseFilename, false, root, false)
}

// OpenWritableBoundDatabase retains a root-relative identity guard while a
// caller opens writable SQLite through DataDir/databaseFilename. On Windows,
// the live SQLite handles and this guard form one session: SQLite denies
// delete sharing while the guard proves the exact namespace binding. After
// SQLite closes, the guard still detects a replacement but does not by itself
// claim to prevent every rename API. Linux likewise relies on Verify at the
// caller-owned transaction boundaries.
func OpenWritableBoundDatabase(dataDir, databaseFilename string) (*BoundDatabase, error) {
	return openBoundDatabase(nil, dataDir, databaseFilename, true)
}

func openBoundDatabase(
	ctx context.Context,
	dataDir, databaseFilename string,
	writable bool,
) (*BoundDatabase, error) {
	if err := validateBoundDatabaseSource(dataDir, databaseFilename); err != nil {
		return nil, err
	}
	policy, err := fssecure.CurrentSecurityPolicy()
	if err != nil {
		return nil, errors.Join(ErrUnsafeDatabaseBoundary, err)
	}
	var root *fssecure.Directory
	if writable {
		root, err = fssecure.OpenRoot(dataDir, policy)
	} else {
		root, err = fssecure.OpenRootReadOnly(dataDir, policy)
	}
	if err != nil {
		return nil, errors.Join(ErrUnsafeDatabaseBoundary, err)
	}
	return finishOpenBoundDatabase(ctx, dataDir, databaseFilename, writable, root, true)
}

func validateBoundDatabaseSource(dataDir, databaseFilename string) error {
	if dataDir == "" || !filepath.IsAbs(dataDir) || filepath.Clean(dataDir) != dataDir ||
		databaseFilename == "" || filepath.Base(databaseFilename) != databaseFilename ||
		strings.ContainsAny(databaseFilename, `/\`) {
		return fmt.Errorf("%w: invalid database source", ErrUnsafeDatabaseBoundary)
	}
	return nil
}

func finishOpenBoundDatabase(
	ctx context.Context,
	dataDir, databaseFilename string,
	writable bool,
	root *fssecure.Directory,
	ownsRoot bool,
) (*BoundDatabase, error) {
	var err error
	boundary := &BoundDatabase{
		dataDir: dataDir, databaseFilename: databaseFilename, root: root,
		writable: writable, ownsRoot: ownsRoot,
	}
	fail := func(cause error) (*BoundDatabase, error) {
		return nil, errors.Join(ErrUnsafeDatabaseBoundary, cause, boundary.Close())
	}
	if boundDatabaseTestHook != nil {
		boundDatabaseTestHook("after_root_open")
	}
	if err := root.VerifyBound(); err != nil {
		return fail(err)
	}
	if !writable {
		if err := requireClosedSQLiteNamespace(root, databaseFilename); err != nil {
			return fail(err)
		}
	}
	if writable {
		boundary.database, err = root.OpenRegularIdentityGuard(databaseFilename)
	} else {
		boundary.database, err = root.OpenRegularRead(databaseFilename)
	}
	if err != nil {
		return fail(err)
	}
	if !writable {
		databasePath, pathErr := descriptorpath.ReadOnly(
			boundary.database.File(), filepath.Join(dataDir, databaseFilename),
		)
		if pathErr != nil {
			return fail(pathErr)
		}
		boundary.inspection, err = OpenBoundInspection(ctx, databasePath)
		if err != nil {
			return fail(err)
		}
	}
	if boundDatabaseTestHook != nil {
		boundDatabaseTestHook("after_database_open")
	}
	if !writable {
		if err := requireClosedSQLiteNamespace(root, databaseFilename); err != nil {
			return fail(err)
		}
	}
	if err := boundary.Verify(); err != nil {
		return fail(err)
	}
	return boundary, nil
}

// requireClosedSQLiteNamespace prevents descriptor-backed readers from
// silently ignoring a hot WAL/rollback journal or shared-memory sidecar. It is
// valid only after the caller owns the offline host lock. Absent or empty
// sidecars prove that the retained main database contains the closed snapshot.
func requireClosedSQLiteNamespace(root *fssecure.Directory, databaseFilename string) error {
	for _, suffix := range []string{"-wal", "-journal", "-shm"} {
		handle, err := root.OpenRegularRead(databaseFilename + suffix)
		if errors.Is(err, os.ErrNotExist) {
			continue
		}
		if err != nil {
			return errors.Join(ErrUnsafeDatabaseBoundary, err)
		}
		info := handle.Snapshot()
		verifyErr := handle.VerifyBound()
		closeErr := handle.Close()
		if info == nil || info.Size() != 0 || verifyErr != nil || closeErr != nil {
			return errors.Join(ErrUnsafeDatabaseBoundary,
				fmt.Errorf("active or unverifiable SQLite %s sidecar", suffix), verifyErr, closeErr)
		}
	}
	return nil
}

// DataDir is the exact clean absolute root retained by the boundary.
func (boundary *BoundDatabase) DataDir() string {
	if boundary == nil {
		return ""
	}
	return boundary.dataDir
}

// Inspection returns the descriptor-backed read surface. Writable boundaries
// deliberately return nil.
func (boundary *BoundDatabase) Inspection() *Inspection {
	if boundary == nil || boundary.writable {
		return nil
	}
	return boundary.inspection
}

// SourceIdentityDigest binds producer metadata to the retained data root.
func (boundary *BoundDatabase) SourceIdentityDigest() (string, error) {
	if boundary == nil || boundary.root == nil {
		return "", ErrUnsafeDatabaseBoundary
	}
	return boundary.root.ArtifactSourceIdentityDigest()
}

// Verify proves root and database namespace identities. Read boundaries also
// freeze the file snapshot; writable boundaries intentionally ignore content
// size/mtime while proving identity and exact security.
func (boundary *BoundDatabase) Verify() error {
	if boundary == nil || boundary.root == nil || boundary.database == nil {
		return ErrUnsafeDatabaseBoundary
	}
	if err := boundary.root.VerifyBound(); err != nil {
		return errors.Join(ErrUnsafeDatabaseBoundary, err)
	}
	var err error
	if boundary.writable {
		err = boundary.database.VerifyBinding()
	} else {
		err = boundary.database.VerifyBound()
	}
	if err != nil {
		return errors.Join(ErrUnsafeDatabaseBoundary, err)
	}
	if !boundary.writable {
		// Descriptor-backed readers intentionally cannot resolve SQLite WAL
		// sidecars. Re-check the retained root-relative namespace so a sidecar
		// introduced after open cannot be silently omitted from the snapshot.
		if err := requireClosedSQLiteNamespace(boundary.root, boundary.databaseFilename); err != nil {
			return err
		}
	}
	return nil
}

// Close releases Inspection, database, then root handles. It is idempotent.
func (boundary *BoundDatabase) Close() error {
	if boundary == nil {
		return nil
	}
	boundary.once.Do(func() {
		var inspectionErr, databaseErr, rootErr error
		if boundary.inspection != nil {
			inspectionErr = boundary.inspection.Close()
			boundary.inspection = nil
		}
		if boundary.database != nil {
			databaseErr = boundary.database.Close()
			boundary.database = nil
		}
		if boundary.root != nil {
			if boundary.ownsRoot {
				rootErr = boundary.root.Close()
			}
			boundary.root = nil
		}
		boundary.closeErr = errors.Join(inspectionErr, databaseErr, rootErr)
	})
	return boundary.closeErr
}
