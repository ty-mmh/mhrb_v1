package sqlite

import (
	"context"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"testing"

	"mahoroba.local/mahoroba/internal/fssecure"
)

func TestM7BoundDatabaseRejectsRootRelativePreOpenReparseSwap(t *testing.T) {
	for _, writable := range []bool{false, true} {
		name := "read"
		if writable {
			name = "write"
		}
		t.Run(name, func(t *testing.T) {
			dataDir, databasePath := newBoundDatabaseFixture(t)
			attackBlocked := false
			boundDatabaseTestHook = func(stage string) {
				if stage != "after_root_open" {
					return
				}
				if err := os.Rename(databasePath, databasePath+".moved"); err != nil {
					if runtime.GOOS == "windows" && !writable {
						attackBlocked = true
						return
					}
					t.Fatal(err)
				}
				installBoundDatabaseReparse(t, databasePath)
			}
			t.Cleanup(func() { boundDatabaseTestHook = nil })
			var boundary *BoundDatabase
			var err error
			if writable {
				boundary, err = OpenWritableBoundDatabase(dataDir, filepath.Base(databasePath))
			} else {
				boundary, err = OpenReadOnlyBoundDatabase(context.Background(), dataDir, filepath.Base(databasePath))
			}
			if attackBlocked {
				if err != nil || boundary == nil {
					t.Fatalf("blocked pre-open swap boundary=%v error=%v", boundary != nil, err)
				}
				if err := boundary.Verify(); err != nil {
					t.Fatalf("blocked pre-open swap invalidated boundary: %v", err)
				}
				if err := boundary.Close(); err != nil {
					t.Fatal(err)
				}
				return
			}
			if boundary != nil {
				_ = boundary.Close()
			}
			if !errors.Is(err, ErrUnsafeDatabaseBoundary) {
				t.Fatalf("pre-open reparse swap error=%v", err)
			}
		})
	}
}

func TestM7ReadOnlyBoundDatabaseUsesRetainedDescriptorAndDetectsSwap(t *testing.T) {
	dataDir, databasePath := newBoundDatabaseFixture(t)
	boundary, err := OpenReadOnlyBoundDatabase(context.Background(), dataDir, filepath.Base(databasePath))
	if err != nil {
		t.Fatal(err)
	}
	defer boundary.Close()
	if boundary.Inspection() == nil || boundary.Inspection().SchemaReport().SchemaFingerprint == "" {
		t.Fatal("descriptor-backed inspection is unavailable")
	}
	assertBoundDatabaseSwapBlockedOrDetected(t, boundary, databasePath)
}

func TestM7ReadOnlyBoundDatabaseBorrowsRetainedPublicationRoot(t *testing.T) {
	dataDir, databasePath := newBoundDatabaseFixture(t)
	policy, err := fssecure.CurrentSecurityPolicy()
	if err != nil {
		t.Fatal(err)
	}
	root, err := fssecure.OpenRootForPublish(dataDir, policy)
	if err != nil {
		skipBoundDatabaseDesktopSandbox(t, err)
		t.Fatal(err)
	}
	defer root.Close()

	boundary, err := OpenReadOnlyBoundDatabaseFromRoot(
		context.Background(), root, filepath.Base(databasePath),
	)
	if err != nil {
		t.Fatal(err)
	}
	if boundary.Inspection() == nil || boundary.Inspection().SchemaReport().SchemaFingerprint == "" {
		t.Fatal("borrowed descriptor-backed inspection is unavailable")
	}
	if err := boundary.Close(); err != nil {
		t.Fatal(err)
	}
	if err := root.VerifyBound(); err != nil {
		t.Fatalf("closing borrowed database boundary closed publication root: %v", err)
	}
}

func TestM7ReadOnlyBoundDatabaseRejectsHotWALSidecar(t *testing.T) {
	dataDir, databasePath := newBoundDatabaseFixture(t)
	policy, err := fssecure.CurrentSecurityPolicy()
	if err != nil {
		t.Fatal(err)
	}
	root, err := fssecure.OpenRoot(dataDir, policy)
	if err != nil {
		t.Fatal(err)
	}
	wal, err := root.OpenOrCreateRegular(filepath.Base(databasePath) + "-wal")
	if err != nil {
		_ = root.Close()
		t.Fatal(err)
	}
	if _, err := wal.File().Write([]byte("hot-wal-must-not-be-ignored")); err != nil {
		t.Fatal(err)
	}
	if err := wal.Seal(); err != nil {
		t.Fatal(err)
	}
	if err := errors.Join(wal.Close(), root.Close()); err != nil {
		t.Fatal(err)
	}
	boundary, err := OpenReadOnlyBoundDatabase(context.Background(), dataDir, filepath.Base(databasePath))
	if boundary != nil {
		_ = boundary.Close()
	}
	if !errors.Is(err, ErrUnsafeDatabaseBoundary) {
		t.Fatalf("hot WAL error=%v", err)
	}
}

func TestM7ReadOnlyBoundDatabaseRejectsHotWALIntroducedAfterOpen(t *testing.T) {
	dataDir, databasePath := newBoundDatabaseFixture(t)
	boundary, err := OpenReadOnlyBoundDatabase(context.Background(), dataDir, filepath.Base(databasePath))
	if err != nil {
		t.Fatal(err)
	}
	defer boundary.Close()
	policy, err := fssecure.CurrentSecurityPolicy()
	if err != nil {
		t.Fatal(err)
	}
	root, err := fssecure.OpenRoot(dataDir, policy)
	if err != nil {
		if runtime.GOOS == "windows" {
			if verifyErr := boundary.Verify(); verifyErr != nil {
				t.Fatalf("blocked late WAL creation invalidated boundary: %v", verifyErr)
			}
			return
		}
		skipBoundDatabaseDesktopSandbox(t, err)
		t.Fatal(err)
	}
	wal, err := root.OpenOrCreateRegular(filepath.Base(databasePath) + "-wal")
	if err != nil {
		_ = root.Close()
		skipBoundDatabaseDesktopSandbox(t, err)
		t.Fatal(err)
	}
	if _, err := wal.File().Write([]byte("late-hot-wal")); err != nil {
		t.Fatal(err)
	}
	if err := wal.Seal(); err != nil {
		t.Fatal(err)
	}
	if err := errors.Join(wal.Close(), root.Close()); err != nil {
		t.Fatal(err)
	}
	if err := boundary.Verify(); !errors.Is(err, ErrUnsafeDatabaseBoundary) {
		t.Fatalf("late hot WAL error=%v", err)
	}
}

func TestM7WritableBoundDatabaseRejectsReparseAndBlocksOrDetectsSwap(t *testing.T) {
	t.Run("initial reparse", func(t *testing.T) {
		dataDir, databasePath := newBoundDatabaseFixture(t)
		if err := os.Rename(databasePath, databasePath+".moved"); err != nil {
			t.Fatal(err)
		}
		installBoundDatabaseReparse(t, databasePath)
		boundary, err := OpenWritableBoundDatabase(dataDir, filepath.Base(databasePath))
		if boundary != nil {
			_ = boundary.Close()
		}
		if !errors.Is(err, ErrUnsafeDatabaseBoundary) {
			t.Fatalf("initial reparse error=%v", err)
		}
	})
	t.Run("post open swap", func(t *testing.T) {
		dataDir, databasePath := newBoundDatabaseFixture(t)
		boundary, err := OpenWritableBoundDatabase(dataDir, filepath.Base(databasePath))
		if err != nil {
			t.Fatal(err)
		}
		defer boundary.Close()
		assertBoundDatabaseSwapBlockedOrDetected(t, boundary, databasePath)
	})
}

func assertBoundDatabaseSwapBlockedOrDetected(
	t *testing.T,
	boundary *BoundDatabase,
	databasePath string,
) {
	t.Helper()
	moved := databasePath + ".moved"
	renameErr := os.Rename(databasePath, moved)
	if renameErr != nil {
		if runtime.GOOS != "windows" {
			t.Fatalf("rename bound database: %v", renameErr)
		}
		if err := boundary.Verify(); err != nil {
			t.Fatalf("blocked Windows rename invalidated boundary: %v", err)
		}
		return
	}
	if err := os.WriteFile(databasePath, []byte("replacement"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := boundary.Verify(); !errors.Is(err, ErrUnsafeDatabaseBoundary) {
		t.Fatalf("post-open swap error=%v", err)
	}
}

func newBoundDatabaseFixture(t *testing.T) (string, string) {
	t.Helper()
	policy, err := fssecure.CurrentSecurityPolicy()
	if err != nil {
		t.Fatal(err)
	}
	// Keep the data root below a separately protected fixture root. Host-lock
	// tests place their rendezvous beside data, so its parent must provide the
	// same managed namespace guarantees as the data root itself on Windows.
	fixtureDir := filepath.Join(t.TempDir(), "fixture")
	fixtureRoot, err := fssecure.OpenOrCreateRoot(fixtureDir, policy)
	if err != nil {
		skipBoundDatabaseDesktopSandbox(t, err)
		t.Fatal(err)
	}
	if err := fixtureRoot.Close(); err != nil {
		t.Fatal(err)
	}
	dataDir := filepath.Join(fixtureDir, "data")
	root, err := fssecure.OpenOrCreateRoot(dataDir, policy)
	if err != nil {
		skipBoundDatabaseDesktopSandbox(t, err)
		t.Fatal(err)
	}
	database, err := root.CreateRegular("mahoroba.db")
	if err != nil {
		_ = root.Close()
		skipBoundDatabaseDesktopSandbox(t, err)
		t.Fatal(err)
	}
	if err := errors.Join(database.Close(), root.Close()); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(dataDir, "mahoroba.db")
	store, err := Open(context.Background(), path)
	if err != nil {
		skipBoundDatabaseDesktopSandbox(t, err)
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	return dataDir, path
}

func installBoundDatabaseReparse(t *testing.T, path string) {
	t.Helper()
	target := path + ".reparse-target"
	if err := os.Mkdir(target, 0o700); err != nil {
		t.Fatal(err)
	}
	if runtime.GOOS == "windows" {
		output, err := exec.Command("cmd.exe", "/d", "/c", "mklink", "/J", path, target).CombinedOutput()
		if err != nil {
			t.Fatalf("create database junction: %v: %s", err, output)
		}
		return
	}
	if err := os.Symlink(target, path); err != nil {
		t.Fatal(err)
	}
}

func skipBoundDatabaseDesktopSandbox(t *testing.T, err error) {
	t.Helper()
	if runtime.GOOS == "windows" && os.Getenv("CI") == "" && errors.Is(err, os.ErrPermission) {
		t.Skipf("desktop sandbox cannot exercise exact protected database boundary: %v", err)
	}
}
