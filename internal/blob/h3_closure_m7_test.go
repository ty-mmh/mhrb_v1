//go:build linux || windows

package blob

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"testing"

	"mahoroba.local/mahoroba/internal/canonical"
	"mahoroba.local/mahoroba/internal/fssecure"
)

func TestM7FileStoreRejectsDirectoryIdentitySwap(t *testing.T) {
	for _, component := range []string{"staging", "objects", "quarantine"} {
		t.Run(component, func(t *testing.T) {
			store := newTestStore(t)
			root, err := fssecure.OpenRoot(store.root, store.policy)
			if err != nil {
				t.Fatal(err)
			}
			replacementName := "replacement-" + component
			replacement, err := root.OpenOrCreateDirectory(replacementName)
			if err != nil {
				_ = root.Close()
				t.Fatal(err)
			}
			_ = replacement.Close()
			_ = root.Close()
			original := filepath.Join(store.root, component)
			displaced := filepath.Join(store.root, "displaced-"+component)
			replacementPath := filepath.Join(store.root, replacementName)
			if err := os.Rename(original, displaced); err != nil {
				t.Fatal(err)
			}
			if err := os.Rename(replacementPath, original); err != nil {
				t.Fatal(err)
			}
			_, err = store.Exists(testResident(t, "01ARZ3NDEKTSV4RRFFQ69G5FAV"), canonical.HashBlob([]byte("absent")))
			if !errors.Is(err, ErrUnsafeFilesystem) {
				t.Fatalf("identity swap error = %v", err)
			}
			if reason, ok := fssecure.Reason(err); !ok || reason != fssecure.ReasonDirectoryIdentityChanged {
				t.Fatalf("identity swap reason = %q/%v, error=%v", reason, ok, err)
			}
			entries, readErr := os.ReadDir(original)
			if readErr != nil || len(entries) != 0 {
				t.Fatalf("replacement directory changed: entries=%v err=%v", entries, readErr)
			}
		})
	}
}

func TestM7FileStoreRejectsSymlinkJunctionAndReparseSwap(t *testing.T) {
	store := newTestStore(t)
	target := filepath.Join(t.TempDir(), "target")
	if err := os.Mkdir(target, 0o700); err != nil {
		t.Fatal(err)
	}
	marker := filepath.Join(target, "must-not-change")
	if err := os.WriteFile(marker, []byte("target"), 0o600); err != nil {
		t.Fatal(err)
	}
	displaced := filepath.Join(store.root, "staging-displaced")
	if err := os.Rename(store.stagingDir, displaced); err != nil {
		t.Fatal(err)
	}
	createDirectoryReparse(t, target, store.stagingDir)
	_, err := store.StageBytes(context.Background(), testResident(t, "01ARZ3NDEKTSV4RRFFQ69G5FAV"), []byte("must not land"))
	if !errors.Is(err, ErrUnsafeFilesystem) {
		t.Fatalf("reparse swap error = %v", err)
	}
	content, err := os.ReadFile(marker)
	if err != nil || !bytes.Equal(content, []byte("target")) {
		t.Fatalf("reparse target changed: %q, %v", content, err)
	}
}

func TestM7FileStoreQuarantinesPostPublicationMismatch(t *testing.T) {
	store := newTestStore(t)
	residentID := testResident(t, "01ARZ3NDEKTSV4RRFFQ69G5FAV")
	staged, err := store.StageBytes(context.Background(), residentID, []byte("trusted"))
	if err != nil {
		t.Fatal(err)
	}
	store.testEvidenceName = func(prefix string) string { return prefix + "fixed" }
	store.testMutatePublished = func(handle *fssecure.Handle) error {
		return replaceHandleContent(handle, []byte("replacement"))
	}
	if _, err := store.Finalize(context.Background(), residentID, staged); !errors.Is(err, ErrDigestMismatch) {
		t.Fatalf("post-publication mismatch error = %v", err)
	}
	objectPath, _ := store.objectPath(residentID, staged.Digest(), false)
	if _, err := os.Lstat(objectPath); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("canonical object survived mismatch: %v", err)
	}
	entries, err := os.ReadDir(store.quarantineDir)
	if err != nil || len(entries) != 1 || !strings.HasPrefix(entries[0].Name(), "evidence-object-") {
		t.Fatalf("persistent evidence = %v, %v", entries, err)
	}
	evidence, err := os.ReadFile(filepath.Join(store.quarantineDir, entries[0].Name()))
	if err != nil || !bytes.Equal(evidence, []byte("replacement")) {
		t.Fatalf("evidence bytes = %q, %v", evidence, err)
	}
	if _, err := os.Stat(filepath.Join(store.stagingDir, staged.TemporaryName())); err != nil {
		t.Fatalf("staging evidence was lost: %v", err)
	}
}

func TestM7FileStoreKeepsEvidenceOnQuarantineMismatch(t *testing.T) {
	t.Run("binding mismatch", func(t *testing.T) {
		store := newTestStore(t)
		residentID := testResident(t, "01ARZ3NDEKTSV4RRFFQ69G5FAV")
		staged, err := store.StageBytes(context.Background(), residentID, []byte("trusted"))
		if err != nil {
			t.Fatal(err)
		}
		var evidencePath, displacedPath string
		var hostileRenameErr error
		store.testEvidenceName = func(prefix string) string {
			evidencePath = filepath.Join(store.quarantineDir, prefix+"fixed")
			displacedPath = evidencePath + "-displaced"
			return prefix + "fixed"
		}
		store.testMutatePublished = func(handle *fssecure.Handle) error {
			return replaceHandleContent(handle, []byte("replacement"))
		}
		store.testHook = func(label string) {
			switch label {
			case "quarantine_after_move":
				hostileRenameErr = os.Rename(evidencePath, displacedPath)
				if hostileRenameErr == nil {
					if err := os.WriteFile(evidencePath, []byte("hostile evidence replacement"), 0o600); err != nil {
						t.Fatal(err)
					}
				}
			}
		}
		if _, err := store.Finalize(context.Background(), residentID, staged); !errors.Is(err, ErrDigestMismatch) {
			t.Fatalf("quarantine mismatch error = %v", err)
		}
		if runtime.GOOS == "windows" && hostileRenameErr == nil {
			t.Fatal("Windows secure handle allowed evidence rename")
		}
		if runtime.GOOS != "windows" {
			if _, err := os.Stat(displacedPath); err != nil {
				t.Fatalf("original evidence lost: %v", err)
			}
			if _, err := os.Stat(evidencePath); err != nil {
				t.Fatalf("replacement evidence lost: %v", err)
			}
		} else if _, err := os.Stat(evidencePath); err != nil {
			t.Fatalf("Windows evidence lost: %v", err)
		}
	})

	t.Run("collision does not overwrite", func(t *testing.T) {
		store := newTestStore(t)
		residentID := testResident(t, "01ARZ3NDEKTSV4RRFFQ69G5FAV")
		staged, err := store.StageBytes(context.Background(), residentID, []byte("trusted"))
		if err != nil {
			t.Fatal(err)
		}
		prefix := "evidence-object-" + residentID.String() + "-" + staged.Digest().Hex() + "-"
		store.testEvidenceName = func(string) string { return prefix + "collision" }
		layout, err := store.openLayout()
		if err != nil {
			t.Fatal(err)
		}
		collision, err := layout.quarantine.CreateRegular(prefix + "collision")
		if err != nil {
			t.Fatal(err)
		}
		if _, err := collision.File().Write([]byte("existing evidence")); err != nil {
			t.Fatal(err)
		}
		if err := collision.Seal(); err != nil {
			t.Fatal(err)
		}
		_ = collision.Close()
		_ = layout.Close()
		store.testMutatePublished = func(handle *fssecure.Handle) error {
			return replaceHandleContent(handle, []byte("replacement"))
		}
		if _, err := store.Finalize(context.Background(), residentID, staged); !errors.Is(err, ErrDigestMismatch) {
			t.Fatalf("collision error = %v", err)
		}
		got, err := os.ReadFile(filepath.Join(store.quarantineDir, prefix+"collision"))
		if err != nil || !bytes.Equal(got, []byte("existing evidence")) {
			t.Fatalf("collision evidence overwritten: %q, %v", got, err)
		}
	})
}

func replaceHandleContent(handle *fssecure.Handle, content []byte) error {
	if err := handle.File().Truncate(0); err != nil {
		return err
	}
	if _, err := handle.File().Seek(0, 0); err != nil {
		return err
	}
	if _, err := handle.File().Write(content); err != nil {
		return err
	}
	return handle.Seal()
}

func TestM7FileStoreAcknowledgeAndCleanupCrashMatrix(t *testing.T) {
	t.Run("acknowledge resumes deterministic quarantine", func(t *testing.T) {
		store := newTestStore(t)
		residentID := testResident(t, "01ARZ3NDEKTSV4RRFFQ69G5FAV")
		staged, err := store.StageBytes(context.Background(), residentID, []byte("ack crash"))
		if err != nil {
			t.Fatal(err)
		}
		if _, err := store.Finalize(context.Background(), residentID, staged); err != nil {
			t.Fatal(err)
		}
		injected := errors.New("injected acknowledge crash")
		store.testFailpoint = func(label string) error {
			if label == "acknowledge_after_quarantine" {
				return injected
			}
			return nil
		}
		if err := store.Acknowledge(context.Background(), residentID, staged); !errors.Is(err, injected) {
			t.Fatalf("acknowledge failpoint error = %v", err)
		}
		if _, err := os.Stat(filepath.Join(store.quarantineDir, "delete-stage-"+staged.TemporaryName())); err != nil {
			t.Fatalf("transient acknowledge evidence missing: %v", err)
		}
		store.testFailpoint = nil
		if err := store.Acknowledge(context.Background(), residentID, staged); err != nil {
			t.Fatalf("acknowledge retry: %v", err)
		}
		if orphans, err := store.ListOrphans(context.Background()); err != nil || len(orphans) != 0 {
			t.Fatalf("acknowledge retry orphans = %v, %v", orphans, err)
		}
	})

	t.Run("cleanup resumes quarantined object before stage", func(t *testing.T) {
		store := newTestStore(t)
		residentID := testResident(t, "01ARZ3NDEKTSV4RRFFQ69G5FAV")
		staged, err := store.StageBytes(context.Background(), residentID, []byte("cleanup crash"))
		if err != nil {
			t.Fatal(err)
		}
		if _, err := store.Finalize(context.Background(), residentID, staged); err != nil {
			t.Fatal(err)
		}
		orphans, err := store.ListOrphans(context.Background())
		if err != nil || len(orphans) != 1 {
			t.Fatalf("orphans = %v, %v", orphans, err)
		}
		injected := errors.New("injected cleanup crash")
		store.testFailpoint = func(label string) error {
			if label == "cleanup_after_object_quarantine" {
				return injected
			}
			return nil
		}
		unreferenced := ReferenceCheckFunc(func(context.Context, canonical.ID, canonical.Digest) (bool, error) { return false, nil })
		if _, err := store.CleanupOrphan(context.Background(), orphans[0], unreferenced); !errors.Is(err, injected) {
			t.Fatalf("cleanup failpoint error = %v", err)
		}
		objectPath, _ := store.objectPath(residentID, staged.Digest(), false)
		if _, err := os.Stat(objectPath); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("canonical object after crash = %v", err)
		}
		store.testFailpoint = nil
		result, err := store.CleanupOrphan(context.Background(), orphans[0], unreferenced)
		if err != nil || result.Disposition != CleanupUnreferencedObjectRemoved {
			t.Fatalf("cleanup retry = %+v, %v", result, err)
		}
		if remaining, err := store.ListOrphans(context.Background()); err != nil || len(remaining) != 0 {
			t.Fatalf("cleanup retry orphans = %v, %v", remaining, err)
		}
	})
}

func TestM7FileStoreLayoutIdentitiesAreImmutableUnderConcurrency(t *testing.T) {
	store := newTestStore(t)
	residentID := testResident(t, "01ARZ3NDEKTSV4RRFFQ69G5FAV")
	content := []byte("immutable layout")
	staged, err := store.StageBytes(context.Background(), residentID, content)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.Finalize(context.Background(), residentID, staged); err != nil {
		t.Fatal(err)
	}
	if err := store.Acknowledge(context.Background(), residentID, staged); err != nil {
		t.Fatal(err)
	}
	var group sync.WaitGroup
	errorsSeen := make(chan error, 16)
	for worker := 0; worker < 8; worker++ {
		group.Add(1)
		go func(worker int) {
			defer group.Done()
			for attempt := 0; attempt < 8; attempt++ {
				if worker%2 == 0 {
					exists, err := store.Exists(residentID, staged.Digest())
					if err != nil || !exists {
						errorsSeen <- fmt.Errorf("exists=%v: %w", exists, err)
						return
					}
				} else if got, err := store.Read(context.Background(), residentID, staged.Digest()); err != nil || !bytes.Equal(got, content) {
					errorsSeen <- fmt.Errorf("read=%q: %w", got, err)
					return
				}
			}
		}(worker)
	}
	group.Wait()
	close(errorsSeen)
	for err := range errorsSeen {
		t.Error(err)
	}
}

func TestM7FileStoreDoesNotQuarantineNonContentFailures(t *testing.T) {
	for _, testCase := range []struct {
		name string
		err  error
	}{
		{name: "identity", err: fssecure.ErrIdentityChanged},
		{name: "security", err: fssecure.ErrUnsafeFilesystem},
		{name: "io", err: io.ErrUnexpectedEOF},
		{name: "context", err: context.Canceled},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			store := newTestStore(t)
			residentID := testResident(t, "01ARZ3NDEKTSV4RRFFQ69G5FAV")
			staged, err := store.StageBytes(context.Background(), residentID, []byte("trusted"))
			if err != nil {
				t.Fatal(err)
			}
			if _, err := store.Finalize(context.Background(), residentID, staged); err != nil {
				t.Fatal(err)
			}
			layout, err := store.openLayout()
			if err != nil {
				t.Fatal(err)
			}
			defer layout.Close()
			location, exists, err := openObjectLocation(layout.objects, residentID, staged.Digest(), false)
			if err != nil || !exists {
				t.Fatalf("object location = %v/%v", exists, err)
			}
			defer location.Close()
			object, err := location.shard.OpenRegular(location.base)
			if err != nil {
				t.Fatal(err)
			}
			defer object.Close()
			if got := store.quarantineObjectMismatch(layout, location, object, residentID, staged.Digest(), testCase.err); !errors.Is(got, testCase.err) {
				t.Fatalf("decision error = %v", got)
			}
			if err := object.VerifyBound(); err != nil {
				t.Fatalf("non-content failure moved object: %v", err)
			}
			entries, err := layout.quarantine.ReadDir()
			if err != nil || len(entries) != 0 {
				t.Fatalf("non-content failure created evidence: %v/%v", entries, err)
			}
		})
	}
}

func TestM7FileStoreListsBothTransientPrefixesAcrossRepeatedReadDir(t *testing.T) {
	store := newTestStore(t)
	residentID := testResident(t, "01ARZ3NDEKTSV4RRFFQ69G5FAV")
	staged, err := store.StageBytes(context.Background(), residentID, []byte("ack transient"))
	if err != nil {
		t.Fatal(err)
	}
	layout, err := store.openLayout()
	if err != nil {
		t.Fatal(err)
	}
	marker, err := layout.staging.OpenRegular(staged.name)
	if err != nil {
		t.Fatal(err)
	}
	if err := layout.staging.MoveTo(marker, layout.quarantine, "delete-stage-"+staged.name); err != nil {
		t.Fatal(err)
	}
	if err := marker.Close(); err != nil {
		t.Fatal(err)
	}
	partial, err := layout.quarantine.CreateRegular("delete-partial-partial-READDIR")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := partial.File().Write([]byte("partial")); err != nil {
		t.Fatal(err)
	}
	if err := partial.Seal(); err != nil {
		t.Fatal(err)
	}
	if err := partial.Close(); err != nil {
		t.Fatal(err)
	}
	if err := layout.Close(); err != nil {
		t.Fatal(err)
	}
	orphans, err := store.ListOrphans(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(orphans) != 2 {
		t.Fatalf("transient orphans = %+v", orphans)
	}
	names := map[string]bool{orphans[0].Name: true, orphans[1].Name: true}
	if !names[staged.name] || !names["partial-READDIR"] {
		t.Fatalf("transient names = %v", names)
	}
}

func TestM7FileStoreCleansPublishedEntryAfterPostRenameFailure(t *testing.T) {
	store := newTestStore(t)
	residentID := testResident(t, "01ARZ3NDEKTSV4RRFFQ69G5FAV")
	staged, err := store.StageBytes(context.Background(), residentID, []byte("publish cleanup"))
	if err != nil {
		t.Fatal(err)
	}
	injected := errors.New("injected post-rename failure")
	store.testAfterPublishError = injected
	if _, err := store.Finalize(context.Background(), residentID, staged); !errors.Is(err, injected) {
		t.Fatalf("post-rename failure = %v", err)
	}
	objectPath, err := store.objectPath(residentID, staged.Digest(), false)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(objectPath); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("failed publication remained canonical: %v", err)
	}
	if _, err := os.Stat(filepath.Join(store.stagingDir, staged.name)); err != nil {
		t.Fatalf("staging source was lost: %v", err)
	}
	err = filepath.WalkDir(store.objectsDir, func(path string, entry os.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if strings.HasPrefix(entry.Name(), ".mahoroba-publish-") {
			t.Errorf("unrecovered publish temporary %q", path)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}
