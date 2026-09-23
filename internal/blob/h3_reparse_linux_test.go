//go:build linux

package blob

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"mahoroba.local/mahoroba/internal/fssecure"
)

func createDirectoryReparse(t *testing.T, target, link string) {
	t.Helper()
	if err := os.Symlink(target, link); err != nil {
		t.Fatal(err)
	}
}

func TestM7FileStoreRejectsLinuxPostPublicationIdentitySwap(t *testing.T) {
	store := newTestStore(t)
	residentID := testResident(t, "01ARZ3NDEKTSV4RRFFQ69G5FAV")
	staged, err := store.StageBytes(context.Background(), residentID, []byte("trusted"))
	if err != nil {
		t.Fatal(err)
	}
	var canonicalPath, displacedPath string
	store.testHook = func(label string) {
		if label != "finalize_after_publish" {
			return
		}
		canonicalPath, err = store.objectPath(residentID, staged.Digest(), false)
		if err != nil {
			t.Fatal(err)
		}
		displacedPath = canonicalPath + "-displaced"
		if err := os.Rename(canonicalPath, displacedPath); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(canonicalPath, []byte("replacement"), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	_, err = store.Finalize(context.Background(), residentID, staged)
	if !errors.Is(err, fssecure.ErrIdentityChanged) {
		t.Fatalf("post-publication identity swap error = %v", err)
	}
	if got, err := os.ReadFile(canonicalPath); err != nil || !bytes.Equal(got, []byte("replacement")) {
		t.Fatalf("current untrusted entry was mutated after identity failure: %q/%v", got, err)
	}
	if got, err := os.ReadFile(displacedPath); err != nil || !bytes.Equal(got, []byte("trusted")) {
		t.Fatalf("original published evidence was mutated: %q/%v", got, err)
	}
	entries, err := os.ReadDir(filepath.Join(store.root, "quarantine"))
	if err != nil || len(entries) != 0 {
		t.Fatalf("identity failure authorized quarantine: %v/%v", entries, err)
	}
}

func TestM7FileStoreRejectsOperationMidflightAncestorSwap(t *testing.T) {
	for _, target := range []string{"staging", "root", "root_parent"} {
		t.Run(target, func(t *testing.T) {
			outer := t.TempDir()
			activeParent := filepath.Join(outer, "active-parent")
			if err := os.Mkdir(activeParent, 0o700); err != nil {
				t.Fatal(err)
			}
			activeRoot := filepath.Join(activeParent, "blob-root")
			store, err := NewFileStore(activeRoot)
			if err != nil {
				t.Fatal(err)
			}
			residentID := testResident(t, "01ARZ3NDEKTSV4RRFFQ69G5FAV")
			content := []byte("trusted midflight evidence")
			staged, err := store.StageBytes(context.Background(), residentID, content)
			if err != nil {
				t.Fatal(err)
			}

			displacedRoot := activeRoot
			var swap func() error
			switch target {
			case "staging":
				replacement := filepath.Join(activeRoot, "staging-replacement")
				if err := os.Mkdir(replacement, 0o700); err != nil {
					t.Fatal(err)
				}
				swap = func() error {
					if err := os.Rename(filepath.Join(activeRoot, "staging"), filepath.Join(activeRoot, "staging-displaced")); err != nil {
						return err
					}
					return os.Rename(replacement, filepath.Join(activeRoot, "staging"))
				}
			case "root":
				replacementRoot := filepath.Join(outer, "root-replacement")
				if _, err := NewFileStore(replacementRoot); err != nil {
					t.Fatal(err)
				}
				displacedRoot = filepath.Join(activeParent, "blob-root-displaced")
				swap = func() error {
					if err := os.Rename(activeRoot, displacedRoot); err != nil {
						return err
					}
					return os.Rename(replacementRoot, activeRoot)
				}
			case "root_parent":
				replacementParent := filepath.Join(outer, "parent-replacement")
				if err := os.Mkdir(replacementParent, 0o700); err != nil {
					t.Fatal(err)
				}
				if _, err := NewFileStore(filepath.Join(replacementParent, filepath.Base(activeRoot))); err != nil {
					t.Fatal(err)
				}
				displacedParent := filepath.Join(outer, "parent-displaced")
				displacedRoot = filepath.Join(displacedParent, filepath.Base(activeRoot))
				swap = func() error {
					if err := os.Rename(activeParent, displacedParent); err != nil {
						return err
					}
					return os.Rename(replacementParent, activeParent)
				}
			default:
				t.Fatalf("unknown target %q", target)
			}

			swapped := false
			store.testHook = func(label string) {
				if label != "finalize_after_source_hash" || swapped {
					return
				}
				swapped = true
				if err := swap(); err != nil {
					t.Fatal(err)
				}
			}
			_, err = store.Finalize(context.Background(), residentID, staged)
			if !swapped {
				t.Fatal("midflight swap hook was not reached")
			}
			if !errors.Is(err, ErrUnsafeFilesystem) || !errors.Is(err, fssecure.ErrUnsafeFilesystem) || !errors.Is(err, fssecure.ErrIdentityChanged) {
				t.Fatalf("midflight identity error = %v", err)
			}
			if errors.Is(err, fssecure.ErrUnsupportedSecureFilesystem) {
				t.Fatalf("identity swap was misclassified as unsupported: %v", err)
			}
			if reason, ok := fssecure.Reason(err); !ok || reason != fssecure.ReasonDirectoryIdentityChanged {
				t.Fatalf("midflight identity reason = %q/%v, error=%v", reason, ok, err)
			}

			currentStaging, currentObjects, currentQuarantine := filepath.Join(activeRoot, "staging"), filepath.Join(activeRoot, "objects"), filepath.Join(activeRoot, "quarantine")
			for _, directory := range []string{currentStaging, currentObjects, currentQuarantine, filepath.Join(displacedRoot, "objects"), filepath.Join(displacedRoot, "quarantine")} {
				entries, readErr := os.ReadDir(directory)
				if readErr != nil || len(entries) != 0 {
					t.Fatalf("unsafe mutation in %s: entries=%v err=%v", directory, entries, readErr)
				}
			}
			displacedStaging := filepath.Join(displacedRoot, "staging")
			if target == "staging" {
				displacedStaging = filepath.Join(displacedRoot, "staging-displaced")
			}
			got, readErr := os.ReadFile(filepath.Join(displacedStaging, staged.TemporaryName()))
			if readErr != nil || !bytes.Equal(got, content) {
				t.Fatalf("trusted staged evidence changed: bytes=%q err=%v", got, readErr)
			}
			evidenceRoot := displacedRoot
			evidenceDirectory := "staging"
			if target == "staging" {
				evidenceRoot = activeRoot
				evidenceDirectory = "staging-displaced"
			}
			rootHandle, openErr := fssecure.OpenRoot(evidenceRoot, store.policy)
			if openErr != nil {
				t.Fatal(openErr)
			}
			stageDirectory, openErr := rootHandle.OpenDirectory(evidenceDirectory)
			if openErr != nil {
				_ = rootHandle.Close()
				t.Fatal(openErr)
			}
			stageHandle, openErr := stageDirectory.OpenRegular(staged.TemporaryName())
			if openErr != nil {
				_ = stageDirectory.Close()
				_ = rootHandle.Close()
				t.Fatal(openErr)
			}
			if !stageHandle.Identity().Equal(staged.identity) || stageHandle.Snapshot().Size() != staged.Size().Int64() || !stageHandle.Snapshot().ModTime().Equal(staged.modTime) {
				t.Fatalf("trusted staged evidence identity changed")
			}
			if closeErr := errors.Join(stageHandle.Close(), stageDirectory.Close(), rootHandle.Close()); closeErr != nil {
				t.Fatal(closeErr)
			}
		})
	}
}
