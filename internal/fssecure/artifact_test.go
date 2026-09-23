package fssecure

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestM7ArtifactSourceIdentityDigestAndSiblingMarkerUseRetainedNamespace(t *testing.T) {
	policy, err := CurrentSecurityPolicy()
	if err != nil {
		t.Fatal(err)
	}
	rootPath := filepath.Join(t.TempDir(), "bundle")
	root, err := OpenOrCreateRoot(rootPath, policy)
	if err != nil {
		t.Fatal(err)
	}
	digest, err := root.ArtifactSourceIdentityDigest()
	if err != nil {
		t.Fatal(err)
	}
	if len(digest) != len("sha256:")+64 || !strings.HasPrefix(digest, "sha256:") {
		t.Fatalf("artifact identity digest = %q", digest)
	}
	if err := root.Close(); err != nil {
		t.Fatal(err)
	}
	root, err = OpenRootForPublish(rootPath, policy)
	if err != nil {
		t.Fatal(err)
	}

	const markerName = ".bundle.publish-pending.01ARZ3NDEKTSV4RRFFQ69G5FAV"
	marker, err := root.CreateSiblingRegular(markerName)
	if err != nil {
		t.Fatal(err)
	}
	defer marker.Close()
	if _, err := marker.File().Write([]byte("marker\n")); err != nil {
		t.Fatal(err)
	}
	if err := marker.Seal(); err != nil {
		t.Fatal(err)
	}
	if err := root.SyncParent(); err != nil {
		t.Fatal(err)
	}
	if err := marker.Close(); err != nil {
		t.Fatal(err)
	}
	reader, found, err := root.OpenUniquePublishMarkerSiblingForUpdate("bundle")
	if err != nil || !found {
		t.Fatal(err)
	}
	body := make([]byte, len("marker\n"))
	if _, err := reader.File().Read(body); err != nil {
		t.Fatal(err)
	}
	if string(body) != "marker\n" {
		t.Fatalf("marker bytes = %q", body)
	}
	if err := reader.Close(); err != nil {
		t.Fatal(err)
	}
	marker, err = root.OpenSiblingRegularForUpdate(markerName)
	if err != nil {
		t.Fatal(err)
	}
	if err := marker.MarkDeleteOnClose(); err != nil {
		t.Fatal(err)
	}
	if err := marker.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Lstat(filepath.Join(filepath.Dir(rootPath), markerName)); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("deleted sibling marker still exists: %v", err)
	}
	if err := root.Close(); err != nil {
		t.Fatal(err)
	}

	readOnly, err := OpenRootReadOnly(rootPath, policy)
	if err != nil {
		t.Fatal(err)
	}
	defer readOnly.Close()
	reopenedDigest, err := readOnly.ArtifactSourceIdentityDigest()
	if err != nil {
		t.Fatal(err)
	}
	if reopenedDigest != digest {
		t.Fatalf("identity digest changed across stable reopen: %q != %q", reopenedDigest, digest)
	}
	if marker, err := readOnly.CreateSiblingRegular(markerName); !errors.Is(err, ErrUnsafeFilesystem) {
		if marker != nil {
			_ = marker.Close()
		}
		t.Fatalf("read-only sibling create error = %v, want ErrUnsafeFilesystem", err)
	}
	if marker, err := readOnly.OpenSiblingRegular(markerName); !errors.Is(err, ErrUnsafeFilesystem) {
		if marker != nil {
			_ = marker.Close()
		}
		t.Fatalf("read-only sibling inspect error = %v, want ErrUnsafeFilesystem", err)
	}
	if marker, _, err := readOnly.OpenUniquePublishMarkerSiblingForUpdate("bundle"); !errors.Is(err, ErrUnsafeFilesystem) {
		if marker != nil {
			_ = marker.Close()
		}
		t.Fatalf("read-only sibling enumeration error = %v, want ErrUnsafeFilesystem", err)
	}
}
