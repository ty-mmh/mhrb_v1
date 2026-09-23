//go:build linux

package sqlite

import (
	"context"
	"io"
	"os"
	"path/filepath"
	"testing"

	"golang.org/x/sys/unix"
)

func TestM7ImmutableDescriptorInspectionReadsSealedMemfdWithoutPathFallback(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "source.db")
	store, err := Open(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	source, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer source.Close()
	descriptor := newSealedInspectionMemfd(t, source)
	defer descriptor.Close()
	if err := os.Remove(path); err != nil {
		t.Fatal(err)
	}

	inspection, err := OpenImmutableDescriptorInspection(ctx, descriptor, filepath.Join(t.TempDir(), "absent.db"))
	if err != nil {
		t.Fatal(err)
	}
	during := countDescriptorIdentityFDs(t, descriptor)
	if during < 2 {
		t.Fatalf("descriptor identity fd count during inspection = %d, want caller plus VFS duplicate", during)
	}
	if got := inspection.SchemaReport().SchemaVersion; got != 13 {
		t.Fatalf("schema version = %d, want 13", got)
	}
	if err := inspection.Close(); err != nil {
		t.Fatal(err)
	}
	if err := inspection.Close(); err != nil {
		t.Fatalf("second inspection close: %v", err)
	}
	if got := countDescriptorIdentityFDs(t, descriptor); got != 1 {
		t.Fatalf("descriptor identity fd count after inspection close = %d, want caller only", got)
	}
	if _, err := descriptor.Stat(); err != nil {
		t.Fatalf("inspection closed caller-owned descriptor: %v", err)
	}
}

func TestImmutableDescriptorInspectionRejectsUnsealedMemfd(t *testing.T) {
	fd, err := unix.MemfdCreate("mahoroba-unsealed-inspection-test", unix.MFD_CLOEXEC|unix.MFD_ALLOW_SEALING)
	if err != nil {
		t.Fatal(err)
	}
	descriptor := os.NewFile(uintptr(fd), "mahoroba-unsealed-inspection-test")
	if descriptor == nil {
		_ = unix.Close(fd)
		t.Fatal("construct unsealed memfd")
	}
	defer descriptor.Close()
	if _, err := OpenImmutableDescriptorInspection(context.Background(), descriptor, filepath.Join(t.TempDir(), "absent.db")); err == nil {
		t.Fatal("unsealed memfd unexpectedly opened")
	}
}

func TestImmutableDescriptorInspectionFailureReleasesDuplicatedDescriptors(t *testing.T) {
	fd, err := unix.MemfdCreate("mahoroba-invalid-inspection-test", unix.MFD_CLOEXEC|unix.MFD_ALLOW_SEALING)
	if err != nil {
		t.Fatal(err)
	}
	descriptor := os.NewFile(uintptr(fd), "mahoroba-invalid-inspection-test")
	if descriptor == nil {
		_ = unix.Close(fd)
		t.Fatal("construct invalid memfd")
	}
	defer descriptor.Close()
	if _, err := descriptor.Write([]byte("not a SQLite database")); err != nil {
		t.Fatal(err)
	}
	if _, err := descriptor.Seek(0, io.SeekStart); err != nil {
		t.Fatal(err)
	}
	if _, err := unix.FcntlInt(descriptor.Fd(), unix.F_ADD_SEALS, requiredImmutableDescriptorSeals); err != nil {
		t.Fatal(err)
	}
	if _, err := OpenImmutableDescriptorInspection(context.Background(), descriptor, filepath.Join(t.TempDir(), "absent.db")); err == nil {
		t.Fatal("invalid sealed SQLite payload unexpectedly opened")
	}
	if got := countDescriptorIdentityFDs(t, descriptor); got != 1 {
		t.Fatalf("descriptor identity fd count after failed inspection = %d, want caller only", got)
	}
}

func newSealedInspectionMemfd(t *testing.T, source *os.File) *os.File {
	t.Helper()
	fd, err := unix.MemfdCreate("mahoroba-sealed-inspection-test", unix.MFD_CLOEXEC|unix.MFD_ALLOW_SEALING)
	if err != nil {
		t.Fatal(err)
	}
	descriptor := os.NewFile(uintptr(fd), "mahoroba-sealed-inspection-test")
	if descriptor == nil {
		_ = unix.Close(fd)
		t.Fatal("construct sealed memfd")
	}
	failed := true
	defer func() {
		if failed {
			_ = descriptor.Close()
		}
	}()
	if _, err := io.Copy(descriptor, source); err != nil {
		t.Fatal(err)
	}
	if _, err := descriptor.Seek(0, io.SeekStart); err != nil {
		t.Fatal(err)
	}
	if _, err := unix.FcntlInt(descriptor.Fd(), unix.F_ADD_SEALS, requiredImmutableDescriptorSeals); err != nil {
		t.Fatal(err)
	}
	actual, err := unix.FcntlInt(descriptor.Fd(), unix.F_GET_SEALS, 0)
	if err != nil || actual&requiredImmutableDescriptorSeals != requiredImmutableDescriptorSeals {
		t.Fatalf("memfd seals = %#x, err=%v", actual, err)
	}
	failed = false
	return descriptor
}

func countDescriptorIdentityFDs(t *testing.T, descriptor *os.File) int {
	t.Helper()
	want, err := descriptor.Stat()
	if err != nil {
		t.Fatal(err)
	}
	entries, err := os.ReadDir("/proc/self/fd")
	if err != nil {
		t.Fatal(err)
	}
	count := 0
	for _, entry := range entries {
		info, err := os.Stat(filepath.Join("/proc/self/fd", entry.Name()))
		if err == nil && os.SameFile(want, info) {
			count++
		}
	}
	return count
}
