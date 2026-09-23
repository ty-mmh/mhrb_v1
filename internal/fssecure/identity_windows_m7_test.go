//go:build windows

package fssecure

import (
	"errors"
	"os"
	"testing"

	"golang.org/x/sys/windows"
)

func TestM7WindowsIdentityQueryFailureIsUnsafeAndUnsupported(t *testing.T) {
	reader, writer, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	_ = writer.Close()
	if err := reader.Close(); err != nil {
		t.Fatal(err)
	}
	_, err = identityFromHandle(reader, KindRegular)
	if !errors.Is(err, ErrUnsafeFilesystem) || !errors.Is(err, ErrUnsupportedSecureFilesystem) {
		t.Fatalf("Windows identity failure sentinel matrix = %v", err)
	}
}

func TestM7WindowsZeroIdentityIsUnsafeAndUnsupported(t *testing.T) {
	_, err := identityFromWindowsInformation(windows.ByHandleFileInformation{}, KindRegular)
	if !errors.Is(err, ErrUnsafeFilesystem) || !errors.Is(err, ErrUnsupportedSecureFilesystem) {
		t.Fatalf("zero Windows identity sentinel matrix = %v", err)
	}
	if reason, ok := Reason(err); !ok || reason != ReasonUnsupportedFilesystem {
		t.Fatalf("zero Windows identity reason = %q/%v", reason, ok)
	}
}

func TestM7WindowsUnsupportedWin32PrimitiveFailsClosed(t *testing.T) {
	for _, source := range []error{
		windows.ERROR_NOT_SUPPORTED,
		windows.ERROR_INVALID_FUNCTION,
		windows.ERROR_NOT_SAME_DEVICE,
	} {
		err := normalizeNTError("fixture primitive", source)
		if !errors.Is(err, ErrUnsafeFilesystem) || !errors.Is(err, ErrUnsupportedSecureFilesystem) {
			t.Fatalf("Win32 error %v sentinel matrix = %v", source, err)
		}
		if reason, ok := Reason(err); !ok || reason != ReasonUnsupportedFilesystem {
			t.Fatalf("Win32 error %v reason = %q/%v", source, reason, ok)
		}
	}
}

func TestWindowsRelativeRenameRetriesOnlyTransientSharingErrors(t *testing.T) {
	for _, source := range []error{
		windows.STATUS_ACCESS_DENIED,
		windows.STATUS_SHARING_VIOLATION,
		windows.ERROR_ACCESS_DENIED,
		windows.ERROR_SHARING_VIOLATION,
	} {
		if !retryableWindowsRenameError(source) {
			t.Fatalf("transient Windows rename error %v was not retryable", source)
		}
	}
	for _, source := range []error{
		windows.STATUS_OBJECT_NAME_COLLISION,
		windows.STATUS_OBJECT_NAME_NOT_FOUND,
		windows.ERROR_FILE_EXISTS,
		windows.ERROR_FILE_NOT_FOUND,
	} {
		if retryableWindowsRenameError(source) {
			t.Fatalf("semantic Windows rename error %v was retryable", source)
		}
	}
}
