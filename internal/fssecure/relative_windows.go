//go:build windows

package fssecure

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"syscall"
	"time"
	"unsafe"

	"golang.org/x/sys/windows"
)

type fileRenameInformation struct {
	ReplaceIfExists uint32
	RootDirectory   windows.Handle
	FileNameLength  uint32
	FileName        [1]uint16
}

const (
	windowsRenameAttempts = 50
	windowsRenameBackoff  = 20 * time.Millisecond
)

func openRootRelative(path string, create bool, policy SecurityPolicy) (*os.File, *os.File, *directoryBinding, string, error) {
	clean := filepath.Clean(path)
	volume := filepath.VolumeName(clean)
	if len(volume) != 2 || volume[1] != ':' || strings.HasPrefix(volume, `\\`) {
		return nil, nil, nil, "", unsupportedFilesystem("only local drive volumes are supported", nil)
	}
	volumeRoot := volume + `\`
	current, err := openAbsoluteDirectoryRaw(volumeRoot)
	if err != nil {
		return nil, nil, nil, "", err
	}
	rest := strings.TrimPrefix(clean[len(volume):], `\`)
	components := strings.FieldsFunc(rest, func(r rune) bool { return r == '\\' || r == '/' })
	if len(components) == 0 {
		_ = current.Close()
		return nil, nil, nil, "", fmt.Errorf("%w: drive root cannot be a managed blob root", ErrUnsafeFilesystem)
	}
	var ancestors *directoryBinding
	for _, component := range components[:len(components)-1] {
		next, openErr := ntOpenRelative(current, component, windows.FILE_GENERIC_READ|windows.READ_CONTROL, windows.FILE_SHARE_READ|windows.FILE_SHARE_WRITE|windows.FILE_SHARE_DELETE, windows.FILE_OPEN, windows.FILE_DIRECTORY_FILE|windows.FILE_OPEN_REPARSE_POINT, nil)
		if openErr != nil {
			_ = current.Close()
			_ = ancestors.close()
			return nil, nil, nil, "", normalizeNTError("traverse root component", openErr)
		}
		if err := rejectWindowsReparse(next); err != nil {
			_ = next.Close()
			_ = current.Close()
			_ = ancestors.close()
			return nil, nil, nil, "", err
		}
		nextAncestors, bindingErr := retainRawDirectoryBinding(ancestors, current, next, component)
		if bindingErr != nil {
			_ = next.Close()
			_ = current.Close()
			_ = ancestors.close()
			return nil, nil, nil, "", bindingErr
		}
		ancestors = nextAncestors
		current = next
	}
	base := components[len(components)-1]
	file, err := openDirectoryRelative(current, base, create, policy)
	if err != nil {
		_ = current.Close()
		_ = ancestors.close()
		return nil, nil, nil, "", err
	}
	return file, current, ancestors, base, nil
}

func openRootRelativeReadOnly(path string, policy SecurityPolicy) (*os.File, *os.File, *directoryBinding, string, error) {
	clean := filepath.Clean(path)
	volume := filepath.VolumeName(clean)
	if len(volume) != 2 || volume[1] != ':' || strings.HasPrefix(volume, `\\`) {
		return nil, nil, nil, "", unsupportedFilesystem("only local drive volumes are supported", nil)
	}
	current, err := openAbsoluteDirectoryRaw(volume + `\`)
	if err != nil {
		return nil, nil, nil, "", err
	}
	components := strings.FieldsFunc(strings.TrimPrefix(clean[len(volume):], `\`), func(r rune) bool { return r == '\\' || r == '/' })
	if len(components) == 0 {
		_ = current.Close()
		return nil, nil, nil, "", fmt.Errorf("%w: drive root cannot be a managed root", ErrUnsafeFilesystem)
	}
	var ancestors *directoryBinding
	for _, component := range components[:len(components)-1] {
		next, openErr := ntOpenRelative(current, component, windows.FILE_GENERIC_READ|windows.READ_CONTROL, windows.FILE_SHARE_READ|windows.FILE_SHARE_WRITE|windows.FILE_SHARE_DELETE, windows.FILE_OPEN, windows.FILE_DIRECTORY_FILE|windows.FILE_OPEN_REPARSE_POINT, nil)
		if openErr != nil {
			_ = current.Close()
			_ = ancestors.close()
			return nil, nil, nil, "", normalizeNTError("traverse read-only root component", openErr)
		}
		if err := rejectWindowsReparse(next); err != nil {
			_ = next.Close()
			_ = current.Close()
			_ = ancestors.close()
			return nil, nil, nil, "", err
		}
		nextAncestors, bindingErr := retainRawDirectoryBinding(ancestors, current, next, component)
		if bindingErr != nil {
			_ = next.Close()
			_ = current.Close()
			_ = ancestors.close()
			return nil, nil, nil, "", bindingErr
		}
		ancestors = nextAncestors
		current = next
	}
	base := components[len(components)-1]
	file, err := openDirectoryRelativeReadOnly(current, base, policy)
	if err != nil {
		_ = current.Close()
		_ = ancestors.close()
		return nil, nil, nil, "", err
	}
	return file, current, ancestors, base, nil
}

func openRootRelativeForPublish(path string, policy SecurityPolicy) (*os.File, *os.File, *directoryBinding, string, error) {
	clean := filepath.Clean(path)
	volume := filepath.VolumeName(clean)
	if len(volume) != 2 || volume[1] != ':' || strings.HasPrefix(volume, `\\`) {
		return nil, nil, nil, "", unsupportedFilesystem("only local drive volumes are supported", nil)
	}
	current, err := openAbsoluteDirectoryRaw(volume + `\`)
	if err != nil {
		return nil, nil, nil, "", err
	}
	components := strings.FieldsFunc(strings.TrimPrefix(clean[len(volume):], `\`), func(r rune) bool { return r == '\\' || r == '/' })
	if len(components) < 2 {
		_ = current.Close()
		return nil, nil, nil, "", unsupportedFilesystem("drive-root publication is unsupported", nil)
	}
	var ancestors *directoryBinding
	for index, component := range components[:len(components)-1] {
		access := uint32(windows.FILE_GENERIC_READ | windows.READ_CONTROL)
		if index == len(components)-2 {
			access |= windows.FILE_GENERIC_WRITE
		}
		next, openErr := ntOpenRelative(current, component, access, windows.FILE_SHARE_READ|windows.FILE_SHARE_WRITE|windows.FILE_SHARE_DELETE, windows.FILE_OPEN, windows.FILE_DIRECTORY_FILE|windows.FILE_OPEN_REPARSE_POINT, nil)
		if openErr != nil {
			_ = current.Close()
			_ = ancestors.close()
			return nil, nil, nil, "", normalizeNTError("traverse publication root component", openErr)
		}
		if err := rejectWindowsReparse(next); err != nil {
			_ = next.Close()
			_ = current.Close()
			_ = ancestors.close()
			return nil, nil, nil, "", err
		}
		nextAncestors, bindingErr := retainRawDirectoryBinding(ancestors, current, next, component)
		if bindingErr != nil {
			_ = next.Close()
			_ = current.Close()
			_ = ancestors.close()
			return nil, nil, nil, "", bindingErr
		}
		ancestors = nextAncestors
		current = next
	}
	base := components[len(components)-1]
	// DELETE access is acquired only immediately around rename. Retaining it
	// here would conflict with fresh digest/ReadDir handles because the secure
	// policy intentionally denies delete sharing.
	file, err := openDirectoryRelative(current, base, false, policy)
	if err != nil {
		_ = current.Close()
		_ = ancestors.close()
		return nil, nil, nil, "", err
	}
	return file, current, ancestors, base, nil
}

func openAbsoluteDirectoryRaw(path string) (*os.File, error) {
	name, err := windows.UTF16PtrFromString(path)
	if err != nil {
		return nil, err
	}
	handle, err := windows.CreateFile(
		name,
		windows.FILE_GENERIC_READ|windows.READ_CONTROL,
		windows.FILE_SHARE_READ|windows.FILE_SHARE_WRITE|windows.FILE_SHARE_DELETE,
		nil,
		windows.OPEN_EXISTING,
		windows.FILE_FLAG_BACKUP_SEMANTICS|windows.FILE_FLAG_OPEN_REPARSE_POINT,
		0,
	)
	if err != nil {
		return nil, err
	}
	file := os.NewFile(uintptr(handle), path)
	if file == nil {
		_ = windows.CloseHandle(handle)
		return nil, ErrIdentityChanged
	}
	if err := rejectWindowsReparse(file); err != nil {
		_ = file.Close()
		return nil, err
	}
	return file, nil
}

func openDirectoryRelative(parent *os.File, name string, create bool, policy SecurityPolicy) (*os.File, error) {
	disposition := uint32(windows.FILE_OPEN)
	var descriptor *windows.SECURITY_DESCRIPTOR
	if create {
		disposition = windows.FILE_OPEN_IF
		var err error
		descriptor, err = buildWindowsSecurityDescriptor(policy)
		if err != nil {
			return nil, err
		}
	}
	file, err := ntOpenRelative(
		parent,
		name,
		windows.FILE_GENERIC_READ|windows.FILE_GENERIC_WRITE|windows.READ_CONTROL,
		windows.FILE_SHARE_READ|windows.FILE_SHARE_WRITE,
		disposition,
		windows.FILE_DIRECTORY_FILE|windows.FILE_OPEN_REPARSE_POINT|windows.FILE_SYNCHRONOUS_IO_NONALERT,
		descriptor,
	)
	if err != nil {
		return nil, normalizeNTError("open directory", err)
	}
	if err := rejectWindowsReparse(file); err != nil {
		_ = file.Close()
		return nil, err
	}
	if err := validateOpenedSecurity(file, KindDirectory, policy); err != nil {
		_ = file.Close()
		return nil, err
	}
	return file, nil
}

func openDirectoryRelativeForPublish(parent *os.File, name string, policy SecurityPolicy) (*os.File, error) {
	file, err := ntOpenRelative(
		parent,
		name,
		windows.FILE_GENERIC_READ|windows.FILE_GENERIC_WRITE|windows.DELETE|windows.READ_CONTROL,
		windows.FILE_SHARE_READ|windows.FILE_SHARE_WRITE,
		windows.FILE_OPEN,
		windows.FILE_DIRECTORY_FILE|windows.FILE_OPEN_REPARSE_POINT|windows.FILE_SYNCHRONOUS_IO_NONALERT,
		nil,
	)
	if err != nil {
		return nil, normalizeNTError("open directory for publication", err)
	}
	if err := rejectWindowsReparse(file); err != nil {
		_ = file.Close()
		return nil, err
	}
	if err := validateOpenedSecurity(file, KindDirectory, policy); err != nil {
		_ = file.Close()
		return nil, err
	}
	return file, nil
}

func openDirectoryRelativeReadOnly(parent *os.File, name string, policy SecurityPolicy) (*os.File, error) {
	file, err := ntOpenRelative(
		parent,
		name,
		windows.FILE_GENERIC_READ|windows.READ_CONTROL,
		windows.FILE_SHARE_READ,
		windows.FILE_OPEN,
		windows.FILE_DIRECTORY_FILE|windows.FILE_OPEN_REPARSE_POINT|windows.FILE_SYNCHRONOUS_IO_NONALERT,
		nil,
	)
	if err != nil {
		return nil, normalizeNTError("open read-only directory", err)
	}
	if err := rejectWindowsReparse(file); err != nil {
		_ = file.Close()
		return nil, err
	}
	if err := validateOpenedSecurity(file, KindDirectory, policy); err != nil {
		_ = file.Close()
		return nil, err
	}
	return file, nil
}

func openRegularRelative(parent *os.File, name string, mode regularOpenMode, policy SecurityPolicy) (*os.File, bool, error) {
	disposition := uint32(windows.FILE_OPEN)
	var descriptor *windows.SECURITY_DESCRIPTOR
	created := false
	switch mode {
	case regularReadExisting, regularObserveMutable, regularReadWriteExisting:
	case regularOpenOrCreate:
		disposition = windows.FILE_OPEN_IF
		var err error
		descriptor, err = buildWindowsSecurityDescriptor(policy)
		if err != nil {
			return nil, false, err
		}
	case regularCreateExclusive:
		disposition = windows.FILE_CREATE
		created = true
		var err error
		descriptor, err = buildWindowsSecurityDescriptor(policy)
		if err != nil {
			return nil, false, err
		}
	default:
		return nil, false, ErrUnsafeFilesystem
	}
	access := uint32(windows.FILE_GENERIC_READ | windows.READ_CONTROL)
	share := uint32(windows.FILE_SHARE_READ)
	if mode == regularObserveMutable {
		access = windows.FILE_READ_ATTRIBUTES | windows.READ_CONTROL | windows.SYNCHRONIZE
		share = windows.FILE_SHARE_READ | windows.FILE_SHARE_WRITE
	} else if mode != regularReadExisting {
		access = windows.FILE_GENERIC_READ | windows.FILE_GENERIC_WRITE | windows.DELETE | windows.READ_CONTROL
	}
	file, information, err := ntOpenRelativeWithInformation(
		parent,
		name,
		access,
		share,
		disposition,
		windows.FILE_NON_DIRECTORY_FILE|windows.FILE_OPEN_REPARSE_POINT|windows.FILE_SYNCHRONOUS_IO_NONALERT,
		descriptor,
	)
	if err != nil {
		return nil, false, normalizeNTError("open regular file", err)
	}
	if mode == regularOpenOrCreate {
		created = information == 2 // FILE_CREATED
	}
	if err := rejectWindowsReparse(file); err != nil {
		_ = file.Close()
		return nil, false, err
	}
	if err := validateOpenedSecurity(file, KindRegular, policy); err != nil {
		_ = file.Close()
		return nil, false, err
	}
	return file, created, nil
}

func ntOpenRelative(parent *os.File, name string, access, share, disposition, options uint32, descriptor *windows.SECURITY_DESCRIPTOR) (*os.File, error) {
	file, _, err := ntOpenRelativeWithInformation(parent, name, access, share, disposition, options, descriptor)
	return file, err
}

func ntOpenRelativeWithInformation(parent *os.File, name string, access, share, disposition, options uint32, descriptor *windows.SECURITY_DESCRIPTOR) (*os.File, uintptr, error) {
	if parent == nil {
		return nil, 0, ErrIdentityChanged
	}
	objectName, err := windows.NewNTUnicodeString(name)
	if err != nil {
		return nil, 0, err
	}
	var selfRelative *windows.SECURITY_DESCRIPTOR
	if descriptor != nil {
		selfRelative, err = descriptor.ToSelfRelative()
		if err != nil {
			return nil, 0, err
		}
	}
	oa := &windows.OBJECT_ATTRIBUTES{
		Length:             uint32(unsafe.Sizeof(windows.OBJECT_ATTRIBUTES{})),
		RootDirectory:      windows.Handle(parent.Fd()),
		ObjectName:         objectName,
		Attributes:         windows.OBJ_CASE_INSENSITIVE | windows.OBJ_DONT_REPARSE,
		SecurityDescriptor: selfRelative,
	}
	var handle windows.Handle
	var status windows.IO_STATUS_BLOCK
	var allocation int64
	err = windows.NtCreateFile(&handle, access, oa, &status, &allocation, 0, share, disposition, options, 0, 0)
	runtime.KeepAlive(descriptor)
	runtime.KeepAlive(selfRelative)
	runtime.KeepAlive(objectName)
	if err != nil {
		return nil, 0, err
	}
	file := os.NewFile(uintptr(handle), name)
	if file == nil {
		_ = windows.CloseHandle(handle)
		return nil, 0, ErrIdentityChanged
	}
	return file, status.Information, nil
}

func rejectWindowsReparse(file *os.File) error {
	if file == nil {
		return ErrIdentityChanged
	}
	var information windows.ByHandleFileInformation
	if err := windows.GetFileInformationByHandle(windows.Handle(file.Fd()), &information); err != nil {
		return err
	}
	if information.FileAttributes&windows.FILE_ATTRIBUTE_REPARSE_POINT != 0 {
		return WithReason(ReasonReparseOrSymlink, ErrUnsafeFilesystem)
	}
	return nil
}

func duplicateOpenFile(file *os.File, name string) (*os.File, error) {
	if file == nil {
		return nil, ErrIdentityChanged
	}
	process := windows.CurrentProcess()
	var duplicate windows.Handle
	if err := windows.DuplicateHandle(process, windows.Handle(file.Fd()), process, &duplicate, 0, false, windows.DUPLICATE_SAME_ACCESS); err != nil {
		return nil, err
	}
	result := os.NewFile(uintptr(duplicate), name)
	if result == nil {
		_ = windows.CloseHandle(duplicate)
		return nil, ErrIdentityChanged
	}
	return result, nil
}

func renameRelativeNoReplace(sourceParent *os.File, sourceName string, source *os.File, destination *os.File, destinationName string) error {
	if sourceParent == nil || source == nil || destination == nil {
		return ErrIdentityChanged
	}
	utf16Name, err := windows.UTF16FromString(destinationName)
	if err != nil {
		return err
	}
	fileNameLen := len(utf16Name)*2 - 2
	var dummy fileRenameInformation
	bufferSize := int(unsafe.Offsetof(dummy.FileName)) + fileNameLen
	buffer := make([]byte, bufferSize)
	information := (*fileRenameInformation)(unsafe.Pointer(&buffer[0]))
	information.ReplaceIfExists = 0
	information.RootDirectory = windows.Handle(destination.Fd())
	information.FileNameLength = uint32(fileNameLen)
	copy((*[windows.MAX_LONG_PATH]uint16)(unsafe.Pointer(&information.FileName[0]))[:fileNameLen/2:fileNameLen/2], utf16Name)
	var lastErr error
	for attempt := 0; attempt < windowsRenameAttempts; attempt++ {
		var status windows.IO_STATUS_BLOCK
		lastErr = windows.NtSetInformationFile(windows.Handle(source.Fd()), &status, &buffer[0], uint32(bufferSize), windows.FileRenameInformation)
		if lastErr == nil {
			return nil
		}
		if !retryableWindowsRenameError(lastErr) || attempt == windowsRenameAttempts-1 {
			return normalizeNTError("relative no-replace rename", lastErr)
		}
		// Directory rename can be transiently denied while a trusted OS scanner
		// closes a child handle. Retrying this exact source/destination handle
		// pair preserves no-replace and never reopens a namespace path.
		time.Sleep(windowsRenameBackoff)
	}
	return normalizeNTError("relative no-replace rename", lastErr)
}

func retryableWindowsRenameError(err error) bool {
	var status windows.NTStatus
	if errors.As(err, &status) {
		return status == windows.STATUS_ACCESS_DENIED || status == windows.STATUS_SHARING_VIOLATION
	}
	return errors.Is(err, windows.ERROR_ACCESS_DENIED) || errors.Is(err, windows.ERROR_SHARING_VIOLATION)
}

func removeRelative(parent *os.File, name string, handle *os.File, _ SecurityPolicy) error {
	if parent == nil || handle == nil {
		return ErrIdentityChanged
	}
	var disposition byte = 1
	if err := windows.SetFileInformationByHandle(windows.Handle(handle.Fd()), windows.FileDispositionInfo, &disposition, 1); err != nil {
		return normalizeNTError("handle-bound delete", err)
	}
	return nil
}

func identityRelativeEntry(parent *os.File, name string, kind EntryKind, policy SecurityPolicy) (Identity, error) {
	if parent == nil {
		return Identity{}, ErrIdentityChanged
	}
	options := uint32(windows.FILE_NON_DIRECTORY_FILE | windows.FILE_OPEN_REPARSE_POINT | windows.FILE_SYNCHRONOUS_IO_NONALERT)
	if kind == KindDirectory {
		options = windows.FILE_DIRECTORY_FILE | windows.FILE_OPEN_REPARSE_POINT | windows.FILE_SYNCHRONOUS_IO_NONALERT
	}
	file, err := ntOpenRelative(
		parent,
		name,
		windows.FILE_GENERIC_READ|windows.READ_CONTROL,
		windows.FILE_SHARE_READ|windows.FILE_SHARE_WRITE|windows.FILE_SHARE_DELETE,
		windows.FILE_OPEN,
		options,
		nil,
	)
	if err != nil {
		return Identity{}, normalizeNTError("verify relative identity", err)
	}
	defer file.Close()
	if err := rejectWindowsReparse(file); err != nil {
		return Identity{}, err
	}
	if err := validateOpenedSecurity(file, kind, policy); err != nil {
		return Identity{}, err
	}
	return identityFromHandle(file, kind)
}

func validateRawDirectoryHandle(file *os.File, expected Identity) error {
	if err := rejectWindowsReparse(file); err != nil {
		return err
	}
	current, err := identityFromHandle(file, KindDirectory)
	if err != nil || !expected.Equal(current) {
		return identityChanged(ReasonDirectoryIdentityChanged, err)
	}
	return nil
}

func identityRelativeDirectoryRaw(parent *os.File, name string) (Identity, error) {
	file, err := ntOpenRelative(
		parent,
		name,
		windows.FILE_GENERIC_READ|windows.READ_CONTROL,
		windows.FILE_SHARE_READ|windows.FILE_SHARE_WRITE|windows.FILE_SHARE_DELETE,
		windows.FILE_OPEN,
		windows.FILE_DIRECTORY_FILE|windows.FILE_OPEN_REPARSE_POINT|windows.FILE_SYNCHRONOUS_IO_NONALERT,
		nil,
	)
	if err != nil {
		return Identity{}, normalizeNTError("verify raw ancestor identity", err)
	}
	defer file.Close()
	if err := rejectWindowsReparse(file); err != nil {
		return Identity{}, err
	}
	return identityFromHandle(file, KindDirectory)
}

func normalizeNTError(operation string, err error) error {
	if errors.Is(err, windows.ERROR_NOT_SUPPORTED) || errors.Is(err, windows.ERROR_INVALID_FUNCTION) || errors.Is(err, windows.ERROR_NOT_SAME_DEVICE) {
		return unsupportedFilesystem(operation, err)
	}
	var status windows.NTStatus
	if errors.As(err, &status) {
		switch status {
		case windows.STATUS_OBJECT_NAME_NOT_FOUND, windows.STATUS_OBJECT_PATH_NOT_FOUND:
			return &os.PathError{Op: operation, Err: syscall.ERROR_FILE_NOT_FOUND}
		case windows.STATUS_OBJECT_NAME_COLLISION:
			return &os.PathError{Op: operation, Err: syscall.ERROR_FILE_EXISTS}
		case windows.STATUS_REPARSE_POINT_ENCOUNTERED:
			return WithReason(ReasonReparseOrSymlink, errors.Join(ErrUnsafeFilesystem, err))
		case windows.STATUS_NOT_SUPPORTED, windows.STATUS_NOT_SAME_DEVICE, windows.STATUS_INVALID_DEVICE_REQUEST, windows.STATUS_NOT_SUPPORTED_IN_APPCONTAINER:
			return unsupportedFilesystem(operation, err)
		}
		return &os.PathError{Op: operation, Err: status.Errno()}
	}
	return err
}
