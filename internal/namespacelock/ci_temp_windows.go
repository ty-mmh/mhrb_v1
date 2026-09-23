//go:build windows

package namespacelock

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"golang.org/x/sys/windows"
)

// PrepareHostedCITestTemp resolves the current token's LocalAppData known
// folder and creates one protected test directory directly below it. There is
// deliberately no environment-variable or path fallback.
func PrepareHostedCITestTemp(basename string) (string, error) {
	parent, err := hostedCITestTempParent()
	if err != nil {
		return "", err
	}
	return PrepareCITestTemp(parent, basename)
}

func hostedCITestTempParent() (string, error) {
	parent, err := windows.KnownFolderPath(windows.FOLDERID_LocalAppData, windows.KF_FLAG_DEFAULT)
	if err != nil {
		return "", errors.Join(ErrUnsupported, err)
	}
	return parent, nil
}

// PrepareCITestTemp creates one new protected test directory below parent.
// It is intentionally create-new-only: an existing or unsafe namespace is
// never repaired, deleted, or reused.
func PrepareCITestTemp(parent, basename string) (string, error) {
	if !filepath.IsAbs(parent) || validateCITestTempBasename(basename) != nil {
		return "", ErrUnsafeTarget
	}
	parent = filepath.Clean(parent)
	volume := filepath.VolumeName(parent)
	if len(volume) != 2 || volume[1] != ':' || strings.EqualFold(parent, volume+`\`) {
		return "", ErrUnsafeTarget
	}

	parentFile, identity, err := platformOpenParent(parent)
	if err != nil {
		return "", fmt.Errorf("namespacelock: open CI temp parent: %w", err)
	}
	defer parentFile.Close()
	binding, err := platformBindParentPath(parent, identity)
	if err != nil {
		return "", fmt.Errorf("namespacelock: bind CI temp parent: %w", err)
	}
	defer binding.close()

	descriptor, err := protectedWindowsSecurityDescriptorWithInheritance(windows.SUB_CONTAINERS_AND_OBJECTS_INHERIT)
	if err != nil {
		return "", err
	}
	target, _, err := ntOpenRelative(
		parentFile,
		basename,
		windows.FILE_GENERIC_READ|windows.FILE_GENERIC_WRITE|windows.READ_CONTROL,
		windows.FILE_SHARE_READ|windows.FILE_SHARE_WRITE,
		windows.FILE_CREATE,
		windows.FILE_DIRECTORY_FILE|windows.FILE_OPEN_REPARSE_POINT|windows.FILE_SYNCHRONOUS_IO_NONALERT,
		descriptor,
	)
	if err != nil {
		return "", normalizeWindowsError("create protected CI test temp", basename, err)
	}
	defer target.Close()
	targetIdentity, err := platformIdentityOf(target, entryDirectory)
	if err != nil {
		return "", err
	}
	if err := validateWindowsInheritableProtectedSecurity(target); err != nil {
		return "", fmt.Errorf("namespacelock: verify protected CI test temp: %w", err)
	}
	if err := binding.verify(); err != nil {
		return "", fmt.Errorf("namespacelock: verify CI temp parent binding: %w", err)
	}

	reopened, _, err := ntOpenRelative(
		parentFile,
		basename,
		windows.FILE_GENERIC_READ|windows.READ_CONTROL,
		windows.FILE_SHARE_READ|windows.FILE_SHARE_WRITE,
		windows.FILE_OPEN,
		windows.FILE_DIRECTORY_FILE|windows.FILE_OPEN_REPARSE_POINT|windows.FILE_SYNCHRONOUS_IO_NONALERT,
		nil,
	)
	if err != nil {
		return "", normalizeWindowsError("reopen protected CI test temp", basename, err)
	}
	reopenedIdentity, identityErr := platformIdentityOf(reopened, entryDirectory)
	securityErr := validateWindowsInheritableProtectedSecurity(reopened)
	closeErr := reopened.Close()
	if identityErr != nil || securityErr != nil || closeErr != nil {
		return "", errors.Join(identityErr, securityErr, closeErr)
	}
	if reopenedIdentity != targetIdentity {
		return "", ErrUnsafeTarget
	}
	if err := binding.verify(); err != nil {
		return "", fmt.Errorf("namespacelock: verify CI temp parent binding: %w", err)
	}
	return filepath.Join(parent, basename), nil
}

// ValidateCITestTemp reopens an existing helper-created Windows test root
// relative to the current token's LocalAppData Known Folder. It performs no
// repair or mutation and fails closed if the path, identity, kind, reparse
// state, parent binding, or exact inheritable protected DACL cannot be proved.
func ValidateCITestTemp(path string) error {
	if !filepath.IsAbs(path) {
		return ErrUnsafeTarget
	}
	path = filepath.Clean(path)
	parent, err := hostedCITestTempParent()
	if err != nil {
		return err
	}
	parent = filepath.Clean(parent)
	basename := filepath.Base(path)
	if !strings.EqualFold(filepath.Dir(path), parent) || validateCITestTempBasename(basename) != nil {
		return ErrUnsafeTarget
	}

	parentFile, identity, err := platformOpenParent(parent)
	if err != nil {
		return fmt.Errorf("namespacelock: open CI test temp parent: %w", err)
	}
	defer parentFile.Close()
	binding, err := platformBindParentPath(parent, identity)
	if err != nil {
		return fmt.Errorf("namespacelock: bind CI test temp parent: %w", err)
	}
	defer binding.close()

	open := func() (*os.File, platformIdentity, error) {
		file, _, openErr := ntOpenRelative(
			parentFile,
			basename,
			windows.FILE_GENERIC_READ|windows.READ_CONTROL,
			windows.FILE_SHARE_READ|windows.FILE_SHARE_WRITE,
			windows.FILE_OPEN,
			windows.FILE_DIRECTORY_FILE|windows.FILE_OPEN_REPARSE_POINT|windows.FILE_SYNCHRONOUS_IO_NONALERT,
			nil,
		)
		if openErr != nil {
			return nil, platformIdentity{}, normalizeWindowsError("open protected CI test temp", basename, openErr)
		}
		entryIdentity, identityErr := platformIdentityOf(file, entryDirectory)
		if identityErr != nil {
			_ = file.Close()
			return nil, platformIdentity{}, identityErr
		}
		if securityErr := validateWindowsInheritableProtectedSecurity(file); securityErr != nil {
			_ = file.Close()
			return nil, platformIdentity{}, fmt.Errorf("namespacelock: verify protected CI test temp: %w", securityErr)
		}
		return file, entryIdentity, nil
	}

	first, firstIdentity, err := open()
	if err != nil {
		return err
	}
	if err := binding.verify(); err != nil {
		_ = first.Close()
		return fmt.Errorf("namespacelock: verify CI test temp parent binding: %w", err)
	}
	second, secondIdentity, secondErr := open()
	firstCloseErr := first.Close()
	if secondErr != nil {
		return errors.Join(firstCloseErr, secondErr)
	}
	secondCloseErr := second.Close()
	if firstCloseErr != nil || secondCloseErr != nil || firstIdentity != secondIdentity {
		return errors.Join(firstCloseErr, secondCloseErr, ErrUnsafeTarget)
	}
	if err := binding.verify(); err != nil {
		return fmt.Errorf("namespacelock: verify CI test temp parent binding: %w", err)
	}
	return nil
}

// PrepareProtectedCITestChild creates one exact protected, inheritable
// directory directly below an already protected CI-test directory. Both the
// parent and child are opened relative to retained handles; an existing child
// is never repaired or reused.
func PrepareProtectedCITestChild(parent, basename string) (string, error) {
	if !filepath.IsAbs(parent) || validateCITestTempBasename(basename) != nil {
		return "", ErrUnsafeTarget
	}
	parent = filepath.Clean(parent)
	parentFile, identity, err := platformOpenParent(parent)
	if err != nil {
		return "", fmt.Errorf("namespacelock: open protected CI parent: %w", err)
	}
	defer parentFile.Close()
	if err := validateWindowsInheritableProtectedSecurity(parentFile); err != nil {
		return "", fmt.Errorf("namespacelock: verify protected CI parent: %w", err)
	}
	binding, err := platformBindParentPath(parent, identity)
	if err != nil {
		return "", fmt.Errorf("namespacelock: bind protected CI parent: %w", err)
	}
	defer binding.close()

	descriptor, err := protectedWindowsSecurityDescriptorWithInheritance(windows.SUB_CONTAINERS_AND_OBJECTS_INHERIT)
	if err != nil {
		return "", err
	}
	child, information, err := ntOpenRelative(
		parentFile,
		basename,
		windows.FILE_GENERIC_READ|windows.FILE_GENERIC_WRITE|windows.READ_CONTROL,
		windows.FILE_SHARE_READ|windows.FILE_SHARE_WRITE,
		windows.FILE_CREATE,
		windows.FILE_DIRECTORY_FILE|windows.FILE_OPEN_REPARSE_POINT|windows.FILE_SYNCHRONOUS_IO_NONALERT,
		descriptor,
	)
	if err != nil {
		return "", normalizeWindowsError("create protected CI child", basename, err)
	}
	defer child.Close()
	// FILE_CREATE must report FILE_CREATED. Treat any other information value
	// as an unsafe namespace even if the filesystem returned success.
	if information != 2 { // FILE_CREATED
		return "", ErrUnsafeTarget
	}
	createdIdentity, err := platformIdentityOf(child, entryDirectory)
	if err != nil {
		return "", err
	}
	if err := validateWindowsInheritableProtectedSecurity(child); err != nil {
		return "", fmt.Errorf("namespacelock: verify protected CI child: %w", err)
	}
	if err := binding.verify(); err != nil {
		return "", fmt.Errorf("namespacelock: verify protected CI parent binding: %w", err)
	}
	path := filepath.Join(parent, basename)
	if err := validateProtectedCITestChild(parentFile, binding, basename, createdIdentity); err != nil {
		return "", err
	}
	return path, nil
}

// ValidateProtectedCITestChild proves that path is one direct child of an
// exact protected, inheritable parent and that two handle-relative reopens
// retain the same directory identity and exact DACL. It performs no mutation.
func ValidateProtectedCITestChild(parent, path string) error {
	if !filepath.IsAbs(parent) || !filepath.IsAbs(path) {
		return ErrUnsafeTarget
	}
	parent = filepath.Clean(parent)
	path = filepath.Clean(path)
	basename := filepath.Base(path)
	if !strings.EqualFold(filepath.Dir(path), parent) || validateCITestTempBasename(basename) != nil {
		return ErrUnsafeTarget
	}
	parentFile, identity, err := platformOpenParent(parent)
	if err != nil {
		return fmt.Errorf("namespacelock: open protected CI parent: %w", err)
	}
	defer parentFile.Close()
	if err := validateWindowsInheritableProtectedSecurity(parentFile); err != nil {
		return fmt.Errorf("namespacelock: verify protected CI parent: %w", err)
	}
	binding, err := platformBindParentPath(parent, identity)
	if err != nil {
		return fmt.Errorf("namespacelock: bind protected CI parent: %w", err)
	}
	defer binding.close()
	return validateProtectedCITestChild(parentFile, binding, basename, platformIdentity{})
}

func validateProtectedCITestChild(parent *os.File, binding parentPathBinding, basename string, expected platformIdentity) error {
	open := func() (*os.File, platformIdentity, error) {
		file, _, err := ntOpenRelative(
			parent,
			basename,
			windows.FILE_GENERIC_READ|windows.READ_CONTROL,
			windows.FILE_SHARE_READ|windows.FILE_SHARE_WRITE,
			windows.FILE_OPEN,
			windows.FILE_DIRECTORY_FILE|windows.FILE_OPEN_REPARSE_POINT|windows.FILE_SYNCHRONOUS_IO_NONALERT,
			nil,
		)
		if err != nil {
			return nil, platformIdentity{}, normalizeWindowsError("open protected CI child", basename, err)
		}
		identity, identityErr := platformIdentityOf(file, entryDirectory)
		if identityErr != nil {
			_ = file.Close()
			return nil, platformIdentity{}, identityErr
		}
		if securityErr := validateWindowsInheritableProtectedSecurity(file); securityErr != nil {
			_ = file.Close()
			return nil, platformIdentity{}, fmt.Errorf("namespacelock: verify protected CI child: %w", securityErr)
		}
		return file, identity, nil
	}
	first, firstIdentity, err := open()
	if err != nil {
		return err
	}
	if expected != (platformIdentity{}) && firstIdentity != expected {
		_ = first.Close()
		return ErrUnsafeTarget
	}
	if err := binding.verify(); err != nil {
		_ = first.Close()
		return fmt.Errorf("namespacelock: verify protected CI parent binding: %w", err)
	}
	second, secondIdentity, secondErr := open()
	firstCloseErr := first.Close()
	if secondErr != nil {
		return errors.Join(firstCloseErr, secondErr)
	}
	secondCloseErr := second.Close()
	if firstCloseErr != nil || secondCloseErr != nil || firstIdentity != secondIdentity {
		return errors.Join(firstCloseErr, secondCloseErr, ErrUnsafeTarget)
	}
	if err := binding.verify(); err != nil {
		return fmt.Errorf("namespacelock: verify protected CI parent binding: %w", err)
	}
	return nil
}

func validateCITestTempBasename(basename string) error {
	if filepath.Base(basename) != basename || basename == "" || basename == "." ||
		len(basename) > 128 || strings.Trim(basename, " .") != basename {
		return ErrUnsafeTarget
	}
	for _, character := range basename {
		if character >= 'a' && character <= 'z' || character >= 'A' && character <= 'Z' ||
			character >= '0' && character <= '9' || strings.ContainsRune("._-", character) {
			continue
		}
		return ErrUnsafeTarget
	}
	return nil
}
