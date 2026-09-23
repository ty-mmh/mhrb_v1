//go:build windows

package fssecure

import (
	"errors"
	"os"
	"runtime"
	"unsafe"

	"golang.org/x/sys/windows"
)

func CurrentSecurityPolicy() (SecurityPolicy, error) {
	token := windows.GetCurrentProcessToken()
	user, err := token.GetTokenUser()
	if err != nil {
		return SecurityPolicy{}, err
	}
	return SecurityPolicy{ownerSID: user.User.Sid.String()}, nil
}

func identityFromHandle(file *os.File, kind EntryKind) (Identity, error) {
	if file == nil {
		return Identity{}, ErrIdentityChanged
	}
	var information windows.ByHandleFileInformation
	if err := windows.GetFileInformationByHandle(windows.Handle(file.Fd()), &information); err != nil {
		return Identity{}, unsupportedFilesystem("stable Windows file identity unavailable", err)
	}
	return identityFromWindowsInformation(information, kind)
}

func identityFromWindowsInformation(information windows.ByHandleFileInformation, kind EntryKind) (Identity, error) {
	if information.VolumeSerialNumber == 0 || information.FileIndexHigh == 0 && information.FileIndexLow == 0 {
		return Identity{}, unsupportedFilesystem("stable Windows file identity unavailable", nil)
	}
	return Identity{volume: uint64(information.VolumeSerialNumber), first: uint64(information.FileIndexHigh), second: uint64(information.FileIndexLow), kind: kind}, nil
}

func validateWindowsEntryKind(info os.FileInfo, kind EntryKind, policy SecurityPolicy) error {
	if info == nil || info.Mode()&os.ModeSymlink != 0 {
		return WithReason(ReasonReparseOrSymlink, ErrUnsafeFilesystem)
	}
	if kind == KindRegular && !info.Mode().IsRegular() || kind == KindDirectory && !info.IsDir() {
		return WithReason(ReasonUnsafeMode, ErrUnsafeFilesystem)
	}
	if !policy.valid() {
		return WithReason(ReasonOwnerMismatch, ErrUnsafeFilesystem)
	}
	return nil
}

const windowsFileAllAccess = windows.ACCESS_MASK(windows.STANDARD_RIGHTS_REQUIRED | windows.SYNCHRONIZE | 0x1ff)

const (
	windowsSystemSID         = "S-1-5-18"
	windowsAdministratorsSID = "S-1-5-32-544"
)

func requiredWindowsSIDs(policy SecurityPolicy) (map[string]*windows.SID, error) {
	if !policy.valid() || policy.ownerSID == "" {
		return nil, WithReason(ReasonOwnerMismatch, ErrUnsafeFilesystem)
	}
	ids := []string{policy.ownerSID, windowsSystemSID, windowsAdministratorsSID}
	result := make(map[string]*windows.SID, len(ids))
	for _, id := range ids {
		sid, err := windows.StringToSid(id)
		if err != nil {
			return nil, WithReason(ReasonOwnerMismatch, errors.Join(ErrUnsafeFilesystem, err))
		}
		result[id] = sid
	}
	return result, nil
}

func buildProtectedWindowsACL(policy SecurityPolicy) (*windows.ACL, error) {
	sids, err := requiredWindowsSIDs(policy)
	if err != nil {
		return nil, err
	}
	ordered := []string{policy.ownerSID, windowsSystemSID, windowsAdministratorsSID}
	entries := make([]windows.EXPLICIT_ACCESS, 0, len(ordered))
	var pinner runtime.Pinner
	defer pinner.Unpin()
	for index, id := range ordered {
		sid := sids[id]
		pinner.Pin(sid)
		trusteeType := windows.TRUSTEE_TYPE(windows.TRUSTEE_IS_GROUP)
		if index == 0 {
			trusteeType = windows.TRUSTEE_IS_USER
		}
		entries = append(entries, windows.EXPLICIT_ACCESS{
			AccessPermissions: windowsFileAllAccess,
			AccessMode:        windows.GRANT_ACCESS,
			Inheritance:       windows.NO_INHERITANCE,
			Trustee: windows.TRUSTEE{
				TrusteeForm:  windows.TRUSTEE_IS_SID,
				TrusteeType:  trusteeType,
				TrusteeValue: windows.TrusteeValueFromSID(sid),
			},
		})
	}
	return windows.ACLFromEntries(entries, nil)
}

func buildWindowsSecurityDescriptor(policy SecurityPolicy) (*windows.SECURITY_DESCRIPTOR, error) {
	owner, err := windows.StringToSid(policy.ownerSID)
	if err != nil {
		return nil, WithReason(ReasonOwnerMismatch, errors.Join(ErrUnsafeFilesystem, err))
	}
	acl, err := buildProtectedWindowsACL(policy)
	if err != nil {
		return nil, err
	}
	sd, err := windows.NewSecurityDescriptor()
	if err != nil {
		return nil, WithReason(ReasonUnsafeACL, errors.Join(ErrUnsafeFilesystem, err))
	}
	if err := sd.SetOwner(owner, false); err != nil {
		return nil, WithReason(ReasonOwnerMismatch, errors.Join(ErrUnsafeFilesystem, err))
	}
	if err := sd.SetDACL(acl, true, false); err != nil {
		return nil, WithReason(ReasonUnsafeACL, errors.Join(ErrUnsafeFilesystem, err))
	}
	if err := sd.SetControl(windows.SE_DACL_PROTECTED, windows.SE_DACL_PROTECTED); err != nil {
		return nil, WithReason(ReasonUnsafeACL, errors.Join(ErrUnsafeFilesystem, err))
	}
	return sd, nil
}

func validateWindowsSecurityDescriptor(sd *windows.SECURITY_DESCRIPTOR, policy SecurityPolicy) error {
	if sd == nil {
		return WithReason(ReasonUnsafeACL, ErrUnsafeFilesystem)
	}
	owner, _, err := sd.Owner()
	if err != nil || owner == nil || owner.String() != policy.ownerSID {
		return WithReason(ReasonOwnerMismatch, errors.Join(ErrUnsafeFilesystem, err))
	}
	control, _, err := sd.Control()
	if err != nil {
		return WithReason(ReasonUnsafeACL, errors.Join(ErrUnsafeFilesystem, err))
	}
	if control&windows.SE_DACL_PRESENT == 0 || control&windows.SE_DACL_PROTECTED == 0 || control&windows.SE_DACL_DEFAULTED != 0 {
		return WithReason(ReasonUnsafeACL, ErrUnsafeFilesystem)
	}
	dacl, defaulted, err := sd.DACL()
	if err != nil || dacl == nil || defaulted {
		return WithReason(ReasonUnsafeACL, errors.Join(ErrUnsafeFilesystem, err))
	}
	expected, err := requiredWindowsSIDs(policy)
	if err != nil {
		return err
	}
	if int(dacl.AceCount) != len(expected) {
		return WithReason(ReasonUnsafeACL, ErrUnsafeFilesystem)
	}
	seen := make(map[string]struct{}, len(expected))
	for index := uint32(0); index < uint32(dacl.AceCount); index++ {
		var ace *windows.ACCESS_ALLOWED_ACE
		if err := windows.GetAce(dacl, index, &ace); err != nil || ace == nil {
			return WithReason(ReasonUnsafeACL, errors.Join(ErrUnsafeFilesystem, err))
		}
		if ace.Header.AceType != windows.ACCESS_ALLOWED_ACE_TYPE || ace.Header.AceFlags != 0 || ace.Mask != windowsFileAllAccess {
			return WithReason(ReasonUnsafeACL, ErrUnsafeFilesystem)
		}
		aceSID := (*windows.SID)(unsafe.Pointer(&ace.SidStart))
		if !aceSID.IsValid() {
			return WithReason(ReasonUnsafeACL, ErrUnsafeFilesystem)
		}
		id := aceSID.String()
		if _, ok := expected[id]; !ok {
			return WithReason(ReasonUnsafeACL, ErrUnsafeFilesystem)
		}
		if _, duplicate := seen[id]; duplicate {
			return WithReason(ReasonUnsafeACL, ErrUnsafeFilesystem)
		}
		seen[id] = struct{}{}
	}
	if len(seen) != len(expected) {
		return WithReason(ReasonUnsafeACL, ErrUnsafeFilesystem)
	}
	return nil
}

func validateOpenedSecurity(file *os.File, kind EntryKind, policy SecurityPolicy) error {
	if file == nil {
		return ErrIdentityChanged
	}
	var information windows.ByHandleFileInformation
	if err := windows.GetFileInformationByHandle(windows.Handle(file.Fd()), &information); err != nil {
		return unsupportedFilesystem("Windows handle metadata unavailable", err)
	}
	if information.FileAttributes&windows.FILE_ATTRIBUTE_REPARSE_POINT != 0 {
		return WithReason(ReasonReparseOrSymlink, ErrUnsafeFilesystem)
	}
	if err := validateWindowsEntryKind(fileInfo(file), kind, policy); err != nil {
		return err
	}
	sd, err := windows.GetSecurityInfo(windows.Handle(file.Fd()), windows.SE_FILE_OBJECT,
		windows.OWNER_SECURITY_INFORMATION|windows.DACL_SECURITY_INFORMATION)
	if err != nil || sd == nil {
		return WithReason(ReasonUnsafeACL, errors.Join(ErrUnsafeFilesystem, err))
	}
	return validateWindowsSecurityDescriptor(sd, policy)
}

func validateDirectoryHandle(file *os.File, expected Identity, policy SecurityPolicy) error {
	if err := validateOpenedSecurity(file, KindDirectory, policy); err != nil {
		return err
	}
	current, err := identityFromHandle(file, KindDirectory)
	if err != nil || !expected.Equal(current) {
		return identityChanged(ReasonDirectoryIdentityChanged, err)
	}
	return nil
}

func syncDirectory(file *os.File) error {
	if file == nil {
		return ErrIdentityChanged
	}
	if err := file.Sync(); err != nil {
		if errors.Is(err, windows.ERROR_NOT_SUPPORTED) || errors.Is(err, windows.ERROR_INVALID_FUNCTION) || errors.Is(err, windows.ERROR_NOT_SAME_DEVICE) {
			return unsupportedFilesystem("directory flush unavailable", err)
		}
		return err
	}
	return nil
}
