//go:build windows

package namespacelock

import (
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"syscall"
	"unsafe"

	"golang.org/x/sys/windows"
)

type platformIdentity struct {
	volume uint32
	high   uint32
	low    uint32
	kind   entryKind
}

type windowsParentPathBinding struct {
	files      []*os.File
	components []string
	identities []platformIdentity
}

func platformBindParentPath(path string, expected platformIdentity) (parentPathBinding, error) {
	clean := filepath.Clean(path)
	volume := filepath.VolumeName(clean)
	if len(volume) != 2 || volume[1] != ':' || strings.HasPrefix(volume, `\\`) {
		return nil, ErrUnsupported
	}
	rootPath := volume + `\`
	rootName, err := windows.UTF16PtrFromString(rootPath)
	if err != nil {
		return nil, err
	}
	rootHandle, err := windows.CreateFile(
		rootName,
		windows.FILE_GENERIC_READ|windows.READ_CONTROL,
		windows.FILE_SHARE_READ|windows.FILE_SHARE_WRITE|windows.FILE_SHARE_DELETE,
		nil,
		windows.OPEN_EXISTING,
		windows.FILE_FLAG_BACKUP_SEMANTICS|windows.FILE_FLAG_OPEN_REPARSE_POINT,
		0,
	)
	if err != nil {
		return nil, errors.Join(ErrUnsafeTarget, err)
	}
	root := os.NewFile(uintptr(rootHandle), rootPath)
	if root == nil {
		_ = windows.CloseHandle(rootHandle)
		return nil, ErrUnsafeTarget
	}
	rootIdentity, err := platformIdentityOf(root, entryDirectory)
	if err != nil {
		_ = root.Close()
		return nil, err
	}
	binding := &windowsParentPathBinding{files: []*os.File{root}, identities: []platformIdentity{rootIdentity}}
	components := strings.FieldsFunc(strings.TrimPrefix(clean[len(volume):], `\`), func(r rune) bool {
		return r == '\\' || r == '/'
	})
	current := root
	for _, component := range components {
		child, _, openErr := ntOpenRelative(
			current,
			component,
			windows.FILE_GENERIC_READ|windows.READ_CONTROL,
			windows.FILE_SHARE_READ|windows.FILE_SHARE_WRITE|windows.FILE_SHARE_DELETE,
			windows.FILE_OPEN,
			windows.FILE_DIRECTORY_FILE|windows.FILE_OPEN_REPARSE_POINT|windows.FILE_SYNCHRONOUS_IO_NONALERT,
			nil,
		)
		if openErr != nil {
			_ = binding.close()
			return nil, normalizeWindowsError("bind parent component", component, openErr)
		}
		identity, identityErr := platformIdentityOf(child, entryDirectory)
		if identityErr != nil {
			_ = child.Close()
			_ = binding.close()
			return nil, identityErr
		}
		binding.files = append(binding.files, child)
		binding.components = append(binding.components, component)
		binding.identities = append(binding.identities, identity)
		current = child
	}
	if len(binding.identities) == 0 || binding.identities[len(binding.identities)-1] != expected {
		_ = binding.close()
		return nil, ErrUnsafeTarget
	}
	return binding, nil
}

func (binding *windowsParentPathBinding) verify() error {
	if binding == nil || len(binding.files) == 0 || len(binding.identities) != len(binding.files) || len(binding.components)+1 != len(binding.files) {
		return ErrUnsafeTarget
	}
	for index, file := range binding.files {
		if err := platformVerifyIdentity(file, binding.identities[index], entryDirectory); err != nil {
			return err
		}
		if index == 0 {
			continue
		}
		current, _, err := ntOpenRelative(
			binding.files[index-1],
			binding.components[index-1],
			windows.FILE_GENERIC_READ|windows.READ_CONTROL,
			windows.FILE_SHARE_READ|windows.FILE_SHARE_WRITE|windows.FILE_SHARE_DELETE,
			windows.FILE_OPEN,
			windows.FILE_DIRECTORY_FILE|windows.FILE_OPEN_REPARSE_POINT|windows.FILE_SYNCHRONOUS_IO_NONALERT,
			nil,
		)
		if err != nil {
			return normalizeWindowsError("verify parent component", binding.components[index-1], err)
		}
		actual, identityErr := platformIdentityOf(current, entryDirectory)
		closeErr := current.Close()
		if identityErr != nil || closeErr != nil {
			return errors.Join(identityErr, closeErr)
		}
		if actual != binding.identities[index] {
			return ErrUnsafeTarget
		}
	}
	return nil
}

func (binding *windowsParentPathBinding) close() error {
	if binding == nil {
		return nil
	}
	var result error
	for index := len(binding.files) - 1; index >= 0; index-- {
		result = errors.Join(result, binding.files[index].Close())
	}
	binding.files = nil
	binding.components = nil
	binding.identities = nil
	return result
}

func platformOpenParent(path string) (*os.File, platformIdentity, error) {
	name, err := windows.UTF16PtrFromString(path)
	if err != nil {
		return nil, platformIdentity{}, err
	}
	handle, err := windows.CreateFile(
		name,
		windows.FILE_GENERIC_READ|windows.FILE_GENERIC_WRITE|windows.READ_CONTROL,
		windows.FILE_SHARE_READ|windows.FILE_SHARE_WRITE,
		nil,
		windows.OPEN_EXISTING,
		windows.FILE_FLAG_BACKUP_SEMANTICS|windows.FILE_FLAG_OPEN_REPARSE_POINT,
		0,
	)
	if err != nil {
		return nil, platformIdentity{}, &os.PathError{Op: "open canonical parent", Path: path, Err: err}
	}
	file := os.NewFile(uintptr(handle), path)
	if file == nil {
		_ = windows.CloseHandle(handle)
		return nil, platformIdentity{}, ErrUnsafeTarget
	}
	identity, err := platformIdentityOf(file, entryDirectory)
	if err != nil {
		_ = file.Close()
		return nil, platformIdentity{}, err
	}
	if err := validateWindowsObservedSecurity(file); err != nil {
		_ = file.Close()
		return nil, platformIdentity{}, err
	}
	return file, identity, nil
}

func platformOpenRegular(parent *os.File, name string, _ os.FileMode) (*os.File, error) {
	descriptor, err := protectedWindowsSecurityDescriptor()
	if err != nil {
		return nil, err
	}
	file, _, err := ntOpenRelative(
		parent,
		name,
		windows.FILE_GENERIC_READ|windows.FILE_GENERIC_WRITE|windows.READ_CONTROL,
		windows.FILE_SHARE_READ|windows.FILE_SHARE_WRITE,
		windows.FILE_OPEN_IF,
		windows.FILE_NON_DIRECTORY_FILE|windows.FILE_OPEN_REPARSE_POINT|windows.FILE_SYNCHRONOUS_IO_NONALERT,
		descriptor,
	)
	if err != nil {
		return nil, normalizeWindowsError("open regular entry", name, err)
	}
	if _, err := platformIdentityOf(file, entryRegular); err != nil {
		_ = file.Close()
		return nil, err
	}
	if err := validateWindowsProtectedSecurity(file); err != nil {
		_ = file.Close()
		return nil, err
	}
	return file, nil
}

func platformOpenRegularRead(parent *os.File, name string) (*os.File, error) {
	file, _, err := ntOpenRelative(
		parent,
		name,
		windows.FILE_GENERIC_READ|windows.READ_CONTROL,
		windows.FILE_SHARE_READ,
		windows.FILE_OPEN,
		windows.FILE_NON_DIRECTORY_FILE|windows.FILE_OPEN_REPARSE_POINT|windows.FILE_SYNCHRONOUS_IO_NONALERT,
		nil,
	)
	if err != nil {
		return nil, normalizeWindowsError("open diagnostic regular entry", name, err)
	}
	if _, err := platformIdentityOf(file, entryRegular); err != nil {
		_ = file.Close()
		return nil, err
	}
	if err := validateWindowsProtectedSecurity(file); err != nil {
		_ = file.Close()
		return nil, err
	}
	return file, nil
}

func platformOpenObservedRegularRead(parent *os.File, name string) (*os.File, error) {
	file, _, err := ntOpenRelative(
		parent,
		name,
		windows.FILE_GENERIC_READ|windows.READ_CONTROL,
		windows.FILE_SHARE_READ,
		windows.FILE_OPEN,
		windows.FILE_NON_DIRECTORY_FILE|windows.FILE_OPEN_REPARSE_POINT|windows.FILE_SYNCHRONOUS_IO_NONALERT,
		nil,
	)
	if err != nil {
		return nil, normalizeWindowsError("open observed regular entry", name, err)
	}
	if _, err := platformIdentityOf(file, entryRegular); err != nil {
		_ = file.Close()
		return nil, err
	}
	if err := validateWindowsObservedSecurity(file); err != nil {
		_ = file.Close()
		return nil, err
	}
	return file, nil
}

func platformOpenTarget(parent *os.File, name string, create bool, _ os.FileMode) (*os.File, bool, error) {
	disposition := uint32(windows.FILE_OPEN)
	var descriptor *windows.SECURITY_DESCRIPTOR
	if create {
		disposition = windows.FILE_OPEN_IF
		var err error
		descriptor, err = protectedWindowsSecurityDescriptor()
		if err != nil {
			return nil, false, err
		}
	}
	file, information, err := ntOpenRelative(
		parent,
		name,
		windows.FILE_GENERIC_READ|windows.FILE_GENERIC_WRITE|windows.READ_CONTROL,
		windows.FILE_SHARE_READ|windows.FILE_SHARE_WRITE,
		disposition,
		windows.FILE_DIRECTORY_FILE|windows.FILE_OPEN_REPARSE_POINT|windows.FILE_SYNCHRONOUS_IO_NONALERT,
		descriptor,
	)
	if err != nil {
		if !create && isWindowsNotExist(err) {
			return nil, false, nil
		}
		return nil, false, normalizeWindowsError("open target", name, err)
	}
	if _, err := platformIdentityOf(file, entryDirectory); err != nil {
		_ = file.Close()
		return nil, false, err
	}
	if securityErr := validateWindowsProtectedSecurity(file); securityErr != nil {
		_ = file.Close()
		return nil, false, securityErr
	}
	return file, information == 1, nil // FILE_OPENED
}

// platformOpenObservedTarget is the read-only namespace-observation form. It
// proves kind, no-reparse identity, and the observed ACL policy without
// accepting the handle as authority for managed-target operations.
func platformOpenObservedTarget(parent *os.File, name string) (*os.File, bool, error) {
	file, _, err := ntOpenRelative(
		parent,
		name,
		windows.FILE_GENERIC_READ|windows.READ_CONTROL,
		windows.FILE_SHARE_READ|windows.FILE_SHARE_WRITE,
		windows.FILE_OPEN,
		windows.FILE_DIRECTORY_FILE|windows.FILE_OPEN_REPARSE_POINT|windows.FILE_SYNCHRONOUS_IO_NONALERT,
		nil,
	)
	if err != nil {
		if isWindowsNotExist(err) {
			return nil, false, nil
		}
		return nil, false, normalizeWindowsError("observe target", name, err)
	}
	if _, err := platformIdentityOf(file, entryDirectory); err != nil {
		_ = file.Close()
		return nil, false, err
	}
	if err := validateWindowsObservedSecurity(file); err != nil {
		_ = file.Close()
		return nil, false, err
	}
	return file, true, nil
}

func ntOpenRelative(parent *os.File, name string, access, share, disposition, options uint32, descriptor *windows.SECURITY_DESCRIPTOR) (*os.File, uintptr, error) {
	if parent == nil {
		return nil, 0, ErrUnsafeTarget
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
	attributes := &windows.OBJECT_ATTRIBUTES{
		Length:             uint32(unsafe.Sizeof(windows.OBJECT_ATTRIBUTES{})),
		RootDirectory:      windows.Handle(parent.Fd()),
		ObjectName:         objectName,
		Attributes:         windows.OBJ_CASE_INSENSITIVE | windows.OBJ_DONT_REPARSE,
		SecurityDescriptor: selfRelative,
	}
	var handle windows.Handle
	var status windows.IO_STATUS_BLOCK
	var allocation int64
	err = windows.NtCreateFile(&handle, access, attributes, &status, &allocation, 0, share, disposition, options, 0, 0)
	runtime.KeepAlive(descriptor)
	runtime.KeepAlive(selfRelative)
	runtime.KeepAlive(objectName)
	if err != nil {
		return nil, 0, err
	}
	file := os.NewFile(uintptr(handle), name)
	if file == nil {
		_ = windows.CloseHandle(handle)
		return nil, 0, ErrUnsafeTarget
	}
	return file, status.Information, nil
}

const windowsFileAllAccess = windows.ACCESS_MASK(windows.STANDARD_RIGHTS_REQUIRED | windows.SYNCHRONIZE | 0x1ff)

const (
	windowsSystemSID         = "S-1-5-18"
	windowsAdministratorsSID = "S-1-5-32-544"
)

func currentWindowsUserSID() (*windows.SID, error) {
	user, err := windows.GetCurrentProcessToken().GetTokenUser()
	if err != nil || user == nil || user.User.Sid == nil {
		return nil, errors.Join(ErrUnsafeTarget, err)
	}
	// GetTokenUser owns a variable-sized backing allocation. Canonicalize
	// through its string form so security descriptors retain a durable SID
	// after the TOKEN_USER wrapper becomes unreachable.
	durable, err := windows.StringToSid(user.User.Sid.String())
	if err != nil {
		return nil, errors.Join(ErrUnsafeTarget, err)
	}
	return durable, nil
}

func protectedWindowsSecurityDescriptor() (*windows.SECURITY_DESCRIPTOR, error) {
	return protectedWindowsSecurityDescriptorWithInheritance(windows.NO_INHERITANCE)
}

func protectedWindowsSecurityDescriptorWithInheritance(inheritance uint32) (*windows.SECURITY_DESCRIPTOR, error) {
	user, err := currentWindowsUserSID()
	if err != nil {
		return nil, err
	}
	ids := []*windows.SID{user}
	for _, id := range []string{windowsSystemSID, windowsAdministratorsSID} {
		sid, sidErr := windows.StringToSid(id)
		if sidErr != nil {
			return nil, errors.Join(ErrUnsafeTarget, sidErr)
		}
		ids = append(ids, sid)
	}
	entries := make([]windows.EXPLICIT_ACCESS, 0, len(ids))
	var pinner runtime.Pinner
	defer pinner.Unpin()
	for index, sid := range ids {
		pinner.Pin(sid)
		trusteeType := windows.TRUSTEE_TYPE(windows.TRUSTEE_IS_GROUP)
		if index == 0 {
			trusteeType = windows.TRUSTEE_IS_USER
		}
		entries = append(entries, windows.EXPLICIT_ACCESS{
			AccessPermissions: windowsFileAllAccess,
			AccessMode:        windows.GRANT_ACCESS,
			Inheritance:       inheritance,
			Trustee: windows.TRUSTEE{
				TrusteeForm:  windows.TRUSTEE_IS_SID,
				TrusteeType:  trusteeType,
				TrusteeValue: windows.TrusteeValueFromSID(sid),
			},
		})
	}
	acl, err := windows.ACLFromEntries(entries, nil)
	if err != nil {
		return nil, errors.Join(ErrUnsafeTarget, err)
	}
	descriptor, err := windows.NewSecurityDescriptor()
	if err != nil {
		return nil, errors.Join(ErrUnsafeTarget, err)
	}
	if err := descriptor.SetOwner(user, false); err != nil {
		return nil, errors.Join(ErrUnsafeTarget, err)
	}
	if err := descriptor.SetDACL(acl, true, false); err != nil {
		return nil, errors.Join(ErrUnsafeTarget, err)
	}
	if err := descriptor.SetControl(windows.SE_DACL_PROTECTED, windows.SE_DACL_PROTECTED); err != nil {
		return nil, errors.Join(ErrUnsafeTarget, err)
	}
	return descriptor, nil
}

func validateWindowsProtectedSecurity(file *os.File) error {
	return validateWindowsExactProtectedSecurity(file, windows.NO_INHERITANCE)
}

func validateWindowsInheritableProtectedSecurity(file *os.File) error {
	return validateWindowsExactProtectedSecurity(file, windows.SUB_CONTAINERS_AND_OBJECTS_INHERIT)
}

type windowsSecurityView struct {
	descriptor    *windows.SECURITY_DESCRIPTOR
	owner         *windows.SID
	control       windows.SECURITY_DESCRIPTOR_CONTROL
	dacl          *windows.ACL
	daclDefaulted bool
}

type windowsBasicACE struct {
	typeID uint8
	flags  uint8
	mask   windows.ACCESS_MASK
	sid    string
}

func validateWindowsExactProtectedSecurity(file *os.File, inheritance uint32) error {
	if file == nil {
		return ErrUnsafeTarget
	}
	view, err := readWindowsSecurityView(file)
	if err != nil {
		return err
	}
	return validateWindowsExactProtectedSecurityView(view, inheritance)
}

func validateWindowsExactProtectedSecurityView(view windowsSecurityView, inheritance uint32) error {
	user, userErr := currentWindowsUserSID()
	if userErr != nil || view.owner == nil || !view.owner.IsValid() || !view.owner.Equals(user) {
		return errors.Join(ErrUnsafeTarget, userErr)
	}
	allowed, err := requiredWindowsSIDs(user)
	if err != nil {
		return err
	}
	if view.control&windows.SE_DACL_PRESENT == 0 || view.control&windows.SE_DACL_PROTECTED == 0 ||
		view.control&windows.SE_DACL_DEFAULTED != 0 || view.dacl == nil || view.daclDefaulted {
		return ErrUnsafeTarget
	}
	aces, err := readWindowsBasicACEs(view.dacl)
	if err != nil || len(aces) != len(allowed) {
		return errors.Join(ErrUnsafeTarget, err)
	}
	seen := make(map[string]struct{}, len(allowed))
	for _, ace := range aces {
		if ace.typeID != windows.ACCESS_ALLOWED_ACE_TYPE {
			return ErrUnsafeTarget
		}
		_, trusted := allowed[ace.sid]
		if !trusted || uint32(ace.flags) != inheritance || ace.mask != windowsFileAllAccess {
			return ErrUnsafeTarget
		}
		if _, duplicate := seen[ace.sid]; duplicate {
			return ErrUnsafeTarget
		}
		seen[ace.sid] = struct{}{}
	}
	if len(seen) != len(allowed) {
		return ErrUnsafeTarget
	}
	runtime.KeepAlive(view.descriptor)
	return nil
}

func validateWindowsObservedSecurity(file *os.File) error {
	if file == nil {
		return ErrUnsafeTarget
	}
	view, err := readWindowsSecurityView(file)
	if err != nil {
		return err
	}
	return validateWindowsObservedSecurityView(view)
}

func readWindowsSecurityView(file *os.File) (windowsSecurityView, error) {
	if file == nil {
		return windowsSecurityView{}, ErrUnsafeTarget
	}
	descriptor, err := windows.GetSecurityInfo(
		windows.Handle(file.Fd()),
		windows.SE_FILE_OBJECT,
		windows.OWNER_SECURITY_INFORMATION|windows.DACL_SECURITY_INFORMATION,
	)
	if err != nil || descriptor == nil {
		return windowsSecurityView{}, errors.Join(ErrUnsafeTarget, err)
	}
	return windowsSecurityViewFromDescriptor(descriptor)
}

func windowsSecurityViewFromDescriptor(descriptor *windows.SECURITY_DESCRIPTOR) (windowsSecurityView, error) {
	if descriptor == nil {
		return windowsSecurityView{}, ErrUnsafeTarget
	}
	owner, _, err := descriptor.Owner()
	if err != nil || owner == nil {
		return windowsSecurityView{}, errors.Join(ErrUnsafeTarget, err)
	}
	control, _, err := descriptor.Control()
	if err != nil {
		return windowsSecurityView{}, errors.Join(ErrUnsafeTarget, err)
	}
	dacl, defaulted, err := descriptor.DACL()
	if err != nil {
		return windowsSecurityView{}, errors.Join(ErrUnsafeTarget, err)
	}
	return windowsSecurityView{descriptor: descriptor, owner: owner, control: control, dacl: dacl, daclDefaulted: defaulted}, nil
}

func validateWindowsObservedSecurityView(view windowsSecurityView) error {
	user, err := currentWindowsUserSID()
	if err != nil {
		return err
	}
	trusted, err := requiredWindowsSIDs(user)
	if err != nil {
		return err
	}
	if view.owner == nil || !view.owner.IsValid() {
		return ErrUnsafeTarget
	}
	if _, ok := trusted[view.owner.String()]; !ok {
		return ErrUnsafeTarget
	}
	if view.control&windows.SE_DACL_PRESENT == 0 || view.control&windows.SE_DACL_DEFAULTED != 0 || view.dacl == nil || view.daclDefaulted {
		return ErrUnsafeTarget
	}
	aces, err := readWindowsBasicACEs(view.dacl)
	if err != nil {
		return err
	}
	untrustedAllow := make(map[string]windows.ACCESS_MASK)
	for _, ace := range aces {
		if ace.typeID == windows.ACCESS_DENIED_ACE_TYPE {
			continue
		}
		if _, ok := trusted[ace.sid]; !ok {
			untrustedAllow[ace.sid] |= mapWindowsDirectoryGenericMask(ace.mask)
		}
	}
	const safe = windows.ACCESS_MASK(
		windows.FILE_LIST_DIRECTORY |
			windows.FILE_READ_EA |
			windows.FILE_TRAVERSE |
			windows.FILE_READ_ATTRIBUTES |
			windows.READ_CONTROL |
			windows.SYNCHRONIZE,
	)
	for _, mask := range untrustedAllow {
		if mask & ^safe != 0 {
			return ErrUnsafeTarget
		}
	}
	runtime.KeepAlive(view.descriptor)
	return nil
}

func readWindowsBasicACEs(dacl *windows.ACL) ([]windowsBasicACE, error) {
	if dacl == nil {
		return nil, ErrUnsafeTarget
	}
	type aclHeader struct {
		revision  byte
		reserved  byte
		size      uint16
		count     uint16
		reserved2 uint16
	}
	header := (*aclHeader)(unsafe.Pointer(dacl))
	const aclHeaderSize = uintptr(8)
	if header.revision != 2 && header.revision != 4 || header.size < uint16(aclHeaderSize) ||
		uint32(header.count) > uint32(header.size-uint16(aclHeaderSize))/16 {
		return nil, ErrUnsafeTarget
	}
	base := uintptr(unsafe.Pointer(dacl))
	end := base + uintptr(header.size)
	if end < base {
		return nil, ErrUnsafeTarget
	}
	result := make([]windowsBasicACE, 0, header.count)
	for index := uint32(0); index < uint32(header.count); index++ {
		var pointer *windows.ACCESS_ALLOWED_ACE
		if err := windows.GetAce(dacl, index, &pointer); err != nil || pointer == nil {
			return nil, errors.Join(ErrUnsafeTarget, err)
		}
		start := uintptr(unsafe.Pointer(pointer))
		if start < base+aclHeaderSize || start > end || end-start < unsafe.Sizeof(windows.ACE_HEADER{}) {
			return nil, ErrUnsafeTarget
		}
		aceHeader := *(*windows.ACE_HEADER)(unsafe.Pointer(pointer))
		size := uintptr(aceHeader.AceSize)
		const sidOffset = uintptr(8)
		const minimumSIDSize = uintptr(8)
		if size < sidOffset+minimumSIDSize || size > end-start {
			return nil, ErrUnsafeTarget
		}
		if aceHeader.AceType != windows.ACCESS_ALLOWED_ACE_TYPE && aceHeader.AceType != windows.ACCESS_DENIED_ACE_TYPE {
			return nil, ErrUnsafeTarget
		}
		if aceHeader.AceFlags & ^uint8(windows.VALID_INHERIT_FLAGS) != 0 ||
			aceHeader.AceFlags&(windows.INHERIT_ONLY_ACE|windows.NO_PROPAGATE_INHERIT_ACE) != 0 &&
				aceHeader.AceFlags&(windows.OBJECT_INHERIT_ACE|windows.CONTAINER_INHERIT_ACE) == 0 {
			return nil, ErrUnsafeTarget
		}
		raw := unsafe.Slice((*byte)(unsafe.Pointer(pointer)), int(size))
		sidRaw := raw[sidOffset:]
		if sidRaw[0] != 1 || sidRaw[1] > 15 || len(sidRaw) != int(minimumSIDSize)+int(sidRaw[1])*4 {
			return nil, ErrUnsafeTarget
		}
		sid := (*windows.SID)(unsafe.Pointer(&sidRaw[0]))
		if !sid.IsValid() || sid.Len() != len(sidRaw) || sid.String() == "" {
			return nil, ErrUnsafeTarget
		}
		result = append(result, windowsBasicACE{
			typeID: aceHeader.AceType,
			flags:  aceHeader.AceFlags,
			mask:   windows.ACCESS_MASK(binary.LittleEndian.Uint32(raw[4:8])),
			sid:    sid.String(),
		})
	}
	runtime.KeepAlive(dacl)
	return result, nil
}

func mapWindowsDirectoryGenericMask(mask windows.ACCESS_MASK) windows.ACCESS_MASK {
	if mask&windows.GENERIC_READ != 0 {
		mask = mask&^windows.GENERIC_READ | windows.FILE_GENERIC_READ
	}
	if mask&windows.GENERIC_WRITE != 0 {
		mask = mask&^windows.GENERIC_WRITE | windows.FILE_GENERIC_WRITE
	}
	if mask&windows.GENERIC_EXECUTE != 0 {
		mask = mask&^windows.GENERIC_EXECUTE | windows.FILE_GENERIC_EXECUTE
	}
	if mask&windows.GENERIC_ALL != 0 {
		mask = mask&^windows.GENERIC_ALL | windowsFileAllAccess
	}
	return mask
}

func requiredWindowsSIDs(user *windows.SID) (map[string]struct{}, error) {
	result := map[string]struct{}{user.String(): {}}
	for _, id := range []string{windowsSystemSID, windowsAdministratorsSID} {
		sid, err := windows.StringToSid(id)
		if err != nil {
			return nil, errors.Join(ErrUnsafeTarget, err)
		}
		result[sid.String()] = struct{}{}
	}
	return result, nil
}

func platformIdentityOf(file *os.File, kind entryKind) (platformIdentity, error) {
	if file == nil {
		return platformIdentity{}, ErrUnsafeTarget
	}
	var information windows.ByHandleFileInformation
	if err := windows.GetFileInformationByHandle(windows.Handle(file.Fd()), &information); err != nil {
		return platformIdentity{}, errors.Join(ErrUnsupported, err)
	}
	if information.FileAttributes&windows.FILE_ATTRIBUTE_REPARSE_POINT != 0 {
		return platformIdentity{}, ErrUnsafeTarget
	}
	if kind == entryDirectory && information.FileAttributes&windows.FILE_ATTRIBUTE_DIRECTORY == 0 ||
		kind == entryRegular && information.FileAttributes&windows.FILE_ATTRIBUTE_DIRECTORY != 0 {
		return platformIdentity{}, ErrUnsafeTarget
	}
	if information.VolumeSerialNumber == 0 || information.FileIndexHigh == 0 && information.FileIndexLow == 0 {
		return platformIdentity{}, ErrUnsupported
	}
	return platformIdentity{
		volume: information.VolumeSerialNumber,
		high:   information.FileIndexHigh,
		low:    information.FileIndexLow,
		kind:   kind,
	}, nil
}

func platformVerifyIdentity(file *os.File, expected platformIdentity, kind entryKind) error {
	actual, err := platformIdentityOf(file, kind)
	if err != nil {
		return err
	}
	if actual != expected {
		return ErrUnsafeTarget
	}
	return nil
}

func platformLock(file *os.File) error {
	overlapped := new(windows.Overlapped)
	err := windows.LockFileEx(windows.Handle(file.Fd()), windows.LOCKFILE_EXCLUSIVE_LOCK|windows.LOCKFILE_FAIL_IMMEDIATELY, 0, 1, 0, overlapped)
	if errors.Is(err, windows.ERROR_LOCK_VIOLATION) {
		return fmt.Errorf("%w: %s", ErrBusy, file.Name())
	}
	if err != nil {
		return fmt.Errorf("namespacelock: lock rendezvous: %w", err)
	}
	return nil
}

func platformUnlock(file *os.File) error {
	overlapped := new(windows.Overlapped)
	if err := windows.UnlockFileEx(windows.Handle(file.Fd()), 0, 1, 0, overlapped); err != nil {
		return fmt.Errorf("namespacelock: unlock rendezvous: %w", err)
	}
	return nil
}

func isWindowsNotExist(err error) bool {
	status, ok := err.(windows.NTStatus)
	return ok && (status == windows.STATUS_OBJECT_NAME_NOT_FOUND || status == windows.STATUS_OBJECT_PATH_NOT_FOUND)
}

func normalizeWindowsError(operation, name string, err error) error {
	status, ok := err.(windows.NTStatus)
	if ok {
		switch status {
		case windows.STATUS_OBJECT_NAME_NOT_FOUND, windows.STATUS_OBJECT_PATH_NOT_FOUND:
			err = syscall.ERROR_FILE_NOT_FOUND
		case windows.STATUS_REPARSE_POINT_ENCOUNTERED, windows.STATUS_OBJECT_TYPE_MISMATCH:
			err = errors.Join(ErrUnsafeTarget, status.Errno())
		case windows.STATUS_NOT_SUPPORTED, windows.STATUS_INVALID_DEVICE_REQUEST, windows.STATUS_NOT_SUPPORTED_IN_APPCONTAINER:
			err = errors.Join(ErrUnsupported, status.Errno())
		default:
			err = status.Errno()
		}
	}
	if errors.Is(err, windows.ERROR_SHARING_VIOLATION) {
		err = errors.Join(ErrBusy, err)
	}
	return &os.PathError{Op: operation, Path: name, Err: err}
}
