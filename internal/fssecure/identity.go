package fssecure

import (
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"

	"mahoroba.local/mahoroba/internal/canonical"
)

type EntryKind uint8

const (
	KindDirectory EntryKind = iota + 1
	KindRegular
)

// Identity contains only stable filesystem identity. Platform-specific
// fields remain private so callers cannot manufacture a trusted token.
type Identity struct {
	volume            uint64
	first             uint64
	second            uint64
	mount             uint64
	generationSeconds int64
	generationNanos   uint32
	kind              EntryKind
}

func (id Identity) Valid() bool {
	return id.volume != 0 && (id.first != 0 || id.second != 0) && id.kind != 0
}
func (id Identity) Equal(other Identity) bool {
	return id.Valid() && other.Valid() && id.volume == other.volume && id.first == other.first && id.second == other.second && id.mount == other.mount &&
		id.generationSeconds == other.generationSeconds && id.generationNanos == other.generationNanos && id.kind == other.kind
}
func (id Identity) SameVolume(other Identity) bool {
	return id.Valid() && other.Valid() && id.volume == other.volume && id.mount == other.mount
}

func (id Identity) artifactIdentityFields() (fileIdentity, volumeIdentity string) {
	return strconv.FormatUint(id.first, 10) + ":" +
			strconv.FormatUint(id.second, 10) + ":" +
			strconv.FormatUint(id.mount, 10) + ":" +
			strconv.FormatInt(id.generationSeconds, 10) + ":" +
			strconv.FormatUint(uint64(id.generationNanos), 10) + ":" +
			strconv.FormatUint(uint64(id.kind), 10),
		strconv.FormatUint(id.volume, 10)
}

type SecurityPolicy struct {
	uid      uint32
	ownerSID string
}

func (policy SecurityPolicy) valid() bool { return policy.uid != 0 || policy.ownerSID != "" }

type regularOpenMode uint8

const (
	regularReadExisting regularOpenMode = iota + 1
	regularObserveMutable
	regularReadWriteExisting
	regularOpenOrCreate
	regularCreateExclusive
)

// Directory is an authority-bearing directory handle. path is diagnostic
// only; namespace operations use file and retainedParent exclusively.
type Directory struct {
	file             *os.File
	retainedParent   *os.File
	ancestors        *directoryBinding
	path             string
	base             string
	identity         Identity
	policy           SecurityPolicy
	readOnly         bool
	publishAuthority bool
}

// directoryBinding is an owned, independently duplicated proof that one
// directory is still bound to its verified parent namespace. Linking these
// proofs retains the complete filesystem/drive-root-to-parent authority even
// after the Directory objects used to open a child or file have been closed.
type directoryBinding struct {
	file     *os.File
	parent   *os.File
	ancestor *directoryBinding
	base     string
	identity Identity
	policy   SecurityPolicy
	managed  bool
}

// retainRawDirectoryBinding transfers ownership of parent and ancestor into
// a new proof node. file remains owned by the caller; the node retains an
// independent duplicate so traversal can continue without shared ownership.
func retainRawDirectoryBinding(ancestor *directoryBinding, parent, file *os.File, base string) (*directoryBinding, error) {
	if parent == nil || file == nil || base == "" {
		return nil, identityChanged(ReasonDirectoryIdentityChanged, nil)
	}
	identity, err := identityFromHandle(file, KindDirectory)
	if err != nil {
		return nil, err
	}
	if ancestor != nil {
		parentIdentity, parentErr := identityFromHandle(parent, KindDirectory)
		if parentErr != nil || !ancestor.identity.Equal(parentIdentity) {
			return nil, identityChanged(ReasonDirectoryIdentityChanged, parentErr)
		}
	}
	proof, err := duplicateOpenFile(file, "raw-binding:"+base)
	if err != nil {
		return nil, err
	}
	return &directoryBinding{
		file: proof, parent: parent, ancestor: ancestor, base: base,
		identity: identity,
	}, nil
}

func (binding *directoryBinding) verify() error {
	if binding == nil {
		return nil
	}
	if err := binding.ancestor.verify(); err != nil {
		return err
	}
	if binding.file == nil || binding.parent == nil || binding.base == "" || !binding.identity.Valid() {
		return identityChanged(ReasonDirectoryIdentityChanged, nil)
	}
	if binding.managed {
		if err := validateDirectoryHandle(binding.file, binding.identity, binding.policy); err != nil {
			return err
		}
	} else if err := validateRawDirectoryHandle(binding.file, binding.identity); err != nil {
		return err
	}
	parentIdentity, err := identityFromHandle(binding.parent, KindDirectory)
	if err != nil {
		return identityChanged(ReasonDirectoryIdentityChanged, err)
	}
	if binding.ancestor != nil && !binding.ancestor.identity.Equal(parentIdentity) {
		return identityChanged(ReasonDirectoryIdentityChanged, nil)
	}
	var current Identity
	if binding.managed {
		current, err = identityRelative(binding.parent, binding.base, KindDirectory, binding.policy)
	} else {
		current, err = identityRelativeDirectoryRaw(binding.parent, binding.base)
	}
	if err != nil || !binding.identity.Equal(current) {
		return identityChanged(ReasonDirectoryIdentityChanged, err)
	}
	return nil
}

func (binding *directoryBinding) clone() (*directoryBinding, error) {
	if binding == nil {
		return nil, nil
	}
	ancestor, err := binding.ancestor.clone()
	if err != nil {
		return nil, err
	}
	file, err := duplicateOpenFile(binding.file, "binding:"+binding.base)
	if err != nil {
		_ = ancestor.close()
		return nil, err
	}
	parent, err := duplicateOpenFile(binding.parent, "binding-parent:"+binding.base)
	if err != nil {
		_ = file.Close()
		_ = ancestor.close()
		return nil, err
	}
	return &directoryBinding{
		file: file, parent: parent, ancestor: ancestor, base: binding.base,
		identity: binding.identity, policy: binding.policy, managed: binding.managed,
	}, nil
}

func (binding *directoryBinding) close() error {
	if binding == nil {
		return nil
	}
	var fileErr, parentErr error
	if binding.file != nil {
		fileErr = binding.file.Close()
		binding.file = nil
	}
	if binding.parent != nil {
		parentErr = binding.parent.Close()
		binding.parent = nil
	}
	ancestorErr := binding.ancestor.close()
	binding.ancestor = nil
	return errors.Join(fileErr, parentErr, ancestorErr)
}

func (dir *Directory) cloneBindingChain() (*directoryBinding, error) {
	if err := dir.VerifyBound(); err != nil {
		return nil, err
	}
	ancestor, err := dir.ancestors.clone()
	if err != nil {
		return nil, err
	}
	file, err := duplicateOpenFile(dir.file, "binding:"+dir.base)
	if err != nil {
		_ = ancestor.close()
		return nil, err
	}
	parent, err := duplicateOpenFile(dir.retainedParent, "binding-parent:"+dir.base)
	if err != nil {
		_ = file.Close()
		_ = ancestor.close()
		return nil, err
	}
	return &directoryBinding{
		file: file, parent: parent, ancestor: ancestor, base: dir.base,
		identity: dir.identity, policy: dir.policy, managed: true,
	}, nil
}

func (dir *Directory) Path() string {
	if dir == nil {
		return ""
	}
	return dir.path
}

func (dir *Directory) Identity() Identity {
	if dir == nil {
		return Identity{}
	}
	return dir.identity
}

// ArtifactSourceIdentityDigest binds a retained directory authority to the
// physical source identity required by M7 DurablePublish input objects. The
// plaintext path is included only in the hashed JCS value and is never
// returned. Callers receive the domain-separated sha256: digest.
func (dir *Directory) ArtifactSourceIdentityDigest() (string, error) {
	if err := dir.VerifyBound(); err != nil {
		return "", err
	}
	canonicalPath, err := filepath.EvalSymlinks(dir.path)
	if err != nil {
		return "", err
	}
	canonicalPath, err = filepath.Abs(canonicalPath)
	if err != nil {
		return "", err
	}
	if err := dir.VerifyBound(); err != nil {
		return "", err
	}
	fileIdentity, volumeIdentity := dir.identity.artifactIdentityFields()
	identity := struct {
		CanonicalPath  string `json:"canonical_path"`
		FileIdentity   string `json:"file_identity"`
		Platform       string `json:"platform"`
		VolumeIdentity string `json:"volume_identity"`
	}{
		CanonicalPath: filepath.Clean(canonicalPath),
		FileIdentity:  fileIdentity,
		Platform:      runtime.GOOS, VolumeIdentity: volumeIdentity,
	}
	encoded, err := canonical.MarshalCanonical(identity)
	if err != nil {
		return "", err
	}
	hasher := sha256.New()
	_, _ = hasher.Write([]byte("mahoroba:artifact-source-identity:v1\x00"))
	_, _ = hasher.Write(encoded.Bytes())
	return "sha256:" + hex.EncodeToString(hasher.Sum(nil)), nil
}

func (dir *Directory) Close() error {
	if dir == nil {
		return nil
	}
	var parentErr, fileErr error
	if dir.retainedParent != nil {
		parentErr = dir.retainedParent.Close()
		dir.retainedParent = nil
	}
	if dir.file != nil {
		fileErr = dir.file.Close()
		dir.file = nil
	}
	ancestorErr := dir.ancestors.close()
	dir.ancestors = nil
	return errors.Join(parentErr, fileErr, ancestorErr)
}

// OpenOrCreateRoot creates only the final basename after component-wise,
// no-follow traversal. Existing unsafe roots are rejected, not repaired.
func OpenOrCreateRoot(path string, policy SecurityPolicy) (*Directory, error) {
	return openRoot(path, policy, true)
}

// OpenRoot reopens an existing root without recreating a missing/replaced
// namespace entry. Runtime verification must use this form.
func OpenRoot(path string, policy SecurityPolicy) (*Directory, error) {
	return openRoot(path, policy, false)
}

// OpenRootReadOnly opens an existing protected root without requesting write
// access to the bundle directories. All descendants inherit this restricted
// authority, so create, sync, and publication methods fail closed.
func OpenRootReadOnly(path string, policy SecurityPolicy) (*Directory, error) {
	if !filepath.IsAbs(path) || filepath.Clean(path) != path {
		return nil, WithReason(ReasonUnsafeMode, ErrUnsafeFilesystem)
	}
	if !policy.valid() {
		var err error
		policy, err = CurrentSecurityPolicy()
		if err != nil {
			return nil, err
		}
	}
	file, parent, ancestors, base, err := openRootRelativeReadOnly(path, policy)
	if err != nil {
		return nil, err
	}
	identity, err := identityFromHandle(file, KindDirectory)
	if err != nil {
		_ = file.Close()
		_ = parent.Close()
		_ = ancestors.close()
		return nil, err
	}
	if err := validateOpenedSecurity(file, KindDirectory, policy); err != nil {
		_ = file.Close()
		_ = parent.Close()
		_ = ancestors.close()
		return nil, err
	}
	return &Directory{
		file: file, retainedParent: parent, ancestors: ancestors, path: path,
		base: base, identity: identity, policy: policy, readOnly: true,
	}, nil
}

// OpenRootForPublish reopens an existing managed root with retained-parent
// authority for sibling markers and a sibling no-replace rename. On Windows
// the final directory deliberately starts without DELETE access so bounded
// digest/read traversal remains compatible with the no-delete-sharing policy;
// PublishSiblingNoReplace acquires and releases DELETE just around the rename.
func OpenRootForPublish(path string, policy SecurityPolicy) (*Directory, error) {
	if !filepath.IsAbs(path) || filepath.Clean(path) != path {
		return nil, WithReason(ReasonUnsafeMode, ErrUnsafeFilesystem)
	}
	if !policy.valid() {
		var err error
		policy, err = CurrentSecurityPolicy()
		if err != nil {
			return nil, err
		}
	}
	file, parent, ancestors, base, err := openRootRelativeForPublish(path, policy)
	if err != nil {
		return nil, fmt.Errorf("fssecure: open publication root traversal: %w", err)
	}
	identity, err := identityFromHandle(file, KindDirectory)
	if err != nil {
		_ = file.Close()
		_ = parent.Close()
		_ = ancestors.close()
		return nil, err
	}
	return &Directory{
		file: file, retainedParent: parent, ancestors: ancestors,
		path: path, base: base, identity: identity, policy: policy, publishAuthority: true,
	}, nil
}

func openRoot(path string, policy SecurityPolicy, create bool) (*Directory, error) {
	if !filepath.IsAbs(path) || filepath.Clean(path) != path {
		return nil, WithReason(ReasonUnsafeMode, ErrUnsafeFilesystem)
	}
	if !policy.valid() {
		var err error
		policy, err = CurrentSecurityPolicy()
		if err != nil {
			return nil, err
		}
	}
	file, parent, ancestors, base, err := openRootRelative(path, create, policy)
	if err != nil {
		return nil, err
	}
	identity, err := identityFromHandle(file, KindDirectory)
	if err != nil {
		_ = file.Close()
		_ = parent.Close()
		_ = ancestors.close()
		return nil, err
	}
	if err := validateOpenedSecurity(file, KindDirectory, policy); err != nil {
		_ = file.Close()
		_ = parent.Close()
		_ = ancestors.close()
		return nil, err
	}
	return &Directory{file: file, retainedParent: parent, ancestors: ancestors, path: path, base: base, identity: identity, policy: policy}, nil
}

func (dir *Directory) VerifyBound() error {
	if dir == nil || dir.file == nil || !dir.identity.Valid() {
		return ErrIdentityChanged
	}
	if err := validateDirectoryHandle(dir.file, dir.identity, dir.policy); err != nil {
		return err
	}
	if err := dir.ancestors.verify(); err != nil {
		return err
	}
	if dir.retainedParent == nil || dir.base == "" {
		return identityChanged(ReasonDirectoryIdentityChanged, nil)
	}
	parentIdentity, err := identityFromHandle(dir.retainedParent, KindDirectory)
	if err != nil {
		return identityChanged(ReasonDirectoryIdentityChanged, err)
	}
	if dir.ancestors != nil && !dir.ancestors.identity.Equal(parentIdentity) {
		return identityChanged(ReasonDirectoryIdentityChanged, nil)
	}
	current, err := identityRelative(dir.retainedParent, dir.base, KindDirectory, dir.policy)
	if err != nil || !dir.identity.Equal(current) {
		return identityChanged(ReasonDirectoryIdentityChanged, err)
	}
	return nil
}

func (dir *Directory) OpenDirectory(name string) (*Directory, error) {
	return dir.openDirectory(name, false)
}

func (dir *Directory) OpenOrCreateDirectory(name string) (*Directory, error) {
	return dir.openDirectory(name, true)
}

func (dir *Directory) openDirectory(name string, create bool) (*Directory, error) {
	if err := validateBasename(name); err != nil {
		return nil, err
	}
	if err := dir.VerifyBound(); err != nil {
		return nil, err
	}
	if dir.readOnly && create {
		return nil, WithReason(ReasonUnsafeMode, ErrUnsafeFilesystem)
	}
	var file *os.File
	var err error
	if dir.readOnly {
		file, err = openDirectoryRelativeReadOnly(dir.file, name, dir.policy)
	} else {
		file, err = openDirectoryRelative(dir.file, name, create, dir.policy)
	}
	if err != nil {
		return nil, err
	}
	identity, err := identityFromHandle(file, KindDirectory)
	if err != nil {
		_ = file.Close()
		return nil, err
	}
	if err := requireSameVolume("managed directory crosses the root volume", dir.identity, identity); err != nil {
		_ = file.Close()
		return nil, err
	}
	parent, err := duplicateOpenFile(dir.file, "parent:"+name)
	if err != nil {
		_ = file.Close()
		return nil, err
	}
	ancestors, err := dir.cloneBindingChain()
	if err != nil {
		_ = parent.Close()
		_ = file.Close()
		return nil, err
	}
	return &Directory{
		file: file, retainedParent: parent, path: filepath.Join(dir.path, name), base: name,
		identity: identity, policy: dir.policy, ancestors: ancestors, readOnly: dir.readOnly,
	}, nil
}

func (dir *Directory) OpenRegular(name string) (*Handle, error) {
	return dir.openRegular(name, regularReadWriteExisting)
}

// OpenRegularRead opens a verified read-only handle. On Windows it shares
// reads with other verified readers while still denying external writes and
// deletes for the lifetime of the handle.
func (dir *Directory) OpenRegularRead(name string) (*Handle, error) {
	return dir.openRegular(name, regularReadExisting)
}

// OpenRegularIdentityGuard opens an existing regular file relative to the
// retained directory for identity-only observation while another cooperating
// handle may write its contents. On Windows the retained handle shares reads
// and writes but not delete; a live SQLite session supplies the compatible
// no-delete-sharing data handles, while this handle supplies root-relative
// identity revalidation. Once those data handles close, VerifyBinding remains
// the authority for detecting replacement. On Linux the retained O_NOFOLLOW
// descriptor likewise detects namespace replacement at each caller-owned
// transaction boundary.
func (dir *Directory) OpenRegularIdentityGuard(name string) (*Handle, error) {
	return dir.openRegular(name, regularObserveMutable)
}

// CreateSiblingRegular creates a protected marker beside dir relative to the
// retained parent handle. It never resolves the sibling through a pathname.
func (dir *Directory) CreateSiblingRegular(name string) (*Handle, error) {
	if dir == nil || dir.readOnly || !dir.publishAuthority {
		return nil, WithReason(ReasonUnsafeMode, ErrUnsafeFilesystem)
	}
	return dir.openSiblingRegular(name, regularCreateExclusive)
}

// OpenSiblingRegular opens an existing protected sibling marker without
// granting mutation authority.
func (dir *Directory) OpenSiblingRegular(name string) (*Handle, error) {
	if dir == nil || !dir.publishAuthority {
		return nil, WithReason(ReasonUnsafeMode, ErrUnsafeFilesystem)
	}
	return dir.openSiblingRegular(name, regularReadExisting)
}

// OpenSiblingRegularForUpdate reopens a protected sibling marker with the
// delete authority required to complete DurablePublish recovery.
func (dir *Directory) OpenSiblingRegularForUpdate(name string) (*Handle, error) {
	if dir == nil || dir.readOnly || !dir.publishAuthority {
		return nil, WithReason(ReasonUnsafeMode, ErrUnsafeFilesystem)
	}
	return dir.openSiblingRegular(name, regularReadWriteExisting)
}

// OpenUniquePublishMarkerSiblingForUpdate performs the one narrow parent
// enumeration needed when a directory artifact has already removed its
// internal PUBLISH_PENDING marker. It returns no parent names, accepts only
// the exact target-derived marker prefix, and fails closed on ambiguity.
func (dir *Directory) OpenUniquePublishMarkerSiblingForUpdate(targetBasename string) (*Handle, bool, error) {
	if dir == nil || dir.readOnly || !dir.publishAuthority {
		return nil, false, WithReason(ReasonUnsafeMode, ErrUnsafeFilesystem)
	}
	if err := validateBasename(targetBasename); err != nil {
		return nil, false, err
	}
	if err := dir.VerifyBound(); err != nil {
		return nil, false, err
	}
	if dir.retainedParent == nil {
		return nil, false, ErrIdentityChanged
	}
	prefix := "." + targetBasename + ".publish-pending."
	entries, err := dir.retainedParent.ReadDir(-1)
	if err != nil {
		return nil, false, err
	}
	var match string
	for _, entry := range entries {
		name := entry.Name()
		if !strings.HasPrefix(name, prefix) {
			continue
		}
		suffix := strings.TrimPrefix(name, prefix)
		if _, err := canonical.ParseID(suffix); err != nil {
			return nil, false, WithReason(ReasonUnsafeMode, ErrUnsafeFilesystem)
		}
		if match != "" {
			return nil, false, WithReason(ReasonUnsafeMode, ErrUnsafeFilesystem)
		}
		match = name
	}
	if err := dir.VerifyBound(); err != nil {
		return nil, false, err
	}
	if match == "" {
		return nil, false, nil
	}
	handle, err := dir.openSiblingRegular(match, regularReadWriteExisting)
	if err != nil {
		return nil, false, err
	}
	return handle, true, nil
}

func (dir *Directory) openSiblingRegular(name string, mode regularOpenMode) (*Handle, error) {
	if err := validateBasename(name); err != nil {
		return nil, err
	}
	if err := dir.VerifyBound(); err != nil {
		return nil, err
	}
	if dir.retainedParent == nil {
		return nil, ErrIdentityChanged
	}
	file, _, err := openRegularRelative(dir.retainedParent, name, mode, dir.policy)
	if err != nil {
		return nil, err
	}
	info, err := file.Stat()
	if err != nil {
		_ = file.Close()
		return nil, err
	}
	identity, err := identityFromHandle(file, KindRegular)
	if err != nil {
		_ = file.Close()
		return nil, err
	}
	parentIdentity, err := identityFromHandle(dir.retainedParent, KindDirectory)
	if err != nil || !parentIdentity.SameVolume(identity) {
		_ = file.Close()
		return nil, errors.Join(unsupportedFilesystem("sibling marker crosses the root volume", nil), err)
	}
	if err := validateOpenedSecurity(file, KindRegular, dir.policy); err != nil {
		_ = file.Close()
		return nil, err
	}
	parent, err := duplicateOpenFile(dir.retainedParent, "sibling-parent:"+name)
	if err != nil {
		_ = file.Close()
		return nil, err
	}
	ancestors, err := dir.ancestors.clone()
	if err != nil {
		_ = parent.Close()
		_ = file.Close()
		return nil, err
	}
	return &Handle{
		file: file, retainedParent: parent, ancestors: ancestors,
		path: filepath.Join(filepath.Dir(dir.path), name), info: info,
		identity: identity, parent: parentIdentity, base: name, policy: dir.policy,
		readOnly: mode == regularReadExisting, sealed: true,
	}, nil
}

func (dir *Directory) openRegular(name string, mode regularOpenMode) (*Handle, error) {
	if err := validateBasename(name); err != nil {
		return nil, err
	}
	if err := dir.VerifyBound(); err != nil {
		return nil, err
	}
	if dir.readOnly && mode != regularReadExisting {
		return nil, WithReason(ReasonUnsafeMode, ErrUnsafeFilesystem)
	}
	file, _, err := openRegularRelative(dir.file, name, mode, dir.policy)
	if err != nil {
		return nil, err
	}
	return newRelativeHandle(dir, name, file,
		dir.readOnly || mode == regularReadExisting || mode == regularObserveMutable)
}

func (dir *Directory) OpenOrCreateRegular(name string) (*Handle, error) {
	return dir.openRegular(name, regularOpenOrCreate)
}

func (dir *Directory) CreateRegular(name string) (*Handle, error) {
	return dir.openRegular(name, regularCreateExclusive)
}

func (dir *Directory) CreateTemp(prefix string) (*Handle, error) {
	if prefix == "" || strings.ContainsAny(prefix, `/\\`) || strings.ContainsRune(prefix, 0) {
		return nil, fmt.Errorf("%w: invalid temporary prefix", ErrUnsafeFilesystem)
	}
	for attempt := 0; attempt < 128; attempt++ {
		var nonce [16]byte
		if _, err := rand.Read(nonce[:]); err != nil {
			return nil, err
		}
		name := prefix + hex.EncodeToString(nonce[:])
		handle, err := dir.CreateRegular(name)
		if errors.Is(err, os.ErrExist) {
			continue
		}
		return handle, err
	}
	return nil, fmt.Errorf("fssecure: temporary name exhaustion")
}

// ReadDir obtains a fresh file description from the retained parent authority.
// Duplicating dir.file would share its enumeration offset and make later scans
// silently remain at EOF.
func (dir *Directory) ReadDir() ([]os.DirEntry, error) {
	if err := dir.VerifyBound(); err != nil {
		return nil, err
	}
	if dir.retainedParent == nil || dir.base == "" {
		return nil, identityChanged(ReasonDirectoryIdentityChanged, nil)
	}
	var fresh *os.File
	var err error
	if dir.readOnly {
		fresh, err = openDirectoryRelativeReadOnly(dir.retainedParent, dir.base, dir.policy)
	} else {
		fresh, err = openDirectoryRelative(dir.retainedParent, dir.base, false, dir.policy)
	}
	if err != nil {
		return nil, err
	}
	defer fresh.Close()
	identity, err := identityFromHandle(fresh, KindDirectory)
	if err != nil || !dir.identity.Equal(identity) {
		return nil, identityChanged(ReasonDirectoryIdentityChanged, err)
	}
	return fresh.ReadDir(-1)
}

func (dir *Directory) PublishNoReplace(source *Handle, name string) error {
	if dir == nil || dir.readOnly {
		return WithReason(ReasonUnsafeMode, ErrUnsafeFilesystem)
	}
	return source.renameTo(dir, name)
}

func (dir *Directory) MoveTo(source *Handle, destination *Directory, name string) error {
	if dir == nil || source == nil || destination == nil || !dir.identity.Equal(source.parent) {
		return ErrIdentityChanged
	}
	if dir.readOnly || destination.readOnly {
		return WithReason(ReasonUnsafeMode, ErrUnsafeFilesystem)
	}
	return source.renameTo(destination, name)
}

func (dir *Directory) Sync() error {
	if dir != nil && dir.readOnly {
		return WithReason(ReasonUnsafeMode, ErrUnsafeFilesystem)
	}
	if err := dir.VerifyBound(); err != nil {
		return err
	}
	return syncDirectory(dir.file)
}

// SyncParent executes the durability barrier for sibling marker and
// publication namespace mutations through the retained parent handle.
func (dir *Directory) SyncParent() error {
	if dir == nil || dir.readOnly || !dir.publishAuthority {
		return WithReason(ReasonUnsafeMode, ErrUnsafeFilesystem)
	}
	if err := dir.VerifyBound(); err != nil {
		return err
	}
	if dir.retainedParent == nil {
		return ErrIdentityChanged
	}
	return syncDirectory(dir.retainedParent)
}

// PublishSiblingNoReplace atomically publishes this directory under another
// basename in the same retained parent. The rename is handle-relative and
// no-replace; the parent durability barrier completes before success returns.
// The Directory remains bound to the published name for subsequent checks.
func (dir *Directory) PublishSiblingNoReplace(name string) error {
	if dir == nil {
		return ErrIdentityChanged
	}
	if dir.readOnly || !dir.publishAuthority {
		return WithReason(ReasonUnsafeMode, ErrUnsafeFilesystem)
	}
	if err := validateBasename(name); err != nil {
		return err
	}
	if err := dir.VerifyBound(); err != nil {
		return err
	}
	if dir.retainedParent == nil || dir.file == nil {
		return ErrIdentityChanged
	}
	oldName := dir.base
	if err := dir.reopenDirectoryHandle(oldName, true); err != nil {
		return err
	}
	if err := renameRelativeNoReplace(dir.retainedParent, dir.base, dir.file, dir.retainedParent, name); err != nil {
		return errors.Join(err, dir.reopenDirectoryHandle(oldName, false))
	}
	if err := dir.reopenDirectoryHandle(name, false); err != nil {
		return identityChanged(ReasonPublishDestinationMismatch, err)
	}
	return errors.Join(dir.VerifyBound(), syncDirectory(dir.retainedParent))
}

// reopenDirectoryHandle performs a parent-handle-relative, no-follow
// capability transition while retaining only the immutable baseline identity.
// Closing before reopening is required on Windows: the ordinary handle denies
// delete sharing, and the transient DELETE handle must not be retained while
// digest or marker cleanup opens fresh directory handles.
func (dir *Directory) reopenDirectoryHandle(name string, forRename bool) error {
	if dir == nil || dir.file == nil || dir.retainedParent == nil {
		return ErrIdentityChanged
	}
	oldPath := dir.path
	old := dir.file
	dir.file = nil
	if err := old.Close(); err != nil {
		return err
	}
	var (
		file *os.File
		err  error
	)
	if forRename {
		file, err = openDirectoryRelativeForPublish(dir.retainedParent, name, dir.policy)
	} else {
		file, err = openDirectoryRelative(dir.retainedParent, name, false, dir.policy)
	}
	if err != nil {
		return err
	}
	identity, err := identityFromHandle(file, KindDirectory)
	if err != nil || !dir.identity.Equal(identity) {
		_ = file.Close()
		return identityChanged(ReasonDirectoryIdentityChanged, err)
	}
	dir.file = file
	dir.base = name
	dir.path = filepath.Join(filepath.Dir(oldPath), name)
	return nil
}

func (handle *Handle) renameTo(destination *Directory, name string) error {
	if handle == nil || destination == nil {
		return ErrIdentityChanged
	}
	if handle.readOnly || destination.readOnly {
		return WithReason(ReasonUnsafeMode, ErrUnsafeFilesystem)
	}
	if err := validateBasename(name); err != nil {
		return err
	}
	if err := handle.VerifyBound(); err != nil {
		return err
	}
	if err := destination.VerifyBound(); err != nil {
		return err
	}
	if err := requireSameVolume("cross-volume rename", handle.identity, destination.identity); err != nil {
		return err
	}
	newParent, err := duplicateOpenFile(destination.file, "parent:"+name)
	if err != nil {
		return err
	}
	newAncestors, err := destination.cloneBindingChain()
	if err != nil {
		_ = newParent.Close()
		return err
	}
	oldParent := handle.retainedParent
	oldAncestors := handle.ancestors
	oldBase := handle.base
	if handle.testBeforeRename != nil {
		handle.testBeforeRename()
	}
	if err := renameRelativeNoReplace(oldParent, oldBase, handle.file, destination.file, name); err != nil {
		_ = newParent.Close()
		_ = newAncestors.close()
		return err
	}
	observed, observedErr := identityRelative(destination.file, name, KindRegular, destination.policy)
	if observedErr != nil || !handle.identity.Equal(observed) {
		_ = newParent.Close()
		_ = newAncestors.close()
		return identityChanged(ReasonPublishDestinationMismatch, observedErr)
	}
	// The namespace mutation has succeeded. Switch the handle authority before
	// durability work so a sync failure still leaves callers able to verify and
	// securely remove/quarantine the published entry.
	handle.retainedParent = newParent
	handle.ancestors = newAncestors
	handle.parent = destination.identity
	handle.base = name
	handle.path = filepath.Join(destination.path, name)
	verifyErr := handle.VerifyBound()
	syncErr := errors.Join(syncDirectory(oldParent), syncDirectory(destination.file))
	closeErr := oldParent.Close()
	ancestorCloseErr := oldAncestors.close()
	return errors.Join(verifyErr, syncErr, closeErr, ancestorCloseErr)
}

func identityRelative(parent *os.File, name string, kind EntryKind, policy SecurityPolicy) (Identity, error) {
	return identityRelativeEntry(parent, name, kind, policy)
}

func unsupportedFilesystem(message string, cause error) error {
	if cause == nil {
		cause = ErrUnsupportedSecureFilesystem
	}
	return fmt.Errorf("fssecure: %s: %w", message, WithReason(ReasonUnsupportedFilesystem, errors.Join(ErrUnsafeFilesystem, ErrUnsupportedSecureFilesystem, cause)))
}

func requireSameVolume(operation string, first, second Identity) error {
	if first.SameVolume(second) {
		return nil
	}
	return unsupportedFilesystem(operation, nil)
}

func fileInfo(file *os.File) os.FileInfo {
	if file == nil {
		return nil
	}
	info, _ := file.Stat()
	return info
}
