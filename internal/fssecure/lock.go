// Package fssecure contains the shared filesystem namespace and handle
// checks used by host locking and blob publication.
package fssecure

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
)

var (
	ErrBusy             = errors.New("fssecure: namespace is already locked")
	ErrUnsafeFilesystem = errors.New("fssecure: unsafe filesystem identity")
	ErrIdentityChanged  = errors.New("fssecure: filesystem identity changed")
)

const namespaceLockSuffix = ".mahoroba-namespace.lock"

// NamespaceLockTargetBasename recognizes only the exact rendezvous name
// produced by AcquireNamespace and returns its original target basename.
// Callers must still validate the target for their own namespace and open the
// lock entry through a protected handle before treating it as metadata.
func NamespaceLockTargetBasename(name string) (string, bool) {
	if !strings.HasPrefix(name, ".") || !strings.HasSuffix(name, namespaceLockSuffix) {
		return "", false
	}
	target := strings.TrimSuffix(strings.TrimPrefix(name, "."), namespaceLockSuffix)
	if target == "" || validateBasename(target) != nil ||
		"."+target+namespaceLockSuffix != name {
		return "", false
	}
	return target, true
}

// NamespaceLock serializes one basename in an already-verified directory.
// The rendezvous entry itself is relative-opened with the managed policy.
type NamespaceLock struct {
	handle *Handle
	base   string
	once   sync.Once
	err    error
}

func (dir *Directory) AcquireNamespace(targetBasename string) (*NamespaceLock, error) {
	if err := validateBasename(targetBasename); err != nil {
		return nil, err
	}
	lockName := "." + targetBasename + namespaceLockSuffix
	handle, err := dir.OpenOrCreateRegular(lockName)
	if err != nil {
		return nil, fmt.Errorf("open namespace lock: %w", err)
	}
	if err := platformLock(handle.file); err != nil {
		_ = handle.Close()
		return nil, err
	}
	return &NamespaceLock{handle: handle, base: targetBasename}, nil
}

func (lock *NamespaceLock) Close() error {
	if lock == nil {
		return nil
	}
	lock.once.Do(func() {
		if lock.handle != nil {
			lock.err = errors.Join(platformUnlock(lock.handle.file), lock.handle.Close())
		}
	})
	return lock.err
}

func (lock *NamespaceLock) OpenTarget() (*Handle, bool, error) {
	if lock == nil || lock.handle == nil || lock.handle.retainedParent == nil {
		return nil, false, ErrUnsafeFilesystem
	}
	if err := lock.handle.VerifyBound(); err != nil {
		return nil, false, err
	}
	file, _, err := openRegularRelative(lock.handle.retainedParent, lock.base, regularReadWriteExisting, lock.handle.policy)
	if errors.Is(err, os.ErrNotExist) {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, err
	}
	parentIdentity, err := identityFromHandle(lock.handle.retainedParent, KindDirectory)
	if err != nil {
		_ = file.Close()
		return nil, false, err
	}
	parentCopy, err := duplicateOpenFile(lock.handle.retainedParent, "namespace-target-parent")
	if err != nil {
		_ = file.Close()
		return nil, false, err
	}
	ancestors, err := lock.handle.ancestors.clone()
	if err != nil {
		_ = file.Close()
		_ = parentCopy.Close()
		return nil, false, err
	}
	info, err := file.Stat()
	if err != nil {
		_ = file.Close()
		_ = parentCopy.Close()
		_ = ancestors.close()
		return nil, false, err
	}
	identity, err := identityFromHandle(file, KindRegular)
	if err != nil {
		_ = file.Close()
		_ = parentCopy.Close()
		_ = ancestors.close()
		return nil, false, err
	}
	if !parentIdentity.SameVolume(identity) {
		_ = file.Close()
		_ = parentCopy.Close()
		_ = ancestors.close()
		return nil, false, unsupportedFilesystem("namespace target crosses the managed volume", nil)
	}
	return &Handle{file: file, retainedParent: parentCopy, ancestors: ancestors, info: info, identity: identity, parent: parentIdentity, base: lock.base, policy: lock.handle.policy, sealed: true}, true, nil
}

// TargetExistsUnderLock preserves the historical query API while resolving
// the target relative to the retained parent handle.
func (lock *NamespaceLock) TargetExistsUnderLock(target string) (bool, os.FileInfo, error) {
	if lock == nil || filepath.Base(target) != lock.base {
		return false, nil, ErrUnsafeFilesystem
	}
	handle, exists, err := lock.OpenTarget()
	if err != nil || !exists {
		return exists, nil, err
	}
	defer handle.Close()
	return true, handle.Snapshot(), nil
}

type Handle struct {
	file             *os.File
	retainedParent   *os.File
	ancestors        *directoryBinding
	path             string
	info             os.FileInfo
	identity         Identity
	parent           Identity
	base             string
	policy           SecurityPolicy
	readOnly         bool
	sealed           bool
	deleted          bool
	testBeforeRename func()
}

func newRelativeHandle(parent *Directory, name string, file *os.File, readOnly bool) (*Handle, error) {
	if parent == nil || file == nil {
		return nil, ErrIdentityChanged
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
	if !parent.identity.SameVolume(identity) {
		_ = file.Close()
		return nil, unsupportedFilesystem("managed file crosses the root volume", nil)
	}
	if err := validateOpenedSecurity(file, KindRegular, parent.policy); err != nil {
		_ = file.Close()
		return nil, err
	}
	parentCopy, err := duplicateOpenFile(parent.file, "file-parent:"+name)
	if err != nil {
		_ = file.Close()
		return nil, err
	}
	ancestors, err := parent.cloneBindingChain()
	if err != nil {
		_ = file.Close()
		_ = parentCopy.Close()
		return nil, err
	}
	return &Handle{
		file: file, retainedParent: parentCopy, path: filepath.Join(parent.path, name),
		info: info, identity: identity, parent: parent.identity, base: name, policy: parent.policy,
		ancestors: ancestors, readOnly: readOnly, sealed: true,
	}, nil
}

func (handle *Handle) Identity() Identity {
	if handle == nil {
		return Identity{}
	}
	return handle.identity
}

func (handle *Handle) Name() string {
	if handle == nil {
		return ""
	}
	return handle.base
}

func (handle *Handle) File() *os.File {
	if handle == nil {
		return nil
	}
	return handle.file
}

func (handle *Handle) Snapshot() os.FileInfo {
	if handle == nil {
		return nil
	}
	return handle.info
}

// Seal flushes a newly written private file and freezes its verification
// snapshot without reopening its name.
func (handle *Handle) Seal() error {
	if handle == nil || handle.file == nil || handle.deleted || handle.readOnly {
		return ErrIdentityChanged
	}
	if err := handle.file.Sync(); err != nil {
		return err
	}
	currentIdentity, err := identityFromHandle(handle.file, KindRegular)
	if err != nil || !handle.identity.Equal(currentIdentity) {
		return identityChanged(ReasonFileIdentityChanged, err)
	}
	if err := validateOpenedSecurity(handle.file, KindRegular, handle.policy); err != nil {
		return err
	}
	info, err := handle.file.Stat()
	if err != nil {
		return err
	}
	handle.info = info
	handle.sealed = true
	return handle.VerifyBound()
}

func (handle *Handle) VerifyHandle() error {
	if handle == nil || handle.file == nil || handle.info == nil || !handle.sealed {
		return ErrIdentityChanged
	}
	current, err := handle.file.Stat()
	if err != nil {
		return err
	}
	if !os.SameFile(handle.info, current) || current.Size() != handle.info.Size() || current.ModTime() != handle.info.ModTime() {
		return identityChanged(ReasonFileIdentityChanged, nil)
	}
	identity, err := identityFromHandle(handle.file, KindRegular)
	if err != nil || !handle.identity.Equal(identity) {
		return identityChanged(ReasonFileIdentityChanged, err)
	}
	if handle.policy.valid() {
		return validateOpenedSecurity(handle.file, KindRegular, handle.policy)
	}
	return nil
}

func (handle *Handle) VerifyBound() error {
	if handle == nil || handle.file == nil || handle.deleted {
		return ErrIdentityChanged
	}
	if err := handle.VerifyHandle(); err != nil {
		return err
	}
	return handle.verifyBinding()
}

// VerifyBinding proves stable handle identity, exact security policy, and the
// current parent-relative basename without treating legitimate content writes
// as an identity change. It is the verification surface for a retained
// OpenRegularIdentityGuard handle.
func (handle *Handle) VerifyBinding() error {
	return handle.verifyBinding()
}

// verifyBinding proves only handle identity, security policy, and namespace
// binding. It deliberately ignores the sealed content snapshot so a partially
// written private file can still be safely removed after a copy/sync failure.
func (handle *Handle) verifyBinding() error {
	if handle == nil || handle.file == nil || handle.deleted || handle.retainedParent == nil || handle.base == "" {
		return identityChanged(ReasonFileIdentityChanged, nil)
	}
	identity, err := identityFromHandle(handle.file, KindRegular)
	if err != nil || !handle.identity.Equal(identity) {
		return identityChanged(ReasonFileIdentityChanged, err)
	}
	if handle.policy.valid() {
		if err := validateOpenedSecurity(handle.file, KindRegular, handle.policy); err != nil {
			return err
		}
	}
	if err := handle.ancestors.verify(); err != nil {
		return err
	}
	if handle.base == "" {
		return ErrIdentityChanged
	}
	parentIdentity, err := identityFromHandle(handle.retainedParent, KindDirectory)
	if err != nil || !handle.parent.Equal(parentIdentity) {
		return identityChanged(ReasonDirectoryIdentityChanged, err)
	}
	if handle.ancestors != nil && !handle.ancestors.identity.Equal(parentIdentity) {
		return identityChanged(ReasonDirectoryIdentityChanged, nil)
	}
	current, err := identityRelative(handle.retainedParent, handle.base, KindRegular, handle.policy)
	if err != nil || !handle.identity.Equal(current) {
		return identityChanged(ReasonFileIdentityChanged, err)
	}
	return nil
}

func (handle *Handle) VerifyIdentity() error { return handle.VerifyBound() }

func (handle *Handle) VerifySecurity(policy SecurityPolicy) error {
	if handle == nil || handle.file == nil {
		return ErrIdentityChanged
	}
	return validateOpenedSecurity(handle.file, KindRegular, policy)
}

func (handle *Handle) Hash(ctx context.Context) ([sha256.Size]byte, int64, error) {
	var digest [sha256.Size]byte
	if ctx == nil {
		return digest, 0, errors.New("fssecure: nil hash context")
	}
	if err := handle.VerifyBound(); err != nil {
		return digest, 0, err
	}
	if _, err := handle.file.Seek(0, io.SeekStart); err != nil {
		return digest, 0, err
	}
	hasher := sha256.New()
	size, err := io.Copy(hasher, &contextReader{ctx: ctx, reader: handle.file})
	if err != nil {
		return digest, size, err
	}
	if err := handle.VerifyBound(); err != nil {
		return digest, size, err
	}
	copy(digest[:], hasher.Sum(nil))
	return digest, size, nil
}

func (handle *Handle) MarkDeleteOnClose() error {
	if handle != nil && handle.readOnly {
		return WithReason(ReasonUnsafeMode, ErrUnsafeFilesystem)
	}
	if err := handle.verifyBinding(); err != nil {
		return err
	}
	if err := removeRelative(handle.retainedParent, handle.base, handle.file, handle.policy); err != nil {
		return err
	}
	if err := syncDirectory(handle.retainedParent); err != nil {
		return err
	}
	handle.deleted = true
	return nil
}

func (handle *Handle) Close() error {
	if handle == nil {
		return nil
	}
	var parentErr, fileErr error
	if handle.file != nil {
		fileErr = handle.file.Close()
		handle.file = nil
	}
	if handle.retainedParent != nil {
		parentErr = handle.retainedParent.Close()
		handle.retainedParent = nil
	}
	ancestorErr := handle.ancestors.close()
	handle.ancestors = nil
	return errors.Join(fileErr, parentErr, ancestorErr)
}

type contextReader struct {
	ctx    context.Context
	reader io.Reader
}

func (reader *contextReader) Read(p []byte) (int, error) {
	if err := reader.ctx.Err(); err != nil {
		return 0, err
	}
	return reader.reader.Read(p)
}

func validateBasename(value string) error {
	if value == "" || value == "." || value == ".." || strings.ContainsRune(value, 0) || filepath.Base(value) != value || strings.ContainsAny(value, `/\\`) {
		return fmt.Errorf("%w: invalid target basename", ErrUnsafeFilesystem)
	}
	return nil
}
