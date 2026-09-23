// Package blob implements the resident-scoped, content-addressed filesystem
// store. Namespace authority is carried exclusively by fssecure handles; path
// strings retained by FileStore are diagnostic and compatibility data only.
package blob

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"mahoroba.local/mahoroba/internal/canonical"
	"mahoroba.local/mahoroba/internal/fssecure"
)

var (
	ErrUnsafePath       = errors.New("blob: path escapes store root")
	ErrUnsafeFilesystem = errors.New("blob: unsafe filesystem entry")
	ErrInvalidStage     = errors.New("blob: invalid staged object")
	ErrInvalidOrphan    = errors.New("blob: invalid orphan candidate")
	ErrDigestMismatch   = errors.New("blob: content digest mismatch")
	ErrObjectNotFound   = errors.New("blob: object not found")
)

type FileStore struct {
	root          string
	stagingDir    string // diagnostic/test compatibility only
	objectsDir    string // diagnostic/test compatibility only
	quarantineDir string // diagnostic/test compatibility only
	policy        fssecure.SecurityPolicy

	// These tokens are initialized once and never mutated. Every operation
	// compares freshly opened handles against them, so verification is race-free.
	rootIdentity       fssecure.Identity
	stagingIdentity    fssecure.Identity
	objectsIdentity    fssecure.Identity
	quarantineIdentity fssecure.Identity
	readOnly           bool

	recoveryObserverMu sync.RWMutex
	recoveryObserver   interface {
		SetStagingOrphanCount(uint64)
		SetFinalOrphanCount(uint64)
	}

	finalizeMu            sync.Mutex
	testHook              func(string)
	testFailpoint         func(string) error
	testEvidenceName      func(string) string
	testMutatePublished   func(*fssecure.Handle) error
	testMutateStage       func(*fssecure.Handle) error
	testAfterPublishError error
}

// SetRecoveryObserver installs the process-local bounded metrics observer.
// The observer contract accepts counts only and cannot receive object paths,
// resident/content identity, file bytes, or underlying filesystem errors.
func (store *FileStore) SetRecoveryObserver(observer interface {
	SetStagingOrphanCount(uint64)
	SetFinalOrphanCount(uint64)
}) {
	if store == nil {
		return
	}
	store.recoveryObserverMu.Lock()
	store.recoveryObserver = observer
	store.recoveryObserverMu.Unlock()
}

type Store interface {
	Stage(context.Context, canonical.ID, io.Reader) (Staged, error)
	Finalize(context.Context, canonical.ID, Staged) (Object, error)
	Acknowledge(context.Context, canonical.ID, Staged) error
	Exists(canonical.ID, canonical.Digest) (bool, error)
	Read(context.Context, canonical.ID, canonical.Digest) ([]byte, error)
	ListOrphans(context.Context) ([]Orphan, error)
	CleanupOrphan(context.Context, Orphan, ReferenceChecker) (CleanupResult, error)
}

var _ Store = (*FileStore)(nil)

type Staged struct {
	storeIdentity  fssecure.Identity
	parentIdentity fssecure.Identity
	name           string
	residentID     canonical.ID
	digest         canonical.Digest
	size           canonical.ByteSize
	modTime        time.Time
	identity       fssecure.Identity
}

func (staged Staged) ResidentID() canonical.ID { return staged.residentID }
func (staged Staged) Digest() canonical.Digest { return staged.digest }
func (staged Staged) Size() canonical.ByteSize { return staged.size }
func (staged Staged) TemporaryName() string    { return staged.name }

type Object struct {
	ResidentID canonical.ID
	Digest     canonical.Digest
	Size       canonical.ByteSize
}

// FinalObject is an opaque authority token for one validated object in the
// final content-addressed namespace.  The exported accessors are descriptive;
// RemoveFinal reopens the complete handle-relative chain and requires every
// private identity to match before it can unlink anything.
type FinalObject struct {
	residentID canonical.ID
	digest     canonical.Digest
	size       canonical.ByteSize

	storeIdentity    fssecure.Identity
	objectsIdentity  fssecure.Identity
	residentIdentity fssecure.Identity
	shardIdentity    fssecure.Identity
	fileIdentity     fssecure.Identity
	shardName        string
	fileName         string
}

func (object FinalObject) ResidentID() canonical.ID { return object.residentID }
func (object FinalObject) Digest() canonical.Digest { return object.digest }
func (object FinalObject) Size() canonical.ByteSize { return object.size }
func (object FinalObject) HashAlgorithm() string    { return canonical.HashAlgorithm }

type OrphanKind string

const (
	OrphanPartial OrphanKind = "partial"
	OrphanSealed  OrphanKind = "sealed"
)

type orphanLocation uint8

const (
	orphanInStaging orphanLocation = iota + 1
	orphanInTransientQuarantine
)

// Orphan is an opaque recovery token. Exported metadata is informational;
// CleanupOrphan verifies all private identity and binding fields again.
type Orphan struct {
	Name       string
	Kind       OrphanKind
	ResidentID canonical.ID
	Digest     canonical.Digest
	Size       int64
	ModTime    time.Time

	storeIdentity  fssecure.Identity
	parentIdentity fssecure.Identity
	identity       fssecure.Identity
	location       orphanLocation
	physicalName   string
}

type ReferenceChecker interface {
	BlobReferenced(context.Context, canonical.ID, canonical.Digest) (bool, error)
}

type ReferenceCheckFunc func(context.Context, canonical.ID, canonical.Digest) (bool, error)

func (check ReferenceCheckFunc) BlobReferenced(ctx context.Context, residentID canonical.ID, digest canonical.Digest) (bool, error) {
	return check(ctx, residentID, digest)
}

type CleanupDisposition string

const (
	CleanupPartialRemoved            CleanupDisposition = "partial_removed"
	CleanupUnpublishedStageRemoved   CleanupDisposition = "unpublished_stage_removed"
	CleanupUnreferencedObjectRemoved CleanupDisposition = "unreferenced_object_removed"
	CleanupReferencedObjectPreserved CleanupDisposition = "referenced_object_preserved"
)

type CleanupResult struct {
	Disposition CleanupDisposition
	ResidentID  canonical.ID
	Digest      canonical.Digest
}

type storeLayout struct {
	root, staging, objects, quarantine *fssecure.Directory
}

func (layout *storeLayout) Close() error {
	if layout == nil {
		return nil
	}
	return errors.Join(layout.quarantine.Close(), layout.objects.Close(), layout.staging.Close(), layout.root.Close())
}

func (layout *storeLayout) VerifyBound() error {
	if layout == nil || layout.root == nil || layout.staging == nil || layout.objects == nil || layout.quarantine == nil {
		return fssecure.ErrUnsafeFilesystem
	}
	for _, directory := range []*fssecure.Directory{layout.root, layout.staging, layout.objects, layout.quarantine} {
		if err := directory.VerifyBound(); err != nil {
			return err
		}
	}
	return nil
}

func NewFileStore(root string) (*FileStore, error) {
	return openFileStore(root, false, true)
}

// OpenFileStoreReadOnly captures the exact existing layout without creating
// or repairing any source entry. Backup and offline verification use this
// authority so inspection cannot mutate the live blob namespace.
func OpenFileStoreReadOnly(root string) (*FileStore, error) {
	return openFileStore(root, true, false)
}

// OpenFileStoreExisting opens an existing layout with delete/write authority
// but never initializes a missing source. Destructive offline commands use it
// only after acquiring the data-directory host lock.
func OpenFileStoreExisting(root string) (*FileStore, error) {
	return openFileStore(root, false, false)
}

func openFileStore(root string, readOnly, create bool) (*FileStore, error) {
	if strings.TrimSpace(root) == "" {
		return nil, fmt.Errorf("blob: empty root")
	}
	absolute, err := filepath.Abs(root)
	if err != nil {
		return nil, fmt.Errorf("blob: resolve root: %w", err)
	}
	absolute = filepath.Clean(absolute)
	policy, err := fssecure.CurrentSecurityPolicy()
	if err != nil {
		return nil, fmt.Errorf("blob: security policy: %w", err)
	}
	var rootHandle *fssecure.Directory
	if readOnly {
		rootHandle, err = fssecure.OpenRootReadOnly(absolute, policy)
	} else if !create {
		rootHandle, err = fssecure.OpenRoot(absolute, policy)
	} else {
		rootHandle, err = fssecure.OpenOrCreateRoot(absolute, policy)
	}
	if err != nil {
		return nil, fmt.Errorf("blob: open root: %w", errors.Join(ErrUnsafeFilesystem, err))
	}
	openDirectory := rootHandle.OpenOrCreateDirectory
	if !create {
		openDirectory = rootHandle.OpenDirectory
	}
	staging, err := openDirectory("staging")
	if err != nil {
		_ = rootHandle.Close()
		return nil, fmt.Errorf("blob: open staging: %w", errors.Join(ErrUnsafeFilesystem, err))
	}
	objects, err := openDirectory("objects")
	if err != nil {
		_ = staging.Close()
		_ = rootHandle.Close()
		return nil, fmt.Errorf("blob: open objects: %w", errors.Join(ErrUnsafeFilesystem, err))
	}
	quarantine, err := openDirectory("quarantine")
	if err != nil {
		_ = objects.Close()
		_ = staging.Close()
		_ = rootHandle.Close()
		return nil, fmt.Errorf("blob: open quarantine: %w", errors.Join(ErrUnsafeFilesystem, err))
	}
	store := &FileStore{
		root: absolute, stagingDir: filepath.Join(absolute, "staging"), objectsDir: filepath.Join(absolute, "objects"), quarantineDir: filepath.Join(absolute, "quarantine"),
		policy: policy, rootIdentity: rootHandle.Identity(), stagingIdentity: staging.Identity(), objectsIdentity: objects.Identity(), quarantineIdentity: quarantine.Identity(),
		readOnly: readOnly,
	}
	closeErr := errors.Join(quarantine.Close(), objects.Close(), staging.Close(), rootHandle.Close())
	if closeErr != nil {
		return nil, closeErr
	}
	if !store.rootIdentity.SameVolume(store.stagingIdentity) || !store.rootIdentity.SameVolume(store.objectsIdentity) || !store.rootIdentity.SameVolume(store.quarantineIdentity) {
		return nil, errors.Join(ErrUnsafeFilesystem, fssecure.WithReason(fssecure.ReasonUnsupportedFilesystem, errors.Join(fssecure.ErrUnsafeFilesystem, fssecure.ErrUnsupportedSecureFilesystem)))
	}
	return store, nil
}

func (store *FileStore) Root() string { return store.root }

func (store *FileStore) openLayout() (*storeLayout, error) {
	if store == nil || store.root == "" {
		return nil, ErrUnsafeFilesystem
	}
	var root *fssecure.Directory
	var err error
	if store.readOnly {
		root, err = fssecure.OpenRootReadOnly(store.root, store.policy)
	} else {
		root, err = fssecure.OpenRoot(store.root, store.policy)
	}
	if err != nil {
		return nil, fmt.Errorf("blob: validate root: %w", errors.Join(ErrUnsafeFilesystem, err))
	}
	if !root.Identity().Equal(store.rootIdentity) {
		_ = root.Close()
		return nil, layoutIdentityError("root")
	}
	staging, err := root.OpenDirectory("staging")
	if err != nil {
		_ = root.Close()
		return nil, fmt.Errorf("blob: validate staging: %w", errors.Join(ErrUnsafeFilesystem, err))
	}
	if !staging.Identity().Equal(store.stagingIdentity) {
		_ = staging.Close()
		_ = root.Close()
		return nil, layoutIdentityError("staging")
	}
	objects, err := root.OpenDirectory("objects")
	if err != nil {
		_ = staging.Close()
		_ = root.Close()
		return nil, fmt.Errorf("blob: validate objects: %w", errors.Join(ErrUnsafeFilesystem, err))
	}
	if !objects.Identity().Equal(store.objectsIdentity) {
		_ = objects.Close()
		_ = staging.Close()
		_ = root.Close()
		return nil, layoutIdentityError("objects")
	}
	quarantine, err := root.OpenDirectory("quarantine")
	if err != nil {
		_ = objects.Close()
		_ = staging.Close()
		_ = root.Close()
		return nil, fmt.Errorf("blob: validate quarantine: %w", errors.Join(ErrUnsafeFilesystem, err))
	}
	if !quarantine.Identity().Equal(store.quarantineIdentity) {
		_ = quarantine.Close()
		_ = objects.Close()
		_ = staging.Close()
		_ = root.Close()
		return nil, layoutIdentityError("quarantine")
	}
	layout := &storeLayout{root: root, staging: staging, objects: objects, quarantine: quarantine}
	if err := layout.VerifyBound(); err != nil {
		_ = layout.Close()
		return nil, fmt.Errorf("blob: validate complete layout: %w", errors.Join(ErrUnsafeFilesystem, err))
	}
	return layout, nil
}

func layoutIdentityError(component string) error {
	return fmt.Errorf("blob: %s identity changed: %w", component, errors.Join(ErrUnsafeFilesystem, fssecure.WithReason(fssecure.ReasonDirectoryIdentityChanged, errors.Join(fssecure.ErrUnsafeFilesystem, fssecure.ErrIdentityChanged))))
}

func (store *FileStore) Stage(ctx context.Context, residentID canonical.ID, source io.Reader) (Staged, error) {
	if ctx == nil {
		return Staged{}, fmt.Errorf("blob: nil stage context")
	}
	if err := residentID.Validate(); err != nil {
		return Staged{}, fmt.Errorf("blob: invalid resident scope: %w", err)
	}
	if source == nil {
		return Staged{}, fmt.Errorf("blob: nil stage source")
	}
	layout, err := store.openLayout()
	if err != nil {
		return Staged{}, err
	}
	defer layout.Close()
	handle, err := layout.staging.CreateTemp("partial-")
	if err != nil {
		return Staged{}, fmt.Errorf("blob: create staging file: %w", err)
	}
	defer handle.Close()
	hasher := sha256.New()
	size, copyErr := io.Copy(io.MultiWriter(handle.File(), hasher), &contextReader{ctx: ctx, reader: source})
	sealErr := handle.Seal()
	if copyErr != nil || sealErr != nil {
		return Staged{}, errors.Join(copyErr, sealErr)
	}
	logicalSize, err := canonical.NewByteSize(size)
	if err != nil {
		return Staged{}, err
	}
	digest, err := canonical.DigestFromBytes(hasher.Sum(nil))
	if err != nil {
		return Staged{}, err
	}
	sealed := sealedName(residentID, digest, handle.Name())
	if err := layout.staging.PublishNoReplace(handle, sealed); err != nil {
		return Staged{}, fmt.Errorf("blob: seal staged object: %w", err)
	}
	info := handle.Snapshot()
	return Staged{
		storeIdentity: store.rootIdentity, parentIdentity: store.stagingIdentity, name: sealed,
		residentID: residentID, digest: digest, size: logicalSize, modTime: info.ModTime(), identity: handle.Identity(),
	}, nil
}

func (store *FileStore) StageBytes(ctx context.Context, residentID canonical.ID, logicalContent []byte) (Staged, error) {
	return store.Stage(ctx, residentID, bytes.NewReader(logicalContent))
}

type objectLocation struct {
	resident *fssecure.Directory
	shard    *fssecure.Directory
	base     string
}

func (location *objectLocation) Close() error {
	if location == nil {
		return nil
	}
	return errors.Join(location.shard.Close(), location.resident.Close())
}

func openObjectLocation(objects *fssecure.Directory, residentID canonical.ID, digest canonical.Digest, create bool) (*objectLocation, bool, error) {
	var resident, shard *fssecure.Directory
	var err error
	if create {
		resident, err = objects.OpenOrCreateDirectory(residentID.String())
	} else {
		resident, err = objects.OpenDirectory(residentID.String())
	}
	if errors.Is(err, os.ErrNotExist) {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, err
	}
	if create {
		shard, err = resident.OpenOrCreateDirectory(digest.Hex()[:2])
	} else {
		shard, err = resident.OpenDirectory(digest.Hex()[:2])
	}
	if errors.Is(err, os.ErrNotExist) {
		_ = resident.Close()
		return nil, false, nil
	}
	if err != nil {
		_ = resident.Close()
		return nil, false, err
	}
	return &objectLocation{resident: resident, shard: shard, base: digest.Hex()[2:]}, true, nil
}

func (store *FileStore) Finalize(ctx context.Context, residentID canonical.ID, staged Staged) (Object, error) {
	if ctx == nil {
		return Object{}, fmt.Errorf("blob: nil finalize context")
	}
	if err := ctx.Err(); err != nil {
		return Object{}, err
	}
	layout, err := store.openLayout()
	if err != nil {
		return Object{}, err
	}
	defer layout.Close()
	if err := store.validateStageToken(residentID, staged); err != nil {
		return Object{}, err
	}
	store.finalizeMu.Lock()
	defer store.finalizeMu.Unlock()
	source, err := layout.staging.OpenRegular(staged.name)
	if err != nil {
		return Object{}, errors.Join(ErrInvalidStage, err)
	}
	defer source.Close()
	if err := verifyStagedHandle(ctx, source, staged); err != nil {
		return Object{}, err
	}
	store.invokeTestHook("finalize_after_source_hash")
	if err := layout.VerifyBound(); err != nil {
		return Object{}, fmt.Errorf("blob: layout changed during finalize: %w", errors.Join(ErrUnsafeFilesystem, err))
	}
	if err := source.VerifyBound(); err != nil {
		return Object{}, fmt.Errorf("blob: staged source changed during finalize: %w", errors.Join(ErrUnsafeFilesystem, err))
	}
	location, _, err := openObjectLocation(layout.objects, residentID, staged.digest, true)
	if err != nil {
		return Object{}, err
	}
	defer location.Close()
	lock, err := location.shard.AcquireNamespace(location.base)
	if err != nil {
		return Object{}, fmt.Errorf("blob: acquire object namespace: %w", err)
	}
	defer lock.Close()
	existing, exists, err := lock.OpenTarget()
	if err != nil {
		return Object{}, err
	}
	if exists {
		defer existing.Close()
		if err := verifyExpectedContent(ctx, existing, staged.digest, staged.size.Int64()); err != nil {
			if !errors.Is(err, ErrDigestMismatch) {
				return Object{}, err
			}
			return Object{}, store.quarantineObjectMismatch(layout, location, existing, residentID, staged.digest, err)
		}
		return Object{ResidentID: residentID, Digest: staged.digest, Size: staged.size}, nil
	}
	destination, err := store.publishFromHandle(ctx, source, location.shard, location.base, staged.digest, staged.size.Int64())
	if err != nil {
		return Object{}, fmt.Errorf("blob: atomically finalize object: %w", err)
	}
	defer destination.Close()
	store.invokeTestHook("finalize_after_publish")
	if store.testMutatePublished != nil {
		if err := store.testMutatePublished(destination); err != nil {
			return Object{}, fmt.Errorf("blob: mutate published object test hook: %w", err)
		}
	}
	if err := verifyExpectedContent(ctx, destination, staged.digest, staged.size.Int64()); err != nil {
		if !errors.Is(err, ErrDigestMismatch) {
			return Object{}, err
		}
		return Object{}, store.quarantineObjectMismatch(layout, location, destination, residentID, staged.digest, err)
	}
	if err := source.VerifyBound(); err != nil {
		return Object{}, fmt.Errorf("blob: staged source changed during publication: %w", err)
	}
	return Object{ResidentID: residentID, Digest: staged.digest, Size: staged.size}, nil
}

func (store *FileStore) publishFromHandle(ctx context.Context, source *fssecure.Handle, destination *fssecure.Directory, name string, expected canonical.Digest, expectedSize int64) (_ *fssecure.Handle, returnErr error) {
	if err := source.VerifyBound(); err != nil {
		return nil, err
	}
	temporary, err := destination.CreateTemp(".mahoroba-publish-")
	if err != nil {
		return nil, err
	}
	published := false
	defer func() {
		if published {
			return
		}
		sealErr := temporary.Seal()
		deleteErr := temporary.MarkDeleteOnClose()
		closeErr := temporary.Close()
		syncErr := destination.Sync()
		returnErr = errors.Join(returnErr, sealErr, deleteErr, closeErr, syncErr)
	}()
	if _, err := source.File().Seek(0, io.SeekStart); err != nil {
		return nil, err
	}
	if _, err := io.Copy(temporary.File(), &contextReader{ctx: ctx, reader: source.File()}); err != nil {
		return nil, err
	}
	if err := temporary.Seal(); err != nil {
		return nil, err
	}
	if err := verifyExpectedContent(ctx, temporary, expected, expectedSize); err != nil {
		return nil, err
	}
	if err := destination.PublishNoReplace(temporary, name); err != nil {
		return nil, err
	}
	if store.testAfterPublishError != nil {
		return nil, store.testAfterPublishError
	}
	published = true
	return temporary, nil
}

func (store *FileStore) quarantineObjectMismatch(layout *storeLayout, location *objectLocation, object *fssecure.Handle, residentID canonical.ID, digest canonical.Digest, mismatch error) error {
	if !errors.Is(mismatch, ErrDigestMismatch) {
		return mismatch
	}
	// A policy-valid in-place writer can change the sealed snapshot on POSIX.
	// Refresh only after rechecking the same handle and binding. A binding or
	// identity failure is not content evidence and must not authorize any move
	// or delete of either the old handle or the current namespace entry.
	if err := object.Seal(); err != nil {
		return errors.Join(ErrDigestMismatch, mismatch, ErrUnsafeFilesystem, err)
	}
	name, err := store.evidenceName("evidence-object-" + residentID.String() + "-" + digest.Hex() + "-")
	if err != nil {
		return errors.Join(ErrDigestMismatch, mismatch, err)
	}
	if err := location.shard.MoveTo(object, layout.quarantine, name); err != nil {
		return errors.Join(ErrDigestMismatch, mismatch, ErrUnsafeFilesystem, err)
	}
	store.invokeTestHook("quarantine_after_move")
	if err := object.VerifyBound(); err != nil {
		return errors.Join(ErrDigestMismatch, mismatch, ErrUnsafeFilesystem, err)
	}
	return fmt.Errorf("%w: object moved to persistent evidence %q: %v", ErrDigestMismatch, name, mismatch)
}

func (store *FileStore) evidenceName(prefix string) (string, error) {
	if store.testEvidenceName != nil {
		return store.testEvidenceName(prefix), nil
	}
	var nonce [16]byte
	if _, err := rand.Read(nonce[:]); err != nil {
		return "", err
	}
	return prefix + hex.EncodeToString(nonce[:]), nil
}

func (store *FileStore) Acknowledge(ctx context.Context, residentID canonical.ID, staged Staged) error {
	if ctx == nil {
		return fmt.Errorf("blob: nil acknowledge context")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	layout, err := store.openLayout()
	if err != nil {
		return err
	}
	defer layout.Close()
	if err := store.validateStageToken(residentID, staged); err != nil {
		return err
	}
	store.finalizeMu.Lock()
	defer store.finalizeMu.Unlock()
	location, exists, err := openObjectLocation(layout.objects, residentID, staged.digest, false)
	if err != nil || !exists {
		return fmt.Errorf("blob: verify finalized object before acknowledge: %w", errors.Join(err, ErrObjectNotFound))
	}
	defer location.Close()
	object, err := location.shard.OpenRegular(location.base)
	if err != nil {
		return fmt.Errorf("blob: verify finalized object before acknowledge: %w", err)
	}
	if err := verifyExpectedContent(ctx, object, staged.digest, staged.size.Int64()); err != nil {
		mismatchErr := err
		if errors.Is(err, ErrDigestMismatch) {
			mismatchErr = store.quarantineObjectMismatch(layout, location, object, residentID, staged.digest, err)
		}
		_ = object.Close()
		return fmt.Errorf("blob: verify finalized object before acknowledge: %w", mismatchErr)
	}
	_ = object.Close()
	transientName := "delete-stage-" + staged.name
	transient, transientExists, err := openOptional(layout.quarantine, transientName)
	if err != nil {
		return err
	}
	if transientExists {
		if err := verifyStagedHandle(ctx, transient, staged); err != nil {
			_ = transient.Close()
			return fmt.Errorf("blob: acknowledged-stage quarantine collision: %w", err)
		}
		if err := store.deleteTransient(layout.quarantine, transient, "acknowledge_before_delete"); err != nil {
			return err
		}
		return nil
	}
	marker, markerExists, err := openOptional(layout.staging, staged.name)
	if err != nil {
		return err
	}
	if !markerExists {
		return nil
	}
	if err := verifyStagedHandle(ctx, marker, staged); err != nil {
		_ = marker.Close()
		return err
	}
	store.invokeTestHook("acknowledge_before_remove")
	if store.testMutateStage != nil {
		if err := store.testMutateStage(marker); err != nil {
			_ = marker.Close()
			return fmt.Errorf("blob: mutate acknowledged stage test hook: %w", err)
		}
	}
	if err := verifyStagedHandle(ctx, marker, staged); err != nil {
		_ = marker.Close()
		return err
	}
	if err := layout.staging.MoveTo(marker, layout.quarantine, transientName); err != nil {
		_ = marker.Close()
		return fmt.Errorf("blob: quarantine acknowledged stage: %w", errors.Join(ErrUnsafeFilesystem, err))
	}
	if err := store.invokeFailpoint("acknowledge_after_quarantine"); err != nil {
		_ = marker.Close()
		return err
	}
	return store.deleteTransient(layout.quarantine, marker, "acknowledge_before_delete")
}

func (store *FileStore) Exists(residentID canonical.ID, digest canonical.Digest) (bool, error) {
	layout, err := store.openLayout()
	if err != nil {
		return false, err
	}
	defer layout.Close()
	location, exists, err := openObjectLocation(layout.objects, residentID, digest, false)
	if err != nil || !exists {
		return false, err
	}
	defer location.Close()
	handle, err := location.shard.OpenRegularRead(location.base)
	if errors.Is(err, os.ErrNotExist) {
		return false, nil
	}
	if err != nil {
		return false, errors.Join(ErrUnsafeFilesystem, err)
	}
	defer handle.Close()
	return true, handle.VerifyBound()
}

func (store *FileStore) Open(ctx context.Context, residentID canonical.ID, digest canonical.Digest) (io.ReadCloser, error) {
	if ctx == nil {
		return nil, fmt.Errorf("blob: nil open context")
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	layout, err := store.openLayout()
	if err != nil {
		return nil, err
	}
	location, exists, err := openObjectLocation(layout.objects, residentID, digest, false)
	if err != nil || !exists {
		_ = layout.Close()
		return nil, errors.Join(ErrObjectNotFound, err)
	}
	handle, err := location.shard.OpenRegularRead(location.base)
	_ = location.Close()
	_ = layout.Close()
	if errors.Is(err, os.ErrNotExist) {
		return nil, ErrObjectNotFound
	}
	if err != nil {
		return nil, errors.Join(ErrUnsafeFilesystem, err)
	}
	return &verifiedReader{handle: handle}, nil
}

type verifiedReader struct{ handle *fssecure.Handle }

func (reader *verifiedReader) Read(p []byte) (int, error) { return reader.handle.File().Read(p) }
func (reader *verifiedReader) Close() error {
	return errors.Join(reader.handle.VerifyBound(), reader.handle.Close())
}

func (store *FileStore) Read(ctx context.Context, residentID canonical.ID, digest canonical.Digest) ([]byte, error) {
	reader, err := store.Open(ctx, residentID, digest)
	if err != nil {
		return nil, err
	}
	content, readErr := io.ReadAll(&contextReader{ctx: ctx, reader: reader})
	closeErr := reader.Close()
	if readErr != nil || closeErr != nil {
		return nil, errors.Join(readErr, closeErr)
	}
	if actual := canonical.HashBlob(content); actual != digest {
		return nil, fmt.Errorf("%w: requested=%s/%s actual=%s", ErrDigestMismatch, residentID, digest, actual)
	}
	return content, nil
}

// WalkFinal enumerates one resident's final objects through validated
// directory and file handles. Unknown namespace entries fail closed: GC must
// never reinterpret an arbitrary path as a content-addressed object.
func (store *FileStore) WalkFinal(ctx context.Context, residentID canonical.ID) ([]FinalObject, error) {
	if ctx == nil {
		return nil, errors.New("blob: nil final-object walk context")
	}
	if err := residentID.Validate(); err != nil {
		return nil, fmt.Errorf("blob: invalid final-object resident: %w", err)
	}
	layout, err := store.openLayout()
	if err != nil {
		return nil, err
	}
	defer layout.Close()
	resident, err := layout.objects.OpenDirectory(residentID.String())
	if errors.Is(err, os.ErrNotExist) {
		return []FinalObject{}, nil
	}
	if err != nil {
		return nil, fmt.Errorf("blob: open final-object resident: %w", errors.Join(ErrUnsafeFilesystem, err))
	}
	defer resident.Close()

	shards, err := resident.ReadDir()
	if err != nil {
		return nil, fmt.Errorf("blob: enumerate final-object shards: %w", err)
	}
	result := make([]FinalObject, 0)
	for _, shardEntry := range shards {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		shardName := shardEntry.Name()
		if len(shardName) != 2 || !isLowerHex(shardName) {
			return nil, fmt.Errorf("%w: invalid final-object shard entry", ErrUnsafeFilesystem)
		}
		shard, err := resident.OpenDirectory(shardName)
		if err != nil {
			return nil, fmt.Errorf("blob: open final-object shard: %w", errors.Join(ErrUnsafeFilesystem, err))
		}
		entries, readErr := shard.ReadDir()
		if readErr != nil {
			_ = shard.Close()
			return nil, fmt.Errorf("blob: enumerate final-object shard: %w", readErr)
		}
		for _, entry := range entries {
			if err := ctx.Err(); err != nil {
				_ = shard.Close()
				return nil, err
			}
			fileName := entry.Name()
			if lockTarget, lockEntry := fssecure.NamespaceLockTargetBasename(fileName); lockEntry {
				// Finalize intentionally retains one empty rendezvous entry for
				// every content-addressed target. It is managed namespace metadata,
				// not a blob candidate. Ignore it only after the encoded target is
				// valid for this shard and a protected regular handle proves the
				// exact ACL/kind plus an empty body. Unknown or malformed entries
				// continue to fail closed below.
				if len(lockTarget) != 62 || !isLowerHex(lockTarget) {
					_ = shard.Close()
					return nil, fmt.Errorf("%w: invalid final-object namespace lock", ErrUnsafeFilesystem)
				}
				lockHandle, err := shard.OpenRegularRead(fileName)
				if err != nil {
					_ = shard.Close()
					return nil, fmt.Errorf("blob: open final-object namespace lock: %w", errors.Join(ErrUnsafeFilesystem, err))
				}
				_, size, hashErr := lockHandle.Hash(ctx)
				closeErr := lockHandle.Close()
				if hashErr != nil || closeErr != nil || size != 0 {
					_ = shard.Close()
					return nil, fmt.Errorf("%w: invalid final-object namespace lock body: %v", ErrUnsafeFilesystem, errors.Join(hashErr, closeErr))
				}
				continue
			}
			if len(fileName) != 62 || !isLowerHex(fileName) {
				_ = shard.Close()
				return nil, fmt.Errorf("%w: invalid final-object entry", ErrUnsafeFilesystem)
			}
			digest, err := canonical.ParseDigestHex(shardName + fileName)
			if err != nil {
				_ = shard.Close()
				return nil, fmt.Errorf("%w: invalid final-object locator", ErrUnsafeFilesystem)
			}
			handle, err := shard.OpenRegularRead(fileName)
			if err != nil {
				_ = shard.Close()
				return nil, fmt.Errorf("blob: open final object: %w", errors.Join(ErrUnsafeFilesystem, err))
			}
			hash, size, hashErr := handle.Hash(ctx)
			if hashErr != nil {
				_ = handle.Close()
				_ = shard.Close()
				return nil, fmt.Errorf("blob: hash final object: %w", hashErr)
			}
			actual, digestErr := canonical.DigestFromBytes(hash[:])
			byteSize, sizeErr := canonical.NewByteSize(size)
			if digestErr != nil || sizeErr != nil || actual != digest {
				_ = handle.Close()
				_ = shard.Close()
				return nil, fmt.Errorf("%w: final-object locator/content mismatch", errors.Join(ErrDigestMismatch, digestErr, sizeErr))
			}
			result = append(result, FinalObject{
				residentID: residentID, digest: digest, size: byteSize,
				storeIdentity: store.rootIdentity, objectsIdentity: store.objectsIdentity,
				residentIdentity: resident.Identity(), shardIdentity: shard.Identity(), fileIdentity: handle.Identity(),
				shardName: shardName, fileName: fileName,
			})
			if err := handle.Close(); err != nil {
				_ = shard.Close()
				return nil, err
			}
		}
		if err := shard.Close(); err != nil {
			return nil, err
		}
	}
	sort.Slice(result, func(left, right int) bool {
		return result[left].digest.Hex() < result[right].digest.Hex()
	})
	for index := 1; index < len(result); index++ {
		if result[index-1].digest == result[index].digest {
			return nil, fmt.Errorf("%w: duplicate final-object locator", ErrUnsafeFilesystem)
		}
	}
	return result, nil
}

// RemoveFinal removes only the final object represented by a token returned
// by WalkFinal. Missing is idempotent. Any changed identity, policy, kind,
// size, or digest is an error and authorizes no deletion of the replacement.
// removed can be true with a non-nil error when unlink succeeded but its
// durability barrier failed; callers must then report partial progress.
func (store *FileStore) RemoveFinal(ctx context.Context, object FinalObject) (removed bool, returnErr error) {
	if ctx == nil {
		return false, errors.New("blob: nil final-object remove context")
	}
	if err := ctx.Err(); err != nil {
		return false, err
	}
	if store == nil || store.readOnly || object.storeIdentity.Valid() == false ||
		!object.storeIdentity.Equal(store.rootIdentity) || !object.objectsIdentity.Equal(store.objectsIdentity) ||
		object.residentID.IsZero() || object.digest == (canonical.Digest{}) ||
		object.shardName != object.digest.Hex()[:2] || object.fileName != object.digest.Hex()[2:] {
		return false, fmt.Errorf("%w: invalid final-object token", ErrUnsafeFilesystem)
	}
	if err := object.residentID.Validate(); err != nil || object.size.Validate() != nil {
		return false, fmt.Errorf("%w: invalid final-object token metadata", ErrUnsafeFilesystem)
	}
	layout, err := store.openLayout()
	if err != nil {
		return false, err
	}
	defer layout.Close()
	location, exists, err := openObjectLocation(layout.objects, object.residentID, object.digest, false)
	if err != nil || !exists {
		return false, err
	}
	defer location.Close()
	if !location.resident.Identity().Equal(object.residentIdentity) || !location.shard.Identity().Equal(object.shardIdentity) {
		return false, fmt.Errorf("%w: final-object directory identity changed", ErrUnsafeFilesystem)
	}
	handle, err := location.shard.OpenRegular(location.base)
	if errors.Is(err, os.ErrNotExist) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("blob: reopen final object for removal: %w", errors.Join(ErrUnsafeFilesystem, err))
	}
	defer func() { returnErr = errors.Join(returnErr, handle.Close()) }()
	if !handle.Identity().Equal(object.fileIdentity) || handle.Snapshot() == nil || handle.Snapshot().Size() != object.size.Int64() {
		return false, fmt.Errorf("%w: final-object identity or size changed", ErrUnsafeFilesystem)
	}
	hash, size, err := handle.Hash(ctx)
	if err != nil {
		return false, err
	}
	actual, err := canonical.DigestFromBytes(hash[:])
	if err != nil || actual != object.digest || size != object.size.Int64() {
		return false, fmt.Errorf("%w: final-object content changed", errors.Join(ErrDigestMismatch, err))
	}
	if err := handle.MarkDeleteOnClose(); err != nil {
		// MarkDeleteOnClose performs unlink before its durability barrier. Treat
		// any error conservatively as a possible physical mutation.
		return true, fmt.Errorf("blob: remove final object: %w", err)
	}
	return true, nil
}

func isLowerHex(value string) bool {
	for _, character := range value {
		if (character < '0' || character > '9') && (character < 'a' || character > 'f') {
			return false
		}
	}
	return value != ""
}

func (store *FileStore) ListOrphans(ctx context.Context) ([]Orphan, error) {
	if ctx == nil {
		return nil, fmt.Errorf("blob: nil orphan context")
	}
	layout, err := store.openLayout()
	if err != nil {
		return nil, err
	}
	defer layout.Close()
	result := make([]Orphan, 0)
	seen := make(map[string]struct{})
	if err := store.collectOrphans(ctx, layout.staging, orphanInStaging, "", store.stagingIdentity, seen, &result); err != nil {
		return nil, err
	}
	if err := store.collectOrphans(ctx, layout.quarantine, orphanInTransientQuarantine, "delete-stage-", store.quarantineIdentity, seen, &result); err != nil {
		return nil, err
	}
	if err := store.collectOrphans(ctx, layout.quarantine, orphanInTransientQuarantine, "delete-partial-", store.quarantineIdentity, seen, &result); err != nil {
		return nil, err
	}
	sort.Slice(result, func(i, j int) bool { return result[i].Name < result[j].Name })
	store.recoveryObserverMu.RLock()
	observer := store.recoveryObserver
	store.recoveryObserverMu.RUnlock()
	if observer != nil {
		observer.SetStagingOrphanCount(uint64(len(result)))
	}
	return result, nil
}

func (store *FileStore) collectOrphans(ctx context.Context, directory *fssecure.Directory, location orphanLocation, prefix string, parentIdentity fssecure.Identity, seen map[string]struct{}, result *[]Orphan) error {
	entries, err := directory.ReadDir()
	if err != nil {
		return err
	}
	for _, entry := range entries {
		if err := ctx.Err(); err != nil {
			return err
		}
		physicalName := entry.Name()
		logicalName := physicalName
		if prefix != "" {
			if !strings.HasPrefix(physicalName, prefix) {
				continue
			}
			logicalName = strings.TrimPrefix(physicalName, prefix)
		}
		kind := OrphanKind("")
		residentID, digest, sealed := parseSealedName(logicalName)
		if isPartialName(logicalName) {
			kind = OrphanPartial
		} else if sealed {
			kind = OrphanSealed
		} else {
			continue
		}
		if _, duplicate := seen[logicalName]; duplicate {
			return fmt.Errorf("%w: duplicate recovery location for %q", ErrUnsafeFilesystem, logicalName)
		}
		handle, err := directory.OpenRegular(physicalName)
		if err != nil {
			return fmt.Errorf("blob: known recovery entry is unsafe: %w", errors.Join(ErrUnsafeFilesystem, err))
		}
		info := handle.Snapshot()
		candidate := Orphan{
			Name: logicalName, Kind: kind, ResidentID: residentID, Digest: digest, Size: info.Size(), ModTime: info.ModTime(),
			storeIdentity: store.rootIdentity, parentIdentity: parentIdentity, identity: handle.Identity(), location: location, physicalName: physicalName,
		}
		_ = handle.Close()
		seen[logicalName] = struct{}{}
		*result = append(*result, candidate)
	}
	return nil
}

func (store *FileStore) CleanupOrphan(ctx context.Context, orphan Orphan, references ReferenceChecker) (CleanupResult, error) {
	if ctx == nil {
		return CleanupResult{}, fmt.Errorf("blob: nil cleanup context")
	}
	if err := ctx.Err(); err != nil {
		return CleanupResult{}, err
	}
	layout, err := store.openLayout()
	if err != nil {
		return CleanupResult{}, err
	}
	defer layout.Close()
	if err := store.validateOrphanToken(orphan); err != nil {
		return CleanupResult{}, err
	}
	store.finalizeMu.Lock()
	defer store.finalizeMu.Unlock()
	directory := layout.staging
	if orphan.location == orphanInTransientQuarantine {
		directory = layout.quarantine
	}
	candidate, err := directory.OpenRegular(orphan.physicalName)
	if err != nil {
		return CleanupResult{}, errors.Join(ErrInvalidOrphan, err)
	}
	if err := verifyOrphanHandle(candidate, orphan); err != nil {
		_ = candidate.Close()
		return CleanupResult{}, err
	}
	if orphan.Kind == OrphanPartial {
		if orphan.location == orphanInStaging {
			name := "delete-partial-" + orphan.Name
			if err := layout.staging.MoveTo(candidate, layout.quarantine, name); err != nil {
				_ = candidate.Close()
				return CleanupResult{}, errors.Join(ErrUnsafeFilesystem, err)
			}
		}
		if err := store.deleteTransient(layout.quarantine, candidate, "cleanup_partial_before_delete"); err != nil {
			return CleanupResult{}, err
		}
		return CleanupResult{Disposition: CleanupPartialRemoved}, nil
	}
	residentID, expectedDigest, ok := parseSealedName(orphan.Name)
	if !ok {
		_ = candidate.Close()
		return CleanupResult{}, ErrInvalidOrphan
	}
	if err := verifyExpectedContent(ctx, candidate, expectedDigest, orphan.Size); err != nil {
		_ = candidate.Close()
		return CleanupResult{}, err
	}
	if references == nil {
		_ = candidate.Close()
		return CleanupResult{}, fmt.Errorf("blob: reference checker is required for a sealed orphan")
	}
	referenced, err := references.BlobReferenced(ctx, residentID, expectedDigest)
	if err != nil {
		_ = candidate.Close()
		return CleanupResult{}, fmt.Errorf("blob: check Canonical reference: %w", err)
	}
	_ = candidate.Close()
	store.invokeTestHook("cleanup_after_reference_check")
	candidate, err = directory.OpenRegular(orphan.physicalName)
	if err != nil {
		return CleanupResult{}, errors.Join(ErrInvalidOrphan, err)
	}
	if err := verifyOrphanHandle(candidate, orphan); err != nil {
		_ = candidate.Close()
		return CleanupResult{}, err
	}
	if err := verifyExpectedContent(ctx, candidate, expectedDigest, orphan.Size); err != nil {
		_ = candidate.Close()
		return CleanupResult{}, err
	}
	result := CleanupResult{ResidentID: residentID, Digest: expectedDigest}
	location, locationExists, err := openObjectLocation(layout.objects, residentID, expectedDigest, false)
	if err != nil {
		_ = candidate.Close()
		return CleanupResult{}, err
	}
	if locationExists {
		defer location.Close()
	}
	var object *fssecure.Handle
	removedObject := false
	if locationExists {
		object, _, err = openOptional(location.shard, location.base)
		if err != nil {
			_ = candidate.Close()
			return CleanupResult{}, err
		}
	}
	objectTransientName := "delete-object-" + residentID.String() + "-" + expectedDigest.Hex()
	if object == nil {
		transientObject, transientExists, transientErr := openOptional(layout.quarantine, objectTransientName)
		if transientErr != nil {
			_ = candidate.Close()
			return CleanupResult{}, transientErr
		}
		if transientExists {
			if err := verifyExpectedContent(ctx, transientObject, expectedDigest, orphan.Size); err != nil {
				_ = transientObject.Close()
				_ = candidate.Close()
				return CleanupResult{}, errors.Join(ErrUnsafeFilesystem, err)
			}
			if referenced {
				if !locationExists {
					location, _, err = openObjectLocation(layout.objects, residentID, expectedDigest, true)
					if err != nil {
						_ = transientObject.Close()
						_ = candidate.Close()
						return CleanupResult{}, err
					}
					defer location.Close()
				}
				if err := layout.quarantine.MoveTo(transientObject, location.shard, location.base); err != nil {
					_ = transientObject.Close()
					_ = candidate.Close()
					return CleanupResult{}, err
				}
				object = transientObject
			} else {
				if err := store.deleteTransient(layout.quarantine, transientObject, "cleanup_object_before_delete"); err != nil {
					_ = candidate.Close()
					return CleanupResult{}, err
				}
				removedObject = true
			}
		}
	}
	if object == nil && referenced {
		if !locationExists {
			location, _, err = openObjectLocation(layout.objects, residentID, expectedDigest, true)
			if err != nil {
				_ = candidate.Close()
				return CleanupResult{}, err
			}
			defer location.Close()
		}
		object, err = store.publishFromHandle(ctx, candidate, location.shard, location.base, expectedDigest, orphan.Size)
		if err != nil {
			_ = candidate.Close()
			return CleanupResult{}, fmt.Errorf("blob: restore referenced object: %w", err)
		}
	}
	if object != nil {
		if err := verifyExpectedContent(ctx, object, expectedDigest, orphan.Size); err != nil {
			mismatchErr := err
			if errors.Is(err, ErrDigestMismatch) {
				mismatchErr = store.quarantineObjectMismatch(layout, location, object, residentID, expectedDigest, err)
			}
			_ = object.Close()
			_ = candidate.Close()
			return CleanupResult{}, mismatchErr
		}
		if referenced {
			_ = object.Close()
			if err := store.removeOrphanMarker(layout, candidate, orphan); err != nil {
				return CleanupResult{}, err
			}
			result.Disposition = CleanupReferencedObjectPreserved
			return result, nil
		}
		if err := location.shard.MoveTo(object, layout.quarantine, objectTransientName); err != nil {
			_ = object.Close()
			_ = candidate.Close()
			return CleanupResult{}, errors.Join(ErrUnsafeFilesystem, err)
		}
		if err := store.invokeFailpoint("cleanup_after_object_quarantine"); err != nil {
			_ = object.Close()
			_ = candidate.Close()
			return CleanupResult{}, err
		}
		if err := store.deleteTransient(layout.quarantine, object, "cleanup_object_before_delete"); err != nil {
			_ = candidate.Close()
			return CleanupResult{}, err
		}
		removedObject = true
		if err := store.invokeFailpoint("cleanup_after_object_delete"); err != nil {
			_ = candidate.Close()
			return CleanupResult{}, err
		}
	}
	if err := store.removeOrphanMarker(layout, candidate, orphan); err != nil {
		return CleanupResult{}, err
	}
	if referenced {
		result.Disposition = CleanupReferencedObjectPreserved
	} else if !removedObject {
		result.Disposition = CleanupUnpublishedStageRemoved
	} else {
		result.Disposition = CleanupUnreferencedObjectRemoved
	}
	return result, nil
}

func (store *FileStore) removeOrphanMarker(layout *storeLayout, marker *fssecure.Handle, orphan Orphan) error {
	if orphan.location == orphanInStaging {
		name := "delete-stage-" + orphan.Name
		if err := layout.staging.MoveTo(marker, layout.quarantine, name); err != nil {
			_ = marker.Close()
			return errors.Join(ErrUnsafeFilesystem, err)
		}
	}
	return store.deleteTransient(layout.quarantine, marker, "cleanup_stage_before_delete")
}

func (store *FileStore) deleteTransient(directory *fssecure.Directory, handle *fssecure.Handle, failpoint string) error {
	if err := handle.VerifyBound(); err != nil {
		_ = handle.Close()
		return errors.Join(ErrUnsafeFilesystem, err)
	}
	if err := store.invokeFailpoint(failpoint); err != nil {
		_ = handle.Close()
		return err
	}
	if err := handle.MarkDeleteOnClose(); err != nil {
		_ = handle.Close()
		return errors.Join(ErrUnsafeFilesystem, err)
	}
	if err := handle.Close(); err != nil {
		return err
	}
	return directory.Sync()
}

func (store *FileStore) validateStageToken(residentID canonical.ID, staged Staged) error {
	if err := residentID.Validate(); err != nil {
		return errors.Join(ErrInvalidStage, err)
	}
	parsedResident, parsedDigest, ok := parseSealedName(staged.name)
	if !staged.storeIdentity.Equal(store.rootIdentity) || !staged.parentIdentity.Equal(store.stagingIdentity) || !staged.identity.Valid() || staged.residentID != residentID || !ok || parsedResident != residentID || parsedDigest != staged.digest {
		return ErrInvalidStage
	}
	return nil
}

func (store *FileStore) validateOrphanToken(orphan Orphan) error {
	if !orphan.storeIdentity.Equal(store.rootIdentity) || !orphan.identity.Valid() || filepath.Base(orphan.Name) != orphan.Name || orphan.Name == "." {
		return ErrInvalidOrphan
	}
	expectedParent := store.stagingIdentity
	if orphan.location == orphanInTransientQuarantine {
		expectedParent = store.quarantineIdentity
	}
	if !orphan.parentIdentity.Equal(expectedParent) || filepath.Base(orphan.physicalName) != orphan.physicalName {
		return ErrInvalidOrphan
	}
	residentID, digest, sealed := parseSealedName(orphan.Name)
	if orphan.Kind == OrphanPartial {
		if !isPartialName(orphan.Name) || !orphan.ResidentID.IsZero() || orphan.Digest != (canonical.Digest{}) {
			return ErrInvalidOrphan
		}
	} else if orphan.Kind == OrphanSealed {
		if !sealed || orphan.ResidentID != residentID || orphan.Digest != digest {
			return ErrInvalidOrphan
		}
	} else {
		return ErrInvalidOrphan
	}
	expectedPhysical := orphan.Name
	if orphan.location == orphanInTransientQuarantine {
		if orphan.Kind == OrphanPartial {
			expectedPhysical = "delete-partial-" + orphan.Name
		} else {
			expectedPhysical = "delete-stage-" + orphan.Name
		}
	}
	if orphan.physicalName != expectedPhysical || orphan.Size < 0 || orphan.ModTime.IsZero() {
		return ErrInvalidOrphan
	}
	return nil
}

func verifyStagedHandle(ctx context.Context, handle *fssecure.Handle, staged Staged) error {
	if !staged.identity.Equal(handle.Identity()) || handle.Snapshot().Size() != staged.size.Int64() || !handle.Snapshot().ModTime().Equal(staged.modTime) {
		return fmt.Errorf("%w: stage identity changed", ErrInvalidStage)
	}
	if err := verifyExpectedContent(ctx, handle, staged.digest, staged.size.Int64()); err != nil {
		return errors.Join(ErrInvalidStage, err)
	}
	return nil
}

func verifyOrphanHandle(handle *fssecure.Handle, orphan Orphan) error {
	info := handle.Snapshot()
	if !orphan.identity.Equal(handle.Identity()) || info.Size() != orphan.Size || !info.ModTime().Equal(orphan.ModTime) {
		return fmt.Errorf("%w: orphan identity changed", ErrInvalidOrphan)
	}
	return handle.VerifyBound()
}

func verifyExpectedContent(ctx context.Context, handle *fssecure.Handle, expected canonical.Digest, expectedSize int64) error {
	sum, size, err := handle.Hash(ctx)
	if err != nil {
		return err
	}
	digest, err := canonical.DigestFromBytes(sum[:])
	if err != nil {
		return err
	}
	if digest != expected || size != expectedSize {
		return fmt.Errorf("%w: expected=%s/%d actual=%s/%d", ErrDigestMismatch, expected, expectedSize, digest, size)
	}
	return nil
}

func openOptional(directory *fssecure.Directory, name string) (*fssecure.Handle, bool, error) {
	handle, err := directory.OpenRegular(name)
	if errors.Is(err, os.ErrNotExist) {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, errors.Join(ErrUnsafeFilesystem, err)
	}
	return handle, true, nil
}

func (store *FileStore) invokeTestHook(label string) {
	if store != nil && store.testHook != nil {
		store.testHook(label)
	}
}

func (store *FileStore) invokeFailpoint(label string) error {
	if store != nil && store.testFailpoint != nil {
		return store.testFailpoint(label)
	}
	return nil
}

// objectPath is diagnostic/test compatibility only. Production namespace
// operations never pass its result back into fssecure.
func (store *FileStore) objectPath(residentID canonical.ID, digest canonical.Digest, create bool) (string, error) {
	if err := residentID.Validate(); err != nil {
		return "", err
	}
	if create {
		layout, err := store.openLayout()
		if err != nil {
			return "", err
		}
		location, _, err := openObjectLocation(layout.objects, residentID, digest, true)
		if location != nil {
			_ = location.Close()
		}
		_ = layout.Close()
		if err != nil {
			return "", err
		}
	}
	return filepath.Join(store.objectsDir, residentID.String(), digest.Hex()[:2], digest.Hex()[2:]), nil
}

func sealedName(residentID canonical.ID, digest canonical.Digest, partialName string) string {
	return "stage-" + residentID.String() + "-" + digest.Hex() + "-" + partialName
}

func parseSealedName(name string) (canonical.ID, canonical.Digest, bool) {
	const prefix = "stage-"
	const digestLength = sha256.Size * 2
	if !strings.HasPrefix(name, prefix) {
		return canonical.ID{}, canonical.Digest{}, false
	}
	rest := strings.TrimPrefix(name, prefix)
	if len(rest) < 26+1+digestLength+1+len("partial-")+1 || rest[26] != '-' || rest[26+1+digestLength] != '-' {
		return canonical.ID{}, canonical.Digest{}, false
	}
	residentID, err := canonical.ParseID(rest[:26])
	if err != nil {
		return canonical.ID{}, canonical.Digest{}, false
	}
	digest, err := canonical.ParseDigestHex(rest[27 : 27+digestLength])
	if err != nil || !isPartialName(rest[28+digestLength:]) {
		return canonical.ID{}, canonical.Digest{}, false
	}
	return residentID, digest, true
}

func isPartialName(name string) bool {
	if !strings.HasPrefix(name, "partial-") || len(name) == len("partial-") {
		return false
	}
	for _, char := range name[len("partial-"):] {
		if (char < '0' || char > '9') && (char < 'A' || char > 'Z') && (char < 'a' || char > 'z') {
			return false
		}
	}
	return true
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
