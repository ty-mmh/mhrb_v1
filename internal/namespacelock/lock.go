// Package namespacelock serializes creation and publication of one named
// target in a parent directory. Ownership is an operating-system advisory
// lock, not the continued existence of the rendezvous file.
package namespacelock

import (
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
)

const lockSuffix = ".mahoroba-namespace.lock"

var (
	// ErrBusy reports that another process owns the same parent/basename
	// namespace. A leftover rendezvous file does not produce this error.
	ErrBusy = errors.New("namespacelock: target namespace is busy")
	// ErrUnsafeTarget reports a target, lock entry, or parent which cannot be
	// proven to be an ordinary directory/regular file without following a
	// target-side symbolic link or reparse point.
	ErrUnsafeTarget = errors.New("namespacelock: unsafe target namespace")
	// ErrUnsupported reports that the platform cannot provide the required
	// handle-relative and advisory-lock primitives.
	ErrUnsupported = errors.New("namespacelock: secure namespace locking is unsupported")
)

// Key identifies a namespace by its resolved physical parent and exact target
// basename. CanonicalParent is diagnostic only; the live Lock is bound to an
// open parent handle.
type Key struct {
	CanonicalParent string
	TargetBasename  string
}

// Lock owns the exclusive advisory lock for one Key.
type Lock struct {
	parent   *os.File
	file     *os.File
	key      Key
	identity platformIdentity
	binding  parentPathBinding
	testHook func(string)
	once     sync.Once
	closeErr error
}

// parentPathBinding retains a component-wise, no-follow proof from the
// filesystem/drive root to the canonical parent. A live parent descriptor by
// itself is insufficient on rename-capable filesystems: it can remain valid
// after its pathname has been replaced, allowing a second lock namespace to
// appear at the same canonical path.
type parentPathBinding interface {
	verify() error
	close() error
}

// Target is an open, no-follow directory handle created or reopened while its
// parent namespace lock is held.
type Target struct {
	file     *os.File
	identity platformIdentity
	once     sync.Once
	closeErr error
}

// BoundRegular is a read-only child file retained under a bound Target. It
// keeps the exact file identity open while a path-based library consumes a
// descriptor-backed path.
type BoundRegular struct {
	file     *os.File
	parent   *Target
	name     string
	identity platformIdentity
	info     os.FileInfo
	once     sync.Once
	closeErr error
}

type DiagnosticMarkerKind string

const (
	DiagnosticRestoreStaging DiagnosticMarkerKind = "restore_staging"
	DiagnosticPublishPending DiagnosticMarkerKind = "publish_pending"
)

// DiagnosticMarkerObservation is a narrow, target-derived parent inspection
// result. Basename is never a path and Marker is bounded marker data only.
// Unreadable entries remain present so callers cannot silently omit a hostile
// or malformed marker-shaped namespace entry.
type DiagnosticMarkerObservation struct {
	Kind          DiagnosticMarkerKind
	Basename      string
	Marker        []byte
	MarkerPresent bool
	Readable      bool
}

// Acquire resolves the target parent, opens it without following its final
// component, and obtains the non-blocking exclusive lock for the exact target
// basename. It never creates target itself.
func Acquire(target string) (*Lock, error) {
	return acquire(target, true)
}

// AcquireExistingParent is the restore/artifact-producer form of Acquire. It
// never creates the parent hierarchy, so a missing parent fails before any
// filesystem mutation. Runtime target creation may use Acquire; publish and
// restore protocols should normally use this stricter form.
func AcquireExistingParent(target string) (*Lock, error) {
	return acquire(target, false)
}

func acquire(target string, createParent bool) (*Lock, error) {
	parentPath, base, err := splitTarget(target)
	if err != nil {
		return nil, err
	}

	// Parent hierarchy creation is deliberately outside the target protocol.
	// Once it exists, aliases are collapsed before the rendezvous is selected.
	if createParent {
		if err := os.MkdirAll(parentPath, 0o700); err != nil {
			return nil, fmt.Errorf("namespacelock: create target parent: %w", err)
		}
	}
	canonicalParent, err := filepath.EvalSymlinks(parentPath)
	if err != nil {
		return nil, fmt.Errorf("namespacelock: resolve target parent: %w", err)
	}
	canonicalParent, err = filepath.Abs(canonicalParent)
	if err != nil {
		return nil, fmt.Errorf("namespacelock: canonical target parent: %w", err)
	}
	canonicalParent = filepath.Clean(canonicalParent)

	parent, identity, err := platformOpenParent(canonicalParent)
	if err != nil {
		return nil, err
	}
	binding, err := platformBindParentPath(canonicalParent, identity)
	if err != nil {
		_ = parent.Close()
		return nil, fmt.Errorf("namespacelock: bind canonical parent: %w", err)
	}
	lockName := "." + base + lockSuffix
	file, err := platformOpenRegular(parent, lockName, 0o600)
	if err != nil {
		_ = binding.close()
		_ = parent.Close()
		return nil, fmt.Errorf("namespacelock: open rendezvous: %w", err)
	}
	if err := platformLock(file); err != nil {
		_ = file.Close()
		_ = binding.close()
		_ = parent.Close()
		return nil, err
	}
	if err := binding.verify(); err != nil {
		_ = platformUnlock(file)
		_ = file.Close()
		_ = binding.close()
		_ = parent.Close()
		return nil, fmt.Errorf("namespacelock: parent binding changed while locking: %w", err)
	}
	if err := platformVerifyIdentity(parent, identity, entryDirectory); err != nil {
		_ = platformUnlock(file)
		_ = file.Close()
		_ = binding.close()
		_ = parent.Close()
		return nil, fmt.Errorf("namespacelock: parent changed while locking: %w", err)
	}
	return &Lock{
		parent:   parent,
		file:     file,
		key:      Key{CanonicalParent: canonicalParent, TargetBasename: base},
		identity: identity,
		binding:  binding,
	}, nil
}

func splitTarget(target string) (parent, base string, err error) {
	if target == "" || !filepath.IsAbs(target) {
		return "", "", errors.New("namespacelock: target must be an absolute path")
	}
	clean := filepath.Clean(target)
	base = filepath.Base(clean)
	parent = filepath.Dir(clean)
	if base == "." || base == string(filepath.Separator) || base == "" || parent == clean || strings.ContainsAny(base, `/\\`) {
		return "", "", errors.New("namespacelock: target must name a child of a parent directory")
	}
	return parent, base, nil
}

// Key returns the canonical parent and exact basename protected by lock.
func (lock *Lock) Key() Key {
	if lock == nil {
		return Key{}
	}
	return lock.key
}

// TargetExists observes the exact target relative to the retained parent. A
// symbolic link, reparse point, non-directory, or namespace whose security
// permits untrusted mutation is an error rather than an absent target. The
// observation does not confer managed-target authority; OpenExistingTarget
// and OpenOrCreateTarget apply the exact managed security policy.
func (lock *Lock) TargetExists() (bool, error) {
	if err := lock.verify(); err != nil {
		return false, err
	}
	target, exists, err := platformOpenObservedTarget(lock.parent, lock.key.TargetBasename)
	if err != nil {
		return false, err
	}
	if lock.testHook != nil {
		lock.testHook("after_target_observation")
	}
	var invariantErr error
	if exists != (target != nil) {
		invariantErr = ErrUnsafeTarget
	}
	var closeErr error
	if target != nil {
		closeErr = target.Close()
	}
	verifyErr := lock.verify()
	if invariantErr != nil || closeErr != nil || verifyErr != nil {
		return false, errors.Join(invariantErr, closeErr, verifyErr)
	}
	return exists, nil
}

// RegularTargetExists checks the exact target as a protected ordinary file,
// relative to the retained parent handle. It is the single-file artifact
// counterpart of TargetExists; a directory, symlink/reparse point, unsafe
// owner/mode/ACL, or sharing conflict fails closed instead of being reported
// as absent.
func (lock *Lock) RegularTargetExists() (bool, error) {
	if err := lock.verify(); err != nil {
		return false, err
	}
	file, err := platformOpenRegularRead(lock.parent, lock.key.TargetBasename)
	if errors.Is(err, os.ErrNotExist) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	closeErr := file.Close()
	verifyErr := lock.verify()
	if closeErr != nil || verifyErr != nil {
		return false, errors.Join(closeErr, verifyErr)
	}
	return true, nil
}

// OpenOrCreateTarget creates the exact target if absent, then returns a
// verified no-follow directory handle. An existing target must already have
// the requested exact managed security; it is rejected rather than repaired.
// Callers must retain Lock until all namespace-sensitive setup or publication
// is complete.
func (lock *Lock) OpenOrCreateTarget(mode os.FileMode) (*Target, error) {
	if err := lock.verify(); err != nil {
		return nil, err
	}
	file, _, err := platformOpenTarget(lock.parent, lock.key.TargetBasename, true, mode)
	if err != nil {
		return nil, err
	}
	identity, err := platformIdentityOf(file, entryDirectory)
	if err != nil {
		_ = file.Close()
		return nil, err
	}
	if err := lock.verify(); err != nil {
		_ = file.Close()
		return nil, err
	}
	return &Target{file: file, identity: identity}, nil
}

// OpenExistingTarget returns the exact target without creating it. It is used
// by offline observers which acquire the host lock while the namespace lock is
// still held.
func (lock *Lock) OpenExistingTarget() (*Target, error) {
	if err := lock.verify(); err != nil {
		return nil, err
	}
	file, exists, err := platformOpenTarget(lock.parent, lock.key.TargetBasename, false, 0)
	if err != nil {
		return nil, err
	}
	if !exists || file == nil {
		return nil, os.ErrNotExist
	}
	identity, err := platformIdentityOf(file, entryDirectory)
	if err != nil {
		_ = file.Close()
		return nil, err
	}
	if err := lock.verify(); err != nil {
		_ = file.Close()
		return nil, err
	}
	return &Target{file: file, identity: identity}, nil
}

// VerifyBoundTarget proves that target is still the exact target basename in
// this lock's root-bound parent namespace.
func (lock *Lock) VerifyBoundTarget(target *Target) error {
	if err := lock.verify(); err != nil {
		return err
	}
	if err := target.verify(); err != nil {
		return err
	}
	current, exists, err := platformOpenTarget(lock.parent, lock.key.TargetBasename, false, 0)
	if err != nil {
		return err
	}
	if !exists || current == nil {
		return ErrUnsafeTarget
	}
	identity, identityErr := platformIdentityOf(current, entryDirectory)
	closeErr := current.Close()
	if identityErr != nil || closeErr != nil {
		return errors.Join(identityErr, closeErr)
	}
	if identity != target.identity {
		return ErrUnsafeTarget
	}
	return lock.verify()
}

// InspectDiagnosticSiblings performs the sole allowed parent enumeration for
// diagnostics. Only exact target-derived restore-staging and publish-pending
// prefixes are observed, and all opens remain parent-handle-relative.
func (lock *Lock) InspectDiagnosticSiblings(maxMarkerBytes int64) ([]DiagnosticMarkerObservation, error) {
	if maxMarkerBytes < 1 || maxMarkerBytes > 16<<10 {
		return nil, ErrUnsafeTarget
	}
	if err := lock.verify(); err != nil {
		return nil, err
	}
	entries, err := lock.parent.ReadDir(-1)
	if err != nil {
		return nil, err
	}
	restorePrefix := "." + lock.key.TargetBasename + ".restore-staging."
	publishPrefix := "." + lock.key.TargetBasename + ".publish-pending."
	result := make([]DiagnosticMarkerObservation, 0)
	for _, entry := range entries {
		name := entry.Name()
		switch {
		case strings.HasPrefix(name, restorePrefix):
			observation := DiagnosticMarkerObservation{Kind: DiagnosticRestoreStaging, Basename: name}
			directory, exists, openErr := platformOpenObservedTarget(lock.parent, name)
			if openErr == nil && exists && directory != nil {
				marker, present, readErr := readDiagnosticRegular(directory, "RESTORE_STAGING", maxMarkerBytes)
				observation.Marker, observation.MarkerPresent = marker, present
				observation.Readable = readErr == nil && present
				_ = directory.Close()
			}
			result = append(result, observation)
		case strings.HasPrefix(name, publishPrefix):
			observation := DiagnosticMarkerObservation{Kind: DiagnosticPublishPending, Basename: name}
			marker, present, readErr := readDiagnosticRegular(lock.parent, name, maxMarkerBytes)
			observation.Marker, observation.MarkerPresent = marker, present
			observation.Readable = readErr == nil && present
			result = append(result, observation)
		}
	}
	if err := lock.verify(); err != nil {
		return nil, err
	}
	sort.Slice(result, func(i, j int) bool {
		if result[i].Kind != result[j].Kind {
			return result[i].Kind < result[j].Kind
		}
		return result[i].Basename < result[j].Basename
	})
	return result, nil
}

func (lock *Lock) verify() error {
	if lock == nil || lock.parent == nil || lock.file == nil || lock.binding == nil {
		return ErrUnsafeTarget
	}
	if err := lock.binding.verify(); err != nil {
		return fmt.Errorf("%w: canonical parent binding changed: %v", ErrUnsafeTarget, err)
	}
	if err := platformVerifyIdentity(lock.parent, lock.identity, entryDirectory); err != nil {
		return fmt.Errorf("%w: parent identity changed: %v", ErrUnsafeTarget, err)
	}
	return nil
}

// OpenOrCreateRegular opens a regular child relative to target without
// following symbolic links or reparse points. It is used by nested protocols
// such as the data-directory host lock.
func (target *Target) OpenOrCreateRegular(name string, mode os.FileMode) (*os.File, error) {
	if target == nil || target.file == nil || filepath.Base(name) != name || name == "." || name == "" {
		return nil, ErrUnsafeTarget
	}
	if err := platformVerifyIdentity(target.file, target.identity, entryDirectory); err != nil {
		return nil, fmt.Errorf("%w: target identity changed: %v", ErrUnsafeTarget, err)
	}
	file, err := platformOpenRegular(target.file, name, mode)
	if err != nil {
		return nil, err
	}
	return file, nil
}

// OpenRegularRead opens one existing protected regular file relative to the
// retained target and returns an identity-bearing read-only handle.
func (target *Target) OpenRegularRead(name string) (*BoundRegular, error) {
	if target == nil || target.file == nil || filepath.Base(name) != name || name == "." || name == "" {
		return nil, ErrUnsafeTarget
	}
	if err := target.verify(); err != nil {
		return nil, err
	}
	file, err := platformOpenObservedRegularRead(target.file, name)
	if err != nil {
		return nil, err
	}
	identity, err := platformIdentityOf(file, entryRegular)
	if err != nil {
		_ = file.Close()
		return nil, err
	}
	info, err := file.Stat()
	if err != nil {
		_ = file.Close()
		return nil, err
	}
	bound := &BoundRegular{file: file, parent: target, name: name, identity: identity, info: info}
	if err := bound.VerifyBound(); err != nil {
		_ = bound.Close()
		return nil, err
	}
	return bound, nil
}

func (target *Target) verify() error {
	if target == nil || target.file == nil {
		return ErrUnsafeTarget
	}
	if err := platformVerifyIdentity(target.file, target.identity, entryDirectory); err != nil {
		return fmt.Errorf("%w: target identity changed: %v", ErrUnsafeTarget, err)
	}
	return nil
}

// File returns the retained read-only file. Ownership stays with BoundRegular.
func (file *BoundRegular) File() *os.File {
	if file == nil {
		return nil
	}
	return file.file
}

// VerifyBound proves both the open handle and its target-relative basename.
func (file *BoundRegular) VerifyBound() error {
	if file == nil || file.file == nil || file.parent == nil || file.name == "" || file.info == nil {
		return ErrUnsafeTarget
	}
	if err := file.parent.verify(); err != nil {
		return err
	}
	if err := platformVerifyIdentity(file.file, file.identity, entryRegular); err != nil {
		return fmt.Errorf("%w: regular identity changed: %v", ErrUnsafeTarget, err)
	}
	currentInfo, err := file.file.Stat()
	if err != nil || !os.SameFile(file.info, currentInfo) || file.info.Size() != currentInfo.Size() || !file.info.ModTime().Equal(currentInfo.ModTime()) {
		return fmt.Errorf("%w: regular snapshot changed: %v", ErrUnsafeTarget, err)
	}
	current, err := platformOpenObservedRegularRead(file.parent.file, file.name)
	if err != nil {
		return err
	}
	identity, identityErr := platformIdentityOf(current, entryRegular)
	closeErr := current.Close()
	if identityErr != nil || closeErr != nil {
		return errors.Join(identityErr, closeErr)
	}
	if identity != file.identity {
		return ErrUnsafeTarget
	}
	return file.parent.verify()
}

// Close closes the retained read-only file. It is idempotent.
func (file *BoundRegular) Close() error {
	if file == nil {
		return nil
	}
	file.once.Do(func() {
		if file.file != nil {
			file.closeErr = file.file.Close()
			file.file = nil
		}
	})
	return file.closeErr
}

// ReadDiagnosticMarker reads one of the two exact in-target recovery markers
// without creating or mutating an entry.
func (target *Target) ReadDiagnosticMarker(name string, maxMarkerBytes int64) ([]byte, bool, error) {
	if target == nil || target.file == nil || (name != "RESTORE_STAGING" && name != "PUBLISH_PENDING") ||
		maxMarkerBytes < 1 || maxMarkerBytes > 16<<10 {
		return nil, false, ErrUnsafeTarget
	}
	if err := platformVerifyIdentity(target.file, target.identity, entryDirectory); err != nil {
		return nil, false, fmt.Errorf("%w: target identity changed: %v", ErrUnsafeTarget, err)
	}
	body, present, err := readDiagnosticRegular(target.file, name, maxMarkerBytes)
	if err != nil {
		return nil, present, err
	}
	if err := platformVerifyIdentity(target.file, target.identity, entryDirectory); err != nil {
		return nil, false, fmt.Errorf("%w: target identity changed: %v", ErrUnsafeTarget, err)
	}
	return body, present, nil
}

func readDiagnosticRegular(parent *os.File, name string, maxBytes int64) ([]byte, bool, error) {
	file, err := platformOpenRegularRead(parent, name)
	if errors.Is(err, os.ErrNotExist) {
		return nil, false, nil
	}
	if err != nil {
		// The exact marker name could not be safely opened. Keep it observable
		// as present/unreadable rather than silently treating an unsafe kind,
		// ACL, sharing state, or reparse point as absence.
		return nil, true, err
	}
	defer file.Close()
	body, err := io.ReadAll(io.LimitReader(file, maxBytes+1))
	if err != nil {
		return nil, true, err
	}
	if int64(len(body)) > maxBytes {
		return nil, true, ErrUnsafeTarget
	}
	return body, true, nil
}

// Close closes the retained target handle. It is idempotent.
func (target *Target) Close() error {
	if target == nil {
		return nil
	}
	target.once.Do(func() {
		if target.file != nil {
			target.closeErr = target.file.Close()
			target.file = nil
		}
	})
	return target.closeErr
}

// Close releases the advisory namespace lock and retained parent handle. It is
// safe to call repeatedly.
func (lock *Lock) Close() error {
	if lock == nil {
		return nil
	}
	lock.once.Do(func() {
		var unlockErr, fileErr, parentErr, bindingErr error
		if lock.file != nil {
			unlockErr = platformUnlock(lock.file)
			fileErr = lock.file.Close()
			lock.file = nil
		}
		if lock.parent != nil {
			parentErr = lock.parent.Close()
			lock.parent = nil
		}
		if lock.binding != nil {
			bindingErr = lock.binding.close()
			lock.binding = nil
		}
		lock.closeErr = errors.Join(unlockErr, fileErr, parentErr, bindingErr)
	})
	return lock.closeErr
}

type entryKind uint8

const (
	entryDirectory entryKind = iota + 1
	entryRegular
)
