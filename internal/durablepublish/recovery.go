package durablepublish

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"mahoroba.local/mahoroba/internal/fssecure"
)

const maxMarkerBytes = 16 << 10

type RecoveryResult struct {
	Action  RecoveryAction
	Marker  Marker
	Payload PayloadSummary
}

// PublishedDirectoryRecoveryRequest completes a directory publication which
// has already crossed the no-replace rename boundary. Target must be reopened
// with fssecure.OpenRootForPublish while the producer still owns its namespace
// lock. Expected is independently recomputed from the current producer input.
type PublishedDirectoryRecoveryRequest struct {
	Target        *fssecure.Directory
	Expected      Marker
	CallerMarkers []string
	Failpoint     FailpointFunc
}

// DiscoverPublishedDirectoryMarker uses only the target handle and its narrow
// target-derived sibling lookup. The producer command and input digest must be
// independently recomputed by the caller; marker bytes are never authority for
// either value.
func DiscoverPublishedDirectoryMarker(
	target *fssecure.Directory,
	producerCommand string,
	producerInputDigest string,
) (Marker, error) {
	if target == nil || !validDigest(producerInputDigest) {
		return Marker{}, fmt.Errorf("%w: directory recovery discovery input", ErrInvalidProducerInput)
	}
	internalHandle, internalPresent, err := openOptionalRegular(target.OpenRegular, MarkerName)
	if err != nil {
		return Marker{}, fmt.Errorf("%w: inspect internal recovery marker: %v", ErrDurabilityUnknown, err)
	}
	var internal Marker
	if internalPresent {
		internal, err = readMarkerHandle(internalHandle)
		closeErr := internalHandle.Close()
		if err != nil || closeErr != nil {
			return Marker{}, fmt.Errorf("%w: authenticate internal recovery marker: %v", ErrDurabilityUnknown, errors.Join(err, closeErr))
		}
	}
	siblingHandle, siblingPresent, err := target.OpenUniquePublishMarkerSiblingForUpdate(filepath.Base(target.Path()))
	if err != nil {
		return Marker{}, fmt.Errorf("%w: inspect sibling recovery marker: %v", ErrDurabilityUnknown, err)
	}
	var sibling Marker
	if siblingPresent {
		sibling, err = readMarkerHandle(siblingHandle)
		if err == nil {
			var expectedName string
			expectedName, err = SiblingMarkerBasename(sibling.TargetBasename, sibling.PublishID)
			if err == nil && siblingHandle.Name() != expectedName {
				err = fmt.Errorf("%w: sibling marker filename differs", ErrDurabilityUnknown)
			}
		}
		closeErr := siblingHandle.Close()
		if err != nil || closeErr != nil {
			return Marker{}, fmt.Errorf("%w: authenticate sibling recovery marker: %v", ErrDurabilityUnknown, errors.Join(err, closeErr))
		}
	}
	if !internalPresent && !siblingPresent {
		return Marker{}, ErrRecoveryMarkerAbsent
	}
	marker := internal
	if !internalPresent {
		marker = sibling
	}
	if internalPresent && siblingPresent && internal != sibling {
		return Marker{}, fmt.Errorf("%w: internal and sibling markers differ", ErrDurabilityUnknown)
	}
	if marker.Variant != VariantDirectory || marker.ProducerCommand != producerCommand ||
		marker.ProducerInputDigest != producerInputDigest || marker.TargetBasename != filepath.Base(target.Path()) {
		return Marker{}, fmt.Errorf("%w: marker differs from recomputed producer identity", ErrDurabilityUnknown)
	}
	return marker, nil
}

// RecoverPublishedDirectory authenticates either the target-internal marker,
// the sibling marker, or both before mutating anything. It then repeats the
// target-marker, target-barrier, sibling-marker, parent-barrier suffix exactly.
func RecoverPublishedDirectory(ctx context.Context, request PublishedDirectoryRecoveryRequest) (RecoveryResult, error) {
	var result RecoveryResult
	if ctx == nil || request.Target == nil {
		return result, errors.New("durable publish: nil directory recovery request")
	}
	if err := request.Expected.Validate(); err != nil {
		return result, err
	}
	if request.Expected.Variant != VariantDirectory || filepath.Base(request.Target.Path()) != request.Expected.TargetBasename {
		return result, fmt.Errorf("%w: recovered directory target identity", ErrInvalidMarker)
	}
	callerMarkers, err := normalizedCallerMarkers(request.CallerMarkers)
	if err != nil {
		return result, err
	}
	expectedBytes, err := request.Expected.Bytes()
	if err != nil {
		return result, err
	}

	internal, internalPresent, err := openOptionalRegular(request.Target.OpenRegular, MarkerName)
	if err != nil {
		return result, err
	}
	if internal != nil {
		defer internal.Close()
		if err := authenticateMarkerHandle(internal, expectedBytes, request.Expected); err != nil {
			return result, err
		}
	}
	siblingName, err := SiblingMarkerBasename(request.Expected.TargetBasename, request.Expected.PublishID)
	if err != nil {
		return result, err
	}
	sibling, siblingPresent, err := openOptionalRegular(request.Target.OpenSiblingRegularForUpdate, siblingName)
	if err != nil {
		return result, err
	}
	if sibling != nil {
		defer sibling.Close()
		if err := authenticateMarkerHandle(sibling, expectedBytes, request.Expected); err != nil {
			return result, err
		}
	}
	if !internalPresent && !siblingPresent {
		return result, fmt.Errorf("%w: no authenticated recovery marker", ErrDurabilityUnknown)
	}
	payload, err := DirectoryPayloadDigest(ctx, request.Target, callerMarkers...)
	if err != nil {
		return result, err
	}
	if request.Expected.PayloadSHA256 != "sha256:"+payload.SHA256.Hex() {
		return result, ErrPayloadMismatch
	}
	result = RecoveryResult{Action: RecoveryCompletePublished, Marker: request.Expected, Payload: payload}

	if internalPresent {
		if err := markDeleteAndClose(internal); err != nil {
			return result, fmt.Errorf("%w: remove recovered target marker: %v", ErrDurabilityUnknown, err)
		}
		internal = nil
	}
	for _, name := range callerMarkers {
		handle, present, err := openOptionalRegular(request.Target.OpenRegular, name)
		if err != nil {
			return result, fmt.Errorf("%w: inspect caller marker %s: %v", ErrDurabilityUnknown, name, err)
		}
		if present {
			if err := markDeleteAndClose(handle); err != nil {
				return result, fmt.Errorf("%w: remove caller marker %s: %v", ErrDurabilityUnknown, name, err)
			}
		}
	}
	if err := request.Target.Sync(); err != nil {
		return result, fmt.Errorf("%w: sync recovered target: %v", ErrDurabilityUnknown, err)
	}
	if err := runFailpoint(request.Failpoint, FailpointAfterTargetCleanup); err != nil {
		return result, fmt.Errorf("%w: %v", ErrDurabilityUnknown, err)
	}
	if siblingPresent {
		if err := markDeleteAndClose(sibling); err != nil {
			return result, fmt.Errorf("%w: remove recovered sibling marker: %v", ErrDurabilityUnknown, err)
		}
		sibling = nil
	}
	if err := runFailpoint(request.Failpoint, FailpointAfterSiblingRemoval); err != nil {
		return result, fmt.Errorf("%w: %v", ErrDurabilityUnknown, err)
	}
	if err := request.Target.SyncParent(); err != nil {
		return result, fmt.Errorf("%w: final recovered parent barrier: %v", ErrDurabilityUnknown, err)
	}
	if err := runFailpoint(request.Failpoint, FailpointAfterFinalParentBarrier); err != nil {
		return result, fmt.Errorf("%w: %v", ErrDurabilityUnknown, err)
	}
	return result, nil
}

type SingleFileRecoveryRequest struct {
	Parent    *fssecure.Directory
	Expected  Marker
	Failpoint FailpointFunc
}

// RecoverSingleFile executes the single-file state machine from an
// authenticated sibling marker. It can resume a matching staging file,
// complete a renamed target, or durably remove a stale marker. Mismatch and
// simultaneous target/staging states are mutation-free repair failures.
func RecoverSingleFile(ctx context.Context, request SingleFileRecoveryRequest) (RecoveryResult, error) {
	var result RecoveryResult
	if ctx == nil || request.Parent == nil {
		return result, errors.New("durable publish: nil single-file recovery request")
	}
	if err := request.Expected.Validate(); err != nil {
		return result, err
	}
	if request.Expected.Variant != VariantSingleFile {
		return result, fmt.Errorf("%w: recovered single-file variant", ErrInvalidMarker)
	}
	expectedBytes, err := request.Expected.Bytes()
	if err != nil {
		return result, err
	}
	siblingName, err := SiblingMarkerBasename(request.Expected.TargetBasename, request.Expected.PublishID)
	if err != nil {
		return result, err
	}
	sibling, siblingPresent, err := openOptionalRegular(request.Parent.OpenRegular, siblingName)
	if err != nil {
		return result, err
	}
	if !siblingPresent {
		return result, ErrRecoveryMarkerAbsent
	}
	defer sibling.Close()
	if err := authenticateMarkerHandle(sibling, expectedBytes, request.Expected); err != nil {
		return result, err
	}

	target, targetPresent, err := openOptionalRegular(request.Parent.OpenRegular, request.Expected.TargetBasename)
	if err != nil {
		return result, err
	}
	if target != nil {
		defer target.Close()
	}
	staging, stagingPresent, err := openOptionalRegular(request.Parent.OpenRegular, request.Expected.StagingBasename)
	if err != nil {
		return result, err
	}
	if staging != nil {
		defer staging.Close()
	}
	if targetPresent && stagingPresent {
		return RecoveryResult{Action: RecoveryRepairRequired, Marker: request.Expected}, ErrDurabilityUnknown
	}
	if targetPresent {
		payload, err := SingleFilePayloadDigest(ctx, target)
		if err != nil {
			return result, err
		}
		if request.Expected.PayloadSHA256 != "sha256:"+payload.SHA256.Hex() {
			return RecoveryResult{Action: RecoveryRepairRequired, Marker: request.Expected}, ErrPayloadMismatch
		}
		result = RecoveryResult{Action: RecoveryCompletePublished, Marker: request.Expected, Payload: payload}
		if err := target.Close(); err != nil {
			return result, err
		}
		target = nil
		if err := markDeleteAndClose(sibling); err != nil {
			return result, fmt.Errorf("%w: remove recovered sibling marker: %v", ErrDurabilityUnknown, err)
		}
		sibling = nil
		if err := runFailpoint(request.Failpoint, FailpointAfterSiblingRemoval); err != nil {
			return result, fmt.Errorf("%w: %v", ErrDurabilityUnknown, err)
		}
		if err := request.Parent.Sync(); err != nil {
			return result, fmt.Errorf("%w: final recovered parent barrier: %v", ErrDurabilityUnknown, err)
		}
		if err := runFailpoint(request.Failpoint, FailpointAfterFinalParentBarrier); err != nil {
			return result, fmt.Errorf("%w: %v", ErrDurabilityUnknown, err)
		}
		return result, nil
	}
	if !stagingPresent {
		result = RecoveryResult{Action: RecoveryRemoveStaleMarker, Marker: request.Expected}
		if err := markDeleteAndClose(sibling); err != nil {
			return result, fmt.Errorf("%w: remove stale sibling marker: %v", ErrDurabilityUnknown, err)
		}
		sibling = nil
		if err := request.Parent.Sync(); err != nil {
			return result, fmt.Errorf("%w: sync stale marker removal: %v", ErrDurabilityUnknown, err)
		}
		return result, nil
	}
	payload, err := SingleFilePayloadDigest(ctx, staging)
	if err != nil {
		return result, err
	}
	if request.Expected.PayloadSHA256 != "sha256:"+payload.SHA256.Hex() {
		return RecoveryResult{Action: RecoveryRepairRequired, Marker: request.Expected}, ErrPayloadMismatch
	}
	result = RecoveryResult{Action: RecoveryResumeSingleFile, Marker: request.Expected, Payload: payload}
	if err := request.Parent.PublishNoReplace(staging, request.Expected.TargetBasename); err != nil {
		if errors.Is(err, os.ErrExist) {
			return result, ErrTargetExists
		}
		return result, fmt.Errorf("%w: resume single-file rename: %v", ErrDurabilityUnknown, err)
	}
	if err := runFailpoint(request.Failpoint, FailpointAfterPayloadRename); err != nil {
		return result, fmt.Errorf("%w: %v", ErrDurabilityUnknown, err)
	}
	if err := markDeleteAndClose(sibling); err != nil {
		return result, fmt.Errorf("%w: remove resumed sibling marker: %v", ErrDurabilityUnknown, err)
	}
	sibling = nil
	if err := runFailpoint(request.Failpoint, FailpointAfterSiblingRemoval); err != nil {
		return result, fmt.Errorf("%w: %v", ErrDurabilityUnknown, err)
	}
	if err := request.Parent.Sync(); err != nil {
		return result, fmt.Errorf("%w: final resumed parent barrier: %v", ErrDurabilityUnknown, err)
	}
	if err := runFailpoint(request.Failpoint, FailpointAfterFinalParentBarrier); err != nil {
		return result, fmt.Errorf("%w: %v", ErrDurabilityUnknown, err)
	}
	return result, nil
}

type openRegularFunc func(string) (*fssecure.Handle, error)

func openOptionalRegular(open openRegularFunc, name string) (*fssecure.Handle, bool, error) {
	handle, err := open(name)
	if errors.Is(err, os.ErrNotExist) {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, err
	}
	return handle, true, nil
}

func authenticateMarkerHandle(handle *fssecure.Handle, expectedBytes []byte, expected Marker) error {
	parsed, body, err := readMarkerHandleBytes(handle)
	if err != nil {
		return err
	}
	if parsed != expected || string(body) != string(expectedBytes) {
		return fmt.Errorf("%w: marker differs from recomputed producer input", ErrDurabilityUnknown)
	}
	return handle.VerifyBound()
}

func readMarkerHandle(handle *fssecure.Handle) (Marker, error) {
	marker, _, err := readMarkerHandleBytes(handle)
	return marker, err
}

func readMarkerHandleBytes(handle *fssecure.Handle) (Marker, []byte, error) {
	if err := handle.VerifyBound(); err != nil {
		return Marker{}, nil, err
	}
	if _, err := handle.File().Seek(0, io.SeekStart); err != nil {
		return Marker{}, nil, err
	}
	body, err := io.ReadAll(io.LimitReader(handle.File(), maxMarkerBytes+1))
	if err != nil {
		return Marker{}, nil, err
	}
	if len(body) > maxMarkerBytes {
		return Marker{}, nil, fmt.Errorf("%w: marker exceeds byte bound", ErrInvalidMarker)
	}
	parsed, err := ParseMarker(body)
	if err != nil {
		return Marker{}, nil, err
	}
	if err := handle.VerifyBound(); err != nil {
		return Marker{}, nil, err
	}
	return parsed, body, nil
}

func markDeleteAndClose(handle *fssecure.Handle) error {
	if handle == nil {
		return nil
	}
	deleteErr := handle.MarkDeleteOnClose()
	return errors.Join(deleteErr, handle.Close())
}
