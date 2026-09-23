package durablepublish

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"

	"mahoroba.local/mahoroba/internal/fssecure"
)

type Failpoint string

const (
	FailpointAfterMarkersDurable     Failpoint = "after_markers_durable"
	FailpointAfterPayloadRename      Failpoint = "after_payload_rename"
	FailpointAfterTargetCleanup      Failpoint = "after_target_cleanup"
	FailpointAfterSiblingRemoval     Failpoint = "after_sibling_removal"
	FailpointAfterFinalParentBarrier Failpoint = "after_final_parent_barrier"
)

type FailpointFunc func(Failpoint) error

type Result struct {
	Marker  Marker
	Payload PayloadSummary
}

type DirectoryRequest struct {
	Staging       *fssecure.Directory
	Marker        Marker
	CallerMarkers []string
	Failpoint     FailpointFunc
}

type SingleFileRequest struct {
	Parent    *fssecure.Directory
	Staging   *fssecure.Handle
	Marker    Marker
	Failpoint FailpointFunc
}

// SiblingMarkerBasename is stable public namespace syntax shared by every
// artifact producer and official consumer.
func SiblingMarkerBasename(target string, publishID fmt.Stringer) (string, error) {
	if !validBasename(target) || publishID == nil || publishID.String() == "" {
		return "", fmt.Errorf("%w: sibling marker identity", ErrInvalidMarker)
	}
	name := "." + target + ".publish-pending." + publishID.String()
	if !validBasename(name) {
		return "", fmt.Errorf("%w: sibling marker basename", ErrInvalidMarker)
	}
	return name, nil
}

func PublishDirectory(ctx context.Context, request DirectoryRequest) (Result, error) {
	var result Result
	if ctx == nil || request.Staging == nil {
		return result, errors.New("durable publish: nil directory request")
	}
	if err := request.Marker.Validate(); err != nil {
		return result, err
	}
	if request.Marker.Variant != VariantDirectory ||
		filepath.Base(request.Staging.Path()) != request.Marker.StagingBasename {
		return result, fmt.Errorf("%w: directory staging identity", ErrInvalidMarker)
	}
	reserved, err := normalizedCallerMarkers(request.CallerMarkers)
	if err != nil {
		return result, err
	}
	payload, err := DirectoryPayloadDigest(ctx, request.Staging, reserved...)
	if err != nil {
		return result, err
	}
	if request.Marker.PayloadSHA256 != "sha256:"+payload.SHA256.Hex() {
		return result, ErrPayloadMismatch
	}
	result = Result{Marker: request.Marker, Payload: payload}

	// Capability preflight is intentionally before either marker exists.
	if err := errors.Join(request.Staging.Sync(), request.Staging.SyncParent()); err != nil {
		return result, fmt.Errorf("%w: preflight directory barriers: %v", ErrUnsupportedBarrier, err)
	}
	markerBytes, err := request.Marker.Bytes()
	if err != nil {
		return result, err
	}
	if err := writeNewFile(request.Staging, MarkerName, markerBytes); err != nil {
		return result, err
	}
	siblingName, err := SiblingMarkerBasename(request.Marker.TargetBasename, request.Marker.PublishID)
	if err != nil {
		return result, err
	}
	sibling, err := request.Staging.CreateSiblingRegular(siblingName)
	if err != nil {
		return result, err
	}
	siblingOpen := true
	defer func() {
		if siblingOpen {
			_ = sibling.Close()
		}
	}()
	if err := writeAndSealOpen(sibling, markerBytes); err != nil {
		return result, err
	}
	if err := errors.Join(request.Staging.Sync(), request.Staging.SyncParent()); err != nil {
		return result, err
	}
	if err := runFailpoint(request.Failpoint, FailpointAfterMarkersDurable); err != nil {
		return result, err
	}

	if err := request.Staging.PublishSiblingNoReplace(request.Marker.TargetBasename); err != nil {
		if errors.Is(err, os.ErrExist) {
			return result, ErrTargetExists
		}
		return result, fmt.Errorf("%w: publish directory: %v", ErrDurabilityUnknown, err)
	}
	if err := runFailpoint(request.Failpoint, FailpointAfterPayloadRename); err != nil {
		return result, fmt.Errorf("%w: %v", ErrDurabilityUnknown, err)
	}

	cleanup := append([]string{MarkerName}, reserved...)
	for _, name := range cleanup {
		if err := deleteFile(request.Staging, name); err != nil {
			return result, fmt.Errorf("%w: remove target marker %s: %v", ErrDurabilityUnknown, name, err)
		}
	}
	if err := request.Staging.Sync(); err != nil {
		return result, fmt.Errorf("%w: sync cleaned target: %v", ErrDurabilityUnknown, err)
	}
	if err := runFailpoint(request.Failpoint, FailpointAfterTargetCleanup); err != nil {
		return result, fmt.Errorf("%w: %v", ErrDurabilityUnknown, err)
	}

	if err := sibling.MarkDeleteOnClose(); err != nil {
		return result, fmt.Errorf("%w: remove sibling marker: %v", ErrDurabilityUnknown, err)
	}
	if err := sibling.Close(); err != nil {
		return result, fmt.Errorf("%w: close sibling marker: %v", ErrDurabilityUnknown, err)
	}
	siblingOpen = false
	if err := runFailpoint(request.Failpoint, FailpointAfterSiblingRemoval); err != nil {
		return result, fmt.Errorf("%w: %v", ErrDurabilityUnknown, err)
	}
	if err := request.Staging.SyncParent(); err != nil {
		return result, fmt.Errorf("%w: final parent barrier: %v", ErrDurabilityUnknown, err)
	}
	if err := runFailpoint(request.Failpoint, FailpointAfterFinalParentBarrier); err != nil {
		return result, fmt.Errorf("%w: %v", ErrDurabilityUnknown, err)
	}
	return result, nil
}

func PublishSingleFile(ctx context.Context, request SingleFileRequest) (Result, error) {
	var result Result
	if ctx == nil || request.Parent == nil || request.Staging == nil {
		return result, errors.New("durable publish: nil single-file request")
	}
	if err := request.Marker.Validate(); err != nil {
		return result, err
	}
	if request.Marker.Variant != VariantSingleFile || request.Staging.Name() != request.Marker.StagingBasename {
		return result, fmt.Errorf("%w: single-file staging identity", ErrInvalidMarker)
	}
	payload, err := SingleFilePayloadDigest(ctx, request.Staging)
	if err != nil {
		return result, err
	}
	if request.Marker.PayloadSHA256 != "sha256:"+payload.SHA256.Hex() {
		return result, ErrPayloadMismatch
	}
	result = Result{Marker: request.Marker, Payload: payload}
	if err := request.Parent.Sync(); err != nil {
		return result, fmt.Errorf("%w: preflight parent barrier: %v", ErrUnsupportedBarrier, err)
	}
	markerBytes, err := request.Marker.Bytes()
	if err != nil {
		return result, err
	}
	siblingName, err := SiblingMarkerBasename(request.Marker.TargetBasename, request.Marker.PublishID)
	if err != nil {
		return result, err
	}
	sibling, err := request.Parent.CreateRegular(siblingName)
	if err != nil {
		return result, err
	}
	siblingOpen := true
	defer func() {
		if siblingOpen {
			_ = sibling.Close()
		}
	}()
	if err := writeAndSealOpen(sibling, markerBytes); err != nil {
		return result, err
	}
	if err := request.Parent.Sync(); err != nil {
		return result, err
	}
	if err := runFailpoint(request.Failpoint, FailpointAfterMarkersDurable); err != nil {
		return result, err
	}
	if err := request.Parent.PublishNoReplace(request.Staging, request.Marker.TargetBasename); err != nil {
		if errors.Is(err, os.ErrExist) {
			return result, ErrTargetExists
		}
		return result, fmt.Errorf("%w: publish single file: %v", ErrDurabilityUnknown, err)
	}
	if err := runFailpoint(request.Failpoint, FailpointAfterPayloadRename); err != nil {
		return result, fmt.Errorf("%w: %v", ErrDurabilityUnknown, err)
	}
	if err := sibling.MarkDeleteOnClose(); err != nil {
		return result, fmt.Errorf("%w: remove sibling marker: %v", ErrDurabilityUnknown, err)
	}
	if err := sibling.Close(); err != nil {
		return result, fmt.Errorf("%w: close sibling marker: %v", ErrDurabilityUnknown, err)
	}
	siblingOpen = false
	if err := runFailpoint(request.Failpoint, FailpointAfterSiblingRemoval); err != nil {
		return result, fmt.Errorf("%w: %v", ErrDurabilityUnknown, err)
	}
	if err := request.Parent.Sync(); err != nil {
		return result, fmt.Errorf("%w: final parent barrier: %v", ErrDurabilityUnknown, err)
	}
	if err := runFailpoint(request.Failpoint, FailpointAfterFinalParentBarrier); err != nil {
		return result, fmt.Errorf("%w: %v", ErrDurabilityUnknown, err)
	}
	return result, nil
}

func normalizedCallerMarkers(values []string) ([]string, error) {
	result := slices.Clone(values)
	slices.Sort(result)
	for index, name := range result {
		if !validBasename(name) || name == MarkerName || index > 0 && result[index-1] == name {
			return nil, fmt.Errorf("durable publish: invalid caller marker %q", name)
		}
	}
	return result, nil
}

func writeNewFile(directory *fssecure.Directory, name string, body []byte) error {
	handle, err := directory.CreateRegular(name)
	if err != nil {
		return err
	}
	return writeAndSeal(handle, body)
}

func writeAndSeal(handle *fssecure.Handle, body []byte) error {
	err := writeAndSealOpen(handle, body)
	return errors.Join(err, handle.Close())
}

func writeAndSealOpen(handle *fssecure.Handle, body []byte) error {
	written, writeErr := handle.File().Write(body)
	if writeErr == nil && written != len(body) {
		writeErr = fmt.Errorf("durable publish: short marker write = %d, want %d", written, len(body))
	}
	sealErr := handle.Seal()
	return errors.Join(writeErr, sealErr)
}

func deleteFile(directory *fssecure.Directory, name string) error {
	handle, err := directory.OpenRegular(name)
	if err != nil {
		return err
	}
	deleteErr := handle.MarkDeleteOnClose()
	return errors.Join(deleteErr, handle.Close())
}

func runFailpoint(failpoint FailpointFunc, point Failpoint) error {
	if failpoint == nil {
		return nil
	}
	return failpoint(point)
}
