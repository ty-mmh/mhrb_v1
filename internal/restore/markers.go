package restore

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"path/filepath"
	"strings"

	"mahoroba.local/mahoroba/internal/backup"
	"mahoroba.local/mahoroba/internal/canonical"
	"mahoroba.local/mahoroba/internal/fssecure"
)

type StagingMarker struct {
	FormatVersion  string       `json:"format_version"`
	ManifestSHA256 string       `json:"manifest_sha256"`
	RestoreID      canonical.ID `json:"restore_id"`
	StagingName    string       `json:"staging_basename"`
	TargetName     string       `json:"target_basename"`
}

func (marker StagingMarker) Validate() error {
	if marker.FormatVersion != RestoreStagingFormat {
		return errors.New("restore: invalid staging marker format")
	}
	if err := marker.RestoreID.Validate(); err != nil {
		return errors.New("restore: invalid staging marker ID")
	}
	if !strings.HasPrefix(marker.ManifestSHA256, "sha256:") {
		return errors.New("restore: invalid staging marker digest")
	}
	if _, err := canonical.ParseDigestHex(strings.TrimPrefix(marker.ManifestSHA256, "sha256:")); err != nil {
		return errors.New("restore: invalid staging marker digest")
	}
	if filepath.Base(marker.TargetName) != marker.TargetName || marker.TargetName == "" || marker.TargetName == "." ||
		filepath.Base(marker.StagingName) != marker.StagingName ||
		marker.StagingName != "."+marker.TargetName+".restore-staging."+marker.RestoreID.String() {
		return errors.New("restore: invalid staging marker namespace")
	}
	return nil
}

// ParseStagingMarker authenticates the exact, one-line JCS marker used by
// absent-target diagnostics. It returns semantic fields only; no path is
// accepted or produced.
func ParseStagingMarker(input []byte) (StagingMarker, error) {
	var marker StagingMarker
	if len(input) < 2 || input[len(input)-1] != '\n' || input[len(input)-2] == '\n' ||
		bytes.Contains(input[:len(input)-1], []byte{'\r'}) {
		return marker, errors.New("restore: invalid staging marker framing")
	}
	body := input[:len(input)-1]
	parsed, err := canonical.ParseCanonicalJSON(body)
	if err != nil || parsed.String() != string(body) {
		return marker, errors.New("restore: invalid staging marker JCS")
	}
	decoder := json.NewDecoder(bytes.NewReader(body))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&marker); err != nil {
		return StagingMarker{}, errors.New("restore: invalid staging marker schema")
	}
	if _, err := decoder.Token(); !errors.Is(err, io.EOF) {
		return StagingMarker{}, errors.New("restore: invalid staging marker trailing data")
	}
	if err := marker.Validate(); err != nil {
		return StagingMarker{}, err
	}
	return marker, nil
}

func manifestBytes(manifest backup.Manifest) ([]byte, error) {
	encoded, err := canonical.MarshalCanonical(manifest)
	if err != nil {
		return nil, err
	}
	return append(encoded.Bytes(), '\n'), nil
}

func manifestDigest(manifest backup.Manifest) (canonical.Digest, error) {
	body, err := manifestBytes(manifest)
	if err != nil {
		return canonical.Digest{}, err
	}
	return canonical.HashBlob(body), nil
}

func encodeRestoreStagingMarker(
	manifest backup.Manifest,
	restoreID canonical.ID,
	stagingName, targetName string,
) ([]byte, canonical.Digest, error) {
	digest, err := manifestDigest(manifest)
	if err != nil {
		return nil, canonical.Digest{}, err
	}
	marker := StagingMarker{
		FormatVersion: RestoreStagingFormat, ManifestSHA256: "sha256:" + digest.Hex(),
		RestoreID: restoreID, StagingName: stagingName, TargetName: targetName,
	}
	if err := marker.Validate(); err != nil {
		return nil, canonical.Digest{}, err
	}
	encoded, err := canonical.MarshalCanonical(marker)
	if err != nil {
		return nil, canonical.Digest{}, err
	}
	return append(encoded.Bytes(), '\n'), digest, nil
}

func writeManagedFile(directory *fssecure.Directory, name string, body []byte) error {
	handle, err := directory.CreateRegular(name)
	if err != nil {
		return err
	}
	written, writeErr := handle.File().Write(body)
	if writeErr == nil && written != len(body) {
		writeErr = fmt.Errorf("restore: short marker write = %d, want %d", written, len(body))
	}
	sealErr := handle.Seal()
	closeErr := handle.Close()
	if writeErr == nil && sealErr == nil {
		return closeErr
	}
	return errors.Join(writeErr, sealErr, closeErr)
}

func writeBundleEntry(
	ctx context.Context,
	root *fssecure.Directory,
	relative string,
	expectedSize int64,
	expectedSHA string,
	reader io.Reader,
) error {
	if relative == backup.DatabaseBundlePath {
		relative = DatabaseFilename
	}
	clean := filepath.ToSlash(filepath.Clean(filepath.FromSlash(relative)))
	if clean != relative || relative == "" || strings.HasPrefix(relative, "/") || strings.Contains("/"+relative+"/", "/../") {
		return errors.New("restore: unsafe bundle entry path")
	}
	parts := strings.Split(relative, "/")
	if len(parts)+1 > 16 {
		return errors.New("restore: bundle entry exceeds 16-handle copy bound")
	}
	directory := root
	opened := make([]*fssecure.Directory, 0, len(parts)-1)
	defer func() {
		for index := len(opened) - 1; index >= 0; index-- {
			_ = opened[index].Close()
		}
	}()
	for _, component := range parts[:len(parts)-1] {
		child, err := directory.OpenOrCreateDirectory(component)
		if err != nil {
			return err
		}
		opened = append(opened, child)
		directory = child
	}
	handle, err := directory.CreateRegular(parts[len(parts)-1])
	if err != nil {
		return err
	}
	hasher := sha256.New()
	written, copyErr := io.Copy(io.MultiWriter(handle.File(), hasher), io.LimitReader(reader, expectedSize+1))
	if copyErr == nil && written != expectedSize {
		copyErr = fmt.Errorf("restore: copied entry size = %d, want %d", written, expectedSize)
	}
	actualSHA := "sha256:" + hex.EncodeToString(hasher.Sum(nil))
	if copyErr == nil && actualSHA != expectedSHA {
		copyErr = errors.New("restore: copied entry digest differs")
	}
	sealErr := handle.Seal()
	closeErr := handle.Close()
	syncErr := directory.Sync()
	for index := len(opened) - 2; index >= 0; index-- {
		syncErr = errors.Join(syncErr, opened[index].Sync())
	}
	syncErr = errors.Join(syncErr, root.Sync())
	if err := ctx.Err(); copyErr == nil && err != nil {
		copyErr = err
	}
	return errors.Join(copyErr, sealErr, closeErr, syncErr)
}
