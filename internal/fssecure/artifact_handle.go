package fssecure

import (
	"crypto/sha256"
	"encoding/hex"
	"path/filepath"
	"runtime"

	"mahoroba.local/mahoroba/internal/canonical"
)

// ArtifactSourceIdentityDigest binds one retained regular-file handle to the
// same domain used for directory artifact sources. The path is diagnostic
// input to the hash only; callers receive no plaintext authority token.
func (handle *Handle) ArtifactSourceIdentityDigest() (string, error) {
	if err := handle.VerifyBound(); err != nil {
		return "", err
	}
	canonicalPath, err := filepath.EvalSymlinks(handle.path)
	if err != nil {
		return "", err
	}
	canonicalPath, err = filepath.Abs(canonicalPath)
	if err != nil {
		return "", err
	}
	if err := handle.VerifyBound(); err != nil {
		return "", err
	}
	fileIdentity, volumeIdentity := handle.identity.artifactIdentityFields()
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
