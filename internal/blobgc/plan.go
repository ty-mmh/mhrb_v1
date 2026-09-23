package blobgc

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"slices"
	"strings"

	"mahoroba.local/mahoroba/internal/blob"
	"mahoroba.local/mahoroba/internal/canonical"
)

type Candidate struct {
	ResidentID         canonical.ID        `json:"resident_id"`
	HashAlgorithm      string              `json:"hash_algorithm"`
	BlobHash           string              `json:"blob_hash"`
	SQLitePresent      bool                `json:"sqlite_present"`
	SQLiteByteSize     *canonical.ByteSize `json:"sqlite_byte_size"`
	FilesystemPresent  bool                `json:"filesystem_present"`
	FilesystemByteSize *canonical.ByteSize `json:"filesystem_byte_size"`

	digest     canonical.Digest
	filesystem *blob.FinalObject
}

func (candidate Candidate) Digest() canonical.Digest { return candidate.digest }

func (candidate Candidate) Validate() error {
	if err := candidate.ResidentID.Validate(); err != nil {
		return err
	}
	if candidate.HashAlgorithm != canonical.HashAlgorithm || candidate.BlobHash != "sha256:"+candidate.digest.Hex() {
		return errors.New("blob gc: candidate locator encoding mismatch")
	}
	if candidate.SQLitePresent != (candidate.SQLiteByteSize != nil) || candidate.FilesystemPresent != (candidate.FilesystemByteSize != nil) ||
		(!candidate.SQLitePresent && !candidate.FilesystemPresent) {
		return errors.New("blob gc: candidate physical presence is invalid")
	}
	if candidate.SQLiteByteSize != nil {
		if err := candidate.SQLiteByteSize.Validate(); err != nil {
			return err
		}
	}
	if candidate.FilesystemByteSize != nil {
		if err := candidate.FilesystemByteSize.Validate(); err != nil {
			return err
		}
	}
	if candidate.SQLiteByteSize != nil && candidate.FilesystemByteSize != nil &&
		*candidate.SQLiteByteSize != *candidate.FilesystemByteSize {
		return errors.New("blob gc: dual-copy byte sizes differ")
	}
	if candidate.FilesystemPresent {
		if candidate.filesystem == nil || candidate.filesystem.ResidentID() != candidate.ResidentID ||
			candidate.filesystem.Digest() != candidate.digest || candidate.filesystem.Size() != *candidate.FilesystemByteSize {
			return errors.New("blob gc: candidate filesystem token mismatch")
		}
	} else if candidate.filesystem != nil {
		return errors.New("blob gc: absent filesystem copy has a token")
	}
	return nil
}

type Plan struct {
	FormatVersion      string              `json:"format_version"`
	ResidentID         canonical.ID        `json:"resident_id"`
	CapturedHead       CapturedHead        `json:"captured_head"`
	ProjectionName     string              `json:"projection_name"`
	ProjectionVersion  string              `json:"projection_version"`
	DependencyVersions []DependencyVersion `json:"dependency_versions"`
	Candidates         []Candidate         `json:"candidates"`

	digest string
}

func (plan Plan) Digest() string { return plan.digest }

func (plan Plan) Validate() error {
	if plan.FormatVersion != PlanFormatVersion || plan.ProjectionName != ProjectionName || plan.ProjectionVersion != ProjectionVersion {
		return errors.New("blob gc: plan version identity mismatch")
	}
	if err := plan.ResidentID.Validate(); err != nil {
		return err
	}
	if err := plan.CapturedHead.Validate(); err != nil || !plan.CapturedHead.Exists {
		return errors.Join(errors.New("blob gc: plan requires a non-empty head"), err)
	}
	if len(plan.DependencyVersions) != 0 {
		return fmt.Errorf("%w: v1 dependency set must be empty", ErrProjectionNotCurrent)
	}
	previous := ""
	for index, candidate := range plan.Candidates {
		if err := candidate.Validate(); err != nil {
			return fmt.Errorf("blob gc: candidate %d: %w", index, err)
		}
		if candidate.ResidentID != plan.ResidentID {
			return errors.New("blob gc: cross-resident candidate")
		}
		key := candidate.ResidentID.String() + "\x00" + candidate.HashAlgorithm + "\x00" + candidate.digest.Hex()
		if index > 0 && key <= previous {
			return errors.New("blob gc: candidates are unordered or duplicated")
		}
		previous = key
	}
	encoded, err := plan.canonicalBytes()
	if err != nil {
		return err
	}
	hasher := sha256.New()
	_, _ = hasher.Write([]byte(PlanDigestDomain + "\x00"))
	_, _ = hasher.Write(encoded)
	want := "sha256:" + hex.EncodeToString(hasher.Sum(nil))
	if plan.digest != want {
		return errors.New("blob gc: plan digest mismatch")
	}
	return nil
}

func (plan Plan) CanonicalBytes() ([]byte, error) { return plan.canonicalBytes() }

func (plan Plan) canonicalBytes() ([]byte, error) {
	// Project the exported wire fields so private authority tokens and cached
	// digest state can never enter the plan evidence.
	projection := struct {
		FormatVersion      string              `json:"format_version"`
		ResidentID         canonical.ID        `json:"resident_id"`
		CapturedHead       CapturedHead        `json:"captured_head"`
		ProjectionName     string              `json:"projection_name"`
		ProjectionVersion  string              `json:"projection_version"`
		DependencyVersions []DependencyVersion `json:"dependency_versions"`
		Candidates         []Candidate         `json:"candidates"`
	}{
		plan.FormatVersion, plan.ResidentID, plan.CapturedHead, plan.ProjectionName,
		plan.ProjectionVersion, nonNil(plan.DependencyVersions), nonNil(plan.Candidates),
	}
	encoded, err := canonical.MarshalCanonical(projection)
	if err != nil {
		return nil, err
	}
	return encoded.Bytes(), nil
}

func buildPlan(snapshot Snapshot, residentID canonical.ID, files []blob.FinalObject) (Plan, int64, error) {
	if err := validateSnapshot(snapshot, residentID); err != nil {
		return Plan{}, 0, err
	}
	fileByDigest := make(map[canonical.Digest]blob.FinalObject, len(files))
	for _, object := range files {
		if object.ResidentID() != residentID || object.HashAlgorithm() != canonical.HashAlgorithm {
			return Plan{}, 0, fmt.Errorf("%w: filesystem object scope mismatch", ErrContentIntegrity)
		}
		if _, exists := fileByDigest[object.Digest()]; exists {
			return Plan{}, 0, fmt.Errorf("%w: duplicate filesystem locator", ErrContentIntegrity)
		}
		fileByDigest[object.Digest()] = object
	}

	states := append([]LocatorState(nil), snapshot.Locators...)
	slices.SortFunc(states, func(left, right LocatorState) int {
		return strings.Compare(left.ResidentID.String()+"\x00"+left.HashAlgorithm+"\x00"+left.Digest.Hex(),
			right.ResidentID.String()+"\x00"+right.HashAlgorithm+"\x00"+right.Digest.Hex())
	})
	plan := Plan{
		FormatVersion: PlanFormatVersion, ResidentID: residentID, CapturedHead: snapshot.CapturedHead,
		ProjectionName: ProjectionName, ProjectionVersion: ProjectionVersion,
		DependencyVersions: []DependencyVersion{}, Candidates: []Candidate{},
	}
	var totalBytes int64
	for index, state := range states {
		if state.ResidentID != residentID || state.HashAlgorithm != canonical.HashAlgorithm ||
			state.AuthoritativeReferences < 0 || state.ProjectionReferences < 0 ||
			state.SQLitePresent != (state.SQLiteByteSize != nil) {
			return Plan{}, 0, fmt.Errorf("%w: invalid captured locator", ErrContentIntegrity)
		}
		if index > 0 && states[index-1].ResidentID == state.ResidentID && states[index-1].HashAlgorithm == state.HashAlgorithm && states[index-1].Digest == state.Digest {
			return Plan{}, 0, fmt.Errorf("%w: duplicate captured locator", ErrContentIntegrity)
		}
		file, filesystemPresent := fileByDigest[state.Digest]
		delete(fileByDigest, state.Digest)
		if !state.SQLitePresent && !filesystemPresent {
			return Plan{}, 0, fmt.Errorf("%w: captured locator has no physical copy", ErrContentIntegrity)
		}
		if state.AuthoritativeReferences != 0 || state.ProjectionReferences != 0 {
			continue
		}
		candidate := Candidate{
			ResidentID: residentID, HashAlgorithm: canonical.HashAlgorithm,
			BlobHash: "sha256:" + state.Digest.Hex(), digest: state.Digest,
			SQLitePresent: state.SQLitePresent, SQLiteByteSize: cloneSize(state.SQLiteByteSize),
			FilesystemPresent: filesystemPresent,
		}
		if filesystemPresent {
			size := file.Size()
			candidate.FilesystemByteSize = &size
			copy := file
			candidate.filesystem = &copy
		}
		if candidate.SQLiteByteSize != nil {
			if totalBytes > int64(^uint64(0)>>1)-candidate.SQLiteByteSize.Int64() {
				return Plan{}, 0, errors.New("blob gc: candidate byte count overflow")
			}
			totalBytes += candidate.SQLiteByteSize.Int64()
		}
		if candidate.FilesystemByteSize != nil {
			if totalBytes > int64(^uint64(0)>>1)-candidate.FilesystemByteSize.Int64() {
				return Plan{}, 0, errors.New("blob gc: candidate byte count overflow")
			}
			totalBytes += candidate.FilesystemByteSize.Int64()
		}
		plan.Candidates = append(plan.Candidates, candidate)
	}
	if len(fileByDigest) != 0 {
		return Plan{}, 0, fmt.Errorf("%w: SQLite snapshot omitted a filesystem locator", ErrContentIntegrity)
	}
	encoded, err := plan.canonicalBytes()
	if err != nil {
		return Plan{}, 0, err
	}
	hasher := sha256.New()
	_, _ = hasher.Write([]byte(PlanDigestDomain + "\x00"))
	_, _ = hasher.Write(encoded)
	plan.digest = "sha256:" + hex.EncodeToString(hasher.Sum(nil))
	if err := plan.Validate(); err != nil {
		return Plan{}, 0, err
	}
	return plan, totalBytes, nil
}

func validateSnapshot(snapshot Snapshot, residentID canonical.ID) error {
	if err := residentID.Validate(); err != nil {
		return err
	}
	if err := snapshot.CapturedHead.Validate(); err != nil || !snapshot.CapturedHead.Exists {
		return fmt.Errorf("%w: invalid captured head", ErrProjectionNotCurrent)
	}
	if snapshot.ProjectionName != ProjectionName || snapshot.ProjectionVersion != ProjectionVersion || len(snapshot.DependencyVersions) != 0 {
		return fmt.Errorf("%w: Projection identity/version/dependencies differ", ErrProjectionNotCurrent)
	}
	return nil
}

func cloneSize(value *canonical.ByteSize) *canonical.ByteSize {
	if value == nil {
		return nil
	}
	copy := *value
	return &copy
}

func nonNil[T any](values []T) []T {
	if values == nil {
		return []T{}
	}
	return values
}
