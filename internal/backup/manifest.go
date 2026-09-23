package backup

import (
	"bytes"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"path"
	"slices"
	"strconv"
	"strings"

	"mahoroba.local/mahoroba/internal/assets/migrations"
	"mahoroba.local/mahoroba/internal/canonical"
	"mahoroba.local/mahoroba/internal/projection"
)

const (
	FormatVersion      = "mahoroba-backup-directory-v1"
	DatabaseBundlePath = "database/mahoroba.db"
	ExternalCopyNotice = "runtime_unmanaged_copy_not_covered_by_future_erasure"
)

type CreatedBy struct {
	BinaryVersion string `json:"binary_version"`
	GitRevision   string `json:"git_revision"`
	GoVersion     string `json:"go_version"`
}

type Schema struct {
	Version     string `json:"version"`
	Fingerprint string `json:"fingerprint"`
}

type MigrationDescriptor struct {
	Version  string `json:"version"`
	Name     string `json:"name"`
	ByteSize string `json:"byte_size"`
	SHA256   string `json:"sha256"`
}

type CapturedHead struct {
	Exists                bool    `json:"exists"`
	CommitID              *string `json:"commit_id"`
	CommitSeq             *string `json:"commit_seq"`
	CommittedAtUnixMicros *string `json:"committed_at_unix_micros"`
	CommittedTZ           *string `json:"committed_tz"`
}

type RuntimeSelection struct {
	ActiveResidentID              *string `json:"active_resident_id"`
	SessionizationPolicyVersionID *string `json:"sessionization_policy_version_id"`
	Source                        string  `json:"source"`
}

type RequiredAction struct {
	Code      string   `json:"code"`
	TargetIDs []string `json:"target_ids"`
}

type ProjectionEntry struct {
	Name    string `json:"name"`
	Version string `json:"version"`
}

type Projections struct {
	Policy  string            `json:"policy"`
	Entries []ProjectionEntry `json:"entries"`
}

type DatabaseFile struct {
	Path     string `json:"path"`
	ByteSize string `json:"byte_size"`
	SHA256   string `json:"sha256"`
}

type BlobFile struct {
	ResidentID string `json:"resident_id"`
	Path       string `json:"path"`
	ByteSize   string `json:"byte_size"`
	SHA256     string `json:"sha256"`
}

type Manifest struct {
	FormatVersion          string                `json:"format_version"`
	CreatedBy              CreatedBy             `json:"created_by"`
	SourceDatabaseFilename string                `json:"source_database_filename"`
	Schema                 Schema                `json:"schema"`
	MigrationDescriptors   []MigrationDescriptor `json:"migration_descriptors"`
	CapturedHead           CapturedHead          `json:"captured_head"`
	RuntimeSelection       RuntimeSelection      `json:"runtime_selection"`
	ServiceReady           bool                  `json:"service_ready"`
	RequiredActions        []RequiredAction      `json:"required_actions"`
	Projections            Projections           `json:"projections"`
	DatabaseFile           DatabaseFile          `json:"database_file"`
	BlobFiles              []BlobFile            `json:"blob_files"`
	ExternalCopyNotice     string                `json:"external_copy_notice"`
	AuthenticityGuaranteed bool                  `json:"authenticity_guaranteed"`
}

func encodeManifest(manifest Manifest) ([]byte, error) {
	if err := validateManifest(manifest); err != nil {
		return nil, err
	}
	encoded, err := canonical.MarshalCanonical(manifest)
	if err != nil {
		return nil, fmt.Errorf("backup: encode manifest: %w", err)
	}
	return append(encoded.Bytes(), '\n'), nil
}

func decodeManifest(input []byte) (Manifest, error) {
	var manifest Manifest
	if len(input) < 2 || input[len(input)-1] != '\n' || input[len(input)-2] == '\n' || bytes.Contains(input[:len(input)-1], []byte{'\r'}) {
		return manifest, invalid("manifest must be one JCS object followed by one LF", nil)
	}
	body := input[:len(input)-1]
	if _, err := canonical.ParseCanonicalJSON(body); err != nil {
		return manifest, invalid("manifest is not exact JCS", err)
	}
	decoder := json.NewDecoder(bytes.NewReader(body))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&manifest); err != nil {
		return Manifest{}, invalid("manifest schema differs", err)
	}
	if _, err := decoder.Token(); !errors.Is(err, io.EOF) {
		return Manifest{}, invalid("manifest contains trailing data", err)
	}
	if err := validateManifest(manifest); err != nil {
		return Manifest{}, err
	}
	return manifest, nil
}

func validateManifest(manifest Manifest) error {
	if manifest.FormatVersion != FormatVersion {
		return invalid("format_version differs", nil)
	}
	if manifest.CreatedBy.BinaryVersion == "" || manifest.CreatedBy.GitRevision == "" || manifest.CreatedBy.GoVersion == "" {
		return invalid("created_by is incomplete", nil)
	}
	if !singleBasename(manifest.SourceDatabaseFilename) {
		return invalid("source_database_filename is not a basename", nil)
	}
	if manifest.Schema.Version != strconv.FormatInt(migrations.BaselineVersion, 10) || !validDigest(manifest.Schema.Fingerprint) {
		return invalid("schema identity differs", nil)
	}
	if err := validateMigrationDescriptors(manifest.MigrationDescriptors); err != nil {
		return err
	}
	if err := validateCapturedHead(manifest.CapturedHead); err != nil {
		return err
	}
	if err := validateRuntimeSelection(manifest.RuntimeSelection); err != nil {
		return err
	}
	if err := validateRequiredActions(manifest.RequiredActions); err != nil {
		return err
	}
	if err := validateProjections(manifest.Projections); err != nil {
		return err
	}
	if manifest.DatabaseFile.Path != DatabaseBundlePath || !validDecimal(manifest.DatabaseFile.ByteSize) || !validDigest(manifest.DatabaseFile.SHA256) {
		return invalid("database_file differs", nil)
	}
	if err := validateBlobFiles(manifest.BlobFiles); err != nil {
		return err
	}
	if manifest.ExternalCopyNotice != ExternalCopyNotice || manifest.AuthenticityGuaranteed {
		return invalid("external copy or authenticity contract differs", nil)
	}
	return nil
}

func validateMigrationDescriptors(values []MigrationDescriptor) error {
	want := migrations.Descriptors()
	if len(values) != len(want) {
		return invalid("migration descriptor count differs", nil)
	}
	for index, descriptor := range want {
		value := values[index]
		if value.Version != strconv.Itoa(index+1) || value.Name != descriptor.Name ||
			value.ByteSize != strconv.Itoa(descriptor.Bytes) || value.SHA256 != "sha256:"+descriptor.SHA256 {
			return invalid("migration descriptor identity differs", nil)
		}
	}
	return nil
}

func validateCapturedHead(head CapturedHead) error {
	values := []*string{head.CommitID, head.CommitSeq, head.CommittedAtUnixMicros, head.CommittedTZ}
	if !head.Exists {
		for _, value := range values {
			if value != nil {
				return invalid("empty captured_head contains identity", nil)
			}
		}
		return nil
	}
	for _, value := range values {
		if value == nil {
			return invalid("captured_head is incomplete", nil)
		}
	}
	if _, err := canonical.ParseID(*head.CommitID); err != nil {
		return invalid("captured_head commit_id is invalid", err)
	}
	if !validPositiveDecimal(*head.CommitSeq) || !validSignedDecimal(*head.CommittedAtUnixMicros) {
		return invalid("captured_head integer is invalid", nil)
	}
	if _, err := canonical.ParseTimezone(*head.CommittedTZ); err != nil {
		return invalid("captured_head timezone is invalid", err)
	}
	return nil
}

func validateRuntimeSelection(selection RuntimeSelection) error {
	for _, value := range []*string{selection.ActiveResidentID, selection.SessionizationPolicyVersionID} {
		if value != nil {
			if _, err := canonical.ParseID(*value); err != nil {
				return invalid("runtime selection ID is invalid", err)
			}
		}
	}
	if selection.ActiveResidentID == nil && selection.SessionizationPolicyVersionID != nil {
		return invalid("session policy exists without resident", nil)
	}
	switch selection.Source {
	case "runtime_config":
		if selection.ActiveResidentID == nil || selection.SessionizationPolicyVersionID == nil {
			return invalid("runtime_config selection is incomplete", nil)
		}
	case "projection_watermark":
		if selection.ActiveResidentID == nil || selection.SessionizationPolicyVersionID == nil {
			return invalid("projection_watermark selection is incomplete", nil)
		}
	case "unresolved":
		if selection.SessionizationPolicyVersionID != nil {
			return invalid("unresolved selection has a session policy", nil)
		}
	default:
		return invalid("runtime selection source is unknown", nil)
	}
	return nil
}

func validateRequiredActions(actions []RequiredAction) error {
	allowed := map[string]struct{}{
		"rebuild_projection": {}, "repair_integrity": {}, "select_active_resident": {}, "select_sessionization_policy": {},
	}
	previous := ""
	for _, action := range actions {
		if _, ok := allowed[action.Code]; !ok || (previous != "" && action.Code <= previous) {
			return invalid("required_actions are unknown, duplicated, or unordered", nil)
		}
		previous = action.Code
		if !slices.IsSorted(action.TargetIDs) {
			return invalid("required action targets are unordered", nil)
		}
		for index, target := range action.TargetIDs {
			if index > 0 && target == action.TargetIDs[index-1] {
				return invalid("required action target is duplicated", nil)
			}
			if _, err := canonical.ParseID(target); err != nil {
				return invalid("required action target is invalid", err)
			}
		}
	}
	return nil
}

func validateProjections(value Projections) error {
	switch value.Policy {
	case "excluded_by_default":
		if len(value.Entries) != 0 {
			return invalid("excluded projections contain entries", nil)
		}
	case "included_requested":
		registry, err := activeProjectionEntries()
		if err != nil {
			return invalid("production projection registry is unavailable", err)
		}
		if !slices.Equal(value.Entries, registry) {
			return invalid("included projection registry differs", nil)
		}
	default:
		return invalid("projection policy is unknown", nil)
	}
	return nil
}

func validateBlobFiles(values []BlobFile) error {
	previous := ""
	residentDigests := make(map[string]struct{}, len(values))
	paths := make(map[string]struct{}, len(values))
	for _, value := range values {
		if _, err := canonical.ParseID(value.ResidentID); err != nil {
			return invalid("blob resident is invalid", err)
		}
		if !validDigest(value.SHA256) || !validDecimal(value.ByteSize) || !validPOSIXRelative(value.Path) {
			return invalid("blob descriptor is invalid", nil)
		}
		hexDigest := strings.TrimPrefix(value.SHA256, "sha256:")
		wantPath := "blobs/objects/" + value.ResidentID + "/" + hexDigest[:2] + "/" + hexDigest[2:]
		if value.Path != wantPath {
			return invalid("blob path is not bound to resident and digest", nil)
		}
		order := value.ResidentID + "\x00" + value.Path
		if previous != "" && order <= previous {
			return invalid("blob descriptors are duplicated or unordered", nil)
		}
		previous = order
		key := value.ResidentID + "\x00" + value.SHA256
		if _, exists := residentDigests[key]; exists {
			return invalid("resident blob digest is duplicated", nil)
		}
		if _, exists := paths[value.Path]; exists {
			return invalid("blob path is duplicated", nil)
		}
		residentDigests[key] = struct{}{}
		paths[value.Path] = struct{}{}
	}
	return nil
}

func activeProjectionEntries() ([]ProjectionEntry, error) {
	registry, err := projectionRegistry()
	if err != nil {
		return nil, err
	}
	definitions := registry.Definitions()
	result := make([]ProjectionEntry, 0, len(definitions))
	for _, definition := range definitions {
		result = append(result, ProjectionEntry{Name: string(definition.Name), Version: string(definition.Version)})
	}
	return result, nil
}

var projectionRegistry = func() (*projection.Registry, error) {
	// Keep this closed list synchronized with sqlite.ActiveProjectionRegistry
	// without importing sqlite into the format-only manifest layer.
	return projection.NewRegistry(
		projection.Definition{Name: projection.ResidentCurrentStatusName, Version: "resident-current-status-v1"},
		projection.Definition{Name: projection.ResidentCurrentRevisionName, Version: "resident-current-revision-v1"},
		projection.Definition{Name: projection.RuntimeStatesName, Version: "runtime-states-v2", TimeSensitive: true},
		projection.ClaimStatesDefinition(),
		projection.ClaimViewScopeCurrentDefinition(),
		projection.ContentReferencesDefinition(),
	)
}

func migrationManifest() []MigrationDescriptor {
	values := migrations.Descriptors()
	result := make([]MigrationDescriptor, 0, len(values))
	for index, value := range values {
		result = append(result, MigrationDescriptor{
			Version: strconv.Itoa(index + 1), Name: value.Name, ByteSize: strconv.Itoa(value.Bytes), SHA256: "sha256:" + value.SHA256,
		})
	}
	return result
}

func validDigest(value string) bool {
	if len(value) != len("sha256:")+64 || !strings.HasPrefix(value, "sha256:") {
		return false
	}
	raw := strings.TrimPrefix(value, "sha256:")
	if raw != strings.ToLower(raw) {
		return false
	}
	decoded, err := hex.DecodeString(raw)
	return err == nil && len(decoded) == 32
}

func validDecimal(value string) bool {
	if value == "0" {
		return true
	}
	if value == "" || value[0] == '0' {
		return false
	}
	_, err := strconv.ParseUint(value, 10, 63)
	return err == nil
}

func validPositiveDecimal(value string) bool { return value != "0" && validDecimal(value) }

func validSignedDecimal(value string) bool {
	if value == "0" {
		return true
	}
	if strings.HasPrefix(value, "-") {
		return len(value) > 1 && value[1] != '0' && validDecimal(value[1:])
	}
	return validDecimal(value)
}

func singleBasename(value string) bool {
	return value != "" && value != "." && value != ".." && path.Base(value) == value && !strings.ContainsAny(value, `/\\\x00`)
}

func validPOSIXRelative(value string) bool {
	return value != "" && !strings.Contains(value, "\\") && !strings.HasPrefix(value, "/") && path.Clean(value) == value &&
		!strings.ContainsRune(value, 0) && !strings.Contains("/"+value+"/", "/../")
}

func invalid(message string, cause error) error {
	if cause == nil {
		return fmt.Errorf("%w: %s", ErrInvalidBundle, message)
	}
	return fmt.Errorf("%w: %s: %v", ErrInvalidBundle, message, cause)
}
