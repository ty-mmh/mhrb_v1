package durablepublish

import (
	"fmt"
	"path/filepath"
	"slices"
	"strconv"
	"strings"

	"mahoroba.local/mahoroba/internal/canonical"
)

// CapturedHead is the exact artifact-producer head projection used as a
// DurablePublish input. Logical integers use decimal strings.
type CapturedHead struct {
	Exists      bool    `json:"exists"`
	CommitID    *string `json:"commit_id"`
	CommitSeq   *string `json:"commit_seq"`
	CommittedAt *string `json:"committed_at"`
	CommittedTZ *string `json:"committed_tz"`
}

type BackupCreateProducerInput struct {
	SourceDataDirIdentity string       `json:"source_data_dir_identity"`
	SchemaFingerprint     string       `json:"schema_fingerprint"`
	CapturedHead          CapturedHead `json:"captured_head"`
	IncludeProjections    bool         `json:"include_projections"`
}

type BackupRestoreProducerInput struct {
	BundleRootIdentity     string  `json:"bundle_root_identity"`
	BundleManifestSHA256   string  `json:"bundle_manifest_sha256"`
	TargetConfigSHA256     *string `json:"target_config_sha256"`
	TargetDatabaseFilename string  `json:"target_database_filename"`
}

type ExportJSONLProducerInput struct {
	SourceDataDirIdentity string       `json:"source_data_dir_identity"`
	SchemaFingerprint     string       `json:"schema_fingerprint"`
	CapturedHead          CapturedHead `json:"captured_head"`
	FormatVersion         string       `json:"format_version"`
}

type ErasurePlanBaseHead struct {
	CommitID  string `json:"commit_id"`
	CommitSeq string `json:"commit_seq"`
}

type ErasurePlanContentProducerInput struct {
	SourceDataDirIdentity string              `json:"source_data_dir_identity"`
	ResidentID            string              `json:"resident_id"`
	RequestedContentIDs   []string            `json:"requested_content_ids"`
	ReasonCode            string              `json:"reason_code"`
	BaseHead              ErasurePlanBaseHead `json:"base_head"`
	ImpactVersion         string              `json:"impact_version"`
}

type ErasurePlanResidentProducerInput struct {
	SourceDataDirIdentity string              `json:"source_data_dir_identity"`
	ResidentID            string              `json:"resident_id"`
	ReasonCode            string              `json:"reason_code"`
	BaseHead              ErasurePlanBaseHead `json:"base_head"`
	ImpactVersion         string              `json:"impact_version"`
}

type ErasureDecision struct {
	ImpactID string `json:"impact_id"`
	Decision string `json:"decision"`
}

type ErasureDecideProducerInput struct {
	SourceDataDirIdentity string            `json:"source_data_dir_identity"`
	InputPlanIdentity     string            `json:"input_plan_identity"`
	InputPlanDigest       string            `json:"input_plan_digest"`
	Decisions             []ErasureDecision `json:"decisions"`
}

// ProducerInputDigest admits only concrete, command-matched exact schemas.
// Later producers add their own exact type before they may use this helper;
// arbitrary maps and anonymous structs are deliberately rejected.
func ProducerInputDigest(command string, input any) (canonical.Digest, error) {
	if _, ok := producerCommands[command]; !ok {
		return canonical.Digest{}, fmt.Errorf("%w: producer command", ErrInvalidProducerInput)
	}
	switch value := input.(type) {
	case BackupCreateProducerInput:
		if command != CommandBackupCreate || !validDigest(value.SourceDataDirIdentity) ||
			!validDigest(value.SchemaFingerprint) || validateCapturedHead(value.CapturedHead) != nil {
			return canonical.Digest{}, fmt.Errorf("%w: backup.create", ErrInvalidProducerInput)
		}
	case BackupRestoreProducerInput:
		if command != CommandBackupRestore || !validDigest(value.BundleRootIdentity) ||
			!validDigest(value.BundleManifestSHA256) ||
			value.TargetConfigSHA256 != nil && !validDigest(*value.TargetConfigSHA256) ||
			value.TargetDatabaseFilename != "mahoroba.db" ||
			!validBasename(value.TargetDatabaseFilename) || filepath.Clean(value.TargetDatabaseFilename) != value.TargetDatabaseFilename {
			return canonical.Digest{}, fmt.Errorf("%w: backup.restore", ErrInvalidProducerInput)
		}
	case ExportJSONLProducerInput:
		if command != CommandExportJSONL || !validDigest(value.SourceDataDirIdentity) ||
			!validDigest(value.SchemaFingerprint) || validateCapturedHead(value.CapturedHead) != nil ||
			value.FormatVersion != "mahoroba-jsonl-v1" {
			return canonical.Digest{}, fmt.Errorf("%w: export.jsonl", ErrInvalidProducerInput)
		}
	case ErasurePlanContentProducerInput:
		if command != CommandErasurePlanContent || !validDigest(value.SourceDataDirIdentity) ||
			validateErasureResident(value.ResidentID) != nil ||
			validateErasureContentIDs(value.RequestedContentIDs) != nil ||
			!validReasonCode(value.ReasonCode) || validateErasureBaseHead(value.BaseHead) != nil ||
			value.ImpactVersion != "erasure-impact-v1" {
			return canonical.Digest{}, fmt.Errorf("%w: admin.erasure.plan.content", ErrInvalidProducerInput)
		}
	case ErasurePlanResidentProducerInput:
		if command != CommandErasurePlanResident || !validDigest(value.SourceDataDirIdentity) ||
			validateErasureResident(value.ResidentID) != nil || !validReasonCode(value.ReasonCode) ||
			validateErasureBaseHead(value.BaseHead) != nil || value.ImpactVersion != "erasure-impact-v1" {
			return canonical.Digest{}, fmt.Errorf("%w: admin.erasure.plan.resident", ErrInvalidProducerInput)
		}
	case ErasureDecideProducerInput:
		if command != CommandErasureDecide || !validDigest(value.SourceDataDirIdentity) ||
			!validDigest(value.InputPlanIdentity) || !validDigest(value.InputPlanDigest) ||
			validateErasureDecisions(value.Decisions) != nil {
			return canonical.Digest{}, fmt.Errorf("%w: admin.erasure.decide", ErrInvalidProducerInput)
		}
	default:
		return canonical.Digest{}, fmt.Errorf("%w: exact schema is unavailable for %s", ErrInvalidProducerInput, command)
	}
	return producerInputDigest(input)
}

func validateErasureResident(value string) error {
	_, err := canonical.ParseID(value)
	return err
}

func validateErasureContentIDs(values []string) error {
	if len(values) == 0 || !slices.IsSorted(values) {
		return ErrInvalidProducerInput
	}
	for index, value := range values {
		if index > 0 && value == values[index-1] {
			return ErrInvalidProducerInput
		}
		if _, err := canonical.ParseID(value); err != nil {
			return err
		}
	}
	return nil
}

func validateErasureBaseHead(head ErasurePlanBaseHead) error {
	if _, err := canonical.ParseID(head.CommitID); err != nil {
		return err
	}
	seq, err := strconv.ParseInt(head.CommitSeq, 10, 64)
	if err != nil || seq <= 0 || strconv.FormatInt(seq, 10) != head.CommitSeq {
		return ErrInvalidProducerInput
	}
	return nil
}

func validReasonCode(value string) bool {
	if value == "" || value[0] < 'a' || value[0] > 'z' || len(value) > 64 {
		return false
	}
	for _, character := range value[1:] {
		if character != '_' && (character < 'a' || character > 'z') && (character < '0' || character > '9') {
			return false
		}
	}
	return true
}

func validateErasureDecisions(values []ErasureDecision) error {
	if values == nil || !slices.IsSortedFunc(values, func(left, right ErasureDecision) int {
		return strings.Compare(left.ImpactID, right.ImpactID)
	}) {
		return ErrInvalidProducerInput
	}
	for index, value := range values {
		if !validDigest(value.ImpactID) || value.Decision != "retain" && value.Decision != "erase" ||
			index > 0 && value.ImpactID == values[index-1].ImpactID {
			return ErrInvalidProducerInput
		}
	}
	return nil
}

func validateCapturedHead(head CapturedHead) error {
	if !head.Exists {
		if head.CommitID != nil || head.CommitSeq != nil || head.CommittedAt != nil || head.CommittedTZ != nil {
			return ErrInvalidProducerInput
		}
		return nil
	}
	if head.CommitID == nil || head.CommitSeq == nil || head.CommittedAt == nil || head.CommittedTZ == nil {
		return ErrInvalidProducerInput
	}
	if _, err := canonical.ParseID(*head.CommitID); err != nil {
		return err
	}
	seq, err := strconv.ParseInt(*head.CommitSeq, 10, 64)
	if err != nil || seq <= 0 || strconv.FormatInt(seq, 10) != *head.CommitSeq {
		return ErrInvalidProducerInput
	}
	instant, err := strconv.ParseInt(*head.CommittedAt, 10, 64)
	if err != nil || instant < 0 || strconv.FormatInt(instant, 10) != *head.CommittedAt {
		return ErrInvalidProducerInput
	}
	if _, err := canonical.ParseTimezone(*head.CommittedTZ); err != nil {
		return err
	}
	return nil
}
