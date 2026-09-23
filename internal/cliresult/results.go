package cliresult

import (
	"fmt"
	"math/big"
	"slices"
)

const (
	familyIntegrityScan       resultFamily = "integrity_scan"
	familyBackup              resultFamily = "backup"
	familyBackupRestore       resultFamily = "backup_restore"
	familyExportJSONL         resultFamily = "export_jsonl"
	familyRecoveryTerminalize resultFamily = "recovery_terminalize"
	familySessionPolicySelect resultFamily = "session_policy_select"
	familyErasurePlan         resultFamily = "erasure_plan"
	familyErasureApply        resultFamily = "erasure_apply"
	familyBlobGC              resultFamily = "blob_gc"
	familyDiagnostics         resultFamily = "diagnostics"
	familyHealthcheck         resultFamily = "healthcheck"
	familyError               resultFamily = "error"
)

const (
	BackupFormatVersion            = "mahoroba-backup-directory-v1"
	JSONLFormatVersion             = "mahoroba-jsonl-v1"
	ErasurePlanFormatVersion       = "mahoroba-erasure-plan-v1"
	ExternalCopyNotice             = "runtime_unmanaged_copy_not_covered_by_future_erasure"
	ProjectionStateCurrent         = "current"
	ProjectionStateNotCurrent      = "not_current"
	HealthReasonReady              = "ready"
	HealthReasonStartupIncomplete  = "startup_incomplete"
	HealthReasonResidentUnselected = "active_resident_unselected"
	HealthReasonResidentNotActive  = "active_resident_not_active"
	HealthReasonMemoryPolicy       = "memory_policy_not_service_current"
	HealthReasonSessionUnresolved  = "session_policy_unresolved"
	HealthReasonIntegrityBlocked   = "integrity_readiness_blocked"
	HealthReasonProjectionCurrent  = "projection_not_current"
	HealthReasonShutdown           = "shutdown_in_progress"
)

type Head struct {
	Exists      bool    `json:"exists"`
	CommitID    *string `json:"commit_id"`
	CommitSeq   *string `json:"commit_seq"`
	CommittedAt *string `json:"committed_at"`
	CommittedTZ *string `json:"committed_tz"`
}

func (head Head) Validate() error {
	if !head.Exists {
		if head.CommitID != nil || head.CommitSeq != nil || head.CommittedAt != nil || head.CommittedTZ != nil {
			return fmt.Errorf("empty head requires four null detail fields")
		}
		return nil
	}
	if head.CommitID == nil || head.CommitSeq == nil || head.CommittedAt == nil || head.CommittedTZ == nil {
		return fmt.Errorf("existing head requires four non-null detail fields")
	}
	if !validULID(*head.CommitID) {
		return fmt.Errorf("head commit_id is not canonical ULID")
	}
	if _, ok := parsePositiveDecimal(*head.CommitSeq); !ok {
		return fmt.Errorf("head commit_seq is not a positive decimal string")
	}
	if !validDecimal(*head.CommittedAt) {
		return fmt.Errorf("head committed_at is not a decimal string")
	}
	if !validTimezone(*head.CommittedTZ) {
		return fmt.Errorf("head committed_tz is not an IANA name")
	}
	return nil
}

type IntegrityScanResult struct {
	CapturedHead       Head   `json:"captured_head"`
	ResultHead         Head   `json:"result_head"`
	ExistingFindings   string `json:"existing_findings"`
	CreatedFindings    string `json:"created_findings"`
	CreatedQuarantines string `json:"created_quarantines"`
	ReadinessBlocked   bool   `json:"readiness_blocked"`
}

func (*IntegrityScanResult) resultFamily() resultFamily { return familyIntegrityScan }
func (result *IntegrityScanResult) validateResult(command CommandID) error {
	if command != CommandAdminIntegrityScan {
		return wrongFamily(command, familyIntegrityScan)
	}
	if result == nil {
		return fmt.Errorf("nil integrity result")
	}
	if err := result.CapturedHead.Validate(); err != nil {
		return fmt.Errorf("captured_head: %w", err)
	}
	if err := result.ResultHead.Validate(); err != nil {
		return fmt.Errorf("result_head: %w", err)
	}
	return validateDecimals(result.ExistingFindings, result.CreatedFindings, result.CreatedQuarantines)
}

type BackupResult struct {
	ArtifactPath           string `json:"artifact_path"`
	FormatVersion          string `json:"format_version"`
	CapturedHead           Head   `json:"captured_head"`
	ProjectionsIncluded    bool   `json:"projections_included"`
	FileCount              string `json:"file_count"`
	ByteCount              string `json:"byte_count"`
	AuthenticityGuaranteed bool   `json:"authenticity_guaranteed"`
}

func (*BackupResult) resultFamily() resultFamily { return familyBackup }
func (result *BackupResult) validateResult(command CommandID) error {
	if command != CommandBackupCreate && command != CommandBackupVerify {
		return wrongFamily(command, familyBackup)
	}
	if result == nil || !validAbsolutePath(result.ArtifactPath) {
		return fmt.Errorf("artifact_path is not canonical absolute")
	}
	if result.FormatVersion != BackupFormatVersion {
		return fmt.Errorf("backup format_version is not closed")
	}
	if err := result.CapturedHead.Validate(); err != nil {
		return fmt.Errorf("captured_head: %w", err)
	}
	if result.AuthenticityGuaranteed {
		return fmt.Errorf("authenticity_guaranteed must be false in v1")
	}
	return validateDecimals(result.FileCount, result.ByteCount)
}

type BackupRestoreResult struct {
	TargetDataDir        string `json:"target_data_dir"`
	DatabaseFilename     string `json:"database_filename"`
	SourceHead           Head   `json:"source_head"`
	RestoredHead         Head   `json:"restored_head"`
	TerminalizedAttempts string `json:"terminalized_attempts"`
	FindingsCreated      string `json:"findings_created"`
	ProjectionsRebuilt   string `json:"projections_rebuilt"`
	Published            bool   `json:"published"`
	StagingMarkerID      string `json:"staging_marker_id"`
	ServiceReady         bool   `json:"service_ready"`
}

func (*BackupRestoreResult) resultFamily() resultFamily { return familyBackupRestore }
func (result *BackupRestoreResult) validateResult(command CommandID) error {
	if command != CommandBackupRestore {
		return wrongFamily(command, familyBackupRestore)
	}
	if result == nil || !validAbsolutePath(result.TargetDataDir) || !validBasename(result.DatabaseFilename) {
		return fmt.Errorf("restore target path or database filename is invalid")
	}
	if err := result.SourceHead.Validate(); err != nil {
		return fmt.Errorf("source_head: %w", err)
	}
	if err := result.RestoredHead.Validate(); err != nil {
		return fmt.Errorf("restored_head: %w", err)
	}
	if !validULID(result.StagingMarkerID) {
		return fmt.Errorf("staging_marker_id is not canonical ULID")
	}
	return validateDecimals(result.TerminalizedAttempts, result.FindingsCreated, result.ProjectionsRebuilt)
}

type ExportJSONLResult struct {
	ArtifactPath       string `json:"artifact_path"`
	FormatVersion      string `json:"format_version"`
	CapturedHead       Head   `json:"captured_head"`
	RecordCount        string `json:"record_count"`
	ByteCount          string `json:"byte_count"`
	ExternalCopyNotice string `json:"external_copy_notice"`
}

func (*ExportJSONLResult) resultFamily() resultFamily { return familyExportJSONL }
func (result *ExportJSONLResult) validateResult(command CommandID) error {
	if command != CommandExportJSONL {
		return wrongFamily(command, familyExportJSONL)
	}
	if result == nil || !validAbsolutePath(result.ArtifactPath) {
		return fmt.Errorf("artifact_path is not canonical absolute")
	}
	if result.FormatVersion != JSONLFormatVersion || result.ExternalCopyNotice != ExternalCopyNotice {
		return fmt.Errorf("JSONL format or external copy notice is not closed")
	}
	if err := result.CapturedHead.Validate(); err != nil {
		return fmt.Errorf("captured_head: %w", err)
	}
	return validateDecimals(result.RecordCount, result.ByteCount)
}

type RecoveryTerminalizeResult struct {
	AllExisting            bool   `json:"all_existing"`
	TerminalizedAttempts   string `json:"terminalized_attempts"`
	CancelledMandatoryWork string `json:"cancelled_mandatory_work"`
	ResultHead             Head   `json:"result_head"`
}

func (*RecoveryTerminalizeResult) resultFamily() resultFamily { return familyRecoveryTerminalize }
func (result *RecoveryTerminalizeResult) validateResult(command CommandID) error {
	if command != CommandAdminRecoveryTerminalize {
		return wrongFamily(command, familyRecoveryTerminalize)
	}
	if result == nil {
		return fmt.Errorf("nil recovery result")
	}
	if err := result.ResultHead.Validate(); err != nil {
		return fmt.Errorf("result_head: %w", err)
	}
	return validateDecimals(result.TerminalizedAttempts, result.CancelledMandatoryWork)
}

type SessionPolicySelectResult struct {
	PreviousVersionID *string `json:"previous_version_id"`
	SelectedVersionID string  `json:"selected_version_id"`
	ServiceReady      bool    `json:"service_ready"`
}

func (*SessionPolicySelectResult) resultFamily() resultFamily { return familySessionPolicySelect }
func (result *SessionPolicySelectResult) validateResult(command CommandID) error {
	if command != CommandAdminSessionPolicySelect {
		return wrongFamily(command, familySessionPolicySelect)
	}
	if result == nil || !validULID(result.SelectedVersionID) {
		return fmt.Errorf("selected_version_id is not canonical ULID")
	}
	if result.PreviousVersionID != nil && !validULID(*result.PreviousVersionID) {
		return fmt.Errorf("previous_version_id is not canonical ULID")
	}
	return nil
}

type ImpactCounts struct {
	Safe         string `json:"safe"`
	NeedsRebuild string `json:"needs_rebuild"`
	NeedsReview  string `json:"needs_review"`
	MustErase    string `json:"must_erase"`
}

type ErasurePlanResult struct {
	ArtifactPath string       `json:"artifact_path"`
	PlanState    string       `json:"plan_state"`
	BaseHead     Head         `json:"base_head"`
	Digest       string       `json:"digest"`
	TargetCount  string       `json:"target_count"`
	ImpactCounts ImpactCounts `json:"impact_counts"`
	BlockerCount string       `json:"blocker_count"`
}

func (*ErasurePlanResult) resultFamily() resultFamily { return familyErasurePlan }
func (result *ErasurePlanResult) validateResult(command CommandID) error {
	if command != CommandAdminErasurePlanContent && command != CommandAdminErasurePlanResident && command != CommandAdminErasureDecide {
		return wrongFamily(command, familyErasurePlan)
	}
	if result == nil || !validAbsolutePath(result.ArtifactPath) || !validDigest(result.Digest) {
		return fmt.Errorf("erasure artifact_path or digest is invalid")
	}
	if result.PlanState != "review_required" && result.PlanState != "ready" {
		return fmt.Errorf("plan_state is not closed")
	}
	if err := result.BaseHead.Validate(); err != nil {
		return fmt.Errorf("base_head: %w", err)
	}
	return validateDecimals(result.TargetCount, result.ImpactCounts.Safe, result.ImpactCounts.NeedsRebuild,
		result.ImpactCounts.NeedsReview, result.ImpactCounts.MustErase, result.BlockerCount)
}

type ErasureApplyResult struct {
	ExistingCommit             bool   `json:"existing_commit"`
	CanonicalErasureCommitID   string `json:"canonical_erasure_commit_id"`
	CanonicalErasureCommitSeq  string `json:"canonical_erasure_commit_seq"`
	ProjectionTargetHead       Head   `json:"projection_target_head"`
	TargetCount                string `json:"target_count"`
	MandatoryWorkFollowupCount string `json:"mandatory_work_followup_count"`
	ProjectionState            string `json:"projection_state"`
}

func (*ErasureApplyResult) resultFamily() resultFamily { return familyErasureApply }
func (result *ErasureApplyResult) validateResult(command CommandID) error {
	if command != CommandAdminErasureApply {
		return wrongFamily(command, familyErasureApply)
	}
	if result == nil || !validULID(result.CanonicalErasureCommitID) {
		return fmt.Errorf("canonical_erasure_commit_id is not canonical ULID")
	}
	if _, ok := parsePositiveDecimal(result.CanonicalErasureCommitSeq); !ok {
		return fmt.Errorf("canonical_erasure_commit_seq is not positive decimal")
	}
	if err := result.ProjectionTargetHead.Validate(); err != nil {
		return fmt.Errorf("projection_target_head: %w", err)
	}
	if result.ProjectionState != ProjectionStateCurrent && result.ProjectionState != ProjectionStateNotCurrent {
		return fmt.Errorf("projection_state is not closed")
	}
	return validateDecimals(result.TargetCount, result.MandatoryWorkFollowupCount)
}

type BlobGCResult struct {
	ResidentID     string `json:"resident_id"`
	CapturedHead   Head   `json:"captured_head"`
	PlanDigest     string `json:"plan_digest"`
	CandidateCount string `json:"candidate_count"`
	CandidateBytes string `json:"candidate_bytes"`
	DeletedCount   string `json:"deleted_count"`
	RemainingCount string `json:"remaining_count"`
}

func (*BlobGCResult) resultFamily() resultFamily { return familyBlobGC }
func (result *BlobGCResult) validateResult(command CommandID) error {
	if command != CommandBlobGC {
		return wrongFamily(command, familyBlobGC)
	}
	if result == nil || !validULID(result.ResidentID) || !validDigest(result.PlanDigest) {
		return fmt.Errorf("blob GC resident_id or plan_digest is invalid")
	}
	if err := result.CapturedHead.Validate(); err != nil {
		return fmt.Errorf("captured_head: %w", err)
	}
	if err := validateDecimals(result.CandidateCount, result.CandidateBytes, result.DeletedCount, result.RemainingCount); err != nil {
		return err
	}
	candidates, _ := parseNonNegativeDecimal(result.CandidateCount)
	deleted, _ := parseNonNegativeDecimal(result.DeletedCount)
	remaining, _ := parseNonNegativeDecimal(result.RemainingCount)
	if new(big.Int).Add(deleted, remaining).Cmp(candidates) != 0 {
		return fmt.Errorf("blob GC deleted_count + remaining_count must equal candidate_count")
	}
	return nil
}

type HealthcheckResult struct {
	Ready               bool   `json:"ready"`
	ReasonCode          string `json:"reason_code"`
	CheckedAtUnixMicros string `json:"checked_at_unix_micros"`
}

func (*HealthcheckResult) resultFamily() resultFamily { return familyHealthcheck }
func (result *HealthcheckResult) validateResult(command CommandID) error {
	if command != CommandHealthcheck {
		return wrongFamily(command, familyHealthcheck)
	}
	if result == nil || !validDecimal(result.CheckedAtUnixMicros) {
		return fmt.Errorf("checked_at_unix_micros is not decimal")
	}
	if result.Ready {
		if result.ReasonCode != HealthReasonReady {
			return fmt.Errorf("ready healthcheck must use reason_code=ready")
		}
	} else if !validNotReadyReason(result.ReasonCode) {
		return fmt.Errorf("not-ready healthcheck reason is not closed")
	}
	return nil
}

type ErrorResult struct {
	ErrorStage          Stage   `json:"error_stage"`
	ArtifactPath        *string `json:"artifact_path"`
	CapturedHead        *Head   `json:"captured_head"`
	Published           *bool   `json:"published"`
	StagingMarkerID     *string `json:"staging_marker_id"`
	Ready               *bool   `json:"ready"`
	ReasonCode          *string `json:"reason_code"`
	CheckedAtUnixMicros *string `json:"checked_at_unix_micros"`
}

func (*ErrorResult) resultFamily() resultFamily { return familyError }
func (result *ErrorResult) validateResult(CommandID) error {
	if result == nil || !validStage(result.ErrorStage) {
		return fmt.Errorf("error_stage is not closed")
	}
	if result.ArtifactPath != nil && !validAbsolutePath(*result.ArtifactPath) {
		return fmt.Errorf("artifact_path is not canonical absolute")
	}
	if result.CapturedHead != nil {
		if err := result.CapturedHead.Validate(); err != nil {
			return fmt.Errorf("captured_head: %w", err)
		}
	}
	if result.StagingMarkerID != nil && !validULID(*result.StagingMarkerID) {
		return fmt.Errorf("staging_marker_id is not canonical ULID")
	}
	if result.ReasonCode != nil && !validNotReadyReason(*result.ReasonCode) {
		return fmt.Errorf("reason_code is not a closed not-ready reason")
	}
	if result.CheckedAtUnixMicros != nil && !validDecimal(*result.CheckedAtUnixMicros) {
		return fmt.Errorf("checked_at_unix_micros is not decimal")
	}
	return nil
}

func validateErrorResultSemantics(command CommandID, code ErrorCode, result *ErrorResult) error {
	if code == ErrorCLIUsage {
		if result.ErrorStage != StageUsage || result.ArtifactPath != nil || result.CapturedHead != nil ||
			result.Published != nil || result.StagingMarkerID != nil || result.Ready != nil ||
			result.ReasonCode != nil || result.CheckedAtUnixMicros != nil {
			return fmt.Errorf("cli_usage requires stage=usage and all optional error result fields null")
		}
		return nil
	}
	if result.ErrorStage == StageUsage {
		return fmt.Errorf("stage=usage requires cli_usage")
	}
	artifactCommands := []CommandID{
		CommandBackupCreate, CommandBackupRestore, CommandExportJSONL,
		CommandAdminErasurePlanContent, CommandAdminErasurePlanResident, CommandAdminErasureDecide,
	}
	if result.ArtifactPath != nil && !slices.Contains(artifactCommands, command) {
		return fmt.Errorf("artifact_path is only valid for artifact commands")
	}
	if result.Published != nil {
		if *result.Published {
			return fmt.Errorf("normal error cannot claim a published artifact; use partial")
		}
		if command != CommandBackupRestore && code != ErrorPublishDurabilityUnknown {
			return fmt.Errorf("published is only valid for restore or durability-unknown errors")
		}
	}
	if result.StagingMarkerID != nil && command != CommandBackupRestore {
		return fmt.Errorf("staging_marker_id is only valid for backup.restore")
	}
	if result.Ready != nil || result.ReasonCode != nil || result.CheckedAtUnixMicros != nil {
		if command != CommandHealthcheck {
			return fmt.Errorf("readiness fields are only valid for healthcheck")
		}
	}
	if code == ErrorPublishDurabilityUnknown {
		if result.ErrorStage != StagePublish || result.ArtifactPath == nil || result.Published == nil || *result.Published {
			return fmt.Errorf("publish_durability_unknown requires artifact_path and published=false")
		}
	}
	if code == ErrorHealthcheckNotReady {
		if result.ErrorStage != StageReadiness || result.Ready == nil || *result.Ready ||
			result.ReasonCode == nil || result.CheckedAtUnixMicros == nil {
			return fmt.Errorf("healthcheck_not_ready requires strict readiness snapshot")
		}
		if result.ArtifactPath != nil || result.CapturedHead != nil || result.Published != nil || result.StagingMarkerID != nil {
			return fmt.Errorf("healthcheck_not_ready cannot carry offline/artifact state")
		}
	} else if command == CommandHealthcheck {
		if result.Ready != nil || result.ReasonCode != nil || result.CheckedAtUnixMicros != nil {
			return fmt.Errorf("unreachable/invalid healthcheck must not invent readiness fields")
		}
	}
	return nil
}

func validateDecimals(values ...string) error {
	for _, value := range values {
		if !validDecimal(value) {
			return fmt.Errorf("%q is not a canonical decimal string", value)
		}
	}
	return nil
}

func validNotReadyReason(value string) bool {
	return slices.Contains([]string{
		HealthReasonStartupIncomplete, HealthReasonResidentUnselected, HealthReasonResidentNotActive,
		HealthReasonMemoryPolicy, HealthReasonSessionUnresolved, HealthReasonIntegrityBlocked, HealthReasonProjectionCurrent,
		HealthReasonShutdown,
	}, value)
}

func wrongFamily(command CommandID, family resultFamily) error {
	return fmt.Errorf("family %s is not valid for command %q", family, command)
}
