package cliresult

import (
	"fmt"
	"reflect"
	"slices"
)

const (
	DiagnosticStatusOK      = "ok"
	DiagnosticStatusWarn    = "warn"
	DiagnosticStatusError   = "error"
	DiagnosticStatusUnknown = "unknown"
)

var diagnosticSectionOrder = []string{
	"schema",
	"sqlite_quick_check",
	"foreign_keys",
	"canonical_head",
	"resident_ledger",
	"content_blob_integrity",
	"runtime_selection",
	"service_readiness",
	"projections",
	"running_attempts",
	"mandatory_work",
	"blob_gc",
	"publish_state",
}

type DiagnosticDetails interface {
	diagnosticSectionCode() string
	validateDiagnosticDetails(string) error
}

type DiagnosticSection struct {
	SectionCode string            `json:"section_code"`
	Status      string            `json:"status"`
	Details     DiagnosticDetails `json:"details"`
}

type DiagnosticsResult struct {
	CapturedHead *Head               `json:"captured_head"`
	OverallState string              `json:"overall_state"`
	Sections     []DiagnosticSection `json:"sections"`
}

func (*DiagnosticsResult) resultFamily() resultFamily { return familyDiagnostics }
func (result *DiagnosticsResult) validateResult(command CommandID) error {
	if command != CommandAdminDiagnostics {
		return wrongFamily(command, familyDiagnostics)
	}
	if result == nil || len(result.Sections) != len(diagnosticSectionOrder) {
		return fmt.Errorf("diagnostics requires exactly 13 sections")
	}
	wantOverall := DiagnosticStatusOK
	for index, section := range result.Sections {
		if section.SectionCode != diagnosticSectionOrder[index] {
			return fmt.Errorf("section %d = %q, want %q", index, section.SectionCode, diagnosticSectionOrder[index])
		}
		if !validDiagnosticStatus(section.Status) {
			return fmt.Errorf("section %q status is not closed", section.SectionCode)
		}
		if section.Details == nil || section.Details.diagnosticSectionCode() != section.SectionCode {
			return fmt.Errorf("section %q detail schema mismatch", section.SectionCode)
		}
		if err := section.Details.validateDiagnosticDetails(section.Status); err != nil {
			return fmt.Errorf("section %q: %w", section.SectionCode, err)
		}
		if diagnosticSeverity(section.Status) > diagnosticSeverity(wantOverall) {
			wantOverall = section.Status
		}
	}
	if result.OverallState != wantOverall {
		return fmt.Errorf("overall_state = %q, want derived %q", result.OverallState, wantOverall)
	}
	canonicalDetails, ok := result.Sections[3].Details.(*CanonicalHeadDetails)
	if !ok {
		return fmt.Errorf("canonical_head detail type is not exact")
	}
	if !reflect.DeepEqual(result.CapturedHead, canonicalDetails.Head) {
		return fmt.Errorf("captured_head must deep-equal canonical_head.details.head")
	}
	if result.CapturedHead != nil {
		if err := result.CapturedHead.Validate(); err != nil {
			return fmt.Errorf("captured_head: %w", err)
		}
	}
	if result.CapturedHead == nil && result.OverallState == DiagnosticStatusOK {
		return fmt.Errorf("captured_head=null cannot report overall_state=ok")
	}
	return nil
}

type SchemaDetails struct {
	ActualVersion     *string `json:"actual_version"`
	ExpectedVersion   string  `json:"expected_version"`
	ActualFingerprint *string `json:"actual_fingerprint"`
	Exact             *bool   `json:"exact"`
	ErrorCode         *string `json:"error_code"`
}

func (*SchemaDetails) diagnosticSectionCode() string { return "schema" }
func (details *SchemaDetails) validateDiagnosticDetails(status string) error {
	if details == nil || !validDecimal(details.ExpectedVersion) {
		return fmt.Errorf("expected_version is not decimal")
	}
	if details.ActualVersion != nil && !validDecimal(*details.ActualVersion) {
		return fmt.Errorf("actual_version is not decimal")
	}
	if details.ActualFingerprint != nil && !validDigest(*details.ActualFingerprint) {
		return fmt.Errorf("actual_fingerprint is not sha256")
	}
	return validateDiagnosticError(status, details.ErrorCode)
}

type QuickCheckDetails struct {
	Result    string  `json:"result"`
	ErrorCode *string `json:"error_code"`
}

func (*QuickCheckDetails) diagnosticSectionCode() string { return "sqlite_quick_check" }
func (details *QuickCheckDetails) validateDiagnosticDetails(status string) error {
	if details == nil || !slices.Contains([]string{"ok", "corrupt", "unavailable"}, details.Result) {
		return fmt.Errorf("result is not closed")
	}
	return validateDiagnosticError(status, details.ErrorCode)
}

type ForeignKeysDetails struct {
	ViolationCount *string `json:"violation_count"`
	ErrorCode      *string `json:"error_code"`
}

func (*ForeignKeysDetails) diagnosticSectionCode() string { return "foreign_keys" }
func (details *ForeignKeysDetails) validateDiagnosticDetails(status string) error {
	if details == nil {
		return fmt.Errorf("nil details")
	}
	if err := validateOptionalDecimal(details.ViolationCount); err != nil {
		return fmt.Errorf("violation_count: %w", err)
	}
	return validateDiagnosticError(status, details.ErrorCode)
}

type CanonicalHeadDetails struct {
	Head        *Head   `json:"head"`
	LedgerValid *bool   `json:"ledger_valid"`
	ErrorCode   *string `json:"error_code"`
}

func (*CanonicalHeadDetails) diagnosticSectionCode() string { return "canonical_head" }
func (details *CanonicalHeadDetails) validateDiagnosticDetails(status string) error {
	if details == nil {
		return fmt.Errorf("nil details")
	}
	if details.Head != nil {
		if err := details.Head.Validate(); err != nil {
			return fmt.Errorf("head: %w", err)
		}
	}
	return validateDiagnosticError(status, details.ErrorCode)
}

type ResidentLedgerDetails struct {
	ResidentCount        *string `json:"resident_count"`
	InvalidResidentCount *string `json:"invalid_resident_count"`
	ErrorCode            *string `json:"error_code"`
}

func (*ResidentLedgerDetails) diagnosticSectionCode() string { return "resident_ledger" }
func (details *ResidentLedgerDetails) validateDiagnosticDetails(status string) error {
	if details == nil {
		return fmt.Errorf("nil details")
	}
	if err := validateOptionalDecimals(details.ResidentCount, details.InvalidResidentCount); err != nil {
		return err
	}
	return validateDiagnosticError(status, details.ErrorCode)
}

type ContentBlobIntegrityDetails struct {
	PresentCount                 *string `json:"present_count"`
	ErasedCount                  *string `json:"erased_count"`
	MissingSQLiteBlobCount       *string `json:"missing_sqlite_blob_count"`
	MissingFilesystemObjectCount *string `json:"missing_filesystem_object_count"`
	ByteMismatchCount            *string `json:"byte_mismatch_count"`
	FilesystemOrphanCount        *string `json:"filesystem_orphan_count"`
	ErrorCode                    *string `json:"error_code"`
}

func (*ContentBlobIntegrityDetails) diagnosticSectionCode() string { return "content_blob_integrity" }
func (details *ContentBlobIntegrityDetails) validateDiagnosticDetails(status string) error {
	if details == nil {
		return fmt.Errorf("nil details")
	}
	if err := validateOptionalDecimals(details.PresentCount, details.ErasedCount, details.MissingSQLiteBlobCount,
		details.MissingFilesystemObjectCount, details.ByteMismatchCount, details.FilesystemOrphanCount); err != nil {
		return err
	}
	return validateDiagnosticError(status, details.ErrorCode)
}

type RuntimeSelectionDetails struct {
	ActiveResidentID         *string `json:"active_resident_id"`
	DesiredSessionPolicyID   *string `json:"desired_session_policy_id"`
	ActiveResidentResolvable *bool   `json:"active_resident_resolvable"`
	SessionPolicyResolvable  *bool   `json:"session_policy_resolvable"`
	ErrorCode                *string `json:"error_code"`
}

func (*RuntimeSelectionDetails) diagnosticSectionCode() string { return "runtime_selection" }
func (details *RuntimeSelectionDetails) validateDiagnosticDetails(status string) error {
	if details == nil {
		return fmt.Errorf("nil details")
	}
	if details.ActiveResidentID != nil && !validULID(*details.ActiveResidentID) {
		return fmt.Errorf("active_resident_id is not canonical ULID")
	}
	if details.DesiredSessionPolicyID != nil && !validULID(*details.DesiredSessionPolicyID) {
		return fmt.Errorf("desired_session_policy_id is not canonical ULID")
	}
	return validateDiagnosticError(status, details.ErrorCode)
}

type ServiceReadinessDetails struct {
	EvaluatedHead *Head    `json:"evaluated_head"`
	Ready         *bool    `json:"ready"`
	ReasonCodes   []string `json:"reason_codes"`
	ErrorCode     *string  `json:"error_code"`
}

func (*ServiceReadinessDetails) diagnosticSectionCode() string { return "service_readiness" }
func (details *ServiceReadinessDetails) validateDiagnosticDetails(status string) error {
	if details == nil || details.ReasonCodes == nil {
		return fmt.Errorf("reason_codes must be an array")
	}
	if details.EvaluatedHead != nil {
		if err := details.EvaluatedHead.Validate(); err != nil {
			return fmt.Errorf("evaluated_head: %w", err)
		}
	}
	if status == DiagnosticStatusUnknown || status == DiagnosticStatusError {
		if details.EvaluatedHead != nil || details.Ready != nil || len(details.ReasonCodes) != 0 {
			return fmt.Errorf("unknown/error readiness must have null snapshot and empty reasons")
		}
	} else if details.EvaluatedHead == nil || details.Ready == nil {
		return fmt.Errorf("known readiness requires evaluated_head and ready")
	} else if *details.Ready {
		if !slices.Equal(details.ReasonCodes, []string{HealthReasonReady}) {
			return fmt.Errorf("ready snapshot requires [ready]")
		}
	} else if err := validateReadinessReasons(details.ReasonCodes); err != nil {
		return err
	}
	return validateDiagnosticError(status, details.ErrorCode)
}

type ProjectionDiagnosticEntry struct {
	ResidentID    string  `json:"resident_id"`
	Name          string  `json:"name"`
	Version       string  `json:"version"`
	Current       bool    `json:"current"`
	Rebuilding    bool    `json:"rebuilding"`
	WatermarkHead *Head   `json:"watermark_head"`
	LagCommits    *string `json:"lag_commits"`
}

type ProjectionsDetails struct {
	Entries   []ProjectionDiagnosticEntry `json:"entries"`
	ErrorCode *string                     `json:"error_code"`
}

func (*ProjectionsDetails) diagnosticSectionCode() string { return "projections" }
func (details *ProjectionsDetails) validateDiagnosticDetails(status string) error {
	if details == nil || details.Entries == nil {
		return fmt.Errorf("entries must be an array")
	}
	previous := ""
	for index, entry := range details.Entries {
		if !validULID(entry.ResidentID) || !validSafeToken(entry.Name) || !validSafeToken(entry.Version) {
			return fmt.Errorf("entries[%d] identity is invalid", index)
		}
		if entry.WatermarkHead != nil {
			if err := entry.WatermarkHead.Validate(); err != nil {
				return fmt.Errorf("entries[%d].watermark_head: %w", index, err)
			}
		}
		if err := validateOptionalDecimal(entry.LagCommits); err != nil {
			return fmt.Errorf("entries[%d].lag_commits: %w", index, err)
		}
		key := entry.ResidentID + "\x00" + entry.Name + "\x00" + entry.Version
		if index > 0 && key <= previous {
			return fmt.Errorf("entries must be sorted and duplicate-free")
		}
		previous = key
	}
	return validateDiagnosticError(status, details.ErrorCode)
}

type RunningAttemptsDetails struct {
	Count       *string  `json:"count"`
	RunIDs      []string `json:"run_ids"`
	ResidentIDs []string `json:"resident_ids"`
	ErrorCode   *string  `json:"error_code"`
}

func (*RunningAttemptsDetails) diagnosticSectionCode() string { return "running_attempts" }
func (details *RunningAttemptsDetails) validateDiagnosticDetails(status string) error {
	if details == nil || details.RunIDs == nil || details.ResidentIDs == nil {
		return fmt.Errorf("ID fields must be arrays")
	}
	if err := validateOptionalDecimal(details.Count); err != nil {
		return err
	}
	if !sortedUniqueULIDs(details.RunIDs) || !sortedUniqueULIDs(details.ResidentIDs) {
		return fmt.Errorf("run_ids/resident_ids must be sorted canonical ULIDs")
	}
	return validateDiagnosticError(status, details.ErrorCode)
}

type MandatoryWorkDetails struct {
	DialoguePending             *string  `json:"dialogue_pending"`
	DialogueRunning             *string  `json:"dialogue_running"`
	DialogueRetryPending        *string  `json:"dialogue_retry_pending"`
	DialogueUnpreparedEventIDs  []string `json:"dialogue_unprepared_event_ids"`
	DialogueUnpreparedTruncated bool     `json:"dialogue_unprepared_truncated"`
	DialogueScanComplete        bool     `json:"dialogue_scan_complete"`
	DialogueScannedCandidates   *string  `json:"dialogue_scanned_candidates"`
	MemoryPending               *string  `json:"memory_pending"`
	MemoryRunning               *string  `json:"memory_running"`
	MemoryRetryPending          *string  `json:"memory_retry_pending"`
	MemoryScanComplete          bool     `json:"memory_scan_complete"`
	MemoryScannedCandidates     *string  `json:"memory_scanned_candidates"`
	ResidentIDs                 []string `json:"resident_ids"`
	ErrorCode                   *string  `json:"error_code"`
}

func (*MandatoryWorkDetails) diagnosticSectionCode() string { return "mandatory_work" }
func (details *MandatoryWorkDetails) validateDiagnosticDetails(status string) error {
	if details == nil || details.ResidentIDs == nil || details.DialogueUnpreparedEventIDs == nil {
		return fmt.Errorf("resident_ids must be an array")
	}
	if err := validateOptionalDecimals(details.DialoguePending, details.DialogueRunning, details.DialogueRetryPending,
		details.MemoryPending, details.MemoryRunning, details.MemoryRetryPending); err != nil {
		return err
	}
	if err := validateOptionalDecimals(details.DialogueScannedCandidates, details.MemoryScannedCandidates); err != nil {
		return err
	}
	if !sortedUniqueULIDs(details.ResidentIDs) || !sortedUniqueULIDs(details.DialogueUnpreparedEventIDs) {
		return fmt.Errorf("resident_ids must be sorted canonical ULIDs")
	}
	if (status == DiagnosticStatusOK || status == DiagnosticStatusWarn) &&
		(details.DialogueScannedCandidates == nil || details.MemoryScannedCandidates == nil) {
		return fmt.Errorf("ok/warn mandatory_work requires dialogue and memory candidate counts")
	}
	if status == DiagnosticStatusOK && (!details.DialogueScanComplete || !details.MemoryScanComplete) {
		return fmt.Errorf("ok mandatory_work requires complete dialogue and memory scans")
	}
	return validateDiagnosticError(status, details.ErrorCode)
}

type BlobGCDetails struct {
	EligibleCount         *string `json:"eligible_count"`
	EligibleBytes         *string `json:"eligible_bytes"`
	BlockedCount          *string `json:"blocked_count"`
	FilesystemOrphanCount *string `json:"filesystem_orphan_count"`
	SQLiteOrphanCount     *string `json:"sqlite_orphan_count"`
	ErrorCode             *string `json:"error_code"`
}

func (*BlobGCDetails) diagnosticSectionCode() string { return "blob_gc" }
func (details *BlobGCDetails) validateDiagnosticDetails(status string) error {
	if details == nil {
		return fmt.Errorf("nil details")
	}
	if err := validateOptionalDecimals(details.EligibleCount, details.EligibleBytes, details.BlockedCount,
		details.FilesystemOrphanCount, details.SQLiteOrphanCount); err != nil {
		return err
	}
	return validateDiagnosticError(status, details.ErrorCode)
}

type RestoreStagingDiagnostic struct {
	MarkerID       *string `json:"marker_id"`
	TargetBasename string  `json:"target_basename"`
	State          string  `json:"state"`
}

type PublishPendingDiagnostic struct {
	PublishID       *string    `json:"publish_id"`
	Variant         *string    `json:"variant"`
	ProducerCommand *CommandID `json:"producer_command"`
	TargetBasename  string     `json:"target_basename"`
	State           string     `json:"state"`
}

type PublishStateDetails struct {
	RestoreStaging []RestoreStagingDiagnostic `json:"restore_staging"`
	PublishPending []PublishPendingDiagnostic `json:"publish_pending"`
	ErrorCode      *string                    `json:"error_code"`
}

func (*PublishStateDetails) diagnosticSectionCode() string { return "publish_state" }
func (details *PublishStateDetails) validateDiagnosticDetails(status string) error {
	if details == nil || details.RestoreStaging == nil || details.PublishPending == nil {
		return fmt.Errorf("marker fields must be arrays")
	}
	previous := ""
	hasUnreadable := false
	for index, marker := range details.RestoreStaging {
		if !validBasename(marker.TargetBasename) || !validPublishState(marker.State) {
			return fmt.Errorf("restore_staging[%d] target/state is invalid", index)
		}
		if marker.MarkerID != nil && !validULID(*marker.MarkerID) {
			return fmt.Errorf("restore_staging[%d].marker_id is not canonical ULID", index)
		}
		if marker.State == "unreadable" {
			hasUnreadable = true
		} else if marker.MarkerID == nil {
			return fmt.Errorf("readable restore staging marker requires marker_id")
		}
		key := marker.TargetBasename + "\x00" + nullFirst(marker.MarkerID)
		if index > 0 && key <= previous {
			return fmt.Errorf("restore_staging must be sorted and duplicate-free")
		}
		previous = key
	}
	previous = ""
	for index, marker := range details.PublishPending {
		if !validBasename(marker.TargetBasename) || !validPublishState(marker.State) {
			return fmt.Errorf("publish_pending[%d] target/state is invalid", index)
		}
		if marker.PublishID != nil && !validULID(*marker.PublishID) {
			return fmt.Errorf("publish_pending[%d].publish_id is not canonical ULID", index)
		}
		if marker.State == "unreadable" {
			hasUnreadable = true
			if marker.Variant != nil || marker.ProducerCommand != nil {
				return fmt.Errorf("unreadable marker cannot invent variant/producer")
			}
		} else {
			if marker.PublishID == nil || marker.Variant == nil || marker.ProducerCommand == nil ||
				(*marker.Variant != "directory" && *marker.Variant != "single_file") ||
				!slices.Contains([]CommandID{
					CommandBackupCreate, CommandBackupRestore, CommandExportJSONL,
					CommandAdminErasurePlanContent, CommandAdminErasurePlanResident, CommandAdminErasureDecide,
				}, *marker.ProducerCommand) {
				return fmt.Errorf("readable marker requires closed ID/variant/producer")
			}
		}
		key := marker.TargetBasename + "\x00" + nullFirst(marker.PublishID)
		if index > 0 && key <= previous {
			return fmt.Errorf("publish_pending must be sorted and duplicate-free")
		}
		previous = key
	}
	if hasUnreadable && (status != DiagnosticStatusError || details.ErrorCode == nil || *details.ErrorCode != "marker_unreadable") {
		return fmt.Errorf("unreadable marker requires status=error and marker_unreadable")
	}
	return validateDiagnosticError(status, details.ErrorCode)
}

func validateDiagnosticError(status string, errorCode *string) error {
	if errorCode != nil && !slices.Contains([]string{
		"table_unavailable", "query_failed", "integrity_violation", "marker_unreadable",
	}, *errorCode) {
		return fmt.Errorf("error_code is not closed")
	}
	if (status == DiagnosticStatusOK || status == DiagnosticStatusWarn) && errorCode != nil {
		return fmt.Errorf("ok/warn section cannot carry error_code")
	}
	if (status == DiagnosticStatusUnknown || status == DiagnosticStatusError) && errorCode == nil {
		return fmt.Errorf("unknown/error section requires error_code")
	}
	return nil
}

func validateOptionalDecimal(value *string) error {
	if value != nil && !validDecimal(*value) {
		return fmt.Errorf("not a canonical decimal string")
	}
	return nil
}

func validateOptionalDecimals(values ...*string) error {
	for _, value := range values {
		if err := validateOptionalDecimal(value); err != nil {
			return err
		}
	}
	return nil
}

func validateReadinessReasons(values []string) error {
	if len(values) == 0 {
		return fmt.Errorf("not-ready snapshot requires at least one reason")
	}
	order := []string{
		HealthReasonStartupIncomplete, HealthReasonResidentUnselected, HealthReasonResidentNotActive,
		HealthReasonMemoryPolicy, HealthReasonSessionUnresolved, HealthReasonIntegrityBlocked, HealthReasonProjectionCurrent,
		HealthReasonShutdown,
	}
	previous := -1
	for _, value := range values {
		index := slices.Index(order, value)
		if index <= previous {
			return fmt.Errorf("reason_codes are unknown, duplicated, or out of priority order")
		}
		previous = index
	}
	return nil
}

func sortedUniqueULIDs(values []string) bool {
	for index, value := range values {
		if !validULID(value) || index > 0 && values[index-1] >= value {
			return false
		}
	}
	return true
}

func validDiagnosticStatus(value string) bool {
	return slices.Contains([]string{DiagnosticStatusOK, DiagnosticStatusWarn, DiagnosticStatusError, DiagnosticStatusUnknown}, value)
}

func diagnosticSeverity(value string) int {
	switch value {
	case DiagnosticStatusError:
		return 3
	case DiagnosticStatusWarn:
		return 2
	case DiagnosticStatusUnknown:
		return 1
	default:
		return 0
	}
}

func validPublishState(value string) bool {
	return slices.Contains([]string{
		"prepared", "target_visible_digest_match", "target_visible_digest_mismatch",
		"staging_missing", "identity_mismatch", "reserved_marker_in_target",
		"target_marker_only", "unreadable",
	}, value)
}

func nullFirst(value *string) string {
	if value == nil {
		return ""
	}
	return *value
}
