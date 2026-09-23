package cliresult

import (
	"encoding/json"
	"strings"
	"testing"
)

func TestM7CLIDiagnosticsRequiresExactThirteenSectionSchemas(t *testing.T) {
	result := unknownDiagnosticsResult()
	envelope := NewSuccess(CommandAdminDiagnostics, result)
	if err := envelope.Validate(); err != nil {
		t.Fatalf("valid absent-database diagnostics rejected: %v", err)
	}
	encoded, err := json.Marshal(result)
	if err != nil {
		t.Fatal(err)
	}
	wire := string(encoded)
	if strings.Count(wire, `"section_code"`) != 13 ||
		strings.Contains(wire, "raw_content") || strings.Contains(wire, "blob_sha256") ||
		strings.Contains(wire, "filesystem_object_path") {
		t.Fatalf("diagnostics boundary keys leaked or sections missing: %s", wire)
	}

	badOrder := unknownDiagnosticsResult()
	badOrder.Sections[0], badOrder.Sections[1] = badOrder.Sections[1], badOrder.Sections[0]
	if err := NewSuccess(CommandAdminDiagnostics, badOrder).Validate(); err == nil {
		t.Fatal("out-of-order diagnostics sections unexpectedly accepted")
	}

	badSchema := unknownDiagnosticsResult()
	badSchema.Sections[0].Details = &QuickCheckDetails{Result: "unavailable", ErrorCode: stringPointer("table_unavailable")}
	if err := NewSuccess(CommandAdminDiagnostics, badSchema).Validate(); err == nil {
		t.Fatal("wrong detail schema unexpectedly accepted")
	}
}

func TestM7CLIDiagnosticsDerivesOverallSeverityAndReadinessPriority(t *testing.T) {
	result := unknownDiagnosticsResult()
	result.Sections[7] = DiagnosticSection{
		SectionCode: "service_readiness",
		Status:      DiagnosticStatusWarn,
		Details: &ServiceReadinessDetails{
			EvaluatedHead: &Head{},
			Ready:         boolPointer(false),
			ReasonCodes: []string{
				HealthReasonStartupIncomplete,
				HealthReasonIntegrityBlocked,
			},
			ErrorCode: nil,
		},
	}
	result.OverallState = DiagnosticStatusWarn
	if err := NewSuccess(CommandAdminDiagnostics, result).Validate(); err != nil {
		t.Fatalf("valid warning diagnostics rejected: %v", err)
	}

	invalid := unknownDiagnosticsResult()
	invalid.Sections[7] = result.Sections[7]
	invalid.OverallState = DiagnosticStatusWarn
	details := invalid.Sections[7].Details.(*ServiceReadinessDetails)
	details.ReasonCodes = []string{HealthReasonIntegrityBlocked, HealthReasonStartupIncomplete}
	if err := NewSuccess(CommandAdminDiagnostics, invalid).Validate(); err == nil {
		t.Fatal("out-of-priority readiness reasons unexpectedly accepted")
	}
}

func TestM7CLIDiagnosticsPublishMarkerDoesNotExposePathOrDigest(t *testing.T) {
	result := unknownDiagnosticsResult()
	result.Sections[12] = DiagnosticSection{
		SectionCode: "publish_state",
		Status:      DiagnosticStatusError,
		Details: &PublishStateDetails{
			RestoreStaging: []RestoreStagingDiagnostic{},
			PublishPending: []PublishPendingDiagnostic{{
				PublishID: nil, Variant: nil, ProducerCommand: nil,
				TargetBasename: "restored-data", State: "unreadable",
			}},
			ErrorCode: stringPointer("marker_unreadable"),
		},
	}
	result.OverallState = DiagnosticStatusError
	if err := NewSuccess(CommandAdminDiagnostics, result).Validate(); err != nil {
		t.Fatalf("sanitized unreadable marker rejected: %v", err)
	}
	encoded, _ := json.Marshal(result.Sections[12])
	if strings.Contains(string(encoded), "path") || strings.Contains(string(encoded), "digest") ||
		strings.Contains(string(encoded), "staging_basename") {
		t.Fatalf("publish diagnostics leaked authority/material: %s", encoded)
	}
}

func TestM7CLIDiagnosticsMandatoryWorkOKRequiresCompleteBoundedScans(t *testing.T) {
	result := unknownDiagnosticsResult()
	details := result.Sections[10].Details.(*MandatoryWorkDetails)
	result.Sections[10].Status = DiagnosticStatusOK
	details.ErrorCode = nil
	details.DialogueScanComplete = true
	details.MemoryScanComplete = true
	details.DialogueScannedCandidates = stringPointer("0")
	details.MemoryScannedCandidates = stringPointer("0")
	result.OverallState = DiagnosticStatusUnknown
	if err := NewSuccess(CommandAdminDiagnostics, result).Validate(); err != nil {
		t.Fatalf("complete bounded mandatory-work scans rejected: %v", err)
	}

	details.MemoryScanComplete = false
	if err := NewSuccess(CommandAdminDiagnostics, result).Validate(); err == nil {
		t.Fatal("ok mandatory_work unexpectedly accepted an incomplete memory scan")
	}
	result.Sections[10].Status = DiagnosticStatusWarn
	result.OverallState = DiagnosticStatusWarn
	if err := NewSuccess(CommandAdminDiagnostics, result).Validate(); err != nil {
		t.Fatalf("warn mandatory_work rejected bounded incomplete scan: %v", err)
	}
	details.MemoryScannedCandidates = nil
	if err := NewSuccess(CommandAdminDiagnostics, result).Validate(); err == nil {
		t.Fatal("warn mandatory_work unexpectedly accepted a missing memory candidate count")
	}
	details.MemoryScannedCandidates = stringPointer("0")
	details.MemoryScanComplete = true
	details.MemoryScannedCandidates = stringPointer("-1")
	if err := NewSuccess(CommandAdminDiagnostics, result).Validate(); err == nil {
		t.Fatal("negative memory candidate count unexpectedly accepted")
	}
}

func unknownDiagnosticsResult() *DiagnosticsResult {
	errorCode := func() *string { return stringPointer("table_unavailable") }
	unknown := DiagnosticStatusUnknown
	return &DiagnosticsResult{
		CapturedHead: nil,
		OverallState: unknown,
		Sections: []DiagnosticSection{
			{SectionCode: "schema", Status: unknown, Details: &SchemaDetails{
				ExpectedVersion: "12", ErrorCode: errorCode(),
			}},
			{SectionCode: "sqlite_quick_check", Status: unknown, Details: &QuickCheckDetails{
				Result: "unavailable", ErrorCode: errorCode(),
			}},
			{SectionCode: "foreign_keys", Status: unknown, Details: &ForeignKeysDetails{ErrorCode: errorCode()}},
			{SectionCode: "canonical_head", Status: unknown, Details: &CanonicalHeadDetails{ErrorCode: errorCode()}},
			{SectionCode: "resident_ledger", Status: unknown, Details: &ResidentLedgerDetails{ErrorCode: errorCode()}},
			{SectionCode: "content_blob_integrity", Status: unknown, Details: &ContentBlobIntegrityDetails{ErrorCode: errorCode()}},
			{SectionCode: "runtime_selection", Status: unknown, Details: &RuntimeSelectionDetails{ErrorCode: errorCode()}},
			{SectionCode: "service_readiness", Status: unknown, Details: &ServiceReadinessDetails{
				ReasonCodes: []string{}, ErrorCode: errorCode(),
			}},
			{SectionCode: "projections", Status: unknown, Details: &ProjectionsDetails{
				Entries: []ProjectionDiagnosticEntry{}, ErrorCode: errorCode(),
			}},
			{SectionCode: "running_attempts", Status: unknown, Details: &RunningAttemptsDetails{
				RunIDs: []string{}, ResidentIDs: []string{}, ErrorCode: errorCode(),
			}},
			{SectionCode: "mandatory_work", Status: unknown, Details: &MandatoryWorkDetails{
				ResidentIDs: []string{}, DialogueUnpreparedEventIDs: []string{}, DialogueScanComplete: false,
				MemoryScanComplete: false, ErrorCode: errorCode(),
			}},
			{SectionCode: "blob_gc", Status: unknown, Details: &BlobGCDetails{ErrorCode: errorCode()}},
			{SectionCode: "publish_state", Status: unknown, Details: &PublishStateDetails{
				RestoreStaging: []RestoreStagingDiagnostic{}, PublishPending: []PublishPendingDiagnostic{},
				ErrorCode: errorCode(),
			}},
		},
	}
}

func boolPointer(value bool) *bool { return &value }
