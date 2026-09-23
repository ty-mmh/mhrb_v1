// Package cliresult implements the closed, machine-readable result boundary
// for M7 one-shot commands. Existing M0-M6 commands intentionally keep their
// historical output until their own compatibility gate says otherwise.
package cliresult

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
)

const FormatVersion = "mahoroba-cli-result-v1"

const (
	ExitSuccess     = 0
	ExitOperational = 1
	ExitUsage       = 2
)

type Outcome string

const (
	OutcomeSuccess Outcome = "success"
	OutcomePartial Outcome = "partial"
	OutcomeError   Outcome = "error"
)

type CommandID string

const (
	CommandUnknown                  CommandID = "unknown"
	CommandAdminIntegrityScan       CommandID = "admin.integrity.scan"
	CommandBackupCreate             CommandID = "backup.create"
	CommandBackupVerify             CommandID = "backup.verify"
	CommandBackupRestore            CommandID = "backup.restore"
	CommandExportJSONL              CommandID = "export.jsonl"
	CommandAdminRecoveryTerminalize CommandID = "admin.recovery.terminalize"
	CommandAdminSessionPolicySelect CommandID = "admin.runtime.session_policy.select"
	CommandAdminErasurePlanContent  CommandID = "admin.erasure.plan.content"
	CommandAdminErasurePlanResident CommandID = "admin.erasure.plan.resident"
	CommandAdminErasureDecide       CommandID = "admin.erasure.decide"
	CommandAdminErasureApply        CommandID = "admin.erasure.apply"
	CommandBlobGC                   CommandID = "blob.gc"
	CommandAdminDiagnostics         CommandID = "admin.diagnostics"
	CommandHealthcheck              CommandID = "healthcheck"
)

type Stage string

const (
	StageUsage     Stage = "usage"
	StagePreflight Stage = "preflight"
	StageCanonical Stage = "canonical"
	StageDerived   Stage = "derived"
	StagePublish   Stage = "publish"
	StageReadiness Stage = "readiness"
)

type ErrorCode string

const (
	ErrorAdminLockBusy                           ErrorCode = "admin_lock_busy"
	ErrorResidentAdmissionClosed                 ErrorCode = "resident_admission_closed"
	ErrorConfigurationInvalid                    ErrorCode = "configuration_invalid"
	ErrorSourceUnavailable                       ErrorCode = "source_unavailable"
	ErrorArtifactTargetExists                    ErrorCode = "artifact_target_exists"
	ErrorArtifactIOFailed                        ErrorCode = "artifact_io_failed"
	ErrorIntegrityFatal                          ErrorCode = "integrity_fatal"
	ErrorIntegrityApplyConflict                  ErrorCode = "integrity_apply_conflict"
	ErrorBackupTargetExists                      ErrorCode = "backup_target_exists"
	ErrorBackupInvalid                           ErrorCode = "backup_invalid"
	ErrorRestoreTargetExists                     ErrorCode = "restore_target_exists"
	ErrorRestoreNamespaceBusy                    ErrorCode = "restore_namespace_busy"
	ErrorPublishDurabilityUnknown                ErrorCode = "publish_durability_unknown"
	ErrorLegacyClaimIdentityRequiresStaticRepair ErrorCode = "legacy_claim_identity_requires_static_repair"
	ErrorIntegrityPipelineRequired               ErrorCode = "integrity_pipeline_required"
	ErrorErasureReviewRequired                   ErrorCode = "erasure_review_required"
	ErrorErasurePlanStale                        ErrorCode = "erasure_plan_stale"
	ErrorErasureIneligible                       ErrorCode = "erasure_ineligible"
	ErrorErasureRetryConflict                    ErrorCode = "erasure_retry_conflict"
	ErrorProjectionNotCurrent                    ErrorCode = "projection_not_current"
	ErrorGCPlanStale                             ErrorCode = "gc_plan_stale"
	ErrorStaticDesignReopenRequired              ErrorCode = "static_design_reopen_required"
	ErrorCLIUsage                                ErrorCode = "cli_usage"
	ErrorOperationPartial                        ErrorCode = "operation_partial"
	ErrorRecoveryAttemptOverflow                 ErrorCode = "recovery_attempt_overflow"
	ErrorHealthcheckNotReady                     ErrorCode = "healthcheck_not_ready"
	ErrorHealthcheckUnreachable                  ErrorCode = "healthcheck_unreachable"
	ErrorHealthcheckInvalidResponse              ErrorCode = "healthcheck_invalid_response"
	ErrorSecretSourceConflict                    ErrorCode = "secret_source_conflict"
	ErrorSecretFileInvalid                       ErrorCode = "secret_file_invalid"
	ErrorSecretFileUnsupportedPlatform           ErrorCode = "secret_file_unsupported_platform"
	ErrorOperationFailed                         ErrorCode = "operation_failed"
)

type EffectCode string

const (
	EffectClaimIdentityErasureRecorded  EffectCode = "claim_identity_erasure_recorded"
	EffectClaimStatusQuarantined        EffectCode = "claim_status_quarantined"
	EffectContentErased                 EffectCode = "content_erased"
	EffectGenerationAttemptTerminalized EffectCode = "generation_attempt_terminalized"
	EffectIntegrityFindingRecorded      EffectCode = "integrity_finding_recorded"
	EffectMandatoryWorkCancelled        EffectCode = "mandatory_work_cancelled"
	EffectPipelineVersionRegistered     EffectCode = "pipeline_version_registered"
	EffectResidentStatusErased          EffectCode = "resident_status_erased"
	EffectRuntimeSelectionCleared       EffectCode = "runtime_selection_cleared"
)

type Disposition string

const (
	DispositionCreated  Disposition = "created"
	DispositionExisting Disposition = "existing"
)

type WarningCode string

const (
	WarningRuntimeUnmanagedCopy         WarningCode = "runtime_unmanaged_copy"
	WarningPhysicalErasureNotGuaranteed WarningCode = "physical_erasure_not_guaranteed"
	WarningServiceNotReady              WarningCode = "service_not_ready"
	WarningLocalAdminNotAttributed      WarningCode = "local_admin_identity_not_attributed"
	WarningProjectionNotApplicable      WarningCode = "projection_not_applicable"
	WarningGCMaintenanceIncomplete      WarningCode = "gc_maintenance_incomplete"
)

type ActionCode string

const (
	ActionSelectActiveResident   ActionCode = "select_active_resident"
	ActionSelectSessionPolicy    ActionCode = "select_sessionization_policy"
	ActionRunIntegrityScan       ActionCode = "run_integrity_scan"
	ActionRunRecoveryTerminalize ActionCode = "run_recovery_terminalize"
	ActionArchiveResident        ActionCode = "archive_resident"
	ActionRebuildProjection      ActionCode = "rebuild_projection"
	ActionRepairConfig           ActionCode = "repair_config"
	ActionStartService           ActionCode = "start_service"
	ActionReplanErasure          ActionCode = "replan_erasure"
	ActionRerunBlobGCDryRun      ActionCode = "rerun_blob_gc_dry_run"
	ActionRepairStaticDesign     ActionCode = "repair_static_design"
)

type CanonicalCommit struct {
	ResidentID  *string      `json:"resident_id"`
	CommitID    string       `json:"commit_id"`
	CommitSeq   string       `json:"commit_seq"`
	Disposition Disposition  `json:"disposition"`
	Effects     []EffectCode `json:"effects"`
}

type RequiredAction struct {
	ActionID              string     `json:"action_id"`
	Code                  ActionCode `json:"code"`
	Argv                  []string   `json:"argv"`
	TargetIDs             []string   `json:"target_ids"`
	PrerequisiteActionIDs []string   `json:"prerequisite_action_ids"`
}

type Warning struct {
	WarningCode WarningCode `json:"warning_code"`
	TargetIDs   []string    `json:"target_ids"`
}

// Result is sealed to the exact M7 command-family result schemas. Packages
// outside cliresult cannot provide a result with extra or free-form fields.
type Result interface {
	resultFamily() resultFamily
	validateResult(CommandID) error
}

type resultFamily string

// Envelope is the in-memory result supplied by an M7 handler. ErrorCode and
// ErrorStage are the root terminal classification. For partial results the
// stdout error_code is always operation_partial while the root code is emitted
// as the final stderr line.
type Envelope struct {
	FormatVersion    string
	Command          CommandID
	Outcome          Outcome
	ErrorCode        ErrorCode
	ErrorStage       Stage
	CanonicalApplied bool
	CanonicalCommits []CanonicalCommit
	TargetIDs        []string
	RequiredActions  []RequiredAction
	Warnings         []Warning
	Result           Result
}

func NewSuccess(command CommandID, result Result) Envelope {
	return Envelope{
		FormatVersion:    FormatVersion,
		Command:          command,
		Outcome:          OutcomeSuccess,
		CanonicalCommits: []CanonicalCommit{},
		TargetIDs:        []string{},
		RequiredActions:  []RequiredAction{},
		Warnings:         []Warning{},
		Result:           result,
	}
}

func NewPartial(command CommandID, rootCode ErrorCode, stage Stage, result Result) Envelope {
	return Envelope{
		FormatVersion:    FormatVersion,
		Command:          command,
		Outcome:          OutcomePartial,
		ErrorCode:        rootCode,
		ErrorStage:       stage,
		CanonicalCommits: []CanonicalCommit{},
		TargetIDs:        []string{},
		RequiredActions:  []RequiredAction{},
		Warnings:         []Warning{},
		Result:           result,
	}
}

func NewFailure(command CommandID, code ErrorCode, stage Stage, result *ErrorResult) Envelope {
	if result == nil {
		result = &ErrorResult{ErrorStage: stage}
	}
	return Envelope{
		FormatVersion:    FormatVersion,
		Command:          command,
		Outcome:          OutcomeError,
		ErrorCode:        code,
		ErrorStage:       stage,
		CanonicalCommits: []CanonicalCommit{},
		TargetIDs:        []string{},
		RequiredActions:  []RequiredAction{},
		Warnings:         []Warning{},
		Result:           result,
	}
}

type successWire struct {
	FormatVersion    string            `json:"format_version"`
	Command          CommandID         `json:"command"`
	Outcome          Outcome           `json:"outcome"`
	CanonicalApplied bool              `json:"canonical_applied"`
	CanonicalCommits []CanonicalCommit `json:"canonical_commits"`
	RequiredActions  []RequiredAction  `json:"required_actions"`
	Warnings         []Warning         `json:"warnings"`
	Result           Result            `json:"result"`
}

type errorWire struct {
	FormatVersion    string            `json:"format_version"`
	Command          CommandID         `json:"command"`
	Outcome          Outcome           `json:"outcome"`
	ErrorCode        ErrorCode         `json:"error_code"`
	CanonicalApplied bool              `json:"canonical_applied"`
	CanonicalCommits []CanonicalCommit `json:"canonical_commits"`
	TargetIDs        []string          `json:"target_ids"`
	RequiredActions  []RequiredAction  `json:"required_actions"`
	Warnings         []Warning         `json:"warnings"`
	Result           Result            `json:"result"`
}

type warningWire struct {
	FormatVersion string      `json:"format_version"`
	Command       CommandID   `json:"command"`
	Outcome       string      `json:"outcome"`
	WarningCode   WarningCode `json:"warning_code"`
	TargetIDs     []string    `json:"target_ids"`
}

// Render validates the complete boundary before writing either stream. It
// returns the process exit code and a renderer/invariant error. A non-nil error
// means no output was written and handlers must fail closed rather than expose
// an unvalidated fallback message.
func Render(stdout, stderr io.Writer, envelope Envelope) (int, error) {
	if stdout == nil || stderr == nil {
		return ExitOperational, fmt.Errorf("cli result: nil output writer")
	}
	if err := envelope.Validate(); err != nil {
		return ExitOperational, err
	}

	stdoutBytes, stderrBytes, exitCode, err := renderBytes(envelope)
	if err != nil {
		return ExitOperational, err
	}
	if len(stdoutBytes) > 0 {
		if _, err := stdout.Write(stdoutBytes); err != nil {
			return ExitOperational, fmt.Errorf("cli result: write stdout: %w", err)
		}
	}
	if len(stderrBytes) > 0 {
		if _, err := stderr.Write(stderrBytes); err != nil {
			return ExitOperational, fmt.Errorf("cli result: write stderr: %w", err)
		}
	}
	return exitCode, nil
}

func renderBytes(envelope Envelope) ([]byte, []byte, int, error) {
	var stdout bytes.Buffer
	var stderr bytes.Buffer

	writeJSONLine := func(target *bytes.Buffer, value any) error {
		encoded, err := json.Marshal(value)
		if err != nil {
			return fmt.Errorf("cli result: marshal envelope: %w", err)
		}
		target.Write(encoded)
		target.WriteByte('\n')
		return nil
	}
	writeJCSLine := func(target *bytes.Buffer, value any) error {
		encoded, err := marshalJCS(value)
		if err != nil {
			return err
		}
		target.Write(encoded)
		target.WriteByte('\n')
		return nil
	}
	writeWarnings := func() error {
		for _, warning := range envelope.Warnings {
			line, err := marshalJCS(warningWire{
				FormatVersion: FormatVersion,
				Command:       envelope.Command,
				Outcome:       "warning",
				WarningCode:   warning.WarningCode,
				TargetIDs:     warning.TargetIDs,
			})
			if err != nil {
				return err
			}
			stderr.Write(line)
			stderr.WriteByte('\n')
		}
		return nil
	}

	switch envelope.Outcome {
	case OutcomeSuccess:
		if err := writeJSONLine(&stdout, envelope.successWire()); err != nil {
			return nil, nil, ExitOperational, err
		}
		if err := writeWarnings(); err != nil {
			return nil, nil, ExitOperational, err
		}
		return stdout.Bytes(), stderr.Bytes(), ExitSuccess, nil
	case OutcomePartial:
		partial := envelope.errorWire(OutcomePartial, ErrorOperationPartial)
		if err := writeJSONLine(&stdout, partial); err != nil {
			return nil, nil, ExitOperational, err
		}
		if err := writeWarnings(); err != nil {
			return nil, nil, ExitOperational, err
		}
		if err := writeJCSLine(&stderr, envelope.errorWire(OutcomeError, envelope.ErrorCode)); err != nil {
			return nil, nil, ExitOperational, err
		}
		return stdout.Bytes(), stderr.Bytes(), ExitOperational, nil
	case OutcomeError:
		if envelope.ErrorCode == ErrorCLIUsage {
			stderr.WriteString(usageFor(envelope.Command))
			if stderr.Len() > 0 && stderr.Bytes()[stderr.Len()-1] != '\n' {
				stderr.WriteByte('\n')
			}
		}
		if err := writeWarnings(); err != nil {
			return nil, nil, ExitOperational, err
		}
		if err := writeJCSLine(&stderr, envelope.errorWire(OutcomeError, envelope.ErrorCode)); err != nil {
			return nil, nil, ExitOperational, err
		}
		if envelope.ErrorCode == ErrorCLIUsage {
			return nil, stderr.Bytes(), ExitUsage, nil
		}
		return nil, stderr.Bytes(), ExitOperational, nil
	default:
		panic("validated outcome became invalid")
	}
}

func (envelope Envelope) successWire() successWire {
	return successWire{
		FormatVersion:    envelope.FormatVersion,
		Command:          envelope.Command,
		Outcome:          OutcomeSuccess,
		CanonicalApplied: envelope.CanonicalApplied,
		CanonicalCommits: nonNil(envelope.CanonicalCommits),
		RequiredActions:  nonNil(envelope.RequiredActions),
		Warnings:         nonNil(envelope.Warnings),
		Result:           envelope.Result,
	}
}

func (envelope Envelope) errorWire(outcome Outcome, code ErrorCode) errorWire {
	return errorWire{
		FormatVersion:    envelope.FormatVersion,
		Command:          envelope.Command,
		Outcome:          outcome,
		ErrorCode:        code,
		CanonicalApplied: envelope.CanonicalApplied,
		CanonicalCommits: nonNil(envelope.CanonicalCommits),
		TargetIDs:        nonNil(envelope.TargetIDs),
		RequiredActions:  nonNil(envelope.RequiredActions),
		Warnings:         nonNil(envelope.Warnings),
		Result:           envelope.Result,
	}
}

func nonNil[T any](values []T) []T {
	if values == nil {
		return []T{}
	}
	return values
}

func usageFor(command CommandID) string {
	switch command {
	case CommandAdminIntegrityScan:
		return "usage: mahoroba admin integrity scan (--all | --resident ULID) [--config FILE] [--data-dir DIR]\n"
	case CommandBackupCreate:
		return "usage: mahoroba backup create --output DIR [--include-projections] [--config FILE] [--data-dir DIR]\n"
	case CommandBackupVerify:
		return "usage: mahoroba backup verify --input DIR\n"
	case CommandBackupRestore:
		return "usage: mahoroba backup restore --input DIR --target-data-dir DIR [--config FILE]\n"
	case CommandExportJSONL:
		return "usage: mahoroba export jsonl --output FILE [--config FILE] [--data-dir DIR]\n"
	case CommandAdminRecoveryTerminalize:
		return "usage: mahoroba admin recovery terminalize [--config FILE] [--data-dir DIR]\n"
	case CommandAdminSessionPolicySelect:
		return "usage: mahoroba admin runtime session-policy select --version ULID [--config FILE] [--data-dir DIR]\n"
	case CommandAdminErasurePlanContent:
		return "usage: mahoroba admin erasure plan content --resident ULID --content ULID [--content ULID ...] --reason CODE --output FILE [--config FILE] [--data-dir DIR]\n"
	case CommandAdminErasurePlanResident:
		return "usage: mahoroba admin erasure plan resident --resident ULID --reason CODE --output FILE [--config FILE] [--data-dir DIR]\n"
	case CommandAdminErasureDecide:
		return "usage: mahoroba admin erasure decide --plan FILE --decision IMPACT_ID=retain|erase [--decision ...] --output FILE [--config FILE] [--data-dir DIR]\n"
	case CommandAdminErasureApply:
		return "usage: mahoroba admin erasure apply --plan FILE --confirm sha256:DIGEST [--config FILE] [--data-dir DIR]\n"
	case CommandBlobGC:
		return "usage: mahoroba blob gc --resident ULID [--apply --confirm sha256:DIGEST] [--config FILE] [--data-dir DIR]\n"
	case CommandAdminDiagnostics:
		return "usage: mahoroba admin diagnostics (--all | --resident ULID) [--format text|json] [--config FILE] [--data-dir DIR]\n"
	case CommandHealthcheck:
		return "usage: mahoroba healthcheck [--config FILE] [--url http://127.0.0.1:PORT/healthz]\n"
	default:
		return "usage: mahoroba <command> [options]\n"
	}
}
