package cliresult

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math/big"
	"path"
	"path/filepath"
	"regexp"
	"slices"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/gowebpki/jcs"
	"github.com/oklog/ulid/v2"
)

var (
	digestPattern       = regexp.MustCompile(`^sha256:[0-9a-f]{64}$`)
	decimalPattern      = regexp.MustCompile(`^(0|[1-9][0-9]*)$`)
	safeTokenPattern    = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._:/-]*$`)
	windowsDrivePattern = regexp.MustCompile(`^[A-Za-z]:[\\/]`)
)

func (envelope Envelope) Validate() error {
	if envelope.FormatVersion != FormatVersion {
		return fmt.Errorf("cli result: format_version = %q, want %q", envelope.FormatVersion, FormatVersion)
	}
	if !validCommand(envelope.Command) {
		return fmt.Errorf("cli result: command %q is not in the v1 catalog", envelope.Command)
	}
	if envelope.Outcome != OutcomeSuccess && envelope.Outcome != OutcomePartial && envelope.Outcome != OutcomeError {
		return fmt.Errorf("cli result: outcome %q is not closed", envelope.Outcome)
	}
	if envelope.Result == nil {
		return fmt.Errorf("cli result: result is required")
	}
	if err := envelope.Result.validateResult(envelope.Command); err != nil {
		return fmt.Errorf("cli result: result: %w", err)
	}
	if err := validateCanonicalCommits(envelope.CanonicalCommits); err != nil {
		return err
	}
	if envelope.CanonicalApplied != (len(envelope.CanonicalCommits) > 0) {
		return fmt.Errorf("cli result: canonical_applied must equal canonical_commits non-empty")
	}
	if err := validateTargetIDs(envelope.TargetIDs, true); err != nil {
		return fmt.Errorf("cli result: target_ids: %w", err)
	}
	if err := validateWarnings(envelope.Warnings); err != nil {
		return err
	}
	if err := validateActions(envelope.RequiredActions); err != nil {
		return err
	}
	if err := validateCommandActions(envelope.Command, envelope.RequiredActions); err != nil {
		return err
	}

	switch envelope.Outcome {
	case OutcomeSuccess:
		if envelope.Command == CommandUnknown {
			return fmt.Errorf("cli result: unknown command cannot succeed")
		}
		if envelope.ErrorCode != "" || envelope.ErrorStage != "" || len(envelope.TargetIDs) != 0 {
			return fmt.Errorf("cli result: success cannot carry error classification or target_ids")
		}
		if envelope.Result.resultFamily() == familyError {
			return fmt.Errorf("cli result: success cannot use common error result")
		}
	case OutcomePartial:
		if !partialCommand(envelope.Command) {
			return fmt.Errorf("cli result: command %q cannot return partial", envelope.Command)
		}
		if envelope.ErrorCode == "" || envelope.ErrorCode == ErrorCLIUsage || envelope.ErrorCode == ErrorOperationPartial {
			return fmt.Errorf("cli result: partial requires a non-usage root error code")
		}
		if envelope.Result.resultFamily() == familyError {
			return fmt.Errorf("cli result: partial must retain its family result")
		}
		if err := validateErrorPair(envelope.Command, envelope.ErrorCode, envelope.ErrorStage); err != nil {
			return err
		}
	case OutcomeError:
		if envelope.CanonicalApplied || len(envelope.CanonicalCommits) != 0 {
			return fmt.Errorf("cli result: durable Canonical effects require outcome=partial")
		}
		if envelope.ErrorCode == "" || envelope.ErrorCode == ErrorOperationPartial {
			return fmt.Errorf("cli result: error requires a root stable error code")
		}
		if envelope.Result.resultFamily() != familyError {
			return fmt.Errorf("cli result: non-partial error must use common error result")
		}
		if err := validateErrorPair(envelope.Command, envelope.ErrorCode, envelope.ErrorStage); err != nil {
			return err
		}
		result, ok := envelope.Result.(*ErrorResult)
		if !ok {
			return fmt.Errorf("cli result: common error result must use *ErrorResult")
		}
		if result.ErrorStage != envelope.ErrorStage {
			return fmt.Errorf("cli result: error stage disagrees with result.error_stage")
		}
		if err := validateErrorResultSemantics(envelope.Command, envelope.ErrorCode, result); err != nil {
			return fmt.Errorf("cli result: error result: %w", err)
		}
		if envelope.ErrorCode == ErrorCLIUsage && len(envelope.Warnings) != 0 {
			return fmt.Errorf("cli result: usage cannot carry runtime warnings")
		}
	}
	return validateEnvelopeSemantics(envelope)
}

func validateEnvelopeSemantics(envelope Envelope) error {
	hasWarning := func(code WarningCode) bool {
		for _, warning := range envelope.Warnings {
			if warning.WarningCode == code {
				return true
			}
		}
		return false
	}
	hasAction := func(code ActionCode) bool {
		for _, action := range envelope.RequiredActions {
			if action.Code == code {
				return true
			}
		}
		return false
	}
	if envelope.Outcome != OutcomeError {
		switch result := envelope.Result.(type) {
		case *ExportJSONLResult:
			if !hasWarning(WarningRuntimeUnmanagedCopy) {
				return fmt.Errorf("cli result: export.jsonl requires runtime_unmanaged_copy warning")
			}
		case *BackupRestoreResult:
			if envelope.Outcome == OutcomeSuccess && !result.Published {
				return fmt.Errorf("cli result: successful backup.restore requires published=true")
			}
			if envelope.Outcome == OutcomeSuccess && !hasAction(ActionStartService) {
				return fmt.Errorf("cli result: successful backup.restore requires start_service handoff")
			}
			if !result.ServiceReady && !hasWarning(WarningServiceNotReady) {
				return fmt.Errorf("cli result: service_ready=false requires service_not_ready warning")
			}
			if result.ServiceReady && hasWarning(WarningServiceNotReady) {
				return fmt.Errorf("cli result: service_ready=true cannot warn service_not_ready")
			}
		case *BlobGCResult:
			if !hasWarning(WarningPhysicalErasureNotGuaranteed) {
				return fmt.Errorf("cli result: blob.gc requires physical_erasure_not_guaranteed warning")
			}
			if envelope.Outcome == OutcomePartial {
				if len(envelope.RequiredActions) != 1 || !hasAction(ActionRerunBlobGCDryRun) {
					return fmt.Errorf("cli result: partial blob.gc requires exactly one rerun_blob_gc_dry_run action")
				}
				if len(envelope.TargetIDs) != 1 || envelope.TargetIDs[0] != result.ResidentID {
					return fmt.Errorf("cli result: partial blob.gc target must equal result resident")
				}
			} else if len(envelope.RequiredActions) != 0 {
				return fmt.Errorf("cli result: successful blob.gc cannot require an action")
			}
		case *ErasureApplyResult:
			if !envelope.CanonicalApplied {
				return fmt.Errorf("cli result: erasure apply success/partial requires a Canonical erasure commit")
			}
			matched := false
			for _, commit := range envelope.CanonicalCommits {
				if commit.CommitID == result.CanonicalErasureCommitID && commit.CommitSeq == result.CanonicalErasureCommitSeq {
					matched = true
					wantDisposition := DispositionCreated
					if result.ExistingCommit {
						wantDisposition = DispositionExisting
					}
					if commit.Disposition != wantDisposition {
						return fmt.Errorf("cli result: erasure existing_commit disagrees with commit disposition")
					}
				}
			}
			if !matched {
				return fmt.Errorf("cli result: erasure result commit is absent from canonical_commits")
			}
		}
	}
	if envelope.Command == CommandHealthcheck && len(envelope.TargetIDs) != 0 {
		return fmt.Errorf("cli result: healthcheck cannot expose target IDs")
	}
	return nil
}

func validateCanonicalCommits(commits []CanonicalCommit) error {
	seen := make(map[string]struct{}, len(commits))
	var previous *big.Int
	for index, commit := range commits {
		if commit.ResidentID != nil && !validULID(*commit.ResidentID) {
			return fmt.Errorf("cli result: canonical_commits[%d].resident_id is not canonical ULID", index)
		}
		if !validULID(commit.CommitID) {
			return fmt.Errorf("cli result: canonical_commits[%d].commit_id is not canonical ULID", index)
		}
		if _, exists := seen[commit.CommitID]; exists {
			return fmt.Errorf("cli result: duplicate canonical commit %q", commit.CommitID)
		}
		seen[commit.CommitID] = struct{}{}
		seq, ok := parsePositiveDecimal(commit.CommitSeq)
		if !ok {
			return fmt.Errorf("cli result: canonical_commits[%d].commit_seq is not a positive decimal string", index)
		}
		if previous != nil && seq.Cmp(previous) <= 0 {
			return fmt.Errorf("cli result: canonical_commits are not in strictly increasing commit_seq order")
		}
		previous = seq
		if commit.Disposition != DispositionCreated && commit.Disposition != DispositionExisting {
			return fmt.Errorf("cli result: canonical_commits[%d].disposition is not closed", index)
		}
		if len(commit.Effects) == 0 {
			return fmt.Errorf("cli result: canonical_commits[%d].effects must be non-empty", index)
		}
		for effectIndex, effect := range commit.Effects {
			if !validEffect(effect) {
				return fmt.Errorf("cli result: canonical_commits[%d].effects[%d] is not closed", index, effectIndex)
			}
			if effectIndex > 0 && commit.Effects[effectIndex-1] >= effect {
				return fmt.Errorf("cli result: canonical_commits[%d].effects must be lexical and duplicate-free", index)
			}
		}
	}
	return nil
}

func validateWarnings(warnings []Warning) error {
	seen := make(map[string]struct{}, len(warnings))
	previous := ""
	for index, warning := range warnings {
		if !validWarningCode(warning.WarningCode) {
			return fmt.Errorf("cli result: warnings[%d].warning_code is not closed", index)
		}
		if warning.TargetIDs == nil {
			return fmt.Errorf("cli result: warnings[%d].target_ids must be an array", index)
		}
		if err := validateTargetIDs(warning.TargetIDs, true); err != nil {
			return fmt.Errorf("cli result: warnings[%d].target_ids: %w", index, err)
		}
		key := string(warning.WarningCode) + "\x00" + strings.Join(warning.TargetIDs, "\x00")
		if _, exists := seen[key]; exists {
			return fmt.Errorf("cli result: duplicate warning %q", warning.WarningCode)
		}
		if index > 0 && key <= previous {
			return fmt.Errorf("cli result: warnings must be sorted by warning_code,target_ids")
		}
		seen[key] = struct{}{}
		previous = key
	}
	return nil
}

func NewWarning(code WarningCode, targetIDs ...string) (Warning, error) {
	warning := Warning{WarningCode: code, TargetIDs: append([]string{}, targetIDs...)}
	slices.Sort(warning.TargetIDs)
	if warning.TargetIDs == nil {
		warning.TargetIDs = []string{}
	}
	if !validWarningCode(code) {
		return Warning{}, fmt.Errorf("cli result: warning code %q is not closed", code)
	}
	if err := validateTargetIDs(warning.TargetIDs, true); err != nil {
		return Warning{}, err
	}
	return warning, nil
}

func validateTargetIDs(values []string, allowEmpty bool) error {
	if !allowEmpty && len(values) == 0 {
		return fmt.Errorf("at least one target ID is required")
	}
	for index, value := range values {
		if !validTargetID(value) {
			return fmt.Errorf("entry %d is not a sanitized target token", index)
		}
		if index > 0 && values[index-1] >= value {
			return fmt.Errorf("entries must be lexical and duplicate-free")
		}
	}
	return nil
}

func NewRequiredAction(code ActionCode, argv, targetIDs, prerequisites []string) (RequiredAction, error) {
	action := RequiredAction{
		Code:                  code,
		Argv:                  append([]string{}, argv...),
		TargetIDs:             append([]string{}, targetIDs...),
		PrerequisiteActionIDs: append([]string{}, prerequisites...),
	}
	slices.Sort(action.TargetIDs)
	slices.Sort(action.PrerequisiteActionIDs)
	if action.Argv == nil {
		action.Argv = []string{}
	}
	if action.TargetIDs == nil {
		action.TargetIDs = []string{}
	}
	if action.PrerequisiteActionIDs == nil {
		action.PrerequisiteActionIDs = []string{}
	}
	if err := validateActionShape(action); err != nil {
		return RequiredAction{}, err
	}
	id, err := requiredActionID(action.Code, action.Argv, action.TargetIDs)
	if err != nil {
		return RequiredAction{}, err
	}
	action.ActionID = id
	return action, nil
}

func requiredActionID(code ActionCode, argv, targetIDs []string) (string, error) {
	projection := struct {
		Code      ActionCode `json:"code"`
		Argv      []string   `json:"argv"`
		TargetIDs []string   `json:"target_ids"`
	}{code, nonNil(argv), nonNil(targetIDs)}
	canonical, err := marshalJCS(projection)
	if err != nil {
		return "", err
	}
	digest := sha256.New()
	_, _ = digest.Write([]byte("mahoroba:cli-required-action:v1\x00"))
	_, _ = digest.Write(canonical)
	return "sha256:" + hex.EncodeToString(digest.Sum(nil)), nil
}

func validateActions(actions []RequiredAction) error {
	ids := make(map[string]int, len(actions))
	previous := ""
	for index, action := range actions {
		if action.Argv == nil || action.TargetIDs == nil || action.PrerequisiteActionIDs == nil {
			return fmt.Errorf("cli result: required_actions[%d] arrays must not be null", index)
		}
		if err := validateActionShape(action); err != nil {
			return fmt.Errorf("cli result: required_actions[%d]: %w", index, err)
		}
		wantID, err := requiredActionID(action.Code, action.Argv, action.TargetIDs)
		if err != nil {
			return err
		}
		if action.ActionID != wantID {
			return fmt.Errorf("cli result: required_actions[%d].action_id does not bind code,argv,target_ids", index)
		}
		if _, exists := ids[action.ActionID]; exists {
			return fmt.Errorf("cli result: duplicate required action ID %q", action.ActionID)
		}
		key := string(action.Code) + "\x00" + action.ActionID
		if index > 0 && key <= previous {
			return fmt.Errorf("cli result: required_actions must be sorted by code,action_id")
		}
		ids[action.ActionID] = index
		previous = key
	}

	for index, action := range actions {
		for prerequisiteIndex, prerequisite := range action.PrerequisiteActionIDs {
			if !validDigest(prerequisite) {
				return fmt.Errorf("cli result: required_actions[%d].prerequisite_action_ids[%d] is not sha256", index, prerequisiteIndex)
			}
			if prerequisite == action.ActionID {
				return fmt.Errorf("cli result: required_actions[%d] self-depends", index)
			}
			if _, exists := ids[prerequisite]; !exists {
				return fmt.Errorf("cli result: required_actions[%d] prerequisite is missing", index)
			}
			if prerequisiteIndex > 0 && action.PrerequisiteActionIDs[prerequisiteIndex-1] >= prerequisite {
				return fmt.Errorf("cli result: prerequisite_action_ids must be lexical and duplicate-free")
			}
		}
	}

	state := make([]uint8, len(actions))
	var visit func(int) error
	visit = func(index int) error {
		if state[index] == 1 {
			return fmt.Errorf("cli result: required_actions prerequisite graph contains a cycle")
		}
		if state[index] == 2 {
			return nil
		}
		state[index] = 1
		for _, prerequisite := range actions[index].PrerequisiteActionIDs {
			if err := visit(ids[prerequisite]); err != nil {
				return err
			}
		}
		state[index] = 2
		return nil
	}
	for index := range actions {
		if err := visit(index); err != nil {
			return err
		}
	}
	return nil
}

func validateCommandActions(command CommandID, actions []RequiredAction) error {
	if command != CommandBackupRestore {
		return nil
	}
	var start *RequiredAction
	required := make(map[string]struct{})
	for index := range actions {
		action := &actions[index]
		switch action.Code {
		case ActionStartService:
			if start != nil {
				return fmt.Errorf("cli result: backup.restore has multiple start_service actions")
			}
			start = action
		case ActionSelectActiveResident, ActionSelectSessionPolicy, ActionRepairConfig, ActionRebuildProjection:
			required[action.ActionID] = struct{}{}
		}
	}
	if start == nil && len(required) != 0 {
		return fmt.Errorf("cli result: backup.restore repair/select/rebuild actions require start_service handoff")
	}
	if start != nil {
		for actionID := range required {
			if !slices.Contains(start.PrerequisiteActionIDs, actionID) {
				return fmt.Errorf("cli result: backup.restore start_service is missing a select/repair/rebuild prerequisite")
			}
		}
	}
	return nil
}

func validateActionShape(action RequiredAction) error {
	if !validActionCode(action.Code) {
		return fmt.Errorf("action code %q is not closed", action.Code)
	}
	if err := validateTargetIDs(action.TargetIDs, true); err != nil {
		return fmt.Errorf("target_ids: %w", err)
	}
	for index, token := range action.Argv {
		if token == "" || !utf8.ValidString(token) || strings.ContainsAny(token, "\x00\r\n") {
			return fmt.Errorf("argv[%d] is not a safe token", index)
		}
	}
	switch action.Code {
	case ActionSelectActiveResident, ActionSelectSessionPolicy:
		if len(action.Argv) != 0 || !allULIDs(action.TargetIDs) {
			return fmt.Errorf("selection action requires empty argv and canonical ULID targets")
		}
	case ActionRunIntegrityScan:
		if len(action.TargetIDs) != 1 || !validULID(action.TargetIDs[0]) ||
			len(action.Argv) < 8 || !slices.Equal(action.Argv[:5], []string{"mahoroba", "admin", "integrity", "scan", "--resident"}) ||
			action.Argv[5] != action.TargetIDs[0] || validateSourceArgv(action.Argv, 6) != nil {
			return fmt.Errorf("run_integrity_scan argv/target shape is invalid")
		}
	case ActionRunRecoveryTerminalize:
		if len(action.TargetIDs) == 0 || !allULIDs(action.TargetIDs) ||
			len(action.Argv) < 6 || !slices.Equal(action.Argv[:4], []string{"mahoroba", "admin", "recovery", "terminalize"}) ||
			validateSourceArgv(action.Argv, 4) != nil {
			return fmt.Errorf("run_recovery_terminalize argv/target shape is invalid")
		}
	case ActionArchiveResident:
		if len(action.TargetIDs) != 1 || !validULID(action.TargetIDs[0]) ||
			len(action.Argv) < 8 || !slices.Equal(action.Argv[:5], []string{"mahoroba", "admin", "resident", "archive", "--resident"}) ||
			action.Argv[5] != action.TargetIDs[0] || validateSourceArgv(action.Argv, 6) != nil {
			return fmt.Errorf("archive_resident argv/target shape is invalid")
		}
	case ActionRebuildProjection:
		if len(action.TargetIDs) != 1 || !validULID(action.TargetIDs[0]) || len(action.Argv) < 9 ||
			!slices.Equal(action.Argv[:4], []string{"mahoroba", "projection", "rebuild", "--resident"}) ||
			action.Argv[4] != action.TargetIDs[0] || action.Argv[5] != "--name" ||
			!validSafeToken(action.Argv[6]) || validateSourceArgv(action.Argv, 7) != nil {
			return fmt.Errorf("rebuild_projection argv/target shape is invalid")
		}
	case ActionRepairConfig:
		if len(action.Argv) != 0 || len(action.TargetIDs) != 0 {
			return fmt.Errorf("repair_config requires empty argv and targets")
		}
	case ActionStartService:
		if len(action.TargetIDs) != 0 || len(action.Argv) < 4 ||
			!slices.Equal(action.Argv[:2], []string{"mahoroba", "serve"}) || validateSourceArgv(action.Argv, 2) != nil {
			return fmt.Errorf("start_service argv/target shape is invalid")
		}
	case ActionReplanErasure:
		if len(action.Argv) != 0 || len(action.TargetIDs) == 0 || !allULIDs(action.TargetIDs) {
			return fmt.Errorf("replan_erasure requires empty argv and canonical ULID targets")
		}
	case ActionRerunBlobGCDryRun:
		if len(action.TargetIDs) != 1 || !validULID(action.TargetIDs[0]) || len(action.Argv) < 7 ||
			!slices.Equal(action.Argv[:4], []string{"mahoroba", "blob", "gc", "--resident"}) ||
			action.Argv[4] != action.TargetIDs[0] || validateSourceArgv(action.Argv, 5) != nil {
			return fmt.Errorf("rerun_blob_gc_dry_run argv/target shape is invalid")
		}
	case ActionRepairStaticDesign:
		if len(action.Argv) != 0 {
			return fmt.Errorf("repair_static_design requires empty argv")
		}
	}
	return nil
}

func validateSourceArgv(argv []string, start int) error {
	if start < 0 || start > len(argv) {
		return fmt.Errorf("invalid source argv")
	}
	remainder := argv[start:]
	switch len(remainder) {
	case 2:
		if remainder[0] != "--data-dir" || !validAbsolutePath(remainder[1]) {
			return fmt.Errorf("source argv requires canonical --data-dir")
		}
	case 4:
		if remainder[0] != "--config" || !validAbsolutePath(remainder[1]) ||
			remainder[2] != "--data-dir" || !validAbsolutePath(remainder[3]) {
			return fmt.Errorf("source argv requires canonical --config then --data-dir")
		}
	default:
		return fmt.Errorf("source argv has an invalid shape")
	}
	return nil
}

func validateErrorPair(command CommandID, code ErrorCode, stage Stage) error {
	if !validStage(stage) {
		return fmt.Errorf("cli result: error stage %q is not closed", stage)
	}
	allowed := func(commands []CommandID, stages []Stage) error {
		if !slices.Contains(commands, command) || !slices.Contains(stages, stage) {
			return fmt.Errorf("cli result: error code %q is not allowed for command %q at stage %q", code, command, stage)
		}
		return nil
	}
	recognized := recognizedCommands()
	source := sourceCommands()
	config := configCommands()
	data := dataCommands()
	artifact := []CommandID{CommandBackupCreate, CommandBackupRestore, CommandExportJSONL, CommandAdminErasurePlanContent, CommandAdminErasurePlanResident, CommandAdminErasureDecide}
	erasureDecisionApply := []CommandID{CommandAdminErasureDecide, CommandAdminErasureApply}
	switch code {
	case ErrorCLIUsage:
		return allowed(append([]CommandID{CommandUnknown}, recognized...), []Stage{StageUsage})
	case ErrorConfigurationInvalid, ErrorSecretSourceConflict, ErrorSecretFileInvalid, ErrorSecretFileUnsupportedPlatform:
		return allowed(config, []Stage{StagePreflight})
	case ErrorAdminLockBusy:
		return allowed(source, []Stage{StagePreflight})
	case ErrorResidentAdmissionClosed:
		return allowed(residentAdmissionCommands(), []Stage{StagePreflight})
	case ErrorSourceUnavailable:
		return allowed(without(recognized, CommandHealthcheck), []Stage{StagePreflight})
	case ErrorArtifactTargetExists:
		return allowed([]CommandID{CommandExportJSONL, CommandAdminErasurePlanContent, CommandAdminErasurePlanResident, CommandAdminErasureDecide}, []Stage{StagePreflight})
	case ErrorBackupTargetExists, ErrorBackupInvalid:
		return allowed([]CommandID{CommandBackupCreate, CommandBackupVerify, CommandBackupRestore}, []Stage{StagePreflight})
	case ErrorRestoreTargetExists, ErrorRestoreNamespaceBusy:
		return allowed([]CommandID{CommandBackupRestore}, []Stage{StagePreflight})
	case ErrorArtifactIOFailed:
		return allowed(artifact, []Stage{StagePreflight, StagePublish})
	case ErrorPublishDurabilityUnknown:
		return allowed(artifact, []Stage{StagePublish})
	case ErrorIntegrityFatal, ErrorLegacyClaimIdentityRequiresStaticRepair, ErrorStaticDesignReopenRequired:
		return allowed(data, []Stage{StagePreflight})
	case ErrorIntegrityApplyConflict, ErrorIntegrityPipelineRequired:
		return allowed([]CommandID{CommandAdminIntegrityScan, CommandBackupRestore, CommandAdminRecoveryTerminalize, CommandAdminErasureApply}, []Stage{StageCanonical})
	case ErrorErasureReviewRequired, ErrorErasurePlanStale, ErrorErasureIneligible, ErrorErasureRetryConflict:
		return allowed(erasureDecisionApply, []Stage{StagePreflight, StageCanonical})
	case ErrorProjectionNotCurrent:
		return allowed([]CommandID{CommandBackupRestore, CommandAdminErasureApply, CommandBlobGC}, []Stage{StageDerived})
	case ErrorGCPlanStale:
		return allowed([]CommandID{CommandBlobGC}, []Stage{StagePreflight, StageCanonical})
	case ErrorRecoveryAttemptOverflow:
		return allowed([]CommandID{CommandAdminRecoveryTerminalize, CommandBackupRestore, CommandAdminErasurePlanContent, CommandAdminErasurePlanResident, CommandAdminErasureDecide, CommandAdminErasureApply}, []Stage{StagePreflight, StageCanonical})
	case ErrorHealthcheckNotReady:
		return allowed([]CommandID{CommandHealthcheck}, []Stage{StageReadiness})
	case ErrorHealthcheckUnreachable, ErrorHealthcheckInvalidResponse:
		return allowed([]CommandID{CommandHealthcheck}, []Stage{StagePreflight})
	case ErrorOperationPartial:
		return allowed([]CommandID{CommandAdminIntegrityScan, CommandBackupRestore, CommandAdminRecoveryTerminalize, CommandAdminErasureApply, CommandBlobGC}, []Stage{StageCanonical, StageDerived})
	case ErrorOperationFailed:
		return allowed(recognized, []Stage{StagePreflight, StageCanonical, StageDerived, StagePublish})
	default:
		return fmt.Errorf("cli result: error code %q is not closed", code)
	}
}

func marshalJCS(value any) ([]byte, error) {
	encoded, err := json.Marshal(value)
	if err != nil {
		return nil, fmt.Errorf("cli result: marshal JCS input: %w", err)
	}
	canonical, err := jcs.Transform(encoded)
	if err != nil {
		return nil, fmt.Errorf("cli result: canonicalize JCS input: %w", err)
	}
	return canonical, nil
}

func validULID(value string) bool {
	parsed, err := ulid.ParseStrict(value)
	return err == nil && parsed.String() == value
}

func allULIDs(values []string) bool {
	for _, value := range values {
		if !validULID(value) {
			return false
		}
	}
	return true
}

func validDigest(value string) bool { return digestPattern.MatchString(value) }

func validDecimal(value string) bool { return decimalPattern.MatchString(value) }

func parsePositiveDecimal(value string) (*big.Int, bool) {
	if !validDecimal(value) || value == "0" {
		return nil, false
	}
	parsed, ok := new(big.Int).SetString(value, 10)
	return parsed, ok
}

func parseNonNegativeDecimal(value string) (*big.Int, bool) {
	if !validDecimal(value) {
		return nil, false
	}
	parsed, ok := new(big.Int).SetString(value, 10)
	return parsed, ok
}

func validSafeToken(value string) bool {
	return len(value) <= 512 && utf8.ValidString(value) && safeTokenPattern.MatchString(value) &&
		!strings.Contains(value, "..")
}

func validTargetID(value string) bool {
	if !validSafeToken(value) {
		return false
	}
	if validULID(value) {
		return true
	}
	for _, component := range strings.Split(value, "/") {
		if validULID(component) {
			return true
		}
	}
	return false
}

func validAbsolutePath(value string) bool {
	if value == "" || len(value) > 32767 || !utf8.ValidString(value) || strings.ContainsAny(value, "\x00\r\n") {
		return false
	}
	abs := filepath.IsAbs(value) || strings.HasPrefix(value, "/") || windowsDrivePattern.MatchString(value)
	if !abs {
		return false
	}
	clean := filepath.Clean(value)
	if filepath.IsAbs(value) {
		return clean == value
	}
	// A foreign-platform canonical path can appear only in an artifact result.
	// Reject traversal and repeated separators even when filepath cannot parse it.
	if strings.HasPrefix(value, "/") {
		return path.Clean(value) == value
	}
	return !strings.Contains(value, "/../") && !strings.Contains(value, "/./") &&
		!strings.Contains(value, "\\..\\") && !strings.Contains(value, "\\.\\") &&
		!strings.Contains(value, "//") && !strings.Contains(value, "\\\\") &&
		!strings.HasSuffix(value, "/.") && !strings.HasSuffix(value, "\\.")
}

func validBasename(value string) bool {
	return value != "" && value != "." && value != ".." && utf8.ValidString(value) &&
		!strings.ContainsAny(value, "\\/\x00\r\n") && filepath.Base(value) == value
}

func validTimezone(value string) bool {
	if value == "" || value == "Local" || strings.TrimSpace(value) != value {
		return false
	}
	_, err := time.LoadLocation(value)
	return err == nil
}

func validCommand(value CommandID) bool {
	return value == CommandUnknown || slices.Contains(recognizedCommands(), value)
}

func recognizedCommands() []CommandID {
	return []CommandID{
		CommandAdminIntegrityScan, CommandBackupCreate, CommandBackupVerify, CommandBackupRestore,
		CommandExportJSONL, CommandAdminRecoveryTerminalize, CommandAdminSessionPolicySelect,
		CommandAdminErasurePlanContent, CommandAdminErasurePlanResident, CommandAdminErasureDecide,
		CommandAdminErasureApply, CommandBlobGC, CommandAdminDiagnostics, CommandHealthcheck,
	}
}

func sourceCommands() []CommandID {
	return []CommandID{
		CommandAdminIntegrityScan, CommandBackupCreate, CommandExportJSONL, CommandAdminRecoveryTerminalize,
		CommandAdminSessionPolicySelect, CommandAdminErasurePlanContent, CommandAdminErasurePlanResident,
		CommandAdminErasureDecide, CommandAdminErasureApply, CommandBlobGC, CommandAdminDiagnostics,
	}
}

func dataCommands() []CommandID {
	return []CommandID{
		CommandAdminIntegrityScan, CommandBackupCreate, CommandBackupVerify, CommandBackupRestore,
		CommandExportJSONL, CommandAdminRecoveryTerminalize, CommandAdminSessionPolicySelect,
		CommandAdminErasurePlanContent, CommandAdminErasurePlanResident, CommandAdminErasureDecide,
		CommandAdminErasureApply, CommandBlobGC,
	}
}

func residentAdmissionCommands() []CommandID {
	return []CommandID{
		CommandAdminIntegrityScan, CommandBackupCreate, CommandExportJSONL, CommandAdminRecoveryTerminalize,
		CommandAdminErasurePlanContent, CommandAdminErasurePlanResident, CommandAdminErasureDecide,
		CommandAdminErasureApply, CommandBlobGC,
	}
}

func configCommands() []CommandID {
	return append(sourceCommands(), CommandBackupRestore, CommandHealthcheck)
}

func without(values []CommandID, excluded CommandID) []CommandID {
	result := make([]CommandID, 0, len(values))
	for _, value := range values {
		if value != excluded {
			result = append(result, value)
		}
	}
	return result
}

func partialCommand(value CommandID) bool {
	return slices.Contains([]CommandID{
		CommandAdminIntegrityScan, CommandBackupRestore, CommandAdminRecoveryTerminalize,
		CommandAdminErasureApply, CommandBlobGC,
	}, value)
}

func validStage(value Stage) bool {
	return slices.Contains([]Stage{StageUsage, StagePreflight, StageCanonical, StageDerived, StagePublish, StageReadiness}, value)
}

func validEffect(value EffectCode) bool {
	return slices.Contains([]EffectCode{
		EffectClaimIdentityErasureRecorded, EffectClaimStatusQuarantined, EffectContentErased,
		EffectGenerationAttemptTerminalized, EffectIntegrityFindingRecorded, EffectMandatoryWorkCancelled,
		EffectPipelineVersionRegistered, EffectResidentStatusErased, EffectRuntimeSelectionCleared,
	}, value)
}

func validWarningCode(value WarningCode) bool {
	return slices.Contains([]WarningCode{
		WarningRuntimeUnmanagedCopy, WarningPhysicalErasureNotGuaranteed, WarningServiceNotReady,
		WarningLocalAdminNotAttributed, WarningProjectionNotApplicable, WarningGCMaintenanceIncomplete,
	}, value)
}

func validActionCode(value ActionCode) bool {
	return slices.Contains([]ActionCode{
		ActionSelectActiveResident, ActionSelectSessionPolicy, ActionRunIntegrityScan,
		ActionRunRecoveryTerminalize, ActionArchiveResident, ActionRebuildProjection,
		ActionRepairConfig, ActionStartService, ActionReplanErasure, ActionRerunBlobGCDryRun,
		ActionRepairStaticDesign,
	}, value)
}
