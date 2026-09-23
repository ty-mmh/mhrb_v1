package main

import (
	"context"
	"errors"
	"flag"
	"io"
	"path/filepath"
	"runtime"
	"runtime/debug"
	"strconv"
	"strings"

	"mahoroba.local/mahoroba/internal/backup"
	"mahoroba.local/mahoroba/internal/cliresult"
	"mahoroba.local/mahoroba/internal/config"
	"mahoroba.local/mahoroba/internal/domain"
	"mahoroba.local/mahoroba/internal/integrity"
	"mahoroba.local/mahoroba/internal/operationalmetrics"
	"mahoroba.local/mahoroba/internal/projection"
	"mahoroba.local/mahoroba/internal/readiness"
	"mahoroba.local/mahoroba/internal/restore"
)

type backupCreateExecutor func(context.Context, backup.CreateRequest) (backup.Result, error)
type backupVerifyExecutor func(context.Context, string) (backup.VerifiedBundle, error)
type backupRestoreExecutor func(context.Context, restore.Request) (restore.Result, error)

func runBackupCreate(ctx context.Context, arguments []string, stdout, stderr io.Writer) int {
	return runBackupCreateWithExecutor(ctx, arguments, stdout, stderr, backup.Create)
}

func runBackupCreateWithExecutor(
	ctx context.Context,
	arguments []string,
	stdout, stderr io.Writer,
	execute backupCreateExecutor,
) int {
	flags := flag.NewFlagSet("backup create", flag.ContinueOnError)
	flags.SetOutput(io.Discard)
	output := flags.String("output", "", "absolute backup directory")
	includeProjections := flags.Bool("include-projections", false, "include rebuildable Projection tables")
	configPath := flags.String("config", "", "TOML configuration path")
	dataDir := flags.String("data-dir", "", "absolute Mahoroba data directory")
	if duplicateBackupFlag(arguments, "--output", "--include-projections", "--config", "--data-dir") ||
		flags.Parse(arguments) != nil || flags.NArg() != 0 || *output == "" || !filepath.IsAbs(*output) {
		return renderAdminIntegrityEnvelope(stdout, stderr,
			cliresult.NewFailure(cliresult.CommandBackupCreate, cliresult.ErrorCLIUsage, cliresult.StageUsage, nil))
	}
	cfg, err := config.Load(config.Overrides{ConfigPath: *configPath, DataDir: *dataDir})
	if err != nil {
		return renderAdminIntegrityEnvelope(stdout, stderr, cliresult.NewFailure(
			cliresult.CommandBackupCreate, classifyHealthcheckConfigError(err), cliresult.StagePreflight, nil,
		))
	}
	artifactPath := filepath.Clean(*output)
	result, operationErr := execute(ctx, backup.CreateRequest{
		SourceDataDir: cfg.DataDir, DatabaseFilename: cfg.Database.Filename,
		Output: artifactPath, IncludeProjections: *includeProjections,
		CreatedBy: currentBackupBuildIdentity(), Observer: operationalmetrics.New(),
	})
	if operationErr != nil {
		return renderBackupFailure(stdout, stderr, cliresult.CommandBackupCreate, artifactPath, operationErr)
	}
	return renderAdminIntegrityEnvelope(stdout, stderr, cliresult.NewSuccess(
		cliresult.CommandBackupCreate, backupResultToCLI(result),
	))
}

func runBackupRestore(ctx context.Context, arguments []string, stdout, stderr io.Writer) int {
	return runBackupRestoreWithExecutor(ctx, arguments, stdout, stderr, restore.Restore)
}

func runBackupRestoreWithExecutor(
	ctx context.Context,
	arguments []string,
	stdout, stderr io.Writer,
	execute backupRestoreExecutor,
) int {
	flags := flag.NewFlagSet("backup restore", flag.ContinueOnError)
	flags.SetOutput(io.Discard)
	input := flags.String("input", "", "absolute backup directory")
	target := flags.String("target-data-dir", "", "absolute restored data directory")
	configPath := flags.String("config", "", "TOML configuration path")
	if duplicateBackupFlag(arguments, "--input", "--target-data-dir", "--config") ||
		flags.Parse(arguments) != nil || flags.NArg() != 0 || *input == "" || *target == "" ||
		!filepath.IsAbs(*input) || !filepath.IsAbs(*target) {
		return renderAdminIntegrityEnvelope(stdout, stderr,
			cliresult.NewFailure(cliresult.CommandBackupRestore, cliresult.ErrorCLIUsage, cliresult.StageUsage, nil))
	}
	bundleRoot := filepath.Clean(*input)
	targetDataDir := filepath.Clean(*target)
	canonicalConfigPath, err := canonicalRestoreConfigPath(*configPath)
	if err != nil {
		return renderAdminIntegrityEnvelope(stdout, stderr, cliresult.NewFailure(
			cliresult.CommandBackupRestore, cliresult.ErrorConfigurationInvalid, cliresult.StagePreflight, nil,
		))
	}
	cfg, err := config.Load(config.Overrides{ConfigPath: canonicalConfigPath, DataDir: targetDataDir})
	if err != nil {
		return renderAdminIntegrityEnvelope(stdout, stderr, cliresult.NewFailure(
			cliresult.CommandBackupRestore, classifyHealthcheckConfigError(err), cliresult.StagePreflight, nil,
		))
	}
	result, operationErr := execute(ctx, restore.Request{
		BundleRoot: bundleRoot, TargetDataDir: targetDataDir, Observer: operationalmetrics.New(),
	})
	if operationErr != nil && !result.Published {
		return renderRestoreFailure(stdout, stderr, targetDataDir, result, operationErr)
	}
	envelope, err := restoreCLIEnvelope(result, targetDataDir, canonicalConfigPath, cfg.Database.Filename, operationErr)
	if err != nil {
		// A malformed production result or action is an internal closed-boundary
		// failure. Do not emit an unvalidated substitute containing partial state.
		return cliresult.ExitOperational
	}
	return renderAdminIntegrityEnvelope(stdout, stderr, envelope)
}

func canonicalRestoreConfigPath(value string) (string, error) {
	if value == "" {
		return "", nil
	}
	absolute, err := filepath.Abs(value)
	if err != nil {
		return "", err
	}
	return filepath.Clean(absolute), nil
}

func restoreCLIEnvelope(
	result restore.Result,
	targetDataDir, configPath, configuredDatabaseFilename string,
	operationErr error,
) (cliresult.Envelope, error) {
	cliResult := restoreResultToCLI(result, targetDataDir)
	configReady := configuredDatabaseFilename == restore.DatabaseFilename
	cliResult.ServiceReady = result.ServiceReady && configReady
	var envelope cliresult.Envelope
	if operationErr == nil {
		envelope = cliresult.NewSuccess(cliresult.CommandBackupRestore, cliResult)
	} else {
		code, stage := classifyRestoreError(operationErr)
		envelope = cliresult.NewPartial(cliresult.CommandBackupRestore, code, stage, cliResult)
	}
	envelope.CanonicalApplied = result.CanonicalApplied
	envelope.CanonicalCommits = restoreCLICommits(result.CanonicalCommits)

	var actions []cliresult.RequiredAction
	var prerequisites []string
	if !configReady {
		repair, err := cliresult.NewRequiredAction(cliresult.ActionRepairConfig, nil, nil, nil)
		if err != nil {
			return cliresult.Envelope{}, err
		}
		actions = append(actions, repair)
		prerequisites = append(prerequisites, repair.ActionID)
	}
	startArgv := []string{"mahoroba", "serve"}
	if configPath != "" {
		startArgv = append(startArgv, "--config", configPath)
	}
	startArgv = append(startArgv, "--data-dir", targetDataDir)
	start, err := cliresult.NewRequiredAction(cliresult.ActionStartService, startArgv, nil, prerequisites)
	if err != nil {
		return cliresult.Envelope{}, err
	}
	actions = append(actions, start)
	envelope.RequiredActions = actions
	if !cliResult.ServiceReady {
		warning, err := cliresult.NewWarning(cliresult.WarningServiceNotReady)
		if err != nil {
			return cliresult.Envelope{}, err
		}
		envelope.Warnings = []cliresult.Warning{warning}
	}
	return envelope, nil
}

func restoreResultToCLI(result restore.Result, targetDataDir string) *cliresult.BackupRestoreResult {
	return &cliresult.BackupRestoreResult{
		TargetDataDir: targetDataDir, DatabaseFilename: result.DatabaseFilename,
		SourceHead: restoreHeadToCLI(result.SourceHead), RestoredHead: restoreHeadToCLI(result.RestoredHead),
		TerminalizedAttempts: strconv.Itoa(result.TerminalizedAttempts),
		FindingsCreated:      strconv.Itoa(result.CreatedIntegrityFindings),
		ProjectionsRebuilt:   strconv.Itoa(result.ProjectionsRebuilt),
		Published:            result.Published, StagingMarkerID: result.RestoreID.String(),
		ServiceReady: result.ServiceReady,
	}
}

func restoreHeadToCLI(head readiness.Head) cliresult.Head {
	if !head.Exists {
		return cliresult.Head{}
	}
	commitID, commitSeq := head.CommitID.String(), head.CommitSeq.String()
	committedAt, committedTZ := head.CommittedAt.String(), head.CommittedTZ.String()
	return cliresult.Head{
		Exists: true, CommitID: &commitID, CommitSeq: &commitSeq,
		CommittedAt: &committedAt, CommittedTZ: &committedTZ,
	}
}

func restoreCLICommits(commits []restore.Commit) []cliresult.CanonicalCommit {
	result := make([]cliresult.CanonicalCommit, 0, len(commits))
	for _, commit := range commits {
		var residentID *string
		if scopedID, scoped := commit.Metadata.Scope.ResidentID(); scoped {
			value := scopedID.String()
			residentID = &value
		}
		effects := make([]cliresult.EffectCode, 0, 5)
		if commit.Effects.ClaimStatusQuarantined {
			effects = append(effects, cliresult.EffectClaimStatusQuarantined)
		}
		if commit.Effects.GenerationAttemptTerminalized {
			effects = append(effects, cliresult.EffectGenerationAttemptTerminalized)
		}
		if commit.Effects.IntegrityFindingRecorded {
			effects = append(effects, cliresult.EffectIntegrityFindingRecorded)
		}
		if commit.Effects.MandatoryWorkCancelled {
			effects = append(effects, cliresult.EffectMandatoryWorkCancelled)
		}
		if commit.Effects.PipelineVersionRegistered {
			effects = append(effects, cliresult.EffectPipelineVersionRegistered)
		}
		result = append(result, cliresult.CanonicalCommit{
			ResidentID: residentID, CommitID: commit.Metadata.CommitID.String(),
			CommitSeq: commit.Metadata.CommitSeq.String(), Disposition: cliresult.Disposition(commit.Disposition),
			Effects: effects,
		})
	}
	return result
}

func renderRestoreFailure(
	stdout, stderr io.Writer,
	targetDataDir string,
	result restore.Result,
	err error,
) int {
	code, stage := classifyRestoreError(err)
	errorResult := &cliresult.ErrorResult{ErrorStage: stage}
	if code == cliresult.ErrorPublishDurabilityUnknown {
		artifactPath, published := targetDataDir, false
		errorResult.ArtifactPath = &artifactPath
		errorResult.Published = &published
	}
	if !result.RestoreID.IsZero() {
		markerID := result.RestoreID.String()
		errorResult.StagingMarkerID = &markerID
	}
	envelope := cliresult.NewFailure(cliresult.CommandBackupRestore, code, stage, errorResult)
	if code == cliresult.ErrorPublishDurabilityUnknown {
		repair, actionErr := cliresult.NewRequiredAction(cliresult.ActionRepairStaticDesign, nil, nil, nil)
		if actionErr != nil {
			return cliresult.ExitOperational
		}
		envelope.RequiredActions = []cliresult.RequiredAction{repair}
	}
	return renderAdminIntegrityEnvelope(stdout, stderr, envelope)
}

func classifyRestoreError(err error) (cliresult.ErrorCode, cliresult.Stage) {
	switch {
	case errors.Is(err, restore.ErrTargetExists):
		return cliresult.ErrorRestoreTargetExists, cliresult.StagePreflight
	case errors.Is(err, restore.ErrNamespaceBusy):
		return cliresult.ErrorRestoreNamespaceBusy, cliresult.StagePreflight
	case errors.Is(err, restore.ErrInvalidBundle):
		return cliresult.ErrorBackupInvalid, cliresult.StagePreflight
	case errors.Is(err, restore.ErrDurabilityUnknown):
		return cliresult.ErrorPublishDurabilityUnknown, cliresult.StagePublish
	case errors.Is(err, restore.ErrArtifactIO):
		return cliresult.ErrorArtifactIOFailed, cliresult.StagePublish
	case errors.Is(err, domain.ErrRecoveryAttemptOverflow):
		return cliresult.ErrorRecoveryAttemptOverflow, cliresult.StageCanonical
	case errors.Is(err, integrity.ErrFatal):
		return cliresult.ErrorIntegrityFatal, cliresult.StagePreflight
	case errors.Is(err, integrity.ErrCandidateConflict), errors.Is(err, integrity.ErrFindingConflict),
		errors.Is(err, integrity.ErrQuarantineConflict):
		return cliresult.ErrorIntegrityApplyConflict, cliresult.StageCanonical
	case errors.Is(err, projection.ErrCASConflict), errors.Is(err, projection.ErrPreflightHeadChanged):
		return cliresult.ErrorProjectionNotCurrent, cliresult.StageDerived
	case errors.Is(err, restore.ErrUnsafeOverlap):
		return cliresult.ErrorSourceUnavailable, cliresult.StagePreflight
	default:
		return cliresult.ErrorOperationFailed, cliresult.StagePreflight
	}
}

func runBackupVerify(ctx context.Context, arguments []string, stdout, stderr io.Writer) int {
	return runBackupVerifyWithExecutor(ctx, arguments, stdout, stderr, backup.Verify)
}

func runBackupVerifyWithExecutor(
	ctx context.Context,
	arguments []string,
	stdout, stderr io.Writer,
	execute backupVerifyExecutor,
) int {
	flags := flag.NewFlagSet("backup verify", flag.ContinueOnError)
	flags.SetOutput(io.Discard)
	input := flags.String("input", "", "absolute backup directory")
	if duplicateBackupFlag(arguments, "--input") || flags.Parse(arguments) != nil || flags.NArg() != 0 ||
		*input == "" || !filepath.IsAbs(*input) {
		return renderAdminIntegrityEnvelope(stdout, stderr,
			cliresult.NewFailure(cliresult.CommandBackupVerify, cliresult.ErrorCLIUsage, cliresult.StageUsage, nil))
	}
	artifactPath := filepath.Clean(*input)
	verified, operationErr := execute(ctx, artifactPath)
	if operationErr != nil {
		return renderBackupFailure(stdout, stderr, cliresult.CommandBackupVerify, "", operationErr)
	}
	result := &cliresult.BackupResult{
		ArtifactPath: artifactPath, FormatVersion: verified.Manifest.FormatVersion,
		CapturedHead:        backupHeadToCLI(verified.Manifest.CapturedHead),
		ProjectionsIncluded: verified.Manifest.Projections.Policy == "included_requested",
		FileCount:           strconv.FormatInt(verified.FileCount, 10), ByteCount: strconv.FormatInt(verified.ByteCount, 10),
		AuthenticityGuaranteed: verified.Manifest.AuthenticityGuaranteed,
	}
	return renderAdminIntegrityEnvelope(stdout, stderr,
		cliresult.NewSuccess(cliresult.CommandBackupVerify, result))
}

func renderBackupFailure(
	stdout, stderr io.Writer,
	command cliresult.CommandID,
	artifactPath string,
	err error,
) int {
	code, stage := classifyBackupError(err)
	result := &cliresult.ErrorResult{ErrorStage: stage}
	if artifactPath != "" && (code == cliresult.ErrorArtifactIOFailed || code == cliresult.ErrorPublishDurabilityUnknown) {
		result.ArtifactPath = &artifactPath
	}
	if code == cliresult.ErrorPublishDurabilityUnknown {
		published := false
		result.Published = &published
	}
	return renderAdminIntegrityEnvelope(stdout, stderr,
		cliresult.NewFailure(command, code, stage, result))
}

func classifyBackupError(err error) (cliresult.ErrorCode, cliresult.Stage) {
	switch {
	case errors.Is(err, backup.ErrTargetExists):
		return cliresult.ErrorBackupTargetExists, cliresult.StagePreflight
	case errors.Is(err, backup.ErrInvalidBundle):
		return cliresult.ErrorBackupInvalid, cliresult.StagePreflight
	case errors.Is(err, backup.ErrUnsupportedSecurity):
		return cliresult.ErrorStaticDesignReopenRequired, cliresult.StagePreflight
	case errors.Is(err, backup.ErrDurabilityUnknown):
		return cliresult.ErrorPublishDurabilityUnknown, cliresult.StagePublish
	case errors.Is(err, backup.ErrArtifactIO):
		return cliresult.ErrorArtifactIOFailed, cliresult.StagePublish
	case errors.Is(err, backup.ErrSourceUnavailable), errors.Is(err, backup.ErrUnsafeOverlap):
		return cliresult.ErrorSourceUnavailable, cliresult.StagePreflight
	default:
		return cliresult.ErrorOperationFailed, cliresult.StagePreflight
	}
}

func backupResultToCLI(result backup.Result) *cliresult.BackupResult {
	return &cliresult.BackupResult{
		ArtifactPath: result.ArtifactPath, FormatVersion: result.FormatVersion,
		CapturedHead: backupHeadToCLI(result.CapturedHead), ProjectionsIncluded: result.ProjectionsIncluded,
		FileCount: strconv.FormatInt(result.FileCount, 10), ByteCount: strconv.FormatInt(result.ByteCount, 10),
		AuthenticityGuaranteed: result.AuthenticityGuaranteed,
	}
}

func backupHeadToCLI(head backup.CapturedHead) cliresult.Head {
	if !head.Exists {
		return cliresult.Head{}
	}
	return cliresult.Head{
		Exists: true, CommitID: head.CommitID, CommitSeq: head.CommitSeq,
		CommittedAt: head.CommittedAtUnixMicros, CommittedTZ: head.CommittedTZ,
	}
}

func currentBackupBuildIdentity() backup.CreatedBy {
	identity := backup.CreatedBy{BinaryVersion: "development", GitRevision: "unknown", GoVersion: runtime.Version()}
	if info, ok := debug.ReadBuildInfo(); ok {
		if info.Main.Version != "" && info.Main.Version != "(devel)" {
			identity.BinaryVersion = info.Main.Version
		}
		for _, setting := range info.Settings {
			if setting.Key == "vcs.revision" && setting.Value != "" {
				identity.GitRevision = setting.Value
			}
		}
	}
	if validBuildGitRevision(buildGitRevision) {
		identity.GitRevision = buildGitRevision
	}
	return identity
}

func duplicateBackupFlag(arguments []string, names ...string) bool {
	seen := make(map[string]bool, len(names))
	for _, argument := range arguments {
		if argument == "--" {
			break
		}
		for _, name := range names {
			if argument == name || strings.HasPrefix(argument, name+"=") {
				if seen[name] {
					return true
				}
				seen[name] = true
			}
		}
	}
	return false
}
