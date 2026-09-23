package main

import (
	"context"
	"errors"
	"flag"
	"io"
	"path/filepath"
	"slices"
	"strconv"
	"strings"

	"mahoroba.local/mahoroba/internal/canonical"
	"mahoroba.local/mahoroba/internal/cliresult"
	"mahoroba.local/mahoroba/internal/config"
	"mahoroba.local/mahoroba/internal/erasure"
	erasureservice "mahoroba.local/mahoroba/internal/erasure/service"
	"mahoroba.local/mahoroba/internal/hostlock"
	"mahoroba.local/mahoroba/internal/integrity"
	"mahoroba.local/mahoroba/internal/projection"
	"mahoroba.local/mahoroba/internal/readiness"
	storesqlite "mahoroba.local/mahoroba/internal/store/sqlite"
)

type sessionPolicySelectExecution struct {
	PreviousVersionID *canonical.ID
	SelectedVersionID canonical.ID
	ServiceReady      bool
}

type sessionPolicySelectExecutor func(context.Context, config.Config, canonical.ID) (sessionPolicySelectExecution, error)

func runAdminSessionPolicySelect(ctx context.Context, arguments []string, stdout, stderr io.Writer) int {
	return runAdminSessionPolicySelectWithExecutor(ctx, arguments, stdout, stderr, executeSessionPolicySelect)
}

func runAdminSessionPolicySelectWithExecutor(
	ctx context.Context,
	arguments []string,
	stdout, stderr io.Writer,
	execute sessionPolicySelectExecutor,
) int {
	flags := flag.NewFlagSet("admin runtime session-policy select", flag.ContinueOnError)
	flags.SetOutput(io.Discard)
	versionRaw := flags.String("version", "", "exact sessionization policy version ULID")
	configPath := flags.String("config", "", "TOML configuration path")
	dataDir := flags.String("data-dir", "", "absolute Mahoroba data directory")
	command := cliresult.CommandAdminSessionPolicySelect
	if duplicateM7AdminFlag(arguments, nil) || flags.Parse(arguments) != nil || flags.NArg() != 0 || *versionRaw == "" {
		return renderAdminIntegrityEnvelope(stdout, stderr,
			cliresult.NewFailure(command, cliresult.ErrorCLIUsage, cliresult.StageUsage, nil))
	}
	versionID, err := canonical.ParseID(*versionRaw)
	if err != nil {
		return renderAdminIntegrityEnvelope(stdout, stderr,
			cliresult.NewFailure(command, cliresult.ErrorCLIUsage, cliresult.StageUsage, nil))
	}
	cfg, err := config.Load(config.Overrides{ConfigPath: *configPath, DataDir: *dataDir})
	if err != nil {
		envelope := cliresult.NewFailure(command, classifyHealthcheckConfigError(err), cliresult.StagePreflight, nil)
		envelope.TargetIDs = []string{versionID.String()}
		return renderAdminIntegrityEnvelope(stdout, stderr, envelope)
	}
	execution, operationErr := execute(ctx, cfg, versionID)
	if operationErr != nil {
		code := cliresult.ErrorOperationFailed
		if errors.Is(operationErr, hostlock.ErrLocked) {
			code = cliresult.ErrorAdminLockBusy
		} else if errors.Is(operationErr, storesqlite.ErrSessionPolicyUnavailable) ||
			errors.Is(operationErr, storesqlite.ErrRuntimeConfigUnavailable) {
			code = cliresult.ErrorSourceUnavailable
		}
		envelope := cliresult.NewFailure(command, code, cliresult.StagePreflight, nil)
		envelope.TargetIDs = []string{versionID.String()}
		return renderAdminIntegrityEnvelope(stdout, stderr, envelope)
	}
	result := &cliresult.SessionPolicySelectResult{
		SelectedVersionID: execution.SelectedVersionID.String(),
		ServiceReady:      execution.ServiceReady,
	}
	if execution.PreviousVersionID != nil {
		value := execution.PreviousVersionID.String()
		result.PreviousVersionID = &value
	}
	envelope := cliresult.NewSuccess(command, result)
	if !execution.ServiceReady {
		warning, warningErr := cliresult.NewWarning(cliresult.WarningServiceNotReady)
		if warningErr != nil {
			return cliresult.ExitOperational
		}
		envelope.Warnings = []cliresult.Warning{warning}
	}
	return renderAdminIntegrityEnvelope(stdout, stderr, envelope)
}

func executeSessionPolicySelect(ctx context.Context, cfg config.Config, versionID canonical.ID) (_ sessionPolicySelectExecution, resultErr error) {
	runtime, err := openRuntimeWithOptions(ctx, cfg, nil, false)
	if err != nil {
		return sessionPolicySelectExecution{}, err
	}
	selected := false
	defer func() {
		closeErr := runtime.Close(cfg.Server.ShutdownTimeout)
		if !selected {
			resultErr = errors.Join(resultErr, closeErr)
		}
	}()
	selection, err := runtime.store.SelectSessionPolicy(ctx, versionID)
	if err != nil {
		return sessionPolicySelectExecution{}, err
	}
	selected = true
	execution := sessionPolicySelectExecution{
		PreviousVersionID: selection.PreviousVersionID,
		SelectedVersionID: selection.SelectedVersionID,
	}
	snapshot, err := runtime.store.ServiceReadinessSource().CaptureServiceReadiness(ctx)
	if err != nil {
		return execution, nil
	}
	var checker readiness.ProjectionChecker
	if snapshot.ActiveResidentID != nil && snapshot.ActiveResidentStatus == "active" && snapshot.SessionPolicyID != nil {
		preflight, preflightErr := runtime.projections.PreflightServiceResident(ctx, *snapshot.ActiveResidentID)
		if preflightErr == nil {
			checker = readiness.CanonicalProjectionProof{
				ResidentID: *snapshot.ActiveResidentID, SessionPolicyID: *snapshot.SessionPolicyID,
				Head: preflight.Target.Head, IsCurrent: true,
			}
		} else {
			checker = readiness.ProjectionCheckFunc(func(context.Context, readiness.ProjectionRequirement) (bool, error) {
				return false, nil
			})
		}
	}
	readinessResult, err := readiness.EvaluateServiceReadiness(ctx, runtime.store.ServiceReadinessSource(), readiness.Request{
		StartupComplete: true, Projection: checker,
	})
	if err != nil {
		return execution, nil
	}
	execution.ServiceReady = readinessResult.Ready
	return execution, nil
}

type erasurePlanExecutor func(context.Context, erasureservice.PlanRequest) (erasureservice.ArtifactResult, error)
type erasureDecideExecutor func(context.Context, erasureservice.DecideRequest) (erasureservice.ArtifactResult, error)

func runAdminErasurePlan(ctx context.Context, scope string, arguments []string, stdout, stderr io.Writer) int {
	return runAdminErasurePlanWithExecutor(ctx, scope, arguments, stdout, stderr, erasureservice.CreatePlan)
}

func runAdminErasurePlanWithExecutor(
	ctx context.Context,
	scope string,
	arguments []string,
	stdout, stderr io.Writer,
	execute erasurePlanExecutor,
) int {
	command := cliresult.CommandAdminErasurePlanContent
	if scope == erasure.ScopeResident {
		command = cliresult.CommandAdminErasurePlanResident
	} else if scope != erasure.ScopeContent {
		return renderAdminIntegrityEnvelope(stdout, stderr,
			cliresult.NewFailure(cliresult.CommandUnknown, cliresult.ErrorCLIUsage, cliresult.StageUsage, nil))
	}
	flags := flag.NewFlagSet("admin erasure plan "+scope, flag.ContinueOnError)
	flags.SetOutput(io.Discard)
	residentRaw := flags.String("resident", "", "resident ULID")
	contents := repeatedStringFlag{}
	flags.Var(&contents, "content", "requested content ULID")
	reason := flags.String("reason", "", "erasure reason code")
	output := flags.String("output", "", "absolute plan output file")
	configPath := flags.String("config", "", "TOML configuration path")
	dataDir := flags.String("data-dir", "", "absolute Mahoroba data directory")
	allowedRepeat := map[string]bool{"--content": true}
	if duplicateM7AdminFlag(arguments, allowedRepeat) || flags.Parse(arguments) != nil || flags.NArg() != 0 ||
		*residentRaw == "" || !validErasureReason(*reason) || *output == "" || !filepath.IsAbs(*output) ||
		scope == erasure.ScopeContent && len(contents) == 0 || scope == erasure.ScopeResident && len(contents) != 0 {
		return renderAdminIntegrityEnvelope(stdout, stderr,
			cliresult.NewFailure(command, cliresult.ErrorCLIUsage, cliresult.StageUsage, nil))
	}
	residentID, err := canonical.ParseID(*residentRaw)
	if err != nil {
		return renderAdminIntegrityEnvelope(stdout, stderr,
			cliresult.NewFailure(command, cliresult.ErrorCLIUsage, cliresult.StageUsage, nil))
	}
	contentIDs := make([]canonical.ID, 0, len(contents))
	for _, raw := range contents {
		id, parseErr := canonical.ParseID(raw)
		if parseErr != nil {
			return renderAdminIntegrityEnvelope(stdout, stderr,
				cliresult.NewFailure(command, cliresult.ErrorCLIUsage, cliresult.StageUsage, nil))
		}
		contentIDs = append(contentIDs, id)
	}
	slices.SortFunc(contentIDs, compareCanonicalIDs)
	for index := 1; index < len(contentIDs); index++ {
		if contentIDs[index] == contentIDs[index-1] {
			return renderAdminIntegrityEnvelope(stdout, stderr,
				cliresult.NewFailure(command, cliresult.ErrorCLIUsage, cliresult.StageUsage, nil))
		}
	}
	cfg, err := config.Load(config.Overrides{ConfigPath: *configPath, DataDir: *dataDir})
	if err != nil {
		envelope := cliresult.NewFailure(command, classifyHealthcheckConfigError(err), cliresult.StagePreflight, nil)
		envelope.TargetIDs = []string{residentID.String()}
		return renderAdminIntegrityEnvelope(stdout, stderr, envelope)
	}
	artifactPath := filepath.Clean(*output)
	result, operationErr := execute(ctx, erasureservice.PlanRequest{
		SourceDataDir: cfg.DataDir, DatabaseFilename: cfg.Database.Filename,
		Scope: scope, ResidentID: residentID, RequestedContent: contentIDs,
		ReasonCode: *reason, Output: artifactPath,
	})
	if operationErr != nil {
		return renderErasureArtifactFailure(stdout, stderr, command, artifactPath, residentID, operationErr)
	}
	envelope := cliresult.NewSuccess(command, erasurePlanCLIResult(result))
	warning, warningErr := cliresult.NewWarning(cliresult.WarningRuntimeUnmanagedCopy)
	if warningErr != nil {
		return cliresult.ExitOperational
	}
	envelope.Warnings = []cliresult.Warning{warning}
	return renderAdminIntegrityEnvelope(stdout, stderr, envelope)
}

func runAdminErasureDecide(ctx context.Context, arguments []string, stdout, stderr io.Writer) int {
	return runAdminErasureDecideWithExecutor(ctx, arguments, stdout, stderr, erasureservice.Decide)
}

func runAdminErasureDecideWithExecutor(
	ctx context.Context,
	arguments []string,
	stdout, stderr io.Writer,
	execute erasureDecideExecutor,
) int {
	command := cliresult.CommandAdminErasureDecide
	flags := flag.NewFlagSet("admin erasure decide", flag.ContinueOnError)
	flags.SetOutput(io.Discard)
	planPath := flags.String("plan", "", "absolute input plan file")
	decisionsRaw := repeatedStringFlag{}
	flags.Var(&decisionsRaw, "decision", "IMPACT_ID=retain|erase")
	output := flags.String("output", "", "absolute decided plan output file")
	configPath := flags.String("config", "", "TOML configuration path")
	dataDir := flags.String("data-dir", "", "absolute Mahoroba data directory")
	if duplicateM7AdminFlag(arguments, map[string]bool{"--decision": true}) || flags.Parse(arguments) != nil ||
		flags.NArg() != 0 || *planPath == "" || !filepath.IsAbs(*planPath) || len(decisionsRaw) == 0 ||
		*output == "" || !filepath.IsAbs(*output) {
		return renderAdminIntegrityEnvelope(stdout, stderr,
			cliresult.NewFailure(command, cliresult.ErrorCLIUsage, cliresult.StageUsage, nil))
	}
	decisions := make([]erasure.DecisionInput, 0, len(decisionsRaw))
	for _, raw := range decisionsRaw {
		impactID, decision, ok := strings.Cut(raw, "=")
		if !ok || strings.Contains(decision, "=") || !validWireDigest(impactID) ||
			decision != "retain" && decision != "erase" {
			return renderAdminIntegrityEnvelope(stdout, stderr,
				cliresult.NewFailure(command, cliresult.ErrorCLIUsage, cliresult.StageUsage, nil))
		}
		decisions = append(decisions, erasure.DecisionInput{ImpactID: impactID, Decision: decision})
	}
	slices.SortFunc(decisions, func(left, right erasure.DecisionInput) int {
		return strings.Compare(left.ImpactID, right.ImpactID)
	})
	for index := 1; index < len(decisions); index++ {
		if decisions[index].ImpactID == decisions[index-1].ImpactID {
			return renderAdminIntegrityEnvelope(stdout, stderr,
				cliresult.NewFailure(command, cliresult.ErrorCLIUsage, cliresult.StageUsage, nil))
		}
	}
	cfg, err := config.Load(config.Overrides{ConfigPath: *configPath, DataDir: *dataDir})
	if err != nil {
		return renderAdminIntegrityEnvelope(stdout, stderr, cliresult.NewFailure(
			command, classifyHealthcheckConfigError(err), cliresult.StagePreflight, nil,
		))
	}
	artifactPath := filepath.Clean(*output)
	result, operationErr := execute(ctx, erasureservice.DecideRequest{
		SourceDataDir: cfg.DataDir, DatabaseFilename: cfg.Database.Filename,
		InputPlan: filepath.Clean(*planPath), Decisions: decisions, Output: artifactPath,
	})
	if operationErr != nil {
		return renderErasureArtifactFailure(stdout, stderr, command, artifactPath, canonical.ID{}, operationErr)
	}
	envelope := cliresult.NewSuccess(command, erasurePlanCLIResult(result))
	warning, warningErr := cliresult.NewWarning(cliresult.WarningRuntimeUnmanagedCopy)
	if warningErr != nil {
		return cliresult.ExitOperational
	}
	envelope.Warnings = []cliresult.Warning{warning}
	return renderAdminIntegrityEnvelope(stdout, stderr, envelope)
}

type erasureApplyExecution struct {
	Plan              erasure.Plan
	Result            erasure.ApplyResult
	ProjectionHead    readiness.Head
	Commit            cliresult.CanonicalCommit
	ProjectionCurrent bool
}

type erasureProjectionCoordinator interface {
	Rebuild(context.Context, canonical.ID, projection.Name) error
	RequireSameTargetCohort(context.Context, canonical.ID, canonical.Head) error
}

var errErasureRuntimeCleanup = errors.New("erasure apply: post-commit runtime cleanup failed")

type erasureApplyExecutor func(context.Context, config.Config, string, string) (erasureApplyExecution, error)

func runAdminErasureApply(ctx context.Context, arguments []string, stdout, stderr io.Writer) int {
	return runAdminErasureApplyWithExecutor(ctx, arguments, stdout, stderr, executeErasureApply)
}

func runAdminErasureApplyWithExecutor(
	ctx context.Context,
	arguments []string,
	stdout, stderr io.Writer,
	execute erasureApplyExecutor,
) int {
	command := cliresult.CommandAdminErasureApply
	flags := flag.NewFlagSet("admin erasure apply", flag.ContinueOnError)
	flags.SetOutput(io.Discard)
	planPath := flags.String("plan", "", "absolute ready plan file")
	confirm := flags.String("confirm", "", "exact sha256 plan digest")
	configPath := flags.String("config", "", "TOML configuration path")
	dataDir := flags.String("data-dir", "", "absolute Mahoroba data directory")
	if duplicateM7AdminFlag(arguments, nil) || flags.Parse(arguments) != nil || flags.NArg() != 0 ||
		*planPath == "" || !filepath.IsAbs(*planPath) || !validWireDigest(*confirm) {
		return renderAdminIntegrityEnvelope(stdout, stderr,
			cliresult.NewFailure(command, cliresult.ErrorCLIUsage, cliresult.StageUsage, nil))
	}
	cfg, err := config.Load(config.Overrides{ConfigPath: *configPath, DataDir: *dataDir})
	if err != nil {
		return renderAdminIntegrityEnvelope(stdout, stderr, cliresult.NewFailure(
			command, classifyHealthcheckConfigError(err), cliresult.StagePreflight, nil,
		))
	}
	execution, operationErr := execute(ctx, cfg, filepath.Clean(*planPath), *confirm)
	warnings := erasureApplyWarnings()
	if operationErr == nil {
		envelope := cliresult.NewSuccess(command, erasureApplyCLIResult(execution, cliresult.ProjectionStateCurrent))
		envelope.CanonicalApplied = true
		envelope.CanonicalCommits = []cliresult.CanonicalCommit{execution.Commit}
		envelope.RequiredActions = erasureApplyRequiredActions(execution, *configPath, cfg.DataDir, false)
		envelope.Warnings = warnings
		return renderAdminIntegrityEnvelope(stdout, stderr, envelope)
	}
	code, stage := classifyErasureApplyError(operationErr)
	if execution.Result.CanonicalErasureCommitID != "" {
		projectionState := cliresult.ProjectionStateNotCurrent
		projectionFailed := true
		if execution.ProjectionCurrent {
			projectionState = cliresult.ProjectionStateCurrent
			projectionFailed = false
		}
		envelope := cliresult.NewPartial(command, code, stage,
			erasureApplyCLIResult(execution, projectionState))
		envelope.CanonicalApplied = true
		envelope.CanonicalCommits = []cliresult.CanonicalCommit{execution.Commit}
		envelope.TargetIDs = erasureApplyTargetIDs(execution.Plan)
		envelope.RequiredActions = erasureApplyRequiredActions(execution, *configPath, cfg.DataDir, projectionFailed)
		envelope.Warnings = warnings
		return renderAdminIntegrityEnvelope(stdout, stderr, envelope)
	}
	envelope := cliresult.NewFailure(command, code, stage, nil)
	envelope.TargetIDs = erasureApplyTargetIDs(execution.Plan)
	if code == cliresult.ErrorStaticDesignReopenRequired {
		action, actionErr := cliresult.NewRequiredAction(cliresult.ActionRepairStaticDesign, nil, envelope.TargetIDs, nil)
		if actionErr != nil {
			return cliresult.ExitOperational
		}
		envelope.RequiredActions = []cliresult.RequiredAction{action}
	}
	return renderAdminIntegrityEnvelope(stdout, stderr, envelope)
}

func executeErasureApply(
	ctx context.Context,
	cfg config.Config,
	planPath, confirm string,
) (execution erasureApplyExecution, resultErr error) {
	boundary, err := erasureservice.OpenApplyBoundary(cfg.DataDir, cfg.Database.Filename)
	if err != nil {
		return erasureApplyExecution{}, err
	}
	defer func() {
		closeErr := boundary.Close()
		if closeErr == nil {
			return
		}
		if execution.Result.CanonicalErasureCommitID != "" {
			resultErr = errors.Join(resultErr, errErasureRuntimeCleanup, closeErr)
			return
		}
		resultErr = errors.Join(resultErr, closeErr)
	}()
	// Use the exact canonical root retained by the boundary for every
	// path-based runtime component; never reopen a caller-supplied alias.
	cfg.DataDir = boundary.DataDir()
	runtime, err := openRuntimeWithOptions(ctx, cfg, nil, false)
	if err != nil {
		return erasureApplyExecution{}, err
	}
	defer func() {
		closeErr := runtime.Close(cfg.Server.ShutdownTimeout)
		if closeErr == nil {
			return
		}
		if execution.Result.CanonicalErasureCommitID != "" {
			resultErr = errors.Join(resultErr, errErasureRuntimeCleanup, closeErr)
			return
		}
		resultErr = errors.Join(resultErr, closeErr)
	}()
	if err := boundary.Verify(); err != nil {
		return erasureApplyExecution{}, err
	}
	plan, _, err := erasureservice.ReadPlan(ctx, planPath)
	if err != nil {
		return erasureApplyExecution{}, err
	}
	execution.Plan = plan
	commandResult, err := runtime.writer.Submit(ctx, erasure.ApplyCommand(erasure.ApplyRequest{
		Plan: plan, Confirm: confirm, Blobs: runtime.blobs, Boundary: boundary,
	}))
	if err != nil {
		return execution, err
	}
	applyResult, ok := commandResult.Value.(erasure.ApplyResult)
	if !ok {
		return execution, errors.New("erasure apply returned an invalid closed result")
	}
	execution.Result = applyResult
	execution.Commit = erasureCanonicalCommit(plan, applyResult)
	if err := boundary.Verify(); err != nil {
		return execution, errors.Join(erasure.ErrPlanStale, err)
	}
	snapshot, err := runtime.store.ServiceReadinessSource().CaptureServiceReadiness(ctx)
	if err != nil {
		return execution, errors.Join(projection.ErrProjectionNotCurrent, err)
	}
	execution.ProjectionHead = snapshot.CapturedHead
	if !snapshot.CapturedHead.Exists || snapshot.CapturedHead.CommitID.String() != applyResult.ProjectionTargetHeadID ||
		snapshot.CapturedHead.CommitSeq.String() != applyResult.ProjectionTargetHeadSeq {
		return execution, projection.ErrProjectionNotCurrent
	}
	residentID, _ := canonical.ParseID(plan.ResidentID)
	execution.ProjectionCurrent, err = rebuildErasureProjections(
		ctx,
		runtime.projections,
		residentID,
		plan.Rebuilds,
		snapshot.CapturedHead.Canonical(),
	)
	if err != nil {
		return execution, errors.Join(projection.ErrProjectionNotCurrent, err)
	}
	if err := boundary.Verify(); err != nil {
		return execution, errors.Join(erasure.ErrPlanStale, err)
	}
	return execution, nil
}

func rebuildErasureProjections(
	ctx context.Context,
	coordinator erasureProjectionCoordinator,
	residentID canonical.ID,
	rebuilds []erasure.Rebuild,
	expectedHead canonical.Head,
) (bool, error) {
	seen := make(map[projection.Name]struct{})
	for _, rebuild := range rebuilds {
		name := projection.Name(rebuild.ProjectionName)
		if _, exists := seen[name]; exists {
			continue
		}
		seen[name] = struct{}{}
		if name == projection.ClaimStatesName || name == projection.RuntimeStatesName {
			seen[projection.ClaimStatesName] = struct{}{}
			seen[projection.RuntimeStatesName] = struct{}{}
		}
		if err := coordinator.Rebuild(ctx, residentID, name); err != nil {
			return false, err
		}
	}
	if err := coordinator.RequireSameTargetCohort(ctx, residentID, expectedHead); err != nil {
		return false, err
	}
	return true, nil
}

func erasureCanonicalCommit(plan erasure.Plan, result erasure.ApplyResult) cliresult.CanonicalCommit {
	resident := plan.ResidentID
	effects := []cliresult.EffectCode{cliresult.EffectContentErased}
	if len(plan.ClaimIdentityErasures) != 0 {
		effects = append(effects, cliresult.EffectClaimIdentityErasureRecorded)
	}
	if len(plan.PlannedFindings) != 0 {
		effects = append(effects, cliresult.EffectIntegrityFindingRecorded)
	}
	if len(plan.PlannedQuarantines) != 0 {
		effects = append(effects, cliresult.EffectClaimStatusQuarantined)
	}
	if plan.ResidentTransition != nil {
		effects = append(effects, cliresult.EffectResidentStatusErased)
	}
	if plan.RuntimeConfigEffect.ClearActiveResident {
		effects = append(effects, cliresult.EffectRuntimeSelectionCleared)
	}
	slices.Sort(effects)
	disposition := cliresult.DispositionCreated
	if result.ExistingCommit {
		disposition = cliresult.DispositionExisting
	}
	return cliresult.CanonicalCommit{
		ResidentID: &resident, CommitID: result.CanonicalErasureCommitID,
		CommitSeq: result.CanonicalErasureCommitSeq, Disposition: disposition, Effects: effects,
	}
}

func erasurePlanCLIResult(result erasureservice.ArtifactResult) *cliresult.ErasurePlanResult {
	counts := cliresult.ImpactCounts{Safe: "0", NeedsRebuild: "0", NeedsReview: "0", MustErase: "0"}
	for _, impact := range result.Plan.Impacts {
		switch impact.Classification {
		case "safe":
			counts.Safe = incrementDecimal(counts.Safe)
		case "needs_rebuild":
			counts.NeedsRebuild = incrementDecimal(counts.NeedsRebuild)
		case "needs_review":
			counts.NeedsReview = incrementDecimal(counts.NeedsReview)
		case "must_erase":
			counts.MustErase = incrementDecimal(counts.MustErase)
		}
	}
	return &cliresult.ErasurePlanResult{
		ArtifactPath: result.ArtifactPath, PlanState: result.Plan.PlanState,
		BaseHead: readinessCLIHead(result.BaseHead), Digest: result.Plan.Digest,
		TargetCount: strconv.Itoa(len(result.Plan.EffectiveTargets)), ImpactCounts: counts,
		BlockerCount: strconv.Itoa(len(result.Plan.Blockers)),
	}
}

func erasureApplyCLIResult(execution erasureApplyExecution, state string) *cliresult.ErasureApplyResult {
	return &cliresult.ErasureApplyResult{
		ExistingCommit:             execution.Result.ExistingCommit,
		CanonicalErasureCommitID:   execution.Result.CanonicalErasureCommitID,
		CanonicalErasureCommitSeq:  execution.Result.CanonicalErasureCommitSeq,
		ProjectionTargetHead:       readinessCLIHead(execution.ProjectionHead),
		TargetCount:                strconv.Itoa(len(execution.Plan.EffectiveTargets)),
		MandatoryWorkFollowupCount: strconv.Itoa(execution.Result.MandatoryWorkFollowupCount),
		ProjectionState:            state,
	}
}

func readinessCLIHead(head readiness.Head) cliresult.Head {
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

func renderErasureArtifactFailure(
	stdout, stderr io.Writer,
	command cliresult.CommandID,
	artifactPath string,
	residentID canonical.ID,
	err error,
) int {
	code, stage := classifyErasureArtifactError(command, err)
	result := &cliresult.ErrorResult{ErrorStage: stage}
	if code == cliresult.ErrorArtifactTargetExists || code == cliresult.ErrorArtifactIOFailed ||
		code == cliresult.ErrorPublishDurabilityUnknown {
		result.ArtifactPath = &artifactPath
	}
	if code == cliresult.ErrorPublishDurabilityUnknown {
		published := false
		result.Published = &published
	}
	envelope := cliresult.NewFailure(command, code, stage, result)
	if !residentID.IsZero() {
		envelope.TargetIDs = []string{residentID.String()}
	}
	if code == cliresult.ErrorPublishDurabilityUnknown {
		action, actionErr := cliresult.NewRequiredAction(cliresult.ActionRepairStaticDesign, nil, nil, nil)
		if actionErr != nil {
			return cliresult.ExitOperational
		}
		envelope.RequiredActions = []cliresult.RequiredAction{action}
	}
	return renderAdminIntegrityEnvelope(stdout, stderr, envelope)
}

func classifyErasureArtifactError(command cliresult.CommandID, err error) (cliresult.ErrorCode, cliresult.Stage) {
	switch {
	case errors.Is(err, hostlock.ErrLocked):
		return cliresult.ErrorAdminLockBusy, cliresult.StagePreflight
	case errors.Is(err, erasureservice.ErrArtifactTargetExists):
		return cliresult.ErrorArtifactTargetExists, cliresult.StagePreflight
	case errors.Is(err, erasureservice.ErrDurabilityUnknown):
		return cliresult.ErrorPublishDurabilityUnknown, cliresult.StagePublish
	case errors.Is(err, integrity.ErrFatal):
		return cliresult.ErrorIntegrityFatal, cliresult.StagePreflight
	case errors.Is(err, erasureservice.ErrArtifactIO):
		return cliresult.ErrorArtifactIOFailed, cliresult.StagePublish
	case errors.Is(err, erasure.ErrPlanStale):
		if command == cliresult.CommandAdminErasureDecide {
			return cliresult.ErrorErasurePlanStale, cliresult.StagePreflight
		}
		return cliresult.ErrorOperationFailed, cliresult.StagePreflight
	case errors.Is(err, erasure.ErrReviewRequired):
		return cliresult.ErrorErasureReviewRequired, cliresult.StagePreflight
	case errors.Is(err, erasure.ErrInvalidPlan), errors.Is(err, erasure.ErrPlanDigestMismatch):
		if command == cliresult.CommandAdminErasureDecide {
			return cliresult.ErrorErasureIneligible, cliresult.StagePreflight
		}
		return cliresult.ErrorOperationFailed, cliresult.StagePreflight
	case errors.Is(err, erasureservice.ErrSourceUnavailable):
		return cliresult.ErrorSourceUnavailable, cliresult.StagePreflight
	default:
		return cliresult.ErrorOperationFailed, cliresult.StagePreflight
	}
}

func classifyErasureApplyError(err error) (cliresult.ErrorCode, cliresult.Stage) {
	switch {
	case errors.Is(err, hostlock.ErrLocked):
		return cliresult.ErrorAdminLockBusy, cliresult.StagePreflight
	case errors.Is(err, integrity.ErrFatal):
		return cliresult.ErrorIntegrityFatal, cliresult.StagePreflight
	case errors.Is(err, erasure.ErrReviewRequired):
		return cliresult.ErrorErasureReviewRequired, cliresult.StagePreflight
	case errors.Is(err, erasure.ErrPlanStale), errors.Is(err, erasure.ErrSafetyBusy):
		return cliresult.ErrorErasurePlanStale, cliresult.StageCanonical
	case errors.Is(err, erasure.ErrRetryConflict):
		return cliresult.ErrorErasureRetryConflict, cliresult.StageCanonical
	case errors.Is(err, erasure.ErrIntegrityPipelineRequired):
		return cliresult.ErrorIntegrityPipelineRequired, cliresult.StageCanonical
	case errors.Is(err, erasure.ErrDesignReopen):
		return cliresult.ErrorStaticDesignReopenRequired, cliresult.StagePreflight
	case errors.Is(err, erasure.ErrInvalidPlan), errors.Is(err, erasure.ErrPlanDigestMismatch),
		errors.Is(err, erasure.ErrBlocked), errors.Is(err, erasure.ErrOwnerHumanRequired):
		return cliresult.ErrorErasureIneligible, cliresult.StagePreflight
	case errors.Is(err, projection.ErrProjectionNotCurrent):
		return cliresult.ErrorProjectionNotCurrent, cliresult.StageDerived
	case errors.Is(err, errErasureRuntimeCleanup):
		return cliresult.ErrorOperationFailed, cliresult.StageDerived
	case errors.Is(err, erasureservice.ErrSourceUnavailable):
		return cliresult.ErrorSourceUnavailable, cliresult.StagePreflight
	default:
		return cliresult.ErrorOperationFailed, cliresult.StageCanonical
	}
}

func erasureApplyWarnings() []cliresult.Warning {
	warning, err := cliresult.NewWarning(cliresult.WarningPhysicalErasureNotGuaranteed)
	if err != nil {
		return nil
	}
	return []cliresult.Warning{warning}
}

func erasureApplyTargetIDs(plan erasure.Plan) []string {
	if plan.ResidentID == "" {
		return []string{}
	}
	result := make([]string, 0, len(plan.EffectiveTargets)+1)
	result = append(result, plan.ResidentID)
	for _, target := range plan.EffectiveTargets {
		result = append(result, target.ContentID)
	}
	slices.Sort(result)
	result = slices.Compact(result)
	return result
}

func erasureApplyRequiredActions(execution erasureApplyExecution, configPath, dataDir string, projectionFailed bool) []cliresult.RequiredAction {
	actions := []cliresult.RequiredAction{}
	residentID := execution.Plan.ResidentID
	if execution.Result.MandatoryWorkFollowupCount > 0 {
		argv := []string{"mahoroba", "admin", "recovery", "terminalize"}
		argv = append(argv, sourceArgv(configPath, dataDir)...)
		action, err := cliresult.NewRequiredAction(
			cliresult.ActionRunRecoveryTerminalize, argv, []string{residentID}, nil,
		)
		if err == nil {
			actions = append(actions, action)
		}
	}
	if projectionFailed {
		seen := map[string]struct{}{}
		for _, rebuild := range execution.Plan.Rebuilds {
			key := rebuild.ResidentID + "\x00" + rebuild.ProjectionName
			if _, exists := seen[key]; exists {
				continue
			}
			seen[key] = struct{}{}
			argv := []string{"mahoroba", "projection", "rebuild", "--resident", rebuild.ResidentID, "--name", rebuild.ProjectionName}
			argv = append(argv, sourceArgv(configPath, dataDir)...)
			action, err := cliresult.NewRequiredAction(
				cliresult.ActionRebuildProjection, argv, []string{rebuild.ResidentID}, nil,
			)
			if err == nil {
				actions = append(actions, action)
			}
		}
	}
	slices.SortFunc(actions, func(left, right cliresult.RequiredAction) int {
		if compared := strings.Compare(string(left.Code), string(right.Code)); compared != 0 {
			return compared
		}
		return strings.Compare(left.ActionID, right.ActionID)
	})
	return actions
}

func sourceArgv(configPath, dataDir string) []string {
	result := []string{}
	if configPath != "" {
		path, err := filepath.Abs(filepath.Clean(configPath))
		if err == nil {
			if resolved, resolveErr := filepath.EvalSymlinks(path); resolveErr == nil {
				path = resolved
			}
			result = append(result, "--config", filepath.Clean(path))
		}
	}
	result = append(result, "--data-dir", filepath.Clean(dataDir))
	return result
}

func duplicateM7AdminFlag(arguments []string, allowedRepeat map[string]bool) bool {
	seen := map[string]bool{}
	for _, argument := range arguments {
		if argument == "--" {
			break
		}
		if !strings.HasPrefix(argument, "--") {
			continue
		}
		name, _, _ := strings.Cut(argument, "=")
		if seen[name] && !allowedRepeat[name] {
			return true
		}
		seen[name] = true
	}
	return false
}

func validWireDigest(value string) bool {
	if len(value) != len("sha256:")+64 || !strings.HasPrefix(value, "sha256:") {
		return false
	}
	_, err := canonical.ParseDigestHex(strings.TrimPrefix(value, "sha256:"))
	return err == nil
}

func validErasureReason(value string) bool {
	if value == "" || len(value) > 64 || value[0] < 'a' || value[0] > 'z' {
		return false
	}
	for _, character := range value[1:] {
		if character != '_' && (character < 'a' || character > 'z') && (character < '0' || character > '9') {
			return false
		}
	}
	return true
}

func compareCanonicalIDs(left, right canonical.ID) int {
	return strings.Compare(left.String(), right.String())
}

func incrementDecimal(value string) string {
	parsed, _ := strconv.ParseUint(value, 10, 64)
	return strconv.FormatUint(parsed+1, 10)
}
