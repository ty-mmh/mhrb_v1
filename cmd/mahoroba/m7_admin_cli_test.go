package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"

	"mahoroba.local/mahoroba/internal/app"
	"mahoroba.local/mahoroba/internal/canonical"
	"mahoroba.local/mahoroba/internal/cliresult"
	"mahoroba.local/mahoroba/internal/config"
	"mahoroba.local/mahoroba/internal/domain"
	"mahoroba.local/mahoroba/internal/erasure"
	erasureservice "mahoroba.local/mahoroba/internal/erasure/service"
	"mahoroba.local/mahoroba/internal/httpui"
	"mahoroba.local/mahoroba/internal/projection"
	"mahoroba.local/mahoroba/internal/readiness"
	store "mahoroba.local/mahoroba/internal/store/sqlite"
)

func TestM7AdminSessionPolicySelectExactEnvelopeAndDuplicateFlagRejection(t *testing.T) {
	version := mustM7AdminCLI_ID(t, "01J00000000000000000000101")
	previous := mustM7AdminCLI_ID(t, "01J00000000000000000000102")
	dataDir := t.TempDir()
	called := 0
	executor := func(_ context.Context, cfg config.Config, got canonical.ID) (sessionPolicySelectExecution, error) {
		called++
		if cfg.DataDir != dataDir || got != version {
			t.Fatalf("request = %s / %s", cfg.DataDir, got)
		}
		return sessionPolicySelectExecution{PreviousVersionID: &previous, SelectedVersionID: version, ServiceReady: false}, nil
	}
	var stdout, stderr bytes.Buffer
	exit := runAdminSessionPolicySelectWithExecutor(context.Background(), []string{
		"--version", version.String(), "--data-dir", dataDir,
	}, &stdout, &stderr, executor)
	if exit != cliresult.ExitSuccess || called != 1 {
		t.Fatalf("exit=%d called=%d stderr=%s", exit, called, stderr.String())
	}
	envelope := decodeM7AdminCLI(t, stdout.Bytes())
	result := envelope["result"].(map[string]any)
	if envelope["command"] != string(cliresult.CommandAdminSessionPolicySelect) ||
		result["previous_version_id"] != previous.String() || result["selected_version_id"] != version.String() ||
		result["service_ready"] != false || !bytes.Contains(stderr.Bytes(), []byte(`"warning_code":"service_not_ready"`)) {
		t.Fatalf("envelope=%#v stderr=%s", envelope, stderr.String())
	}
	stdout.Reset()
	stderr.Reset()
	exit = runAdminSessionPolicySelectWithExecutor(context.Background(), []string{
		"--version", version.String(), "--version=" + version.String(), "--data-dir", dataDir,
	}, &stdout, &stderr, executor)
	if exit != cliresult.ExitUsage || called != 1 {
		t.Fatalf("duplicate flag exit=%d called=%d stderr=%s", exit, called, stderr.String())
	}
}

type erasureProjectionCoordinatorFake struct {
	rebuilds          []projection.Name
	rebuildErr        error
	requiredResident  canonical.ID
	requiredHead      canonical.Head
	requireExactCalls int
	requireExactErr   error
}

func (fake *erasureProjectionCoordinatorFake) Rebuild(_ context.Context, _ canonical.ID, name projection.Name) error {
	fake.rebuilds = append(fake.rebuilds, name)
	return fake.rebuildErr
}

func (fake *erasureProjectionCoordinatorFake) RequireSameTargetCohort(
	_ context.Context,
	residentID canonical.ID,
	expectedHead canonical.Head,
) error {
	fake.requiredResident = residentID
	fake.requiredHead = expectedHead
	fake.requireExactCalls++
	return fake.requireExactErr
}

func TestCOV56ErasureProjectionCurrentRequiresExactCohort(t *testing.T) {
	residentID := mustM7AdminCLI_ID(t, "01J00000000000000000000111")
	expectedHead := canonical.Head{Exists: true, CommitSeq: 12, CommittedAt: 34}
	rebuilds := []erasure.Rebuild{
		{ResidentID: residentID.String(), ProjectionName: string(projection.ClaimStatesName), ProjectionVersion: "claim-states-v1", ReasonCode: "content_erasure"},
		{ResidentID: residentID.String(), ProjectionName: string(projection.ClaimStatesName), ProjectionVersion: "claim-states-v1", ReasonCode: "claim_state_change"},
		{ResidentID: residentID.String(), ProjectionName: string(projection.RuntimeStatesName), ProjectionVersion: "runtime-states-v2", ReasonCode: "content_erasure"},
		{ResidentID: residentID.String(), ProjectionName: string(projection.ContentReferencesName), ProjectionVersion: "content-references-v1", ReasonCode: "content_erasure"},
	}

	fake := &erasureProjectionCoordinatorFake{}
	current, err := rebuildErasureProjections(context.Background(), fake, residentID, rebuilds, expectedHead)
	if err != nil || !current {
		t.Fatalf("rebuildErasureProjections current/error = %v/%v", current, err)
	}
	if len(fake.rebuilds) != 2 || fake.rebuilds[0] != projection.ClaimStatesName ||
		fake.rebuilds[1] != projection.ContentReferencesName {
		t.Fatalf("deduplicated rebuilds = %v, want one cohort rebuild plus content references", fake.rebuilds)
	}
	if fake.requireExactCalls != 1 || fake.requiredResident != residentID || fake.requiredHead != expectedHead {
		t.Fatalf("exact cohort verification = calls:%d resident:%s head:%+v", fake.requireExactCalls, fake.requiredResident, fake.requiredHead)
	}

	fake = &erasureProjectionCoordinatorFake{requireExactErr: projection.ErrProjectionNotCurrent}
	current, err = rebuildErasureProjections(context.Background(), fake, residentID, rebuilds, expectedHead)
	if current || !errors.Is(err, projection.ErrProjectionNotCurrent) {
		t.Fatalf("skewed cohort publication = current:%v error:%v, want fail closed", current, err)
	}
}

func TestM7AdminSessionPolicySelectProductionIntegration(t *testing.T) {
	ctx := context.Background()
	dataDir := managedCommandDataDir(t)
	cfg := commandRuntimeConfig(dataDir)
	runtime, err := openRuntime(ctx, cfg, nil)
	if err != nil {
		t.Fatal(err)
	}
	state, err := runtime.app.BootstrapInit(ctx, app.BootstrapInput{
		OwnerName: "Owner", Name: "Resident", SeedKey: "m7-session-policy-cli", Principles: "be durable",
	})
	if err != nil {
		t.Fatal(err)
	}
	residentID := state.Residents[0].ResidentID
	if err := runtime.app.ApprovePrinciples(ctx, residentID); err != nil {
		t.Fatal(err)
	}
	if err := runtime.app.FinalizeBootstrap(ctx, residentID, "focused", defaultMemory); err != nil {
		t.Fatal(err)
	}
	if err := runtime.app.SelectResident(ctx, residentID); err != nil {
		t.Fatal(err)
	}
	if err := runtime.Close(cfg.Server.ShutdownTimeout); err != nil {
		t.Fatal(err)
	}

	var stdout, stderr bytes.Buffer
	exit := run(ctx, []string{
		"admin", "runtime", "session-policy", "select",
		"--version", state.SessionPolicyID.String(), "--config", writeCommandConfig(t, dataDir),
	}, &stdout, &stderr)
	if exit != cliresult.ExitSuccess {
		t.Fatalf("production select exit=%d stdout=%s stderr=%s", exit, stdout.String(), stderr.String())
	}
	envelope := decodeM7AdminCLI(t, stdout.Bytes())
	result := envelope["result"].(map[string]any)
	if envelope["command"] != string(cliresult.CommandAdminSessionPolicySelect) ||
		result["selected_version_id"] != state.SessionPolicyID.String() {
		t.Fatalf("production select envelope=%#v", envelope)
	}

	inspection, err := store.OpenInspection(ctx, cfg.DatabasePath())
	if err != nil {
		t.Fatal(err)
	}
	defer inspection.Close()
	snapshot, err := inspection.ServiceReadinessSource().CaptureServiceReadiness(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if snapshot.SessionPolicyID == nil || *snapshot.SessionPolicyID != state.SessionPolicyID {
		t.Fatalf("persisted session policy=%v want=%s", snapshot.SessionPolicyID, state.SessionPolicyID)
	}
}

func TestM7AdminErasurePlanContentExactEnvelopeAndCanonicalizedRoots(t *testing.T) {
	resident := mustM7AdminCLI_ID(t, "01J00000000000000000000110")
	contentA := mustM7AdminCLI_ID(t, "01J00000000000000000000111")
	contentB := mustM7AdminCLI_ID(t, "01J00000000000000000000112")
	head := m7AdminCLIHead(t)
	output := filepath.Join(t.TempDir(), "plan.json")
	dataDir := t.TempDir()
	called := 0
	executor := func(_ context.Context, request erasureservice.PlanRequest) (erasureservice.ArtifactResult, error) {
		called++
		if request.Scope != erasure.ScopeContent || request.ResidentID != resident || request.ReasonCode != "privacy_request" ||
			len(request.RequestedContent) != 2 || request.RequestedContent[0] != contentA || request.RequestedContent[1] != contentB {
			t.Fatalf("request = %#v", request)
		}
		return erasureservice.ArtifactResult{
			ArtifactPath: output, BaseHead: head,
			Plan: erasure.Plan{
				PlanState: erasure.StateReady, Digest: "sha256:" + string(bytes.Repeat([]byte{'a'}, 64)),
				EffectiveTargets: []erasure.EffectiveTarget{{ContentID: contentA.String()}},
				Impacts:          []erasure.Impact{{Classification: "safe"}, {Classification: "needs_rebuild"}, {Classification: "needs_review"}, {Classification: "must_erase"}},
				Blockers:         []erasure.Blocker{},
			},
		}, nil
	}
	var stdout, stderr bytes.Buffer
	exit := runAdminErasurePlanWithExecutor(context.Background(), erasure.ScopeContent, []string{
		"--resident", resident.String(), "--content", contentB.String(), "--content", contentA.String(),
		"--reason", "privacy_request", "--output", output, "--data-dir", dataDir,
	}, &stdout, &stderr, executor)
	if exit != cliresult.ExitSuccess || called != 1 {
		t.Fatalf("exit=%d called=%d stderr=%s", exit, called, stderr.String())
	}
	envelope := decodeM7AdminCLI(t, stdout.Bytes())
	result := envelope["result"].(map[string]any)
	counts := result["impact_counts"].(map[string]any)
	if envelope["command"] != string(cliresult.CommandAdminErasurePlanContent) || result["artifact_path"] != output ||
		result["target_count"] != "1" || counts["safe"] != "1" || counts["needs_rebuild"] != "1" ||
		counts["needs_review"] != "1" || counts["must_erase"] != "1" ||
		!bytes.Contains(stderr.Bytes(), []byte(`"warning_code":"runtime_unmanaged_copy"`)) {
		t.Fatalf("envelope=%#v stderr=%s", envelope, stderr.String())
	}
	stdout.Reset()
	stderr.Reset()
	exit = runAdminErasurePlanWithExecutor(context.Background(), erasure.ScopeContent, []string{
		"--resident", resident.String(), "--content", contentA.String(), "--reason", "privacy_request",
		"--output", output, "--output=" + output, "--data-dir", dataDir,
	}, &stdout, &stderr, executor)
	if exit != cliresult.ExitUsage || called != 1 {
		t.Fatalf("duplicate output exit=%d called=%d stderr=%s", exit, called, stderr.String())
	}
}

func TestM7AdminErasurePlanContentProductionIntegration(t *testing.T) {
	ctx := context.Background()
	dataDir := managedCommandDataDir(t)
	residentRaw := bootstrapProjectionCommandDatabase(t, dataDir)
	residentID := mustM7AdminCLI_ID(t, residentRaw)
	cfg := commandRuntimeConfig(dataDir)
	configPath := writeCommandConfig(t, dataDir)
	runtime, err := openRuntimeWithOptions(ctx, cfg, memoryCLIExtractionGenerator{}, false)
	if err != nil {
		t.Fatal(err)
	}
	event, err := runtime.app.Ingress(ctx, "erase this independent content")
	if err != nil {
		_ = runtime.Close(cfg.Server.ShutdownTimeout)
		t.Fatal(err)
	}
	contentID := event.ContentID
	if err := runtime.app.ProcessResident(ctx, residentID); err != nil {
		_ = runtime.Close(cfg.Server.ShutdownTimeout)
		t.Fatal(err)
	}
	if err := runtime.Close(cfg.Server.ShutdownTimeout); err != nil {
		t.Fatal(err)
	}
	var scanStdout, scanStderr bytes.Buffer
	if exit := run(ctx, []string{
		"admin", "integrity", "scan", "--all", "--config", configPath,
	}, &scanStdout, &scanStderr); exit != cliresult.ExitSuccess {
		t.Fatalf("integrity foundation exit=%d stdout=%s stderr=%s", exit, scanStdout.String(), scanStderr.String())
	}
	output := filepath.Join(managedCommandDataDir(t), "content-plan.json")
	var stdout, stderr bytes.Buffer
	exit := run(ctx, []string{
		"admin", "erasure", "plan", "content",
		"--resident", residentID.String(), "--content", contentID.String(),
		"--reason", "privacy_request", "--output", output,
		"--config", configPath,
	}, &stdout, &stderr)
	if exit != cliresult.ExitSuccess {
		t.Fatalf("production plan exit=%d stdout=%s stderr=%s", exit, stdout.String(), stderr.String())
	}
	envelope := decodeM7AdminCLI(t, stdout.Bytes())
	result := envelope["result"].(map[string]any)
	if envelope["command"] != string(cliresult.CommandAdminErasurePlanContent) ||
		result["artifact_path"] != output {
		t.Fatalf("production plan envelope=%#v", envelope)
	}
	body, err := os.ReadFile(output)
	if err != nil {
		t.Fatal(err)
	}
	plan, err := erasure.ParsePlan(body)
	if err != nil {
		t.Fatal(err)
	}
	if plan.Digest != result["digest"] || plan.ResidentID != residentID.String() ||
		len(plan.RequestedContentIDs) != 1 || plan.RequestedContentIDs[0] != contentID.String() {
		t.Fatalf("published plan/result mismatch: plan=%#v result=%#v", plan, result)
	}

	readyPath, readyPlan := output, plan
	if plan.PlanState == erasure.StateReviewRequired {
		decidedOutput := filepath.Join(managedCommandDataDir(t), "content-plan-decided.json")
		arguments := []string{
			"admin", "erasure", "decide", "--plan", output, "--output", decidedOutput,
			"--config", configPath,
		}
		for _, impact := range plan.Impacts {
			if impact.DecisionMode != "none" {
				arguments = append(arguments, "--decision", impact.ImpactID+"=retain")
			}
		}
		stdout.Reset()
		stderr.Reset()
		if exit := run(ctx, arguments, &stdout, &stderr); exit != cliresult.ExitSuccess {
			t.Fatalf("production decide exit=%d stdout=%s stderr=%s", exit, stdout.String(), stderr.String())
		}
		decidedBody, err := os.ReadFile(decidedOutput)
		if err != nil {
			t.Fatal(err)
		}
		readyPlan, err = erasure.ParsePlan(decidedBody)
		if err != nil {
			t.Fatal(err)
		}
		readyPath = decidedOutput
	}
	if readyPlan.PlanState != erasure.StateReady {
		t.Fatalf("production apply plan state=%s blockers=%+v", readyPlan.PlanState, readyPlan.Blockers)
	}

	stdout.Reset()
	stderr.Reset()
	applyArguments := []string{
		"admin", "erasure", "apply", "--plan", readyPath,
		"--confirm", readyPlan.Digest, "--config", configPath,
	}
	if exit := run(ctx, applyArguments, &stdout, &stderr); exit != cliresult.ExitSuccess {
		t.Fatalf("production apply exit=%d stdout=%s stderr=%s", exit, stdout.String(), stderr.String())
	}
	applyEnvelope := decodeM7AdminCLI(t, stdout.Bytes())
	applyResult := applyEnvelope["result"].(map[string]any)
	if applyEnvelope["command"] != string(cliresult.CommandAdminErasureApply) ||
		applyEnvelope["canonical_applied"] != true || applyResult["existing_commit"] != false ||
		applyResult["projection_state"] != cliresult.ProjectionStateCurrent {
		t.Fatalf("production apply envelope=%#v", applyEnvelope)
	}

	inspection, err := store.OpenInspection(ctx, cfg.DatabasePath())
	if err != nil {
		t.Fatal(err)
	}
	erased := false
	if err := inspection.Canonical().WalkResidentContents(ctx, residentID, func(content canonical.LedgerContent) error {
		if content.ID == contentID {
			erased = content.Found && content.ErasureState == canonical.ContentErasureErased
		}
		return nil
	}); err != nil {
		_ = inspection.Close()
		t.Fatal(err)
	}
	if err := inspection.Close(); err != nil {
		t.Fatal(err)
	}
	if !erased {
		t.Fatal("production apply did not erase the requested content")
	}

	// Exact retry consumes the same ready plan and must surface the existing
	// Canonical erasure commit without creating a second identity.
	stdout.Reset()
	stderr.Reset()
	if exit := run(ctx, applyArguments, &stdout, &stderr); exit != cliresult.ExitSuccess {
		t.Fatalf("production apply retry exit=%d stdout=%s stderr=%s", exit, stdout.String(), stderr.String())
	}
	retryEnvelope := decodeM7AdminCLI(t, stdout.Bytes())
	retryResult := retryEnvelope["result"].(map[string]any)
	if retryResult["existing_commit"] != true ||
		retryEnvelope["canonical_commits"].([]any)[0].(map[string]any)["disposition"] !=
			string(cliresult.DispositionExisting) {
		t.Fatalf("production apply retry envelope=%#v", retryEnvelope)
	}
}

func TestM7AdminErasureDecideRejectsDuplicateImpactAndSortsExactDecisions(t *testing.T) {
	impactA := "sha256:" + string(bytes.Repeat([]byte{'1'}, 64))
	impactB := "sha256:" + string(bytes.Repeat([]byte{'2'}, 64))
	input := filepath.Join(t.TempDir(), "input.json")
	output := filepath.Join(t.TempDir(), "decided.json")
	dataDir := t.TempDir()
	called := 0
	executor := func(_ context.Context, request erasureservice.DecideRequest) (erasureservice.ArtifactResult, error) {
		called++
		if len(request.Decisions) != 2 || request.Decisions[0].ImpactID != impactA || request.Decisions[1].ImpactID != impactB {
			t.Fatalf("decisions = %#v", request.Decisions)
		}
		return erasureservice.ArtifactResult{
			ArtifactPath: output, BaseHead: m7AdminCLIHead(t),
			Plan: erasure.Plan{PlanState: erasure.StateReady, Digest: "sha256:" + string(bytes.Repeat([]byte{'3'}, 64)),
				EffectiveTargets: []erasure.EffectiveTarget{}, Impacts: []erasure.Impact{}, Blockers: []erasure.Blocker{}},
		}, nil
	}
	var stdout, stderr bytes.Buffer
	exit := runAdminErasureDecideWithExecutor(context.Background(), []string{
		"--plan", input, "--decision", impactB + "=retain", "--decision", impactA + "=erase",
		"--output", output, "--data-dir", dataDir,
	}, &stdout, &stderr, executor)
	if exit != cliresult.ExitSuccess || called != 1 {
		t.Fatalf("exit=%d called=%d stderr=%s", exit, called, stderr.String())
	}
	stdout.Reset()
	stderr.Reset()
	exit = runAdminErasureDecideWithExecutor(context.Background(), []string{
		"--plan", input, "--decision", impactA + "=retain", "--decision", impactA + "=erase",
		"--output", output, "--data-dir", dataDir,
	}, &stdout, &stderr, executor)
	if exit != cliresult.ExitUsage || called != 1 {
		t.Fatalf("duplicate decision exit=%d called=%d stderr=%s", exit, called, stderr.String())
	}
}

func TestM7AdminErasureApplyRendersCreatedExistingAndProjectionPartial(t *testing.T) {
	resident := mustM7AdminCLI_ID(t, "01J00000000000000000000120")
	content := mustM7AdminCLI_ID(t, "01J00000000000000000000121")
	commit := mustM7AdminCLI_ID(t, "01J00000000000000000000122")
	planPath := filepath.Join(t.TempDir(), "ready.json")
	dataDir := t.TempDir()
	digest := "sha256:" + string(bytes.Repeat([]byte{'4'}, 64))
	base := erasureApplyExecution{
		Plan: erasure.Plan{ResidentID: resident.String(), EffectiveTargets: []erasure.EffectiveTarget{{ContentID: content.String()}},
			Rebuilds: []erasure.Rebuild{{ResidentID: resident.String(), ProjectionName: "claim_states", ProjectionVersion: "claim-states-v1"}}},
		Result: erasure.ApplyResult{CanonicalErasureCommitID: commit.String(), CanonicalErasureCommitSeq: "7",
			ProjectionTargetHeadID: commit.String(), ProjectionTargetHeadSeq: "7", MandatoryWorkFollowupCount: 1},
		ProjectionHead: m7AdminCLIHead(t),
		Commit: cliresult.CanonicalCommit{ResidentID: m7AdminCLIStringPtr(resident.String()), CommitID: commit.String(),
			CommitSeq: "7", Disposition: cliresult.DispositionCreated, Effects: []cliresult.EffectCode{cliresult.EffectContentErased}},
	}
	base.ProjectionHead.CommitID = commit
	base.ProjectionHead.CommitSeq = 7
	var stdout, stderr bytes.Buffer
	exit := runAdminErasureApplyWithExecutor(context.Background(), []string{
		"--plan", planPath, "--confirm", digest, "--data-dir", dataDir,
	}, &stdout, &stderr, func(context.Context, config.Config, string, string) (erasureApplyExecution, error) {
		return base, nil
	})
	if exit != cliresult.ExitSuccess {
		t.Fatalf("success exit=%d stdout=%s stderr=%s", exit, stdout.String(), stderr.String())
	}
	envelope := decodeM7AdminCLI(t, stdout.Bytes())
	result := envelope["result"].(map[string]any)
	if envelope["canonical_applied"] != true || result["projection_state"] != cliresult.ProjectionStateCurrent ||
		result["mandatory_work_followup_count"] != "1" ||
		!bytes.Contains(stderr.Bytes(), []byte(`"warning_code":"physical_erasure_not_guaranteed"`)) {
		t.Fatalf("success envelope=%#v stderr=%s", envelope, stderr.String())
	}
	stdout.Reset()
	stderr.Reset()
	partial := base
	partial.Result.ExistingCommit = true
	partial.Commit.Disposition = cliresult.DispositionExisting
	exit = runAdminErasureApplyWithExecutor(context.Background(), []string{
		"--plan", planPath, "--confirm", digest, "--data-dir", dataDir,
	}, &stdout, &stderr, func(context.Context, config.Config, string, string) (erasureApplyExecution, error) {
		return partial, projection.ErrProjectionNotCurrent
	})
	if exit != cliresult.ExitOperational {
		t.Fatalf("partial exit=%d stdout=%s stderr=%s", exit, stdout.String(), stderr.String())
	}
	envelope = decodeM7AdminCLI(t, stdout.Bytes())
	result = envelope["result"].(map[string]any)
	if envelope["outcome"] != string(cliresult.OutcomePartial) || envelope["error_code"] != string(cliresult.ErrorOperationPartial) ||
		result["existing_commit"] != true || result["projection_state"] != cliresult.ProjectionStateNotCurrent ||
		!bytes.Contains(stderr.Bytes(), []byte(`"error_code":"projection_not_current"`)) {
		t.Fatalf("partial envelope=%#v stderr=%s", envelope, stderr.String())
	}

	stdout.Reset()
	stderr.Reset()
	cleanupPartial := base
	cleanupPartial.ProjectionCurrent = true
	exit = runAdminErasureApplyWithExecutor(context.Background(), []string{
		"--plan", planPath, "--confirm", digest, "--data-dir", dataDir,
	}, &stdout, &stderr, func(context.Context, config.Config, string, string) (erasureApplyExecution, error) {
		return cleanupPartial, errErasureRuntimeCleanup
	})
	if exit != cliresult.ExitOperational {
		t.Fatalf("cleanup partial exit=%d stdout=%s stderr=%s", exit, stdout.String(), stderr.String())
	}
	envelope = decodeM7AdminCLI(t, stdout.Bytes())
	result = envelope["result"].(map[string]any)
	if result["projection_state"] != cliresult.ProjectionStateCurrent ||
		!bytes.Contains(stderr.Bytes(), []byte(`"error_code":"operation_failed"`)) ||
		bytes.Contains(stdout.Bytes(), []byte(`"code":"rebuild_projection"`)) {
		t.Fatalf("cleanup partial envelope=%#v stderr=%s", envelope, stderr.String())
	}
}

func TestM7PublicErasureAndSessionPolicyRoutesAreRecognized(t *testing.T) {
	tests := []struct {
		arguments []string
		command   cliresult.CommandID
	}{
		{[]string{"admin", "runtime", "session-policy", "select", "--version", "bad"}, cliresult.CommandAdminSessionPolicySelect},
		{[]string{"admin", "erasure", "plan", "content"}, cliresult.CommandAdminErasurePlanContent},
		{[]string{"admin", "erasure", "plan", "resident"}, cliresult.CommandAdminErasurePlanResident},
		{[]string{"admin", "erasure", "decide"}, cliresult.CommandAdminErasureDecide},
		{[]string{"admin", "erasure", "apply"}, cliresult.CommandAdminErasureApply},
	}
	for _, test := range tests {
		var stdout, stderr bytes.Buffer
		if exit := run(context.Background(), test.arguments, &stdout, &stderr); exit != cliresult.ExitUsage {
			t.Fatalf("%v exit=%d stderr=%s", test.arguments, exit, stderr.String())
		}
		envelope := decodeM7AdminCLI(t, stderr.Bytes())
		if envelope["command"] != string(test.command) || envelope["error_code"] != string(cliresult.ErrorCLIUsage) {
			t.Fatalf("%v envelope=%#v", test.arguments, envelope)
		}
	}
}

func TestM7RTI28BranchSurfaceAbsentBlackBox(t *testing.T) {
	commands := [][]string{
		{"branch"},
		{"admin", "branch", "create"},
		{"admin", "resident", "branch", "--resident", "01J00000000000000000000130"},
	}
	for _, arguments := range commands {
		var stdout, stderr bytes.Buffer
		if exit := run(context.Background(), arguments, &stdout, &stderr); exit == cliresult.ExitSuccess {
			t.Fatalf("branch surface accepted: %v stdout=%s", arguments, stdout.String())
		}
		if stdout.Len() != 0 {
			t.Fatalf("branch rejection leaked stdout for %v: %q", arguments, stdout.String())
		}
	}

	service := &m7BranchBlackBoxService{resident: domain.ResidentSnapshot{
		ResidentID: mustM7AdminCLI_ID(t, "01J00000000000000000000131"), Name: "M7", Status: "active",
	}}
	hub := httpui.NewHub(1)
	defer hub.Close()
	server, err := httpui.New(service, hub, httpui.Options{})
	if err != nil {
		t.Fatal(err)
	}
	request := httptest.NewRequest(http.MethodGet, "http://127.0.0.1/branch", nil)
	response := httptest.NewRecorder()
	server.Handler().ServeHTTP(response, request)
	if response.Code != http.StatusNotFound {
		t.Fatalf("GET /branch status=%d body=%q", response.Code, response.Body.String())
	}

	configPath := filepath.Join(t.TempDir(), "branch.toml")
	if err := os.WriteFile(configPath, []byte("[branch]\nmode = \"must-not-leak\"\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	version := "01J00000000000000000000132"
	called := false
	var stdout, stderr bytes.Buffer
	exit := runAdminSessionPolicySelectWithExecutor(context.Background(), []string{
		"--version", version, "--config", configPath, "--data-dir", t.TempDir(),
	}, &stdout, &stderr, func(context.Context, config.Config, canonical.ID) (sessionPolicySelectExecution, error) {
		called = true
		return sessionPolicySelectExecution{}, nil
	})
	if exit != cliresult.ExitOperational || called || stdout.Len() != 0 ||
		!bytes.Contains(stderr.Bytes(), []byte(`"error_code":"configuration_invalid"`)) ||
		bytes.Contains(stderr.Bytes(), []byte("must-not-leak")) {
		t.Fatalf("branch config exit=%d called=%v stdout=%q stderr=%q", exit, called, stdout.String(), stderr.String())
	}
}

type m7BranchBlackBoxService struct{ resident domain.ResidentSnapshot }

func (service *m7BranchBlackBoxService) IngressWithMetadata(context.Context, domain.IngressRequest) (domain.Event, error) {
	return domain.Event{}, errors.New("unexpected branch ingress")
}
func (service *m7BranchBlackBoxService) History(context.Context, canonical.ID, int) ([]domain.Event, error) {
	return []domain.Event{}, nil
}
func (service *m7BranchBlackBoxService) ActiveResident(context.Context) (domain.ResidentSnapshot, error) {
	return service.resident, nil
}

func decodeM7AdminCLI(t *testing.T, body []byte) map[string]any {
	t.Helper()
	lines := bytes.Split(bytes.TrimSpace(body), []byte{'\n'})
	if len(lines) == 0 {
		t.Fatal("empty CLI envelope")
	}
	var result map[string]any
	if err := json.Unmarshal(lines[len(lines)-1], &result); err != nil {
		t.Fatalf("decode %q: %v", body, err)
	}
	return result
}

func mustM7AdminCLI_ID(t *testing.T, raw string) canonical.ID {
	t.Helper()
	id, err := canonical.ParseID(raw)
	if err != nil {
		t.Fatal(err)
	}
	return id
}

func m7AdminCLIHead(t *testing.T) readiness.Head {
	t.Helper()
	return readiness.Head{
		Exists: true, CommitID: mustM7AdminCLI_ID(t, "01J00000000000000000000199"),
		CommitSeq: 9, CommittedAt: 1_700_000_000_000_000, CommittedTZ: canonical.MustTimezone("UTC"),
	}
}

func m7AdminCLIStringPtr(value string) *string { return &value }
