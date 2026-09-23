package main

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	"mahoroba.local/mahoroba/internal/app"
	"mahoroba.local/mahoroba/internal/blob"
	"mahoroba.local/mahoroba/internal/canonical"
	"mahoroba.local/mahoroba/internal/cliresult"
	"mahoroba.local/mahoroba/internal/config"
	"mahoroba.local/mahoroba/internal/domain"
	"mahoroba.local/mahoroba/internal/fssecure"
	"mahoroba.local/mahoroba/internal/generation"
	"mahoroba.local/mahoroba/internal/hostlock"
	"mahoroba.local/mahoroba/internal/integrity"
	"mahoroba.local/mahoroba/internal/memory"
	"mahoroba.local/mahoroba/internal/projection"
	"mahoroba.local/mahoroba/internal/runtimegate"
	store "mahoroba.local/mahoroba/internal/store/sqlite"
)

func TestM7HTTPServeWaitsOnTheSharedRuntimeStartGate(t *testing.T) {
	t.Run("open", func(t *testing.T) {
		gate := runtimegate.New()
		called := make(chan struct{}, 1)
		returned := make(chan error, 1)
		go func() {
			returned <- serveAfterRuntimeStartGate(context.Background(), gate, func() error {
				called <- struct{}{}
				return nil
			})
		}()
		select {
		case <-called:
			t.Fatal("HTTP serve ran before RuntimeStartGate opened")
		case <-time.After(20 * time.Millisecond):
		}
		if err := gate.Open(); err != nil {
			t.Fatal(err)
		}
		if err := <-returned; err != nil {
			t.Fatal(err)
		}
	})
	t.Run("failed", func(t *testing.T) {
		gate := runtimegate.New()
		want := errors.New("preflight failed")
		called := false
		if err := gate.Fail(want); err != nil {
			t.Fatal(err)
		}
		err := serveAfterRuntimeStartGate(context.Background(), gate, func() error {
			called = true
			return nil
		})
		if !errors.Is(err, want) || called {
			t.Fatalf("failed gate serve = %v, called=%v", err, called)
		}
	})
}

func TestM7AdminIntegrityScanExactCLIIsIdempotent(t *testing.T) {
	dataDir := managedCommandDataDir(t)
	invoke := func() (int, map[string]any, string) {
		t.Helper()
		var stdout, stderr bytes.Buffer
		exitCode := run(context.Background(), []string{
			"admin", "integrity", "scan", "--all", "--data-dir", dataDir,
		}, &stdout, &stderr)
		var envelope map[string]any
		if stdout.Len() != 0 {
			if err := json.Unmarshal(bytes.TrimSpace(stdout.Bytes()), &envelope); err != nil {
				t.Fatalf("decode integrity stdout %q: %v", stdout.String(), err)
			}
		}
		return exitCode, envelope, stderr.String()
	}

	firstExit, first, firstStderr := invoke()
	if firstExit != cliresult.ExitSuccess {
		t.Fatalf("first exit = %d, stderr=%s", firstExit, firstStderr)
	}
	if first["command"] != string(cliresult.CommandAdminIntegrityScan) || first["outcome"] != string(cliresult.OutcomeSuccess) {
		t.Fatalf("first envelope identity = %#v", first)
	}
	if first["canonical_applied"] != true {
		t.Fatalf("first canonical_applied = %#v", first["canonical_applied"])
	}
	commits, ok := first["canonical_commits"].([]any)
	if !ok || len(commits) != 1 {
		t.Fatalf("first canonical commits = %#v", first["canonical_commits"])
	}
	firstResult, ok := first["result"].(map[string]any)
	if !ok || firstResult["existing_findings"] != "0" || firstResult["created_findings"] != "0" ||
		firstResult["created_quarantines"] != "0" || firstResult["readiness_blocked"] != false {
		t.Fatalf("first integrity result = %#v", first["result"])
	}
	if !strings.Contains(firstStderr, `"warning_code":"local_admin_identity_not_attributed"`) {
		t.Fatalf("first warning stream = %q", firstStderr)
	}

	secondExit, second, secondStderr := invoke()
	if secondExit != cliresult.ExitSuccess {
		t.Fatalf("second exit = %d, stderr=%s", secondExit, secondStderr)
	}
	if second["canonical_applied"] != true {
		t.Fatalf("second canonical_applied = %#v", second["canonical_applied"])
	}
	commits, ok = second["canonical_commits"].([]any)
	if !ok || len(commits) != 1 {
		t.Fatalf("second canonical commits = %#v", second["canonical_commits"])
	}
	secondCommit, ok := commits[0].(map[string]any)
	if !ok || secondCommit["disposition"] != string(cliresult.DispositionExisting) {
		t.Fatalf("second commit disposition = %#v", commits[0])
	}
	secondResult, ok := second["result"].(map[string]any)
	if !ok || secondResult["captured_head"] == nil || secondResult["result_head"] == nil {
		t.Fatalf("second integrity result = %#v", second["result"])
	}

	inspection, err := store.OpenInspection(context.Background(), filepath.Join(dataDir, "mahoroba.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer inspection.Close()
	for _, pipeline := range [][2]string{
		{integrity.IntegrityPipelineKind, integrity.IntegrityPipelineVersion},
		{"memory_status", domain.MemoryStatusPipelineVersion},
	} {
		if _, err := inspection.Canonical().PipelineVersion(context.Background(), pipeline[0], pipeline[1]); err != nil {
			t.Fatalf("exact pipeline %s/%s: %v", pipeline[0], pipeline[1], err)
		}
	}
}

func TestM7AdminIntegrityScanRejectsAmbiguousScopeWithExactUsage(t *testing.T) {
	var stdout, stderr bytes.Buffer
	exitCode := run(context.Background(), []string{
		"admin", "integrity", "scan", "--all", "--resident", "01H00000000000000000000001",
	}, &stdout, &stderr)
	if exitCode != cliresult.ExitUsage || stdout.Len() != 0 {
		t.Fatalf("usage exit/stdout = %d/%q", exitCode, stdout.String())
	}
	if !strings.HasPrefix(stderr.String(), "usage: mahoroba admin integrity scan") ||
		!strings.Contains(stderr.String(), `"error_code":"cli_usage"`) {
		t.Fatalf("usage stderr = %q", stderr.String())
	}
}

func TestM7AdminIntegrityScanLockBusyUsesStableError(t *testing.T) {
	dataDir := managedCommandDataDir(t)
	guard, err := hostlock.Acquire(dataDir)
	if err != nil {
		t.Fatal(err)
	}
	defer guard.Close()
	var stdout, stderr bytes.Buffer
	exitCode := run(context.Background(), []string{
		"admin", "integrity", "scan", "--all", "--data-dir", dataDir,
	}, &stdout, &stderr)
	if exitCode != cliresult.ExitOperational || stdout.Len() != 0 {
		t.Fatalf("lock-busy exit/stdout = %d/%q", exitCode, stdout.String())
	}
	lines := bytes.Split(bytes.TrimSpace(stderr.Bytes()), []byte{'\n'})
	if len(lines) == 0 || !bytes.Contains(lines[len(lines)-1], []byte(`"error_code":"admin_lock_busy"`)) {
		t.Fatalf("lock-busy stderr = %q", stderr.String())
	}
}

func TestM7AdminRecoveryTerminalizeExactCLIIsIdempotent(t *testing.T) {
	dataDir := managedCommandDataDir(t)
	invoke := func() (int, map[string]any, string) {
		t.Helper()
		var stdout, stderr bytes.Buffer
		exitCode := run(context.Background(), []string{
			"admin", "recovery", "terminalize", "--data-dir", dataDir,
		}, &stdout, &stderr)
		var envelope map[string]any
		if stdout.Len() != 0 {
			if err := json.Unmarshal(bytes.TrimSpace(stdout.Bytes()), &envelope); err != nil {
				t.Fatalf("decode recovery stdout %q: %v", stdout.String(), err)
			}
		}
		return exitCode, envelope, stderr.String()
	}

	firstExit, first, firstStderr := invoke()
	if firstExit != cliresult.ExitSuccess ||
		first["command"] != string(cliresult.CommandAdminRecoveryTerminalize) ||
		first["outcome"] != string(cliresult.OutcomeSuccess) {
		t.Fatalf("first recovery = exit %d envelope=%#v stderr=%s", firstExit, first, firstStderr)
	}
	firstResult, ok := first["result"].(map[string]any)
	if !ok || firstResult["terminalized_attempts"] != "0" ||
		firstResult["cancelled_mandatory_work"] != "0" || firstResult["all_existing"] != false {
		t.Fatalf("first recovery result = %#v", first["result"])
	}
	if first["canonical_applied"] != true ||
		!strings.Contains(firstStderr, `"warning_code":"local_admin_identity_not_attributed"`) {
		t.Fatalf("first recovery evidence/warnings = %#v / %q", first, firstStderr)
	}

	secondExit, second, secondStderr := invoke()
	if secondExit != cliresult.ExitSuccess {
		t.Fatalf("second recovery exit = %d stderr=%s", secondExit, secondStderr)
	}
	secondResult, ok := second["result"].(map[string]any)
	if !ok || secondResult["all_existing"] != true {
		t.Fatalf("second recovery result = %#v", second["result"])
	}
	commits, ok := second["canonical_commits"].([]any)
	if !ok || len(commits) != 1 {
		t.Fatalf("second recovery commits = %#v", second["canonical_commits"])
	}
	commit, ok := commits[0].(map[string]any)
	if !ok || commit["disposition"] != string(cliresult.DispositionExisting) {
		t.Fatalf("second recovery commit = %#v", commits[0])
	}
}

func TestM7AdminRecoveryTerminalizeRejectsArgumentsWithExactUsage(t *testing.T) {
	var stdout, stderr bytes.Buffer
	exitCode := run(context.Background(), []string{
		"admin", "recovery", "terminalize", "unexpected",
	}, &stdout, &stderr)
	if exitCode != cliresult.ExitUsage || stdout.Len() != 0 {
		t.Fatalf("usage exit/stdout = %d/%q", exitCode, stdout.String())
	}
	if !strings.HasPrefix(stderr.String(), "usage: mahoroba admin recovery terminalize") ||
		!strings.Contains(stderr.String(), `"error_code":"cli_usage"`) {
		t.Fatalf("usage stderr = %q", stderr.String())
	}
}

func TestRuntimeCloseWaitsForGenerationAndPersistsInterruptionBeforeDatabaseClose(t *testing.T) {
	ctx := context.Background()
	cfg := config.Config{
		DataDir: managedCommandDataDir(t), Timezone: "UTC",
		Server:   config.Server{Listen: "127.0.0.1:0", ShutdownTimeout: 5 * time.Second},
		Database: config.Database{Filename: "mahoroba.db"},
		Generation: config.Generation{
			Provider: "test", Model: "test-model", MaxAttempts: 3,
			RetryBackoff: []time.Duration{0, 0}, MaxConcurrency: 1,
			DisconnectPolicy: "continue", MaxInputBytes: 64 << 10, MaxOutputBytes: 64 << 10,
			SafetyScanInterval: time.Hour,
		},
		Projection: testProjectionConfig(),
	}
	generator := newRuntimeCancellationGenerator()
	runtime, err := openRuntime(ctx, cfg, generator)
	if err != nil {
		t.Fatal(err)
	}
	state, err := runtime.app.BootstrapInit(ctx, app.BootstrapInput{
		OwnerName: "Owner", Name: "Resident", SeedKey: "runtime-close", Principles: "be helpful",
	})
	if err != nil {
		t.Fatal(err)
	}
	residentID := state.Residents[0].ResidentID
	if err := runtime.app.ApprovePrinciples(ctx, residentID); err != nil {
		t.Fatal(err)
	}
	if err := runtime.app.FinalizeBootstrap(ctx, residentID, "friendly", defaultMemory); err != nil {
		t.Fatal(err)
	}
	if err := runtime.app.SelectResident(ctx, residentID); err != nil {
		t.Fatal(err)
	}
	if err := runtime.app.Start(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := runtime.app.Ingress(ctx, "shutdown ordering"); err != nil {
		t.Fatal(err)
	}
	select {
	case <-generator.started:
	case <-time.After(5 * time.Second):
		t.Fatal("provider did not start")
	}
	if err := runtime.Close(5 * time.Second); err != nil {
		t.Fatal(err)
	}
	select {
	case <-generator.returned:
	default:
		t.Fatal("runtime closed persistence before generation returned")
	}
	select {
	case <-runtime.writer.Done():
	default:
		t.Fatal("runtime close did not join the Canonical Writer")
	}

	reopened, err := store.Open(ctx, filepath.Join(cfg.DataDir, cfg.Database.Filename))
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	var stateName, code string
	if err := reopened.Reader().QueryRowContext(ctx, `SELECT state, error_class
		FROM generation_run_outcomes ORDER BY outcome_id DESC LIMIT 1`).Scan(&stateName, &code); err != nil {
		t.Fatal(err)
	}
	if stateName != "failed" || code != generation.RuntimeInterruptedErrorCode().String() {
		t.Fatalf("shutdown outcome = state=%q code=%q", stateName, code)
	}
}

func TestRuntimeParentCancellationDoesNotStopProjectionBeforeApplicationShutdown(t *testing.T) {
	parent, cancelParent := context.WithCancel(context.Background())
	runtime, err := openRuntime(parent, commandRuntimeConfig(managedCommandDataDir(t)), nil)
	if err != nil {
		t.Fatal(err)
	}
	cancelParent()

	waitCtx, cancelWait := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancelWait()
	if err := runtime.projections.Wait(waitCtx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Projection stopped directly with parent context: %v", err)
	}
	if err := runtime.Close(5 * time.Second); err != nil {
		t.Fatal(err)
	}
}

func TestDBAndLedgerVerifyDoNotRecoverPreExistingRunningGeneration(t *testing.T) {
	ctx := context.Background()
	cfg := commandRuntimeConfig(managedCommandDataDir(t))
	runtime, err := openRuntime(ctx, cfg, runningAttemptCrashGenerator{})
	if err != nil {
		t.Fatal(err)
	}
	state, err := runtime.app.BootstrapInit(ctx, app.BootstrapInput{
		OwnerName: "Owner", Name: "Resident", SeedKey: "verify-no-recovery", Principles: "be helpful",
	})
	if err != nil {
		t.Fatal(err)
	}
	residentID := state.Residents[0].ResidentID
	if err := runtime.app.ApprovePrinciples(ctx, residentID); err != nil {
		t.Fatal(err)
	}
	if err := runtime.app.FinalizeBootstrap(ctx, residentID, "friendly", defaultMemory); err != nil {
		t.Fatal(err)
	}
	if err := runtime.app.SelectResident(ctx, residentID); err != nil {
		t.Fatal(err)
	}
	// Simulate a process crash immediately after durable Commit B. Verification
	// must preserve this running attempt without launching recovery.
	leaveRunningDialogueAttempt(t, ctx, runtime, residentID, "leave this attempt running")
	beforeCommits, beforeOutcomes, beforeState := generationState(t, runtime.store)
	if beforeState != "running" {
		t.Fatalf("fixture outcome state = %q, want running", beforeState)
	}
	if err := runtime.Close(5 * time.Second); err != nil {
		t.Fatal(err)
	}

	configPath := writeCommandConfig(t, cfg.DataDir)
	for name, verify := range map[string]func(context.Context, []string, *bytes.Buffer, *bytes.Buffer) error{
		"db": func(ctx context.Context, args []string, out, errOut *bytes.Buffer) error {
			return runDBVerify(ctx, args, out, errOut)
		},
		"ledger": func(ctx context.Context, args []string, out, errOut *bytes.Buffer) error {
			return runLedgerVerify(ctx, args, out, errOut)
		},
	} {
		t.Run(name, func(t *testing.T) {
			var stdout, stderr bytes.Buffer
			if err := verify(ctx, []string{"--config", configPath}, &stdout, &stderr); err != nil {
				t.Fatalf("verify error = %v, stderr=%s", err, stderr.String())
			}
		})
	}

	reopened, err := store.Open(ctx, cfg.DatabasePath())
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	afterCommits, afterOutcomes, afterState := generationState(t, reopened)
	if afterCommits != beforeCommits || afterOutcomes != beforeOutcomes || afterState != beforeState {
		t.Fatalf("verification mutated generation state: before commits=%d outcomes=%d state=%q; after commits=%d outcomes=%d state=%q",
			beforeCommits, beforeOutcomes, beforeState, afterCommits, afterOutcomes, afterState)
	}
}

func TestLedgerVerifyAndRuntimeStartupRejectEventlessResidentContentTampering(t *testing.T) {
	ctx := context.Background()
	cfg := commandRuntimeConfig(managedCommandDataDir(t))
	runtime, err := openRuntime(ctx, cfg, nil)
	if err != nil {
		t.Fatal(err)
	}
	const principles = "eventless-principles-integrity-probe-7c925d40"
	state, err := runtime.app.BootstrapInit(ctx, app.BootstrapInput{
		OwnerName: "Owner", Name: "Eventless Resident", SeedKey: "eventless-integrity", Principles: principles,
	})
	if err != nil {
		t.Fatal(err)
	}
	residentID := state.Residents[0].ResidentID
	if err := runtime.app.ApprovePrinciples(ctx, residentID); err != nil {
		t.Fatal(err)
	}
	if err := runtime.app.FinalizeBootstrap(ctx, residentID, "eventless persona", defaultMemory); err != nil {
		t.Fatal(err)
	}
	var eventCount int
	if err := runtime.store.Reader().QueryRowContext(ctx, `SELECT COUNT(*) FROM events WHERE resident_id = ?`, residentID.String()).Scan(&eventCount); err != nil {
		t.Fatal(err)
	}
	if eventCount != 0 {
		t.Fatalf("eventless integrity fixture events = %d, want 0", eventCount)
	}
	if err := runtime.Close(5 * time.Second); err != nil {
		t.Fatal(err)
	}

	replacement := bytes.Repeat([]byte("X"), len(principles))
	replaceSQLitePayloadSameLength(t, cfg.DatabasePath(), []byte(principles), replacement)
	configPath := writeCommandConfig(t, cfg.DataDir)
	var stdout, stderr bytes.Buffer
	if err := runDBVerify(ctx, []string{"--config", configPath}, &stdout, &stderr); err != nil {
		t.Fatalf("same-length content tamper broke the schema gate: %v, stderr=%s", err, stderr.String())
	}
	stdout.Reset()
	stderr.Reset()
	if err := runLedgerVerify(ctx, []string{"--config", configPath}, &stdout, &stderr); !errors.Is(err, canonical.ErrLedgerViolation) {
		t.Fatalf("ledger verify tamper error = %v, want ErrLedgerViolation", err)
	}
	if reopened, err := openRuntime(ctx, cfg, nil); !errors.Is(err, canonical.ErrLedgerViolation) {
		if reopened != nil {
			_ = reopened.Close(time.Second)
		}
		t.Fatalf("runtime startup tamper error = %v, want ErrLedgerViolation", err)
	}
	lock, err := hostlock.Acquire(cfg.DataDir)
	if err != nil {
		t.Fatalf("failed runtime verification retained the host lock: %v", err)
	}
	if err := lock.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestM7RuntimeStartGateRejectsMissingFilesystemBlobBeforeWriter(t *testing.T) {
	ctx := context.Background()
	cfg := commandRuntimeConfig(managedCommandDataDir(t))
	runtime, err := openRuntime(ctx, cfg, runningAttemptCrashGenerator{})
	if err != nil {
		t.Fatal(err)
	}
	state, err := runtime.app.BootstrapInit(ctx, app.BootstrapInput{
		OwnerName: "Owner", Name: "Dual Blob Resident", SeedKey: "dual-blob-startup",
		Principles: "filesystem copies are independently verified",
	})
	if err != nil {
		t.Fatal(err)
	}
	residentID := state.Residents[0].ResidentID
	if err := runtime.app.ApprovePrinciples(ctx, residentID); err != nil {
		t.Fatal(err)
	}
	if err := runtime.app.FinalizeBootstrap(ctx, residentID, "dual blob persona", defaultMemory); err != nil {
		t.Fatal(err)
	}
	if err := runtime.app.SelectResident(ctx, residentID); err != nil {
		t.Fatal(err)
	}
	// Leave an interrupted Commit-B attempt. A fatal MinimumCheck must reject
	// the next startup before recovery can append a terminal outcome or finding.
	leaveRunningDialogueAttempt(t, ctx, runtime, residentID, "leave recovery work behind the fatal gate")
	var digestBytes []byte
	if err := runtime.store.Reader().QueryRowContext(ctx, `SELECT blob_hash
		FROM content_objects WHERE owner_resident_id = ? AND erasure_state = 'present'
		ORDER BY content_id LIMIT 1`, residentID.String()).Scan(&digestBytes); err != nil {
		t.Fatal(err)
	}
	digest, err := canonical.DigestFromBytes(digestBytes)
	if err != nil {
		t.Fatal(err)
	}
	beforeCommits, beforeOutcomes, beforeState := generationState(t, runtime.store)
	var beforeFindings int
	if err := runtime.store.Reader().QueryRowContext(ctx, `SELECT COUNT(*) FROM integrity_findings`).Scan(&beforeFindings); err != nil {
		t.Fatal(err)
	}
	if beforeState != "running" {
		t.Fatalf("fatal gate fixture state = %q, want running", beforeState)
	}
	if err := runtime.Close(5 * time.Second); err != nil {
		t.Fatal(err)
	}
	objectPath := filepath.Join(cfg.BlobPath(), "objects", residentID.String(), digest.Hex()[:2], digest.Hex()[2:])
	if err := os.Remove(objectPath); err != nil {
		t.Fatal(err)
	}

	reopened, err := openRuntime(ctx, cfg, nil)
	if reopened != nil {
		_ = reopened.Close(time.Second)
	}
	if !errors.Is(err, integrity.ErrFatal) {
		t.Fatalf("runtime startup missing filesystem blob error = %v, want integrity.ErrFatal", err)
	}
	inspection, inspectErr := store.Open(ctx, cfg.DatabasePath())
	if inspectErr != nil {
		t.Fatal(inspectErr)
	}
	afterCommits, afterOutcomes, afterState := generationState(t, inspection)
	var afterFindings int
	if err := inspection.Reader().QueryRowContext(ctx, `SELECT COUNT(*) FROM integrity_findings`).Scan(&afterFindings); err != nil {
		_ = inspection.Close()
		t.Fatal(err)
	}
	if err := inspection.Close(); err != nil {
		t.Fatal(err)
	}
	if afterCommits != beforeCommits || afterOutcomes != beforeOutcomes || afterState != beforeState || afterFindings != beforeFindings {
		t.Fatalf("fatal preflight mutated Canonical state/findings: before=%d/%d/%s/%d after=%d/%d/%s/%d",
			beforeCommits, beforeOutcomes, beforeState, beforeFindings,
			afterCommits, afterOutcomes, afterState, afterFindings)
	}
	lock, lockErr := hostlock.Acquire(cfg.DataDir)
	if lockErr != nil {
		t.Fatalf("failed MinimumCheck retained the host lock: %v", lockErr)
	}
	if err := lock.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestM7ServeRejectsHalfErasedClaimAliasBeforeListenerOrFinding(t *testing.T) {
	ctx := context.Background()
	cfg := commandRuntimeConfig(managedCommandDataDir(t))
	runtime, err := openRuntimeForServe(ctx, cfg, memoryCLIExtractionGenerator{})
	if err != nil {
		t.Fatal(err)
	}
	state, err := runtime.app.BootstrapInit(ctx, app.BootstrapInput{
		OwnerName: "Owner", Name: "Alias Resident", SeedKey: "alias-startup",
		Principles: "alias groups must remain atomic",
	})
	if err != nil {
		t.Fatal(err)
	}
	residentID := state.Residents[0].ResidentID
	if err := runtime.app.ApprovePrinciples(ctx, residentID); err != nil {
		t.Fatal(err)
	}
	if err := runtime.app.FinalizeBootstrap(ctx, residentID, "alias persona", defaultMemory); err != nil {
		t.Fatal(err)
	}
	if err := runtime.app.SelectResident(ctx, residentID); err != nil {
		t.Fatal(err)
	}
	if _, err := runtime.app.ActivateMemoryPolicyV4(ctx, app.ActivateMemoryPolicyV4Options{
		ResidentID: residentID, ExpectedFrom: memory.PolicyVersionV1,
		AcknowledgeRecallEnable: true, AcknowledgeSelfTalkExtraction: true,
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := runtime.app.Ingress(ctx, "I own a blue bicycle"); err != nil {
		t.Fatal(err)
	}
	if err := runtime.app.ProcessResident(ctx, residentID); err != nil {
		t.Fatal(err)
	}
	var beforeCommits, beforeFindings int
	if err := runtime.store.Reader().QueryRowContext(ctx, `SELECT COUNT(*) FROM canonical_commits`).Scan(&beforeCommits); err != nil {
		t.Fatal(err)
	}
	if err := runtime.store.Reader().QueryRowContext(ctx, `SELECT COUNT(*) FROM integrity_findings`).Scan(&beforeFindings); err != nil {
		t.Fatal(err)
	}
	if err := runtime.Close(5 * time.Second); err != nil {
		t.Fatal(err)
	}
	makeHalfErasedAliasFixture(t, cfg.DatabasePath())

	reopened, err := openRuntimeForServe(ctx, cfg, nil)
	if reopened != nil {
		_ = reopened.Close(time.Second)
	}
	if !errors.Is(err, integrity.ErrFatal) {
		t.Fatalf("serve startup half-erased alias error = %v, want integrity.ErrFatal", err)
	}
	inspection, err := store.Open(ctx, cfg.DatabasePath())
	if err != nil {
		t.Fatal(err)
	}
	defer inspection.Close()
	var afterCommits, afterFindings int
	if err := inspection.Reader().QueryRowContext(ctx, `SELECT COUNT(*) FROM canonical_commits`).Scan(&afterCommits); err != nil {
		t.Fatal(err)
	}
	if err := inspection.Reader().QueryRowContext(ctx, `SELECT COUNT(*) FROM integrity_findings`).Scan(&afterFindings); err != nil {
		t.Fatal(err)
	}
	if afterCommits != beforeCommits || afterFindings != beforeFindings {
		t.Fatalf("fatal alias startup mutated commits/findings: before=%d/%d after=%d/%d",
			beforeCommits, beforeFindings, afterCommits, afterFindings)
	}
}

func makeHalfErasedAliasFixture(t *testing.T, databasePath string) {
	t.Helper()
	database, err := sql.Open("sqlite", databasePath)
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	database.SetMaxOpenConns(1)
	tx, err := database.BeginTx(context.Background(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	var sourceClaim string
	if err := tx.QueryRow(`SELECT claim_id FROM claims ORDER BY claim_id LIMIT 1`).Scan(&sourceClaim); err != nil {
		t.Fatal(err)
	}
	aliasID, err := canonical.NewSecureIDGenerator().New()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec(`INSERT INTO claims
		SELECT ?, canonical_commit_id, owner_resident_id, subject_principal_id,
		perspective_principal_id, kind, temporal_kind, statement_content_id,
		statement_hash, statement_hash_algorithm, statement_normalization_version,
		created_by_run_id, recorded_at, recorded_tz
		FROM claims WHERE claim_id = ?`, aliasID.String(), sourceClaim); err != nil {
		t.Fatal(err)
	}
	var triggerSQL string
	if err := tx.QueryRow(`SELECT sql FROM sqlite_schema
		WHERE type = 'trigger' AND name = 'trg_claims_update_contract'`).Scan(&triggerSQL); err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec(`DROP TRIGGER trg_claims_update_contract`); err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec(`UPDATE claims SET statement_hash = NULL WHERE claim_id = ?`, aliasID.String()); err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec(triggerSQL); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
}

func TestM7ServePreflightTerminalizesRunningAttemptBeforeProjectionCurrentness(t *testing.T) {
	ctx := context.Background()
	cfg := commandRuntimeConfig(managedCommandDataDir(t))
	runtime, err := openRuntimeForServe(ctx, cfg, runningAttemptCrashGenerator{})
	if err != nil {
		t.Fatal(err)
	}
	state, err := runtime.app.BootstrapInit(ctx, app.BootstrapInput{
		OwnerName: "Owner", Name: "Ready Resident", SeedKey: "serve-readiness",
		Principles: "recover before activation",
	})
	if err != nil {
		t.Fatal(err)
	}
	residentID := state.Residents[0].ResidentID
	if err := runtime.app.ApprovePrinciples(ctx, residentID); err != nil {
		t.Fatal(err)
	}
	if err := runtime.app.FinalizeBootstrap(ctx, residentID, "ready persona", defaultMemory); err != nil {
		t.Fatal(err)
	}
	if err := runtime.app.SelectResident(ctx, residentID); err != nil {
		t.Fatal(err)
	}
	leaveRunningDialogueAttempt(t, ctx, runtime, residentID, "recover me before Projection preflight")
	commitsBefore, outcomesBefore, stateBefore := generationState(t, runtime.store)
	if stateBefore != "running" {
		t.Fatalf("fixture state = %q, want running", stateBefore)
	}
	if err := runtime.app.Prepare(ctx); err != nil {
		t.Fatal(err)
	}
	commitsAfterRecovery, outcomesAfterRecovery, stateAfterRecovery := generationState(t, runtime.store)
	if commitsAfterRecovery != commitsBefore+1 || outcomesAfterRecovery != outcomesBefore+1 || stateAfterRecovery != "cancelled" {
		t.Fatalf("startup recovery state = %d/%d/%s, before %d/%d/%s",
			commitsAfterRecovery, outcomesAfterRecovery, stateAfterRecovery,
			commitsBefore, outcomesBefore, stateBefore)
	}
	integrityResult, err := runtime.integrity.Run(ctx, nil)
	if err != nil {
		t.Fatalf("startup integrity scan/apply: %v", err)
	}
	if len(integrityResult.CanonicalCommits) != 1 ||
		!integrityResult.CanonicalCommits[0].Effects.PipelineVersionRegistered {
		t.Fatalf("startup integrity bootstrap = %+v", integrityResult.CanonicalCommits)
	}
	commitsAfterIntegrity, outcomesAfterIntegrity, stateAfterIntegrity := generationState(t, runtime.store)
	if commitsAfterIntegrity != commitsAfterRecovery+1 ||
		outcomesAfterIntegrity != outcomesAfterRecovery || stateAfterIntegrity != stateAfterRecovery {
		t.Fatalf("integrity startup ordering state = %d/%d/%s, recovery %d/%d/%s",
			commitsAfterIntegrity, outcomesAfterIntegrity, stateAfterIntegrity,
			commitsAfterRecovery, outcomesAfterRecovery, stateAfterRecovery)
	}
	selected, err := runtime.app.ActiveResident(ctx)
	if err != nil {
		t.Fatal(err)
	}
	preflight, err := runtime.projections.PreflightServiceResident(ctx, selected.ResidentID)
	if err != nil {
		t.Fatalf("Projection preflight: %v", err)
	}
	if preflight.ResidentID != residentID || len(preflight.Names) != len(projection.ServiceRequiredNames()) {
		t.Fatalf("Projection preflight result = %+v", preflight)
	}
	if count := projectionWatermarkCount(t, cfg.DataDir, residentID.String()); count != len(projection.ServiceRequiredNames()) {
		t.Fatalf("service-required watermarks = %d, want %d", count, len(projection.ServiceRequiredNames()))
	}
	if err := runtime.app.Recover(ctx); err != nil {
		t.Fatal(err)
	}
	commitsAfterRetry, outcomesAfterRetry, stateAfterRetry := generationState(t, runtime.store)
	if commitsAfterRetry != commitsAfterIntegrity || outcomesAfterRetry != outcomesAfterIntegrity || stateAfterRetry != stateAfterIntegrity {
		t.Fatalf("recovery retry changed state: first=%d/%d/%s retry=%d/%d/%s",
			commitsAfterIntegrity, outcomesAfterIntegrity, stateAfterIntegrity,
			commitsAfterRetry, outcomesAfterRetry, stateAfterRetry)
	}
	if err := runtime.Close(5 * time.Second); err != nil {
		t.Fatal(err)
	}
}

func TestWriterRuntimeAndBlobRecoveryShareDataDirectoryLock(t *testing.T) {
	ctx := context.Background()
	cfg := commandRuntimeConfig(managedCommandDataDir(t))
	configPath := writeCommandConfig(t, cfg.DataDir)

	guard, err := hostlock.Acquire(cfg.DataDir)
	if err != nil {
		t.Fatal(err)
	}
	if runtime, err := openRuntime(ctx, cfg, nil); !errors.Is(err, hostlock.ErrLocked) {
		if runtime != nil {
			_ = runtime.Close(time.Second)
		}
		t.Fatalf("openRuntime while locked = %v, want ErrLocked", err)
	}
	if err := guard.Close(); err != nil {
		t.Fatal(err)
	}

	runtime, err := openRuntime(ctx, cfg, nil)
	if err != nil {
		t.Fatal(err)
	}
	var stdout, stderr bytes.Buffer
	if err := runBlobRecover(ctx, []string{"--config", configPath}, &stdout, &stderr); !errors.Is(err, hostlock.ErrLocked) {
		t.Fatalf("blob recover while writer runtime is open = %v, want ErrLocked", err)
	}
	if err := runtime.Close(5 * time.Second); err != nil {
		t.Fatal(err)
	}
	stdout.Reset()
	stderr.Reset()
	if err := runBlobRecover(ctx, []string{"--config", configPath}, &stdout, &stderr); err != nil {
		t.Fatalf("blob recover after runtime release = %v, stderr=%s", err, stderr.String())
	}
}

func TestBlobRecoverFailsClosedWithoutCanonicalDatabaseAndPreservesSealedObject(t *testing.T) {
	ctx := context.Background()
	cfg := commandRuntimeConfig(managedCommandDataDir(t))
	configPath := writeCommandConfig(t, cfg.DataDir)
	objects, err := blob.NewFileStore(cfg.BlobPath())
	if err != nil {
		t.Fatal(err)
	}
	residentID, err := canonical.ParseID("01ARZ3NDEKTSV4RRFFQ69G5FAV")
	if err != nil {
		t.Fatal(err)
	}
	staged, err := objects.StageBytes(ctx, residentID, []byte("must survive a missing authority"))
	if err != nil {
		t.Fatal(err)
	}
	object, err := objects.Finalize(ctx, residentID, staged)
	if err != nil {
		t.Fatal(err)
	}

	var stdout, stderr bytes.Buffer
	if err := runBlobRecover(ctx, []string{"--config", configPath}, &stdout, &stderr); err == nil {
		t.Fatal("blob recover succeeded without an authoritative Canonical database")
	}
	if _, err := os.Stat(cfg.DatabasePath()); !os.IsNotExist(err) {
		t.Fatalf("inspection created missing Canonical database: %v", err)
	}
	if exists, err := objects.Exists(residentID, object.Digest); err != nil || !exists {
		t.Fatalf("sealed object after failed recovery = %v, %v", exists, err)
	}
	if orphans, err := objects.ListOrphans(ctx); err != nil || len(orphans) != 1 {
		t.Fatalf("recovery marker after failed recovery = %v, %v", orphans, err)
	}
}

func TestHTTPDrainFailureDoesNotClosePersistenceUnderLiveHandlers(t *testing.T) {
	ctx := context.Background()
	cfg := commandRuntimeConfig(managedCommandDataDir(t))
	runtime, err := openRuntime(ctx, cfg, nil)
	if err != nil {
		t.Fatal(err)
	}
	closed := false
	defer func() {
		if !closed {
			_ = runtime.Close(5 * time.Second)
		}
	}()

	drainErr := context.DeadlineExceeded
	if err := closeRuntimeAfterHTTPDrain(runtime, drainErr, time.Second); !errors.Is(err, drainErr) {
		t.Fatalf("drain failure = %v, want context deadline exceeded", err)
	}
	select {
	case <-runtime.writer.Done():
		t.Fatal("HTTP drain failure closed the Canonical Writer while handlers may still be live")
	default:
	}
	if err := runtime.store.Reader().PingContext(ctx); err != nil {
		t.Fatalf("HTTP drain failure closed SQLite while handlers may still be live: %v", err)
	}
	if competing, err := hostlock.Acquire(cfg.DataDir); !errors.Is(err, hostlock.ErrLocked) {
		if competing != nil {
			_ = competing.Close()
		}
		t.Fatalf("host lock after drain failure = %v, want ErrLocked", err)
	}

	if err := runtime.Close(5 * time.Second); err != nil {
		t.Fatal(err)
	}
	closed = true
}

func commandRuntimeConfig(dataDir string) config.Config {
	return config.Config{
		DataDir: dataDir, Timezone: "UTC",
		Server:   config.Server{Listen: "127.0.0.1:0", ShutdownTimeout: 5 * time.Second},
		Database: config.Database{Filename: "mahoroba.db"},
		Generation: config.Generation{
			Provider: "chat-completions", Model: "test-model", MaxAttempts: 3,
			RetryBackoff: []time.Duration{time.Millisecond, time.Millisecond}, MaxConcurrency: 1,
			DisconnectPolicy: "continue", MaxInputBytes: 64 << 10, MaxOutputBytes: 64 << 10,
			SafetyScanInterval: time.Hour, RequestTimeout: time.Second,
		},
		Projection: testProjectionConfig(),
	}
}

func managedCommandDataDir(t *testing.T) string {
	t.Helper()
	dataDir := filepath.Join(t.TempDir(), "data")
	lock, err := hostlock.Acquire(dataDir)
	if err != nil {
		if runtime.GOOS == "windows" && os.Getenv("CI") == "" && errors.Is(err, os.ErrPermission) {
			t.Skipf("desktop sandbox cannot create the exact protected data-root ACL: %v", err)
		}
		t.Fatalf("create managed command data root: %v", err)
	}
	if err := lock.Close(); err != nil {
		t.Fatal(err)
	}
	policy, err := fssecure.CurrentSecurityPolicy()
	if err != nil {
		t.Fatal(err)
	}
	root, err := fssecure.OpenRootReadOnly(dataDir, policy)
	if err != nil {
		t.Fatalf("managed command data root is not exact protected: %v", err)
	}
	if err := root.Close(); err != nil {
		t.Fatal(err)
	}
	return dataDir
}

func testProjectionConfig() config.Projection {
	return config.Projection{
		ScanInterval: time.Hour, AsOfRefreshInterval: time.Hour,
		RebuildRetryInterval: time.Hour, MaxStaleness: 5 * time.Minute,
		RebuildTransactionTimeout: 2 * time.Second,
	}
}

func writeCommandConfig(t *testing.T, dataDir string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "config.toml")
	content := fmt.Sprintf("data_dir = %q\n\n[generation]\nbase_url = %q\n", dataDir, "http://127.0.0.1:1/v1")
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}

func replaceSQLitePayloadSameLength(t *testing.T, path string, original, replacement []byte) {
	t.Helper()
	if len(original) == 0 || len(original) != len(replacement) {
		t.Fatalf("invalid same-length corruption fixture: original=%d replacement=%d", len(original), len(replacement))
	}
	database, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	database.SetMaxOpenConns(1)
	defer database.Close()
	if _, err := database.ExecContext(context.Background(), "PRAGMA busy_timeout = 5000"); err != nil {
		t.Fatal(err)
	}

	tx, err := database.BeginTx(context.Background(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	var triggerSQL string
	if err := tx.QueryRowContext(context.Background(), `
SELECT sql
FROM sqlite_schema
WHERE type = 'trigger' AND name = 'trg_blobs_no_update'`).Scan(&triggerSQL); err != nil {
		t.Fatal(err)
	}
	if _, err := tx.ExecContext(context.Background(), "DROP TRIGGER trg_blobs_no_update"); err != nil {
		t.Fatal(err)
	}
	result, err := tx.ExecContext(context.Background(),
		"UPDATE blobs SET content = ? WHERE content = ? AND byte_size = ?",
		replacement, original, len(original))
	if err != nil {
		t.Fatal(err)
	}
	if count, err := result.RowsAffected(); err != nil {
		t.Fatal(err)
	} else if count != 1 {
		t.Fatalf("SQLite corruption fixture occurrences = %d, want exactly 1", count)
	}
	if _, err := tx.ExecContext(context.Background(), triggerSQL); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
}

func generationState(t *testing.T, database *store.Store) (commits, outcomes int, state string) {
	t.Helper()
	ctx := context.Background()
	if err := database.Reader().QueryRowContext(ctx, "SELECT COUNT(*) FROM canonical_commits").Scan(&commits); err != nil {
		t.Fatal(err)
	}
	if err := database.Reader().QueryRowContext(ctx, "SELECT COUNT(*) FROM generation_run_outcomes").Scan(&outcomes); err != nil {
		t.Fatal(err)
	}
	if err := database.Reader().QueryRowContext(ctx,
		"SELECT state FROM generation_run_outcomes ORDER BY outcome_id DESC LIMIT 1").Scan(&state); err != nil {
		t.Fatal(err)
	}
	return commits, outcomes, state
}

const runningAttemptCrashPanic = "fixture crash after durable Commit B"

type runningAttemptCrashGenerator struct{}

func (runningAttemptCrashGenerator) Stream(
	context.Context,
	generation.Request,
	generation.DeltaSink,
) (generation.Result, error) {
	panic(runningAttemptCrashPanic)
}

func leaveRunningDialogueAttempt(
	t *testing.T,
	ctx context.Context,
	runtime *runtimeComponents,
	residentID canonical.ID,
	text string,
) {
	t.Helper()
	if _, err := runtime.app.Ingress(ctx, text); err != nil {
		t.Fatal(err)
	}
	var recovered any
	func() {
		defer func() { recovered = recover() }()
		_ = runtime.app.ProcessResident(ctx, residentID)
	}()
	if recovered != runningAttemptCrashPanic {
		t.Fatalf("running-attempt fixture panic = %v, want %q", recovered, runningAttemptCrashPanic)
	}
}

type runtimeCancellationGenerator struct {
	started  chan struct{}
	returned chan struct{}
	once     sync.Once
}

func newRuntimeCancellationGenerator() *runtimeCancellationGenerator {
	return &runtimeCancellationGenerator{started: make(chan struct{}), returned: make(chan struct{})}
}

func (generator *runtimeCancellationGenerator) Stream(ctx context.Context, _ generation.Request, _ generation.DeltaSink) (generation.Result, error) {
	generator.once.Do(func() { close(generator.started) })
	<-ctx.Done()
	close(generator.returned)
	return generation.Result{}, &generation.ProviderError{Class: generation.ErrorCancelled, Cause: ctx.Err()}
}

var _ generation.Generator = (*runtimeCancellationGenerator)(nil)
