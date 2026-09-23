package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"math"
	"testing"

	"mahoroba.local/mahoroba/internal/app"
	"mahoroba.local/mahoroba/internal/canonical"
	"mahoroba.local/mahoroba/internal/cliresult"
	"mahoroba.local/mahoroba/internal/config"
	"mahoroba.local/mahoroba/internal/domain"
	"mahoroba.local/mahoroba/internal/integrity"
)

type adminRecoveryEvidenceRepository struct {
	running       bool
	runningWork   domain.RunningAttempt
	mandatoryWork domain.MandatoryRecoveryWork
	commands      []string
	mutations     int
	failCommand   string
}

func (repository *adminRecoveryEvidenceRepository) RunningAttempts(
	context.Context, int,
) ([]domain.RunningAttempt, error) {
	if !repository.running {
		return nil, nil
	}
	return []domain.RunningAttempt{repository.runningWork}, nil
}

func (repository *adminRecoveryEvidenceRepository) DiscoverMandatoryRecoveryWork(
	context.Context, *domain.MandatoryRecoveryCursor, int,
) ([]domain.MandatoryRecoveryWork, *domain.MandatoryRecoveryCursor, error) {
	return []domain.MandatoryRecoveryWork{repository.mandatoryWork}, nil, nil
}

func (repository *adminRecoveryEvidenceRepository) ResolveMandatoryCancellationEnvelope(
	context.Context, domain.MandatoryRecoveryWork,
) (domain.CancellationEnvelopeResolution, error) {
	return domain.CancellationEnvelopeResolution{
		Generation: domain.PrepareGeneration{ResidentID: repository.runningWork.ResidentID},
	}, nil
}

func TestM7AdminRecoveryTerminalizeRunsRunningAndMandatoryWorkAndSortsCommits(t *testing.T) {
	repository := newAdminRecoveryEvidenceRepository(t, 2)
	executor := newAdminRecoveryEvidenceExecutor(t, repository, []int64{4, 12})

	var stdout, stderr bytes.Buffer
	exitCode := runAdminRecoveryTerminalizeWithExecutor(
		context.Background(), []string{"--data-dir", t.TempDir()}, &stdout, &stderr, executor,
	)
	if exitCode != cliresult.ExitSuccess {
		t.Fatalf("exit = %d, stderr=%s", exitCode, stderr.String())
	}
	envelope := decodeAdminRecoveryLine(t, stdout.Bytes())
	if envelope["command"] != string(cliresult.CommandAdminRecoveryTerminalize) ||
		envelope["outcome"] != string(cliresult.OutcomeSuccess) || envelope["canonical_applied"] != true {
		t.Fatalf("success envelope = %#v", envelope)
	}
	result := envelope["result"].(map[string]any)
	if result["terminalized_attempts"] != "1" || result["cancelled_mandatory_work"] != "1" {
		t.Fatalf("recovery result = %#v", result)
	}
	commits := envelope["canonical_commits"].([]any)
	if len(commits) != 2 || commits[0].(map[string]any)["commit_seq"] != "4" ||
		commits[1].(map[string]any)["commit_seq"] != "12" {
		t.Fatalf("commit_seq order = %#v", commits)
	}
	if got := repository.commands; len(got) != 2 ||
		got[0] != "FailGenerationAttempt" || got[1] != "CancelDialogue" {
		t.Fatalf("recovery commands = %v", got)
	}
	if repository.mutations != 2 || repository.running {
		t.Fatalf("recovery mutations/running = %d/%v", repository.mutations, repository.running)
	}
}

func TestM7AdminRecoveryTerminalizeOverflowRequiresStaticRepairWithoutMutation(t *testing.T) {
	repository := newAdminRecoveryEvidenceRepository(t, math.MaxInt64)
	runID := *repository.mandatoryWork.RunID
	executor := func(
		ctx context.Context, _ config.Config,
	) (adminRecoveryExecution, cliresult.ErrorCode, cliresult.Stage, error) {
		terminalizer, err := app.NewRecoveryTerminalizer(app.RecoveryTerminalizerOptions{
			Repository: repository, MandatoryRepository: repository, EnvelopeResolver: repository,
			IDs: canonical.NewSecureIDGenerator(),
			Submit: func(context.Context, canonical.Command) (canonical.CommandResult, error) {
				repository.mutations++
				return canonical.CommandResult{}, nil
			},
		})
		if err != nil {
			t.Fatal(err)
		}
		err = terminalizer.PreflightMandatory(ctx)
		return adminRecoveryExecution{}, cliresult.ErrorRecoveryAttemptOverflow,
			cliresult.StagePreflight, err
	}

	var stdout, stderr bytes.Buffer
	exitCode := runAdminRecoveryTerminalizeWithExecutor(
		context.Background(), []string{"--data-dir", t.TempDir()}, &stdout, &stderr, executor,
	)
	if exitCode != cliresult.ExitOperational || stdout.Len() != 0 {
		t.Fatalf("overflow exit/stdout = %d/%q", exitCode, stdout.String())
	}
	errorEnvelope := decodeAdminRecoveryLastLine(t, stderr.Bytes())
	if errorEnvelope["error_code"] != string(cliresult.ErrorRecoveryAttemptOverflow) ||
		errorEnvelope["canonical_applied"] != false {
		t.Fatalf("overflow envelope = %#v", errorEnvelope)
	}
	actions := errorEnvelope["required_actions"].([]any)
	if len(actions) != 1 ||
		actions[0].(map[string]any)["code"] != string(cliresult.ActionRepairStaticDesign) {
		t.Fatalf("overflow required_actions = %#v", actions)
	}
	targets := actions[0].(map[string]any)["target_ids"].([]any)
	if len(targets) != 1 || targets[0] != runID.String() {
		t.Fatalf("overflow action targets = %#v, want %s", targets, runID)
	}
	if repository.mutations != 0 || !repository.running || len(repository.commands) != 0 {
		t.Fatalf("overflow mutated recovery state: mutations=%d running=%v commands=%v",
			repository.mutations, repository.running, repository.commands)
	}
}

func TestM7AdminRecoveryTerminalizeRendersPartialAfterMidCommandFailure(t *testing.T) {
	repository := newAdminRecoveryEvidenceRepository(t, 2)
	repository.failCommand = "CancelDialogue"
	executor := newAdminRecoveryEvidenceExecutor(t, repository, []int64{7})

	var stdout, stderr bytes.Buffer
	exitCode := runAdminRecoveryTerminalizeWithExecutor(
		context.Background(), []string{"--data-dir", t.TempDir()}, &stdout, &stderr, executor,
	)
	if exitCode != cliresult.ExitOperational {
		t.Fatalf("partial exit = %d, stdout=%s stderr=%s", exitCode, stdout.String(), stderr.String())
	}
	partial := decodeAdminRecoveryLine(t, stdout.Bytes())
	if partial["outcome"] != string(cliresult.OutcomePartial) ||
		partial["error_code"] != string(cliresult.ErrorOperationPartial) ||
		partial["canonical_applied"] != true {
		t.Fatalf("partial stdout = %#v", partial)
	}
	result := partial["result"].(map[string]any)
	if result["terminalized_attempts"] != "1" || result["cancelled_mandatory_work"] != "0" {
		t.Fatalf("partial counts = %#v", result)
	}
	commits := partial["canonical_commits"].([]any)
	if len(commits) != 1 || commits[0].(map[string]any)["commit_seq"] != "7" {
		t.Fatalf("partial commits = %#v", commits)
	}
	rootFailure := decodeAdminRecoveryLastLine(t, stderr.Bytes())
	if rootFailure["outcome"] != string(cliresult.OutcomeError) ||
		rootFailure["error_code"] != string(cliresult.ErrorOperationFailed) ||
		rootFailure["canonical_applied"] != true {
		t.Fatalf("partial root failure = %#v", rootFailure)
	}
	if repository.mutations != 1 || repository.running || len(repository.commands) != 2 {
		t.Fatalf("partial state = mutations=%d running=%v commands=%v",
			repository.mutations, repository.running, repository.commands)
	}
}

func newAdminRecoveryEvidenceRepository(
	t *testing.T, mandatoryAttempt int64,
) *adminRecoveryEvidenceRepository {
	t.Helper()
	residentID := mustAdminRecoveryEvidenceID(t, "01J00000000000000000000032")
	runningRunID := mustAdminRecoveryEvidenceID(t, "01J00000000000000000000031")
	mandatoryRunID := mustAdminRecoveryEvidenceID(t, "01J00000000000000000000033")
	return &adminRecoveryEvidenceRepository{
		running: true,
		runningWork: domain.RunningAttempt{
			RunID: runningRunID, ResidentID: residentID, AttemptNo: 1,
		},
		mandatoryWork: domain.MandatoryRecoveryWork{
			Kind: domain.MandatoryRecoveryDialogue,
			SourceEvent: domain.Event{
				ID: mustAdminRecoveryEvidenceID(t, "01J00000000000000000000034"), ResidentID: residentID,
			},
			IdempotencyKey: "admin-recovery-evidence", RunID: &mandatoryRunID,
			AttemptNo: mandatoryAttempt, State: domain.WorkRetryPending,
			CancellationCode: "source_content_erased",
		},
	}
}

func newAdminRecoveryEvidenceExecutor(
	t *testing.T,
	repository *adminRecoveryEvidenceRepository,
	commitSequences []int64,
) adminRecoveryExecutor {
	t.Helper()
	return func(
		ctx context.Context, _ config.Config,
	) (adminRecoveryExecution, cliresult.ErrorCode, cliresult.Stage, error) {
		var execution adminRecoveryExecution
		ids := canonical.NewSecureIDGenerator()
		commits := make([]canonical.CommitMetadata, 0, len(commitSequences))
		terminalizer, err := app.NewRecoveryTerminalizer(app.RecoveryTerminalizerOptions{
			Repository: repository, MandatoryRepository: repository, EnvelopeResolver: repository,
			IDs: ids,
			Submit: func(_ context.Context, command canonical.Command) (canonical.CommandResult, error) {
				repository.commands = append(repository.commands, command.Name())
				if repository.failCommand == command.Name() {
					return canonical.CommandResult{}, errors.New("injected recovery command failure")
				}
				if len(commits) >= len(commitSequences) {
					t.Fatal("recovery submitted more commands than commit evidence")
				}
				commitID, idErr := ids.New()
				if idErr != nil {
					t.Fatal(idErr)
				}
				metadata := canonical.CommitMetadata{
					CommitID: commitID, CommitSeq: canonical.CommitSeq(commitSequences[len(commits)]),
					Scope: command.Scope(), CommittedAt: canonical.Instant(1_000 + commitSequences[len(commits)]),
					CommittedTZ: canonical.MustTimezone("UTC"),
				}
				commits = append(commits, metadata)
				repository.mutations++
				if command.Name() == "FailGenerationAttempt" {
					repository.running = false
				}
				return canonical.CommandResult{Commit: metadata}, nil
			},
		})
		if err != nil {
			t.Fatal(err)
		}
		if err := terminalizer.PreflightMandatory(ctx); err != nil {
			return execution, cliresult.ErrorRecoveryAttemptOverflow, cliresult.StagePreflight, err
		}
		running, err := terminalizer.TerminalizeRunning(ctx)
		execution.terminalized += running.TerminalizedAttempts
		mergeRecoveryMetadata(&execution.commits, running.CanonicalCommits,
			cliresult.EffectGenerationAttemptTerminalized)
		if err != nil {
			return execution, cliresult.ErrorOperationFailed, cliresult.StageCanonical, err
		}
		mandatory, err := terminalizer.TerminalizeMandatory(ctx)
		execution.cancelled += mandatory.CancelledMandatoryWork
		mergeRecoveryMetadata(&execution.commits, mandatory.CanonicalCommits,
			cliresult.EffectGenerationAttemptTerminalized, cliresult.EffectMandatoryWorkCancelled)
		if len(commits) != 0 {
			head := commits[0]
			for _, commit := range commits[1:] {
				if commit.CommitSeq > head.CommitSeq {
					head = commit
				}
			}
			execution.head = integrity.HeadMetadata{
				Head: canonical.Head{
					Exists: true, CommitSeq: head.CommitSeq, CommittedAt: head.CommittedAt,
				},
				CommitID: head.CommitID, CommittedTZ: head.CommittedTZ,
			}
		}
		// Prove the command boundary, rather than this fixture, owns deterministic
		// commit_seq ordering.
		for left, right := 0, len(execution.commits)-1; left < right; left, right = left+1, right-1 {
			execution.commits[left], execution.commits[right] = execution.commits[right], execution.commits[left]
		}
		if err != nil {
			return execution, cliresult.ErrorOperationFailed, cliresult.StageCanonical, err
		}
		return execution, "", "", nil
	}
}

func decodeAdminRecoveryLine(t *testing.T, data []byte) map[string]any {
	t.Helper()
	var decoded map[string]any
	if err := json.Unmarshal(bytes.TrimSpace(data), &decoded); err != nil {
		t.Fatalf("decode Admin recovery JSON %q: %v", data, err)
	}
	return decoded
}

func decodeAdminRecoveryLastLine(t *testing.T, data []byte) map[string]any {
	t.Helper()
	lines := bytes.Split(bytes.TrimSpace(data), []byte{'\n'})
	if len(lines) == 0 {
		t.Fatal("Admin recovery stderr was empty")
	}
	return decodeAdminRecoveryLine(t, lines[len(lines)-1])
}

func mustAdminRecoveryEvidenceID(t *testing.T, raw string) canonical.ID {
	t.Helper()
	id, err := canonical.ParseID(raw)
	if err != nil {
		t.Fatal(err)
	}
	return id
}
