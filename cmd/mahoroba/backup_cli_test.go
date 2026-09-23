package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"mahoroba.local/mahoroba/internal/backup"
	"mahoroba.local/mahoroba/internal/canonical"
	"mahoroba.local/mahoroba/internal/cliresult"
	"mahoroba.local/mahoroba/internal/readiness"
	"mahoroba.local/mahoroba/internal/restore"
)

func TestM7BackupCreateAndVerifyRenderExactTypedResults(t *testing.T) {
	output := filepath.Join(t.TempDir(), "bundle")
	headID, sequence, instant, timezone := "01ARZ3NDEKTSV4RRFFQ69G5FAV", "7", "1787220000000000", "UTC"
	head := backup.CapturedHead{Exists: true, CommitID: &headID, CommitSeq: &sequence, CommittedAtUnixMicros: &instant, CommittedTZ: &timezone}
	var stdout, stderr bytes.Buffer
	exit := runBackupCreateWithExecutor(context.Background(), []string{"--output", output}, &stdout, &stderr,
		func(_ context.Context, request backup.CreateRequest) (backup.Result, error) {
			if request.Output != output || request.CreatedBy.BinaryVersion == "" || request.CreatedBy.GoVersion == "" ||
				request.Observer == nil {
				t.Fatalf("request = %+v", request)
			}
			return backup.Result{ArtifactPath: output, FormatVersion: backup.FormatVersion, CapturedHead: head, FileCount: 3, ByteCount: 11}, nil
		})
	if exit != 0 || stderr.Len() != 0 || !strings.Contains(stdout.String(), `"file_count":"3"`) ||
		!strings.Contains(stdout.String(), `"captured_head":{"exists":true`) {
		t.Fatalf("create exit/output = %d %q %q", exit, stdout.String(), stderr.String())
	}

	stdout.Reset()
	exit = runBackupVerifyWithExecutor(context.Background(), []string{"--input", output}, &stdout, &stderr,
		func(context.Context, string) (backup.VerifiedBundle, error) {
			return backup.VerifiedBundle{Manifest: backup.Manifest{
				FormatVersion: backup.FormatVersion, CapturedHead: head,
				Projections: backup.Projections{Policy: "included_requested"},
			}, FileCount: 3, ByteCount: 11}, nil
		})
	if exit != 0 || stderr.Len() != 0 || !strings.Contains(stdout.String(), `"projections_included":true`) {
		t.Fatalf("verify exit/output = %d %q %q", exit, stdout.String(), stderr.String())
	}
}

func TestM7BackupRestoreRendersPublishedEffectsHandoffAndWarning(t *testing.T) {
	bundle, parent := t.TempDir(), t.TempDir()
	target := filepath.Join(parent, "restored")
	restoreID := mustBackupCLIParseID(t, "01J00000000000000000000091")
	commitID := mustBackupCLIParseID(t, "01J00000000000000000000092")
	residentID := mustBackupCLIParseID(t, "01J00000000000000000000093")
	scope, err := canonical.ResidentScope(residentID)
	if err != nil {
		t.Fatal(err)
	}
	head := readiness.Head{
		Exists: true, CommitID: commitID, CommitSeq: canonical.CommitSeq(7),
		CommittedAt: canonical.Instant(1787220000000000), CommittedTZ: canonical.MustTimezone("UTC"),
	}
	var stdout, stderr bytes.Buffer
	exit := runBackupRestoreWithExecutor(context.Background(), []string{
		"--input", bundle, "--target-data-dir", target,
	}, &stdout, &stderr, func(_ context.Context, request restore.Request) (restore.Result, error) {
		if request.BundleRoot != bundle || request.TargetDataDir != target || request.Observer == nil {
			t.Fatalf("request = %+v", request)
		}
		return restore.Result{
			DatabaseFilename: restore.DatabaseFilename, SourceHead: head, RestoredHead: head,
			TerminalizedAttempts: 2, CreatedIntegrityFindings: 3, ProjectionsRebuilt: 6,
			Published: true, CanonicalApplied: true, ServiceReady: false, RestoreID: restoreID,
			CanonicalCommits: []restore.Commit{{
				Metadata: canonical.CommitMetadata{
					CommitID: commitID, CommitSeq: canonical.CommitSeq(7), Scope: scope,
					CommittedAt: head.CommittedAt, CommittedTZ: head.CommittedTZ,
				},
				Disposition: restore.CommitCreated,
				Effects: restore.CommitEffects{
					ClaimStatusQuarantined: true, IntegrityFindingRecorded: true,
					MandatoryWorkCancelled: true,
				},
			}},
		}, nil
	})
	if exit != cliresult.ExitSuccess || !strings.Contains(stderr.String(), `"warning_code":"service_not_ready"`) {
		t.Fatalf("exit=%d stdout=%q stderr=%q", exit, stdout.String(), stderr.String())
	}
	var envelope struct {
		Command          string `json:"command"`
		CanonicalApplied bool   `json:"canonical_applied"`
		CanonicalCommits []struct {
			ResidentID  *string  `json:"resident_id"`
			Disposition string   `json:"disposition"`
			Effects     []string `json:"effects"`
		} `json:"canonical_commits"`
		RequiredActions []struct {
			Code                  string   `json:"code"`
			Argv                  []string `json:"argv"`
			PrerequisiteActionIDs []string `json:"prerequisite_action_ids"`
		} `json:"required_actions"`
		Result struct {
			TerminalizedAttempts string `json:"terminalized_attempts"`
			FindingsCreated      string `json:"findings_created"`
			ProjectionsRebuilt   string `json:"projections_rebuilt"`
			Published            bool   `json:"published"`
			ServiceReady         bool   `json:"service_ready"`
		} `json:"result"`
	}
	if err := json.Unmarshal(bytes.TrimSpace(stdout.Bytes()), &envelope); err != nil {
		t.Fatalf("stdout=%q: %v", stdout.String(), err)
	}
	if envelope.Command != string(cliresult.CommandBackupRestore) || !envelope.CanonicalApplied ||
		len(envelope.CanonicalCommits) != 1 || envelope.CanonicalCommits[0].ResidentID == nil ||
		*envelope.CanonicalCommits[0].ResidentID != residentID.String() ||
		envelope.CanonicalCommits[0].Disposition != string(cliresult.DispositionCreated) ||
		strings.Join(envelope.CanonicalCommits[0].Effects, ",") !=
			"claim_status_quarantined,integrity_finding_recorded,mandatory_work_cancelled" {
		t.Fatalf("canonical evidence = %+v", envelope)
	}
	if len(envelope.RequiredActions) != 1 || envelope.RequiredActions[0].Code != string(cliresult.ActionStartService) ||
		strings.Join(envelope.RequiredActions[0].Argv, "\x00") !=
			strings.Join([]string{"mahoroba", "serve", "--data-dir", target}, "\x00") ||
		len(envelope.RequiredActions[0].PrerequisiteActionIDs) != 0 {
		t.Fatalf("handoff = %+v", envelope.RequiredActions)
	}
	if envelope.Result.TerminalizedAttempts != "2" || envelope.Result.FindingsCreated != "3" ||
		envelope.Result.ProjectionsRebuilt != "6" || !envelope.Result.Published || envelope.Result.ServiceReady {
		t.Fatalf("result = %+v", envelope.Result)
	}
}

func TestM7BackupRestoreConfigMismatchRequiresRepairBeforePreservedHandoff(t *testing.T) {
	bundle, parent := t.TempDir(), t.TempDir()
	target := filepath.Join(parent, "restored")
	configPath := filepath.Join(t.TempDir(), "restore.toml")
	if err := os.WriteFile(configPath, []byte("[database]\nfilename = \"other.db\"\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	restoreID := mustBackupCLIParseID(t, "01J00000000000000000000094")
	var stdout, stderr bytes.Buffer
	exit := runBackupRestoreWithExecutor(context.Background(), []string{
		"--input", bundle, "--target-data-dir", target, "--config", configPath,
	}, &stdout, &stderr, func(context.Context, restore.Request) (restore.Result, error) {
		return restore.Result{
			DatabaseFilename: restore.DatabaseFilename, Published: true,
			RestoreID: restoreID, ServiceReady: true,
		}, nil
	})
	if exit != cliresult.ExitSuccess || !strings.Contains(stderr.String(), `"warning_code":"service_not_ready"`) {
		t.Fatalf("exit=%d stdout=%q stderr=%q", exit, stdout.String(), stderr.String())
	}
	var envelope struct {
		RequiredActions []struct {
			ActionID              string   `json:"action_id"`
			Code                  string   `json:"code"`
			Argv                  []string `json:"argv"`
			PrerequisiteActionIDs []string `json:"prerequisite_action_ids"`
		} `json:"required_actions"`
	}
	if err := json.Unmarshal(bytes.TrimSpace(stdout.Bytes()), &envelope); err != nil {
		t.Fatal(err)
	}
	if len(envelope.RequiredActions) != 2 ||
		envelope.RequiredActions[0].Code != string(cliresult.ActionRepairConfig) ||
		envelope.RequiredActions[1].Code != string(cliresult.ActionStartService) ||
		strings.Join(envelope.RequiredActions[1].Argv, "\x00") != strings.Join([]string{
			"mahoroba", "serve", "--config", filepath.Clean(configPath), "--data-dir", target,
		}, "\x00") || len(envelope.RequiredActions[1].PrerequisiteActionIDs) != 1 ||
		envelope.RequiredActions[1].PrerequisiteActionIDs[0] != envelope.RequiredActions[0].ActionID {
		t.Fatalf("required actions = %+v", envelope.RequiredActions)
	}
}

func TestM7BackupRestoreClosedFailureAndPublishedPartial(t *testing.T) {
	bundle, parent := t.TempDir(), t.TempDir()
	target := filepath.Join(parent, "restored")
	restoreID := mustBackupCLIParseID(t, "01J00000000000000000000095")
	for _, test := range []struct {
		name       string
		result     restore.Result
		err        error
		wantCode   string
		wantExit   int
		wantStdout bool
	}{
		{name: "target exists", err: restore.ErrTargetExists, wantCode: "restore_target_exists", wantExit: 1},
		{name: "pre-publish durability", result: restore.Result{RestoreID: restoreID}, err: restore.ErrDurabilityUnknown,
			wantCode: "publish_durability_unknown", wantExit: 1},
		{name: "published durability", result: restore.Result{
			DatabaseFilename: restore.DatabaseFilename, Published: true, RestoreID: restoreID, ServiceReady: true,
		}, err: restore.ErrDurabilityUnknown, wantCode: "operation_partial", wantExit: 1, wantStdout: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			var stdout, stderr bytes.Buffer
			exit := runBackupRestoreWithExecutor(context.Background(), []string{
				"--input", bundle, "--target-data-dir", target,
			}, &stdout, &stderr, func(context.Context, restore.Request) (restore.Result, error) {
				return test.result, test.err
			})
			if exit != test.wantExit || (stdout.Len() != 0) != test.wantStdout ||
				!strings.Contains(func() string {
					if test.wantStdout {
						return stdout.String()
					}
					return stderr.String()
				}(), `"error_code":"`+test.wantCode+`"`) {
				t.Fatalf("exit=%d stdout=%q stderr=%q", exit, stdout.String(), stderr.String())
			}
			if test.name == "pre-publish durability" &&
				(!strings.Contains(stderr.String(), `"published":false`) ||
					!strings.Contains(stderr.String(), `"code":"repair_static_design"`)) {
				t.Fatalf("durability evidence = %q", stderr.String())
			}
		})
	}
}

func mustBackupCLIParseID(t *testing.T, raw string) canonical.ID {
	t.Helper()
	id, err := canonical.ParseID(raw)
	if err != nil {
		t.Fatal(err)
	}
	return id
}

func TestM7BackupCLIRejectsInvalidShapeAndClassifiesFailures(t *testing.T) {
	for _, arguments := range [][]string{{}, {"--output", "relative"}, {"--output", `C:\a`, "--output", `C:\b`}, {"--unknown"}} {
		var stdout, stderr bytes.Buffer
		if exit := runBackupCreateWithExecutor(context.Background(), arguments, &stdout, &stderr, nil); exit != 2 || stdout.Len() != 0 {
			t.Fatalf("arguments %q exit/output = %d %q %q", arguments, exit, stdout.String(), stderr.String())
		}
	}
	input := filepath.Join(t.TempDir(), "bundle")
	var stdout, stderr bytes.Buffer
	exit := runBackupVerifyWithExecutor(context.Background(), []string{"--input", input}, &stdout, &stderr,
		func(context.Context, string) (backup.VerifiedBundle, error) {
			return backup.VerifiedBundle{}, backup.ErrInvalidBundle
		})
	if exit != 1 || stdout.Len() != 0 || !strings.Contains(stderr.String(), `"error_code":"backup_invalid"`) {
		t.Fatalf("verify failure = %d %q %q", exit, stdout.String(), stderr.String())
	}
	if code, stage := classifyBackupError(errors.Join(errors.New("context"), backup.ErrDurabilityUnknown)); code != "publish_durability_unknown" || stage != "publish" {
		t.Fatalf("durability classification = %q/%q", code, stage)
	}
}
