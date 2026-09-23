package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"path/filepath"
	"testing"

	"mahoroba.local/mahoroba/internal/canonical"
	"mahoroba.local/mahoroba/internal/cliresult"
	exportjsonlservice "mahoroba.local/mahoroba/internal/exportjsonl/service"
	"mahoroba.local/mahoroba/internal/hostlock"
)

func TestM7ExportJSONLExactCLIResultAndWarning(t *testing.T) {
	dataDir, output := t.TempDir(), filepath.Join(t.TempDir(), "export.jsonl")
	called := 0
	executor := func(_ context.Context, request exportjsonlservice.Request) (exportjsonlservice.Result, error) {
		called++
		if request.SourceDataDir != dataDir || request.DatabaseFilename != "mahoroba.db" || request.Output != output {
			t.Fatalf("request = %#v", request)
		}
		return exportjsonlservice.Result{
			ArtifactPath: output, FormatVersion: exportjsonlservice.FormatVersion,
			CapturedHead: exportjsonlservice.CapturedHead{}, RecordCount: 7, ByteCount: 4096,
		}, nil
	}
	var stdout, stderr bytes.Buffer
	exitCode := runExportJSONLWithExecutor(context.Background(), []string{
		"--output", output, "--data-dir", dataDir,
	}, &stdout, &stderr, executor)
	if exitCode != cliresult.ExitSuccess || called != 1 {
		t.Fatalf("exit=%d called=%d stderr=%s", exitCode, called, stderr.String())
	}
	var envelope map[string]any
	if err := json.Unmarshal(bytes.TrimSpace(stdout.Bytes()), &envelope); err != nil {
		t.Fatalf("stdout=%q: %v", stdout.String(), err)
	}
	result := envelope["result"].(map[string]any)
	if envelope["command"] != string(cliresult.CommandExportJSONL) ||
		envelope["outcome"] != string(cliresult.OutcomeSuccess) ||
		envelope["canonical_applied"] != false || result["record_count"] != "7" ||
		result["byte_count"] != "4096" || result["external_copy_notice"] != cliresult.ExternalCopyNotice {
		t.Fatalf("envelope = %#v", envelope)
	}
	if !bytes.Contains(stderr.Bytes(), []byte(`"warning_code":"runtime_unmanaged_copy"`)) ||
		bytes.Count(bytes.TrimSpace(stderr.Bytes()), []byte{'\n'}) != 0 {
		t.Fatalf("warning stderr = %q", stderr.String())
	}
}

func TestM7ExportJSONLErrorMatrixUsesClosedRenderer(t *testing.T) {
	dataDir, output := t.TempDir(), filepath.Join(t.TempDir(), "export.jsonl")
	tests := []struct {
		name   string
		err    error
		code   cliresult.ErrorCode
		stage  cliresult.Stage
		action bool
	}{
		{"host lock", hostlock.ErrLocked, cliresult.ErrorAdminLockBusy, cliresult.StagePreflight, false},
		{"target exists", exportjsonlservice.ErrArtifactTargetExists, cliresult.ErrorArtifactTargetExists, cliresult.StagePreflight, false},
		{"source", exportjsonlservice.ErrSourceUnavailable, cliresult.ErrorSourceUnavailable, cliresult.StagePreflight, false},
		{"schema", exportjsonlservice.ErrSchemaCoverage, cliresult.ErrorSourceUnavailable, cliresult.StagePreflight, false},
		{"integrity", exportjsonlservice.ErrContentIntegrity, cliresult.ErrorIntegrityFatal, cliresult.StagePreflight, false},
		{"artifact", exportjsonlservice.ErrArtifactIO, cliresult.ErrorArtifactIOFailed, cliresult.StagePublish, false},
		{"durability", exportjsonlservice.ErrDurabilityUnknown, cliresult.ErrorPublishDurabilityUnknown, cliresult.StagePublish, true},
		{"fallback", errors.New("closed internal failure"), cliresult.ErrorOperationFailed, cliresult.StagePreflight, false},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			var stdout, stderr bytes.Buffer
			exitCode := runExportJSONLWithExecutor(context.Background(), []string{
				"--output", output, "--data-dir", dataDir,
			}, &stdout, &stderr, func(context.Context, exportjsonlservice.Request) (exportjsonlservice.Result, error) {
				return exportjsonlservice.Result{}, test.err
			})
			if exitCode != cliresult.ExitOperational || stdout.Len() != 0 {
				t.Fatalf("exit=%d stdout=%q", exitCode, stdout.String())
			}
			var envelope map[string]any
			if err := json.Unmarshal(bytes.TrimSpace(stderr.Bytes()), &envelope); err != nil {
				t.Fatalf("stderr=%q: %v", stderr.String(), err)
			}
			result := envelope["result"].(map[string]any)
			if envelope["error_code"] != string(test.code) || result["error_stage"] != string(test.stage) {
				t.Fatalf("envelope = %#v", envelope)
			}
			actions := envelope["required_actions"].([]any)
			if (len(actions) == 1) != test.action {
				t.Fatalf("actions = %#v", actions)
			}
			if test.action {
				action := actions[0].(map[string]any)
				if action["code"] != string(cliresult.ActionRepairStaticDesign) || result["published"] != false || result["artifact_path"] != output {
					t.Fatalf("durability evidence = %#v / %#v", action, result)
				}
			}
		})
	}
}

func TestM7ExportJSONLUsageAndNoImportSurface(t *testing.T) {
	called := false
	var stdout, stderr bytes.Buffer
	exitCode := runExportJSONLWithExecutor(context.Background(), []string{"--output", "relative.jsonl"}, &stdout, &stderr,
		func(context.Context, exportjsonlservice.Request) (exportjsonlservice.Result, error) {
			called = true
			return exportjsonlservice.Result{}, nil
		})
	if exitCode != cliresult.ExitUsage || called || stdout.Len() != 0 ||
		!bytes.HasPrefix(stderr.Bytes(), []byte("usage: mahoroba export jsonl")) ||
		!bytes.Contains(stderr.Bytes(), []byte(`"error_code":"cli_usage"`)) {
		t.Fatalf("usage exit=%d called=%v stdout=%q stderr=%q", exitCode, called, stdout.String(), stderr.String())
	}
	stdout.Reset()
	stderr.Reset()
	abs := filepath.Join(t.TempDir(), "copy.jsonl")
	exitCode = runExportJSONLWithExecutor(context.Background(), []string{
		"--output", abs, "--output=" + abs,
	}, &stdout, &stderr, func(context.Context, exportjsonlservice.Request) (exportjsonlservice.Result, error) {
		called = true
		return exportjsonlservice.Result{}, nil
	})
	if exitCode != cliresult.ExitUsage || called {
		t.Fatalf("duplicate flag exit=%d called=%v stderr=%q", exitCode, called, stderr.String())
	}

	stdout.Reset()
	stderr.Reset()
	if exit := run(context.Background(), []string{"export", "import", "--input", "copy.jsonl"}, &stdout, &stderr); exit == 0 {
		t.Fatal("JSONL import surface unexpectedly exists")
	}
}

func TestM7ExportJSONLCLIHeadPreservesExactCanonicalValues(t *testing.T) {
	id, err := canonical.ParseID("00000000000000000000000001")
	if err != nil {
		t.Fatal(err)
	}
	head := exportJSONLCLIHead(exportjsonlservice.CapturedHead{
		Exists: true, CommitID: id, CommitSeq: 9, CommittedAt: 1_700_000_000_000_000,
		CommittedTZ: canonical.MustTimezone("Asia/Tokyo"),
	})
	if head.CommitID == nil || *head.CommitID != id.String() || head.CommitSeq == nil || *head.CommitSeq != "9" ||
		head.CommittedAt == nil || *head.CommittedAt != "1700000000000000" ||
		head.CommittedTZ == nil || *head.CommittedTZ != "Asia/Tokyo" {
		t.Fatalf("head = %#v", head)
	}
}
