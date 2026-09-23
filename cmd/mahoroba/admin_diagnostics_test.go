package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"

	"mahoroba.local/mahoroba/internal/cliresult"
	diagnosticsservice "mahoroba.local/mahoroba/internal/diagnostics"
)

func TestAdminDiagnosticsJSONAndTextBlackBoxCore(t *testing.T) {
	for _, format := range []string{"json", "text"} {
		t.Run(format, func(t *testing.T) {
			target := filepath.Join(t.TempDir(), "absent")
			var stdout, stderr bytes.Buffer
			exit := runAdminDiagnosticsWithExecutor(context.Background(), []string{
				"--all", "--format", format, "--data-dir", target,
			}, &stdout, &stderr, diagnosticsservice.Inspect)
			if exit != cliresult.ExitSuccess {
				if runtime.GOOS == "windows" && os.Getenv("CI") == "" &&
					strings.Contains(stderr.String(), string(cliresult.ErrorOperationFailed)) {
					t.Skip("secure namespace primitive unavailable in test sandbox")
				}
				t.Fatalf("exit=%d stdout=%s stderr=%s", exit, stdout.String(), stderr.String())
			}
			if !strings.Contains(stderr.String(), string(cliresult.WarningLocalAdminNotAttributed)) {
				t.Fatalf("warning stream = %s", stderr.String())
			}
			if strings.Contains(stdout.String(), target) {
				t.Fatalf("diagnostics leaked data path: %s", stdout.String())
			}
			if format == "text" {
				if !strings.HasPrefix(stdout.String(), "overall_state: unknown\ncaptured_head: unavailable\n") {
					t.Fatalf("text output = %s", stdout.String())
				}
				return
			}
			var envelope struct {
				FormatVersion string              `json:"format_version"`
				Command       cliresult.CommandID `json:"command"`
				Result        struct {
					Sections []json.RawMessage `json:"sections"`
				} `json:"result"`
			}
			if err := json.Unmarshal(stdout.Bytes(), &envelope); err != nil {
				t.Fatal(err)
			}
			if envelope.FormatVersion != cliresult.FormatVersion || envelope.Command != cliresult.CommandAdminDiagnostics ||
				len(envelope.Result.Sections) != 13 {
				t.Fatalf("JSON envelope = %+v", envelope)
			}
		})
	}
}

func TestAdminDiagnosticsRejectsNonExactScopeAndFlags(t *testing.T) {
	target := filepath.Join(t.TempDir(), "absent")
	for _, arguments := range [][]string{
		{"--data-dir", target},
		{"--all", "--resident", "01ARZ3NDEKTSV4RRFFQ69G5FAV", "--data-dir", target},
		{"--all", "--all", "--data-dir", target},
		{"--all", "--format", "yaml", "--data-dir", target},
		{"--resident", "not-an-id", "--data-dir", target},
	} {
		var stdout, stderr bytes.Buffer
		exit := runAdminDiagnosticsWithExecutor(context.Background(), arguments, &stdout, &stderr,
			func(context.Context, diagnosticsservice.Request) (*cliresult.DiagnosticsResult, error) {
				return nil, errors.New("executor must not run")
			})
		if exit != cliresult.ExitUsage || stdout.Len() != 0 || !strings.Contains(stderr.String(), "usage: mahoroba admin diagnostics") {
			t.Fatalf("args=%v exit=%d stdout=%s stderr=%s", arguments, exit, stdout.String(), stderr.String())
		}
	}
}
