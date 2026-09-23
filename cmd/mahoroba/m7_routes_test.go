package main

import (
	"bytes"
	"context"
	"strings"
	"testing"
)

func TestM7ProductionRoutesUseExactClosedCLIUsageEnvelopes(t *testing.T) {
	for _, testCase := range []struct {
		name      string
		arguments []string
		command   string
		usage     string
	}{
		{name: "healthcheck", arguments: []string{"healthcheck", "--unknown"}, command: "healthcheck", usage: "mahoroba healthcheck"},
		{name: "backup create", arguments: []string{"backup", "create"}, command: "backup.create", usage: "mahoroba backup create"},
		{name: "backup verify", arguments: []string{"backup", "verify"}, command: "backup.verify", usage: "mahoroba backup verify"},
		{name: "backup restore", arguments: []string{"backup", "restore"}, command: "backup.restore", usage: "mahoroba backup restore"},
		{name: "blob GC", arguments: []string{"blob", "gc"}, command: "blob.gc", usage: "mahoroba blob gc"},
		{name: "admin diagnostics", arguments: []string{"admin", "diagnostics"}, command: "admin.diagnostics", usage: "mahoroba admin diagnostics"},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			var stdout, stderr bytes.Buffer
			exit := run(context.Background(), testCase.arguments, &stdout, &stderr)
			if exit != 2 || stdout.Len() != 0 || !strings.Contains(stderr.String(), "usage: "+testCase.usage) ||
				!strings.Contains(stderr.String(), `"command":"`+testCase.command+`"`) ||
				!strings.Contains(stderr.String(), `"error_code":"cli_usage"`) {
				t.Fatalf("exit=%d stdout=%q stderr=%q", exit, stdout.String(), stderr.String())
			}
		})
	}
}
