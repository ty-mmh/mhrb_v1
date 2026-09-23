package main

import (
	"bytes"
	"log/slog"
	"strings"
	"testing"

	"mahoroba.local/mahoroba/internal/operationalmetrics"
)

func TestCOVR03ServeOperationalObserverLogsOnlySanitizedWorkerFields(t *testing.T) {
	metrics := operationalmetrics.New()
	var output bytes.Buffer
	observer := serveOperationalObserver{
		metrics: metrics,
		logger:  slog.New(slog.NewTextHandler(&output, nil)),
	}
	failure := operationalmetrics.MandatoryWorkerFailure{
		Phase:          operationalmetrics.MandatoryWorkerPhaseDialoguePrepare,
		Class:          operationalmetrics.MandatoryWorkerErrorWriterUnavailable,
		RescanExpected: true,
	}
	observer.MandatoryWorkerFailed(failure)

	snapshot := metrics.Snapshot().MandatoryWorkerFailures
	if snapshot.Total != 1 || snapshot.RescanExpected != 1 ||
		snapshot.ByPhase.DialoguePrepare != 1 || snapshot.ByClass.WriterUnavailable != 1 {
		t.Fatalf("snapshot = %+v", snapshot)
	}
	logLine := output.String()
	for _, required := range []string{
		string(failure.Phase), string(failure.Class),
		"rescan_expected=true",
	} {
		if !strings.Contains(logLine, required) {
			t.Fatalf("sanitized log is missing %q: %s", required, logLine)
		}
	}
	for _, forbidden := range []string{
		"resident_id", "source_event_id", "01J00000000000000000000001", "01J00000000000000000000002",
		"raw dialogue secret", "sqlite: disk I/O error", `C:\Users\alice\private`, "retry_planned",
	} {
		if strings.Contains(logLine, forbidden) {
			t.Fatalf("sanitized log leaked %q: %s", forbidden, logLine)
		}
	}
}
