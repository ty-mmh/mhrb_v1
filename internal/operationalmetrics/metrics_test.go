package operationalmetrics

import (
	"bytes"
	"context"
	"encoding/json"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestM7OperationalMetricsEmptySnapshotRetainsFixedHistogramSchema(t *testing.T) {
	snapshot := New().Snapshot()
	histograms := []DurationHistogram{
		snapshot.Writer.Latency,
		snapshot.Provider.Latency,
		snapshot.Projection.RebuildDuration,
	}
	for histogramIndex, histogram := range histograms {
		for boundIndex, bound := range histogram.BoundsMicroseconds {
			if bound != uint64(histogramBounds[boundIndex].Microseconds()) {
				t.Fatalf("histogram %d bound[%d] = %d", histogramIndex, boundIndex, bound)
			}
		}
	}
}

func TestM7OperationalMetricsSnapshotIsBoundedAndSanitized(t *testing.T) {
	metrics := New()
	for index := 0; index < 1_000; index++ {
		metrics.WriterAccepted()
		metrics.WriterQueued()
		metrics.WriterInFlight()
		metrics.WriterFinished(WriterOutcomeCommitted, time.Duration(index)*time.Microsecond)
		metrics.ProviderFinished(ProviderRetryableFailure, time.Duration(index)*time.Millisecond)
		metrics.ProjectionObserved(uint64(index), time.Duration(index)*time.Millisecond, true, index%7 == 0)
	}
	metrics.SetMandatoryWorkCounts(MandatoryWorkCounts{
		DialoguePending: 1, DialogueRunning: 2, DialogueRetryPending: 3,
		MemoryPending: 4, MemoryRunning: 5, MemoryRetryPending: 6,
	})
	metrics.SetStagingOrphanCount(7)
	metrics.SetFinalOrphanCount(8)
	metrics.OperationFinished(OperationBackup, OperationSucceeded)
	metrics.OperationFinished(OperationRestore, OperationPartial)
	metrics.OperationFinished(OperationErasure, OperationFailed)
	metrics.OperationFinished(OperationBlobGC, OperationSucceeded)

	snapshot := metrics.Snapshot()
	if snapshot.Writer.AcceptedTotal != 1_000 || snapshot.Writer.AcceptedCurrent != 0 ||
		snapshot.Writer.QueuedCurrent != 0 || snapshot.Writer.InFlightCurrent != 0 ||
		snapshot.Writer.CommittedTotal != 1_000 {
		t.Fatalf("writer snapshot = %+v", snapshot.Writer)
	}
	if snapshot.Provider.RetryableFailureTotal != 1_000 || snapshot.Projection.RebuildTotal != 1_000 ||
		snapshot.Projection.CurrentCommitLag != 999 || snapshot.Projection.MaximumCommitLag != 999 {
		t.Fatalf("provider/projection snapshot = %+v / %+v", snapshot.Provider, snapshot.Projection)
	}
	if snapshot.Mandatory.MemoryRetryPending != 6 || snapshot.BlobRecovery.FinalOrphanCount != 8 ||
		snapshot.Operations.Restore.Partial != 1 {
		t.Fatalf("remaining snapshot = %+v", snapshot)
	}
	var writerSamples, providerSamples, rebuildSamples uint64
	for _, count := range snapshot.Writer.Latency.BucketCounts {
		writerSamples += count
	}
	for _, count := range snapshot.Provider.Latency.BucketCounts {
		providerSamples += count
	}
	for _, count := range snapshot.Projection.RebuildDuration.BucketCounts {
		rebuildSamples += count
	}
	if writerSamples != 1_000 || providerSamples != 1_000 || rebuildSamples != 1_000 {
		t.Fatalf("histogram samples = %d/%d/%d", writerSamples, providerSamples, rebuildSamples)
	}
	for index, bound := range snapshot.Writer.Latency.BoundsMicroseconds {
		if bound != uint64(histogramBounds[index].Microseconds()) {
			t.Fatalf("fixed writer bound[%d] = %d", index, bound)
		}
	}
}

func TestCOVR03MandatoryWorkerFailureUsesClosedBoundedCounters(t *testing.T) {
	metrics := New()
	failure := MandatoryWorkerFailure{
		Phase: MandatoryWorkerPhaseDialoguePrepare, Class: MandatoryWorkerErrorWriterUnavailable,
		RescanExpected: true,
	}
	if err := failure.Validate(); err != nil {
		t.Fatal(err)
	}
	metrics.MandatoryWorkerFailed(failure)
	metrics.MandatoryWorkerFailed(MandatoryWorkerFailure{
		ResidentID: "01J00000000000000000000001", Phase: MandatoryWorkerPhaseDialogueScan,
		Class: MandatoryWorkerErrorIntegrityRejected,
	})
	// Identifier-bearing pre-Prepare observations are rejected rather than
	// leaking source identity through the otherwise closed metrics boundary.
	metrics.MandatoryWorkerFailed(MandatoryWorkerFailure{
		ResidentID: "01J00000000000000000000001", SourceEventID: "01J00000000000000000000002",
		Phase: MandatoryWorkerPhaseDialoguePrepare, Class: MandatoryWorkerErrorWriterUnavailable,
	})
	// Invalid or open-ended values are ignored instead of becoming labels.
	metrics.MandatoryWorkerFailed(MandatoryWorkerFailure{
		ResidentID: "raw resident", Phase: "free-form", Class: "sqlite: C:\\private\\secret",
	})

	snapshot := metrics.Snapshot().MandatoryWorkerFailures
	if snapshot.Total != 2 || snapshot.RescanExpected != 1 ||
		snapshot.ByPhase.DialoguePrepare != 1 || snapshot.ByPhase.DialogueScan != 1 ||
		snapshot.ByClass.WriterUnavailable != 1 || snapshot.ByClass.IntegrityRejected != 1 {
		t.Fatalf("mandatory worker failure snapshot = %+v", snapshot)
	}
	encoded, err := json.Marshal(metrics.Snapshot())
	if err != nil {
		t.Fatal(err)
	}
	encodedText := string(encoded)
	if !strings.Contains(encodedText, `"rescan_expected":1`) {
		t.Fatalf("metrics omitted rescan_expected wire field: %s", encoded)
	}
	if strings.Contains(encodedText, "retry_planned") {
		t.Fatalf("metrics retained retired retry_planned wire field: %s", encoded)
	}
	for _, forbidden := range []string{
		"01J00000000000000000000001", "01J00000000000000000000002",
		"raw resident", `C:\private\secret`,
	} {
		if strings.Contains(encodedText, forbidden) {
			t.Fatalf("metrics retained forbidden identifier/content %q: %s", forbidden, encoded)
		}
	}
}

func TestCOVR03MandatoryWorkerFailureCountersSaturate(t *testing.T) {
	metrics := New()
	metrics.state.mandatoryWorkerFailures.Total = ^uint64(0)
	metrics.state.mandatoryWorkerFailures.RescanExpected = ^uint64(0)
	metrics.state.mandatoryWorkerFailures.ByPhase.WorkerLaunch = ^uint64(0)
	metrics.state.mandatoryWorkerFailures.ByClass.InternalFailure = ^uint64(0)
	metrics.MandatoryWorkerFailed(MandatoryWorkerFailure{
		ResidentID: "01J00000000000000000000001", Phase: MandatoryWorkerPhaseWorkerLaunch,
		Class: MandatoryWorkerErrorInternalFailure, RescanExpected: true,
	})
	snapshot := metrics.Snapshot().MandatoryWorkerFailures
	if snapshot.Total != ^uint64(0) || snapshot.RescanExpected != ^uint64(0) ||
		snapshot.ByPhase.WorkerLaunch != ^uint64(0) || snapshot.ByClass.InternalFailure != ^uint64(0) {
		t.Fatalf("saturation failed: %+v", snapshot)
	}
}

func TestM7OperationalMetricsReporterPeriodicAndShutdownLogIsClosed(t *testing.T) {
	metrics := New()
	metrics.ProviderFinished(ProviderSucceeded, 2*time.Millisecond)
	metrics.SetStagingOrphanCount(3)
	var output bytes.Buffer
	frozen := time.Date(2026, 8, 24, 12, 34, 56, 789_000_000, time.UTC)
	reporter, err := NewReporter(metrics, ReporterOptions{
		Output: &output, Interval: 5 * time.Millisecond, Now: func() time.Time { return frozen },
	})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- reporter.Run(ctx) }()
	time.Sleep(18 * time.Millisecond)
	cancel()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	lines := strings.Split(strings.TrimSpace(output.String()), "\n")
	if len(lines) < 2 {
		t.Fatalf("structured snapshots = %d, want periodic plus shutdown: %q", len(lines), output.String())
	}
	for _, line := range lines {
		var envelope struct {
			Version              string   `json:"version"`
			CapturedAtUnixMicros string   `json:"captured_at_unix_micros"`
			Metrics              Snapshot `json:"metrics"`
		}
		if err := json.Unmarshal([]byte(line), &envelope); err != nil {
			t.Fatalf("invalid structured snapshot: %v: %q", err, line)
		}
		if envelope.Version != FormatVersion || envelope.CapturedAtUnixMicros != "1787574896789000" ||
			envelope.Metrics.Provider.SucceededTotal != 1 {
			t.Fatalf("envelope = %+v", envelope)
		}
	}
	for _, forbidden := range []string{
		`C:\\Users\\alice\\private`, "raw dialogue secret", "Bearer super-secret", "sqlite: disk I/O error",
	} {
		if strings.Contains(output.String(), forbidden) {
			t.Fatalf("structured log leaked forbidden data %q: %s", forbidden, output.String())
		}
	}
}

func TestM7OperationalMetricsConcurrentObserverIsRaceSafe(t *testing.T) {
	metrics := New()
	var group sync.WaitGroup
	for worker := 0; worker < 16; worker++ {
		group.Add(1)
		go func() {
			defer group.Done()
			for index := 0; index < 500; index++ {
				metrics.WriterAccepted()
				metrics.WriterQueued()
				metrics.WriterInFlight()
				metrics.WriterFinished(WriterOutcomeNoMutation, time.Microsecond)
				metrics.ProviderFinished(ProviderCancelled, time.Microsecond)
				metrics.OperationFinished(OperationBlobGC, OperationSucceeded)
				_ = metrics.Snapshot()
			}
		}()
	}
	group.Wait()
	snapshot := metrics.Snapshot()
	if snapshot.Writer.AcceptedTotal != 8_000 || snapshot.Writer.AcceptedCurrent != 0 ||
		snapshot.Writer.NoMutationTotal != 8_000 || snapshot.Provider.CancelledTotal != 8_000 ||
		snapshot.Operations.BlobGC.Succeeded != 8_000 {
		t.Fatalf("snapshot = %+v", snapshot)
	}
}
