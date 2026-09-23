// Package operationalmetrics provides the process-local, bounded observer
// required by the M7 operational surface.  Its mutation API accepts only
// closed enums, counters, and durations: content, filesystem paths, provider
// bodies, credentials, and arbitrary error strings cannot enter a snapshot.
package operationalmetrics

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"sync"
	"time"
)

const FormatVersion = "mahoroba-operational-metrics-v2"

const DefaultReportInterval = 60 * time.Second

var histogramBounds = [...]time.Duration{
	time.Millisecond,
	5 * time.Millisecond,
	25 * time.Millisecond,
	100 * time.Millisecond,
	500 * time.Millisecond,
	2 * time.Second,
	10 * time.Second,
	60 * time.Second,
}

// WriterOutcome is the closed terminal set for one accepted Writer request.
type WriterOutcome uint8

const (
	WriterOutcomeCommitted WriterOutcome = iota + 1
	WriterOutcomeNoMutation
	WriterOutcomeFailed
)

// ProviderTerminal is deliberately coarser than persisted provider errors.
// It is sufficient for live operations and cannot carry response data.
type ProviderTerminal uint8

const (
	ProviderSucceeded ProviderTerminal = iota + 1
	ProviderCancelled
	ProviderRetryableFailure
	ProviderNonretryableFailure
)

type Operation uint8

const (
	OperationBackup Operation = iota + 1
	OperationRestore
	OperationErasure
	OperationBlobGC
)

type OperationResult uint8

const (
	OperationSucceeded OperationResult = iota + 1
	OperationPartial
	OperationFailed
)

// MandatoryWorkCounts is a gauge, not an accumulating counter.  It mirrors
// the closed mandatory dialogue/extraction state matrix.
type MandatoryWorkCounts struct {
	DialoguePending      uint64 `json:"dialogue_pending"`
	DialogueRunning      uint64 `json:"dialogue_running"`
	DialogueRetryPending uint64 `json:"dialogue_retry_pending"`
	MemoryPending        uint64 `json:"memory_pending"`
	MemoryRunning        uint64 `json:"memory_running"`
	MemoryRetryPending   uint64 `json:"memory_retry_pending"`
}

// MandatoryWorkerPhase is the closed set of mandatory-worker boundaries at
// which an asynchronous failure may become otherwise invisible to callers.
// String values are stable operational wire values, not free-form labels.
type MandatoryWorkerPhase string

const (
	MandatoryWorkerPhaseWorkerLaunch        MandatoryWorkerPhase = "worker_launch"
	MandatoryWorkerPhaseDialogueScan        MandatoryWorkerPhase = "dialogue_scan"
	MandatoryWorkerPhaseProjectionReconcile MandatoryWorkerPhase = "projection_reconcile"
	MandatoryWorkerPhaseDialogueAssembly    MandatoryWorkerPhase = "dialogue_assembly"
	MandatoryWorkerPhaseDialoguePrepare     MandatoryWorkerPhase = "dialogue_prepare"
	MandatoryWorkerPhaseFrozenRunRead       MandatoryWorkerPhase = "frozen_run_read"
	MandatoryWorkerPhaseAttemptTransition   MandatoryWorkerPhase = "attempt_transition"
	MandatoryWorkerPhaseDialogueLanding     MandatoryWorkerPhase = "dialogue_landing"
	MandatoryWorkerPhaseMemoryExtraction    MandatoryWorkerPhase = "memory_extraction"
)

// MandatoryWorkerErrorClass is deliberately coarser than an error string.
// It carries enough information for operations without accepting content,
// database text, provider bodies, or filesystem paths.
type MandatoryWorkerErrorClass string

const (
	MandatoryWorkerErrorContextCancelled      MandatoryWorkerErrorClass = "context_cancelled"
	MandatoryWorkerErrorDeadlineExceeded      MandatoryWorkerErrorClass = "deadline_exceeded"
	MandatoryWorkerErrorAssemblyTargetChanged MandatoryWorkerErrorClass = "assembly_target_changed"
	MandatoryWorkerErrorIntegrityRejected     MandatoryWorkerErrorClass = "integrity_rejected"
	MandatoryWorkerErrorWriterUnavailable     MandatoryWorkerErrorClass = "writer_unavailable"
	MandatoryWorkerErrorDependencyUnavailable MandatoryWorkerErrorClass = "dependency_unavailable"
	MandatoryWorkerErrorInternalFailure       MandatoryWorkerErrorClass = "internal_failure"
)

// MandatoryWorkerFailure is a transient, sanitized observation. COV-5/6
// pre-Prepare phases forbid identifiers entirely. Other phases retain the
// older COVR-03 contract: the resident identifier is required and the source
// event identifier is optional. Metrics never retains either identifier.
type MandatoryWorkerFailure struct {
	ResidentID    string
	SourceEventID string
	Phase         MandatoryWorkerPhase
	Class         MandatoryWorkerErrorClass
	// RescanExpected says only that a later Canonical scan may rediscover the
	// obligation; it is not retry eligibility or a scheduled-retry promise.
	RescanExpected bool
}

func (failure MandatoryWorkerFailure) Validate() error {
	if !validMandatoryWorkerPhase(failure.Phase) {
		return errors.New("operational metrics: mandatory worker phase is invalid")
	}
	if !validMandatoryWorkerErrorClass(failure.Class) {
		return errors.New("operational metrics: mandatory worker error class is invalid")
	}
	if mandatoryWorkerPhaseForbidsIdentifiers(failure.Phase) {
		if failure.ResidentID != "" || failure.SourceEventID != "" {
			return errors.New("operational metrics: pre-Prepare mandatory worker failure must not contain identifiers")
		}
		return nil
	}
	if !validCanonicalULID(failure.ResidentID) {
		return errors.New("operational metrics: mandatory worker resident ID is invalid")
	}
	if failure.SourceEventID != "" && !validCanonicalULID(failure.SourceEventID) {
		return errors.New("operational metrics: mandatory worker source event ID is invalid")
	}
	return nil
}

func mandatoryWorkerPhaseForbidsIdentifiers(phase MandatoryWorkerPhase) bool {
	switch phase {
	case MandatoryWorkerPhaseProjectionReconcile,
		MandatoryWorkerPhaseDialogueAssembly,
		MandatoryWorkerPhaseDialoguePrepare:
		return true
	default:
		return false
	}
}

type MandatoryWorkerPhaseCounters struct {
	WorkerLaunch        uint64 `json:"worker_launch"`
	DialogueScan        uint64 `json:"dialogue_scan"`
	ProjectionReconcile uint64 `json:"projection_reconcile"`
	DialogueAssembly    uint64 `json:"dialogue_assembly"`
	DialoguePrepare     uint64 `json:"dialogue_prepare"`
	FrozenRunRead       uint64 `json:"frozen_run_read"`
	AttemptTransition   uint64 `json:"attempt_transition"`
	DialogueLanding     uint64 `json:"dialogue_landing"`
	MemoryExtraction    uint64 `json:"memory_extraction"`
}

type MandatoryWorkerErrorCounters struct {
	ContextCancelled      uint64 `json:"context_cancelled"`
	DeadlineExceeded      uint64 `json:"deadline_exceeded"`
	AssemblyTargetChanged uint64 `json:"assembly_target_changed"`
	IntegrityRejected     uint64 `json:"integrity_rejected"`
	WriterUnavailable     uint64 `json:"writer_unavailable"`
	DependencyUnavailable uint64 `json:"dependency_unavailable"`
	InternalFailure       uint64 `json:"internal_failure"`
}

type MandatoryWorkerFailureSnapshot struct {
	Total          uint64                       `json:"total"`
	RescanExpected uint64                       `json:"rescan_expected"`
	ByPhase        MandatoryWorkerPhaseCounters `json:"by_phase"`
	ByClass        MandatoryWorkerErrorCounters `json:"by_class"`
}

// DurationHistogram has fixed buckets, so memory use is independent of the
// number or latency of observed operations.
type DurationHistogram struct {
	BoundsMicroseconds [8]uint64 `json:"bounds_microseconds"`
	BucketCounts       [9]uint64 `json:"bucket_counts"`
}

type WriterSnapshot struct {
	AcceptedTotal   uint64            `json:"accepted_total"`
	AcceptedCurrent uint64            `json:"accepted_current"`
	QueuedCurrent   uint64            `json:"queued_current"`
	InFlightCurrent uint64            `json:"in_flight_current"`
	CommittedTotal  uint64            `json:"committed_total"`
	NoMutationTotal uint64            `json:"no_mutation_total"`
	FailedTotal     uint64            `json:"failed_total"`
	Latency         DurationHistogram `json:"latency"`
}

type ProviderSnapshot struct {
	SucceededTotal           uint64            `json:"succeeded_total"`
	CancelledTotal           uint64            `json:"cancelled_total"`
	RetryableFailureTotal    uint64            `json:"retryable_failure_total"`
	NonretryableFailureTotal uint64            `json:"nonretryable_failure_total"`
	Latency                  DurationHistogram `json:"request_latency"`
}

type ProjectionSnapshot struct {
	ObservedTotal    uint64            `json:"observed_total"`
	CurrentCommitLag uint64            `json:"current_commit_lag"`
	MaximumCommitLag uint64            `json:"maximum_commit_lag"`
	RebuildTotal     uint64            `json:"rebuild_total"`
	FailureTotal     uint64            `json:"failure_total"`
	RebuildDuration  DurationHistogram `json:"rebuild_duration"`
}

type BlobRecoverySnapshot struct {
	StagingOrphanCount uint64 `json:"staging_orphan_count"`
	FinalOrphanCount   uint64 `json:"final_orphan_count"`
}

type OperationCounters struct {
	Succeeded uint64 `json:"succeeded"`
	Partial   uint64 `json:"partial"`
	Failed    uint64 `json:"failed"`
}

type OperationsSnapshot struct {
	Backup  OperationCounters `json:"backup"`
	Restore OperationCounters `json:"restore"`
	Erasure OperationCounters `json:"erasure"`
	BlobGC  OperationCounters `json:"blob_gc"`
}

type Snapshot struct {
	Writer                  WriterSnapshot                 `json:"writer"`
	Provider                ProviderSnapshot               `json:"provider"`
	Projection              ProjectionSnapshot             `json:"projection"`
	Mandatory               MandatoryWorkCounts            `json:"mandatory_work"`
	MandatoryWorkerFailures MandatoryWorkerFailureSnapshot `json:"mandatory_worker_failures"`
	BlobRecovery            BlobRecoverySnapshot           `json:"blob_recovery"`
	Operations              OperationsSnapshot             `json:"operations"`
}

type state struct {
	writer                  WriterSnapshot
	provider                ProviderSnapshot
	projection              ProjectionSnapshot
	mandatory               MandatoryWorkCounts
	mandatoryWorkerFailures MandatoryWorkerFailureSnapshot
	blobRecovery            BlobRecoverySnapshot
	operations              OperationsSnapshot
}

// Metrics is safe for concurrent observers.  One mutex keeps snapshots
// internally consistent and bounds state to the fixed struct above.
type Metrics struct {
	mu    sync.Mutex
	state state
}

func New() *Metrics {
	metrics := &Metrics{}
	initializeDurationHistogram(&metrics.state.writer.Latency)
	initializeDurationHistogram(&metrics.state.provider.Latency)
	initializeDurationHistogram(&metrics.state.projection.RebuildDuration)
	return metrics
}

func (metrics *Metrics) WriterAccepted() {
	if metrics == nil {
		return
	}
	metrics.mu.Lock()
	metrics.state.writer.AcceptedTotal = saturatingIncrement(metrics.state.writer.AcceptedTotal)
	metrics.state.writer.AcceptedCurrent = saturatingIncrement(metrics.state.writer.AcceptedCurrent)
	metrics.mu.Unlock()
}

func (metrics *Metrics) WriterQueued() {
	if metrics == nil {
		return
	}
	metrics.mu.Lock()
	metrics.state.writer.QueuedCurrent = saturatingIncrement(metrics.state.writer.QueuedCurrent)
	metrics.mu.Unlock()
}

func (metrics *Metrics) WriterInFlight() {
	if metrics == nil {
		return
	}
	metrics.mu.Lock()
	metrics.state.writer.QueuedCurrent = saturatingDecrement(metrics.state.writer.QueuedCurrent)
	metrics.state.writer.InFlightCurrent = saturatingIncrement(metrics.state.writer.InFlightCurrent)
	metrics.mu.Unlock()
}

func (metrics *Metrics) WriterFinished(outcome WriterOutcome, latency time.Duration) {
	if metrics == nil || !validWriterOutcome(outcome) {
		return
	}
	metrics.mu.Lock()
	metrics.state.writer.AcceptedCurrent = saturatingDecrement(metrics.state.writer.AcceptedCurrent)
	if metrics.state.writer.InFlightCurrent > 0 {
		metrics.state.writer.InFlightCurrent--
	} else {
		metrics.state.writer.QueuedCurrent = saturatingDecrement(metrics.state.writer.QueuedCurrent)
	}
	switch outcome {
	case WriterOutcomeCommitted:
		metrics.state.writer.CommittedTotal = saturatingIncrement(metrics.state.writer.CommittedTotal)
	case WriterOutcomeNoMutation:
		metrics.state.writer.NoMutationTotal = saturatingIncrement(metrics.state.writer.NoMutationTotal)
	case WriterOutcomeFailed:
		metrics.state.writer.FailedTotal = saturatingIncrement(metrics.state.writer.FailedTotal)
	}
	recordDuration(&metrics.state.writer.Latency, latency)
	metrics.mu.Unlock()
}

func (metrics *Metrics) ProviderFinished(terminal ProviderTerminal, latency time.Duration) {
	if metrics == nil || !validProviderTerminal(terminal) {
		return
	}
	metrics.mu.Lock()
	switch terminal {
	case ProviderSucceeded:
		metrics.state.provider.SucceededTotal = saturatingIncrement(metrics.state.provider.SucceededTotal)
	case ProviderCancelled:
		metrics.state.provider.CancelledTotal = saturatingIncrement(metrics.state.provider.CancelledTotal)
	case ProviderRetryableFailure:
		metrics.state.provider.RetryableFailureTotal = saturatingIncrement(metrics.state.provider.RetryableFailureTotal)
	case ProviderNonretryableFailure:
		metrics.state.provider.NonretryableFailureTotal = saturatingIncrement(metrics.state.provider.NonretryableFailureTotal)
	}
	recordDuration(&metrics.state.provider.Latency, latency)
	metrics.mu.Unlock()
}

func (metrics *Metrics) ProjectionObserved(commitLag uint64, rebuildDuration time.Duration, rebuilt, failed bool) {
	if metrics == nil {
		return
	}
	metrics.mu.Lock()
	metrics.state.projection.ObservedTotal = saturatingIncrement(metrics.state.projection.ObservedTotal)
	metrics.state.projection.CurrentCommitLag = commitLag
	if commitLag > metrics.state.projection.MaximumCommitLag {
		metrics.state.projection.MaximumCommitLag = commitLag
	}
	if rebuilt {
		metrics.state.projection.RebuildTotal = saturatingIncrement(metrics.state.projection.RebuildTotal)
		recordDuration(&metrics.state.projection.RebuildDuration, rebuildDuration)
	}
	if failed {
		metrics.state.projection.FailureTotal = saturatingIncrement(metrics.state.projection.FailureTotal)
	}
	metrics.mu.Unlock()
}

func (metrics *Metrics) SetMandatoryWorkCounts(counts MandatoryWorkCounts) {
	if metrics == nil {
		return
	}
	metrics.mu.Lock()
	metrics.state.mandatory = counts
	metrics.mu.Unlock()
}

func (metrics *Metrics) MandatoryWorkerFailed(failure MandatoryWorkerFailure) {
	if metrics == nil || failure.Validate() != nil {
		return
	}
	metrics.mu.Lock()
	snapshot := &metrics.state.mandatoryWorkerFailures
	snapshot.Total = saturatingIncrement(snapshot.Total)
	if failure.RescanExpected {
		snapshot.RescanExpected = saturatingIncrement(snapshot.RescanExpected)
	}
	recordMandatoryWorkerPhase(&snapshot.ByPhase, failure.Phase)
	recordMandatoryWorkerErrorClass(&snapshot.ByClass, failure.Class)
	metrics.mu.Unlock()
}

func (metrics *Metrics) SetStagingOrphanCount(count uint64) {
	if metrics == nil {
		return
	}
	metrics.mu.Lock()
	metrics.state.blobRecovery.StagingOrphanCount = count
	metrics.mu.Unlock()
}

func (metrics *Metrics) SetFinalOrphanCount(count uint64) {
	if metrics == nil {
		return
	}
	metrics.mu.Lock()
	metrics.state.blobRecovery.FinalOrphanCount = count
	metrics.mu.Unlock()
}

func (metrics *Metrics) OperationFinished(operation Operation, result OperationResult) {
	if metrics == nil || !validOperation(operation) || !validOperationResult(result) {
		return
	}
	metrics.mu.Lock()
	var counters *OperationCounters
	switch operation {
	case OperationBackup:
		counters = &metrics.state.operations.Backup
	case OperationRestore:
		counters = &metrics.state.operations.Restore
	case OperationErasure:
		counters = &metrics.state.operations.Erasure
	case OperationBlobGC:
		counters = &metrics.state.operations.BlobGC
	}
	switch result {
	case OperationSucceeded:
		counters.Succeeded = saturatingIncrement(counters.Succeeded)
	case OperationPartial:
		counters.Partial = saturatingIncrement(counters.Partial)
	case OperationFailed:
		counters.Failed = saturatingIncrement(counters.Failed)
	}
	metrics.mu.Unlock()
}

func (metrics *Metrics) Snapshot() Snapshot {
	if metrics == nil {
		return Snapshot{}
	}
	metrics.mu.Lock()
	defer metrics.mu.Unlock()
	return Snapshot{
		Writer: metrics.state.writer, Provider: metrics.state.provider,
		Projection: metrics.state.projection, Mandatory: metrics.state.mandatory,
		MandatoryWorkerFailures: metrics.state.mandatoryWorkerFailures,
		BlobRecovery:            metrics.state.blobRecovery, Operations: metrics.state.operations,
	}
}

func recordMandatoryWorkerPhase(counters *MandatoryWorkerPhaseCounters, phase MandatoryWorkerPhase) {
	var value *uint64
	switch phase {
	case MandatoryWorkerPhaseWorkerLaunch:
		value = &counters.WorkerLaunch
	case MandatoryWorkerPhaseDialogueScan:
		value = &counters.DialogueScan
	case MandatoryWorkerPhaseProjectionReconcile:
		value = &counters.ProjectionReconcile
	case MandatoryWorkerPhaseDialogueAssembly:
		value = &counters.DialogueAssembly
	case MandatoryWorkerPhaseDialoguePrepare:
		value = &counters.DialoguePrepare
	case MandatoryWorkerPhaseFrozenRunRead:
		value = &counters.FrozenRunRead
	case MandatoryWorkerPhaseAttemptTransition:
		value = &counters.AttemptTransition
	case MandatoryWorkerPhaseDialogueLanding:
		value = &counters.DialogueLanding
	case MandatoryWorkerPhaseMemoryExtraction:
		value = &counters.MemoryExtraction
	}
	*value = saturatingIncrement(*value)
}

func recordMandatoryWorkerErrorClass(counters *MandatoryWorkerErrorCounters, class MandatoryWorkerErrorClass) {
	var value *uint64
	switch class {
	case MandatoryWorkerErrorContextCancelled:
		value = &counters.ContextCancelled
	case MandatoryWorkerErrorDeadlineExceeded:
		value = &counters.DeadlineExceeded
	case MandatoryWorkerErrorAssemblyTargetChanged:
		value = &counters.AssemblyTargetChanged
	case MandatoryWorkerErrorIntegrityRejected:
		value = &counters.IntegrityRejected
	case MandatoryWorkerErrorWriterUnavailable:
		value = &counters.WriterUnavailable
	case MandatoryWorkerErrorDependencyUnavailable:
		value = &counters.DependencyUnavailable
	case MandatoryWorkerErrorInternalFailure:
		value = &counters.InternalFailure
	}
	*value = saturatingIncrement(*value)
}

type ReporterOptions struct {
	Output   io.Writer
	Interval time.Duration
	Now      func() time.Time
}

type Reporter struct {
	metrics  *Metrics
	output   io.Writer
	interval time.Duration
	now      func() time.Time
	writeMu  sync.Mutex
}

func NewReporter(metrics *Metrics, options ReporterOptions) (*Reporter, error) {
	if metrics == nil || options.Output == nil {
		return nil, errors.New("operational metrics: metrics and output are required")
	}
	interval := options.Interval
	if interval == 0 {
		interval = DefaultReportInterval
	}
	if interval < 0 {
		return nil, errors.New("operational metrics: report interval must be positive")
	}
	now := options.Now
	if now == nil {
		now = time.Now
	}
	return &Reporter{metrics: metrics, output: options.Output, interval: interval, now: now}, nil
}

// Run emits every interval and emits once more after shutdown is requested.
// It deliberately exposes no endpoint and retains no unbounded labels.
func (reporter *Reporter) Run(ctx context.Context) error {
	if reporter == nil || ctx == nil {
		return errors.New("operational metrics: reporter and context are required")
	}
	ticker := time.NewTicker(reporter.interval)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			if err := reporter.Emit(); err != nil {
				return err
			}
		case <-ctx.Done():
			return reporter.Emit()
		}
	}
}

func (reporter *Reporter) Emit() error {
	if reporter == nil {
		return errors.New("operational metrics: nil reporter")
	}
	when := reporter.now().UTC()
	envelope := struct {
		Version              string   `json:"version"`
		CapturedAtUnixMicros string   `json:"captured_at_unix_micros"`
		Metrics              Snapshot `json:"metrics"`
	}{
		Version: FormatVersion, CapturedAtUnixMicros: fmt.Sprintf("%d", when.UnixMicro()),
		Metrics: reporter.metrics.Snapshot(),
	}
	reporter.writeMu.Lock()
	defer reporter.writeMu.Unlock()
	encoder := json.NewEncoder(reporter.output)
	encoder.SetEscapeHTML(false)
	return encoder.Encode(envelope)
}

func recordDuration(histogram *DurationHistogram, duration time.Duration) {
	if duration < 0 {
		duration = 0
	}
	initializeDurationHistogram(histogram)
	for index, bound := range histogramBounds {
		if duration <= bound {
			histogram.BucketCounts[index] = saturatingIncrement(histogram.BucketCounts[index])
			return
		}
	}
	histogram.BucketCounts[len(histogram.BucketCounts)-1] = saturatingIncrement(histogram.BucketCounts[len(histogram.BucketCounts)-1])
}

func initializeDurationHistogram(histogram *DurationHistogram) {
	for index, bound := range histogramBounds {
		histogram.BoundsMicroseconds[index] = uint64(bound.Microseconds())
	}
}

func saturatingIncrement(value uint64) uint64 {
	if value == math.MaxUint64 {
		return value
	}
	return value + 1
}

func saturatingDecrement(value uint64) uint64 {
	if value == 0 {
		return 0
	}
	return value - 1
}

func validWriterOutcome(value WriterOutcome) bool {
	return value >= WriterOutcomeCommitted && value <= WriterOutcomeFailed
}

func validProviderTerminal(value ProviderTerminal) bool {
	return value >= ProviderSucceeded && value <= ProviderNonretryableFailure
}

func validOperation(value Operation) bool {
	return value >= OperationBackup && value <= OperationBlobGC
}

func validOperationResult(value OperationResult) bool {
	return value >= OperationSucceeded && value <= OperationFailed
}

func validMandatoryWorkerPhase(value MandatoryWorkerPhase) bool {
	switch value {
	case MandatoryWorkerPhaseWorkerLaunch,
		MandatoryWorkerPhaseDialogueScan,
		MandatoryWorkerPhaseProjectionReconcile,
		MandatoryWorkerPhaseDialogueAssembly,
		MandatoryWorkerPhaseDialoguePrepare,
		MandatoryWorkerPhaseFrozenRunRead,
		MandatoryWorkerPhaseAttemptTransition,
		MandatoryWorkerPhaseDialogueLanding,
		MandatoryWorkerPhaseMemoryExtraction:
		return true
	default:
		return false
	}
}

func validMandatoryWorkerErrorClass(value MandatoryWorkerErrorClass) bool {
	switch value {
	case MandatoryWorkerErrorContextCancelled,
		MandatoryWorkerErrorDeadlineExceeded,
		MandatoryWorkerErrorAssemblyTargetChanged,
		MandatoryWorkerErrorIntegrityRejected,
		MandatoryWorkerErrorWriterUnavailable,
		MandatoryWorkerErrorDependencyUnavailable,
		MandatoryWorkerErrorInternalFailure:
		return true
	default:
		return false
	}
}

func validCanonicalULID(value string) bool {
	if len(value) != 26 || value[0] > '7' {
		return false
	}
	const alphabet = "0123456789ABCDEFGHJKMNPQRSTVWXYZ"
	for index := range value {
		valid := false
		for candidate := range alphabet {
			if value[index] == alphabet[candidate] {
				valid = true
				break
			}
		}
		if !valid {
			return false
		}
	}
	return true
}
