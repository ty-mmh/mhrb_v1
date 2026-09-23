package main

import (
	"log/slog"
	"time"

	"mahoroba.local/mahoroba/internal/operationalmetrics"
)

// serveOperationalObserver fans the same sanitized mandatory-worker event to
// process-local counters and the serve log. It has no method that accepts a
// raw error, prompt, provider body, content value, or path.
type serveOperationalObserver struct {
	metrics *operationalmetrics.Metrics
	logger  *slog.Logger
}

func (observer serveOperationalObserver) ProviderFinished(
	terminal operationalmetrics.ProviderTerminal,
	latency time.Duration,
) {
	observer.metrics.ProviderFinished(terminal, latency)
}

func (observer serveOperationalObserver) SetMandatoryWorkCounts(counts operationalmetrics.MandatoryWorkCounts) {
	observer.metrics.SetMandatoryWorkCounts(counts)
}

func (observer serveOperationalObserver) MandatoryWorkerFailed(failure operationalmetrics.MandatoryWorkerFailure) {
	if failure.Validate() != nil {
		return
	}
	observer.metrics.MandatoryWorkerFailed(failure)
	if observer.logger == nil {
		return
	}
	attributes := []any{
		"phase", string(failure.Phase),
		"class", string(failure.Class),
		"rescan_expected", failure.RescanExpected,
	}
	if failure.ResidentID != "" {
		attributes = append(attributes, "resident_id", failure.ResidentID)
	}
	if failure.SourceEventID != "" {
		attributes = append(attributes, "source_event_id", failure.SourceEventID)
	}
	observer.logger.Warn("Mandatory worker failed", attributes...)
}

var _ interface {
	ProviderFinished(operationalmetrics.ProviderTerminal, time.Duration)
	SetMandatoryWorkCounts(operationalmetrics.MandatoryWorkCounts)
	MandatoryWorkerFailed(operationalmetrics.MandatoryWorkerFailure)
} = serveOperationalObserver{}
