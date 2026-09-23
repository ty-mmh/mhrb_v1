package app

import (
	"context"
	"errors"
	"time"

	"mahoroba.local/mahoroba/internal/generation"
	"mahoroba.local/mahoroba/internal/operationalmetrics"
)

type providerObserver interface {
	ProviderFinished(operationalmetrics.ProviderTerminal, time.Duration)
}

// observedGenerator is the single Application/provider boundary.  It records
// only elapsed time and a closed terminal class and deliberately cannot pass
// request messages, deltas, provider response bodies, or errors to metrics.
type observedGenerator struct {
	delegate         generation.Generator
	observer         providerObserver
	providerIdentity string
}

func (generator observedGenerator) Stream(
	ctx context.Context,
	request generation.Request,
	sink generation.DeltaSink,
) (generation.Result, error) {
	started := time.Now()
	result, err := generator.delegate.Stream(ctx, request, sink)
	generator.observer.ProviderFinished(classifyProviderTerminal(err), time.Since(started))
	return result, err
}

func (generator observedGenerator) Capabilities() generation.Capabilities {
	if provider, ok := generator.delegate.(generation.CapabilityProvider); ok {
		return provider.Capabilities()
	}
	return generation.Capabilities{}
}

func (generator observedGenerator) ProviderIdentity() string {
	return generator.providerIdentity
}

func classifyProviderTerminal(err error) operationalmetrics.ProviderTerminal {
	if err == nil {
		return operationalmetrics.ProviderSucceeded
	}
	code := generation.OutcomeErrorCodeFromError(err)
	if errors.Is(err, context.Canceled) || code.Class() == generation.ErrorCancelled {
		return operationalmetrics.ProviderCancelled
	}
	if errors.Is(err, context.DeadlineExceeded) || code.Retryable() {
		return operationalmetrics.ProviderRetryableFailure
	}
	return operationalmetrics.ProviderNonretryableFailure
}

var _ generation.Generator = observedGenerator{}
var _ generation.CapabilityProvider = observedGenerator{}
var _ generation.IdentifiedGenerator = observedGenerator{}
