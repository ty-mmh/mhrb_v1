package app

import (
	"context"
	"errors"
	"testing"
	"time"

	"mahoroba.local/mahoroba/internal/generation"
	"mahoroba.local/mahoroba/internal/operationalmetrics"
)

type observedGeneratorFake struct {
	result generation.Result
	err    error
}

func (fake observedGeneratorFake) Stream(context.Context, generation.Request, generation.DeltaSink) (generation.Result, error) {
	return fake.result, fake.err
}

func (observedGeneratorFake) Capabilities() generation.Capabilities {
	return generation.Capabilities{SupportsJSONSchema: true}
}

func (observedGeneratorFake) ProviderIdentity() string { return "provider-route" }

type providerObservation struct {
	terminal operationalmetrics.ProviderTerminal
	latency  time.Duration
}

func (observation *providerObservation) ProviderFinished(terminal operationalmetrics.ProviderTerminal, latency time.Duration) {
	observation.terminal = terminal
	observation.latency = latency
}

func TestM7ApplicationProviderObserverPreservesCapabilitiesAndRecordsClosedTerminal(t *testing.T) {
	observation := &providerObservation{}
	wrapper := observedGenerator{
		delegate: observedGeneratorFake{result: generation.Result{Text: "raw provider output"}},
		observer: observation, providerIdentity: "provider-route",
	}
	result, err := wrapper.Stream(context.Background(), generation.Request{
		Messages: []generation.Message{{Role: generation.RoleUser, Text: "raw secret input"}},
	}, nil)
	if err != nil || result.Text != "raw provider output" {
		t.Fatalf("Stream = %+v, %v", result, err)
	}
	if observation.terminal != operationalmetrics.ProviderSucceeded || observation.latency < 0 {
		t.Fatalf("observation = %+v", observation)
	}
	if !wrapper.Capabilities().SupportsJSONSchema || wrapper.ProviderIdentity() != "provider-route" {
		t.Fatalf("wrapper capabilities/identity were not preserved")
	}

	cases := []struct {
		name string
		err  error
		want operationalmetrics.ProviderTerminal
	}{
		{name: "cancelled", err: context.Canceled, want: operationalmetrics.ProviderCancelled},
		{name: "deadline", err: context.DeadlineExceeded, want: operationalmetrics.ProviderRetryableFailure},
		{name: "transport", err: &generation.ProviderError{Class: generation.ErrorTransport}, want: operationalmetrics.ProviderRetryableFailure},
		{name: "invalid", err: &generation.ProviderError{Class: generation.ErrorInvalidResponse}, want: operationalmetrics.ProviderNonretryableFailure},
		{name: "opaque", err: errors.New("provider body that must not be observed"), want: operationalmetrics.ProviderNonretryableFailure},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			if got := classifyProviderTerminal(test.err); got != test.want {
				t.Fatalf("terminal = %d, want %d", got, test.want)
			}
		})
	}
}
