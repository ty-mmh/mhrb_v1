package main

import (
	"errors"
	"fmt"
)

// serveStartupOperations is the narrow activation seam used by production
// runServe. Values shared between phases (the selected resident, Projection
// proof, listener, and gate) remain owned by runServe's closures; this type
// owns only the mandatory ordering and fail-closed transition.
type serveStartupOperations struct {
	StartTTSGated          func() error
	PreflightMandatory     func() error
	TerminalizeRunning     func() error
	ScanIntegrity          func(postRecovery bool) error
	TerminalizeMandatory   func() (unresolved bool, err error)
	PreflightEligibility   func() error
	PauseAdmissions        func() error
	PreflightContentRefs   func() error
	PreflightProjection    func() error
	EvaluateFinalReadiness func() error
	BindHTTP               func() error
	StartProjection        func() error
	WaitProjectionReady    func() error
	StartWorkersGated      func() error
	StartHTTPGated         func() error
	RequireCanonicalHead   func() error
	ResumeAdmissions       func() error
	OpenGate               func() error
	FailGate               func(error)
}

func runServeStartupSequence(operations serveStartupOperations) error {
	fail := func(err error) error {
		if err != nil && operations.FailGate != nil {
			operations.FailGate(err)
		}
		return err
	}
	step := func(name string, operation func() error) error {
		if operation == nil {
			return fail(fmt.Errorf("serve startup: nil %s operation", name))
		}
		return fail(operation())
	}

	for _, phase := range []struct {
		name      string
		operation func() error
	}{
		{"TTS gate registration", operations.StartTTSGated},
		{"mandatory recovery preflight", operations.PreflightMandatory},
		{"running-attempt terminalization", operations.TerminalizeRunning},
	} {
		if err := step(phase.name, phase.operation); err != nil {
			return err
		}
	}
	if operations.ScanIntegrity == nil {
		return fail(errors.New("serve startup: nil integrity scan operation"))
	}
	if err := fail(operations.ScanIntegrity(false)); err != nil {
		return err
	}
	if operations.TerminalizeMandatory == nil {
		return fail(errors.New("serve startup: nil mandatory terminalization operation"))
	}
	unresolved, err := operations.TerminalizeMandatory()
	if err := fail(err); err != nil {
		return err
	}
	if unresolved {
		if err := fail(operations.ScanIntegrity(true)); err != nil {
			return err
		}
	}

	for _, phase := range []struct {
		name      string
		operation func() error
	}{
		{"preflight eligibility", operations.PreflightEligibility},
		{"Canonical admission pause", operations.PauseAdmissions},
		{"content references preflight", operations.PreflightContentRefs},
		{"Projection preflight", operations.PreflightProjection},
		{"final readiness", operations.EvaluateFinalReadiness},
		{"HTTP bind", operations.BindHTTP},
		{"Projection start", operations.StartProjection},
		{"Projection readiness", operations.WaitProjectionReady},
		{"worker gate registration", operations.StartWorkersGated},
		{"HTTP gate registration", operations.StartHTTPGated},
		{"activation head verification", operations.RequireCanonicalHead},
		{"Canonical admission resume", operations.ResumeAdmissions},
		{"RuntimeStartGate open", operations.OpenGate},
	} {
		if err := step(phase.name, phase.operation); err != nil {
			return err
		}
	}
	return nil
}
