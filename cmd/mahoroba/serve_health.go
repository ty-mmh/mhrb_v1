package main

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"

	"mahoroba.local/mahoroba/internal/projection"
	"mahoroba.local/mahoroba/internal/readiness"
)

type healthProjectionStatuses interface {
	Status(context.Context, projection.StatusFilter) ([]projection.Status, error)
}

// serveHealthState owns only lifecycle observations. Canonical and Projection
// truth are recaptured for every request; no startup-ready boolean is reused as
// authority after Canonical advances.
type serveHealthState struct {
	source      readiness.Source
	projections healthProjectionStatuses
	started     atomic.Bool
	shutdown    atomic.Bool
}

func newServeHealthState(runtime *runtimeComponents) *serveHealthState {
	return &serveHealthState{
		source: runtime.store.ServiceReadinessSource(), projections: runtime.projections,
	}
}

func (state *serveHealthState) MarkStarted()  { state.started.Store(true) }
func (state *serveHealthState) MarkShutdown() { state.shutdown.Store(true) }

func (state *serveHealthState) Evaluate(ctx context.Context) (readiness.Result, error) {
	if state == nil || state.source == nil || state.projections == nil {
		return readiness.Result{}, errors.New("health: evaluator is incomplete")
	}
	return readiness.EvaluateServiceReadiness(ctx, state.source, readiness.Request{
		StartupComplete:    state.started.Load(),
		ShutdownInProgress: state.shutdown.Load(),
		Projection:         healthProjectionChecker{statuses: state.projections},
	})
}

type healthProjectionChecker struct{ statuses healthProjectionStatuses }

func (checker healthProjectionChecker) Current(
	ctx context.Context,
	requirement readiness.ProjectionRequirement,
) (bool, error) {
	if checker.statuses == nil {
		return false, errors.New("health: Projection status source is unavailable")
	}
	statuses, err := checker.statuses.Status(ctx, projection.StatusFilter{ResidentID: &requirement.ResidentID})
	if err != nil {
		return false, fmt.Errorf("health: inspect Projection status: %w", err)
	}
	required := make(map[projection.Name]struct{}, len(projection.ServiceRequiredNames()))
	for _, name := range projection.ServiceRequiredNames() {
		required[name] = struct{}{}
	}
	seen := make(map[projection.Name]struct{}, len(required))
	memoryTargets := make(map[projection.Name]*projection.Watermark, 2)
	for _, status := range statuses {
		if _, needed := required[status.Name]; !needed {
			continue
		}
		if _, duplicate := seen[status.Name]; duplicate {
			return false, nil
		}
		seen[status.Name] = struct{}{}
		if status.ResidentID != requirement.ResidentID || status.Head != requirement.CapturedHead.Canonical() ||
			!status.Built || !status.UpToDate || status.Stale || status.BlockingReason != "" || status.Watermark == nil ||
			status.Watermark.SourceCommitSeq != requirement.CapturedHead.CommitSeq {
			return false, nil
		}
		for _, dependency := range status.Dependencies {
			if dependency.Kind == projection.SessionizationPolicyDependency &&
				dependency.VersionID != requirement.SessionPolicyID {
				return false, nil
			}
		}
		if status.Name == projection.ClaimStatesName || status.Name == projection.RuntimeStatesName {
			memoryTargets[status.Name] = status.Watermark
		}
	}
	if len(seen) != len(required) {
		return false, nil
	}
	return exactMemoryProjectionTarget(memoryTargets), nil
}

func exactMemoryProjectionTarget(targets map[projection.Name]*projection.Watermark) bool {
	claim, claimExists := targets[projection.ClaimStatesName]
	runtime, runtimeExists := targets[projection.RuntimeStatesName]
	return len(targets) == 2 && claimExists && runtimeExists && claim != nil && runtime != nil &&
		claim.SourceCommitSeq == runtime.SourceCommitSeq && claim.AsOf == runtime.AsOf && claim.AsOfTZ == runtime.AsOfTZ
}
