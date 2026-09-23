package projection

import (
	"context"
	"errors"
	"fmt"
	"time"

	"mahoroba.local/mahoroba/internal/canonical"
)

var ErrProjectionNotCurrent = errors.New("projection: current watermark required")

type StatusFilter struct {
	ResidentID *canonical.ID
	Name       *Name
}

type Status struct {
	Head           canonical.Head `json:"head"`
	ResidentID     canonical.ID   `json:"resident_id"`
	Name           Name           `json:"name"`
	Version        Version        `json:"version"`
	Built          bool           `json:"built"`
	Watermark      *Watermark     `json:"watermark"`
	Dependencies   []Dependency   `json:"dependencies"`
	CommitLag      int64          `json:"commit_lag"`
	UpToDate       bool           `json:"up_to_date"`
	Stale          bool           `json:"stale"`
	BlockingReason string         `json:"blocking_reason"`
}

func (coordinator *Coordinator) Status(ctx context.Context, filter StatusFilter) ([]Status, error) {
	if ctx == nil {
		return nil, errors.New("projection: nil status context")
	}
	definitions := coordinator.registry.Definitions()
	if filter.Name != nil {
		definition, err := coordinator.registry.Definition(*filter.Name)
		if err != nil {
			return nil, err
		}
		definitions = []Definition{definition}
	}
	residents, err := coordinator.resolveResidentsForReconcile(ctx, filter.ResidentID)
	if err != nil {
		return nil, err
	}
	statuses := make([]Status, 0, len(residents)*len(definitions))
	for _, residentID := range residents {
		lock := coordinator.residentLock(residentID)
		lock.Lock()
		target, err := coordinator.captureTarget(ctx)
		if err != nil {
			lock.Unlock()
			return nil, err
		}
		for _, definition := range definitions {
			statuses = append(statuses, coordinator.status(ctx, target, residentID, definition))
		}
		lock.Unlock()
	}
	return statuses, nil
}

func (coordinator *Coordinator) status(ctx context.Context, target Target, residentID canonical.ID, definition Definition) Status {
	result := Status{
		Head: target.Head, ResidentID: residentID, Name: definition.Name, Version: definition.Version,
	}
	stored, exists, err := coordinator.store.Watermark(ctx, definition.Name, residentID)
	if err != nil {
		result.BlockingReason = err.Error()
		result.Stale = true
		return result
	}
	var previous *Watermark
	if exists {
		copyWatermark := stored.clone()
		previous = &copyWatermark
		result.Built = true
		result.Watermark = cloneWatermarkPointer(previous)
		result.Dependencies = append([]Dependency(nil), stored.Dependencies...)
		result.CommitLag = target.Head.CommitSeq.Int64() - stored.SourceCommitSeq.Int64()
	} else if target.Head.Exists {
		result.CommitLag = target.Head.CommitSeq.Int64()
	}
	planTarget := target
	if previous != nil && definition.TimeSensitive && target.AsOf >= stored.AsOf {
		age := time.Duration(int64(target.AsOf-stored.AsOf)) * time.Microsecond
		if age < coordinator.schedules[definition.Name].AsOfRefreshInterval {
			planTarget.AsOf = stored.AsOf
			planTarget.AsOfTZ = stored.AsOfTZ
		}
	}
	dependencies, err := coordinator.source.ResolveDependencies(ctx, DependencyRequest{
		Definition: definition, ResidentID: residentID, Target: planTarget, Previous: previous,
	})
	if err == nil {
		input := PlanInput{
			Definition: definition, ResidentID: residentID, Target: planTarget, Stored: previous,
			DesiredDependencies: dependencies,
		}
		var plan UpdatePlan
		plan, err = coordinator.plan(ctx, input)
		result.UpToDate = err == nil && plan.Kind == UpToDate
		if err == nil && !result.UpToDate {
			result.BlockingReason = string(plan.Reason)
		}
	}
	key := targetKey{name: definition.Name, residentID: residentID}
	coordinator.failuresMu.RLock()
	lastFailure, failed := coordinator.failures[key]
	coordinator.failuresMu.RUnlock()
	if err != nil {
		result.BlockingReason = err.Error()
	} else if failed {
		result.BlockingReason = lastFailure.err.Error()
	}
	result.Stale = !result.Built
	if result.Built && !result.UpToDate {
		if definition.TimeSensitive {
			ageMicros := int64(target.AsOf - stored.AsOf)
			result.Stale = ageMicros > coordinator.schedules[definition.Name].MaxStaleness.Microseconds()
		} else {
			// Non-time-sensitive projections deliberately do not advance as_of
			// on commit-only updates, so a time-age test would be misleading.
			result.Stale = true
		}
	}
	if result.BlockingReason != "" {
		result.UpToDate = false
	}
	if result.CommitLag < 0 {
		result.BlockingReason = fmt.Errorf("%w: watermark is %d commits ahead", ErrSourceRegression, -result.CommitLag).Error()
		result.UpToDate = false
		result.Stale = true
	}
	return result
}

// RequireCurrent is the fail-closed boundary for future safety-sensitive
// consumers such as content GC. Deterministic read fallbacks do not use it.
func (coordinator *Coordinator) RequireCurrent(ctx context.Context, residentID canonical.ID, name Name) error {
	statuses, err := coordinator.Status(ctx, StatusFilter{ResidentID: &residentID, Name: &name})
	if err != nil {
		return err
	}
	if len(statuses) != 1 {
		return fmt.Errorf("%w: no status for %s/%s", ErrProjectionNotCurrent, name, residentID)
	}
	status := statuses[0]
	if !status.Built || !status.UpToDate || status.Stale || status.BlockingReason != "" {
		return fmt.Errorf("%w: %s/%s: %s", ErrProjectionNotCurrent, name, residentID, status.BlockingReason)
	}
	return nil
}
