package projection

import (
	"context"
	"errors"
	"fmt"
	"slices"

	"mahoroba.local/mahoroba/internal/canonical"
)

// PreflightResult binds synchronous Projection readiness to one immutable
// Canonical head. It is intentionally path- and process-independent so restore
// and serve can apply the same boundary.
type PreflightResult struct {
	ResidentID canonical.ID
	Target     Target
	Names      []Name
}

// PreflightServiceResident synchronously reconciles and verifies the complete
// service-required Projection set for the selected active resident.
func (coordinator *Coordinator) PreflightServiceResident(ctx context.Context, residentID canonical.ID) (PreflightResult, error) {
	return coordinator.PreflightResident(ctx, residentID, ServiceRequiredNames())
}

// PreflightContentReferences synchronously rebuilds the authoritative physical
// reachability Projection for every resident at one Canonical head. It is a
// startup/restore publication gate, not part of selected-resident dialogue
// readiness.
func (coordinator *Coordinator) PreflightContentReferences(ctx context.Context) ([]PreflightResult, error) {
	return coordinator.PreflightAllResidents(ctx, []Name{ContentReferencesName})
}

// PreflightAllResidents captures one immutable target, enumerates residents
// through that same target, and reconciles the named Projection set for each
// resident before verifying that Canonical did not advance. No partially
// reconciled result is represented as a successful proof.
func (coordinator *Coordinator) PreflightAllResidents(ctx context.Context, names []Name) ([]PreflightResult, error) {
	if coordinator == nil {
		return nil, errors.New("projection: nil coordinator")
	}
	if ctx == nil {
		return nil, errors.New("projection: nil preflight context")
	}
	definitions, normalizedNames, err := coordinator.preflightDefinitions(names)
	if err != nil {
		return nil, err
	}
	target, err := coordinator.captureTarget(ctx)
	if err != nil {
		return nil, err
	}
	if !target.Head.Exists {
		return []PreflightResult{}, nil
	}
	residents, err := coordinator.source.Residents(ctx, target)
	if err != nil {
		return nil, fmt.Errorf("projection: enumerate preflight residents: %w", err)
	}
	slices.SortFunc(residents, func(left, right canonical.ID) int {
		return slices.Compare(left[:], right[:])
	})
	for index, residentID := range residents {
		if err := residentID.Validate(); err != nil {
			return nil, fmt.Errorf("projection: invalid enumerated resident: %w", err)
		}
		if index > 0 && residentID == residents[index-1] {
			return nil, fmt.Errorf("projection: duplicate enumerated resident %s", residentID)
		}
	}
	results := make([]PreflightResult, 0, len(residents))
	for _, residentID := range residents {
		result := PreflightResult{
			ResidentID: residentID, Target: target, Names: append([]Name(nil), normalizedNames...),
		}
		lock := coordinator.residentLock(residentID)
		lock.Lock()
		err = coordinator.preflightAtTarget(ctx, target, residentID, definitions)
		lock.Unlock()
		if err != nil {
			return nil, err
		}
		results = append(results, result)
	}
	if err := coordinator.RequireCanonicalHead(ctx, target.Head); err != nil {
		return nil, err
	}
	return results, nil
}

// PreflightResident captures the Canonical head exactly once, uses that target
// for every named Projection, and rejects any concurrent Canonical advance.
// Projection writes are derived-only; this method never submits a Canonical
// command or creates an integrity finding.
func (coordinator *Coordinator) PreflightResident(ctx context.Context, residentID canonical.ID, names []Name) (PreflightResult, error) {
	var result PreflightResult
	if coordinator == nil {
		return result, errors.New("projection: nil coordinator")
	}
	if ctx == nil {
		return result, errors.New("projection: nil preflight context")
	}
	if err := residentID.Validate(); err != nil {
		return result, fmt.Errorf("projection: invalid preflight resident: %w", err)
	}
	definitions, normalizedNames, err := coordinator.preflightDefinitions(names)
	if err != nil {
		return result, err
	}

	lock := coordinator.residentLock(residentID)
	lock.Lock()
	defer lock.Unlock()

	target, err := coordinator.captureTarget(ctx)
	if err != nil {
		return result, err
	}
	if !target.Head.Exists {
		return result, errors.New("projection: service preflight requires a non-empty Canonical head")
	}
	result = PreflightResult{
		ResidentID: residentID,
		Target:     target,
		Names:      append([]Name(nil), normalizedNames...),
	}
	if err := coordinator.preflightAtTarget(ctx, target, residentID, definitions); err != nil {
		return result, err
	}
	if err := coordinator.RequireCanonicalHead(ctx, target.Head); err != nil {
		return result, err
	}
	return result, nil
}

func (coordinator *Coordinator) preflightAtTarget(ctx context.Context, target Target, residentID canonical.ID, definitions []Definition) error {
	for _, definition := range definitions {
		key := targetKey{name: definition.Name, residentID: residentID}
		if err := coordinator.update(ctx, target, residentID, definition, false, true); err != nil {
			coordinator.recordFailure(key, err, true)
			return fmt.Errorf("projection: startup preflight reconcile %s/%s: %w", definition.Name, residentID, err)
		}
		coordinator.clearFailure(key)
		if err := coordinator.requireCurrentAtTarget(ctx, target, residentID, definition); err != nil {
			return err
		}
	}
	return nil
}

func (coordinator *Coordinator) preflightDefinitions(names []Name) ([]Definition, []Name, error) {
	if len(names) == 0 {
		return nil, nil, errors.New("projection: startup preflight requires at least one Projection")
	}
	normalized := append([]Name(nil), names...)
	slices.Sort(normalized)
	for index, name := range normalized {
		if index > 0 && name == normalized[index-1] {
			return nil, nil, fmt.Errorf("projection: duplicate startup Projection %q", name)
		}
	}
	definitions := make([]Definition, 0, len(normalized))
	for _, name := range normalized {
		definition, err := coordinator.registry.Definition(name)
		if err != nil {
			return nil, nil, err
		}
		definitions = append(definitions, definition)
	}
	return definitions, normalized, nil
}

func (coordinator *Coordinator) requireCurrentAtTarget(ctx context.Context, target Target, residentID canonical.ID, definition Definition) error {
	stored, exists, err := coordinator.store.Watermark(ctx, definition.Name, residentID)
	if err != nil {
		return fmt.Errorf("%w: read %s/%s watermark: %v", ErrProjectionNotCurrent, definition.Name, residentID, err)
	}
	if !exists {
		return fmt.Errorf("%w: %s/%s watermark is missing", ErrProjectionNotCurrent, definition.Name, residentID)
	}
	if stored.ProjectionName != definition.Name || stored.ResidentID != residentID ||
		stored.ProjectionVersion != definition.Version || stored.SourceCommitSeq != target.Head.CommitSeq {
		return fmt.Errorf("%w: %s/%s watermark identity, version, or head differs", ErrProjectionNotCurrent, definition.Name, residentID)
	}
	if definition.TimeSensitive && (stored.AsOf != target.AsOf || stored.AsOfTZ != target.AsOfTZ) {
		return fmt.Errorf("%w: %s/%s as_of differs from preflight target", ErrProjectionNotCurrent, definition.Name, residentID)
	}
	dependencies, err := coordinator.source.ResolveDependencies(ctx, DependencyRequest{
		Definition: definition, ResidentID: residentID, Target: target, Previous: &stored,
	})
	if err != nil {
		return fmt.Errorf("%w: resolve %s/%s dependencies: %v", ErrProjectionNotCurrent, definition.Name, residentID, err)
	}
	equal, err := DependencySetEqual(stored.Dependencies, dependencies)
	if err != nil || !equal {
		return fmt.Errorf("%w: %s/%s dependency set differs", ErrProjectionNotCurrent, definition.Name, residentID)
	}
	plan, err := coordinator.plan(ctx, PlanInput{
		Definition: definition, ResidentID: residentID, Target: target, Stored: &stored,
		DesiredDependencies: dependencies,
	})
	if err != nil {
		return fmt.Errorf("%w: plan %s/%s: %v", ErrProjectionNotCurrent, definition.Name, residentID, err)
	}
	if plan.Kind != UpToDate {
		return fmt.Errorf("%w: %s/%s remains %s", ErrProjectionNotCurrent, definition.Name, residentID, plan.Reason)
	}
	return nil
}

// RequireSameTargetCohort verifies that the production claim/runtime cohort is
// current at one exact Canonical head and shares the same as_of and timezone.
// It is the post-rebuild publication boundary for offline administrative work.
func (coordinator *Coordinator) RequireSameTargetCohort(
	ctx context.Context,
	residentID canonical.ID,
	expectedHead canonical.Head,
) error {
	if coordinator == nil {
		return errors.New("projection: nil coordinator")
	}
	if ctx == nil {
		return errors.New("projection: nil cohort verification context")
	}
	if err := residentID.Validate(); err != nil {
		return err
	}
	if err := expectedHead.Validate(); err != nil {
		return fmt.Errorf("projection: invalid expected cohort head: %w", err)
	}
	if !expectedHead.Exists {
		return fmt.Errorf("%w: same-Target cohort requires a non-empty Canonical head", ErrProjectionNotCurrent)
	}
	if !coordinator.isCohortGroup(coordinator.cohort) {
		return fmt.Errorf("%w: same-Target cohort is not registered", ErrProjectionNotCurrent)
	}

	lock := coordinator.residentLock(residentID)
	lock.Lock()
	defer lock.Unlock()

	watermarks := make([]Watermark, 0, len(coordinator.cohort))
	for _, definition := range coordinator.cohort {
		watermark, exists, err := coordinator.store.Watermark(ctx, definition.Name, residentID)
		if err != nil {
			return fmt.Errorf("%w: read %s/%s watermark: %v", ErrProjectionNotCurrent, definition.Name, residentID, err)
		}
		if !exists {
			return fmt.Errorf("%w: %s/%s watermark is missing", ErrProjectionNotCurrent, definition.Name, residentID)
		}
		if err := watermark.Validate(); err != nil {
			return fmt.Errorf("%w: invalid %s/%s watermark: %v", ErrProjectionNotCurrent, definition.Name, residentID, err)
		}
		watermarks = append(watermarks, watermark)
	}
	claim, runtime := watermarks[0], watermarks[1]
	if claim.SourceCommitSeq != expectedHead.CommitSeq || runtime.SourceCommitSeq != expectedHead.CommitSeq ||
		claim.AsOf != runtime.AsOf || claim.AsOfTZ != runtime.AsOfTZ {
		return fmt.Errorf("%w: claim_states/runtime_states watermarks do not share the expected Target", ErrProjectionNotCurrent)
	}
	target := Target{Head: expectedHead, AsOf: claim.AsOf, AsOfTZ: claim.AsOfTZ}
	if err := target.Validate(); err != nil {
		return fmt.Errorf("%w: invalid same-Target cohort: %v", ErrProjectionNotCurrent, err)
	}
	for _, definition := range coordinator.cohort {
		if err := coordinator.requireCurrentAtTarget(ctx, target, residentID, definition); err != nil {
			return err
		}
	}
	return nil
}

// RequireCanonicalHead verifies the no-Canonical-write interval immediately
// before RuntimeStartGate opens.
func (coordinator *Coordinator) RequireCanonicalHead(ctx context.Context, expected canonical.Head) error {
	if coordinator == nil {
		return errors.New("projection: nil coordinator")
	}
	if ctx == nil {
		return errors.New("projection: nil head verification context")
	}
	if err := expected.Validate(); err != nil {
		return fmt.Errorf("projection: invalid expected Canonical head: %w", err)
	}
	actual, err := coordinator.source.Head(ctx)
	if err != nil {
		return fmt.Errorf("projection: recapture Canonical head: %w", err)
	}
	if actual != expected {
		return fmt.Errorf("%w: expected %+v, got %+v", ErrPreflightHeadChanged, expected, actual)
	}
	return nil
}
