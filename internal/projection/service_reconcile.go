package projection

import (
	"context"
	"errors"
	"fmt"

	"mahoroba.local/mahoroba/internal/canonical"
)

// ReconcileServiceRequiredResident captures one immutable Target after taking
// the resident serialization lock, then brings exactly the service-required
// Projection set toward it. The captured Target is returned even when an
// update fails so the caller can distinguish reconciliation failure from
// target-capture failure and must not silently assemble against another head.
func (coordinator *Coordinator) ReconcileServiceRequiredResident(
	ctx context.Context,
	residentID canonical.ID,
) (Target, error) {
	if coordinator == nil {
		return Target{}, errors.New("projection: nil coordinator")
	}
	if ctx == nil {
		return Target{}, errors.New("projection: nil service-required reconcile context")
	}
	if err := residentID.Validate(); err != nil {
		return Target{}, err
	}

	definitions := make([]Definition, 0, len(ServiceRequiredNames()))
	for _, name := range ServiceRequiredNames() {
		definition, err := coordinator.registry.Definition(name)
		if err != nil {
			return Target{}, err
		}
		definitions = append(definitions, definition)
	}

	lock := coordinator.residentLock(residentID)
	lock.Lock()
	defer lock.Unlock()

	target, err := coordinator.captureTarget(ctx)
	if err != nil {
		return Target{}, err
	}
	// Canonical Writer preserves a strictly increasing commit timestamp even
	// when a coarse/frozen clock returns the same instant. Assembly as_of must
	// never precede the captured head, so close that sub-microsecond gap before
	// reconciling any time-sensitive Projection.
	if target.Head.Exists && target.AsOf < target.Head.CommittedAt {
		target.AsOf = target.Head.CommittedAt
	}

	var result error
	for _, definition := range definitions {
		key := targetKey{name: definition.Name, residentID: residentID}
		if updateErr := coordinator.update(ctx, target, residentID, definition, false, true); updateErr != nil {
			coordinator.recordFailure(key, updateErr, true)
			result = errors.Join(result, fmt.Errorf(
				"projection: reconcile service-required %s/%s: %w",
				definition.Name,
				residentID,
				updateErr,
			))
			continue
		}
		coordinator.clearFailure(key)
	}
	return target, result
}

// ReconcileDialogueAssembly exposes the service-required reconciliation
// boundary without forcing the application package to depend on
// projection.Target. The fields preserve the one captured Target even when
// reconciliation returns an error.
func (coordinator *Coordinator) ReconcileDialogueAssembly(
	ctx context.Context,
	residentID canonical.ID,
) (canonical.Head, canonical.Instant, canonical.Timezone, error) {
	target, err := coordinator.ReconcileServiceRequiredResident(ctx, residentID)
	return target.Head, target.AsOf, target.AsOfTZ, err
}
