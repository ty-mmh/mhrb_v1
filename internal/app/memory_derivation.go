package app

import (
	"context"
	"errors"
	"fmt"

	"mahoroba.local/mahoroba/internal/canonical"
	"mahoroba.local/mahoroba/internal/domain"
)

// LandMemoryClaimAbstraction atomically lands an Admin-reviewed abstraction.
// The caller owns provider-output parsing and Content construction; the
// Canonical Writer independently revalidates the active policy, pipeline,
// source identity, scope and evidence provenance before accepting it.
func (a *Application) LandMemoryClaimAbstraction(
	ctx context.Context,
	value domain.LandDerivedClaim,
) (domain.DerivedClaimLandingResult, error) {
	return a.landMemoryDerivedClaim(ctx, value, domain.LandClaimAbstractionCommand, nil)
}

// LandMemoryClaimDifferentiation atomically lands an Admin-reviewed split.
func (a *Application) LandMemoryClaimDifferentiation(
	ctx context.Context,
	value domain.LandDerivedClaim,
) (domain.DerivedClaimLandingResult, error) {
	return a.landMemoryDerivedClaim(ctx, value, domain.LandClaimDifferentiationCommand, nil)
}

func (a *Application) landMemoryDerivedClaim(
	ctx context.Context,
	value domain.LandDerivedClaim,
	command func(domain.LandDerivedClaim) canonical.Command,
	lease *backgroundCallLease,
) (domain.DerivedClaimLandingResult, error) {
	if err := a.ensureAccepting(); err != nil {
		return domain.DerivedClaimLandingResult{}, err
	}
	if command == nil {
		return domain.DerivedClaimLandingResult{}, errors.New("app: derived claim command is required")
	}
	resident, err := a.repository.Resident(ctx, value.ResidentID)
	if err != nil {
		return domain.DerivedClaimLandingResult{}, err
	}
	if resident.Status != "active" {
		return domain.DerivedClaimLandingResult{}, errors.New("app: derived claims require an active resident")
	}
	if resident.OwnerPrincipalID != value.OwnerPrincipalID {
		return domain.DerivedClaimLandingResult{}, errors.New("app: derived claim owner does not own resident")
	}
	var result canonical.CommandResult
	if lease == nil {
		result, err = a.submitWithContent(ctx, command(value), []domain.Content{value.Output, value.Statement})
	} else {
		result, err = a.submitBackgroundLandingWithContent(
			ctx, *lease, command(value), []domain.Content{value.Output, value.Statement},
		)
	}
	if err != nil {
		return domain.DerivedClaimLandingResult{}, err
	}
	landing, ok := result.Value.(domain.DerivedClaimLandingResult)
	if !ok {
		return domain.DerivedClaimLandingResult{}, fmt.Errorf("app: derived claim mutation returned an unexpected value")
	}
	return landing, nil
}
