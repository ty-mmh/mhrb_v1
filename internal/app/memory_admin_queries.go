package app

import (
	"context"
	"errors"

	"mahoroba.local/mahoroba/internal/canonical"
	"mahoroba.local/mahoroba/internal/domain"
)

// MemoryAdminQueries is the query-only half of the M5 Admin service. It can be
// constructed over OpenInspection, so list/show never acquire writer authority,
// run recovery, or advance a Projection watermark.
type MemoryAdminQueries struct {
	repository domain.MemoryAdminRepository
}

func NewMemoryAdminQueries(repository domain.MemoryAdminRepository) (*MemoryAdminQueries, error) {
	if repository == nil {
		return nil, errors.New("app: memory Admin repository is required")
	}
	return &MemoryAdminQueries{repository: repository}, nil
}

func (queries *MemoryAdminQueries) ListClaims(
	ctx context.Context,
	filter domain.MemoryClaimFilter,
) ([]domain.MemoryClaimSummary, error) {
	return queries.repository.ListMemoryClaims(ctx, filter)
}

func (queries *MemoryAdminQueries) ShowClaim(
	ctx context.Context,
	residentID canonical.ID,
	claimID canonical.ID,
) (domain.MemoryClaimProvenance, error) {
	return queries.repository.MemoryClaimProvenance(ctx, residentID, claimID)
}

func (queries *MemoryAdminQueries) ListPersonaRevisions(
	ctx context.Context,
	residentID canonical.ID,
) ([]domain.MemoryPersonaRevisionView, error) {
	return queries.repository.ListMemoryPersonaRevisions(ctx, residentID)
}

func (a *Application) ListMemoryClaims(
	ctx context.Context,
	filter domain.MemoryClaimFilter,
) ([]domain.MemoryClaimSummary, error) {
	repository, ok := a.repository.(domain.MemoryAdminRepository)
	if !ok {
		return nil, errors.New("app: repository lacks memory Admin query capability")
	}
	queries, _ := NewMemoryAdminQueries(repository)
	return queries.ListClaims(ctx, filter)
}

func (a *Application) MemoryClaimProvenance(
	ctx context.Context,
	residentID canonical.ID,
	claimID canonical.ID,
) (domain.MemoryClaimProvenance, error) {
	repository, ok := a.repository.(domain.MemoryAdminRepository)
	if !ok {
		return domain.MemoryClaimProvenance{}, errors.New("app: repository lacks memory Admin query capability")
	}
	queries, _ := NewMemoryAdminQueries(repository)
	return queries.ShowClaim(ctx, residentID, claimID)
}

func (a *Application) ListMemoryPersonaRevisions(
	ctx context.Context,
	residentID canonical.ID,
) ([]domain.MemoryPersonaRevisionView, error) {
	repository, ok := a.repository.(domain.MemoryAdminRepository)
	if !ok {
		return nil, errors.New("app: repository lacks memory Admin query capability")
	}
	queries, _ := NewMemoryAdminQueries(repository)
	return queries.ListPersonaRevisions(ctx, residentID)
}
