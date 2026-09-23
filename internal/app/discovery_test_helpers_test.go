package app

import (
	"context"

	"mahoroba.local/mahoroba/internal/canonical"
	"mahoroba.local/mahoroba/internal/domain"
)

type dialogueDiscoveryTestRepository interface {
	DiscoverDialogueWork(context.Context, canonical.ID, domain.DialogueDiscoveryRequest) (domain.DialogueDiscoveryResult, error)
}

type memoryDiscoveryTestRepository interface {
	DiscoverMemoryExtractionWork(
		context.Context,
		canonical.ID,
		domain.MemoryDiscoveryRequest,
	) (domain.MemoryDiscoveryResult, error)
}

func discoverDialogueWorkForTest(
	ctx context.Context,
	repository dialogueDiscoveryTestRepository,
	residentID canonical.ID,
	limit, maxAttempts int,
) ([]domain.DialogueWork, error) {
	if limit < 1 {
		limit = 1
	}
	budget := domain.ProductionDialogueDiscoveryBudget()
	if limit < budget.RecentCandidates {
		budget.RecentCandidates = limit
	}
	if limit < budget.OlderPageSize {
		budget.OlderPageSize = limit
	}
	if limit < budget.OlderCandidates {
		budget.OlderCandidates = limit
	}
	if budget.OlderCandidates < budget.OlderPageSize {
		budget.OlderCandidates = budget.OlderPageSize
	}
	if budget.OlderCandidates > budget.OlderPageSize*budget.OlderPages {
		budget.OlderPages = (budget.OlderCandidates + budget.OlderPageSize - 1) / budget.OlderPageSize
	}
	if budget.OlderPages > 8 {
		budget.OlderPages = 8
		budget.OlderCandidates = budget.OlderPageSize * budget.OlderPages
	}
	result, err := repository.DiscoverDialogueWork(ctx, residentID, domain.DialogueDiscoveryRequest{
		MaxAttempts: maxAttempts, Budget: budget,
	})
	return result.Work, err
}

func discoverMemoryExtractionWorkForTest(
	ctx context.Context,
	repository memoryDiscoveryTestRepository,
	residentID canonical.ID,
	limit, maxAttempts int,
) ([]domain.MemoryExtractionWork, error) {
	if limit < 1 {
		limit = 1
	}
	works := make([]domain.MemoryExtractionWork, 0, limit)
	var cursor *domain.MemoryDiscoveryCursor
	for len(works) < limit {
		result, err := repository.DiscoverMemoryExtractionWork(ctx, residentID, domain.MemoryDiscoveryRequest{
			Cursor: cursor, MaxAttempts: maxAttempts, Budget: domain.ProductionMemoryDiscoveryBudget(),
		})
		if err != nil {
			return nil, err
		}
		if result.Work != nil {
			works = append(works, *result.Work)
		}
		if result.CycleComplete || result.NextCursor == nil {
			break
		}
		cursor = result.NextCursor
	}
	return works, nil
}
