package domain

import (
	"context"

	"mahoroba.local/mahoroba/internal/canonical"
)

type DerivedClaimSourceContext struct {
	ClaimID     canonical.ID
	Statement   string
	EvidenceIDs []canonical.ID
}

type DerivedClaimPreparation struct {
	Resident          ResidentSnapshot
	PolicyContent     string
	PipelineVersionID canonical.ID
	Sources           []DerivedClaimSourceContext
}

type MemoryDerivationRepository interface {
	PrepareMemoryDerivation(context.Context, canonical.ID, GenerationPurpose, []canonical.ID) (DerivedClaimPreparation, error)
}

type MemoryDerivationResult struct {
	RunID   canonical.ID              `json:"run_id"`
	Landing DerivedClaimLandingResult `json:"landing"`
}
