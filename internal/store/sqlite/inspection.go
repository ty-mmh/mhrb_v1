package sqlite

import (
	"context"
	"time"

	"mahoroba.local/mahoroba/internal/autonomy"
	"mahoroba.local/mahoroba/internal/canonical"
	"mahoroba.local/mahoroba/internal/domain"
)

// InspectionRepository deliberately exposes only the Canonical queries needed
// by verification and physical blob recovery. It cannot satisfy Backend,
// MutationStore, or domain.Repository and therefore cannot be passed to the
// Canonical Writer or Application by mistake.
type InspectionRepository struct {
	repository *CanonicalRepository
}

func (inspection *Inspection) Canonical() *InspectionRepository {
	return &InspectionRepository{repository: inspection.store.Canonical()}
}

func (repository *InspectionRepository) ListResidents(ctx context.Context) ([]domain.ResidentSnapshot, error) {
	return repository.repository.ListResidents(ctx)
}

func (repository *InspectionRepository) ListResidentIDs(ctx context.Context) ([]canonical.ID, error) {
	return repository.repository.ListResidentIDs(ctx)
}

func (repository *InspectionRepository) ResidentExists(ctx context.Context, residentID canonical.ID) (bool, error) {
	return repository.repository.ResidentExists(ctx, residentID)
}

func (repository *InspectionRepository) Resident(ctx context.Context, residentID canonical.ID) (domain.ResidentSnapshot, error) {
	return repository.repository.Resident(ctx, residentID)
}

func (repository *InspectionRepository) PipelineVersion(
	ctx context.Context,
	kind, versionKey string,
) (domain.PipelineVersionDefinition, error) {
	return repository.repository.PipelineVersion(ctx, kind, versionKey)
}

func (repository *InspectionRepository) ListMemoryClaims(ctx context.Context, filter domain.MemoryClaimFilter) ([]domain.MemoryClaimSummary, error) {
	return repository.repository.ListMemoryClaims(ctx, filter)
}

func (repository *InspectionRepository) MemoryClaimProvenance(ctx context.Context, residentID, claimID canonical.ID) (domain.MemoryClaimProvenance, error) {
	return repository.repository.MemoryClaimProvenance(ctx, residentID, claimID)
}

func (repository *InspectionRepository) ListMemoryPersonaRevisions(ctx context.Context, residentID canonical.ID) ([]domain.MemoryPersonaRevisionView, error) {
	return repository.repository.ListMemoryPersonaRevisions(ctx, residentID)
}

func (repository *InspectionRepository) WalkResidentContents(ctx context.Context, residentID canonical.ID, visit func(canonical.LedgerContent) error) error {
	return repository.repository.WalkResidentContents(ctx, residentID, visit)
}

func (repository *InspectionRepository) WalkResidentEvents(ctx context.Context, residentID canonical.ID, visit func(canonical.LedgerRecord) error) error {
	return repository.repository.WalkResidentEvents(ctx, residentID, visit)
}

func (repository *InspectionRepository) ValidateLedgerEnvelope(record canonical.LedgerRecord) error {
	return repository.repository.ValidateLedgerEnvelope(record)
}

func (repository *InspectionRepository) BlobReferenced(ctx context.Context, residentID canonical.ID, digest canonical.Digest) (bool, error) {
	return repository.repository.BlobReferenced(ctx, residentID, digest)
}

func (repository *InspectionRepository) AutonomySnapshot(ctx context.Context, request autonomy.SnapshotRequest) (autonomy.Snapshot, error) {
	return repository.repository.AutonomySnapshot(ctx, request)
}

func (repository *InspectionRepository) RetentionSources(ctx context.Context, request autonomy.RetentionRequest) (autonomy.RetentionCapture, error) {
	return repository.repository.RetentionSources(ctx, request)
}

func (repository *InspectionRepository) DiscoverReevaluationTriggers(
	ctx context.Context,
	residentID canonical.ID,
	limit int,
) ([]autonomy.Trigger, error) {
	return repository.repository.DiscoverReevaluationTriggers(ctx, residentID, limit)
}

func (repository *InspectionRepository) DiscoverInitiativeTriggers(
	ctx context.Context,
	residentID canonical.ID,
	asOf canonical.Instant,
	maxStaleness time.Duration,
	limit int,
) ([]autonomy.Trigger, error) {
	return repository.repository.DiscoverInitiativeTriggers(ctx, residentID, asOf, maxStaleness, limit)
}

func (repository *InspectionRepository) AutonomousProjectionEvidence(
	ctx context.Context,
	residentID canonical.ID,
	asOf canonical.Instant,
	maxStaleness time.Duration,
) (domain.AutonomousProjectionEvidence, bool, error) {
	return repository.repository.AutonomousProjectionEvidence(ctx, residentID, asOf, maxStaleness)
}

func (repository *InspectionRepository) DiscoverExecutableAutonomousWork(
	ctx context.Context,
	residentID canonical.ID,
	maxAttempts, limit int,
) ([]domain.AutonomousWork, error) {
	return repository.repository.DiscoverExecutableAutonomousWork(ctx, residentID, maxAttempts, limit)
}

func (repository *InspectionRepository) LoadHead(ctx context.Context) (canonical.Head, error) {
	return repository.repository.LoadHead(ctx)
}

func (repository *InspectionRepository) RunningAttempts(ctx context.Context, limit int) ([]domain.RunningAttempt, error) {
	return repository.repository.RunningAttempts(ctx, limit)
}

func (repository *InspectionRepository) RunningAttemptsForResident(
	ctx context.Context,
	residentID canonical.ID,
	limit int,
) ([]domain.RunningAttempt, error) {
	return repository.repository.RunningAttemptsForResident(ctx, residentID, limit)
}

func (repository *InspectionRepository) ResolveMandatoryCancellationEnvelope(
	ctx context.Context,
	work domain.MandatoryRecoveryWork,
) (domain.CancellationEnvelopeResolution, error) {
	return repository.repository.ResolveMandatoryCancellationEnvelope(ctx, work)
}

func (repository *InspectionRepository) DiscoverDialogueWork(
	ctx context.Context,
	residentID canonical.ID,
	request domain.DialogueDiscoveryRequest,
) (domain.DialogueDiscoveryResult, error) {
	return repository.repository.DiscoverDialogueWork(ctx, residentID, request)
}

func (repository *InspectionRepository) DiscoverMemoryExtractionWork(
	ctx context.Context,
	residentID canonical.ID,
	request domain.MemoryDiscoveryRequest,
) (domain.MemoryDiscoveryResult, error) {
	return repository.repository.DiscoverMemoryExtractionWork(ctx, residentID, request)
}

func (repository *InspectionRepository) DiscoverMemoryReextractionWork(
	ctx context.Context,
	residentID canonical.ID,
	request domain.MemoryReextractionDiscoveryRequest,
) (domain.MemoryReextractionDiscoveryResult, error) {
	return repository.repository.DiscoverMemoryReextractionWork(ctx, residentID, request)
}

var _ canonical.LedgerSource = (*InspectionRepository)(nil)
var _ canonical.EnvelopeValidator = (*InspectionRepository)(nil)
var _ domain.MemoryAdminRepository = (*InspectionRepository)(nil)
var _ autonomy.Source = (*InspectionRepository)(nil)
