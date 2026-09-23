// Package integrityrun owns the mutation-capable M7 integrity scan workflow.
// It is deliberately independent of the application and CLI packages so
// serve startup, restore staging, and offline admin commands can share one
// ordering and idempotency boundary.
package integrityrun

import (
	"context"
	"errors"
	"fmt"
	"slices"

	"mahoroba.local/mahoroba/internal/canonical"
	"mahoroba.local/mahoroba/internal/domain"
	"mahoroba.local/mahoroba/internal/integrity"
)

type Submitter interface {
	Submit(context.Context, canonical.Command) (canonical.CommandResult, error)
}

type Scanner interface {
	Scan(context.Context) (integrity.ScanResult, error)
}

type Repository interface {
	PipelineVersion(context.Context, string, string) (domain.PipelineVersionDefinition, error)
	IntegrityPipelineCommit(context.Context, canonical.ID) (canonical.CommitMetadata, error)
	IntegrityHeadMetadata(context.Context, canonical.Head) (integrity.HeadMetadata, error)
}

type Options struct {
	Writer     Submitter
	Repository Repository
	Scanner    Scanner
	IDs        *canonical.IDGenerator
}

type Service struct {
	writer     Submitter
	repository Repository
	scanner    Scanner
	ids        *canonical.IDGenerator
}

type CommitEffects struct {
	PipelineVersionRegistered bool
	IntegrityFindingRecorded  bool
	ClaimStatusQuarantined    bool
}

type Commit struct {
	Metadata    canonical.CommitMetadata
	Disposition Disposition
	Effects     CommitEffects
}

type Disposition string

const (
	DispositionCreated  Disposition = "created"
	DispositionExisting Disposition = "existing"
)

type Result struct {
	CapturedHead       integrity.HeadMetadata
	ResultHead         integrity.HeadMetadata
	ExistingFindings   int
	CreatedFindings    int
	CreatedQuarantines int
	ReadinessBlocked   bool
	CanonicalCommits   []Commit
}

func New(options Options) (*Service, error) {
	if options.Writer == nil || options.Repository == nil || options.Scanner == nil || options.IDs == nil {
		return nil, errors.New("integrity run: writer, repository, scanner, and IDs are required")
	}
	return &Service{
		writer: options.Writer, repository: options.Repository,
		scanner: options.Scanner, ids: options.IDs,
	}, nil
}

// Run registers the two exact scan dependencies, captures one candidate head,
// and applies resident-scoped batches in deterministic resident order. A
// non-nil filter limits both Apply and final readiness evaluation to one
// resident; nil means every resident, including the selected active resident.
// On a mid-run error Result retains every durable commit already observed.
func (service *Service) Run(ctx context.Context, residentFilter *canonical.ID) (Result, error) {
	var result Result
	if service == nil {
		return result, errors.New("integrity run: nil service")
	}
	if ctx == nil {
		return result, errors.New("integrity run: nil context")
	}
	if residentFilter != nil {
		if err := residentFilter.Validate(); err != nil {
			return result, fmt.Errorf("integrity run: invalid resident filter: %w", err)
		}
	}

	integrityPipelineID, err := service.ids.New()
	if err != nil {
		return result, err
	}
	memoryStatusPipelineID, err := service.ids.New()
	if err != nil {
		return result, err
	}
	definitions, err := domain.IntegrityBootstrapPipelineDefinitions(
		integrityPipelineID, memoryStatusPipelineID,
	)
	if err != nil {
		return result, err
	}
	registration, err := service.writer.Submit(ctx, domain.RegisterPipelineVersionsCommand(definitions))
	if err != nil {
		return result, fmt.Errorf("integrity run: bootstrap pipelines: %w", err)
	}
	integrityDefinition, err := service.loadExactPipeline(
		ctx, integrity.IntegrityPipelineKind, integrity.IntegrityPipelineVersion,
	)
	if err != nil {
		return service.finishAfterError(ctx, residentFilter, result,
			fmt.Errorf("integrity run: resolve integrity pipeline: %w", err))
	}
	memoryStatusDefinition, err := service.loadExactPipeline(
		ctx, "memory_status", domain.MemoryStatusPipelineVersion,
	)
	if err != nil {
		return service.finishAfterError(ctx, residentFilter, result,
			fmt.Errorf("integrity run: resolve memory-status pipeline: %w", err))
	}
	for _, definition := range []domain.PipelineVersionDefinition{integrityDefinition, memoryStatusDefinition} {
		metadata, err := service.repository.IntegrityPipelineCommit(ctx, definition.ID)
		if err != nil {
			return service.finishAfterError(ctx, residentFilter, result,
				fmt.Errorf("integrity run: resolve pipeline commit: %w", err))
		}
		disposition := DispositionExisting
		if !registration.Commit.CommitID.IsZero() && metadata.CommitID == registration.Commit.CommitID {
			disposition = DispositionCreated
		}
		addCommit(&result, metadata, disposition, CommitEffects{PipelineVersionRegistered: true})
	}

	scan, err := service.scanner.Scan(ctx)
	if err != nil {
		return service.finishAfterError(ctx, residentFilter, result,
			fmt.Errorf("integrity run: capture candidates: %w", err))
	}
	result.CapturedHead, err = service.repository.IntegrityHeadMetadata(ctx, scan.CapturedHead)
	if err != nil {
		return service.finishAfterError(ctx, residentFilter, result,
			fmt.Errorf("integrity run: resolve captured head: %w", err))
	}

	grouped := groupCandidates(scan.Candidates, residentFilter)
	residentIDs := make([]canonical.ID, 0, len(grouped))
	for residentID := range grouped {
		residentIDs = append(residentIDs, residentID)
	}
	slices.SortFunc(residentIDs, func(left, right canonical.ID) int {
		if left.String() < right.String() {
			return -1
		}
		if left.String() > right.String() {
			return 1
		}
		return 0
	})

	for _, residentID := range residentIDs {
		planned, requiresMemoryStatus, err := service.plan(grouped[residentID])
		if err != nil {
			return service.finishAfterError(ctx, residentFilter, result, err)
		}
		request := integrity.RecordFindings{
			ResidentID: residentID, CapturedHead: scan.CapturedHead,
			PipelineVersionID: integrityDefinition.ID, Findings: planned,
		}
		if requiresMemoryStatus {
			memoryID := memoryStatusDefinition.ID
			request.MemoryStatusPipelineVersionID = &memoryID
		}
		applied, err := service.writer.Submit(ctx, domain.RecordIntegrityFindingsCommand(request))
		if err != nil {
			return service.finishAfterError(ctx, residentFilter, result,
				fmt.Errorf("integrity run: apply resident %s: %w", residentID, err))
		}
		recorded, ok := applied.Value.(integrity.RecordFindingsResult)
		if !ok {
			return service.finishAfterError(ctx, residentFilter, result,
				fmt.Errorf("integrity run: apply resident %s returned %T", residentID, applied.Value))
		}
		result.ExistingFindings += len(recorded.Existing)
		result.CreatedFindings += len(recorded.Created)
		result.CreatedQuarantines += len(recorded.Quarantined)
		for _, existing := range recorded.Existing {
			addCommit(&result, existing.Commit, DispositionExisting, CommitEffects{
				IntegrityFindingRecorded: true,
			})
		}
		if !applied.Commit.CommitID.IsZero() {
			addCommit(&result, applied.Commit, DispositionCreated, CommitEffects{
				IntegrityFindingRecorded: len(recorded.Created) > 0,
				ClaimStatusQuarantined:   len(recorded.Quarantined) > 0,
			})
		}
	}

	return service.finish(ctx, residentFilter, result)
}

func addCommit(result *Result, metadata canonical.CommitMetadata, disposition Disposition, effects CommitEffects) {
	if result == nil || metadata.CommitID.IsZero() {
		return
	}
	for index := range result.CanonicalCommits {
		if result.CanonicalCommits[index].Metadata.CommitID != metadata.CommitID {
			continue
		}
		entry := &result.CanonicalCommits[index]
		if disposition == DispositionCreated {
			entry.Disposition = DispositionCreated
		}
		entry.Effects.PipelineVersionRegistered = entry.Effects.PipelineVersionRegistered || effects.PipelineVersionRegistered
		entry.Effects.IntegrityFindingRecorded = entry.Effects.IntegrityFindingRecorded || effects.IntegrityFindingRecorded
		entry.Effects.ClaimStatusQuarantined = entry.Effects.ClaimStatusQuarantined || effects.ClaimStatusQuarantined
		return
	}
	result.CanonicalCommits = append(result.CanonicalCommits, Commit{
		Metadata: metadata, Disposition: disposition, Effects: effects,
	})
	slices.SortFunc(result.CanonicalCommits, func(left, right Commit) int {
		if left.Metadata.CommitSeq < right.Metadata.CommitSeq {
			return -1
		}
		if left.Metadata.CommitSeq > right.Metadata.CommitSeq {
			return 1
		}
		return 0
	})
}

func (service *Service) loadExactPipeline(
	ctx context.Context,
	kind, version string,
) (domain.PipelineVersionDefinition, error) {
	actual, err := service.repository.PipelineVersion(ctx, kind, version)
	if err != nil {
		return domain.PipelineVersionDefinition{}, err
	}
	if err := actual.ID.Validate(); err != nil {
		return domain.PipelineVersionDefinition{}, err
	}
	want, err := canonical.MarshalCanonical(struct {
		Version string `json:"version"`
	}{Version: version})
	if err != nil {
		return domain.PipelineVersionDefinition{}, err
	}
	if actual.Kind != kind || actual.VersionKey != version || actual.Definition.String() != want.String() {
		return domain.PipelineVersionDefinition{}, fmt.Errorf("pipeline %s/%s is not the exact v1 definition", kind, version)
	}
	return actual, nil
}

func groupCandidates(
	candidates []integrity.Candidate,
	residentFilter *canonical.ID,
) map[canonical.ID][]integrity.Candidate {
	result := make(map[canonical.ID][]integrity.Candidate)
	for _, candidate := range candidates {
		if residentFilter != nil && candidate.ResidentID != *residentFilter {
			continue
		}
		result[candidate.ResidentID] = append(result[candidate.ResidentID], candidate)
	}
	return result
}

func (service *Service) plan(
	candidates []integrity.Candidate,
) ([]integrity.PlannedFinding, bool, error) {
	planned := make([]integrity.PlannedFinding, 0, len(candidates))
	claimTransitions := make(map[canonical.ID]canonical.ID)
	requiresMemoryStatus := false
	for _, candidate := range candidates {
		findingID, err := service.ids.New()
		if err != nil {
			return nil, false, err
		}
		finding := integrity.PlannedFinding{FindingID: findingID, Candidate: candidate}
		if candidate.RequiresQuarantine() {
			transitionID, exists := claimTransitions[candidate.TargetID]
			if !exists {
				transitionID, err = service.ids.New()
				if err != nil {
					return nil, false, err
				}
				claimTransitions[candidate.TargetID] = transitionID
			}
			finding.QuarantineTransitionID = &transitionID
			requiresMemoryStatus = true
		}
		planned = append(planned, finding)
	}
	return planned, requiresMemoryStatus, nil
}

func (service *Service) finish(
	ctx context.Context,
	residentFilter *canonical.ID,
	result Result,
) (Result, error) {
	finalScan, err := service.scanner.Scan(ctx)
	if err != nil {
		return result, fmt.Errorf("integrity run: capture result head: %w", err)
	}
	result.ResultHead, err = service.repository.IntegrityHeadMetadata(ctx, finalScan.CapturedHead)
	if err != nil {
		return result, fmt.Errorf("integrity run: resolve result head: %w", err)
	}
	for _, candidate := range finalScan.Candidates {
		if residentFilter != nil && candidate.ResidentID != *residentFilter {
			continue
		}
		if candidate.ReadinessBlocking() {
			result.ReadinessBlocked = true
			break
		}
	}
	return result, nil
}

func (service *Service) finishAfterError(
	ctx context.Context,
	residentFilter *canonical.ID,
	result Result,
	cause error,
) (Result, error) {
	finished, finishErr := service.finish(ctx, residentFilter, result)
	return finished, errors.Join(cause, finishErr)
}
