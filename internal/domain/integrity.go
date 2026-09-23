package domain

import (
	"context"
	"fmt"

	"mahoroba.local/mahoroba/internal/canonical"
	"mahoroba.local/mahoroba/internal/integrity"
)

// IntegrityPipelineDefinition returns the one exact integrity-check-v1 JCS
// registration accepted by Slice 1.
func IntegrityPipelineDefinition(id canonical.ID) (RegisterPipelineVersions, error) {
	if err := id.Validate(); err != nil {
		return RegisterPipelineVersions{}, err
	}
	definition, err := canonical.MarshalCanonical(struct {
		Version string `json:"version"`
	}{Version: integrity.IntegrityPipelineVersion})
	if err != nil {
		return RegisterPipelineVersions{}, err
	}
	return RegisterPipelineVersions{Versions: []PipelineVersionDefinition{{
		ID: id, Kind: integrity.IntegrityPipelineKind,
		VersionKey: integrity.IntegrityPipelineVersion, Definition: definition,
	}}}, nil
}

// IntegrityBootstrapPipelineDefinitions returns the two exact, global
// pipeline definitions required before an M7 structural scan can be applied.
// Keeping them in one RegisterPipelineVersions value makes first-time
// registration one Canonical unit of work; callers must query the persisted
// rows afterwards because an idempotent retry can retain earlier IDs.
func IntegrityBootstrapPipelineDefinitions(
	integrityID, memoryStatusID canonical.ID,
) (RegisterPipelineVersions, error) {
	integrityDefinition, err := IntegrityPipelineDefinition(integrityID)
	if err != nil {
		return RegisterPipelineVersions{}, err
	}
	if err := memoryStatusID.Validate(); err != nil {
		return RegisterPipelineVersions{}, err
	}
	memoryStatusDefinition, err := canonical.MarshalCanonical(struct {
		Version string `json:"version"`
	}{Version: MemoryStatusPipelineVersion})
	if err != nil {
		return RegisterPipelineVersions{}, err
	}
	return RegisterPipelineVersions{Versions: []PipelineVersionDefinition{
		integrityDefinition.Versions[0],
		{
			ID: memoryStatusID, Kind: "memory_status",
			VersionKey: MemoryStatusPipelineVersion, Definition: memoryStatusDefinition,
		},
	}}, nil
}

type integrityFindingMutator interface {
	RecordIntegrityFindings(context.Context, integrity.RecordFindings) (integrity.RecordFindingsResult, error)
}

func RecordIntegrityFindingsCommand(value integrity.RecordFindings) canonical.Command {
	scope, _ := canonical.ResidentScope(value.ResidentID)
	return command{
		name: "RecordIntegrityFindings", scope: scope,
		validate: value.Validate,
		execute: func(ctx context.Context, store MutationStore) (any, error) {
			mutator, ok := store.(integrityFindingMutator)
			if !ok {
				return nil, fmt.Errorf("domain: canonical UoW lacks integrity finding capability")
			}
			return mutator.RecordIntegrityFindings(ctx, value)
		},
	}
}
