package domain

import (
	"context"
	"fmt"

	"mahoroba.local/mahoroba/internal/canonical"
)

const (
	MemoryExtractionPipelineVersion      = "memory-extraction-v1"
	MemoryMaturationPipelineVersion      = "memory-maturation-v1"
	MemoryAlignmentPipelineVersion       = "memory-alignment-v1"
	MemoryRecallPipelineVersion          = "memory-recall-v1"
	MemoryStatusPipelineVersion          = "memory-status-v1"
	MemoryAbstractionPipelineVersion     = "memory-abstraction-v1"
	MemoryDifferentiationPipelineVersion = "memory-differentiation-v1"
	PersonaRevisionPipelineVersion       = "persona-revision-v1"
	SelfTalkPipelineVersion              = "self-talk-v1"
	OutboundInitiativePipelineVersion    = "outbound-initiative-v1"
)

type PipelineVersionDefinition struct {
	ID         canonical.ID
	Kind       string
	VersionKey string
	Definition canonical.CanonicalJSON
}

type RegisterPipelineVersions struct {
	Versions []PipelineVersionDefinition
}

type pipelineVersionMutator interface {
	RegisterPipelineVersions(context.Context, RegisterPipelineVersions) error
}

func RegisterPipelineVersionsCommand(value RegisterPipelineVersions) canonical.Command {
	return command{
		name: "RegisterPipelineVersions", scope: canonical.GlobalScope(),
		validate: func() error {
			if len(value.Versions) == 0 {
				return fmt.Errorf("domain: at least one pipeline version is required")
			}
			ids := make(map[canonical.ID]struct{}, len(value.Versions))
			keys := make(map[string]struct{}, len(value.Versions))
			for _, version := range value.Versions {
				if err := version.ID.Validate(); err != nil {
					return err
				}
				if version.Kind == "" || version.VersionKey == "" || version.Definition.IsZero() {
					return fmt.Errorf("domain: incomplete pipeline version")
				}
				if err := ValidateExactDialoguePipelineDefinition(version); err != nil {
					return err
				}
				if _, exists := ids[version.ID]; exists {
					return fmt.Errorf("domain: duplicate pipeline version ID")
				}
				ids[version.ID] = struct{}{}
				key := version.Kind + "\x00" + version.VersionKey
				if _, exists := keys[key]; exists {
					return fmt.Errorf("domain: duplicate pipeline kind/version key")
				}
				keys[key] = struct{}{}
			}
			return nil
		},
		execute: func(ctx context.Context, store MutationStore) (any, error) {
			mutator, ok := store.(pipelineVersionMutator)
			if !ok {
				return nil, fmt.Errorf("domain: canonical UoW lacks pipeline registration capability")
			}
			return nil, mutator.RegisterPipelineVersions(ctx, value)
		},
	}
}

func MemoryPipelineDefinitions(ids []canonical.ID) (RegisterPipelineVersions, error) {
	versions := []struct {
		kind string
		key  string
	}{
		{"memory_extraction", MemoryExtractionPipelineVersion},
		{"memory_maturation", MemoryMaturationPipelineVersion},
		{"memory_alignment", MemoryAlignmentPipelineVersion},
		{"memory_recall", MemoryRecallPipelineVersion},
		{"memory_status", MemoryStatusPipelineVersion},
		{"memory_abstraction", MemoryAbstractionPipelineVersion},
		{"memory_differentiation", MemoryDifferentiationPipelineVersion},
		{"persona_revision", PersonaRevisionPipelineVersion},
	}
	if len(ids) != len(versions) {
		return RegisterPipelineVersions{}, fmt.Errorf("domain: memory pipeline registration requires %d IDs", len(versions))
	}
	result := RegisterPipelineVersions{Versions: make([]PipelineVersionDefinition, len(versions))}
	for index, version := range versions {
		definition, err := canonical.MarshalCanonical(struct {
			Version string `json:"version"`
		}{Version: version.key})
		if err != nil {
			return RegisterPipelineVersions{}, err
		}
		result.Versions[index] = PipelineVersionDefinition{
			ID: ids[index], Kind: version.kind, VersionKey: version.key, Definition: definition,
		}
	}
	return result, nil
}

// AutonomyPipelineDefinitions registers only the M6 generation pipelines.
// A resident activating memory-policy-v3 still needs MemoryPipelineDefinitions
// as well because self-talk extraction and the M5 memory lifecycle remain
// independently versioned.
func AutonomyPipelineDefinitions(ids []canonical.ID) (RegisterPipelineVersions, error) {
	versions := []struct {
		kind string
		key  string
	}{
		{"self_talk", SelfTalkPipelineVersion},
		{"outbound_initiative", OutboundInitiativePipelineVersion},
	}
	if len(ids) != len(versions) {
		return RegisterPipelineVersions{}, fmt.Errorf("domain: autonomy pipeline registration requires %d IDs", len(versions))
	}
	result := RegisterPipelineVersions{Versions: make([]PipelineVersionDefinition, len(versions))}
	for index, version := range versions {
		definition, err := canonical.MarshalCanonical(struct {
			Version string `json:"version"`
		}{Version: version.key})
		if err != nil {
			return RegisterPipelineVersions{}, err
		}
		result.Versions[index] = PipelineVersionDefinition{
			ID: ids[index], Kind: version.kind, VersionKey: version.key, Definition: definition,
		}
	}
	return result, nil
}
