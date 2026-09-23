package projection

import (
	"fmt"
	"slices"
)

type Registry struct {
	definitions map[Name]Definition
	names       []Name
}

func NewRegistry(definitions ...Definition) (*Registry, error) {
	registry := &Registry{definitions: make(map[Name]Definition, len(definitions))}
	for _, definition := range definitions {
		if err := definition.Validate(); err != nil {
			return nil, err
		}
		if _, exists := registry.definitions[definition.Name]; exists {
			return nil, fmt.Errorf("projection: duplicate definition %q", definition.Name)
		}
		definition.Dependencies = append([]DependencyKind(nil), definition.Dependencies...)
		definition.RebuildOnActivation = append([]DependencyKind(nil), definition.RebuildOnActivation...)
		registry.definitions[definition.Name] = definition
		registry.names = append(registry.names, definition.Name)
	}
	slices.Sort(registry.names)
	return registry, nil
}

func (registry *Registry) Definition(name Name) (Definition, error) {
	if registry == nil {
		return Definition{}, fmt.Errorf("%w: %q", ErrUndeclaredProjection, name)
	}
	definition, exists := registry.definitions[name]
	if !exists {
		return Definition{}, fmt.Errorf("%w: %q", ErrUndeclaredProjection, name)
	}
	definition.Dependencies = append([]DependencyKind(nil), definition.Dependencies...)
	definition.RebuildOnActivation = append([]DependencyKind(nil), definition.RebuildOnActivation...)
	return definition, nil
}

func (registry *Registry) Definitions() []Definition {
	if registry == nil {
		return nil
	}
	definitions := make([]Definition, 0, len(registry.names))
	for _, name := range registry.names {
		definition, _ := registry.Definition(name)
		definitions = append(definitions, definition)
	}
	return definitions
}

func (registry *Registry) Names() []Name {
	if registry == nil {
		return nil
	}
	return append([]Name(nil), registry.names...)
}
