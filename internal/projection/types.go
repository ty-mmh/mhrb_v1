// Package projection defines rebuildable, derived views over Canonical data.
//
// The package deliberately keeps SQL details behind Source and Store. A Source
// must evaluate against the captured Target supplied in an EvaluationRequest;
// a Store must apply the derived body, dependency set, and watermark in one
// transaction and compare Observed as a CAS precondition.
package projection

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"

	"mahoroba.local/mahoroba/internal/canonical"
)

type Name string
type Version string

const (
	ResidentCurrentStatusName   Name = "resident_current_status"
	ResidentCurrentRevisionName Name = "resident_current_revision"
	RuntimeStatesName           Name = "runtime_states"
	ClaimStatesName             Name = "claim_states"
	ClaimViewScopeCurrentName   Name = "claim_view_scope_current"
)

type DependencyKind string

const (
	SessionizationPolicyDependency DependencyKind = "sessionization_policy"
	MemoryPolicyDependency         DependencyKind = "memory_policy"
)

var (
	ErrUndeclaredProjection           = errors.New("projection: undeclared projection")
	ErrUnknownDependency              = errors.New("projection: unknown dependency")
	ErrUnresolvedDependency           = errors.New("projection: unresolved dependency")
	ErrInvalidWatermarkMetadata       = errors.New("projection: invalid watermark metadata")
	ErrAsOfRegression                 = errors.New("projection: as_of regression")
	ErrSourceRegression               = errors.New("projection: source commit regression")
	ErrCASConflict                    = errors.New("projection: watermark CAS conflict")
	ErrCoordinatorStopped             = errors.New("projection: coordinator is stopped")
	ErrClaimStateEvaluatorUnavailable = errors.New("projection: claim state evaluator is not configured")
	ErrPreflightHeadChanged           = errors.New("projection: Canonical head changed during startup preflight")
)

// ServiceRequiredNames is the closed Projection set required before the
// selected active resident may accept dialogue. The returned slice is a copy
// so callers cannot weaken the process-wide readiness contract.
func ServiceRequiredNames() []Name {
	return []Name{
		ResidentCurrentStatusName,
		ResidentCurrentRevisionName,
		RuntimeStatesName,
		ClaimStatesName,
		ClaimViewScopeCurrentName,
	}
}

func (name Name) validate() error {
	if name == "" || strings.TrimSpace(string(name)) != string(name) {
		return fmt.Errorf("projection: invalid projection name %q", name)
	}
	return nil
}

func (version Version) validate() error {
	if version == "" || strings.TrimSpace(string(version)) != string(version) {
		return fmt.Errorf("projection: invalid projection version %q", version)
	}
	return nil
}

func (kind DependencyKind) Validate() error {
	switch kind {
	case SessionizationPolicyDependency, MemoryPolicyDependency:
		return nil
	default:
		return fmt.Errorf("%w: %q", ErrUnknownDependency, kind)
	}
}

type Dependency struct {
	Kind      DependencyKind `json:"kind"`
	VersionID canonical.ID   `json:"version_id"`
}

func (dependency Dependency) Validate() error {
	if err := dependency.Kind.Validate(); err != nil {
		return err
	}
	if err := dependency.VersionID.Validate(); err != nil {
		return fmt.Errorf("projection: invalid %s dependency version: %w", dependency.Kind, err)
	}
	return nil
}

// DependencySetEqual compares complete sets rather than treating a stored set
// as a subset. Ordering has no meaning, but duplicates make a set invalid.
func DependencySetEqual(left, right []Dependency) (bool, error) {
	leftKeys, err := dependencyKeys(left)
	if err != nil {
		return false, err
	}
	rightKeys, err := dependencyKeys(right)
	if err != nil {
		return false, err
	}
	return slices.Equal(leftKeys, rightKeys), nil
}

func dependencyKeys(dependencies []Dependency) ([]string, error) {
	keys := make([]string, 0, len(dependencies))
	seen := make(map[string]struct{}, len(dependencies))
	for _, dependency := range dependencies {
		if err := dependency.Validate(); err != nil {
			return nil, err
		}
		key := string(dependency.Kind) + "\x00" + dependency.VersionID.String()
		if _, exists := seen[key]; exists {
			return nil, fmt.Errorf("projection: duplicate dependency %s/%s", dependency.Kind, dependency.VersionID)
		}
		seen[key] = struct{}{}
		keys = append(keys, key)
	}
	slices.Sort(keys)
	return keys, nil
}

type Definition struct {
	Name                Name
	Version             Version
	TimeSensitive       bool
	Dependencies        []DependencyKind
	RebuildOnActivation []DependencyKind
}

func (definition Definition) Validate() error {
	if err := definition.Name.validate(); err != nil {
		return err
	}
	if err := definition.Version.validate(); err != nil {
		return err
	}
	declared := make(map[DependencyKind]struct{}, len(definition.Dependencies))
	for _, kind := range definition.Dependencies {
		if err := kind.Validate(); err != nil {
			return err
		}
		if _, exists := declared[kind]; exists {
			return fmt.Errorf("projection: %s declares dependency %q more than once", definition.Name, kind)
		}
		declared[kind] = struct{}{}
	}
	activated := make(map[DependencyKind]struct{}, len(definition.RebuildOnActivation))
	for _, kind := range definition.RebuildOnActivation {
		if err := kind.Validate(); err != nil {
			return err
		}
		if _, exists := activated[kind]; exists {
			return fmt.Errorf("projection: %s repeats activation dependency %q", definition.Name, kind)
		}
		activated[kind] = struct{}{}
	}
	return nil
}

// Target is captured before dependency resolution and evaluation begins.
// Source implementations must not return Canonical rows whose commit_seq is
// greater than Head.CommitSeq.
type Target struct {
	Head   canonical.Head
	AsOf   canonical.Instant
	AsOfTZ canonical.Timezone
}

func (target Target) Validate() error {
	if err := target.Head.Validate(); err != nil {
		return fmt.Errorf("projection: invalid target head: %w", err)
	}
	if err := target.AsOfTZ.Validate(); err != nil {
		return fmt.Errorf("projection: invalid target timezone: %w", err)
	}
	return nil
}

type Watermark struct {
	ProjectionName    Name                `json:"name"`
	ResidentID        canonical.ID        `json:"resident_id"`
	ProjectionVersion Version             `json:"version"`
	SourceCommitSeq   canonical.CommitSeq `json:"source_commit_seq"`
	AsOf              canonical.Instant   `json:"as_of"`
	AsOfTZ            canonical.Timezone  `json:"as_of_tz"`
	Dependencies      []Dependency        `json:"dependencies"`
}

func (watermark Watermark) Validate() error {
	if err := watermark.ProjectionName.validate(); err != nil {
		return err
	}
	if err := watermark.ProjectionVersion.validate(); err != nil {
		return err
	}
	if err := watermark.ResidentID.Validate(); err != nil {
		return fmt.Errorf("projection: invalid watermark resident: %w", err)
	}
	if err := watermark.SourceCommitSeq.Validate(); err != nil {
		return fmt.Errorf("projection: invalid watermark cursor: %w", err)
	}
	if err := watermark.AsOfTZ.Validate(); err != nil {
		return fmt.Errorf("projection: invalid watermark timezone: %w", err)
	}
	_, err := dependencyKeys(watermark.Dependencies)
	return err
}

func (watermark Watermark) clone() Watermark {
	watermark.Dependencies = append([]Dependency(nil), watermark.Dependencies...)
	return watermark
}

type DependencyRequest struct {
	Definition Definition
	ResidentID canonical.ID
	Target     Target
	Previous   *Watermark
}

type ActivationRequest struct {
	Definition Definition
	Kind       DependencyKind
	ResidentID canonical.ID
	After      canonical.CommitSeq
	Through    canonical.CommitSeq
}

type EvaluationRequest struct {
	Definition   Definition
	ResidentID   canonical.ID
	Target       Target
	Previous     *Watermark
	Dependencies []Dependency
	Plan         UpdatePlan
}

// Evaluation is opaque to the coordinator. Store and Source adapters agree on
// its concrete Value type without exposing a database transaction here.
type Evaluation struct {
	Value any
}

// Source owns Canonical reads and evaluator dispatch. Evaluate must constrain
// every read to request.Target.Head.CommitSeq even if the database advances
// after Head returned.
type Source interface {
	Head(ctx context.Context) (canonical.Head, error)
	// Residents must only consider rows at or before target.Head.CommitSeq.
	Residents(ctx context.Context, target Target) ([]canonical.ID, error)
	ResolveDependencies(ctx context.Context, request DependencyRequest) ([]Dependency, error)
	DependencyActivated(ctx context.Context, request ActivationRequest) (bool, error)
	Evaluate(ctx context.Context, request EvaluationRequest) (Evaluation, error)
}

type ApplyRequest struct {
	Definition Definition
	ResidentID canonical.ID
	Observed   *Watermark
	Plan       UpdatePlan
	Evaluation Evaluation
	Watermark  Watermark
}

type DropRequest struct {
	Definition Definition
	ResidentID canonical.ID
	Observed   *Watermark
}

// Store is logically separate from the Canonical adapter. Apply must perform
// a CAS check against Observed and atomically replace/update the Projection
// body, complete dependency set, and Watermark.
type Store interface {
	Watermark(ctx context.Context, name Name, residentID canonical.ID) (Watermark, bool, error)
	Apply(ctx context.Context, request ApplyRequest) error
	Drop(ctx context.Context, request DropRequest) error
}
