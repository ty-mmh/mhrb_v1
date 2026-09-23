package projection

import (
	"errors"
	"fmt"

	"mahoroba.local/mahoroba/internal/canonical"
)

type UpdateKind string

const (
	FullBuild   UpdateKind = "full_build"
	FullRebuild UpdateKind = "full_rebuild"
	Update      UpdateKind = "update"
	UpToDate    UpdateKind = "up_to_date"
)

type PlanReason string

const (
	ReasonUnbuilt              PlanReason = "watermark_missing"
	ReasonVersionMismatch      PlanReason = "version_mismatch"
	ReasonDependencyMismatch   PlanReason = "dependency_mismatch"
	ReasonDependencyActivation PlanReason = "dependency_activation"
	ReasonCursorAdvance        PlanReason = "cursor_advance"
	ReasonAsOfAdvance          PlanReason = "as_of_advance"
	ReasonCursorAndAsOfAdvance PlanReason = "cursor_and_as_of_advance"
	ReasonCurrent              PlanReason = "current"
	ReasonManualRebuild        PlanReason = "manual_rebuild"
)

type UpdatePlan struct {
	Kind                 UpdateKind `json:"kind"`
	Reason               PlanReason `json:"reason"`
	NeedCommitCatchUp    bool       `json:"need_commit_catch_up"`
	NeedAsOfReEvaluation bool       `json:"need_as_of_re_evaluation"`
}

func (plan UpdatePlan) ChangesState() bool { return plan.Kind != UpToDate }

type PlanInput struct {
	Definition          Definition
	ResidentID          canonical.ID
	Target              Target
	Stored              *Watermark
	DesiredDependencies []Dependency
	ActivationDetected  bool
}

// PlanUpdate follows the fixed decision order. It first chooses full build or
// full rebuild conditions, then independently computes commit catch-up and
// as-of re-evaluation.
func PlanUpdate(input PlanInput) (UpdatePlan, error) {
	if err := input.Definition.Validate(); err != nil {
		return UpdatePlan{}, err
	}
	if err := input.ResidentID.Validate(); err != nil {
		return UpdatePlan{}, fmt.Errorf("projection: invalid resident: %w", err)
	}
	if err := input.Target.Validate(); err != nil {
		return UpdatePlan{}, err
	}
	if err := validateDesiredDependencies(input.Definition, input.DesiredDependencies); err != nil {
		return UpdatePlan{}, err
	}
	if !input.Target.Head.Exists {
		return UpdatePlan{}, errors.New("projection: cannot persist a resident projection at an empty Canonical head")
	}

	if input.Stored == nil {
		return UpdatePlan{Kind: FullBuild, Reason: ReasonUnbuilt, NeedCommitCatchUp: true, NeedAsOfReEvaluation: input.Definition.TimeSensitive}, nil
	}
	stored := input.Stored
	if err := stored.Validate(); err != nil {
		return UpdatePlan{}, err
	}
	if err := validateStoredDependencyMetadata(input.Definition, stored.Dependencies); err != nil {
		return UpdatePlan{}, err
	}
	if stored.ProjectionName != input.Definition.Name || stored.ResidentID != input.ResidentID {
		return UpdatePlan{}, errors.New("projection: stored watermark identity does not match update target")
	}
	if input.Target.AsOf < stored.AsOf {
		return UpdatePlan{}, fmt.Errorf("%w: stored=%s target=%s", ErrAsOfRegression, stored.AsOf, input.Target.AsOf)
	}
	if input.Target.Head.CommitSeq < stored.SourceCommitSeq {
		return UpdatePlan{}, fmt.Errorf("%w: stored=%s target=%s", ErrSourceRegression, stored.SourceCommitSeq, input.Target.Head.CommitSeq)
	}
	if stored.ProjectionVersion != input.Definition.Version {
		return UpdatePlan{Kind: FullRebuild, Reason: ReasonVersionMismatch, NeedCommitCatchUp: true, NeedAsOfReEvaluation: input.Definition.TimeSensitive}, nil
	}
	dependenciesEqual, err := DependencySetEqual(stored.Dependencies, input.DesiredDependencies)
	if err != nil {
		return UpdatePlan{}, err
	}
	if !dependenciesEqual {
		return UpdatePlan{Kind: FullRebuild, Reason: ReasonDependencyMismatch, NeedCommitCatchUp: true, NeedAsOfReEvaluation: input.Definition.TimeSensitive}, nil
	}
	if input.ActivationDetected {
		if len(input.Definition.RebuildOnActivation) == 0 {
			return UpdatePlan{}, errors.New("projection: activation reported for a definition without an activation rebuild rule")
		}
		return UpdatePlan{Kind: FullRebuild, Reason: ReasonDependencyActivation, NeedCommitCatchUp: true, NeedAsOfReEvaluation: input.Definition.TimeSensitive}, nil
	}

	needCommit := stored.SourceCommitSeq < input.Target.Head.CommitSeq
	needAsOf := input.Definition.TimeSensitive && stored.AsOf < input.Target.AsOf
	switch {
	case needCommit && needAsOf:
		return UpdatePlan{Kind: Update, Reason: ReasonCursorAndAsOfAdvance, NeedCommitCatchUp: true, NeedAsOfReEvaluation: true}, nil
	case needCommit:
		return UpdatePlan{Kind: Update, Reason: ReasonCursorAdvance, NeedCommitCatchUp: true}, nil
	case needAsOf:
		return UpdatePlan{Kind: Update, Reason: ReasonAsOfAdvance, NeedAsOfReEvaluation: true}, nil
	default:
		return UpdatePlan{Kind: UpToDate, Reason: ReasonCurrent}, nil
	}
}

// validateStoredDependencyMetadata keeps the one intentionally recoverable
// COV-4 upgrade shape (a legacy claim_states watermark with no dependency)
// separate from corrupt production metadata. A valid one-item dependency may
// still differ from desired and therefore cause a rebuild in PlanUpdate.
func validateStoredDependencyMetadata(definition Definition, dependencies []Dependency) error {
	if definition.Name != ClaimStatesName || len(definition.Dependencies) != 1 ||
		definition.Dependencies[0] != MemoryPolicyDependency {
		return nil
	}
	if len(dependencies) == 0 {
		return nil
	}
	if len(dependencies) != 1 || dependencies[0].Kind != MemoryPolicyDependency {
		return fmt.Errorf("%w: claim_states requires zero legacy dependencies or exactly one memory_policy dependency, got %d", ErrInvalidWatermarkMetadata, len(dependencies))
	}
	return nil
}

func validateDesiredDependencies(definition Definition, dependencies []Dependency) error {
	if _, err := dependencyKeys(dependencies); err != nil {
		return err
	}
	counts := make(map[DependencyKind]int, len(dependencies))
	declared := make(map[DependencyKind]struct{}, len(definition.Dependencies))
	for _, kind := range definition.Dependencies {
		declared[kind] = struct{}{}
	}
	for _, dependency := range dependencies {
		if _, exists := declared[dependency.Kind]; !exists {
			return fmt.Errorf("projection: %s returned undeclared dependency %q", definition.Name, dependency.Kind)
		}
		counts[dependency.Kind]++
	}
	for _, kind := range definition.Dependencies {
		if counts[kind] != 1 {
			return fmt.Errorf("%w: %s requires exactly one %s dependency, got %d", ErrUnresolvedDependency, definition.Name, kind, counts[kind])
		}
	}
	return nil
}

func targetWatermark(input PlanInput, plan UpdatePlan) Watermark {
	watermark := Watermark{
		ProjectionName:    input.Definition.Name,
		ResidentID:        input.ResidentID,
		ProjectionVersion: input.Definition.Version,
		Dependencies:      append([]Dependency(nil), input.DesiredDependencies...),
	}
	if input.Stored == nil || plan.Kind == FullBuild || plan.Kind == FullRebuild {
		watermark.SourceCommitSeq = input.Target.Head.CommitSeq
		watermark.AsOf = input.Target.AsOf
		watermark.AsOfTZ = input.Target.AsOfTZ
		return watermark
	}
	watermark.SourceCommitSeq = input.Stored.SourceCommitSeq
	watermark.AsOf = input.Stored.AsOf
	watermark.AsOfTZ = input.Stored.AsOfTZ
	if plan.NeedCommitCatchUp {
		watermark.SourceCommitSeq = input.Target.Head.CommitSeq
	}
	if plan.NeedAsOfReEvaluation {
		watermark.AsOf = input.Target.AsOf
		watermark.AsOfTZ = input.Target.AsOfTZ
	}
	return watermark
}
