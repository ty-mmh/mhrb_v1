package memory

import (
	"fmt"

	"mahoroba.local/mahoroba/internal/canonical"
)

type AlignmentGate struct {
	Kind                      ClaimKind
	Status                    ClaimStatus
	Stage                     ClaimStage
	Confidence                canonical.Ratio
	HasExternalSupport        bool
	HasPerspectiveUserSupport bool
}

type MaturationInput struct {
	CurrentStage ClaimStage
	Kind         ClaimKind
	Evidence     EvidenceAggregate
	Alignment    *AlignmentGate
}

type MaturationMetrics struct {
	DistinctSupportEvents     canonical.Count  `json:"distinct_support_events"`
	SupportWeight             canonical.Weight `json:"support_weight"`
	Confidence                canonical.Ratio  `json:"confidence"`
	HasExternalSupport        bool             `json:"has_external_support"`
	HasTrustedSupport         bool             `json:"has_trusted_support"`
	HasSubjectUserSupport     bool             `json:"has_subject_user_support"`
	HasPerspectiveUserSupport bool             `json:"has_perspective_user_support"`
	AlignmentSatisfied        bool             `json:"alignment_satisfied"`
}

func (metrics MaturationMetrics) CanonicalJSON() (canonical.CanonicalJSON, error) {
	return canonical.MarshalCanonical(metrics)
}

type MaturationDecision struct {
	Advance        bool
	From           *ClaimStage
	To             ClaimStage
	Reason         StageReason
	Metrics        MaturationMetrics
	BlockingReason string
}

func InitialMaturationDecision() MaturationDecision {
	return MaturationDecision{Advance: true, To: StageFloating, Reason: StageReasonInitialExtraction}
}

// EvaluateMaturation advances at most one irreversible stage per evaluation.
// This prevents a newly-created claim from skipping its durable floating and
// sediment transitions even when a batch already satisfies later thresholds.
func EvaluateMaturation(policy Policy, input MaturationInput) (MaturationDecision, error) {
	if err := policy.RequireEnabled(); err != nil {
		return MaturationDecision{}, err
	}
	if err := input.CurrentStage.Validate(); err != nil {
		return MaturationDecision{}, fmt.Errorf("%w: %v", ErrInvalidMaturation, err)
	}
	if err := input.Kind.Validate(); err != nil {
		return MaturationDecision{}, fmt.Errorf("%w: %v", ErrInvalidMaturation, err)
	}
	if err := input.Evidence.SupportWeight.Validate(); err != nil {
		return MaturationDecision{}, fmt.Errorf("%w: support weight: %v", ErrInvalidMaturation, err)
	}
	if err := input.Evidence.ContradictWeight.Validate(); err != nil {
		return MaturationDecision{}, fmt.Errorf("%w: contradict weight: %v", ErrInvalidMaturation, err)
	}
	if err := input.Evidence.Confidence.Validate(); err != nil {
		return MaturationDecision{}, fmt.Errorf("%w: confidence: %v", ErrInvalidMaturation, err)
	}
	if err := input.Evidence.DistinctSupportEvents.Validate(); err != nil {
		return MaturationDecision{}, fmt.Errorf("%w: distinct support events: %v", ErrInvalidMaturation, err)
	}

	from := input.CurrentStage
	decision := MaturationDecision{
		From: &from, To: input.CurrentStage,
		Metrics: MaturationMetrics{
			DistinctSupportEvents:     input.Evidence.DistinctSupportEvents,
			SupportWeight:             input.Evidence.SupportWeight,
			Confidence:                input.Evidence.Confidence,
			HasExternalSupport:        input.Evidence.HasExternalSupport,
			HasTrustedSupport:         input.Evidence.HasTrustedSupport,
			HasSubjectUserSupport:     input.Evidence.HasSubjectUserSupport,
			HasPerspectiveUserSupport: input.Evidence.HasPerspectiveUserSupport,
		},
	}

	switch input.CurrentStage {
	case StageFloating:
		if input.Evidence.DistinctSupportEvents.Int64() < policy.Maturation.SedimentDistinctEvents {
			decision.BlockingReason = "insufficient_distinct_support_events"
			return decision, nil
		}
		if input.Evidence.SupportWeight.Millionths() < policy.Maturation.SedimentSupportWeight {
			decision.BlockingReason = "insufficient_support_weight"
			return decision, nil
		}
		decision.Advance = true
		decision.To = StageSediment
		decision.Reason = StageReasonMaturationThreshold
		return decision, nil
	case StageSediment:
		if !input.Evidence.HasTrustedSupport {
			decision.BlockingReason = "trusted_support_required"
			return decision, nil
		}
		if input.Evidence.DistinctSupportEvents.Int64() < policy.Maturation.SettledDistinctEvents {
			decision.BlockingReason = "insufficient_distinct_support_events"
			return decision, nil
		}
		if input.Evidence.SupportWeight.Millionths() < policy.Maturation.SettledSupportWeight {
			decision.BlockingReason = "insufficient_support_weight"
			return decision, nil
		}
		if input.Evidence.Confidence.Millionths() < policy.Maturation.SettledConfidence {
			decision.BlockingReason = "insufficient_confidence"
			return decision, nil
		}
		reason, satisfied, err := settledProvenanceGate(policy, input)
		if err != nil {
			return MaturationDecision{}, err
		}
		decision.Metrics.AlignmentSatisfied = reason == StageReasonExternalAlignment && satisfied
		if !satisfied {
			decision.BlockingReason = "settled_provenance_gate"
			return decision, nil
		}
		decision.Advance = true
		decision.To = StageSettled
		decision.Reason = reason
		return decision, nil
	case StageSettled:
		decision.BlockingReason = "already_settled"
		return decision, nil
	default:
		return MaturationDecision{}, fmt.Errorf("%w: unsupported current stage %q", ErrInvalidMaturation, input.CurrentStage)
	}
}

func settledProvenanceGate(policy Policy, input MaturationInput) (StageReason, bool, error) {
	switch input.Kind {
	case ClaimKindDirect:
		if input.Alignment == nil {
			return StageReasonExternalAlignment, false, nil
		}
		alignment := input.Alignment
		if err := alignment.Kind.Validate(); err != nil {
			return "", false, fmt.Errorf("%w: alignment kind: %v", ErrInvalidMaturation, err)
		}
		if err := alignment.Status.Validate(); err != nil {
			return "", false, fmt.Errorf("%w: alignment status: %v", ErrInvalidMaturation, err)
		}
		if err := alignment.Stage.Validate(); err != nil {
			return "", false, fmt.Errorf("%w: alignment stage: %v", ErrInvalidMaturation, err)
		}
		if err := alignment.Confidence.Validate(); err != nil {
			return "", false, fmt.Errorf("%w: alignment confidence: %v", ErrInvalidMaturation, err)
		}
		satisfied := alignment.Kind == ClaimKindMeta && alignment.Status == StatusActive &&
			stageAtLeast(alignment.Stage, policy.Maturation.MetaMinimumStage) &&
			alignment.Confidence.Millionths() >= policy.Maturation.AlignmentConfidence &&
			alignment.HasExternalSupport &&
			alignment.HasPerspectiveUserSupport
		return StageReasonExternalAlignment, satisfied, nil
	case ClaimKindOther, ClaimKindUnclassified:
		return StageReasonMaturationThreshold, input.Evidence.HasSubjectUserSupport, nil
	case ClaimKindMeta:
		return StageReasonMaturationThreshold, input.Evidence.HasPerspectiveUserSupport, nil
	default:
		return "", false, fmt.Errorf("%w: unknown claim kind %q", ErrInvalidMaturation, input.Kind)
	}
}

func stageAtLeast(actual, minimum ClaimStage) bool {
	order := func(stage ClaimStage) int {
		switch stage {
		case StageFloating:
			return 0
		case StageSediment:
			return 1
		case StageSettled:
			return 2
		default:
			return -1
		}
	}
	return order(actual) >= order(minimum)
}
