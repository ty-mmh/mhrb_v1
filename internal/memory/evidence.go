package memory

import (
	"fmt"

	"mahoroba.local/mahoroba/internal/canonical"
)

type EvidencePoint struct {
	SourceEventID      canonical.ID
	SourceEvidenceID   *canonical.ID
	EventType          EventType
	Polarity           EvidencePolarity
	Grade              EvidenceGrade
	Trust              TrustLevel
	Derivation         EvidenceDerivation
	InheritanceDepth   int64
	Reason             EvidenceReason
	ActorIsSubject     bool
	ActorIsPerspective bool
}

type EvaluatedEvidence struct {
	EvidencePoint
	EffectiveGrade  EvidenceGrade
	EffectiveWeight canonical.Weight
}

type EvidenceAggregate struct {
	SupportWeight             canonical.Weight
	ContradictWeight          canonical.Weight
	Confidence                canonical.Ratio
	DistinctSupportEvents     canonical.Count
	HasTrustedSupport         bool
	HasExternalSupport        bool
	HasSubjectUserSupport     bool
	HasPerspectiveUserSupport bool
}

// EvaluateEvidence applies the recorded policy pointwise. A self-talk source
// is always downgraded to inferred. Inherited evidence is additionally reduced
// once by the v0 inheritance multiplier and may not exceed max_depth.
func EvaluateEvidence(policy Policy, point EvidencePoint) (EvaluatedEvidence, error) {
	if err := policy.RequireEnabled(); err != nil {
		return EvaluatedEvidence{}, err
	}
	if err := point.SourceEventID.Validate(); err != nil {
		return EvaluatedEvidence{}, fmt.Errorf("%w: source event: %v", ErrInvalidEvidence, err)
	}
	if err := point.EventType.Validate(); err != nil {
		return EvaluatedEvidence{}, fmt.Errorf("%w: %v", ErrInvalidEvidence, err)
	}
	if !containsEventType(policy.EligibleEvidenceEventTypes, point.EventType) ||
		containsEventType(policy.ForbiddenEvidenceEventTypes, point.EventType) {
		return EvaluatedEvidence{}, fmt.Errorf("%w: event type %q is not eligible", ErrInvalidEvidence, point.EventType)
	}
	if err := point.Polarity.Validate(); err != nil {
		return EvaluatedEvidence{}, fmt.Errorf("%w: %v", ErrInvalidEvidence, err)
	}
	if err := point.Grade.Validate(); err != nil {
		return EvaluatedEvidence{}, fmt.Errorf("%w: %v", ErrInvalidEvidence, err)
	}
	if err := point.Trust.Validate(); err != nil {
		return EvaluatedEvidence{}, fmt.Errorf("%w: %v", ErrInvalidEvidence, err)
	}
	if err := point.Derivation.Validate(); err != nil {
		return EvaluatedEvidence{}, fmt.Errorf("%w: %v", ErrInvalidEvidence, err)
	}
	if err := point.Reason.Validate(); err != nil {
		return EvaluatedEvidence{}, fmt.Errorf("%w: %v", ErrInvalidEvidence, err)
	}

	effectiveGrade := point.Grade
	if point.EventType == EventSelfTalk {
		effectiveGrade = policy.Evidence.SelfTalkForcedGrade
	}
	if effectiveGrade == GradeObserved && !policy.Evidence.ObservedEnabled {
		return EvaluatedEvidence{}, fmt.Errorf("%w: observed grade is reserved", ErrInvalidEvidence)
	}
	if err := validateEvidenceProvenance(point, effectiveGrade, policy); err != nil {
		return EvaluatedEvidence{}, err
	}

	var weight int64
	switch effectiveGrade {
	case GradeStated:
		weight = policy.Evidence.StatedWeight
	case GradeInferred:
		weight = policy.Evidence.InferredWeight
	default:
		return EvaluatedEvidence{}, fmt.Errorf("%w: unsupported effective grade %q", ErrInvalidEvidence, effectiveGrade)
	}
	var err error
	if point.Trust == TrustUntrusted {
		weight, err = multiplyDivideFloor(weight, policy.Evidence.UntrustedMultiplier, fixedPointScale)
		if err != nil {
			return EvaluatedEvidence{}, fmt.Errorf("%w: untrusted multiplier: %v", ErrInvalidEvidence, err)
		}
	}
	if point.Derivation == DerivationInherited {
		weight, err = multiplyDivideFloor(weight, policy.Inheritance.WeightMultiplier, fixedPointScale)
		if err != nil {
			return EvaluatedEvidence{}, fmt.Errorf("%w: inheritance multiplier: %v", ErrInvalidEvidence, err)
		}
	}
	canonicalWeight, err := canonical.NewWeight(weight)
	if err != nil {
		return EvaluatedEvidence{}, fmt.Errorf("%w: effective weight: %v", ErrInvalidEvidence, err)
	}
	return EvaluatedEvidence{
		EvidencePoint: point, EffectiveGrade: effectiveGrade, EffectiveWeight: canonicalWeight,
	}, nil
}

func validateEvidenceProvenance(point EvidencePoint, effectiveGrade EvidenceGrade, policy Policy) error {
	switch point.Derivation {
	case DerivationExtracted:
		if point.SourceEvidenceID != nil || point.InheritanceDepth != 0 {
			return fmt.Errorf("%w: extracted evidence cannot have inherited provenance", ErrInvalidEvidence)
		}
		if effectiveGrade == GradeStated && point.Reason != EvidenceReasonSourceStated {
			return fmt.Errorf("%w: stated extracted evidence requires %q", ErrInvalidEvidence, EvidenceReasonSourceStated)
		}
		if effectiveGrade == GradeInferred && point.Reason != EvidenceReasonSourceInferred {
			return fmt.Errorf("%w: inferred extracted evidence requires %q", ErrInvalidEvidence, EvidenceReasonSourceInferred)
		}
	case DerivationInherited:
		if point.SourceEvidenceID == nil {
			return fmt.Errorf("%w: inherited evidence requires source evidence", ErrInvalidEvidence)
		}
		if err := point.SourceEvidenceID.Validate(); err != nil {
			return fmt.Errorf("%w: source evidence: %v", ErrInvalidEvidence, err)
		}
		if point.InheritanceDepth < 1 || point.InheritanceDepth > policy.Inheritance.MaxDepth {
			return fmt.Errorf("%w: inheritance depth %d exceeds policy", ErrInvalidEvidence, point.InheritanceDepth)
		}
		if point.Reason != EvidenceReasonInheritedAbstraction && point.Reason != EvidenceReasonInheritedSplit {
			return fmt.Errorf("%w: inherited evidence requires an inherited reason", ErrInvalidEvidence)
		}
	}
	return nil
}

// AggregateEvidence combines already policy-evaluated rows. With v0's
// dedupe_underlying_event rule, repeated rows for the same source event and
// polarity contribute only their greatest effective weight.
func AggregateEvidence(policy Policy, points []EvaluatedEvidence) (EvidenceAggregate, error) {
	if err := policy.RequireEnabled(); err != nil {
		return EvidenceAggregate{}, err
	}
	type evidenceKey struct {
		event    canonical.ID
		polarity EvidencePolarity
	}
	deduplicated := make(map[evidenceKey]EvaluatedEvidence, len(points))
	for _, point := range points {
		if err := point.SourceEventID.Validate(); err != nil {
			return EvidenceAggregate{}, fmt.Errorf("%w: source event: %v", ErrInvalidEvidence, err)
		}
		if err := point.EffectiveWeight.Validate(); err != nil {
			return EvidenceAggregate{}, fmt.Errorf("%w: weight: %v", ErrInvalidEvidence, err)
		}
		if err := point.Polarity.Validate(); err != nil {
			return EvidenceAggregate{}, fmt.Errorf("%w: %v", ErrInvalidEvidence, err)
		}
		key := evidenceKey{event: point.SourceEventID, polarity: point.Polarity}
		previous, found := deduplicated[key]
		if !found || point.EffectiveWeight > previous.EffectiveWeight {
			deduplicated[key] = point
		}
	}

	var support, contradict int64
	distinctSupport := make(map[canonical.ID]struct{})
	result := EvidenceAggregate{}
	for _, point := range deduplicated {
		weight := point.EffectiveWeight.Millionths()
		var err error
		switch point.Polarity {
		case PolaritySupport:
			support, err = checkedAdd(support, weight)
			distinctSupport[point.SourceEventID] = struct{}{}
			if point.Trust == TrustTrusted {
				result.HasTrustedSupport = true
			}
			if point.EventType != EventSelfTalk {
				result.HasExternalSupport = true
			}
			if point.EventType == EventUserMessage && point.ActorIsSubject {
				result.HasSubjectUserSupport = true
			}
			if point.EventType == EventUserMessage && point.ActorIsPerspective {
				result.HasPerspectiveUserSupport = true
			}
		case PolarityContradict:
			contradict, err = checkedAdd(contradict, weight)
		default:
			return EvidenceAggregate{}, fmt.Errorf("%w: unknown polarity %q", ErrInvalidEvidence, point.Polarity)
		}
		if err != nil {
			return EvidenceAggregate{}, fmt.Errorf("%w: aggregate weight: %v", ErrInvalidEvidence, err)
		}
	}
	total, err := checkedAdd(support, contradict)
	if err != nil {
		return EvidenceAggregate{}, fmt.Errorf("%w: total weight: %v", ErrInvalidEvidence, err)
	}
	confidence, err := ratioOf(support, total)
	if err != nil {
		return EvidenceAggregate{}, fmt.Errorf("%w: confidence: %v", ErrInvalidEvidence, err)
	}
	supportWeight, _ := canonical.NewWeight(support)
	contradictWeight, _ := canonical.NewWeight(contradict)
	distinct, err := canonical.NewCount(int64(len(distinctSupport)))
	if err != nil {
		return EvidenceAggregate{}, fmt.Errorf("%w: distinct events: %v", ErrInvalidEvidence, err)
	}
	result.SupportWeight = supportWeight
	result.ContradictWeight = contradictWeight
	result.Confidence = confidence
	result.DistinctSupportEvents = distinct
	return result, nil
}

func EvaluateAndAggregateEvidence(policy Policy, points []EvidencePoint) (EvidenceAggregate, error) {
	evaluated := make([]EvaluatedEvidence, 0, len(points))
	for _, point := range points {
		value, err := EvaluateEvidence(policy, point)
		if err != nil {
			return EvidenceAggregate{}, err
		}
		evaluated = append(evaluated, value)
	}
	return AggregateEvidence(policy, evaluated)
}

func InitialScopeForEvent(policy Policy, eventType EventType) (ViewScope, error) {
	if err := policy.RequireEnabled(); err != nil {
		return "", err
	}
	switch eventType {
	case EventUserMessage:
		return policy.InitialScope.UserMessage, nil
	case EventSelfTalk:
		return policy.InitialScope.SelfTalk, nil
	default:
		return "", fmt.Errorf("%w: no initial scope for %q", ErrInvalidEvidence, eventType)
	}
}

func NarrowestSourceScope(policy Policy, scopes []ViewScope) (ViewScope, error) {
	if err := policy.RequireEnabled(); err != nil {
		return "", err
	}
	if policy.InitialScope.Derived != DerivedScopeNarrowestSource || len(scopes) == 0 {
		return "", fmt.Errorf("%w: derived scope cannot be resolved", ErrInvalidEvidence)
	}
	result := ScopeResidentUI
	for _, scope := range scopes {
		if err := scope.Validate(); err != nil {
			return "", fmt.Errorf("%w: %v", ErrInvalidEvidence, err)
		}
		if scope == ScopeAdminOnly {
			result = ScopeAdminOnly
		}
	}
	return result, nil
}

func containsEventType(values []EventType, target EventType) bool {
	for _, value := range values {
		if value == target {
			return true
		}
	}
	return false
}
