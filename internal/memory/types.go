// Package memory contains the pure, versioned policy and evaluation rules for
// provenanced memory. It deliberately has no database, provider, clock, or
// application dependency.
package memory

import (
	"errors"
	"fmt"

	"mahoroba.local/mahoroba/internal/canonical"
)

const fixedPointScale int64 = canonical.FixedPointScale

var (
	ErrInvalidPolicy     = errors.New("memory: invalid policy")
	ErrUnsupportedPolicy = errors.New("memory: unsupported policy")
	ErrFeatureDisabled   = errors.New("memory: feature is disabled by policy")
	ErrInvalidEvidence   = errors.New("memory: invalid evidence")
	ErrInvalidMaturation = errors.New("memory: invalid maturation input")
	ErrInvalidSalience   = errors.New("memory: invalid salience input")
	ErrInvalidTemporal   = errors.New("memory: invalid temporal input")
	ErrInvalidRecall     = errors.New("memory: invalid recall input")
	ErrInvalidPersona    = errors.New("memory: invalid persona input")
	ErrInvalidExtraction = errors.New("memory: invalid extraction output")
	ErrInvalidAlignment  = errors.New("memory: invalid alignment output")
)

type PolicyVersion string

const (
	PolicyVersionV1 PolicyVersion = "memory-policy-v1"
	PolicyVersionV2 PolicyVersion = "memory-policy-v2"
	PolicyVersionV3 PolicyVersion = "memory-policy-v3"
	PolicyVersionV4 PolicyVersion = "memory-policy-v4"
)

func (value PolicyVersion) Validate() error {
	switch value {
	case PolicyVersionV1, PolicyVersionV2, PolicyVersionV3, PolicyVersionV4:
		return nil
	default:
		return enumError("policy version", string(value))
	}
}

type EventType string

const (
	EventUserMessage        EventType = "user_message"
	EventSelfTalk           EventType = "self_talk"
	EventResidentMessage    EventType = "resident_message"
	EventOutboundInitiative EventType = "outbound_initiative"
)

func (value EventType) Validate() error {
	switch value {
	case EventUserMessage, EventSelfTalk, EventResidentMessage, EventOutboundInitiative:
		return nil
	default:
		return enumError("event type", string(value))
	}
}

type EvidenceGrade string

const (
	GradeStated   EvidenceGrade = "stated"
	GradeObserved EvidenceGrade = "observed"
	GradeInferred EvidenceGrade = "inferred"
)

func (value EvidenceGrade) Validate() error {
	switch value {
	case GradeStated, GradeObserved, GradeInferred:
		return nil
	default:
		return enumError("evidence grade", string(value))
	}
}

type EvidencePolarity string

const (
	PolaritySupport    EvidencePolarity = "support"
	PolarityContradict EvidencePolarity = "contradict"
)

func (value EvidencePolarity) Validate() error {
	switch value {
	case PolaritySupport, PolarityContradict:
		return nil
	default:
		return enumError("evidence polarity", string(value))
	}
}

type TrustLevel string

const (
	TrustTrusted   TrustLevel = "trusted"
	TrustUntrusted TrustLevel = "untrusted"
)

func (value TrustLevel) Validate() error {
	switch value {
	case TrustTrusted, TrustUntrusted:
		return nil
	default:
		return enumError("trust level", string(value))
	}
}

type EvidenceDerivation string

const (
	DerivationExtracted EvidenceDerivation = "extracted"
	DerivationInherited EvidenceDerivation = "inherited"
)

func (value EvidenceDerivation) Validate() error {
	switch value {
	case DerivationExtracted, DerivationInherited:
		return nil
	default:
		return enumError("evidence derivation", string(value))
	}
}

type ClaimKind string

const (
	// ClaimKindUnclassified represents the Canonical NULL claim kind.
	ClaimKindUnclassified ClaimKind = ""
	ClaimKindDirect       ClaimKind = "direct"
	ClaimKindOther        ClaimKind = "other"
	ClaimKindMeta         ClaimKind = "meta"
)

func (value ClaimKind) Validate() error {
	switch value {
	case ClaimKindUnclassified, ClaimKindDirect, ClaimKindOther, ClaimKindMeta:
		return nil
	default:
		return enumError("claim kind", string(value))
	}
}

type ClaimStage string

const (
	StageFloating ClaimStage = "floating"
	StageSediment ClaimStage = "sediment"
	StageSettled  ClaimStage = "settled"
)

func (value ClaimStage) Validate() error {
	switch value {
	case StageFloating, StageSediment, StageSettled:
		return nil
	default:
		return enumError("claim stage", string(value))
	}
}

type ClaimStatus string

const (
	StatusActive      ClaimStatus = "active"
	StatusInvalidated ClaimStatus = "invalidated"
	StatusSuperseded  ClaimStatus = "superseded"
	StatusQuarantined ClaimStatus = "quarantined"
)

func (value ClaimStatus) Validate() error {
	switch value {
	case StatusActive, StatusInvalidated, StatusSuperseded, StatusQuarantined:
		return nil
	default:
		return enumError("claim status", string(value))
	}
}

type TemporalKind string

const (
	TemporalStable   TemporalKind = "stable"
	TemporalVolatile TemporalKind = "volatile"
	TemporalEpisodic TemporalKind = "episodic"
)

func (value TemporalKind) Validate() error {
	switch value {
	case TemporalStable, TemporalVolatile, TemporalEpisodic:
		return nil
	default:
		return enumError("temporal kind", string(value))
	}
}

type TemporalRelation string

const (
	RelationFuture       TemporalRelation = "future"
	RelationCurrent      TemporalRelation = "current"
	RelationPast         TemporalRelation = "past"
	RelationStaleUnknown TemporalRelation = "stale_unknown"
)

func (value TemporalRelation) Validate() error {
	switch value {
	case RelationFuture, RelationCurrent, RelationPast, RelationStaleUnknown:
		return nil
	default:
		return enumError("temporal relation", string(value))
	}
}

type ViewScope string

const (
	ScopeResidentUI ViewScope = "resident_ui"
	ScopeAdminOnly  ViewScope = "admin_only"
)

func (value ViewScope) Validate() error {
	switch value {
	case ScopeResidentUI, ScopeAdminOnly:
		return nil
	default:
		return enumError("view scope", string(value))
	}
}

type UsageType string

const (
	UsageCandidate            UsageType = "candidate"
	UsageSelected             UsageType = "selected"
	UsagePromptIncluded       UsageType = "prompt_included"
	UsageExplicitlyReferenced UsageType = "explicitly_referenced"
)

func (value UsageType) Validate() error {
	switch value {
	case UsageCandidate, UsageSelected, UsagePromptIncluded, UsageExplicitlyReferenced:
		return nil
	default:
		return enumError("usage type", string(value))
	}
}

type RelationType string

const (
	RelationAbstracts   RelationType = "abstracts"
	RelationSplitFrom   RelationType = "split_from"
	RelationSupersedes  RelationType = "supersedes"
	RelationContradicts RelationType = "contradicts"
)

func (value RelationType) Validate() error {
	switch value {
	case RelationAbstracts, RelationSplitFrom, RelationSupersedes, RelationContradicts:
		return nil
	default:
		return enumError("relation type", string(value))
	}
}

// RelationReason is the typed vocabulary for Canonical claim-relation
// provenance. Keep the intent reason distinct from the eventual
// explicit_supersession relation: the former only nominates a candidate and
// never changes claim status by itself.
type RelationReason string

const (
	RelationReasonReplacementIntent RelationReason = "replacement_intent"
)

func (value RelationReason) Validate() error {
	switch value {
	case RelationReasonReplacementIntent:
		return nil
	default:
		return enumError("relation reason", string(value))
	}
}

type EvidenceReason string

const (
	EvidenceReasonSourceStated         EvidenceReason = "source_stated"
	EvidenceReasonSourceInferred       EvidenceReason = "source_inferred"
	EvidenceReasonInheritedAbstraction EvidenceReason = "inherited_abstraction"
	EvidenceReasonInheritedSplit       EvidenceReason = "inherited_split"
)

func (value EvidenceReason) Validate() error {
	switch value {
	case EvidenceReasonSourceStated, EvidenceReasonSourceInferred,
		EvidenceReasonInheritedAbstraction, EvidenceReasonInheritedSplit:
		return nil
	default:
		return enumError("evidence reason", string(value))
	}
}

type StageReason string

const (
	StageReasonInitialExtraction   StageReason = "initial_extraction"
	StageReasonMaturationThreshold StageReason = "maturation_threshold"
	StageReasonExternalAlignment   StageReason = "external_alignment"
)

func (value StageReason) Validate() error {
	switch value {
	case StageReasonInitialExtraction, StageReasonMaturationThreshold, StageReasonExternalAlignment:
		return nil
	default:
		return enumError("stage reason", string(value))
	}
}

type AutomaticDecisionReason string

const (
	AutomaticReasonExplicitCorrection   AutomaticDecisionReason = "explicit_correction"
	AutomaticReasonExplicitSupersession AutomaticDecisionReason = "explicit_supersession"
	AutomaticReasonStructuralQuarantine AutomaticDecisionReason = "structural_quarantine"
)

func (value AutomaticDecisionReason) Validate() error {
	switch value {
	case AutomaticReasonExplicitCorrection, AutomaticReasonExplicitSupersession, AutomaticReasonStructuralQuarantine:
		return nil
	default:
		return enumError("automatic decision reason", string(value))
	}
}

type HumanDecisionReason string

const (
	HumanReasonInvalidation HumanDecisionReason = "human_invalidation"
	HumanReasonSupersession HumanDecisionReason = "human_supersession"
	HumanReasonQuarantine   HumanDecisionReason = "human_quarantine"
	HumanReasonReactivation HumanDecisionReason = "human_reactivation"
)

func (value HumanDecisionReason) Validate() error {
	switch value {
	case HumanReasonInvalidation, HumanReasonSupersession, HumanReasonQuarantine, HumanReasonReactivation:
		return nil
	default:
		return enumError("human decision reason", string(value))
	}
}

type RecallExclusionReason string

const (
	ExclusionProvenanceDuplicateV1 RecallExclusionReason = "source_event_already_in_context"
	ExclusionProvenanceDuplicateV2 RecallExclusionReason = "provenance_duplicate"
	ExclusionTokenBudget           RecallExclusionReason = "token_budget"
	ExclusionPolicyFilter          RecallExclusionReason = "policy_filter"

	// ExclusionProvenanceDuplicate remains the context-v1 compatibility alias.
	ExclusionProvenanceDuplicate = ExclusionProvenanceDuplicateV1
)

func (value RecallExclusionReason) Validate() error {
	return value.ValidateV1()
}

func (value RecallExclusionReason) ValidateV1() error {
	switch value {
	case ExclusionProvenanceDuplicateV1, ExclusionTokenBudget, ExclusionPolicyFilter:
		return nil
	default:
		return enumError("recall exclusion reason", string(value))
	}
}

func (value RecallExclusionReason) ValidateV2() error {
	switch value {
	case ExclusionProvenanceDuplicateV2, ExclusionTokenBudget, ExclusionPolicyFilter:
		return nil
	default:
		return enumError("recall exclusion reason", string(value))
	}
}

func enumError(kind, value string) error {
	return fmt.Errorf("memory: unknown %s %q", kind, value)
}
