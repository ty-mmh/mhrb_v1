package memory

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"

	"mahoroba.local/mahoroba/internal/canonical"
)

const (
	DefinitionSchemaV0     = "mahoroba-memory-policy-v0"
	NormalizationVersionV1 = "memory-normalization-v1"
	RenderingVersionV1     = "memory-rendering-v1"
	RenderingVersionV2     = "memory-rendering-v2"
)

type DerivedScopeRule string

const DerivedScopeNarrowestSource DerivedScopeRule = "narrowest_source"

type EvidencePolicy struct {
	StatedWeight        int64
	InferredWeight      int64
	UntrustedMultiplier int64
	ObservedEnabled     bool
	SelfTalkForcedGrade EvidenceGrade
}

type InitialScopePolicy struct {
	UserMessage ViewScope
	SelfTalk    ViewScope
	Derived     DerivedScopeRule
}

type MaturationPolicy struct {
	SedimentDistinctEvents int64
	SedimentSupportWeight  int64
	SettledDistinctEvents  int64
	SettledSupportWeight   int64
	SettledConfidence      int64
	AlignmentConfidence    int64
	MetaMinimumStage       ClaimStage
}

type SaliencePolicy struct {
	CandidateContribution            int64
	SelectedContribution             int64
	PromptIncludedContribution       int64
	ExplicitlyReferencedContribution int64
	HalfLifeDays                     int64
	Cap                              int64
}

type TemporalPolicy struct {
	VolatileStaleDays    int64
	EpisodicCurrentHours int64
	FutureCurrentness    int64
	CurrentCurrentness   int64
	PastCurrentness      int64
	StaleCurrentness     int64
}

type RecallWeights struct {
	Context    int64
	Salience   int64
	Confidence int64
	State      int64
	Temporal   int64
}

type RecallPolicy struct {
	CandidateLimit      int64
	SelectedLimit       int64
	PromptIncludedLimit int64
	ByteBudget          int64
	Weights             RecallWeights
}

type InheritancePolicy struct {
	WeightMultiplier      int64
	MaxDepth              int64
	MaxPerTarget          int64
	MaxPerSourceClaim     int64
	DedupeUnderlyingEvent bool
}

type PersonaPolicy struct {
	MinimumClaims           int64
	MaximumClaims           int64
	MaximumChangedBytes     int64
	MaximumChangedRatio     int64
	MaximumChangedLines     int64
	MaximumTotalBytes       int64
	ActivationCooldownHours int64
}

// Policy is the parsed policy contract. V1 is the disabled M3/M4 compatibility
// envelope. V2 is the immutable M5 memory-policy-v0 definition. V3 keeps the
// same numeric rules while also making self-talk a mandatory extraction source.
type Policy struct {
	Version                     PolicyVersion
	DefinitionSchema            string
	MandatoryEventTypes         []EventType
	MemoryRecallEnabled         bool
	EligibleEvidenceEventTypes  []EventType
	ForbiddenEvidenceEventTypes []EventType
	Evidence                    EvidencePolicy
	InitialScope                InitialScopePolicy
	Maturation                  MaturationPolicy
	Salience                    SaliencePolicy
	Temporal                    TemporalPolicy
	Recall                      RecallPolicy
	Inheritance                 InheritancePolicy
	Persona                     PersonaPolicy
	NormalizationVersion        string
	RenderingVersion            string
}

type policyV1Wire struct {
	MandatoryEventTypes []EventType   `json:"mandatory_event_types"`
	MemoryRecallEnabled bool          `json:"memory_recall_enabled"`
	Version             PolicyVersion `json:"version"`
}

type policyV2Wire struct {
	Version                     PolicyVersion          `json:"version"`
	DefinitionSchema            string                 `json:"definition_schema"`
	MandatoryEventTypes         []EventType            `json:"mandatory_event_types"`
	EligibleEvidenceEventTypes  []EventType            `json:"eligible_evidence_event_types"`
	ForbiddenEvidenceEventTypes []EventType            `json:"forbidden_evidence_event_types"`
	Evidence                    evidencePolicyWire     `json:"evidence"`
	InitialScope                initialScopePolicyWire `json:"initial_scope"`
	Maturation                  maturationPolicyWire   `json:"maturation"`
	Salience                    saliencePolicyWire     `json:"salience"`
	Temporal                    temporalPolicyWire     `json:"temporal"`
	Recall                      recallPolicyWire       `json:"recall"`
	Inheritance                 inheritancePolicyWire  `json:"inheritance"`
	Persona                     personaPolicyWire      `json:"persona"`
	NormalizationVersion        string                 `json:"normalization_version"`
	RenderingVersion            string                 `json:"rendering_version"`
}

type evidencePolicyWire struct {
	GradeWeights struct {
		Stated   int64 `json:"stated"`
		Inferred int64 `json:"inferred"`
	} `json:"grade_weights"`
	UntrustedMultiplier int64         `json:"untrusted_multiplier"`
	ObservedEnabled     bool          `json:"observed_enabled"`
	SelfTalkForcedGrade EvidenceGrade `json:"self_talk_forced_grade"`
}

type initialScopePolicyWire struct {
	UserMessage ViewScope        `json:"user_message"`
	SelfTalk    ViewScope        `json:"self_talk"`
	Derived     DerivedScopeRule `json:"derived"`
}

type maturationPolicyWire struct {
	SedimentDistinctEvents int64      `json:"sediment_distinct_events"`
	SedimentSupportWeight  int64      `json:"sediment_support_weight"`
	SettledDistinctEvents  int64      `json:"settled_distinct_events"`
	SettledSupportWeight   int64      `json:"settled_support_weight"`
	SettledConfidence      int64      `json:"settled_confidence"`
	AlignmentConfidence    int64      `json:"alignment_confidence"`
	MetaMinimumStage       ClaimStage `json:"meta_min_stage"`
}

type saliencePolicyWire struct {
	Candidate            int64 `json:"candidate"`
	Selected             int64 `json:"selected"`
	PromptIncluded       int64 `json:"prompt_included"`
	ExplicitlyReferenced int64 `json:"explicitly_referenced"`
	HalfLifeDays         int64 `json:"half_life_days"`
	Cap                  int64 `json:"cap"`
}

type temporalPolicyWire struct {
	VolatileStaleDays    int64 `json:"volatile_stale_days"`
	EpisodicCurrentHours int64 `json:"episodic_current_hours"`
	Currentness          struct {
		Future       int64 `json:"future"`
		Current      int64 `json:"current"`
		Past         int64 `json:"past"`
		StaleUnknown int64 `json:"stale_unknown"`
	} `json:"currentness"`
}

type recallPolicyWire struct {
	CandidateLimit      int64 `json:"candidate_limit"`
	SelectedLimit       int64 `json:"selected_limit"`
	PromptIncludedLimit int64 `json:"prompt_included_limit"`
	ByteBudget          int64 `json:"byte_budget"`
	Weights             struct {
		Context    int64 `json:"context"`
		Salience   int64 `json:"salience"`
		Confidence int64 `json:"confidence"`
		State      int64 `json:"state"`
		Temporal   int64 `json:"temporal"`
	} `json:"weights"`
}

type inheritancePolicyWire struct {
	WeightMultiplier      int64 `json:"weight_multiplier"`
	MaxDepth              int64 `json:"max_depth"`
	MaxPerTarget          int64 `json:"max_per_target"`
	MaxPerSourceClaim     int64 `json:"max_per_source_claim"`
	DedupeUnderlyingEvent bool  `json:"dedupe_underlying_event"`
}

type personaPolicyWire struct {
	MinimumClaims           int64 `json:"min_claims"`
	MaximumClaims           int64 `json:"max_claims"`
	MaximumChangedBytes     int64 `json:"max_changed_bytes"`
	MaximumChangedRatio     int64 `json:"max_changed_ratio"`
	MaximumChangedLines     int64 `json:"max_changed_lines"`
	MaximumTotalBytes       int64 `json:"max_total_bytes"`
	ActivationCooldownHours int64 `json:"activation_cooldown_hours"`
}

func DefaultPolicyV1() Policy {
	return Policy{
		Version:             PolicyVersionV1,
		MandatoryEventTypes: []EventType{},
		MemoryRecallEnabled: false,
	}
}

func DefaultPolicyV2() Policy {
	return Policy{
		Version:                     PolicyVersionV2,
		DefinitionSchema:            DefinitionSchemaV0,
		MandatoryEventTypes:         []EventType{EventUserMessage},
		MemoryRecallEnabled:         true,
		EligibleEvidenceEventTypes:  []EventType{EventUserMessage, EventSelfTalk},
		ForbiddenEvidenceEventTypes: []EventType{EventResidentMessage, EventOutboundInitiative},
		Evidence: EvidencePolicy{
			StatedWeight: 1_000_000, InferredWeight: 600_000,
			UntrustedMultiplier: 250_000, ObservedEnabled: false,
			SelfTalkForcedGrade: GradeInferred,
		},
		InitialScope: InitialScopePolicy{
			UserMessage: ScopeResidentUI, SelfTalk: ScopeAdminOnly, Derived: DerivedScopeNarrowestSource,
		},
		Maturation: MaturationPolicy{
			SedimentDistinctEvents: 2, SedimentSupportWeight: 1_200_000,
			SettledDistinctEvents: 3, SettledSupportWeight: 2_000_000,
			SettledConfidence: 750_000, AlignmentConfidence: 800_000,
			MetaMinimumStage: StageSediment,
		},
		Salience: SaliencePolicy{
			CandidateContribution: 0, SelectedContribution: 50_000,
			PromptIncludedContribution: 250_000, ExplicitlyReferencedContribution: 500_000,
			HalfLifeDays: 30, Cap: 1_000_000,
		},
		Temporal: TemporalPolicy{
			VolatileStaleDays: 30, EpisodicCurrentHours: 24,
			FutureCurrentness: 250_000, CurrentCurrentness: 1_000_000,
			PastCurrentness: 600_000, StaleCurrentness: 400_000,
		},
		Recall: RecallPolicy{
			CandidateLimit: 64, SelectedLimit: 8, PromptIncludedLimit: 4, ByteBudget: 8192,
			Weights: RecallWeights{
				Context: 350_000, Salience: 200_000, Confidence: 200_000,
				State: 150_000, Temporal: 100_000,
			},
		},
		Inheritance: InheritancePolicy{
			WeightMultiplier: 500_000, MaxDepth: 1, MaxPerTarget: 8,
			MaxPerSourceClaim: 2, DedupeUnderlyingEvent: true,
		},
		Persona: PersonaPolicy{
			MinimumClaims: 2, MaximumClaims: 8, MaximumChangedBytes: 256,
			MaximumChangedRatio: 100_000, MaximumChangedLines: 3,
			MaximumTotalBytes: 8192, ActivationCooldownHours: 24,
		},
		NormalizationVersion: NormalizationVersionV1,
		RenderingVersion:     RenderingVersionV1,
	}
}

func DefaultPolicyV3() Policy {
	policy := DefaultPolicyV2()
	policy.Version = PolicyVersionV3
	policy.MandatoryEventTypes = []EventType{EventUserMessage, EventSelfTalk}
	return policy
}

// DefaultPolicyV4 preserves the complete V3 policy contract and changes only
// the structured memory renderer selected for dialogue Recall inputs.
func DefaultPolicyV4() Policy {
	policy := DefaultPolicyV3()
	policy.Version = PolicyVersionV4
	policy.RenderingVersion = RenderingVersionV2
	return policy
}

// DefaultPolicyV5 ranks by the current query, without feeding Recall's own
// selections back into its ranking. Historical usage contributions remain
// replayable, but their accumulated salience has no V5 ranking weight.
func DefaultPolicyV5() Policy {
	policy := DefaultPolicyV4()
	policy.Version = PolicyVersionV5
	policy.Salience.SelectedContribution = 0
	policy.Salience.PromptIncludedContribution = 0
	policy.Recall.Weights.Context = 550_000
	policy.Recall.Weights.Salience = 0
	return policy
}

// ParsePolicy accepts only an exact JCS representation of a supported policy.
// A changed fixed rule requires a new policy version; policy definitions are
// not runtime-tunable under an existing version.
func ParsePolicy(input []byte) (Policy, canonical.CanonicalJSON, error) {
	encoded, err := canonical.ParseCanonicalJSON(input)
	if err != nil {
		return Policy{}, canonical.CanonicalJSON{}, fmt.Errorf("%w: policy is not exact JCS: %v", ErrInvalidPolicy, err)
	}
	var header map[string]json.RawMessage
	if err := json.Unmarshal(encoded.Bytes(), &header); err != nil {
		return Policy{}, canonical.CanonicalJSON{}, fmt.Errorf("%w: decode header: %v", ErrInvalidPolicy, err)
	}
	versionRaw, present := header["version"]
	if !present {
		return Policy{}, canonical.CanonicalJSON{}, fmt.Errorf("%w: missing version", ErrInvalidPolicy)
	}
	var version PolicyVersion
	if err := json.Unmarshal(versionRaw, &version); err != nil {
		return Policy{}, canonical.CanonicalJSON{}, fmt.Errorf("%w: invalid version: %v", ErrInvalidPolicy, err)
	}

	switch version {
	case PolicyVersionV1:
		var wire policyV1Wire
		if err := decodeClosed(encoded.Bytes(), &wire); err != nil {
			return Policy{}, canonical.CanonicalJSON{}, fmt.Errorf("%w: %v", ErrInvalidPolicy, err)
		}
		policy := DefaultPolicyV1()
		if err := requireExactPolicy(encoded, policy); err != nil {
			return Policy{}, canonical.CanonicalJSON{}, err
		}
		return policy, encoded, nil
	case PolicyVersionV2, PolicyVersionV3, PolicyVersionV4, PolicyVersionV5:
		var wire policyV2Wire
		if err := decodeClosed(encoded.Bytes(), &wire); err != nil {
			return Policy{}, canonical.CanonicalJSON{}, fmt.Errorf("%w: %v", ErrInvalidPolicy, err)
		}
		policy := policyFromV2Wire(wire)
		expected := DefaultPolicyV2()
		if version == PolicyVersionV3 {
			expected = DefaultPolicyV3()
		} else if version == PolicyVersionV4 {
			expected = DefaultPolicyV4()
		} else if version == PolicyVersionV5 {
			expected = DefaultPolicyV5()
		}
		if err := requireExactPolicy(encoded, expected); err != nil {
			return Policy{}, canonical.CanonicalJSON{}, err
		}
		return policy, encoded, nil
	default:
		return Policy{}, canonical.CanonicalJSON{}, fmt.Errorf("%w: %q", ErrUnsupportedPolicy, version)
	}
}

// CanonicalJSON returns the exact wire definition for a supported immutable
// policy. It rejects a caller-mutated copy rather than blessing a silent v2
// policy change.
func (policy Policy) CanonicalJSON() (canonical.CanonicalJSON, error) {
	wire, err := policyWire(policy)
	if err != nil {
		return canonical.CanonicalJSON{}, err
	}
	raw, err := json.Marshal(wire)
	if err != nil {
		return canonical.CanonicalJSON{}, fmt.Errorf("%w: marshal: %v", ErrInvalidPolicy, err)
	}
	encoded, err := canonical.CanonicalizeRFC8785(raw)
	if err != nil {
		return canonical.CanonicalJSON{}, fmt.Errorf("%w: canonicalize: %v", ErrInvalidPolicy, err)
	}
	expected := DefaultPolicyV1()
	if policy.Version == PolicyVersionV2 {
		expected = DefaultPolicyV2()
	} else if policy.Version == PolicyVersionV3 {
		expected = DefaultPolicyV3()
	} else if policy.Version == PolicyVersionV4 {
		expected = DefaultPolicyV4()
	} else if policy.Version == PolicyVersionV5 {
		expected = DefaultPolicyV5()
	}
	expectedWire, _ := policyWireUnchecked(expected)
	expectedRaw, _ := json.Marshal(expectedWire)
	expectedEncoded, _ := canonical.CanonicalizeRFC8785(expectedRaw)
	if !bytes.Equal(encoded.Bytes(), expectedEncoded.Bytes()) {
		return canonical.CanonicalJSON{}, fmt.Errorf("%w: %s definition differs from its fixed contract", ErrInvalidPolicy, policy.Version)
	}
	return encoded, nil
}

func (policy Policy) RequireV2() error {
	if policy.Version != PolicyVersionV2 {
		if policy.Version == PolicyVersionV1 {
			return ErrFeatureDisabled
		}
		return fmt.Errorf("%w: %q", ErrUnsupportedPolicy, policy.Version)
	}
	_, err := policy.CanonicalJSON()
	return err
}

// RequireEnabled accepts every explicitly activatable provenanced-memory
// policy while preserving RequireV2 as the exact M5 compatibility check.
func (policy Policy) RequireEnabled() error {
	switch policy.Version {
	case PolicyVersionV2, PolicyVersionV3, PolicyVersionV4, PolicyVersionV5:
		_, err := policy.CanonicalJSON()
		return err
	case PolicyVersionV1:
		return ErrFeatureDisabled
	default:
		return fmt.Errorf("%w: %q", ErrUnsupportedPolicy, policy.Version)
	}
}

func requireExactPolicy(got canonical.CanonicalJSON, expected Policy) error {
	want, err := expected.CanonicalJSON()
	if err != nil {
		return err
	}
	if !bytes.Equal(got.Bytes(), want.Bytes()) {
		return fmt.Errorf("%w: %s must match the fixed definition exactly", ErrInvalidPolicy, expected.Version)
	}
	return nil
}

func policyWire(policy Policy) (any, error) {
	if err := policy.Version.Validate(); err != nil {
		return nil, fmt.Errorf("%w: %v", ErrUnsupportedPolicy, err)
	}
	return policyWireUnchecked(policy)
}

func policyWireUnchecked(policy Policy) (any, error) {
	switch policy.Version {
	case PolicyVersionV1:
		mandatory := make([]EventType, len(policy.MandatoryEventTypes))
		copy(mandatory, policy.MandatoryEventTypes)
		return policyV1Wire{
			MandatoryEventTypes: mandatory,
			MemoryRecallEnabled: policy.MemoryRecallEnabled, Version: policy.Version,
		}, nil
	case PolicyVersionV2, PolicyVersionV3, PolicyVersionV4, PolicyVersionV5:
		wire := policyV2Wire{
			Version: policy.Version, DefinitionSchema: policy.DefinitionSchema,
			MandatoryEventTypes:         append([]EventType(nil), policy.MandatoryEventTypes...),
			EligibleEvidenceEventTypes:  append([]EventType(nil), policy.EligibleEvidenceEventTypes...),
			ForbiddenEvidenceEventTypes: append([]EventType(nil), policy.ForbiddenEvidenceEventTypes...),
			InitialScope: initialScopePolicyWire{
				UserMessage: policy.InitialScope.UserMessage, SelfTalk: policy.InitialScope.SelfTalk,
				Derived: policy.InitialScope.Derived,
			},
			Maturation: maturationPolicyWire{
				SedimentDistinctEvents: policy.Maturation.SedimentDistinctEvents,
				SedimentSupportWeight:  policy.Maturation.SedimentSupportWeight,
				SettledDistinctEvents:  policy.Maturation.SettledDistinctEvents,
				SettledSupportWeight:   policy.Maturation.SettledSupportWeight,
				SettledConfidence:      policy.Maturation.SettledConfidence,
				AlignmentConfidence:    policy.Maturation.AlignmentConfidence,
				MetaMinimumStage:       policy.Maturation.MetaMinimumStage,
			},
			Salience: saliencePolicyWire{
				Candidate:            policy.Salience.CandidateContribution,
				Selected:             policy.Salience.SelectedContribution,
				PromptIncluded:       policy.Salience.PromptIncludedContribution,
				ExplicitlyReferenced: policy.Salience.ExplicitlyReferencedContribution,
				HalfLifeDays:         policy.Salience.HalfLifeDays, Cap: policy.Salience.Cap,
			},
			Inheritance: inheritancePolicyWire{
				WeightMultiplier: policy.Inheritance.WeightMultiplier, MaxDepth: policy.Inheritance.MaxDepth,
				MaxPerTarget:          policy.Inheritance.MaxPerTarget,
				MaxPerSourceClaim:     policy.Inheritance.MaxPerSourceClaim,
				DedupeUnderlyingEvent: policy.Inheritance.DedupeUnderlyingEvent,
			},
			Persona: personaPolicyWire{
				MinimumClaims: policy.Persona.MinimumClaims, MaximumClaims: policy.Persona.MaximumClaims,
				MaximumChangedBytes:     policy.Persona.MaximumChangedBytes,
				MaximumChangedRatio:     policy.Persona.MaximumChangedRatio,
				MaximumChangedLines:     policy.Persona.MaximumChangedLines,
				MaximumTotalBytes:       policy.Persona.MaximumTotalBytes,
				ActivationCooldownHours: policy.Persona.ActivationCooldownHours,
			},
			NormalizationVersion: policy.NormalizationVersion, RenderingVersion: policy.RenderingVersion,
		}
		wire.Evidence.GradeWeights.Stated = policy.Evidence.StatedWeight
		wire.Evidence.GradeWeights.Inferred = policy.Evidence.InferredWeight
		wire.Evidence.UntrustedMultiplier = policy.Evidence.UntrustedMultiplier
		wire.Evidence.ObservedEnabled = policy.Evidence.ObservedEnabled
		wire.Evidence.SelfTalkForcedGrade = policy.Evidence.SelfTalkForcedGrade
		wire.Temporal.VolatileStaleDays = policy.Temporal.VolatileStaleDays
		wire.Temporal.EpisodicCurrentHours = policy.Temporal.EpisodicCurrentHours
		wire.Temporal.Currentness.Future = policy.Temporal.FutureCurrentness
		wire.Temporal.Currentness.Current = policy.Temporal.CurrentCurrentness
		wire.Temporal.Currentness.Past = policy.Temporal.PastCurrentness
		wire.Temporal.Currentness.StaleUnknown = policy.Temporal.StaleCurrentness
		wire.Recall.CandidateLimit = policy.Recall.CandidateLimit
		wire.Recall.SelectedLimit = policy.Recall.SelectedLimit
		wire.Recall.PromptIncludedLimit = policy.Recall.PromptIncludedLimit
		wire.Recall.ByteBudget = policy.Recall.ByteBudget
		wire.Recall.Weights.Context = policy.Recall.Weights.Context
		wire.Recall.Weights.Salience = policy.Recall.Weights.Salience
		wire.Recall.Weights.Confidence = policy.Recall.Weights.Confidence
		wire.Recall.Weights.State = policy.Recall.Weights.State
		wire.Recall.Weights.Temporal = policy.Recall.Weights.Temporal
		return wire, nil
	default:
		return nil, fmt.Errorf("%w: %q", ErrUnsupportedPolicy, policy.Version)
	}
}

func policyFromV2Wire(wire policyV2Wire) Policy {
	return Policy{
		Version: wire.Version, DefinitionSchema: wire.DefinitionSchema,
		MandatoryEventTypes:         append([]EventType(nil), wire.MandatoryEventTypes...),
		MemoryRecallEnabled:         true,
		EligibleEvidenceEventTypes:  append([]EventType(nil), wire.EligibleEvidenceEventTypes...),
		ForbiddenEvidenceEventTypes: append([]EventType(nil), wire.ForbiddenEvidenceEventTypes...),
		Evidence: EvidencePolicy{
			StatedWeight:        wire.Evidence.GradeWeights.Stated,
			InferredWeight:      wire.Evidence.GradeWeights.Inferred,
			UntrustedMultiplier: wire.Evidence.UntrustedMultiplier,
			ObservedEnabled:     wire.Evidence.ObservedEnabled,
			SelfTalkForcedGrade: wire.Evidence.SelfTalkForcedGrade,
		},
		InitialScope: InitialScopePolicy{
			UserMessage: wire.InitialScope.UserMessage, SelfTalk: wire.InitialScope.SelfTalk, Derived: wire.InitialScope.Derived,
		},
		Maturation: MaturationPolicy{
			SedimentDistinctEvents: wire.Maturation.SedimentDistinctEvents,
			SedimentSupportWeight:  wire.Maturation.SedimentSupportWeight,
			SettledDistinctEvents:  wire.Maturation.SettledDistinctEvents,
			SettledSupportWeight:   wire.Maturation.SettledSupportWeight,
			SettledConfidence:      wire.Maturation.SettledConfidence,
			AlignmentConfidence:    wire.Maturation.AlignmentConfidence,
			MetaMinimumStage:       wire.Maturation.MetaMinimumStage,
		},
		Salience: SaliencePolicy{
			CandidateContribution:            wire.Salience.Candidate,
			SelectedContribution:             wire.Salience.Selected,
			PromptIncludedContribution:       wire.Salience.PromptIncluded,
			ExplicitlyReferencedContribution: wire.Salience.ExplicitlyReferenced,
			HalfLifeDays:                     wire.Salience.HalfLifeDays, Cap: wire.Salience.Cap,
		},
		Temporal: TemporalPolicy{
			VolatileStaleDays:    wire.Temporal.VolatileStaleDays,
			EpisodicCurrentHours: wire.Temporal.EpisodicCurrentHours,
			FutureCurrentness:    wire.Temporal.Currentness.Future,
			CurrentCurrentness:   wire.Temporal.Currentness.Current,
			PastCurrentness:      wire.Temporal.Currentness.Past,
			StaleCurrentness:     wire.Temporal.Currentness.StaleUnknown,
		},
		Recall: RecallPolicy{
			CandidateLimit: wire.Recall.CandidateLimit, SelectedLimit: wire.Recall.SelectedLimit,
			PromptIncludedLimit: wire.Recall.PromptIncludedLimit, ByteBudget: wire.Recall.ByteBudget,
			Weights: RecallWeights{
				Context: wire.Recall.Weights.Context, Salience: wire.Recall.Weights.Salience,
				Confidence: wire.Recall.Weights.Confidence, State: wire.Recall.Weights.State,
				Temporal: wire.Recall.Weights.Temporal,
			},
		},
		Inheritance: InheritancePolicy{
			WeightMultiplier: wire.Inheritance.WeightMultiplier, MaxDepth: wire.Inheritance.MaxDepth,
			MaxPerTarget:          wire.Inheritance.MaxPerTarget,
			MaxPerSourceClaim:     wire.Inheritance.MaxPerSourceClaim,
			DedupeUnderlyingEvent: wire.Inheritance.DedupeUnderlyingEvent,
		},
		Persona: PersonaPolicy{
			MinimumClaims: wire.Persona.MinimumClaims, MaximumClaims: wire.Persona.MaximumClaims,
			MaximumChangedBytes:     wire.Persona.MaximumChangedBytes,
			MaximumChangedRatio:     wire.Persona.MaximumChangedRatio,
			MaximumChangedLines:     wire.Persona.MaximumChangedLines,
			MaximumTotalBytes:       wire.Persona.MaximumTotalBytes,
			ActivationCooldownHours: wire.Persona.ActivationCooldownHours,
		},
		NormalizationVersion: wire.NormalizationVersion, RenderingVersion: wire.RenderingVersion,
	}
}

func decodeClosed(input []byte, destination any) error {
	decoder := json.NewDecoder(bytes.NewReader(input))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(destination); err != nil {
		return err
	}
	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		if err == nil {
			return errors.New("additional JSON value")
		}
		return err
	}
	return nil
}
