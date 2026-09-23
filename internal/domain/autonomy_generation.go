package domain

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"time"

	"mahoroba.local/mahoroba/internal/autonomy"
	"mahoroba.local/mahoroba/internal/canonical"
	"mahoroba.local/mahoroba/internal/generation"
	"mahoroba.local/mahoroba/internal/projection"
)

// AutonomousClaimUsage binds every claim rendered into an autonomous prompt
// to its Canonical prompt_included provenance row.
type AutonomousClaimUsage struct {
	ID      canonical.ID
	ClaimID canonical.ID
	Ordinal canonical.Ordinal
}

const AutonomousRuntimeProjectionSchema = "mahoroba-autonomous-runtime-v1"

var ErrAutonomousContextIneligible = errors.New("domain: autonomous context is ineligible")

// AutonomousProjectionEvidence freezes the exact Projection snapshot used to
// authorize an initiative. It is persisted in the runtime_projection input so
// retries and landing never silently switch to a newer derived view.
type AutonomousProjectionEvidence struct {
	CapturedHead canonical.CommitSeq  `json:"captured_head"`
	ClaimStates  projection.Watermark `json:"claim_states"`
	ViewScope    projection.Watermark `json:"claim_view_scope_current"`
}

func (value AutonomousProjectionEvidence) Validate(residentID, policyRevisionID canonical.ID) error {
	if err := value.CapturedHead.Validate(); err != nil {
		return err
	}
	if err := value.ClaimStates.Validate(); err != nil {
		return err
	}
	if err := value.ViewScope.Validate(); err != nil {
		return err
	}
	if value.ClaimStates.ProjectionName != projection.ClaimStatesName ||
		value.ClaimStates.ProjectionVersion != "claim-states-v1" ||
		value.ViewScope.ProjectionName != projection.ClaimViewScopeCurrentName ||
		value.ViewScope.ProjectionVersion != "claim-view-scope-current-v1" ||
		value.ClaimStates.ResidentID != residentID || value.ViewScope.ResidentID != residentID ||
		value.ClaimStates.SourceCommitSeq != value.CapturedHead || value.ViewScope.SourceCommitSeq != value.CapturedHead {
		return errors.New("domain: autonomous Projection evidence does not describe one captured resident head")
	}
	equal, err := projection.DependencySetEqual(value.ClaimStates.Dependencies, []projection.Dependency{{
		Kind: projection.MemoryPolicyDependency, VersionID: policyRevisionID,
	}})
	if err != nil || !equal {
		return errors.New("domain: autonomous claim-state evidence lacks the exact memory-policy dependency")
	}
	equal, err = projection.DependencySetEqual(value.ViewScope.Dependencies, nil)
	if err != nil || !equal {
		return errors.New("domain: autonomous view-scope evidence unexpectedly has dependencies")
	}
	return nil
}

type autonomousTriggerJSON struct {
	Kind            autonomy.TriggerKind `json:"kind"`
	SourceID        canonical.ID         `json:"source_id"`
	RelatedClaimIDs []canonical.ID       `json:"related_claim_ids"`
	Boundary        *canonical.Instant   `json:"boundary,omitempty"`
	Ordinal         *canonical.Ordinal   `json:"ordinal,omitempty"`
}

type autonomousRuntimeProjectionJSON struct {
	Schema                 string                        `json:"schema"`
	Trigger                autonomousTriggerJSON         `json:"trigger"`
	Policy                 autonomousPolicyJSON          `json:"policy"`
	ProjectionMaxStaleness string                        `json:"projection_max_staleness"`
	Projection             *AutonomousProjectionEvidence `json:"projection,omitempty"`
}

type autonomousPolicyJSON struct {
	Version      autonomy.PolicyVersion `json:"version"`
	Timezone     string                 `json:"timezone"`
	ScanInterval string                 `json:"scan_interval"`
	SelfTalk     struct {
		Enabled          bool            `json:"enabled"`
		Interval         string          `json:"interval"`
		ConsecutiveLimit canonical.Count `json:"consecutive_limit"`
		HourlyLimit      canonical.Count `json:"hourly_limit"`
		DailyLimit       canonical.Count `json:"daily_limit"`
		QuietHoursStart  string          `json:"quiet_hours_start"`
		QuietHoursEnd    string          `json:"quiet_hours_end"`
	} `json:"self_talk"`
	Initiative struct {
		Enabled               bool                   `json:"enabled"`
		MinimumInterval       string                 `json:"minimum_interval"`
		HourlyLimit           canonical.Count        `json:"hourly_limit"`
		DailyLimit            canonical.Count        `json:"daily_limit"`
		RecentUserSuppression string                 `json:"recent_user_suppression"`
		QuietHoursStart       string                 `json:"quiet_hours_start"`
		QuietHoursEnd         string                 `json:"quiet_hours_end"`
		Triggers              []autonomy.TriggerKind `json:"triggers"`
	} `json:"initiative"`
	Retention struct {
		Mode         autonomy.RetentionMode        `json:"mode"`
		Duration     string                        `json:"duration"`
		ScanInterval string                        `json:"scan_interval"`
		Eligibility  autonomy.RetentionEligibility `json:"eligibility"`
	} `json:"retention"`
}

func freezeAutonomyPolicy(policy autonomy.Policy) (autonomousPolicyJSON, error) {
	if err := policy.Validate(); err != nil {
		return autonomousPolicyJSON{}, err
	}
	var wire autonomousPolicyJSON
	wire.Version, wire.Timezone, wire.ScanInterval = policy.Version, policy.Timezone, policy.ScanInterval.String()
	wire.SelfTalk.Enabled, wire.SelfTalk.Interval = policy.SelfTalk.Enabled, policy.SelfTalk.Interval.String()
	wire.SelfTalk.ConsecutiveLimit = canonical.Count(policy.SelfTalk.ConsecutiveLimit)
	wire.SelfTalk.HourlyLimit, wire.SelfTalk.DailyLimit = canonical.Count(policy.SelfTalk.HourlyLimit), canonical.Count(policy.SelfTalk.DailyLimit)
	wire.SelfTalk.QuietHoursStart, wire.SelfTalk.QuietHoursEnd = policy.SelfTalk.QuietHours.Start.String(), policy.SelfTalk.QuietHours.End.String()
	wire.Initiative.Enabled, wire.Initiative.MinimumInterval = policy.Initiative.Enabled, policy.Initiative.MinimumInterval.String()
	wire.Initiative.HourlyLimit, wire.Initiative.DailyLimit = canonical.Count(policy.Initiative.HourlyLimit), canonical.Count(policy.Initiative.DailyLimit)
	wire.Initiative.RecentUserSuppression = policy.Initiative.RecentUserSuppression.String()
	wire.Initiative.QuietHoursStart, wire.Initiative.QuietHoursEnd = policy.Initiative.QuietHours.Start.String(), policy.Initiative.QuietHours.End.String()
	wire.Initiative.Triggers = append([]autonomy.TriggerKind(nil), policy.Initiative.Triggers...)
	if wire.Initiative.Triggers == nil {
		wire.Initiative.Triggers = []autonomy.TriggerKind{}
	}
	wire.Retention.Mode, wire.Retention.Duration = policy.Retention.Mode, policy.Retention.Duration.String()
	wire.Retention.ScanInterval, wire.Retention.Eligibility = policy.Retention.ScanInterval.String(), policy.Retention.Eligibility
	return wire, nil
}

func thawAutonomyPolicy(wire autonomousPolicyJSON) (autonomy.Policy, error) {
	parseDuration := func(raw string) (time.Duration, error) {
		value, err := time.ParseDuration(raw)
		if err != nil || value <= 0 || value.String() != raw {
			return 0, errors.New("domain: invalid frozen autonomy duration")
		}
		return value, nil
	}
	var policy autonomy.Policy
	policy.Version, policy.Timezone = wire.Version, wire.Timezone
	var err error
	if policy.ScanInterval, err = parseDuration(wire.ScanInterval); err != nil {
		return autonomy.Policy{}, err
	}
	policy.SelfTalk.Enabled = wire.SelfTalk.Enabled
	if policy.SelfTalk.Interval, err = parseDuration(wire.SelfTalk.Interval); err != nil {
		return autonomy.Policy{}, err
	}
	policy.SelfTalk.ConsecutiveLimit, policy.SelfTalk.HourlyLimit, policy.SelfTalk.DailyLimit =
		wire.SelfTalk.ConsecutiveLimit.Int64(), wire.SelfTalk.HourlyLimit.Int64(), wire.SelfTalk.DailyLimit.Int64()
	if policy.SelfTalk.QuietHours.Start, err = autonomy.ParseLocalTime(wire.SelfTalk.QuietHoursStart); err != nil {
		return autonomy.Policy{}, err
	}
	if policy.SelfTalk.QuietHours.End, err = autonomy.ParseLocalTime(wire.SelfTalk.QuietHoursEnd); err != nil {
		return autonomy.Policy{}, err
	}
	policy.Initiative.Enabled = wire.Initiative.Enabled
	if policy.Initiative.MinimumInterval, err = parseDuration(wire.Initiative.MinimumInterval); err != nil {
		return autonomy.Policy{}, err
	}
	policy.Initiative.HourlyLimit, policy.Initiative.DailyLimit = wire.Initiative.HourlyLimit.Int64(), wire.Initiative.DailyLimit.Int64()
	if policy.Initiative.RecentUserSuppression, err = parseDuration(wire.Initiative.RecentUserSuppression); err != nil {
		return autonomy.Policy{}, err
	}
	if policy.Initiative.QuietHours.Start, err = autonomy.ParseLocalTime(wire.Initiative.QuietHoursStart); err != nil {
		return autonomy.Policy{}, err
	}
	if policy.Initiative.QuietHours.End, err = autonomy.ParseLocalTime(wire.Initiative.QuietHoursEnd); err != nil {
		return autonomy.Policy{}, err
	}
	policy.Initiative.Triggers = append([]autonomy.TriggerKind(nil), wire.Initiative.Triggers...)
	policy.Retention.Mode, policy.Retention.Eligibility = wire.Retention.Mode, wire.Retention.Eligibility
	if policy.Retention.Duration, err = parseDuration(wire.Retention.Duration); err != nil {
		return autonomy.Policy{}, err
	}
	if policy.Retention.ScanInterval, err = parseDuration(wire.Retention.ScanInterval); err != nil {
		return autonomy.Policy{}, err
	}
	if err := policy.Validate(); err != nil {
		return autonomy.Policy{}, err
	}
	return policy, nil
}

func NewAutonomousRuntimeProjection(
	trigger autonomy.Trigger,
	policy autonomy.Policy,
	projectionMaxStaleness time.Duration,
	evidence *AutonomousProjectionEvidence,
) (canonical.CanonicalJSON, error) {
	if err := trigger.Validate(); err != nil {
		return canonical.CanonicalJSON{}, err
	}
	frozenPolicy, err := freezeAutonomyPolicy(policy)
	if err != nil {
		return canonical.CanonicalJSON{}, err
	}
	wire := autonomousRuntimeProjectionJSON{
		Schema: AutonomousRuntimeProjectionSchema,
		Trigger: autonomousTriggerJSON{Kind: trigger.Kind, SourceID: trigger.SourceID,
			RelatedClaimIDs: append([]canonical.ID(nil), trigger.RelatedClaimIDs...)},
		Policy: frozenPolicy, ProjectionMaxStaleness: projectionMaxStaleness.String(), Projection: evidence,
	}
	if projectionMaxStaleness < 0 || (evidence != nil && projectionMaxStaleness <= 0) {
		return canonical.CanonicalJSON{}, errors.New("domain: invalid frozen Projection staleness bound")
	}
	if wire.Trigger.RelatedClaimIDs == nil {
		wire.Trigger.RelatedClaimIDs = []canonical.ID{}
	}
	if trigger.Boundary != 0 {
		boundary := trigger.Boundary
		wire.Trigger.Boundary = &boundary
	}
	if trigger.Ordinal != 0 {
		ordinal, _ := canonical.NewOrdinal(trigger.Ordinal)
		wire.Trigger.Ordinal = &ordinal
	}
	return canonical.MarshalCanonical(wire)
}

func ParseAutonomousRuntimeProjection(
	input []byte,
) (autonomy.Trigger, autonomy.Policy, time.Duration, *AutonomousProjectionEvidence, error) {
	canonicalValue, err := canonical.ParseCanonicalJSON(input)
	if err != nil {
		return autonomy.Trigger{}, autonomy.Policy{}, 0, nil, err
	}
	decoder := json.NewDecoder(bytes.NewReader(canonicalValue.Bytes()))
	decoder.DisallowUnknownFields()
	var wire autonomousRuntimeProjectionJSON
	if err := decoder.Decode(&wire); err != nil {
		return autonomy.Trigger{}, autonomy.Policy{}, 0, nil, fmt.Errorf("domain: decode autonomous runtime projection: %w", err)
	}
	if err := consumeAutonomousJSONEOF(decoder); err != nil {
		return autonomy.Trigger{}, autonomy.Policy{}, 0, nil, err
	}
	if wire.Schema != AutonomousRuntimeProjectionSchema {
		return autonomy.Trigger{}, autonomy.Policy{}, 0, nil, errors.New("domain: unsupported autonomous runtime projection schema")
	}
	trigger := autonomy.Trigger{Kind: wire.Trigger.Kind, SourceID: wire.Trigger.SourceID,
		RelatedClaimIDs: append([]canonical.ID(nil), wire.Trigger.RelatedClaimIDs...)}
	if wire.Trigger.Boundary != nil {
		trigger.Boundary = *wire.Trigger.Boundary
	}
	if wire.Trigger.Ordinal != nil {
		trigger.Ordinal = wire.Trigger.Ordinal.Int64()
	}
	if err := trigger.Validate(); err != nil {
		return autonomy.Trigger{}, autonomy.Policy{}, 0, nil, err
	}
	policy, err := thawAutonomyPolicy(wire.Policy)
	if err != nil {
		return autonomy.Trigger{}, autonomy.Policy{}, 0, nil, err
	}
	maxStaleness, err := time.ParseDuration(wire.ProjectionMaxStaleness)
	if err != nil || maxStaleness < 0 || maxStaleness.String() != wire.ProjectionMaxStaleness ||
		(wire.Projection != nil && maxStaleness <= 0) {
		return autonomy.Trigger{}, autonomy.Policy{}, 0, nil, errors.New("domain: invalid frozen Projection staleness bound")
	}
	return trigger, policy, maxStaleness, wire.Projection, nil
}

func consumeAutonomousJSONEOF(decoder *json.Decoder) error {
	var extra any
	if err := decoder.Decode(&extra); !errors.Is(err, io.EOF) {
		if err == nil {
			return errors.New("domain: trailing autonomous runtime projection value")
		}
		return err
	}
	return nil
}

type PrepareAutonomousGeneration struct {
	Generation             PrepareGeneration
	MaxAttempts            int
	Trigger                autonomy.Trigger
	Policy                 autonomy.Policy
	ProjectionMaxStaleness time.Duration
	ProjectionEvidence     *AutonomousProjectionEvidence
	Usages                 []AutonomousClaimUsage
}

type autonomousGenerationPreparer interface {
	PrepareAutonomousGeneration(context.Context, PrepareAutonomousGeneration) error
}

func PrepareAutonomousGenerationCommand(value PrepareAutonomousGeneration) canonical.Command {
	scope, _ := canonical.ResidentScope(value.Generation.ResidentID)
	return command{
		name: "PrepareAutonomousGeneration", scope: scope,
		validate: func() error { return validatePrepareAutonomousGeneration(value) },
		execute: func(ctx context.Context, store MutationStore) (any, error) {
			mutator, ok := store.(autonomousGenerationPreparer)
			if !ok {
				return nil, errors.New("domain: canonical UoW lacks autonomous generation prepare capability")
			}
			return value.Generation.RunID, mutator.PrepareAutonomousGeneration(ctx, value)
		},
	}
}

func validatePrepareAutonomousGeneration(value PrepareAutonomousGeneration) error {
	generation := value.Generation
	if value.MaxAttempts < 1 {
		return errors.New("domain: autonomous prepare requires positive max attempts")
	}
	purpose := generation.Purpose.Effective()
	if purpose != GenerationPurposeSelfTalk && purpose != GenerationPurposeOutboundInitiative {
		return fmt.Errorf("domain: autonomous prepare has non-autonomous purpose %q", purpose)
	}
	if err := value.Trigger.Validate(); err != nil {
		return err
	}
	if purpose == GenerationPurposeSelfTalk && !value.Trigger.Kind.IsSelfTalk() {
		return errors.New("domain: self-talk purpose requires a self-talk trigger")
	}
	if purpose == GenerationPurposeOutboundInitiative && !value.Trigger.Kind.IsInitiative() {
		return errors.New("domain: outbound initiative purpose requires an initiative trigger")
	}
	if purpose == GenerationPurposeOutboundInitiative && value.ProjectionMaxStaleness <= 0 {
		return errors.New("domain: outbound initiative requires a positive Projection staleness bound")
	}
	if purpose == GenerationPurposeOutboundInitiative {
		if value.ProjectionEvidence == nil {
			return errors.New("domain: outbound initiative requires frozen Projection evidence")
		}
		if err := value.ProjectionEvidence.Validate(generation.ResidentID, generation.MemoryPolicyRevisionID); err != nil {
			return err
		}
	} else if value.ProjectionEvidence != nil {
		return errors.New("domain: self-talk cannot carry initiative Projection evidence")
	}
	if err := value.Policy.Validate(); err != nil {
		return err
	}
	wantKey, err := value.Trigger.IdempotencyKey(string(value.Policy.Version), generation.ResidentID)
	if err != nil || generation.IdempotencyKey != wantKey {
		return errors.New("domain: autonomous generation key does not match its trigger")
	}
	if generation.Provider == "" || generation.Model == "" || generation.GeneratorParams.IsZero() ||
		len(generation.Inputs) == 0 || generation.DroppedInputSummary.IsZero() {
		return errors.New("domain: incomplete autonomous generation envelope")
	}
	params, _, err := ParseGeneratorParams(generation.GeneratorParams.Bytes())
	if err != nil {
		return err
	}
	if err := ValidateNewGeneratorParamsForPurpose(purpose, params); err != nil {
		return err
	}
	if params.Streaming {
		return errors.New("domain: autonomous generation must be non-streaming")
	}
	if generation.SessionPolicyID != nil || generation.RecallRunID != nil {
		return errors.New("domain: autonomous generation cannot pin sessionization or Recall")
	}
	if err := ValidateGenerationVersions(purpose, GenerationVersionContract{
		PromptTemplateVersion: generation.PromptTemplateVersion, ContextPolicyVersion: generation.ContextPolicyVersion,
		MemoryRenderingVersion: generation.MemoryRenderingVersion,
	}); err != nil {
		return err
	}
	for _, id := range []canonical.ID{
		generation.RunID, generation.ResidentID, generation.PipelineVersionID,
		generation.PrinciplesRevisionID, generation.PersonaRevisionID,
		generation.MemoryPolicyRevisionID, generation.RunningOutcomeID,
	} {
		if err := id.Validate(); err != nil {
			return err
		}
	}
	if err := generation.AsOfTZ.Validate(); err != nil {
		return err
	}
	claimInputs := make(map[canonical.ID]int64)
	runtimeInputs := 0
	for index, input := range generation.Inputs {
		if input.Ordinal != int64(index) {
			return errors.New("domain: autonomous input ordinals must be contiguous")
		}
		if err := input.ID.Validate(); err != nil {
			return err
		}
		if err := input.Content.Validate(); err != nil {
			return err
		}
		if input.SourceType == "claim" && input.InclusionMode == "memory_recall" {
			if input.SourceID == nil {
				return errors.New("domain: autonomous claim input has no source claim")
			}
			if _, duplicate := claimInputs[*input.SourceID]; duplicate {
				return errors.New("domain: autonomous prompt repeats a claim")
			}
			claimInputs[*input.SourceID] = input.Ordinal
		}
		if input.SourceType == "runtime_projection" && input.InclusionMode == "runtime_projection" {
			runtimeInputs++
			persistedTrigger, persistedPolicy, persistedMaxStaleness, persistedEvidence, err := ParseAutonomousRuntimeProjection(input.Content.Bytes)
			if err != nil || !autonomousTriggersEqual(persistedTrigger, value.Trigger) {
				return errors.New("domain: autonomous runtime input does not match its trigger")
			}
			if !autonomyPoliciesEqual(persistedPolicy, value.Policy) {
				return errors.New("domain: autonomous runtime input does not match its frozen policy")
			}
			if persistedMaxStaleness != value.ProjectionMaxStaleness {
				return errors.New("domain: autonomous runtime input does not match its frozen Projection staleness bound")
			}
			if !autonomousProjectionEvidenceEqual(persistedEvidence, value.ProjectionEvidence) {
				return errors.New("domain: autonomous runtime input does not match frozen Projection evidence")
			}
		}
	}
	if runtimeInputs != 1 {
		return errors.New("domain: autonomous generation requires exactly one runtime projection input")
	}
	if len(claimInputs) != len(value.Usages) {
		return errors.New("domain: autonomous prompt claim/usage count mismatch")
	}
	seenUsage := make(map[canonical.ID]struct{}, len(value.Usages))
	for index, usage := range value.Usages {
		if err := usage.ID.Validate(); err != nil {
			return err
		}
		if err := usage.ClaimID.Validate(); err != nil {
			return err
		}
		if usage.Ordinal.Int64() != int64(index) {
			return errors.New("domain: autonomous usage ordinals must be contiguous")
		}
		if _, duplicate := seenUsage[usage.ClaimID]; duplicate {
			return errors.New("domain: autonomous prompt repeats a claim usage")
		}
		seenUsage[usage.ClaimID] = struct{}{}
		if _, exists := claimInputs[usage.ClaimID]; !exists {
			return errors.New("domain: autonomous usage has no matching prompt input")
		}
	}
	return nil
}

type LandAutonomousEvent struct {
	Attempt
	EventID                canonical.ID
	ResidentPrincipalID    canonical.ID
	OwnerPrincipalID       canonical.ID
	Output                 Content
	OccurredAt             canonical.Instant
	OccurredTZ             canonical.Timezone
	PromptTokens           *int64
	CompletionTokens       *int64
	LatencyMicros          int64
	Trigger                autonomy.Trigger
	Policy                 autonomy.Policy
	ProjectionMaxStaleness time.Duration
	ProjectionEvidence     *AutonomousProjectionEvidence
}

type autonomousEventLander interface {
	LandAutonomousEvent(context.Context, LandAutonomousEvent) (Event, error)
}

type CancelAutonomousGeneration struct {
	RunID              canonical.ID
	ResidentID         canonical.ID
	AttemptNo          int64
	RunningOutcomeID   canonical.ID
	CancelledOutcomeID canonical.ID
	ErrorClass         string
}

type autonomousGenerationCanceller interface {
	CancelAutonomousGeneration(context.Context, CancelAutonomousGeneration) error
}

func CancelAutonomousGenerationCommand(value CancelAutonomousGeneration) canonical.Command {
	scope, _ := canonical.ResidentScope(value.ResidentID)
	return command{
		name: "CancelAutonomousGeneration", scope: scope,
		validate: func() error {
			if value.AttemptNo < 1 {
				return errors.New("domain: autonomous cancellation attempt must be positive")
			}
			for _, id := range []canonical.ID{
				value.RunID, value.ResidentID, value.RunningOutcomeID, value.CancelledOutcomeID,
			} {
				if err := id.Validate(); err != nil {
					return err
				}
			}
			if value.RunningOutcomeID == value.CancelledOutcomeID {
				return errors.New("domain: autonomous cancellation outcome IDs must differ")
			}
			code, err := generation.ParseOutcomeErrorCode(value.ErrorClass)
			if err != nil || code.Class() != generation.ErrorResidentInactive &&
				code.Class() != generation.ErrorSourceContentErased {
				return errors.New("domain: invalid autonomous cancellation error class")
			}
			return nil
		},
		execute: func(ctx context.Context, store MutationStore) (any, error) {
			mutator, ok := store.(autonomousGenerationCanceller)
			if !ok {
				return nil, errors.New("domain: canonical UoW lacks autonomous cancellation capability")
			}
			return nil, mutator.CancelAutonomousGeneration(ctx, value)
		},
	}
}

func LandAutonomousEventCommand(value LandAutonomousEvent) canonical.Command {
	scope, _ := canonical.ResidentScope(value.ResidentID)
	return command{
		name: "LandAutonomousEvent", scope: scope,
		validate: func() error {
			if value.AttemptNo < 1 || value.MaxAttempts < 1 || value.LatencyMicros < 0 {
				return errors.New("domain: invalid autonomous landing attempt or latency")
			}
			for _, id := range []canonical.ID{
				value.RunID, value.ResidentID, value.OutcomeID, value.EventID,
				value.ResidentPrincipalID, value.OwnerPrincipalID,
			} {
				if err := id.Validate(); err != nil {
					return err
				}
			}
			if err := value.Trigger.Validate(); err != nil {
				return err
			}
			if err := value.Policy.Validate(); err != nil {
				return err
			}
			if value.Trigger.Kind.IsInitiative() && value.ProjectionMaxStaleness <= 0 {
				return errors.New("domain: initiative landing requires a positive Projection staleness bound")
			}
			if value.Trigger.Kind.IsInitiative() {
				if value.ProjectionEvidence == nil {
					return errors.New("domain: initiative landing requires frozen Projection evidence")
				}
			} else if value.ProjectionEvidence != nil {
				return errors.New("domain: self-talk landing cannot carry Projection evidence")
			}
			if err := value.OccurredTZ.Validate(); err != nil {
				return err
			}
			return value.Output.Validate()
		},
		execute: func(ctx context.Context, store MutationStore) (any, error) {
			mutator, ok := store.(autonomousEventLander)
			if !ok {
				return nil, errors.New("domain: canonical UoW lacks autonomous event landing capability")
			}
			return mutator.LandAutonomousEvent(ctx, value)
		},
	}
}

type AutonomousWork struct {
	ResidentID          canonical.ID
	Purpose             GenerationPurpose
	Trigger             autonomy.Trigger
	RunID               *canonical.ID
	AttemptNo           int64
	RetryCount          int64
	State               WorkState
	ForegroundPreempted bool
}

type AutonomousClaimContext struct {
	ClaimID   canonical.ID
	Statement string
}

type AutonomyWorkRepository interface {
	DiscoverReevaluationTriggers(context.Context, canonical.ID, int) ([]autonomy.Trigger, error)
	DiscoverInitiativeTriggers(context.Context, canonical.ID, canonical.Instant, time.Duration, int) ([]autonomy.Trigger, error)
	AutonomousSelfTalkEventTimeEligible(context.Context, canonical.ID, autonomy.Trigger) (bool, error)
	AutonomousWork(context.Context, canonical.ID, autonomy.Trigger, int) (AutonomousWork, error)
	AutonomousProjectionEvidence(context.Context, canonical.ID, canonical.Instant, time.Duration) (AutonomousProjectionEvidence, bool, error)
	AutonomousClaimContexts(context.Context, canonical.ID, autonomy.Trigger) ([]AutonomousClaimContext, error)
	DiscoverExecutableAutonomousWork(context.Context, canonical.ID, int, int) ([]AutonomousWork, error)
}

func autonomousTriggersEqual(left, right autonomy.Trigger) bool {
	leftIdentity, leftErr := left.StableIdentity()
	rightIdentity, rightErr := right.StableIdentity()
	return leftErr == nil && rightErr == nil && leftIdentity == rightIdentity
}

func autonomousProjectionEvidenceEqual(left, right *AutonomousProjectionEvidence) bool {
	if left == nil || right == nil {
		return left == nil && right == nil
	}
	leftJSON, leftErr := canonical.MarshalCanonical(left)
	rightJSON, rightErr := canonical.MarshalCanonical(right)
	return leftErr == nil && rightErr == nil && bytes.Equal(leftJSON.Bytes(), rightJSON.Bytes())
}

func autonomyPoliciesEqual(left, right autonomy.Policy) bool {
	leftWire, leftErr := freezeAutonomyPolicy(left)
	rightWire, rightErr := freezeAutonomyPolicy(right)
	if leftErr != nil || rightErr != nil {
		return false
	}
	leftJSON, leftErr := canonical.MarshalCanonical(leftWire)
	rightJSON, rightErr := canonical.MarshalCanonical(rightWire)
	return leftErr == nil && rightErr == nil && bytes.Equal(leftJSON.Bytes(), rightJSON.Bytes())
}
