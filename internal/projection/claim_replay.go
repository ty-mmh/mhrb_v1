package projection

import (
	"context"
	"errors"
	"fmt"
	"math"
	"slices"
	"strings"

	"mahoroba.local/mahoroba/internal/canonical"
)

// ClaimStatesDefinition is the production claim-state contract. The active
// memory policy is an exact watermark dependency, and any activation after a
// stored watermark forces a full rebuild before normal cursor planning.
func ClaimStatesDefinition() Definition {
	return Definition{
		Name: ClaimStatesName, Version: "claim-states-v1", TimeSensitive: true,
		Dependencies:        []DependencyKind{MemoryPolicyDependency},
		RebuildOnActivation: []DependencyKind{MemoryPolicyDependency},
	}
}

type ClaimStage string

const (
	ClaimStageFloating ClaimStage = "floating"
	ClaimStageSediment ClaimStage = "sediment"
	ClaimStageSettled  ClaimStage = "settled"
)

func (value ClaimStage) Validate() error {
	switch value {
	case ClaimStageFloating, ClaimStageSediment, ClaimStageSettled:
		return nil
	default:
		return fmt.Errorf("projection: invalid claim stage %q", value)
	}
}

type ClaimStatus string

const (
	ClaimStatusActive      ClaimStatus = "active"
	ClaimStatusInvalidated ClaimStatus = "invalidated"
	ClaimStatusSuperseded  ClaimStatus = "superseded"
	ClaimStatusQuarantined ClaimStatus = "quarantined"
)

func (value ClaimStatus) Validate() error {
	switch value {
	case ClaimStatusActive, ClaimStatusInvalidated, ClaimStatusSuperseded, ClaimStatusQuarantined:
		return nil
	default:
		return fmt.Errorf("projection: invalid claim status %q", value)
	}
}

type ClaimTemporalKind string

const (
	ClaimTemporalStable   ClaimTemporalKind = "stable"
	ClaimTemporalVolatile ClaimTemporalKind = "volatile"
	ClaimTemporalEpisodic ClaimTemporalKind = "episodic"
)

func (value ClaimTemporalKind) Validate() error {
	switch value {
	case ClaimTemporalStable, ClaimTemporalVolatile, ClaimTemporalEpisodic:
		return nil
	default:
		return fmt.Errorf("projection: invalid claim temporal kind %q", value)
	}
}

type ClaimEvidencePolarity string

const (
	ClaimEvidenceSupport    ClaimEvidencePolarity = "support"
	ClaimEvidenceContradict ClaimEvidencePolarity = "contradict"
)

func (value ClaimEvidencePolarity) Validate() error {
	switch value {
	case ClaimEvidenceSupport, ClaimEvidenceContradict:
		return nil
	default:
		return fmt.Errorf("projection: invalid claim evidence polarity %q", value)
	}
}

type ClaimEvidenceGrade string

const (
	ClaimEvidenceStated   ClaimEvidenceGrade = "stated"
	ClaimEvidenceObserved ClaimEvidenceGrade = "observed"
	ClaimEvidenceInferred ClaimEvidenceGrade = "inferred"
)

func (value ClaimEvidenceGrade) Validate() error {
	switch value {
	case ClaimEvidenceStated, ClaimEvidenceObserved, ClaimEvidenceInferred:
		return nil
	default:
		return fmt.Errorf("projection: invalid claim evidence grade %q", value)
	}
}

type ClaimTrustLevel string

const (
	ClaimTrustTrusted   ClaimTrustLevel = "trusted"
	ClaimTrustUntrusted ClaimTrustLevel = "untrusted"
)

func (value ClaimTrustLevel) Validate() error {
	switch value {
	case ClaimTrustTrusted, ClaimTrustUntrusted:
		return nil
	default:
		return fmt.Errorf("projection: invalid claim trust level %q", value)
	}
}

type ClaimSourceEventType string

const (
	ClaimSourceUserMessage        ClaimSourceEventType = "user_message"
	ClaimSourceResidentMessage    ClaimSourceEventType = "resident_message"
	ClaimSourceSelfTalk           ClaimSourceEventType = "self_talk"
	ClaimSourceOutboundInitiative ClaimSourceEventType = "outbound_initiative"
)

func (value ClaimSourceEventType) Validate() error {
	switch value {
	case ClaimSourceUserMessage, ClaimSourceResidentMessage, ClaimSourceSelfTalk, ClaimSourceOutboundInitiative:
		return nil
	default:
		return fmt.Errorf("projection: invalid claim source event type %q", value)
	}
}

type ClaimEvidenceDerivation string

const (
	ClaimEvidenceExtracted ClaimEvidenceDerivation = "extracted"
	ClaimEvidenceInherited ClaimEvidenceDerivation = "inherited"
)

func (value ClaimEvidenceDerivation) Validate() error {
	switch value {
	case ClaimEvidenceExtracted, ClaimEvidenceInherited:
		return nil
	default:
		return fmt.Errorf("projection: invalid claim evidence derivation %q", value)
	}
}

type ClaimUsageType string

const (
	ClaimUsageCandidate            ClaimUsageType = "candidate"
	ClaimUsageSelected             ClaimUsageType = "selected"
	ClaimUsagePromptIncluded       ClaimUsageType = "prompt_included"
	ClaimUsageExplicitlyReferenced ClaimUsageType = "explicitly_referenced"
)

func (value ClaimUsageType) Validate() error {
	switch value {
	case ClaimUsageCandidate, ClaimUsageSelected, ClaimUsagePromptIncluded, ClaimUsageExplicitlyReferenced:
		return nil
	default:
		return fmt.Errorf("projection: invalid claim usage type %q", value)
	}
}

type ClaimTemporalRelation string

const (
	ClaimTemporalFuture       ClaimTemporalRelation = "future"
	ClaimTemporalCurrent      ClaimTemporalRelation = "current"
	ClaimTemporalPast         ClaimTemporalRelation = "past"
	ClaimTemporalStaleUnknown ClaimTemporalRelation = "stale_unknown"
)

func (value ClaimTemporalRelation) Validate() error {
	switch value {
	case ClaimTemporalFuture, ClaimTemporalCurrent, ClaimTemporalPast, ClaimTemporalStaleUnknown:
		return nil
	default:
		return fmt.Errorf("projection: invalid claim temporal relation %q", value)
	}
}

type ClaimSeed struct {
	ClaimID      canonical.ID
	CommitSeq    canonical.CommitSeq
	RecordedAt   canonical.Instant
	TemporalKind ClaimTemporalKind
}

type ClaimEvidence struct {
	EvidenceID         canonical.ID
	ClaimID            canonical.ID
	SourceEventID      canonical.ID
	SourceEvidenceID   *canonical.ID
	CommitSeq          canonical.CommitSeq
	RecordedAt         canonical.Instant
	PolicyRevisionID   canonical.ID
	SourceEventType    ClaimSourceEventType
	Polarity           ClaimEvidencePolarity
	Grade              ClaimEvidenceGrade
	TrustLevel         ClaimTrustLevel
	Derivation         ClaimEvidenceDerivation
	InheritanceDepth   int64
	ReasonCode         string
	ActorIsSubject     bool
	ActorIsPerspective bool
	Weight             canonical.Weight
}

type ClaimUsage struct {
	UsageID canonical.ID
	ClaimID canonical.ID
	// RecallRunID groups candidate/selected/prompt rows emitted by one Recall.
	// It is nil for non-Recall usage such as a standalone explicit reference.
	RecallRunID      *canonical.ID
	CommitSeq        canonical.CommitSeq
	RecordedAt       canonical.Instant
	PolicyRevisionID canonical.ID
	UsageType        ClaimUsageType
}

type ClaimValidityAssertion struct {
	AssertionID canonical.ID
	ClaimID     canonical.ID
	CommitSeq   canonical.CommitSeq
	RecordedAt  canonical.Instant
	ValidFrom   *canonical.Instant
	ValidTo     *canonical.Instant
	Confidence  *canonical.Ratio
}

// ClaimTransition represents either a status or stage transition. From is
// empty only for the initial NULL -> floating stage transition.
type ClaimTransition struct {
	TransitionID canonical.ID
	ClaimID      canonical.ID
	CommitSeq    canonical.CommitSeq
	RecordedAt   canonical.Instant
	From         string
	To           string
}

type ClaimReplayInput struct {
	ThroughCommitSeq       canonical.CommitSeq
	AsOf                   canonical.Instant
	PreviousAsOf           *canonical.Instant
	ActivePolicyRevisionID canonical.ID
	Claims                 []ClaimSeed
	Evidence               []ClaimEvidence
	Usages                 []ClaimUsage
	ValidityAssertions     []ClaimValidityAssertion
	StatusTransitions      []ClaimTransition
	StageTransitions       []ClaimTransition
}

// ClaimEvaluationInput is the policy-owned numeric calculation boundary. A
// ClaimStateEvaluator must evaluate each evidence/usage row under that row's
// PolicyRevisionID, then aggregate and decay using ActivePolicyRevisionID at
// the explicit AsOf. Projection replay owns ordering, capture bounds, and the
// status/stage state machine; it deliberately does not duplicate policy math.
type ClaimEvaluationInput struct {
	Claim                  ClaimSeed
	Status                 ClaimStatus
	Stage                  ClaimStage
	AsOf                   canonical.Instant
	ActivePolicyRevisionID canonical.ID
	Evidence               []ClaimEvidence
	Usages                 []ClaimUsage
	ValidityAssertions     []ClaimValidityAssertion
}

type ClaimStateMetrics struct {
	Salience         float64
	Confidence       canonical.Ratio
	Currentness      canonical.Ratio
	TemporalRelation ClaimTemporalRelation
	LastReferencedAt *canonical.Instant
}

type ClaimState struct {
	ClaimID          canonical.ID
	Stage            ClaimStage
	Status           ClaimStatus
	Salience         float64
	Confidence       canonical.Ratio
	Currentness      canonical.Ratio
	TemporalRelation ClaimTemporalRelation
	LastReferencedAt *canonical.Instant
	EvidenceCount    canonical.Count
}

// ClaimStateEvaluator is implemented by the versioned memory-policy package.
// Keeping this interface in projection avoids a projection -> memory import
// while still making every replay input and output strongly typed.
type ClaimStateEvaluator interface {
	EvaluateClaim(context.Context, ClaimEvaluationInput) (ClaimStateMetrics, error)
}

// ClaimStateEvaluatorFunc is the function-injection form used by focused
// tests and small adapters.
type ClaimStateEvaluatorFunc func(context.Context, ClaimEvaluationInput) (ClaimStateMetrics, error)

func (function ClaimStateEvaluatorFunc) EvaluateClaim(ctx context.Context, input ClaimEvaluationInput) (ClaimStateMetrics, error) {
	return function(ctx, input)
}

// ReplayClaimStates evaluates a complete resident snapshot bounded by the
// captured commit cursor and explicit as_of. Rows outside either bound are
// ignored, so a source adapter cannot accidentally leak a later commit into
// the result even if its reader races with Canonical writes.
func ReplayClaimStates(ctx context.Context, input ClaimReplayInput, evaluator ClaimStateEvaluator) ([]ClaimState, error) {
	if ctx == nil {
		return nil, errors.New("projection: nil claim replay context")
	}
	if err := input.ThroughCommitSeq.Validate(); err != nil {
		return nil, err
	}
	if input.PreviousAsOf != nil && input.AsOf < *input.PreviousAsOf {
		return nil, ErrAsOfRegression
	}
	if err := input.ActivePolicyRevisionID.Validate(); err != nil {
		return nil, fmt.Errorf("%w: invalid active memory policy: %v", ErrUnresolvedDependency, err)
	}
	if evaluator == nil {
		return nil, ErrClaimStateEvaluatorUnavailable
	}

	claims := make([]ClaimSeed, 0, len(input.Claims))
	claimIDs := make(map[canonical.ID]struct{}, len(input.Claims))
	for _, claim := range input.Claims {
		if err := validateClaimSeed(claim); err != nil {
			return nil, err
		}
		if claim.CommitSeq > input.ThroughCommitSeq || claim.RecordedAt > input.AsOf {
			continue
		}
		if _, duplicate := claimIDs[claim.ClaimID]; duplicate {
			return nil, fmt.Errorf("projection: duplicate claim seed %s", claim.ClaimID)
		}
		claimIDs[claim.ClaimID] = struct{}{}
		claims = append(claims, claim)
	}
	slices.SortFunc(claims, func(left, right ClaimSeed) int {
		return strings.Compare(left.ClaimID.String(), right.ClaimID.String())
	})

	evidence, err := capturedEvidence(input.Evidence, claimIDs, input.ThroughCommitSeq, input.AsOf)
	if err != nil {
		return nil, err
	}
	usages, err := capturedUsages(input.Usages, claimIDs, input.ThroughCommitSeq, input.AsOf)
	if err != nil {
		return nil, err
	}
	validity, err := capturedValidity(input.ValidityAssertions, claimIDs, input.ThroughCommitSeq, input.AsOf)
	if err != nil {
		return nil, err
	}
	if err := validateCapturedTransitions(input.StatusTransitions, claimIDs, input.ThroughCommitSeq, input.AsOf); err != nil {
		return nil, err
	}
	if err := validateCapturedTransitions(input.StageTransitions, claimIDs, input.ThroughCommitSeq, input.AsOf); err != nil {
		return nil, err
	}

	states := make([]ClaimState, 0, len(claims))
	for _, claim := range claims {
		statusRaw, err := replayClaimTransition(string(ClaimStatusActive), claim.ClaimID, input.StatusTransitions, input.ThroughCommitSeq, input.AsOf, validClaimStatus)
		if err != nil {
			return nil, err
		}
		stageRaw, err := replayClaimTransition(string(ClaimStageFloating), claim.ClaimID, input.StageTransitions, input.ThroughCommitSeq, input.AsOf, validClaimStage)
		if err != nil {
			return nil, err
		}
		claimEvidence := append([]ClaimEvidence(nil), evidence[claim.ClaimID]...)
		claimUsages := append([]ClaimUsage(nil), usages[claim.ClaimID]...)
		claimValidity := append([]ClaimValidityAssertion(nil), validity[claim.ClaimID]...)
		metrics, err := evaluator.EvaluateClaim(ctx, ClaimEvaluationInput{
			Claim: claim, Status: ClaimStatus(statusRaw), Stage: ClaimStage(stageRaw), AsOf: input.AsOf,
			ActivePolicyRevisionID: input.ActivePolicyRevisionID,
			Evidence:               claimEvidence, Usages: claimUsages, ValidityAssertions: claimValidity,
		})
		if err != nil {
			return nil, fmt.Errorf("projection: evaluate claim %s: %w", claim.ClaimID, err)
		}
		if err := validateClaimStateMetrics(metrics, input.AsOf); err != nil {
			return nil, fmt.Errorf("projection: evaluate claim %s: %w", claim.ClaimID, err)
		}
		states = append(states, ClaimState{
			ClaimID: claim.ClaimID, Status: ClaimStatus(statusRaw), Stage: ClaimStage(stageRaw),
			Salience: metrics.Salience, Confidence: metrics.Confidence, Currentness: metrics.Currentness,
			TemporalRelation: metrics.TemporalRelation, LastReferencedAt: cloneInstantPointer(metrics.LastReferencedAt),
			EvidenceCount: canonical.Count(len(claimEvidence)),
		})
	}
	return states, nil
}

func validateClaimSeed(claim ClaimSeed) error {
	if err := claim.ClaimID.Validate(); err != nil {
		return fmt.Errorf("projection: invalid claim id: %w", err)
	}
	if err := claim.CommitSeq.Validate(); err != nil {
		return fmt.Errorf("projection: invalid claim commit: %w", err)
	}
	return claim.TemporalKind.Validate()
}

func capturedEvidence(rows []ClaimEvidence, claimIDs map[canonical.ID]struct{}, through canonical.CommitSeq, asOf canonical.Instant) (map[canonical.ID][]ClaimEvidence, error) {
	result := make(map[canonical.ID][]ClaimEvidence)
	for _, row := range rows {
		if err := validateClaimEvidence(row); err != nil {
			return nil, err
		}
		if row.CommitSeq > through || row.RecordedAt > asOf {
			continue
		}
		if _, exists := claimIDs[row.ClaimID]; !exists {
			return nil, fmt.Errorf("projection: evidence %s references uncaptured claim %s", row.EvidenceID, row.ClaimID)
		}
		result[row.ClaimID] = append(result[row.ClaimID], row)
	}
	for claimID := range result {
		slices.SortFunc(result[claimID], compareClaimEvidence)
	}
	return result, nil
}

func capturedUsages(rows []ClaimUsage, claimIDs map[canonical.ID]struct{}, through canonical.CommitSeq, asOf canonical.Instant) (map[canonical.ID][]ClaimUsage, error) {
	result := make(map[canonical.ID][]ClaimUsage)
	for _, row := range rows {
		if err := validateClaimUsage(row); err != nil {
			return nil, err
		}
		if row.CommitSeq > through || row.RecordedAt > asOf {
			continue
		}
		if _, exists := claimIDs[row.ClaimID]; !exists {
			return nil, fmt.Errorf("projection: usage %s references uncaptured claim %s", row.UsageID, row.ClaimID)
		}
		result[row.ClaimID] = append(result[row.ClaimID], row)
	}
	for claimID := range result {
		slices.SortFunc(result[claimID], compareClaimUsage)
	}
	return result, nil
}

func capturedValidity(rows []ClaimValidityAssertion, claimIDs map[canonical.ID]struct{}, through canonical.CommitSeq, asOf canonical.Instant) (map[canonical.ID][]ClaimValidityAssertion, error) {
	result := make(map[canonical.ID][]ClaimValidityAssertion)
	for _, row := range rows {
		if err := validateClaimValidity(row); err != nil {
			return nil, err
		}
		if row.CommitSeq > through || row.RecordedAt > asOf {
			continue
		}
		if _, exists := claimIDs[row.ClaimID]; !exists {
			return nil, fmt.Errorf("projection: validity assertion %s references uncaptured claim %s", row.AssertionID, row.ClaimID)
		}
		result[row.ClaimID] = append(result[row.ClaimID], row)
	}
	for claimID := range result {
		slices.SortFunc(result[claimID], compareClaimValidity)
	}
	return result, nil
}

func validateCapturedTransitions(rows []ClaimTransition, claimIDs map[canonical.ID]struct{}, through canonical.CommitSeq, asOf canonical.Instant) error {
	for _, row := range rows {
		if err := row.TransitionID.Validate(); err != nil {
			return fmt.Errorf("projection: invalid claim transition id: %w", err)
		}
		if err := row.ClaimID.Validate(); err != nil {
			return fmt.Errorf("projection: invalid claim transition claim: %w", err)
		}
		if err := row.CommitSeq.Validate(); err != nil {
			return fmt.Errorf("projection: invalid claim transition commit: %w", err)
		}
		if row.CommitSeq > through || row.RecordedAt > asOf {
			continue
		}
		if _, exists := claimIDs[row.ClaimID]; !exists {
			return fmt.Errorf("projection: transition %s references uncaptured claim %s", row.TransitionID, row.ClaimID)
		}
	}
	return nil
}

func validateClaimEvidence(row ClaimEvidence) error {
	for label, id := range map[string]canonical.ID{
		"evidence": row.EvidenceID, "claim": row.ClaimID, "source event": row.SourceEventID, "policy": row.PolicyRevisionID,
	} {
		if err := id.Validate(); err != nil {
			return fmt.Errorf("projection: invalid claim evidence %s id: %w", label, err)
		}
	}
	if row.SourceEvidenceID != nil {
		if err := row.SourceEvidenceID.Validate(); err != nil {
			return fmt.Errorf("projection: invalid source evidence id: %w", err)
		}
		if *row.SourceEvidenceID == row.EvidenceID {
			return errors.New("projection: claim evidence cannot inherit from itself")
		}
	}
	if err := row.CommitSeq.Validate(); err != nil {
		return err
	}
	if err := row.SourceEventType.Validate(); err != nil {
		return err
	}
	if err := row.Polarity.Validate(); err != nil {
		return err
	}
	if err := row.Grade.Validate(); err != nil {
		return err
	}
	if err := row.TrustLevel.Validate(); err != nil {
		return err
	}
	if err := row.Derivation.Validate(); err != nil {
		return err
	}
	if (row.Derivation == ClaimEvidenceExtracted && (row.SourceEvidenceID != nil || row.InheritanceDepth != 0)) ||
		(row.Derivation == ClaimEvidenceInherited && (row.SourceEvidenceID == nil || row.InheritanceDepth < 1)) {
		return errors.New("projection: claim evidence derivation provenance is inconsistent")
	}
	if row.ReasonCode == "" || strings.TrimSpace(row.ReasonCode) != row.ReasonCode {
		return errors.New("projection: claim evidence reason code is empty or has surrounding whitespace")
	}
	return row.Weight.Validate()
}

func validateClaimUsage(row ClaimUsage) error {
	for label, id := range map[string]canonical.ID{"usage": row.UsageID, "claim": row.ClaimID, "policy": row.PolicyRevisionID} {
		if err := id.Validate(); err != nil {
			return fmt.Errorf("projection: invalid claim usage %s id: %w", label, err)
		}
	}
	if row.RecallRunID != nil {
		if err := row.RecallRunID.Validate(); err != nil {
			return fmt.Errorf("projection: invalid claim usage recall run id: %w", err)
		}
	}
	if err := row.CommitSeq.Validate(); err != nil {
		return err
	}
	return row.UsageType.Validate()
}

func validateClaimValidity(row ClaimValidityAssertion) error {
	if err := row.AssertionID.Validate(); err != nil {
		return err
	}
	if err := row.ClaimID.Validate(); err != nil {
		return err
	}
	if err := row.CommitSeq.Validate(); err != nil {
		return err
	}
	if row.ValidFrom != nil && row.ValidTo != nil && *row.ValidFrom > *row.ValidTo {
		return errors.New("projection: claim validity valid_from is after valid_to")
	}
	if row.Confidence != nil {
		return row.Confidence.Validate()
	}
	return nil
}

func validateClaimStateMetrics(metrics ClaimStateMetrics, asOf canonical.Instant) error {
	if math.IsNaN(metrics.Salience) || math.IsInf(metrics.Salience, 0) {
		return errors.New("claim salience must be finite")
	}
	if err := metrics.Confidence.Validate(); err != nil {
		return err
	}
	if err := metrics.Currentness.Validate(); err != nil {
		return err
	}
	if err := metrics.TemporalRelation.Validate(); err != nil {
		return err
	}
	if metrics.LastReferencedAt != nil && *metrics.LastReferencedAt > asOf {
		return errors.New("claim last reference is after explicit as_of")
	}
	return nil
}

func compareClaimEvidence(left, right ClaimEvidence) int {
	return compareCommitAndID(left.CommitSeq, left.EvidenceID, right.CommitSeq, right.EvidenceID)
}

func compareClaimUsage(left, right ClaimUsage) int {
	return compareCommitAndID(left.CommitSeq, left.UsageID, right.CommitSeq, right.UsageID)
}

func compareClaimValidity(left, right ClaimValidityAssertion) int {
	return compareCommitAndID(left.CommitSeq, left.AssertionID, right.CommitSeq, right.AssertionID)
}

func compareCommitAndID(leftCommit canonical.CommitSeq, leftID canonical.ID, rightCommit canonical.CommitSeq, rightID canonical.ID) int {
	if leftCommit < rightCommit {
		return -1
	}
	if leftCommit > rightCommit {
		return 1
	}
	return strings.Compare(leftID.String(), rightID.String())
}

func replayClaimTransition(initial string, claimID canonical.ID, transitions []ClaimTransition, through canonical.CommitSeq, asOf canonical.Instant, valid func(string) bool) (string, error) {
	selected := make([]ClaimTransition, 0)
	for _, transition := range transitions {
		if transition.ClaimID == claimID && transition.CommitSeq <= through && transition.RecordedAt <= asOf {
			selected = append(selected, transition)
		}
	}
	slices.SortFunc(selected, func(left, right ClaimTransition) int {
		return compareCommitAndID(left.CommitSeq, left.TransitionID, right.CommitSeq, right.TransitionID)
	})
	current := initial
	for index, transition := range selected {
		if !valid(transition.To) || (transition.From != "" && !valid(transition.From)) {
			return "", fmt.Errorf("projection: invalid claim transition %q -> %q", transition.From, transition.To)
		}
		if transition.From == "" {
			if index != 0 || transition.To != initial {
				return "", fmt.Errorf("projection: invalid initial claim transition NULL -> %s", transition.To)
			}
			current = transition.To
			continue
		}
		if transition.From != current || transition.To == current {
			return "", fmt.Errorf("projection: invalid claim transition %s -> %s from current %s", transition.From, transition.To, current)
		}
		current = transition.To
	}
	return current, nil
}

func validClaimStatus(value string) bool { return ClaimStatus(value).Validate() == nil }
func validClaimStage(value string) bool  { return ClaimStage(value).Validate() == nil }

func cloneInstantPointer(value *canonical.Instant) *canonical.Instant {
	if value == nil {
		return nil
	}
	copyValue := *value
	return &copyValue
}
