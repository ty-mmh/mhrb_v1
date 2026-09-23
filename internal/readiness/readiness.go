// Package readiness evaluates the shared M7 service-activation predicate.
//
// The package deliberately owns no database or HTTP details. A Source captures
// all Canonical inputs in one read snapshot, while a ProjectionChecker binds
// the selected resident's required derived state to that captured head.
package readiness

import (
	"context"
	"errors"
	"fmt"
	"slices"

	"mahoroba.local/mahoroba/internal/canonical"
)

type ReasonCode string

const (
	ReasonReady              ReasonCode = "ready"
	ReasonStartupIncomplete  ReasonCode = "startup_incomplete"
	ReasonResidentUnselected ReasonCode = "active_resident_unselected"
	ReasonResidentNotActive  ReasonCode = "active_resident_not_active"
	ReasonMemoryPolicy       ReasonCode = "memory_policy_not_service_current"
	ReasonSessionUnresolved  ReasonCode = "session_policy_unresolved"
	ReasonIntegrityBlocked   ReasonCode = "integrity_readiness_blocked"
	ReasonProjectionCurrent  ReasonCode = "projection_not_current"
	ReasonShutdown           ReasonCode = "shutdown_in_progress"
)

var notReadyReasonOrder = []ReasonCode{
	ReasonStartupIncomplete,
	ReasonResidentUnselected,
	ReasonResidentNotActive,
	ReasonMemoryPolicy,
	ReasonSessionUnresolved,
	ReasonIntegrityBlocked,
	ReasonProjectionCurrent,
	ReasonShutdown,
}

// NotReadyReasonOrder returns a copy of the closed CLI/diagnostics priority
// order. Consumers cannot extend or reorder the service contract.
func NotReadyReasonOrder() []ReasonCode {
	return append([]ReasonCode(nil), notReadyReasonOrder...)
}

type BlockingRule string

const (
	RuleActiveRequiredRevisionErased     BlockingRule = "active_required_revision_erased"
	RuleRunningAttemptInputErased        BlockingRule = "running_attempt_input_erased"
	RuleCancellationEnvelopeUnresolvable BlockingRule = "mandatory_work_cancellation_envelope_unresolvable"
)

var blockingRuleOrder = []BlockingRule{
	RuleActiveRequiredRevisionErased,
	RuleRunningAttemptInputErased,
	RuleCancellationEnvelopeUnresolvable,
}

func blockingRuleIndex(rule BlockingRule) int {
	return slices.Index(blockingRuleOrder, rule)
}

// Head is the rich, exact Canonical head used by CLI and diagnostic output.
// Unlike canonical.Head it retains the commit identity and timezone.
type Head struct {
	Exists      bool
	CommitID    canonical.ID
	CommitSeq   canonical.CommitSeq
	CommittedAt canonical.Instant
	CommittedTZ canonical.Timezone
}

func (head Head) Validate() error {
	if !head.Exists {
		if head.CommitID != (canonical.ID{}) || head.CommitSeq != 0 ||
			head.CommittedAt != 0 || head.CommittedTZ != "" {
			return errors.New("readiness: empty head contains cursor state")
		}
		return nil
	}
	if err := head.CommitID.Validate(); err != nil {
		return fmt.Errorf("readiness: invalid head commit ID: %w", err)
	}
	if err := head.CommitSeq.Validate(); err != nil {
		return fmt.Errorf("readiness: invalid head commit sequence: %w", err)
	}
	if err := head.CommittedTZ.Validate(); err != nil {
		return fmt.Errorf("readiness: invalid head timezone: %w", err)
	}
	return nil
}

func (head Head) Canonical() canonical.Head {
	if !head.Exists {
		return canonical.Head{}
	}
	return canonical.Head{
		Exists: true, CommitSeq: head.CommitSeq, CommittedAt: head.CommittedAt,
	}
}

// PredicateSummary contains only non-sensitive aggregate evidence. It never
// exposes a target identity, content bytes, digest, filesystem path, or SQL
// error. RecordedCount is coverage by an exact append-only finding at this
// snapshot; it does not make historical finding rows authoritative.
type PredicateSummary struct {
	Rule          BlockingRule
	CurrentCount  uint64
	RecordedCount uint64
}

func (summary PredicateSummary) Validate() error {
	if blockingRuleIndex(summary.Rule) < 0 {
		return fmt.Errorf("readiness: unknown blocking rule %q", summary.Rule)
	}
	if summary.CurrentCount == 0 {
		return fmt.Errorf("readiness: blocking summary %s has no current predicate", summary.Rule)
	}
	if summary.RecordedCount > summary.CurrentCount {
		return fmt.Errorf("readiness: blocking summary %s overstates finding coverage", summary.Rule)
	}
	return nil
}

func (summary PredicateSummary) ScanIncomplete() bool {
	return summary.RecordedCount < summary.CurrentCount
}

// Snapshot is captured by one source transaction at one immutable Canonical
// head. A nil session policy means runtime_config and the unique valid saved
// activity_sessions dependency could not resolve one.
type Snapshot struct {
	CapturedHead         Head
	ActiveResidentID     *canonical.ID
	ActiveResidentStatus string
	// MemoryPolicyServiceCurrent is true only for the disabled bootstrap V1
	// policy or the current V4 policy. Historical enabled V2/V3 policies remain
	// readable for frozen work but cannot open service admission after cutover.
	MemoryPolicyServiceCurrent bool
	SessionPolicyID            *canonical.ID
	BlockingPredicates         []PredicateSummary
}

func (snapshot Snapshot) Validate() error {
	if err := snapshot.CapturedHead.Validate(); err != nil {
		return err
	}
	if !snapshot.CapturedHead.Exists &&
		(snapshot.ActiveResidentID != nil || len(snapshot.BlockingPredicates) != 0) {
		return errors.New("readiness: empty Canonical head contains operational state")
	}
	if snapshot.ActiveResidentID == nil {
		if snapshot.ActiveResidentStatus != "" || snapshot.MemoryPolicyServiceCurrent || snapshot.SessionPolicyID != nil {
			return errors.New("readiness: unselected snapshot contains resident state")
		}
	} else {
		if err := snapshot.ActiveResidentID.Validate(); err != nil {
			return fmt.Errorf("readiness: invalid active resident ID: %w", err)
		}
		switch snapshot.ActiveResidentStatus {
		case "", "draft", "active", "archived", "erased":
		default:
			return fmt.Errorf("readiness: invalid resident status %q", snapshot.ActiveResidentStatus)
		}
		if snapshot.SessionPolicyID != nil {
			if err := snapshot.SessionPolicyID.Validate(); err != nil {
				return fmt.Errorf("readiness: invalid session policy ID: %w", err)
			}
		}
	}
	previous := -1
	for _, summary := range snapshot.BlockingPredicates {
		if err := summary.Validate(); err != nil {
			return err
		}
		index := blockingRuleIndex(summary.Rule)
		if index <= previous {
			return errors.New("readiness: blocking predicates are duplicated or out of order")
		}
		previous = index
	}
	return nil
}

type Source interface {
	CaptureServiceReadiness(context.Context) (Snapshot, error)
}

// ProjectionRequirement binds the derived-state check to the same captured
// head and exact runtime selection used by the Canonical predicate.
type ProjectionRequirement struct {
	CapturedHead    Head
	ResidentID      canonical.ID
	SessionPolicyID canonical.ID
}

type ProjectionChecker interface {
	Current(context.Context, ProjectionRequirement) (bool, error)
}

type ProjectionCheckFunc func(context.Context, ProjectionRequirement) (bool, error)

func (check ProjectionCheckFunc) Current(ctx context.Context, requirement ProjectionRequirement) (bool, error) {
	if check == nil {
		return false, errors.New("readiness: nil Projection checker")
	}
	return check(ctx, requirement)
}

// CanonicalProjectionProof adapts an already-completed synchronous Projection
// preflight. projection.PreflightResult.Target.Head can be assigned directly
// to CanonicalProjectionProof.Head; Current rejects a proof captured for
// another resident or head.
type CanonicalProjectionProof struct {
	ResidentID      canonical.ID
	SessionPolicyID canonical.ID
	Head            canonical.Head
	IsCurrent       bool
}

func (proof CanonicalProjectionProof) Current(_ context.Context, requirement ProjectionRequirement) (bool, error) {
	if err := proof.ResidentID.Validate(); err != nil {
		return false, fmt.Errorf("readiness: invalid Projection proof resident: %w", err)
	}
	if err := proof.Head.Validate(); err != nil {
		return false, fmt.Errorf("readiness: invalid Projection proof head: %w", err)
	}
	if err := proof.SessionPolicyID.Validate(); err != nil {
		return false, fmt.Errorf("readiness: invalid Projection proof session policy: %w", err)
	}
	if proof.ResidentID != requirement.ResidentID || proof.Head != requirement.CapturedHead.Canonical() {
		return false, errors.New("readiness: Projection proof is not bound to the captured selection and head")
	}
	if proof.SessionPolicyID != requirement.SessionPolicyID {
		return false, errors.New("readiness: Projection proof is not bound to the captured session policy")
	}
	return proof.IsCurrent, nil
}

type Request struct {
	// StartupComplete is set only after synchronous recovery, integrity scan,
	// cancellation terminalization, and other startup mutations finish.
	StartupComplete    bool
	ShutdownInProgress bool
	Projection         ProjectionChecker
}

type Result struct {
	CapturedHead               Head
	Ready                      bool
	ReasonCodes                []ReasonCode
	ActiveResidentID           *canonical.ID
	ActiveResidentStatus       string
	MemoryPolicyServiceCurrent bool
	SessionPolicyID            *canonical.ID
	BlockingPredicates         []PredicateSummary
	IntegrityScanIncomplete    bool
	ProjectionCurrent          bool
}

// EvaluateServiceReadiness applies the single ordered readiness contract used
// by serve, restore, health, and diagnostics. The current predicate snapshot,
// never the historical finding row count, determines integrity readiness.
func EvaluateServiceReadiness(
	ctx context.Context,
	source Source,
	request Request,
) (Result, error) {
	if ctx == nil {
		return Result{}, errors.New("readiness: nil evaluation context")
	}
	if source == nil {
		return Result{}, errors.New("readiness: nil snapshot source")
	}
	snapshot, err := source.CaptureServiceReadiness(ctx)
	if err != nil {
		return Result{}, fmt.Errorf("readiness: capture snapshot: %w", err)
	}
	if err := snapshot.Validate(); err != nil {
		return Result{}, err
	}
	result := Result{
		CapturedHead:               snapshot.CapturedHead,
		ActiveResidentID:           cloneID(snapshot.ActiveResidentID),
		ActiveResidentStatus:       snapshot.ActiveResidentStatus,
		MemoryPolicyServiceCurrent: snapshot.MemoryPolicyServiceCurrent,
		SessionPolicyID:            cloneID(snapshot.SessionPolicyID),
		BlockingPredicates:         append([]PredicateSummary(nil), snapshot.BlockingPredicates...),
	}
	for _, predicate := range result.BlockingPredicates {
		result.IntegrityScanIncomplete = result.IntegrityScanIncomplete || predicate.ScanIncomplete()
	}

	if !request.StartupComplete {
		result.ReasonCodes = append(result.ReasonCodes, ReasonStartupIncomplete)
	}
	if snapshot.ActiveResidentID == nil {
		result.ReasonCodes = append(result.ReasonCodes, ReasonResidentUnselected)
	} else {
		if snapshot.ActiveResidentStatus != "active" {
			result.ReasonCodes = append(result.ReasonCodes, ReasonResidentNotActive)
		} else if !snapshot.MemoryPolicyServiceCurrent {
			result.ReasonCodes = append(result.ReasonCodes, ReasonMemoryPolicy)
		}
		if snapshot.SessionPolicyID == nil {
			result.ReasonCodes = append(result.ReasonCodes, ReasonSessionUnresolved)
		}
	}
	if len(snapshot.BlockingPredicates) != 0 {
		result.ReasonCodes = append(result.ReasonCodes, ReasonIntegrityBlocked)
	}

	projectionApplicable := snapshot.ActiveResidentID != nil &&
		snapshot.ActiveResidentStatus == "active" && snapshot.MemoryPolicyServiceCurrent &&
		snapshot.SessionPolicyID != nil
	if projectionApplicable {
		if request.Projection == nil {
			return Result{}, errors.New("readiness: Projection checker is required for an eligible resident")
		}
		current, err := request.Projection.Current(ctx, ProjectionRequirement{
			CapturedHead:    snapshot.CapturedHead,
			ResidentID:      *snapshot.ActiveResidentID,
			SessionPolicyID: *snapshot.SessionPolicyID,
		})
		if err != nil {
			return Result{}, fmt.Errorf("readiness: check required Projections: %w", err)
		}
		result.ProjectionCurrent = current
		if !current {
			result.ReasonCodes = append(result.ReasonCodes, ReasonProjectionCurrent)
		}
	}
	if request.ShutdownInProgress {
		result.ReasonCodes = append(result.ReasonCodes, ReasonShutdown)
	}
	if len(result.ReasonCodes) == 0 {
		result.Ready = true
		result.ReasonCodes = []ReasonCode{ReasonReady}
	}
	return result, nil
}

func cloneID(value *canonical.ID) *canonical.ID {
	if value == nil {
		return nil
	}
	copy := *value
	return &copy
}
