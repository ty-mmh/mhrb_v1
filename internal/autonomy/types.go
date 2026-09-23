// Package autonomy contains the database-independent policy and scheduling
// rules for bounded resident activity. It deliberately has no provider,
// application, SQL, or blob-store dependency.
package autonomy

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"

	"mahoroba.local/mahoroba/internal/canonical"
)

var (
	ErrInvalidPolicy    = errors.New("autonomy: invalid policy")
	ErrInvalidSnapshot  = errors.New("autonomy: invalid snapshot")
	ErrInvalidTrigger   = errors.New("autonomy: invalid trigger")
	ErrInvalidRetention = errors.New("autonomy: invalid retention input")
)

type PolicyVersion string

const PolicyVersionV0 PolicyVersion = "autonomy-policy-v0"

type EventType string

const (
	EventUserMessage        EventType = "user_message"
	EventResidentMessage    EventType = "resident_message"
	EventSelfTalk           EventType = "self_talk"
	EventOutboundInitiative EventType = "outbound_initiative"
)

func (value EventType) Validate() error {
	switch value {
	case EventUserMessage, EventResidentMessage, EventSelfTalk, EventOutboundInitiative:
		return nil
	default:
		return fmt.Errorf("%w: unknown event type %q", ErrInvalidSnapshot, value)
	}
}

type TriggerKind string

const (
	TriggerIdle                       TriggerKind = "idle"
	TriggerMetaDependencyStatusChange TriggerKind = "meta_dependency_status_change"
	TriggerSettledDirectConflict      TriggerKind = "settled_direct_conflict"
	TriggerFutureToCurrent            TriggerKind = "future_to_current"
	TriggerVolatileAging              TriggerKind = "volatile_aging"
)

func (kind TriggerKind) Validate() error {
	switch kind {
	case TriggerIdle, TriggerMetaDependencyStatusChange, TriggerSettledDirectConflict,
		TriggerFutureToCurrent, TriggerVolatileAging:
		return nil
	default:
		return fmt.Errorf("%w: unknown trigger kind %q", ErrInvalidTrigger, kind)
	}
}

func (kind TriggerKind) IsSelfTalk() bool {
	return kind == TriggerIdle || kind == TriggerMetaDependencyStatusChange || kind == TriggerSettledDirectConflict
}

func (kind TriggerKind) IsInitiative() bool {
	return kind == TriggerFutureToCurrent || kind == TriggerVolatileAging
}

// Trigger is a durable identity, not an instruction supplied by a provider.
// SourceID identifies the triggering Canonical row. RelatedClaimIDs are
// required only for a settled-direct conflict and are normalized in identity
// order so discovery order cannot change idempotency.
type Trigger struct {
	Kind            TriggerKind
	SourceID        canonical.ID
	RelatedClaimIDs []canonical.ID
	Boundary        canonical.Instant
	Ordinal         int64
}

func (trigger Trigger) Validate() error {
	if err := trigger.Kind.Validate(); err != nil {
		return err
	}
	if err := trigger.SourceID.Validate(); err != nil {
		return fmt.Errorf("%w: source ID: %v", ErrInvalidTrigger, err)
	}
	for _, id := range trigger.RelatedClaimIDs {
		if err := id.Validate(); err != nil {
			return fmt.Errorf("%w: related claim ID: %v", ErrInvalidTrigger, err)
		}
	}
	switch trigger.Kind {
	case TriggerIdle:
		if trigger.Ordinal < 1 || len(trigger.RelatedClaimIDs) != 0 || trigger.Boundary != 0 {
			return fmt.Errorf("%w: idle requires a positive ordinal and only a source user event", ErrInvalidTrigger)
		}
	case TriggerMetaDependencyStatusChange:
		if trigger.Ordinal != 0 || len(trigger.RelatedClaimIDs) != 1 || trigger.Boundary != 0 {
			return fmt.Errorf("%w: dependency change requires exactly one related direct claim", ErrInvalidTrigger)
		}
	case TriggerSettledDirectConflict:
		if trigger.Ordinal != 0 || len(trigger.RelatedClaimIDs) != 2 || trigger.Boundary != 0 ||
			trigger.RelatedClaimIDs[0] == trigger.RelatedClaimIDs[1] {
			return fmt.Errorf("%w: conflict requires two distinct related claims", ErrInvalidTrigger)
		}
	case TriggerFutureToCurrent, TriggerVolatileAging:
		if trigger.Ordinal != 0 || len(trigger.RelatedClaimIDs) != 0 || trigger.Boundary == 0 {
			return fmt.Errorf("%w: temporal initiative requires a non-zero boundary and one source claim", ErrInvalidTrigger)
		}
	}
	return nil
}

func (trigger Trigger) StableIdentity() (string, error) {
	if err := trigger.Validate(); err != nil {
		return "", err
	}
	parts := []string{string(trigger.Kind), trigger.SourceID.String()}
	if trigger.Ordinal != 0 {
		parts = append(parts, strconv.FormatInt(trigger.Ordinal, 10))
	}
	if trigger.Boundary != 0 {
		parts = append(parts, trigger.Boundary.String())
	}
	if len(trigger.RelatedClaimIDs) > 0 {
		claims := append([]canonical.ID(nil), trigger.RelatedClaimIDs...)
		sort.Slice(claims, func(i, j int) bool { return claims[i].String() < claims[j].String() })
		for _, claimID := range claims {
			parts = append(parts, claimID.String())
		}
	}
	return strings.Join(parts, ":"), nil
}

// IdempotencyKey is the single canonical key encoder used by discovery,
// prepare, and Writer revalidation. residentID is validated because keys are
// scoped by the generation_runs resident uniqueness constraint; it is not
// duplicated in the textual key.
func (trigger Trigger) IdempotencyKey(policyVersion string, residentID canonical.ID) (string, error) {
	if err := residentID.Validate(); err != nil {
		return "", fmt.Errorf("%w: resident ID: %v", ErrInvalidTrigger, err)
	}
	if policyVersion != string(PolicyVersionV0) {
		return "", fmt.Errorf("%w: unsupported policy version %q", ErrInvalidTrigger, policyVersion)
	}
	identity, err := trigger.StableIdentity()
	if err != nil {
		return "", err
	}
	prefix := "self_talk:v1:"
	if trigger.Kind.IsInitiative() {
		prefix = "outbound_initiative:v1:"
	}
	return prefix + identity + ":" + policyVersion, nil
}

type BlockingReason string

const (
	BlockingNone                      BlockingReason = ""
	BlockingDisabled                  BlockingReason = "disabled"
	BlockingResidentInactive          BlockingReason = "resident_inactive"
	BlockingMemoryPolicyNotAutonomyV3 BlockingReason = "memory_policy_not_autonomy_v3"
	BlockingProjectionUnavailable     BlockingReason = "projection_unavailable"
	BlockingQuietHours                BlockingReason = "quiet_hours"
	BlockingMinimumInterval           BlockingReason = "minimum_interval"
	BlockingConsecutiveLimit          BlockingReason = "consecutive_limit"
	BlockingHourlyLimit               BlockingReason = "hourly_limit"
	BlockingDailyLimit                BlockingReason = "daily_limit"
	BlockingRecentUserSuppression     BlockingReason = "recent_user_suppression"
	BlockingNoTrigger                 BlockingReason = "no_trigger"
	BlockingClockRegression           BlockingReason = "clock_regression"
	BlockingForegroundPending         BlockingReason = "foreground_pending"
)

type EventPoint struct {
	ID         canonical.ID
	Seq        canonical.Seq
	RecordedAt canonical.Instant
}

func (point EventPoint) Validate() error {
	if err := point.ID.Validate(); err != nil {
		return err
	}
	if err := point.Seq.Validate(); err != nil {
		return err
	}
	return nil
}

type Snapshot struct {
	CapturedHead        canonical.Head
	ResidentID          canonical.ID
	ResidentStatus      string
	MemoryPolicyVersion string
	LastUser            *EventPoint
	LastSelfTalk        *EventPoint
	LastInitiative      *EventPoint
	ConsecutiveSelfTalk int64
	SelfTalkHourCount   int64
	SelfTalkDayCount    int64
	InitiativeHourCount int64
	InitiativeDayCount  int64
	LatestRecordedAt    canonical.Instant
	ProjectionAvailable bool
	ForegroundPending   bool
}

func (snapshot Snapshot) Validate() error {
	if err := snapshot.CapturedHead.Validate(); err != nil {
		return fmt.Errorf("%w: %v", ErrInvalidSnapshot, err)
	}
	if err := snapshot.ResidentID.Validate(); err != nil {
		return fmt.Errorf("%w: resident ID: %v", ErrInvalidSnapshot, err)
	}
	if snapshot.ResidentStatus == "" || snapshot.MemoryPolicyVersion == "" {
		return fmt.Errorf("%w: resident status and memory policy version are required", ErrInvalidSnapshot)
	}
	for _, point := range []*EventPoint{snapshot.LastUser, snapshot.LastSelfTalk, snapshot.LastInitiative} {
		if point != nil {
			if err := point.Validate(); err != nil {
				return fmt.Errorf("%w: event point: %v", ErrInvalidSnapshot, err)
			}
		}
	}
	for _, count := range []int64{
		snapshot.ConsecutiveSelfTalk, snapshot.SelfTalkHourCount, snapshot.SelfTalkDayCount,
		snapshot.InitiativeHourCount, snapshot.InitiativeDayCount,
	} {
		if count < 0 {
			return fmt.Errorf("%w: counters cannot be negative", ErrInvalidSnapshot)
		}
	}
	return nil
}

type EvaluationTime struct {
	WallNow             time.Time
	SinceLastUser       *time.Duration
	SinceLastSelfTalk   *time.Duration
	SinceLastInitiative *time.Duration
	ClockRegressed      bool
}

type ObservedAnchor struct {
	EventID canonical.ID
	At      TimePoint
}

type RuntimeAnchors struct {
	LastUser       *ObservedAnchor
	LastSelfTalk   *ObservedAnchor
	LastInitiative *ObservedAnchor
}

// ResolveEvaluationTime uses process-monotonic elapsed time whenever the
// observed anchor still identifies the Canonical event in the snapshot. After
// restart, anchors are absent and the same calculation safely falls back to
// Canonical recorded_at wall time.
func ResolveEvaluationTime(snapshot Snapshot, now TimePoint, anchors RuntimeAnchors) EvaluationTime {
	result := EvaluationTime{WallNow: now.Wall}
	result.SinceLastUser, result.ClockRegressed = elapsedFor(snapshot.LastUser, anchors.LastUser, now)
	var regressed bool
	result.SinceLastSelfTalk, regressed = elapsedFor(snapshot.LastSelfTalk, anchors.LastSelfTalk, now)
	result.ClockRegressed = result.ClockRegressed || regressed
	result.SinceLastInitiative, regressed = elapsedFor(snapshot.LastInitiative, anchors.LastInitiative, now)
	result.ClockRegressed = result.ClockRegressed || regressed
	if snapshot.LatestRecordedAt != 0 && snapshot.LatestRecordedAt.Time().After(now.Wall) {
		result.ClockRegressed = true
	}
	return result
}

func elapsedFor(point *EventPoint, anchor *ObservedAnchor, now TimePoint) (*time.Duration, bool) {
	if point == nil {
		return nil, false
	}
	if anchor != nil && anchor.EventID == point.ID {
		elapsed := now.Monotonic - anchor.At.Monotonic
		if elapsed < 0 {
			return &elapsed, true
		}
		return &elapsed, false
	}
	elapsed := now.Wall.Sub(point.RecordedAt.Time())
	return &elapsed, elapsed < 0
}

type Decision struct {
	Eligible       bool
	Trigger        Trigger
	BlockingReason BlockingReason
	NextEligibleAt *time.Time
}

type SnapshotRequest struct {
	ResidentID  canonical.ID
	WallNow     time.Time
	Timezone    canonical.Timezone
	MaxAttempts int
}

func (request SnapshotRequest) Validate() error {
	if err := request.ResidentID.Validate(); err != nil {
		return err
	}
	if request.WallNow.IsZero() {
		return fmt.Errorf("%w: wall time is required", ErrInvalidSnapshot)
	}
	if request.MaxAttempts < 1 {
		return fmt.Errorf("%w: max attempts must be positive", ErrInvalidSnapshot)
	}
	return request.Timezone.Validate()
}

type RetentionRequest struct {
	ResidentID canonical.ID
	WallNow    time.Time
}

func (request RetentionRequest) Validate() error {
	if err := request.ResidentID.Validate(); err != nil {
		return err
	}
	if request.WallNow.IsZero() {
		return fmt.Errorf("%w: wall time is required", ErrInvalidRetention)
	}
	return nil
}

// Source is an optional read-only capability. It intentionally exposes
// policy-neutral facts and never a transaction or mutation method.
type Source interface {
	AutonomySnapshot(context.Context, SnapshotRequest) (Snapshot, error)
	RetentionSources(context.Context, RetentionRequest) (RetentionCapture, error)
}
