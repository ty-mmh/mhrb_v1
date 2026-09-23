package autonomy

import (
	"fmt"
	"math"
	"time"
)

func ConsecutiveSelfTalkAfter(current int64, eventType EventType) (int64, error) {
	if current < 0 {
		return 0, fmt.Errorf("%w: negative consecutive count", ErrInvalidSnapshot)
	}
	if err := eventType.Validate(); err != nil {
		return 0, err
	}
	switch eventType {
	case EventUserMessage:
		return 0, nil
	case EventSelfTalk:
		if current == math.MaxInt64 {
			return current, fmt.Errorf("%w: consecutive count overflow", ErrInvalidSnapshot)
		}
		return current + 1, nil
	default:
		return current, nil
	}
}

func DecideSelfTalk(policy Policy, snapshot Snapshot, trigger Trigger, timing EvaluationTime) Decision {
	decision := Decision{Trigger: trigger}
	if !policy.SelfTalk.Enabled {
		decision.BlockingReason = BlockingDisabled
		return decision
	}
	if snapshot.ResidentStatus != "active" {
		decision.BlockingReason = BlockingResidentInactive
		return decision
	}
	if snapshot.MemoryPolicyVersion != MemoryPolicyVersionV3 &&
		snapshot.MemoryPolicyVersion != MemoryPolicyVersionV4 {
		decision.BlockingReason = BlockingMemoryPolicyNotAutonomyV3
		return decision
	}
	if snapshot.ForegroundPending {
		decision.BlockingReason = BlockingForegroundPending
		return decision
	}
	if clockRegressed(snapshot, timing) {
		decision.BlockingReason = BlockingClockRegression
		return decision
	}
	location, err := policy.Location()
	if err != nil {
		decision.BlockingReason = BlockingDisabled
		return decision
	}
	local := timing.WallNow.In(location)
	if policy.SelfTalk.QuietHours.Contains(local) {
		next := policy.SelfTalk.QuietHours.EndAfter(local)
		decision.BlockingReason, decision.NextEligibleAt = BlockingQuietHours, &next
		return decision
	}
	if !trigger.Kind.IsSelfTalk() || trigger.Validate() != nil || snapshot.LastUser == nil {
		decision.BlockingReason = BlockingNoTrigger
		return decision
	}
	anchorElapsed := timing.SinceLastUser
	if snapshot.LastSelfTalk != nil && snapshot.LastSelfTalk.Seq > snapshot.LastUser.Seq {
		anchorElapsed = timing.SinceLastSelfTalk
	}
	if anchorElapsed == nil || *anchorElapsed < 0 {
		decision.BlockingReason = BlockingClockRegression
		return decision
	}
	if *anchorElapsed < policy.SelfTalk.Interval {
		next := timing.WallNow.Add(policy.SelfTalk.Interval - *anchorElapsed)
		decision.BlockingReason, decision.NextEligibleAt = BlockingMinimumInterval, &next
		return decision
	}
	if snapshot.ConsecutiveSelfTalk >= policy.SelfTalk.ConsecutiveLimit {
		decision.BlockingReason = BlockingConsecutiveLimit
		return decision
	}
	if snapshot.SelfTalkHourCount >= policy.SelfTalk.HourlyLimit {
		_, next, _, _ := CalendarBounds(timing.WallNow, location)
		decision.BlockingReason, decision.NextEligibleAt = BlockingHourlyLimit, &next
		return decision
	}
	if snapshot.SelfTalkDayCount >= policy.SelfTalk.DailyLimit {
		_, _, _, next := CalendarBounds(timing.WallNow, location)
		decision.BlockingReason, decision.NextEligibleAt = BlockingDailyLimit, &next
		return decision
	}
	decision.Eligible = true
	return decision
}

func DecideInitiative(policy Policy, snapshot Snapshot, trigger Trigger, timing EvaluationTime) Decision {
	decision := Decision{Trigger: trigger}
	if !policy.Initiative.Enabled {
		decision.BlockingReason = BlockingDisabled
		return decision
	}
	if snapshot.ResidentStatus != "active" {
		decision.BlockingReason = BlockingResidentInactive
		return decision
	}
	if snapshot.ForegroundPending {
		decision.BlockingReason = BlockingForegroundPending
		return decision
	}
	if !snapshot.ProjectionAvailable {
		decision.BlockingReason = BlockingProjectionUnavailable
		return decision
	}
	if !trigger.Kind.IsInitiative() || trigger.Validate() != nil || !policy.InitiativeAllows(trigger.Kind) {
		decision.BlockingReason = BlockingNoTrigger
		return decision
	}
	if clockRegressed(snapshot, timing) {
		decision.BlockingReason = BlockingClockRegression
		return decision
	}
	location, err := policy.Location()
	if err != nil {
		decision.BlockingReason = BlockingDisabled
		return decision
	}
	local := timing.WallNow.In(location)
	if policy.Initiative.QuietHours.Contains(local) {
		next := policy.Initiative.QuietHours.EndAfter(local)
		decision.BlockingReason, decision.NextEligibleAt = BlockingQuietHours, &next
		return decision
	}
	if snapshot.LastUser != nil {
		if timing.SinceLastUser == nil || *timing.SinceLastUser < 0 {
			decision.BlockingReason = BlockingClockRegression
			return decision
		}
		if *timing.SinceLastUser < policy.Initiative.RecentUserSuppression {
			next := timing.WallNow.Add(policy.Initiative.RecentUserSuppression - *timing.SinceLastUser)
			decision.BlockingReason, decision.NextEligibleAt = BlockingRecentUserSuppression, &next
			return decision
		}
	}
	if snapshot.LastInitiative != nil {
		if timing.SinceLastInitiative == nil || *timing.SinceLastInitiative < 0 {
			decision.BlockingReason = BlockingClockRegression
			return decision
		}
		if *timing.SinceLastInitiative < policy.Initiative.MinimumInterval {
			next := timing.WallNow.Add(policy.Initiative.MinimumInterval - *timing.SinceLastInitiative)
			decision.BlockingReason, decision.NextEligibleAt = BlockingMinimumInterval, &next
			return decision
		}
	}
	if snapshot.InitiativeHourCount >= policy.Initiative.HourlyLimit {
		_, next, _, _ := CalendarBounds(timing.WallNow, location)
		decision.BlockingReason, decision.NextEligibleAt = BlockingHourlyLimit, &next
		return decision
	}
	if snapshot.InitiativeDayCount >= policy.Initiative.DailyLimit {
		_, _, _, next := CalendarBounds(timing.WallNow, location)
		decision.BlockingReason, decision.NextEligibleAt = BlockingDailyLimit, &next
		return decision
	}
	decision.Eligible = true
	return decision
}

func clockRegressed(snapshot Snapshot, timing EvaluationTime) bool {
	if timing.ClockRegressed || timing.WallNow.IsZero() {
		return true
	}
	if snapshot.LatestRecordedAt != 0 && snapshot.LatestRecordedAt.Time().After(timing.WallNow) {
		return true
	}
	for _, elapsed := range []*time.Duration{
		timing.SinceLastUser, timing.SinceLastSelfTalk, timing.SinceLastInitiative,
	} {
		if elapsed != nil && *elapsed < 0 {
			return true
		}
	}
	return false
}
