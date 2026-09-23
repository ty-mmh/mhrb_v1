package autonomy

import (
	"fmt"
	"strconv"
	"strings"
	"time"
)

const (
	MemoryPolicyVersionV3 = "memory-policy-v3"
	MemoryPolicyVersionV4 = "memory-policy-v4"
	MemoryPolicyVersionV5 = "memory-policy-v5"
)

type LocalTime struct {
	minute int
}

func ParseLocalTime(raw string) (LocalTime, error) {
	if len(raw) != 5 || raw[2] != ':' {
		return LocalTime{}, fmt.Errorf("%w: local time %q must use HH:MM", ErrInvalidPolicy, raw)
	}
	hour, hourErr := strconv.Atoi(raw[:2])
	minute, minuteErr := strconv.Atoi(raw[3:])
	if hourErr != nil || minuteErr != nil || hour < 0 || hour > 23 || minute < 0 || minute > 59 {
		return LocalTime{}, fmt.Errorf("%w: invalid local time %q", ErrInvalidPolicy, raw)
	}
	return LocalTime{minute: hour*60 + minute}, nil
}

func MustLocalTime(raw string) LocalTime {
	value, err := ParseLocalTime(raw)
	if err != nil {
		panic(err)
	}
	return value
}

func (value LocalTime) String() string {
	return fmt.Sprintf("%02d:%02d", value.minute/60, value.minute%60)
}

func (value LocalTime) Validate() error {
	if value.minute < 0 || value.minute >= 24*60 {
		return fmt.Errorf("%w: invalid local minute %d", ErrInvalidPolicy, value.minute)
	}
	return nil
}

type QuietHours struct {
	Start LocalTime
	End   LocalTime
}

func (hours QuietHours) Validate() error {
	if err := hours.Start.Validate(); err != nil {
		return err
	}
	if err := hours.End.Validate(); err != nil {
		return err
	}
	if hours.Start.minute == hours.End.minute {
		return fmt.Errorf("%w: quiet-hours start and end must differ", ErrInvalidPolicy)
	}
	return nil
}

func (hours QuietHours) Contains(local time.Time) bool {
	minute := local.Hour()*60 + local.Minute()
	if hours.Start.minute < hours.End.minute {
		return minute >= hours.Start.minute && minute < hours.End.minute
	}
	return minute >= hours.Start.minute || minute < hours.End.minute
}

func (hours QuietHours) EndAfter(local time.Time) time.Time {
	year, month, day := local.Date()
	end := time.Date(year, month, day, hours.End.minute/60, hours.End.minute%60, 0, 0, local.Location())
	if !end.After(local) {
		end = end.AddDate(0, 0, 1)
	}
	return end
}

type SelfTalkPolicy struct {
	Enabled          bool
	Interval         time.Duration
	ConsecutiveLimit int64
	HourlyLimit      int64
	DailyLimit       int64
	QuietHours       QuietHours
}

type InitiativePolicy struct {
	Enabled               bool
	MinimumInterval       time.Duration
	HourlyLimit           int64
	DailyLimit            int64
	RecentUserSuppression time.Duration
	QuietHours            QuietHours
	Triggers              []TriggerKind
}

type RetentionMode string

const (
	RetentionDisabled       RetentionMode = "disabled"
	RetentionCandidateAfter RetentionMode = "candidate_after"
)

type RetentionEligibility string

const RetentionPresentSelfTalk RetentionEligibility = "present_self_talk"

type RetentionPolicy struct {
	Mode         RetentionMode
	Duration     time.Duration
	ScanInterval time.Duration
	Eligibility  RetentionEligibility
}

type Policy struct {
	Version      PolicyVersion
	Timezone     string
	ScanInterval time.Duration
	SelfTalk     SelfTalkPolicy
	Initiative   InitiativePolicy
	Retention    RetentionPolicy
}

func DefaultPolicy(timezone string) Policy {
	return Policy{
		Version: PolicyVersionV0, Timezone: timezone, ScanInterval: 30 * time.Second,
		SelfTalk: SelfTalkPolicy{
			Interval: 30 * time.Minute, ConsecutiveLimit: 10, HourlyLimit: 2, DailyLimit: 12,
			QuietHours: QuietHours{Start: MustLocalTime("23:00"), End: MustLocalTime("07:00")},
		},
		Initiative: InitiativePolicy{
			MinimumInterval: 6 * time.Hour, HourlyLimit: 1, DailyLimit: 2,
			RecentUserSuppression: 30 * time.Minute,
			QuietHours:            QuietHours{Start: MustLocalTime("23:00"), End: MustLocalTime("07:00")},
			Triggers:              []TriggerKind{TriggerFutureToCurrent, TriggerVolatileAging},
		},
		Retention: RetentionPolicy{
			Mode: RetentionDisabled, Duration: 720 * time.Hour, ScanInterval: 24 * time.Hour,
			Eligibility: RetentionPresentSelfTalk,
		},
	}
}

func (policy Policy) Validate() error {
	if policy.Version != PolicyVersionV0 {
		return fmt.Errorf("%w: unsupported version %q", ErrInvalidPolicy, policy.Version)
	}
	if _, err := time.LoadLocation(policy.Timezone); err != nil || policy.Timezone == "Local" || strings.TrimSpace(policy.Timezone) != policy.Timezone {
		return fmt.Errorf("%w: invalid timezone %q", ErrInvalidPolicy, policy.Timezone)
	}
	if policy.ScanInterval <= 0 {
		return fmt.Errorf("%w: scan interval must be positive", ErrInvalidPolicy)
	}
	if policy.SelfTalk.Interval <= 0 || policy.SelfTalk.ConsecutiveLimit <= 0 ||
		policy.SelfTalk.HourlyLimit <= 0 || policy.SelfTalk.DailyLimit <= 0 {
		return fmt.Errorf("%w: self-talk durations and limits must be positive", ErrInvalidPolicy)
	}
	if err := policy.SelfTalk.QuietHours.Validate(); err != nil {
		return err
	}
	if policy.Initiative.MinimumInterval <= 0 || policy.Initiative.HourlyLimit <= 0 ||
		policy.Initiative.DailyLimit <= 0 || policy.Initiative.RecentUserSuppression <= 0 {
		return fmt.Errorf("%w: initiative durations and limits must be positive", ErrInvalidPolicy)
	}
	if err := policy.Initiative.QuietHours.Validate(); err != nil {
		return err
	}
	if len(policy.Initiative.Triggers) == 0 {
		return fmt.Errorf("%w: initiative requires at least one trigger", ErrInvalidPolicy)
	}
	seen := make(map[TriggerKind]struct{}, len(policy.Initiative.Triggers))
	for _, trigger := range policy.Initiative.Triggers {
		if !trigger.IsInitiative() {
			return fmt.Errorf("%w: unsupported initiative trigger %q", ErrInvalidPolicy, trigger)
		}
		if _, duplicate := seen[trigger]; duplicate {
			return fmt.Errorf("%w: duplicate initiative trigger %q", ErrInvalidPolicy, trigger)
		}
		seen[trigger] = struct{}{}
	}
	if policy.Retention.Mode != RetentionDisabled && policy.Retention.Mode != RetentionCandidateAfter {
		return fmt.Errorf("%w: unsupported retention mode %q", ErrInvalidPolicy, policy.Retention.Mode)
	}
	if policy.Retention.Duration <= 0 || policy.Retention.ScanInterval <= 0 {
		return fmt.Errorf("%w: retention durations must be positive", ErrInvalidPolicy)
	}
	if policy.Retention.Eligibility != RetentionPresentSelfTalk {
		return fmt.Errorf("%w: unsupported retention eligibility %q", ErrInvalidPolicy, policy.Retention.Eligibility)
	}
	return nil
}

func (policy Policy) Location() (*time.Location, error) {
	if err := policy.Validate(); err != nil {
		return nil, err
	}
	return time.LoadLocation(policy.Timezone)
}

func (policy Policy) InitiativeAllows(kind TriggerKind) bool {
	for _, configured := range policy.Initiative.Triggers {
		if configured == kind {
			return true
		}
	}
	return false
}

func CalendarBounds(now time.Time, location *time.Location) (hourStart, hourEnd, dayStart, dayEnd time.Time) {
	local := now.In(location)
	year, month, day := local.Date()
	hourStart = time.Date(year, month, day, local.Hour(), 0, 0, 0, location)
	hourEnd = hourStart.Add(time.Hour)
	dayStart = time.Date(year, month, day, 0, 0, 0, 0, location)
	dayEnd = dayStart.AddDate(0, 0, 1)
	return
}
