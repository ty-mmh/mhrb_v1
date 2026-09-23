package autonomy

import (
	"testing"
	"time"
)

func TestM6AutonomyDefaultsAreDisabledAndFixed(t *testing.T) {
	policy := DefaultPolicy("Asia/Tokyo")
	if err := policy.Validate(); err != nil {
		t.Fatal(err)
	}
	if policy.SelfTalk.Enabled || policy.Initiative.Enabled || policy.Retention.Mode != RetentionDisabled {
		t.Fatalf("optional defaults are not disabled: %#v", policy)
	}
	if policy.ScanInterval != 30*time.Second || policy.SelfTalk.Interval != 30*time.Minute ||
		policy.SelfTalk.ConsecutiveLimit != 10 || policy.SelfTalk.HourlyLimit != 2 || policy.SelfTalk.DailyLimit != 12 {
		t.Fatalf("unexpected self-talk defaults: %#v", policy.SelfTalk)
	}
	if policy.Initiative.MinimumInterval != 6*time.Hour || policy.Initiative.HourlyLimit != 1 ||
		policy.Initiative.DailyLimit != 2 || policy.Initiative.RecentUserSuppression != 30*time.Minute {
		t.Fatalf("unexpected initiative defaults: %#v", policy.Initiative)
	}
	if policy.Retention.Duration != 720*time.Hour || policy.Retention.ScanInterval != 24*time.Hour {
		t.Fatalf("unexpected retention defaults: %#v", policy.Retention)
	}
}

func TestM6SelfTalkQuietHoursAreStartInclusiveEndExclusive(t *testing.T) {
	hours := QuietHours{Start: MustLocalTime("23:00"), End: MustLocalTime("07:00")}
	location, err := time.LoadLocation("Asia/Tokyo")
	if err != nil {
		t.Fatal(err)
	}
	for _, test := range []struct {
		name string
		hour int
		want bool
	}{
		{name: "before", hour: 22},
		{name: "start inclusive", hour: 23, want: true},
		{name: "overnight", hour: 3, want: true},
		{name: "end exclusive", hour: 7},
	} {
		t.Run(test.name, func(t *testing.T) {
			at := time.Date(2026, 8, 21, test.hour, 0, 0, 0, location)
			if got := hours.Contains(at); got != test.want {
				t.Fatalf("Contains(%s)=%v, want %v", at, got, test.want)
			}
		})
	}
}

func TestM6AutonomyPolicyRejectsInvalidValues(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*Policy)
	}{
		{name: "version", mutate: func(p *Policy) { p.Version = "v-next" }},
		{name: "scan", mutate: func(p *Policy) { p.ScanInterval = 0 }},
		{name: "self interval", mutate: func(p *Policy) { p.SelfTalk.Interval = -1 }},
		{name: "same quiet boundary", mutate: func(p *Policy) { p.SelfTalk.QuietHours.End = p.SelfTalk.QuietHours.Start }},
		{name: "duplicate trigger", mutate: func(p *Policy) { p.Initiative.Triggers = append(p.Initiative.Triggers, TriggerFutureToCurrent) }},
		{name: "self trigger in initiative", mutate: func(p *Policy) { p.Initiative.Triggers = []TriggerKind{TriggerIdle} }},
		{name: "retention mode", mutate: func(p *Policy) { p.Retention.Mode = "delete" }},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			policy := DefaultPolicy("UTC")
			test.mutate(&policy)
			if err := policy.Validate(); err == nil {
				t.Fatal("invalid policy was accepted")
			}
		})
	}
}
