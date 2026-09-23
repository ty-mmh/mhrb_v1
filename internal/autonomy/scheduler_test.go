package autonomy

import (
	"sync"
	"testing"
	"time"

	"mahoroba.local/mahoroba/internal/canonical"
)

func TestM6SelfTalkDecisionAtExactIntervalBoundary(t *testing.T) {
	policy := DefaultPolicy("UTC")
	policy.SelfTalk.Enabled = true
	snapshot := testSnapshot(t)
	elapsed := policy.SelfTalk.Interval
	decision := DecideSelfTalk(policy, snapshot, idleTrigger(t, 1), EvaluationTime{
		WallNow: time.Date(2026, 8, 21, 12, 0, 0, 0, time.UTC), SinceLastUser: &elapsed,
	})
	if !decision.Eligible || decision.BlockingReason != BlockingNone {
		t.Fatalf("decision = %#v", decision)
	}
}

func TestCOV6SelfTalkSchedulerAcceptsMemoryPolicyV4(t *testing.T) {
	policy := DefaultPolicy("UTC")
	policy.SelfTalk.Enabled = true
	snapshot := testSnapshot(t)
	snapshot.MemoryPolicyVersion = MemoryPolicyVersionV4
	elapsed := policy.SelfTalk.Interval
	decision := DecideSelfTalk(policy, snapshot, idleTrigger(t, 1), EvaluationTime{
		WallNow: time.Date(2026, 8, 21, 12, 0, 0, 0, time.UTC), SinceLastUser: &elapsed,
	})
	if !decision.Eligible || decision.BlockingReason != BlockingNone {
		t.Fatalf("memory-policy-v4 decision = %#v", decision)
	}
}

func TestM6RTI22SelfTalkCannotExtendItsOwnActivityWindow(t *testing.T) {
	policy := DefaultPolicy("UTC")
	policy.SelfTalk.Enabled = true
	snapshot := testSnapshot(t)
	snapshot.ConsecutiveSelfTalk = policy.SelfTalk.ConsecutiveLimit
	selfTalk := EventPoint{ID: testID(t, "01K00000000000000000000002"), Seq: 2, RecordedAt: canonical.InstantFromTime(time.Date(2026, 8, 21, 11, 0, 0, 0, time.UTC))}
	snapshot.LastSelfTalk = &selfTalk
	elapsed := policy.SelfTalk.Interval
	decision := DecideSelfTalk(policy, snapshot, idleTrigger(t, 11), EvaluationTime{
		WallNow:       time.Date(2026, 8, 21, 12, 0, 0, 0, time.UTC),
		SinceLastUser: &elapsed, SinceLastSelfTalk: &elapsed,
	})
	if decision.Eligible || decision.BlockingReason != BlockingConsecutiveLimit {
		t.Fatalf("decision = %#v", decision)
	}
}

func TestM6RTI23OnlyUserMessageResetsConsecutiveSelfTalkCount(t *testing.T) {
	count := int64(4)
	for _, eventType := range []EventType{EventResidentMessage, EventOutboundInitiative} {
		got, err := ConsecutiveSelfTalkAfter(count, eventType)
		if err != nil || got != count {
			t.Fatalf("%s changed count to %d, err=%v", eventType, got, err)
		}
	}
	got, err := ConsecutiveSelfTalkAfter(count, EventSelfTalk)
	if err != nil || got != count+1 {
		t.Fatalf("self-talk count=%d err=%v", got, err)
	}
	got, err = ConsecutiveSelfTalkAfter(got, EventUserMessage)
	if err != nil || got != 0 {
		t.Fatalf("user reset count=%d err=%v", got, err)
	}
}

func TestM6SelfTalkClockRegressionFailsClosed(t *testing.T) {
	policy := DefaultPolicy("UTC")
	policy.SelfTalk.Enabled = true
	snapshot := testSnapshot(t)
	negative := -time.Second
	decision := DecideSelfTalk(policy, snapshot, idleTrigger(t, 1), EvaluationTime{
		WallNow: time.Date(2026, 8, 21, 12, 0, 0, 0, time.UTC), SinceLastUser: &negative,
	})
	if decision.BlockingReason != BlockingClockRegression {
		t.Fatalf("reason=%q", decision.BlockingReason)
	}
}

func TestM6InitiativeRequiresConfiguredExplicitTrigger(t *testing.T) {
	policy := DefaultPolicy("UTC")
	policy.Initiative.Enabled = true
	snapshot := testSnapshot(t)
	snapshot.ProjectionAvailable = true
	decision := DecideInitiative(policy, snapshot, Trigger{}, EvaluationTime{WallNow: time.Date(2026, 8, 21, 12, 0, 0, 0, time.UTC)})
	if decision.Eligible || decision.BlockingReason != BlockingNoTrigger {
		t.Fatalf("decision = %#v", decision)
	}
}

func TestM6TriggerIdempotencyKeysHaveOneCanonicalEncoding(t *testing.T) {
	residentID := testID(t, "01K00000000000000000000000")
	idle := idleTrigger(t, 3)
	key, err := idle.IdempotencyKey(string(PolicyVersionV0), residentID)
	if err != nil {
		t.Fatal(err)
	}
	if want := "self_talk:v1:idle:01K00000000000000000000001:3:autonomy-policy-v0"; key != want {
		t.Fatalf("key=%q, want %q", key, want)
	}
	initiative := Trigger{
		Kind: TriggerFutureToCurrent, SourceID: testID(t, "01K00000000000000000000003"),
		Boundary: canonical.InstantFromTime(time.Date(2026, 9, 1, 0, 0, 0, 0, time.UTC)),
	}
	key, err = initiative.IdempotencyKey(string(PolicyVersionV0), residentID)
	if err != nil {
		t.Fatal(err)
	}
	if want := "outbound_initiative:v1:future_to_current:01K00000000000000000000003:1788220800000000:autonomy-policy-v0"; key != want {
		t.Fatalf("key=%q, want %q", key, want)
	}
}

func TestM6SelfTalkLimitsBlockAtExactHourlyAndDailyBoundaries(t *testing.T) {
	policy := DefaultPolicy("Asia/Tokyo")
	policy.SelfTalk.Enabled = true
	location, err := policy.Location()
	if err != nil {
		t.Fatal(err)
	}
	wallNow := time.Date(2026, 8, 21, 21, 34, 56, 0, location)
	elapsed := policy.SelfTalk.Interval
	tests := []struct {
		name       string
		mutate     func(*Snapshot)
		wantReason BlockingReason
		wantNext   time.Time
	}{
		{
			name: "hourly exact limit",
			mutate: func(snapshot *Snapshot) {
				snapshot.SelfTalkHourCount = policy.SelfTalk.HourlyLimit
			},
			wantReason: BlockingHourlyLimit,
			wantNext:   time.Date(2026, 8, 21, 22, 0, 0, 0, location),
		},
		{
			name: "daily exact limit",
			mutate: func(snapshot *Snapshot) {
				snapshot.SelfTalkDayCount = policy.SelfTalk.DailyLimit
			},
			wantReason: BlockingDailyLimit,
			wantNext:   time.Date(2026, 8, 22, 0, 0, 0, 0, location),
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			snapshot := testSnapshot(t)
			test.mutate(&snapshot)
			decision := DecideSelfTalk(policy, snapshot, idleTrigger(t, 1), EvaluationTime{
				WallNow: wallNow, SinceLastUser: &elapsed,
			})
			if decision.Eligible || decision.BlockingReason != test.wantReason || decision.NextEligibleAt == nil ||
				!decision.NextEligibleAt.Equal(test.wantNext) {
				t.Fatalf("decision=%#v, want reason=%q next=%s", decision, test.wantReason, test.wantNext)
			}
		})
	}
}

func TestM6InitiativeRecentUserAndMinimumIntervalsBlockUntilExactBoundary(t *testing.T) {
	policy := DefaultPolicy("UTC")
	policy.Initiative.Enabled = true
	wallNow := time.Date(2026, 8, 21, 12, 0, 0, 0, time.UTC)
	trigger := Trigger{
		Kind: TriggerFutureToCurrent, SourceID: testID(t, "01K00000000000000000000003"),
		Boundary: canonical.InstantFromTime(wallNow),
	}
	tests := []struct {
		name       string
		sinceUser  time.Duration
		sinceInit  *time.Duration
		wantReason BlockingReason
		wantNext   time.Time
	}{
		{
			name: "recent user suppression", sinceUser: policy.Initiative.RecentUserSuppression - time.Microsecond,
			wantReason: BlockingRecentUserSuppression, wantNext: wallNow.Add(time.Microsecond),
		},
		{
			name: "minimum initiative interval", sinceUser: policy.Initiative.RecentUserSuppression,
			sinceInit:  durationPointer(policy.Initiative.MinimumInterval - time.Microsecond),
			wantReason: BlockingMinimumInterval, wantNext: wallNow.Add(time.Microsecond),
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			snapshot := testSnapshot(t)
			snapshot.ProjectionAvailable = true
			if test.sinceInit != nil {
				point := EventPoint{
					ID: testID(t, "01K00000000000000000000004"), Seq: 2,
					RecordedAt: canonical.InstantFromTime(wallNow.Add(-*test.sinceInit)),
				}
				snapshot.LastInitiative = &point
			}
			decision := DecideInitiative(policy, snapshot, trigger, EvaluationTime{
				WallNow: wallNow, SinceLastUser: &test.sinceUser, SinceLastInitiative: test.sinceInit,
			})
			if decision.Eligible || decision.BlockingReason != test.wantReason || decision.NextEligibleAt == nil ||
				!decision.NextEligibleAt.Equal(test.wantNext) {
				t.Fatalf("decision=%#v, want reason=%q next=%s", decision, test.wantReason, test.wantNext)
			}
		})
	}
}

func TestM6ConcurrentScansDeriveOneIdempotencyKey(t *testing.T) {
	residentID := testID(t, "01K00000000000000000000000")
	trigger := idleTrigger(t, 3)
	const scans = 64
	keys := make(chan string, scans)
	errorsFound := make(chan error, scans)
	var group sync.WaitGroup
	for range scans {
		group.Add(1)
		go func() {
			defer group.Done()
			key, err := trigger.IdempotencyKey(string(PolicyVersionV0), residentID)
			if err != nil {
				errorsFound <- err
				return
			}
			keys <- key
		}()
	}
	group.Wait()
	close(keys)
	close(errorsFound)
	for err := range errorsFound {
		t.Fatal(err)
	}
	unique := make(map[string]int)
	for key := range keys {
		unique[key]++
	}
	if len(unique) != 1 {
		t.Fatalf("concurrent scans derived %d keys: %v", len(unique), unique)
	}
	for _, count := range unique {
		if count != scans {
			t.Fatalf("idempotency key count=%d, want %d", count, scans)
		}
	}
}

func TestM6EvaluationTimeUsesMonotonicAnchorAndRestartWallFallback(t *testing.T) {
	snapshot := testSnapshot(t)
	observed := TimePoint{Wall: snapshot.LastUser.RecordedAt.Time(), Monotonic: 5 * time.Minute}
	now := TimePoint{Wall: observed.Wall.Add(10 * time.Hour), Monotonic: 15 * time.Minute}
	resolved := ResolveEvaluationTime(snapshot, now, RuntimeAnchors{
		LastUser: &ObservedAnchor{EventID: snapshot.LastUser.ID, At: observed},
	})
	if resolved.SinceLastUser == nil || *resolved.SinceLastUser != 10*time.Minute {
		t.Fatalf("in-process elapsed=%v", resolved.SinceLastUser)
	}
	restarted := ResolveEvaluationTime(snapshot, now, RuntimeAnchors{})
	if restarted.SinceLastUser == nil || *restarted.SinceLastUser != 10*time.Hour {
		t.Fatalf("restart wall elapsed=%v", restarted.SinceLastUser)
	}
}

func testSnapshot(t *testing.T) Snapshot {
	t.Helper()
	point := EventPoint{
		ID: testID(t, "01K00000000000000000000001"), Seq: 1,
		RecordedAt: canonical.InstantFromTime(time.Date(2026, 8, 21, 10, 0, 0, 0, time.UTC)),
	}
	return Snapshot{
		ResidentID: testID(t, "01K00000000000000000000000"), ResidentStatus: "active",
		MemoryPolicyVersion: MemoryPolicyVersionV3, LastUser: &point, LatestRecordedAt: point.RecordedAt,
	}
}

func idleTrigger(t *testing.T, ordinal int64) Trigger {
	t.Helper()
	return Trigger{Kind: TriggerIdle, SourceID: testID(t, "01K00000000000000000000001"), Ordinal: ordinal}
}

func testID(t *testing.T, raw string) canonical.ID {
	t.Helper()
	id, err := canonical.ParseID(raw)
	if err != nil {
		t.Fatal(err)
	}
	return id
}

func durationPointer(value time.Duration) *time.Duration { return &value }
