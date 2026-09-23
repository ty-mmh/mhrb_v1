package testsupport

import (
	"testing"
	"time"
)

func TestM6ManualSchedulerClockControlsWallMonotonicAndTickIndependently(t *testing.T) {
	start := time.Date(2026, 8, 21, 12, 0, 0, 0, time.UTC)
	clock := NewManualSchedulerClock(start)
	ticker := clock.NewTicker(time.Minute)
	defer ticker.Stop()

	clock.AdvanceWall(-time.Hour)
	clock.AdvanceMonotonic(30 * time.Minute)
	point := clock.Now()
	if !point.Wall.Equal(start.Add(-time.Hour)) || point.Monotonic != 30*time.Minute {
		t.Fatalf("point=%#v", point)
	}
	select {
	case <-ticker.C():
		t.Fatal("advancing clocks emitted a tick")
	default:
	}
	clock.Tick()
	select {
	case <-ticker.C():
	default:
		t.Fatal("explicit tick was not emitted")
	}
	ticker.Stop()
	clock.Tick()
	select {
	case <-ticker.C():
		t.Fatal("stopped ticker received a tick")
	default:
	}
}
