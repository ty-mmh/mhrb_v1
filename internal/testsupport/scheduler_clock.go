package testsupport

import (
	"sync"
	"time"

	"mahoroba.local/mahoroba/internal/autonomy"
)

type ManualSchedulerClock struct {
	mu      sync.RWMutex
	point   autonomy.TimePoint
	tickers map[*manualSchedulerTicker]struct{}
}

func NewManualSchedulerClock(wall time.Time) *ManualSchedulerClock {
	return &ManualSchedulerClock{
		point: autonomy.TimePoint{Wall: wall}, tickers: make(map[*manualSchedulerTicker]struct{}),
	}
}

func (clock *ManualSchedulerClock) Now() autonomy.TimePoint {
	clock.mu.RLock()
	defer clock.mu.RUnlock()
	return clock.point
}

func (clock *ManualSchedulerClock) SetWall(wall time.Time) {
	clock.mu.Lock()
	clock.point.Wall = wall
	clock.mu.Unlock()
}

func (clock *ManualSchedulerClock) AdvanceWall(duration time.Duration) {
	clock.mu.Lock()
	clock.point.Wall = clock.point.Wall.Add(duration)
	clock.mu.Unlock()
}

func (clock *ManualSchedulerClock) AdvanceMonotonic(duration time.Duration) {
	clock.mu.Lock()
	clock.point.Monotonic += duration
	clock.mu.Unlock()
}

func (clock *ManualSchedulerClock) NewTicker(_ time.Duration) autonomy.Ticker {
	ticker := &manualSchedulerTicker{clock: clock, ticks: make(chan struct{}, 1)}
	clock.mu.Lock()
	clock.tickers[ticker] = struct{}{}
	clock.mu.Unlock()
	return ticker
}

func (clock *ManualSchedulerClock) Tick() {
	clock.mu.RLock()
	tickers := make([]*manualSchedulerTicker, 0, len(clock.tickers))
	for ticker := range clock.tickers {
		tickers = append(tickers, ticker)
	}
	clock.mu.RUnlock()
	for _, ticker := range tickers {
		select {
		case ticker.ticks <- struct{}{}:
		default:
		}
	}
}

type manualSchedulerTicker struct {
	clock *ManualSchedulerClock
	ticks chan struct{}
	stop  sync.Once
}

func (ticker *manualSchedulerTicker) C() <-chan struct{} { return ticker.ticks }

func (ticker *manualSchedulerTicker) Stop() {
	ticker.stop.Do(func() {
		ticker.clock.mu.Lock()
		delete(ticker.clock.tickers, ticker)
		ticker.clock.mu.Unlock()
	})
}

var _ autonomy.SchedulerClock = (*ManualSchedulerClock)(nil)
