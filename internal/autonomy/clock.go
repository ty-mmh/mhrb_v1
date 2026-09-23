package autonomy

import (
	"sync"
	"time"
)

type TimePoint struct {
	Wall      time.Time
	Monotonic time.Duration
}

type Ticker interface {
	C() <-chan struct{}
	Stop()
}

type SchedulerClock interface {
	Now() TimePoint
	NewTicker(time.Duration) Ticker
}

type SystemSchedulerClock struct {
	origin time.Time
}

func NewSystemSchedulerClock() *SystemSchedulerClock {
	return &SystemSchedulerClock{origin: time.Now()}
}

func (clock *SystemSchedulerClock) Now() TimePoint {
	now := time.Now()
	return TimePoint{Wall: now, Monotonic: now.Sub(clock.origin)}
}

func (*SystemSchedulerClock) NewTicker(interval time.Duration) Ticker {
	ticker := time.NewTicker(interval)
	result := &systemTicker{ticker: ticker, ticks: make(chan struct{}, 1), done: make(chan struct{})}
	go result.forward()
	return result
}

type systemTicker struct {
	ticker *time.Ticker
	ticks  chan struct{}
	done   chan struct{}
	stop   sync.Once
}

func (ticker *systemTicker) C() <-chan struct{} { return ticker.ticks }

func (ticker *systemTicker) Stop() {
	ticker.stop.Do(func() {
		close(ticker.done)
		ticker.ticker.Stop()
	})
}

func (ticker *systemTicker) forward() {
	for {
		select {
		case <-ticker.done:
			return
		case <-ticker.ticker.C:
			select {
			case ticker.ticks <- struct{}{}:
			default:
			}
		}
	}
}
