package sqlite

import (
	"context"
	"sync"
)

// writePriorityGate serializes write transactions before they reach SQLite.
// Canonical and Operational callers use acquireHigh; Projection callers use
// acquireProjection. Once a high-priority caller is waiting, no new Projection
// transaction may start. An already-running transaction is never preempted.
type writePriorityGate struct {
	mu          sync.Mutex
	active      bool
	highWaiting int
	changed     chan struct{}
}

func newWritePriorityGate() *writePriorityGate {
	return &writePriorityGate{changed: make(chan struct{})}
}

func (gate *writePriorityGate) acquireHigh(ctx context.Context) (func(), error) {
	if ctx == nil {
		ctx = context.Background()
	}
	gate.mu.Lock()
	gate.highWaiting++
	for gate.active {
		changed := gate.changed
		gate.mu.Unlock()
		select {
		case <-ctx.Done():
			gate.mu.Lock()
			gate.highWaiting--
			gate.signalLocked()
			gate.mu.Unlock()
			return nil, ctx.Err()
		case <-changed:
		}
		gate.mu.Lock()
	}
	gate.highWaiting--
	gate.active = true
	gate.mu.Unlock()
	return gate.releaseFunc(), nil
}

func (gate *writePriorityGate) acquireProjection(ctx context.Context) (func(), error) {
	if ctx == nil {
		ctx = context.Background()
	}
	gate.mu.Lock()
	for gate.active || gate.highWaiting > 0 {
		changed := gate.changed
		gate.mu.Unlock()
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-changed:
		}
		gate.mu.Lock()
	}
	gate.active = true
	gate.mu.Unlock()
	return gate.releaseFunc(), nil
}

func (gate *writePriorityGate) releaseFunc() func() {
	var once sync.Once
	return func() {
		once.Do(func() {
			gate.mu.Lock()
			gate.active = false
			gate.signalLocked()
			gate.mu.Unlock()
		})
	}
}

func (gate *writePriorityGate) signalLocked() {
	close(gate.changed)
	gate.changed = make(chan struct{})
}
