// Package runtimegate provides the one-shot activation barrier shared by the
// HTTP accept loop and long-lived background workers.
package runtimegate

import (
	"context"
	"errors"
	"sync"
)

var (
	ErrAlreadyResolved = errors.New("runtime gate: already resolved")
	ErrClosed          = errors.New("runtime gate: closed before activation")
)

// Gate is born closed and resolves exactly once to open or failed. A failed
// gate can never be reopened.
type Gate struct {
	once sync.Once
	done chan struct{}
	mu   sync.RWMutex
	err  error
	open bool
}

func New() *Gate { return &Gate{done: make(chan struct{})} }

func (gate *Gate) Open() error { return gate.resolve(nil, true) }

func (gate *Gate) Fail(err error) error {
	if err == nil {
		err = ErrClosed
	}
	return gate.resolve(err, false)
}

func (gate *Gate) resolve(err error, open bool) error {
	if gate == nil {
		return ErrClosed
	}
	resolved := false
	gate.once.Do(func() {
		gate.mu.Lock()
		gate.err = err
		gate.open = open
		gate.mu.Unlock()
		close(gate.done)
		resolved = true
	})
	if !resolved {
		return ErrAlreadyResolved
	}
	return nil
}

func (gate *Gate) Wait(ctx context.Context) error {
	if gate == nil {
		return ErrClosed
	}
	if ctx == nil {
		return errors.New("runtime gate: nil context")
	}
	select {
	case <-gate.done:
		gate.mu.RLock()
		defer gate.mu.RUnlock()
		if gate.open {
			return nil
		}
		return gate.err
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (gate *Gate) IsOpen() bool {
	if gate == nil {
		return false
	}
	gate.mu.RLock()
	defer gate.mu.RUnlock()
	return gate.open
}
