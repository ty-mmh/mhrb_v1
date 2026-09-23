package canonical

import (
	"context"
	"sync"
)

type activityState uint8

const (
	activityAccepted activityState = iota + 1
	activityQueued
	activityInFlight
)

// ActivitySnapshot is a point-in-time count for one resident. Done commands
// are removed and never retained as authority.
type ActivitySnapshot struct {
	Accepted int
	Queued   int
	InFlight int
}

func (value ActivitySnapshot) Total() int {
	return value.Accepted + value.Queued + value.InFlight
}

// ActivityToken is Writer-generated opaque authority. Its fields are private
// so plans, CLI input, and persistence rows cannot manufacture an exclusion.
type ActivityToken struct {
	registry   *activityRegistry
	sequence   uint64
	residentID ID
}

func (token ActivityToken) Valid() bool {
	return token.registry != nil && token.sequence != 0 && token.residentID.Validate() == nil
}

func (token ActivityToken) ResidentID() (ID, bool) {
	if !token.Valid() {
		return ID{}, false
	}
	return token.residentID, true
}

// residentAdmissionFenceCommand is deliberately package-private. Commands in
// other packages opt in through the identical method, while callers cannot
// pass a token or fence owner through command data.
type residentAdmissionFenceCommand interface {
	RequiresResidentAdmissionFence() bool
}

type activityEntry struct {
	residentID ID
	state      activityState
	fence      bool
	contended  bool
}

type activityRegistry struct {
	mu      sync.Mutex
	next    uint64
	entries map[uint64]activityEntry
	fences  map[ID]uint64
}

func newActivityRegistry() *activityRegistry {
	return &activityRegistry{entries: make(map[uint64]activityEntry), fences: make(map[ID]uint64)}
}

func (registry *activityRegistry) admit(scope Scope, fence bool) (ActivityToken, error) {
	residentID, scoped := scope.ResidentID()
	if !scoped {
		return ActivityToken{}, nil
	}
	registry.mu.Lock()
	defer registry.mu.Unlock()
	if _, closed := registry.fences[residentID]; closed {
		return ActivityToken{}, ErrResidentAdmissionClosed
	}
	registry.next++
	if registry.next == 0 {
		registry.next++
	}
	sequence := registry.next
	contended := false
	if fence {
		for _, entry := range registry.entries {
			if entry.residentID == residentID {
				contended = true
				break
			}
		}
	}
	registry.entries[sequence] = activityEntry{residentID: residentID, state: activityAccepted, fence: fence, contended: contended}
	if fence {
		registry.fences[residentID] = sequence
	}
	return ActivityToken{registry: registry, sequence: sequence, residentID: residentID}, nil
}

func (registry *activityRegistry) transition(token ActivityToken, state activityState) error {
	if !token.Valid() || token.registry != registry {
		return ErrInvalidActivityToken
	}
	registry.mu.Lock()
	defer registry.mu.Unlock()
	entry, exists := registry.entries[token.sequence]
	if !exists || entry.residentID != token.residentID {
		return ErrInvalidActivityToken
	}
	valid := entry.state == activityAccepted && state == activityQueued ||
		entry.state == activityQueued && state == activityInFlight
	if !valid {
		return ErrInvalidActivityToken
	}
	entry.state = state
	registry.entries[token.sequence] = entry
	return nil
}

func (registry *activityRegistry) finish(token ActivityToken) {
	if !token.Valid() || token.registry != registry {
		return
	}
	registry.mu.Lock()
	defer registry.mu.Unlock()
	entry, exists := registry.entries[token.sequence]
	if !exists || entry.residentID != token.residentID {
		return
	}
	delete(registry.entries, token.sequence)
	if entry.fence && registry.fences[entry.residentID] == token.sequence {
		delete(registry.fences, entry.residentID)
	}
}

func (registry *activityRegistry) snapshot(residentID ID, exclude uint64) ActivitySnapshot {
	registry.mu.Lock()
	defer registry.mu.Unlock()
	var result ActivitySnapshot
	for sequence, entry := range registry.entries {
		if sequence == exclude || entry.residentID != residentID {
			continue
		}
		switch entry.state {
		case activityAccepted:
			result.Accepted++
		case activityQueued:
			result.Queued++
		case activityInFlight:
			result.InFlight++
		}
	}
	return result
}

func (registry *activityRegistry) requireExclusive(token ActivityToken, residentID ID) error {
	if !token.Valid() || token.registry != registry || token.residentID != residentID {
		return ErrInvalidActivityToken
	}
	registry.mu.Lock()
	defer registry.mu.Unlock()
	entry, exists := registry.entries[token.sequence]
	if !exists || entry.residentID != residentID || entry.state != activityInFlight || !entry.fence ||
		registry.fences[residentID] != token.sequence {
		return ErrInvalidActivityToken
	}
	if entry.contended {
		return ErrResidentActivityBusy
	}
	for sequence, other := range registry.entries {
		if sequence != token.sequence && other.residentID == residentID {
			return ErrResidentActivityBusy
		}
	}
	return nil
}

type activityContextKey struct{}

func contextWithActivityToken(ctx context.Context, token ActivityToken) context.Context {
	if !token.Valid() {
		return ctx
	}
	return context.WithValue(ctx, activityContextKey{}, token)
}

// ActivityTokenFromContext returns only Writer-injected authority.
func ActivityTokenFromContext(ctx context.Context) (ActivityToken, bool) {
	if ctx == nil {
		return ActivityToken{}, false
	}
	token, ok := ctx.Value(activityContextKey{}).(ActivityToken)
	return token, ok && token.Valid()
}

// RequireCurrentResidentFence is the final in-UoW exclusion check used by
// Resident Erase. It permits exactly the current in-flight fence owner.
func RequireCurrentResidentFence(ctx context.Context, residentID ID) error {
	token, ok := ActivityTokenFromContext(ctx)
	if !ok {
		return ErrInvalidActivityToken
	}
	return token.registry.requireExclusive(token, residentID)
}
