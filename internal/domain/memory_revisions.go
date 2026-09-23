package domain

import (
	"context"

	"mahoroba.local/mahoroba/internal/canonical"
)

// ActiveRevision is the immutable revision selected by an activation, not by
// revision creation order. M5 uses this value for captured-head, event-time,
// and explicit as-of policy decisions.
type ActiveRevision struct {
	RevisionID       canonical.ID
	ActivationID     canonical.ID
	ActivationCommit canonical.CommitSeq
	ActivatedAt      canonical.Instant
	Content          string
	ContentErased    bool
}

// RevisionRepository is deliberately separate from Repository so the M4
// dialogue mocks remain source-compatible while M5 callers opt into strict
// activation-aware resolution.
type RevisionRepository interface {
	ActiveRevisionAtHead(context.Context, canonical.ID, string, canonical.CommitSeq) (ActiveRevision, error)
	ActiveRevisionAsOf(context.Context, canonical.ID, string, canonical.CommitSeq, canonical.Instant) (ActiveRevision, error)
	ActiveRevisionForEvent(context.Context, canonical.ID, string, canonical.ID) (ActiveRevision, error)
}
