// Package integrity defines read-only checks that must succeed before a
// Canonical writer, service listener, or artifact publisher is allowed to
// proceed.
package integrity

import (
	"context"
	"errors"
	"fmt"
	"slices"
)

// ErrFatal identifies a Canonical invariant violation. Fatal violations must
// never be converted into integrity findings because writing a finding would
// append a commit to a ledger that has already failed its minimum contract.
var ErrFatal = errors.New("integrity: fatal canonical invariant violation")

// FatalCode is safe for diagnostics and deliberately contains no content or
// digest material.
type FatalCode string

const (
	// ClaimStatementAliasGroupInvalid covers the bidirectional present/erased
	// statement-content alias invariant.
	ClaimStatementAliasGroupInvalid FatalCode = "claim_statement_alias_group_invalid"
	// CanonicalCommitSequenceInvalid covers a missing, duplicated, or
	// non-monotonic Canonical commit cursor. A Writer must never be opened on
	// such a history because it cannot allocate an unambiguous successor.
	CanonicalCommitSequenceInvalid FatalCode = "canonical_commit_sequence_invalid"
	// ContentErasureInvariantInvalid covers the bidirectional relation between
	// content erasure state and its exactly-one erasure event.
	ContentErasureInvariantInvalid FatalCode = "content_erasure_invariant_invalid"
	// PresentClaimIdentityInvalid covers a present claim whose raw blob or
	// normalized statement identity cannot be reproduced.
	PresentClaimIdentityInvalid FatalCode = "present_claim_identity_invalid"
	// GenerationOutcomeHistoryInvalid covers malformed full run histories,
	// including zero outcomes, gaps, duplicates, and illegal successors.
	GenerationOutcomeHistoryInvalid FatalCode = "generation_outcome_history_invalid"
	// RuntimeConfigurationInvalid covers a non-NULL runtime selection whose
	// referenced resident or exact sessionization-policy contract cannot be
	// resolved. An intentionally NULL selection remains a readiness condition,
	// not Canonical corruption.
	RuntimeConfigurationInvalid FatalCode = "runtime_configuration_invalid"
)

// FatalError reports one stable, non-sensitive integrity failure.
type FatalError struct {
	Code       FatalCode
	TargetKind string
	TargetID   string
	Reason     string
}

func (e *FatalError) Error() string {
	return fmt.Sprintf("%v: %s %s/%s: %s", ErrFatal, e.Code, e.TargetKind, e.TargetID, e.Reason)
}

func (e *FatalError) Unwrap() error { return ErrFatal }

// ClaimStatementContent describes one content object referenced by at least
// one claim. Statement hashes themselves are intentionally reduced to a
// presence bit so MinimumCheck cannot expose identity material.
type ClaimStatementContent struct {
	ContentID       string
	OwnerResidentID string
	ErasureState    string
	Claims          []ClaimStatementAlias
	ErasureEvents   []ContentErasureEvent
}

type ClaimStatementAlias struct {
	ClaimID            string
	OwnerResidentID    string
	StatementHashIsSet bool
}

type ContentErasureEvent struct {
	EventID           string
	ContentID         string
	CanonicalCommitID string
	CommitResidentID  string
	ErasureScope      string
	RecordedAt        int64
	RecordedTZ        string
	CommitRecordedAt  int64
	CommitRecordedTZ  string
}

type ClaimStatementErasureEvent struct {
	EventID               string
	ClaimID               string
	ResidentID            string
	CanonicalCommitID     string
	ContentErasureEventID string
	RecordedAt            int64
	RecordedTZ            string
}

// ClaimStatementAliasSnapshot is a captured read view. Pair events are kept
// outside groups so the checker can prove the relation in both directions,
// including an orphan or a pair that points at the wrong content event.
type ClaimStatementAliasSnapshot struct {
	Contents      []ClaimStatementContent
	ErasureEvents []ClaimStatementErasureEvent
}

// MinimumSource returns one internally consistent, read-only snapshot.
type MinimumSource interface {
	LoadClaimStatementAliases(context.Context) (ClaimStatementAliasSnapshot, error)
}

// StructuralMinimumSource is an optional production capability. It keeps the
// adapter-specific relational scan out of this package while making every
// caller of MinimumChecker receive the same fail-closed structural gate.
// Legacy/test sources that only exercise alias semantics remain compatible.
type StructuralMinimumSource interface {
	ValidateMinimumStructure(context.Context) error
}

// MinimumChecker evaluates fatal invariants without writing findings or any
// other state.
type MinimumChecker struct {
	source  MinimumSource
	objects BlobObjectReader
}

// MinimumOption configures a caller-owned physical boundary without widening
// the read-only database source.
type MinimumOption func(*MinimumChecker)

// WithBlobObjects enables the mandatory SQLite/filesystem dual-copy check.
func WithBlobObjects(objects BlobObjectReader) MinimumOption {
	return func(checker *MinimumChecker) { checker.objects = objects }
}

func NewMinimumChecker(source MinimumSource, options ...MinimumOption) *MinimumChecker {
	checker := &MinimumChecker{source: source}
	for _, option := range options {
		if option != nil {
			option(checker)
		}
	}
	return checker
}

// Check runs the minimum fatal checks. Additional Slice 1 checks can be added
// behind MinimumSource without changing callers at startup/backup/restore
// boundaries.
func (checker *MinimumChecker) Check(ctx context.Context) error {
	if checker == nil || checker.source == nil {
		return errors.New("integrity: minimum source is required")
	}
	if ctx == nil {
		return errors.New("integrity: nil context")
	}
	snapshot, err := checker.source.LoadClaimStatementAliases(ctx)
	if err != nil {
		return fmt.Errorf("integrity: load claim statement aliases: %w", err)
	}
	if err := checkClaimStatementAliases(snapshot); err != nil {
		return err
	}
	if structural, ok := checker.source.(StructuralMinimumSource); ok {
		if err := structural.ValidateMinimumStructure(ctx); err != nil {
			return err
		}
	}
	if checker.objects != nil {
		blobSource, ok := checker.source.(PresentBlobSource)
		if !ok {
			return errors.New("integrity: present blob source is required")
		}
		if err := checkPresentBlobCopies(ctx, blobSource, checker.objects); err != nil {
			return err
		}
	}
	return nil
}

type claimLocation struct {
	contentID string
	resident  string
	claim     ClaimStatementAlias
}

func checkClaimStatementAliases(snapshot ClaimStatementAliasSnapshot) error {
	contents := append([]ClaimStatementContent(nil), snapshot.Contents...)
	slices.SortFunc(contents, func(left, right ClaimStatementContent) int {
		return compareStrings(left.ContentID, right.ContentID)
	})
	pairs := append([]ClaimStatementErasureEvent(nil), snapshot.ErasureEvents...)
	slices.SortFunc(pairs, func(left, right ClaimStatementErasureEvent) int {
		return compareStrings(left.EventID, right.EventID)
	})

	contentSeen := make(map[string]struct{}, len(contents))
	claims := make(map[string]claimLocation)
	contentEvents := make(map[string]ContentErasureEvent)
	for contentIndex := range contents {
		content := &contents[contentIndex]
		if content.ContentID == "" || content.OwnerResidentID == "" {
			return aliasFatal("content", content.ContentID, "content or owner resident identity is empty")
		}
		if _, duplicate := contentSeen[content.ContentID]; duplicate {
			return aliasFatal("content", content.ContentID, "content appears in more than one alias group")
		}
		contentSeen[content.ContentID] = struct{}{}
		if len(content.Claims) == 0 {
			return aliasFatal("content", content.ContentID, "alias group has no claims")
		}

		content.Claims = append([]ClaimStatementAlias(nil), content.Claims...)
		slices.SortFunc(content.Claims, func(left, right ClaimStatementAlias) int {
			return compareStrings(left.ClaimID, right.ClaimID)
		})
		for _, claim := range content.Claims {
			if claim.ClaimID == "" || claim.OwnerResidentID == "" {
				return aliasFatal("content", content.ContentID, "claim or owner resident identity is empty")
			}
			if claim.OwnerResidentID != content.OwnerResidentID {
				return aliasFatal("claim", claim.ClaimID, "claim owner does not match statement content owner")
			}
			if _, duplicate := claims[claim.ClaimID]; duplicate {
				return aliasFatal("claim", claim.ClaimID, "claim appears in more than one alias group")
			}
			claims[claim.ClaimID] = claimLocation{
				contentID: content.ContentID,
				resident:  content.OwnerResidentID,
				claim:     claim,
			}
		}

		content.ErasureEvents = append([]ContentErasureEvent(nil), content.ErasureEvents...)
		slices.SortFunc(content.ErasureEvents, func(left, right ContentErasureEvent) int {
			return compareStrings(left.EventID, right.EventID)
		})
		for _, event := range content.ErasureEvents {
			if event.EventID == "" || event.CanonicalCommitID == "" {
				return aliasFatal("content", content.ContentID, "content erasure event or commit identity is empty")
			}
			if event.ContentID != content.ContentID {
				return aliasFatal("event", event.EventID, "content erasure event belongs to another content object")
			}
			if event.CommitResidentID != content.OwnerResidentID {
				return aliasFatal("event", event.EventID, "content erasure commit is not scoped to the content owner")
			}
			if event.ErasureScope != "content" && event.ErasureScope != "resident" {
				return aliasFatal("event", event.EventID, "content erasure scope is invalid")
			}
			if event.RecordedAt != event.CommitRecordedAt || event.RecordedTZ != event.CommitRecordedTZ {
				return aliasFatal("event", event.EventID, "content erasure event does not use its Canonical commit ledger time")
			}
			if _, duplicate := contentEvents[event.EventID]; duplicate {
				return aliasFatal("event", event.EventID, "content erasure event appears more than once")
			}
			contentEvents[event.EventID] = event
		}
	}

	pairsByClaim := make(map[string][]ClaimStatementErasureEvent, len(claims))
	pairSeen := make(map[string]struct{}, len(pairs))
	for _, pair := range pairs {
		if pair.EventID == "" || pair.ClaimID == "" || pair.ResidentID == "" ||
			pair.CanonicalCommitID == "" || pair.ContentErasureEventID == "" {
			return aliasFatal("event", pair.EventID, "claim statement erasure event has an empty identity")
		}
		if _, duplicate := pairSeen[pair.EventID]; duplicate {
			return aliasFatal("event", pair.EventID, "claim statement erasure event appears more than once")
		}
		pairSeen[pair.EventID] = struct{}{}
		location, exists := claims[pair.ClaimID]
		if !exists {
			return aliasFatal("event", pair.EventID, "claim statement erasure event references a missing claim")
		}
		if pair.ResidentID != location.resident {
			return aliasFatal("event", pair.EventID, "claim statement erasure event resident does not match the alias group")
		}
		contentEvent, exists := contentEvents[pair.ContentErasureEventID]
		if !exists {
			return aliasFatal("event", pair.EventID, "claim statement erasure event references a missing content erasure event")
		}
		if contentEvent.ContentID != location.contentID {
			return aliasFatal("event", pair.EventID, "claim statement erasure event points to another statement content")
		}
		if pair.CanonicalCommitID != contentEvent.CanonicalCommitID {
			return aliasFatal("event", pair.EventID, "claim and content erasure events belong to different Canonical commits")
		}
		if pair.RecordedAt != contentEvent.RecordedAt || pair.RecordedTZ != contentEvent.RecordedTZ {
			return aliasFatal("event", pair.EventID, "claim and content erasure events use different ledger times")
		}
		pairsByClaim[pair.ClaimID] = append(pairsByClaim[pair.ClaimID], pair)
	}

	for _, content := range contents {
		switch content.ErasureState {
		case "present":
			if len(content.ErasureEvents) != 0 {
				return aliasFatal("content", content.ContentID, "present content has a content erasure event")
			}
			for _, claim := range content.Claims {
				if !claim.StatementHashIsSet {
					return aliasFatal("claim", claim.ClaimID, "present statement content has a NULL claim identity")
				}
				if len(pairsByClaim[claim.ClaimID]) != 0 {
					return aliasFatal("claim", claim.ClaimID, "present statement content has a claim erasure event")
				}
			}
		case "erased":
			if len(content.ErasureEvents) != 1 {
				return aliasFatal("content", content.ContentID, "erased statement content does not have exactly one content erasure event")
			}
			contentEventID := content.ErasureEvents[0].EventID
			for _, claim := range content.Claims {
				if claim.StatementHashIsSet {
					return aliasFatal("claim", claim.ClaimID, "erased statement content retains a claim identity")
				}
				claimPairs := pairsByClaim[claim.ClaimID]
				if len(claimPairs) != 1 {
					return aliasFatal("claim", claim.ClaimID, "erased claim does not have exactly one claim statement erasure event")
				}
				if claimPairs[0].ContentErasureEventID != contentEventID {
					return aliasFatal("claim", claim.ClaimID, "erased claim is paired to the wrong content erasure event")
				}
			}
		default:
			return aliasFatal("content", content.ContentID, "statement content erasure state is invalid")
		}
	}
	return nil
}

func aliasFatal(targetKind, targetID, reason string) error {
	return &FatalError{
		Code:       ClaimStatementAliasGroupInvalid,
		TargetKind: targetKind,
		TargetID:   targetID,
		Reason:     reason,
	}
}

func compareStrings(left, right string) int {
	switch {
	case left < right:
		return -1
	case left > right:
		return 1
	default:
		return 0
	}
}
