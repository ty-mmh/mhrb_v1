package domain

import (
	"context"
	"errors"
	"fmt"
	"time"

	"mahoroba.local/mahoroba/internal/canonical"
)

const (
	productionMemoryDiscoveryPageSize   = 128
	productionMemoryDiscoveryCandidates = 1024
	productionMemoryDiscoveryPages      = 8
)

const productionMemoryDiscoveryElapsed = 2 * time.Second

// MemoryExtractionWork is an event-scoped durable obligation. PolicyRevisionID
// is resolved at the source event's Canonical commit, so a later policy
// activation cannot retroactively create mandatory work.
type MemoryExtractionWork struct {
	SourceEvent         Event
	IdempotencyKey      string
	PolicyRevisionID    canonical.ID
	RunID               *canonical.ID
	AttemptNo           int64
	RetryCount          int64
	State               WorkState
	CancellationCode    string
	ForegroundPreempted bool
}

// MemoryWorkRepository is an optional M5 capability kept separate from the
// M0-M4 Repository contract so legacy adapters remain source compatible.
type MemoryWorkRepository interface {
	DiscoverMemoryExtractionWork(context.Context, canonical.ID, MemoryDiscoveryRequest) (MemoryDiscoveryResult, error)
	DiscoverMemoryReextractionWork(context.Context, canonical.ID, MemoryReextractionDiscoveryRequest) (MemoryReextractionDiscoveryResult, error)
	MemoryReextractionWork(context.Context, canonical.ID, canonical.ID, canonical.ID, int) (MemoryExtractionWork, bool, error)
	PipelineVersion(context.Context, string, string) (PipelineVersionDefinition, error)
}

// MemoryDiscoveryCursor is Operational, process-local progress through a
// finite memory sweep. CycleThroughSeq is fixed for the lifetime of the
// sweep so newer ingress cannot extend it indefinitely. Losing the cursor may
// repeat immutable-history classification, but cannot skip a durable
// obligation.
type MemoryDiscoveryCursor struct {
	AfterSeq        *canonical.Seq
	CycleThroughSeq canonical.Seq
}

func (cursor MemoryDiscoveryCursor) Validate() error {
	if err := cursor.CycleThroughSeq.Validate(); err != nil {
		return fmt.Errorf("memory discovery cursor cycle bound: %w", err)
	}
	if cursor.AfterSeq == nil {
		return nil
	}
	if err := cursor.AfterSeq.Validate(); err != nil {
		return fmt.Errorf("memory discovery cursor position: %w", err)
	}
	if *cursor.AfterSeq > cursor.CycleThroughSeq {
		return errors.New("memory discovery cursor position exceeds its cycle bound")
	}
	return nil
}

// MemoryDiscoveryBudget bounds every repository pass independently of the
// amount of Canonical event history. Tests and focused callers may choose
// smaller positive values, but no request may exceed the production envelope.
type MemoryDiscoveryBudget struct {
	PageSize   int
	Candidates int
	Pages      int
	Elapsed    time.Duration
}

func ProductionMemoryDiscoveryBudget() MemoryDiscoveryBudget {
	return MemoryDiscoveryBudget{
		PageSize: productionMemoryDiscoveryPageSize, Candidates: productionMemoryDiscoveryCandidates,
		Pages: productionMemoryDiscoveryPages, Elapsed: productionMemoryDiscoveryElapsed,
	}
}

func (budget MemoryDiscoveryBudget) Validate() error {
	if budget.PageSize < 1 || budget.Candidates < 1 || budget.Pages < 1 {
		return errors.New("memory discovery budget requires positive row and page limits")
	}
	if budget.Elapsed <= 0 {
		return errors.New("memory discovery budget requires a positive elapsed limit")
	}
	if budget.PageSize > productionMemoryDiscoveryPageSize ||
		budget.Candidates > productionMemoryDiscoveryCandidates ||
		budget.Pages > productionMemoryDiscoveryPages ||
		budget.Elapsed > productionMemoryDiscoveryElapsed {
		return errors.New("memory discovery budget exceeds the production upper bound")
	}
	if budget.PageSize > budget.Candidates {
		return errors.New("memory discovery page size exceeds its candidate budget")
	}
	if budget.Candidates > budget.PageSize*budget.Pages {
		return errors.New("memory discovery candidate budget exceeds its page budget")
	}
	return nil
}

type MemoryDiscoveryRequest struct {
	Cursor *MemoryDiscoveryCursor
	// ThroughSeq supplies a foreground fairness ceiling. When nil at the
	// start of a cycle, the repository captures the current maximum evidence
	// sequence and keeps it fixed in the returned cursor.
	ThroughSeq  *canonical.Seq
	MaxAttempts int
	Budget      MemoryDiscoveryBudget
}

func (request MemoryDiscoveryRequest) Validate() error {
	if request.ThroughSeq != nil {
		if err := request.ThroughSeq.Validate(); err != nil {
			return fmt.Errorf("memory discovery source bound: %w", err)
		}
	}
	if request.MaxAttempts < 1 {
		return errors.New("memory discovery requires a positive maximum attempt count")
	}
	if err := request.Budget.Validate(); err != nil {
		return err
	}
	if request.Cursor != nil {
		if err := request.Cursor.Validate(); err != nil {
			return err
		}
	}
	return nil
}

// MemoryDiscoveryResult reports at most one mandatory extraction and the
// exact bounded progress that found it. An error result never advances the
// cursor. CycleComplete implies Work and NextCursor are nil; the next pass may
// begin a new sweep with a newly captured ThroughSeq.
type MemoryDiscoveryResult struct {
	Work              *MemoryExtractionWork
	NextCursor        *MemoryDiscoveryCursor
	CandidatesScanned int
	PageQueries       int
	DialogueDeferred  int
	CycleComplete     bool
	BudgetExhausted   bool
}

// MemoryReextractionCommitCursor is the within-commit keyset position of a
// re-extraction sweep. CommitID is a physical lookup key; CommitSeq preserves
// Canonical ordering. A nil AfterRunID means that the commit has been selected
// but no generation run in it has been consumed yet.
type MemoryReextractionCommitCursor struct {
	CommitID   canonical.ID
	CommitSeq  canonical.CommitSeq
	AfterRunID *canonical.ID
}

func (cursor MemoryReextractionCommitCursor) Validate() error {
	if err := cursor.CommitID.Validate(); err != nil {
		return fmt.Errorf("memory re-extraction active commit ID: %w", err)
	}
	if err := cursor.CommitSeq.Validate(); err != nil {
		return fmt.Errorf("memory re-extraction active commit sequence: %w", err)
	}
	if cursor.AfterRunID != nil {
		if err := cursor.AfterRunID.Validate(); err != nil {
			return fmt.Errorf("memory re-extraction active run position: %w", err)
		}
	}
	return nil
}

// MemoryReextractionDiscoveryCursor is Operational and process-local. The
// fixed commit ceiling prevents newer Canonical commits from extending an
// in-progress sweep. CompletedThroughCommitSeq includes zero-run commits;
// ActiveCommit, when present, is the sole commit whose runs remain in flight.
type MemoryReextractionDiscoveryCursor struct {
	CycleThroughCommitSeq     canonical.CommitSeq
	CompletedThroughCommitSeq *canonical.CommitSeq
	ActiveCommit              *MemoryReextractionCommitCursor
}

func (cursor MemoryReextractionDiscoveryCursor) Validate() error {
	if err := cursor.CycleThroughCommitSeq.Validate(); err != nil {
		return fmt.Errorf("memory re-extraction discovery cycle bound: %w", err)
	}
	if cursor.CompletedThroughCommitSeq != nil {
		if err := cursor.CompletedThroughCommitSeq.Validate(); err != nil {
			return fmt.Errorf("memory re-extraction completed commit position: %w", err)
		}
		if *cursor.CompletedThroughCommitSeq > cursor.CycleThroughCommitSeq {
			return errors.New("memory re-extraction completed commit exceeds its cycle bound")
		}
	}
	if cursor.ActiveCommit == nil {
		return nil
	}
	if err := cursor.ActiveCommit.Validate(); err != nil {
		return err
	}
	if cursor.ActiveCommit.CommitSeq > cursor.CycleThroughCommitSeq {
		return errors.New("memory re-extraction active commit exceeds its cycle bound")
	}
	if cursor.CompletedThroughCommitSeq != nil &&
		cursor.ActiveCommit.CommitSeq <= *cursor.CompletedThroughCommitSeq {
		return errors.New("memory re-extraction active commit is not after completed progress")
	}
	return nil
}

type MemoryReextractionDiscoveryRequest struct {
	Cursor      *MemoryReextractionDiscoveryCursor
	MaxAttempts int
	Budget      MemoryDiscoveryBudget
}

func (request MemoryReextractionDiscoveryRequest) Validate() error {
	if request.MaxAttempts < 1 {
		return errors.New("memory re-extraction discovery requires a positive maximum attempt count")
	}
	if err := request.Budget.Validate(); err != nil {
		return err
	}
	if request.Cursor != nil {
		if err := request.Cursor.Validate(); err != nil {
			return err
		}
	}
	return nil
}

// MemoryReextractionDiscoveryResult reports at most one Admin re-extraction
// obligation. CandidatesScanned counts ordered physical discovery positions:
// every Canonical commit inspected and every memory_extraction run consumed.
// A one-row run lookahead is excluded because it remains unconsumed.
type MemoryReextractionDiscoveryResult struct {
	Work              *MemoryExtractionWork
	NextCursor        *MemoryReextractionDiscoveryCursor
	CandidatesScanned int
	PageQueries       int
	CycleComplete     bool
	BudgetExhausted   bool
}
