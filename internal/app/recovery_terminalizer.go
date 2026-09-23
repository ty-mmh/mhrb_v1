package app

import (
	"context"
	"errors"
	"fmt"
	"math"

	"mahoroba.local/mahoroba/internal/canonical"
	"mahoroba.local/mahoroba/internal/domain"
	"mahoroba.local/mahoroba/internal/generation"
	"mahoroba.local/mahoroba/internal/integrity"
	"mahoroba.local/mahoroba/internal/operationalmetrics"
)

const recoveryBatchSize = 256

// RunningAttemptRepository is the read-only discovery boundary shared by
// serve startup, offline recovery, and restore. Implementations must return
// only attempts whose latest logical outcome is still running and must fail
// closed when an outcome history is malformed.
type RunningAttemptRepository interface {
	RunningAttempts(context.Context, int) ([]domain.RunningAttempt, error)
}

// ResidentRunningAttemptRepository is the hard-lifecycle variant of running
// discovery. Keeping the resident predicate in the repository prevents an
// Archive boundary from depending on malformed or continuously growing work
// owned by another resident.
type ResidentRunningAttemptRepository interface {
	RunningAttemptsForResident(context.Context, canonical.ID, int) ([]domain.RunningAttempt, error)
}

func (a *Application) cancelArchivedResidentRunningAttempts(
	ctx context.Context,
	residentID canonical.ID,
) error {
	repository, ok := a.repository.(ResidentRunningAttemptRepository)
	if !ok {
		return nil
	}
	for {
		attempts, err := repository.RunningAttemptsForResident(ctx, residentID, recoveryBatchSize)
		if err != nil {
			return fmt.Errorf("app: discover archived resident running attempts: %w", err)
		}
		if len(attempts) == 0 {
			return nil
		}
		for _, attempt := range attempts {
			outcomeID, err := a.ids.New()
			if err != nil {
				return err
			}
			_, err = a.submit(ctx, domain.FailAttemptCommand(domain.FailAttempt{
				Attempt: domain.Attempt{
					RunID: attempt.RunID, ResidentID: residentID,
					AttemptNo: attempt.AttemptNo, OutcomeID: outcomeID,
				},
				State: "cancelled",
				ErrorClass: generation.MustOutcomeErrorCode(
					generation.ErrorResidentInactive, 0,
				).String(),
			}))
			if err != nil && !errors.Is(err, canonical.ErrNoMutation) {
				latest, readErr := a.repository.Generation(context.WithoutCancel(ctx), attempt.RunID)
				if readErr == nil && latest.State != domain.WorkRunning {
					continue
				}
				return fmt.Errorf("app: cancel archived resident running attempt %s: %w", attempt.RunID, err)
			}
		}
	}
}

// MandatoryWorkRepository performs bounded, stable all-resident discovery.
// It returns only unavailable dialogue and mandatory extraction obligations;
// available work is never dispatched by RecoveryTerminalizer.
type MandatoryWorkRepository interface {
	DiscoverMandatoryRecoveryWork(
		context.Context, *domain.MandatoryRecoveryCursor, int,
	) ([]domain.MandatoryRecoveryWork, *domain.MandatoryRecoveryCursor, error)
}

// CancellationEnvelopeResolver freezes the synthetic attempt-zero envelope
// from event-time history or exposes a typed integrity candidate. It must not
// consult provider configuration or call a provider.
type CancellationEnvelopeResolver interface {
	ResolveMandatoryCancellationEnvelope(
		context.Context, domain.MandatoryRecoveryWork,
	) (domain.CancellationEnvelopeResolution, error)
}

// RecoverySubmit appends one Canonical terminal outcome. Keeping submission as
// a function makes the terminalizer independent of Application lifecycle and
// reusable by offline operations while still allowing Application to preserve
// its post-commit notification behavior.
type RecoverySubmit func(context.Context, canonical.Command) (canonical.CommandResult, error)

type RecoveryTerminalizerOptions struct {
	Repository          RunningAttemptRepository
	MandatoryRepository MandatoryWorkRepository
	EnvelopeResolver    CancellationEnvelopeResolver
	IDs                 *canonical.IDGenerator
	Submit              RecoverySubmit
	Observer            interface {
		SetMandatoryWorkCounts(operationalmetrics.MandatoryWorkCounts)
	}
}

// RecoveryTerminalizer appends cancelled/runtime_interrupted outcomes for
// attempts left running by an earlier process. It never updates or deletes an
// existing outcome. Reinvocation is therefore an exact retry: already
// terminal attempts disappear from discovery and receive no second outcome.
type RecoveryTerminalizer struct {
	repository RunningAttemptRepository
	mandatory  MandatoryWorkRepository
	resolver   CancellationEnvelopeResolver
	ids        *canonical.IDGenerator
	submit     RecoverySubmit
	observer   interface {
		SetMandatoryWorkCounts(operationalmetrics.MandatoryWorkCounts)
	}
}

type RecoveryTerminalizationResult struct {
	TerminalizedAttempts   int
	CancelledMandatoryWork int
	CanonicalCommits       []canonical.CommitMetadata
	UnresolvedCandidates   []integrity.CandidateInput
}

func NewRecoveryTerminalizer(options RecoveryTerminalizerOptions) (*RecoveryTerminalizer, error) {
	if options.Repository == nil || options.IDs == nil || options.Submit == nil {
		return nil, errors.New("app: recovery repository, IDs, and submitter are required")
	}
	if (options.MandatoryRepository == nil) != (options.EnvelopeResolver == nil) {
		return nil, errors.New("app: mandatory recovery repository and envelope resolver must be supplied together")
	}
	return &RecoveryTerminalizer{
		repository: options.Repository,
		mandatory:  options.MandatoryRepository,
		resolver:   options.EnvelopeResolver,
		ids:        options.IDs,
		submit:     options.Submit,
		observer:   options.Observer,
	}, nil
}

// Terminalize drains discovery in bounded batches. A command error is
// returned immediately with the durable commits already observed in Result;
// the caller can safely retry after a crash or ambiguous transport failure.
func (terminalizer *RecoveryTerminalizer) Terminalize(ctx context.Context) (RecoveryTerminalizationResult, error) {
	var result RecoveryTerminalizationResult
	if err := terminalizer.PreflightMandatory(ctx); err != nil {
		return result, err
	}
	result, err := terminalizer.TerminalizeRunning(ctx)
	if err != nil {
		return result, err
	}
	mandatory, err := terminalizer.TerminalizeMandatory(ctx)
	mergeRecoveryTerminalizationResult(&result, mandatory)
	return result, err
}

// TerminalizeRunning is the first startup phase. Hosts run it before the
// integrity pipeline so structural scan/apply observes no stale running rows.
func (terminalizer *RecoveryTerminalizer) TerminalizeRunning(ctx context.Context) (RecoveryTerminalizationResult, error) {
	var result RecoveryTerminalizationResult
	if terminalizer == nil {
		return result, errors.New("app: nil recovery terminalizer")
	}
	if ctx == nil {
		return result, errors.New("app: nil recovery context")
	}
	for {
		running, err := terminalizer.repository.RunningAttempts(ctx, recoveryBatchSize)
		if err != nil {
			return result, fmt.Errorf("app: discover interrupted attempts: %w", err)
		}
		if len(running) == 0 {
			break
		}
		for _, attempt := range running {
			outcomeID, err := terminalizer.ids.New()
			if err != nil {
				return result, err
			}
			commandResult, err := terminalizer.submit(ctx, domain.FailAttemptCommand(domain.FailAttempt{
				Attempt: domain.Attempt{
					RunID: attempt.RunID, ResidentID: attempt.ResidentID,
					AttemptNo: attempt.AttemptNo, OutcomeID: outcomeID,
				},
				State: "cancelled", ErrorClass: generation.RuntimeInterruptedErrorCode().String(),
			}))
			if err != nil {
				return result, fmt.Errorf("app: cancel interrupted attempt %s: %w", attempt.RunID, err)
			}
			result.TerminalizedAttempts++
			if !commandResult.Commit.CommitID.IsZero() {
				result.CanonicalCommits = append(result.CanonicalCommits, commandResult.Commit)
			}
		}
	}
	return result, nil
}

// TerminalizeMandatory is the post-integrity startup phase. It performs the
// complete overflow preflight before any mandatory cancellation and retains
// unresolved typed candidates for the shared integrity pipeline/Admin result.
func (terminalizer *RecoveryTerminalizer) TerminalizeMandatory(ctx context.Context) (RecoveryTerminalizationResult, error) {
	var result RecoveryTerminalizationResult
	if terminalizer == nil {
		return result, errors.New("app: nil recovery terminalizer")
	}
	if ctx == nil {
		return result, errors.New("app: nil recovery context")
	}
	if terminalizer.mandatory == nil {
		return result, nil
	}
	if err := terminalizer.PreflightMandatory(ctx); err != nil {
		return result, err
	}
	if err := terminalizer.cancelMandatory(ctx, &result); err != nil {
		return result, err
	}
	return result, nil
}

// PreflightMandatory traverses every bounded mandatory-work page without
// mutation. Composite recovery and offline Admin callers invoke it before
// running-attempt terminalization so a retry-attempt overflow makes the whole
// command fail without a partial Canonical commit.
func (terminalizer *RecoveryTerminalizer) PreflightMandatory(ctx context.Context) error {
	if terminalizer == nil {
		return errors.New("app: nil recovery terminalizer")
	}
	if ctx == nil {
		return errors.New("app: nil recovery context")
	}
	if terminalizer.mandatory == nil {
		return nil
	}
	return terminalizer.preflightMandatory(ctx)
}

func mergeRecoveryTerminalizationResult(target *RecoveryTerminalizationResult, source RecoveryTerminalizationResult) {
	target.TerminalizedAttempts += source.TerminalizedAttempts
	target.CancelledMandatoryWork += source.CancelledMandatoryWork
	target.CanonicalCommits = append(target.CanonicalCommits, source.CanonicalCommits...)
	target.UnresolvedCandidates = append(target.UnresolvedCandidates, source.UnresolvedCandidates...)
}

// preflightMandatory traverses the complete event cursor before the first
// mandatory-work mutation. Consequently a retryable math.MaxInt64 candidate
// cannot leave an earlier obligation partially cancelled.
func (terminalizer *RecoveryTerminalizer) preflightMandatory(ctx context.Context) error {
	var cursor *domain.MandatoryRecoveryCursor
	var counts operationalmetrics.MandatoryWorkCounts
	for {
		works, next, err := terminalizer.mandatory.DiscoverMandatoryRecoveryWork(ctx, cursor, recoveryBatchSize)
		if err != nil {
			return fmt.Errorf("app: discover mandatory recovery work: %w", err)
		}
		for _, work := range works {
			incrementMandatoryRecoveryCount(&counts, work)
			// A running MaxInt64 attempt would become retry-pending after the
			// pre-integrity interruption and could never receive its mandatory
			// cancellation attempt. Reject it before that interruption commit.
			if (work.State == domain.WorkRunning || work.State == domain.WorkRetryPending) &&
				work.AttemptNo == math.MaxInt64 {
				if work.RunID == nil {
					return errors.New("app: retry-pending mandatory work has no run")
				}
				return &domain.RecoveryAttemptOverflowError{RunID: *work.RunID}
			}
		}
		if next == nil {
			if terminalizer.observer != nil {
				terminalizer.observer.SetMandatoryWorkCounts(counts)
			}
			return nil
		}
		cursor = next
	}
}

func incrementMandatoryRecoveryCount(counts *operationalmetrics.MandatoryWorkCounts, work domain.MandatoryRecoveryWork) {
	var target *uint64
	switch work.Kind {
	case domain.MandatoryRecoveryDialogue:
		switch work.State {
		case domain.WorkPending:
			target = &counts.DialoguePending
		case domain.WorkRunning:
			target = &counts.DialogueRunning
		case domain.WorkRetryPending:
			target = &counts.DialogueRetryPending
		}
	case domain.MandatoryRecoveryMemoryExtraction:
		switch work.State {
		case domain.WorkPending:
			target = &counts.MemoryPending
		case domain.WorkRunning:
			target = &counts.MemoryRunning
		case domain.WorkRetryPending:
			target = &counts.MemoryRetryPending
		}
	}
	if target != nil && *target != ^uint64(0) {
		*target = *target + 1
	}
}

func (terminalizer *RecoveryTerminalizer) cancelMandatory(
	ctx context.Context,
	result *RecoveryTerminalizationResult,
) error {
	var cursor *domain.MandatoryRecoveryCursor
	seenUnresolved := make(map[canonical.Digest]struct{})
	for {
		works, next, err := terminalizer.mandatory.DiscoverMandatoryRecoveryWork(ctx, cursor, recoveryBatchSize)
		if err != nil {
			return fmt.Errorf("app: rediscover mandatory recovery work: %w", err)
		}
		for _, work := range works {
			resolution, err := terminalizer.resolver.ResolveMandatoryCancellationEnvelope(ctx, work)
			if err != nil {
				return fmt.Errorf("app: resolve mandatory cancellation envelope %s: %w", work.IdempotencyKey, err)
			}
			if resolution.Unresolved != nil {
				candidate, err := integrity.NewCandidate(*resolution.Unresolved)
				if err != nil {
					return err
				}
				if _, duplicate := seenUnresolved[candidate.Fingerprint]; !duplicate {
					seenUnresolved[candidate.Fingerprint] = struct{}{}
					result.UnresolvedCandidates = append(result.UnresolvedCandidates, candidate.CandidateInput)
				}
				continue
			}
			if err := terminalizer.submitMandatoryCancellation(ctx, work, resolution.Generation, result); err != nil {
				return err
			}
		}
		if next == nil {
			return nil
		}
		cursor = next
	}
}

func (terminalizer *RecoveryTerminalizer) submitMandatoryCancellation(
	ctx context.Context,
	work domain.MandatoryRecoveryWork,
	envelope domain.PrepareGeneration,
	result *RecoveryTerminalizationResult,
) error {
	// Only a runless obligation creates the provider-free synthetic envelope
	// whose restore-invariant meaning is bound by semantic-v1. An existing run
	// must retain its original provider/model envelope; recovery appends only a
	// terminal outcome and never rewrites it into an internal provider run.
	if work.RunID == nil {
		if _, _, err := domain.CancellationEnvelopeSemanticDigest(envelope, work.CancellationCode); err != nil {
			return fmt.Errorf("app: invalid mandatory cancellation semantic envelope %s: %w", work.IdempotencyKey, err)
		}
	}
	ids := make([]canonical.ID, 3)
	for index := range ids {
		id, err := terminalizer.ids.New()
		if err != nil {
			return err
		}
		ids[index] = id
	}
	if work.RunID == nil {
		envelope.RunID = ids[0]
	} else {
		envelope.RunID = *work.RunID
	}
	envelope.RunningOutcomeID = ids[1]
	var command canonical.Command
	switch work.Kind {
	case domain.MandatoryRecoveryDialogue:
		command = domain.CancelDialogueCommand(domain.CancelDialogue{
			Generation: envelope, SourceEventID: work.SourceEvent.ID,
			CancelledOutcomeID: ids[2], ErrorClass: work.CancellationCode,
		})
	case domain.MandatoryRecoveryMemoryExtraction:
		command = domain.CancelMemoryExtractionCommand(domain.CancelMemoryExtraction{
			Generation: envelope, SourceEventID: work.SourceEvent.ID,
			CancelledOutcomeID: ids[2], ErrorClass: work.CancellationCode,
		})
	default:
		return fmt.Errorf("app: unsupported mandatory recovery kind %q", work.Kind)
	}
	commandResult, err := terminalizer.submit(ctx, command)
	if errors.Is(err, canonical.ErrNoMutation) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("app: cancel mandatory work %s: %w", work.IdempotencyKey, err)
	}
	if !commandResult.Commit.CommitID.IsZero() {
		result.CancelledMandatoryWork++
		result.CanonicalCommits = append(result.CanonicalCommits, commandResult.Commit)
	}
	return nil
}
