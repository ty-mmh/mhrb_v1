package blobgc

import (
	"context"
	"errors"
	"fmt"
	"regexp"

	"mahoroba.local/mahoroba/internal/blob"
	"mahoroba.local/mahoroba/internal/operationalmetrics"
)

var wireDigestPattern = regexp.MustCompile(`^sha256:[0-9a-f]{64}$`)

func (request Request) Validate() error {
	if err := request.ResidentID.Validate(); err != nil {
		return err
	}
	if request.Apply {
		if !wireDigestPattern.MatchString(request.Confirm) {
			return ErrPlanStale
		}
	} else if request.Confirm != "" {
		return ErrPlanStale
	}
	return nil
}

// Execute creates a fresh plan for every invocation. Apply never consumes a
// stored candidate list: --confirm is matched only against the plan
// recomputed under the current host-lock ownership.
func Execute(ctx context.Context, repository Repository, files FinalStore, request Request) (result Result, returnErr error) {
	if request.Observer != nil {
		defer func() {
			outcome := operationalmetrics.OperationSucceeded
			if returnErr != nil {
				outcome = operationalmetrics.OperationFailed
				if errors.Is(returnErr, ErrPartial) || result.DeletedCount > 0 || result.PhysicalMutation {
					outcome = operationalmetrics.OperationPartial
				}
			}
			request.Observer.OperationFinished(operationalmetrics.OperationBlobGC, outcome)
		}()
	}
	if ctx == nil || repository == nil || files == nil {
		return result, fmt.Errorf("%w: nil GC dependency", ErrSourceUnavailable)
	}
	if err := request.Validate(); err != nil {
		return result, err
	}
	if err := verifyBoundary(request.Boundary); err != nil {
		return result, err
	}
	objects, err := files.WalkFinal(ctx, request.ResidentID)
	if err != nil {
		if errors.Is(err, ErrContentIntegrity) || errors.Is(err, blob.ErrDigestMismatch) || errors.Is(err, blob.ErrUnsafeFilesystem) {
			return result, fmt.Errorf("%w: enumerate final objects: %v", ErrContentIntegrity, err)
		}
		return result, fmt.Errorf("%w: enumerate final objects: %v", ErrSourceUnavailable, err)
	}
	snapshot, err := repository.Capture(ctx, request.ResidentID, objects)
	if err != nil {
		return result, err
	}
	plan, candidateBytes, err := buildPlan(snapshot, request.ResidentID, objects)
	if err != nil {
		return result, err
	}
	if request.Observer != nil {
		var finalOrphans uint64
		for _, candidate := range plan.Candidates {
			if candidate.filesystem != nil {
				finalOrphans++
			}
		}
		request.Observer.SetFinalOrphanCount(finalOrphans)
	}
	result = Result{
		Plan: plan, CandidateCount: int64(len(plan.Candidates)), CandidateBytes: candidateBytes,
		RemainingCount: int64(len(plan.Candidates)),
	}
	if err := verifyBoundary(request.Boundary); err != nil {
		return result, err
	}
	if !request.Apply {
		return result, result.Validate()
	}
	if request.Confirm != plan.Digest() {
		return Result{}, ErrPlanStale
	}
	if err := invokeFailpoint(request.Failpoint, "after_all_candidates_preflight"); err != nil {
		return result, err
	}

	for index := range plan.Candidates {
		candidate := plan.Candidates[index]
		transaction, beginErr := repository.BeginCandidate(ctx, candidate, plan.CapturedHead)
		if beginErr != nil {
			return finishFailure(result, beginErr)
		}
		closed := false
		rollback := func() error {
			if closed {
				return nil
			}
			closed = true
			return transaction.Rollback(context.WithoutCancel(ctx))
		}
		if err := verifyBoundary(request.Boundary); err != nil {
			return finishFailure(result, errors.Join(err, rollback()))
		}
		if err := invokeFailpoint(request.Failpoint, "before_filesystem_delete"); err != nil {
			return finishFailure(result, errors.Join(err, rollback()))
		}
		if candidate.filesystem != nil {
			removed, removeErr := files.RemoveFinal(ctx, *candidate.filesystem)
			result.PhysicalMutation = result.PhysicalMutation || removed
			if removeErr != nil {
				return finishFailure(result, errors.Join(removeErr, rollback()))
			}
			if !removed {
				// The exact object captured by this fresh invocation disappeared
				// before our handle-bound delete. Do not report a deletion or
				// commit a paired SQLite change against a different physical
				// outcome; require a newly computed plan.
				return finishFailure(result, errors.Join(ErrPlanStale, rollback()))
			}
		}
		if err := invokeFailpoint(request.Failpoint, "after_filesystem_delete"); err != nil {
			return finishFailure(result, errors.Join(err, rollback()))
		}
		removedSQLite, deleteErr := transaction.DeleteSQLite(ctx)
		if deleteErr != nil {
			return finishFailure(result, errors.Join(deleteErr, rollback()))
		}
		if err := invokeFailpoint(request.Failpoint, "after_sqlite_delete"); err != nil {
			return finishFailure(result, errors.Join(err, rollback()))
		}
		if err := invokeFailpoint(request.Failpoint, "before_candidate_commit"); err != nil {
			return finishFailure(result, errors.Join(err, rollback()))
		}
		if err := verifyBoundary(request.Boundary); err != nil {
			return finishFailure(result, errors.Join(err, rollback()))
		}
		commitErr := transaction.Commit(ctx)
		closed = true
		if commitErr != nil {
			// Commit errors are outcome-ambiguous. A filesystem deletion, or a
			// possibly committed DB-only deletion, requires a new dry run.
			result.PhysicalMutation = result.PhysicalMutation || removedSQLite
			return finishFailure(result, commitErr)
		}
		result.PhysicalMutation = result.PhysicalMutation || removedSQLite
		result.DeletedCount++
		result.RemainingCount--
		if err := verifyBoundary(request.Boundary); err != nil {
			return finishFailure(result, err)
		}
		if err := invokeFailpoint(request.Failpoint, "after_candidate_commit"); err != nil {
			return finishFailure(result, err)
		}
	}
	if err := repository.Maintenance(ctx); err != nil {
		result.MaintenanceIncomplete = true
	}
	if err := verifyBoundary(request.Boundary); err != nil {
		return finishFailure(result, err)
	}
	return result, result.Validate()
}

func verifyBoundary(boundary Boundary) error {
	if boundary == nil {
		return nil
	}
	if err := boundary.Verify(); err != nil {
		return errors.Join(ErrPlanStale, err)
	}
	return nil
}

func finishFailure(result Result, cause error) (Result, error) {
	if result.DeletedCount > 0 || result.PhysicalMutation {
		return result, &PartialError{Cause: cause}
	}
	return result, cause
}

func invokeFailpoint(failpoint Failpoint, name string) error {
	if failpoint == nil {
		return nil
	}
	return failpoint(name)
}
