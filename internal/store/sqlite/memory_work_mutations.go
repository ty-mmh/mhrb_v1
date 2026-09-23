package sqlite

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"math"

	"mahoroba.local/mahoroba/internal/canonical"
	"mahoroba.local/mahoroba/internal/domain"
	"mahoroba.local/mahoroba/internal/generation"
)

func (u *canonicalUoW) CancelMemoryExtraction(
	ctx context.Context,
	value domain.CancelMemoryExtraction,
) (domain.MemoryExtractionCancellationResult, error) {
	envelope := value.Generation
	if err := u.requireResidentScope(envelope.ResidentID); err != nil {
		return domain.MemoryExtractionCancellationResult{}, err
	}
	code, err := generation.ParseOutcomeErrorCode(value.ErrorClass)
	if err != nil || (code.Class() != generation.ErrorSourceContentErased && code.Class() != generation.ErrorResidentInactive) {
		return domain.MemoryExtractionCancellationResult{}, errors.New("sqlite: invalid memory extraction cancellation reason")
	}
	request, err := domain.ParseMemoryExtractionObligation(envelope.IdempotencyKey)
	if err != nil || request.SourceEventID != value.SourceEventID {
		return domain.MemoryExtractionCancellationResult{}, errors.New("sqlite: memory extraction cancellation key/source mismatch")
	}
	var eventType, erasureState string
	if err := u.tx.QueryRowContext(ctx, `SELECT event.event_type, content.erasure_state
		FROM events event JOIN content_objects content ON content.content_id = event.content_id
		WHERE event.event_id = ? AND event.resident_id = ?`,
		value.SourceEventID.String(), envelope.ResidentID.String(),
	).Scan(&eventType, &erasureState); err != nil {
		return domain.MemoryExtractionCancellationResult{}, fmt.Errorf("sqlite: resolve memory cancellation source: %w", err)
	}
	if eventType != "user_message" && eventType != "self_talk" {
		return domain.MemoryExtractionCancellationResult{}, errors.New("sqlite: memory extraction source is not eligible")
	}
	status, err := u.currentStatus(ctx, envelope.ResidentID)
	if err != nil {
		return domain.MemoryExtractionCancellationResult{}, err
	}
	if code.Class() == generation.ErrorSourceContentErased && erasureState != "erased" {
		return domain.MemoryExtractionCancellationResult{}, errors.New("sqlite: memory source content is not erased")
	}
	if code.Class() == generation.ErrorResidentInactive && status == "active" {
		return domain.MemoryExtractionCancellationResult{}, errors.New("sqlite: memory resident is still active")
	}
	expectedCode := memoryRecoveryReason(erasureState == "erased", status)
	if expectedCode == "" || code.String() != expectedCode {
		return domain.MemoryExtractionCancellationResult{}, fmt.Errorf(
			"sqlite: memory cancellation reason %q conflicts with priority-selected reason %q",
			code.String(), expectedCode,
		)
	}

	var runRaw string
	err = u.tx.QueryRowContext(ctx, `SELECT generation_run_id FROM generation_runs
		WHERE resident_id = ? AND idempotency_key = ?`, envelope.ResidentID.String(), envelope.IdempotencyKey).Scan(&runRaw)
	if err != nil {
		if !errors.Is(err, sql.ErrNoRows) {
			return domain.MemoryExtractionCancellationResult{}, fmt.Errorf("sqlite: resolve memory generation run: %w", err)
		}
		if request.Mode == domain.MemoryExtractionMandatory {
			expected, resolvable, resolveErr := resolveSyntheticCancellationEnvelope(ctx, u.tx, domain.MandatoryRecoveryWork{
				Kind:           domain.MandatoryRecoveryMemoryExtraction,
				SourceEvent:    domain.Event{ID: value.SourceEventID, ResidentID: envelope.ResidentID},
				IdempotencyKey: envelope.IdempotencyKey, PolicyRevisionID: envelope.MemoryPolicyRevisionID,
				CancellationCode: code.String(),
			})
			if resolveErr != nil {
				return domain.MemoryExtractionCancellationResult{}, resolveErr
			}
			if !resolvable || !cancellationEnvelopeSemanticallyEqual(envelope, expected) {
				return domain.MemoryExtractionCancellationResult{}, errors.New("sqlite: memory cancellation envelope is not the event-time recovery envelope")
			}
		}
		if request.Mode == domain.MemoryExtractionReextract {
			if err := u.requireExactMemoryPipeline(ctx, envelope.PipelineVersionID,
				"memory_extraction", domain.MemoryExtractionPipelineVersion); err != nil {
				return domain.MemoryExtractionCancellationResult{}, err
			}
			policy, err := u.loadDecisionMemoryPolicy(ctx, envelope.ResidentID, envelope.MemoryPolicyRevisionID)
			if err != nil {
				return domain.MemoryExtractionCancellationResult{}, err
			}
			if err := policy.RequireEnabled(); err != nil {
				return domain.MemoryExtractionCancellationResult{}, err
			}
		}
		if err := u.insertGenerationRun(ctx, envelope); err != nil {
			return domain.MemoryExtractionCancellationResult{}, err
		}
		if err := u.insertOutcome(ctx, value.CancelledOutcomeID, envelope.RunID, 0, "cancelled", nil, nil, nil, 0, code.String()); err != nil {
			return domain.MemoryExtractionCancellationResult{}, err
		}
		return domain.MemoryExtractionCancellationResult{RunID: envelope.RunID, AttemptNo: 0}, nil
	}
	runID, err := canonical.ParseID(runRaw)
	if err != nil {
		return domain.MemoryExtractionCancellationResult{}, err
	}
	attempt, state, previousError, err := u.latestOutcomeWithError(ctx, runID, envelope.ResidentID)
	if err != nil {
		return domain.MemoryExtractionCancellationResult{}, err
	}
	result := domain.MemoryExtractionCancellationResult{RunID: runID, AttemptNo: attempt}
	switch state {
	case "succeeded":
		return result, canonical.ErrNoMutation
	case "running":
		if err := u.insertOutcome(ctx, value.CancelledOutcomeID, runID, attempt, "cancelled", nil, nil, nil, 0, code.String()); err != nil {
			return domain.MemoryExtractionCancellationResult{}, err
		}
		return result, nil
	case "failed", "cancelled":
		previousCode, parseErr := generation.ParseOutcomeErrorCode(previousError)
		if parseErr != nil {
			return domain.MemoryExtractionCancellationResult{}, fmt.Errorf("sqlite: parse latest memory cancellation outcome: %w", parseErr)
		}
		if state == "cancelled" && previousCode.String() == code.String() {
			return result, canonical.ErrNoMutation
		}
		if !previousCode.Retryable() {
			return result, canonical.ErrNoMutation
		}
		if attempt == math.MaxInt64 {
			return domain.MemoryExtractionCancellationResult{}, &domain.RecoveryAttemptOverflowError{RunID: runID}
		}
		attempt++
		if err := u.insertOutcome(ctx, envelope.RunningOutcomeID, runID, attempt, "running", nil, nil, nil, 0, ""); err != nil {
			return domain.MemoryExtractionCancellationResult{}, err
		}
		if err := u.insertOutcome(ctx, value.CancelledOutcomeID, runID, attempt, "cancelled", nil, nil, nil, 0, code.String()); err != nil {
			return domain.MemoryExtractionCancellationResult{}, err
		}
		return domain.MemoryExtractionCancellationResult{RunID: runID, AttemptNo: attempt}, nil
	default:
		return domain.MemoryExtractionCancellationResult{}, fmt.Errorf("sqlite: unknown memory generation state %q", state)
	}
}

func (u *canonicalUoW) FailMemoryExtraction(ctx context.Context, value domain.FailMemoryExtraction) error {
	if err := u.requireResidentScope(value.ResidentID); err != nil {
		return err
	}
	if _, err := generation.ParseOutcomeErrorCode(value.ErrorClass); err != nil {
		return fmt.Errorf("sqlite: invalid memory extraction failure code: %w", err)
	}
	var purpose string
	if err := u.tx.QueryRowContext(ctx, `SELECT purpose FROM generation_runs
		WHERE generation_run_id = ? AND resident_id = ?`, value.RunID.String(), value.ResidentID.String()).Scan(&purpose); err != nil {
		return fmt.Errorf("sqlite: resolve failed memory generation: %w", err)
	}
	if purpose != string(domain.GenerationPurposeMemoryExtraction) {
		return errors.New("sqlite: memory failure targets a different generation purpose")
	}
	attempt, state, err := u.latestOutcome(ctx, value.RunID, value.ResidentID)
	if err != nil {
		return err
	}
	if state != "running" || attempt != value.AttemptNo {
		return fmt.Errorf("sqlite: memory failure must terminate running attempt %d; got %s/%d", attempt, state, value.AttemptNo)
	}
	var detailID any
	if value.ErrorDetail != nil {
		if err := u.insertContent(ctx, *value.ErrorDetail); err != nil {
			return err
		}
		detailID = value.ErrorDetail.ID.String()
	}
	m := u.metadata
	if _, err := u.tx.ExecContext(ctx, `INSERT INTO generation_run_outcomes(
		outcome_id, canonical_commit_id, generation_run_id, attempt_no, state,
		output_content_id, prompt_tokens, completion_tokens, latency, estimated_cost,
		error_class, error_detail_content_id, recorded_at, recorded_tz
	) VALUES (?, ?, ?, ?, ?, NULL, NULL, NULL, NULL, NULL, ?, ?, ?, ?)`,
		value.OutcomeID.String(), m.CommitID.String(), value.RunID.String(), value.AttemptNo, value.State,
		value.ErrorClass, detailID, m.CommittedAt.UnixMicro(), m.CommittedTZ.String(),
	); err != nil {
		return fmt.Errorf("sqlite: insert memory extraction failure outcome: %w", err)
	}
	return nil
}
