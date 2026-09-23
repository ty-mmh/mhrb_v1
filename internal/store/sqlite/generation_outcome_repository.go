package sqlite

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"math"
	"sort"

	"mahoroba.local/mahoroba/internal/canonical"
	"mahoroba.local/mahoroba/internal/domain"
	"mahoroba.local/mahoroba/internal/generation"
)

var ErrInvalidGenerationOutcomeHistory = errors.New("sqlite: invalid generation outcome history")

// generationOutcomeQueryer is implemented by both *sql.DB and *sql.Tx. Keeping
// this boundary typed prevents callers from inventing a second ordering rule.
type generationOutcomeQueryer interface {
	QueryContext(context.Context, string, ...any) (*sql.Rows, error)
}

type generationOutcome struct {
	ID                   canonical.ID
	CanonicalCommitID    canonical.ID
	AttemptNo            int64
	State                string
	OutputContentID      *canonical.ID
	PromptTokens         sql.NullInt64
	CompletionTokens     sql.NullInt64
	Latency              sql.NullInt64
	EstimatedCost        sql.NullInt64
	ErrorClass           sql.NullString
	ErrorDetailContentID *canonical.ID
	RecordedAt           canonical.Instant
	RecordedTZ           canonical.Timezone
}

type generationOutcomeSummary struct {
	RunID                         canonical.ID
	ResidentID                    canonical.ID
	InputCount                    int64
	Latest                        generationOutcome
	PersistedState                domain.WorkState
	ClassifiedState               domain.WorkState
	ChargedRetryCount             int64
	RetryEligible                 bool
	LatestForegroundPreempted     bool
	HasForegroundPreemptedAttempt bool
	// RetryCount and ForegroundPreempted remain aliases for existing internal
	// work models while consumers migrate to the explicit names above.
	RetryCount          int64
	ForegroundPreempted bool
}

func (s generationOutcomeSummary) Classify(maxAttempts int64) (generationOutcomeSummary, error) {
	if maxAttempts < 1 {
		return generationOutcomeSummary{}, fmt.Errorf("%w: maxAttempts must be positive", ErrInvalidGenerationOutcomeHistory)
	}
	s.RetryEligible = false
	s.LatestForegroundPreempted = false
	s.PersistedState = domain.WorkTerminalFailed
	s.ClassifiedState = domain.WorkTerminalFailed
	switch s.Latest.State {
	case "running":
		s.PersistedState = domain.WorkRunning
		s.ClassifiedState = domain.WorkRunning
	case "succeeded":
		s.PersistedState = domain.WorkSucceeded
		s.ClassifiedState = domain.WorkSucceeded
	case "failed", "cancelled":
		if !s.Latest.ErrorClass.Valid {
			return generationOutcomeSummary{}, invalidOutcome(s.Latest, "terminal row has no error class")
		}
		code, err := generation.ParseOutcomeErrorCode(s.Latest.ErrorClass.String)
		if err != nil {
			return generationOutcomeSummary{}, invalidOutcome(s.Latest, err.Error())
		}
		s.LatestForegroundPreempted = code.Class() == generation.ErrorForegroundPreempted
		s.ForegroundPreempted = s.LatestForegroundPreempted
		s.RetryEligible = s.LatestForegroundPreempted || (code.Retryable() && s.ChargedRetryCount < maxAttempts)
		if s.RetryEligible {
			s.ClassifiedState = domain.WorkRetryPending
		}
	}
	return s, nil
}

// generationOutcomeRepository owns the logical ordering of generation
// attempts. SQLite row storage order is deliberately not part of this API.
type generationOutcomeRepository struct{}

// MandatoryRecoveryOverflow inspects the typed, validated history captured by
// the single approved outcome SQL owner. It preserves recovery's command-wide
// MaxInt64 precedence without granting the mandatory-work adapter a second
// SELECT/order implementation.
func (generationOutcomeRepository) MandatoryRecoveryOverflow(
	ctx context.Context,
	q generationOutcomeQueryer,
	runID, residentID canonical.ID,
) (int64, domain.WorkState, bool, error) {
	_, history, err := (generationOutcomeRepository{}).capturedHistory(ctx, q, runID, residentID, nil)
	if err != nil {
		return 0, "", false, err
	}
	if len(history) == 0 || history[0].AttemptNo != math.MaxInt64 {
		return 0, "", false, nil
	}
	latest := history[0]
	switch latest.State {
	case "running":
		return latest.AttemptNo, domain.WorkRunning, true, nil
	case "failed", "cancelled":
		if !latest.ErrorClass.Valid {
			return 0, "", false, nil
		}
		code, parseErr := generation.ParseOutcomeErrorCode(latest.ErrorClass.String)
		if parseErr == nil && (code.Retryable() || code.Class() == generation.ErrorForegroundPreempted) {
			return latest.AttemptNo, domain.WorkRetryPending, true, nil
		}
	}
	return 0, "", false, nil
}

func (generationOutcomeRepository) OutputReferenceCounts(ctx context.Context, q generationOutcomeQueryer, headSeq int64) (map[string]int64, error) {
	rows, err := q.QueryContext(ctx, `SELECT outcome.output_content_id, COUNT(*)
		FROM generation_run_outcomes outcome
		JOIN canonical_commits outcome_commit ON outcome_commit.canonical_commit_id = outcome.canonical_commit_id
		WHERE outcome.output_content_id IS NOT NULL AND outcome_commit.commit_seq <= ?
		GROUP BY outcome.output_content_id`, headSeq)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	counts := make(map[string]int64)
	for rows.Next() {
		var contentID string
		var count int64
		if err := rows.Scan(&contentID, &count); err != nil {
			return nil, err
		}
		counts[contentID] = count
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return counts, nil
}

func (generationOutcomeRepository) Latest(ctx context.Context, q generationOutcomeQueryer, runID, residentID canonical.ID) (generationOutcome, error) {
	summary, err := (generationOutcomeRepository{}).Summary(ctx, q, runID, residentID)
	if err != nil {
		return generationOutcome{}, err
	}
	return summary.Latest, nil
}

func (generationOutcomeRepository) LatestAtHead(ctx context.Context, q generationOutcomeQueryer, runID, residentID canonical.ID, headSeq int64) (generationOutcome, error) {
	summary, err := (generationOutcomeRepository{}).SummaryAtHead(ctx, q, runID, residentID, headSeq)
	if err != nil {
		return generationOutcome{}, err
	}
	return summary.Latest, nil
}

func (generationOutcomeRepository) Summary(ctx context.Context, q generationOutcomeQueryer, runID, residentID canonical.ID) (generationOutcomeSummary, error) {
	return (generationOutcomeRepository{}).summaryAtHead(ctx, q, runID, residentID, nil)
}

func (generationOutcomeRepository) SummaryAtHead(ctx context.Context, q generationOutcomeQueryer, runID, residentID canonical.ID, headSeq int64) (generationOutcomeSummary, error) {
	return (generationOutcomeRepository{}).summaryAtHead(ctx, q, runID, residentID, &headSeq)
}

func (generationOutcomeRepository) summaryAtHead(ctx context.Context, q generationOutcomeQueryer, runID, residentID canonical.ID, headSeq *int64) (generationOutcomeSummary, error) {
	run, history, err := (generationOutcomeRepository{}).capturedHistory(ctx, q, runID, residentID, headSeq)
	if err != nil {
		return generationOutcomeSummary{}, err
	}
	if len(history) == 0 {
		return generationOutcomeSummary{}, fmt.Errorf("%w: run %s has zero outcomes", ErrInvalidGenerationOutcomeHistory, run.RunID)
	}
	latest, err := reduceGenerationOutcomeHistory(history)
	if err != nil {
		return generationOutcomeSummary{}, fmt.Errorf("%w: run %s: %v", ErrInvalidGenerationOutcomeHistory, run.RunID, err)
	}
	summary := generationOutcomeSummary{RunID: run.RunID, ResidentID: run.ResidentID,
		InputCount: run.InputCount, Latest: latest, PersistedState: persistedWorkState(latest.State)}
	if run.InputCount < 1 && latest.AttemptNo != 0 {
		return generationOutcomeSummary{}, invalidOutcome(latest, "normal run has no generation inputs")
	}
	if latest.AttemptNo == 0 && run.InputCount != 0 {
		return generationOutcomeSummary{}, invalidOutcome(latest, "synthetic cancellation has generation inputs")
	}
	for _, outcome := range history {
		if outcome.State != "failed" && outcome.State != "cancelled" {
			continue
		}
		// Attempt zero is the typed, pre-dispatch synthetic cancellation
		// envelope. It never consumed provider/retry budget.
		if outcome.AttemptNo == 0 {
			continue
		}
		if !outcome.ErrorClass.Valid {
			return generationOutcomeSummary{}, fmt.Errorf("generation outcome %s has no error class", outcome.ID)
		}
		code, err := generation.ParseOutcomeErrorCode(outcome.ErrorClass.String)
		if err != nil {
			return generationOutcomeSummary{}, err
		}
		if code.Class() != generation.ErrorForegroundPreempted {
			summary.ChargedRetryCount++
		}
		if code.Class() == generation.ErrorForegroundPreempted {
			summary.HasForegroundPreemptedAttempt = true
		}
	}
	summary.RetryCount = summary.ChargedRetryCount
	return summary, nil
}

func persistedWorkState(state string) domain.WorkState {
	switch state {
	case "running":
		return domain.WorkRunning
	case "succeeded":
		return domain.WorkSucceeded
	default:
		return domain.WorkTerminalFailed
	}
}

type capturedGenerationRun struct {
	RunID      canonical.ID
	ResidentID canonical.ID
	InputCount int64
}

func (generationOutcomeRepository) capturedHistory(ctx context.Context, q generationOutcomeQueryer, runID, residentID canonical.ID, headSeq *int64) (capturedGenerationRun, []generationOutcome, error) {
	query := `WITH input_counts AS (
		SELECT input.generation_run_id, COUNT(*) AS input_count
		FROM generation_run_inputs input
		JOIN canonical_commits input_commit ON input_commit.canonical_commit_id = input.canonical_commit_id`
	args := []any{}
	if headSeq != nil {
		query += ` WHERE input_commit.commit_seq <= ?`
		args = append(args, *headSeq)
	}
	query += ` GROUP BY input.generation_run_id
	)
	SELECT run.generation_run_id, run.resident_id, COALESCE(input_counts.input_count, 0),
		run_commit.commit_seq,
		outcome.outcome_id, outcome.canonical_commit_id, outcome.attempt_no, outcome.state,
		outcome.output_content_id, outcome.prompt_tokens, outcome.completion_tokens,
		outcome.latency, outcome.estimated_cost, outcome.error_class,
		outcome.error_detail_content_id, outcome.recorded_at, outcome.recorded_tz
	FROM generation_runs run
	JOIN canonical_commits run_commit ON run_commit.canonical_commit_id = run.canonical_commit_id
	LEFT JOIN input_counts ON input_counts.generation_run_id = run.generation_run_id
	LEFT JOIN generation_run_outcomes outcome ON outcome.generation_run_id = run.generation_run_id
	LEFT JOIN canonical_commits outcome_commit ON outcome_commit.canonical_commit_id = outcome.canonical_commit_id
	WHERE run.generation_run_id = ? AND run.resident_id = ?`
	args = append(args, runID.String(), residentID.String())
	if headSeq != nil {
		query += ` AND run_commit.commit_seq <= ? AND (outcome.outcome_id IS NULL OR outcome_commit.commit_seq <= ?)`
		args = append(args, *headSeq, *headSeq)
	}
	query += ` ORDER BY outcome.attempt_no DESC,
		CASE WHEN outcome.state IN ('succeeded','failed','cancelled') THEN 1 ELSE 0 END DESC,
		outcome.outcome_id ASC`
	rows, err := q.QueryContext(ctx, query, args...)
	if err != nil {
		return capturedGenerationRun{}, nil, err
	}
	defer rows.Close()

	var run capturedGenerationRun
	var history []generationOutcome
	seenRun := false
	for rows.Next() {
		var rawRun, rawResident string
		var runCommitSeq int64
		var inputCount int64
		var rawID, rawCommit, rawState, rawOutput, rawErrorDetail, rawRecordedTZ sql.NullString
		var rawAttempt, rawRecordedAt sql.NullInt64
		var outcome generationOutcome
		if err := rows.Scan(&rawRun, &rawResident, &inputCount, &runCommitSeq,
			&rawID, &rawCommit, &rawAttempt, &rawState, &rawOutput,
			&outcome.PromptTokens, &outcome.CompletionTokens, &outcome.Latency,
			&outcome.EstimatedCost, &outcome.ErrorClass, &rawErrorDetail,
			&rawRecordedAt, &rawRecordedTZ); err != nil {
			return capturedGenerationRun{}, nil, err
		}
		if !seenRun {
			parsedRun, parseErr := canonical.ParseID(rawRun)
			if parseErr != nil {
				return capturedGenerationRun{}, nil, parseErr
			}
			parsedResident, parseErr := canonical.ParseID(rawResident)
			if parseErr != nil {
				return capturedGenerationRun{}, nil, parseErr
			}
			if runCommitSeq <= 0 || inputCount < 0 {
				return capturedGenerationRun{}, nil, fmt.Errorf("%w: invalid run envelope", ErrInvalidGenerationOutcomeHistory)
			}
			run = capturedGenerationRun{RunID: parsedRun, ResidentID: parsedResident, InputCount: inputCount}
			seenRun = true
		}
		if !rawID.Valid {
			continue
		}
		if !rawAttempt.Valid || !rawState.Valid || !rawRecordedAt.Valid || !rawRecordedTZ.Valid {
			return capturedGenerationRun{}, nil, invalidOutcome(outcome, "outcome row has null required fields")
		}
		outcome.AttemptNo = rawAttempt.Int64
		outcome.State = rawState.String
		outcome.RecordedAt = canonical.Instant(rawRecordedAt.Int64)
		outcome.RecordedTZ = canonical.Timezone(rawRecordedTZ.String)
		parsedID, parseErr := canonical.ParseID(rawID.String)
		if parseErr != nil {
			return capturedGenerationRun{}, nil, invalidOutcome(outcome, "parse outcome id: "+parseErr.Error())
		}
		outcome.ID = parsedID
		parsedCommit, parseErr := canonical.ParseID(rawCommit.String)
		if parseErr != nil {
			return capturedGenerationRun{}, nil, invalidOutcome(outcome, "parse canonical commit: "+parseErr.Error())
		}
		outcome.CanonicalCommitID = parsedCommit
		if rawOutput.Valid {
			parsedOutput, parseErr := canonical.ParseID(rawOutput.String)
			if parseErr != nil {
				return capturedGenerationRun{}, nil, invalidOutcome(outcome, "parse output content: "+parseErr.Error())
			}
			outcome.OutputContentID = &parsedOutput
		}
		if rawErrorDetail.Valid {
			parsedDetail, parseErr := canonical.ParseID(rawErrorDetail.String)
			if parseErr != nil {
				return capturedGenerationRun{}, nil, invalidOutcome(outcome, "parse error detail content: "+parseErr.Error())
			}
			outcome.ErrorDetailContentID = &parsedDetail
		}
		if err := validateGenerationOutcomeRow(outcome); err != nil {
			return capturedGenerationRun{}, nil, err
		}
		history = append(history, outcome)
	}
	if err := rows.Err(); err != nil {
		return capturedGenerationRun{}, nil, err
	}
	if !seenRun {
		return capturedGenerationRun{}, nil, sql.ErrNoRows
	}
	return run, history, nil
}

func (generationOutcomeRepository) readHistory(ctx context.Context, q generationOutcomeQueryer, runID, residentID canonical.ID, headSeq *int64) ([]generationOutcome, error) {
	_, history, err := (generationOutcomeRepository{}).capturedHistory(ctx, q, runID, residentID, headSeq)
	return history, err
}

func invalidOutcome(outcome generationOutcome, reason string) error {
	return fmt.Errorf("%w: outcome %s: %s", ErrInvalidGenerationOutcomeHistory, outcome.ID, reason)
}

func validateGenerationOutcomeRow(outcome generationOutcome) error {
	if outcome.ID.IsZero() || outcome.CanonicalCommitID.IsZero() || outcome.RecordedAt <= 0 {
		return invalidOutcome(outcome, "missing identity or recorded time")
	}
	if _, err := canonical.ParseTimezone(outcome.RecordedTZ.String()); err != nil {
		return invalidOutcome(outcome, "invalid recorded timezone")
	}
	if outcome.OutputContentID != nil && outcome.OutputContentID.IsZero() ||
		outcome.ErrorDetailContentID != nil && outcome.ErrorDetailContentID.IsZero() {
		return invalidOutcome(outcome, "invalid content identity")
	}
	for name, metric := range map[string]sql.NullInt64{
		"prompt_tokens": outcome.PromptTokens, "completion_tokens": outcome.CompletionTokens,
		"latency": outcome.Latency, "estimated_cost": outcome.EstimatedCost,
	} {
		if metric.Valid && metric.Int64 < 0 {
			return invalidOutcome(outcome, name+" is negative")
		}
	}
	if outcome.AttemptNo == 0 {
		if outcome.State != "cancelled" || outcome.OutputContentID != nil || outcome.ErrorDetailContentID != nil ||
			outcome.PromptTokens.Valid || outcome.CompletionTokens.Valid || outcome.Latency.Valid || outcome.EstimatedCost.Valid ||
			!outcome.ErrorClass.Valid {
			return invalidOutcome(outcome, "illegal synthetic cancellation row")
		}
		code, err := generation.ParseOutcomeErrorCode(outcome.ErrorClass.String)
		if err != nil || (code.Class() != generation.ErrorSourceContentErased &&
			code.Class() != generation.ErrorResidentInactive && code.Class() != generation.ErrorResidentUnselected) {
			return invalidOutcome(outcome, "invalid synthetic cancellation code")
		}
		return nil
	}
	if outcome.AttemptNo < 1 {
		return invalidOutcome(outcome, "invalid attempt number")
	}
	switch outcome.State {
	case "running":
		if outcome.OutputContentID != nil || outcome.ErrorClass.Valid || outcome.ErrorDetailContentID != nil ||
			outcome.PromptTokens.Valid || outcome.CompletionTokens.Valid || outcome.Latency.Valid || outcome.EstimatedCost.Valid {
			return invalidOutcome(outcome, "running row has terminal fields")
		}
	case "succeeded":
		if outcome.OutputContentID == nil || outcome.ErrorClass.Valid || outcome.ErrorDetailContentID != nil {
			return invalidOutcome(outcome, "succeeded row has illegal fields")
		}
	case "failed", "cancelled":
		if outcome.OutputContentID != nil || !outcome.ErrorClass.Valid ||
			outcome.PromptTokens.Valid || outcome.CompletionTokens.Valid || outcome.Latency.Valid || outcome.EstimatedCost.Valid {
			return invalidOutcome(outcome, "terminal failure row has illegal fields")
		}
		if _, err := generation.ParseOutcomeErrorCode(outcome.ErrorClass.String); err != nil {
			return invalidOutcome(outcome, "invalid terminal error code")
		}
	default:
		return invalidOutcome(outcome, "unknown state")
	}
	return nil
}

func reduceGenerationOutcomeHistory(ordered []generationOutcome) (generationOutcome, error) {
	if len(ordered) == 0 {
		return generationOutcome{}, sql.ErrNoRows
	}
	ordered = append([]generationOutcome(nil), ordered...)
	for _, outcome := range ordered {
		if outcome.AttemptNo != 0 {
			continue
		}
		// The cancellation-envelope contract permits exactly one synthetic
		// attempt zero when no generation attempt was ever dispatched. It is
		// not a retry history and must never be mixed with attempt >= 1.
		if len(ordered) != 1 || outcome.State != "cancelled" || !outcome.ErrorClass.Valid {
			return generationOutcome{}, fmt.Errorf("generation outcome history has invalid attempt zero")
		}
		code, err := generation.ParseOutcomeErrorCode(outcome.ErrorClass.String)
		if err != nil {
			return generationOutcome{}, fmt.Errorf("generation outcome %s error class: %w", outcome.ID, err)
		}
		switch code.Class() {
		case generation.ErrorSourceContentErased, generation.ErrorResidentInactive, generation.ErrorResidentUnselected:
			return outcome, nil
		default:
			return generationOutcome{}, fmt.Errorf("generation outcome history has invalid attempt zero cancellation class %q", code.Class())
		}
	}
	sort.SliceStable(ordered, func(left, right int) bool {
		if ordered[left].AttemptNo != ordered[right].AttemptNo {
			return ordered[left].AttemptNo > ordered[right].AttemptNo
		}
		leftTerminal := ordered[left].State != "running"
		rightTerminal := ordered[right].State != "running"
		if leftTerminal != rightTerminal {
			return leftTerminal
		}
		return ordered[left].ID.String() < ordered[right].ID.String()
	})
	// A retry history is contiguous and an older attempt must already be
	// terminal before a newer attempt exists. These checks make a malformed
	// history fatal instead of allowing each caller to derive a different state.
	maxAttempt := ordered[0].AttemptNo
	type attemptState struct {
		terminalCount int
		runningCount  int
		terminal      *generationOutcome
	}
	byAttempt := make(map[int64]attemptState, len(ordered))
	for _, outcome := range ordered {
		if outcome.AttemptNo < 1 {
			return generationOutcome{}, fmt.Errorf("generation outcome %s has invalid attempt %d", outcome.ID, outcome.AttemptNo)
		}
		if outcome.State != "running" && outcome.State != "succeeded" && outcome.State != "failed" && outcome.State != "cancelled" {
			return generationOutcome{}, fmt.Errorf("generation outcome %s has unknown state %q", outcome.ID, outcome.State)
		}
		group := byAttempt[outcome.AttemptNo]
		if outcome.State == "running" {
			group.runningCount++
		} else {
			group.terminalCount++
			copy := outcome
			group.terminal = &copy
		}
		byAttempt[outcome.AttemptNo] = group
	}
	attempts := make([]int64, 0, len(byAttempt))
	for attempt := range byAttempt {
		attempts = append(attempts, attempt)
	}
	sort.Slice(attempts, func(left, right int) bool { return attempts[left] < attempts[right] })
	for index, attempt := range attempts {
		expected := int64(index) + 1
		if attempt != expected {
			return generationOutcome{}, fmt.Errorf("generation outcome history has attempt gap before %d", maxAttempt)
		}
		group, exists := byAttempt[attempt]
		if !exists {
			return generationOutcome{}, fmt.Errorf("generation outcome history has attempt gap before %d", maxAttempt)
		}
		if group.runningCount > 1 || group.terminalCount > 1 {
			return generationOutcome{}, fmt.Errorf("generation outcome attempt %d has duplicate precedence rows", attempt)
		}
		if group.runningCount != 1 {
			return generationOutcome{}, fmt.Errorf("generation outcome attempt %d has no exactly-one running row", attempt)
		}
		if attempt == maxAttempt {
			continue
		}
		if group.terminal == nil {
			return generationOutcome{}, fmt.Errorf("generation outcome attempt %d remains running before attempt %d", attempt, maxAttempt)
		}
		if group.terminal.State == "succeeded" {
			return generationOutcome{}, fmt.Errorf("generation outcome attempt %d has illegal successor", attempt)
		}
		if !group.terminal.ErrorClass.Valid {
			return generationOutcome{}, fmt.Errorf("generation outcome attempt %d terminal row has no error class", attempt)
		}
		code, err := generation.ParseOutcomeErrorCode(group.terminal.ErrorClass.String)
		if err != nil {
			return generationOutcome{}, fmt.Errorf("generation outcome attempt %d error class: %w", attempt, err)
		}
		if code.Class() != generation.ErrorForegroundPreempted && !code.Retryable() {
			return generationOutcome{}, fmt.Errorf("generation outcome attempt %d has non-retryable successor", attempt)
		}
	}
	return ordered[0], nil
}
