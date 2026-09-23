package sqlite

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"mahoroba.local/mahoroba/internal/canonical"
	"mahoroba.local/mahoroba/internal/domain"
	"mahoroba.local/mahoroba/internal/integrity"
)

type cancellationEnvelopeSource struct {
	residentID   canonical.ID
	eventID      canonical.ID
	commitSeq    int64
	recordedAt   canonical.Instant
	recordedTZ   canonical.Timezone
	eventType    string
	contentState string
}

// loadCancellationEnvelopeCandidates covers every runless mandatory
// cancellation reason owned by RecoveryTerminalizer. Dialogue includes
// erased/inactive/unselected; memory extraction includes erased/inactive and
// is admitted only by the source event's frozen mandatory memory policy.
func loadCancellationEnvelopeCandidates(ctx context.Context, tx *sql.Tx) ([]integrity.CandidateInput, error) {
	rows, err := tx.QueryContext(ctx, `SELECT
		event.resident_id, event.event_id, source_commit.commit_seq,
		event.recorded_at, event.recorded_tz, event.event_type, content.erasure_state
	FROM events event
	JOIN canonical_commits source_commit ON source_commit.canonical_commit_id = event.canonical_commit_id
	JOIN content_objects content ON content.content_id = event.content_id
	WHERE event.event_type IN ('user_message', 'self_talk')
	ORDER BY event.resident_id, event.seq, event.event_id`)
	if err != nil {
		return nil, fmt.Errorf("sqlite: scan cancellation-envelope sources: %w", err)
	}
	defer rows.Close()
	sources := make([]cancellationEnvelopeSource, 0)
	for rows.Next() {
		source, err := scanCancellationEnvelopeSource(rows)
		if err != nil {
			return nil, err
		}
		sources = append(sources, source)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("sqlite: iterate cancellation-envelope sources: %w", err)
	}
	if err := rows.Close(); err != nil {
		return nil, err
	}

	result := make([]integrity.CandidateInput, 0)
	for _, source := range sources {
		candidate, err := cancellationEnvelopeCandidateForSource(ctx, tx, source)
		if err != nil {
			return nil, err
		}
		if candidate != nil {
			result = append(result, *candidate)
		}
	}
	return result, nil
}

type cancellationEnvelopeSourceScanner interface {
	Scan(...any) error
}

func scanCancellationEnvelopeSource(row cancellationEnvelopeSourceScanner) (cancellationEnvelopeSource, error) {
	var source cancellationEnvelopeSource
	var residentRaw, eventRaw, timezoneRaw string
	var recordedAt int64
	if err := row.Scan(
		&residentRaw, &eventRaw, &source.commitSeq, &recordedAt, &timezoneRaw,
		&source.eventType, &source.contentState,
	); err != nil {
		return cancellationEnvelopeSource{}, err
	}
	var err error
	source.residentID, err = canonical.ParseID(residentRaw)
	if err != nil {
		return cancellationEnvelopeSource{}, err
	}
	source.eventID, err = canonical.ParseID(eventRaw)
	if err != nil {
		return cancellationEnvelopeSource{}, err
	}
	source.recordedTZ, err = canonical.ParseTimezone(timezoneRaw)
	if err != nil {
		return cancellationEnvelopeSource{}, err
	}
	source.recordedAt = canonical.Instant(recordedAt)
	return source, nil
}

func cancellationEnvelopeCandidateForSource(
	ctx context.Context,
	tx *sql.Tx,
	source cancellationEnvelopeSource,
) (*integrity.CandidateInput, error) {
	var status string
	statusResolved := true
	if err := tx.QueryRowContext(ctx, `SELECT transition.to_status
		FROM resident_status_transitions transition
		JOIN canonical_commits transition_commit
		  ON transition_commit.canonical_commit_id = transition.canonical_commit_id
		WHERE transition.resident_id = ?
		ORDER BY transition_commit.commit_seq DESC, transition.resident_status_transition_id DESC
		LIMIT 1`, source.residentID.String()).Scan(&status); err != nil {
		if !errors.Is(err, sql.ErrNoRows) {
			return nil, fmt.Errorf("sqlite: resolve cancellation-envelope resident status: %w", err)
		}
		statusResolved = false
	}
	sourceErased := source.contentState == "erased"
	if !sourceErased && !statusResolved {
		// An event attached to a resident without lifecycle history is diagnosed
		// by the resident invariant. It is not evidence of inactive/unselected
		// mandatory work and must not manufacture that reason.
		return nil, nil
	}
	var selected int
	if err := tx.QueryRowContext(ctx, `SELECT EXISTS(
		SELECT 1 FROM runtime_config
		WHERE singleton_id = 1 AND active_resident_id = ?
	)`, source.residentID.String()).Scan(&selected); err != nil {
		return nil, fmt.Errorf("sqlite: resolve cancellation-envelope selection: %w", err)
	}
	event := domain.Event{
		ID: source.eventID, ResidentID: source.residentID,
		RecordedAt: source.recordedAt, RecordedTZ: source.recordedTZ,
	}

	if source.eventType == "user_message" {
		reason := dialogueRecoveryReason(sourceErased, status, selected != 0)
		if reason != "" {
			key := domain.DialogueObligation(source.eventID)
			runless, err := cancellationObligationRunless(ctx, tx, source.residentID, key)
			if err != nil {
				return nil, err
			}
			if runless {
				_, resolvable, err := resolveSyntheticCancellationEnvelope(ctx, tx, domain.MandatoryRecoveryWork{
					Kind: domain.MandatoryRecoveryDialogue, SourceEvent: event,
					IdempotencyKey: key, CancellationCode: reason,
				})
				if err != nil {
					return nil, err
				}
				if !resolvable {
					return newCancellationEnvelopeCandidate(source), nil
				}
			}
		}
	}

	memoryReason := memoryRecoveryReason(sourceErased, status)
	if memoryReason == "" {
		return nil, nil
	}
	key := domain.MemoryExtractionObligation(source.eventID)
	runless, err := cancellationObligationRunless(ctx, tx, source.residentID, key)
	if err != nil {
		return nil, err
	}
	if !runless {
		return nil, nil
	}
	policy, err := resolveEventTimeMandatoryPolicy(ctx, tx, source.residentID, source.eventID, source.eventType)
	if err != nil {
		return nil, err
	}
	if !policy.Resolvable {
		return newCancellationEnvelopeCandidate(source), nil
	}
	if !policy.Mandatory {
		return nil, nil
	}
	_, resolvable, err := resolveSyntheticCancellationEnvelope(ctx, tx, domain.MandatoryRecoveryWork{
		Kind: domain.MandatoryRecoveryMemoryExtraction, SourceEvent: event,
		IdempotencyKey: key, PolicyRevisionID: policy.RevisionID, CancellationCode: memoryReason,
	})
	if err != nil {
		return nil, err
	}
	if !resolvable {
		return newCancellationEnvelopeCandidate(source), nil
	}
	return nil, nil
}

func cancellationObligationRunless(
	ctx context.Context,
	tx *sql.Tx,
	residentID canonical.ID,
	key string,
) (bool, error) {
	var count int
	if err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM generation_runs
		WHERE resident_id = ? AND idempotency_key = ?`, residentID.String(), key).Scan(&count); err != nil {
		return false, fmt.Errorf("sqlite: resolve cancellation-envelope generation run: %w", err)
	}
	return count == 0, nil
}

func newCancellationEnvelopeCandidate(source cancellationEnvelopeSource) *integrity.CandidateInput {
	return &integrity.CandidateInput{
		ResidentID: source.residentID,
		Kind:       integrity.FindingProvenanceUnresolvable,
		RuleCode:   integrity.RuleCancellationEnvelopeUnresolvable,
		TargetKind: integrity.TargetEvent, TargetID: source.eventID,
		TargetField: "cancellation_envelope",
		OccurredAt:  source.recordedAt, OccurredTZ: source.recordedTZ,
	}
}

func loadOneCancellationEnvelopeCandidate(
	ctx context.Context,
	tx *sql.Tx,
	residentID, eventID canonical.ID,
) (*integrity.CandidateInput, error) {
	row := tx.QueryRowContext(ctx, `SELECT
		event.resident_id, event.event_id, source_commit.commit_seq,
		event.recorded_at, event.recorded_tz, event.event_type, content.erasure_state
	FROM events event
	JOIN canonical_commits source_commit ON source_commit.canonical_commit_id = event.canonical_commit_id
	JOIN content_objects content ON content.content_id = event.content_id
	WHERE event.event_id = ? AND event.resident_id = ?
	  AND event.event_type IN ('user_message', 'self_talk')`, eventID.String(), residentID.String())
	source, err := scanCancellationEnvelopeSource(row)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return cancellationEnvelopeCandidateForSource(ctx, tx, source)
}
