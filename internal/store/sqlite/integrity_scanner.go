package sqlite

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"mahoroba.local/mahoroba/internal/canonical"
	"mahoroba.local/mahoroba/internal/integrity"
)

// IntegrityScanner reads through the query-only pool and never exposes a
// writer connection to the scanning phase.
func (s *Store) IntegrityScanner() *integrity.Scanner {
	return newSQLiteIntegrityScanner(s.reader)
}

func (inspection *Inspection) IntegrityScanner() *integrity.Scanner {
	return newSQLiteIntegrityScanner(inspection.store.reader)
}

func newSQLiteIntegrityScanner(reader *sql.DB) *integrity.Scanner {
	return integrity.NewScanner(&integrityScanSource{reader: reader})
}

type integrityScanSource struct {
	reader *sql.DB
}

func (source *integrityScanSource) CaptureIntegrityCandidates(
	ctx context.Context,
) (_ integrity.ScanSnapshot, resultErr error) {
	if source == nil || source.reader == nil {
		return integrity.ScanSnapshot{}, errors.New("sqlite: integrity scanner reader is required")
	}
	tx, err := source.reader.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return integrity.ScanSnapshot{}, fmt.Errorf("sqlite: begin integrity scan snapshot: %w", err)
	}
	defer func() {
		if resultErr != nil {
			_ = tx.Rollback()
		}
	}()

	head, err := captureIntegrityHead(ctx, tx)
	if err != nil {
		return integrity.ScanSnapshot{}, err
	}
	candidates := make([]integrity.CandidateInput, 0)
	loaders := []func(context.Context, *sql.Tx) ([]integrity.CandidateInput, error){
		loadErasedClaimStatementCandidates,
		loadQualifyingSupportCandidates,
		loadMissingGenerationSourceCandidates,
		loadErasedActiveRevisionCandidates,
		loadErasedRunningInputCandidates,
		loadCancellationEnvelopeCandidates,
	}
	for _, load := range loaders {
		loaded, err := load(ctx, tx)
		if err != nil {
			return integrity.ScanSnapshot{}, err
		}
		candidates = append(candidates, loaded...)
	}
	if err := tx.Commit(); err != nil {
		return integrity.ScanSnapshot{}, fmt.Errorf("sqlite: finish integrity scan snapshot: %w", err)
	}
	return integrity.ScanSnapshot{CapturedHead: head, Candidates: candidates}, nil
}

func captureIntegrityHead(ctx context.Context, tx *sql.Tx) (canonical.Head, error) {
	var seq, committedAt int64
	err := tx.QueryRowContext(ctx, `SELECT commit_seq, committed_at
		FROM canonical_commits ORDER BY commit_seq DESC LIMIT 1`).Scan(&seq, &committedAt)
	if errors.Is(err, sql.ErrNoRows) {
		return canonical.Head{}, nil
	}
	if err != nil {
		return canonical.Head{}, fmt.Errorf("sqlite: capture integrity scan head: %w", err)
	}
	commitSeq, err := canonical.NewCommitSeq(seq)
	if err != nil {
		return canonical.Head{}, err
	}
	return canonical.Head{Exists: true, CommitSeq: commitSeq, CommittedAt: canonical.Instant(committedAt)}, nil
}

func loadErasedClaimStatementCandidates(ctx context.Context, tx *sql.Tx) ([]integrity.CandidateInput, error) {
	rows, err := tx.QueryContext(ctx, `SELECT
		claim.owner_resident_id,
		claim.claim_id,
		erasure.content_erasure_event_id,
		erasure.occurred_at,
		erasure.occurred_tz
	FROM claims claim
	JOIN content_objects content ON content.content_id = claim.statement_content_id
	LEFT JOIN content_erasure_events erasure ON erasure.content_id = content.content_id
	WHERE content.erasure_state = 'erased'
	ORDER BY claim.owner_resident_id, claim.claim_id, erasure.content_erasure_event_id`)
	if err != nil {
		return nil, fmt.Errorf("sqlite: scan erased claim statements: %w", err)
	}
	defer rows.Close()
	var result []integrity.CandidateInput
	for rows.Next() {
		var residentRaw, claimRaw string
		var sourceRaw, timezoneRaw sql.NullString
		var occurredRaw sql.NullInt64
		if err := rows.Scan(&residentRaw, &claimRaw, &sourceRaw, &occurredRaw, &timezoneRaw); err != nil {
			return nil, fmt.Errorf("sqlite: scan erased claim statement candidate: %w", err)
		}
		if !sourceRaw.Valid || !occurredRaw.Valid || !timezoneRaw.Valid {
			return nil, fmt.Errorf("%w: erased claim statement %s has no attributable erasure event", integrity.ErrCandidateConflict, claimRaw)
		}
		residentID, claimID, sourceID, timezone, err := parseIntegrityCandidateIdentity(
			residentRaw, claimRaw, sourceRaw.String, timezoneRaw.String,
		)
		if err != nil {
			return nil, err
		}
		claimCopy, sourceCopy := claimID, sourceID
		result = append(result, integrity.CandidateInput{
			ResidentID: residentID, ClaimID: &claimCopy,
			Kind: integrity.FindingRequiredProvenanceErased, RuleCode: integrity.RuleClaimStatementErased,
			TargetKind: integrity.TargetClaim, TargetID: claimID, TargetField: "statement_content_id",
			SourceContentErasureEventID: &sourceCopy,
			OccurredAt:                  canonical.Instant(occurredRaw.Int64), OccurredTZ: timezone,
		})
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("sqlite: iterate erased claim statements: %w", err)
	}
	return result, nil
}

func loadMissingGenerationSourceCandidates(ctx context.Context, tx *sql.Tx) ([]integrity.CandidateInput, error) {
	rows, err := tx.QueryContext(ctx, `SELECT
		run.resident_id,
		input.generation_run_input_id,
		input.recorded_at,
		input.recorded_tz
	FROM generation_run_inputs input
	JOIN generation_runs run ON run.generation_run_id = input.generation_run_id
	WHERE CASE input.source_type
		WHEN 'runtime_projection' THEN input.source_id IS NOT NULL
		WHEN 'event' THEN input.source_id IS NULL OR NOT EXISTS (
			SELECT 1 FROM events source
			WHERE source.event_id = input.source_id AND source.resident_id = run.resident_id
		)
		WHEN 'claim' THEN input.source_id IS NULL OR NOT EXISTS (
			SELECT 1 FROM claims source
			WHERE source.claim_id = input.source_id AND source.owner_resident_id = run.resident_id
		)
		WHEN 'resident_revision' THEN input.source_id IS NULL OR NOT EXISTS (
			SELECT 1 FROM resident_revisions source
			WHERE source.revision_id = input.source_id AND source.resident_id = run.resident_id
		)
		ELSE 1
	END
	ORDER BY run.resident_id, input.generation_run_input_id`)
	if err != nil {
		return nil, fmt.Errorf("sqlite: scan missing generation sources: %w", err)
	}
	defer rows.Close()
	var result []integrity.CandidateInput
	for rows.Next() {
		var residentRaw, inputRaw, timezoneRaw string
		var recordedAt int64
		if err := rows.Scan(&residentRaw, &inputRaw, &recordedAt, &timezoneRaw); err != nil {
			return nil, fmt.Errorf("sqlite: scan missing generation source candidate: %w", err)
		}
		residentID, inputID, _, timezone, err := parseIntegrityCandidateIdentity(
			residentRaw, inputRaw, inputRaw, timezoneRaw,
		)
		if err != nil {
			return nil, err
		}
		result = append(result, integrity.CandidateInput{
			ResidentID: residentID,
			Kind:       integrity.FindingProvenanceUnresolvable, RuleCode: integrity.RuleGenerationInputSourceMissing,
			TargetKind: integrity.TargetGenerationInput, TargetID: inputID, TargetField: "source",
			OccurredAt: canonical.Instant(recordedAt), OccurredTZ: timezone,
		})
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("sqlite: iterate missing generation sources: %w", err)
	}
	return result, nil
}

func loadErasedActiveRevisionCandidates(ctx context.Context, tx *sql.Tx) ([]integrity.CandidateInput, error) {
	rows, err := tx.QueryContext(ctx, `SELECT
		revision.resident_id,
		revision.revision_id,
		erasure.content_erasure_event_id,
		erasure.occurred_at,
		erasure.occurred_tz
	FROM resident_revisions revision
	JOIN content_objects content ON content.content_id = revision.content_id
	JOIN resident_revision_activations activation ON activation.revision_id = revision.revision_id
	JOIN canonical_commits activation_commit ON activation_commit.canonical_commit_id = activation.canonical_commit_id
	LEFT JOIN content_erasure_events erasure ON erasure.content_id = content.content_id
	WHERE content.erasure_state = 'erased'
	  AND NOT EXISTS (
		SELECT 1
		FROM resident_revision_activations later
		JOIN resident_revisions later_revision ON later_revision.revision_id = later.revision_id
		JOIN canonical_commits later_commit ON later_commit.canonical_commit_id = later.canonical_commit_id
		WHERE later.resident_id = activation.resident_id
		  AND later_revision.revision_class = revision.revision_class
		  AND later_commit.commit_seq > activation_commit.commit_seq
	  )
	  AND EXISTS (
		SELECT 1
		FROM resident_status_transitions status
		JOIN canonical_commits status_commit ON status_commit.canonical_commit_id = status.canonical_commit_id
		WHERE status.resident_id = revision.resident_id
		  AND status.to_status = 'active'
		  AND NOT EXISTS (
			SELECT 1
			FROM resident_status_transitions later_status
			JOIN canonical_commits later_status_commit ON later_status_commit.canonical_commit_id = later_status.canonical_commit_id
			WHERE later_status.resident_id = status.resident_id
			  AND later_status_commit.commit_seq > status_commit.commit_seq
		  )
	  )
	ORDER BY revision.resident_id, revision.revision_id, erasure.content_erasure_event_id`)
	if err != nil {
		return nil, fmt.Errorf("sqlite: scan erased active revisions: %w", err)
	}
	defer rows.Close()
	return scanErasedNonClaimCandidates(rows, integrity.RuleActiveRequiredRevisionErased, integrity.TargetResidentRevision)
}

func loadErasedRunningInputCandidates(ctx context.Context, tx *sql.Tx) ([]integrity.CandidateInput, error) {
	rows, err := tx.QueryContext(ctx, `SELECT
		run.resident_id,
		input.generation_run_input_id,
		erasure.content_erasure_event_id,
		erasure.occurred_at,
		erasure.occurred_tz
	FROM generation_run_inputs input
	JOIN generation_runs run ON run.generation_run_id = input.generation_run_id
	JOIN generation_run_outcomes running ON running.generation_run_id = run.generation_run_id
	JOIN content_objects content ON content.content_id = input.content_id
	LEFT JOIN content_erasure_events erasure ON erasure.content_id = content.content_id
	WHERE running.state = 'running'
	  AND content.erasure_state = 'erased'
	  AND NOT EXISTS (
		SELECT 1 FROM generation_run_outcomes terminal
		WHERE terminal.generation_run_id = running.generation_run_id
		  AND terminal.attempt_no = running.attempt_no
		  AND terminal.state IN ('succeeded', 'failed', 'cancelled')
	  )
	ORDER BY run.resident_id, input.generation_run_input_id, erasure.content_erasure_event_id`)
	if err != nil {
		return nil, fmt.Errorf("sqlite: scan erased running inputs: %w", err)
	}
	defer rows.Close()
	return scanErasedNonClaimCandidates(rows, integrity.RuleRunningAttemptInputErased, integrity.TargetGenerationInput)
}

func scanErasedNonClaimCandidates(
	rows *sql.Rows,
	rule integrity.RuleCode,
	targetKind integrity.TargetKind,
) ([]integrity.CandidateInput, error) {
	var result []integrity.CandidateInput
	for rows.Next() {
		var residentRaw, targetRaw string
		var sourceRaw, timezoneRaw sql.NullString
		var occurredRaw sql.NullInt64
		if err := rows.Scan(&residentRaw, &targetRaw, &sourceRaw, &occurredRaw, &timezoneRaw); err != nil {
			return nil, fmt.Errorf("sqlite: scan erased integrity target: %w", err)
		}
		if !sourceRaw.Valid || !occurredRaw.Valid || !timezoneRaw.Valid {
			return nil, fmt.Errorf("%w: erased target %s has no attributable erasure event", integrity.ErrCandidateConflict, targetRaw)
		}
		residentID, targetID, sourceID, timezone, err := parseIntegrityCandidateIdentity(
			residentRaw, targetRaw, sourceRaw.String, timezoneRaw.String,
		)
		if err != nil {
			return nil, err
		}
		sourceCopy := sourceID
		result = append(result, integrity.CandidateInput{
			ResidentID: residentID,
			Kind:       integrity.FindingCanonicalInvariant, RuleCode: rule,
			TargetKind: targetKind, TargetID: targetID, TargetField: "content_id",
			SourceContentErasureEventID: &sourceCopy,
			OccurredAt:                  canonical.Instant(occurredRaw.Int64), OccurredTZ: timezone,
		})
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("sqlite: iterate erased integrity targets: %w", err)
	}
	return result, nil
}

func parseIntegrityCandidateIdentity(
	residentRaw, targetRaw, sourceRaw, timezoneRaw string,
) (canonical.ID, canonical.ID, canonical.ID, canonical.Timezone, error) {
	residentID, err := canonical.ParseID(residentRaw)
	if err != nil {
		return canonical.ID{}, canonical.ID{}, canonical.ID{}, "", err
	}
	targetID, err := canonical.ParseID(targetRaw)
	if err != nil {
		return canonical.ID{}, canonical.ID{}, canonical.ID{}, "", err
	}
	sourceID, err := canonical.ParseID(sourceRaw)
	if err != nil {
		return canonical.ID{}, canonical.ID{}, canonical.ID{}, "", err
	}
	timezone, err := canonical.ParseTimezone(timezoneRaw)
	if err != nil {
		return canonical.ID{}, canonical.ID{}, canonical.ID{}, "", err
	}
	return residentID, targetID, sourceID, timezone, nil
}

var _ integrity.ScanSource = (*integrityScanSource)(nil)
