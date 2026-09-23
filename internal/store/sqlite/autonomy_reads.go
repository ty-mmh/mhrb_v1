package sqlite

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"mahoroba.local/mahoroba/internal/autonomy"
	"mahoroba.local/mahoroba/internal/canonical"
	"mahoroba.local/mahoroba/internal/domain"
)

func (r *CanonicalRepository) AutonomySnapshot(
	ctx context.Context,
	request autonomy.SnapshotRequest,
) (autonomy.Snapshot, error) {
	if err := request.Validate(); err != nil {
		return autonomy.Snapshot{}, err
	}
	tx, err := r.store.reader.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return autonomy.Snapshot{}, fmt.Errorf("sqlite: begin autonomy snapshot: %w", err)
	}
	defer tx.Rollback()
	head, err := autonomyCapturedHead(ctx, tx)
	if err != nil {
		return autonomy.Snapshot{}, err
	}
	if !head.Exists {
		return autonomy.Snapshot{}, errors.New("sqlite: autonomy snapshot requires a Canonical head")
	}
	snapshot := autonomy.Snapshot{CapturedHead: head, ResidentID: request.ResidentID}
	headSeq := head.CommitSeq.Int64()
	if err := tx.QueryRowContext(ctx, `SELECT transition.to_status
		FROM resident_status_transitions transition
		JOIN canonical_commits commit_row ON commit_row.canonical_commit_id = transition.canonical_commit_id
		WHERE transition.resident_id = ? AND commit_row.commit_seq <= ?
		ORDER BY commit_row.commit_seq DESC, transition.resident_status_transition_id DESC LIMIT 1`,
		request.ResidentID.String(), headSeq).Scan(&snapshot.ResidentStatus); err != nil {
		return autonomy.Snapshot{}, fmt.Errorf("sqlite: resolve autonomy resident status: %w", err)
	}

	var policyContent []byte
	if err := tx.QueryRowContext(ctx, `SELECT blob.content
		FROM resident_revision_activations activation
		JOIN canonical_commits activation_commit
		  ON activation_commit.canonical_commit_id = activation.canonical_commit_id
		JOIN resident_revisions revision ON revision.revision_id = activation.revision_id
		JOIN content_objects content ON content.content_id = revision.content_id
		JOIN blobs blob ON blob.dedupe_scope_id = content.owner_resident_id
		 AND blob.hash_algorithm = content.blob_hash_algorithm AND blob.blob_hash = content.blob_hash
		WHERE activation.resident_id = ? AND revision.revision_class = 'memory_policy'
		  AND activation_commit.commit_seq <= ? AND content.erasure_state = 'present'
		ORDER BY activation_commit.commit_seq DESC, activation.activation_id DESC LIMIT 1`,
		request.ResidentID.String(), headSeq).Scan(&policyContent); err != nil {
		return autonomy.Snapshot{}, fmt.Errorf("sqlite: resolve autonomy memory policy: %w", err)
	}
	var policyHeader struct {
		Version string `json:"version"`
	}
	if err := json.Unmarshal(policyContent, &policyHeader); err != nil {
		return autonomy.Snapshot{}, fmt.Errorf("sqlite: decode autonomy memory policy version: %w", err)
	}
	if policyHeader.Version == "" {
		return autonomy.Snapshot{}, errors.New("sqlite: autonomy memory policy has no version")
	}
	snapshot.MemoryPolicyVersion = policyHeader.Version

	snapshot.LastUser, err = autonomyLatestEvent(ctx, tx, request.ResidentID, autonomy.EventUserMessage, headSeq)
	if err != nil {
		return autonomy.Snapshot{}, err
	}
	snapshot.LastSelfTalk, err = autonomyLatestEvent(ctx, tx, request.ResidentID, autonomy.EventSelfTalk, headSeq)
	if err != nil {
		return autonomy.Snapshot{}, err
	}
	snapshot.LastInitiative, err = autonomyLatestEvent(ctx, tx, request.ResidentID, autonomy.EventOutboundInitiative, headSeq)
	if err != nil {
		return autonomy.Snapshot{}, err
	}
	lastUserSeq := int64(0)
	if snapshot.LastUser != nil {
		lastUserSeq = snapshot.LastUser.Seq.Int64()
	}
	if err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM events event
		JOIN canonical_commits commit_row ON commit_row.canonical_commit_id = event.canonical_commit_id
		WHERE event.resident_id = ? AND event.event_type = 'self_talk'
		  AND event.seq > ? AND commit_row.commit_seq <= ?`,
		request.ResidentID.String(), lastUserSeq, headSeq).Scan(&snapshot.ConsecutiveSelfTalk); err != nil {
		return autonomy.Snapshot{}, fmt.Errorf("sqlite: count consecutive self-talk: %w", err)
	}
	location, err := time.LoadLocation(request.Timezone.String())
	if err != nil {
		return autonomy.Snapshot{}, err
	}
	hourStart, hourEnd, dayStart, dayEnd := autonomy.CalendarBounds(request.WallNow, location)
	for _, count := range []struct {
		eventType autonomy.EventType
		start     time.Time
		end       time.Time
		dst       *int64
		label     string
	}{
		{autonomy.EventSelfTalk, hourStart, hourEnd, &snapshot.SelfTalkHourCount, "self-talk hour"},
		{autonomy.EventSelfTalk, dayStart, dayEnd, &snapshot.SelfTalkDayCount, "self-talk day"},
		{autonomy.EventOutboundInitiative, hourStart, hourEnd, &snapshot.InitiativeHourCount, "initiative hour"},
		{autonomy.EventOutboundInitiative, dayStart, dayEnd, &snapshot.InitiativeDayCount, "initiative day"},
	} {
		if err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM events event
			JOIN canonical_commits commit_row ON commit_row.canonical_commit_id = event.canonical_commit_id
			WHERE event.resident_id = ? AND event.event_type = ?
			  AND event.recorded_at >= ? AND event.recorded_at < ? AND commit_row.commit_seq <= ?`,
			request.ResidentID.String(), string(count.eventType), count.start.UnixMicro(), count.end.UnixMicro(), headSeq,
		).Scan(count.dst); err != nil {
			return autonomy.Snapshot{}, fmt.Errorf("sqlite: count autonomy %s: %w", count.label, err)
		}
	}
	var latest sql.NullInt64
	if err := tx.QueryRowContext(ctx, `SELECT MAX(event.recorded_at) FROM events event
		JOIN canonical_commits commit_row ON commit_row.canonical_commit_id = event.canonical_commit_id
		WHERE event.resident_id = ? AND commit_row.commit_seq <= ?`, request.ResidentID.String(), headSeq).Scan(&latest); err != nil {
		return autonomy.Snapshot{}, fmt.Errorf("sqlite: resolve latest autonomy event time: %w", err)
	}
	if latest.Valid {
		snapshot.LatestRecordedAt = canonical.Instant(latest.Int64)
	}
	foregroundPending, err := foregroundDialoguePending(ctx, tx, request.ResidentID, headSeq, request.MaxAttempts)
	if err != nil {
		return autonomy.Snapshot{}, fmt.Errorf("sqlite: resolve foreground dialogue work: %w", err)
	}
	snapshot.ForegroundPending = foregroundPending
	if err := snapshot.Validate(); err != nil {
		return autonomy.Snapshot{}, err
	}
	return snapshot, nil
}

func foregroundDialoguePending(
	ctx context.Context,
	q generationOutcomeQueryer,
	residentID canonical.ID,
	headSeq int64,
	maxAttempts int,
) (bool, error) {
	if maxAttempts < 1 {
		return false, fmt.Errorf("sqlite: foreground dialogue max attempts must be positive")
	}
	rows, err := q.QueryContext(ctx, `SELECT event.event_id,
		CASE WHEN run.generation_run_id IS NOT NULL AND run_commit.commit_seq <= ?
			THEN run.generation_run_id END
		FROM events event
		JOIN canonical_commits event_commit ON event_commit.canonical_commit_id = event.canonical_commit_id
		LEFT JOIN generation_runs run
		  ON run.resident_id = event.resident_id
		 AND run.idempotency_key = ('dialogue:v1:' || event.event_id)
		 AND run.purpose = 'dialogue'
		LEFT JOIN canonical_commits run_commit ON run_commit.canonical_commit_id = run.canonical_commit_id
		WHERE event.resident_id = ? AND event.event_type = 'user_message'
		  AND event_commit.commit_seq <= ?
		ORDER BY event.seq`, headSeq, residentID.String(), headSeq)
	if err != nil {
		return false, err
	}
	missingRun := false
	var runIDs []canonical.ID
	for rows.Next() {
		var eventRaw, runRaw sql.NullString
		if err := rows.Scan(&eventRaw, &runRaw); err != nil {
			_ = rows.Close()
			return false, err
		}
		if !runRaw.Valid {
			missingRun = true
			continue
		}
		runID, err := canonical.ParseID(runRaw.String)
		if err != nil {
			_ = rows.Close()
			return false, err
		}
		runIDs = append(runIDs, runID)
	}
	if err := rows.Err(); err != nil {
		_ = rows.Close()
		return false, err
	}
	if err := rows.Close(); err != nil {
		return false, err
	}
	// Close the candidate cursor before reducer reads. This keeps the snapshot
	// consumer correct even when SQLite is configured with one reader
	// connection and removes an implicit connection-pool dependency.
	pending := missingRun
	for _, runID := range runIDs {
		summary, err := (generationOutcomeRepository{}).SummaryAtHead(ctx, q, runID, residentID, headSeq)
		if err != nil {
			return false, err
		}
		summary, err = summary.Classify(int64(maxAttempts))
		if err != nil {
			return false, err
		}
		if summary.ClassifiedState == domain.WorkRunning {
			pending = true
			continue
		}
		if summary.ClassifiedState != domain.WorkRetryPending && summary.ClassifiedState != domain.WorkTerminalFailed {
			continue
		}
		if summary.RetryEligible {
			pending = true
		}
	}
	return pending, nil
}

func autonomyLatestEvent(
	ctx context.Context,
	tx *sql.Tx,
	residentID canonical.ID,
	eventType autonomy.EventType,
	headSeq int64,
) (*autonomy.EventPoint, error) {
	var eventRaw string
	var seq, recordedAt int64
	err := tx.QueryRowContext(ctx, `SELECT event.event_id, event.seq, event.recorded_at
		FROM events event
		JOIN canonical_commits commit_row ON commit_row.canonical_commit_id = event.canonical_commit_id
		WHERE event.resident_id = ? AND event.event_type = ? AND commit_row.commit_seq <= ?
		ORDER BY event.seq DESC LIMIT 1`, residentID.String(), string(eventType), headSeq).Scan(&eventRaw, &seq, &recordedAt)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("sqlite: resolve latest %s event: %w", eventType, err)
	}
	id, err := canonical.ParseID(eventRaw)
	if err != nil {
		return nil, err
	}
	canonicalSeq, err := canonical.NewSeq(seq)
	if err != nil {
		return nil, err
	}
	return &autonomy.EventPoint{ID: id, Seq: canonicalSeq, RecordedAt: canonical.Instant(recordedAt)}, nil
}

func (r *CanonicalRepository) RetentionSources(
	ctx context.Context,
	request autonomy.RetentionRequest,
) (autonomy.RetentionCapture, error) {
	if err := request.Validate(); err != nil {
		return autonomy.RetentionCapture{}, err
	}
	tx, err := r.store.reader.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return autonomy.RetentionCapture{}, fmt.Errorf("sqlite: begin retention source scan: %w", err)
	}
	defer tx.Rollback()
	head, err := autonomyCapturedHead(ctx, tx)
	if err != nil {
		return autonomy.RetentionCapture{}, err
	}
	capture := autonomy.RetentionCapture{CapturedHead: head}
	if !head.Exists {
		return capture, nil
	}
	headSeq := head.CommitSeq.Int64()
	outputReferenceCounts, err := (generationOutcomeRepository{}).OutputReferenceCounts(ctx, tx, headSeq)
	if err != nil {
		return autonomy.RetentionCapture{}, fmt.Errorf("sqlite: query generation output references: %w", err)
	}
	rows, err := tx.QueryContext(ctx, `SELECT
		event.resident_id, event.event_id, event.content_id, event.recorded_at,
		content.erasure_state, content.erasure_policy,
		(SELECT COUNT(*) FROM claim_evidence evidence
		 JOIN canonical_commits evidence_commit ON evidence_commit.canonical_commit_id = evidence.canonical_commit_id
		 WHERE evidence.event_id = event.event_id AND evidence_commit.commit_seq <= ?),
		(SELECT COUNT(DISTINCT evidence.claim_id) FROM claim_evidence evidence
		 JOIN canonical_commits evidence_commit ON evidence_commit.canonical_commit_id = evidence.canonical_commit_id
		 WHERE evidence.event_id = event.event_id AND evidence_commit.commit_seq <= ?),
		(SELECT COUNT(DISTINCT input.generation_run_id) FROM generation_run_inputs input
			JOIN canonical_commits input_commit ON input_commit.canonical_commit_id = input.canonical_commit_id
			WHERE input.source_type = 'event' AND input.source_id = event.event_id AND input_commit.commit_seq <= ?)
		FROM events event
		JOIN canonical_commits event_commit ON event_commit.canonical_commit_id = event.canonical_commit_id
		JOIN content_objects content ON content.content_id = event.content_id
		WHERE event.resident_id = ? AND event.event_type = 'self_talk' AND event_commit.commit_seq <= ?
		ORDER BY event.recorded_at, event.event_id`,
		headSeq, headSeq, headSeq, request.ResidentID.String(), headSeq)
	if err != nil {
		return autonomy.RetentionCapture{}, fmt.Errorf("sqlite: query retention sources: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var source autonomy.RetentionSource
		var residentRaw, eventRaw, contentRaw, erasureState string
		if err := rows.Scan(
			&residentRaw, &eventRaw, &contentRaw, &source.RecordedAt, &erasureState, &source.ErasurePolicy,
			&source.References.EvidenceReferences, &source.References.ClaimReferences,
			&source.References.GenerationInputReferences,
		); err != nil {
			return autonomy.RetentionCapture{}, err
		}
		if source.ResidentID, err = canonical.ParseID(residentRaw); err != nil {
			return autonomy.RetentionCapture{}, err
		}
		if source.EventID, err = canonical.ParseID(eventRaw); err != nil {
			return autonomy.RetentionCapture{}, err
		}
		if source.ContentID, err = canonical.ParseID(contentRaw); err != nil {
			return autonomy.RetentionCapture{}, err
		}
		source.References.GenerationOutputReferences = outputReferenceCounts[contentRaw]
		source.EventType = autonomy.EventSelfTalk
		source.ContentPresent = erasureState == canonical.ContentErasurePresent
		capture.Sources = append(capture.Sources, source)
	}
	if err := rows.Err(); err != nil {
		return autonomy.RetentionCapture{}, err
	}
	return capture, nil
}

func autonomyCapturedHead(ctx context.Context, tx *sql.Tx) (canonical.Head, error) {
	var seq, committedAt int64
	err := tx.QueryRowContext(ctx, `SELECT commit_seq, committed_at FROM canonical_commits ORDER BY commit_seq DESC LIMIT 1`).Scan(&seq, &committedAt)
	if errors.Is(err, sql.ErrNoRows) {
		return canonical.Head{}, nil
	}
	if err != nil {
		return canonical.Head{}, fmt.Errorf("sqlite: capture autonomy head: %w", err)
	}
	commitSeq, err := canonical.NewCommitSeq(seq)
	if err != nil {
		return canonical.Head{}, err
	}
	return canonical.Head{Exists: true, CommitSeq: commitSeq, CommittedAt: canonical.Instant(committedAt)}, nil
}

var _ autonomy.Source = (*CanonicalRepository)(nil)
