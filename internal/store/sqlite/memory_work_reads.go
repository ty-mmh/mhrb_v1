package sqlite

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"

	"mahoroba.local/mahoroba/internal/canonical"
	"mahoroba.local/mahoroba/internal/domain"
	"mahoroba.local/mahoroba/internal/generation"
	"mahoroba.local/mahoroba/internal/memory"
)

func (r *CanonicalRepository) PipelineVersion(
	ctx context.Context,
	kind, versionKey string,
) (domain.PipelineVersionDefinition, error) {
	if kind == "" || versionKey == "" {
		return domain.PipelineVersionDefinition{}, errors.New("sqlite: pipeline kind and version key are required")
	}
	var rawID, rawDefinition string
	err := r.store.reader.QueryRowContext(ctx, `SELECT pipeline_version_id, definition
		FROM pipeline_versions WHERE pipeline_kind = ? AND version_key = ?`, kind, versionKey).Scan(&rawID, &rawDefinition)
	if errors.Is(err, sql.ErrNoRows) {
		return domain.PipelineVersionDefinition{}, fmt.Errorf("sqlite: pipeline %s/%s is unavailable", kind, versionKey)
	}
	if err != nil {
		return domain.PipelineVersionDefinition{}, fmt.Errorf("sqlite: load pipeline version: %w", err)
	}
	id, err := canonical.ParseID(rawID)
	if err != nil {
		return domain.PipelineVersionDefinition{}, err
	}
	definition, err := canonical.ParseCanonicalJSON([]byte(rawDefinition))
	if err != nil {
		return domain.PipelineVersionDefinition{}, fmt.Errorf("sqlite: pipeline definition is not canonical: %w", err)
	}
	return domain.PipelineVersionDefinition{
		ID: id, Kind: kind, VersionKey: versionKey, Definition: definition,
	}, nil
}

func (r *CanonicalRepository) DiscoverMemoryExtractionWork(
	ctx context.Context,
	residentID canonical.ID,
	request domain.MemoryDiscoveryRequest,
) (domain.MemoryDiscoveryResult, error) {
	if err := residentID.Validate(); err != nil {
		return domain.MemoryDiscoveryResult{}, fmt.Errorf("sqlite: invalid memory discovery resident: %w", err)
	}
	if err := request.Validate(); err != nil {
		return domain.MemoryDiscoveryResult{}, fmt.Errorf("sqlite: invalid memory discovery request: %w", err)
	}

	scanCtx, cancel := context.WithTimeout(ctx, request.Budget.Elapsed)
	defer cancel()

	result := domain.MemoryDiscoveryResult{}
	initialCursor := cloneMemoryDiscoveryCursor(request.Cursor)
	resident, err := r.Resident(scanCtx, residentID)
	if err != nil {
		return boundedMemoryDiscoveryFailure(ctx, scanCtx, result, initialCursor, nil, err)
	}
	cursor, empty, err := r.startMemoryDiscoveryCursor(scanCtx, residentID, request)
	if err != nil {
		return boundedMemoryDiscoveryFailure(ctx, scanCtx, result, initialCursor, nil, err)
	}
	if empty {
		result.CycleComplete = true
		return result, nil
	}

	for result.CandidatesScanned < request.Budget.Candidates &&
		result.PageQueries < request.Budget.Pages {
		if err := scanCtx.Err(); err != nil {
			return boundedMemoryDiscoveryFailure(ctx, scanCtx, result, initialCursor, cursor, err)
		}

		remaining := request.Budget.Candidates - result.CandidatesScanned
		pageLimit := min(request.Budget.PageSize, remaining)
		events, err := r.memoryEvidenceEventAscendingBatch(
			scanCtx, residentID, cursor.AfterSeq, cursor.CycleThroughSeq, pageLimit,
		)
		if err != nil {
			return boundedMemoryDiscoveryFailure(ctx, scanCtx, result, initialCursor, cursor, err)
		}
		result.PageQueries++
		if len(events) == 0 {
			result.CycleComplete = true
			return result, nil
		}

		for _, event := range events {
			if err := scanCtx.Err(); err != nil {
				return boundedMemoryDiscoveryFailure(ctx, scanCtx, result, initialCursor, cursor, err)
			}
			revision, err := r.ActiveRevisionForEvent(scanCtx, residentID, "memory_policy", event.ID)
			if err != nil {
				return boundedMemoryDiscoveryFailure(ctx, scanCtx, result, initialCursor, cursor, err)
			}
			if revision.ContentErased {
				return boundedMemoryDiscoveryFailure(ctx, scanCtx, result, initialCursor, cursor, fmt.Errorf(
					"sqlite: event-time memory policy %s is erased", revision.RevisionID,
				))
			}
			policy, _, err := memory.ParsePolicy([]byte(revision.Content))
			if err != nil {
				return boundedMemoryDiscoveryFailure(ctx, scanCtx, result, initialCursor, cursor, fmt.Errorf(
					"sqlite: parse event-time memory policy %s: %w", revision.RevisionID, err,
				))
			}

			eventType := memory.EventType(event.Type)
			if err := policy.RequireEnabled(); err == nil && memoryPolicyRequires(policy, eventType) {
				if event.Type == string(memory.EventUserMessage) {
					prepared, err := r.dialogueCommitBExists(scanCtx, event)
					if err != nil {
						return boundedMemoryDiscoveryFailure(ctx, scanCtx, result, initialCursor, cursor, err)
					}
					if !prepared {
						result.DialogueDeferred++
						result.CandidatesScanned++
						cursor = memoryDiscoveryCursorAfter(event.Seq, cursor.CycleThroughSeq)
						continue
					}
				}

				work, err := r.classifyMemoryExtractionWork(
					scanCtx, event, revision.RevisionID, resident.Status, request.MaxAttempts,
				)
				if err != nil {
					return boundedMemoryDiscoveryFailure(ctx, scanCtx, result, initialCursor, cursor, err)
				}
				result.CandidatesScanned++
				cursor = memoryDiscoveryCursorAfter(event.Seq, cursor.CycleThroughSeq)
				if memoryWorkNeedsAttention(work) {
					result.Work = &work
					result.NextCursor = cursor
					return result, nil
				}
				continue
			}

			result.CandidatesScanned++
			cursor = memoryDiscoveryCursorAfter(event.Seq, cursor.CycleThroughSeq)
		}

		if len(events) < pageLimit || events[len(events)-1].Seq == cursor.CycleThroughSeq {
			result.CycleComplete = true
			return result, nil
		}
	}

	result.BudgetExhausted = true
	result.NextCursor = cursor
	return result, nil
}

func (r *CanonicalRepository) startMemoryDiscoveryCursor(
	ctx context.Context,
	residentID canonical.ID,
	request domain.MemoryDiscoveryRequest,
) (*domain.MemoryDiscoveryCursor, bool, error) {
	if request.Cursor != nil {
		if request.ThroughSeq != nil && request.Cursor.CycleThroughSeq > *request.ThroughSeq {
			return &domain.MemoryDiscoveryCursor{CycleThroughSeq: *request.ThroughSeq}, false, nil
		}
		return cloneMemoryDiscoveryCursor(request.Cursor), false, nil
	}
	if request.ThroughSeq != nil {
		return &domain.MemoryDiscoveryCursor{CycleThroughSeq: *request.ThroughSeq}, false, nil
	}

	var maximum sql.NullInt64
	err := r.store.reader.QueryRowContext(ctx, `SELECT MAX(e.seq) FROM events e
		WHERE e.resident_id = ? AND e.event_type IN ('user_message', 'self_talk')`,
		residentID.String()).Scan(&maximum)
	if err != nil {
		return nil, false, fmt.Errorf("sqlite: capture memory discovery cycle bound: %w", err)
	}
	if !maximum.Valid {
		return nil, true, nil
	}
	through := canonical.Seq(maximum.Int64)
	if err := through.Validate(); err != nil {
		return nil, false, fmt.Errorf("sqlite: invalid memory discovery cycle bound: %w", err)
	}
	return &domain.MemoryDiscoveryCursor{CycleThroughSeq: through}, false, nil
}

func cloneMemoryDiscoveryCursor(cursor *domain.MemoryDiscoveryCursor) *domain.MemoryDiscoveryCursor {
	if cursor == nil {
		return nil
	}
	result := &domain.MemoryDiscoveryCursor{CycleThroughSeq: cursor.CycleThroughSeq}
	if cursor.AfterSeq != nil {
		after := *cursor.AfterSeq
		result.AfterSeq = &after
	}
	return result
}

func memoryDiscoveryCursorAfter(after canonical.Seq, through canonical.Seq) *domain.MemoryDiscoveryCursor {
	return &domain.MemoryDiscoveryCursor{AfterSeq: &after, CycleThroughSeq: through}
}

func boundedMemoryDiscoveryFailure(
	parentCtx context.Context,
	scanCtx context.Context,
	progress domain.MemoryDiscoveryResult,
	initialCursor *domain.MemoryDiscoveryCursor,
	cursor *domain.MemoryDiscoveryCursor,
	cause error,
) (domain.MemoryDiscoveryResult, error) {
	if err := parentCtx.Err(); err != nil {
		return domain.MemoryDiscoveryResult{}, err
	}
	if errors.Is(scanCtx.Err(), context.DeadlineExceeded) {
		if cursor == nil || memoryDiscoveryCursorsEqual(initialCursor, cursor) {
			return domain.MemoryDiscoveryResult{}, fmt.Errorf(
				"sqlite: memory discovery deadline before cursor progress: %w", cause,
			)
		}
		progress.BudgetExhausted = true
		progress.NextCursor = cloneMemoryDiscoveryCursor(cursor)
		return progress, nil
	}
	return domain.MemoryDiscoveryResult{}, cause
}

func memoryDiscoveryCursorsEqual(
	left *domain.MemoryDiscoveryCursor,
	right *domain.MemoryDiscoveryCursor,
) bool {
	if left == nil || right == nil {
		return left == nil && right == nil
	}
	if left.CycleThroughSeq != right.CycleThroughSeq ||
		(left.AfterSeq == nil) != (right.AfterSeq == nil) {
		return false
	}
	return left.AfterSeq == nil || *left.AfterSeq == *right.AfterSeq
}

const memoryEvidenceEventAscendingQuery = `SELECT e.event_id, e.resident_id, e.seq, e.event_type,
		e.actor_principal_id, e.target_principal_id, e.generation_run_id, e.occurred_at,
		e.occurred_tz, e.recorded_at, e.recorded_tz, e.content_id, e.payload_commitment,
		e.prev_event_hash, e.event_hash, co.erasure_state, b.content
	FROM events e INDEXED BY idx_events_resident_type_seq
	JOIN content_objects co ON co.content_id = e.content_id
	LEFT JOIN blobs b ON b.dedupe_scope_id = co.owner_resident_id
	 AND b.hash_algorithm = co.blob_hash_algorithm AND b.blob_hash = co.blob_hash
	WHERE e.resident_id = ? AND e.event_type = 'user_message'
	  AND e.seq > ? AND e.seq <= ?
	UNION ALL
	SELECT e.event_id, e.resident_id, e.seq, e.event_type,
		e.actor_principal_id, e.target_principal_id, e.generation_run_id, e.occurred_at,
		e.occurred_tz, e.recorded_at, e.recorded_tz, e.content_id, e.payload_commitment,
		e.prev_event_hash, e.event_hash, co.erasure_state, b.content
	FROM events e INDEXED BY idx_events_resident_type_seq
	JOIN content_objects co ON co.content_id = e.content_id
	LEFT JOIN blobs b ON b.dedupe_scope_id = co.owner_resident_id
	 AND b.hash_algorithm = co.blob_hash_algorithm AND b.blob_hash = co.blob_hash
	WHERE e.resident_id = ? AND e.event_type = 'self_talk'
	  AND e.seq > ? AND e.seq <= ?
	ORDER BY seq ASC LIMIT ?`

func (r *CanonicalRepository) memoryEvidenceEventAscendingBatch(
	ctx context.Context,
	residentID canonical.ID,
	afterSeq *canonical.Seq,
	throughSeq canonical.Seq,
	limit int,
) ([]domain.Event, error) {
	after := int64(0)
	if afterSeq != nil {
		after = afterSeq.Int64()
	}
	rows, err := r.store.reader.QueryContext(ctx, memoryEvidenceEventAscendingQuery,
		residentID.String(), after, throughSeq.Int64(),
		residentID.String(), after, throughSeq.Int64(), limit,
	)
	if err != nil {
		return nil, fmt.Errorf("sqlite: discover bounded memory evidence events: %w", err)
	}
	defer rows.Close()
	var events []domain.Event
	for rows.Next() {
		event, err := scanEvent(rows)
		if err != nil {
			return nil, err
		}
		events = append(events, event)
	}
	return events, rows.Err()
}

func (r *CanonicalRepository) dialogueCommitBExists(ctx context.Context, event domain.Event) (bool, error) {
	var exists int
	err := r.store.reader.QueryRowContext(ctx, `SELECT EXISTS(
		SELECT 1 FROM generation_runs
		WHERE resident_id = ? AND purpose = 'dialogue' AND idempotency_key = ?
	)`, event.ResidentID.String(), domain.DialogueObligation(event.ID)).Scan(&exists)
	if err != nil {
		return false, fmt.Errorf("sqlite: check dialogue Commit B for memory source %s: %w", event.ID, err)
	}
	return exists != 0, nil
}

func (r *CanonicalRepository) DiscoverMemoryReextractionWork(
	ctx context.Context,
	residentID canonical.ID,
	request domain.MemoryReextractionDiscoveryRequest,
) (domain.MemoryReextractionDiscoveryResult, error) {
	if err := residentID.Validate(); err != nil {
		return domain.MemoryReextractionDiscoveryResult{}, fmt.Errorf(
			"sqlite: invalid memory re-extraction discovery resident: %w", err,
		)
	}
	if err := request.Validate(); err != nil {
		return domain.MemoryReextractionDiscoveryResult{}, fmt.Errorf(
			"sqlite: invalid memory re-extraction discovery request: %w", err,
		)
	}

	scanCtx, cancel := context.WithTimeout(ctx, request.Budget.Elapsed)
	defer cancel()
	result := domain.MemoryReextractionDiscoveryResult{}
	initialCursor := cloneMemoryReextractionDiscoveryCursor(request.Cursor)
	cursor, empty, err := r.startMemoryReextractionDiscoveryCursor(scanCtx, residentID, request)
	if err != nil {
		return boundedMemoryReextractionDiscoveryFailure(ctx, scanCtx, result, initialCursor, cursor, err)
	}
	if empty {
		result.CycleComplete = true
		return result, nil
	}
	resident, err := r.Resident(scanCtx, residentID)
	if err != nil {
		return boundedMemoryReextractionDiscoveryFailure(ctx, scanCtx, result, initialCursor, cursor, err)
	}

	for result.CandidatesScanned < request.Budget.Candidates &&
		result.PageQueries < request.Budget.Pages {
		if err := scanCtx.Err(); err != nil {
			return boundedMemoryReextractionDiscoveryFailure(ctx, scanCtx, result, initialCursor, cursor, err)
		}
		remaining := request.Budget.Candidates - result.CandidatesScanned
		pageLimit := min(request.Budget.PageSize, remaining)

		if cursor.ActiveCommit == nil {
			commits, err := r.memoryReextractionCommitAscendingBatch(
				scanCtx, residentID, cursor, pageLimit,
			)
			if err != nil {
				return boundedMemoryReextractionDiscoveryFailure(ctx, scanCtx, result, initialCursor, cursor, err)
			}
			result.PageQueries++
			if len(commits) == 0 {
				result.CycleComplete = true
				return result, nil
			}
			for _, commit := range commits {
				result.CandidatesScanned++
				if commit.hasRuns {
					cursor = memoryReextractionCursorAtCommit(cursor, commit)
					break
				}
				cursor = memoryReextractionCursorAfterCommit(cursor, commit.commitSeq)
			}
			if cursor.ActiveCommit == nil {
				last := commits[len(commits)-1]
				if len(commits) < pageLimit || last.commitSeq == cursor.CycleThroughCommitSeq {
					result.CycleComplete = true
					return result, nil
				}
				continue
			}
			continue
		}

		candidates, hasMore, err := r.memoryReextractionRunAscendingBatch(
			scanCtx, residentID, *cursor.ActiveCommit, pageLimit,
		)
		if err != nil {
			return boundedMemoryReextractionDiscoveryFailure(ctx, scanCtx, result, initialCursor, cursor, err)
		}
		result.PageQueries++
		for index, candidate := range candidates {
			if err := scanCtx.Err(); err != nil {
				return boundedMemoryReextractionDiscoveryFailure(ctx, scanCtx, result, initialCursor, cursor, err)
			}
			if !strings.HasPrefix(candidate.idempotencyKey, "memory_reextract:v1:") {
				result.CandidatesScanned++
				cursor = memoryReextractionCursorAfterRun(cursor, candidate.runID)
				continue
			}
			obligation, err := domain.ParseMemoryExtractionObligation(candidate.idempotencyKey)
			if err != nil {
				return boundedMemoryReextractionDiscoveryFailure(ctx, scanCtx, result, initialCursor, cursor, fmt.Errorf(
					"sqlite: invalid durable memory-extraction key %q: %w", candidate.idempotencyKey, err,
				))
			}
			event, err := r.Event(scanCtx, residentID, obligation.SourceEventID)
			if err != nil {
				return boundedMemoryReextractionDiscoveryFailure(ctx, scanCtx, result, initialCursor, cursor, err)
			}
			work, err := r.classifyMemoryExtractionWorkForKey(
				scanCtx, event, candidate.policyID, resident.Status, request.MaxAttempts, candidate.idempotencyKey,
			)
			if err != nil {
				return boundedMemoryReextractionDiscoveryFailure(ctx, scanCtx, result, initialCursor, cursor, err)
			}
			result.CandidatesScanned++
			cursor = memoryReextractionCursorAfterRun(cursor, candidate.runID)
			if memoryWorkNeedsAttention(work) {
				if !hasMore && index == len(candidates)-1 {
					cursor = memoryReextractionCursorAfterCommit(cursor, cursor.ActiveCommit.CommitSeq)
				}
				result.Work = &work
				result.NextCursor = cloneMemoryReextractionDiscoveryCursor(cursor)
				return result, nil
			}
		}
		if !hasMore {
			cursor = memoryReextractionCursorAfterCommit(cursor, cursor.ActiveCommit.CommitSeq)
		}
	}

	result.BudgetExhausted = true
	result.NextCursor = cloneMemoryReextractionDiscoveryCursor(cursor)
	return result, nil
}

type memoryReextractionCandidate struct {
	runID          canonical.ID
	idempotencyKey string
	policyID       canonical.ID
}

type memoryReextractionCommitProbe struct {
	commitID  canonical.ID
	commitSeq canonical.CommitSeq
	hasRuns   bool
}

const memoryReextractionCycleCeilingQuery = `SELECT commit_seq
	FROM canonical_commits INDEXED BY idx_canonical_commits_resident_seq
	WHERE resident_id = ? ORDER BY commit_seq DESC LIMIT 1`

const memoryReextractionCommitPageQuery = `SELECT commit_row.canonical_commit_id,
		commit_row.commit_seq,
		EXISTS(
			SELECT 1 FROM generation_runs run
			INDEXED BY idx_generation_runs_commit_resident_purpose_run
			WHERE run.canonical_commit_id = commit_row.canonical_commit_id
			  AND run.resident_id = commit_row.resident_id
			  AND run.purpose = 'memory_extraction'
		) AS has_runs
	FROM canonical_commits commit_row INDEXED BY idx_canonical_commits_resident_seq
	WHERE commit_row.resident_id = ? AND commit_row.commit_seq > ?
	  AND commit_row.commit_seq <= ?
	ORDER BY commit_row.commit_seq ASC LIMIT ?`

const memoryReextractionInitialRunPageQuery = `SELECT generation_run_id, idempotency_key,
		memory_policy_revision_id
	FROM generation_runs INDEXED BY idx_generation_runs_commit_resident_purpose_run
	WHERE canonical_commit_id = ? AND resident_id = ? AND purpose = 'memory_extraction'
	ORDER BY generation_run_id ASC LIMIT ?`

const memoryReextractionContinuationRunPageQuery = `SELECT generation_run_id, idempotency_key,
		memory_policy_revision_id
	FROM generation_runs INDEXED BY idx_generation_runs_commit_resident_purpose_run
	WHERE canonical_commit_id = ? AND resident_id = ? AND purpose = 'memory_extraction'
	  AND generation_run_id > ?
	ORDER BY generation_run_id ASC LIMIT ?`

func (r *CanonicalRepository) startMemoryReextractionDiscoveryCursor(
	ctx context.Context,
	residentID canonical.ID,
	request domain.MemoryReextractionDiscoveryRequest,
) (*domain.MemoryReextractionDiscoveryCursor, bool, error) {
	if request.Cursor != nil {
		return cloneMemoryReextractionDiscoveryCursor(request.Cursor), false, nil
	}
	var commitSeqRaw int64
	err := r.store.reader.QueryRowContext(ctx, memoryReextractionCycleCeilingQuery,
		residentID.String()).Scan(&commitSeqRaw)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, true, nil
	}
	if err != nil {
		return nil, false, fmt.Errorf("sqlite: capture memory re-extraction discovery cycle bound: %w", err)
	}
	commitSeq, err := canonical.NewCommitSeq(commitSeqRaw)
	if err != nil {
		return nil, false, fmt.Errorf("sqlite: parse memory re-extraction cycle-bound commit: %w", err)
	}
	return &domain.MemoryReextractionDiscoveryCursor{CycleThroughCommitSeq: commitSeq}, false, nil
}

func (r *CanonicalRepository) memoryReextractionCommitAscendingBatch(
	ctx context.Context,
	residentID canonical.ID,
	cursor *domain.MemoryReextractionDiscoveryCursor,
	limit int,
) ([]memoryReextractionCommitProbe, error) {
	after := int64(0)
	if cursor.CompletedThroughCommitSeq != nil {
		after = cursor.CompletedThroughCommitSeq.Int64()
	}
	rows, err := r.store.reader.QueryContext(ctx, memoryReextractionCommitPageQuery,
		residentID.String(), after, cursor.CycleThroughCommitSeq.Int64(), limit)
	if err != nil {
		return nil, fmt.Errorf("sqlite: discover bounded memory re-extraction commits: %w", err)
	}
	defer rows.Close()
	commits := make([]memoryReextractionCommitProbe, 0, limit)
	for rows.Next() {
		var commitRaw string
		var commitSeqRaw int64
		var hasRuns bool
		if err := rows.Scan(&commitRaw, &commitSeqRaw, &hasRuns); err != nil {
			return nil, err
		}
		commitID, err := canonical.ParseID(commitRaw)
		if err != nil {
			return nil, err
		}
		commitSeq, err := canonical.NewCommitSeq(commitSeqRaw)
		if err != nil {
			return nil, err
		}
		commits = append(commits, memoryReextractionCommitProbe{
			commitID: commitID, commitSeq: commitSeq, hasRuns: hasRuns,
		})
		if hasRuns {
			break
		}
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("sqlite: scan bounded memory re-extraction commits: %w", err)
	}
	return commits, nil
}

func (r *CanonicalRepository) memoryReextractionRunAscendingBatch(
	ctx context.Context,
	residentID canonical.ID,
	active domain.MemoryReextractionCommitCursor,
	limit int,
) ([]memoryReextractionCandidate, bool, error) {
	query := memoryReextractionInitialRunPageQuery
	args := []any{active.CommitID.String(), residentID.String()}
	if active.AfterRunID != nil {
		query = memoryReextractionContinuationRunPageQuery
		args = append(args, active.AfterRunID.String())
	}
	args = append(args, limit+1)
	rows, err := r.store.reader.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, false, fmt.Errorf("sqlite: discover bounded memory re-extraction runs: %w", err)
	}
	defer rows.Close()
	candidates := make([]memoryReextractionCandidate, 0, limit+1)
	for rows.Next() {
		var runRaw, key, policyRaw string
		if err := rows.Scan(&runRaw, &key, &policyRaw); err != nil {
			return nil, false, err
		}
		runID, err := canonical.ParseID(runRaw)
		if err != nil {
			return nil, false, err
		}
		policyID, err := canonical.ParseID(policyRaw)
		if err != nil {
			return nil, false, err
		}
		candidates = append(candidates, memoryReextractionCandidate{
			runID: runID, idempotencyKey: key, policyID: policyID,
		})
	}
	if err := rows.Err(); err != nil {
		return nil, false, fmt.Errorf("sqlite: scan bounded memory re-extraction runs: %w", err)
	}
	hasMore := len(candidates) > limit
	if hasMore {
		candidates = candidates[:limit]
	}
	return candidates, hasMore, nil
}

func cloneMemoryReextractionDiscoveryCursor(
	cursor *domain.MemoryReextractionDiscoveryCursor,
) *domain.MemoryReextractionDiscoveryCursor {
	if cursor == nil {
		return nil
	}
	result := &domain.MemoryReextractionDiscoveryCursor{
		CycleThroughCommitSeq: cursor.CycleThroughCommitSeq,
	}
	if cursor.CompletedThroughCommitSeq != nil {
		completed := *cursor.CompletedThroughCommitSeq
		result.CompletedThroughCommitSeq = &completed
	}
	if cursor.ActiveCommit != nil {
		active := *cursor.ActiveCommit
		if cursor.ActiveCommit.AfterRunID != nil {
			after := *cursor.ActiveCommit.AfterRunID
			active.AfterRunID = &after
		}
		result.ActiveCommit = &active
	}
	return result
}

func memoryReextractionCursorAtCommit(
	cursor *domain.MemoryReextractionDiscoveryCursor,
	commit memoryReextractionCommitProbe,
) *domain.MemoryReextractionDiscoveryCursor {
	result := cloneMemoryReextractionDiscoveryCursor(cursor)
	result.ActiveCommit = &domain.MemoryReextractionCommitCursor{
		CommitID: commit.commitID, CommitSeq: commit.commitSeq,
	}
	return result
}

func memoryReextractionCursorAfterRun(
	cursor *domain.MemoryReextractionDiscoveryCursor,
	runID canonical.ID,
) *domain.MemoryReextractionDiscoveryCursor {
	result := cloneMemoryReextractionDiscoveryCursor(cursor)
	after := runID
	result.ActiveCommit.AfterRunID = &after
	return result
}

func memoryReextractionCursorAfterCommit(
	cursor *domain.MemoryReextractionDiscoveryCursor,
	commitSeq canonical.CommitSeq,
) *domain.MemoryReextractionDiscoveryCursor {
	result := cloneMemoryReextractionDiscoveryCursor(cursor)
	completed := commitSeq
	result.CompletedThroughCommitSeq = &completed
	result.ActiveCommit = nil
	return result
}

func boundedMemoryReextractionDiscoveryFailure(
	parentCtx context.Context,
	scanCtx context.Context,
	progress domain.MemoryReextractionDiscoveryResult,
	initialCursor *domain.MemoryReextractionDiscoveryCursor,
	cursor *domain.MemoryReextractionDiscoveryCursor,
	cause error,
) (domain.MemoryReextractionDiscoveryResult, error) {
	if err := parentCtx.Err(); err != nil {
		return domain.MemoryReextractionDiscoveryResult{}, err
	}
	if errors.Is(scanCtx.Err(), context.DeadlineExceeded) {
		if cursor == nil || memoryReextractionCursorsEqual(initialCursor, cursor) {
			return domain.MemoryReextractionDiscoveryResult{}, fmt.Errorf(
				"sqlite: memory re-extraction deadline before cursor progress: %w", cause,
			)
		}
		progress.BudgetExhausted = true
		progress.NextCursor = cloneMemoryReextractionDiscoveryCursor(cursor)
		return progress, nil
	}
	return domain.MemoryReextractionDiscoveryResult{}, cause
}

func memoryReextractionCursorsEqual(
	left *domain.MemoryReextractionDiscoveryCursor,
	right *domain.MemoryReextractionDiscoveryCursor,
) bool {
	if left == nil || right == nil {
		return left == nil && right == nil
	}
	if left.CycleThroughCommitSeq != right.CycleThroughCommitSeq ||
		(left.CompletedThroughCommitSeq == nil) != (right.CompletedThroughCommitSeq == nil) ||
		(left.ActiveCommit == nil) != (right.ActiveCommit == nil) {
		return false
	}
	if left.CompletedThroughCommitSeq != nil &&
		*left.CompletedThroughCommitSeq != *right.CompletedThroughCommitSeq {
		return false
	}
	if left.ActiveCommit == nil {
		return true
	}
	if left.ActiveCommit.CommitID != right.ActiveCommit.CommitID ||
		left.ActiveCommit.CommitSeq != right.ActiveCommit.CommitSeq ||
		(left.ActiveCommit.AfterRunID == nil) != (right.ActiveCommit.AfterRunID == nil) {
		return false
	}
	return left.ActiveCommit.AfterRunID == nil ||
		*left.ActiveCommit.AfterRunID == *right.ActiveCommit.AfterRunID
}

func (r *CanonicalRepository) MemoryReextractionWork(
	ctx context.Context,
	residentID, eventID, requestID canonical.ID,
	maxAttempts int,
) (domain.MemoryExtractionWork, bool, error) {
	for _, id := range []canonical.ID{residentID, eventID, requestID} {
		if err := id.Validate(); err != nil {
			return domain.MemoryExtractionWork{}, false, err
		}
	}
	if maxAttempts < 1 {
		return domain.MemoryExtractionWork{}, false, nil
	}
	key := domain.MemoryReextractionObligation(eventID, requestID)
	var policyRaw string
	err := r.store.reader.QueryRowContext(ctx, `SELECT memory_policy_revision_id
		FROM generation_runs WHERE resident_id = ? AND idempotency_key = ?`,
		residentID.String(), key).Scan(&policyRaw)
	if errors.Is(err, sql.ErrNoRows) {
		return domain.MemoryExtractionWork{}, false, nil
	}
	if err != nil {
		return domain.MemoryExtractionWork{}, false, err
	}
	policyID, err := canonical.ParseID(policyRaw)
	if err != nil {
		return domain.MemoryExtractionWork{}, false, err
	}
	resident, err := r.Resident(ctx, residentID)
	if err != nil {
		return domain.MemoryExtractionWork{}, false, err
	}
	event, err := r.Event(ctx, residentID, eventID)
	if err != nil {
		return domain.MemoryExtractionWork{}, false, err
	}
	work, err := r.classifyMemoryExtractionWorkForKey(ctx, event, policyID, resident.Status, maxAttempts, key)
	return work, true, err
}

func memoryPolicyRequires(policy memory.Policy, eventType memory.EventType) bool {
	for _, required := range policy.MandatoryEventTypes {
		if required == eventType {
			return true
		}
	}
	return false
}

func (r *CanonicalRepository) classifyMemoryExtractionWork(
	ctx context.Context,
	event domain.Event,
	policyRevisionID canonical.ID,
	residentStatus string,
	maxAttempts int,
) (domain.MemoryExtractionWork, error) {
	return r.classifyMemoryExtractionWorkForKey(
		ctx, event, policyRevisionID, residentStatus, maxAttempts, domain.MemoryExtractionObligation(event.ID),
	)
}

func (r *CanonicalRepository) classifyMemoryExtractionWorkForKey(
	ctx context.Context,
	event domain.Event,
	policyRevisionID canonical.ID,
	residentStatus string,
	maxAttempts int,
	idempotencyKey string,
) (domain.MemoryExtractionWork, error) {
	work := domain.MemoryExtractionWork{
		SourceEvent: event, IdempotencyKey: idempotencyKey,
		PolicyRevisionID: policyRevisionID, State: domain.WorkPending,
	}
	if event.ContentErased {
		work.CancellationCode = generation.MustOutcomeErrorCode(generation.ErrorSourceContentErased, 0).String()
	} else if residentStatus != "active" {
		work.CancellationCode = generation.MustOutcomeErrorCode(generation.ErrorResidentInactive, 0).String()
	}
	var runRaw string
	err := r.store.reader.QueryRowContext(ctx, `SELECT generation_run_id FROM generation_runs
		WHERE resident_id = ? AND idempotency_key = ?`, event.ResidentID.String(), idempotencyKey).Scan(&runRaw)
	if errors.Is(err, sql.ErrNoRows) {
		return work, nil
	}
	if err != nil {
		return domain.MemoryExtractionWork{}, err
	}
	runID, err := canonical.ParseID(runRaw)
	if err != nil {
		return domain.MemoryExtractionWork{}, err
	}
	work.RunID = &runID
	summary, err := (generationOutcomeRepository{}).Summary(ctx, r.store.reader, runID, event.ResidentID)
	if err != nil {
		return domain.MemoryExtractionWork{}, err
	}
	summary, err = summary.Classify(int64(maxAttempts))
	if err != nil {
		return domain.MemoryExtractionWork{}, err
	}
	work.AttemptNo = summary.Latest.AttemptNo
	work.RetryCount = summary.RetryCount
	switch summary.ClassifiedState {
	case domain.WorkRunning:
		work.State = domain.WorkRunning
	case domain.WorkSucceeded:
		work.State = domain.WorkSucceeded
		work.CancellationCode = ""
	case domain.WorkRetryPending:
		if summary.LatestForegroundPreempted {
			work.State = domain.WorkRetryPending
			work.ForegroundPreempted = true
		} else if summary.RetryEligible {
			work.State = domain.WorkRetryPending
		} else if !summary.RetryEligible {
			work.State = domain.WorkTerminalFailed
			work.CancellationCode = ""
		}
	case domain.WorkTerminalFailed:
		work.State = domain.WorkTerminalFailed
		work.CancellationCode = ""
	default:
		return domain.MemoryExtractionWork{}, fmt.Errorf("sqlite: unknown memory generation outcome state %q", summary.PersistedState)
	}
	return work, nil
}

func memoryWorkNeedsAttention(work domain.MemoryExtractionWork) bool {
	return work.CancellationCode != "" || work.State == domain.WorkPending ||
		work.State == domain.WorkRunning || work.State == domain.WorkRetryPending
}

var _ domain.MemoryWorkRepository = (*CanonicalRepository)(nil)
