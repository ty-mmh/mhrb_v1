package sqlite

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"mahoroba.local/mahoroba/internal/canonical"
	"mahoroba.local/mahoroba/internal/domain"
	"mahoroba.local/mahoroba/internal/generation"
)

func (r *CanonicalRepository) BootstrapSnapshot(ctx context.Context) (domain.BootstrapState, error) {
	var count int
	if err := r.store.reader.QueryRowContext(ctx, `SELECT count(*) FROM principals`).Scan(&count); err != nil {
		return domain.BootstrapState{}, fmt.Errorf("sqlite: count bootstrap principals: %w", err)
	}
	if count == 0 {
		return domain.BootstrapState{}, nil
	}
	state := domain.BootstrapState{Initialized: true}
	if err := scanSingleID(r.store.reader.QueryRowContext(ctx, `SELECT principal_id FROM principals WHERE kind = 'system' ORDER BY created_at LIMIT 1`), &state.SystemPrincipalID); err != nil {
		return domain.BootstrapState{}, fmt.Errorf("sqlite: bootstrap system principal: %w", err)
	}
	var ownerRaw string
	if err := r.store.reader.QueryRowContext(ctx, `SELECT principal_id, display_name FROM principals WHERE kind = 'human' ORDER BY created_at LIMIT 1`).Scan(&ownerRaw, &state.OwnerDisplayName); err != nil {
		return domain.BootstrapState{}, fmt.Errorf("sqlite: bootstrap owner principal: %w", err)
	}
	ownerID, err := canonical.ParseID(ownerRaw)
	if err != nil {
		return domain.BootstrapState{}, fmt.Errorf("sqlite: parse bootstrap owner principal: %w", err)
	}
	state.OwnerPrincipalID = ownerID
	if err := scanSingleID(r.store.reader.QueryRowContext(ctx, `SELECT pipeline_version_id
		FROM pipeline_versions
		WHERE pipeline_kind = 'dialogue' AND version_key IN (?, ?, ?)
		ORDER BY CASE version_key WHEN ? THEN 0 WHEN ? THEN 1 ELSE 2 END LIMIT 1`,
		domain.DialoguePipelineVersionV3, domain.DialoguePipelineVersionV2, domain.DialoguePipelineVersionV1,
		domain.DialoguePipelineVersionV3, domain.DialoguePipelineVersionV2,
	), &state.PipelineVersionID); err != nil {
		return domain.BootstrapState{}, fmt.Errorf("sqlite: bootstrap dialogue pipeline: %w", err)
	}
	if err := scanSingleID(r.store.reader.QueryRowContext(ctx, `SELECT sessionization_policy_version_id FROM sessionization_policy_versions WHERE version_key = ?`, domain.SessionPolicyVersion), &state.SessionPolicyID); err != nil {
		return domain.BootstrapState{}, fmt.Errorf("sqlite: bootstrap sessionization policy: %w", err)
	}
	residents, err := r.ListResidents(ctx)
	if err != nil {
		return domain.BootstrapState{}, err
	}
	state.Residents = residents
	return state, nil
}

func (r *CanonicalRepository) ListResidents(ctx context.Context) ([]domain.ResidentSnapshot, error) {
	rows, err := r.store.reader.QueryContext(ctx, `SELECT resident_id FROM residents ORDER BY created_at, resident_id`)
	if err != nil {
		return nil, fmt.Errorf("sqlite: list resident IDs: %w", err)
	}
	defer rows.Close()
	var ids []canonical.ID
	for rows.Next() {
		var raw string
		if err := rows.Scan(&raw); err != nil {
			return nil, err
		}
		id, err := canonical.ParseID(raw)
		if err != nil {
			return nil, err
		}
		ids = append(ids, id)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	result := make([]domain.ResidentSnapshot, 0, len(ids))
	for _, id := range ids {
		snapshot, err := r.resident(ctx, id, false)
		if err != nil {
			return nil, err
		}
		result = append(result, snapshot)
	}
	return result, nil
}

func (r *CanonicalRepository) Resident(ctx context.Context, residentID canonical.ID) (domain.ResidentSnapshot, error) {
	return r.resident(ctx, residentID, false)
}

func (r *CanonicalRepository) resident(ctx context.Context, residentID canonical.ID, requireSessionPolicy bool) (domain.ResidentSnapshot, error) {
	var residentRaw, principalRaw, name string
	var seedKey sql.NullString
	err := r.store.reader.QueryRowContext(ctx, `SELECT resident_id, principal_id, name, seed_key FROM residents WHERE resident_id = ?`, residentID.String()).Scan(&residentRaw, &principalRaw, &name, &seedKey)
	if err != nil {
		return domain.ResidentSnapshot{}, fmt.Errorf("sqlite: load resident: %w", err)
	}
	parsedResident, err := canonical.ParseID(residentRaw)
	if err != nil {
		return domain.ResidentSnapshot{}, err
	}
	principal, err := canonical.ParseID(principalRaw)
	if err != nil {
		return domain.ResidentSnapshot{}, err
	}
	snapshot := domain.ResidentSnapshot{ResidentID: parsedResident, ResidentPrincipalID: principal, Name: name, SeedKey: seedKey.String}
	if err := scanSingleID(r.store.reader.QueryRowContext(ctx, `SELECT principal_id FROM principals WHERE kind = 'human' ORDER BY created_at LIMIT 1`), &snapshot.OwnerPrincipalID); err != nil {
		return domain.ResidentSnapshot{}, err
	}
	if err := r.store.reader.QueryRowContext(ctx, `SELECT t.to_status
		FROM resident_status_transitions t JOIN canonical_commits c ON c.canonical_commit_id = t.canonical_commit_id
		WHERE t.resident_id = ? ORDER BY c.commit_seq DESC LIMIT 1`, residentID.String()).Scan(&snapshot.Status); err != nil {
		return domain.ResidentSnapshot{}, fmt.Errorf("sqlite: load resident status: %w", err)
	}
	for _, revision := range []struct {
		class   string
		id      *canonical.ID
		content *string
	}{
		{"principles", &snapshot.PrinciplesRevisionID, &snapshot.Principles},
		{"persona", &snapshot.PersonaRevisionID, &snapshot.Persona},
		{"memory_policy", &snapshot.MemoryPolicyRevisionID, &snapshot.MemoryPolicy},
	} {
		id, content, found, err := r.latestRevision(ctx, residentID, revision.class)
		if err != nil {
			return domain.ResidentSnapshot{}, err
		}
		if found {
			*revision.id = id
			*revision.content = content
		}
	}
	if err := scanSingleID(r.store.reader.QueryRowContext(ctx, `SELECT pipeline_version_id
		FROM pipeline_versions
		WHERE pipeline_kind = 'dialogue' AND version_key IN (?, ?, ?)
		ORDER BY CASE version_key WHEN ? THEN 0 WHEN ? THEN 1 ELSE 2 END LIMIT 1`,
		domain.DialoguePipelineVersionV3, domain.DialoguePipelineVersionV2, domain.DialoguePipelineVersionV1,
		domain.DialoguePipelineVersionV3, domain.DialoguePipelineVersionV2,
	), &snapshot.PipelineVersionID); err != nil {
		return domain.ResidentSnapshot{}, err
	}
	if err := r.loadSessionPolicy(ctx, residentID, &snapshot); err != nil {
		if requireSessionPolicy || !errors.Is(err, errSessionPolicyUnresolved) {
			return domain.ResidentSnapshot{}, err
		}
		// Administrative inventory and inactive-work terminalization carry a
		// policy identity because the existing command envelope requires one,
		// but they never calculate an Activity Session. Only ActiveResident
		// takes the strict resolver above and receives the policy definition.
		if err := scanSingleID(r.store.reader.QueryRowContext(ctx, `SELECT sessionization_policy_version_id
			FROM sessionization_policy_versions WHERE version_key = ?`, domain.SessionPolicyVersion), &snapshot.SessionPolicyID); err != nil {
			return domain.ResidentSnapshot{}, fmt.Errorf("sqlite: load administrative sessionization policy identity: %w", err)
		}
	}
	return snapshot, nil
}

var errSessionPolicyUnresolved = errors.New("sqlite: desired sessionization policy is unresolved")

// ActivitySessionPolicy resolves the policy for ad-hoc deterministic session
// calculation without consulting an implicit newest/default version.
func (r *CanonicalRepository) ActivitySessionPolicy(ctx context.Context, residentID canonical.ID) (canonical.ID, canonical.Duration, error) {
	if err := residentID.Validate(); err != nil {
		return canonical.ID{}, 0, err
	}
	var snapshot domain.ResidentSnapshot
	if err := r.loadSessionPolicy(ctx, residentID, &snapshot); err != nil {
		return canonical.ID{}, 0, err
	}
	return snapshot.SessionPolicyID, snapshot.SessionIdleGap, nil
}

func (r *CanonicalRepository) loadSessionPolicy(ctx context.Context, residentID canonical.ID, snapshot *domain.ResidentSnapshot) error {
	policyID, idleGap, err := resolveActivitySessionPolicy(ctx, r.store.reader, residentID)
	if err != nil {
		return err
	}
	snapshot.SessionPolicyID = policyID
	snapshot.SessionIdleGap = idleGap
	return nil
}

type sessionPolicyQuerier interface {
	QueryRowContext(context.Context, string, ...any) *sql.Row
	QueryContext(context.Context, string, ...any) (*sql.Rows, error)
}

func resolveActivitySessionPolicy(ctx context.Context, query sessionPolicyQuerier, residentID canonical.ID) (canonical.ID, canonical.Duration, error) {
	var policyRaw string
	err := query.QueryRowContext(ctx, `SELECT desired_sessionization_policy_version_id
		FROM runtime_config
		WHERE singleton_id = 1 AND active_resident_id = ?
		  AND desired_sessionization_policy_version_id IS NOT NULL`, residentID.String()).Scan(&policyRaw)
	if errors.Is(err, sql.ErrNoRows) {
		rows, queryErr := query.QueryContext(ctx, `SELECT DISTINCT d.dependency_version_id
			FROM projection_watermark_dependencies d
			JOIN projection_watermarks w
			  ON w.projection_name = d.projection_name AND w.resident_id = d.resident_id
			WHERE d.projection_name = 'activity_sessions'
			  AND d.resident_id = ? AND d.dependency_kind = 'sessionization_policy'
			ORDER BY d.dependency_version_id`, residentID.String())
		if queryErr != nil {
			return canonical.ID{}, 0, fmt.Errorf("sqlite: inspect saved sessionization dependency: %w", queryErr)
		}
		defer rows.Close()
		var resolved []string
		for rows.Next() {
			var value string
			if scanErr := rows.Scan(&value); scanErr != nil {
				return canonical.ID{}, 0, fmt.Errorf("sqlite: scan saved sessionization dependency: %w", scanErr)
			}
			resolved = append(resolved, value)
		}
		if rowsErr := rows.Err(); rowsErr != nil {
			return canonical.ID{}, 0, fmt.Errorf("sqlite: inspect saved sessionization dependency: %w", rowsErr)
		}
		if len(resolved) == 0 {
			return canonical.ID{}, 0, errSessionPolicyUnresolved
		}
		if len(resolved) != 1 {
			return canonical.ID{}, 0, fmt.Errorf("sqlite: conflicting saved sessionization policy dependencies")
		}
		policyRaw = resolved[0]
		err = nil
	}
	if err != nil {
		return canonical.ID{}, 0, fmt.Errorf("sqlite: resolve desired sessionization policy: %w", err)
	}
	policyID, err := canonical.ParseID(policyRaw)
	if err != nil {
		return canonical.ID{}, 0, fmt.Errorf("sqlite: parse desired sessionization policy ID: %w", err)
	}
	var definitionRaw string
	if err := query.QueryRowContext(ctx, `SELECT definition FROM sessionization_policy_versions
		WHERE sessionization_policy_version_id = ?`, policyRaw).Scan(&definitionRaw); err != nil {
		return canonical.ID{}, 0, fmt.Errorf("%w: sessionization policy %s is unavailable: %v", errSessionPolicyUnresolved, policyRaw, err)
	}
	var definition struct {
		Version string             `json:"version"`
		IdleGap canonical.Duration `json:"idle_gap_microseconds"`
	}
	if err := json.Unmarshal([]byte(definitionRaw), &definition); err != nil {
		return canonical.ID{}, 0, fmt.Errorf("sqlite: decode desired sessionization policy: %w", err)
	}
	if definition.Version != domain.SessionPolicyVersion || definition.IdleGap.Microseconds() <= 0 {
		return canonical.ID{}, 0, fmt.Errorf("sqlite: unsupported desired sessionization policy definition")
	}
	return policyID, definition.IdleGap, nil
}

func (r *CanonicalRepository) latestRevision(ctx context.Context, residentID canonical.ID, class string) (canonical.ID, string, bool, error) {
	var raw string
	var content []byte
	err := r.store.reader.QueryRowContext(ctx, `SELECT rr.revision_id, b.content
		FROM resident_revisions rr
		JOIN canonical_commits revision_commit ON revision_commit.canonical_commit_id = rr.canonical_commit_id
		JOIN content_objects co ON co.content_id = rr.content_id
		LEFT JOIN blobs b ON b.dedupe_scope_id = co.owner_resident_id
		 AND b.hash_algorithm = co.blob_hash_algorithm AND b.blob_hash = co.blob_hash
		LEFT JOIN (
			SELECT activation.revision_id, MAX(activation_commit.commit_seq) AS activation_commit_seq
			FROM resident_revision_activations activation
			JOIN canonical_commits activation_commit
			  ON activation_commit.canonical_commit_id = activation.canonical_commit_id
			GROUP BY activation.revision_id
		) active ON active.revision_id = rr.revision_id
		WHERE rr.resident_id = ? AND rr.revision_class = ?
		ORDER BY active.activation_commit_seq IS NOT NULL DESC,
		 active.activation_commit_seq DESC,
		 revision_commit.commit_seq DESC
		LIMIT 1`, residentID.String(), class).Scan(&raw, &content)
	if errors.Is(err, sql.ErrNoRows) {
		return canonical.ID{}, "", false, nil
	}
	if err != nil {
		return canonical.ID{}, "", false, fmt.Errorf("sqlite: load %s revision: %w", class, err)
	}
	id, err := canonical.ParseID(raw)
	if err != nil {
		return canonical.ID{}, "", false, err
	}
	if content == nil {
		return id, "[erased]", true, nil
	}
	return id, string(content), true, nil
}

func (r *CanonicalRepository) SelectActiveResident(ctx context.Context, residentID canonical.ID, at canonical.Instant, tz canonical.Timezone) error {
	if err := residentID.Validate(); err != nil {
		return fmt.Errorf("sqlite: invalid resident selection: %w", err)
	}
	if err := tz.Validate(); err != nil {
		return fmt.Errorf("sqlite: invalid resident selection timezone: %w", err)
	}
	release, err := r.store.writes.acquireHigh(ctx)
	if err != nil {
		return fmt.Errorf("sqlite: acquire resident selection write priority: %w", err)
	}
	defer release()
	if err := r.store.verifyWritableBoundary("before resident selection transaction"); err != nil {
		return err
	}
	tx, err := r.store.writer.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelSerializable})
	if err != nil {
		return fmt.Errorf("sqlite: begin resident selection: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	if err := r.store.verifyWritableBoundary("after resident selection transaction open"); err != nil {
		return err
	}

	var status, policyID string
	err = tx.QueryRowContext(ctx, `SELECT t.to_status,
		(SELECT sessionization_policy_version_id FROM sessionization_policy_versions WHERE version_key = ?)
		FROM resident_status_transitions t
		JOIN canonical_commits c ON c.canonical_commit_id = t.canonical_commit_id
		WHERE t.resident_id = ?
		ORDER BY c.commit_seq DESC LIMIT 1`, domain.SessionPolicyVersion, residentID.String()).Scan(&status, &policyID)
	if err != nil {
		return fmt.Errorf("sqlite: resolve resident selection state: %w", err)
	}
	if status != "active" {
		return fmt.Errorf("sqlite: cannot select resident in %s status", status)
	}
	if _, err = tx.ExecContext(ctx, `INSERT INTO runtime_config(singleton_id, active_resident_id, desired_sessionization_policy_version_id, updated_at, updated_tz)
		VALUES (1, ?, ?, ?, ?)
		ON CONFLICT(singleton_id) DO UPDATE SET active_resident_id=excluded.active_resident_id,
		desired_sessionization_policy_version_id=excluded.desired_sessionization_policy_version_id,
		updated_at=excluded.updated_at, updated_tz=excluded.updated_tz`, residentID.String(), policyID, at.UnixMicro(), tz.String()); err != nil {
		return fmt.Errorf("sqlite: select active resident: %w", err)
	}
	if err := r.store.verifyWritableBoundary("before resident selection commit"); err != nil {
		return err
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("sqlite: commit resident selection: %w", err)
	}
	return r.store.verifyWritableBoundary("after resident selection commit")
}

func (r *CanonicalRepository) ActiveResident(ctx context.Context) (domain.ResidentSnapshot, error) {
	var raw string
	err := r.store.reader.QueryRowContext(ctx, `SELECT active_resident_id FROM runtime_config WHERE singleton_id = 1 AND active_resident_id IS NOT NULL`).Scan(&raw)
	if err != nil {
		return domain.ResidentSnapshot{}, fmt.Errorf("sqlite: active resident is not configured: %w", err)
	}
	id, err := canonical.ParseID(raw)
	if err != nil {
		return domain.ResidentSnapshot{}, err
	}
	return r.resident(ctx, id, true)
}

func (r *CanonicalRepository) History(ctx context.Context, residentID canonical.ID, limit int) ([]domain.Event, error) {
	if limit < 1 {
		return nil, nil
	}
	rows, err := r.store.reader.QueryContext(ctx, eventSelect+`
		WHERE e.resident_id = ? AND e.visibility = 'conversation'
		ORDER BY e.seq DESC LIMIT ?`, residentID.String(), limit)
	if err != nil {
		return nil, fmt.Errorf("sqlite: read history: %w", err)
	}
	defer rows.Close()
	var reversed []domain.Event
	for rows.Next() {
		event, err := scanEvent(rows)
		if err != nil {
			return nil, err
		}
		reversed = append(reversed, event)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	result := make([]domain.Event, len(reversed))
	for i := range reversed {
		result[len(reversed)-1-i] = reversed[i]
	}
	return result, nil
}

func (r *CanonicalRepository) Event(ctx context.Context, residentID, eventID canonical.ID) (domain.Event, error) {
	return scanEvent(r.store.reader.QueryRowContext(ctx, eventSelect+` WHERE e.resident_id = ? AND e.event_id = ?`, residentID.String(), eventID.String()))
}

// BlobReferenced answers from Canonical content_objects rather than a
// projection or the inline blobs table. A present logical content reference
// must preserve its resident-scoped filesystem object even when the inline
// material is missing and needs repair.
func (r *CanonicalRepository) BlobReferenced(ctx context.Context, residentID canonical.ID, digest canonical.Digest) (bool, error) {
	if err := residentID.Validate(); err != nil {
		return false, fmt.Errorf("sqlite: invalid blob reference resident: %w", err)
	}
	var referenced int
	if err := r.store.reader.QueryRowContext(ctx, `SELECT EXISTS(
		SELECT 1 FROM content_objects
		WHERE owner_resident_id = ? AND blob_hash_algorithm = 'sha256'
		  AND blob_hash = ? AND erasure_state = 'present'
	)`, residentID.String(), digest.Bytes()).Scan(&referenced); err != nil {
		return false, fmt.Errorf("sqlite: check Canonical blob reference: %w", err)
	}
	return referenced != 0, nil
}

func (r *CanonicalRepository) DiscoverDialogueWork(
	ctx context.Context,
	residentID canonical.ID,
	request domain.DialogueDiscoveryRequest,
) (domain.DialogueDiscoveryResult, error) {
	if err := residentID.Validate(); err != nil {
		return domain.DialogueDiscoveryResult{}, fmt.Errorf("sqlite: invalid dialogue discovery resident: %w", err)
	}
	if err := request.Validate(); err != nil {
		return domain.DialogueDiscoveryResult{}, fmt.Errorf("sqlite: invalid dialogue discovery request: %w", err)
	}
	scanCtx, cancel := context.WithTimeout(ctx, request.Budget.Elapsed)
	defer cancel()

	var residentStatus string
	if err := r.store.reader.QueryRowContext(scanCtx, `SELECT t.to_status
		FROM resident_status_transitions t JOIN canonical_commits c ON c.canonical_commit_id = t.canonical_commit_id
		WHERE t.resident_id = ? ORDER BY c.commit_seq DESC LIMIT 1`, residentID.String()).Scan(&residentStatus); err != nil {
		return domain.DialogueDiscoveryResult{}, fmt.Errorf("sqlite: resolve dialogue resident status: %w", err)
	}
	var residentSelected int
	if err := r.store.reader.QueryRowContext(scanCtx, `SELECT EXISTS(
		SELECT 1 FROM runtime_config WHERE singleton_id = 1 AND active_resident_id = ?
	)`, residentID.String()).Scan(&residentSelected); err != nil {
		return domain.DialogueDiscoveryResult{}, fmt.Errorf("sqlite: resolve operational dialogue resident: %w", err)
	}

	// Preserve the prompt recent window independently of the older keyset
	// cursor. New Commit-A obligations therefore remain immediately visible
	// while a long-running safety scan continues to make bounded progress.
	recentEvents, err := r.dialogueEventBatch(scanCtx, residentID, nil, request.Budget.RecentCandidates)
	if err != nil {
		return domain.DialogueDiscoveryResult{}, err
	}
	result := domain.DialogueDiscoveryResult{RecentScanned: len(recentEvents)}
	recent := make([]domain.DialogueWork, 0, len(recentEvents))
	for _, event := range recentEvents {
		work, err := r.classifyDialogueWork(scanCtx, event, residentStatus, residentSelected != 0, request.MaxAttempts)
		if err != nil {
			return domain.DialogueDiscoveryResult{}, err
		}
		recent = append(recent, work)
	}
	if len(recentEvents) < request.Budget.RecentCandidates {
		result.CycleComplete = true
		result.Work = reverseDialogueWork(recent)
		return result, nil
	}

	cursor := recentEvents[len(recentEvents)-1].Seq
	if request.Cursor != nil && request.Cursor.BeforeSeq < cursor {
		cursor = request.Cursor.BeforeSeq
	}
	olderActionable := make([]domain.DialogueWork, 0, request.Budget.OlderPageSize)
	for result.OlderScanned < request.Budget.OlderCandidates &&
		result.OlderPageQueries < request.Budget.OlderPages {
		select {
		case <-scanCtx.Done():
			if ctx.Err() != nil {
				return domain.DialogueDiscoveryResult{}, ctx.Err()
			}
			result.BudgetExhausted = true
			result.NextCursor = dialogueDiscoveryCursor(cursor)
			result.Work = reverseDialogueWork(recent)
			return result, nil
		default:
		}

		remaining := request.Budget.OlderCandidates - result.OlderScanned
		pageLimit := min(request.Budget.OlderPageSize, remaining)
		pageStart := cursor
		olderEvents, err := r.dialogueEventBatch(scanCtx, residentID, &pageStart, pageLimit)
		if err != nil {
			// No cursor is returned with an error. The caller retains its prior
			// cursor and may safely repeat already-classified immutable events.
			return domain.DialogueDiscoveryResult{}, err
		}
		result.OlderPageQueries++
		result.OlderScanned += len(olderEvents)
		if len(olderEvents) == 0 {
			result.CycleComplete = true
			break
		}

		pageActionable := make([]domain.DialogueWork, 0, len(olderEvents))
		for _, event := range olderEvents {
			work, err := r.classifyDialogueWork(
				scanCtx, event, residentStatus, residentSelected != 0, request.MaxAttempts,
			)
			if err != nil {
				return domain.DialogueDiscoveryResult{}, err
			}
			if dialogueWorkNeedsAttention(work) {
				pageActionable = append(pageActionable, work)
			}
		}
		cursor = olderEvents[len(olderEvents)-1].Seq
		if len(pageActionable) > 0 {
			olderActionable = append(olderActionable, pageActionable...)
			result.ActionablePage = true
			result.NextCursor = dialogueDiscoveryCursor(pageStart)
			break
		}
		if len(olderEvents) < pageLimit {
			result.CycleComplete = true
			break
		}
	}
	if !result.CycleComplete && !result.ActionablePage {
		result.BudgetExhausted = true
		result.NextCursor = dialogueDiscoveryCursor(cursor)
	}
	descending := make([]domain.DialogueWork, 0, len(recent)+len(olderActionable))
	descending = append(descending, recent...)
	descending = append(descending, olderActionable...)
	result.Work = reverseDialogueWork(descending)
	return result, nil
}

func dialogueDiscoveryCursor(beforeSeq canonical.Seq) *domain.DialogueDiscoveryCursor {
	return &domain.DialogueDiscoveryCursor{BeforeSeq: beforeSeq}
}

func reverseDialogueWork(descending []domain.DialogueWork) []domain.DialogueWork {
	result := make([]domain.DialogueWork, len(descending))
	for i := range descending {
		result[len(descending)-1-i] = descending[i]
	}
	return result
}

const (
	dialogueRecentCandidatesQuery = eventSelect + ` WHERE e.resident_id = ? AND e.event_type = 'user_message'
		ORDER BY e.seq DESC LIMIT ?`
	dialogueOlderCandidatesQuery = eventSelect + ` WHERE e.resident_id = ? AND e.event_type = 'user_message'
		AND e.seq < ? ORDER BY e.seq DESC LIMIT ?`
)

func (r *CanonicalRepository) dialogueEventBatch(ctx context.Context, residentID canonical.ID, beforeSeq *canonical.Seq, limit int) ([]domain.Event, error) {
	query := dialogueRecentCandidatesQuery
	args := []any{residentID.String(), limit}
	if beforeSeq != nil {
		query = dialogueOlderCandidatesQuery
		args = []any{residentID.String(), beforeSeq.Int64(), limit}
	}
	rows, err := r.store.reader.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("sqlite: discover dialogue input events: %w", err)
	}
	var events []domain.Event
	for rows.Next() {
		event, err := scanEvent(rows)
		if err != nil {
			_ = rows.Close()
			return nil, err
		}
		events = append(events, event)
	}
	if err := rows.Err(); err != nil {
		_ = rows.Close()
		return nil, err
	}
	if err := rows.Close(); err != nil {
		return nil, err
	}
	return events, nil
}

func (r *CanonicalRepository) classifyDialogueWork(ctx context.Context, event domain.Event, residentStatus string, residentSelected bool, maxAttempts int) (domain.DialogueWork, error) {
	work := domain.DialogueWork{UserEvent: event, State: domain.WorkPending}
	cancellationCode := ""
	if event.ContentErased {
		cancellationCode = generation.MustOutcomeErrorCode(generation.ErrorSourceContentErased, 0).String()
	} else if residentStatus != "active" {
		cancellationCode = generation.MustOutcomeErrorCode(generation.ErrorResidentInactive, 0).String()
	} else if !residentSelected {
		cancellationCode = generation.MustOutcomeErrorCode(generation.ErrorResidentUnselected, 0).String()
	}
	var runRaw string
	err := r.store.reader.QueryRowContext(ctx, `SELECT generation_run_id FROM generation_runs WHERE resident_id = ? AND idempotency_key = ?`, event.ResidentID.String(), domain.DialogueObligation(event.ID)).Scan(&runRaw)
	if errors.Is(err, sql.ErrNoRows) {
		work.CancellationCode = cancellationCode
		return work, nil
	}
	if err != nil {
		return domain.DialogueWork{}, err
	}
	runID, err := canonical.ParseID(runRaw)
	if err != nil {
		return domain.DialogueWork{}, err
	}
	work.RunID = &runID
	summary, err := (generationOutcomeRepository{}).Summary(ctx, r.store.reader, runID, event.ResidentID)
	if err != nil {
		return domain.DialogueWork{}, err
	}
	summary, err = summary.Classify(int64(maxAttempts))
	if err != nil {
		return domain.DialogueWork{}, err
	}
	work.AttemptNo = summary.Latest.AttemptNo
	switch summary.ClassifiedState {
	case domain.WorkRunning:
		work.State = domain.WorkRunning
		work.CancellationCode = cancellationCode
	case domain.WorkSucceeded:
		work.State = domain.WorkSucceeded
	case domain.WorkRetryPending:
		if summary.RetryEligible {
			work.State = domain.WorkRetryPending
			work.CancellationCode = cancellationCode
		} else {
			work.State = domain.WorkTerminalFailed
		}
	case domain.WorkTerminalFailed:
		work.State = domain.WorkTerminalFailed
	default:
		return domain.DialogueWork{}, fmt.Errorf("sqlite: unknown classified generation outcome state %q", summary.PersistedState)
	}
	return work, nil
}

func dialogueWorkNeedsAttention(work domain.DialogueWork) bool {
	return work.CancellationCode != "" || work.State == domain.WorkPending ||
		work.State == domain.WorkRetryPending || work.State == domain.WorkRunning
}

type runningAttemptCandidate struct {
	runID      canonical.ID
	residentID canonical.ID
}

type runningAttemptCandidateRows interface {
	Next() bool
	Scan(...any) error
	Err() error
	Close() error
}

func readRunningAttemptCandidates(rows runningAttemptCandidateRows) ([]runningAttemptCandidate, error) {
	defer rows.Close()
	var candidates []runningAttemptCandidate
	for rows.Next() {
		var runRaw, residentRaw string
		if err := rows.Scan(&runRaw, &residentRaw); err != nil {
			return nil, err
		}
		runID, err := canonical.ParseID(runRaw)
		if err != nil {
			return nil, err
		}
		residentID, err := canonical.ParseID(residentRaw)
		if err != nil {
			return nil, err
		}
		candidates = append(candidates, runningAttemptCandidate{runID: runID, residentID: residentID})
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	if err := rows.Close(); err != nil {
		return nil, err
	}
	return candidates, nil
}

func (r *CanonicalRepository) RunningAttempts(ctx context.Context, limit int) ([]domain.RunningAttempt, error) {
	if limit < 1 {
		return nil, nil
	}
	rows, err := r.store.reader.QueryContext(ctx, `SELECT run.generation_run_id, run.resident_id
		FROM generation_runs run
		JOIN canonical_commits run_commit ON run_commit.canonical_commit_id = run.canonical_commit_id
		ORDER BY run_commit.commit_seq, run.generation_run_id`)
	if err != nil {
		return nil, err
	}
	candidates, err := readRunningAttemptCandidates(rows)
	if err != nil {
		return nil, err
	}
	result := make([]domain.RunningAttempt, 0, limit)
	for _, candidate := range candidates {
		summary, err := (generationOutcomeRepository{}).Summary(ctx, r.store.reader,
			candidate.runID, candidate.residentID)
		if err != nil {
			return nil, err
		}
		if summary.PersistedState != domain.WorkRunning {
			continue
		}
		result = append(result, domain.RunningAttempt{
			RunID:      candidate.runID,
			ResidentID: candidate.residentID,
			AttemptNo:  summary.Latest.AttemptNo,
		})
		if len(result) == limit {
			break
		}
	}
	return result, nil
}

func (r *CanonicalRepository) RunningAttemptsForResident(
	ctx context.Context,
	residentID canonical.ID,
	limit int,
) ([]domain.RunningAttempt, error) {
	if err := residentID.Validate(); err != nil {
		return nil, err
	}
	if limit < 1 {
		return nil, nil
	}
	rows, err := r.store.reader.QueryContext(ctx, `SELECT run.generation_run_id, run.resident_id
		FROM generation_runs run
		JOIN canonical_commits run_commit ON run_commit.canonical_commit_id = run.canonical_commit_id
		WHERE run.resident_id = ?
		ORDER BY run_commit.commit_seq, run.generation_run_id`, residentID.String())
	if err != nil {
		return nil, err
	}
	candidates, err := readRunningAttemptCandidates(rows)
	if err != nil {
		return nil, err
	}
	result := make([]domain.RunningAttempt, 0, limit)
	for _, candidate := range candidates {
		summary, err := (generationOutcomeRepository{}).Summary(
			ctx, r.store.reader, candidate.runID, candidate.residentID,
		)
		if err != nil {
			return nil, err
		}
		if summary.PersistedState != domain.WorkRunning {
			continue
		}
		result = append(result, domain.RunningAttempt{
			RunID: candidate.runID, ResidentID: candidate.residentID,
			AttemptNo: summary.Latest.AttemptNo,
		})
		if len(result) == limit {
			break
		}
	}
	return result, nil
}

func (r *CanonicalRepository) Generation(ctx context.Context, runID canonical.ID) (domain.PreparedGeneration, error) {
	var runRaw, residentRaw, purposeRaw, idempotencyKey, provider, model, generatorParamsRaw, asOfTZ string
	var asOf int64
	var pipelineRaw, pipelineKind, pipelineVersionKey, pipelineDefinitionRaw string
	var principlesRaw, personaRaw, memoryPolicyRaw string
	var sessionRaw, recallRaw sql.NullString
	var promptVersion, contextVersion, renderingVersion string
	err := r.store.reader.QueryRowContext(ctx, `SELECT run.generation_run_id, run.resident_id, run.purpose,
		run.idempotency_key, run.provider, run.model, run.prompt_template_version, run.pipeline_version_id,
		pipeline.pipeline_kind, pipeline.version_key, pipeline.definition, run.context_policy_version,
		run.sessionization_policy_version_id, run.memory_rendering_version,
		run.principles_revision_id, run.persona_revision_id, run.memory_policy_revision_id,
		run.recall_run_id, run.generator_params, run.as_of, run.as_of_tz
		FROM generation_runs run
		JOIN pipeline_versions pipeline ON pipeline.pipeline_version_id = run.pipeline_version_id
		WHERE run.generation_run_id = ?`, runID.String()).Scan(
		&runRaw, &residentRaw, &purposeRaw, &idempotencyKey, &provider, &model,
		&promptVersion, &pipelineRaw, &pipelineKind, &pipelineVersionKey, &pipelineDefinitionRaw,
		&contextVersion, &sessionRaw, &renderingVersion,
		&principlesRaw, &personaRaw, &memoryPolicyRaw, &recallRaw, &generatorParamsRaw, &asOf, &asOfTZ,
	)
	if err != nil {
		return domain.PreparedGeneration{}, fmt.Errorf("sqlite: load prepared generation: %w", err)
	}
	prepared := domain.PreparedGeneration{
		Purpose: domain.GenerationPurpose(purposeRaw), IdempotencyKey: idempotencyKey,
		Provider: provider, Model: model, AsOf: canonical.Instant(asOf), AsOfTZ: canonical.Timezone(asOfTZ),
		PromptTemplateVersion: promptVersion, ContextPolicyVersion: contextVersion,
		MemoryRenderingVersion: renderingVersion, PipelineVersionKey: pipelineVersionKey,
	}
	prepared.RunID, err = canonical.ParseID(runRaw)
	if err != nil {
		return domain.PreparedGeneration{}, err
	}
	prepared.ResidentID, err = canonical.ParseID(residentRaw)
	if err != nil {
		return domain.PreparedGeneration{}, err
	}
	prepared.PipelineVersionID, err = canonical.ParseID(pipelineRaw)
	if err != nil {
		return prepared, fmt.Errorf("sqlite: invalid generation pipeline ID: %w", err)
	}
	if sessionRaw.Valid {
		sessionPolicyID, err := canonical.ParseID(sessionRaw.String)
		if err != nil {
			return prepared, fmt.Errorf("sqlite: invalid generation sessionization policy ID: %w", err)
		}
		prepared.SessionPolicyID = &sessionPolicyID
	}
	prepared.PrinciplesRevisionID, err = canonical.ParseID(principlesRaw)
	if err != nil {
		return prepared, fmt.Errorf("sqlite: invalid generation principles revision ID: %w", err)
	}
	prepared.PersonaRevisionID, err = canonical.ParseID(personaRaw)
	if err != nil {
		return prepared, fmt.Errorf("sqlite: invalid generation persona revision ID: %w", err)
	}
	prepared.MemoryPolicyRevisionID, err = canonical.ParseID(memoryPolicyRaw)
	if err != nil {
		return prepared, fmt.Errorf("sqlite: invalid generation memory policy revision ID: %w", err)
	}
	if recallRaw.Valid {
		recallRunID, parseErr := canonical.ParseID(recallRaw.String)
		if parseErr != nil {
			return prepared, fmt.Errorf("sqlite: invalid generation Recall run ID: %w", parseErr)
		}
		prepared.RecallRunID = &recallRunID
	}
	if prepared.Purpose.Effective() == domain.GenerationPurposeDialogue && prepared.SessionPolicyID == nil {
		return prepared, errors.New("sqlite: dialogue generation is missing its sessionization policy")
	}
	if prepared.Purpose.Effective() != domain.GenerationPurposeDialogue && prepared.RecallRunID != nil {
		return prepared, errors.New("sqlite: non-dialogue generation unexpectedly links a Recall run")
	}
	if prepared.Purpose.Effective() != domain.GenerationPurposeDialogue && prepared.SessionPolicyID != nil {
		return prepared, errors.New("sqlite: non-dialogue generation unexpectedly pins a sessionization policy")
	}
	versions := domain.GenerationVersionContract{
		PromptTemplateVersion: prepared.PromptTemplateVersion, ContextPolicyVersion: prepared.ContextPolicyVersion,
		MemoryRenderingVersion: prepared.MemoryRenderingVersion,
	}
	if prepared.Purpose.Effective() != domain.GenerationPurposeDialogue {
		if err := domain.ValidateGenerationVersions(prepared.Purpose, versions); err != nil {
			return prepared, fmt.Errorf("sqlite: generation purpose contract: %w", err)
		}
	}
	summary, err := (generationOutcomeRepository{}).Summary(ctx, r.store.reader, runID, prepared.ResidentID)
	if err != nil {
		return domain.PreparedGeneration{}, err
	}
	prepared.AttemptNo = summary.Latest.AttemptNo
	switch summary.PersistedState {
	case domain.WorkRunning:
		prepared.State = domain.WorkRunning
	case domain.WorkSucceeded:
		prepared.State = domain.WorkSucceeded
	case domain.WorkTerminalFailed:
		prepared.State = domain.WorkTerminalFailed
	default:
		return domain.PreparedGeneration{}, fmt.Errorf("sqlite: unknown prepared generation outcome state %q", summary.PersistedState)
	}
	pipelineDefinition, err := canonical.ParseCanonicalJSON([]byte(pipelineDefinitionRaw))
	if err != nil {
		return prepared, fmt.Errorf("sqlite: generation pipeline definition is not canonical: %w", err)
	}
	if prepared.Purpose.Effective() == domain.GenerationPurposeDialogue && pipelineKind != "dialogue" {
		return prepared, fmt.Errorf("sqlite: dialogue generation references pipeline kind %q", pipelineKind)
	}
	if err := domain.ValidateExactDialoguePipelineDefinition(domain.PipelineVersionDefinition{
		ID: prepared.PipelineVersionID, Kind: pipelineKind,
		VersionKey: prepared.PipelineVersionKey, Definition: pipelineDefinition,
	}); err != nil {
		return prepared, fmt.Errorf("sqlite: generation pipeline definition: %w", err)
	}
	if prepared.Purpose.Effective() == domain.GenerationPurposeDialogue {
		contract := domain.DialogueExecutionContract{
			PipelineVersionKey: prepared.PipelineVersionKey, PromptTemplateVersion: versions.PromptTemplateVersion,
			ContextPolicyVersion: versions.ContextPolicyVersion, MemoryRenderingVersion: versions.MemoryRenderingVersion,
		}
		kind := domain.DialogueEnvelopeNormal
		if summary.Latest.AttemptNo == 0 {
			kind = domain.DialogueEnvelopeSyntheticNoDispatch
		}
		if _, err := domain.ClassifyPersistedDialogueExecutionContract(contract, kind); err != nil {
			return prepared, fmt.Errorf("sqlite: generation purpose contract: generation version contract mismatch: %w", err)
		}
		if kind == domain.DialogueEnvelopeSyntheticNoDispatch {
			if err := r.validatePersistedSyntheticDialogue(ctx, prepared, summary, contract); err != nil {
				return prepared, err
			}
		}
	}
	if provider == "" || model == "" {
		return prepared, errors.New("sqlite: prepared generation has an incomplete provider identity")
	}
	params, encodedParams, err := domain.ParseGeneratorParams([]byte(generatorParamsRaw))
	if err != nil {
		return prepared, fmt.Errorf("sqlite: load prepared generation parameters: %w", err)
	}
	prepared.GeneratorParams = params
	prepared.CanonicalGeneratorParams = encodedParams
	if err := domain.ValidateGeneratorParamsForPurpose(prepared.Purpose, params); err != nil {
		return prepared, fmt.Errorf("sqlite: generation purpose/parameters contract: %w", err)
	}
	rows, err := r.store.reader.QueryContext(ctx, `SELECT i.generation_run_input_id, i.ordinal, i.role, i.source_type,
		i.source_id, i.inclusion_mode, co.content_id, co.owner_resident_id, co.content_class,
		co.blob_hash, co.commitment, co.commitment_salt, co.erasure_policy, b.content
		FROM generation_run_inputs i
		JOIN content_objects co ON co.content_id = i.content_id
		LEFT JOIN blobs b ON b.dedupe_scope_id = co.owner_resident_id
		 AND b.hash_algorithm = co.blob_hash_algorithm AND b.blob_hash = co.blob_hash
		WHERE i.generation_run_id = ? ORDER BY i.ordinal`, runID.String())
	if err != nil {
		return domain.PreparedGeneration{}, err
	}
	defer rows.Close()
	for rows.Next() {
		var input domain.GenerationInput
		var inputRaw, contentRaw, ownerRaw string
		var source sql.NullString
		var blobHash, commitment, salt, content []byte
		if err := rows.Scan(&inputRaw, &input.Ordinal, &input.Role, &input.SourceType, &source, &input.InclusionMode,
			&contentRaw, &ownerRaw, &input.Content.Class, &blobHash, &commitment, &salt, &input.Content.ErasurePolicy, &content); err != nil {
			return domain.PreparedGeneration{}, err
		}
		if content == nil {
			return domain.PreparedGeneration{}, errors.New("sqlite: prepared generation input content was erased")
		}
		input.ID, err = canonical.ParseID(inputRaw)
		if err != nil {
			return domain.PreparedGeneration{}, err
		}
		input.Content.ID, err = canonical.ParseID(contentRaw)
		if err != nil {
			return domain.PreparedGeneration{}, err
		}
		input.Content.ResidentID, err = canonical.ParseID(ownerRaw)
		if err != nil {
			return domain.PreparedGeneration{}, err
		}
		input.Content.BlobHash, err = canonical.DigestFromBytes(blobHash)
		if err != nil {
			return domain.PreparedGeneration{}, err
		}
		input.Content.Commitment, err = canonical.DigestFromBytes(commitment)
		if err != nil {
			return domain.PreparedGeneration{}, err
		}
		input.Content.CommitmentSalt, err = canonical.ContentSaltFromBytes(salt)
		if err != nil {
			return domain.PreparedGeneration{}, err
		}
		input.Content.Bytes = append([]byte(nil), content...)
		if source.Valid {
			id, err := canonical.ParseID(source.String)
			if err != nil {
				return domain.PreparedGeneration{}, err
			}
			input.SourceID = &id
		}
		prepared.Inputs = append(prepared.Inputs, input)
	}
	return prepared, rows.Err()
}

func (r *CanonicalRepository) validatePersistedSyntheticDialogue(
	ctx context.Context,
	prepared domain.PreparedGeneration,
	summary generationOutcomeSummary,
	contract domain.DialogueExecutionContract,
) error {
	if prepared.Provider != "mahoroba-internal" || prepared.Model != "not-dispatched" {
		return fmt.Errorf("%w: synthetic dialogue has a dispatchable provider identity", ErrInvalidGenerationOutcomeHistory)
	}
	if summary.InputCount != 0 || prepared.RecallRunID != nil || summary.Latest.AttemptNo != 0 ||
		summary.Latest.State != "cancelled" {
		return fmt.Errorf("%w: synthetic dialogue has an invalid no-dispatch structure", ErrInvalidGenerationOutcomeHistory)
	}
	var usageCount int64
	if err := r.store.reader.QueryRowContext(ctx, `SELECT COUNT(*) FROM claim_usages
		WHERE generation_run_id = ? OR detected_by_run_id = ?`,
		prepared.RunID.String(), prepared.RunID.String()).Scan(&usageCount); err != nil {
		return fmt.Errorf("%w: inspect synthetic dialogue claim usage: %v", ErrInvalidGenerationOutcomeHistory, err)
	}
	if usageCount != 0 {
		return fmt.Errorf("%w: synthetic dialogue has claim usage provenance", ErrInvalidGenerationOutcomeHistory)
	}

	const obligationPrefix = "dialogue:v1:"
	if !strings.HasPrefix(prepared.IdempotencyKey, obligationPrefix) {
		return fmt.Errorf("%w: synthetic dialogue has an invalid obligation key", ErrInvalidGenerationOutcomeHistory)
	}
	sourceEventID, err := canonical.ParseID(strings.TrimPrefix(prepared.IdempotencyKey, obligationPrefix))
	if err != nil || domain.DialogueObligation(sourceEventID) != prepared.IdempotencyKey {
		return fmt.Errorf("%w: synthetic dialogue has an invalid source event identity", ErrInvalidGenerationOutcomeHistory)
	}
	var sourceCommit int64
	var eventType string
	if err := r.store.reader.QueryRowContext(ctx, `SELECT source_commit.commit_seq, source.event_type
		FROM events source
		JOIN canonical_commits source_commit
		  ON source_commit.canonical_commit_id = source.canonical_commit_id
		WHERE source.event_id = ? AND source.resident_id = ?`,
		sourceEventID.String(), prepared.ResidentID.String(),
	).Scan(&sourceCommit, &eventType); err != nil {
		return fmt.Errorf("%w: resolve synthetic dialogue source event: %v", ErrInvalidGenerationOutcomeHistory, err)
	}
	if eventType != "user_message" {
		return fmt.Errorf("%w: synthetic dialogue source is not a user message", ErrInvalidGenerationOutcomeHistory)
	}

	pipelineID, pipelineVersion, resolvable, err := resolveCancellationDialoguePipeline(
		ctx, r.store.reader, sourceCommit,
	)
	if err != nil {
		return fmt.Errorf("%w: resolve synthetic dialogue source-time pipeline: %v", ErrInvalidGenerationOutcomeHistory, err)
	}
	if !resolvable || pipelineID != prepared.PipelineVersionID || pipelineVersion != prepared.PipelineVersionKey {
		return fmt.Errorf("%w: synthetic dialogue pipeline is not the source-time contract", ErrInvalidGenerationOutcomeHistory)
	}
	if err := domain.ValidateSyntheticDialogueExecutionContract(contract, pipelineVersion); err != nil {
		return fmt.Errorf("%w: synthetic dialogue source-time version contract: %v", ErrInvalidGenerationOutcomeHistory, err)
	}
	return nil
}

const eventSelect = `SELECT e.event_id, e.resident_id, e.seq, e.event_type, e.actor_principal_id,
	e.target_principal_id, e.generation_run_id, e.occurred_at, e.occurred_tz, e.recorded_at,
	e.recorded_tz, e.content_id, e.payload_commitment, e.prev_event_hash, e.event_hash, co.erasure_state, b.content
	FROM events e JOIN content_objects co ON co.content_id = e.content_id
	LEFT JOIN blobs b ON b.dedupe_scope_id = co.owner_resident_id
	 AND b.hash_algorithm = co.blob_hash_algorithm AND b.blob_hash = co.blob_hash `

type rowScanner interface {
	Scan(...any) error
}

func scanEvent(row rowScanner) (domain.Event, error) {
	var event domain.Event
	var eventRaw, residentRaw, actorRaw, contentRaw, occurredTZ, recordedTZ, erasureState string
	var target, run sql.NullString
	var seq int64
	var commitment, previous, hash, content []byte
	err := row.Scan(&eventRaw, &residentRaw, &seq, &event.Type, &actorRaw, &target, &run,
		&event.OccurredAt, &occurredTZ, &event.RecordedAt, &recordedTZ, &contentRaw,
		&commitment, &previous, &hash, &erasureState, &content)
	if err != nil {
		return domain.Event{}, err
	}
	var parseErr error
	event.ID, parseErr = canonical.ParseID(eventRaw)
	if parseErr != nil {
		return domain.Event{}, parseErr
	}
	event.ResidentID, parseErr = canonical.ParseID(residentRaw)
	if parseErr != nil {
		return domain.Event{}, parseErr
	}
	event.ActorPrincipalID, parseErr = canonical.ParseID(actorRaw)
	if parseErr != nil {
		return domain.Event{}, parseErr
	}
	event.ContentID, parseErr = canonical.ParseID(contentRaw)
	if parseErr != nil {
		return domain.Event{}, parseErr
	}
	event.Seq, parseErr = canonical.NewSeq(seq)
	if parseErr != nil {
		return domain.Event{}, parseErr
	}
	event.OccurredTZ, parseErr = canonical.ParseTimezone(occurredTZ)
	if parseErr != nil {
		return domain.Event{}, parseErr
	}
	event.RecordedTZ, parseErr = canonical.ParseTimezone(recordedTZ)
	if parseErr != nil {
		return domain.Event{}, parseErr
	}
	event.Commitment, parseErr = canonical.DigestFromBytes(commitment)
	if parseErr != nil {
		return domain.Event{}, parseErr
	}
	event.Hash, parseErr = canonical.DigestFromBytes(hash)
	if parseErr != nil {
		return domain.Event{}, parseErr
	}
	if previous != nil {
		value, err := canonical.DigestFromBytes(previous)
		if err != nil {
			return domain.Event{}, err
		}
		event.PrevHash = &value
	}
	if target.Valid {
		value, err := canonical.ParseID(target.String)
		if err != nil {
			return domain.Event{}, err
		}
		event.TargetPrincipalID = &value
	}
	if run.Valid {
		value, err := canonical.ParseID(run.String)
		if err != nil {
			return domain.Event{}, err
		}
		event.GenerationRunID = &value
	}
	event.ContentErased = erasureState == "erased" || content == nil
	if event.ContentErased {
		event.Content = "[erased]"
	} else {
		event.Content = string(content)
	}
	return event, nil
}

func scanSingleID(row rowScanner, target *canonical.ID) error {
	var raw string
	if err := row.Scan(&raw); err != nil {
		return err
	}
	parsed, err := canonical.ParseID(raw)
	if err != nil {
		return err
	}
	*target = parsed
	return nil
}

var _ domain.Repository = (*CanonicalRepository)(nil)
