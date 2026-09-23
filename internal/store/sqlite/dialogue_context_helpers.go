package sqlite

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"math"
	"sort"

	"mahoroba.local/mahoroba/internal/activity"
	"mahoroba.local/mahoroba/internal/canonical"
	"mahoroba.local/mahoroba/internal/domain"
	"mahoroba.local/mahoroba/internal/memory"
	"mahoroba.local/mahoroba/internal/surfaceref"
)

type dialogueInputCandidate struct {
	role           string
	sourceType     string
	sourceID       *canonical.ID
	inclusionMode  string
	contentID      canonical.ID
	content        []byte
	byteSize       int64
	backfill       bool
	atomicBackfill bool
	live           bool
	eventSeq       canonical.Seq
	recall         bool
	initiative     bool
	recallClaimID  *canonical.ID
}

func dialogueRuntimeProjectionBytes(recallEnabled bool) []byte {
	recallState := "disabled"
	if recallEnabled {
		recallState = "enabled"
	}
	return []byte("runtime_state=active; memory_recall=" + recallState + "; self_talk=disabled")
}

type dialogueSnapshot struct {
	pipelineID   canonical.ID
	sessionID    canonical.ID
	idleGap      canonical.Duration
	principlesID canonical.ID
	personaID    canonical.ID
	memoryID     canonical.ID
	memoryPolicy []byte
	definitions  []dialogueInputCandidate
}

func (u *canonicalUoW) requireOperationallySelectedResident(ctx context.Context, residentID canonical.ID) error {
	var selected string
	if err := u.tx.QueryRowContext(ctx, `SELECT active_resident_id FROM runtime_config WHERE singleton_id = 1`).Scan(&selected); err != nil {
		return fmt.Errorf("sqlite: resolve operational resident selection: %w", err)
	}
	if selected != residentID.String() {
		return errors.New("sqlite: resident is not operationally selected")
	}
	return nil
}

func (u *canonicalUoW) loadLiveDialogueInputs(ctx context.Context, current domain.Event, idleGap canonical.Duration, limit int64, explicit map[canonical.ID]struct{}) ([]dialogueInputCandidate, error) {
	if limit < 1 {
		return nil, errors.New("sqlite: live event limit must be positive")
	}
	var currentCommitRaw int64
	if err := u.tx.QueryRowContext(ctx, `SELECT c.commit_seq
		FROM events e
		JOIN canonical_commits c ON c.canonical_commit_id = e.canonical_commit_id
		WHERE e.event_id = ? AND e.resident_id = ? AND c.commit_seq <= ?`,
		current.ID.String(), current.ResidentID.String(), u.metadata.CommitSeq.Int64(),
	).Scan(&currentCommitRaw); err != nil {
		return nil, fmt.Errorf("sqlite: load current activity event commit: %w", err)
	}
	currentCommit, err := canonical.NewCommitSeq(currentCommitRaw)
	if err != nil {
		return nil, fmt.Errorf("sqlite: parse current activity event commit: %w", err)
	}

	// The bounded dialogue tail supplies the output rows. A separate bounded
	// user-message sample supplies the preceding activity anchors even when a
	// large number of resident replies displaced them from that tail. Both are
	// fed into the same pure calculator used by ad-hoc reads.
	records := map[canonical.ID]activity.Event{
		current.ID: {CommitSeq: currentCommit, Value: current},
	}
	for _, eventTypeClause := range []string{
		"e.event_type IN ('user_message', 'resident_message')",
		"e.event_type = 'user_message'",
	} {
		loaded, err := u.loadActivityEventBatch(ctx, current, eventTypeClause, limit)
		if err != nil {
			return nil, err
		}
		for _, record := range loaded {
			records[record.Value.ID] = record
		}
	}
	inputEvents := make([]activity.Event, 0, len(records))
	for _, record := range records {
		inputEvents = append(inputEvents, record)
	}
	transitions, err := u.loadActivityStatusTransitions(ctx, current.ResidentID)
	if err != nil {
		return nil, err
	}
	session, err := activity.Calculate(activity.Input{
		Events:            inputEvents,
		StatusTransitions: transitions,
		ThroughCommit:     u.metadata.CommitSeq,
		AsOf:              current.RecordedAt,
		IdleGap:           idleGap,
		MaxEvents:         int(limit),
	})
	if err != nil {
		return nil, fmt.Errorf("sqlite: calculate activity session: %w", err)
	}
	if !session.ResidentActive {
		return nil, errors.New("sqlite: resident became inactive during activity calculation")
	}

	live, err := u.loadPresentLiveDialogueInputs(ctx, current, session.StartSeq, limit, explicit)
	if err != nil || len(live) != 0 {
		return live, err
	}
	markers, err := surfaceref.Detect([]byte(current.Content))
	if err != nil {
		return nil, fmt.Errorf("sqlite: detect dialogue source surface reference: %w", err)
	}
	if len(markers) == 0 {
		return live, nil
	}
	initiative, err := u.loadInitiativeDialogueInput(ctx, current, explicit)
	if err != nil {
		return nil, err
	}
	if initiative != nil {
		return live, nil
	}
	return u.loadAutomaticDialogueBackfill(ctx, current, session.StartSeq)
}

const automaticDialogueBackfillEventTailQuery = `SELECT e.event_id, e.seq, e.event_type, e.content_id,
		object.erasure_state, blob.content
	FROM events e INDEXED BY idx_events_resident_type_seq
	JOIN canonical_commits commit_row ON commit_row.canonical_commit_id = e.canonical_commit_id
	JOIN content_objects object ON object.content_id = e.content_id
	LEFT JOIN blobs blob ON blob.dedupe_scope_id = object.owner_resident_id
	 AND blob.hash_algorithm = object.blob_hash_algorithm AND blob.blob_hash = object.blob_hash
	WHERE e.resident_id = ? AND e.event_type = ? AND e.visibility = 'conversation'
	  AND e.seq < ? AND commit_row.commit_seq <= ?
	ORDER BY e.seq DESC LIMIT 2`

// loadAutomaticDialogueBackfill restores at most the immediately preceding
// user event or complete user/resident exchange. It never searches past an
// erased, incomplete, or differently typed latest conversation event.
func (u *canonicalUoW) loadAutomaticDialogueBackfill(
	ctx context.Context,
	current domain.Event,
	sessionStart canonical.Seq,
) ([]dialogueInputCandidate, error) {
	type backfillRow struct {
		eventID      canonical.ID
		seq          canonical.Seq
		eventType    string
		contentID    canonical.ID
		erasureState string
		content      []byte
	}
	loaded := make([]backfillRow, 0, 6)
	for _, requestedType := range []string{"user_message", "resident_message", "outbound_initiative"} {
		rows, err := u.tx.QueryContext(ctx, automaticDialogueBackfillEventTailQuery,
			current.ResidentID.String(), requestedType, sessionStart.Int64(), u.metadata.CommitSeq.Int64(),
		)
		if err != nil {
			return nil, fmt.Errorf("sqlite: load automatic dialogue Backfill %s tail: %w", requestedType, err)
		}
		for rows.Next() {
			var eventRaw, eventType, contentRaw, erasureState string
			var rawSeq int64
			var content []byte
			if err := rows.Scan(&eventRaw, &rawSeq, &eventType, &contentRaw, &erasureState, &content); err != nil {
				_ = rows.Close()
				return nil, err
			}
			eventID, err := canonical.ParseID(eventRaw)
			if err != nil {
				_ = rows.Close()
				return nil, err
			}
			seq, err := canonical.NewSeq(rawSeq)
			if err != nil {
				_ = rows.Close()
				return nil, err
			}
			contentID, err := canonical.ParseID(contentRaw)
			if err != nil {
				_ = rows.Close()
				return nil, err
			}
			loaded = append(loaded, backfillRow{
				eventID: eventID, seq: seq, eventType: eventType, contentID: contentID,
				erasureState: erasureState, content: append([]byte(nil), content...),
			})
		}
		if err := rows.Err(); err != nil {
			_ = rows.Close()
			return nil, err
		}
		if err := rows.Close(); err != nil {
			return nil, err
		}
	}
	sort.Slice(loaded, func(left, right int) bool { return loaded[left].seq > loaded[right].seq })
	if len(loaded) > 2 {
		loaded = loaded[:2]
	}
	if len(loaded) == 0 || loaded[0].eventType == "outbound_initiative" {
		return nil, nil
	}
	selected := loaded[:1]
	if loaded[0].eventType == "resident_message" {
		if len(loaded) < 2 || loaded[1].eventType != "user_message" {
			return nil, nil
		}
		selected = loaded[:2]
	} else if loaded[0].eventType != "user_message" {
		return nil, nil
	}
	for _, event := range selected {
		if event.erasureState != "present" {
			return nil, nil
		}
		if event.content == nil {
			return nil, errors.New("sqlite: automatic dialogue Backfill content blob is unavailable")
		}
	}
	result := make([]dialogueInputCandidate, 0, len(selected))
	for index := len(selected) - 1; index >= 0; index-- {
		event := selected[index]
		role := "assistant"
		if event.eventType == "user_message" {
			role = "user"
		}
		eventID := event.eventID
		result = append(result, dialogueInputCandidate{
			role: role, sourceType: "event", sourceID: &eventID, inclusionMode: "context_backfill",
			contentID: event.contentID, content: append([]byte(nil), event.content...), byteSize: int64(len(event.content)),
			backfill: true, atomicBackfill: true, eventSeq: event.seq,
		})
	}
	return result, nil
}

func (u *canonicalUoW) loadActivityEventBatch(ctx context.Context, current domain.Event, eventTypeClause string, limit int64) ([]activity.Event, error) {
	query := `SELECT c.commit_seq, e.event_id, e.seq, e.event_type, e.recorded_at, e.content_id
		FROM events e
		JOIN canonical_commits c ON c.canonical_commit_id = e.canonical_commit_id
		WHERE e.resident_id = ? AND e.seq < ? AND c.commit_seq <= ? AND ` + eventTypeClause + `
		ORDER BY e.seq DESC LIMIT ?`
	rows, err := u.tx.QueryContext(ctx, query, current.ResidentID.String(), current.Seq.Int64(), u.metadata.CommitSeq.Int64(), limit)
	if err != nil {
		return nil, fmt.Errorf("sqlite: load activity event sample: %w", err)
	}
	defer rows.Close()
	var result []activity.Event
	for rows.Next() {
		var commitSeq, seq, recordedAt int64
		var eventRaw, eventType, contentRaw string
		if err := rows.Scan(&commitSeq, &eventRaw, &seq, &eventType, &recordedAt, &contentRaw); err != nil {
			return nil, err
		}
		eventID, err := canonical.ParseID(eventRaw)
		if err != nil {
			return nil, err
		}
		contentID, err := canonical.ParseID(contentRaw)
		if err != nil {
			return nil, err
		}
		canonicalSeq, err := canonical.NewSeq(seq)
		if err != nil {
			return nil, err
		}
		canonicalCommit, err := canonical.NewCommitSeq(commitSeq)
		if err != nil {
			return nil, err
		}
		result = append(result, activity.Event{
			CommitSeq: canonicalCommit,
			Value: domain.Event{
				ID: eventID, ResidentID: current.ResidentID, Seq: canonicalSeq, Type: eventType,
				RecordedAt: canonical.Instant(recordedAt), ContentID: contentID,
			},
		})
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return result, nil
}

func (u *canonicalUoW) loadPresentLiveDialogueInputs(ctx context.Context, current domain.Event, startSeq canonical.Seq, limit int64, explicit map[canonical.ID]struct{}) ([]dialogueInputCandidate, error) {
	if startSeq <= 0 || limit <= 1 {
		return nil, nil
	}
	rows, err := u.tx.QueryContext(ctx, `SELECT e.event_id, e.seq, e.event_type, e.content_id, b.content
		FROM events e
		JOIN content_objects co ON co.content_id = e.content_id
		LEFT JOIN blobs b ON b.dedupe_scope_id = co.owner_resident_id
		 AND b.hash_algorithm = co.blob_hash_algorithm AND b.blob_hash = co.blob_hash
		WHERE e.resident_id = ? AND e.seq >= ? AND e.seq < ?
		  AND e.event_type IN ('user_message', 'resident_message')
		  AND co.erasure_state = 'present' AND b.content IS NOT NULL
		ORDER BY e.seq DESC LIMIT ?`, current.ResidentID.String(), startSeq.Int64(), current.Seq.Int64(), limit-1)
	if err != nil {
		return nil, fmt.Errorf("sqlite: load present live dialogue inputs: %w", err)
	}
	defer rows.Close()
	var descending []dialogueInputCandidate
	for rows.Next() {
		var eventRaw, eventType, contentRaw string
		var eventSeq int64
		var content []byte
		if err := rows.Scan(&eventRaw, &eventSeq, &eventType, &contentRaw, &content); err != nil {
			return nil, err
		}
		eventID, err := canonical.ParseID(eventRaw)
		if err != nil {
			return nil, err
		}
		if _, requested := explicit[eventID]; requested {
			continue
		}
		contentID, err := canonical.ParseID(contentRaw)
		if err != nil {
			return nil, err
		}
		role := "assistant"
		if eventType == "user_message" {
			role = "user"
		}
		copyID := eventID
		parsedSeq, err := canonical.NewSeq(eventSeq)
		if err != nil {
			return nil, err
		}
		descending = append(descending, dialogueInputCandidate{
			role: role, sourceType: "event", sourceID: &copyID, inclusionMode: "live_context",
			contentID: contentID, content: append([]byte(nil), content...), byteSize: int64(len(content)),
			live: true, eventSeq: parsedSeq,
		})
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	for left, right := 0, len(descending)-1; left < right; left, right = left+1, right-1 {
		descending[left], descending[right] = descending[right], descending[left]
	}
	return descending, nil
}

func (u *canonicalUoW) loadActivityStatusTransitions(ctx context.Context, residentID canonical.ID) ([]activity.StatusTransition, error) {
	rows, err := u.tx.QueryContext(ctx, `SELECT c.commit_seq, t.recorded_at, t.to_status
		FROM resident_status_transitions t
		JOIN canonical_commits c ON c.canonical_commit_id = t.canonical_commit_id
		WHERE t.resident_id = ? AND c.commit_seq <= ?
		ORDER BY c.commit_seq`, residentID.String(), u.metadata.CommitSeq.Int64())
	if err != nil {
		return nil, fmt.Errorf("sqlite: load activity lifecycle: %w", err)
	}
	defer rows.Close()
	var result []activity.StatusTransition
	for rows.Next() {
		var commitSeq, recordedAt int64
		var status string
		if err := rows.Scan(&commitSeq, &recordedAt, &status); err != nil {
			return nil, err
		}
		canonicalCommit, err := canonical.NewCommitSeq(commitSeq)
		if err != nil {
			return nil, err
		}
		result = append(result, activity.StatusTransition{
			CommitSeq: canonicalCommit, RecordedAt: canonical.Instant(recordedAt), Status: status,
		})
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return result, nil
}

func (u *canonicalUoW) loadInitiativeDialogueInput(
	ctx context.Context,
	current domain.Event,
	explicit map[canonical.ID]struct{},
) (*dialogueInputCandidate, error) {
	var eventRaw, eventType, contentRaw, erasure string
	var seq int64
	var content []byte
	err := u.tx.QueryRowContext(ctx, `SELECT event.event_id, event.seq, event.event_type,
		event.content_id, object.erasure_state, blob.content
		FROM events event
		JOIN content_objects object ON object.content_id = event.content_id
		LEFT JOIN blobs blob ON blob.dedupe_scope_id = object.owner_resident_id
		 AND blob.hash_algorithm = object.blob_hash_algorithm AND blob.blob_hash = object.blob_hash
		WHERE event.resident_id = ? AND event.visibility = 'conversation' AND event.seq < ?
		ORDER BY event.seq DESC LIMIT 1`, current.ResidentID.String(), current.Seq.Int64()).Scan(
		&eventRaw, &seq, &eventType, &contentRaw, &erasure, &content,
	)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	if eventType != "outbound_initiative" {
		return nil, nil
	}
	eventID, err := canonical.ParseID(eventRaw)
	if err != nil {
		return nil, err
	}
	if _, alreadyExplicit := explicit[eventID]; alreadyExplicit {
		return nil, nil
	}
	if erasure != "present" || content == nil {
		return nil, nil
	}
	contentID, err := canonical.ParseID(contentRaw)
	if err != nil {
		return nil, err
	}
	eventSeq, err := canonical.NewSeq(seq)
	if err != nil {
		return nil, err
	}
	return &dialogueInputCandidate{
		role: "assistant", sourceType: "event", sourceID: &eventID, inclusionMode: "context_backfill",
		contentID: contentID, content: append([]byte(nil), content...), byteSize: int64(len(content)),
		initiative: true, eventSeq: eventSeq,
	}, nil
}

func applyDialogueBudget(
	candidates []dialogueInputCandidate,
	maximum int64,
	maximumInputs int,
) ([]dialogueInputCandidate, bool, int, int, int, map[canonical.ID]struct{}, error) {
	if maximumInputs < 1 {
		return nil, false, 0, 0, 0, nil, errors.New("sqlite: dialogue input limit must be positive")
	}
	total := int64(0)
	for _, candidate := range candidates {
		if candidate.byteSize < 0 {
			return nil, false, 0, 0, 0, nil, errors.New("sqlite: dialogue input has negative byte size")
		}
		if total > math.MaxInt64-candidate.byteSize {
			total = math.MaxInt64
			continue
		}
		total += candidate.byteSize
	}
	budgetExceeded := total > maximum || len(candidates) > maximumInputs
	droppedBackfill := 0
	droppedLive := 0
	droppedInitiative := 0
	droppedRecall := make(map[canonical.ID]struct{})
	for total > maximum || len(candidates) > maximumInputs {
		index := lastRecallCandidate(candidates)
		if index >= 0 {
			if candidates[index].recallClaimID == nil {
				return nil, false, 0, 0, 0, nil, errors.New("sqlite: Recall input has no claim provenance")
			}
			droppedRecall[*candidates[index].recallClaimID] = struct{}{}
		} else {
			index = oldestDialogueCandidate(candidates, true)
			if index >= 0 {
				if candidates[index].atomicBackfill {
					kept := candidates[:0]
					for _, candidate := range candidates {
						if candidate.atomicBackfill {
							total -= candidate.byteSize
							droppedBackfill++
							continue
						}
						kept = append(kept, candidate)
					}
					candidates = kept
					continue
				}
				droppedBackfill++
			} else {
				index = oldestDialogueCandidate(candidates, false)
				if index >= 0 {
					droppedLive++
				} else {
					index = initiativeDialogueCandidate(candidates)
					if index >= 0 {
						droppedInitiative++
					}
				}
			}
		}
		if index < 0 {
			return nil, false, 0, 0, 0, nil, fmt.Errorf("sqlite: mandatory generation inputs exceed %d-byte/%d-input budget", maximum, maximumInputs)
		}
		total -= candidates[index].byteSize
		candidates = append(candidates[:index], candidates[index+1:]...)
	}
	return candidates, budgetExceeded, droppedBackfill, droppedLive, droppedInitiative, droppedRecall, nil
}

func initiativeDialogueCandidate(candidates []dialogueInputCandidate) int {
	for index, candidate := range candidates {
		if candidate.initiative {
			return index
		}
	}
	return -1
}

func lastRecallCandidate(candidates []dialogueInputCandidate) int {
	for index := len(candidates) - 1; index >= 0; index-- {
		if candidates[index].recall {
			return index
		}
	}
	return -1
}

func oldestDialogueCandidate(candidates []dialogueInputCandidate, backfill bool) int {
	index := -1
	for candidateIndex, candidate := range candidates {
		matches := candidate.backfill
		if !backfill {
			matches = candidate.live
		}
		if !matches {
			continue
		}
		if index < 0 || candidate.eventSeq < candidates[index].eventSeq {
			index = candidateIndex
		}
	}
	return index
}

func recallContextCompatibility() canonical.Ratio {
	value, _ := canonical.NewRatio(canonical.FixedPointScale)
	return value
}

func recallUsagesFromPlan(
	plan memory.RecallPlan,
	ids []canonical.ID,
	dropped map[canonical.ID]struct{},
) []domain.RecallUsage {
	result := make([]domain.RecallUsage, 0, len(plan.Decisions)*3)
	nextID := 0
	appendUsage := func(claimID canonical.ID, kind memory.UsageType, ordinal canonical.Ordinal, exclusion memory.RecallExclusionReason) {
		result = append(result, domain.RecallUsage{
			ID: ids[nextID], ClaimID: claimID, Type: kind, Ordinal: ordinal, ExclusionReason: exclusion,
		})
		nextID++
	}
	for _, decision := range plan.Decisions {
		appendUsage(
			decision.Candidate.Candidate.ClaimID, memory.UsageCandidate,
			decision.Candidate.CandidateOrdinal, "",
		)
	}
	for selectedOrdinal := int64(0); ; selectedOrdinal++ {
		decision, exists := recallDecisionBySelectedOrdinal(plan.Decisions, selectedOrdinal)
		if !exists {
			break
		}
		exclusion := decision.ExclusionReason
		if _, wasDropped := dropped[decision.Candidate.Candidate.ClaimID]; wasDropped {
			exclusion = memory.ExclusionTokenBudget
		}
		ordinal, _ := canonical.NewOrdinal(selectedOrdinal)
		appendUsage(decision.Candidate.Candidate.ClaimID, memory.UsageSelected, ordinal, exclusion)
	}
	promptOrdinal := int64(0)
	for originalOrdinal := int64(0); ; originalOrdinal++ {
		decision, exists := recallDecisionByPromptOrdinal(plan.Decisions, originalOrdinal)
		if !exists {
			break
		}
		if _, wasDropped := dropped[decision.Candidate.Candidate.ClaimID]; wasDropped {
			continue
		}
		ordinal, _ := canonical.NewOrdinal(promptOrdinal)
		appendUsage(decision.Candidate.Candidate.ClaimID, memory.UsagePromptIncluded, ordinal, "")
		promptOrdinal++
	}
	return result
}

func recallDecisionBySelectedOrdinal(decisions []memory.RecallDecision, ordinal int64) (memory.RecallDecision, bool) {
	for _, decision := range decisions {
		if decision.SelectedOrdinal != nil && decision.SelectedOrdinal.Int64() == ordinal {
			return decision, true
		}
	}
	return memory.RecallDecision{}, false
}

func recallDecisionByPromptOrdinal(decisions []memory.RecallDecision, ordinal int64) (memory.RecallDecision, bool) {
	for _, decision := range decisions {
		if decision.PromptOrdinal != nil && decision.PromptOrdinal.Int64() == ordinal {
			return decision, true
		}
	}
	return memory.RecallDecision{}, false
}

func dialogueDroppedInputSummary(
	droppedBackfill, droppedLive, droppedInitiative int,
	recallReason domain.MemoryRecallFallbackReason,
) (canonical.CanonicalJSON, error) {
	if recallReason != "" {
		if err := recallReason.Validate(); err != nil {
			return canonical.CanonicalJSON{}, err
		}
	}
	return canonical.MarshalCanonical(struct {
		Backfill     canonical.Count                   `json:"backfill"`
		Live         canonical.Count                   `json:"live_context"`
		MemoryRecall domain.MemoryRecallFallbackReason `json:"memory_recall,omitempty"`
		Initiative   canonical.Count                   `json:"initiative_context,omitempty"`
	}{
		Backfill: canonical.Count(droppedBackfill), Live: canonical.Count(droppedLive),
		MemoryRecall: recallReason, Initiative: canonical.Count(droppedInitiative),
	})
}
