package sqlite

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"

	"mahoroba.local/mahoroba/internal/canonical"
	"mahoroba.local/mahoroba/internal/domain"
	"mahoroba.local/mahoroba/internal/generation"
	"mahoroba.local/mahoroba/internal/integrity"
	"mahoroba.local/mahoroba/internal/memory"
)

type mandatoryRecoveryQueryer interface {
	QueryContext(context.Context, string, ...any) (*sql.Rows, error)
	QueryRowContext(context.Context, string, ...any) *sql.Row
}

// DiscoverMandatoryRecoveryWork scans a bounded number of source events in a
// stable all-resident order and returns only provider-ineligible mandatory
// obligations. The cursor advances over source events, including those that
// produce no work, so available work cannot starve an older erased source.
func (r *CanonicalRepository) DiscoverMandatoryRecoveryWork(
	ctx context.Context,
	cursor *domain.MandatoryRecoveryCursor,
	eventLimit int,
) ([]domain.MandatoryRecoveryWork, *domain.MandatoryRecoveryCursor, error) {
	if eventLimit < 1 {
		return nil, nil, nil
	}
	query := eventSelect + ` WHERE e.event_type IN ('user_message', 'self_talk')`
	args := make([]any, 0, 4)
	if cursor != nil {
		if err := cursor.ResidentID.Validate(); err != nil {
			return nil, nil, fmt.Errorf("sqlite: invalid mandatory recovery cursor resident: %w", err)
		}
		if err := cursor.EventSeq.Validate(); err != nil {
			return nil, nil, fmt.Errorf("sqlite: invalid mandatory recovery cursor event seq: %w", err)
		}
		query += ` AND (e.resident_id > ? OR (e.resident_id = ? AND e.seq > ?))`
		args = append(args, cursor.ResidentID.String(), cursor.ResidentID.String(), cursor.EventSeq.Int64())
	}
	query += ` ORDER BY e.resident_id, e.seq LIMIT ?`
	args = append(args, eventLimit+1)
	rows, err := r.store.reader.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, nil, fmt.Errorf("sqlite: discover mandatory recovery source events: %w", err)
	}
	events := make([]domain.Event, 0, eventLimit+1)
	for rows.Next() {
		event, scanErr := scanEvent(rows)
		if scanErr != nil {
			_ = rows.Close()
			return nil, nil, scanErr
		}
		events = append(events, event)
	}
	if err := rows.Err(); err != nil {
		_ = rows.Close()
		return nil, nil, err
	}
	if err := rows.Close(); err != nil {
		return nil, nil, err
	}
	hasMore := len(events) > eventLimit
	if hasMore {
		events = events[:eventLimit]
	}

	statusRows, err := r.store.reader.QueryContext(ctx, `SELECT resident.resident_id,
		(SELECT transition.to_status
		 FROM resident_status_transitions transition
		 JOIN canonical_commits status_commit
		   ON status_commit.canonical_commit_id = transition.canonical_commit_id
		 WHERE transition.resident_id = resident.resident_id
		 ORDER BY status_commit.commit_seq DESC LIMIT 1)
		FROM residents resident`)
	if err != nil {
		return nil, nil, fmt.Errorf("sqlite: load mandatory recovery resident statuses: %w", err)
	}
	statuses := make(map[canonical.ID]string)
	for statusRows.Next() {
		var residentRaw, status string
		if err := statusRows.Scan(&residentRaw, &status); err != nil {
			_ = statusRows.Close()
			return nil, nil, err
		}
		residentID, err := canonical.ParseID(residentRaw)
		if err != nil {
			_ = statusRows.Close()
			return nil, nil, err
		}
		statuses[residentID] = status
	}
	if err := statusRows.Err(); err != nil {
		_ = statusRows.Close()
		return nil, nil, err
	}
	if err := statusRows.Close(); err != nil {
		return nil, nil, err
	}
	var selectedRaw sql.NullString
	if err := r.store.reader.QueryRowContext(ctx, `SELECT active_resident_id
		FROM runtime_config WHERE singleton_id = 1`).Scan(&selectedRaw); err != nil && !errors.Is(err, sql.ErrNoRows) {
		return nil, nil, fmt.Errorf("sqlite: resolve mandatory recovery resident selection: %w", err)
	}
	var selected canonical.ID
	if selectedRaw.Valid {
		selected, err = canonical.ParseID(selectedRaw.String)
		if err != nil {
			return nil, nil, err
		}
	}

	works := make([]domain.MandatoryRecoveryWork, 0, len(events)*2)
	for _, event := range events {
		status, exists := statuses[event.ResidentID]
		if !exists {
			return nil, nil, fmt.Errorf("sqlite: mandatory recovery source %s has no resident", event.ID)
		}
		if event.Type == "user_message" {
			reason := dialogueRecoveryReason(event.ContentErased, status, selected == event.ResidentID)
			if reason != "" {
				work, actionable, classifyErr := r.classifyMandatoryRecoveryRun(ctx, domain.MandatoryRecoveryWork{
					Kind: domain.MandatoryRecoveryDialogue, SourceEvent: event,
					IdempotencyKey: domain.DialogueObligation(event.ID), CancellationCode: reason,
				})
				if classifyErr != nil {
					return nil, nil, classifyErr
				}
				if actionable {
					works = append(works, work)
				}
			}
		}

		memoryReason := memoryRecoveryReason(event.ContentErased, status)
		if memoryReason == "" {
			continue
		}
		policy, policyErr := resolveEventTimeMandatoryPolicy(ctx, r.store.reader, event.ResidentID, event.ID, event.Type)
		if policyErr != nil {
			return nil, nil, policyErr
		}
		if policy.Resolvable && !policy.Mandatory {
			continue
		}
		work, actionable, classifyErr := r.classifyMandatoryRecoveryRun(ctx, domain.MandatoryRecoveryWork{
			Kind: domain.MandatoryRecoveryMemoryExtraction, SourceEvent: event,
			IdempotencyKey:   domain.MemoryExtractionObligation(event.ID),
			PolicyRevisionID: policy.RevisionID, CancellationCode: memoryReason,
		})
		if classifyErr != nil {
			return nil, nil, classifyErr
		}
		if actionable {
			works = append(works, work)
		}
	}

	var next *domain.MandatoryRecoveryCursor
	if hasMore && len(events) != 0 {
		last := events[len(events)-1]
		next = &domain.MandatoryRecoveryCursor{ResidentID: last.ResidentID, EventSeq: last.Seq}
	}
	return works, next, nil
}

type eventTimeMandatoryPolicy struct {
	RevisionID canonical.ID
	Mandatory  bool
	Resolvable bool
}

// resolveEventTimeMandatoryPolicy distinguishes an operational SQL failure
// from incomplete Canonical history. Missing/erased/missing-blob/malformed
// policy history is an unresolvable cancellation envelope, not a discovery
// failure: recovery must expose a typed integrity candidate instead of
// inventing policy semantics or aborting before the integrity pipeline.
func resolveEventTimeMandatoryPolicy(
	ctx context.Context,
	q mandatoryRecoveryQueryer,
	residentID, eventID canonical.ID,
	eventType string,
) (eventTimeMandatoryPolicy, error) {
	var revisionRaw, erasureState string
	var content []byte
	err := q.QueryRowContext(ctx, `SELECT revision.revision_id, content.erasure_state, blob.content
		FROM events source
		JOIN canonical_commits source_commit
		  ON source_commit.canonical_commit_id = source.canonical_commit_id
		JOIN resident_revision_activations activation
		  ON activation.resident_id = source.resident_id
		JOIN canonical_commits activation_commit
		  ON activation_commit.canonical_commit_id = activation.canonical_commit_id
		JOIN resident_revisions revision ON revision.revision_id = activation.revision_id
		JOIN content_objects content ON content.content_id = revision.content_id
		LEFT JOIN blobs blob ON blob.dedupe_scope_id = content.owner_resident_id
		 AND blob.hash_algorithm = content.blob_hash_algorithm
		 AND blob.blob_hash = content.blob_hash
		WHERE source.event_id = ? AND source.resident_id = ?
		  AND revision.resident_id = source.resident_id
		  AND revision.revision_class = 'memory_policy'
		  AND activation_commit.commit_seq <= source_commit.commit_seq
		ORDER BY activation_commit.commit_seq DESC, activation.activation_id DESC
		LIMIT 1`, eventID.String(), residentID.String()).Scan(&revisionRaw, &erasureState, &content)
	if errors.Is(err, sql.ErrNoRows) {
		return eventTimeMandatoryPolicy{}, nil
	}
	if err != nil {
		return eventTimeMandatoryPolicy{}, fmt.Errorf("sqlite: resolve event-time mandatory memory policy: %w", err)
	}
	revisionID, err := canonical.ParseID(revisionRaw)
	if err != nil || erasureState != "present" || content == nil {
		return eventTimeMandatoryPolicy{}, nil
	}
	policy, _, err := memory.ParsePolicy(content)
	if err != nil {
		return eventTimeMandatoryPolicy{}, nil
	}
	if err := policy.RequireEnabled(); err != nil {
		return eventTimeMandatoryPolicy{RevisionID: revisionID, Resolvable: true}, nil
	}
	return eventTimeMandatoryPolicy{
		RevisionID: revisionID,
		Mandatory:  memoryPolicyRequires(policy, memory.EventType(eventType)),
		Resolvable: true,
	}, nil
}

func dialogueRecoveryReason(sourceErased bool, residentStatus string, selected bool) string {
	if sourceErased {
		return generation.MustOutcomeErrorCode(generation.ErrorSourceContentErased, 0).String()
	}
	if residentStatus != "active" {
		return generation.MustOutcomeErrorCode(generation.ErrorResidentInactive, 0).String()
	}
	if !selected {
		return generation.MustOutcomeErrorCode(generation.ErrorResidentUnselected, 0).String()
	}
	return ""
}

func memoryRecoveryReason(sourceErased bool, residentStatus string) string {
	if sourceErased {
		return generation.MustOutcomeErrorCode(generation.ErrorSourceContentErased, 0).String()
	}
	if residentStatus != "active" {
		return generation.MustOutcomeErrorCode(generation.ErrorResidentInactive, 0).String()
	}
	return ""
}

func (r *CanonicalRepository) classifyMandatoryRecoveryRun(
	ctx context.Context,
	work domain.MandatoryRecoveryWork,
) (domain.MandatoryRecoveryWork, bool, error) {
	var runRaw, purposeRaw string
	err := r.store.reader.QueryRowContext(ctx, `SELECT generation_run_id, purpose
		FROM generation_runs WHERE resident_id = ? AND idempotency_key = ?`,
		work.SourceEvent.ResidentID.String(), work.IdempotencyKey).Scan(&runRaw, &purposeRaw)
	if errors.Is(err, sql.ErrNoRows) {
		work.State = domain.WorkPending
		return work, true, nil
	}
	if err != nil {
		return domain.MandatoryRecoveryWork{}, false, fmt.Errorf("sqlite: resolve mandatory recovery run: %w", err)
	}
	if purposeRaw != string(work.Kind) {
		return domain.MandatoryRecoveryWork{}, false, fmt.Errorf(
			"%w: mandatory obligation %q has purpose %q", ErrInvalidGenerationOutcomeHistory, work.IdempotencyKey, purposeRaw,
		)
	}
	runID, err := canonical.ParseID(runRaw)
	if err != nil {
		return domain.MandatoryRecoveryWork{}, false, err
	}
	work.RunID = &runID
	// Overflow is a command-level, no-mutation recovery failure. Recognize the
	// two states that necessarily need a successor attempt before reducing the
	// complete history: a MaxInt64 row cannot have a legal successor, and the
	// reducer's gap diagnostic must not hide that stronger recovery contract.
	overflowAttempt, overflowState, overflow, err := (generationOutcomeRepository{}).MandatoryRecoveryOverflow(
		ctx, r.store.reader, runID, work.SourceEvent.ResidentID,
	)
	if err != nil {
		return domain.MandatoryRecoveryWork{}, false, fmt.Errorf("sqlite: inspect mandatory recovery overflow: %w", err)
	}
	if overflow {
		work.AttemptNo = overflowAttempt
		work.State = overflowState
		return work, true, nil
	}
	summary, err := (generationOutcomeRepository{}).Summary(ctx, r.store.reader, runID, work.SourceEvent.ResidentID)
	if err != nil {
		return domain.MandatoryRecoveryWork{}, false, err
	}
	work.AttemptNo = summary.Latest.AttemptNo
	switch summary.Latest.State {
	case "running":
		work.State = domain.WorkRunning
		return work, true, nil
	case "succeeded":
		return work, false, nil
	case "failed", "cancelled":
		if !summary.Latest.ErrorClass.Valid {
			return domain.MandatoryRecoveryWork{}, false, invalidOutcome(summary.Latest, "terminal row has no error class")
		}
		code, err := generation.ParseOutcomeErrorCode(summary.Latest.ErrorClass.String)
		if err != nil {
			return domain.MandatoryRecoveryWork{}, false, invalidOutcome(summary.Latest, err.Error())
		}
		if !code.Retryable() && code.Class() != generation.ErrorForegroundPreempted {
			return work, false, nil
		}
		work.State = domain.WorkRetryPending
		return work, true, nil
	default:
		return domain.MandatoryRecoveryWork{}, false, invalidOutcome(summary.Latest, "unknown latest state")
	}
}

// ResolveMandatoryCancellationEnvelope reconstructs runless cancellation
// envelopes exclusively from event-time Canonical history. Existing runs use
// their persisted envelope and never depend on erased input bytes.
func (r *CanonicalRepository) ResolveMandatoryCancellationEnvelope(
	ctx context.Context,
	work domain.MandatoryRecoveryWork,
) (domain.CancellationEnvelopeResolution, error) {
	if work.RunID != nil {
		envelope, err := loadExistingCancellationEnvelope(ctx, r.store.reader, work)
		if err != nil {
			return domain.CancellationEnvelopeResolution{}, err
		}
		return domain.CancellationEnvelopeResolution{Generation: envelope}, nil
	}
	envelope, resolvable, err := resolveSyntheticCancellationEnvelope(ctx, r.store.reader, work)
	if err != nil {
		return domain.CancellationEnvelopeResolution{}, err
	}
	if resolvable {
		return domain.CancellationEnvelopeResolution{Generation: envelope}, nil
	}
	candidate := integrity.CandidateInput{
		ResidentID: work.SourceEvent.ResidentID,
		Kind:       integrity.FindingProvenanceUnresolvable,
		RuleCode:   integrity.RuleCancellationEnvelopeUnresolvable,
		TargetKind: integrity.TargetEvent, TargetID: work.SourceEvent.ID,
		TargetField: "cancellation_envelope",
		OccurredAt:  work.SourceEvent.RecordedAt, OccurredTZ: work.SourceEvent.RecordedTZ,
	}
	if err := candidate.Validate(); err != nil {
		return domain.CancellationEnvelopeResolution{}, err
	}
	return domain.CancellationEnvelopeResolution{Unresolved: &candidate}, nil
}

func loadExistingCancellationEnvelope(
	ctx context.Context,
	q mandatoryRecoveryQueryer,
	work domain.MandatoryRecoveryWork,
) (domain.PrepareGeneration, error) {
	if work.RunID == nil {
		return domain.PrepareGeneration{}, errors.New("sqlite: existing cancellation envelope requires a run")
	}
	var runRaw, residentRaw, purposeRaw, key, provider, model string
	var prompt, pipelineRaw, contextVersion, rendering, principlesRaw, personaRaw, policyRaw string
	var sessionRaw, recallRaw sql.NullString
	var paramsRaw, droppedRaw, asOfTZ string
	var asOf int64
	var budget int
	err := q.QueryRowContext(ctx, `SELECT generation_run_id, resident_id, purpose, idempotency_key,
		provider, model, prompt_template_version, pipeline_version_id, context_policy_version,
		sessionization_policy_version_id, memory_rendering_version, principles_revision_id,
		persona_revision_id, memory_policy_revision_id, recall_run_id, generator_params,
		as_of, as_of_tz, budget_exceeded, dropped_input_summary
		FROM generation_runs WHERE generation_run_id = ? AND resident_id = ?`,
		work.RunID.String(), work.SourceEvent.ResidentID.String()).Scan(
		&runRaw, &residentRaw, &purposeRaw, &key, &provider, &model, &prompt, &pipelineRaw,
		&contextVersion, &sessionRaw, &rendering, &principlesRaw, &personaRaw, &policyRaw,
		&recallRaw, &paramsRaw, &asOf, &asOfTZ, &budget, &droppedRaw,
	)
	if err != nil {
		return domain.PrepareGeneration{}, fmt.Errorf("sqlite: load existing mandatory cancellation envelope: %w", err)
	}
	if purposeRaw != string(work.Kind) || key != work.IdempotencyKey {
		return domain.PrepareGeneration{}, fmt.Errorf("%w: mandatory recovery run envelope mismatch", ErrInvalidGenerationOutcomeHistory)
	}
	envelope := domain.PrepareGeneration{
		Purpose: domain.GenerationPurpose(purposeRaw), IdempotencyKey: key, Provider: provider, Model: model,
		PromptTemplateVersion: prompt, ContextPolicyVersion: contextVersion, MemoryRenderingVersion: rendering,
		AsOf: canonical.Instant(asOf), AsOfTZ: canonical.Timezone(asOfTZ), BudgetExceeded: budget != 0,
	}
	var errParse error
	envelope.RunID, errParse = canonical.ParseID(runRaw)
	if errParse != nil {
		return domain.PrepareGeneration{}, errParse
	}
	envelope.ResidentID, errParse = canonical.ParseID(residentRaw)
	if errParse != nil {
		return domain.PrepareGeneration{}, errParse
	}
	envelope.PipelineVersionID, errParse = canonical.ParseID(pipelineRaw)
	if errParse != nil {
		return domain.PrepareGeneration{}, errParse
	}
	envelope.PrinciplesRevisionID, errParse = canonical.ParseID(principlesRaw)
	if errParse != nil {
		return domain.PrepareGeneration{}, errParse
	}
	envelope.PersonaRevisionID, errParse = canonical.ParseID(personaRaw)
	if errParse != nil {
		return domain.PrepareGeneration{}, errParse
	}
	envelope.MemoryPolicyRevisionID, errParse = canonical.ParseID(policyRaw)
	if errParse != nil {
		return domain.PrepareGeneration{}, errParse
	}
	if sessionRaw.Valid {
		id, parseErr := canonical.ParseID(sessionRaw.String)
		if parseErr != nil {
			return domain.PrepareGeneration{}, parseErr
		}
		envelope.SessionPolicyID = &id
	}
	if recallRaw.Valid {
		id, parseErr := canonical.ParseID(recallRaw.String)
		if parseErr != nil {
			return domain.PrepareGeneration{}, parseErr
		}
		envelope.RecallRunID = &id
	}
	envelope.GeneratorParams, errParse = canonical.ParseCanonicalJSON([]byte(paramsRaw))
	if errParse != nil {
		return domain.PrepareGeneration{}, errParse
	}
	envelope.DroppedInputSummary, errParse = canonical.ParseCanonicalJSON([]byte(droppedRaw))
	if errParse != nil {
		return domain.PrepareGeneration{}, errParse
	}
	return envelope, nil
}

func resolveSyntheticCancellationEnvelope(
	ctx context.Context,
	q mandatoryRecoveryQueryer,
	work domain.MandatoryRecoveryWork,
) (domain.PrepareGeneration, bool, error) {
	if err := work.Kind.Validate(); err != nil {
		return domain.PrepareGeneration{}, false, err
	}
	var sourceCommit int64
	var recordedAt int64
	var recordedTZ string
	var eventType string
	err := q.QueryRowContext(ctx, `SELECT source_commit.commit_seq, event.recorded_at,
		event.recorded_tz, event.event_type
		FROM events event JOIN canonical_commits source_commit
		  ON source_commit.canonical_commit_id = event.canonical_commit_id
		WHERE event.event_id = ? AND event.resident_id = ?`,
		work.SourceEvent.ID.String(), work.SourceEvent.ResidentID.String()).Scan(
		&sourceCommit, &recordedAt, &recordedTZ, &eventType,
	)
	if err != nil {
		return domain.PrepareGeneration{}, false, fmt.Errorf("sqlite: resolve cancellation source event: %w", err)
	}
	if work.Kind == domain.MandatoryRecoveryDialogue && eventType != "user_message" ||
		work.Kind == domain.MandatoryRecoveryMemoryExtraction && eventType != "user_message" && eventType != "self_talk" {
		return domain.PrepareGeneration{}, false, nil
	}
	timezone, err := canonical.ParseTimezone(recordedTZ)
	if err != nil {
		return domain.PrepareGeneration{}, false, err
	}
	revisions := make(map[string]canonical.ID, 3)
	for _, class := range []string{"principles", "persona", "memory_policy"} {
		id, resolvable, resolveErr := resolveCancellationRevision(ctx, q, work.SourceEvent.ResidentID, class, sourceCommit)
		if resolveErr != nil {
			return domain.PrepareGeneration{}, false, resolveErr
		}
		if !resolvable {
			return domain.PrepareGeneration{}, false, nil
		}
		revisions[class] = id
	}
	if work.Kind == domain.MandatoryRecoveryMemoryExtraction && revisions["memory_policy"] != work.PolicyRevisionID {
		return domain.PrepareGeneration{}, false, nil
	}

	purpose := domain.GenerationPurposeDialogue
	var pipelineID canonical.ID
	var versions domain.GenerationVersionContract
	if work.Kind == domain.MandatoryRecoveryDialogue {
		var pipelineVersion string
		var resolvable bool
		pipelineID, pipelineVersion, resolvable, err = resolveCancellationDialoguePipeline(ctx, q, sourceCommit)
		if err != nil {
			return domain.PrepareGeneration{}, false, err
		}
		if !resolvable {
			return domain.PrepareGeneration{}, false, nil
		}
		contract, contractErr := domain.SyntheticDialogueExecutionContract(pipelineVersion)
		if contractErr != nil {
			return domain.PrepareGeneration{}, false, contractErr
		}
		versions = domain.GenerationVersionContract{
			PromptTemplateVersion: contract.PromptTemplateVersion, ContextPolicyVersion: contract.ContextPolicyVersion,
			MemoryRenderingVersion: contract.MemoryRenderingVersion,
		}
	} else {
		purpose = domain.GenerationPurposeMemoryExtraction
		var resolvable bool
		pipelineID, resolvable, err = resolveCancellationPipeline(
			ctx, q, "memory_extraction", domain.MemoryExtractionPipelineVersion, sourceCommit,
		)
		if err != nil {
			return domain.PrepareGeneration{}, false, err
		}
		if !resolvable {
			return domain.PrepareGeneration{}, false, nil
		}
		versions, err = domain.GenerationVersionsForPurpose(purpose)
		if err != nil {
			return domain.PrepareGeneration{}, false, err
		}
	}
	dropped, err := cancellationDroppedSummary(work.Kind, work.CancellationCode)
	if err != nil {
		return domain.PrepareGeneration{}, false, err
	}
	var params canonical.CanonicalJSON
	var sessionID *canonical.ID
	if work.Kind == domain.MandatoryRecoveryDialogue {
		_, params, err = domain.NewUnstructuredGeneratorParams(false, 1)
		if err != nil {
			return domain.PrepareGeneration{}, false, err
		}
		id, found, resolveErr := resolveCancellationSessionPolicy(ctx, q, sourceCommit)
		if resolveErr != nil {
			return domain.PrepareGeneration{}, false, resolveErr
		}
		if !found {
			return domain.PrepareGeneration{}, false, nil
		}
		sessionID = &id
	} else {
		schema, schemaErr := memory.ExtractionJSONSchema()
		if schemaErr != nil {
			return domain.PrepareGeneration{}, false, schemaErr
		}
		_, params, err = domain.NewStructuredGeneratorParams(
			false, 1, generation.StructuredOutputJSONSchema,
			memory.ExtractionOutputSchemaVersionV1, canonical.HashBlob(schema.Bytes()),
		)
		if err != nil {
			return domain.PrepareGeneration{}, false, err
		}
	}
	return domain.PrepareGeneration{
		ResidentID: work.SourceEvent.ResidentID, Purpose: purpose, IdempotencyKey: work.IdempotencyKey,
		Provider: "mahoroba-internal", Model: "not-dispatched",
		PromptTemplateVersion: versions.PromptTemplateVersion,
		ContextPolicyVersion:  versions.ContextPolicyVersion, MemoryRenderingVersion: versions.MemoryRenderingVersion,
		PipelineVersionID: pipelineID, SessionPolicyID: sessionID,
		PrinciplesRevisionID: revisions["principles"], PersonaRevisionID: revisions["persona"],
		MemoryPolicyRevisionID: revisions["memory_policy"], AsOf: canonical.Instant(recordedAt), AsOfTZ: timezone,
		DroppedInputSummary: dropped, GeneratorParams: params,
	}, true, nil
}

func resolveCancellationRevision(
	ctx context.Context,
	q mandatoryRecoveryQueryer,
	residentID canonical.ID,
	class string,
	through int64,
) (canonical.ID, bool, error) {
	var revisionRaw, erasureState string
	var content []byte
	err := q.QueryRowContext(ctx, `SELECT revision.revision_id, content.erasure_state, blob.content
		FROM resident_revision_activations activation
		JOIN canonical_commits activation_commit
		  ON activation_commit.canonical_commit_id = activation.canonical_commit_id
		JOIN resident_revisions revision ON revision.revision_id = activation.revision_id
		JOIN content_objects content ON content.content_id = revision.content_id
		LEFT JOIN blobs blob ON blob.dedupe_scope_id = content.owner_resident_id
		 AND blob.hash_algorithm = content.blob_hash_algorithm AND blob.blob_hash = content.blob_hash
		WHERE activation.resident_id = ? AND revision.resident_id = ?
		  AND revision.revision_class = ? AND activation_commit.commit_seq <= ?
		ORDER BY activation_commit.commit_seq DESC, activation.activation_id DESC LIMIT 1`,
		residentID.String(), residentID.String(), class, through).Scan(&revisionRaw, &erasureState, &content)
	if errors.Is(err, sql.ErrNoRows) {
		return canonical.ID{}, false, nil
	}
	if err != nil {
		return canonical.ID{}, false, fmt.Errorf("sqlite: resolve cancellation %s revision: %w", class, err)
	}
	// Do not fall back to an older present activation. The event-time meaning
	// is fixed by the latest activation; unavailable bytes make the synthetic
	// envelope unresolvable and must surface through integrity.
	if erasureState != "present" || content == nil {
		return canonical.ID{}, false, nil
	}
	id, err := canonical.ParseID(revisionRaw)
	return id, err == nil, err
}

func resolveCancellationPipeline(
	ctx context.Context,
	q mandatoryRecoveryQueryer,
	kind, version string,
	through int64,
) (canonical.ID, bool, error) {
	var rawID, rawDefinition string
	err := q.QueryRowContext(ctx, `SELECT pipeline.pipeline_version_id, pipeline.definition
		FROM pipeline_versions pipeline JOIN canonical_commits pipeline_commit
		  ON pipeline_commit.canonical_commit_id = pipeline.canonical_commit_id
		WHERE pipeline.pipeline_kind = ? AND pipeline.version_key = ?
		  AND pipeline_commit.commit_seq <= ?`, kind, version, through).Scan(&rawID, &rawDefinition)
	if errors.Is(err, sql.ErrNoRows) {
		return canonical.ID{}, false, nil
	}
	if err != nil {
		return canonical.ID{}, false, fmt.Errorf("sqlite: resolve cancellation pipeline: %w", err)
	}
	expected, err := canonical.MarshalCanonical(struct {
		Version string `json:"version"`
	}{Version: version})
	if err != nil || rawDefinition != expected.String() {
		return canonical.ID{}, false, err
	}
	id, err := canonical.ParseID(rawID)
	return id, err == nil, err
}

func resolveCancellationDialoguePipeline(
	ctx context.Context,
	q mandatoryRecoveryQueryer,
	through int64,
) (canonical.ID, string, bool, error) {
	var rawID, versionKey, rawDefinition string
	err := q.QueryRowContext(ctx, `SELECT pipeline.pipeline_version_id, pipeline.version_key, pipeline.definition
		FROM pipeline_versions pipeline
		JOIN canonical_commits pipeline_commit
		  ON pipeline_commit.canonical_commit_id = pipeline.canonical_commit_id
		WHERE pipeline.pipeline_kind = 'dialogue'
		  AND pipeline.version_key IN (?, ?, ?)
		  AND pipeline_commit.commit_seq <= ?
		ORDER BY pipeline_commit.commit_seq DESC, pipeline.pipeline_version_id DESC LIMIT 1`,
		domain.DialoguePipelineVersionV1, domain.DialoguePipelineVersionV2,
		domain.DialoguePipelineVersionV3, through,
	).Scan(&rawID, &versionKey, &rawDefinition)
	if errors.Is(err, sql.ErrNoRows) {
		return canonical.ID{}, "", false, nil
	}
	if err != nil {
		return canonical.ID{}, "", false, fmt.Errorf("sqlite: resolve dialogue cancellation pipeline: %w", err)
	}
	id, err := canonical.ParseID(rawID)
	if err != nil {
		return canonical.ID{}, "", false, err
	}
	definition, err := canonical.ParseCanonicalJSON([]byte(rawDefinition))
	if err != nil {
		return canonical.ID{}, "", false, nil
	}
	if err := domain.ValidateExactDialoguePipelineDefinition(domain.PipelineVersionDefinition{
		ID: id, Kind: "dialogue", VersionKey: versionKey, Definition: definition,
	}); err != nil {
		return canonical.ID{}, "", false, nil
	}
	return id, versionKey, true, nil
}

func resolveCancellationSessionPolicy(
	ctx context.Context,
	q mandatoryRecoveryQueryer,
	through int64,
) (canonical.ID, bool, error) {
	rows, err := q.QueryContext(ctx, `SELECT policy.sessionization_policy_version_id, policy.definition
		FROM sessionization_policy_versions policy JOIN canonical_commits policy_commit
		  ON policy_commit.canonical_commit_id = policy.canonical_commit_id
		WHERE policy_commit.commit_seq <= ?
		ORDER BY policy_commit.commit_seq DESC, policy.sessionization_policy_version_id DESC`, through)
	if err != nil {
		return canonical.ID{}, false, fmt.Errorf("sqlite: resolve cancellation session policy: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var rawID, rawDefinition string
		if err := rows.Scan(&rawID, &rawDefinition); err != nil {
			return canonical.ID{}, false, err
		}
		definition, err := canonical.ParseCanonicalJSON([]byte(rawDefinition))
		if err != nil {
			continue
		}
		var wire struct {
			Version string             `json:"version"`
			IdleGap canonical.Duration `json:"idle_gap_microseconds"`
		}
		if err := json.Unmarshal(definition.Bytes(), &wire); err != nil ||
			wire.Version != domain.SessionPolicyVersion || wire.IdleGap.Microseconds() <= 0 {
			continue
		}
		reencoded, err := canonical.MarshalCanonical(wire)
		if err != nil || !bytes.Equal(reencoded.Bytes(), definition.Bytes()) {
			continue
		}
		id, err := canonical.ParseID(rawID)
		if err != nil {
			return canonical.ID{}, false, err
		}
		return id, true, nil
	}
	if err := rows.Err(); err != nil {
		return canonical.ID{}, false, err
	}
	return canonical.ID{}, false, nil
}

func cancellationDroppedSummary(kind domain.MandatoryRecoveryKind, reason string) (canonical.CanonicalJSON, error) {
	if kind == domain.MandatoryRecoveryDialogue {
		return canonical.MarshalCanonical(struct {
			Backfill canonical.Count `json:"backfill"`
			Live     canonical.Count `json:"live_context"`
			Reason   string          `json:"reason"`
		}{Reason: reason})
	}
	return canonical.MarshalCanonical(struct {
		Reason string `json:"reason"`
	}{Reason: reason})
}

func cancellationEnvelopeSemanticallyEqual(left, right domain.PrepareGeneration) bool {
	return left.ResidentID == right.ResidentID && left.Purpose.Effective() == right.Purpose.Effective() &&
		left.IdempotencyKey == right.IdempotencyKey && left.Provider == right.Provider && left.Model == right.Model &&
		left.PromptTemplateVersion == right.PromptTemplateVersion &&
		left.ContextPolicyVersion == right.ContextPolicyVersion &&
		left.MemoryRenderingVersion == right.MemoryRenderingVersion &&
		left.PipelineVersionID == right.PipelineVersionID && equalRecoveryID(left.SessionPolicyID, right.SessionPolicyID) &&
		left.PrinciplesRevisionID == right.PrinciplesRevisionID && left.PersonaRevisionID == right.PersonaRevisionID &&
		left.MemoryPolicyRevisionID == right.MemoryPolicyRevisionID && equalRecoveryID(left.RecallRunID, right.RecallRunID) &&
		left.AsOf == right.AsOf && left.AsOfTZ == right.AsOfTZ && left.BudgetExceeded == right.BudgetExceeded &&
		bytes.Equal(left.DroppedInputSummary.Bytes(), right.DroppedInputSummary.Bytes()) &&
		bytes.Equal(left.GeneratorParams.Bytes(), right.GeneratorParams.Bytes()) && len(left.Inputs) == 0
}

func equalRecoveryID(left, right *canonical.ID) bool {
	if left == nil || right == nil {
		return left == nil && right == nil
	}
	return *left == *right
}
