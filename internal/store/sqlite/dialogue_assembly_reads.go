package sqlite

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"

	"mahoroba.local/mahoroba/internal/canonical"
	"mahoroba.local/mahoroba/internal/domain"
	"mahoroba.local/mahoroba/internal/memory"
	"mahoroba.local/mahoroba/internal/projection"
)

// AssembleDialogue creates a complete immutable Prepare plan from one
// read-only SQLite snapshot. It performs no Canonical or Projection writes.
func (r *CanonicalRepository) AssembleDialogue(
	ctx context.Context,
	request domain.DialogueAssemblyRequest,
) (domain.DialogueAssemblyResult, error) {
	if err := request.Validate(); err != nil {
		return domain.DialogueAssemblyResult{}, err
	}
	tx, err := r.store.reader.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return domain.DialogueAssemblyResult{}, fmt.Errorf("sqlite: begin dialogue Assembly snapshot: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	u := &canonicalUoW{tx: tx, metadata: canonical.CommitMetadata{
		CommitSeq: request.Target.Head.CommitSeq, CommittedAt: request.Target.AsOf, CommittedTZ: request.Target.AsOfTZ,
	}}
	if err := requireDialogueAssemblyHead(ctx, tx, request.Target); err != nil {
		return domain.DialogueAssemblyResult{}, err
	}
	if err := u.requireOperationallySelectedResident(ctx, request.ResidentID); err != nil {
		return domain.DialogueAssemblyResult{}, err
	}
	snapshot, err := u.loadDialogueSnapshotAt(ctx, request.ResidentID, request.Target)
	if err != nil {
		return domain.DialogueAssemblyResult{}, err
	}
	if err := requireDialogueAssemblyWatermarks(ctx, tx, request.ResidentID, request.Target, snapshot); err != nil {
		return domain.DialogueAssemblyResult{}, err
	}
	current, currentBytes, err := loadDialogueAssemblySource(ctx, tx, request)
	if err != nil {
		return domain.DialogueAssemblyResult{}, err
	}
	recallSnapshot, err := u.loadDialogueRecallSnapshotAt(ctx, request.ResidentID, snapshot, request.Target)
	if err != nil {
		return domain.DialogueAssemblyResult{}, err
	}

	base := append([]dialogueInputCandidate(nil), snapshot.definitions...)
	runtimeBytes := dialogueRuntimeProjectionBytes(recallSnapshot.enabled)
	base = append(base, dialogueInputCandidate{
		role: "system", sourceType: "runtime_projection", inclusionMode: "runtime_projection",
		content: runtimeBytes, byteSize: int64(len(runtimeBytes)),
	})
	live, err := u.loadLiveDialogueInputs(ctx, current, snapshot.idleGap, request.LiveEventLimit, nil)
	if err != nil {
		return domain.DialogueAssemblyResult{}, err
	}
	base = append(base, live...)
	initiative, err := u.loadInitiativeDialogueInput(ctx, current, nil)
	if err != nil {
		return domain.DialogueAssemblyResult{}, err
	}
	if initiative != nil {
		base = append(base, *initiative)
	}
	currentID := current.ID
	base = append(base, dialogueInputCandidate{
		role: "user", sourceType: "event", sourceID: &currentID, inclusionMode: "current_input",
		contentID: current.ContentID, content: append([]byte(nil), currentBytes...), byteSize: int64(len(currentBytes)),
	})

	disposition := domain.RecallDispositionPolicyDisabled
	var recall *domain.DialogueRecall
	var recallCandidates []domain.RecallCandidateSnapshot
	finalCandidates := base
	budgetExceeded := false
	droppedBackfill, droppedLive, droppedInitiative := 0, 0, 0
	if recallSnapshot.enabled && recallSnapshot.fallback != "" {
		disposition = domain.RecallDispositionExplicitFallback
		finalCandidates, budgetExceeded, droppedBackfill, droppedLive, droppedInitiative, _, err =
			applyDialogueBudget(base, request.MaxInputBytes, len(request.InputIDs))
		if err != nil {
			return domain.DialogueAssemblyResult{}, err
		}
	} else if recallSnapshot.enabled {
		disposition = domain.RecallDispositionSuccess
		selection, selectErr := memory.SelectRecall(recallSnapshot.policy, recallSnapshot.candidates)
		if selectErr != nil {
			return domain.DialogueAssemblyResult{}, selectErr
		}
		if err := u.revalidateRecallSelection(ctx, current.ResidentID, snapshot.memoryID, request.Target.Head.CommitSeq, selection); err != nil {
			return domain.DialogueAssemblyResult{}, err
		}
		var plan memory.RecallPlan
		var droppedRecall map[canonical.ID]struct{}
		finalCandidates, plan, droppedRecall, budgetExceeded, droppedBackfill, droppedLive, droppedInitiative, err =
			assembleDialogueContextV2(base, recallSnapshot.policy, selection, request.MaxInputBytes, len(request.InputIDs))
		if err != nil {
			return domain.DialogueAssemblyResult{}, err
		}
		query, constraints, jsonErr := domain.NewRecallDecisionJSON(
			request.Target.Head.CommitSeq, recallSnapshot.policy, recallContextCompatibility(),
		)
		if jsonErr != nil {
			return domain.DialogueAssemblyResult{}, jsonErr
		}
		recall = &domain.DialogueRecall{
			RunID: request.RecallRunID, ResidentID: request.ResidentID, QueryContentID: current.ContentID,
			PipelineVersionID: recallSnapshot.pipelineID, MemoryPolicyRevisionID: snapshot.memoryID,
			AsOf: request.Target.AsOf, AsOfTZ: request.Target.AsOfTZ,
			QueryConditions: query, ContextConstraints: constraints,
			Usages: recallUsagesFromPlan(plan, request.RecallUsageIDs, droppedRecall),
		}
		recallCandidates = dialogueRecallCandidateSnapshots(selection)
	} else {
		finalCandidates, budgetExceeded, droppedBackfill, droppedLive, droppedInitiative, _, err =
			applyDialogueBudget(base, request.MaxInputBytes, len(request.InputIDs))
		if err != nil {
			return domain.DialogueAssemblyResult{}, err
		}
	}

	dropped, err := dialogueDroppedInputSummary(
		droppedBackfill, droppedLive, droppedInitiative, recallSnapshot.fallback,
	)
	if err != nil {
		return domain.DialogueAssemblyResult{}, err
	}
	generation := domain.PrepareGeneration{
		RunID: request.RunID, ResidentID: request.ResidentID, Purpose: domain.GenerationPurposeDialogue,
		IdempotencyKey: domain.DialogueObligation(current.ID), Provider: request.Provider, Model: request.Model,
		PromptTemplateVersion:  domain.DialoguePromptTemplateVersionV1,
		ContextPolicyVersion:   domain.DialogueContextPolicyVersionV3,
		MemoryRenderingVersion: domain.MemoryRenderingVersionV2,
		PipelineVersionID:      snapshot.pipelineID, SessionPolicyID: &snapshot.sessionID,
		PrinciplesRevisionID: snapshot.principlesID, PersonaRevisionID: snapshot.personaID,
		MemoryPolicyRevisionID: snapshot.memoryID, AsOf: request.Target.AsOf, AsOfTZ: request.Target.AsOfTZ,
		BudgetExceeded: budgetExceeded, DroppedInputSummary: dropped, GeneratorParams: request.GeneratorParams,
		RunningOutcomeID: request.RunningOutcomeID,
	}
	if recall != nil {
		recallID := recall.RunID
		generation.RecallRunID = &recallID
	}
	contents := make([]domain.Content, 0, len(finalCandidates))
	for index, candidate := range finalCandidates {
		content, contentErr := frozenDialogueInputContent(
			request.ResidentID, request.InputContentIDs[index], request.InputContentSalts[index], candidate.content,
		)
		if contentErr != nil {
			return domain.DialogueAssemblyResult{}, contentErr
		}
		generation.Inputs = append(generation.Inputs, domain.GenerationInput{
			ID: request.InputIDs[index], Ordinal: int64(index), Role: candidate.role,
			SourceType: candidate.sourceType, SourceID: candidate.sourceID,
			InclusionMode: candidate.inclusionMode, Content: content,
		})
		contents = append(contents, content)
	}
	prepare := domain.PrepareDialogue{
		SourceEventID: current.ID, Target: request.Target,
		MaxInputBytes: request.MaxInputBytes, LiveEventLimit: request.LiveEventLimit,
		Generation: generation, RecallDisposition: disposition,
		RecallFallbackReason: recallSnapshot.fallback, Recall: recall, RecallCandidates: recallCandidates,
	}
	if err := domain.PrepareDialogueCommand(prepare).Validate(); err != nil {
		return domain.DialogueAssemblyResult{}, fmt.Errorf("sqlite: validate assembled dialogue: %w", err)
	}
	return domain.DialogueAssemblyResult{Prepare: prepare, Contents: contents}, nil
}

func requireDialogueAssemblyHead(ctx context.Context, tx *sql.Tx, target domain.AssemblyTarget) error {
	var sequence, committedAt int64
	err := tx.QueryRowContext(ctx, `SELECT commit_seq, committed_at
		FROM canonical_commits ORDER BY commit_seq DESC LIMIT 1`).Scan(&sequence, &committedAt)
	if err != nil {
		return fmt.Errorf("sqlite: capture dialogue Assembly head: %w", err)
	}
	if sequence != target.Head.CommitSeq.Int64() || canonical.Instant(committedAt) != target.Head.CommittedAt {
		return fmt.Errorf("%w: expected %+v, got seq=%d committed_at=%d",
			domain.ErrDialogueAssemblyTargetChanged, target.Head, sequence, committedAt)
	}
	return nil
}

func (u *canonicalUoW) loadDialogueSnapshotAt(
	ctx context.Context,
	residentID canonical.ID,
	target domain.AssemblyTarget,
) (dialogueSnapshot, error) {
	var snapshot dialogueSnapshot
	var pipelineRaw, definitionRaw string
	err := u.tx.QueryRowContext(ctx, `SELECT pipeline.pipeline_version_id, pipeline.definition
		FROM pipeline_versions pipeline
		JOIN canonical_commits commit_row ON commit_row.canonical_commit_id = pipeline.canonical_commit_id
		WHERE pipeline.pipeline_kind = 'dialogue' AND pipeline.version_key = ?
		  AND commit_row.commit_seq <= ? AND pipeline.recorded_at <= ?`,
		domain.DialoguePipelineVersionV3, target.Head.CommitSeq.Int64(), target.AsOf.UnixMicro(),
	).Scan(&pipelineRaw, &definitionRaw)
	if err != nil {
		return dialogueSnapshot{}, fmt.Errorf("sqlite: resolve target dialogue pipeline: %w", err)
	}
	parsedPipeline, err := canonical.ParseID(pipelineRaw)
	if err != nil {
		return dialogueSnapshot{}, err
	}
	definition, err := canonical.ParseCanonicalJSON([]byte(definitionRaw))
	if err != nil {
		return dialogueSnapshot{}, err
	}
	if err := domain.ValidateExactDialoguePipelineDefinition(domain.PipelineVersionDefinition{
		ID: parsedPipeline, Kind: "dialogue", VersionKey: domain.DialoguePipelineVersionV3, Definition: definition,
	}); err != nil {
		return dialogueSnapshot{}, err
	}
	snapshot.pipelineID = parsedPipeline

	var sessionRaw, sessionDefinitionRaw string
	var selectionUpdatedAt int64
	err = u.tx.QueryRowContext(ctx, `SELECT config.desired_sessionization_policy_version_id,
		policy.definition, config.updated_at
		FROM runtime_config config
		JOIN sessionization_policy_versions policy
		  ON policy.sessionization_policy_version_id = config.desired_sessionization_policy_version_id
		JOIN canonical_commits commit_row ON commit_row.canonical_commit_id = policy.canonical_commit_id
		WHERE config.singleton_id = 1 AND config.active_resident_id = ?
		  AND config.desired_sessionization_policy_version_id IS NOT NULL
		  AND commit_row.commit_seq <= ? AND policy.recorded_at <= ?`,
		residentID.String(), target.Head.CommitSeq.Int64(), target.AsOf.UnixMicro(),
	).Scan(&sessionRaw, &sessionDefinitionRaw, &selectionUpdatedAt)
	if err != nil {
		return dialogueSnapshot{}, fmt.Errorf("sqlite: resolve target Sessionization Policy: %w", err)
	}
	if canonical.Instant(selectionUpdatedAt) > target.AsOf {
		return dialogueSnapshot{}, domain.ErrDialogueAssemblyTargetChanged
	}
	snapshot.sessionID, err = canonical.ParseID(sessionRaw)
	if err != nil {
		return dialogueSnapshot{}, err
	}
	var sessionDefinition struct {
		Version string             `json:"version"`
		IdleGap canonical.Duration `json:"idle_gap_microseconds"`
	}
	if err := json.Unmarshal([]byte(sessionDefinitionRaw), &sessionDefinition); err != nil {
		return dialogueSnapshot{}, fmt.Errorf("sqlite: decode target Sessionization Policy: %w", err)
	}
	if sessionDefinition.Version != domain.SessionPolicyVersion || sessionDefinition.IdleGap.Microseconds() <= 0 {
		return dialogueSnapshot{}, errors.New("sqlite: unsupported target Sessionization Policy definition")
	}
	snapshot.idleGap = sessionDefinition.IdleGap

	for _, revision := range []struct {
		class string
		id    *canonical.ID
	}{
		{class: "principles", id: &snapshot.principlesID},
		{class: "persona", id: &snapshot.personaID},
		{class: "memory_policy", id: &snapshot.memoryID},
	} {
		var revisionRaw, contentRaw string
		var content []byte
		err := u.tx.QueryRowContext(ctx, `SELECT revision.revision_id, revision.content_id, blob.content
			FROM resident_revision_activations activation
			JOIN canonical_commits activation_commit
			  ON activation_commit.canonical_commit_id = activation.canonical_commit_id
			JOIN resident_revisions revision ON revision.revision_id = activation.revision_id
			JOIN content_objects content ON content.content_id = revision.content_id
			LEFT JOIN blobs blob ON blob.dedupe_scope_id = content.owner_resident_id
			 AND blob.hash_algorithm = content.blob_hash_algorithm AND blob.blob_hash = content.blob_hash
			WHERE activation.resident_id = ? AND revision.revision_class = ?
			  AND activation_commit.commit_seq <= ? AND activation.recorded_at <= ?
			  AND revision.recorded_at <= ? AND content.erasure_state = 'present'
			ORDER BY activation_commit.commit_seq DESC LIMIT 1`,
			residentID.String(), revision.class, target.Head.CommitSeq.Int64(), target.AsOf.UnixMicro(), target.AsOf.UnixMicro(),
		).Scan(&revisionRaw, &contentRaw, &content)
		if err != nil {
			return dialogueSnapshot{}, fmt.Errorf("sqlite: load target active %s revision: %w", revision.class, err)
		}
		if content == nil {
			return dialogueSnapshot{}, fmt.Errorf("sqlite: target active %s revision content is unavailable", revision.class)
		}
		parsedRevision, err := canonical.ParseID(revisionRaw)
		if err != nil {
			return dialogueSnapshot{}, err
		}
		parsedContent, err := canonical.ParseID(contentRaw)
		if err != nil {
			return dialogueSnapshot{}, err
		}
		*revision.id = parsedRevision
		if revision.class == "memory_policy" {
			snapshot.memoryPolicy = append([]byte(nil), content...)
		}
		copyID := parsedRevision
		snapshot.definitions = append(snapshot.definitions, dialogueInputCandidate{
			role: "system", sourceType: "resident_revision", sourceID: &copyID,
			inclusionMode: "resident_definition", contentID: parsedContent,
			content: append([]byte(nil), content...), byteSize: int64(len(content)),
		})
	}
	return snapshot, nil
}

func requireDialogueAssemblyWatermarks(
	ctx context.Context,
	tx *sql.Tx,
	residentID canonical.ID,
	target domain.AssemblyTarget,
	snapshot dialogueSnapshot,
) error {
	serviceNames := make(map[projection.Name]struct{}, len(projection.ServiceRequiredNames()))
	for _, name := range projection.ServiceRequiredNames() {
		serviceNames[name] = struct{}{}
	}
	for _, definition := range activeProjectionDefinitions {
		if _, required := serviceNames[definition.Name]; !required {
			continue
		}
		var version, asOfTZ string
		var sourceCommit, asOf int64
		err := tx.QueryRowContext(ctx, `SELECT projection_version, source_commit_seq, as_of, as_of_tz
			FROM projection_watermarks WHERE projection_name = ? AND resident_id = ?`,
			definition.Name, residentID.String(),
		).Scan(&version, &sourceCommit, &asOf, &asOfTZ)
		if err != nil {
			return fmt.Errorf("%w: load %s watermark: %v", domain.ErrDialogueAssemblyTargetChanged, definition.Name, err)
		}
		parsedTZ, err := canonical.ParseTimezone(asOfTZ)
		if err != nil || version != string(definition.Version) || sourceCommit != target.Head.CommitSeq.Int64() {
			return fmt.Errorf("%w: %s watermark is not current", domain.ErrDialogueAssemblyTargetChanged, definition.Name)
		}
		if definition.TimeSensitive && (canonical.Instant(asOf) != target.AsOf || parsedTZ != target.AsOfTZ) {
			return fmt.Errorf("%w: %s watermark time differs", domain.ErrDialogueAssemblyTargetChanged, definition.Name)
		}
		actual, err := loadDialogueAssemblyDependencies(ctx, tx, definition.Name, residentID)
		if err != nil {
			return err
		}
		expected := make([]projection.Dependency, 0, len(definition.Dependencies))
		for _, kind := range definition.Dependencies {
			var versionID canonical.ID
			switch kind {
			case projection.MemoryPolicyDependency:
				versionID = snapshot.memoryID
			case projection.SessionizationPolicyDependency:
				versionID = snapshot.sessionID
			default:
				return fmt.Errorf("sqlite: unsupported dialogue Assembly dependency %q", kind)
			}
			expected = append(expected, projection.Dependency{Kind: kind, VersionID: versionID})
		}
		equal, err := projection.DependencySetEqual(actual, expected)
		if err != nil || !equal {
			return fmt.Errorf("%w: %s dependency metadata differs", domain.ErrDialogueAssemblyTargetChanged, definition.Name)
		}
	}
	return nil
}

func loadDialogueAssemblyDependencies(
	ctx context.Context,
	tx *sql.Tx,
	name projection.Name,
	residentID canonical.ID,
) ([]projection.Dependency, error) {
	rows, err := tx.QueryContext(ctx, `SELECT dependency_kind, dependency_version_id
		FROM projection_watermark_dependencies
		WHERE projection_name = ? AND resident_id = ?
		ORDER BY dependency_kind, dependency_version_id`, name, residentID.String())
	if err != nil {
		return nil, fmt.Errorf("sqlite: load %s dialogue Assembly dependencies: %w", name, err)
	}
	defer rows.Close()
	var result []projection.Dependency
	for rows.Next() {
		var kindRaw, idRaw string
		if err := rows.Scan(&kindRaw, &idRaw); err != nil {
			return nil, err
		}
		id, err := canonical.ParseID(idRaw)
		if err != nil {
			return nil, err
		}
		result = append(result, projection.Dependency{Kind: projection.DependencyKind(kindRaw), VersionID: id})
	}
	return result, rows.Err()
}

func loadDialogueAssemblySource(
	ctx context.Context,
	tx *sql.Tx,
	request domain.DialogueAssemblyRequest,
) (domain.Event, []byte, error) {
	var eventRaw, residentRaw, eventType, recordedTZ, contentRaw, erasure string
	var seq, recordedAt, commitSeq int64
	var content []byte
	err := tx.QueryRowContext(ctx, `SELECT event.event_id, event.resident_id, event.seq, event.event_type,
		event.recorded_at, event.recorded_tz, event.content_id, content.erasure_state, blob.content,
		commit_row.commit_seq
		FROM events event
		JOIN canonical_commits commit_row ON commit_row.canonical_commit_id = event.canonical_commit_id
		JOIN content_objects content ON content.content_id = event.content_id
		LEFT JOIN blobs blob ON blob.dedupe_scope_id = content.owner_resident_id
		 AND blob.hash_algorithm = content.blob_hash_algorithm AND blob.blob_hash = content.blob_hash
		WHERE event.event_id = ? AND event.resident_id = ? AND commit_row.commit_seq <= ?`,
		request.SourceEventID.String(), request.ResidentID.String(), request.Target.Head.CommitSeq.Int64(),
	).Scan(&eventRaw, &residentRaw, &seq, &eventType, &recordedAt, &recordedTZ, &contentRaw, &erasure, &content, &commitSeq)
	if err != nil {
		return domain.Event{}, nil, fmt.Errorf("sqlite: load dialogue Assembly source: %w", err)
	}
	if eventType != "user_message" || erasure != "present" || content == nil {
		return domain.Event{}, nil, errors.New("sqlite: dialogue Assembly source is unavailable")
	}
	eventID, err := canonical.ParseID(eventRaw)
	if err != nil {
		return domain.Event{}, nil, err
	}
	residentID, err := canonical.ParseID(residentRaw)
	if err != nil {
		return domain.Event{}, nil, err
	}
	contentID, err := canonical.ParseID(contentRaw)
	if err != nil {
		return domain.Event{}, nil, err
	}
	parsedSeq, err := canonical.NewSeq(seq)
	if err != nil {
		return domain.Event{}, nil, err
	}
	tz, err := canonical.ParseTimezone(recordedTZ)
	if err != nil {
		return domain.Event{}, nil, err
	}
	_ = commitSeq // queried to bind the source to the same snapshot and target.
	return domain.Event{
		ID: eventID, ResidentID: residentID, Seq: parsedSeq, Type: eventType,
		RecordedAt: canonical.Instant(recordedAt), RecordedTZ: tz,
		ContentID: contentID, Content: string(content),
	}, append([]byte(nil), content...), nil
}

func assembleDialogueContextV2(
	base []dialogueInputCandidate,
	policy memory.Policy,
	selection memory.RecallSelection,
	maximumBytes int64,
	maximumInputs int,
) ([]dialogueInputCandidate, memory.RecallPlan, map[canonical.ID]struct{}, bool, int, int, int, error) {
	original := append([]dialogueInputCandidate(nil), base...)
	working := append([]dialogueInputCandidate(nil), base...)
	budgetExceeded := false
	for pass := 0; pass <= len(original)+1; pass++ {
		plan, err := memory.PlanRecallV2(policy, selection, finalDialogueContextEventIDs(working))
		if err != nil {
			return nil, memory.RecallPlan{}, nil, false, 0, 0, 0, err
		}
		withRecall := insertDialogueRecallPrompt(working, plan)
		kept, exceeded, _, _, _, droppedRecall, err := applyDialogueBudget(withRecall, maximumBytes, maximumInputs)
		if err != nil {
			return nil, memory.RecallPlan{}, nil, false, 0, 0, 0, err
		}
		budgetExceeded = budgetExceeded || exceeded
		nonRecall := make([]dialogueInputCandidate, 0, len(kept))
		for _, candidate := range kept {
			if !candidate.recall {
				nonRecall = append(nonRecall, candidate)
			}
		}
		if len(nonRecall) == len(working) {
			backfill, live, initiative := dialogueContextDropCounts(original, nonRecall)
			return kept, plan, droppedRecall, budgetExceeded, backfill, live, initiative, nil
		}
		working = nonRecall
	}
	return nil, memory.RecallPlan{}, nil, false, 0, 0, 0,
		errors.New("sqlite: dialogue context-v2 budget/dedup did not converge")
}

func finalDialogueContextEventIDs(candidates []dialogueInputCandidate) []canonical.ID {
	seen := make(map[canonical.ID]struct{})
	var result []canonical.ID
	for _, candidate := range candidates {
		if candidate.sourceID == nil ||
			(candidate.inclusionMode != "live_context" && candidate.inclusionMode != "context_backfill") {
			continue
		}
		if _, duplicate := seen[*candidate.sourceID]; duplicate {
			continue
		}
		seen[*candidate.sourceID] = struct{}{}
		result = append(result, *candidate.sourceID)
	}
	return result
}

func insertDialogueRecallPrompt(base []dialogueInputCandidate, plan memory.RecallPlan) []dialogueInputCandidate {
	result := make([]dialogueInputCandidate, 0, len(base)+len(plan.Prompt))
	inserted := false
	for _, candidate := range base {
		if !inserted && candidate.inclusionMode == "current_input" {
			for _, decision := range plan.Prompt {
				claimID := decision.Candidate.Candidate.ClaimID
				copyID := claimID
				content := []byte(decision.RenderedText)
				result = append(result, dialogueInputCandidate{
					role: "system", sourceType: "claim", sourceID: &copyID,
					inclusionMode: "memory_recall", content: content, byteSize: int64(len(content)),
					recall: true, recallClaimID: &copyID,
				})
			}
			inserted = true
		}
		result = append(result, candidate)
	}
	return result
}

func dialogueContextDropCounts(original, kept []dialogueInputCandidate) (int, int, int) {
	count := func(values []dialogueInputCandidate, selector func(dialogueInputCandidate) bool) int {
		result := 0
		for _, value := range values {
			if selector(value) {
				result++
			}
		}
		return result
	}
	backfillSelector := func(value dialogueInputCandidate) bool { return value.backfill }
	liveSelector := func(value dialogueInputCandidate) bool { return value.live }
	initiativeSelector := func(value dialogueInputCandidate) bool { return value.initiative }
	return count(original, backfillSelector) - count(kept, backfillSelector),
		count(original, liveSelector) - count(kept, liveSelector),
		count(original, initiativeSelector) - count(kept, initiativeSelector)
}

func dialogueRecallCandidateSnapshots(selection memory.RecallSelection) []domain.RecallCandidateSnapshot {
	result := make([]domain.RecallCandidateSnapshot, 0, len(selection.Candidates))
	for _, scored := range selection.Candidates {
		candidate := scored.Candidate
		var lastConfirmed *canonical.Instant
		if candidate.LastConfirmed != nil {
			value := *candidate.LastConfirmed
			lastConfirmed = &value
		}
		result = append(result, domain.RecallCandidateSnapshot{
			ClaimID: candidate.ClaimID, Statement: candidate.Statement,
			ContextCompatibility: candidate.ContextCompatibility, Salience: candidate.Salience,
			Confidence: candidate.Confidence, Currentness: candidate.Currentness, LastConfirmed: lastConfirmed,
			Status: string(candidate.Status), Stage: string(candidate.Stage),
			TemporalRelation: string(candidate.TemporalRelation),
			SourceEventIDs:   append([]canonical.ID(nil), candidate.SourceEventIDs...), Abstract: candidate.Abstract,
		})
	}
	return result
}

func frozenDialogueInputContent(
	residentID, contentID canonical.ID,
	salt canonical.ContentSalt,
	value []byte,
) (domain.Content, error) {
	commitment, err := canonical.CommitContent("generation_input", salt, value)
	if err != nil {
		return domain.Content{}, err
	}
	return domain.Content{
		ID: contentID, ResidentID: residentID, Class: "generation_input",
		Bytes: append([]byte(nil), value...), BlobHash: canonical.HashBlob(value),
		Commitment: commitment, CommitmentSalt: salt, ErasurePolicy: "independent",
	}, nil
}
