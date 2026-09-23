package sqlite

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"slices"

	"mahoroba.local/mahoroba/internal/canonical"
	"mahoroba.local/mahoroba/internal/domain"
	"mahoroba.local/mahoroba/internal/memory"
	"mahoroba.local/mahoroba/internal/projection"
)

var errInvalidPreparedDialogueHistory = errors.New("sqlite: invalid prepared dialogue history")

// PrepareDialogue is the Commit-B writer for the two-commit dialogue path.
// The supplied plan is an untrusted snapshot: every Canonical identity and
// every derivable Recall decision is rechecked inside this transaction before
// any durable row is inserted.
func (u *canonicalUoW) PrepareDialogue(
	ctx context.Context,
	value domain.PrepareDialogue,
) (domain.PrepareDialogueResult, error) {
	if u == nil || u.tx == nil {
		return domain.PrepareDialogueResult{}, errors.New("sqlite: PrepareDialogue requires an open Canonical UoW")
	}
	if err := domain.PrepareDialogueCommand(value).Validate(); err != nil {
		return domain.PrepareDialogueResult{}, err
	}
	generation := value.Generation
	if err := u.requireResidentScope(generation.ResidentID); err != nil {
		return domain.PrepareDialogueResult{}, err
	}

	existingID, exists, err := u.preparedDialogueRunForObligation(ctx, generation.ResidentID, generation.IdempotencyKey)
	if err != nil {
		return domain.PrepareDialogueResult{}, err
	}
	if exists {
		executionClass, err := classifyNormalDialogueExecutionContract(ctx, u.tx, existingID)
		if err != nil {
			return domain.PrepareDialogueResult{}, fmt.Errorf(
				"%w: existing run is not an exact normal dialogue: %v",
				errInvalidPreparedDialogueHistory,
				err,
			)
		}
		var resolution domain.PrepareDialogueResolution
		switch executionClass {
		case domain.DialogueExecutionCurrentNormal:
			resolution = domain.PrepareDialogueExistingCurrentV4
		case domain.DialogueExecutionLegacyNormal, domain.DialogueExecutionPriorSplitNormal, domain.DialogueExecutionPriorBoundedNormal:
			resolution = domain.PrepareDialogueDispatchExistingFrozenRun
		default:
			return domain.PrepareDialogueResult{}, fmt.Errorf(
				"%w: existing run has unsupported execution class %q",
				errInvalidPreparedDialogueHistory,
				executionClass,
			)
		}
		if err := u.validateExistingPreparedDialogue(
			ctx,
			existingID,
			value.SourceEventID,
			executionClass,
		); err != nil {
			return domain.PrepareDialogueResult{}, err
		}
		return domain.PrepareDialogueResult{
			RunID: existingID, Resolution: resolution,
		}, canonical.ErrNoMutation
	}

	policy, err := u.validateNewPreparedDialogue(ctx, value)
	if err != nil {
		return domain.PrepareDialogueResult{}, err
	}
	if value.RecallDisposition == domain.RecallDispositionSuccess {
		if err := u.validateSuccessfulDialogueRecall(ctx, value, policy); err != nil {
			return domain.PrepareDialogueResult{}, err
		}
		if err := u.insertDialogueRecallForContext(
			ctx,
			*value.Recall,
			domain.DialogueContextPolicyVersionV4,
		); err != nil {
			return domain.PrepareDialogueResult{}, err
		}
	}
	if err := u.insertCurrentDialogueGenerationRun(ctx, generation); err != nil {
		return domain.PrepareDialogueResult{}, err
	}
	if value.RecallDisposition == domain.RecallDispositionSuccess {
		if err := u.insertDialogueRecallUsages(ctx, *value.Recall, generation.RunID); err != nil {
			return domain.PrepareDialogueResult{}, err
		}
	}
	for index, input := range generation.Inputs {
		if err := u.insertContent(ctx, input.Content); err != nil {
			return domain.PrepareDialogueResult{}, fmt.Errorf("sqlite: insert prepared dialogue input content %d: %w", index, err)
		}
		var sourceID any
		if input.SourceID != nil {
			sourceID = input.SourceID.String()
		}
		if _, err := u.tx.ExecContext(ctx, `INSERT INTO generation_run_inputs(
			generation_run_input_id, canonical_commit_id, generation_run_id, ordinal, role,
			source_type, source_id, inclusion_mode, content_id, recorded_at, recorded_tz
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`, input.ID.String(), u.metadata.CommitID.String(),
			generation.RunID.String(), input.Ordinal, input.Role, input.SourceType, sourceID,
			input.InclusionMode, input.Content.ID.String(), u.metadata.CommittedAt.UnixMicro(),
			u.metadata.CommittedTZ.String()); err != nil {
			return domain.PrepareDialogueResult{}, fmt.Errorf("sqlite: insert prepared dialogue input %d: %w", index, err)
		}
	}
	if err := u.insertOutcome(ctx, generation.RunningOutcomeID, generation.RunID, 1, "running", nil, nil, nil, 0, ""); err != nil {
		return domain.PrepareDialogueResult{}, err
	}
	return domain.PrepareDialogueResult{
		RunID: generation.RunID, Resolution: domain.PrepareDialoguePreparedCurrentV4,
	}, nil
}

func (u *canonicalUoW) preparedDialogueRunForObligation(
	ctx context.Context,
	residentID canonical.ID,
	idempotencyKey string,
) (canonical.ID, bool, error) {
	var raw string
	err := u.tx.QueryRowContext(ctx, `SELECT generation_run_id FROM generation_runs
		WHERE resident_id = ? AND idempotency_key = ?`, residentID.String(), idempotencyKey).Scan(&raw)
	if errors.Is(err, sql.ErrNoRows) {
		return canonical.ID{}, false, nil
	}
	if err != nil {
		return canonical.ID{}, false, fmt.Errorf("sqlite: resolve prepared dialogue obligation: %w", err)
	}
	id, err := canonical.ParseID(raw)
	if err != nil {
		return canonical.ID{}, false, fmt.Errorf("%w: invalid existing run ID: %v", errInvalidPreparedDialogueHistory, err)
	}
	return id, true, nil
}

func (u *canonicalUoW) insertCurrentDialogueGenerationRun(
	ctx context.Context,
	value domain.PrepareGeneration,
) error {
	params, _, err := domain.ParseGeneratorParams(value.GeneratorParams.Bytes())
	if err != nil {
		return err
	}
	if err := domain.ValidateNewGeneratorParamsForPurpose(domain.GenerationPurposeDialogue, params); err != nil {
		return err
	}
	if err := domain.ValidateNewDialogueNormalExecutionContract(domain.DialogueExecutionContract{
		PipelineVersionKey:     domain.DialoguePipelineVersionV4,
		PromptTemplateVersion:  value.PromptTemplateVersion,
		ContextPolicyVersion:   value.ContextPolicyVersion,
		MemoryRenderingVersion: value.MemoryRenderingVersion,
	}); err != nil {
		return err
	}
	if value.Purpose.Effective() != domain.GenerationPurposeDialogue || value.SessionPolicyID == nil {
		return errors.New("sqlite: current prepared dialogue has an invalid purpose or Sessionization Policy")
	}
	var recallRunID any
	if value.RecallRunID != nil {
		recallRunID = value.RecallRunID.String()
	}
	m := u.metadata
	if _, err := u.tx.ExecContext(ctx, `INSERT INTO generation_runs(
		generation_run_id, canonical_commit_id, resident_id, purpose, idempotency_key,
		provider, model, model_version, prompt_template_version, pipeline_version_id,
		context_policy_version, sessionization_policy_version_id, memory_rendering_version,
		principles_revision_id, persona_revision_id, memory_policy_revision_id, recall_run_id,
		temperature, top_p, max_tokens, seed, generator_params, as_of, as_of_tz,
		budget_exceeded, dropped_input_summary, requested_at, requested_tz
	) VALUES (?, ?, ?, 'dialogue', ?, ?, ?, NULL, ?, ?, ?, ?, ?, ?, ?, ?, ?,
		NULL, NULL, NULL, NULL, ?, ?, ?, ?, ?, ?, ?)`,
		value.RunID.String(), m.CommitID.String(), value.ResidentID.String(), value.IdempotencyKey,
		value.Provider, value.Model, value.PromptTemplateVersion, value.PipelineVersionID.String(),
		value.ContextPolicyVersion, value.SessionPolicyID.String(), value.MemoryRenderingVersion,
		value.PrinciplesRevisionID.String(), value.PersonaRevisionID.String(),
		value.MemoryPolicyRevisionID.String(), recallRunID, value.GeneratorParams.String(),
		value.AsOf.UnixMicro(), value.AsOfTZ.String(), boolInt(value.BudgetExceeded),
		value.DroppedInputSummary.String(), m.CommittedAt.UnixMicro(), m.CommittedTZ.String(),
	); err != nil {
		return fmt.Errorf("sqlite: insert current prepared dialogue run: %w", err)
	}
	return nil
}

func (u *canonicalUoW) validateExistingPreparedDialogue(
	ctx context.Context,
	runID, requestedSourceID canonical.ID,
	expectedClass domain.DialogueExecutionClass,
) error {
	var runCommitRaw, residentRaw, purposeRaw, obligation string
	var promptVersion, pipelineRaw, pipelineKind, pipelineVersion, pipelineDefinition string
	var contextVersion, sessionRaw, sessionVersion, sessionDefinition, renderingVersion string
	var principlesRaw, personaRaw, memoryRaw, paramsRaw, asOfTZ, droppedRaw string
	var recallRaw sql.NullString
	var runCommitSeq, asOf int64
	err := u.tx.QueryRowContext(ctx, `SELECT run.canonical_commit_id, run_commit.commit_seq,
		run.resident_id, run.purpose, run.idempotency_key, run.prompt_template_version,
		run.pipeline_version_id, pipeline.pipeline_kind, pipeline.version_key, pipeline.definition,
		run.context_policy_version, run.sessionization_policy_version_id,
		session.version_key, session.definition, run.memory_rendering_version,
		run.principles_revision_id, run.persona_revision_id, run.memory_policy_revision_id,
		run.recall_run_id, run.generator_params, run.as_of, run.as_of_tz,
		run.dropped_input_summary
		FROM generation_runs run
		JOIN canonical_commits run_commit ON run_commit.canonical_commit_id = run.canonical_commit_id
		JOIN pipeline_versions pipeline ON pipeline.pipeline_version_id = run.pipeline_version_id
		JOIN sessionization_policy_versions session
		  ON session.sessionization_policy_version_id = run.sessionization_policy_version_id
		WHERE run.generation_run_id = ?`, runID.String()).Scan(
		&runCommitRaw, &runCommitSeq, &residentRaw, &purposeRaw, &obligation, &promptVersion,
		&pipelineRaw, &pipelineKind, &pipelineVersion, &pipelineDefinition, &contextVersion,
		&sessionRaw, &sessionVersion, &sessionDefinition, &renderingVersion,
		&principlesRaw, &personaRaw, &memoryRaw, &recallRaw, &paramsRaw, &asOf, &asOfTZ,
		&droppedRaw,
	)
	if err != nil {
		return fmt.Errorf("%w: load existing envelope: %v", errInvalidPreparedDialogueHistory, err)
	}
	parseID := func(label, raw string) (canonical.ID, error) {
		id, err := canonical.ParseID(raw)
		if err != nil {
			return canonical.ID{}, fmt.Errorf("%w: invalid existing %s: %v", errInvalidPreparedDialogueHistory, label, err)
		}
		return id, nil
	}
	runCommitID, err := parseID("commit", runCommitRaw)
	if err != nil {
		return err
	}
	residentID, err := parseID("resident", residentRaw)
	if err != nil {
		return err
	}
	pipelineID, err := parseID("pipeline", pipelineRaw)
	if err != nil {
		return err
	}
	sessionID, err := parseID("Sessionization Policy", sessionRaw)
	if err != nil {
		return err
	}
	principlesID, err := parseID("principles revision", principlesRaw)
	if err != nil {
		return err
	}
	personaID, err := parseID("persona revision", personaRaw)
	if err != nil {
		return err
	}
	memoryID, err := parseID("memory policy revision", memoryRaw)
	if err != nil {
		return err
	}
	contractClass, contractErr := domain.ClassifyPersistedDialogueExecutionContract(
		domain.DialogueExecutionContract{
			PipelineVersionKey: pipelineVersion, PromptTemplateVersion: promptVersion,
			ContextPolicyVersion: contextVersion, MemoryRenderingVersion: renderingVersion,
		}, domain.DialogueEnvelopeNormal,
	)
	if domain.GenerationPurpose(purposeRaw).Effective() != domain.GenerationPurposeDialogue ||
		obligation != domain.DialogueObligation(requestedSourceID) || contractErr != nil ||
		contractClass != expectedClass ||
		pipelineKind != "dialogue" || sessionVersion != domain.SessionPolicyVersion ||
		!validReadinessSessionDefinition(sessionDefinition) {
		return fmt.Errorf("%w: existing run is not a supported normal dialogue tuple", errInvalidPreparedDialogueHistory)
	}
	pipelineJSON, err := canonical.ParseCanonicalJSON([]byte(pipelineDefinition))
	if err != nil || domain.ValidateExactDialoguePipelineDefinition(domain.PipelineVersionDefinition{
		ID: pipelineID, Kind: pipelineKind, VersionKey: pipelineVersion, Definition: pipelineJSON,
	}) != nil {
		return fmt.Errorf("%w: existing dialogue pipeline definition is not exact", errInvalidPreparedDialogueHistory)
	}
	params, _, err := domain.ParseGeneratorParams([]byte(paramsRaw))
	if err != nil || domain.ValidateGeneratorParamsForPurpose(domain.GenerationPurposeDialogue, params) != nil {
		return fmt.Errorf("%w: existing dialogue generator parameters are invalid", errInvalidPreparedDialogueHistory)
	}
	if _, err := canonical.ParseTimezone(asOfTZ); err != nil || asOf <= 0 || runCommitSeq < 2 {
		return fmt.Errorf("%w: existing dialogue time or commit is invalid", errInvalidPreparedDialogueHistory)
	}
	for _, revision := range []struct {
		id    canonical.ID
		class string
	}{{principlesID, "principles"}, {personaID, "persona"}, {memoryID, "memory_policy"}} {
		if err := u.requireResidentRevision(ctx, residentID, revision.id, revision.class); err != nil {
			return fmt.Errorf("%w: %v", errInvalidPreparedDialogueHistory, err)
		}
	}
	policy, err := u.loadPreparedDialogueMemoryPolicy(ctx, residentID, memoryID)
	if err != nil || (contractClass == domain.DialogueExecutionPriorSplitNormal &&
		policy.MemoryRecallEnabled && policy.RenderingVersion != memory.RenderingVersionV1) ||
		((contractClass == domain.DialogueExecutionCurrentNormal || contractClass == domain.DialogueExecutionPriorBoundedNormal) &&
			policy.MemoryRecallEnabled && policy.RenderingVersion != memory.RenderingVersionV2) {
		return fmt.Errorf("%w: existing memory policy is invalid: %v", errInvalidPreparedDialogueHistory, err)
	}
	fallback, err := parsePreparedDialogueDroppedSummary(droppedRaw)
	if err != nil {
		return err
	}

	var sourceResident, sourceType, sourceContentRaw string
	var sourceSeq, sourceCommitSeq int64
	if err := u.tx.QueryRowContext(ctx, `SELECT event.resident_id, event.seq, event.event_type,
		event.content_id, commit_row.commit_seq
		FROM events event JOIN canonical_commits commit_row
		  ON commit_row.canonical_commit_id = event.canonical_commit_id
		WHERE event.event_id = ?`, requestedSourceID.String()).Scan(
		&sourceResident, &sourceSeq, &sourceType, &sourceContentRaw, &sourceCommitSeq,
	); err != nil || sourceResident != residentID.String() || sourceType != "user_message" ||
		!validPreparedDialogueSourceCommitOrder(contractClass, sourceCommitSeq, runCommitSeq) {
		return fmt.Errorf("%w: existing source is not a preceding resident user_message", errInvalidPreparedDialogueHistory)
	}
	if _, err := parseID("source content", sourceContentRaw); err != nil {
		return err
	}
	if err := u.validateExistingPreparedDialogueInputs(
		ctx, runID, runCommitID, residentID, requestedSourceID, sourceSeq,
		principlesID, personaID, memoryID, recallRaw.Valid,
	); err != nil {
		return err
	}
	if _, err := (generationOutcomeRepository{}).Summary(ctx, u.tx, runID, residentID); err != nil {
		return fmt.Errorf("%w: %v", errInvalidPreparedDialogueHistory, err)
	}
	var initialOutcomes, earlyOutcomes int64
	if err := u.tx.QueryRowContext(ctx, `SELECT
		SUM(CASE WHEN outcome.attempt_no = 1 AND outcome.state = 'running'
		 AND outcome.canonical_commit_id = ? THEN 1 ELSE 0 END),
		SUM(CASE WHEN commit_row.commit_seq < ? THEN 1 ELSE 0 END)
		FROM generation_run_outcomes outcome
		JOIN canonical_commits commit_row ON commit_row.canonical_commit_id = outcome.canonical_commit_id
		WHERE outcome.generation_run_id = ?`, runCommitID.String(), runCommitSeq, runID.String()).Scan(
		&initialOutcomes, &earlyOutcomes,
	); err != nil || initialOutcomes != 1 || earlyOutcomes != 0 {
		return fmt.Errorf("%w: existing outcome history is not rooted in Commit B", errInvalidPreparedDialogueHistory)
	}
	return u.validateExistingPreparedDialogueRecall(ctx, preparedDialogueExistingRecall{
		runID: runID, runCommitID: runCommitID, runCommitSeq: runCommitSeq,
		residentID: residentID, sourceEventID: requestedSourceID, sourceContentRaw: sourceContentRaw,
		memoryPolicyID: memoryID, asOf: canonical.Instant(asOf), asOfTZ: canonical.Timezone(asOfTZ),
		recallRaw: recallRaw, fallback: fallback, policyEnabled: policy.MemoryRecallEnabled,
		sessionID: sessionID, executionClass: contractClass,
	})
}

func validPreparedDialogueSourceCommitOrder(
	executionClass domain.DialogueExecutionClass,
	sourceCommitSeq, runCommitSeq int64,
) bool {
	switch executionClass {
	case domain.DialogueExecutionLegacyNormal:
		// dialogue-v1 was the historical atomic ingress+prepare path.
		return sourceCommitSeq == runCommitSeq
	case domain.DialogueExecutionPriorSplitNormal, domain.DialogueExecutionPriorBoundedNormal, domain.DialogueExecutionCurrentNormal:
		// dialogue-v2 and later use Commit A -> Commit B.
		return sourceCommitSeq < runCommitSeq
	default:
		return false
	}
}

func parsePreparedDialogueDroppedSummary(raw string) (domain.MemoryRecallFallbackReason, error) {
	var value struct {
		Backfill     canonical.Count                   `json:"backfill"`
		Live         canonical.Count                   `json:"live_context"`
		MemoryRecall domain.MemoryRecallFallbackReason `json:"memory_recall,omitempty"`
		Initiative   canonical.Count                   `json:"initiative_context,omitempty"`
	}
	parsed, err := canonical.ParseCanonicalJSON([]byte(raw))
	if err != nil || json.Unmarshal(parsed.Bytes(), &value) != nil ||
		value.Backfill < 0 || value.Live < 0 || value.Initiative < 0 {
		return "", fmt.Errorf("%w: invalid dropped-input summary", errInvalidPreparedDialogueHistory)
	}
	if value.MemoryRecall != "" {
		if err := value.MemoryRecall.Validate(); err != nil {
			return "", fmt.Errorf("%w: invalid Recall fallback: %v", errInvalidPreparedDialogueHistory, err)
		}
	}
	exact, err := dialogueDroppedInputSummary(int(value.Backfill), int(value.Live), int(value.Initiative), value.MemoryRecall)
	if err != nil || !bytes.Equal(parsed.Bytes(), exact.Bytes()) {
		return "", fmt.Errorf("%w: non-exact dropped-input summary", errInvalidPreparedDialogueHistory)
	}
	return value.MemoryRecall, nil
}

func (u *canonicalUoW) validateExistingPreparedDialogueInputs(
	ctx context.Context,
	runID, runCommitID, residentID, sourceEventID canonical.ID,
	sourceSeq int64,
	principlesID, personaID, memoryID canonical.ID,
	hasRecall bool,
) error {
	rows, err := u.tx.QueryContext(ctx, `SELECT input.ordinal, input.role, input.source_type,
		input.source_id, input.inclusion_mode, input.canonical_commit_id,
		object.owner_resident_id, object.content_class
		FROM generation_run_inputs input
		JOIN content_objects object ON object.content_id = input.content_id
		WHERE input.generation_run_id = ? ORDER BY input.ordinal`, runID.String())
	if err != nil {
		return err
	}
	defer rows.Close()
	pinned := map[canonical.ID]string{principlesID: "principles", personaID: "persona", memoryID: "memory_policy"}
	seenPinned := make(map[canonical.ID]bool, 3)
	count, currentCount, runtimeCount, currentOrdinal := int64(0), 0, 0, int64(-1)
	for rows.Next() {
		var ordinal int64
		var role, sourceType, inclusion, commitRaw, ownerRaw, contentClass string
		var sourceRaw sql.NullString
		if err := rows.Scan(&ordinal, &role, &sourceType, &sourceRaw, &inclusion, &commitRaw, &ownerRaw, &contentClass); err != nil {
			return err
		}
		if ordinal != count || commitRaw != runCommitID.String() || ownerRaw != residentID.String() ||
			contentClass != "generation_input" {
			return fmt.Errorf("%w: existing input envelope differs from Commit B", errInvalidPreparedDialogueHistory)
		}
		count++
		var sourceID *canonical.ID
		if sourceRaw.Valid {
			parsed, err := canonical.ParseID(sourceRaw.String)
			if err != nil {
				return fmt.Errorf("%w: invalid existing input source", errInvalidPreparedDialogueHistory)
			}
			sourceID = &parsed
		}
		switch sourceType {
		case "resident_revision":
			class, ok := pinnedValue(pinned, sourceID)
			if !ok || role != "system" || inclusion != "resident_definition" || seenPinned[*sourceID] {
				return fmt.Errorf("%w: invalid existing resident definition input", errInvalidPreparedDialogueHistory)
			}
			var actualClass, actualResident string
			if err := u.tx.QueryRowContext(ctx, `SELECT revision_class, resident_id FROM resident_revisions WHERE revision_id = ?`,
				sourceID.String()).Scan(&actualClass, &actualResident); err != nil || actualClass != class || actualResident != residentID.String() {
				return fmt.Errorf("%w: invalid existing revision source", errInvalidPreparedDialogueHistory)
			}
			seenPinned[*sourceID] = true
		case "runtime_projection":
			if sourceID != nil || role != "system" || inclusion != "runtime_projection" {
				return fmt.Errorf("%w: invalid existing runtime Projection input", errInvalidPreparedDialogueHistory)
			}
			runtimeCount++
		case "event":
			if sourceID == nil {
				return fmt.Errorf("%w: existing event input has no source", errInvalidPreparedDialogueHistory)
			}
			var eventResident string
			var eventSeq int64
			if err := u.tx.QueryRowContext(ctx, `SELECT resident_id, seq FROM events WHERE event_id = ?`, sourceID.String()).Scan(
				&eventResident, &eventSeq,
			); err != nil || eventResident != residentID.String() {
				return fmt.Errorf("%w: invalid existing event source", errInvalidPreparedDialogueHistory)
			}
			if *sourceID == sourceEventID {
				if role != "user" || inclusion != "current_input" {
					return fmt.Errorf("%w: invalid existing current input", errInvalidPreparedDialogueHistory)
				}
				currentCount++
				currentOrdinal = ordinal
			} else if eventSeq >= sourceSeq || inclusion == "current_input" {
				return fmt.Errorf("%w: existing context event is not before its source", errInvalidPreparedDialogueHistory)
			}
		case "claim":
			if !hasRecall || sourceID == nil || role != "system" || inclusion != "memory_recall" {
				return fmt.Errorf("%w: invalid existing Recall input", errInvalidPreparedDialogueHistory)
			}
		case "content":
			if sourceID == nil {
				return fmt.Errorf("%w: invalid existing content input", errInvalidPreparedDialogueHistory)
			}
			var sourceOwner string
			if err := u.tx.QueryRowContext(ctx, `SELECT owner_resident_id FROM content_objects WHERE content_id = ?`,
				sourceID.String()).Scan(&sourceOwner); err != nil || sourceOwner != residentID.String() {
				return fmt.Errorf("%w: invalid existing content source", errInvalidPreparedDialogueHistory)
			}
		default:
			return fmt.Errorf("%w: unsupported existing input source type", errInvalidPreparedDialogueHistory)
		}
	}
	if err := rows.Err(); err != nil {
		return err
	}
	if count < 1 || count > domain.MaxDialogueInputs || currentCount != 1 || runtimeCount != 1 ||
		len(seenPinned) != len(pinned) || currentOrdinal != count-1 {
		return fmt.Errorf("%w: incomplete existing dialogue input graph", errInvalidPreparedDialogueHistory)
	}
	return nil
}

func pinnedValue(values map[canonical.ID]string, id *canonical.ID) (string, bool) {
	if id == nil {
		return "", false
	}
	value, ok := values[*id]
	return value, ok
}

type preparedDialogueExistingRecall struct {
	runID, runCommitID, residentID, sourceEventID, memoryPolicyID, sessionID canonical.ID
	runCommitSeq                                                             int64
	sourceContentRaw                                                         string
	asOf                                                                     canonical.Instant
	asOfTZ                                                                   canonical.Timezone
	recallRaw                                                                sql.NullString
	fallback                                                                 domain.MemoryRecallFallbackReason
	policyEnabled                                                            bool
	executionClass                                                           domain.DialogueExecutionClass
}

func (u *canonicalUoW) validateExistingPreparedDialogueRecall(
	ctx context.Context,
	value preparedDialogueExistingRecall,
) error {
	if !value.recallRaw.Valid {
		if value.policyEnabled != (value.fallback != "") {
			return fmt.Errorf("%w: existing Recall branch conflicts with pinned policy", errInvalidPreparedDialogueHistory)
		}
		var usages int64
		if err := u.tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM claim_usages WHERE generation_run_id = ?`,
			value.runID.String()).Scan(&usages); err != nil || usages != 0 {
			return fmt.Errorf("%w: no-Recall branch contains claim usages", errInvalidPreparedDialogueHistory)
		}
		return nil
	}
	if !value.policyEnabled || value.fallback != "" {
		return fmt.Errorf("%w: successful Recall branch conflicts with pinned policy", errInvalidPreparedDialogueHistory)
	}
	recallID, err := canonical.ParseID(value.recallRaw.String)
	if err != nil {
		return fmt.Errorf("%w: invalid Recall run ID", errInvalidPreparedDialogueHistory)
	}
	var commitRaw, residentRaw, queryContentRaw, pipelineRaw, policyRaw, timezone, queryRaw string
	var asOf int64
	if err := u.tx.QueryRowContext(ctx, `SELECT canonical_commit_id, resident_id, query_content_id,
		pipeline_version_id, memory_policy_revision_id, as_of, as_of_tz, query_conditions
		FROM recall_runs WHERE recall_run_id = ?`, recallID.String()).Scan(
		&commitRaw, &residentRaw, &queryContentRaw, &pipelineRaw, &policyRaw, &asOf, &timezone, &queryRaw,
	); err != nil || commitRaw != value.runCommitID.String() || residentRaw != value.residentID.String() ||
		queryContentRaw != value.sourceContentRaw || policyRaw != value.memoryPolicyID.String() ||
		canonical.Instant(asOf) != value.asOf || timezone != value.asOfTZ.String() {
		return fmt.Errorf("%w: existing Recall envelope differs from Commit B", errInvalidPreparedDialogueHistory)
	}
	var target struct {
		ProjectionHead canonical.CommitSeq `json:"projection_head"`
	}
	if err := json.Unmarshal([]byte(queryRaw), &target); err != nil || target.ProjectionHead.Int64() >= value.runCommitSeq {
		return fmt.Errorf("%w: existing Recall target does not precede Commit B", errInvalidPreparedDialogueHistory)
	}
	expectedPipeline, fallback, err := u.loadRecallPipeline(ctx, target.ProjectionHead)
	if err != nil || fallback != "" || pipelineRaw != expectedPipeline.String() {
		return fmt.Errorf("%w: existing Recall pipeline is not exact", errInvalidPreparedDialogueHistory)
	}
	provenanceReason := memory.ExclusionProvenanceDuplicateV2
	if value.executionClass == domain.DialogueExecutionLegacyNormal {
		provenanceReason = memory.ExclusionProvenanceDuplicateV1
	}
	var mismatched, invalidUsages int64
	if err := u.tx.QueryRowContext(ctx, `SELECT
		COALESCE(SUM(CASE WHEN canonical_commit_id <> ? OR recall_run_id <> ? THEN 1 ELSE 0 END), 0),
		COALESCE(SUM(CASE
		 WHEN usage_type = 'candidate' AND exclusion_reason IS NOT NULL THEN 1
		 WHEN usage_type = 'selected' AND exclusion_reason IS NOT NULL
		      AND exclusion_reason NOT IN (?, 'token_budget', 'policy_filter') THEN 1
		 WHEN usage_type = 'prompt_included' AND exclusion_reason IS NOT NULL THEN 1
		 WHEN usage_type NOT IN ('candidate','selected','prompt_included') THEN 1
		 ELSE 0 END), 0)
		FROM claim_usages WHERE generation_run_id = ?`, value.runCommitID.String(), recallID.String(),
		string(provenanceReason), value.runID.String()).Scan(&mismatched, &invalidUsages); err != nil {
		return err
	}
	if mismatched != 0 || invalidUsages != 0 {
		return fmt.Errorf("%w: existing Recall usage envelope is malformed", errInvalidPreparedDialogueHistory)
	}
	var foreign int64
	if err := u.tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM claim_usages
		WHERE recall_run_id = ? AND (generation_run_id IS NULL OR generation_run_id <> ?)`,
		recallID.String(), value.runID.String()).Scan(&foreign); err != nil || foreign != 0 {
		return fmt.Errorf("%w: existing Recall usage is not linked to its dialogue", errInvalidPreparedDialogueHistory)
	}
	for _, usageType := range []string{"candidate", "selected", "prompt_included"} {
		var count, minimum, maximum int64
		if err := u.tx.QueryRowContext(ctx, `SELECT COUNT(*), COALESCE(MIN(ordinal), 0),
			COALESCE(MAX(ordinal), -1) FROM claim_usages
			WHERE generation_run_id = ? AND usage_type = ?`, value.runID.String(), usageType).Scan(
			&count, &minimum, &maximum,
		); err != nil || (count != 0 && (minimum != 0 || maximum != count-1)) {
			return fmt.Errorf("%w: existing Recall %s ordinals are not contiguous", errInvalidPreparedDialogueHistory, usageType)
		}
	}
	var graphInvalid int64
	if err := u.tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM claim_usages selected
		WHERE selected.generation_run_id = ? AND selected.usage_type = 'selected'
		  AND (NOT EXISTS(SELECT 1 FROM claim_usages candidate
		       WHERE candidate.generation_run_id = selected.generation_run_id
		         AND candidate.usage_type = 'candidate' AND candidate.claim_id = selected.claim_id)
		   OR (selected.exclusion_reason IS NULL AND NOT EXISTS(
		       SELECT 1 FROM claim_usages prompt WHERE prompt.generation_run_id = selected.generation_run_id
		         AND prompt.usage_type = 'prompt_included' AND prompt.claim_id = selected.claim_id))
		   OR (selected.exclusion_reason IS NOT NULL AND EXISTS(
		       SELECT 1 FROM claim_usages prompt WHERE prompt.generation_run_id = selected.generation_run_id
		         AND prompt.usage_type = 'prompt_included' AND prompt.claim_id = selected.claim_id)))`,
		value.runID.String()).Scan(&graphInvalid); err != nil || graphInvalid != 0 {
		return fmt.Errorf("%w: existing Recall usage graph is inconsistent", errInvalidPreparedDialogueHistory)
	}
	return nil
}

func (u *canonicalUoW) validateNewPreparedDialogue(
	ctx context.Context,
	value domain.PrepareDialogue,
) (memory.Policy, error) {
	generation := value.Generation
	if err := u.requireActiveResident(ctx, generation.ResidentID); err != nil {
		return memory.Policy{}, err
	}
	if err := u.requireOperationallySelectedResident(ctx, generation.ResidentID); err != nil {
		return memory.Policy{}, err
	}
	if err := u.validateDialogueAssemblyTarget(ctx, value.Target); err != nil {
		return memory.Policy{}, err
	}
	if err := u.validateCurrentDialoguePipeline(ctx, generation.PipelineVersionID, value.Target.Head.CommitSeq); err != nil {
		return memory.Policy{}, err
	}
	for _, revision := range []struct {
		id    canonical.ID
		class string
	}{
		{generation.PrinciplesRevisionID, "principles"},
		{generation.PersonaRevisionID, "persona"},
		{generation.MemoryPolicyRevisionID, "memory_policy"},
	} {
		if err := u.requireActiveRevision(ctx, generation.ResidentID, revision.id, revision.class); err != nil {
			return memory.Policy{}, err
		}
		if err := u.requireRevisionVisibleAtTarget(ctx, generation.ResidentID, revision.id, revision.class, value.Target.Head.CommitSeq); err != nil {
			return memory.Policy{}, err
		}
	}
	if generation.SessionPolicyID == nil {
		return memory.Policy{}, errors.New("sqlite: prepared dialogue has no desired sessionization policy")
	}
	desiredSession, _, err := resolveActivitySessionPolicy(ctx, u.tx, generation.ResidentID)
	if err != nil {
		return memory.Policy{}, err
	}
	if desiredSession != *generation.SessionPolicyID {
		return memory.Policy{}, errors.New("sqlite: prepared dialogue sessionization policy changed after Assembly")
	}
	if err := u.requireSessionPolicyVisibleAtTarget(ctx, desiredSession, value.Target.Head.CommitSeq); err != nil {
		return memory.Policy{}, err
	}

	source, err := u.loadPreparedDialogueEvent(ctx, value.SourceEventID, generation.ResidentID, value.Target.Head.CommitSeq)
	if err != nil {
		return memory.Policy{}, err
	}
	if source.eventType != "user_message" {
		return memory.Policy{}, errors.New("sqlite: prepared dialogue source is not a user message")
	}
	if generation.IdempotencyKey != domain.DialogueObligation(value.SourceEventID) {
		return memory.Policy{}, errors.New("sqlite: prepared dialogue obligation differs from source event")
	}

	policy, err := u.loadPreparedDialogueMemoryPolicy(ctx, generation.ResidentID, generation.MemoryPolicyRevisionID)
	if err != nil {
		return memory.Policy{}, err
	}
	if generation.MemoryRenderingVersion != domain.MemoryRenderingVersionV2 ||
		(policy.MemoryRecallEnabled && policy.RenderingVersion != memory.RenderingVersionV2) {
		return memory.Policy{}, errors.New("sqlite: prepared dialogue memory rendering contract differs from rendering-v2")
	}
	switch value.RecallDisposition {
	case domain.RecallDispositionSuccess, domain.RecallDispositionExplicitFallback:
		if !policy.MemoryRecallEnabled {
			return memory.Policy{}, errors.New("sqlite: prepared dialogue Recall branch conflicts with disabled policy")
		}
	case domain.RecallDispositionPolicyDisabled:
		if policy.MemoryRecallEnabled {
			return memory.Policy{}, errors.New("sqlite: prepared dialogue policy-disabled branch conflicts with enabled policy")
		}
	}
	if value.RecallDisposition == domain.RecallDispositionSuccess {
		if err := u.requireNoDialogueRecallChangeAfter(ctx, generation.ResidentID, value.Target.Head.CommitSeq); err != nil {
			return memory.Policy{}, err
		}
	}

	if err := u.validatePreparedDialogueInputs(ctx, value, source, policy); err != nil {
		return memory.Policy{}, err
	}
	if err := u.validatePreparedDialogueAssembly(ctx, value); err != nil {
		return memory.Policy{}, err
	}
	return policy, nil
}

// requireNoDialogueRecallChangeAfter closes the Assembly-to-Commit-B race
// without rejecting unrelated Canonical head movement. These append-only rows
// are the complete Canonical input set for the claim_states/view-scope
// Projections and the Recall-only abstractness bit. A change after the frozen
// head requires a new Projection reconciliation and Assembly snapshot.
func (u *canonicalUoW) requireNoDialogueRecallChangeAfter(
	ctx context.Context,
	residentID canonical.ID,
	frozenHead canonical.CommitSeq,
) error {
	var changed int
	err := u.tx.QueryRowContext(ctx, `WITH boundary(resident_id, frozen_head) AS (VALUES (?, ?))
		SELECT EXISTS(
			SELECT 1 FROM claims row
			JOIN canonical_commits commit_row ON commit_row.canonical_commit_id = row.canonical_commit_id
			JOIN boundary ON boundary.resident_id = row.owner_resident_id
			WHERE commit_row.commit_seq > boundary.frozen_head
			UNION ALL
			SELECT 1 FROM claim_evidence row
			JOIN claims claim ON claim.claim_id = row.claim_id
			JOIN canonical_commits commit_row ON commit_row.canonical_commit_id = row.canonical_commit_id
			JOIN boundary ON boundary.resident_id = claim.owner_resident_id
			WHERE commit_row.commit_seq > boundary.frozen_head
			UNION ALL
			SELECT 1 FROM claim_usages row
			JOIN claims claim ON claim.claim_id = row.claim_id
			JOIN canonical_commits commit_row ON commit_row.canonical_commit_id = row.canonical_commit_id
			JOIN boundary ON boundary.resident_id = claim.owner_resident_id
			WHERE commit_row.commit_seq > boundary.frozen_head
			UNION ALL
			SELECT 1 FROM claim_validity_assertions row
			JOIN claims claim ON claim.claim_id = row.claim_id
			JOIN canonical_commits commit_row ON commit_row.canonical_commit_id = row.canonical_commit_id
			JOIN boundary ON boundary.resident_id = claim.owner_resident_id
			WHERE commit_row.commit_seq > boundary.frozen_head
			UNION ALL
			SELECT 1 FROM claim_status_transitions row
			JOIN claims claim ON claim.claim_id = row.claim_id
			JOIN canonical_commits commit_row ON commit_row.canonical_commit_id = row.canonical_commit_id
			JOIN boundary ON boundary.resident_id = claim.owner_resident_id
			WHERE commit_row.commit_seq > boundary.frozen_head
			UNION ALL
			SELECT 1 FROM claim_stage_transitions row
			JOIN claims claim ON claim.claim_id = row.claim_id
			JOIN canonical_commits commit_row ON commit_row.canonical_commit_id = row.canonical_commit_id
			JOIN boundary ON boundary.resident_id = claim.owner_resident_id
			WHERE commit_row.commit_seq > boundary.frozen_head
			UNION ALL
			SELECT 1 FROM claim_view_scope_assertions row
			JOIN claims claim ON claim.claim_id = row.claim_id
			JOIN canonical_commits commit_row ON commit_row.canonical_commit_id = row.canonical_commit_id
			JOIN boundary ON boundary.resident_id = claim.owner_resident_id
			WHERE commit_row.commit_seq > boundary.frozen_head
			UNION ALL
			SELECT 1 FROM claim_relations row
			JOIN claims claim ON claim.claim_id = row.from_claim_id
			JOIN canonical_commits commit_row ON commit_row.canonical_commit_id = row.canonical_commit_id
			JOIN boundary ON boundary.resident_id = claim.owner_resident_id
			WHERE row.relation_type = 'abstracts'
			  AND commit_row.commit_seq > boundary.frozen_head
		)`, residentID.String(), frozenHead.Int64()).Scan(&changed)
	if err != nil {
		return fmt.Errorf("sqlite: inspect post-Assembly Recall changes: %w", err)
	}
	if changed != 0 {
		return fmt.Errorf("%w: Recall Canonical inputs changed after Assembly",
			domain.ErrDialogueAssemblyTargetChanged)
	}
	return nil
}

// validatePreparedDialogueAssembly rebuilds the complete context-v3 result
// from the captured Assembly Target. Besides checking the final byte/input
// limits, this is the Writer-side authority for ordering, event deduplication,
// and the dropped-input summary. Callers cannot make those facts true merely
// by supplying internally consistent frozen rows.
func (u *canonicalUoW) validatePreparedDialogueAssembly(
	ctx context.Context,
	value domain.PrepareDialogue,
) error {
	generation := value.Generation
	snapshot, err := u.loadDialogueSnapshotAt(ctx, generation.ResidentID, value.Target)
	if err != nil {
		return fmt.Errorf("sqlite: rebuild prepared dialogue snapshot: %w", err)
	}
	if snapshot.pipelineID != generation.PipelineVersionID || snapshot.sessionID != *generation.SessionPolicyID ||
		snapshot.principlesID != generation.PrinciplesRevisionID || snapshot.personaID != generation.PersonaRevisionID ||
		snapshot.memoryID != generation.MemoryPolicyRevisionID {
		return errors.New("sqlite: prepared dialogue pinned tuple differs from its Assembly Target")
	}
	current, currentBytes, err := loadDialogueAssemblySource(ctx, u.tx, domain.DialogueAssemblyRequest{
		ResidentID: generation.ResidentID, SourceEventID: value.SourceEventID, Target: value.Target,
	})
	if err != nil {
		return err
	}
	recallSnapshot, err := u.loadPreparedDialogueRecallSnapshot(ctx, generation.ResidentID, snapshot, value.Target)
	if err != nil {
		return err
	}
	// Rebuild the context against the Assembly Target, not the Commit-B
	// transaction head. The bounded activity/backfill readers take their
	// Canonical ceiling from UoW metadata; retaining the Writer metadata here
	// would let a post-Assembly event participate in Commit-B validation.
	targetUoW := *u
	targetUoW.metadata.CommitSeq = value.Target.Head.CommitSeq
	targetUoW.metadata.CommittedAt = value.Target.AsOf
	targetUoW.metadata.CommittedTZ = value.Target.AsOfTZ

	base := append([]dialogueInputCandidate(nil), snapshot.definitions...)
	runtimeBytes := dialogueRuntimeProjectionBytes(recallSnapshot.enabled)
	base = append(base, dialogueInputCandidate{
		role: "system", sourceType: "runtime_projection", inclusionMode: "runtime_projection",
		content: runtimeBytes, byteSize: int64(len(runtimeBytes)),
	})
	live, err := targetUoW.loadLiveDialogueInputs(ctx, current, snapshot.idleGap, value.LiveEventLimit, nil)
	if err != nil {
		return err
	}
	base = append(base, live...)
	initiative, err := targetUoW.loadInitiativeDialogueInput(ctx, current, nil)
	if err != nil {
		return err
	}
	if initiative != nil {
		base = append(base, *initiative)
	}
	currentID := current.ID
	base = append(base, dialogueInputCandidate{
		role: "user", sourceType: "event", sourceID: &currentID, inclusionMode: "current_input",
		contentID: current.ContentID, content: append([]byte(nil), currentBytes...), byteSize: int64(len(currentBytes)),
	})

	var final []dialogueInputCandidate
	var budgetExceeded bool
	var droppedBackfill, droppedLive, droppedInitiative int
	switch value.RecallDisposition {
	case domain.RecallDispositionSuccess:
		if !recallSnapshot.enabled || recallSnapshot.fallback != "" {
			return errors.New("sqlite: successful dialogue Recall no longer matches its Assembly Target")
		}
		selection, err := memory.SelectRecall(recallSnapshot.policy, recallSnapshot.candidates)
		if err != nil {
			return err
		}
		var plan memory.RecallPlan
		var droppedRecall map[canonical.ID]struct{}
		final, plan, droppedRecall, budgetExceeded, droppedBackfill, droppedLive, droppedInitiative, err =
			assembleDialogueContextV2(base, recallSnapshot.policy, selection, value.MaxInputBytes, domain.MaxDialogueInputs)
		if err != nil {
			return err
		}
		if err := validatePreparedDialogueRecallPlanWithDrops(value, plan, droppedRecall); err != nil {
			return err
		}
	case domain.RecallDispositionExplicitFallback:
		if !recallSnapshot.enabled || recallSnapshot.fallback == "" ||
			recallSnapshot.fallback != value.RecallFallbackReason {
			return errors.New("sqlite: explicit dialogue Recall fallback differs from its Assembly Target")
		}
		final, budgetExceeded, droppedBackfill, droppedLive, droppedInitiative, _, err =
			applyDialogueBudget(base, value.MaxInputBytes, domain.MaxDialogueInputs)
		if err != nil {
			return err
		}
	case domain.RecallDispositionPolicyDisabled:
		if recallSnapshot.enabled || recallSnapshot.fallback != "" {
			return errors.New("sqlite: policy-disabled dialogue Recall differs from its Assembly Target")
		}
		final, budgetExceeded, droppedBackfill, droppedLive, droppedInitiative, _, err =
			applyDialogueBudget(base, value.MaxInputBytes, domain.MaxDialogueInputs)
		if err != nil {
			return err
		}
	default:
		return errors.New("sqlite: unsupported prepared dialogue Recall disposition")
	}

	dropped, err := dialogueDroppedInputSummary(
		droppedBackfill, droppedLive, droppedInitiative, recallSnapshot.fallback,
	)
	if err != nil {
		return err
	}
	if generation.BudgetExceeded != budgetExceeded ||
		!bytes.Equal(generation.DroppedInputSummary.Bytes(), dropped.Bytes()) {
		return errors.New("sqlite: prepared dialogue budget or dropped-input provenance differs from recomputed context-v4")
	}
	if len(generation.Inputs) != len(final) {
		return errors.New("sqlite: prepared dialogue input count differs from recomputed context-v4")
	}
	var total int64
	for index, expected := range final {
		actual := generation.Inputs[index]
		if actual.Role != expected.role || actual.SourceType != expected.sourceType ||
			actual.InclusionMode != expected.inclusionMode ||
			!equalPreparedDialogueSource(actual.SourceID, expected.sourceID) ||
			!bytes.Equal(actual.Content.Bytes, expected.content) {
			return fmt.Errorf("sqlite: prepared dialogue input %d differs from recomputed context-v4", index)
		}
		if int64(len(actual.Content.Bytes)) > math.MaxInt64-total {
			return errors.New("sqlite: prepared dialogue input byte total overflows")
		}
		total += int64(len(actual.Content.Bytes))
	}
	if total > value.MaxInputBytes {
		return errors.New("sqlite: prepared dialogue final inputs exceed MaxInputBytes")
	}
	return nil
}

func equalPreparedDialogueSource(left, right *canonical.ID) bool {
	if left == nil || right == nil {
		return left == nil && right == nil
	}
	return *left == *right
}

// loadPreparedDialogueRecallSnapshot permits Projection reconciliation to have
// advanced beyond the captured global head. Candidate material is still
// rebuilt against target.Head and compared with the frozen snapshot, so a
// related change fails closed while an unrelated Canonical commit does not.
func (u *canonicalUoW) loadPreparedDialogueRecallSnapshot(
	ctx context.Context,
	residentID canonical.ID,
	dialogue dialogueSnapshot,
	target domain.AssemblyTarget,
) (dialogueRecallSnapshot, error) {
	policy, _, err := memory.ParsePolicy(dialogue.memoryPolicy)
	if err != nil {
		return dialogueRecallSnapshot{}, fmt.Errorf("sqlite: parse prepared dialogue Recall policy: %w", err)
	}
	if policy.Version == memory.PolicyVersionV1 {
		return dialogueRecallSnapshot{}, nil
	}
	if err := policy.RequireEnabled(); err != nil {
		return dialogueRecallSnapshot{}, err
	}
	result := dialogueRecallSnapshot{enabled: true, policy: policy, head: target.Head.CommitSeq}
	for _, definition := range []projection.Definition{
		projection.ClaimStatesDefinition(), projection.ClaimViewScopeCurrentDefinition(),
	} {
		reason, err := u.inspectPreparedDialogueRecallProjection(
			ctx, residentID, dialogue.memoryID, target, definition,
		)
		if err != nil {
			return dialogueRecallSnapshot{}, err
		}
		if reason != "" {
			result.fallback = reason
			return result, nil
		}
	}
	result.pipelineID, result.fallback, err = u.loadRecallPipeline(ctx, target.Head.CommitSeq)
	if err != nil || result.fallback != "" {
		return result, err
	}
	result.candidates, err = u.loadRecallCandidates(ctx, residentID, target.Head.CommitSeq, policy)
	if errors.Is(err, errRecallProjectionProvenance) {
		result.fallback = domain.MemoryRecallProjectionProvenanceUnknown
		result.candidates = nil
		return result, nil
	}
	return result, err
}

func (u *canonicalUoW) inspectPreparedDialogueRecallProjection(
	ctx context.Context,
	residentID, memoryPolicyID canonical.ID,
	target domain.AssemblyTarget,
	definition projection.Definition,
) (domain.MemoryRecallFallbackReason, error) {
	var version, timezoneRaw string
	var sourceCommit, asOf int64
	err := u.tx.QueryRowContext(ctx, `SELECT projection_version, source_commit_seq, as_of, as_of_tz
		FROM projection_watermarks WHERE projection_name = ? AND resident_id = ?`,
		definition.Name, residentID.String()).Scan(&version, &sourceCommit, &asOf, &timezoneRaw)
	if errors.Is(err, sql.ErrNoRows) {
		return domain.MemoryRecallProjectionUnavailable, nil
	}
	if err != nil {
		return "", err
	}
	timezone, timezoneErr := canonical.ParseTimezone(timezoneRaw)
	if version != string(definition.Version) || sourceCommit < target.Head.CommitSeq.Int64() ||
		timezoneErr != nil || (definition.TimeSensitive &&
		(canonical.Instant(asOf) < target.AsOf || timezone != target.AsOfTZ)) {
		return domain.MemoryRecallProjectionProvenanceUnknown, nil
	}
	rows, err := u.tx.QueryContext(ctx, `SELECT dependency_kind, dependency_version_id
		FROM projection_watermark_dependencies
		WHERE projection_name = ? AND resident_id = ?
		ORDER BY dependency_kind, dependency_version_id`, definition.Name, residentID.String())
	if err != nil {
		return "", err
	}
	defer rows.Close()
	var actual []projection.Dependency
	for rows.Next() {
		var kindRaw, idRaw string
		if err := rows.Scan(&kindRaw, &idRaw); err != nil {
			return "", err
		}
		id, err := canonical.ParseID(idRaw)
		if err != nil {
			return domain.MemoryRecallProjectionProvenanceUnknown, nil
		}
		actual = append(actual, projection.Dependency{
			Kind: projection.DependencyKind(kindRaw), VersionID: id,
		})
	}
	if err := rows.Err(); err != nil {
		return "", err
	}
	expected := make([]projection.Dependency, 0, len(definition.Dependencies))
	for _, kind := range definition.Dependencies {
		if kind != projection.MemoryPolicyDependency {
			return domain.MemoryRecallProjectionProvenanceUnknown, nil
		}
		expected = append(expected, projection.Dependency{Kind: kind, VersionID: memoryPolicyID})
	}
	equal, err := projection.DependencySetEqual(actual, expected)
	if err != nil || !equal {
		return domain.MemoryRecallProjectionProvenanceUnknown, nil
	}
	return "", nil
}

func (u *canonicalUoW) validateDialogueAssemblyTarget(ctx context.Context, target domain.AssemblyTarget) error {
	if target.Head.CommitSeq >= u.metadata.CommitSeq {
		return errors.New("sqlite: dialogue Assembly target is not an existing pre-Prepare head")
	}
	if target.AsOf > u.metadata.CommittedAt {
		return errors.New("sqlite: dialogue Assembly target time is after Prepare commit time")
	}
	var committedAt int64
	if err := u.tx.QueryRowContext(ctx, `SELECT committed_at FROM canonical_commits WHERE commit_seq = ?`,
		target.Head.CommitSeq.Int64()).Scan(&committedAt); err != nil {
		return fmt.Errorf("sqlite: resolve dialogue Assembly target head: %w", err)
	}
	if canonical.Instant(committedAt) != target.Head.CommittedAt {
		return errors.New("sqlite: dialogue Assembly target head metadata differs from Canonical")
	}
	return nil
}

func (u *canonicalUoW) validateCurrentDialoguePipeline(
	ctx context.Context,
	pipelineID canonical.ID,
	head canonical.CommitSeq,
) error {
	var kind, versionKey, definitionRaw string
	var commitSeq int64
	if err := u.tx.QueryRowContext(ctx, `SELECT pipeline.pipeline_kind, pipeline.version_key,
		pipeline.definition, commit_row.commit_seq
		FROM pipeline_versions pipeline
		JOIN canonical_commits commit_row ON commit_row.canonical_commit_id = pipeline.canonical_commit_id
		WHERE pipeline.pipeline_version_id = ?`, pipelineID.String()).Scan(
		&kind, &versionKey, &definitionRaw, &commitSeq,
	); err != nil {
		return fmt.Errorf("sqlite: resolve prepared dialogue pipeline: %w", err)
	}
	if commitSeq > head.Int64() {
		return errors.New("sqlite: prepared dialogue pipeline was not visible at Assembly target")
	}
	definition, err := canonical.ParseCanonicalJSON([]byte(definitionRaw))
	if err != nil {
		return fmt.Errorf("sqlite: parse prepared dialogue pipeline definition: %w", err)
	}
	if err := domain.ValidateExactDialoguePipelineDefinition(domain.PipelineVersionDefinition{
		ID: pipelineID, Kind: kind, VersionKey: versionKey, Definition: definition,
	}); err != nil {
		return err
	}
	if kind != "dialogue" || versionKey != domain.DialoguePipelineVersionV4 {
		return errors.New("sqlite: new prepared dialogue requires the exact dialogue-v4 pipeline row")
	}
	return nil
}

func (u *canonicalUoW) requireRevisionVisibleAtTarget(
	ctx context.Context,
	residentID, revisionID canonical.ID,
	class string,
	head canonical.CommitSeq,
) error {
	var active string
	err := u.tx.QueryRowContext(ctx, `SELECT activation.revision_id
		FROM resident_revision_activations activation
		JOIN resident_revisions revision ON revision.revision_id = activation.revision_id
		JOIN canonical_commits commit_row ON commit_row.canonical_commit_id = activation.canonical_commit_id
		WHERE activation.resident_id = ? AND revision.resident_id = ?
		  AND revision.revision_class = ? AND commit_row.commit_seq <= ?
		ORDER BY commit_row.commit_seq DESC, activation.activation_id DESC LIMIT 1`,
		residentID.String(), residentID.String(), class, head.Int64()).Scan(&active)
	if err != nil {
		return fmt.Errorf("sqlite: resolve active %s revision at dialogue Assembly target: %w", class, err)
	}
	if active != revisionID.String() {
		return fmt.Errorf("sqlite: prepared dialogue %s revision was not active at Assembly target", class)
	}
	return nil
}

func (u *canonicalUoW) requireSessionPolicyVisibleAtTarget(
	ctx context.Context,
	policyID canonical.ID,
	head canonical.CommitSeq,
) error {
	var commitSeq int64
	if err := u.tx.QueryRowContext(ctx, `SELECT commit_row.commit_seq
		FROM sessionization_policy_versions policy
		JOIN canonical_commits commit_row ON commit_row.canonical_commit_id = policy.canonical_commit_id
		WHERE policy.sessionization_policy_version_id = ?`, policyID.String()).Scan(&commitSeq); err != nil {
		return fmt.Errorf("sqlite: resolve prepared dialogue sessionization policy: %w", err)
	}
	if commitSeq > head.Int64() {
		return errors.New("sqlite: prepared dialogue sessionization policy was not visible at Assembly target")
	}
	return nil
}

type preparedDialogueEvent struct {
	id         canonical.ID
	seq        canonical.Seq
	eventType  string
	visibility string
	contentID  canonical.ID
	content    []byte
	commitment canonical.Digest
}

func (u *canonicalUoW) loadPreparedDialogueEvent(
	ctx context.Context,
	eventID, residentID canonical.ID,
	head canonical.CommitSeq,
) (preparedDialogueEvent, error) {
	var event preparedDialogueEvent
	var rawID, rawResident, rawContent, erasureState string
	var seq, commitSeq int64
	var payloadCommitment, objectCommitment []byte
	err := u.tx.QueryRowContext(ctx, `SELECT event.event_id, event.resident_id, event.seq,
		event.event_type, event.visibility, event.content_id, event.payload_commitment, object.commitment,
		object.erasure_state, blob.content, commit_row.commit_seq
		FROM events event
		JOIN canonical_commits commit_row ON commit_row.canonical_commit_id = event.canonical_commit_id
		JOIN content_objects object ON object.content_id = event.content_id
		LEFT JOIN blobs blob ON blob.dedupe_scope_id = object.owner_resident_id
		 AND blob.hash_algorithm = object.blob_hash_algorithm AND blob.blob_hash = object.blob_hash
		WHERE event.event_id = ?`, eventID.String()).Scan(
		&rawID, &rawResident, &seq, &event.eventType, &event.visibility, &rawContent, &payloadCommitment,
		&objectCommitment, &erasureState, &event.content, &commitSeq,
	)
	if err != nil {
		return preparedDialogueEvent{}, fmt.Errorf("sqlite: resolve prepared dialogue event source: %w", err)
	}
	if rawResident != residentID.String() || commitSeq > head.Int64() {
		return preparedDialogueEvent{}, errors.New("sqlite: prepared dialogue event source is outside resident or Assembly target")
	}
	if erasureState != "present" || event.content == nil {
		return preparedDialogueEvent{}, domain.ErrClaimSourceIneligible
	}
	if !bytes.Equal(payloadCommitment, objectCommitment) {
		return preparedDialogueEvent{}, errors.New("sqlite: prepared dialogue event payload commitment differs from content")
	}
	var parseErr error
	event.id, parseErr = canonical.ParseID(rawID)
	if parseErr != nil {
		return preparedDialogueEvent{}, parseErr
	}
	event.contentID, parseErr = canonical.ParseID(rawContent)
	if parseErr != nil {
		return preparedDialogueEvent{}, parseErr
	}
	event.seq, parseErr = canonical.NewSeq(seq)
	if parseErr != nil {
		return preparedDialogueEvent{}, parseErr
	}
	event.commitment, parseErr = canonical.DigestFromBytes(payloadCommitment)
	if parseErr != nil {
		return preparedDialogueEvent{}, parseErr
	}
	if err := u.validatePresentContentObject(ctx, event.contentID, residentID, event.content, ""); err != nil {
		return preparedDialogueEvent{}, err
	}
	return event, nil
}

func (u *canonicalUoW) validatePresentContentObject(
	ctx context.Context,
	contentID, residentID canonical.ID,
	expected []byte,
	expectedClass string,
) error {
	var owner, class, erasureState, erasurePolicy, blobAlgorithm, commitmentAlgorithm, commitmentDomain, canonicalization string
	var blobHashRaw, commitmentRaw, saltRaw, content []byte
	err := u.tx.QueryRowContext(ctx, `SELECT object.owner_resident_id, object.content_class,
		object.erasure_state, object.erasure_policy, object.blob_hash_algorithm,
		object.commitment_hash_algorithm, object.commitment_domain, object.canonicalization_version,
		object.blob_hash, object.commitment, object.commitment_salt, blob.content
		FROM content_objects object
		LEFT JOIN blobs blob ON blob.dedupe_scope_id = object.owner_resident_id
		 AND blob.hash_algorithm = object.blob_hash_algorithm AND blob.blob_hash = object.blob_hash
		WHERE object.content_id = ?`, contentID.String()).Scan(
		&owner, &class, &erasureState, &erasurePolicy, &blobAlgorithm, &commitmentAlgorithm,
		&commitmentDomain, &canonicalization, &blobHashRaw, &commitmentRaw, &saltRaw, &content,
	)
	if err != nil {
		return fmt.Errorf("sqlite: resolve prepared dialogue Canonical content: %w", err)
	}
	if owner != residentID.String() || (expectedClass != "" && class != expectedClass) ||
		erasureState != "present" || content == nil ||
		blobAlgorithm != canonical.HashAlgorithm || commitmentAlgorithm != canonical.HashAlgorithm ||
		commitmentDomain != canonical.ContentCommitmentDomain || canonicalization != canonical.CanonicalizationVersion {
		return errors.New("sqlite: prepared dialogue Canonical content metadata is invalid or unavailable")
	}
	if !bytes.Equal(content, expected) {
		return errors.New("sqlite: prepared dialogue frozen bytes differ from Canonical source")
	}
	blobHash, err := canonical.DigestFromBytes(blobHashRaw)
	if err != nil || blobHash != canonical.HashBlob(content) {
		return errors.New("sqlite: prepared dialogue Canonical source blob hash is invalid")
	}
	salt, err := canonical.ContentSaltFromBytes(saltRaw)
	if err != nil {
		return errors.New("sqlite: prepared dialogue Canonical source content salt is invalid")
	}
	commitment, err := canonical.CommitContent(class, salt, content)
	if err != nil || !bytes.Equal(commitment.Bytes(), commitmentRaw) {
		return errors.New("sqlite: prepared dialogue Canonical source commitment is invalid")
	}
	if erasurePolicy != "independent" && erasurePolicy != "resident_only" {
		return errors.New("sqlite: prepared dialogue Canonical source erasure policy is invalid")
	}
	return nil
}

func (u *canonicalUoW) validatePreparedDialogueInputs(
	ctx context.Context,
	value domain.PrepareDialogue,
	source preparedDialogueEvent,
	policy memory.Policy,
) error {
	generation := value.Generation
	for _, input := range generation.Inputs {
		switch input.SourceType {
		case "event":
			if input.SourceID == nil {
				return errors.New("sqlite: prepared dialogue event input has no source")
			}
			event, err := u.loadPreparedDialogueEvent(ctx, *input.SourceID, generation.ResidentID, value.Target.Head.CommitSeq)
			if err != nil {
				return err
			}
			if err := validatePreparedDialogueEventInput(input, event, source); err != nil {
				return err
			}
		case "resident_revision":
			if err := u.validatePreparedDialogueRevisionInput(
				ctx, input, generation, value.Target.Head.CommitSeq,
			); err != nil {
				return err
			}
		case "content":
			if input.SourceID == nil {
				return errors.New("sqlite: prepared dialogue content input has no source")
			}
			if err := u.validatePresentContentObject(ctx, *input.SourceID, generation.ResidentID, input.Content.Bytes, ""); err != nil {
				return err
			}
		case "runtime_projection":
			if err := validatePreparedDialogueRuntimeInput(input, policy); err != nil {
				return err
			}
		case "claim":
			if input.SourceID == nil || input.InclusionMode != "memory_recall" || input.Role != "system" {
				return errors.New("sqlite: prepared dialogue claim provenance is invalid")
			}
		default:
			return fmt.Errorf("sqlite: unsupported prepared dialogue source type %q", input.SourceType)
		}
	}
	return nil
}

func validatePreparedDialogueEventInput(
	input domain.GenerationInput,
	event, source preparedDialogueEvent,
) error {
	if event.visibility != "conversation" {
		return errors.New("sqlite: prepared dialogue event input is not conversation-visible")
	}
	if event.id == source.id {
		if input.SourceID == nil || *input.SourceID != source.id || input.InclusionMode != "current_input" ||
			input.Role != "user" || event.eventType != "user_message" {
			return errors.New("sqlite: prepared dialogue current input provenance is invalid")
		}
	} else {
		if event.seq >= source.seq {
			return errors.New("sqlite: prepared dialogue event input is not before its source event")
		}
		wantRole := ""
		switch event.eventType {
		case "user_message":
			wantRole = "user"
		case "resident_message":
			wantRole = "assistant"
		case "outbound_initiative":
			if input.InclusionMode == "context_backfill" {
				wantRole = "assistant"
			}
		}
		validMode := input.InclusionMode == "context_backfill" ||
			(input.InclusionMode == "live_context" && event.eventType != "outbound_initiative")
		if !validMode || wantRole == "" || input.Role != wantRole {
			return errors.New("sqlite: prepared dialogue context event role/type/inclusion is invalid")
		}
	}
	if !bytes.Equal(input.Content.Bytes, event.content) {
		return errors.New("sqlite: prepared dialogue event input bytes differ from Canonical source")
	}
	return nil
}

func (u *canonicalUoW) validatePreparedDialogueRevisionInput(
	ctx context.Context,
	input domain.GenerationInput,
	generation domain.PrepareGeneration,
	head canonical.CommitSeq,
) error {
	if input.SourceID == nil {
		return errors.New("sqlite: prepared dialogue revision input has no source")
	}
	pinned := map[canonical.ID]string{
		generation.PrinciplesRevisionID:   "principles",
		generation.PersonaRevisionID:      "persona",
		generation.MemoryPolicyRevisionID: "memory_policy",
	}
	class, expected := pinned[*input.SourceID]
	if !expected || input.InclusionMode != "resident_definition" || input.Role != "system" {
		return errors.New("sqlite: prepared dialogue revision input is not one of the pinned definitions")
	}
	var contentRaw, actualClass string
	var commitSeq int64
	if err := u.tx.QueryRowContext(ctx, `SELECT revision.content_id, revision.revision_class, commit_row.commit_seq
		FROM resident_revisions revision
		JOIN canonical_commits commit_row ON commit_row.canonical_commit_id = revision.canonical_commit_id
		WHERE revision.revision_id = ? AND revision.resident_id = ?`,
		input.SourceID.String(), generation.ResidentID.String()).Scan(
		&contentRaw, &actualClass, &commitSeq,
	); err != nil {
		return fmt.Errorf("sqlite: resolve prepared dialogue revision source: %w", err)
	}
	if actualClass != class || commitSeq > head.Int64() {
		return errors.New("sqlite: prepared dialogue revision source differs from pinned class or commit ceiling")
	}
	contentID, err := canonical.ParseID(contentRaw)
	if err != nil {
		return err
	}
	return u.validatePresentContentObject(ctx, contentID, generation.ResidentID, input.Content.Bytes, "")
}

func validatePreparedDialogueRuntimeInput(input domain.GenerationInput, policy memory.Policy) error {
	if input.SourceID != nil || input.InclusionMode != "runtime_projection" || input.Role != "system" ||
		!bytes.Equal(input.Content.Bytes, dialogueRuntimeProjectionBytes(policy.MemoryRecallEnabled)) {
		return errors.New("sqlite: prepared dialogue runtime Projection provenance or frozen bytes are invalid")
	}
	return nil
}

func (u *canonicalUoW) loadPreparedDialogueMemoryPolicy(
	ctx context.Context,
	residentID, revisionID canonical.ID,
) (memory.Policy, error) {
	var revisionResident, revisionClass, contentClass, erasureState string
	var content []byte
	err := u.tx.QueryRowContext(ctx, `SELECT revision.resident_id, revision.revision_class,
		object.content_class, object.erasure_state, blob.content
		FROM resident_revisions revision
		JOIN content_objects object ON object.content_id = revision.content_id
		LEFT JOIN blobs blob ON blob.dedupe_scope_id = object.owner_resident_id
		 AND blob.hash_algorithm = object.blob_hash_algorithm AND blob.blob_hash = object.blob_hash
		WHERE revision.revision_id = ?`, revisionID.String()).Scan(
		&revisionResident, &revisionClass, &contentClass, &erasureState, &content,
	)
	if err != nil {
		return memory.Policy{}, fmt.Errorf("sqlite: load prepared dialogue memory policy: %w", err)
	}
	if revisionResident != residentID.String() || revisionClass != "memory_policy" ||
		contentClass != "memory_policy_text" || erasureState != "present" || content == nil {
		return memory.Policy{}, errors.New("sqlite: prepared dialogue memory policy is unavailable or invalid")
	}
	policy, _, err := memory.ParsePolicy(content)
	if err != nil {
		return memory.Policy{}, fmt.Errorf("sqlite: parse prepared dialogue memory policy: %w", err)
	}
	return policy, nil
}

func (u *canonicalUoW) validateSuccessfulDialogueRecall(
	ctx context.Context,
	value domain.PrepareDialogue,
	policy memory.Policy,
) error {
	if value.Recall == nil {
		return errors.New("sqlite: successful prepared dialogue has no Recall envelope")
	}
	recall := *value.Recall
	for _, definition := range []projection.Definition{
		projection.ClaimStatesDefinition(), projection.ClaimViewScopeCurrentDefinition(),
	} {
		if err := u.requireRecallProjectionAtTarget(ctx, value.Generation.ResidentID, value.Generation.MemoryPolicyRevisionID, value.Target, definition); err != nil {
			return err
		}
	}
	pipelineID, fallback, err := u.loadRecallPipeline(ctx, value.Target.Head.CommitSeq)
	if err != nil {
		return err
	}
	if fallback != "" || pipelineID != recall.PipelineVersionID {
		return errors.New("sqlite: successful prepared dialogue does not reference the exact Recall pipeline at target")
	}
	sourceContentID, err := u.preparedDialogueSourceContentID(ctx, value.SourceEventID, value.Generation.ResidentID)
	if err != nil {
		return err
	}
	if recall.QueryContentID != sourceContentID {
		return errors.New("sqlite: dialogue Recall query content differs from source user message")
	}
	query, constraints, err := domain.NewRecallDecisionJSON(value.Target.Head.CommitSeq, policy, recallContextCompatibility())
	if err != nil {
		return err
	}
	if !bytes.Equal(query.Bytes(), recall.QueryConditions.Bytes()) ||
		!bytes.Equal(constraints.Bytes(), recall.ContextConstraints.Bytes()) {
		return errors.New("sqlite: dialogue Recall decision parameters differ from policy and Assembly target")
	}

	canonicalCandidates, err := u.loadRecallCandidates(ctx, value.Generation.ResidentID, value.Target.Head.CommitSeq, policy)
	if err != nil {
		return fmt.Errorf("sqlite: rebuild prepared dialogue Recall candidates: %w", err)
	}
	candidates, err := preparedDialogueMemoryCandidates(value.RecallCandidates)
	if err != nil {
		return err
	}
	if !equalRecallCandidates(candidates, canonicalCandidates) {
		return errors.New("sqlite: dialogue Recall candidate snapshot differs from target Projection and Canonical provenance")
	}
	selection, err := memory.SelectRecall(policy, candidates)
	if err != nil {
		return err
	}
	if err := u.revalidateRecallSelection(ctx, value.Generation.ResidentID,
		value.Generation.MemoryPolicyRevisionID, value.Target.Head.CommitSeq, selection); err != nil {
		return fmt.Errorf("sqlite: revalidate prepared dialogue Recall selection: %w", err)
	}
	plan, err := memory.PlanRecallV2(policy, selection, finalPreparedDialogueContextEventIDs(value.Generation.Inputs))
	if err != nil {
		return err
	}
	return validatePreparedDialogueRecallPlan(value, plan)
}

func (u *canonicalUoW) preparedDialogueSourceContentID(
	ctx context.Context,
	eventID, residentID canonical.ID,
) (canonical.ID, error) {
	var raw string
	if err := u.tx.QueryRowContext(ctx, `SELECT content_id FROM events WHERE event_id = ? AND resident_id = ?`,
		eventID.String(), residentID.String()).Scan(&raw); err != nil {
		return canonical.ID{}, fmt.Errorf("sqlite: resolve prepared dialogue source content: %w", err)
	}
	id, err := canonical.ParseID(raw)
	if err != nil {
		return canonical.ID{}, err
	}
	return id, nil
}

func (u *canonicalUoW) requireRecallProjectionAtTarget(
	ctx context.Context,
	residentID, memoryPolicyID canonical.ID,
	target domain.AssemblyTarget,
	definition projection.Definition,
) error {
	var version, timezone string
	var sourceCommit, asOf int64
	err := u.tx.QueryRowContext(ctx, `SELECT projection_version, source_commit_seq, as_of, as_of_tz
		FROM projection_watermarks WHERE projection_name = ? AND resident_id = ?`,
		definition.Name, residentID.String()).Scan(&version, &sourceCommit, &asOf, &timezone)
	if err != nil {
		return fmt.Errorf("sqlite: resolve successful Recall Projection target %s: %w", definition.Name, err)
	}
	if version != string(definition.Version) || sourceCommit < target.Head.CommitSeq.Int64() {
		return fmt.Errorf("sqlite: successful Recall Projection %s is behind Assembly head", definition.Name)
	}
	if definition.TimeSensitive && (asOf < target.AsOf.UnixMicro() || timezone != target.AsOfTZ.String()) {
		return fmt.Errorf("sqlite: successful Recall Projection %s is behind Assembly time", definition.Name)
	}
	rows, err := u.tx.QueryContext(ctx, `SELECT dependency_kind, dependency_version_id
		FROM projection_watermark_dependencies WHERE projection_name = ? AND resident_id = ?
		ORDER BY dependency_kind, dependency_version_id`, definition.Name, residentID.String())
	if err != nil {
		return err
	}
	defer rows.Close()
	var dependencies []projection.Dependency
	for rows.Next() {
		var kind, raw string
		if err := rows.Scan(&kind, &raw); err != nil {
			return err
		}
		id, err := canonical.ParseID(raw)
		if err != nil {
			return err
		}
		dependencies = append(dependencies, projection.Dependency{Kind: projection.DependencyKind(kind), VersionID: id})
	}
	if err := rows.Err(); err != nil {
		return err
	}
	expected := make([]projection.Dependency, 0, len(definition.Dependencies))
	for _, kind := range definition.Dependencies {
		if kind != projection.MemoryPolicyDependency {
			return fmt.Errorf("sqlite: unsupported Recall Projection dependency %q", kind)
		}
		expected = append(expected, projection.Dependency{Kind: kind, VersionID: memoryPolicyID})
	}
	equal, err := projection.DependencySetEqual(dependencies, expected)
	if err != nil || !equal {
		return fmt.Errorf("sqlite: successful Recall Projection %s dependency differs from pinned policy", definition.Name)
	}
	return nil
}

func preparedDialogueMemoryCandidates(values []domain.RecallCandidateSnapshot) ([]memory.RecallCandidate, error) {
	result := make([]memory.RecallCandidate, 0, len(values))
	for _, value := range values {
		if err := value.Validate(); err != nil {
			return nil, err
		}
		result = append(result, memory.RecallCandidate{
			ClaimID: value.ClaimID, Statement: value.Statement,
			ContextCompatibility: value.ContextCompatibility, Salience: value.Salience,
			Confidence: value.Confidence, Currentness: value.Currentness,
			LastConfirmed: cloneRecallInstant(value.LastConfirmed), Status: memory.ClaimStatus(value.Status),
			Stage: memory.ClaimStage(value.Stage), TemporalRelation: memory.TemporalRelation(value.TemporalRelation),
			SourceEventIDs: append([]canonical.ID(nil), value.SourceEventIDs...), Abstract: value.Abstract,
		})
	}
	return result, nil
}

func cloneRecallInstant(value *canonical.Instant) *canonical.Instant {
	if value == nil {
		return nil
	}
	cloned := *value
	return &cloned
}

func equalRecallCandidates(left, right []memory.RecallCandidate) bool {
	if len(left) != len(right) {
		return false
	}
	for index := range left {
		l, r := left[index], right[index]
		if l.ClaimID != r.ClaimID || l.Statement != r.Statement ||
			l.ContextCompatibility != r.ContextCompatibility || l.Salience != r.Salience ||
			l.Confidence != r.Confidence || l.Currentness != r.Currentness ||
			!sameRecallInstant(l.LastConfirmed, r.LastConfirmed) ||
			l.Status != r.Status || l.Stage != r.Stage ||
			l.TemporalRelation != r.TemporalRelation || l.Abstract != r.Abstract ||
			!slices.Equal(l.SourceEventIDs, r.SourceEventIDs) {
			return false
		}
	}
	return true
}

func finalPreparedDialogueContextEventIDs(inputs []domain.GenerationInput) []canonical.ID {
	seen := make(map[canonical.ID]struct{})
	var result []canonical.ID
	for _, input := range inputs {
		if input.SourceType != "event" || input.SourceID == nil ||
			(input.InclusionMode != "live_context" && input.InclusionMode != "context_backfill") {
			continue
		}
		if _, duplicate := seen[*input.SourceID]; duplicate {
			continue
		}
		seen[*input.SourceID] = struct{}{}
		result = append(result, *input.SourceID)
	}
	return result
}

type recallUsageSemantic struct {
	claimID   canonical.ID
	kind      memory.UsageType
	ordinal   canonical.Ordinal
	exclusion memory.RecallExclusionReason
}

func validatePreparedDialogueRecallPlan(value domain.PrepareDialogue, plan memory.RecallPlan) error {
	return validatePreparedDialogueRecallPlanWithDrops(value, plan, nil)
}

func validatePreparedDialogueRecallPlanWithDrops(
	value domain.PrepareDialogue,
	plan memory.RecallPlan,
	droppedRecall map[canonical.ID]struct{},
) error {
	if value.Recall == nil {
		return errors.New("sqlite: successful prepared dialogue has no Recall envelope")
	}
	finalClaims := make([]canonical.ID, 0, len(plan.Prompt))
	finalBytes := make(map[canonical.ID][]byte, len(plan.Prompt))
	for _, input := range value.Generation.Inputs {
		if input.SourceType != "claim" || input.InclusionMode != "memory_recall" || input.SourceID == nil {
			continue
		}
		finalClaims = append(finalClaims, *input.SourceID)
		finalBytes[*input.SourceID] = input.Content.Bytes
	}
	finalSet := make(map[canonical.ID]struct{}, len(finalClaims))
	for _, claimID := range finalClaims {
		if _, duplicate := finalSet[claimID]; duplicate {
			return errors.New("sqlite: prepared dialogue final Recall inputs repeat a claim")
		}
		finalSet[claimID] = struct{}{}
	}
	if droppedRecall != nil {
		knownDropped := make(map[canonical.ID]struct{}, len(droppedRecall))
		for _, decision := range plan.Prompt {
			claimID := decision.Candidate.Candidate.ClaimID
			_, dropped := droppedRecall[claimID]
			_, included := finalSet[claimID]
			if dropped == included {
				return errors.New("sqlite: prepared dialogue Recall budget exclusions differ from recomputed context-v4")
			}
			if dropped {
				knownDropped[claimID] = struct{}{}
			}
		}
		if len(knownDropped) != len(droppedRecall) {
			return errors.New("sqlite: prepared dialogue Recall drop set contains a non-prompt claim")
		}
	}

	var expected []recallUsageSemantic
	for _, decision := range plan.Decisions {
		expected = append(expected, recallUsageSemantic{
			claimID: decision.Candidate.Candidate.ClaimID, kind: memory.UsageCandidate,
			ordinal: decision.Candidate.CandidateOrdinal,
		})
	}
	for selectedOrdinal := int64(0); ; selectedOrdinal++ {
		decision, found := recallDecisionBySelectedOrdinal(plan.Decisions, selectedOrdinal)
		if !found {
			break
		}
		exclusion := decision.ExclusionReason
		if decision.PromptIncluded {
			if _, included := finalSet[decision.Candidate.Candidate.ClaimID]; !included {
				exclusion = memory.ExclusionTokenBudget
			}
		}
		ordinal, _ := canonical.NewOrdinal(selectedOrdinal)
		expected = append(expected, recallUsageSemantic{
			claimID: decision.Candidate.Candidate.ClaimID, kind: memory.UsageSelected,
			ordinal: ordinal, exclusion: exclusion,
		})
	}
	var expectedPrompt []canonical.ID
	for promptOrdinal := int64(0); ; promptOrdinal++ {
		decision, found := recallDecisionByPromptOrdinal(plan.Decisions, promptOrdinal)
		if !found {
			break
		}
		claimID := decision.Candidate.Candidate.ClaimID
		if _, included := finalSet[claimID]; !included {
			continue
		}
		expectedPrompt = append(expectedPrompt, claimID)
		ordinal, _ := canonical.NewOrdinal(int64(len(expectedPrompt) - 1))
		expected = append(expected, recallUsageSemantic{
			claimID: claimID, kind: memory.UsagePromptIncluded, ordinal: ordinal,
		})
		if !bytes.Equal(finalBytes[claimID], []byte(decision.RenderedText)) {
			return errors.New("sqlite: prepared dialogue Recall rendered input bytes differ from rendering-v2")
		}
	}
	if !slices.Equal(finalClaims, expectedPrompt) {
		return errors.New("sqlite: prepared dialogue final Recall input order differs from recomputed prompt plan")
	}
	if len(value.Recall.Usages) != len(expected) {
		return errors.New("sqlite: prepared dialogue Recall usage count differs from recomputed context-v4 plan")
	}
	for index, actual := range value.Recall.Usages {
		want := expected[index]
		if actual.ClaimID != want.claimID || actual.Type != want.kind || actual.Ordinal != want.ordinal ||
			actual.ExclusionReason != want.exclusion {
			return fmt.Errorf("sqlite: prepared dialogue Recall usage %d differs from recomputed context-v4 plan", index)
		}
	}
	return nil
}
