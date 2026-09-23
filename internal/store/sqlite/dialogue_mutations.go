package sqlite

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"math"

	"mahoroba.local/mahoroba/internal/canonical"
	"mahoroba.local/mahoroba/internal/domain"
	"mahoroba.local/mahoroba/internal/generation"
	"mahoroba.local/mahoroba/internal/memory"
)

func (u *canonicalUoW) IngressUserMessage(ctx context.Context, value domain.IngressUserMessage) (domain.Event, error) {
	if err := u.requireResidentScope(value.ResidentID); err != nil {
		return domain.Event{}, err
	}
	if err := u.requireActiveResident(ctx, value.ResidentID); err != nil {
		return domain.Event{}, err
	}
	if err := u.requireOperationallySelectedResident(ctx, value.ResidentID); err != nil {
		return domain.Event{}, err
	}
	if err := u.requirePrincipal(ctx, value.OwnerPrincipalID, "human"); err != nil {
		return domain.Event{}, err
	}
	if err := u.requireResidentPrincipal(ctx, value.ResidentID, value.ResidentPrincipalID); err != nil {
		return domain.Event{}, err
	}
	if err := u.insertContent(ctx, value.Content); err != nil {
		return domain.Event{}, err
	}
	target := value.ResidentPrincipalID
	event, err := u.makeEvent(ctx, eventSpec{
		ID: value.EventID, ResidentID: value.ResidentID, Type: "user_message",
		Visibility: "conversation", DeliveryScreen: true, DeliveryAudio: false,
		Ingress: "local_ui", ActorPrincipalID: value.OwnerPrincipalID,
		TargetPrincipalID: &target, OccurredAt: value.OccurredAt, OccurredTZ: value.OccurredTZ,
		Content: value.Content,
	})
	if err != nil {
		return domain.Event{}, err
	}
	return event, u.insertEvent(ctx, event, "conversation", true, false, "local_ui")
}

func (u *canonicalUoW) PrepareGeneration(ctx context.Context, value domain.PrepareGeneration) error {
	return u.prepareGeneration(ctx, value, false)
}

func (u *canonicalUoW) prepareGeneration(
	ctx context.Context,
	value domain.PrepareGeneration,
	allowMemoryReextraction bool,
) error {
	if err := u.requireResidentScope(value.ResidentID); err != nil {
		return err
	}
	purpose := value.Purpose.Effective()
	if err := purpose.Validate(); err != nil {
		return err
	}
	if purpose == domain.GenerationPurposeDialogue {
		return errors.New("sqlite: dialogue generation requires its dedicated PrepareDialogue command")
	}
	if purpose == domain.GenerationPurposeSelfTalk || purpose == domain.GenerationPurposeOutboundInitiative {
		return errors.New("sqlite: autonomous generation requires its dedicated prepare command")
	}
	if purpose == domain.GenerationPurposeMemoryExtraction {
		request, err := domain.ParseMemoryExtractionObligation(value.IdempotencyKey)
		if err != nil {
			return fmt.Errorf("sqlite: invalid memory extraction key: %w", err)
		}
		if request.Mode == domain.MemoryExtractionReextract && !allowMemoryReextraction {
			return errors.New("sqlite: Admin memory re-extraction requires its dedicated command")
		}
		if request.Mode == domain.MemoryExtractionMandatory {
			if err := u.requireDialoguePreparedForMemory(ctx, value.ResidentID, request.SourceEventID); err != nil {
				return err
			}
		}
	}
	if err := u.requireActiveResident(ctx, value.ResidentID); err != nil {
		return err
	}
	if purpose == domain.GenerationPurposeDialogue {
		if err := u.requireOperationallySelectedResident(ctx, value.ResidentID); err != nil {
			return err
		}
	}
	if err := u.requireGenerationRevisions(ctx, value, purpose); err != nil {
		return err
	}
	for _, input := range value.Inputs {
		if err := u.validateGenerationSource(ctx, value, input, false); err != nil {
			return err
		}
	}
	if err := u.insertGenerationRun(ctx, value); err != nil {
		return err
	}
	m := u.metadata
	for _, input := range value.Inputs {
		if err := u.insertContent(ctx, input.Content); err != nil {
			return err
		}
		var sourceID any
		if input.SourceID != nil {
			sourceID = input.SourceID.String()
		}
		if _, err := u.tx.ExecContext(ctx, `INSERT INTO generation_run_inputs(
			generation_run_input_id, canonical_commit_id, generation_run_id, ordinal, role,
			source_type, source_id, inclusion_mode, content_id, recorded_at, recorded_tz
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`, input.ID.String(), m.CommitID.String(), value.RunID.String(),
			input.Ordinal, input.Role, input.SourceType, sourceID, input.InclusionMode, input.Content.ID.String(),
			m.CommittedAt.UnixMicro(), m.CommittedTZ.String()); err != nil {
			return fmt.Errorf("insert generation input %d: %w", input.Ordinal, err)
		}
	}
	return u.insertOutcome(ctx, value.RunningOutcomeID, value.RunID, 1, "running", nil, nil, nil, 0, "")
}

// requireDialoguePreparedForMemory enforces the source-event ordering
// contract for user messages: their mandatory memory extraction may only be
// written after the dialogue obligation has a durable normal (legacy/current)
// or synthetic no-dispatch run. Self-talk is also a mandatory memory source,
// but has no dialogue obligation and therefore does not require a predecessor.
// The check is inside the same UoW as the memory rows so a missing user-message
// dialogue run cannot leave a half-prepared extraction behind.
func (u *canonicalUoW) requireDialoguePreparedForMemory(ctx context.Context, residentID, sourceEventID canonical.ID) error {
	var sourceType string
	if err := u.tx.QueryRowContext(ctx, `SELECT event_type FROM events
		WHERE resident_id = ? AND event_id = ?`, residentID.String(), sourceEventID.String()).Scan(&sourceType); err != nil {
		return fmt.Errorf("sqlite: resolve mandatory memory source event: %w", err)
	}
	if sourceType != "user_message" {
		return nil
	}
	var runRaw string
	err := u.tx.QueryRowContext(ctx, `SELECT generation_run_id FROM generation_runs
		WHERE resident_id = ? AND idempotency_key = ? AND purpose = 'dialogue'`,
		residentID.String(), domain.DialogueObligation(sourceEventID)).Scan(&runRaw)
	if errors.Is(err, sql.ErrNoRows) {
		return errors.New("sqlite: mandatory memory extraction requires a prepared dialogue obligation")
	}
	if err != nil {
		return fmt.Errorf("sqlite: resolve source dialogue obligation: %w", err)
	}
	runID, err := canonical.ParseID(runRaw)
	if err != nil {
		return err
	}
	if _, err := classifyNormalDialogueExecutionContract(ctx, u.tx, runID); err == nil {
		return nil
	}
	// Synthetic cancellation is a valid terminal predecessor, but only when the
	// persisted row is the exact source-time no-dispatch envelope. A bare
	// attempt-0/cancelled outcome is insufficient: accepting it would let a
	// malformed or mixed-version dialogue row authorize memory extraction.
	return u.validateSyntheticDialoguePredecessor(ctx, residentID, sourceEventID, runID)
}

func (u *canonicalUoW) validateSyntheticDialoguePredecessor(
	ctx context.Context,
	residentID, sourceEventID, runID canonical.ID,
) error {
	var residentRaw, purpose, obligation, provider, model string
	var pipelineRaw, pipelineKind, pipelineVersion, pipelineDefinition string
	var promptVersion, contextVersion, renderingVersion string
	var recallRaw, sessionRaw sql.NullString
	if err := u.tx.QueryRowContext(ctx, `SELECT run.resident_id, run.purpose,
		run.idempotency_key, run.provider, run.model,
		run.prompt_template_version, run.pipeline_version_id,
		pipeline.pipeline_kind, pipeline.version_key, pipeline.definition,
		run.context_policy_version, run.memory_rendering_version,
		run.recall_run_id, run.sessionization_policy_version_id
		FROM generation_runs run
		JOIN pipeline_versions pipeline ON pipeline.pipeline_version_id = run.pipeline_version_id
		WHERE run.generation_run_id = ?`, runID.String()).Scan(
		&residentRaw, &purpose, &obligation, &provider, &model,
		&promptVersion, &pipelineRaw, &pipelineKind, &pipelineVersion, &pipelineDefinition,
		&contextVersion, &renderingVersion, &recallRaw, &sessionRaw,
	); err != nil {
		return fmt.Errorf("sqlite: validate source dialogue obligation: %w", err)
	}
	if residentRaw != residentID.String() || domain.GenerationPurpose(purpose).Effective() != domain.GenerationPurposeDialogue ||
		obligation != domain.DialogueObligation(sourceEventID) || provider != "mahoroba-internal" || model != "not-dispatched" ||
		recallRaw.Valid || !sessionRaw.Valid {
		return errors.New("sqlite: source dialogue obligation has an invalid synthetic envelope")
	}
	pipelineID, err := canonical.ParseID(pipelineRaw)
	if err != nil {
		return fmt.Errorf("sqlite: parse source dialogue pipeline: %w", err)
	}
	definition, err := canonical.ParseCanonicalJSON([]byte(pipelineDefinition))
	if err != nil || pipelineKind != "dialogue" {
		return errors.New("sqlite: source dialogue pipeline definition is invalid")
	}
	if err := domain.ValidateExactDialoguePipelineDefinition(domain.PipelineVersionDefinition{
		ID: pipelineID, Kind: pipelineKind, VersionKey: pipelineVersion, Definition: definition,
	}); err != nil {
		return fmt.Errorf("sqlite: source dialogue pipeline definition: %w", err)
	}
	if _, err := domain.ClassifyPersistedDialogueExecutionContract(domain.DialogueExecutionContract{
		PipelineVersionKey: pipelineVersion, PromptTemplateVersion: promptVersion,
		ContextPolicyVersion: contextVersion, MemoryRenderingVersion: renderingVersion,
	}, domain.DialogueEnvelopeSyntheticNoDispatch); err != nil {
		return fmt.Errorf("sqlite: source dialogue synthetic version contract: %w", err)
	}
	var sourceResident, sourceType string
	var sourceCommit int64
	if err := u.tx.QueryRowContext(ctx, `SELECT event.resident_id, event.event_type,
		commit_row.commit_seq
		FROM events event JOIN canonical_commits commit_row
		  ON commit_row.canonical_commit_id = event.canonical_commit_id
		WHERE event.event_id = ?`, sourceEventID.String()).Scan(&sourceResident, &sourceType, &sourceCommit); err != nil {
		return fmt.Errorf("sqlite: resolve source dialogue event: %w", err)
	}
	if sourceResident != residentID.String() || sourceType != "user_message" {
		return errors.New("sqlite: source dialogue event is not a resident user message")
	}
	expectedPipelineID, expectedVersion, resolvable, err := resolveCancellationDialoguePipeline(ctx, u.tx, sourceCommit)
	if err != nil {
		return err
	}
	if !resolvable || expectedPipelineID != pipelineID || expectedVersion != pipelineVersion {
		return errors.New("sqlite: source dialogue synthetic pipeline is not source-time contract")
	}
	contract := domain.DialogueExecutionContract{
		PipelineVersionKey: pipelineVersion, PromptTemplateVersion: promptVersion,
		ContextPolicyVersion: contextVersion, MemoryRenderingVersion: renderingVersion,
	}
	if err := domain.ValidateSyntheticDialogueExecutionContract(contract, expectedVersion); err != nil {
		return fmt.Errorf("sqlite: source dialogue synthetic contract: %w", err)
	}
	var attempt int64
	var state string
	if err := u.tx.QueryRowContext(ctx, `SELECT attempt_no, state FROM generation_run_outcomes
		WHERE generation_run_id = ? ORDER BY attempt_no DESC, recorded_at DESC LIMIT 1`, runID.String()).Scan(&attempt, &state); err != nil {
		return fmt.Errorf("sqlite: validate source dialogue outcome: %w", err)
	}
	if attempt != 0 || state != "cancelled" {
		return errors.New("sqlite: source dialogue obligation is not synthetic cancelled")
	}
	var inputCount, usageCount int64
	if err := u.tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM generation_run_inputs WHERE generation_run_id = ?`, runID.String()).Scan(&inputCount); err != nil {
		return fmt.Errorf("sqlite: validate source dialogue inputs: %w", err)
	}
	if err := u.tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM claim_usages
		WHERE generation_run_id = ? OR detected_by_run_id = ?`, runID.String(), runID.String()).Scan(&usageCount); err != nil {
		return fmt.Errorf("sqlite: validate source dialogue Recall usages: %w", err)
	}
	if inputCount != 0 || usageCount != 0 {
		return errors.New("sqlite: synthetic source dialogue contains dispatch or Recall provenance")
	}
	return nil
}

func (u *canonicalUoW) CancelDialogue(ctx context.Context, value domain.CancelDialogue) (domain.DialogueCancellationResult, error) {
	envelope := value.Generation
	if err := u.requireResidentScope(envelope.ResidentID); err != nil {
		return domain.DialogueCancellationResult{}, err
	}
	code, err := generation.ParseOutcomeErrorCode(value.ErrorClass)
	if err != nil {
		return domain.DialogueCancellationResult{}, fmt.Errorf("sqlite: invalid dialogue cancellation code: %w", err)
	}
	if code.Class() != generation.ErrorSourceContentErased && code.Class() != generation.ErrorResidentInactive &&
		code.Class() != generation.ErrorResidentUnselected {
		return domain.DialogueCancellationResult{}, fmt.Errorf("sqlite: error class %q is not a dialogue cancellation reason", code)
	}
	attemptBound := value.ExpectedRunID != nil || value.ExpectedAttemptNo != nil
	if (value.ExpectedRunID == nil) != (value.ExpectedAttemptNo == nil) {
		return domain.DialogueCancellationResult{}, errors.New("sqlite: dialogue cancellation run and attempt expectations must be supplied together")
	}
	if attemptBound {
		if code.Class() != generation.ErrorSourceContentErased {
			return domain.DialogueCancellationResult{}, errors.New("sqlite: only erased-source dialogue cancellation may be attempt-bound")
		}
		if *value.ExpectedAttemptNo < 1 {
			return domain.DialogueCancellationResult{}, errors.New("sqlite: expected dialogue cancellation attempt must be positive")
		}
	}

	var eventType, erasureState string
	if err := u.tx.QueryRowContext(ctx, `SELECT e.event_type, co.erasure_state
		FROM events e JOIN content_objects co ON co.content_id = e.content_id
		WHERE e.event_id = ? AND e.resident_id = ?`, value.SourceEventID.String(), envelope.ResidentID.String()).Scan(&eventType, &erasureState); err != nil {
		return domain.DialogueCancellationResult{}, fmt.Errorf("sqlite: resolve dialogue cancellation source: %w", err)
	}
	if eventType != "user_message" {
		return domain.DialogueCancellationResult{}, errors.New("sqlite: dialogue cancellation source is not a user message")
	}
	status, err := u.currentStatus(ctx, envelope.ResidentID)
	if err != nil {
		return domain.DialogueCancellationResult{}, err
	}
	var selected int
	if status == "active" {
		if err := u.tx.QueryRowContext(ctx, `SELECT EXISTS(
			SELECT 1 FROM runtime_config WHERE singleton_id = 1 AND active_resident_id = ?
		)`, envelope.ResidentID.String()).Scan(&selected); err != nil {
			return domain.DialogueCancellationResult{}, fmt.Errorf("sqlite: resolve operational resident selection: %w", err)
		}
	}
	if code.Class() == generation.ErrorSourceContentErased && erasureState != "erased" {
		if !attemptBound {
			return domain.DialogueCancellationResult{}, errors.New("sqlite: source content is not erased")
		}
		ineligibleClaim, claimErr := u.dialogueRunHasIneligibleClaimSource(
			ctx, *value.ExpectedRunID, envelope.ResidentID,
		)
		if claimErr != nil {
			return domain.DialogueCancellationResult{}, claimErr
		}
		if !ineligibleClaim {
			return domain.DialogueCancellationResult{}, errors.New("sqlite: neither dialogue source content nor a frozen claim source is erased")
		}
	}
	if code.Class() == generation.ErrorResidentInactive && status == "active" {
		return domain.DialogueCancellationResult{}, errors.New("sqlite: resident is still active")
	}
	if code.Class() == generation.ErrorResidentUnselected {
		if status != "active" {
			return domain.DialogueCancellationResult{}, errors.New("sqlite: unselected cancellation requires an active resident")
		}
		if selected != 0 {
			return domain.DialogueCancellationResult{}, errors.New("sqlite: resident is still operationally selected")
		}
	}
	if !attemptBound {
		expectedCode := dialogueRecoveryReason(erasureState == "erased", status, selected != 0)
		if expectedCode == "" || code.String() != expectedCode {
			return domain.DialogueCancellationResult{}, fmt.Errorf(
				"sqlite: dialogue cancellation reason %q conflicts with priority-selected reason %q",
				code.String(), expectedCode,
			)
		}
	}

	var runRaw string
	err = u.tx.QueryRowContext(ctx, `SELECT generation_run_id FROM generation_runs
		WHERE resident_id = ? AND idempotency_key = ?`, envelope.ResidentID.String(), envelope.IdempotencyKey).Scan(&runRaw)
	if errors.Is(err, sql.ErrNoRows) {
		if attemptBound {
			return domain.DialogueCancellationResult{}, errors.New("sqlite: attempt-bound dialogue cancellation has no existing generation run")
		}
		expected, resolvable, resolveErr := resolveSyntheticCancellationEnvelope(ctx, u.tx, domain.MandatoryRecoveryWork{
			Kind:           domain.MandatoryRecoveryDialogue,
			SourceEvent:    domain.Event{ID: value.SourceEventID, ResidentID: envelope.ResidentID},
			IdempotencyKey: envelope.IdempotencyKey, CancellationCode: code.String(),
		})
		if resolveErr != nil {
			return domain.DialogueCancellationResult{}, resolveErr
		}
		if !resolvable || !cancellationEnvelopeSemanticallyEqual(envelope, expected) {
			return domain.DialogueCancellationResult{}, errors.New("sqlite: dialogue cancellation envelope is not the event-time recovery envelope")
		}
		if err := u.insertGenerationRun(ctx, envelope); err != nil {
			return domain.DialogueCancellationResult{}, err
		}
		if err := u.insertOutcome(ctx, value.CancelledOutcomeID, envelope.RunID, 0, "cancelled", nil, nil, nil, 0, code.String()); err != nil {
			return domain.DialogueCancellationResult{}, err
		}
		return domain.DialogueCancellationResult{RunID: envelope.RunID, AttemptNo: 0}, nil
	}
	if err != nil {
		return domain.DialogueCancellationResult{}, fmt.Errorf("sqlite: resolve dialogue generation run: %w", err)
	}
	runID, err := canonical.ParseID(runRaw)
	if err != nil {
		return domain.DialogueCancellationResult{}, err
	}
	if attemptBound && runID != *value.ExpectedRunID {
		return domain.DialogueCancellationResult{}, fmt.Errorf(
			"sqlite: erased-source cancellation run %s conflicts with expected run %s", runID, *value.ExpectedRunID,
		)
	}
	latestAttempt, state, latestError, err := u.latestOutcomeWithError(ctx, runID, envelope.ResidentID)
	if err != nil {
		return domain.DialogueCancellationResult{}, err
	}
	if attemptBound && latestAttempt != *value.ExpectedAttemptNo {
		return domain.DialogueCancellationResult{}, fmt.Errorf(
			"sqlite: erased-source cancellation attempt %d conflicts with latest attempt %d",
			*value.ExpectedAttemptNo, latestAttempt,
		)
	}
	if attemptBound {
		switch state {
		case "running":
			if err := u.insertOutcome(ctx, value.CancelledOutcomeID, runID, latestAttempt, "cancelled", nil, nil, nil, 0, code.String()); err != nil {
				return domain.DialogueCancellationResult{}, err
			}
			return domain.DialogueCancellationResult{RunID: runID, AttemptNo: latestAttempt}, nil
		case "cancelled":
			previousCode, parseErr := generation.ParseOutcomeErrorCode(latestError)
			if parseErr != nil {
				return domain.DialogueCancellationResult{}, fmt.Errorf("sqlite: parse latest generation error class: %w", parseErr)
			}
			if previousCode.String() == code.String() {
				return domain.DialogueCancellationResult{RunID: runID, AttemptNo: latestAttempt}, canonical.ErrNoMutation
			}
			return domain.DialogueCancellationResult{}, errors.New("sqlite: erased-source cancellation conflicts with existing dialogue cancellation")
		case "failed", "succeeded":
			return domain.DialogueCancellationResult{}, fmt.Errorf(
				"sqlite: erased-source cancellation conflicts with existing dialogue terminal state %q", state,
			)
		default:
			return domain.DialogueCancellationResult{}, fmt.Errorf("sqlite: unknown generation outcome state %q", state)
		}
	}
	switch state {
	case "succeeded":
		return domain.DialogueCancellationResult{RunID: runID, AttemptNo: latestAttempt}, nil
	case "running":
		if err := u.insertOutcome(ctx, value.CancelledOutcomeID, runID, latestAttempt, "cancelled", nil, nil, nil, 0, code.String()); err != nil {
			return domain.DialogueCancellationResult{}, err
		}
		return domain.DialogueCancellationResult{RunID: runID, AttemptNo: latestAttempt}, nil
	case "failed", "cancelled":
		previousCode, err := generation.ParseOutcomeErrorCode(latestError)
		if err != nil {
			return domain.DialogueCancellationResult{}, fmt.Errorf("sqlite: parse latest generation error class: %w", err)
		}
		if !previousCode.Retryable() {
			return domain.DialogueCancellationResult{RunID: runID, AttemptNo: latestAttempt}, nil
		}
		if latestAttempt == math.MaxInt64 {
			return domain.DialogueCancellationResult{}, &domain.RecoveryAttemptOverflowError{RunID: runID}
		}
		nextAttempt := latestAttempt + 1
		if err := u.insertOutcome(ctx, envelope.RunningOutcomeID, runID, nextAttempt, "running", nil, nil, nil, 0, ""); err != nil {
			return domain.DialogueCancellationResult{}, err
		}
		if err := u.insertOutcome(ctx, value.CancelledOutcomeID, runID, nextAttempt, "cancelled", nil, nil, nil, 0, code.String()); err != nil {
			return domain.DialogueCancellationResult{}, err
		}
		return domain.DialogueCancellationResult{RunID: runID, AttemptNo: nextAttempt}, nil
	default:
		return domain.DialogueCancellationResult{}, fmt.Errorf("sqlite: unknown generation outcome state %q", state)
	}
}

func (u *canonicalUoW) dialogueRunHasIneligibleClaimSource(
	ctx context.Context,
	runID canonical.ID,
	residentID canonical.ID,
) (bool, error) {
	rows, err := u.tx.QueryContext(ctx, `SELECT input.source_id
		FROM generation_run_inputs input
		JOIN generation_runs run ON run.generation_run_id = input.generation_run_id
		WHERE input.generation_run_id = ? AND run.resident_id = ? AND input.source_type = 'claim'
		ORDER BY input.ordinal, input.generation_run_input_id`, runID.String(), residentID.String())
	if err != nil {
		return false, fmt.Errorf("sqlite: resolve frozen dialogue claim sources: %w", err)
	}
	defer rows.Close()
	ineligible := false
	for rows.Next() {
		var claimRaw string
		if err := rows.Scan(&claimRaw); err != nil {
			return false, fmt.Errorf("sqlite: scan frozen dialogue claim source: %w", err)
		}
		claimID, err := canonical.ParseID(claimRaw)
		if err != nil {
			return false, fmt.Errorf("sqlite: parse frozen dialogue claim source: %w", err)
		}
		if _, err := loadEligibleClaimStatement(ctx, u.tx, residentID, claimID); err != nil {
			if errors.Is(err, domain.ErrClaimSourceIneligible) {
				ineligible = true
				continue
			}
			return false, fmt.Errorf("sqlite: verify frozen dialogue claim source: %w", err)
		}
	}
	if err := rows.Err(); err != nil {
		return false, fmt.Errorf("sqlite: iterate frozen dialogue claim sources: %w", err)
	}
	return ineligible, nil
}

func (u *canonicalUoW) insertGenerationRun(ctx context.Context, value domain.PrepareGeneration) error {
	m := u.metadata
	params, _, err := domain.ParseGeneratorParams(value.GeneratorParams.Bytes())
	if err != nil {
		return err
	}
	if err := domain.ValidateNewGeneratorParamsForPurpose(value.Purpose, params); err != nil {
		return err
	}
	versionContract := domain.GenerationVersionContract{
		PromptTemplateVersion: value.PromptTemplateVersion, ContextPolicyVersion: value.ContextPolicyVersion,
		MemoryRenderingVersion: value.MemoryRenderingVersion,
	}
	if value.Purpose.Effective() == domain.GenerationPurposeDialogue {
		if len(value.Inputs) != 0 || value.RecallRunID != nil {
			return errors.New("sqlite: normal dialogue generation requires the dedicated PrepareDialogue writer")
		}
		var pipelineKey, definitionRaw string
		if err := u.tx.QueryRowContext(ctx, `SELECT version_key, definition
			FROM pipeline_versions
			WHERE pipeline_version_id = ? AND pipeline_kind = 'dialogue'`,
			value.PipelineVersionID.String(),
		).Scan(&pipelineKey, &definitionRaw); err != nil {
			return fmt.Errorf("sqlite: resolve synthetic dialogue pipeline: %w", err)
		}
		definition, err := canonical.ParseCanonicalJSON([]byte(definitionRaw))
		if err != nil {
			return err
		}
		if err := domain.ValidateExactDialoguePipelineDefinition(domain.PipelineVersionDefinition{
			ID: value.PipelineVersionID, Kind: "dialogue", VersionKey: pipelineKey, Definition: definition,
		}); err != nil {
			return err
		}
		if err := domain.ValidateSyntheticDialogueExecutionContract(domain.DialogueExecutionContract{
			PipelineVersionKey: pipelineKey, PromptTemplateVersion: versionContract.PromptTemplateVersion,
			ContextPolicyVersion:   versionContract.ContextPolicyVersion,
			MemoryRenderingVersion: versionContract.MemoryRenderingVersion,
		}, pipelineKey); err != nil {
			return err
		}
	} else if err := domain.ValidateGenerationVersions(value.Purpose, versionContract); err != nil {
		return err
	}
	var sessionPolicyID any
	if value.SessionPolicyID != nil {
		sessionPolicyID = value.SessionPolicyID.String()
	}
	var recallRunID any
	if value.RecallRunID != nil {
		recallRunID = value.RecallRunID.String()
	}
	if _, err := u.tx.ExecContext(ctx, `INSERT INTO generation_runs(
		generation_run_id, canonical_commit_id, resident_id, purpose, idempotency_key,
		provider, model, model_version, prompt_template_version, pipeline_version_id,
		context_policy_version, sessionization_policy_version_id, memory_rendering_version,
		principles_revision_id, persona_revision_id, memory_policy_revision_id, recall_run_id,
		temperature, top_p, max_tokens, seed, generator_params, as_of, as_of_tz,
		budget_exceeded, dropped_input_summary, requested_at, requested_tz
	) VALUES (?, ?, ?, ?, ?, ?, ?, NULL, ?, ?, ?, ?, ?, ?, ?, ?, ?,
		NULL, NULL, NULL, NULL, ?, ?, ?, ?, ?, ?, ?)`,
		value.RunID.String(), m.CommitID.String(), value.ResidentID.String(), string(value.Purpose.Effective()), value.IdempotencyKey,
		value.Provider, value.Model, value.PromptTemplateVersion, value.PipelineVersionID.String(),
		value.ContextPolicyVersion, sessionPolicyID, value.MemoryRenderingVersion,
		value.PrinciplesRevisionID.String(), value.PersonaRevisionID.String(), value.MemoryPolicyRevisionID.String(), recallRunID,
		value.GeneratorParams.String(), value.AsOf.UnixMicro(), value.AsOfTZ.String(), boolInt(value.BudgetExceeded),
		value.DroppedInputSummary.String(), m.CommittedAt.UnixMicro(), m.CommittedTZ.String()); err != nil {
		return fmt.Errorf("insert generation run: %w", err)
	}
	return nil
}

func (u *canonicalUoW) StartAttempt(ctx context.Context, value domain.Attempt) error {
	if err := u.requireResidentScope(value.ResidentID); err != nil {
		return err
	}
	purpose, err := u.generationRunPurpose(ctx, value.RunID, value.ResidentID)
	if err != nil {
		return err
	}
	latestAttempt, state, err := u.latestOutcome(ctx, value.RunID, value.ResidentID)
	if err != nil {
		return err
	}
	if latestAttempt == 0 {
		return errors.New("sqlite: synthetic cancellation cannot be retried")
	}
	if purpose == domain.GenerationPurposeDialogue {
		if err := validateDialogueRetryExecutionContract(ctx, u.tx, value.RunID); err != nil {
			return err
		}
	}
	if err := u.requireActiveResident(ctx, value.ResidentID); err != nil {
		return err
	}
	if purpose == domain.GenerationPurposeDialogue ||
		purpose == domain.GenerationPurposeSelfTalk || purpose == domain.GenerationPurposeOutboundInitiative {
		if err := u.requireOperationallySelectedResident(ctx, value.ResidentID); err != nil {
			return err
		}
	}
	if (state != "failed" && state != "cancelled") || value.AttemptNo != latestAttempt+1 {
		return fmt.Errorf("sqlite: retry must follow failed attempt %d; got state=%s attempt=%d", latestAttempt, state, value.AttemptNo)
	}
	if purpose == domain.GenerationPurposeSelfTalk || purpose == domain.GenerationPurposeOutboundInitiative {
		if err := u.validateAutonomousRetryStart(
			ctx, value.RunID, value.AttemptNo, value.MaxAttempts, purpose,
		); err != nil {
			return err
		}
	}
	return u.insertOutcome(ctx, value.OutcomeID, value.RunID, value.AttemptNo, "running", nil, nil, nil, 0, "")
}

type dialogueRetryContractQueryer interface {
	QueryRowContext(context.Context, string, ...any) *sql.Row
}

func validateDialogueRetryExecutionContract(
	ctx context.Context,
	q dialogueRetryContractQueryer,
	runID canonical.ID,
) error {
	_, err := classifyNormalDialogueExecutionContract(ctx, q, runID)
	return err
}

func classifyNormalDialogueExecutionContract(
	ctx context.Context,
	q dialogueRetryContractQueryer,
	runID canonical.ID,
) (domain.DialogueExecutionClass, error) {
	var pipelineRaw, pipelineKind, pipelineVersion, definitionRaw string
	var promptVersion, contextVersion, renderingVersion string
	if err := q.QueryRowContext(ctx, `SELECT pipeline.pipeline_version_id,
		pipeline.pipeline_kind, pipeline.version_key, pipeline.definition,
		run.prompt_template_version, run.context_policy_version,
		run.memory_rendering_version
		FROM generation_runs run
		JOIN pipeline_versions pipeline
		  ON pipeline.pipeline_version_id = run.pipeline_version_id
		WHERE run.generation_run_id = ? AND run.purpose = 'dialogue'`,
		runID.String(),
	).Scan(&pipelineRaw, &pipelineKind, &pipelineVersion, &definitionRaw,
		&promptVersion, &contextVersion, &renderingVersion); err != nil {
		return "", fmt.Errorf("sqlite: resolve dialogue retry execution contract: %w", err)
	}
	pipelineID, err := canonical.ParseID(pipelineRaw)
	if err != nil {
		return "", fmt.Errorf("sqlite: parse dialogue retry pipeline: %w", err)
	}
	definition, err := canonical.ParseCanonicalJSON([]byte(definitionRaw))
	if err != nil {
		return "", fmt.Errorf("sqlite: parse dialogue retry pipeline definition: %w", err)
	}
	if pipelineKind != "dialogue" {
		return "", fmt.Errorf("sqlite: dialogue retry references pipeline kind %q", pipelineKind)
	}
	if err := domain.ValidateExactDialoguePipelineDefinition(domain.PipelineVersionDefinition{
		ID: pipelineID, Kind: pipelineKind, VersionKey: pipelineVersion, Definition: definition,
	}); err != nil {
		return "", fmt.Errorf("sqlite: dialogue retry pipeline definition: %w", err)
	}
	executionClass, err := domain.ClassifyPersistedDialogueExecutionContract(domain.DialogueExecutionContract{
		PipelineVersionKey: pipelineVersion, PromptTemplateVersion: promptVersion,
		ContextPolicyVersion: contextVersion, MemoryRenderingVersion: renderingVersion,
	}, domain.DialogueEnvelopeNormal)
	if err != nil {
		return "", fmt.Errorf("sqlite: dialogue retry generation version contract mismatch: %w", err)
	}
	return executionClass, nil
}

func (u *canonicalUoW) requireGenerationRevisions(
	ctx context.Context,
	value domain.PrepareGeneration,
	purpose domain.GenerationPurpose,
) error {
	revisions := []struct {
		id    canonical.ID
		class string
	}{
		{value.PrinciplesRevisionID, "principles"},
		{value.PersonaRevisionID, "persona"},
		{value.MemoryPolicyRevisionID, "memory_policy"},
	}
	switch purpose {
	case domain.GenerationPurposeDialogue:
		for _, revision := range revisions {
			if err := u.requireActiveRevision(ctx, value.ResidentID, revision.id, revision.class); err != nil {
				return err
			}
		}
		return nil
	case domain.GenerationPurposeMemoryExtraction:
		if value.SessionPolicyID != nil {
			return errors.New("sqlite: memory extraction must not pin a sessionization policy")
		}
		for _, revision := range revisions[:2] {
			if err := u.requireActiveRevision(ctx, value.ResidentID, revision.id, revision.class); err != nil {
				return err
			}
		}
		return u.requireResidentRevision(ctx, value.ResidentID, value.MemoryPolicyRevisionID, "memory_policy")
	default:
		// Other registered background purposes are not dispatch-enabled yet, but
		// their immutable envelope must still be resident/class safe.
		for _, revision := range revisions {
			if err := u.requireResidentRevision(ctx, value.ResidentID, revision.id, revision.class); err != nil {
				return err
			}
		}
		return nil
	}
}

func (u *canonicalUoW) requireResidentRevision(
	ctx context.Context,
	residentID, revisionID canonical.ID,
	class string,
) error {
	var actualResident, actualClass string
	if err := u.tx.QueryRowContext(ctx, `SELECT resident_id, revision_class
		FROM resident_revisions WHERE revision_id = ?`, revisionID.String()).Scan(&actualResident, &actualClass); err != nil {
		return fmt.Errorf("sqlite: resolve %s revision: %w", class, err)
	}
	if actualResident != residentID.String() || actualClass != class {
		return fmt.Errorf("sqlite: generation references wrong-resident or wrong-class %s revision", class)
	}
	return nil
}

func (u *canonicalUoW) generationRunPurpose(
	ctx context.Context,
	runID, residentID canonical.ID,
) (domain.GenerationPurpose, error) {
	var purposeRaw, paramsRaw string
	if err := u.tx.QueryRowContext(ctx, `SELECT purpose, generator_params FROM generation_runs
		WHERE generation_run_id = ? AND resident_id = ?`, runID.String(), residentID.String()).Scan(&purposeRaw, &paramsRaw); err != nil {
		return "", fmt.Errorf("sqlite: resolve generation purpose: %w", err)
	}
	purpose := domain.GenerationPurpose(purposeRaw)
	params, _, err := domain.ParseGeneratorParams([]byte(paramsRaw))
	if err != nil {
		return "", fmt.Errorf("sqlite: resolve generation parameters: %w", err)
	}
	if err := domain.ValidateGeneratorParamsForPurpose(purpose, params); err != nil {
		return "", fmt.Errorf("sqlite: generation purpose/parameters contract: %w", err)
	}
	return purpose.Effective(), nil
}

func (u *canonicalUoW) FailAttempt(ctx context.Context, value domain.FailAttempt) error {
	if err := u.requireResidentScope(value.ResidentID); err != nil {
		return err
	}
	latestAttempt, state, previousError, err := u.latestOutcomeWithError(ctx, value.RunID, value.ResidentID)
	if err != nil {
		return err
	}
	if state == "cancelled" && value.State == "cancelled" && value.AttemptNo == latestAttempt {
		code, codeErr := generation.ParseOutcomeErrorCode(value.ErrorClass)
		if codeErr == nil && code.Class() == generation.ErrorSourceContentErased {
			if previousError == code.String() {
				return canonical.ErrNoMutation
			}
			return errors.New("sqlite: erased-source cancellation conflicts with existing terminal outcome")
		}
	}
	if state != "running" || value.AttemptNo != latestAttempt {
		return fmt.Errorf("sqlite: failure must terminate latest running attempt %d; got state=%s attempt=%d", latestAttempt, state, value.AttemptNo)
	}
	return u.insertOutcome(ctx, value.OutcomeID, value.RunID, value.AttemptNo, value.State, nil, nil, nil, 0, value.ErrorClass)
}

func (u *canonicalUoW) RejectGenerationEnvelope(ctx context.Context, value domain.RejectGenerationEnvelope) (domain.GenerationRejectionResult, error) {
	if err := u.requireResidentScope(value.ResidentID); err != nil {
		return domain.GenerationRejectionResult{}, err
	}
	code, err := generation.ParseOutcomeErrorCode(value.ErrorClass)
	if err != nil || code.Class() != generation.ErrorProviderUnsupported || code.Retryable() {
		return domain.GenerationRejectionResult{}, errors.New("sqlite: invalid generation envelope rejection class")
	}
	latestAttempt, state, previousError, err := u.latestOutcomeWithError(ctx, value.RunID, value.ResidentID)
	if err != nil {
		return domain.GenerationRejectionResult{}, err
	}
	result := domain.GenerationRejectionResult{AttemptNo: latestAttempt}
	switch state {
	case "running":
		if err := u.insertOutcome(ctx, value.RejectedOutcomeID, value.RunID, latestAttempt, "failed", nil, nil, nil, 0, code.String()); err != nil {
			return domain.GenerationRejectionResult{}, err
		}
		return result, nil
	case "failed", "cancelled":
		previousCode, err := generation.ParseOutcomeErrorCode(previousError)
		if err != nil {
			return domain.GenerationRejectionResult{}, fmt.Errorf("sqlite: parse latest generation error class: %w", err)
		}
		if !previousCode.Retryable() {
			if state == "failed" && previousCode.String() == code.String() {
				return result, canonical.ErrNoMutation
			}
			return domain.GenerationRejectionResult{}, fmt.Errorf(
				"sqlite: generation envelope rejection conflicts with existing terminal outcome state=%q error_class=%q",
				state, previousCode,
			)
		}
		if latestAttempt == math.MaxInt64 {
			return domain.GenerationRejectionResult{}, errors.New("sqlite: generation attempt overflow")
		}
		result.AttemptNo++
		if err := u.insertOutcome(ctx, value.RunningOutcomeID, value.RunID, result.AttemptNo, "running", nil, nil, nil, 0, ""); err != nil {
			return domain.GenerationRejectionResult{}, err
		}
		if err := u.insertOutcome(ctx, value.RejectedOutcomeID, value.RunID, result.AttemptNo, "failed", nil, nil, nil, 0, code.String()); err != nil {
			return domain.GenerationRejectionResult{}, err
		}
		return result, nil
	case "succeeded":
		return domain.GenerationRejectionResult{}, errors.New(
			"sqlite: generation envelope rejection conflicts with existing succeeded outcome",
		)
	default:
		return domain.GenerationRejectionResult{}, fmt.Errorf("sqlite: unknown generation outcome state %q", state)
	}
}

func (u *canonicalUoW) LandDialogue(ctx context.Context, value domain.LandDialogue) (domain.Event, error) {
	if err := u.requireResidentScope(value.ResidentID); err != nil {
		return domain.Event{}, err
	}
	purpose, err := u.generationRunPurpose(ctx, value.RunID, value.ResidentID)
	if err != nil {
		return domain.Event{}, err
	}
	if purpose != domain.GenerationPurposeDialogue {
		return domain.Event{}, errors.New("sqlite: generic dialogue landing rejects non-dialogue generation purpose")
	}
	if err := u.requireActiveResident(ctx, value.ResidentID); err != nil {
		return domain.Event{}, err
	}
	if err := u.requireOperationallySelectedResident(ctx, value.ResidentID); err != nil {
		return domain.Event{}, err
	}
	latestAttempt, state, err := u.latestOutcome(ctx, value.RunID, value.ResidentID)
	if err != nil {
		return domain.Event{}, err
	}
	if state != "running" || value.AttemptNo != latestAttempt {
		return domain.Event{}, fmt.Errorf("sqlite: landing must terminate latest running attempt %d; got state=%s attempt=%d", latestAttempt, state, value.AttemptNo)
	}
	if err := u.revalidateDialogueInputsAtLanding(ctx, value.RunID, value.ResidentID); err != nil {
		return domain.Event{}, err
	}
	if err := u.requirePrincipal(ctx, value.OwnerPrincipalID, "human"); err != nil {
		return domain.Event{}, err
	}
	if err := u.requireResidentPrincipal(ctx, value.ResidentID, value.ResidentPrincipalID); err != nil {
		return domain.Event{}, err
	}
	if err := u.insertContent(ctx, value.Output); err != nil {
		return domain.Event{}, err
	}
	if err := u.insertOutcome(ctx, value.OutcomeID, value.RunID, value.AttemptNo, "succeeded", &value.Output.ID,
		value.PromptTokens, value.CompletionTokens, value.LatencyMicros, ""); err != nil {
		return domain.Event{}, err
	}
	runID := value.RunID
	target := value.OwnerPrincipalID
	event, err := u.makeEvent(ctx, eventSpec{
		ID: value.EventID, ResidentID: value.ResidentID, Type: "resident_message",
		Visibility: "conversation", DeliveryScreen: true, DeliveryAudio: false,
		Ingress: "resident_runtime", ActorPrincipalID: value.ResidentPrincipalID,
		TargetPrincipalID: &target, GenerationRunID: &runID,
		OccurredAt: value.OccurredAt, OccurredTZ: value.OccurredTZ, Content: value.Output,
	})
	if err != nil {
		return domain.Event{}, err
	}
	if err := u.insertEvent(ctx, event, "conversation", true, false, "resident_runtime"); err != nil {
		return domain.Event{}, err
	}
	return event, nil
}

// revalidateDialogueInputsAtLanding closes the prepare/land TOCTOU window for
// explicit Canonical sources. The frozen generation_input bytes and the live
// source are checked in this same write transaction.
func (u *canonicalUoW) revalidateDialogueInputsAtLanding(ctx context.Context, runID, residentID canonical.ID) error {
	executionClass, err := classifyNormalDialogueExecutionContract(ctx, u.tx, runID)
	if err != nil {
		return fmt.Errorf("sqlite: validate dialogue landing execution contract: %w", err)
	}
	var purposeRaw, obligation, rendering, principlesRaw, personaRaw, policyRaw, runCommitRaw, asOfTZRaw string
	var recallRaw sql.NullString
	var asOf, runCommitSeq int64
	if err := u.tx.QueryRowContext(ctx, `SELECT run.purpose, run.idempotency_key, run.memory_rendering_version,
		run.principles_revision_id, run.persona_revision_id, run.memory_policy_revision_id,
		run.recall_run_id, run.as_of, run.as_of_tz,
		run.canonical_commit_id, commit_row.commit_seq
		FROM generation_runs run
		JOIN canonical_commits commit_row ON commit_row.canonical_commit_id = run.canonical_commit_id
		WHERE run.generation_run_id = ? AND run.resident_id = ?`,
		runID.String(), residentID.String()).Scan(
		&purposeRaw, &obligation, &rendering, &principlesRaw, &personaRaw, &policyRaw,
		&recallRaw, &asOf, &asOfTZRaw,
		&runCommitRaw, &runCommitSeq,
	); err != nil {
		return fmt.Errorf("sqlite: load dialogue landing envelope: %w", err)
	}
	policyID, err := canonical.ParseID(policyRaw)
	if err != nil {
		return err
	}
	prepare := domain.PrepareGeneration{
		RunID: runID, ResidentID: residentID, Purpose: domain.GenerationPurpose(purposeRaw),
		IdempotencyKey:         obligation,
		MemoryRenderingVersion: rendering, MemoryPolicyRevisionID: policyID,
		AsOf: canonical.Instant(asOf),
	}
	prepare.PrinciplesRevisionID, err = canonical.ParseID(principlesRaw)
	if err != nil {
		return fmt.Errorf("sqlite: parse dialogue landing principles revision: %w", err)
	}
	prepare.PersonaRevisionID, err = canonical.ParseID(personaRaw)
	if err != nil {
		return fmt.Errorf("sqlite: parse dialogue landing persona revision: %w", err)
	}
	prepare.AsOfTZ, err = canonical.ParseTimezone(asOfTZRaw)
	if err != nil {
		return fmt.Errorf("sqlite: parse dialogue landing as_of timezone: %w", err)
	}
	runCommitID, err := canonical.ParseID(runCommitRaw)
	if err != nil {
		return fmt.Errorf("sqlite: parse dialogue landing commit: %w", err)
	}
	if recallRaw.Valid {
		recallID, parseErr := canonical.ParseID(recallRaw.String)
		if parseErr != nil {
			return parseErr
		}
		prepare.RecallRunID = &recallID
	}
	var currentRecall *dialogueV3LandingRecall
	var currentPolicy *memory.Policy
	var currentSource *preparedDialogueEvent
	var currentHead canonical.CommitSeq
	if executionClass == domain.DialogueExecutionCurrentNormal || executionClass == domain.DialogueExecutionPriorBoundedNormal {
		if runCommitSeq <= 1 {
			return errors.New("sqlite: dialogue-v3 run has no pre-Commit-B source ceiling")
		}
		currentHead, err = canonical.NewCommitSeq(runCommitSeq - 1)
		if err != nil {
			return fmt.Errorf("sqlite: parse dialogue-v3 Landing source ceiling: %w", err)
		}
		var sourceRaw string
		if err := u.tx.QueryRowContext(ctx, `SELECT event.event_id FROM events event
			WHERE event.resident_id = ? AND ? = 'dialogue:v1:' || event.event_id`,
			residentID.String(), obligation,
		).Scan(&sourceRaw); err != nil {
			return fmt.Errorf("sqlite: resolve dialogue-v3 Landing source obligation: %w", err)
		}
		sourceID, parseErr := canonical.ParseID(sourceRaw)
		if parseErr != nil {
			return parseErr
		}
		loadedSource, loadErr := u.loadPreparedDialogueEvent(ctx, sourceID, residentID, currentHead)
		if loadErr != nil {
			// Landing source invalidation fails closed through the existing landing
			// failure/recovery path. Do not reinterpret a context-row mutation as a
			// claim-erasure cancellation contract.
			if errors.Is(loadErr, domain.ErrClaimSourceIneligible) {
				return errors.New("sqlite: dialogue-v3 Landing source content is unavailable")
			}
			return loadErr
		}
		currentSource = &loadedSource
		loadedPolicy, loadErr := u.loadPreparedDialogueMemoryPolicy(ctx, residentID, policyID)
		if loadErr != nil {
			return loadErr
		}
		currentPolicy = &loadedPolicy
	}
	if (executionClass == domain.DialogueExecutionCurrentNormal || executionClass == domain.DialogueExecutionPriorBoundedNormal) && prepare.RecallRunID != nil {
		loaded, loadErr := u.loadDialogueV3LandingRecall(
			ctx, prepare, runCommitID, runCommitSeq,
		)
		if loadErr != nil {
			return loadErr
		}
		currentRecall = &loaded
		currentPolicy = &loaded.policy
		// A successful Recall persists the exact Assembly Target head. Use that
		// stronger ceiling for every v3 event/revision source at Landing instead
		// of the merely safe pre-Commit-B upper bound.
		currentHead = loaded.head
	}
	rows, err := u.tx.QueryContext(ctx, `SELECT input.generation_run_input_id, input.ordinal, input.role,
		input.source_type, input.source_id, input.inclusion_mode, input.content_id, blob.content
		FROM generation_run_inputs input
		JOIN content_objects content ON content.content_id = input.content_id
		LEFT JOIN blobs blob ON blob.dedupe_scope_id = content.owner_resident_id
		 AND blob.hash_algorithm = content.blob_hash_algorithm AND blob.blob_hash = content.blob_hash
		WHERE input.generation_run_id = ? ORDER BY input.ordinal`, runID.String())
	if err != nil {
		return fmt.Errorf("sqlite: load dialogue landing inputs: %w", err)
	}
	defer rows.Close()
	currentInputs, runtimeInputs := 0, 0
	seenCurrentEvents := make(map[canonical.ID]struct{})
	seenCurrentRevisions := make(map[canonical.ID]int)
	for rows.Next() {
		var inputIDRaw, role, sourceType, inclusion, inputContentRaw string
		var ordinal int64
		var sourceRaw sql.NullString
		var content []byte
		if err := rows.Scan(
			&inputIDRaw, &ordinal, &role, &sourceType, &sourceRaw, &inclusion,
			&inputContentRaw, &content,
		); err != nil {
			return err
		}
		inputID, err := canonical.ParseID(inputIDRaw)
		if err != nil {
			return err
		}
		var sourceID *canonical.ID
		if sourceRaw.Valid {
			parsed, parseErr := canonical.ParseID(sourceRaw.String)
			if parseErr != nil {
				return parseErr
			}
			sourceID = &parsed
		}
		if content == nil {
			return fmt.Errorf("%w: dialogue input %s content is missing", errClaimContentIntegrity, inputID)
		}
		input := domain.GenerationInput{
			ID: inputID, Ordinal: ordinal, Role: role, SourceType: sourceType,
			SourceID: sourceID, InclusionMode: inclusion,
			Content: domain.Content{Bytes: content},
		}
		inputContentID, err := canonical.ParseID(inputContentRaw)
		if err != nil {
			return err
		}
		var validateErr error
		switch executionClass {
		case domain.DialogueExecutionLegacyNormal:
			validateErr = u.validateGenerationSource(ctx, prepare, input, true)
		case domain.DialogueExecutionPriorSplitNormal:
			validateErr = u.validateGenerationSource(ctx, prepare, input, false)
		case domain.DialogueExecutionCurrentNormal, domain.DialogueExecutionPriorBoundedNormal:
			if currentSource == nil || currentPolicy == nil {
				return errors.New("sqlite: dialogue-v3 Landing source envelope is incomplete")
			}
			if validateErr = u.validatePresentContentObject(
				ctx, inputContentID, residentID, input.Content.Bytes, "generation_input",
			); validateErr != nil {
				break
			}
			switch sourceType {
			case "claim":
				if currentRecall == nil {
					return errors.New("sqlite: current dialogue Recall input has no pinned Recall envelope")
				}
				validateErr = u.validateDialogueV3RecallInputAtLanding(ctx, *currentRecall, input)
			case "event":
				if input.SourceID == nil {
					validateErr = errors.New("sqlite: dialogue-v3 Landing event input has no source")
					break
				}
				if _, duplicate := seenCurrentEvents[*input.SourceID]; duplicate {
					validateErr = errors.New("sqlite: dialogue-v3 Landing repeats an event source")
					break
				}
				seenCurrentEvents[*input.SourceID] = struct{}{}
				event, loadErr := u.loadPreparedDialogueEvent(ctx, *input.SourceID, residentID, currentHead)
				if errors.Is(loadErr, domain.ErrClaimSourceIneligible) {
					validateErr = errors.New("sqlite: dialogue-v3 Landing event source content is unavailable")
				} else if loadErr != nil {
					validateErr = loadErr
				} else {
					validateErr = validatePreparedDialogueEventInput(input, event, *currentSource)
					if event.id == currentSource.id {
						currentInputs++
					}
				}
			case "resident_revision":
				validateErr = u.validatePreparedDialogueRevisionInput(ctx, input, prepare, currentHead)
				if validateErr == nil && input.SourceID != nil {
					seenCurrentRevisions[*input.SourceID]++
				}
			case "runtime_projection":
				validateErr = validatePreparedDialogueRuntimeInput(input, *currentPolicy)
				if validateErr == nil {
					runtimeInputs++
				}
			default:
				validateErr = fmt.Errorf("sqlite: unsupported dialogue-v3 Landing source type %q", sourceType)
			}
		default:
			return fmt.Errorf("sqlite: unsupported dialogue landing execution class %q", executionClass)
		}
		if validateErr != nil {
			return fmt.Errorf("sqlite: revalidate dialogue source at landing: %w", validateErr)
		}
	}
	if err := rows.Err(); err != nil {
		return err
	}
	if executionClass == domain.DialogueExecutionCurrentNormal || executionClass == domain.DialogueExecutionPriorBoundedNormal {
		if currentInputs != 1 || runtimeInputs != 1 || currentSource == nil {
			return errors.New("sqlite: dialogue-v3 Landing lacks exact current/runtime inputs")
		}
		for _, revisionID := range []canonical.ID{
			prepare.PrinciplesRevisionID, prepare.PersonaRevisionID, prepare.MemoryPolicyRevisionID,
		} {
			if seenCurrentRevisions[revisionID] != 1 {
				return errors.New("sqlite: dialogue-v3 Landing lacks exact pinned revision inputs")
			}
		}
	}
	return nil
}

type dialogueV3LandingRecall struct {
	runID, residentID, recallID, policyID canonical.ID
	head                                  canonical.CommitSeq
	asOf                                  canonical.Instant
	asOfTZ                                canonical.Timezone
	policy                                memory.Policy
}

// loadDialogueV3LandingRecall reconstructs the immutable Assembly Target from
// the persisted Recall envelope. The query and context JSON are regenerated
// from the pinned policy so a syntactically valid but non-exact target cannot
// select different rendering inputs at Landing.
func (u *canonicalUoW) loadDialogueV3LandingRecall(
	ctx context.Context,
	prepare domain.PrepareGeneration,
	runCommitID canonical.ID,
	runCommitSeq int64,
) (dialogueV3LandingRecall, error) {
	if prepare.RecallRunID == nil || prepare.MemoryRenderingVersion != domain.MemoryRenderingVersionV2 {
		return dialogueV3LandingRecall{}, errors.New("sqlite: dialogue-v3 Recall rendering envelope is incomplete")
	}
	var recallCommitRaw, residentRaw, queryContentRaw, policyRaw, timezoneRaw, queryRaw, constraintsRaw, pipelineRaw string
	var asOf int64
	if err := u.tx.QueryRowContext(ctx, `SELECT canonical_commit_id, resident_id, query_content_id,
		memory_policy_revision_id, as_of, as_of_tz, query_conditions,
		context_constraints, pipeline_version_id
		FROM recall_runs WHERE recall_run_id = ?`, prepare.RecallRunID.String()).Scan(
		&recallCommitRaw, &residentRaw, &queryContentRaw, &policyRaw, &asOf, &timezoneRaw,
		&queryRaw, &constraintsRaw, &pipelineRaw,
	); err != nil {
		return dialogueV3LandingRecall{}, fmt.Errorf("sqlite: load dialogue-v3 Recall landing envelope: %w", err)
	}
	if recallCommitRaw != runCommitID.String() || residentRaw != prepare.ResidentID.String() ||
		policyRaw != prepare.MemoryPolicyRevisionID.String() || canonical.Instant(asOf) != prepare.AsOf ||
		timezoneRaw != prepare.AsOfTZ.String() {
		return dialogueV3LandingRecall{}, errors.New("sqlite: dialogue-v3 Recall landing envelope differs from its generation run")
	}
	var sourceContentRaw string
	if err := u.tx.QueryRowContext(ctx, `SELECT event.content_id
		FROM events event
		WHERE event.resident_id = ? AND event.event_type = 'user_message'
		  AND ? = 'dialogue:v1:' || event.event_id`,
		prepare.ResidentID.String(), prepare.IdempotencyKey,
	).Scan(&sourceContentRaw); err != nil || queryContentRaw != sourceContentRaw {
		return dialogueV3LandingRecall{}, errors.New("sqlite: dialogue-v3 Recall query content differs from its source obligation")
	}
	policy, err := u.loadPreparedDialogueMemoryPolicy(
		ctx, prepare.ResidentID, prepare.MemoryPolicyRevisionID,
	)
	if err != nil {
		return dialogueV3LandingRecall{}, err
	}
	if (policy.Version != memory.PolicyVersionV4 && policy.Version != memory.PolicyVersionV5) || !policy.MemoryRecallEnabled ||
		policy.RenderingVersion != memory.RenderingVersionV2 {
		return dialogueV3LandingRecall{}, errors.New("sqlite: dialogue-v3 Recall landing requires the supported memory-rendering-v2 policy")
	}
	query, err := canonical.ParseCanonicalJSON([]byte(queryRaw))
	if err != nil {
		return dialogueV3LandingRecall{}, fmt.Errorf("sqlite: parse dialogue-v3 Recall query: %w", err)
	}
	constraints, err := canonical.ParseCanonicalJSON([]byte(constraintsRaw))
	if err != nil {
		return dialogueV3LandingRecall{}, fmt.Errorf("sqlite: parse dialogue-v3 Recall constraints: %w", err)
	}
	var parameters domain.RecallDecisionParameters
	if err := json.Unmarshal(query.Bytes(), &parameters); err != nil {
		return dialogueV3LandingRecall{}, fmt.Errorf("sqlite: decode dialogue-v3 Recall query: %w", err)
	}
	expectedQuery, expectedConstraints, err := domain.NewRecallDecisionJSON(
		parameters.ProjectionHead, policy, recallContextCompatibility(),
	)
	if err != nil || !bytes.Equal(query.Bytes(), expectedQuery.Bytes()) ||
		!bytes.Equal(constraints.Bytes(), expectedConstraints.Bytes()) {
		return dialogueV3LandingRecall{}, errors.New("sqlite: dialogue-v3 Recall query differs from its pinned Target and policy")
	}
	if parameters.ProjectionHead.Int64() >= runCommitSeq {
		return dialogueV3LandingRecall{}, errors.New("sqlite: dialogue-v3 Recall target does not precede Commit B")
	}
	var headCommittedAt int64
	if err := u.tx.QueryRowContext(ctx, `SELECT committed_at FROM canonical_commits
		WHERE commit_seq = ?`, parameters.ProjectionHead.Int64()).Scan(&headCommittedAt); err != nil {
		return dialogueV3LandingRecall{}, fmt.Errorf("sqlite: resolve dialogue-v3 Recall target head: %w", err)
	}
	if canonical.Instant(headCommittedAt) > prepare.AsOf {
		return dialogueV3LandingRecall{}, errors.New("sqlite: dialogue-v3 Recall as_of precedes its Target head")
	}
	if err := u.requireRevisionVisibleAtTarget(
		ctx, prepare.ResidentID, prepare.MemoryPolicyRevisionID,
		"memory_policy", parameters.ProjectionHead,
	); err != nil {
		return dialogueV3LandingRecall{}, err
	}
	expectedPipeline, fallback, err := u.loadRecallPipeline(ctx, parameters.ProjectionHead)
	if err != nil || fallback != "" || pipelineRaw != expectedPipeline.String() {
		return dialogueV3LandingRecall{}, errors.New("sqlite: dialogue-v3 Recall landing pipeline is not exact at its Target")
	}
	return dialogueV3LandingRecall{
		runID: prepare.RunID, residentID: prepare.ResidentID, recallID: *prepare.RecallRunID,
		policyID: prepare.MemoryPolicyRevisionID, head: parameters.ProjectionHead,
		asOf: prepare.AsOf, asOfTZ: prepare.AsOfTZ, policy: policy,
	}, nil
}

func (u *canonicalUoW) validateDialogueV3RecallInputAtLanding(
	ctx context.Context,
	envelope dialogueV3LandingRecall,
	input domain.GenerationInput,
) error {
	if input.SourceID == nil || input.SourceType != "claim" || input.Role != "system" ||
		input.InclusionMode != "memory_recall" {
		return errors.New("sqlite: dialogue-v3 Recall input provenance is invalid")
	}
	var promptUsages int64
	if err := u.tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM claim_usages
		WHERE generation_run_id = ? AND recall_run_id = ? AND claim_id = ?
		  AND usage_type = 'prompt_included'
		  AND memory_policy_revision_id = ? AND exclusion_reason IS NULL`,
		envelope.runID.String(), envelope.recallID.String(), input.SourceID.String(),
		envelope.policyID.String(),
	).Scan(&promptUsages); err != nil {
		return fmt.Errorf("sqlite: resolve dialogue-v3 Recall prompt usage: %w", err)
	}
	if promptUsages != 1 {
		return errors.New("sqlite: dialogue-v3 Recall input lacks exact prompt-included provenance")
	}
	eligible, err := loadEligibleClaimStatement(ctx, u.tx, envelope.residentID, *input.SourceID)
	if err != nil {
		return err
	}
	candidate, err := u.rebuildDialogueV3RenderingCandidate(
		ctx, envelope, *input.SourceID, string(eligible.Statement),
	)
	if err != nil {
		return err
	}
	rendered, err := memory.RenderClaim(envelope.policy, candidate)
	if err != nil {
		return err
	}
	if !bytes.Equal(input.Content.Bytes, []byte(rendered)) {
		return fmt.Errorf("sqlite: dialogue-v3 Recall input %s differs from pinned rendering-v2", input.SourceID)
	}
	return nil
}

// rebuildDialogueV3RenderingCandidate replays only the structured renderer
// inputs. Evidence is evaluated under the policy recorded on each row and
// aggregated under the run-pinned V4 policy, exactly as claim_states does.
func (u *canonicalUoW) rebuildDialogueV3RenderingCandidate(
	ctx context.Context,
	envelope dialogueV3LandingRecall,
	claimID canonical.ID,
	statement string,
) (memory.RecallCandidate, error) {
	var temporalKindRaw string
	var claimRecordedAt, claimCommitSeq int64
	if err := u.tx.QueryRowContext(ctx, `SELECT claim.temporal_kind, claim.recorded_at,
		commit_row.commit_seq
		FROM claims claim
		JOIN canonical_commits commit_row ON commit_row.canonical_commit_id = claim.canonical_commit_id
		WHERE claim.claim_id = ? AND claim.owner_resident_id = ?`,
		claimID.String(), envelope.residentID.String(),
	).Scan(&temporalKindRaw, &claimRecordedAt, &claimCommitSeq); err != nil {
		return memory.RecallCandidate{}, fmt.Errorf("sqlite: load dialogue-v3 Recall claim at Target: %w", err)
	}
	if claimCommitSeq > envelope.head.Int64() || canonical.Instant(claimRecordedAt) > envelope.asOf {
		return memory.RecallCandidate{}, errors.New("sqlite: dialogue-v3 Recall claim is outside its pinned Target")
	}
	temporalKind := memory.TemporalKind(temporalKindRaw)
	if err := temporalKind.Validate(); err != nil {
		return memory.RecallCandidate{}, err
	}

	rows, err := u.tx.QueryContext(ctx, `SELECT evidence.evidence_id, evidence.event_id,
		evidence.source_evidence_id, parent.evidence_id, evidence.recorded_at,
		evidence.memory_policy_revision_id, event.event_type, evidence.polarity,
		evidence.grade, evidence.trust_level, evidence.derivation,
		CASE WHEN evidence.source_evidence_id IS NULL THEN 0
		     WHEN parent.evidence_id IS NULL THEN -1
		     WHEN parent.source_evidence_id IS NULL THEN 1 ELSE 2 END,
		evidence.reason_code, event.actor_principal_id = claim.subject_principal_id,
		event.actor_principal_id = claim.perspective_principal_id, evidence.weight,
		event.resident_id, policy.resident_id, policy.revision_class,
		evidence_commit.commit_seq, event_commit.commit_seq, policy_commit.commit_seq
		FROM claim_evidence evidence
		JOIN claims claim ON claim.claim_id = evidence.claim_id
		JOIN events event ON event.event_id = evidence.event_id
		LEFT JOIN claim_evidence parent ON parent.evidence_id = evidence.source_evidence_id
		JOIN resident_revisions policy ON policy.revision_id = evidence.memory_policy_revision_id
		JOIN canonical_commits evidence_commit ON evidence_commit.canonical_commit_id = evidence.canonical_commit_id
		JOIN canonical_commits event_commit ON event_commit.canonical_commit_id = event.canonical_commit_id
		JOIN canonical_commits policy_commit ON policy_commit.canonical_commit_id = policy.canonical_commit_id
		WHERE evidence.claim_id = ? AND evidence_commit.commit_seq <= ?
		  AND event_commit.commit_seq <= ? AND policy_commit.commit_seq <= ?
		  AND evidence.recorded_at <= ?
		ORDER BY evidence_commit.commit_seq, evidence.evidence_id`,
		claimID.String(), envelope.head.Int64(), envelope.head.Int64(), envelope.head.Int64(),
		envelope.asOf.UnixMicro(),
	)
	if err != nil {
		return memory.RecallCandidate{}, fmt.Errorf("sqlite: load dialogue-v3 Recall evidence at Target: %w", err)
	}
	defer rows.Close()
	var evaluated []memory.EvaluatedEvidence
	var lastConfirmed *canonical.Instant
	for rows.Next() {
		var evidenceRaw, eventRaw, policyRaw, eventTypeRaw, polarityRaw string
		var gradeRaw, trustRaw, derivationRaw, reasonRaw string
		var eventResidentRaw, policyResidentRaw, policyClass string
		var sourceRaw, parentRaw sql.NullString
		var recordedAt, inheritanceDepth, weightRaw int64
		var evidenceCommit, eventCommit, policyCommit int64
		var actorIsSubject, actorIsPerspective bool
		if err := rows.Scan(
			&evidenceRaw, &eventRaw, &sourceRaw, &parentRaw, &recordedAt, &policyRaw,
			&eventTypeRaw, &polarityRaw, &gradeRaw, &trustRaw, &derivationRaw,
			&inheritanceDepth, &reasonRaw, &actorIsSubject, &actorIsPerspective,
			&weightRaw, &eventResidentRaw, &policyResidentRaw, &policyClass,
			&evidenceCommit, &eventCommit, &policyCommit,
		); err != nil {
			return memory.RecallCandidate{}, err
		}
		if evidenceCommit > envelope.head.Int64() || eventCommit > envelope.head.Int64() ||
			policyCommit > envelope.head.Int64() || eventResidentRaw != envelope.residentID.String() ||
			policyResidentRaw != envelope.residentID.String() || policyClass != "memory_policy" ||
			(sourceRaw.Valid != parentRaw.Valid) || inheritanceDepth < 0 {
			return memory.RecallCandidate{}, errors.New("sqlite: dialogue-v3 Recall evidence provenance is invalid at Target")
		}
		evidenceID, err := canonical.ParseID(evidenceRaw)
		if err != nil {
			return memory.RecallCandidate{}, err
		}
		_ = evidenceID // identity parsing is part of fail-closed provenance validation.
		eventID, err := canonical.ParseID(eventRaw)
		if err != nil {
			return memory.RecallCandidate{}, err
		}
		policyID, err := canonical.ParseID(policyRaw)
		if err != nil {
			return memory.RecallCandidate{}, err
		}
		point := memory.EvidencePoint{
			SourceEventID: eventID, EventType: memory.EventType(eventTypeRaw),
			Polarity: memory.EvidencePolarity(polarityRaw), Grade: memory.EvidenceGrade(gradeRaw),
			Trust: memory.TrustLevel(trustRaw), Derivation: memory.EvidenceDerivation(derivationRaw),
			InheritanceDepth: inheritanceDepth, Reason: memory.EvidenceReason(reasonRaw),
			ActorIsSubject: actorIsSubject, ActorIsPerspective: actorIsPerspective,
		}
		if sourceRaw.Valid {
			sourceID, err := canonical.ParseID(sourceRaw.String)
			if err != nil {
				return memory.RecallCandidate{}, err
			}
			point.SourceEvidenceID = &sourceID
		}
		rowPolicy, err := u.loadPreparedDialogueMemoryPolicy(ctx, envelope.residentID, policyID)
		if err != nil {
			return memory.RecallCandidate{}, err
		}
		value, err := memory.EvaluateEvidence(rowPolicy, point)
		if err != nil {
			return memory.RecallCandidate{}, fmt.Errorf("sqlite: evaluate dialogue-v3 Recall evidence %s: %w", evidenceID, err)
		}
		weight, err := canonical.NewWeight(weightRaw)
		if err != nil || value.EffectiveWeight != weight {
			return memory.RecallCandidate{}, errors.New("sqlite: dialogue-v3 Recall evidence weight differs from policy replay")
		}
		evaluated = append(evaluated, value)
		if point.Polarity == memory.PolaritySupport &&
			(lastConfirmed == nil || canonical.Instant(recordedAt) > *lastConfirmed) {
			confirmed := canonical.Instant(recordedAt)
			lastConfirmed = &confirmed
		}
	}
	if err := rows.Err(); err != nil {
		return memory.RecallCandidate{}, err
	}
	if err := rows.Close(); err != nil {
		return memory.RecallCandidate{}, err
	}
	aggregate, err := memory.AggregateEvidence(envelope.policy, evaluated)
	if err != nil {
		return memory.RecallCandidate{}, err
	}

	var validFromRaw, validToRaw sql.NullInt64
	err = u.tx.QueryRowContext(ctx, `SELECT validity.valid_from, validity.valid_to
		FROM claim_validity_assertions validity
		JOIN canonical_commits commit_row ON commit_row.canonical_commit_id = validity.canonical_commit_id
		WHERE validity.claim_id = ? AND commit_row.commit_seq <= ? AND validity.recorded_at <= ?
		ORDER BY commit_row.commit_seq DESC, validity.validity_assertion_id DESC LIMIT 1`,
		claimID.String(), envelope.head.Int64(), envelope.asOf.UnixMicro(),
	).Scan(&validFromRaw, &validToRaw)
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return memory.RecallCandidate{}, fmt.Errorf("sqlite: load dialogue-v3 Recall validity at Target: %w", err)
	}
	temporalInput := memory.TemporalInput{
		Kind: temporalKind, AnchorAt: canonical.Instant(claimRecordedAt), AsOf: envelope.asOf,
	}
	if lastConfirmed != nil {
		temporalInput.AnchorAt = *lastConfirmed
	}
	if validFromRaw.Valid {
		value := canonical.Instant(validFromRaw.Int64)
		temporalInput.ValidFrom = &value
	}
	if validToRaw.Valid {
		value := canonical.Instant(validToRaw.Int64)
		temporalInput.ValidTo = &value
	}
	temporal, err := memory.EvaluateTemporal(envelope.policy, temporalInput)
	if err != nil {
		return memory.RecallCandidate{}, err
	}
	return memory.RecallCandidate{
		ClaimID: claimID, Statement: statement, Confidence: aggregate.Confidence,
		Currentness: temporal.Currentness, LastConfirmed: cloneRecallInstant(lastConfirmed),
		TemporalRelation: temporal.Relation,
	}, nil
}

func (u *canonicalUoW) insertOutcome(ctx context.Context, outcomeID, runID canonical.ID, attempt int64, state string,
	outputID *canonical.ID, promptTokens, completionTokens *int64, latency int64, errorClass string) error {
	m := u.metadata
	var output, prompt, completion, latencyValue, failure any
	if outputID != nil {
		output = outputID.String()
	}
	if promptTokens != nil {
		prompt = *promptTokens
	}
	if completionTokens != nil {
		completion = *completionTokens
	}
	if latency > 0 {
		latencyValue = latency
	}
	if errorClass != "" {
		failure = errorClass
	}
	_, err := u.tx.ExecContext(ctx, `INSERT INTO generation_run_outcomes(
		outcome_id, canonical_commit_id, generation_run_id, attempt_no, state, output_content_id,
		prompt_tokens, completion_tokens, latency, estimated_cost, error_class, error_detail_content_id,
		recorded_at, recorded_tz
	) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, NULL, ?, NULL, ?, ?)`, outcomeID.String(), m.CommitID.String(), runID.String(),
		attempt, state, output, prompt, completion, latencyValue, failure, m.CommittedAt.UnixMicro(), m.CommittedTZ.String())
	if err != nil {
		return fmt.Errorf("insert generation %s outcome: %w", state, err)
	}
	return nil
}

func (u *canonicalUoW) latestOutcome(ctx context.Context, runID, residentID canonical.ID) (int64, string, error) {
	attempt, state, _, err := u.latestOutcomeWithError(ctx, runID, residentID)
	return attempt, state, err
}

func (u *canonicalUoW) latestOutcomeWithError(ctx context.Context, runID, residentID canonical.ID) (int64, string, string, error) {
	outcome, err := (generationOutcomeRepository{}).Latest(ctx, u.tx, runID, residentID)
	if err != nil {
		return 0, "", "", fmt.Errorf("resolve latest generation outcome: %w", err)
	}
	return outcome.AttemptNo, outcome.State, outcome.ErrorClass.String, nil
}

func (u *canonicalUoW) validateGenerationSource(
	ctx context.Context,
	prepare domain.PrepareGeneration,
	input domain.GenerationInput,
	keepFrozenLegacyRecall bool,
) error {
	residentID := prepare.ResidentID
	if input.SourceType == "runtime_projection" && input.SourceID == nil {
		return nil
	}
	if input.SourceID == nil {
		return errors.New("sqlite: generation input source_id is required for canonical sources")
	}
	var actual string
	var err error
	switch input.SourceType {
	case "event":
		err = u.tx.QueryRowContext(ctx, `SELECT resident_id FROM events WHERE event_id = ?`, input.SourceID.String()).Scan(&actual)
	case "resident_revision":
		err = u.tx.QueryRowContext(ctx, `SELECT resident_id FROM resident_revisions WHERE revision_id = ?`, input.SourceID.String()).Scan(&actual)
	case "content":
		err = u.tx.QueryRowContext(ctx, `SELECT owner_resident_id FROM content_objects WHERE content_id = ?`, input.SourceID.String()).Scan(&actual)
	case "claim":
		renderRecall, contractErr := claimGenerationSourceContract(prepare.Purpose, input.InclusionMode)
		if contractErr != nil {
			return contractErr
		}
		eligible, eligibleErr := loadEligibleClaimStatement(ctx, u.tx, residentID, *input.SourceID)
		if eligibleErr != nil {
			return eligibleErr
		}
		var expected []byte
		if renderRecall {
			if prepare.RecallRunID == nil {
				return errors.New("sqlite: dialogue Recall generation rendering envelope mismatch")
			}
			if keepFrozenLegacyRecall {
				if prepare.MemoryRenderingVersion != domain.MemoryRenderingVersionNoneV1 {
					return errors.New("sqlite: legacy dialogue Recall generation rendering envelope mismatch")
				}
			} else if prepare.MemoryRenderingVersion != domain.MemoryRenderingVersionV1 {
				return errors.New("sqlite: dialogue Recall generation rendering envelope mismatch")
			}
			var recallPolicyRaw, recallResident string
			if err := u.tx.QueryRowContext(ctx, `SELECT memory_policy_revision_id, resident_id FROM recall_runs WHERE recall_run_id = ?`, prepare.RecallRunID.String()).Scan(&recallPolicyRaw, &recallResident); err != nil {
				return fmt.Errorf("sqlite: resolve dialogue Recall run: %w", err)
			}
			if recallResident != residentID.String() || recallPolicyRaw != prepare.MemoryPolicyRevisionID.String() {
				return errors.New("sqlite: dialogue Recall run does not pin the memory policy revision")
			}
			if keepFrozenLegacyRecall {
				// dialogue-v1 historically labelled rendered Recall inputs as
				// memory-none-v1. The exact legacy tuple and Recall linkage have been
				// validated, and the claim is still eligible above; retry/landing must
				// use its immutable frozen bytes rather than re-rendering them from a
				// later TemporalRelation projection.
				return nil
			}
			var policyBytes []byte
			if err := u.tx.QueryRowContext(ctx, `SELECT blob.content FROM resident_revisions revision
				JOIN content_objects content ON content.content_id = revision.content_id
				JOIN blobs blob ON blob.dedupe_scope_id = content.owner_resident_id
				 AND blob.hash_algorithm = content.blob_hash_algorithm AND blob.blob_hash = content.blob_hash
				WHERE revision.revision_id = ? AND revision.resident_id = ? AND revision.revision_class = 'memory_policy'
				  AND content.erasure_state = 'present'`, prepare.MemoryPolicyRevisionID.String(), residentID.String()).Scan(&policyBytes); err != nil {
				return fmt.Errorf("sqlite: resolve dialogue Recall policy: %w", err)
			}
			policy, _, err := memory.ParsePolicy(policyBytes)
			if err != nil {
				return err
			}
			if policy.RenderingVersion != memory.RenderingVersionV1 {
				return errors.New("sqlite: prior-split dialogue Recall requires the exact rendering-v1 policy")
			}
			candidate := memory.RecallCandidate{ClaimID: eligible.ClaimID, Statement: string(eligible.Statement), TemporalRelation: eligible.TemporalRelation}
			rendered, renderErr := memory.RenderClaim(policy, candidate)
			if renderErr != nil {
				return renderErr
			}
			expected = []byte(rendered)
		} else {
			expected = eligible.Statement
		}
		if !bytes.Equal(input.Content.Bytes, expected) {
			return fmt.Errorf("sqlite: claim generation input %s does not match frozen eligible statement", input.SourceID)
		}
		return nil
	default:
		return fmt.Errorf("sqlite: unsupported generation source type %q", input.SourceType)
	}
	if err != nil {
		return fmt.Errorf("sqlite: resolve %s generation source: %w", input.SourceType, err)
	}
	if actual != residentID.String() {
		return errors.New("sqlite: generation input source belongs to another resident")
	}
	return nil
}

func claimGenerationSourceContract(purpose domain.GenerationPurpose, inclusionMode string) (bool, error) {
	if inclusionMode != "memory_recall" {
		return false, fmt.Errorf(
			"sqlite: claim generation source is unsupported for purpose %q and inclusion mode %q",
			purpose.Effective(), inclusionMode,
		)
	}
	switch purpose.Effective() {
	case domain.GenerationPurposeDialogue:
		return true, nil
	case domain.GenerationPurposePersonaRevision,
		domain.GenerationPurposeMemoryAlignment,
		domain.GenerationPurposeMemoryAbstraction,
		domain.GenerationPurposeMemoryDifferentiation,
		domain.GenerationPurposeSelfTalk,
		domain.GenerationPurposeOutboundInitiative:
		return false, nil
	default:
		return false, fmt.Errorf(
			"sqlite: claim generation source is unsupported for purpose %q and inclusion mode %q",
			purpose.Effective(), inclusionMode,
		)
	}
}

func (u *canonicalUoW) requireActiveRevision(ctx context.Context, residentID, revisionID canonical.ID, class string) error {
	var active string
	err := u.tx.QueryRowContext(ctx, `SELECT a.revision_id
		FROM resident_revision_activations a
		JOIN resident_revisions r ON r.revision_id = a.revision_id
		JOIN canonical_commits c ON c.canonical_commit_id = a.canonical_commit_id
		WHERE a.resident_id = ? AND r.revision_class = ? ORDER BY c.commit_seq DESC LIMIT 1`, residentID.String(), class).Scan(&active)
	if err != nil {
		return fmt.Errorf("sqlite: resolve active %s revision: %w", class, err)
	}
	if active != revisionID.String() {
		return fmt.Errorf("sqlite: generation references inactive %s revision", class)
	}
	return nil
}

func (u *canonicalUoW) requireActiveResident(ctx context.Context, residentID canonical.ID) error {
	status, err := u.currentStatus(ctx, residentID)
	if err != nil {
		return err
	}
	if status != "active" {
		return fmt.Errorf("sqlite: resident is %s, not active", status)
	}
	return nil
}

func (u *canonicalUoW) requirePrincipal(ctx context.Context, principalID canonical.ID, kind string) error {
	var actual string
	if err := u.tx.QueryRowContext(ctx, `SELECT kind FROM principals WHERE principal_id = ?`, principalID.String()).Scan(&actual); err != nil {
		return fmt.Errorf("sqlite: resolve principal: %w", err)
	}
	if actual != kind {
		return fmt.Errorf("sqlite: principal kind is %s, want %s", actual, kind)
	}
	return nil
}

func (u *canonicalUoW) requireResidentPrincipal(ctx context.Context, residentID, principalID canonical.ID) error {
	var actual string
	if err := u.tx.QueryRowContext(ctx, `SELECT principal_id FROM residents WHERE resident_id = ?`, residentID.String()).Scan(&actual); err != nil {
		return fmt.Errorf("sqlite: resolve resident principal: %w", err)
	}
	if actual != principalID.String() {
		return errors.New("sqlite: resident principal mismatch")
	}
	return nil
}

type eventSpec struct {
	ID                canonical.ID
	ResidentID        canonical.ID
	Type              string
	Visibility        string
	DeliveryScreen    bool
	DeliveryAudio     bool
	Ingress           string
	ActorPrincipalID  canonical.ID
	TargetPrincipalID *canonical.ID
	GenerationRunID   *canonical.ID
	OccurredAt        canonical.Instant
	OccurredTZ        canonical.Timezone
	Content           domain.Content
}

type eventHashEnvelope struct {
	EventID                 canonical.ID       `json:"event_id"`
	ResidentID              canonical.ID       `json:"resident_id"`
	Seq                     canonical.Seq      `json:"seq"`
	EventType               string             `json:"event_type"`
	Visibility              string             `json:"visibility"`
	DeliveryScreen          bool               `json:"delivery_screen"`
	DeliveryAudio           bool               `json:"delivery_audio"`
	Ingress                 string             `json:"ingress"`
	TrustLevel              string             `json:"trust_level"`
	ActorPrincipalID        canonical.ID       `json:"actor_principal_id"`
	TargetPrincipalID       *canonical.ID      `json:"target_principal_id"`
	GenerationRunID         *canonical.ID      `json:"generation_run_id"`
	OccurredAt              canonical.Instant  `json:"occurred_at"`
	OccurredTZ              canonical.Timezone `json:"occurred_tz"`
	RecordedAt              canonical.Instant  `json:"recorded_at"`
	RecordedTZ              canonical.Timezone `json:"recorded_tz"`
	ContentID               canonical.ID       `json:"content_id"`
	PayloadCommitment       canonical.Digest   `json:"payload_commitment"`
	PrevEventHash           *canonical.Digest  `json:"prev_event_hash"`
	EventHashAlgorithm      string             `json:"event_hash_algorithm"`
	EventHashDomain         string             `json:"event_hash_domain"`
	CanonicalizationVersion string             `json:"canonicalization_version"`
}

func (u *canonicalUoW) makeEvent(ctx context.Context, spec eventSpec) (domain.Event, error) {
	seq, prev, err := u.nextEventChain(ctx, spec.ResidentID)
	if err != nil {
		return domain.Event{}, err
	}
	envelope := eventHashEnvelope{
		EventID: spec.ID, ResidentID: spec.ResidentID, Seq: seq, EventType: spec.Type,
		Visibility: spec.Visibility, DeliveryScreen: spec.DeliveryScreen, DeliveryAudio: spec.DeliveryAudio,
		Ingress: spec.Ingress, TrustLevel: "trusted", ActorPrincipalID: spec.ActorPrincipalID,
		TargetPrincipalID: spec.TargetPrincipalID, GenerationRunID: spec.GenerationRunID,
		OccurredAt: spec.OccurredAt, OccurredTZ: spec.OccurredTZ,
		RecordedAt: u.metadata.CommittedAt, RecordedTZ: u.metadata.CommittedTZ,
		ContentID: spec.Content.ID, PayloadCommitment: spec.Content.Commitment, PrevEventHash: prev,
		EventHashAlgorithm: canonical.HashAlgorithm, EventHashDomain: canonical.EventHashDomain,
		CanonicalizationVersion: canonical.CanonicalizationVersion,
	}
	encoded, err := canonical.MarshalCanonical(envelope)
	if err != nil {
		return domain.Event{}, fmt.Errorf("canonicalize event hash envelope: %w", err)
	}
	hash, err := canonical.HashEvent(encoded)
	if err != nil {
		return domain.Event{}, err
	}
	return domain.Event{
		ID: spec.ID, ResidentID: spec.ResidentID, Seq: seq, Type: spec.Type,
		ActorPrincipalID: spec.ActorPrincipalID, TargetPrincipalID: spec.TargetPrincipalID,
		GenerationRunID: spec.GenerationRunID, OccurredAt: spec.OccurredAt, OccurredTZ: spec.OccurredTZ,
		RecordedAt: u.metadata.CommittedAt,
		RecordedTZ: u.metadata.CommittedTZ, ContentID: spec.Content.ID, Content: string(spec.Content.Bytes),
		Commitment: spec.Content.Commitment, PrevHash: prev, Hash: hash,
	}, nil
}

func (u *canonicalUoW) nextEventChain(ctx context.Context, residentID canonical.ID) (canonical.Seq, *canonical.Digest, error) {
	var seq int64
	var raw []byte
	err := u.tx.QueryRowContext(ctx, `SELECT seq, event_hash FROM events WHERE resident_id = ? ORDER BY seq DESC LIMIT 1`, residentID.String()).Scan(&seq, &raw)
	if errors.Is(err, sql.ErrNoRows) {
		first, firstErr := canonical.NewSeq(1)
		return first, nil, firstErr
	}
	if err != nil {
		return 0, nil, fmt.Errorf("resolve event chain head: %w", err)
	}
	digest, err := canonical.DigestFromBytes(raw)
	if err != nil {
		return 0, nil, err
	}
	next, err := canonical.NewSeq(seq + 1)
	if err != nil {
		return 0, nil, err
	}
	return next, &digest, nil
}

func (u *canonicalUoW) insertEvent(ctx context.Context, event domain.Event, visibility string, screen, audio bool, ingress string) error {
	var target, run, previous any
	if event.TargetPrincipalID != nil {
		target = event.TargetPrincipalID.String()
	}
	if event.GenerationRunID != nil {
		run = event.GenerationRunID.String()
	}
	if event.PrevHash != nil {
		previous = event.PrevHash.Bytes()
	}
	_, err := u.tx.ExecContext(ctx, `INSERT INTO events(
		event_id, canonical_commit_id, resident_id, seq, event_type, visibility,
		delivery_screen, delivery_audio, ingress, trust_level, actor_principal_id,
		target_principal_id, generation_run_id, occurred_at, occurred_tz, recorded_at,
		recorded_tz, content_id, payload_commitment, prev_event_hash, event_hash,
		event_hash_algorithm, event_hash_domain, canonicalization_version
	) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 'trusted', ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'sha256', ?, ?)`,
		event.ID.String(), u.metadata.CommitID.String(), event.ResidentID.String(), event.Seq.Int64(), event.Type,
		visibility, boolInt(screen), boolInt(audio), ingress, event.ActorPrincipalID.String(), target, run,
		event.OccurredAt.UnixMicro(), event.OccurredTZ.String(), event.RecordedAt.UnixMicro(), event.RecordedTZ.String(),
		event.ContentID.String(), event.Commitment.Bytes(), previous, event.Hash.Bytes(), canonical.EventHashDomain,
		canonical.CanonicalizationVersion)
	if err != nil {
		return fmt.Errorf("insert %s event: %w", event.Type, err)
	}
	return nil
}

func boolInt(value bool) int {
	if value {
		return 1
	}
	return 0
}
