package app

import (
	"context"
	"errors"
	"math"
	"testing"

	"mahoroba.local/mahoroba/internal/canonical"
	"mahoroba.local/mahoroba/internal/domain"
	"mahoroba.local/mahoroba/internal/integrity"
)

func TestM7RecoveryRunlessDialogueAndMemoryUseFrozenInternalEnvelopeWithoutProvider(t *testing.T) {
	ctx := context.Background()
	generator := &scriptedGenerator{}
	fixture := newApplicationFixture(t, generator, 1)
	activation, err := fixture.application.activateMemoryPolicyV4ForTest(ctx, fixture.residentID)
	if err != nil {
		t.Fatal(err)
	}
	event := ingressPendingForTest(t, fixture, "cancel both runless obligations")
	newerActivation, err := fixture.application.activateMemoryPolicyV4ForTest(ctx, fixture.residentID)
	if err != nil {
		t.Fatal(err)
	}
	if newerActivation.RevisionID != activation.RevisionID || newerActivation.Changed {
		t.Fatalf("memory-policy-v4 retry changed frozen revision: before=%+v after=%+v", activation, newerActivation)
	}
	eraseEventContentForTest(t, fixture.store.Path(), event.ContentID)

	if err := fixture.application.Recover(ctx); err != nil {
		t.Fatal(err)
	}
	if generator.CallCount() != 0 {
		t.Fatalf("recovery provider calls = %d, want 0", generator.CallCount())
	}

	dialogueRun := assertSyntheticRecoveryRun(t, fixture, domain.DialogueObligation(event.ID),
		"dialogue", "source_content_erased", activation.RevisionID)
	var currentPipelineRaw string
	if err := fixture.store.Reader().QueryRow(`SELECT pipeline_version_id FROM pipeline_versions
		WHERE pipeline_kind = 'dialogue' AND version_key = ?`, domain.DialoguePipelineVersionV3).Scan(&currentPipelineRaw); err != nil {
		t.Fatal(err)
	}
	currentPipelineID, err := canonical.ParseID(currentPipelineRaw)
	if err != nil {
		t.Fatal(err)
	}
	assertCOV2SyntheticDialogueContract(t, fixture, dialogueRun, currentPipelineID, domain.DialoguePipelineVersionV3)
	memoryRun := assertSyntheticRecoveryRun(t, fixture, domain.MemoryExtractionObligation(event.ID),
		"memory_extraction", "source_content_erased", activation.RevisionID)
	if dialogueRun == memoryRun {
		t.Fatal("dialogue and memory cancellation reused one generation identity")
	}
	var dialogueDropped, memoryDropped string
	if err := fixture.store.Reader().QueryRow(`SELECT dropped_input_summary FROM generation_runs
		WHERE generation_run_id = ?`, dialogueRun.String()).Scan(&dialogueDropped); err != nil {
		t.Fatal(err)
	}
	if err := fixture.store.Reader().QueryRow(`SELECT dropped_input_summary FROM generation_runs
		WHERE generation_run_id = ?`, memoryRun.String()).Scan(&memoryDropped); err != nil {
		t.Fatal(err)
	}
	if dialogueDropped != `{"backfill":"0","live_context":"0","reason":"source_content_erased"}` {
		t.Fatalf("dialogue dropped summary = %s", dialogueDropped)
	}
	if memoryDropped != `{"reason":"source_content_erased"}` {
		t.Fatalf("memory dropped summary = %s", memoryDropped)
	}

	before := recoveryRunOutcomeCount(t, fixture, dialogueRun) + recoveryRunOutcomeCount(t, fixture, memoryRun)
	if err := fixture.application.Recover(ctx); err != nil {
		t.Fatal(err)
	}
	after := recoveryRunOutcomeCount(t, fixture, dialogueRun) + recoveryRunOutcomeCount(t, fixture, memoryRun)
	if before != 2 || after != before || generator.CallCount() != 0 {
		t.Fatalf("idempotent recovery outcomes/calls = %d -> %d / %d", before, after, generator.CallCount())
	}
}

func TestM7RecoveryLatestFrozenRevisionErasedRemainsUnresolvable(t *testing.T) {
	for _, revisionClass := range []string{"principles", "persona"} {
		t.Run(revisionClass, func(t *testing.T) {
			ctx := context.Background()
			generator := &scriptedGenerator{}
			fixture := newApplicationFixture(t, generator, 1)
			if _, err := fixture.application.activateMemoryPolicyV4ForTest(ctx, fixture.residentID); err != nil {
				t.Fatal(err)
			}
			event := ingressPendingForTest(t, fixture, "latest frozen revision erased")
			contentID := latestRecoveryRevisionContentForEvent(t, fixture, event, revisionClass)
			eraseEventContentForTest(t, fixture.store.Path(), contentID)
			archiveResidentWithoutDrain(t, fixture)

			result, err := fixture.application.RecoverMandatory(ctx)
			if err != nil {
				t.Fatal(err)
			}
			if result.CancelledMandatoryWork != 0 || len(result.UnresolvedCandidates) != 1 {
				t.Fatalf("latest-erased recovery result = %+v", result)
			}
			if result.UnresolvedCandidates[0].RuleCode != integrity.RuleCancellationEnvelopeUnresolvable {
				t.Fatalf("latest-erased candidate = %+v", result.UnresolvedCandidates[0])
			}
			for _, key := range []string{
				domain.DialogueObligation(event.ID), domain.MemoryExtractionObligation(event.ID),
			} {
				var runs int
				if err := fixture.store.Reader().QueryRow(`SELECT COUNT(*) FROM generation_runs
					WHERE resident_id = ? AND idempotency_key = ?`, fixture.residentID.String(), key).Scan(&runs); err != nil {
					t.Fatal(err)
				}
				if runs != 0 {
					t.Fatalf("latest-erased revision invented run for %s", key)
				}
			}
			if generator.CallCount() != 0 {
				t.Fatalf("latest-erased provider calls = %d", generator.CallCount())
			}
		})
	}
}

func TestM7RecoveryRunningMandatoryWorkInterruptsThenCancelsNextAttempt(t *testing.T) {
	ctx := context.Background()
	generator := &scriptedGenerator{}
	fixture := newApplicationFixture(t, generator, 1)
	activation, err := fixture.application.activateMemoryPolicyV4ForTest(ctx, fixture.residentID)
	if err != nil {
		t.Fatal(err)
	}
	event := ingressPendingForTest(t, fixture, "running obligations")
	dialogueRun := prepareCommittedDialogueForRecoveryTest(t, fixture, event)
	memoryRun := prepareMandatoryMemoryRecoveryRun(t, fixture, event, activation.RevisionID)
	archiveResidentWithoutDrain(t, fixture)

	runningResult, err := fixture.application.RecoverRunning(ctx)
	if err != nil {
		t.Fatal(err)
	}
	for _, runID := range []canonical.ID{dialogueRun, memoryRun} {
		assertRecoveryAttemptHistory(t, fixture, runID, []recoveryOutcomeWant{
			{1, "running", ""}, {1, "cancelled", "runtime_interrupted"},
		})
	}
	if runningResult.TerminalizedAttempts != 2 || runningResult.CancelledMandatoryWork != 0 {
		t.Fatalf("running phase result = %+v", runningResult)
	}
	mandatoryResult, err := fixture.application.RecoverMandatory(ctx)
	if err != nil {
		t.Fatal(err)
	}
	for _, runID := range []canonical.ID{dialogueRun, memoryRun} {
		assertRecoveryAttemptHistory(t, fixture, runID, []recoveryOutcomeWant{
			{1, "running", ""},
			{1, "cancelled", "runtime_interrupted"},
			{2, "running", ""},
			{2, "cancelled", "resident_inactive"},
		})
	}
	if mandatoryResult.TerminalizedAttempts != 0 || mandatoryResult.CancelledMandatoryWork != 2 {
		t.Fatalf("mandatory phase result = %+v", mandatoryResult)
	}
	if generator.CallCount() != 0 {
		t.Fatalf("running recovery provider calls = %d, want 0", generator.CallCount())
	}
}

func TestM7RecoveryRetryPendingMandatoryWorkCancelsNextAttemptDespiteRuntimeMax(t *testing.T) {
	ctx := context.Background()
	fixture := newApplicationFixture(t, &scriptedGenerator{}, 1)
	activation, err := fixture.application.activateMemoryPolicyV4ForTest(ctx, fixture.residentID)
	if err != nil {
		t.Fatal(err)
	}
	event := ingressPendingForTest(t, fixture, "retry pending obligations")
	dialogueRun := prepareCommittedDialogueForRecoveryTest(t, fixture, event)
	memoryRun := prepareMandatoryMemoryRecoveryRun(t, fixture, event, activation.RevisionID)
	for _, runID := range []canonical.ID{dialogueRun, memoryRun} {
		failRecoveryAttempt(t, fixture, runID, 1, "provider_timeout")
	}
	archiveResidentWithoutDrain(t, fixture)

	if err := fixture.application.Recover(ctx); err != nil {
		t.Fatal(err)
	}
	for _, runID := range []canonical.ID{dialogueRun, memoryRun} {
		assertRecoveryAttemptHistory(t, fixture, runID, []recoveryOutcomeWant{
			{1, "running", ""}, {1, "failed", "provider_timeout"},
			{2, "running", ""}, {2, "cancelled", "resident_inactive"},
		})
	}
}

func TestM7RecoveryReasonPriorityAndMemoryUnselectedExclusion(t *testing.T) {
	t.Run("source erased outranks inactive", func(t *testing.T) {
		ctx := context.Background()
		fixture := newApplicationFixture(t, &scriptedGenerator{}, 1)
		activation, err := fixture.application.activateMemoryPolicyV4ForTest(ctx, fixture.residentID)
		if err != nil {
			t.Fatal(err)
		}
		event := ingressPendingForTest(t, fixture, "priority")
		eraseEventContentForTest(t, fixture.store.Path(), event.ContentID)
		archiveResidentWithoutDrain(t, fixture)
		if err := fixture.application.Recover(ctx); err != nil {
			t.Fatal(err)
		}
		assertSyntheticRecoveryRun(t, fixture, domain.DialogueObligation(event.ID),
			"dialogue", "source_content_erased", activation.RevisionID)
		assertSyntheticRecoveryRun(t, fixture, domain.MemoryExtractionObligation(event.ID),
			"memory_extraction", "source_content_erased", activation.RevisionID)
	})

	t.Run("memory ignores active unselected", func(t *testing.T) {
		ctx := context.Background()
		fixture := newApplicationFixture(t, &scriptedGenerator{}, 1)
		if _, err := fixture.application.activateMemoryPolicyV4ForTest(ctx, fixture.residentID); err != nil {
			t.Fatal(err)
		}
		event := ingressPendingForTest(t, fixture, "selection changed")
		state, err := fixture.application.BootstrapInit(ctx, BootstrapInput{
			OwnerName: "Owner", Name: "Selected", SeedKey: "selected-2", Principles: "be exact",
		})
		if err != nil {
			t.Fatal(err)
		}
		var selected canonical.ID
		for _, resident := range state.Residents {
			if resident.SeedKey == "selected-2" {
				selected = resident.ResidentID
			}
		}
		if err := fixture.application.ApprovePrinciples(ctx, selected); err != nil {
			t.Fatal(err)
		}
		if err := fixture.application.FinalizeBootstrap(ctx, selected, "persona",
			`{"mandatory_event_types":[],"memory_recall_enabled":false,"version":"memory-policy-v1"}`); err != nil {
			t.Fatal(err)
		}
		if err := fixture.application.SelectResident(ctx, selected); err != nil {
			t.Fatal(err)
		}
		if err := fixture.application.Recover(ctx); err != nil {
			t.Fatal(err)
		}
		assertSyntheticRecoveryRun(t, fixture, domain.DialogueObligation(event.ID),
			"dialogue", "resident_unselected", canonical.ID{})
		var memoryRuns int
		if err := fixture.store.Reader().QueryRow(`SELECT COUNT(*) FROM generation_runs
			WHERE resident_id = ? AND idempotency_key = ?`, fixture.residentID.String(),
			domain.MemoryExtractionObligation(event.ID)).Scan(&memoryRuns); err != nil {
			t.Fatal(err)
		}
		if memoryRuns != 0 {
			t.Fatalf("unselected resident memory cancellation runs = %d, want 0", memoryRuns)
		}
	})
}

func TestM7RecoveryAttemptOverflowIsTypedAndPreflightMakesNoMutation(t *testing.T) {
	runID := mustRecoveryTestID(t, "01J00000000000000000000041")
	residentID := mustRecoveryTestID(t, "01J00000000000000000000042")
	eventID := mustRecoveryTestID(t, "01J00000000000000000000043")
	repository := &overflowMandatoryRecoveryRepository{work: domain.MandatoryRecoveryWork{
		Kind:           domain.MandatoryRecoveryDialogue,
		SourceEvent:    domain.Event{ID: eventID, ResidentID: residentID},
		IdempotencyKey: domain.DialogueObligation(eventID), RunID: &runID,
		AttemptNo: math.MaxInt64, State: domain.WorkRetryPending, CancellationCode: "resident_inactive",
	}}
	var submits int
	terminalizer, err := NewRecoveryTerminalizer(RecoveryTerminalizerOptions{
		Repository: repository, MandatoryRepository: repository, EnvelopeResolver: repository,
		IDs: canonical.NewSecureIDGenerator(),
		Submit: func(context.Context, canonical.Command) (canonical.CommandResult, error) {
			submits++
			return canonical.CommandResult{}, nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	_, err = terminalizer.Terminalize(context.Background())
	var overflow *domain.RecoveryAttemptOverflowError
	if !errors.Is(err, domain.ErrRecoveryAttemptOverflow) || !errors.As(err, &overflow) || overflow.RunID != runID {
		t.Fatalf("overflow error = %v, want typed run %s", err, runID)
	}
	if submits != 0 || repository.resolveCalls != 0 {
		t.Fatalf("overflow mutation/resolve calls = %d/%d, want 0/0", submits, repository.resolveCalls)
	}
}

func TestM7RecoveryAttemptOverflowPrecedesRunningTerminalization(t *testing.T) {
	overflowRunID := mustRecoveryTestID(t, "01J00000000000000000000061")
	runningRunID := mustRecoveryTestID(t, "01J00000000000000000000062")
	residentID := mustRecoveryTestID(t, "01J00000000000000000000063")
	eventID := mustRecoveryTestID(t, "01J00000000000000000000064")
	repository := &overflowMandatoryRecoveryRepository{
		running: []domain.RunningAttempt{{
			RunID: runningRunID, ResidentID: residentID, AttemptNo: 1,
		}},
		works: []domain.MandatoryRecoveryWork{
			{
				Kind: domain.MandatoryRecoveryDialogue, SourceEvent: domain.Event{ID: eventID, ResidentID: residentID},
				IdempotencyKey: domain.DialogueObligation(eventID), RunID: &runningRunID,
				AttemptNo: 1, State: domain.WorkRunning, CancellationCode: "resident_inactive",
			},
			{
				Kind: domain.MandatoryRecoveryMemoryExtraction, SourceEvent: domain.Event{ID: eventID, ResidentID: residentID},
				IdempotencyKey: domain.MemoryExtractionObligation(eventID), RunID: &overflowRunID,
				AttemptNo: math.MaxInt64, State: domain.WorkRetryPending, CancellationCode: "resident_inactive",
			},
		},
	}
	var submits int
	terminalizer, err := NewRecoveryTerminalizer(RecoveryTerminalizerOptions{
		Repository: repository, MandatoryRepository: repository, EnvelopeResolver: repository,
		IDs: canonical.NewSecureIDGenerator(), Submit: func(context.Context, canonical.Command) (canonical.CommandResult, error) {
			submits++
			return canonical.CommandResult{}, nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := terminalizer.PreflightMandatory(context.Background()); !errors.Is(err, domain.ErrRecoveryAttemptOverflow) {
		t.Fatalf("public preflight error = %v, want ErrRecoveryAttemptOverflow", err)
	}
	_, err = terminalizer.Terminalize(context.Background())
	var overflow *domain.RecoveryAttemptOverflowError
	if !errors.Is(err, domain.ErrRecoveryAttemptOverflow) || !errors.As(err, &overflow) || overflow.RunID != overflowRunID {
		t.Fatalf("composite overflow error = %v, want typed run %s", err, overflowRunID)
	}
	if submits != 0 || repository.runningCalls != 0 || repository.resolveCalls != 0 {
		t.Fatalf("overflow touched running/submit/resolve = %d/%d/%d, want 0/0/0",
			repository.runningCalls, submits, repository.resolveCalls)
	}
}

func TestM7RecoveryRunningAttemptAtMaxInt64FailsBeforeInterruption(t *testing.T) {
	runID := mustRecoveryTestID(t, "01J00000000000000000000071")
	residentID := mustRecoveryTestID(t, "01J00000000000000000000072")
	eventID := mustRecoveryTestID(t, "01J00000000000000000000073")
	repository := &overflowMandatoryRecoveryRepository{
		running: []domain.RunningAttempt{{RunID: runID, ResidentID: residentID, AttemptNo: math.MaxInt64}},
		works: []domain.MandatoryRecoveryWork{{
			Kind: domain.MandatoryRecoveryDialogue, SourceEvent: domain.Event{ID: eventID, ResidentID: residentID},
			IdempotencyKey: domain.DialogueObligation(eventID), RunID: &runID,
			AttemptNo: math.MaxInt64, State: domain.WorkRunning, CancellationCode: "resident_inactive",
		}},
	}
	var submits int
	terminalizer, err := NewRecoveryTerminalizer(RecoveryTerminalizerOptions{
		Repository: repository, MandatoryRepository: repository, EnvelopeResolver: repository,
		IDs: canonical.NewSecureIDGenerator(), Submit: func(context.Context, canonical.Command) (canonical.CommandResult, error) {
			submits++
			return canonical.CommandResult{}, nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	_, err = terminalizer.Terminalize(context.Background())
	var overflow *domain.RecoveryAttemptOverflowError
	if !errors.Is(err, domain.ErrRecoveryAttemptOverflow) || !errors.As(err, &overflow) || overflow.RunID != runID {
		t.Fatalf("running overflow error = %v, want typed run %s", err, runID)
	}
	if submits != 0 || repository.runningCalls != 0 || repository.resolveCalls != 0 {
		t.Fatalf("running overflow touched discovery/submit/resolve = %d/%d/%d",
			repository.runningCalls, submits, repository.resolveCalls)
	}
}

func TestM7RecoveryExistingRetryKeepsPersistedProviderEnvelope(t *testing.T) {
	runID := mustRecoveryTestID(t, "01J00000000000000000000081")
	residentID := mustRecoveryTestID(t, "01J00000000000000000000082")
	eventID := mustRecoveryTestID(t, "01J00000000000000000000083")
	repository := &existingMandatoryRecoveryRepository{
		work: domain.MandatoryRecoveryWork{
			Kind: domain.MandatoryRecoveryDialogue, SourceEvent: domain.Event{ID: eventID, ResidentID: residentID},
			IdempotencyKey: domain.DialogueObligation(eventID), RunID: &runID,
			AttemptNo: 1, State: domain.WorkRetryPending, CancellationCode: "resident_inactive",
		},
		envelope: domain.PrepareGeneration{
			RunID: runID, ResidentID: residentID, Purpose: domain.GenerationPurposeDialogue,
			IdempotencyKey: domain.DialogueObligation(eventID), Provider: "persisted-provider", Model: "persisted-model",
		},
	}
	var submits int
	terminalizer, err := NewRecoveryTerminalizer(RecoveryTerminalizerOptions{
		Repository: repository, MandatoryRepository: repository, EnvelopeResolver: repository,
		IDs: canonical.NewSecureIDGenerator(), Submit: func(context.Context, canonical.Command) (canonical.CommandResult, error) {
			submits++
			return canonical.CommandResult{}, nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := terminalizer.Terminalize(context.Background()); err != nil {
		t.Fatalf("existing persisted provider envelope recovery failed: %v", err)
	}
	if submits != 1 || repository.resolveCalls != 1 {
		t.Fatalf("existing retry submit/resolve = %d/%d, want 1/1", submits, repository.resolveCalls)
	}
}

func TestM7RecoveryUnresolvableEnvelopeExposesIntegrityCandidateWithoutRun(t *testing.T) {
	residentID := mustRecoveryTestID(t, "01J00000000000000000000051")
	eventID := mustRecoveryTestID(t, "01J00000000000000000000052")
	repository := &unresolvedMandatoryRecoveryRepository{work: domain.MandatoryRecoveryWork{
		Kind: domain.MandatoryRecoveryDialogue,
		SourceEvent: domain.Event{ID: eventID, ResidentID: residentID,
			RecordedAt: 1, RecordedTZ: canonical.MustTimezone("UTC")},
		IdempotencyKey: domain.DialogueObligation(eventID), State: domain.WorkPending,
		CancellationCode: "source_content_erased",
	}}
	var submits int
	terminalizer, err := NewRecoveryTerminalizer(RecoveryTerminalizerOptions{
		Repository: repository, MandatoryRepository: repository, EnvelopeResolver: repository,
		IDs: canonical.NewSecureIDGenerator(), Submit: func(context.Context, canonical.Command) (canonical.CommandResult, error) {
			submits++
			return canonical.CommandResult{}, nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	result, err := terminalizer.Terminalize(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if submits != 0 || result.CancelledMandatoryWork != 0 || len(result.UnresolvedCandidates) != 1 {
		t.Fatalf("unresolved result submits=%d result=%+v", submits, result)
	}
	candidate := result.UnresolvedCandidates[0]
	if candidate.RuleCode != integrity.RuleCancellationEnvelopeUnresolvable || candidate.TargetID != eventID {
		t.Fatalf("unresolved candidate = %+v", candidate)
	}
}

func assertSyntheticRecoveryRun(
	t *testing.T,
	fixture applicationFixture,
	key, purpose, reason string,
	wantMemoryPolicy canonical.ID,
) canonical.ID {
	t.Helper()
	var runRaw, provider, model, policyRaw, params string
	var inputs, attempt int64
	var state, errorClass string
	err := fixture.store.Reader().QueryRow(`SELECT run.generation_run_id, run.provider, run.model,
		run.memory_policy_revision_id, run.generator_params,
		(SELECT COUNT(*) FROM generation_run_inputs input WHERE input.generation_run_id = run.generation_run_id),
		outcome.attempt_no, outcome.state, outcome.error_class
		FROM generation_runs run JOIN generation_run_outcomes outcome
		  ON outcome.generation_run_id = run.generation_run_id
		WHERE run.resident_id = ? AND run.idempotency_key = ? AND run.purpose = ?`,
		fixture.residentID.String(), key, purpose).Scan(
		&runRaw, &provider, &model, &policyRaw, &params, &inputs, &attempt, &state, &errorClass,
	)
	if err != nil {
		t.Fatal(err)
	}
	runID, err := canonical.ParseID(runRaw)
	if err != nil {
		t.Fatal(err)
	}
	if provider != "mahoroba-internal" || model != "not-dispatched" || inputs != 0 ||
		attempt != 0 || state != "cancelled" || errorClass != reason {
		t.Fatalf("synthetic run = provider=%s model=%s inputs=%d outcome=%d/%s/%s params=%s",
			provider, model, inputs, attempt, state, errorClass, params)
	}
	parsedParams, _, err := domain.ParseGeneratorParams([]byte(params))
	if err != nil {
		t.Fatal(err)
	}
	if parsedParams.MaxOutputBytes != 1 || parsedParams.Streaming {
		t.Fatalf("synthetic generator params = %+v", parsedParams)
	}
	if purpose == "dialogue" && parsedParams.IsStructured() {
		t.Fatalf("dialogue synthetic params unexpectedly structured: %+v", parsedParams)
	}
	if purpose == "memory_extraction" &&
		(parsedParams.StructuredOutputMode != "json_schema" ||
			parsedParams.SchemaVersion != "memory-extraction-output-schema-v1" || parsedParams.SchemaHash == nil) {
		t.Fatalf("memory synthetic params = %+v, want exact JSON Schema contract", parsedParams)
	}
	if !wantMemoryPolicy.IsZero() && policyRaw != wantMemoryPolicy.String() {
		t.Fatalf("synthetic memory policy = %s, want %s", policyRaw, wantMemoryPolicy)
	}
	return runID
}

func assertCOV2SyntheticDialogueContract(
	t *testing.T,
	fixture applicationFixture,
	runID, wantPipelineID canonical.ID,
	wantPipelineVersion string,
) {
	t.Helper()
	var pipelineRaw, pipelineVersion, promptVersion, contextVersion, renderingVersion string
	if err := fixture.store.Reader().QueryRow(`SELECT run.pipeline_version_id, pipeline.version_key,
		run.prompt_template_version, run.context_policy_version, run.memory_rendering_version
		FROM generation_runs run
		JOIN pipeline_versions pipeline ON pipeline.pipeline_version_id = run.pipeline_version_id
		WHERE run.generation_run_id = ?`, runID.String()).Scan(
		&pipelineRaw, &pipelineVersion, &promptVersion, &contextVersion, &renderingVersion,
	); err != nil {
		t.Fatal(err)
	}
	pipelineID, err := canonical.ParseID(pipelineRaw)
	if err != nil {
		t.Fatal(err)
	}
	want, err := domain.SyntheticDialogueExecutionContract(wantPipelineVersion)
	if err != nil {
		t.Fatal(err)
	}
	got := domain.DialogueExecutionContract{
		PipelineVersionKey:     pipelineVersion,
		PromptTemplateVersion:  promptVersion,
		ContextPolicyVersion:   contextVersion,
		MemoryRenderingVersion: renderingVersion,
	}
	if pipelineID != wantPipelineID || got != want {
		t.Fatalf("synthetic dialogue run %s contract = pipeline %s / %+v, want %s / %+v",
			runID, pipelineID, got, wantPipelineID, want)
	}
	if _, err := domain.ClassifyPersistedDialogueExecutionContract(
		got, domain.DialogueEnvelopeSyntheticNoDispatch,
	); err != nil {
		t.Fatalf("synthetic dialogue run %s persisted an invalid tuple: %v", runID, err)
	}
}

func prepareCommittedDialogueForRecoveryTest(
	t *testing.T,
	fixture applicationFixture,
	event domain.Event,
) canonical.ID {
	t.Helper()
	ctx := context.Background()
	resident, err := fixture.repository.Resident(ctx, fixture.residentID)
	if err != nil {
		t.Fatal(err)
	}
	prepared, err := fixture.application.preparePendingDialogue(ctx, resident, domain.DialogueWork{
		UserEvent: event,
		State:     domain.WorkPending,
	})
	if err != nil {
		t.Fatal(err)
	}
	if prepared.State != domain.WorkRunning || prepared.AttemptNo != 1 {
		t.Fatalf("prepared recovery dialogue = %+v, want committed running attempt 1", prepared)
	}
	return prepared.RunID
}

func prepareMandatoryMemoryRecoveryRun(
	t *testing.T,
	fixture applicationFixture,
	event domain.Event,
	policyID canonical.ID,
) canonical.ID {
	t.Helper()
	work := domain.MemoryExtractionWork{
		SourceEvent: event, IdempotencyKey: domain.MemoryExtractionObligation(event.ID),
		PolicyRevisionID: policyID, State: domain.WorkPending,
	}
	assembly, err := fixture.application.assembleMemoryExtraction(context.Background(), work)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := fixture.application.submitWithContent(context.Background(),
		domain.PrepareGenerationCommand(assembly.Prepare), assembly.Contents); err != nil {
		t.Fatal(err)
	}
	return assembly.Prepare.RunID
}

func failRecoveryAttempt(t *testing.T, fixture applicationFixture, runID canonical.ID, attempt int64, code string) {
	t.Helper()
	outcomeID, err := fixture.application.ids.New()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := fixture.writer.Submit(context.Background(), domain.FailAttemptCommand(domain.FailAttempt{
		Attempt: domain.Attempt{RunID: runID, ResidentID: fixture.residentID, AttemptNo: attempt, OutcomeID: outcomeID},
		State:   "failed", ErrorClass: code,
	})); err != nil {
		t.Fatal(err)
	}
}

func archiveResidentWithoutDrain(t *testing.T, fixture applicationFixture) {
	t.Helper()
	ctx := context.Background()
	state, err := fixture.repository.BootstrapSnapshot(ctx)
	if err != nil {
		t.Fatal(err)
	}
	transitionID, err := fixture.application.ids.New()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := fixture.writer.Submit(ctx, domain.ArchiveResidentCommand(domain.ArchiveResident{
		ResidentID: fixture.residentID, OwnerPrincipalID: state.OwnerPrincipalID,
		StatusTransitionID: transitionID,
	})); err != nil {
		t.Fatal(err)
	}
}

type recoveryOutcomeWant struct {
	attempt int64
	state   string
	code    string
}

func assertRecoveryAttemptHistory(t *testing.T, fixture applicationFixture, runID canonical.ID, want []recoveryOutcomeWant) {
	t.Helper()
	rows, err := fixture.store.Reader().Query(`SELECT attempt_no, state, COALESCE(error_class, '')
		FROM generation_run_outcomes WHERE generation_run_id = ?
		ORDER BY attempt_no, CASE WHEN state = 'running' THEN 0 ELSE 1 END`, runID.String())
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var got []recoveryOutcomeWant
	for rows.Next() {
		var outcome recoveryOutcomeWant
		if err := rows.Scan(&outcome.attempt, &outcome.state, &outcome.code); err != nil {
			t.Fatal(err)
		}
		got = append(got, outcome)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	if len(got) != len(want) {
		t.Fatalf("run %s history = %+v, want %+v", runID, got, want)
	}
	for index := range want {
		if got[index] != want[index] {
			t.Fatalf("run %s history[%d] = %+v, want %+v", runID, index, got[index], want[index])
		}
	}
}

func recoveryRunOutcomeCount(t *testing.T, fixture applicationFixture, runID canonical.ID) int {
	t.Helper()
	var count int
	if err := fixture.store.Reader().QueryRow(`SELECT COUNT(*) FROM generation_run_outcomes
		WHERE generation_run_id = ?`, runID.String()).Scan(&count); err != nil {
		t.Fatal(err)
	}
	return count
}

func latestRecoveryRevisionContentForEvent(
	t *testing.T,
	fixture applicationFixture,
	event domain.Event,
	revisionClass string,
) canonical.ID {
	t.Helper()
	var raw string
	err := fixture.store.Reader().QueryRow(`SELECT revision.content_id
		FROM events source
		JOIN canonical_commits source_commit
		  ON source_commit.canonical_commit_id = source.canonical_commit_id
		JOIN resident_revision_activations activation
		  ON activation.resident_id = source.resident_id
		JOIN canonical_commits activation_commit
		  ON activation_commit.canonical_commit_id = activation.canonical_commit_id
		JOIN resident_revisions revision ON revision.revision_id = activation.revision_id
		WHERE source.event_id = ? AND source.resident_id = ?
		  AND revision.revision_class = ?
		  AND activation_commit.commit_seq <= source_commit.commit_seq
		ORDER BY activation_commit.commit_seq DESC, activation.activation_id DESC LIMIT 1`,
		event.ID.String(), event.ResidentID.String(), revisionClass).Scan(&raw)
	if err != nil {
		t.Fatal(err)
	}
	id, err := canonical.ParseID(raw)
	if err != nil {
		t.Fatal(err)
	}
	return id
}

type overflowMandatoryRecoveryRepository struct {
	work         domain.MandatoryRecoveryWork
	works        []domain.MandatoryRecoveryWork
	running      []domain.RunningAttempt
	runningCalls int
	resolveCalls int
}

func (repository *overflowMandatoryRecoveryRepository) RunningAttempts(context.Context, int) ([]domain.RunningAttempt, error) {
	repository.runningCalls++
	return repository.running, nil
}

func (repository *overflowMandatoryRecoveryRepository) DiscoverMandatoryRecoveryWork(
	context.Context, *domain.MandatoryRecoveryCursor, int,
) ([]domain.MandatoryRecoveryWork, *domain.MandatoryRecoveryCursor, error) {
	if repository.works != nil {
		return repository.works, nil, nil
	}
	return []domain.MandatoryRecoveryWork{repository.work}, nil, nil
}

func (repository *overflowMandatoryRecoveryRepository) ResolveMandatoryCancellationEnvelope(
	context.Context, domain.MandatoryRecoveryWork,
) (domain.CancellationEnvelopeResolution, error) {
	repository.resolveCalls++
	return domain.CancellationEnvelopeResolution{}, nil
}

type unresolvedMandatoryRecoveryRepository struct {
	work domain.MandatoryRecoveryWork
}

type existingMandatoryRecoveryRepository struct {
	work         domain.MandatoryRecoveryWork
	envelope     domain.PrepareGeneration
	resolveCalls int
}

func (*existingMandatoryRecoveryRepository) RunningAttempts(context.Context, int) ([]domain.RunningAttempt, error) {
	return nil, nil
}

func (repository *existingMandatoryRecoveryRepository) DiscoverMandatoryRecoveryWork(
	context.Context, *domain.MandatoryRecoveryCursor, int,
) ([]domain.MandatoryRecoveryWork, *domain.MandatoryRecoveryCursor, error) {
	return []domain.MandatoryRecoveryWork{repository.work}, nil, nil
}

func (repository *existingMandatoryRecoveryRepository) ResolveMandatoryCancellationEnvelope(
	context.Context, domain.MandatoryRecoveryWork,
) (domain.CancellationEnvelopeResolution, error) {
	repository.resolveCalls++
	return domain.CancellationEnvelopeResolution{Generation: repository.envelope}, nil
}

func (*unresolvedMandatoryRecoveryRepository) RunningAttempts(context.Context, int) ([]domain.RunningAttempt, error) {
	return nil, nil
}

func (repository *unresolvedMandatoryRecoveryRepository) DiscoverMandatoryRecoveryWork(
	context.Context, *domain.MandatoryRecoveryCursor, int,
) ([]domain.MandatoryRecoveryWork, *domain.MandatoryRecoveryCursor, error) {
	return []domain.MandatoryRecoveryWork{repository.work}, nil, nil
}

func (repository *unresolvedMandatoryRecoveryRepository) ResolveMandatoryCancellationEnvelope(
	context.Context, domain.MandatoryRecoveryWork,
) (domain.CancellationEnvelopeResolution, error) {
	candidate := integrity.CandidateInput{
		ResidentID: repository.work.SourceEvent.ResidentID,
		Kind:       integrity.FindingProvenanceUnresolvable, RuleCode: integrity.RuleCancellationEnvelopeUnresolvable,
		TargetKind: integrity.TargetEvent, TargetID: repository.work.SourceEvent.ID,
		TargetField: "cancellation_envelope", OccurredAt: repository.work.SourceEvent.RecordedAt,
		OccurredTZ: repository.work.SourceEvent.RecordedTZ,
	}
	return domain.CancellationEnvelopeResolution{Unresolved: &candidate}, nil
}
