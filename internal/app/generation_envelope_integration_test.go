package app

import (
	"context"
	"database/sql"
	"errors"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"mahoroba.local/mahoroba/internal/blob"
	"mahoroba.local/mahoroba/internal/canonical"
	"mahoroba.local/mahoroba/internal/domain"
	"mahoroba.local/mahoroba/internal/generation"
	"mahoroba.local/mahoroba/internal/generation/chatcompletions"
	"mahoroba.local/mahoroba/internal/memory"
	storesqlite "mahoroba.local/mahoroba/internal/store/sqlite"
)

func TestApplicationUsesIdentifiedGeneratorRouteInsteadOfGenericProviderName(t *testing.T) {
	fixture := newApplicationFixture(t, &scriptedGenerator{}, 2)
	adapter, err := chatcompletions.New("https://api.example.test/v1", "never-persist-me", time.Second, 1, nil)
	if err != nil {
		t.Fatal(err)
	}
	identified, err := New(Options{
		Writer: fixture.writer, Repository: fixture.repository, IDs: fixture.application.ids, Clock: fixture.clock,
		Timezone: canonical.MustTimezone("UTC"), Blobs: fixture.blobs, Generator: adapter,
		Provider: "chat-completions", Model: "m", MaxAttempts: 2, RetryBackoff: []time.Duration{0},
		MaxInputBytes: 64 << 10, MaxOutputBytes: 64 << 10, SafetyScanInterval: time.Hour,
	})
	if err != nil {
		t.Fatal(err)
	}
	if identified.provider != adapter.ProviderIdentity() || identified.provider == "chat-completions" {
		t.Fatalf("application provider identity=%q want=%q", identified.provider, adapter.ProviderIdentity())
	}
}

func TestGenerationRestartReplaysPersistedRequestLimits(t *testing.T) {
	ctx := context.Background()
	fixture := newApplicationFixture(t, &scriptedGenerator{}, 2)
	event, err := fixture.application.Ingress(ctx, "persist the exact request")
	if err != nil {
		t.Fatal(err)
	}
	runID := dialogueRunIDForEventForTest(t, fixture, event)
	restartGenerator := &scriptedGenerator{steps: []generatorStep{{text: "replayed"}}}
	restarted, repository := restartApplicationForEnvelopeTest(
		t, fixture, restartGenerator, "test", "new-default-model", 7,
	)

	if err := restarted.Recover(ctx); err != nil {
		t.Fatal(err)
	}
	if err := restarted.ProcessResident(ctx, fixture.residentID); err != nil {
		t.Fatal(err)
	}
	requests := restartGenerator.Requests()
	if len(requests) != 1 {
		t.Fatalf("provider requests=%d want=1", len(requests))
	}
	request := requests[0]
	if request.GenerationRunID != runID.String() || request.Model != "test-model" ||
		request.MaxOutputBytes != 64<<10 || !request.Streaming || request.Purpose != "dialogue" ||
		request.StructuredOutput != nil {
		t.Fatalf("replayed request=%+v", request)
	}
	work, err := discoverDialogueWorkForTest(ctx, repository, fixture.residentID, 10, 2)
	if err != nil {
		t.Fatal(err)
	}
	if len(work) != 1 || work[0].State != domain.WorkSucceeded || work[0].AttemptNo != 2 {
		t.Fatalf("work after replay=%+v", work)
	}
	prepared, err := repository.Generation(ctx, runID)
	if err != nil {
		t.Fatal(err)
	}
	current := domain.CurrentDialogueNormalExecutionContract()
	if prepared.Provider != "test" || prepared.Purpose != domain.GenerationPurposeDialogue ||
		!prepared.GeneratorParams.IsV2() || prepared.GeneratorParams.IsStructured() || prepared.PipelineVersionID.IsZero() ||
		prepared.SessionPolicyID == nil || prepared.PrinciplesRevisionID.IsZero() ||
		prepared.PersonaRevisionID.IsZero() || prepared.MemoryPolicyRevisionID.IsZero() ||
		prepared.CanonicalGeneratorParams.String() !=
			`{"max_output_bytes":"65536","streaming":true,"version":"generator-params-v2"}` ||
		prepared.PipelineVersionKey != current.PipelineVersionKey ||
		prepared.PromptTemplateVersion != current.PromptTemplateVersion ||
		prepared.ContextPolicyVersion != current.ContextPolicyVersion ||
		prepared.MemoryRenderingVersion != current.MemoryRenderingVersion {
		t.Fatalf("loaded durable envelope purpose=%q provider=%q params=%s", prepared.Purpose, prepared.Provider, prepared.CanonicalGeneratorParams.String())
	}
}

func TestLegacyTwoFieldDialogueGenerationRemainsReadableAcrossRestart(t *testing.T) {
	ctx := context.Background()
	fixture := newApplicationFixture(t, &scriptedGenerator{}, 2)
	event, err := fixture.application.Ingress(ctx, "replay a pre-M5 dialogue envelope")
	if err != nil {
		t.Fatal(err)
	}
	runID := dialogueRunIDForEventForTest(t, fixture, event)
	setLegacyDialogueExecutionContractForEnvelopeTest(
		t, fixture, runID,
		`{"max_output_bytes":"65536","streaming":true}`,
	)

	restartGenerator := &scriptedGenerator{steps: []generatorStep{{text: "legacy replayed"}}}
	restarted, repository := restartApplicationForEnvelopeTest(
		t, fixture, restartGenerator, "test", "changed-default-model", 7,
	)
	prepared, err := repository.Generation(ctx, runID)
	if err != nil {
		t.Fatal(err)
	}
	legacy := domain.LegacyDialogueNormalExecutionContract()
	if !prepared.GeneratorParams.IsLegacy() ||
		prepared.CanonicalGeneratorParams.String() != `{"max_output_bytes":"65536","streaming":true}` ||
		prepared.PipelineVersionKey != legacy.PipelineVersionKey ||
		prepared.PromptTemplateVersion != legacy.PromptTemplateVersion ||
		prepared.ContextPolicyVersion != legacy.ContextPolicyVersion ||
		prepared.MemoryRenderingVersion != legacy.MemoryRenderingVersion {
		t.Fatalf("legacy prepared envelope=%+v params=%s", prepared, prepared.CanonicalGeneratorParams.String())
	}
	if err := restarted.Recover(ctx); err != nil {
		t.Fatal(err)
	}
	if err := restarted.ProcessResident(ctx, fixture.residentID); err != nil {
		t.Fatal(err)
	}
	requests := restartGenerator.Requests()
	if len(requests) != 1 || requests[0].Model != "test-model" ||
		requests[0].MaxOutputBytes != 64<<10 || !requests[0].Streaming ||
		requests[0].Purpose != "dialogue" || requests[0].StructuredOutput != nil {
		t.Fatalf("legacy replay requests=%+v", requests)
	}
}

func TestGenerationReadAcceptsCurrentDialogueExecutionContract(t *testing.T) {
	ctx := context.Background()
	fixture := newApplicationFixture(t, &scriptedGenerator{}, 2)
	event, err := fixture.application.Ingress(ctx, "read a current dialogue envelope")
	if err != nil {
		t.Fatal(err)
	}
	runID := dialogueRunIDForEventForTest(t, fixture, event)

	prepared, err := fixture.repository.Generation(ctx, runID)
	if err != nil {
		t.Fatal(err)
	}
	current := domain.CurrentDialogueNormalExecutionContract()
	if prepared.PipelineVersionKey != current.PipelineVersionKey ||
		prepared.PromptTemplateVersion != current.PromptTemplateVersion ||
		prepared.ContextPolicyVersion != current.ContextPolicyVersion ||
		prepared.MemoryRenderingVersion != current.MemoryRenderingVersion {
		t.Fatalf("current prepared execution tuple = %+v", prepared)
	}
	if err := fixture.application.validatePreparedGeneration(prepared); err != nil {
		t.Fatalf("validate current prepared generation: %v", err)
	}
}

func TestPreparedDialogueDispatchAcceptsOnlyExactLegacyAndCurrentTuples(t *testing.T) {
	ctx := context.Background()
	fixture := newApplicationFixture(t, &scriptedGenerator{}, 2)
	event, err := fixture.application.Ingress(ctx, "validate dialogue dispatch tuples")
	if err != nil {
		t.Fatal(err)
	}
	runID := dialogueRunIDForEventForTest(t, fixture, event)
	prepared, err := fixture.repository.Generation(ctx, runID)
	if err != nil {
		t.Fatal(err)
	}
	mixed := domain.CurrentDialogueNormalExecutionContract()
	mixed.MemoryRenderingVersion = domain.MemoryRenderingVersionNoneV1
	for _, test := range []struct {
		name    string
		value   domain.DialogueExecutionContract
		wantErr bool
	}{
		{name: "legacy", value: domain.LegacyDialogueNormalExecutionContract()},
		{name: "current", value: domain.CurrentDialogueNormalExecutionContract()},
		{name: "mixed", value: mixed, wantErr: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			candidate := prepared
			candidate.PipelineVersionKey = test.value.PipelineVersionKey
			candidate.PromptTemplateVersion = test.value.PromptTemplateVersion
			candidate.ContextPolicyVersion = test.value.ContextPolicyVersion
			candidate.MemoryRenderingVersion = test.value.MemoryRenderingVersion
			err := fixture.application.validatePreparedGeneration(candidate)
			if test.wantErr {
				if !errors.Is(err, ErrGenerationEnvelopeUnsupported) ||
					!strings.Contains(err.Error(), "mixed or unsupported dialogue execution contract") {
					t.Fatalf("mixed dispatch error = %v", err)
				}
				return
			}
			if err != nil {
				t.Fatalf("validatePreparedGeneration: %v", err)
			}
		})
	}
}

func TestGenerationReadValidatesPurposeAndParameterVersionTogether(t *testing.T) {
	v2 := structuredGeneratorParamsForEnvelopeTest(t)
	for _, test := range []struct {
		name    string
		purpose string
		params  string
		want    string
	}{
		{name: "unknown purpose", purpose: "future_generation", params: `{"max_output_bytes":"65536","streaming":true}`, want: "unsupported generation purpose"},
		{name: "non-dialogue with legacy params", purpose: "memory_extraction", params: `{"max_output_bytes":"65536","streaming":true}`, want: "requires v2 params"},
		{name: "dialogue with structured v2 params", purpose: "dialogue", params: v2, want: "dialogue purpose requires unstructured"},
	} {
		t.Run(test.name, func(t *testing.T) {
			ctx := context.Background()
			fixture := newApplicationFixture(t, &scriptedGenerator{}, 2)
			event, err := fixture.application.Ingress(ctx, "validate durable purpose and params")
			if err != nil {
				t.Fatal(err)
			}
			runID := dialogueRunIDForEventForTest(t, fixture, event)
			tamperGenerationPurposeAndParams(t, fixture.store.Path(), runID, test.purpose, test.params)

			prepared, err := fixture.repository.Generation(ctx, runID)
			if err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("prepared=%+v error=%v, want containing %q", prepared, err, test.want)
			}
			if string(prepared.Purpose) != test.purpose {
				t.Fatalf("typed purpose=%q want=%q", prepared.Purpose, test.purpose)
			}
		})
	}
}

func TestGenerationReadRejectsChangedExplicitVersionContract(t *testing.T) {
	ctx := context.Background()
	fixture := newApplicationFixture(t, &scriptedGenerator{}, 2)
	event, err := fixture.application.Ingress(ctx, "pin all semantic envelope versions")
	if err != nil {
		t.Fatal(err)
	}
	runID := dialogueRunIDForEventForTest(t, fixture, event)
	tamperGenerationVersionForEnvelopeTest(t, fixture.store.Path(), runID, "context_policy_version", "changed-v2")

	prepared, err := fixture.repository.Generation(ctx, runID)
	if err == nil || !strings.Contains(err.Error(), "generation version contract mismatch") ||
		!strings.Contains(err.Error(), "mixed or unsupported dialogue execution contract") {
		t.Fatalf("prepared=%+v error=%v", prepared, err)
	}
	current := domain.CurrentDialogueNormalExecutionContract()
	if prepared.PipelineVersionKey != current.PipelineVersionKey ||
		prepared.PromptTemplateVersion != current.PromptTemplateVersion ||
		prepared.ContextPolicyVersion != "changed-v2" ||
		prepared.MemoryRenderingVersion != current.MemoryRenderingVersion {
		t.Fatalf("reader did not restore stored versions before validation: %+v", prepared)
	}
}

func TestNonDialoguePreparedGenerationLoadsTypedThenFailsClosedBeforeDispatch(t *testing.T) {
	ctx := context.Background()
	generator := &scriptedGenerator{}
	fixture := newApplicationFixture(t, generator, 2)
	event, err := fixture.application.Ingress(ctx, "do not dispatch memory landing yet")
	if err != nil {
		t.Fatal(err)
	}
	runID := dialogueRunIDForEventForTest(t, fixture, event)
	v2 := structuredGeneratorParamsForEnvelopeTest(t)
	tamperGenerationPurposeAndParams(t, fixture.store.Path(), runID, "memory_extraction", v2)

	prepared, err := fixture.repository.Generation(ctx, runID)
	if err != nil {
		t.Fatal(err)
	}
	if prepared.Purpose != domain.GenerationPurposeMemoryExtraction || !prepared.GeneratorParams.IsV2() ||
		prepared.GeneratorParams.SchemaHash == nil {
		t.Fatalf("typed non-dialogue envelope=%+v", prepared)
	}

	err = fixture.application.processPreparedRun(ctx, fixture.residentID, runID)
	if !errors.Is(err, ErrGenerationEnvelopeUnsupported) || !strings.Contains(err.Error(), "not dispatch-enabled") {
		t.Fatalf("non-dialogue dispatch error=%v", err)
	}
	if generator.CallCount() != 0 {
		t.Fatalf("provider calls=%d want=0", generator.CallCount())
	}
	assertUnsupportedRunTerminal(t, fixture.repository, fixture.store.Path(), runID, fixture.residentID, 1)
}

func TestBackgroundGenerationPrepareAndRetryIgnoreSelectionWithPinnedPolicy(t *testing.T) {
	ctx := context.Background()
	fixture := newApplicationFixture(t, &scriptedGenerator{}, 2)
	event := ingressPendingForTest(t, fixture, "pin the event-time memory policy")
	resident, err := fixture.repository.Resident(ctx, fixture.residentID)
	if err != nil {
		t.Fatal(err)
	}
	works, err := discoverDialogueWorkForTest(ctx, fixture.repository, fixture.residentID, 128, fixture.application.maxAttempts)
	if err != nil {
		t.Fatal(err)
	}
	var dialogueWork domain.DialogueWork
	for _, candidate := range works {
		if candidate.UserEvent.ID == event.ID {
			dialogueWork = candidate
			break
		}
	}
	if dialogueWork.UserEvent.ID.IsZero() {
		t.Fatal("dialogue work was not discovered")
	}
	preparedDialogue, err := fixture.application.preparePendingDialogue(ctx, resident, dialogueWork)
	if err != nil {
		t.Fatal(err)
	}
	pinnedPolicyID := preparedDialogue.MemoryPolicyRevisionID
	prepareIDs, err := fixture.application.allocateIDs(2)
	if err != nil {
		t.Fatal(err)
	}
	dropped, err := canonical.MarshalCanonical(struct {
		MemoryRecall string `json:"memory_recall"`
	}{MemoryRecall: "not_applicable"})
	if err != nil {
		t.Fatal(err)
	}
	assembly := struct {
		Prepare  domain.PrepareGeneration
		Contents []domain.Content
	}{
		Prepare: domain.PrepareGeneration{
			RunID: prepareIDs[0], RunningOutcomeID: prepareIDs[1], ResidentID: preparedDialogue.ResidentID,
			Provider: preparedDialogue.Provider, Model: preparedDialogue.Model,
			PromptTemplateVersion: preparedDialogue.PromptTemplateVersion, ContextPolicyVersion: preparedDialogue.ContextPolicyVersion,
			MemoryRenderingVersion: preparedDialogue.MemoryRenderingVersion, PipelineVersionID: preparedDialogue.PipelineVersionID,
			SessionPolicyID: preparedDialogue.SessionPolicyID, PrinciplesRevisionID: preparedDialogue.PrinciplesRevisionID,
			PersonaRevisionID: preparedDialogue.PersonaRevisionID, MemoryPolicyRevisionID: preparedDialogue.MemoryPolicyRevisionID,
			AsOf: preparedDialogue.AsOf, AsOfTZ: preparedDialogue.AsOfTZ, GeneratorParams: preparedDialogue.CanonicalGeneratorParams,
			DroppedInputSummary: dropped,
		},
	}
	for _, input := range preparedDialogue.Inputs {
		inputIDs, err := fixture.application.allocateIDs(1)
		if err != nil {
			t.Fatal(err)
		}
		content, err := fixture.application.newContent(
			input.Content.ResidentID, input.Content.Class, input.Content.Bytes, input.Content.ErasurePolicy,
		)
		if err != nil {
			t.Fatal(err)
		}
		input.ID = inputIDs[0]
		input.Content = content
		assembly.Prepare.Inputs = append(assembly.Prepare.Inputs, input)
		assembly.Contents = append(assembly.Contents, content)
	}

	activation, err := fixture.application.activateMemoryPolicyV4ForTest(ctx, fixture.residentID)
	if err != nil {
		t.Fatal(err)
	}
	if !activation.Changed || activation.RevisionID == pinnedPolicyID {
		t.Fatalf("memory policy activation=%+v pinned=%s", activation, pinnedPolicyID)
	}
	pipelineID := memoryPipelineIDForEnvelopeTest(t, fixture.store.Path(), "memory_extraction", domain.MemoryExtractionPipelineVersion)
	schema, err := memory.ExtractionJSONSchema()
	if err != nil {
		t.Fatal(err)
	}
	_, params, err := domain.NewStructuredGeneratorParams(
		true, 64<<10, generation.StructuredOutputPrompt,
		memory.ExtractionOutputSchemaVersionV1, canonical.HashBlob(schema.Bytes()),
	)
	if err != nil {
		t.Fatal(err)
	}
	prepare := assembly.Prepare
	prepare.Purpose = domain.GenerationPurposeMemoryExtraction
	prepare.IdempotencyKey = domain.MemoryExtractionObligation(event.ID)
	prepare.PipelineVersionID = pipelineID
	prepare.SessionPolicyID = nil
	prepare.GeneratorParams = params
	if err := pinGenerationVersions(&prepare); err != nil {
		t.Fatal(err)
	}

	clearOperationalSelectionForEnvelopeTest(t, fixture.store.Path())
	if _, err := fixture.application.submitWithContent(
		ctx, domain.PrepareGenerationCommand(prepare), assembly.Contents,
	); err != nil {
		t.Fatalf("unselected background prepare with event-time policy: %v", err)
	}

	database, err := sql.Open("sqlite", fixture.store.Path())
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	var purpose, promptVersion, contextVersion, renderingVersion, policyRaw string
	var sessionPolicy sql.NullString
	if err := database.QueryRowContext(ctx, `SELECT purpose, prompt_template_version,
		context_policy_version, memory_rendering_version, memory_policy_revision_id,
		sessionization_policy_version_id FROM generation_runs WHERE generation_run_id = ?`,
		prepare.RunID.String(),
	).Scan(&purpose, &promptVersion, &contextVersion, &renderingVersion, &policyRaw, &sessionPolicy); err != nil {
		t.Fatal(err)
	}
	if purpose != "memory_extraction" || promptVersion != "memory_extraction-prompt-v1" ||
		contextVersion != "memory_extraction-context-v1" || renderingVersion != "memory-rendering-v1" ||
		policyRaw != pinnedPolicyID.String() || sessionPolicy.Valid {
		t.Fatalf("background generation provenance=%q/%q/%q/%q policy=%q session=%+v",
			purpose, promptVersion, contextVersion, renderingVersion, policyRaw, sessionPolicy)
	}
	prepared, err := fixture.repository.Generation(ctx, prepare.RunID)
	if err != nil {
		t.Fatal(err)
	}
	if prepared.PipelineVersionID != pipelineID || prepared.SessionPolicyID != nil ||
		prepared.PrinciplesRevisionID != prepare.PrinciplesRevisionID ||
		prepared.PersonaRevisionID != prepare.PersonaRevisionID ||
		prepared.MemoryPolicyRevisionID != pinnedPolicyID {
		t.Fatalf("loaded immutable background envelope=%+v", prepared)
	}

	ids, err := fixture.application.allocateIDs(2)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := fixture.application.submit(ctx, domain.FailAttemptCommand(domain.FailAttempt{
		Attempt: domain.Attempt{
			RunID: prepare.RunID, ResidentID: fixture.residentID, AttemptNo: 1, OutcomeID: ids[0],
		},
		State: "failed", ErrorClass: generation.MustOutcomeErrorCode(generation.ErrorTransport, 0).String(),
	})); err != nil {
		t.Fatal(err)
	}
	if _, err := fixture.application.submit(ctx, domain.StartAttemptCommand(domain.Attempt{
		RunID: prepare.RunID, ResidentID: fixture.residentID, AttemptNo: 2, OutcomeID: ids[1],
	})); err != nil {
		t.Fatalf("unselected background retry: %v", err)
	}
}

func TestDialogueRetryStillRequiresOperationalSelection(t *testing.T) {
	ctx := context.Background()
	fixture := newApplicationFixture(t, &scriptedGenerator{}, 2)
	event, err := fixture.application.Ingress(ctx, "dialogue retry stays foreground")
	if err != nil {
		t.Fatal(err)
	}
	runID := dialogueRunIDForEventForTest(t, fixture, event)
	ids, err := fixture.application.allocateIDs(2)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := fixture.application.submit(ctx, domain.FailAttemptCommand(domain.FailAttempt{
		Attempt: domain.Attempt{
			RunID: runID, ResidentID: fixture.residentID, AttemptNo: 1, OutcomeID: ids[0],
		},
		State: "failed", ErrorClass: generation.MustOutcomeErrorCode(generation.ErrorTransport, 0).String(),
	})); err != nil {
		t.Fatal(err)
	}
	clearOperationalSelectionForEnvelopeTest(t, fixture.store.Path())
	if _, err := fixture.application.submit(ctx, domain.StartAttemptCommand(domain.Attempt{
		RunID: runID, ResidentID: fixture.residentID, AttemptNo: 2, OutcomeID: ids[1],
	})); err == nil {
		t.Fatalf("unselected dialogue retry error=%v", err)
	}
}

func structuredGeneratorParamsForEnvelopeTest(t *testing.T) string {
	t.Helper()
	schema, err := memory.ExtractionJSONSchema()
	if err != nil {
		t.Fatal(err)
	}
	_, encoded, err := domain.NewStructuredGeneratorParams(
		true, 64<<10, generation.StructuredOutputPrompt,
		memory.ExtractionOutputSchemaVersionV1, canonical.HashBlob(schema.Bytes()),
	)
	if err != nil {
		t.Fatal(err)
	}
	return encoded.String()
}

func clearOperationalSelectionForEnvelopeTest(t *testing.T, databasePath string) {
	t.Helper()
	database, err := sql.Open("sqlite", databasePath)
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	if _, err := database.Exec(`UPDATE runtime_config SET active_resident_id = NULL WHERE singleton_id = 1`); err != nil {
		t.Fatal(err)
	}
}

func memoryPipelineIDForEnvelopeTest(t *testing.T, databasePath, kind, version string) canonical.ID {
	t.Helper()
	database, err := sql.Open("sqlite", databasePath)
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	var raw string
	if err := database.QueryRow(`SELECT pipeline_version_id FROM pipeline_versions
		WHERE pipeline_kind = ? AND version_key = ?`, kind, version).Scan(&raw); err != nil {
		t.Fatal(err)
	}
	id, err := canonical.ParseID(raw)
	if err != nil {
		t.Fatal(err)
	}
	return id
}

func TestGenerationRestartRejectsChangedProviderEndpointWithoutCallingIt(t *testing.T) {
	ctx := context.Background()
	fixture := newApplicationFixture(t, &scriptedGenerator{}, 2)
	firstAdapter, err := chatcompletions.New("https://first.example.test/v1", "secret-a", time.Second, 1, nil)
	if err != nil {
		t.Fatal(err)
	}
	secondAdapter, err := chatcompletions.New("https://second.example.test/v1", "secret-b", time.Second, 1, nil)
	if err != nil {
		t.Fatal(err)
	}
	// Production derives this field from IdentifiedGenerator. This test swaps
	// the fixture's already-constructed fake identity so it can keep using the
	// deterministic scripted transport while persisting a real adapter identity.
	fixture.application.provider = firstAdapter.ProviderIdentity()
	event, err := fixture.application.Ingress(ctx, "do not reroute me")
	if err != nil {
		t.Fatal(err)
	}
	runID := dialogueRunIDForEventForTest(t, fixture, event)
	restartGenerator := &scriptedGenerator{}
	restarted, repository := restartApplicationForEnvelopeTest(
		t, fixture, restartGenerator, secondAdapter.ProviderIdentity(), "test-model", 64<<10,
	)
	if err := restarted.Recover(ctx); err != nil {
		t.Fatal(err)
	}
	err = restarted.ProcessResident(ctx, fixture.residentID)
	if !errors.Is(err, ErrGenerationEnvelopeUnsupported) || !errors.Is(err, ErrGenerationProviderMismatch) {
		t.Fatalf("provider mismatch error=%v", err)
	}
	if restartGenerator.CallCount() != 0 {
		t.Fatalf("provider calls=%d want=0", restartGenerator.CallCount())
	}
	assertUnsupportedRunTerminal(t, repository, fixture.store.Path(), runID, fixture.residentID, 2)
}

func TestGenerationRestartRejectsTamperedOrUnsupportedRunningEnvelopeWithoutCallingProvider(t *testing.T) {
	for name, params := range map[string]string{
		"unknown parameter": `{"future":true,"max_output_bytes":"65536","streaming":true}`,
		"non-streaming":     `{"max_output_bytes":"65536","streaming":false}`,
	} {
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			fixture := newApplicationFixture(t, &scriptedGenerator{}, 2)
			event, err := fixture.application.Ingress(ctx, "reject unsupported params")
			if err != nil {
				t.Fatal(err)
			}
			runID := dialogueRunIDForEventForTest(t, fixture, event)
			tamperGeneratorParams(t, fixture.store.Path(), runID, params)
			restartGenerator := &scriptedGenerator{}
			restarted, repository := restartApplicationForEnvelopeTest(
				t, fixture, restartGenerator, "test", "test-model", 64<<10,
			)

			// Exercise the just-committed running path directly. Read-time schema
			// rejection and a well-formed but unsupported option both terminalize
			// the known attempt-1 obligation.
			err = restarted.processPreparedRun(ctx, fixture.residentID, runID)
			if !errors.Is(err, ErrGenerationEnvelopeUnsupported) {
				t.Fatalf("unsupported envelope error=%v", err)
			}
			if restartGenerator.CallCount() != 0 {
				t.Fatalf("provider calls=%d want=0", restartGenerator.CallCount())
			}
			assertUnsupportedRunTerminal(t, repository, fixture.store.Path(), runID, fixture.residentID, 1)
		})
	}
}

func restartApplicationForEnvelopeTest(
	t *testing.T,
	fixture applicationFixture,
	generator generation.Generator,
	provider, model string,
	maxOutputBytes int,
) (*Application, *storesqlite.CanonicalRepository) {
	t.Helper()
	ctx := context.Background()
	if err := fixture.writer.Close(ctx); err != nil {
		t.Fatal(err)
	}
	if err := fixture.store.Close(); err != nil {
		t.Fatal(err)
	}
	store, err := storesqlite.Open(ctx, filepath.Join(fixture.root, "mahoroba.db"))
	if err != nil {
		t.Fatal(err)
	}
	ids := canonical.NewSecureIDGenerator()
	writer, err := canonical.OpenWriter(ctx, canonical.WriterOptions{
		Backend: store.Canonical(), IDs: ids, Clock: fixture.clock,
		Timezone: canonical.MustTimezone("UTC"), QueueCapacity: 32,
	})
	if err != nil {
		_ = store.Close()
		t.Fatal(err)
	}
	blobs, err := blob.NewFileStore(filepath.Join(fixture.root, "blobs"))
	if err != nil {
		_ = writer.Close(ctx)
		_ = store.Close()
		t.Fatal(err)
	}
	application, err := New(Options{
		Writer: writer, Repository: store.Canonical(), IDs: ids, Clock: fixture.clock,
		Timezone: canonical.MustTimezone("UTC"), Blobs: blobs, Generator: generator,
		Provider: provider, Model: model, MaxAttempts: 2, RetryBackoff: []time.Duration{0},
		MaxInputBytes: 64 << 10, MaxOutputBytes: maxOutputBytes, SafetyScanInterval: time.Hour,
	})
	if err != nil {
		_ = writer.Close(ctx)
		_ = store.Close()
		t.Fatal(err)
	}
	// A restarted application must retain the COV-1 read-side Assembly and
	// service-required Projection boundary. Existing frozen-run tests do not
	// need it, but Commit-A-only obligations do and must not depend on request
	// memory surviving the restart.
	coordinator := newCOV1ProjectionCoordinator(t, applicationFixture{store: store, clock: fixture.clock})
	application.commitNotifier = coordinator
	application.dialogueReconciler = coordinator
	t.Cleanup(func() {
		_ = writer.Close(context.Background())
		_ = store.Close()
	})
	return application, store.Canonical()
}

func tamperGeneratorParams(t *testing.T, databasePath string, runID canonical.ID, params string) {
	t.Helper()
	database, err := sql.Open("sqlite", databasePath)
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	var triggerSQL string
	if err := database.QueryRow(`SELECT sql FROM sqlite_schema
		WHERE type = 'trigger' AND name = 'trg_generation_runs_no_update'`).Scan(&triggerSQL); err != nil {
		t.Fatal(err)
	}
	tx, err := database.Begin()
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	if _, err := tx.Exec(`DROP TRIGGER trg_generation_runs_no_update`); err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec(`UPDATE generation_runs SET generator_params = ? WHERE generation_run_id = ?`, params, runID.String()); err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec(triggerSQL); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
}

func setLegacyDialogueExecutionContractForEnvelopeTest(
	t *testing.T,
	fixture applicationFixture,
	runID canonical.ID,
	params string,
) {
	t.Helper()
	database, err := sql.Open("sqlite", fixture.store.Path())
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	var pipelineID, triggerSQL string
	err = database.QueryRow(`SELECT pipeline_version_id FROM pipeline_versions
		WHERE pipeline_kind = 'dialogue' AND version_key = ?`,
		domain.DialoguePipelineVersionV1).Scan(&pipelineID)
	if errors.Is(err, sql.ErrNoRows) {
		ids, idErr := fixture.application.allocateIDs(1)
		if idErr != nil {
			t.Fatal(idErr)
		}
		definition, definitionErr := domain.DialoguePipelineDefinition(ids[0], domain.DialoguePipelineVersionV1)
		if definitionErr != nil {
			t.Fatal(definitionErr)
		}
		var commitID, recordedTZ string
		var recordedAt int64
		if queryErr := database.QueryRow(`SELECT canonical_commit_id, recorded_at, recorded_tz
			FROM pipeline_versions WHERE pipeline_kind = 'dialogue' AND version_key = ?`,
			domain.DialoguePipelineVersionV4).Scan(&commitID, &recordedAt, &recordedTZ); queryErr != nil {
			t.Fatal(queryErr)
		}
		if _, insertErr := database.Exec(`INSERT INTO pipeline_versions(
			pipeline_version_id, canonical_commit_id, pipeline_kind, version_key,
			definition, recorded_at, recorded_tz
		) VALUES (?, ?, 'dialogue', ?, ?, ?, ?)`, ids[0].String(), commitID,
			definition.VersionKey, definition.Definition.String(), recordedAt, recordedTZ); insertErr != nil {
			t.Fatal(insertErr)
		}
		pipelineID = ids[0].String()
	} else if err != nil {
		t.Fatal(err)
	}
	if err := database.QueryRow(`SELECT sql FROM sqlite_schema
		WHERE type = 'trigger' AND name = 'trg_generation_runs_no_update'`).Scan(&triggerSQL); err != nil {
		t.Fatal(err)
	}
	tx, err := database.Begin()
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	if _, err := tx.Exec(`DROP TRIGGER trg_generation_runs_no_update`); err != nil {
		t.Fatal(err)
	}
	legacy := domain.LegacyDialogueNormalExecutionContract()
	if _, err := tx.Exec(`UPDATE generation_runs SET generator_params = ?, pipeline_version_id = ?,
		prompt_template_version = ?, context_policy_version = ?, memory_rendering_version = ?
		WHERE generation_run_id = ?`, params, pipelineID, legacy.PromptTemplateVersion,
		legacy.ContextPolicyVersion, legacy.MemoryRenderingVersion, runID.String()); err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec(triggerSQL); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
}

func tamperGenerationPurposeAndParams(
	t *testing.T,
	databasePath string,
	runID canonical.ID,
	purpose, params string,
) {
	t.Helper()
	database, err := sql.Open("sqlite", databasePath)
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	var triggerSQL string
	if err := database.QueryRow(`SELECT sql FROM sqlite_schema
		WHERE type = 'trigger' AND name = 'trg_generation_runs_no_update'`).Scan(&triggerSQL); err != nil {
		t.Fatal(err)
	}
	tx, err := database.Begin()
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	if _, err := tx.Exec(`DROP TRIGGER trg_generation_runs_no_update`); err != nil {
		t.Fatal(err)
	}
	current := domain.CurrentDialogueNormalExecutionContract()
	promptVersion, contextVersion, renderingVersion := current.PromptTemplateVersion,
		current.ContextPolicyVersion, current.MemoryRenderingVersion
	if purpose != string(domain.GenerationPurposeDialogue) {
		promptVersion = purpose + "-prompt-v1"
		contextVersion = purpose + "-context-v1"
		renderingVersion = "memory-rendering-v1"
	}
	if _, err := tx.Exec(`UPDATE generation_runs SET purpose = ?, generator_params = ?,
		prompt_template_version = ?, context_policy_version = ?, memory_rendering_version = ?,
		sessionization_policy_version_id = CASE WHEN ? = 'dialogue'
			THEN sessionization_policy_version_id ELSE NULL END
		WHERE generation_run_id = ?`, purpose, params, promptVersion, contextVersion, renderingVersion, purpose, runID.String()); err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec(triggerSQL); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
}

func tamperGenerationVersionForEnvelopeTest(
	t *testing.T,
	databasePath string,
	runID canonical.ID,
	column, value string,
) {
	t.Helper()
	allowed := map[string]bool{
		"prompt_template_version":  true,
		"context_policy_version":   true,
		"memory_rendering_version": true,
	}
	if !allowed[column] {
		t.Fatalf("unsupported generation version test column %q", column)
	}
	database, err := sql.Open("sqlite", databasePath)
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	var triggerSQL string
	if err := database.QueryRow(`SELECT sql FROM sqlite_schema
		WHERE type = 'trigger' AND name = 'trg_generation_runs_no_update'`).Scan(&triggerSQL); err != nil {
		t.Fatal(err)
	}
	tx, err := database.Begin()
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	if _, err := tx.Exec(`DROP TRIGGER trg_generation_runs_no_update`); err != nil {
		t.Fatal(err)
	}
	query := `UPDATE generation_runs SET ` + column + ` = ? WHERE generation_run_id = ?`
	if _, err := tx.Exec(query, value, runID.String()); err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec(triggerSQL); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
}

func assertUnsupportedRunTerminal(
	t *testing.T,
	repository *storesqlite.CanonicalRepository,
	databasePath string,
	runID, residentID canonical.ID,
	wantAttempt int64,
) {
	t.Helper()
	ctx := context.Background()
	work, err := discoverDialogueWorkForTest(ctx, repository, residentID, 10, 2)
	if err != nil {
		t.Fatal(err)
	}
	if len(work) != 1 || work[0].State != domain.WorkTerminalFailed || work[0].AttemptNo != wantAttempt {
		t.Fatalf("unsupported work=%+v want terminal attempt %d", work, wantAttempt)
	}
	running, err := repository.RunningAttempts(ctx, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(running) != 0 {
		t.Fatalf("unsupported run remains running: %+v", running)
	}
	database, err := sql.Open("sqlite", databasePath)
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	var attempt int64
	var state, errorClass string
	if err := database.QueryRowContext(ctx, `SELECT attempt_no, state, error_class
		FROM generation_run_outcomes WHERE generation_run_id = ? ORDER BY outcome_id DESC LIMIT 1`, runID.String()).Scan(
		&attempt, &state, &errorClass,
	); err != nil {
		t.Fatal(err)
	}
	if attempt != wantAttempt || state != "failed" || errorClass != string(generation.ErrorProviderUnsupported) {
		t.Fatalf("latest unsupported outcome=attempt:%d state:%q class:%q", attempt, state, errorClass)
	}
	prepared, err := repository.Generation(ctx, runID)
	if err == nil && prepared.State != domain.WorkTerminalFailed {
		t.Fatalf("prepared state=%q want terminal", prepared.State)
	}
}
