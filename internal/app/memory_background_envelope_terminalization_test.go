package app

import (
	"context"
	"errors"
	"testing"

	"mahoroba.local/mahoroba/internal/canonical"
	"mahoroba.local/mahoroba/internal/domain"
	"mahoroba.local/mahoroba/internal/generation"
	"mahoroba.local/mahoroba/internal/memory"
)

const terminalizationExtraction = `{"claims":[` +
	`{"grade":"stated","perspective":"resident","source_quote":"likes tea","statement":"The owner likes tea.","subject":"source_actor","temporal_kind":"stable"},` +
	`{"grade":"stated","perspective":"resident","source_quote":"likes books","statement":"The owner likes books.","subject":"source_actor","temporal_kind":"stable"}` +
	`],"version":"memory-extraction-output-v1"}`

func TestPersonaDurableEnvelopeMismatchTerminalizesEveryExecutableState(t *testing.T) {
	for _, test := range []struct {
		name        string
		prepare     func(*testing.T, applicationFixture, domain.PersonaRevisionWork) canonical.ID
		wantAttempt int64
	}{
		{
			name: "pending prepare then load",
			prepare: func(t *testing.T, fixture applicationFixture, work domain.PersonaRevisionWork) canonical.ID {
				t.Helper()
				base := fixture.application.repository
				fixture.application.repository = &generationLoadHookRepository{
					Repository: base,
					hook: func(runID canonical.ID, call int) {
						if call == 1 {
							tamperGeneratorParams(t, fixture.store.Path(), runID, `{"future":true}`)
						}
					},
				}
				result, err := fixture.application.processPersonaRevisionWork(context.Background(), work)
				if err != nil {
					t.Fatalf("process pending persona: %v", err)
				}
				if result.RunID != nil || fixture.application.generator.(*scriptedGenerator).CallCount() != 6 {
					t.Fatalf("pending persona dispatched provider: result=%+v", result)
				}
				return generationRunIDForKey(t, fixture, domain.PersonaRevisionObligation(work.TriggerStageTransitionID))
			},
			wantAttempt: 1,
		},
		{
			name: "running after restart",
			prepare: func(t *testing.T, fixture applicationFixture, work domain.PersonaRevisionWork) canonical.ID {
				t.Helper()
				runID := preparePersonaRevisionRun(t, fixture, work)
				tamperGeneratorParams(t, fixture.store.Path(), runID,
					mismatchedStructuredParams(t, memory.PersonaOutputSchemaVersionV1))
				restarted, _ := restartApplicationForEnvelopeTest(
					t, fixture, &scriptedGenerator{}, "test", "test-model", 64<<10,
				)
				if err := restarted.ProcessResident(context.Background(), fixture.residentID); err != nil {
					t.Fatalf("restart persona scan: %v", err)
				}
				if restarted.generator.(*scriptedGenerator).CallCount() != 0 {
					t.Fatal("running persona mismatch reached provider")
				}
				return runID
			},
			wantAttempt: 1,
		},
		{
			name: "retry pending after restart",
			prepare: func(t *testing.T, fixture applicationFixture, work domain.PersonaRevisionWork) canonical.ID {
				t.Helper()
				runID := preparePersonaRevisionRun(t, fixture, work)
				failRunningAttemptForTerminalization(t, fixture, runID, 1)
				tamperGeneratorParams(t, fixture.store.Path(), runID,
					mismatchedStructuredParams(t, memory.PersonaOutputSchemaVersionV1))
				restarted, _ := restartApplicationForEnvelopeTest(
					t, fixture, &scriptedGenerator{}, "test", "test-model", 64<<10,
				)
				if err := restarted.ProcessResident(context.Background(), fixture.residentID); err != nil {
					t.Fatalf("restart retry-pending persona scan: %v", err)
				}
				if restarted.generator.(*scriptedGenerator).CallCount() != 0 {
					t.Fatal("retry-pending persona mismatch reached provider")
				}
				return runID
			},
			wantAttempt: 2,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			fixture, work := personaWorkForTerminalization(t, 2)
			runID := test.prepare(t, fixture, work)
			assertBackgroundRunTerminalized(t, fixture.store.Path(), runID, test.wantAttempt)
		})
	}
}

func TestAdminDerivationDurableEnvelopeMismatchTerminalizesPreparedAndRetryPendingRuns(t *testing.T) {
	for _, test := range []struct {
		name        string
		hookCall    int
		derivation  generatorStep
		wantAttempt int64
	}{
		{
			name:        "prepared run load",
			hookCall:    1,
			wantAttempt: 1,
		},
		{
			name:     "retry pending reload",
			hookCall: 2,
			derivation: generatorStep{err: &generation.ProviderError{
				Class: generation.ErrorTransport, Retryable: true, Detail: "retry derivation",
			}},
			wantAttempt: 2,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			fixture, sources, generator := derivationSourcesForTerminalization(t, test.derivation)
			// The helper executes extraction work directly. Establish the Admin
			// precondition explicitly so this test reaches frozen-envelope loading.
			fixture.application.setMemoryExtractionScansComplete(fixture.residentID, true)
			base := fixture.application.repository
			fixture.application.repository = &generationLoadHookRepository{
				Repository: base,
				derivation: fixture.repository,
				hook: func(runID canonical.ID, call int) {
					if call == test.hookCall {
						params := `{"future":true}`
						if call > 1 {
							params = mismatchedStructuredParams(t, memory.DerivedClaimOutputSchemaVersionV1)
						}
						tamperGeneratorParams(t, fixture.store.Path(), runID, params)
					}
				},
			}
			_, err := fixture.application.DeriveMemoryClaim(
				context.Background(), fixture.residentID,
				domain.GenerationPurposeMemoryAbstraction, sources,
			)
			if !errors.Is(err, ErrGenerationEnvelopeUnsupported) {
				t.Fatalf("derivation envelope error=%v", err)
			}
			wantCalls := 4
			if test.hookCall > 1 {
				wantCalls++
			}
			if generator.CallCount() != wantCalls {
				t.Fatalf("provider calls=%d want=%d", generator.CallCount(), wantCalls)
			}
			runID := latestGenerationRunForPurpose(t, fixture, domain.GenerationPurposeMemoryAbstraction)
			assertBackgroundRunTerminalized(t, fixture.store.Path(), runID, test.wantAttempt)
		})
	}
}

type generationLoadHookRepository struct {
	domain.Repository
	derivation domain.MemoryDerivationRepository
	hook       func(canonical.ID, int)
	calls      int
}

func (repository *generationLoadHookRepository) Generation(
	ctx context.Context,
	runID canonical.ID,
) (domain.PreparedGeneration, error) {
	repository.calls++
	if repository.hook != nil {
		repository.hook(runID, repository.calls)
	}
	return repository.Repository.Generation(ctx, runID)
}

func (repository *generationLoadHookRepository) PrepareMemoryDerivation(
	ctx context.Context,
	residentID canonical.ID,
	purpose domain.GenerationPurpose,
	sourceIDs []canonical.ID,
) (domain.DerivedClaimPreparation, error) {
	return repository.derivation.PrepareMemoryDerivation(ctx, residentID, purpose, sourceIDs)
}

func personaWorkForTerminalization(t *testing.T, maxAttempts int) (applicationFixture, domain.PersonaRevisionWork) {
	t.Helper()
	steps := make([]generatorStep, 0, 6)
	for index := 0; index < 3; index++ {
		steps = append(steps, generatorStep{text: "dialogue"}, generatorStep{text: terminalizationExtraction})
	}
	fixture := newApplicationFixture(t, &scriptedGenerator{steps: steps}, maxAttempts)
	ctx := context.Background()
	if _, err := fixture.application.activateMemoryPolicyV4ForTest(ctx, fixture.residentID); err != nil {
		t.Fatal(err)
	}
	for index := 0; index < 3; index++ {
		event, err := fixture.application.Ingress(ctx, "I likes tea and likes books")
		if err != nil {
			t.Fatal(err)
		}
		if err := fixture.application.processPreparedRun(ctx, fixture.residentID, dialogueRunIDForEventForTest(t, fixture, event)); err != nil {
			t.Fatalf("dialogue %d: %v", index, err)
		}
		works, err := discoverMemoryExtractionWorkForTest(ctx, fixture.repository, fixture.residentID, 128, maxAttempts)
		if err != nil {
			t.Fatal(err)
		}
		if len(works) != 1 || works[0].State != domain.WorkPending {
			t.Fatalf("extraction %d work=%+v", index, works)
		}
		seedForegroundCleanProofForTest(t, fixture)
		if err := fixture.application.processMemoryExtractionWork(ctx, works[0]); err != nil {
			t.Fatalf("extraction %d: %v", index, err)
		}
	}
	work, err := fixture.repository.DiscoverPersonaRevisionWork(ctx, fixture.residentID, maxAttempts)
	if err != nil {
		t.Fatal(err)
	}
	if work == nil || work.State != domain.WorkPending {
		t.Fatalf("persona work=%+v want pending", work)
	}
	return fixture, *work
}

func derivationSourcesForTerminalization(
	t *testing.T,
	derivation generatorStep,
) (applicationFixture, []canonical.ID, *scriptedGenerator) {
	t.Helper()
	steps := []generatorStep{
		{text: "dialogue one"}, {text: terminalizationExtraction},
		{text: "dialogue two"}, {text: terminalizationExtraction},
	}
	if derivation.err != nil || derivation.text != "" {
		steps = append(steps, derivation)
	}
	generator := &scriptedGenerator{steps: steps}
	fixture := newApplicationFixture(t, generator, 2)
	ctx := context.Background()
	if _, err := fixture.application.activateMemoryPolicyV4ForTest(ctx, fixture.residentID); err != nil {
		t.Fatal(err)
	}
	for index := 0; index < 2; index++ {
		event, err := fixture.application.Ingress(ctx, "I likes tea and likes books")
		if err != nil {
			t.Fatal(err)
		}
		if err := fixture.application.processPreparedRun(ctx, fixture.residentID, dialogueRunIDForEventForTest(t, fixture, event)); err != nil {
			t.Fatal(err)
		}
		works, err := discoverMemoryExtractionWorkForTest(ctx, fixture.repository, fixture.residentID, 128, 2)
		if err != nil || len(works) != 1 {
			t.Fatalf("derivation extraction work=%+v error=%v", works, err)
		}
		seedForegroundCleanProofForTest(t, fixture)
		if err := fixture.application.processMemoryExtractionWork(ctx, works[0]); err != nil {
			t.Fatal(err)
		}
	}
	database := openApplicationDatabase(t, fixture.store.Path())
	defer database.Close()
	rows, err := database.Query(`SELECT claim_id FROM claims WHERE owner_resident_id = ? ORDER BY claim_id`, fixture.residentID.String())
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var sources []canonical.ID
	for rows.Next() {
		var raw string
		if err := rows.Scan(&raw); err != nil {
			t.Fatal(err)
		}
		id, err := canonical.ParseID(raw)
		if err != nil {
			t.Fatal(err)
		}
		sources = append(sources, id)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	if len(sources) != 2 {
		t.Fatalf("derivation sources=%d want=2", len(sources))
	}
	return fixture, sources, generator
}

func preparePersonaRevisionRun(t *testing.T, fixture applicationFixture, work domain.PersonaRevisionWork) canonical.ID {
	t.Helper()
	assembly, err := fixture.application.assemblePersonaRevision(context.Background(), work)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := fixture.application.submitWithContent(
		context.Background(), domain.PrepareGenerationCommand(assembly.Prepare), assembly.Contents,
	); err != nil {
		t.Fatal(err)
	}
	return assembly.Prepare.RunID
}

func failRunningAttemptForTerminalization(
	t *testing.T,
	fixture applicationFixture,
	runID canonical.ID,
	attempt int64,
) {
	t.Helper()
	outcomeID, err := fixture.application.ids.New()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := fixture.application.submit(context.Background(), domain.FailAttemptCommand(domain.FailAttempt{
		Attempt: domain.Attempt{
			RunID: runID, ResidentID: fixture.residentID, AttemptNo: attempt, OutcomeID: outcomeID,
		},
		State: "failed", ErrorClass: generation.MustOutcomeErrorCode(generation.ErrorTransport, 0).String(),
	})); err != nil {
		t.Fatal(err)
	}
}

func generationRunIDForKey(t *testing.T, fixture applicationFixture, key string) canonical.ID {
	t.Helper()
	database := openApplicationDatabase(t, fixture.store.Path())
	defer database.Close()
	var raw string
	if err := database.QueryRow(`SELECT generation_run_id FROM generation_runs
		WHERE resident_id = ? AND idempotency_key = ?`, fixture.residentID.String(), key).Scan(&raw); err != nil {
		t.Fatal(err)
	}
	id, err := canonical.ParseID(raw)
	if err != nil {
		t.Fatal(err)
	}
	return id
}

func latestGenerationRunForPurpose(
	t *testing.T,
	fixture applicationFixture,
	purpose domain.GenerationPurpose,
) canonical.ID {
	t.Helper()
	database := openApplicationDatabase(t, fixture.store.Path())
	defer database.Close()
	var raw string
	if err := database.QueryRow(`SELECT generation_run_id FROM generation_runs
		WHERE resident_id = ? AND purpose = ? ORDER BY requested_at DESC, generation_run_id DESC LIMIT 1`,
		fixture.residentID.String(), string(purpose)).Scan(&raw); err != nil {
		t.Fatal(err)
	}
	id, err := canonical.ParseID(raw)
	if err != nil {
		t.Fatal(err)
	}
	return id
}

func assertBackgroundRunTerminalized(
	t *testing.T,
	databasePath string,
	runID canonical.ID,
	wantAttempt int64,
) {
	t.Helper()
	database := openApplicationDatabase(t, databasePath)
	defer database.Close()
	var attempt int64
	var state, errorClass string
	if err := database.QueryRow(`SELECT attempt_no, state, COALESCE(error_class, '')
		FROM generation_run_outcomes WHERE generation_run_id = ? ORDER BY outcome_id DESC LIMIT 1`,
		runID.String()).Scan(&attempt, &state, &errorClass); err != nil {
		t.Fatal(err)
	}
	if attempt != wantAttempt || state != "failed" || errorClass != string(generation.ErrorProviderUnsupported) {
		t.Fatalf("terminal outcome=%d/%s/%s want=%d/failed/%s",
			attempt, state, errorClass, wantAttempt, generation.ErrorProviderUnsupported)
	}
	var latestRunning int
	if err := database.QueryRow(`SELECT COUNT(*) FROM generation_run_outcomes outcome
		WHERE outcome.generation_run_id = ? AND outcome.state = 'running'
		  AND NOT EXISTS (
			SELECT 1 FROM generation_run_outcomes newer
			WHERE newer.generation_run_id = outcome.generation_run_id AND newer.outcome_id > outcome.outcome_id
		  )`, runID.String()).Scan(&latestRunning); err != nil {
		t.Fatal(err)
	}
	if latestRunning != 0 {
		t.Fatal("unsupported run remains running")
	}
}

func mismatchedStructuredParams(t *testing.T, schemaVersion string) string {
	t.Helper()
	_, encoded, err := domain.NewStructuredGeneratorParams(
		false, 64<<10, generation.StructuredOutputPrompt,
		schemaVersion, canonical.HashBlob([]byte("deliberately-wrong-schema")),
	)
	if err != nil {
		t.Fatal(err)
	}
	return encoded.String()
}

var _ domain.Repository = (*generationLoadHookRepository)(nil)
var _ domain.MemoryDerivationRepository = (*generationLoadHookRepository)(nil)
