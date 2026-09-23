package app

import (
	"context"
	"database/sql"
	"reflect"
	"testing"
	"time"

	"mahoroba.local/mahoroba/internal/autonomy"
	"mahoroba.local/mahoroba/internal/canonical"
	"mahoroba.local/mahoroba/internal/domain"
	"mahoroba.local/mahoroba/internal/generation"
	"mahoroba.local/mahoroba/internal/memory"
	"mahoroba.local/mahoroba/internal/testsupport"
)

type m6DisabledParityFixture struct {
	applicationFixture
	generator      *scriptedGenerator
	currentEvent   domain.Event
	dialogueRunID  canonical.ID
	personaTrigger canonical.ID
}

type m6DisabledParityInput struct {
	Ordinal       int64
	Role          string
	SourceType    string
	SourceID      string
	InclusionMode string
	Content       string
}

type m6DisabledParityResult struct {
	Requests          []generation.Request
	DialogueInputs    []m6DisabledParityInput
	Counts            map[string]int
	DialogueState     string
	ExtractionState   string
	PersonaState      string
	RecallPromptUsage int
}

func TestM6DisabledAutonomyPreservesM5DialogueGolden(t *testing.T) {
	omitted := newM6DisabledParityFixture(t, false)
	disabled := newM6DisabledParityFixture(t, true)

	for _, fixture := range []*m6DisabledParityFixture{&omitted, &disabled} {
		// V4 keeps the same foreground and mandatory-memory work, but its
		// structured dialogue Recall commit occupies the first fairness quantum.
		// A third clean turn reaches the pre-existing persona recovery work.
		for turn := 0; turn < 3; turn++ {
			if err := fixture.application.ProcessResident(context.Background(), fixture.residentID); err != nil {
				t.Fatal(err)
			}
		}
	}

	want := readM6DisabledParityResult(t, omitted)
	got := readM6DisabledParityResult(t, disabled)
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("explicitly-disabled M6 result differs from omitted autonomy:\ngot=%#v\nwant=%#v", got, want)
	}

	if len(got.Requests) != 2 {
		t.Fatalf("provider calls = %d, want dialogue + mandatory extraction", len(got.Requests))
	}
	wantPurposes := []string{
		string(domain.GenerationPurposeDialogue),
		string(domain.GenerationPurposeMemoryExtraction),
	}
	for index, purpose := range wantPurposes {
		if got.Requests[index].Purpose != purpose {
			t.Fatalf("provider request[%d].purpose = %q, want %q", index, got.Requests[index].Purpose, purpose)
		}
	}
	if got.Counts["recall_runs"] == 0 || got.RecallPromptUsage == 0 ||
		!containsM6DisabledParityInput(got.DialogueInputs, "memory_recall") {
		t.Fatalf("non-zero Recall path was not exercised: counts=%v usages=%d inputs=%+v",
			got.Counts, got.RecallPromptUsage, got.DialogueInputs)
	}
	if got.DialogueState != "succeeded" || got.ExtractionState != "succeeded" || got.PersonaState != "not_run" {
		t.Fatalf("M5 processing states = dialogue:%s extraction:%s persona:%s",
			got.DialogueState, got.ExtractionState, got.PersonaState)
	}
	if got.Counts["claim_evidence"] == 0 || got.Counts["persona_revisions"] != 1 {
		t.Fatalf("memory/persona recovery path remained empty: %v", got.Counts)
	}
	if got.Counts["autonomy_events"] != 0 || got.Counts["autonomy_runs"] != 0 {
		t.Fatalf("disabled autonomy changed M5 state: %v", got.Counts)
	}
}

func newM6DisabledParityFixture(t *testing.T, explicitlyDisabled bool) m6DisabledParityFixture {
	t.Helper()
	fixture, personaWork := personaWorkForTerminalization(t, 2)
	ctx := context.Background()

	resident, err := fixture.repository.Resident(ctx, fixture.residentID)
	if err != nil {
		t.Fatal(err)
	}
	policy, _, err := memory.ParsePolicy([]byte(resident.MemoryPolicy))
	if err != nil {
		t.Fatal(err)
	}
	if policy.Version != memory.PolicyVersionV4 {
		t.Fatalf("active memory policy = %q, want memory-policy-v4", policy.Version)
	}
	if personaWork.State != domain.WorkPending {
		t.Fatalf("persona recovery work = %+v, want pending", personaWork)
	}

	// Move every support event for the settled claims behind the seven-event
	// Live Context window. Process only each dialogue and its mandatory empty
	// extraction directly so the pre-existing persona proposal stays pending.
	advanceM6DisabledParityLiveContext(t, fixture)

	// The settled claims created by personaWorkForTerminalization become a
	// non-empty Recall source only after both production Projections catch up.
	reconcileRecallResident(t, fixture)
	generator := &scriptedGenerator{steps: []generatorStep{
		{text: "unchanged M5 reply"},
		{text: `{"claims":[],"version":"memory-extraction-output-v1"}`},
		{text: `{"contradiction":false,"persona":"friendly"}`},
	}}
	fixture.application.generator = generator
	if explicitlyDisabled {
		policy := autonomy.DefaultPolicy("UTC")
		fixture.application.autonomyPolicy = &policy
		fixture.application.autonomySource = fixture.repository
		fixture.application.autonomyClock = testsupport.NewManualSchedulerClock(fixture.clock.Now())
		fixture.application.projectionMaxStaleness = 5 * time.Minute
	}

	event, err := fixture.application.Ingress(ctx, "M5 parity input likes tea and likes books")
	if err != nil {
		t.Fatal(err)
	}
	runID := dialogueRunIDForEventForTest(t, fixture, event)
	var recallInputs int
	if err := fixture.store.Reader().QueryRow(`SELECT COUNT(*) FROM generation_run_inputs
		WHERE generation_run_id = ? AND inclusion_mode = 'memory_recall'`, runID.String()).Scan(&recallInputs); err != nil {
		t.Fatal(err)
	}
	if recallInputs == 0 {
		t.Fatal("active memory-policy-v4 fixture did not prepare a non-empty Recall input")
	}
	works, err := discoverMemoryExtractionWorkForTest(ctx, fixture.repository, fixture.residentID, 128, 2)
	if err != nil {
		t.Fatal(err)
	}
	if len(works) != 1 || works[0].State != domain.WorkPending || works[0].SourceEvent.ID != event.ID {
		t.Fatalf("mandatory extraction work = %+v, want current event pending", works)
	}
	pendingPersona, err := fixture.repository.DiscoverPersonaRevisionWork(ctx, fixture.residentID, 2)
	if err != nil {
		t.Fatal(err)
	}
	if pendingPersona == nil || pendingPersona.State != domain.WorkPending ||
		pendingPersona.TriggerStageTransitionID != personaWork.TriggerStageTransitionID {
		t.Fatalf("persona recovery work after Recall ingress = %+v, want original pending trigger", pendingPersona)
	}

	return m6DisabledParityFixture{
		applicationFixture: fixture,
		generator:          generator,
		currentEvent:       event,
		dialogueRunID:      runID,
		personaTrigger:     personaWork.TriggerStageTransitionID,
	}
}

func advanceM6DisabledParityLiveContext(t *testing.T, fixture applicationFixture) {
	t.Helper()
	steps := make([]generatorStep, 0, 8)
	for index := 0; index < 4; index++ {
		steps = append(steps,
			generatorStep{text: "context reply"},
			generatorStep{text: `{"claims":[],"version":"memory-extraction-output-v1"}`},
		)
	}
	fixture.application.generator = &scriptedGenerator{steps: steps}
	ctx := context.Background()
	for index := 0; index < 4; index++ {
		event, err := fixture.application.Ingress(ctx, "newer M5 context")
		if err != nil {
			t.Fatal(err)
		}
		if err := fixture.application.processPreparedRun(
			ctx, fixture.residentID, dialogueRunIDForEventForTest(t, fixture, event),
		); err != nil {
			t.Fatal(err)
		}
		works, err := discoverMemoryExtractionWorkForTest(ctx, fixture.repository, fixture.residentID, 128, 2)
		if err != nil {
			t.Fatal(err)
		}
		if len(works) != 1 || works[0].SourceEvent.ID != event.ID {
			t.Fatalf("context extraction[%d] = %+v, want current event", index, works)
		}
		seedForegroundCleanProofForTest(t, fixture)
		if err := fixture.application.processMemoryExtractionWork(ctx, works[0]); err != nil {
			t.Fatal(err)
		}
	}
}

func readM6DisabledParityResult(t *testing.T, fixture m6DisabledParityFixture) m6DisabledParityResult {
	t.Helper()
	result := m6DisabledParityResult{
		Requests: fixture.generator.Requests(),
		Counts:   make(map[string]int),
	}
	rows, err := fixture.store.Reader().Query(`SELECT input.ordinal, input.role, input.source_type,
		COALESCE(input.source_id, ''), input.inclusion_mode, blob.content
		FROM generation_run_inputs input
		JOIN content_objects object ON object.content_id = input.content_id
		JOIN blobs blob ON blob.dedupe_scope_id = object.owner_resident_id
		 AND blob.hash_algorithm = object.blob_hash_algorithm AND blob.blob_hash = object.blob_hash
		WHERE input.generation_run_id = ? ORDER BY input.ordinal`, fixture.dialogueRunID.String())
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	for rows.Next() {
		var input m6DisabledParityInput
		var content []byte
		if err := rows.Scan(&input.Ordinal, &input.Role, &input.SourceType, &input.SourceID,
			&input.InclusionMode, &content); err != nil {
			t.Fatal(err)
		}
		input.Content = string(content)
		result.DialogueInputs = append(result.DialogueInputs, input)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}

	queries := map[string]string{
		"events":             `SELECT COUNT(*) FROM events`,
		"generation_runs":    `SELECT COUNT(*) FROM generation_runs`,
		"generation_inputs":  `SELECT COUNT(*) FROM generation_run_inputs`,
		"generation_outputs": `SELECT COUNT(*) FROM generation_run_outcomes`,
		"recall_runs":        `SELECT COUNT(*) FROM recall_runs`,
		"claim_evidence":     `SELECT COUNT(*) FROM claim_evidence`,
		"persona_revisions":  `SELECT COUNT(*) FROM resident_revisions WHERE revision_class = 'persona'`,
		"autonomy_events":    `SELECT COUNT(*) FROM events WHERE event_type IN ('self_talk','outbound_initiative')`,
		"autonomy_runs":      `SELECT COUNT(*) FROM generation_runs WHERE purpose IN ('self_talk','outbound_initiative')`,
	}
	for label, query := range queries {
		var count int
		if err := fixture.store.Reader().QueryRow(query).Scan(&count); err != nil {
			t.Fatal(err)
		}
		result.Counts[label] = count
	}
	if err := fixture.store.Reader().QueryRow(`SELECT COUNT(*) FROM claim_usages
		WHERE generation_run_id = ? AND usage_type = 'prompt_included'`, fixture.dialogueRunID.String()).Scan(
		&result.RecallPromptUsage,
	); err != nil {
		t.Fatal(err)
	}
	result.DialogueState = latestM6DisabledParityOutcome(t, fixture, fixture.dialogueRunID.String())
	result.ExtractionState = latestM6DisabledParityOutcomeForKey(
		t, fixture, domain.MemoryExtractionObligation(fixture.currentEvent.ID),
	)
	result.PersonaState = latestM6DisabledParityOutcomeForKey(
		t, fixture, domain.PersonaRevisionObligation(fixture.personaTrigger),
	)
	return result
}

func latestM6DisabledParityOutcomeForKey(t *testing.T, fixture m6DisabledParityFixture, key string) string {
	t.Helper()
	var runID string
	err := fixture.store.Reader().QueryRow(`SELECT generation_run_id FROM generation_runs
		WHERE resident_id = ? AND idempotency_key = ?`, fixture.residentID.String(), key).Scan(&runID)
	if err != nil {
		if err == sql.ErrNoRows {
			return "not_run"
		}
		t.Fatal(err)
	}
	return latestM6DisabledParityOutcome(t, fixture, runID)
}

func latestM6DisabledParityOutcome(t *testing.T, fixture m6DisabledParityFixture, runID string) string {
	t.Helper()
	var state string
	if err := fixture.store.Reader().QueryRow(`SELECT state FROM generation_run_outcomes
		WHERE generation_run_id = ? ORDER BY outcome_id DESC LIMIT 1`, runID).Scan(&state); err != nil {
		t.Fatal(err)
	}
	return state
}

func containsM6DisabledParityInput(inputs []m6DisabledParityInput, mode string) bool {
	for _, input := range inputs {
		if input.InclusionMode == mode {
			return true
		}
	}
	return false
}
