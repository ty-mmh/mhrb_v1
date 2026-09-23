package app

import (
	"context"
	"testing"

	"mahoroba.local/mahoroba/internal/domain"
)

func TestM5RTI14MandatoryExtractionTerminalizesSuccessFailureOrCancellation(t *testing.T) {
	t.Run("success", func(t *testing.T) {
		fixture := newApplicationFixture(t, &scriptedGenerator{steps: []generatorStep{
			{text: "dialogue"},
			{text: `{"claims":[],"version":"memory-extraction-output-v1"}`},
		}}, 1)
		ctx := context.Background()
		if _, err := fixture.application.activateMemoryPolicyV4ForTest(ctx, fixture.residentID); err != nil {
			t.Fatal(err)
		}
		if _, err := fixture.application.Ingress(ctx, "terminalize successfully"); err != nil {
			t.Fatal(err)
		}
		if err := fixture.application.ProcessResident(ctx, fixture.residentID); err != nil {
			t.Fatal(err)
		}
		assertLatestMandatoryExtractionOutcome(t, fixture, "succeeded", "")
	})

	t.Run("terminal failure", func(t *testing.T) {
		fixture := newApplicationFixture(t, &scriptedGenerator{steps: []generatorStep{
			{text: "dialogue"}, {text: `{"claims":[{"unknown":true}],"version":"memory-extraction-output-v1"}`},
		}}, 1)
		ctx := context.Background()
		if _, err := fixture.application.activateMemoryPolicyV4ForTest(ctx, fixture.residentID); err != nil {
			t.Fatal(err)
		}
		if _, err := fixture.application.Ingress(ctx, "terminalize invalid output"); err != nil {
			t.Fatal(err)
		}
		if err := fixture.application.ProcessResident(ctx, fixture.residentID); err != nil {
			t.Fatal(err)
		}
		assertLatestMandatoryExtractionOutcome(t, fixture, "failed", "provider_invalid_response")
	})

	t.Run("cancellation", func(t *testing.T) {
		fixture := newApplicationFixture(t, &scriptedGenerator{steps: []generatorStep{{text: "dialogue"}}}, 1)
		ctx := context.Background()
		if _, err := fixture.application.activateMemoryPolicyV4ForTest(ctx, fixture.residentID); err != nil {
			t.Fatal(err)
		}
		event, err := fixture.application.Ingress(ctx, "terminalize after archive")
		if err != nil {
			t.Fatal(err)
		}
		dialogueRun := dialogueRunIDForEventForTest(t, fixture, event)
		if err := fixture.application.processPreparedRun(ctx, fixture.residentID, dialogueRun); err != nil {
			t.Fatal(err)
		}
		if err := fixture.application.ArchiveResident(ctx, fixture.residentID); err != nil {
			t.Fatal(err)
		}
		if err := fixture.application.processMemoryExtractionQueue(ctx, fixture.residentID); err != nil {
			t.Fatal(err)
		}
		assertLatestMandatoryExtractionOutcome(t, fixture, "cancelled", "resident_inactive")
	})
}

func assertLatestMandatoryExtractionOutcome(
	t *testing.T,
	fixture applicationFixture,
	wantState, wantErrorClass string,
) {
	t.Helper()
	database := openApplicationDatabase(t, fixture.store.Path())
	defer database.Close()
	var state, errorClass, key string
	if err := database.QueryRow(`SELECT outcome.state, COALESCE(outcome.error_class, ''), run.idempotency_key
		FROM generation_runs run
		JOIN generation_run_outcomes outcome ON outcome.generation_run_id = run.generation_run_id
		WHERE run.resident_id = ? AND run.purpose = 'memory_extraction'
		ORDER BY outcome.outcome_id DESC LIMIT 1`, fixture.residentID.String()).Scan(&state, &errorClass, &key); err != nil {
		t.Fatal(err)
	}
	request, err := domain.ParseMemoryExtractionObligation(key)
	if err != nil || request.Mode != domain.MemoryExtractionMandatory {
		t.Fatalf("mandatory obligation=%q parsed=%+v error=%v", key, request, err)
	}
	if state != wantState || errorClass != wantErrorClass {
		t.Fatalf("mandatory terminal outcome=%s/%s want=%s/%s", state, errorClass, wantState, wantErrorClass)
	}
}
