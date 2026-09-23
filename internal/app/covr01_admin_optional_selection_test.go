package app

import (
	"context"
	"testing"

	"mahoroba.local/mahoroba/internal/canonical"
	"mahoroba.local/mahoroba/internal/domain"
)

func TestCOVR01AdminPersonaDoesNotPrepareOrCallProviderForActiveUnselectedResidentAfterRestart(t *testing.T) {
	ctx := context.Background()
	fixture, work := personaWorkForTerminalization(t, 2)
	selected := createCOVR01ActiveResident(t, fixture, "covr01-admin-persona-selected")
	if err := fixture.application.SelectResident(ctx, selected); err != nil {
		t.Fatal(err)
	}
	databasePath := fixture.store.Path()
	generator := &scriptedGenerator{steps: []generatorStep{{
		text: `{"contradiction":false,"persona":"friendly, careful, and grounded"}`,
	}}}
	restarted, _ := restartApplicationForEnvelopeTest(t, fixture, generator, "test", "test-model", 64<<10)
	seedCOVR01ForegroundCleanProof(t, restarted, fixture.residentID)

	result, err := restarted.ProposeMemoryPersona(ctx, fixture.residentID)
	if err != nil {
		t.Fatal(err)
	}
	if result.Changed || result.RunID != nil || result.BlockingReason != "resident_unselected" {
		t.Fatalf("unselected persona result = %+v, want provider-free resident_unselected no-op", result)
	}
	if calls := generator.CallCount(); calls != 0 {
		t.Fatalf("unselected persona provider calls = %d, want 0", calls)
	}
	database := openApplicationDatabase(t, databasePath)
	defer database.Close()
	var runs int
	if err := database.QueryRow(`SELECT COUNT(*) FROM generation_runs
		WHERE resident_id = ? AND idempotency_key = ?`, fixture.residentID.String(),
		domain.PersonaRevisionObligation(work.TriggerStageTransitionID)).Scan(&runs); err != nil {
		t.Fatal(err)
	}
	if runs != 0 {
		t.Fatalf("unselected persona prepared runs = %d, want 0", runs)
	}
}

func TestCOVR01AdminDerivationDoesNotPrepareOrCallProviderForActiveUnselectedResidentAfterRestart(t *testing.T) {
	ctx := context.Background()
	fixture, sources, _ := derivationSourcesForTerminalization(t, generatorStep{})
	selected := createCOVR01ActiveResident(t, fixture, "covr01-admin-derivation-selected")
	if err := fixture.application.SelectResident(ctx, selected); err != nil {
		t.Fatal(err)
	}
	databasePath := fixture.store.Path()
	generator := &scriptedGenerator{steps: []generatorStep{{
		text: `{"statement":"The owner enjoys quiet hobbies.","temporal_kind":"stable","version":"memory-derived-output-v1"}`,
	}}}
	restarted, _ := restartApplicationForEnvelopeTest(t, fixture, generator, "test", "test-model", 64<<10)
	seedCOVR01ForegroundCleanProof(t, restarted, fixture.residentID)

	result, err := restarted.DeriveMemoryClaim(
		ctx, fixture.residentID, domain.GenerationPurposeMemoryAbstraction, sources,
	)
	if err != nil {
		t.Fatal(err)
	}
	if !result.RunID.IsZero() || !result.Landing.ClaimID.IsZero() {
		t.Fatalf("unselected derivation result = %+v, want provider-free no-op", result)
	}
	if calls := generator.CallCount(); calls != 0 {
		t.Fatalf("unselected derivation provider calls = %d, want 0", calls)
	}
	database := openApplicationDatabase(t, databasePath)
	defer database.Close()
	var runs int
	if err := database.QueryRow(`SELECT COUNT(*) FROM generation_runs
		WHERE resident_id = ? AND idempotency_key LIKE ?`, fixture.residentID.String(),
		string(domain.GenerationPurposeMemoryAbstraction)+":v1:%").Scan(&runs); err != nil {
		t.Fatal(err)
	}
	if runs != 0 {
		t.Fatalf("unselected derivation prepared runs = %d, want 0", runs)
	}
}

func seedCOVR01ForegroundCleanProof(t *testing.T, application *Application, residentID canonical.ID) {
	t.Helper()
	epoch := application.captureForegroundEpoch(residentID)
	if !application.recordDialogueCleanAtEpoch(residentID, epoch) {
		t.Fatal("failed to seed foreground clean proof")
	}
}
