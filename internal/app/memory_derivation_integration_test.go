package app

import (
	"context"
	"testing"

	"mahoroba.local/mahoroba/internal/canonical"
	"mahoroba.local/mahoroba/internal/domain"
)

func TestM5AdminAbstractionAndDifferentiationUseStructuredAtomicLanding(t *testing.T) {
	ctx := context.Background()
	extractionOne := `{"claims":[{"grade":"stated","perspective":"resident","source_quote":"likes tea","statement":"The owner likes tea.","subject":"source_actor","temporal_kind":"stable"}],"version":"memory-extraction-output-v1"}`
	extractionTwo := `{"claims":[{"grade":"stated","perspective":"resident","source_quote":"likes books","statement":"The owner likes books.","subject":"source_actor","temporal_kind":"stable"}],"version":"memory-extraction-output-v1"}`
	abstraction := `{"statement":"The owner enjoys quiet indoor hobbies.","temporal_kind":"stable","version":"memory-derived-output-v1"}`
	differentiation := `{"statement":"The owner specifically enjoys ceremonial tea.","temporal_kind":"stable","version":"memory-derived-output-v1"}`
	generator := &scriptedGenerator{steps: []generatorStep{
		{text: "dialogue one"}, {text: extractionOne},
		{text: "dialogue two"}, {text: extractionTwo},
		{text: abstraction}, {text: differentiation},
	}}
	fixture := newApplicationFixture(t, generator, 2)
	if _, err := fixture.application.activateMemoryPolicyV4ForTest(ctx, fixture.residentID); err != nil {
		t.Fatal(err)
	}
	for _, text := range []string{"I likes tea", "I likes books"} {
		if _, err := fixture.application.Ingress(ctx, text); err != nil {
			t.Fatal(err)
		}
		if err := fixture.application.ProcessResident(ctx, fixture.residentID); err != nil {
			t.Fatal(err)
		}
	}
	// Each dialogue turn yields after its fair mandatory extraction. Run one
	// clean turn to establish the mandatory-memory completeness proof required
	// by the Admin derivation boundary.
	if err := fixture.application.ProcessResident(ctx, fixture.residentID); err != nil {
		t.Fatal(err)
	}
	database := openApplicationDatabase(t, fixture.store.Path())
	defer database.Close()
	rows, err := database.Query(`SELECT claim_id FROM claims WHERE owner_resident_id = ? ORDER BY recorded_at, claim_id`, fixture.residentID.String())
	if err != nil {
		t.Fatal(err)
	}
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
	rows.Close()
	if len(sources) != 2 {
		t.Fatalf("source claims = %d", len(sources))
	}
	abstracted, err := fixture.application.DeriveMemoryClaim(
		ctx, fixture.residentID, domain.GenerationPurposeMemoryAbstraction, sources,
	)
	if err != nil {
		t.Fatal(err)
	}
	split, err := fixture.application.DeriveMemoryClaim(
		ctx, fixture.residentID, domain.GenerationPurposeMemoryDifferentiation, sources[:1],
	)
	if err != nil {
		t.Fatal(err)
	}
	if abstracted.Landing.ClaimID.IsZero() || split.Landing.ClaimID.IsZero() ||
		len(abstracted.Landing.RelationIDs) != 2 || len(split.Landing.RelationIDs) != 1 {
		t.Fatalf("abstract=%+v split=%+v", abstracted, split)
	}
	requests := generator.Requests()
	for _, request := range requests[len(requests)-2:] {
		if request.Streaming || request.StructuredOutput == nil {
			t.Fatalf("derived request = %+v", request)
		}
		for _, message := range request.Messages {
			if message.Text == "be helpful" {
				t.Fatalf("derived request included principles: %+v", request.Messages)
			}
		}
	}
	var distinctCommits int
	if err := database.QueryRow(`SELECT COUNT(DISTINCT canonical_commit_id) FROM (
		SELECT canonical_commit_id FROM claims WHERE claim_id = ?
		UNION ALL SELECT canonical_commit_id FROM claim_evidence WHERE claim_id = ?
		UNION ALL SELECT canonical_commit_id FROM claim_stage_transitions WHERE claim_id = ?
		UNION ALL SELECT canonical_commit_id FROM claim_view_scope_assertions WHERE claim_id = ?
		UNION ALL SELECT canonical_commit_id FROM claim_relations WHERE from_claim_id = ?
	)`, abstracted.Landing.ClaimID.String(), abstracted.Landing.ClaimID.String(),
		abstracted.Landing.ClaimID.String(), abstracted.Landing.ClaimID.String(),
		abstracted.Landing.ClaimID.String()).Scan(&distinctCommits); err != nil {
		t.Fatal(err)
	}
	if distinctCommits != 1 {
		t.Fatalf("abstracted landing spans %d commits", distinctCommits)
	}
	for _, source := range sources {
		var statusTransitions int
		if err := database.QueryRow(`SELECT COUNT(*) FROM claim_status_transitions WHERE claim_id = ?`, source.String()).Scan(&statusTransitions); err != nil {
			t.Fatal(err)
		}
		if statusTransitions != 0 {
			t.Fatalf("source %s status changed", source)
		}
	}
}
