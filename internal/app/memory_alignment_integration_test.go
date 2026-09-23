package app

import (
	"context"
	"strings"
	"testing"

	"mahoroba.local/mahoroba/internal/canonical"
	"mahoroba.local/mahoroba/internal/domain"
)

func TestMemoryAlignmentProductionQueueSettlesDirectAtomicallyWithoutPrinciplesOrSSE(t *testing.T) {
	ctx := context.Background()
	direct := `{"claims":[{"grade":"stated","perspective":"resident","source_quote":"I am calm","statement":"The resident is calm.","subject":"resident","temporal_kind":"stable"}],"version":"memory-extraction-output-v1"}`
	meta := `{"claims":[{"grade":"stated","perspective":"source_actor","source_quote":"I see calm","statement":"The resident appears calm to the owner.","subject":"resident","temporal_kind":"stable"}],"version":"memory-extraction-output-v1"}`
	alignment := `{"aligned":true,"confidence":"800000","version":"memory-alignment-output-v1"}`
	generator := &scriptedGenerator{steps: []generatorStep{
		{text: "dialogue 1"}, {text: direct},
		{text: "dialogue 2"}, {text: direct},
		{text: "dialogue 3"}, {text: direct},
		{text: "dialogue 4"}, {text: meta},
		{text: "dialogue 5"}, {text: meta},
		{text: alignment, deltas: []string{"not an SSE delta"}},
	}}
	fixture := newApplicationFixture(t, generator, 2)
	if _, err := fixture.application.activateMemoryPolicyV4ForTest(ctx, fixture.residentID); err != nil {
		t.Fatal(err)
	}
	for index, message := range []string{
		"I am calm", "I am calm", "I am calm", "I see calm", "I see calm",
	} {
		if _, err := fixture.application.Ingress(ctx, message); err != nil {
			t.Fatalf("ingress %d: %v", index, err)
		}
		if err := fixture.application.ProcessResident(ctx, fixture.residentID); err != nil {
			t.Fatalf("process %d: %v", index, err)
		}
	}
	// The final dialogue turn spends its single fair quantum on mandatory
	// extraction. A following clean turn is what admits optional alignment.
	for turn := 0; turn < 2; turn++ {
		if err := fixture.application.ProcessResident(ctx, fixture.residentID); err != nil {
			t.Fatalf("process alignment turn %d: %v", turn, err)
		}
	}
	if generator.CallCount() != 11 {
		t.Fatalf("provider calls = %d, want five dialogue + five extraction + one alignment", generator.CallCount())
	}
	requests := generator.Requests()
	alignmentRequest := requests[len(requests)-1]
	if alignmentRequest.Purpose != string(domain.GenerationPurposeMemoryAlignment) || alignmentRequest.Streaming {
		t.Fatalf("alignment request = %+v", alignmentRequest)
	}
	if len(alignmentRequest.Messages) != domain.MemoryAlignmentInputCount {
		t.Fatalf("alignment inputs = %d", len(alignmentRequest.Messages))
	}
	for _, message := range alignmentRequest.Messages {
		if message.Text == "be helpful" || strings.Contains(strings.ToLower(message.Text), "principles") {
			t.Fatalf("alignment request leaked principles: %q", message.Text)
		}
	}

	database := openApplicationDatabase(t, fixture.store.Path())
	defer database.Close()
	var runID, transitionID, dependencyID, outputCommit, transitionCommit, dependencyCommit, directID, metaID string
	if err := database.QueryRow(`SELECT run.generation_run_id, transition.stage_transition_id,
		dependency.stage_transition_dependency_id, outcome.canonical_commit_id,
		transition.canonical_commit_id, dependency.canonical_commit_id,
		transition.claim_id, dependency.dependency_claim_id
		FROM generation_runs run
		JOIN generation_run_outcomes outcome ON outcome.generation_run_id = run.generation_run_id
		 AND outcome.state = 'succeeded'
		JOIN claim_stage_transitions transition ON transition.generation_run_id = run.generation_run_id
		 AND transition.to_stage = 'settled'
		JOIN claim_stage_transition_dependencies dependency
		 ON dependency.stage_transition_id = transition.stage_transition_id
		WHERE run.resident_id = ? AND run.purpose = 'memory_alignment'`, fixture.residentID.String()).Scan(
		&runID, &transitionID, &dependencyID, &outputCommit, &transitionCommit, &dependencyCommit,
		&directID, &metaID,
	); err != nil {
		t.Fatal(err)
	}
	if runID == "" || transitionID == "" || dependencyID == "" || directID == metaID ||
		outputCommit != transitionCommit || outputCommit != dependencyCommit {
		t.Fatalf("alignment landing run=%s transition=%s dependency=%s claims=%s/%s commits=%s/%s/%s",
			runID, transitionID, dependencyID, directID, metaID, outputCommit, transitionCommit, dependencyCommit)
	}
	work, err := fixture.repository.DiscoverMemoryAlignmentWork(ctx, fixture.residentID, 256, 2)
	if err != nil {
		t.Fatal(err)
	}
	if work != nil {
		t.Fatalf("succeeded alignment was rediscovered: %+v", work)
	}
}

func TestMemoryAlignmentProductionReplacementIntentLandsAtomicSupersession(t *testing.T) {
	ctx := context.Background()
	oldDirect := `{"claims":[{"grade":"stated","perspective":"resident","source_quote":"I am calm","statement":"The resident is calm.","subject":"resident","temporal_kind":"stable"}],"version":"memory-extraction-output-v1"}`
	newDirect := `{"claims":[{"grade":"stated","perspective":"resident","source_quote":"I am composed","statement":"The resident is composed.","subject":"resident","temporal_kind":"stable"}],"version":"memory-extraction-output-v1"}`
	meta := `{"claims":[{"grade":"stated","perspective":"source_actor","source_quote":"I see calm","statement":"The resident appears calm to the owner.","subject":"resident","temporal_kind":"stable"}],"version":"memory-extraction-output-v1"}`
	newMeta := `{"claims":[{"grade":"stated","perspective":"source_actor","source_quote":"I see composed","statement":"The resident appears composed to the owner.","subject":"resident","temporal_kind":"stable"}],"version":"memory-extraction-output-v1"}`
	positive := `{"aligned":true,"confidence":"800000","version":"memory-alignment-output-v1"}`
	negative := `{"aligned":false,"confidence":"800000","version":"memory-alignment-output-v1"}`
	generator := &scriptedGenerator{steps: []generatorStep{
		{text: "dialogue 1"}, {text: oldDirect},
		{text: "dialogue 2"}, {text: oldDirect},
		{text: "dialogue 3"}, {text: oldDirect},
		{text: "dialogue 4"}, {text: meta},
		{text: "dialogue 5"}, {text: meta}, {text: positive},
		{text: "dialogue 6"}, {text: newDirect},
		{text: "dialogue 7"}, {text: newDirect},
		{text: "dialogue 8"}, {text: newDirect}, {text: negative},
		{text: "dialogue 9"}, {text: newMeta},
		{text: "dialogue 10"}, {text: newMeta}, {text: positive},
	}}
	fixture := newApplicationFixture(t, generator, 2)
	if _, err := fixture.application.activateMemoryPolicyV4ForTest(ctx, fixture.residentID); err != nil {
		t.Fatal(err)
	}
	process := func(index int, message string) {
		t.Helper()
		if _, err := fixture.application.Ingress(ctx, message); err != nil {
			t.Fatalf("ingress %d: %v", index, err)
		}
		if err := fixture.application.ProcessResident(ctx, fixture.residentID); err != nil {
			t.Fatalf("process %d: %v", index, err)
		}
	}
	for index, message := range []string{"I am calm", "I am calm", "I am calm", "I see calm", "I see calm"} {
		process(index, message)
	}
	for turn := 0; turn < 2; turn++ {
		if err := fixture.application.ProcessResident(ctx, fixture.residentID); err != nil {
			t.Fatalf("process initial alignment turn %d: %v", turn, err)
		}
	}
	database := openApplicationDatabase(t, fixture.store.Path())
	defer database.Close()
	claimIDForStatement := func(statement string) canonical.ID {
		t.Helper()
		var raw string
		if err := database.QueryRow(`SELECT claim.claim_id FROM claims claim
			JOIN content_objects content ON content.content_id = claim.statement_content_id
			JOIN blobs blob ON blob.dedupe_scope_id = content.owner_resident_id
			 AND blob.hash_algorithm = content.blob_hash_algorithm AND blob.blob_hash = content.blob_hash
			WHERE claim.owner_resident_id = ? AND blob.content = ?`, fixture.residentID.String(), []byte(statement)).Scan(&raw); err != nil {
			t.Fatal(err)
		}
		result, err := canonical.ParseID(raw)
		if err != nil {
			t.Fatal(err)
		}
		return result
	}
	oldClaimID := claimIDForStatement("The resident is calm.")
	for index := 0; index < 3; index++ {
		process(index+5, "I am composed")
	}
	for turn := 0; turn < 5; turn++ {
		if err := fixture.application.ProcessResident(ctx, fixture.residentID); err != nil {
			t.Fatalf("process negative alignment turn %d: %v", turn, err)
		}
	}
	newClaimID := claimIDForStatement("The resident is composed.")
	intent, err := fixture.application.CreateMemoryReplacementIntent(ctx, fixture.residentID, newClaimID, oldClaimID)
	if err != nil {
		t.Fatal(err)
	}
	if !intent.Created || intent.NewClaimID != newClaimID || intent.OldClaimID != oldClaimID {
		t.Fatalf("replacement intent = %+v", intent)
	}
	process(8, "I see composed")
	process(9, "I see composed")
	for turn := 0; turn < 5; turn++ {
		if err := fixture.application.ProcessResident(ctx, fixture.residentID); err != nil {
			t.Fatalf("process replacement alignment turn %d: %v", turn, err)
		}
	}
	// The foreground queue has drained through its bounded cycles. Re-publish
	// that exact clean epoch before exercising the same optional worker entry
	// used by the scheduler; this keeps the replacement assertion independent
	// of how many no-work scan pages preceded it.
	seedForegroundCleanProofForTest(t, fixture)
	lock := fixture.application.residentLock(fixture.residentID)
	lock.Lock()
	err = fixture.application.processMemoryAlignmentQueue(ctx, fixture.residentID)
	lock.Unlock()
	if err != nil {
		t.Fatalf("process replacement alignment queue: %v", err)
	}

	var key, outcomeCommit, stageCommit, dependencyCommit, relationCommit, statusCommit string
	var settledClaim, dependencyClaim, relationFrom, relationTo, statusClaim, status, relationType string
	if err := database.QueryRow(`SELECT run.idempotency_key, outcome.canonical_commit_id,
		stage.canonical_commit_id, dependency.canonical_commit_id, relation.canonical_commit_id,
		status.canonical_commit_id, stage.claim_id, dependency.dependency_claim_id,
		relation.from_claim_id, relation.to_claim_id, status.claim_id, status.to_status,
		relation.relation_type
		FROM generation_runs run
		JOIN generation_run_outcomes outcome ON outcome.generation_run_id = run.generation_run_id
		 AND outcome.state = 'succeeded'
		JOIN claim_stage_transitions stage ON stage.generation_run_id = run.generation_run_id
		 AND stage.to_stage = 'settled'
		JOIN claim_stage_transition_dependencies dependency ON dependency.stage_transition_id = stage.stage_transition_id
		JOIN claim_relations relation ON relation.from_claim_id = stage.claim_id
		 AND relation.relation_type = 'supersedes'
		JOIN claim_status_transitions status ON status.trigger_claim_relation_id = relation.claim_relation_id
		WHERE run.resident_id = ? AND run.purpose = 'memory_alignment'
		 AND run.idempotency_key LIKE 'memory_alignment:v2:%'`, fixture.residentID.String()).Scan(
		&key, &outcomeCommit, &stageCommit, &dependencyCommit, &relationCommit, &statusCommit,
		&settledClaim, &dependencyClaim, &relationFrom, &relationTo, &statusClaim, &status, &relationType,
	); err != nil {
		t.Fatal(err)
	}
	identity, err := domain.ParseMemoryAlignmentObligation(key)
	if err != nil {
		t.Fatal(err)
	}
	if identity.Replacement == nil || identity.Replacement.OldClaimID != oldClaimID ||
		identity.Replacement.IntentRelationID != intent.RelationID {
		t.Fatalf("durable replacement identity = %+v, intent=%+v", identity, intent)
	}
	if outcomeCommit != stageCommit || outcomeCommit != dependencyCommit || outcomeCommit != relationCommit ||
		outcomeCommit != statusCommit || settledClaim != newClaimID.String() || relationFrom != newClaimID.String() ||
		relationTo != oldClaimID.String() || statusClaim != oldClaimID.String() || status != "superseded" ||
		relationType != "supersedes" || dependencyClaim == "" {
		t.Fatalf("atomic replacement commits=%s/%s/%s/%s/%s claims=%s/%s/%s->%s status=%s/%s relation=%s",
			outcomeCommit, stageCommit, dependencyCommit, relationCommit, statusCommit,
			settledClaim, dependencyClaim, relationFrom, relationTo, statusClaim, status, relationType)
	}
	if generator.CallCount() != 23 {
		t.Fatalf("provider calls = %d, want 23", generator.CallCount())
	}
}
