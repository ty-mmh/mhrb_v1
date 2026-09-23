package app

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"reflect"
	"testing"
	"time"

	"mahoroba.local/mahoroba/internal/canonical"
	"mahoroba.local/mahoroba/internal/domain"
	"mahoroba.local/mahoroba/internal/generation"
	"mahoroba.local/mahoroba/internal/memory"
	"mahoroba.local/mahoroba/internal/projection"
	store "mahoroba.local/mahoroba/internal/store/sqlite"
)

type recallReadyFixture struct {
	applicationFixture
	policyID canonical.ID
	claimID  canonical.ID
	sourceID canonical.ID
}

func TestM5I35RecallInputsMatchPromptIncludedUsagesAndResidentScope(t *testing.T) {
	fixture := newRecallReadyFixture(t)
	_, runID := ingressRecallDialogue(t, fixture, "What do you remember about tea?")

	inputClaims := recallInputClaims(t, fixture, runID)
	usageClaims := recallUsageClaims(t, fixture, runID, memory.UsagePromptIncluded)
	if !reflect.DeepEqual(inputClaims, usageClaims) {
		t.Fatalf("memory_recall inputs = %v, prompt_included usages = %v", inputClaims, usageClaims)
	}
	if len(inputClaims) != 1 || inputClaims[0] != fixture.claimID {
		t.Fatalf("recalled claims = %v, want [%s]", inputClaims, fixture.claimID)
	}

	var inputResident, claimResident, recallResident, generationResident string
	err := fixture.store.Reader().QueryRow(`SELECT content.owner_resident_id, claim.owner_resident_id,
		recall.resident_id, generation.resident_id
		FROM generation_runs generation
		JOIN recall_runs recall ON recall.recall_run_id = generation.recall_run_id
		JOIN generation_run_inputs input ON input.generation_run_id = generation.generation_run_id
		JOIN claims claim ON claim.claim_id = input.source_id
		JOIN content_objects content ON content.content_id = input.content_id
		WHERE generation.generation_run_id = ? AND input.inclusion_mode = 'memory_recall'`,
		runID.String()).Scan(&inputResident, &claimResident, &recallResident, &generationResident)
	if err != nil {
		t.Fatal(err)
	}
	for name, got := range map[string]string{
		"input": inputResident, "claim": claimResident,
		"recall": recallResident, "generation": generationResident,
	} {
		if got != fixture.residentID.String() {
			t.Fatalf("%s resident = %s, want %s", name, got, fixture.residentID)
		}
	}
}

func TestM5I94RecallPersistsActualCandidateSelectionAndPromptInclusion(t *testing.T) {
	fixture := newRecallReadyFixture(t)
	event, runID := ingressRecallDialogue(t, fixture, "Use my relevant memory now")

	counts := make(map[string]int)
	rows, err := fixture.store.Reader().Query(`SELECT usage_type, COUNT(*) FROM claim_usages
		WHERE generation_run_id = ? GROUP BY usage_type`, runID.String())
	if err != nil {
		t.Fatal(err)
	}
	for rows.Next() {
		var kind string
		var count int
		if err := rows.Scan(&kind, &count); err != nil {
			_ = rows.Close()
			t.Fatal(err)
		}
		counts[kind] = count
	}
	if err := rows.Close(); err != nil {
		t.Fatal(err)
	}
	wantCounts := map[string]int{"candidate": 1, "selected": 1, "prompt_included": 1}
	if !reflect.DeepEqual(counts, wantCounts) {
		t.Fatalf("actual Recall usage counts = %v, want %v", counts, wantCounts)
	}

	var recallRaw, query, constraints string
	if err := fixture.store.Reader().QueryRow(`SELECT recall.recall_run_id,
		recall.query_conditions, recall.context_constraints
		FROM generation_runs generation
		JOIN recall_runs recall ON recall.recall_run_id = generation.recall_run_id
		WHERE generation.generation_run_id = ?`, runID.String()).Scan(
		&recallRaw, &query, &constraints,
	); err != nil {
		t.Fatal(err)
	}
	if recallRaw == "" || query == "" || constraints == "" {
		t.Fatalf("incomplete durable Recall provenance: %q %q %q", recallRaw, query, constraints)
	}

	var commitCount int
	if err := fixture.store.Reader().QueryRow(`SELECT COUNT(DISTINCT canonical_commit_id) FROM (
		SELECT canonical_commit_id FROM events WHERE event_id = ?
		UNION ALL SELECT canonical_commit_id FROM generation_runs WHERE generation_run_id = ?
		UNION ALL SELECT canonical_commit_id FROM recall_runs WHERE recall_run_id = ?
		UNION ALL SELECT canonical_commit_id FROM generation_run_inputs WHERE generation_run_id = ?
		UNION ALL SELECT canonical_commit_id FROM claim_usages WHERE generation_run_id = ?
		UNION ALL SELECT canonical_commit_id FROM generation_run_outcomes
		 WHERE generation_run_id = ? AND state = 'running'
	)`, event.ID.String(), runID.String(), recallRaw, runID.String(), runID.String(), runID.String()).Scan(&commitCount); err != nil {
		t.Fatal(err)
	}
	if commitCount != 2 {
		t.Fatalf("two-commit dialogue Canonical commit count = %d, want Commit A plus Commit B", commitCount)
	}
}

func TestM5I32RecallInputInclusionDoesNotMutateClaimState(t *testing.T) {
	fixture := newRecallReadyFixture(t)
	before := readRecallClaimMutationSnapshot(t, fixture)
	_, runID := ingressRecallDialogue(t, fixture, "Recall without changing the memory")
	after := readRecallClaimMutationSnapshot(t, fixture)
	if !reflect.DeepEqual(after, before) {
		t.Fatalf("claim state changed from %+v to %+v", before, after)
	}
	var usages int
	if err := fixture.store.Reader().QueryRow(`SELECT COUNT(*) FROM claim_usages
		WHERE generation_run_id = ?`, runID.String()).Scan(&usages); err != nil {
		t.Fatal(err)
	}
	if usages != 3 {
		t.Fatalf("Recall usages = %d, want candidate/selected/prompt_included", usages)
	}
}

func TestRecallProjectionUnavailableIsReconciledBeforePrepare(t *testing.T) {
	fixture := newApplicationFixture(t, &scriptedGenerator{}, 1)
	if _, err := fixture.application.activateMemoryPolicyV4ForTest(context.Background(), fixture.residentID); err != nil {
		t.Fatal(err)
	}
	event, err := fixture.application.Ingress(context.Background(), "continue without Recall Projection")
	if err != nil {
		t.Fatal(err)
	}
	runID := dialogueRunIDForEventForTest(t, fixture, event)
	assertRecallPreparedRun(t, fixture, runID, "")
}

func TestRecallDisabledV1PreservesM4PromptBytesAndDroppedSummary(t *testing.T) {
	fixture := newApplicationFixture(t, &scriptedGenerator{}, 1)
	resident, err := fixture.repository.Resident(context.Background(), fixture.residentID)
	if err != nil {
		t.Fatal(err)
	}
	const current = "M4 byte parity"
	event, err := fixture.application.Ingress(context.Background(), current)
	if err != nil {
		t.Fatal(err)
	}
	runID := dialogueRunIDForEventForTest(t, fixture, event)
	rows, err := fixture.store.Reader().Query(`SELECT blob.content
		FROM generation_run_inputs input
		JOIN content_objects content ON content.content_id = input.content_id
		JOIN blobs blob ON blob.dedupe_scope_id = content.owner_resident_id
		 AND blob.hash_algorithm = content.blob_hash_algorithm AND blob.blob_hash = content.blob_hash
		WHERE input.generation_run_id = ? ORDER BY input.ordinal`, runID.String())
	if err != nil {
		t.Fatal(err)
	}
	var actual []string
	for rows.Next() {
		var content []byte
		if err := rows.Scan(&content); err != nil {
			_ = rows.Close()
			t.Fatal(err)
		}
		actual = append(actual, string(content))
	}
	if err := rows.Close(); err != nil {
		t.Fatal(err)
	}
	want := []string{
		resident.Principles, resident.Persona, resident.MemoryPolicy,
		"runtime_state=active; memory_recall=disabled; self_talk=disabled", current,
	}
	if !reflect.DeepEqual(actual, want) {
		t.Fatalf("v1 prompt bytes = %#v, want %#v", actual, want)
	}
	var recall sql.NullString
	var dropped string
	if err := fixture.store.Reader().QueryRow(`SELECT recall_run_id, dropped_input_summary
		FROM generation_runs WHERE generation_run_id = ?`, runID.String()).Scan(&recall, &dropped); err != nil {
		t.Fatal(err)
	}
	if recall.Valid || dropped != `{"backfill":"0","live_context":"0"}` {
		t.Fatalf("v1 recall=%+v dropped=%s", recall, dropped)
	}
}

func TestRecallRetryUsesFrozenInputsAndRecallRun(t *testing.T) {
	fixture := newRecallReadyFixture(t)
	_, runID := ingressRecallDialogue(t, fixture, "freeze this Recall envelope")
	before, err := fixture.repository.Generation(context.Background(), runID)
	if err != nil {
		t.Fatal(err)
	}
	failDialogueRunForTest(t, fixture.applicationFixture, runID, "provider_transient")
	outcomeIDs, err := fixture.application.allocateIDs(1)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := fixture.writer.Submit(context.Background(), domain.StartAttemptCommand(domain.Attempt{
		RunID: runID, ResidentID: fixture.residentID, AttemptNo: 2, OutcomeID: outcomeIDs[0],
	})); err != nil {
		t.Fatal(err)
	}
	after, err := fixture.repository.Generation(context.Background(), runID)
	if err != nil {
		t.Fatal(err)
	}
	if before.RecallRunID == nil || after.RecallRunID == nil || *before.RecallRunID != *after.RecallRunID {
		t.Fatalf("retry Recall link changed: before=%v after=%v", before.RecallRunID, after.RecallRunID)
	}
	if !reflect.DeepEqual(before.Inputs, after.Inputs) {
		t.Fatalf("retry recomputed frozen inputs\nbefore=%+v\nafter=%+v", before.Inputs, after.Inputs)
	}
}

func TestRecallAvailableWithZeroCandidatesPersistsCompletedRun(t *testing.T) {
	fixture := newApplicationFixture(t, &scriptedGenerator{}, 1)
	if _, err := fixture.application.activateMemoryPolicyV4ForTest(context.Background(), fixture.residentID); err != nil {
		t.Fatal(err)
	}
	reconcileRecallResident(t, fixture)
	event, err := fixture.application.Ingress(context.Background(), "Recall has no candidates")
	if err != nil {
		t.Fatal(err)
	}
	runID := dialogueRunIDForEventForTest(t, fixture, event)
	var recallRaw string
	if err := fixture.store.Reader().QueryRow(`SELECT recall_run_id FROM generation_runs
		WHERE generation_run_id = ?`, runID.String()).Scan(&recallRaw); err != nil {
		t.Fatal(err)
	}
	var usages int
	if err := fixture.store.Reader().QueryRow(`SELECT COUNT(*) FROM claim_usages
		WHERE recall_run_id = ?`, recallRaw).Scan(&usages); err != nil {
		t.Fatal(err)
	}
	if usages != 0 {
		t.Fatalf("zero-candidate Recall usages = %d, want 0", usages)
	}
}

func TestRecallCommitLagIsReconciledBeforePrepare(t *testing.T) {
	fixture := newRecallReadyFixture(t)
	ingressPendingForTest(t, fixture.applicationFixture, "commit after captured Projection head")
	event, err := fixture.application.Ingress(context.Background(), "continue while Projection catches up")
	if err != nil {
		t.Fatal(err)
	}
	runID := dialogueRunIDForEventForTest(t, fixture.applicationFixture, event)
	assertRecallPreparedRun(t, fixture.applicationFixture, runID, "")
}

func TestRecallAsOfStalenessIsReconciledBeforePrepare(t *testing.T) {
	fixture := newRecallReadyFixture(t)
	fixture.clock.Advance(5*time.Minute + time.Microsecond)
	event, err := fixture.application.Ingress(context.Background(), "continue after freshness horizon")
	if err != nil {
		t.Fatal(err)
	}
	runID := dialogueRunIDForEventForTest(t, fixture.applicationFixture, event)
	assertRecallPreparedRun(t, fixture.applicationFixture, runID, "")
}

func TestRecallProvenanceDedupIsNotRoutedThroughExplicitIngressReferences(t *testing.T) {
	fixture := newRecallReadyFixture(t)
	var before int
	if err := fixture.store.Reader().QueryRow(`SELECT COUNT(*) FROM canonical_commits`).Scan(&before); err != nil {
		t.Fatal(err)
	}
	// Until durable Context References exist, app/HTTP ingress rejects explicit
	// event references before Commit A. Context-v2 provenance dedup is therefore
	// proved at read Assembly/SQLite, where the final Live Context/Backfill set is known.
	_, err := fixture.application.IngressWithMetadata(context.Background(), domain.IngressRequest{
		RawText: "do not duplicate explicit provenance", ExplicitEventIDs: []canonical.ID{fixture.sourceID},
	})
	if !errors.Is(err, domain.ErrInvalidEventReference) {
		t.Fatalf("explicit event reference error = %v", err)
	}
	var after int
	if err := fixture.store.Reader().QueryRow(`SELECT COUNT(*) FROM canonical_commits`).Scan(&after); err != nil {
		t.Fatal(err)
	}
	if after != before {
		t.Fatalf("explicit reference rejection advanced Canonical head: before=%+v after=%+v", before, after)
	}
}

func addRecallSupportForTest(t *testing.T, fixture recallReadyFixture, source domain.Event) {
	t.Helper()
	var createdByRaw, pipelineRaw string
	if err := fixture.store.Reader().QueryRow(`SELECT created_by_run_id FROM claim_evidence
		WHERE claim_id = ? ORDER BY recorded_at, evidence_id LIMIT 1`, fixture.claimID.String()).Scan(&createdByRaw); err != nil {
		t.Fatal(err)
	}
	if err := fixture.store.Reader().QueryRow(`SELECT pipeline_version_id FROM pipeline_versions
		WHERE pipeline_kind = 'memory_maturation' AND version_key = ?`,
		domain.MemoryMaturationPipelineVersion).Scan(&pipelineRaw); err != nil {
		t.Fatal(err)
	}
	createdBy, err := canonical.ParseID(createdByRaw)
	if err != nil {
		t.Fatal(err)
	}
	pipelineID, err := canonical.ParseID(pipelineRaw)
	if err != nil {
		t.Fatal(err)
	}
	ids, err := fixture.application.allocateIDs(3)
	if err != nil {
		t.Fatal(err)
	}
	result, err := fixture.application.submit(context.Background(), domain.AddClaimEvidenceCommand(domain.AddClaimEvidence{
		ResidentID: fixture.residentID, ClaimID: fixture.claimID,
		EvidenceID: ids[0], SourceEventID: source.ID,
		Polarity: memory.PolaritySupport, Grade: memory.GradeStated,
		Derivation: memory.DerivationExtracted, Reason: memory.EvidenceReasonSourceStated,
		MemoryPolicyRevisionID: fixture.policyID, CreatedByRunID: createdBy,
		MaturationPipelineVersionID: pipelineID,
		SedimentStageTransitionID:   ids[1], SettledStageTransitionID: ids[2],
	}))
	if err != nil {
		t.Fatal(err)
	}
	added, ok := result.Value.(domain.AddClaimEvidenceResult)
	if !ok {
		t.Fatalf("AddClaimEvidence result = %T", result.Value)
	}
	if added.FinalStage != memory.StageSediment {
		t.Fatalf("second support final stage = %s, want sediment", added.FinalStage)
	}
}

func TestRecallSelectedCanonicalMismatchIsRebuiltBeforePrepare(t *testing.T) {
	fixture := newRecallReadyFixture(t)
	ctx := context.Background()
	surface := fixture.store.Projection()
	definition := projection.ClaimStatesDefinition()
	watermark, exists, err := surface.Watermark(ctx, definition.Name, fixture.residentID)
	if err != nil || !exists {
		t.Fatalf("claim watermark = %+v exists=%t err=%v", watermark, exists, err)
	}
	var salience float64
	var confidence, currentness, evidenceCount int64
	var temporal string
	var lastReferenced sql.NullInt64
	if err := fixture.store.Reader().QueryRow(`SELECT salience, confidence, currentness,
		temporal_relation, last_referenced_at, evidence_count FROM claim_states
		WHERE claim_id = ? AND resident_id = ?`, fixture.claimID.String(), fixture.residentID.String()).Scan(
		&salience, &confidence, &currentness, &temporal, &lastReferenced, &evidenceCount,
	); err != nil {
		t.Fatal(err)
	}
	confidenceRatio, err := canonical.NewRatio(confidence)
	if err != nil {
		t.Fatal(err)
	}
	currentnessRatio, err := canonical.NewRatio(currentness)
	if err != nil {
		t.Fatal(err)
	}
	state := projection.ClaimState{
		ClaimID: fixture.claimID, Stage: projection.ClaimStageSettled,
		Status: projection.ClaimStatusActive, Salience: salience,
		Confidence: confidenceRatio, Currentness: currentnessRatio,
		TemporalRelation: projection.ClaimTemporalRelation(temporal),
		EvidenceCount:    canonical.Count(evidenceCount),
	}
	if lastReferenced.Valid {
		value := canonical.Instant(lastReferenced.Int64)
		state.LastReferencedAt = &value
	}
	if err := surface.Apply(ctx, projection.ApplyRequest{
		Definition: definition, ResidentID: fixture.residentID, Observed: &watermark,
		Plan: projection.UpdatePlan{
			Kind: projection.FullRebuild, Reason: projection.ReasonManualRebuild,
			NeedCommitCatchUp: true, NeedAsOfReEvaluation: true,
		},
		Evaluation: projection.Evaluation{Value: []projection.ClaimState{state}},
		Watermark:  watermark,
	}); err != nil {
		t.Fatal(err)
	}
	event, err := fixture.application.Ingress(ctx, "fail closed on forged selected state")
	if err != nil {
		t.Fatal(err)
	}
	runID := dialogueRunIDForEventForTest(t, fixture.applicationFixture, event)
	assertRecallPreparedRun(t, fixture.applicationFixture, runID, "")
}

func assertRecallPreparedRun(
	t *testing.T,
	fixture applicationFixture,
	runID canonical.ID,
	reason string,
) {
	t.Helper()
	var recall sql.NullString
	var dropped string
	if err := fixture.store.Reader().QueryRow(`SELECT recall_run_id, dropped_input_summary
		FROM generation_runs WHERE generation_run_id = ?`, runID.String()).Scan(&recall, &dropped); err != nil {
		t.Fatal(err)
	}
	if !recall.Valid {
		t.Fatal("prepared dialogue did not persist its Recall provenance run")
	}
	want := `{"backfill":"0","live_context":"0"}`
	if reason != "" {
		want = fmt.Sprintf(`{"backfill":"0","live_context":"0","memory_recall":"%s"}`, reason)
	}
	if dropped != want {
		t.Fatalf("prepared Recall summary = %s, want %s", dropped, want)
	}
	var queryConditions, constraints string
	if err := fixture.store.Reader().QueryRow(`SELECT query_conditions, context_constraints
		FROM recall_runs WHERE recall_run_id = ?`, recall.String).Scan(&queryConditions, &constraints); err != nil {
		t.Fatal(err)
	}
	if queryConditions == "" || constraints == "" {
		t.Fatalf("prepared Recall provenance is incomplete: query=%q constraints=%q", queryConditions, constraints)
	}
}

type recallClaimMutationSnapshot struct {
	Evidence, Stages, Statuses, Scopes, Relations int
	ProjectedStage, ProjectedStatus, Temporal     string
	Confidence                                    int64
	Salience                                      float64
}

func readRecallClaimMutationSnapshot(t *testing.T, fixture recallReadyFixture) recallClaimMutationSnapshot {
	t.Helper()
	var result recallClaimMutationSnapshot
	queries := []struct {
		query string
		into  *int
	}{
		{`SELECT COUNT(*) FROM claim_evidence WHERE claim_id = ?`, &result.Evidence},
		{`SELECT COUNT(*) FROM claim_stage_transitions WHERE claim_id = ?`, &result.Stages},
		{`SELECT COUNT(*) FROM claim_status_transitions WHERE claim_id = ?`, &result.Statuses},
		{`SELECT COUNT(*) FROM claim_view_scope_assertions WHERE claim_id = ?`, &result.Scopes},
		{`SELECT COUNT(*) FROM claim_relations WHERE from_claim_id = ? OR to_claim_id = ?`, &result.Relations},
	}
	for index, item := range queries {
		args := []any{fixture.claimID.String()}
		if index == len(queries)-1 {
			args = append(args, fixture.claimID.String())
		}
		if err := fixture.store.Reader().QueryRow(item.query, args...).Scan(item.into); err != nil {
			t.Fatal(err)
		}
	}
	if err := fixture.store.Reader().QueryRow(`SELECT stage, status, temporal_relation,
		confidence, salience FROM claim_states WHERE claim_id = ? AND resident_id = ?`,
		fixture.claimID.String(), fixture.residentID.String()).Scan(
		&result.ProjectedStage, &result.ProjectedStatus, &result.Temporal,
		&result.Confidence, &result.Salience,
	); err != nil {
		t.Fatal(err)
	}
	return result
}

func newRecallReadyFixture(t *testing.T) recallReadyFixture {
	t.Helper()
	fixture := newApplicationFixture(t, &scriptedGenerator{}, 1)
	ctx := context.Background()
	activation, err := fixture.application.activateMemoryPolicyV4ForTest(ctx, fixture.residentID)
	if err != nil {
		t.Fatal(err)
	}
	source := ingressPendingForTest(t, fixture, "I like tea")
	claimID := landRecallClaimForTest(t, fixture, activation.RevisionID, source, "Resident likes tea")
	for index := 0; index < int(domain.DialogueLiveEventLimit)-1; index++ {
		ingressPendingForTest(t, fixture, fmt.Sprintf("newer context %d", index))
	}
	reconcileRecallResident(t, fixture)
	return recallReadyFixture{
		applicationFixture: fixture, policyID: activation.RevisionID,
		claimID: claimID, sourceID: source.ID,
	}
}

func landRecallClaimForTest(
	t *testing.T,
	fixture applicationFixture,
	policyID canonical.ID,
	source domain.Event,
	statementText string,
) canonical.ID {
	t.Helper()
	ctx := context.Background()
	_ = dialogueRunIDForEventForTest(t, fixture, source)
	resident, err := fixture.repository.Resident(ctx, fixture.residentID)
	if err != nil {
		t.Fatal(err)
	}
	var pipelineRaw string
	if err := fixture.store.Reader().QueryRow(`SELECT pipeline_version_id FROM pipeline_versions
		WHERE pipeline_kind = 'memory_extraction' AND version_key = ?`,
		domain.MemoryExtractionPipelineVersion).Scan(&pipelineRaw); err != nil {
		t.Fatal(err)
	}
	pipelineID, err := canonical.ParseID(pipelineRaw)
	if err != nil {
		t.Fatal(err)
	}
	var maturationRaw string
	if err := fixture.store.Reader().QueryRow(`SELECT pipeline_version_id FROM pipeline_versions
		WHERE pipeline_kind = 'memory_maturation' AND version_key = ?`,
		domain.MemoryMaturationPipelineVersion).Scan(&maturationRaw); err != nil {
		t.Fatal(err)
	}
	maturationID, err := canonical.ParseID(maturationRaw)
	if err != nil {
		t.Fatal(err)
	}
	schema, err := memory.ExtractionJSONSchema()
	if err != nil {
		t.Fatal(err)
	}
	_, params, err := domain.NewStructuredGeneratorParams(
		false, canonical.ByteSize(64<<10), generation.StructuredOutputPrompt,
		memory.ExtractionOutputSchemaVersionV1, canonical.HashBlob(schema.Bytes()),
	)
	if err != nil {
		t.Fatal(err)
	}
	dropped, err := canonical.MarshalCanonical(struct {
		Dropped canonical.Count `json:"dropped"`
	}{})
	if err != nil {
		t.Fatal(err)
	}
	input, err := fixture.application.newContent(
		fixture.residentID, "generation_input", []byte(source.Content), "independent",
	)
	if err != nil {
		t.Fatal(err)
	}
	prepareIDs, err := fixture.application.allocateIDs(3)
	if err != nil {
		t.Fatal(err)
	}
	sourceID := source.ID
	prepare := domain.PrepareGeneration{
		RunID: prepareIDs[0], ResidentID: fixture.residentID,
		Purpose:        domain.GenerationPurposeMemoryExtraction,
		IdempotencyKey: domain.MemoryExtractionObligation(source.ID),
		Provider:       "test", Model: "test-model", PipelineVersionID: pipelineID,
		PrinciplesRevisionID: resident.PrinciplesRevisionID, PersonaRevisionID: resident.PersonaRevisionID,
		MemoryPolicyRevisionID: policyID, AsOf: source.RecordedAt, AsOfTZ: source.RecordedTZ,
		DroppedInputSummary: dropped, GeneratorParams: params, RunningOutcomeID: prepareIDs[1],
		Inputs: []domain.GenerationInput{{
			ID: prepareIDs[2], Ordinal: 0, Role: string(generation.RoleUser), SourceType: "event",
			SourceID: &sourceID, InclusionMode: "current_input", Content: input,
		}},
	}
	if err := pinGenerationVersions(&prepare); err != nil {
		t.Fatal(err)
	}
	if _, err := fixture.application.submitWithContent(
		ctx, domain.PrepareGenerationCommand(prepare), []domain.Content{input},
	); err != nil {
		t.Fatal(err)
	}

	extraction, err := canonical.MarshalCanonical(memory.ExtractionOutput{
		Version: memory.ExtractionOutputVersionV1,
		Claims: []memory.ExtractionClaim{{
			Statement: statementText, Subject: memory.SelectorResident, Perspective: memory.SelectorResident,
			TemporalKind: memory.TemporalStable, Grade: memory.GradeStated, SourceQuote: source.Content,
		}},
	})
	if err != nil {
		t.Fatal(err)
	}
	output, err := fixture.application.newContent(
		fixture.residentID, "generation_output", extraction.Bytes(), "independent",
	)
	if err != nil {
		t.Fatal(err)
	}
	statement, err := fixture.application.newContent(
		fixture.residentID, "claim_statement", []byte(statementText), "independent",
	)
	if err != nil {
		t.Fatal(err)
	}
	landingIDs, err := fixture.application.allocateIDs(7)
	if err != nil {
		t.Fatal(err)
	}
	land := domain.LandMemoryExtraction{
		Attempt: domain.Attempt{
			RunID: prepare.RunID, ResidentID: fixture.residentID, AttemptNo: 1, OutcomeID: landingIDs[0],
		},
		SourceEventID: source.ID, PipelineVersionID: pipelineID,
		MaturationPipelineVersionID: maturationID, MemoryPolicyRevisionID: policyID,
		Output: output,
		Claims: []domain.ExtractedClaimLanding{{
			ClaimID: landingIDs[1], EvidenceID: landingIDs[2], InitialStageID: landingIDs[3],
			InitialViewScopeID: landingIDs[4], SedimentStageTransitionID: landingIDs[5],
			SettledStageTransitionID: landingIDs[6], Statement: statement,
		}},
	}
	if _, err := fixture.application.submitWithContent(
		ctx, domain.LandMemoryExtractionCommand(land), []domain.Content{output, statement},
	); err != nil {
		t.Fatal(err)
	}
	return landingIDs[1]
}

func reconcileRecallResident(t *testing.T, fixture applicationFixture) {
	t.Helper()
	fixture.clock.Advance(time.Second)
	registry, err := store.ActiveProjectionRegistry()
	if err != nil {
		t.Fatal(err)
	}
	surface := fixture.store.Projection()
	coordinator, err := projection.NewCoordinator(projection.CoordinatorOptions{
		Registry: registry, Source: surface, Store: surface,
		Clock: fixture.clock, Timezone: canonical.MustTimezone("UTC"),
		ScanInterval: time.Hour, AsOfRefreshInterval: time.Hour,
		RebuildRetryInterval: time.Hour, MaxStaleness: 5 * time.Minute,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := coordinator.ReconcileResident(context.Background(), fixture.residentID); err != nil {
		t.Fatal(err)
	}
}

func ingressRecallDialogue(
	t *testing.T,
	fixture recallReadyFixture,
	text string,
) (domain.Event, canonical.ID) {
	t.Helper()
	event, err := fixture.application.Ingress(context.Background(), text)
	if err != nil {
		t.Fatal(err)
	}
	return event, dialogueRunIDForEventForTest(t, fixture.applicationFixture, event)
}

func recallInputClaims(t *testing.T, fixture recallReadyFixture, runID canonical.ID) []canonical.ID {
	t.Helper()
	rows, err := fixture.store.Reader().Query(`SELECT source_id FROM generation_run_inputs
		WHERE generation_run_id = ? AND inclusion_mode = 'memory_recall' ORDER BY ordinal`, runID.String())
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var result []canonical.ID
	for rows.Next() {
		var raw string
		if err := rows.Scan(&raw); err != nil {
			t.Fatal(err)
		}
		id, err := canonical.ParseID(raw)
		if err != nil {
			t.Fatal(err)
		}
		result = append(result, id)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	return result
}

func recallUsageClaims(
	t *testing.T,
	fixture recallReadyFixture,
	runID canonical.ID,
	kind memory.UsageType,
) []canonical.ID {
	t.Helper()
	rows, err := fixture.store.Reader().Query(`SELECT claim_id, ordinal FROM claim_usages
		WHERE generation_run_id = ? AND usage_type = ? ORDER BY ordinal`, runID.String(), string(kind))
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var result []canonical.ID
	for rows.Next() {
		var raw string
		var ordinal int64
		if err := rows.Scan(&raw, &ordinal); err != nil {
			t.Fatal(err)
		}
		if ordinal != int64(len(result)) {
			t.Fatalf("%s usage ordinal = %d, want %d", kind, ordinal, len(result))
		}
		id, err := canonical.ParseID(raw)
		if err != nil {
			t.Fatal(err)
		}
		result = append(result, id)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	return result
}
