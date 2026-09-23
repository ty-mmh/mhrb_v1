package app

import (
	"context"
	"database/sql"
	"reflect"
	"sync"
	"testing"
	"time"

	"mahoroba.local/mahoroba/internal/canonical"
	"mahoroba.local/mahoroba/internal/domain"
	"mahoroba.local/mahoroba/internal/generation"
)

func TestM5AdminReextractUsesCurrentV2AndRequestScopedIdempotency(t *testing.T) {
	ctx := context.Background()
	empty := `{"claims":[],"version":"memory-extraction-output-v1"}`
	generator := &scriptedGenerator{steps: []generatorStep{
		{text: "legacy dialogue"}, {text: empty}, {text: empty},
	}}
	fixture := newApplicationFixture(t, generator, 2)
	event, err := fixture.application.Ingress(ctx, "remember this under legacy policy")
	if err != nil {
		t.Fatal(err)
	}
	if err := fixture.application.ProcessResident(ctx, fixture.residentID); err != nil {
		t.Fatal(err)
	}
	activation, err := fixture.application.activateMemoryPolicyV4ForTest(ctx, fixture.residentID)
	if err != nil {
		t.Fatal(err)
	}
	requestOne := memoryExtractionTestID(t, fixture)
	bypassWork := domain.MemoryExtractionWork{
		SourceEvent: event, IdempotencyKey: domain.MemoryReextractionObligation(event.ID, requestOne),
		PolicyRevisionID: activation.RevisionID, State: domain.WorkPending,
	}
	bypass, err := fixture.application.assembleMemoryExtraction(ctx, bypassWork)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := fixture.application.submitWithContent(
		ctx, domain.PrepareGenerationCommand(bypass.Prepare), bypass.Contents,
	); err == nil {
		t.Fatal("generic generation command bypassed the Admin re-extraction Writer boundary")
	}
	first, err := fixture.application.ReextractMemoryEvent(ctx, fixture.residentID, event.ID, requestOne)
	if err != nil {
		t.Fatal(err)
	}
	if !first.Changed || first.State != domain.WorkSucceeded || generator.CallCount() != 2 {
		t.Fatalf("first re-extraction=%+v calls=%d", first, generator.CallCount())
	}
	retry, err := fixture.application.ReextractMemoryEvent(ctx, fixture.residentID, event.ID, requestOne)
	if err != nil {
		t.Fatal(err)
	}
	if retry.Changed || retry.RunID != first.RunID || retry.State != domain.WorkSucceeded || generator.CallCount() != 2 {
		t.Fatalf("same-request re-extraction=%+v first=%+v calls=%d", retry, first, generator.CallCount())
	}
	requestTwo := memoryExtractionTestID(t, fixture)
	second, err := fixture.application.ReextractMemoryEvent(ctx, fixture.residentID, event.ID, requestTwo)
	if err != nil {
		t.Fatal(err)
	}
	if !second.Changed || second.RunID == first.RunID || second.State != domain.WorkSucceeded || generator.CallCount() != 3 {
		t.Fatalf("second request=%+v first=%+v calls=%d", second, first, generator.CallCount())
	}

	database := openApplicationDatabase(t, fixture.store.Path())
	defer database.Close()
	rows, err := database.Query(`SELECT idempotency_key, memory_policy_revision_id
		FROM generation_runs WHERE resident_id = ? AND purpose = 'memory_extraction'
		ORDER BY requested_at, generation_run_id`, fixture.residentID.String())
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var gotKeys, gotPolicies []string
	for rows.Next() {
		var key, policy string
		if err := rows.Scan(&key, &policy); err != nil {
			t.Fatal(err)
		}
		gotKeys = append(gotKeys, key)
		gotPolicies = append(gotPolicies, policy)
	}
	wantKeys := []string{
		domain.MemoryReextractionObligation(event.ID, requestOne),
		domain.MemoryReextractionObligation(event.ID, requestTwo),
	}
	if !reflect.DeepEqual(gotKeys, wantKeys) {
		t.Fatalf("re-extraction keys=%q want=%q", gotKeys, wantKeys)
	}
	for _, policy := range gotPolicies {
		if policy != activation.RevisionID.String() {
			t.Fatalf("re-extraction policy=%s want current v2 %s", policy, activation.RevisionID)
		}
	}
}

func TestM5AdminReextractInvalidRawIsTerminalAndDurable(t *testing.T) {
	ctx := context.Background()
	const raw = `{"claims":[{"grade":"stated","perspective":"source_actor","source_quote":"missing","statement":"bad","subject":"source_actor","temporal_kind":"stable"}],"version":"memory-extraction-output-v1"}`
	generator := &scriptedGenerator{steps: []generatorStep{{text: "dialogue"}, {text: raw}}}
	fixture := newApplicationFixture(t, generator, 2)
	event, err := fixture.application.Ingress(ctx, "actual source")
	if err != nil {
		t.Fatal(err)
	}
	if err := fixture.application.ProcessResident(ctx, fixture.residentID); err != nil {
		t.Fatal(err)
	}
	if _, err := fixture.application.activateMemoryPolicyV4ForTest(ctx, fixture.residentID); err != nil {
		t.Fatal(err)
	}
	requestID := memoryExtractionTestID(t, fixture)
	result, err := fixture.application.ReextractMemoryEvent(ctx, fixture.residentID, event.ID, requestID)
	if err != nil {
		t.Fatal(err)
	}
	if result.State != domain.WorkTerminalFailed || !result.Changed {
		t.Fatalf("invalid re-extraction result=%+v", result)
	}
	database := openApplicationDatabase(t, fixture.store.Path())
	defer database.Close()
	var state, errorClass, detail string
	if err := database.QueryRow(`SELECT outcome.state, outcome.error_class, blob.content
		FROM generation_runs run
		JOIN generation_run_outcomes outcome ON outcome.generation_run_id = run.generation_run_id
		JOIN content_objects content ON content.content_id = outcome.error_detail_content_id
		JOIN blobs blob ON blob.dedupe_scope_id = content.owner_resident_id
		 AND blob.hash_algorithm = content.blob_hash_algorithm AND blob.blob_hash = content.blob_hash
		WHERE run.resident_id = ? AND run.idempotency_key = ?
		ORDER BY outcome.outcome_id DESC LIMIT 1`, fixture.residentID.String(),
		domain.MemoryReextractionObligation(event.ID, requestID)).Scan(&state, &errorClass, &detail); err != nil {
		t.Fatal(err)
	}
	if state != "failed" || errorClass != "provider_invalid_response" || detail != raw {
		t.Fatalf("invalid durable outcome=%s/%s detail=%q", state, errorClass, detail)
	}
}

func TestM5AdminReextractArchivedResidentCancelsWithoutProvider(t *testing.T) {
	ctx := context.Background()
	generator := &scriptedGenerator{steps: []generatorStep{{text: "legacy dialogue"}}}
	fixture := newApplicationFixture(t, generator, 2)
	event, err := fixture.application.Ingress(ctx, "do not extract after archive")
	if err != nil {
		t.Fatal(err)
	}
	if err := fixture.application.ProcessResident(ctx, fixture.residentID); err != nil {
		t.Fatal(err)
	}
	if _, err := fixture.application.activateMemoryPolicyV4ForTest(ctx, fixture.residentID); err != nil {
		t.Fatal(err)
	}
	if err := fixture.application.ArchiveResident(ctx, fixture.residentID); err != nil {
		t.Fatal(err)
	}
	result, err := fixture.application.ReextractMemoryEvent(
		ctx, fixture.residentID, event.ID, memoryExtractionTestID(t, fixture),
	)
	if err != nil {
		t.Fatal(err)
	}
	if !result.Changed || result.State != domain.WorkTerminalFailed || result.AttemptNo != 0 || generator.CallCount() != 1 {
		t.Fatalf("archived re-extraction=%+v calls=%d", result, generator.CallCount())
	}
	database := openApplicationDatabase(t, fixture.store.Path())
	defer database.Close()
	var state, errorClass string
	if err := database.QueryRow(`SELECT state, error_class FROM generation_run_outcomes
		WHERE generation_run_id = ? ORDER BY outcome_id DESC LIMIT 1`, result.RunID.String()).Scan(&state, &errorClass); err != nil {
		t.Fatal(err)
	}
	if state != "cancelled" || errorClass != "resident_inactive" {
		t.Fatalf("archived durable outcome=%s/%s", state, errorClass)
	}
}

func TestM5AdminReextractErasedSourceCancelsWithoutProvider(t *testing.T) {
	ctx := context.Background()
	generator := &scriptedGenerator{steps: []generatorStep{{text: "legacy dialogue"}}}
	fixture := newApplicationFixture(t, generator, 2)
	event, err := fixture.application.Ingress(ctx, "erase before Admin extraction")
	if err != nil {
		t.Fatal(err)
	}
	if err := fixture.application.ProcessResident(ctx, fixture.residentID); err != nil {
		t.Fatal(err)
	}
	if _, err := fixture.application.activateMemoryPolicyV4ForTest(ctx, fixture.residentID); err != nil {
		t.Fatal(err)
	}
	eraseEventContentForTest(t, fixture.store.Path(), event.ContentID)
	result, err := fixture.application.ReextractMemoryEvent(
		ctx, fixture.residentID, event.ID, memoryExtractionTestID(t, fixture),
	)
	if err != nil {
		t.Fatal(err)
	}
	if !result.Changed || result.State != domain.WorkTerminalFailed || result.AttemptNo != 0 || generator.CallCount() != 1 {
		t.Fatalf("erased-source re-extraction=%+v calls=%d", result, generator.CallCount())
	}
	database := openApplicationDatabase(t, fixture.store.Path())
	defer database.Close()
	var state, errorClass string
	if err := database.QueryRow(`SELECT state, error_class FROM generation_run_outcomes
		WHERE generation_run_id = ? ORDER BY outcome_id DESC LIMIT 1`, result.RunID.String()).Scan(&state, &errorClass); err != nil {
		t.Fatal(err)
	}
	if state != "cancelled" || errorClass != "source_content_erased" {
		t.Fatalf("erased-source durable outcome=%s/%s", state, errorClass)
	}
}

func TestM7MemoryExtractionErasedAfterRetryableFailureCancelsNextAttemptExactlyOnce(t *testing.T) {
	ctx := context.Background()
	fixture := newApplicationFixture(t, &scriptedGenerator{steps: []generatorStep{{text: "dialogue"}}}, 2)
	event, err := fixture.application.Ingress(ctx, "erase after extraction provider timeout")
	if err != nil {
		t.Fatal(err)
	}
	if err := fixture.application.ProcessResident(ctx, fixture.residentID); err != nil {
		t.Fatal(err)
	}
	if _, err := fixture.application.activateMemoryPolicyV4ForTest(ctx, fixture.residentID); err != nil {
		t.Fatal(err)
	}
	fixture.application.generator = &scriptedGenerator{steps: []generatorStep{{
		err: &generation.ProviderError{Class: generation.ErrorTimeout, Detail: "retryable extraction timeout"},
	}}}
	requestID := memoryExtractionTestID(t, fixture)
	first, err := fixture.application.ReextractMemoryEvent(ctx, fixture.residentID, event.ID, requestID)
	if err != nil {
		t.Fatal(err)
	}
	if first.State != domain.WorkRetryPending || first.AttemptNo != 1 {
		t.Fatalf("retryable first extraction = %+v", first)
	}
	eraseEventContentForTest(t, fixture.store.Path(), event.ContentID)
	second, err := fixture.application.ReextractMemoryEvent(ctx, fixture.residentID, event.ID, requestID)
	if err != nil {
		t.Fatal(err)
	}
	if second.State != domain.WorkTerminalFailed || second.AttemptNo != 2 || second.Changed {
		t.Fatalf("erased retry cancellation = %+v", second)
	}
	replay, err := fixture.application.ReextractMemoryEvent(ctx, fixture.residentID, event.ID, requestID)
	if err != nil {
		t.Fatal(err)
	}
	if replay.State != domain.WorkTerminalFailed || replay.AttemptNo != 2 || replay.Changed {
		t.Fatalf("erased retry cancellation replay = %+v", replay)
	}
	var rows, runningTwo, cancelledTwo int
	if err := fixture.store.Reader().QueryRow(`SELECT COUNT(*),
		SUM(CASE WHEN attempt_no = 2 AND state = 'running' THEN 1 ELSE 0 END),
		SUM(CASE WHEN attempt_no = 2 AND state = 'cancelled' AND error_class = 'source_content_erased' THEN 1 ELSE 0 END)
		FROM generation_run_outcomes WHERE generation_run_id = ?`, second.RunID.String()).Scan(
		&rows, &runningTwo, &cancelledTwo,
	); err != nil {
		t.Fatal(err)
	}
	if rows != 4 || runningTwo != 1 || cancelledTwo != 1 {
		t.Fatalf("memory retry cancellation rows/running2/cancelled2 = %d/%d/%d", rows, runningTwo, cancelledTwo)
	}
}

func TestCOVR02AdminReextractInvalidatesOptionalCompletenessUntilFreshScan(t *testing.T) {
	ctx := context.Background()
	fixture := newApplicationFixture(t, &scriptedGenerator{steps: []generatorStep{{text: "dialogue"}}}, 2)
	event, err := fixture.application.Ingress(ctx, "re-extraction must invalidate optional completeness")
	if err != nil {
		t.Fatal(err)
	}
	if err := fixture.application.ProcessResident(ctx, fixture.residentID); err != nil {
		t.Fatal(err)
	}
	if _, err := fixture.application.activateMemoryPolicyV4ForTest(ctx, fixture.residentID); err != nil {
		t.Fatal(err)
	}
	fixture.application.setMemoryExtractionScansComplete(fixture.residentID, true)
	fixture.application.generator = &scriptedGenerator{steps: []generatorStep{{
		err: &generation.ProviderError{Class: generation.ErrorTimeout, Detail: "retryable re-extraction timeout"},
	}}}
	requestID := memoryExtractionTestID(t, fixture)
	result, err := fixture.application.ReextractMemoryEvent(ctx, fixture.residentID, event.ID, requestID)
	if err != nil {
		t.Fatal(err)
	}
	if result.State != domain.WorkRetryPending {
		t.Fatalf("retryable re-extraction = %+v, want retry_pending", result)
	}
	if fixture.application.memoryExtractionScansAreComplete(fixture.residentID) {
		t.Fatal("retry-pending Admin re-extraction retained an old optional completeness proof")
	}
	fixture.application.memoryStateMu.Lock()
	rescanPending := fixture.application.memoryReextractionRescanPending[fixture.residentID]
	fixture.application.memoryStateMu.Unlock()
	if !rescanPending {
		t.Fatal("Admin re-extraction admitted beyond a prior ceiling did not request a fresh scan")
	}

	fixture.application.generator = &scriptedGenerator{steps: []generatorStep{
		{text: covr01EmptyExtraction}, {text: covr01EmptyExtraction},
		{text: covr01EmptyExtraction}, {text: covr01EmptyExtraction},
	}}
	for pass := 0; pass < 12 && !fixture.application.memoryExtractionScansAreComplete(fixture.residentID); pass++ {
		if err := fixture.application.ProcessResident(ctx, fixture.residentID); err != nil {
			t.Fatal(err)
		}
	}
	if !fixture.application.memoryExtractionScansAreComplete(fixture.residentID) {
		t.Fatal("fresh normal and re-extraction verification did not restore completeness")
	}
}

func TestCOVR01AdminReextractDoesNotCallProviderForActiveUnselectedResident(t *testing.T) {
	ctx := context.Background()
	generator := &scriptedGenerator{steps: []generatorStep{{text: covr01EmptyExtraction}}}
	fixture := newApplicationFixture(t, generator, 1)
	if _, err := fixture.application.activateMemoryPolicyV4ForTest(ctx, fixture.residentID); err != nil {
		t.Fatal(err)
	}
	event, err := fixture.application.Ingress(ctx, "Admin re-extraction remains idle while unselected")
	if err != nil {
		t.Fatal(err)
	}
	selected := createCOVR01ActiveResident(t, fixture, "covr01-admin-reextract-selected")
	if err := fixture.application.SelectResident(ctx, selected); err != nil {
		t.Fatal(err)
	}
	requestID := memoryExtractionTestID(t, fixture)
	result, err := fixture.application.ReextractMemoryEvent(ctx, fixture.residentID, event.ID, requestID)
	if err != nil {
		t.Fatal(err)
	}
	if result.Changed || generator.CallCount() != 0 {
		t.Fatalf("unselected Admin re-extraction = %+v calls=%d, want unchanged/no provider", result, generator.CallCount())
	}
	if runs := covr01RunCount(t, fixture, domain.MemoryReextractionObligation(event.ID, requestID)); runs != 0 {
		t.Fatalf("unselected Admin re-extraction runs = %d, want 0", runs)
	}

	if err := fixture.application.SelectResident(ctx, fixture.residentID); err != nil {
		t.Fatal(err)
	}
	seedForegroundCleanProofForTest(t, fixture)
	result, err = fixture.application.ReextractMemoryEvent(ctx, fixture.residentID, event.ID, requestID)
	if err != nil {
		t.Fatal(err)
	}
	if !result.Changed || result.State != domain.WorkSucceeded || generator.CallCount() != 1 {
		t.Fatalf("reselected Admin re-extraction = %+v calls=%d, want succeeded/1", result, generator.CallCount())
	}
}

func TestM5AdminReextractRestartUsesFrozenEnvelope(t *testing.T) {
	ctx := context.Background()
	fixture := newApplicationFixture(t, &scriptedGenerator{steps: []generatorStep{{text: "legacy dialogue"}}}, 2)
	event, err := fixture.application.Ingress(ctx, "frozen re-extraction input")
	if err != nil {
		t.Fatal(err)
	}
	if err := fixture.application.ProcessResident(ctx, fixture.residentID); err != nil {
		t.Fatal(err)
	}
	activation, err := fixture.application.activateMemoryPolicyV4ForTest(ctx, fixture.residentID)
	if err != nil {
		t.Fatal(err)
	}
	requestID := memoryExtractionTestID(t, fixture)
	work := domain.MemoryExtractionWork{
		SourceEvent: event, IdempotencyKey: domain.MemoryReextractionObligation(event.ID, requestID),
		PolicyRevisionID: activation.RevisionID, State: domain.WorkPending,
	}
	assembly, err := fixture.application.assembleMemoryExtraction(ctx, work)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := fixture.application.submitWithContent(ctx,
		domain.PrepareMemoryReextractionCommand(domain.PrepareMemoryReextraction{
			Generation: assembly.Prepare, SourceEventID: event.ID, RequestID: requestID,
		}), assembly.Contents); err != nil {
		t.Fatal(err)
	}
	wantMessages := make([]generation.Message, len(assembly.Prepare.Inputs))
	for index, input := range assembly.Prepare.Inputs {
		wantMessages[index] = generation.Message{Role: generation.Role(input.Role), Text: string(input.Content.Bytes)}
	}
	params, _, err := domain.ParseGeneratorParams(assembly.Prepare.GeneratorParams.Bytes())
	if err != nil {
		t.Fatal(err)
	}

	restartedGenerator := &scriptedGenerator{steps: []generatorStep{{text: `{"claims":[],"version":"memory-extraction-output-v1"}`}}}
	restarted, _ := restartApplicationForEnvelopeTest(t, fixture, restartedGenerator, "test", "changed-default-model", 64<<10)
	if err := restarted.Recover(ctx); err != nil {
		t.Fatal(err)
	}
	if err := restarted.ProcessResident(ctx, fixture.residentID); err != nil {
		t.Fatal(err)
	}
	requests := restartedGenerator.Requests()
	if len(requests) != 1 || !reflect.DeepEqual(requests[0].Messages, wantMessages) ||
		requests[0].StructuredOutput == nil || requests[0].StructuredOutput.Mode != params.StructuredOutputMode {
		t.Fatalf("recovered request=%+v want messages=%+v", requests, wantMessages)
	}
	database := openApplicationDatabase(t, fixture.store.Path())
	defer database.Close()
	var policy, key, state string
	var attempt int64
	if err := database.QueryRow(`SELECT run.memory_policy_revision_id, run.idempotency_key,
		outcome.attempt_no, outcome.state FROM generation_runs run
		JOIN generation_run_outcomes outcome ON outcome.generation_run_id = run.generation_run_id
		WHERE run.generation_run_id = ? ORDER BY outcome.outcome_id DESC LIMIT 1`,
		assembly.Prepare.RunID.String()).Scan(&policy, &key, &attempt, &state); err != nil {
		t.Fatal(err)
	}
	if policy != activation.RevisionID.String() || key != work.IdempotencyKey || attempt != 2 || state != "succeeded" {
		t.Fatalf("recovered durable envelope=%s/%s outcome=%d/%s", policy, key, attempt, state)
	}
}

func TestM5ExtractionExistingIdentityAdvancesThroughMaturation(t *testing.T) {
	ctx := context.Background()
	claimOutput := `{"claims":[{"grade":"stated","perspective":"resident","source_quote":"tea","statement":"Owner likes tea","subject":"source_actor","temporal_kind":"stable"}],"version":"memory-extraction-output-v1"}`
	generator := &scriptedGenerator{steps: []generatorStep{
		{text: "dialogue one"}, {text: "dialogue two"}, {text: "dialogue three"},
		{text: claimOutput}, {text: claimOutput}, {text: claimOutput},
	}}
	fixture := newApplicationFixture(t, generator, 2)
	var events []domain.Event
	for _, text := range []string{"tea one", "tea two", "tea three"} {
		event, err := fixture.application.Ingress(ctx, text)
		if err != nil {
			t.Fatal(err)
		}
		if err := fixture.application.ProcessResident(ctx, fixture.residentID); err != nil {
			t.Fatal(err)
		}
		events = append(events, event)
	}
	if _, err := fixture.application.activateMemoryPolicyV4ForTest(ctx, fixture.residentID); err != nil {
		t.Fatal(err)
	}
	for _, event := range events {
		result, err := fixture.application.ReextractMemoryEvent(
			ctx, fixture.residentID, event.ID, memoryExtractionTestID(t, fixture),
		)
		if err != nil || result.State != domain.WorkSucceeded {
			t.Fatalf("re-extract %s = %+v, %v", event.ID, result, err)
		}
	}
	database := openApplicationDatabase(t, fixture.store.Path())
	defer database.Close()
	var claims, evidence int
	if err := database.QueryRow(`SELECT COUNT(*) FROM claims WHERE owner_resident_id = ?`,
		fixture.residentID.String()).Scan(&claims); err != nil {
		t.Fatal(err)
	}
	if err := database.QueryRow(`SELECT COUNT(*) FROM claim_evidence evidence
		JOIN claims claim ON claim.claim_id = evidence.claim_id WHERE claim.owner_resident_id = ?`,
		fixture.residentID.String()).Scan(&evidence); err != nil {
		t.Fatal(err)
	}
	rows, err := database.Query(`SELECT transition.to_stage, pipeline.pipeline_kind
		FROM claim_stage_transitions transition
		JOIN claims claim ON claim.claim_id = transition.claim_id
		JOIN pipeline_versions pipeline ON pipeline.pipeline_version_id = transition.pipeline_version_id
		JOIN canonical_commits commit_row ON commit_row.canonical_commit_id = transition.canonical_commit_id
		WHERE claim.owner_resident_id = ?
		ORDER BY commit_row.commit_seq, transition.stage_transition_id`, fixture.residentID.String())
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var stages, pipelines []string
	for rows.Next() {
		var stage, pipeline string
		if err := rows.Scan(&stage, &pipeline); err != nil {
			t.Fatal(err)
		}
		stages = append(stages, stage)
		pipelines = append(pipelines, pipeline)
	}
	if claims != 1 || evidence != 3 || !reflect.DeepEqual(stages, []string{"floating", "sediment", "settled"}) ||
		!reflect.DeepEqual(pipelines, []string{"memory_extraction", "memory_maturation", "memory_maturation"}) {
		t.Fatalf("maturation claims=%d evidence=%d stages=%q pipelines=%q", claims, evidence, stages, pipelines)
	}
}

func memoryExtractionTestID(t *testing.T, fixture applicationFixture) canonical.ID {
	t.Helper()
	id, err := fixture.application.ids.New()
	if err != nil {
		t.Fatal(err)
	}
	return id
}

type memoryObserver struct {
	mu     sync.Mutex
	deltas []string
}

func TestM5InvalidExtractionBatchIsTerminalAndPersistsRawResponse(t *testing.T) {
	ctx := context.Background()
	const raw = `{"claims":[{"grade":"stated","perspective":"source_actor","source_quote":"not present","statement":"invalid provenance","subject":"source_actor","temporal_kind":"stable"}],"version":"memory-extraction-output-v1"}`
	generator := &scriptedGenerator{steps: []generatorStep{{text: "dialogue"}, {text: raw}}}
	fixture := newApplicationFixture(t, generator, 3)
	if _, err := fixture.application.activateMemoryPolicyV4ForTest(ctx, fixture.residentID); err != nil {
		t.Fatal(err)
	}
	if _, err := fixture.application.Ingress(ctx, "source text"); err != nil {
		t.Fatal(err)
	}
	if err := fixture.application.ProcessResident(ctx, fixture.residentID); err != nil {
		t.Fatal(err)
	}
	database := openApplicationDatabase(t, fixture.store.Path())
	defer database.Close()
	var state, errorClass, detail string
	if err := database.QueryRow(`SELECT outcome.state, outcome.error_class, blob.content
		FROM generation_runs run
		JOIN generation_run_outcomes outcome ON outcome.generation_run_id = run.generation_run_id
		JOIN content_objects content ON content.content_id = outcome.error_detail_content_id
		JOIN blobs blob ON blob.dedupe_scope_id = content.owner_resident_id
		 AND blob.hash_algorithm = content.blob_hash_algorithm AND blob.blob_hash = content.blob_hash
		WHERE run.resident_id = ? AND run.purpose = 'memory_extraction'
		ORDER BY outcome.outcome_id DESC LIMIT 1`, fixture.residentID.String()).Scan(&state, &errorClass, &detail); err != nil {
		t.Fatal(err)
	}
	if state != "failed" || errorClass != "provider_invalid_response" || detail != raw {
		t.Fatalf("invalid response outcome=%s/%s detail=%q", state, errorClass, detail)
	}
	var claims int
	if err := database.QueryRow(`SELECT COUNT(*) FROM claims WHERE owner_resident_id = ?`, fixture.residentID.String()).Scan(&claims); err != nil {
		t.Fatal(err)
	}
	if claims != 0 || generator.CallCount() != 2 {
		t.Fatalf("invalid batch claims=%d provider calls=%d", claims, generator.CallCount())
	}
}

func TestM5I105PolicyActivationDoesNotRetroactivelyCreateMandatoryObligation(t *testing.T) {
	ctx := context.Background()
	generator := &scriptedGenerator{steps: []generatorStep{
		{text: "legacy dialogue"}, {text: "enabled dialogue"},
		{text: `{"claims":[],"version":"memory-extraction-output-v1"}`},
	}}
	fixture := newApplicationFixture(t, generator, 2)
	legacyEvent, err := fixture.application.Ingress(ctx, "before explicit activation")
	if err != nil {
		t.Fatal(err)
	}
	if err := fixture.application.ProcessResident(ctx, fixture.residentID); err != nil {
		t.Fatal(err)
	}
	if _, err := fixture.application.activateMemoryPolicyV4ForTest(ctx, fixture.residentID); err != nil {
		t.Fatal(err)
	}
	if err := fixture.application.ProcessResident(ctx, fixture.residentID); err != nil {
		t.Fatal(err)
	}
	if generator.CallCount() != 1 {
		t.Fatalf("activation retroactively scheduled provider calls=%d", generator.CallCount())
	}
	enabledEvent, err := fixture.application.Ingress(ctx, "after explicit activation")
	if err != nil {
		t.Fatal(err)
	}
	if err := fixture.application.ProcessResident(ctx, fixture.residentID); err != nil {
		t.Fatal(err)
	}
	if generator.CallCount() != 3 {
		t.Fatalf("provider calls=%d want legacy dialogue + enabled dialogue/extraction", generator.CallCount())
	}
	database := openApplicationDatabase(t, fixture.store.Path())
	defer database.Close()
	var key string
	if err := database.QueryRow(`SELECT idempotency_key FROM generation_runs
		WHERE resident_id = ? AND purpose = 'memory_extraction'`, fixture.residentID.String()).Scan(&key); err != nil {
		t.Fatal(err)
	}
	if key != domain.MemoryExtractionObligation(enabledEvent.ID) || key == domain.MemoryExtractionObligation(legacyEvent.ID) {
		t.Fatalf("memory obligation key=%q", key)
	}
}

func TestM5MandatoryExtractionRestartRecoveryUsesFrozenEnvelope(t *testing.T) {
	ctx := context.Background()
	fixture := newApplicationFixture(t, &scriptedGenerator{steps: []generatorStep{{text: "dialogue"}}}, 2)
	if _, err := fixture.application.activateMemoryPolicyV4ForTest(ctx, fixture.residentID); err != nil {
		t.Fatal(err)
	}
	event, err := fixture.application.Ingress(ctx, "recover my memory extraction")
	if err != nil {
		t.Fatal(err)
	}
	dialogueRun := dialogueRunIDForEventForTest(t, fixture, event)
	if err := fixture.application.processPreparedRun(ctx, fixture.residentID, dialogueRun); err != nil {
		t.Fatal(err)
	}
	memoryRepository := any(fixture.repository).(domain.MemoryWorkRepository)
	works, err := discoverMemoryExtractionWorkForTest(ctx, memoryRepository, fixture.residentID, 10, 2)
	if err != nil || len(works) != 1 || works[0].State != domain.WorkPending {
		t.Fatalf("pending memory work=%+v error=%v", works, err)
	}
	assembly, err := fixture.application.assembleMemoryExtraction(ctx, works[0])
	if err != nil {
		t.Fatal(err)
	}
	if _, err := fixture.application.submitWithContent(ctx, domain.PrepareGenerationCommand(assembly.Prepare), assembly.Contents); err != nil {
		t.Fatal(err)
	}

	restartedGenerator := &scriptedGenerator{steps: []generatorStep{{text: `{"claims":[],"version":"memory-extraction-output-v1"}`}}}
	restarted, _ := restartApplicationForEnvelopeTest(
		t, fixture, restartedGenerator, "test", "new-default-model", 64<<10,
	)
	restarted.retryBackoff = []time.Duration{0}
	if err := restarted.Recover(ctx); err != nil {
		t.Fatal(err)
	}
	if err := restarted.ProcessResident(ctx, fixture.residentID); err != nil {
		t.Fatal(err)
	}
	if restartedGenerator.CallCount() != 1 {
		t.Fatalf("recovered provider calls=%d want 1", restartedGenerator.CallCount())
	}
	database := openApplicationDatabase(t, fixture.store.Path())
	defer database.Close()
	var attempt int64
	var state string
	if err := database.QueryRow(`SELECT outcome.attempt_no, outcome.state
		FROM generation_runs run JOIN generation_run_outcomes outcome
		 ON outcome.generation_run_id = run.generation_run_id
		WHERE run.resident_id = ? AND run.purpose = 'memory_extraction'
		ORDER BY outcome.outcome_id DESC LIMIT 1`, fixture.residentID.String()).Scan(&attempt, &state); err != nil {
		t.Fatal(err)
	}
	if attempt != 2 || state != "succeeded" {
		t.Fatalf("recovered memory outcome=%d/%s", attempt, state)
	}
}

type preemptionGenerator struct {
	mu          sync.Mutex
	requests    []generation.Request
	memoryCalls int
	started     chan struct{}
}

func (generator *preemptionGenerator) Stream(
	ctx context.Context,
	request generation.Request,
	_ generation.DeltaSink,
) (generation.Result, error) {
	generator.mu.Lock()
	generator.requests = append(generator.requests, request)
	if request.Purpose == "memory_extraction" {
		generator.memoryCalls++
		call := generator.memoryCalls
		generator.mu.Unlock()
		if call == 1 {
			close(generator.started)
			<-ctx.Done()
			return generation.Result{}, ctx.Err()
		}
		return generation.Result{Text: `{"claims":[],"version":"memory-extraction-output-v1"}`}, nil
	}
	generator.mu.Unlock()
	return generation.Result{Text: "dialogue"}, nil
}

func TestM5BackgroundPreemptionDoesNotConsumeRetryAndDialogueRunsFirst(t *testing.T) {
	ctx := context.Background()
	generator := &preemptionGenerator{started: make(chan struct{})}
	// maxAttempts=1 makes the assertion meaningful: preemption may advance the
	// durable attempt ordinal, but it must not consume the provider retry budget.
	fixture := newApplicationFixture(t, generator, 1)
	if _, err := fixture.application.activateMemoryPolicyV4ForTest(ctx, fixture.residentID); err != nil {
		t.Fatal(err)
	}
	if _, err := fixture.application.Ingress(ctx, "first foreground"); err != nil {
		t.Fatal(err)
	}
	firstDone := make(chan error, 1)
	go func() { firstDone <- fixture.application.ProcessResident(ctx, fixture.residentID) }()
	select {
	case <-generator.started:
	case <-time.After(5 * time.Second):
		t.Fatal("background extraction did not start")
	}
	if _, err := fixture.application.Ingress(ctx, "second foreground"); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-firstDone:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("preempted extraction did not yield")
	}
	if err := fixture.application.ProcessResident(ctx, fixture.residentID); err != nil {
		t.Fatal(err)
	}
	// The second dialogue turn grants exactly one fair memory quantum and then
	// yields. A separate clean turn publishes the proof and drains the remaining
	// mandatory extraction without violating the per-turn fair bound.
	if err := fixture.application.ProcessResident(ctx, fixture.residentID); err != nil {
		t.Fatal(err)
	}
	// Both terminal dialogue obligations were traversed by a complete real
	// SQLite discovery cycle. The fifth provider request asserted below is the
	// ordinary background drain, whose admission requires this exact proof.
	if !fixture.application.currentForegroundClean(fixture.residentID) {
		t.Fatal("terminal dialogue history did not publish a foreground clean proof")
	}

	generator.mu.Lock()
	requests := append([]generation.Request(nil), generator.requests...)
	generator.mu.Unlock()
	if len(requests) != 5 {
		t.Fatalf("request order length=%d", len(requests))
	}
	want := []string{"dialogue", "memory_extraction", "dialogue", "memory_extraction", "memory_extraction"}
	for index := range want {
		if requests[index].Purpose != want[index] {
			t.Fatalf("request[%d].purpose=%q want=%q", index, requests[index].Purpose, want[index])
		}
	}
	database := openApplicationDatabase(t, fixture.store.Path())
	defer database.Close()
	rows, err := database.Query(`SELECT run.generation_run_id, outcome.attempt_no, outcome.state, COALESCE(outcome.error_class, '')
		FROM generation_runs run JOIN generation_run_outcomes outcome
		 ON outcome.generation_run_id = run.generation_run_id
		WHERE run.resident_id = ? AND run.purpose = 'memory_extraction'
		ORDER BY run.requested_at, outcome.outcome_id`, fixture.residentID.String())
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var preemptedRun string
	for rows.Next() {
		var runID string
		var attempt int64
		var state, code string
		if err := rows.Scan(&runID, &attempt, &state, &code); err != nil {
			t.Fatal(err)
		}
		if code == "foreground_preempted" {
			preemptedRun = runID
			if attempt != 1 || state != "cancelled" {
				t.Fatalf("preempted outcome=%d/%s", attempt, state)
			}
		}
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	if preemptedRun == "" {
		t.Fatal("foreground_preempted outcome was not persisted")
	}
	var finalAttempt int64
	var finalState string
	if err := database.QueryRow(`SELECT attempt_no, state FROM generation_run_outcomes
		WHERE generation_run_id = ? ORDER BY outcome_id DESC LIMIT 1`, preemptedRun).Scan(&finalAttempt, &finalState); err != nil {
		t.Fatal(err)
	}
	if finalAttempt != 2 || finalState != "succeeded" {
		t.Fatalf("preempted run did not resume outside retry budget: %d/%s", finalAttempt, finalState)
	}
}

func (*memoryObserver) UserCommitted(domain.Event)                          {}
func (*memoryObserver) GenerationStarted(canonical.ID, canonical.ID, int64) {}
func (observer *memoryObserver) GenerationDelta(_ canonical.ID, _ canonical.ID, text string) {
	observer.mu.Lock()
	observer.deltas = append(observer.deltas, text)
	observer.mu.Unlock()
}
func (*memoryObserver) ResidentCommitted(domain.Event)                                   {}
func (*memoryObserver) GenerationFailed(canonical.ID, canonical.ID, int64, string, bool) {}

func TestM5I38ClaimEvidenceAndInitialStageLandAtomically(t *testing.T) {
	ctx := context.Background()
	extraction := `{"claims":[{"grade":"stated","perspective":"source_actor","source_quote":"blue bicycle","statement":"The owner has a blue bicycle","subject":"source_actor","temporal_kind":"stable"}],"version":"memory-extraction-output-v1"}`
	generator := &scriptedGenerator{steps: []generatorStep{
		{text: "dialogue response", deltas: []string{"dialogue ", "response"}},
		{text: extraction, deltas: []string{"must-not-reach-sse"}},
	}}
	fixture := newApplicationFixture(t, generator, 2)
	observer := &memoryObserver{}
	fixture.application.SetObserver(observer)
	if _, err := fixture.application.activateMemoryPolicyV4ForTest(ctx, fixture.residentID); err != nil {
		t.Fatal(err)
	}
	if _, err := fixture.application.Ingress(ctx, "I own a blue bicycle"); err != nil {
		t.Fatal(err)
	}
	if err := fixture.application.ProcessResident(ctx, fixture.residentID); err != nil {
		t.Fatal(err)
	}

	requests := generator.Requests()
	if len(requests) != 2 {
		t.Fatalf("provider requests=%d want dialogue+extraction", len(requests))
	}
	if requests[1].Purpose != "memory_extraction" || requests[1].Streaming || requests[1].StructuredOutput == nil {
		t.Fatalf("memory provider request=%+v", requests[1])
	}
	observer.mu.Lock()
	deltas := append([]string(nil), observer.deltas...)
	observer.mu.Unlock()
	if len(deltas) != 2 || deltas[0] != "dialogue " || deltas[1] != "response" {
		t.Fatalf("published deltas=%q; background output leaked", deltas)
	}

	database := openApplicationDatabase(t, fixture.store.Path())
	defer database.Close()
	var claimCommit, evidenceCommit, stageCommit, scopeCommit string
	var weight int64
	var stage, scope, outcomeState string
	err := database.QueryRow(`SELECT claim.canonical_commit_id, evidence.canonical_commit_id,
		stage.canonical_commit_id, scope.canonical_commit_id, evidence.weight,
		stage.to_stage, scope.view_scope, outcome.state
		FROM claims claim
		JOIN claim_evidence evidence ON evidence.claim_id = claim.claim_id
		JOIN claim_stage_transitions stage ON stage.claim_id = claim.claim_id
		JOIN claim_view_scope_assertions scope ON scope.claim_id = claim.claim_id
		JOIN generation_runs run ON run.generation_run_id = claim.created_by_run_id
		JOIN generation_run_outcomes outcome ON outcome.generation_run_id = run.generation_run_id
		WHERE claim.owner_resident_id = ? AND run.purpose = 'memory_extraction'
		  AND outcome.state = 'succeeded'`, fixture.residentID.String()).Scan(
		&claimCommit, &evidenceCommit, &stageCommit, &scopeCommit, &weight, &stage, &scope, &outcomeState,
	)
	if err != nil {
		t.Fatal(err)
	}
	if claimCommit != evidenceCommit || claimCommit != stageCommit || claimCommit != scopeCommit {
		t.Fatalf("landing crossed commits: claim=%s evidence=%s stage=%s scope=%s", claimCommit, evidenceCommit, stageCommit, scopeCommit)
	}
	if weight != 1_000_000 || stage != "floating" || scope != "resident_ui" || outcomeState != "succeeded" {
		t.Fatalf("landing weight/stage/scope/outcome=%d/%s/%s/%s", weight, stage, scope, outcomeState)
	}
}

func TestM5I78InitialViewScopeLandsAtomically(t *testing.T) {
	ctx := context.Background()
	extraction := `{"claims":[{"grade":"stated","perspective":"source_actor","source_quote":"green notebook","statement":"The owner has a green notebook","subject":"source_actor","temporal_kind":"stable"}],"version":"memory-extraction-output-v1"}`
	fixture := newApplicationFixture(t, &scriptedGenerator{steps: []generatorStep{
		{text: "dialogue response"}, {text: extraction},
	}}, 2)
	if _, err := fixture.application.activateMemoryPolicyV4ForTest(ctx, fixture.residentID); err != nil {
		t.Fatal(err)
	}
	if _, err := fixture.application.Ingress(ctx, "I have a green notebook"); err != nil {
		t.Fatal(err)
	}
	if err := fixture.application.ProcessResident(ctx, fixture.residentID); err != nil {
		t.Fatal(err)
	}
	database := openApplicationDatabase(t, fixture.store.Path())
	defer database.Close()
	var claimCommit, scopeCommit, scope, reason string
	var actor sql.NullString
	var generationRun, policyRevision string
	if err := database.QueryRow(`SELECT claim.canonical_commit_id, assertion.canonical_commit_id,
		assertion.view_scope, assertion.reason_code, assertion.actor_principal_id,
		assertion.generation_run_id, assertion.memory_policy_revision_id
		FROM claims claim JOIN claim_view_scope_assertions assertion ON assertion.claim_id = claim.claim_id
		WHERE claim.owner_resident_id = ?`, fixture.residentID.String()).Scan(
		&claimCommit, &scopeCommit, &scope, &reason, &actor, &generationRun, &policyRevision,
	); err != nil {
		t.Fatal(err)
	}
	if claimCommit != scopeCommit || scope != "resident_ui" || reason != "initial_extraction" ||
		actor.Valid || generationRun == "" || policyRevision == "" {
		t.Fatalf("initial scope landing = %s/%s %s %s actor=%+v run=%s policy=%s",
			claimCommit, scopeCommit, scope, reason, actor, generationRun, policyRevision)
	}
}

func TestM5RTI10NonEventSuccessRequiresPurposeLanding(t *testing.T) {
	ctx := context.Background()
	generator := &scriptedGenerator{steps: []generatorStep{
		{text: "dialogue response"},
		{text: `{"claims":[],"version":"memory-extraction-output-v1"}`},
	}}
	fixture := newApplicationFixture(t, generator, 2)
	if _, err := fixture.application.activateMemoryPolicyV4ForTest(ctx, fixture.residentID); err != nil {
		t.Fatal(err)
	}
	if _, err := fixture.application.Ingress(ctx, "There may be nothing durable here"); err != nil {
		t.Fatal(err)
	}
	if err := fixture.application.ProcessResident(ctx, fixture.residentID); err != nil {
		t.Fatal(err)
	}
	database := openApplicationDatabase(t, fixture.store.Path())
	defer database.Close()
	var succeeded, claims int
	if err := database.QueryRow(`SELECT
		COUNT(*) FILTER (WHERE outcome.state = 'succeeded'),
		(SELECT COUNT(*) FROM claims WHERE owner_resident_id = ?)
		FROM generation_runs run
		JOIN generation_run_outcomes outcome ON outcome.generation_run_id = run.generation_run_id
		WHERE run.resident_id = ? AND run.purpose = 'memory_extraction'`,
		fixture.residentID.String(), fixture.residentID.String()).Scan(&succeeded, &claims); err != nil {
		t.Fatal(err)
	}
	if succeeded != 1 || claims != 0 {
		t.Fatalf("zero-claim purpose landing succeeded=%d claims=%d", succeeded, claims)
	}
}

func openApplicationDatabase(t *testing.T, path string) *sql.DB {
	t.Helper()
	database, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	return database
}
