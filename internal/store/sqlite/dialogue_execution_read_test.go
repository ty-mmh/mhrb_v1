package sqlite

import (
	"context"
	"strings"
	"testing"

	"mahoroba.local/mahoroba/internal/canonical"
	"mahoroba.local/mahoroba/internal/domain"
)

func TestCOV56GenerationReadKeepsAllNormalDialogueCompatibility(t *testing.T) {
	fixture, closeFixture := newSemanticFixture(t)
	defer closeFixture()
	ctx := context.Background()
	repository := &CanonicalRepository{store: &Store{reader: fixture.db}}

	legacyPipelineID := mustParseSnapshotID(fixture.ids.new())
	priorSplitPipelineID := mustParseSnapshotID(fixture.ids.new())
	currentPipelineID := mustParseSnapshotID(fixture.ids.new())
	for _, pipeline := range []struct {
		id      canonical.ID
		version string
	}{
		{id: legacyPipelineID, version: domain.DialoguePipelineVersionV1},
		{id: priorSplitPipelineID, version: domain.DialoguePipelineVersionV2},
		{id: currentPipelineID, version: domain.DialoguePipelineVersionV3},
	} {
		definition, err := domain.DialoguePipelineDefinition(pipeline.id, pipeline.version)
		if err != nil {
			t.Fatal(err)
		}
		mustExec(t, fixture.db, `INSERT INTO pipeline_versions(
			pipeline_version_id, canonical_commit_id, pipeline_kind, version_key,
			definition, recorded_at, recorded_tz
		) VALUES (?, ?, 'dialogue', ?, ?, ?, ?)`, pipeline.id.String(), fixture.commit["global"],
			pipeline.version, definition.Definition.String(), semanticTime, semanticTZ)
	}

	params, canonicalParams, err := domain.NewUnstructuredGeneratorParams(false, 128)
	if err != nil {
		t.Fatal(err)
	}
	if params.MaxOutputBytes != 128 {
		t.Fatalf("generator params = %+v", params)
	}
	dropped, err := canonical.MarshalCanonical(struct {
		Backfill canonical.Count `json:"backfill"`
		Live     canonical.Count `json:"live_context"`
	}{})
	if err != nil {
		t.Fatal(err)
	}

	tests := []struct {
		name       string
		pipelineID canonical.ID
		contract   domain.DialogueExecutionContract
		wantErr    bool
	}{
		{
			name: "legacy normal", pipelineID: legacyPipelineID,
			contract: domain.LegacyDialogueNormalExecutionContract(),
		},
		{
			name: "prior-split normal", pipelineID: priorSplitPipelineID,
			contract: domain.PriorSplitDialogueNormalExecutionContract(),
		},
		{
			name: "current normal", pipelineID: currentPipelineID,
			contract: domain.CurrentDialogueNormalExecutionContract(),
		},
		{
			name: "mixed prior-split pipeline and legacy context", pipelineID: priorSplitPipelineID,
			contract: domain.DialogueExecutionContract{
				PipelineVersionKey:     domain.DialoguePipelineVersionV2,
				PromptTemplateVersion:  domain.DialoguePromptTemplateVersionV1,
				ContextPolicyVersion:   domain.DialogueContextPolicyVersionV1,
				MemoryRenderingVersion: domain.MemoryRenderingVersionV1,
			},
			wantErr: true,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			runID := mustParseSnapshotID(fixture.ids.new())
			inputID := mustParseSnapshotID(fixture.ids.new())
			outcomeID := mustParseSnapshotID(fixture.ids.new())
			mustInsertCOV2NormalDialogueRun(t, fixture, runID, inputID, outcomeID,
				test.pipelineID, test.contract, canonicalParams, dropped)

			prepared, err := repository.Generation(ctx, runID)
			if test.wantErr {
				if err == nil {
					t.Fatalf("mixed persisted tuple was read as %+v", prepared)
				}
				return
			}
			if err != nil {
				t.Fatalf("read compatible %s run: %v", test.name, err)
			}
			got := domain.DialogueExecutionContract{
				PipelineVersionKey:     prepared.PipelineVersionKey,
				PromptTemplateVersion:  prepared.PromptTemplateVersion,
				ContextPolicyVersion:   prepared.ContextPolicyVersion,
				MemoryRenderingVersion: prepared.MemoryRenderingVersion,
			}
			if got != test.contract || prepared.PipelineVersionID != test.pipelineID ||
				prepared.State != domain.WorkRunning || len(prepared.Inputs) != 1 {
				t.Fatalf("read %s run = pipeline %s contract %+v state %s inputs %d",
					test.name, prepared.PipelineVersionID, got, prepared.State, len(prepared.Inputs))
			}
		})
	}
}

func TestCOV2GenerationReadClassifiesLegacyAndCurrentSyntheticDialogue(t *testing.T) {
	for _, pipelineVersion := range []string{
		domain.DialoguePipelineVersionV1,
		domain.DialoguePipelineVersionV2,
	} {
		t.Run(pipelineVersion, func(t *testing.T) {
			fixture := newCOV2SyntheticDialogueReadFixture(t, pipelineVersion, false, false)
			prepared, err := fixture.repository.Generation(context.Background(), fixture.runID)
			if err != nil {
				t.Fatalf("read valid synthetic dialogue: %v", err)
			}
			want, err := domain.SyntheticDialogueExecutionContract(pipelineVersion)
			if err != nil {
				t.Fatal(err)
			}
			got := domain.DialogueExecutionContract{
				PipelineVersionKey:     prepared.PipelineVersionKey,
				PromptTemplateVersion:  prepared.PromptTemplateVersion,
				ContextPolicyVersion:   prepared.ContextPolicyVersion,
				MemoryRenderingVersion: prepared.MemoryRenderingVersion,
			}
			if got != want || prepared.PipelineVersionID != fixture.pipelineID ||
				prepared.Provider != "mahoroba-internal" || prepared.Model != "not-dispatched" ||
				prepared.State != domain.WorkTerminalFailed || prepared.AttemptNo != 0 ||
				prepared.RecallRunID != nil || len(prepared.Inputs) != 0 {
				t.Fatalf("synthetic persisted read = %+v, contract=%+v", prepared, got)
			}
		})
	}
}

func TestCOV2GenerationReadRejectsMalformedSyntheticDialogue(t *testing.T) {
	t.Run("dispatchable provider identity", func(t *testing.T) {
		fixture := newCOV2SyntheticDialogueReadFixture(t, domain.DialoguePipelineVersionV2, false, false)
		rewriteCOV2SyntheticGenerationField(t, fixture, "provider", "test-provider")
		if _, err := fixture.repository.Generation(context.Background(), fixture.runID); err == nil ||
			!strings.Contains(err.Error(), "dispatchable provider identity") {
			t.Fatalf("synthetic provider identity error = %v", err)
		}
	})

	t.Run("recall link", func(t *testing.T) {
		fixture := newCOV2SyntheticDialogueReadFixture(t, domain.DialoguePipelineVersionV2, false, false)
		rewriteCOV2SyntheticGenerationField(t, fixture, "recall_run_id", fixture.fixture.recall["A"])
		if _, err := fixture.repository.Generation(context.Background(), fixture.runID); err == nil ||
			!strings.Contains(err.Error(), "invalid no-dispatch structure") {
			t.Fatalf("synthetic Recall link error = %v", err)
		}
	})

	t.Run("claim usage provenance", func(t *testing.T) {
		fixture := newCOV2SyntheticDialogueReadFixture(t, domain.DialoguePipelineVersionV2, false, false)
		mustExec(t, fixture.fixture.db, `INSERT INTO claim_usages(
			claim_usage_id, canonical_commit_id, claim_id, recall_run_id, generation_run_id,
			usage_type, ordinal, memory_policy_revision_id, exclusion_reason, detection_method,
			detection_confidence, detected_by_run_id, recorded_at, recorded_tz
		) VALUES (?, ?, ?, NULL, ?, 'selected', 0, ?, NULL, NULL, NULL, NULL, ?, ?)`,
			fixture.fixture.ids.new(), fixture.fixture.commit["A"], fixture.fixture.claim["A"],
			fixture.runID.String(), fixture.fixture.revision["A"]["memory_policy"], semanticTime, semanticTZ)
		if _, err := fixture.repository.Generation(context.Background(), fixture.runID); err == nil ||
			!strings.Contains(err.Error(), "claim usage provenance") {
			t.Fatalf("synthetic claim usage error = %v", err)
		}
	})

	t.Run("current normal tuple on attempt zero", func(t *testing.T) {
		fixture := newCOV2SyntheticDialogueReadFixture(t, domain.DialoguePipelineVersionV2, true, false)
		if _, err := fixture.repository.Generation(context.Background(), fixture.runID); err == nil ||
			!strings.Contains(err.Error(), "version contract mismatch") {
			t.Fatalf("mixed synthetic tuple error = %v", err)
		}
	})

	t.Run("attempt zero mixed with a dispatched attempt", func(t *testing.T) {
		fixture := newCOV2SyntheticDialogueReadFixture(t, domain.DialoguePipelineVersionV2, false, true)
		if _, err := fixture.repository.Generation(context.Background(), fixture.runID); err == nil ||
			!strings.Contains(err.Error(), "invalid attempt zero") {
			t.Fatalf("mixed synthetic history error = %v", err)
		}
	})

	t.Run("post-v2 tuple for a pre-v2 source event", func(t *testing.T) {
		fixture := newCOV2SyntheticDialogueReadFixture(t, domain.DialoguePipelineVersionV2, false, false)
		var triggerSQL string
		if err := fixture.fixture.db.QueryRow(`SELECT sql FROM sqlite_schema
			WHERE type = 'trigger' AND name = 'trg_generation_runs_no_update'`).Scan(&triggerSQL); err != nil {
			t.Fatal(err)
		}
		tx, err := fixture.fixture.db.Begin()
		if err != nil {
			t.Fatal(err)
		}
		defer tx.Rollback()
		mustExec(t, tx, `DROP TRIGGER trg_generation_runs_no_update`)
		mustExec(t, tx, `UPDATE generation_runs SET idempotency_key = ?
			WHERE generation_run_id = ?`, domain.DialogueObligation(mustParseSnapshotID(fixture.fixture.event["A"])),
			fixture.runID.String())
		mustExec(t, tx, triggerSQL)
		if err := tx.Commit(); err != nil {
			t.Fatal(err)
		}
		if _, err := fixture.repository.Generation(context.Background(), fixture.runID); err == nil ||
			!strings.Contains(err.Error(), "source-time contract") {
			t.Fatalf("source-time pipeline mismatch error = %v", err)
		}
	})
}

func TestCOV2StartAttemptRejectsSyntheticCancellationHistory(t *testing.T) {
	fixture := newCOV2SyntheticDialogueReadFixture(t, domain.DialoguePipelineVersionV2, false, false)
	tx, err := fixture.fixture.db.Begin()
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	scope, err := canonical.ResidentScope(mustParseSnapshotID(fixture.fixture.resident["A"]))
	if err != nil {
		t.Fatal(err)
	}
	uow := &canonicalUoW{tx: tx, metadata: canonical.CommitMetadata{Scope: scope}}
	err = uow.StartAttempt(context.Background(), domain.Attempt{
		RunID: fixture.runID, ResidentID: mustParseSnapshotID(fixture.fixture.resident["A"]),
		AttemptNo: 1, OutcomeID: mustParseSnapshotID(fixture.fixture.ids.new()),
	})
	if err == nil || !strings.Contains(err.Error(), "synthetic cancellation cannot be retried") {
		t.Fatalf("StartAttempt synthetic retry error = %v", err)
	}
	var outcomes int
	if err := tx.QueryRow(`SELECT COUNT(*) FROM generation_run_outcomes
		WHERE generation_run_id = ?`, fixture.runID.String()).Scan(&outcomes); err != nil {
		t.Fatal(err)
	}
	if outcomes != 1 {
		t.Fatalf("synthetic outcome count after rejected retry = %d, want 1", outcomes)
	}
}

func TestCOV2DialogueRetryVersionValidatorAcceptsOnlyLegacyAndCurrentNormal(t *testing.T) {
	legacy := newCOV2SyntheticDialogueReadFixture(t, domain.DialoguePipelineVersionV1, false, false)
	if err := validateDialogueRetryExecutionContract(context.Background(), legacy.fixture.db, legacy.runID); err != nil {
		t.Fatalf("legacy normal-compatible tuple rejected: %v", err)
	}

	current := newCOV2SyntheticDialogueReadFixture(t, domain.DialoguePipelineVersionV2, false, false)
	rewriteCOV2SyntheticGenerationField(t, current, "memory_rendering_version", domain.MemoryRenderingVersionV1)
	if err := validateDialogueRetryExecutionContract(context.Background(), current.fixture.db, current.runID); err != nil {
		t.Fatalf("current normal tuple rejected: %v", err)
	}

	mixed := newCOV2SyntheticDialogueReadFixture(t, domain.DialoguePipelineVersionV2, false, false)
	if err := validateDialogueRetryExecutionContract(context.Background(), mixed.fixture.db, mixed.runID); err == nil ||
		!strings.Contains(err.Error(), "version contract mismatch") {
		t.Fatalf("current synthetic tuple accepted for retry: %v", err)
	}
}

func TestCOV2LegacyRecallLandingKeepsFrozenBytesWithoutRerender(t *testing.T) {
	fixture, closeFixture := newSemanticFixture(t)
	defer closeFixture()

	pipelineID := mustParseSnapshotID(fixture.ids.new())
	definition, err := domain.DialoguePipelineDefinition(pipelineID, domain.DialoguePipelineVersionV1)
	if err != nil {
		t.Fatal(err)
	}
	mustExec(t, fixture.db, `INSERT INTO pipeline_versions(
		pipeline_version_id, canonical_commit_id, pipeline_kind, version_key,
		definition, recorded_at, recorded_tz
	) VALUES (?, ?, 'dialogue', ?, ?, ?, ?)`, pipelineID.String(), fixture.commit["global"],
		domain.DialoguePipelineVersionV1, definition.Definition.String(), semanticTime, semanticTZ)

	runID := mustParseSnapshotID(fixture.ids.new())
	_, params, err := domain.NewUnstructuredGeneratorParams(true, 128)
	if err != nil {
		t.Fatal(err)
	}
	dropped, err := canonical.MarshalCanonical(struct {
		Backfill canonical.Count `json:"backfill"`
		Live     canonical.Count `json:"live_context"`
	}{})
	if err != nil {
		t.Fatal(err)
	}
	mustExec(t, fixture.db, `INSERT INTO generation_runs(
		generation_run_id, canonical_commit_id, resident_id, purpose, idempotency_key,
		provider, model, model_version, prompt_template_version, pipeline_version_id,
		context_policy_version, sessionization_policy_version_id, memory_rendering_version,
		principles_revision_id, persona_revision_id, memory_policy_revision_id, recall_run_id,
		temperature, top_p, max_tokens, seed, generator_params, as_of, as_of_tz,
		budget_exceeded, dropped_input_summary, requested_at, requested_tz
	) VALUES (?, ?, ?, 'dialogue', ?, 'test-provider', 'test-model', NULL, ?, ?, ?, ?, ?, ?, ?, ?, ?,
		NULL, NULL, NULL, NULL, ?, ?, ?, 0, ?, ?, ?)`,
		runID.String(), fixture.commit["A"], fixture.resident["A"], "dialogue:v1:frozen-legacy",
		domain.DialoguePromptTemplateVersionV1, pipelineID.String(), domain.DialogueContextPolicyVersionV1,
		fixture.session, domain.MemoryRenderingVersionNoneV1, fixture.revision["A"]["principles"],
		fixture.revision["A"]["persona"], fixture.revision["A"]["memory_policy"], fixture.recall["A"],
		params.String(), semanticTime, semanticTZ, dropped.String(), semanticTime, semanticTZ)

	// This is intentionally not the output of the current renderer. A v1 retry
	// must use the immutable bytes frozen by its original Prepare rather than
	// deriving new bytes from the current claim TemporalRelation.
	inputContentID := fixture.addContent(t, "A", "generation_input", "legacy frozen rendered claim", "independent")
	mustExec(t, fixture.db, `INSERT INTO generation_run_inputs(
		generation_run_input_id, canonical_commit_id, generation_run_id, ordinal, role,
		source_type, source_id, inclusion_mode, content_id, recorded_at, recorded_tz
	) VALUES (?, ?, ?, 0, 'system', 'claim', ?, 'memory_recall', ?, ?, ?)`,
		fixture.ids.new(), fixture.commit["A"], runID.String(), fixture.claim["A"],
		inputContentID, semanticTime, semanticTZ)

	tx, err := fixture.db.Begin()
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	if err := (&canonicalUoW{tx: tx}).revalidateDialogueInputsAtLanding(
		context.Background(), runID, mustParseSnapshotID(fixture.resident["A"]),
	); err != nil {
		t.Fatalf("legacy frozen Recall input was re-rendered at landing: %v", err)
	}
}

type cov2SyntheticDialogueReadFixture struct {
	fixture    *semanticFixture
	repository *CanonicalRepository
	runID      canonical.ID
	pipelineID canonical.ID
}

func newCOV2SyntheticDialogueReadFixture(
	t *testing.T,
	pipelineVersion string,
	useNormalTuple bool,
	addDispatchedAttempt bool,
) cov2SyntheticDialogueReadFixture {
	t.Helper()
	fixture, closeFixture := newSemanticFixture(t)
	t.Cleanup(closeFixture)

	legacyPipelineID := mustParseSnapshotID(fixture.ids.new())
	legacyDefinition, err := domain.DialoguePipelineDefinition(legacyPipelineID, domain.DialoguePipelineVersionV1)
	if err != nil {
		t.Fatal(err)
	}
	mustExec(t, fixture.db, `INSERT INTO pipeline_versions(
		pipeline_version_id, canonical_commit_id, pipeline_kind, version_key,
		definition, recorded_at, recorded_tz
	) VALUES (?, ?, 'dialogue', ?, ?, ?, ?)`, legacyPipelineID.String(), fixture.commit["global"],
		domain.DialoguePipelineVersionV1, legacyDefinition.Definition.String(), semanticTime, semanticTZ)

	pipelineID := legacyPipelineID
	sourceID := mustParseSnapshotID(fixture.event["A"])
	nextCommitSeq := int64(4)
	if pipelineVersion == domain.DialoguePipelineVersionV2 {
		pipelineCommit := fixture.ids.new()
		mustExec(t, fixture.db, `INSERT INTO canonical_commits(
			canonical_commit_id, commit_seq, resident_id, committed_at, committed_tz
		) VALUES (?, 4, NULL, ?, ?)`, pipelineCommit, semanticTime+3, semanticTZ)
		pipelineID = mustParseSnapshotID(fixture.ids.new())
		definition, err := domain.DialoguePipelineDefinition(pipelineID, domain.DialoguePipelineVersionV2)
		if err != nil {
			t.Fatal(err)
		}
		mustExec(t, fixture.db, `INSERT INTO pipeline_versions(
			pipeline_version_id, canonical_commit_id, pipeline_kind, version_key,
			definition, recorded_at, recorded_tz
		) VALUES (?, ?, 'dialogue', ?, ?, ?, ?)`, pipelineID.String(), pipelineCommit,
			domain.DialoguePipelineVersionV2, definition.Definition.String(), semanticTime+3, semanticTZ)

		sourceCommit := fixture.ids.new()
		mustExec(t, fixture.db, `INSERT INTO canonical_commits(
			canonical_commit_id, commit_seq, resident_id, committed_at, committed_tz
		) VALUES (?, 5, ?, ?, ?)`, sourceCommit, fixture.resident["A"], semanticTime+4, semanticTZ)
		sourceID = mustParseSnapshotID(fixture.ids.new())
		mustExec(t, fixture.db, `INSERT INTO events(
			event_id, canonical_commit_id, resident_id, seq, event_type, visibility,
			delivery_screen, delivery_audio, ingress, trust_level, actor_principal_id,
			target_principal_id, generation_run_id, occurred_at, occurred_tz, recorded_at,
			recorded_tz, content_id, payload_commitment, prev_event_hash, event_hash,
			event_hash_algorithm, event_hash_domain, canonicalization_version
		) VALUES (?, ?, ?, 2, 'user_message', 'conversation', 0, 0, 'local_ui', 'trusted', ?, ?, NULL,
			?, ?, ?, ?, ?, ?, ?, ?, 'sha256', 'mahoroba:event-hash:v1', 'mahoroba-jcs-v1')`,
			sourceID.String(), sourceCommit, fixture.resident["A"], fixture.principal["human"],
			fixture.principal["A"], semanticTime+4, semanticTZ, semanticTime+4, semanticTZ,
			fixture.content["A"]["event"], semanticDigest("synthetic-current-payload"),
			semanticDigest("event-A"), semanticDigest("synthetic-current-event"))
		nextCommitSeq = 6
	}

	cancelCommit := fixture.ids.new()
	mustExec(t, fixture.db, `INSERT INTO canonical_commits(
		canonical_commit_id, commit_seq, resident_id, committed_at, committed_tz
	) VALUES (?, ?, ?, ?, ?)`, cancelCommit, nextCommitSeq, fixture.resident["A"],
		semanticTime+nextCommitSeq-1, semanticTZ)
	runID := mustParseSnapshotID(fixture.ids.new())
	_, params, err := domain.NewUnstructuredGeneratorParams(false, 1)
	if err != nil {
		t.Fatal(err)
	}
	dropped, err := canonical.MarshalCanonical(struct {
		Backfill canonical.Count `json:"backfill"`
		Live     canonical.Count `json:"live_context"`
		Reason   string          `json:"reason"`
	}{Reason: "source_content_erased"})
	if err != nil {
		t.Fatal(err)
	}
	contract, err := domain.SyntheticDialogueExecutionContract(pipelineVersion)
	if err != nil {
		t.Fatal(err)
	}
	if useNormalTuple {
		contract = domain.CurrentDialogueNormalExecutionContract()
	}
	mustExec(t, fixture.db, `INSERT INTO generation_runs(
		generation_run_id, canonical_commit_id, resident_id, purpose, idempotency_key,
		provider, model, model_version, prompt_template_version, pipeline_version_id,
		context_policy_version, sessionization_policy_version_id, memory_rendering_version,
		principles_revision_id, persona_revision_id, memory_policy_revision_id, recall_run_id,
		temperature, top_p, max_tokens, seed, generator_params, as_of, as_of_tz,
		budget_exceeded, dropped_input_summary, requested_at, requested_tz
	) VALUES (?, ?, ?, 'dialogue', ?, 'mahoroba-internal', 'not-dispatched', NULL, ?, ?, ?, ?, ?, ?, ?, ?, NULL,
		NULL, NULL, NULL, NULL, ?, ?, ?, 0, ?, ?, ?)`,
		runID.String(), cancelCommit, fixture.resident["A"], domain.DialogueObligation(sourceID),
		contract.PromptTemplateVersion, pipelineID.String(), contract.ContextPolicyVersion, fixture.session,
		contract.MemoryRenderingVersion, fixture.revision["A"]["principles"], fixture.revision["A"]["persona"],
		fixture.revision["A"]["memory_policy"], params.String(), semanticTime+nextCommitSeq-1, semanticTZ,
		dropped.String(), semanticTime+nextCommitSeq-1, semanticTZ)
	mustExec(t, fixture.db, `INSERT INTO generation_run_outcomes(
		outcome_id, canonical_commit_id, generation_run_id, attempt_no, state,
		output_content_id, prompt_tokens, completion_tokens, latency, estimated_cost,
		error_class, error_detail_content_id, recorded_at, recorded_tz
	) VALUES (?, ?, ?, 0, 'cancelled', NULL, NULL, NULL, NULL, NULL,
		'source_content_erased', NULL, ?, ?)`, fixture.ids.new(), cancelCommit, runID.String(),
		semanticTime+nextCommitSeq-1, semanticTZ)
	if addDispatchedAttempt {
		mustExec(t, fixture.db, `INSERT INTO generation_run_outcomes(
			outcome_id, canonical_commit_id, generation_run_id, attempt_no, state,
			output_content_id, prompt_tokens, completion_tokens, latency, estimated_cost,
			error_class, error_detail_content_id, recorded_at, recorded_tz
		) VALUES (?, ?, ?, 1, 'running', NULL, NULL, NULL, NULL, NULL,
			NULL, NULL, ?, ?)`, fixture.ids.new(), cancelCommit, runID.String(),
			semanticTime+nextCommitSeq-1, semanticTZ)
	}
	return cov2SyntheticDialogueReadFixture{
		fixture: fixture, repository: &CanonicalRepository{store: &Store{reader: fixture.db}},
		runID: runID, pipelineID: pipelineID,
	}
}

func rewriteCOV2SyntheticGenerationField(
	t *testing.T,
	fixture cov2SyntheticDialogueReadFixture,
	column string,
	value any,
) {
	t.Helper()
	allowed := map[string]bool{
		"provider":                 true,
		"recall_run_id":            true,
		"memory_rendering_version": true,
	}
	if !allowed[column] {
		t.Fatalf("unsupported synthetic generation rewrite column %q", column)
	}
	var triggerSQL string
	if err := fixture.fixture.db.QueryRow(`SELECT sql FROM sqlite_schema
		WHERE type = 'trigger' AND name = 'trg_generation_runs_no_update'`).Scan(&triggerSQL); err != nil {
		t.Fatal(err)
	}
	tx, err := fixture.fixture.db.Begin()
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	mustExec(t, tx, `DROP TRIGGER trg_generation_runs_no_update`)
	mustExec(t, tx, `UPDATE generation_runs SET `+column+` = ? WHERE generation_run_id = ?`,
		value, fixture.runID.String())
	mustExec(t, tx, triggerSQL)
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
}

func TestCOV56SyntheticCancellationResolvesPipelineAtSourceEventCommit(t *testing.T) {
	fixture, closeFixture := newSemanticFixture(t)
	defer closeFixture()
	ctx := context.Background()

	legacyPipelineID := mustParseSnapshotID(fixture.ids.new())
	legacyDefinition, err := domain.DialoguePipelineDefinition(legacyPipelineID, domain.DialoguePipelineVersionV1)
	if err != nil {
		t.Fatal(err)
	}
	mustExec(t, fixture.db, `INSERT INTO pipeline_versions(
		pipeline_version_id, canonical_commit_id, pipeline_kind, version_key,
		definition, recorded_at, recorded_tz
	) VALUES (?, ?, 'dialogue', ?, ?, ?, ?)`, legacyPipelineID.String(), fixture.commit["global"],
		domain.DialoguePipelineVersionV1, legacyDefinition.Definition.String(), semanticTime, semanticTZ)

	sessionID := mustParseSnapshotID(fixture.ids.new())
	sessionDefinition, err := canonical.MarshalCanonical(struct {
		Version string             `json:"version"`
		IdleGap canonical.Duration `json:"idle_gap_microseconds"`
	}{Version: domain.SessionPolicyVersion, IdleGap: canonical.Duration(30 * 60 * 1_000_000)})
	if err != nil {
		t.Fatal(err)
	}
	mustExec(t, fixture.db, `INSERT INTO sessionization_policy_versions(
		sessionization_policy_version_id, canonical_commit_id, version_key,
		definition, recorded_at, recorded_tz
	) VALUES (?, ?, ?, ?, ?, ?)`, sessionID.String(), fixture.commit["global"],
		domain.SessionPolicyVersion, sessionDefinition.String(), semanticTime, semanticTZ)
	for _, class := range []string{"principles", "persona", "memory_policy"} {
		mustExec(t, fixture.db, `INSERT INTO resident_revision_activations(
			activation_id, canonical_commit_id, resident_id, revision_id,
			actor_principal_id, approval_id, reason_code, reason_content_id,
			recorded_at, recorded_tz
		) VALUES (?, ?, ?, ?, ?, NULL, 'cov2_source_contract', NULL, ?, ?)`,
			fixture.ids.new(), fixture.commit["A"], fixture.resident["A"], fixture.revision["A"][class],
			fixture.principal["human"], semanticTime, semanticTZ)
	}

	legacySourceID := mustParseSnapshotID(fixture.event["A"])
	v2CommitRaw := fixture.ids.new()
	mustExec(t, fixture.db, `INSERT INTO canonical_commits(
		canonical_commit_id, commit_seq, resident_id, committed_at, committed_tz
	) VALUES (?, 4, NULL, ?, ?)`, v2CommitRaw, semanticTime+3, semanticTZ)
	v2PipelineID := mustParseSnapshotID(fixture.ids.new())
	v2Definition, err := domain.DialoguePipelineDefinition(v2PipelineID, domain.DialoguePipelineVersionV2)
	if err != nil {
		t.Fatal(err)
	}
	mustExec(t, fixture.db, `INSERT INTO pipeline_versions(
		pipeline_version_id, canonical_commit_id, pipeline_kind, version_key,
		definition, recorded_at, recorded_tz
	) VALUES (?, ?, 'dialogue', ?, ?, ?, ?)`, v2PipelineID.String(), v2CommitRaw,
		domain.DialoguePipelineVersionV2, v2Definition.Definition.String(), semanticTime+3, semanticTZ)

	currentCommitRaw := fixture.ids.new()
	mustExec(t, fixture.db, `INSERT INTO canonical_commits(
		canonical_commit_id, commit_seq, resident_id, committed_at, committed_tz
	) VALUES (?, 5, ?, ?, ?)`, currentCommitRaw, fixture.resident["A"], semanticTime+4, semanticTZ)
	currentSourceID := mustParseSnapshotID(fixture.ids.new())
	currentContentID := fixture.addContent(t, "A", "event_payload", "post-dialogue-v2 source", "independent")
	mustExec(t, fixture.db, `INSERT INTO events(
		event_id, canonical_commit_id, resident_id, seq, event_type, visibility,
		delivery_screen, delivery_audio, ingress, trust_level, actor_principal_id,
		target_principal_id, generation_run_id, occurred_at, occurred_tz, recorded_at,
		recorded_tz, content_id, payload_commitment, prev_event_hash, event_hash,
		event_hash_algorithm, event_hash_domain, canonicalization_version
	) VALUES (?, ?, ?, 2, 'user_message', 'conversation', 0, 0, 'local_ui', 'trusted', ?, ?, NULL,
		?, ?, ?, ?, ?, ?, ?, ?, 'sha256', 'mahoroba:event-hash:v1', 'mahoroba-jcs-v1')`,
		currentSourceID.String(), currentCommitRaw, fixture.resident["A"], fixture.principal["human"],
		fixture.principal["A"], semanticTime+4, semanticTZ, semanticTime+4, semanticTZ, currentContentID,
		semanticDigest("payload-current"), semanticDigest("event-A"), semanticDigest("event-current"))

	v3CommitRaw := fixture.ids.new()
	mustExec(t, fixture.db, `INSERT INTO canonical_commits(
		canonical_commit_id, commit_seq, resident_id, committed_at, committed_tz
	) VALUES (?, 6, NULL, ?, ?)`, v3CommitRaw, semanticTime+5, semanticTZ)
	v3PipelineID := mustParseSnapshotID(fixture.ids.new())
	v3Definition, err := domain.DialoguePipelineDefinition(v3PipelineID, domain.DialoguePipelineVersionV3)
	if err != nil {
		t.Fatal(err)
	}
	mustExec(t, fixture.db, `INSERT INTO pipeline_versions(
		pipeline_version_id, canonical_commit_id, pipeline_kind, version_key,
		definition, recorded_at, recorded_tz
	) VALUES (?, ?, 'dialogue', ?, ?, ?, ?)`, v3PipelineID.String(), v3CommitRaw,
		domain.DialoguePipelineVersionV3, v3Definition.Definition.String(), semanticTime+5, semanticTZ)

	v3SourceCommitRaw := fixture.ids.new()
	mustExec(t, fixture.db, `INSERT INTO canonical_commits(
		canonical_commit_id, commit_seq, resident_id, committed_at, committed_tz
	) VALUES (?, 7, ?, ?, ?)`, v3SourceCommitRaw, fixture.resident["A"], semanticTime+6, semanticTZ)
	v3SourceID := mustParseSnapshotID(fixture.ids.new())
	v3ContentID := fixture.addContent(t, "A", "event_payload", "post-dialogue-v3 source", "independent")
	mustExec(t, fixture.db, `INSERT INTO events(
		event_id, canonical_commit_id, resident_id, seq, event_type, visibility,
		delivery_screen, delivery_audio, ingress, trust_level, actor_principal_id,
		target_principal_id, generation_run_id, occurred_at, occurred_tz, recorded_at,
		recorded_tz, content_id, payload_commitment, prev_event_hash, event_hash,
		event_hash_algorithm, event_hash_domain, canonicalization_version
	) VALUES (?, ?, ?, 3, 'user_message', 'conversation', 0, 0, 'local_ui', 'trusted', ?, ?, NULL,
		?, ?, ?, ?, ?, ?, ?, ?, 'sha256', 'mahoroba:event-hash:v1', 'mahoroba-jcs-v1')`,
		v3SourceID.String(), v3SourceCommitRaw, fixture.resident["A"], fixture.principal["human"],
		fixture.principal["A"], semanticTime+6, semanticTZ, semanticTime+6, semanticTZ, v3ContentID,
		semanticDigest("payload-v3"), semanticDigest("event-current"), semanticDigest("event-v3"))

	tests := []struct {
		name            string
		sourceID        canonical.ID
		wantPipelineID  canonical.ID
		wantPipelineKey string
	}{
		{name: "source before v2 registration", sourceID: legacySourceID, wantPipelineID: legacyPipelineID, wantPipelineKey: domain.DialoguePipelineVersionV1},
		{name: "source after v2 registration", sourceID: currentSourceID, wantPipelineID: v2PipelineID, wantPipelineKey: domain.DialoguePipelineVersionV2},
		{name: "source after v3 registration", sourceID: v3SourceID, wantPipelineID: v3PipelineID, wantPipelineKey: domain.DialoguePipelineVersionV3},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			envelope, resolvable, err := resolveSyntheticCancellationEnvelope(ctx, fixture.db, domain.MandatoryRecoveryWork{
				Kind:             domain.MandatoryRecoveryDialogue,
				SourceEvent:      domain.Event{ID: test.sourceID, ResidentID: mustParseSnapshotID(fixture.resident["A"])},
				IdempotencyKey:   domain.DialogueObligation(test.sourceID),
				CancellationCode: "source_content_erased",
			})
			if err != nil || !resolvable {
				t.Fatalf("resolve source-bound synthetic envelope = %+v, %v, resolvable=%v", envelope, err, resolvable)
			}
			want, err := domain.SyntheticDialogueExecutionContract(test.wantPipelineKey)
			if err != nil {
				t.Fatal(err)
			}
			got := domain.DialogueExecutionContract{
				PipelineVersionKey:     test.wantPipelineKey,
				PromptTemplateVersion:  envelope.PromptTemplateVersion,
				ContextPolicyVersion:   envelope.ContextPolicyVersion,
				MemoryRenderingVersion: envelope.MemoryRenderingVersion,
			}
			if envelope.PipelineVersionID != test.wantPipelineID || envelope.SessionPolicyID == nil ||
				*envelope.SessionPolicyID != sessionID || got != want {
				t.Fatalf("source-bound synthetic envelope = pipeline %s session %v tuple %+v, want %s/%s/%+v",
					envelope.PipelineVersionID, envelope.SessionPolicyID, got,
					test.wantPipelineID, sessionID, want)
			}
		})
	}
}

func mustInsertCOV2NormalDialogueRun(
	t *testing.T,
	fixture *semanticFixture,
	runID, inputID, outcomeID, pipelineID canonical.ID,
	contract domain.DialogueExecutionContract,
	params, dropped canonical.CanonicalJSON,
) {
	t.Helper()
	key := "dialogue-read:" + runID.String()
	mustExec(t, fixture.db, `INSERT INTO generation_runs(
		generation_run_id, canonical_commit_id, resident_id, purpose, idempotency_key,
		provider, model, model_version, prompt_template_version, pipeline_version_id,
		context_policy_version, sessionization_policy_version_id, memory_rendering_version,
		principles_revision_id, persona_revision_id, memory_policy_revision_id, recall_run_id,
		temperature, top_p, max_tokens, seed, generator_params, as_of, as_of_tz,
		budget_exceeded, dropped_input_summary, requested_at, requested_tz
	) VALUES (?, ?, ?, 'dialogue', ?, 'test-provider', 'test-model', NULL, ?, ?, ?, ?, ?, ?, ?, ?, NULL,
		NULL, NULL, NULL, NULL, ?, ?, ?, 0, ?, ?, ?)`,
		runID.String(), fixture.commit["A"], fixture.resident["A"], key,
		contract.PromptTemplateVersion, pipelineID.String(), contract.ContextPolicyVersion,
		fixture.session, contract.MemoryRenderingVersion, fixture.revision["A"]["principles"],
		fixture.revision["A"]["persona"], fixture.revision["A"]["memory_policy"],
		params.String(), semanticTime, semanticTZ, dropped.String(), semanticTime, semanticTZ)
	mustExec(t, fixture.db, `INSERT INTO generation_run_inputs(
		generation_run_input_id, canonical_commit_id, generation_run_id, ordinal, role,
		source_type, source_id, inclusion_mode, content_id, recorded_at, recorded_tz
	) VALUES (?, ?, ?, 0, 'user', 'event', ?, 'current_input', ?, ?, ?)`,
		inputID.String(), fixture.commit["A"], runID.String(), fixture.event["A"],
		fixture.content["A"]["input"], semanticTime, semanticTZ)
	mustExec(t, fixture.db, `INSERT INTO generation_run_outcomes(
		outcome_id, canonical_commit_id, generation_run_id, attempt_no, state,
		output_content_id, prompt_tokens, completion_tokens, latency, estimated_cost,
		error_class, error_detail_content_id, recorded_at, recorded_tz
	) VALUES (?, ?, ?, 1, 'running', NULL, NULL, NULL, NULL, NULL, NULL, NULL, ?, ?)`,
		outcomeID.String(), fixture.commit["A"], runID.String(), semanticTime, semanticTZ)
}
