package sqlite

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"testing"

	"mahoroba.local/mahoroba/internal/canonical"
	"mahoroba.local/mahoroba/internal/domain"
	"mahoroba.local/mahoroba/internal/memory"
)

func TestCOV1PrepareDialoguePersistsCommitBAtomicallyAfterUnrelatedHeadAdvance(t *testing.T) {
	fixture := newDialoguePrepareWriterFixture(t)
	prepare := fixture.assemble(t)

	// A global commit and Projection refresh may happen while the durable
	// Commit-A obligation is waiting to resume. Neither changes the resident
	// inputs captured at target, so Commit B must remain admissible.
	unrelatedCommit := fixture.id(t)
	mustExec(t, fixture.semantic.db, `INSERT INTO canonical_commits(
		canonical_commit_id, commit_seq, resident_id, committed_at, committed_tz
	) VALUES (?, 9, NULL, ?, ?)`, unrelatedCommit.String(), fixture.target.AsOf.UnixMicro()+1,
		fixture.target.AsOfTZ.String())
	mustExec(t, fixture.semantic.db, `UPDATE projection_watermarks
		SET source_commit_seq = 9, as_of = as_of + 1
		WHERE resident_id = ?`, fixture.residentID.String())

	uow := fixture.begin(t, 10, fixture.target.AsOf+2)
	result, err := uow.PrepareDialogue(context.Background(), prepare)
	if err != nil {
		_ = uow.Rollback(context.Background())
		t.Fatalf("PrepareDialogue: %v", err)
	}
	if result.Resolution != domain.PrepareDialoguePreparedCurrentV3 || result.RunID != prepare.Generation.RunID {
		t.Fatalf("PrepareDialogue result = %+v", result)
	}
	if err := uow.Commit(context.Background()); err != nil {
		t.Fatal(err)
	}

	var runCommit string
	if err := fixture.semantic.db.QueryRow(`SELECT canonical_commit_id FROM generation_runs
		WHERE generation_run_id = ?`, prepare.Generation.RunID.String()).Scan(&runCommit); err != nil {
		t.Fatal(err)
	}
	var ingressCommit string
	if err := fixture.semantic.db.QueryRow(`SELECT canonical_commit_id FROM events
		WHERE event_id = ?`, fixture.sourceEventID.String()).Scan(&ingressCommit); err != nil {
		t.Fatal(err)
	}
	if runCommit == ingressCommit {
		t.Fatalf("Commit A and Commit B share canonical_commit_id %s", runCommit)
	}
	var inputCount, runningCount, recallCount, wrongCommit int64
	if err := fixture.semantic.db.QueryRow(`SELECT
		(SELECT COUNT(*) FROM generation_run_inputs WHERE generation_run_id = ?),
		(SELECT COUNT(*) FROM generation_run_outcomes WHERE generation_run_id = ?
		 AND attempt_no = 1 AND state = 'running'),
		(SELECT COUNT(*) FROM recall_runs WHERE recall_run_id = ?),
		(SELECT COUNT(*) FROM generation_run_inputs WHERE generation_run_id = ?
		 AND canonical_commit_id <> ?)`, prepare.Generation.RunID.String(), prepare.Generation.RunID.String(),
		prepare.Generation.RunID.String(), prepare.Generation.RunID.String(), runCommit).Scan(
		&inputCount, &runningCount, &recallCount, &wrongCommit,
	); err != nil {
		t.Fatal(err)
	}
	if inputCount != int64(len(prepare.Generation.Inputs)) || runningCount != 1 || recallCount != 0 || wrongCommit != 0 {
		t.Fatalf("Commit B rows inputs=%d running=%d recall=%d wrong_commit=%d",
			inputCount, runningCount, recallCount, wrongCommit)
	}
}

func TestCOV5PrepareDialogueRebuildsActivityAtAssemblyTargetHead(t *testing.T) {
	fixture := newDialoguePrepareWriterFixtureWithPolicyAndSource(
		t, memory.DefaultPolicyV1(), []byte("前回の続き"), true,
	)
	prepare := fixture.assemble(t)
	var backfillInputs int
	for _, input := range prepare.Generation.Inputs {
		if input.InclusionMode == "context_backfill" {
			backfillInputs++
		}
	}
	if backfillInputs == 0 {
		t.Fatal("surface-reference fixture did not assemble Target-bound Backfill")
	}

	// A lifecycle transition committed after Assembly must not participate in
	// Commit-B's activity calculation. Give it an event-time anchor at the
	// source instant so this test would fail if validation used the Commit-B
	// UoW head instead of the frozen Assembly Target.
	change := fixture.begin(t, 9, fixture.target.AsOf+1)
	if _, err := change.tx.ExecContext(context.Background(), `INSERT INTO resident_status_transitions(
		resident_status_transition_id, canonical_commit_id, resident_id, from_status, to_status,
		actor_principal_id, reason_code, reason_content_id, occurred_at, occurred_tz,
		recorded_at, recorded_tz
	) VALUES (?, ?, ?, 'draft', 'active', ?, 'cov5_post_target', NULL, ?, ?, ?, ?)`,
		fixture.id(t).String(), change.metadata.CommitID.String(), fixture.residentID.String(),
		fixture.semantic.principal["human"], fixture.target.AsOf.UnixMicro(), fixture.target.AsOfTZ.String(),
		fixture.target.AsOf.UnixMicro(), fixture.target.AsOfTZ.String()); err != nil {
		_ = change.Rollback(context.Background())
		t.Fatalf("insert post-Target lifecycle transition: %v", err)
	}
	if err := change.Commit(context.Background()); err != nil {
		t.Fatal(err)
	}

	uow := fixture.begin(t, 10, fixture.target.AsOf+2)
	if _, err := uow.PrepareDialogue(context.Background(), prepare); err != nil {
		_ = uow.Rollback(context.Background())
		t.Fatalf("PrepareDialogue included post-Target activity input: %v", err)
	}
	if err := uow.Commit(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func TestMandatoryMemoryDialoguePredecessorGateAppliesOnlyToUserMessages(t *testing.T) {
	fixture, _, residentID, closeFixture := newBoundedMemoryDiscoveryFixture(t)
	defer closeFixture()
	commitID := activateMemoryDiscoveryPolicyV3(t, fixture)
	selfTalkRaw := insertMemoryDiscoveryEventAtCommit(t, fixture, 2, "self_talk", commitID)
	selfTalkID, err := canonical.ParseID(selfTalkRaw)
	if err != nil {
		t.Fatal(err)
	}
	var userRaw string
	if err := fixture.db.QueryRow(`SELECT event_id FROM events
		WHERE resident_id = ? AND seq = 1`, residentID.String()).Scan(&userRaw); err != nil {
		t.Fatal(err)
	}
	userID, err := canonical.ParseID(userRaw)
	if err != nil {
		t.Fatal(err)
	}
	tx, err := fixture.db.BeginTx(context.Background(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	uow := &canonicalUoW{tx: tx}

	if err := uow.requireDialoguePreparedForMemory(context.Background(), residentID, userID); err == nil ||
		!strings.Contains(err.Error(), "requires a prepared dialogue obligation") {
		t.Fatalf("user-message predecessor error = %v", err)
	}
	if err := uow.requireDialoguePreparedForMemory(context.Background(), residentID, selfTalkID); err != nil {
		t.Fatalf("self-talk unexpectedly required a dialogue predecessor: %v", err)
	}
}

func TestCOV1PrepareDialogueRecallSuccessAllowsUnrelatedProjectionAdvance(t *testing.T) {
	fixture := newDialoguePrepareWriterFixtureWithPolicy(t, memory.DefaultPolicyV4())
	prepare := fixture.assemble(t)
	if prepare.RecallDisposition != domain.RecallDispositionSuccess {
		t.Fatalf("Recall disposition = %s, want success", prepare.RecallDisposition)
	}
	unrelatedCommit := fixture.id(t)
	mustExec(t, fixture.semantic.db, `INSERT INTO canonical_commits(
		canonical_commit_id, commit_seq, resident_id, committed_at, committed_tz
	) VALUES (?, 9, NULL, ?, ?)`, unrelatedCommit.String(), fixture.target.AsOf.UnixMicro()+1,
		fixture.target.AsOfTZ.String())
	mustExec(t, fixture.semantic.db, `UPDATE projection_watermarks
		SET source_commit_seq = 9, as_of = as_of + 1
		WHERE resident_id = ?`, fixture.residentID.String())
	uow := fixture.begin(t, 10, fixture.target.AsOf+2)
	if _, err := uow.PrepareDialogue(context.Background(), prepare); err != nil {
		_ = uow.Rollback(context.Background())
		t.Fatalf("PrepareDialogue after unrelated Projection advance: %v", err)
	}
	if err := uow.Commit(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func TestCOV1PrepareDialogueRecallSuccessRejectsRelatedChangeWithStaleProjection(t *testing.T) {
	fixture := newDialoguePrepareWriterFixtureWithPolicy(t, memory.DefaultPolicyV4())
	prepare := fixture.assemble(t)
	if prepare.RecallDisposition != domain.RecallDispositionSuccess {
		t.Fatalf("Recall disposition = %s, want success", prepare.RecallDisposition)
	}

	claimID := dialogueAssemblyReadParseID(t, fixture.semantic.claim["A"])
	change := fixture.begin(t, 9, fixture.target.AsOf+1)
	if _, err := change.tx.ExecContext(context.Background(), `INSERT INTO claim_view_scope_assertions(
		view_scope_assertion_id, canonical_commit_id, claim_id, view_scope, actor_principal_id,
		generation_run_id, memory_policy_revision_id, reason_code, reason_content_id,
		recorded_at, recorded_tz
	) VALUES (?, ?, ?, ?, ?, NULL, NULL, 'cov1_related_change', NULL, ?, ?)`,
		fixture.id(t).String(), change.metadata.CommitID.String(), claimID.String(), memory.ScopeAdminOnly,
		fixture.semantic.principal["human"], change.metadata.CommittedAt.UnixMicro(),
		change.metadata.CommittedTZ.String()); err != nil {
		_ = change.Rollback(context.Background())
		t.Fatalf("change Recall-related Canonical scope: %v", err)
	}
	if err := change.Commit(context.Background()); err != nil {
		t.Fatal(err)
	}

	// Deliberately leave both Recall Projections at target.Head. The Writer
	// must inspect Canonical rows directly instead of accepting the stale body.
	uow := fixture.begin(t, 10, fixture.target.AsOf+2)
	_, err := uow.PrepareDialogue(context.Background(), prepare)
	if !errors.Is(err, domain.ErrDialogueAssemblyTargetChanged) {
		_ = uow.Rollback(context.Background())
		t.Fatalf("PrepareDialogue related-change error = %v, want ErrDialogueAssemblyTargetChanged", err)
	}
	if rollbackErr := uow.Rollback(context.Background()); rollbackErr != nil {
		t.Fatal(rollbackErr)
	}
	fixture.requireNoPreparedRowsAtCommit(t, prepare.Generation.RunID, 10)
}

func TestCOV1PrepareDialogueStableObligationConvergesToExistingRun(t *testing.T) {
	fixture := newDialoguePrepareWriterFixture(t)
	prepare := fixture.assemble(t)
	uow := fixture.begin(t, 9, fixture.target.AsOf+1)
	first, err := uow.PrepareDialogue(context.Background(), prepare)
	if err != nil {
		t.Fatal(err)
	}
	if err := uow.Commit(context.Background()); err != nil {
		t.Fatal(err)
	}

	replay := prepare
	replay.Generation.RunID = fixture.id(t)
	replay.Generation.RunningOutcomeID = fixture.id(t)
	uow = fixture.begin(t, 10, fixture.target.AsOf+2)
	result, err := uow.PrepareDialogue(context.Background(), replay)
	if !errors.Is(err, canonical.ErrNoMutation) {
		_ = uow.Rollback(context.Background())
		t.Fatalf("replay error = %v, want ErrNoMutation", err)
	}
	if result.Resolution != domain.PrepareDialogueExistingCurrentV3 || result.RunID != first.RunID {
		_ = uow.Rollback(context.Background())
		t.Fatalf("replay result = %+v, want existing %s", result, first.RunID)
	}
	if err := uow.Rollback(context.Background()); err != nil {
		t.Fatal(err)
	}
	var runs int64
	if err := fixture.semantic.db.QueryRow(`SELECT COUNT(*) FROM generation_runs
		WHERE resident_id = ? AND idempotency_key = ?`, fixture.residentID.String(),
		domain.DialogueObligation(fixture.sourceEventID)).Scan(&runs); err != nil {
		t.Fatal(err)
	}
	if runs != 1 {
		t.Fatalf("stable obligation run count = %d, want 1", runs)
	}
}

func TestCOV1PrepareDialogueRecognizesValidLegacyAtomicObligation(t *testing.T) {
	fixture := newDialoguePrepareWriterFixture(t)
	existingRun := fixture.seedLegacyPreparedDialogue(t)
	prepare := fixture.assemble(t)
	uow := fixture.begin(t, 9, fixture.target.AsOf+1)
	result, err := uow.PrepareDialogue(context.Background(), prepare)
	if !errors.Is(err, canonical.ErrNoMutation) {
		_ = uow.Rollback(context.Background())
		t.Fatalf("legacy replay error = %v, want ErrNoMutation", err)
	}
	if result.Resolution != domain.PrepareDialogueDispatchExistingFrozenRun || result.RunID != existingRun {
		_ = uow.Rollback(context.Background())
		t.Fatalf("legacy replay result = %+v, want existing %s", result, existingRun)
	}
	if err := uow.Rollback(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func TestCOVR04PrepareDialogueRoutesRecallBearingLegacyRunToFrozenDispatch(t *testing.T) {
	fixture := newDialoguePrepareWriterFixtureWithPolicy(t, memory.DefaultPolicyV2())
	prepare := fixture.assemble(t)
	existingRun := fixture.seedLegacyPreparedDialogueWithRecall(t)

	uow := fixture.begin(t, 9, fixture.target.AsOf+1)
	result, err := uow.PrepareDialogue(context.Background(), prepare)
	if !errors.Is(err, canonical.ErrNoMutation) {
		_ = uow.Rollback(context.Background())
		t.Fatalf("Recall-bearing legacy replay error = %v, want ErrNoMutation", err)
	}
	if result.Resolution != domain.PrepareDialogueDispatchExistingFrozenRun || result.RunID != existingRun {
		_ = uow.Rollback(context.Background())
		t.Fatalf("Recall-bearing legacy replay result = %+v, want frozen dispatch %s", result, existingRun)
	}
	if err := uow.Rollback(context.Background()); err != nil {
		t.Fatal(err)
	}
	fixture.requireNoPreparedRowsAtCommit(t, prepare.Generation.RunID, 9)

	var obligationRuns, inputs, outcomes, recalls, usages int64
	if err := fixture.semantic.db.QueryRow(`SELECT
		(SELECT COUNT(*) FROM generation_runs WHERE resident_id = ? AND idempotency_key = ?),
		(SELECT COUNT(*) FROM generation_run_inputs WHERE generation_run_id = ?),
		(SELECT COUNT(*) FROM generation_run_outcomes WHERE generation_run_id = ?),
		(SELECT COUNT(*) FROM recall_runs recall JOIN generation_runs run
		 ON run.recall_run_id = recall.recall_run_id WHERE run.generation_run_id = ?),
		(SELECT COUNT(*) FROM claim_usages WHERE generation_run_id = ?)`,
		fixture.residentID.String(), domain.DialogueObligation(fixture.sourceEventID),
		existingRun.String(), existingRun.String(), existingRun.String(), existingRun.String(),
	).Scan(&obligationRuns, &inputs, &outcomes, &recalls, &usages); err != nil {
		t.Fatal(err)
	}
	if obligationRuns != 1 || inputs != 6 || outcomes != 1 || recalls != 1 || usages != 3 {
		t.Fatalf("frozen legacy rows runs=%d inputs=%d outcomes=%d recalls=%d usages=%d",
			obligationRuns, inputs, outcomes, recalls, usages)
	}
}

func TestCOV1PrepareDialogueRejectsFrozenRuntimeTamperWithoutRows(t *testing.T) {
	fixture := newDialoguePrepareWriterFixture(t)
	prepare := fixture.assemble(t)
	for index := range prepare.Generation.Inputs {
		input := &prepare.Generation.Inputs[index]
		if input.SourceType != "runtime_projection" {
			continue
		}
		input.Content = dialoguePrepareWriterContent(t, input.Content.ID, fixture.residentID,
			"generation_input", []byte("runtime_state=active; memory_recall=enabled; self_talk=disabled"),
			"independent", input.Content.CommitmentSalt)
	}
	uow := fixture.begin(t, 9, fixture.target.AsOf+1)
	_, err := uow.PrepareDialogue(context.Background(), prepare)
	if err == nil {
		_ = uow.Rollback(context.Background())
		t.Fatal("PrepareDialogue accepted tampered runtime Projection bytes")
	}
	if rollbackErr := uow.Rollback(context.Background()); rollbackErr != nil {
		t.Fatal(rollbackErr)
	}
	fixture.requireNoPreparedRows(t, prepare.Generation.RunID)
}

func TestCOV1PrepareDialogueLateFailureRollsBackWholeCommitB(t *testing.T) {
	fixture := newDialoguePrepareWriterFixture(t)
	request := fixture.request(t)
	collision := fixture.id(t)
	mustExec(t, fixture.semantic.db, `INSERT INTO generation_run_outcomes(
		outcome_id, canonical_commit_id, generation_run_id, attempt_no, state,
		output_content_id, prompt_tokens, completion_tokens, latency, estimated_cost,
		error_class, error_detail_content_id, recorded_at, recorded_tz
	) VALUES (?, ?, ?, 1, 'running', NULL, NULL, NULL, NULL, NULL, NULL, NULL, ?, ?)`,
		collision.String(), fixture.semantic.commit["A"], fixture.semantic.run["A"], semanticTime, semanticTZ)
	request.RunningOutcomeID = collision
	assembled, err := fixture.repository.AssembleDialogue(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	uow := fixture.begin(t, 9, fixture.target.AsOf+1)
	_, err = uow.PrepareDialogue(context.Background(), assembled.Prepare)
	if err == nil {
		_ = uow.Rollback(context.Background())
		t.Fatal("PrepareDialogue accepted duplicate late outcome identity")
	}
	if rollbackErr := uow.Rollback(context.Background()); rollbackErr != nil {
		t.Fatal(rollbackErr)
	}
	fixture.requireNoPreparedRows(t, assembled.Prepare.Generation.RunID)
}

func TestCOV1PrepareDialogueSuccessPersistsRecallUsageAndGenerationInOneCommit(t *testing.T) {
	fixture := newDialoguePrepareWriterFixtureWithPolicy(t, memory.DefaultPolicyV4())
	prepare := fixture.assemble(t)
	if prepare.RecallDisposition != domain.RecallDispositionSuccess || prepare.Recall == nil ||
		len(prepare.Recall.Usages) == 0 {
		t.Fatalf("assembled Recall branch = %s recall=%+v", prepare.RecallDisposition, prepare.Recall)
	}
	uow := fixture.begin(t, 9, fixture.target.AsOf+1)
	if _, err := uow.PrepareDialogue(context.Background(), prepare); err != nil {
		_ = uow.Rollback(context.Background())
		t.Fatalf("PrepareDialogue success Recall: %v", err)
	}
	commitID := uow.metadata.CommitID
	if err := uow.Commit(context.Background()); err != nil {
		t.Fatal(err)
	}
	var recalls, usages, wrongLinks int64
	if err := fixture.semantic.db.QueryRow(`SELECT
		(SELECT COUNT(*) FROM recall_runs WHERE recall_run_id = ? AND canonical_commit_id = ?),
		(SELECT COUNT(*) FROM claim_usages WHERE recall_run_id = ? AND generation_run_id = ?),
		(SELECT COUNT(*) FROM claim_usages WHERE recall_run_id = ?
		 AND (generation_run_id <> ? OR canonical_commit_id <> ?))`,
		prepare.Recall.RunID.String(), commitID.String(), prepare.Recall.RunID.String(),
		prepare.Generation.RunID.String(), prepare.Recall.RunID.String(), prepare.Generation.RunID.String(),
		commitID.String()).Scan(&recalls, &usages, &wrongLinks); err != nil {
		t.Fatal(err)
	}
	if recalls != 1 || usages != int64(len(prepare.Recall.Usages)) || wrongLinks != 0 {
		t.Fatalf("Recall Commit B rows recall=%d usages=%d wrong_links=%d", recalls, usages, wrongLinks)
	}
}

func TestCOV1PrepareDialogueRecallSuccessPersistsZeroCandidateRun(t *testing.T) {
	fixture := newDialoguePrepareWriterFixtureWithPolicy(t, memory.DefaultPolicyV4())
	mustExec(t, fixture.semantic.db, `UPDATE claim_states SET status = 'invalidated' WHERE resident_id = ?`, fixture.residentID.String())
	prepare := fixture.assemble(t)
	if prepare.RecallDisposition != domain.RecallDispositionSuccess || prepare.Recall == nil ||
		len(prepare.Recall.Usages) != 0 {
		t.Fatalf("zero-candidate Recall branch = %s recall=%+v", prepare.RecallDisposition, prepare.Recall)
	}
	for _, input := range prepare.Generation.Inputs {
		if input.SourceType == "claim" || input.InclusionMode == "memory_recall" {
			t.Fatalf("zero-candidate Recall contains claim input %+v", input)
		}
	}
	uow := fixture.begin(t, 9, fixture.target.AsOf+1)
	if _, err := uow.PrepareDialogue(context.Background(), prepare); err != nil {
		_ = uow.Rollback(context.Background())
		t.Fatalf("PrepareDialogue zero-candidate Recall: %v", err)
	}
	if err := uow.Commit(context.Background()); err != nil {
		t.Fatal(err)
	}
	var recalls, usages, claimInputs, running int64
	var renderingVersion string
	if err := fixture.semantic.db.QueryRow(`SELECT
		(SELECT COUNT(*) FROM recall_runs WHERE recall_run_id = ?),
		(SELECT COUNT(*) FROM claim_usages WHERE recall_run_id = ?),
		(SELECT COUNT(*) FROM generation_run_inputs
		 WHERE generation_run_id = ? AND source_type = 'claim' AND inclusion_mode = 'memory_recall'),
		(SELECT COUNT(*) FROM generation_run_outcomes
		 WHERE generation_run_id = ? AND attempt_no = 1 AND state = 'running'),
		(SELECT memory_rendering_version FROM generation_runs WHERE generation_run_id = ?)`,
		prepare.Recall.RunID.String(), prepare.Recall.RunID.String(), prepare.Generation.RunID.String(),
		prepare.Generation.RunID.String(), prepare.Generation.RunID.String(),
	).Scan(&recalls, &usages, &claimInputs, &running, &renderingVersion); err != nil {
		t.Fatal(err)
	}
	if recalls != 1 || usages != 0 || claimInputs != 0 || running != 1 ||
		renderingVersion != domain.MemoryRenderingVersionV2 {
		t.Fatalf("zero-candidate Commit B recall=%d usages=%d inputs=%d running=%d rendering=%q",
			recalls, usages, claimInputs, running, renderingVersion)
	}
}

func TestCOV1PrepareDialogueExplicitFallbackPersistsNoRecallRows(t *testing.T) {
	fixture := newDialoguePrepareWriterFixtureWithPolicy(t, memory.DefaultPolicyV4())
	mustExec(t, fixture.semantic.db, `DELETE FROM claim_states WHERE resident_id = ?`, fixture.residentID.String())
	prepare := fixture.assemble(t)
	if prepare.RecallDisposition != domain.RecallDispositionExplicitFallback || prepare.Recall != nil ||
		prepare.RecallFallbackReason == "" || prepare.Generation.RecallRunID != nil {
		t.Fatalf("explicit fallback branch = %s reason=%q recall=%+v generation_recall=%+v",
			prepare.RecallDisposition, prepare.RecallFallbackReason, prepare.Recall, prepare.Generation.RecallRunID)
	}
	uow := fixture.begin(t, 9, fixture.target.AsOf+1)
	if _, err := uow.PrepareDialogue(context.Background(), prepare); err != nil {
		_ = uow.Rollback(context.Background())
		t.Fatalf("PrepareDialogue explicit fallback: %v", err)
	}
	if err := uow.Commit(context.Background()); err != nil {
		t.Fatal(err)
	}
	var recalls, usages, claimInputs, running int64
	var renderingVersion, dropped string
	if err := fixture.semantic.db.QueryRow(`SELECT
		(SELECT COUNT(*) FROM recall_runs WHERE resident_id = ? AND as_of = ?),
		(SELECT COUNT(*) FROM claim_usages WHERE generation_run_id = ?),
		(SELECT COUNT(*) FROM generation_run_inputs
		 WHERE generation_run_id = ? AND source_type = 'claim' AND inclusion_mode = 'memory_recall'),
		(SELECT COUNT(*) FROM generation_run_outcomes
		 WHERE generation_run_id = ? AND attempt_no = 1 AND state = 'running'),
		memory_rendering_version, dropped_input_summary
		FROM generation_runs WHERE generation_run_id = ?`,
		fixture.residentID.String(), fixture.target.AsOf.UnixMicro(), prepare.Generation.RunID.String(),
		prepare.Generation.RunID.String(), prepare.Generation.RunID.String(), prepare.Generation.RunID.String(),
	).Scan(&recalls, &usages, &claimInputs, &running, &renderingVersion, &dropped); err != nil {
		t.Fatal(err)
	}
	if recalls != 0 || usages != 0 || claimInputs != 0 || running != 1 ||
		renderingVersion != domain.MemoryRenderingVersionV2 ||
		!strings.Contains(dropped, `"memory_recall":"`+string(prepare.RecallFallbackReason)+`"`) {
		t.Fatalf("fallback Commit B recall=%d usages=%d inputs=%d running=%d rendering=%q dropped=%s",
			recalls, usages, claimInputs, running, renderingVersion, dropped)
	}
}

func TestCOV1PrepareDialoguePolicyDisabledPersistsCurrentNormalTupleWithoutFallback(t *testing.T) {
	fixture := newDialoguePrepareWriterFixture(t)
	prepare := fixture.assemble(t)
	if prepare.RecallDisposition != domain.RecallDispositionPolicyDisabled || prepare.Recall != nil ||
		prepare.RecallFallbackReason != "" || prepare.Generation.RecallRunID != nil {
		t.Fatalf("policy-disabled branch = %s reason=%q recall=%+v generation_recall=%+v",
			prepare.RecallDisposition, prepare.RecallFallbackReason, prepare.Recall, prepare.Generation.RecallRunID)
	}
	uow := fixture.begin(t, 9, fixture.target.AsOf+1)
	if _, err := uow.PrepareDialogue(context.Background(), prepare); err != nil {
		_ = uow.Rollback(context.Background())
		t.Fatalf("PrepareDialogue policy disabled: %v", err)
	}
	if err := uow.Commit(context.Background()); err != nil {
		t.Fatal(err)
	}
	var recalls, usages, claimInputs, running int64
	var pipelineKey, promptVersion, contextVersion, renderingVersion, dropped string
	if err := fixture.semantic.db.QueryRow(`SELECT
		(SELECT COUNT(*) FROM recall_runs WHERE resident_id = ? AND as_of = ?),
		(SELECT COUNT(*) FROM claim_usages WHERE generation_run_id = run.generation_run_id),
		(SELECT COUNT(*) FROM generation_run_inputs input
		 WHERE input.generation_run_id = run.generation_run_id
		   AND input.source_type = 'claim' AND input.inclusion_mode = 'memory_recall'),
		(SELECT COUNT(*) FROM generation_run_outcomes outcome
		 WHERE outcome.generation_run_id = run.generation_run_id
		   AND outcome.attempt_no = 1 AND outcome.state = 'running'),
		pipeline.version_key, run.prompt_template_version, run.context_policy_version,
		run.memory_rendering_version, run.dropped_input_summary
		FROM generation_runs run
		JOIN pipeline_versions pipeline ON pipeline.pipeline_version_id = run.pipeline_version_id
		WHERE run.generation_run_id = ?`, fixture.residentID.String(), fixture.target.AsOf.UnixMicro(),
		prepare.Generation.RunID.String(),
	).Scan(&recalls, &usages, &claimInputs, &running, &pipelineKey, &promptVersion,
		&contextVersion, &renderingVersion, &dropped); err != nil {
		t.Fatal(err)
	}
	current := domain.CurrentDialogueNormalExecutionContract()
	if recalls != 0 || usages != 0 || claimInputs != 0 || running != 1 ||
		pipelineKey != current.PipelineVersionKey || promptVersion != current.PromptTemplateVersion ||
		contextVersion != current.ContextPolicyVersion || renderingVersion != current.MemoryRenderingVersion ||
		strings.Contains(dropped, `"memory_recall"`) {
		t.Fatalf("policy-disabled Commit B recall=%d usages=%d inputs=%d running=%d tuple=%s/%s/%s/%s dropped=%s",
			recalls, usages, claimInputs, running, pipelineKey, promptVersion, contextVersion, renderingVersion, dropped)
	}
}

func TestCOV1PrepareDialogueRejectsFakeProvenanceDuplicateAndRenderedTamper(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*testing.T, *dialoguePrepareWriterFixture, *domain.PrepareDialogue)
	}{
		{
			name: "fake provenance_duplicate",
			mutate: func(t *testing.T, _ *dialoguePrepareWriterFixture, prepare *domain.PrepareDialogue) {
				var usages []domain.RecallUsage
				for _, usage := range prepare.Recall.Usages {
					switch usage.Type {
					case memory.UsageSelected:
						usage.ExclusionReason = memory.ExclusionProvenanceDuplicateV2
						usages = append(usages, usage)
					case memory.UsagePromptIncluded:
						// A forged dedup claim omits its prompt provenance and input.
					default:
						usages = append(usages, usage)
					}
				}
				prepare.Recall.Usages = usages
				var inputs []domain.GenerationInput
				for _, input := range prepare.Generation.Inputs {
					if input.SourceType == "claim" {
						continue
					}
					input.Ordinal = int64(len(inputs))
					inputs = append(inputs, input)
				}
				prepare.Generation.Inputs = inputs
			},
		},
		{
			name: "rendered bytes",
			mutate: func(t *testing.T, fixture *dialoguePrepareWriterFixture, prepare *domain.PrepareDialogue) {
				for index := range prepare.Generation.Inputs {
					input := &prepare.Generation.Inputs[index]
					if input.SourceType != "claim" {
						continue
					}
					input.Content = dialoguePrepareWriterContent(t, input.Content.ID, fixture.residentID,
						"generation_input", []byte("tampered rendering"), "independent", input.Content.CommitmentSalt)
				}
			},
		},
		{
			name: "structured currentness snapshot",
			mutate: func(t *testing.T, _ *dialoguePrepareWriterFixture, prepare *domain.PrepareDialogue) {
				if len(prepare.RecallCandidates) == 0 {
					t.Fatal("assembled Recall has no candidates")
				}
				prepare.RecallCandidates[0].Currentness = canonical.Ratio(999_999)
			},
		},
		{
			name: "structured last-confirmed snapshot",
			mutate: func(t *testing.T, _ *dialoguePrepareWriterFixture, prepare *domain.PrepareDialogue) {
				if len(prepare.RecallCandidates) == 0 {
					t.Fatal("assembled Recall has no candidates")
				}
				if prepare.RecallCandidates[0].LastConfirmed == nil {
					value := canonical.Instant(1)
					prepare.RecallCandidates[0].LastConfirmed = &value
				} else {
					prepare.RecallCandidates[0].LastConfirmed = nil
				}
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			fixture := newDialoguePrepareWriterFixtureWithPolicy(t, memory.DefaultPolicyV4())
			prepare := fixture.assemble(t)
			test.mutate(t, fixture, &prepare)
			uow := fixture.begin(t, 9, fixture.target.AsOf+1)
			if _, err := uow.PrepareDialogue(context.Background(), prepare); err == nil {
				_ = uow.Rollback(context.Background())
				t.Fatal("PrepareDialogue accepted forged context-v3 provenance")
			}
			if err := uow.Rollback(context.Background()); err != nil {
				t.Fatal(err)
			}
			fixture.requireNoPreparedRows(t, prepare.Generation.RunID)
		})
	}
}

func TestCOV6PreparedDialogueCandidateTransferAndEqualityIncludeStructuredRenderingInputs(t *testing.T) {
	claimID := dialogueAssemblyReadParseID(t, "00000000000000000000000001")
	sourceID := dialogueAssemblyReadParseID(t, "00000000000000000000000002")
	confirmed := canonical.Instant(123_456)
	snapshot := domain.RecallCandidateSnapshot{
		ClaimID: claimID, Statement: "structured candidate",
		ContextCompatibility: canonical.Ratio(1_000_000), Salience: canonical.Ratio(900_000),
		Confidence: canonical.Ratio(750_000), Currentness: canonical.Ratio(400_000),
		LastConfirmed: &confirmed, Status: "active", Stage: "floating",
		TemporalRelation: "stale_unknown", SourceEventIDs: []canonical.ID{sourceID},
	}
	candidates, err := preparedDialogueMemoryCandidates([]domain.RecallCandidateSnapshot{snapshot})
	if err != nil {
		t.Fatal(err)
	}
	if len(candidates) != 1 || candidates[0].Currentness != snapshot.Currentness ||
		!sameRecallInstant(candidates[0].LastConfirmed, snapshot.LastConfirmed) ||
		!slices.Equal(candidates[0].SourceEventIDs, snapshot.SourceEventIDs) {
		t.Fatalf("prepared candidate = %+v, want structured snapshot %+v", candidates, snapshot)
	}
	if candidates[0].LastConfirmed == snapshot.LastConfirmed {
		t.Fatal("prepared candidate aliases the untrusted LastConfirmed pointer")
	}
	equal := append([]memory.RecallCandidate(nil), candidates...)
	if !equalRecallCandidates(candidates, equal) {
		t.Fatal("identical structured candidates compare unequal")
	}
	equal[0].Currentness = canonical.Ratio(400_001)
	if equalRecallCandidates(candidates, equal) {
		t.Fatal("candidate equality ignored Currentness")
	}
	equal[0].Currentness = candidates[0].Currentness
	otherConfirmed := canonical.Instant(123_457)
	equal[0].LastConfirmed = &otherConfirmed
	if equalRecallCandidates(candidates, equal) {
		t.Fatal("candidate equality ignored LastConfirmed")
	}
}

func TestCOV56ExistingPreparedDialogueSourceCommitOrderByExecutionClass(t *testing.T) {
	tests := []struct {
		name        string
		execution   domain.DialogueExecutionClass
		source, run int64
		want        bool
	}{
		{name: "v1 atomic", execution: domain.DialogueExecutionLegacyNormal, source: 8, run: 8, want: true},
		{name: "v1 split rejected", execution: domain.DialogueExecutionLegacyNormal, source: 8, run: 9},
		{name: "v2 prior split", execution: domain.DialogueExecutionPriorSplitNormal, source: 8, run: 9, want: true},
		{name: "v2 same commit rejected", execution: domain.DialogueExecutionPriorSplitNormal, source: 8, run: 8},
		{name: "v3 current split", execution: domain.DialogueExecutionCurrentNormal, source: 8, run: 9, want: true},
		{name: "v3 future source rejected", execution: domain.DialogueExecutionCurrentNormal, source: 10, run: 9},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if got := validPreparedDialogueSourceCommitOrder(test.execution, test.source, test.run); got != test.want {
				t.Fatalf("source/run order for %s = %v, want %v", test.execution, got, test.want)
			}
		})
	}
}

func TestCOV6DialogueV3LandingRebuildsRenderingFromPinnedTarget(t *testing.T) {
	fixture := newDialoguePrepareWriterFixtureWithPolicy(t, memory.DefaultPolicyV4())
	prepare := fixture.assemble(t)
	var rendered []byte
	for _, input := range prepare.Generation.Inputs {
		if input.SourceType == "claim" && input.InclusionMode == "memory_recall" {
			rendered = append([]byte(nil), input.Content.Bytes...)
			break
		}
	}
	if rendered == nil || !bytes.Contains(rendered, []byte(`"version":"memory-rendering-v2"`)) ||
		!bytes.Contains(rendered, []byte(`"currentness":"1000000"`)) ||
		!bytes.Contains(rendered, []byte(`"certainty":"high"`)) ||
		!bytes.Contains(rendered, []byte(`"last_confirmed":"`+fmt.Sprint(semanticTime)+`"`)) {
		t.Fatalf("assembled rendering-v2 bytes = %s", rendered)
	}

	prepareUOW := fixture.begin(t, 9, fixture.target.AsOf+1)
	if _, err := prepareUOW.PrepareDialogue(context.Background(), prepare); err != nil {
		_ = prepareUOW.Rollback(context.Background())
		t.Fatalf("PrepareDialogue: %v", err)
	}
	if err := prepareUOW.Commit(context.Background()); err != nil {
		t.Fatal(err)
	}

	// Landing must not reinterpret the run with a later mutable Projection
	// body. The exact structured fields are replayed from Canonical rows at the
	// Recall query's pinned head/as_of instead.
	mustExec(t, fixture.semantic.db, `UPDATE claim_states SET confidence = 0,
		currentness = 250000, temporal_relation = 'future' WHERE resident_id = ?`,
		fixture.residentID.String())
	landingUOW := fixture.begin(t, 10, fixture.target.AsOf+2)
	if err := landingUOW.revalidateDialogueInputsAtLanding(
		context.Background(), prepare.Generation.RunID, fixture.residentID,
	); err != nil {
		_ = landingUOW.Rollback(context.Background())
		t.Fatalf("dialogue-v3 Landing revalidation: %v", err)
	}
	if err := landingUOW.Rollback(context.Background()); err != nil {
		t.Fatal(err)
	}

	// Canonical evidence is immutable in production. Simulate corruption after
	// Commit B and prove Landing replays last_confirmed from the pinned Target
	// instead of trusting the frozen generation bytes.
	mustExec(t, fixture.semantic.db, `DROP TRIGGER trg_claim_evidence_no_update`)
	mustExec(t, fixture.semantic.db, `UPDATE claim_evidence SET recorded_at = recorded_at - 1
		WHERE claim_id = ? AND polarity = 'support'`, fixture.semantic.claim["A"])
	tamperedUOW := fixture.begin(t, 10, fixture.target.AsOf+2)
	if err := tamperedUOW.revalidateDialogueInputsAtLanding(
		context.Background(), prepare.Generation.RunID, fixture.residentID,
	); err == nil {
		_ = tamperedUOW.Rollback(context.Background())
		t.Fatal("dialogue-v3 Landing accepted tampered pinned last_confirmed provenance")
	}
	if err := tamperedUOW.Rollback(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func TestCOV56DialogueV3LandingRevalidatesCanonicalEventRevisionAndFrozenInputs(t *testing.T) {
	type mutation func(*testing.T, *dialoguePrepareWriterFixture, domain.PrepareDialogue)
	tests := []struct {
		name     string
		source   []byte
		backfill bool
		corrupt  mutation
	}{
		{
			name: "current source erased",
			corrupt: func(t *testing.T, fixture *dialoguePrepareWriterFixture, _ domain.PrepareDialogue) {
				var contentRaw string
				if err := fixture.semantic.db.QueryRow(`SELECT content_id FROM events WHERE event_id = ?`,
					fixture.sourceEventID.String()).Scan(&contentRaw); err != nil {
					t.Fatal(err)
				}
				mustExec(t, fixture.semantic.db, `UPDATE content_objects
					SET erasure_state = 'erased', blob_hash = NULL, commitment_salt = NULL
					WHERE content_id = ?`, contentRaw)
			},
		},
		{
			name:     "backfill loses conversation visibility",
			source:   []byte("continue"),
			backfill: true,
			corrupt: func(t *testing.T, fixture *dialoguePrepareWriterFixture, prepare domain.PrepareDialogue) {
				var eventRaw string
				if err := fixture.semantic.db.QueryRow(`SELECT source_id FROM generation_run_inputs
					WHERE generation_run_id = ? AND inclusion_mode = 'context_backfill' LIMIT 1`,
					prepare.Generation.RunID.String()).Scan(&eventRaw); err != nil {
					t.Fatal(err)
				}
				mustExec(t, fixture.semantic.db, `DROP TRIGGER trg_events_no_update`)
				mustExec(t, fixture.semantic.db, `UPDATE events
					SET event_type = 'self_talk', visibility = 'internal', delivery_screen = 0,
					    delivery_audio = 0, ingress = 'resident_runtime', generation_run_id = ?
					WHERE event_id = ?`, prepare.Generation.RunID.String(), eventRaw)
			},
		},
		{
			name: "pinned revision erased",
			corrupt: func(t *testing.T, fixture *dialoguePrepareWriterFixture, prepare domain.PrepareDialogue) {
				var contentRaw string
				if err := fixture.semantic.db.QueryRow(`SELECT content_id FROM resident_revisions
					WHERE revision_id = ?`, prepare.Generation.PersonaRevisionID.String()).Scan(&contentRaw); err != nil {
					t.Fatal(err)
				}
				mustExec(t, fixture.semantic.db, `UPDATE content_objects
					SET erasure_state = 'erased', blob_hash = NULL, commitment_salt = NULL
					WHERE content_id = ?`, contentRaw)
			},
		},
		{
			name: "persisted current role tampered",
			corrupt: func(t *testing.T, fixture *dialoguePrepareWriterFixture, prepare domain.PrepareDialogue) {
				mustExec(t, fixture.semantic.db, `DROP TRIGGER trg_generation_run_inputs_no_update`)
				mustExec(t, fixture.semantic.db, `UPDATE generation_run_inputs SET role = 'assistant'
					WHERE generation_run_id = ? AND inclusion_mode = 'current_input'`,
					prepare.Generation.RunID.String())
			},
		},
		{
			name: "frozen input commitment tampered",
			corrupt: func(t *testing.T, fixture *dialoguePrepareWriterFixture, prepare domain.PrepareDialogue) {
				var contentRaw string
				if err := fixture.semantic.db.QueryRow(`SELECT content_id FROM generation_run_inputs
					WHERE generation_run_id = ? AND inclusion_mode = 'current_input'`,
					prepare.Generation.RunID.String()).Scan(&contentRaw); err != nil {
					t.Fatal(err)
				}
				mustExec(t, fixture.semantic.db, `DROP TRIGGER trg_content_objects_erasure_only`)
				mustExec(t, fixture.semantic.db, `UPDATE content_objects SET commitment = ? WHERE content_id = ?`,
					semanticDigest("tampered-v3-frozen-input"), contentRaw)
			},
		},
		{
			name: "frozen input content object replaced by source object",
			corrupt: func(t *testing.T, fixture *dialoguePrepareWriterFixture, prepare domain.PrepareDialogue) {
				var sourceContentRaw string
				if err := fixture.semantic.db.QueryRow(`SELECT content_id FROM events WHERE event_id = ?`,
					fixture.sourceEventID.String()).Scan(&sourceContentRaw); err != nil {
					t.Fatal(err)
				}
				mustExec(t, fixture.semantic.db, `DROP TRIGGER trg_generation_run_inputs_no_update`)
				mustExec(t, fixture.semantic.db, `UPDATE generation_run_inputs SET content_id = ?
					WHERE generation_run_id = ? AND inclusion_mode = 'current_input'`,
					sourceContentRaw, prepare.Generation.RunID.String())
			},
		},
		{
			name: "current event moved beyond Commit-B ceiling",
			corrupt: func(t *testing.T, fixture *dialoguePrepareWriterFixture, prepare domain.PrepareDialogue) {
				var commitRaw string
				if err := fixture.semantic.db.QueryRow(`SELECT canonical_commit_id FROM generation_runs
					WHERE generation_run_id = ?`, prepare.Generation.RunID.String()).Scan(&commitRaw); err != nil {
					t.Fatal(err)
				}
				mustExec(t, fixture.semantic.db, `DROP TRIGGER trg_events_no_update`)
				mustExec(t, fixture.semantic.db, `UPDATE events SET canonical_commit_id = ? WHERE event_id = ?`,
					commitRaw, fixture.sourceEventID.String())
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			source := test.source
			if source == nil {
				source = []byte("writer source")
			}
			fixture := newDialoguePrepareWriterFixtureWithPolicyAndSource(
				t, memory.DefaultPolicyV1(), source, test.backfill,
			)
			prepare := fixture.assemble(t)
			prepareUOW := fixture.begin(t, 9, fixture.target.AsOf+1)
			if _, err := prepareUOW.PrepareDialogue(context.Background(), prepare); err != nil {
				_ = prepareUOW.Rollback(context.Background())
				t.Fatalf("PrepareDialogue: %v", err)
			}
			if err := prepareUOW.Commit(context.Background()); err != nil {
				t.Fatal(err)
			}

			test.corrupt(t, fixture, prepare)
			landingUOW := fixture.begin(t, 10, fixture.target.AsOf+2)
			if err := landingUOW.revalidateDialogueInputsAtLanding(
				context.Background(), prepare.Generation.RunID, fixture.residentID,
			); err == nil {
				_ = landingUOW.Rollback(context.Background())
				t.Fatal("dialogue-v3 Landing accepted tampered Canonical input provenance")
			}
			if err := landingUOW.Rollback(context.Background()); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestCOV6DialogueV3LandingRejectsRecallQueryContentOutsideSourceObligation(t *testing.T) {
	fixture := newDialoguePrepareWriterFixtureWithPolicy(t, memory.DefaultPolicyV4())
	prepare := fixture.assemble(t)
	prepareUOW := fixture.begin(t, 9, fixture.target.AsOf+1)
	if _, err := prepareUOW.PrepareDialogue(context.Background(), prepare); err != nil {
		_ = prepareUOW.Rollback(context.Background())
		t.Fatalf("PrepareDialogue: %v", err)
	}
	if err := prepareUOW.Commit(context.Background()); err != nil {
		t.Fatal(err)
	}

	mustExec(t, fixture.semantic.db, `DROP TRIGGER trg_recall_runs_no_update`)
	mustExec(t, fixture.semantic.db, `UPDATE recall_runs SET query_content_id = ?
		WHERE recall_run_id = ?`, fixture.semantic.content["A"]["claim"], prepare.Recall.RunID.String())
	landingUOW := fixture.begin(t, 10, fixture.target.AsOf+2)
	if err := landingUOW.revalidateDialogueInputsAtLanding(
		context.Background(), prepare.Generation.RunID, fixture.residentID,
	); err == nil {
		_ = landingUOW.Rollback(context.Background())
		t.Fatal("dialogue-v3 Landing accepted Recall query content outside the source obligation")
	}
	if err := landingUOW.Rollback(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func TestCOV6DialogueV2LandingRevalidatesHistoricalRenderingV1(t *testing.T) {
	fixture, closeFixture := newSemanticFixture(t)
	defer closeFixture()
	ctx := context.Background()

	pipelineID := dialogueAssemblyReadParseID(t, fixture.ids.new())
	definition, err := domain.DialoguePipelineDefinition(pipelineID, domain.DialoguePipelineVersionV2)
	if err != nil {
		t.Fatal(err)
	}
	mustExec(t, fixture.db, `INSERT INTO pipeline_versions(
		pipeline_version_id, canonical_commit_id, pipeline_kind, version_key,
		definition, recorded_at, recorded_tz
	) VALUES (?, ?, ?, ?, ?, ?, ?)`, pipelineID.String(), fixture.commit["global"],
		definition.Kind, definition.VersionKey, definition.Definition.String(), semanticTime, semanticTZ)
	_, params, err := domain.NewUnstructuredGeneratorParams(false, 128)
	if err != nil {
		t.Fatal(err)
	}
	dropped, err := dialogueDroppedInputSummary(0, 0, 0, "")
	if err != nil {
		t.Fatal(err)
	}
	runID := dialogueAssemblyReadParseID(t, fixture.ids.new())
	mustInsertCOV2NormalDialogueRun(t, fixture, runID,
		dialogueAssemblyReadParseID(t, fixture.ids.new()),
		dialogueAssemblyReadParseID(t, fixture.ids.new()), pipelineID,
		domain.PriorSplitDialogueNormalExecutionContract(), params, dropped)

	// The helper seeds a no-Recall normal envelope. Link the historical Recall
	// and append the exact renderer-v1 claim input that dialogue-v2 persisted.
	mustExec(t, fixture.db, `DROP TRIGGER trg_generation_runs_no_update`)
	mustExec(t, fixture.db, `UPDATE generation_runs SET recall_run_id = ?
		WHERE generation_run_id = ?`, fixture.recall["A"], runID.String())
	residentID := dialogueAssemblyReadParseID(t, fixture.resident["A"])
	claimID := dialogueAssemblyReadParseID(t, fixture.claim["A"])
	eligible, err := loadEligibleClaimStatement(ctx, fixture.db, residentID, claimID)
	if err != nil {
		t.Fatal(err)
	}
	rendered, err := memory.RenderClaim(memory.DefaultPolicyV2(), memory.RecallCandidate{
		ClaimID: claimID, Statement: string(eligible.Statement), TemporalRelation: eligible.TemporalRelation,
	})
	if err != nil {
		t.Fatal(err)
	}
	inputContentID := fixture.addContent(t, "A", "generation_input", rendered, "independent")
	mustExec(t, fixture.db, `INSERT INTO generation_run_inputs(
		generation_run_input_id, canonical_commit_id, generation_run_id, ordinal, role,
		source_type, source_id, inclusion_mode, content_id, recorded_at, recorded_tz
	) VALUES (?, ?, ?, 1, 'system', 'claim', ?, 'memory_recall', ?, ?, ?)`,
		fixture.ids.new(), fixture.commit["A"], runID.String(), fixture.claim["A"],
		inputContentID, semanticTime, semanticTZ)

	tx, err := fixture.db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := (&canonicalUoW{tx: tx}).revalidateDialogueInputsAtLanding(ctx, runID, residentID); err != nil {
		_ = tx.Rollback()
		t.Fatalf("dialogue-v2 Landing renderer-v1 revalidation: %v", err)
	}
	if err := tx.Rollback(); err != nil {
		t.Fatal(err)
	}

	// A later temporal Projection changes renderer-v1 bytes. Unlike dialogue-v1,
	// dialogue-v2 must not bypass that renderer comparison.
	mustExec(t, fixture.db, `INSERT INTO claim_states(
		claim_id, resident_id, stage, status, salience, confidence, currentness,
		temporal_relation, last_referenced_at, evidence_count
	) VALUES (?, ?, 'floating', 'active', 1, 1000000, 250000, 'future', NULL, 1)`,
		claimID.String(), residentID.String())
	tx, err = fixture.db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := (&canonicalUoW{tx: tx}).revalidateDialogueInputsAtLanding(ctx, runID, residentID); err == nil {
		_ = tx.Rollback()
		t.Fatal("dialogue-v2 Landing bypassed a rendering-v1 mismatch")
	}
	if err := tx.Rollback(); err != nil {
		t.Fatal(err)
	}
}

type dialoguePrepareWriterFixture struct {
	*dialogueAssemblyReadFixture
}

func newDialoguePrepareWriterFixture(t *testing.T) *dialoguePrepareWriterFixture {
	return newDialoguePrepareWriterFixtureWithPolicy(t, memory.DefaultPolicyV1())
}

func newDialoguePrepareWriterFixtureWithPolicy(
	t *testing.T,
	policy memory.Policy,
) *dialoguePrepareWriterFixture {
	return newDialoguePrepareWriterFixtureWithPolicyAndSource(t, policy, []byte("writer source"), false)
}

func newDialoguePrepareWriterFixtureWithPolicyAndSource(
	t *testing.T,
	policy memory.Policy,
	sourceBytes []byte,
	seedBackfill bool,
) *dialoguePrepareWriterFixture {
	t.Helper()
	base := newDialogueAssemblyReadFixture(t)
	fixture := &dialoguePrepareWriterFixture{dialogueAssemblyReadFixture: base}
	ctx := context.Background()

	activationTime := canonical.Instant(semanticTime + 6)
	uow := fixture.begin(t, 7, activationTime)
	policyJSON, err := policy.CanonicalJSON()
	if err != nil {
		t.Fatal(err)
	}
	type revisionSpec struct {
		class, contentClass string
		bytes               []byte
		policy              string
	}
	specs := []revisionSpec{
		{"principles", "principles_text", []byte("writer principles"), "resident_only"},
		{"persona", "persona_text", []byte("writer persona"), "resident_only"},
		{"memory_policy", "memory_policy_text", policyJSON.Bytes(), "resident_only"},
	}
	for index, spec := range specs {
		contentID := fixture.id(t)
		revisionID := fixture.id(t)
		salt := canonical.ContentSalt{}
		salt[0] = byte(index + 1)
		content := dialoguePrepareWriterContent(t, contentID, fixture.residentID,
			spec.contentClass, spec.bytes, spec.policy, salt)
		if err := uow.insertContent(ctx, content); err != nil {
			t.Fatal(err)
		}
		if _, err := uow.tx.ExecContext(ctx, `INSERT INTO resident_revisions(
			revision_id, canonical_commit_id, resident_id, revision_class, content_id,
			parent_revision_id, created_by_run_id, reason_content_id, recorded_at, recorded_tz
		) VALUES (?, ?, ?, ?, ?, NULL, NULL, NULL, ?, ?)`, revisionID.String(),
			uow.metadata.CommitID.String(), fixture.residentID.String(), spec.class, content.ID.String(),
			activationTime.UnixMicro(), semanticTZ); err != nil {
			t.Fatal(err)
		}
		if _, err := uow.tx.ExecContext(ctx, `INSERT INTO resident_revision_activations(
			activation_id, canonical_commit_id, resident_id, revision_id, actor_principal_id,
			approval_id, reason_code, reason_content_id, recorded_at, recorded_tz
		) VALUES (?, ?, ?, ?, ?, NULL, 'writer_fixture', NULL, ?, ?)`, fixture.id(t).String(),
			uow.metadata.CommitID.String(), fixture.residentID.String(), revisionID.String(),
			fixture.semantic.principal["human"], activationTime.UnixMicro(), semanticTZ); err != nil {
			t.Fatal(err)
		}
		fixture.semantic.revision["A"][spec.class] = revisionID.String()
	}
	if policy.MemoryRecallEnabled {
		mustExec(t, uow.tx, `INSERT INTO claim_view_scope_assertions(
			view_scope_assertion_id, canonical_commit_id, claim_id, view_scope, actor_principal_id,
			generation_run_id, memory_policy_revision_id, reason_code, reason_content_id,
			recorded_at, recorded_tz
		) VALUES (?, ?, ?, 'resident_ui', ?, NULL, ?, 'writer_fixture', NULL, ?, ?)`,
			fixture.id(t).String(), uow.metadata.CommitID.String(), fixture.semantic.claim["A"],
			fixture.semantic.principal["human"], fixture.semantic.revision["A"]["memory_policy"],
			activationTime.UnixMicro(), semanticTZ)
	}
	if seedBackfill {
		priorContentID := fixture.id(t)
		priorSalt := canonical.ContentSalt{8}
		priorContent := dialoguePrepareWriterContent(t, priorContentID, fixture.residentID,
			"event_payload", []byte("writer previous context"), "independent", priorSalt)
		if err := uow.insertContent(ctx, priorContent); err != nil {
			t.Fatal(err)
		}
		residentPrincipal, err := canonical.ParseID(fixture.semantic.principal["A"])
		if err != nil {
			t.Fatal(err)
		}
		ownerPrincipal, err := canonical.ParseID(fixture.semantic.principal["human"])
		if err != nil {
			t.Fatal(err)
		}
		targetPrincipal := residentPrincipal
		prior, err := uow.makeEvent(ctx, eventSpec{
			ID: fixture.id(t), ResidentID: fixture.residentID, Type: "user_message",
			Visibility: "conversation", DeliveryScreen: true, Ingress: "local_ui",
			ActorPrincipalID: ownerPrincipal, TargetPrincipalID: &targetPrincipal,
			OccurredAt: activationTime, OccurredTZ: canonical.MustTimezone(semanticTZ), Content: priorContent,
		})
		if err != nil {
			t.Fatal(err)
		}
		if err := uow.insertEvent(ctx, prior, "conversation", true, false, "local_ui"); err != nil {
			t.Fatal(err)
		}
	}
	if err := uow.Commit(ctx); err != nil {
		t.Fatal(err)
	}

	sourceTime := canonical.Instant(semanticTime + 2*60*60*1_000_000)
	uow = fixture.begin(t, 8, sourceTime)
	sourceID := fixture.id(t)
	sourceContentID := fixture.id(t)
	salt := canonical.ContentSalt{}
	salt[0] = 9
	sourceContent := dialoguePrepareWriterContent(t, sourceContentID, fixture.residentID,
		"event_payload", sourceBytes, "independent", salt)
	if err := uow.insertContent(ctx, sourceContent); err != nil {
		t.Fatal(err)
	}
	residentPrincipal, err := canonical.ParseID(fixture.semantic.principal["A"])
	if err != nil {
		t.Fatal(err)
	}
	ownerPrincipal, err := canonical.ParseID(fixture.semantic.principal["human"])
	if err != nil {
		t.Fatal(err)
	}
	targetPrincipal := residentPrincipal
	event, err := uow.makeEvent(ctx, eventSpec{
		ID: sourceID, ResidentID: fixture.residentID, Type: "user_message",
		Visibility: "conversation", DeliveryScreen: true, Ingress: "local_ui",
		ActorPrincipalID: ownerPrincipal, TargetPrincipalID: &targetPrincipal,
		OccurredAt: sourceTime, OccurredTZ: canonical.MustTimezone(semanticTZ), Content: sourceContent,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := uow.insertEvent(ctx, event, "conversation", true, false, "local_ui"); err != nil {
		t.Fatal(err)
	}
	if err := uow.Commit(ctx); err != nil {
		t.Fatal(err)
	}

	fixture.sourceEventID = sourceID
	fixture.target = domain.AssemblyTarget{
		Head: canonical.Head{Exists: true, CommitSeq: 8, CommittedAt: sourceTime},
		AsOf: sourceTime, AsOfTZ: canonical.MustTimezone(semanticTZ),
	}
	mustExec(t, fixture.semantic.db, `UPDATE projection_watermarks
		SET source_commit_seq = 8, as_of = ?, as_of_tz = ? WHERE resident_id = ?`,
		sourceTime.UnixMicro(), semanticTZ, fixture.residentID.String())
	mustExec(t, fixture.semantic.db, `UPDATE projection_watermark_dependencies
		SET dependency_version_id = ? WHERE resident_id = ? AND dependency_kind = 'memory_policy'`,
		fixture.semantic.revision["A"]["memory_policy"], fixture.residentID.String())
	if policy.MemoryRecallEnabled {
		mustExec(t, fixture.semantic.db, `INSERT INTO pipeline_versions(
			pipeline_version_id, canonical_commit_id, pipeline_kind, version_key,
			definition, recorded_at, recorded_tz
		) VALUES (?, ?, 'memory_recall', ?, '{"version":"memory-recall-v1"}', ?, ?)`,
			fixture.id(t).String(), fixture.semantic.commit["global"], domain.MemoryRecallPipelineVersion,
			semanticTime, semanticTZ)
		mustExec(t, fixture.semantic.db, `INSERT INTO claim_states(
			claim_id, resident_id, stage, status, salience, confidence, currentness,
			temporal_relation, last_referenced_at, evidence_count
		) VALUES (?, ?, 'floating', 'active', 1.0, 1000000, 1000000, 'current', NULL, 1)`,
			fixture.semantic.claim["A"], fixture.residentID.String())
		mustExec(t, fixture.semantic.db, `INSERT INTO claim_view_scope_current(
			claim_id, resident_id, view_scope, source_assertion_id
		) SELECT claim_id, ?, view_scope, view_scope_assertion_id
		  FROM claim_view_scope_assertions WHERE claim_id = ? ORDER BY recorded_at DESC LIMIT 1`,
			fixture.residentID.String(), fixture.semantic.claim["A"])
	}
	return fixture
}

func (fixture *dialoguePrepareWriterFixture) id(t *testing.T) canonical.ID {
	t.Helper()
	return dialogueAssemblyReadParseID(t, fixture.semantic.ids.new())
}

func (fixture *dialoguePrepareWriterFixture) begin(
	t *testing.T,
	sequence int64,
	committedAt canonical.Instant,
) *canonicalUoW {
	t.Helper()
	scope, err := canonical.ResidentScope(fixture.residentID)
	if err != nil {
		t.Fatal(err)
	}
	commitSeq, err := canonical.NewCommitSeq(sequence)
	if err != nil {
		t.Fatal(err)
	}
	metadata := canonical.CommitMetadata{
		CommitID: fixture.id(t), CommitSeq: commitSeq, Scope: scope,
		CommittedAt: committedAt, CommittedTZ: canonical.MustTimezone(semanticTZ),
	}
	tx, err := fixture.semantic.db.BeginTx(context.Background(), nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec(`INSERT INTO canonical_commits(
		canonical_commit_id, commit_seq, resident_id, committed_at, committed_tz
	) VALUES (?, ?, ?, ?, ?)`, metadata.CommitID.String(), metadata.CommitSeq.Int64(),
		fixture.residentID.String(), metadata.CommittedAt.UnixMicro(), metadata.CommittedTZ.String()); err != nil {
		_ = tx.Rollback()
		t.Fatal(err)
	}
	return &canonicalUoW{tx: tx, metadata: metadata, releaseWrite: func() {}}
}

func (fixture *dialoguePrepareWriterFixture) assemble(t *testing.T) domain.PrepareDialogue {
	t.Helper()
	result, err := fixture.repository.AssembleDialogue(context.Background(), fixture.request(t))
	if err != nil {
		t.Fatal(err)
	}
	return result.Prepare
}

func (fixture *dialoguePrepareWriterFixture) seedLegacyPreparedDialogue(t *testing.T) canonical.ID {
	return fixture.seedLegacyPreparedDialogueShape(t, false)
}

func (fixture *dialoguePrepareWriterFixture) seedLegacyPreparedDialogueWithRecall(t *testing.T) canonical.ID {
	return fixture.seedLegacyPreparedDialogueShape(t, true)
}

func (fixture *dialoguePrepareWriterFixture) seedLegacyPreparedDialogueShape(
	t *testing.T,
	withRecall bool,
) canonical.ID {
	t.Helper()
	ctx := context.Background()
	legacyPipelineID := fixture.id(t)
	definition, err := domain.DialoguePipelineDefinition(legacyPipelineID, domain.DialoguePipelineVersionV1)
	if err != nil {
		t.Fatal(err)
	}
	mustExec(t, fixture.semantic.db, `INSERT INTO pipeline_versions(
		pipeline_version_id, canonical_commit_id, pipeline_kind, version_key,
		definition, recorded_at, recorded_tz
	) VALUES (?, ?, ?, ?, ?, ?, ?)`, legacyPipelineID.String(), fixture.semantic.commit["global"],
		definition.Kind, definition.VersionKey, definition.Definition.String(), semanticTime, semanticTZ)
	var sessionRaw string
	if err := fixture.semantic.db.QueryRow(`SELECT desired_sessionization_policy_version_id
		FROM runtime_config WHERE singleton_id = 1`).Scan(&sessionRaw); err != nil {
		t.Fatal(err)
	}
	sessionID := dialogueAssemblyReadParseID(t, sessionRaw)
	var sourceCommitRaw string
	if err := fixture.semantic.db.QueryRow(`SELECT canonical_commit_id FROM canonical_commits
		WHERE commit_seq = 8`).Scan(&sourceCommitRaw); err != nil {
		t.Fatal(err)
	}
	scope, _ := canonical.ResidentScope(fixture.residentID)
	tx, err := fixture.semantic.db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	uow := &canonicalUoW{tx: tx, metadata: canonical.CommitMetadata{
		CommitID:  dialogueAssemblyReadParseID(t, sourceCommitRaw),
		CommitSeq: 8, Scope: scope, CommittedAt: fixture.target.AsOf, CommittedTZ: fixture.target.AsOfTZ,
	}}
	type source struct {
		role, sourceType, inclusion string
		id                          *canonical.ID
		bytes                       []byte
	}
	principlesID := dialogueAssemblyReadParseID(t, fixture.semantic.revision["A"]["principles"])
	personaID := dialogueAssemblyReadParseID(t, fixture.semantic.revision["A"]["persona"])
	memoryID := dialogueAssemblyReadParseID(t, fixture.semantic.revision["A"]["memory_policy"])
	sourceEventID := fixture.sourceEventID
	sources := []source{
		{"system", "resident_revision", "resident_definition", &principlesID, []byte("legacy principles")},
		{"system", "resident_revision", "resident_definition", &personaID, []byte("legacy persona")},
		{"system", "resident_revision", "resident_definition", &memoryID, []byte("legacy memory")},
		{"system", "runtime_projection", "runtime_projection", nil, []byte("legacy runtime")},
	}
	var recallID *canonical.ID
	if withRecall {
		claimID := dialogueAssemblyReadParseID(t, fixture.semantic.claim["A"])
		sources = append(sources,
			source{"system", "claim", "memory_recall", &claimID, []byte("legacy frozen rendered claim")},
		)
		id := fixture.id(t)
		recallID = &id
		var queryContentRaw, recallPipelineRaw string
		if err := tx.QueryRowContext(ctx, `SELECT content_id FROM events WHERE event_id = ?`,
			fixture.sourceEventID.String()).Scan(&queryContentRaw); err != nil {
			_ = tx.Rollback()
			t.Fatal(err)
		}
		if err := tx.QueryRowContext(ctx, `SELECT pipeline_version_id FROM pipeline_versions
			WHERE pipeline_kind = 'memory_recall' AND version_key = ?`,
			domain.MemoryRecallPipelineVersion).Scan(&recallPipelineRaw); err != nil {
			_ = tx.Rollback()
			t.Fatal(err)
		}
		query, err := canonical.ParseCanonicalJSON([]byte(`{"projection_head":"7"}`))
		if err != nil {
			_ = tx.Rollback()
			t.Fatal(err)
		}
		constraints, err := canonical.ParseCanonicalJSON([]byte(`{}`))
		if err != nil {
			_ = tx.Rollback()
			t.Fatal(err)
		}
		if _, err := tx.ExecContext(ctx, `INSERT INTO recall_runs(
			recall_run_id, canonical_commit_id, resident_id, query_content_id, query_conditions,
			pipeline_version_id, memory_policy_revision_id, as_of, as_of_tz,
			context_constraints, recorded_at, recorded_tz
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
			recallID.String(), uow.metadata.CommitID.String(), fixture.residentID.String(), queryContentRaw,
			query.String(), recallPipelineRaw, memoryID.String(), fixture.target.AsOf.UnixMicro(),
			fixture.target.AsOfTZ.String(), constraints.String(), fixture.target.AsOf.UnixMicro(),
			fixture.target.AsOfTZ.String()); err != nil {
			_ = tx.Rollback()
			t.Fatal(err)
		}
	}
	sources = append(sources, source{"user", "event", "current_input", &sourceEventID, []byte("writer source")})
	runID := fixture.id(t)
	outcomeID := fixture.id(t)
	_, params, err := domain.NewUnstructuredGeneratorParams(false, canonical.ByteSize(1024))
	if err != nil {
		t.Fatal(err)
	}
	dropped, err := dialogueDroppedInputSummary(0, 0, 0, "")
	if err != nil {
		t.Fatal(err)
	}
	var recallRaw any
	if recallID != nil {
		recallRaw = recallID.String()
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO generation_runs(
		generation_run_id, canonical_commit_id, resident_id, purpose, idempotency_key,
		provider, model, model_version, prompt_template_version, pipeline_version_id,
		context_policy_version, sessionization_policy_version_id, memory_rendering_version,
		principles_revision_id, persona_revision_id, memory_policy_revision_id, recall_run_id,
		temperature, top_p, max_tokens, seed, generator_params, as_of, as_of_tz,
		budget_exceeded, dropped_input_summary, requested_at, requested_tz
	) VALUES (?, ?, ?, 'dialogue', ?, 'legacy-test', 'legacy-model', NULL, ?, ?, ?, ?, ?,
		?, ?, ?, ?, NULL, NULL, NULL, NULL, ?, ?, ?, 0, ?, ?, ?)`,
		runID.String(), uow.metadata.CommitID.String(), fixture.residentID.String(),
		domain.DialogueObligation(fixture.sourceEventID), domain.DialoguePromptTemplateVersionV1,
		legacyPipelineID.String(), domain.DialogueContextPolicyVersionV1, sessionID.String(),
		domain.MemoryRenderingVersionNoneV1, principlesID.String(), personaID.String(), memoryID.String(),
		recallRaw, params.String(), fixture.target.AsOf.UnixMicro(), fixture.target.AsOfTZ.String(), dropped.String(),
		fixture.target.AsOf.UnixMicro(), fixture.target.AsOfTZ.String()); err != nil {
		_ = tx.Rollback()
		t.Fatal(err)
	}
	for index, item := range sources {
		contentID := fixture.id(t)
		salt := canonical.ContentSalt{}
		salt[0] = byte(40 + index)
		content := dialoguePrepareWriterContent(t, contentID, fixture.residentID,
			"generation_input", item.bytes, "independent", salt)
		if err := uow.insertContent(ctx, content); err != nil {
			_ = tx.Rollback()
			t.Fatal(err)
		}
		var sourceID any
		if item.id != nil {
			sourceID = item.id.String()
		}
		if _, err := tx.ExecContext(ctx, `INSERT INTO generation_run_inputs(
			generation_run_input_id, canonical_commit_id, generation_run_id, ordinal, role,
			source_type, source_id, inclusion_mode, content_id, recorded_at, recorded_tz
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`, fixture.id(t).String(),
			uow.metadata.CommitID.String(), runID.String(), index, item.role, item.sourceType, sourceID,
			item.inclusion, content.ID.String(), fixture.target.AsOf.UnixMicro(), fixture.target.AsOfTZ.String()); err != nil {
			_ = tx.Rollback()
			t.Fatal(err)
		}
	}
	if recallID != nil {
		claimID := dialogueAssemblyReadParseID(t, fixture.semantic.claim["A"])
		for _, usageType := range []memory.UsageType{
			memory.UsageCandidate,
			memory.UsageSelected,
			memory.UsagePromptIncluded,
		} {
			if _, err := tx.ExecContext(ctx, `INSERT INTO claim_usages(
				claim_usage_id, canonical_commit_id, claim_id, recall_run_id, generation_run_id,
				usage_type, ordinal, memory_policy_revision_id, exclusion_reason,
				detection_method, detection_confidence, detected_by_run_id, recorded_at, recorded_tz
			) VALUES (?, ?, ?, ?, ?, ?, 0, ?, NULL, NULL, NULL, NULL, ?, ?)`,
				fixture.id(t).String(), uow.metadata.CommitID.String(), claimID.String(), recallID.String(),
				runID.String(), string(usageType), memoryID.String(), fixture.target.AsOf.UnixMicro(),
				fixture.target.AsOfTZ.String()); err != nil {
				_ = tx.Rollback()
				t.Fatal(err)
			}
		}
	}
	if err := uow.insertOutcome(ctx, outcomeID, runID, 1, "running", nil, nil, nil, 0, ""); err != nil {
		_ = tx.Rollback()
		t.Fatal(err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	return runID
}

func (fixture *dialoguePrepareWriterFixture) requireNoPreparedRows(t *testing.T, runID canonical.ID) {
	fixture.requireNoPreparedRowsAtCommit(t, runID, 9)
}

func (fixture *dialoguePrepareWriterFixture) requireNoPreparedRowsAtCommit(
	t *testing.T,
	runID canonical.ID,
	commitSeq int64,
) {
	t.Helper()
	var commits, runs, inputs, outcomes int64
	if err := fixture.semantic.db.QueryRow(`SELECT
		(SELECT COUNT(*) FROM canonical_commits WHERE commit_seq = ?),
		(SELECT COUNT(*) FROM generation_runs WHERE generation_run_id = ?),
		(SELECT COUNT(*) FROM generation_run_inputs WHERE generation_run_id = ?),
		(SELECT COUNT(*) FROM generation_run_outcomes WHERE generation_run_id = ?)`,
		commitSeq, runID.String(), runID.String(), runID.String()).Scan(&commits, &runs, &inputs, &outcomes); err != nil {
		t.Fatal(err)
	}
	if commits != 0 || runs != 0 || inputs != 0 || outcomes != 0 {
		t.Fatalf("rollback residue commits=%d runs=%d inputs=%d outcomes=%d", commits, runs, inputs, outcomes)
	}
}

func dialoguePrepareWriterContent(
	t *testing.T,
	id, residentID canonical.ID,
	class string,
	bytes []byte,
	erasurePolicy string,
	salt canonical.ContentSalt,
) domain.Content {
	t.Helper()
	commitment, err := canonical.CommitContent(class, salt, bytes)
	if err != nil {
		t.Fatal(err)
	}
	return domain.Content{
		ID: id, ResidentID: residentID, Class: class, Bytes: append([]byte(nil), bytes...),
		BlobHash: canonical.HashBlob(bytes), Commitment: commitment, CommitmentSalt: salt,
		ErasurePolicy: erasurePolicy,
	}
}
