package app

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"fmt"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"mahoroba.local/mahoroba/internal/blob"
	"mahoroba.local/mahoroba/internal/canonical"
	"mahoroba.local/mahoroba/internal/domain"
	"mahoroba.local/mahoroba/internal/generation"
	"mahoroba.local/mahoroba/internal/memory"
	"mahoroba.local/mahoroba/internal/store/sqlite"
	"mahoroba.local/mahoroba/internal/testsupport"
)

func TestApplicationRetriesProviderFailureAndCommitsOneResidentReply(t *testing.T) {
	providerFailure := &generation.ProviderError{
		Class: generation.ErrorHTTP, Retryable: true, StatusCode: 429, Detail: "test rate limit",
	}
	generator := &scriptedGenerator{steps: []generatorStep{
		{err: providerFailure},
		{text: "resident reply", deltas: []string{"resident ", "reply"}},
	}}
	fixture := newApplicationFixture(t, generator, 2)

	userEvent, err := fixture.application.Ingress(context.Background(), "hello")
	if err != nil {
		t.Fatal(err)
	}
	if err := fixture.application.ProcessResident(context.Background(), fixture.residentID); err != nil {
		t.Fatal(err)
	}

	if got := generator.CallCount(); got != 2 {
		t.Fatalf("provider calls = %d, want 2", got)
	}
	history, err := fixture.repository.History(context.Background(), fixture.residentID, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(history) != 2 {
		t.Fatalf("history length = %d, want 2", len(history))
	}
	if history[0].ID != userEvent.ID || history[0].Type != "user_message" || history[0].Content != "hello" {
		t.Fatalf("user event = %+v", history[0])
	}
	if history[1].Type != "resident_message" || history[1].Content != "resident reply" {
		t.Fatalf("resident event = %+v", history[1])
	}
	if history[1].GenerationRunID == nil {
		t.Fatal("resident event has no generation run")
	}
	verification, err := (canonical.LedgerVerifier{EnvelopeValidator: fixture.repository}).Verify(
		context.Background(), fixture.repository, fixture.residentID,
	)
	if err != nil {
		t.Fatalf("verify dialogue ledger: %v", err)
	}
	if verification.EventCount != 2 || verification.LastSeq != history[1].Seq {
		t.Fatalf("ledger verification = %+v, want two events through seq %s", verification, history[1].Seq)
	}
	work, err := discoverDialogueWorkForTest(context.Background(), fixture.repository, fixture.residentID, 10, 2)
	if err != nil {
		t.Fatal(err)
	}
	if len(work) != 1 || work[0].State != domain.WorkSucceeded || work[0].AttemptNo != 2 {
		t.Fatalf("work = %+v, want succeeded attempt 2", work)
	}
	orphans, err := fixture.blobs.ListOrphans(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(orphans) != 0 {
		t.Fatalf("staged orphans = %v, want none", orphans)
	}
}

func TestBootstrapCreatesMultipleActiveResidentsWithOneOperationalSelection(t *testing.T) {
	fixture := newApplicationFixture(t, &scriptedGenerator{}, 2)
	ctx := context.Background()

	secondInput := BootstrapInput{
		OwnerName: "Owner", Name: "Second Resident", SeedKey: "blank-v1", Principles: "be careful",
	}
	state, err := fixture.application.BootstrapInit(ctx, secondInput)
	if err != nil {
		t.Fatal(err)
	}
	if len(state.Residents) != 2 {
		t.Fatalf("residents after second bootstrap = %d, want 2", len(state.Residents))
	}
	state, err = fixture.application.BootstrapInit(ctx, secondInput)
	if err != nil {
		t.Fatal(err)
	}
	if len(state.Residents) != 2 {
		t.Fatalf("idempotent second bootstrap residents = %d, want 2", len(state.Residents))
	}
	var second domain.ResidentSnapshot
	for _, resident := range state.Residents {
		if resident.Name == secondInput.Name {
			second = resident
		}
	}
	if second.ResidentID.IsZero() || second.Status != "draft" || second.SeedKey != secondInput.SeedKey {
		t.Fatalf("second resident = %+v, want matching draft", second)
	}
	invalidPolicies := []string{
		`{"mandatory_event_types":["user_message"],"memory_recall_enabled":false,"version":"memory-policy-v1"}`,
		`{"mandatory_event_types":[],"memory_recall_enabled":true,"version":"memory-policy-v1"}`,
		`{"mandatory_event_types":[],"memory_recall_enabled":false,"version":"future"}`,
		`{"memory_recall_enabled":false,"version":"memory-policy-v1"}`,
	}
	for _, policy := range invalidPolicies {
		if err := fixture.application.FinalizeBootstrap(ctx, second.ResidentID, "persona", policy); err == nil {
			t.Fatalf("unsafe initial memory policy was accepted: %s", policy)
		}
	}
	if err := fixture.application.ApprovePrinciples(ctx, second.ResidentID); err != nil {
		t.Fatal(err)
	}
	if err := fixture.application.FinalizeBootstrap(ctx, second.ResidentID, "persona",
		`{"mandatory_event_types":[],"memory_recall_enabled":false,"version":"memory-policy-v1"}`); err != nil {
		t.Fatal(err)
	}
	residents, err := fixture.application.ListResidents(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(residents) != 2 || residents[0].Status != "active" || residents[1].Status != "active" {
		t.Fatalf("canonical resident lifecycle = %+v, want two active residents", residents)
	}
	if err := fixture.application.SelectResident(ctx, second.ResidentID); err != nil {
		t.Fatal(err)
	}
	selected, err := fixture.application.ActiveResident(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if selected.ResidentID != second.ResidentID {
		t.Fatalf("operational selection = %s, want %s", selected.ResidentID, second.ResidentID)
	}
}

func TestArchiveAndConcurrentSelectionCannotLeaveArchivedResidentSelected(t *testing.T) {
	fixture := newApplicationFixture(t, &scriptedGenerator{}, 2)
	ctx := context.Background()
	start := make(chan struct{})
	var group sync.WaitGroup
	group.Add(2)
	var archiveErr, selectErr error
	go func() {
		defer group.Done()
		<-start
		archiveErr = fixture.application.ArchiveResident(ctx, fixture.residentID)
	}()
	go func() {
		defer group.Done()
		<-start
		selectErr = fixture.application.SelectResident(ctx, fixture.residentID)
	}()
	close(start)
	group.Wait()
	if archiveErr != nil {
		t.Fatalf("archive selected resident: %v", archiveErr)
	}
	// Selection may have committed before archive (success) or observed the
	// archived state (error). Neither serialization order may leave the
	// archived resident in Operational configuration.
	_ = selectErr
	var active, policy sql.NullString
	if err := fixture.store.Reader().QueryRowContext(ctx, `SELECT active_resident_id,
		desired_sessionization_policy_version_id FROM runtime_config WHERE singleton_id = 1`).Scan(&active, &policy); err != nil {
		t.Fatal(err)
	}
	if active.Valid || policy.Valid {
		t.Fatalf("archived resident remained selected: active=%q policy=%q", active.String, policy.String)
	}
	resident, err := fixture.application.Resident(ctx, fixture.residentID)
	if err != nil {
		t.Fatal(err)
	}
	if resident.Status != "archived" {
		t.Fatalf("resident status = %q, want archived", resident.Status)
	}
	if _, err := fixture.application.ActiveResident(ctx); err == nil {
		t.Fatal("ActiveResident succeeded after selected resident was archived")
	}
	if err := fixture.application.SelectResident(ctx, fixture.residentID); err == nil {
		t.Fatal("archived resident was selected")
	}
}

func TestBootstrapStageRetriesAreCrashSafeAndRejectConflictsInsideUoW(t *testing.T) {
	fixture := newApplicationFixture(t, &scriptedGenerator{}, 2)
	ctx := context.Background()
	const memoryPolicy = `{"mandatory_event_types":[],"memory_recall_enabled":false,"version":"memory-policy-v1"}`

	state, err := fixture.application.BootstrapInit(ctx, BootstrapInput{
		OwnerName: "Owner", Name: "Retry Resident", SeedKey: "retry-v1", Principles: "stay deterministic",
	})
	if err != nil {
		t.Fatal(err)
	}
	resident := state.Residents[len(state.Residents)-1]
	if resident.Status != "draft" {
		t.Fatalf("retry resident status = %q, want draft", resident.Status)
	}
	if got := readBootstrapRowCounts(t, fixture.store.Reader(), resident.ResidentID); got != (bootstrapRowCounts{
		commits: 1, approvals: 0, activations: 0, revisions: 1, transitions: 1, contents: 1, blobs: 1,
	}) {
		t.Fatalf("draft row counts = %+v", got)
	}
	draftCounts := readBootstrapRowCounts(t, fixture.store.Reader(), resident.ResidentID)
	idempotent, err := fixture.application.BootstrapInit(ctx, BootstrapInput{
		OwnerName: "Owner", Name: "Retry Resident", SeedKey: "retry-v1", Principles: "stay deterministic",
	})
	if err != nil {
		t.Fatalf("identical BootstrapInit retry: %v", err)
	}
	if len(idempotent.Residents) != len(state.Residents) {
		t.Fatalf("identical BootstrapInit retry residents = %d, want %d", len(idempotent.Residents), len(state.Residents))
	}
	assertBootstrapRowCounts(t, fixture.store.Reader(), resident.ResidentID, draftCounts)
	assertNoBootstrapOrphans(t, fixture.blobs)

	if _, err := fixture.application.BootstrapInit(ctx, BootstrapInput{
		OwnerName: "Different Owner", Name: "Retry Resident", SeedKey: "retry-v1", Principles: "stay deterministic",
	}); err == nil || !strings.Contains(err.Error(), "initialized owner") {
		t.Fatalf("changed-owner BootstrapInit retry error = %v, want fail-closed owner conflict", err)
	}
	if _, err := fixture.application.BootstrapInit(ctx, BootstrapInput{
		OwnerName: "Owner", Name: "Retry Resident", SeedKey: "retry-v1", Principles: "changed principles",
	}); err == nil || !strings.Contains(err.Error(), "initialized principles") {
		t.Fatalf("changed-principles BootstrapInit retry error = %v, want fail-closed principles conflict", err)
	}
	assertBootstrapRowCounts(t, fixture.store.Reader(), resident.ResidentID, draftCounts)
	assertNoBootstrapOrphans(t, fixture.blobs)

	if err := fixture.application.ApprovePrinciples(ctx, resident.ResidentID); err != nil {
		t.Fatal(err)
	}
	afterApproval := readBootstrapRowCounts(t, fixture.store.Reader(), resident.ResidentID)
	if afterApproval != (bootstrapRowCounts{
		commits: 2, approvals: 1, activations: 1, revisions: 1, transitions: 1, contents: 1, blobs: 1,
	}) {
		t.Fatalf("approved row counts = %+v", afterApproval)
	}
	// Simulate a caller losing the success response after the transaction was
	// durable. The retry reaches the UoW while the resident is still draft.
	if err := fixture.application.ApprovePrinciples(ctx, resident.ResidentID); err != nil {
		t.Fatalf("approval retry: %v", err)
	}
	assertBootstrapRowCounts(t, fixture.store.Reader(), resident.ResidentID, afterApproval)

	if err := fixture.application.FinalizeBootstrap(ctx, resident.ResidentID, "retry persona", memoryPolicy); err != nil {
		t.Fatal(err)
	}
	afterFinalize := readBootstrapRowCounts(t, fixture.store.Reader(), resident.ResidentID)
	if afterFinalize != (bootstrapRowCounts{
		commits: 3, approvals: 1, activations: 3, revisions: 3, transitions: 2, contents: 3, blobs: 3,
	}) {
		t.Fatalf("finalized row counts = %+v", afterFinalize)
	}

	// Restart the Canonical Writer before retrying, which covers a process crash
	// after commit but before the caller persisted/observed the response.
	if err := fixture.writer.Close(ctx); err != nil {
		t.Fatal(err)
	}
	restartIDs := canonical.NewSecureIDGenerator()
	restartedWriter, err := canonical.OpenWriter(ctx, canonical.WriterOptions{
		Backend: fixture.store.Canonical(), IDs: restartIDs, Clock: fixture.clock,
		Timezone: canonical.MustTimezone("UTC"), QueueCapacity: 32,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := restartedWriter.Close(context.Background()); err != nil {
			t.Errorf("close restarted bootstrap writer: %v", err)
		}
	})
	restarted, err := New(Options{
		Writer: restartedWriter, Repository: fixture.store.Canonical(), IDs: restartIDs, Clock: fixture.clock,
		Timezone: canonical.MustTimezone("UTC"), Blobs: fixture.blobs, Generator: &scriptedGenerator{},
		Provider: "test", Model: "test-model", MaxAttempts: 2, RetryBackoff: []time.Duration{0},
		MaxInputBytes: 64 << 10, MaxOutputBytes: 64 << 10, SafetyScanInterval: time.Hour,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := restarted.ApprovePrinciples(ctx, resident.ResidentID); err != nil {
		t.Fatalf("post-restart approval retry: %v", err)
	}
	if err := restarted.FinalizeBootstrap(ctx, resident.ResidentID, "retry persona", memoryPolicy); err != nil {
		t.Fatalf("post-restart finalize retry: %v", err)
	}
	assertBootstrapRowCounts(t, fixture.store.Reader(), resident.ResidentID, afterFinalize)
	assertNoBootstrapOrphans(t, fixture.blobs)

	if err := restarted.FinalizeBootstrap(ctx, resident.ResidentID, "different persona", memoryPolicy); err == nil {
		t.Fatal("changed persona finalization retry succeeded")
	}
	assertBootstrapRowCounts(t, fixture.store.Reader(), resident.ResidentID, afterFinalize)
	assertNoBootstrapOrphans(t, fixture.blobs)
	// JSON member order is not a logical policy change.
	const equivalentPolicy = `{"version":"memory-policy-v1","memory_recall_enabled":false,"mandatory_event_types":[]}`
	equivalentDigest := canonical.HashBlob([]byte(equivalentPolicy))
	if exists, err := fixture.blobs.Exists(resident.ResidentID, equivalentDigest); err != nil || exists {
		t.Fatalf("equivalent raw policy object existed before retry: exists=%v err=%v", exists, err)
	}
	if err := restarted.FinalizeBootstrap(ctx, resident.ResidentID, "retry persona", equivalentPolicy); err != nil {
		t.Fatalf("logically identical memory policy retry: %v", err)
	}
	assertBootstrapRowCounts(t, fixture.store.Reader(), resident.ResidentID, afterFinalize)
	if exists, err := fixture.blobs.Exists(resident.ResidentID, equivalentDigest); err != nil || exists {
		t.Fatalf("equivalent raw policy retry published an object: exists=%v err=%v", exists, err)
	}
	assertNoBootstrapOrphans(t, fixture.blobs)

	resident, err = fixture.store.Canonical().Resident(ctx, resident.ResidentID)
	if err != nil {
		t.Fatal(err)
	}
	personaContent, err := restarted.newContent(resident.ResidentID, "persona_text", []byte("retry persona"), "resident_only")
	if err != nil {
		t.Fatal(err)
	}
	changedMemoryContent, err := restarted.newContent(resident.ResidentID, "memory_policy_text",
		[]byte(`{"mandatory_event_types":["user_message"],"memory_recall_enabled":false,"version":"memory-policy-v1"}`), "resident_only")
	if err != nil {
		t.Fatal(err)
	}
	finalizeIDs, err := allocateTestIDs(restartIDs, 5)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := restartedWriter.Submit(ctx, domain.FinalizeResidentCommand(domain.FinalizeResident{
		ResidentID: resident.ResidentID, OwnerPrincipalID: resident.OwnerPrincipalID,
		PersonaRevisionID: finalizeIDs[0], PersonaActivationID: finalizeIDs[1], PersonaContent: personaContent,
		MemoryRevisionID: finalizeIDs[2], MemoryActivationID: finalizeIDs[3], MemoryContent: changedMemoryContent,
		StatusTransitionID: finalizeIDs[4],
	})); err == nil {
		t.Fatal("changed memory policy finalization retry succeeded inside UoW")
	}
	assertBootstrapRowCounts(t, fixture.store.Reader(), resident.ResidentID, afterFinalize)
	exactMemoryContent, err := restarted.newContent(resident.ResidentID, "memory_policy_text", []byte(memoryPolicy), "resident_only")
	if err != nil {
		t.Fatal(err)
	}

	for _, actor := range []struct {
		name string
		id   canonical.ID
	}{
		{name: "system", id: state.SystemPrincipalID},
		{name: "resident", id: resident.ResidentPrincipalID},
	} {
		approvalIDs, err := allocateTestIDs(restartIDs, 2)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := restartedWriter.Submit(ctx, domain.ApprovePrinciplesCommand(domain.ApprovePrinciples{
			ResidentID: resident.ResidentID, RevisionID: resident.PrinciplesRevisionID,
			OwnerPrincipalID: actor.id, ApprovalID: approvalIDs[0], ActivationID: approvalIDs[1],
		})); err == nil {
			t.Fatalf("%s principal approved principles", actor.name)
		}
		assertBootstrapRowCounts(t, fixture.store.Reader(), resident.ResidentID, afterFinalize)
		finalizeIDs, err := allocateTestIDs(restartIDs, 5)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := restartedWriter.Submit(ctx, domain.FinalizeResidentCommand(domain.FinalizeResident{
			ResidentID: resident.ResidentID, OwnerPrincipalID: actor.id,
			PersonaRevisionID: finalizeIDs[0], PersonaActivationID: finalizeIDs[1], PersonaContent: personaContent,
			MemoryRevisionID: finalizeIDs[2], MemoryActivationID: finalizeIDs[3], MemoryContent: exactMemoryContent,
			StatusTransitionID: finalizeIDs[4],
		})); err == nil {
			t.Fatalf("%s principal finalized persona and memory policy", actor.name)
		}
		assertBootstrapRowCounts(t, fixture.store.Reader(), resident.ResidentID, afterFinalize)
	}

	if err := restarted.ArchiveResident(ctx, resident.ResidentID); err != nil {
		t.Fatal(err)
	}
	afterArchive := readBootstrapRowCounts(t, fixture.store.Reader(), resident.ResidentID)
	if afterArchive.commits != afterFinalize.commits+1 || afterArchive.transitions != afterFinalize.transitions+1 {
		t.Fatalf("archive row counts = %+v, before = %+v", afterArchive, afterFinalize)
	}
	approvalIDs, err := allocateTestIDs(restartIDs, 2)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := restartedWriter.Submit(ctx, domain.ApprovePrinciplesCommand(domain.ApprovePrinciples{
		ResidentID: resident.ResidentID, RevisionID: resident.PrinciplesRevisionID,
		OwnerPrincipalID: resident.OwnerPrincipalID, ApprovalID: approvalIDs[0], ActivationID: approvalIDs[1],
	})); err == nil {
		t.Fatal("archived resident accepted an approval retry")
	}
	assertBootstrapRowCounts(t, fixture.store.Reader(), resident.ResidentID, afterArchive)
	if err := restarted.FinalizeBootstrap(ctx, resident.ResidentID, "retry persona", memoryPolicy); err == nil {
		t.Fatal("archived resident accepted a finalization retry")
	}
	assertBootstrapRowCounts(t, fixture.store.Reader(), resident.ResidentID, afterArchive)
}

func TestDraftResidentCannotPrepareGeneration(t *testing.T) {
	fixture := newApplicationFixture(t, &scriptedGenerator{}, 2)
	ctx := context.Background()
	state, err := fixture.application.BootstrapInit(ctx, BootstrapInput{
		OwnerName: "Owner", Name: "Draft Generation Negative", SeedKey: "draft-generation-negative-v1",
		Principles: "wait for approval",
	})
	if err != nil {
		t.Fatal(err)
	}
	var draft domain.ResidentSnapshot
	for _, candidate := range state.Residents {
		if candidate.Name == "Draft Generation Negative" {
			draft = candidate
			break
		}
	}
	if draft.ResidentID.IsZero() || draft.Status != "draft" {
		t.Fatalf("draft resident = %+v", draft)
	}
	active, err := fixture.repository.Resident(ctx, fixture.residentID)
	if err != nil {
		t.Fatal(err)
	}
	ids, err := fixture.application.allocateIDs(3)
	if err != nil {
		t.Fatal(err)
	}
	dropped, err := canonical.MarshalCanonical(struct {
		Backfill canonical.Count `json:"backfill"`
		Live     canonical.Count `json:"live"`
	}{})
	if err != nil {
		t.Fatal(err)
	}
	_, params, err := domain.NewUnstructuredGeneratorParams(true, canonical.ByteSize(64<<10))
	if err != nil {
		t.Fatal(err)
	}
	input, err := fixture.application.newContent(draft.ResidentID, "generation_input",
		[]byte("runtime_state=draft"), "independent")
	if err != nil {
		t.Fatal(err)
	}
	before := readBootstrapRowCounts(t, fixture.store.Reader(), draft.ResidentID)
	var generationRunsBefore int
	if err := fixture.store.Reader().QueryRow(`SELECT count(*) FROM generation_runs WHERE resident_id = ?`, draft.ResidentID.String()).Scan(&generationRunsBefore); err != nil {
		t.Fatal(err)
	}
	prepare := domain.PrepareGeneration{
		RunID: ids[0], ResidentID: draft.ResidentID, IdempotencyKey: "draft-generation-negative-v1",
		Provider: "test", Model: "test-model", PipelineVersionID: draft.PipelineVersionID,
		SessionPolicyID: &draft.SessionPolicyID, PrinciplesRevisionID: draft.PrinciplesRevisionID,
		PersonaRevisionID: active.PersonaRevisionID, MemoryPolicyRevisionID: active.MemoryPolicyRevisionID,
		AsOf: canonical.InstantFromTime(fixture.clock.Now()), AsOfTZ: canonical.MustTimezone("UTC"),
		DroppedInputSummary: dropped, GeneratorParams: params, RunningOutcomeID: ids[1],
		Inputs: []domain.GenerationInput{{
			ID: ids[2], Ordinal: 0, Role: string(generation.RoleSystem), SourceType: "runtime_projection",
			InclusionMode: "runtime_projection", Content: input,
		}},
	}
	versions := domain.CurrentDialogueNormalExecutionContract()
	prepare.PromptTemplateVersion = versions.PromptTemplateVersion
	prepare.ContextPolicyVersion = versions.ContextPolicyVersion
	prepare.MemoryRenderingVersion = versions.MemoryRenderingVersion
	_, err = fixture.writer.Submit(ctx, domain.PrepareGenerationCommand(prepare))
	if err == nil || !strings.Contains(err.Error(), "requires PrepareDialogue") {
		t.Fatalf("draft PrepareGeneration error = %v, want generic dialogue-prepare rejection", err)
	}
	assertBootstrapRowCounts(t, fixture.store.Reader(), draft.ResidentID, before)
	var generationRunsAfter int
	if err := fixture.store.Reader().QueryRow(`SELECT count(*) FROM generation_runs WHERE resident_id = ?`, draft.ResidentID.String()).Scan(&generationRunsAfter); err != nil {
		t.Fatal(err)
	}
	if generationRunsAfter != generationRunsBefore {
		t.Fatalf("draft generation runs = %d, want unchanged %d", generationRunsAfter, generationRunsBefore)
	}
}

type bootstrapRowCounts struct {
	commits, approvals, activations, revisions, transitions, contents, blobs int
}

func readBootstrapRowCounts(t *testing.T, db *sql.DB, residentID canonical.ID) bootstrapRowCounts {
	t.Helper()
	var counts bootstrapRowCounts
	queries := []struct {
		destination *int
		query       string
	}{
		{&counts.commits, `SELECT count(*) FROM canonical_commits WHERE resident_id = ?`},
		{&counts.approvals, `SELECT count(*) FROM resident_revision_approvals p JOIN resident_revisions r ON r.revision_id = p.revision_id WHERE r.resident_id = ?`},
		{&counts.activations, `SELECT count(*) FROM resident_revision_activations WHERE resident_id = ?`},
		{&counts.revisions, `SELECT count(*) FROM resident_revisions WHERE resident_id = ?`},
		{&counts.transitions, `SELECT count(*) FROM resident_status_transitions WHERE resident_id = ?`},
		{&counts.contents, `SELECT count(*) FROM content_objects WHERE owner_resident_id = ?`},
		{&counts.blobs, `SELECT count(*) FROM blobs WHERE dedupe_scope_id = ?`},
	}
	for _, item := range queries {
		if err := db.QueryRow(item.query, residentID.String()).Scan(item.destination); err != nil {
			t.Fatal(err)
		}
	}
	return counts
}

func assertBootstrapRowCounts(t *testing.T, db *sql.DB, residentID canonical.ID, want bootstrapRowCounts) {
	t.Helper()
	if got := readBootstrapRowCounts(t, db, residentID); got != want {
		t.Fatalf("bootstrap row counts = %+v, want %+v", got, want)
	}
}

func assertNoBootstrapOrphans(t *testing.T, store blob.Store) {
	t.Helper()
	orphans, err := store.ListOrphans(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(orphans) != 0 {
		t.Fatalf("bootstrap retry left recovery markers: %+v", orphans)
	}
}

func allocateTestIDs(generator *canonical.IDGenerator, count int) ([]canonical.ID, error) {
	result := make([]canonical.ID, count)
	for index := range result {
		id, err := generator.New()
		if err != nil {
			return nil, err
		}
		result[index] = id
	}
	return result, nil
}

func TestApplicationDoesNotRetryTerminalProviderFailure(t *testing.T) {
	tests := []struct {
		name string
		err  error
	}{
		{
			name: "HTTP 401",
			err:  &generation.ProviderError{Class: generation.ErrorHTTP, StatusCode: 401, Retryable: true},
		},
		{
			name: "invalid response",
			err:  &generation.ProviderError{Class: generation.ErrorInvalidResponse},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			generator := &scriptedGenerator{steps: []generatorStep{{err: test.err}}}
			fixture := newApplicationFixture(t, generator, 3)
			ctx := context.Background()
			if _, err := fixture.application.Ingress(ctx, "terminal provider failure"); err != nil {
				t.Fatal(err)
			}
			if err := fixture.application.ProcessResident(ctx, fixture.residentID); err != nil {
				t.Fatal(err)
			}
			if got := generator.CallCount(); got != 1 {
				t.Fatalf("provider calls = %d, want 1", got)
			}
			work, err := discoverDialogueWorkForTest(ctx, fixture.repository, fixture.residentID, 10, 3)
			if err != nil {
				t.Fatal(err)
			}
			if len(work) != 1 || work[0].State != domain.WorkTerminalFailed || work[0].AttemptNo != 1 {
				t.Fatalf("work = %+v, want terminal_failed attempt 1", work)
			}
		})
	}
}

func TestTerminalProviderFailureRemainsTerminalAfterRestart(t *testing.T) {
	ctx := context.Background()
	firstGenerator := &scriptedGenerator{steps: []generatorStep{{
		err: &generation.ProviderError{Class: generation.ErrorHTTP, StatusCode: 401},
	}}}
	fixture := newApplicationFixture(t, firstGenerator, 3)
	if _, err := fixture.application.Ingress(ctx, "persist terminal failure"); err != nil {
		t.Fatal(err)
	}
	if err := fixture.application.ProcessResident(ctx, fixture.residentID); err != nil {
		t.Fatal(err)
	}
	var persistedCode string
	if err := fixture.store.Reader().QueryRowContext(ctx, `SELECT error_class FROM generation_run_outcomes
		WHERE state = 'failed' ORDER BY outcome_id DESC LIMIT 1`).Scan(&persistedCode); err != nil {
		t.Fatal(err)
	}
	if persistedCode != "provider_http:401" {
		t.Fatalf("persisted error code = %q, want provider_http:401", persistedCode)
	}
	if err := fixture.writer.Close(ctx); err != nil {
		t.Fatal(err)
	}
	if err := fixture.store.Close(); err != nil {
		t.Fatal(err)
	}

	reopenedStore, err := sqlite.Open(ctx, filepath.Join(fixture.root, "mahoroba.db"))
	if err != nil {
		t.Fatal(err)
	}
	restartedIDs := canonical.NewSecureIDGenerator()
	reopenedWriter, err := canonical.OpenWriter(ctx, canonical.WriterOptions{
		Backend: reopenedStore.Canonical(), IDs: restartedIDs, Clock: fixture.clock,
		Timezone: canonical.MustTimezone("UTC"), QueueCapacity: 32,
	})
	if err != nil {
		_ = reopenedStore.Close()
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := reopenedWriter.Close(context.Background()); err != nil {
			t.Errorf("close restarted writer: %v", err)
		}
		if err := reopenedStore.Close(); err != nil {
			t.Errorf("close restarted store: %v", err)
		}
	})
	reopenedBlobs, err := blob.NewFileStore(filepath.Join(fixture.root, "blobs"))
	if err != nil {
		t.Fatal(err)
	}
	restartGenerator := &scriptedGenerator{}
	restarted, err := New(Options{
		Writer: reopenedWriter, Repository: reopenedStore.Canonical(), IDs: restartedIDs, Clock: fixture.clock,
		Timezone: canonical.MustTimezone("UTC"), Blobs: reopenedBlobs, Generator: restartGenerator,
		Provider: "test", Model: "test-model", MaxAttempts: 3,
		RetryBackoff: []time.Duration{time.Nanosecond, time.Nanosecond}, MaxInputBytes: 64 << 10,
		MaxOutputBytes: 64 << 10, SafetyScanInterval: time.Hour,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := restarted.Recover(ctx); err != nil {
		t.Fatal(err)
	}
	work, err := discoverDialogueWorkForTest(ctx, reopenedStore.Canonical(), fixture.residentID, 10, 3)
	if err != nil {
		t.Fatal(err)
	}
	if len(work) != 1 || work[0].State != domain.WorkTerminalFailed {
		t.Fatalf("work after restart = %+v, want terminal_failed", work)
	}
	if err := restarted.ProcessResident(ctx, fixture.residentID); err != nil {
		t.Fatal(err)
	}
	if got := restartGenerator.CallCount(); got != 0 {
		t.Fatalf("provider calls after restart = %d, want 0", got)
	}
}

func TestRunContextCancellationRemainsRetryable(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	generator := &scriptedGenerator{steps: []generatorStep{{
		beforeReturn: cancel,
		err:          &generation.ProviderError{Class: generation.ErrorCancelled},
	}}}
	fixture := newApplicationFixture(t, generator, 3)
	if _, err := fixture.application.Ingress(ctx, "survive graceful shutdown"); err != nil {
		t.Fatal(err)
	}
	if err := fixture.application.ProcessResident(ctx, fixture.residentID); !errors.Is(err, context.Canceled) {
		t.Fatalf("ProcessResident error = %v, want context cancellation after durable failure", err)
	}
	var persistedCode string
	if err := fixture.store.Reader().QueryRowContext(context.Background(), `SELECT error_class FROM generation_run_outcomes
		WHERE state = 'failed' ORDER BY outcome_id DESC LIMIT 1`).Scan(&persistedCode); err != nil {
		t.Fatal(err)
	}
	if persistedCode != generation.RuntimeInterruptedErrorCode().String() {
		t.Fatalf("persisted error code = %q, want %q", persistedCode, generation.RuntimeInterruptedErrorCode())
	}
	work, err := discoverDialogueWorkForTest(context.Background(), fixture.repository, fixture.residentID, 10, 3)
	if err != nil {
		t.Fatal(err)
	}
	if len(work) != 1 || work[0].State != domain.WorkRetryPending {
		t.Fatalf("work = %+v, want retry_pending after run-context cancellation", work)
	}
}

func TestInactivePendingDialogueIsCanonicallyCancelledAndSurvivesRestart(t *testing.T) {
	ctx := context.Background()
	generator := &scriptedGenerator{}
	fixture := newApplicationFixture(t, generator, 3)
	if _, err := fixture.application.Ingress(ctx, "will become inactive"); err != nil {
		t.Fatal(err)
	}
	if err := fixture.application.ArchiveResident(ctx, fixture.residentID); err != nil {
		t.Fatal(err)
	}
	if err := fixture.application.ProcessResident(ctx, fixture.residentID); err != nil {
		t.Fatal(err)
	}
	if got := generator.CallCount(); got != 0 {
		t.Fatalf("provider calls = %d, want 0", got)
	}
	work, err := discoverDialogueWorkForTest(ctx, fixture.repository, fixture.residentID, 10, 3)
	if err != nil {
		t.Fatal(err)
	}
	if len(work) != 1 || work[0].State != domain.WorkTerminalFailed || work[0].AttemptNo != 0 {
		t.Fatalf("work = %+v, want no-dispatch terminal cancellation for the pending obligation", work)
	}
	var attempt int64
	var state, errorCode string
	var inputCount int
	if err := fixture.store.Reader().QueryRowContext(ctx, `SELECT o.attempt_no, o.state, o.error_class,
		(SELECT COUNT(*) FROM generation_run_inputs i WHERE i.generation_run_id = o.generation_run_id)
		FROM generation_run_outcomes o JOIN canonical_commits c ON c.canonical_commit_id = o.canonical_commit_id
		ORDER BY c.commit_seq DESC, o.outcome_id DESC LIMIT 1`).Scan(&attempt, &state, &errorCode, &inputCount); err != nil {
		t.Fatal(err)
	}
	if attempt != 0 || state != "cancelled" || errorCode != string(generation.ErrorResidentInactive) || inputCount != 0 {
		t.Fatalf("cancellation outcome = attempt=%d state=%q code=%q inputs=%d", attempt, state, errorCode, inputCount)
	}

	if err := fixture.writer.Close(ctx); err != nil {
		t.Fatal(err)
	}
	if err := fixture.store.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := sqlite.Open(ctx, filepath.Join(fixture.root, "mahoroba.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := reopened.Close(); err != nil {
			t.Errorf("close reopened cancellation store: %v", err)
		}
	})
	work, err = discoverDialogueWorkForTest(ctx, reopened.Canonical(), fixture.residentID, 10, 3)
	if err != nil {
		t.Fatal(err)
	}
	if len(work) != 1 || work[0].State != domain.WorkTerminalFailed {
		t.Fatalf("work after restart = %+v, want terminal_failed", work)
	}
}

func TestErasedSourceIsCancelledWithoutProviderPlaceholderAndLaterWorkContinues(t *testing.T) {
	ctx := context.Background()
	generator := &scriptedGenerator{steps: []generatorStep{{text: "reply to surviving message"}}}
	fixture := newApplicationFixture(t, generator, 3)
	erasedEvent, err := fixture.application.Ingress(ctx, "erase this source")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := fixture.application.Ingress(ctx, "surviving message"); err != nil {
		t.Fatal(err)
	}
	eraseEventContentForTest(t, fixture.store.Path(), erasedEvent.ContentID)

	if err := fixture.application.ProcessResident(ctx, fixture.residentID); err != nil {
		t.Fatal(err)
	}
	if got := generator.CallCount(); got != 1 {
		t.Fatalf("provider calls = %d, want only the surviving obligation", got)
	}
	for _, request := range generator.Requests() {
		for _, message := range request.Messages {
			if message.Text == "[erased]" {
				t.Fatal("provider received erased-content display placeholder")
			}
		}
	}
	work, err := discoverDialogueWorkForTest(ctx, fixture.repository, fixture.residentID, 10, 3)
	if err != nil {
		t.Fatal(err)
	}
	if len(work) != 2 || work[0].State != domain.WorkTerminalFailed || work[1].State != domain.WorkSucceeded {
		t.Fatalf("work = %+v, want terminal erased obligation followed by succeeded work", work)
	}
	var errorCode string
	if err := fixture.store.Reader().QueryRowContext(ctx, `SELECT o.error_class
		FROM generation_run_outcomes o JOIN generation_runs r ON r.generation_run_id = o.generation_run_id
		WHERE r.idempotency_key = ? ORDER BY o.outcome_id DESC LIMIT 1`, domain.DialogueObligation(erasedEvent.ID)).Scan(&errorCode); err != nil {
		t.Fatal(err)
	}
	if errorCode != string(generation.ErrorSourceContentErased) {
		t.Fatalf("erased-source cancellation code = %q", errorCode)
	}
}

func TestUnavailableDialogueCancelsExistingRunningAndRetryPendingAttempts(t *testing.T) {
	tests := []struct {
		name         string
		retryPending bool
		wantAttempt  int64
		wantOutcomes int
	}{
		{name: "running", wantAttempt: 1, wantOutcomes: 2},
		{name: "retry pending", retryPending: true, wantAttempt: 2, wantOutcomes: 4},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			ctx := context.Background()
			generator := &scriptedGenerator{}
			fixture := newApplicationFixture(t, generator, 3)
			event, err := fixture.application.Ingress(ctx, "prepared before archive")
			if err != nil {
				t.Fatal(err)
			}
			_ = dialogueRunIDForEventForTest(t, fixture, event)
			if test.retryPending {
				if err := fixture.application.Recover(ctx); err != nil {
					t.Fatal(err)
				}
			}
			if err := fixture.application.ArchiveResident(ctx, fixture.residentID); err != nil {
				t.Fatal(err)
			}
			if err := fixture.application.ProcessResident(ctx, fixture.residentID); err != nil {
				t.Fatal(err)
			}
			if got := generator.CallCount(); got != 0 {
				t.Fatalf("provider calls = %d, want 0", got)
			}
			work, err := discoverDialogueWorkForTest(ctx, fixture.repository, fixture.residentID, 10, 3)
			if err != nil {
				t.Fatal(err)
			}
			if len(work) != 1 || work[0].State != domain.WorkTerminalFailed || work[0].AttemptNo != test.wantAttempt {
				t.Fatalf("work = %+v, want terminal attempt %d", work, test.wantAttempt)
			}
			var outcomeCount int
			if err := fixture.store.Reader().QueryRowContext(ctx, `SELECT COUNT(*) FROM generation_run_outcomes`).Scan(&outcomeCount); err != nil {
				t.Fatal(err)
			}
			if outcomeCount != test.wantOutcomes {
				t.Fatalf("outcome count = %d, want %d", outcomeCount, test.wantOutcomes)
			}
			running, err := fixture.repository.RunningAttempts(ctx, 10)
			if err != nil {
				t.Fatal(err)
			}
			if len(running) != 0 {
				t.Fatalf("running attempts after same-UoW cancellation = %+v", running)
			}
			if err := fixture.application.Recover(ctx); err != nil {
				t.Fatalf("recovery after cancellation: %v", err)
			}
		})
	}
}

func TestDialogueDiscoveryKeysetFindsOlderActionableWorkBeyondTerminalWindow(t *testing.T) {
	ctx := context.Background()
	fixture := newApplicationFixture(t, &scriptedGenerator{}, 3)

	pending := ingressPendingForTest(t, fixture, "old pending")
	retryPending, err := fixture.application.Ingress(ctx, "old retry pending")
	if err != nil {
		t.Fatal(err)
	}
	retryRunID := dialogueRunIDForEventForTest(t, fixture, retryPending)
	failDialogueRunForTest(t, fixture, retryRunID, generation.RuntimeInterruptedErrorCode().String())
	running, err := fixture.application.Ingress(ctx, "old running")
	if err != nil {
		t.Fatal(err)
	}
	_ = dialogueRunIDForEventForTest(t, fixture, running)
	erased := ingressPendingForTest(t, fixture, "old erased")
	eraseEventContentForTest(t, fixture.store.Path(), erased.ContentID)

	terminalEvents := make([]domain.Event, 129)
	terminalContentIDs := make([]canonical.ID, len(terminalEvents))
	for index := range terminalEvents {
		terminalEvents[index] = ingressPendingForTest(t, fixture, fmt.Sprintf("new terminal %03d", index))
		terminalContentIDs[index] = terminalEvents[index].ContentID
	}
	eraseEventContentsForTest(t, fixture.store.Path(), terminalContentIDs)
	terminalCode := generation.MustOutcomeErrorCode(generation.ErrorSourceContentErased, 0).String()
	for _, event := range terminalEvents {
		if err := fixture.application.cancelDialogueWork(ctx, domain.DialogueWork{
			UserEvent: event, State: domain.WorkPending, CancellationCode: terminalCode,
		}, nil, nil); err != nil {
			t.Fatal(err)
		}
	}

	work, err := discoverDialogueWorkForTest(ctx, fixture.repository, fixture.residentID, 128, 3)
	if err != nil {
		t.Fatal(err)
	}
	if len(work) != 132 {
		t.Fatalf("discovered work count = %d, want newest 128 plus 4 older actionable obligations", len(work))
	}
	for index := 1; index < len(work); index++ {
		if work[index-1].UserEvent.Seq >= work[index].UserEvent.Seq {
			t.Fatalf("work is not in ascending event order at %d: %s then %s", index, work[index-1].UserEvent.Seq, work[index].UserEvent.Seq)
		}
	}
	wantOldest := []struct {
		event            domain.Event
		state            domain.WorkState
		cancellationCode string
	}{
		{event: pending, state: domain.WorkPending},
		{event: retryPending, state: domain.WorkRetryPending},
		{event: running, state: domain.WorkRunning},
		{
			event: erased, state: domain.WorkPending,
			cancellationCode: generation.MustOutcomeErrorCode(generation.ErrorSourceContentErased, 0).String(),
		},
	}
	for index, want := range wantOldest {
		got := work[index]
		if got.UserEvent.ID != want.event.ID || got.State != want.state || got.CancellationCode != want.cancellationCode {
			t.Fatalf("older actionable work %d = %+v, want event=%s state=%s cancellation=%q", index, got, want.event.ID, want.state, want.cancellationCode)
		}
	}
	for _, item := range work[len(wantOldest):] {
		if item.State != domain.WorkTerminalFailed {
			t.Fatalf("newest-window work = %+v, want terminal_failed", item)
		}
	}
}

func TestApplicationRecoverDrainsRunningAttemptsBeyondBatchBoundary(t *testing.T) {
	ctx := context.Background()
	fixture := newApplicationFixture(t, &scriptedGenerator{}, 3)

	const runningCount = 257
	prepareRunningRecoveryAttemptsForTest(t, fixture, runningCount)
	running, err := fixture.repository.RunningAttempts(ctx, runningCount+1)
	if err != nil {
		t.Fatal(err)
	}
	if len(running) != runningCount {
		t.Fatalf("running attempts before recovery = %d, want %d", len(running), runningCount)
	}

	if err := fixture.application.Recover(ctx); err != nil {
		t.Fatal(err)
	}
	running, err = fixture.repository.RunningAttempts(ctx, 1)
	if err != nil {
		t.Fatal(err)
	}
	if len(running) != 0 {
		t.Fatalf("running attempts after recovery = %+v, want none", running)
	}
	var cancelled int
	if err := fixture.store.Reader().QueryRowContext(ctx, `SELECT COUNT(*) FROM generation_run_outcomes
		WHERE state = 'cancelled' AND error_class = ?`, generation.RuntimeInterruptedErrorCode().String()).Scan(&cancelled); err != nil {
		t.Fatal(err)
	}
	if cancelled != runningCount {
		t.Fatalf("recovery cancellation outcomes = %d, want %d", cancelled, runningCount)
	}
}

func TestApplicationRecoverCancelsRunningAttemptAndResumesDurableRequest(t *testing.T) {
	generator := &scriptedGenerator{steps: []generatorStep{{text: "after restart"}}}
	fixture := newApplicationFixture(t, generator, 2)
	ctx := context.Background()

	event, err := fixture.application.Ingress(ctx, "survive restart")
	if err != nil {
		t.Fatal(err)
	}
	_ = dialogueRunIDForEventForTest(t, fixture, event)
	work, err := discoverDialogueWorkForTest(ctx, fixture.repository, fixture.residentID, 10, 2)
	if err != nil {
		t.Fatal(err)
	}
	if len(work) != 1 || work[0].UserEvent.ID != event.ID || work[0].State != domain.WorkRunning || work[0].AttemptNo != 1 {
		t.Fatalf("work after explicit PrepareDialogue = %+v, want running attempt 1", work)
	}
	running, err := fixture.repository.RunningAttempts(ctx, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(running) != 1 || running[0].AttemptNo != 1 {
		t.Fatalf("running attempts = %+v, want attempt 1", running)
	}

	if err := fixture.application.Recover(ctx); err != nil {
		t.Fatal(err)
	}
	if err := fixture.application.Recover(ctx); err != nil {
		t.Fatalf("second recovery was not idempotent: %v", err)
	}
	running, err = fixture.repository.RunningAttempts(ctx, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(running) != 0 {
		t.Fatalf("running attempts after recovery = %+v", running)
	}
	work, err = discoverDialogueWorkForTest(ctx, fixture.repository, fixture.residentID, 10, 2)
	if err != nil {
		t.Fatal(err)
	}
	if len(work) != 1 || work[0].State != domain.WorkRetryPending || work[0].AttemptNo != 1 {
		t.Fatalf("recovered work = %+v, want retry_pending attempt 1", work)
	}

	if err := fixture.application.ProcessResident(ctx, fixture.residentID); err != nil {
		t.Fatal(err)
	}
	history, err := fixture.repository.History(ctx, fixture.residentID, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(history) != 2 || history[1].Content != "after restart" {
		t.Fatalf("history after recovery = %+v", history)
	}
}

type applicationFixture struct {
	application *Application
	repository  *sqlite.CanonicalRepository
	blobs       *blob.FileStore
	residentID  canonical.ID
	root        string
	store       *sqlite.Store
	writer      *canonical.Writer
	clock       *testsupport.ManualClock
}

func newApplicationFixture(t *testing.T, generator generation.Generator, maxAttempts int) applicationFixture {
	return newApplicationFixtureWithPersona(t, generator, maxAttempts, "friendly")
}

func newApplicationFixtureWithPersona(t *testing.T, generator generation.Generator, maxAttempts int, persona string) applicationFixture {
	t.Helper()
	ctx := context.Background()
	root := t.TempDir()
	store, err := sqlite.Open(ctx, filepath.Join(root, "mahoroba.db"))
	if err != nil {
		t.Fatal(err)
	}
	clock := testsupport.NewManualClock(time.Unix(1_700_000_000, 0).UTC())
	ids, err := canonical.NewIDGenerator(clock, bytes.NewReader(bytes.Repeat([]byte{0x52}, 64)))
	if err != nil {
		_ = store.Close()
		t.Fatal(err)
	}
	writer, err := canonical.OpenWriter(ctx, canonical.WriterOptions{
		Backend: store.Canonical(), IDs: ids, Clock: clock,
		Timezone: canonical.MustTimezone("UTC"), QueueCapacity: 32,
	})
	if err != nil {
		_ = store.Close()
		t.Fatal(err)
	}
	blobStore, err := blob.NewFileStore(filepath.Join(root, "blobs"))
	if err != nil {
		_ = writer.Close(ctx)
		_ = store.Close()
		t.Fatal(err)
	}
	retryBackoff := make([]time.Duration, maxAttempts-1)
	application, err := New(Options{
		Writer: writer, Repository: store.Canonical(), IDs: ids, Clock: clock,
		Timezone: canonical.MustTimezone("UTC"), Blobs: blobStore, Generator: generator,
		Provider: "test", Model: "test-model", MaxAttempts: maxAttempts,
		RetryBackoff: retryBackoff, MaxInputBytes: 64 << 10, MaxOutputBytes: 64 << 10,
		SafetyScanInterval: time.Hour,
	})
	if err != nil {
		_ = writer.Close(ctx)
		_ = store.Close()
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := writer.Close(context.Background()); err != nil {
			t.Errorf("close writer: %v", err)
		}
		if err := store.Close(); err != nil {
			t.Errorf("close store: %v", err)
		}
	})

	state, err := application.BootstrapInit(ctx, BootstrapInput{
		OwnerName: "Owner", Name: "Resident", SeedKey: "resident-1", Principles: "be helpful",
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(state.Residents) != 1 {
		t.Fatalf("bootstrap residents = %d, want 1", len(state.Residents))
	}
	residentID := state.Residents[0].ResidentID
	if err := application.ApprovePrinciples(ctx, residentID); err != nil {
		t.Fatal(err)
	}
	if err := application.FinalizeBootstrap(ctx, residentID, persona, `{"mandatory_event_types":[],"memory_recall_enabled":false,"version":"memory-policy-v1"}`); err != nil {
		t.Fatal(err)
	}
	if err := application.SelectResident(ctx, residentID); err != nil {
		t.Fatal(err)
	}
	selected, err := store.Canonical().ActiveResident(ctx)
	if err != nil {
		t.Fatal(err)
	}
	wantIdleGap := canonical.Duration((30 * time.Minute) / time.Microsecond)
	if selected.SessionIdleGap != wantIdleGap {
		t.Fatalf("selected resident session idle gap = %s, want canonical policy value %s", selected.SessionIdleGap, wantIdleGap)
	}
	fixture := applicationFixture{
		application: application, repository: store.Canonical(), blobs: blobStore, residentID: residentID,
		root: root, store: store, writer: writer, clock: clock,
	}
	// Canonical Writer advances equal-clock commits by a microsecond. Move the
	// manual wall clock beyond bootstrap so Projection as_of can close over all
	// activated revisions exactly as production wall time does.
	fixture.clock.Advance(time.Second)
	coordinator := newCOV1ProjectionCoordinator(t, fixture)
	fixture.application.commitNotifier = coordinator
	fixture.application.dialogueReconciler = coordinator
	return fixture
}

type generatorStep struct {
	text         string
	deltas       []string
	err          error
	beforeReturn func()
}

type scriptedGenerator struct {
	mu       sync.Mutex
	steps    []generatorStep
	calls    int
	requests []generation.Request
}

func (generator *scriptedGenerator) Stream(_ context.Context, request generation.Request, sink generation.DeltaSink) (generation.Result, error) {
	generator.mu.Lock()
	index := generator.calls
	generator.calls++
	generator.requests = append(generator.requests, request)
	if index >= len(generator.steps) {
		generator.mu.Unlock()
		return generation.Result{}, errors.New("unexpected generator call")
	}
	step := generator.steps[index]
	generator.mu.Unlock()
	for _, text := range step.deltas {
		sink(generation.Delta{Text: text})
	}
	if step.beforeReturn != nil {
		step.beforeReturn()
	}
	if step.err != nil {
		return generation.Result{}, step.err
	}
	return generation.Result{Text: step.text, FinishReason: "stop", ModelVersion: "test-model-v1"}, nil
}

func (generator *scriptedGenerator) CallCount() int {
	generator.mu.Lock()
	defer generator.mu.Unlock()
	return generator.calls
}

func (generator *scriptedGenerator) Requests() []generation.Request {
	generator.mu.Lock()
	defer generator.mu.Unlock()
	return append([]generation.Request(nil), generator.requests...)
}

func prepareDialogueRunForTest(t *testing.T, fixture applicationFixture, event domain.Event) {
	t.Helper()
	ctx := context.Background()
	resident, err := fixture.repository.Resident(ctx, fixture.residentID)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := fixture.application.preparePendingDialogue(ctx, resident, domain.DialogueWork{
		UserEvent: event,
		State:     domain.WorkPending,
	}); err != nil {
		t.Fatal(err)
	}
}

func ingressPendingForTest(t *testing.T, fixture applicationFixture, text string) domain.Event {
	t.Helper()
	ctx := context.Background()
	resident, err := fixture.repository.Resident(ctx, fixture.residentID)
	if err != nil {
		t.Fatal(err)
	}
	content, err := fixture.application.newContent(fixture.residentID, "event_payload", []byte(text), "independent")
	if err != nil {
		t.Fatal(err)
	}
	eventID, err := fixture.application.ids.New()
	if err != nil {
		t.Fatal(err)
	}
	now := canonical.InstantFromTime(fixture.clock.Now())
	result, err := fixture.application.submitWithContent(ctx, domain.IngressUserMessageCommand(domain.IngressUserMessage{
		EventID: eventID, ResidentID: fixture.residentID,
		OwnerPrincipalID: resident.OwnerPrincipalID, ResidentPrincipalID: resident.ResidentPrincipalID,
		Content: content, OccurredAt: now, OccurredTZ: canonical.MustTimezone("UTC"),
	}), []domain.Content{content})
	if err != nil {
		t.Fatal(err)
	}
	event, ok := result.Value.(domain.Event)
	if !ok {
		t.Fatalf("pending ingress result = %T, want domain.Event", result.Value)
	}
	return event
}

func dialogueRunIDForEventForTest(t *testing.T, fixture applicationFixture, event domain.Event) canonical.ID {
	t.Helper()
	var raw string
	err := fixture.store.Reader().QueryRow(`SELECT generation_run_id FROM generation_runs
		WHERE resident_id = ? AND idempotency_key = ?`, fixture.residentID.String(), domain.DialogueObligation(event.ID)).Scan(&raw)
	if errors.Is(err, sql.ErrNoRows) {
		ctx := context.Background()
		resident, residentErr := fixture.repository.ActiveResident(ctx)
		if residentErr != nil {
			t.Fatal(residentErr)
		}
		prepared, prepareErr := fixture.application.preparePendingDialogue(ctx, resident, domain.DialogueWork{
			UserEvent: event,
			State:     domain.WorkPending,
		})
		if prepareErr != nil {
			t.Fatal(prepareErr)
		}
		return prepared.RunID
	}
	if err != nil {
		t.Fatal(err)
	}
	runID, err := canonical.ParseID(raw)
	if err != nil {
		t.Fatal(err)
	}
	return runID
}

func prepareRunningRecoveryAttemptsForTest(t *testing.T, fixture applicationFixture, count int) {
	t.Helper()
	ctx := context.Background()
	resident, err := fixture.repository.Resident(ctx, fixture.residentID)
	if err != nil {
		t.Fatal(err)
	}
	pipelineIDs, err := fixture.application.allocateIDs(1)
	if err != nil {
		t.Fatal(err)
	}
	pipelineDefinition, err := canonical.MarshalCanonical(struct {
		Version string `json:"version"`
	}{Version: domain.PersonaRevisionPipelineVersion})
	if err != nil {
		t.Fatal(err)
	}
	pipeline := domain.PipelineVersionDefinition{
		ID: pipelineIDs[0], Kind: "persona_revision",
		VersionKey: domain.PersonaRevisionPipelineVersion, Definition: pipelineDefinition,
	}
	if _, err := fixture.application.submit(ctx, domain.RegisterPipelineVersionsCommand(
		domain.RegisterPipelineVersions{Versions: []domain.PipelineVersionDefinition{pipeline}},
	)); err != nil {
		t.Fatal(err)
	}
	schema, err := memory.PersonaRevisionJSONSchema()
	if err != nil {
		t.Fatal(err)
	}
	_, params, err := domain.NewStructuredGeneratorParams(
		false, canonical.ByteSize(64<<10), generation.StructuredOutputPrompt,
		memory.PersonaOutputSchemaVersionV1, canonical.HashBlob(schema.Bytes()),
	)
	if err != nil {
		t.Fatal(err)
	}
	dropped, err := canonical.MarshalCanonical(struct {
		MemoryRecall string `json:"memory_recall"`
	}{MemoryRecall: "not_applicable"})
	if err != nil {
		t.Fatal(err)
	}

	for index := 0; index < count; index++ {
		content, err := fixture.application.newContent(
			fixture.residentID, "generation_input", []byte("recovery running attempt fixture"), "independent",
		)
		if err != nil {
			t.Fatal(err)
		}
		ids, err := fixture.application.allocateIDs(3)
		if err != nil {
			t.Fatal(err)
		}
		prepare := domain.PrepareGeneration{
			RunID: ids[0], ResidentID: fixture.residentID, Purpose: domain.GenerationPurposePersonaRevision,
			IdempotencyKey: fmt.Sprintf("recovery-running-fixture:%03d", index),
			Provider:       "test", Model: "test-model", PipelineVersionID: pipeline.ID,
			PrinciplesRevisionID: resident.PrinciplesRevisionID, PersonaRevisionID: resident.PersonaRevisionID,
			MemoryPolicyRevisionID: resident.MemoryPolicyRevisionID,
			AsOf:                   canonical.InstantFromTime(fixture.clock.Now()),
			AsOfTZ:                 canonical.MustTimezone("UTC"),
			DroppedInputSummary:    dropped, GeneratorParams: params, RunningOutcomeID: ids[1],
			Inputs: []domain.GenerationInput{{
				ID: ids[2], Ordinal: 0, Role: string(generation.RoleSystem),
				SourceType: "runtime_projection", InclusionMode: "runtime_projection", Content: content,
			}},
		}
		if err := pinGenerationVersions(&prepare); err != nil {
			t.Fatal(err)
		}
		if _, err := fixture.application.submitWithContent(
			ctx, domain.PrepareGenerationCommand(prepare), []domain.Content{content},
		); err != nil {
			t.Fatal(err)
		}
	}
}

func failDialogueRunForTest(t *testing.T, fixture applicationFixture, runID canonical.ID, errorClass string) {
	t.Helper()
	ids, err := fixture.application.allocateIDs(1)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := fixture.writer.Submit(context.Background(), domain.FailAttemptCommand(domain.FailAttempt{
		Attempt: domain.Attempt{
			RunID: runID, ResidentID: fixture.residentID, AttemptNo: 1, OutcomeID: ids[0],
		},
		State: "failed", ErrorClass: errorClass,
	})); err != nil {
		t.Fatal(err)
	}
}

func eraseEventContentForTest(t *testing.T, databasePath string, contentID canonical.ID) {
	eraseEventContentsForTest(t, databasePath, []canonical.ID{contentID})
}

func eraseEventContentsForTest(t *testing.T, databasePath string, contentIDs []canonical.ID) {
	t.Helper()
	database, err := sql.Open("sqlite", databasePath)
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	tx, err := database.Begin()
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	for _, contentID := range contentIDs {
		if _, err := tx.Exec(`UPDATE content_objects
			SET erasure_state = 'erased', blob_hash = NULL, commitment_salt = NULL
			WHERE content_id = ?`, contentID.String()); err != nil {
			t.Fatal(err)
		}
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
}

func TestSubmitWithContentFinalizesBeforeCanonicalAndRetainsRollbackOrphan(t *testing.T) {
	fixture := newApplicationFixture(t, &scriptedGenerator{}, 1)
	ctx := context.Background()
	resident, err := fixture.repository.Resident(ctx, fixture.residentID)
	if err != nil {
		t.Fatal(err)
	}
	newIngress := func(text string) (domain.Content, canonical.Command) {
		content, err := fixture.application.newContent(fixture.residentID, "event_payload", []byte(text), "independent")
		if err != nil {
			t.Fatal(err)
		}
		eventID, err := fixture.application.ids.New()
		if err != nil {
			t.Fatal(err)
		}
		return content, domain.IngressUserMessageCommand(domain.IngressUserMessage{
			EventID: eventID, ResidentID: fixture.residentID,
			OwnerPrincipalID: resident.OwnerPrincipalID, ResidentPrincipalID: resident.ResidentPrincipalID,
			Content: content, OccurredAt: canonical.InstantFromTime(fixture.clock.Now()), OccurredTZ: canonical.MustTimezone("UTC"),
		})
	}

	committedContent, committedCommand := newIngress("physical first")
	physicalObserved := false
	checkedCommitted := checkedCanonicalCommand{
		Command: committedCommand,
		beforeExecute: func() error {
			exists, err := fixture.blobs.Exists(fixture.residentID, committedContent.BlobHash)
			physicalObserved = exists
			return err
		},
	}
	if _, err := fixture.application.submitWithContent(ctx, checkedCommitted, []domain.Content{committedContent}); err != nil {
		t.Fatal(err)
	}
	if !physicalObserved {
		t.Fatal("Canonical execution began before the physical blob existed")
	}
	if orphans, err := fixture.blobs.ListOrphans(ctx); err != nil || len(orphans) != 0 {
		t.Fatalf("committed recovery markers = %v, %v", orphans, err)
	}

	rolledBackContent, rolledBackCommand := newIngress("rollback orphan")
	rollbackErr := errors.New("test rollback after physical finalize")
	physicalObserved = false
	checkedRollback := checkedCanonicalCommand{
		Command: rolledBackCommand,
		beforeExecute: func() error {
			exists, err := fixture.blobs.Exists(fixture.residentID, rolledBackContent.BlobHash)
			physicalObserved = exists
			return err
		},
		forcedError: rollbackErr,
	}
	if _, err := fixture.application.submitWithContent(ctx, checkedRollback, []domain.Content{rolledBackContent}); !errors.Is(err, rollbackErr) {
		t.Fatalf("rollback error = %v", err)
	}
	if !physicalObserved {
		t.Fatal("rollback command did not observe its finalized blob")
	}
	orphans, err := fixture.blobs.ListOrphans(ctx)
	if err != nil || len(orphans) != 1 || orphans[0].Kind != blob.OrphanSealed {
		t.Fatalf("rollback orphans = %v, %v", orphans, err)
	}
	result, err := fixture.blobs.CleanupOrphan(ctx, orphans[0], fixture.repository)
	if err != nil {
		t.Fatal(err)
	}
	if result.Disposition != blob.CleanupUnreferencedObjectRemoved {
		t.Fatalf("rollback cleanup = %+v", result)
	}
	if exists, err := fixture.blobs.Exists(fixture.residentID, rolledBackContent.BlobHash); err != nil || exists {
		t.Fatalf("rolled-back physical object = %v, %v", exists, err)
	}
}

type checkedCanonicalCommand struct {
	canonical.Command
	beforeExecute func() error
	forcedError   error
}

func (command checkedCanonicalCommand) Execute(ctx context.Context, uow canonical.CanonicalUoW) (any, error) {
	if command.beforeExecute != nil {
		if err := command.beforeExecute(); err != nil {
			return nil, err
		}
	}
	if command.forcedError != nil {
		return nil, command.forcedError
	}
	return command.Command.Execute(ctx, uow)
}

var _ generation.Generator = (*scriptedGenerator)(nil)
