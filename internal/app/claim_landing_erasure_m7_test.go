package app

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"

	"mahoroba.local/mahoroba/internal/canonical"
	"mahoroba.local/mahoroba/internal/domain"
	"mahoroba.local/mahoroba/internal/generation"
)

var errM7CancelWriterInjected = errors.New("app test: injected erased-source cancellation writer failure")

func TestM7DialogueRecalledClaimLandingRaceCancelsExactAttempt(t *testing.T) {
	fixture := newRecallReadyFixture(t)
	fixture.application.generator = &scriptedGenerator{steps: []generatorStep{{
		text: "reply after recalled claim",
		beforeReturn: func() {
			eraseM7ApplicationClaim(t, fixture.applicationFixture, fixture.claimID)
		},
	}}}
	event, runID := ingressRecallDialogue(t, fixture, "use the recalled tea memory")
	err := fixture.application.processPreparedRun(context.Background(), fixture.residentID, runID)
	if !errors.Is(err, domain.ErrClaimSourceIneligible) {
		t.Fatalf("recalled-claim landing race error = %v, want ErrClaimSourceIneligible", err)
	}
	assertM7DialogueCancellationRows(t, fixture.applicationFixture, runID, 2, 1, 0)
	var eventState string
	if err := fixture.store.Reader().QueryRow(`SELECT content.erasure_state FROM events event
		JOIN content_objects content ON content.content_id = event.content_id
		WHERE event.event_id = ?`, event.ID.String()).Scan(&eventState); err != nil {
		t.Fatal(err)
	}
	if eventState != "present" {
		t.Fatalf("dialogue event erasure state = %q, want present; only recalled claim should be erased", eventState)
	}
	var latestState, latestCode string
	var latestAttempt int64
	if err := fixture.store.Reader().QueryRow(`SELECT attempt_no, state, error_class
		FROM generation_run_outcomes WHERE generation_run_id = ?
		ORDER BY attempt_no DESC, CASE WHEN state = 'running' THEN 0 ELSE 1 END DESC LIMIT 1`, runID.String()).Scan(
		&latestAttempt, &latestState, &latestCode,
	); err != nil {
		t.Fatal(err)
	}
	if latestAttempt != 1 || latestState != "cancelled" || latestCode != string(generation.ErrorSourceContentErased) {
		t.Fatalf("recalled-claim cancellation = attempt %d %s/%s", latestAttempt, latestState, latestCode)
	}
}

func TestM7DialogueErasedSourceCancellationFailureJoinsCauseAndLeavesRunning(t *testing.T) {
	fixture, prepared := prepareM7RunningDialogueForErasedCancellation(t)
	backend := installM7ArmableBeginFailureBackend(t, fixture)
	backend.ArmNext()
	err := fixture.application.recordLandingFailure(
		context.Background(), prepared,
		fmt.Errorf("landing validation: %w", domain.ErrClaimSourceIneligible),
	)
	if !errors.Is(err, domain.ErrClaimSourceIneligible) || !errors.Is(err, errM7CancelWriterInjected) {
		t.Fatalf("joined dialogue landing/cancellation error = %v", err)
	}
	assertM7DialogueCancellationRows(t, fixture, prepared.RunID, 1, 0, 0)
}

func TestM7DialogueErasedSourceCancellationIsAttemptBoundAndExactlyOnce(t *testing.T) {
	t.Run("same attempt replay", func(t *testing.T) {
		fixture, prepared := prepareM7RunningDialogueForErasedCancellation(t)
		observer := &m7DialogueCancellationObserver{}
		fixture.application.SetObserver(observer)
		if err := fixture.application.cancelDialogueForErasedSource(context.Background(), prepared); err != nil {
			t.Fatalf("first cancellation: %v", err)
		}
		var committedAfterFirst, committedAfterReplay int
		if err := fixture.store.Reader().QueryRow(`SELECT COUNT(*) FROM canonical_commits`).Scan(&committedAfterFirst); err != nil {
			t.Fatal(err)
		}
		if err := fixture.application.cancelDialogueForErasedSource(context.Background(), prepared); err != nil {
			t.Fatalf("cancellation replay: %v", err)
		}
		if err := fixture.store.Reader().QueryRow(`SELECT COUNT(*) FROM canonical_commits`).Scan(&committedAfterReplay); err != nil {
			t.Fatal(err)
		}
		observer.mu.Lock()
		failedNotifications := observer.failed
		observer.mu.Unlock()
		if committedAfterReplay != committedAfterFirst || failedNotifications != 1 {
			t.Fatalf("replay commits/notifications = %d->%d/%d, want unchanged/1",
				committedAfterFirst, committedAfterReplay, failedNotifications)
		}
		assertM7DialogueCancellationRows(t, fixture, prepared.RunID, 2, 1, 0)
	})

	t.Run("newer attempt conflicts", func(t *testing.T) {
		fixture, prepared := prepareM7RunningDialogueForErasedCancellation(t)
		ids, err := fixture.application.allocateIDs(2)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := fixture.application.submit(context.Background(), domain.FailAttemptCommand(domain.FailAttempt{
			Attempt: domain.Attempt{RunID: prepared.RunID, ResidentID: prepared.ResidentID,
				AttemptNo: prepared.AttemptNo, OutcomeID: ids[0]},
			State: "failed", ErrorClass: generation.MustOutcomeErrorCode(generation.ErrorTimeout, 0).String(),
		})); err != nil {
			t.Fatal(err)
		}
		if _, err := fixture.application.submit(context.Background(), domain.StartAttemptCommand(domain.Attempt{
			RunID: prepared.RunID, ResidentID: prepared.ResidentID,
			AttemptNo: prepared.AttemptNo + 1, OutcomeID: ids[1],
		})); err != nil {
			t.Fatal(err)
		}
		err = fixture.application.cancelDialogueForErasedSource(context.Background(), prepared)
		if err == nil || !strings.Contains(err.Error(), "conflicts with latest attempt") {
			t.Fatalf("stale cancellation error = %v", err)
		}
		assertM7DialogueCancellationRows(t, fixture, prepared.RunID, 3, 0, 1)
	})

	t.Run("different terminal conflicts", func(t *testing.T) {
		fixture, prepared := prepareM7RunningDialogueForErasedCancellation(t)
		outcomeID, err := fixture.application.ids.New()
		if err != nil {
			t.Fatal(err)
		}
		if _, err := fixture.application.submit(context.Background(), domain.FailAttemptCommand(domain.FailAttempt{
			Attempt: domain.Attempt{RunID: prepared.RunID, ResidentID: prepared.ResidentID,
				AttemptNo: prepared.AttemptNo, OutcomeID: outcomeID},
			State: "failed", ErrorClass: generation.MustOutcomeErrorCode(generation.ErrorLandingFailure, 0).String(),
		})); err != nil {
			t.Fatal(err)
		}
		err = fixture.application.cancelDialogueForErasedSource(context.Background(), prepared)
		if err == nil || !strings.Contains(err.Error(), "conflicts with existing dialogue terminal state") {
			t.Fatalf("different-terminal cancellation error = %v", err)
		}
		assertM7DialogueCancellationRows(t, fixture, prepared.RunID, 2, 0, 0)
	})
}

type m7DialogueCancellationObserver struct {
	mu     sync.Mutex
	failed int
}

func (*m7DialogueCancellationObserver) UserCommitted(domain.Event)                          {}
func (*m7DialogueCancellationObserver) GenerationStarted(canonical.ID, canonical.ID, int64) {}
func (*m7DialogueCancellationObserver) GenerationDelta(canonical.ID, canonical.ID, string)  {}
func (*m7DialogueCancellationObserver) ResidentCommitted(domain.Event)                      {}
func (observer *m7DialogueCancellationObserver) GenerationFailed(
	canonical.ID,
	canonical.ID,
	int64,
	string,
	bool,
) {
	observer.mu.Lock()
	observer.failed++
	observer.mu.Unlock()
}

func prepareM7RunningDialogueForErasedCancellation(t *testing.T) (applicationFixture, domain.PreparedGeneration) {
	t.Helper()
	ctx := context.Background()
	fixture := newApplicationFixture(t, &scriptedGenerator{}, 3)
	event, err := fixture.application.Ingress(ctx, "claim-backed dialogue source")
	if err != nil {
		t.Fatal(err)
	}
	runID := dialogueRunIDForEventForTest(t, fixture, event)
	works, err := discoverDialogueWorkForTest(ctx, fixture.repository, fixture.residentID, 10, 3)
	if err != nil {
		t.Fatal(err)
	}
	if len(works) != 1 || works[0].RunID == nil || *works[0].RunID != runID ||
		works[0].AttemptNo != 1 || works[0].State != domain.WorkRunning {
		t.Fatalf("Commit B prepared dialogue work = %+v", works)
	}
	prepared, err := fixture.repository.Generation(ctx, *works[0].RunID)
	if err != nil {
		t.Fatal(err)
	}
	eraseEventContentForTest(t, fixture.store.Path(), event.ContentID)
	return fixture, prepared
}

func assertM7DialogueCancellationRows(
	t *testing.T,
	fixture applicationFixture,
	runID canonical.ID,
	wantOutcomes int,
	wantErased int,
	wantAttemptTwoRunning int,
) {
	t.Helper()
	var outcomes, erased, attemptTwoRunning int
	if err := fixture.store.Reader().QueryRow(`SELECT COUNT(*),
		SUM(CASE WHEN state = 'cancelled' AND error_class = 'source_content_erased' THEN 1 ELSE 0 END),
		SUM(CASE WHEN attempt_no = 2 AND state = 'running' THEN 1 ELSE 0 END)
		FROM generation_run_outcomes WHERE generation_run_id = ?`, runID.String()).Scan(
		&outcomes, &erased, &attemptTwoRunning,
	); err != nil {
		t.Fatal(err)
	}
	if outcomes != wantOutcomes || erased != wantErased || attemptTwoRunning != wantAttemptTwoRunning {
		t.Fatalf("outcomes/erased/attempt2-running = %d/%d/%d, want %d/%d/%d",
			outcomes, erased, attemptTwoRunning, wantOutcomes, wantErased, wantAttemptTwoRunning)
	}
}

func TestM7ErasedClaimLandingRacesCancelLikeDialogue(t *testing.T) {
	tests := []struct {
		name    string
		purpose domain.GenerationPurpose
		run     func(*testing.T, bool) (applicationFixture, canonical.ID, error)
	}{
		{name: "persona", purpose: domain.GenerationPurposePersonaRevision, run: runM7PersonaLandingRace},
		{name: "alignment", purpose: domain.GenerationPurposeMemoryAlignment, run: runM7AlignmentLandingRace},
		{name: "abstraction", purpose: domain.GenerationPurposeMemoryAbstraction, run: runM7DerivationLandingRace},
		{name: "differentiation", purpose: domain.GenerationPurposeMemoryDifferentiation, run: runM7DifferentiationLandingRace},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			fixture, runID, err := test.run(t, false)
			if !errors.Is(err, domain.ErrClaimSourceIneligible) {
				t.Fatalf("landing race error = %v, want ErrClaimSourceIneligible", err)
			}
			attempt := assertM7SourceErasedCancellation(t, fixture, runID, test.purpose)
			prepared := domain.PreparedGeneration{
				RunID: runID, ResidentID: fixture.residentID, AttemptNo: attempt,
			}
			code := generation.MustOutcomeErrorCode(generation.ErrorSourceContentErased, 0)
			var resendErr error
			if test.purpose == domain.GenerationPurposeMemoryAlignment {
				resendErr = fixture.application.recordMemoryAlignmentFailure(
					context.Background(), prepared, "cancelled", code,
				)
			} else {
				resendErr = fixture.application.recordPersonaFailure(
					context.Background(), prepared, "cancelled", code,
				)
			}
			if resendErr != nil {
				t.Fatalf("same-attempt erased-source cancellation retry = %v", resendErr)
			}
			assertM7SourceErasedCancellation(t, fixture, runID, test.purpose)

			conflictCode := generation.MustOutcomeErrorCode(generation.ErrorLandingFailure, 0)
			if test.purpose == domain.GenerationPurposeMemoryAlignment {
				resendErr = fixture.application.recordMemoryAlignmentFailure(
					context.Background(), prepared, "failed", conflictCode,
				)
			} else {
				resendErr = fixture.application.recordPersonaFailure(
					context.Background(), prepared, "failed", conflictCode,
				)
			}
			if resendErr == nil || errors.Is(resendErr, canonical.ErrNoMutation) {
				t.Fatalf("different terminal retry = %v, want conflict", resendErr)
			}
			assertM7SourceErasedCancellation(t, fixture, runID, test.purpose)
		})
	}
}

func TestM7ErasedClaimLandingRaceCancellationFailureJoinsAndLeavesRunning(t *testing.T) {
	tests := []struct {
		name    string
		purpose domain.GenerationPurpose
		run     func(*testing.T, bool) (applicationFixture, canonical.ID, error)
	}{
		{name: "persona", purpose: domain.GenerationPurposePersonaRevision, run: runM7PersonaLandingRace},
		{name: "alignment", purpose: domain.GenerationPurposeMemoryAlignment, run: runM7AlignmentLandingRace},
		{name: "abstraction", purpose: domain.GenerationPurposeMemoryAbstraction, run: runM7DerivationLandingRace},
		{name: "differentiation", purpose: domain.GenerationPurposeMemoryDifferentiation, run: runM7DifferentiationLandingRace},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			fixture, runID, err := test.run(t, true)
			if !errors.Is(err, domain.ErrClaimSourceIneligible) || !errors.Is(err, errM7CancelWriterInjected) {
				t.Fatalf("joined landing/cancellation error = %v", err)
			}
			assertM7RunningAfterCancellationFailure(t, fixture, runID, test.purpose)
		})
	}
}

func runM7PersonaLandingRace(t *testing.T, failCancellation bool) (applicationFixture, canonical.ID, error) {
	t.Helper()
	fixture, work := personaWorkForTerminalization(t, 2)
	var backend *m7ArmableBeginFailureBackend
	if failCancellation {
		backend = installM7ArmableBeginFailureBackend(t, fixture)
	}
	fixture.application.generator = &scriptedGenerator{steps: []generatorStep{{
		text: `{"contradiction":false,"persona":"friendly, careful, and grounded"}`,
		beforeReturn: func() {
			eraseM7ApplicationClaim(t, fixture, work.Claims[0].ClaimID)
			if backend != nil {
				backend.Arm()
			}
		},
	}}}
	_, err := fixture.application.processPersonaRevisionWork(context.Background(), work)
	runID := generationRunIDForKey(t, fixture, domain.PersonaRevisionObligation(work.TriggerStageTransitionID))
	return fixture, runID, err
}

func runM7DerivationLandingRace(t *testing.T, failCancellation bool) (applicationFixture, canonical.ID, error) {
	return runM7DerivedLandingRace(t, failCancellation, domain.GenerationPurposeMemoryAbstraction)
}

func runM7DifferentiationLandingRace(t *testing.T, failCancellation bool) (applicationFixture, canonical.ID, error) {
	return runM7DerivedLandingRace(t, failCancellation, domain.GenerationPurposeMemoryDifferentiation)
}

func runM7DerivedLandingRace(
	t *testing.T,
	failCancellation bool,
	purpose domain.GenerationPurpose,
) (applicationFixture, canonical.ID, error) {
	t.Helper()
	fixture, sources, _ := derivationSourcesForTerminalization(t, generatorStep{})
	// This fixture drives extraction work directly. The Admin derivation under
	// test starts only after the mandatory-memory completeness gate has been
	// established; scan-gate behavior is covered separately.
	fixture.application.setMemoryExtractionScansComplete(fixture.residentID, true)
	if purpose == domain.GenerationPurposeMemoryDifferentiation {
		sources = sources[:1]
	}
	var backend *m7ArmableBeginFailureBackend
	if failCancellation {
		backend = installM7ArmableBeginFailureBackend(t, fixture)
	}
	fixture.application.generator = &scriptedGenerator{steps: []generatorStep{{
		text: `{"statement":"The owner enjoys quiet indoor hobbies.","temporal_kind":"stable","version":"memory-derived-output-v1"}`,
		beforeReturn: func() {
			eraseM7ApplicationClaim(t, fixture, sources[0])
			if backend != nil {
				backend.Arm()
			}
		},
	}}}
	_, err := fixture.application.DeriveMemoryClaim(
		context.Background(), fixture.residentID, purpose, sources,
	)
	runID := latestGenerationRunForPurpose(t, fixture, purpose)
	return fixture, runID, err
}

func runM7AlignmentLandingRace(t *testing.T, failCancellation bool) (applicationFixture, canonical.ID, error) {
	t.Helper()
	ctx := context.Background()
	direct := `{"claims":[{"grade":"stated","perspective":"resident","source_quote":"I am calm","statement":"The resident is calm.","subject":"resident","temporal_kind":"stable"}],"version":"memory-extraction-output-v1"}`
	meta := `{"claims":[{"grade":"stated","perspective":"source_actor","source_quote":"I see calm","statement":"The resident appears calm to the owner.","subject":"resident","temporal_kind":"stable"}],"version":"memory-extraction-output-v1"}`
	var fixture applicationFixture
	var backend *m7ArmableBeginFailureBackend
	generator := &scriptedGenerator{steps: []generatorStep{
		{text: "dialogue 1"}, {text: direct},
		{text: "dialogue 2"}, {text: direct},
		{text: "dialogue 3"}, {text: direct},
		{text: "dialogue 4"}, {text: meta},
		{text: "dialogue 5"}, {text: meta},
		{
			text: `{"aligned":true,"confidence":"800000","version":"memory-alignment-output-v1"}`,
			beforeReturn: func() {
				eraseM7ApplicationClaim(t, fixture, m7ApplicationClaimForStatement(t, fixture, "The resident is calm."))
				if backend != nil {
					backend.Arm()
				}
			},
		},
	}}
	fixture = newApplicationFixture(t, generator, 2)
	if _, err := fixture.application.activateMemoryPolicyV4ForTest(ctx, fixture.residentID); err != nil {
		t.Fatal(err)
	}
	for index, message := range []string{"I am calm", "I am calm", "I am calm", "I see calm"} {
		if _, err := fixture.application.Ingress(ctx, message); err != nil {
			t.Fatal(err)
		}
		if err := fixture.application.ProcessResident(ctx, fixture.residentID); err != nil {
			t.Fatalf("alignment setup %d: %v", index, err)
		}
	}
	if failCancellation {
		backend = installM7ArmableBeginFailureBackend(t, fixture)
	}
	if _, err := fixture.application.Ingress(ctx, "I see calm"); err != nil {
		t.Fatal(err)
	}
	if err := fixture.application.ProcessResident(ctx, fixture.residentID); err != nil {
		t.Fatalf("final alignment source turn: %v", err)
	}
	// A dialogue turn yields after its one fair mandatory-memory quantum. The
	// bounded dialogue scan needs two clean turns before optional alignment is
	// admitted; the second call is deliberately returned because its landing is
	// the erased-source race under test.
	if err := fixture.application.ProcessResident(ctx, fixture.residentID); err != nil {
		t.Fatalf("clean scan before alignment: %v", err)
	}
	err := fixture.application.ProcessResident(ctx, fixture.residentID)
	runID := latestGenerationRunForPurpose(t, fixture, domain.GenerationPurposeMemoryAlignment)
	return fixture, runID, err
}

func eraseM7ApplicationClaim(t *testing.T, fixture applicationFixture, claimID canonical.ID) {
	t.Helper()
	resident, err := fixture.repository.Resident(context.Background(), fixture.residentID)
	if err != nil {
		t.Fatal(err)
	}
	ids, err := fixture.application.allocateIDs(2)
	if err != nil {
		t.Fatal(err)
	}
	now := canonical.InstantFromTime(fixture.clock.Now())
	_, err = fixture.application.submit(context.Background(), domain.EraseClaimStatementCommand(domain.EraseClaimStatement{
		ResidentID: fixture.residentID, ClaimID: claimID,
		ClaimStatementErasureEventID: ids[0], ContentErasureEventID: ids[1],
		ActorPrincipalID: resident.OwnerPrincipalID, ReasonCode: "m7_provider_landing_race",
		OccurredAt: now, OccurredTZ: canonical.MustTimezone("UTC"),
	}))
	if err != nil {
		t.Fatal(err)
	}
}

func m7ApplicationClaimForStatement(t *testing.T, fixture applicationFixture, statement string) canonical.ID {
	t.Helper()
	database := openApplicationDatabase(t, fixture.store.Path())
	defer database.Close()
	var raw string
	if err := database.QueryRow(`SELECT claim.claim_id FROM claims claim
		JOIN content_objects content ON content.content_id = claim.statement_content_id
		JOIN blobs blob ON blob.dedupe_scope_id = content.owner_resident_id
		 AND blob.hash_algorithm = content.blob_hash_algorithm AND blob.blob_hash = content.blob_hash
		WHERE claim.owner_resident_id = ? AND blob.content = ?`, fixture.residentID.String(), []byte(statement)).Scan(&raw); err != nil {
		t.Fatal(err)
	}
	id, err := canonical.ParseID(raw)
	if err != nil {
		t.Fatal(err)
	}
	return id
}

func assertM7SourceErasedCancellation(
	t *testing.T,
	fixture applicationFixture,
	runID canonical.ID,
	purpose domain.GenerationPurpose,
) int64 {
	t.Helper()
	database := openApplicationDatabase(t, fixture.store.Path())
	defer database.Close()
	var attempt int64
	var state, errorClass string
	if err := database.QueryRow(`SELECT outcome.attempt_no, outcome.state, COALESCE(outcome.error_class, '')
		FROM generation_run_outcomes outcome
		JOIN canonical_commits commit_row ON commit_row.canonical_commit_id = outcome.canonical_commit_id
		WHERE outcome.generation_run_id = ?
		ORDER BY commit_row.commit_seq DESC, outcome.outcome_id DESC LIMIT 1`, runID.String()).Scan(
		&attempt, &state, &errorClass,
	); err != nil {
		t.Fatal(err)
	}
	if state != "cancelled" || errorClass != generation.MustOutcomeErrorCode(generation.ErrorSourceContentErased, 0).String() {
		t.Fatalf("latest erased-source outcome = attempt %d %s/%s", attempt, state, errorClass)
	}
	var cancellations, succeeded int
	if err := database.QueryRow(`SELECT
		SUM(CASE WHEN state = 'cancelled' AND error_class = 'source_content_erased' THEN 1 ELSE 0 END),
		SUM(CASE WHEN state = 'succeeded' THEN 1 ELSE 0 END)
		FROM generation_run_outcomes WHERE generation_run_id = ?`, runID.String()).Scan(&cancellations, &succeeded); err != nil {
		t.Fatal(err)
	}
	if cancellations != 1 || succeeded != 0 {
		t.Fatalf("terminal rows cancellation/succeeded = %d/%d", cancellations, succeeded)
	}
	assertM7NoLandingArtifact(t, database, runID, purpose)
	return attempt
}

func assertM7RunningAfterCancellationFailure(
	t *testing.T,
	fixture applicationFixture,
	runID canonical.ID,
	purpose domain.GenerationPurpose,
) {
	t.Helper()
	database := openApplicationDatabase(t, fixture.store.Path())
	defer database.Close()
	var state string
	var terminal int
	if err := database.QueryRow(`SELECT outcome.state FROM generation_run_outcomes outcome
		JOIN canonical_commits commit_row ON commit_row.canonical_commit_id = outcome.canonical_commit_id
		WHERE outcome.generation_run_id = ?
		ORDER BY commit_row.commit_seq DESC, outcome.outcome_id DESC LIMIT 1`, runID.String()).Scan(&state); err != nil {
		t.Fatal(err)
	}
	if err := database.QueryRow(`SELECT COUNT(*) FROM generation_run_outcomes
		WHERE generation_run_id = ? AND state IN ('succeeded', 'failed', 'cancelled')`, runID.String()).Scan(&terminal); err != nil {
		t.Fatal(err)
	}
	if state != "running" || terminal != 0 {
		t.Fatalf("cancellation failure state=%s terminal rows=%d, want running/0", state, terminal)
	}
	assertM7NoLandingArtifact(t, database, runID, purpose)
}

type m7QueryRower interface {
	QueryRow(string, ...any) *sql.Row
}

func assertM7NoLandingArtifact(t *testing.T, database m7QueryRower, runID canonical.ID, purpose domain.GenerationPurpose) {
	t.Helper()
	query := ""
	switch purpose {
	case domain.GenerationPurposePersonaRevision:
		query = "SELECT COUNT(*) FROM resident_revisions WHERE created_by_run_id = ?"
	case domain.GenerationPurposeMemoryAlignment:
		query = "SELECT COUNT(*) FROM claim_stage_transitions WHERE generation_run_id = ? AND to_stage = 'settled'"
	case domain.GenerationPurposeMemoryAbstraction, domain.GenerationPurposeMemoryDifferentiation:
		query = "SELECT COUNT(*) FROM claims WHERE created_by_run_id = ?"
	default:
		t.Fatalf("unsupported M7 landing purpose %q", purpose)
	}
	var count int
	if err := database.QueryRow(query, runID.String()).Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 0 {
		t.Fatalf("%s landing artifacts = %d, want 0", purpose, count)
	}
}

type m7ArmableBeginFailureBackend struct {
	delegate canonical.Backend

	mu         sync.Mutex
	armed      bool
	failNext   bool
	afterArmed int
}

func (backend *m7ArmableBeginFailureBackend) LoadHead(ctx context.Context) (canonical.Head, error) {
	return backend.delegate.LoadHead(ctx)
}

func (backend *m7ArmableBeginFailureBackend) Begin(
	ctx context.Context,
	metadata canonical.CommitMetadata,
) (canonical.CanonicalUoW, error) {
	backend.mu.Lock()
	if backend.failNext {
		backend.failNext = false
		backend.mu.Unlock()
		return nil, errM7CancelWriterInjected
	}
	if backend.armed {
		backend.afterArmed++
		if backend.afterArmed == 2 {
			backend.mu.Unlock()
			return nil, errM7CancelWriterInjected
		}
	}
	backend.mu.Unlock()
	return backend.delegate.Begin(ctx, metadata)
}

func (backend *m7ArmableBeginFailureBackend) Arm() {
	backend.mu.Lock()
	backend.armed = true
	backend.afterArmed = 0
	backend.mu.Unlock()
}

func (backend *m7ArmableBeginFailureBackend) ArmNext() {
	backend.mu.Lock()
	backend.failNext = true
	backend.mu.Unlock()
}

func installM7ArmableBeginFailureBackend(t *testing.T, fixture applicationFixture) *m7ArmableBeginFailureBackend {
	t.Helper()
	ctx := context.Background()
	if err := fixture.application.writer.Close(ctx); err != nil {
		t.Fatal(err)
	}
	backend := &m7ArmableBeginFailureBackend{delegate: fixture.store.Canonical()}
	writer, err := canonical.OpenWriter(ctx, canonical.WriterOptions{
		Backend: backend, IDs: fixture.application.ids, Clock: fixture.application.clock,
		Timezone: fixture.application.timezone, QueueCapacity: 32,
	})
	if err != nil {
		t.Fatal(err)
	}
	fixture.application.writer = writer
	t.Cleanup(func() {
		if err := writer.Close(context.Background()); err != nil {
			t.Errorf("close M7 fault writer: %v", err)
		}
	})
	return backend
}
