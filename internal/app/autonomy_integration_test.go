package app

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"mahoroba.local/mahoroba/internal/autonomy"
	"mahoroba.local/mahoroba/internal/canonical"
	"mahoroba.local/mahoroba/internal/domain"
	"mahoroba.local/mahoroba/internal/generation"
	"mahoroba.local/mahoroba/internal/memory"
	"mahoroba.local/mahoroba/internal/projection"
	"mahoroba.local/mahoroba/internal/store/sqlite"
	"mahoroba.local/mahoroba/internal/testsupport"
)

const emptyM6Extraction = `{"claims":[],"version":"memory-extraction-output-v1"}`

func TestM6I44AutonomyEventPurposeLandsExactlyOneEvent(t *testing.T) {
	fixture := landedSelfTalkFixture(t)
	var runs, events int
	if err := fixture.store.Reader().QueryRow(`SELECT COUNT(*) FROM generation_runs WHERE resident_id = ? AND purpose = 'self_talk'`,
		fixture.residentID.String()).Scan(&runs); err != nil {
		t.Fatal(err)
	}
	if err := fixture.store.Reader().QueryRow(`SELECT COUNT(*) FROM events WHERE resident_id = ? AND event_type = 'self_talk'`,
		fixture.residentID.String()).Scan(&events); err != nil {
		t.Fatal(err)
	}
	if runs != 1 || events != 1 {
		t.Fatalf("self-talk runs/events = %d/%d, want 1/1", runs, events)
	}
}

func TestM6RTI9AutonomyEventAndSucceededOutcomeLandAtomically(t *testing.T) {
	fixture := landedSelfTalkFixture(t)
	var eventCommit, outcomeCommit string
	err := fixture.store.Reader().QueryRow(`SELECT event.canonical_commit_id, outcome.canonical_commit_id
		FROM events event
		JOIN generation_runs run ON run.generation_run_id = event.generation_run_id
		JOIN generation_run_outcomes outcome ON outcome.generation_run_id = run.generation_run_id
		WHERE event.resident_id = ? AND event.event_type = 'self_talk' AND outcome.state = 'succeeded'`,
		fixture.residentID.String()).Scan(&eventCommit, &outcomeCommit)
	if err != nil {
		t.Fatal(err)
	}
	if eventCommit != outcomeCommit {
		t.Fatalf("self-talk event commit %s != succeeded outcome commit %s", eventCommit, outcomeCommit)
	}
}

func TestM6I34SelfTalkNeverEntersDialogueLiveContext(t *testing.T) {
	fixture := landedSelfTalkFixture(t)
	event, err := fixture.application.Ingress(context.Background(), "after private thought")
	if err != nil {
		t.Fatal(err)
	}
	// Commit A no longer contains frozen dialogue inputs. Explicitly execute
	// Commit B so this test observes the current context-v2 Assembly result.
	_ = dialogueRunIDForEventForTest(t, fixture, event)
	var leaked int
	err = fixture.store.Reader().QueryRow(`SELECT COUNT(*)
		FROM generation_run_inputs input
		JOIN generation_runs run ON run.generation_run_id = input.generation_run_id
		JOIN events event ON event.event_id = input.source_id
		WHERE run.resident_id = ? AND run.purpose = 'dialogue'
		 AND input.inclusion_mode IN ('live_context','context_backfill') AND event.event_type = 'self_talk'`,
		fixture.residentID.String()).Scan(&leaked)
	if err != nil {
		t.Fatal(err)
	}
	if leaked != 0 {
		t.Fatalf("self-talk dialogue inputs = %d, want 0", leaked)
	}
	var selfTalkRaw string
	if err := fixture.store.Reader().QueryRow(`SELECT event_id FROM events WHERE resident_id = ? AND event_type = 'self_talk'`,
		fixture.residentID.String()).Scan(&selfTalkRaw); err != nil {
		t.Fatal(err)
	}
	selfTalkID, err := canonical.ParseID(selfTalkRaw)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := fixture.application.IngressWithMetadata(context.Background(), struct {
		RawText          string
		ExplicitEventIDs []canonical.ID
	}{RawText: "try explicit private thought", ExplicitEventIDs: []canonical.ID{selfTalkID}}); err == nil ||
		!errors.Is(err, domain.ErrInvalidEventReference) {
		t.Fatalf("explicit self-talk error = %v", err)
	}
}

func TestM6AutonomyMemoryPolicyActivationIsNonRetroactive(t *testing.T) {
	ctx := context.Background()
	generator := &scriptedGenerator{steps: []generatorStep{
		{text: "legacy reply"}, {text: "enabled reply"}, {text: emptyM6Extraction},
	}}
	fixture := newApplicationFixture(t, generator, 2)
	legacyEvent, err := fixture.application.Ingress(ctx, "before autonomy activation")
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
		t.Fatalf("autonomy activation retroactively scheduled provider calls=%d", generator.CallCount())
	}
	enabledEvent, err := fixture.application.Ingress(ctx, "after autonomy activation")
	if err != nil {
		t.Fatal(err)
	}
	if err := fixture.application.ProcessResident(ctx, fixture.residentID); err != nil {
		t.Fatal(err)
	}
	var key string
	if err := fixture.store.Reader().QueryRow(`SELECT idempotency_key FROM generation_runs
		WHERE resident_id = ? AND purpose = 'memory_extraction'`, fixture.residentID.String()).Scan(&key); err != nil {
		t.Fatal(err)
	}
	if key != domain.MemoryExtractionObligation(enabledEvent.ID) || key == domain.MemoryExtractionObligation(legacyEvent.ID) {
		t.Fatalf("autonomy memory obligation key=%q", key)
	}
}

func TestM6SelfTalkIdleUsesEventTimeMemoryPolicy(t *testing.T) {
	ctx := context.Background()
	generator := &scriptedGenerator{steps: []generatorStep{
		{text: "legacy dialogue"}, {text: "new dialogue"}, {text: emptyM6Extraction}, {text: "new private thought"},
	}}
	fixture := newApplicationFixture(t, generator, 1)
	legacyEvent, err := fixture.application.Ingress(ctx, "before self-talk activation")
	if err != nil {
		t.Fatal(err)
	}
	for turn := 0; turn < 2; turn++ {
		if err := fixture.application.ProcessResident(ctx, fixture.residentID); err != nil {
			t.Fatal(err)
		}
	}
	legacyProviderCalls := generator.CallCount()
	enableSelfTalkForTest(t, fixture)
	quietStart, _ := autonomy.ParseLocalTime("02:00")
	quietEnd, _ := autonomy.ParseLocalTime("03:00")
	fixture.application.autonomyPolicy.SelfTalk.QuietHours = autonomy.QuietHours{Start: quietStart, End: quietEnd}
	legacyTrigger := autonomy.Trigger{Kind: autonomy.TriggerIdle, SourceID: legacyEvent.ID, Ordinal: 1}
	eligible, err := fixture.repository.AutonomousSelfTalkEventTimeEligible(ctx, fixture.residentID, legacyTrigger)
	if err != nil {
		t.Fatal(err)
	}
	if eligible {
		t.Fatal("pre-v3 user event became eligible for self-talk")
	}
	advanceSelfTalkClock(fixture)
	if err := fixture.application.processAutonomyOnce(ctx, fixture.residentID); err != nil {
		t.Fatal(err)
	}
	var runs int
	if err := fixture.store.Reader().QueryRowContext(ctx, `SELECT COUNT(*) FROM generation_runs
		WHERE resident_id = ? AND purpose = 'self_talk'`, fixture.residentID.String()).Scan(&runs); err != nil {
		t.Fatal(err)
	}
	if runs != 0 || generator.CallCount() != legacyProviderCalls {
		t.Fatalf("legacy self-talk runs/provider calls = %d/%d, want 0/%d", runs, generator.CallCount(), legacyProviderCalls)
	}

	newEvent, err := fixture.application.Ingress(ctx, "after self-talk activation")
	if err != nil {
		t.Fatal(err)
	}
	if err := fixture.application.ProcessResident(ctx, fixture.residentID); err != nil {
		t.Fatal(err)
	}
	if err := fixture.application.ProcessResident(ctx, fixture.residentID); err != nil {
		t.Fatal(err)
	}
	postV3ProviderCalls := generator.CallCount()
	newTrigger := autonomy.Trigger{Kind: autonomy.TriggerIdle, SourceID: newEvent.ID, Ordinal: 1}
	eligible, err = fixture.repository.AutonomousSelfTalkEventTimeEligible(ctx, fixture.residentID, newTrigger)
	if err != nil {
		t.Fatal(err)
	}
	if !eligible {
		t.Fatal("post-v3 user event was not eligible for self-talk")
	}
	advanceSelfTalkClock(fixture)
	if err := fixture.application.processAutonomyOnce(ctx, fixture.residentID); err != nil {
		t.Fatal(err)
	}
	if generator.CallCount() != postV3ProviderCalls+1 {
		t.Fatalf("post-v3 self-talk provider calls = %d, want %d", generator.CallCount(), postV3ProviderCalls+1)
	}
	if err := fixture.store.Reader().QueryRowContext(ctx, `SELECT COUNT(*) FROM events
		WHERE resident_id = ? AND event_type = 'self_talk'`, fixture.residentID.String()).Scan(&runs); err != nil {
		t.Fatal(err)
	}
	if runs != 1 {
		t.Fatalf("post-v3 self-talk events = %d, want 1", runs)
	}
}

func TestM6OptionalAutonomyRunsOnlyForSelectedResident(t *testing.T) {
	ctx := context.Background()
	generator := &scriptedGenerator{}
	fixture := newApplicationFixture(t, generator, 1)
	enableSelfTalkForTest(t, fixture)
	state, err := fixture.application.BootstrapInit(ctx, BootstrapInput{
		OwnerName: "Owner", Name: "Second", SeedKey: "resident-2", Principles: "be helpful",
	})
	if err != nil {
		t.Fatal(err)
	}
	var second canonical.ID
	for _, resident := range state.Residents {
		if resident.SeedKey == "resident-2" {
			second = resident.ResidentID
		}
	}
	if second.IsZero() {
		t.Fatal("second resident was not created")
	}
	if err := fixture.application.ApprovePrinciples(ctx, second); err != nil {
		t.Fatal(err)
	}
	if err := fixture.application.FinalizeBootstrap(ctx, second, `friendly`,
		`{"mandatory_event_types":[],"memory_recall_enabled":false,"version":"memory-policy-v1"}`); err != nil {
		t.Fatal(err)
	}
	if err := fixture.application.SelectResident(ctx, second); err != nil {
		t.Fatal(err)
	}
	if _, err := fixture.application.activateMemoryPolicyV4ForTest(ctx, second); err != nil {
		t.Fatal(err)
	}
	if _, err := fixture.application.Ingress(ctx, "unselected resident activity"); err != nil {
		t.Fatal(err)
	}
	if err := fixture.application.SelectResident(ctx, fixture.residentID); err != nil {
		t.Fatal(err)
	}
	advanceSelfTalkClock(fixture)
	if err := fixture.application.processAutonomyResident(ctx, second); err != nil {
		t.Fatal(err)
	}
	var runs int
	if err := fixture.store.Reader().QueryRowContext(ctx, `SELECT COUNT(*) FROM generation_runs
		WHERE resident_id = ? AND purpose IN ('self_talk','outbound_initiative')`, second.String()).Scan(&runs); err != nil {
		t.Fatal(err)
	}
	if runs != 0 || generator.CallCount() != 0 {
		t.Fatalf("unselected optional runs/provider calls = %d/%d, want 0/0", runs, generator.CallCount())
	}
}

func TestM6SelfTalkMandatoryExtractionForcesInferredAdminOnly(t *testing.T) {
	ctx := context.Background()
	extraction := `{"claims":[{"grade":"stated","perspective":"resident","source_quote":"private thought","statement":"The resident has a private thought.","subject":"resident","temporal_kind":"stable"}],"version":"memory-extraction-output-v1"}`
	generator := &scriptedGenerator{steps: []generatorStep{
		{text: "dialogue reply"}, {text: emptyM6Extraction}, {text: "private thought"}, {text: extraction},
	}}
	fixture := newApplicationFixture(t, generator, 3)
	enableSelfTalkForTest(t, fixture)
	if _, err := fixture.application.Ingress(ctx, "seed private extraction"); err != nil {
		t.Fatal(err)
	}
	if err := fixture.application.ProcessResident(ctx, fixture.residentID); err != nil {
		t.Fatal(err)
	}
	if err := fixture.application.ProcessResident(ctx, fixture.residentID); err != nil {
		t.Fatal(err)
	}
	advanceSelfTalkClock(fixture)
	if err := fixture.application.processAutonomyResident(ctx, fixture.residentID); err != nil {
		t.Fatal(err)
	}
	if err := fixture.application.ProcessResident(ctx, fixture.residentID); err != nil {
		t.Fatal(err)
	}
	var grade, reason, scope string
	var weight int64
	if err := fixture.store.Reader().QueryRow(`SELECT evidence.grade, evidence.reason_code,
		evidence.weight, scope.view_scope
		FROM events event
		JOIN claim_evidence evidence ON evidence.event_id = event.event_id
		JOIN claim_view_scope_assertions scope ON scope.claim_id = evidence.claim_id
		WHERE event.resident_id = ? AND event.event_type = 'self_talk'`, fixture.residentID.String()).Scan(
		&grade, &reason, &weight, &scope,
	); err != nil {
		t.Fatal(err)
	}
	if grade != "inferred" || reason != "source_inferred" || weight != 600_000 || scope != "admin_only" {
		t.Fatalf("self-talk evidence grade/reason/weight/scope=%s/%s/%d/%s", grade, reason, weight, scope)
	}
}

type recordingErasureCandidateSink struct {
	candidates []autonomy.RetentionCandidate
}

func (sink *recordingErasureCandidateSink) NotifyErasureCandidate(candidate autonomy.RetentionCandidate) bool {
	sink.candidates = append(sink.candidates, candidate)
	return true
}

func TestM6RTI24RetentionProducesEraseCandidateWithoutDeletingContent(t *testing.T) {
	ctx := context.Background()
	fixture := landedSelfTalkFixture(t)
	policy := *fixture.application.autonomyPolicy
	policy.Retention.Mode = autonomy.RetentionCandidateAfter
	fixture.application.autonomyPolicy = &policy
	schedulerClock := fixture.application.autonomyClock.(*testsupport.ManualSchedulerClock)
	advance := policy.Retention.Duration + time.Second
	fixture.clock.Advance(advance)
	schedulerClock.AdvanceWall(advance)
	schedulerClock.AdvanceMonotonic(advance)

	counts := func() [3]int {
		t.Helper()
		var result [3]int
		for index, query := range []string{
			`SELECT COUNT(*) FROM content_objects`,
			`SELECT COUNT(*) FROM blobs`,
			`SELECT COUNT(*) FROM content_erasure_events`,
		} {
			if err := fixture.store.Reader().QueryRow(query).Scan(&result[index]); err != nil {
				t.Fatal(err)
			}
		}
		return result
	}
	before := counts()
	sink := &recordingErasureCandidateSink{}
	fixture.application.erasureCandidateSink = sink
	fixture.application.scanRetentionCandidates(ctx)
	if len(sink.candidates) != 1 || sink.candidates[0].ResidentID != fixture.residentID {
		t.Fatalf("retention candidates=%#v", sink.candidates)
	}
	if got := sink.candidates[0].BlockingReasons; len(got) != 1 || got[0] != autonomy.ImpactGenerationOutputReference {
		t.Fatalf("retention blocking reasons=%v", got)
	}
	if after := counts(); after != before {
		t.Fatalf("retention scan changed content/blob/erasure rows: before=%v after=%v", before, after)
	}
	var state string
	if err := fixture.store.Reader().QueryRow(`SELECT content.erasure_state
		FROM events event JOIN content_objects content ON content.content_id = event.content_id
		WHERE event.resident_id = ? AND event.event_type = 'self_talk'`, fixture.residentID.String()).Scan(&state); err != nil {
		t.Fatal(err)
	}
	if state != canonical.ContentErasurePresent {
		t.Fatalf("self-talk content state=%q", state)
	}
}

func TestM6RTI7AutonomyRetryPreservesFrozenEnvelope(t *testing.T) {
	generator := &scriptedGenerator{steps: []generatorStep{
		{text: "dialogue reply"}, {text: emptyM6Extraction},
		{err: &generation.ProviderError{Class: generation.ErrorTransport, Detail: "retry self-talk"}},
		{text: "retry private thought"},
	}}
	fixture := newApplicationFixture(t, generator, 3)
	enableSelfTalkForTest(t, fixture)
	if _, err := fixture.application.Ingress(context.Background(), "seed retry activity"); err != nil {
		t.Fatal(err)
	}
	if err := fixture.application.ProcessResident(context.Background(), fixture.residentID); err != nil {
		t.Fatal(err)
	}
	// A resident turn yields immediately after its single fair mandatory-memory
	// quantum. A second turn proves the normal memory scans clean and admits the
	// optional persona/autonomy tail.
	if err := fixture.application.ProcessResident(context.Background(), fixture.residentID); err != nil {
		t.Fatal(err)
	}
	advanceSelfTalkClock(fixture)
	if err := fixture.application.processAutonomyResident(context.Background(), fixture.residentID); err != nil {
		t.Fatal(err)
	}
	changedPolicy := *fixture.application.autonomyPolicy
	changedPolicy.SelfTalk.Interval = 24 * time.Hour
	changedPolicy.SelfTalk.HourlyLimit = 1
	fixture.application.autonomyPolicy = &changedPolicy
	fixture.application.model = "changed-config-model"
	fixture.application.projectionMaxStaleness = 37 * time.Second
	if err := fixture.application.processAutonomyResident(context.Background(), fixture.residentID); err != nil {
		t.Fatal(err)
	}
	requests := generator.Requests()
	if len(requests) != 4 {
		t.Fatalf("provider requests = %d, want 4", len(requests))
	}
	first, retry := requests[2], requests[3]
	if first.GenerationRunID != retry.GenerationRunID || first.Model != "test-model" || retry.Model != first.Model ||
		!reflect.DeepEqual(first.Messages, retry.Messages) || first.Streaming || retry.Streaming {
		t.Fatalf("autonomous retry envelope changed: first=%+v retry=%+v", first, retry)
	}
	var events, succeeded int
	if err := fixture.store.Reader().QueryRow(`SELECT COUNT(*) FROM events event
		JOIN generation_runs run ON run.generation_run_id = event.generation_run_id
		WHERE run.generation_run_id = ? AND event.event_type = 'self_talk'`, first.GenerationRunID).Scan(&events); err != nil {
		t.Fatal(err)
	}
	if err := fixture.store.Reader().QueryRow(`SELECT COUNT(*) FROM generation_run_outcomes outcome
		JOIN generation_runs run ON run.generation_run_id = outcome.generation_run_id
		WHERE run.generation_run_id = ? AND outcome.state = 'succeeded'`, first.GenerationRunID).Scan(&succeeded); err != nil {
		t.Fatal(err)
	}
	if events != 1 || succeeded != 1 {
		t.Fatalf("frozen retry landing event/succeeded = %d/%d, want 1/1", events, succeeded)
	}
}

func TestM6AutonomousRunCannotLandThroughGenericDialogueWriter(t *testing.T) {
	fixture, assembly, trigger := prepareRunningSelfTalkFixture(t)
	ctx := context.Background()
	resident, err := fixture.repository.Resident(ctx, fixture.residentID)
	if err != nil {
		t.Fatal(err)
	}
	output, err := fixture.application.newContent(
		fixture.residentID, "generation_output", []byte("must remain private"), "independent",
	)
	if err != nil {
		t.Fatal(err)
	}
	ids, err := fixture.application.allocateIDs(2)
	if err != nil {
		t.Fatal(err)
	}
	_, err = fixture.application.submit(ctx, domain.LandDialogueCommand(domain.LandDialogue{
		Attempt: domain.Attempt{RunID: assembly.Prepare.Generation.RunID, ResidentID: fixture.residentID,
			AttemptNo: 1, OutcomeID: ids[0]},
		EventID: ids[1], ResidentID: fixture.residentID, ResidentPrincipalID: resident.ResidentPrincipalID,
		OwnerPrincipalID: resident.OwnerPrincipalID, Output: output,
		OccurredAt: canonical.InstantFromTime(fixture.clock.Now()), OccurredTZ: canonical.MustTimezone("UTC"),
	}))
	if err == nil || !strings.Contains(err.Error(), "rejects non-dialogue") {
		t.Fatalf("generic landing error = %v", err)
	}
	var publicEvents, succeeded int
	if err := fixture.store.Reader().QueryRow(`SELECT COUNT(*) FROM events
		WHERE resident_id = ? AND event_type = 'resident_message' AND generation_run_id = ?`,
		fixture.residentID.String(), assembly.Prepare.Generation.RunID.String()).Scan(&publicEvents); err != nil {
		t.Fatal(err)
	}
	if err := fixture.store.Reader().QueryRow(`SELECT COUNT(*) FROM generation_run_outcomes
		WHERE generation_run_id = ? AND state = 'succeeded'`, assembly.Prepare.Generation.RunID.String()).Scan(&succeeded); err != nil {
		t.Fatal(err)
	}
	if publicEvents != 0 || succeeded != 0 {
		t.Fatalf("bypass rows public/succeeded = %d/%d for trigger %+v", publicEvents, succeeded, trigger)
	}
}

func TestM6LandAutonomousEventRejectsWrongOwnerHuman(t *testing.T) {
	fixture, assembly, trigger := prepareRunningSelfTalkFixture(t)
	ctx := context.Background()
	resident, err := fixture.repository.Resident(ctx, fixture.residentID)
	if err != nil {
		t.Fatal(err)
	}
	nonOwnerID, err := fixture.application.ids.New()
	if err != nil {
		t.Fatal(err)
	}
	var bootstrapCommit string
	if err := fixture.store.Reader().QueryRow(`SELECT canonical_commit_id FROM principals
		WHERE principal_id = ?`, resident.OwnerPrincipalID.String()).Scan(&bootstrapCommit); err != nil {
		t.Fatal(err)
	}
	writable, err := sql.Open("sqlite", fixture.store.Path())
	if err != nil {
		t.Fatal(err)
	}
	defer writable.Close()
	if _, err := writable.Exec(`INSERT INTO principals(
		principal_id, canonical_commit_id, kind, display_name, created_at, created_tz
	) VALUES (?, ?, 'human', 'Other Resident Owner', ?, 'UTC')`, nonOwnerID.String(), bootstrapCommit,
		fixture.clock.Now().UnixMicro()); err != nil {
		t.Fatal(err)
	}
	output, err := fixture.application.newContent(
		fixture.residentID, "generation_output", []byte("wrong target"), "independent",
	)
	if err != nil {
		t.Fatal(err)
	}
	ids, err := fixture.application.allocateIDs(2)
	if err != nil {
		t.Fatal(err)
	}
	_, err = fixture.application.submit(ctx, domain.LandAutonomousEventCommand(domain.LandAutonomousEvent{
		Attempt: domain.Attempt{RunID: assembly.Prepare.Generation.RunID, ResidentID: fixture.residentID,
			AttemptNo: 1, OutcomeID: ids[0], MaxAttempts: fixture.application.maxAttempts},
		EventID: ids[1], ResidentPrincipalID: resident.ResidentPrincipalID, OwnerPrincipalID: nonOwnerID,
		Output: output, OccurredAt: canonical.InstantFromTime(fixture.clock.Now()), OccurredTZ: canonical.MustTimezone("UTC"),
		Trigger: trigger, Policy: assembly.Prepare.Policy,
		ProjectionMaxStaleness: assembly.Prepare.ProjectionMaxStaleness,
		ProjectionEvidence:     assembly.Prepare.ProjectionEvidence,
	}))
	if err == nil || !strings.Contains(err.Error(), "owner human principal required") {
		t.Fatalf("wrong-owner landing error = %v", err)
	}
	var events, succeeded int
	if err := fixture.store.Reader().QueryRow(`SELECT COUNT(*) FROM events WHERE generation_run_id = ?`,
		assembly.Prepare.Generation.RunID.String()).Scan(&events); err != nil {
		t.Fatal(err)
	}
	if err := fixture.store.Reader().QueryRow(`SELECT COUNT(*) FROM generation_run_outcomes
		WHERE generation_run_id = ? AND state = 'succeeded'`, assembly.Prepare.Generation.RunID.String()).Scan(&succeeded); err != nil {
		t.Fatal(err)
	}
	if events != 0 || succeeded != 0 {
		t.Fatalf("wrong-owner rollback event/succeeded = %d/%d", events, succeeded)
	}
}

func TestM6AutonomousRunningCrashRecoveryResumesFrozenRun(t *testing.T) {
	fixture, assembly, _ := prepareRunningSelfTalkFixture(t)
	ctx := context.Background()
	if err := fixture.application.Recover(ctx); err != nil {
		t.Fatal(err)
	}
	var state, failure string
	if err := fixture.store.Reader().QueryRow(`SELECT state, error_class FROM generation_run_outcomes
		WHERE generation_run_id = ? ORDER BY outcome_id DESC LIMIT 1`, assembly.Prepare.Generation.RunID.String()).Scan(
		&state, &failure,
	); err != nil {
		t.Fatal(err)
	}
	if state != "cancelled" || failure != generation.RuntimeInterruptedErrorCode().String() {
		t.Fatalf("recovered autonomous outcome = %s/%s", state, failure)
	}
	retryGenerator := &scriptedGenerator{steps: []generatorStep{{text: "resumed private thought"}}}
	fixture.application.generator = retryGenerator
	if err := fixture.application.processAutonomyResident(ctx, fixture.residentID); err != nil {
		t.Fatal(err)
	}
	var attempts, events int
	if err := fixture.store.Reader().QueryRow(`SELECT MAX(attempt_no) FROM generation_run_outcomes
		WHERE generation_run_id = ?`, assembly.Prepare.Generation.RunID.String()).Scan(&attempts); err != nil {
		t.Fatal(err)
	}
	if err := fixture.store.Reader().QueryRow(`SELECT COUNT(*) FROM events
		WHERE generation_run_id = ? AND event_type = 'self_talk'`, assembly.Prepare.Generation.RunID.String()).Scan(&events); err != nil {
		t.Fatal(err)
	}
	if attempts != 2 || events != 1 || retryGenerator.CallCount() != 1 {
		t.Fatalf("crash recovery attempts/events/provider = %d/%d/%d", attempts, events, retryGenerator.CallCount())
	}
}

type autonomyPreemptionGenerator struct {
	mu              sync.Mutex
	selfTalkCalls   int
	selfTalkStarted chan struct{}
}

func (generator *autonomyPreemptionGenerator) Stream(
	ctx context.Context,
	request generation.Request,
	_ generation.DeltaSink,
) (generation.Result, error) {
	switch request.Purpose {
	case string(domain.GenerationPurposeDialogue):
		return generation.Result{Text: "foreground reply"}, nil
	case string(domain.GenerationPurposeMemoryExtraction):
		return generation.Result{Text: emptyM6Extraction}, nil
	case string(domain.GenerationPurposeSelfTalk):
		generator.mu.Lock()
		generator.selfTalkCalls++
		call := generator.selfTalkCalls
		generator.mu.Unlock()
		if call == 1 {
			close(generator.selfTalkStarted)
			<-ctx.Done()
			return generation.Result{}, ctx.Err()
		}
		return generation.Result{Text: "resumed after foreground"}, nil
	default:
		return generation.Result{}, errors.New("unexpected autonomy preemption purpose")
	}
}

func TestM6AutonomyForegroundPreemptionYieldsAndDoesNotConsumeRetryBudget(t *testing.T) {
	ctx := context.Background()
	generator := &autonomyPreemptionGenerator{selfTalkStarted: make(chan struct{})}
	// maxAttempts=1 makes retry-budget accounting observable: a preemption may
	// append attempt 2 but cannot make the frozen run terminal.
	fixture := newApplicationFixture(t, generator, 1)
	enableSelfTalkForTest(t, fixture)
	quietStart, _ := autonomy.ParseLocalTime("02:00")
	quietEnd, _ := autonomy.ParseLocalTime("03:00")
	fixture.application.autonomyPolicy.SelfTalk.QuietHours = autonomy.QuietHours{Start: quietStart, End: quietEnd}
	if _, err := fixture.application.Ingress(ctx, "first foreground"); err != nil {
		t.Fatal(err)
	}
	if err := fixture.application.ProcessResident(ctx, fixture.residentID); err != nil {
		t.Fatal(err)
	}
	if err := fixture.application.ProcessResident(ctx, fixture.residentID); err != nil {
		t.Fatal(err)
	}
	advanceSelfTalkClock(fixture)
	backgroundDone := make(chan error, 1)
	go func() {
		backgroundDone <- fixture.application.processAutonomyResident(ctx, fixture.residentID)
	}()
	select {
	case <-generator.selfTalkStarted:
	case <-time.After(5 * time.Second):
		t.Fatal("autonomous provider call did not start")
	}
	if _, err := fixture.application.Ingress(ctx, "second foreground"); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-backgroundDone:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("preempted autonomous call did not yield the resident lock")
	}
	var runRaw, latestState, latestError string
	if err := fixture.store.Reader().QueryRow(`SELECT run.generation_run_id, outcome.state, outcome.error_class
		FROM generation_runs run JOIN generation_run_outcomes outcome ON outcome.generation_run_id = run.generation_run_id
		WHERE run.resident_id = ? AND run.purpose = 'self_talk'
		ORDER BY outcome.outcome_id DESC LIMIT 1`, fixture.residentID.String()).Scan(
		&runRaw, &latestState, &latestError,
	); err != nil {
		t.Fatal(err)
	}
	if latestState != "cancelled" || latestError != generation.MustOutcomeErrorCode(
		generation.ErrorForegroundPreempted, 0,
	).String() {
		t.Fatalf("preempted outcome = %s/%s", latestState, latestError)
	}
	if err := fixture.application.ProcessResident(ctx, fixture.residentID); err != nil {
		t.Fatal(err)
	}
	if err := fixture.application.ProcessResident(ctx, fixture.residentID); err != nil {
		t.Fatal(err)
	}
	advanceSelfTalkClock(fixture)
	if err := fixture.application.processAutonomyResident(ctx, fixture.residentID); err != nil {
		t.Fatal(err)
	}
	var attempts, retryFailures, events int
	if err := fixture.store.Reader().QueryRow(`SELECT MAX(attempt_no),
		SUM(CASE WHEN state IN ('failed','cancelled') AND error_class <> ? THEN 1 ELSE 0 END)
		FROM generation_run_outcomes WHERE generation_run_id = ?`, generation.MustOutcomeErrorCode(
		generation.ErrorForegroundPreempted, 0,
	).String(), runRaw).Scan(&attempts, &retryFailures); err != nil {
		t.Fatal(err)
	}
	if err := fixture.store.Reader().QueryRow(`SELECT COUNT(*) FROM events
		WHERE generation_run_id = ? AND event_type = 'self_talk'`, runRaw).Scan(&events); err != nil {
		t.Fatal(err)
	}
	if attempts != 2 || retryFailures != 0 || events != 1 || generator.selfTalkCalls != 2 {
		t.Fatalf("preemption retry attempts/failures/events/calls = %d/%d/%d/%d", attempts, retryFailures, events, generator.selfTalkCalls)
	}
}

func TestM6ForegroundEpochRejectsLateAutonomousProviderRegistration(t *testing.T) {
	residentID, err := canonical.ParseID("01HF7YAT00A9954MJJA9954M80")
	if err != nil {
		t.Fatal(err)
	}
	application := &Application{
		background:      make(map[canonical.ID]*backgroundCall),
		foregroundEpoch: make(map[canonical.ID]uint64),
	}
	epoch := application.captureForegroundEpoch(residentID)
	application.preemptBackground(residentID)
	ctx, finish, registered := application.beginBackgroundCallAtEpoch(
		context.Background(), residentID, epoch, true,
	)
	defer finish()
	if registered || !errors.Is(context.Cause(ctx), errForegroundPreempted) {
		t.Fatalf("late registration = registered:%v cause:%v", registered, context.Cause(ctx))
	}
}

type cancellationIgnoringAutonomyGenerator struct {
	selfTalkStarted chan struct{}
	releaseSelfTalk chan struct{}
	selfTalkDone    chan struct{}
}

func (generator *cancellationIgnoringAutonomyGenerator) Stream(
	_ context.Context,
	request generation.Request,
	_ generation.DeltaSink,
) (generation.Result, error) {
	switch request.Purpose {
	case string(domain.GenerationPurposeDialogue):
		return generation.Result{Text: "foreground reply"}, nil
	case string(domain.GenerationPurposeMemoryExtraction):
		return generation.Result{Text: emptyM6Extraction}, nil
	case string(domain.GenerationPurposeSelfTalk):
		close(generator.selfTalkStarted)
		<-generator.releaseSelfTalk
		close(generator.selfTalkDone)
		return generation.Result{Text: "must not land after foreground"}, nil
	default:
		return generation.Result{}, errors.New("unexpected cancellation-ignoring purpose")
	}
}

func TestM6ForegroundEpochPreemptsSuccessfulProviderBeforeLanding(t *testing.T) {
	ctx := context.Background()
	generator := &cancellationIgnoringAutonomyGenerator{
		selfTalkStarted: make(chan struct{}), releaseSelfTalk: make(chan struct{}), selfTalkDone: make(chan struct{}),
	}
	fixture := newApplicationFixture(t, generator, 2)
	enableSelfTalkForTest(t, fixture)
	if _, err := fixture.application.Ingress(ctx, "first foreground"); err != nil {
		t.Fatal(err)
	}
	if err := fixture.application.ProcessResident(ctx, fixture.residentID); err != nil {
		t.Fatal(err)
	}
	if err := fixture.application.ProcessResident(ctx, fixture.residentID); err != nil {
		t.Fatal(err)
	}
	advanceSelfTalkClock(fixture)
	backgroundDone := make(chan error, 1)
	go func() { backgroundDone <- fixture.application.processAutonomyResident(ctx, fixture.residentID) }()
	select {
	case <-generator.selfTalkStarted:
	case <-time.After(5 * time.Second):
		t.Fatal("autonomous provider call did not start")
	}
	if _, err := fixture.application.Ingress(ctx, "foreground wins before landing"); err != nil {
		t.Fatal(err)
	}
	close(generator.releaseSelfTalk)
	select {
	case err := <-backgroundDone:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("cancellation-ignoring provider did not yield")
	}
	var events int
	var state, errorClass string
	if err := fixture.store.Reader().QueryRow(`SELECT outcome.state, outcome.error_class
		FROM generation_runs run JOIN generation_run_outcomes outcome ON outcome.generation_run_id = run.generation_run_id
		WHERE run.resident_id = ? AND run.purpose = 'self_talk' ORDER BY outcome.outcome_id DESC LIMIT 1`,
		fixture.residentID.String()).Scan(&state, &errorClass); err != nil {
		t.Fatal(err)
	}
	if err := fixture.store.Reader().QueryRow(`SELECT COUNT(*) FROM events
		WHERE resident_id = ? AND event_type = 'self_talk'`, fixture.residentID.String()).Scan(&events); err != nil {
		t.Fatal(err)
	}
	if state != "cancelled" || errorClass != string(generation.ErrorForegroundPreempted) || events != 0 {
		t.Fatalf("post-provider preemption state/error/events = %s/%s/%d", state, errorClass, events)
	}
}

type blockingDialogueGenerator struct {
	dialogueStarted chan struct{}
	releaseDialogue chan struct{}
	dialogueDone    chan struct{}
	mu              sync.Mutex
	selfTalkCalls   int
}

func (generator *blockingDialogueGenerator) Stream(
	_ context.Context,
	request generation.Request,
	_ generation.DeltaSink,
) (generation.Result, error) {
	switch request.Purpose {
	case string(domain.GenerationPurposeDialogue):
		close(generator.dialogueStarted)
		<-generator.releaseDialogue
		close(generator.dialogueDone)
		return generation.Result{Text: "foreground reply"}, nil
	case string(domain.GenerationPurposeMemoryExtraction):
		return generation.Result{Text: emptyM6Extraction}, nil
	case string(domain.GenerationPurposeSelfTalk):
		generator.mu.Lock()
		generator.selfTalkCalls++
		generator.mu.Unlock()
		return generation.Result{Text: "unexpected private thought"}, nil
	default:
		return generation.Result{}, errors.New("unexpected blocking-dialogue purpose")
	}
}

func TestM6AutonomousRetryRechecksForegroundBeforeAppendingRunningOutcome(t *testing.T) {
	ctx := context.Background()
	fixture, assembly, _ := prepareRunningSelfTalkFixture(t)
	prepared, err := fixture.repository.Generation(ctx, assembly.Prepare.Generation.RunID)
	if err != nil {
		t.Fatal(err)
	}
	if err := fixture.application.recordAutonomousForegroundPreempted(ctx, prepared); err != nil {
		t.Fatal(err)
	}
	generator := &blockingDialogueGenerator{
		dialogueStarted: make(chan struct{}), releaseDialogue: make(chan struct{}), dialogueDone: make(chan struct{}),
	}
	fixture.application.generator = generator
	if _, err := fixture.application.Ingress(ctx, "foreground remains pending"); err != nil {
		t.Fatal(err)
	}
	foregroundDone := make(chan error, 1)
	go func() { foregroundDone <- fixture.application.ProcessResident(ctx, fixture.residentID) }()
	select {
	case <-generator.dialogueStarted:
	case <-time.After(5 * time.Second):
		t.Fatal("foreground dialogue provider did not start")
	}
	outcomeID, err := fixture.application.ids.New()
	if err != nil {
		t.Fatal(err)
	}
	_, startErr := fixture.application.submit(ctx, domain.StartAttemptCommand(domain.Attempt{
		RunID: prepared.RunID, ResidentID: fixture.residentID, AttemptNo: 2,
		OutcomeID: outcomeID, MaxAttempts: fixture.application.maxAttempts,
	}))
	if startErr == nil || !strings.Contains(startErr.Error(), string(autonomy.BlockingForegroundPending)) {
		t.Fatalf("autonomous retry foreground error = %v", startErr)
	}
	var outcomes int
	if err := fixture.store.Reader().QueryRow(`SELECT COUNT(*) FROM generation_run_outcomes
		WHERE generation_run_id = ?`, prepared.RunID.String()).Scan(&outcomes); err != nil {
		t.Fatal(err)
	}
	if outcomes != 2 {
		t.Fatalf("blocked retry outcomes = %d, want unchanged 2", outcomes)
	}
	close(generator.releaseDialogue)
	select {
	case err := <-foregroundDone:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("foreground dialogue provider did not finish")
	}
}

func TestM6DurableAutonomousWorkRechecksQuietHoursBeforeProvider(t *testing.T) {
	ctx := context.Background()
	fixture, assembly, _ := prepareRunningSelfTalkFixture(t)
	blockedGenerator := &scriptedGenerator{steps: []generatorStep{{text: "must not be called in quiet hours"}}}
	fixture.application.generator = blockedGenerator

	schedulerClock := fixture.application.autonomyClock.(*testsupport.ManualSchedulerClock)
	current := schedulerClock.Now().Wall.UTC()
	schedulerClock.SetWall(time.Date(current.Year(), current.Month(), current.Day(), 23, 30, 0, 0, time.UTC))

	var before int
	if err := fixture.store.Reader().QueryRow(`SELECT COUNT(*) FROM generation_run_outcomes
		WHERE generation_run_id = ?`, assembly.Prepare.Generation.RunID.String()).Scan(&before); err != nil {
		t.Fatal(err)
	}
	if err := fixture.application.processAutonomyOnce(ctx, fixture.residentID); err != nil {
		t.Fatal(err)
	}
	var after int
	if err := fixture.store.Reader().QueryRow(`SELECT COUNT(*) FROM generation_run_outcomes
		WHERE generation_run_id = ?`, assembly.Prepare.Generation.RunID.String()).Scan(&after); err != nil {
		t.Fatal(err)
	}
	if blockedGenerator.CallCount() != 0 || after != before {
		t.Fatalf("quiet-hour durable work provider/outcomes = %d/%d, want 0/%d",
			blockedGenerator.CallCount(), after, before)
	}
}

func TestM6ExecutableDiscoveryAppliesLimitAfterTerminalFiltering(t *testing.T) {
	ctx := context.Background()
	fixture := landedSelfTalkFixture(t)
	quietStart, _ := autonomy.ParseLocalTime("02:00")
	quietEnd, _ := autonomy.ParseLocalTime("03:00")
	fixture.application.autonomyPolicy.SelfTalk.QuietHours = autonomy.QuietHours{Start: quietStart, End: quietEnd}
	var terminalRunRaw string
	if err := fixture.store.Reader().QueryRow(`SELECT run.generation_run_id FROM generation_runs run
		JOIN generation_run_outcomes outcome ON outcome.generation_run_id = run.generation_run_id
		WHERE run.resident_id = ? AND run.purpose = 'self_talk' AND outcome.state = 'succeeded' LIMIT 1`,
		fixture.residentID.String()).Scan(&terminalRunRaw); err != nil {
		t.Fatal(err)
	}
	writable, err := sql.Open("sqlite", fixture.store.Path())
	if err != nil {
		t.Fatal(err)
	}
	for index := 0; index < 8; index++ {
		ids, err := fixture.application.allocateIDs(3)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := writable.Exec(`INSERT INTO generation_runs(
			generation_run_id, canonical_commit_id, resident_id, purpose, idempotency_key, provider, model,
			model_version, prompt_template_version, pipeline_version_id, context_policy_version,
			sessionization_policy_version_id, memory_rendering_version, principles_revision_id,
			persona_revision_id, memory_policy_revision_id, recall_run_id, temperature, top_p, max_tokens,
			seed, generator_params, as_of, as_of_tz, budget_exceeded, dropped_input_summary, requested_at, requested_tz)
			SELECT ?, canonical_commit_id, resident_id, purpose, ?, provider, model, model_version,
			prompt_template_version, pipeline_version_id, context_policy_version, sessionization_policy_version_id,
			memory_rendering_version, principles_revision_id, persona_revision_id, memory_policy_revision_id,
			recall_run_id, temperature, top_p, max_tokens, seed, generator_params, as_of, as_of_tz,
			budget_exceeded, dropped_input_summary, requested_at, requested_tz
			FROM generation_runs WHERE generation_run_id = ?`, ids[0].String(), "terminal-filler:"+ids[0].String(), terminalRunRaw); err != nil {
			t.Fatal(err)
		}
		if _, err := writable.Exec(`INSERT INTO generation_run_inputs(
			generation_run_input_id, canonical_commit_id, generation_run_id, ordinal, role, source_type,
			source_id, inclusion_mode, content_id, recorded_at, recorded_tz)
			SELECT ?, canonical_commit_id, ?, ordinal, role, source_type, source_id, inclusion_mode,
			content_id, recorded_at, recorded_tz FROM generation_run_inputs
			WHERE generation_run_id = ? AND source_type = 'runtime_projection'`,
			ids[1].String(), ids[0].String(), terminalRunRaw); err != nil {
			t.Fatal(err)
		}
		if _, err := writable.Exec(`INSERT INTO generation_run_outcomes(
			outcome_id, canonical_commit_id, generation_run_id, attempt_no, state, output_content_id,
			prompt_tokens, completion_tokens, latency, estimated_cost, error_class, error_detail_content_id,
			recorded_at, recorded_tz)
			SELECT ?, canonical_commit_id, ?, attempt_no, state, output_content_id, prompt_tokens,
			completion_tokens, latency, estimated_cost, error_class, error_detail_content_id, recorded_at, recorded_tz
			FROM generation_run_outcomes WHERE generation_run_id = ? AND state = 'succeeded'`,
			ids[2].String(), ids[0].String(), terminalRunRaw); err != nil {
			t.Fatal(err)
		}
	}
	if err := writable.Close(); err != nil {
		t.Fatal(err)
	}
	advanceSelfTalkClock(fixture)
	snapshot, err := fixture.repository.AutonomySnapshot(ctx, autonomy.SnapshotRequest{
		ResidentID: fixture.residentID, WallNow: fixture.application.autonomyClock.Now().Wall,
		Timezone: canonical.MustTimezone("UTC"), MaxAttempts: fixture.application.maxAttempts,
	})
	if err != nil || snapshot.LastUser == nil {
		t.Fatalf("autonomy snapshot = %+v, %v", snapshot, err)
	}
	trigger := autonomy.Trigger{Kind: autonomy.TriggerIdle, SourceID: snapshot.LastUser.ID,
		Ordinal: snapshot.ConsecutiveSelfTalk + 1}
	assembly, err := fixture.application.assembleAutonomous(ctx, fixture.repository, fixture.residentID, trigger)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := fixture.application.submitWithContent(
		ctx, domain.PrepareAutonomousGenerationCommand(assembly.Prepare), assembly.Contents,
	); err != nil {
		t.Fatal(err)
	}
	work, err := fixture.repository.DiscoverExecutableAutonomousWork(ctx, fixture.residentID, 3, 1)
	if err != nil {
		t.Fatal(err)
	}
	if len(work) != 1 || work[0].RunID == nil || *work[0].RunID != assembly.Prepare.Generation.RunID {
		t.Fatalf("executable work after terminal history = %+v", work)
	}
}

func TestM6ArchivedResidentCancelsRunningAndRetryPendingAutonomousWork(t *testing.T) {
	for _, test := range []struct {
		name         string
		retryPending bool
		wantAttempt  int64
		wantRows     int
	}{
		{name: "running", wantAttempt: 1, wantRows: 2},
		{name: "retry_pending", retryPending: true, wantAttempt: 2, wantRows: 4},
	} {
		t.Run(test.name, func(t *testing.T) {
			fixture, assembly, trigger := prepareRunningSelfTalkFixture(t)
			ctx := context.Background()
			if test.retryPending {
				if err := fixture.application.Recover(ctx); err != nil {
					t.Fatal(err)
				}
			}
			if err := fixture.application.ArchiveResident(ctx, fixture.residentID); err != nil {
				t.Fatal(err)
			}
			var attempt, rows int64
			var state, failure string
			if err := fixture.store.Reader().QueryRow(`SELECT attempt_no, state, error_class
				FROM generation_run_outcomes WHERE generation_run_id = ? ORDER BY outcome_id DESC LIMIT 1`,
				assembly.Prepare.Generation.RunID.String()).Scan(&attempt, &state, &failure); err != nil {
				t.Fatal(err)
			}
			if err := fixture.store.Reader().QueryRow(`SELECT COUNT(*) FROM generation_run_outcomes
				WHERE generation_run_id = ?`, assembly.Prepare.Generation.RunID.String()).Scan(&rows); err != nil {
				t.Fatal(err)
			}
			if attempt != test.wantAttempt || state != "cancelled" ||
				failure != generation.MustOutcomeErrorCode(generation.ErrorResidentInactive, 0).String() ||
				rows != int64(test.wantRows) {
				t.Fatalf("archived autonomous attempt/state/error/rows = %d/%s/%s/%d", attempt, state, failure, rows)
			}
			work, err := fixture.repository.AutonomousWork(ctx, fixture.residentID, trigger, 2)
			if err != nil {
				t.Fatal(err)
			}
			if work.State != domain.WorkTerminalFailed {
				t.Fatalf("archived autonomous work = %+v", work)
			}
		})
	}
}

func TestM6AutonomousClaimContextsRejectSelfTalkOnlySupport(t *testing.T) {
	extraction := `{"claims":[{"grade":"inferred","perspective":"resident","source_quote":"","statement":"The resident is considering rain.","subject":"resident","temporal_kind":"volatile"}],"version":"memory-extraction-output-v1"}`
	generator := &scriptedGenerator{steps: []generatorStep{
		{text: "dialogue reply"}, {text: emptyM6Extraction}, {text: "considering rain"}, {text: extraction},
	}}
	fixture := newApplicationFixture(t, generator, 2)
	enableSelfTalkForTest(t, fixture)
	ctx := context.Background()
	if _, err := fixture.application.Ingress(ctx, "seed private reflection"); err != nil {
		t.Fatal(err)
	}
	if err := fixture.application.ProcessResident(ctx, fixture.residentID); err != nil {
		t.Fatal(err)
	}
	if err := fixture.application.ProcessResident(ctx, fixture.residentID); err != nil {
		t.Fatal(err)
	}
	advanceSelfTalkClock(fixture)
	if err := fixture.application.processAutonomyResident(ctx, fixture.residentID); err != nil {
		t.Fatal(err)
	}
	if err := fixture.application.ProcessResident(ctx, fixture.residentID); err != nil {
		t.Fatal(err)
	}
	var claimRaw string
	if err := fixture.store.Reader().QueryRow(`SELECT claim.claim_id FROM claims claim
		JOIN claim_evidence evidence ON evidence.claim_id = claim.claim_id
		JOIN events event ON event.event_id = evidence.event_id
		WHERE claim.owner_resident_id = ? AND event.event_type = 'self_talk' LIMIT 1`,
		fixture.residentID.String()).Scan(&claimRaw); err != nil {
		t.Fatal(err)
	}
	claimID, err := canonical.ParseID(claimRaw)
	if err != nil {
		t.Fatal(err)
	}
	_, err = fixture.repository.AutonomousClaimContexts(ctx, fixture.residentID, autonomy.Trigger{
		Kind: autonomy.TriggerVolatileAging, SourceID: claimID,
		Boundary: canonical.InstantFromTime(fixture.clock.Now()),
	})
	if !errors.Is(err, domain.ErrAutonomousContextIneligible) {
		t.Fatalf("self-talk-only context error = %v", err)
	}
}

func TestM6AutonomyHintAndScanSkipOptionalWorkAfterMandatoryFailure(t *testing.T) {
	generator := &scriptedGenerator{steps: []generatorStep{
		{text: "dialogue reply"}, {text: emptyM6Extraction}, {text: "must not be called"},
	}}
	fixture := newApplicationFixture(t, generator, 2)
	enableSelfTalkForTest(t, fixture)
	ctx := context.Background()
	if _, err := fixture.application.Ingress(ctx, "seed scheduler failure"); err != nil {
		t.Fatal(err)
	}
	if err := fixture.application.ProcessResident(ctx, fixture.residentID); err != nil {
		t.Fatal(err)
	}
	advanceSelfTalkClock(fixture)
	failing := &failingDialogueDiscoveryRepository{
		CanonicalRepository: fixture.repository, failure: errors.New("mandatory discovery failed"),
	}
	fixture.application.repository = failing
	// This is the exact hint-handler seam used by autonomyLoop.
	fixture.application.processAutonomyTurn(ctx, fixture.residentID)
	// The periodic path must apply the same suppression rule.
	fixture.application.scanAutonomyResidents(ctx)
	if failing.calls != 2 {
		t.Fatalf("mandatory discovery calls = %d, want hint + scan", failing.calls)
	}
	var runs int
	if err := fixture.store.Reader().QueryRow(`SELECT COUNT(*) FROM generation_runs WHERE purpose = 'self_talk'`).Scan(&runs); err != nil {
		t.Fatal(err)
	}
	if runs != 0 || generator.CallCount() != 2 {
		t.Fatalf("optional work after mandatory failure: self-talk runs=%d provider calls=%d", runs, generator.CallCount())
	}
}

func TestM6InitiativeFreshProjectionLandsAtomicallyAndEntersNextUserContextOnce(t *testing.T) {
	ctx := context.Background()
	direct := `{"claims":[{"grade":"stated","perspective":"resident","source_quote":"I may need an umbrella","statement":"The resident may need an umbrella.","subject":"resident","temporal_kind":"volatile"}],"version":"memory-extraction-output-v1"}`
	meta := `{"claims":[{"grade":"stated","perspective":"source_actor","source_quote":"I think you may need an umbrella","statement":"The owner thinks the resident may need an umbrella.","subject":"resident","temporal_kind":"stable"}],"version":"memory-extraction-output-v1"}`
	alignment := `{"aligned":true,"confidence":"800000","version":"memory-alignment-output-v1"}`
	generator := &scriptedGenerator{steps: []generatorStep{
		{text: "dialogue 1"}, {text: direct}, {text: "dialogue 2"}, {text: direct},
		{text: "dialogue 3"}, {text: direct}, {text: "dialogue 4"}, {text: meta},
		{text: "dialogue 5"}, {text: meta}, {text: alignment}, {text: "Bring an umbrella today."},
	}}
	fixture := newApplicationFixture(t, generator, 2)
	if _, err := fixture.application.activateMemoryPolicyV4ForTest(ctx, fixture.residentID); err != nil {
		t.Fatal(err)
	}
	for _, text := range []string{
		"I may need an umbrella", "I may need an umbrella", "I may need an umbrella",
		"I think you may need an umbrella", "I think you may need an umbrella",
	} {
		if _, err := fixture.application.Ingress(ctx, text); err != nil {
			t.Fatal(err)
		}
		if err := fixture.application.ProcessResident(ctx, fixture.residentID); err != nil {
			t.Fatal(err)
		}
	}
	drainForegroundToOptionalForTest(t, fixture)
	if _, err := fixture.application.activateMemoryPolicyV4ForTest(ctx, fixture.residentID); err != nil {
		t.Fatal(err)
	}
	policy := autonomy.DefaultPolicy("UTC")
	policy.Initiative.Enabled = true
	fixture.application.autonomyPolicy = &policy
	fixture.application.autonomySource = fixture.repository
	fixture.application.autonomyClock = testsupport.NewManualSchedulerClock(fixture.clock.Now())
	fixture.application.projectionMaxStaleness = 5 * time.Minute
	advance := 31*24*time.Hour + time.Minute
	fixture.clock.Advance(advance)
	schedulerClock := fixture.application.autonomyClock.(*testsupport.ManualSchedulerClock)
	schedulerClock.AdvanceWall(advance)
	schedulerClock.AdvanceMonotonic(advance)
	reconcileRecallResident(t, fixture)
	// reconcileRecallResident advances the Canonical/evaluator clock by one
	// second; keep the independent scheduler wall/monotonic sources aligned.
	schedulerClock.AdvanceWall(time.Second)
	schedulerClock.AdvanceMonotonic(time.Second)
	triggers, err := fixture.repository.DiscoverInitiativeTriggers(
		ctx, fixture.residentID, canonical.InstantFromTime(fixture.clock.Now()), 5*time.Minute, 16,
	)
	if err != nil {
		t.Fatal(err)
	}
	if len(triggers) == 0 {
		rows, queryErr := fixture.store.Reader().Query(`SELECT claim.kind, state.stage, state.status,
			state.temporal_relation, scope.view_scope FROM claim_states state
			JOIN claims claim ON claim.claim_id = state.claim_id
			JOIN claim_view_scope_current scope ON scope.claim_id = state.claim_id
			WHERE state.resident_id = ?`, fixture.residentID.String())
		if queryErr != nil {
			t.Fatal(queryErr)
		}
		defer rows.Close()
		var states []string
		for rows.Next() {
			var kind, stage, status, temporal, scope string
			if err := rows.Scan(&kind, &stage, &status, &temporal, &scope); err != nil {
				t.Fatal(err)
			}
			states = append(states, strings.Join([]string{kind, stage, status, temporal, scope}, "/"))
		}
		t.Fatalf("initiative trigger discovery empty; claim states=%v", states)
	}
	snapshot, err := fixture.repository.AutonomySnapshot(ctx, autonomy.SnapshotRequest{
		ResidentID: fixture.residentID, WallNow: schedulerClock.Now().Wall,
		Timezone: canonical.MustTimezone("UTC"), MaxAttempts: fixture.application.maxAttempts,
	})
	if err != nil {
		t.Fatal(err)
	}
	timing := autonomy.ResolveEvaluationTime(snapshot, schedulerClock.Now(), autonomy.RuntimeAnchors{})
	initiativeSnapshot := snapshot
	initiativeSnapshot.ProjectionAvailable = true
	decision := autonomy.DecideInitiative(policy, initiativeSnapshot, triggers[0], timing)
	if !decision.Eligible {
		t.Fatalf("initiative decision blocked before app: reason=%s snapshot=%+v timing=%+v", decision.BlockingReason, snapshot, timing)
	}
	contexts, err := fixture.repository.AutonomousClaimContexts(ctx, fixture.residentID, triggers[0])
	if err != nil || len(contexts) != 1 {
		t.Fatalf("initiative claim contexts = %+v, %v", contexts, err)
	}
	evidence, available, err := fixture.repository.AutonomousProjectionEvidence(
		ctx, fixture.residentID, canonical.InstantFromTime(schedulerClock.Now().Wall), 5*time.Minute,
	)
	if err != nil || !available {
		t.Fatalf("initiative Projection evidence = %+v, available=%v err=%v", evidence, available, err)
	}
	processed, err := fixture.application.processAutonomousTrigger(
		ctx, fixture.repository, fixture.residentID, triggers[0],
	)
	if err != nil || !processed {
		t.Fatalf("process initiative trigger = %v, %v", processed, err)
	}
	var eventRaw, runRaw, eventCommit, outcomeCommit string
	if err := fixture.store.Reader().QueryRow(`SELECT event.event_id, event.generation_run_id,
		event.canonical_commit_id, outcome.canonical_commit_id
		FROM events event JOIN generation_run_outcomes outcome
		  ON outcome.generation_run_id = event.generation_run_id AND outcome.state = 'succeeded'
		WHERE event.resident_id = ? AND event.event_type = 'outbound_initiative'`,
		fixture.residentID.String()).Scan(&eventRaw, &runRaw, &eventCommit, &outcomeCommit); err != nil {
		var autonomyRuns int
		if countErr := fixture.store.Reader().QueryRow(`SELECT COUNT(*) FROM generation_runs
			WHERE resident_id = ? AND purpose = 'outbound_initiative'`, fixture.residentID.String()).Scan(&autonomyRuns); countErr != nil {
			t.Fatal(countErr)
		}
		t.Fatalf("load initiative event: %v; triggers=%v runs=%d provider calls=%d", err, triggers, autonomyRuns, generator.CallCount())
	}
	if eventCommit != outcomeCommit {
		t.Fatalf("initiative event/outcome commits = %s/%s", eventCommit, outcomeCommit)
	}
	var claimInputs, usages int
	if err := fixture.store.Reader().QueryRow(`SELECT COUNT(*) FROM generation_run_inputs
		WHERE generation_run_id = ? AND inclusion_mode = 'memory_recall'`, runRaw).Scan(&claimInputs); err != nil {
		t.Fatal(err)
	}
	if err := fixture.store.Reader().QueryRow(`SELECT COUNT(*) FROM claim_usages
		WHERE generation_run_id = ? AND usage_type = 'prompt_included'`, runRaw).Scan(&usages); err != nil {
		t.Fatal(err)
	}
	if claimInputs != 1 || usages != 1 {
		t.Fatalf("initiative claim inputs/usages = %d/%d", claimInputs, usages)
	}
	initiativeID, err := canonical.ParseID(eventRaw)
	if err != nil {
		t.Fatal(err)
	}
	first, err := fixture.application.Ingress(ctx, "Thanks, should I bring it?")
	if err != nil {
		t.Fatal(err)
	}
	firstRun := dialogueRunIDForEventForTest(t, fixture, first)
	assertInitiativeContextCount(t, fixture, firstRun, initiativeID, 1)
	second, err := fixture.application.Ingress(ctx, "One more question")
	if err != nil {
		t.Fatal(err)
	}
	secondRun := dialogueRunIDForEventForTest(t, fixture, second)
	assertInitiativeContextCount(t, fixture, secondRun, initiativeID, 0)
}

func TestM6FutureToCurrentRequiresHistoricallyFutureClaimInReadAndWriter(t *testing.T) {
	ctx := context.Background()
	direct := `{"claims":[{"grade":"stated","perspective":"resident","source_quote":"I may need an umbrella","statement":"The resident may need an umbrella.","subject":"resident","temporal_kind":"stable"}],"version":"memory-extraction-output-v1"}`
	meta := `{"claims":[{"grade":"stated","perspective":"source_actor","source_quote":"I think you may need an umbrella","statement":"The owner thinks the resident may need an umbrella.","subject":"resident","temporal_kind":"stable"}],"version":"memory-extraction-output-v1"}`
	alignment := `{"aligned":true,"confidence":"800000","version":"memory-alignment-output-v1"}`
	fixture := newApplicationFixture(t, &scriptedGenerator{steps: []generatorStep{
		{text: "dialogue 1"}, {text: direct}, {text: "dialogue 2"}, {text: direct},
		{text: "dialogue 3"}, {text: direct}, {text: "dialogue 4"}, {text: meta},
		{text: "dialogue 5"}, {text: meta}, {text: alignment},
	}}, 2)
	if _, err := fixture.application.activateMemoryPolicyV4ForTest(ctx, fixture.residentID); err != nil {
		t.Fatal(err)
	}
	for _, text := range []string{
		"I may need an umbrella", "I may need an umbrella", "I may need an umbrella",
		"I think you may need an umbrella", "I think you may need an umbrella",
	} {
		if _, err := fixture.application.Ingress(ctx, text); err != nil {
			t.Fatal(err)
		}
		if err := fixture.application.ProcessResident(ctx, fixture.residentID); err != nil {
			t.Fatal(err)
		}
	}
	drainForegroundToOptionalForTest(t, fixture)
	if _, err := fixture.application.activateMemoryPolicyV4ForTest(ctx, fixture.residentID); err != nil {
		t.Fatal(err)
	}
	var claimRaw, claimCommitRaw string
	var claimRecordedAt int64
	if err := fixture.store.Reader().QueryRow(`SELECT claim_id, canonical_commit_id, recorded_at
		FROM claims WHERE owner_resident_id = ? AND kind = 'direct' LIMIT 1`, fixture.residentID.String()).Scan(
		&claimRaw, &claimCommitRaw, &claimRecordedAt,
	); err != nil {
		t.Fatal(err)
	}
	claimID, err := canonical.ParseID(claimRaw)
	if err != nil {
		t.Fatal(err)
	}
	assertionID, err := fixture.application.ids.New()
	if err != nil {
		t.Fatal(err)
	}
	writable, err := sql.Open("sqlite", fixture.store.Path())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := writable.Exec(`INSERT INTO claim_validity_assertions(
		validity_assertion_id, canonical_commit_id, claim_id, assertion_type,
		valid_from, valid_from_tz, valid_to, valid_to_tz, evidence_event_id, confidence,
		actor_principal_id, reason_code, reason_content_id, recorded_at, recorded_tz
	) VALUES (?, ?, ?, 'interval', ?, 'UTC', NULL, NULL, NULL, 1000000,
		NULL, 'test_not_historically_future', NULL, ?, 'UTC')`, assertionID.String(), claimCommitRaw,
		claimRaw, claimRecordedAt, claimRecordedAt); err != nil {
		_ = writable.Close()
		t.Fatal(err)
	}
	if err := writable.Close(); err != nil {
		t.Fatal(err)
	}
	registry, err := sqlite.ActiveProjectionRegistry()
	if err != nil {
		t.Fatal(err)
	}
	surface := fixture.store.Projection()
	dropClaimProjections := func() {
		t.Helper()
		for _, name := range []projection.Name{projection.ClaimStatesName, projection.ClaimViewScopeCurrentName} {
			definition, err := registry.Definition(name)
			if err != nil {
				t.Fatal(err)
			}
			watermark, exists, err := surface.Watermark(ctx, name, fixture.residentID)
			if err != nil {
				t.Fatal(err)
			}
			var observed *projection.Watermark
			if exists {
				observed = &watermark
			}
			if err := surface.Drop(ctx, projection.DropRequest{
				Definition: definition, ResidentID: fixture.residentID, Observed: observed,
			}); err != nil {
				t.Fatal(err)
			}
		}
	}
	dropClaimProjections()
	policy := autonomy.DefaultPolicy("UTC")
	policy.Initiative.Enabled = true
	fixture.application.autonomyPolicy = &policy
	fixture.application.autonomySource = fixture.repository
	fixture.application.autonomyClock = testsupport.NewManualSchedulerClock(fixture.clock.Now())
	fixture.application.projectionMaxStaleness = 5 * time.Minute
	advance := 31*24*time.Hour + time.Minute
	fixture.clock.Advance(advance)
	schedulerClock := fixture.application.autonomyClock.(*testsupport.ManualSchedulerClock)
	schedulerClock.AdvanceWall(advance)
	schedulerClock.AdvanceMonotonic(advance)
	reconcileRecallResident(t, fixture)
	schedulerClock.AdvanceWall(time.Second)
	schedulerClock.AdvanceMonotonic(time.Second)
	triggers, err := fixture.repository.DiscoverInitiativeTriggers(
		ctx, fixture.residentID, canonical.InstantFromTime(fixture.clock.Now()), 5*time.Minute, 16,
	)
	if err != nil {
		t.Fatal(err)
	}
	for _, trigger := range triggers {
		if trigger.Kind == autonomy.TriggerFutureToCurrent && trigger.SourceID == claimID {
			t.Fatalf("read discovery accepted a claim that was never future: %+v", trigger)
		}
	}
	trigger := autonomy.Trigger{
		Kind: autonomy.TriggerFutureToCurrent, SourceID: claimID, Boundary: canonical.Instant(claimRecordedAt),
	}
	assembly, err := fixture.application.assembleAutonomous(ctx, fixture.repository, fixture.residentID, trigger)
	if err != nil {
		t.Fatal(err)
	}
	_, err = fixture.application.submitWithContent(
		ctx, domain.PrepareAutonomousGenerationCommand(assembly.Prepare), assembly.Contents,
	)
	if err == nil || !strings.Contains(err.Error(), "was not recorded while its boundary was future") {
		t.Fatalf("Writer future-to-current historical validation error = %v", err)
	}

	// Build a historically valid interval whose target claim is current when
	// the durable run is prepared, then let valid_to pass without changing the
	// frozen trigger or its Canonical claim inputs. The refreshed claim_states
	// row, rather than watermark freshness alone, must block every execution
	// seam for that durable run.
	validAssertionID, err := fixture.application.ids.New()
	if err != nil {
		t.Fatal(err)
	}
	validFrom := canonical.InstantFromTime(fixture.clock.Now().Add(-time.Minute))
	validTo := canonical.InstantFromTime(fixture.clock.Now().Add(time.Minute))
	writable, err = sql.Open("sqlite", fixture.store.Path())
	if err != nil {
		t.Fatal(err)
	}
	var validityCommitRaw string
	var validityRecordedAt int64
	if err := fixture.store.Reader().QueryRow(`SELECT canonical_commit_id, committed_at
		FROM canonical_commits
		WHERE resident_id = ? AND canonical_commit_id <> ?
		ORDER BY commit_seq DESC LIMIT 1`, fixture.residentID.String(), claimCommitRaw).Scan(
		&validityCommitRaw, &validityRecordedAt,
	); err != nil {
		_ = writable.Close()
		t.Fatal(err)
	}
	if _, err := writable.Exec(`INSERT INTO claim_validity_assertions(
		validity_assertion_id, canonical_commit_id, claim_id, assertion_type,
		valid_from, valid_from_tz, valid_to, valid_to_tz, evidence_event_id, confidence,
		actor_principal_id, reason_code, reason_content_id, recorded_at, recorded_tz
	) VALUES (?, ?, ?, 'interval', ?, 'UTC', ?, 'UTC', NULL, 1000000,
		NULL, 'test_current_then_past', NULL, ?, 'UTC')`, validAssertionID.String(), validityCommitRaw,
		claimRaw, int64(validFrom), int64(validTo), validityRecordedAt); err != nil {
		_ = writable.Close()
		t.Fatal(err)
	}
	if err := writable.Close(); err != nil {
		t.Fatal(err)
	}
	dropClaimProjections()
	reconcileRecallResident(t, fixture)
	schedulerClock.AdvanceWall(time.Second)
	schedulerClock.AdvanceMonotonic(time.Second)
	triggers, err = fixture.repository.DiscoverInitiativeTriggers(
		ctx, fixture.residentID, canonical.InstantFromTime(fixture.clock.Now()), 5*time.Minute, 16,
	)
	if err != nil {
		t.Fatal(err)
	}
	var currentRelation string
	var currentTemporalKind string
	if err := fixture.store.Reader().QueryRow(`SELECT state.temporal_relation, claim.temporal_kind FROM claim_states state
		JOIN claims claim ON claim.claim_id = state.claim_id
		WHERE state.resident_id = ? AND state.claim_id = ?`, fixture.residentID.String(), claimID.String()).Scan(
		&currentRelation, &currentTemporalKind,
	); err != nil {
		t.Fatal(err)
	}
	var latestAssertionRaw string
	var latestValidFrom sql.NullInt64
	if err := fixture.store.Reader().QueryRow(`SELECT assertion.validity_assertion_id, assertion.valid_from
		FROM claim_validity_assertions assertion
		JOIN canonical_commits commit_row ON commit_row.canonical_commit_id = assertion.canonical_commit_id
		WHERE assertion.claim_id = ? ORDER BY commit_row.commit_seq DESC, assertion.validity_assertion_id DESC LIMIT 1`,
		claimID.String()).Scan(&latestAssertionRaw, &latestValidFrom); err != nil {
		t.Fatal(err)
	}
	var projectedAsOf int64
	if err := fixture.store.Reader().QueryRow(`SELECT as_of FROM projection_watermarks
		WHERE projection_name = 'claim_states' AND resident_id = ?`, fixture.residentID.String()).Scan(&projectedAsOf); err != nil {
		t.Fatal(err)
	}
	var currentTrigger autonomy.Trigger
	for _, candidate := range triggers {
		if candidate.Kind == autonomy.TriggerFutureToCurrent && candidate.SourceID == claimID {
			currentTrigger = candidate
			break
		}
	}
	if err := currentTrigger.Validate(); err != nil || currentTrigger.Boundary != validFrom {
		t.Fatalf("current future-to-current trigger = %+v, validation=%v, projected relation=%q/%q as_of=%d latest=%s/%v want=%s/%d..%d",
			currentTrigger, err, currentRelation, currentTemporalKind, projectedAsOf, latestAssertionRaw, latestValidFrom,
			validAssertionID, validFrom, validTo)
	}
	assembly, err = fixture.application.assembleAutonomous(ctx, fixture.repository, fixture.residentID, currentTrigger)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := fixture.application.submitWithContent(
		ctx, domain.PrepareAutonomousGenerationCommand(assembly.Prepare), assembly.Contents,
	); err != nil {
		t.Fatal(err)
	}
	prepared, err := fixture.repository.Generation(ctx, assembly.Prepare.Generation.RunID)
	if err != nil {
		t.Fatal(err)
	}

	fixture.clock.Advance(2 * time.Minute)
	schedulerClock.AdvanceWall(2 * time.Minute)
	schedulerClock.AdvanceMonotonic(2 * time.Minute)
	reconcileRecallResident(t, fixture)
	schedulerClock.AdvanceWall(time.Second)
	schedulerClock.AdvanceMonotonic(time.Second)
	var relation string
	if err := fixture.store.Reader().QueryRow(`SELECT temporal_relation FROM claim_states
		WHERE resident_id = ? AND claim_id = ?`, fixture.residentID.String(), claimID.String()).Scan(&relation); err != nil {
		t.Fatal(err)
	}
	if relation != "past" {
		t.Fatalf("claim relation after valid_to = %q, want past", relation)
	}

	blockedGenerator := &scriptedGenerator{steps: []generatorStep{{text: "must not be called after valid_to"}}}
	fixture.application.generator = blockedGenerator
	processed, err := fixture.application.processAutonomousTrigger(
		ctx, fixture.repository, fixture.residentID, currentTrigger,
	)
	if err != nil || !processed {
		t.Fatalf("process durable past initiative = %v, processed=%v", err, processed)
	}
	if blockedGenerator.CallCount() != 0 {
		t.Fatalf("provider calls after valid_to = %d, want 0", blockedGenerator.CallCount())
	}

	resident, err := fixture.repository.Resident(ctx, fixture.residentID)
	if err != nil {
		t.Fatal(err)
	}
	output, err := fixture.application.newContent(
		fixture.residentID, "generation_output", []byte("must not land after valid_to"), "independent",
	)
	if err != nil {
		t.Fatal(err)
	}
	landingIDs, err := fixture.application.allocateIDs(2)
	if err != nil {
		t.Fatal(err)
	}
	_, err = fixture.application.submit(ctx, domain.LandAutonomousEventCommand(domain.LandAutonomousEvent{
		Attempt: domain.Attempt{RunID: prepared.RunID, ResidentID: fixture.residentID,
			AttemptNo: prepared.AttemptNo, OutcomeID: landingIDs[0], MaxAttempts: fixture.application.maxAttempts},
		EventID: landingIDs[1], ResidentPrincipalID: resident.ResidentPrincipalID,
		OwnerPrincipalID: resident.OwnerPrincipalID, Output: output,
		OccurredAt: canonical.InstantFromTime(fixture.clock.Now()), OccurredTZ: canonical.MustTimezone("UTC"),
		Trigger: currentTrigger, Policy: assembly.Prepare.Policy,
		ProjectionMaxStaleness: assembly.Prepare.ProjectionMaxStaleness,
		ProjectionEvidence:     assembly.Prepare.ProjectionEvidence,
	}))
	if err == nil || !strings.Contains(err.Error(), "live Projection relation is no longer eligible") {
		t.Fatalf("past initiative landing error = %v", err)
	}
	if err := fixture.application.recordAutonomousFailure(
		ctx, prepared, "failed", generation.MustOutcomeErrorCode(generation.ErrorTransport, 0),
	); err != nil {
		t.Fatal(err)
	}
	retryOutcomeID, err := fixture.application.ids.New()
	if err != nil {
		t.Fatal(err)
	}
	_, err = fixture.application.submit(ctx, domain.StartAttemptCommand(domain.Attempt{
		RunID: prepared.RunID, ResidentID: fixture.residentID,
		AttemptNo: prepared.AttemptNo + 1, OutcomeID: retryOutcomeID,
		MaxAttempts: fixture.application.maxAttempts,
	}))
	if err == nil || !strings.Contains(err.Error(), "live Projection relation is no longer eligible") {
		t.Fatalf("past initiative retry error = %v", err)
	}
	var outcomes, events int
	if err := fixture.store.Reader().QueryRow(`SELECT COUNT(*) FROM generation_run_outcomes
		WHERE generation_run_id = ?`, prepared.RunID.String()).Scan(&outcomes); err != nil {
		t.Fatal(err)
	}
	if err := fixture.store.Reader().QueryRow(`SELECT COUNT(*) FROM events
		WHERE generation_run_id = ?`, prepared.RunID.String()).Scan(&events); err != nil {
		t.Fatal(err)
	}
	if outcomes != 2 || events != 0 {
		t.Fatalf("past initiative outcomes/events = %d/%d, want 2/0", outcomes, events)
	}
}

func assertInitiativeContextCount(
	t *testing.T,
	fixture applicationFixture,
	runID, initiativeID canonical.ID,
	want int,
) {
	t.Helper()
	var count int
	if err := fixture.store.Reader().QueryRow(`SELECT COUNT(*) FROM generation_run_inputs
		WHERE generation_run_id = ? AND source_type = 'event' AND source_id = ?
		  AND inclusion_mode = 'context_backfill'`, runID.String(), initiativeID.String()).Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != want {
		t.Fatalf("initiative context count for run %s = %d, want %d", runID, count, want)
	}
}

func TestM6InitiativeLandingRejectsCanonicalScopeRaceAfterFrozenProjection(t *testing.T) {
	fixture, generator, trigger := initiativeReadyForRaceTest(t)
	ctx := context.Background()
	assembly, err := fixture.application.assembleAutonomous(ctx, fixture.repository, fixture.residentID, trigger)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := fixture.application.submitWithContent(
		ctx, domain.PrepareAutonomousGenerationCommand(assembly.Prepare), assembly.Contents,
	); err != nil {
		t.Fatal(err)
	}
	if _, err := fixture.application.SetMemoryClaimScope(
		ctx, fixture.residentID, trigger.SourceID, memory.ScopeAdminOnly,
	); err != nil {
		t.Fatal(err)
	}
	prepared, err := fixture.repository.Generation(ctx, assembly.Prepare.Generation.RunID)
	if err != nil {
		t.Fatal(err)
	}
	err = fixture.application.callAndLandAutonomous(ctx, prepared, trigger)
	if err == nil || (!strings.Contains(err.Error(), "changed after its frozen Projection capture") &&
		!strings.Contains(err.Error(), "not direct resident_ui")) {
		t.Fatalf("initiative scope-race landing error = %v", err)
	}
	var events, succeeded int
	if err := fixture.store.Reader().QueryRow(`SELECT COUNT(*) FROM events WHERE generation_run_id = ?`,
		prepared.RunID.String()).Scan(&events); err != nil {
		t.Fatal(err)
	}
	if err := fixture.store.Reader().QueryRow(`SELECT COUNT(*) FROM generation_run_outcomes
		WHERE generation_run_id = ? AND state = 'succeeded'`, prepared.RunID.String()).Scan(&succeeded); err != nil {
		t.Fatal(err)
	}
	if events != 0 || succeeded != 0 || generator.CallCount() != 12 {
		t.Fatalf("initiative race rollback events/succeeded/provider = %d/%d/%d", events, succeeded, generator.CallCount())
	}
}

func TestM6InitiativeWriterRejectsLegacyMemoryPolicyV1(t *testing.T) {
	fixture := newApplicationFixture(t, &scriptedGenerator{}, 1)
	ctx := context.Background()
	resident, err := fixture.repository.Resident(ctx, fixture.residentID)
	if err != nil {
		t.Fatal(err)
	}
	var headRaw int64
	if err := fixture.store.Reader().QueryRow(`SELECT MAX(commit_seq) FROM canonical_commits`).Scan(&headRaw); err != nil {
		t.Fatal(err)
	}
	head, err := canonical.NewCommitSeq(headRaw)
	if err != nil {
		t.Fatal(err)
	}
	now := canonical.InstantFromTime(fixture.clock.Now())
	evidence := &domain.AutonomousProjectionEvidence{
		CapturedHead: head,
		ClaimStates: projection.Watermark{
			ProjectionName: projection.ClaimStatesName, ResidentID: fixture.residentID,
			ProjectionVersion: "claim-states-v1", SourceCommitSeq: head, AsOf: now,
			AsOfTZ: canonical.MustTimezone("UTC"), Dependencies: []projection.Dependency{{
				Kind: projection.MemoryPolicyDependency, VersionID: resident.MemoryPolicyRevisionID,
			}},
		},
		ViewScope: projection.Watermark{
			ProjectionName: projection.ClaimViewScopeCurrentName, ResidentID: fixture.residentID,
			ProjectionVersion: "claim-view-scope-current-v1", SourceCommitSeq: head, AsOf: now,
			AsOfTZ: canonical.MustTimezone("UTC"), Dependencies: []projection.Dependency{},
		},
	}
	sourceID, err := fixture.application.ids.New()
	if err != nil {
		t.Fatal(err)
	}
	trigger := autonomy.Trigger{Kind: autonomy.TriggerVolatileAging, SourceID: sourceID, Boundary: now}
	policy := autonomy.DefaultPolicy("UTC")
	policy.Initiative.Enabled = true
	runtimeJSON, err := domain.NewAutonomousRuntimeProjection(trigger, policy, 5*time.Minute, evidence)
	if err != nil {
		t.Fatal(err)
	}
	runtimeContent, err := fixture.application.newContent(
		fixture.residentID, "generation_input", runtimeJSON.Bytes(), "independent",
	)
	if err != nil {
		t.Fatal(err)
	}
	ids, err := fixture.application.allocateIDs(3)
	if err != nil {
		t.Fatal(err)
	}
	_, params, err := domain.NewUnstructuredGeneratorParams(false, canonical.ByteSize(64<<10))
	if err != nil {
		t.Fatal(err)
	}
	dropped, err := canonical.MarshalCanonical(struct {
		Claims canonical.Count `json:"claims"`
	}{})
	if err != nil {
		t.Fatal(err)
	}
	versions, err := domain.GenerationVersionsForPurpose(domain.GenerationPurposeOutboundInitiative)
	if err != nil {
		t.Fatal(err)
	}
	key, err := trigger.IdempotencyKey(string(policy.Version), fixture.residentID)
	if err != nil {
		t.Fatal(err)
	}
	inputID := ids[2]
	prepare := domain.PrepareAutonomousGeneration{
		Generation: domain.PrepareGeneration{
			RunID: ids[0], ResidentID: fixture.residentID, Purpose: domain.GenerationPurposeOutboundInitiative,
			IdempotencyKey: key, Provider: "test", Model: "test-model",
			PromptTemplateVersion: versions.PromptTemplateVersion, ContextPolicyVersion: versions.ContextPolicyVersion,
			MemoryRenderingVersion: versions.MemoryRenderingVersion, PipelineVersionID: resident.PipelineVersionID,
			PrinciplesRevisionID: resident.PrinciplesRevisionID, PersonaRevisionID: resident.PersonaRevisionID,
			MemoryPolicyRevisionID: resident.MemoryPolicyRevisionID, AsOf: now, AsOfTZ: canonical.MustTimezone("UTC"),
			DroppedInputSummary: dropped, GeneratorParams: params, RunningOutcomeID: ids[1],
			Inputs: []domain.GenerationInput{{
				ID: inputID, Ordinal: 0, Role: "system", SourceType: "runtime_projection",
				InclusionMode: "runtime_projection", Content: runtimeContent,
			}},
		},
		MaxAttempts: fixture.application.maxAttempts,
		Trigger:     trigger, Policy: policy, ProjectionMaxStaleness: 5 * time.Minute, ProjectionEvidence: evidence,
	}
	_, err = fixture.application.submit(
		ctx, domain.PrepareAutonomousGenerationCommand(prepare),
	)
	if err == nil || !strings.Contains(err.Error(), "memory-policy-v2, memory-policy-v3, memory-policy-v4, or memory-policy-v5") {
		t.Fatalf("legacy initiative Writer error = %v", err)
	}
	var runs int
	if err := fixture.store.Reader().QueryRow(`SELECT COUNT(*) FROM generation_runs WHERE generation_run_id = ?`,
		ids[0].String()).Scan(&runs); err != nil {
		t.Fatal(err)
	}
	if runs != 0 {
		t.Fatalf("legacy initiative prepare rows = %d", runs)
	}
}

func initiativeReadyForRaceTest(t *testing.T) (applicationFixture, *scriptedGenerator, autonomy.Trigger) {
	t.Helper()
	ctx := context.Background()
	direct := `{"claims":[{"grade":"stated","perspective":"resident","source_quote":"I may need a coat","statement":"The resident may need a coat.","subject":"resident","temporal_kind":"volatile"}],"version":"memory-extraction-output-v1"}`
	meta := `{"claims":[{"grade":"stated","perspective":"source_actor","source_quote":"I think you may need a coat","statement":"The owner thinks the resident may need a coat.","subject":"resident","temporal_kind":"stable"}],"version":"memory-extraction-output-v1"}`
	generator := &scriptedGenerator{steps: []generatorStep{
		{text: "dialogue 1"}, {text: direct}, {text: "dialogue 2"}, {text: direct},
		{text: "dialogue 3"}, {text: direct}, {text: "dialogue 4"}, {text: meta},
		{text: "dialogue 5"}, {text: meta},
		{text: `{"aligned":true,"confidence":"800000","version":"memory-alignment-output-v1"}`},
		{text: "Wear a coat today."},
	}}
	fixture := newApplicationFixture(t, generator, 2)
	if _, err := fixture.application.activateMemoryPolicyV4ForTest(ctx, fixture.residentID); err != nil {
		t.Fatal(err)
	}
	for _, text := range []string{
		"I may need a coat", "I may need a coat", "I may need a coat",
		"I think you may need a coat", "I think you may need a coat",
	} {
		if _, err := fixture.application.Ingress(ctx, text); err != nil {
			t.Fatal(err)
		}
		if err := fixture.application.ProcessResident(ctx, fixture.residentID); err != nil {
			t.Fatal(err)
		}
	}
	drainForegroundToOptionalForTest(t, fixture)
	if _, err := fixture.application.activateMemoryPolicyV4ForTest(ctx, fixture.residentID); err != nil {
		t.Fatal(err)
	}
	policy := autonomy.DefaultPolicy("UTC")
	policy.Initiative.Enabled = true
	fixture.application.autonomyPolicy = &policy
	fixture.application.autonomySource = fixture.repository
	schedulerClock := testsupport.NewManualSchedulerClock(fixture.clock.Now())
	fixture.application.autonomyClock = schedulerClock
	fixture.application.projectionMaxStaleness = 5 * time.Minute
	advance := 31*24*time.Hour + time.Minute
	fixture.clock.Advance(advance)
	schedulerClock.AdvanceWall(advance)
	schedulerClock.AdvanceMonotonic(advance)
	reconcileRecallResident(t, fixture)
	schedulerClock.AdvanceWall(time.Second)
	schedulerClock.AdvanceMonotonic(time.Second)
	triggers, err := fixture.repository.DiscoverInitiativeTriggers(
		ctx, fixture.residentID, canonical.InstantFromTime(schedulerClock.Now().Wall), 5*time.Minute, 16,
	)
	if err != nil || len(triggers) != 1 {
		t.Fatalf("race fixture initiative triggers = %+v, %v", triggers, err)
	}
	return fixture, generator, triggers[0]
}

func TestM6EligibleSelfTalkPrecedesRecoveredInitiative(t *testing.T) {
	ctx := context.Background()
	fixture, _, initiativeTrigger := initiativeReadyForRaceTest(t)
	policy := *fixture.application.autonomyPolicy
	policy.SelfTalk.Enabled = true
	quietStart, _ := autonomy.ParseLocalTime("02:00")
	quietEnd, _ := autonomy.ParseLocalTime("03:00")
	policy.SelfTalk.QuietHours = autonomy.QuietHours{Start: quietStart, End: quietEnd}
	fixture.application.autonomyPolicy = &policy
	assembly, err := fixture.application.assembleAutonomous(ctx, fixture.repository, fixture.residentID, initiativeTrigger)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := fixture.application.submitWithContent(
		ctx, domain.PrepareAutonomousGenerationCommand(assembly.Prepare), assembly.Contents,
	); err != nil {
		t.Fatal(err)
	}
	if _, err := fixture.application.Ingress(ctx, "fresh self-talk activity"); err != nil {
		t.Fatal(err)
	}
	fixture.application.generator = &scriptedGenerator{steps: []generatorStep{
		{text: "fresh dialogue"}, {text: emptyM6Extraction},
	}}
	if err := fixture.application.ProcessResident(ctx, fixture.residentID); err != nil {
		t.Fatal(err)
	}
	if err := fixture.application.ProcessResident(ctx, fixture.residentID); err != nil {
		t.Fatal(err)
	}
	fixture.application.generator = &scriptedGenerator{steps: []generatorStep{{text: "higher priority private thought"}}}
	generator := fixture.application.generator.(*scriptedGenerator)
	advanceSelfTalkClock(fixture)
	if err := fixture.application.processAutonomyOnce(ctx, fixture.residentID); err != nil {
		t.Fatal(err)
	}
	requests := generator.Requests()
	if len(requests) != 1 || requests[0].Purpose != string(domain.GenerationPurposeSelfTalk) {
		t.Fatalf("recovery priority requests = %+v, want one self-talk request", requests)
	}
	var initiativeState string
	if err := fixture.store.Reader().QueryRowContext(ctx, `SELECT outcome.state
		FROM generation_run_outcomes outcome
		WHERE outcome.generation_run_id = ? ORDER BY outcome.outcome_id DESC LIMIT 1`,
		assembly.Prepare.Generation.RunID.String()).Scan(&initiativeState); err != nil {
		t.Fatal(err)
	}
	if initiativeState != "running" {
		t.Fatalf("recovered initiative state = %q, want running until self-talk turn completes", initiativeState)
	}
}

type failingDialogueDiscoveryRepository struct {
	*sqlite.CanonicalRepository
	failure error
	calls   int
}

func (repository *failingDialogueDiscoveryRepository) DiscoverDialogueWork(
	context.Context, canonical.ID, domain.DialogueDiscoveryRequest,
) (domain.DialogueDiscoveryResult, error) {
	repository.calls++
	return domain.DialogueDiscoveryResult{}, repository.failure
}

func TestM6I91DependencyStatusChangeCreatesReevaluationWithoutRewritingStageOrQuarantining(t *testing.T) {
	ctx := context.Background()
	direct := `{"claims":[{"grade":"stated","perspective":"resident","source_quote":"I am calm","statement":"The resident is calm.","subject":"resident","temporal_kind":"stable"}],"version":"memory-extraction-output-v1"}`
	meta := `{"claims":[{"grade":"stated","perspective":"source_actor","source_quote":"I see calm","statement":"The resident appears calm to the owner.","subject":"resident","temporal_kind":"stable"}],"version":"memory-extraction-output-v1"}`
	alignment := `{"aligned":true,"confidence":"800000","version":"memory-alignment-output-v1"}`
	generator := &scriptedGenerator{steps: []generatorStep{
		{text: "dialogue 1"}, {text: direct}, {text: "dialogue 2"}, {text: direct},
		{text: "dialogue 3"}, {text: direct}, {text: "dialogue 4"}, {text: meta},
		{text: "dialogue 5"}, {text: meta}, {text: alignment}, {text: "reevaluation thought"},
	}}
	fixture := newApplicationFixture(t, generator, 2)
	if _, err := fixture.application.activateMemoryPolicyV4ForTest(ctx, fixture.residentID); err != nil {
		t.Fatal(err)
	}
	for _, text := range []string{"I am calm", "I am calm", "I am calm", "I see calm", "I see calm"} {
		if _, err := fixture.application.Ingress(ctx, text); err != nil {
			t.Fatal(err)
		}
		if err := fixture.application.ProcessResident(ctx, fixture.residentID); err != nil {
			t.Fatal(err)
		}
	}
	drainForegroundToOptionalForTest(t, fixture)
	var dependencyRaw string
	if err := fixture.store.Reader().QueryRow(`SELECT dependency.dependency_claim_id
		FROM claim_stage_transition_dependencies dependency
		JOIN claim_stage_transitions stage ON stage.stage_transition_id = dependency.stage_transition_id
		WHERE stage.claim_id <> dependency.dependency_claim_id AND stage.to_stage = 'settled' LIMIT 1`).Scan(&dependencyRaw); err != nil {
		t.Fatal(err)
	}
	dependencyID, err := canonical.ParseID(dependencyRaw)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := fixture.application.SetMemoryClaimStatus(
		ctx, fixture.residentID, dependencyID, memory.StatusQuarantined, memory.HumanReasonQuarantine,
	); err != nil {
		t.Fatal(err)
	}
	policy := autonomy.DefaultPolicy("UTC")
	policy.SelfTalk.Enabled = true
	fixture.application.autonomyPolicy = &policy
	fixture.application.autonomySource = fixture.repository
	fixture.application.autonomyClock = testsupport.NewManualSchedulerClock(fixture.clock.Now())
	advanceSelfTalkClock(fixture)
	var stagesBefore, statusesBefore, findingsBefore int
	for destination, query := range map[*int]string{
		&stagesBefore:   `SELECT COUNT(*) FROM claim_stage_transitions`,
		&statusesBefore: `SELECT COUNT(*) FROM claim_status_transitions`,
		&findingsBefore: `SELECT COUNT(*) FROM integrity_findings`,
	} {
		if err := fixture.store.Reader().QueryRow(query).Scan(destination); err != nil {
			t.Fatal(err)
		}
	}
	if err := fixture.application.processAutonomyResident(ctx, fixture.residentID); err != nil {
		t.Fatal(err)
	}
	var runs, stagesAfter, statusesAfter, findingsAfter int
	if err := fixture.store.Reader().QueryRow(`SELECT COUNT(*) FROM generation_runs WHERE purpose = 'self_talk'`).Scan(&runs); err != nil {
		t.Fatal(err)
	}
	for destination, query := range map[*int]string{
		&stagesAfter:   `SELECT COUNT(*) FROM claim_stage_transitions`,
		&statusesAfter: `SELECT COUNT(*) FROM claim_status_transitions`,
		&findingsAfter: `SELECT COUNT(*) FROM integrity_findings`,
	} {
		if err := fixture.store.Reader().QueryRow(query).Scan(destination); err != nil {
			t.Fatal(err)
		}
	}
	if runs != 1 || stagesAfter != stagesBefore || statusesAfter != statusesBefore || findingsAfter != findingsBefore {
		t.Fatalf("reevaluation runs/stage/status/findings = %d %d/%d %d/%d %d/%d",
			runs, stagesBefore, stagesAfter, statusesBefore, statusesAfter, findingsBefore, findingsAfter)
	}
	remaining, err := fixture.repository.DiscoverReevaluationTriggers(ctx, fixture.residentID, 1)
	if err != nil {
		t.Fatal(err)
	}
	if len(remaining) != 0 {
		t.Fatalf("completed reevaluation key remained discoverable: %+v", remaining)
	}
}

func landedSelfTalkFixture(t *testing.T) applicationFixture {
	t.Helper()
	generator := &scriptedGenerator{steps: []generatorStep{
		{text: "dialogue reply"}, {text: emptyM6Extraction}, {text: "private thought"},
	}}
	fixture := newApplicationFixture(t, generator, 3)
	enableSelfTalkForTest(t, fixture)
	if _, err := fixture.application.Ingress(context.Background(), "seed activity"); err != nil {
		t.Fatal(err)
	}
	if err := fixture.application.ProcessResident(context.Background(), fixture.residentID); err != nil {
		t.Fatal(err)
	}
	// The first turn ends after its single fair mandatory-memory quantum. The
	// next turn completes the normal/re-extraction proof before autonomy runs.
	if err := fixture.application.ProcessResident(context.Background(), fixture.residentID); err != nil {
		t.Fatal(err)
	}
	advanceSelfTalkClock(fixture)
	if err := fixture.application.processAutonomyResident(context.Background(), fixture.residentID); err != nil {
		t.Fatal(err)
	}
	return fixture
}

func TestM7ForegroundDialogueBudgetParityAcrossSnapshotAndAutonomousWriters(t *testing.T) {
	ctx := context.Background()

	t.Run("snapshot and prepare", func(t *testing.T) {
		fixture, assembly := exhaustedDialogueAutonomyAssembly(t)
		for _, test := range []struct {
			maxAttempts int
			wantPending bool
		}{{maxAttempts: 1, wantPending: false}, {maxAttempts: 2, wantPending: true}} {
			snapshot, err := fixture.repository.AutonomySnapshot(ctx, autonomy.SnapshotRequest{
				ResidentID: fixture.residentID, WallNow: fixture.application.autonomyClock.Now().Wall,
				Timezone: canonical.MustTimezone("UTC"), MaxAttempts: test.maxAttempts,
			})
			if err != nil {
				t.Fatal(err)
			}
			if snapshot.ForegroundPending != test.wantPending {
				t.Fatalf("maxAttempts=%d foreground pending = %v, want %v", test.maxAttempts, snapshot.ForegroundPending, test.wantPending)
			}
		}
		if _, err := fixture.application.submitWithContent(
			ctx, domain.PrepareAutonomousGenerationCommand(assembly.Prepare), assembly.Contents,
		); err != nil {
			t.Fatalf("prepare with exhausted maxAttempts=1: %v", err)
		}

		blockedFixture, blockedAssembly := exhaustedDialogueAutonomyAssembly(t)
		blockedAssembly.Prepare.MaxAttempts = 2
		if _, err := blockedFixture.application.submitWithContent(
			ctx, domain.PrepareAutonomousGenerationCommand(blockedAssembly.Prepare), blockedAssembly.Contents,
		); err == nil || !strings.Contains(err.Error(), string(autonomy.BlockingForegroundPending)) {
			t.Fatalf("prepare maxAttempts=2 error = %v", err)
		}
		var runs int
		if err := blockedFixture.store.Reader().QueryRow(`SELECT COUNT(*) FROM generation_runs WHERE purpose = 'self_talk'`).Scan(&runs); err != nil {
			t.Fatal(err)
		}
		if runs != 0 {
			t.Fatalf("blocked prepare persisted %d autonomous runs", runs)
		}
	})

	t.Run("retry", func(t *testing.T) {
		for _, test := range []struct {
			maxAttempts int
			wantBlocked bool
		}{{maxAttempts: 1}, {maxAttempts: 2, wantBlocked: true}} {
			t.Run(fmt.Sprintf("max_%d", test.maxAttempts), func(t *testing.T) {
				fixture, assembly := exhaustedDialogueAutonomyAssembly(t)
				if _, err := fixture.application.submitWithContent(
					ctx, domain.PrepareAutonomousGenerationCommand(assembly.Prepare), assembly.Contents,
				); err != nil {
					t.Fatal(err)
				}
				prepared, err := fixture.repository.Generation(ctx, assembly.Prepare.Generation.RunID)
				if err != nil {
					t.Fatal(err)
				}
				if err := fixture.application.recordAutonomousForegroundPreempted(ctx, prepared); err != nil {
					t.Fatal(err)
				}
				outcomeID, err := fixture.application.ids.New()
				if err != nil {
					t.Fatal(err)
				}
				_, err = fixture.application.submit(ctx, domain.StartAttemptCommand(domain.Attempt{
					RunID: prepared.RunID, ResidentID: fixture.residentID, AttemptNo: 2,
					OutcomeID: outcomeID, MaxAttempts: test.maxAttempts,
				}))
				if test.wantBlocked {
					if err == nil || !strings.Contains(err.Error(), string(autonomy.BlockingForegroundPending)) {
						t.Fatalf("retry maxAttempts=%d error = %v", test.maxAttempts, err)
					}
				} else if err != nil {
					t.Fatalf("retry maxAttempts=%d: %v", test.maxAttempts, err)
				}
				var outcomes int
				if err := fixture.store.Reader().QueryRow(`SELECT COUNT(*) FROM generation_run_outcomes WHERE generation_run_id = ?`,
					prepared.RunID.String()).Scan(&outcomes); err != nil {
					t.Fatal(err)
				}
				want := 3
				if test.wantBlocked {
					want = 2
				}
				if outcomes != want {
					t.Fatalf("retry maxAttempts=%d outcomes = %d, want %d", test.maxAttempts, outcomes, want)
				}
			})
		}
	})

	t.Run("landing", func(t *testing.T) {
		for _, test := range []struct {
			maxAttempts int
			wantBlocked bool
		}{{maxAttempts: 1}, {maxAttempts: 2, wantBlocked: true}} {
			t.Run(fmt.Sprintf("max_%d", test.maxAttempts), func(t *testing.T) {
				fixture, assembly := exhaustedDialogueAutonomyAssembly(t)
				if _, err := fixture.application.submitWithContent(
					ctx, domain.PrepareAutonomousGenerationCommand(assembly.Prepare), assembly.Contents,
				); err != nil {
					t.Fatal(err)
				}
				resident, err := fixture.repository.Resident(ctx, fixture.residentID)
				if err != nil {
					t.Fatal(err)
				}
				output, err := fixture.application.newContent(
					fixture.residentID, "generation_output", []byte("budget parity private thought"), "independent",
				)
				if err != nil {
					t.Fatal(err)
				}
				ids, err := fixture.application.allocateIDs(2)
				if err != nil {
					t.Fatal(err)
				}
				_, err = fixture.application.submit(ctx, domain.LandAutonomousEventCommand(domain.LandAutonomousEvent{
					Attempt: domain.Attempt{RunID: assembly.Prepare.Generation.RunID, ResidentID: fixture.residentID,
						AttemptNo: 1, OutcomeID: ids[0], MaxAttempts: test.maxAttempts},
					EventID: ids[1], ResidentPrincipalID: resident.ResidentPrincipalID, OwnerPrincipalID: resident.OwnerPrincipalID,
					Output: output, OccurredAt: canonical.InstantFromTime(fixture.clock.Now()), OccurredTZ: canonical.MustTimezone("UTC"),
					Trigger: assembly.Prepare.Trigger, Policy: assembly.Prepare.Policy,
					ProjectionMaxStaleness: assembly.Prepare.ProjectionMaxStaleness,
					ProjectionEvidence:     assembly.Prepare.ProjectionEvidence,
				}))
				if test.wantBlocked {
					if err == nil || !strings.Contains(err.Error(), string(autonomy.BlockingForegroundPending)) {
						t.Fatalf("landing maxAttempts=%d error = %v", test.maxAttempts, err)
					}
				} else if err != nil {
					t.Fatalf("landing maxAttempts=%d: %v", test.maxAttempts, err)
				}
				var events, succeeded int
				if err := fixture.store.Reader().QueryRow(`SELECT COUNT(*) FROM events WHERE generation_run_id = ?`,
					assembly.Prepare.Generation.RunID.String()).Scan(&events); err != nil {
					t.Fatal(err)
				}
				if err := fixture.store.Reader().QueryRow(`SELECT COUNT(*) FROM generation_run_outcomes WHERE generation_run_id = ? AND state = 'succeeded'`,
					assembly.Prepare.Generation.RunID.String()).Scan(&succeeded); err != nil {
					t.Fatal(err)
				}
				want := 1
				if test.wantBlocked {
					want = 0
				}
				if events != want || succeeded != want {
					t.Fatalf("landing maxAttempts=%d event/succeeded = %d/%d, want %d/%d",
						test.maxAttempts, events, succeeded, want, want)
				}
			})
		}
	})
}

func TestM7AutonomousErasedSourceCancellationIsExactlyOnce(t *testing.T) {
	fixture, assembly, _ := prepareRunningSelfTalkFixture(t)
	ctx := context.Background()
	prepared, err := fixture.repository.Generation(ctx, assembly.Prepare.Generation.RunID)
	if err != nil {
		t.Fatal(err)
	}
	for call := 1; call <= 2; call++ {
		err := fixture.application.recordAutonomousLandingOrPreemption(
			ctx, prepared, fixture.application.captureForegroundEpoch(fixture.residentID), domain.ErrClaimSourceIneligible,
		)
		if !errors.Is(err, domain.ErrClaimSourceIneligible) {
			t.Fatalf("call %d error = %v", call, err)
		}
	}
	var outcomes, cancelled, events int
	var code string
	if err := fixture.store.Reader().QueryRow(`SELECT COUNT(*),
		SUM(CASE WHEN state = 'cancelled' AND error_class = ? THEN 1 ELSE 0 END)
		FROM generation_run_outcomes WHERE generation_run_id = ?`,
		generation.MustOutcomeErrorCode(generation.ErrorSourceContentErased, 0).String(), prepared.RunID.String(),
	).Scan(&outcomes, &cancelled); err != nil {
		t.Fatal(err)
	}
	if err := fixture.store.Reader().QueryRow(`SELECT error_class FROM generation_run_outcomes
		WHERE generation_run_id = ? AND state = 'cancelled'`, prepared.RunID.String()).Scan(&code); err != nil {
		t.Fatal(err)
	}
	if err := fixture.store.Reader().QueryRow(`SELECT COUNT(*) FROM events WHERE generation_run_id = ?`,
		prepared.RunID.String()).Scan(&events); err != nil {
		t.Fatal(err)
	}
	if outcomes != 2 || cancelled != 1 || events != 0 || code != string(generation.ErrorSourceContentErased) {
		t.Fatalf("erased-source outcomes/cancelled/events/code = %d/%d/%d/%q", outcomes, cancelled, events, code)
	}
}

func TestM7AutonomousErasedSourceCancellationRejectsStaleAttempt(t *testing.T) {
	fixture, assembly, _ := prepareRunningSelfTalkFixture(t)
	ctx := context.Background()
	prepared, err := fixture.repository.Generation(ctx, assembly.Prepare.Generation.RunID)
	if err != nil {
		t.Fatal(err)
	}
	if err := fixture.application.recordAutonomousFailure(
		ctx, prepared, "failed", generation.MustOutcomeErrorCode(generation.ErrorTimeout, 0),
	); err != nil {
		t.Fatal(err)
	}
	retryID, err := fixture.application.ids.New()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := fixture.application.submit(ctx, domain.StartAttemptCommand(domain.Attempt{
		RunID: prepared.RunID, ResidentID: prepared.ResidentID,
		AttemptNo: prepared.AttemptNo + 1, OutcomeID: retryID,
		MaxAttempts: fixture.application.maxAttempts,
	})); err != nil {
		t.Fatal(err)
	}
	err = fixture.application.recordAutonomousLandingOrPreemption(
		ctx, prepared, fixture.application.captureForegroundEpoch(fixture.residentID),
		domain.ErrClaimSourceIneligible,
	)
	if !errors.Is(err, domain.ErrClaimSourceIneligible) ||
		!strings.Contains(err.Error(), "conflicts with latest attempt") {
		t.Fatalf("stale erased-source landing error = %v", err)
	}
	var latestAttempt int64
	var latestState string
	var erasedCancellations int
	if err := fixture.store.Reader().QueryRow(`SELECT attempt_no, state FROM generation_run_outcomes
		WHERE generation_run_id = ? ORDER BY attempt_no DESC,
		CASE WHEN state = 'running' THEN 0 ELSE 1 END DESC LIMIT 1`, prepared.RunID.String()).Scan(
		&latestAttempt, &latestState,
	); err != nil {
		t.Fatal(err)
	}
	if err := fixture.store.Reader().QueryRow(`SELECT COUNT(*) FROM generation_run_outcomes
		WHERE generation_run_id = ? AND state = 'cancelled' AND error_class = 'source_content_erased'`,
		prepared.RunID.String()).Scan(&erasedCancellations); err != nil {
		t.Fatal(err)
	}
	if latestAttempt != prepared.AttemptNo+1 || latestState != "running" || erasedCancellations != 0 {
		t.Fatalf("stale cancellation mutated latest attempt/state/count = %d/%s/%d",
			latestAttempt, latestState, erasedCancellations)
	}
}

func TestM7AutonomousRetryStartUsesExactMaxAttempts(t *testing.T) {
	for _, test := range []struct {
		name        string
		maxAttempts int
		wantStarted bool
	}{
		{name: "exhausted", maxAttempts: 1},
		{name: "eligible control", maxAttempts: 2, wantStarted: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			fixture, assembly, _ := prepareRunningSelfTalkFixture(t)
			ctx := context.Background()
			prepared, err := fixture.repository.Generation(ctx, assembly.Prepare.Generation.RunID)
			if err != nil {
				t.Fatal(err)
			}
			if err := fixture.application.recordAutonomousFailure(
				ctx, prepared, "failed", generation.MustOutcomeErrorCode(generation.ErrorTimeout, 0),
			); err != nil {
				t.Fatal(err)
			}
			retryID, err := fixture.application.ids.New()
			if err != nil {
				t.Fatal(err)
			}
			_, err = fixture.application.submit(ctx, domain.StartAttemptCommand(domain.Attempt{
				RunID: prepared.RunID, ResidentID: prepared.ResidentID,
				AttemptNo: prepared.AttemptNo + 1, OutcomeID: retryID, MaxAttempts: test.maxAttempts,
			}))
			if test.wantStarted {
				if err != nil {
					t.Fatalf("eligible retry start: %v", err)
				}
			} else if err == nil || !strings.Contains(err.Error(), "not eligible under maxAttempts=1") {
				t.Fatalf("exhausted retry start error = %v", err)
			}
			var outcomes, attemptTwoRunning int
			if err := fixture.store.Reader().QueryRow(`SELECT COUNT(*),
				SUM(CASE WHEN attempt_no = 2 AND state = 'running' THEN 1 ELSE 0 END)
				FROM generation_run_outcomes WHERE generation_run_id = ?`, prepared.RunID.String()).Scan(
				&outcomes, &attemptTwoRunning,
			); err != nil {
				t.Fatal(err)
			}
			wantOutcomes, wantRunning := 2, 0
			if test.wantStarted {
				wantOutcomes, wantRunning = 3, 1
			}
			if outcomes != wantOutcomes || attemptTwoRunning != wantRunning {
				t.Fatalf("outcomes/attempt2-running = %d/%d, want %d/%d",
					outcomes, attemptTwoRunning, wantOutcomes, wantRunning)
			}
		})
	}
}

func TestM7AutonomousErasedSourceCancellationFailureJoinsCauseAndLeavesRunning(t *testing.T) {
	fixture, assembly, _ := prepareRunningSelfTalkFixture(t)
	ctx := context.Background()
	prepared, err := fixture.repository.Generation(ctx, assembly.Prepare.Generation.RunID)
	if err != nil {
		t.Fatal(err)
	}
	if err := fixture.writer.Close(ctx); err != nil {
		t.Fatal(err)
	}
	err = fixture.application.recordAutonomousLandingOrPreemption(
		ctx, prepared, fixture.application.captureForegroundEpoch(fixture.residentID), domain.ErrClaimSourceIneligible,
	)
	if !errors.Is(err, domain.ErrClaimSourceIneligible) || !errors.Is(err, canonical.ErrWriterClosed) {
		t.Fatalf("joined cancellation error = %v", err)
	}
	var rows, running int
	if err := fixture.store.Reader().QueryRow(`SELECT COUNT(*), SUM(CASE WHEN state = 'running' THEN 1 ELSE 0 END)
		FROM generation_run_outcomes WHERE generation_run_id = ?`, prepared.RunID.String()).Scan(&rows, &running); err != nil {
		t.Fatal(err)
	}
	if rows != 1 || running != 1 {
		t.Fatalf("failed cancellation rows/running = %d/%d, want 1/1 for startup recovery", rows, running)
	}
}

func TestM7AutonomousAmbiguousErasedSourceLandingLeavesRunningForRecovery(t *testing.T) {
	fixture, assembly, _ := prepareRunningSelfTalkFixture(t)
	ctx := context.Background()
	prepared, err := fixture.repository.Generation(ctx, assembly.Prepare.Generation.RunID)
	if err != nil {
		t.Fatal(err)
	}
	cause := errors.Join(domain.ErrClaimSourceIneligible, canonical.ErrWriterPoisoned)
	err = fixture.application.recordAutonomousLandingOrPreemption(
		ctx, prepared, fixture.application.captureForegroundEpoch(fixture.residentID), cause,
	)
	if !errors.Is(err, domain.ErrClaimSourceIneligible) || !errors.Is(err, canonical.ErrWriterPoisoned) {
		t.Fatalf("ambiguous landing error = %v", err)
	}
	var rows, running, terminal, events int
	if err := fixture.store.Reader().QueryRow(`SELECT COUNT(*),
		SUM(CASE WHEN state = 'running' THEN 1 ELSE 0 END),
		SUM(CASE WHEN state IN ('succeeded', 'failed', 'cancelled') THEN 1 ELSE 0 END)
		FROM generation_run_outcomes WHERE generation_run_id = ?`, prepared.RunID.String()).Scan(
		&rows, &running, &terminal,
	); err != nil {
		t.Fatal(err)
	}
	if err := fixture.store.Reader().QueryRow(`SELECT COUNT(*) FROM events WHERE generation_run_id = ?`,
		prepared.RunID.String()).Scan(&events); err != nil {
		t.Fatal(err)
	}
	if rows != 1 || running != 1 || terminal != 0 || events != 0 {
		t.Fatalf("ambiguous landing rows/running/terminal/events = %d/%d/%d/%d", rows, running, terminal, events)
	}
}

func TestM7AutonomousProviderLandingRaceErasesClaimAndCancelsSameAttemptExactlyOnce(t *testing.T) {
	race := runM7AutonomousProviderLandingErasureRace(t, false)
	fixture, prepared := race.fixture, race.prepared
	ctx := context.Background()
	if race.erasureErr != nil {
		t.Fatalf("provider-race erasure: %v", race.erasureErr)
	}
	if !errors.Is(race.landingErr, domain.ErrClaimSourceIneligible) {
		t.Fatalf("provider-race landing error = %v", race.landingErr)
	}
	// A duplicate handling pass models restart/ambiguous-delivery replay. The
	// source-erased terminal code is idempotent only for this same attempt.
	if err := fixture.application.recordAutonomousLandingOrPreemption(
		ctx, prepared, fixture.application.captureForegroundEpoch(fixture.residentID), domain.ErrClaimSourceIneligible,
	); !errors.Is(err, domain.ErrClaimSourceIneligible) {
		t.Fatalf("duplicate race handling error = %v", err)
	}
	var rows, cancelled, succeeded, events int
	var attempt int64
	var state, code string
	if err := fixture.store.Reader().QueryRow(`SELECT COUNT(*),
		SUM(CASE WHEN state = 'cancelled' AND error_class = ? THEN 1 ELSE 0 END),
		SUM(CASE WHEN state = 'succeeded' THEN 1 ELSE 0 END)
		FROM generation_run_outcomes WHERE generation_run_id = ?`, string(generation.ErrorSourceContentErased),
		prepared.RunID.String()).Scan(&rows, &cancelled, &succeeded); err != nil {
		t.Fatal(err)
	}
	if err := fixture.store.Reader().QueryRow(`SELECT attempt_no, state, error_class
		FROM generation_run_outcomes WHERE generation_run_id = ? ORDER BY outcome_id DESC LIMIT 1`,
		prepared.RunID.String()).Scan(&attempt, &state, &code); err != nil {
		t.Fatal(err)
	}
	if err := fixture.store.Reader().QueryRow(`SELECT COUNT(*) FROM events WHERE generation_run_id = ?`,
		prepared.RunID.String()).Scan(&events); err != nil {
		t.Fatal(err)
	}
	if race.generator.CallCount() != 1 || rows != 2 || cancelled != 1 || succeeded != 0 || events != 0 ||
		attempt != prepared.AttemptNo || state != "cancelled" || code != string(generation.ErrorSourceContentErased) {
		t.Fatalf("race calls/rows/cancelled/succeeded/events/latest = %d/%d/%d/%d/%d %d/%s/%s",
			race.generator.CallCount(), rows, cancelled, succeeded, events, attempt, state, code)
	}
}

func TestM7AutonomousProviderLandingRaceCancellationFailureJoinsAndLeavesRunning(t *testing.T) {
	race := runM7AutonomousProviderLandingErasureRace(t, true)
	if race.erasureErr != nil {
		t.Fatalf("provider-race erasure: %v", race.erasureErr)
	}
	if !errors.Is(race.landingErr, domain.ErrClaimSourceIneligible) ||
		!errors.Is(race.landingErr, errM7AutonomyCancelWriterInjected) {
		t.Fatalf("joined provider-race cancellation error = %v", race.landingErr)
	}
	var rows, running, terminal, events int
	if err := race.fixture.store.Reader().QueryRow(`SELECT COUNT(*),
		SUM(CASE WHEN state = 'running' THEN 1 ELSE 0 END),
		SUM(CASE WHEN state IN ('succeeded', 'failed', 'cancelled') THEN 1 ELSE 0 END)
		FROM generation_run_outcomes WHERE generation_run_id = ?`, race.prepared.RunID.String()).Scan(
		&rows, &running, &terminal,
	); err != nil {
		t.Fatal(err)
	}
	if err := race.fixture.store.Reader().QueryRow(`SELECT COUNT(*) FROM events WHERE generation_run_id = ?`,
		race.prepared.RunID.String()).Scan(&events); err != nil {
		t.Fatal(err)
	}
	if race.generator.CallCount() != 1 || rows != 1 || running != 1 || terminal != 0 || events != 0 {
		t.Fatalf("failed race calls/rows/running/terminal/events = %d/%d/%d/%d/%d",
			race.generator.CallCount(), rows, running, terminal, events)
	}
}

type m7AutonomousErasureRace struct {
	fixture    applicationFixture
	prepared   domain.PreparedGeneration
	generator  *scriptedGenerator
	erasureErr error
	landingErr error
}

func runM7AutonomousProviderLandingErasureRace(t *testing.T, failCancellation bool) m7AutonomousErasureRace {
	t.Helper()
	fixture, _, trigger := initiativeReadyForRaceTest(t)
	ctx := context.Background()
	assembly, err := fixture.application.assembleAutonomous(ctx, fixture.repository, fixture.residentID, trigger)
	if err != nil {
		t.Fatal(err)
	}
	if len(assembly.Prepare.Usages) == 0 {
		t.Fatal("initiative race assembly has no frozen claim usage")
	}
	if _, err := fixture.application.submitWithContent(
		ctx, domain.PrepareAutonomousGenerationCommand(assembly.Prepare), assembly.Contents,
	); err != nil {
		t.Fatal(err)
	}
	prepared, err := fixture.repository.Generation(ctx, assembly.Prepare.Generation.RunID)
	if err != nil {
		t.Fatal(err)
	}
	resident, err := fixture.repository.Resident(ctx, fixture.residentID)
	if err != nil {
		t.Fatal(err)
	}
	erasureIDs, err := fixture.application.allocateIDs(2)
	if err != nil {
		t.Fatal(err)
	}
	var backend *m7AutonomyArmableBeginFailureBackend
	if failCancellation {
		backend = installM7AutonomyArmableBeginFailureBackend(t, fixture)
	}
	race := m7AutonomousErasureRace{fixture: fixture, prepared: prepared}
	race.generator = &scriptedGenerator{steps: []generatorStep{{
		text: "must roll back after claim erasure",
		beforeReturn: func() {
			_, race.erasureErr = fixture.application.submit(ctx, domain.EraseClaimStatementCommand(domain.EraseClaimStatement{
				ResidentID: fixture.residentID, ClaimID: assembly.Prepare.Usages[0].ClaimID,
				ClaimStatementErasureEventID: erasureIDs[0], ContentErasureEventID: erasureIDs[1],
				ActorPrincipalID: resident.OwnerPrincipalID, ReasonCode: "m7_autonomy_landing_race",
				OccurredAt: canonical.InstantFromTime(fixture.clock.Now()), OccurredTZ: canonical.MustTimezone("UTC"),
			}))
			if race.erasureErr == nil && backend != nil {
				backend.Arm()
			}
		},
	}}}
	fixture.application.generator = race.generator
	race.landingErr = fixture.application.callAndLandAutonomous(ctx, prepared, trigger)
	return race
}

var errM7AutonomyCancelWriterInjected = errors.New("app test: injected autonomous erased-source cancellation writer failure")

type m7AutonomyArmableBeginFailureBackend struct {
	delegate canonical.Backend

	mu         sync.Mutex
	armed      bool
	afterArmed int
}

func (backend *m7AutonomyArmableBeginFailureBackend) LoadHead(ctx context.Context) (canonical.Head, error) {
	return backend.delegate.LoadHead(ctx)
}

func (backend *m7AutonomyArmableBeginFailureBackend) Begin(
	ctx context.Context,
	metadata canonical.CommitMetadata,
) (canonical.CanonicalUoW, error) {
	backend.mu.Lock()
	if backend.armed {
		backend.afterArmed++
		if backend.afterArmed == 2 {
			backend.mu.Unlock()
			return nil, errM7AutonomyCancelWriterInjected
		}
	}
	backend.mu.Unlock()
	return backend.delegate.Begin(ctx, metadata)
}

func (backend *m7AutonomyArmableBeginFailureBackend) Arm() {
	backend.mu.Lock()
	backend.armed = true
	backend.afterArmed = 0
	backend.mu.Unlock()
}

func installM7AutonomyArmableBeginFailureBackend(
	t *testing.T,
	fixture applicationFixture,
) *m7AutonomyArmableBeginFailureBackend {
	t.Helper()
	ctx := context.Background()
	if err := fixture.application.writer.Close(ctx); err != nil {
		t.Fatal(err)
	}
	backend := &m7AutonomyArmableBeginFailureBackend{delegate: fixture.store.Canonical()}
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
			t.Errorf("close M7 autonomy fault writer: %v", err)
		}
	})
	return backend
}

func exhaustedDialogueAutonomyAssembly(t *testing.T) (applicationFixture, autonomousAssembly) {
	t.Helper()
	fixture := newApplicationFixture(t, &scriptedGenerator{steps: []generatorStep{{
		err: &generation.ProviderError{Class: generation.ErrorTimeout, Detail: "retryable foreground"},
	}}}, 1)
	enableSelfTalkForTest(t, fixture)
	ctx := context.Background()
	if _, err := fixture.application.Ingress(ctx, "foreground failed once"); err != nil {
		t.Fatal(err)
	}
	if err := fixture.application.ProcessResident(ctx, fixture.residentID); err != nil {
		t.Fatal(err)
	}
	var foregroundError string
	if err := fixture.store.Reader().QueryRow(`SELECT outcome.error_class
		FROM generation_run_outcomes outcome
		JOIN generation_runs run ON run.generation_run_id = outcome.generation_run_id
		WHERE run.resident_id = ? AND run.purpose = 'dialogue' AND outcome.state = 'failed'`,
		fixture.residentID.String()).Scan(&foregroundError); err != nil {
		t.Fatal(err)
	}
	if foregroundError != string(generation.ErrorTimeout) {
		t.Fatalf("foreground attempt 1 error = %q, want provider_timeout", foregroundError)
	}
	advanceSelfTalkClock(fixture)
	snapshot, err := fixture.repository.AutonomySnapshot(ctx, autonomy.SnapshotRequest{
		ResidentID: fixture.residentID, WallNow: fixture.application.autonomyClock.Now().Wall,
		Timezone: canonical.MustTimezone("UTC"), MaxAttempts: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	if snapshot.LastUser == nil || snapshot.ForegroundPending {
		t.Fatalf("exhausted dialogue snapshot = %+v", snapshot)
	}
	trigger := autonomy.Trigger{Kind: autonomy.TriggerIdle, SourceID: snapshot.LastUser.ID,
		Ordinal: snapshot.ConsecutiveSelfTalk + 1}
	assembly, err := fixture.application.assembleAutonomous(ctx, fixture.repository, fixture.residentID, trigger)
	if err != nil {
		t.Fatal(err)
	}
	return fixture, assembly
}

func prepareRunningSelfTalkFixture(t *testing.T) (applicationFixture, autonomousAssembly, autonomy.Trigger) {
	t.Helper()
	fixture := newApplicationFixture(t, &scriptedGenerator{steps: []generatorStep{
		{text: "dialogue reply"}, {text: emptyM6Extraction},
	}}, 2)
	enableSelfTalkForTest(t, fixture)
	ctx := context.Background()
	if _, err := fixture.application.Ingress(ctx, "seed running autonomy"); err != nil {
		t.Fatal(err)
	}
	if err := fixture.application.ProcessResident(ctx, fixture.residentID); err != nil {
		t.Fatal(err)
	}
	if err := fixture.application.ProcessResident(ctx, fixture.residentID); err != nil {
		t.Fatal(err)
	}
	advanceSelfTalkClock(fixture)
	snapshot, err := fixture.repository.AutonomySnapshot(ctx, autonomy.SnapshotRequest{
		ResidentID: fixture.residentID, WallNow: fixture.application.autonomyClock.Now().Wall,
		Timezone: canonical.MustTimezone("UTC"), MaxAttempts: fixture.application.maxAttempts,
	})
	if err != nil {
		t.Fatal(err)
	}
	if snapshot.LastUser == nil {
		t.Fatal("autonomy snapshot has no last user")
	}
	trigger := autonomy.Trigger{Kind: autonomy.TriggerIdle, SourceID: snapshot.LastUser.ID,
		Ordinal: snapshot.ConsecutiveSelfTalk + 1}
	assembly, err := fixture.application.assembleAutonomous(ctx, fixture.repository, fixture.residentID, trigger)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := fixture.application.submitWithContent(
		ctx, domain.PrepareAutonomousGenerationCommand(assembly.Prepare), assembly.Contents,
	); err != nil {
		t.Fatal(err)
	}
	return fixture, assembly, trigger
}

func enableSelfTalkForTest(t *testing.T, fixture applicationFixture) {
	t.Helper()
	if _, err := fixture.application.activateMemoryPolicyV4ForTest(context.Background(), fixture.residentID); err != nil {
		t.Fatal(err)
	}
	policy := autonomy.DefaultPolicy("UTC")
	policy.SelfTalk.Enabled = true
	fixture.application.autonomyPolicy = &policy
	fixture.application.autonomySource = fixture.repository
	fixture.application.autonomyClock = testsupport.NewManualSchedulerClock(fixture.clock.Now())
}

func advanceSelfTalkClock(fixture applicationFixture) {
	advance := fixture.application.autonomyPolicy.SelfTalk.Interval + time.Second
	fixture.clock.Advance(advance)
	schedulerClock := fixture.application.autonomyClock.(*testsupport.ManualSchedulerClock)
	schedulerClock.AdvanceWall(advance)
	schedulerClock.AdvanceMonotonic(advance)
}
