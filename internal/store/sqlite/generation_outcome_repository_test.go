package sqlite

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"math"
	"testing"

	"mahoroba.local/mahoroba/internal/autonomy"
	"mahoroba.local/mahoroba/internal/canonical"
	"mahoroba.local/mahoroba/internal/domain"
	"mahoroba.local/mahoroba/internal/generation"
)

func TestM7GenerationOutcomeRowLegalityMatrix(t *testing.T) {
	validID := mustOutcomeTestID(t, "01ARZ3NDEKTSV4RRFFQ69G5FAT")
	commitID := mustOutcomeTestID(t, "01ARZ3NDEKTSV4RRFFQ69G5FAV")
	contentID := mustOutcomeTestID(t, "01ARZ3NDEKTSV4RRFFQ69G5FAW")
	base := func() generationOutcome {
		return generationOutcome{
			ID: validID, CanonicalCommitID: commitID, AttemptNo: 1, State: "running",
			RecordedAt: canonical.Instant(1), RecordedTZ: canonical.MustTimezone("UTC"),
		}
	}
	tests := []struct {
		name  string
		build func() generationOutcome
		valid bool
	}{
		{name: "running", build: base, valid: true},
		{name: "succeeded", build: func() generationOutcome {
			row := base()
			row.State, row.OutputContentID = "succeeded", &contentID
			row.PromptTokens, row.CompletionTokens = sql.NullInt64{Int64: 0, Valid: true}, sql.NullInt64{Int64: 2, Valid: true}
			row.Latency, row.EstimatedCost = sql.NullInt64{Int64: 3, Valid: true}, sql.NullInt64{Int64: 0, Valid: true}
			return row
		}, valid: true},
		{name: "failed with detail", build: func() generationOutcome {
			row := base()
			row.State = "failed"
			row.ErrorClass = sql.NullString{String: generation.MustOutcomeErrorCode(generation.ErrorTransport, 0).String(), Valid: true}
			row.ErrorDetailContentID = &contentID
			return row
		}, valid: true},
		{name: "cancelled", build: func() generationOutcome {
			row := base()
			row.State = "cancelled"
			row.ErrorClass = sql.NullString{String: generation.MustOutcomeErrorCode(generation.ErrorForegroundPreempted, 0).String(), Valid: true}
			return row
		}, valid: true},
		{name: "synthetic erased cancellation", build: func() generationOutcome {
			row := base()
			row.AttemptNo, row.State = 0, "cancelled"
			row.ErrorClass = sql.NullString{String: generation.MustOutcomeErrorCode(generation.ErrorSourceContentErased, 0).String(), Valid: true}
			return row
		}, valid: true},
		{name: "synthetic inactive cancellation", build: func() generationOutcome {
			row := base()
			row.AttemptNo, row.State = 0, "cancelled"
			row.ErrorClass = sql.NullString{String: generation.MustOutcomeErrorCode(generation.ErrorResidentInactive, 0).String(), Valid: true}
			return row
		}, valid: true},
		{name: "synthetic unselected cancellation", build: func() generationOutcome {
			row := base()
			row.AttemptNo, row.State = 0, "cancelled"
			row.ErrorClass = sql.NullString{String: generation.MustOutcomeErrorCode(generation.ErrorResidentUnselected, 0).String(), Valid: true}
			return row
		}, valid: true},
		{name: "missing identity", build: func() generationOutcome { row := base(); row.ID = canonical.ID{}; return row }},
		{name: "missing recorded time", build: func() generationOutcome { row := base(); row.RecordedAt = 0; return row }},
		{name: "invalid timezone", build: func() generationOutcome { row := base(); row.RecordedTZ = "Mars/Olympus"; return row }},
		{name: "negative metric", build: func() generationOutcome {
			row := base()
			row.PromptTokens = sql.NullInt64{Int64: -1, Valid: true}
			return row
		}},
		{name: "synthetic wrong state", build: func() generationOutcome {
			row := base()
			row.AttemptNo, row.State = 0, "failed"
			row.ErrorClass = sql.NullString{String: string(generation.ErrorSourceContentErased), Valid: true}
			return row
		}},
		{name: "synthetic wrong code", build: func() generationOutcome {
			row := base()
			row.AttemptNo, row.State = 0, "cancelled"
			row.ErrorClass = sql.NullString{String: string(generation.ErrorTransport), Valid: true}
			return row
		}},
		{name: "synthetic metric", build: func() generationOutcome {
			row := base()
			row.AttemptNo, row.State = 0, "cancelled"
			row.ErrorClass = sql.NullString{String: string(generation.ErrorResidentInactive), Valid: true}
			row.Latency = sql.NullInt64{Int64: 1, Valid: true}
			return row
		}},
		{name: "negative attempt", build: func() generationOutcome { row := base(); row.AttemptNo = -1; return row }},
		{name: "running terminal field", build: func() generationOutcome { row := base(); row.OutputContentID = &contentID; return row }},
		{name: "running error detail", build: func() generationOutcome { row := base(); row.ErrorDetailContentID = &contentID; return row }},
		{name: "running metric", build: func() generationOutcome {
			row := base()
			row.Latency = sql.NullInt64{Int64: 1, Valid: true}
			return row
		}},
		{name: "succeeded without output", build: func() generationOutcome { row := base(); row.State = "succeeded"; return row }},
		{name: "succeeded with error", build: func() generationOutcome {
			row := base()
			row.State, row.OutputContentID = "succeeded", &contentID
			row.ErrorClass = sql.NullString{String: string(generation.ErrorTransport), Valid: true}
			return row
		}},
		{name: "succeeded with error detail", build: func() generationOutcome {
			row := base()
			row.State, row.OutputContentID, row.ErrorDetailContentID = "succeeded", &contentID, &contentID
			return row
		}},
		{name: "zero output identity", build: func() generationOutcome {
			row := base()
			zero := canonical.ID{}
			row.State, row.OutputContentID = "succeeded", &zero
			return row
		}},
		{name: "failed without error", build: func() generationOutcome { row := base(); row.State = "failed"; return row }},
		{name: "failed with output", build: func() generationOutcome {
			row := base()
			row.State, row.OutputContentID = "failed", &contentID
			row.ErrorClass = sql.NullString{String: string(generation.ErrorTransport), Valid: true}
			return row
		}},
		{name: "failed with metrics", build: func() generationOutcome {
			row := base()
			row.State = "failed"
			row.ErrorClass = sql.NullString{String: string(generation.ErrorTransport), Valid: true}
			row.Latency = sql.NullInt64{Int64: 1, Valid: true}
			return row
		}},
		{name: "cancelled with metrics", build: func() generationOutcome {
			row := base()
			row.State = "cancelled"
			row.ErrorClass = sql.NullString{String: string(generation.ErrorForegroundPreempted), Valid: true}
			row.Latency = sql.NullInt64{Int64: 1, Valid: true}
			return row
		}},
		{name: "zero error detail identity", build: func() generationOutcome {
			row := base()
			zero := canonical.ID{}
			row.State = "failed"
			row.ErrorClass = sql.NullString{String: string(generation.ErrorTransport), Valid: true}
			row.ErrorDetailContentID = &zero
			return row
		}},
		{name: "failed with invalid code", build: func() generationOutcome {
			row := base()
			row.State = "failed"
			row.ErrorClass = sql.NullString{String: "unknown", Valid: true}
			return row
		}},
		{name: "unknown state", build: func() generationOutcome { row := base(); row.State = "queued"; return row }},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			err := validateGenerationOutcomeRow(test.build())
			if test.valid && err != nil {
				t.Fatalf("valid row rejected: %v", err)
			}
			if !test.valid && !errors.Is(err, ErrInvalidGenerationOutcomeHistory) {
				t.Fatalf("illegal row error = %v", err)
			}
		})
	}
}

func TestM7GenerationOutcomeRepositoryRejectsInvalidHistory(t *testing.T) {
	t.Run("attempt zero", func(t *testing.T) {
		fixture, closeFixture := newSemanticFixture(t)
		defer closeFixture()
		insertTestOutcome(t, fixture, 0, "failed")
		if _, err := (generationOutcomeRepository{}).Latest(context.Background(), fixture.db,
			mustParseSnapshotID(fixture.run["A"]), mustParseSnapshotID(fixture.resident["A"])); err == nil {
			t.Fatal("attempt zero was accepted")
		}
	})
	t.Run("attempt gap", func(t *testing.T) {
		fixture, closeFixture := newSemanticFixture(t)
		defer closeFixture()
		insertTestOutcome(t, fixture, 1, "failed")
		insertTestOutcome(t, fixture, 3, "failed")
		if _, err := (generationOutcomeRepository{}).Latest(context.Background(), fixture.db,
			mustParseSnapshotID(fixture.run["A"]), mustParseSnapshotID(fixture.resident["A"])); err == nil {
			t.Fatal("attempt gap was accepted")
		}
	})
	t.Run("older running attempt", func(t *testing.T) {
		fixture, closeFixture := newSemanticFixture(t)
		defer closeFixture()
		insertTestOutcome(t, fixture, 1, "running")
		insertTestOutcome(t, fixture, 2, "failed")
		if _, err := (generationOutcomeRepository{}).Latest(context.Background(), fixture.db,
			mustParseSnapshotID(fixture.run["A"]), mustParseSnapshotID(fixture.resident["A"])); err == nil {
			t.Fatal("older running attempt was accepted")
		}
	})
}

func TestM7OutcomeSummaryClassifierChargesForegroundPreemptionSeparately(t *testing.T) {
	preempted := generation.MustOutcomeErrorCode(generation.ErrorForegroundPreempted, 0).String()
	summary := generationOutcomeSummary{
		Latest:            generationOutcome{State: "failed", ErrorClass: sql.NullString{String: preempted, Valid: true}},
		ChargedRetryCount: 1,
	}
	classified, err := summary.Classify(1)
	if err != nil {
		t.Fatal(err)
	}
	if classified.ClassifiedState != domain.WorkRetryPending || !classified.RetryEligible || !classified.LatestForegroundPreempted {
		t.Fatalf("foreground preemption classification = %+v", classified)
	}
	if classified.ChargedRetryCount != 1 {
		t.Fatalf("charged retry count = %d", classified.ChargedRetryCount)
	}
	if _, err := summary.Classify(0); err == nil {
		t.Fatal("zero maxAttempts was accepted by classifier")
	} else if !errors.Is(err, ErrInvalidGenerationOutcomeHistory) {
		t.Fatalf("zero maxAttempts error = %v", err)
	}
}

func TestM7OutcomeSummaryClassifierMatrix(t *testing.T) {
	retryable := generation.MustOutcomeErrorCode(generation.ErrorTimeout, 0).String()
	preempted := generation.MustOutcomeErrorCode(generation.ErrorForegroundPreempted, 0).String()
	nonRetryable := generation.MustOutcomeErrorCode(generation.ErrorInvalidResponse, 0).String()
	tests := []struct {
		name            string
		state, code     string
		charged, max    int64
		persisted       domain.WorkState
		classified      domain.WorkState
		retryEligible   bool
		latestPreempted bool
	}{
		{name: "running", state: "running", max: 1, persisted: domain.WorkRunning, classified: domain.WorkRunning},
		{name: "succeeded", state: "succeeded", max: 1, persisted: domain.WorkSucceeded, classified: domain.WorkSucceeded},
		{name: "attempt one exhausted at max one", state: "failed", code: retryable, charged: 1, max: 1,
			persisted: domain.WorkTerminalFailed, classified: domain.WorkTerminalFailed},
		{name: "attempt one retryable at max two", state: "failed", code: retryable, charged: 1, max: 2,
			persisted: domain.WorkTerminalFailed, classified: domain.WorkRetryPending, retryEligible: true},
		{name: "cancelled retryable", state: "cancelled", code: retryable, charged: 1, max: 2,
			persisted: domain.WorkTerminalFailed, classified: domain.WorkRetryPending, retryEligible: true},
		{name: "non retryable below budget", state: "failed", code: nonRetryable, charged: 0, max: 2,
			persisted: domain.WorkTerminalFailed, classified: domain.WorkTerminalFailed},
		{name: "foreground preemption ignores exhausted budget", state: "cancelled", code: preempted, charged: 7, max: 1,
			persisted: domain.WorkTerminalFailed, classified: domain.WorkRetryPending, retryEligible: true, latestPreempted: true},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			summary := generationOutcomeSummary{Latest: generationOutcome{State: test.state}, ChargedRetryCount: test.charged}
			if test.code != "" {
				summary.Latest.ErrorClass = sql.NullString{String: test.code, Valid: true}
			}
			got, err := summary.Classify(test.max)
			if err != nil {
				t.Fatal(err)
			}
			if got.PersistedState != test.persisted || got.ClassifiedState != test.classified ||
				got.RetryEligible != test.retryEligible || got.LatestForegroundPreempted != test.latestPreempted {
				t.Fatalf("classification = %+v", got)
			}
		})
	}
}

func TestM7PureOutcomeReducerRejectsIllegalSuccessorsAndAcceptsRetryChain(t *testing.T) {
	running := reducerOutcome(t, "01ARZ3NDEKTSV4RRFFQ69G5FAT", 1, "running", "")
	failed := reducerOutcome(t, "01ARZ3NDEKTSV4RRFFQ69G5FAV", 1, "failed", "provider_timeout")
	succeeded := reducerOutcome(t, "01ARZ3NDEKTSV4RRFFQ69G5FAW", 1, "succeeded", "")
	if _, err := reduceGenerationOutcomeHistory([]generationOutcome{running, succeeded, reducerOutcome(t, "01ARZ3NDEKTSV4RRFFQ69G5FAX", 2, "failed", "provider_timeout")}); err == nil {
		t.Fatal("success followed by retry was accepted")
	}
	if _, err := reduceGenerationOutcomeHistory([]generationOutcome{running, reducerOutcome(t, "01ARZ3NDEKTSV4RRFFQ69G5FAY", 1, "failed", "validation_error"), reducerOutcome(t, "01ARZ3NDEKTSV4RRFFQ69G5FAZ", 2, "running", "")}); err == nil {
		t.Fatal("non-retryable failure followed by retry was accepted")
	}
	latest, err := reduceGenerationOutcomeHistory([]generationOutcome{running, failed, reducerOutcome(t, "01ARZ3NDEKTSV4RRFFQ69G5FB0", 2, "running", "")})
	if err != nil || latest.AttemptNo != 2 || latest.State != "running" {
		t.Fatalf("valid retry chain = %+v, err=%v", latest, err)
	}
	if _, err := reduceGenerationOutcomeHistory([]generationOutcome{running, failed, reducerOutcome(t, "01ARZ3NDEKTSV4RRFFQ69G5FB1", 1, "failed", "provider_timeout")}); err == nil {
		t.Fatal("duplicate terminal attempt was accepted")
	}
	if _, err := reduceGenerationOutcomeHistory([]generationOutcome{reducerOutcome(t, "01ARZ3NDEKTSV4RRFFQ69G5FB2", 0, "running", "")}); err == nil {
		t.Fatal("pure reducer accepted attempt zero")
	}
}

func TestM7PureOutcomeReducerTransitionMatrix(t *testing.T) {
	running := func(id string, attempt int64) generationOutcome { return reducerOutcome(t, id, attempt, "running", "") }
	terminal := func(id string, attempt int64, state, code string) generationOutcome {
		return reducerOutcome(t, id, attempt, state, code)
	}
	transport := generation.MustOutcomeErrorCode(generation.ErrorTransport, 0).String()
	validation := generation.MustOutcomeErrorCode(generation.ErrorInvalidResponse, 0).String()
	preempted := generation.MustOutcomeErrorCode(generation.ErrorForegroundPreempted, 0).String()
	sourceErased := generation.MustOutcomeErrorCode(generation.ErrorSourceContentErased, 0).String()
	tests := []struct {
		name      string
		history   []generationOutcome
		wantError bool
		attempt   int64
		state     string
	}{
		{name: "empty", wantError: true},
		{name: "synthetic cancellation", history: []generationOutcome{terminal("01ARZ3NDEKTSV4RRFFQ69G5FC0", 0, "cancelled", sourceErased)}, attempt: 0, state: "cancelled"},
		{name: "synthetic mixed with normal", history: []generationOutcome{
			terminal("01ARZ3NDEKTSV4RRFFQ69G5FC1", 0, "cancelled", sourceErased),
			running("01ARZ3NDEKTSV4RRFFQ69G5FC2", 1),
		}, wantError: true},
		{name: "attempt gap", history: []generationOutcome{
			running("01ARZ3NDEKTSV4RRFFQ69G5FC3", 1), terminal("01ARZ3NDEKTSV4RRFFQ69G5FC4", 1, "failed", transport),
			running("01ARZ3NDEKTSV4RRFFQ69G5FC5", 3),
		}, wantError: true},
		{name: "unbounded attempt fails without iteration", history: []generationOutcome{
			running("01ARZ3NDEKTSV4RRFFQ69G5FCS", math.MaxInt64),
		}, wantError: true},
		{name: "duplicate running", history: []generationOutcome{
			running("01ARZ3NDEKTSV4RRFFQ69G5FC6", 1), running("01ARZ3NDEKTSV4RRFFQ69G5FC7", 1),
		}, wantError: true},
		{name: "terminal without running", history: []generationOutcome{
			terminal("01ARZ3NDEKTSV4RRFFQ69G5FC8", 1, "failed", transport),
		}, wantError: true},
		{name: "duplicate terminal", history: []generationOutcome{
			running("01ARZ3NDEKTSV4RRFFQ69G5FC9", 1),
			terminal("01ARZ3NDEKTSV4RRFFQ69G5FCA", 1, "failed", transport),
			terminal("01ARZ3NDEKTSV4RRFFQ69G5FCB", 1, "cancelled", transport),
		}, wantError: true},
		{name: "successor after success", history: []generationOutcome{
			running("01ARZ3NDEKTSV4RRFFQ69G5FCC", 1), terminal("01ARZ3NDEKTSV4RRFFQ69G5FCD", 1, "succeeded", ""),
			running("01ARZ3NDEKTSV4RRFFQ69G5FCE", 2),
		}, wantError: true},
		{name: "successor after non retryable", history: []generationOutcome{
			running("01ARZ3NDEKTSV4RRFFQ69G5FCF", 1), terminal("01ARZ3NDEKTSV4RRFFQ69G5FCG", 1, "failed", validation),
			running("01ARZ3NDEKTSV4RRFFQ69G5FCH", 2),
		}, wantError: true},
		{name: "retryable successor", history: []generationOutcome{
			running("01ARZ3NDEKTSV4RRFFQ69G5FCJ", 1), terminal("01ARZ3NDEKTSV4RRFFQ69G5FCK", 1, "failed", transport),
			running("01ARZ3NDEKTSV4RRFFQ69G5FCM", 2),
		}, attempt: 2, state: "running"},
		{name: "preemption successor", history: []generationOutcome{
			running("01ARZ3NDEKTSV4RRFFQ69G5FCN", 1), terminal("01ARZ3NDEKTSV4RRFFQ69G5FCP", 1, "cancelled", preempted),
			running("01ARZ3NDEKTSV4RRFFQ69G5FCQ", 2), terminal("01ARZ3NDEKTSV4RRFFQ69G5FCR", 2, "failed", transport),
		}, attempt: 2, state: "failed"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got, err := reduceGenerationOutcomeHistory(test.history)
			if test.wantError {
				if err == nil {
					t.Fatalf("illegal history accepted: %+v", got)
				}
				return
			}
			if err != nil || got.AttemptNo != test.attempt || got.State != test.state {
				t.Fatalf("reduced = %+v, err=%v", got, err)
			}
		})
	}
}

func TestM7ZeroOutcomeHistoryFailsAllRealConsumers(t *testing.T) {
	assertM7InvalidOutcomeHistoryFailsAllRealConsumers(t, nil)
}

func TestM7MalformedOutcomeHistoryFailsAllRealConsumers(t *testing.T) {
	assertM7InvalidOutcomeHistoryFailsAllRealConsumers(t, func(t *testing.T, fixture *semanticFixture) {
		insertTestOutcome(t, fixture, 1, "running")
		insertTestOutcome(t, fixture, 1, "failed")
		insertTestOutcome(t, fixture, 3, "running")
	})
}

func assertM7InvalidOutcomeHistoryFailsAllRealConsumers(t *testing.T, prepare func(*testing.T, *semanticFixture)) {
	t.Helper()
	tests := []struct {
		name string
		run  func(context.Context, *semanticFixture, *CanonicalRepository, domain.Event) error
	}{
		{name: "repository summary", run: func(ctx context.Context, fixture *semanticFixture, _ *CanonicalRepository, event domain.Event) error {
			_, err := (generationOutcomeRepository{}).Summary(ctx, fixture.db,
				mustParseSnapshotID(fixture.run["A"]), event.ResidentID)
			return err
		}},
		{name: "dialogue", run: func(ctx context.Context, fixture *semanticFixture, repository *CanonicalRepository, event domain.Event) error {
			repurposeMalformedRun(t, fixture, domain.GenerationPurposeDialogue, domain.DialogueObligation(event.ID))
			_, err := repository.classifyDialogueWork(ctx, event, "active", true, 2)
			return err
		}},
		{name: "memory extraction", run: func(ctx context.Context, _ *semanticFixture, repository *CanonicalRepository, event domain.Event) error {
			_, err := repository.classifyMemoryExtractionWorkForKey(
				ctx, event, mustOutcomeTestID(t, "01ARZ3NDEKTSV4RRFFQ69G5FCS"), "active", 2, "idem-A",
			)
			return err
		}},
		{name: "prepared generation", run: func(ctx context.Context, fixture *semanticFixture, repository *CanonicalRepository, event domain.Event) error {
			repurposeMalformedRun(t, fixture, domain.GenerationPurposeDialogue, domain.DialogueObligation(event.ID))
			mustExec(t, fixture.db, `UPDATE generation_runs SET prompt_template_version = ?,
				context_policy_version = ?, memory_rendering_version = ? WHERE generation_run_id = ?`,
				domain.DialoguePromptTemplateVersionV1, domain.DialogueContextPolicyVersionV1, domain.MemoryRenderingVersionNoneV1, fixture.run["A"])
			_, err := repository.Generation(ctx, mustParseSnapshotID(fixture.run["A"]))
			return err
		}},
		{name: "persona", run: func(ctx context.Context, fixture *semanticFixture, _ *CanonicalRepository, event domain.Event) error {
			repurposeMalformedRun(t, fixture, domain.GenerationPurposePersonaRevision, domain.PersonaRevisionObligation(event.ID))
			tx, err := fixture.db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
			if err != nil {
				return err
			}
			defer tx.Rollback()
			_, err = classifyPersonaRun(ctx, tx, &domain.PersonaRevisionWork{
				ResidentID: mustParseSnapshotID(fixture.resident["A"]), TriggerStageTransitionID: event.ID,
			}, 2)
			return err
		}},
		{name: "alignment", run: func(ctx context.Context, fixture *semanticFixture, _ *CanonicalRepository, _ domain.Event) error {
			const key = "m7-malformed-alignment"
			repurposeMalformedRun(t, fixture, domain.GenerationPurposeMemoryAlignment, key)
			tx, err := fixture.db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
			if err != nil {
				return err
			}
			defer tx.Rollback()
			_, err = classifyAlignmentRun(ctx, tx, &domain.MemoryAlignmentWork{
				Resident:       domain.ResidentSnapshot{ResidentID: mustParseSnapshotID(fixture.resident["A"])},
				IdempotencyKey: key,
			}, 2)
			return err
		}},
		{name: "autonomy", run: func(ctx context.Context, fixture *semanticFixture, repository *CanonicalRepository, event domain.Event) error {
			trigger := autonomy.Trigger{Kind: autonomy.TriggerIdle, SourceID: event.ID, Ordinal: 1}
			key, err := trigger.IdempotencyKey(string(autonomy.PolicyVersionV0), event.ResidentID)
			if err != nil {
				return err
			}
			repurposeMalformedRun(t, fixture, domain.GenerationPurposeSelfTalk, key)
			_, err = repository.AutonomousWork(ctx, event.ResidentID, trigger, 2)
			return err
		}},
		{name: "autonomy snapshot foreground", run: func(ctx context.Context, fixture *semanticFixture, _ *CanonicalRepository, event domain.Event) error {
			repurposeMalformedRun(t, fixture, domain.GenerationPurposeDialogue, domain.DialogueObligation(event.ID))
			// A second pending dialogue must not short-circuit validation of the
			// malformed run already captured at this head.
			mustExec(t, fixture.db, `INSERT INTO events(
				event_id, canonical_commit_id, resident_id, seq, event_type, visibility,
				delivery_screen, delivery_audio, ingress, trust_level, actor_principal_id,
				target_principal_id, generation_run_id, occurred_at, occurred_tz, recorded_at,
				recorded_tz, content_id, payload_commitment, prev_event_hash, event_hash,
				event_hash_algorithm, event_hash_domain, canonicalization_version
			) SELECT ?, canonical_commit_id, resident_id, seq + 1, event_type, visibility,
				delivery_screen, delivery_audio, ingress, trust_level, actor_principal_id,
				target_principal_id, NULL, occurred_at, occurred_tz, recorded_at, recorded_tz,
				content_id, payload_commitment, prev_event_hash, ?,
				event_hash_algorithm, event_hash_domain, canonicalization_version
			FROM events WHERE event_id = ?`, fixture.ids.new(), semanticDigest("m7-pending-event"), event.ID.String())
			var headSeq int64
			if err := fixture.db.QueryRowContext(ctx, `SELECT MAX(commit_seq) FROM canonical_commits`).Scan(&headSeq); err != nil {
				return err
			}
			_, err := foregroundDialoguePending(ctx, fixture.db, event.ResidentID, headSeq, 2)
			return err
		}},
		{name: "writer latest precondition", run: func(ctx context.Context, fixture *semanticFixture, _ *CanonicalRepository, _ domain.Event) error {
			tx, err := fixture.db.BeginTx(ctx, nil)
			if err != nil {
				return err
			}
			defer tx.Rollback()
			_, _, _, err = (&canonicalUoW{tx: tx}).latestOutcomeWithError(
				ctx, mustParseSnapshotID(fixture.run["A"]), mustParseSnapshotID(fixture.resident["A"]),
			)
			return err
		}},
		{name: "autonomous preemption helper", run: func(ctx context.Context, fixture *semanticFixture, _ *CanonicalRepository, _ domain.Event) error {
			tx, err := fixture.db.BeginTx(ctx, nil)
			if err != nil {
				return err
			}
			defer tx.Rollback()
			_, err = (&canonicalUoW{tx: tx}).autonomousRunWasForegroundPreempted(
				ctx, mustParseSnapshotID(fixture.run["A"]), 2,
			)
			return err
		}},
		{name: "recovery", run: func(ctx context.Context, _ *semanticFixture, repository *CanonicalRepository, _ domain.Event) error {
			_, err := repository.RunningAttempts(ctx, 10)
			return err
		}},
		{name: "mandatory recovery discovery", run: func(ctx context.Context, fixture *semanticFixture, repository *CanonicalRepository, event domain.Event) error {
			repurposeMalformedRun(t, fixture, domain.GenerationPurposeDialogue, domain.DialogueObligation(event.ID))
			// Discovery resolves lifecycle for every resident before classifying
			// mandatory work. Keep that surrounding Canonical history valid so
			// this parity case reaches the deliberately malformed outcome row.
			mustExec(t, fixture.db, `INSERT INTO pipeline_versions(
				pipeline_version_id, canonical_commit_id, pipeline_kind, version_key,
				definition, recorded_at, recorded_tz
			) VALUES (?, ?, 'dialogue', ?, '{}', ?, ?)`, fixture.ids.new(), fixture.commit["global"],
				domain.DialoguePromptTemplateVersionV1, semanticTime, semanticTZ)
			mustExec(t, fixture.db, `INSERT INTO sessionization_policy_versions(
				sessionization_policy_version_id, canonical_commit_id, version_key,
				definition, recorded_at, recorded_tz
			) VALUES (?, ?, ?, ?, ?, ?)`, fixture.ids.new(), fixture.commit["global"], domain.SessionPolicyVersion,
				`{"idle_gap_microseconds":"1800000000","version":"sessionization-v1"}`,
				semanticTime, semanticTZ)
			for _, residentKey := range []string{"A", "B"} {
				mustExec(t, fixture.db, `INSERT INTO resident_status_transitions(
					resident_status_transition_id, canonical_commit_id, resident_id,
					from_status, to_status, actor_principal_id, reason_code,
					reason_content_id, occurred_at, occurred_tz, recorded_at, recorded_tz
				) VALUES (?, ?, ?, NULL, 'active', ?, 'test_fixture', NULL, ?, ?, ?, ?)`,
					fixture.ids.new(), fixture.commit[residentKey], fixture.resident[residentKey],
					fixture.principal["human"], semanticTime, semanticTZ, semanticTime, semanticTZ)
			}
			mustExec(t, fixture.db, `INSERT INTO runtime_config(
				singleton_id, active_resident_id, desired_sessionization_policy_version_id,
				updated_at, updated_tz
			) VALUES (1, ?, NULL, ?, ?)
			ON CONFLICT(singleton_id) DO UPDATE SET active_resident_id=excluded.active_resident_id,
				desired_sessionization_policy_version_id=NULL`, fixture.resident["B"], semanticTime, semanticTZ)
			_, _, err := repository.DiscoverMandatoryRecoveryWork(ctx, nil, 10)
			return err
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			fixture, closeFixture := newSemanticFixture(t)
			defer closeFixture()
			if prepare != nil {
				prepare(t, fixture)
			}
			repository := &CanonicalRepository{store: &Store{reader: fixture.db}}
			event, err := repository.Event(
				context.Background(), mustParseSnapshotID(fixture.resident["A"]), mustParseSnapshotID(fixture.event["A"]),
			)
			if err != nil {
				t.Fatal(err)
			}
			err = test.run(context.Background(), fixture, repository, event)
			if !errors.Is(err, ErrInvalidGenerationOutcomeHistory) {
				t.Fatalf("consumer error = %v, want ErrInvalidGenerationOutcomeHistory", err)
			}
		})
	}
}

func TestM7GenerationOutcomeRepositoryInputEnvelopeMatrix(t *testing.T) {
	t.Run("normal attempt requires input", func(t *testing.T) {
		fixture, closeFixture := newSemanticFixture(t)
		defer closeFixture()
		insertTestOutcome(t, fixture, 1, "running")
		_, err := (generationOutcomeRepository{}).Summary(context.Background(), fixture.db,
			mustParseSnapshotID(fixture.run["A"]), mustParseSnapshotID(fixture.resident["A"]))
		if !errors.Is(err, ErrInvalidGenerationOutcomeHistory) {
			t.Fatalf("summary error = %v, want input-envelope failure", err)
		}
	})

	t.Run("synthetic attempt rejects input", func(t *testing.T) {
		fixture, closeFixture := newSemanticFixture(t)
		defer closeFixture()
		insertGenerationInput(t, fixture, 0)
		insertTestOutcomeWithClass(t, fixture, 0, "cancelled",
			generation.MustOutcomeErrorCode(generation.ErrorSourceContentErased, 0).String(), nil)
		_, err := (generationOutcomeRepository{}).Summary(context.Background(), fixture.db,
			mustParseSnapshotID(fixture.run["A"]), mustParseSnapshotID(fixture.resident["A"]))
		if !errors.Is(err, ErrInvalidGenerationOutcomeHistory) {
			t.Fatalf("summary error = %v, want synthetic input-envelope failure", err)
		}
	})

	t.Run("synthetic attempt accepts zero inputs", func(t *testing.T) {
		fixture, closeFixture := newSemanticFixture(t)
		defer closeFixture()
		insertTestOutcomeWithClass(t, fixture, 0, "cancelled",
			generation.MustOutcomeErrorCode(generation.ErrorSourceContentErased, 0).String(), nil)
		summary, err := (generationOutcomeRepository{}).Summary(context.Background(), fixture.db,
			mustParseSnapshotID(fixture.run["A"]), mustParseSnapshotID(fixture.resident["A"]))
		if err != nil || summary.InputCount != 0 || summary.Latest.AttemptNo != 0 || summary.ChargedRetryCount != 0 {
			t.Fatalf("summary = %+v, err=%v", summary, err)
		}
		classified, err := summary.Classify(2)
		if err != nil || classified.RetryEligible || classified.ClassifiedState != domain.WorkTerminalFailed {
			t.Fatalf("classified synthetic summary = %+v, err=%v", classified, err)
		}
	})

	t.Run("multiple inputs do not multiply outcome history", func(t *testing.T) {
		fixture, closeFixture := newSemanticFixture(t)
		defer closeFixture()
		insertGenerationInput(t, fixture, 0)
		insertGenerationInput(t, fixture, 1)
		insertTestOutcome(t, fixture, 1, "running")
		insertTestOutcome(t, fixture, 1, "failed")
		insertTestOutcome(t, fixture, 2, "running")
		run, history, err := (generationOutcomeRepository{}).capturedHistory(context.Background(), fixture.db,
			mustParseSnapshotID(fixture.run["A"]), mustParseSnapshotID(fixture.resident["A"]), nil)
		if err != nil {
			t.Fatal(err)
		}
		if run.InputCount != 2 || len(history) != 3 {
			t.Fatalf("input count=%d history rows=%d, want 2 and 3", run.InputCount, len(history))
		}
		summary, err := (generationOutcomeRepository{}).Summary(context.Background(), fixture.db,
			mustParseSnapshotID(fixture.run["A"]), mustParseSnapshotID(fixture.resident["A"]))
		if err != nil || summary.Latest.AttemptNo != 2 || summary.ChargedRetryCount != 1 {
			t.Fatalf("summary = %+v, err=%v", summary, err)
		}
	})
}

func TestM7OutputReferenceCountsRemainConservativeForIllegalHistory(t *testing.T) {
	fixture, closeFixture := newSemanticFixture(t)
	defer closeFixture()
	insertGenerationInput(t, fixture, 0)
	insertTestOutcome(t, fixture, 1, "running")
	mustExec(t, fixture.db, `PRAGMA ignore_check_constraints = ON`)
	outputID := mustParseSnapshotID(fixture.content["A"]["output"])
	insertTestOutcomeWithClass(t, fixture, 1, "failed",
		generation.MustOutcomeErrorCode(generation.ErrorTransport, 0).String(), &outputID)
	mustExec(t, fixture.db, `PRAGMA ignore_check_constraints = OFF`)

	_, err := (generationOutcomeRepository{}).Summary(context.Background(), fixture.db,
		mustParseSnapshotID(fixture.run["A"]), mustParseSnapshotID(fixture.resident["A"]))
	if !errors.Is(err, ErrInvalidGenerationOutcomeHistory) {
		t.Fatalf("summary error = %v, want illegal failed+output history", err)
	}
	var headSeq int64
	if err := fixture.db.QueryRow(`SELECT MAX(commit_seq) FROM canonical_commits`).Scan(&headSeq); err != nil {
		t.Fatal(err)
	}
	counts, err := (generationOutcomeRepository{}).OutputReferenceCounts(context.Background(), fixture.db, headSeq)
	if err != nil {
		t.Fatal(err)
	}
	if got := counts[outputID.String()]; got != 1 {
		t.Fatalf("conservative output reference count = %d, want 1", got)
	}
}

func TestM7RunningAttemptsPropagatesCandidateRowIterationError(t *testing.T) {
	want := errors.New("injected candidate iteration failure")
	rows := &failingRunningAttemptRows{iterationErr: want}
	_, err := readRunningAttemptCandidates(rows)
	if !errors.Is(err, want) {
		t.Fatalf("candidate error = %v, want %v", err, want)
	}
	if !rows.closed {
		t.Fatal("candidate rows were not closed")
	}
}

type failingRunningAttemptRows struct {
	yielded      bool
	closed       bool
	iterationErr error
}

func (rows *failingRunningAttemptRows) Next() bool {
	if rows.yielded {
		return false
	}
	rows.yielded = true
	return true
}

func (rows *failingRunningAttemptRows) Scan(destinations ...any) error {
	if len(destinations) != 2 {
		return fmt.Errorf("unexpected destination count %d", len(destinations))
	}
	*(destinations[0].(*string)) = "01ARZ3NDEKTSV4RRFFQ69G5FA0"
	*(destinations[1].(*string)) = "01ARZ3NDEKTSV4RRFFQ69G5FA1"
	return nil
}

func (rows *failingRunningAttemptRows) Err() error { return rows.iterationErr }
func (rows *failingRunningAttemptRows) Close() error {
	rows.closed = true
	return nil
}

func repurposeMalformedRun(t *testing.T, fixture *semanticFixture, purpose domain.GenerationPurpose, key string) {
	t.Helper()
	mustExec(t, fixture.db, `DROP TRIGGER trg_generation_runs_no_update`)
	mustExec(t, fixture.db, `UPDATE generation_runs SET purpose = ?, idempotency_key = ? WHERE generation_run_id = ?`,
		string(purpose), key, fixture.run["A"])
}

func reducerOutcome(t *testing.T, rawID string, attempt int64, state, class string) generationOutcome {
	t.Helper()
	id, err := canonical.ParseID(rawID)
	if err != nil {
		t.Fatal(err)
	}
	var errorClass sql.NullString
	if class != "" {
		errorClass = sql.NullString{String: class, Valid: true}
	}
	return generationOutcome{ID: id, AttemptNo: attempt, State: state, ErrorClass: errorClass}
}

func mustOutcomeTestID(t *testing.T, raw string) canonical.ID {
	t.Helper()
	id, err := canonical.ParseID(raw)
	if err != nil {
		t.Fatal(fmt.Errorf("parse test ID %q: %w", raw, err))
	}
	return id
}

func insertTestOutcome(t *testing.T, fixture *semanticFixture, attempt int, state string) {
	t.Helper()
	var errorClass any
	if state != "running" {
		errorClass = "provider_timeout"
	}
	mustExec(t, fixture.db, `INSERT INTO generation_run_outcomes(
		outcome_id, canonical_commit_id, generation_run_id, attempt_no, state,
		output_content_id, prompt_tokens, completion_tokens, latency, estimated_cost,
		error_class, error_detail_content_id, recorded_at, recorded_tz
	) VALUES (?, ?, ?, ?, ?, NULL, NULL, NULL, NULL, NULL, ?, NULL, ?, ?)`,
		fixture.ids.new(), fixture.commit["A"], fixture.run["A"], attempt, state,
		errorClass, semanticTime, semanticTZ)
}

func insertTestOutcomeWithClass(t *testing.T, fixture *semanticFixture, attempt int, state, errorClass string, outputID *canonical.ID) {
	t.Helper()
	var output any
	if outputID != nil {
		output = outputID.String()
	}
	mustExec(t, fixture.db, `INSERT INTO generation_run_outcomes(
		outcome_id, canonical_commit_id, generation_run_id, attempt_no, state,
		output_content_id, prompt_tokens, completion_tokens, latency, estimated_cost,
		error_class, error_detail_content_id, recorded_at, recorded_tz
	) VALUES (?, ?, ?, ?, ?, ?, NULL, NULL, NULL, NULL, ?, NULL, ?, ?)`,
		fixture.ids.new(), fixture.commit["A"], fixture.run["A"], attempt, state,
		output, errorClass, semanticTime, semanticTZ)
}

func insertGenerationInput(t *testing.T, fixture *semanticFixture, ordinal int) {
	t.Helper()
	role, sourceType, sourceID, inclusionMode, contentID := "user", "event", fixture.event["A"], "current_input", fixture.content["A"]["event"]
	if ordinal != 0 {
		role, sourceType, sourceID, inclusionMode, contentID = "system", "resident_revision",
			fixture.revision["A"]["principles"], "resident_definition", fixture.content["A"]["principles"]
	}
	mustExec(t, fixture.db, `INSERT INTO generation_run_inputs(
		generation_run_input_id, canonical_commit_id, generation_run_id, ordinal,
		role, source_type, source_id, inclusion_mode, content_id, recorded_at, recorded_tz
	) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`, fixture.ids.new(), fixture.commit["A"], fixture.run["A"], ordinal,
		role, sourceType, sourceID, inclusionMode, contentID, semanticTime, semanticTZ)
}
