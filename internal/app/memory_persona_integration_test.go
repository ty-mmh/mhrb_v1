package app

import (
	"context"
	"database/sql"
	"strings"
	"sync"
	"testing"
	"time"

	"mahoroba.local/mahoroba/internal/domain"
	"mahoroba.local/mahoroba/internal/generation"
)

func TestM5I20SmallPersonaRevisionAutoActivates(t *testing.T) {
	ctx := context.Background()
	const previous = "friendly, patient, careful, curious, grounded, concise, warm, and consistently thoughtful"
	const proposed = "friendly, patient, careful, curious, grounded, concise, warm, and consistently mindful"
	extraction := `{"claims":[` +
		`{"grade":"stated","perspective":"resident","source_quote":"likes tea","statement":"The owner likes tea.","subject":"source_actor","temporal_kind":"stable"},` +
		`{"grade":"stated","perspective":"resident","source_quote":"likes books","statement":"The owner likes books.","subject":"source_actor","temporal_kind":"stable"}` +
		`],"version":"memory-extraction-output-v1"}`
	personaOutput := `{"contradiction":false,"persona":"` + proposed + `"}`
	generator := &scriptedGenerator{steps: []generatorStep{
		{text: "dialogue one"}, {text: extraction},
		{text: "dialogue two"}, {text: extraction},
		{text: "dialogue three"}, {text: extraction},
		{text: personaOutput},
	}}
	fixture := newApplicationFixtureWithPersona(t, generator, 2, previous)
	if _, err := fixture.application.activateMemoryPolicyV4ForTest(ctx, fixture.residentID); err != nil {
		t.Fatal(err)
	}
	fixture.clock.Advance(24 * time.Hour)
	for index := 0; index < 3; index++ {
		if _, err := fixture.application.Ingress(ctx, "I likes tea and likes books"); err != nil {
			t.Fatal(err)
		}
		if err := fixture.application.ProcessResident(ctx, fixture.residentID); err != nil {
			t.Fatalf("ProcessResident(%d): %v", index, err)
		}
	}
	// The third dialogue turn yields after mandatory extraction. Persona is
	// optional and is admitted by the following clean resident turn.
	if err := fixture.application.ProcessResident(ctx, fixture.residentID); err != nil {
		t.Fatalf("ProcessResident(persona): %v", err)
	}
	if generator.CallCount() != 7 {
		t.Fatalf("generator calls = %d, want 3 dialogue + 3 extraction + persona", generator.CallCount())
	}
	requests := generator.Requests()
	personaRequest := requests[len(requests)-1]
	if personaRequest.Purpose != string(domain.GenerationPurposePersonaRevision) || personaRequest.Streaming {
		t.Fatalf("persona request = %+v", personaRequest)
	}
	for _, message := range personaRequest.Messages {
		if message.Text == "be helpful" || strings.Contains(message.Text, "principles") {
			t.Fatalf("persona request leaked principles: %q", message.Text)
		}
	}

	database := openApplicationDatabase(t, fixture.store.Path())
	defer database.Close()
	var revisionID, activationID string
	var revisionCommit, activationCommit int64
	var persona string
	err := database.QueryRow(`SELECT revision.revision_id, activation.activation_id,
		revision_commit.commit_seq, activation_commit.commit_seq, blob.content
		FROM resident_revisions revision
		JOIN canonical_commits revision_commit ON revision_commit.canonical_commit_id = revision.canonical_commit_id
		JOIN resident_revision_activations activation ON activation.revision_id = revision.revision_id
		JOIN canonical_commits activation_commit ON activation_commit.canonical_commit_id = activation.canonical_commit_id
		JOIN content_objects content ON content.content_id = revision.content_id
		JOIN blobs blob ON blob.dedupe_scope_id = content.owner_resident_id
		 AND blob.hash_algorithm = content.blob_hash_algorithm AND blob.blob_hash = content.blob_hash
		WHERE revision.resident_id = ? AND revision.revision_class = 'persona'
		ORDER BY activation_commit.commit_seq DESC LIMIT 1`, fixture.residentID.String()).Scan(
		&revisionID, &activationID, &revisionCommit, &activationCommit, &persona,
	)
	if err != nil {
		if err == sql.ErrNoRows {
			t.Fatal("automatic persona revision was not activated")
		}
		t.Fatal(err)
	}
	if revisionCommit != activationCommit || persona != proposed || revisionID == "" || activationID == "" {
		t.Fatalf("revision=%s activation=%s commits=%d/%d persona=%q", revisionID, activationID, revisionCommit, activationCommit, persona)
	}
}

type personaPreemptionGenerator struct {
	mu             sync.Mutex
	requests       []generation.Request
	personaCalls   int
	personaStarted chan struct{}
	dialogueDone   chan struct{}
}

func (generator *personaPreemptionGenerator) Stream(
	ctx context.Context,
	request generation.Request,
	_ generation.DeltaSink,
) (generation.Result, error) {
	generator.mu.Lock()
	generator.requests = append(generator.requests, request)
	if request.Purpose == string(domain.GenerationPurposePersonaRevision) {
		generator.personaCalls++
		call := generator.personaCalls
		generator.mu.Unlock()
		if call == 1 {
			close(generator.personaStarted)
			<-ctx.Done()
			return generation.Result{}, ctx.Err()
		}
		return generation.Result{Text: `{"contradiction":false,"persona":"friendly"}`}, nil
	}
	generator.mu.Unlock()
	if request.Purpose == string(domain.GenerationPurposeDialogue) {
		close(generator.dialogueDone)
		return generation.Result{Text: "foreground reply"}, nil
	}
	if request.Purpose == string(domain.GenerationPurposeMemoryExtraction) {
		return generation.Result{Text: `{"claims":[],"version":"memory-extraction-output-v1"}`}, nil
	}
	return generation.Result{}, nil
}

func (generator *personaPreemptionGenerator) Requests() []generation.Request {
	generator.mu.Lock()
	defer generator.mu.Unlock()
	return append([]generation.Request(nil), generator.requests...)
}

func TestPersonaForegroundPreemptionYieldsResidentLockBeforeRetry(t *testing.T) {
	ctx := context.Background()
	// The fixture creates two eligible settled claims without consuming their
	// pending persona proposal. maxAttempts=1 proves foreground preemption does
	// not consume the provider retry budget even though attempt ordinals remain
	// append-only.
	fixture, work := personaWorkForTerminalization(t, 1)
	generator := &personaPreemptionGenerator{
		personaStarted: make(chan struct{}), dialogueDone: make(chan struct{}),
	}
	fixture.application.generator = generator

	backgroundDone := make(chan error, 1)
	go func() {
		backgroundDone <- fixture.application.ProcessResident(ctx, fixture.residentID)
	}()
	select {
	case <-generator.personaStarted:
	case <-time.After(5 * time.Second):
		t.Fatal("persona provider call did not start")
	}
	if _, err := fixture.application.Ingress(ctx, "foreground interrupts persona"); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-backgroundDone:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("preempted persona queue did not yield the resident lock")
	}
	for turn := 0; turn < 2; turn++ {
		if err := fixture.application.ProcessResident(ctx, fixture.residentID); err != nil {
			t.Fatal(err)
		}
	}
	select {
	case <-generator.dialogueDone:
	default:
		t.Fatal("foreground dialogue did not run before persona retry")
	}
	// That foreground turn spends its fair quantum on mandatory extraction.
	// Persona retry is intentionally deferred to a separate clean turn.
	if err := fixture.application.ProcessResident(ctx, fixture.residentID); err != nil {
		t.Fatal(err)
	}

	requests := generator.Requests()
	wantPurposes := []string{
		string(domain.GenerationPurposePersonaRevision),
		string(domain.GenerationPurposeDialogue),
		string(domain.GenerationPurposeMemoryExtraction),
		string(domain.GenerationPurposePersonaRevision),
	}
	if len(requests) != len(wantPurposes) {
		t.Fatalf("provider requests=%d want=%d", len(requests), len(wantPurposes))
	}
	for index, want := range wantPurposes {
		if requests[index].Purpose != want {
			t.Fatalf("request[%d].purpose=%q want=%q", index, requests[index].Purpose, want)
		}
	}

	database := openApplicationDatabase(t, fixture.store.Path())
	defer database.Close()
	var runID string
	if err := database.QueryRow(`SELECT generation_run_id FROM generation_runs
		WHERE resident_id = ? AND idempotency_key = ?`, fixture.residentID.String(),
		domain.PersonaRevisionObligation(work.TriggerStageTransitionID)).Scan(&runID); err != nil {
		t.Fatal(err)
	}
	rows, err := database.Query(`SELECT attempt_no, state, COALESCE(error_class, '')
		FROM generation_run_outcomes WHERE generation_run_id = ? ORDER BY outcome_id`, runID)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	type outcome struct {
		attempt int64
		state   string
		code    string
	}
	var outcomes []outcome
	for rows.Next() {
		var item outcome
		if err := rows.Scan(&item.attempt, &item.state, &item.code); err != nil {
			t.Fatal(err)
		}
		outcomes = append(outcomes, item)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	want := []outcome{
		{attempt: 1, state: "running"},
		{attempt: 1, state: "cancelled", code: "foreground_preempted"},
		{attempt: 2, state: "running"},
		{attempt: 2, state: "succeeded"},
	}
	if len(outcomes) != len(want) {
		t.Fatalf("persona outcomes=%+v want=%+v", outcomes, want)
	}
	for index := range want {
		if outcomes[index] != want[index] {
			t.Fatalf("persona outcome[%d]=%+v want=%+v", index, outcomes[index], want[index])
		}
	}
}
