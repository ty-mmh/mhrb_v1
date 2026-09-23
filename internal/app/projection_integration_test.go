package app

import (
	"context"
	"database/sql"
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

func TestProjectionDropAndRebuildMatchesGolden(t *testing.T) {
	fixture := newApplicationFixture(t, &scriptedGenerator{}, 1)
	coordinator, surface := projectionFixtureCoordinator(t, fixture)
	fixture.application.commitNotifier = coordinator
	ctx := context.Background()

	if err := coordinator.ReconcileResident(ctx, fixture.residentID); err != nil {
		t.Fatal(err)
	}
	event, err := fixture.application.Ingress(ctx, "projection golden")
	if err != nil {
		t.Fatal(err)
	}
	fixture.clock.Advance(5 * time.Minute)
	if err := coordinator.ReconcileResident(ctx, fixture.residentID); err != nil {
		t.Fatal(err)
	}
	golden := readProjectionGolden(t, fixture, surface)
	wantIdle := canonical.InstantFromTime(fixture.clock.Now()).UnixMicro() - event.RecordedAt.UnixMicro()
	if golden.Status != "active" || len(golden.Revisions) != 3 || !golden.LastUserSeq.Valid || golden.IdleDuration != wantIdle || golden.UnresolvedMarkers != "[]" {
		t.Fatalf("unexpected built Projection golden: %+v", golden)
	}

	registry := m4ProjectionRegistry(t)
	for _, definition := range registry.Definitions() {
		watermark, exists, err := surface.Watermark(ctx, definition.Name, fixture.residentID)
		if err != nil || !exists {
			t.Fatalf("watermark %s = %+v, %v, %v", definition.Name, watermark, exists, err)
		}
		if err := surface.Drop(ctx, projection.DropRequest{Definition: definition, ResidentID: fixture.residentID, Observed: &watermark}); err != nil {
			t.Fatal(err)
		}
	}
	if err := coordinator.RebuildAll(ctx, fixture.residentID); err != nil {
		t.Fatal(err)
	}
	rebuilt := readProjectionGolden(t, fixture, surface)
	if !reflect.DeepEqual(rebuilt, golden) {
		t.Fatalf("drop/rebuild differs:\ngolden=%+v\nrebuilt=%+v", golden, rebuilt)
	}
}

func TestProjectionEvaluationIsDeterministicForCapturedInputs(t *testing.T) {
	fixture := newApplicationFixture(t, &scriptedGenerator{}, 1)
	_, surface := projectionFixtureCoordinator(t, fixture)
	head, err := surface.Head(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	target := projection.Target{Head: head, AsOf: canonical.InstantFromTime(fixture.clock.Now()), AsOfTZ: canonical.MustTimezone("UTC")}
	registry := m4ProjectionRegistry(t)
	for _, definition := range registry.Definitions() {
		request := projection.EvaluationRequest{Definition: definition, ResidentID: fixture.residentID, Target: target, Plan: projection.UpdatePlan{Kind: projection.FullBuild}}
		first, err := surface.Evaluate(context.Background(), request)
		if err != nil {
			t.Fatal(err)
		}
		second, err := surface.Evaluate(context.Background(), request)
		if err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(first, second) {
			t.Fatalf("%s evaluation is not deterministic: %+v / %+v", definition.Name, first, second)
		}
	}
}

func TestM5ProductionProjectionRegistryUsesV2PolicyAndConcreteClaimEvaluator(t *testing.T) {
	fixture := newApplicationFixture(t, &scriptedGenerator{}, 1)
	ctx := context.Background()
	activation, err := fixture.application.activateMemoryPolicyV4ForTest(ctx, fixture.residentID)
	if err != nil {
		t.Fatal(err)
	}
	source := ingressPendingForTest(t, fixture, "I like tea")
	_ = dialogueRunIDForEventForTest(t, fixture, source)
	resident, err := fixture.repository.Resident(ctx, fixture.residentID)
	if err != nil {
		t.Fatal(err)
	}
	var pipelineRaw string
	if err := fixture.store.Reader().QueryRowContext(ctx, `SELECT pipeline_version_id FROM pipeline_versions
		WHERE pipeline_kind = 'memory_extraction' AND version_key = ?`, domain.MemoryExtractionPipelineVersion).Scan(&pipelineRaw); err != nil {
		t.Fatal(err)
	}
	pipelineID, err := canonical.ParseID(pipelineRaw)
	if err != nil {
		t.Fatal(err)
	}
	var maturationRaw string
	if err := fixture.store.Reader().QueryRowContext(ctx, `SELECT pipeline_version_id FROM pipeline_versions
		WHERE pipeline_kind = 'memory_maturation' AND version_key = ?`, domain.MemoryMaturationPipelineVersion).Scan(&maturationRaw); err != nil {
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
	input, err := fixture.application.newContent(fixture.residentID, "generation_input", []byte(source.Content), "independent")
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
		Purpose: domain.GenerationPurposeMemoryExtraction, IdempotencyKey: domain.MemoryExtractionObligation(source.ID),
		Provider: "test", Model: "test-model", PipelineVersionID: pipelineID,
		PrinciplesRevisionID: resident.PrinciplesRevisionID, PersonaRevisionID: resident.PersonaRevisionID,
		MemoryPolicyRevisionID: activation.RevisionID, AsOf: source.RecordedAt, AsOfTZ: source.RecordedTZ,
		DroppedInputSummary: dropped, GeneratorParams: params, RunningOutcomeID: prepareIDs[1],
		Inputs: []domain.GenerationInput{{
			ID: prepareIDs[2], Ordinal: 0, Role: string(generation.RoleUser), SourceType: "event",
			SourceID: &sourceID, InclusionMode: "current_input", Content: input,
		}},
	}
	if err := pinGenerationVersions(&prepare); err != nil {
		t.Fatal(err)
	}
	if _, err := fixture.application.submitWithContent(ctx, domain.PrepareGenerationCommand(prepare), []domain.Content{input}); err != nil {
		t.Fatal(err)
	}

	extraction, err := canonical.MarshalCanonical(memory.ExtractionOutput{
		Version: memory.ExtractionOutputVersionV1,
		Claims: []memory.ExtractionClaim{{
			Statement: "Resident likes tea", Subject: memory.SelectorResident, Perspective: memory.SelectorResident,
			TemporalKind: memory.TemporalStable, Grade: memory.GradeStated, SourceQuote: "I like tea",
		}},
	})
	if err != nil {
		t.Fatal(err)
	}
	output, err := fixture.application.newContent(fixture.residentID, "generation_output", extraction.Bytes(), "independent")
	if err != nil {
		t.Fatal(err)
	}
	statement, err := fixture.application.newContent(fixture.residentID, "claim_statement", []byte("Resident likes tea"), "independent")
	if err != nil {
		t.Fatal(err)
	}
	landingIDs, err := fixture.application.allocateIDs(7)
	if err != nil {
		t.Fatal(err)
	}
	land := domain.LandMemoryExtraction{
		Attempt:       domain.Attempt{RunID: prepare.RunID, ResidentID: fixture.residentID, AttemptNo: 1, OutcomeID: landingIDs[0]},
		SourceEventID: source.ID, PipelineVersionID: pipelineID,
		MaturationPipelineVersionID: maturationID, MemoryPolicyRevisionID: activation.RevisionID,
		Output: output,
		Claims: []domain.ExtractedClaimLanding{{
			ClaimID: landingIDs[1], EvidenceID: landingIDs[2], InitialStageID: landingIDs[3],
			InitialViewScopeID: landingIDs[4], SedimentStageTransitionID: landingIDs[5],
			SettledStageTransitionID: landingIDs[6], Statement: statement,
		}},
	}
	if _, err := fixture.application.submitWithContent(ctx, domain.LandMemoryExtractionCommand(land), []domain.Content{output, statement}); err != nil {
		t.Fatal(err)
	}
	// The fixture clock is intentionally frozen while the Canonical Writer
	// preserves strictly increasing commit instants. Advance the evaluation
	// as_of beyond the final durable commit before reconciling time-bounded
	// policy dependencies.
	fixture.clock.Advance(time.Second)

	registry, err := store.ActiveProjectionRegistry()
	if err != nil {
		t.Fatal(err)
	}
	surface := fixture.store.Projection()
	coordinator, err := projection.NewCoordinator(projection.CoordinatorOptions{
		Registry: registry, Source: surface, Store: surface, Clock: fixture.clock, Timezone: canonical.MustTimezone("UTC"),
		ScanInterval: time.Hour, AsOfRefreshInterval: time.Hour, RebuildRetryInterval: time.Hour,
		MaxStaleness: 5 * time.Minute,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := coordinator.ReconcileResident(ctx, fixture.residentID); err != nil {
		t.Fatal(err)
	}

	claimWatermark, exists, err := surface.Watermark(ctx, projection.ClaimStatesName, fixture.residentID)
	if err != nil || !exists {
		t.Fatalf("claim watermark = %+v, %v, %v", claimWatermark, exists, err)
	}
	wantDependency := []projection.Dependency{{Kind: projection.MemoryPolicyDependency, VersionID: activation.RevisionID}}
	if equal, err := projection.DependencySetEqual(claimWatermark.Dependencies, wantDependency); err != nil || !equal {
		t.Fatalf("claim dependencies = %+v, want %+v: %v", claimWatermark.Dependencies, wantDependency, err)
	}
	viewWatermark, exists, err := surface.Watermark(ctx, projection.ClaimViewScopeCurrentName, fixture.residentID)
	if err != nil || !exists || len(viewWatermark.Dependencies) != 0 {
		t.Fatalf("claim view watermark = %+v, %v, %v", viewWatermark, exists, err)
	}
	var stage, status, temporal string
	var evidenceCount int64
	if err := fixture.store.Reader().QueryRowContext(ctx, `SELECT stage, status, temporal_relation, evidence_count
		FROM claim_states WHERE claim_id = ? AND resident_id = ?`, landingIDs[1].String(), fixture.residentID.String()).Scan(
		&stage, &status, &temporal, &evidenceCount,
	); err != nil {
		t.Fatal(err)
	}
	if stage != "floating" || status != "active" || temporal != "current" || evidenceCount != 1 {
		t.Fatalf("concrete claim projection = stage:%s status:%s temporal:%s evidence:%d", stage, status, temporal, evidenceCount)
	}
	var viewScope string
	if err := fixture.store.Reader().QueryRowContext(ctx, `SELECT view_scope FROM claim_view_scope_current
		WHERE claim_id = ? AND resident_id = ?`, landingIDs[1].String(), fixture.residentID.String()).Scan(&viewScope); err != nil {
		t.Fatal(err)
	}
	if viewScope == "" {
		t.Fatal("production claim view scope is empty")
	}
}

func TestSourceQueriesAreBoundedByCapturedTargetCommit(t *testing.T) {
	fixture := newApplicationFixture(t, &scriptedGenerator{}, 1)
	_, surface := projectionFixtureCoordinator(t, fixture)
	ctx := context.Background()
	headBefore, err := surface.Head(ctx)
	if err != nil {
		t.Fatal(err)
	}
	target := projection.Target{Head: headBefore, AsOf: canonical.InstantFromTime(fixture.clock.Now()), AsOfTZ: canonical.MustTimezone("UTC")}
	if _, err := fixture.application.Ingress(ctx, "after captured head"); err != nil {
		t.Fatal(err)
	}
	definition := projection.Definition{Name: projection.RuntimeStatesName, Version: "runtime-states-v2", TimeSensitive: true}
	evaluation, err := surface.Evaluate(ctx, projection.EvaluationRequest{Definition: definition, ResidentID: fixture.residentID, Target: target, Plan: projection.UpdatePlan{Kind: projection.FullBuild}})
	if err != nil {
		t.Fatal(err)
	}
	state := evaluation.Value.(store.RuntimeStateProjection)
	if state.LastUserEventSeq != nil || state.LastResidentEventSeq != nil {
		t.Fatalf("captured target leaked a later event: %+v", state)
	}
}

func TestRuntimeStatesAreProjectionOnlyAndRebuildable(t *testing.T) {
	fixture := newApplicationFixture(t, &scriptedGenerator{}, 1)
	coordinator, surface := projectionFixtureCoordinator(t, fixture)
	ctx := context.Background()
	if _, err := fixture.application.Ingress(ctx, "runtime state"); err != nil {
		t.Fatal(err)
	}
	if err := coordinator.Rebuild(ctx, fixture.residentID, projection.RuntimeStatesName); err != nil {
		t.Fatal(err)
	}
	var markers string
	var userSeq, residentSeq sql.NullInt64
	if err := fixture.store.Reader().QueryRowContext(ctx, `SELECT last_user_event_seq, last_resident_event_seq, unresolved_reference_markers FROM runtime_states WHERE resident_id = ?`, fixture.residentID.String()).Scan(&userSeq, &residentSeq, &markers); err != nil {
		t.Fatal(err)
	}
	if !userSeq.Valid || residentSeq.Valid || markers != "[]" {
		t.Fatalf("thin runtime state = user:%v resident:%v markers:%s", userSeq, residentSeq, markers)
	}
	watermark, exists, err := surface.Watermark(ctx, projection.RuntimeStatesName, fixture.residentID)
	if err != nil || !exists || len(watermark.Dependencies) != 0 {
		t.Fatalf("runtime watermark = %+v, %v, %v", watermark, exists, err)
	}
	for _, forbidden := range []string{"activity_session", "live_context"} {
		var count int
		if err := fixture.store.Reader().QueryRowContext(ctx, `SELECT count(*) FROM pragma_table_info('runtime_states') WHERE name LIKE ?`, "%"+forbidden+"%").Scan(&count); err != nil {
			t.Fatal(err)
		}
		if count != 0 {
			t.Fatalf("runtime_states persisted forbidden %s field", forbidden)
		}
	}
}

type projectionGolden struct {
	Status            string
	TransitionID      string
	Revisions         [][3]string
	LastUserSeq       sql.NullInt64
	LastResidentSeq   sql.NullInt64
	UnresolvedMarkers string
	IdleDuration      int64
}

func projectionFixtureCoordinator(t *testing.T, fixture applicationFixture) (*projection.Coordinator, *store.ProjectionRepository) {
	t.Helper()
	registry := m4ProjectionRegistry(t)
	surface := fixture.store.Projection()
	coordinator, err := projection.NewCoordinator(projection.CoordinatorOptions{
		Registry: registry, Source: surface, Store: surface, Clock: fixture.clock, Timezone: canonical.MustTimezone("UTC"),
		ScanInterval: time.Hour, AsOfRefreshInterval: time.Hour, RebuildRetryInterval: time.Hour, MaxStaleness: 5 * time.Minute,
	})
	if err != nil {
		t.Fatal(err)
	}
	return coordinator, surface
}

// m4ProjectionRegistry keeps the legacy M4 acceptance fixtures scoped to the
// three projections whose golden data they assert while using the current
// runtime-state implementation version.
func m4ProjectionRegistry(t *testing.T) *projection.Registry {
	t.Helper()
	registry, err := projection.NewRegistry(
		projection.Definition{Name: projection.ResidentCurrentStatusName, Version: "resident-current-status-v1"},
		projection.Definition{Name: projection.ResidentCurrentRevisionName, Version: "resident-current-revision-v1"},
		projection.Definition{Name: projection.RuntimeStatesName, Version: "runtime-states-v2", TimeSensitive: true},
	)
	if err != nil {
		t.Fatal(err)
	}
	return registry
}

func readProjectionGolden(t *testing.T, fixture applicationFixture, surface *store.ProjectionRepository) projectionGolden {
	t.Helper()
	ctx := context.Background()
	var result projectionGolden
	if err := fixture.store.Reader().QueryRowContext(ctx, `SELECT status, source_transition_id FROM resident_current_status WHERE resident_id = ?`, fixture.residentID.String()).Scan(&result.Status, &result.TransitionID); err != nil {
		t.Fatal(err)
	}
	rows, err := fixture.store.Reader().QueryContext(ctx, `SELECT revision_class, revision_id, activation_id FROM resident_current_revision WHERE resident_id = ? ORDER BY revision_class`, fixture.residentID.String())
	if err != nil {
		t.Fatal(err)
	}
	for rows.Next() {
		var row [3]string
		if err := rows.Scan(&row[0], &row[1], &row[2]); err != nil {
			_ = rows.Close()
			t.Fatal(err)
		}
		result.Revisions = append(result.Revisions, row)
	}
	if err := rows.Close(); err != nil {
		t.Fatal(err)
	}
	if err := fixture.store.Reader().QueryRowContext(ctx, `SELECT last_user_event_seq, last_resident_event_seq, unresolved_reference_markers, idle_duration FROM runtime_states WHERE resident_id = ?`, fixture.residentID.String()).Scan(&result.LastUserSeq, &result.LastResidentSeq, &result.UnresolvedMarkers, &result.IdleDuration); err != nil {
		t.Fatal(err)
	}
	return result
}

var _ generation.Generator = (*scriptedGenerator)(nil)
