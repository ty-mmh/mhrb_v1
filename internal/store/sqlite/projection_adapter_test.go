package sqlite

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"mahoroba.local/mahoroba/internal/canonical"
	"mahoroba.local/mahoroba/internal/memory"
	"mahoroba.local/mahoroba/internal/projection"
	"mahoroba.local/mahoroba/internal/surfaceref"
)

func TestProjectionTransactionCommitsBodyWatermarkAndDependenciesAtomically(t *testing.T) {
	database := openProjectionTestStore(t)
	residentID := projectionTestID(t, "00000000000000000000000001")
	dependencyID := projectionTestID(t, "00000000000000000000000002")
	definition := projection.Definition{
		Name: projection.RuntimeStatesName, Version: "runtime-states-v1", TimeSensitive: true,
		Dependencies: []projection.DependencyKind{projection.SessionizationPolicyDependency},
	}
	testRegistry, err := projection.NewRegistry(definition)
	if err != nil {
		t.Fatal(err)
	}
	repository := database.Projection()
	repository.registry = testRegistry
	request := projection.ApplyRequest{
		Definition: definition, ResidentID: residentID,
		Plan:       projection.UpdatePlan{Kind: projection.FullBuild, NeedCommitCatchUp: true, NeedAsOfReEvaluation: true},
		Evaluation: projection.Evaluation{Value: RuntimeStateProjection{IdleDuration: canonical.Duration(42)}},
		Watermark: projection.Watermark{
			ProjectionName: definition.Name, ResidentID: residentID, ProjectionVersion: definition.Version,
			SourceCommitSeq: mustProjectionCommitSeq(t, 1), AsOf: 100, AsOfTZ: canonical.MustTimezone("UTC"),
			Dependencies: []projection.Dependency{{Kind: projection.SessionizationPolicyDependency, VersionID: dependencyID}},
		},
	}
	if err := repository.Apply(context.Background(), request); err != nil {
		t.Fatal(err)
	}
	var idle, bodyCount, dependencyCount int64
	if err := database.reader.QueryRow(`SELECT idle_duration FROM runtime_states WHERE resident_id = ?`, residentID.String()).Scan(&idle); err != nil {
		t.Fatal(err)
	}
	if err := database.reader.QueryRow(`SELECT count(*) FROM projection_watermarks WHERE resident_id = ?`, residentID.String()).Scan(&bodyCount); err != nil {
		t.Fatal(err)
	}
	if err := database.reader.QueryRow(`SELECT count(*) FROM projection_watermark_dependencies WHERE resident_id = ?`, residentID.String()).Scan(&dependencyCount); err != nil {
		t.Fatal(err)
	}
	if idle != 42 || bodyCount != 1 || dependencyCount != 1 {
		t.Fatalf("atomic Projection state = idle:%d watermark:%d dependencies:%d", idle, bodyCount, dependencyCount)
	}
}

func TestProjectionRollbackCannotAdvanceWatermarkOrBody(t *testing.T) {
	t.Run("after body", func(t *testing.T) {
		database := openProjectionTestStore(t)
		residentID := projectionTestID(t, "00000000000000000000000003")
		repository := database.Projection()
		repository.applyHook = func(context.Context, string) error { return errors.New("injected Projection failure") }
		request := statusProjectionApply(t, residentID, nil, 1, "active")
		if err := repository.Apply(context.Background(), request); err == nil {
			t.Fatal("injected Projection failure unexpectedly committed")
		}
		assertProjectionRows(t, database, residentID, 0, 0)
	})
	t.Run("after dependency and watermark", func(t *testing.T) {
		database := openProjectionTestStore(t)
		residentID := projectionTestID(t, "00000000000000000000000009")
		dependencyID := projectionTestID(t, "0000000000000000000000000A")
		definition := projection.Definition{
			Name: projection.RuntimeStatesName, Version: "runtime-states-v1", TimeSensitive: true,
			Dependencies: []projection.DependencyKind{projection.SessionizationPolicyDependency},
		}
		registry, err := projection.NewRegistry(definition)
		if err != nil {
			t.Fatal(err)
		}
		repository := database.Projection()
		repository.registry = registry
		repository.applyHook = func(_ context.Context, stage string) error {
			if stage == "after_watermark" {
				return errors.New("injected failure after dependency replacement")
			}
			return nil
		}
		request := projection.ApplyRequest{
			Definition: definition, ResidentID: residentID,
			Plan:       projection.UpdatePlan{Kind: projection.FullBuild, NeedCommitCatchUp: true, NeedAsOfReEvaluation: true},
			Evaluation: projection.Evaluation{Value: RuntimeStateProjection{}},
			Watermark: projection.Watermark{
				ProjectionName: definition.Name, ResidentID: residentID, ProjectionVersion: definition.Version,
				SourceCommitSeq: mustProjectionCommitSeq(t, 1), AsOf: 100, AsOfTZ: canonical.MustTimezone("UTC"),
				Dependencies: []projection.Dependency{{Kind: projection.SessionizationPolicyDependency, VersionID: dependencyID}},
			},
		}
		if err := repository.Apply(context.Background(), request); err == nil {
			t.Fatal("post-watermark failure unexpectedly committed")
		}
		for _, table := range []string{"runtime_states", "projection_watermarks", "projection_watermark_dependencies"} {
			var count int
			if err := database.reader.QueryRow(`SELECT count(*) FROM `+table+` WHERE resident_id = ?`, residentID.String()).Scan(&count); err != nil {
				t.Fatal(err)
			}
			if count != 0 {
				t.Fatalf("%s rows after rollback = %d", table, count)
			}
		}
	})
}

func TestProjectionApplyTimeoutRollsBackEntireTransaction(t *testing.T) {
	database := openProjectionTestStore(t)
	residentID := projectionTestID(t, "00000000000000000000000004")
	repository := database.ProjectionWithOptions(ProjectionOptions{TransactionTimeout: 20 * time.Millisecond})
	repository.applyHook = func(ctx context.Context, _ string) error {
		<-ctx.Done()
		return ctx.Err()
	}
	err := repository.Apply(context.Background(), statusProjectionApply(t, residentID, nil, 1, "active"))
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Projection timeout error = %v", err)
	}
	assertProjectionRows(t, database, residentID, 0, 0)
}

func TestProjectionCASConflictRollsBackStaleEvaluation(t *testing.T) {
	database := openProjectionTestStore(t)
	residentID := projectionTestID(t, "00000000000000000000000005")
	repository := database.Projection()
	initial := statusProjectionApply(t, residentID, nil, 1, "draft")
	if err := repository.Apply(context.Background(), initial); err != nil {
		t.Fatal(err)
	}
	stale := statusProjectionApply(t, residentID, nil, 2, "active")
	if err := repository.Apply(context.Background(), stale); !errors.Is(err, projection.ErrCASConflict) {
		t.Fatalf("stale apply error = %v, want CAS conflict", err)
	}
	var status string
	if err := database.reader.QueryRow(`SELECT status FROM resident_current_status WHERE resident_id = ?`, residentID.String()).Scan(&status); err != nil {
		t.Fatal(err)
	}
	if status != "draft" {
		t.Fatalf("status after stale apply = %s", status)
	}
}

func TestProjectionWatermarkReadUsesOneConsistentSnapshot(t *testing.T) {
	database := openProjectionTestStore(t)
	residentID := projectionTestID(t, "0000000000000000000000000B")
	dependencyOne := projectionTestID(t, "0000000000000000000000000C")
	dependencyTwo := projectionTestID(t, "0000000000000000000000000D")
	definition := projection.Definition{
		Name: projection.RuntimeStatesName, Version: "runtime-states-v1", TimeSensitive: true,
		Dependencies: []projection.DependencyKind{projection.SessionizationPolicyDependency},
	}
	registry, err := projection.NewRegistry(definition)
	if err != nil {
		t.Fatal(err)
	}
	repository := database.Projection()
	repository.registry = registry
	apply := func(observed *projection.Watermark, sequence int64, dependency canonical.ID, kind projection.UpdateKind) error {
		return repository.Apply(context.Background(), projection.ApplyRequest{
			Definition: definition, ResidentID: residentID, Observed: observed,
			Plan: projection.UpdatePlan{
				Kind: kind, NeedCommitCatchUp: true, NeedAsOfReEvaluation: true,
			},
			Evaluation: projection.Evaluation{Value: RuntimeStateProjection{}},
			Watermark: projection.Watermark{
				ProjectionName: definition.Name, ResidentID: residentID, ProjectionVersion: definition.Version,
				SourceCommitSeq: mustProjectionCommitSeq(t, sequence), AsOf: canonical.Instant(100 + sequence),
				AsOfTZ:       canonical.MustTimezone("UTC"),
				Dependencies: []projection.Dependency{{Kind: projection.SessionizationPolicyDependency, VersionID: dependency}},
			},
		})
	}
	if err := apply(nil, 1, dependencyOne, projection.FullBuild); err != nil {
		t.Fatal(err)
	}
	var writer sync.WaitGroup
	writer.Add(1)
	writeErr := make(chan error, 1)
	go func() {
		defer writer.Done()
		for sequence := int64(2); sequence <= 50; sequence++ {
			observed, exists, err := repository.Watermark(context.Background(), definition.Name, residentID)
			if err != nil || !exists {
				writeErr <- fmt.Errorf("read observed watermark: exists=%v: %w", exists, err)
				return
			}
			dependency := dependencyOne
			if sequence%2 == 0 {
				dependency = dependencyTwo
			}
			if err := apply(&observed, sequence, dependency, projection.FullRebuild); err != nil {
				writeErr <- err
				return
			}
		}
	}()
	for {
		select {
		case err := <-writeErr:
			t.Fatal(err)
		default:
		}
		watermark, exists, err := repository.Watermark(context.Background(), definition.Name, residentID)
		if err != nil || !exists || len(watermark.Dependencies) != 1 {
			t.Fatalf("concurrent watermark read = %+v, %v, %v", watermark, exists, err)
		}
		want := dependencyOne
		if watermark.SourceCommitSeq.Int64()%2 == 0 {
			want = dependencyTwo
		}
		if watermark.Dependencies[0].VersionID != want {
			t.Fatalf("torn watermark snapshot: seq=%s dependency=%s want=%s", watermark.SourceCommitSeq, watermark.Dependencies[0].VersionID, want)
		}
		if watermark.SourceCommitSeq.Int64() == 50 {
			break
		}
	}
	writer.Wait()
	select {
	case err := <-writeErr:
		t.Fatal(err)
	default:
	}
}

func TestProjectionStoreRejectsPersistentAsOfRegression(t *testing.T) {
	database := openProjectionTestStore(t)
	residentID := projectionTestID(t, "00000000000000000000000006")
	repository := database.Projection()
	initial := statusProjectionApply(t, residentID, nil, 1, "draft")
	initial.Watermark.AsOf = 200
	if err := repository.Apply(context.Background(), initial); err != nil {
		t.Fatal(err)
	}
	observed, exists, err := repository.Watermark(context.Background(), projection.ResidentCurrentStatusName, residentID)
	if err != nil || !exists {
		t.Fatalf("stored watermark = %+v, %v, %v", observed, exists, err)
	}
	regressed := statusProjectionApply(t, residentID, &observed, 2, "active")
	regressed.Watermark.AsOf = 199
	if err := repository.Apply(context.Background(), regressed); !errors.Is(err, projection.ErrAsOfRegression) {
		t.Fatalf("as_of regression error = %v", err)
	}
	var status string
	if err := database.reader.QueryRow(`SELECT status FROM resident_current_status WHERE resident_id = ?`, residentID.String()).Scan(&status); err != nil {
		t.Fatal(err)
	}
	if status != "draft" {
		t.Fatalf("status after as_of regression = %s", status)
	}
}

func TestProjectionStoreRejectsUnexecutedCursorChanges(t *testing.T) {
	database := openProjectionTestStore(t)
	residentID := projectionTestID(t, "00000000000000000000000008")
	repository := database.Projection()
	initial := statusProjectionApply(t, residentID, nil, 1, "draft")
	if err := repository.Apply(context.Background(), initial); err != nil {
		t.Fatal(err)
	}
	observed, exists, err := repository.Watermark(context.Background(), projection.ResidentCurrentStatusName, residentID)
	if err != nil || !exists {
		t.Fatalf("stored watermark = %+v, %v, %v", observed, exists, err)
	}
	malformed := statusProjectionApply(t, residentID, &observed, 2, "active")
	malformed.Plan.NeedCommitCatchUp = false
	if err := repository.Apply(context.Background(), malformed); err == nil || !strings.Contains(err.Error(), "executes no cursor") {
		t.Fatalf("unexecuted cursor change error = %v", err)
	}
	malformed = statusProjectionApply(t, residentID, nil, 2, "active")
	malformed.Plan.Kind = projection.Update
	if err := repository.Apply(context.Background(), malformed); err == nil || !strings.Contains(err.Error(), "requires an observed") {
		t.Fatalf("incremental update without observed error = %v", err)
	}
	timeDefinition := projection.Definition{Name: projection.RuntimeStatesName, Version: "runtime-states-v1", TimeSensitive: true}
	timeObserved := projection.Watermark{
		ProjectionName: timeDefinition.Name, ResidentID: residentID, ProjectionVersion: timeDefinition.Version,
		SourceCommitSeq: mustProjectionCommitSeq(t, 1), AsOf: 100, AsOfTZ: canonical.MustTimezone("UTC"),
	}
	err = validateProjectionApplyPlan(projection.ApplyRequest{
		Definition: timeDefinition, ResidentID: residentID, Observed: &timeObserved,
		Plan: projection.UpdatePlan{Kind: projection.Update, NeedAsOfReEvaluation: true},
		Watermark: projection.Watermark{
			ProjectionName: timeDefinition.Name, ResidentID: residentID, ProjectionVersion: timeDefinition.Version,
			SourceCommitSeq: mustProjectionCommitSeq(t, 2), AsOf: 101, AsOfTZ: canonical.MustTimezone("UTC"),
		},
	})
	if err == nil || !strings.Contains(err.Error(), "unexecuted commit cursor") {
		t.Fatalf("unexecuted commit component error = %v", err)
	}
}

func TestRegistryDeclaresExactDependenciesForEveryProjection(t *testing.T) {
	registry, err := ActiveProjectionRegistry()
	if err != nil {
		t.Fatal(err)
	}
	want := map[projection.Name]projection.Version{
		projection.ResidentCurrentStatusName:   "resident-current-status-v1",
		projection.ResidentCurrentRevisionName: "resident-current-revision-v1",
		projection.RuntimeStatesName:           "runtime-states-v2",
		projection.ClaimStatesName:             "claim-states-v1",
		projection.ClaimViewScopeCurrentName:   "claim-view-scope-current-v1",
		projection.ContentReferencesName:       projection.ContentReferencesVersion,
	}
	definitions := registry.Definitions()
	if len(definitions) != len(want) {
		t.Fatalf("active Projection count = %d, want %d", len(definitions), len(want))
	}
	for _, definition := range definitions {
		if version, ok := want[definition.Name]; !ok || version != definition.Version {
			t.Fatalf("unexpected active definition: %+v", definition)
		}
		if definition.Name == projection.ClaimStatesName {
			if fmt.Sprint(definition.Dependencies) != fmt.Sprint([]projection.DependencyKind{projection.MemoryPolicyDependency}) ||
				fmt.Sprint(definition.RebuildOnActivation) != fmt.Sprint([]projection.DependencyKind{projection.MemoryPolicyDependency}) {
				t.Fatalf("claim_states dependency declaration = %+v", definition)
			}
		} else if len(definition.Dependencies) != 0 || len(definition.RebuildOnActivation) != 0 {
			t.Fatalf("active definition has unexpected dependencies: %+v", definition)
		}
	}
	claim := projection.ClaimStatesDefinition()
	if len(claim.Dependencies) != 1 || claim.Dependencies[0] != projection.MemoryPolicyDependency ||
		len(claim.RebuildOnActivation) != 1 || claim.RebuildOnActivation[0] != projection.MemoryPolicyDependency {
		t.Fatalf("production claim dependency declaration = %+v", claim)
	}
}

func TestCOV5RuntimeStatesV2ReplacesLatestUserMarkersAndClearsOnErasure(t *testing.T) {
	fixture, closeFixture := newSemanticFixture(t)
	defer closeFixture()
	residentID := projectionTestID(t, fixture.resident["A"])
	mustExec(t, fixture.db, `INSERT INTO resident_status_transitions(
		resident_status_transition_id, canonical_commit_id, resident_id, from_status, to_status,
		actor_principal_id, reason_code, reason_content_id, occurred_at, occurred_tz,
		recorded_at, recorded_tz
	) VALUES (?, ?, ?, NULL, 'active', ?, 'activate', NULL, ?, ?, ?, ?)`,
		fixture.ids.new(), fixture.commit["A"], residentID.String(), fixture.principal["human"],
		semanticTime, semanticTZ, semanticTime, semanticTZ)

	markerContent := fixture.addContent(t, "A", "event_payload", "前回の続きを再開しよう", "independent")
	markerEvent := fixture.ids.new()
	mustExec(t, fixture.db, `INSERT INTO events(
		event_id, canonical_commit_id, resident_id, seq, event_type, visibility,
		delivery_screen, delivery_audio, ingress, trust_level, actor_principal_id,
		target_principal_id, generation_run_id, occurred_at, occurred_tz, recorded_at,
		recorded_tz, content_id, payload_commitment, prev_event_hash, event_hash,
		event_hash_algorithm, event_hash_domain, canonicalization_version
	) VALUES (?, ?, ?, 2, 'user_message', 'conversation', 1, 0, 'local_ui', 'trusted',
		?, ?, NULL, ?, ?, ?, ?, ?, ?, NULL, ?, 'sha256', 'mahoroba:event-hash:v1', 'mahoroba-jcs-v1')`,
		markerEvent, fixture.commit["A"], residentID.String(), fixture.principal["human"],
		fixture.principal["A"], semanticTime+1, semanticTZ, semanticTime+1, semanticTZ,
		markerContent, semanticDigest("runtime-v2-marker-payload"), semanticDigest("runtime-v2-marker-event"))

	repository := fixtureStoreRepository(fixture).store.Projection()
	definition := projection.Definition{Name: projection.RuntimeStatesName, Version: "runtime-states-v2", TimeSensitive: true}
	target := projection.Target{
		Head: canonical.Head{Exists: true, CommitSeq: canonical.CommitSeq(3), CommittedAt: canonical.Instant(semanticTime + 2)},
		AsOf: canonical.Instant(semanticTime + 10), AsOfTZ: canonical.MustTimezone(semanticTZ),
	}
	evaluate := func() RuntimeStateProjection {
		t.Helper()
		result, err := repository.Evaluate(context.Background(), projection.EvaluationRequest{
			Definition: definition, ResidentID: residentID, Target: target,
			Plan: projection.UpdatePlan{Kind: projection.FullBuild},
		})
		if err != nil {
			t.Fatal(err)
		}
		return result.Value.(RuntimeStateProjection)
	}
	state := evaluate()
	if state.LastUserEventSeq == nil || state.LastUserEventSeq.Int64() != 2 ||
		fmt.Sprint(state.UnresolvedReferenceMarkers) != fmt.Sprint([]surfaceref.Marker{
			surfaceref.ContinuationRequest, surfaceref.PriorContextReference,
		}) {
		t.Fatalf("marker runtime state = %+v", state)
	}
	tx, err := fixture.db.BeginTx(context.Background(), nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := replaceProjectionBody(context.Background(), tx, projection.RuntimeStatesName, residentID, state); err != nil {
		_ = tx.Rollback()
		t.Fatal(err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	var stored string
	if err := fixture.db.QueryRow(`SELECT unresolved_reference_markers FROM runtime_states WHERE resident_id = ?`, residentID.String()).Scan(&stored); err != nil {
		t.Fatal(err)
	}
	if stored != `["continuation_request","prior_context_reference"]` {
		t.Fatalf("stored marker set = %s", stored)
	}

	plainContent := fixture.addContent(t, "A", "event_payload", "a new topic", "independent")
	mustExec(t, fixture.db, `INSERT INTO events(
		event_id, canonical_commit_id, resident_id, seq, event_type, visibility,
		delivery_screen, delivery_audio, ingress, trust_level, actor_principal_id,
		target_principal_id, generation_run_id, occurred_at, occurred_tz, recorded_at,
		recorded_tz, content_id, payload_commitment, prev_event_hash, event_hash,
		event_hash_algorithm, event_hash_domain, canonicalization_version
	) VALUES (?, ?, ?, 3, 'user_message', 'conversation', 1, 0, 'local_ui', 'trusted',
		?, ?, NULL, ?, ?, ?, ?, ?, ?, NULL, ?, 'sha256', 'mahoroba:event-hash:v1', 'mahoroba-jcs-v1')`,
		fixture.ids.new(), fixture.commit["A"], residentID.String(), fixture.principal["human"],
		fixture.principal["A"], semanticTime+2, semanticTZ, semanticTime+2, semanticTZ,
		plainContent, semanticDigest("runtime-v2-plain-payload"), semanticDigest("runtime-v2-plain-event"))
	state = evaluate()
	if len(state.UnresolvedReferenceMarkers) != 0 || state.LastUserEventSeq == nil || state.LastUserEventSeq.Int64() != 3 {
		t.Fatalf("replacement runtime state = %+v", state)
	}
	mustExec(t, fixture.db, `UPDATE content_objects
		SET erasure_state = 'erased', blob_hash = NULL, commitment_salt = NULL WHERE content_id = ?`, plainContent)
	state = evaluate()
	if len(state.UnresolvedReferenceMarkers) != 0 || state.LastUserEventSeq == nil || state.LastUserEventSeq.Int64() != 3 {
		t.Fatalf("erased latest-user runtime state = %+v", state)
	}

	invalidBytes := []byte{0xff}
	invalidHash := canonical.HashBlob(invalidBytes).Bytes()
	invalidContent := fixture.ids.new()
	mustExec(t, fixture.db, `INSERT INTO blobs(
		dedupe_scope_id, hash_algorithm, blob_hash, content, byte_size, encoding,
		compression, created_at, created_tz
	) VALUES (?, 'sha256', ?, ?, 1, 'utf-8', 'none', ?, ?)`,
		residentID.String(), invalidHash, invalidBytes, semanticTime+3, semanticTZ)
	mustExec(t, fixture.db, `INSERT INTO content_objects(
		content_id, owner_resident_id, content_class, blob_hash, blob_hash_algorithm,
		commitment, commitment_salt, commitment_hash_algorithm, commitment_domain,
		canonicalization_version, erasure_state, erasure_policy, created_at, created_tz
	) VALUES (?, ?, 'event_payload', ?, 'sha256', ?, ?, 'sha256',
		'mahoroba:content-commitment:v1', 'mahoroba-jcs-v1', 'present', 'independent', ?, ?)`,
		invalidContent, residentID.String(), invalidHash, semanticDigest("runtime-v2-invalid-commitment"),
		semanticDigest("runtime-v2-invalid-salt"), semanticTime+3, semanticTZ)
	mustExec(t, fixture.db, `INSERT INTO events(
		event_id, canonical_commit_id, resident_id, seq, event_type, visibility,
		delivery_screen, delivery_audio, ingress, trust_level, actor_principal_id,
		target_principal_id, generation_run_id, occurred_at, occurred_tz, recorded_at,
		recorded_tz, content_id, payload_commitment, prev_event_hash, event_hash,
		event_hash_algorithm, event_hash_domain, canonicalization_version
	) VALUES (?, ?, ?, 4, 'user_message', 'conversation', 1, 0, 'local_ui', 'trusted',
		?, ?, NULL, ?, ?, ?, ?, ?, ?, NULL, ?, 'sha256', 'mahoroba:event-hash:v1', 'mahoroba-jcs-v1')`,
		fixture.ids.new(), fixture.commit["A"], residentID.String(), fixture.principal["human"],
		fixture.principal["A"], semanticTime+3, semanticTZ, semanticTime+3, semanticTZ,
		invalidContent, semanticDigest("runtime-v2-invalid-payload"), semanticDigest("runtime-v2-invalid-event"))
	_, err = repository.Evaluate(context.Background(), projection.EvaluationRequest{
		Definition: definition, ResidentID: residentID, Target: target,
		Plan: projection.UpdatePlan{Kind: projection.FullBuild},
	})
	if !errors.Is(err, surfaceref.ErrInvalidUTF8) {
		t.Fatalf("invalid UTF-8 latest-user projection error = %v", err)
	}
}

func TestCOV5RuntimeAndBackfillTailQueriesStayIndexBoundedOnLargeHistory(t *testing.T) {
	fixture, closeFixture := newSemanticFixture(t)
	defer closeFixture()
	// A single statement creates a history beyond the requested 50k threshold;
	// the production queries must still resolve their bounded tails through the
	// resident/type/seq index without a history-sized sort.
	mustExec(t, fixture.db, `WITH RECURSIVE sequence(value) AS (
		VALUES(2) UNION ALL SELECT value + 1 FROM sequence WHERE value < 50002
	) INSERT INTO events(
		event_id, canonical_commit_id, resident_id, seq, event_type, visibility,
		delivery_screen, delivery_audio, ingress, trust_level, actor_principal_id,
		target_principal_id, generation_run_id, occurred_at, occurred_tz, recorded_at,
		recorded_tz, content_id, payload_commitment, prev_event_hash, event_hash,
		event_hash_algorithm, event_hash_domain, canonicalization_version
	) SELECT printf('%026d', 100000 + value), ?, ?, value, 'user_message', 'conversation',
		1, 0, 'local_ui', 'trusted', ?, ?, NULL, ?, ?, ? + value, ?, ?, zeroblob(32), NULL,
		CAST(printf('%032d', value) AS BLOB), 'sha256', 'mahoroba:event-hash:v1', 'mahoroba-jcs-v1'
	FROM sequence`, fixture.commit["A"], fixture.resident["A"], fixture.principal["human"],
		fixture.principal["A"], semanticTime, semanticTZ, semanticTime, semanticTZ, fixture.content["A"]["event"])

	userPlan := explainMemoryDiscoveryQueryPlan(t, fixture, runtimeLatestUserEventQuery,
		fixture.resident["A"], int64(2))
	residentPlan := explainMemoryDiscoveryQueryPlan(t, fixture, runtimeLatestResidentEventQuery,
		fixture.resident["A"], int64(2), fixture.resident["A"], int64(2), fixture.resident["A"], int64(2))
	var backfillPlans []string
	for _, eventType := range []string{"user_message", "resident_message", "outbound_initiative"} {
		backfillPlans = append(backfillPlans, explainMemoryDiscoveryQueryPlan(t, fixture,
			automaticDialogueBackfillEventTailQuery,
			fixture.resident["A"], eventType, int64(50003), int64(2)))
	}
	backfillPlan := strings.Join(backfillPlans, "\n")
	for name, plan := range map[string]string{"latest user": userPlan, "latest resident": residentPlan, "Backfill tail": backfillPlan} {
		if !strings.Contains(plan, "idx_events_resident_type_seq") {
			t.Fatalf("%s plan misses resident/type/seq index:\n%s", name, plan)
		}
		for _, forbidden := range []string{"SCAN e", "USE TEMP B-TREE", "AUTOMATIC"} {
			if strings.Contains(plan, forbidden) {
				t.Fatalf("%s plan contains %q:\n%s", name, forbidden, plan)
			}
		}
	}
	if count := strings.Count(residentPlan, "idx_events_resident_type_seq"); count != 3 {
		t.Fatalf("latest resident plan index branches = %d, want 3:\n%s", count, residentPlan)
	}
	if count := strings.Count(backfillPlan, "idx_events_resident_type_seq"); count != 3 {
		t.Fatalf("Backfill plan index branches = %d, want 3:\n%s", count, backfillPlan)
	}
	var latest int64
	var recordedAt int64
	var erasure string
	var content []byte
	if err := fixture.db.QueryRow(runtimeLatestUserEventQuery, fixture.resident["A"], int64(2)).Scan(
		&latest, &recordedAt, &erasure, &content,
	); err != nil {
		t.Fatal(err)
	}
	if latest != 50002 {
		t.Fatalf("large-history latest user seq = %d, want 50002", latest)
	}
}

func TestM4I102SQLiteActivationRangeForcesClaimRebuild(t *testing.T) {
	fixture, closeFixture := newSemanticFixture(t)
	defer closeFixture()
	ctx := context.Background()
	residentID := projectionTestID(t, fixture.resident["A"])
	commitID := fixture.ids.new()
	mustExec(t, fixture.db, `INSERT INTO canonical_commits(
		canonical_commit_id, commit_seq, resident_id, committed_at, committed_tz
	) VALUES (?, 4, ?, ?, ?)`, commitID, residentID.String(), semanticTime+4, semanticTZ)
	mustExec(t, fixture.db, `INSERT INTO resident_revision_activations(
		activation_id, canonical_commit_id, resident_id, revision_id, actor_principal_id,
		approval_id, reason_code, reason_content_id, recorded_at, recorded_tz
	) VALUES (?, ?, ?, ?, ?, NULL, 'activate-memory-policy', NULL, ?, ?)`, fixture.ids.new(), commitID,
		residentID.String(), fixture.revision["A"]["memory_policy"], fixture.principal["human"], semanticTime+4, semanticTZ)
	repository := &ProjectionRepository{store: &Store{reader: fixture.db}}
	after, through := mustProjectionCommitSeq(t, 2), mustProjectionCommitSeq(t, 4)
	activated, err := repository.DependencyActivated(ctx, projection.ActivationRequest{
		Definition: projection.ClaimStatesDefinition(), Kind: projection.MemoryPolicyDependency,
		ResidentID: residentID, After: after, Through: through,
	})
	if err != nil || !activated {
		t.Fatalf("activation range result = %v / %v", activated, err)
	}
	definition := projection.ClaimStatesDefinition()
	policyID := projectionTestID(t, fixture.revision["A"]["memory_policy"])
	stored := projection.Watermark{
		ProjectionName: definition.Name, ResidentID: residentID, ProjectionVersion: definition.Version,
		SourceCommitSeq: after, AsOf: 100, AsOfTZ: canonical.MustTimezone("UTC"),
		Dependencies: []projection.Dependency{{Kind: projection.MemoryPolicyDependency, VersionID: policyID}},
	}
	plan, err := projection.PlanUpdate(projection.PlanInput{
		Definition: definition, ResidentID: residentID,
		Target: projection.Target{
			Head: canonical.Head{Exists: true, CommitSeq: through, CommittedAt: 104},
			AsOf: 200, AsOfTZ: canonical.MustTimezone("UTC"),
		},
		Stored: &stored, DesiredDependencies: stored.Dependencies, ActivationDetected: activated,
	})
	if err != nil || plan.Kind != projection.FullRebuild || plan.Reason != projection.ReasonDependencyActivation {
		t.Fatalf("activation rebuild plan/error = %+v / %v", plan, err)
	}
}

func TestM5ClaimStatesSQLiteEvaluationUsesTypedCapturedReplay(t *testing.T) {
	fixture, closeFixture := newSemanticFixture(t)
	defer closeFixture()
	residentID := projectionTestID(t, fixture.resident["A"])
	policyID := projectionTestID(t, fixture.revision["A"]["memory_policy"])
	mustExec(t, fixture.db, `INSERT INTO resident_revision_activations(
		activation_id, canonical_commit_id, resident_id, revision_id, actor_principal_id,
		approval_id, reason_code, reason_content_id, recorded_at, recorded_tz
	) VALUES (?, ?, ?, ?, ?, NULL, 'activate-memory-policy', NULL, ?, ?)`, fixture.ids.new(),
		fixture.commit["A"], residentID.String(), policyID.String(), fixture.principal["human"], semanticTime, semanticTZ)

	var captured projection.ClaimEvaluationInput
	repository := &ProjectionRepository{
		store: &Store{reader: fixture.db},
		claimStateEvaluator: projection.ClaimStateEvaluatorFunc(func(_ context.Context, input projection.ClaimEvaluationInput) (projection.ClaimStateMetrics, error) {
			captured = input
			return projection.ClaimStateMetrics{
				Salience: 0.75, Confidence: canonical.Ratio(900_000), Currentness: canonical.Ratio(1_000_000),
				TemporalRelation: projection.ClaimTemporalCurrent,
			}, nil
		}),
	}
	target := projection.Target{
		Head: canonical.Head{Exists: true, CommitSeq: mustProjectionCommitSeq(t, 3), CommittedAt: canonical.Instant(semanticTime + 2)},
		AsOf: canonical.Instant(semanticTime), AsOfTZ: canonical.MustTimezone(semanticTZ),
	}
	definition := projection.ClaimStatesDefinition()
	dependencies := []projection.Dependency{{Kind: projection.MemoryPolicyDependency, VersionID: policyID}}
	evaluation, err := repository.Evaluate(context.Background(), projection.EvaluationRequest{
		Definition: definition, ResidentID: residentID, Target: target, Dependencies: dependencies,
		Plan: projection.UpdatePlan{Kind: projection.FullBuild, NeedCommitCatchUp: true, NeedAsOfReEvaluation: true},
	})
	if err != nil {
		t.Fatal(err)
	}
	states, ok := evaluation.Value.([]projection.ClaimState)
	if !ok || len(states) != 1 || states[0].ClaimID.String() != fixture.claim["A"] || states[0].EvidenceCount != 1 {
		t.Fatalf("claim_states evaluation = %#v", evaluation.Value)
	}
	if captured.ActivePolicyRevisionID != policyID || captured.Status != projection.ClaimStatusActive ||
		captured.Stage != projection.ClaimStageFloating || len(captured.Evidence) != 1 ||
		captured.Evidence[0].PolicyRevisionID != policyID {
		t.Fatalf("typed replay input = %+v", captured)
	}
	previous := projection.Watermark{
		ProjectionName: definition.Name, ResidentID: residentID, ProjectionVersion: definition.Version,
		SourceCommitSeq: mustProjectionCommitSeq(t, 2), AsOf: canonical.Instant(semanticTime),
		AsOfTZ: canonical.MustTimezone(semanticTZ), Dependencies: dependencies,
	}
	incremental, err := repository.Evaluate(context.Background(), projection.EvaluationRequest{
		Definition: definition, ResidentID: residentID, Target: target, Previous: &previous, Dependencies: dependencies,
		Plan: projection.UpdatePlan{Kind: projection.Update, NeedCommitCatchUp: true},
	})
	if err != nil {
		t.Fatal(err)
	}
	if fmt.Sprint(incremental.Value) != fmt.Sprint(evaluation.Value) {
		t.Fatalf("incremental claim evaluation = %#v, full = %#v", incremental.Value, evaluation.Value)
	}
}

func TestM5SQLiteDefaultClaimStateEvaluatorUsesVersionedMemoryPolicies(t *testing.T) {
	fixture, closeFixture := newSemanticFixture(t)
	defer closeFixture()
	residentID := projectionTestID(t, fixture.resident["A"])
	policyJSON, err := memory.DefaultPolicyV2().CanonicalJSON()
	if err != nil {
		t.Fatal(err)
	}
	policyContentID := fixture.addContent(t, "A", "memory_policy_text", policyJSON.String(), "resident_only")
	commitFour := fixture.ids.new()
	policyID := fixture.ids.new()
	mustExec(t, fixture.db, `INSERT INTO canonical_commits(
		canonical_commit_id, commit_seq, resident_id, committed_at, committed_tz
	) VALUES (?, 4, ?, ?, ?)`, commitFour, residentID.String(), semanticTime+3, semanticTZ)
	mustExec(t, fixture.db, `INSERT INTO resident_revisions(
		revision_id, canonical_commit_id, resident_id, revision_class, content_id,
		parent_revision_id, created_by_run_id, reason_content_id, recorded_at, recorded_tz
	) VALUES (?, ?, ?, 'memory_policy', ?, ?, NULL, NULL, ?, ?)`, policyID, commitFour,
		residentID.String(), policyContentID, fixture.revision["A"]["memory_policy"], semanticTime+3, semanticTZ)
	parsedPolicyID := projectionTestID(t, policyID)
	claimID := projectionTestID(t, fixture.claim["A"])
	repository := (&Store{reader: fixture.db}).Projection()
	if _, ok := repository.claimStateEvaluator.(*sqliteClaimStateEvaluator); !ok {
		t.Fatalf("default claim evaluator = %T", repository.claimStateEvaluator)
	}
	lastReference := canonical.Instant(semanticTime + 3)
	recallRunID := projectionTestID(t, "00000000000000000000000048")
	metrics, err := repository.claimStateEvaluator.EvaluateClaim(context.Background(), projection.ClaimEvaluationInput{
		Claim: projection.ClaimSeed{
			ClaimID: claimID, CommitSeq: mustProjectionCommitSeq(t, 2), RecordedAt: canonical.Instant(semanticTime),
			TemporalKind: projection.ClaimTemporalStable,
		},
		Status: projection.ClaimStatusActive, Stage: projection.ClaimStageFloating,
		AsOf: lastReference, ActivePolicyRevisionID: parsedPolicyID,
		Evidence: []projection.ClaimEvidence{{
			EvidenceID: projectionTestID(t, fixture.evidence["A"]), ClaimID: claimID,
			SourceEventID: projectionTestID(t, fixture.event["A"]), CommitSeq: mustProjectionCommitSeq(t, 2),
			RecordedAt: canonical.Instant(semanticTime), PolicyRevisionID: parsedPolicyID,
			SourceEventType: projection.ClaimSourceUserMessage, Polarity: projection.ClaimEvidenceSupport,
			Grade: projection.ClaimEvidenceStated, TrustLevel: projection.ClaimTrustTrusted,
			Derivation: projection.ClaimEvidenceExtracted, ReasonCode: string(memory.EvidenceReasonSourceStated),
			ActorIsSubject: true, Weight: canonical.Weight(1_000_000),
		}},
		Usages: []projection.ClaimUsage{
			{
				UsageID: projectionTestID(t, "00000000000000000000000045"), ClaimID: claimID,
				RecallRunID: &recallRunID, CommitSeq: mustProjectionCommitSeq(t, 4), RecordedAt: lastReference,
				PolicyRevisionID: parsedPolicyID, UsageType: projection.ClaimUsageCandidate,
			},
			{
				UsageID: projectionTestID(t, "00000000000000000000000046"), ClaimID: claimID,
				RecallRunID: &recallRunID, CommitSeq: mustProjectionCommitSeq(t, 4), RecordedAt: lastReference,
				PolicyRevisionID: parsedPolicyID, UsageType: projection.ClaimUsageSelected,
			},
			{
				UsageID: projectionTestID(t, "00000000000000000000000047"), ClaimID: claimID,
				RecallRunID: &recallRunID, CommitSeq: mustProjectionCommitSeq(t, 4), RecordedAt: lastReference,
				PolicyRevisionID: parsedPolicyID, UsageType: projection.ClaimUsagePromptIncluded,
			},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if metrics.Salience != 0.25 || metrics.Confidence != canonical.Ratio(1_000_000) ||
		metrics.Currentness != canonical.Ratio(1_000_000) || metrics.TemporalRelation != projection.ClaimTemporalCurrent ||
		metrics.LastReferencedAt == nil || *metrics.LastReferencedAt != lastReference {
		t.Fatalf("default claim metrics = %+v", metrics)
	}
}

func TestM5ProductionClaimUsagePerRecallRunMaxSurvivesDropRebuild(t *testing.T) {
	fixture, closeFixture := newSemanticFixture(t)
	defer closeFixture()
	residentID := projectionTestID(t, fixture.resident["A"])
	policyJSON, err := memory.DefaultPolicyV2().CanonicalJSON()
	if err != nil {
		t.Fatal(err)
	}
	policyContentID := fixture.addContent(t, "A", "memory_policy_text", policyJSON.String(), "resident_only")
	commitFour := fixture.ids.new()
	policyRaw := fixture.ids.new()
	mustExec(t, fixture.db, `INSERT INTO canonical_commits(
		canonical_commit_id, commit_seq, resident_id, committed_at, committed_tz
	) VALUES (?, 4, ?, ?, ?)`, commitFour, residentID.String(), semanticTime+3, semanticTZ)
	mustExec(t, fixture.db, `INSERT INTO resident_revisions(
		revision_id, canonical_commit_id, resident_id, revision_class, content_id,
		parent_revision_id, created_by_run_id, reason_content_id, recorded_at, recorded_tz
	) VALUES (?, ?, ?, 'memory_policy', ?, ?, NULL, NULL, ?, ?)`, policyRaw, commitFour,
		residentID.String(), policyContentID, fixture.revision["A"]["memory_policy"], semanticTime+3, semanticTZ)
	policyID := projectionTestID(t, policyRaw)
	claimID := projectionTestID(t, fixture.claim["A"])
	recallRunID := projectionTestID(t, fixture.recall["A"])
	asOf := canonical.Instant(semanticTime + 3)
	for ordinal, usageType := range []projection.ClaimUsageType{
		projection.ClaimUsageCandidate, projection.ClaimUsageSelected, projection.ClaimUsagePromptIncluded,
	} {
		mustExec(t, fixture.db, `INSERT INTO claim_usages(
			claim_usage_id, canonical_commit_id, claim_id, recall_run_id, generation_run_id,
			usage_type, ordinal, memory_policy_revision_id, exclusion_reason, detection_method,
			detection_confidence, detected_by_run_id, recorded_at, recorded_tz
		) VALUES (?, ?, ?, ?, NULL, ?, ?, ?, NULL, NULL, NULL, NULL, ?, ?)`,
			fixture.ids.new(), commitFour, claimID.String(), recallRunID.String(), string(usageType),
			ordinal, policyID.String(), asOf.UnixMicro(), semanticTZ)
	}
	input := projection.ClaimEvaluationInput{
		Claim: projection.ClaimSeed{
			ClaimID: claimID, CommitSeq: mustProjectionCommitSeq(t, 2),
			RecordedAt: canonical.Instant(semanticTime), TemporalKind: projection.ClaimTemporalStable,
		},
		Status: projection.ClaimStatusActive, Stage: projection.ClaimStageFloating,
		AsOf: asOf, ActivePolicyRevisionID: policyID,
		Evidence: []projection.ClaimEvidence{{
			EvidenceID: projectionTestID(t, fixture.evidence["A"]), ClaimID: claimID,
			SourceEventID: projectionTestID(t, fixture.event["A"]), CommitSeq: mustProjectionCommitSeq(t, 2),
			RecordedAt: canonical.Instant(semanticTime), PolicyRevisionID: policyID,
			SourceEventType: projection.ClaimSourceUserMessage, Polarity: projection.ClaimEvidenceSupport,
			Grade: projection.ClaimEvidenceStated, TrustLevel: projection.ClaimTrustTrusted,
			Derivation: projection.ClaimEvidenceExtracted, ReasonCode: string(memory.EvidenceReasonSourceStated),
			ActorIsSubject: true, Weight: canonical.Weight(1_000_000),
		}},
	}
	store := &Store{reader: fixture.db, writer: fixture.db, writes: newWritePriorityGate()}
	repository := store.Projection()
	input.Usages, err = repository.loadClaimUsages(context.Background(), residentID, projection.Target{
		Head: canonical.Head{Exists: true, CommitSeq: mustProjectionCommitSeq(t, 4), CommittedAt: asOf},
		AsOf: asOf, AsOfTZ: canonical.MustTimezone(semanticTZ),
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(input.Usages) != 3 {
		t.Fatalf("captured Recall usages = %d, want 3", len(input.Usages))
	}
	for _, usage := range input.Usages {
		if usage.RecallRunID == nil || *usage.RecallRunID != recallRunID {
			t.Fatalf("usage recall run = %v, want %s", usage.RecallRunID, recallRunID)
		}
	}
	evaluate := func() projection.ClaimState {
		metrics, err := repository.claimStateEvaluator.EvaluateClaim(context.Background(), input)
		if err != nil {
			t.Fatal(err)
		}
		if metrics.Salience != 0.25 {
			t.Fatalf("candidate+selected+prompt salience = %g, want per-run max 0.25", metrics.Salience)
		}
		return projection.ClaimState{
			ClaimID: claimID, Stage: input.Stage, Status: input.Status,
			Salience: metrics.Salience, Confidence: metrics.Confidence,
			Currentness: metrics.Currentness, TemporalRelation: metrics.TemporalRelation,
			LastReferencedAt: metrics.LastReferencedAt, EvidenceCount: 1,
		}
	}
	definition := projection.ClaimStatesDefinition()
	watermark := projection.Watermark{
		ProjectionName: definition.Name, ResidentID: residentID, ProjectionVersion: definition.Version,
		SourceCommitSeq: mustProjectionCommitSeq(t, 4), AsOf: asOf,
		AsOfTZ:       canonical.MustTimezone(semanticTZ),
		Dependencies: []projection.Dependency{{Kind: projection.MemoryPolicyDependency, VersionID: policyID}},
	}
	apply := func(state projection.ClaimState) {
		t.Helper()
		if err := repository.Apply(context.Background(), projection.ApplyRequest{
			Definition: definition, ResidentID: residentID,
			Plan:       projection.UpdatePlan{Kind: projection.FullBuild, NeedCommitCatchUp: true, NeedAsOfReEvaluation: true},
			Evaluation: projection.Evaluation{Value: []projection.ClaimState{state}}, Watermark: watermark,
		}); err != nil {
			t.Fatal(err)
		}
	}
	apply(evaluate())
	golden := readClaimProjectionGolden(t, store, residentID)
	observed, exists, err := repository.Watermark(context.Background(), definition.Name, residentID)
	if err != nil || !exists {
		t.Fatalf("claim watermark = %+v, exists=%v, err=%v", observed, exists, err)
	}
	if err := repository.Drop(context.Background(), projection.DropRequest{
		Definition: definition, ResidentID: residentID, Observed: &observed,
	}); err != nil {
		t.Fatal(err)
	}
	apply(evaluate())
	if rebuilt := readClaimProjectionGolden(t, store, residentID); rebuilt != golden {
		t.Fatalf("production claim usage drop/rebuild = %q, want %q", rebuilt, golden)
	}
}

func TestM5I101ClaimAggregateUsesPolicyActiveAtExplicitAsOf(t *testing.T) {
	fixture, closeFixture := newSemanticFixture(t)
	defer closeFixture()
	residentID := projectionTestID(t, fixture.resident["A"])
	policyJSON, err := memory.DefaultPolicyV2().CanonicalJSON()
	if err != nil {
		t.Fatal(err)
	}
	policyContentID := fixture.addContent(t, "A", "memory_policy_text", policyJSON.String(), "resident_only")
	oldPolicyRaw := fixture.ids.new()
	mustExec(t, fixture.db, `INSERT INTO resident_revisions(
		revision_id, canonical_commit_id, resident_id, revision_class, content_id,
		parent_revision_id, created_by_run_id, reason_content_id, recorded_at, recorded_tz
	) VALUES (?, ?, ?, 'memory_policy', ?, ?, NULL, NULL, ?, ?)`, oldPolicyRaw, fixture.commit["A"],
		residentID.String(), policyContentID, fixture.revision["A"]["memory_policy"], semanticTime, semanticTZ)
	oldPolicyID := projectionTestID(t, oldPolicyRaw)
	mustExec(t, fixture.db, `INSERT INTO resident_revision_activations(
		activation_id, canonical_commit_id, resident_id, revision_id, actor_principal_id,
		approval_id, reason_code, reason_content_id, recorded_at, recorded_tz
	) VALUES (?, ?, ?, ?, ?, NULL, 'activate-memory-policy', NULL, ?, ?)`, fixture.ids.new(),
		fixture.commit["A"], residentID.String(), oldPolicyID.String(), fixture.principal["human"], semanticTime, semanticTZ)

	commitFour := fixture.ids.new()
	newPolicyID := fixture.ids.new()
	mustExec(t, fixture.db, `INSERT INTO canonical_commits(
		canonical_commit_id, commit_seq, resident_id, committed_at, committed_tz
	) VALUES (?, 4, ?, ?, ?)`, commitFour, residentID.String(), semanticTime+3, semanticTZ)
	mustExec(t, fixture.db, `INSERT INTO resident_revisions(
		revision_id, canonical_commit_id, resident_id, revision_class, content_id,
		parent_revision_id, created_by_run_id, reason_content_id, recorded_at, recorded_tz
	) VALUES (?, ?, ?, 'memory_policy', ?, ?, NULL, NULL, ?, ?)`, newPolicyID, commitFour,
		residentID.String(), policyContentID, oldPolicyID.String(), semanticTime+100, semanticTZ)
	mustExec(t, fixture.db, `INSERT INTO resident_revision_activations(
		activation_id, canonical_commit_id, resident_id, revision_id, actor_principal_id,
		approval_id, reason_code, reason_content_id, recorded_at, recorded_tz
	) VALUES (?, ?, ?, ?, ?, NULL, 'activate-memory-policy', NULL, ?, ?)`, fixture.ids.new(), commitFour,
		residentID.String(), newPolicyID, fixture.principal["human"], semanticTime+100, semanticTZ)

	repository := &ProjectionRepository{store: &Store{reader: fixture.db}}
	resolve := func(head int64, asOf int64) canonical.ID {
		t.Helper()
		dependencies, err := repository.ResolveDependencies(context.Background(), projection.DependencyRequest{
			Definition: projection.ClaimStatesDefinition(), ResidentID: residentID,
			Target: projection.Target{
				Head: canonical.Head{Exists: true, CommitSeq: mustProjectionCommitSeq(t, head), CommittedAt: canonical.Instant(semanticTime + head)},
				AsOf: canonical.Instant(asOf), AsOfTZ: canonical.MustTimezone(semanticTZ),
			},
		})
		if err != nil {
			t.Fatal(err)
		}
		if len(dependencies) != 1 {
			t.Fatalf("dependencies = %+v", dependencies)
		}
		return dependencies[0].VersionID
	}
	if got := resolve(3, semanticTime+200); got != oldPolicyID {
		t.Fatalf("dependency beyond captured head = %s, want %s", got, oldPolicyID)
	}
	if got := resolve(4, semanticTime+99); got != oldPolicyID {
		t.Fatalf("dependency beyond captured as_of = %s, want %s", got, oldPolicyID)
	}
	if got := resolve(4, semanticTime+100); got.String() != newPolicyID {
		t.Fatalf("dependency at captured activation = %s, want %s", got, newPolicyID)
	}
}

func TestCOV4StoredClaimStatesDependencyUpgradeAndInvalidMetadataBoundary(t *testing.T) {
	fixture, closeFixture := newSemanticFixture(t)
	defer closeFixture()
	residentID := projectionTestID(t, fixture.resident["A"])
	policyID := projectionTestID(t, fixture.revision["A"]["memory_policy"])
	otherResidentPolicyID := projectionTestID(t, fixture.revision["B"]["memory_policy"])
	mustExec(t, fixture.db, `INSERT INTO resident_revision_activations(
		activation_id, canonical_commit_id, resident_id, revision_id, actor_principal_id,
		approval_id, reason_code, reason_content_id, recorded_at, recorded_tz
	) VALUES (?, ?, ?, ?, ?, NULL, 'activate-memory-policy', NULL, ?, ?)`, fixture.ids.new(),
		fixture.commit["A"], residentID.String(), policyID.String(), fixture.principal["human"], semanticTime, semanticTZ)

	definition := projection.ClaimStatesDefinition()
	target := projection.Target{
		Head: canonical.Head{Exists: true, CommitSeq: mustProjectionCommitSeq(t, 3), CommittedAt: canonical.Instant(semanticTime + 2)},
		AsOf: canonical.Instant(semanticTime), AsOfTZ: canonical.MustTimezone(semanticTZ),
	}
	repository := &ProjectionRepository{store: &Store{reader: fixture.db}}
	base := projection.Watermark{
		ProjectionName: definition.Name, ResidentID: residentID, ProjectionVersion: definition.Version,
		SourceCommitSeq: target.Head.CommitSeq, AsOf: target.AsOf, AsOfTZ: target.AsOfTZ,
	}

	legacy := base
	desired, err := repository.ResolveDependencies(context.Background(), projection.DependencyRequest{
		Definition: definition, ResidentID: residentID, Target: target, Previous: &legacy,
	})
	if err != nil || len(desired) != 1 || desired[0].VersionID != policyID {
		t.Fatalf("legacy zero desired/error = %+v / %v", desired, err)
	}
	plan, err := projection.PlanUpdate(projection.PlanInput{
		Definition: definition, ResidentID: residentID, Target: target, Stored: &legacy,
		DesiredDependencies: desired,
	})
	if err != nil || plan.Kind != projection.FullRebuild || plan.Reason != projection.ReasonDependencyMismatch {
		t.Fatalf("legacy zero plan/error = %+v / %v", plan, err)
	}

	unknownID := projectionTestID(t, fixture.ids.new())
	for _, test := range []struct {
		name         string
		dependencies []projection.Dependency
		asOf         canonical.Instant
	}{
		{name: "two dependencies", dependencies: []projection.Dependency{
			{Kind: projection.MemoryPolicyDependency, VersionID: policyID},
			{Kind: projection.MemoryPolicyDependency, VersionID: otherResidentPolicyID},
		}},
		{name: "unknown id", dependencies: []projection.Dependency{{Kind: projection.MemoryPolicyDependency, VersionID: unknownID}}},
		{name: "other resident", dependencies: []projection.Dependency{{Kind: projection.MemoryPolicyDependency, VersionID: otherResidentPolicyID}}},
		{name: "as_of closure mismatch", dependencies: []projection.Dependency{{Kind: projection.MemoryPolicyDependency, VersionID: policyID}}, asOf: canonical.Instant(semanticTime - 1)},
	} {
		t.Run(test.name, func(t *testing.T) {
			stored := base
			stored.Dependencies = append([]projection.Dependency(nil), test.dependencies...)
			if test.asOf != 0 {
				stored.AsOf = test.asOf
			}
			_, err := repository.ResolveDependencies(context.Background(), projection.DependencyRequest{
				Definition: definition, ResidentID: residentID, Target: target, Previous: &stored,
			})
			if !errors.Is(err, projection.ErrInvalidWatermarkMetadata) {
				t.Fatalf("error = %v, want ErrInvalidWatermarkMetadata", err)
			}
		})
	}
}

func TestM5MemoryPolicyDependencyResolutionFailsClosedForInvalidPolicyContent(t *testing.T) {
	fixture, closeFixture := newSemanticFixture(t)
	defer closeFixture()
	residentID := projectionTestID(t, fixture.resident["A"])
	invalidContentID := fixture.addContent(t, "A", "memory_policy_text", "not-json", "resident_only")
	invalidRevisionID := fixture.ids.new()
	mustExec(t, fixture.db, `INSERT INTO resident_revisions(
		revision_id, canonical_commit_id, resident_id, revision_class, content_id,
		parent_revision_id, created_by_run_id, reason_content_id, recorded_at, recorded_tz
	) VALUES (?, ?, ?, 'memory_policy', ?, ?, NULL, NULL, ?, ?)`, invalidRevisionID,
		fixture.commit["A"], residentID.String(), invalidContentID,
		fixture.revision["A"]["memory_policy"], semanticTime, semanticTZ)
	mustExec(t, fixture.db, `INSERT INTO resident_revision_activations(
		activation_id, canonical_commit_id, resident_id, revision_id, actor_principal_id,
		approval_id, reason_code, reason_content_id, recorded_at, recorded_tz
	) VALUES (?, ?, ?, ?, ?, NULL, 'activate-memory-policy', NULL, ?, ?)`, fixture.ids.new(),
		fixture.commit["A"], residentID.String(), invalidRevisionID,
		fixture.principal["human"], semanticTime, semanticTZ)

	repository := &ProjectionRepository{store: &Store{reader: fixture.db}}
	_, err := repository.ResolveDependencies(context.Background(), projection.DependencyRequest{
		Definition: projection.ClaimStatesDefinition(), ResidentID: residentID,
		Target: projection.Target{
			Head: canonical.Head{Exists: true, CommitSeq: mustProjectionCommitSeq(t, 3), CommittedAt: canonical.Instant(semanticTime + 2)},
			AsOf: canonical.Instant(semanticTime), AsOfTZ: canonical.MustTimezone(semanticTZ),
		},
	})
	if !errors.Is(err, projection.ErrUnresolvedDependency) {
		t.Fatalf("invalid policy dependency error = %v, want unresolved dependency", err)
	}
}

func TestM5ClaimViewScopeSQLiteEvaluationReplaysCurrentAssertion(t *testing.T) {
	fixture, closeFixture := newSemanticFixture(t)
	defer closeFixture()
	residentID := projectionTestID(t, fixture.resident["A"])
	assertionID := fixture.ids.new()
	mustExec(t, fixture.db, `INSERT INTO claim_view_scope_assertions(
		view_scope_assertion_id, canonical_commit_id, claim_id, view_scope, actor_principal_id,
		generation_run_id, memory_policy_revision_id, reason_code, reason_content_id, recorded_at, recorded_tz
	) VALUES (?, ?, ?, 'resident_ui', ?, NULL, ?, 'initial', NULL, ?, ?)`, assertionID,
		fixture.commit["A"], fixture.claim["A"], fixture.principal["human"],
		fixture.revision["A"]["memory_policy"], semanticTime, semanticTZ)
	repository := &ProjectionRepository{store: &Store{reader: fixture.db}}
	evaluation, err := repository.Evaluate(context.Background(), projection.EvaluationRequest{
		Definition: projection.ClaimViewScopeCurrentDefinition(), ResidentID: residentID,
		Target: projection.Target{
			Head: canonical.Head{Exists: true, CommitSeq: mustProjectionCommitSeq(t, 3), CommittedAt: canonical.Instant(semanticTime + 2)},
			AsOf: canonical.Instant(semanticTime), AsOfTZ: canonical.MustTimezone(semanticTZ),
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	states, ok := evaluation.Value.([]projection.ClaimViewScopeState)
	if !ok || len(states) != 1 || states[0].ClaimID.String() != fixture.claim["A"] ||
		states[0].ViewScope != projection.ClaimViewScopeResidentUI || states[0].SourceAssertionID.String() != assertionID {
		t.Fatalf("claim view scope evaluation = %#v", evaluation.Value)
	}
}

func TestM5ClaimProjectionDropRebuildMatchesGolden(t *testing.T) {
	database := openProjectionTestStore(t)
	repository := database.Projection()
	residentID := projectionTestID(t, "00000000000000000000000041")
	claimID := projectionTestID(t, "00000000000000000000000042")
	policyID := projectionTestID(t, "00000000000000000000000043")
	assertionID := projectionTestID(t, "00000000000000000000000044")
	lastReferenced := canonical.Instant(90)

	claimDefinition := projection.ClaimStatesDefinition()
	claimEvaluation := projection.Evaluation{Value: []projection.ClaimState{{
		ClaimID: claimID, Stage: projection.ClaimStageSediment, Status: projection.ClaimStatusActive,
		Salience: 0.5, Confidence: canonical.Ratio(750_000), Currentness: canonical.Ratio(600_000),
		TemporalRelation: projection.ClaimTemporalPast, LastReferencedAt: &lastReferenced, EvidenceCount: 2,
	}}}
	claimWatermark := projection.Watermark{
		ProjectionName: claimDefinition.Name, ResidentID: residentID, ProjectionVersion: claimDefinition.Version,
		SourceCommitSeq: mustProjectionCommitSeq(t, 1), AsOf: 100, AsOfTZ: canonical.MustTimezone("UTC"),
		Dependencies: []projection.Dependency{{Kind: projection.MemoryPolicyDependency, VersionID: policyID}},
	}
	claimApply := projection.ApplyRequest{
		Definition: claimDefinition, ResidentID: residentID,
		Plan:       projection.UpdatePlan{Kind: projection.FullBuild, NeedCommitCatchUp: true, NeedAsOfReEvaluation: true},
		Evaluation: claimEvaluation, Watermark: claimWatermark,
	}
	if err := repository.Apply(context.Background(), claimApply); err != nil {
		t.Fatal(err)
	}
	claimGolden := readClaimProjectionGolden(t, database, residentID)
	observed, exists, err := repository.Watermark(context.Background(), claimDefinition.Name, residentID)
	if err != nil || !exists {
		t.Fatalf("claim watermark = %+v, %v, %v", observed, exists, err)
	}
	if err := repository.Drop(context.Background(), projection.DropRequest{Definition: claimDefinition, ResidentID: residentID, Observed: &observed}); err != nil {
		t.Fatal(err)
	}
	if got := readClaimProjectionGolden(t, database, residentID); got != "" {
		t.Fatalf("claim body after drop = %q", got)
	}
	if err := repository.Apply(context.Background(), claimApply); err != nil {
		t.Fatal(err)
	}
	if rebuilt := readClaimProjectionGolden(t, database, residentID); rebuilt != claimGolden {
		t.Fatalf("claim rebuild = %q, want %q", rebuilt, claimGolden)
	}

	viewDefinition := projection.ClaimViewScopeCurrentDefinition()
	viewApply := projection.ApplyRequest{
		Definition: viewDefinition, ResidentID: residentID,
		Plan: projection.UpdatePlan{Kind: projection.FullBuild, NeedCommitCatchUp: true},
		Evaluation: projection.Evaluation{Value: []projection.ClaimViewScopeState{{
			ClaimID: claimID, ViewScope: projection.ClaimViewScopeAdminOnly, SourceAssertionID: assertionID,
		}}},
		Watermark: projection.Watermark{
			ProjectionName: viewDefinition.Name, ResidentID: residentID, ProjectionVersion: viewDefinition.Version,
			SourceCommitSeq: mustProjectionCommitSeq(t, 1), AsOf: 100, AsOfTZ: canonical.MustTimezone("UTC"),
		},
	}
	if err := repository.Apply(context.Background(), viewApply); err != nil {
		t.Fatal(err)
	}
	viewGolden := readClaimViewScopeGolden(t, database, residentID)
	viewObserved, exists, err := repository.Watermark(context.Background(), viewDefinition.Name, residentID)
	if err != nil || !exists {
		t.Fatalf("view watermark = %+v, %v, %v", viewObserved, exists, err)
	}
	if err := repository.Drop(context.Background(), projection.DropRequest{Definition: viewDefinition, ResidentID: residentID, Observed: &viewObserved}); err != nil {
		t.Fatal(err)
	}
	if got := readClaimViewScopeGolden(t, database, residentID); got != "" {
		t.Fatalf("view body after drop = %q", got)
	}
	if err := repository.Apply(context.Background(), viewApply); err != nil {
		t.Fatal(err)
	}
	if rebuilt := readClaimViewScopeGolden(t, database, residentID); rebuilt != viewGolden {
		t.Fatalf("view rebuild = %q, want %q", rebuilt, viewGolden)
	}
}

func readClaimProjectionGolden(t *testing.T, database *Store, residentID canonical.ID) string {
	t.Helper()
	rows, err := database.reader.Query(`SELECT claim_id, stage, status, salience, confidence, currentness,
		temporal_relation, coalesce(last_referenced_at, -1), evidence_count
		FROM claim_states WHERE resident_id = ? ORDER BY claim_id`, residentID.String())
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var values []string
	for rows.Next() {
		var claimID, stage, status, relation string
		var salience float64
		var confidence, currentness, lastReferenced, evidenceCount int64
		if err := rows.Scan(&claimID, &stage, &status, &salience, &confidence, &currentness, &relation, &lastReferenced, &evidenceCount); err != nil {
			t.Fatal(err)
		}
		values = append(values, fmt.Sprintf("%s/%s/%s/%g/%d/%d/%s/%d/%d", claimID, stage, status, salience, confidence, currentness, relation, lastReferenced, evidenceCount))
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	return strings.Join(values, "|")
}

func readClaimViewScopeGolden(t *testing.T, database *Store, residentID canonical.ID) string {
	t.Helper()
	rows, err := database.reader.Query(`SELECT claim_id, view_scope, source_assertion_id
		FROM claim_view_scope_current WHERE resident_id = ? ORDER BY claim_id`, residentID.String())
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var values []string
	for rows.Next() {
		var claimID, viewScope, assertionID string
		if err := rows.Scan(&claimID, &viewScope, &assertionID); err != nil {
			t.Fatal(err)
		}
		values = append(values, claimID+"/"+viewScope+"/"+assertionID)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	return strings.Join(values, "|")
}

func openProjectionTestStore(t *testing.T) *Store {
	t.Helper()
	database, err := Open(context.Background(), filepath.Join(t.TempDir(), "projection.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = database.Close() })
	return database
}

func projectionTestID(t *testing.T, raw string) canonical.ID {
	t.Helper()
	id, err := canonical.ParseID(raw)
	if err != nil {
		t.Fatal(err)
	}
	return id
}

func mustProjectionCommitSeq(t *testing.T, value int64) canonical.CommitSeq {
	t.Helper()
	seq, err := canonical.NewCommitSeq(value)
	if err != nil {
		t.Fatal(err)
	}
	return seq
}

func statusProjectionApply(t *testing.T, residentID canonical.ID, observed *projection.Watermark, sourceCommit int64, status string) projection.ApplyRequest {
	t.Helper()
	definition := projection.Definition{Name: projection.ResidentCurrentStatusName, Version: "resident-current-status-v1"}
	transitionID := projectionTestID(t, "00000000000000000000000007")
	plan := projection.UpdatePlan{Kind: projection.FullBuild, NeedCommitCatchUp: true}
	if observed != nil {
		plan = projection.UpdatePlan{Kind: projection.Update, NeedCommitCatchUp: true}
	}
	return projection.ApplyRequest{
		Definition: definition, ResidentID: residentID, Observed: observed,
		Plan:       plan,
		Evaluation: projection.Evaluation{Value: ResidentStatusProjection{Status: status, SourceTransitionID: transitionID}},
		Watermark: projection.Watermark{
			ProjectionName: definition.Name, ResidentID: residentID, ProjectionVersion: definition.Version,
			SourceCommitSeq: mustProjectionCommitSeq(t, sourceCommit), AsOf: 100, AsOfTZ: canonical.MustTimezone("UTC"),
		},
	}
}

func assertProjectionRows(t *testing.T, database *Store, residentID canonical.ID, body, watermark int) {
	t.Helper()
	var bodyCount, watermarkCount int
	if err := database.reader.QueryRow(`SELECT count(*) FROM resident_current_status WHERE resident_id = ?`, residentID.String()).Scan(&bodyCount); err != nil {
		t.Fatal(err)
	}
	if err := database.reader.QueryRow(`SELECT count(*) FROM projection_watermarks WHERE resident_id = ?`, residentID.String()).Scan(&watermarkCount); err != nil {
		t.Fatal(err)
	}
	if bodyCount != body || watermarkCount != watermark {
		t.Fatalf("Projection rows = body:%d watermark:%d, want %d/%d", bodyCount, watermarkCount, body, watermark)
	}
}
