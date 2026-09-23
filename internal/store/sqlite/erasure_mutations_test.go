package sqlite

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"testing"

	"mahoroba.local/mahoroba/internal/blob"
	"mahoroba.local/mahoroba/internal/canonical"
	"mahoroba.local/mahoroba/internal/contentref"
	"mahoroba.local/mahoroba/internal/domain"
	"mahoroba.local/mahoroba/internal/erasure"
	"mahoroba.local/mahoroba/internal/integrity"
	"mahoroba.local/mahoroba/internal/memory"
)

type batchErasureEvidence struct {
	db         *sql.DB
	store      *Store
	plan       erasure.Plan
	blobs      databaseBlobReader
	commitID   string
	contentID  string
	claimIDs   []string
	commitTime int64
}

func performM7BatchAliasErasure(t *testing.T, failpoint erasure.Failpoint) batchErasureEvidence {
	return performM7BatchAliasErasureWithBoundary(t, failpoint, nil)
}

func performM7BatchAliasErasureWithBoundary(
	t *testing.T,
	failpoint erasure.Failpoint,
	boundary erasure.ApplyBoundary,
) batchErasureEvidence {
	return buildM7BatchAliasErasure(t, failpoint, boundary, true, false)
}

func prepareM7ProductionBatchAliasErasure(t *testing.T) batchErasureEvidence {
	return buildM7BatchAliasErasure(t, nil, nil, false, true)
}

func buildM7BatchAliasErasure(
	t *testing.T,
	failpoint erasure.Failpoint,
	boundary erasure.ApplyBoundary,
	apply bool,
	productionStore bool,
) batchErasureEvidence {
	t.Helper()
	var fixture *semanticFixture
	var store *Store
	if productionStore {
		fixture, store = newM7ErasureProductionSemanticFixture(t)
	} else {
		var closeFixture func()
		fixture, closeFixture = newSemanticFixture(t)
		t.Cleanup(closeFixture)
	}
	makeSemanticFixtureMinimumValid(t, fixture)
	insertResolvableDialogueCancellationHistory(t, fixture, "A")
	ctx := context.Background()
	resident := fixture.resident["A"]
	actor := fixture.principal["human"]
	content := fixture.content["A"]["claim"]
	var ownerRows int
	if err := fixture.db.QueryRow(`SELECT COUNT(*) FROM resident_status_transitions WHERE resident_id=? AND from_status IS NULL AND to_status='draft'`, resident).Scan(&ownerRows); err != nil {
		t.Fatal(err)
	}
	if ownerRows == 0 {
		if _, err := fixture.db.Exec(`INSERT INTO resident_status_transitions VALUES (?,?,?,?,?,?,?,?,?,?,?,?)`, fixture.ids.new(), fixture.commit["A"], resident, nil, "draft", actor, "fixture_owner", nil, semanticTime, semanticTZ, semanticTime, semanticTZ); err != nil {
			t.Fatal(err)
		}
	}
	aliasID := fixture.ids.new()
	if _, err := fixture.db.Exec(`INSERT INTO claims SELECT ?,canonical_commit_id,owner_resident_id,subject_principal_id,perspective_principal_id,kind,temporal_kind,statement_content_id,statement_hash,statement_hash_algorithm,statement_normalization_version,created_by_run_id,recorded_at,recorded_tz FROM claims WHERE claim_id=?`, aliasID, fixture.claim["A"]); err != nil {
		t.Fatal(err)
	}
	integrityPipeline := fixture.ids.new()
	memoryStatusPipeline := fixture.ids.new()
	if _, err := fixture.db.Exec(`INSERT INTO pipeline_versions VALUES (?,?,?,?,?,?,?)`, integrityPipeline, fixture.commit["global"], "integrity_check", integrity.IntegrityPipelineVersion, integrityPipelineDefinitionV1, semanticTime, semanticTZ); err != nil {
		t.Fatal(err)
	}
	if _, err := fixture.db.Exec(`INSERT INTO pipeline_versions VALUES (?,?,?,?,?,?,?)`, memoryStatusPipeline, fixture.commit["global"], "memory_status", domain.MemoryStatusPipelineVersion, memoryStatusPipelineDefinitionV1, semanticTime, semanticTZ); err != nil {
		t.Fatal(err)
	}
	var headID string
	var headSeq int64
	if err := fixture.db.QueryRow(`SELECT canonical_commit_id,commit_seq FROM canonical_commits ORDER BY commit_seq DESC LIMIT 1`).Scan(&headID, &headSeq); err != nil {
		t.Fatal(err)
	}
	contentEvent := fixture.ids.new()
	var class, policy string
	var commitment []byte
	if err := fixture.db.QueryRow(`SELECT content_class,erasure_policy,commitment FROM content_objects WHERE content_id=?`, content).Scan(&class, &policy, &commitment); err != nil {
		t.Fatal(err)
	}
	plan := erasure.Plan{PlanState: erasure.StateReady, Scope: erasure.ScopeContent, ResidentID: resident, BaseHead: erasure.BaseHead{CommitID: headID, CommitSeq: strconv.FormatInt(headSeq, 10)}, ActorPrincipalID: actor, ReasonCode: "privacy_request", IntegrityPipelineVersionID: integrityPipeline, MemoryStatusPipelineVersionID: memoryStatusPipeline, RequestedContentIDs: []string{content}, EffectiveTargets: []erasure.EffectiveTarget{{ContentID: content, ContentClass: class, ErasurePolicy: policy, Commitment: "sha256:" + strings.ToLower(strings.TrimSpace(fmtHex(commitment))), ErasureEventID: contentEvent, LineageDepth: "0"}}, Impacts: []erasure.Impact{}, PlannedFindings: []erasure.PlannedFinding{}, ExistingFindingDependencies: []erasure.ExistingFindingDependency{}, PlannedQuarantines: []erasure.PlannedQuarantine{}, ClaimIdentityErasures: []erasure.ClaimIdentityErasure{}, ExistingClaimIdentityDependencies: []erasure.ExistingClaimIdentityDependency{}, RuntimeConfigEffect: erasure.RuntimeConfigEffect{}, Rebuilds: []erasure.Rebuild{}, Blockers: []erasure.Blocker{}}
	for _, descriptor := range contentref.DirectDescriptors() {
		query := "SELECT " + descriptor.PrimaryKey + " FROM " + descriptor.Table + " WHERE " + descriptor.ContentField + "=? ORDER BY " + descriptor.PrimaryKey
		rows, err := fixture.db.Query(query, content)
		if err != nil {
			t.Fatal(err)
		}
		for rows.Next() {
			var referrer string
			if err := rows.Scan(&referrer); err != nil {
				t.Fatal(err)
			}
			actions := make([]string, len(descriptor.Actions))
			for i, a := range descriptor.Actions {
				actions[i] = string(a)
			}
			impact := erasure.Impact{RuleID: descriptor.RuleID, ReferrerKind: descriptor.ReferrerKind, ReferrerID: referrer, ReferrerField: descriptor.ReferrerField, Classification: string(descriptor.Classification), Actions: actions, DecisionMode: string(descriptor.DecisionMode)}
			if impact.DecisionMode != "none" {
				decision := "retain"
				impact.Decision = &decision
			}
			impact.ImpactID, err = erasure.ImpactID(impact)
			if err != nil {
				t.Fatal(err)
			}
			plan.Impacts = append(plan.Impacts, impact)
		}
		if err := rows.Close(); err != nil {
			t.Fatal(err)
		}
	}
	claimIDs := []string{fixture.claim["A"], aliasID}
	slices.Sort(claimIDs)
	residentID, _ := canonical.ParseID(resident)
	eventID, _ := canonical.ParseID(contentEvent)
	pipelineID, _ := canonical.ParseID(integrityPipeline)
	for _, claimRaw := range claimIDs {
		pair := fixture.ids.new()
		plan.ClaimIdentityErasures = append(plan.ClaimIdentityErasures, erasure.ClaimIdentityErasure{ClaimStatementErasureEventID: pair, ClaimID: claimRaw, StatementContentID: content, ContentErasureEventID: contentEvent})
		claimID, _ := canonical.ParseID(claimRaw)
		candidate, err := integrity.NewCandidate(integrity.CandidateInput{ResidentID: residentID, ClaimID: &claimID, Kind: integrity.FindingRequiredProvenanceErased, RuleCode: integrity.RuleClaimStatementErased, TargetKind: integrity.TargetClaim, TargetID: claimID, TargetField: "statement_content_id", SourceContentErasureEventID: &eventID, OccurredTZ: canonical.MustTimezone(semanticTZ)})
		if err != nil {
			t.Fatal(err)
		}
		findingID := fixture.ids.new()
		source := contentEvent
		claim := claimRaw
		plan.PlannedFindings = append(plan.PlannedFindings, erasure.PlannedFinding{IntegrityFindingID: findingID, FindingKind: string(candidate.Kind), RuleCode: string(candidate.RuleCode), TargetKind: string(candidate.TargetKind), TargetID: claim, TargetField: candidate.TargetField, ResidentID: resident, ClaimID: &claim, SourceContentErasureEventID: &source, PipelineVersionID: pipelineID.String(), FindingFingerprint: "sha256:" + candidate.Fingerprint.Hex()})
		plan.PlannedQuarantines = append(plan.PlannedQuarantines, erasure.PlannedQuarantine{StatusTransitionID: fixture.ids.new(), ClaimID: claim, FromStatus: "active", ToStatus: "quarantined", DecisionKind: "automatic", TriggerKind: "integrity_finding", TriggerIntegrityFindingID: findingID, PipelineVersionID: memoryStatusPipeline, GateMetrics: `{"reason":"required_provenance_erased"}`, DecisionReasonCode: "structural_quarantine"})
		decision := "retain"
		impact := erasure.Impact{RuleID: "claim_provenance_break_v1", ReferrerKind: "claim", ReferrerID: claim, ReferrerField: "qualifying_provenance", Classification: "needs_review", Actions: []string{"quarantine_claim", "record_integrity_finding"}, DecisionMode: "consequence_only", Decision: &decision}
		impact.ImpactID, err = erasure.ImpactID(impact)
		if err != nil {
			t.Fatal(err)
		}
		plan.Impacts = append(plan.Impacts, impact)
	}
	for _, definition := range activeProjectionDefinitions {
		impact := erasure.Impact{RuleID: "projection_derived_state_v1", ReferrerKind: "projection", ReferrerID: string(definition.Name), ReferrerField: "body", Classification: "needs_rebuild", Actions: []string{"rebuild_projection"}, DecisionMode: "none"}
		impactID, err := erasure.ImpactID(impact)
		if err != nil {
			t.Fatal(err)
		}
		impact.ImpactID = impactID
		plan.Impacts = append(plan.Impacts, impact)
		plan.Rebuilds = append(plan.Rebuilds,
			erasure.Rebuild{ResidentID: resident, ProjectionName: string(definition.Name), ProjectionVersion: string(definition.Version), ReasonCode: "claim_state_change"},
			erasure.Rebuild{ResidentID: resident, ProjectionName: string(definition.Name), ProjectionVersion: string(definition.Version), ReasonCode: "content_erasure"},
		)
	}
	slices.SortFunc(plan.Impacts, func(a, b erasure.Impact) int { return strings.Compare(a.ImpactID, b.ImpactID) })
	slices.SortFunc(plan.PlannedFindings, func(a, b erasure.PlannedFinding) int {
		return strings.Compare(a.FindingFingerprint+"\x00"+a.IntegrityFindingID, b.FindingFingerprint+"\x00"+b.IntegrityFindingID)
	})
	slices.SortFunc(plan.PlannedQuarantines, func(a, b erasure.PlannedQuarantine) int {
		return strings.Compare(a.ClaimID+"\x00"+a.StatusTransitionID, b.ClaimID+"\x00"+b.StatusTransitionID)
	})
	slices.SortFunc(plan.ClaimIdentityErasures, func(a, b erasure.ClaimIdentityErasure) int {
		return strings.Compare(a.ClaimID+"\x00"+a.ClaimStatementErasureEventID, b.ClaimID+"\x00"+b.ClaimStatementErasureEventID)
	})
	slices.SortFunc(plan.Rebuilds, func(a, b erasure.Rebuild) int {
		return strings.Compare(a.ResidentID+"\x00"+a.ProjectionName+"\x00"+a.ProjectionVersion+"\x00"+a.ReasonCode, b.ResidentID+"\x00"+b.ProjectionName+"\x00"+b.ProjectionVersion+"\x00"+b.ReasonCode)
	})
	plan, err := erasure.Seal(plan)
	if err != nil {
		t.Fatal(err)
	}
	commitID := fixture.ids.new()
	scope, _ := canonical.ResidentScope(residentID)
	seq, _ := canonical.NewCommitSeq(headSeq + 1)
	metadata := canonical.CommitMetadata{CommitID: mustCanonicalID(t, commitID), CommitSeq: seq, Scope: scope, CommittedAt: canonical.Instant(semanticTime + 999), CommittedTZ: canonical.MustTimezone(semanticTZ)}
	evidence := batchErasureEvidence{
		db: fixture.db, store: store, plan: plan, commitID: commitID, contentID: content,
		claimIDs: claimIDs, commitTime: metadata.CommittedAt.UnixMicro(),
	}
	if !apply {
		return evidence
	}
	blobs := loadDatabaseBlobReader(t, fixture.db, residentID)
	evidence.blobs = blobs
	tx, err := fixture.db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec(`INSERT INTO canonical_commits VALUES (?,?,?,?,?)`, commitID, seq.Int64(), resident, metadata.CommittedAt.UnixMicro(), semanticTZ); err != nil {
		t.Fatal(err)
	}
	uow := &canonicalUoW{tx: tx, metadata: metadata}
	if _, err := uow.ApplyErasure(ctx, erasure.ApplyRequest{
		Plan: plan, Confirm: plan.Digest, Blobs: blobs, Boundary: boundary, Failpoint: failpoint,
	}); err != nil {
		_ = tx.Rollback()
		if failpoint != nil {
			return evidence
		}
		t.Fatal(err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	return evidence
}

func newM7ErasureProductionSemanticFixture(t *testing.T) (*semanticFixture, *Store) {
	t.Helper()
	store, err := Open(context.Background(), filepath.Join(t.TempDir(), "mahoroba.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := store.Close(); err != nil {
			t.Errorf("close production erasure store: %v", err)
		}
	})
	fixture := &semanticFixture{
		db: store.writer, ids: &semanticIDs{next: 1},
		commit: make(map[string]string), resident: make(map[string]string), principal: make(map[string]string),
		content:  map[string]map[string]string{"A": {}, "B": {}},
		revision: map[string]map[string]string{"A": {}, "B": {}},
		recall:   make(map[string]string), run: make(map[string]string), event: make(map[string]string),
		claim: make(map[string]string), evidence: make(map[string]string), stage: make(map[string]string),
		erasure: make(map[string]string), finding: make(map[string]string),
	}
	fixture.seed(t)
	return fixture, store
}

type erasureBoundaryProbe struct {
	calls  int
	failAt int
}

func (probe *erasureBoundaryProbe) Verify() error {
	probe.calls++
	if probe.calls == probe.failAt {
		return errors.New("injected database identity swap")
	}
	return nil
}

func TestM7ErasureApplyBoundarySwapMatrixRollsBack(t *testing.T) {
	for _, failAt := range []int{1, 2, 3} {
		t.Run(strconv.Itoa(failAt), func(t *testing.T) {
			probe := &erasureBoundaryProbe{failAt: failAt}
			e := performM7BatchAliasErasureWithBoundary(t, func(string) error { return nil }, probe)
			if probe.calls != failAt {
				t.Fatalf("boundary calls=%d want=%d", probe.calls, failAt)
			}
			var present, nonNullHashes, events, pairs, commits int
			if err := e.db.QueryRow(`SELECT COUNT(*) FROM content_objects WHERE content_id=? AND erasure_state='present' AND blob_hash IS NOT NULL AND commitment_salt IS NOT NULL`, e.contentID).Scan(&present); err != nil {
				t.Fatal(err)
			}
			if err := e.db.QueryRow(`SELECT COUNT(statement_hash) FROM claims WHERE statement_content_id=?`, e.contentID).Scan(&nonNullHashes); err != nil {
				t.Fatal(err)
			}
			if err := e.db.QueryRow(`SELECT COUNT(*) FROM content_erasure_events WHERE content_erasure_event_id=?`, e.plan.EffectiveTargets[0].ErasureEventID).Scan(&events); err != nil {
				t.Fatal(err)
			}
			if err := e.db.QueryRow(`SELECT COUNT(*) FROM claim_statement_erasure_events WHERE content_erasure_event_id=?`, e.plan.EffectiveTargets[0].ErasureEventID).Scan(&pairs); err != nil {
				t.Fatal(err)
			}
			if err := e.db.QueryRow(`SELECT COUNT(*) FROM canonical_commits WHERE canonical_commit_id=?`, e.commitID).Scan(&commits); err != nil {
				t.Fatal(err)
			}
			if present != 1 || nonNullHashes != len(e.claimIDs) || events != 0 || pairs != 0 || commits != 0 {
				t.Fatalf("partial boundary failure mutation: present=%d hashes=%d events=%d pairs=%d commits=%d",
					present, nonNullHashes, events, pairs, commits)
			}
		})
	}
}

func TestM7I5ContentEraseAppendsExactlyOneErasureEvent(t *testing.T) {
	e := performM7BatchAliasErasure(t, nil)
	var count int
	if err := e.db.QueryRow(`SELECT COUNT(*) FROM content_erasure_events WHERE content_id=?`, e.contentID).Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 1 {
		t.Fatalf("events=%d", count)
	}
}
func TestM7I6ErasePreservesCommitmentAndDestroysSalt(t *testing.T) {
	e := performM7BatchAliasErasure(t, nil)
	var commitment, salt []byte
	if err := e.db.QueryRow(`SELECT commitment,commitment_salt FROM content_objects WHERE content_id=?`, e.contentID).Scan(&commitment, &salt); err != nil {
		t.Fatal(err)
	}
	if len(commitment) != 32 || salt != nil {
		t.Fatalf("commitment/salt=%x/%x", commitment, salt)
	}
}
func TestM7I58PerContentSaltIsDestroyedWhileCommitmentRemains(t *testing.T) {
	TestM7I6ErasePreservesCommitmentAndDestroysSalt(t)
}
func TestM7I59ErasureAuditDoesNotRetainSaltOrBlobHash(t *testing.T) {
	e := performM7BatchAliasErasure(t, nil)
	columns, err := e.db.Query(`PRAGMA table_info(content_erasure_events)`)
	if err != nil {
		t.Fatal(err)
	}
	defer columns.Close()
	for columns.Next() {
		var cid, notnull, pk int
		var name, kind string
		var dflt any
		if err := columns.Scan(&cid, &name, &kind, &notnull, &dflt, &pk); err != nil {
			t.Fatal(err)
		}
		if name == "commitment_salt" || name == "blob_hash" {
			t.Fatalf("audit retains %s", name)
		}
	}
}
func TestM7I71ErasureStateIsOneWay(t *testing.T) {
	e := performM7BatchAliasErasure(t, nil)
	if _, err := e.db.Exec(`UPDATE content_objects SET erasure_state='present' WHERE content_id=?`, e.contentID); err == nil {
		t.Fatal("erased content restored")
	}
}
func TestM7I79ErasureEventsPreserveScopeActorReasonAndSource(t *testing.T) {
	e := performM7BatchAliasErasure(t, nil)
	var scope, actor, reason string
	var source sql.NullString
	if err := e.db.QueryRow(`SELECT erasure_scope,actor_principal_id,reason_code,source_erasure_event_id FROM content_erasure_events WHERE content_id=?`, e.contentID).Scan(&scope, &actor, &reason, &source); err != nil {
		t.Fatal(err)
	}
	if scope != "content" || actor != e.plan.ActorPrincipalID || reason != e.plan.ReasonCode || source.Valid {
		t.Fatalf("event=%s/%s/%s/%v", scope, actor, reason, source)
	}
}
func TestM7I87ErasureFindingQuarantinesOnlyMatchingClaim(t *testing.T) {
	e := performM7BatchAliasErasure(t, nil)
	for _, claim := range e.claimIDs {
		var findingClaim, transitionClaim string
		if err := e.db.QueryRow(`SELECT f.claim_id,s.claim_id FROM integrity_findings f JOIN claim_status_transitions s ON s.trigger_integrity_finding_id=f.integrity_finding_id WHERE f.claim_id=?`, claim).Scan(&findingClaim, &transitionClaim); err != nil {
			t.Fatal(err)
		}
		if findingClaim != claim || transitionClaim != claim {
			t.Fatalf("claim tuple=%s/%s/%s", claim, findingClaim, transitionClaim)
		}
	}
}

func TestM7ErasureCrashMatrixRollsBackEveryAliasMutation(t *testing.T) {
	for _, point := range []string{"before_mutation", "event:0", "finding:0", "finding:1", "quarantine:0", "quarantine:1", "content:0", "pair:0", "claim_hash:0", "pair:1", "claim_hash:1", "pre_commit"} {
		t.Run(point, func(t *testing.T) {
			hit := false
			e := performM7BatchAliasErasure(t, func(name string) error {
				if name == point {
					hit = true
					return errors.New("injected")
				}
				return nil
			})
			if !hit {
				t.Fatalf("failpoint %s was not reached", point)
			}
			var state string
			var salt, blob []byte
			if err := e.db.QueryRow(`SELECT erasure_state,commitment_salt,blob_hash FROM content_objects WHERE content_id=?`, e.contentID).Scan(&state, &salt, &blob); err != nil {
				t.Fatal(err)
			}
			if state != "present" || len(salt) != 32 || len(blob) != 32 {
				t.Fatalf("content after rollback=%s/%d/%d", state, len(salt), len(blob))
			}
			for _, claim := range e.claimIDs {
				var hash []byte
				if err := e.db.QueryRow(`SELECT statement_hash FROM claims WHERE claim_id=?`, claim).Scan(&hash); err != nil {
					t.Fatal(err)
				}
				if len(hash) != 32 {
					t.Fatalf("claim %s hash=%x", claim, hash)
				}
			}
			var rows int
			if err := e.db.QueryRow(`SELECT (SELECT COUNT(*) FROM content_erasure_events WHERE content_erasure_event_id=?)+(SELECT COUNT(*) FROM canonical_commits WHERE canonical_commit_id=?)`, e.plan.EffectiveTargets[0].ErasureEventID, e.commitID).Scan(&rows); err != nil {
				t.Fatal(err)
			}
			if rows != 0 {
				t.Fatalf("partial rows=%d", rows)
			}
		})
	}
}

func TestM7ErasureExactRetryReturnsHistoricalCommitWithoutNewCommit(t *testing.T) {
	e := performM7BatchAliasErasure(t, nil)
	ctx := context.Background()
	var headSeq int64
	if err := e.db.QueryRow(`SELECT MAX(commit_seq) FROM canonical_commits`).Scan(&headSeq); err != nil {
		t.Fatal(err)
	}
	newCommit := "01J00000000000000000000YYY"
	residentID := mustCanonicalID(t, e.plan.ResidentID)
	scope, _ := canonical.ResidentScope(residentID)
	seq, _ := canonical.NewCommitSeq(headSeq + 1)
	metadata := canonical.CommitMetadata{CommitID: mustCanonicalID(t, newCommit), CommitSeq: seq, Scope: scope, CommittedAt: canonical.Instant(e.commitTime + 1), CommittedTZ: canonical.MustTimezone(semanticTZ)}
	tx, err := e.db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec(`INSERT INTO canonical_commits VALUES (?,?,?,?,?)`, newCommit, seq.Int64(), e.plan.ResidentID, metadata.CommittedAt.UnixMicro(), semanticTZ); err != nil {
		t.Fatal(err)
	}
	probe := &rejectingBlobReader{}
	value, err := (&canonicalUoW{tx: tx, metadata: metadata}).ApplyErasure(ctx, erasure.ApplyRequest{Plan: e.plan, Confirm: e.plan.Digest, Blobs: probe})
	if err != canonical.ErrNoMutation {
		_ = tx.Rollback()
		t.Fatalf("retry error=%v", err)
	}
	if !value.ExistingCommit || value.CanonicalErasureCommitID != e.commitID {
		_ = tx.Rollback()
		t.Fatalf("retry result=%+v", value)
	}
	if probe.opens != 0 {
		_ = tx.Rollback()
		t.Fatalf("exact retry reopened erased blob %d times", probe.opens)
	}
	if err := tx.Rollback(); err != nil {
		t.Fatal(err)
	}
	var commits int
	if err := e.db.QueryRow(`SELECT COUNT(*) FROM canonical_commits WHERE canonical_commit_id=?`, newCommit).Scan(&commits); err != nil {
		t.Fatal(err)
	}
	if commits != 0 {
		t.Fatal("retry committed provisional row")
	}
}

func TestM7ErasureExactRetryRejectsUnplannedPairedAliasWithoutMutation(t *testing.T) {
	tests := []struct {
		name        string
		corruptPair func(*testing.T, batchErasureEvidence) (string, string)
	}{
		{
			name: "same_event_same_commit",
			corruptPair: func(_ *testing.T, e batchErasureEvidence) (string, string) {
				return e.plan.EffectiveTargets[0].ErasureEventID, e.commitID
			},
		},
		{
			name: "wrong_event_same_commit",
			corruptPair: func(t *testing.T, e batchErasureEvidence) (string, string) {
				var otherContent string
				if err := e.db.QueryRow(`SELECT object.content_id
					FROM content_objects object
					WHERE object.owner_resident_id=? AND object.content_id<>?
					  AND NOT EXISTS (SELECT 1 FROM content_erasure_events event WHERE event.content_id=object.content_id)
					ORDER BY object.content_id LIMIT 1`, e.plan.ResidentID, e.contentID).Scan(&otherContent); err != nil {
					t.Fatal(err)
				}
				eventID := newM7ErasureTestID(t)
				if _, err := e.db.Exec(`INSERT INTO content_erasure_events(
					content_erasure_event_id,canonical_commit_id,content_id,erasure_scope,
					actor_principal_id,reason_code,reason_content_id,source_erasure_event_id,
					occurred_at,occurred_tz,recorded_at,recorded_tz
				) SELECT ?,canonical_commit_id,?,erasure_scope,actor_principal_id,reason_code,
					reason_content_id,NULL,occurred_at,occurred_tz,recorded_at,recorded_tz
				  FROM content_erasure_events WHERE content_erasure_event_id=?`, eventID, otherContent, e.plan.EffectiveTargets[0].ErasureEventID); err != nil {
					t.Fatal(err)
				}
				return eventID, e.commitID
			},
		},
		{
			name: "same_event_wrong_commit",
			corruptPair: func(_ *testing.T, e batchErasureEvidence) (string, string) {
				return e.plan.EffectiveTargets[0].ErasureEventID, e.plan.BaseHead.CommitID
			},
		},
	}
	for _, test := range tests {
		t.Run("content_scope/"+test.name, func(t *testing.T) {
			e := performM7BatchAliasErasure(t, nil)
			disableM7ErasureAliasInsertGuards(t, e.db)
			contentEventID, pairCommitID := test.corruptPair(t, e)
			insertM7UnplannedErasedAlias(t, e, e.contentID, contentEventID, pairCommitID)

			writer := openM7ErasureTestWriter(t, e.db)
			assertM7ErasureRetryConflictWithoutMutation(t, e.db, writer, e.plan)
		})
	}

	t.Run("resident_scope/historical_non_target_extra_alias", func(t *testing.T) {
		e := performM7BatchAliasErasure(t, nil)
		plan, blobs, _ := prepareM7ResidentErasurePlan(t, e, len(e.claimIDs))
		writer := openM7ErasureTestWriter(t, e.db)
		if _, err := writer.Submit(context.Background(), erasure.ApplyCommand(erasure.ApplyRequest{
			Plan: plan, Confirm: plan.Digest, Blobs: blobs,
		})); err != nil {
			t.Fatalf("resident erasure: %v", err)
		}

		disableM7ErasureAliasInsertGuards(t, e.db)
		insertM7UnplannedErasedAlias(t, e, e.contentID, e.plan.EffectiveTargets[0].ErasureEventID, e.commitID)
		assertM7ErasureRetryConflictWithoutMutation(t, e.db, writer, plan)
	})
}

func disableM7ErasureAliasInsertGuards(t *testing.T, db *sql.DB) {
	t.Helper()
	for _, trigger := range []string{"trg_claims_resident_scope", "trg_claim_statement_erasure_events_scope"} {
		if _, err := db.Exec("DROP TRIGGER " + trigger); err != nil {
			t.Fatal(err)
		}
	}
}

func insertM7UnplannedErasedAlias(t *testing.T, e batchErasureEvidence, statementContentID, contentEventID, pairCommitID string) {
	t.Helper()
	claimID := newM7ErasureTestID(t)
	if _, err := e.db.Exec(`INSERT INTO claims(
		claim_id,canonical_commit_id,owner_resident_id,subject_principal_id,
		perspective_principal_id,kind,temporal_kind,statement_content_id,
		statement_hash,statement_hash_algorithm,statement_normalization_version,
		created_by_run_id,recorded_at,recorded_tz
	) SELECT ?,canonical_commit_id,owner_resident_id,subject_principal_id,
		perspective_principal_id,kind,temporal_kind,?,NULL,statement_hash_algorithm,
		statement_normalization_version,created_by_run_id,recorded_at,recorded_tz
	  FROM claims WHERE claim_id=?`, claimID, statementContentID, e.claimIDs[0]); err != nil {
		t.Fatal(err)
	}
	pairID := newM7ErasureTestID(t)
	if _, err := e.db.Exec(`INSERT INTO claim_statement_erasure_events(
		claim_statement_erasure_event_id,canonical_commit_id,resident_id,claim_id,
		content_erasure_event_id,recorded_at,recorded_tz
	) VALUES (?,?,?,?,?,?,?)`, pairID, pairCommitID, e.plan.ResidentID, claimID,
		contentEventID, e.commitTime, semanticTZ); err != nil {
		t.Fatal(err)
	}
}

func openM7ErasureTestWriter(t *testing.T, db *sql.DB) *canonical.Writer {
	t.Helper()
	backend := &Store{writer: db, reader: db, writes: newWritePriorityGate()}
	writer, err := canonical.OpenWriter(context.Background(), canonical.WriterOptions{
		Backend: backend.Canonical(), IDs: canonical.NewSecureIDGenerator(), Clock: canonical.SystemClock{},
		Timezone: canonical.MustTimezone("UTC"), QueueCapacity: 4,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := writer.Close(context.Background()); err != nil {
			t.Errorf("close writer: %v", err)
		}
	})
	return writer
}

func assertM7ErasureRetryConflictWithoutMutation(t *testing.T, db *sql.DB, writer *canonical.Writer, plan erasure.Plan) {
	t.Helper()
	before := snapshotM7ErasureRetryState(t, db)
	probe := &rejectingBlobReader{}
	_, err := writer.Submit(context.Background(), erasure.ApplyCommand(erasure.ApplyRequest{
		Plan: plan, Confirm: plan.Digest, Blobs: probe,
	}))
	if !errors.Is(err, erasure.ErrRetryConflict) {
		t.Fatalf("retry error=%v, want ErrRetryConflict", err)
	}
	if errors.Is(err, erasure.ErrPlanStale) || errors.Is(err, canonical.ErrNoMutation) || errors.Is(err, canonical.ErrWriterPoisoned) {
		t.Fatalf("retry error precedence=%v", err)
	}
	if probe.opens != 0 {
		t.Fatalf("conflicting retry reopened erased blob %d times", probe.opens)
	}
	after := snapshotM7ErasureRetryState(t, db)
	if !bytes.Equal(before, after) {
		t.Fatalf("conflicting retry mutated durable state:\nbefore=%s\nafter=%s", before, after)
	}
}

func snapshotM7ErasureRetryState(t *testing.T, db *sql.DB) []byte {
	t.Helper()
	tables := []struct {
		name  string
		order string
	}{
		{"canonical_commits", "commit_seq,canonical_commit_id"},
		{"blobs", "dedupe_scope_id,hash_algorithm,blob_hash"},
		{"content_objects", "content_id"},
		{"content_erasure_events", "content_erasure_event_id"},
		{"claims", "claim_id"},
		{"claim_statement_erasure_events", "claim_statement_erasure_event_id"},
		{"integrity_findings", "integrity_finding_id"},
		{"claim_status_transitions", "status_transition_id"},
		{"resident_status_transitions", "resident_status_transition_id"},
		{"runtime_config", "singleton_id"},
	}
	state := make(map[string][][]any, len(tables))
	for _, table := range tables {
		rows, err := db.Query(fmt.Sprintf("SELECT * FROM %s ORDER BY %s", table.name, table.order))
		if err != nil {
			t.Fatal(err)
		}
		columns, err := rows.Columns()
		if err != nil {
			_ = rows.Close()
			t.Fatal(err)
		}
		values := make([][]any, 0)
		for rows.Next() {
			row := make([]any, len(columns))
			destinations := make([]any, len(columns))
			for index := range row {
				destinations[index] = &row[index]
			}
			if err := rows.Scan(destinations...); err != nil {
				_ = rows.Close()
				t.Fatal(err)
			}
			for index, value := range row {
				if raw, ok := value.([]byte); ok {
					row[index] = slices.Clone(raw)
				}
			}
			values = append(values, row)
		}
		if err := rows.Err(); err != nil {
			_ = rows.Close()
			t.Fatal(err)
		}
		if err := rows.Close(); err != nil {
			t.Fatal(err)
		}
		state[table.name] = values
	}
	body, err := json.Marshal(state)
	if err != nil {
		t.Fatal(err)
	}
	return body
}

func newM7ErasureTestID(t *testing.T) string {
	t.Helper()
	id, err := canonical.NewSecureIDGenerator().New()
	if err != nil {
		t.Fatal(err)
	}
	return id.String()
}

func TestM7ErasureRejectsAliasAddedAfterPlanAndRollsBack(t *testing.T) {
	injected := errors.New("capture plan before mutation")
	e := performM7BatchAliasErasure(t, func(name string) error {
		if name == "before_mutation" {
			return injected
		}
		return nil
	})
	newAlias := "01J00000000000000000000YX1"
	if _, err := e.db.Exec(`INSERT INTO claims SELECT ?,canonical_commit_id,owner_resident_id,subject_principal_id,perspective_principal_id,kind,temporal_kind,statement_content_id,statement_hash,statement_hash_algorithm,statement_normalization_version,created_by_run_id,recorded_at,recorded_tz FROM claims WHERE claim_id=?`, newAlias, e.claimIDs[0]); err != nil {
		t.Fatal(err)
	}

	headSeq, err := strconv.ParseInt(e.plan.BaseHead.CommitSeq, 10, 64)
	if err != nil {
		t.Fatal(err)
	}
	commitID := "01J00000000000000000000YX2"
	residentID := mustCanonicalID(t, e.plan.ResidentID)
	scope, _ := canonical.ResidentScope(residentID)
	seq, _ := canonical.NewCommitSeq(headSeq + 1)
	metadata := canonical.CommitMetadata{CommitID: mustCanonicalID(t, commitID), CommitSeq: seq, Scope: scope, CommittedAt: canonical.Instant(e.commitTime + 2), CommittedTZ: canonical.MustTimezone(semanticTZ)}
	tx, err := e.db.BeginTx(context.Background(), nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec(`INSERT INTO canonical_commits VALUES (?,?,?,?,?)`, commitID, seq.Int64(), e.plan.ResidentID, metadata.CommittedAt.UnixMicro(), semanticTZ); err != nil {
		_ = tx.Rollback()
		t.Fatal(err)
	}
	_, err = (&canonicalUoW{tx: tx, metadata: metadata}).ApplyErasure(context.Background(), erasure.ApplyRequest{Plan: e.plan, Confirm: e.plan.Digest, Blobs: e.blobs})
	if !errors.Is(err, erasure.ErrPlanStale) {
		_ = tx.Rollback()
		t.Fatalf("error=%v, want ErrPlanStale", err)
	}
	if err := tx.Rollback(); err != nil {
		t.Fatal(err)
	}
	var present, nonNullHashes, events, pairs, commitRows int
	if err := e.db.QueryRow(`SELECT COUNT(*) FROM content_objects WHERE content_id=? AND erasure_state='present' AND blob_hash IS NOT NULL AND commitment_salt IS NOT NULL`, e.contentID).Scan(&present); err != nil {
		t.Fatal(err)
	}
	if err := e.db.QueryRow(`SELECT COUNT(statement_hash) FROM claims WHERE statement_content_id=?`, e.contentID).Scan(&nonNullHashes); err != nil {
		t.Fatal(err)
	}
	if err := e.db.QueryRow(`SELECT COUNT(*) FROM content_erasure_events WHERE content_erasure_event_id=?`, e.plan.EffectiveTargets[0].ErasureEventID).Scan(&events); err != nil {
		t.Fatal(err)
	}
	if err := e.db.QueryRow(`SELECT COUNT(*) FROM claim_statement_erasure_events WHERE content_erasure_event_id=?`, e.plan.EffectiveTargets[0].ErasureEventID).Scan(&pairs); err != nil {
		t.Fatal(err)
	}
	if err := e.db.QueryRow(`SELECT COUNT(*) FROM canonical_commits WHERE canonical_commit_id=?`, commitID).Scan(&commitRows); err != nil {
		t.Fatal(err)
	}
	if present != 1 || nonNullHashes != len(e.claimIDs)+1 || events != 0 || pairs != 0 || commitRows != 0 {
		t.Fatalf("partial mutation: content=%d hashes=%d events=%d pairs=%d commit=%d", present, nonNullHashes, events, pairs, commitRows)
	}
}

func TestM7ErasureRejectsAliasRemovedAfterPlanAndRollsBack(t *testing.T) {
	e := performM7BatchAliasErasure(t, func(name string) error {
		if name == "before_mutation" {
			return errors.New("capture plan before alias removal")
		}
		return nil
	})
	var removable string
	if err := e.db.QueryRow(`SELECT c.claim_id
		FROM claims c LEFT JOIN claim_evidence evidence ON evidence.claim_id=c.claim_id
		WHERE c.statement_content_id=?
		GROUP BY c.claim_id HAVING COUNT(evidence.evidence_id)=0
		ORDER BY c.claim_id LIMIT 1`, e.contentID).Scan(&removable); err != nil {
		t.Fatal(err)
	}
	mustExec(t, e.db, "DROP TRIGGER trg_claims_no_delete")
	result, err := e.db.Exec(`DELETE FROM claims WHERE claim_id=?`, removable)
	if err != nil {
		t.Fatal(err)
	}
	if rows, err := result.RowsAffected(); err != nil || rows != 1 {
		t.Fatalf("removed alias rows=%d error=%v", rows, err)
	}

	assertM7AliasMembershipStaleNoMutation(t, e, "01J00000000000000000000YX3", len(e.claimIDs)-1)
}

func assertM7AliasMembershipStaleNoMutation(t *testing.T, e batchErasureEvidence, commitID string, wantClaims int) {
	t.Helper()
	headSeq, err := strconv.ParseInt(e.plan.BaseHead.CommitSeq, 10, 64)
	if err != nil {
		t.Fatal(err)
	}
	residentID := mustCanonicalID(t, e.plan.ResidentID)
	scope, _ := canonical.ResidentScope(residentID)
	seq, _ := canonical.NewCommitSeq(headSeq + 1)
	metadata := canonical.CommitMetadata{CommitID: mustCanonicalID(t, commitID), CommitSeq: seq, Scope: scope, CommittedAt: canonical.Instant(e.commitTime + 2), CommittedTZ: canonical.MustTimezone(semanticTZ)}
	tx, err := e.db.BeginTx(context.Background(), nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec(`INSERT INTO canonical_commits VALUES (?,?,?,?,?)`, commitID, seq.Int64(), e.plan.ResidentID, metadata.CommittedAt.UnixMicro(), semanticTZ); err != nil {
		_ = tx.Rollback()
		t.Fatal(err)
	}
	_, err = (&canonicalUoW{tx: tx, metadata: metadata}).ApplyErasure(context.Background(), erasure.ApplyRequest{Plan: e.plan, Confirm: e.plan.Digest, Blobs: e.blobs})
	if !errors.Is(err, erasure.ErrPlanStale) {
		_ = tx.Rollback()
		t.Fatalf("error=%v, want ErrPlanStale", err)
	}
	if err := tx.Rollback(); err != nil {
		t.Fatal(err)
	}

	var present, aliases, nonNullHashes, distinctHashes, blobRows, events, pairs, commitRows int
	if err := e.db.QueryRow(`SELECT COUNT(*) FROM content_objects
		WHERE content_id=? AND erasure_state='present' AND blob_hash IS NOT NULL AND commitment_salt IS NOT NULL`, e.contentID).Scan(&present); err != nil {
		t.Fatal(err)
	}
	if err := e.db.QueryRow(`SELECT COUNT(*),COUNT(statement_hash),COUNT(DISTINCT hex(statement_hash))
		FROM claims WHERE statement_content_id=?`, e.contentID).Scan(&aliases, &nonNullHashes, &distinctHashes); err != nil {
		t.Fatal(err)
	}
	if err := e.db.QueryRow(`SELECT COUNT(*) FROM content_objects content JOIN blobs blob
		ON blob.dedupe_scope_id=content.owner_resident_id
		AND blob.hash_algorithm=content.blob_hash_algorithm AND blob.blob_hash=content.blob_hash
		WHERE content.content_id=?`, e.contentID).Scan(&blobRows); err != nil {
		t.Fatal(err)
	}
	if err := e.db.QueryRow(`SELECT COUNT(*) FROM content_erasure_events WHERE content_id=?`, e.contentID).Scan(&events); err != nil {
		t.Fatal(err)
	}
	if err := e.db.QueryRow(`SELECT COUNT(*) FROM claim_statement_erasure_events pair
		JOIN content_erasure_events event ON event.content_erasure_event_id=pair.content_erasure_event_id
		WHERE event.content_id=?`, e.contentID).Scan(&pairs); err != nil {
		t.Fatal(err)
	}
	if err := e.db.QueryRow(`SELECT COUNT(*) FROM canonical_commits WHERE canonical_commit_id=?`, commitID).Scan(&commitRows); err != nil {
		t.Fatal(err)
	}
	if present != 1 || aliases != wantClaims || nonNullHashes != wantClaims || distinctHashes != 1 ||
		blobRows != 1 || events != 0 || pairs != 0 || commitRows != 0 {
		t.Fatalf("partial mutation: content=%d aliases=%d hashes=%d/%d blob=%d events=%d pairs=%d commit=%d",
			present, aliases, nonNullHashes, distinctHashes, blobRows, events, pairs, commitRows)
	}
}

func TestM7ErasureNormalApplyRequiresAndRevalidatesFilesystemBlob(t *testing.T) {
	e := prepareM7ProductionBatchAliasErasure(t)

	t.Run("valid live object reaches the mutation boundary", func(t *testing.T) {
		files, target := newM7ErasureLiveFileStore(t, e)
		content, err := files.Read(context.Background(), target.residentID, target.digest)
		if err != nil || !bytes.Equal(content, target.content) {
			t.Fatalf("live object precondition content=%x error=%v", content, err)
		}
		stop := errors.New("stop after live filesystem revalidation")
		hit := false
		beforeDatabase := snapshotM7ErasureRetryState(t, e.db)
		beforeFilesystem := snapshotM7ErasureFileStoreTree(t, files)
		err = submitM7ErasureThroughProductionWriter(t, e, files, func(name string) error {
			if name == "before_mutation" {
				hit = true
				return stop
			}
			return nil
		})
		if !hit || !errors.Is(err, stop) {
			t.Fatalf("live filesystem revalidation did not reach mutation boundary: hit=%t error=%v", hit, err)
		}
		if errors.Is(err, canonical.ErrWriterPoisoned) {
			t.Fatalf("live filesystem revalidation poisoned the writer: %v", err)
		}
		assertM7ErasureDatabaseSnapshotEqual(t, e.db, beforeDatabase)
		assertM7ErasureFileStoreTreeEqual(t, files, beforeFilesystem)
		assertM7ErasureLiveObject(t, files, target, true, target.content)
	})

	t.Run("missing filesystem authority", func(t *testing.T) {
		beforeDatabase := snapshotM7ErasureRetryState(t, e.db)
		err := submitM7ErasureThroughProductionWriter(t, e, nil, nil)
		if !errors.Is(err, erasure.ErrPlanStale) {
			t.Fatalf("error=%v, want ErrPlanStale", err)
		}
		if errors.Is(err, canonical.ErrWriterPoisoned) {
			t.Fatalf("missing filesystem authority poisoned the writer: %v", err)
		}
		assertM7ErasureDatabaseSnapshotEqual(t, e.db, beforeDatabase)
	})

	t.Run("live filesystem digest mismatch is mutation free", func(t *testing.T) {
		files, target := newM7ErasureLiveFileStore(t, e)
		corrupted := slices.Clone(target.content)
		if len(corrupted) == 0 {
			t.Fatal("production fixture unexpectedly has an empty blob")
		}
		corrupted[0] ^= 0xff
		mutateM7ErasureLiveObjectInPlace(t, files, target, corrupted)
		if _, err := files.Read(context.Background(), target.residentID, target.digest); !errors.Is(err, blob.ErrDigestMismatch) {
			t.Fatalf("corrupt live object read error=%v, want ErrDigestMismatch", err)
		}
		beforeDatabase := snapshotM7ErasureRetryState(t, e.db)
		beforeFilesystem := snapshotM7ErasureFileStoreTree(t, files)
		err := submitM7ErasureThroughProductionWriter(t, e, files, nil)
		if !errors.Is(err, erasure.ErrPlanStale) {
			t.Fatalf("error=%v, want ErrPlanStale", err)
		}
		if errors.Is(err, canonical.ErrWriterPoisoned) {
			t.Fatalf("live filesystem mismatch poisoned the writer: %v", err)
		}
		assertM7ErasureDatabaseSnapshotEqual(t, e.db, beforeDatabase)
		assertM7ErasureFileStoreTreeEqual(t, files, beforeFilesystem)
		assertM7ErasureLiveObject(t, files, target, true, corrupted)
	})

	t.Run("live filesystem disappearance is mutation free", func(t *testing.T) {
		files, target := newM7ErasureLiveFileStore(t, e)
		objects, err := files.WalkFinal(context.Background(), target.residentID)
		if err != nil {
			t.Fatal(err)
		}
		var removed bool
		for _, object := range objects {
			if object.Digest() != target.digest {
				continue
			}
			removed, err = files.RemoveFinal(context.Background(), object)
			if err != nil {
				t.Fatal(err)
			}
			break
		}
		if !removed {
			t.Fatal("live filesystem target was not removed")
		}
		beforeDatabase := snapshotM7ErasureRetryState(t, e.db)
		beforeFilesystem := snapshotM7ErasureFileStoreTree(t, files)
		err = submitM7ErasureThroughProductionWriter(t, e, files, nil)
		if !errors.Is(err, erasure.ErrPlanStale) {
			t.Fatalf("error=%v, want ErrPlanStale", err)
		}
		if errors.Is(err, canonical.ErrWriterPoisoned) {
			t.Fatalf("live filesystem disappearance poisoned the writer: %v", err)
		}
		assertM7ErasureDatabaseSnapshotEqual(t, e.db, beforeDatabase)
		assertM7ErasureFileStoreTreeEqual(t, files, beforeFilesystem)
		assertM7ErasureLiveObject(t, files, target, false, nil)
	})
}

type m7ErasureLiveTarget struct {
	residentID canonical.ID
	digest     canonical.Digest
	content    []byte
}

func newM7ErasureLiveFileStore(t *testing.T, e batchErasureEvidence) (*blob.FileStore, m7ErasureLiveTarget) {
	t.Helper()
	files, err := blob.NewFileStore(filepath.Join(t.TempDir(), "erasure-live-blobs"))
	if err != nil {
		t.Fatal(err)
	}
	var selected m7ErasureLiveTarget
	for index, target := range e.plan.EffectiveTargets {
		var owner string
		var rawDigest, content []byte
		var size int64
		if err := e.db.QueryRow(`SELECT o.owner_resident_id,o.blob_hash,b.content,b.byte_size
			FROM content_objects o
			JOIN blobs b ON b.dedupe_scope_id=o.owner_resident_id
				AND b.hash_algorithm=o.blob_hash_algorithm AND b.blob_hash=o.blob_hash
			WHERE o.content_id=? AND o.erasure_state='present'`, target.ContentID).Scan(&owner, &rawDigest, &content, &size); err != nil {
			t.Fatal(err)
		}
		residentID := mustCanonicalID(t, owner)
		digest, err := canonical.DigestFromBytes(rawDigest)
		if err != nil || canonical.HashBlob(content) != digest || int64(len(content)) != size {
			t.Fatalf("invalid production SQLite blob fixture for %s: digest=%v size=%d/%d error=%v", target.ContentID, digest, len(content), size, err)
		}
		staged, err := files.Stage(context.Background(), residentID, bytes.NewReader(content))
		if err != nil {
			t.Fatal(err)
		}
		if staged.Digest() != digest || staged.Size().Int64() != size {
			t.Fatalf("staged live object=%s/%d want=%s/%d", staged.Digest(), staged.Size().Int64(), digest, size)
		}
		if _, err := files.Finalize(context.Background(), residentID, staged); err != nil {
			t.Fatal(err)
		}
		if err := files.Acknowledge(context.Background(), residentID, staged); err != nil {
			t.Fatal(err)
		}
		if index == 0 {
			selected = m7ErasureLiveTarget{residentID: residentID, digest: digest, content: slices.Clone(content)}
		}
	}
	if len(e.plan.EffectiveTargets) == 0 {
		t.Fatal("production erasure plan has no effective target")
	}
	return files, selected
}

func submitM7ErasureThroughProductionWriter(
	t *testing.T,
	e batchErasureEvidence,
	files erasure.BlobReader,
	failpoint erasure.Failpoint,
) error {
	t.Helper()
	if e.store == nil {
		t.Fatal("production SQLite store is required")
	}
	writer, err := canonical.OpenWriter(context.Background(), canonical.WriterOptions{
		Backend: e.store.Canonical(), IDs: canonical.NewSecureIDGenerator(), Clock: canonical.SystemClock{},
		Timezone: canonical.MustTimezone(semanticTZ), QueueCapacity: 2,
	})
	if err != nil {
		t.Fatal(err)
	}
	_, submitErr := writer.Submit(context.Background(), erasure.ApplyCommand(erasure.ApplyRequest{
		Plan: e.plan, Confirm: e.plan.Digest, Blobs: files, Failpoint: failpoint,
	}))
	closeErr := writer.Close(context.Background())
	return errors.Join(submitErr, closeErr)
}

func assertM7ErasureDatabaseSnapshotEqual(t *testing.T, db *sql.DB, want []byte) {
	t.Helper()
	got := snapshotM7ErasureRetryState(t, db)
	if !bytes.Equal(want, got) {
		t.Fatalf("erasure preflight mutated production SQLite:\n--- before ---\n%s\n--- after ---\n%s", want, got)
	}
}

type m7ErasureFileStoreTreeSnapshot struct {
	nodes map[string]m7ErasureFileStoreTreeNode
}

type m7ErasureFileStoreTreeNode struct {
	info    os.FileInfo
	kind    os.FileMode
	mode    os.FileMode
	size    int64
	modTime int64
	content []byte
}

func snapshotM7ErasureFileStoreTree(t *testing.T, files *blob.FileStore) m7ErasureFileStoreTreeSnapshot {
	t.Helper()
	root := files.Root()
	snapshot := m7ErasureFileStoreTreeSnapshot{nodes: map[string]m7ErasureFileStoreTreeNode{}}
	if err := filepath.WalkDir(root, func(path string, entry os.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		relative, err := filepath.Rel(root, path)
		if err != nil {
			return err
		}
		relative = filepath.ToSlash(relative)
		info, err := entry.Info()
		if err != nil {
			return err
		}
		var content []byte
		if info.Mode().IsRegular() {
			content, err = os.ReadFile(path)
			if err != nil {
				return err
			}
		}
		stable, err := os.Lstat(path)
		if err != nil {
			return err
		}
		if !os.SameFile(info, stable) || info.Mode() != stable.Mode() || info.Size() != stable.Size() || !info.ModTime().Equal(stable.ModTime()) {
			return fmt.Errorf("unstable FileStore snapshot node %q", relative)
		}
		snapshot.nodes[relative] = m7ErasureFileStoreTreeNode{
			info: info, kind: info.Mode().Type(), mode: info.Mode(), size: info.Size(), modTime: info.ModTime().UnixNano(), content: slices.Clone(content),
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	return snapshot
}

func assertM7ErasureFileStoreTreeEqual(t *testing.T, files *blob.FileStore, want m7ErasureFileStoreTreeSnapshot) {
	t.Helper()
	got := snapshotM7ErasureFileStoreTree(t, files)
	if len(got.nodes) != len(want.nodes) {
		t.Fatalf("FileStore node count=%d want=%d; got=%v want=%v", len(got.nodes), len(want.nodes), m7ErasureFileStoreTreePaths(got), m7ErasureFileStoreTreePaths(want))
	}
	for relative, wantNode := range want.nodes {
		gotNode, exists := got.nodes[relative]
		if !exists {
			t.Fatalf("FileStore node %q disappeared; got=%v", relative, m7ErasureFileStoreTreePaths(got))
		}
		if !os.SameFile(wantNode.info, gotNode.info) {
			t.Fatalf("FileStore node %q identity changed", relative)
		}
		if gotNode.kind != wantNode.kind || gotNode.mode != wantNode.mode || gotNode.size != wantNode.size || gotNode.modTime != wantNode.modTime || !bytes.Equal(gotNode.content, wantNode.content) {
			t.Fatalf("FileStore node %q changed: kind=%v/%v mode=%v/%v size=%d/%d mtime=%d/%d bytes=%x/%x",
				relative, gotNode.kind, wantNode.kind, gotNode.mode, wantNode.mode, gotNode.size, wantNode.size, gotNode.modTime, wantNode.modTime, gotNode.content, wantNode.content)
		}
	}
}

func m7ErasureFileStoreTreePaths(snapshot m7ErasureFileStoreTreeSnapshot) []string {
	paths := make([]string, 0, len(snapshot.nodes))
	for path := range snapshot.nodes {
		paths = append(paths, path)
	}
	slices.Sort(paths)
	return paths
}

func mutateM7ErasureLiveObjectInPlace(t *testing.T, files *blob.FileStore, target m7ErasureLiveTarget, content []byte) {
	t.Helper()
	// Path access is deliberately confined to hostile fixture construction.
	// Erasure receives only the live FileStore handle authority and must detect
	// the resulting content mismatch without mutating or quarantining the entry.
	path := filepath.Join(files.Root(), "objects", target.residentID.String(), target.digest.Hex()[:2], target.digest.Hex()[2:])
	handle, err := os.OpenFile(path, os.O_WRONLY|os.O_TRUNC, 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := handle.Write(content); err != nil {
		_ = handle.Close()
		t.Fatal(err)
	}
	if err := handle.Sync(); err != nil {
		_ = handle.Close()
		t.Fatal(err)
	}
	if err := handle.Close(); err != nil {
		t.Fatal(err)
	}
}

func assertM7ErasureLiveObject(
	t *testing.T,
	files *blob.FileStore,
	target m7ErasureLiveTarget,
	wantExists bool,
	wantContent []byte,
) {
	t.Helper()
	exists, err := files.Exists(target.residentID, target.digest)
	if err != nil || exists != wantExists {
		t.Fatalf("live object exists=%t want=%t error=%v", exists, wantExists, err)
	}
	if !wantExists {
		return
	}
	reader, err := files.Open(context.Background(), target.residentID, target.digest)
	if err != nil {
		t.Fatal(err)
	}
	content, readErr := io.ReadAll(reader)
	closeErr := reader.Close()
	if readErr != nil || closeErr != nil || !bytes.Equal(content, wantContent) {
		t.Fatalf("live object content=%x want=%x read=%v close=%v", content, wantContent, readErr, closeErr)
	}
}

func TestM7ErasureLogicalProvenanceValidToBrokenUsesHistoricalReplaySource(t *testing.T) {
	fixture, plan, blobs := prepareM7LogicalSupportErasurePlan(t)
	wantClaim := fixture.claim["A"]
	var planned erasure.PlannedFinding
	found := false
	for _, finding := range plan.PlannedFindings {
		if finding.RuleCode == string(integrity.RuleClaimQualifyingSupportErased) && finding.ClaimID != nil && *finding.ClaimID == wantClaim {
			planned, found = finding, true
		}
	}
	if !found || planned.SourceContentErasureEventID == nil || *planned.SourceContentErasureEventID != plan.EffectiveTargets[0].ErasureEventID {
		t.Fatalf("support finding/source=%+v targets=%+v", planned, plan.EffectiveTargets)
	}
	conditional := false
	for _, impact := range plan.Impacts {
		if impact.RuleID == "claim_evidence_event_provenance_v1" && impact.ReferrerID == fixture.evidence["A"] {
			conditional = impact.Classification == "needs_review" && impact.DecisionMode == "consequence_only" && impact.Decision != nil && *impact.Decision == "retain"
		}
	}
	if !conditional {
		t.Fatalf("conditional logical impact missing: %+v", plan.Impacts)
	}
	writer := openM7ErasureWriter(t, fixture.db)
	result, err := writer.Submit(context.Background(), erasure.ApplyCommand(erasure.ApplyRequest{Plan: plan, Confirm: plan.Digest, Blobs: blobs}))
	if closeErr := writer.Close(context.Background()); closeErr != nil {
		t.Fatal(closeErr)
	}
	if err != nil {
		t.Fatal(err)
	}
	if value, ok := result.Value.(erasure.ApplyResult); !ok || value.ExistingCommit {
		t.Fatalf("logical apply result=%T %+v", result.Value, result.Value)
	}
	residentID := mustCanonicalID(t, fixture.resident["A"])
	claimID := mustCanonicalID(t, wantClaim)
	readTx := mustReadTx(t, fixture.db)
	candidate, err := loadOneQualifyingSupportCandidate(context.Background(), readTx, residentID, claimID)
	if err != nil {
		t.Fatal(err)
	}
	if err := readTx.Rollback(); err != nil && !errors.Is(err, sql.ErrTxDone) {
		t.Fatal(err)
	}
	if candidate == nil || candidate.RuleCode != integrity.RuleClaimQualifyingSupportErased || candidate.SourceContentErasureEventID == nil || candidate.SourceContentErasureEventID.String() != *planned.SourceContentErasureEventID {
		t.Fatalf("post-apply replay candidate=%+v", candidate)
	}
	sealed, err := integrity.NewCandidate(*candidate)
	if err != nil || planned.FindingFingerprint != "sha256:"+sealed.Fingerprint.Hex() {
		t.Fatalf("replay fingerprint=%v/%v planned=%s", sealed.Fingerprint, err, planned.FindingFingerprint)
	}

	// A later plan that reaches the already-broken claim through a
	// non-qualifying evidence row must retain the original first-break source,
	// not attribute the historical break to the new target.
	contradictContent := addM7ContradictEvidence(t, fixture, "A")
	freshBlobs := loadDatabaseBlobReader(t, fixture.db, residentID)
	planner := erasure.Planner{Source: (&Store{reader: fixture.db}).ErasureSource(freshBlobs), IDs: canonical.NewSecureIDGenerator()}
	historical, err := planner.Plan(context.Background(), erasure.PlanRequest{
		Scope: erasure.ScopeContent, ResidentID: residentID,
		ActorPrincipalID: mustCanonicalID(t, fixture.principal["human"]), ReasonCode: "privacy_request",
		IntegrityPipelineVersionID: mustCanonicalID(t, plan.IntegrityPipelineVersionID), MemoryStatusPipelineVersionID: mustCanonicalID(t, plan.MemoryStatusPipelineVersionID),
		RequestedContentIDs: []canonical.ID{mustCanonicalID(t, contradictContent)},
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(historical.Blockers) != 0 || len(historical.ExistingFindingDependencies) != 1 {
		t.Fatalf("historical replay plan blockers/dependencies=%+v/%+v", historical.Blockers, historical.ExistingFindingDependencies)
	}
	dependency := historical.ExistingFindingDependencies[0]
	if dependency.SourceContentErasureEventID == nil || *dependency.SourceContentErasureEventID != *planned.SourceContentErasureEventID || dependency.FindingFingerprint != planned.FindingFingerprint {
		t.Fatalf("historical source/fingerprint changed: %+v vs %+v", dependency, planned)
	}
	for _, finding := range historical.PlannedFindings {
		if finding.RuleCode == string(integrity.RuleClaimQualifyingSupportErased) {
			t.Fatalf("historical break was duplicated: %+v", finding)
		}
	}
}

func TestM7ErasureApplyRevalidatesLogicalProvenanceAndRollsBack(t *testing.T) {
	fixture, plan, blobs := prepareM7LogicalSupportErasurePlan(t)
	addSecondQualifyingSupport(t, fixture, "A")
	var beforeCommits int
	if err := fixture.db.QueryRow(`SELECT COUNT(*) FROM canonical_commits`).Scan(&beforeCommits); err != nil {
		t.Fatal(err)
	}
	writer := openM7ErasureWriter(t, fixture.db)
	_, err := writer.Submit(context.Background(), erasure.ApplyCommand(erasure.ApplyRequest{Plan: plan, Confirm: plan.Digest, Blobs: blobs}))
	if closeErr := writer.Close(context.Background()); closeErr != nil {
		t.Fatal(closeErr)
	}
	if !errors.Is(err, erasure.ErrPlanStale) {
		t.Fatalf("logical stale error=%v", err)
	}
	var state string
	var events, findings, commits int
	if err := fixture.db.QueryRow(`SELECT erasure_state FROM content_objects WHERE content_id=?`, fixture.content["A"]["event"]).Scan(&state); err != nil {
		t.Fatal(err)
	}
	if err := fixture.db.QueryRow(`SELECT COUNT(*) FROM content_erasure_events WHERE content_erasure_event_id=?`, plan.EffectiveTargets[0].ErasureEventID).Scan(&events); err != nil {
		t.Fatal(err)
	}
	if err := fixture.db.QueryRow(`SELECT COUNT(*) FROM integrity_findings WHERE rule_code=? AND claim_id=?`, string(integrity.RuleClaimQualifyingSupportErased), fixture.claim["A"]).Scan(&findings); err != nil {
		t.Fatal(err)
	}
	if err := fixture.db.QueryRow(`SELECT COUNT(*) FROM canonical_commits`).Scan(&commits); err != nil {
		t.Fatal(err)
	}
	if state != "present" || events != 0 || findings != 0 || commits != beforeCommits {
		t.Fatalf("logical stale partial mutation state=%s events=%d findings=%d commits=%d/%d", state, events, findings, commits, beforeCommits)
	}
}

func prepareM7LogicalSupportErasurePlan(t *testing.T) (*semanticFixture, erasure.Plan, databaseBlobReader) {
	t.Helper()
	fixture, closeFixture := newSemanticFixture(t)
	t.Cleanup(closeFixture)
	makeSemanticFixtureMinimumValid(t, fixture)
	insertResolvableDialogueCancellationHistory(t, fixture, "A")
	var ownerRows int
	if err := fixture.db.QueryRow(`SELECT COUNT(*) FROM resident_status_transitions WHERE resident_id=? AND from_status IS NULL AND to_status='draft'`, fixture.resident["A"]).Scan(&ownerRows); err != nil {
		t.Fatal(err)
	}
	if ownerRows == 0 {
		mustExec(t, fixture.db, `INSERT INTO resident_status_transitions VALUES (?,?,?,?,?,?,?,?,?,?,?,?)`, fixture.ids.new(), fixture.commit["A"], fixture.resident["A"], nil, "draft", fixture.principal["human"], "fixture_owner", nil, semanticTime, semanticTZ, semanticTime, semanticTZ)
	}
	integrityPipeline := fixture.ids.new()
	memoryStatusPipeline := fixture.ids.new()
	mustExec(t, fixture.db, `INSERT INTO pipeline_versions VALUES (?,?,?,?,?,?,?)`, integrityPipeline, fixture.commit["global"], "integrity_check", integrity.IntegrityPipelineVersion, integrityPipelineDefinitionV1, semanticTime, semanticTZ)
	mustExec(t, fixture.db, `INSERT INTO pipeline_versions VALUES (?,?,?,?,?,?,?)`, memoryStatusPipeline, fixture.commit["global"], "memory_status", domain.MemoryStatusPipelineVersion, memoryStatusPipelineDefinitionV1, semanticTime, semanticTZ)
	residentID := mustCanonicalID(t, fixture.resident["A"])
	blobs := loadDatabaseBlobReader(t, fixture.db, residentID)
	planner := erasure.Planner{Source: (&Store{reader: fixture.db}).ErasureSource(blobs), IDs: canonical.NewSecureIDGenerator()}
	plan, err := planner.Plan(context.Background(), erasure.PlanRequest{
		Scope: erasure.ScopeContent, ResidentID: residentID,
		ActorPrincipalID: mustCanonicalID(t, fixture.principal["human"]), ReasonCode: "privacy_request",
		IntegrityPipelineVersionID: mustCanonicalID(t, integrityPipeline), MemoryStatusPipelineVersionID: mustCanonicalID(t, memoryStatusPipeline),
		RequestedContentIDs: []canonical.ID{mustCanonicalID(t, fixture.content["A"]["event"])},
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(plan.Blockers) != 0 {
		t.Fatalf("logical support plan blockers=%+v", plan.Blockers)
	}
	decisions := []erasure.DecisionInput{}
	for _, impact := range plan.Impacts {
		if impact.DecisionMode != "none" {
			decisions = append(decisions, erasure.DecisionInput{ImpactID: impact.ImpactID, Decision: "retain"})
		}
	}
	plan, err = erasure.Decide(plan, decisions)
	if err != nil {
		t.Fatal(err)
	}
	if plan.PlanState != erasure.StateReady {
		t.Fatalf("logical plan state=%s", plan.PlanState)
	}
	return fixture, plan, blobs
}

func openM7ErasureWriter(t *testing.T, database *sql.DB) *canonical.Writer {
	t.Helper()
	writer, err := canonical.OpenWriter(context.Background(), canonical.WriterOptions{Backend: (&Store{writer: database, reader: database, writes: newWritePriorityGate()}).Canonical(), IDs: canonical.NewSecureIDGenerator(), Clock: canonical.SystemClock{}, Timezone: canonical.MustTimezone("UTC"), QueueCapacity: 2})
	if err != nil {
		t.Fatal(err)
	}
	return writer
}

func mustReadTx(t *testing.T, database *sql.DB) *sql.Tx {
	t.Helper()
	tx, err := database.BeginTx(context.Background(), &sql.TxOptions{ReadOnly: true})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = tx.Rollback() })
	return tx
}

func addM7ContradictEvidence(t *testing.T, fixture *semanticFixture, residentKey string) string {
	t.Helper()
	contentID := fixture.addContent(t, residentKey, "event_payload", "later-contradiction", "independent")
	eventID := fixture.ids.new()
	mustExec(t, fixture.db, "INSERT INTO events VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
		eventID, fixture.commit[residentKey], fixture.resident[residentKey], 2,
		"user_message", "conversation", 1, 0, "local_ui", "trusted",
		fixture.principal["human"], fixture.principal[residentKey], nil,
		semanticTime+2, semanticTZ, semanticTime+2, semanticTZ, contentID,
		semanticDigest("contradict-payload"), semanticDigest("first-event"), semanticDigest("contradict-event"),
		"sha256", "mahoroba:event-hash:v1", "mahoroba-jcs-v1")
	mustExec(t, fixture.db, `INSERT INTO claim_evidence(
		evidence_id,canonical_commit_id,claim_id,event_id,polarity,grade,trust_level,weight,
		derivation,source_evidence_id,memory_policy_revision_id,created_by_run_id,reason_code,
		reason_content_id,recorded_at,recorded_tz
	) VALUES (?,?,?,?,'contradict','stated','trusted',1000000,'extracted',NULL,?,?,?,NULL,?,?)`,
		fixture.ids.new(), fixture.commit[residentKey], fixture.claim[residentKey], eventID,
		fixture.revision[residentKey]["memory_policy"], fixture.run[residentKey], string(memory.EvidenceReasonSourceStated), semanticTime+2, semanticTZ)
	return contentID
}

func TestM7ErasurePlanRequiresExactPreexistingPipelines(t *testing.T) {
	e := performM7BatchAliasErasure(t, func(name string) error {
		if name == "before_mutation" {
			return errors.New("retain fixture for pipeline preflight")
		}
		return nil
	})
	missing, err := canonical.NewSecureIDGenerator().New()
	if err != nil {
		t.Fatal(err)
	}
	var beforeHead string
	var beforeCount int
	if err := e.db.QueryRow(`SELECT canonical_commit_id FROM canonical_commits ORDER BY commit_seq DESC LIMIT 1`).Scan(&beforeHead); err != nil {
		t.Fatal(err)
	}
	if err := e.db.QueryRow(`SELECT COUNT(*) FROM canonical_commits`).Scan(&beforeCount); err != nil {
		t.Fatal(err)
	}
	planner := erasure.Planner{Source: (&Store{reader: e.db}).ErasureSource(e.blobs), IDs: canonical.NewSecureIDGenerator()}
	_, err = planner.Plan(context.Background(), erasure.PlanRequest{
		Scope:                         erasure.ScopeContent,
		ResidentID:                    mustCanonicalID(t, e.plan.ResidentID),
		ActorPrincipalID:              mustCanonicalID(t, e.plan.ActorPrincipalID),
		ReasonCode:                    e.plan.ReasonCode,
		IntegrityPipelineVersionID:    missing,
		MemoryStatusPipelineVersionID: mustCanonicalID(t, e.plan.MemoryStatusPipelineVersionID),
		RequestedContentIDs:           []canonical.ID{mustCanonicalID(t, e.contentID)},
	})
	if !errors.Is(err, erasure.ErrIntegrityPipelineRequired) {
		t.Fatalf("error=%v, want ErrIntegrityPipelineRequired", err)
	}
	var afterHead string
	var afterCount int
	if err := e.db.QueryRow(`SELECT canonical_commit_id FROM canonical_commits ORDER BY commit_seq DESC LIMIT 1`).Scan(&afterHead); err != nil {
		t.Fatal(err)
	}
	if err := e.db.QueryRow(`SELECT COUNT(*) FROM canonical_commits`).Scan(&afterCount); err != nil {
		t.Fatal(err)
	}
	if beforeHead != afterHead || beforeCount != afterCount {
		t.Fatalf("pipeline preflight mutated Canonical head/count: before=(%s,%d) after=(%s,%d)", beforeHead, beforeCount, afterHead, afterCount)
	}
}

func TestM7ResidentErasureRunsThroughWriterFenceAndClearsSelection(t *testing.T) {
	t.Run("all content erased in resident commit", func(t *testing.T) {
		e := performM7BatchAliasErasure(t, func(name string) error {
			if name == "before_mutation" {
				return errors.New("retain fixture for resident plan")
			}
			return nil
		})
		runM7ResidentErasure(t, e, 0)
	})
	t.Run("historical content erasure dependencies revalidated", func(t *testing.T) {
		e := performM7BatchAliasErasure(t, nil)
		runM7ResidentErasure(t, e, len(e.claimIDs))
	})
}

func runM7ResidentErasure(t *testing.T, e batchErasureEvidence, wantHistoricalDependencies int) {
	t.Helper()
	plan, blobs, _ := prepareM7ResidentErasurePlan(t, e, wantHistoricalDependencies)

	backend := &Store{writer: e.db, reader: e.db, writes: newWritePriorityGate()}
	writer, err := canonical.OpenWriter(context.Background(), canonical.WriterOptions{
		Backend: backend.Canonical(), IDs: canonical.NewSecureIDGenerator(), Clock: canonical.SystemClock{},
		Timezone: canonical.MustTimezone("UTC"), QueueCapacity: 4,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := writer.Close(context.Background()); err != nil {
			t.Errorf("close writer: %v", err)
		}
	})
	result, err := writer.Submit(context.Background(), erasure.ApplyCommand(erasure.ApplyRequest{Plan: plan, Confirm: plan.Digest, Blobs: blobs}))
	if err != nil {
		t.Fatal(err)
	}
	applyResult, ok := result.Value.(erasure.ApplyResult)
	if !ok || applyResult.ExistingCommit || applyResult.MandatoryWorkFollowupCount != 0 {
		t.Fatalf("apply result=%T %+v", result.Value, result.Value)
	}
	probe := &rejectingBlobReader{}
	retry, err := writer.Submit(context.Background(), erasure.ApplyCommand(erasure.ApplyRequest{Plan: plan, Confirm: plan.Digest, Blobs: probe}))
	if err != nil {
		t.Fatalf("exact resident retry: %v", err)
	}
	retryResult, ok := retry.Value.(erasure.ApplyResult)
	if !ok || !retryResult.ExistingCommit || retryResult.CanonicalErasureCommitID != applyResult.CanonicalErasureCommitID || probe.opens != 0 {
		t.Fatalf("resident retry result=%T %+v blob_opens=%d", retry.Value, retry.Value, probe.opens)
	}
	var present, nonNullHashes int
	if err := e.db.QueryRow(`SELECT COUNT(*) FROM content_objects WHERE owner_resident_id=? AND erasure_state='present'`, e.plan.ResidentID).Scan(&present); err != nil {
		t.Fatal(err)
	}
	if err := e.db.QueryRow(`SELECT COUNT(statement_hash) FROM claims WHERE owner_resident_id=?`, e.plan.ResidentID).Scan(&nonNullHashes); err != nil {
		t.Fatal(err)
	}
	var status string
	if err := e.db.QueryRow(`SELECT to_status FROM resident_status_transitions t JOIN canonical_commits c ON c.canonical_commit_id=t.canonical_commit_id WHERE t.resident_id=? ORDER BY c.commit_seq DESC,t.resident_status_transition_id DESC LIMIT 1`, e.plan.ResidentID).Scan(&status); err != nil {
		t.Fatal(err)
	}
	var active sql.NullString
	if err := e.db.QueryRow(`SELECT active_resident_id FROM runtime_config WHERE singleton_id=1`).Scan(&active); err != nil && !errors.Is(err, sql.ErrNoRows) {
		t.Fatal(err)
	}
	if present != 0 || nonNullHashes != 0 || status != "erased" || active.Valid {
		t.Fatalf("resident poststate: present=%d hashes=%d status=%s active=%v", present, nonNullHashes, status, active)
	}
}

func prepareM7ResidentErasurePlan(t *testing.T, e batchErasureEvidence, wantHistoricalDependencies int) (erasure.Plan, databaseBlobReader, canonical.ID) {
	t.Helper()
	residentID := mustCanonicalID(t, e.plan.ResidentID)
	actorID := mustCanonicalID(t, e.plan.ActorPrincipalID)
	resolveResidentErasureMandatoryWork(t, e.db, residentID)
	sourceStore := &Store{reader: e.db}
	blobs := loadDatabaseBlobReader(t, e.db, residentID)
	planner := erasure.Planner{Source: sourceStore.ErasureSource(blobs), IDs: canonical.NewSecureIDGenerator()}
	plan, err := planner.Plan(context.Background(), erasure.PlanRequest{
		Scope:                         erasure.ScopeResident,
		ResidentID:                    residentID,
		ActorPrincipalID:              actorID,
		ReasonCode:                    "privacy_request",
		IntegrityPipelineVersionID:    mustCanonicalID(t, e.plan.IntegrityPipelineVersionID),
		MemoryStatusPipelineVersionID: mustCanonicalID(t, e.plan.MemoryStatusPipelineVersionID),
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(plan.Blockers) != 0 {
		t.Fatalf("resident plan blockers=%+v", plan.Blockers)
	}
	if len(plan.ExistingClaimIdentityDependencies) != wantHistoricalDependencies {
		t.Fatalf("historical claim identity dependencies=%d, want %d", len(plan.ExistingClaimIdentityDependencies), wantHistoricalDependencies)
	}
	decisions := []erasure.DecisionInput{}
	for _, impact := range plan.Impacts {
		if impact.DecisionMode != "none" {
			decisions = append(decisions, erasure.DecisionInput{ImpactID: impact.ImpactID, Decision: "retain"})
		}
	}
	plan, err = erasure.Decide(plan, decisions)
	if err != nil {
		t.Fatal(err)
	}
	if plan.PlanState != erasure.StateReady || plan.ResidentTransition == nil {
		t.Fatalf("resident plan=%s transition=%+v", plan.PlanState, plan.ResidentTransition)
	}
	return plan, blobs, residentID
}

func TestM7ResidentErasureLifecycleCrashMatrixRollsBack(t *testing.T) {
	for _, point := range []string{"resident_lifecycle", "runtime_selection", "pre_commit"} {
		t.Run(point, func(t *testing.T) {
			e := performM7BatchAliasErasure(t, func(name string) error {
				if name == "before_mutation" {
					return errors.New("retain fixture for resident crash plan")
				}
				return nil
			})
			if _, err := e.db.Exec(`INSERT INTO runtime_config(
				singleton_id, active_resident_id, desired_sessionization_policy_version_id,
				updated_at, updated_tz
			) VALUES (1, ?, NULL, ?, ?)
			ON CONFLICT(singleton_id) DO UPDATE SET
				active_resident_id=excluded.active_resident_id,
				desired_sessionization_policy_version_id=NULL,
				updated_at=excluded.updated_at,
				updated_tz=excluded.updated_tz`, e.plan.ResidentID, semanticTime, semanticTZ); err != nil {
				t.Fatal(err)
			}
			plan, blobs, _ := prepareM7ResidentErasurePlan(t, e, 0)
			if !plan.RuntimeConfigEffect.ClearActiveResident {
				t.Fatalf("resident plan did not capture the active selection: %+v", plan.RuntimeConfigEffect)
			}
			backend := &Store{writer: e.db, reader: e.db, writes: newWritePriorityGate()}
			writer, err := canonical.OpenWriter(context.Background(), canonical.WriterOptions{
				Backend: backend.Canonical(), IDs: canonical.NewSecureIDGenerator(), Clock: canonical.SystemClock{},
				Timezone: canonical.MustTimezone("UTC"), QueueCapacity: 2,
			})
			if err != nil {
				t.Fatal(err)
			}
			hit := false
			_, err = writer.Submit(context.Background(), erasure.ApplyCommand(erasure.ApplyRequest{Plan: plan, Confirm: plan.Digest, Blobs: blobs, Failpoint: func(name string) error {
				if name == point {
					hit = true
					return errors.New("injected resident crash")
				}
				return nil
			}}))
			if closeErr := writer.Close(context.Background()); closeErr != nil {
				t.Fatal(closeErr)
			}
			if err == nil || !hit {
				t.Fatalf("failpoint %s: hit=%t error=%v", point, hit, err)
			}
			var transitionRows, erasureRows, present int
			if err := e.db.QueryRow(`SELECT COUNT(*) FROM resident_status_transitions WHERE resident_status_transition_id=?`, plan.ResidentTransition.TransitionID).Scan(&transitionRows); err != nil {
				t.Fatal(err)
			}
			for _, target := range plan.EffectiveTargets {
				var count int
				if err := e.db.QueryRow(`SELECT COUNT(*) FROM content_erasure_events WHERE content_erasure_event_id=?`, target.ErasureEventID).Scan(&count); err != nil {
					t.Fatal(err)
				}
				erasureRows += count
			}
			if err := e.db.QueryRow(`SELECT COUNT(*) FROM content_objects WHERE owner_resident_id=? AND erasure_state='present'`, e.plan.ResidentID).Scan(&present); err != nil {
				t.Fatal(err)
			}
			var active sql.NullString
			if err := e.db.QueryRow(`SELECT active_resident_id FROM runtime_config WHERE singleton_id=1`).Scan(&active); err != nil {
				t.Fatal(err)
			}
			if transitionRows != 0 || erasureRows != 0 || present != len(plan.EffectiveTargets) || !active.Valid || active.String != e.plan.ResidentID {
				t.Fatalf("partial resident mutation: transition=%d events=%d present=%d active=%v", transitionRows, erasureRows, present, active)
			}
		})
	}
}

type databaseBlobReader struct{ content map[canonical.Digest][]byte }

type rejectingBlobReader struct{ opens int }

func (reader *rejectingBlobReader) Open(context.Context, canonical.ID, canonical.Digest) (io.ReadCloser, error) {
	reader.opens++
	return nil, errors.New("test blob reader must not be opened")
}

func resolveResidentErasureMandatoryWork(t *testing.T, db *sql.DB, residentID canonical.ID) {
	t.Helper()
	var eventRaw, templateRun, commitID string
	if err := db.QueryRow(`SELECT event_id,canonical_commit_id FROM events WHERE resident_id=? AND event_type='user_message' ORDER BY seq LIMIT 1`, residentID.String()).Scan(&eventRaw, &commitID); err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRow(`SELECT generation_run_id FROM generation_runs WHERE resident_id=? ORDER BY generation_run_id LIMIT 1`, residentID.String()).Scan(&templateRun); err != nil {
		t.Fatal(err)
	}
	eventID := mustCanonicalID(t, eventRaw)
	for index, value := range []struct{ purpose, key string }{
		{string(domain.GenerationPurposeDialogue), domain.DialogueObligation(eventID)},
		{string(domain.GenerationPurposeMemoryExtraction), domain.MemoryExtractionObligation(eventID)},
	} {
		runID, err := canonical.NewSecureIDGenerator().New()
		if err != nil {
			t.Fatal(err)
		}
		_, err = db.Exec(`INSERT INTO generation_runs(
			generation_run_id,canonical_commit_id,resident_id,purpose,idempotency_key,
			provider,model,model_version,prompt_template_version,pipeline_version_id,
			context_policy_version,sessionization_policy_version_id,memory_rendering_version,
			principles_revision_id,persona_revision_id,memory_policy_revision_id,recall_run_id,
			temperature,top_p,max_tokens,seed,generator_params,as_of,as_of_tz,budget_exceeded,
			dropped_input_summary,requested_at,requested_tz
		) SELECT ?,canonical_commit_id,resident_id,?,?,provider,model,model_version,
			prompt_template_version,pipeline_version_id,context_policy_version,
			sessionization_policy_version_id,memory_rendering_version,principles_revision_id,
			persona_revision_id,memory_policy_revision_id,recall_run_id,temperature,top_p,
			max_tokens,seed,generator_params,as_of,as_of_tz,budget_exceeded,
			dropped_input_summary,requested_at,requested_tz
		  FROM generation_runs WHERE generation_run_id=?`, runID.String(), value.purpose, value.key, templateRun)
		if err != nil {
			t.Fatal(err)
		}
		outcomeID, err := canonical.NewSecureIDGenerator().New()
		if err != nil {
			t.Fatal(err)
		}
		if index == 0 {
			inputID, err := canonical.NewSecureIDGenerator().New()
			if err != nil {
				t.Fatal(err)
			}
			var inputContent string
			if err := db.QueryRow(`SELECT content_id FROM content_objects WHERE owner_resident_id=? AND content_class='generation_input' LIMIT 1`, residentID.String()).Scan(&inputContent); err != nil {
				t.Fatal(err)
			}
			if _, err := db.Exec(`INSERT INTO generation_run_inputs VALUES (?,?,?,?,?,?,?,?,?,?,?)`, inputID.String(), commitID, runID.String(), 0, "user", "event", eventRaw, "current_input", inputContent, semanticTime, semanticTZ); err != nil {
				t.Fatal(err)
			}
			if _, err := db.Exec(`INSERT INTO generation_run_outcomes VALUES (?,?,?,1,'running',NULL,NULL,NULL,NULL,NULL,NULL,NULL,?,?)`, outcomeID.String(), commitID, runID.String(), semanticTime, semanticTZ); err != nil {
				t.Fatal(err)
			}
			outcomeID, err = canonical.NewSecureIDGenerator().New()
			if err != nil {
				t.Fatal(err)
			}
		}
		attempt := 0
		if index == 0 {
			attempt = 1
		}
		if _, err := db.Exec(`INSERT INTO generation_run_outcomes VALUES (?,?,?,?, 'cancelled',NULL,NULL,NULL,NULL,NULL,'source_content_erased',NULL,?,?)`, outcomeID.String(), commitID, runID.String(), attempt, semanticTime, semanticTZ); err != nil {
			t.Fatal(err)
		}
	}
}

func loadDatabaseBlobReader(t *testing.T, db *sql.DB, residentID canonical.ID) databaseBlobReader {
	t.Helper()
	rows, err := db.Query(`SELECT blob_hash,content FROM blobs WHERE dedupe_scope_id=? AND hash_algorithm='sha256'`, residentID.String())
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	reader := databaseBlobReader{content: map[canonical.Digest][]byte{}}
	for rows.Next() {
		var raw, content []byte
		if err := rows.Scan(&raw, &content); err != nil {
			t.Fatal(err)
		}
		digest, err := canonical.DigestFromBytes(raw)
		if err != nil {
			t.Fatal(err)
		}
		reader.content[digest] = slices.Clone(content)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	return reader
}

func (reader databaseBlobReader) Open(_ context.Context, _ canonical.ID, digest canonical.Digest) (io.ReadCloser, error) {
	content, ok := reader.content[digest]
	if !ok {
		return nil, sql.ErrNoRows
	}
	return io.NopCloser(bytes.NewReader(content)), nil
}

func mustCanonicalID(t *testing.T, raw string) canonical.ID {
	t.Helper()
	id, err := canonical.ParseID(raw)
	if err != nil {
		t.Fatal(err)
	}
	return id
}
func fmtHex(value []byte) string {
	const digits = "0123456789abcdef"
	out := make([]byte, len(value)*2)
	for i, b := range value {
		out[i*2] = digits[b>>4]
		out[i*2+1] = digits[b&15]
	}
	return string(out)
}
