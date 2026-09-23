package sqlite

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"fmt"
	"io/fs"
	"path/filepath"
	"strings"
	"testing"

	"mahoroba.local/mahoroba/internal/assets/migrations"
	"mahoroba.local/mahoroba/internal/canonical"
	"mahoroba.local/mahoroba/internal/memory"
)

const (
	semanticTime = int64(1_700_000_000_000_000)
	semanticTZ   = "Asia/Tokyo"
)

type semanticFixture struct {
	db        *sql.DB
	ids       *semanticIDs
	commit    map[string]string
	resident  map[string]string
	principal map[string]string
	content   map[string]map[string]string
	revision  map[string]map[string]string
	recall    map[string]string
	run       map[string]string
	event     map[string]string
	claim     map[string]string
	evidence  map[string]string
	stage     map[string]string
	erasure   map[string]string
	finding   map[string]string
	pipeline  string
	session   string
}

type semanticIDs struct{ next int }

func (ids *semanticIDs) new() string {
	value := fmt.Sprintf("%026d", ids.next)
	ids.next++
	return value
}

func TestDDLResidentScopeFamiliesAndUniqueDefenses(t *testing.T) {
	fixture, closeFixture := newSemanticFixture(t)
	defer closeFixture()

	type negativeCase struct {
		name            string
		assuranceID     string
		inventoryObject string
		triggerName     string
		commit          bool
		query           string
		args            []any
		errorText       string
	}
	ca, cb := fixture.content["A"], fixture.content["B"]
	ra, rb := fixture.resident["A"], fixture.resident["B"]
	commitA, globalCommit := fixture.commit["A"], fixture.commit["global"]
	human, principalA := fixture.principal["human"], fixture.principal["A"]
	var crossResidentBlobHash []byte
	if err := fixture.db.QueryRow("SELECT blob_hash FROM content_objects WHERE content_id=?", cb["event"]).Scan(&crossResidentBlobHash); err != nil {
		t.Fatal(err)
	}

	cases := []negativeCase{
		{name: "U1_duplicate_claim_stage", query: "INSERT INTO claim_stage_transitions VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)", args: []any{
			fixture.ids.new(), commitA, fixture.claim["A"], nil, "floating", "{}", fixture.pipeline, fixture.revision["A"]["memory_policy"], fixture.run["A"], "initial", nil, semanticTime, semanticTZ, semanticTime, semanticTZ,
		}, errorText: "identity"},
		{name: "U4_duplicate_event_hash", query: "INSERT INTO events VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)", args: []any{
			fixture.ids.new(), commitA, ra, 2, "user_message", "conversation", 1, 0, "local_ui", "trusted", human, principalA, nil, semanticTime, semanticTZ, semanticTime, semanticTZ, ca["event"], semanticDigest("payload-A"), semanticDigest("event-A"), semanticDigest("event-A"), "sha256", "mahoroba:event-hash:v1", "mahoroba-jcs-v1",
		}, errorText: "UNIQUE constraint failed"},
		{name: "RS_content_blob", assuranceID: "RS-CONTENT-BLOB", inventoryObject: "content_objects(owner_resident_id,algorithm,hash) -> blobs", commit: true, query: "INSERT INTO content_objects VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)", args: []any{
			fixture.ids.new(), ra, "event_payload", crossResidentBlobHash, "sha256", semanticDigest("cross-resident-commitment"), semanticDigest("cross-resident-salt"), "sha256", "mahoroba:content-commitment:v1", "mahoroba-jcs-v1", "present", "independent", semanticTime, semanticTZ,
		}, errorText: "FOREIGN KEY constraint failed"},
		{name: "RS_resident_description", assuranceID: "RS-RES-DESCRIPTION", inventoryObject: "trg_residents_content_scope", triggerName: "trg_residents_content_scope", query: "INSERT INTO residents VALUES (?,?,?,?,?,?,?,?,?,?,?,?)", args: []any{
			fixture.resident["C"], globalCommit, fixture.principal["C"], "C", cb["description"], nil, nil, nil, nil, nil, semanticTime, semanticTZ,
		}, errorText: "must belong"},
		{name: "RS_revision", assuranceID: "RS-REVISION", inventoryObject: "trg_resident_revisions_resident_scope", triggerName: "trg_resident_revisions_resident_scope", query: "INSERT INTO resident_revisions VALUES (?,?,?,?,?,?,?,?,?,?)", args: []any{
			fixture.ids.new(), commitA, ra, "persona", cb["persona"], nil, nil, nil, semanticTime, semanticTZ,
		}, errorText: "cross-resident"},
		{name: "RS_revision_approval", assuranceID: "RS-REV-APPROVAL", inventoryObject: "trg_resident_revision_approvals_resident_scope", triggerName: "trg_resident_revision_approvals_resident_scope", query: "INSERT INTO resident_revision_approvals VALUES (?,?,?,?,?,?,?,?)", args: []any{
			fixture.ids.new(), commitA, fixture.revision["A"]["principles"], human, "approved", cb["reason"], semanticTime, semanticTZ,
		}, errorText: "must belong"},
		{name: "RS_revision_activation", assuranceID: "RS-REV-ACTIVATION", inventoryObject: "trg_resident_revision_activations_resident_scope", triggerName: "trg_resident_revision_activations_resident_scope", query: "INSERT INTO resident_revision_activations VALUES (?,?,?,?,?,?,?,?,?,?)", args: []any{
			fixture.ids.new(), commitA, ra, fixture.revision["B"]["principles"], human, nil, "activate", nil, semanticTime, semanticTZ,
		}, errorText: "cross-resident"},
		{name: "RS_resident_status", assuranceID: "RS-RES-STATUS", inventoryObject: "trg_resident_status_transitions_resident_scope", triggerName: "trg_resident_status_transitions_resident_scope", query: "INSERT INTO resident_status_transitions VALUES (?,?,?,?,?,?,?,?,?,?,?,?)", args: []any{
			fixture.ids.new(), commitA, ra, nil, "draft", human, "create", cb["reason"], semanticTime, semanticTZ, semanticTime, semanticTZ,
		}, errorText: "must belong"},
		{name: "RS_erasure", assuranceID: "RS-ERASURE", inventoryObject: "trg_content_erasure_events_resident_scope", triggerName: "trg_content_erasure_events_resident_scope", query: "INSERT INTO content_erasure_events VALUES (?,?,?,?,?,?,?,?,?,?,?,?)", args: []any{
			fixture.ids.new(), commitA, ca["event"], "content", human, "test", cb["reason"], nil, semanticTime, semanticTZ, semanticTime, semanticTZ,
		}, errorText: "cross-resident"},
		{name: "RS_recall", assuranceID: "RS-RECALL", inventoryObject: "trg_recall_runs_resident_scope", triggerName: "trg_recall_runs_resident_scope", query: "INSERT INTO recall_runs VALUES (?,?,?,?,?,?,?,?,?,?,?,?)", args: []any{
			fixture.ids.new(), commitA, ra, nil, "{}", fixture.pipeline, fixture.revision["B"]["memory_policy"], semanticTime, semanticTZ, "{}", semanticTime, semanticTZ,
		}, errorText: "cross-resident"},
		{name: "RS_generation_run", assuranceID: "RS-GEN-RUN", inventoryObject: "trg_generation_runs_resident_scope", triggerName: "trg_generation_runs_resident_scope", query: "INSERT INTO generation_runs VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)", args: []any{
			fixture.ids.new(), commitA, ra, "dialogue", "bad-gen", "test", "model", nil, "prompt-v1", fixture.pipeline, "context-v1", fixture.session, "render-v1", fixture.revision["B"]["principles"], fixture.revision["A"]["persona"], fixture.revision["A"]["memory_policy"], nil, nil, nil, nil, nil, "{}", semanticTime, semanticTZ, 0, "{}", semanticTime, semanticTZ,
		}, errorText: "cross-resident"},
		{name: "RS_generation_input_content", assuranceID: "RS-GEN-INPUT-CONTENT", inventoryObject: "trg_generation_run_inputs_resident_scope", triggerName: "trg_generation_run_inputs_resident_scope", query: "INSERT INTO generation_run_inputs VALUES (?,?,?,?,?,?,?,?,?,?,?)", args: []any{
			fixture.ids.new(), commitA, fixture.run["A"], 0, "user", "event", fixture.event["A"], "current_input", cb["input"], semanticTime, semanticTZ,
		}, errorText: "must belong"},
		{name: "RS_generation_outcome_content", assuranceID: "RS-GEN-OUTCOME", inventoryObject: "trg_generation_run_outcomes_resident_scope", triggerName: "trg_generation_run_outcomes_resident_scope", query: "INSERT INTO generation_run_outcomes VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)", args: []any{
			fixture.ids.new(), commitA, fixture.run["A"], 0, "succeeded", cb["output"], 1, 1, 1, 0, nil, nil, semanticTime, semanticTZ,
		}, errorText: "must belong"},
		{name: "RS_event", assuranceID: "RS-EVENT", inventoryObject: "trg_events_resident_scope", triggerName: "trg_events_resident_scope", query: "INSERT INTO events VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)", args: []any{
			fixture.ids.new(), commitA, ra, 2, "user_message", "conversation", 1, 0, "local_ui", "trusted", human, principalA, nil, semanticTime, semanticTZ, semanticTime, semanticTZ, cb["event"], semanticDigest("payload-x"), semanticDigest("event-A"), semanticDigest("event-x"), "sha256", "mahoroba:event-hash:v1", "mahoroba-jcs-v1",
		}, errorText: "cross-resident"},
		{name: "RS_claim", assuranceID: "RS-CLAIM", inventoryObject: "trg_claims_resident_scope", triggerName: "trg_claims_resident_scope", query: "INSERT INTO claims VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)", args: []any{
			fixture.ids.new(), commitA, ra, human, principalA, "other", "stable", cb["claim"], semanticDigest("bad-claim"), "sha256", "norm-v1", fixture.run["A"], semanticTime, semanticTZ,
		}, errorText: "cross-resident"},
		{name: "RS_evidence", assuranceID: "RS-EVIDENCE", inventoryObject: "trg_claim_evidence_resident_scope", triggerName: "trg_claim_evidence_resident_scope", query: "INSERT INTO claim_evidence VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)", args: []any{
			fixture.ids.new(), commitA, fixture.claim["A"], fixture.event["B"], "support", "stated", "trusted", 1_000_000, "extracted", nil, fixture.revision["A"]["memory_policy"], fixture.run["A"], "x", nil, semanticTime, semanticTZ,
		}, errorText: "cross-resident"},
		{name: "RS_stage", assuranceID: "RS-STAGE", inventoryObject: "trg_claim_stage_transitions_resident_scope", triggerName: "trg_claim_stage_transitions_resident_scope", query: "INSERT INTO claim_stage_transitions VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)", args: []any{
			fixture.ids.new(), commitA, fixture.claim["A"], "floating", "sediment", "{}", fixture.pipeline, fixture.revision["B"]["memory_policy"], fixture.run["A"], "x", nil, semanticTime, semanticTZ, semanticTime, semanticTZ,
		}, errorText: "cross-resident"},
		{name: "RS_stage_dependency", assuranceID: "RS-STAGE-DEPENDENCY", inventoryObject: "trg_claim_stage_dependency_resident_scope", triggerName: "trg_claim_stage_dependency_resident_scope", query: "INSERT INTO claim_stage_transition_dependencies VALUES (?,?,?,?,?)", args: []any{
			fixture.ids.new(), commitA, fixture.stage["A"], "meta_alignment", fixture.claim["B"],
		}, errorText: "must not cross"},
		{name: "RS_relation", assuranceID: "RS-RELATION", inventoryObject: "trg_claim_relations_resident_scope", triggerName: "trg_claim_relations_resident_scope", query: "INSERT INTO claim_relations VALUES (?,?,?,?,?,?,?,?,?,?,?,?)", args: []any{
			fixture.ids.new(), commitA, fixture.claim["A"], fixture.claim["B"], "contradicts", "x", nil, nil, semanticTime, semanticTZ, semanticTime, semanticTZ,
		}, errorText: "cross-resident"},
		{name: "RS_integrity", assuranceID: "RS-INTEGRITY", inventoryObject: "trg_integrity_findings_resident_scope", triggerName: "trg_integrity_findings_resident_scope", query: `INSERT INTO integrity_findings(
			integrity_finding_id, canonical_commit_id, resident_id, claim_id, finding_kind,
			source_content_erasure_event_id, pipeline_version_id, details_content_id,
			occurred_at, occurred_tz, recorded_at, recorded_tz
		) VALUES (?,?,?,?,?,?,?,?,?,?,?,?)`, args: []any{
			fixture.ids.new(), commitA, ra, fixture.claim["B"], "provenance_unresolvable", nil, fixture.pipeline, nil, semanticTime, semanticTZ, semanticTime, semanticTZ,
		}, errorText: "cross-resident"},
		{name: "RS_status", assuranceID: "RS-STATUS", inventoryObject: "trg_claim_status_transitions_resident_scope + semantic validator", triggerName: "trg_claim_status_transitions_resident_scope", query: "INSERT INTO claim_status_transitions VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)", args: []any{
			fixture.ids.new(), commitA, fixture.claim["A"], "active", "quarantined", "automatic", nil, "integrity_finding", nil, nil, nil, fixture.finding["B"], fixture.pipeline, nil, "{}", "structural_quarantine", nil, semanticTime, semanticTZ, semanticTime, semanticTZ,
		}, errorText: "cross-resident"},
		{name: "RS_validity", assuranceID: "RS-VALIDITY", inventoryObject: "trg_claim_validity_assertions_resident_scope", triggerName: "trg_claim_validity_assertions_resident_scope", query: "INSERT INTO claim_validity_assertions VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)", args: []any{
			fixture.ids.new(), commitA, fixture.claim["A"], "observed", nil, nil, nil, nil, fixture.event["B"], 1_000_000, human, "x", nil, semanticTime, semanticTZ,
		}, errorText: "cross-resident"},
		{name: "RS_view_scope", assuranceID: "RS-VIEW-SCOPE", inventoryObject: "trg_claim_view_scope_assertions_resident_scope", triggerName: "trg_claim_view_scope_assertions_resident_scope", query: "INSERT INTO claim_view_scope_assertions VALUES (?,?,?,?,?,?,?,?,?,?,?)", args: []any{
			fixture.ids.new(), commitA, fixture.claim["A"], "resident_ui", nil, fixture.run["B"], fixture.revision["A"]["memory_policy"], "x", nil, semanticTime, semanticTZ,
		}, errorText: "cross-resident"},
		{name: "RS_usage", assuranceID: "RS-USAGE", inventoryObject: "trg_claim_usages_resident_scope", triggerName: "trg_claim_usages_resident_scope", query: "INSERT INTO claim_usages VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)", args: []any{
			fixture.ids.new(), commitA, fixture.claim["A"], fixture.recall["B"], nil, "candidate", 0, fixture.revision["A"]["memory_policy"], nil, nil, nil, nil, semanticTime, semanticTZ,
		}, errorText: "cross-resident"},
	}

	seenScopeTriggers := make(map[string]struct{})
	seenInventoryIDs := make(map[string]struct{})
	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			err := executeNegative(fixture.db, testCase.commit, testCase.query, testCase.args...)
			if err == nil {
				t.Fatal("invalid insert unexpectedly succeeded")
			}
			if !strings.Contains(strings.ToLower(err.Error()), strings.ToLower(testCase.errorText)) {
				t.Fatalf("error %q does not contain %q", err, testCase.errorText)
			}
		})
		if testCase.triggerName != "" {
			seenScopeTriggers[testCase.triggerName] = struct{}{}
		}
		if testCase.assuranceID != "" {
			if testCase.inventoryObject == "" {
				t.Fatalf("assurance negative case %s has no inventory object", testCase.assuranceID)
			}
			if _, duplicate := seenInventoryIDs[testCase.assuranceID]; duplicate {
				t.Fatalf("duplicate assurance negative case %s", testCase.assuranceID)
			}
			seenInventoryIDs[testCase.assuranceID] = struct{}{}
		}
	}
	if len(seenScopeTriggers) != 21 {
		t.Fatalf("dynamic scope trigger coverage = %d, want 21", len(seenScopeTriggers))
	}
	if len(seenInventoryIDs) != 22 {
		t.Fatalf("SQLite-enforced inventory negative coverage = %d, want 22", len(seenInventoryIDs))
	}

	validContent := fixture.addContent(t, "A", "claim_statement", "A-valid-cross-principal", "independent")
	err := executeRolledBack(fixture.db, "INSERT INTO claims VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
		fixture.ids.new(), commitA, ra, human, principalA, "other", "stable", validContent,
		semanticDigest("valid-cross-principal"), "sha256", "norm-v1", fixture.run["A"], semanticTime, semanticTZ,
	)
	if err != nil {
		t.Fatalf("intentional cross-principal reference was rejected: %v", err)
	}
	_ = rb
}

func TestDDLImmutabilityErasureAndOutcomeUniqueness(t *testing.T) {
	fixture, closeFixture := newSemanticFixture(t)
	defer closeFixture()

	if err := executeRolledBack(fixture.db, "UPDATE events SET seq=2 WHERE event_id=?", fixture.event["A"]); err == nil || !strings.Contains(err.Error(), "append-only") {
		t.Fatalf("event UPDATE should be append-only blocked, got %v", err)
	}
	if err := executeRolledBack(fixture.db, "DELETE FROM claims WHERE claim_id=?", fixture.claim["A"]); err == nil || !strings.Contains(err.Error(), "append-only") {
		t.Fatalf("claim DELETE should be append-only blocked, got %v", err)
	}
	contentID := fixture.content["A"]["input"]
	if err := executeRolledBack(fixture.db, "UPDATE content_objects SET erasure_policy='resident_only' WHERE content_id=?", contentID); err == nil {
		t.Fatal("content erasure_policy mutation unexpectedly succeeded")
	}
	if _, err := fixture.db.Exec("UPDATE content_objects SET erasure_state='erased', blob_hash=NULL, commitment_salt=NULL WHERE content_id=?", contentID); err != nil {
		t.Fatalf("allowed present -> erased transition failed: %v", err)
	}
	if err := executeRolledBack(fixture.db,
		"UPDATE content_objects SET erasure_state='present', blob_hash=?, commitment_salt=? WHERE content_id=?",
		semanticDigest("restored-blob"), semanticDigest("restored-salt"), contentID,
	); err == nil {
		t.Fatal("erased -> present transition unexpectedly succeeded")
	}

	commitA := fixture.commit["A"]
	runA := fixture.run["A"]
	if _, err := fixture.db.Exec("INSERT INTO generation_run_outcomes VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
		fixture.ids.new(), commitA, runA, 7, "running", nil, nil, nil, nil, nil, nil, nil, semanticTime, semanticTZ,
	); err != nil {
		t.Fatal(err)
	}
	if err := executeRolledBack(fixture.db, "INSERT INTO generation_run_outcomes VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
		fixture.ids.new(), commitA, runA, 7, "running", nil, nil, nil, nil, nil, nil, nil, semanticTime, semanticTZ,
	); err == nil {
		t.Fatal("duplicate running outcome unexpectedly succeeded")
	}
	if _, err := fixture.db.Exec("INSERT INTO generation_run_outcomes VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
		fixture.ids.new(), commitA, runA, 7, "succeeded", fixture.content["A"]["output"], 1, 1, 1, 0, nil, nil, semanticTime, semanticTZ,
	); err != nil {
		t.Fatal(err)
	}
	if err := executeRolledBack(fixture.db, "INSERT INTO generation_run_outcomes VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
		fixture.ids.new(), commitA, runA, 7, "failed", nil, nil, nil, nil, nil, "provider", fixture.content["A"]["error"], semanticTime, semanticTZ,
	); err == nil {
		t.Fatal("duplicate terminal outcome unexpectedly succeeded")
	}

	if err := executeRolledBack(fixture.db, "INSERT INTO canonical_commits VALUES (?,?,?,?,?)", "not-a-ulid", 99, nil, semanticTime, semanticTZ); err == nil {
		t.Fatal("invalid ULID unexpectedly succeeded")
	}
	if err := executeRolledBack(fixture.db, "INSERT INTO pipeline_versions VALUES (?,?,?,?,?,?,?)", fixture.ids.new(), fixture.commit["global"], "invalid-json", "v1", "{", semanticTime, semanticTZ); err == nil {
		t.Fatal("invalid Canonical JSON unexpectedly succeeded")
	}
	if err := executeRolledBack(fixture.db, "INSERT INTO blobs VALUES (?,?,?,?,?,?,?,?,?)", fixture.resident["A"], "sha256", []byte{1, 2, 3}, []byte("x"), 1, "utf-8", "none", semanticTime, semanticTZ); err == nil {
		t.Fatal("invalid digest length unexpectedly succeeded")
	}
}

func newSemanticFixture(t *testing.T) (*semanticFixture, func()) {
	return newSemanticFixtureWithMigrations(t, migrations.Files)
}

func newSemanticFixtureWithMigrations(t *testing.T, migrationFS fs.FS) (*semanticFixture, func()) {
	t.Helper()
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "semantic.db")
	writer, err := openWriter(ctx, path, DefaultOptions())
	if err != nil {
		t.Fatal(err)
	}
	if err := initializeEmptyDatabase(ctx, writer); err != nil {
		_ = writer.Close()
		t.Fatal(err)
	}
	if err := migrateUpFS(ctx, writer, migrationFS); err != nil {
		_ = writer.Close()
		t.Fatal(err)
	}
	fixture := &semanticFixture{
		db: writer, ids: &semanticIDs{next: 1},
		commit: make(map[string]string), resident: make(map[string]string), principal: make(map[string]string),
		content:  map[string]map[string]string{"A": {}, "B": {}},
		revision: map[string]map[string]string{"A": {}, "B": {}},
		recall:   make(map[string]string), run: make(map[string]string), event: make(map[string]string),
		claim: make(map[string]string), evidence: make(map[string]string), stage: make(map[string]string),
		erasure: make(map[string]string), finding: make(map[string]string),
	}
	fixture.seed(t)
	return fixture, func() { _ = writer.Close() }
}

func (fixture *semanticFixture) seed(t *testing.T) {
	t.Helper()
	fixture.commit["global"], fixture.commit["A"], fixture.commit["B"] = fixture.ids.new(), fixture.ids.new(), fixture.ids.new()
	for _, key := range []string{"system", "human", "human2", "A", "B", "C"} {
		fixture.principal[key] = fixture.ids.new()
	}
	for _, key := range []string{"A", "B", "C"} {
		fixture.resident[key] = fixture.ids.new()
	}

	tx, err := fixture.db.Begin()
	if err != nil {
		t.Fatal(err)
	}
	mustExec(t, tx, "INSERT INTO canonical_commits VALUES (?,?,?,?,?)", fixture.commit["global"], 1, nil, semanticTime, semanticTZ)
	mustExec(t, tx, "INSERT INTO canonical_commits VALUES (?,?,?,?,?)", fixture.commit["A"], 2, fixture.resident["A"], semanticTime+1, semanticTZ)
	mustExec(t, tx, "INSERT INTO canonical_commits VALUES (?,?,?,?,?)", fixture.commit["B"], 3, fixture.resident["B"], semanticTime+2, semanticTZ)
	for _, row := range []struct{ key, commit, kind, name string }{
		{"system", "global", "system", "system"}, {"human", "global", "human", "owner"}, {"human2", "global", "human", "other-human"},
		{"A", "A", "resident", "A"}, {"B", "B", "resident", "B"}, {"C", "global", "resident", "C"},
	} {
		mustExec(t, tx, "INSERT INTO principals VALUES (?,?,?,?,?,?)", fixture.principal[row.key], fixture.commit[row.commit], row.kind, row.name, semanticTime, semanticTZ)
	}
	mustExec(t, tx, "INSERT INTO residents VALUES (?,?,?,?,?,?,?,?,?,?,?,?)", fixture.resident["A"], fixture.commit["A"], fixture.principal["A"], "A", nil, nil, nil, nil, nil, nil, semanticTime, semanticTZ)
	mustExec(t, tx, "INSERT INTO residents VALUES (?,?,?,?,?,?,?,?,?,?,?,?)", fixture.resident["B"], fixture.commit["B"], fixture.principal["B"], "B", nil, nil, nil, nil, nil, nil, semanticTime, semanticTZ)
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}

	classes := []struct{ key, class string }{
		{"principles", "principles_text"}, {"persona", "persona_text"}, {"memory", "memory_policy_text"},
		{"event", "event_payload"}, {"claim", "claim_statement"}, {"reason", "reason_text"},
		{"input", "generation_input"}, {"output", "generation_output"}, {"error", "error_detail"},
		{"description", "reason_text"},
	}
	for _, residentKey := range []string{"A", "B"} {
		for _, class := range classes {
			policy := "independent"
			text := residentKey + "-" + class.key
			if class.class == "principles_text" || class.class == "persona_text" || class.class == "memory_policy_text" {
				policy = "resident_only"
			}
			if class.class == "memory_policy_text" {
				encoded, err := memory.DefaultPolicyV2().CanonicalJSON()
				if err != nil {
					t.Fatal(err)
				}
				text = encoded.String()
			}
			fixture.content[residentKey][class.key] = fixture.addContent(t, residentKey, class.class, text, policy)
		}
	}

	fixture.pipeline, fixture.session = fixture.ids.new(), fixture.ids.new()
	mustExec(t, fixture.db, "INSERT INTO pipeline_versions VALUES (?,?,?,?,?,?,?)", fixture.pipeline, fixture.commit["global"], "memory", "v1", "{}", semanticTime, semanticTZ)
	mustExec(t, fixture.db, "INSERT INTO sessionization_policy_versions VALUES (?,?,?,?,?,?)", fixture.session, fixture.commit["global"], "v1", "{}", semanticTime, semanticTZ)

	for _, residentKey := range []string{"A", "B"} {
		for _, revisionClass := range []struct{ class, contentKey string }{{"principles", "principles"}, {"persona", "persona"}, {"memory_policy", "memory"}} {
			revisionID := fixture.ids.new()
			fixture.revision[residentKey][revisionClass.class] = revisionID
			mustExec(t, fixture.db, "INSERT INTO resident_revisions VALUES (?,?,?,?,?,?,?,?,?,?)", revisionID, fixture.commit[residentKey], fixture.resident[residentKey], revisionClass.class, fixture.content[residentKey][revisionClass.contentKey], nil, nil, nil, semanticTime, semanticTZ)
		}
		mustExec(t, fixture.db, "INSERT INTO resident_revision_approvals VALUES (?,?,?,?,?,?,?,?)", fixture.ids.new(), fixture.commit[residentKey], fixture.revision[residentKey]["principles"], fixture.principal["human"], "approved", nil, semanticTime, semanticTZ)

		fixture.recall[residentKey] = fixture.ids.new()
		mustExec(t, fixture.db, "INSERT INTO recall_runs VALUES (?,?,?,?,?,?,?,?,?,?,?,?)", fixture.recall[residentKey], fixture.commit[residentKey], fixture.resident[residentKey], nil, "{}", fixture.pipeline, fixture.revision[residentKey]["memory_policy"], semanticTime, semanticTZ, "{}", semanticTime, semanticTZ)
		fixture.run[residentKey] = fixture.ids.new()
		mustExec(t, fixture.db, "INSERT INTO generation_runs VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
			fixture.run[residentKey], fixture.commit[residentKey], fixture.resident[residentKey], "memory_extraction", "idem-"+residentKey, "test", "model", nil, "prompt-v1", fixture.pipeline, "context-v1", fixture.session, "render-v1", fixture.revision[residentKey]["principles"], fixture.revision[residentKey]["persona"], fixture.revision[residentKey]["memory_policy"], fixture.recall[residentKey], nil, nil, nil, nil, "{}", semanticTime, semanticTZ, 0, "{}", semanticTime, semanticTZ)

		fixture.event[residentKey] = fixture.ids.new()
		mustExec(t, fixture.db, "INSERT INTO events VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
			fixture.event[residentKey], fixture.commit[residentKey], fixture.resident[residentKey], 1, "user_message", "conversation", 1, 0, "local_ui", "trusted", fixture.principal["human"], fixture.principal[residentKey], nil, semanticTime, semanticTZ, semanticTime, semanticTZ, fixture.content[residentKey]["event"], semanticDigest("payload-"+residentKey), nil, semanticDigest("event-"+residentKey), "sha256", "mahoroba:event-hash:v1", "mahoroba-jcs-v1")

		fixture.claim[residentKey] = fixture.ids.new()
		statement := residentKey + "-claim"
		normalized, err := memory.NormalizeStatementV1(statement)
		if err != nil {
			t.Fatal(err)
		}
		mustExec(t, fixture.db, "INSERT INTO claims VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
			fixture.claim[residentKey], fixture.commit[residentKey], fixture.resident[residentKey], fixture.principal["human"], fixture.principal[residentKey], "other", "stable", fixture.content[residentKey]["claim"], canonical.HashBlob([]byte(normalized)).Bytes(), canonical.HashAlgorithm, memory.NormalizationVersionV1, fixture.run[residentKey], semanticTime, semanticTZ)

		fixture.evidence[residentKey] = fixture.ids.new()
		mustExec(t, fixture.db, "INSERT INTO claim_evidence VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
			fixture.evidence[residentKey], fixture.commit[residentKey], fixture.claim[residentKey], fixture.event[residentKey], "support", "stated", "trusted", 1_000_000, "extracted", nil, fixture.revision[residentKey]["memory_policy"], fixture.run[residentKey], string(memory.EvidenceReasonSourceStated), nil, semanticTime, semanticTZ)
		fixture.stage[residentKey] = fixture.ids.new()
		mustExec(t, fixture.db, "INSERT INTO claim_stage_transitions VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
			fixture.stage[residentKey], fixture.commit[residentKey], fixture.claim[residentKey], nil, "floating", "{}", fixture.pipeline, fixture.revision[residentKey]["memory_policy"], fixture.run[residentKey], "initial", nil, semanticTime, semanticTZ, semanticTime, semanticTZ)

		fixture.erasure[residentKey] = fixture.ids.new()
		mustExec(t, fixture.db, "INSERT INTO content_erasure_events VALUES (?,?,?,?,?,?,?,?,?,?,?,?)", fixture.erasure[residentKey], fixture.commit[residentKey], fixture.content[residentKey]["event"], "content", fixture.principal["human"], "test", nil, nil, semanticTime, semanticTZ, semanticTime, semanticTZ)
		fixture.finding[residentKey] = fixture.ids.new()
		mustExec(t, fixture.db, `INSERT INTO integrity_findings(
			integrity_finding_id, canonical_commit_id, resident_id, claim_id, finding_kind,
			source_content_erasure_event_id, pipeline_version_id, details_content_id,
			occurred_at, occurred_tz, recorded_at, recorded_tz
		) VALUES (?,?,?,?,?,?,?,?,?,?,?,?)`, fixture.finding[residentKey], fixture.commit[residentKey], fixture.resident[residentKey], fixture.claim[residentKey], "provenance_unresolvable", fixture.erasure[residentKey], fixture.pipeline, fixture.content[residentKey]["reason"], semanticTime, semanticTZ, semanticTime, semanticTZ)
	}
}

func (fixture *semanticFixture) addContent(t *testing.T, residentKey, class, text, policy string) string {
	t.Helper()
	contentID := fixture.ids.new()
	blobHash := canonical.HashBlob([]byte(text)).Bytes()
	salt := semanticDigest("salt:" + residentKey + ":" + contentID)
	commitment := semanticDigest("commit:" + residentKey + ":" + contentID + ":" + text)
	mustExec(t, fixture.db, "INSERT OR IGNORE INTO blobs VALUES (?,?,?,?,?,?,?,?,?)", fixture.resident[residentKey], canonical.HashAlgorithm, blobHash, []byte(text), len([]byte(text)), "utf-8", "none", semanticTime, semanticTZ)
	mustExec(t, fixture.db, "INSERT INTO content_objects VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)", contentID, fixture.resident[residentKey], class, blobHash, "sha256", commitment, salt, "sha256", "mahoroba:content-commitment:v1", "mahoroba-jcs-v1", "present", policy, semanticTime, semanticTZ)
	return contentID
}

type semanticExecer interface {
	Exec(string, ...any) (sql.Result, error)
}

func mustExec(t *testing.T, execer semanticExecer, query string, args ...any) {
	t.Helper()
	if _, err := execer.Exec(query, args...); err != nil {
		t.Fatalf("fixture SQL failed: %v\n%s", err, query)
	}
}

func executeRolledBack(db *sql.DB, query string, args ...any) error {
	return executeNegative(db, false, query, args...)
}

func executeNegative(db *sql.DB, commit bool, query string, args ...any) error {
	tx, err := db.Begin()
	if err != nil {
		return err
	}
	_, execErr := tx.Exec(query, args...)
	if execErr != nil {
		_ = tx.Rollback()
		return execErr
	}
	if commit {
		return tx.Commit()
	}
	return tx.Rollback()
}

func semanticDigest(label string) []byte {
	digest := sha256.Sum256([]byte(label))
	return digest[:]
}
