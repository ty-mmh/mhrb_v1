package sqlite

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"fmt"
	"sort"
	"strconv"
	"strings"

	"mahoroba.local/mahoroba/internal/assets/migrations"
)

const minimumSQLiteVersion = "3.37.0"

// expectedApplicationSchemaFingerprint pins the normalized sqlite_schema
// definitions produced by the embedded v0.1.3 migrations. Goose's metadata
// table and SQLite-owned objects are deliberately outside this application
// schema contract.
const expectedApplicationSchemaFingerprint = "8a2ff7e183326b881a8cc00df68973d5e68535305cf40ebfdac23ff49abaf66c"

// SchemaReport is the evidence produced by the startup schema gate.
type SchemaReport struct {
	SQLiteVersion         string
	SchemaVersion         int64
	ApplicationTables     int
	StrictTables          int
	WithoutRowIDTables    int
	NamedIndexes          int
	Triggers              int
	ResidentScopeTriggers int
	SchemaFingerprint     string
	QuickCheck            string
	ForeignKeyViolations  int
}

// ValidateSchema verifies the exact v0.1.3 object contract and health checks.
func ValidateSchema(ctx context.Context, db *sql.DB) (SchemaReport, error) {
	var report SchemaReport
	if err := db.QueryRowContext(ctx, "SELECT sqlite_version()").Scan(&report.SQLiteVersion); err != nil {
		return report, fmt.Errorf("read SQLite version: %w", err)
	}
	if compareSQLiteVersions(report.SQLiteVersion, minimumSQLiteVersion) < 0 {
		return report, fmt.Errorf("SQLite %s is older than required %s", report.SQLiteVersion, minimumSQLiteVersion)
	}
	version, err := currentVersion(ctx, db)
	if err != nil {
		return report, err
	}
	report.SchemaVersion = version
	if version != migrations.BaselineVersion {
		return report, fmt.Errorf("schema version = %d, want %d", version, migrations.BaselineVersion)
	}

	tables, strictCount, withoutRowIDCount, err := applicationTableContract(ctx, db)
	if err != nil {
		return report, err
	}
	report.ApplicationTables = len(tables)
	report.StrictTables = strictCount
	report.WithoutRowIDTables = withoutRowIDCount
	if err := compareObjectSet("application table", tables, expectedTables); err != nil {
		return report, err
	}
	if strictCount != len(expectedTables) {
		return report, fmt.Errorf("STRICT application tables = %d, want %d", strictCount, len(expectedTables))
	}
	if withoutRowIDCount != 5 {
		return report, fmt.Errorf("implicit-key application tables = %d, want 5", withoutRowIDCount)
	}

	indexes, err := namedObjects(ctx, db, "index")
	if err != nil {
		return report, err
	}
	report.NamedIndexes = len(indexes)
	if err := compareObjectSet("named index", indexes, expectedIndexes); err != nil {
		return report, err
	}

	triggers, err := namedObjects(ctx, db, "trigger")
	if err != nil {
		return report, err
	}
	report.Triggers = len(triggers)
	if err := compareObjectSet("trigger", triggers, expectedTriggers); err != nil {
		return report, err
	}
	for _, name := range triggers {
		if strings.HasSuffix(name, "_resident_scope") || name == "trg_residents_content_scope" {
			report.ResidentScopeTriggers++
		}
	}
	if report.ResidentScopeTriggers != 21 {
		return report, fmt.Errorf("resident-scope triggers = %d, want 21", report.ResidentScopeTriggers)
	}
	fingerprint, err := applicationSchemaFingerprint(ctx, db)
	if err != nil {
		return report, err
	}
	report.SchemaFingerprint = fingerprint
	if fingerprint != expectedApplicationSchemaFingerprint {
		return report, fmt.Errorf("application schema fingerprint = %s, want embedded v0.1.3 baseline %s", fingerprint, expectedApplicationSchemaFingerprint)
	}

	quick, err := quickCheck(ctx, db)
	if err != nil {
		return report, err
	}
	report.QuickCheck = quick
	if quick != "ok" {
		return report, fmt.Errorf("PRAGMA quick_check = %q, want ok", quick)
	}
	fkViolations, err := foreignKeyViolationCount(ctx, db)
	if err != nil {
		return report, err
	}
	report.ForeignKeyViolations = fkViolations
	if fkViolations != 0 {
		return report, fmt.Errorf("PRAGMA foreign_key_check found %d violation(s)", fkViolations)
	}
	return report, nil
}

func applicationSchemaFingerprint(ctx context.Context, db *sql.DB) (string, error) {
	rows, err := db.QueryContext(ctx, `
SELECT type, name, tbl_name, sql
FROM sqlite_schema
WHERE name NOT LIKE 'sqlite_%'
  AND name <> ?
  AND tbl_name <> ?
ORDER BY type, name, tbl_name`, migrations.GooseTableName, migrations.GooseTableName)
	if err != nil {
		return "", fmt.Errorf("list application schema definitions: %w", err)
	}
	defer rows.Close()

	digest := sha256.New()
	for rows.Next() {
		var objectType, name, table string
		var definition sql.NullString
		if err := rows.Scan(&objectType, &name, &table, &definition); err != nil {
			return "", fmt.Errorf("scan application schema definition: %w", err)
		}
		writeFingerprintField(digest, objectType)
		writeFingerprintField(digest, name)
		writeFingerprintField(digest, table)
		if definition.Valid {
			writeFingerprintField(digest, "sql")
			writeFingerprintField(digest, normalizeSchemaDefinition(definition.String))
		} else {
			writeFingerprintField(digest, "null")
		}
	}
	if err := rows.Err(); err != nil {
		return "", fmt.Errorf("iterate application schema definitions: %w", err)
	}
	return hex.EncodeToString(digest.Sum(nil)), nil
}

func writeFingerprintField(digest interface{ Write([]byte) (int, error) }, value string) {
	encoded := []byte(value)
	_, _ = digest.Write([]byte(strconv.Itoa(len(encoded))))
	_, _ = digest.Write([]byte{':'})
	_, _ = digest.Write(encoded)
}

func normalizeSchemaDefinition(definition string) string {
	definition = strings.ReplaceAll(definition, "\r\n", "\n")
	definition = strings.ReplaceAll(definition, "\r", "\n")
	return strings.TrimSpace(definition)
}

func applicationTableContract(ctx context.Context, db *sql.DB) ([]string, int, int, error) {
	rows, err := db.QueryContext(ctx, "PRAGMA table_list")
	if err != nil {
		return nil, 0, 0, fmt.Errorf("PRAGMA table_list: %w", err)
	}
	defer rows.Close()
	var names []string
	strictCount := 0
	withoutRowIDCount := 0
	for rows.Next() {
		var schema, name, objectType string
		var columns, withoutRowID, strict int
		if err := rows.Scan(&schema, &name, &objectType, &columns, &withoutRowID, &strict); err != nil {
			return nil, 0, 0, fmt.Errorf("scan PRAGMA table_list: %w", err)
		}
		if schema != "main" || objectType != "table" || strings.HasPrefix(name, "sqlite_") || name == migrations.GooseTableName {
			continue
		}
		names = append(names, name)
		strictCount += strict
		withoutRowIDCount += withoutRowID
	}
	if err := rows.Err(); err != nil {
		return nil, 0, 0, fmt.Errorf("iterate PRAGMA table_list: %w", err)
	}
	sort.Strings(names)
	return names, strictCount, withoutRowIDCount, nil
}

func namedObjects(ctx context.Context, db *sql.DB, objectType string) ([]string, error) {
	rows, err := db.QueryContext(ctx,
		"SELECT name FROM sqlite_schema WHERE type = ? AND name NOT LIKE 'sqlite_%' AND tbl_name <> ? ORDER BY name",
		objectType, migrations.GooseTableName,
	)
	if err != nil {
		return nil, fmt.Errorf("list SQLite %ss: %w", objectType, err)
	}
	defer rows.Close()
	var result []string
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			return nil, fmt.Errorf("scan SQLite %s: %w", objectType, err)
		}
		result = append(result, name)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate SQLite %ss: %w", objectType, err)
	}
	return result, nil
}

func quickCheck(ctx context.Context, db *sql.DB) (string, error) {
	rows, err := db.QueryContext(ctx, "PRAGMA quick_check")
	if err != nil {
		return "", fmt.Errorf("PRAGMA quick_check: %w", err)
	}
	defer rows.Close()
	var results []string
	for rows.Next() {
		var result string
		if err := rows.Scan(&result); err != nil {
			return "", fmt.Errorf("scan PRAGMA quick_check: %w", err)
		}
		results = append(results, result)
	}
	if err := rows.Err(); err != nil {
		return "", fmt.Errorf("iterate PRAGMA quick_check: %w", err)
	}
	return strings.Join(results, "; "), nil
}

func foreignKeyViolationCount(ctx context.Context, db *sql.DB) (int, error) {
	rows, err := db.QueryContext(ctx, "PRAGMA foreign_key_check")
	if err != nil {
		return 0, fmt.Errorf("PRAGMA foreign_key_check: %w", err)
	}
	defer rows.Close()
	count := 0
	for rows.Next() {
		var table, parent string
		var foreignKeyIDValue sql.NullInt64
		var foreignKeyID int
		if err := rows.Scan(&table, &foreignKeyIDValue, &parent, &foreignKeyID); err != nil {
			return 0, fmt.Errorf("scan PRAGMA foreign_key_check: %w", err)
		}
		count++
	}
	if err := rows.Err(); err != nil {
		return 0, fmt.Errorf("iterate PRAGMA foreign_key_check: %w", err)
	}
	return count, nil
}

func compareObjectSet(kind string, got, want []string) error {
	gotSet := make(map[string]struct{}, len(got))
	for _, name := range got {
		gotSet[name] = struct{}{}
	}
	wantSet := make(map[string]struct{}, len(want))
	for _, name := range want {
		wantSet[name] = struct{}{}
	}
	var missing, unexpected []string
	for name := range wantSet {
		if _, ok := gotSet[name]; !ok {
			missing = append(missing, name)
		}
	}
	for name := range gotSet {
		if _, ok := wantSet[name]; !ok {
			unexpected = append(unexpected, name)
		}
	}
	if len(missing) == 0 && len(unexpected) == 0 {
		return nil
	}
	sort.Strings(missing)
	sort.Strings(unexpected)
	return fmt.Errorf("%s contract mismatch: missing=%v unexpected=%v", kind, missing, unexpected)
}

func compareSQLiteVersions(got, want string) int {
	parse := func(version string) [3]int {
		var result [3]int
		parts := strings.Split(version, ".")
		for i := 0; i < len(result) && i < len(parts); i++ {
			result[i], _ = strconv.Atoi(parts[i])
		}
		return result
	}
	a, b := parse(got), parse(want)
	for i := range a {
		if a[i] < b[i] {
			return -1
		}
		if a[i] > b[i] {
			return 1
		}
	}
	return 0
}

var expectedTables = []string{
	"analytics_outbox", "blobs", "canonical_commits", "claim_evidence", "claim_relations",
	"claim_stage_transition_dependencies", "claim_stage_transitions", "claim_states",
	"claim_status_transitions", "claim_usages", "claim_validity_assertions",
	"claim_view_scope_assertions", "claim_view_scope_current", "claim_statement_erasure_events", "claims", "content_erasure_events",
	"content_objects", "content_references", "events", "generation_run_inputs",
	"generation_run_outcomes", "generation_runs", "integrity_findings", "pipeline_versions",
	"principals", "projection_watermark_dependencies", "projection_watermarks", "recall_runs",
	"resident_current_revision", "resident_current_status", "resident_revision_activations",
	"resident_revision_approvals", "resident_revisions", "resident_status_transitions", "residents",
	"runtime_config", "runtime_states", "search_outbox", "sessionization_policy_versions",
}

var expectedIndexes = []string{
	"idx_analytics_outbox_pending", "idx_canonical_commits_resident_seq", "idx_claim_evidence_claim",
	"idx_claim_evidence_event", "idx_claim_evidence_source", "idx_claim_relations_from",
	"idx_claim_relations_to", "idx_claim_stage_transition_dependencies_dependency_claim",
	"idx_claim_stage_transitions_claim", "idx_claim_states_resident_salience",
	"idx_claim_states_resident_status_stage", "idx_claim_status_transitions_claim",
	"idx_claim_status_trigger_evidence", "idx_claim_status_trigger_finding", "idx_claim_status_trigger_relation",
	"idx_claim_usages_claim_time", "idx_claim_usages_generation", "idx_claim_usages_recall",
	"idx_claim_validity_claim", "idx_claim_view_scope_claim", "idx_claims_owner_recorded",
	"idx_claims_subject_kind", "idx_claim_statement_erasure_events_claim", "idx_claim_statement_erasure_events_content_event", "idx_content_erasure_events_content", "idx_content_erasure_events_source",
	"idx_content_objects_owner_state", "idx_content_references_blob", "idx_events_actor_recorded",
	"idx_events_resident_recorded", "idx_events_resident_type_seq", "idx_generation_run_inputs_run_ordinal",
	"idx_generation_run_outcomes_latest_v12", "idx_generation_run_outcomes_run_attempt", "idx_generation_runs_commit_resident_purpose_run", "idx_generation_runs_resident_purpose_requested",
	"idx_integrity_findings_claim", "idx_pipeline_versions_kind_key", "idx_principals_kind",
	"idx_projection_watermarks_source", "idx_recall_runs_resident_time",
	"idx_resident_revision_activations_resident_time", "idx_resident_revision_activations_revision",
	"idx_resident_revisions_class_time", "idx_resident_status_transitions_resident_time",
	"idx_residents_parent", "idx_search_outbox_pending", "idx_sessionization_policy_key",
	"uq_claim_stage_transitions_stage", "uq_claim_stage_transitions_entity_commit", "uq_claim_status_transitions_entity_commit",
	"uq_claim_validity_assertions_entity_commit", "uq_claim_view_scope_assertions_entity_commit", "uq_integrity_findings_fingerprint",
	"uq_events_event_hash", "uq_generation_run_outcomes_running", "uq_generation_run_outcomes_terminal",
	"uq_resident_revision_activations_entity_commit_revision", "uq_resident_status_transitions_entity_commit",
}

var expectedTriggers = []string{
	"trg_blobs_no_update", "trg_canonical_commits_no_delete", "trg_canonical_commits_no_update",
	"trg_claim_evidence_no_delete", "trg_claim_evidence_no_update", "trg_claim_evidence_resident_scope",
	"trg_claim_relations_no_delete", "trg_claim_relations_no_update", "trg_claim_relations_resident_scope",
	"trg_claim_stage_transition_dependencies_no_delete", "trg_claim_stage_transition_dependencies_no_update",
	"trg_claim_stage_dependency_resident_scope", "trg_claim_stage_transitions_no_delete",
	"trg_claim_stage_transitions_no_update", "trg_claim_stage_transitions_resident_scope",
	"trg_claim_status_transitions_no_delete", "trg_claim_status_transitions_no_update",
	"trg_claim_status_transitions_resident_scope", "trg_claim_usages_no_delete", "trg_claim_usages_no_update",
	"trg_claim_usages_resident_scope", "trg_claim_validity_assertions_no_delete",
	"trg_claim_validity_assertions_no_update", "trg_claim_validity_assertions_resident_scope",
	"trg_claim_view_scope_assertions_no_delete", "trg_claim_view_scope_assertions_no_update",
	"trg_claim_view_scope_assertions_resident_scope", "trg_claims_no_delete",
	"trg_claims_resident_scope", "trg_claims_update_contract", "trg_claim_statement_erasure_events_no_delete", "trg_claim_statement_erasure_events_no_update", "trg_claim_statement_erasure_events_scope", "trg_content_erasure_events_no_delete", "trg_content_erasure_events_no_update",
	"trg_content_erasure_events_resident_scope", "trg_content_objects_erasure_only", "trg_content_objects_no_delete",
	"trg_events_no_delete", "trg_events_no_update", "trg_events_resident_scope",
	"trg_generation_run_inputs_no_delete", "trg_generation_run_inputs_no_update",
	"trg_generation_run_inputs_resident_scope", "trg_generation_run_outcomes_no_delete",
	"trg_generation_run_outcomes_no_update", "trg_generation_run_outcomes_resident_scope",
	"trg_generation_runs_no_delete", "trg_generation_runs_no_update", "trg_generation_runs_resident_scope",
	"trg_integrity_findings_m7_writer_guard", "trg_integrity_findings_no_delete", "trg_integrity_findings_no_update", "trg_integrity_findings_resident_scope",
	"trg_pipeline_versions_no_delete", "trg_pipeline_versions_no_update", "trg_principals_no_delete",
	"trg_principals_no_update", "trg_recall_runs_no_delete", "trg_recall_runs_no_update",
	"trg_recall_runs_resident_scope", "trg_resident_revision_activations_no_delete",
	"trg_resident_revision_activations_no_update", "trg_resident_revision_activations_resident_scope",
	"trg_resident_revision_approvals_no_delete", "trg_resident_revision_approvals_no_update",
	"trg_resident_revision_approvals_resident_scope", "trg_resident_revisions_no_delete",
	"trg_resident_revisions_no_update", "trg_resident_revisions_resident_scope",
	"trg_resident_status_transitions_no_delete", "trg_resident_status_transitions_no_update",
	"trg_resident_status_transitions_resident_scope", "trg_resident_status_transitions_identity_contract", "trg_resident_revision_activations_identity_contract", "trg_claim_stage_transitions_identity_contract", "trg_claim_status_transitions_identity_contract", "trg_residents_content_scope", "trg_residents_no_delete",
	"trg_residents_no_update", "trg_sessionization_policy_versions_no_delete",
	"trg_sessionization_policy_versions_no_update",
}
