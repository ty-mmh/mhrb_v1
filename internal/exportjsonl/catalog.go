package exportjsonl

import (
	"fmt"
	"slices"
)

type StorageKind string
type WireEncoding string

const (
	StorageText    StorageKind = "TEXT"
	StorageInteger StorageKind = "INTEGER"
	StorageBlob    StorageKind = "BLOB"

	WireText    WireEncoding = "text"
	WireInteger WireEncoding = "decimal_string"
	WireDigest  WireEncoding = "lowercase_hex_no_prefix"
	WireBase64  WireEncoding = "rfc4648_base64_padded"
)

type ColumnDescriptor struct {
	Name     string
	Storage  StorageKind
	Nullable bool
	Wire     WireEncoding
}

type RecordDescriptor struct {
	Ordinal      int
	RecordType   string
	Table        string
	IDColumn     string
	CommitColumn string
	EventSort    bool
	Columns      []ColumnDescriptor
}

func text(name string, nullable bool) ColumnDescriptor {
	return ColumnDescriptor{Name: name, Storage: StorageText, Nullable: nullable, Wire: WireText}
}
func integer(name string, nullable bool) ColumnDescriptor {
	return ColumnDescriptor{Name: name, Storage: StorageInteger, Nullable: nullable, Wire: WireInteger}
}
func digest(name string, nullable bool) ColumnDescriptor {
	return ColumnDescriptor{Name: name, Storage: StorageBlob, Nullable: nullable, Wire: WireDigest}
}
func binary(name string, nullable bool) ColumnDescriptor {
	return ColumnDescriptor{Name: name, Storage: StorageBlob, Nullable: nullable, Wire: WireBase64}
}

// canonicalCatalog is the compile-time v12 record catalog. Every column is
// deliberately repeated here: PRAGMA coverage rejects either a missing entry
// or an unreviewed schema addition before any export byte is emitted.
var canonicalCatalog = [...]RecordDescriptor{
	{0, "canonical_commit", "canonical_commits", "canonical_commit_id", "", false, []ColumnDescriptor{
		text("canonical_commit_id", false), integer("commit_seq", false), text("resident_id", true),
		integer("committed_at", false), text("committed_tz", false),
	}},
	{10, "principal", "principals", "principal_id", "canonical_commit_id", false, []ColumnDescriptor{
		text("principal_id", false), text("canonical_commit_id", false), text("kind", false),
		text("display_name", false), integer("created_at", false), text("created_tz", false),
	}},
	{20, "resident", "residents", "resident_id", "canonical_commit_id", false, []ColumnDescriptor{
		text("resident_id", false), text("canonical_commit_id", false), text("principal_id", false),
		text("name", false), text("description_content_id", true), text("seed_key", true),
		text("parent_resident_id", true), integer("branched_from_seq", true), integer("branched_at", true),
		text("branched_tz", true), integer("created_at", false), text("created_tz", false),
	}},
	{30, "resident_revision", "resident_revisions", "revision_id", "canonical_commit_id", false, []ColumnDescriptor{
		text("revision_id", false), text("canonical_commit_id", false), text("resident_id", false),
		text("revision_class", false), text("content_id", false), text("parent_revision_id", true),
		text("created_by_run_id", true), text("reason_content_id", true), integer("recorded_at", false), text("recorded_tz", false),
	}},
	{40, "resident_revision_approval", "resident_revision_approvals", "approval_id", "canonical_commit_id", false, []ColumnDescriptor{
		text("approval_id", false), text("canonical_commit_id", false), text("revision_id", false),
		text("approver_principal_id", false), text("decision", false), text("reason_content_id", true),
		integer("recorded_at", false), text("recorded_tz", false),
	}},
	{50, "resident_revision_activation", "resident_revision_activations", "activation_id", "canonical_commit_id", false, []ColumnDescriptor{
		text("activation_id", false), text("canonical_commit_id", false), text("resident_id", false),
		text("revision_id", false), text("actor_principal_id", false), text("approval_id", true),
		text("reason_code", false), text("reason_content_id", true), integer("recorded_at", false), text("recorded_tz", false),
	}},
	{60, "resident_status_transition", "resident_status_transitions", "resident_status_transition_id", "canonical_commit_id", false, []ColumnDescriptor{
		text("resident_status_transition_id", false), text("canonical_commit_id", false), text("resident_id", false),
		text("from_status", true), text("to_status", false), text("actor_principal_id", false),
		text("reason_code", false), text("reason_content_id", true), integer("occurred_at", false),
		text("occurred_tz", false), integer("recorded_at", false), text("recorded_tz", false),
	}},
	{70, "content_erasure_event", "content_erasure_events", "content_erasure_event_id", "canonical_commit_id", false, []ColumnDescriptor{
		text("content_erasure_event_id", false), text("canonical_commit_id", false), text("content_id", false),
		text("erasure_scope", false), text("actor_principal_id", false), text("reason_code", false),
		text("reason_content_id", true), text("source_erasure_event_id", true), integer("occurred_at", false),
		text("occurred_tz", false), integer("recorded_at", false), text("recorded_tz", false),
	}},
	{75, "claim_statement_erasure_event", "claim_statement_erasure_events", "claim_statement_erasure_event_id", "canonical_commit_id", false, []ColumnDescriptor{
		text("claim_statement_erasure_event_id", false), text("canonical_commit_id", false), text("resident_id", false),
		text("claim_id", false), text("content_erasure_event_id", false), integer("recorded_at", false), text("recorded_tz", false),
	}},
	{80, "pipeline_version", "pipeline_versions", "pipeline_version_id", "canonical_commit_id", false, []ColumnDescriptor{
		text("pipeline_version_id", false), text("canonical_commit_id", false), text("pipeline_kind", false),
		text("version_key", false), text("definition", false), integer("recorded_at", false), text("recorded_tz", false),
	}},
	{90, "sessionization_policy_version", "sessionization_policy_versions", "sessionization_policy_version_id", "canonical_commit_id", false, []ColumnDescriptor{
		text("sessionization_policy_version_id", false), text("canonical_commit_id", false), text("version_key", false),
		text("definition", false), integer("recorded_at", false), text("recorded_tz", false),
	}},
	{100, "recall_run", "recall_runs", "recall_run_id", "canonical_commit_id", false, []ColumnDescriptor{
		text("recall_run_id", false), text("canonical_commit_id", false), text("resident_id", false),
		text("query_content_id", true), text("query_conditions", false), text("pipeline_version_id", false),
		text("memory_policy_revision_id", false), integer("as_of", false), text("as_of_tz", false),
		text("context_constraints", false), integer("recorded_at", false), text("recorded_tz", false),
	}},
	{110, "generation_run", "generation_runs", "generation_run_id", "canonical_commit_id", false, []ColumnDescriptor{
		text("generation_run_id", false), text("canonical_commit_id", false), text("resident_id", false),
		text("purpose", false), text("idempotency_key", false), text("provider", false), text("model", false),
		text("model_version", true), text("prompt_template_version", false), text("pipeline_version_id", false),
		text("context_policy_version", false), text("sessionization_policy_version_id", true),
		text("memory_rendering_version", false), text("principles_revision_id", false), text("persona_revision_id", false),
		text("memory_policy_revision_id", false), text("recall_run_id", true), integer("temperature", true),
		integer("top_p", true), integer("max_tokens", true), integer("seed", true), text("generator_params", false),
		integer("as_of", false), text("as_of_tz", false), integer("budget_exceeded", false),
		text("dropped_input_summary", false), integer("requested_at", false), text("requested_tz", false),
	}},
	{120, "generation_run_input", "generation_run_inputs", "generation_run_input_id", "canonical_commit_id", false, []ColumnDescriptor{
		text("generation_run_input_id", false), text("canonical_commit_id", false), text("generation_run_id", false),
		integer("ordinal", false), text("role", false), text("source_type", false), text("source_id", true),
		text("inclusion_mode", false), text("content_id", false), integer("recorded_at", false), text("recorded_tz", false),
	}},
	{130, "generation_run_outcome", "generation_run_outcomes", "outcome_id", "canonical_commit_id", false, []ColumnDescriptor{
		text("outcome_id", false), text("canonical_commit_id", false), text("generation_run_id", false),
		integer("attempt_no", false), text("state", false), text("output_content_id", true),
		integer("prompt_tokens", true), integer("completion_tokens", true), integer("latency", true),
		integer("estimated_cost", true), text("error_class", true), text("error_detail_content_id", true),
		integer("recorded_at", false), text("recorded_tz", false),
	}},
	{140, "event", "events", "event_id", "canonical_commit_id", true, []ColumnDescriptor{
		text("event_id", false), text("canonical_commit_id", false), text("resident_id", false), integer("seq", false),
		text("event_type", false), text("visibility", false), integer("delivery_screen", false), integer("delivery_audio", false),
		text("ingress", false), text("trust_level", false), text("actor_principal_id", false), text("target_principal_id", true),
		text("generation_run_id", true), integer("occurred_at", false), text("occurred_tz", false),
		integer("recorded_at", false), text("recorded_tz", false), text("content_id", false), digest("payload_commitment", false),
		digest("prev_event_hash", true), digest("event_hash", false), text("event_hash_algorithm", false),
		text("event_hash_domain", false), text("canonicalization_version", false),
	}},
	{150, "claim", "claims", "claim_id", "canonical_commit_id", false, []ColumnDescriptor{
		text("claim_id", false), text("canonical_commit_id", false), text("owner_resident_id", false),
		text("subject_principal_id", false), text("perspective_principal_id", false), text("kind", true),
		text("temporal_kind", false), text("statement_content_id", false), digest("statement_hash", true),
		text("statement_hash_algorithm", false), text("statement_normalization_version", false),
		text("created_by_run_id", false), integer("recorded_at", false), text("recorded_tz", false),
	}},
	{160, "claim_evidence", "claim_evidence", "evidence_id", "canonical_commit_id", false, []ColumnDescriptor{
		text("evidence_id", false), text("canonical_commit_id", false), text("claim_id", false), text("event_id", false),
		text("polarity", false), text("grade", false), text("trust_level", false), integer("weight", false),
		text("derivation", false), text("source_evidence_id", true), text("memory_policy_revision_id", false),
		text("created_by_run_id", false), text("reason_code", false), text("reason_content_id", true),
		integer("recorded_at", false), text("recorded_tz", false),
	}},
	{170, "claim_stage_transition", "claim_stage_transitions", "stage_transition_id", "canonical_commit_id", false, []ColumnDescriptor{
		text("stage_transition_id", false), text("canonical_commit_id", false), text("claim_id", false),
		text("from_stage", true), text("to_stage", false), text("gate_metrics", false), text("pipeline_version_id", false),
		text("memory_policy_revision_id", false), text("generation_run_id", true), text("reason_code", false),
		text("reason_content_id", true), integer("occurred_at", false), text("occurred_tz", false),
		integer("recorded_at", false), text("recorded_tz", false),
	}},
	{180, "claim_stage_transition_dependency", "claim_stage_transition_dependencies", "stage_transition_dependency_id", "canonical_commit_id", false, []ColumnDescriptor{
		text("stage_transition_dependency_id", false), text("canonical_commit_id", false), text("stage_transition_id", false),
		text("dependency_kind", false), text("dependency_claim_id", false),
	}},
	{190, "claim_relation", "claim_relations", "claim_relation_id", "canonical_commit_id", false, []ColumnDescriptor{
		text("claim_relation_id", false), text("canonical_commit_id", false), text("from_claim_id", false),
		text("to_claim_id", false), text("relation_type", false), text("reason_code", false), text("reason_content_id", true),
		text("generation_run_id", true), integer("occurred_at", false), text("occurred_tz", false),
		integer("recorded_at", false), text("recorded_tz", false),
	}},
	{200, "integrity_finding", "integrity_findings", "integrity_finding_id", "canonical_commit_id", false, []ColumnDescriptor{
		text("integrity_finding_id", false), text("canonical_commit_id", false), text("resident_id", false),
		text("claim_id", true), text("finding_kind", false), text("source_content_erasure_event_id", true),
		text("pipeline_version_id", false), text("details_content_id", true), integer("occurred_at", false),
		text("occurred_tz", false), integer("recorded_at", false), text("recorded_tz", false),
		digest("finding_fingerprint", true), text("rule_code", true), text("target_kind", true),
		text("target_id", true), text("target_field", true),
	}},
	{210, "claim_status_transition", "claim_status_transitions", "status_transition_id", "canonical_commit_id", false, []ColumnDescriptor{
		text("status_transition_id", false), text("canonical_commit_id", false), text("claim_id", false),
		text("from_status", false), text("to_status", false), text("decision_kind", false), text("actor_principal_id", true),
		text("trigger_kind", true), text("trigger_event_id", true), text("trigger_evidence_id", true),
		text("trigger_claim_relation_id", true), text("trigger_integrity_finding_id", true), text("pipeline_version_id", true),
		text("memory_policy_revision_id", true), text("gate_metrics", true), text("decision_reason_code", false),
		text("decision_reason_content_id", true), integer("occurred_at", false), text("occurred_tz", false),
		integer("recorded_at", false), text("recorded_tz", false),
	}},
	{220, "claim_validity_assertion", "claim_validity_assertions", "validity_assertion_id", "canonical_commit_id", false, []ColumnDescriptor{
		text("validity_assertion_id", false), text("canonical_commit_id", false), text("claim_id", false),
		text("assertion_type", false), integer("valid_from", true), text("valid_from_tz", true),
		integer("valid_to", true), text("valid_to_tz", true), text("evidence_event_id", true), integer("confidence", true),
		text("actor_principal_id", true), text("reason_code", false), text("reason_content_id", true),
		integer("recorded_at", false), text("recorded_tz", false),
	}},
	{230, "claim_view_scope_assertion", "claim_view_scope_assertions", "view_scope_assertion_id", "canonical_commit_id", false, []ColumnDescriptor{
		text("view_scope_assertion_id", false), text("canonical_commit_id", false), text("claim_id", false),
		text("view_scope", false), text("actor_principal_id", true), text("generation_run_id", true),
		text("memory_policy_revision_id", true), text("reason_code", false), text("reason_content_id", true),
		integer("recorded_at", false), text("recorded_tz", false),
	}},
	{240, "claim_usage", "claim_usages", "claim_usage_id", "canonical_commit_id", false, []ColumnDescriptor{
		text("claim_usage_id", false), text("canonical_commit_id", false), text("claim_id", false),
		text("recall_run_id", true), text("generation_run_id", true), text("usage_type", false), integer("ordinal", true),
		text("memory_policy_revision_id", false), text("exclusion_reason", true), text("detection_method", true),
		integer("detection_confidence", true), text("detected_by_run_id", true), integer("recorded_at", false), text("recorded_tz", false),
	}},
}

var contentObjectColumns = [...]ColumnDescriptor{
	text("content_id", false), text("owner_resident_id", false), text("content_class", false),
	digest("blob_hash", true), text("blob_hash_algorithm", false), digest("commitment", false),
	binary("commitment_salt", true), text("commitment_hash_algorithm", false), text("commitment_domain", false),
	text("canonicalization_version", false), text("erasure_state", false), text("erasure_policy", false),
	integer("created_at", false), text("created_tz", false),
}

func Catalog() []RecordDescriptor {
	result := slices.Clone(canonicalCatalog[:])
	for index := range result {
		result[index].Columns = slices.Clone(result[index].Columns)
	}
	return result
}

func ContentObjectColumns() []ColumnDescriptor { return slices.Clone(contentObjectColumns[:]) }

func ValidateCatalog() error {
	previousOrdinal := -1
	tables, recordTypes := map[string]struct{}{}, map[string]struct{}{}
	for index, descriptor := range canonicalCatalog {
		if descriptor.Ordinal <= previousOrdinal || descriptor.RecordType == "" || descriptor.Table == "" ||
			descriptor.IDColumn == "" || len(descriptor.Columns) == 0 {
			return fmt.Errorf("%w: invalid descriptor %d", ErrSchemaCoverage, index)
		}
		previousOrdinal = descriptor.Ordinal
		if _, duplicate := tables[descriptor.Table]; duplicate {
			return fmt.Errorf("%w: duplicate table %s", ErrSchemaCoverage, descriptor.Table)
		}
		if _, duplicate := recordTypes[descriptor.RecordType]; duplicate {
			return fmt.Errorf("%w: duplicate record type %s", ErrSchemaCoverage, descriptor.RecordType)
		}
		tables[descriptor.Table], recordTypes[descriptor.RecordType] = struct{}{}, struct{}{}
		seen, hasID, hasCommit := map[string]struct{}{}, false, descriptor.CommitColumn == ""
		for _, column := range descriptor.Columns {
			if column.Name == "" || column.Storage == "" || column.Wire == "" {
				return fmt.Errorf("%w: incomplete %s column", ErrSchemaCoverage, descriptor.Table)
			}
			if _, duplicate := seen[column.Name]; duplicate {
				return fmt.Errorf("%w: duplicate %s.%s", ErrSchemaCoverage, descriptor.Table, column.Name)
			}
			seen[column.Name] = struct{}{}
			hasID = hasID || column.Name == descriptor.IDColumn
			hasCommit = hasCommit || column.Name == descriptor.CommitColumn
		}
		if !hasID || !hasCommit {
			return fmt.Errorf("%w: %s lacks ID or commit column", ErrSchemaCoverage, descriptor.Table)
		}
	}
	if len(contentObjectColumns) != 14 {
		return fmt.Errorf("%w: content_objects has %d catalog columns", ErrSchemaCoverage, len(contentObjectColumns))
	}
	return nil
}
