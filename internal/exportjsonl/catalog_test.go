package exportjsonl

import (
	"fmt"
	"slices"
	"testing"
)

func TestM7JSONLExportCatalogFixesV12OrdinalsAndTypedBinaryColumns(t *testing.T) {
	want := []struct {
		ordinal    int
		recordType string
		table      string
		id         string
	}{
		{0, "canonical_commit", "canonical_commits", "canonical_commit_id"},
		{10, "principal", "principals", "principal_id"},
		{20, "resident", "residents", "resident_id"},
		{30, "resident_revision", "resident_revisions", "revision_id"},
		{40, "resident_revision_approval", "resident_revision_approvals", "approval_id"},
		{50, "resident_revision_activation", "resident_revision_activations", "activation_id"},
		{60, "resident_status_transition", "resident_status_transitions", "resident_status_transition_id"},
		{70, "content_erasure_event", "content_erasure_events", "content_erasure_event_id"},
		{75, "claim_statement_erasure_event", "claim_statement_erasure_events", "claim_statement_erasure_event_id"},
		{80, "pipeline_version", "pipeline_versions", "pipeline_version_id"},
		{90, "sessionization_policy_version", "sessionization_policy_versions", "sessionization_policy_version_id"},
		{100, "recall_run", "recall_runs", "recall_run_id"},
		{110, "generation_run", "generation_runs", "generation_run_id"},
		{120, "generation_run_input", "generation_run_inputs", "generation_run_input_id"},
		{130, "generation_run_outcome", "generation_run_outcomes", "outcome_id"},
		{140, "event", "events", "event_id"},
		{150, "claim", "claims", "claim_id"},
		{160, "claim_evidence", "claim_evidence", "evidence_id"},
		{170, "claim_stage_transition", "claim_stage_transitions", "stage_transition_id"},
		{180, "claim_stage_transition_dependency", "claim_stage_transition_dependencies", "stage_transition_dependency_id"},
		{190, "claim_relation", "claim_relations", "claim_relation_id"},
		{200, "integrity_finding", "integrity_findings", "integrity_finding_id"},
		{210, "claim_status_transition", "claim_status_transitions", "status_transition_id"},
		{220, "claim_validity_assertion", "claim_validity_assertions", "validity_assertion_id"},
		{230, "claim_view_scope_assertion", "claim_view_scope_assertions", "view_scope_assertion_id"},
		{240, "claim_usage", "claim_usages", "claim_usage_id"},
	}
	catalog := Catalog()
	if len(catalog) != len(want) {
		t.Fatalf("catalog count = %d, want %d", len(catalog), len(want))
	}
	var typed []string
	for index, descriptor := range catalog {
		expected := want[index]
		if descriptor.Ordinal != expected.ordinal || descriptor.RecordType != expected.recordType ||
			descriptor.Table != expected.table || descriptor.IDColumn != expected.id {
			t.Fatalf("catalog[%d] = %#v, want %#v", index, descriptor, expected)
		}
		for _, column := range descriptor.Columns {
			if column.Storage == StorageBlob {
				typed = append(typed, fmt.Sprintf("%s.%s=%s", descriptor.Table, column.Name, column.Wire))
			}
		}
	}
	for _, column := range ContentObjectColumns() {
		if column.Storage == StorageBlob {
			typed = append(typed, fmt.Sprintf("content_objects.%s=%s", column.Name, column.Wire))
		}
	}
	wantTyped := []string{
		"events.payload_commitment=lowercase_hex_no_prefix",
		"events.prev_event_hash=lowercase_hex_no_prefix",
		"events.event_hash=lowercase_hex_no_prefix",
		"claims.statement_hash=lowercase_hex_no_prefix",
		"integrity_findings.finding_fingerprint=lowercase_hex_no_prefix",
		"content_objects.blob_hash=lowercase_hex_no_prefix",
		"content_objects.commitment=lowercase_hex_no_prefix",
		"content_objects.commitment_salt=rfc4648_base64_padded",
	}
	if !slices.Equal(typed, wantTyped) {
		t.Fatalf("typed BLOB catalog = %#v", typed)
	}
}
