package contentref

import (
	"reflect"
	"testing"
)

func TestM7ContentReferenceCatalogIsClosedAndShared(t *testing.T) {
	descriptors := DirectDescriptors()
	if len(descriptors) != 22 {
		t.Fatalf("descriptor count = %d, want 22", len(descriptors))
	}
	want := []string{
		"claim_evidence_reason_v1|claim_evidence(evidence_id).reason_content_id|safe|none|",
		"claim_relation_reason_v1|claim_relation(claim_relation_id).reason_content_id|safe|none|",
		"claim_stage_transition_reason_v1|claim_stage_transition(stage_transition_id).reason_content_id|safe|none|",
		"claim_statement_v1|claim(claim_id).statement_content_id|needs_review|consequence_only|erase_claim_identity",
		"claim_status_transition_reason_v1|claim_status_transition(status_transition_id).decision_reason_content_id|safe|none|",
		"claim_validity_assertion_reason_v1|claim_validity_assertion(validity_assertion_id).reason_content_id|safe|none|",
		"claim_view_scope_assertion_reason_v1|claim_view_scope_assertion(view_scope_assertion_id).reason_content_id|safe|none|",
		"content_erasure_event_content_v1|content_erasure_event(content_erasure_event_id).content_id|safe|none|",
		"content_erasure_event_reason_v1|content_erasure_event(content_erasure_event_id).reason_content_id|safe|none|",
		"content_object_blob_v1|content_object(content_id).blob|needs_rebuild|none|block_physical_gc,rebuild_projection",
		"event_content_v1|event(event_id).content_id|safe|none|",
		"generation_input_content_v1|generation_run_input(generation_run_input_id).content_id|needs_review|consequence_only|",
		"generation_outcome_error_detail_v1|generation_run_outcome(outcome_id).error_detail_content_id|safe|none|",
		"generation_outcome_output_v1|generation_run_outcome(outcome_id).output_content_id|needs_review|consequence_only|",
		"integrity_finding_details_v1|integrity_finding(integrity_finding_id).details_content_id|safe|none|",
		"recall_query_v1|recall_run(recall_run_id).query_content_id|needs_review|consequence_only|",
		"resident_description_v1|resident(resident_id).description_content_id|safe|none|",
		"resident_revision_activation_reason_v1|resident_revision_activation(activation_id).reason_content_id|safe|none|",
		"resident_revision_approval_reason_v1|resident_revision_approval(approval_id).reason_content_id|safe|none|",
		"resident_revision_content_v1|resident_revision(revision_id).content_id|safe|none|",
		"resident_revision_reason_v1|resident_revision(revision_id).reason_content_id|safe|none|",
		"resident_status_transition_reason_v1|resident_status_transition(resident_status_transition_id).reason_content_id|safe|none|",
	}
	got := make([]string, 0, len(descriptors))
	for _, descriptor := range descriptors {
		actions := ""
		for index, action := range descriptor.Actions {
			if index != 0 {
				actions += ","
			}
			actions += string(action)
		}
		got = append(got, descriptor.RuleID+"|"+descriptor.ReferrerKind+"("+descriptor.PrimaryKey+")."+descriptor.ReferrerField+"|"+
			string(descriptor.Classification)+"|"+string(descriptor.DecisionMode)+"|"+actions)
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("catalog differs\n got: %#v\nwant: %#v", got, want)
	}

	// A consumer must not be able to mutate the process-global catalog.
	descriptors[0].Actions = append(descriptors[0].Actions, BlockPhysicalGC)
	descriptors[0].RuleID = "changed"
	if again := DirectDescriptors(); again[0].RuleID != "claim_evidence_reason_v1" || len(again[0].Actions) != 0 {
		t.Fatalf("catalog copy leaked caller mutation: %+v", again[0])
	}
}
