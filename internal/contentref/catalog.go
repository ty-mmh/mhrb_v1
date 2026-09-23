// Package contentref owns the compile-time catalog of Canonical fields which
// directly reference content. Erasure impact analysis and the
// content-references-v1 Projection consume the same descriptors so their
// understanding of reachability cannot drift independently.
package contentref

import (
	"fmt"
	"slices"
	"strings"
)

const (
	CatalogVersion    = "content-reference-catalog-v1"
	ProjectionName    = "content_references"
	ProjectionVersion = "content-references-v1"
)

type Classification string

const (
	Safe         Classification = "safe"
	NeedsRebuild Classification = "needs_rebuild"
	NeedsReview  Classification = "needs_review"
)

type DecisionMode string

const (
	DecisionNone    DecisionMode = "none"
	ConsequenceOnly DecisionMode = "consequence_only"
)

type Action string

const (
	BlockPhysicalGC    Action = "block_physical_gc"
	EraseClaimIdentity Action = "erase_claim_identity"
	RebuildProjection  Action = "rebuild_projection"
)

// Descriptor contains only compile-time SQL identifiers. None of its fields
// may be populated from a plan, CLI argument, database row, or configuration.
// ReferrerKind and ReferrerField are the exact public erasure-impact tuple.
type Descriptor struct {
	RuleID         string
	Table          string
	PrimaryKey     string
	ContentField   string
	ReferrerKind   string
	ReferrerField  string
	Classification Classification
	DecisionMode   DecisionMode
	Actions        []Action
}

var directDescriptors = []Descriptor{
	{RuleID: "claim_evidence_reason_v1", Table: "claim_evidence", PrimaryKey: "evidence_id", ContentField: "reason_content_id", ReferrerKind: "claim_evidence", ReferrerField: "reason_content_id", Classification: Safe, DecisionMode: DecisionNone},
	{RuleID: "claim_relation_reason_v1", Table: "claim_relations", PrimaryKey: "claim_relation_id", ContentField: "reason_content_id", ReferrerKind: "claim_relation", ReferrerField: "reason_content_id", Classification: Safe, DecisionMode: DecisionNone},
	{RuleID: "claim_stage_transition_reason_v1", Table: "claim_stage_transitions", PrimaryKey: "stage_transition_id", ContentField: "reason_content_id", ReferrerKind: "claim_stage_transition", ReferrerField: "reason_content_id", Classification: Safe, DecisionMode: DecisionNone},
	{RuleID: "claim_statement_v1", Table: "claims", PrimaryKey: "claim_id", ContentField: "statement_content_id", ReferrerKind: "claim", ReferrerField: "statement_content_id", Classification: NeedsReview, DecisionMode: ConsequenceOnly, Actions: []Action{EraseClaimIdentity}},
	{RuleID: "claim_status_transition_reason_v1", Table: "claim_status_transitions", PrimaryKey: "status_transition_id", ContentField: "decision_reason_content_id", ReferrerKind: "claim_status_transition", ReferrerField: "decision_reason_content_id", Classification: Safe, DecisionMode: DecisionNone},
	{RuleID: "claim_validity_assertion_reason_v1", Table: "claim_validity_assertions", PrimaryKey: "validity_assertion_id", ContentField: "reason_content_id", ReferrerKind: "claim_validity_assertion", ReferrerField: "reason_content_id", Classification: Safe, DecisionMode: DecisionNone},
	{RuleID: "claim_view_scope_assertion_reason_v1", Table: "claim_view_scope_assertions", PrimaryKey: "view_scope_assertion_id", ContentField: "reason_content_id", ReferrerKind: "claim_view_scope_assertion", ReferrerField: "reason_content_id", Classification: Safe, DecisionMode: DecisionNone},
	{RuleID: "content_erasure_event_content_v1", Table: "content_erasure_events", PrimaryKey: "content_erasure_event_id", ContentField: "content_id", ReferrerKind: "content_erasure_event", ReferrerField: "content_id", Classification: Safe, DecisionMode: DecisionNone},
	{RuleID: "content_erasure_event_reason_v1", Table: "content_erasure_events", PrimaryKey: "content_erasure_event_id", ContentField: "reason_content_id", ReferrerKind: "content_erasure_event", ReferrerField: "reason_content_id", Classification: Safe, DecisionMode: DecisionNone},
	{RuleID: "content_object_blob_v1", Table: "content_objects", PrimaryKey: "content_id", ContentField: "content_id", ReferrerKind: "content_object", ReferrerField: "blob", Classification: NeedsRebuild, DecisionMode: DecisionNone, Actions: []Action{BlockPhysicalGC, RebuildProjection}},
	{RuleID: "event_content_v1", Table: "events", PrimaryKey: "event_id", ContentField: "content_id", ReferrerKind: "event", ReferrerField: "content_id", Classification: Safe, DecisionMode: DecisionNone},
	{RuleID: "generation_input_content_v1", Table: "generation_run_inputs", PrimaryKey: "generation_run_input_id", ContentField: "content_id", ReferrerKind: "generation_run_input", ReferrerField: "content_id", Classification: NeedsReview, DecisionMode: ConsequenceOnly},
	{RuleID: "generation_outcome_error_detail_v1", Table: "generation_run_outcomes", PrimaryKey: "outcome_id", ContentField: "error_detail_content_id", ReferrerKind: "generation_run_outcome", ReferrerField: "error_detail_content_id", Classification: Safe, DecisionMode: DecisionNone},
	{RuleID: "generation_outcome_output_v1", Table: "generation_run_outcomes", PrimaryKey: "outcome_id", ContentField: "output_content_id", ReferrerKind: "generation_run_outcome", ReferrerField: "output_content_id", Classification: NeedsReview, DecisionMode: ConsequenceOnly},
	{RuleID: "integrity_finding_details_v1", Table: "integrity_findings", PrimaryKey: "integrity_finding_id", ContentField: "details_content_id", ReferrerKind: "integrity_finding", ReferrerField: "details_content_id", Classification: Safe, DecisionMode: DecisionNone},
	{RuleID: "recall_query_v1", Table: "recall_runs", PrimaryKey: "recall_run_id", ContentField: "query_content_id", ReferrerKind: "recall_run", ReferrerField: "query_content_id", Classification: NeedsReview, DecisionMode: ConsequenceOnly},
	{RuleID: "resident_description_v1", Table: "residents", PrimaryKey: "resident_id", ContentField: "description_content_id", ReferrerKind: "resident", ReferrerField: "description_content_id", Classification: Safe, DecisionMode: DecisionNone},
	{RuleID: "resident_revision_activation_reason_v1", Table: "resident_revision_activations", PrimaryKey: "activation_id", ContentField: "reason_content_id", ReferrerKind: "resident_revision_activation", ReferrerField: "reason_content_id", Classification: Safe, DecisionMode: DecisionNone},
	{RuleID: "resident_revision_approval_reason_v1", Table: "resident_revision_approvals", PrimaryKey: "approval_id", ContentField: "reason_content_id", ReferrerKind: "resident_revision_approval", ReferrerField: "reason_content_id", Classification: Safe, DecisionMode: DecisionNone},
	{RuleID: "resident_revision_content_v1", Table: "resident_revisions", PrimaryKey: "revision_id", ContentField: "content_id", ReferrerKind: "resident_revision", ReferrerField: "content_id", Classification: Safe, DecisionMode: DecisionNone},
	{RuleID: "resident_revision_reason_v1", Table: "resident_revisions", PrimaryKey: "revision_id", ContentField: "reason_content_id", ReferrerKind: "resident_revision", ReferrerField: "reason_content_id", Classification: Safe, DecisionMode: DecisionNone},
	{RuleID: "resident_status_transition_reason_v1", Table: "resident_status_transitions", PrimaryKey: "resident_status_transition_id", ContentField: "reason_content_id", ReferrerKind: "resident_status_transition", ReferrerField: "reason_content_id", Classification: Safe, DecisionMode: DecisionNone},
}

func init() {
	if err := validateCatalog(directDescriptors); err != nil {
		panic(err)
	}
}

// DirectDescriptors returns a deep copy in stable rule-ID order.
func DirectDescriptors() []Descriptor {
	result := make([]Descriptor, len(directDescriptors))
	for index, descriptor := range directDescriptors {
		result[index] = descriptor
		result[index].Actions = slices.Clone(descriptor.Actions)
	}
	return result
}

func validateCatalog(values []Descriptor) error {
	previousRule := ""
	tuples := make(map[string]struct{}, len(values))
	for _, value := range values {
		if value.RuleID == "" || value.Table == "" || value.PrimaryKey == "" || value.ContentField == "" || value.ReferrerKind == "" || value.ReferrerField == "" {
			return fmt.Errorf("contentref: incomplete descriptor %q", value.RuleID)
		}
		if previousRule != "" && value.RuleID <= previousRule {
			return fmt.Errorf("contentref: rule IDs are duplicated or unordered: %q", value.RuleID)
		}
		previousRule = value.RuleID
		for _, identifier := range []string{value.Table, value.PrimaryKey, value.ContentField} {
			if !validIdentifier(identifier) {
				return fmt.Errorf("contentref: unsafe SQL identifier %q", identifier)
			}
		}
		switch value.Classification {
		case Safe, NeedsRebuild, NeedsReview:
		default:
			return fmt.Errorf("contentref: unknown classification %q", value.Classification)
		}
		switch value.DecisionMode {
		case DecisionNone, ConsequenceOnly:
		default:
			return fmt.Errorf("contentref: unknown decision mode %q", value.DecisionMode)
		}
		if !slices.IsSorted(value.Actions) {
			return fmt.Errorf("contentref: actions are not sorted for %q", value.RuleID)
		}
		for index, action := range value.Actions {
			if index > 0 && action == value.Actions[index-1] {
				return fmt.Errorf("contentref: duplicate action for %q", value.RuleID)
			}
			switch action {
			case BlockPhysicalGC, EraseClaimIdentity, RebuildProjection:
			default:
				return fmt.Errorf("contentref: unknown action %q", action)
			}
		}
		tuple := value.ReferrerKind + "\x00" + value.ReferrerField
		if _, exists := tuples[tuple]; exists {
			return fmt.Errorf("contentref: duplicate referrer tuple %q", tuple)
		}
		tuples[tuple] = struct{}{}
	}
	return nil
}

func validIdentifier(value string) bool {
	if value == "" || value[0] < 'a' || value[0] > 'z' {
		return false
	}
	for _, character := range value[1:] {
		if (character < 'a' || character > 'z') && (character < '0' || character > '9') && character != '_' {
			return false
		}
	}
	return !strings.Contains(value, "__")
}
