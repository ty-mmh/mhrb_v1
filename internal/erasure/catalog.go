package erasure

import (
	"fmt"
	"slices"
	"strings"

	"mahoroba.local/mahoroba/internal/contentref"
)

type impactDescriptor struct {
	RuleID         string
	ReferrerKind   string
	ReferrerField  string
	Classification string
	DecisionMode   string
	Actions        []string
}

// Synthetic impact rules are plan-only facts. They intentionally do not
// widen contentref's authoritative direct-field registry.
var syntheticImpactDescriptors = []impactDescriptor{
	{RuleID: "claim_provenance_break_v1", ReferrerKind: "claim", ReferrerField: "qualifying_provenance", Classification: "needs_review", DecisionMode: "consequence_only"},
	{RuleID: "explicit_lineage_exact_bytes_v1", ReferrerKind: "content_object", ReferrerField: "derived_bytes", Classification: "must_erase", DecisionMode: "none", Actions: []string{"erase_derived_content"}},
	{RuleID: "explicit_lineage_nonexact_bytes_v1", ReferrerKind: "content_object", ReferrerField: "derived_bytes", Classification: "needs_review", DecisionMode: "content_candidate"},
	{RuleID: "projection_derived_state_v1", ReferrerKind: "projection", ReferrerField: "body", Classification: "needs_rebuild", DecisionMode: "none", Actions: []string{"rebuild_projection"}},
	{RuleID: "resident_lifecycle_erase_v1", ReferrerKind: "resident", ReferrerField: "lifecycle", Classification: "needs_rebuild", DecisionMode: "none", Actions: []string{"rebuild_projection", "transition_resident_erased"}},
	{RuleID: "same_bytes_without_lineage_v1", ReferrerKind: "content_object", ReferrerField: "shared_bytes", Classification: "needs_review", DecisionMode: "content_candidate", Actions: []string{"block_physical_gc"}},
}

var logicalImpactDescriptors = []impactDescriptor{
	{RuleID: "claim_evidence_event_provenance_v1", ReferrerKind: "claim_evidence", ReferrerField: "event_id", Classification: "safe", DecisionMode: "none"},
	{RuleID: "claim_evidence_source_chain_v1", ReferrerKind: "claim_evidence", ReferrerField: "source_evidence_id", Classification: "safe", DecisionMode: "none"},
	{RuleID: "claim_relation_from_v1", ReferrerKind: "claim_relation", ReferrerField: "from_claim_id", Classification: "needs_review", DecisionMode: "consequence_only"},
	{RuleID: "claim_relation_to_v1", ReferrerKind: "claim_relation", ReferrerField: "to_claim_id", Classification: "needs_review", DecisionMode: "consequence_only"},
	{RuleID: "claim_stage_dependency_v1", ReferrerKind: "claim_stage_transition_dependency", ReferrerField: "dependency_claim_id", Classification: "needs_review", DecisionMode: "consequence_only"},
	{RuleID: "claim_status_trigger_event_v1", ReferrerKind: "claim_status_transition", ReferrerField: "trigger_event_id", Classification: "needs_review", DecisionMode: "consequence_only"},
	{RuleID: "claim_status_trigger_evidence_v1", ReferrerKind: "claim_status_transition", ReferrerField: "trigger_evidence_id", Classification: "needs_review", DecisionMode: "consequence_only"},
	{RuleID: "claim_status_trigger_finding_v1", ReferrerKind: "claim_status_transition", ReferrerField: "trigger_integrity_finding_id", Classification: "needs_review", DecisionMode: "consequence_only"},
	{RuleID: "claim_status_trigger_relation_v1", ReferrerKind: "claim_status_transition", ReferrerField: "trigger_claim_relation_id", Classification: "needs_review", DecisionMode: "consequence_only"},
	{RuleID: "claim_usage_claim_v1", ReferrerKind: "claim_usage", ReferrerField: "claim_id", Classification: "safe", DecisionMode: "none"},
	{RuleID: "claim_validity_evidence_event_v1", ReferrerKind: "claim_validity_assertion", ReferrerField: "evidence_event_id", Classification: "needs_review", DecisionMode: "consequence_only"},
	{RuleID: "generation_recall_v1", ReferrerKind: "generation_run", ReferrerField: "recall_run_id", Classification: "safe", DecisionMode: "none"},
}

func IsLogicalImpactRule(ruleID string) bool {
	return slices.ContainsFunc(logicalImpactDescriptors, func(value impactDescriptor) bool { return value.RuleID == ruleID })
}

// ImpactForLogicalReference materializes the one closed wire policy shared by
// planning and apply-time revalidation. Only the two evidence-chain rules may
// conditionally rise from safe/none to needs_review/consequence_only.
func ImpactForLogicalReference(reference LogicalReferenceSnapshot) (Impact, error) {
	descriptor, exists := impactCatalog()[reference.RuleID]
	if !exists || !IsLogicalImpactRule(reference.RuleID) {
		return Impact{}, fmt.Errorf("%w: unknown logical impact rule %q", ErrInvalidPlan, reference.RuleID)
	}
	classification, decisionMode := descriptor.Classification, descriptor.DecisionMode
	if reference.BreaksClaim {
		if reference.RuleID != "claim_evidence_event_provenance_v1" && reference.RuleID != "claim_evidence_source_chain_v1" {
			return Impact{}, fmt.Errorf("%w: unsupported conditional logical impact %q", ErrInvalidPlan, reference.RuleID)
		}
		classification, decisionMode = "needs_review", "consequence_only"
	}
	impact := Impact{RuleID: reference.RuleID, ReferrerKind: reference.ReferrerKind, ReferrerID: reference.ReferrerID,
		ReferrerField: reference.ReferrerField, Classification: classification, Actions: slices.Clone(descriptor.Actions), DecisionMode: decisionMode}
	var err error
	impact.ImpactID, err = ImpactID(impact)
	return impact, err
}

func impactCatalog() map[string]impactDescriptor {
	values := make(map[string]impactDescriptor, len(contentref.DirectDescriptors())+len(syntheticImpactDescriptors)+len(logicalImpactDescriptors))
	for _, direct := range contentref.DirectDescriptors() {
		actions := make([]string, len(direct.Actions))
		for index, action := range direct.Actions {
			actions[index] = string(action)
		}
		values[direct.RuleID] = impactDescriptor{
			RuleID: direct.RuleID, ReferrerKind: direct.ReferrerKind, ReferrerField: direct.ReferrerField,
			Classification: string(direct.Classification), DecisionMode: string(direct.DecisionMode), Actions: actions,
		}
	}
	for _, value := range append(slices.Clone(syntheticImpactDescriptors), logicalImpactDescriptors...) {
		if _, exists := values[value.RuleID]; exists {
			panic("erasure: duplicate impact rule " + value.RuleID)
		}
		values[value.RuleID] = value
	}
	return values
}

func validateImpactAgainstCatalog(value Impact) error {
	descriptor, ok := impactCatalog()[value.RuleID]
	if !ok {
		return fmt.Errorf("%w: unknown impact rule %q", ErrInvalidPlan, value.RuleID)
	}
	if value.ReferrerKind != descriptor.ReferrerKind || value.ReferrerField != descriptor.ReferrerField {
		return fmt.Errorf("%w: impact tuple differs for %s", ErrInvalidPlan, value.RuleID)
	}
	if value.Classification != descriptor.Classification || value.DecisionMode != descriptor.DecisionMode {
		conditionalBreak := (value.RuleID == "claim_evidence_event_provenance_v1" || value.RuleID == "claim_evidence_source_chain_v1") &&
			value.Classification == "needs_review" && value.DecisionMode == "consequence_only"
		if !conditionalBreak {
			return fmt.Errorf("%w: impact policy differs for %s", ErrInvalidPlan, value.RuleID)
		}
	}
	want := descriptor.Actions
	// claim_provenance_break actions are conditional but closed.
	if value.RuleID == "claim_provenance_break_v1" {
		for _, action := range value.Actions {
			if action != "quarantine_claim" && action != "record_integrity_finding" {
				return fmt.Errorf("%w: invalid provenance action %q", ErrInvalidPlan, action)
			}
		}
		return nil
	}
	if value.RuleID == "resident_lifecycle_erase_v1" {
		withSelection := []string{"clear_runtime_selection", "rebuild_projection", "transition_resident_erased"}
		if slices.Equal(value.Actions, want) || slices.Equal(value.Actions, withSelection) {
			return nil
		}
		return fmt.Errorf("%w: invalid resident lifecycle actions", ErrInvalidPlan)
	}
	if !slices.Equal(value.Actions, want) {
		return fmt.Errorf("%w: impact actions differ for %s", ErrInvalidPlan, value.RuleID)
	}
	return nil
}

type blockerContract struct {
	Kind, Field string
	Global      bool
	Actions     []string
}

var blockerCatalog = map[string]blockerContract{
	"actor_not_owner_human":          {Kind: "resident", Field: "owner_principal_id"},
	"blob_hash_mismatch":             {Kind: "content", Actions: []string{"repair_static_design"}},
	"blob_missing":                   {Kind: "content", Actions: []string{"repair_static_design"}},
	"cross_resident_reference":       {Kind: "lineage", Field: "cross_resident_reference", Actions: []string{"repair_static_design"}},
	"invalid_run_product":            {Kind: "generation_run", Field: "product_set", Actions: []string{"repair_static_design"}},
	"lineage_cycle":                  {Kind: "lineage", Field: "cycle", Actions: []string{"repair_static_design"}},
	"mandatory_work_present":         {Kind: "resident", Field: "mandatory_work"},
	"partial_retry_state":            {Kind: "plan", Field: "retry_state", Global: true, Actions: []string{"repair_static_design"}},
	"plan_state_changed":             {Kind: "plan", Field: "canonical_state", Global: true, Actions: []string{"replan_erasure"}},
	"planned_effect_mismatch":        {Kind: "plan", Field: "planned_effects", Global: true, Actions: []string{"repair_static_design"}},
	"preexisting_integrity_break":    {Kind: "claim", Field: "provenance", Actions: []string{"run_integrity_scan"}},
	"resident_only_in_content_scope": {Kind: "content", Field: "erasure_policy", Actions: []string{"replan_erasure"}},
	"review_incomplete":              {Kind: "plan", Field: "review_decisions", Global: true},
	"running_attempt":                {Kind: "generation_run", Field: "attempt", Actions: []string{"replan_erasure", "run_recovery_terminalize"}},
	"target_already_erased":          {Kind: "content", Field: "erasure_state", Actions: []string{"replan_erasure"}},
	"target_missing":                 {Kind: "content", Field: "existence", Actions: []string{"replan_erasure"}},
	"transaction_size_unsupported":   {Kind: "resident", Field: "effective_targets", Actions: []string{"repair_static_design"}},
	"unknown_lineage_source":         {Kind: "generation_input", Field: "source", Actions: []string{"repair_static_design"}},
}

func validateBlocker(value Blocker) error {
	contract, ok := blockerCatalog[value.Code]
	if !ok || value.TargetKind != contract.Kind || value.TargetField == nil {
		return fmt.Errorf("%w: invalid blocker tuple for %q", ErrInvalidPlan, value.Code)
	}
	if value.Code == "blob_hash_mismatch" || value.Code == "blob_missing" {
		if *value.TargetField != "sqlite_blob" && *value.TargetField != "filesystem_blob" {
			return fmt.Errorf("%w: invalid blob blocker field", ErrInvalidPlan)
		}
	} else if *value.TargetField != contract.Field {
		return fmt.Errorf("%w: invalid blocker field for %q", ErrInvalidPlan, value.Code)
	}
	if contract.Global != (value.TargetID == nil) {
		return fmt.Errorf("%w: invalid blocker identity/actions for %q", ErrInvalidPlan, value.Code)
	}
	if value.Code == "mandatory_work_present" {
		active := []string{"archive_resident", "replan_erasure", "run_recovery_terminalize"}
		inactive := []string{"replan_erasure", "run_recovery_terminalize"}
		if !slices.Equal(value.RequiredActionCodes, active) && !slices.Equal(value.RequiredActionCodes, inactive) {
			return fmt.Errorf("%w: invalid mandatory-work actions", ErrInvalidPlan)
		}
	} else if !slices.Equal(value.RequiredActionCodes, contract.Actions) {
		return fmt.Errorf("%w: invalid blocker actions for %q", ErrInvalidPlan, value.Code)
	}
	return nil
}

func compareNullable(left, right *string) int {
	if left == nil && right == nil {
		return 0
	}
	if left == nil {
		return -1
	}
	if right == nil {
		return 1
	}
	return strings.Compare(*left, *right)
}
