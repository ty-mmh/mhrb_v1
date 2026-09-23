package memory

import (
	"bytes"
	"errors"
	"reflect"
	"slices"
	"strings"
	"testing"

	"mahoroba.local/mahoroba/internal/canonical"
)

func TestPolicyV2CanonicalContractAndStrictParser(t *testing.T) {
	policy := DefaultPolicyV2()
	encoded, err := policy.CanonicalJSON()
	if err != nil {
		t.Fatalf("CanonicalJSON: %v", err)
	}
	text := string(encoded.Bytes())
	for _, fragment := range []string{
		`"version":"memory-policy-v2"`,
		`"definition_schema":"mahoroba-memory-policy-v0"`,
		`"mandatory_event_types":["user_message"]`,
		`"temporal":100000`,
		`"normalization_version":"memory-normalization-v1"`,
		`"rendering_version":"memory-rendering-v1"`,
	} {
		if !strings.Contains(text, fragment) {
			t.Errorf("canonical policy missing %s\n%s", fragment, text)
		}
	}
	expected, err := canonical.CanonicalizeRFC8785([]byte(`{
		"version":"memory-policy-v2","definition_schema":"mahoroba-memory-policy-v0",
		"mandatory_event_types":["user_message"],"eligible_evidence_event_types":["user_message","self_talk"],"forbidden_evidence_event_types":["resident_message","outbound_initiative"],
		"evidence":{"grade_weights":{"stated":1000000,"inferred":600000},"untrusted_multiplier":250000,"observed_enabled":false,"self_talk_forced_grade":"inferred"},
		"initial_scope":{"user_message":"resident_ui","self_talk":"admin_only","derived":"narrowest_source"},
		"maturation":{"sediment_distinct_events":2,"sediment_support_weight":1200000,"settled_distinct_events":3,"settled_support_weight":2000000,"settled_confidence":750000,"alignment_confidence":800000,"meta_min_stage":"sediment"},
		"salience":{"candidate":0,"selected":50000,"prompt_included":250000,"explicitly_referenced":500000,"half_life_days":30,"cap":1000000},
		"temporal":{"volatile_stale_days":30,"episodic_current_hours":24,"currentness":{"future":250000,"current":1000000,"past":600000,"stale_unknown":400000}},
		"recall":{"candidate_limit":64,"selected_limit":8,"prompt_included_limit":4,"byte_budget":8192,"weights":{"context":350000,"salience":200000,"confidence":200000,"state":150000,"temporal":100000}},
		"inheritance":{"weight_multiplier":500000,"max_depth":1,"max_per_target":8,"max_per_source_claim":2,"dedupe_underlying_event":true},
		"persona":{"min_claims":2,"max_claims":8,"max_changed_bytes":256,"max_changed_ratio":100000,"max_changed_lines":3,"max_total_bytes":8192,"activation_cooldown_hours":24},
		"normalization_version":"memory-normalization-v1","rendering_version":"memory-rendering-v1"
	}`))
	if err != nil {
		t.Fatalf("canonicalize explicit policy fixture: %v", err)
	}
	if !bytes.Equal(encoded.Bytes(), expected.Bytes()) {
		t.Fatalf("fixed policy differs from v0 fixture\n got: %s\nwant: %s", encoded.Bytes(), expected.Bytes())
	}
	parsed, persisted, err := ParsePolicy(encoded.Bytes())
	if err != nil {
		t.Fatalf("ParsePolicy: %v", err)
	}
	if parsed.Recall.Weights.Temporal != 100_000 || parsed.Recall.Weights.Context != 350_000 {
		t.Fatalf("unexpected recall weights: %+v", parsed.Recall.Weights)
	}
	if !bytes.Equal(persisted.Bytes(), encoded.Bytes()) {
		t.Fatal("parser did not preserve exact canonical policy bytes")
	}
}

func TestPolicyV3AddsOnlyMandatorySelfTalk(t *testing.T) {
	v2 := DefaultPolicyV2()
	v3 := DefaultPolicyV3()
	if v3.Version != PolicyVersionV3 {
		t.Fatalf("version = %q, want %q", v3.Version, PolicyVersionV3)
	}
	if got, want := v3.MandatoryEventTypes, []EventType{EventUserMessage, EventSelfTalk}; !slices.Equal(got, want) {
		t.Fatalf("mandatory event types = %v, want %v", got, want)
	}
	v2.Version = PolicyVersionV3
	v2.MandatoryEventTypes = []EventType{EventUserMessage, EventSelfTalk}
	if !reflect.DeepEqual(v3, v2) {
		t.Fatal("memory-policy-v3 changed a v0 rule other than mandatory event types")
	}
	encoded, err := v3.CanonicalJSON()
	if err != nil {
		t.Fatal(err)
	}
	parsed, persisted, err := ParsePolicy(encoded.Bytes())
	if err != nil {
		t.Fatal(err)
	}
	if err := parsed.RequireEnabled(); err != nil {
		t.Fatalf("RequireEnabled(v3): %v", err)
	}
	if err := parsed.RequireV2(); !errors.Is(err, ErrUnsupportedPolicy) {
		t.Fatalf("RequireV2(v3) = %v, want exact-version rejection", err)
	}
	if !bytes.Equal(encoded.Bytes(), persisted.Bytes()) {
		t.Fatal("v3 parser did not preserve exact canonical bytes")
	}
	changed := bytes.Replace(encoded.Bytes(), []byte(`"mandatory_event_types":["user_message","self_talk"]`), []byte(`"mandatory_event_types":["self_talk"]`), 1)
	if _, _, err := ParsePolicy(changed); !errors.Is(err, ErrInvalidPolicy) {
		t.Fatalf("changed v3 mandatory set error = %v, want ErrInvalidPolicy", err)
	}
}

func TestCOV6PolicyV4ChangesOnlyRenderingVersion(t *testing.T) {
	v3 := DefaultPolicyV3()
	v4 := DefaultPolicyV4()
	if v4.Version != PolicyVersionV4 || v4.RenderingVersion != RenderingVersionV2 {
		t.Fatalf("v4 identity = %q / %q", v4.Version, v4.RenderingVersion)
	}
	v3.Version = PolicyVersionV4
	v3.RenderingVersion = RenderingVersionV2
	if !reflect.DeepEqual(v4, v3) {
		t.Fatal("memory-policy-v4 changed a V3 field other than rendering version")
	}
	encoded, err := v4.CanonicalJSON()
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(encoded.String(), `"version":"memory-policy-v4"`) ||
		!strings.Contains(encoded.String(), `"rendering_version":"memory-rendering-v2"`) {
		t.Fatalf("v4 canonical contract = %s", encoded.String())
	}
	parsed, persisted, err := ParsePolicy(encoded.Bytes())
	if err != nil {
		t.Fatal(err)
	}
	if err := parsed.RequireEnabled(); err != nil {
		t.Fatalf("RequireEnabled(v4): %v", err)
	}
	if !bytes.Equal(encoded.Bytes(), persisted.Bytes()) {
		t.Fatal("v4 parser did not preserve exact canonical bytes")
	}
	changed := bytes.Replace(encoded.Bytes(), []byte(`"rendering_version":"memory-rendering-v2"`),
		[]byte(`"rendering_version":"memory-rendering-v1"`), 1)
	if _, _, err := ParsePolicy(changed); !errors.Is(err, ErrInvalidPolicy) {
		t.Fatalf("changed v4 renderer error = %v, want ErrInvalidPolicy", err)
	}
}

func TestPolicyParsersFailClosed(t *testing.T) {
	v1, err := DefaultPolicyV1().CanonicalJSON()
	if err != nil {
		t.Fatalf("v1 CanonicalJSON: %v", err)
	}
	if _, _, err := ParsePolicy(v1.Bytes()); err != nil {
		t.Fatalf("ParsePolicy(v1): %v", err)
	}
	if err := DefaultPolicyV1().RequireV2(); !errors.Is(err, ErrFeatureDisabled) {
		t.Fatalf("RequireV2(v1) error = %v, want ErrFeatureDisabled", err)
	}

	v2, err := DefaultPolicyV2().CanonicalJSON()
	if err != nil {
		t.Fatalf("v2 CanonicalJSON: %v", err)
	}
	changed := bytes.Replace(v2.Bytes(), []byte(`"temporal":100000`), []byte(`"temporal":100001`), 1)
	if bytes.Equal(changed, v2.Bytes()) {
		t.Fatal("test did not locate temporal weight")
	}
	if _, _, err := ParsePolicy(changed); !errors.Is(err, ErrInvalidPolicy) {
		t.Fatalf("changed fixed value error = %v, want ErrInvalidPolicy", err)
	}

	withUnknown, err := canonical.CanonicalizeRFC8785([]byte(`{"mandatory_event_types":[],"memory_recall_enabled":false,"unknown":0,"version":"memory-policy-v1"}`))
	if err != nil {
		t.Fatalf("canonicalize unknown policy: %v", err)
	}
	if _, _, err := ParsePolicy(withUnknown.Bytes()); !errors.Is(err, ErrInvalidPolicy) {
		t.Fatalf("unknown field error = %v, want ErrInvalidPolicy", err)
	}
	if _, _, err := ParsePolicy([]byte("{\n\"mandatory_event_types\":[],\"memory_recall_enabled\":false,\"version\":\"memory-policy-v1\"\n}")); !errors.Is(err, ErrInvalidPolicy) {
		t.Fatalf("non-JCS policy error = %v, want ErrInvalidPolicy", err)
	}
	if _, _, err := ParsePolicy([]byte(`{"version":"memory-policy-v9"}`)); !errors.Is(err, ErrUnsupportedPolicy) {
		t.Fatalf("unknown version error = %v, want ErrUnsupportedPolicy", err)
	}

	mutated := DefaultPolicyV2()
	mutated.Persona.MaximumChangedBytes++
	if _, err := mutated.CanonicalJSON(); !errors.Is(err, ErrInvalidPolicy) {
		t.Fatalf("mutated policy error = %v, want ErrInvalidPolicy", err)
	}
}

func TestTypedReasonAndRelationEnumsRejectUnknownValues(t *testing.T) {
	valid := []interface{ Validate() error }{
		RelationAbstracts, RelationSplitFrom, RelationSupersedes, RelationContradicts,
		EvidenceReasonSourceStated, EvidenceReasonSourceInferred,
		EvidenceReasonInheritedAbstraction, EvidenceReasonInheritedSplit,
		StageReasonInitialExtraction, StageReasonMaturationThreshold, StageReasonExternalAlignment,
		AutomaticReasonExplicitCorrection, AutomaticReasonExplicitSupersession, AutomaticReasonStructuralQuarantine,
		HumanReasonInvalidation, HumanReasonSupersession, HumanReasonQuarantine, HumanReasonReactivation,
	}
	for _, value := range valid {
		if err := value.Validate(); err != nil {
			t.Errorf("valid enum rejected: %v", err)
		}
	}
	invalid := []interface{ Validate() error }{
		RelationType("contains"), EvidenceReason("source_observed"), StageReason("manual"),
		AutomaticDecisionReason("typo"), HumanDecisionReason("admin_override"),
	}
	for _, value := range invalid {
		if err := value.Validate(); err == nil {
			t.Error("unknown enum was accepted")
		}
	}
}
