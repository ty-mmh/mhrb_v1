package erasure

const (
	PlanFormatVersion  = "mahoroba-erasure-plan-v1"
	ImpactRulesVersion = "erasure-impact-v1"
	ExternalCopyNotice = "runtime_unmanaged_copy_not_covered_by_future_erasure"

	ScopeContent        = "content"
	ScopeResident       = "resident"
	StateReviewRequired = "review_required"
	StateReady          = "ready"

	// MaxSingleTransactionMutations bounds the number of individually checked
	// Canonical row mutations in one Erasure Apply UoW. Every mutation uses a
	// fixed-arity statement, so this is independent of SQLite's host-parameter
	// limit and makes the no-chunking guarantee directly testable.
	MaxSingleTransactionMutations = 4096
)

// Every Plan field is required on the wire. Nullable fields use pointers;
// empty collections must be encoded as [] and never omitted.
type Plan struct {
	FormatVersion                     string                            `json:"format_version"`
	RulesVersion                      string                            `json:"rules_version"`
	PlanState                         string                            `json:"plan_state"`
	Scope                             string                            `json:"scope"`
	ResidentID                        string                            `json:"resident_id"`
	BaseHead                          BaseHead                          `json:"base_head"`
	ActorPrincipalID                  string                            `json:"actor_principal_id"`
	ReasonCode                        string                            `json:"reason_code"`
	IntegrityPipelineVersionID        string                            `json:"integrity_pipeline_version_id"`
	MemoryStatusPipelineVersionID     string                            `json:"memory_status_pipeline_version_id"`
	RequestedContentIDs               []string                          `json:"requested_content_ids"`
	EffectiveTargets                  []EffectiveTarget                 `json:"effective_targets"`
	Impacts                           []Impact                          `json:"impacts"`
	PlannedFindings                   []PlannedFinding                  `json:"planned_findings"`
	ExistingFindingDependencies       []ExistingFindingDependency       `json:"existing_finding_dependencies"`
	PlannedQuarantines                []PlannedQuarantine               `json:"planned_quarantines"`
	ClaimIdentityErasures             []ClaimIdentityErasure            `json:"claim_identity_erasures"`
	ExistingClaimIdentityDependencies []ExistingClaimIdentityDependency `json:"existing_claim_identity_dependencies"`
	ResidentTransition                *ResidentTransition               `json:"resident_transition"`
	RuntimeConfigEffect               RuntimeConfigEffect               `json:"runtime_config_effect"`
	Rebuilds                          []Rebuild                         `json:"rebuilds"`
	Blockers                          []Blocker                         `json:"blockers"`
	ExternalCopyNotice                string                            `json:"external_copy_notice"`
	Digest                            string                            `json:"digest"`
}

type BaseHead struct {
	CommitID  string `json:"commit_id"`
	CommitSeq string `json:"commit_seq"`
}

type EffectiveTarget struct {
	ContentID            string  `json:"content_id"`
	ContentClass         string  `json:"content_class"`
	ErasurePolicy        string  `json:"erasure_policy"`
	Commitment           string  `json:"commitment"`
	ErasureEventID       string  `json:"erasure_event_id"`
	SourceErasureEventID *string `json:"source_erasure_event_id"`
	LineageDepth         string  `json:"lineage_depth"`
}

type Impact struct {
	ImpactID           string   `json:"impact_id"`
	RuleID             string   `json:"rule_id"`
	ReferrerKind       string   `json:"referrer_kind"`
	ReferrerID         string   `json:"referrer_id"`
	ReferrerField      string   `json:"referrer_field"`
	Classification     string   `json:"classification"`
	Actions            []string `json:"actions"`
	DecisionMode       string   `json:"decision_mode"`
	CandidateContentID *string  `json:"candidate_content_id"`
	Decision           *string  `json:"decision"`
}

type PlannedFinding struct {
	IntegrityFindingID          string  `json:"integrity_finding_id"`
	FindingKind                 string  `json:"finding_kind"`
	RuleCode                    string  `json:"rule_code"`
	TargetKind                  string  `json:"target_kind"`
	TargetID                    string  `json:"target_id"`
	TargetField                 string  `json:"target_field"`
	ResidentID                  string  `json:"resident_id"`
	ClaimID                     *string `json:"claim_id"`
	SourceContentErasureEventID *string `json:"source_content_erasure_event_id"`
	PipelineVersionID           string  `json:"pipeline_version_id"`
	FindingFingerprint          string  `json:"finding_fingerprint"`
	DetailsContentID            *string `json:"details_content_id"`
}

type ExistingFindingDependency struct {
	IntegrityFindingID          string  `json:"integrity_finding_id"`
	ClaimID                     string  `json:"claim_id"`
	FindingKind                 string  `json:"finding_kind"`
	RuleCode                    string  `json:"rule_code"`
	SourceContentErasureEventID *string `json:"source_content_erasure_event_id"`
	PipelineVersionID           string  `json:"pipeline_version_id"`
	FindingFingerprint          string  `json:"finding_fingerprint"`
}

type PlannedQuarantine struct {
	StatusTransitionID        string  `json:"status_transition_id"`
	ClaimID                   string  `json:"claim_id"`
	FromStatus                string  `json:"from_status"`
	ToStatus                  string  `json:"to_status"`
	DecisionKind              string  `json:"decision_kind"`
	ActorPrincipalID          *string `json:"actor_principal_id"`
	TriggerKind               string  `json:"trigger_kind"`
	TriggerEventID            *string `json:"trigger_event_id"`
	TriggerEvidenceID         *string `json:"trigger_evidence_id"`
	TriggerClaimRelationID    *string `json:"trigger_claim_relation_id"`
	TriggerIntegrityFindingID string  `json:"trigger_integrity_finding_id"`
	PipelineVersionID         string  `json:"pipeline_version_id"`
	MemoryPolicyRevisionID    *string `json:"memory_policy_revision_id"`
	GateMetrics               string  `json:"gate_metrics"`
	DecisionReasonCode        string  `json:"decision_reason_code"`
	DecisionReasonContentID   *string `json:"decision_reason_content_id"`
}

type ClaimIdentityErasure struct {
	ClaimStatementErasureEventID string `json:"claim_statement_erasure_event_id"`
	ClaimID                      string `json:"claim_id"`
	StatementContentID           string `json:"statement_content_id"`
	ContentErasureEventID        string `json:"content_erasure_event_id"`
}

type ExistingClaimIdentityDependency struct {
	ClaimStatementErasureEventID string `json:"claim_statement_erasure_event_id"`
	ClaimID                      string `json:"claim_id"`
	StatementContentID           string `json:"statement_content_id"`
	ContentErasureEventID        string `json:"content_erasure_event_id"`
	CanonicalCommitID            string `json:"canonical_commit_id"`
}

type ResidentTransition struct {
	TransitionID     string  `json:"transition_id"`
	ResidentID       string  `json:"resident_id"`
	FromStatus       string  `json:"from_status"`
	ToStatus         string  `json:"to_status"`
	ActorPrincipalID string  `json:"actor_principal_id"`
	ReasonCode       string  `json:"reason_code"`
	ReasonContentID  *string `json:"reason_content_id"`
}

type RuntimeConfigEffect struct {
	ExpectedActiveResidentID  *string `json:"expected_active_resident_id"`
	ResultingActiveResidentID *string `json:"resulting_active_resident_id"`
	ClearActiveResident       bool    `json:"clear_active_resident"`
}

type Rebuild struct {
	ResidentID        string `json:"resident_id"`
	ProjectionName    string `json:"projection_name"`
	ProjectionVersion string `json:"projection_version"`
	ReasonCode        string `json:"reason_code"`
}

type Blocker struct {
	Code                string   `json:"code"`
	TargetKind          string   `json:"target_kind"`
	TargetID            *string  `json:"target_id"`
	TargetField         *string  `json:"target_field"`
	RequiredActionCodes []string `json:"required_action_codes"`
}

type DecisionInput struct {
	ImpactID string `json:"impact_id"`
	Decision string `json:"decision"`
}

type ApplyResult struct {
	ExistingCommit             bool
	CanonicalErasureCommitID   string
	CanonicalErasureCommitSeq  string
	ProjectionTargetHeadID     string
	ProjectionTargetHeadSeq    string
	MandatoryWorkFollowupCount int
	ProjectionRebuilds         []Rebuild
}
