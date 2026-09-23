package domain

import (
	"context"

	"mahoroba.local/mahoroba/internal/canonical"
	"mahoroba.local/mahoroba/internal/memory"
)

// MemoryClaimFilter is the query-only Admin filter. A zero-valued typed field
// means "all"; Limit is normalized by the repository.
type MemoryClaimFilter struct {
	ResidentID canonical.ID       `json:"resident_id"`
	Stage      memory.ClaimStage  `json:"stage,omitempty"`
	Status     memory.ClaimStatus `json:"status,omitempty"`
	Scope      memory.ViewScope   `json:"scope,omitempty"`
	Limit      int                `json:"limit,omitempty"`
}

// MemoryClaimSummary is assembled from Canonical history rather than trusting
// a Projection. It is therefore suitable for local Admin inspection even when
// the rebuildable views are stale or absent.
type MemoryClaimSummary struct {
	ClaimID                         canonical.ID        `json:"claim_id"`
	ResidentID                      canonical.ID        `json:"resident_id"`
	SubjectPrincipalID              canonical.ID        `json:"subject_principal_id"`
	PerspectivePrincipalID          canonical.ID        `json:"perspective_principal_id"`
	Kind                            memory.ClaimKind    `json:"kind"`
	TemporalKind                    memory.TemporalKind `json:"temporal_kind"`
	Statement                       string              `json:"statement"`
	StatementErased                 bool                `json:"statement_erased"`
	Stage                           memory.ClaimStage   `json:"stage"`
	Status                          memory.ClaimStatus  `json:"status"`
	Scope                           memory.ViewScope    `json:"scope"`
	CreatedByRunID                  canonical.ID        `json:"created_by_run_id"`
	CreatedByPurpose                GenerationPurpose   `json:"created_by_purpose"`
	CreatedByPipelineVersionID      canonical.ID        `json:"created_by_pipeline_version_id"`
	CreatedByMemoryPolicyRevisionID canonical.ID        `json:"created_by_memory_policy_revision_id"`
	RecordedAt                      canonical.Instant   `json:"recorded_at"`
	RecordedTZ                      canonical.Timezone  `json:"recorded_tz"`
}

type MemoryEvidenceProvenance struct {
	EvidenceID             canonical.ID              `json:"evidence_id"`
	SourceEventID          canonical.ID              `json:"source_event_id"`
	SourceEventType        memory.EventType          `json:"source_event_type"`
	SourceActorPrincipalID canonical.ID              `json:"source_actor_principal_id"`
	SourceEventContent     string                    `json:"source_event_content"`
	SourceContentErased    bool                      `json:"source_content_erased"`
	Polarity               memory.EvidencePolarity   `json:"polarity"`
	Grade                  memory.EvidenceGrade      `json:"grade"`
	Trust                  memory.TrustLevel         `json:"trust"`
	Weight                 int64                     `json:"weight"`
	Derivation             memory.EvidenceDerivation `json:"derivation"`
	SourceEvidenceID       *canonical.ID             `json:"source_evidence_id,omitempty"`
	MemoryPolicyRevisionID canonical.ID              `json:"memory_policy_revision_id"`
	CreatedByRunID         canonical.ID              `json:"created_by_run_id"`
	CreatedByPurpose       GenerationPurpose         `json:"created_by_purpose"`
	PipelineVersionID      canonical.ID              `json:"pipeline_version_id"`
	ReasonCode             string                    `json:"reason_code"`
	RecordedAt             canonical.Instant         `json:"recorded_at"`
	RecordedTZ             canonical.Timezone        `json:"recorded_tz"`
}

type MemoryStageTransitionView struct {
	TransitionID           canonical.ID       `json:"transition_id"`
	FromStage              *memory.ClaimStage `json:"from_stage,omitempty"`
	ToStage                memory.ClaimStage  `json:"to_stage"`
	GateMetrics            string             `json:"gate_metrics"`
	PipelineVersionID      canonical.ID       `json:"pipeline_version_id"`
	MemoryPolicyRevisionID canonical.ID       `json:"memory_policy_revision_id"`
	GenerationRunID        *canonical.ID      `json:"generation_run_id,omitempty"`
	ReasonCode             string             `json:"reason_code"`
	RecordedAt             canonical.Instant  `json:"recorded_at"`
	RecordedTZ             canonical.Timezone `json:"recorded_tz"`
}

type MemoryStatusTransitionView struct {
	TransitionID           canonical.ID       `json:"transition_id"`
	FromStatus             memory.ClaimStatus `json:"from_status"`
	ToStatus               memory.ClaimStatus `json:"to_status"`
	DecisionKind           string             `json:"decision_kind"`
	ActorPrincipalID       *canonical.ID      `json:"actor_principal_id,omitempty"`
	TriggerKind            string             `json:"trigger_kind,omitempty"`
	TriggerID              *canonical.ID      `json:"trigger_id,omitempty"`
	PipelineVersionID      *canonical.ID      `json:"pipeline_version_id,omitempty"`
	MemoryPolicyRevisionID *canonical.ID      `json:"memory_policy_revision_id,omitempty"`
	GateMetrics            string             `json:"gate_metrics,omitempty"`
	ReasonCode             string             `json:"reason_code"`
	RecordedAt             canonical.Instant  `json:"recorded_at"`
	RecordedTZ             canonical.Timezone `json:"recorded_tz"`
}

type MemoryRelationView struct {
	RelationID      canonical.ID        `json:"relation_id"`
	FromClaimID     canonical.ID        `json:"from_claim_id"`
	ToClaimID       canonical.ID        `json:"to_claim_id"`
	RelationType    memory.RelationType `json:"relation_type"`
	GenerationRunID *canonical.ID       `json:"generation_run_id,omitempty"`
	ReasonCode      string              `json:"reason_code"`
	RecordedAt      canonical.Instant   `json:"recorded_at"`
	RecordedTZ      canonical.Timezone  `json:"recorded_tz"`
}

type MemoryUsageView struct {
	UsageID                canonical.ID       `json:"usage_id"`
	RecallRunID            *canonical.ID      `json:"recall_run_id,omitempty"`
	GenerationRunID        *canonical.ID      `json:"generation_run_id,omitempty"`
	UsageType              memory.UsageType   `json:"usage_type"`
	Ordinal                *int64             `json:"ordinal,omitempty"`
	MemoryPolicyRevisionID canonical.ID       `json:"memory_policy_revision_id"`
	ExclusionReason        string             `json:"exclusion_reason,omitempty"`
	RecordedAt             canonical.Instant  `json:"recorded_at"`
	RecordedTZ             canonical.Timezone `json:"recorded_tz"`
}

type MemoryClaimProvenance struct {
	Claim             MemoryClaimSummary           `json:"claim"`
	Evidence          []MemoryEvidenceProvenance   `json:"evidence"`
	StageTransitions  []MemoryStageTransitionView  `json:"stage_transitions"`
	StatusTransitions []MemoryStatusTransitionView `json:"status_transitions"`
	Relations         []MemoryRelationView         `json:"relations"`
	Usages            []MemoryUsageView            `json:"usages"`
}

type MemoryPersonaRevisionView struct {
	RevisionID              canonical.ID       `json:"revision_id"`
	ParentRevisionID        *canonical.ID      `json:"parent_revision_id,omitempty"`
	Content                 string             `json:"content"`
	ContentErased           bool               `json:"content_erased"`
	CreatedByRunID          *canonical.ID      `json:"created_by_run_id,omitempty"`
	PipelineVersionID       *canonical.ID      `json:"pipeline_version_id,omitempty"`
	MemoryPolicyRevisionID  *canonical.ID      `json:"memory_policy_revision_id,omitempty"`
	Active                  bool               `json:"active"`
	LatestActivationID      *canonical.ID      `json:"latest_activation_id,omitempty"`
	LatestActivationActorID *canonical.ID      `json:"latest_activation_actor_id,omitempty"`
	LatestActivationReason  string             `json:"latest_activation_reason,omitempty"`
	LatestActivatedAt       *canonical.Instant `json:"latest_activated_at,omitempty"`
	RecordedAt              canonical.Instant  `json:"recorded_at"`
	RecordedTZ              canonical.Timezone `json:"recorded_tz"`
}

// MemoryAdminRepository is intentionally read-only. Both the running
// Application and OpenInspection can expose it without granting Canonical
// mutation authority.
type MemoryAdminRepository interface {
	ListMemoryClaims(context.Context, MemoryClaimFilter) ([]MemoryClaimSummary, error)
	MemoryClaimProvenance(context.Context, canonical.ID, canonical.ID) (MemoryClaimProvenance, error)
	ListMemoryPersonaRevisions(context.Context, canonical.ID) ([]MemoryPersonaRevisionView, error)
}
