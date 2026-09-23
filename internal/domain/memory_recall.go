package domain

import (
	"fmt"

	"mahoroba.local/mahoroba/internal/canonical"
	"mahoroba.local/mahoroba/internal/memory"
)

type MemoryRecallFallbackReason string

const (
	MemoryRecallProjectionUnavailable       MemoryRecallFallbackReason = "projection_unavailable"
	MemoryRecallProjectionStale             MemoryRecallFallbackReason = "projection_stale"
	MemoryRecallProjectionProvenanceUnknown MemoryRecallFallbackReason = "projection_provenance_unknown"
)

func (reason MemoryRecallFallbackReason) Validate() error {
	switch reason {
	case MemoryRecallProjectionUnavailable, MemoryRecallProjectionStale,
		MemoryRecallProjectionProvenanceUnknown:
		return nil
	default:
		return fmt.Errorf("domain: invalid memory Recall fallback reason %q", reason)
	}
}

// RecallUsage is one actual decision emitted by the pure Recall planner. A
// selected claim may have candidate and selected rows, plus prompt_included
// only when its rendered content is a durable generation input.
type RecallUsage struct {
	ID              canonical.ID
	ClaimID         canonical.ID
	Type            memory.UsageType
	Ordinal         canonical.Ordinal
	ExclusionReason memory.RecallExclusionReason
}

// DialogueRecall is the immutable Recall provenance inserted alongside the
// dialogue generation envelope. QueryConditions and ContextConstraints use
// fixed-point logical integers and never contain Projection float values.
type DialogueRecall struct {
	RunID                  canonical.ID
	ResidentID             canonical.ID
	QueryContentID         canonical.ID
	PipelineVersionID      canonical.ID
	MemoryPolicyRevisionID canonical.ID
	AsOf                   canonical.Instant
	AsOfTZ                 canonical.Timezone
	QueryConditions        canonical.CanonicalJSON
	ContextConstraints     canonical.CanonicalJSON
	Usages                 []RecallUsage
}

func (recall DialogueRecall) Validate() error {
	return recall.ValidateForContext(DialogueContextPolicyVersionV1)
}

// ValidateForContext binds exclusion-reason vocabulary to the immutable
// context-policy version that produced the Recall graph. Validate remains the
// legacy context-v1 entry point for persisted-history compatibility.
func (recall DialogueRecall) ValidateForContext(contextPolicyVersion string) error {
	if contextPolicyVersion != DialogueContextPolicyVersionV1 &&
		contextPolicyVersion != DialogueContextPolicyVersionV2 &&
		contextPolicyVersion != DialogueContextPolicyVersionV3 &&
		contextPolicyVersion != DialogueContextPolicyVersionV4 {
		return fmt.Errorf("domain: unsupported dialogue context policy version %q", contextPolicyVersion)
	}
	for _, id := range []canonical.ID{
		recall.RunID, recall.ResidentID, recall.QueryContentID,
		recall.PipelineVersionID, recall.MemoryPolicyRevisionID,
	} {
		if err := id.Validate(); err != nil {
			return err
		}
	}
	if recall.AsOfTZ == "" || recall.QueryConditions.IsZero() || recall.ContextConstraints.IsZero() {
		return fmt.Errorf("domain: incomplete dialogue Recall provenance")
	}
	if len(recall.Usages) > MaxRecallUsages {
		return fmt.Errorf("domain: dialogue Recall has too many usages")
	}
	ids := make(map[canonical.ID]struct{}, len(recall.Usages))
	typeClaim := make(map[string]struct{}, len(recall.Usages))
	byType := map[memory.UsageType]map[canonical.ID]RecallUsage{
		memory.UsageCandidate:      {},
		memory.UsageSelected:       {},
		memory.UsagePromptIncluded: {},
	}
	nextOrdinal := map[memory.UsageType]int64{
		memory.UsageCandidate: 0, memory.UsageSelected: 0, memory.UsagePromptIncluded: 0,
	}
	for _, usage := range recall.Usages {
		if err := usage.ID.Validate(); err != nil {
			return err
		}
		if err := usage.ClaimID.Validate(); err != nil {
			return err
		}
		if err := usage.Type.Validate(); err != nil || usage.Type == memory.UsageExplicitlyReferenced {
			return fmt.Errorf("domain: invalid dialogue Recall usage type %q", usage.Type)
		}
		if err := usage.Ordinal.Validate(); err != nil {
			return err
		}
		if usage.Ordinal.Int64() != nextOrdinal[usage.Type] {
			return fmt.Errorf("domain: dialogue Recall usage ordinals must be contiguous by type")
		}
		nextOrdinal[usage.Type]++
		if _, duplicate := ids[usage.ID]; duplicate {
			return fmt.Errorf("domain: duplicate dialogue Recall usage ID")
		}
		ids[usage.ID] = struct{}{}
		key := string(usage.Type) + "\x00" + usage.ClaimID.String()
		if _, duplicate := typeClaim[key]; duplicate {
			return fmt.Errorf("domain: duplicate dialogue Recall claim usage")
		}
		typeClaim[key] = struct{}{}
		if usage.ExclusionReason != "" {
			var err error
			if contextPolicyVersion == DialogueContextPolicyVersionV2 ||
				contextPolicyVersion == DialogueContextPolicyVersionV3 ||
				contextPolicyVersion == DialogueContextPolicyVersionV4 {
				err = usage.ExclusionReason.ValidateV2()
			} else {
				err = usage.ExclusionReason.ValidateV1()
			}
			if err != nil {
				return err
			}
		}
		if usage.Type == memory.UsagePromptIncluded && usage.ExclusionReason != "" {
			return fmt.Errorf("domain: prompt-included Recall usage cannot have an exclusion reason")
		}
		if usage.Type == memory.UsageCandidate && usage.ExclusionReason != "" {
			return fmt.Errorf("domain: candidate Recall usage cannot have an exclusion reason")
		}
		byType[usage.Type][usage.ClaimID] = usage
	}
	if len(byType[memory.UsageCandidate]) > MaxRecallCandidates ||
		len(byType[memory.UsageSelected]) > MaxRecallSelected ||
		len(byType[memory.UsagePromptIncluded]) > MaxRecallPromptInputs {
		return fmt.Errorf("domain: dialogue Recall usage type exceeds its fixed limit")
	}
	for claimID, selected := range byType[memory.UsageSelected] {
		if _, exists := byType[memory.UsageCandidate][claimID]; !exists {
			return fmt.Errorf("domain: selected Recall usage lacks candidate usage")
		}
		_, included := byType[memory.UsagePromptIncluded][claimID]
		if included && selected.ExclusionReason != "" {
			return fmt.Errorf("domain: prompt-included selected Recall usage has an exclusion reason")
		}
		if !included && selected.ExclusionReason == "" {
			return fmt.Errorf("domain: excluded selected Recall usage lacks an exclusion reason")
		}
	}
	for claimID := range byType[memory.UsagePromptIncluded] {
		if _, exists := byType[memory.UsageSelected][claimID]; !exists {
			return fmt.Errorf("domain: prompt-included Recall usage lacks selected usage")
		}
	}
	return nil
}

type RecallDecisionParameters struct {
	CandidateLimit       canonical.Count     `json:"candidate_limit"`
	ContextCompatibility canonical.Ratio     `json:"context_compatibility"`
	ProjectionHead       canonical.CommitSeq `json:"projection_head"`
	ScoringVersion       string              `json:"scoring_version"`
}

type RecallContextParameters struct {
	ByteBudget          canonical.ByteSize `json:"byte_budget"`
	PromptIncludedLimit canonical.Count    `json:"prompt_included_limit"`
	SelectedLimit       canonical.Count    `json:"selected_limit"`
	ViewScope           memory.ViewScope   `json:"view_scope"`
}

func NewRecallDecisionJSON(
	head canonical.CommitSeq,
	policy memory.Policy,
	contextCompatibility canonical.Ratio,
) (canonical.CanonicalJSON, canonical.CanonicalJSON, error) {
	if err := head.Validate(); err != nil {
		return canonical.CanonicalJSON{}, canonical.CanonicalJSON{}, err
	}
	if err := policy.RequireEnabled(); err != nil {
		return canonical.CanonicalJSON{}, canonical.CanonicalJSON{}, err
	}
	if err := contextCompatibility.Validate(); err != nil {
		return canonical.CanonicalJSON{}, canonical.CanonicalJSON{}, err
	}
	query, err := canonical.MarshalCanonical(RecallDecisionParameters{
		CandidateLimit:       canonical.Count(policy.Recall.CandidateLimit),
		ContextCompatibility: contextCompatibility,
		ProjectionHead:       head,
		ScoringVersion:       MemoryRecallPipelineVersion,
	})
	if err != nil {
		return canonical.CanonicalJSON{}, canonical.CanonicalJSON{}, err
	}
	constraints, err := canonical.MarshalCanonical(RecallContextParameters{
		ByteBudget:          canonical.ByteSize(policy.Recall.ByteBudget),
		PromptIncludedLimit: canonical.Count(policy.Recall.PromptIncludedLimit),
		SelectedLimit:       canonical.Count(policy.Recall.SelectedLimit),
		ViewScope:           memory.ScopeResidentUI,
	})
	if err != nil {
		return canonical.CanonicalJSON{}, canonical.CanonicalJSON{}, err
	}
	return query, constraints, nil
}
