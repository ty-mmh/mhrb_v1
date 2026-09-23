package autonomy

import (
	"fmt"
	"sort"
	"time"

	"mahoroba.local/mahoroba/internal/canonical"
)

type RetentionImpactReason string

const (
	ImpactClaimProvenanceReference           RetentionImpactReason = "claim_provenance_reference"
	ImpactGenerationInputReference           RetentionImpactReason = "generation_input_reference"
	ImpactGenerationOutputReference          RetentionImpactReason = "generation_output_reference"
	ImpactContentPolicyRequiresResidentErase RetentionImpactReason = "content_policy_requires_resident_erasure"
)

type ReferenceImpact struct {
	EvidenceReferences         int64 `json:"evidence_references"`
	ClaimReferences            int64 `json:"claim_references"`
	GenerationInputReferences  int64 `json:"generation_input_references"`
	GenerationOutputReferences int64 `json:"generation_output_references"`
}

type RetentionSource struct {
	ResidentID     canonical.ID
	EventID        canonical.ID
	ContentID      canonical.ID
	EventType      EventType
	RecordedAt     canonical.Instant
	ContentPresent bool
	ErasurePolicy  string
	References     ReferenceImpact
}

type RetentionCapture struct {
	CapturedHead canonical.Head
	Sources      []RetentionSource
}

type RetentionCandidate struct {
	ResidentID      canonical.ID            `json:"resident_id"`
	EventID         canonical.ID            `json:"event_id"`
	ContentID       canonical.ID            `json:"content_id"`
	RecordedAt      canonical.Instant       `json:"recorded_at"`
	Age             time.Duration           `json:"-"`
	AgeMicroseconds int64                   `json:"age_microseconds"`
	References      ReferenceImpact         `json:"references"`
	BlockingReasons []RetentionImpactReason `json:"blocking_reasons"`
}

func Candidates(policy RetentionPolicy, wallNow time.Time, capture RetentionCapture) ([]RetentionCandidate, error) {
	if policy.Mode == RetentionDisabled {
		return nil, nil
	}
	if policy.Mode != RetentionCandidateAfter || policy.Duration <= 0 || policy.ScanInterval <= 0 ||
		policy.Eligibility != RetentionPresentSelfTalk {
		return nil, fmt.Errorf("%w: invalid retention policy", ErrInvalidRetention)
	}
	if wallNow.IsZero() {
		return nil, fmt.Errorf("%w: wall time is required", ErrInvalidRetention)
	}
	if err := capture.CapturedHead.Validate(); err != nil {
		return nil, fmt.Errorf("%w: captured head: %v", ErrInvalidRetention, err)
	}
	result := make([]RetentionCandidate, 0, len(capture.Sources))
	seen := make(map[canonical.ID]struct{}, len(capture.Sources))
	for _, source := range capture.Sources {
		for _, id := range []canonical.ID{source.ResidentID, source.EventID, source.ContentID} {
			if err := id.Validate(); err != nil {
				return nil, fmt.Errorf("%w: invalid source ID: %v", ErrInvalidRetention, err)
			}
		}
		if _, duplicate := seen[source.EventID]; duplicate {
			return nil, fmt.Errorf("%w: duplicate event %s", ErrInvalidRetention, source.EventID)
		}
		seen[source.EventID] = struct{}{}
		if source.EventType != EventSelfTalk || !source.ContentPresent {
			continue
		}
		for _, count := range []int64{
			source.References.EvidenceReferences, source.References.ClaimReferences,
			source.References.GenerationInputReferences, source.References.GenerationOutputReferences,
		} {
			if count < 0 {
				return nil, fmt.Errorf("%w: reference counts cannot be negative", ErrInvalidRetention)
			}
		}
		recorded := source.RecordedAt.Time()
		if recorded.After(wallNow) {
			continue
		}
		age := wallNow.Sub(recorded)
		if age < policy.Duration {
			continue
		}
		candidate := RetentionCandidate{
			ResidentID: source.ResidentID, EventID: source.EventID, ContentID: source.ContentID,
			RecordedAt: source.RecordedAt, Age: age, AgeMicroseconds: age.Microseconds(), References: source.References,
		}
		if source.ErasurePolicy != "independent" {
			candidate.BlockingReasons = append(candidate.BlockingReasons, ImpactContentPolicyRequiresResidentErase)
		}
		if source.References.EvidenceReferences > 0 || source.References.ClaimReferences > 0 {
			candidate.BlockingReasons = append(candidate.BlockingReasons, ImpactClaimProvenanceReference)
		}
		if source.References.GenerationInputReferences > 0 {
			candidate.BlockingReasons = append(candidate.BlockingReasons, ImpactGenerationInputReference)
		}
		if source.References.GenerationOutputReferences > 0 {
			candidate.BlockingReasons = append(candidate.BlockingReasons, ImpactGenerationOutputReference)
		}
		result = append(result, candidate)
	}
	sort.Slice(result, func(i, j int) bool {
		if result[i].RecordedAt != result[j].RecordedAt {
			return result[i].RecordedAt < result[j].RecordedAt
		}
		return result[i].EventID.String() < result[j].EventID.String()
	})
	return result, nil
}
