package domain

import (
	"context"
	"fmt"
	"strings"

	"mahoroba.local/mahoroba/internal/canonical"
)

const (
	MemoryAlignmentInputCount = 4
	// Discovery considers at most 64 direct and 64 meta candidates in one
	// deterministic pass. This bounds provider obligations without allowing a
	// completed prefix of the selected candidate set to starve its tail.
	MaximumMemoryAlignmentPairs = 64 * 64
)

func MemoryAlignmentObligation(
	directClaimID, metaClaimID, directEvidenceID, metaEvidenceID canonical.ID,
) string {
	return "memory_alignment:v1:" + directClaimID.String() + ":" + metaClaimID.String() + ":" +
		directEvidenceID.String() + ":" + metaEvidenceID.String()
}

// MemoryAlignmentReplacementObligation freezes an owner-authorized
// replacement intent into the durable generation envelope. The intent
// relation is Canonical and immutable; the old claim remains subject to a
// landing-time active/settled CAS.
func MemoryAlignmentReplacementObligation(
	directClaimID, metaClaimID, directEvidenceID, metaEvidenceID,
	oldClaimID, intentRelationID canonical.ID,
) string {
	return "memory_alignment:v2:" + directClaimID.String() + ":" + metaClaimID.String() + ":" +
		directEvidenceID.String() + ":" + metaEvidenceID.String() + ":" +
		oldClaimID.String() + ":" + intentRelationID.String()
}

type MemoryAlignmentReplacementIdentity struct {
	OldClaimID       canonical.ID
	IntentRelationID canonical.ID
}

type MemoryAlignmentIdentity struct {
	DirectClaimID    canonical.ID
	MetaClaimID      canonical.ID
	DirectEvidenceID canonical.ID
	MetaEvidenceID   canonical.ID
	Replacement      *MemoryAlignmentReplacementIdentity
}

func ParseMemoryAlignmentObligation(value string) (MemoryAlignmentIdentity, error) {
	parts := strings.Split(value, ":")
	if len(parts) < 2 || parts[0] != "memory_alignment" ||
		!((parts[1] == "v1" && len(parts) == 6) || (parts[1] == "v2" && len(parts) == 8)) {
		return MemoryAlignmentIdentity{}, fmt.Errorf("domain: invalid memory alignment obligation")
	}
	ids := make([]canonical.ID, len(parts)-2)
	for index := range ids {
		parsed, err := canonical.ParseID(parts[index+2])
		if err != nil {
			return MemoryAlignmentIdentity{}, fmt.Errorf("domain: invalid memory alignment obligation ID: %w", err)
		}
		ids[index] = parsed
	}
	if ids[0] == ids[1] || ids[2] == ids[3] {
		return MemoryAlignmentIdentity{}, fmt.Errorf("domain: memory alignment identities must be distinct")
	}
	result := MemoryAlignmentIdentity{
		DirectClaimID: ids[0], MetaClaimID: ids[1], DirectEvidenceID: ids[2], MetaEvidenceID: ids[3],
	}
	if parts[1] == "v2" {
		if ids[4] == ids[0] || ids[5] == ids[0] || ids[5] == ids[4] {
			return MemoryAlignmentIdentity{}, fmt.Errorf("domain: invalid memory replacement identities")
		}
		result.Replacement = &MemoryAlignmentReplacementIdentity{
			OldClaimID: ids[4], IntentRelationID: ids[5],
		}
	}
	return result, nil
}

type MemoryAlignmentClaim struct {
	ClaimID          canonical.ID
	LatestEvidenceID canonical.ID
	Statement        string
}

type MemoryAlignmentWork struct {
	Resident                    ResidentSnapshot
	PolicyContent               string
	AlignmentPipelineVersionID  canonical.ID
	MaturationPipelineVersionID canonical.ID
	Direct                      MemoryAlignmentClaim
	Meta                        MemoryAlignmentClaim
	IdempotencyKey              string
	RunID                       *canonical.ID
	AttemptNo                   int64
	RetryCount                  int64
	State                       WorkState
	ForegroundPreempted         bool
	Replacement                 *MemoryAlignmentReplacementWork
}

type MemoryAlignmentReplacementWork struct {
	OldClaimID              canonical.ID
	IntentRelationID        canonical.ID
	StatusPipelineVersionID canonical.ID
}

type MemoryAlignmentWorkRepository interface {
	DiscoverMemoryAlignmentWork(context.Context, canonical.ID, int, int) (*MemoryAlignmentWork, error)
}

type LandMemoryAlignment struct {
	Attempt
	DirectClaimID               canonical.ID
	MetaClaimID                 canonical.ID
	DirectEvidenceID            canonical.ID
	MetaEvidenceID              canonical.ID
	AlignmentPipelineVersionID  canonical.ID
	MaturationPipelineVersionID canonical.ID
	MemoryPolicyRevisionID      canonical.ID
	StageTransitionID           canonical.ID
	StageTransitionDependencyID canonical.ID
	Output                      Content
	PromptTokens                *int64
	CompletionTokens            *int64
	LatencyMicros               int64
	Replacement                 *MemoryAlignmentReplacement
}

// MemoryAlignmentReplacement extends a validated positive alignment landing
// with the I-96 atomic supersession. The replacement claim is DirectClaimID;
// it must reach settled in this same Canonical commit.
type MemoryAlignmentReplacement struct {
	OldClaimID              canonical.ID
	IntentRelationID        canonical.ID
	RelationID              canonical.ID
	OldStatusTransitionID   canonical.ID
	StatusPipelineVersionID canonical.ID
}

type MemoryAlignmentLandingResult struct {
	Aligned            bool            `json:"aligned"`
	Confidence         canonical.Ratio `json:"confidence"`
	StageTransitionID  *canonical.ID   `json:"stage_transition_id,omitempty"`
	ReplacementApplied bool            `json:"replacement_applied"`
}

type memoryAlignmentMutator interface {
	LandMemoryAlignment(context.Context, LandMemoryAlignment) (MemoryAlignmentLandingResult, error)
}

func LandMemoryAlignmentCommand(value LandMemoryAlignment) canonical.Command {
	scope, _ := canonical.ResidentScope(value.ResidentID)
	return command{
		name: "LandMemoryAlignment", scope: scope,
		validate: func() error {
			if value.AttemptNo < 1 || value.LatencyMicros < 0 {
				return fmt.Errorf("domain: invalid memory alignment attempt")
			}
			for _, id := range []canonical.ID{
				value.RunID, value.ResidentID, value.OutcomeID, value.DirectClaimID, value.MetaClaimID,
				value.DirectEvidenceID, value.MetaEvidenceID, value.AlignmentPipelineVersionID,
				value.MaturationPipelineVersionID, value.MemoryPolicyRevisionID,
				value.StageTransitionID, value.StageTransitionDependencyID,
			} {
				if err := id.Validate(); err != nil {
					return err
				}
			}
			if value.DirectClaimID == value.MetaClaimID || value.DirectEvidenceID == value.MetaEvidenceID ||
				value.StageTransitionID == value.StageTransitionDependencyID {
				return fmt.Errorf("domain: memory alignment identities must be distinct")
			}
			if value.Replacement != nil {
				for _, id := range []canonical.ID{
					value.Replacement.OldClaimID, value.Replacement.IntentRelationID,
					value.Replacement.RelationID,
					value.Replacement.OldStatusTransitionID, value.Replacement.StatusPipelineVersionID,
				} {
					if err := id.Validate(); err != nil {
						return err
					}
				}
				if value.Replacement.OldClaimID == value.DirectClaimID {
					return fmt.Errorf("domain: replacement and old claim must differ")
				}
			}
			if value.Output.ResidentID != value.ResidentID || value.Output.Class != "generation_output" {
				return fmt.Errorf("domain: invalid memory alignment output content")
			}
			return value.Output.Validate()
		},
		execute: func(ctx context.Context, store MutationStore) (any, error) {
			mutator, ok := store.(memoryAlignmentMutator)
			if !ok {
				return nil, fmt.Errorf("domain: canonical UoW lacks memory alignment landing capability")
			}
			return mutator.LandMemoryAlignment(ctx, value)
		},
	}
}
