package durablepublish

import (
	"errors"
	"testing"
)

func TestM7ErasureProducerInputsAreExactCommandBoundAndSorted(t *testing.T) {
	digest := "sha256:1111111111111111111111111111111111111111111111111111111111111111"
	resident := "01J00000000000000000000300"
	contentA := "01J00000000000000000000301"
	contentB := "01J00000000000000000000302"
	head := ErasurePlanBaseHead{CommitID: "01J00000000000000000000303", CommitSeq: "9"}
	input := ErasurePlanContentProducerInput{
		SourceDataDirIdentity: digest, ResidentID: resident,
		RequestedContentIDs: []string{contentA, contentB}, ReasonCode: "privacy_request",
		BaseHead: head, ImpactVersion: "erasure-impact-v1",
	}
	if _, err := ProducerInputDigest(CommandErasurePlanContent, input); err != nil {
		t.Fatal(err)
	}
	if _, err := ProducerInputDigest(CommandErasurePlanResident, input); !errors.Is(err, ErrInvalidProducerInput) {
		t.Fatalf("content schema crossed command boundary: %v", err)
	}
	input.RequestedContentIDs = []string{contentB, contentA}
	if _, err := ProducerInputDigest(CommandErasurePlanContent, input); !errors.Is(err, ErrInvalidProducerInput) {
		t.Fatalf("unsorted roots accepted: %v", err)
	}
	decision := ErasureDecideProducerInput{
		SourceDataDirIdentity: digest, InputPlanIdentity: digest, InputPlanDigest: digest,
		Decisions: []ErasureDecision{{ImpactID: digest, Decision: "retain"}},
	}
	if _, err := ProducerInputDigest(CommandErasureDecide, decision); err != nil {
		t.Fatal(err)
	}
	decision.Decisions = append(decision.Decisions, decision.Decisions[0])
	if _, err := ProducerInputDigest(CommandErasureDecide, decision); !errors.Is(err, ErrInvalidProducerInput) {
		t.Fatalf("duplicate decision accepted: %v", err)
	}
}
