package sqlite

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"

	"mahoroba.local/mahoroba/internal/canonical"
	"mahoroba.local/mahoroba/internal/domain"
	"mahoroba.local/mahoroba/internal/memory"
)

func TestRecallV5RanksRelevantClaimBeforeCandidateLimitDespiteOldSaturatedSalience(t *testing.T) {
	fixture := newMemoryClaimMutationFixture(t)
	eventID := fixture.seedEvent(t, memory.EventUserMessage, fixture.ownerPrincipal, memory.TrustTrusted)
	addState := func(claimID canonical.ID, salience float64) {
		mustExec(t, fixture.semantic.db, `INSERT INTO claim_states(claim_id,resident_id,stage,status,salience,confidence,currentness,temporal_relation,last_referenced_at,evidence_count) VALUES (?,?,'floating','active',?,500000,1000000,'current',NULL,1)`, claimID.String(), fixture.residentID.String(), salience)
		mustExec(t, fixture.semantic.db, `INSERT INTO claim_view_scope_current(claim_id,resident_id,view_scope,source_assertion_id) VALUES (?,?,'resident_ui',?)`, claimID.String(), fixture.residentID.String(), fixture.newID(t).String())
	}
	addState(claimMutationParseID(t, fixture.semantic.claim["A"]), 1)
	for i := 0; i < 70; i++ {
		claimID, _ := fixture.seedClaim(t, memory.ClaimKindOther, fixture.ownerPrincipal, fixture.residentPrincipal, []seededClaimEvidence{trustedEvidence(eventID, memory.EventUserMessage, fixture.ownerPrincipal, memory.PolaritySupport)}, memory.StageFloating, fmt.Sprintf("大阪で音楽を聴く %d", i))
		addState(claimID, 1)
	}
	fresh, _ := fixture.seedClaim(t, memory.ClaimKindOther, fixture.ownerPrincipal, fixture.residentPrincipal, []seededClaimEvidence{trustedEvidence(eventID, memory.EventUserMessage, fixture.ownerPrincipal, memory.PolaritySupport)}, memory.StageFloating, "東京の天気")
	addState(fresh, 0)
	uow, metadata := fixture.beginUoW(t)
	defer uow.tx.Rollback()
	head := canonical.CommitSeq(metadata.CommitSeq.Int64() - 1)
	old, err := uow.loadRecallCandidates(context.Background(), fixture.residentID, head, memory.DefaultPolicyV4(), "東京の天気")
	if err != nil {
		t.Fatal(err)
	}
	for _, c := range old {
		if c.ClaimID == fresh {
			t.Fatal("fixture did not reproduce v4 candidate starvation")
		}
	}
	candidates, err := uow.loadRecallCandidates(context.Background(), fixture.residentID, head, memory.DefaultPolicyV5(), "東京の天気")
	if err != nil {
		t.Fatal(err)
	}
	if len(candidates) != domain.MaxRecallCandidates || candidates[0].ClaimID != fresh || candidates[0].ContextCompatibility != 1_000_000 {
		t.Fatalf("V5 top64=%+v", candidates)
	}
	selection, err := memory.SelectRecall(memory.DefaultPolicyV5(), candidates)
	if err != nil || len(selection.Selected) != 1 || selection.Selected[0].Candidate.ClaimID != fresh {
		t.Fatalf("V5 selected unrelated claims: %+v / %v", selection, err)
	}
}

func TestRecallV5WriterRecomputesQueryCompatibilityAndRetriesFrozenPlan(t *testing.T) {
	for _, tamper := range []bool{false, true} {
		t.Run(fmt.Sprintf("tamper=%t", tamper), func(t *testing.T) {
			fixture := newDialoguePrepareWriterFixtureWithPolicyAndSource(t, memory.DefaultPolicyV5(), []byte("A-claim"), false)
			prepare := fixture.assemble(t)
			if prepare.RecallDisposition != domain.RecallDispositionSuccess || len(prepare.RecallCandidates) == 0 || prepare.RecallCandidates[0].ContextCompatibility != 1_000_000 {
				t.Fatalf("query compatibility not captured: %+v", prepare.RecallCandidates)
			}
			if !strings.Contains(prepare.Recall.QueryConditions.String(), "memory-recall-v2-bigram") || strings.Contains(prepare.Recall.QueryConditions.String(), "A-claim") {
				t.Fatal("query decision lacks scoring version or leaks raw query")
			}
			if tamper {
				prepare.RecallCandidates[0].ContextCompatibility = 500_000
			}
			uow := fixture.begin(t, 9, fixture.target.AsOf+1)
			_, err := uow.PrepareDialogue(context.Background(), prepare)
			if tamper {
				_ = uow.Rollback(context.Background())
				if err == nil {
					t.Fatal("forged compatibility was accepted")
				}
				return
			}
			if err != nil {
				_ = uow.Rollback(context.Background())
				t.Fatal(err)
			}
			if err := uow.Commit(context.Background()); err != nil {
				t.Fatal(err)
			}
			retry := fixture.begin(t, 10, fixture.target.AsOf+2)
			defer retry.Rollback(context.Background())
			result, err := retry.PrepareDialogue(context.Background(), prepare)
			if !errors.Is(err, canonical.ErrNoMutation) || result.RunID != prepare.Generation.RunID {
				t.Fatalf("V5 frozen retry=%+v err=%v", result, err)
			}
		})
	}
}

func TestMemoryPolicyV5WriterActivationContract(t *testing.T) {
	for _, current := range []memory.Policy{memory.DefaultPolicyV1(), memory.DefaultPolicyV4(), memory.DefaultPolicyV5()} {
		err := validateMemoryPolicyActivationTransition(current, memory.DefaultPolicyV5(), domain.ActivateMemoryPolicy{ExpectedFrom: current.Version, AcknowledgeRecallEnable: true, AcknowledgeSelfTalkExtraction: true})
		if err != nil {
			t.Fatalf("%s to V5: %v", current.Version, err)
		}
	}
	if err := validateMemoryPolicyActivationTransition(memory.DefaultPolicyV5(), memory.DefaultPolicyV4(), domain.ActivateMemoryPolicy{ExpectedFrom: memory.PolicyVersionV5}); err == nil {
		t.Fatal("Writer allowed downgrade")
	}
}

func TestRecallV5CandidateCapKeepsNewestEqualScoreClaims(t *testing.T) {
	var ranked []rankedRecallCandidate
	for index := 1; index <= 70; index++ {
		ranked = retainRankedRecallCandidate(ranked, rankedRecallCandidate{
			candidate: memory.RecallCandidate{ClaimID: recallAssuranceID(t, index)}, score: 750_000,
		}, domain.MaxRecallCandidates, true)
	}
	if len(ranked) != domain.MaxRecallCandidates || ranked[0].candidate.ClaimID != recallAssuranceID(t, 70) || ranked[len(ranked)-1].candidate.ClaimID != recallAssuranceID(t, 7) {
		t.Fatalf("equal-score candidate cap did not retain newest IDs: %+v", ranked)
	}
}

func TestRecallV5RejectsQueryErasedAfterAssembly(t *testing.T) {
	fixture := newDialoguePrepareWriterFixtureWithPolicyAndSource(t, memory.DefaultPolicyV5(), []byte("A-claim"), false)
	prepare := fixture.assemble(t)
	mustExec(t, fixture.semantic.db, `UPDATE content_objects
		SET erasure_state = 'erased', blob_hash = NULL, commitment_salt = NULL
		WHERE content_id = (SELECT content_id FROM events WHERE event_id = ?)`, fixture.sourceEventID.String())
	uow := fixture.begin(t, 9, fixture.target.AsOf+1)
	defer uow.Rollback(context.Background())
	if _, err := uow.PrepareDialogue(context.Background(), prepare); err == nil {
		t.Fatal("an erased query was reconstructed from the candidate snapshot")
	}
}

func TestMemoryPolicyV5ReadinessKeepsV1AndV4Compatible(t *testing.T) {
	for _, policy := range []memory.Policy{memory.DefaultPolicyV1(), memory.DefaultPolicyV4(), memory.DefaultPolicyV5()} {
		t.Run(string(policy.Version), func(t *testing.T) {
			fixture := newDialoguePrepareWriterFixtureWithPolicy(t, policy)
			tx, err := fixture.semantic.db.BeginTx(context.Background(), nil)
			if err != nil {
				t.Fatal(err)
			}
			defer tx.Rollback()
			current, err := captureReadinessMemoryPolicyServiceCurrent(context.Background(), tx, fixture.residentID)
			if err != nil || !current {
				t.Fatalf("service-current=%t err=%v", current, err)
			}
		})
	}
}
