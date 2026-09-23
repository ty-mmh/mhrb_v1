package sqlite

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"mahoroba.local/mahoroba/internal/canonical"
	"mahoroba.local/mahoroba/internal/domain"
	"mahoroba.local/mahoroba/internal/memory"
)

func TestM5I47ClaimUsageRequiresUsageTimeMemoryPolicy(t *testing.T) {
	fixture := newMemoryClaimMutationFixture(t)
	uow, metadata := fixture.beginUoW(t)
	defer func() { _ = uow.tx.Rollback() }()

	wrongPolicy := claimMutationParseID(t, fixture.semantic.revision["A"]["memory_policy"])
	if wrongPolicy == fixture.policyID {
		t.Fatal("negative policy fixture unexpectedly equals the active v2 revision")
	}
	empty, err := canonical.MarshalCanonical(struct{}{})
	if err != nil {
		t.Fatal(err)
	}
	recall := domain.DialogueRecall{
		RunID: fixture.newID(t), ResidentID: fixture.residentID,
		QueryContentID:         claimMutationParseID(t, fixture.semantic.content["A"]["event"]),
		PipelineVersionID:      fixture.maturationPipeline,
		MemoryPolicyRevisionID: wrongPolicy,
		AsOf:                   metadata.CommittedAt,
		AsOfTZ:                 metadata.CommittedTZ,
		QueryConditions:        empty,
		ContextConstraints:     empty,
	}
	err = uow.insertDialogueRecall(context.Background(), recall)
	if err == nil || !strings.Contains(err.Error(), "not active at usage time") {
		t.Fatalf("inactive usage-time memory policy error = %v", err)
	}
	var rows int
	if err := uow.tx.QueryRow(`SELECT COUNT(*) FROM recall_runs WHERE recall_run_id = ?`,
		recall.RunID.String()).Scan(&rows); err != nil {
		t.Fatal(err)
	}
	if rows != 0 {
		t.Fatalf("rejected Recall rows = %d, want 0", rows)
	}
}

func TestRecallCandidateLimitUsesFullFixedPointScore(t *testing.T) {
	policy := memory.DefaultPolicyV2()
	maximum, _ := canonical.NewRatio(canonical.FixedPointScale)
	zero, _ := canonical.NewRatio(0)
	ranked := make([]rankedRecallCandidate, 0, domain.MaxRecallCandidates)
	for index := 0; index < domain.MaxRecallCandidates+1; index++ {
		candidate := memory.RecallCandidate{
			ClaimID: recallAssuranceID(t, index+1), Statement: "high salience but weak state",
			ContextCompatibility: maximum, Salience: maximum, Confidence: zero,
			Status: memory.StatusActive, Stage: memory.StageFloating,
			TemporalRelation: memory.RelationStaleUnknown,
		}
		scored, err := memory.ScoreRecall(policy, candidate, 0)
		if err != nil {
			t.Fatal(err)
		}
		ranked = retainRankedRecallCandidate(ranked, rankedRecallCandidate{
			candidate: candidate, score: scored.Score,
		}, domain.MaxRecallCandidates)
	}
	special := memory.RecallCandidate{
		ClaimID: recallAssuranceID(t, domain.MaxRecallCandidates+2), Statement: "low salience but current and settled",
		ContextCompatibility: maximum, Salience: zero, Confidence: maximum,
		Status: memory.StatusActive, Stage: memory.StageSettled,
		TemporalRelation: memory.RelationCurrent,
	}
	scored, err := memory.ScoreRecall(policy, special, 0)
	if err != nil {
		t.Fatal(err)
	}
	ranked = retainRankedRecallCandidate(ranked, rankedRecallCandidate{
		candidate: special, score: scored.Score,
	}, domain.MaxRecallCandidates)
	if len(ranked) != domain.MaxRecallCandidates {
		t.Fatalf("ranked candidates = %d, want %d", len(ranked), domain.MaxRecallCandidates)
	}
	if ranked[0].candidate.ClaimID != special.ClaimID {
		t.Fatalf("full-score winner = %s, want low-salience current/settled %s",
			ranked[0].candidate.ClaimID, special.ClaimID)
	}
}

func TestDialogueBudgetDropsRecallThenOldestBackfillThenOldestLive(t *testing.T) {
	recallA := recallAssuranceID(t, 101)
	recallB := recallAssuranceID(t, 102)
	seq := func(value int64) canonical.Seq {
		result, err := canonical.NewSeq(value)
		if err != nil {
			t.Fatal(err)
		}
		return result
	}
	candidates := []dialogueInputCandidate{
		{inclusionMode: "resident_definition", byteSize: 1},
		{inclusionMode: "live_context", byteSize: 1, live: true, eventSeq: seq(3)},
		{inclusionMode: "live_context", byteSize: 1, live: true, eventSeq: seq(2)},
		{inclusionMode: "context_backfill", byteSize: 1, backfill: true, eventSeq: seq(4)},
		{inclusionMode: "context_backfill", byteSize: 1, backfill: true, eventSeq: seq(1)},
		{inclusionMode: "context_backfill", byteSize: 1, initiative: true, eventSeq: seq(5)},
		{inclusionMode: "memory_recall", byteSize: 1, recall: true, recallClaimID: &recallA},
		{inclusionMode: "memory_recall", byteSize: 1, recall: true, recallClaimID: &recallB},
	}
	kept, exceeded, backfill, live, initiative, recalled, err := applyDialogueBudget(
		append([]dialogueInputCandidate(nil), candidates...), 2, domain.MaxDialogueInputs,
	)
	if err != nil {
		t.Fatal(err)
	}
	if initiative != 0 {
		t.Fatalf("dropped initiative = %d, want 0", initiative)
	}
	if !exceeded || backfill != 2 || live != 2 || len(recalled) != 2 {
		t.Fatalf("budget result exceeded=%t backfill=%d live=%d recall=%d",
			exceeded, backfill, live, len(recalled))
	}
	if _, ok := recalled[recallA]; !ok {
		t.Fatalf("first Recall claim %s was not dropped", recallA)
	}
	if _, ok := recalled[recallB]; !ok {
		t.Fatalf("second Recall claim %s was not dropped", recallB)
	}
	if len(kept) != 2 || kept[0].inclusionMode != "resident_definition" || !kept[1].initiative {
		t.Fatalf("kept inputs = %+v, want mandatory plus initiative after all lower-priority inputs drop", kept)
	}
	_, _, _, _, initiative, _, err = applyDialogueBudget(
		append([]dialogueInputCandidate(nil), candidates...), 1, domain.MaxDialogueInputs,
	)
	if err != nil {
		t.Fatal(err)
	}
	if initiative != 1 {
		t.Fatalf("dropped initiative at final optional tier = %d, want 1", initiative)
	}
}

func recallAssuranceID(t *testing.T, ordinal int) canonical.ID {
	t.Helper()
	id, err := canonical.ParseID(fmt.Sprintf("%026d", ordinal))
	if err != nil {
		t.Fatal(err)
	}
	return id
}
