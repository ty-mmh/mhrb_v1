package erasure

import (
	"bytes"
	"context"
	"errors"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"mahoroba.local/mahoroba/internal/canonical"
)

func TestM7ErasureLogicalProvenanceCatalogAndConditionalBreak(t *testing.T) {
	type expected struct{ rule, kind, field, classification, mode string }
	rows := []expected{
		{"claim_evidence_event_provenance_v1", "claim_evidence", "event_id", "safe", "none"},
		{"claim_evidence_source_chain_v1", "claim_evidence", "source_evidence_id", "safe", "none"},
		{"claim_validity_evidence_event_v1", "claim_validity_assertion", "evidence_event_id", "needs_review", "consequence_only"},
		{"claim_status_trigger_event_v1", "claim_status_transition", "trigger_event_id", "needs_review", "consequence_only"},
		{"claim_status_trigger_evidence_v1", "claim_status_transition", "trigger_evidence_id", "needs_review", "consequence_only"},
		{"claim_status_trigger_relation_v1", "claim_status_transition", "trigger_claim_relation_id", "needs_review", "consequence_only"},
		{"claim_status_trigger_finding_v1", "claim_status_transition", "trigger_integrity_finding_id", "needs_review", "consequence_only"},
		{"claim_stage_dependency_v1", "claim_stage_transition_dependency", "dependency_claim_id", "needs_review", "consequence_only"},
		{"claim_relation_from_v1", "claim_relation", "from_claim_id", "needs_review", "consequence_only"},
		{"claim_relation_to_v1", "claim_relation", "to_claim_id", "needs_review", "consequence_only"},
		{"claim_usage_claim_v1", "claim_usage", "claim_id", "safe", "none"},
		{"generation_recall_v1", "generation_run", "recall_run_id", "safe", "none"},
	}
	for _, row := range rows {
		t.Run(row.rule, func(t *testing.T) {
			impact, err := ImpactForLogicalReference(LogicalReferenceSnapshot{RuleID: row.rule, ReferrerKind: row.kind, ReferrerID: testIDs[5], ReferrerField: row.field})
			if err != nil {
				t.Fatal(err)
			}
			if !IsLogicalImpactRule(row.rule) || impact.Classification != row.classification || impact.DecisionMode != row.mode || impact.ImpactID == "" {
				t.Fatalf("logical impact=%+v", impact)
			}
			conditional := row.rule == "claim_evidence_event_provenance_v1" || row.rule == "claim_evidence_source_chain_v1"
			_, err = ImpactForLogicalReference(LogicalReferenceSnapshot{RuleID: row.rule, ReferrerKind: row.kind, ReferrerID: testIDs[5], ReferrerField: row.field, BreaksClaim: true})
			if conditional != (err == nil) {
				t.Fatalf("conditional break accepted=%t error=%v", err == nil, err)
			}
		})
	}
	if _, err := ImpactForLogicalReference(LogicalReferenceSnapshot{RuleID: "unknown_v1"}); err == nil {
		t.Fatal("unknown logical rule accepted")
	}
}

func TestM7ErasureTransactionSizeUnsupportedBlocksBeforeMutation(t *testing.T) {
	ids := canonical.NewSecureIDGenerator()
	resident, _ := ids.New()
	head, _ := ids.New()
	actor, _ := ids.New()
	integrityID, _ := ids.New()
	statusID, _ := ids.New()
	seq, _ := canonical.NewCommitSeq(1)
	contents := make([]ContentSnapshot, 0, MaxSingleTransactionMutations/2+1)
	for index := 0; index < MaxSingleTransactionMutations/2+1; index++ {
		contentID, err := ids.New()
		if err != nil {
			t.Fatal(err)
		}
		digest := canonical.HashBlob([]byte(strconv.Itoa(index)))
		contents = append(contents, ContentSnapshot{ContentID: contentID, ResidentID: resident, ContentClass: "event_text", ErasurePolicy: "resident_only", ErasureState: "present", Commitment: digest, BlobHash: &digest, SQLiteBlobValid: true, FilesystemBlobValid: true})
	}
	source := staticSource{Snapshot{HeadCommitID: head, HeadCommitSeq: seq, ResidentStatus: "archived", OwnerHuman: true, Contents: contents, DirectReferences: []DirectReference{}, Aliases: []AliasSnapshot{}, Lineage: []LineageEdge{}, Running: []RunningWork{}, ProjectionDefinitions: []ProjectionDefinition{}}}
	plan, err := (Planner{Source: source, IDs: ids}).Plan(context.Background(), PlanRequest{Scope: ScopeResident, ResidentID: resident, ActorPrincipalID: actor, ReasonCode: "privacy_request", IntegrityPipelineVersionID: integrityID, MemoryStatusPipelineVersionID: statusID})
	if err != nil {
		t.Fatal(err)
	}
	if plannedMutationCount(plan) <= MaxSingleTransactionMutations || plan.PlanState != StateReviewRequired {
		t.Fatalf("mutation bound/state=%d/%s", plannedMutationCount(plan), plan.PlanState)
	}
	found := false
	for _, blocker := range plan.Blockers {
		if blocker.Code == "transaction_size_unsupported" && blocker.TargetID != nil && *blocker.TargetID == resident.String() {
			found = true
		}
	}
	if !found {
		t.Fatalf("missing transaction-size blocker: %+v", plan.Blockers)
	}
	if err := ApplyCommand(ApplyRequest{Plan: plan, Confirm: plan.Digest}).Validate(); !errors.Is(err, ErrReviewRequired) {
		t.Fatalf("blocked plan reached mutation validation: %v", err)
	}
}

var testIDs = []string{
	"01J00000000000000000000001", "01J00000000000000000000002", "01J00000000000000000000003",
	"01J00000000000000000000004", "01J00000000000000000000005", "01J00000000000000000000006",
}

func minimalPlan(t *testing.T) Plan {
	t.Helper()
	plan, err := Seal(Plan{
		PlanState: StateReady, Scope: ScopeContent, ResidentID: testIDs[0], BaseHead: BaseHead{CommitID: testIDs[1], CommitSeq: "1"},
		ActorPrincipalID: testIDs[2], ReasonCode: "privacy_request", IntegrityPipelineVersionID: testIDs[3], MemoryStatusPipelineVersionID: testIDs[4],
		RequestedContentIDs: []string{testIDs[5]}, EffectiveTargets: []EffectiveTarget{{ContentID: testIDs[5], ContentClass: "event_text", ErasurePolicy: "independent", Commitment: "sha256:" + strings.Repeat("1", 64), ErasureEventID: testIDs[1], LineageDepth: "0"}},
		Impacts: []Impact{}, PlannedFindings: []PlannedFinding{}, ExistingFindingDependencies: []ExistingFindingDependency{}, PlannedQuarantines: []PlannedQuarantine{}, ClaimIdentityErasures: []ClaimIdentityErasure{}, ExistingClaimIdentityDependencies: []ExistingClaimIdentityDependency{}, RuntimeConfigEffect: RuntimeConfigEffect{}, Rebuilds: []Rebuild{}, Blockers: []Blocker{},
	})
	if err != nil {
		t.Fatal(err)
	}
	return plan
}

func TestM7ErasurePlanStrictJCSAndDomainDigest(t *testing.T) {
	plan := minimalPlan(t)
	wire, err := MarshalPlan(plan)
	if err != nil {
		t.Fatal(err)
	}
	parsed, err := ParsePlan(wire)
	if err != nil {
		t.Fatal(err)
	}
	if parsed.Digest != plan.Digest {
		t.Fatalf("digest=%s", parsed.Digest)
	}
	for _, mutate := range []func([]byte) []byte{
		func(v []byte) []byte { return append([]byte(" "), v...) },
		func(v []byte) []byte {
			return bytes.Replace(v, []byte(`"rules_version":"erasure-impact-v1"`), []byte(`"unknown":true,"rules_version":"erasure-impact-v1"`), 1)
		},
		func(v []byte) []byte {
			return bytes.Replace(v, []byte(plan.Digest), []byte("sha256:"+strings.Repeat("0", 64)), 1)
		},
	} {
		if _, err := ParsePlan(mutate(slices.Clone(wire))); err == nil {
			t.Fatal("forged plan accepted")
		}
	}
}

func TestM7ErasureImpactIDIsDecisionIndependent(t *testing.T) {
	candidate := testIDs[5]
	impact := Impact{RuleID: "same_bytes_without_lineage_v1", ReferrerKind: "content_object", ReferrerID: candidate, ReferrerField: "shared_bytes", Classification: "needs_review", Actions: []string{"block_physical_gc"}, DecisionMode: "content_candidate", CandidateContentID: &candidate}
	first, err := ImpactID(impact)
	if err != nil {
		t.Fatal(err)
	}
	decision := "retain"
	impact.Decision = &decision
	second, err := ImpactID(impact)
	if err != nil {
		t.Fatal(err)
	}
	if first != second {
		t.Fatal("decision changed impact identity")
	}
}

func TestM7ErasureDecidePromotesCandidateAndRetainsStablePlannedIDs(t *testing.T) {
	request, snapshot := plannerFixture(t)
	candidate := mustTestID(t, "01J00000000000000000000007")
	digest := *snapshot.Contents[0].BlobHash
	snapshot.Contents = append(snapshot.Contents, ContentSnapshot{
		ContentID: candidate, ResidentID: request.ResidentID, ContentClass: "event_text",
		ErasurePolicy: "independent", ErasureState: "present", Commitment: digest, BlobHash: &digest,
		SQLiteBlobValid: true, FilesystemBlobValid: true,
	})
	snapshot.DirectReferences = append(snapshot.DirectReferences, DirectReference{
		ContentID: candidate, RuleID: "content_object_blob_v1", ReferrerKind: "content_object",
		ReferrerID: candidate.String(), ReferrerField: "blob",
	}, DirectReference{
		ContentID: candidate, RuleID: "claim_statement_v1", ReferrerKind: "claim",
		ReferrerID: "01J00000000000000000000008", ReferrerField: "statement_content_id",
	}, DirectReference{
		ContentID: request.RequestedContentIDs[0], RuleID: "generation_input_content_v1", ReferrerKind: "generation_run_input",
		ReferrerID: "01J00000000000000000000009", ReferrerField: "content_id",
	})
	planner := Planner{Source: staticSource{snapshot}, IDs: newSequenceIDs(t)}
	initial, err := planner.Plan(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	if initial.PlanState != StateReviewRequired || len(initial.EffectiveTargets) != 1 {
		t.Fatalf("initial plan=%s targets=%d", initial.PlanState, len(initial.EffectiveTargets))
	}
	rootEvent := initial.EffectiveTargets[0].ErasureEventID
	decisions := make([]DecisionInput, 0, len(initial.Impacts))
	for _, impact := range initial.Impacts {
		if impact.DecisionMode == "none" {
			continue
		}
		decision := "retain"
		if impact.DecisionMode == "content_candidate" {
			decision = "erase"
		}
		decisions = append(decisions, DecisionInput{ImpactID: impact.ImpactID, Decision: decision})
	}
	replanned, err := planner.Decide(context.Background(), initial, decisions)
	if err != nil {
		t.Fatal(err)
	}
	if len(replanned.EffectiveTargets) != 2 || replanned.EffectiveTargets[0].ErasureEventID != rootEvent {
		t.Fatalf("replanned targets=%+v", replanned.EffectiveTargets)
	}
	if !slices.Contains(replanned.RequestedContentIDs, candidate.String()) {
		t.Fatalf("promoted candidate missing from roots: %v", replanned.RequestedContentIDs)
	}
	for _, impact := range replanned.Impacts {
		if impact.DecisionMode != "none" && impact.ReferrerID == "01J00000000000000000000009" && (impact.Decision == nil || *impact.Decision != "retain") {
			t.Fatalf("stable review decision was not retained: %+v", impact)
		}
		if impact.DecisionMode == "content_candidate" {
			t.Fatalf("promoted candidate remains review candidate: %+v", impact)
		}
	}
	if replanned.PlanState != StateReviewRequired {
		t.Fatalf("newly exposed consequence must require review, got %s", replanned.PlanState)
	}
	remaining := []DecisionInput{}
	for _, impact := range replanned.Impacts {
		if impact.DecisionMode != "none" && impact.Decision == nil {
			remaining = append(remaining, DecisionInput{ImpactID: impact.ImpactID, Decision: "retain"})
		}
	}
	ready, err := planner.Decide(context.Background(), replanned, remaining)
	if err != nil {
		t.Fatal(err)
	}
	if ready.PlanState != StateReady || ready.EffectiveTargets[0].ErasureEventID != rootEvent {
		t.Fatalf("ready replanning lost stable IDs: %+v", ready)
	}
}

func TestM7ErasureLineageFixedPointChoosesShortestLexicalParentAndBlocksCycles(t *testing.T) {
	request, snapshot := plannerFixture(t)
	b := mustTestID(t, "01J00000000000000000000007")
	c := mustTestID(t, "01J00000000000000000000008")
	d := mustTestID(t, "01J00000000000000000000009")
	digest := *snapshot.Contents[0].BlobHash
	for _, id := range []canonical.ID{b, c, d} {
		snapshot.Contents = append(snapshot.Contents, ContentSnapshot{
			ContentID: id, ResidentID: request.ResidentID, ContentClass: "generation_output",
			ErasurePolicy: "independent", ErasureState: "present", Commitment: digest, BlobHash: &digest,
			SQLiteBlobValid: true, FilesystemBlobValid: true,
		})
		snapshot.DirectReferences = append(snapshot.DirectReferences, DirectReference{
			ContentID: id, RuleID: "content_object_blob_v1", ReferrerKind: "content_object",
			ReferrerID: id.String(), ReferrerField: "blob",
		})
	}
	run := mustTestID(t, "01J0000000000000000000000A")
	root := request.RequestedContentIDs[0]
	// Deliberately reverse parent order; the result must still choose B.
	snapshot.Lineage = []LineageEdge{
		{ParentContentID: c, ChildContentID: d, RunID: run, SameLogicalBlob: true, Valid: true},
		{ParentContentID: root, ChildContentID: c, RunID: run, SameLogicalBlob: true, Valid: true},
		{ParentContentID: b, ChildContentID: d, RunID: run, SameLogicalBlob: true, Valid: true},
		{ParentContentID: root, ChildContentID: b, RunID: run, SameLogicalBlob: true, Valid: true},
	}
	plan, err := (Planner{Source: staticSource{snapshot}, IDs: newSequenceIDs(t)}).Plan(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	if plan.PlanState != StateReady || len(plan.EffectiveTargets) != 4 {
		t.Fatalf("fixed point plan=%s targets=%+v blockers=%+v", plan.PlanState, plan.EffectiveTargets, plan.Blockers)
	}
	eventByContent := map[string]string{}
	for _, target := range plan.EffectiveTargets {
		eventByContent[target.ContentID] = target.ErasureEventID
	}
	for _, target := range plan.EffectiveTargets {
		if target.ContentID == d.String() {
			if target.LineageDepth != "2" || target.SourceErasureEventID == nil || *target.SourceErasureEventID != eventByContent[b.String()] {
				t.Fatalf("D parent/depth=%+v, want lexical B", target)
			}
		}
	}
	exact := 0
	for _, impact := range plan.Impacts {
		if impact.RuleID == "explicit_lineage_exact_bytes_v1" {
			exact++
		}
	}
	if exact != 3 {
		t.Fatalf("exact lineage impacts=%d", exact)
	}

	snapshot.Lineage = append(snapshot.Lineage, LineageEdge{ParentContentID: d, ChildContentID: b, RunID: run, SameLogicalBlob: true, Valid: true})
	blocked, err := (Planner{Source: staticSource{snapshot}, IDs: newSequenceIDs(t)}).Plan(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	foundCycle := false
	for _, blocker := range blocked.Blockers {
		if blocker.Code == "lineage_cycle" && blocker.TargetID != nil && *blocker.TargetID == b.String() {
			foundCycle = true
		}
	}
	if !foundCycle || blocked.PlanState != StateReviewRequired {
		t.Fatalf("cycle was not deterministically blocked: %+v", blocked.Blockers)
	}
}

func TestM7I72ContentEraseRejectsResidentOnlyContent(t *testing.T) {
	plan := minimalPlan(t)
	plan.EffectiveTargets[0].ErasurePolicy = "resident_only"
	if _, err := Seal(plan); !errors.Is(err, ErrInvalidPlan) {
		t.Fatalf("error=%v", err)
	}
}

func TestM7RTI15QuiescenceRediscoversMandatoryWork(t *testing.T) {
	snapshot := QuiescenceSnapshot{MandatoryDialogueWork: 1}
	if snapshot.BranchQuiescentV1() {
		t.Fatal("Canonical mandatory work was ignored")
	}
	snapshot = QuiescenceSnapshot{MandatoryMemoryExtractionWork: 1}
	if snapshot.ResidentEraseSafe() {
		t.Fatal("mandatory extraction was ignored")
	}
}

func TestM7RTI21QuiescenceExcludesProjectionAndOptionalWork(t *testing.T) {
	snapshot := QuiescenceSnapshot{RunningPurposes: []string{"self_talk", "outbound_initiative", "persona_revision"}}
	if !snapshot.BranchQuiescentV1() {
		t.Fatal("optional work affected branch predicate")
	}
	if snapshot.ResidentEraseSafe() {
		t.Fatal("resident erasure ignored optional running attempt")
	}
}

func TestM7I73ImpactIsSeparateAndDoesNotPropagateAcrossResidents(t *testing.T) {
	request, snapshot := plannerFixture(t)
	snapshot.Contents[0].ResidentID = mustTestID(t, "01J00000000000000000000006")
	plan, err := (Planner{Source: staticSource{snapshot}, IDs: newSequenceIDs(t)}).Plan(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	if len(plan.EffectiveTargets) != 0 || len(plan.Blockers) != 1 || plan.Blockers[0].Code != "cross_resident_reference" {
		t.Fatalf("cross-resident closure = targets=%d blockers=%+v", len(plan.EffectiveTargets), plan.Blockers)
	}
}

func TestM7RTI26SystemAndResidentPrincipalsCannotErase(t *testing.T) {
	for _, kind := range []string{"system", "resident"} {
		t.Run(kind, func(t *testing.T) {
			request, snapshot := plannerFixture(t)
			snapshot.OwnerHuman = false
			plan, err := (Planner{Source: staticSource{snapshot}, IDs: newSequenceIDs(t)}).Plan(context.Background(), request)
			if err != nil {
				t.Fatal(err)
			}
			if len(plan.Blockers) == 0 || plan.Blockers[0].Code != "actor_not_owner_human" {
				t.Fatalf("blockers=%+v", plan.Blockers)
			}
		})
	}
}

func TestM7ContentEraseBlocksOnlyTargetInvolvedRunningAttempts(t *testing.T) {
	request, snapshot := plannerFixture(t)
	runID := mustTestID(t, "01J00000000000000000000007")
	unrelatedID := mustTestID(t, "01J00000000000000000000008")
	digest := canonical.HashBlob([]byte("unrelated"))
	snapshot.Contents = append(snapshot.Contents, ContentSnapshot{ContentID: unrelatedID, ResidentID: request.ResidentID, ContentClass: "event_text", ErasurePolicy: "independent", ErasureState: "present", Commitment: digest, BlobHash: &digest, SQLiteBlobValid: true, FilesystemBlobValid: true})
	snapshot.Running = []RunningWork{{RunID: runID, Purpose: "persona_revision", ContentIDs: []canonical.ID{unrelatedID}}}
	plan, err := (Planner{Source: staticSource{snapshot}, IDs: newSequenceIDs(t)}).Plan(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	for _, blocker := range plan.Blockers {
		if blocker.Code == "running_attempt" {
			t.Fatalf("unrelated running attempt blocked Content Erase: %+v", blocker)
		}
	}

	snapshot.Running[0].ContentIDs = []canonical.ID{request.RequestedContentIDs[0]}
	plan, err = (Planner{Source: staticSource{snapshot}, IDs: newSequenceIDs(t)}).Plan(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	found := false
	for _, blocker := range plan.Blockers {
		if blocker.Code == "running_attempt" && blocker.TargetID != nil && *blocker.TargetID == runID.String() {
			found = true
		}
	}
	if !found {
		t.Fatalf("target-involved running attempt was not blocked: %+v", plan.Blockers)
	}
}

func plannerFixture(t *testing.T) (PlanRequest, Snapshot) {
	t.Helper()
	resident := mustTestID(t, testIDs[0])
	head := mustTestID(t, testIDs[1])
	actor := mustTestID(t, testIDs[2])
	integrityID := mustTestID(t, testIDs[3])
	status := mustTestID(t, testIDs[4])
	content := mustTestID(t, testIDs[5])
	seq, _ := canonical.NewCommitSeq(1)
	digest := canonical.HashBlob([]byte("payload"))
	request := PlanRequest{Scope: ScopeContent, ResidentID: resident, ActorPrincipalID: actor, ReasonCode: "privacy_request", IntegrityPipelineVersionID: integrityID, MemoryStatusPipelineVersionID: status, RequestedContentIDs: []canonical.ID{content}}
	snapshot := Snapshot{HeadCommitID: head, HeadCommitSeq: seq, ResidentStatus: "active", OwnerHuman: true, Contents: []ContentSnapshot{{ContentID: content, ResidentID: resident, ContentClass: "event_text", ErasurePolicy: "independent", ErasureState: "present", Commitment: digest, BlobHash: &digest, SQLiteBlobValid: true, FilesystemBlobValid: true}}, DirectReferences: []DirectReference{{ContentID: content, RuleID: "content_object_blob_v1", ReferrerKind: "content_object", ReferrerID: content.String(), ReferrerField: "blob"}}, ProjectionDefinitions: []ProjectionDefinition{}}
	return request, snapshot
}

func mustTestID(t *testing.T, raw string) canonical.ID {
	t.Helper()
	id, err := canonical.ParseID(raw)
	if err != nil {
		t.Fatal(err)
	}
	return id
}

type staticSource struct{ snapshot Snapshot }

func (source staticSource) CaptureErasureSnapshot(context.Context, CaptureRequest) (Snapshot, error) {
	return source.snapshot, nil
}

type sequenceIDs struct{ generator *canonical.IDGenerator }

func newSequenceIDs(t *testing.T) *sequenceIDs {
	t.Helper()
	clock := fixedClock{time.UnixMilli(1720000000000)}
	generator, err := canonical.NewIDGenerator(clock, bytes.NewReader(bytes.Repeat([]byte{7}, 128)))
	if err != nil {
		t.Fatal(err)
	}
	return &sequenceIDs{generator}
}
func (ids *sequenceIDs) New() (canonical.ID, error) { return ids.generator.New() }

type fixedClock struct{ value time.Time }

func (clock fixedClock) Now() time.Time { return clock.value }
