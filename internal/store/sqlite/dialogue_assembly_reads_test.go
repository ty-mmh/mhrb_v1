package sqlite

import (
	"bytes"
	"context"
	"errors"
	"testing"

	"mahoroba.local/mahoroba/internal/canonical"
	"mahoroba.local/mahoroba/internal/domain"
	"mahoroba.local/mahoroba/internal/memory"
	"mahoroba.local/mahoroba/internal/projection"
)

func TestCOV1DialogueAssemblyRejectsChangedHead(t *testing.T) {
	fixture := newDialogueAssemblyReadFixture(t)
	mustExec(t, fixture.semantic.db, `INSERT INTO canonical_commits(
		canonical_commit_id, commit_seq, resident_id, committed_at, committed_tz
	) VALUES (?, 7, ?, ?, ?)`, fixture.semantic.ids.new(), fixture.residentID.String(),
		semanticTime+6, semanticTZ)

	_, err := fixture.repository.AssembleDialogue(context.Background(), fixture.request(t))
	if !errors.Is(err, domain.ErrDialogueAssemblyTargetChanged) {
		t.Fatalf("AssembleDialogue changed-head error = %v, want ErrDialogueAssemblyTargetChanged", err)
	}
}

func TestCOV1DialogueAssemblyStopsLiveContextAtSourceEvent(t *testing.T) {
	fixture := newDialogueAssemblyReadFixture(t)
	result, err := fixture.repository.AssembleDialogue(context.Background(), fixture.request(t))
	if err != nil {
		t.Fatalf("AssembleDialogue: %v", err)
	}

	var currentInputs int
	for _, input := range result.Prepare.Generation.Inputs {
		if input.SourceID == nil {
			continue
		}
		if *input.SourceID == fixture.laterEventID {
			t.Fatalf("event after source seq entered immutable input set: %+v", input)
		}
		if *input.SourceID == fixture.sourceEventID {
			if input.InclusionMode != "current_input" || string(input.Content.Bytes) != "assembly-source" {
				t.Fatalf("source event input = %+v", input)
			}
			currentInputs++
		}
	}
	if currentInputs != 1 {
		t.Fatalf("source current-input count = %d, want 1", currentInputs)
	}
	if result.Prepare.SourceEventID != fixture.sourceEventID || result.Prepare.Target != fixture.target {
		t.Fatalf("assembled source/target = %s / %+v", result.Prepare.SourceEventID, result.Prepare.Target)
	}
	if result.Prepare.Generation.ContextPolicyVersion != domain.DialogueContextPolicyVersionV3 ||
		result.Prepare.Generation.MemoryRenderingVersion != domain.MemoryRenderingVersionV2 {
		t.Fatalf("assembled version tuple = %s / %s",
			result.Prepare.Generation.ContextPolicyVersion,
			result.Prepare.Generation.MemoryRenderingVersion)
	}
}

func TestCOV1DialogueAssemblyRequiresExactServiceWatermarks(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*testing.T, *dialogueAssemblyReadFixture)
	}{
		{
			name: "source head",
			mutate: func(t *testing.T, fixture *dialogueAssemblyReadFixture) {
				mustExec(t, fixture.semantic.db, `UPDATE projection_watermarks
					SET source_commit_seq = source_commit_seq - 1
					WHERE projection_name = ? AND resident_id = ?`,
					projection.ResidentCurrentStatusName, fixture.residentID.String())
			},
		},
		{
			name: "time-sensitive as-of",
			mutate: func(t *testing.T, fixture *dialogueAssemblyReadFixture) {
				mustExec(t, fixture.semantic.db, `UPDATE projection_watermarks
					SET as_of = as_of - 1
					WHERE projection_name = ? AND resident_id = ?`,
					projection.RuntimeStatesName, fixture.residentID.String())
			},
		},
		{
			name: "complete dependency set",
			mutate: func(t *testing.T, fixture *dialogueAssemblyReadFixture) {
				mustExec(t, fixture.semantic.db, `DELETE FROM projection_watermark_dependencies
					WHERE projection_name = ? AND resident_id = ?`,
					projection.ClaimStatesName, fixture.residentID.String())
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			fixture := newDialogueAssemblyReadFixture(t)
			test.mutate(t, fixture)
			_, err := fixture.repository.AssembleDialogue(context.Background(), fixture.request(t))
			if !errors.Is(err, domain.ErrDialogueAssemblyTargetChanged) {
				t.Fatalf("AssembleDialogue watermark error = %v, want ErrDialogueAssemblyTargetChanged", err)
			}
		})
	}
}

func TestCOV3DialogueContextV2DeduplicatesOnlyCompleteFloatingConcreteProvenance(t *testing.T) {
	policy := memory.DefaultPolicyV2()
	firstSource := dialogueAssemblyReadID(t, 700)
	secondSource := dialogueAssemblyReadID(t, 701)
	tests := []struct {
		name          string
		stage         memory.ClaimStage
		abstract      bool
		contextSource []canonical.ID
		wantDedup     bool
	}{
		{
			name: "floating concrete all sources", stage: memory.StageFloating,
			contextSource: []canonical.ID{firstSource, secondSource}, wantDedup: true,
		},
		{
			name: "floating concrete partial sources", stage: memory.StageFloating,
			contextSource: []canonical.ID{firstSource},
		},
		{
			name: "sediment concrete all sources", stage: memory.StageSediment,
			contextSource: []canonical.ID{firstSource, secondSource},
		},
		{
			name: "floating abstract all sources", stage: memory.StageFloating, abstract: true,
			contextSource: []canonical.ID{firstSource, secondSource},
		},
	}
	for index, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			candidate := dialogueAssemblyRecallCandidate(t, 720+index, test.stage, test.abstract,
				[]canonical.ID{firstSource, secondSource})
			selection, err := memory.SelectRecall(policy, []memory.RecallCandidate{candidate})
			if err != nil {
				t.Fatal(err)
			}
			base := make([]dialogueInputCandidate, 0, len(test.contextSource)+1)
			for sourceIndex, sourceID := range test.contextSource {
				copyID := sourceID
				base = append(base, dialogueInputCandidate{
					role: "user", sourceType: "event", sourceID: &copyID,
					inclusionMode: "live_context", content: []byte("source"), byteSize: 6,
					live: true, eventSeq: canonical.Seq(sourceIndex + 1),
				})
			}
			base = append(base, dialogueInputCandidate{
				role: "user", sourceType: "event", inclusionMode: "current_input",
				content: []byte("current"), byteSize: 7,
			})

			assembled, plan, _, _, _, _, _, err := assembleDialogueContextV2(
				base, policy, selection, 1<<20, domain.MaxDialogueInputs,
			)
			if err != nil {
				t.Fatal(err)
			}
			if len(plan.Decisions) != 1 {
				t.Fatalf("decisions = %d, want 1", len(plan.Decisions))
			}
			decision := plan.Decisions[0]
			if test.wantDedup {
				if decision.ExclusionReason != memory.ExclusionProvenanceDuplicateV2 ||
					decision.PromptIncluded || dialogueAssemblyRecallInputCount(assembled) != 0 {
					t.Fatalf("deduplicated plan/input = %+v / %+v", decision, assembled)
				}
				return
			}
			if decision.ExclusionReason != "" || !decision.PromptIncluded ||
				dialogueAssemblyRecallInputCount(assembled) != 1 {
				t.Fatalf("non-deduplicated plan/input = %+v / %+v", decision, assembled)
			}
		})
	}
}

func TestCOV3DialogueContextV2ReevaluatesDedupAfterBudgetDropsSource(t *testing.T) {
	policy := memory.DefaultPolicyV2()
	sourceID := dialogueAssemblyReadID(t, 800)
	candidate := dialogueAssemblyRecallCandidate(
		t, 801, memory.StageFloating, false, []canonical.ID{sourceID},
	)
	selection, err := memory.SelectRecall(policy, []memory.RecallCandidate{candidate})
	if err != nil {
		t.Fatal(err)
	}
	rendered, err := memory.RenderClaim(policy, candidate)
	if err != nil {
		t.Fatal(err)
	}
	copySourceID := sourceID
	base := []dialogueInputCandidate{
		{
			role: "user", sourceType: "event", sourceID: &copySourceID,
			inclusionMode: "live_context", content: bytes.Repeat([]byte("x"), 512), byteSize: 512,
			live: true, eventSeq: 1,
		},
		{
			role: "user", sourceType: "event", inclusionMode: "current_input",
			content: []byte("q"), byteSize: 1,
		},
	}
	maximum := int64(len([]byte(rendered)) + 1)
	assembled, plan, droppedRecall, exceeded, droppedBackfill, droppedLive, droppedInitiative, err :=
		assembleDialogueContextV2(base, policy, selection, maximum, domain.MaxDialogueInputs)
	if err != nil {
		t.Fatal(err)
	}
	if !exceeded || droppedLive != 1 || droppedBackfill != 0 || droppedInitiative != 0 ||
		len(droppedRecall) != 0 {
		t.Fatalf("budget result exceeded=%v backfill=%d live=%d initiative=%d recall=%v",
			exceeded, droppedBackfill, droppedLive, droppedInitiative, droppedRecall)
	}
	if len(plan.Decisions) != 1 || plan.Decisions[0].ExclusionReason != "" ||
		!plan.Decisions[0].PromptIncluded || dialogueAssemblyRecallInputCount(assembled) != 1 {
		t.Fatalf("re-evaluated plan/input = %+v / %+v", plan, assembled)
	}
	for _, input := range assembled {
		if input.sourceID != nil && *input.sourceID == sourceID {
			t.Fatalf("budget-dropped provenance source survived final context: %+v", input)
		}
	}
}

func TestCOV5AutomaticBackfillExchangeIsDroppedAtomically(t *testing.T) {
	firstID := dialogueAssemblyReadID(t, 820)
	secondID := dialogueAssemblyReadID(t, 821)
	base := []dialogueInputCandidate{
		{
			role: "user", sourceType: "event", sourceID: &firstID,
			inclusionMode: "context_backfill", content: bytes.Repeat([]byte("u"), 8), byteSize: 8,
			backfill: true, atomicBackfill: true, eventSeq: 1,
		},
		{
			role: "assistant", sourceType: "event", sourceID: &secondID,
			inclusionMode: "context_backfill", content: bytes.Repeat([]byte("a"), 8), byteSize: 8,
			backfill: true, atomicBackfill: true, eventSeq: 2,
		},
		{role: "user", sourceType: "event", inclusionMode: "current_input", content: []byte("q"), byteSize: 1},
	}
	kept, exceeded, droppedBackfill, droppedLive, droppedInitiative, droppedRecall, err :=
		applyDialogueBudget(base, 9, domain.MaxDialogueInputs)
	if err != nil {
		t.Fatal(err)
	}
	if !exceeded || droppedBackfill != 2 || droppedLive != 0 || droppedInitiative != 0 ||
		len(droppedRecall) != 0 || len(kept) != 1 || kept[0].inclusionMode != "current_input" {
		t.Fatalf("atomic Backfill budget = kept:%+v exceeded:%v backfill:%d live:%d initiative:%d recall:%v",
			kept, exceeded, droppedBackfill, droppedLive, droppedInitiative, droppedRecall)
	}
}

func TestCOV5RecallDedupReachesFixedPointAfterAtomicBackfillWithdrawal(t *testing.T) {
	policy := memory.DefaultPolicyV2()
	firstID := dialogueAssemblyReadID(t, 830)
	secondID := dialogueAssemblyReadID(t, 831)
	candidate := dialogueAssemblyRecallCandidate(t, 832, memory.StageFloating, false,
		[]canonical.ID{firstID, secondID})
	selection, err := memory.SelectRecall(policy, []memory.RecallCandidate{candidate})
	if err != nil {
		t.Fatal(err)
	}
	rendered, err := memory.RenderClaim(policy, candidate)
	if err != nil {
		t.Fatal(err)
	}
	base := []dialogueInputCandidate{
		{
			role: "user", sourceType: "event", sourceID: &firstID,
			inclusionMode: "context_backfill", content: bytes.Repeat([]byte("u"), 256), byteSize: 256,
			backfill: true, atomicBackfill: true, eventSeq: 1,
		},
		{
			role: "assistant", sourceType: "event", sourceID: &secondID,
			inclusionMode: "context_backfill", content: bytes.Repeat([]byte("a"), 256), byteSize: 256,
			backfill: true, atomicBackfill: true, eventSeq: 2,
		},
		{role: "user", sourceType: "event", inclusionMode: "current_input", content: []byte("q"), byteSize: 1},
	}
	maximum := int64(len([]byte(rendered)) + 1)
	assembled, plan, droppedRecall, exceeded, droppedBackfill, droppedLive, droppedInitiative, err :=
		assembleDialogueContextV2(base, policy, selection, maximum, domain.MaxDialogueInputs)
	if err != nil {
		t.Fatal(err)
	}
	if !exceeded || droppedBackfill != 2 || droppedLive != 0 || droppedInitiative != 0 || len(droppedRecall) != 0 {
		t.Fatalf("fixed-point drops exceeded=%v backfill=%d live=%d initiative=%d recall=%v",
			exceeded, droppedBackfill, droppedLive, droppedInitiative, droppedRecall)
	}
	if len(plan.Decisions) != 1 || plan.Decisions[0].ExclusionReason != "" ||
		!plan.Decisions[0].PromptIncluded || dialogueAssemblyRecallInputCount(assembled) != 1 {
		t.Fatalf("fixed-point Recall plan/input = %+v / %+v", plan, assembled)
	}
	for _, input := range assembled {
		if input.atomicBackfill || input.backfill {
			t.Fatalf("partial automatic Backfill survived fixed point: %+v", input)
		}
	}
}

func TestDialogueRecallCandidateSnapshotsCloneStructuredRenderingInputs(t *testing.T) {
	candidate := dialogueAssemblyRecallCandidate(t, 840, memory.StageFloating, false, nil)
	one, err := canonical.NewRatio(canonical.FixedPointScale)
	if err != nil {
		t.Fatal(err)
	}
	wantConfirmed := canonical.Instant(1234)
	confirmed := wantConfirmed
	candidate.Currentness = one
	candidate.LastConfirmed = &confirmed
	selection, err := memory.SelectRecall(memory.DefaultPolicyV2(), []memory.RecallCandidate{candidate})
	if err != nil {
		t.Fatal(err)
	}
	snapshots := dialogueRecallCandidateSnapshots(selection)
	if len(snapshots) != 1 || snapshots[0].Currentness != one || snapshots[0].LastConfirmed == nil ||
		*snapshots[0].LastConfirmed != wantConfirmed {
		t.Fatalf("structured Recall candidate snapshot = %+v", snapshots)
	}
	changed := canonical.Instant(9999)
	*selection.Candidates[0].Candidate.LastConfirmed = changed
	if *snapshots[0].LastConfirmed != wantConfirmed {
		t.Fatalf("LastConfirmed snapshot aliases candidate: got %s want %s", *snapshots[0].LastConfirmed, wantConfirmed)
	}
}

type dialogueAssemblyReadFixture struct {
	semantic      *semanticFixture
	repository    *CanonicalRepository
	residentID    canonical.ID
	sourceEventID canonical.ID
	laterEventID  canonical.ID
	target        domain.AssemblyTarget
}

func newDialogueAssemblyReadFixture(t *testing.T) *dialogueAssemblyReadFixture {
	t.Helper()
	semantic, closeFixture := newSemanticFixture(t)
	t.Cleanup(closeFixture)
	residentID := dialogueAssemblyReadParseID(t, semantic.resident["A"])

	pipelineID := dialogueAssemblyReadParseID(t, semantic.ids.new())
	pipeline, err := domain.DialoguePipelineDefinition(pipelineID, domain.DialoguePipelineVersionV3)
	if err != nil {
		t.Fatal(err)
	}
	mustExec(t, semantic.db, `INSERT INTO pipeline_versions(
		pipeline_version_id, canonical_commit_id, pipeline_kind, version_key,
		definition, recorded_at, recorded_tz
	) VALUES (?, ?, ?, ?, ?, ?, ?)`, pipeline.ID.String(), semantic.commit["global"], pipeline.Kind,
		pipeline.VersionKey, pipeline.Definition.String(), semanticTime, semanticTZ)

	sessionID := semantic.ids.new()
	mustExec(t, semantic.db, `INSERT INTO sessionization_policy_versions(
		sessionization_policy_version_id, canonical_commit_id, version_key,
		definition, recorded_at, recorded_tz
	) VALUES (?, ?, ?, ?, ?, ?)`, sessionID, semantic.commit["global"], domain.SessionPolicyVersion,
		`{"idle_gap_microseconds":"1800000000","version":"sessionization-v1"}`,
		semanticTime, semanticTZ)

	activationCommit := semantic.ids.new()
	sourceCommit := semantic.ids.new()
	laterCommit := semantic.ids.new()
	mustExec(t, semantic.db, `INSERT INTO canonical_commits VALUES (?, 4, ?, ?, ?)`,
		activationCommit, residentID.String(), semanticTime+3, semanticTZ)
	mustExec(t, semantic.db, `INSERT INTO canonical_commits VALUES (?, 5, ?, ?, ?)`,
		sourceCommit, residentID.String(), semanticTime+4, semanticTZ)
	mustExec(t, semantic.db, `INSERT INTO canonical_commits VALUES (?, 6, ?, ?, ?)`,
		laterCommit, residentID.String(), semanticTime+5, semanticTZ)

	mustExec(t, semantic.db, `INSERT INTO resident_status_transitions(
		resident_status_transition_id, canonical_commit_id, resident_id, from_status, to_status,
		actor_principal_id, reason_code, reason_content_id, occurred_at, occurred_tz,
		recorded_at, recorded_tz
	) VALUES (?, ?, ?, NULL, 'active', ?, 'activate', NULL, ?, ?, ?, ?)`,
		semantic.ids.new(), activationCommit, residentID.String(), semantic.principal["human"],
		semanticTime+3, semanticTZ, semanticTime+3, semanticTZ)
	for _, class := range []string{"principles", "persona", "memory_policy"} {
		mustExec(t, semantic.db, `INSERT INTO resident_revision_activations(
			activation_id, canonical_commit_id, resident_id, revision_id,
			actor_principal_id, approval_id, reason_code, reason_content_id,
			recorded_at, recorded_tz
		) VALUES (?, ?, ?, ?, ?, NULL, 'activate', NULL, ?, ?)`, semantic.ids.new(), activationCommit,
			residentID.String(), semantic.revision["A"][class], semantic.principal["human"],
			semanticTime+3, semanticTZ)
	}
	mustExec(t, semantic.db, `INSERT INTO runtime_config(
		singleton_id, active_resident_id, desired_sessionization_policy_version_id,
		updated_at, updated_tz
	) VALUES (1, ?, ?, ?, ?)`, residentID.String(), sessionID, semanticTime+3, semanticTZ)

	var previousHash []byte
	if err := semantic.db.QueryRow(`SELECT event_hash FROM events
		WHERE resident_id = ? ORDER BY seq DESC LIMIT 1`, residentID.String()).Scan(&previousHash); err != nil {
		t.Fatal(err)
	}
	sourceEventID := dialogueAssemblyReadParseID(t, semantic.ids.new())
	sourceContentID := semantic.addContent(t, "A", "event_payload", "assembly-source", "independent")
	sourceHash := semanticDigest("dialogue-assembly-source-event")
	insertDialogueAssemblyReadEvent(t, semantic, sourceEventID, sourceCommit, 2,
		sourceContentID, "assembly-source", previousHash, sourceHash, semanticTime+4)

	laterEventID := dialogueAssemblyReadParseID(t, semantic.ids.new())
	laterContentID := semantic.addContent(t, "A", "event_payload", "must-not-enter-source-snapshot", "independent")
	laterHash := semanticDigest("dialogue-assembly-later-event")
	insertDialogueAssemblyReadEvent(t, semantic, laterEventID, laterCommit, 3,
		laterContentID, "must-not-enter-source-snapshot", sourceHash, laterHash, semanticTime+5)

	target := domain.AssemblyTarget{
		Head: canonical.Head{
			Exists: true, CommitSeq: canonical.CommitSeq(6), CommittedAt: canonical.Instant(semanticTime + 5),
		},
		AsOf: canonical.Instant(semanticTime + 5), AsOfTZ: canonical.MustTimezone(semanticTZ),
	}
	service := make(map[projection.Name]struct{}, len(projection.ServiceRequiredNames()))
	for _, name := range projection.ServiceRequiredNames() {
		service[name] = struct{}{}
	}
	for _, definition := range activeProjectionDefinitions {
		if _, required := service[definition.Name]; !required {
			continue
		}
		mustExec(t, semantic.db, `INSERT INTO projection_watermarks(
			projection_name, resident_id, projection_version, source_commit_seq, as_of, as_of_tz
		) VALUES (?, ?, ?, ?, ?, ?)`, definition.Name, residentID.String(), definition.Version,
			target.Head.CommitSeq.Int64(), target.AsOf.UnixMicro(), target.AsOfTZ.String())
		for _, dependency := range definition.Dependencies {
			var versionID string
			switch dependency {
			case projection.MemoryPolicyDependency:
				versionID = semantic.revision["A"]["memory_policy"]
			case projection.SessionizationPolicyDependency:
				versionID = sessionID
			default:
				t.Fatalf("unsupported test Projection dependency %q", dependency)
			}
			mustExec(t, semantic.db, `INSERT INTO projection_watermark_dependencies(
				projection_name, resident_id, dependency_kind, dependency_version_id
			) VALUES (?, ?, ?, ?)`, definition.Name, residentID.String(), dependency, versionID)
		}
	}

	return &dialogueAssemblyReadFixture{
		semantic: semantic, repository: fixtureStoreRepository(semantic), residentID: residentID,
		sourceEventID: sourceEventID, laterEventID: laterEventID, target: target,
	}
}

func (fixture *dialogueAssemblyReadFixture) request(t *testing.T) domain.DialogueAssemblyRequest {
	t.Helper()
	ids := make([]canonical.ID, 5+2*domain.MaxDialogueInputs+domain.MaxRecallUsages)
	for index := range ids {
		ids[index] = dialogueAssemblyReadParseID(t, fixture.semantic.ids.new())
	}
	salts := make([]canonical.ContentSalt, domain.MaxDialogueInputs)
	for index := range salts {
		salts[index] = canonical.ContentSalt{byte(index + 1)}
	}
	_, params, err := domain.NewUnstructuredGeneratorParams(false, canonical.ByteSize(1024))
	if err != nil {
		t.Fatal(err)
	}
	return domain.DialogueAssemblyRequest{
		SourceEventID: fixture.sourceEventID, ResidentID: fixture.residentID, Target: fixture.target,
		RunID: ids[0], RunningOutcomeID: ids[1], RecallRunID: ids[2],
		InputIDs:          ids[3 : 3+domain.MaxDialogueInputs],
		InputContentIDs:   ids[3+domain.MaxDialogueInputs : 3+2*domain.MaxDialogueInputs],
		InputContentSalts: salts,
		RecallUsageIDs:    ids[3+2*domain.MaxDialogueInputs : 3+2*domain.MaxDialogueInputs+domain.MaxRecallUsages],
		Provider:          "test", Model: "test-model", GeneratorParams: params,
		MaxInputBytes: 1 << 20, LiveEventLimit: domain.DialogueLiveEventLimit,
	}
}

func insertDialogueAssemblyReadEvent(
	t *testing.T,
	fixture *semanticFixture,
	eventID canonical.ID,
	commitID string,
	seq int64,
	contentID, content string,
	previousHash, eventHash []byte,
	recordedAt int64,
) {
	t.Helper()
	mustExec(t, fixture.db, `INSERT INTO events(
		event_id, canonical_commit_id, resident_id, seq, event_type, visibility,
		delivery_screen, delivery_audio, ingress, trust_level, actor_principal_id,
		target_principal_id, generation_run_id, occurred_at, occurred_tz, recorded_at,
		recorded_tz, content_id, payload_commitment, prev_event_hash, event_hash,
		event_hash_algorithm, event_hash_domain, canonicalization_version
	) VALUES (?, ?, ?, ?, 'user_message', 'conversation', 1, 0, 'local_ui', 'trusted',
		?, ?, NULL, ?, ?, ?, ?, ?, ?, ?, ?, 'sha256', 'mahoroba:event-hash:v1', 'mahoroba-jcs-v1')`,
		eventID.String(), commitID, fixture.resident["A"], seq, fixture.principal["human"],
		fixture.principal["A"], recordedAt, semanticTZ, recordedAt, semanticTZ, contentID,
		semanticDigest("payload:"+content), previousHash, eventHash)
}

func dialogueAssemblyRecallCandidate(
	t *testing.T,
	id int,
	stage memory.ClaimStage,
	abstract bool,
	sources []canonical.ID,
) memory.RecallCandidate {
	t.Helper()
	one, err := canonical.NewRatio(canonical.FixedPointScale)
	if err != nil {
		t.Fatal(err)
	}
	return memory.RecallCandidate{
		ClaimID: dialogueAssemblyReadID(t, id), Statement: "remember this",
		ContextCompatibility: one, Salience: one, Confidence: one,
		Status: memory.StatusActive, Stage: stage, TemporalRelation: memory.RelationCurrent,
		SourceEventIDs: append([]canonical.ID(nil), sources...), Abstract: abstract,
	}
}

func dialogueAssemblyRecallInputCount(inputs []dialogueInputCandidate) int {
	count := 0
	for _, input := range inputs {
		if input.inclusionMode == "memory_recall" {
			count++
		}
	}
	return count
}

func dialogueAssemblyReadID(t *testing.T, value int) canonical.ID {
	t.Helper()
	return dialogueAssemblyReadParseID(t, leftPadDecimal(value, 26))
}

func dialogueAssemblyReadParseID(t *testing.T, raw string) canonical.ID {
	t.Helper()
	id, err := canonical.ParseID(raw)
	if err != nil {
		t.Fatal(err)
	}
	return id
}

func leftPadDecimal(value, width int) string {
	raw := []byte{}
	for value > 0 {
		raw = append([]byte{byte('0' + value%10)}, raw...)
		value /= 10
	}
	if len(raw) == 0 {
		raw = []byte{'0'}
	}
	if padding := width - len(raw); padding > 0 {
		raw = append(bytes.Repeat([]byte{'0'}, padding), raw...)
	}
	return string(raw)
}
