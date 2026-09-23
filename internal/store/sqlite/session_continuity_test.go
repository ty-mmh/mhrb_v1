package sqlite

import (
	"context"
	"errors"
	"reflect"
	"testing"

	"mahoroba.local/mahoroba/internal/canonical"
	"mahoroba.local/mahoroba/internal/domain"
	"mahoroba.local/mahoroba/internal/memory"
)

func TestSessionContinuityRejectsDifferentParticipantAndResident(t *testing.T) {
	fixture := newDialoguePrepareWriterFixtureWithPolicyAndSource(t, memory.DefaultPolicyV1(), []byte("ordinary question"), true)
	ctx := context.Background()
	tx, err := fixture.semantic.db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	current, _, err := loadDialogueAssemblySource(ctx, tx, fixture.request(t))
	if err != nil {
		t.Fatal(err)
	}
	uow := &canonicalUoW{tx: tx, metadata: canonical.CommitMetadata{CommitSeq: fixture.target.Head.CommitSeq}}
	start := current.Seq
	got, err := uow.loadAutomaticDialogueBackfill(ctx, current, start)
	if err != nil || len(got) != 1 {
		t.Fatalf("valid prior user = %v / %v", got, err)
	}
	for _, change := range []func(*domain.Event){
		func(event *domain.Event) { event.ActorPrincipalID = fixture.id(t) },
		func(event *domain.Event) { id := fixture.id(t); event.TargetPrincipalID = &id },
		func(event *domain.Event) { event.ResidentID = fixture.id(t) },
	} {
		other := current
		change(&other)
		got, err := uow.loadAutomaticDialogueBackfill(ctx, other, start)
		if err != nil || len(got) != 0 {
			t.Fatalf("different participant/resident leaked context: %v / %v", got, err)
		}
	}
}

func TestSessionContinuityV3FrozenRunRemainsReplayable(t *testing.T) {
	for _, policy := range []memory.Policy{memory.DefaultPolicyV1(), memory.DefaultPolicyV4()} {
		t.Run(string(policy.Version), func(t *testing.T) {
			fixture := newDialoguePrepareWriterFixtureWithPolicyAndSource(t, policy, []byte("continue"), true)
			ctx := context.Background()
			prepare := fixture.assemble(t)
			uow := fixture.begin(t, 9, fixture.target.AsOf+1)
			result, err := uow.PrepareDialogue(ctx, prepare)
			if err != nil {
				_ = uow.Rollback(ctx)
				t.Fatal(err)
			}
			if err := uow.Commit(ctx); err != nil {
				t.Fatal(err)
			}
			// This source has a surface-reference marker, so its frozen input set is
			// also exactly the pre-upgrade v3 selection. Seed only the historical tuple.
			priorID := fixture.id(t)
			prior, err := domain.DialoguePipelineDefinition(priorID, domain.DialoguePipelineVersionV3)
			if err != nil {
				t.Fatal(err)
			}
			mustExec(t, fixture.semantic.db, `INSERT INTO pipeline_versions(
	 pipeline_version_id, canonical_commit_id, pipeline_kind, version_key, definition, recorded_at, recorded_tz
	) VALUES (?, ?, 'dialogue', ?, ?, ?, ?)`, priorID.String(), fixture.semantic.commit["global"], prior.VersionKey, prior.Definition.String(), semanticTime, semanticTZ)
			mustExec(t, fixture.semantic.db, `DROP TRIGGER trg_generation_runs_no_update`)
			mustExec(t, fixture.semantic.db, `UPDATE generation_runs SET pipeline_version_id = ?, context_policy_version = ?
	 WHERE generation_run_id = ?`, priorID.String(), domain.DialogueContextPolicyVersionV3, result.RunID.String())
			before, err := fixture.repository.Generation(ctx, result.RunID)
			if err != nil {
				t.Fatal(err)
			}
			if before.ContextPolicyVersion != domain.DialogueContextPolicyVersionV3 {
				t.Fatal("fixture did not retain v3")
			}

			// A current v4 proposal for the same obligation must return the frozen run.
			replay := prepare
			replay.Generation.RunID = fixture.id(t)
			replay.Generation.RunningOutcomeID = fixture.id(t)
			uow = fixture.begin(t, 10, fixture.target.AsOf+2)
			defer uow.Rollback(ctx)
			actual, err := uow.PrepareDialogue(ctx, replay)
			if !errors.Is(err, canonical.ErrNoMutation) || actual.RunID != result.RunID || actual.Resolution != domain.PrepareDialogueDispatchExistingFrozenRun {
				t.Fatalf("v3 replay = %+v / %v", actual, err)
			}
			if err := uow.revalidateDialogueInputsAtLanding(ctx, result.RunID, fixture.residentID); err != nil {
				t.Fatalf("v3 landing: %v", err)
			}
			if err := uow.Rollback(ctx); err != nil {
				t.Fatal(err)
			}
			after, err := fixture.repository.Generation(ctx, result.RunID)
			if err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(before, after) {
				t.Fatal("v3 replay changed frozen inputs or version tuple")
			}
		})
	}
}

func TestSessionContinuityBackfillBudgetDropsWholeExchange(t *testing.T) {
	first, _ := canonical.NewSeq(1)
	second, _ := canonical.NewSeq(2)
	input := []dialogueInputCandidate{
		{sourceType: "event", role: "user", inclusionMode: "context_backfill", byteSize: 3, eventSeq: first, backfill: true, atomicBackfill: true},
		{sourceType: "event", role: "assistant", inclusionMode: "context_backfill", byteSize: 3, eventSeq: second, backfill: true, atomicBackfill: true},
		{sourceType: "event", role: "user", inclusionMode: "current_input", byteSize: 4},
	}
	kept, exceeded, dropped, _, _, _, err := applyDialogueBudget(input, 8, domain.MaxDialogueInputs)
	if err != nil || !exceeded || dropped != 2 || len(kept) != 1 || kept[0].inclusionMode != "current_input" {
		t.Fatalf("partial prior exchange survived budget: kept=%v exceeded=%t dropped=%d err=%v", kept, exceeded, dropped, err)
	}
}
