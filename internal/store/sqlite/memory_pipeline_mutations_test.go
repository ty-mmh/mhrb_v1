package sqlite

import (
	"context"
	"errors"
	"testing"

	"mahoroba.local/mahoroba/internal/canonical"
	"mahoroba.local/mahoroba/internal/domain"
)

func TestM5PipelineRegistrationIsIdempotentAndFailsClosedOnConflict(t *testing.T) {
	fixture, closeFixture := newSemanticFixture(t)
	defer closeFixture()
	ctx := context.Background()
	ids := make([]canonical.ID, 8)
	for index := range ids {
		ids[index] = projectionTestID(t, fixture.ids.new())
	}
	definitions, err := domain.MemoryPipelineDefinitions(ids)
	if err != nil {
		t.Fatal(err)
	}

	commitID := fixture.ids.new()
	mustExec(t, fixture.db, `INSERT INTO canonical_commits(
		canonical_commit_id, commit_seq, resident_id, committed_at, committed_tz
	) VALUES (?, 4, NULL, ?, ?)`, commitID, semanticTime+3, semanticTZ)
	tx, err := fixture.db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	uow := &canonicalUoW{tx: tx, metadata: canonical.CommitMetadata{
		CommitID: projectionTestID(t, commitID), CommitSeq: mustCommitSeq(t, 4),
		Scope: canonical.GlobalScope(), CommittedAt: semanticInstant(semanticTime + 3),
		CommittedTZ: canonical.Timezone(semanticTZ),
	}}
	if err := uow.RegisterPipelineVersions(ctx, definitions); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}

	tx, err = fixture.db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	uow = &canonicalUoW{tx: tx, metadata: uow.metadata}
	if err := uow.RegisterPipelineVersions(ctx, definitions); !errors.Is(err, canonical.ErrNoMutation) {
		t.Fatalf("identical registration = %v, want no mutation", err)
	}
	_ = tx.Rollback()

	conflict, err := canonical.MarshalCanonical(struct {
		Version string `json:"version"`
	}{Version: "conflicting-v2"})
	if err != nil {
		t.Fatal(err)
	}
	definitions.Versions[0].Definition = conflict
	tx, err = fixture.db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	uow = &canonicalUoW{tx: tx, metadata: uow.metadata}
	if err := uow.RegisterPipelineVersions(ctx, definitions); err == nil {
		t.Fatal("conflicting pipeline definition unexpectedly succeeded")
	}
	_ = tx.Rollback()
}

func TestCOV2DialogueV2RegistrationRequiresExactDefinitionAndPreservesV1(t *testing.T) {
	fixture, closeFixture := newSemanticFixture(t)
	defer closeFixture()
	ctx := context.Background()
	legacyID := projectionTestID(t, fixture.ids.new())
	legacy, err := domain.DialoguePipelineDefinition(legacyID, domain.DialoguePipelineVersionV1)
	if err != nil {
		t.Fatal(err)
	}
	mustExec(t, fixture.db, `INSERT INTO pipeline_versions(
		pipeline_version_id, canonical_commit_id, pipeline_kind, version_key, definition, recorded_at, recorded_tz
	) VALUES (?, ?, ?, ?, ?, ?, ?)`, legacy.ID.String(), fixture.commit["A"], legacy.Kind, legacy.VersionKey,
		legacy.Definition.String(), semanticTime, semanticTZ)
	commitID := fixture.ids.new()
	mustExec(t, fixture.db, `INSERT INTO canonical_commits(
		canonical_commit_id, commit_seq, resident_id, committed_at, committed_tz
	) VALUES (?, 4, NULL, ?, ?)`, commitID, semanticTime+3, semanticTZ)
	metadata := canonical.CommitMetadata{
		CommitID: projectionTestID(t, commitID), CommitSeq: mustCommitSeq(t, 4), Scope: canonical.GlobalScope(),
		CommittedAt: semanticInstant(semanticTime + 3), CommittedTZ: canonical.Timezone(semanticTZ),
	}
	versionID := projectionTestID(t, fixture.ids.new())
	exact, err := domain.DialoguePipelineDefinition(versionID, domain.DialoguePipelineVersionV2)
	if err != nil {
		t.Fatal(err)
	}
	tamperedJSON, err := canonical.MarshalCanonical(struct {
		Purpose string `json:"purpose"`
		Version string `json:"version"`
	}{Purpose: "dialogue", Version: "dialogue-v2-tampered"})
	if err != nil {
		t.Fatal(err)
	}
	tampered := exact
	tampered.Definition = tamperedJSON

	tx, err := fixture.db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	uow := &canonicalUoW{tx: tx, metadata: metadata}
	if err := uow.RegisterPipelineVersions(ctx, domain.RegisterPipelineVersions{Versions: []domain.PipelineVersionDefinition{tampered}}); err == nil {
		t.Fatal("tampered dialogue-v2 definition was accepted")
	}
	_ = tx.Rollback()
	var count int
	if err := fixture.db.QueryRow(`SELECT count(*) FROM pipeline_versions WHERE pipeline_kind='dialogue' AND version_key=?`,
		domain.DialoguePipelineVersionV2).Scan(&count); err != nil || count != 0 {
		t.Fatalf("tampered registration count/error = %d / %v", count, err)
	}

	tx, err = fixture.db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	uow = &canonicalUoW{tx: tx, metadata: metadata}
	if err := uow.RegisterPipelineVersions(ctx, domain.RegisterPipelineVersions{Versions: []domain.PipelineVersionDefinition{exact}}); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	if err := fixture.db.QueryRow(`SELECT count(*) FROM pipeline_versions WHERE pipeline_kind='dialogue'`).Scan(&count); err != nil || count != 2 {
		t.Fatalf("dialogue pipeline rows/error = %d / %v, want legacy+current", count, err)
	}
}
