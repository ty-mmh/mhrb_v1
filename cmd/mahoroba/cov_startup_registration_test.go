package main

import (
	"context"
	"database/sql"
	"path/filepath"
	"testing"

	"mahoroba.local/mahoroba/internal/canonical"
	"mahoroba.local/mahoroba/internal/domain"
	store "mahoroba.local/mahoroba/internal/store/sqlite"
)

func TestCOV56StartupRegistrationPreservesV1V2AndIsIdempotent(t *testing.T) {
	ctx := context.Background()
	database, err := store.Open(ctx, filepath.Join(t.TempDir(), "startup-registration.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()

	ids := canonical.NewSecureIDGenerator()
	writer, err := canonical.OpenWriter(ctx, canonical.WriterOptions{
		Backend: database.Canonical(), IDs: ids, Clock: canonical.SystemClock{},
		Timezone: canonical.MustTimezone("UTC"), QueueCapacity: 4,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := writer.Close(context.Background()); err != nil {
			t.Error(err)
		}
	}()

	legacyID, err := ids.New()
	if err != nil {
		t.Fatal(err)
	}
	legacy, err := domain.DialoguePipelineDefinition(legacyID, domain.DialoguePipelineVersionV1)
	if err != nil {
		t.Fatal(err)
	}
	priorSplitID, err := ids.New()
	if err != nil {
		t.Fatal(err)
	}
	priorSplit, err := domain.DialoguePipelineDefinition(priorSplitID, domain.DialoguePipelineVersionV2)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := writer.Submit(ctx, domain.RegisterPipelineVersionsCommand(domain.RegisterPipelineVersions{
		Versions: []domain.PipelineVersionDefinition{legacy, priorSplit},
	})); err != nil {
		t.Fatal(err)
	}

	if err := registerDialogueV3(ctx, writer, ids); err != nil {
		t.Fatal(err)
	}
	var commitsAfterFirst int
	if err := database.Reader().QueryRowContext(ctx, `SELECT count(*) FROM canonical_commits`).Scan(&commitsAfterFirst); err != nil {
		t.Fatal(err)
	}
	if err := registerDialogueV3(ctx, writer, ids); err != nil {
		t.Fatalf("idempotent startup registration: %v", err)
	}

	var dialogueRows, commitsAfterRetry int
	if err := database.Reader().QueryRowContext(ctx, `SELECT count(*) FROM pipeline_versions WHERE pipeline_kind = 'dialogue'`).Scan(&dialogueRows); err != nil {
		t.Fatal(err)
	}
	if err := database.Reader().QueryRowContext(ctx, `SELECT count(*) FROM canonical_commits`).Scan(&commitsAfterRetry); err != nil {
		t.Fatal(err)
	}
	if dialogueRows != 3 || commitsAfterFirst != 2 || commitsAfterRetry != commitsAfterFirst {
		t.Fatalf("registration state = rows %d / first commits %d / retry commits %d, want 3 / 2 / 2",
			dialogueRows, commitsAfterFirst, commitsAfterRetry)
	}

	var storedLegacyID, currentDefinition string
	if err := database.Reader().QueryRowContext(ctx, `SELECT pipeline_version_id FROM pipeline_versions
		WHERE pipeline_kind = 'dialogue' AND version_key = ?`, domain.DialoguePipelineVersionV1).Scan(&storedLegacyID); err != nil {
		t.Fatal(err)
	}
	if storedLegacyID != legacyID.String() {
		t.Fatalf("legacy pipeline ID = %s, want preserved %s", storedLegacyID, legacyID)
	}
	var storedPriorSplitID string
	if err := database.Reader().QueryRowContext(ctx, `SELECT pipeline_version_id FROM pipeline_versions
		WHERE pipeline_kind = 'dialogue' AND version_key = ?`, domain.DialoguePipelineVersionV2).Scan(&storedPriorSplitID); err != nil {
		t.Fatal(err)
	}
	if storedPriorSplitID != priorSplitID.String() {
		t.Fatalf("prior-split pipeline ID = %s, want preserved %s", storedPriorSplitID, priorSplitID)
	}
	if err := database.Reader().QueryRowContext(ctx, `SELECT definition FROM pipeline_versions
		WHERE pipeline_kind = 'dialogue' AND version_key = ?`, domain.DialoguePipelineVersionV3).Scan(&currentDefinition); err != nil {
		t.Fatal(err)
	}
	expectedCurrent, err := domain.DialoguePipelineDefinition(legacyID, domain.DialoguePipelineVersionV3)
	if err != nil {
		t.Fatal(err)
	}
	if currentDefinition != expectedCurrent.Definition.String() {
		t.Fatalf("dialogue-v3 definition = %s, want exact %s", currentDefinition, expectedCurrent.Definition.String())
	}
}

func TestCOV56StartupRegistrationRejectsConflictingDialogueV3(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "startup-conflict.db")
	database, err := store.Open(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()

	ids := canonical.NewSecureIDGenerator()
	writer, err := canonical.OpenWriter(ctx, canonical.WriterOptions{
		Backend: database.Canonical(), IDs: ids, Clock: canonical.SystemClock{},
		Timezone: canonical.MustTimezone("UTC"), QueueCapacity: 4,
	})
	if err != nil {
		t.Fatal(err)
	}
	legacyID, err := ids.New()
	if err != nil {
		t.Fatal(err)
	}
	legacy, err := domain.DialoguePipelineDefinition(legacyID, domain.DialoguePipelineVersionV1)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := writer.Submit(ctx, domain.RegisterPipelineVersionsCommand(domain.RegisterPipelineVersions{
		Versions: []domain.PipelineVersionDefinition{legacy},
	})); err != nil {
		t.Fatal(err)
	}
	if err := writer.Close(ctx); err != nil {
		t.Fatal(err)
	}

	var commitID, recordedTZ string
	var recordedAt int64
	if err := database.Reader().QueryRowContext(ctx, `SELECT canonical_commit_id, recorded_at, recorded_tz
		FROM pipeline_versions WHERE pipeline_kind = 'dialogue' AND version_key = ?`,
		domain.DialoguePipelineVersionV1).Scan(&commitID, &recordedAt, &recordedTZ); err != nil {
		t.Fatal(err)
	}
	conflicting, err := canonical.MarshalCanonical(struct {
		Purpose string `json:"purpose"`
		Version string `json:"version"`
	}{Purpose: "dialogue", Version: "dialogue-v3-conflict"})
	if err != nil {
		t.Fatal(err)
	}
	conflictingID, err := ids.New()
	if err != nil {
		t.Fatal(err)
	}
	raw, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := raw.ExecContext(ctx, `INSERT INTO pipeline_versions(
		pipeline_version_id, canonical_commit_id, pipeline_kind, version_key,
		definition, recorded_at, recorded_tz
	) VALUES (?, ?, 'dialogue', ?, ?, ?, ?)`, conflictingID.String(), commitID,
		domain.DialoguePipelineVersionV3, conflicting.String(), recordedAt, recordedTZ); err != nil {
		_ = raw.Close()
		t.Fatal(err)
	}
	if err := raw.Close(); err != nil {
		t.Fatal(err)
	}

	writer, err = canonical.OpenWriter(ctx, canonical.WriterOptions{
		Backend: database.Canonical(), IDs: ids, Clock: canonical.SystemClock{},
		Timezone: canonical.MustTimezone("UTC"), QueueCapacity: 4,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := writer.Close(context.Background()); err != nil {
			t.Error(err)
		}
	}()
	if err := registerDialogueV3(ctx, writer, ids); err == nil {
		t.Fatal("startup accepted a conflicting dialogue-v3 definition")
	}

	var commits int
	var storedDefinition string
	if err := database.Reader().QueryRowContext(ctx, `SELECT count(*) FROM canonical_commits`).Scan(&commits); err != nil {
		t.Fatal(err)
	}
	if err := database.Reader().QueryRowContext(ctx, `SELECT definition FROM pipeline_versions
		WHERE pipeline_kind = 'dialogue' AND version_key = ?`, domain.DialoguePipelineVersionV3).Scan(&storedDefinition); err != nil {
		t.Fatal(err)
	}
	if commits != 1 || storedDefinition != conflicting.String() {
		t.Fatalf("conflict rollback state = commits %d / definition %s", commits, storedDefinition)
	}
}
