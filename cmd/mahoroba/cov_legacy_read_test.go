package main

import (
	"bytes"
	"context"
	"database/sql"
	"strings"
	"testing"
	"time"

	"mahoroba.local/mahoroba/internal/app"
	"mahoroba.local/mahoroba/internal/blob"
	"mahoroba.local/mahoroba/internal/canonical"
	"mahoroba.local/mahoroba/internal/domain"
	store "mahoroba.local/mahoroba/internal/store/sqlite"
)

func TestCOV2LedgerVerifyReadsLegacyV1OnlyDatabaseAndRejectsUnknownResident(t *testing.T) {
	ctx := context.Background()
	cfg := commandRuntimeConfig(t.TempDir())
	database, err := store.Open(ctx, cfg.DatabasePath())
	if err != nil {
		t.Fatal(err)
	}
	objects, err := blob.NewFileStore(cfg.BlobPath())
	if err != nil {
		_ = database.Close()
		t.Fatal(err)
	}
	ids := canonical.NewSecureIDGenerator()
	writer, err := canonical.OpenWriter(ctx, canonical.WriterOptions{
		Backend: database.Canonical(), IDs: ids, Clock: canonical.SystemClock{},
		Timezone: canonical.MustTimezone("UTC"), QueueCapacity: 16,
	})
	if err != nil {
		_ = database.Close()
		t.Fatal(err)
	}
	application, err := app.New(app.Options{
		Writer: writer, Repository: database.Canonical(), IDs: ids, Clock: canonical.SystemClock{},
		Timezone: canonical.MustTimezone("UTC"), Blobs: objects, Provider: "test", Model: "test-model",
		MaxAttempts: 1, RetryBackoff: []time.Duration{}, MaxInputBytes: 4096, MaxOutputBytes: 4096,
		SafetyScanInterval: time.Second,
	})
	if err != nil {
		_ = writer.Close(context.Background())
		_ = database.Close()
		t.Fatal(err)
	}
	state, err := application.BootstrapInit(ctx, app.BootstrapInput{
		OwnerName: "Owner", Name: "Legacy Resident", SeedKey: "cov2-legacy-ledger", Principles: "preserve history",
	})
	if err != nil {
		_ = writer.Close(context.Background())
		_ = database.Close()
		t.Fatal(err)
	}
	residentID := state.Residents[0].ResidentID
	if err := writer.Close(context.Background()); err != nil {
		_ = database.Close()
		t.Fatal(err)
	}
	if err := database.Close(); err != nil {
		t.Fatal(err)
	}
	retainOnlyLegacyDialoguePipeline(t, cfg.DatabasePath())

	configPath := writeCommandConfig(t, cfg.DataDir)
	for _, arguments := range [][]string{
		{"--config", configPath},
		{"--config", configPath, "--resident", residentID.String()},
	} {
		var stdout, stderr bytes.Buffer
		if err := runLedgerVerify(ctx, arguments, &stdout, &stderr); err != nil {
			t.Fatalf("legacy-v1 ledger verify %v: %v, stderr=%s", arguments, err, stderr.String())
		}
		if !strings.Contains(stdout.String(), residentID.String()) {
			t.Fatalf("legacy-v1 ledger output = %s", stdout.String())
		}
	}

	unknownID := "01J00000000000000000000999"
	var stdout, stderr bytes.Buffer
	err = runLedgerVerify(ctx, []string{"--config", configPath, "--resident", unknownID}, &stdout, &stderr)
	if err == nil || !strings.Contains(err.Error(), "does not exist") {
		t.Fatalf("unknown resident ledger verify error = %v", err)
	}
}

func TestCOV2AdminIntegrityUnknownResidentFilterIsVersionIndependent(t *testing.T) {
	ctx := context.Background()
	database, err := store.Open(ctx, t.TempDir()+"/mahoroba.db")
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	unknownID, err := canonical.ParseID("01J00000000000000000000998")
	if err != nil {
		t.Fatal(err)
	}
	err = requireResidentExists(ctx, database.Canonical(), unknownID)
	if err == nil || !strings.Contains(err.Error(), unknownID.String()) || !strings.Contains(err.Error(), "does not exist") {
		t.Fatalf("unknown resident integrity preflight error = %v", err)
	}
}

func retainOnlyLegacyDialoguePipeline(t *testing.T, databasePath string) {
	t.Helper()
	database, err := sql.Open("sqlite", databasePath)
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	database.SetMaxOpenConns(1)
	tx, err := database.BeginTx(context.Background(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()

	var legacyCount int
	if err := tx.QueryRow(`SELECT count(*) FROM pipeline_versions
		WHERE pipeline_kind = 'dialogue' AND version_key = ?`, domain.DialoguePipelineVersionV1).Scan(&legacyCount); err != nil {
		t.Fatal(err)
	}
	if legacyCount == 0 {
		pipelineID, err := canonical.NewSecureIDGenerator().New()
		if err != nil {
			t.Fatal(err)
		}
		definition, err := domain.DialoguePipelineDefinition(pipelineID, domain.DialoguePipelineVersionV1)
		if err != nil {
			t.Fatal(err)
		}
		var commitID string
		var registeredAt int64
		var registeredTZ string
		if err := tx.QueryRow(`SELECT canonical_commit_id, committed_at, committed_tz
			FROM canonical_commits WHERE resident_id IS NULL ORDER BY commit_seq LIMIT 1`).Scan(
			&commitID, &registeredAt, &registeredTZ,
		); err != nil {
			t.Fatal(err)
		}
		if _, err := tx.Exec(`INSERT INTO pipeline_versions(
			pipeline_version_id, canonical_commit_id, pipeline_kind, version_key, definition, recorded_at, recorded_tz
		) VALUES (?, ?, 'dialogue', ?, ?, ?, ?)`, pipelineID.String(), commitID,
			domain.DialoguePipelineVersionV1, definition.Definition.String(), registeredAt, registeredTZ); err != nil {
			t.Fatal(err)
		}
	}

	var triggerSQL string
	if err := tx.QueryRow(`SELECT sql FROM sqlite_schema
		WHERE type = 'trigger' AND name = 'trg_pipeline_versions_no_delete'`).Scan(&triggerSQL); err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec(`DROP TRIGGER trg_pipeline_versions_no_delete`); err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec(`DELETE FROM pipeline_versions
		WHERE pipeline_kind = 'dialogue' AND version_key <> ?`, domain.DialoguePipelineVersionV1); err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec(triggerSQL); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
}
