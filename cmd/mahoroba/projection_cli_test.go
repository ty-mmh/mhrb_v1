package main

import (
	"bytes"
	"context"
	"encoding/json"
	"strings"
	"testing"

	"mahoroba.local/mahoroba/internal/app"
	"mahoroba.local/mahoroba/internal/canonical"
	"mahoroba.local/mahoroba/internal/hostlock"
	"mahoroba.local/mahoroba/internal/projection"
	store "mahoroba.local/mahoroba/internal/store/sqlite"
)

func TestProjectionStatusIsQueryOnlyAndReportsLagStalenessAndBlocking(t *testing.T) {
	dataDir := managedCommandDataDir(t)
	residentID := bootstrapProjectionCommandDatabase(t, dataDir)
	configPath := writeCommandConfig(t, dataDir)
	before := projectionWatermarkCount(t, dataDir, residentID)
	var stdout, stderr bytes.Buffer
	if code := run(context.Background(), []string{
		"projection", "status", "--config", configPath, "--resident", residentID,
	}, &stdout, &stderr); code != 0 {
		t.Fatalf("projection status exit=%d stderr=%s", code, stderr.String())
	}
	after := projectionWatermarkCount(t, dataDir, residentID)
	if after != before {
		t.Fatalf("query-only status changed watermark count from %d to %d", before, after)
	}
	for _, field := range []string{`"head"`, `"name"`, `"version"`, `"built"`, `"watermark"`, `"dependencies"`, `"commit_lag"`, `"up_to_date"`, `"stale"`, `"blocking_reason"`} {
		if !strings.Contains(stdout.String(), field) {
			t.Fatalf("projection status missing %s:\n%s", field, stdout.String())
		}
	}
	var statuses []projection.Status
	if err := json.Unmarshal(stdout.Bytes(), &statuses); err != nil {
		t.Fatalf("decode projection status: %v", err)
	}
	if len(statuses) != 6 {
		t.Fatalf("production projection status count = %d, want 6", len(statuses))
	}
	foundContentReferences := false
	for _, status := range statuses {
		foundContentReferences = foundContentReferences || status.Name == projection.ContentReferencesName
	}
	if !foundContentReferences {
		t.Fatalf("production status omitted %s", projection.ContentReferencesName)
	}
}

func TestProjectionRebuildRequiresExactlyNameOrAll(t *testing.T) {
	resident := "00000000000000000000000001"
	for _, arguments := range [][]string{
		{"projection", "rebuild", "--resident", resident},
		{"projection", "rebuild", "--resident", resident, "--name", "runtime_states", "--all"},
	} {
		var stdout, stderr bytes.Buffer
		if code := run(context.Background(), arguments, &stdout, &stderr); code != 1 || !strings.Contains(stderr.String(), "exactly one") {
			t.Fatalf("arguments=%v exit=%d stderr=%s", arguments, code, stderr.String())
		}
	}
}

func TestProjectionRebuildRequiresHostLockAndRejectsServe(t *testing.T) {
	dataDir := managedCommandDataDir(t)
	configPath := writeCommandConfig(t, dataDir)
	lock, err := hostlock.Acquire(dataDir)
	if err != nil {
		t.Fatal(err)
	}
	defer lock.Close()
	var stdout, stderr bytes.Buffer
	code := run(context.Background(), []string{
		"projection", "rebuild", "--config", configPath,
		"--resident", "00000000000000000000000001", "--all",
	}, &stdout, &stderr)
	if code != 1 || !strings.Contains(stderr.String(), "exclusive offline access") {
		t.Fatalf("locked projection rebuild exit=%d stderr=%s", code, stderr.String())
	}
}

func TestProjectionRebuildBuildsAllActiveViewsOffline(t *testing.T) {
	dataDir := managedCommandDataDir(t)
	residentID := bootstrapProjectionCommandDatabase(t, dataDir)
	configPath := writeCommandConfig(t, dataDir)
	var stdout, stderr bytes.Buffer
	if code := run(context.Background(), []string{
		"projection", "rebuild", "--config", configPath, "--resident", residentID, "--all",
	}, &stdout, &stderr); code != 0 {
		t.Fatalf("projection rebuild exit=%d stderr=%s", code, stderr.String())
	}
	if !strings.Contains(stdout.String(), `"rebuilt": true`) || projectionWatermarkCount(t, dataDir, residentID) != 6 {
		t.Fatalf("rebuild output/count = %s / %d", stdout.String(), projectionWatermarkCount(t, dataDir, residentID))
	}
}

func TestProjectionRebuildNamedCohortMemberKeepsExactPairOffline(t *testing.T) {
	for _, name := range []projection.Name{projection.ClaimStatesName, projection.RuntimeStatesName} {
		t.Run(string(name), func(t *testing.T) {
			dataDir := managedCommandDataDir(t)
			residentRaw := bootstrapProjectionCommandDatabase(t, dataDir)
			configPath := writeCommandConfig(t, dataDir)
			var stdout, stderr bytes.Buffer
			if code := run(context.Background(), []string{
				"projection", "rebuild", "--config", configPath, "--resident", residentRaw, "--name", string(name),
			}, &stdout, &stderr); code != 0 {
				t.Fatalf("projection rebuild exit=%d stderr=%s", code, stderr.String())
			}

			inspection, err := store.OpenInspection(context.Background(), commandRuntimeConfig(dataDir).DatabasePath())
			if err != nil {
				t.Fatal(err)
			}
			defer inspection.Close()
			residentID, err := canonical.ParseID(residentRaw)
			if err != nil {
				t.Fatal(err)
			}
			claim, claimExists, err := inspection.Projection().Watermark(context.Background(), projection.ClaimStatesName, residentID)
			if err != nil {
				t.Fatal(err)
			}
			runtime, runtimeExists, err := inspection.Projection().Watermark(context.Background(), projection.RuntimeStatesName, residentID)
			if err != nil {
				t.Fatal(err)
			}
			if !claimExists || !runtimeExists || claim.SourceCommitSeq != runtime.SourceCommitSeq ||
				claim.AsOf != runtime.AsOf || claim.AsOfTZ != runtime.AsOfTZ {
				t.Fatalf("named rebuild left cohort skewed: claim=%+v/%v runtime=%+v/%v", claim, claimExists, runtime, runtimeExists)
			}
		})
	}
}

func bootstrapProjectionCommandDatabase(t *testing.T, dataDir string) string {
	t.Helper()
	runtime, err := openRuntime(context.Background(), commandRuntimeConfig(dataDir), nil)
	if err != nil {
		t.Fatal(err)
	}
	state, err := runtime.app.BootstrapInit(context.Background(), app.BootstrapInput{
		OwnerName: "Owner", Name: "Resident", SeedKey: "projection-cli", Principles: "be durable",
	})
	if err != nil {
		t.Fatal(err)
	}
	residentID := state.Residents[0].ResidentID
	if err := runtime.app.ApprovePrinciples(context.Background(), residentID); err != nil {
		t.Fatal(err)
	}
	if err := runtime.app.FinalizeBootstrap(context.Background(), residentID, "friendly", defaultMemory); err != nil {
		t.Fatal(err)
	}
	if err := runtime.app.SelectResident(context.Background(), residentID); err != nil {
		t.Fatal(err)
	}
	if err := runtime.Close(commandRuntimeConfig(dataDir).Server.ShutdownTimeout); err != nil {
		t.Fatal(err)
	}
	return residentID.String()
}

func projectionWatermarkCount(t *testing.T, dataDir, residentRaw string) int {
	t.Helper()
	inspection, err := store.OpenInspection(context.Background(), commandRuntimeConfig(dataDir).DatabasePath())
	if err != nil {
		t.Fatal(err)
	}
	defer inspection.Close()
	var count int
	registry, err := store.ActiveProjectionRegistry()
	if err != nil {
		t.Fatal(err)
	}
	residentID, err := canonical.ParseID(residentRaw)
	if err != nil {
		t.Fatal(err)
	}
	for _, definition := range registry.Definitions() {
		_, exists, err := inspection.Projection().Watermark(context.Background(), definition.Name, residentID)
		if err != nil {
			t.Fatal(err)
		}
		if exists {
			count++
		}
	}
	return count
}
