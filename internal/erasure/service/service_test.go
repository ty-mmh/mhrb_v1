package service

import (
	"bytes"
	"context"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"testing"

	"mahoroba.local/mahoroba/internal/blob"
	"mahoroba.local/mahoroba/internal/canonical"
	"mahoroba.local/mahoroba/internal/durablepublish"
	"mahoroba.local/mahoroba/internal/erasure"
	"mahoroba.local/mahoroba/internal/fssecure"
	storesqlite "mahoroba.local/mahoroba/internal/store/sqlite"
)

func TestM7ErasurePlanSourceUsesRootRelativeBoundDatabaseDescriptor(t *testing.T) {
	dataDir, databasePath := newServiceSourceFixture(t)
	boundary, err := openSource(context.Background(), dataDir, filepath.Base(databasePath))
	if err != nil {
		t.Fatal(err)
	}
	defer boundary.close()
	if err := boundary.verify(); err != nil {
		t.Fatal(err)
	}

	moved := databasePath + ".moved"
	renameErr := os.Rename(databasePath, moved)
	if renameErr != nil {
		if runtime.GOOS != "windows" {
			t.Fatalf("rename bound plan database: %v", renameErr)
		}
		if err := boundary.verify(); err != nil {
			t.Fatalf("blocked Windows rename invalidated boundary: %v", err)
		}
		return
	}
	if runtime.GOOS == "windows" {
		t.Fatal("Windows plan database rename succeeded while no-delete-sharing handle was retained")
	}
	if err := os.WriteFile(databasePath, []byte("replacement"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := boundary.verify(); !errors.Is(err, ErrSourceUnavailable) {
		t.Fatalf("post-open plan database swap error=%v, want source unavailable", err)
	}
}

func TestM7ErasureApplyBoundaryRejectsDatabaseReparse(t *testing.T) {
	dataDir, databasePath := newServiceSourceFixture(t)
	if err := os.Rename(databasePath, databasePath+".moved"); err != nil {
		t.Fatal(err)
	}
	installServiceDatabaseReparse(t, databasePath)
	if boundary, err := OpenApplyBoundary(dataDir, filepath.Base(databasePath)); !errors.Is(err, ErrSourceUnavailable) {
		if boundary != nil {
			_ = boundary.Close()
		}
		t.Fatalf("database reparse error=%v, want source unavailable", err)
	}
}

func TestM7ErasureApplyBoundaryBlocksOrDetectsPostOpenDatabaseSwap(t *testing.T) {
	dataDir, databasePath := newServiceSourceFixture(t)
	boundary, err := OpenApplyBoundary(dataDir, filepath.Base(databasePath))
	if err != nil {
		t.Fatal(err)
	}
	defer boundary.Close()

	moved := databasePath + ".moved"
	renameErr := os.Rename(databasePath, moved)
	if renameErr != nil {
		if runtime.GOOS != "windows" {
			t.Fatalf("rename guarded database: %v", renameErr)
		}
		if err := boundary.Verify(); err != nil {
			t.Fatalf("blocked Windows rename invalidated boundary: %v", err)
		}
		return
	}
	if err := os.WriteFile(databasePath, []byte("replacement"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := boundary.Verify(); !errors.Is(err, ErrSourceUnavailable) {
		t.Fatalf("post-open apply database swap error=%v, want source unavailable", err)
	}
}

func newServiceSourceFixture(t *testing.T) (string, string) {
	t.Helper()
	dataDir := filepath.Join(t.TempDir(), "data")
	policy, err := fssecure.CurrentSecurityPolicy()
	if err != nil {
		t.Fatal(err)
	}
	root, err := fssecure.OpenOrCreateRoot(dataDir, policy)
	if err != nil {
		skipServiceDesktopSandbox(t, err)
		t.Fatalf("create protected source root: %v", err)
	}
	database, err := root.CreateRegular("mahoroba.db")
	if err != nil {
		_ = root.Close()
		skipServiceDesktopSandbox(t, err)
		t.Fatalf("create protected source database: %v", err)
	}
	if err := errors.Join(database.Close(), root.Close()); err != nil {
		t.Fatal(err)
	}
	databasePath := filepath.Join(dataDir, "mahoroba.db")
	store, err := storesqlite.Open(context.Background(), databasePath)
	if err != nil {
		skipServiceDesktopSandbox(t, err)
		t.Fatalf("initialize protected source database: %v", err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := blob.NewFileStore(filepath.Join(dataDir, "blobs")); err != nil {
		skipServiceDesktopSandbox(t, err)
		t.Fatalf("initialize protected source blob root: %v", err)
	}
	return dataDir, databasePath
}

func skipServiceDesktopSandbox(t *testing.T, err error) {
	t.Helper()
	if runtime.GOOS == "windows" && os.Getenv("CI") == "" && errors.Is(err, os.ErrPermission) {
		t.Skipf("desktop sandbox cannot exercise exact protected filesystem handles: %v", err)
	}
}

func installServiceDatabaseReparse(t *testing.T, path string) {
	t.Helper()
	target := path + ".reparse-target"
	if err := os.Mkdir(target, 0o700); err != nil {
		t.Fatal(err)
	}
	if runtime.GOOS == "windows" {
		output, err := exec.Command("cmd.exe", "/d", "/c", "mklink", "/J", path, target).CombinedOutput()
		if err != nil {
			t.Fatalf("create database junction: %v: %s", err, output)
		}
		return
	}
	if err := os.Symlink(target, path); err != nil {
		t.Fatal(err)
	}
}

func TestM7ErasurePlanArtifactUsesNoReplaceDurableSingleFileAndBoundedConsumer(t *testing.T) {
	ctx := context.Background()
	parentPath := filepath.Join(t.TempDir(), "protected")
	policy, err := fssecure.CurrentSecurityPolicy()
	if err != nil {
		t.Fatal(err)
	}
	parent, err := fssecure.OpenOrCreateRoot(parentPath, policy)
	if err != nil {
		t.Fatal(err)
	}
	if err := parent.Close(); err != nil {
		t.Fatal(err)
	}
	plan := minimalServicePlan(t)
	body, err := erasure.MarshalPlan(plan)
	if err != nil {
		t.Fatal(err)
	}
	inputDigest, err := durablepublish.ProducerInputDigest(
		durablepublish.CommandErasurePlanContent,
		durablepublish.ErasurePlanContentProducerInput{
			SourceDataDirIdentity: "sha256:" + string(bytes.Repeat([]byte{'1'}, 64)),
			ResidentID:            plan.ResidentID, RequestedContentIDs: plan.RequestedContentIDs,
			ReasonCode: plan.ReasonCode,
			BaseHead: durablepublish.ErasurePlanBaseHead{
				CommitID: plan.BaseHead.CommitID, CommitSeq: plan.BaseHead.CommitSeq,
			},
			ImpactVersion: plan.RulesVersion,
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	output := filepath.Join(parentPath, "plan.json")
	publicationCloseTestHook = func() error { return errors.New("injected post-barrier close failure") }
	t.Cleanup(func() { publicationCloseTestHook = nil })
	if err := publishPlan(ctx, output, durablepublish.CommandErasurePlanContent, inputDigest, body); err != nil {
		skipServiceDesktopSandbox(t, err)
		t.Fatal(err)
	}
	publicationCloseTestHook = nil
	published, err := os.ReadFile(output)
	if err != nil || !bytes.Equal(published, body) {
		t.Fatalf("published bytes differ: %v", err)
	}
	if err := publishPlan(ctx, output, durablepublish.CommandErasurePlanContent, inputDigest, body); !errors.Is(err, ErrArtifactTargetExists) {
		t.Fatalf("second publish = %v, want target exists", err)
	}
	parsed, identity, err := ReadPlan(ctx, output)
	if err != nil {
		t.Fatal(err)
	}
	if parsed.Digest != plan.Digest || len(identity) != len("sha256:")+64 {
		t.Fatalf("parsed digest/identity = %s / %s", parsed.Digest, identity)
	}

	parent, err = fssecure.OpenRoot(parentPath, policy)
	if err != nil {
		t.Fatal(err)
	}
	publishID := mustServiceID(t, "01J00000000000000000000209")
	marker, err := parent.CreateRegular(".plan.json.publish-pending." + publishID.String())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := marker.File().Write([]byte("pending")); err != nil {
		t.Fatal(err)
	}
	if err := marker.Seal(); err != nil {
		t.Fatal(err)
	}
	if err := marker.Close(); err != nil {
		t.Fatal(err)
	}
	if err := parent.Close(); err != nil {
		t.Fatal(err)
	}
	if _, _, err := ReadPlan(ctx, output); !errors.Is(err, ErrSourceUnavailable) {
		t.Fatalf("consumer with pending marker = %v", err)
	}
}

func minimalServicePlan(t *testing.T) erasure.Plan {
	t.Helper()
	resident := mustServiceID(t, "01J00000000000000000000200")
	content := mustServiceID(t, "01J00000000000000000000201")
	baseCommit := mustServiceID(t, "01J00000000000000000000202")
	actor := mustServiceID(t, "01J00000000000000000000203")
	integrityPipeline := mustServiceID(t, "01J00000000000000000000204")
	memoryPipeline := mustServiceID(t, "01J00000000000000000000205")
	event := mustServiceID(t, "01J00000000000000000000206")
	plan, err := erasure.Seal(erasure.Plan{
		PlanState: erasure.StateReady, Scope: erasure.ScopeContent,
		ResidentID: resident.String(), BaseHead: erasure.BaseHead{CommitID: baseCommit.String(), CommitSeq: "7"},
		ActorPrincipalID: actor.String(), ReasonCode: "privacy_request",
		IntegrityPipelineVersionID: integrityPipeline.String(), MemoryStatusPipelineVersionID: memoryPipeline.String(),
		RequestedContentIDs: []string{content.String()},
		EffectiveTargets: []erasure.EffectiveTarget{{
			ContentID: content.String(), ContentClass: "event_text", ErasurePolicy: "independent",
			Commitment:     "sha256:" + string(bytes.Repeat([]byte{'2'}, 64)),
			ErasureEventID: event.String(), LineageDepth: "0",
		}},
		Impacts: []erasure.Impact{}, PlannedFindings: []erasure.PlannedFinding{},
		ExistingFindingDependencies: []erasure.ExistingFindingDependency{},
		PlannedQuarantines:          []erasure.PlannedQuarantine{}, ClaimIdentityErasures: []erasure.ClaimIdentityErasure{},
		ExistingClaimIdentityDependencies: []erasure.ExistingClaimIdentityDependency{},
		RuntimeConfigEffect:               erasure.RuntimeConfigEffect{}, Rebuilds: []erasure.Rebuild{}, Blockers: []erasure.Blocker{},
	})
	if err != nil {
		t.Fatal(err)
	}
	return plan
}

func mustServiceID(t *testing.T, raw string) canonical.ID {
	t.Helper()
	id, err := canonical.ParseID(raw)
	if err != nil {
		t.Fatal(err)
	}
	return id
}
