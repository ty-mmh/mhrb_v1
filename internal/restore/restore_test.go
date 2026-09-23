package restore

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"math"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"testing"
	"time"

	"mahoroba.local/mahoroba/internal/app"
	"mahoroba.local/mahoroba/internal/backup"
	"mahoroba.local/mahoroba/internal/blob"
	"mahoroba.local/mahoroba/internal/canonical"
	"mahoroba.local/mahoroba/internal/domain"
	"mahoroba.local/mahoroba/internal/durablepublish"
	"mahoroba.local/mahoroba/internal/fssecure"
	"mahoroba.local/mahoroba/internal/generation"
	"mahoroba.local/mahoroba/internal/hostlock"
	"mahoroba.local/mahoroba/internal/memory"
	"mahoroba.local/mahoroba/internal/operationalmetrics"
	"mahoroba.local/mahoroba/internal/projection"
	storesqlite "mahoroba.local/mahoroba/internal/store/sqlite"
)

func TestM7RestoreValidBundlePublishesClosedDirectory(t *testing.T) {
	fixture := newRestoreFixture(t, false, false)
	target := filepath.Join(fixture.parent, "restored")
	metrics := operationalmetrics.New()
	result, err := Restore(context.Background(), Request{
		BundleRoot: fixture.bundle, TargetDataDir: target, Observer: metrics,
	})
	if err != nil {
		t.Fatalf("Restore: %v", err)
	}
	if !result.Published || result.DatabaseFilename != DatabaseFilename || result.SourceHead.Exists ||
		!result.RestoredHead.Exists || result.ServiceReady || result.RestoreID.IsZero() {
		t.Fatalf("restore result = %+v", result)
	}
	if len(result.CanonicalCommits) == 0 || !result.CanonicalApplied {
		t.Fatalf("restore did not report staged integrity bootstrap commits: %+v", result.CanonicalCommits)
	}
	if result.ProjectionsRebuilt != 0 {
		t.Fatalf("empty-resident restore projection mutations = %d, want 0", result.ProjectionsRebuilt)
	}
	for _, commit := range result.CanonicalCommits {
		if commit.Disposition != CommitCreated && commit.Disposition != CommitExisting {
			t.Fatalf("restore commit disposition = %q", commit.Disposition)
		}
		if !hasRestoreCommitEffect(commit.Effects) {
			t.Fatalf("restore commit lacks a typed effect: %+v", commit)
		}
	}
	snapshot := metrics.Snapshot()
	if snapshot.Operations.Restore.Succeeded != 1 || snapshot.Writer.AcceptedTotal == 0 {
		t.Fatalf("restore operational observation = %+v", snapshot)
	}
	for _, path := range []string{
		filepath.Join(target, RestoreStagingMarker),
		filepath.Join(target, durablepublish.MarkerName),
		filepath.Join(fixture.parent, result.StagingBasename),
		filepath.Join(fixture.parent, mustRestoreSiblingMarker(t, result)),
		filepath.Join(target, "manifest.json"),
		filepath.Join(target, "database"),
	} {
		if _, err := os.Lstat(path); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("reserved/non-target path %s remains: %v", path, err)
		}
	}
	inspection, err := storesqlite.OpenInspection(context.Background(), filepath.Join(target, DatabaseFilename))
	if err != nil {
		t.Fatal(err)
	}
	objects, err := blob.NewFileStore(filepath.Join(target, "blobs"))
	if err != nil {
		_ = inspection.Close()
		t.Fatal(err)
	}
	if err := inspection.MinimumCheckerWithBlobObjects(objects).Check(context.Background()); err != nil {
		_ = inspection.Close()
		t.Fatal(err)
	}
	if err := inspection.Close(); err != nil {
		t.Fatal(err)
	}

	restoredStore, err := storesqlite.Open(context.Background(), filepath.Join(target, DatabaseFilename))
	if err != nil {
		t.Fatal(err)
	}
	var pipelineCount int
	if err := restoredStore.Reader().QueryRow(`SELECT COUNT(*) FROM pipeline_versions
		WHERE pipeline_kind = 'dialogue' AND version_key = ?`, domain.DialoguePipelineVersionV3).Scan(&pipelineCount); err != nil {
		_ = restoredStore.Close()
		t.Fatal(err)
	}
	var pipelineRaw, registrationCommitRaw, definitionRaw string
	if err := restoredStore.Reader().QueryRow(`SELECT pipeline_version_id, canonical_commit_id, definition
		FROM pipeline_versions WHERE pipeline_kind = 'dialogue' AND version_key = ?`,
		domain.DialoguePipelineVersionV3).Scan(&pipelineRaw, &registrationCommitRaw, &definitionRaw); err != nil {
		_ = restoredStore.Close()
		t.Fatal(err)
	}
	pipelineID, err := canonical.ParseID(pipelineRaw)
	if err != nil {
		_ = restoredStore.Close()
		t.Fatal(err)
	}
	definition, err := canonical.ParseCanonicalJSON([]byte(definitionRaw))
	if err != nil {
		_ = restoredStore.Close()
		t.Fatal(err)
	}
	if pipelineCount != 1 || domain.ValidateExactDialoguePipelineDefinition(domain.PipelineVersionDefinition{
		ID: pipelineID, Kind: "dialogue", VersionKey: domain.DialoguePipelineVersionV3, Definition: definition,
	}) != nil {
		_ = restoredStore.Close()
		t.Fatalf("restored dialogue-v3 registration count/definition = %d / %s", pipelineCount, definitionRaw)
	}
	registrationCommitID, err := canonical.ParseID(registrationCommitRaw)
	if err != nil {
		_ = restoredStore.Close()
		t.Fatal(err)
	}
	var registrationEffectReported bool
	for _, commit := range result.CanonicalCommits {
		if commit.Metadata.CommitID == registrationCommitID && commit.Effects.PipelineVersionRegistered {
			registrationEffectReported = true
		}
	}
	if !registrationEffectReported {
		_ = restoredStore.Close()
		t.Fatalf("restore result omitted dialogue-v3 registration commit %s: %+v",
			registrationCommitID, result.CanonicalCommits)
	}
	if err := restoredStore.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestM7RestoreRunningAttemptTerminalizesWithoutProviderAndIsIdempotent(t *testing.T) {
	fixture := newRestoreFixture(t, true, false)
	target := filepath.Join(fixture.parent, "restored-running")
	metrics := operationalmetrics.New()
	result, err := Restore(context.Background(), Request{
		BundleRoot: fixture.bundle, TargetDataDir: target, Observer: metrics,
	})
	if err != nil {
		t.Fatalf("Restore running bundle: %v", err)
	}
	if result.TerminalizedAttempts != 1 || !result.Published {
		t.Fatalf("running restore result = %+v", result)
	}
	var reportedTerminalization bool
	for _, commit := range result.CanonicalCommits {
		reportedTerminalization = reportedTerminalization || commit.Effects.GenerationAttemptTerminalized
	}
	if !reportedTerminalization {
		t.Fatalf("running restore omitted terminalization commit effect: %+v", result.CanonicalCommits)
	}
	if snapshot := metrics.Snapshot(); snapshot.Projection.ObservedTotal == 0 || snapshot.Provider.SucceededTotal != 0 ||
		result.ProjectionsRebuilt == 0 || uint64(result.ProjectionsRebuilt) != snapshot.Projection.RebuildTotal {
		t.Fatalf("staged restore component observation/result = %+v / %+v", snapshot, result)
	}
	db := openRestoreSQL(t, filepath.Join(target, DatabaseFilename))
	defer db.Close()
	var running, interrupted int
	if err := db.QueryRow(`SELECT COUNT(*) FROM generation_run_outcomes WHERE state = 'running'`).Scan(&running); err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRow(`SELECT COUNT(*) FROM generation_run_outcomes WHERE state = 'cancelled' AND error_class = 'runtime_interrupted'`).Scan(&interrupted); err != nil {
		t.Fatal(err)
	}
	if running != 1 || interrupted != 1 {
		t.Fatalf("outcomes running=%d interrupted=%d, want immutable running=1 plus cancellation=1", running, interrupted)
	}
}

func TestM7RestoreValidClaimAliasRoundTripRetainsBatchGuard(t *testing.T) {
	fixture := newRestoreFixture(t, true, false)
	claimID, contentID := addRestoreClaimAliases(t, fixture.source)
	aliasBundle := filepath.Join(fixture.parent, "alias-bundle")
	_, err := backup.Create(context.Background(), backup.CreateRequest{
		SourceDataDir: fixture.source, DatabaseFilename: DatabaseFilename, Output: aliasBundle,
		CreatedBy: backup.CreatedBy{BinaryVersion: "test", GitRevision: "0123456789abcdef", GoVersion: runtime.Version()},
	})
	if err != nil {
		t.Fatalf("alias backup Create: %v", err)
	}
	target := filepath.Join(fixture.parent, "alias-restored")
	result, err := Restore(context.Background(), Request{BundleRoot: aliasBundle, TargetDataDir: target})
	if err != nil {
		t.Fatalf("Restore alias bundle: %v", err)
	}
	if !result.Published {
		t.Fatalf("alias restore result = %+v", result)
	}
	db := openRestoreSQL(t, filepath.Join(target, DatabaseFilename))
	var total, nonNull, distinctContent int
	if err := db.QueryRow(`SELECT COUNT(*), COUNT(statement_hash), COUNT(DISTINCT statement_content_id)
		FROM claims WHERE statement_content_id = ?`, contentID.String()).Scan(&total, &nonNull, &distinctContent); err != nil {
		_ = db.Close()
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	if total != 2 || nonNull != 2 || distinctContent != 1 {
		t.Fatalf("restored alias group total=%d non-null=%d content=%d", total, nonNull, distinctContent)
	}

	store, err := storesqlite.Open(context.Background(), filepath.Join(target, DatabaseFilename))
	if err != nil {
		t.Fatal(err)
	}
	ids := canonical.NewSecureIDGenerator()
	writer, err := canonical.OpenWriter(context.Background(), canonical.WriterOptions{
		Backend: store.Canonical(), IDs: ids, Clock: canonical.SystemClock{},
		Timezone: canonical.MustTimezone("UTC"), QueueCapacity: 8,
	})
	if err != nil {
		_ = store.Close()
		t.Fatal(err)
	}
	var residentRaw, actorRaw string
	if err := store.Reader().QueryRow(`SELECT owner_resident_id FROM claims WHERE claim_id = ?`, claimID.String()).Scan(&residentRaw); err != nil {
		t.Fatal(err)
	}
	if err := store.Reader().QueryRow(`SELECT principal_id FROM principals WHERE kind = 'human' ORDER BY created_at LIMIT 1`).Scan(&actorRaw); err != nil {
		t.Fatal(err)
	}
	residentID, err := canonical.ParseID(residentRaw)
	if err != nil {
		t.Fatal(err)
	}
	actorID, err := canonical.ParseID(actorRaw)
	if err != nil {
		t.Fatal(err)
	}
	claimEventID, _ := ids.New()
	contentEventID, _ := ids.New()
	_, erasureErr := writer.Submit(context.Background(), domain.EraseClaimStatementCommand(domain.EraseClaimStatement{
		ResidentID: residentID, ClaimID: claimID,
		ClaimStatementErasureEventID: claimEventID, ContentErasureEventID: contentEventID,
		ActorPrincipalID: actorID, ReasonCode: "restore_alias_guard",
		OccurredAt: canonical.InstantFromTime(time.Now()), OccurredTZ: canonical.MustTimezone("UTC"),
	}))
	if !errors.Is(erasureErr, domain.ErrClaimStatementErasureRequiresBatch) {
		t.Fatalf("restored alias single erasure error = %v", erasureErr)
	}
	if err := writer.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestM7RestoreRejectsHalfErasedAliasBundleBeforePublish(t *testing.T) {
	fixture := newRestoreFixture(t, true, false)
	_, _ = addRestoreClaimAliases(t, fixture.source)
	bundle := filepath.Join(fixture.parent, "half-erased-bundle")
	_, err := backup.Create(context.Background(), backup.CreateRequest{
		SourceDataDir: fixture.source, DatabaseFilename: DatabaseFilename, Output: bundle,
		CreatedBy: backup.CreatedBy{BinaryVersion: "test", GitRevision: "0123456789abcdef", GoVersion: runtime.Version()},
	})
	if err != nil {
		t.Fatal(err)
	}
	makeRestoreBundleHalfErased(t, bundle)
	target := filepath.Join(fixture.parent, "half-erased-target")
	result, err := Restore(context.Background(), Request{BundleRoot: bundle, TargetDataDir: target})
	if !errors.Is(err, ErrInvalidBundle) {
		t.Fatalf("half-erased restore error = %v", err)
	}
	if result.Published || result.StagingBasename != "" {
		t.Fatalf("half-erased restore created staging/public result: %+v", result)
	}
	assertRestorePath(t, target, false)
}

func TestM7RestoreAttemptOverflowLeavesStagingCanonicalUnchanged(t *testing.T) {
	fixture := newRestoreFixture(t, true, true)
	sourceCommits := countRestoreRows(t, filepath.Join(fixture.bundle, filepath.FromSlash(backup.DatabaseBundlePath)), "canonical_commits")
	target := filepath.Join(fixture.parent, "overflow-target")
	result, err := restore(context.Background(), Request{BundleRoot: fixture.bundle, TargetDataDir: target}, restoreHooks{
		beforeRecovery: func(database string) error {
			setRestoreRunningAttemptMax(t, database)
			return nil
		},
	})
	if !errors.Is(err, domain.ErrRecoveryAttemptOverflow) {
		t.Fatalf("Restore overflow error = %v", err)
	}
	if result.Published || result.CanonicalApplied || len(result.CanonicalCommits) != 0 || result.StagingBasename == "" {
		t.Fatalf("overflow leaked staged Canonical effects: %+v", result)
	}
	if _, err := os.Lstat(target); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("overflow target exists: %v", err)
	}
	staging := filepath.Join(fixture.parent, result.StagingBasename)
	if got := countRestoreRows(t, filepath.Join(staging, DatabaseFilename), "canonical_commits"); got != sourceCommits {
		t.Fatalf("overflow staging commits = %d, want source %d", got, sourceCommits)
	}
	assertRestorePath(t, filepath.Join(staging, RestoreStagingMarker), true)
	assertRestorePath(t, filepath.Join(staging, durablepublish.MarkerName), false)
}

func TestM7RestoreTargetRacePreservesMarkersAndHidesStagedCommits(t *testing.T) {
	fixture := newRestoreFixture(t, false, false)
	target := filepath.Join(fixture.parent, "raced-target")
	result, err := restore(context.Background(), Request{BundleRoot: fixture.bundle, TargetDataDir: target}, restoreHooks{
		beforePublish: func(_, target string) error { return os.Mkdir(target, 0o700) },
	})
	if !errors.Is(err, ErrTargetExists) {
		t.Fatalf("target race error = %v", err)
	}
	if result.Published || result.CanonicalApplied || len(result.CanonicalCommits) != 0 {
		t.Fatalf("target race leaked staged effects: %+v", result)
	}
	staging := filepath.Join(fixture.parent, result.StagingBasename)
	assertRestorePath(t, staging, true)
	assertRestorePath(t, filepath.Join(staging, RestoreStagingMarker), true)
	assertRestorePath(t, filepath.Join(staging, durablepublish.MarkerName), true)
	assertRestorePath(t, filepath.Join(fixture.parent, mustRestoreSiblingMarker(t, result)), true)
}

func TestM7RestoreMatchingMarkerRecoveryCompletesPostRenameStates(t *testing.T) {
	tests := []struct {
		name        string
		point       durablepublish.Failpoint
		loseSibling bool
	}{
		{name: "internal and sibling", point: durablepublish.FailpointAfterPayloadRename},
		{name: "sibling only", point: durablepublish.FailpointAfterTargetCleanup},
		{name: "internal only", point: durablepublish.FailpointAfterPayloadRename, loseSibling: true},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			fixture := newRestoreFixture(t, true, false)
			target := filepath.Join(fixture.parent, "recover-target")
			crashed, err := restore(context.Background(), Request{BundleRoot: fixture.bundle, TargetDataDir: target}, restoreHooks{
				publishFailpoint: func(actual durablepublish.Failpoint) error {
					if actual == test.point {
						return errors.New("injected crash")
					}
					return nil
				},
			})
			if !errors.Is(err, ErrDurabilityUnknown) || crashed.Published || crashed.RestoreID.IsZero() {
				t.Fatalf("crashed restore = %+v, %v", crashed, err)
			}
			if test.loseSibling {
				policy, err := fssecure.CurrentSecurityPolicy()
				if err != nil {
					t.Fatal(err)
				}
				root, err := fssecure.OpenRootForPublish(target, policy)
				if err != nil {
					t.Fatal(err)
				}
				sibling, found, err := root.OpenUniquePublishMarkerSiblingForUpdate(filepath.Base(target))
				if err != nil || !found {
					t.Fatalf("open recovery sibling = %v, found=%v", err, found)
				}
				if err := sibling.MarkDeleteOnClose(); err != nil {
					t.Fatal(err)
				}
				if err := errors.Join(sibling.Close(), root.SyncParent(), root.Close()); err != nil {
					t.Fatal(err)
				}
			}

			recovered, err := Restore(context.Background(), Request{BundleRoot: fixture.bundle, TargetDataDir: target})
			if err != nil {
				t.Fatalf("matching recovery: %v", err)
			}
			if !recovered.Published || recovered.CanonicalApplied || len(recovered.CanonicalCommits) != 0 ||
				recovered.ProjectionsRebuilt != 0 ||
				recovered.RestoreID != crashed.RestoreID || recovered.StagingBasename != crashed.StagingBasename ||
				!recovered.RestoredHead.Exists {
				t.Fatalf("recovered result = %+v", recovered)
			}
			assertRestorePath(t, filepath.Join(target, durablepublish.MarkerName), false)
			assertRestorePath(t, filepath.Join(target, RestoreStagingMarker), false)
			assertRestorePath(t, filepath.Join(fixture.parent, mustRestoreSiblingMarker(t, recovered)), false)
		})
	}
}

func TestM7RestoreCommitEffectsMergeWithoutInventingCommits(t *testing.T) {
	first := canonical.CommitMetadata{
		CommitID:  mustRestoreID(t, "01J00000000000000000000081"),
		CommitSeq: canonical.CommitSeq(2),
	}
	second := canonical.CommitMetadata{
		CommitID:  mustRestoreID(t, "01J00000000000000000000082"),
		CommitSeq: canonical.CommitSeq(1),
	}
	var commits []Commit
	appendRestoreCommit(&commits, Commit{
		Metadata: first, Disposition: CommitExisting,
		Effects: CommitEffects{PipelineVersionRegistered: true},
	})
	appendRestoreCommit(&commits, Commit{
		Metadata: first, Disposition: CommitCreated,
		Effects: CommitEffects{IntegrityFindingRecorded: true},
	})
	appendRestoreCommit(&commits, Commit{
		Metadata: second, Disposition: CommitCreated,
		Effects: CommitEffects{MandatoryWorkCancelled: true},
	})
	if len(commits) != 2 || commits[0].Metadata.CommitID != second.CommitID || commits[1].Metadata.CommitID != first.CommitID {
		t.Fatalf("merged/sorted commits = %+v", commits)
	}
	if commits[1].Disposition != CommitCreated || !commits[1].Effects.PipelineVersionRegistered ||
		!commits[1].Effects.IntegrityFindingRecorded {
		t.Fatalf("merged commit = %+v", commits[1])
	}
}

func TestCOV56RegisterRestoredDialogueV3IsExactAndIdempotent(t *testing.T) {
	ctx := context.Background()
	store, err := storesqlite.Open(ctx, filepath.Join(t.TempDir(), "restore-register.db"))
	if err != nil {
		t.Fatal(err)
	}
	ids := canonical.NewSecureIDGenerator()
	writer, err := canonical.OpenWriter(ctx, canonical.WriterOptions{
		Backend: store.Canonical(), IDs: ids, Clock: canonical.SystemClock{},
		Timezone: canonical.MustTimezone("UTC"), QueueCapacity: 8,
	})
	if err != nil {
		_ = store.Close()
		t.Fatal(err)
	}
	legacyID, err := ids.New()
	if err != nil {
		_ = writer.Close(ctx)
		_ = store.Close()
		t.Fatal(err)
	}
	legacy, err := domain.DialoguePipelineDefinition(legacyID, domain.DialoguePipelineVersionV1)
	if err != nil {
		_ = writer.Close(ctx)
		_ = store.Close()
		t.Fatal(err)
	}
	priorSplitID, err := ids.New()
	if err != nil {
		_ = writer.Close(ctx)
		_ = store.Close()
		t.Fatal(err)
	}
	priorSplit, err := domain.DialoguePipelineDefinition(priorSplitID, domain.DialoguePipelineVersionV2)
	if err != nil {
		_ = writer.Close(ctx)
		_ = store.Close()
		t.Fatal(err)
	}
	if _, err := writer.Submit(ctx, domain.RegisterPipelineVersionsCommand(domain.RegisterPipelineVersions{
		Versions: []domain.PipelineVersionDefinition{legacy, priorSplit},
	})); err != nil {
		_ = writer.Close(ctx)
		_ = store.Close()
		t.Fatal(err)
	}
	first, err := registerRestoredDialogueV3(ctx, writer, ids)
	if err != nil {
		_ = writer.Close(ctx)
		_ = store.Close()
		t.Fatal(err)
	}
	if first.Commit.CommitID.IsZero() {
		_ = writer.Close(ctx)
		_ = store.Close()
		t.Fatal("first restored dialogue-v3 registration created no Canonical commit")
	}
	var commits []Commit
	appendRestoreCommit(&commits, Commit{
		Metadata: first.Commit, Disposition: CommitCreated,
		Effects: CommitEffects{PipelineVersionRegistered: true},
	})
	if len(commits) != 1 || !commits[0].Effects.PipelineVersionRegistered {
		_ = writer.Close(ctx)
		_ = store.Close()
		t.Fatalf("registration commit effect = %+v", commits)
	}
	headBefore, err := store.Canonical().LoadHead(ctx)
	if err != nil {
		_ = writer.Close(ctx)
		_ = store.Close()
		t.Fatal(err)
	}
	second, err := registerRestoredDialogueV3(ctx, writer, ids)
	if err != nil {
		_ = writer.Close(ctx)
		_ = store.Close()
		t.Fatal(err)
	}
	headAfter, err := store.Canonical().LoadHead(ctx)
	if err != nil {
		_ = writer.Close(ctx)
		_ = store.Close()
		t.Fatal(err)
	}
	if !second.Commit.CommitID.IsZero() || headAfter != headBefore {
		_ = writer.Close(ctx)
		_ = store.Close()
		t.Fatalf("idempotent restored registration mutated Canonical state: %+v, %+v -> %+v",
			second, headBefore, headAfter)
	}

	var pipelineRaw, commitRaw, definitionRaw string
	var count int
	if err := store.Reader().QueryRow(`SELECT COUNT(*) FROM pipeline_versions
		WHERE pipeline_kind = 'dialogue' AND version_key = ?`, domain.DialoguePipelineVersionV3).Scan(&count); err != nil {
		_ = writer.Close(ctx)
		_ = store.Close()
		t.Fatal(err)
	}
	if err := store.Reader().QueryRow(`SELECT pipeline_version_id, canonical_commit_id, definition
		FROM pipeline_versions WHERE pipeline_kind = 'dialogue' AND version_key = ?`,
		domain.DialoguePipelineVersionV3).Scan(&pipelineRaw, &commitRaw, &definitionRaw); err != nil {
		_ = writer.Close(ctx)
		_ = store.Close()
		t.Fatal(err)
	}
	pipelineID, err := canonical.ParseID(pipelineRaw)
	if err != nil {
		_ = writer.Close(ctx)
		_ = store.Close()
		t.Fatal(err)
	}
	definition, err := canonical.ParseCanonicalJSON([]byte(definitionRaw))
	if err != nil {
		_ = writer.Close(ctx)
		_ = store.Close()
		t.Fatal(err)
	}
	if count != 1 || commitRaw != first.Commit.CommitID.String() {
		_ = writer.Close(ctx)
		_ = store.Close()
		t.Fatalf("restored dialogue-v3 row count/commit = %d/%s, want 1/%s",
			count, commitRaw, first.Commit.CommitID)
	}
	for version, wantID := range map[string]canonical.ID{
		domain.DialoguePipelineVersionV1: legacyID,
		domain.DialoguePipelineVersionV2: priorSplitID,
	} {
		var storedID string
		if err := store.Reader().QueryRow(`SELECT pipeline_version_id FROM pipeline_versions
			WHERE pipeline_kind = 'dialogue' AND version_key = ?`, version).Scan(&storedID); err != nil {
			_ = writer.Close(ctx)
			_ = store.Close()
			t.Fatal(err)
		}
		if storedID != wantID.String() {
			_ = writer.Close(ctx)
			_ = store.Close()
			t.Fatalf("restored historical dialogue pipeline %s = %s, want preserved %s", version, storedID, wantID)
		}
	}
	if err := domain.ValidateExactDialoguePipelineDefinition(domain.PipelineVersionDefinition{
		ID: pipelineID, Kind: "dialogue", VersionKey: domain.DialoguePipelineVersionV3, Definition: definition,
	}); err != nil {
		_ = writer.Close(ctx)
		_ = store.Close()
		t.Fatal(err)
	}
	if err := writer.Close(ctx); err != nil {
		_ = store.Close()
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestM7RestoreObserverRecordsClosedFailureWithoutArtifactData(t *testing.T) {
	metrics := operationalmetrics.New()
	result, err := Restore(context.Background(), Request{Observer: metrics})
	if err == nil || result.Published {
		t.Fatalf("invalid restore = %+v, %v", result, err)
	}
	if got := metrics.Snapshot().Operations.Restore.Failed; got != 1 {
		t.Fatalf("restore failed operation count = %d", got)
	}
}

func hasRestoreCommitEffect(effects CommitEffects) bool {
	return effects.GenerationAttemptTerminalized || effects.MandatoryWorkCancelled ||
		effects.PipelineVersionRegistered || effects.IntegrityFindingRecorded ||
		effects.ClaimStatusQuarantined
}

func mustRestoreID(t *testing.T, raw string) canonical.ID {
	t.Helper()
	id, err := canonical.ParseID(raw)
	if err != nil {
		t.Fatal(err)
	}
	return id
}

func TestM7RestoreMismatchedRecoveryMarkerIsMutationFree(t *testing.T) {
	fixture := newRestoreFixture(t, false, false)
	target := filepath.Join(fixture.parent, "mismatch-target")
	crashed, err := restore(context.Background(), Request{BundleRoot: fixture.bundle, TargetDataDir: target}, restoreHooks{
		publishFailpoint: func(actual durablepublish.Failpoint) error {
			if actual == durablepublish.FailpointAfterPayloadRename {
				return errors.New("injected crash")
			}
			return nil
		},
	})
	if !errors.Is(err, ErrDurabilityUnknown) {
		t.Fatalf("crash setup: %+v, %v", crashed, err)
	}
	policy, err := fssecure.CurrentSecurityPolicy()
	if err != nil {
		t.Fatal(err)
	}
	root, err := fssecure.OpenRootForPublish(target, policy)
	if err != nil {
		t.Fatal(err)
	}
	sibling, found, err := root.OpenUniquePublishMarkerSiblingForUpdate(filepath.Base(target))
	if err != nil || !found {
		t.Fatalf("open sibling = %v, found=%v", err, found)
	}
	if err := sibling.File().Truncate(0); err != nil {
		t.Fatal(err)
	}
	if _, err := sibling.File().Seek(0, 0); err != nil {
		t.Fatal(err)
	}
	if _, err := sibling.File().Write([]byte("{}\n")); err != nil {
		t.Fatal(err)
	}
	if err := sibling.Seal(); err != nil {
		t.Fatal(err)
	}
	if err := errors.Join(sibling.Close(), root.Close()); err != nil {
		t.Fatal(err)
	}

	result, err := Restore(context.Background(), Request{BundleRoot: fixture.bundle, TargetDataDir: target})
	if !errors.Is(err, ErrDurabilityUnknown) || result.Published || result.CanonicalApplied || len(result.CanonicalCommits) != 0 {
		t.Fatalf("mismatched recovery = %+v, %v", result, err)
	}
	assertRestorePath(t, filepath.Join(target, durablepublish.MarkerName), true)
	assertRestorePath(t, filepath.Join(fixture.parent, mustRestoreSiblingMarker(t, crashed)), true)
}

func TestM7RestoreRejectsExistingTargetAndInvalidBundleBeforeStaging(t *testing.T) {
	t.Run("existing target", func(t *testing.T) {
		fixture := newRestoreFixture(t, false, false)
		target := filepath.Join(fixture.parent, "exists")
		if err := os.Mkdir(target, 0o700); err != nil {
			t.Fatal(err)
		}
		result, err := Restore(context.Background(), Request{BundleRoot: fixture.bundle, TargetDataDir: target})
		if !errors.Is(err, ErrTargetExists) || result.StagingBasename != "" {
			t.Fatalf("existing target result = %+v, %v", result, err)
		}
	})
	t.Run("invalid bundle", func(t *testing.T) {
		fixture := newRestoreFixture(t, false, false)
		if err := os.Remove(filepath.Join(fixture.bundle, "COMPLETE")); err != nil {
			t.Fatal(err)
		}
		target := filepath.Join(fixture.parent, "invalid-target")
		result, err := Restore(context.Background(), Request{BundleRoot: fixture.bundle, TargetDataDir: target})
		if !errors.Is(err, ErrInvalidBundle) || result.StagingBasename != "" {
			t.Fatalf("invalid bundle result = %+v, %v", result, err)
		}
		assertRestorePath(t, target, false)
	})
}

type restoreFixture struct {
	parent string
	source string
	bundle string
}

func newRestoreFixture(t *testing.T, running, inactive bool) restoreFixture {
	t.Helper()
	parent := t.TempDir()
	source := filepath.Join(parent, "source")
	sourceLock, err := hostlock.Acquire(source)
	if err != nil {
		skipRestoreWindowsSandbox(t, err)
		t.Fatalf("create managed restore source: %v", err)
	}
	if err := sourceLock.Close(); err != nil {
		t.Fatal(err)
	}
	policy, err := fssecure.CurrentSecurityPolicy()
	if err != nil {
		t.Fatal(err)
	}
	sourceRoot, err := fssecure.OpenRoot(source, policy)
	if err != nil {
		skipRestoreWindowsSandbox(t, err)
		t.Fatal(err)
	}
	databaseHandle, err := sourceRoot.CreateRegular(DatabaseFilename)
	if err != nil {
		_ = sourceRoot.Close()
		skipRestoreWindowsSandbox(t, err)
		t.Fatal(err)
	}
	if err := errors.Join(databaseHandle.Close(), sourceRoot.Close()); err != nil {
		t.Fatal(err)
	}
	database := filepath.Join(source, DatabaseFilename)
	store, err := storesqlite.Open(context.Background(), database)
	if err != nil {
		t.Fatal(err)
	}
	objects, err := blob.NewFileStore(filepath.Join(source, "blobs"))
	if err != nil {
		_ = store.Close()
		skipRestoreWindowsSandbox(t, err)
		t.Fatal(err)
	}
	if running {
		populateRestoreRunning(t, store, objects)
	}
	if inactive {
		archiveRestoreResidentWithoutRecovery(t, store)
	}
	preflightRestoreContentReferences(t, store)
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	bundle := filepath.Join(parent, "bundle")
	_, err = backup.Create(context.Background(), backup.CreateRequest{
		SourceDataDir: source, DatabaseFilename: DatabaseFilename, Output: bundle,
		CreatedBy: backup.CreatedBy{BinaryVersion: "test", GitRevision: "0123456789abcdef", GoVersion: runtime.Version()},
	})
	if err != nil {
		skipRestoreWindowsSandbox(t, err)
		t.Fatalf("backup Create: %v", err)
	}
	return restoreFixture{parent: parent, source: source, bundle: bundle}
}

func populateRestoreRunning(t *testing.T, store *storesqlite.Store, objects *blob.FileStore) {
	t.Helper()
	ctx := context.Background()
	ids := canonical.NewSecureIDGenerator()
	clock := canonical.SystemClock{}
	timezone := canonical.MustTimezone("UTC")
	writer, err := canonical.OpenWriter(ctx, canonical.WriterOptions{
		Backend: store.Canonical(), IDs: ids, Clock: clock, Timezone: timezone, QueueCapacity: 32,
	})
	if err != nil {
		t.Fatal(err)
	}
	registry, err := storesqlite.ActiveProjectionRegistry()
	if err != nil {
		_ = writer.Close(ctx)
		t.Fatal(err)
	}
	projectionStore := store.Projection()
	coordinator, err := projection.NewCoordinator(projection.CoordinatorOptions{
		Registry: registry, Source: projectionStore, Store: projectionStore,
		Clock: clock, Timezone: timezone,
		ScanInterval: time.Hour, AsOfRefreshInterval: time.Hour,
		RebuildRetryInterval: time.Minute, MaxStaleness: time.Hour,
	})
	if err != nil {
		_ = writer.Close(ctx)
		t.Fatal(err)
	}
	application, err := app.New(app.Options{
		Writer: writer, CommitNotifier: coordinator, Repository: store.Canonical(), IDs: ids, Clock: clock, Timezone: timezone,
		Blobs: objects, Generator: restoreCrashAfterPrepareGenerator{}, Provider: "fixture-provider", Model: "fixture-model",
		MaxAttempts: 3, RetryBackoff: []time.Duration{0, 0}, MaxInputBytes: 4096,
		MaxOutputBytes: 4096, SafetyScanInterval: time.Hour,
	})
	if err != nil {
		t.Fatal(err)
	}
	state, err := application.BootstrapInit(ctx, app.BootstrapInput{
		OwnerName: "owner", Name: "resident", SeedKey: "seed", Principles: "principles",
	})
	if err != nil || len(state.Residents) != 1 {
		t.Fatalf("BootstrapInit = %+v, %v", state, err)
	}
	residentID := state.Residents[0].ResidentID
	if err := application.ApprovePrinciples(ctx, residentID); err != nil {
		t.Fatal(err)
	}
	if err := application.FinalizeBootstrap(ctx, residentID, "persona", `{"mandatory_event_types":[],"memory_recall_enabled":false,"version":"memory-policy-v1"}`); err != nil {
		t.Fatal(err)
	}
	if err := application.SelectResident(ctx, residentID); err != nil {
		t.Fatal(err)
	}
	if _, err := application.Ingress(ctx, "uninterrupted source message"); err != nil {
		t.Fatal(err)
	}
	var recovered any
	func() {
		defer func() { recovered = recover() }()
		_ = application.ProcessResident(ctx, residentID)
	}()
	if recovered != restoreCrashAfterPreparePanic {
		t.Fatalf("restore crash fixture panic = %v, want %q", recovered, restoreCrashAfterPreparePanic)
	}
	if err := writer.Close(ctx); err != nil {
		t.Fatal(err)
	}
}

type restoreCrashAfterPrepareGenerator struct{}

const restoreCrashAfterPreparePanic = "restore fixture crash after durable Commit B"

func (restoreCrashAfterPrepareGenerator) Stream(
	context.Context,
	generation.Request,
	generation.DeltaSink,
) (generation.Result, error) {
	panic(restoreCrashAfterPreparePanic)
}

func archiveRestoreResidentWithoutRecovery(t *testing.T, store *storesqlite.Store) {
	t.Helper()
	ctx := context.Background()
	residents, err := store.Canonical().ListResidents(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(residents) != 1 || residents[0].Status != "active" {
		t.Fatalf("restore archive fixture residents = %+v", residents)
	}
	ids := canonical.NewSecureIDGenerator()
	writer, err := canonical.OpenWriter(ctx, canonical.WriterOptions{
		Backend: store.Canonical(), IDs: ids, Clock: canonical.SystemClock{},
		Timezone: canonical.MustTimezone("UTC"), QueueCapacity: 8,
	})
	if err != nil {
		t.Fatal(err)
	}
	transitionID, err := ids.New()
	if err != nil {
		_ = writer.Close(ctx)
		t.Fatal(err)
	}
	_, err = writer.Submit(ctx, domain.ArchiveResidentCommand(domain.ArchiveResident{
		ResidentID: residents[0].ResidentID, OwnerPrincipalID: residents[0].OwnerPrincipalID,
		StatusTransitionID: transitionID,
	}))
	if err != nil {
		_ = writer.Close(ctx)
		t.Fatal(err)
	}
	if err := writer.Close(ctx); err != nil {
		t.Fatal(err)
	}
}

func setRestoreRunningAttemptMax(t *testing.T, database string) {
	t.Helper()
	db := openRestoreSQL(t, database)
	defer db.Close()
	if _, err := db.Exec(`DROP TRIGGER trg_generation_run_outcomes_no_update`); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`UPDATE generation_run_outcomes SET attempt_no = ? WHERE state = 'running'`, int64(math.MaxInt64)); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`CREATE TRIGGER trg_generation_run_outcomes_no_update BEFORE UPDATE ON generation_run_outcomes BEGIN SELECT RAISE(ABORT, 'generation_run_outcomes is append-only'); END`); err != nil {
		t.Fatal(err)
	}
}

func addRestoreClaimAliases(t *testing.T, source string) (canonical.ID, canonical.ID) {
	t.Helper()
	activateRestoreMemoryPolicy(t, source)
	database := filepath.Join(source, DatabaseFilename)
	db := openRestoreSQL(t, database)
	defer db.Close()
	var residentRaw, principalRaw, commitRaw, runRaw string
	if err := db.QueryRow(`SELECT resident_id, principal_id FROM residents LIMIT 1`).Scan(&residentRaw, &principalRaw); err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRow(`SELECT canonical_commit_id FROM canonical_commits ORDER BY commit_seq DESC LIMIT 1`).Scan(&commitRaw); err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRow(`SELECT generation_run_id FROM generation_runs ORDER BY requested_at DESC, generation_run_id DESC LIMIT 1`).Scan(&runRaw); err != nil {
		t.Fatal(err)
	}
	residentID, err := canonical.ParseID(residentRaw)
	if err != nil {
		t.Fatal(err)
	}
	ids := canonical.NewSecureIDGenerator()
	contentID, _ := ids.New()
	claimA, _ := ids.New()
	claimB, _ := ids.New()
	statement := []byte("The restored resident retains one shared alias statement.")
	digest := canonical.HashBlob(statement)
	salt, err := canonical.NewContentSalt(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	commitment, err := canonical.CommitContent("claim_statement", salt, statement)
	if err != nil {
		t.Fatal(err)
	}
	normalized, err := memory.NormalizeStatementV1(string(statement))
	if err != nil {
		t.Fatal(err)
	}
	statementHash := canonical.HashBlob([]byte(normalized))
	objects, err := blob.NewFileStore(filepath.Join(source, "blobs"))
	if err != nil {
		t.Fatal(err)
	}
	staged, err := objects.Stage(context.Background(), residentID, strings.NewReader(string(statement)))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := objects.Finalize(context.Background(), residentID, staged); err != nil {
		t.Fatal(err)
	}
	now := time.Now().UnixMicro()
	tx, err := db.Begin()
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	if _, err := tx.Exec(`INSERT INTO blobs(
		dedupe_scope_id, hash_algorithm, blob_hash, content, byte_size, encoding, compression, created_at, created_tz
	) VALUES (?, 'sha256', ?, ?, ?, 'utf-8', 'none', ?, 'UTC')`, residentRaw, digest.Bytes(), statement, len(statement), now); err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec(`INSERT INTO content_objects(
		content_id, owner_resident_id, content_class, blob_hash, blob_hash_algorithm,
		commitment, commitment_salt, commitment_hash_algorithm, commitment_domain,
		canonicalization_version, erasure_state, erasure_policy, created_at, created_tz
	) VALUES (?, ?, 'claim_statement', ?, 'sha256', ?, ?, 'sha256',
		'mahoroba:content-commitment:v1', 'mahoroba-jcs-v1', 'present', 'independent', ?, 'UTC')`,
		contentID.String(), residentRaw, digest.Bytes(), commitment.Bytes(), salt.Bytes(), now); err != nil {
		t.Fatal(err)
	}
	for _, claimID := range []canonical.ID{claimA, claimB} {
		if _, err := tx.Exec(`INSERT INTO claims(
			claim_id, canonical_commit_id, owner_resident_id, subject_principal_id,
			perspective_principal_id, kind, temporal_kind, statement_content_id,
			statement_hash, statement_hash_algorithm, statement_normalization_version,
			created_by_run_id, recorded_at, recorded_tz
		) VALUES (?, ?, ?, ?, ?, 'direct', 'stable', ?, ?, 'sha256',
			'memory-normalization-v1', ?, ?, 'UTC')`, claimID.String(), commitRaw, residentRaw,
			principalRaw, principalRaw, contentID.String(), statementHash.Bytes(), runRaw, now); err != nil {
			t.Fatal(err)
		}
		validityID, _ := ids.New()
		if _, err := tx.Exec(`INSERT INTO claim_validity_assertions(
			validity_assertion_id, canonical_commit_id, claim_id, assertion_type,
			valid_from, valid_from_tz, valid_to, valid_to_tz, evidence_event_id,
			confidence, actor_principal_id, reason_code, reason_content_id, recorded_at, recorded_tz
		) VALUES (?, ?, ?, 'interval', NULL, NULL, NULL, NULL, NULL, 1000000, ?,
			'restore_alias_fixture', NULL, ?, 'UTC')`, validityID.String(), commitRaw,
			claimID.String(), principalRaw, now); err != nil {
			t.Fatal(err)
		}
		scopeID, _ := ids.New()
		if _, err := tx.Exec(`INSERT INTO claim_view_scope_assertions(
			view_scope_assertion_id, canonical_commit_id, claim_id, view_scope,
			actor_principal_id, generation_run_id, memory_policy_revision_id,
			reason_code, reason_content_id, recorded_at, recorded_tz
		) VALUES (?, ?, ?, 'resident_ui', ?, NULL, NULL, 'restore_alias_fixture', NULL, ?, 'UTC')`,
			scopeID.String(), commitRaw, claimID.String(), principalRaw, now); err != nil {
			t.Fatal(err)
		}
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	store, err := storesqlite.Open(context.Background(), database)
	if err != nil {
		t.Fatal(err)
	}
	preflightRestoreContentReferences(t, store)
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	return claimA, contentID
}

func preflightRestoreContentReferences(t *testing.T, store *storesqlite.Store) {
	t.Helper()
	registry, err := storesqlite.ActiveProjectionRegistry()
	if err != nil {
		t.Fatal(err)
	}
	repository := store.Projection()
	coordinator, err := projection.NewCoordinator(projection.CoordinatorOptions{
		Registry: registry, Source: repository, Store: repository,
		Clock: canonical.SystemClock{}, Timezone: canonical.MustTimezone("UTC"),
		ScanInterval: time.Hour, AsOfRefreshInterval: time.Hour,
		RebuildRetryInterval: time.Minute, MaxStaleness: time.Hour,
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := coordinator.PreflightContentReferences(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func activateRestoreMemoryPolicy(t *testing.T, source string) {
	t.Helper()
	ctx := context.Background()
	store, err := storesqlite.Open(ctx, filepath.Join(source, DatabaseFilename))
	if err != nil {
		t.Fatal(err)
	}
	objects, err := blob.NewFileStore(filepath.Join(source, "blobs"))
	if err != nil {
		_ = store.Close()
		t.Fatal(err)
	}
	residents, err := store.Canonical().ListResidents(ctx)
	if err != nil {
		_ = store.Close()
		t.Fatal(err)
	}
	var residentID canonical.ID
	for _, resident := range residents {
		if resident.Status == "active" {
			residentID = resident.ResidentID
			break
		}
	}
	if residentID.IsZero() {
		_ = store.Close()
		t.Fatal("restore fixture has no active resident")
	}
	ids := canonical.NewSecureIDGenerator()
	clock := canonical.SystemClock{}
	timezone := canonical.MustTimezone("UTC")
	writer, err := canonical.OpenWriter(ctx, canonical.WriterOptions{
		Backend: store.Canonical(), IDs: ids, Clock: clock, Timezone: timezone, QueueCapacity: 16,
	})
	if err != nil {
		_ = store.Close()
		t.Fatal(err)
	}
	application, err := app.New(app.Options{
		Writer: writer, Repository: store.Canonical(), IDs: ids, Clock: clock, Timezone: timezone,
		Blobs: objects, Provider: "fixture-provider", Model: "fixture-model",
		MaxAttempts: 3, RetryBackoff: []time.Duration{0, 0}, MaxInputBytes: 4096,
		MaxOutputBytes: 4096, SafetyScanInterval: time.Hour,
	})
	if err != nil {
		_ = writer.Close(ctx)
		_ = store.Close()
		t.Fatal(err)
	}
	if _, err := application.ActivateMemoryPolicyV4(ctx, app.ActivateMemoryPolicyV4Options{
		ResidentID: residentID, ExpectedFrom: memory.PolicyVersionV1,
		AcknowledgeRecallEnable: true, AcknowledgeSelfTalkExtraction: true,
	}); err != nil {
		_ = writer.Close(ctx)
		_ = store.Close()
		t.Fatal(err)
	}
	if err := writer.Close(ctx); err != nil {
		_ = store.Close()
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
}

func makeRestoreBundleHalfErased(t *testing.T, bundle string) {
	t.Helper()
	database := filepath.Join(bundle, filepath.FromSlash(backup.DatabaseBundlePath))
	db := openRestoreSQL(t, database)
	var triggerSQL string
	if err := db.QueryRow(`SELECT sql FROM sqlite_schema WHERE type = 'trigger' AND name = 'trg_claims_update_contract'`).Scan(&triggerSQL); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`DROP TRIGGER trg_claims_update_contract`); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`UPDATE claims SET statement_hash = NULL
		WHERE claim_id = (
			SELECT MIN(claim_id) FROM claims
			WHERE statement_content_id = (
				SELECT statement_content_id FROM claims
				GROUP BY statement_content_id HAVING COUNT(*) = 2
				ORDER BY statement_content_id LIMIT 1
			)
		)`); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(triggerSQL); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	databaseBytes, err := os.ReadFile(database)
	if err != nil {
		t.Fatal(err)
	}
	manifestPath := filepath.Join(bundle, "manifest.json")
	manifestBytes, err := os.ReadFile(manifestPath)
	if err != nil {
		t.Fatal(err)
	}
	var manifest backup.Manifest
	if err := json.Unmarshal(manifestBytes, &manifest); err != nil {
		t.Fatal(err)
	}
	databaseDigest := sha256.Sum256(databaseBytes)
	manifest.DatabaseFile.ByteSize = strconv.FormatInt(int64(len(databaseBytes)), 10)
	manifest.DatabaseFile.SHA256 = "sha256:" + hex.EncodeToString(databaseDigest[:])
	encoded, err := canonical.MarshalCanonical(manifest)
	if err != nil {
		t.Fatal(err)
	}
	manifestBytes = append(encoded.Bytes(), '\n')
	if err := os.WriteFile(manifestPath, manifestBytes, 0o600); err != nil {
		t.Fatal(err)
	}
	manifestDigest := sha256.Sum256(manifestBytes)
	if err := os.WriteFile(filepath.Join(bundle, "COMPLETE"), []byte("sha256:"+hex.EncodeToString(manifestDigest[:])+"\n"), 0o600); err != nil {
		t.Fatal(err)
	}
}

func openRestoreSQL(t *testing.T, path string) *sql.DB {
	t.Helper()
	db, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	if err := db.Ping(); err != nil {
		_ = db.Close()
		t.Fatal(err)
	}
	return db
}

func countRestoreRows(t *testing.T, database, table string) int {
	t.Helper()
	db := openRestoreSQL(t, database)
	defer db.Close()
	var count int
	if _, ok := map[string]struct{}{"canonical_commits": {}}[table]; !ok {
		t.Fatalf("unsupported count table %q", table)
	}
	if err := db.QueryRow(`SELECT COUNT(*) FROM canonical_commits`).Scan(&count); err != nil {
		t.Fatal(err)
	}
	return count
}

func mustRestoreSiblingMarker(t *testing.T, result Result) string {
	t.Helper()
	parts := strings.Split(strings.TrimPrefix(result.StagingBasename, "."), ".restore-staging.")
	if len(parts) != 2 {
		t.Fatalf("invalid restore staging basename %q", result.StagingBasename)
	}
	name, err := durablepublish.SiblingMarkerBasename(parts[0], result.RestoreID)
	if err != nil {
		t.Fatal(err)
	}
	return name
}

func assertRestorePath(t *testing.T, path string, exists bool) {
	t.Helper()
	_, err := os.Lstat(path)
	if exists && err != nil {
		t.Fatalf("%s is absent: %v", path, err)
	}
	if !exists && !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("%s unexpectedly exists: %v", path, err)
	}
}

func skipRestoreWindowsSandbox(t *testing.T, err error) {
	t.Helper()
	reason, hasReason := fssecure.Reason(err)
	knownACLCondition := errors.Is(err, os.ErrPermission) || hasReason &&
		(reason == fssecure.ReasonUnsafeACL || reason == fssecure.ReasonOwnerMismatch)
	if runtime.GOOS == "windows" && os.Getenv("CI") == "" && err != nil && knownACLCondition {
		t.Skip("Windows capability sandbox cannot exercise exact protected ACL; Hosted/non-sandbox test remains active")
	}
}
