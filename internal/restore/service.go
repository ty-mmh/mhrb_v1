package restore

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"slices"
	"strconv"
	"strings"
	"time"

	"mahoroba.local/mahoroba/internal/app"
	"mahoroba.local/mahoroba/internal/backup"
	"mahoroba.local/mahoroba/internal/blob"
	"mahoroba.local/mahoroba/internal/canonical"
	"mahoroba.local/mahoroba/internal/domain"
	"mahoroba.local/mahoroba/internal/durablepublish"
	"mahoroba.local/mahoroba/internal/fssecure"
	"mahoroba.local/mahoroba/internal/generation"
	"mahoroba.local/mahoroba/internal/integrityrun"
	"mahoroba.local/mahoroba/internal/namespacelock"
	"mahoroba.local/mahoroba/internal/operationalmetrics"
	"mahoroba.local/mahoroba/internal/projection"
	"mahoroba.local/mahoroba/internal/readiness"
	storesqlite "mahoroba.local/mahoroba/internal/store/sqlite"
)

const restoreFormatVersion = "mahoroba-restore-result-v1"

type restoreHooks struct {
	afterStaging     func(string) error
	beforeRecovery   func(string) error
	beforePublish    func(string, string) error
	publishFailpoint durablepublish.FailpointFunc
}

// Restore verifies an offline bundle, reconstructs one private data directory,
// runs the complete staged mutation/readiness gate, and atomically publishes
// it. It never opens a provider, listener, worker, or source-bundle writer.
func Restore(ctx context.Context, request Request) (Result, error) {
	return restore(ctx, request, restoreHooks{})
}

func restore(ctx context.Context, request Request, hooks restoreHooks) (result Result, resultErr error) {
	result = Result{FormatVersion: restoreFormatVersion, DatabaseFilename: DatabaseFilename}
	if request.Observer != nil {
		defer func() {
			outcome := operationalmetrics.OperationSucceeded
			if resultErr != nil {
				outcome = operationalmetrics.OperationFailed
				if result.Published || errors.Is(resultErr, ErrDurabilityUnknown) {
					outcome = operationalmetrics.OperationPartial
				}
			}
			request.Observer.OperationFinished(operationalmetrics.OperationRestore, outcome)
		}()
	}
	if ctx == nil {
		return result, errors.New("restore: nil context")
	}
	bundleRoot, target, err := preflightPaths(request)
	if err != nil {
		return result, err
	}

	targetLock, err := namespacelock.AcquireExistingParent(target)
	if err != nil {
		return result, classifyNamespace("lock target namespace", err)
	}
	defer func() { resultErr = errors.Join(resultErr, targetLock.Close()) }()
	exists, err := targetLock.TargetExists()
	if err != nil {
		return result, classifyNamespace("inspect target namespace", err)
	}
	verified, err := backup.Verify(ctx, bundleRoot)
	if err != nil {
		return result, fmt.Errorf("%w: %v", ErrInvalidBundle, err)
	}
	result.Bundle = verified
	sourceHead, err := sourceHead(verified.Manifest.CapturedHead)
	if err != nil {
		return result, fmt.Errorf("%w: captured head: %v", ErrInvalidBundle, err)
	}
	if exists {
		return recoverPublishedRestore(ctx, result, target, verified, sourceHead)
	}

	ids := canonical.NewSecureIDGenerator()
	restoreID, err := ids.New()
	if err != nil {
		return result, err
	}
	targetBase := filepath.Base(target)
	stagingBase := "." + targetBase + ".restore-staging." + restoreID.String()
	stagingPath := filepath.Join(filepath.Dir(target), stagingBase)
	result.RestoreID = restoreID
	result.StagingBasename = stagingBase

	stagingLock, err := namespacelock.AcquireExistingParent(stagingPath)
	if err != nil {
		return result, classifyNamespace("lock restore staging namespace", err)
	}
	defer func() { resultErr = errors.Join(resultErr, stagingLock.Close()) }()
	stagingExists, err := stagingLock.TargetExists()
	if err != nil {
		return result, classifyNamespace("inspect restore staging namespace", err)
	}
	if stagingExists {
		return result, fmt.Errorf("%w: generated staging already exists", ErrStagingFailed)
	}
	stagingTarget, err := stagingLock.OpenOrCreateTarget(0o700)
	if err != nil {
		return result, classifyNamespace("create restore staging", err)
	}
	if err := stagingTarget.Close(); err != nil {
		return result, fmt.Errorf("%w: close staging creation handle: %v", ErrArtifactIO, err)
	}

	policy, err := fssecure.CurrentSecurityPolicy()
	if err != nil {
		return result, fmt.Errorf("%w: resolve secure filesystem policy: %v", ErrArtifactIO, err)
	}
	root, err := fssecure.OpenRoot(stagingPath, policy)
	if err != nil {
		return result, fmt.Errorf("%w: open restore staging: %v", ErrArtifactIO, err)
	}
	rootOpen := true
	defer func() {
		if rootOpen {
			resultErr = errors.Join(resultErr, root.Close())
		}
	}()
	stagingIdentity := root.Identity()
	stagingMarker, _, err := encodeRestoreStagingMarker(verified.Manifest, restoreID, stagingBase, targetBase)
	if err != nil {
		return result, err
	}
	if err := writeManagedFile(root, RestoreStagingMarker, stagingMarker); err != nil {
		return result, fmt.Errorf("%w: write restore staging marker: %v", ErrArtifactIO, err)
	}
	if err := root.Sync(); err != nil {
		return result, fmt.Errorf("%w: sync restore staging marker: %v", ErrArtifactIO, err)
	}
	if hooks.afterStaging != nil {
		if err := hooks.afterStaging(stagingPath); err != nil {
			return result, err
		}
	}

	copied, err := backup.CopyVerified(ctx, bundleRoot, func(
		copyCtx context.Context,
		entry backup.BundleEntry,
		reader io.Reader,
	) error {
		return writeBundleEntry(copyCtx, root, entry.Path, entry.ByteSize, entry.SHA256, reader)
	})
	if err != nil {
		return result, fmt.Errorf("%w: copy verified bundle: %v", ErrInvalidBundle, err)
	}
	if err := sameVerifiedBundle(verified, copied); err != nil {
		return result, fmt.Errorf("%w: source bundle changed between verification and copy: %v", ErrInvalidBundle, err)
	}
	if err := root.VerifyBound(); err != nil || !stagingIdentity.Equal(root.Identity()) {
		return result, fmt.Errorf("%w: restore staging identity changed: %v", ErrStagingFailed, err)
	}
	if err := root.Sync(); err != nil {
		return result, fmt.Errorf("%w: sync copied bundle: %v", ErrArtifactIO, err)
	}

	private, err := runStagedDatabase(ctx, stagingPath, root, ids, request.Observer, hooks.beforeRecovery)
	if err != nil {
		return result, fmt.Errorf("%w: %w", ErrStagingFailed, err)
	}
	if err := root.VerifyBound(); err != nil || !stagingIdentity.Equal(root.Identity()) {
		return result, fmt.Errorf("%w: staged database changed root identity: %v", ErrStagingFailed, err)
	}
	if err := root.Sync(); err != nil {
		return result, fmt.Errorf("%w: sync staged database: %v", ErrArtifactIO, err)
	}
	if err := root.Close(); err != nil {
		return result, fmt.Errorf("%w: close staged filesystem authority: %v", ErrArtifactIO, err)
	}
	rootOpen = false

	root, err = fssecure.OpenRootForPublish(stagingPath, policy)
	if err != nil {
		return result, fmt.Errorf("%w: reopen staged directory for publication: %v", ErrArtifactIO, err)
	}
	rootOpen = true
	if !stagingIdentity.Equal(root.Identity()) {
		return result, fmt.Errorf("%w: staged directory identity differs before publish", ErrStagingFailed)
	}
	payload, err := durablepublish.DirectoryPayloadDigest(ctx, root, RestoreStagingMarker)
	if err != nil {
		return result, fmt.Errorf("%w: hash staged payload: %v", ErrArtifactIO, err)
	}
	producerInput, err := restoreProducerInputDigest(copied)
	if err != nil {
		return result, err
	}
	publishMarker, err := durablepublish.NewMarker(
		restoreID, durablepublish.VariantDirectory, RestoreProducerCommand,
		producerInput, targetBase, stagingBase, payload.SHA256,
	)
	if err != nil {
		return result, err
	}
	if hooks.beforePublish != nil {
		if err := hooks.beforePublish(stagingPath, target); err != nil {
			return result, err
		}
	}
	published, err := durablepublish.PublishDirectory(ctx, durablepublish.DirectoryRequest{
		Staging: root, Marker: publishMarker,
		CallerMarkers: []string{RestoreStagingMarker}, Failpoint: hooks.publishFailpoint,
	})
	if err != nil {
		if errors.Is(err, durablepublish.ErrTargetExists) {
			return result, ErrTargetExists
		}
		if errors.Is(err, durablepublish.ErrDurabilityUnknown) {
			return result, fmt.Errorf("%w: %v", ErrDurabilityUnknown, err)
		}
		return result, fmt.Errorf("%w: publish restored directory: %v", ErrArtifactIO, err)
	}
	closeErr := root.Close()
	rootOpen = closeErr != nil

	// Canonical staging effects become live only after the full marker/barrier
	// protocol succeeds. No pre-publication return path exposes these fields.
	result.SourceHead = sourceHead
	result.RestoredHead = private.restoredHead
	result.TerminalizedAttempts = private.terminalizedAttempts
	result.CancelledMandatoryWork = private.cancelledMandatoryWork
	result.CreatedIntegrityFindings = private.createdIntegrityFindings
	result.CreatedQuarantines = private.createdQuarantines
	result.ProjectionsRebuilt = private.projectionsRebuilt
	result.FileCount = published.Payload.FileCount
	result.ByteCount = published.Payload.ByteCount
	result.Published = true
	result.CanonicalCommits = private.commits
	result.CanonicalApplied = len(private.commits) != 0
	result.ServiceReady = private.serviceReady
	result.ReadinessReasons = private.readinessReasons
	if closeErr != nil {
		return result, fmt.Errorf("%w: close published directory authority: %v", ErrDurabilityUnknown, closeErr)
	}
	return result, nil
}

func recoverPublishedRestore(
	ctx context.Context,
	result Result,
	target string,
	verified backup.VerifiedBundle,
	source readiness.Head,
) (_ Result, resultErr error) {
	policy, err := fssecure.CurrentSecurityPolicy()
	if err != nil {
		return result, fmt.Errorf("%w: resolve recovery filesystem policy: %v", ErrArtifactIO, err)
	}
	root, err := fssecure.OpenRootForPublish(target, policy)
	if err != nil {
		return result, ErrTargetExists
	}
	rootOpen := true
	defer func() {
		if rootOpen {
			resultErr = errors.Join(resultErr, root.Close())
		}
	}()
	input, err := restoreProducerInputDigest(verified)
	if err != nil {
		return result, err
	}
	marker, err := durablepublish.DiscoverPublishedDirectoryMarker(
		root, durablepublish.CommandBackupRestore, "sha256:"+input.Hex(),
	)
	if err != nil {
		if errors.Is(err, durablepublish.ErrRecoveryMarkerAbsent) {
			return result, ErrTargetExists
		}
		if errors.Is(err, durablepublish.ErrDurabilityUnknown) {
			return result, fmt.Errorf("%w: %v", ErrDurabilityUnknown, err)
		}
		return result, ErrTargetExists
	}
	wantStaging := "." + marker.TargetBasename + ".restore-staging." + marker.PublishID.String()
	if marker.StagingBasename != wantStaging {
		return result, fmt.Errorf("%w: restore staging marker identity differs", ErrDurabilityUnknown)
	}
	payload, err := durablepublish.DirectoryPayloadDigest(ctx, root, RestoreStagingMarker)
	if err != nil {
		return result, fmt.Errorf("%w: inspect recovered payload: %v", ErrDurabilityUnknown, err)
	}
	if marker.PayloadSHA256 != "sha256:"+payload.SHA256.Hex() {
		return result, fmt.Errorf("%w: recovered payload differs", ErrDurabilityUnknown)
	}
	// Inspect through this retained publication root. The database is opened
	// parent-handle-relative, so Windows sharing remains compatible without
	// dropping namespace authority or introducing a close/reopen ABA window.
	private, err := inspectRecoveredRestore(ctx, root)
	if err != nil {
		return result, fmt.Errorf("%w: recovered target validation: %v", ErrDurabilityUnknown, err)
	}
	recovered, err := durablepublish.RecoverPublishedDirectory(ctx, durablepublish.PublishedDirectoryRecoveryRequest{
		Target: root, Expected: marker, CallerMarkers: []string{RestoreStagingMarker},
	})
	if err != nil {
		return result, fmt.Errorf("%w: complete recovered publication: %v", ErrDurabilityUnknown, err)
	}
	closeErr := root.Close()
	rootOpen = closeErr != nil
	result.Bundle = verified
	result.SourceHead = source
	result.RestoredHead = private.restoredHead
	result.FileCount = recovered.Payload.FileCount
	result.ByteCount = recovered.Payload.ByteCount
	result.Published = true
	result.CanonicalApplied = false
	result.CanonicalCommits = []Commit{}
	result.ServiceReady = private.serviceReady
	result.ReadinessReasons = private.readinessReasons
	result.RestoreID = marker.PublishID
	result.StagingBasename = marker.StagingBasename
	if closeErr != nil {
		return result, fmt.Errorf("%w: close recovered publication authority: %v", ErrDurabilityUnknown, closeErr)
	}
	return result, nil
}

func inspectRecoveredRestore(ctx context.Context, root *fssecure.Directory) (_ stagedResult, resultErr error) {
	var result stagedResult
	databaseBoundary, err := storesqlite.OpenReadOnlyBoundDatabaseFromRoot(ctx, root, DatabaseFilename)
	if err != nil {
		return result, err
	}
	defer func() { resultErr = errors.Join(resultErr, databaseBoundary.Close()) }()
	inspection := databaseBoundary.Inspection()
	objects, err := blob.OpenFileStoreReadOnly(filepath.Join(databaseBoundary.DataDir(), "blobs"))
	if err != nil {
		return result, err
	}
	if err := databaseBoundary.Verify(); err != nil {
		return result, err
	}
	if err := inspection.MinimumCheckerWithBlobObjects(objects).Check(ctx); err != nil {
		return result, err
	}
	if err := inspection.RequireCurrentContentReferences(ctx); err != nil {
		return result, err
	}
	if err := verifyCanonical(ctx, inspection.Canonical()); err != nil {
		return result, err
	}
	ready, err := readiness.EvaluateServiceReadiness(ctx, inspection.ServiceReadinessSource(), readiness.Request{
		StartupComplete: true, Projection: recoveredProjectionChecker(inspection),
	})
	if err != nil {
		return result, err
	}
	result.restoredHead = ready.CapturedHead
	result.serviceReady = ready.Ready
	result.readinessReasons = slices.Clone(ready.ReasonCodes)
	if err := databaseBoundary.Verify(); err != nil {
		return stagedResult{}, err
	}
	return result, nil
}

type stagedResult struct {
	restoredHead             readiness.Head
	terminalizedAttempts     int
	cancelledMandatoryWork   int
	createdIntegrityFindings int
	createdQuarantines       int
	projectionsRebuilt       int
	commits                  []Commit
	serviceReady             bool
	readinessReasons         []readiness.ReasonCode
}

func runStagedDatabase(
	ctx context.Context,
	stagingPath string,
	root *fssecure.Directory,
	ids *canonical.IDGenerator,
	observer Observer,
	beforeRecovery func(string) error,
) (_ stagedResult, resultErr error) {
	var result stagedResult
	// This pathname open is confined to the newly-created private staging
	// namespace: the caller retains both its namespace lock and root handle,
	// verifies the root identity around this call, and verifyClosedStagingRoot
	// rejects every unexpected entry after SQLite and its sidecars are closed.
	// It is not an online/public source reopen and therefore must not use the
	// descriptor-backed read surface (which cannot resolve SQLite WAL sidecars).
	store, err := storesqlite.Open(ctx, filepath.Join(stagingPath, DatabaseFilename))
	if err != nil {
		return result, fmt.Errorf("open and migrate staged database: %w", err)
	}
	storeOpen := true
	defer func() {
		if storeOpen {
			resultErr = errors.Join(resultErr, store.Close())
		}
	}()
	blobs, err := blob.NewFileStore(filepath.Join(stagingPath, "blobs"))
	if err != nil {
		return result, fmt.Errorf("open staged blob boundary: %w", err)
	}
	if observer != nil {
		blobs.SetRecoveryObserver(observer)
	}
	if err := store.MinimumCheckerWithBlobObjects(blobs).Check(ctx); err != nil {
		return result, fmt.Errorf("initial MinimumCheck: %w", err)
	}
	if err := verifyCanonical(ctx, store.Canonical()); err != nil {
		return result, err
	}
	if beforeRecovery != nil {
		if err := beforeRecovery(filepath.Join(stagingPath, DatabaseFilename)); err != nil {
			return result, err
		}
	}

	clock := canonical.SystemClock{}
	timezone := canonical.MustTimezone("UTC")
	writer, err := canonical.OpenWriter(ctx, canonical.WriterOptions{
		Backend: store.Canonical(), IDs: ids, Clock: clock,
		Timezone: timezone, QueueCapacity: 64, Observer: observer,
	})
	if err != nil {
		return result, err
	}
	writerOpen := true
	defer func() {
		if writerOpen {
			resultErr = errors.Join(resultErr, writer.Close(context.Background()))
		}
	}()
	registration, err := registerRestoredDialogueV3(ctx, writer, ids)
	if err != nil {
		return result, fmt.Errorf("restore: register current dialogue pipeline: %w", err)
	}
	if !registration.Commit.CommitID.IsZero() {
		appendRestoreCommit(&result.commits, Commit{
			Metadata: registration.Commit, Disposition: CommitCreated,
			Effects: CommitEffects{PipelineVersionRegistered: true},
		})
	}
	application, err := app.New(app.Options{
		Writer: writer, Repository: store.Canonical(), IDs: ids, Clock: clock,
		Timezone: timezone, Blobs: blobs, Generator: nil,
		Provider: "mahoroba-internal", Model: "not-dispatched",
		StructuredOutputMode: generation.StructuredOutputPrompt,
		MaxAttempts:          3, RetryBackoff: []time.Duration{0, 0},
		MaxInputBytes: 1, MaxOutputBytes: 1, SafetyScanInterval: time.Hour,
		OperationalObserver: observer,
	})
	if err != nil {
		return result, err
	}
	if err := application.PreflightMandatory(ctx); err != nil {
		return result, err
	}
	running, err := application.RecoverRunning(ctx)
	if err != nil {
		return result, err
	}
	appendRecoveryCommits(&result.commits, running.CanonicalCommits, CommitEffects{
		GenerationAttemptTerminalized: true,
	})
	result.terminalizedAttempts += running.TerminalizedAttempts

	integrityService, err := integrityrun.New(integrityrun.Options{
		Writer: writer, Repository: store.Canonical(), Scanner: store.IntegrityScanner(), IDs: ids,
	})
	if err != nil {
		return result, err
	}
	firstScan, err := integrityService.Run(ctx, nil)
	appendIntegrityCommits(&result.commits, firstScan)
	result.createdIntegrityFindings += firstScan.CreatedFindings
	result.createdQuarantines += firstScan.CreatedQuarantines
	if err != nil {
		return result, err
	}
	mandatory, err := application.RecoverMandatory(ctx)
	appendRecoveryCommits(&result.commits, mandatory.CanonicalCommits, CommitEffects{
		GenerationAttemptTerminalized: true,
		MandatoryWorkCancelled:        true,
	})
	result.terminalizedAttempts += mandatory.TerminalizedAttempts
	result.cancelledMandatoryWork += mandatory.CancelledMandatoryWork
	if err != nil {
		return result, err
	}
	secondScan, err := integrityService.Run(ctx, nil)
	appendIntegrityCommits(&result.commits, secondScan)
	result.createdIntegrityFindings += secondScan.CreatedFindings
	result.createdQuarantines += secondScan.CreatedQuarantines
	if err != nil {
		return result, err
	}

	proof, rebuilt, err := rebuildEligibleProjections(ctx, store, clock, timezone, observer)
	if err != nil {
		return result, err
	}
	result.projectionsRebuilt = rebuilt
	if err := store.RequireCurrentContentReferences(ctx); err != nil {
		return result, fmt.Errorf("content references publication gate: %w", err)
	}
	if err := store.MinimumCheckerWithBlobObjects(blobs).Check(ctx); err != nil {
		return result, fmt.Errorf("final MinimumCheck: %w", err)
	}
	if err := verifyCanonical(ctx, store.Canonical()); err != nil {
		return result, err
	}
	ready, err := readiness.EvaluateServiceReadiness(ctx, store.ServiceReadinessSource(), readiness.Request{
		StartupComplete: true, Projection: proof,
	})
	if err != nil {
		return result, err
	}
	result.restoredHead = ready.CapturedHead
	result.serviceReady = ready.Ready
	result.readinessReasons = slices.Clone(ready.ReasonCodes)

	if err := writer.Close(ctx); err != nil {
		return result, err
	}
	writerOpen = false
	if err := store.Close(); err != nil {
		return result, err
	}
	storeOpen = false
	if err := verifyClosedStagingRoot(root); err != nil {
		return result, err
	}
	if err := root.VerifyBound(); err != nil {
		return result, err
	}
	return result, nil
}

func verifyClosedStagingRoot(root *fssecure.Directory) error {
	entries, err := root.ReadDir()
	if err != nil {
		return err
	}
	seen := make(map[string]bool, 3)
	for _, entry := range entries {
		name := entry.Name()
		switch name {
		case DatabaseFilename, RestoreStagingMarker:
			if entry.IsDir() {
				return fmt.Errorf("restore: closed staging entry %s has the wrong kind", name)
			}
		case "blobs":
			if !entry.IsDir() {
				return errors.New("restore: closed staging blobs entry is not a directory")
			}
		default:
			return fmt.Errorf("restore: closed staging contains unexpected root entry %q", name)
		}
		if seen[name] {
			return fmt.Errorf("restore: closed staging duplicates root entry %q", name)
		}
		seen[name] = true
	}
	for _, required := range []string{DatabaseFilename, RestoreStagingMarker, "blobs"} {
		if !seen[required] {
			return fmt.Errorf("restore: closed staging is missing %s", required)
		}
	}
	return nil
}

func registerRestoredDialogueV3(
	ctx context.Context,
	writer *canonical.Writer,
	ids *canonical.IDGenerator,
) (canonical.CommandResult, error) {
	pipelineID, err := ids.New()
	if err != nil {
		return canonical.CommandResult{}, err
	}
	definition, err := domain.DialoguePipelineDefinition(pipelineID, domain.DialoguePipelineVersionV3)
	if err != nil {
		return canonical.CommandResult{}, err
	}
	return writer.Submit(ctx, domain.RegisterPipelineVersionsCommand(domain.RegisterPipelineVersions{
		Versions: []domain.PipelineVersionDefinition{definition},
	}))
}

func rebuildEligibleProjections(
	ctx context.Context,
	store *storesqlite.Store,
	clock canonical.Clock,
	timezone canonical.Timezone,
	observer Observer,
) (readiness.ProjectionChecker, int, error) {
	registry, err := storesqlite.ActiveProjectionRegistry()
	if err != nil {
		return nil, 0, err
	}
	repository := store.Projection()
	mutations := &projectionMutationCounter{delegate: repository}
	coordinator, err := projection.NewCoordinator(projection.CoordinatorOptions{
		Registry: registry, Source: repository, Store: mutations, Clock: clock, Timezone: timezone,
		ScanInterval: time.Hour, AsOfRefreshInterval: time.Hour,
		RebuildRetryInterval: time.Minute, MaxStaleness: time.Hour, Observer: observer,
	})
	if err != nil {
		return nil, 0, err
	}
	snapshot, err := store.ServiceReadinessSource().CaptureServiceReadiness(ctx)
	if err != nil {
		return nil, 0, err
	}
	residents, err := store.Canonical().ListResidents(ctx)
	if err != nil {
		return nil, 0, err
	}
	var proof *readiness.CanonicalProjectionProof
	universal := []projection.Name{projection.ResidentCurrentStatusName, projection.ResidentCurrentRevisionName}
	for _, resident := range residents {
		if _, err := coordinator.PreflightResident(ctx, resident.ResidentID, universal); err != nil {
			return nil, 0, err
		}
		selectedActive := snapshot.ActiveResidentID != nil && *snapshot.ActiveResidentID == resident.ResidentID &&
			snapshot.ActiveResidentStatus == "active"
		dependenciesPresent := resident.Status != "erased" &&
			!resident.PrinciplesRevisionID.IsZero() && resident.Principles != "" &&
			!resident.PersonaRevisionID.IsZero() && resident.Persona != "" &&
			!resident.MemoryPolicyRevisionID.IsZero() && resident.MemoryPolicy != ""
		if selectedActive && snapshot.SessionPolicyID == nil {
			dependenciesPresent = false
		}
		if dependenciesPresent {
			preflight, err := coordinator.PreflightServiceResident(ctx, resident.ResidentID)
			if err != nil {
				if !errors.Is(err, projection.ErrUnresolvedDependency) {
					return nil, 0, err
				}
				if err := dropInapplicableProjections(ctx, mutations, registry, resident.ResidentID); err != nil {
					return nil, 0, err
				}
				continue
			}
			if selectedActive && snapshot.SessionPolicyID != nil {
				proof = &readiness.CanonicalProjectionProof{
					ResidentID: resident.ResidentID, SessionPolicyID: *snapshot.SessionPolicyID,
					Head: preflight.Target.Head, IsCurrent: true,
				}
			}
			continue
		}
		if err := dropInapplicableProjections(ctx, mutations, registry, resident.ResidentID); err != nil {
			return nil, 0, err
		}
	}
	if _, err := coordinator.PreflightContentReferences(ctx); err != nil {
		return nil, 0, fmt.Errorf("restore: content references preflight: %w", err)
	}
	if proof != nil {
		return *proof, mutations.count, nil
	}
	return readiness.ProjectionCheckFunc(func(context.Context, readiness.ProjectionRequirement) (bool, error) {
		return false, nil
	}), mutations.count, nil
}

type projectionMutationCounter struct {
	delegate projection.Store
	count    int
}

func (counter *projectionMutationCounter) Watermark(
	ctx context.Context,
	name projection.Name,
	residentID canonical.ID,
) (projection.Watermark, bool, error) {
	return counter.delegate.Watermark(ctx, name, residentID)
}

func (counter *projectionMutationCounter) Apply(ctx context.Context, request projection.ApplyRequest) error {
	if err := counter.delegate.Apply(ctx, request); err != nil {
		return err
	}
	counter.count++
	return nil
}

func (counter *projectionMutationCounter) Drop(ctx context.Context, request projection.DropRequest) error {
	if err := counter.delegate.Drop(ctx, request); err != nil {
		return err
	}
	counter.count++
	return nil
}

func dropInapplicableProjections(
	ctx context.Context,
	repository projection.Store,
	registry *projection.Registry,
	residentID canonical.ID,
) error {
	for _, name := range []projection.Name{
		projection.RuntimeStatesName, projection.ClaimStatesName, projection.ClaimViewScopeCurrentName,
	} {
		definition, err := registry.Definition(name)
		if err != nil {
			return err
		}
		observed, exists, err := repository.Watermark(ctx, name, residentID)
		if err != nil {
			return err
		}
		if !exists {
			continue
		}
		if err := repository.Drop(ctx, projection.DropRequest{
			Definition: definition, ResidentID: residentID, Observed: &observed,
		}); err != nil {
			return err
		}
	}
	return nil
}

func recoveredProjectionChecker(inspection *storesqlite.Inspection) readiness.ProjectionChecker {
	return readiness.ProjectionCheckFunc(func(ctx context.Context, requirement readiness.ProjectionRequirement) (bool, error) {
		registry, err := storesqlite.ActiveProjectionRegistry()
		if err != nil {
			return false, err
		}
		surface := inspection.Projection()
		head, err := surface.Head(ctx)
		if err != nil {
			return false, err
		}
		if head != requirement.CapturedHead.Canonical() {
			return false, nil
		}
		for _, name := range projection.ServiceRequiredNames() {
			definition, err := registry.Definition(name)
			if err != nil {
				return false, err
			}
			watermark, exists, err := surface.Watermark(ctx, name, requirement.ResidentID)
			if err != nil {
				return false, err
			}
			if !exists || watermark.ProjectionName != name || watermark.ResidentID != requirement.ResidentID ||
				watermark.ProjectionVersion != definition.Version || watermark.SourceCommitSeq != requirement.CapturedHead.CommitSeq {
				return false, nil
			}
			target := projection.Target{
				Head: requirement.CapturedHead.Canonical(), AsOf: watermark.AsOf, AsOfTZ: watermark.AsOfTZ,
			}
			desired, err := surface.ResolveDependencies(ctx, projection.DependencyRequest{
				Definition: definition, ResidentID: requirement.ResidentID, Target: target, Previous: &watermark,
			})
			if err != nil {
				if errors.Is(err, projection.ErrUnresolvedDependency) {
					return false, nil
				}
				return false, err
			}
			equal, err := projection.DependencySetEqual(watermark.Dependencies, desired)
			if err != nil {
				return false, err
			}
			if !equal {
				return false, nil
			}
		}
		return true, nil
	})
}

type ledgerRepository interface {
	canonical.LedgerSource
	canonical.EnvelopeValidator
	ListResidentIDs(context.Context) ([]canonical.ID, error)
}

func verifyCanonical(ctx context.Context, repository ledgerRepository) error {
	residentIDs, err := repository.ListResidentIDs(ctx)
	if err != nil {
		return err
	}
	verifier := canonical.LedgerVerifier{EnvelopeValidator: repository}
	for _, residentID := range residentIDs {
		if _, err := verifier.Verify(ctx, repository, residentID); err != nil {
			return fmt.Errorf("restore: verify resident %s Canonical ledger: %w", residentID, err)
		}
	}
	return nil
}

func appendIntegrityCommits(target *[]Commit, result integrityrun.Result) {
	for _, commit := range result.CanonicalCommits {
		disposition := CommitExisting
		if commit.Disposition == integrityrun.DispositionCreated {
			disposition = CommitCreated
		}
		appendRestoreCommit(target, Commit{
			Metadata:    commit.Metadata,
			Disposition: disposition,
			Effects: CommitEffects{
				PipelineVersionRegistered: commit.Effects.PipelineVersionRegistered,
				IntegrityFindingRecorded:  commit.Effects.IntegrityFindingRecorded,
				ClaimStatusQuarantined:    commit.Effects.ClaimStatusQuarantined,
			},
		})
	}
}

func appendRecoveryCommits(target *[]Commit, values []canonical.CommitMetadata, effects CommitEffects) {
	for _, value := range values {
		appendRestoreCommit(target, Commit{
			Metadata: value, Disposition: CommitCreated, Effects: effects,
		})
	}
}

func appendRestoreCommit(target *[]Commit, value Commit) {
	if value.Metadata.CommitID.IsZero() {
		return
	}
	for index := range *target {
		existing := &(*target)[index]
		if existing.Metadata.CommitID != value.Metadata.CommitID {
			continue
		}
		if value.Disposition == CommitCreated {
			existing.Disposition = CommitCreated
		}
		existing.Effects.GenerationAttemptTerminalized = existing.Effects.GenerationAttemptTerminalized || value.Effects.GenerationAttemptTerminalized
		existing.Effects.MandatoryWorkCancelled = existing.Effects.MandatoryWorkCancelled || value.Effects.MandatoryWorkCancelled
		existing.Effects.PipelineVersionRegistered = existing.Effects.PipelineVersionRegistered || value.Effects.PipelineVersionRegistered
		existing.Effects.IntegrityFindingRecorded = existing.Effects.IntegrityFindingRecorded || value.Effects.IntegrityFindingRecorded
		existing.Effects.ClaimStatusQuarantined = existing.Effects.ClaimStatusQuarantined || value.Effects.ClaimStatusQuarantined
		return
	}
	*target = append(*target, value)
	slices.SortFunc(*target, func(left, right Commit) int {
		if left.Metadata.CommitSeq < right.Metadata.CommitSeq {
			return -1
		}
		if left.Metadata.CommitSeq > right.Metadata.CommitSeq {
			return 1
		}
		return 0
	})
}

func restoreProducerInputDigest(bundle backup.VerifiedBundle) (canonical.Digest, error) {
	if bundle.RootIdentitySHA256 == "" {
		return canonical.Digest{}, errors.New("restore: verified bundle lacks root identity digest")
	}
	manifestSHA, err := manifestDigest(bundle.Manifest)
	if err != nil {
		return canonical.Digest{}, err
	}
	return durablepublish.ProducerInputDigest(durablepublish.CommandBackupRestore, durablepublish.BackupRestoreProducerInput{
		BundleRootIdentity:     bundle.RootIdentitySHA256,
		BundleManifestSHA256:   "sha256:" + manifestSHA.Hex(),
		TargetConfigSHA256:     nil,
		TargetDatabaseFilename: DatabaseFilename,
	})
}

func sourceHead(head backup.CapturedHead) (readiness.Head, error) {
	if !head.Exists {
		return readiness.Head{}, nil
	}
	if head.CommitID == nil || head.CommitSeq == nil || head.CommittedAtUnixMicros == nil || head.CommittedTZ == nil {
		return readiness.Head{}, errors.New("present captured head is incomplete")
	}
	commitID, err := canonical.ParseID(*head.CommitID)
	if err != nil {
		return readiness.Head{}, err
	}
	seqValue, err := strconv.ParseInt(*head.CommitSeq, 10, 64)
	if err != nil {
		return readiness.Head{}, err
	}
	seq, err := canonical.NewCommitSeq(seqValue)
	if err != nil {
		return readiness.Head{}, err
	}
	instantValue, err := strconv.ParseInt(*head.CommittedAtUnixMicros, 10, 64)
	if err != nil {
		return readiness.Head{}, err
	}
	timezone, err := canonical.ParseTimezone(*head.CommittedTZ)
	if err != nil {
		return readiness.Head{}, err
	}
	result := readiness.Head{
		Exists: true, CommitID: commitID, CommitSeq: seq,
		CommittedAt: canonical.Instant(instantValue), CommittedTZ: timezone,
	}
	return result, result.Validate()
}

func sameVerifiedBundle(left, right backup.VerifiedBundle) error {
	if !left.RootIdentity.Equal(right.RootIdentity) || left.RootIdentitySHA256 != right.RootIdentitySHA256 ||
		left.FileCount != right.FileCount || left.ByteCount != right.ByteCount {
		return errors.New("verified bundle identity or counts differ")
	}
	leftManifest, err := canonical.MarshalCanonical(left.Manifest)
	if err != nil {
		return err
	}
	rightManifest, err := canonical.MarshalCanonical(right.Manifest)
	if err != nil {
		return err
	}
	if leftManifest.String() != rightManifest.String() {
		return errors.New("verified bundle manifest differs")
	}
	return nil
}

func preflightPaths(request Request) (bundle, target string, err error) {
	if request.BundleRoot == "" || !filepath.IsAbs(request.BundleRoot) ||
		request.TargetDataDir == "" || !filepath.IsAbs(request.TargetDataDir) {
		return "", "", errors.New("restore: bundle and target must be absolute paths")
	}
	bundle, err = canonicalExistingDirectory(request.BundleRoot)
	if err != nil {
		return "", "", fmt.Errorf("%w: bundle root: %v", ErrInvalidBundle, err)
	}
	cleanTarget := filepath.Clean(request.TargetDataDir)
	targetBase := filepath.Base(cleanTarget)
	if targetBase == "." || targetBase == string(filepath.Separator) || targetBase == "" {
		return "", "", errors.New("restore: target must name an absent child")
	}
	parent, err := canonicalExistingDirectory(filepath.Dir(cleanTarget))
	if err != nil {
		return "", "", fmt.Errorf("restore: target parent: %w", err)
	}
	target = filepath.Join(parent, targetBase)
	if pathsOverlap(bundle, target) {
		return "", "", ErrUnsafeOverlap
	}
	return bundle, target, nil
}

func canonicalExistingDirectory(value string) (string, error) {
	resolved, err := filepath.EvalSymlinks(filepath.Clean(value))
	if err != nil {
		return "", err
	}
	resolved, err = filepath.Abs(resolved)
	if err != nil {
		return "", err
	}
	info, err := os.Lstat(resolved)
	if err != nil || !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
		return "", errors.New("path is not a stable directory")
	}
	return filepath.Clean(resolved), nil
}

func pathsOverlap(left, right string) bool {
	left = filepath.Clean(left)
	right = filepath.Clean(right)
	if runtime.GOOS == "windows" {
		left = strings.ToLower(left)
		right = strings.ToLower(right)
	}
	return left == right || containsPath(left, right) || containsPath(right, left)
}

func containsPath(parent, child string) bool {
	relative, err := filepath.Rel(parent, child)
	return err == nil && relative != "." && relative != ".." && !filepath.IsAbs(relative) &&
		!strings.HasPrefix(relative, ".."+string(filepath.Separator))
}

func classifyNamespace(operation string, err error) error {
	switch {
	case errors.Is(err, namespacelock.ErrBusy):
		return fmt.Errorf("%w: %s", ErrNamespaceBusy, operation)
	case errors.Is(err, namespacelock.ErrUnsafeTarget), errors.Is(err, namespacelock.ErrUnsupported):
		return fmt.Errorf("%w: %s: %v", ErrArtifactIO, operation, err)
	default:
		return fmt.Errorf("%w: %s: %v", ErrArtifactIO, operation, err)
	}
}

var _ domain.Repository = (*storesqlite.CanonicalRepository)(nil)
