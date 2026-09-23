package backup

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"slices"
	"strconv"
	"strings"

	"mahoroba.local/mahoroba/internal/blob"
	"mahoroba.local/mahoroba/internal/canonical"
	"mahoroba.local/mahoroba/internal/durablepublish"
	"mahoroba.local/mahoroba/internal/fssecure"
	"mahoroba.local/mahoroba/internal/hostlock"
	"mahoroba.local/mahoroba/internal/namespacelock"
	"mahoroba.local/mahoroba/internal/operationalmetrics"
	"mahoroba.local/mahoroba/internal/readiness"
	storesqlite "mahoroba.local/mahoroba/internal/store/sqlite"
)

type CreateRequest struct {
	SourceDataDir      string
	DatabaseFilename   string
	Output             string
	IncludeProjections bool
	CreatedBy          CreatedBy
	Observer           interface {
		OperationFinished(operationalmetrics.Operation, operationalmetrics.OperationResult)
	}
	publishFailpoint durablepublish.FailpointFunc
}

type Result struct {
	ArtifactPath           string
	FormatVersion          string
	CapturedHead           CapturedHead
	ProjectionsIncluded    bool
	FileCount              int64
	ByteCount              int64
	AuthenticityGuaranteed bool
}

func Create(ctx context.Context, request CreateRequest) (_ Result, resultErr error) {
	if request.Observer != nil {
		defer func() {
			outcome := operationalmetrics.OperationSucceeded
			if resultErr != nil {
				outcome = operationalmetrics.OperationFailed
				if errors.Is(resultErr, ErrDurabilityUnknown) {
					outcome = operationalmetrics.OperationPartial
				}
			}
			request.Observer.OperationFinished(operationalmetrics.OperationBackup, outcome)
		}()
	}
	if ctx == nil {
		return Result{}, errors.New("backup: nil create context")
	}
	sourceDir, _, output, err := preflightCreatePaths(request)
	if err != nil {
		return Result{}, err
	}
	lock, err := hostlock.Acquire(sourceDir)
	if err != nil {
		return Result{}, fmt.Errorf("%w: acquire source host lock: %v", ErrSourceUnavailable, err)
	}
	defer func() { resultErr = errors.Join(resultErr, lock.Close()) }()
	policy, err := fssecure.CurrentSecurityPolicy()
	if err != nil {
		return Result{}, classifySecurity("resolve artifact security policy", err)
	}
	databaseBoundary, err := storesqlite.OpenReadOnlyBoundDatabase(
		ctx, sourceDir, request.DatabaseFilename,
	)
	if err != nil {
		return Result{}, classifySourceSecurity("open source database boundary", err)
	}
	sourcePublicationCommitted := false
	defer func() {
		closeErr := databaseBoundary.Close()
		if closeErr != nil && sourcePublicationCommitted {
			closeErr = errors.Join(ErrDurabilityUnknown, closeErr)
		}
		resultErr = errors.Join(resultErr, closeErr)
	}()
	sourceIdentity, err := databaseBoundary.SourceIdentityDigest()
	if err != nil {
		return Result{}, classifySourceSecurity("bind source data identity", err)
	}
	inspection := databaseBoundary.Inspection()
	sourceBlobs, err := blob.OpenFileStoreReadOnly(filepath.Join(sourceDir, "blobs"))
	if err != nil {
		return Result{}, classifySecurity("open source blob boundary", err)
	}
	if err := inspection.MinimumCheckerWithBlobObjects(sourceBlobs).Check(ctx); err != nil {
		return Result{}, fmt.Errorf("%w: source MinimumCheck: %v", ErrSourceUnavailable, err)
	}
	if err := inspection.RequireCurrentContentReferences(ctx); err != nil {
		return Result{}, fmt.Errorf("%w: content references publication gate: %v", ErrSourceUnavailable, err)
	}
	readinessSnapshot, err := inspection.ServiceReadinessSource().CaptureServiceReadiness(ctx)
	if err != nil {
		return Result{}, fmt.Errorf("%w: capture source head: %v", ErrSourceUnavailable, err)
	}
	producerInput, err := backupProducerInputDigest(
		sourceIdentity, inspection.SchemaReport(), readinessSnapshot.CapturedHead, request.IncludeProjections,
	)
	if err != nil {
		return Result{}, artifactIO("bind backup producer input", err)
	}

	outputLock, err := namespacelock.AcquireExistingParent(output)
	if err != nil {
		return Result{}, classifySecurity("lock backup output namespace", err)
	}
	defer func() { resultErr = errors.Join(resultErr, outputLock.Close()) }()
	exists, err := outputLock.TargetExists()
	if err != nil {
		return Result{}, classifySecurity("inspect backup output", err)
	}
	if exists {
		if err := databaseBoundary.Verify(); err != nil {
			return Result{}, classifySourceSecurity("revalidate source database identity", err)
		}
		recovered, recoverErr := recoverPublishedBackup(ctx, output, policy, producerInput)
		if err := databaseBoundary.Verify(); err != nil {
			return Result{}, errors.Join(ErrDurabilityUnknown, recoverErr,
				fmt.Errorf("source database identity changed during recovery: %w", err))
		}
		sourcePublicationCommitted = recoverErr == nil
		return recovered, recoverErr
	}
	observations, err := outputLock.InspectDiagnosticSiblings(16 << 10)
	if err != nil {
		return Result{}, classifySecurity("inspect backup publication markers", err)
	}
	for _, observation := range observations {
		if observation.Kind == namespacelock.DiagnosticPublishPending {
			// Directory staging is never resumed or removed automatically.
			return Result{}, ErrDurabilityUnknown
		}
	}

	publishIDs, err := canonical.NewIDGenerator(canonical.SystemClock{}, rand.Reader)
	if err != nil {
		return Result{}, artifactIO("create publication ID generator", err)
	}
	publishID, err := publishIDs.New()
	if err != nil {
		return Result{}, artifactIO("allocate publication ID", err)
	}
	stagingPath, stagingLock, err := createStagingNamespace(output, publishID)
	if err != nil {
		return Result{}, err
	}
	defer func() { resultErr = errors.Join(resultErr, stagingLock.Close()) }()
	root, err := fssecure.OpenRoot(stagingPath, policy)
	if err != nil {
		return Result{}, classifySecurity("open private backup staging", err)
	}
	defer func() {
		if root != nil {
			resultErr = errors.Join(resultErr, root.Close())
		}
	}()

	databaseDir, err := root.OpenOrCreateDirectory("database")
	if err != nil {
		return Result{}, artifactIO("create backup database directory", err)
	}
	databaseFile, err := databaseDir.CreateRegular("mahoroba.db")
	if err != nil {
		_ = databaseDir.Close()
		return Result{}, artifactIO("create backup database file", err)
	}
	if err := databaseFile.Close(); err != nil {
		_ = databaseDir.Close()
		return Result{}, artifactIO("close empty backup database file", err)
	}
	if err := inspection.SnapshotTo(ctx, filepath.Join(stagingPath, filepath.FromSlash(DatabaseBundlePath))); err != nil {
		_ = databaseDir.Close()
		return Result{}, artifactIO("snapshot source database", err)
	}
	metadata, err := storesqlite.PrepareBackupClone(ctx, filepath.Join(stagingPath, filepath.FromSlash(DatabaseBundlePath)), storesqlite.BackupCloneOptions{
		IncludeProjections: request.IncludeProjections,
	})
	if err != nil {
		_ = databaseDir.Close()
		return Result{}, artifactIO("prepare backup clone", err)
	}
	databaseHandle, err := databaseDir.OpenRegularRead("mahoroba.db")
	if err != nil {
		_ = databaseDir.Close()
		return Result{}, artifactIO("seal prepared backup database", err)
	}
	databaseDigest, databaseSize, err := databaseHandle.Hash(ctx)
	if closeErr := databaseHandle.Close(); err == nil {
		err = closeErr
	}
	if err != nil {
		_ = databaseDir.Close()
		return Result{}, artifactIO("hash prepared backup database", err)
	}
	if err := databaseDir.Sync(); err != nil {
		_ = databaseDir.Close()
		return Result{}, artifactIO("sync backup database directory", err)
	}
	if err := databaseDir.Close(); err != nil {
		return Result{}, artifactIO("close backup database directory", err)
	}

	blobFiles, _, err := copyBackupBlobs(ctx, root, sourceBlobs, metadata.Blobs)
	if err != nil {
		return Result{}, err
	}
	manifest, err := makeManifest(request, metadata, databaseDigest, databaseSize, blobFiles)
	if err != nil {
		return Result{}, err
	}
	manifestBytes, err := encodeManifest(manifest)
	if err != nil {
		return Result{}, err
	}
	if err := writeRegular(root, "manifest.json", manifestBytes); err != nil {
		return Result{}, artifactIO("write backup manifest", err)
	}
	manifestDigest := sha256.Sum256(manifestBytes)
	complete := []byte("sha256:" + hex.EncodeToString(manifestDigest[:]) + "\n")
	if err := writeRegular(root, "COMPLETE", complete); err != nil {
		return Result{}, artifactIO("write backup completion marker", err)
	}
	if err := root.Sync(); err != nil {
		return Result{}, artifactIO("sync complete backup staging", err)
	}
	if err := root.Close(); err != nil {
		return Result{}, artifactIO("close complete backup staging", err)
	}
	root = nil
	if _, err := Verify(ctx, stagingPath); err != nil {
		return Result{}, err
	}
	root, err = fssecure.OpenRootForPublish(stagingPath, policy)
	if err != nil {
		return Result{}, classifySecurity("reopen verified backup staging", err)
	}
	if err := databaseBoundary.Verify(); err != nil {
		return Result{}, classifySourceSecurity("revalidate source database identity", err)
	}
	payload, err := durablepublish.DirectoryPayloadDigest(ctx, root)
	if err != nil {
		return Result{}, artifactIO("hash verified backup staging", err)
	}
	marker, err := durablepublish.NewMarker(
		publishID, durablepublish.VariantDirectory, durablepublish.CommandBackupCreate,
		producerInput, filepath.Base(output), filepath.Base(stagingPath), payload.SHA256,
	)
	if err != nil {
		return Result{}, artifactIO("construct backup publication marker", err)
	}
	published, err := durablepublish.PublishDirectory(ctx, durablepublish.DirectoryRequest{
		Staging: root, Marker: marker, Failpoint: request.publishFailpoint,
	})
	if err != nil {
		if errors.Is(err, durablepublish.ErrTargetExists) {
			return Result{}, ErrTargetExists
		}
		markerName, nameErr := durablepublish.SiblingMarkerBasename(filepath.Base(output), publishID)
		if nameErr == nil {
			markerHandle, markerErr := root.OpenSiblingRegular(markerName)
			if markerErr == nil {
				_ = markerHandle.Close()
				return Result{}, fmt.Errorf("%w: backup publication requires recovery: %v", ErrDurabilityUnknown, err)
			}
			if !errors.Is(markerErr, os.ErrNotExist) {
				return Result{}, fmt.Errorf("%w: backup publication marker state is unknown", ErrDurabilityUnknown)
			}
		}
		return Result{}, artifactIO("publish backup directory", err)
	}
	sourcePublicationCommitted = true
	if err := root.Close(); err != nil {
		return Result{}, fmt.Errorf("%w: close published backup directory: %v", ErrDurabilityUnknown, err)
	}
	root = nil
	verified, err := Verify(ctx, output)
	if err != nil {
		return Result{}, fmt.Errorf("%w: verify published backup: %v", ErrDurabilityUnknown, err)
	}
	if published.Payload.SHA256 != payload.SHA256 {
		return Result{}, fmt.Errorf("%w: published payload accounting differs", ErrDurabilityUnknown)
	}
	if err := databaseBoundary.Verify(); err != nil {
		return Result{}, fmt.Errorf("%w: source database identity changed after publication: %v",
			ErrDurabilityUnknown, err)
	}
	return resultFromVerified(output, verified), nil
}

func preflightCreatePaths(request CreateRequest) (source, database, output string, err error) {
	if !singleBasename(request.DatabaseFilename) {
		return "", "", "", fmt.Errorf("%w: database filename is not a basename", ErrSourceUnavailable)
	}
	if request.CreatedBy.BinaryVersion == "" || request.CreatedBy.GitRevision == "" || request.CreatedBy.GoVersion == "" {
		return "", "", "", errors.New("backup: created_by is required")
	}
	source, err = canonicalExistingDirectory(request.SourceDataDir)
	if err != nil {
		return "", "", "", fmt.Errorf("%w: source data directory: %v", ErrSourceUnavailable, err)
	}
	database = filepath.Join(source, request.DatabaseFilename)
	info, err := os.Lstat(database)
	if err != nil || !info.Mode().IsRegular() {
		return "", "", "", fmt.Errorf("%w: source database is unavailable", ErrSourceUnavailable)
	}
	if request.Output == "" || !filepath.IsAbs(request.Output) {
		return "", "", "", errors.New("backup: output must be an absolute path")
	}
	outputParent, err := canonicalExistingDirectory(filepath.Dir(filepath.Clean(request.Output)))
	if err != nil {
		return "", "", "", artifactIO("resolve output parent", err)
	}
	output = filepath.Join(outputParent, filepath.Base(filepath.Clean(request.Output)))
	protectedPaths := []string{source}
	if info, statErr := os.Stat(filepath.Join(source, "blobs")); statErr == nil && info.IsDir() {
		protectedPaths = append(protectedPaths, filepath.Join(source, "blobs"))
	} else if statErr != nil && !errors.Is(statErr, os.ErrNotExist) {
		return "", "", "", artifactIO("inspect source blob root", statErr)
	}
	for _, protected := range protectedPaths {
		identityOverlap, identityErr := ancestorHasSameIdentity(outputParent, protected)
		if identityErr != nil {
			return "", "", "", artifactIO("compare source/output filesystem identity", identityErr)
		}
		if pathsOverlap(protected, output) || identityOverlap {
			return "", "", "", ErrUnsafeOverlap
		}
	}
	if _, err := os.Lstat(output); err != nil && !errors.Is(err, os.ErrNotExist) {
		return "", "", "", artifactIO("inspect output target", err)
	}
	return source, database, output, nil
}

func ancestorHasSameIdentity(start, candidate string) (bool, error) {
	candidateInfo, err := os.Stat(candidate)
	if err != nil {
		return false, err
	}
	current := filepath.Clean(start)
	for {
		info, err := os.Stat(current)
		if err != nil {
			return false, err
		}
		if os.SameFile(info, candidateInfo) {
			return true, nil
		}
		parent := filepath.Dir(current)
		if parent == current {
			return false, nil
		}
		current = parent
	}
}

func canonicalExistingDirectory(value string) (string, error) {
	if value == "" || !filepath.IsAbs(value) {
		return "", errors.New("path must be absolute")
	}
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
	return left == right || pathContains(left, right) || pathContains(right, left)
}

func pathContains(parent, child string) bool {
	relative, err := filepath.Rel(parent, child)
	return err == nil && relative != "." && relative != ".." && !strings.HasPrefix(relative, ".."+string(filepath.Separator)) && !filepath.IsAbs(relative)
}

func createStagingNamespace(output string, publishID canonical.ID) (string, *namespacelock.Lock, error) {
	if err := publishID.Validate(); err != nil {
		return "", nil, artifactIO("validate staging identity", err)
	}
	name := "." + filepath.Base(output) + ".mahoroba-backup-staging-" + publishID.String()
	staging := filepath.Join(filepath.Dir(output), name)
	lock, err := namespacelock.AcquireExistingParent(staging)
	if err != nil {
		return "", nil, classifySecurity("lock backup staging namespace", err)
	}
	exists, err := lock.TargetExists()
	if err != nil {
		_ = lock.Close()
		return "", nil, classifySecurity("inspect backup staging namespace", err)
	}
	if exists {
		_ = lock.Close()
		return "", nil, artifactIO("allocate private staging directory", os.ErrExist)
	}
	target, err := lock.OpenOrCreateTarget(0o700)
	if err != nil {
		_ = lock.Close()
		return "", nil, classifySecurity("create private backup staging", err)
	}
	if err := target.Close(); err != nil {
		_ = lock.Close()
		return "", nil, artifactIO("close private backup staging", err)
	}
	return staging, lock, nil
}

func copyBackupBlobs(ctx context.Context, root *fssecure.Directory, source *blob.FileStore, values []storesqlite.BackupBlob) ([]BlobFile, int64, error) {
	blobsDir, err := root.OpenOrCreateDirectory("blobs")
	if err != nil {
		return nil, 0, artifactIO("create backup blobs directory", err)
	}
	defer blobsDir.Close()
	objectsDir, err := blobsDir.OpenOrCreateDirectory("objects")
	if err != nil {
		return nil, 0, artifactIO("create backup objects directory", err)
	}
	defer objectsDir.Close()
	var result []BlobFile
	var total int64
	residentDirectories := make(map[string]*fssecure.Directory)
	prefixDirectories := make(map[string]*fssecure.Directory)
	defer func() {
		for _, directory := range prefixDirectories {
			_ = directory.Close()
		}
		for _, directory := range residentDirectories {
			_ = directory.Close()
		}
	}()
	for _, value := range values {
		body, err := source.Read(ctx, value.ResidentID, value.Digest)
		if err != nil {
			return nil, total, fmt.Errorf("%w: read source filesystem blob", ErrSourceUnavailable)
		}
		if int64(len(body)) != value.ByteSize || canonical.HashBlob(body) != value.Digest || !bytes.Equal(body, value.Content) {
			return nil, total, fmt.Errorf("%w: database and filesystem blob copies differ", ErrSourceUnavailable)
		}
		resident := value.ResidentID.String()
		residentDir := residentDirectories[resident]
		if residentDir == nil {
			residentDir, err = objectsDir.OpenOrCreateDirectory(resident)
			if err != nil {
				return nil, total, artifactIO("create backup resident blob directory", err)
			}
			residentDirectories[resident] = residentDir
		}
		digest := value.Digest.Hex()
		prefixKey := resident + "\x00" + digest[:2]
		prefixDir := prefixDirectories[prefixKey]
		if prefixDir == nil {
			prefixDir, err = residentDir.OpenOrCreateDirectory(digest[:2])
			if err != nil {
				return nil, total, artifactIO("create backup digest directory", err)
			}
			prefixDirectories[prefixKey] = prefixDir
		}
		if err := writeRegular(prefixDir, digest[2:], body); err != nil {
			return nil, total, artifactIO("copy backup blob", err)
		}
		bundlePath := "blobs/objects/" + resident + "/" + digest[:2] + "/" + digest[2:]
		result = append(result, BlobFile{ResidentID: resident, Path: bundlePath, ByteSize: strconv.FormatInt(value.ByteSize, 10), SHA256: "sha256:" + digest})
		total += value.ByteSize
	}
	slices.SortFunc(result, func(left, right BlobFile) int {
		if value := strings.Compare(left.ResidentID, right.ResidentID); value != 0 {
			return value
		}
		return strings.Compare(left.Path, right.Path)
	})
	if err := objectsDir.Sync(); err != nil {
		return nil, total, artifactIO("sync backup objects directory", err)
	}
	if err := blobsDir.Sync(); err != nil {
		return nil, total, artifactIO("sync backup blobs directory", err)
	}
	return result, total, nil
}

func writeRegular(directory *fssecure.Directory, name string, body []byte) error {
	handle, err := directory.CreateRegular(name)
	if err != nil {
		return err
	}
	if _, err := handle.File().Write(body); err != nil {
		_ = handle.Close()
		return err
	}
	if err := handle.Seal(); err != nil {
		_ = handle.Close()
		return err
	}
	if err := handle.Close(); err != nil {
		return err
	}
	return directory.Sync()
}

func makeManifest(request CreateRequest, metadata storesqlite.BackupCloneMetadata, databaseDigest [sha256.Size]byte, databaseSize int64, blobs []BlobFile) (Manifest, error) {
	head := CapturedHead{Exists: metadata.Head.Exists}
	if metadata.Head.Exists {
		id, sequence, at, timezone := metadata.Head.CommitID.String(), metadata.Head.CommitSeq.String(), metadata.Head.CommittedAt.String(), metadata.Head.CommittedTZ.String()
		head.CommitID, head.CommitSeq, head.CommittedAtUnixMicros, head.CommittedTZ = &id, &sequence, &at, &timezone
	}
	selection := RuntimeSelection{Source: metadata.SelectionSource}
	if metadata.ActiveResidentID != nil {
		value := metadata.ActiveResidentID.String()
		selection.ActiveResidentID = &value
	}
	if metadata.SessionPolicyID != nil {
		value := metadata.SessionPolicyID.String()
		selection.SessionizationPolicyVersionID = &value
	}
	actions := make([]RequiredAction, 0, len(metadata.RequiredActions))
	for code, ids := range metadata.RequiredActions {
		targets := make([]string, 0, len(ids))
		for _, id := range ids {
			targets = append(targets, id.String())
		}
		slices.Sort(targets)
		actions = append(actions, RequiredAction{Code: code, TargetIDs: targets})
	}
	slices.SortFunc(actions, func(left, right RequiredAction) int { return strings.Compare(left.Code, right.Code) })
	projectionValue := Projections{Policy: "excluded_by_default", Entries: []ProjectionEntry{}}
	if request.IncludeProjections {
		projectionValue.Policy = "included_requested"
		for _, definition := range metadata.ProjectionDefinitions {
			projectionValue.Entries = append(projectionValue.Entries, ProjectionEntry{Name: string(definition.Name), Version: string(definition.Version)})
		}
		slices.SortFunc(projectionValue.Entries, func(left, right ProjectionEntry) int { return strings.Compare(left.Name, right.Name) })
	}
	manifest := Manifest{
		FormatVersion: FormatVersion, CreatedBy: request.CreatedBy, SourceDatabaseFilename: request.DatabaseFilename,
		Schema:               Schema{Version: strconv.FormatInt(metadata.SchemaReport.SchemaVersion, 10), Fingerprint: "sha256:" + metadata.SchemaReport.SchemaFingerprint},
		MigrationDescriptors: migrationManifest(), CapturedHead: head, RuntimeSelection: selection,
		ServiceReady: metadata.ServiceReady, RequiredActions: actions, Projections: projectionValue,
		DatabaseFile: DatabaseFile{Path: DatabaseBundlePath, ByteSize: strconv.FormatInt(databaseSize, 10), SHA256: "sha256:" + hex.EncodeToString(databaseDigest[:])},
		BlobFiles:    blobs, ExternalCopyNotice: ExternalCopyNotice, AuthenticityGuaranteed: false,
	}
	return manifest, validateManifest(manifest)
}

func backupProducerInputDigest(
	sourceIdentity string,
	schema storesqlite.SchemaReport,
	head readiness.Head,
	includeProjections bool,
) (canonical.Digest, error) {
	captured := durablepublish.CapturedHead{Exists: head.Exists}
	if head.Exists {
		commitID, commitSeq := head.CommitID.String(), head.CommitSeq.String()
		committedAt, committedTZ := head.CommittedAt.String(), head.CommittedTZ.String()
		captured.CommitID, captured.CommitSeq = &commitID, &commitSeq
		captured.CommittedAt, captured.CommittedTZ = &committedAt, &committedTZ
	}
	return durablepublish.ProducerInputDigest(durablepublish.CommandBackupCreate, durablepublish.BackupCreateProducerInput{
		SourceDataDirIdentity: sourceIdentity,
		SchemaFingerprint:     "sha256:" + schema.SchemaFingerprint,
		CapturedHead:          captured,
		IncludeProjections:    includeProjections,
	})
}

func recoverPublishedBackup(
	ctx context.Context,
	output string,
	policy fssecure.SecurityPolicy,
	producerInput canonical.Digest,
) (_ Result, resultErr error) {
	root, err := fssecure.OpenRootForPublish(output, policy)
	if err != nil {
		return Result{}, ErrTargetExists
	}
	rootOpen := true
	defer func() {
		if rootOpen {
			resultErr = errors.Join(resultErr, root.Close())
		}
	}()
	marker, err := durablepublish.DiscoverPublishedDirectoryMarker(
		root, durablepublish.CommandBackupCreate, "sha256:"+producerInput.Hex(),
	)
	if errors.Is(err, durablepublish.ErrRecoveryMarkerAbsent) {
		return Result{}, ErrTargetExists
	}
	if err != nil {
		return Result{}, fmt.Errorf("%w: backup recovery marker differs", ErrDurabilityUnknown)
	}
	if _, err := durablepublish.RecoverPublishedDirectory(ctx, durablepublish.PublishedDirectoryRecoveryRequest{
		Target: root, Expected: marker,
	}); err != nil {
		return Result{}, fmt.Errorf("%w: complete backup publication: %v", ErrDurabilityUnknown, err)
	}
	if err := root.Close(); err != nil {
		return Result{}, fmt.Errorf("%w: close recovered backup authority: %v", ErrDurabilityUnknown, err)
	}
	rootOpen = false
	verified, err := Verify(ctx, output)
	if err != nil {
		return Result{}, fmt.Errorf("%w: verify recovered backup: %v", ErrDurabilityUnknown, err)
	}
	return resultFromVerified(output, verified), nil
}

func resultFromVerified(output string, verified VerifiedBundle) Result {
	return Result{
		ArtifactPath: output, FormatVersion: FormatVersion, CapturedHead: verified.Manifest.CapturedHead,
		ProjectionsIncluded: verified.Manifest.Projections.Policy == "included_requested",
		FileCount:           verified.FileCount, ByteCount: verified.ByteCount, AuthenticityGuaranteed: false,
	}
}

func artifactIO(operation string, err error) error {
	return fmt.Errorf("%w: %s: %v", ErrArtifactIO, operation, err)
}

func classifySecurity(operation string, err error) error {
	if errors.Is(err, fssecure.ErrUnsupportedSecureFilesystem) || errors.Is(err, namespacelock.ErrUnsupported) {
		return fmt.Errorf("%w: %s: %v", ErrUnsupportedSecurity, operation, err)
	}
	return artifactIO(operation, err)
}

func classifySourceSecurity(operation string, err error) error {
	if errors.Is(err, fssecure.ErrUnsupportedSecureFilesystem) || errors.Is(err, namespacelock.ErrUnsupported) {
		return fmt.Errorf("%w: %s: %v", ErrUnsupportedSecurity, operation, err)
	}
	return fmt.Errorf("%w: %s: %v", ErrSourceUnavailable, operation, err)
}
