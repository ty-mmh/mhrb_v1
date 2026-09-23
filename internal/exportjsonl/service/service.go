package exportjsonlservice

import (
	"bytes"
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"strings"

	"mahoroba.local/mahoroba/internal/blob"
	"mahoroba.local/mahoroba/internal/canonical"
	"mahoroba.local/mahoroba/internal/durablepublish"
	"mahoroba.local/mahoroba/internal/exportjsonl"
	"mahoroba.local/mahoroba/internal/fssecure"
	"mahoroba.local/mahoroba/internal/hostlock"
	"mahoroba.local/mahoroba/internal/namespacelock"
	storesqlite "mahoroba.local/mahoroba/internal/store/sqlite"
)

const FormatVersion = exportjsonl.FormatVersion

var (
	ErrSourceUnavailable    = exportjsonl.ErrSourceUnavailable
	ErrArtifactTargetExists = exportjsonl.ErrArtifactTargetExists
	ErrArtifactIO           = exportjsonl.ErrArtifactIO
	ErrDurabilityUnknown    = exportjsonl.ErrDurabilityUnknown
	ErrContentIntegrity     = exportjsonl.ErrContentIntegrity
	ErrSchemaCoverage       = exportjsonl.ErrSchemaCoverage
)

type CapturedHead = exportjsonl.CapturedHead
type SnapshotMetadata = exportjsonl.SnapshotMetadata

type Request struct {
	SourceDataDir    string
	DatabaseFilename string
	Output           string
}

type Result struct {
	ArtifactPath  string
	FormatVersion string
	CapturedHead  CapturedHead
	RecordCount   int64
	ByteCount     int64
}

// Create publishes one deterministic JSONL file. The source host lock and the
// output namespace lock are retained from preflight through the final parent
// durability barrier.
func Create(ctx context.Context, request Request) (_ Result, resultErr error) {
	if ctx == nil {
		return Result{}, fmt.Errorf("%w: nil context", ErrSourceUnavailable)
	}
	sourceDir, _, output, err := preflightPaths(request)
	if err != nil {
		return Result{}, err
	}
	sourceLock, err := hostlock.Acquire(sourceDir)
	if err != nil {
		return Result{}, fmt.Errorf("%w: acquire source host lock: %w", ErrSourceUnavailable, err)
	}
	defer func() { resultErr = errors.Join(resultErr, sourceLock.Close()) }()

	policy, err := fssecure.CurrentSecurityPolicy()
	if err != nil {
		return Result{}, fmt.Errorf("%w: resolve filesystem policy", ErrSourceUnavailable)
	}
	databaseBoundary, err := storesqlite.OpenReadOnlyBoundDatabase(
		ctx, sourceDir, request.DatabaseFilename,
	)
	if err != nil {
		return Result{}, fmt.Errorf("%w: open source database boundary: %v", ErrSourceUnavailable, err)
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
		return Result{}, fmt.Errorf("%w: bind source identity", ErrSourceUnavailable)
	}
	inspection := databaseBoundary.Inspection()
	blobs, err := blob.OpenFileStoreReadOnly(filepath.Join(sourceDir, "blobs"))
	if err != nil {
		return Result{}, fmt.Errorf("%w: open source blobs", ErrSourceUnavailable)
	}
	if err := inspection.MinimumCheckerWithBlobObjects(blobs).Check(ctx); err != nil {
		// Keep integrity.ErrFatal in the chain so the CLI can render the exact
		// integrity_fatal code without exposing the finding material.
		return Result{}, fmt.Errorf("export jsonl: source MinimumCheck: %w", err)
	}

	namespace, err := namespacelock.AcquireExistingParent(output)
	if err != nil {
		if errors.Is(err, namespacelock.ErrBusy) {
			return Result{}, fmt.Errorf("%w: output namespace busy", ErrArtifactIO)
		}
		return Result{}, fmt.Errorf("%w: lock output namespace", ErrArtifactIO)
	}
	defer func() { resultErr = errors.Join(resultErr, namespace.Close()) }()
	parent, err := fssecure.OpenRoot(filepath.Dir(output), policy)
	if err != nil {
		return Result{}, fmt.Errorf("%w: open protected output parent", ErrArtifactIO)
	}
	defer func() { resultErr = errors.Join(resultErr, parent.Close()) }()

	repository := inspection.ExportRepository()
	if recovered, recoveryResult, recoveryErr := recoverPending(
		ctx, repository, parent, sourceIdentity, output,
	); recoveryErr != nil {
		return Result{}, recoveryErr
	} else if recovered {
		if err := databaseBoundary.Verify(); err != nil {
			return Result{}, fmt.Errorf("%w: source database identity changed during recovery: %v",
				ErrDurabilityUnknown, err)
		}
		sourcePublicationCommitted = true
		return recoveryResult, nil
	}
	exists, err := namespace.RegularTargetExists()
	if err != nil {
		return Result{}, fmt.Errorf("%w: inspect output target", ErrArtifactIO)
	}
	if exists {
		return Result{}, ErrArtifactTargetExists
	}

	generator, err := canonical.NewIDGenerator(canonical.SystemClock{}, rand.Reader)
	if err != nil {
		return Result{}, fmt.Errorf("%w: allocate publication identity", ErrArtifactIO)
	}
	publishID, err := generator.New()
	if err != nil {
		return Result{}, fmt.Errorf("%w: allocate publication identity", ErrArtifactIO)
	}
	targetName := filepath.Base(output)
	stagingName := "." + targetName + ".staging." + publishID.String()
	staging, err := parent.CreateRegular(stagingName)
	if err != nil {
		return Result{}, fmt.Errorf("%w: create staging file", ErrArtifactIO)
	}
	publicationStarted := false
	defer func() {
		if !publicationStarted && staging != nil {
			resultErr = errors.Join(resultErr, staging.MarkDeleteOnClose())
		}
		if staging != nil {
			resultErr = errors.Join(resultErr, staging.Close())
		}
	}()

	encoder, err := exportjsonl.NewEncoder(exportjsonl.EncoderOptions{Writer: staging.File(), Blobs: blobs})
	if err != nil {
		return Result{}, err
	}
	metadata, err := repository.StreamSnapshot(ctx, encoder)
	if err != nil {
		return Result{}, err
	}
	if err := staging.Seal(); err != nil {
		return Result{}, fmt.Errorf("%w: seal staging file", ErrArtifactIO)
	}
	payload, err := durablepublish.SingleFilePayloadDigest(ctx, staging)
	if err != nil {
		return Result{}, fmt.Errorf("%w: hash staging file", ErrArtifactIO)
	}
	stats := encoder.Stats()
	if stats.ByteCount != payload.ByteCount || stats.Metadata.SchemaFingerprint != metadata.SchemaFingerprint {
		return Result{}, fmt.Errorf("%w: staged export accounting differs", ErrArtifactIO)
	}
	inputDigest, err := exportProducerInputDigest(sourceIdentity, metadata)
	if err != nil {
		return Result{}, fmt.Errorf("%w: bind producer input", ErrArtifactIO)
	}
	marker, err := durablepublish.NewMarker(
		publishID, durablepublish.VariantSingleFile, durablepublish.CommandExportJSONL,
		inputDigest, targetName, stagingName, payload.SHA256,
	)
	if err != nil {
		return Result{}, fmt.Errorf("%w: construct publication marker", ErrArtifactIO)
	}
	if err := databaseBoundary.Verify(); err != nil {
		return Result{}, fmt.Errorf("%w: source database identity changed before publication: %v",
			ErrSourceUnavailable, err)
	}
	publicationStarted = true
	if _, err := durablepublish.PublishSingleFile(ctx, durablepublish.SingleFileRequest{
		Parent: parent, Staging: staging, Marker: marker,
	}); err != nil {
		siblingName, nameErr := durablepublish.SiblingMarkerBasename(targetName, publishID)
		if nameErr != nil {
			return Result{}, fmt.Errorf("%w: publication marker identity", ErrDurabilityUnknown)
		}
		sibling, siblingErr := parent.OpenRegularRead(siblingName)
		if siblingErr == nil {
			_ = sibling.Close()
			return Result{}, fmt.Errorf("%w: publication marker requires recovery", ErrDurabilityUnknown)
		}
		if !errors.Is(siblingErr, os.ErrNotExist) {
			return Result{}, fmt.Errorf("%w: publication marker state is unknown", ErrDurabilityUnknown)
		}
		// No marker crossed its creation boundary, so the retained staging
		// handle can be securely removed by the deferred pre-publication path.
		publicationStarted = false
		switch {
		case errors.Is(err, durablepublish.ErrTargetExists):
			return Result{}, ErrArtifactTargetExists
		case errors.Is(err, durablepublish.ErrDurabilityUnknown):
			return Result{}, fmt.Errorf("%w: publication requires recovery", ErrDurabilityUnknown)
		default:
			return Result{}, fmt.Errorf("%w: publish staging file", ErrArtifactIO)
		}
	}
	sourcePublicationCommitted = true
	if err := databaseBoundary.Verify(); err != nil {
		return Result{}, fmt.Errorf("%w: source database identity changed after publication: %v",
			ErrDurabilityUnknown, err)
	}
	return Result{
		ArtifactPath: output, FormatVersion: FormatVersion, CapturedHead: metadata.CapturedHead,
		RecordCount: stats.RecordCount, ByteCount: stats.ByteCount,
	}, nil
}

func preflightPaths(request Request) (source, database, output string, err error) {
	if request.SourceDataDir == "" || !filepath.IsAbs(request.SourceDataDir) ||
		request.DatabaseFilename == "" || filepath.Base(request.DatabaseFilename) != request.DatabaseFilename ||
		strings.ContainsAny(request.DatabaseFilename, `/\\`) {
		return "", "", "", fmt.Errorf("%w: invalid source path", ErrSourceUnavailable)
	}
	source, err = canonicalDirectory(request.SourceDataDir)
	if err != nil {
		return "", "", "", fmt.Errorf("%w: source data directory", ErrSourceUnavailable)
	}
	database = filepath.Join(source, request.DatabaseFilename)
	info, statErr := os.Lstat(database)
	if statErr != nil || !info.Mode().IsRegular() || info.Mode()&os.ModeSymlink != 0 {
		return "", "", "", fmt.Errorf("%w: source database", ErrSourceUnavailable)
	}
	if request.Output == "" || !filepath.IsAbs(request.Output) {
		return "", "", "", fmt.Errorf("%w: output must be absolute", ErrArtifactIO)
	}
	cleanOutput := filepath.Clean(request.Output)
	if base := filepath.Base(cleanOutput); base == "." || base == "" || strings.ContainsAny(base, `/\\`) {
		return "", "", "", fmt.Errorf("%w: invalid output basename", ErrArtifactIO)
	}
	parent, err := canonicalDirectory(filepath.Dir(cleanOutput))
	if err != nil {
		return "", "", "", fmt.Errorf("%w: output parent", ErrArtifactIO)
	}
	output = filepath.Join(parent, filepath.Base(cleanOutput))
	if pathWithin(source, output) {
		return "", "", "", fmt.Errorf("%w: output overlaps source", ErrArtifactIO)
	}
	return source, database, output, nil
}

func canonicalDirectory(value string) (string, error) {
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
		return "", errors.New("not a stable directory")
	}
	return filepath.Clean(resolved), nil
}

func pathWithin(parent, child string) bool {
	if runtime.GOOS == "windows" {
		parent, child = strings.ToLower(parent), strings.ToLower(child)
	}
	relative, err := filepath.Rel(parent, child)
	return err == nil && relative != ".." && !strings.HasPrefix(relative, ".."+string(filepath.Separator)) && !filepath.IsAbs(relative)
}

func exportProducerInputDigest(sourceIdentity string, metadata SnapshotMetadata) (canonical.Digest, error) {
	return durablepublish.ProducerInputDigest(durablepublish.CommandExportJSONL, durablepublish.ExportJSONLProducerInput{
		SourceDataDirIdentity: sourceIdentity,
		SchemaFingerprint:     metadata.SchemaFingerprint,
		CapturedHead:          durableCapturedHead(metadata.CapturedHead),
		FormatVersion:         FormatVersion,
	})
}

func durableCapturedHead(head CapturedHead) durablepublish.CapturedHead {
	if !head.Exists {
		return durablepublish.CapturedHead{Exists: false}
	}
	commitID, commitSeq := head.CommitID.String(), head.CommitSeq.String()
	committedAt, committedTZ := head.CommittedAt.String(), head.CommittedTZ.String()
	return durablepublish.CapturedHead{
		Exists: true, CommitID: &commitID, CommitSeq: &commitSeq,
		CommittedAt: &committedAt, CommittedTZ: &committedTZ,
	}
}

func recoverPending(
	ctx context.Context,
	repository *storesqlite.ExportRepository,
	parent *fssecure.Directory,
	sourceIdentity, output string,
) (bool, Result, error) {
	target := filepath.Base(output)
	prefix := "." + target + ".publish-pending."
	entries, err := parent.ReadDir()
	if err != nil {
		return false, Result{}, fmt.Errorf("%w: inspect publication markers", ErrArtifactIO)
	}
	var markerName string
	for _, entry := range entries {
		if strings.HasPrefix(entry.Name(), prefix) {
			if markerName != "" {
				return false, Result{}, fmt.Errorf("%w: multiple publication markers", ErrDurabilityUnknown)
			}
			markerName = entry.Name()
		}
	}
	if markerName == "" {
		return false, Result{}, nil
	}
	handle, err := parent.OpenRegularRead(markerName)
	if err != nil {
		return false, Result{}, fmt.Errorf("%w: open publication marker", ErrDurabilityUnknown)
	}
	body, readErr := io.ReadAll(io.LimitReader(handle.File(), (16<<10)+1))
	closeErr := handle.Close()
	if err := errors.Join(readErr, closeErr); err != nil || len(body) > 16<<10 {
		return false, Result{}, fmt.Errorf("%w: read publication marker", ErrDurabilityUnknown)
	}
	marker, err := durablepublish.ParseMarker(body)
	if err != nil || marker.Variant != durablepublish.VariantSingleFile ||
		marker.ProducerCommand != durablepublish.CommandExportJSONL || marker.TargetBasename != target ||
		markerName != "."+target+".publish-pending."+marker.PublishID.String() {
		return false, Result{}, fmt.Errorf("%w: invalid publication marker", ErrDurabilityUnknown)
	}
	metadata, err := repository.CaptureMetadata(ctx)
	if err != nil {
		return false, Result{}, err
	}
	inputDigest, err := exportProducerInputDigest(sourceIdentity, metadata)
	if err != nil {
		return false, Result{}, fmt.Errorf("%w: recompute producer input", ErrDurabilityUnknown)
	}
	payloadDigest, err := canonical.ParseDigestHex(strings.TrimPrefix(marker.PayloadSHA256, "sha256:"))
	if err != nil {
		return false, Result{}, fmt.Errorf("%w: invalid marker payload", ErrDurabilityUnknown)
	}
	expected, err := durablepublish.NewMarker(
		marker.PublishID, durablepublish.VariantSingleFile, durablepublish.CommandExportJSONL,
		inputDigest, marker.TargetBasename, marker.StagingBasename, payloadDigest,
	)
	if err != nil {
		return false, Result{}, fmt.Errorf("%w: recompute publication marker", ErrDurabilityUnknown)
	}
	recovered, err := durablepublish.RecoverSingleFile(ctx, durablepublish.SingleFileRecoveryRequest{Parent: parent, Expected: expected})
	if err != nil {
		return false, Result{}, fmt.Errorf("%w: publication recovery requires repair", ErrDurabilityUnknown)
	}
	if recovered.Action == durablepublish.RecoveryRemoveStaleMarker {
		return false, Result{}, nil
	}
	if recovered.Action != durablepublish.RecoveryResumeSingleFile && recovered.Action != durablepublish.RecoveryCompletePublished {
		return false, Result{}, fmt.Errorf("%w: publication recovery did not complete", ErrDurabilityUnknown)
	}
	recordCount, byteCount, err := countPublishedLines(ctx, parent, target)
	if err != nil || byteCount != recovered.Payload.ByteCount {
		return false, Result{}, fmt.Errorf("%w: validate recovered export", ErrDurabilityUnknown)
	}
	return true, Result{
		ArtifactPath: output, FormatVersion: FormatVersion, CapturedHead: metadata.CapturedHead,
		RecordCount: recordCount, ByteCount: byteCount,
	}, nil
}

func countPublishedLines(ctx context.Context, parent *fssecure.Directory, name string) (int64, int64, error) {
	handle, err := parent.OpenRegularRead(name)
	if err != nil {
		return 0, 0, err
	}
	defer handle.Close()
	var records, size int64
	buffer := make([]byte, 32<<10)
	last := byte(0)
	for {
		if err := ctx.Err(); err != nil {
			return 0, size, err
		}
		read, readErr := handle.File().Read(buffer)
		if read > 0 {
			records += int64(bytes.Count(buffer[:read], []byte{'\n'}))
			size += int64(read)
			last = buffer[read-1]
		}
		if errors.Is(readErr, io.EOF) {
			break
		}
		if readErr != nil {
			return 0, size, readErr
		}
	}
	if size == 0 || last != '\n' {
		return 0, size, errors.New("recovered export is not LF terminated")
	}
	return records, size, nil
}
