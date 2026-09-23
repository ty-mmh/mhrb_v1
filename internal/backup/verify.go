package backup

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"

	"mahoroba.local/mahoroba/internal/canonical"
	"mahoroba.local/mahoroba/internal/fssecure"
	storesqlite "mahoroba.local/mahoroba/internal/store/sqlite"
)

type VerifiedBundle struct {
	RootIdentity       fssecure.Identity
	RootIdentitySHA256 string
	Manifest           Manifest
	FileCount          int64
	ByteCount          int64
}

// BundleEntry describes one payload file which restore must stream from the
// verified source namespace. Manifest and COMPLETE are verification metadata,
// not restored payload entries.
type BundleEntry struct {
	Path     string
	ByteSize int64
	SHA256   string
}

// EntrySink must consume reader completely before returning. The reader is
// valid only for the duration of the callback and is backed by one verified,
// no-follow file handle in the retained bundle namespace.
type EntrySink func(context.Context, BundleEntry, io.Reader) error

type openVerifiedBundle struct {
	result              VerifiedBundle
	root                *fssecure.Directory
	database            *boundBundleFile
	databaseSnapshot    *os.File
	closeSnapshot       bool
	expectedFiles       map[string]struct{}
	expectedDirectories map[string]struct{}
}

// Verify performs standalone strict verification and closes every retained
// authority before returning. Restore callers which need to stream the exact
// verified bytes must use CopyVerified instead of reopening result paths.
func Verify(ctx context.Context, input string) (_ VerifiedBundle, resultErr error) {
	opened, err := openAndVerify(ctx, input)
	if err != nil {
		return VerifiedBundle{}, err
	}
	result := opened.result
	if err := opened.Close(); err != nil {
		return VerifiedBundle{}, invalid("close retained bundle authority", err)
	}
	return result, nil
}

// CopyVerified verifies the bundle and then streams the database followed by
// manifest-ordered blob entries from the same retained root authority. Every
// callback must consume exactly the declared bytes. Source handles and the
// complete tree are revalidated after copying, closing verify-to-copy races.
func CopyVerified(ctx context.Context, input string, sink EntrySink) (_ VerifiedBundle, resultErr error) {
	if sink == nil {
		return VerifiedBundle{}, errors.New("backup: nil verified entry sink")
	}
	opened, err := openAndVerify(ctx, input)
	if err != nil {
		return VerifiedBundle{}, err
	}
	defer func() { resultErr = errors.Join(resultErr, opened.Close()) }()

	database := opened.result.Manifest.DatabaseFile
	databaseSize, _ := strconv.ParseInt(database.ByteSize, 10, 64)
	if err := streamVerifiedFile(ctx, opened.databaseSnapshot, BundleEntry{
		Path: database.Path, ByteSize: databaseSize, SHA256: database.SHA256,
	}, sink); err != nil {
		return VerifiedBundle{}, err
	}
	for _, descriptor := range opened.result.Manifest.BlobFiles {
		bound, err := openBoundBundleFile(opened.root, descriptor.Path)
		if err != nil {
			return VerifiedBundle{}, invalid("open verified blob for copy", err)
		}
		size, _ := strconv.ParseInt(descriptor.ByteSize, 10, 64)
		copyErr := streamVerifiedHandle(ctx, bound.handle, BundleEntry{
			Path: descriptor.Path, ByteSize: size, SHA256: descriptor.SHA256,
		}, sink)
		closeErr := bound.Close()
		if copyErr != nil {
			return VerifiedBundle{}, errors.Join(copyErr, closeErr)
		}
		if closeErr != nil {
			return VerifiedBundle{}, invalid("close verified blob after copy", closeErr)
		}
	}
	if err := opened.database.handle.VerifyBound(); err != nil {
		return VerifiedBundle{}, invalid("database identity changed during copy", err)
	}
	if err := verifyTreeInventory(opened.root, opened.expectedFiles, opened.expectedDirectories); err != nil {
		return VerifiedBundle{}, err
	}
	if err := opened.root.VerifyBound(); err != nil {
		return VerifiedBundle{}, invalid("bundle root identity changed during copy", err)
	}
	return opened.result, nil
}

func openAndVerify(ctx context.Context, input string) (_ *openVerifiedBundle, resultErr error) {
	if ctx == nil {
		return nil, errors.New("backup: nil verification context")
	}
	rootPath, err := strictBundleRoot(input)
	if err != nil {
		return nil, fmt.Errorf("%w: bundle root is unavailable", ErrInvalidBundle)
	}
	policy, err := fssecure.CurrentSecurityPolicy()
	if err != nil {
		return nil, classifySecurity("resolve bundle security policy", err)
	}
	root, err := fssecure.OpenRootReadOnly(rootPath, policy)
	if err != nil {
		return nil, invalid("bundle root is not a protected ordinary directory", err)
	}
	keepRoot := false
	defer func() {
		if !keepRoot {
			resultErr = errors.Join(resultErr, root.Close())
		}
	}()

	manifestBytes, err := readBundleFile(ctx, root, "manifest.json", 16<<20)
	if err != nil {
		return nil, invalid("manifest is unavailable", err)
	}
	manifest, err := decodeManifest(manifestBytes)
	if err != nil {
		return nil, err
	}
	completeBytes, err := readBundleFile(ctx, root, "COMPLETE", 72)
	if err != nil {
		return nil, invalid("completion marker is unavailable", err)
	}
	manifestDigest := sha256.Sum256(manifestBytes)
	wantComplete := []byte("sha256:" + hex.EncodeToString(manifestDigest[:]) + "\n")
	if !bytes.Equal(completeBytes, wantComplete) {
		return nil, invalid("completion marker does not bind manifest", nil)
	}

	expectedFiles, expectedDirectories := expectedBundleInventory(manifest)
	if err := verifyTreeInventory(root, expectedFiles, expectedDirectories); err != nil {
		return nil, err
	}
	databaseBound, err := openBoundBundleFile(root, DatabaseBundlePath)
	if err != nil {
		return nil, invalid("database file is unavailable", err)
	}
	keepDatabase := false
	defer func() {
		if !keepDatabase {
			resultErr = errors.Join(resultErr, databaseBound.Close())
		}
	}()
	databaseSnapshot, closeSnapshot, databaseSize, err := captureVerifiedDatabase(ctx, databaseBound.handle, manifest.DatabaseFile)
	if err != nil {
		return nil, invalid("database file cannot be captured", err)
	}
	keepSnapshot := false
	defer func() {
		if closeSnapshot && !keepSnapshot {
			resultErr = errors.Join(resultErr, databaseSnapshot.Close())
		}
	}()

	includeProjections := manifest.Projections.Policy == "included_requested"
	databaseNamespacePath := filepath.Join(rootPath, filepath.FromSlash(DatabaseBundlePath))
	inspection, err := storesqlite.OpenImmutableDescriptorInspection(ctx, databaseSnapshot, databaseNamespacePath)
	if err != nil {
		return nil, invalid("database retained descriptor is unavailable", err)
	}
	inspectionOpen := true
	defer func() {
		if inspectionOpen {
			resultErr = errors.Join(resultErr, inspection.Close())
		}
	}()
	metadata, err := storesqlite.InspectBackupCloneInspection(ctx, inspection, includeProjections, manifest.RuntimeSelection.Source)
	if err != nil {
		return nil, invalid("database schema or backup contract differs", err)
	}
	if err := compareManifestDatabase(manifest, metadata); err != nil {
		return nil, err
	}
	reader := &bundleBlobReader{root: root, descriptors: make(map[string]BlobFile, len(manifest.BlobFiles))}
	for _, descriptor := range manifest.BlobFiles {
		reader.descriptors[descriptor.ResidentID+"\x00"+strings.TrimPrefix(descriptor.SHA256, "sha256:")] = descriptor
	}
	minimumErr := inspection.MinimumCheckerWithBlobObjects(reader).Check(ctx)
	if minimumErr == nil {
		minimumErr = verifyBundleLedgers(ctx, inspection)
	}
	closeErr := inspection.Close()
	inspectionOpen = false
	if minimumErr != nil || closeErr != nil {
		return nil, invalid("bundle Canonical or dual-copy integrity differs", errors.Join(minimumErr, closeErr))
	}
	finalDatabaseDigest, finalDatabaseSize, err := databaseBound.handle.Hash(ctx)
	if err != nil || strconv.FormatInt(finalDatabaseSize, 10) != manifest.DatabaseFile.ByteSize ||
		"sha256:"+hex.EncodeToString(finalDatabaseDigest[:]) != manifest.DatabaseFile.SHA256 {
		return nil, invalid("database file changed during verification", err)
	}

	var byteCount int64 = databaseSize
	for _, descriptor := range manifest.BlobFiles {
		digest, size, err := hashBundlePath(ctx, root, descriptor.Path)
		if err != nil {
			return nil, invalid("blob file is unavailable", err)
		}
		if strconv.FormatInt(size, 10) != descriptor.ByteSize || "sha256:"+hex.EncodeToString(digest[:]) != descriptor.SHA256 {
			return nil, invalid("blob file hash or size differs", nil)
		}
		if size > int64(^uint64(0)>>1)-byteCount {
			return nil, invalid("bundle byte count overflows", nil)
		}
		byteCount += size
	}
	// A second complete traversal closes add/remove/replace races which occur
	// while SQLite or blob bytes are being semantically verified.
	if err := verifyTreeInventory(root, expectedFiles, expectedDirectories); err != nil {
		return nil, err
	}
	if err := root.VerifyBound(); err != nil {
		return nil, invalid("bundle root identity changed", err)
	}
	rootIdentityDigest, err := root.ArtifactSourceIdentityDigest()
	if err != nil {
		return nil, invalid("bundle source identity cannot be bound", err)
	}
	result := VerifiedBundle{
		RootIdentity: root.Identity(), RootIdentitySHA256: rootIdentityDigest, Manifest: manifest,
		FileCount: int64(len(manifest.BlobFiles) + 1), ByteCount: byteCount,
	}
	keepRoot = true
	keepDatabase = true
	keepSnapshot = true
	return &openVerifiedBundle{
		result: result, root: root, database: databaseBound,
		databaseSnapshot: databaseSnapshot, closeSnapshot: closeSnapshot,
		expectedFiles: expectedFiles, expectedDirectories: expectedDirectories,
	}, nil
}

func (bundle *openVerifiedBundle) Close() error {
	if bundle == nil {
		return nil
	}
	var result error
	if bundle.closeSnapshot && bundle.databaseSnapshot != nil {
		result = bundle.databaseSnapshot.Close()
	}
	bundle.databaseSnapshot = nil
	bundle.closeSnapshot = false
	if bundle.database != nil {
		result = errors.Join(result, bundle.database.Close())
		bundle.database = nil
	}
	if bundle.root != nil {
		result = errors.Join(result, bundle.root.Close())
		bundle.root = nil
	}
	return result
}

func streamVerifiedHandle(ctx context.Context, handle *fssecure.Handle, entry BundleEntry, sink EntrySink) error {
	if entry.ByteSize < 0 || handle == nil {
		return invalid("verified entry descriptor differs", nil)
	}
	if handle.Snapshot().Size() != entry.ByteSize {
		return invalid("verified entry size changed before copy", nil)
	}
	if _, err := handle.File().Seek(0, io.SeekStart); err != nil {
		return invalid("seek verified entry for copy", err)
	}
	hasher := sha256.New()
	reader := &countingReader{reader: io.TeeReader(io.LimitReader(&bundleContextReader{ctx: ctx, reader: handle.File()}, entry.ByteSize), hasher)}
	if err := sink(ctx, entry, reader); err != nil {
		return fmt.Errorf("backup: verified entry sink for %s: %w", entry.Path, err)
	}
	if reader.count != entry.ByteSize {
		return errors.New("backup: verified entry sink did not consume the complete entry")
	}
	if "sha256:"+hex.EncodeToString(hasher.Sum(nil)) != entry.SHA256 {
		return invalid("verified entry hash changed during copy", nil)
	}
	if err := handle.VerifyBound(); err != nil {
		return invalid("verified entry identity changed during copy", err)
	}
	return ctx.Err()
}

func streamVerifiedFile(ctx context.Context, file *os.File, entry BundleEntry, sink EntrySink) error {
	if entry.ByteSize < 0 || file == nil {
		return invalid("verified database snapshot descriptor differs", nil)
	}
	info, err := file.Stat()
	if err != nil || info.Size() != entry.ByteSize {
		return invalid("verified database snapshot size differs", err)
	}
	if _, err := file.Seek(0, io.SeekStart); err != nil {
		return invalid("seek verified database snapshot", err)
	}
	hasher := sha256.New()
	reader := &countingReader{reader: io.TeeReader(io.LimitReader(&bundleContextReader{ctx: ctx, reader: file}, entry.ByteSize), hasher)}
	if err := sink(ctx, entry, reader); err != nil {
		return fmt.Errorf("backup: verified entry sink for %s: %w", entry.Path, err)
	}
	if reader.count != entry.ByteSize {
		return errors.New("backup: verified entry sink did not consume the complete entry")
	}
	if "sha256:"+hex.EncodeToString(hasher.Sum(nil)) != entry.SHA256 {
		return invalid("verified database snapshot hash differs during copy", nil)
	}
	return ctx.Err()
}

type countingReader struct {
	reader io.Reader
	count  int64
}

func (reader *countingReader) Read(value []byte) (int, error) {
	count, err := reader.reader.Read(value)
	reader.count += int64(count)
	return count, err
}

type bundleContextReader struct {
	ctx    context.Context
	reader io.Reader
}

func (reader *bundleContextReader) Read(value []byte) (int, error) {
	if err := reader.ctx.Err(); err != nil {
		return 0, err
	}
	return reader.reader.Read(value)
}

func strictBundleRoot(value string) (string, error) {
	if value == "" || !filepath.IsAbs(value) {
		return "", errors.New("bundle root must be an absolute path")
	}
	root := filepath.Clean(value)
	info, err := os.Lstat(root)
	if err != nil {
		return "", err
	}
	if !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
		return "", errors.New("bundle root is not an ordinary directory")
	}
	return root, nil
}

func expectedBundleInventory(manifest Manifest) (map[string]struct{}, map[string]struct{}) {
	files := map[string]struct{}{"manifest.json": {}, "COMPLETE": {}, DatabaseBundlePath: {}}
	directories := map[string]struct{}{"": {}, "database": {}, "blobs": {}, "blobs/objects": {}}
	for _, descriptor := range manifest.BlobFiles {
		files[descriptor.Path] = struct{}{}
		parts := strings.Split(descriptor.Path, "/")
		for index := 1; index < len(parts); index++ {
			directories[strings.Join(parts[:index], "/")] = struct{}{}
		}
	}
	return files, directories
}

func verifyTreeInventory(root *fssecure.Directory, expectedFiles, expectedDirectories map[string]struct{}) error {
	seenFiles := make(map[string]struct{}, len(expectedFiles))
	seenDirectories := map[string]struct{}{"": {}}
	var walk func(*fssecure.Directory, string) error
	walk = func(directory *fssecure.Directory, prefix string) error {
		entries, err := directory.ReadDir()
		if err != nil {
			return invalid("read bundle inventory", err)
		}
		slices.SortFunc(entries, func(left, right os.DirEntry) int { return strings.Compare(left.Name(), right.Name()) })
		for _, entry := range entries {
			name := entry.Name()
			if name == "" || name == "." || name == ".." || strings.ContainsRune(name, 0) || strings.ContainsAny(name, `/\`) {
				return invalid("bundle contains an unsafe entry name", nil)
			}
			relative := name
			if prefix != "" {
				relative = prefix + "/" + name
			}
			if _, ok := expectedDirectories[relative]; ok {
				child, err := directory.OpenDirectory(name)
				if err != nil {
					return invalid("bundle directory is a symlink, reparse point, or unsafe entry", err)
				}
				seenDirectories[relative] = struct{}{}
				walkErr := walk(child, relative)
				closeErr := child.Close()
				if walkErr != nil {
					return walkErr
				}
				if closeErr != nil {
					return invalid("close bundle directory", closeErr)
				}
				continue
			}
			if _, ok := expectedFiles[relative]; ok {
				handle, err := directory.OpenRegularRead(name)
				if err != nil {
					return invalid("bundle file is a symlink, reparse point, or unsafe entry", err)
				}
				if err := handle.VerifyBound(); err != nil {
					_ = handle.Close()
					return invalid("bundle file identity differs", err)
				}
				if err := handle.Close(); err != nil {
					return invalid("close bundle file", err)
				}
				seenFiles[relative] = struct{}{}
				continue
			}
			return invalid("bundle contains an unlisted entry "+relative, nil)
		}
		return nil
	}
	if err := walk(root, ""); err != nil {
		return err
	}
	if len(seenFiles) != len(expectedFiles) || len(seenDirectories) != len(expectedDirectories) {
		return invalid("bundle inventory is incomplete", nil)
	}
	return nil
}

func readBundleFile(ctx context.Context, directory *fssecure.Directory, name string, maximum int64) ([]byte, error) {
	handle, err := directory.OpenRegularRead(name)
	if err != nil {
		return nil, err
	}
	defer handle.Close()
	if handle.Snapshot().Size() > maximum {
		return nil, errors.New("bundle file exceeds fixed bound")
	}
	if _, err := handle.File().Seek(0, io.SeekStart); err != nil {
		return nil, err
	}
	body, err := io.ReadAll(io.LimitReader(handle.File(), maximum+1))
	if err != nil {
		return nil, err
	}
	if int64(len(body)) > maximum {
		return nil, errors.New("bundle file exceeds fixed bound")
	}
	if err := handle.VerifyBound(); err != nil {
		return nil, err
	}
	return body, nil
}

func hashBundlePath(ctx context.Context, root *fssecure.Directory, relative string) ([sha256.Size]byte, int64, error) {
	var zero [sha256.Size]byte
	parts := strings.Split(relative, "/")
	if len(parts) == 0 {
		return zero, 0, errors.New("empty bundle path")
	}
	directory := root
	opened := make([]*fssecure.Directory, 0, len(parts)-1)
	defer func() {
		for index := len(opened) - 1; index >= 0; index-- {
			_ = opened[index].Close()
		}
	}()
	for _, component := range parts[:len(parts)-1] {
		child, err := directory.OpenDirectory(component)
		if err != nil {
			return zero, 0, err
		}
		opened = append(opened, child)
		directory = child
	}
	handle, err := directory.OpenRegularRead(parts[len(parts)-1])
	if err != nil {
		return zero, 0, err
	}
	digest, size, hashErr := handle.Hash(ctx)
	closeErr := handle.Close()
	return digest, size, errors.Join(hashErr, closeErr)
}

type boundBundleFile struct {
	handle      *fssecure.Handle
	directories []*fssecure.Directory
}

func openBoundBundleFile(root *fssecure.Directory, relative string) (*boundBundleFile, error) {
	parts := strings.Split(relative, "/")
	if len(parts) == 0 {
		return nil, errors.New("empty bundle path")
	}
	result := &boundBundleFile{}
	directory := root
	for _, component := range parts[:len(parts)-1] {
		child, err := directory.OpenDirectory(component)
		if err != nil {
			_ = result.Close()
			return nil, err
		}
		result.directories = append(result.directories, child)
		directory = child
	}
	handle, err := directory.OpenRegularRead(parts[len(parts)-1])
	if err != nil {
		_ = result.Close()
		return nil, err
	}
	result.handle = handle
	return result, nil
}

func (file *boundBundleFile) Close() error {
	if file == nil {
		return nil
	}
	var result error
	if file.handle != nil {
		result = file.handle.Close()
		file.handle = nil
	}
	for index := len(file.directories) - 1; index >= 0; index-- {
		result = errors.Join(result, file.directories[index].Close())
	}
	file.directories = nil
	return result
}

func compareManifestDatabase(manifest Manifest, metadata storesqlite.BackupCloneMetadata) error {
	if manifest.Schema.Version != strconv.FormatInt(metadata.SchemaReport.SchemaVersion, 10) || manifest.Schema.Fingerprint != "sha256:"+metadata.SchemaReport.SchemaFingerprint {
		return invalid("manifest schema does not match database", nil)
	}
	wantHead := manifest.CapturedHead
	if wantHead.Exists != metadata.Head.Exists {
		return invalid("captured head existence differs", nil)
	}
	if metadata.Head.Exists {
		if wantHead.CommitID == nil || *wantHead.CommitID != metadata.Head.CommitID.String() || wantHead.CommitSeq == nil || *wantHead.CommitSeq != metadata.Head.CommitSeq.String() ||
			wantHead.CommittedAtUnixMicros == nil || *wantHead.CommittedAtUnixMicros != metadata.Head.CommittedAt.String() || wantHead.CommittedTZ == nil || *wantHead.CommittedTZ != metadata.Head.CommittedTZ.String() {
			return invalid("captured head does not match database", nil)
		}
	}
	if !sameOptionalID(manifest.RuntimeSelection.ActiveResidentID, metadata.ActiveResidentID) || !sameOptionalID(manifest.RuntimeSelection.SessionizationPolicyVersionID, metadata.SessionPolicyID) {
		return invalid("runtime selection does not match database", nil)
	}
	if manifest.ServiceReady != metadata.ServiceReady {
		return invalid("service readiness does not match database", nil)
	}
	actualActions := make([]RequiredAction, 0, len(metadata.RequiredActions))
	for code, ids := range metadata.RequiredActions {
		targets := make([]string, 0, len(ids))
		for _, id := range ids {
			targets = append(targets, id.String())
		}
		slices.Sort(targets)
		actualActions = append(actualActions, RequiredAction{Code: code, TargetIDs: targets})
	}
	slices.SortFunc(actualActions, func(left, right RequiredAction) int { return strings.Compare(left.Code, right.Code) })
	if !slices.EqualFunc(manifest.RequiredActions, actualActions, func(left, right RequiredAction) bool {
		return left.Code == right.Code && slices.Equal(left.TargetIDs, right.TargetIDs)
	}) {
		return invalid("required actions do not match database", nil)
	}
	if len(manifest.BlobFiles) != len(metadata.Blobs) {
		return invalid("manifest and database blob counts differ", nil)
	}
	for index, value := range metadata.Blobs {
		descriptor := manifest.BlobFiles[index]
		if descriptor.ResidentID != value.ResidentID.String() || descriptor.SHA256 != "sha256:"+value.Digest.Hex() || descriptor.ByteSize != strconv.FormatInt(value.ByteSize, 10) {
			return invalid("manifest blob identity does not match database", nil)
		}
	}
	return nil
}

func sameOptionalID(raw *string, value *canonical.ID) bool {
	if raw == nil || value == nil {
		return raw == nil && value == nil
	}
	return *raw == value.String()
}

type bundleBlobReader struct {
	root        *fssecure.Directory
	descriptors map[string]BlobFile
}

func (reader *bundleBlobReader) Read(ctx context.Context, residentID canonical.ID, digest canonical.Digest) ([]byte, error) {
	descriptor, ok := reader.descriptors[residentID.String()+"\x00"+digest.Hex()]
	if !ok {
		return nil, errors.New("bundle blob is not in manifest")
	}
	parts := strings.Split(descriptor.Path, "/")
	directory := reader.root
	opened := make([]*fssecure.Directory, 0, len(parts)-1)
	defer func() {
		for index := len(opened) - 1; index >= 0; index-- {
			_ = opened[index].Close()
		}
	}()
	for _, component := range parts[:len(parts)-1] {
		child, err := directory.OpenDirectory(component)
		if err != nil {
			return nil, err
		}
		opened = append(opened, child)
		directory = child
	}
	handle, err := directory.OpenRegularRead(parts[len(parts)-1])
	if err != nil {
		return nil, err
	}
	defer handle.Close()
	expected, _ := strconv.ParseInt(descriptor.ByteSize, 10, 64)
	if handle.Snapshot().Size() != expected {
		return nil, errors.New("bundle blob size differs")
	}
	if _, err := handle.File().Seek(0, io.SeekStart); err != nil {
		return nil, err
	}
	body, err := io.ReadAll(io.LimitReader(handle.File(), expected+1))
	if err != nil {
		return nil, err
	}
	if int64(len(body)) != expected || canonical.HashBlob(body) != digest {
		return nil, errors.New("bundle blob content differs")
	}
	if err := handle.VerifyBound(); err != nil {
		return nil, err
	}
	return body, ctx.Err()
}

func verifyBundleLedgers(ctx context.Context, inspection *storesqlite.Inspection) error {
	repository := inspection.Canonical()
	residentIDs, err := repository.ListResidentIDs(ctx)
	if err != nil {
		return err
	}
	verifier := canonical.LedgerVerifier{EnvelopeValidator: repository}
	for _, residentID := range residentIDs {
		if _, err := verifier.Verify(ctx, repository, residentID); err != nil {
			return err
		}
	}
	return nil
}
