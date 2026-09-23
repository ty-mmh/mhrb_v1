package exportjsonl

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"mahoroba.local/mahoroba/internal/assets/migrations"
	"mahoroba.local/mahoroba/internal/canonical"
)

type staticBlobReader map[string][]byte

func (reader staticBlobReader) Read(_ context.Context, resident canonical.ID, digest canonical.Digest) ([]byte, error) {
	value, exists := reader[resident.String()+"/"+digest.Hex()]
	if !exists {
		return nil, errors.New("missing")
	}
	return bytes.Clone(value), nil
}

func TestM7JSONLExportGoldenDeterministic(t *testing.T) {
	first := encodeGoldenFixture(t)
	second := encodeGoldenFixture(t)
	if !bytes.Equal(first, second) {
		t.Fatal("same snapshot produced different bytes")
	}
	want, err := os.ReadFile(filepath.Join("testdata", "mahoroba-jsonl-v1.golden.jsonl"))
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(first, want) {
		t.Fatalf("golden differs\ngot:\n%s\nwant:\n%s", first, want)
	}
	if bytes.Contains(first, []byte("source_data_dir")) || bytes.Contains(first, []byte("database_filename")) || bytes.Contains(first, []byte("exported_at")) {
		t.Fatal("host-dependent metadata leaked into export")
	}
}

func TestM7JSONLExportPresentUTF8Base64AndErasedAreDistinct(t *testing.T) {
	body := encodeGoldenFixture(t)
	for _, fragment := range []string{
		`"payload":{"bytes_base64":null,"encoding":"utf-8","representation":"text","text":"[erased]"}`,
		`"payload":{"bytes_base64":"/wA=","encoding":"base64","representation":"base64","text":null}`,
		`"payload":{"bytes_base64":null,"encoding":null,"representation":"erased","text":null}`,
	} {
		if !bytes.Contains(body, []byte(fragment)) {
			t.Fatalf("missing payload fragment %s", fragment)
		}
	}
}

func TestM7JSONLExportEmptyHeadUsesRequiredNullFields(t *testing.T) {
	metadata := goldenMetadata(t)
	metadata.CapturedHead = CapturedHead{}
	var output bytes.Buffer
	encoder, err := NewEncoder(EncoderOptions{Writer: &output, Blobs: staticBlobReader{}})
	if err != nil {
		t.Fatal(err)
	}
	if err := encoder.BeginSnapshot(context.Background(), metadata); err != nil {
		t.Fatal(err)
	}
	if !bytes.Contains(output.Bytes(), []byte(`"captured_head":{"commit_id":null,"commit_seq":null,"committed_at":null,"committed_tz":null,"exists":false}`)) {
		t.Fatalf("empty header = %s", output.Bytes())
	}
}

func TestM7JSONLExportRejectsUnreviewedHeaderMetadataBeforeWriting(t *testing.T) {
	tests := []func(*SnapshotMetadata){
		func(metadata *SnapshotMetadata) { metadata.SchemaVersion++ },
		func(metadata *SnapshotMetadata) {
			metadata.Migrations = metadata.Migrations[:len(metadata.Migrations)-1]
		},
		func(metadata *SnapshotMetadata) { metadata.Migrations[0].SHA256 = "sha256:" + strings.Repeat("0", 64) },
	}
	for index, mutate := range tests {
		metadata := goldenMetadata(t)
		mutate(&metadata)
		var output bytes.Buffer
		encoder, err := NewEncoder(EncoderOptions{Writer: &output, Blobs: staticBlobReader{}})
		if err != nil {
			t.Fatal(err)
		}
		if err := encoder.BeginSnapshot(context.Background(), metadata); !errors.Is(err, ErrSchemaCoverage) || output.Len() != 0 {
			t.Fatalf("case %d: error=%v output=%q", index, err, output.String())
		}
	}
}

func TestM7JSONLExportRejectsDualBlobMismatchWithoutRawBytes(t *testing.T) {
	ctx := context.Background()
	resident := mustExportID(t, "00000000000000000000000002")
	logical := []byte("private-content-that-must-not-appear")
	digest := sha256.Sum256(logical)
	reader := staticBlobReader{resident.String() + "/" + canonical.Digest(digest).Hex(): []byte("different")}
	encoder, err := NewEncoder(EncoderOptions{Writer: new(bytes.Buffer), Blobs: reader})
	if err != nil {
		t.Fatal(err)
	}
	if err := encoder.BeginSnapshot(ctx, goldenMetadata(t)); err != nil {
		t.Fatal(err)
	}
	err = encoder.WriteContentObject(ctx, presentContent(t, "00000000000000000000000004", resident.String(), logical))
	if !errors.Is(err, ErrContentIntegrity) {
		t.Fatalf("WriteContentObject() error = %v", err)
	}
	if strings.Contains(err.Error(), string(logical)) || strings.Contains(err.Error(), "different") || strings.Contains(err.Error(), canonical.Digest(digest).Hex()) {
		t.Fatalf("integrity error exposed raw authority: %v", err)
	}
}

func TestM7JSONLExportConcurrentDeterminism(t *testing.T) {
	const workers = 12
	results := make([][]byte, workers)
	var wait sync.WaitGroup
	wait.Add(workers)
	for index := range results {
		go func(index int) {
			defer wait.Done()
			results[index] = encodeGoldenFixture(t)
		}(index)
	}
	wait.Wait()
	for index := 1; index < len(results); index++ {
		if !bytes.Equal(results[0], results[index]) {
			t.Fatalf("worker %d produced different bytes", index)
		}
	}
}

func encodeGoldenFixture(t *testing.T) []byte {
	t.Helper()
	ctx := context.Background()
	resident := mustExportID(t, "00000000000000000000000002")
	textBytes, binaryBytes := []byte("[erased]"), []byte{0xff, 0x00}
	textDigest, binaryDigest := sha256.Sum256(textBytes), sha256.Sum256(binaryBytes)
	reader := staticBlobReader{
		resident.String() + "/" + canonical.Digest(textDigest).Hex():   textBytes,
		resident.String() + "/" + canonical.Digest(binaryDigest).Hex(): binaryBytes,
	}
	var output bytes.Buffer
	encoder, err := NewEncoder(EncoderOptions{Writer: &output, Blobs: reader})
	if err != nil {
		t.Fatal(err)
	}
	metadata := goldenMetadata(t)
	if err := encoder.BeginSnapshot(ctx, metadata); err != nil {
		t.Fatal(err)
	}
	if err := encoder.WriteCanonicalRecord(ctx, RawRecord{
		DescriptorIndex: 0,
		RecordID:        metadata.CapturedHead.CommitID.String(),
		Values: []any{
			metadata.CapturedHead.CommitID.String(), int64(1), resident.String(), int64(1_700_000_000_000_000), "Asia/Tokyo",
		},
	}); err != nil {
		t.Fatal(err)
	}
	if err := encoder.WriteContentObject(ctx, presentContent(t, "00000000000000000000000004", resident.String(), textBytes)); err != nil {
		t.Fatal(err)
	}
	if err := encoder.WriteContentObject(ctx, presentContent(t, "00000000000000000000000005", resident.String(), binaryBytes)); err != nil {
		t.Fatal(err)
	}
	if err := encoder.WriteContentObject(ctx, erasedContent(t, "00000000000000000000000006", resident.String())); err != nil {
		t.Fatal(err)
	}
	stats := encoder.Stats()
	if stats.RecordCount != 5 || stats.ByteCount != int64(output.Len()) {
		t.Fatalf("stats = %#v, bytes=%d", stats, output.Len())
	}
	return output.Bytes()
}

func goldenMetadata(t *testing.T) SnapshotMetadata {
	t.Helper()
	descriptors := migrations.Descriptors()
	migrationMetadata := make([]MigrationDescriptor, len(descriptors))
	for index, descriptor := range descriptors {
		migrationMetadata[index] = MigrationDescriptor{
			Version: int64(index + 1), Name: descriptor.Name, ByteSize: int64(descriptor.Bytes),
			SHA256: "sha256:" + descriptor.SHA256,
		}
	}
	return SnapshotMetadata{
		SchemaVersion:     migrations.BaselineVersion,
		SchemaFingerprint: "sha256:" + strings.Repeat("a", 64),
		Migrations:        migrationMetadata,
		CapturedHead: CapturedHead{
			Exists: true, CommitID: mustExportID(t, "00000000000000000000000001"),
			CommitSeq: 1, CommittedAt: 1_700_000_000_000_000, CommittedTZ: canonical.MustTimezone("Asia/Tokyo"),
		},
	}
}

func presentContent(t *testing.T, id, resident string, body []byte) ContentObject {
	t.Helper()
	digest := sha256.Sum256(body)
	commitment := sha256.Sum256([]byte("commitment-" + id))
	salt := make([]byte, 32)
	for index := range salt {
		salt[index] = byte(index)
	}
	return ContentObject{
		RecordID: id,
		Values: []any{
			id, resident, "event_payload", digest[:], "sha256", commitment[:], salt,
			"sha256", "mahoroba:content-commitment:v1", "mahoroba-jcs-v1",
			"present", "independent", int64(1_700_000_000_000_000), "Asia/Tokyo",
		},
		SQLiteBlob: &SQLiteBlob{
			ResidentID: resident, HashAlgorithm: "sha256", Hash: digest[:], ByteSize: int64(len(body)), Bytes: bytes.Clone(body),
		},
	}
}

func erasedContent(t *testing.T, id, resident string) ContentObject {
	t.Helper()
	commitment := sha256.Sum256([]byte("commitment-" + id))
	return ContentObject{
		RecordID: id,
		Values: []any{
			id, resident, "claim_statement", nil, "sha256", commitment[:], nil,
			"sha256", "mahoroba:content-commitment:v1", "mahoroba-jcs-v1",
			"erased", "independent", int64(1_700_000_000_000_000), "Asia/Tokyo",
		},
	}
}

func mustExportID(t *testing.T, value string) canonical.ID {
	t.Helper()
	id, err := canonical.ParseID(value)
	if err != nil {
		t.Fatal(err)
	}
	return id
}
