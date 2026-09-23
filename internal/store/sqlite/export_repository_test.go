package sqlite

import (
	"bytes"
	"context"
	"errors"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"mahoroba.local/mahoroba/internal/assets/migrations"
	"mahoroba.local/mahoroba/internal/canonical"
	"mahoroba.local/mahoroba/internal/exportjsonl"
)

type sqliteExportBlobs map[string][]byte

func (blobs sqliteExportBlobs) Read(_ context.Context, resident canonical.ID, digest canonical.Digest) ([]byte, error) {
	body, exists := blobs[resident.String()+"/"+digest.Hex()]
	if !exists {
		return nil, errors.New("missing filesystem copy")
	}
	return bytes.Clone(body), nil
}

type blockingExportSink struct {
	collectingExportSink
	begun   chan struct{}
	release chan struct{}
}

func (sink *blockingExportSink) BeginSnapshot(ctx context.Context, metadata exportjsonl.SnapshotMetadata) error {
	if err := sink.collectingExportSink.BeginSnapshot(ctx, metadata); err != nil {
		return err
	}
	close(sink.begun)
	select {
	case <-sink.release:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

type collectingExportSink struct {
	metadata []exportjsonl.SnapshotMetadata
	records  []exportjsonl.RawRecord
	contents []exportjsonl.ContentObject
}

func TestM7JSONLExportRejectsUncataloguedSchemaBeforeHeader(t *testing.T) {
	tests := []struct {
		name string
		ddl  string
	}{
		{"commit-bound table", `CREATE TABLE unreviewed_records (
			unreviewed_id TEXT PRIMARY KEY,
			canonical_commit_id TEXT NOT NULL
		) STRICT`},
		{"catalogued table column", `ALTER TABLE principals ADD COLUMN unreviewed TEXT NULL`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			fixture, closeFixture := newSemanticFixture(t)
			defer closeFixture()
			ctx := context.Background()
			report, err := ValidateSchema(ctx, fixture.db)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := fixture.db.ExecContext(ctx, test.ddl); err != nil {
				t.Fatal(err)
			}
			sink := new(collectingExportSink)
			_, err = (&ExportRepository{reader: fixture.db, report: report}).StreamSnapshot(ctx, sink)
			if !errors.Is(err, exportjsonl.ErrSchemaCoverage) {
				t.Fatalf("StreamSnapshot() error = %v", err)
			}
			if len(sink.metadata) != 0 || len(sink.records) != 0 || len(sink.contents) != 0 {
				t.Fatalf("schema failure emitted output: %#v", sink)
			}
		})
	}
}

func TestM7JSONLExportRepositoryUsesSingleSnapshotFixedOrdering(t *testing.T) {
	fixture, closeFixture := newSemanticFixture(t)
	defer closeFixture()
	ctx := context.Background()
	report, err := ValidateSchema(ctx, fixture.db)
	if err != nil {
		t.Fatal(err)
	}
	sink := new(collectingExportSink)
	metadata, err := (&ExportRepository{reader: fixture.db, report: report}).StreamSnapshot(ctx, sink)
	if err != nil {
		t.Fatalf("StreamSnapshot() error = %v", err)
	}
	if !metadata.CapturedHead.Exists || metadata.CapturedHead.CommitID.String() != fixture.commit["B"] {
		t.Fatalf("captured head = %#v", metadata.CapturedHead)
	}

	catalog := exportjsonl.Catalog()
	commitSeq := map[string]int64{
		fixture.commit["global"]: 1,
		fixture.commit["A"]:      2,
		fixture.commit["B"]:      3,
	}
	var previousSeq int64
	previousOrdinal := -1
	for index, record := range sink.records {
		descriptor := catalog[record.DescriptorIndex]
		var seq int64
		if descriptor.Table == "canonical_commits" {
			seq = record.Values[1].(int64)
		} else {
			commitIndex := -1
			for columnIndex, column := range descriptor.Columns {
				if column.Name == descriptor.CommitColumn {
					commitIndex = columnIndex
					break
				}
			}
			seq = commitSeq[record.Values[commitIndex].(string)]
		}
		if seq < previousSeq || seq == previousSeq && descriptor.Ordinal < previousOrdinal {
			t.Fatalf("record %d order regressed: seq=%d ordinal=%d after seq=%d ordinal=%d", index, seq, descriptor.Ordinal, previousSeq, previousOrdinal)
		}
		previousSeq, previousOrdinal = seq, descriptor.Ordinal
	}
	if len(sink.records) == 0 || len(sink.contents) != 20 {
		t.Fatalf("records=%d contents=%d", len(sink.records), len(sink.contents))
	}
	previousOwner, previousContent := "", ""
	for _, content := range sink.contents {
		owner := content.Values[1].(string)
		if owner < previousOwner || owner == previousOwner && strings.Compare(content.RecordID, previousContent) < 0 {
			t.Fatalf("content order regressed: owner=%s id=%s", owner, content.RecordID)
		}
		if content.SQLiteBlob == nil || content.SQLiteBlob.ResidentID != owner {
			t.Fatalf("content %s blob = %#v", content.RecordID, content.SQLiteBlob)
		}
		previousOwner, previousContent = owner, content.RecordID
	}
}

func TestM7JSONLExportSemanticFixtureIsDeterministicJCS(t *testing.T) {
	fixture, closeFixture := newSemanticFixture(t)
	defer closeFixture()
	ctx := context.Background()
	report, err := ValidateSchema(ctx, fixture.db)
	if err != nil {
		t.Fatal(err)
	}
	blobs := sqliteExportBlobs{}
	rows, err := fixture.db.QueryContext(ctx, `SELECT dedupe_scope_id, blob_hash, content FROM blobs`)
	if err != nil {
		t.Fatal(err)
	}
	for rows.Next() {
		var resident string
		var digestBytes, body []byte
		if err := rows.Scan(&resident, &digestBytes, &body); err != nil {
			t.Fatal(err)
		}
		digest, err := canonical.DigestFromBytes(digestBytes)
		if err != nil {
			t.Fatal(err)
		}
		blobs[resident+"/"+digest.Hex()] = bytes.Clone(body)
	}
	if err := errors.Join(rows.Err(), rows.Close()); err != nil {
		t.Fatal(err)
	}

	encode := func() []byte {
		t.Helper()
		var output bytes.Buffer
		encoder, err := exportjsonl.NewEncoder(exportjsonl.EncoderOptions{Writer: &output, Blobs: blobs})
		if err != nil {
			t.Fatal(err)
		}
		if _, err := (&ExportRepository{reader: fixture.db, report: report}).StreamSnapshot(ctx, encoder); err != nil {
			t.Fatal(err)
		}
		if encoder.Stats().ByteCount != int64(output.Len()) || encoder.Stats().RecordCount != int64(bytes.Count(output.Bytes(), []byte{'\n'})) {
			t.Fatalf("stats=%#v length=%d", encoder.Stats(), output.Len())
		}
		return output.Bytes()
	}
	first, second := encode(), encode()
	if !bytes.Equal(first, second) {
		t.Fatal("same semantic snapshot produced different JSONL bytes")
	}
	for _, forbidden := range [][]byte{
		[]byte(`"record_type":"blob"`), []byte(`"record_type":"projection"`),
		[]byte(`"record_type":"runtime_config"`), []byte(`"database_filename"`),
	} {
		if bytes.Contains(first, forbidden) {
			t.Fatalf("forbidden storage/host detail %s was exported", forbidden)
		}
	}
}

func TestM7JSONLExportSingleReadSnapshotExcludesConcurrentCommit(t *testing.T) {
	ctx := context.Background()
	store, err := Open(ctx, filepath.Join(t.TempDir(), "mahoroba.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	sink := &blockingExportSink{begun: make(chan struct{}), release: make(chan struct{})}
	done := make(chan error, 1)
	go func() {
		_, err := store.ExportRepository().StreamSnapshot(ctx, sink)
		done <- err
	}()
	select {
	case <-sink.begun:
	case <-time.After(5 * time.Second):
		t.Fatal("export did not capture its snapshot")
	}
	commitID := "00000000000000000000000001"
	writeDone := make(chan error, 1)
	go func() {
		_, err := store.writer.ExecContext(ctx,
			"INSERT INTO canonical_commits VALUES (?,?,?,?,?)",
			commitID, 1, nil, int64(1_700_000_000_000_000), "UTC")
		writeDone <- err
	}()
	select {
	case err := <-writeDone:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("concurrent WAL commit did not complete")
	}
	close(sink.release)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if len(sink.metadata) != 1 || sink.metadata[0].CapturedHead.Exists || len(sink.records) != 0 {
		t.Fatalf("in-flight export observed concurrent commit: metadata=%#v records=%#v", sink.metadata, sink.records)
	}

	after := new(collectingExportSink)
	metadata, err := store.ExportRepository().StreamSnapshot(ctx, after)
	if err != nil {
		t.Fatal(err)
	}
	if !metadata.CapturedHead.Exists || metadata.CapturedHead.CommitID.String() != commitID || len(after.records) != 1 {
		t.Fatalf("next export did not observe committed row: metadata=%#v records=%#v", metadata, after.records)
	}
}

func (sink *collectingExportSink) BeginSnapshot(_ context.Context, metadata exportjsonl.SnapshotMetadata) error {
	sink.metadata = append(sink.metadata, metadata)
	return nil
}
func (sink *collectingExportSink) WriteCanonicalRecord(_ context.Context, record exportjsonl.RawRecord) error {
	sink.records = append(sink.records, record)
	return nil
}
func (sink *collectingExportSink) WriteContentObject(_ context.Context, content exportjsonl.ContentObject) error {
	sink.contents = append(sink.contents, content)
	return nil
}

func TestM7JSONLExportCatalogCoversExactV12Schema(t *testing.T) {
	ctx := context.Background()
	store, err := Open(ctx, filepath.Join(t.TempDir(), "mahoroba.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	sink := new(collectingExportSink)
	metadata, err := store.ExportRepository().StreamSnapshot(ctx, sink)
	if err != nil {
		t.Fatalf("StreamSnapshot() error = %v", err)
	}
	if metadata.SchemaVersion != migrations.BaselineVersion || len(metadata.Migrations) != int(migrations.BaselineVersion) {
		t.Fatalf("metadata = %#v", metadata)
	}
	if len(sink.metadata) != 1 || !reflect.DeepEqual(sink.metadata[0], metadata) {
		t.Fatalf("BeginSnapshot metadata = %#v", sink.metadata)
	}
	if len(sink.records) != 0 || len(sink.contents) != 0 {
		t.Fatalf("clean database emitted records=%d contents=%d", len(sink.records), len(sink.contents))
	}
	if got := len(exportjsonl.Catalog()); got != 26 {
		t.Fatalf("catalog descriptor count = %d, want 26", got)
	}
	if got := len(exportjsonl.ContentObjectColumns()); got != 14 {
		t.Fatalf("content column count = %d, want 14", got)
	}
}
