// Package exportjsonl implements the deterministic M7 Canonical JSONL export.
// It is an external copy format, not an import, replay, backup, or migration
// surface.
package exportjsonl

import (
	"context"
	"errors"
	"io"

	"mahoroba.local/mahoroba/internal/canonical"
)

const (
	FormatVersion      = "mahoroba-jsonl-v1"
	CatalogVersion     = "canonical-record-catalog-v1"
	OrderingVersion    = "canonical-record-order-v1"
	SnapshotSemantics  = "current-snapshot-not-replay"
	ExternalCopyNotice = "runtime_unmanaged_copy_not_covered_by_future_erasure"
	ProducerCommand    = "export.jsonl"
)

var (
	ErrSourceUnavailable    = errors.New("export jsonl: source unavailable")
	ErrSchemaCoverage       = errors.New("export jsonl: schema is not covered by the fixed catalog")
	ErrContentIntegrity     = errors.New("export jsonl: content copies differ")
	ErrArtifactTargetExists = errors.New("export jsonl: artifact target exists")
	ErrArtifactIO           = errors.New("export jsonl: artifact I/O failed")
	ErrDurabilityUnknown    = errors.New("export jsonl: publish durability unknown")
)

type CapturedHead struct {
	Exists      bool
	CommitID    canonical.ID
	CommitSeq   canonical.CommitSeq
	CommittedAt canonical.Instant
	CommittedTZ canonical.Timezone
}

type MigrationDescriptor struct {
	Version  int64
	Name     string
	ByteSize int64
	SHA256   string
}

type SnapshotMetadata struct {
	SchemaVersion     int64
	SchemaFingerprint string
	Migrations        []MigrationDescriptor
	CapturedHead      CapturedHead
}

type RawRecord struct {
	DescriptorIndex int
	RecordID        string
	Values          []any
}

type SQLiteBlob struct {
	ResidentID    string
	HashAlgorithm string
	Hash          []byte
	ByteSize      int64
	Bytes         []byte
}

type ContentObject struct {
	RecordID   string
	Values     []any
	SQLiteBlob *SQLiteBlob
}

type SnapshotSink interface {
	BeginSnapshot(context.Context, SnapshotMetadata) error
	WriteCanonicalRecord(context.Context, RawRecord) error
	WriteContentObject(context.Context, ContentObject) error
}

// SnapshotSource holds one read-only database transaction from header capture
// through the last content object and invokes the sink in fixed export order.
type SnapshotSource interface {
	StreamSnapshot(context.Context, SnapshotSink) (SnapshotMetadata, error)
}

type BlobReader interface {
	Read(context.Context, canonical.ID, canonical.Digest) ([]byte, error)
}

type Stats struct {
	Metadata    SnapshotMetadata
	RecordCount int64
	ByteCount   int64
}

type EncoderOptions struct {
	Writer io.Writer
	Blobs  BlobReader
}
