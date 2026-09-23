package exportjsonl

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"strconv"
	"strings"
	"unicode/utf8"

	"mahoroba.local/mahoroba/internal/assets/migrations"
	"mahoroba.local/mahoroba/internal/canonical"
)

// Encoder converts the raw, fixed-catalog database stream into exact JCS
// objects separated by a single LF. It never closes the caller-owned writer.
type Encoder struct {
	writer io.Writer
	blobs  BlobReader
	stats  Stats
	begun  bool
}

func NewEncoder(options EncoderOptions) (*Encoder, error) {
	if options.Writer == nil || options.Blobs == nil {
		return nil, fmt.Errorf("export jsonl: encoder requires writer and blob reader")
	}
	if err := ValidateCatalog(); err != nil {
		return nil, err
	}
	return &Encoder{writer: options.Writer, blobs: options.Blobs}, nil
}

func (encoder *Encoder) BeginSnapshot(ctx context.Context, metadata SnapshotMetadata) error {
	if encoder == nil || encoder.begun {
		return fmt.Errorf("export jsonl: snapshot header already written")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	header, err := headerEnvelope(metadata)
	if err != nil {
		return err
	}
	encoder.begun = true
	encoder.stats.Metadata = metadata
	return encoder.writeLine(header)
}

func (encoder *Encoder) WriteCanonicalRecord(ctx context.Context, raw RawRecord) error {
	if err := encoder.ready(ctx); err != nil {
		return err
	}
	catalog := Catalog()
	if raw.DescriptorIndex < 0 || raw.DescriptorIndex >= len(catalog) {
		return fmt.Errorf("%w: descriptor index", ErrSchemaCoverage)
	}
	descriptor := catalog[raw.DescriptorIndex]
	if len(raw.Values) != len(descriptor.Columns) {
		return fmt.Errorf("%w: %s value count", ErrSchemaCoverage, descriptor.RecordType)
	}
	record, err := encodeColumns(descriptor.Columns, raw.Values)
	if err != nil {
		return fmt.Errorf("%w: %s record", ErrSchemaCoverage, descriptor.RecordType)
	}
	encodedID, ok := record[descriptor.IDColumn].(string)
	if !ok || encodedID != raw.RecordID || raw.RecordID == "" {
		return fmt.Errorf("%w: %s record identity", ErrSchemaCoverage, descriptor.RecordType)
	}
	return encoder.writeLine(recordEnvelope(descriptor.RecordType, raw.RecordID, record))
}

func (encoder *Encoder) WriteContentObject(ctx context.Context, raw ContentObject) error {
	if err := encoder.ready(ctx); err != nil {
		return err
	}
	columns := ContentObjectColumns()
	if len(raw.Values) != len(columns) {
		return fmt.Errorf("%w: content_object value count", ErrSchemaCoverage)
	}
	record, err := encodeColumns(columns, raw.Values)
	if err != nil {
		return fmt.Errorf("%w: content_object record", ErrSchemaCoverage)
	}
	contentID, ok := record["content_id"].(string)
	if !ok || contentID == "" || contentID != raw.RecordID {
		return fmt.Errorf("%w: content_object identity", ErrSchemaCoverage)
	}
	owner, ownerOK := record["owner_resident_id"].(string)
	state, stateOK := record["erasure_state"].(string)
	if !ownerOK || !stateOK {
		return fmt.Errorf("%w: content_object state", ErrSchemaCoverage)
	}
	if _, err := canonical.ParseID(contentID); err != nil {
		return fmt.Errorf("%w: invalid content identity", ErrSchemaCoverage)
	}
	if _, err := canonical.ParseID(owner); err != nil {
		return fmt.Errorf("%w: invalid content owner", ErrSchemaCoverage)
	}

	switch state {
	case "present":
		payload, payloadErr := encoder.presentPayload(ctx, contentID, owner, raw.Values, raw.SQLiteBlob)
		if payloadErr != nil {
			return payloadErr
		}
		record["payload"] = payload
	case "erased":
		if raw.Values[3] != nil || raw.Values[6] != nil || raw.SQLiteBlob != nil {
			return fmt.Errorf("%w: erased content retains blob authority", ErrContentIntegrity)
		}
		record["payload"] = map[string]any{
			"representation": "erased",
			"encoding":       nil,
			"text":           nil,
			"bytes_base64":   nil,
		}
	default:
		return fmt.Errorf("%w: invalid content state", ErrContentIntegrity)
	}
	return encoder.writeLine(recordEnvelope("content_object", contentID, record))
}

func (encoder *Encoder) Stats() Stats {
	if encoder == nil {
		return Stats{}
	}
	return encoder.stats
}

func (encoder *Encoder) ready(ctx context.Context) error {
	if encoder == nil || !encoder.begun {
		return fmt.Errorf("export jsonl: snapshot header is required")
	}
	return ctx.Err()
}

func (encoder *Encoder) presentPayload(
	ctx context.Context,
	contentID, owner string,
	values []any,
	blob *SQLiteBlob,
) (map[string]any, error) {
	if blob == nil {
		return nil, fmt.Errorf("%w: present content %s lacks SQLite blob", ErrContentIntegrity, contentID)
	}
	ownerID, err := canonical.ParseID(owner)
	if err != nil {
		return nil, fmt.Errorf("%w: invalid content owner", ErrContentIntegrity)
	}
	databaseHash, ok := values[3].([]byte)
	if !ok {
		return nil, fmt.Errorf("%w: present content %s lacks blob hash", ErrContentIntegrity, contentID)
	}
	digest, err := canonical.DigestFromBytes(databaseHash)
	if err != nil {
		return nil, fmt.Errorf("%w: invalid blob hash", ErrContentIntegrity)
	}
	contentAlgorithm, ok := values[4].(string)
	if !ok || contentAlgorithm != canonical.HashAlgorithm || blob.ResidentID != owner ||
		blob.HashAlgorithm != contentAlgorithm ||
		!bytes.Equal(blob.Hash, databaseHash) || blob.ByteSize != int64(len(blob.Bytes)) {
		return nil, fmt.Errorf("%w: SQLite blob metadata differs for content %s", ErrContentIntegrity, contentID)
	}
	sum := sha256.Sum256(blob.Bytes)
	if !bytes.Equal(sum[:], databaseHash) {
		return nil, fmt.Errorf("%w: SQLite blob digest differs for content %s", ErrContentIntegrity, contentID)
	}
	filesystemBytes, err := encoder.blobs.Read(ctx, ownerID, digest)
	if err != nil {
		return nil, fmt.Errorf("%w: filesystem blob unavailable for content %s", ErrContentIntegrity, contentID)
	}
	if !bytes.Equal(filesystemBytes, blob.Bytes) {
		return nil, fmt.Errorf("%w: blob copies differ for content %s", ErrContentIntegrity, contentID)
	}
	if utf8.Valid(blob.Bytes) {
		return map[string]any{
			"representation": "text",
			"encoding":       "utf-8",
			"text":           string(blob.Bytes),
			"bytes_base64":   nil,
		}, nil
	}
	return map[string]any{
		"representation": "base64",
		"encoding":       "base64",
		"text":           nil,
		"bytes_base64":   base64.StdEncoding.EncodeToString(blob.Bytes),
	}, nil
}

func (encoder *Encoder) writeLine(value map[string]any) error {
	encoded, err := canonical.MarshalCanonical(value)
	if err != nil {
		return fmt.Errorf("export jsonl: encode JCS: %w", err)
	}
	line := append(encoded.Bytes(), '\n')
	written, err := encoder.writer.Write(line)
	if err != nil {
		return fmt.Errorf("%w: write export", ErrArtifactIO)
	}
	if written != len(line) {
		return fmt.Errorf("%w: %v", ErrArtifactIO, io.ErrShortWrite)
	}
	encoder.stats.RecordCount++
	encoder.stats.ByteCount += int64(written)
	return nil
}

func headerEnvelope(metadata SnapshotMetadata) (map[string]any, error) {
	baseline := migrations.Descriptors()
	if metadata.SchemaVersion != migrations.BaselineVersion || !validPrefixedDigest(metadata.SchemaFingerprint) ||
		len(metadata.Migrations) != len(baseline) {
		return nil, fmt.Errorf("%w: invalid snapshot metadata", ErrSchemaCoverage)
	}
	migrationRecords := make([]any, len(metadata.Migrations))
	for index, descriptor := range metadata.Migrations {
		want := baseline[index]
		if descriptor.Version != int64(index+1) || descriptor.Name != want.Name || descriptor.ByteSize != int64(want.Bytes) ||
			descriptor.SHA256 != "sha256:"+want.SHA256 || !validPrefixedDigest(descriptor.SHA256) {
			return nil, fmt.Errorf("%w: invalid migration descriptor", ErrSchemaCoverage)
		}
		migrationRecords[index] = map[string]any{
			"version":   strconv.FormatInt(descriptor.Version, 10),
			"name":      descriptor.Name,
			"byte_size": strconv.FormatInt(descriptor.ByteSize, 10),
			"sha256":    descriptor.SHA256,
		}
	}
	head, err := headObject(metadata.CapturedHead)
	if err != nil {
		return nil, err
	}
	return recordEnvelope("export_header", "header", map[string]any{
		"catalog_version":       CatalogVersion,
		"schema_version":        strconv.FormatInt(metadata.SchemaVersion, 10),
		"schema_fingerprint":    metadata.SchemaFingerprint,
		"migration_descriptors": migrationRecords,
		"captured_head":         head,
		"ordering_version":      OrderingVersion,
		"semantics":             SnapshotSemantics,
		"encoding": map[string]any{
			"integer":         "decimal-string",
			"record_digest":   "lowercase-hex-no-prefix",
			"metadata_digest": "sha256-prefix-lowercase-hex",
			"binary":          "rfc4648-base64-padded",
			"text":            "utf-8-json-string",
			"line_end":        "lf",
		},
		"external_copy_notice": ExternalCopyNotice,
	}), nil
}

func headObject(head CapturedHead) (map[string]any, error) {
	result := map[string]any{
		"exists":       head.Exists,
		"commit_id":    nil,
		"commit_seq":   nil,
		"committed_at": nil,
		"committed_tz": nil,
	}
	if !head.Exists {
		if !head.CommitID.IsZero() || head.CommitSeq != 0 || head.CommittedAt != 0 || head.CommittedTZ != "" {
			return nil, fmt.Errorf("%w: absent captured head has values", ErrSchemaCoverage)
		}
		return result, nil
	}
	if err := errors.Join(head.CommitID.Validate(), head.CommitSeq.Validate(), head.CommittedTZ.Validate()); err != nil {
		return nil, fmt.Errorf("%w: invalid captured head", ErrSchemaCoverage)
	}
	result["commit_id"] = head.CommitID.String()
	result["commit_seq"] = head.CommitSeq.String()
	result["committed_at"] = head.CommittedAt.String()
	result["committed_tz"] = head.CommittedTZ.String()
	return result, nil
}

func recordEnvelope(recordType, recordID string, record map[string]any) map[string]any {
	return map[string]any{
		"format_version": FormatVersion,
		"record_type":    recordType,
		"record_id":      recordID,
		"record":         record,
	}
}

func encodeColumns(columns []ColumnDescriptor, values []any) (map[string]any, error) {
	record := make(map[string]any, len(columns))
	for index, descriptor := range columns {
		value := values[index]
		if value == nil {
			if !descriptor.Nullable {
				return nil, fmt.Errorf("non-null column %s is NULL", descriptor.Name)
			}
			record[descriptor.Name] = nil
			continue
		}
		switch descriptor.Storage {
		case StorageText:
			textValue, ok := value.(string)
			if !ok || !utf8.ValidString(textValue) {
				return nil, fmt.Errorf("column %s is not UTF-8 TEXT", descriptor.Name)
			}
			if strings.HasSuffix(descriptor.Name, "_id") {
				if _, err := canonical.ParseID(textValue); err != nil {
					return nil, fmt.Errorf("column %s is not a canonical ID", descriptor.Name)
				}
			}
			if strings.HasSuffix(descriptor.Name, "_tz") {
				if _, err := canonical.ParseTimezone(textValue); err != nil {
					return nil, fmt.Errorf("column %s is not a canonical timezone", descriptor.Name)
				}
			}
			record[descriptor.Name] = textValue
		case StorageInteger:
			integerValue, ok := value.(int64)
			if !ok {
				return nil, fmt.Errorf("column %s is not INTEGER", descriptor.Name)
			}
			record[descriptor.Name] = strconv.FormatInt(integerValue, 10)
		case StorageBlob:
			binaryValue, ok := value.([]byte)
			if !ok {
				return nil, fmt.Errorf("column %s is not BLOB", descriptor.Name)
			}
			switch descriptor.Wire {
			case WireDigest:
				if len(binaryValue) != sha256.Size {
					return nil, fmt.Errorf("digest column %s has invalid size", descriptor.Name)
				}
				record[descriptor.Name] = hex.EncodeToString(binaryValue)
			case WireBase64:
				record[descriptor.Name] = base64.StdEncoding.EncodeToString(binaryValue)
			default:
				return nil, fmt.Errorf("column %s has invalid BLOB encoding", descriptor.Name)
			}
		default:
			return nil, fmt.Errorf("column %s has invalid storage kind", descriptor.Name)
		}
	}
	return record, nil
}

func validPrefixedDigest(value string) bool {
	if !strings.HasPrefix(value, "sha256:") {
		return false
	}
	_, err := canonical.ParseDigestHex(strings.TrimPrefix(value, "sha256:"))
	return err == nil
}
