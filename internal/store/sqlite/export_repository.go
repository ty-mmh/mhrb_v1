package sqlite

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"slices"
	"strings"

	"mahoroba.local/mahoroba/internal/assets/migrations"
	"mahoroba.local/mahoroba/internal/canonical"
	"mahoroba.local/mahoroba/internal/exportjsonl"
)

// ExportRepository exposes the raw Canonical storage representation required
// by mahoroba-jsonl-v1. It deliberately has no mutation methods.
type ExportRepository struct {
	reader *sql.DB
	report SchemaReport
}

// CaptureMetadata obtains the exact header inputs in one short read-only
// transaction. Artifact producers use it only to independently authenticate a
// crash marker before resuming the marker's already-materialized payload.
func (repository *ExportRepository) CaptureMetadata(ctx context.Context) (_ exportjsonl.SnapshotMetadata, resultErr error) {
	if repository == nil || repository.reader == nil {
		return exportjsonl.SnapshotMetadata{}, fmt.Errorf("%w: export repository unavailable", exportjsonl.ErrSourceUnavailable)
	}
	tx, err := repository.reader.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return exportjsonl.SnapshotMetadata{}, fmt.Errorf("%w: begin metadata snapshot", exportjsonl.ErrSourceUnavailable)
	}
	defer func() {
		rollbackErr := tx.Rollback()
		if errors.Is(rollbackErr, sql.ErrTxDone) {
			rollbackErr = nil
		}
		resultErr = errors.Join(resultErr, rollbackErr)
	}()
	if err := validateExportCatalogCoverage(ctx, tx); err != nil {
		return exportjsonl.SnapshotMetadata{}, err
	}
	metadata, err := repository.snapshotMetadata(ctx, tx)
	if err != nil {
		return exportjsonl.SnapshotMetadata{}, err
	}
	if err := tx.Commit(); err != nil {
		return exportjsonl.SnapshotMetadata{}, fmt.Errorf("%w: close metadata snapshot", exportjsonl.ErrSourceUnavailable)
	}
	return metadata, nil
}

func (store *Store) ExportRepository() *ExportRepository {
	if store == nil {
		return &ExportRepository{}
	}
	return &ExportRepository{reader: store.reader, report: store.report}
}

func (inspection *Inspection) ExportRepository() *ExportRepository {
	if inspection == nil || inspection.store == nil {
		return &ExportRepository{}
	}
	return inspection.store.ExportRepository()
}

// StreamSnapshot holds one read-only transaction through header capture, all
// commit-bound rows, and the content section. Table cursors are merged without
// materializing the database so ordering is fixed while memory stays bounded.
func (repository *ExportRepository) StreamSnapshot(
	ctx context.Context,
	sink exportjsonl.SnapshotSink,
) (_ exportjsonl.SnapshotMetadata, resultErr error) {
	if repository == nil || repository.reader == nil || sink == nil {
		return exportjsonl.SnapshotMetadata{}, fmt.Errorf("%w: export repository unavailable", exportjsonl.ErrSourceUnavailable)
	}
	if err := exportjsonl.ValidateCatalog(); err != nil {
		return exportjsonl.SnapshotMetadata{}, err
	}
	tx, err := repository.reader.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return exportjsonl.SnapshotMetadata{}, fmt.Errorf("%w: begin snapshot", exportjsonl.ErrSourceUnavailable)
	}
	defer func() {
		rollbackErr := tx.Rollback()
		if errors.Is(rollbackErr, sql.ErrTxDone) {
			rollbackErr = nil
		}
		resultErr = errors.Join(resultErr, rollbackErr)
	}()

	if err := validateExportCatalogCoverage(ctx, tx); err != nil {
		return exportjsonl.SnapshotMetadata{}, err
	}
	metadata, err := repository.snapshotMetadata(ctx, tx)
	if err != nil {
		return exportjsonl.SnapshotMetadata{}, err
	}
	cursors, err := openExportCursors(ctx, tx)
	if err != nil {
		return exportjsonl.SnapshotMetadata{}, err
	}
	defer func() { resultErr = errors.Join(resultErr, closeExportCursors(cursors)) }()

	if err := sink.BeginSnapshot(ctx, metadata); err != nil {
		return exportjsonl.SnapshotMetadata{}, err
	}
	queue := exportCursorQueue{}
	for _, cursor := range cursors {
		if cursor.hasRow {
			queue = append(queue, cursor)
		}
	}
	for len(queue) > 0 {
		cursor := queue.popFirst()
		if err := sink.WriteCanonicalRecord(ctx, cursor.current); err != nil {
			return exportjsonl.SnapshotMetadata{}, err
		}
		if err := cursor.advance(); err != nil {
			return exportjsonl.SnapshotMetadata{}, fmt.Errorf("%w: read %s", exportjsonl.ErrSourceUnavailable, cursor.descriptor.RecordType)
		}
		if cursor.hasRow {
			queue = append(queue, cursor)
		}
	}
	if err := streamContentObjects(ctx, tx, sink); err != nil {
		return exportjsonl.SnapshotMetadata{}, err
	}
	if err := tx.Commit(); err != nil {
		return exportjsonl.SnapshotMetadata{}, fmt.Errorf("%w: close snapshot", exportjsonl.ErrSourceUnavailable)
	}
	return metadata, nil
}

func (repository *ExportRepository) snapshotMetadata(ctx context.Context, tx *sql.Tx) (exportjsonl.SnapshotMetadata, error) {
	metadata := exportjsonl.SnapshotMetadata{
		SchemaVersion:     repository.report.SchemaVersion,
		SchemaFingerprint: "sha256:" + repository.report.SchemaFingerprint,
	}
	for index, descriptor := range migrations.Descriptors() {
		metadata.Migrations = append(metadata.Migrations, exportjsonl.MigrationDescriptor{
			Version:  int64(index + 1),
			Name:     descriptor.Name,
			ByteSize: int64(descriptor.Bytes),
			SHA256:   "sha256:" + descriptor.SHA256,
		})
	}
	var commitID, committedTZ string
	var commitSeq, committedAt int64
	err := tx.QueryRowContext(ctx, `
		SELECT canonical_commit_id, commit_seq, committed_at, committed_tz
		FROM canonical_commits
		ORDER BY commit_seq DESC
		LIMIT 1
	`).Scan(&commitID, &commitSeq, &committedAt, &committedTZ)
	if errors.Is(err, sql.ErrNoRows) {
		return metadata, nil
	}
	if err != nil {
		return exportjsonl.SnapshotMetadata{}, fmt.Errorf("%w: capture head", exportjsonl.ErrSourceUnavailable)
	}
	parsedID, err := canonical.ParseID(commitID)
	if err != nil {
		return exportjsonl.SnapshotMetadata{}, fmt.Errorf("%w: invalid captured head", exportjsonl.ErrSchemaCoverage)
	}
	parsedSeq, err := canonical.NewCommitSeq(commitSeq)
	if err != nil {
		return exportjsonl.SnapshotMetadata{}, fmt.Errorf("%w: invalid captured head", exportjsonl.ErrSchemaCoverage)
	}
	parsedTZ, err := canonical.ParseTimezone(committedTZ)
	if err != nil {
		return exportjsonl.SnapshotMetadata{}, fmt.Errorf("%w: invalid captured head", exportjsonl.ErrSchemaCoverage)
	}
	metadata.CapturedHead = exportjsonl.CapturedHead{
		Exists: true, CommitID: parsedID, CommitSeq: parsedSeq,
		CommittedAt: canonical.Instant(committedAt), CommittedTZ: parsedTZ,
	}
	return metadata, nil
}

type pragmaColumn struct {
	name    string
	storage string
	notNull bool
	primary bool
	hidden  int
}

func validateExportCatalogCoverage(ctx context.Context, tx *sql.Tx) error {
	catalog := exportjsonl.Catalog()
	expectedCommitTables := make(map[string]struct{}, len(catalog))
	for _, descriptor := range catalog {
		expectedCommitTables[descriptor.Table] = struct{}{}
		columns, err := loadTableColumns(ctx, tx, descriptor.Table)
		if err != nil {
			return err
		}
		if err := compareExportColumns(descriptor.Table, descriptor.Columns, columns); err != nil {
			return err
		}
	}
	contentColumns, err := loadTableColumns(ctx, tx, "content_objects")
	if err != nil {
		return err
	}
	if err := compareExportColumns("content_objects", exportjsonl.ContentObjectColumns(), contentColumns); err != nil {
		return err
	}

	rows, err := tx.QueryContext(ctx, `
		SELECT name
		FROM pragma_table_list
		WHERE schema = 'main' AND type = 'table' AND name NOT LIKE 'sqlite_%'
		ORDER BY name
	`)
	if err != nil {
		return fmt.Errorf("%w: enumerate tables", exportjsonl.ErrSchemaCoverage)
	}
	var tables []string
	for rows.Next() {
		var table string
		if err := rows.Scan(&table); err != nil {
			_ = rows.Close()
			return fmt.Errorf("%w: enumerate tables", exportjsonl.ErrSchemaCoverage)
		}
		tables = append(tables, table)
	}
	if err := errors.Join(rows.Err(), rows.Close()); err != nil {
		return fmt.Errorf("%w: enumerate tables", exportjsonl.ErrSchemaCoverage)
	}
	actualCommitTables := map[string]struct{}{}
	for _, table := range tables {
		columns, err := loadTableColumns(ctx, tx, table)
		if err != nil {
			return err
		}
		for _, column := range columns {
			if column.name == "canonical_commit_id" {
				actualCommitTables[table] = struct{}{}
				break
			}
		}
	}
	if !sameStringSet(expectedCommitTables, actualCommitTables) {
		return fmt.Errorf("%w: commit-bound table set differs", exportjsonl.ErrSchemaCoverage)
	}
	return nil
}

func loadTableColumns(ctx context.Context, tx *sql.Tx, table string) ([]pragmaColumn, error) {
	escaped := strings.ReplaceAll(table, "'", "''")
	rows, err := tx.QueryContext(ctx, "PRAGMA table_xinfo('"+escaped+"')")
	if err != nil {
		return nil, fmt.Errorf("%w: inspect table %s", exportjsonl.ErrSchemaCoverage, table)
	}
	defer rows.Close()
	var result []pragmaColumn
	for rows.Next() {
		var cid, notNull, primary, hidden int
		var name, storage string
		var defaultValue any
		if err := rows.Scan(&cid, &name, &storage, &notNull, &defaultValue, &primary, &hidden); err != nil {
			return nil, fmt.Errorf("%w: inspect table %s", exportjsonl.ErrSchemaCoverage, table)
		}
		result = append(result, pragmaColumn{name: name, storage: storage, notNull: notNull != 0, primary: primary != 0, hidden: hidden})
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("%w: inspect table %s", exportjsonl.ErrSchemaCoverage, table)
	}
	return result, nil
}

func compareExportColumns(table string, expected []exportjsonl.ColumnDescriptor, actual []pragmaColumn) error {
	if len(actual) != len(expected) {
		return fmt.Errorf("%w: %s column count differs", exportjsonl.ErrSchemaCoverage, table)
	}
	for index, want := range expected {
		got := actual[index]
		nonNull := got.notNull || got.primary
		if got.name != want.Name || got.storage != string(want.Storage) || got.hidden != 0 || nonNull == want.Nullable {
			return fmt.Errorf("%w: %s column %d differs", exportjsonl.ErrSchemaCoverage, table, index)
		}
	}
	return nil
}

func sameStringSet(left, right map[string]struct{}) bool {
	if len(left) != len(right) {
		return false
	}
	for value := range left {
		if _, exists := right[value]; !exists {
			return false
		}
	}
	return true
}

type exportCursor struct {
	ctx             context.Context
	rows            *sql.Rows
	descriptor      exportjsonl.RecordDescriptor
	descriptorIndex int
	commitSeq       int64
	current         exportjsonl.RawRecord
	hasRow          bool
}

func openExportCursors(ctx context.Context, tx *sql.Tx) ([]*exportCursor, error) {
	catalog := exportjsonl.Catalog()
	cursors := make([]*exportCursor, 0, len(catalog))
	for index, descriptor := range catalog {
		query := exportRecordQuery(descriptor)
		rows, err := tx.QueryContext(ctx, query)
		if err != nil {
			_ = closeExportCursors(cursors)
			return nil, fmt.Errorf("%w: open %s cursor", exportjsonl.ErrSourceUnavailable, descriptor.RecordType)
		}
		cursor := &exportCursor{ctx: ctx, rows: rows, descriptor: descriptor, descriptorIndex: index}
		if err := cursor.advance(); err != nil {
			_ = rows.Close()
			_ = closeExportCursors(cursors)
			return nil, fmt.Errorf("%w: read %s cursor", exportjsonl.ErrSourceUnavailable, descriptor.RecordType)
		}
		cursors = append(cursors, cursor)
	}
	return cursors, nil
}

func exportRecordQuery(descriptor exportjsonl.RecordDescriptor) string {
	columns := make([]string, len(descriptor.Columns))
	for index, column := range descriptor.Columns {
		columns[index] = "record." + quotedIdentifier(column.Name)
	}
	var query, order string
	if descriptor.Table == "canonical_commits" {
		query = "SELECT record.commit_seq, " + strings.Join(columns, ", ") + " FROM canonical_commits AS record"
	} else {
		query = "SELECT commit_record.commit_seq, " + strings.Join(columns, ", ") +
			" FROM " + quotedIdentifier(descriptor.Table) + " AS record" +
			" JOIN canonical_commits AS commit_record ON commit_record.canonical_commit_id = record." + quotedIdentifier(descriptor.CommitColumn)
	}
	if descriptor.EventSort {
		order = " ORDER BY commit_record.commit_seq, record.resident_id, record.seq, record.event_id"
	} else if descriptor.Table == "canonical_commits" {
		order = " ORDER BY record.commit_seq, record." + quotedIdentifier(descriptor.IDColumn)
	} else {
		order = " ORDER BY commit_record.commit_seq, record." + quotedIdentifier(descriptor.IDColumn)
	}
	return query + order
}

func quotedIdentifier(value string) string {
	return `"` + strings.ReplaceAll(value, `"`, `""`) + `"`
}

func (cursor *exportCursor) advance() error {
	if !cursor.rows.Next() {
		cursor.hasRow = false
		return cursor.rows.Err()
	}
	values := make([]any, len(cursor.descriptor.Columns))
	destinations := make([]any, 1+len(values))
	destinations[0] = &cursor.commitSeq
	for index := range values {
		destinations[index+1] = &values[index]
	}
	if err := cursor.rows.Scan(destinations...); err != nil {
		return err
	}
	idIndex := slices.IndexFunc(cursor.descriptor.Columns, func(column exportjsonl.ColumnDescriptor) bool {
		return column.Name == cursor.descriptor.IDColumn
	})
	if idIndex < 0 {
		return exportjsonl.ErrSchemaCoverage
	}
	recordID, ok := values[idIndex].(string)
	if !ok || recordID == "" {
		return exportjsonl.ErrSchemaCoverage
	}
	cursor.current = exportjsonl.RawRecord{DescriptorIndex: cursor.descriptorIndex, RecordID: recordID, Values: values}
	cursor.hasRow = true
	return nil
}

func closeExportCursors(cursors []*exportCursor) error {
	var result error
	for _, cursor := range cursors {
		if cursor != nil && cursor.rows != nil {
			result = errors.Join(result, cursor.rows.Close())
		}
	}
	return result
}

type exportCursorQueue []*exportCursor

func (queue exportCursorQueue) less(left, right int) bool {
	if queue[left].commitSeq != queue[right].commitSeq {
		return queue[left].commitSeq < queue[right].commitSeq
	}
	return queue[left].descriptor.Ordinal < queue[right].descriptor.Ordinal
}

// popFirst is a bounded 26-way merge. The fixed catalog bound makes the
// linear selection both simpler and independent of mutable row material.
func (queue *exportCursorQueue) popFirst() *exportCursor {
	best := 0
	for index := 1; index < len(*queue); index++ {
		if queue.less(index, best) {
			best = index
		}
	}
	result := (*queue)[best]
	*queue = append((*queue)[:best], (*queue)[best+1:]...)
	return result
}

func streamContentObjects(ctx context.Context, tx *sql.Tx, sink exportjsonl.SnapshotSink) error {
	columns := exportjsonl.ContentObjectColumns()
	selected := make([]string, len(columns))
	for index, column := range columns {
		selected[index] = "content_object." + quotedIdentifier(column.Name)
	}
	query := "SELECT " + strings.Join(selected, ", ") + `,
		blob.dedupe_scope_id, blob.hash_algorithm, blob.blob_hash, blob.byte_size, blob.content
		FROM content_objects AS content_object
		LEFT JOIN blobs AS blob
		  ON blob.dedupe_scope_id = content_object.owner_resident_id
		 AND blob.hash_algorithm = content_object.blob_hash_algorithm
		 AND blob.blob_hash = content_object.blob_hash
		ORDER BY content_object.owner_resident_id, content_object.created_at, content_object.content_id`
	rows, err := tx.QueryContext(ctx, query)
	if err != nil {
		return fmt.Errorf("%w: open content cursor", exportjsonl.ErrSourceUnavailable)
	}
	defer rows.Close()
	for rows.Next() {
		values := make([]any, len(columns))
		destinations := make([]any, len(values)+5)
		for index := range values {
			destinations[index] = &values[index]
		}
		var scope, algorithm, hash, size, content any
		destinations[len(values)] = &scope
		destinations[len(values)+1] = &algorithm
		destinations[len(values)+2] = &hash
		destinations[len(values)+3] = &size
		destinations[len(values)+4] = &content
		if err := rows.Scan(destinations...); err != nil {
			return fmt.Errorf("%w: read content cursor", exportjsonl.ErrSourceUnavailable)
		}
		contentID, ok := values[0].(string)
		if !ok || contentID == "" {
			return fmt.Errorf("%w: content identity", exportjsonl.ErrSchemaCoverage)
		}
		var sqliteBlob *exportjsonl.SQLiteBlob
		if scope != nil || algorithm != nil || hash != nil || size != nil || content != nil {
			scopeString, scopeOK := scope.(string)
			algorithmString, algorithmOK := algorithm.(string)
			hashBytes, hashOK := hash.([]byte)
			sizeInt, sizeOK := size.(int64)
			contentBytes, contentOK := content.([]byte)
			if !scopeOK || !algorithmOK || !hashOK || !sizeOK || !contentOK {
				return fmt.Errorf("%w: malformed SQLite blob for content %s", exportjsonl.ErrContentIntegrity, contentID)
			}
			sqliteBlob = &exportjsonl.SQLiteBlob{
				ResidentID: scopeString, HashAlgorithm: algorithmString,
				Hash: slices.Clone(hashBytes), ByteSize: sizeInt, Bytes: slices.Clone(contentBytes),
			}
		}
		if err := sink.WriteContentObject(ctx, exportjsonl.ContentObject{
			RecordID: contentID, Values: values, SQLiteBlob: sqliteBlob,
		}); err != nil {
			return err
		}
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("%w: read content cursor", exportjsonl.ErrSourceUnavailable)
	}
	return nil
}

var _ exportjsonl.SnapshotSource = (*ExportRepository)(nil)
