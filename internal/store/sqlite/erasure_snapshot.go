package sqlite

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"slices"
	"strings"

	"mahoroba.local/mahoroba/internal/canonical"
	"mahoroba.local/mahoroba/internal/contentref"
	"mahoroba.local/mahoroba/internal/domain"
	"mahoroba.local/mahoroba/internal/erasure"
	"mahoroba.local/mahoroba/internal/integrity"
)

type ErasureSnapshotRepository struct {
	store *Store
	blobs erasure.BlobReader
}

func (store *Store) ErasureSource(blobs erasure.BlobReader) *ErasureSnapshotRepository {
	return &ErasureSnapshotRepository{store: store, blobs: blobs}
}

func (inspection *Inspection) ErasureSource(blobs erasure.BlobReader) *ErasureSnapshotRepository {
	return &ErasureSnapshotRepository{store: inspection.store, blobs: blobs}
}

func (repository *ErasureSnapshotRepository) CaptureErasureSnapshot(ctx context.Context, request erasure.CaptureRequest) (_ erasure.Snapshot, resultErr error) {
	if repository == nil || repository.store == nil || repository.store.reader == nil {
		return erasure.Snapshot{}, errors.New("sqlite: nil erasure source")
	}
	if repository.blobs == nil {
		return erasure.Snapshot{}, errors.New("sqlite: erasure planning requires filesystem blob authority")
	}
	tx, err := repository.store.reader.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return erasure.Snapshot{}, err
	}
	defer func() {
		if err := tx.Rollback(); err != nil && !errors.Is(err, sql.ErrTxDone) {
			resultErr = errors.Join(resultErr, err)
		}
	}()
	if err := requireErasurePlanningPipelines(ctx, tx, request.IntegrityPipelineVersionID, request.MemoryStatusPipelineVersionID); err != nil {
		return erasure.Snapshot{}, err
	}
	var snapshot erasure.Snapshot
	var headRaw string
	var headSeq int64
	if err := tx.QueryRowContext(ctx, `SELECT canonical_commit_id,commit_seq FROM canonical_commits ORDER BY commit_seq DESC LIMIT 1`).Scan(&headRaw, &headSeq); err != nil {
		return snapshot, fmt.Errorf("sqlite: erasure capture head: %w", err)
	}
	snapshot.HeadCommitID, err = canonical.ParseID(headRaw)
	if err != nil {
		return snapshot, err
	}
	snapshot.HeadCommitSeq, err = canonical.NewCommitSeq(headSeq)
	if err != nil {
		return snapshot, err
	}
	var ownerRaw, ownerKind string
	err = tx.QueryRowContext(ctx, `SELECT t.actor_principal_id,p.kind FROM resident_status_transitions t JOIN canonical_commits c ON c.canonical_commit_id=t.canonical_commit_id JOIN principals p ON p.principal_id=t.actor_principal_id WHERE t.resident_id=? AND t.from_status IS NULL AND t.to_status='draft' ORDER BY c.commit_seq,t.resident_status_transition_id LIMIT 1`, request.ResidentID.String()).Scan(&ownerRaw, &ownerKind)
	if err != nil {
		return snapshot, fmt.Errorf("sqlite: erasure owner: %w", err)
	}
	snapshot.OwnerHuman = ownerRaw == request.ActorPrincipalID.String() && ownerKind == "human"
	if err := tx.QueryRowContext(ctx, `SELECT to_status FROM resident_status_transitions t JOIN canonical_commits c ON c.canonical_commit_id=t.canonical_commit_id WHERE t.resident_id=? ORDER BY c.commit_seq DESC,t.resident_status_transition_id DESC LIMIT 1`, request.ResidentID.String()).Scan(&snapshot.ResidentStatus); err != nil {
		return snapshot, err
	}
	var active sql.NullString
	if err := tx.QueryRowContext(ctx, `SELECT active_resident_id FROM runtime_config WHERE singleton_id=1`).Scan(&active); err != nil && !errors.Is(err, sql.ErrNoRows) {
		return snapshot, err
	}
	if active.Valid {
		id, err := canonical.ParseID(active.String)
		if err != nil {
			return snapshot, err
		}
		snapshot.ActiveResidentID = &id
	}

	contents, err := captureErasureContents(ctx, tx, repository.blobs, request.ResidentID)
	if err != nil {
		return snapshot, err
	}
	snapshot.Contents = contents
	aliases, err := captureErasureAliases(ctx, tx, request.ResidentID)
	if err != nil {
		return snapshot, err
	}
	snapshot.Aliases = aliases
	references, err := captureErasureDirectReferences(ctx, tx, request.ResidentID)
	if err != nil {
		return snapshot, err
	}
	snapshot.DirectReferences = references
	lineage, blockers, err := captureErasureLineage(ctx, tx, request.ResidentID, contents)
	if err != nil {
		return snapshot, err
	}
	snapshot.Lineage = lineage
	snapshot.Blockers = append(snapshot.Blockers, blockers...)
	snapshot.MandatoryWorkCount, err = mandatoryErasureWorkCount(ctx, tx, request.ResidentID)
	if err != nil {
		return snapshot, err
	}
	snapshot.Running, err = captureErasureRunning(ctx, tx, request.ResidentID)
	if err != nil {
		return snapshot, err
	}
	for _, definition := range activeProjectionDefinitions {
		snapshot.ProjectionDefinitions = append(snapshot.ProjectionDefinitions, erasure.ProjectionDefinition{Name: string(definition.Name), Version: string(definition.Version)})
	}
	if err := tx.Commit(); err != nil {
		return snapshot, err
	}
	return snapshot, nil
}

func requireErasurePlanningPipelines(ctx context.Context, tx *sql.Tx, integrityID, memoryStatusID canonical.ID) error {
	if err := integrityID.Validate(); err != nil {
		return errors.Join(erasure.ErrIntegrityPipelineRequired, err)
	}
	if err := memoryStatusID.Validate(); err != nil {
		return errors.Join(erasure.ErrIntegrityPipelineRequired, err)
	}
	for _, expected := range []struct {
		id, kind, version, definition string
	}{
		{integrityID.String(), integrity.IntegrityPipelineKind, integrity.IntegrityPipelineVersion, integrityPipelineDefinitionV1},
		{memoryStatusID.String(), "memory_status", domain.MemoryStatusPipelineVersion, memoryStatusPipelineDefinitionV1},
	} {
		var kind, version, definition string
		if err := tx.QueryRowContext(ctx, `SELECT pipeline_kind,version_key,definition FROM pipeline_versions WHERE pipeline_version_id=?`, expected.id).Scan(&kind, &version, &definition); err != nil {
			return erasure.ErrIntegrityPipelineRequired
		}
		if kind != expected.kind || version != expected.version || definition != expected.definition {
			return erasure.ErrIntegrityPipelineRequired
		}
	}
	return nil
}

func captureErasureContents(ctx context.Context, tx *sql.Tx, blobs erasure.BlobReader, residentID canonical.ID) ([]erasure.ContentSnapshot, error) {
	rows, err := tx.QueryContext(ctx, `SELECT o.content_id,o.owner_resident_id,o.content_class,o.erasure_policy,o.erasure_state,o.commitment,o.blob_hash,b.content,b.byte_size FROM content_objects o LEFT JOIN blobs b ON b.dedupe_scope_id=o.owner_resident_id AND b.hash_algorithm=o.blob_hash_algorithm AND b.blob_hash=o.blob_hash WHERE o.owner_resident_id=? ORDER BY o.content_id`, residentID.String())
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	result := []erasure.ContentSnapshot{}
	for rows.Next() {
		var contentRaw, ownerRaw, class, policy, state string
		var commitmentRaw, blobRaw, bytes []byte
		var size sql.NullInt64
		if err := rows.Scan(&contentRaw, &ownerRaw, &class, &policy, &state, &commitmentRaw, &blobRaw, &bytes, &size); err != nil {
			return nil, err
		}
		contentID, err := canonical.ParseID(contentRaw)
		if err != nil {
			return nil, err
		}
		ownerID, err := canonical.ParseID(ownerRaw)
		if err != nil {
			return nil, err
		}
		commitment, err := canonical.DigestFromBytes(commitmentRaw)
		if err != nil {
			return nil, err
		}
		value := erasure.ContentSnapshot{ContentID: contentID, ResidentID: ownerID, ContentClass: class, ErasurePolicy: policy, ErasureState: state, Commitment: commitment}
		if len(blobRaw) != 0 {
			digest, err := canonical.DigestFromBytes(blobRaw)
			if err != nil {
				return nil, err
			}
			value.BlobHash = &digest
			value.SQLiteBlobValid = size.Valid && size.Int64 == int64(len(bytes)) && canonical.HashBlob(bytes) == digest
			if !size.Valid {
				value.SQLiteBlobStatus = "missing"
			} else if value.SQLiteBlobValid {
				value.SQLiteBlobStatus = "valid"
			} else {
				value.SQLiteBlobStatus = "hash_mismatch"
			}
			if value.SQLiteBlobValid {
				reader, openErr := blobs.Open(ctx, ownerID, digest)
				if openErr == nil {
					actual, n, readErr := canonical.HashBlobReader(reader)
					closeErr := reader.Close()
					value.FilesystemBlobValid = readErr == nil && closeErr == nil && n == int64(len(bytes)) && actual == digest
					if value.FilesystemBlobValid {
						value.FilesystemBlobStatus = "valid"
					} else {
						value.FilesystemBlobStatus = "hash_mismatch"
					}
				} else {
					value.FilesystemBlobStatus = "missing"
				}
			}
		}
		result = append(result, value)
	}
	return result, rows.Err()
}

func captureErasureAliases(ctx context.Context, tx *sql.Tx, residentID canonical.ID) ([]erasure.AliasSnapshot, error) {
	rows, err := tx.QueryContext(ctx, `SELECT c.claim_id,c.owner_resident_id,c.statement_content_id,c.statement_hash,e.claim_statement_erasure_event_id,e.content_erasure_event_id,e.canonical_commit_id,COALESCE((SELECT st.to_status FROM claim_status_transitions st JOIN canonical_commits cc ON cc.canonical_commit_id=st.canonical_commit_id WHERE st.claim_id=c.claim_id ORDER BY cc.commit_seq DESC,st.status_transition_id DESC LIMIT 1),'active') FROM claims c LEFT JOIN claim_statement_erasure_events e ON e.claim_id=c.claim_id WHERE c.owner_resident_id=? ORDER BY c.statement_content_id,c.claim_id`, residentID.String())
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	result := []erasure.AliasSnapshot{}
	for rows.Next() {
		var claimRaw, ownerRaw, contentRaw, status string
		var hash []byte
		var eventRaw, contentEventRaw, commitRaw sql.NullString
		if err := rows.Scan(&claimRaw, &ownerRaw, &contentRaw, &hash, &eventRaw, &contentEventRaw, &commitRaw, &status); err != nil {
			return nil, err
		}
		claimID, err := canonical.ParseID(claimRaw)
		if err != nil {
			return nil, err
		}
		ownerID, err := canonical.ParseID(ownerRaw)
		if err != nil {
			return nil, err
		}
		contentID, err := canonical.ParseID(contentRaw)
		if err != nil {
			return nil, err
		}
		value := erasure.AliasSnapshot{ClaimID: claimID, ResidentID: ownerID, StatementContentID: contentID, StatementHashPresent: len(hash) != 0, CurrentStatus: status}
		if eventRaw.Valid {
			id, _ := canonical.ParseID(eventRaw.String)
			value.ClaimStatementErasureEventID = &id
		}
		if contentEventRaw.Valid {
			id, _ := canonical.ParseID(contentEventRaw.String)
			value.ContentErasureEventID = &id
		}
		if commitRaw.Valid {
			id, _ := canonical.ParseID(commitRaw.String)
			value.CanonicalCommitID = &id
		}
		result = append(result, value)
	}
	return result, rows.Err()
}

func captureErasureDirectReferences(ctx context.Context, tx *sql.Tx, residentID canonical.ID) ([]erasure.DirectReference, error) {
	result := []erasure.DirectReference{}
	for _, descriptor := range contentref.DirectDescriptors() {
		rows, err := queryContentReferenceDescriptor(ctx, tx, descriptor, residentID)
		if err != nil {
			return nil, err
		}
		for rows.Next() {
			var referrerRaw, contentRaw, owner, state string
			var algorithm sql.NullString
			var digest []byte
			if err := rows.Scan(&referrerRaw, &contentRaw, &owner, &state, &algorithm, &digest); err != nil {
				_ = rows.Close()
				return nil, err
			}
			contentID, err := canonical.ParseID(contentRaw)
			if err != nil {
				_ = rows.Close()
				return nil, err
			}
			result = append(result, erasure.DirectReference{ContentID: contentID, RuleID: descriptor.RuleID, ReferrerKind: descriptor.ReferrerKind, ReferrerID: referrerRaw, ReferrerField: descriptor.ReferrerField})
		}
		if err := rows.Err(); err != nil {
			_ = rows.Close()
			return nil, err
		}
		if err := rows.Close(); err != nil {
			return nil, err
		}
	}
	slices.SortFunc(result, func(a, b erasure.DirectReference) int {
		return strings.Compare(a.ContentID.String()+"\x00"+a.RuleID+"\x00"+a.ReferrerID, b.ContentID.String()+"\x00"+b.RuleID+"\x00"+b.ReferrerID)
	})
	return result, nil
}

func captureErasureLineage(ctx context.Context, tx *sql.Tx, residentID canonical.ID, contents []erasure.ContentSnapshot) ([]erasure.LineageEdge, []erasure.Blocker, error) {
	digests := map[canonical.ID]*canonical.Digest{}
	for _, content := range contents {
		digests[content.ContentID] = content.BlobHash
	}
	rows, err := tx.QueryContext(ctx, `SELECT i.generation_run_input_id,i.generation_run_id,i.source_type,i.source_id,i.content_id,r.resident_id FROM generation_run_inputs i JOIN generation_runs r ON r.generation_run_id=i.generation_run_id WHERE r.resident_id=? ORDER BY i.generation_run_id,i.ordinal`, residentID.String())
	if err != nil {
		return nil, nil, err
	}
	defer rows.Close()
	edges := []erasure.LineageEdge{}
	blockers := []erasure.Blocker{}
	for rows.Next() {
		var inputRaw, runRaw, sourceType, inputContentRaw, runResident string
		var sourceRaw sql.NullString
		if err := rows.Scan(&inputRaw, &runRaw, &sourceType, &sourceRaw, &inputContentRaw, &runResident); err != nil {
			return nil, nil, err
		}
		runID, _ := canonical.ParseID(runRaw)
		inputID, _ := canonical.ParseID(inputContentRaw)
		var parentRaw, parentResident string
		valid := true
		hasSource := true
		switch sourceType {
		case "event":
			if !sourceRaw.Valid {
				valid = false
			} else {
				valid = tx.QueryRowContext(ctx, `SELECT content_id,resident_id FROM events WHERE event_id=?`, sourceRaw.String).Scan(&parentRaw, &parentResident) == nil
			}
		case "claim":
			if !sourceRaw.Valid {
				valid = false
			} else {
				valid = tx.QueryRowContext(ctx, `SELECT statement_content_id,owner_resident_id FROM claims WHERE claim_id=?`, sourceRaw.String).Scan(&parentRaw, &parentResident) == nil
			}
		case "resident_revision":
			if !sourceRaw.Valid {
				valid = false
			} else {
				valid = tx.QueryRowContext(ctx, `SELECT content_id,resident_id FROM resident_revisions WHERE revision_id=?`, sourceRaw.String).Scan(&parentRaw, &parentResident) == nil
			}
		case "content":
			if !sourceRaw.Valid {
				valid = false
			} else {
				parentRaw = sourceRaw.String
				valid = tx.QueryRowContext(ctx, `SELECT owner_resident_id FROM content_objects WHERE content_id=?`, parentRaw).Scan(&parentResident) == nil
			}
		case "runtime_projection":
			valid = !sourceRaw.Valid
			hasSource = false
		default:
			valid = false
		}
		if valid && hasSource && parentResident != residentID.String() {
			field := "cross_resident_reference"
			target := parentRaw
			blockers = append(blockers, erasure.Blocker{Code: "cross_resident_reference", TargetKind: "lineage", TargetID: &target, TargetField: &field, RequiredActionCodes: []string{"repair_static_design"}})
			continue
		}
		if !valid {
			field := "source"
			target := inputRaw
			blockers = append(blockers, erasure.Blocker{Code: "unknown_lineage_source", TargetKind: "generation_input", TargetID: &target, TargetField: &field, RequiredActionCodes: []string{"repair_static_design"}})
			continue
		}
		if hasSource {
			parentID, err := canonical.ParseID(parentRaw)
			if err != nil {
				return nil, nil, err
			}
			edges = append(edges, makeLineageEdge(parentID, inputID, runID, digests))
		}
		products, err := captureRunProducts(ctx, tx, runRaw, residentID)
		if err != nil {
			return nil, nil, err
		}
		for _, child := range products {
			if _, exists := digests[child]; !exists {
				field := "product_set"
				target := runRaw
				blockers = append(blockers, erasure.Blocker{Code: "invalid_run_product", TargetKind: "generation_run", TargetID: &target, TargetField: &field, RequiredActionCodes: []string{"repair_static_design"}})
				continue
			}
			edges = append(edges, makeLineageEdge(inputID, child, runID, digests))
		}
	}
	if err := rows.Err(); err != nil {
		return nil, nil, err
	}
	recallRows, err := tx.QueryContext(ctx, `SELECT r.generation_run_id,q.query_content_id FROM generation_runs r JOIN recall_runs q ON q.recall_run_id=r.recall_run_id WHERE r.resident_id=? AND q.query_content_id IS NOT NULL ORDER BY r.generation_run_id`, residentID.String())
	if err != nil {
		return nil, nil, err
	}
	defer recallRows.Close()
	for recallRows.Next() {
		var runRaw, parentRaw string
		if err := recallRows.Scan(&runRaw, &parentRaw); err != nil {
			return nil, nil, err
		}
		runID, _ := canonical.ParseID(runRaw)
		parentID, _ := canonical.ParseID(parentRaw)
		if _, exists := digests[parentID]; !exists {
			field := "cross_resident_reference"
			target := parentRaw
			blockers = append(blockers, erasure.Blocker{Code: "cross_resident_reference", TargetKind: "lineage", TargetID: &target, TargetField: &field, RequiredActionCodes: []string{"repair_static_design"}})
			continue
		}
		products, err := captureRunProducts(ctx, tx, runRaw, residentID)
		if err != nil {
			return nil, nil, err
		}
		for _, child := range products {
			if _, exists := digests[child]; !exists {
				field := "product_set"
				target := runRaw
				blockers = append(blockers, erasure.Blocker{Code: "invalid_run_product", TargetKind: "generation_run", TargetID: &target, TargetField: &field, RequiredActionCodes: []string{"repair_static_design"}})
				continue
			}
			edges = append(edges, makeLineageEdge(parentID, child, runID, digests))
		}
	}
	return edges, blockers, recallRows.Err()
}

func captureRunProducts(ctx context.Context, tx *sql.Tx, runID string, residentID canonical.ID) ([]canonical.ID, error) {
	parsedRunID, err := canonical.ParseID(runID)
	if err != nil {
		return nil, err
	}
	// Outcome ordering and ownership stay behind the single typed reducer
	// repository. Erasure may consume its typed content references, but must
	// not grow an independent direct outcome-table SELECT.
	run, history, err := (generationOutcomeRepository{}).capturedHistory(ctx, tx, parsedRunID, residentID, nil)
	if err != nil {
		return nil, err
	}
	if run.RunID != parsedRunID || run.ResidentID != residentID {
		return nil, ErrInvalidGenerationOutcomeHistory
	}
	products := make(map[canonical.ID]struct{})
	for _, outcome := range history {
		if outcome.OutputContentID != nil {
			products[*outcome.OutputContentID] = struct{}{}
		}
		if outcome.ErrorDetailContentID != nil {
			products[*outcome.ErrorDetailContentID] = struct{}{}
		}
	}
	rows, err := tx.QueryContext(ctx, `SELECT content_id FROM (SELECT content_id FROM events WHERE generation_run_id=? UNION SELECT statement_content_id FROM claims WHERE created_by_run_id=? UNION SELECT content_id FROM resident_revisions WHERE created_by_run_id=? UNION SELECT reason_content_id FROM resident_revisions WHERE created_by_run_id=? UNION SELECT reason_content_id FROM claim_evidence WHERE created_by_run_id=? UNION SELECT reason_content_id FROM claim_stage_transitions WHERE generation_run_id=? UNION SELECT reason_content_id FROM claim_relations WHERE generation_run_id=? UNION SELECT reason_content_id FROM claim_view_scope_assertions WHERE generation_run_id=?) WHERE content_id IS NOT NULL ORDER BY content_id`, runID, runID, runID, runID, runID, runID, runID, runID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	for rows.Next() {
		var raw string
		if err := rows.Scan(&raw); err != nil {
			return nil, err
		}
		id, err := canonical.ParseID(raw)
		if err != nil {
			return nil, err
		}
		products[id] = struct{}{}
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	result := make([]canonical.ID, 0, len(products))
	for id := range products {
		result = append(result, id)
	}
	slices.SortFunc(result, func(a, b canonical.ID) int { return strings.Compare(a.String(), b.String()) })
	return result, nil
}
func makeLineageEdge(parent, child, run canonical.ID, digests map[canonical.ID]*canonical.Digest) erasure.LineageEdge {
	same := false
	if a, b := digests[parent], digests[child]; a != nil && b != nil {
		same = *a == *b
	}
	return erasure.LineageEdge{ParentContentID: parent, ChildContentID: child, RunID: run, SameLogicalBlob: same, Valid: true}
}
func captureErasureRunning(ctx context.Context, tx *sql.Tx, residentID canonical.ID) ([]erasure.RunningWork, error) {
	rows, err := tx.QueryContext(ctx, `SELECT generation_run_id,purpose FROM generation_runs WHERE resident_id=? ORDER BY generation_run_id`, residentID.String())
	if err != nil {
		return nil, err
	}
	type candidate struct {
		id      canonical.ID
		purpose string
	}
	candidates := []candidate{}
	for rows.Next() {
		var raw, purpose string
		if err := rows.Scan(&raw, &purpose); err != nil {
			return nil, err
		}
		id, err := canonical.ParseID(raw)
		if err != nil {
			return nil, err
		}
		candidates = append(candidates, candidate{id: id, purpose: purpose})
	}
	if err := rows.Err(); err != nil {
		_ = rows.Close()
		return nil, err
	}
	if err := rows.Close(); err != nil {
		return nil, err
	}
	result := []erasure.RunningWork{}
	for _, value := range candidates {
		summary, err := (generationOutcomeRepository{}).Summary(ctx, tx, value.id, residentID)
		if err != nil {
			return nil, err
		}
		if summary.Latest.State != "running" {
			continue
		}
		contentIDs, err := captureRunningContentIDs(ctx, tx, value.id, residentID)
		if err != nil {
			return nil, err
		}
		result = append(result, erasure.RunningWork{RunID: value.id, Purpose: value.purpose, ContentIDs: contentIDs})
	}
	return result, nil
}

func captureRunningContentIDs(ctx context.Context, tx *sql.Tx, runID, residentID canonical.ID) ([]canonical.ID, error) {
	products, err := captureRunProducts(ctx, tx, runID.String(), residentID)
	if err != nil {
		return nil, err
	}
	values := make(map[canonical.ID]struct{}, len(products))
	for _, id := range products {
		values[id] = struct{}{}
	}
	rows, err := tx.QueryContext(ctx, `SELECT content_id FROM generation_run_inputs WHERE generation_run_id=? UNION SELECT recall.query_content_id FROM generation_runs run JOIN recall_runs recall ON recall.recall_run_id=run.recall_run_id WHERE run.generation_run_id=? AND recall.query_content_id IS NOT NULL ORDER BY content_id`, runID.String(), runID.String())
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	for rows.Next() {
		var raw string
		if err := rows.Scan(&raw); err != nil {
			return nil, err
		}
		id, err := canonical.ParseID(raw)
		if err != nil {
			return nil, err
		}
		values[id] = struct{}{}
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	result := make([]canonical.ID, 0, len(values))
	for id := range values {
		result = append(result, id)
	}
	slices.SortFunc(result, func(a, b canonical.ID) int { return strings.Compare(a.String(), b.String()) })
	return result, nil
}

var _ erasure.SnapshotSource = (*ErasureSnapshotRepository)(nil)
