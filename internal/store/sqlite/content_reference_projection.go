package sqlite

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"mahoroba.local/mahoroba/internal/canonical"
	"mahoroba.local/mahoroba/internal/contentref"
	"mahoroba.local/mahoroba/internal/projection"
)

// evaluateContentReferences evaluates the complete resident body from one
// SQLite read snapshot. Because content erasure updates locator columns in
// place, a commit-sequence predicate on individual rows is insufficient: the
// snapshot head itself must equal the requested Projection target.
func (repository *ProjectionRepository) evaluateContentReferences(
	ctx context.Context,
	residentID canonical.ID,
	target canonical.Head,
) (_ []projection.ContentReference, resultErr error) {
	if ctx == nil {
		return nil, errors.New("sqlite: nil content references context")
	}
	if err := residentID.Validate(); err != nil {
		return nil, err
	}
	if err := target.Validate(); err != nil {
		return nil, err
	}
	tx, err := repository.store.reader.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return nil, fmt.Errorf("sqlite: begin content references snapshot: %w", err)
	}
	defer func() {
		if rollbackErr := tx.Rollback(); rollbackErr != nil && !errors.Is(rollbackErr, sql.ErrTxDone) {
			resultErr = errors.Join(resultErr, rollbackErr)
		}
	}()

	var snapshotSeq sql.NullInt64
	if err := tx.QueryRowContext(ctx, `SELECT MAX(commit_seq) FROM canonical_commits`).Scan(&snapshotSeq); err != nil {
		return nil, fmt.Errorf("sqlite: capture content references head: %w", err)
	}
	if !snapshotSeq.Valid {
		if target.Exists {
			return nil, projection.ErrPreflightHeadChanged
		}
		return []projection.ContentReference{}, nil
	}
	if !target.Exists || snapshotSeq.Int64 != target.CommitSeq.Int64() {
		return nil, projection.ErrPreflightHeadChanged
	}

	result := make([]projection.ContentReference, 0)
	for _, descriptor := range contentref.DirectDescriptors() {
		rows, err := queryContentReferenceDescriptor(ctx, tx, descriptor, residentID)
		if err != nil {
			return nil, err
		}
		for rows.Next() {
			var referrerIDText, contentIDText, ownerText, erasureState string
			var algorithm sql.NullString
			var rawDigest []byte
			if err := rows.Scan(&referrerIDText, &contentIDText, &ownerText, &erasureState, &algorithm, &rawDigest); err != nil {
				_ = rows.Close()
				return nil, fmt.Errorf("sqlite: scan content reference %s: %w", descriptor.RuleID, err)
			}
			contentID, err := canonical.ParseID(contentIDText)
			if err != nil {
				_ = rows.Close()
				return nil, fmt.Errorf("sqlite: parse content reference content: %w", err)
			}
			referrerID, err := canonical.ParseID(referrerIDText)
			if err != nil {
				_ = rows.Close()
				return nil, fmt.Errorf("sqlite: parse content reference referrer: %w", err)
			}
			ownerID, err := canonical.ParseID(ownerText)
			if err != nil || ownerID != residentID {
				_ = rows.Close()
				return nil, errors.New("sqlite: content reference owner scope mismatch")
			}
			reference := projection.ContentReference{
				ContentID: contentID, ResidentID: ownerID, ReferrerKind: descriptor.ReferrerKind,
				ReferrerID: referrerID, ReferrerField: descriptor.ReferrerField,
			}
			switch erasureState {
			case "present":
				if !algorithm.Valid || algorithm.String != "sha256" || len(rawDigest) == 0 {
					_ = rows.Close()
					return nil, errors.New("sqlite: present content reference has no supported locator")
				}
				digest, err := canonical.DigestFromBytes(rawDigest)
				if err != nil {
					_ = rows.Close()
					return nil, fmt.Errorf("sqlite: parse content reference digest: %w", err)
				}
				algorithmValue := algorithm.String
				dedupeScope := ownerID
				reference.DedupeScopeID = &dedupeScope
				reference.BlobHashAlgorithm = &algorithmValue
				reference.BlobHash = &digest
			case "erased":
				// content_objects retains its code-owned hash algorithm after
				// erasure, while the Projection deliberately removes the entire
				// physical locator tuple.
				if !algorithm.Valid || algorithm.String != "sha256" || rawDigest != nil {
					_ = rows.Close()
					return nil, errors.New("sqlite: erased content reference has an invalid canonical locator state")
				}
			default:
				_ = rows.Close()
				return nil, fmt.Errorf("sqlite: unknown content erasure state %q", erasureState)
			}
			result = append(result, reference)
		}
		if err := rows.Err(); err != nil {
			_ = rows.Close()
			return nil, fmt.Errorf("sqlite: iterate content reference %s: %w", descriptor.RuleID, err)
		}
		if err := rows.Close(); err != nil {
			return nil, err
		}
	}
	projection.SortContentReferences(result)
	if err := projection.ValidateContentReferences(result, residentID); err != nil {
		return nil, err
	}
	if err := tx.Commit(); err != nil {
		return nil, fmt.Errorf("sqlite: close content references snapshot: %w", err)
	}
	return result, nil
}

func queryContentReferenceDescriptor(
	ctx context.Context,
	tx *sql.Tx,
	descriptor contentref.Descriptor,
	residentID canonical.ID,
) (*sql.Rows, error) {
	// Every identifier is sourced only from contentref.DirectDescriptors, whose
	// package init validates a closed compile-time catalog. No request, plan,
	// configuration, or database value can become SQL syntax here.
	query := "SELECT ref." + descriptor.PrimaryKey + ", ref." + descriptor.ContentField +
		", co.owner_resident_id, co.erasure_state, co.blob_hash_algorithm, co.blob_hash" +
		" FROM " + descriptor.Table + " ref" +
		" JOIN content_objects co ON co.content_id = ref." + descriptor.ContentField +
		" WHERE ref." + descriptor.ContentField + " IS NOT NULL AND co.owner_resident_id = ?" +
		" ORDER BY ref." + descriptor.ContentField + ", ref." + descriptor.PrimaryKey
	rows, err := tx.QueryContext(ctx, query, residentID.String())
	if err != nil {
		return nil, fmt.Errorf("sqlite: evaluate content reference %s: %w", descriptor.RuleID, err)
	}
	return rows, nil
}
