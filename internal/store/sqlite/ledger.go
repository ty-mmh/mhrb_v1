package sqlite

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"

	"mahoroba.local/mahoroba/internal/canonical"
)

func (r *CanonicalRepository) WalkResidentContents(ctx context.Context, residentID canonical.ID, visit func(canonical.LedgerContent) error) error {
	if visit == nil {
		return fmt.Errorf("sqlite: nil content visitor")
	}
	rows, err := r.store.reader.QueryContext(ctx, `SELECT
		c.content_id, c.owner_resident_id, c.content_class, c.erasure_state,
		c.blob_hash, c.commitment, c.commitment_salt,
		b.blob_hash IS NOT NULL, b.content
		FROM content_objects AS c
		LEFT JOIN blobs AS b
		  ON b.dedupe_scope_id = c.owner_resident_id
		 AND b.hash_algorithm = c.blob_hash_algorithm
		 AND b.blob_hash = c.blob_hash
		WHERE c.owner_resident_id = ?
		ORDER BY c.content_id`, residentID.String())
	if err != nil {
		return fmt.Errorf("sqlite: open content verification cursor: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		if err := ctx.Err(); err != nil {
			return err
		}
		content := canonical.LedgerContent{Found: true}
		var contentRaw, ownerRaw string
		var blobHashRaw, commitmentRaw, saltRaw, logicalBytes []byte
		var blobFound int
		if err := rows.Scan(&contentRaw, &ownerRaw, &content.Class, &content.ErasureState,
			&blobHashRaw, &commitmentRaw, &saltRaw, &blobFound, &logicalBytes); err != nil {
			return err
		}
		content.ID, err = canonical.ParseID(contentRaw)
		if err != nil {
			return err
		}
		content.ResidentID, err = canonical.ParseID(ownerRaw)
		if err != nil {
			return err
		}
		content.Commitment, err = canonical.DigestFromBytes(commitmentRaw)
		if err != nil {
			return err
		}
		if blobHashRaw != nil {
			value, err := canonical.DigestFromBytes(blobHashRaw)
			if err != nil {
				return err
			}
			content.BlobHash = &value
		}
		if saltRaw != nil {
			value, err := canonical.ContentSaltFromBytes(saltRaw)
			if err != nil {
				return err
			}
			content.CommitmentSalt = &value
		}
		content.BlobFound = blobFound == 1
		if content.BlobFound {
			content.LogicalBytes = append([]byte(nil), logicalBytes...)
		}
		if err := visit(content); err != nil {
			return err
		}
	}
	return rows.Err()
}

func (r *CanonicalRepository) WalkResidentEvents(ctx context.Context, residentID canonical.ID, visit func(canonical.LedgerRecord) error) error {
	if visit == nil {
		return fmt.Errorf("sqlite: nil ledger visitor")
	}
	rows, err := r.store.reader.QueryContext(ctx, `SELECT
		e.event_id, e.resident_id, e.seq, e.event_type, e.visibility,
		e.delivery_screen, e.delivery_audio, e.ingress, e.trust_level, e.actor_principal_id,
		e.target_principal_id, e.generation_run_id, e.occurred_at, e.occurred_tz,
		e.recorded_at, e.recorded_tz, e.content_id, e.payload_commitment,
		e.prev_event_hash, e.event_hash, e.event_hash_algorithm, e.event_hash_domain,
		e.canonicalization_version,
		c.content_id IS NOT NULL, c.content_id, c.owner_resident_id, c.content_class,
		c.erasure_state, c.blob_hash, c.commitment, c.commitment_salt,
		b.blob_hash IS NOT NULL, b.content
		FROM events AS e
		LEFT JOIN content_objects AS c ON c.content_id = e.content_id
		LEFT JOIN blobs AS b
		  ON b.dedupe_scope_id = c.owner_resident_id
		 AND b.hash_algorithm = c.blob_hash_algorithm
		 AND b.blob_hash = c.blob_hash
		WHERE e.resident_id = ?
		ORDER BY e.seq`, residentID.String())
	if err != nil {
		return fmt.Errorf("sqlite: open ledger cursor: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var rawID, rawResident, eventType, visibility, ingress, trust, actorRaw string
		var targetRaw, runRaw sql.NullString
		var seq, occurredAt, recordedAt int64
		var screen, audio int
		var occurredTZRaw, recordedTZRaw, contentRaw, algorithm, domain, version string
		var commitmentRaw, previousRaw, hashRaw []byte
		var contentFound, blobFound int
		var objectIDRaw, ownerRaw, classRaw, erasureRaw sql.NullString
		var objectBlobHashRaw, objectCommitmentRaw, saltRaw, logicalBytes []byte
		if err := rows.Scan(&rawID, &rawResident, &seq, &eventType, &visibility, &screen, &audio,
			&ingress, &trust, &actorRaw, &targetRaw, &runRaw, &occurredAt, &occurredTZRaw,
			&recordedAt, &recordedTZRaw, &contentRaw, &commitmentRaw, &previousRaw, &hashRaw,
			&algorithm, &domain, &version, &contentFound, &objectIDRaw, &ownerRaw, &classRaw,
			&erasureRaw, &objectBlobHashRaw, &objectCommitmentRaw, &saltRaw, &blobFound,
			&logicalBytes); err != nil {
			return err
		}
		eventID, err := canonical.ParseID(rawID)
		if err != nil {
			return err
		}
		rowResident, err := canonical.ParseID(rawResident)
		if err != nil {
			return err
		}
		rowSeq, err := canonical.NewSeq(seq)
		if err != nil {
			return err
		}
		actorID, err := canonical.ParseID(actorRaw)
		if err != nil {
			return err
		}
		contentID, err := canonical.ParseID(contentRaw)
		if err != nil {
			return err
		}
		occurredTZ, err := canonical.ParseTimezone(occurredTZRaw)
		if err != nil {
			return err
		}
		recordedTZ, err := canonical.ParseTimezone(recordedTZRaw)
		if err != nil {
			return err
		}
		commitment, err := canonical.DigestFromBytes(commitmentRaw)
		if err != nil {
			return err
		}
		hash, err := canonical.DigestFromBytes(hashRaw)
		if err != nil {
			return err
		}
		var target, run *canonical.ID
		if targetRaw.Valid {
			value, err := canonical.ParseID(targetRaw.String)
			if err != nil {
				return err
			}
			target = &value
		}
		if runRaw.Valid {
			value, err := canonical.ParseID(runRaw.String)
			if err != nil {
				return err
			}
			run = &value
		}
		var previous *canonical.Digest
		if previousRaw != nil {
			value, err := canonical.DigestFromBytes(previousRaw)
			if err != nil {
				return err
			}
			previous = &value
		}
		content := canonical.LedgerContent{Found: contentFound == 1, BlobFound: blobFound == 1}
		if content.Found {
			if !objectIDRaw.Valid || !ownerRaw.Valid || !classRaw.Valid || !erasureRaw.Valid {
				return fmt.Errorf("sqlite: incomplete content object for event %s", eventID)
			}
			content.ID, err = canonical.ParseID(objectIDRaw.String)
			if err != nil {
				return err
			}
			content.ResidentID, err = canonical.ParseID(ownerRaw.String)
			if err != nil {
				return err
			}
			content.Class = classRaw.String
			content.ErasureState = erasureRaw.String
			content.Commitment, err = canonical.DigestFromBytes(objectCommitmentRaw)
			if err != nil {
				return err
			}
			if objectBlobHashRaw != nil {
				value, err := canonical.DigestFromBytes(objectBlobHashRaw)
				if err != nil {
					return err
				}
				content.BlobHash = &value
			}
			if saltRaw != nil {
				value, err := canonical.ContentSaltFromBytes(saltRaw)
				if err != nil {
					return err
				}
				content.CommitmentSalt = &value
			}
			if content.BlobFound {
				content.LogicalBytes = append([]byte(nil), logicalBytes...)
			}
		}
		envelope, err := canonical.MarshalCanonical(eventHashEnvelope{
			EventID: eventID, ResidentID: rowResident, Seq: rowSeq, EventType: eventType,
			Visibility: visibility, DeliveryScreen: screen == 1, DeliveryAudio: audio == 1,
			Ingress: ingress, TrustLevel: trust, ActorPrincipalID: actorID,
			TargetPrincipalID: target, GenerationRunID: run,
			OccurredAt: canonical.Instant(occurredAt), OccurredTZ: occurredTZ,
			RecordedAt: canonical.Instant(recordedAt), RecordedTZ: recordedTZ,
			ContentID: contentID, PayloadCommitment: commitment, PrevEventHash: previous,
			EventHashAlgorithm: algorithm, EventHashDomain: domain, CanonicalizationVersion: version,
		})
		if err != nil {
			return err
		}
		if err := visit(canonical.LedgerRecord{
			EventID: eventID, ResidentID: rowResident, Seq: rowSeq,
			ContentID: contentID, PayloadCommitment: commitment, Content: content,
			PrevEventHash: previous, EventHash: hash, Envelope: envelope,
		}); err != nil {
			return err
		}
	}
	return rows.Err()
}

func (r *CanonicalRepository) ValidateLedgerEnvelope(record canonical.LedgerRecord) error {
	var envelope eventHashEnvelope
	if err := json.Unmarshal(record.Envelope.Bytes(), &envelope); err != nil {
		return err
	}
	if envelope.EventID != record.EventID || envelope.ResidentID != record.ResidentID || envelope.Seq != record.Seq {
		return fmt.Errorf("sqlite: ledger envelope identity mismatch")
	}
	if (envelope.PrevEventHash == nil) != (record.PrevEventHash == nil) ||
		(envelope.PrevEventHash != nil && *envelope.PrevEventHash != *record.PrevEventHash) {
		return fmt.Errorf("sqlite: ledger envelope previous hash mismatch")
	}
	if envelope.ContentID != record.ContentID {
		return fmt.Errorf("sqlite: ledger envelope content reference mismatch")
	}
	if envelope.PayloadCommitment != record.PayloadCommitment {
		return fmt.Errorf("sqlite: ledger envelope payload commitment mismatch")
	}
	return nil
}

var _ canonical.LedgerSource = (*CanonicalRepository)(nil)
var _ canonical.EnvelopeValidator = (*CanonicalRepository)(nil)
