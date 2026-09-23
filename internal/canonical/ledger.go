package canonical

import (
	"context"
	"fmt"
)

const (
	ContentErasurePresent = "present"
	ContentErasureErased  = "erased"
)

// LedgerContent is the adapter-neutral material for a Canonical content object.
// Found and BlobFound distinguish a missing physical row from a valid empty
// blob. LogicalBytes are the uncompressed bytes to which BlobHash and
// Commitment apply.
type LedgerContent struct {
	Found          bool
	ID             ID
	ResidentID     ID
	Class          string
	ErasureState   string
	BlobFound      bool
	LogicalBytes   []byte
	BlobHash       *Digest
	Commitment     Digest
	CommitmentSalt *ContentSalt
}

// LedgerRecord is the adapter-neutral immutable material required to verify a
// resident event hash chain and its content reference. Envelope must be the
// exact stored canonical bytes.
type LedgerRecord struct {
	EventID           ID
	ResidentID        ID
	Seq               Seq
	ContentID         ID
	PayloadCommitment Digest
	Content           LedgerContent
	PrevEventHash     *Digest
	EventHash         Digest
	Envelope          CanonicalJSON
}

// LedgerSource streams every resident-owned content object and then supports
// the event ledger in ascending resident Seq order. An SQLite adapter should
// implement both walks with read cursors rather than loading them into memory.
type LedgerSource interface {
	WalkResidentContents(ctx context.Context, residentID ID, visit func(LedgerContent) error) error
	WalkResidentEvents(ctx context.Context, residentID ID, visit func(LedgerRecord) error) error
}

// EnvelopeValidator can enforce schema-to-envelope field equality in the
// storage adapter without coupling this package to physical rows.
type EnvelopeValidator interface {
	ValidateLedgerEnvelope(record LedgerRecord) error
}

type LedgerVerifier struct {
	EnvelopeValidator EnvelopeValidator
}

type LedgerVerification struct {
	ResidentID   ID
	ContentCount Count
	EventCount   Count
	FirstSeq     Seq
	LastSeq      Seq
	HeadHash     *Digest
}

func (verifier LedgerVerifier) Verify(ctx context.Context, source LedgerSource, residentID ID) (LedgerVerification, error) {
	if ctx == nil {
		return LedgerVerification{}, fmt.Errorf("canonical: nil ledger verification context")
	}
	if source == nil {
		return LedgerVerification{}, fmt.Errorf("canonical: nil ledger source")
	}
	if err := residentID.Validate(); err != nil {
		return LedgerVerification{}, err
	}

	report := LedgerVerification{ResidentID: residentID}
	if err := source.WalkResidentContents(ctx, residentID, func(content LedgerContent) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := verifyContentObject(content, residentID); err != nil {
			return contentError(content, "%v", err)
		}
		if report.ContentCount.Int64() == maxInt64 {
			return contentError(content, "content count overflow")
		}
		report.ContentCount++
		return nil
	}); err != nil {
		return LedgerVerification{}, err
	}
	var previousSeq Seq
	var previousHash Digest
	var seen bool
	err := source.WalkResidentEvents(ctx, residentID, func(record LedgerRecord) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := record.EventID.Validate(); err != nil {
			return ledgerError(record, "invalid event ID: %v", err)
		}
		if record.ResidentID != residentID {
			return ledgerError(record, "resident mismatch: got %s want %s", record.ResidentID, residentID)
		}
		if err := record.Seq.Validate(); err != nil {
			return ledgerError(record, "invalid sequence: %v", err)
		}
		if !seen {
			if record.PrevEventHash != nil {
				return ledgerError(record, "first event has prev_event_hash")
			}
			report.FirstSeq = record.Seq
		} else {
			if record.Seq <= previousSeq {
				return ledgerError(record, "sequence is not strictly increasing after %s", previousSeq)
			}
			if record.PrevEventHash == nil || *record.PrevEventHash != previousHash {
				return ledgerError(record, "previous hash does not match prior event")
			}
		}
		if err := verifyLedgerContent(record); err != nil {
			return ledgerError(record, "content validation: %v", err)
		}
		if verifier.EnvelopeValidator != nil {
			if err := verifier.EnvelopeValidator.ValidateLedgerEnvelope(record); err != nil {
				return ledgerError(record, "envelope validation: %v", err)
			}
		}
		actual, err := HashEvent(record.Envelope)
		if err != nil {
			return ledgerError(record, "hash envelope: %v", err)
		}
		if actual != record.EventHash {
			return ledgerError(record, "event hash mismatch: got %s want %s", record.EventHash, actual)
		}

		seen = true
		previousSeq = record.Seq
		previousHash = record.EventHash
		report.LastSeq = record.Seq
		if report.EventCount.Int64() == maxInt64 {
			return ledgerError(record, "event count overflow")
		}
		report.EventCount++
		head := record.EventHash
		report.HeadHash = &head
		return nil
	})
	if err != nil {
		return LedgerVerification{}, err
	}
	return report, nil
}

func verifyLedgerContent(record LedgerRecord) error {
	if err := record.ContentID.Validate(); err != nil {
		return fmt.Errorf("invalid event content ID: %v", err)
	}
	content := record.Content
	if !content.Found {
		return fmt.Errorf("referenced content object is missing")
	}
	if err := content.ID.Validate(); err != nil {
		return fmt.Errorf("invalid content object ID: %v", err)
	}
	if content.ID != record.ContentID {
		return fmt.Errorf("content reference mismatch: event=%s object=%s", record.ContentID, content.ID)
	}
	if record.PayloadCommitment != content.Commitment {
		return fmt.Errorf("payload commitment does not match content object")
	}
	return verifyContentObject(content, record.ResidentID)
}

func verifyContentObject(content LedgerContent, residentID ID) error {
	if !content.Found {
		return fmt.Errorf("content object is missing")
	}
	if err := content.ID.Validate(); err != nil {
		return fmt.Errorf("invalid content object ID: %v", err)
	}
	if err := content.ResidentID.Validate(); err != nil {
		return fmt.Errorf("invalid content owner resident ID: %v", err)
	}
	if content.ResidentID != residentID {
		return fmt.Errorf("content owner resident mismatch: got %s want %s", content.ResidentID, residentID)
	}
	if _, err := CommitContent(content.Class, ContentSalt{}, nil); err != nil {
		return fmt.Errorf("invalid content class: %v", err)
	}
	switch content.ErasureState {
	case ContentErasurePresent:
		if !content.BlobFound {
			return fmt.Errorf("present content blob is missing")
		}
		if content.BlobHash == nil {
			return fmt.Errorf("present content has no blob hash")
		}
		if content.CommitmentSalt == nil {
			return fmt.Errorf("present content has no commitment salt")
		}
		if actual := HashBlob(content.LogicalBytes); actual != *content.BlobHash {
			return fmt.Errorf("blob hash mismatch: got %s want %s", *content.BlobHash, actual)
		}
		if err := VerifyContentCommitment(content.Class, *content.CommitmentSalt, content.LogicalBytes, content.Commitment); err != nil {
			return err
		}
	case ContentErasureErased:
		if content.BlobFound || content.BlobHash != nil || content.CommitmentSalt != nil || len(content.LogicalBytes) != 0 {
			return fmt.Errorf("erased content retains blob or commitment salt material")
		}
	default:
		return fmt.Errorf("invalid erasure state %q", content.ErasureState)
	}
	return nil
}

func contentError(content LedgerContent, format string, args ...any) error {
	detail := fmt.Sprintf(format, args...)
	return fmt.Errorf("%w: content=%s: %s", ErrLedgerViolation, content.ID, detail)
}

func ledgerError(record LedgerRecord, format string, args ...any) error {
	detail := fmt.Sprintf(format, args...)
	return fmt.Errorf("%w: event=%s seq=%s: %s", ErrLedgerViolation, record.EventID, record.Seq, detail)
}
