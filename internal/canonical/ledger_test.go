package canonical

import (
	"context"
	"errors"
	"testing"
)

type sliceLedgerSource []LedgerRecord

func (source sliceLedgerSource) WalkResidentContents(_ context.Context, _ ID, visit func(LedgerContent) error) error {
	seen := make(map[ID]struct{}, len(source))
	for _, record := range source {
		if _, exists := seen[record.Content.ID]; exists {
			continue
		}
		seen[record.Content.ID] = struct{}{}
		if err := visit(record.Content); err != nil {
			return err
		}
	}
	return nil
}

func (source sliceLedgerSource) WalkResidentEvents(_ context.Context, _ ID, visit func(LedgerRecord) error) error {
	for _, record := range source {
		if err := visit(record); err != nil {
			return err
		}
	}
	return nil
}

type contentOnlyLedgerSource []LedgerContent

func (source contentOnlyLedgerSource) WalkResidentContents(_ context.Context, _ ID, visit func(LedgerContent) error) error {
	for _, content := range source {
		if err := visit(content); err != nil {
			return err
		}
	}
	return nil
}

func (contentOnlyLedgerSource) WalkResidentEvents(context.Context, ID, func(LedgerRecord) error) error {
	return nil
}

func TestLedgerVerifierAcceptsGapsAndRejectsBrokenChain(t *testing.T) {
	resident, _ := ParseID("01ARZ3NDEKTSV4RRFFQ69G5FAV")
	event1, _ := ParseID("01ARZ3NDEKTSV4RRFFQ69G5FAW")
	event2, _ := ParseID("01ARZ3NDEKTSV4RRFFQ69G5FAX")
	seq1, _ := NewSeq(1)
	seq3, _ := NewSeq(3)
	record1 := newLedgerRecord(t, event1, resident, seq1, nil, []byte("one"))
	record2 := newLedgerRecord(t, event2, resident, seq3, &record1.EventHash, []byte("two"))
	source := sliceLedgerSource{
		record1,
		record2,
	}
	report, err := (LedgerVerifier{}).Verify(context.Background(), source, resident)
	if err != nil {
		t.Fatal(err)
	}
	if report.ContentCount.Int64() != 2 || report.EventCount.Int64() != 2 || report.LastSeq != seq3 {
		t.Fatalf("report = %+v", report)
	}

	wrong := HashBlob([]byte("wrong"))
	broken := append(sliceLedgerSource(nil), source...)
	broken[1].PrevEventHash = &wrong
	if _, err := (LedgerVerifier{}).Verify(context.Background(), broken, resident); !errors.Is(err, ErrLedgerViolation) {
		t.Fatalf("broken chain error = %v", err)
	}
}

func TestLedgerVerifierRejectsPresentContentTampering(t *testing.T) {
	resident, _ := ParseID("01ARZ3NDEKTSV4RRFFQ69G5FAV")
	eventID, _ := ParseID("01ARZ3NDEKTSV4RRFFQ69G5FAW")
	seq, _ := NewSeq(1)

	tests := []struct {
		name   string
		tamper func(*LedgerRecord)
	}{
		{
			name: "logical blob bytes",
			tamper: func(record *LedgerRecord) {
				record.Content.LogicalBytes[0] ^= 0xff
			},
		},
		{
			name: "blob hash",
			tamper: func(record *LedgerRecord) {
				wrong := HashBlob([]byte("wrong blob"))
				record.Content.BlobHash = &wrong
			},
		},
		{
			name: "content commitment",
			tamper: func(record *LedgerRecord) {
				wrong := HashBlob([]byte("wrong commitment"))
				record.Content.Commitment = wrong
				record.PayloadCommitment = wrong
			},
		},
		{
			name: "commitment salt",
			tamper: func(record *LedgerRecord) {
				wrong, _ := ContentSaltFromBytes(make([]byte, ContentCommitmentSaltLen))
				record.Content.CommitmentSalt = &wrong
			},
		},
		{
			name: "content reference",
			tamper: func(record *LedgerRecord) {
				wrong, _ := ParseID("01ARZ3NDEKTSV4RRFFQ69G5FAX")
				record.Content.ID = wrong
			},
		},
		{
			name: "missing blob row",
			tamper: func(record *LedgerRecord) {
				record.Content.BlobFound = false
				record.Content.LogicalBytes = nil
			},
		},
	}

	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			record := newLedgerRecord(t, eventID, resident, seq, nil, []byte("payload"))
			testCase.tamper(&record)
			_, err := (LedgerVerifier{}).Verify(context.Background(), sliceLedgerSource{record}, resident)
			if !errors.Is(err, ErrLedgerViolation) {
				t.Fatalf("tampered content error = %v", err)
			}
		})
	}
}

func TestLedgerVerifierChecksContentWithoutEvents(t *testing.T) {
	resident, _ := ParseID("01ARZ3NDEKTSV4RRFFQ69G5FAV")
	contentID, _ := ParseID("01ARZ3NDEKTSV4RRFFQ69G5FAW")
	seq, _ := NewSeq(1)
	content := newLedgerRecord(t, contentID, resident, seq, nil, []byte("active principles")).Content

	report, err := (LedgerVerifier{}).Verify(context.Background(), contentOnlyLedgerSource{content}, resident)
	if err != nil {
		t.Fatal(err)
	}
	if report.ContentCount.Int64() != 1 || report.EventCount.Int64() != 0 {
		t.Fatalf("eventless report = %+v", report)
	}
	content.LogicalBytes[0] ^= 0xff
	if _, err := (LedgerVerifier{}).Verify(context.Background(), contentOnlyLedgerSource{content}, resident); !errors.Is(err, ErrLedgerViolation) {
		t.Fatalf("eventless content tamper error = %v", err)
	}
}

func TestLedgerVerifierAcceptsErasedContentWithoutBlobMaterial(t *testing.T) {
	resident, _ := ParseID("01ARZ3NDEKTSV4RRFFQ69G5FAV")
	eventID, _ := ParseID("01ARZ3NDEKTSV4RRFFQ69G5FAW")
	seq, _ := NewSeq(1)
	record := newLedgerRecord(t, eventID, resident, seq, nil, []byte("payload later erased"))
	record.Content.ErasureState = ContentErasureErased
	record.Content.BlobFound = false
	record.Content.LogicalBytes = nil
	record.Content.BlobHash = nil
	record.Content.CommitmentSalt = nil

	if _, err := (LedgerVerifier{}).Verify(context.Background(), sliceLedgerSource{record}, resident); err != nil {
		t.Fatalf("erased content should retain a verifiable reference and commitment: %v", err)
	}

	record.Content.BlobFound = true
	if _, err := (LedgerVerifier{}).Verify(context.Background(), sliceLedgerSource{record}, resident); !errors.Is(err, ErrLedgerViolation) {
		t.Fatalf("erased content retaining blob material error = %v", err)
	}
}

func newLedgerRecord(t *testing.T, eventID, residentID ID, seq Seq, previous *Digest, logicalBytes []byte) LedgerRecord {
	t.Helper()
	saltBytes := make([]byte, ContentCommitmentSaltLen)
	for i := range saltBytes {
		saltBytes[i] = byte(i + 1)
	}
	salt, err := ContentSaltFromBytes(saltBytes)
	if err != nil {
		t.Fatal(err)
	}
	commitment, err := CommitContent("event_payload", salt, logicalBytes)
	if err != nil {
		t.Fatal(err)
	}
	envelope, err := MarshalCanonical(map[string]any{
		"content_id":         eventID,
		"event_id":           eventID,
		"payload_commitment": commitment,
		"resident_id":        residentID,
		"seq":                seq,
	})
	if err != nil {
		t.Fatal(err)
	}
	eventHash, err := HashEvent(envelope)
	if err != nil {
		t.Fatal(err)
	}
	blobHash := HashBlob(logicalBytes)
	return LedgerRecord{
		EventID:           eventID,
		ResidentID:        residentID,
		Seq:               seq,
		ContentID:         eventID,
		PayloadCommitment: commitment,
		Content: LedgerContent{
			Found: true, ID: eventID, ResidentID: residentID, Class: "event_payload",
			ErasureState: ContentErasurePresent, BlobFound: true,
			LogicalBytes: append([]byte(nil), logicalBytes...), BlobHash: &blobHash,
			Commitment: commitment, CommitmentSalt: &salt,
		},
		PrevEventHash: previous,
		EventHash:     eventHash,
		Envelope:      envelope,
	}
}
