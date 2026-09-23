package sqlite

import (
	"context"
	"testing"

	"mahoroba.local/mahoroba/internal/canonical"
)

func TestWalkResidentEventsIncludesPresentAndErasedContentMaterial(t *testing.T) {
	fixture, closeFixture := newSemanticFixture(t)
	defer closeFixture()

	residentID, err := canonical.ParseID(fixture.resident["A"])
	if err != nil {
		t.Fatal(err)
	}
	repository := &CanonicalRepository{store: &Store{reader: fixture.db}}

	present := collectLedgerRecords(t, repository, residentID)
	if len(present) != 1 {
		t.Fatalf("present records = %d", len(present))
	}
	content := present[0].Content
	if !content.Found || !content.BlobFound || content.ErasureState != canonical.ContentErasurePresent {
		t.Fatalf("present content material = %+v", content)
	}
	if content.ID != present[0].ContentID || content.ResidentID != residentID ||
		content.BlobHash == nil || content.CommitmentSalt == nil || len(content.LogicalBytes) == 0 {
		t.Fatalf("incomplete present content material = %+v", content)
	}
	if err := repository.ValidateLedgerEnvelope(present[0]); err != nil {
		t.Fatalf("valid reconstructed envelope: %v", err)
	}

	tamperedCommitment := present[0]
	tamperedCommitment.PayloadCommitment = canonical.HashBlob([]byte("wrong payload commitment"))
	if err := repository.ValidateLedgerEnvelope(tamperedCommitment); err == nil {
		t.Fatal("envelope validator accepted a payload commitment mismatch")
	}

	if _, err := fixture.db.Exec(
		"UPDATE content_objects SET erasure_state='erased', blob_hash=NULL, commitment_salt=NULL WHERE content_id=?",
		fixture.content["A"]["event"],
	); err != nil {
		t.Fatal(err)
	}
	erased := collectLedgerRecords(t, repository, residentID)
	if len(erased) != 1 {
		t.Fatalf("erased records = %d", len(erased))
	}
	content = erased[0].Content
	if !content.Found || content.BlobFound || content.ErasureState != canonical.ContentErasureErased ||
		content.BlobHash != nil || content.CommitmentSalt != nil || len(content.LogicalBytes) != 0 {
		t.Fatalf("erased content material = %+v", content)
	}
}

func collectLedgerRecords(t *testing.T, repository *CanonicalRepository, residentID canonical.ID) []canonical.LedgerRecord {
	t.Helper()
	var records []canonical.LedgerRecord
	if err := repository.WalkResidentEvents(context.Background(), residentID, func(record canonical.LedgerRecord) error {
		record.Content.LogicalBytes = append([]byte(nil), record.Content.LogicalBytes...)
		records = append(records, record)
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	return records
}
