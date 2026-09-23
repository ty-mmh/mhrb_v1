package testsupport

import (
	"bytes"
	"context"
	"testing"
	"time"

	"mahoroba.local/mahoroba/internal/canonical"
)

func TestCanonicalFixtureUsesWriterAndVerifiesLedger(t *testing.T) {
	clock := NewManualClock(time.Unix(1_700_000_000, 0))
	fixture, err := OpenCanonicalFixture(context.Background(), clock, bytes.NewReader(bytes.Repeat([]byte{0x22}, 10)))
	if err != nil {
		t.Fatal(err)
	}
	defer fixture.Writer.Close(context.Background())

	residentID, err := fixture.NewID()
	if err != nil {
		t.Fatal(err)
	}
	for _, text := range []string{"first", "second"} {
		envelope, err := canonical.MarshalCanonical(map[string]any{"text": text})
		if err != nil {
			t.Fatal(err)
		}
		if _, _, err := fixture.AppendEvent(context.Background(), residentID, envelope); err != nil {
			t.Fatal(err)
		}
	}

	commits := fixture.Backend.Commits()
	if len(commits) != 2 {
		t.Fatalf("commits = %d", len(commits))
	}
	if commits[0].CommitSeq.Int64() != 1 || commits[1].CommitSeq.Int64() != 2 {
		t.Fatalf("commit sequences = %s, %s", commits[0].CommitSeq, commits[1].CommitSeq)
	}
	if commits[1].CommittedAt != commits[0].CommittedAt+1 {
		t.Fatalf("ledger time did not clamp by one microsecond: %d, %d", commits[0].CommittedAt, commits[1].CommittedAt)
	}
	for _, commit := range commits {
		scopeResident, ok := commit.Scope.ResidentID()
		if !ok || scopeResident != residentID {
			t.Fatalf("commit scope = %v, %v", scopeResident, ok)
		}
	}

	report, err := (canonical.LedgerVerifier{EnvelopeValidator: fixture.Backend}).Verify(context.Background(), fixture.Backend, residentID)
	if err != nil {
		t.Fatal(err)
	}
	if report.EventCount.Int64() != 2 || report.FirstSeq.Int64() != 1 || report.LastSeq.Int64() != 2 {
		t.Fatalf("ledger report = %+v", report)
	}

	events := fixture.Backend.Events(residentID)
	if len(events) != 2 {
		t.Fatalf("events = %d", len(events))
	}
	first := events[0]
	if !first.Content.Found || !first.Content.BlobFound || first.Content.ID != first.ContentID ||
		first.Content.ResidentID != residentID || first.Content.Commitment != first.PayloadCommitment {
		t.Fatalf("fixture content material = %+v", first.Content)
	}
	if len(first.Content.LogicalBytes) == 0 || first.Content.BlobHash == nil || first.Content.CommitmentSalt == nil {
		t.Fatalf("fixture present content lacks verification material: %+v", first.Content)
	}

	// Events returns deep clones so tests cannot mutate the backend through blob
	// bytes or pointer-backed verification material.
	originalByte := first.Content.LogicalBytes[0]
	first.Content.LogicalBytes[0] ^= 0xff
	*first.Content.BlobHash = canonical.HashBlob([]byte("tampered"))
	*first.Content.CommitmentSalt = canonical.ContentSalt{}
	fresh := fixture.Backend.Events(residentID)[0]
	if fresh.Content.LogicalBytes[0] != originalByte ||
		*fresh.Content.BlobHash == *first.Content.BlobHash ||
		*fresh.Content.CommitmentSalt == *first.Content.CommitmentSalt {
		t.Fatal("fixture content clone aliases backend verification material")
	}

	tamperedEnvelopeReference := fresh
	tamperedEnvelopeReference.PayloadCommitment = canonical.HashBlob([]byte("wrong commitment"))
	if err := fixture.Backend.ValidateLedgerEnvelope(tamperedEnvelopeReference); err == nil {
		t.Fatal("fixture envelope validator accepted a payload commitment mismatch")
	}
}
