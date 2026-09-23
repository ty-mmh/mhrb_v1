package restore

import (
	"context"
	"testing"

	"mahoroba.local/mahoroba/internal/canonical"
)

type cov2ReadOnlyLedgerRepository struct {
	residentIDs []canonical.ID
	walked      []canonical.ID
}

func (repository *cov2ReadOnlyLedgerRepository) ListResidentIDs(context.Context) ([]canonical.ID, error) {
	return append([]canonical.ID(nil), repository.residentIDs...), nil
}

func (repository *cov2ReadOnlyLedgerRepository) WalkResidentContents(
	_ context.Context,
	residentID canonical.ID,
	_ func(canonical.LedgerContent) error,
) error {
	repository.walked = append(repository.walked, residentID)
	return nil
}

func (*cov2ReadOnlyLedgerRepository) WalkResidentEvents(
	context.Context,
	canonical.ID,
	func(canonical.LedgerRecord) error,
) error {
	return nil
}

func (*cov2ReadOnlyLedgerRepository) ValidateLedgerEnvelope(canonical.LedgerRecord) error {
	return nil
}

func TestCOV2RestoreReadOnlyVerificationEnumeratesResidentIDsWithoutSnapshots(t *testing.T) {
	residentID, err := canonical.ParseID("01J00000000000000000000997")
	if err != nil {
		t.Fatal(err)
	}
	repository := &cov2ReadOnlyLedgerRepository{residentIDs: []canonical.ID{residentID}}
	if err := verifyCanonical(context.Background(), repository); err != nil {
		t.Fatal(err)
	}
	if len(repository.walked) != 1 || repository.walked[0] != residentID {
		t.Fatalf("verified resident IDs = %v", repository.walked)
	}
}
