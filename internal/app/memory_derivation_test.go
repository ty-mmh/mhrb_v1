package app

import (
	"context"
	"testing"

	"mahoroba.local/mahoroba/internal/domain"
)

func TestMemoryDerivedClaimServiceRejectsInactiveResidentAndNonOwnerBeforeWriter(t *testing.T) {
	ids := newContextTestIDs(t)
	residentID := ids.next(t)
	ownerID := ids.next(t)
	nonOwnerID := ids.next(t)
	value := domain.LandDerivedClaim{
		Attempt: domain.Attempt{ResidentID: residentID}, OwnerPrincipalID: ownerID,
	}

	t.Run("inactive resident", func(t *testing.T) {
		application := contextApplication(t, &contextRepository{resident: domain.ResidentSnapshot{
			ResidentID: residentID, OwnerPrincipalID: ownerID, Status: "archived",
		}}, 4096)
		if _, err := application.LandMemoryClaimAbstraction(context.Background(), value); err == nil {
			t.Fatal("inactive resident reached the Canonical Writer")
		}
	})

	t.Run("non owner", func(t *testing.T) {
		application := contextApplication(t, &contextRepository{resident: domain.ResidentSnapshot{
			ResidentID: residentID, OwnerPrincipalID: nonOwnerID, Status: "active",
		}}, 4096)
		if _, err := application.LandMemoryClaimDifferentiation(context.Background(), value); err == nil {
			t.Fatal("non-owner derived claim reached the Canonical Writer")
		}
	})
}
