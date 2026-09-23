package sqlite

import (
	"context"
	"errors"
	"testing"

	"mahoroba.local/mahoroba/internal/canonical"
	"mahoroba.local/mahoroba/internal/domain"
	"mahoroba.local/mahoroba/internal/memory"
)

func replacementIntentValue(
	t *testing.T,
	fixture *alignmentMutationFixture,
	newClaimID, oldClaimID canonical.ID,
) domain.CreateMemoryReplacementIntent {
	t.Helper()
	return domain.CreateMemoryReplacementIntent{
		ResidentID: fixture.residentID, NewClaimID: newClaimID, OldClaimID: oldClaimID,
		RelationID: fixture.newID(t), OwnerPrincipalID: fixture.ownerPrincipal,
	}
}

func TestMemoryReplacementIntentPersistsDirectedOwnerMarkerOnly(t *testing.T) {
	fixture := newAlignmentMutationFixture(t, true)
	oldClaimID := fixture.seedSettledDirectClaim(t)
	value := replacementIntentValue(t, fixture, fixture.directID, oldClaimID)
	uow, metadata := fixture.beginUoW(t)
	result, err := uow.CreateMemoryReplacementIntent(context.Background(), value)
	if err != nil {
		_ = uow.tx.Rollback()
		t.Fatal(err)
	}
	if !result.Created || result.RelationID != value.RelationID ||
		result.NewClaimID != fixture.directID || result.OldClaimID != oldClaimID {
		_ = uow.tx.Rollback()
		t.Fatalf("replacement intent result = %+v", result)
	}
	if err := uow.tx.Commit(); err != nil {
		t.Fatal(err)
	}
	var commitID, fromClaim, toClaim, relationType, reason string
	var generationRun any
	if err := fixture.semantic.db.QueryRow(`SELECT canonical_commit_id, from_claim_id, to_claim_id,
		relation_type, reason_code, generation_run_id FROM claim_relations
		WHERE claim_relation_id = ?`, value.RelationID.String()).Scan(
		&commitID, &fromClaim, &toClaim, &relationType, &reason, &generationRun,
	); err != nil {
		t.Fatal(err)
	}
	if commitID != metadata.CommitID.String() || fromClaim != fixture.directID.String() ||
		toClaim != oldClaimID.String() || relationType != string(memory.RelationContradicts) ||
		reason != string(memory.RelationReasonReplacementIntent) || generationRun != nil {
		t.Fatalf("persisted replacement intent = %s %s->%s %s/%s run=%v",
			commitID, fromClaim, toClaim, relationType, reason, generationRun)
	}
	var statuses int
	if err := fixture.semantic.db.QueryRow(`SELECT COUNT(*) FROM claim_status_transitions
		WHERE claim_id IN (?, ?)`, fixture.directID.String(), oldClaimID.String()).Scan(&statuses); err != nil {
		t.Fatal(err)
	}
	if statuses != 0 {
		t.Fatalf("replacement intent mutated claim status: %d transitions", statuses)
	}
}

func TestMemoryReplacementIntentIsIdempotentForSamePairAndUniquePerReplacement(t *testing.T) {
	fixture := newAlignmentMutationFixture(t, true)
	oldClaimID := fixture.seedSettledDirectClaim(t)
	value := replacementIntentValue(t, fixture, fixture.directID, oldClaimID)
	first, _ := fixture.beginUoW(t)
	if _, err := first.CreateMemoryReplacementIntent(context.Background(), value); err != nil {
		_ = first.tx.Rollback()
		t.Fatal(err)
	}
	if err := first.tx.Commit(); err != nil {
		t.Fatal(err)
	}

	retry := value
	retry.RelationID = fixture.newID(t)
	second, _ := fixture.beginUoW(t)
	result, err := second.CreateMemoryReplacementIntent(context.Background(), retry)
	if err != canonical.ErrNoMutation {
		_ = second.tx.Rollback()
		t.Fatalf("idempotent retry error = %v", err)
	}
	if result.Created || result.RelationID != value.RelationID {
		_ = second.tx.Rollback()
		t.Fatalf("idempotent retry result = %+v", result)
	}
	_ = second.tx.Rollback()

	otherOldClaimID := fixture.seedSettledDirectClaim(t)
	conflict := replacementIntentValue(t, fixture, fixture.directID, otherOldClaimID)
	third, _ := fixture.beginUoW(t)
	if _, err := third.CreateMemoryReplacementIntent(context.Background(), conflict); err == nil {
		_ = third.tx.Rollback()
		t.Fatal("a second replacement-intent target was accepted")
	}
	_ = third.tx.Rollback()
	var markers int
	if err := fixture.semantic.db.QueryRow(`SELECT COUNT(*) FROM claim_relations
		WHERE from_claim_id = ? AND relation_type = ? AND reason_code = ?`, fixture.directID.String(),
		string(memory.RelationContradicts), string(memory.RelationReasonReplacementIntent)).Scan(&markers); err != nil {
		t.Fatal(err)
	}
	if markers != 1 {
		t.Fatalf("replacement intent markers = %d", markers)
	}
}

func TestMemoryReplacementIntentFailsClosedOnClaimAndAuthorityGates(t *testing.T) {
	t.Run("prior-settled replacement is human-only", func(t *testing.T) {
		fixture := newAlignmentMutationFixture(t, true)
		newClaimID, _ := fixture.seedReplacementCandidate(t, memory.StageSettled)
		oldClaimID := fixture.seedSettledDirectClaim(t)
		value := replacementIntentValue(t, fixture, newClaimID, oldClaimID)
		uow, _ := fixture.beginUoW(t)
		if _, err := uow.CreateMemoryReplacementIntent(context.Background(), value); err == nil {
			_ = uow.tx.Rollback()
			t.Fatal("prior-settled replacement intent was accepted")
		}
		_ = uow.tx.Rollback()
	})

	t.Run("replacement must be sediment", func(t *testing.T) {
		fixture := newAlignmentMutationFixture(t, true)
		newClaimID, _ := fixture.seedReplacementCandidate(t, memory.StageFloating)
		oldClaimID := fixture.seedSettledDirectClaim(t)
		value := replacementIntentValue(t, fixture, newClaimID, oldClaimID)
		uow, _ := fixture.beginUoW(t)
		if _, err := uow.CreateMemoryReplacementIntent(context.Background(), value); err == nil {
			_ = uow.tx.Rollback()
			t.Fatal("floating replacement intent was accepted")
		}
		_ = uow.tx.Rollback()
	})

	t.Run("target must be settled", func(t *testing.T) {
		fixture := newAlignmentMutationFixture(t, true)
		oldClaimID, _ := fixture.seedReplacementCandidate(t, memory.StageSediment)
		value := replacementIntentValue(t, fixture, fixture.directID, oldClaimID)
		uow, _ := fixture.beginUoW(t)
		if _, err := uow.CreateMemoryReplacementIntent(context.Background(), value); err == nil {
			_ = uow.tx.Rollback()
			t.Fatal("non-settled replacement target was accepted")
		}
		_ = uow.tx.Rollback()
	})

	t.Run("owner human is required", func(t *testing.T) {
		fixture := newAlignmentMutationFixture(t, true)
		oldClaimID := fixture.seedSettledDirectClaim(t)
		value := replacementIntentValue(t, fixture, fixture.directID, oldClaimID)
		value.OwnerPrincipalID = fixture.residentPrincipal
		uow, _ := fixture.beginUoW(t)
		if _, err := uow.CreateMemoryReplacementIntent(context.Background(), value); err == nil {
			_ = uow.tx.Rollback()
			t.Fatal("non-owner resident principal created replacement intent")
		}
		_ = uow.tx.Rollback()
	})

	t.Run("both claims must remain active", func(t *testing.T) {
		fixture := newAlignmentMutationFixture(t, true)
		oldClaimID := fixture.seedSettledDirectClaim(t)
		statusUoW, _ := fixture.beginUoW(t)
		if _, err := statusUoW.HumanClaimStatusDecision(context.Background(), domain.HumanClaimStatusDecision{
			ResidentID: fixture.residentID, ClaimID: oldClaimID,
			StatusTransitionID: fixture.newID(t), OwnerPrincipalID: fixture.ownerPrincipal,
			ToStatus: memory.StatusQuarantined, Reason: memory.HumanReasonQuarantine,
		}); err != nil {
			_ = statusUoW.tx.Rollback()
			t.Fatal(err)
		}
		if err := statusUoW.tx.Commit(); err != nil {
			t.Fatal(err)
		}
		value := replacementIntentValue(t, fixture, fixture.directID, oldClaimID)
		uow, _ := fixture.beginUoW(t)
		if _, err := uow.CreateMemoryReplacementIntent(context.Background(), value); err == nil {
			_ = uow.tx.Rollback()
			t.Fatal("non-active replacement target was accepted")
		}
		_ = uow.tx.Rollback()
	})

	t.Run("resident must remain active", func(t *testing.T) {
		fixture := newAlignmentMutationFixture(t, true)
		oldClaimID := fixture.seedSettledDirectClaim(t)
		lifecycleUoW, metadata := fixture.beginUoW(t)
		if _, err := lifecycleUoW.tx.ExecContext(context.Background(), `INSERT INTO resident_status_transitions(
			resident_status_transition_id, canonical_commit_id, resident_id, from_status, to_status,
			actor_principal_id, reason_code, reason_content_id, occurred_at, occurred_tz,
			recorded_at, recorded_tz
		) VALUES (?, ?, ?, 'active', 'archived', ?, 'test_archive', NULL, ?, ?, ?, ?)`,
			fixture.newID(t).String(), metadata.CommitID.String(), fixture.residentID.String(),
			fixture.ownerPrincipal.String(), metadata.CommittedAt.UnixMicro(), metadata.CommittedTZ.String(),
			metadata.CommittedAt.UnixMicro(), metadata.CommittedTZ.String(),
		); err != nil {
			_ = lifecycleUoW.tx.Rollback()
			t.Fatal(err)
		}
		if err := lifecycleUoW.tx.Commit(); err != nil {
			t.Fatal(err)
		}
		value := replacementIntentValue(t, fixture, fixture.directID, oldClaimID)
		uow, _ := fixture.beginUoW(t)
		if _, err := uow.CreateMemoryReplacementIntent(context.Background(), value); err == nil {
			_ = uow.tx.Rollback()
			t.Fatal("archived resident created replacement intent")
		}
		_ = uow.tx.Rollback()
	})

	t.Run("identity must match", func(t *testing.T) {
		fixture := newAlignmentMutationFixture(t, true)
		seeds := make([]seededClaimEvidence, 0, 4)
		for range 4 {
			eventID := fixture.seedEvent(t, memory.EventSelfTalk, fixture.residentPrincipal, memory.TrustTrusted)
			seeds = append(seeds, trustedEvidence(eventID, memory.EventSelfTalk,
				fixture.residentPrincipal, memory.PolaritySupport))
		}
		oldClaimID, _ := fixture.seedClaim(t, memory.ClaimKindDirect, fixture.ownerPrincipal,
			fixture.residentPrincipal, seeds, memory.StageSettled)
		value := replacementIntentValue(t, fixture, fixture.directID, oldClaimID)
		uow, _ := fixture.beginUoW(t)
		_, err := uow.CreateMemoryReplacementIntent(context.Background(), value)
		if err == nil || errors.Is(err, canonical.ErrNoMutation) {
			_ = uow.tx.Rollback()
			t.Fatal("identity-mismatched replacement target was accepted")
		}
		_ = uow.tx.Rollback()
	})
}
