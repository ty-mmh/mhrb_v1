package sqlite

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"mahoroba.local/mahoroba/internal/canonical"
	"mahoroba.local/mahoroba/internal/domain"
	"mahoroba.local/mahoroba/internal/memory"
)

func (u *canonicalUoW) CreateMemoryReplacementIntent(
	ctx context.Context,
	value domain.CreateMemoryReplacementIntent,
) (domain.MemoryReplacementIntentResult, error) {
	if err := u.requireResidentScope(value.ResidentID); err != nil {
		return domain.MemoryReplacementIntentResult{}, err
	}
	if err := u.requireActiveResident(ctx, value.ResidentID); err != nil {
		return domain.MemoryReplacementIntentResult{}, err
	}
	if err := u.requireOwnerHuman(ctx, value.ResidentID, value.OwnerPrincipalID); err != nil {
		return domain.MemoryReplacementIntentResult{}, err
	}

	// An exact marker is a successful logical retry. Authorization and active
	// resident state are rechecked above, while later maturation/status changes
	// do not turn an already durable request into a conflicting retry.
	var existingCount int
	var existingRelationRaw, existingOldRaw sql.NullString
	err := u.tx.QueryRowContext(ctx, `SELECT COUNT(*), MIN(claim_relation_id), MIN(to_claim_id)
		FROM claim_relations
		WHERE from_claim_id = ? AND relation_type = ? AND reason_code = ?
		`, value.NewClaimID.String(), string(memory.RelationContradicts),
		string(memory.RelationReasonReplacementIntent)).Scan(&existingCount, &existingRelationRaw, &existingOldRaw)
	if err != nil {
		return domain.MemoryReplacementIntentResult{}, fmt.Errorf("sqlite: inspect replacement intent: %w", err)
	}
	if existingCount > 1 {
		return domain.MemoryReplacementIntentResult{}, errors.New("sqlite: replacement claim has multiple replacement intents")
	}
	if existingCount == 1 {
		if !existingRelationRaw.Valid || !existingOldRaw.Valid {
			return domain.MemoryReplacementIntentResult{}, errors.New("sqlite: replacement intent identity is incomplete")
		}
		if existingOldRaw.String != value.OldClaimID.String() {
			return domain.MemoryReplacementIntentResult{}, errors.New("sqlite: replacement claim already has another replacement intent")
		}
		existingRelationID, parseErr := canonical.ParseID(existingRelationRaw.String)
		if parseErr != nil {
			return domain.MemoryReplacementIntentResult{}, parseErr
		}
		return domain.MemoryReplacementIntentResult{
			RelationID: existingRelationID, NewClaimID: value.NewClaimID,
			OldClaimID: value.OldClaimID, Created: false,
		}, canonical.ErrNoMutation
	}

	newClaim, err := u.loadClaimForMutation(ctx, value.ResidentID, value.NewClaimID)
	if err != nil {
		return domain.MemoryReplacementIntentResult{}, err
	}
	oldClaim, err := u.loadClaimForMutation(ctx, value.ResidentID, value.OldClaimID)
	if err != nil {
		return domain.MemoryReplacementIntentResult{}, err
	}
	if newClaim.Kind != memory.ClaimKindDirect || oldClaim.Kind != memory.ClaimKindDirect {
		return domain.MemoryReplacementIntentResult{}, errors.New("sqlite: replacement intent requires two direct claims")
	}
	if newClaim.SubjectID != oldClaim.SubjectID || newClaim.PerspectiveID != oldClaim.PerspectiveID {
		return domain.MemoryReplacementIntentResult{}, errors.New("sqlite: replacement intent claim identity mismatch")
	}
	newStatus, err := u.currentClaimStatus(ctx, newClaim.ID)
	if err != nil {
		return domain.MemoryReplacementIntentResult{}, err
	}
	oldStatus, err := u.currentClaimStatus(ctx, oldClaim.ID)
	if err != nil {
		return domain.MemoryReplacementIntentResult{}, err
	}
	if newStatus != memory.StatusActive || oldStatus != memory.StatusActive {
		return domain.MemoryReplacementIntentResult{}, errors.New("sqlite: replacement intent requires active claims")
	}
	newStage, err := u.currentClaimStage(ctx, newClaim.ID)
	if err != nil {
		return domain.MemoryReplacementIntentResult{}, err
	}
	if newStage == memory.StageSettled {
		return domain.MemoryReplacementIntentResult{}, errors.New("sqlite: prior-settled replacement requires human decision")
	}
	if newStage != memory.StageSediment {
		return domain.MemoryReplacementIntentResult{}, errors.New("sqlite: replacement intent requires a sediment replacement claim")
	}
	oldStage, err := u.currentClaimStage(ctx, oldClaim.ID)
	if err != nil {
		return domain.MemoryReplacementIntentResult{}, err
	}
	if oldStage != memory.StageSettled {
		return domain.MemoryReplacementIntentResult{}, errors.New("sqlite: replacement intent target must be settled")
	}

	// A pre-existing non-intent contradiction for the same directed pair owns
	// the schema's unique (from,to,type) identity and must not be silently
	// reinterpreted as owner-Admin intent.
	var conflictingReason string
	err = u.tx.QueryRowContext(ctx, `SELECT reason_code FROM claim_relations
		WHERE from_claim_id = ? AND to_claim_id = ? AND relation_type = ?`,
		value.NewClaimID.String(), value.OldClaimID.String(), string(memory.RelationContradicts)).Scan(&conflictingReason)
	if err == nil {
		return domain.MemoryReplacementIntentResult{}, fmt.Errorf(
			"sqlite: directed contradiction already exists with reason %q", conflictingReason,
		)
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return domain.MemoryReplacementIntentResult{}, fmt.Errorf("sqlite: inspect directed contradiction: %w", err)
	}

	m := u.metadata
	if _, err := u.tx.ExecContext(ctx, `INSERT INTO claim_relations(
		claim_relation_id, canonical_commit_id, from_claim_id, to_claim_id,
		relation_type, reason_code, reason_content_id, generation_run_id,
		occurred_at, occurred_tz, recorded_at, recorded_tz
	) VALUES (?, ?, ?, ?, ?, ?, NULL, NULL, ?, ?, ?, ?)`,
		value.RelationID.String(), m.CommitID.String(), value.NewClaimID.String(), value.OldClaimID.String(),
		string(memory.RelationContradicts), string(memory.RelationReasonReplacementIntent),
		m.CommittedAt.UnixMicro(), m.CommittedTZ.String(), m.CommittedAt.UnixMicro(), m.CommittedTZ.String(),
	); err != nil {
		return domain.MemoryReplacementIntentResult{}, fmt.Errorf("sqlite: insert replacement intent: %w", err)
	}
	return domain.MemoryReplacementIntentResult{
		RelationID: value.RelationID, NewClaimID: value.NewClaimID,
		OldClaimID: value.OldClaimID, Created: true,
	}, nil
}
