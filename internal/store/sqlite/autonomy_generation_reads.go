package sqlite

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"sort"
	"time"

	"mahoroba.local/mahoroba/internal/autonomy"
	"mahoroba.local/mahoroba/internal/canonical"
	"mahoroba.local/mahoroba/internal/domain"
	"mahoroba.local/mahoroba/internal/memory"
	"mahoroba.local/mahoroba/internal/projection"
)

func (r *CanonicalRepository) AutonomousProjectionEvidence(
	ctx context.Context,
	residentID canonical.ID,
	asOf canonical.Instant,
	maxStaleness time.Duration,
) (domain.AutonomousProjectionEvidence, bool, error) {
	if err := residentID.Validate(); err != nil {
		return domain.AutonomousProjectionEvidence{}, false, err
	}
	if asOf == 0 || maxStaleness <= 0 {
		return domain.AutonomousProjectionEvidence{}, false, nil
	}
	tx, err := r.store.reader.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return domain.AutonomousProjectionEvidence{}, false, err
	}
	defer tx.Rollback()
	head, err := autonomyCapturedHead(ctx, tx)
	if err != nil || !head.Exists {
		return domain.AutonomousProjectionEvidence{}, false, err
	}
	claimWatermark, claimExists, err := readProjectionWatermark(ctx, tx, projection.ClaimStatesName, residentID)
	if err != nil || !claimExists {
		return domain.AutonomousProjectionEvidence{}, false, err
	}
	viewWatermark, viewExists, err := readProjectionWatermark(ctx, tx, projection.ClaimViewScopeCurrentName, residentID)
	if err != nil || !viewExists {
		return domain.AutonomousProjectionEvidence{}, false, err
	}
	var activePolicyRaw string
	if err := tx.QueryRowContext(ctx, `SELECT activation.revision_id
		FROM resident_revision_activations activation
		JOIN canonical_commits commit_row ON commit_row.canonical_commit_id = activation.canonical_commit_id
		JOIN resident_revisions revision ON revision.revision_id = activation.revision_id
		WHERE activation.resident_id = ? AND revision.revision_class = 'memory_policy'
		 AND commit_row.commit_seq <= ? ORDER BY commit_row.commit_seq DESC, activation.activation_id DESC LIMIT 1`,
		residentID.String(), head.CommitSeq.Int64()).Scan(&activePolicyRaw); err != nil {
		return domain.AutonomousProjectionEvidence{}, false, err
	}
	activePolicyID, err := canonical.ParseID(activePolicyRaw)
	if err != nil {
		return domain.AutonomousProjectionEvidence{}, false, err
	}
	evidence := domain.AutonomousProjectionEvidence{
		CapturedHead: head.CommitSeq, ClaimStates: claimWatermark, ViewScope: viewWatermark,
	}
	if err := evidence.Validate(residentID, activePolicyID); err != nil ||
		claimWatermark.AsOf > asOf || asOf.Time().Sub(claimWatermark.AsOf.Time()) > maxStaleness {
		return domain.AutonomousProjectionEvidence{}, false, nil
	}
	if err := tx.Commit(); err != nil {
		return domain.AutonomousProjectionEvidence{}, false, err
	}
	return evidence, true, nil
}

// AutonomousProjectionAvailable validates the live Projection artifacts used
// by an already-durable autonomous run. Their source head may trail Canonical
// by the run/outcome commits themselves, but the body, versions, dependency
// set, and as-of freshness must still be intact.
func (r *CanonicalRepository) AutonomousProjectionAvailable(
	ctx context.Context,
	residentID canonical.ID,
	asOf canonical.Instant,
	maxStaleness time.Duration,
) (bool, error) {
	if err := residentID.Validate(); err != nil {
		return false, err
	}
	if asOf == 0 || maxStaleness <= 0 {
		return false, nil
	}
	tx, err := r.store.reader.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return false, err
	}
	defer tx.Rollback()
	head, err := autonomyCapturedHead(ctx, tx)
	if err != nil || !head.Exists {
		return false, err
	}
	claimWatermark, claimExists, err := readProjectionWatermark(ctx, tx, projection.ClaimStatesName, residentID)
	if err != nil || !claimExists {
		return false, err
	}
	viewWatermark, viewExists, err := readProjectionWatermark(ctx, tx, projection.ClaimViewScopeCurrentName, residentID)
	if err != nil || !viewExists {
		return false, err
	}
	var activePolicyRaw string
	if err := tx.QueryRowContext(ctx, `SELECT activation.revision_id
		FROM resident_revision_activations activation
		JOIN canonical_commits commit_row ON commit_row.canonical_commit_id = activation.canonical_commit_id
		JOIN resident_revisions revision ON revision.revision_id = activation.revision_id
		WHERE activation.resident_id = ? AND revision.revision_class = 'memory_policy'
		 AND commit_row.commit_seq <= ? ORDER BY commit_row.commit_seq DESC, activation.activation_id DESC LIMIT 1`,
		residentID.String(), head.CommitSeq.Int64()).Scan(&activePolicyRaw); err != nil {
		return false, err
	}
	activePolicyID, err := canonical.ParseID(activePolicyRaw)
	if err != nil {
		return false, err
	}
	evidence := domain.AutonomousProjectionEvidence{
		CapturedHead: claimWatermark.SourceCommitSeq, ClaimStates: claimWatermark, ViewScope: viewWatermark,
	}
	available := claimWatermark.SourceCommitSeq <= head.CommitSeq &&
		evidence.Validate(residentID, activePolicyID) == nil && claimWatermark.AsOf <= asOf &&
		asOf.Time().Sub(claimWatermark.AsOf.Time()) <= maxStaleness
	if err := tx.Commit(); err != nil {
		return false, err
	}
	return available, nil
}

// AutonomousInitiativeProjectionEligible adds the target claim's live
// temporal relation to the durable Projection artifact check. A frozen
// trigger cannot authorize provider work after an as-of refresh moves the
// claim beyond its triggering relation.
func (r *CanonicalRepository) AutonomousInitiativeProjectionEligible(
	ctx context.Context,
	residentID canonical.ID,
	trigger autonomy.Trigger,
	asOf canonical.Instant,
	maxStaleness time.Duration,
) (bool, error) {
	if err := residentID.Validate(); err != nil {
		return false, err
	}
	if err := trigger.Validate(); err != nil || !trigger.Kind.IsInitiative() || asOf == 0 || maxStaleness <= 0 {
		return false, err
	}
	tx, err := r.store.reader.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return false, err
	}
	defer tx.Rollback()
	head, err := autonomyCapturedHead(ctx, tx)
	if err != nil || !head.Exists {
		return false, err
	}
	claimWatermark, claimExists, err := readProjectionWatermark(ctx, tx, projection.ClaimStatesName, residentID)
	if err != nil || !claimExists {
		return false, err
	}
	viewWatermark, viewExists, err := readProjectionWatermark(ctx, tx, projection.ClaimViewScopeCurrentName, residentID)
	if err != nil || !viewExists {
		return false, err
	}
	var activePolicyRaw string
	if err := tx.QueryRowContext(ctx, `SELECT activation.revision_id
		FROM resident_revision_activations activation
		JOIN canonical_commits commit_row ON commit_row.canonical_commit_id = activation.canonical_commit_id
		JOIN resident_revisions revision ON revision.revision_id = activation.revision_id
		WHERE activation.resident_id = ? AND revision.revision_class = 'memory_policy'
		 AND commit_row.commit_seq <= ? ORDER BY commit_row.commit_seq DESC, activation.activation_id DESC LIMIT 1`,
		residentID.String(), head.CommitSeq.Int64()).Scan(&activePolicyRaw); err != nil {
		return false, err
	}
	activePolicyID, err := canonical.ParseID(activePolicyRaw)
	if err != nil {
		return false, err
	}
	evidence := domain.AutonomousProjectionEvidence{
		CapturedHead: claimWatermark.SourceCommitSeq, ClaimStates: claimWatermark, ViewScope: viewWatermark,
	}
	if claimWatermark.SourceCommitSeq > head.CommitSeq || evidence.Validate(residentID, activePolicyID) != nil ||
		claimWatermark.AsOf > asOf || asOf.Time().Sub(claimWatermark.AsOf.Time()) > maxStaleness {
		return false, nil
	}
	var relation, scope string
	if err := tx.QueryRowContext(ctx, `SELECT state.temporal_relation, scope.view_scope
		FROM claim_states state JOIN claim_view_scope_current scope
		 ON scope.resident_id = state.resident_id AND scope.claim_id = state.claim_id
		WHERE state.resident_id = ? AND state.claim_id = ?`, residentID.String(), trigger.SourceID.String()).Scan(
		&relation, &scope,
	); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return false, nil
		}
		return false, err
	}
	wantRelation := "current"
	if trigger.Kind == autonomy.TriggerVolatileAging {
		wantRelation = "stale_unknown"
	}
	if err := tx.Commit(); err != nil {
		return false, err
	}
	return relation == wantRelation && scope == "resident_ui", nil
}

func (r *CanonicalRepository) AutonomousClaimContexts(
	ctx context.Context,
	residentID canonical.ID,
	trigger autonomy.Trigger,
) ([]domain.AutonomousClaimContext, error) {
	if err := residentID.Validate(); err != nil {
		return nil, err
	}
	if err := trigger.Validate(); err != nil {
		return nil, err
	}
	var claimIDs []canonical.ID
	switch trigger.Kind {
	case autonomy.TriggerIdle:
		return nil, nil
	case autonomy.TriggerMetaDependencyStatusChange:
		var dependencyRaw string
		if err := r.store.reader.QueryRowContext(ctx, `SELECT dependency.dependency_claim_id
			FROM claim_status_transitions status
			JOIN claim_stage_transition_dependencies dependency ON dependency.dependency_claim_id = status.claim_id
			JOIN claim_stage_transitions stage ON stage.stage_transition_id = dependency.stage_transition_id
			WHERE status.status_transition_id = ? AND stage.claim_id = ?`, trigger.SourceID.String(),
			trigger.RelatedClaimIDs[0].String()).Scan(&dependencyRaw); err != nil {
			return nil, err
		}
		dependencyID, err := canonical.ParseID(dependencyRaw)
		if err != nil {
			return nil, err
		}
		claimIDs = []canonical.ID{trigger.RelatedClaimIDs[0], dependencyID}
	case autonomy.TriggerSettledDirectConflict:
		claimIDs = append([]canonical.ID(nil), trigger.RelatedClaimIDs...)
	case autonomy.TriggerFutureToCurrent, autonomy.TriggerVolatileAging:
		claimIDs = []canonical.ID{trigger.SourceID}
	default:
		return nil, errors.New("sqlite: unsupported autonomous claim-context trigger")
	}
	seen := make(map[canonical.ID]struct{}, len(claimIDs))
	result := make([]domain.AutonomousClaimContext, 0, len(claimIDs))
	for _, claimID := range claimIDs {
		if err := claimID.Validate(); err != nil {
			return nil, err
		}
		if _, duplicate := seen[claimID]; duplicate {
			return nil, errors.New("sqlite: duplicate autonomous claim context")
		}
		seen[claimID] = struct{}{}
		var externalSupport int
		if err := r.store.reader.QueryRowContext(ctx, `SELECT EXISTS(
			SELECT 1 FROM claim_evidence evidence
			JOIN events event ON event.event_id = evidence.event_id
			WHERE evidence.claim_id = ? AND evidence.polarity = 'support'
			  AND event.resident_id = ? AND event.event_type <> 'self_talk'
		)`, claimID.String(), residentID.String()).Scan(&externalSupport); err != nil {
			return nil, err
		}
		if externalSupport == 0 {
			return nil, fmt.Errorf("%w: claim %s has only self-talk support",
				domain.ErrAutonomousContextIneligible, claimID)
		}
		eligible, err := loadEligibleClaimStatement(ctx, r.store.reader, residentID, claimID)
		if err != nil {
			return nil, fmt.Errorf("sqlite: load autonomous claim context %s: %w", claimID, err)
		}
		result = append(result, domain.AutonomousClaimContext{ClaimID: claimID, Statement: string(eligible.Statement)})
	}
	return result, nil
}

func (r *CanonicalRepository) DiscoverReevaluationTriggers(
	ctx context.Context,
	residentID canonical.ID,
	limit int,
) ([]autonomy.Trigger, error) {
	if err := residentID.Validate(); err != nil {
		return nil, err
	}
	if limit < 1 {
		return nil, nil
	}
	type orderedTrigger struct {
		commit  int64
		trigger autonomy.Trigger
	}
	result := make([]orderedTrigger, 0, limit)
	rows, err := r.store.reader.QueryContext(ctx, `SELECT status_commit.commit_seq,
		status.status_transition_id, stage.claim_id
		FROM claim_status_transitions status
		JOIN canonical_commits status_commit ON status_commit.canonical_commit_id = status.canonical_commit_id
		JOIN claim_stage_transition_dependencies dependency ON dependency.dependency_claim_id = status.claim_id
		JOIN canonical_commits dependency_commit ON dependency_commit.canonical_commit_id = dependency.canonical_commit_id
		JOIN claim_stage_transitions stage ON stage.stage_transition_id = dependency.stage_transition_id
		JOIN claims direct ON direct.claim_id = stage.claim_id
		JOIN claims dependency_claim ON dependency_claim.claim_id = dependency.dependency_claim_id
		LEFT JOIN content_objects direct_content ON direct_content.content_id = direct.statement_content_id
		LEFT JOIN content_objects dependency_content ON dependency_content.content_id = dependency_claim.statement_content_id
		WHERE direct.owner_resident_id = ? AND direct.kind = 'direct' AND stage.to_stage = 'settled'
		  AND direct.statement_hash IS NOT NULL
		  AND (direct_content.content_id IS NULL OR direct_content.erasure_state = 'present')
		  AND dependency_claim.statement_hash IS NOT NULL
		  AND (dependency_content.content_id IS NULL OR dependency_content.erasure_state = 'present')
		  AND status_commit.commit_seq > dependency_commit.commit_seq
		  AND EXISTS(SELECT 1 FROM claim_evidence evidence JOIN events event ON event.event_id = evidence.event_id
		      WHERE evidence.claim_id = direct.claim_id AND evidence.polarity = 'support'
		        AND event.resident_id = direct.owner_resident_id AND event.event_type <> 'self_talk')
		  AND EXISTS(SELECT 1 FROM claim_evidence evidence JOIN events event ON event.event_id = evidence.event_id
		      WHERE evidence.claim_id = dependency.dependency_claim_id AND evidence.polarity = 'support'
		        AND event.resident_id = direct.owner_resident_id AND event.event_type <> 'self_talk')
		  AND NOT EXISTS(SELECT 1 FROM generation_runs run
		      WHERE run.resident_id = direct.owner_resident_id
		        AND run.idempotency_key = ('self_talk:v1:meta_dependency_status_change:' ||
		          status.status_transition_id || ':' || stage.claim_id || ':autonomy-policy-v0'))
		ORDER BY status_commit.commit_seq, status.status_transition_id, stage.claim_id
		LIMIT ?`, residentID.String(), limit)
	if err != nil {
		return nil, fmt.Errorf("sqlite: discover dependency reevaluation triggers: %w", err)
	}
	for rows.Next() {
		var commit int64
		var sourceRaw, directRaw string
		if err := rows.Scan(&commit, &sourceRaw, &directRaw); err != nil {
			_ = rows.Close()
			return nil, err
		}
		sourceID, err := canonical.ParseID(sourceRaw)
		if err != nil {
			_ = rows.Close()
			return nil, err
		}
		directID, err := canonical.ParseID(directRaw)
		if err != nil {
			_ = rows.Close()
			return nil, err
		}
		result = append(result, orderedTrigger{commit: commit, trigger: autonomy.Trigger{
			Kind: autonomy.TriggerMetaDependencyStatusChange, SourceID: sourceID,
			RelatedClaimIDs: []canonical.ID{directID},
		}})
	}
	if err := rows.Err(); err != nil {
		_ = rows.Close()
		return nil, err
	}
	if err := rows.Close(); err != nil {
		return nil, err
	}
	rows, err = r.store.reader.QueryContext(ctx, `SELECT relation_commit.commit_seq,
		relation.claim_relation_id, relation.from_claim_id, relation.to_claim_id
		FROM claim_relations relation
		JOIN canonical_commits relation_commit ON relation_commit.canonical_commit_id = relation.canonical_commit_id
		JOIN claims source ON source.claim_id = relation.from_claim_id
		JOIN claims target ON target.claim_id = relation.to_claim_id
		LEFT JOIN content_objects source_content ON source_content.content_id = source.statement_content_id
		LEFT JOIN content_objects target_content ON target_content.content_id = target.statement_content_id
		WHERE relation.relation_type = 'contradicts'
		  AND source.owner_resident_id = ? AND target.owner_resident_id = ?
		  AND source.kind = 'direct' AND target.kind = 'direct'
		  AND source.statement_hash IS NOT NULL AND target.statement_hash IS NOT NULL
		  AND (source_content.content_id IS NULL OR source_content.erasure_state = 'present')
		  AND (target_content.content_id IS NULL OR target_content.erasure_state = 'present')
		  AND (SELECT transition.to_stage FROM claim_stage_transitions transition
		       JOIN canonical_commits commit_row ON commit_row.canonical_commit_id = transition.canonical_commit_id
		       WHERE transition.claim_id = source.claim_id ORDER BY commit_row.commit_seq DESC LIMIT 1) = 'settled'
		  AND (SELECT transition.to_stage FROM claim_stage_transitions transition
		       JOIN canonical_commits commit_row ON commit_row.canonical_commit_id = transition.canonical_commit_id
		       WHERE transition.claim_id = target.claim_id ORDER BY commit_row.commit_seq DESC LIMIT 1) = 'settled'
		  AND COALESCE((SELECT transition.to_status FROM claim_status_transitions transition
		       JOIN canonical_commits commit_row ON commit_row.canonical_commit_id = transition.canonical_commit_id
		       WHERE transition.claim_id = source.claim_id ORDER BY commit_row.commit_seq DESC LIMIT 1), 'active') = 'active'
		  AND COALESCE((SELECT transition.to_status FROM claim_status_transitions transition
		       JOIN canonical_commits commit_row ON commit_row.canonical_commit_id = transition.canonical_commit_id
		       WHERE transition.claim_id = target.claim_id ORDER BY commit_row.commit_seq DESC LIMIT 1), 'active') = 'active'
		  AND EXISTS(SELECT 1 FROM claim_evidence evidence JOIN events event ON event.event_id = evidence.event_id
		      WHERE evidence.claim_id = source.claim_id AND evidence.polarity = 'support'
		        AND event.resident_id = source.owner_resident_id AND event.event_type <> 'self_talk')
		  AND EXISTS(SELECT 1 FROM claim_evidence evidence JOIN events event ON event.event_id = evidence.event_id
		      WHERE evidence.claim_id = target.claim_id AND evidence.polarity = 'support'
		        AND event.resident_id = target.owner_resident_id AND event.event_type <> 'self_talk')
		  AND NOT EXISTS(SELECT 1 FROM generation_runs run
		      WHERE run.resident_id = source.owner_resident_id
		        AND run.idempotency_key = ('self_talk:v1:settled_direct_conflict:' ||
		          relation.claim_relation_id || ':' ||
		          CASE WHEN relation.from_claim_id < relation.to_claim_id
		            THEN relation.from_claim_id ELSE relation.to_claim_id END || ':' ||
		          CASE WHEN relation.from_claim_id < relation.to_claim_id
		            THEN relation.to_claim_id ELSE relation.from_claim_id END ||
		          ':autonomy-policy-v0'))
		ORDER BY relation_commit.commit_seq, relation.claim_relation_id LIMIT ?`,
		residentID.String(), residentID.String(), limit)
	if err != nil {
		return nil, fmt.Errorf("sqlite: discover conflict reevaluation triggers: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var commit int64
		var sourceRaw, fromRaw, toRaw string
		if err := rows.Scan(&commit, &sourceRaw, &fromRaw, &toRaw); err != nil {
			return nil, err
		}
		sourceID, err := canonical.ParseID(sourceRaw)
		if err != nil {
			return nil, err
		}
		fromID, err := canonical.ParseID(fromRaw)
		if err != nil {
			return nil, err
		}
		toID, err := canonical.ParseID(toRaw)
		if err != nil {
			return nil, err
		}
		claims := []canonical.ID{fromID, toID}
		sort.Slice(claims, func(i, j int) bool { return claims[i].String() < claims[j].String() })
		result = append(result, orderedTrigger{commit: commit, trigger: autonomy.Trigger{
			Kind: autonomy.TriggerSettledDirectConflict, SourceID: sourceID, RelatedClaimIDs: claims,
		}})
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	if err := rows.Close(); err != nil {
		return nil, err
	}
	filtered := make([]orderedTrigger, 0, len(result))
	for _, candidate := range result {
		eligible, err := r.AutonomousSelfTalkEventTimeEligible(ctx, residentID, candidate.trigger)
		if err != nil {
			return nil, fmt.Errorf("sqlite: validate reevaluation trigger event-time policy: %w", err)
		}
		if !eligible {
			continue
		}
		if _, err := r.AutonomousClaimContexts(ctx, residentID, candidate.trigger); err != nil {
			if errors.Is(err, domain.ErrClaimSourceIneligible) {
				continue
			}
			return nil, fmt.Errorf("sqlite: validate reevaluation trigger claim context: %w", err)
		}
		filtered = append(filtered, candidate)
	}
	result = filtered
	sort.SliceStable(result, func(i, j int) bool {
		if result[i].commit != result[j].commit {
			return result[i].commit < result[j].commit
		}
		left, _ := result[i].trigger.StableIdentity()
		right, _ := result[j].trigger.StableIdentity()
		return left < right
	})
	if len(result) > limit {
		result = result[:limit]
	}
	triggers := make([]autonomy.Trigger, len(result))
	for index := range result {
		triggers[index] = result[index].trigger
	}
	return triggers, nil
}

func (r *CanonicalRepository) DiscoverInitiativeTriggers(
	ctx context.Context,
	residentID canonical.ID,
	asOf canonical.Instant,
	maxStaleness time.Duration,
	limit int,
) ([]autonomy.Trigger, error) {
	if err := residentID.Validate(); err != nil {
		return nil, err
	}
	if asOf == 0 || maxStaleness <= 0 || limit < 1 {
		return nil, nil
	}
	tx, err := r.store.reader.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	var head int64
	if err := tx.QueryRowContext(ctx, `SELECT COALESCE(MAX(commit_seq), 0) FROM canonical_commits`).Scan(&head); err != nil {
		return nil, err
	}
	if head < 1 {
		return nil, nil
	}
	var activePolicyRaw string
	var policyContent []byte
	if err := tx.QueryRowContext(ctx, `SELECT revision.revision_id, blob.content
		FROM resident_revision_activations activation
		JOIN canonical_commits commit_row ON commit_row.canonical_commit_id = activation.canonical_commit_id
		JOIN resident_revisions revision ON revision.revision_id = activation.revision_id
		JOIN content_objects content ON content.content_id = revision.content_id
		JOIN blobs blob ON blob.dedupe_scope_id = content.owner_resident_id
		 AND blob.hash_algorithm = content.blob_hash_algorithm AND blob.blob_hash = content.blob_hash
		WHERE activation.resident_id = ? AND revision.revision_class = 'memory_policy'
		  AND commit_row.commit_seq <= ? ORDER BY commit_row.commit_seq DESC LIMIT 1`, residentID.String(), head).Scan(
		&activePolicyRaw, &policyContent,
	); err != nil {
		return nil, err
	}
	policy, _, err := memory.ParsePolicy(policyContent)
	if err != nil || policy.RequireEnabled() != nil {
		return nil, nil
	}
	var claimHead, claimAsOf int64
	var claimVersion string
	if err := tx.QueryRowContext(ctx, `SELECT source_commit_seq, as_of, projection_version
		FROM projection_watermarks WHERE projection_name = 'claim_states' AND resident_id = ?`, residentID.String()).Scan(
		&claimHead, &claimAsOf, &claimVersion,
	); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, nil
		}
		return nil, err
	}
	var scopeHead int64
	var scopeVersion string
	if err := tx.QueryRowContext(ctx, `SELECT source_commit_seq, projection_version
		FROM projection_watermarks WHERE projection_name = 'claim_view_scope_current' AND resident_id = ?`, residentID.String()).Scan(
		&scopeHead, &scopeVersion,
	); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, nil
		}
		return nil, err
	}
	var dependencyCount, exactDependency, scopeDependencyCount int
	if err := tx.QueryRowContext(ctx, `SELECT COUNT(*), COALESCE(SUM(CASE WHEN dependency_kind = 'memory_policy'
		AND dependency_version_id = ? THEN 1 ELSE 0 END), 0)
		FROM projection_watermark_dependencies WHERE projection_name = 'claim_states' AND resident_id = ?`,
		activePolicyRaw, residentID.String()).Scan(&dependencyCount, &exactDependency); err != nil {
		return nil, err
	}
	if err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM projection_watermark_dependencies
		WHERE projection_name = 'claim_view_scope_current' AND resident_id = ?`, residentID.String()).Scan(
		&scopeDependencyCount,
	); err != nil {
		return nil, err
	}
	projectionAsOf := canonical.Instant(claimAsOf)
	if claimHead != head || scopeHead != head || claimVersion != "claim-states-v1" ||
		scopeVersion != "claim-view-scope-current-v1" || dependencyCount != 1 || exactDependency != 1 ||
		scopeDependencyCount != 0 || projectionAsOf > asOf || asOf.Time().Sub(projectionAsOf.Time()) > maxStaleness {
		return nil, nil
	}
	volatileStaleMicros := int64(policy.Temporal.VolatileStaleDays) * int64(24*time.Hour/time.Microsecond)
	rows, err := tx.QueryContext(ctx, `WITH temporal_candidates AS (
		SELECT state.claim_id, claim.temporal_kind, state.temporal_relation, claim.recorded_at AS claim_recorded_at,
		(SELECT assertion.valid_from FROM claim_validity_assertions assertion
		 JOIN canonical_commits commit_row ON commit_row.canonical_commit_id = assertion.canonical_commit_id
		 WHERE assertion.claim_id = claim.claim_id AND commit_row.commit_seq <= ?
		 ORDER BY commit_row.commit_seq DESC, assertion.validity_assertion_id DESC LIMIT 1) AS valid_from,
		(SELECT assertion.recorded_at FROM claim_validity_assertions assertion
		 JOIN canonical_commits commit_row ON commit_row.canonical_commit_id = assertion.canonical_commit_id
		 WHERE assertion.claim_id = claim.claim_id AND commit_row.commit_seq <= ?
		 ORDER BY commit_row.commit_seq DESC, assertion.validity_assertion_id DESC LIMIT 1) AS validity_recorded_at,
		(SELECT MAX(evidence.recorded_at) FROM claim_evidence evidence
		 JOIN canonical_commits commit_row ON commit_row.canonical_commit_id = evidence.canonical_commit_id
		 WHERE evidence.claim_id = claim.claim_id AND evidence.polarity = 'support' AND commit_row.commit_seq <= ?) AS latest_support
		FROM claim_states state
		JOIN claim_view_scope_current scope ON scope.claim_id = state.claim_id AND scope.resident_id = state.resident_id
		JOIN claims claim ON claim.claim_id = state.claim_id
		LEFT JOIN content_objects content ON content.content_id = claim.statement_content_id
		WHERE state.resident_id = ? AND state.stage = 'settled' AND state.status = 'active'
		  AND scope.view_scope = 'resident_ui' AND claim.kind = 'direct'
		  AND state.temporal_relation IN ('current', 'stale_unknown')
		  AND claim.statement_hash IS NOT NULL
		  AND (content.content_id IS NULL OR content.erasure_state = 'present')
		  AND EXISTS(SELECT 1 FROM claim_evidence evidence JOIN events event ON event.event_id = evidence.event_id
		      WHERE evidence.claim_id = claim.claim_id AND evidence.polarity = 'support'
		        AND event.resident_id = state.resident_id AND event.event_type <> 'self_talk')
	)
		SELECT candidate.claim_id, candidate.temporal_kind, candidate.temporal_relation,
		 candidate.claim_recorded_at, candidate.valid_from, candidate.validity_recorded_at, candidate.latest_support
		FROM temporal_candidates candidate
		WHERE (
		 candidate.temporal_relation = 'current' AND candidate.valid_from IS NOT NULL AND candidate.valid_from > 0
		 AND candidate.valid_from <= ? AND candidate.claim_recorded_at < candidate.valid_from
		 AND candidate.validity_recorded_at < candidate.valid_from
		 AND NOT EXISTS(SELECT 1 FROM generation_runs run WHERE run.resident_id = ?
		  AND run.idempotency_key = ('outbound_initiative:v1:future_to_current:' || candidate.claim_id || ':' ||
		   candidate.valid_from || ':autonomy-policy-v0'))
		) OR (
		 candidate.temporal_relation = 'stale_unknown' AND candidate.temporal_kind = 'volatile'
		 AND candidate.latest_support IS NOT NULL
		 AND NOT EXISTS(SELECT 1 FROM generation_runs run WHERE run.resident_id = ?
		  AND run.idempotency_key = ('outbound_initiative:v1:volatile_aging:' || candidate.claim_id || ':' ||
		   (candidate.latest_support + ?) || ':autonomy-policy-v0'))
		)
		ORDER BY candidate.claim_id LIMIT ?`, head, head, head, residentID.String(),
		projectionAsOf.UnixMicro(), residentID.String(), residentID.String(), volatileStaleMicros, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	result := make([]autonomy.Trigger, 0, limit)
	for rows.Next() {
		var claimRaw, temporalKind, relation string
		var claimRecordedAt int64
		var validFrom, validityRecordedAt, latestSupport sql.NullInt64
		if err := rows.Scan(&claimRaw, &temporalKind, &relation, &claimRecordedAt,
			&validFrom, &validityRecordedAt, &latestSupport); err != nil {
			return nil, err
		}
		claimID, err := canonical.ParseID(claimRaw)
		if err != nil {
			return nil, err
		}
		if _, err := loadEligibleClaimStatement(ctx, tx, residentID, claimID); err != nil {
			if errors.Is(err, domain.ErrClaimSourceIneligible) {
				continue
			}
			return nil, fmt.Errorf("sqlite: validate initiative trigger claim %s: %w", claimID, err)
		}
		switch {
		case relation == "current" && validFrom.Valid && validityRecordedAt.Valid && validFrom.Int64 > 0 &&
			claimRecordedAt < validFrom.Int64 && validityRecordedAt.Int64 < validFrom.Int64 &&
			projectionAsOf >= canonical.Instant(validFrom.Int64):
			result = append(result, autonomy.Trigger{Kind: autonomy.TriggerFutureToCurrent, SourceID: claimID,
				Boundary: canonical.Instant(validFrom.Int64)})
		case relation == "stale_unknown" && temporalKind == "volatile" && latestSupport.Valid:
			boundary := time.UnixMicro(latestSupport.Int64).Add(time.Duration(policy.Temporal.VolatileStaleDays) * 24 * time.Hour)
			result = append(result, autonomy.Trigger{Kind: autonomy.TriggerVolatileAging, SourceID: claimID,
				Boundary: canonical.Instant(boundary.UnixMicro())})
		}
		if len(result) == limit {
			break
		}
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return result, tx.Commit()
}

func (r *CanonicalRepository) AutonomousWork(
	ctx context.Context,
	residentID canonical.ID,
	trigger autonomy.Trigger,
	maxAttempts int,
) (domain.AutonomousWork, error) {
	if err := residentID.Validate(); err != nil {
		return domain.AutonomousWork{}, err
	}
	if err := trigger.Validate(); err != nil {
		return domain.AutonomousWork{}, err
	}
	if maxAttempts < 1 {
		return domain.AutonomousWork{}, errors.New("sqlite: autonomous max attempts must be positive")
	}
	purpose := domain.GenerationPurposeSelfTalk
	if trigger.Kind.IsInitiative() {
		purpose = domain.GenerationPurposeOutboundInitiative
	}
	work := domain.AutonomousWork{ResidentID: residentID, Purpose: purpose, Trigger: trigger, State: domain.WorkPending}
	key, err := trigger.IdempotencyKey(string(autonomy.PolicyVersionV0), residentID)
	if err != nil {
		return domain.AutonomousWork{}, err
	}
	var runRaw, purposeRaw string
	err = r.store.reader.QueryRowContext(ctx, `SELECT generation_run_id, purpose FROM generation_runs
		WHERE resident_id = ? AND idempotency_key = ?`, residentID.String(), key).Scan(&runRaw, &purposeRaw)
	if errors.Is(err, sql.ErrNoRows) {
		return work, nil
	}
	if err != nil {
		return domain.AutonomousWork{}, err
	}
	if purposeRaw != string(purpose) {
		return domain.AutonomousWork{}, errors.New("sqlite: autonomous trigger key belongs to a different purpose")
	}
	runID, err := canonical.ParseID(runRaw)
	if err != nil {
		return domain.AutonomousWork{}, err
	}
	work.RunID = &runID
	summary, err := (generationOutcomeRepository{}).Summary(ctx, r.store.reader, runID, residentID)
	if err != nil {
		return domain.AutonomousWork{}, err
	}
	summary, err = summary.Classify(int64(maxAttempts))
	if err != nil {
		return domain.AutonomousWork{}, err
	}
	work.AttemptNo = summary.Latest.AttemptNo
	work.RetryCount = summary.RetryCount
	switch summary.ClassifiedState {
	case domain.WorkRunning:
		work.State = domain.WorkRunning
	case domain.WorkSucceeded:
		work.State = domain.WorkSucceeded
	case domain.WorkRetryPending:
		if summary.LatestForegroundPreempted {
			work.State = domain.WorkRetryPending
			work.ForegroundPreempted = true
		} else if summary.RetryEligible {
			work.State = domain.WorkRetryPending
		} else if !summary.RetryEligible {
			work.State = domain.WorkTerminalFailed
		}
	case domain.WorkTerminalFailed:
		work.State = domain.WorkTerminalFailed
	default:
		return domain.AutonomousWork{}, fmt.Errorf("sqlite: unknown autonomous outcome state %q", summary.PersistedState)
	}
	return work, nil
}

func (r *CanonicalRepository) DiscoverExecutableAutonomousWork(
	ctx context.Context,
	residentID canonical.ID,
	maxAttempts int,
	limit int,
) ([]domain.AutonomousWork, error) {
	if err := residentID.Validate(); err != nil {
		return nil, err
	}
	if maxAttempts < 1 || limit < 1 {
		return nil, nil
	}
	rows, err := r.store.reader.QueryContext(ctx, `SELECT run.generation_run_id, blob.content
		FROM generation_runs run
		JOIN generation_run_inputs input ON input.generation_run_id = run.generation_run_id
		JOIN canonical_commits run_commit ON run_commit.canonical_commit_id = run.canonical_commit_id
		JOIN content_objects object ON object.content_id = input.content_id
		JOIN blobs blob ON blob.dedupe_scope_id = object.owner_resident_id
		 AND blob.hash_algorithm = object.blob_hash_algorithm AND blob.blob_hash = object.blob_hash
		WHERE run.resident_id = ? AND run.purpose IN ('self_talk','outbound_initiative')
		 AND input.source_type = 'runtime_projection' AND input.inclusion_mode = 'runtime_projection'
		 AND object.erasure_state = 'present'
		ORDER BY run_commit.commit_seq, run.generation_run_id`, residentID.String())
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	type candidate struct {
		runID   canonical.ID
		trigger autonomy.Trigger
	}
	candidates := make([]candidate, 0, limit)
	for rows.Next() {
		var runRaw string
		var runtimeJSON []byte
		if err := rows.Scan(&runRaw, &runtimeJSON); err != nil {
			return nil, err
		}
		runID, err := canonical.ParseID(runRaw)
		if err != nil {
			return nil, err
		}
		trigger, _, _, _, err := domain.ParseAutonomousRuntimeProjection(runtimeJSON)
		if err != nil {
			return nil, fmt.Errorf("sqlite: parse executable autonomous run %s: %w", runID, err)
		}
		candidates = append(candidates, candidate{runID: runID, trigger: trigger})
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	result := make([]domain.AutonomousWork, 0, limit)
	for _, candidate := range candidates {
		work, err := r.AutonomousWork(ctx, residentID, candidate.trigger, maxAttempts)
		if err != nil {
			return nil, err
		}
		if work.RunID == nil || *work.RunID != candidate.runID ||
			(work.State != domain.WorkRunning && work.State != domain.WorkRetryPending) {
			continue
		}
		result = append(result, work)
		if len(result) == limit {
			break
		}
	}
	return result, nil
}

var _ domain.AutonomyWorkRepository = (*CanonicalRepository)(nil)
