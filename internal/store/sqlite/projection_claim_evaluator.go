package sqlite

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"mahoroba.local/mahoroba/internal/canonical"
	"mahoroba.local/mahoroba/internal/memory"
	"mahoroba.local/mahoroba/internal/projection"
)

// sqliteClaimStateEvaluator is the narrow adapter between typed Projection
// replay and the pure memory-policy package. Policy definitions are loaded for
// every claim evaluation so content erasure cannot be masked by a stale cache.
type sqliteClaimStateEvaluator struct {
	store *Store
}

func newSQLiteClaimStateEvaluator(store *Store) projection.ClaimStateEvaluator {
	return &sqliteClaimStateEvaluator{store: store}
}

func (evaluator *sqliteClaimStateEvaluator) EvaluateClaim(ctx context.Context, input projection.ClaimEvaluationInput) (projection.ClaimStateMetrics, error) {
	if evaluator == nil || evaluator.store == nil || evaluator.store.reader == nil {
		return projection.ClaimStateMetrics{}, projection.ErrClaimStateEvaluatorUnavailable
	}
	activePolicy, err := evaluator.loadPolicy(ctx, input.ActivePolicyRevisionID)
	if err != nil {
		return projection.ClaimStateMetrics{}, err
	}
	if err := activePolicy.RequireEnabled(); err != nil {
		return projection.ClaimStateMetrics{}, fmt.Errorf("sqlite: active memory policy %s cannot evaluate claim states: %w", input.ActivePolicyRevisionID, err)
	}

	evaluatedEvidence := make([]memory.EvaluatedEvidence, 0, len(input.Evidence))
	latestSupportAt := input.Claim.RecordedAt
	for _, row := range input.Evidence {
		policy, err := evaluator.loadPolicy(ctx, row.PolicyRevisionID)
		if err != nil {
			return projection.ClaimStateMetrics{}, err
		}
		point := memory.EvidencePoint{
			SourceEventID: row.SourceEventID, SourceEvidenceID: cloneCanonicalID(row.SourceEvidenceID),
			EventType: memory.EventType(row.SourceEventType), Polarity: memory.EvidencePolarity(row.Polarity),
			Grade: memory.EvidenceGrade(row.Grade), Trust: memory.TrustLevel(row.TrustLevel),
			Derivation: memory.EvidenceDerivation(row.Derivation), InheritanceDepth: row.InheritanceDepth,
			Reason: memory.EvidenceReason(row.ReasonCode), ActorIsSubject: row.ActorIsSubject,
			ActorIsPerspective: row.ActorIsPerspective,
		}
		value, err := memory.EvaluateEvidence(policy, point)
		if err != nil {
			return projection.ClaimStateMetrics{}, fmt.Errorf("sqlite: evaluate claim evidence %s under policy %s: %w", row.EvidenceID, row.PolicyRevisionID, err)
		}
		if value.EffectiveWeight != row.Weight {
			return projection.ClaimStateMetrics{}, fmt.Errorf("sqlite: claim evidence %s recorded weight %s differs from policy replay %s", row.EvidenceID, row.Weight, value.EffectiveWeight)
		}
		evaluatedEvidence = append(evaluatedEvidence, value)
		if row.Polarity == projection.ClaimEvidenceSupport && row.RecordedAt > latestSupportAt {
			latestSupportAt = row.RecordedAt
		}
	}
	evidenceAggregate, err := memory.AggregateEvidence(activePolicy, evaluatedEvidence)
	if err != nil {
		return projection.ClaimStateMetrics{}, fmt.Errorf("sqlite: aggregate claim evidence: %w", err)
	}

	// Recall records candidate, selected and prompt inclusion as separate
	// provenance rows. Policy grants only the greatest contribution within one
	// claim+Recall run, so those rows collapse to one salience point. Usage that
	// has no Recall run remains independently countable.
	type salienceGroup struct {
		point   memory.SaliencePoint
		usageID canonical.ID
	}
	groups := make([]salienceGroup, 0, len(input.Usages))
	groupIndexes := make(map[string]int, len(input.Usages))
	var lastReferencedAt *canonical.Instant
	for _, row := range input.Usages {
		policy, err := evaluator.loadPolicy(ctx, row.PolicyRevisionID)
		if err != nil {
			return projection.ClaimStateMetrics{}, err
		}
		contribution, err := memory.SalienceContribution(policy, memory.UsageType(row.UsageType))
		if err != nil {
			return projection.ClaimStateMetrics{}, fmt.Errorf("sqlite: evaluate claim usage %s under policy %s: %w", row.UsageID, row.PolicyRevisionID, err)
		}
		key := "usage:" + row.UsageID.String()
		if row.RecallRunID != nil {
			key = "recall:" + row.RecallRunID.String()
		}
		candidate := salienceGroup{
			point:   memory.SaliencePoint{Contribution: contribution, RecordedAt: row.RecordedAt},
			usageID: row.UsageID,
		}
		if index, exists := groupIndexes[key]; !exists {
			groupIndexes[key] = len(groups)
			groups = append(groups, candidate)
		} else {
			current := groups[index]
			if candidate.point.Contribution > current.point.Contribution ||
				(candidate.point.Contribution == current.point.Contribution &&
					(candidate.point.RecordedAt > current.point.RecordedAt ||
						(candidate.point.RecordedAt == current.point.RecordedAt &&
							candidate.usageID.String() < current.usageID.String()))) {
				groups[index] = candidate
			}
		}
		if row.UsageType != projection.ClaimUsageCandidate && (lastReferencedAt == nil || row.RecordedAt > *lastReferencedAt) {
			value := row.RecordedAt
			lastReferencedAt = &value
		}
	}
	saliencePoints := make([]memory.SaliencePoint, len(groups))
	for index, group := range groups {
		saliencePoints[index] = group.point
	}
	salience, err := memory.AggregateSalience(activePolicy, saliencePoints, input.AsOf)
	if err != nil {
		return projection.ClaimStateMetrics{}, fmt.Errorf("sqlite: aggregate claim salience: %w", err)
	}

	temporalInput := memory.TemporalInput{
		Kind: memory.TemporalKind(input.Claim.TemporalKind), AnchorAt: latestSupportAt, AsOf: input.AsOf,
	}
	if len(input.ValidityAssertions) > 0 {
		latest := input.ValidityAssertions[len(input.ValidityAssertions)-1]
		temporalInput.ValidFrom = cloneCanonicalInstant(latest.ValidFrom)
		temporalInput.ValidTo = cloneCanonicalInstant(latest.ValidTo)
	}
	temporal, err := memory.EvaluateTemporal(activePolicy, temporalInput)
	if err != nil {
		return projection.ClaimStateMetrics{}, fmt.Errorf("sqlite: evaluate claim temporal state: %w", err)
	}
	return projection.ClaimStateMetrics{
		Salience:   float64(salience.Millionths()) / float64(canonical.FixedPointScale),
		Confidence: evidenceAggregate.Confidence, Currentness: temporal.Currentness,
		TemporalRelation: projection.ClaimTemporalRelation(temporal.Relation),
		LastReferencedAt: cloneCanonicalInstant(lastReferencedAt),
	}, nil
}

func (evaluator *sqliteClaimStateEvaluator) loadPolicy(ctx context.Context, revisionID canonical.ID) (memory.Policy, error) {
	if evaluator == nil || evaluator.store == nil || evaluator.store.reader == nil {
		return memory.Policy{}, projection.ErrClaimStateEvaluatorUnavailable
	}
	return loadSQLiteMemoryPolicy(ctx, evaluator.store, revisionID)
}

// loadSQLiteMemoryPolicy is shared by dependency resolution and numeric
// evaluation. Resolving only the revision ID would allow an erased or invalid
// policy to be recorded as a supposedly usable watermark dependency when a
// resident currently has no claims.
func loadSQLiteMemoryPolicy(ctx context.Context, store *Store, revisionID canonical.ID) (memory.Policy, error) {
	if store == nil || store.reader == nil {
		return memory.Policy{}, projection.ErrClaimStateEvaluatorUnavailable
	}
	if err := revisionID.Validate(); err != nil {
		return memory.Policy{}, fmt.Errorf("%w: invalid memory policy revision: %v", projection.ErrUnresolvedDependency, err)
	}
	var content []byte
	err := store.reader.QueryRowContext(ctx, `SELECT b.content
		FROM resident_revisions r
		JOIN content_objects o ON o.content_id = r.content_id
		JOIN blobs b ON b.dedupe_scope_id = o.owner_resident_id
		 AND b.hash_algorithm = o.blob_hash_algorithm AND b.blob_hash = o.blob_hash
		WHERE r.revision_id = ? AND r.revision_class = 'memory_policy'
		  AND o.content_class = 'memory_policy_text' AND o.erasure_state = 'present'`, revisionID.String()).Scan(&content)
	if errors.Is(err, sql.ErrNoRows) {
		return memory.Policy{}, fmt.Errorf("%w: memory policy revision %s content is unavailable", projection.ErrUnresolvedDependency, revisionID)
	}
	if err != nil {
		return memory.Policy{}, fmt.Errorf("sqlite: load memory policy revision %s: %w", revisionID, err)
	}
	policy, _, err := memory.ParsePolicy(content)
	if err != nil {
		return memory.Policy{}, fmt.Errorf("%w: memory policy revision %s is invalid: %v", projection.ErrUnresolvedDependency, revisionID, err)
	}
	return policy, nil
}

func cloneCanonicalID(value *canonical.ID) *canonical.ID {
	if value == nil {
		return nil
	}
	copyValue := *value
	return &copyValue
}

func cloneCanonicalInstant(value *canonical.Instant) *canonical.Instant {
	if value == nil {
		return nil
	}
	copyValue := *value
	return &copyValue
}

var _ projection.ClaimStateEvaluator = (*sqliteClaimStateEvaluator)(nil)
