package sqlite

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"slices"

	"mahoroba.local/mahoroba/internal/canonical"
	"mahoroba.local/mahoroba/internal/integrity"
	"mahoroba.local/mahoroba/internal/memory"
)

// qualifyingSupportClaim is deliberately limited to the identity and typed
// gate fields needed by the replay. Statement bytes, hashes, salts, and
// commitments never enter scanner candidates or diagnostics.
type qualifyingSupportClaim struct {
	residentID    canonical.ID
	claimID       canonical.ID
	kind          memory.ClaimKind
	subjectID     canonical.ID
	perspectiveID canonical.ID
	recordedAt    canonical.Instant
	recordedTZ    canonical.Timezone
}

type qualifyingSupportEvidence struct {
	id        canonical.ID
	commitSeq int64
	contentID canonical.ID
	qualifies bool
}

type qualifyingSupportErasure struct {
	id         canonical.ID
	commitSeq  int64
	contentID  canonical.ID
	occurredAt canonical.Instant
	occurredTZ canonical.Timezone
}

func loadQualifyingSupportCandidates(ctx context.Context, tx *sql.Tx) ([]integrity.CandidateInput, error) {
	rows, err := tx.QueryContext(ctx, `SELECT
		claim.owner_resident_id, claim.claim_id, claim.kind,
		claim.subject_principal_id, claim.perspective_principal_id,
		claim.recorded_at, claim.recorded_tz
	FROM claims claim
	JOIN content_objects statement ON statement.content_id = claim.statement_content_id
	WHERE statement.erasure_state = 'present'
	ORDER BY claim.owner_resident_id, claim.claim_id`)
	if err != nil {
		return nil, fmt.Errorf("sqlite: scan qualifying-support claims: %w", err)
	}
	defer rows.Close()

	claims := make([]qualifyingSupportClaim, 0)
	for rows.Next() {
		var residentRaw, claimRaw, subjectRaw, perspectiveRaw, timezoneRaw string
		var kindRaw sql.NullString
		var recordedAt int64
		if err := rows.Scan(&residentRaw, &claimRaw, &kindRaw, &subjectRaw, &perspectiveRaw, &recordedAt, &timezoneRaw); err != nil {
			return nil, fmt.Errorf("sqlite: scan qualifying-support claim: %w", err)
		}
		residentID, err := canonical.ParseID(residentRaw)
		if err != nil {
			return nil, err
		}
		claimID, err := canonical.ParseID(claimRaw)
		if err != nil {
			return nil, err
		}
		subjectID, err := canonical.ParseID(subjectRaw)
		if err != nil {
			return nil, err
		}
		perspectiveID, err := canonical.ParseID(perspectiveRaw)
		if err != nil {
			return nil, err
		}
		kind := memory.ClaimKindUnclassified
		if kindRaw.Valid {
			kind = memory.ClaimKind(kindRaw.String)
		}
		if err := kind.Validate(); err != nil {
			return nil, fmt.Errorf("sqlite: invalid qualifying-support claim kind: %w", err)
		}
		timezone, err := canonical.ParseTimezone(timezoneRaw)
		if err != nil {
			return nil, err
		}
		claims = append(claims, qualifyingSupportClaim{
			residentID: residentID, claimID: claimID, kind: kind,
			subjectID: subjectID, perspectiveID: perspectiveID,
			recordedAt: canonical.Instant(recordedAt), recordedTZ: timezone,
		})
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("sqlite: iterate qualifying-support claims: %w", err)
	}

	result := make([]integrity.CandidateInput, 0)
	for _, claim := range claims {
		candidate, err := replayQualifyingSupportCandidate(ctx, tx, claim)
		if err != nil {
			return nil, err
		}
		if candidate != nil {
			result = append(result, *candidate)
		}
	}
	return result, nil
}

func replayQualifyingSupportCandidate(
	ctx context.Context,
	tx *sql.Tx,
	claim qualifyingSupportClaim,
) (*integrity.CandidateInput, error) {
	evidence, contentStates, hadSupport, err := loadQualifyingSupportEvidence(ctx, tx, claim)
	if err != nil {
		return nil, err
	}
	erasures, err := loadQualifyingSupportErasures(ctx, tx, claim.claimID)
	if err != nil {
		return nil, err
	}
	erasureCounts := make(map[canonical.ID]int, len(erasures))
	for _, erasure := range erasures {
		erasureCounts[erasure.contentID]++
	}
	for contentID, state := range contentStates {
		count := erasureCounts[contentID]
		if state == "erased" && count != 1 {
			return nil, fmt.Errorf("%w: qualifying-support content/event pairing is inconsistent", integrity.ErrCandidateConflict)
		}
	}

	type replayStep struct {
		commitSeq int64
		kind      int // evidence=0, erasure=1; a Canonical UoW establishes support before erasing it.
		identity  canonical.ID
		evidence  *qualifyingSupportEvidence
		erasure   *qualifyingSupportErasure
	}
	steps := make([]replayStep, 0, len(evidence)+len(erasures))
	for index := range evidence {
		row := &evidence[index]
		steps = append(steps, replayStep{commitSeq: row.commitSeq, kind: 0, identity: row.id, evidence: row})
	}
	for index := range erasures {
		row := &erasures[index]
		steps = append(steps, replayStep{commitSeq: row.commitSeq, kind: 1, identity: row.id, erasure: row})
	}
	slices.SortFunc(steps, func(left, right replayStep) int {
		if left.commitSeq != right.commitSeq {
			if left.commitSeq < right.commitSeq {
				return -1
			}
			return 1
		}
		if left.kind != right.kind {
			return left.kind - right.kind
		}
		if left.identity.String() < right.identity.String() {
			return -1
		}
		if left.identity.String() > right.identity.String() {
			return 1
		}
		return 0
	})

	erased := make(map[canonical.ID]struct{}, len(erasures))
	active := make(map[canonical.ID]canonical.ID, len(evidence))
	everValid := false
	var firstBreak *qualifyingSupportErasure
	for _, step := range steps {
		before := len(active) > 0
		if step.erasure != nil {
			erased[step.erasure.contentID] = struct{}{}
			for evidenceID, contentID := range active {
				if contentID == step.erasure.contentID {
					delete(active, evidenceID)
				}
			}
		} else if step.evidence.qualifies {
			if _, unavailable := erased[step.evidence.contentID]; !unavailable {
				active[step.evidence.id] = step.evidence.contentID
			}
		}
		after := len(active) > 0
		if after {
			everValid = true
		}
		if firstBreak == nil && everValid && before && !after && step.erasure != nil {
			copy := *step.erasure
			firstBreak = &copy
		}
	}
	if len(active) > 0 || !hadSupport {
		return nil, nil
	}
	claimID := claim.claimID
	if firstBreak != nil {
		sourceID := firstBreak.id
		return &integrity.CandidateInput{
			ResidentID: claim.residentID, ClaimID: &claimID,
			Kind:       integrity.FindingRequiredProvenanceErased,
			RuleCode:   integrity.RuleClaimQualifyingSupportErased,
			TargetKind: integrity.TargetClaim, TargetID: claim.claimID, TargetField: "qualifying_support",
			SourceContentErasureEventID: &sourceID,
			OccurredAt:                  firstBreak.occurredAt, OccurredTZ: firstBreak.occurredTZ,
		}, nil
	}
	return &integrity.CandidateInput{
		ResidentID: claim.residentID, ClaimID: &claimID,
		Kind:       integrity.FindingProvenanceUnresolvable,
		RuleCode:   integrity.RuleClaimQualifyingSupportUnresolvable,
		TargetKind: integrity.TargetClaim, TargetID: claim.claimID, TargetField: "qualifying_support",
		OccurredAt: claim.recordedAt, OccurredTZ: claim.recordedTZ,
	}, nil
}

func loadQualifyingSupportEvidence(
	ctx context.Context,
	tx *sql.Tx,
	claim qualifyingSupportClaim,
) ([]qualifyingSupportEvidence, map[canonical.ID]string, bool, error) {
	rows, err := tx.QueryContext(ctx, `SELECT
		evidence.evidence_id, evidence_commit.commit_seq, event.event_id, event.content_id,
		evidence.polarity, evidence.grade, evidence.trust_level, evidence.weight,
		evidence.derivation, evidence.source_evidence_id, evidence.reason_code,
		event.event_type, event.trust_level, event.actor_principal_id,
		source.event_id, source.derivation,
		policy_revision.resident_id, policy_revision.revision_class,
		policy_content.erasure_state, policy_blob.content,
		event_content.erasure_state
	FROM claim_evidence evidence
	JOIN canonical_commits evidence_commit ON evidence_commit.canonical_commit_id = evidence.canonical_commit_id
	JOIN events event ON event.event_id = evidence.event_id
	JOIN content_objects event_content ON event_content.content_id = event.content_id
	LEFT JOIN claim_evidence source ON source.evidence_id = evidence.source_evidence_id
	JOIN resident_revisions policy_revision ON policy_revision.revision_id = evidence.memory_policy_revision_id
	JOIN content_objects policy_content ON policy_content.content_id = policy_revision.content_id
	LEFT JOIN blobs policy_blob
	  ON policy_blob.dedupe_scope_id = policy_content.owner_resident_id
	 AND policy_blob.hash_algorithm = policy_content.blob_hash_algorithm
	 AND policy_blob.blob_hash = policy_content.blob_hash
	WHERE evidence.claim_id = ?
	ORDER BY evidence_commit.commit_seq, evidence.evidence_id`, claim.claimID.String())
	if err != nil {
		return nil, nil, false, fmt.Errorf("sqlite: load qualifying-support evidence: %w", err)
	}
	defer rows.Close()

	result := make([]qualifyingSupportEvidence, 0)
	states := make(map[canonical.ID]string)
	hadSupport := false
	for rows.Next() {
		var evidenceRaw, eventRaw, contentRaw, polarityRaw, gradeRaw, trustRaw, derivationRaw, reasonRaw string
		var eventTypeRaw, eventTrustRaw, actorRaw, policyResidentRaw, policyClassRaw string
		var policyStateRaw, eventContentState string
		var sourceRaw, sourceEventRaw, sourceDerivationRaw sql.NullString
		var commitSeq, storedWeight int64
		var policyBytes []byte
		if err := rows.Scan(
			&evidenceRaw, &commitSeq, &eventRaw, &contentRaw,
			&polarityRaw, &gradeRaw, &trustRaw, &storedWeight,
			&derivationRaw, &sourceRaw, &reasonRaw,
			&eventTypeRaw, &eventTrustRaw, &actorRaw,
			&sourceEventRaw, &sourceDerivationRaw,
			&policyResidentRaw, &policyClassRaw, &policyStateRaw, &policyBytes,
			&eventContentState,
		); err != nil {
			return nil, nil, false, fmt.Errorf("sqlite: scan qualifying-support evidence: %w", err)
		}
		evidenceID, err := canonical.ParseID(evidenceRaw)
		if err != nil {
			return nil, nil, false, err
		}
		contentID, err := canonical.ParseID(contentRaw)
		if err != nil {
			return nil, nil, false, err
		}
		states[contentID] = eventContentState
		polarity := memory.EvidencePolarity(polarityRaw)
		if polarity == memory.PolaritySupport {
			hadSupport = true
		}
		row := qualifyingSupportEvidence{id: evidenceID, commitSeq: commitSeq, contentID: contentID}
		if polarity != memory.PolaritySupport || trustRaw != eventTrustRaw ||
			policyResidentRaw != claim.residentID.String() || policyClassRaw != "memory_policy" ||
			policyStateRaw != "present" || len(policyBytes) == 0 {
			result = append(result, row)
			continue
		}
		if memory.EvidenceDerivation(derivationRaw) == memory.DerivationInherited {
			if !sourceRaw.Valid || !sourceEventRaw.Valid || !sourceDerivationRaw.Valid ||
				sourceEventRaw.String != eventRaw ||
				sourceDerivationRaw.String != string(memory.DerivationExtracted) {
				result = append(result, row)
				continue
			}
		} else if sourceRaw.Valid {
			result = append(result, row)
			continue
		}
		policy, _, err := memory.ParsePolicy(policyBytes)
		if err != nil {
			result = append(result, row)
			continue
		}
		actorID, err := canonical.ParseID(actorRaw)
		if err != nil {
			return nil, nil, false, err
		}
		eventID, err := canonical.ParseID(eventRaw)
		if err != nil {
			return nil, nil, false, err
		}
		point := memory.EvidencePoint{
			SourceEventID: eventID, EventType: memory.EventType(eventTypeRaw),
			Polarity: polarity, Grade: memory.EvidenceGrade(gradeRaw), Trust: memory.TrustLevel(trustRaw),
			Derivation: memory.EvidenceDerivation(derivationRaw), Reason: memory.EvidenceReason(reasonRaw),
			ActorIsSubject: actorID == claim.subjectID, ActorIsPerspective: actorID == claim.perspectiveID,
		}
		if sourceRaw.Valid {
			sourceID, err := canonical.ParseID(sourceRaw.String)
			if err != nil {
				return nil, nil, false, err
			}
			point.SourceEvidenceID = &sourceID
			point.InheritanceDepth = 1
		}
		// EvaluateEvidence is the same immutable pointwise policy evaluator used
		// by the Writer. Contradictions and aggregate confidence are deliberately
		// absent: semantic disagreement is not a structural integrity failure.
		evaluated, err := memory.EvaluateEvidence(policy, point)
		if err == nil && evaluated.EffectiveWeight.Millionths() == storedWeight &&
			qualifyingActorGate(claim.kind, point) {
			row.qualifies = true
		}
		result = append(result, row)
	}
	if err := rows.Err(); err != nil {
		return nil, nil, false, fmt.Errorf("sqlite: iterate qualifying-support evidence: %w", err)
	}
	return result, states, hadSupport, nil
}

func qualifyingActorGate(kind memory.ClaimKind, point memory.EvidencePoint) bool {
	switch kind {
	case memory.ClaimKindOther:
		return point.ActorIsSubject
	case memory.ClaimKindMeta:
		return point.ActorIsPerspective
	case memory.ClaimKindDirect, memory.ClaimKindUnclassified:
		return true
	default:
		return false
	}
}

func loadQualifyingSupportErasures(
	ctx context.Context,
	tx *sql.Tx,
	claimID canonical.ID,
) ([]qualifyingSupportErasure, error) {
	rows, err := tx.QueryContext(ctx, `SELECT DISTINCT
		event.content_id, erasure.content_erasure_event_id, erasure_commit.commit_seq,
		erasure.occurred_at, erasure.occurred_tz
	FROM claim_evidence evidence
	JOIN events event ON event.event_id = evidence.event_id
	JOIN content_objects event_content ON event_content.content_id = event.content_id
	JOIN content_erasure_events erasure ON erasure.content_id = event.content_id
	JOIN canonical_commits erasure_commit ON erasure_commit.canonical_commit_id = erasure.canonical_commit_id
	WHERE evidence.claim_id = ?
	  AND event_content.erasure_state = 'erased'
	ORDER BY erasure_commit.commit_seq, erasure.content_erasure_event_id`, claimID.String())
	if err != nil {
		return nil, fmt.Errorf("sqlite: load qualifying-support erasures: %w", err)
	}
	defer rows.Close()
	result := make([]qualifyingSupportErasure, 0)
	for rows.Next() {
		var contentRaw, erasureRaw, timezoneRaw string
		var commitSeq, occurredAt int64
		if err := rows.Scan(&contentRaw, &erasureRaw, &commitSeq, &occurredAt, &timezoneRaw); err != nil {
			return nil, fmt.Errorf("sqlite: scan qualifying-support erasure: %w", err)
		}
		contentID, err := canonical.ParseID(contentRaw)
		if err != nil {
			return nil, err
		}
		erasureID, err := canonical.ParseID(erasureRaw)
		if err != nil {
			return nil, err
		}
		timezone, err := canonical.ParseTimezone(timezoneRaw)
		if err != nil {
			return nil, err
		}
		result = append(result, qualifyingSupportErasure{
			id: erasureID, commitSeq: commitSeq, contentID: contentID,
			occurredAt: canonical.Instant(occurredAt), occurredTZ: timezone,
		})
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("sqlite: iterate qualifying-support erasures: %w", err)
	}
	return result, nil
}

func loadOneQualifyingSupportCandidate(
	ctx context.Context,
	tx *sql.Tx,
	residentID, claimID canonical.ID,
) (*integrity.CandidateInput, error) {
	var residentRaw, claimRaw, subjectRaw, perspectiveRaw, timezoneRaw, statementState string
	var kindRaw sql.NullString
	var recordedAt int64
	err := tx.QueryRowContext(ctx, `SELECT
		claim.owner_resident_id, claim.claim_id, claim.kind,
		claim.subject_principal_id, claim.perspective_principal_id,
		claim.recorded_at, claim.recorded_tz, statement.erasure_state
	FROM claims claim
	JOIN content_objects statement ON statement.content_id = claim.statement_content_id
	WHERE claim.claim_id = ? AND claim.owner_resident_id = ?`, claimID.String(), residentID.String()).Scan(
		&residentRaw, &claimRaw, &kindRaw, &subjectRaw, &perspectiveRaw,
		&recordedAt, &timezoneRaw, &statementState,
	)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("sqlite: load qualifying-support claim for revalidation: %w", err)
	}
	if statementState != "present" {
		return nil, nil
	}
	parsedResident, err := canonical.ParseID(residentRaw)
	if err != nil {
		return nil, err
	}
	parsedClaim, err := canonical.ParseID(claimRaw)
	if err != nil {
		return nil, err
	}
	subjectID, err := canonical.ParseID(subjectRaw)
	if err != nil {
		return nil, err
	}
	perspectiveID, err := canonical.ParseID(perspectiveRaw)
	if err != nil {
		return nil, err
	}
	timezone, err := canonical.ParseTimezone(timezoneRaw)
	if err != nil {
		return nil, err
	}
	kind := memory.ClaimKindUnclassified
	if kindRaw.Valid {
		kind = memory.ClaimKind(kindRaw.String)
	}
	return replayQualifyingSupportCandidate(ctx, tx, qualifyingSupportClaim{
		residentID: parsedResident, claimID: parsedClaim, kind: kind,
		subjectID: subjectID, perspectiveID: perspectiveID,
		recordedAt: canonical.Instant(recordedAt), recordedTZ: timezone,
	})
}
