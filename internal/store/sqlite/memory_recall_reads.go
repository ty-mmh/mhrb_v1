package sqlite

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"fmt"
	"math"
	"slices"
	"time"

	"mahoroba.local/mahoroba/internal/canonical"
	"mahoroba.local/mahoroba/internal/domain"
	"mahoroba.local/mahoroba/internal/memory"
	"mahoroba.local/mahoroba/internal/projection"
)

const recallProjectionMaxStaleness = 5 * time.Minute

var errRecallProjectionProvenance = errors.New("sqlite: Recall Projection provenance is unknown")

type dialogueRecallSnapshot struct {
	enabled    bool
	head       canonical.CommitSeq
	policy     memory.Policy
	pipelineID canonical.ID
	candidates []memory.RecallCandidate
	fallback   domain.MemoryRecallFallbackReason
}

func (u *canonicalUoW) loadDialogueRecallSnapshot(
	ctx context.Context,
	residentID canonical.ID,
	dialogue dialogueSnapshot,
) (dialogueRecallSnapshot, error) {
	policy, _, err := memory.ParsePolicy(dialogue.memoryPolicy)
	if err != nil {
		return dialogueRecallSnapshot{}, fmt.Errorf("sqlite: parse active memory policy for Recall: %w", err)
	}
	if policy.Version == memory.PolicyVersionV1 {
		return dialogueRecallSnapshot{}, nil
	}
	if err := policy.RequireEnabled(); err != nil {
		return dialogueRecallSnapshot{}, fmt.Errorf("sqlite: validate active memory policy for Recall: %w", err)
	}
	result := dialogueRecallSnapshot{enabled: true, policy: policy}
	headValue := u.metadata.CommitSeq.Int64() - 1
	if headValue < 1 {
		result.fallback = domain.MemoryRecallProjectionUnavailable
		return result, nil
	}
	result.head, err = canonical.NewCommitSeq(headValue)
	if err != nil {
		return dialogueRecallSnapshot{}, err
	}

	for _, definition := range []projection.Definition{
		projection.ClaimStatesDefinition(), projection.ClaimViewScopeCurrentDefinition(),
	} {
		reason, inspectErr := u.inspectRecallProjection(
			ctx, residentID, dialogue.memoryID, result.head, u.metadata.CommittedAt, definition,
		)
		if inspectErr != nil {
			return dialogueRecallSnapshot{}, inspectErr
		}
		if reason != "" {
			result.fallback = reason
			return result, nil
		}
	}

	pipelineID, reason, err := u.loadRecallPipeline(ctx, result.head)
	if err != nil {
		return dialogueRecallSnapshot{}, err
	}
	if reason != "" {
		result.fallback = reason
		return result, nil
	}
	result.pipelineID = pipelineID
	result.candidates, err = u.loadRecallCandidates(ctx, residentID, result.head, result.policy)
	if errors.Is(err, errRecallProjectionProvenance) {
		result.fallback = domain.MemoryRecallProjectionProvenanceUnknown
		result.candidates = nil
		return result, nil
	}
	if err != nil {
		return dialogueRecallSnapshot{}, err
	}
	return result, nil
}

// loadDialogueRecallSnapshotAt is the read-side COV assembly path. Unlike the
// legacy combined write path, target.Head already includes Commit A and must
// be used directly rather than inferred as the previous Writer commit.
func (u *canonicalUoW) loadDialogueRecallSnapshotAt(
	ctx context.Context,
	residentID canonical.ID,
	dialogue dialogueSnapshot,
	target domain.AssemblyTarget,
) (dialogueRecallSnapshot, error) {
	policy, _, err := memory.ParsePolicy(dialogue.memoryPolicy)
	if err != nil {
		return dialogueRecallSnapshot{}, fmt.Errorf("sqlite: parse active memory policy for Recall: %w", err)
	}
	if policy.Version == memory.PolicyVersionV1 {
		return dialogueRecallSnapshot{}, nil
	}
	if err := policy.RequireEnabled(); err != nil {
		return dialogueRecallSnapshot{}, fmt.Errorf("sqlite: validate active memory policy for Recall: %w", err)
	}
	result := dialogueRecallSnapshot{enabled: true, policy: policy, head: target.Head.CommitSeq}
	for _, definition := range []projection.Definition{
		projection.ClaimStatesDefinition(), projection.ClaimViewScopeCurrentDefinition(),
	} {
		reason, inspectErr := u.inspectRecallProjectionAt(ctx, residentID, dialogue.memoryID, target, definition)
		if inspectErr != nil {
			return dialogueRecallSnapshot{}, inspectErr
		}
		if reason != "" {
			result.fallback = reason
			return result, nil
		}
	}
	pipelineID, reason, err := u.loadRecallPipeline(ctx, result.head)
	if err != nil {
		return dialogueRecallSnapshot{}, err
	}
	if reason != "" {
		result.fallback = reason
		return result, nil
	}
	result.pipelineID = pipelineID
	result.candidates, err = u.loadRecallCandidates(ctx, residentID, result.head, result.policy)
	if errors.Is(err, errRecallProjectionProvenance) {
		result.fallback = domain.MemoryRecallProjectionProvenanceUnknown
		result.candidates = nil
		return result, nil
	}
	if err != nil {
		return dialogueRecallSnapshot{}, err
	}
	return result, nil
}

func (u *canonicalUoW) inspectRecallProjectionAt(
	ctx context.Context,
	residentID, memoryPolicyID canonical.ID,
	target domain.AssemblyTarget,
	definition projection.Definition,
) (domain.MemoryRecallFallbackReason, error) {
	var version, asOfTZ string
	var sourceCommit, asOf int64
	err := u.tx.QueryRowContext(ctx, `SELECT projection_version, source_commit_seq, as_of, as_of_tz
		FROM projection_watermarks WHERE projection_name = ? AND resident_id = ?`,
		definition.Name, residentID.String(),
	).Scan(&version, &sourceCommit, &asOf, &asOfTZ)
	if errors.Is(err, sql.ErrNoRows) {
		return domain.MemoryRecallProjectionUnavailable, nil
	}
	if err != nil {
		return "", fmt.Errorf("sqlite: inspect Recall Projection %s: %w", definition.Name, err)
	}
	parsedSource, sourceErr := canonical.NewCommitSeq(sourceCommit)
	parsedTZ, timezoneErr := canonical.ParseTimezone(asOfTZ)
	if sourceErr != nil || timezoneErr != nil || version != string(definition.Version) ||
		parsedSource != target.Head.CommitSeq {
		return domain.MemoryRecallProjectionProvenanceUnknown, nil
	}
	if definition.TimeSensitive && (canonical.Instant(asOf) != target.AsOf || parsedTZ != target.AsOfTZ) {
		return domain.MemoryRecallProjectionProvenanceUnknown, nil
	}

	rows, err := u.tx.QueryContext(ctx, `SELECT dependency_kind, dependency_version_id
		FROM projection_watermark_dependencies
		WHERE projection_name = ? AND resident_id = ?
		ORDER BY dependency_kind, dependency_version_id`, definition.Name, residentID.String())
	if err != nil {
		return "", fmt.Errorf("sqlite: inspect Recall Projection dependencies %s: %w", definition.Name, err)
	}
	defer rows.Close()
	type dependencyRow struct{ kind, version string }
	var actual []dependencyRow
	for rows.Next() {
		var row dependencyRow
		if err := rows.Scan(&row.kind, &row.version); err != nil {
			return "", err
		}
		actual = append(actual, row)
	}
	if err := rows.Err(); err != nil {
		return "", err
	}
	expected := make([]dependencyRow, 0, len(definition.Dependencies))
	for _, kind := range definition.Dependencies {
		switch kind {
		case projection.MemoryPolicyDependency:
			expected = append(expected, dependencyRow{kind: string(kind), version: memoryPolicyID.String()})
		default:
			return domain.MemoryRecallProjectionProvenanceUnknown, nil
		}
	}
	if !slices.Equal(actual, expected) {
		return domain.MemoryRecallProjectionProvenanceUnknown, nil
	}
	return "", nil
}

func (u *canonicalUoW) inspectRecallProjection(
	ctx context.Context,
	residentID, memoryPolicyID canonical.ID,
	head canonical.CommitSeq,
	now canonical.Instant,
	definition projection.Definition,
) (domain.MemoryRecallFallbackReason, error) {
	var version, asOfTZ string
	var sourceCommit, asOf int64
	err := u.tx.QueryRowContext(ctx, `SELECT projection_version, source_commit_seq, as_of, as_of_tz
		FROM projection_watermarks WHERE projection_name = ? AND resident_id = ?`,
		definition.Name, residentID.String(),
	).Scan(&version, &sourceCommit, &asOf, &asOfTZ)
	if errors.Is(err, sql.ErrNoRows) {
		return domain.MemoryRecallProjectionUnavailable, nil
	}
	if err != nil {
		return "", fmt.Errorf("sqlite: inspect Recall Projection %s: %w", definition.Name, err)
	}
	parsedSource, sourceErr := canonical.NewCommitSeq(sourceCommit)
	_, timezoneErr := canonical.ParseTimezone(asOfTZ)
	if sourceErr != nil || timezoneErr != nil || version != string(definition.Version) ||
		parsedSource > head || asOf > now.UnixMicro() {
		return domain.MemoryRecallProjectionProvenanceUnknown, nil
	}
	if parsedSource < head || now.UnixMicro()-asOf > recallProjectionMaxStaleness.Microseconds() {
		return domain.MemoryRecallProjectionStale, nil
	}

	rows, err := u.tx.QueryContext(ctx, `SELECT dependency_kind, dependency_version_id
		FROM projection_watermark_dependencies
		WHERE projection_name = ? AND resident_id = ?
		ORDER BY dependency_kind, dependency_version_id`, definition.Name, residentID.String())
	if err != nil {
		return "", fmt.Errorf("sqlite: inspect Recall Projection dependencies %s: %w", definition.Name, err)
	}
	defer rows.Close()
	type dependencyRow struct{ kind, version string }
	var actual []dependencyRow
	for rows.Next() {
		var row dependencyRow
		if err := rows.Scan(&row.kind, &row.version); err != nil {
			return "", err
		}
		actual = append(actual, row)
	}
	if err := rows.Err(); err != nil {
		return "", err
	}
	expected := make([]dependencyRow, 0, len(definition.Dependencies))
	for _, kind := range definition.Dependencies {
		switch kind {
		case projection.MemoryPolicyDependency:
			expected = append(expected, dependencyRow{kind: string(kind), version: memoryPolicyID.String()})
		default:
			return domain.MemoryRecallProjectionProvenanceUnknown, nil
		}
	}
	if !slices.Equal(actual, expected) {
		return domain.MemoryRecallProjectionProvenanceUnknown, nil
	}
	return "", nil
}

func (u *canonicalUoW) loadRecallPipeline(
	ctx context.Context,
	head canonical.CommitSeq,
) (canonical.ID, domain.MemoryRecallFallbackReason, error) {
	var rawID, rawDefinition string
	err := u.tx.QueryRowContext(ctx, `SELECT pipeline.pipeline_version_id, pipeline.definition
		FROM pipeline_versions pipeline
		JOIN canonical_commits commit_row ON commit_row.canonical_commit_id = pipeline.canonical_commit_id
		WHERE pipeline.pipeline_kind = 'memory_recall' AND pipeline.version_key = ?
		  AND commit_row.commit_seq <= ?`, domain.MemoryRecallPipelineVersion, head.Int64(),
	).Scan(&rawID, &rawDefinition)
	if errors.Is(err, sql.ErrNoRows) {
		return canonical.ID{}, domain.MemoryRecallProjectionUnavailable, nil
	}
	if err != nil {
		return canonical.ID{}, "", fmt.Errorf("sqlite: load memory Recall pipeline: %w", err)
	}
	id, err := canonical.ParseID(rawID)
	if err != nil {
		return canonical.ID{}, domain.MemoryRecallProjectionProvenanceUnknown, nil
	}
	want, err := canonical.MarshalCanonical(struct {
		Version string `json:"version"`
	}{Version: domain.MemoryRecallPipelineVersion})
	if err != nil {
		return canonical.ID{}, "", err
	}
	got, err := canonical.ParseCanonicalJSON([]byte(rawDefinition))
	if err != nil || !bytes.Equal(got.Bytes(), want.Bytes()) {
		return canonical.ID{}, domain.MemoryRecallProjectionProvenanceUnknown, nil
	}
	return id, "", nil
}

func (u *canonicalUoW) loadRecallCandidates(
	ctx context.Context,
	residentID canonical.ID,
	head canonical.CommitSeq,
	policy memory.Policy,
) ([]memory.RecallCandidate, error) {
	var invalid int
	if err := u.tx.QueryRowContext(ctx, `SELECT COUNT(*)
		FROM claim_states state
		LEFT JOIN claims claim ON claim.claim_id = state.claim_id
		LEFT JOIN canonical_commits claim_commit ON claim_commit.canonical_commit_id = claim.canonical_commit_id
		WHERE state.resident_id = ?
		  AND (claim.claim_id IS NULL OR claim.owner_resident_id <> state.resident_id
		   OR claim_commit.commit_seq > ?)`, residentID.String(), head.Int64()).Scan(&invalid); err != nil {
		return nil, err
	}
	if invalid != 0 {
		return nil, errRecallProjectionProvenance
	}
	if err := u.tx.QueryRowContext(ctx, `SELECT COUNT(*)
		FROM claim_view_scope_current scope
		LEFT JOIN claims claim ON claim.claim_id = scope.claim_id
		LEFT JOIN canonical_commits claim_commit ON claim_commit.canonical_commit_id = claim.canonical_commit_id
		WHERE scope.resident_id = ?
		  AND (claim.claim_id IS NULL OR claim.owner_resident_id <> scope.resident_id
		   OR claim_commit.commit_seq > ?)`, residentID.String(), head.Int64()).Scan(&invalid); err != nil {
		return nil, err
	}
	if invalid != 0 {
		return nil, errRecallProjectionProvenance
	}
	var canonicalClaims, stateClaims, scopeClaims int
	if err := u.tx.QueryRowContext(ctx, `SELECT
		(SELECT COUNT(*) FROM claims claim
		 JOIN canonical_commits commit_row ON commit_row.canonical_commit_id = claim.canonical_commit_id
		 WHERE claim.owner_resident_id = ? AND commit_row.commit_seq <= ?),
		(SELECT COUNT(*) FROM claim_states WHERE resident_id = ?),
		(SELECT COUNT(*) FROM claim_view_scope_current WHERE resident_id = ?)`,
		residentID.String(), head.Int64(), residentID.String(), residentID.String(),
	).Scan(&canonicalClaims, &stateClaims, &scopeClaims); err != nil {
		return nil, err
	}
	if canonicalClaims != stateClaims || canonicalClaims != scopeClaims {
		return nil, errRecallProjectionProvenance
	}
	if err := u.tx.QueryRowContext(ctx, `SELECT COUNT(*)
		FROM claims claim
		JOIN canonical_commits commit_row ON commit_row.canonical_commit_id = claim.canonical_commit_id
		WHERE claim.owner_resident_id = ? AND commit_row.commit_seq <= ?
		  AND (NOT EXISTS(SELECT 1 FROM claim_states state
		       WHERE state.claim_id = claim.claim_id AND state.resident_id = ?)
		   OR NOT EXISTS(SELECT 1 FROM claim_view_scope_current scope
		       WHERE scope.claim_id = claim.claim_id AND scope.resident_id = ?))`,
		residentID.String(), head.Int64(), residentID.String(), residentID.String(),
	).Scan(&invalid); err != nil {
		return nil, err
	}
	if invalid != 0 {
		return nil, errRecallProjectionProvenance
	}

	rows, err := u.tx.QueryContext(ctx, `SELECT state.claim_id, state.salience,
		state.confidence, state.currentness, state.stage, state.status, state.temporal_relation,
		EXISTS(SELECT 1 FROM claim_relations relation
			JOIN canonical_commits relation_commit
			  ON relation_commit.canonical_commit_id = relation.canonical_commit_id
			WHERE relation.relation_type = 'abstracts'
			  AND relation_commit.commit_seq <= ?
			  AND relation.from_claim_id = state.claim_id)
		FROM claim_states state
		JOIN claim_view_scope_current scope ON scope.claim_id = state.claim_id
		 AND scope.resident_id = state.resident_id
		JOIN claims claim ON claim.claim_id = state.claim_id
		JOIN canonical_commits claim_commit ON claim_commit.canonical_commit_id = claim.canonical_commit_id
		LEFT JOIN content_objects content ON content.content_id = claim.statement_content_id
		WHERE state.resident_id = ? AND claim.owner_resident_id = ?
		  AND state.status = 'active' AND scope.view_scope = 'resident_ui'
		  AND claim_commit.commit_seq <= ? AND claim.statement_hash IS NOT NULL
		  AND (content.content_id IS NULL OR content.erasure_state = 'present')
		ORDER BY state.claim_id`, head.Int64(), residentID.String(), residentID.String(), head.Int64())
	if err != nil {
		return nil, fmt.Errorf("sqlite: stream Recall candidates: %w", err)
	}
	defer rows.Close()
	type rawRecallCandidate struct {
		claimRaw, stageRaw, statusRaw, temporalRaw string
		salience                                   float64
		confidence, currentness                    int64
		abstract                                   int
	}
	var raw []rawRecallCandidate
	for rows.Next() {
		var item rawRecallCandidate
		if err := rows.Scan(
			&item.claimRaw, &item.salience, &item.confidence, &item.currentness,
			&item.stageRaw, &item.statusRaw, &item.temporalRaw, &item.abstract,
		); err != nil {
			return nil, err
		}
		if math.IsNaN(item.salience) || math.IsInf(item.salience, 0) {
			return nil, errRecallProjectionProvenance
		}
		raw = append(raw, item)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	if err := rows.Close(); err != nil {
		return nil, err
	}
	contextCompatibility, _ := canonical.NewRatio(canonical.FixedPointScale)
	ranked := make([]rankedRecallCandidate, 0, domain.MaxRecallCandidates)
	for _, item := range raw {
		claimID, err := canonical.ParseID(item.claimRaw)
		if err != nil {
			return nil, errRecallProjectionProvenance
		}
		eligible, err := loadEligibleClaimStatement(ctx, u.tx, residentID, claimID)
		if err != nil {
			return nil, fmt.Errorf("sqlite: validate Recall candidate %s: %w", claimID, err)
		}
		salienceRatio, err := quantizeProjectionRatio(item.salience)
		if err != nil {
			return nil, errRecallProjectionProvenance
		}
		confidenceRatio, err := canonical.NewRatio(item.confidence)
		if err != nil {
			return nil, errRecallProjectionProvenance
		}
		currentnessRatio, err := canonical.NewRatio(item.currentness)
		if err != nil {
			return nil, errRecallProjectionProvenance
		}
		candidate := memory.RecallCandidate{
			ClaimID: claimID, Statement: string(eligible.Statement), ContextCompatibility: contextCompatibility,
			Salience: salienceRatio, Confidence: confidenceRatio, Currentness: currentnessRatio,
			Stage: memory.ClaimStage(item.stageRaw), Status: memory.ClaimStatus(item.statusRaw),
			TemporalRelation: memory.TemporalRelation(item.temporalRaw),
			Abstract:         item.abstract != 0,
		}
		scored, err := memory.ScoreRecall(policy, candidate, 0)
		if err != nil {
			return nil, errRecallProjectionProvenance
		}
		ranked = retainRankedRecallCandidate(ranked, rankedRecallCandidate{
			candidate: candidate, score: scored.Score,
		}, domain.MaxRecallCandidates)
	}
	result := make([]memory.RecallCandidate, 0, len(ranked))
	for _, item := range ranked {
		sources, err := u.loadRecallEvidenceSources(ctx, item.candidate.ClaimID, residentID, head)
		if err != nil {
			return nil, err
		}
		item.candidate.SourceEventIDs = sources
		item.candidate.LastConfirmed, err = u.loadRecallLastConfirmed(ctx, item.candidate.ClaimID, head)
		if err != nil {
			return nil, err
		}
		result = append(result, item.candidate)
	}
	return result, nil
}

// loadRecallLastConfirmed mirrors the claim-state evaluator's temporal anchor:
// the latest SUPPORT evidence recorded at or below the pinned Target head. A
// contradict-only claim has no confirmation timestamp.
func (u *canonicalUoW) loadRecallLastConfirmed(
	ctx context.Context,
	claimID canonical.ID,
	head canonical.CommitSeq,
) (*canonical.Instant, error) {
	var recordedAt sql.NullInt64
	err := u.tx.QueryRowContext(ctx, `SELECT MAX(evidence.recorded_at)
		FROM claim_evidence evidence
		JOIN canonical_commits evidence_commit
		  ON evidence_commit.canonical_commit_id = evidence.canonical_commit_id
		JOIN events event ON event.event_id = evidence.event_id
		JOIN canonical_commits event_commit
		  ON event_commit.canonical_commit_id = event.canonical_commit_id
		JOIN resident_revisions policy ON policy.revision_id = evidence.memory_policy_revision_id
		JOIN canonical_commits policy_commit
		  ON policy_commit.canonical_commit_id = policy.canonical_commit_id
		WHERE evidence.claim_id = ? AND evidence.polarity = 'support'
		  AND evidence_commit.commit_seq <= ? AND event_commit.commit_seq <= ?
		  AND policy_commit.commit_seq <= ?`,
		claimID.String(), head.Int64(), head.Int64(), head.Int64(),
	).Scan(&recordedAt)
	if err != nil {
		return nil, err
	}
	if !recordedAt.Valid {
		return nil, nil
	}
	value := canonical.Instant(recordedAt.Int64)
	return &value, nil
}

type rankedRecallCandidate struct {
	candidate memory.RecallCandidate
	score     canonical.Ratio
}

func retainRankedRecallCandidate(
	ranked []rankedRecallCandidate,
	candidate rankedRecallCandidate,
	limit int,
) []rankedRecallCandidate {
	if limit < 1 {
		return nil
	}
	insertAt := len(ranked)
	for index, existing := range ranked {
		if candidate.score > existing.score ||
			(candidate.score == existing.score && candidate.candidate.ClaimID.String() < existing.candidate.ClaimID.String()) {
			insertAt = index
			break
		}
	}
	if insertAt >= limit {
		return ranked
	}
	if len(ranked) < limit {
		ranked = append(ranked, rankedRecallCandidate{})
	} else {
		ranked = ranked[:limit]
	}
	copy(ranked[insertAt+1:], ranked[insertAt:len(ranked)-1])
	ranked[insertAt] = candidate
	return ranked
}

func quantizeProjectionRatio(value float64) (canonical.Ratio, error) {
	if math.IsNaN(value) || math.IsInf(value, 0) || value < 0 || value > 1 {
		return 0, errRecallProjectionProvenance
	}
	return canonical.NewRatio(int64(math.Round(value * float64(canonical.FixedPointScale))))
}

func (u *canonicalUoW) loadRecallEvidenceSources(
	ctx context.Context,
	claimID, residentID canonical.ID,
	head canonical.CommitSeq,
) ([]canonical.ID, error) {
	rows, err := u.tx.QueryContext(ctx, `SELECT evidence.event_id, event.resident_id,
		policy.resident_id, policy.revision_class, evidence_commit.commit_seq,
		event_commit.commit_seq, policy_commit.commit_seq
		FROM claim_evidence evidence
		JOIN canonical_commits evidence_commit ON evidence_commit.canonical_commit_id = evidence.canonical_commit_id
		JOIN events event ON event.event_id = evidence.event_id
		JOIN canonical_commits event_commit ON event_commit.canonical_commit_id = event.canonical_commit_id
		JOIN resident_revisions policy ON policy.revision_id = evidence.memory_policy_revision_id
		JOIN canonical_commits policy_commit ON policy_commit.canonical_commit_id = policy.canonical_commit_id
		WHERE evidence.claim_id = ? AND evidence_commit.commit_seq <= ?
		  AND event_commit.commit_seq <= ? AND policy_commit.commit_seq <= ?
		ORDER BY evidence.event_id, evidence.evidence_id`, claimID.String(), head.Int64(), head.Int64(), head.Int64())
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	seen := make(map[canonical.ID]struct{})
	var result []canonical.ID
	for rows.Next() {
		var eventRaw, eventResident, policyResident, policyClass string
		var evidenceCommit, eventCommit, policyCommit int64
		if err := rows.Scan(
			&eventRaw, &eventResident, &policyResident, &policyClass,
			&evidenceCommit, &eventCommit, &policyCommit,
		); err != nil {
			return nil, err
		}
		if eventResident != residentID.String() || policyResident != residentID.String() ||
			policyClass != "memory_policy" || evidenceCommit > head.Int64() ||
			eventCommit > head.Int64() || policyCommit > head.Int64() {
			return nil, errRecallProjectionProvenance
		}
		eventID, err := canonical.ParseID(eventRaw)
		if err != nil {
			return nil, errRecallProjectionProvenance
		}
		if _, exists := seen[eventID]; !exists {
			seen[eventID] = struct{}{}
			result = append(result, eventID)
		}
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	if len(result) == 0 {
		return nil, errRecallProjectionProvenance
	}
	return result, nil
}

func (u *canonicalUoW) revalidateRecallSelection(
	ctx context.Context,
	residentID, memoryPolicyID canonical.ID,
	head canonical.CommitSeq,
	selection memory.RecallSelection,
) error {
	var activePolicy string
	if err := u.tx.QueryRowContext(ctx, `SELECT activation.revision_id
		FROM resident_revision_activations activation
		JOIN resident_revisions revision ON revision.revision_id = activation.revision_id
		JOIN canonical_commits commit_row ON commit_row.canonical_commit_id = activation.canonical_commit_id
		WHERE activation.resident_id = ? AND revision.revision_class = 'memory_policy'
		  AND commit_row.commit_seq <= ?
		ORDER BY commit_row.commit_seq DESC, activation.activation_id DESC LIMIT 1`,
		residentID.String(), head.Int64(),
	).Scan(&activePolicy); err != nil || activePolicy != memoryPolicyID.String() {
		return errRecallProjectionProvenance
	}
	for _, selected := range selection.Selected {
		candidate := selected.Candidate
		var claimCommit int64
		err := u.tx.QueryRowContext(ctx, `SELECT claim_commit.commit_seq
			FROM claims claim
			JOIN canonical_commits claim_commit ON claim_commit.canonical_commit_id = claim.canonical_commit_id
			WHERE claim.claim_id = ?`, candidate.ClaimID.String()).Scan(&claimCommit)
		if err != nil || claimCommit > head.Int64() {
			return errRecallProjectionProvenance
		}
		eligible, err := loadEligibleClaimStatement(ctx, u.tx, residentID, candidate.ClaimID)
		if err != nil {
			if errors.Is(err, domain.ErrClaimSourceIneligible) {
				return errRecallProjectionProvenance
			}
			return fmt.Errorf("sqlite: validate selected Recall claim %s: %w", candidate.ClaimID, err)
		}
		if string(eligible.Statement) != candidate.Statement {
			return errRecallProjectionProvenance
		}
		stage, err := u.canonicalClaimStageAt(ctx, candidate.ClaimID, head)
		if err != nil || stage != candidate.Stage {
			return errRecallProjectionProvenance
		}
		status, err := u.canonicalClaimStatusAt(ctx, candidate.ClaimID, head)
		if err != nil || status != candidate.Status {
			return errRecallProjectionProvenance
		}
		scope, err := u.canonicalClaimScopeAt(ctx, candidate.ClaimID, head)
		if err != nil || scope != memory.ScopeResidentUI {
			return errRecallProjectionProvenance
		}

		var temporal string
		var salience float64
		var confidence, currentness int64
		err = u.tx.QueryRowContext(ctx, `SELECT state.salience, state.confidence,
			state.currentness, state.temporal_relation
			FROM claim_states state
			JOIN claim_view_scope_current scope ON scope.claim_id = state.claim_id
			 AND scope.resident_id = state.resident_id
			WHERE state.claim_id = ? AND state.resident_id = ?`,
			candidate.ClaimID.String(), residentID.String(),
		).Scan(&salience, &confidence, &currentness, &temporal)
		if err != nil || confidence != candidate.Confidence.Millionths() ||
			currentness != candidate.Currentness.Millionths() || temporal != string(candidate.TemporalRelation) {
			return errRecallProjectionProvenance
		}
		quantized, err := quantizeProjectionRatio(salience)
		if err != nil || quantized != candidate.Salience {
			return errRecallProjectionProvenance
		}
		sources, err := u.loadRecallEvidenceSources(ctx, candidate.ClaimID, residentID, head)
		if err != nil || !slices.Equal(sources, candidate.SourceEventIDs) {
			return errRecallProjectionProvenance
		}
		lastConfirmed, err := u.loadRecallLastConfirmed(ctx, candidate.ClaimID, head)
		if err != nil || !sameRecallInstant(lastConfirmed, candidate.LastConfirmed) {
			return errRecallProjectionProvenance
		}
	}
	return nil
}

func sameRecallInstant(left, right *canonical.Instant) bool {
	if left == nil || right == nil {
		return left == nil && right == nil
	}
	return *left == *right
}

func (u *canonicalUoW) canonicalClaimStageAt(
	ctx context.Context,
	claimID canonical.ID,
	head canonical.CommitSeq,
) (memory.ClaimStage, error) {
	rows, err := u.tx.QueryContext(ctx, `SELECT transition.from_stage, transition.to_stage
		FROM claim_stage_transitions transition
		JOIN canonical_commits commit_row ON commit_row.canonical_commit_id = transition.canonical_commit_id
		WHERE transition.claim_id = ? AND commit_row.commit_seq <= ?
		ORDER BY commit_row.commit_seq, transition.stage_transition_id`, claimID.String(), head.Int64())
	if err != nil {
		return "", err
	}
	defer rows.Close()
	var current memory.ClaimStage
	seen := false
	for rows.Next() {
		var from sql.NullString
		var toRaw string
		if err := rows.Scan(&from, &toRaw); err != nil {
			return "", err
		}
		to := memory.ClaimStage(toRaw)
		if err := to.Validate(); err != nil {
			return "", err
		}
		if !seen {
			if from.Valid || to != memory.StageFloating {
				return "", errRecallProjectionProvenance
			}
			current, seen = to, true
			continue
		}
		if !from.Valid || memory.ClaimStage(from.String) != current || !validStageAdvance(current, to) {
			return "", errRecallProjectionProvenance
		}
		current = to
	}
	if err := rows.Err(); err != nil {
		return "", err
	}
	if !seen {
		return "", errRecallProjectionProvenance
	}
	return current, nil
}

func (u *canonicalUoW) canonicalClaimStatusAt(
	ctx context.Context,
	claimID canonical.ID,
	head canonical.CommitSeq,
) (memory.ClaimStatus, error) {
	rows, err := u.tx.QueryContext(ctx, `SELECT transition.from_status, transition.to_status
		FROM claim_status_transitions transition
		JOIN canonical_commits commit_row ON commit_row.canonical_commit_id = transition.canonical_commit_id
		WHERE transition.claim_id = ? AND commit_row.commit_seq <= ?
		ORDER BY commit_row.commit_seq, transition.status_transition_id`, claimID.String(), head.Int64())
	if err != nil {
		return "", err
	}
	defer rows.Close()
	current := memory.StatusActive
	for rows.Next() {
		var fromRaw, toRaw string
		if err := rows.Scan(&fromRaw, &toRaw); err != nil {
			return "", err
		}
		from, to := memory.ClaimStatus(fromRaw), memory.ClaimStatus(toRaw)
		if err := from.Validate(); err != nil {
			return "", err
		}
		if err := to.Validate(); err != nil {
			return "", err
		}
		if from != current || !allowedClaimStatusTransition(from, to) {
			return "", errRecallProjectionProvenance
		}
		current = to
	}
	return current, rows.Err()
}

func (u *canonicalUoW) canonicalClaimScopeAt(
	ctx context.Context,
	claimID canonical.ID,
	head canonical.CommitSeq,
) (memory.ViewScope, error) {
	rows, err := u.tx.QueryContext(ctx, `SELECT assertion.view_scope
		FROM claim_view_scope_assertions assertion
		JOIN canonical_commits commit_row ON commit_row.canonical_commit_id = assertion.canonical_commit_id
		WHERE assertion.claim_id = ? AND commit_row.commit_seq <= ?
		ORDER BY commit_row.commit_seq, assertion.view_scope_assertion_id`, claimID.String(), head.Int64())
	if err != nil {
		return "", err
	}
	defer rows.Close()
	var current memory.ViewScope
	seen := false
	for rows.Next() {
		var raw string
		if err := rows.Scan(&raw); err != nil {
			return "", err
		}
		current = memory.ViewScope(raw)
		if err := current.Validate(); err != nil {
			return "", err
		}
		seen = true
	}
	if err := rows.Err(); err != nil {
		return "", err
	}
	if !seen {
		return "", errRecallProjectionProvenance
	}
	return current, nil
}
