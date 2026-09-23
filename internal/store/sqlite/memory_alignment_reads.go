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

const maximumAlignmentCandidates = 64

type alignmentReadClaim struct {
	record           claimMutationRecord
	latestEvidenceID canonical.ID
	statement        string
	stage            memory.ClaimStage
	aggregate        memory.EvidenceAggregate
}

func (r *CanonicalRepository) DiscoverMemoryAlignmentWork(
	ctx context.Context,
	residentID canonical.ID,
	limit, maxAttempts int,
) (*domain.MemoryAlignmentWork, error) {
	if err := residentID.Validate(); err != nil {
		return nil, err
	}
	if limit < 1 || maxAttempts < 1 {
		return nil, errors.New("sqlite: alignment limits must be positive")
	}
	resident, err := r.Resident(ctx, residentID)
	if err != nil {
		return nil, err
	}
	if resident.Status != "active" {
		return nil, nil
	}
	policy, _, err := memory.ParsePolicy([]byte(resident.MemoryPolicy))
	if err != nil {
		return nil, fmt.Errorf("sqlite: parse active alignment policy: %w", err)
	}
	if err := policy.RequireEnabled(); err != nil {
		return nil, nil
	}
	tx, err := r.store.reader.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return nil, fmt.Errorf("sqlite: begin alignment read snapshot: %w", err)
	}
	defer tx.Rollback()

	if err := requireSnapshotActiveRevision(ctx, tx, residentID, resident.MemoryPolicyRevisionID, "memory_policy"); err != nil {
		return nil, err
	}
	alignmentPipeline, err := loadSnapshotPipeline(ctx, tx, "memory_alignment", domain.MemoryAlignmentPipelineVersion)
	if err != nil {
		return nil, err
	}
	maturationPipeline, err := loadSnapshotPipeline(ctx, tx, "memory_maturation", domain.MemoryMaturationPipelineVersion)
	if err != nil {
		return nil, err
	}

	reader := &canonicalUoW{tx: tx}
	directs, err := loadAlignmentClaims(ctx, reader, residentID, memory.ClaimKindDirect, maximumAlignmentCandidates, policy)
	if err != nil {
		return nil, err
	}
	metas, err := loadAlignmentClaims(ctx, reader, residentID, memory.ClaimKindMeta, maximumAlignmentCandidates, policy)
	if err != nil {
		return nil, err
	}
	checked := 0
	for _, direct := range directs {
		decision, err := memory.EvaluateMaturation(policy, memory.MaturationInput{
			CurrentStage: direct.stage, Kind: direct.record.Kind, Evidence: direct.aggregate,
		})
		if err != nil {
			return nil, err
		}
		// Every numerical and non-alignment provenance threshold passed iff the
		// only remaining blocker is the direct claim's external alignment gate.
		if decision.Advance || decision.BlockingReason != "settled_provenance_gate" {
			continue
		}
		replacement, err := loadAlignmentReplacementIntent(ctx, reader, direct.record)
		if err != nil {
			return nil, err
		}
		if replacement != nil {
			replacement.StatusPipelineVersionID, err = loadSnapshotPipeline(
				ctx, tx, "memory_status", domain.MemoryStatusPipelineVersion,
			)
			if err != nil {
				return nil, err
			}
		}
		for _, meta := range metas {
			if checked == limit {
				if err := tx.Commit(); err != nil {
					return nil, err
				}
				return nil, nil
			}
			checked++
			if !alignmentMetaEligible(policy, meta) {
				continue
			}
			key := domain.MemoryAlignmentObligation(
				direct.record.ID, meta.record.ID, direct.latestEvidenceID, meta.latestEvidenceID,
			)
			if replacement != nil {
				key = domain.MemoryAlignmentReplacementObligation(
					direct.record.ID, meta.record.ID, direct.latestEvidenceID, meta.latestEvidenceID,
					replacement.OldClaimID, replacement.IntentRelationID,
				)
			}
			work := &domain.MemoryAlignmentWork{
				Resident: resident, PolicyContent: resident.MemoryPolicy,
				AlignmentPipelineVersionID:  alignmentPipeline,
				MaturationPipelineVersionID: maturationPipeline,
				Direct:                      domain.MemoryAlignmentClaim{ClaimID: direct.record.ID, LatestEvidenceID: direct.latestEvidenceID, Statement: direct.statement},
				Meta:                        domain.MemoryAlignmentClaim{ClaimID: meta.record.ID, LatestEvidenceID: meta.latestEvidenceID, Statement: meta.statement},
				IdempotencyKey:              key, State: domain.WorkPending,
				Replacement: replacement,
			}
			needsAttention, err := classifyAlignmentRun(ctx, tx, work, maxAttempts)
			if err != nil {
				return nil, err
			}
			if needsAttention {
				if err := tx.Commit(); err != nil {
					return nil, err
				}
				return work, nil
			}
		}
	}
	if err := tx.Commit(); err != nil {
		return nil, err
	}
	return nil, nil
}

func loadAlignmentReplacementIntent(
	ctx context.Context,
	reader *canonicalUoW,
	direct claimMutationRecord,
) (*domain.MemoryAlignmentReplacementWork, error) {
	rows, err := reader.tx.QueryContext(ctx, `SELECT relation.claim_relation_id, relation.to_claim_id
		FROM claim_relations relation
		JOIN canonical_commits commit_row ON commit_row.canonical_commit_id = relation.canonical_commit_id
		WHERE relation.from_claim_id = ? AND relation.relation_type = ?
		  AND relation.reason_code = ?
		ORDER BY commit_row.commit_seq, relation.claim_relation_id LIMIT 2`, direct.ID.String(),
		string(memory.RelationContradicts), string(memory.RelationReasonReplacementIntent))
	if err != nil {
		return nil, fmt.Errorf("sqlite: discover alignment replacement intent: %w", err)
	}
	defer rows.Close()
	type marker struct{ relationRaw, oldRaw string }
	markers := make([]marker, 0, 2)
	for rows.Next() {
		var item marker
		if err := rows.Scan(&item.relationRaw, &item.oldRaw); err != nil {
			return nil, err
		}
		markers = append(markers, item)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	if len(markers) == 0 {
		return nil, nil
	}
	if len(markers) != 1 {
		return nil, errors.New("sqlite: direct claim has multiple replacement intents")
	}
	oldID, err := canonical.ParseID(markers[0].oldRaw)
	if err != nil {
		return nil, err
	}
	intentID, err := canonical.ParseID(markers[0].relationRaw)
	if err != nil {
		return nil, err
	}
	old, err := reader.loadClaimForMutation(ctx, direct.ResidentID, oldID)
	if err != nil {
		return nil, fmt.Errorf("sqlite: load alignment replacement target: %w", err)
	}
	if old.Kind != memory.ClaimKindDirect || old.ResidentID != direct.ResidentID ||
		old.SubjectID != direct.SubjectID || old.PerspectiveID != direct.PerspectiveID {
		return nil, errors.New("sqlite: replacement intent target identity mismatch")
	}
	oldStage, err := reader.currentClaimStage(ctx, old.ID)
	if err != nil {
		return nil, err
	}
	oldStatus, err := reader.currentClaimStatus(ctx, old.ID)
	if err != nil {
		return nil, err
	}
	if oldStage != memory.StageSettled || oldStatus != memory.StatusActive {
		return nil, errors.New("sqlite: replacement intent target requires human decision")
	}
	return &domain.MemoryAlignmentReplacementWork{
		OldClaimID: old.ID, IntentRelationID: intentID,
	}, nil
}

func loadAlignmentClaims(
	ctx context.Context,
	reader *canonicalUoW,
	residentID canonical.ID,
	kind memory.ClaimKind,
	limit int,
	policy memory.Policy,
) ([]alignmentReadClaim, error) {
	rows, err := reader.tx.QueryContext(ctx, `SELECT claim.claim_id,
		(SELECT evidence.evidence_id FROM claim_evidence evidence
		 JOIN canonical_commits evidence_commit ON evidence_commit.canonical_commit_id = evidence.canonical_commit_id
		 WHERE evidence.claim_id = claim.claim_id
		 ORDER BY evidence_commit.commit_seq DESC, evidence.evidence_id DESC LIMIT 1)
		FROM claims claim
		LEFT JOIN content_objects content ON content.content_id = claim.statement_content_id
		WHERE claim.owner_resident_id = ? AND claim.kind = ?
		  AND claim.statement_hash IS NOT NULL
		  AND (content.content_id IS NULL OR content.erasure_state = 'present')
			 AND COALESCE((SELECT status.to_status FROM claim_status_transitions status
		  JOIN canonical_commits status_commit ON status_commit.canonical_commit_id = status.canonical_commit_id
		  WHERE status.claim_id = claim.claim_id
		  ORDER BY status_commit.commit_seq DESC, status.status_transition_id DESC LIMIT 1), 'active') = 'active'
		ORDER BY claim.claim_id LIMIT ?`, residentID.String(), string(kind), limit)
	if err != nil {
		return nil, fmt.Errorf("sqlite: discover %s alignment claims: %w", kind, err)
	}
	defer rows.Close()
	type rawClaim struct {
		idRaw       string
		evidenceRaw sql.NullString
	}
	var raw []rawClaim
	for rows.Next() {
		var item rawClaim
		if err := rows.Scan(&item.idRaw, &item.evidenceRaw); err != nil {
			return nil, err
		}
		raw = append(raw, item)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	if err := rows.Close(); err != nil {
		return nil, err
	}
	result := make([]alignmentReadClaim, 0, len(raw))
	for _, item := range raw {
		if !item.evidenceRaw.Valid {
			continue
		}
		claimID, err := canonical.ParseID(item.idRaw)
		if err != nil {
			return nil, err
		}
		eligible, err := loadEligibleClaimStatement(ctx, reader.tx, residentID, claimID)
		if err != nil {
			return nil, fmt.Errorf("sqlite: validate %s alignment claim %s: %w", kind, claimID, err)
		}
		evidenceID, err := canonical.ParseID(item.evidenceRaw.String)
		if err != nil {
			return nil, err
		}
		record, err := reader.loadClaimForMutation(ctx, residentID, claimID)
		if err != nil {
			return nil, err
		}
		stage, err := reader.currentClaimStage(ctx, claimID)
		if err != nil {
			return nil, err
		}
		if kind == memory.ClaimKindDirect && stage != memory.StageSediment {
			continue
		}
		if kind == memory.ClaimKindMeta && stage != memory.StageSediment && stage != memory.StageSettled {
			continue
		}
		aggregate, _, err := reader.aggregateClaimEvidence(ctx, policy, record)
		if err != nil {
			return nil, err
		}
		result = append(result, alignmentReadClaim{
			record: record, latestEvidenceID: evidenceID, statement: string(eligible.Statement),
			stage: stage, aggregate: aggregate,
		})
	}
	return result, nil
}

func alignmentMetaEligible(policy memory.Policy, claim alignmentReadClaim) bool {
	return claim.record.Kind == memory.ClaimKindMeta &&
		(claim.stage == memory.StageSediment || claim.stage == memory.StageSettled) &&
		claim.aggregate.Confidence.Millionths() >= policy.Maturation.AlignmentConfidence &&
		claim.aggregate.HasExternalSupport && claim.aggregate.HasPerspectiveUserSupport
}

func classifyAlignmentRun(ctx context.Context, tx *sql.Tx, work *domain.MemoryAlignmentWork, maxAttempts int) (bool, error) {
	var runRaw string
	err := tx.QueryRowContext(ctx, `SELECT generation_run_id FROM generation_runs
		WHERE resident_id = ? AND idempotency_key = ?`, work.Resident.ResidentID.String(), work.IdempotencyKey).Scan(&runRaw)
	if errors.Is(err, sql.ErrNoRows) {
		return true, nil
	}
	if err != nil {
		return false, err
	}
	runID, err := canonical.ParseID(runRaw)
	if err != nil {
		return false, err
	}
	work.RunID = &runID
	runSummary, err := (generationOutcomeRepository{}).Summary(ctx, tx, runID, work.Resident.ResidentID)
	if err != nil {
		return false, err
	}
	runSummary, err = runSummary.Classify(int64(maxAttempts))
	if err != nil {
		return false, err
	}
	work.AttemptNo = runSummary.Latest.AttemptNo
	work.RetryCount = runSummary.RetryCount
	switch runSummary.ClassifiedState {
	case domain.WorkRunning:
		work.State = domain.WorkRunning
		return true, nil
	case domain.WorkSucceeded:
		return false, nil
	case domain.WorkRetryPending:
		if runSummary.LatestForegroundPreempted {
			work.ForegroundPreempted = true
			work.State = domain.WorkRetryPending
			return true, nil
		}
		if runSummary.RetryEligible {
			work.State = domain.WorkRetryPending
			return true, nil
		}
		return false, nil
	case domain.WorkTerminalFailed:
		return false, nil
	default:
		return false, fmt.Errorf("sqlite: unknown memory alignment outcome %q", runSummary.PersistedState)
	}
}

func loadSnapshotPipeline(ctx context.Context, tx *sql.Tx, kind, version string) (canonical.ID, error) {
	var rawID, rawDefinition string
	if err := tx.QueryRowContext(ctx, `SELECT pipeline_version_id, definition FROM pipeline_versions
		WHERE pipeline_kind = ? AND version_key = ?`, kind, version).Scan(&rawID, &rawDefinition); err != nil {
		return canonical.ID{}, fmt.Errorf("sqlite: resolve %s pipeline: %w", kind, err)
	}
	expected, err := canonical.MarshalCanonical(struct {
		Version string `json:"version"`
	}{Version: version})
	if err != nil || rawDefinition != expected.String() {
		return canonical.ID{}, fmt.Errorf("sqlite: %s pipeline definition mismatch", kind)
	}
	return canonical.ParseID(rawID)
}

func requireSnapshotActiveRevision(ctx context.Context, tx *sql.Tx, residentID, revisionID canonical.ID, class string) error {
	var active string
	if err := tx.QueryRowContext(ctx, `SELECT activation.revision_id
		FROM resident_revision_activations activation
		JOIN resident_revisions revision ON revision.revision_id = activation.revision_id
		JOIN canonical_commits commit_row ON commit_row.canonical_commit_id = activation.canonical_commit_id
		WHERE activation.resident_id = ? AND revision.revision_class = ?
		ORDER BY commit_row.commit_seq DESC, activation.activation_id DESC LIMIT 1`, residentID.String(), class).Scan(&active); err != nil {
		return fmt.Errorf("sqlite: resolve active %s revision: %w", class, err)
	}
	if active != revisionID.String() {
		return fmt.Errorf("sqlite: %s revision changed during alignment discovery", class)
	}
	return nil
}

var _ domain.MemoryAlignmentWorkRepository = (*CanonicalRepository)(nil)
