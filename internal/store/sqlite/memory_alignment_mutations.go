package sqlite

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"fmt"

	"mahoroba.local/mahoroba/internal/canonical"
	"mahoroba.local/mahoroba/internal/domain"
	"mahoroba.local/mahoroba/internal/generation"
	"mahoroba.local/mahoroba/internal/memory"
)

func (u *canonicalUoW) LandMemoryAlignment(
	ctx context.Context,
	value domain.LandMemoryAlignment,
) (domain.MemoryAlignmentLandingResult, error) {
	if err := u.requireResidentScope(value.ResidentID); err != nil {
		return domain.MemoryAlignmentLandingResult{}, err
	}
	if err := u.requireActiveResident(ctx, value.ResidentID); err != nil {
		return domain.MemoryAlignmentLandingResult{}, err
	}
	latestAttempt, state, err := u.latestOutcome(ctx, value.RunID, value.ResidentID)
	if err != nil {
		return domain.MemoryAlignmentLandingResult{}, err
	}
	if state != "running" || latestAttempt != value.AttemptNo {
		return domain.MemoryAlignmentLandingResult{}, errors.New("sqlite: alignment landing must terminate latest running attempt")
	}
	if err := u.requireExactMemoryPipeline(ctx, value.AlignmentPipelineVersionID,
		"memory_alignment", domain.MemoryAlignmentPipelineVersion); err != nil {
		return domain.MemoryAlignmentLandingResult{}, err
	}
	if err := u.requireExactMemoryPipeline(ctx, value.MaturationPipelineVersionID,
		"memory_maturation", domain.MemoryMaturationPipelineVersion); err != nil {
		return domain.MemoryAlignmentLandingResult{}, err
	}

	var purpose, key, pipelineRaw, policyRaw, personaRaw, principlesRaw, generatorParamsRaw string
	var sessionPolicy sql.NullString
	if err := u.tx.QueryRowContext(ctx, `SELECT purpose, idempotency_key, pipeline_version_id,
		memory_policy_revision_id, persona_revision_id, principles_revision_id, generator_params,
		sessionization_policy_version_id FROM generation_runs
		WHERE generation_run_id = ? AND resident_id = ?`, value.RunID.String(), value.ResidentID.String()).Scan(
		&purpose, &key, &pipelineRaw, &policyRaw, &personaRaw, &principlesRaw, &generatorParamsRaw, &sessionPolicy,
	); err != nil {
		return domain.MemoryAlignmentLandingResult{}, fmt.Errorf("sqlite: load alignment envelope: %w", err)
	}
	identity, err := domain.ParseMemoryAlignmentObligation(key)
	if err != nil || identity.DirectClaimID != value.DirectClaimID || identity.MetaClaimID != value.MetaClaimID ||
		identity.DirectEvidenceID != value.DirectEvidenceID || identity.MetaEvidenceID != value.MetaEvidenceID ||
		purpose != string(domain.GenerationPurposeMemoryAlignment) ||
		pipelineRaw != value.AlignmentPipelineVersionID.String() ||
		policyRaw != value.MemoryPolicyRevisionID.String() || sessionPolicy.Valid {
		return domain.MemoryAlignmentLandingResult{}, errors.New("sqlite: memory alignment generation envelope mismatch")
	}
	if (identity.Replacement == nil) != (value.Replacement == nil) {
		return domain.MemoryAlignmentLandingResult{}, errors.New("sqlite: memory alignment replacement envelope mismatch")
	}
	if identity.Replacement != nil &&
		(identity.Replacement.OldClaimID != value.Replacement.OldClaimID ||
			identity.Replacement.IntentRelationID != value.Replacement.IntentRelationID) {
		return domain.MemoryAlignmentLandingResult{}, errors.New("sqlite: memory alignment replacement identity mismatch")
	}
	if err := validateAlignmentGeneratorParams([]byte(generatorParamsRaw)); err != nil {
		return domain.MemoryAlignmentLandingResult{}, err
	}
	if err := u.requireActiveRevision(ctx, value.ResidentID, value.MemoryPolicyRevisionID, "memory_policy"); err != nil {
		return domain.MemoryAlignmentLandingResult{}, err
	}
	personaID, err := canonical.ParseID(personaRaw)
	if err != nil {
		return domain.MemoryAlignmentLandingResult{}, err
	}
	if err := u.requireActiveRevision(ctx, value.ResidentID, personaID, "persona"); err != nil {
		return domain.MemoryAlignmentLandingResult{}, err
	}
	principlesID, err := canonical.ParseID(principlesRaw)
	if err != nil {
		return domain.MemoryAlignmentLandingResult{}, err
	}
	if err := u.requireResidentRevision(ctx, value.ResidentID, principlesID, "principles"); err != nil {
		return domain.MemoryAlignmentLandingResult{}, err
	}
	policy, err := u.loadDecisionMemoryPolicy(ctx, value.ResidentID, value.MemoryPolicyRevisionID)
	if err != nil {
		return domain.MemoryAlignmentLandingResult{}, err
	}
	parsed, _, err := memory.ParseAlignmentOutput(value.Output.Bytes)
	if err != nil {
		return domain.MemoryAlignmentLandingResult{}, err
	}

	direct, err := u.loadClaimForMutation(ctx, value.ResidentID, value.DirectClaimID)
	if err != nil {
		return domain.MemoryAlignmentLandingResult{}, err
	}
	meta, err := u.loadClaimForMutation(ctx, value.ResidentID, value.MetaClaimID)
	if err != nil {
		return domain.MemoryAlignmentLandingResult{}, err
	}
	if direct.Kind != memory.ClaimKindDirect || meta.Kind != memory.ClaimKindMeta {
		return domain.MemoryAlignmentLandingResult{}, errors.New("sqlite: alignment claim kinds are not direct/meta")
	}
	for _, claim := range []claimMutationRecord{direct, meta} {
		status, err := u.currentClaimStatus(ctx, claim.ID)
		if err != nil {
			return domain.MemoryAlignmentLandingResult{}, err
		}
		if status != memory.StatusActive {
			return domain.MemoryAlignmentLandingResult{}, errors.New("sqlite: alignment claim is no longer active")
		}
	}
	directStage, err := u.currentClaimStage(ctx, direct.ID)
	if err != nil {
		return domain.MemoryAlignmentLandingResult{}, err
	}
	metaStage, err := u.currentClaimStage(ctx, meta.ID)
	if err != nil {
		return domain.MemoryAlignmentLandingResult{}, err
	}
	if directStage != memory.StageSediment || (metaStage != memory.StageSediment && metaStage != memory.StageSettled) {
		return domain.MemoryAlignmentLandingResult{}, errors.New("sqlite: alignment claim stages changed")
	}
	var oldClaim *claimMutationRecord
	if replacement := value.Replacement; replacement != nil {
		if err := u.requireExactMemoryPipeline(ctx, replacement.StatusPipelineVersionID,
			"memory_status", domain.MemoryStatusPipelineVersion); err != nil {
			return domain.MemoryAlignmentLandingResult{}, err
		}
		loaded, err := u.loadClaimForMutation(ctx, value.ResidentID, replacement.OldClaimID)
		if err != nil {
			return domain.MemoryAlignmentLandingResult{}, err
		}
		if loaded.Kind != memory.ClaimKindDirect || loaded.SubjectID != direct.SubjectID ||
			loaded.PerspectiveID != direct.PerspectiveID {
			return domain.MemoryAlignmentLandingResult{}, errors.New("sqlite: settled direct replacement identity mismatch")
		}
		if err := u.requireMemoryReplacementIntent(ctx, value.RunID, direct.ID, loaded.ID,
			replacement.IntentRelationID); err != nil {
			return domain.MemoryAlignmentLandingResult{}, err
		}
		oldStage, err := u.currentClaimStage(ctx, loaded.ID)
		if err != nil {
			return domain.MemoryAlignmentLandingResult{}, err
		}
		oldStatus, err := u.currentClaimStatus(ctx, loaded.ID)
		if err != nil {
			return domain.MemoryAlignmentLandingResult{}, err
		}
		if oldStage != memory.StageSettled || oldStatus != memory.StatusActive {
			return domain.MemoryAlignmentLandingResult{}, errors.New("sqlite: replacement target must be active settled direct claim")
		}
		oldClaim = &loaded
	}
	if err := u.requireLatestClaimEvidence(ctx, direct.ID, value.DirectEvidenceID); err != nil {
		return domain.MemoryAlignmentLandingResult{}, err
	}
	if err := u.requireLatestClaimEvidence(ctx, meta.ID, value.MetaEvidenceID); err != nil {
		return domain.MemoryAlignmentLandingResult{}, err
	}
	directAggregate, _, err := u.aggregateClaimEvidence(ctx, policy, direct)
	if err != nil {
		return domain.MemoryAlignmentLandingResult{}, err
	}
	metaAggregate, _, err := u.aggregateClaimEvidence(ctx, policy, meta)
	if err != nil {
		return domain.MemoryAlignmentLandingResult{}, err
	}
	preliminary, err := memory.EvaluateMaturation(policy, memory.MaturationInput{
		CurrentStage: directStage, Kind: direct.Kind, Evidence: directAggregate,
	})
	if err != nil {
		return domain.MemoryAlignmentLandingResult{}, err
	}
	if preliminary.Advance || preliminary.BlockingReason != "settled_provenance_gate" {
		return domain.MemoryAlignmentLandingResult{}, errors.New("sqlite: direct claim no longer satisfies pre-alignment settlement gates")
	}
	metaRead := alignmentReadClaim{record: meta, stage: metaStage, aggregate: metaAggregate}
	if !alignmentMetaEligible(policy, metaRead) {
		return domain.MemoryAlignmentLandingResult{}, errors.New("sqlite: meta claim no longer satisfies alignment provenance gates")
	}
	if err := u.validateAlignmentInputs(ctx, value.RunID, value.ResidentID, personaID,
		value.MemoryPolicyRevisionID, direct.ID, meta.ID); err != nil {
		return domain.MemoryAlignmentLandingResult{}, err
	}

	if err := u.insertContent(ctx, value.Output); err != nil {
		return domain.MemoryAlignmentLandingResult{}, err
	}
	if err := u.insertOutcome(ctx, value.OutcomeID, value.RunID, value.AttemptNo, "succeeded", &value.Output.ID,
		value.PromptTokens, value.CompletionTokens, value.LatencyMicros, ""); err != nil {
		return domain.MemoryAlignmentLandingResult{}, err
	}
	result := domain.MemoryAlignmentLandingResult{Aligned: parsed.Aligned, Confidence: parsed.Confidence}
	if !parsed.Aligned || parsed.Confidence.Millionths() < policy.Maturation.AlignmentConfidence {
		return result, nil
	}
	decision, err := memory.EvaluateMaturation(policy, memory.MaturationInput{
		CurrentStage: directStage, Kind: direct.Kind, Evidence: directAggregate,
		Alignment: &memory.AlignmentGate{
			Kind: meta.Kind, Status: memory.StatusActive, Stage: metaStage, Confidence: metaAggregate.Confidence,
			HasExternalSupport:        metaAggregate.HasExternalSupport,
			HasPerspectiveUserSupport: metaAggregate.HasPerspectiveUserSupport,
		},
	})
	if err != nil {
		return domain.MemoryAlignmentLandingResult{}, err
	}
	if !decision.Advance || decision.To != memory.StageSettled || decision.Reason != memory.StageReasonExternalAlignment {
		return domain.MemoryAlignmentLandingResult{}, errors.New("sqlite: qualified alignment did not produce settled transition")
	}
	if err := u.insertClaimStageTransition(ctx, value.StageTransitionID, direct.ID, directStage, decision,
		value.MaturationPipelineVersionID, value.MemoryPolicyRevisionID, value.RunID); err != nil {
		return domain.MemoryAlignmentLandingResult{}, err
	}
	m := u.metadata
	if _, err := u.tx.ExecContext(ctx, `INSERT INTO claim_stage_transition_dependencies(
		stage_transition_dependency_id, canonical_commit_id, stage_transition_id,
		dependency_kind, dependency_claim_id
	) VALUES (?, ?, ?, 'meta_alignment', ?)`, value.StageTransitionDependencyID.String(),
		m.CommitID.String(), value.StageTransitionID.String(), meta.ID.String()); err != nil {
		return domain.MemoryAlignmentLandingResult{}, fmt.Errorf("sqlite: insert memory alignment dependency: %w", err)
	}
	transitionID := value.StageTransitionID
	result.StageTransitionID = &transitionID
	if oldClaim != nil {
		if err := u.applyMemoryAlignmentReplacement(ctx, value, direct, *oldClaim); err != nil {
			return domain.MemoryAlignmentLandingResult{}, err
		}
		result.ReplacementApplied = true
	}
	return result, nil
}

func (u *canonicalUoW) requireMemoryReplacementIntent(
	ctx context.Context,
	runID, replacementClaimID, oldClaimID, intentRelationID canonical.ID,
) error {
	var fromRaw, toRaw, relationType, reasonCode string
	var intentSeq, runSeq int64
	if err := u.tx.QueryRowContext(ctx, `SELECT relation.from_claim_id, relation.to_claim_id,
		relation.relation_type, relation.reason_code, intent_commit.commit_seq, run_commit.commit_seq
		FROM claim_relations relation
		JOIN canonical_commits intent_commit ON intent_commit.canonical_commit_id = relation.canonical_commit_id
		JOIN generation_runs run ON run.generation_run_id = ?
		JOIN canonical_commits run_commit ON run_commit.canonical_commit_id = run.canonical_commit_id
		WHERE relation.claim_relation_id = ?`, runID.String(), intentRelationID.String()).Scan(
		&fromRaw, &toRaw, &relationType, &reasonCode, &intentSeq, &runSeq,
	); err != nil {
		return fmt.Errorf("sqlite: load memory replacement intent: %w", err)
	}
	if fromRaw != replacementClaimID.String() || toRaw != oldClaimID.String() ||
		relationType != string(memory.RelationContradicts) ||
		reasonCode != string(memory.RelationReasonReplacementIntent) {
		return errors.New("sqlite: memory replacement intent relation mismatch")
	}
	if intentSeq > runSeq {
		return errors.New("sqlite: memory replacement intent was not fixed before prepare")
	}
	return nil
}

func (u *canonicalUoW) applyMemoryAlignmentReplacement(
	ctx context.Context,
	value domain.LandMemoryAlignment,
	replacementClaim, oldClaim claimMutationRecord,
) error {
	replacement := value.Replacement
	if replacement == nil {
		return errors.New("sqlite: missing memory alignment replacement contract")
	}
	m := u.metadata
	if _, err := u.tx.ExecContext(ctx, `INSERT INTO claim_relations(
		claim_relation_id, canonical_commit_id, from_claim_id, to_claim_id,
		relation_type, reason_code, reason_content_id, generation_run_id,
		occurred_at, occurred_tz, recorded_at, recorded_tz
	) VALUES (?, ?, ?, ?, 'supersedes', 'explicit_supersession', NULL, ?, ?, ?, ?, ?)`,
		replacement.RelationID.String(), m.CommitID.String(), replacementClaim.ID.String(), oldClaim.ID.String(),
		value.RunID.String(), m.CommittedAt.UnixMicro(), m.CommittedTZ.String(),
		m.CommittedAt.UnixMicro(), m.CommittedTZ.String(),
	); err != nil {
		return fmt.Errorf("sqlite: insert settled replacement relation: %w", err)
	}
	metrics, err := canonical.MarshalCanonical(struct {
		ReplacementClaimID  canonical.ID `json:"replacement_claim_id"`
		SettledTransitionID canonical.ID `json:"settled_transition_id"`
		RelationID          canonical.ID `json:"relation_id"`
		AtomicReplacement   bool         `json:"atomic_replacement"`
	}{replacementClaim.ID, value.StageTransitionID, replacement.RelationID, true})
	if err != nil {
		return err
	}
	policyID := value.MemoryPolicyRevisionID
	if err := u.insertAutomaticStatusTransition(ctx, replacement.OldStatusTransitionID, oldClaim.ID,
		memory.StatusActive, memory.StatusSuperseded, memory.AutomaticReasonExplicitSupersession,
		domain.ClaimTriggerRelation, replacement.RelationID, replacement.StatusPipelineVersionID,
		&policyID, metrics); err != nil {
		return err
	}
	return nil
}

func (u *canonicalUoW) requireLatestClaimEvidence(ctx context.Context, claimID, expected canonical.ID) error {
	var actual string
	if err := u.tx.QueryRowContext(ctx, `SELECT evidence.evidence_id FROM claim_evidence evidence
		JOIN canonical_commits commit_row ON commit_row.canonical_commit_id = evidence.canonical_commit_id
		WHERE evidence.claim_id = ?
		ORDER BY commit_row.commit_seq DESC, evidence.evidence_id DESC LIMIT 1`, claimID.String()).Scan(&actual); err != nil {
		return fmt.Errorf("sqlite: resolve latest claim evidence: %w", err)
	}
	if actual != expected.String() {
		return errors.New("sqlite: claim evidence changed after alignment prepare")
	}
	return nil
}

func (u *canonicalUoW) validateAlignmentInputs(
	ctx context.Context,
	runID, residentID, personaID, policyID, directID, metaID canonical.ID,
) error {
	expectedTypes := []string{"resident_revision", "resident_revision", "claim", "claim"}
	expectedRoles := []string{"system", "system", "user", "user"}
	expectedInclusions := []string{"resident_definition", "resident_definition", "memory_recall", "memory_recall"}
	expectedIDs := []canonical.ID{personaID, policyID, directID, metaID}
	rows, err := u.tx.QueryContext(ctx, `SELECT input.ordinal, input.role, input.source_type, input.source_id,
		input.inclusion_mode,
		blob.content FROM generation_run_inputs input
		JOIN content_objects content ON content.content_id = input.content_id
		LEFT JOIN blobs blob ON blob.dedupe_scope_id = content.owner_resident_id
		 AND blob.hash_algorithm = content.blob_hash_algorithm AND blob.blob_hash = content.blob_hash
		WHERE input.generation_run_id = ? ORDER BY input.ordinal`, runID.String())
	if err != nil {
		return err
	}
	defer rows.Close()
	index := 0
	for rows.Next() {
		var ordinal int64
		var role, sourceType, sourceRaw, inclusion string
		var inputContent []byte
		if err := rows.Scan(&ordinal, &role, &sourceType, &sourceRaw, &inclusion, &inputContent); err != nil {
			return err
		}
		if index >= len(expectedIDs) || ordinal != int64(index) || role != expectedRoles[index] ||
			sourceType != expectedTypes[index] || inclusion != expectedInclusions[index] ||
			sourceRaw != expectedIDs[index].String() || inputContent == nil {
			return errors.New("sqlite: memory alignment input identity/order mismatch")
		}
		var sourceContent []byte
		if sourceType == "resident_revision" {
			err = u.tx.QueryRowContext(ctx, `SELECT blob.content FROM resident_revisions revision
				JOIN content_objects content ON content.content_id = revision.content_id
				LEFT JOIN blobs blob ON blob.dedupe_scope_id = content.owner_resident_id
				 AND blob.hash_algorithm = content.blob_hash_algorithm AND blob.blob_hash = content.blob_hash
				WHERE revision.revision_id = ? AND revision.resident_id = ?`, sourceRaw, residentID.String()).Scan(&sourceContent)
		} else {
			claimID, parseErr := canonical.ParseID(sourceRaw)
			if parseErr != nil {
				return parseErr
			}
			eligible, eligibleErr := loadEligibleClaimStatement(ctx, u.tx, residentID, claimID)
			if eligibleErr != nil {
				return eligibleErr
			}
			sourceContent = eligible.Statement
		}
		if sourceType == "claim" && (errors.Is(err, sql.ErrNoRows) || sourceContent == nil) {
			return fmt.Errorf("%w: memory alignment claim source %s is erased or ineligible", domain.ErrClaimSourceIneligible, sourceRaw)
		}
		if err != nil || sourceContent == nil || !bytes.Equal(inputContent, sourceContent) {
			return errors.New("sqlite: memory alignment input content/source mismatch")
		}
		index++
	}
	if err := rows.Err(); err != nil {
		return err
	}
	if index != len(expectedIDs) {
		return errors.New("sqlite: memory alignment requires exactly persona, policy, direct, and meta inputs")
	}
	return nil
}

func validateAlignmentGeneratorParams(raw []byte) error {
	params, _, err := domain.ParseGeneratorParams(raw)
	if err != nil {
		return fmt.Errorf("sqlite: parse alignment generator params: %w", err)
	}
	if err := domain.ValidateGeneratorParamsForPurpose(domain.GenerationPurposeMemoryAlignment, params); err != nil {
		return err
	}
	if params.Streaming || params.SchemaHash == nil {
		return errors.New("sqlite: alignment generation must use non-streaming structured output")
	}
	schema, err := memory.AlignmentJSONSchema()
	if err != nil {
		return err
	}
	contract, err := generation.NewSchemaContract(
		"memory_alignment", memory.AlignmentOutputVersionV1, params.StructuredOutputMode, schema,
	)
	if err != nil {
		return err
	}
	if params.SchemaVersion != contract.Version || *params.SchemaHash != contract.Hash {
		return errors.New("sqlite: alignment generator schema version/hash mismatch")
	}
	return nil
}
