package sqlite

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"slices"

	"mahoroba.local/mahoroba/internal/canonical"
	"mahoroba.local/mahoroba/internal/domain"
	"mahoroba.local/mahoroba/internal/integrity"
	"mahoroba.local/mahoroba/internal/memory"
)

const integrityPipelineDefinitionV1 = `{"version":"integrity-check-v1"}`
const memoryStatusPipelineDefinitionV1 = `{"version":"memory-status-v1"}`

func (u *canonicalUoW) RecordIntegrityFindings(
	ctx context.Context,
	value integrity.RecordFindings,
) (integrity.RecordFindingsResult, error) {
	if err := value.Validate(); err != nil {
		return integrity.RecordFindingsResult{}, err
	}
	residentID, scoped := u.metadata.Scope.ResidentID()
	if !scoped || residentID != value.ResidentID {
		return integrity.RecordFindingsResult{}, errors.New("sqlite: integrity finding recording requires matching resident scope")
	}
	if value.CapturedHead.Exists && value.CapturedHead.CommitSeq >= u.metadata.CommitSeq {
		return integrity.RecordFindingsResult{}, fmt.Errorf("%w: captured head is not before provisional finding commit", integrity.ErrFindingConflict)
	}
	if err := u.requireIntegrityPipeline(ctx, value.PipelineVersionID); err != nil {
		return integrity.RecordFindingsResult{}, err
	}
	if value.MemoryStatusPipelineVersionID != nil {
		if err := u.requireMemoryStatusPipeline(ctx, *value.MemoryStatusPipelineVersionID); err != nil {
			return integrity.RecordFindingsResult{}, err
		}
	}

	planned := slices.Clone(value.Findings)
	slices.SortFunc(planned, func(left, right integrity.PlannedFinding) int {
		if left.Candidate.Fingerprint.Hex() < right.Candidate.Fingerprint.Hex() {
			return -1
		}
		if left.Candidate.Fingerprint.Hex() > right.Candidate.Fingerprint.Hex() {
			return 1
		}
		return 0
	})
	for index := 1; index < len(planned); index++ {
		if planned[index-1].Candidate.Fingerprint == planned[index].Candidate.Fingerprint {
			return integrity.RecordFindingsResult{}, fmt.Errorf("%w: duplicate candidate fingerprint in batch", integrity.ErrInvalidCandidate)
		}
	}

	type classifiedFinding struct {
		planned   integrity.PlannedFinding
		findingID canonical.ID
		active    bool
	}
	result := integrity.RecordFindingsResult{}
	missing := make([]integrity.PlannedFinding, 0, len(planned))
	classified := make([]classifiedFinding, 0, len(planned))
	for _, finding := range planned {
		if err := u.revalidateIntegrityCandidate(ctx, finding.Candidate); err != nil {
			return integrity.RecordFindingsResult{}, err
		}
		existingID, exists, err := u.classifyIntegrityFinding(ctx, finding.Candidate)
		if err != nil {
			return integrity.RecordFindingsResult{}, err
		}
		if exists {
			commit, err := u.integrityFindingCommit(ctx, existingID)
			if err != nil {
				return integrity.RecordFindingsResult{}, err
			}
			result.Existing = append(result.Existing, integrity.RecordedFinding{
				Fingerprint: finding.Candidate.Fingerprint, FindingID: existingID, Commit: commit,
			})
		} else {
			missing = append(missing, finding)
		}
		findingID := finding.FindingID
		if exists {
			findingID = existingID
		}
		entry := classifiedFinding{planned: finding, findingID: findingID}
		if isStructuralClaimRule(finding.Candidate.RuleCode) {
			status, err := u.currentClaimStatus(ctx, finding.Candidate.TargetID)
			if err != nil {
				return integrity.RecordFindingsResult{}, fmt.Errorf("%w: load current claim status: %v", integrity.ErrQuarantineConflict, err)
			}
			entry.active = status == memory.StatusActive
		}
		classified = append(classified, entry)
	}

	for _, finding := range missing {
		candidate := finding.Candidate
		var claimID, sourceEventID any
		if candidate.ClaimID != nil {
			claimID = candidate.ClaimID.String()
		}
		if candidate.SourceContentErasureEventID != nil {
			sourceEventID = candidate.SourceContentErasureEventID.String()
		}
		if _, err := u.tx.ExecContext(ctx, `INSERT INTO integrity_findings(
			integrity_finding_id, canonical_commit_id, resident_id, claim_id, finding_kind,
			source_content_erasure_event_id, pipeline_version_id, details_content_id,
			occurred_at, occurred_tz, recorded_at, recorded_tz,
			finding_fingerprint, rule_code, target_kind, target_id, target_field
		) VALUES (?, ?, ?, ?, ?, ?, ?, NULL, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
			finding.FindingID.String(), u.metadata.CommitID.String(), value.ResidentID.String(), claimID,
			string(candidate.Kind), sourceEventID, value.PipelineVersionID.String(),
			candidate.OccurredAt.UnixMicro(), candidate.OccurredTZ.String(),
			u.metadata.CommittedAt.UnixMicro(), u.metadata.CommittedTZ.String(),
			candidate.Fingerprint.Bytes(), string(candidate.RuleCode), string(candidate.TargetKind),
			candidate.TargetID.String(), candidate.TargetField,
		); err != nil {
			return integrity.RecordFindingsResult{}, fmt.Errorf("sqlite: insert integrity finding: %w", err)
		}
		result.Created = append(result.Created, integrity.RecordedFinding{
			Fingerprint: candidate.Fingerprint, FindingID: finding.FindingID, Created: true,
			Commit: u.metadata,
		})
	}
	quarantinedClaims := make(map[canonical.ID]struct{})
	for _, entry := range classified {
		if !entry.active {
			continue
		}
		if _, alreadyQuarantined := quarantinedClaims[entry.planned.Candidate.TargetID]; alreadyQuarantined {
			continue
		}
		if entry.planned.QuarantineTransitionID == nil || value.MemoryStatusPipelineVersionID == nil {
			return integrity.RecordFindingsResult{}, fmt.Errorf("%w: active structural claim lacks a complete quarantine plan", integrity.ErrQuarantineConflict)
		}
		_, err := u.AutomaticClaimStatusDecision(ctx, domain.AutomaticClaimStatusDecision{
			ResidentID: value.ResidentID, ClaimID: entry.planned.Candidate.TargetID,
			StatusTransitionID:      *entry.planned.QuarantineTransitionID,
			ToStatus:                memory.StatusQuarantined,
			Reason:                  memory.AutomaticReasonStructuralQuarantine,
			TriggerKind:             domain.ClaimTriggerIntegrityFinding,
			TriggerID:               entry.findingID,
			StatusPipelineVersionID: *value.MemoryStatusPipelineVersionID,
		})
		if err != nil {
			return integrity.RecordFindingsResult{}, fmt.Errorf("%w: apply claim quarantine: %v", integrity.ErrQuarantineConflict, err)
		}
		result.Quarantined = append(result.Quarantined, integrity.RecordedQuarantine{
			ClaimID: entry.planned.Candidate.TargetID, FindingID: entry.findingID,
			StatusTransitionID: *entry.planned.QuarantineTransitionID,
		})
		quarantinedClaims[entry.planned.Candidate.TargetID] = struct{}{}
	}
	if len(missing) == 0 && len(result.Quarantined) == 0 {
		return result, canonical.ErrNoMutation
	}
	return result, nil
}

func (u *canonicalUoW) integrityFindingCommit(
	ctx context.Context,
	findingID canonical.ID,
) (canonical.CommitMetadata, error) {
	row := u.tx.QueryRowContext(ctx, `SELECT
		canonical_commit.canonical_commit_id, canonical_commit.commit_seq, canonical_commit.resident_id,
		canonical_commit.committed_at, canonical_commit.committed_tz
		FROM integrity_findings finding
		JOIN canonical_commits canonical_commit
		  ON canonical_commit.canonical_commit_id = finding.canonical_commit_id
		WHERE finding.integrity_finding_id = ?`, findingID.String())
	metadata, err := scanIntegrityCommitMetadata(row)
	if errors.Is(err, sql.ErrNoRows) {
		return canonical.CommitMetadata{}, fmt.Errorf("%w: existing finding commit is missing", integrity.ErrFindingConflict)
	}
	if err != nil {
		return canonical.CommitMetadata{}, fmt.Errorf("sqlite: resolve existing finding commit: %w", err)
	}
	return metadata, nil
}

func isStructuralClaimRule(rule integrity.RuleCode) bool {
	switch rule {
	case integrity.RuleClaimStatementErased,
		integrity.RuleClaimQualifyingSupportErased,
		integrity.RuleClaimQualifyingSupportUnresolvable:
		return true
	default:
		return false
	}
}

func (u *canonicalUoW) requireIntegrityPipeline(ctx context.Context, pipelineID canonical.ID) error {
	var kind, version, definition string
	if err := u.tx.QueryRowContext(ctx, `SELECT pipeline_kind, version_key, definition
		FROM pipeline_versions WHERE pipeline_version_id = ?`, pipelineID.String()).Scan(&kind, &version, &definition); err != nil {
		return fmt.Errorf("sqlite: load integrity pipeline: %w", err)
	}
	if kind != integrity.IntegrityPipelineKind || version != integrity.IntegrityPipelineVersion ||
		definition != integrityPipelineDefinitionV1 {
		return fmt.Errorf("%w: pipeline is not exact integrity-check-v1", integrity.ErrFindingConflict)
	}
	return nil
}

func (u *canonicalUoW) requireMemoryStatusPipeline(ctx context.Context, pipelineID canonical.ID) error {
	var kind, version, definition string
	if err := u.tx.QueryRowContext(ctx, `SELECT pipeline_kind, version_key, definition
		FROM pipeline_versions WHERE pipeline_version_id = ?`, pipelineID.String()).Scan(&kind, &version, &definition); err != nil {
		return fmt.Errorf("sqlite: load memory-status pipeline: %w", err)
	}
	if kind != "memory_status" || version != domain.MemoryStatusPipelineVersion ||
		definition != memoryStatusPipelineDefinitionV1 {
		return fmt.Errorf("%w: pipeline is not exact memory-status-v1", integrity.ErrQuarantineConflict)
	}
	return nil
}

func (u *canonicalUoW) classifyIntegrityFinding(
	ctx context.Context,
	candidate integrity.Candidate,
) (canonical.ID, bool, error) {
	var existingIDRaw, residentRaw, kindRaw string
	var claimRaw, sourceRaw, ruleRaw, targetKindRaw, targetIDRaw, targetFieldRaw sql.NullString
	var fingerprintRaw []byte
	err := u.tx.QueryRowContext(ctx, `SELECT
		integrity_finding_id, resident_id, claim_id, finding_kind,
		source_content_erasure_event_id, finding_fingerprint, rule_code,
		target_kind, target_id, target_field
	FROM integrity_findings
	WHERE resident_id = ? AND finding_fingerprint = ?`,
		candidate.ResidentID.String(), candidate.Fingerprint.Bytes(),
	).Scan(
		&existingIDRaw, &residentRaw, &claimRaw, &kindRaw,
		&sourceRaw, &fingerprintRaw, &ruleRaw,
		&targetKindRaw, &targetIDRaw, &targetFieldRaw,
	)
	if errors.Is(err, sql.ErrNoRows) {
		return canonical.ID{}, false, nil
	}
	if err != nil {
		return canonical.ID{}, false, fmt.Errorf("sqlite: classify integrity finding: %w", err)
	}
	existingID, err := canonical.ParseID(existingIDRaw)
	if err != nil {
		return canonical.ID{}, false, fmt.Errorf("%w: existing finding ID is invalid", integrity.ErrFindingConflict)
	}
	if residentRaw != candidate.ResidentID.String() || kindRaw != string(candidate.Kind) ||
		!nullableIDMatches(claimRaw, candidate.ClaimID) ||
		!nullableIDMatches(sourceRaw, candidate.SourceContentErasureEventID) ||
		len(fingerprintRaw) != len(candidate.Fingerprint.Bytes()) ||
		!slices.Equal(fingerprintRaw, candidate.Fingerprint.Bytes()) ||
		!ruleRaw.Valid || ruleRaw.String != string(candidate.RuleCode) ||
		!targetKindRaw.Valid || targetKindRaw.String != string(candidate.TargetKind) ||
		!targetIDRaw.Valid || targetIDRaw.String != candidate.TargetID.String() ||
		!targetFieldRaw.Valid || targetFieldRaw.String != candidate.TargetField {
		return canonical.ID{}, false, fmt.Errorf("%w: fingerprint matched a different logical finding", integrity.ErrFindingConflict)
	}
	return existingID, true, nil
}

func nullableIDMatches(raw sql.NullString, expected *canonical.ID) bool {
	if expected == nil {
		return !raw.Valid
	}
	return raw.Valid && raw.String == expected.String()
}

func (u *canonicalUoW) revalidateIntegrityCandidate(ctx context.Context, candidate integrity.Candidate) error {
	if err := candidate.Validate(); err != nil {
		return err
	}
	var matches int
	var err error
	switch candidate.RuleCode {
	case integrity.RuleClaimStatementErased:
		err = u.tx.QueryRowContext(ctx, `SELECT COUNT(*)
			FROM claims claim
			JOIN content_objects content ON content.content_id = claim.statement_content_id
			JOIN content_erasure_events erasure ON erasure.content_id = content.content_id
			WHERE claim.claim_id = ?
			  AND claim.owner_resident_id = ?
			  AND content.erasure_state = 'erased'
			  AND erasure.content_erasure_event_id = ?`,
			candidate.TargetID.String(), candidate.ResidentID.String(),
			candidate.SourceContentErasureEventID.String(),
		).Scan(&matches)
	case integrity.RuleClaimQualifyingSupportErased,
		integrity.RuleClaimQualifyingSupportUnresolvable:
		var expected *integrity.CandidateInput
		expected, err = loadOneQualifyingSupportCandidate(
			ctx, u.tx, candidate.ResidentID, candidate.TargetID,
		)
		if err == nil && expected != nil {
			var recomputed integrity.Candidate
			recomputed, err = integrity.NewCandidate(*expected)
			if err == nil && integrity.SemanticEqual(recomputed, candidate) {
				matches = 1
			}
		}
	case integrity.RuleGenerationInputSourceMissing:
		err = u.tx.QueryRowContext(ctx, `SELECT COUNT(*)
			FROM generation_run_inputs input
			JOIN generation_runs run ON run.generation_run_id = input.generation_run_id
			WHERE input.generation_run_input_id = ?
			  AND run.resident_id = ?
			  AND CASE input.source_type
				WHEN 'runtime_projection' THEN input.source_id IS NOT NULL
				WHEN 'event' THEN input.source_id IS NULL OR NOT EXISTS (
					SELECT 1 FROM events source
					WHERE source.event_id = input.source_id AND source.resident_id = run.resident_id
				)
				WHEN 'claim' THEN input.source_id IS NULL OR NOT EXISTS (
					SELECT 1 FROM claims source
					WHERE source.claim_id = input.source_id AND source.owner_resident_id = run.resident_id
				)
				WHEN 'resident_revision' THEN input.source_id IS NULL OR NOT EXISTS (
					SELECT 1 FROM resident_revisions source
					WHERE source.revision_id = input.source_id AND source.resident_id = run.resident_id
				)
				ELSE 1
			  END`, candidate.TargetID.String(), candidate.ResidentID.String()).Scan(&matches)
	case integrity.RuleActiveRequiredRevisionErased:
		err = u.tx.QueryRowContext(ctx, `SELECT COUNT(*)
			FROM resident_revisions revision
			JOIN content_objects content ON content.content_id = revision.content_id
			JOIN resident_revision_activations activation ON activation.revision_id = revision.revision_id
			JOIN canonical_commits activation_commit ON activation_commit.canonical_commit_id = activation.canonical_commit_id
			JOIN content_erasure_events erasure ON erasure.content_id = content.content_id
			WHERE revision.revision_id = ?
			  AND revision.resident_id = ?
			  AND content.erasure_state = 'erased'
			  AND erasure.content_erasure_event_id = ?
			  AND NOT EXISTS (
				SELECT 1
				FROM resident_revision_activations later
				JOIN resident_revisions later_revision ON later_revision.revision_id = later.revision_id
				JOIN canonical_commits later_commit ON later_commit.canonical_commit_id = later.canonical_commit_id
				WHERE later.resident_id = activation.resident_id
				  AND later_revision.revision_class = revision.revision_class
				  AND later_commit.commit_seq > activation_commit.commit_seq
			  )
			  AND EXISTS (
				SELECT 1
				FROM resident_status_transitions status
				JOIN canonical_commits status_commit ON status_commit.canonical_commit_id = status.canonical_commit_id
				WHERE status.resident_id = revision.resident_id
				  AND status.to_status = 'active'
				  AND NOT EXISTS (
					SELECT 1
					FROM resident_status_transitions later_status
					JOIN canonical_commits later_status_commit ON later_status_commit.canonical_commit_id = later_status.canonical_commit_id
					WHERE later_status.resident_id = status.resident_id
					  AND later_status_commit.commit_seq > status_commit.commit_seq
				  )
			  )`, candidate.TargetID.String(), candidate.ResidentID.String(),
			candidate.SourceContentErasureEventID.String()).Scan(&matches)
	case integrity.RuleRunningAttemptInputErased:
		err = u.tx.QueryRowContext(ctx, `SELECT COUNT(*)
			FROM generation_run_inputs input
			JOIN generation_runs run ON run.generation_run_id = input.generation_run_id
			JOIN generation_run_outcomes running ON running.generation_run_id = run.generation_run_id
			JOIN content_objects content ON content.content_id = input.content_id
			JOIN content_erasure_events erasure ON erasure.content_id = content.content_id
			WHERE input.generation_run_input_id = ?
			  AND run.resident_id = ?
			  AND running.state = 'running'
			  AND content.erasure_state = 'erased'
			  AND erasure.content_erasure_event_id = ?
			  AND NOT EXISTS (
				SELECT 1 FROM generation_run_outcomes terminal
				WHERE terminal.generation_run_id = running.generation_run_id
				  AND terminal.attempt_no = running.attempt_no
				  AND terminal.state IN ('succeeded', 'failed', 'cancelled')
			  )`, candidate.TargetID.String(), candidate.ResidentID.String(),
			candidate.SourceContentErasureEventID.String()).Scan(&matches)
	case integrity.RuleCancellationEnvelopeUnresolvable:
		var expected *integrity.CandidateInput
		expected, err = loadOneCancellationEnvelopeCandidate(
			ctx, u.tx, candidate.ResidentID, candidate.TargetID,
		)
		if err == nil && expected != nil {
			var recomputed integrity.Candidate
			recomputed, err = integrity.NewCandidate(*expected)
			if err == nil && integrity.SemanticEqual(recomputed, candidate) {
				matches = 1
			}
		}
	default:
		return fmt.Errorf("%w: rule %s predicate is not registered in the Slice 1 scanner", integrity.ErrFindingConflict, candidate.RuleCode)
	}
	if err != nil {
		return fmt.Errorf("sqlite: revalidate integrity candidate: %w", err)
	}
	if matches != 1 {
		return fmt.Errorf("%w: rule %s predicate changed or is ambiguous for %s/%s",
			integrity.ErrFindingConflict, candidate.RuleCode, candidate.TargetKind, candidate.TargetID)
	}
	return nil
}
