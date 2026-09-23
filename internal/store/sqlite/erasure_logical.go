package sqlite

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"slices"
	"strings"

	"mahoroba.local/mahoroba/internal/canonical"
	"mahoroba.local/mahoroba/internal/erasure"
	"mahoroba.local/mahoroba/internal/integrity"
	"mahoroba.local/mahoroba/internal/memory"
)

func (repository *ErasureSnapshotRepository) CaptureErasureLogicalProvenance(
	ctx context.Context,
	request erasure.LogicalProvenanceRequest,
) (_ erasure.LogicalProvenanceSnapshot, resultErr error) {
	if repository == nil || repository.store == nil || repository.store.reader == nil {
		return erasure.LogicalProvenanceSnapshot{}, errors.New("sqlite: nil erasure logical provenance source")
	}
	tx, err := repository.store.reader.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return erasure.LogicalProvenanceSnapshot{}, err
	}
	defer func() {
		if err := tx.Rollback(); err != nil && !errors.Is(err, sql.ErrTxDone) {
			resultErr = errors.Join(resultErr, err)
		}
	}()
	if err := requireLogicalBaseHead(ctx, tx, request); err != nil {
		return erasure.LogicalProvenanceSnapshot{}, err
	}
	result, err := evaluateErasureLogicalProvenance(ctx, tx, request)
	if err != nil {
		return erasure.LogicalProvenanceSnapshot{}, err
	}
	if err := tx.Commit(); err != nil {
		return erasure.LogicalProvenanceSnapshot{}, err
	}
	return result, nil
}

func requireLogicalBaseHead(ctx context.Context, tx *sql.Tx, request erasure.LogicalProvenanceRequest) error {
	var id string
	var seq int64
	if err := tx.QueryRowContext(ctx, `SELECT canonical_commit_id,commit_seq FROM canonical_commits ORDER BY commit_seq DESC LIMIT 1`).Scan(&id, &seq); err != nil {
		return err
	}
	if id != request.BaseHeadCommitID.String() || seq != request.BaseHeadCommitSeq.Int64() {
		return erasure.ErrPlanStale
	}
	return nil
}

type logicalReferenceQuery struct {
	rule, kind, field string
	query             string
}

var erasureLogicalReferenceQueries = []logicalReferenceQuery{
	{
		rule: "claim_evidence_event_provenance_v1", kind: "claim_evidence", field: "event_id",
		query: `SELECT e.evidence_id,ev.content_id,e.claim_id
			FROM claim_evidence e JOIN claims c ON c.claim_id=e.claim_id JOIN events ev ON ev.event_id=e.event_id
			WHERE c.owner_resident_id=? ORDER BY e.evidence_id`,
	},
	{
		rule: "claim_evidence_source_chain_v1", kind: "claim_evidence", field: "source_evidence_id",
		query: `SELECT e.evidence_id,sev.content_id,e.claim_id
			FROM claim_evidence e JOIN claims c ON c.claim_id=e.claim_id
			JOIN claim_evidence source ON source.evidence_id=e.source_evidence_id
			JOIN events sev ON sev.event_id=source.event_id
			WHERE c.owner_resident_id=? ORDER BY e.evidence_id`,
	},
	{
		rule: "claim_validity_evidence_event_v1", kind: "claim_validity_assertion", field: "evidence_event_id",
		query: `SELECT a.validity_assertion_id,ev.content_id,a.claim_id
			FROM claim_validity_assertions a JOIN claims c ON c.claim_id=a.claim_id JOIN events ev ON ev.event_id=a.evidence_event_id
			WHERE c.owner_resident_id=? ORDER BY a.validity_assertion_id`,
	},
	{
		rule: "claim_status_trigger_event_v1", kind: "claim_status_transition", field: "trigger_event_id",
		query: `SELECT s.status_transition_id,ev.content_id,s.claim_id
			FROM claim_status_transitions s JOIN claims c ON c.claim_id=s.claim_id JOIN events ev ON ev.event_id=s.trigger_event_id
			WHERE c.owner_resident_id=? ORDER BY s.status_transition_id`,
	},
	{
		rule: "claim_status_trigger_evidence_v1", kind: "claim_status_transition", field: "trigger_evidence_id",
		query: `SELECT s.status_transition_id,ev.content_id,s.claim_id
			FROM claim_status_transitions s JOIN claims c ON c.claim_id=s.claim_id
			JOIN claim_evidence e ON e.evidence_id=s.trigger_evidence_id JOIN events ev ON ev.event_id=e.event_id
			WHERE c.owner_resident_id=? ORDER BY s.status_transition_id`,
	},
	{
		rule: "claim_status_trigger_relation_v1", kind: "claim_status_transition", field: "trigger_claim_relation_id",
		query: `SELECT s.status_transition_id,endpoint.statement_content_id,s.claim_id
			FROM claim_status_transitions s JOIN claims c ON c.claim_id=s.claim_id
			JOIN claim_relations r ON r.claim_relation_id=s.trigger_claim_relation_id
			JOIN claims endpoint ON endpoint.claim_id=r.from_claim_id OR endpoint.claim_id=r.to_claim_id
			WHERE c.owner_resident_id=? ORDER BY s.status_transition_id,endpoint.claim_id`,
	},
	{
		rule: "claim_status_trigger_finding_v1", kind: "claim_status_transition", field: "trigger_integrity_finding_id",
		query: `SELECT s.status_transition_id,material.content_id,s.claim_id
			FROM claim_status_transitions s JOIN claims c ON c.claim_id=s.claim_id
			JOIN integrity_findings f ON f.integrity_finding_id=s.trigger_integrity_finding_id
			JOIN (
				SELECT f1.integrity_finding_id,claim.statement_content_id AS content_id
				FROM integrity_findings f1 JOIN claims claim ON f1.target_kind='claim' AND claim.claim_id=f1.target_id
				UNION ALL
				SELECT f2.integrity_finding_id,event.content_id
				FROM integrity_findings f2 JOIN events event ON f2.target_kind='event' AND event.event_id=f2.target_id
				UNION ALL
				SELECT f3.integrity_finding_id,input.content_id
				FROM integrity_findings f3 JOIN generation_run_inputs input ON f3.target_kind='generation_input' AND input.generation_run_input_id=f3.target_id
				UNION ALL
				SELECT f4.integrity_finding_id,revision.content_id
				FROM integrity_findings f4 JOIN resident_revisions revision ON f4.target_kind='resident_revision' AND revision.revision_id=f4.target_id
			) material ON material.integrity_finding_id=f.integrity_finding_id
			WHERE c.owner_resident_id=? ORDER BY s.status_transition_id,material.content_id`,
	},
	{
		rule: "claim_stage_dependency_v1", kind: "claim_stage_transition_dependency", field: "dependency_claim_id",
		query: `SELECT d.stage_transition_dependency_id,dependency.statement_content_id,stage.claim_id
			FROM claim_stage_transition_dependencies d
			JOIN claim_stage_transitions stage ON stage.stage_transition_id=d.stage_transition_id
			JOIN claims owner ON owner.claim_id=stage.claim_id JOIN claims dependency ON dependency.claim_id=d.dependency_claim_id
			WHERE owner.owner_resident_id=? ORDER BY d.stage_transition_dependency_id`,
	},
	{
		rule: "claim_relation_from_v1", kind: "claim_relation", field: "from_claim_id",
		query: `SELECT r.claim_relation_id,endpoint.statement_content_id,r.from_claim_id
			FROM claim_relations r JOIN claims endpoint ON endpoint.claim_id=r.from_claim_id
			WHERE endpoint.owner_resident_id=? ORDER BY r.claim_relation_id`,
	},
	{
		rule: "claim_relation_to_v1", kind: "claim_relation", field: "to_claim_id",
		query: `SELECT r.claim_relation_id,endpoint.statement_content_id,r.to_claim_id
			FROM claim_relations r JOIN claims endpoint ON endpoint.claim_id=r.to_claim_id
			WHERE endpoint.owner_resident_id=? ORDER BY r.claim_relation_id`,
	},
	{
		rule: "claim_usage_claim_v1", kind: "claim_usage", field: "claim_id",
		query: `SELECT u.claim_usage_id,c.statement_content_id,u.claim_id
			FROM claim_usages u JOIN claims c ON c.claim_id=u.claim_id
			WHERE c.owner_resident_id=? ORDER BY u.claim_usage_id`,
	},
	{
		rule: "generation_recall_v1", kind: "generation_run", field: "recall_run_id",
		query: `SELECT g.generation_run_id,r.query_content_id,NULL
			FROM generation_runs g JOIN recall_runs r ON r.recall_run_id=g.recall_run_id
			WHERE g.resident_id=? AND r.query_content_id IS NOT NULL ORDER BY g.generation_run_id`,
	},
}

func evaluateErasureLogicalProvenance(
	ctx context.Context,
	tx *sql.Tx,
	request erasure.LogicalProvenanceRequest,
) (erasure.LogicalProvenanceSnapshot, error) {
	targetEvents := make(map[canonical.ID]canonical.ID, len(request.Targets))
	for _, target := range request.Targets {
		if err := target.ContentID.Validate(); err != nil {
			return erasure.LogicalProvenanceSnapshot{}, err
		}
		if err := target.ErasureEventID.Validate(); err != nil {
			return erasure.LogicalProvenanceSnapshot{}, err
		}
		if _, exists := targetEvents[target.ContentID]; exists {
			return erasure.LogicalProvenanceSnapshot{}, fmt.Errorf("%w: duplicate logical target", erasure.ErrInvalidPlan)
		}
		targetEvents[target.ContentID] = target.ErasureEventID
	}
	chainBlockers, err := captureLogicalSourceChainBlockers(ctx, tx, request.ResidentID)
	if err != nil {
		return erasure.LogicalProvenanceSnapshot{}, err
	}

	references, supportAffected, err := captureTargetedLogicalReferences(ctx, tx, request.ResidentID, targetEvents)
	if err != nil {
		return erasure.LogicalProvenanceSnapshot{}, err
	}
	result := erasure.LogicalProvenanceSnapshot{References: references, Breaks: []erasure.ProvenanceBreakSnapshot{}, ExistingFindingDependencies: []erasure.ExistingProvenanceFindingSnapshot{}, Blockers: chainBlockers}
	newBreaks := make(map[canonical.ID]struct{})
	for claimID := range supportAffected {
		statementID, claim, status, found, err := loadLogicalClaim(ctx, tx, request.ResidentID, claimID)
		if err != nil {
			return result, err
		}
		if !found {
			return result, fmt.Errorf("sqlite: logical provenance claim disappeared")
		}
		if _, statementTargeted := targetEvents[statementID]; statementTargeted {
			// Statement erasure is the unique cause for this operation and the
			// planner's alias path owns the finding/pair.
			continue
		}
		current, err := replayQualifyingSupportCandidate(ctx, tx, claim)
		if err != nil {
			return result, err
		}
		if current != nil {
			finding, exists, err := loadExistingProvenanceFinding(ctx, tx, *current)
			if err != nil {
				return result, err
			}
			if !exists {
				field, id := "provenance", claimID.String()
				result.Blockers = append(result.Blockers, erasure.Blocker{Code: "preexisting_integrity_break", TargetKind: "claim", TargetID: &id, TargetField: &field, RequiredActionCodes: []string{"run_integrity_scan"}})
				continue
			}
			result.ExistingFindingDependencies = append(result.ExistingFindingDependencies, erasure.ExistingProvenanceFindingSnapshot{ClaimID: claimID, CurrentStatus: status, Finding: finding})
			continue
		}
		brokenBy, broke, err := plannedSupportBreak(ctx, tx, claim, targetEvents)
		if err != nil {
			return result, err
		}
		if broke {
			newBreaks[claimID] = struct{}{}
			result.Breaks = append(result.Breaks, erasure.ProvenanceBreakSnapshot{ClaimID: claimID, CurrentStatus: status, SourceContentErasureEventID: brokenBy})
		}
	}
	for index := range result.References {
		claimID := result.References[index].AffectedClaimID
		if claimID == nil {
			continue
		}
		if _, broken := newBreaks[*claimID]; broken &&
			(result.References[index].RuleID == "claim_evidence_event_provenance_v1" || result.References[index].RuleID == "claim_evidence_source_chain_v1") {
			result.References[index].BreaksClaim = true
		}
	}
	sortLogicalProvenanceSnapshot(&result)
	return result, nil
}

type logicalEvidenceChainRow struct {
	id, claimID, eventID canonical.ID
	derivation           string
	sourceID             *canonical.ID
}

func captureLogicalSourceChainBlockers(ctx context.Context, tx *sql.Tx, residentID canonical.ID) ([]erasure.Blocker, error) {
	rows, err := tx.QueryContext(ctx, `SELECT e.evidence_id,e.claim_id,e.event_id,e.derivation,e.source_evidence_id
		FROM claim_evidence e JOIN claims c ON c.claim_id=e.claim_id
		WHERE c.owner_resident_id=? ORDER BY e.evidence_id`, residentID.String())
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	byID := map[canonical.ID]logicalEvidenceChainRow{}
	ordered := []canonical.ID{}
	for rows.Next() {
		var evidenceRaw, claimRaw, eventRaw, derivation string
		var sourceRaw sql.NullString
		if err := rows.Scan(&evidenceRaw, &claimRaw, &eventRaw, &derivation, &sourceRaw); err != nil {
			return nil, err
		}
		evidenceID, err := canonical.ParseID(evidenceRaw)
		if err != nil {
			return nil, err
		}
		claimID, err := canonical.ParseID(claimRaw)
		if err != nil {
			return nil, err
		}
		eventID, err := canonical.ParseID(eventRaw)
		if err != nil {
			return nil, err
		}
		row := logicalEvidenceChainRow{id: evidenceID, claimID: claimID, eventID: eventID, derivation: derivation}
		if sourceRaw.Valid {
			sourceID, err := canonical.ParseID(sourceRaw.String)
			if err != nil {
				return nil, err
			}
			row.sourceID = &sourceID
		}
		byID[evidenceID] = row
		ordered = append(ordered, evidenceID)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	blockedClaims := map[canonical.ID]struct{}{}
	for _, evidenceID := range ordered {
		row := byID[evidenceID]
		if row.derivation == "extracted" {
			if row.sourceID != nil {
				blockedClaims[row.claimID] = struct{}{}
			}
			continue
		}
		if row.derivation != "inherited" || row.sourceID == nil {
			blockedClaims[row.claimID] = struct{}{}
			continue
		}
		visited := map[canonical.ID]struct{}{row.id: {}}
		current := row
		valid := false
		for current.sourceID != nil {
			if _, cycle := visited[*current.sourceID]; cycle {
				break
			}
			visited[*current.sourceID] = struct{}{}
			source, exists := byID[*current.sourceID]
			if !exists || source.eventID != row.eventID {
				break
			}
			if source.derivation == "extracted" && source.sourceID == nil {
				// The current Writer admits exactly this direct terminal shape.
				// A longer inherited->inherited chain can only enter through a
				// malformed restore and remains fail-closed even if it eventually
				// reaches an extracted row.
				valid = current.id == row.id
				break
			}
			if source.derivation != "inherited" || source.sourceID == nil {
				break
			}
			current = source
		}
		if !valid {
			blockedClaims[row.claimID] = struct{}{}
		}
	}
	result := make([]erasure.Blocker, 0, len(blockedClaims))
	for claimID := range blockedClaims {
		id, field := claimID.String(), "provenance"
		result = append(result, erasure.Blocker{Code: "preexisting_integrity_break", TargetKind: "claim", TargetID: &id, TargetField: &field, RequiredActionCodes: []string{"run_integrity_scan"}})
	}
	slices.SortFunc(result, func(a, b erasure.Blocker) int { return strings.Compare(*a.TargetID, *b.TargetID) })
	return result, nil
}

func captureTargetedLogicalReferences(
	ctx context.Context,
	tx *sql.Tx,
	residentID canonical.ID,
	targets map[canonical.ID]canonical.ID,
) ([]erasure.LogicalReferenceSnapshot, map[canonical.ID]struct{}, error) {
	result := []erasure.LogicalReferenceSnapshot{}
	supportAffected := make(map[canonical.ID]struct{})
	for _, descriptor := range erasureLogicalReferenceQueries {
		rows, err := tx.QueryContext(ctx, descriptor.query, residentID.String())
		if err != nil {
			return nil, nil, fmt.Errorf("sqlite: capture logical rule %s: %w", descriptor.rule, err)
		}
		for rows.Next() {
			var referrerRaw, contentRaw string
			var claimRaw sql.NullString
			if err := rows.Scan(&referrerRaw, &contentRaw, &claimRaw); err != nil {
				_ = rows.Close()
				return nil, nil, err
			}
			contentID, err := canonical.ParseID(contentRaw)
			if err != nil {
				_ = rows.Close()
				return nil, nil, err
			}
			if _, targeted := targets[contentID]; !targeted {
				continue
			}
			value := erasure.LogicalReferenceSnapshot{ContentID: contentID, RuleID: descriptor.rule, ReferrerKind: descriptor.kind, ReferrerID: referrerRaw, ReferrerField: descriptor.field}
			if claimRaw.Valid {
				claimID, err := canonical.ParseID(claimRaw.String)
				if err != nil {
					_ = rows.Close()
					return nil, nil, err
				}
				value.AffectedClaimID = &claimID
				if descriptor.rule == "claim_evidence_event_provenance_v1" || descriptor.rule == "claim_evidence_source_chain_v1" || descriptor.rule == "claim_validity_evidence_event_v1" {
					supportAffected[claimID] = struct{}{}
				}
			}
			result = append(result, value)
		}
		if err := rows.Err(); err != nil {
			_ = rows.Close()
			return nil, nil, err
		}
		if err := rows.Close(); err != nil {
			return nil, nil, err
		}
	}
	slices.SortFunc(result, func(a, b erasure.LogicalReferenceSnapshot) int {
		return strings.Compare(a.RuleID+"\x00"+a.ReferrerID+"\x00"+a.ContentID.String(), b.RuleID+"\x00"+b.ReferrerID+"\x00"+b.ContentID.String())
	})
	result = slices.CompactFunc(result, func(a, b erasure.LogicalReferenceSnapshot) bool {
		return a.RuleID == b.RuleID && a.ReferrerKind == b.ReferrerKind && a.ReferrerID == b.ReferrerID && a.ReferrerField == b.ReferrerField
	})
	return result, supportAffected, nil
}

func loadLogicalClaim(ctx context.Context, tx *sql.Tx, residentID, claimID canonical.ID) (canonical.ID, qualifyingSupportClaim, string, bool, error) {
	var statementRaw, kindRaw, subjectRaw, perspectiveRaw, timezoneRaw, status string
	var recordedAt int64
	err := tx.QueryRowContext(ctx, `SELECT c.statement_content_id,COALESCE(c.kind,'unclassified'),c.subject_principal_id,c.perspective_principal_id,c.recorded_at,c.recorded_tz,
		COALESCE((SELECT s.to_status FROM claim_status_transitions s JOIN canonical_commits cc ON cc.canonical_commit_id=s.canonical_commit_id WHERE s.claim_id=c.claim_id ORDER BY cc.commit_seq DESC,s.status_transition_id DESC LIMIT 1),'active')
		FROM claims c WHERE c.owner_resident_id=? AND c.claim_id=?`, residentID.String(), claimID.String()).Scan(&statementRaw, &kindRaw, &subjectRaw, &perspectiveRaw, &recordedAt, &timezoneRaw, &status)
	if errors.Is(err, sql.ErrNoRows) {
		return canonical.ID{}, qualifyingSupportClaim{}, "", false, nil
	}
	if err != nil {
		return canonical.ID{}, qualifyingSupportClaim{}, "", false, err
	}
	statementID, err := canonical.ParseID(statementRaw)
	if err != nil {
		return canonical.ID{}, qualifyingSupportClaim{}, "", false, err
	}
	subjectID, err := canonical.ParseID(subjectRaw)
	if err != nil {
		return canonical.ID{}, qualifyingSupportClaim{}, "", false, err
	}
	perspectiveID, err := canonical.ParseID(perspectiveRaw)
	if err != nil {
		return canonical.ID{}, qualifyingSupportClaim{}, "", false, err
	}
	timezone, err := canonical.ParseTimezone(timezoneRaw)
	if err != nil {
		return canonical.ID{}, qualifyingSupportClaim{}, "", false, err
	}
	kind := memory.ClaimKind(kindRaw)
	if kindRaw == "unclassified" {
		kind = memory.ClaimKindUnclassified
	}
	if err := kind.Validate(); err != nil {
		return canonical.ID{}, qualifyingSupportClaim{}, "", false, err
	}
	return statementID, qualifyingSupportClaim{residentID: residentID, claimID: claimID, kind: kind, subjectID: subjectID, perspectiveID: perspectiveID, recordedAt: canonical.Instant(recordedAt), recordedTZ: timezone}, status, true, nil
}

func plannedSupportBreak(ctx context.Context, tx *sql.Tx, claim qualifyingSupportClaim, targets map[canonical.ID]canonical.ID) (canonical.ID, bool, error) {
	evidence, states, hadSupport, err := loadQualifyingSupportEvidence(ctx, tx, claim)
	if err != nil {
		return canonical.ID{}, false, err
	}
	if !hadSupport {
		return canonical.ID{}, false, nil
	}
	active := make(map[canonical.ID]canonical.ID)
	for _, row := range evidence {
		if row.qualifies && states[row.contentID] == "present" {
			active[row.id] = row.contentID
		}
	}
	if len(active) == 0 {
		return canonical.ID{}, false, nil
	}
	type targetEvent struct{ contentID, eventID canonical.ID }
	ordered := []targetEvent{}
	for contentID, eventID := range targets {
		for _, activeContent := range active {
			if activeContent == contentID {
				ordered = append(ordered, targetEvent{contentID, eventID})
				break
			}
		}
	}
	if len(ordered) == 0 {
		return canonical.ID{}, false, nil
	}
	slices.SortFunc(ordered, func(a, b targetEvent) int { return strings.Compare(a.eventID.String(), b.eventID.String()) })
	for _, target := range ordered {
		for evidenceID, contentID := range active {
			if contentID == target.contentID {
				delete(active, evidenceID)
			}
		}
		if len(active) == 0 {
			return target.eventID, true, nil
		}
	}
	return canonical.ID{}, false, nil
}

func loadExistingProvenanceFinding(ctx context.Context, tx *sql.Tx, input integrity.CandidateInput) (erasure.ExistingFindingSnapshot, bool, error) {
	candidate, err := integrity.NewCandidate(input)
	if err != nil {
		return erasure.ExistingFindingSnapshot{}, false, err
	}
	var findingRaw, kindRaw, ruleRaw, pipelineRaw string
	var sourceRaw sql.NullString
	var fingerprintRaw []byte
	err = tx.QueryRowContext(ctx, `SELECT integrity_finding_id,finding_kind,rule_code,source_content_erasure_event_id,pipeline_version_id,finding_fingerprint
		FROM integrity_findings WHERE resident_id=? AND finding_fingerprint=?`, input.ResidentID.String(), candidate.Fingerprint.Bytes()).Scan(&findingRaw, &kindRaw, &ruleRaw, &sourceRaw, &pipelineRaw, &fingerprintRaw)
	if errors.Is(err, sql.ErrNoRows) {
		return erasure.ExistingFindingSnapshot{}, false, nil
	}
	if err != nil {
		return erasure.ExistingFindingSnapshot{}, false, err
	}
	if kindRaw != string(candidate.Kind) || ruleRaw != string(candidate.RuleCode) || !slices.Equal(fingerprintRaw, candidate.Fingerprint.Bytes()) || sourceRaw.Valid != (candidate.SourceContentErasureEventID != nil) || (sourceRaw.Valid && sourceRaw.String != candidate.SourceContentErasureEventID.String()) {
		return erasure.ExistingFindingSnapshot{}, false, integrity.ErrFindingConflict
	}
	findingID, err := canonical.ParseID(findingRaw)
	if err != nil {
		return erasure.ExistingFindingSnapshot{}, false, err
	}
	pipelineID, err := canonical.ParseID(pipelineRaw)
	if err != nil {
		return erasure.ExistingFindingSnapshot{}, false, err
	}
	result := erasure.ExistingFindingSnapshot{IntegrityFindingID: findingID, FindingKind: kindRaw, RuleCode: ruleRaw, PipelineVersionID: pipelineID, Fingerprint: candidate.Fingerprint}
	if sourceRaw.Valid {
		sourceID, err := canonical.ParseID(sourceRaw.String)
		if err != nil {
			return erasure.ExistingFindingSnapshot{}, false, err
		}
		result.SourceContentErasureEventID = &sourceID
	}
	return result, true, nil
}

func sortLogicalProvenanceSnapshot(value *erasure.LogicalProvenanceSnapshot) {
	slices.SortFunc(value.References, func(a, b erasure.LogicalReferenceSnapshot) int {
		return strings.Compare(a.RuleID+"\x00"+a.ReferrerID+"\x00"+a.ContentID.String(), b.RuleID+"\x00"+b.ReferrerID+"\x00"+b.ContentID.String())
	})
	slices.SortFunc(value.Breaks, func(a, b erasure.ProvenanceBreakSnapshot) int {
		return strings.Compare(a.ClaimID.String(), b.ClaimID.String())
	})
	slices.SortFunc(value.ExistingFindingDependencies, func(a, b erasure.ExistingProvenanceFindingSnapshot) int {
		return strings.Compare(a.Finding.Fingerprint.Hex()+"\x00"+a.Finding.IntegrityFindingID.String(), b.Finding.Fingerprint.Hex()+"\x00"+b.Finding.IntegrityFindingID.String())
	})
	slices.SortFunc(value.Blockers, func(a, b erasure.Blocker) int {
		return strings.Compare(a.Code+"\x00"+stringValue(a.TargetID), b.Code+"\x00"+stringValue(b.TargetID))
	})
}
