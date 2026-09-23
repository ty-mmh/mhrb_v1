package sqlite

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	"slices"
	"strconv"
	"strings"

	"mahoroba.local/mahoroba/internal/canonical"
	"mahoroba.local/mahoroba/internal/contentref"
	"mahoroba.local/mahoroba/internal/domain"
	"mahoroba.local/mahoroba/internal/erasure"
	"mahoroba.local/mahoroba/internal/generation"
	"mahoroba.local/mahoroba/internal/integrity"
)

func (u *canonicalUoW) ApplyErasure(ctx context.Context, request erasure.ApplyRequest) (erasure.ApplyResult, error) {
	if err := verifyErasureApplyBoundary(request.Boundary); err != nil {
		return erasure.ApplyResult{}, err
	}
	plan := request.Plan
	residentID, err := canonical.ParseID(plan.ResidentID)
	if err != nil {
		return erasure.ApplyResult{}, err
	}
	actorID, err := canonical.ParseID(plan.ActorPrincipalID)
	if err != nil {
		return erasure.ApplyResult{}, err
	}
	if err := u.requireResidentScope(residentID); err != nil {
		return erasure.ApplyResult{}, err
	}
	if err := u.requireOwnerHuman(ctx, residentID, actorID); err != nil {
		return erasure.ApplyResult{}, errors.Join(erasure.ErrOwnerHumanRequired, err)
	}
	integrityPipelineID, _ := canonical.ParseID(plan.IntegrityPipelineVersionID)
	memoryStatusPipelineID, _ := canonical.ParseID(plan.MemoryStatusPipelineVersionID)
	if err := u.requireIntegrityPipeline(ctx, integrityPipelineID); err != nil {
		return erasure.ApplyResult{}, errors.Join(erasure.ErrIntegrityPipelineRequired, err)
	}
	if err := u.requireMemoryStatusPipeline(ctx, memoryStatusPipelineID); err != nil {
		return erasure.ApplyResult{}, errors.Join(erasure.ErrIntegrityPipelineRequired, err)
	}

	retry, err := u.inspectErasureRetry(ctx, plan)
	if err != nil {
		return erasure.ApplyResult{}, err
	}
	if retry != nil {
		return *retry, canonical.ErrNoMutation
	}
	if err := u.requireErasureBaseHead(ctx, plan); err != nil {
		return erasure.ApplyResult{}, err
	}
	if err := u.revalidateErasurePlan(ctx, plan, request.Blobs); err != nil {
		return erasure.ApplyResult{}, err
	}
	if plan.Scope == erasure.ScopeResident {
		if err := canonical.RequireCurrentResidentFence(ctx, residentID); err != nil {
			return erasure.ApplyResult{}, errors.Join(erasure.ErrSafetyBusy, err)
		}
		if busy, err := u.residentErasureBusy(ctx, residentID); err != nil {
			return erasure.ApplyResult{}, err
		} else if busy {
			return erasure.ApplyResult{}, erasure.ErrSafetyBusy
		}
	}
	if err := verifyErasureApplyBoundary(request.Boundary); err != nil {
		return erasure.ApplyResult{}, err
	}
	if err := runErasureFailpoint(request.Failpoint, "before_mutation"); err != nil {
		return erasure.ApplyResult{}, err
	}

	m := u.metadata
	for index, target := range plan.EffectiveTargets {
		var source any
		if target.SourceErasureEventID != nil {
			source = *target.SourceErasureEventID
			var parentCommit string
			if err := u.tx.QueryRowContext(ctx, `SELECT canonical_commit_id FROM content_erasure_events WHERE content_erasure_event_id=?`, *target.SourceErasureEventID).Scan(&parentCommit); err != nil || parentCommit != m.CommitID.String() {
				return erasure.ApplyResult{}, errors.Join(erasure.ErrPlanStale, err)
			}
		}
		if _, err := u.tx.ExecContext(ctx, `INSERT INTO content_erasure_events(
			content_erasure_event_id, canonical_commit_id, content_id, erasure_scope,
			actor_principal_id, reason_code, reason_content_id, source_erasure_event_id,
			occurred_at, occurred_tz, recorded_at, recorded_tz
		) VALUES (?, ?, ?, ?, ?, ?, NULL, ?, ?, ?, ?, ?)`, target.ErasureEventID, m.CommitID.String(), target.ContentID,
			plan.Scope, plan.ActorPrincipalID, plan.ReasonCode, source, m.CommittedAt.UnixMicro(), m.CommittedTZ.String(),
			m.CommittedAt.UnixMicro(), m.CommittedTZ.String()); err != nil {
			return erasure.ApplyResult{}, fmt.Errorf("sqlite: insert erasure event: %w", err)
		}
		if err := runErasureFailpoint(request.Failpoint, fmt.Sprintf("event:%d", index)); err != nil {
			return erasure.ApplyResult{}, err
		}
	}
	for index, finding := range plan.PlannedFindings {
		fingerprint, err := decodeWireDigest(finding.FindingFingerprint)
		if err != nil {
			return erasure.ApplyResult{}, err
		}
		if _, err := u.tx.ExecContext(ctx, `INSERT INTO integrity_findings(
			integrity_finding_id, canonical_commit_id, resident_id, claim_id, finding_kind,
			source_content_erasure_event_id, pipeline_version_id, details_content_id,
			occurred_at, occurred_tz, recorded_at, recorded_tz,
			finding_fingerprint, rule_code, target_kind, target_id, target_field
		) VALUES (?, ?, ?, ?, ?, ?, ?, NULL, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
			finding.IntegrityFindingID, m.CommitID.String(), finding.ResidentID, optionalString(finding.ClaimID),
			finding.FindingKind, optionalString(finding.SourceContentErasureEventID), finding.PipelineVersionID,
			m.CommittedAt.UnixMicro(), m.CommittedTZ.String(), m.CommittedAt.UnixMicro(), m.CommittedTZ.String(),
			fingerprint, finding.RuleCode, finding.TargetKind, finding.TargetID, finding.TargetField); err != nil {
			return erasure.ApplyResult{}, fmt.Errorf("sqlite: insert erasure finding: %w", err)
		}
		if err := runErasureFailpoint(request.Failpoint, fmt.Sprintf("finding:%d", index)); err != nil {
			return erasure.ApplyResult{}, err
		}
	}
	for index, transition := range plan.PlannedQuarantines {
		if status, err := u.currentClaimStatusRaw(ctx, transition.ClaimID); err != nil || status != transition.FromStatus {
			return erasure.ApplyResult{}, errors.Join(erasure.ErrPlanStale, err)
		}
		if _, err := u.tx.ExecContext(ctx, `INSERT INTO claim_status_transitions(
			status_transition_id, canonical_commit_id, claim_id, from_status, to_status, decision_kind,
			actor_principal_id, trigger_kind, trigger_event_id, trigger_evidence_id, trigger_claim_relation_id,
			trigger_integrity_finding_id, pipeline_version_id, memory_policy_revision_id, gate_metrics,
			decision_reason_code, decision_reason_content_id, occurred_at, occurred_tz, recorded_at, recorded_tz
		) VALUES (?, ?, ?, 'active', 'quarantined', 'automatic', NULL, 'integrity_finding', NULL, NULL, NULL,
			?, ?, ?, ?, 'structural_quarantine', NULL, ?, ?, ?, ?)`, transition.StatusTransitionID, m.CommitID.String(),
			transition.ClaimID, transition.TriggerIntegrityFindingID, transition.PipelineVersionID,
			optionalString(transition.MemoryPolicyRevisionID), transition.GateMetrics, m.CommittedAt.UnixMicro(), m.CommittedTZ.String(),
			m.CommittedAt.UnixMicro(), m.CommittedTZ.String()); err != nil {
			return erasure.ApplyResult{}, fmt.Errorf("sqlite: insert erasure quarantine: %w", err)
		}
		if err := runErasureFailpoint(request.Failpoint, fmt.Sprintf("quarantine:%d", index)); err != nil {
			return erasure.ApplyResult{}, err
		}
	}
	for index, target := range plan.EffectiveTargets {
		result, err := u.tx.ExecContext(ctx, `UPDATE content_objects SET erasure_state='erased', blob_hash=NULL, commitment_salt=NULL WHERE content_id=? AND owner_resident_id=? AND erasure_state='present'`, target.ContentID, plan.ResidentID)
		if err != nil {
			return erasure.ApplyResult{}, fmt.Errorf("sqlite: erase content: %w", err)
		}
		if err := requireRows(result, 1, "erase content"); err != nil {
			return erasure.ApplyResult{}, err
		}
		if err := runErasureFailpoint(request.Failpoint, fmt.Sprintf("content:%d", index)); err != nil {
			return erasure.ApplyResult{}, err
		}
	}
	for index, pair := range plan.ClaimIdentityErasures {
		if _, err := u.tx.ExecContext(ctx, `INSERT INTO claim_statement_erasure_events(
			claim_statement_erasure_event_id, canonical_commit_id, resident_id, claim_id,
			content_erasure_event_id, recorded_at, recorded_tz
		) VALUES (?, ?, ?, ?, ?, ?, ?)`, pair.ClaimStatementErasureEventID, m.CommitID.String(), plan.ResidentID,
			pair.ClaimID, pair.ContentErasureEventID, m.CommittedAt.UnixMicro(), m.CommittedTZ.String()); err != nil {
			return erasure.ApplyResult{}, fmt.Errorf("sqlite: insert claim erasure pair: %w", err)
		}
		if err := runErasureFailpoint(request.Failpoint, fmt.Sprintf("pair:%d", index)); err != nil {
			return erasure.ApplyResult{}, err
		}
		result, err := u.tx.ExecContext(ctx, `UPDATE claims SET statement_hash=NULL WHERE claim_id=? AND owner_resident_id=? AND statement_content_id=? AND statement_hash IS NOT NULL`, pair.ClaimID, plan.ResidentID, pair.StatementContentID)
		if err != nil {
			return erasure.ApplyResult{}, fmt.Errorf("sqlite: erase claim identity: %w", err)
		}
		if err := requireRows(result, 1, "erase claim identity"); err != nil {
			return erasure.ApplyResult{}, err
		}
		if err := runErasureFailpoint(request.Failpoint, fmt.Sprintf("claim_hash:%d", index)); err != nil {
			return erasure.ApplyResult{}, err
		}
	}
	if plan.ResidentTransition != nil {
		transition := plan.ResidentTransition
		if status, err := u.currentStatus(ctx, residentID); err != nil || status != transition.FromStatus {
			return erasure.ApplyResult{}, errors.Join(erasure.ErrPlanStale, err)
		}
		if _, err := u.tx.ExecContext(ctx, `INSERT INTO resident_status_transitions(
			resident_status_transition_id, canonical_commit_id, resident_id, from_status, to_status,
			actor_principal_id, reason_code, reason_content_id, occurred_at, occurred_tz, recorded_at, recorded_tz
		) VALUES (?, ?, ?, ?, 'erased', ?, ?, NULL, ?, ?, ?, ?)`, transition.TransitionID, m.CommitID.String(),
			plan.ResidentID, transition.FromStatus, plan.ActorPrincipalID, plan.ReasonCode, m.CommittedAt.UnixMicro(),
			m.CommittedTZ.String(), m.CommittedAt.UnixMicro(), m.CommittedTZ.String()); err != nil {
			return erasure.ApplyResult{}, fmt.Errorf("sqlite: insert resident erasure transition: %w", err)
		}
		if err := runErasureFailpoint(request.Failpoint, "resident_lifecycle"); err != nil {
			return erasure.ApplyResult{}, err
		}
	}
	if plan.RuntimeConfigEffect.ClearActiveResident {
		result, err := u.tx.ExecContext(ctx, `UPDATE runtime_config SET active_resident_id=NULL, desired_sessionization_policy_version_id=NULL, updated_at=?, updated_tz=? WHERE singleton_id=1 AND active_resident_id=?`, m.CommittedAt.UnixMicro(), m.CommittedTZ.String(), optionalString(plan.RuntimeConfigEffect.ExpectedActiveResidentID))
		if err != nil {
			return erasure.ApplyResult{}, fmt.Errorf("sqlite: clear erased selection: %w", err)
		}
		if err := requireRows(result, 1, "clear erased selection"); err != nil {
			return erasure.ApplyResult{}, err
		}
		if err := runErasureFailpoint(request.Failpoint, "runtime_selection"); err != nil {
			return erasure.ApplyResult{}, err
		}
	}
	if err := u.verifyErasurePostcondition(ctx, plan, m.CommitID.String()); err != nil {
		return erasure.ApplyResult{}, err
	}
	if err := runErasureFailpoint(request.Failpoint, "pre_commit"); err != nil {
		return erasure.ApplyResult{}, err
	}
	followupCount, err := contentMandatoryFollowupCount(ctx, u.tx, plan)
	if err != nil {
		return erasure.ApplyResult{}, err
	}
	if err := verifyErasureApplyBoundary(request.Boundary); err != nil {
		return erasure.ApplyResult{}, err
	}
	return erasure.ApplyResult{ExistingCommit: false, CanonicalErasureCommitID: m.CommitID.String(), CanonicalErasureCommitSeq: strconv.FormatInt(m.CommitSeq.Int64(), 10), ProjectionTargetHeadID: m.CommitID.String(), ProjectionTargetHeadSeq: strconv.FormatInt(m.CommitSeq.Int64(), 10), MandatoryWorkFollowupCount: followupCount, ProjectionRebuilds: slices.Clone(plan.Rebuilds)}, nil
}

func verifyErasureApplyBoundary(boundary erasure.ApplyBoundary) error {
	if boundary == nil {
		return nil
	}
	if err := boundary.Verify(); err != nil {
		return errors.Join(erasure.ErrPlanStale, err)
	}
	return nil
}

func (u *canonicalUoW) requireErasureBaseHead(ctx context.Context, plan erasure.Plan) error {
	wantSeq, _ := strconv.ParseInt(plan.BaseHead.CommitSeq, 10, 64)
	if u.metadata.CommitSeq.Int64()-1 != wantSeq {
		return erasure.ErrPlanStale
	}
	var id string
	if err := u.tx.QueryRowContext(ctx, `SELECT canonical_commit_id FROM canonical_commits WHERE commit_seq=?`, wantSeq).Scan(&id); err != nil || id != plan.BaseHead.CommitID {
		return errors.Join(erasure.ErrPlanStale, err)
	}
	return nil
}

func (u *canonicalUoW) revalidateErasurePlan(ctx context.Context, plan erasure.Plan, blobs erasure.BlobReader) error {
	if blobs == nil {
		return fmt.Errorf("%w: filesystem blob authority is required", erasure.ErrPlanStale)
	}
	targets := make(map[string]erasure.EffectiveTarget, len(plan.EffectiveTargets))
	for _, target := range plan.EffectiveTargets {
		commitment, err := decodeWireDigest(target.Commitment)
		if err != nil {
			return err
		}
		var owner, class, policy, state, hashAlgorithm string
		var actualCommitment, rawDigest, databaseBytes []byte
		var databaseSize sql.NullInt64
		if err := u.tx.QueryRowContext(ctx, `SELECT o.owner_resident_id,o.content_class,o.erasure_policy,o.erasure_state,o.commitment,o.blob_hash_algorithm,o.blob_hash,b.content,b.byte_size
			FROM content_objects o
			LEFT JOIN blobs b ON b.dedupe_scope_id=o.owner_resident_id AND b.hash_algorithm=o.blob_hash_algorithm AND b.blob_hash=o.blob_hash
			WHERE o.content_id=?`, target.ContentID).Scan(&owner, &class, &policy, &state, &actualCommitment, &hashAlgorithm, &rawDigest, &databaseBytes, &databaseSize); err != nil {
			return errors.Join(erasure.ErrPlanStale, err)
		}
		if owner != plan.ResidentID || class != target.ContentClass || policy != target.ErasurePolicy || state != "present" || !slices.Equal(commitment, actualCommitment) {
			return erasure.ErrPlanStale
		}
		if hashAlgorithm != "sha256" || len(rawDigest) != sha256.Size || !databaseSize.Valid || databaseSize.Int64 < 0 || int64(len(databaseBytes)) != databaseSize.Int64 {
			return fmt.Errorf("%w: invalid database blob locator for content %s", erasure.ErrPlanStale, target.ContentID)
		}
		digest, err := canonical.DigestFromBytes(rawDigest)
		if err != nil || canonical.HashBlob(databaseBytes) != digest {
			return fmt.Errorf("%w: database blob integrity failed for content %s", erasure.ErrPlanStale, target.ContentID)
		}
		residentID, err := canonical.ParseID(owner)
		if err != nil {
			return erasure.ErrPlanStale
		}
		reader, err := blobs.Open(ctx, residentID, digest)
		if err != nil {
			// Filesystem implementations may include a path in their error. Do
			// not carry that authority-bearing detail into the public error.
			return fmt.Errorf("%w: filesystem blob unavailable for content %s", erasure.ErrPlanStale, target.ContentID)
		}
		actualDigest, actualSize, readErr := canonical.HashBlobReader(reader)
		closeErr := reader.Close()
		if readErr != nil || closeErr != nil || actualSize != databaseSize.Int64 || actualDigest != digest {
			return fmt.Errorf("%w: filesystem blob integrity failed for content %s", erasure.ErrPlanStale, target.ContentID)
		}
		targets[target.ContentID] = target
	}
	if plan.Scope == erasure.ScopeResident {
		rows, err := u.tx.QueryContext(ctx, `SELECT content_id FROM content_objects WHERE owner_resident_id=? AND erasure_state='present' ORDER BY content_id`, plan.ResidentID)
		if err != nil {
			return err
		}
		defer rows.Close()
		actual := []string{}
		for rows.Next() {
			var id string
			if err := rows.Scan(&id); err != nil {
				return err
			}
			actual = append(actual, id)
		}
		planned := make([]string, 0, len(targets))
		for id := range targets {
			planned = append(planned, id)
		}
		slices.Sort(planned)
		if !slices.Equal(actual, planned) {
			return erasure.ErrPlanStale
		}
	}
	if err := u.revalidateDirectErasureImpacts(ctx, plan, targets); err != nil {
		return err
	}
	if err := u.revalidateLogicalErasureEffects(ctx, plan); err != nil {
		return err
	}
	if err := u.revalidateProjectionErasureEffects(plan); err != nil {
		return err
	}
	if err := u.revalidateErasureLineage(ctx, plan, targets); err != nil {
		return err
	}
	return u.revalidateAliasGroups(ctx, plan, targets)
}

func (u *canonicalUoW) revalidateLogicalErasureEffects(ctx context.Context, plan erasure.Plan) error {
	residentID, err := canonical.ParseID(plan.ResidentID)
	if err != nil {
		return erasure.ErrPlanStale
	}
	request := erasure.LogicalProvenanceRequest{ResidentID: residentID}
	for _, target := range plan.EffectiveTargets {
		contentID, parseErr := canonical.ParseID(target.ContentID)
		if parseErr != nil {
			return erasure.ErrPlanStale
		}
		eventID, parseErr := canonical.ParseID(target.ErasureEventID)
		if parseErr != nil {
			return erasure.ErrPlanStale
		}
		request.Targets = append(request.Targets, erasure.LogicalErasureTarget{ContentID: contentID, ErasureEventID: eventID})
	}
	evaluated, err := evaluateErasureLogicalProvenance(ctx, u.tx, request)
	if err != nil || len(evaluated.Blockers) != 0 {
		return errors.Join(erasure.ErrPlanStale, err)
	}

	wantImpacts := make(map[string]erasure.Impact)
	for _, reference := range evaluated.References {
		impact, err := erasure.ImpactForLogicalReference(reference)
		if err != nil {
			return erasure.ErrPlanStale
		}
		wantImpacts[impact.ImpactID] = impact
	}
	gotImpacts := make(map[string]erasure.Impact)
	for _, impact := range plan.Impacts {
		if erasure.IsLogicalImpactRule(impact.RuleID) {
			gotImpacts[impact.ImpactID] = impact
		}
	}
	if len(wantImpacts) != len(gotImpacts) {
		return erasure.ErrPlanStale
	}
	for id, want := range wantImpacts {
		got, ok := gotImpacts[id]
		if !ok || got.RuleID != want.RuleID || got.ReferrerKind != want.ReferrerKind || got.ReferrerID != want.ReferrerID ||
			got.ReferrerField != want.ReferrerField || got.Classification != want.Classification || !slices.Equal(got.Actions, want.Actions) || got.DecisionMode != want.DecisionMode {
			return erasure.ErrPlanStale
		}
		if got.DecisionMode == "none" {
			if got.Decision != nil {
				return erasure.ErrPlanStale
			}
		} else if got.Decision == nil || *got.Decision != "retain" {
			return erasure.ErrPlanStale
		}
	}

	quarantineByFinding := make(map[string]erasure.PlannedQuarantine, len(plan.PlannedQuarantines))
	for _, quarantine := range plan.PlannedQuarantines {
		quarantineByFinding[quarantine.TriggerIntegrityFindingID] = quarantine
	}
	wantBreaks := make(map[string]erasure.ProvenanceBreakSnapshot, len(evaluated.Breaks))
	for _, value := range evaluated.Breaks {
		wantBreaks[value.ClaimID.String()] = value
	}
	gotBreaks := make(map[string]erasure.PlannedFinding)
	for _, finding := range plan.PlannedFindings {
		if finding.RuleCode == string(integrity.RuleClaimQualifyingSupportErased) && finding.ClaimID != nil {
			gotBreaks[*finding.ClaimID] = finding
		}
	}
	if len(wantBreaks) != len(gotBreaks) {
		return erasure.ErrPlanStale
	}
	for claim, want := range wantBreaks {
		got, ok := gotBreaks[claim]
		if !ok || got.SourceContentErasureEventID == nil || *got.SourceContentErasureEventID != want.SourceContentErasureEventID.String() {
			return erasure.ErrPlanStale
		}
		_, quarantined := quarantineByFinding[got.IntegrityFindingID]
		if quarantined != (want.CurrentStatus == "active") {
			return erasure.ErrPlanStale
		}
	}

	wantExisting := make(map[string]erasure.ExistingProvenanceFindingSnapshot, len(evaluated.ExistingFindingDependencies))
	for _, value := range evaluated.ExistingFindingDependencies {
		wantExisting[value.Finding.IntegrityFindingID.String()] = value
	}
	gotExisting := make(map[string]erasure.ExistingFindingDependency)
	for _, value := range plan.ExistingFindingDependencies {
		if value.RuleCode == string(integrity.RuleClaimQualifyingSupportErased) || value.RuleCode == string(integrity.RuleClaimQualifyingSupportUnresolvable) {
			gotExisting[value.IntegrityFindingID] = value
		}
	}
	if len(wantExisting) != len(gotExisting) {
		return erasure.ErrPlanStale
	}
	for id, want := range wantExisting {
		got, ok := gotExisting[id]
		if !ok || got.ClaimID != want.ClaimID.String() || got.FindingFingerprint != "sha256:"+want.Finding.Fingerprint.Hex() ||
			got.FindingKind != want.Finding.FindingKind || got.RuleCode != want.Finding.RuleCode || got.PipelineVersionID != want.Finding.PipelineVersionID.String() {
			return erasure.ErrPlanStale
		}
		_, quarantined := quarantineByFinding[id]
		if quarantined != (want.CurrentStatus == "active") {
			return erasure.ErrPlanStale
		}
	}
	return nil
}

func (u *canonicalUoW) revalidateProjectionErasureEffects(plan erasure.Plan) error {
	want := map[string]struct{}{}
	for _, definition := range activeProjectionDefinitions {
		want[plan.ResidentID+"\x00"+string(definition.Name)+"\x00"+string(definition.Version)+"\x00content_erasure"] = struct{}{}
		if len(plan.PlannedQuarantines) != 0 {
			want[plan.ResidentID+"\x00"+string(definition.Name)+"\x00"+string(definition.Version)+"\x00claim_state_change"] = struct{}{}
		}
		if plan.Scope == erasure.ScopeResident {
			want[plan.ResidentID+"\x00"+string(definition.Name)+"\x00"+string(definition.Version)+"\x00resident_lifecycle_change"] = struct{}{}
		}
	}
	if len(want) != len(plan.Rebuilds) {
		return erasure.ErrPlanStale
	}
	for _, rebuild := range plan.Rebuilds {
		key := rebuild.ResidentID + "\x00" + rebuild.ProjectionName + "\x00" + rebuild.ProjectionVersion + "\x00" + rebuild.ReasonCode
		if _, ok := want[key]; !ok {
			return erasure.ErrPlanStale
		}
	}
	return nil
}

func (u *canonicalUoW) revalidateErasureLineage(ctx context.Context, plan erasure.Plan, targets map[string]erasure.EffectiveTarget) error {
	residentID, _ := canonical.ParseID(plan.ResidentID)
	rows, err := u.tx.QueryContext(ctx, `SELECT content_id,owner_resident_id,erasure_policy,erasure_state,blob_hash FROM content_objects WHERE owner_resident_id=? ORDER BY content_id`, plan.ResidentID)
	if err != nil {
		return err
	}
	contents := []erasure.ContentSnapshot{}
	contentByID := map[string]erasure.ContentSnapshot{}
	for rows.Next() {
		var contentRaw, ownerRaw, policy, state string
		var rawDigest []byte
		if err := rows.Scan(&contentRaw, &ownerRaw, &policy, &state, &rawDigest); err != nil {
			_ = rows.Close()
			return err
		}
		contentID, err := canonical.ParseID(contentRaw)
		if err != nil {
			_ = rows.Close()
			return err
		}
		ownerID, err := canonical.ParseID(ownerRaw)
		if err != nil {
			_ = rows.Close()
			return err
		}
		content := erasure.ContentSnapshot{ContentID: contentID, ResidentID: ownerID, ErasurePolicy: policy, ErasureState: state}
		if rawDigest != nil {
			digest, err := canonical.DigestFromBytes(rawDigest)
			if err != nil {
				_ = rows.Close()
				return err
			}
			content.BlobHash = &digest
		}
		contents = append(contents, content)
		contentByID[contentRaw] = content
	}
	if err := rows.Err(); err != nil {
		_ = rows.Close()
		return err
	}
	if err := rows.Close(); err != nil {
		return err
	}
	edges, blockers, err := captureErasureLineage(ctx, u.tx, residentID, contents)
	if err != nil {
		return err
	}
	if len(blockers) != 0 {
		return erasure.ErrPlanStale
	}
	targetByEvent := map[string]erasure.EffectiveTarget{}
	for _, target := range plan.EffectiveTargets {
		targetByEvent[target.ErasureEventID] = target
	}
	expectedImpacts := map[string]struct{}{}
	for _, target := range plan.EffectiveTargets {
		if target.SourceErasureEventID == nil {
			continue
		}
		parent, ok := targetByEvent[*target.SourceErasureEventID]
		if !ok || !hasExactLineageEdge(edges, parent.ContentID, target.ContentID) {
			return erasure.ErrPlanStale
		}
		expectedImpacts["explicit_lineage_exact_bytes_v1\x00"+target.ContentID] = struct{}{}
	}
	for _, edge := range edges {
		parent := edge.ParentContentID.String()
		child := edge.ChildContentID.String()
		if _, parentTarget := targets[parent]; !parentTarget {
			continue
		}
		childContent, exists := contentByID[child]
		if !exists || childContent.ErasureState != "present" || childContent.ResidentID != residentID {
			return erasure.ErrPlanStale
		}
		if edge.SameLogicalBlob {
			if _, childTarget := targets[child]; !childTarget {
				return erasure.ErrPlanStale
			}
			continue
		}
		if _, childTarget := targets[child]; !childTarget {
			expectedImpacts["explicit_lineage_nonexact_bytes_v1\x00"+child] = struct{}{}
		}
	}
	for childID, child := range contentByID {
		if child.ErasureState != "present" || child.BlobHash == nil {
			continue
		}
		if _, childTarget := targets[childID]; childTarget {
			continue
		}
		for targetID := range targets {
			target := contentByID[targetID]
			if target.BlobHash != nil && *target.BlobHash == *child.BlobHash {
				expectedImpacts["same_bytes_without_lineage_v1\x00"+childID] = struct{}{}
				break
			}
		}
	}
	actualImpacts := map[string]struct{}{}
	for _, impact := range plan.Impacts {
		switch impact.RuleID {
		case "explicit_lineage_exact_bytes_v1", "explicit_lineage_nonexact_bytes_v1", "same_bytes_without_lineage_v1":
			actualImpacts[impact.RuleID+"\x00"+impact.ReferrerID] = struct{}{}
			if impact.DecisionMode != "none" && (impact.Decision == nil || *impact.Decision != "retain") {
				return erasure.ErrPlanStale
			}
		}
	}
	if len(expectedImpacts) != len(actualImpacts) {
		return erasure.ErrPlanStale
	}
	for key := range expectedImpacts {
		if _, ok := actualImpacts[key]; !ok {
			return erasure.ErrPlanStale
		}
	}
	return nil
}

func hasExactLineageEdge(edges []erasure.LineageEdge, parentRaw, childRaw string) bool {
	for _, edge := range edges {
		if edge.Valid && edge.SameLogicalBlob && edge.ParentContentID.String() == parentRaw && edge.ChildContentID.String() == childRaw {
			return true
		}
	}
	return false
}

func (u *canonicalUoW) revalidateAliasGroups(ctx context.Context, plan erasure.Plan, targets map[string]erasure.EffectiveTarget) error {
	newByClaim := map[string]erasure.ClaimIdentityErasure{}
	existingByClaim := map[string]erasure.ExistingClaimIdentityDependency{}
	quarantineByClaim := map[string]struct{}{}
	for _, v := range plan.ClaimIdentityErasures {
		newByClaim[v.ClaimID] = v
	}
	for _, v := range plan.ExistingClaimIdentityDependencies {
		existingByClaim[v.ClaimID] = v
	}
	for _, v := range plan.PlannedQuarantines {
		quarantineByClaim[v.ClaimID] = struct{}{}
	}
	for contentID := range targets {
		rows, err := u.tx.QueryContext(ctx, `SELECT c.claim_id,c.owner_resident_id,c.statement_hash,e.claim_statement_erasure_event_id,e.content_erasure_event_id,e.canonical_commit_id,COALESCE((SELECT st.to_status FROM claim_status_transitions st JOIN canonical_commits cc ON cc.canonical_commit_id=st.canonical_commit_id WHERE st.claim_id=c.claim_id ORDER BY cc.commit_seq DESC,st.status_transition_id DESC LIMIT 1),'active') FROM claims c LEFT JOIN claim_statement_erasure_events e ON e.claim_id=c.claim_id WHERE c.statement_content_id=? ORDER BY c.claim_id`, contentID)
		if err != nil {
			return err
		}
		seen := 0
		for rows.Next() {
			seen++
			var claim, owner, status string
			var hash []byte
			var eventID, contentEventID, commitID sql.NullString
			if err := rows.Scan(&claim, &owner, &hash, &eventID, &contentEventID, &commitID, &status); err != nil {
				_ = rows.Close()
				return err
			}
			if owner != plan.ResidentID {
				_ = rows.Close()
				return erasure.ErrPlanStale
			}
			if planned, ok := newByClaim[claim]; ok {
				if len(hash) == 0 || eventID.Valid || planned.StatementContentID != contentID || planned.ContentErasureEventID != targets[contentID].ErasureEventID {
					_ = rows.Close()
					return erasure.ErrPlanStale
				}
				_, plannedQuarantine := quarantineByClaim[claim]
				if (status == "active") != plannedQuarantine {
					_ = rows.Close()
					return erasure.ErrPlanStale
				}
				delete(newByClaim, claim)
			} else if planned, ok := existingByClaim[claim]; ok {
				if len(hash) != 0 || !eventID.Valid || eventID.String != planned.ClaimStatementErasureEventID || contentEventID.String != planned.ContentErasureEventID || commitID.String != planned.CanonicalCommitID || planned.StatementContentID != contentID {
					_ = rows.Close()
					return erasure.ErrPlanStale
				}
				delete(existingByClaim, claim)
			} else {
				_ = rows.Close()
				return erasure.ErrPlanStale
			}
		}
		if err := rows.Err(); err != nil {
			_ = rows.Close()
			return err
		}
		if err := rows.Close(); err != nil {
			return err
		}
		_ = seen
	}
	if plan.Scope == erasure.ScopeResident {
		rows, err := u.tx.QueryContext(ctx, `SELECT c.claim_id,c.statement_content_id,c.statement_hash,
			e.claim_statement_erasure_event_id,e.content_erasure_event_id,e.canonical_commit_id,e.resident_id,
			o.erasure_state,ce.content_id,ce.canonical_commit_id
			FROM claims c
			JOIN content_objects o ON o.content_id=c.statement_content_id
			LEFT JOIN claim_statement_erasure_events e ON e.claim_id=c.claim_id
			LEFT JOIN content_erasure_events ce ON ce.content_erasure_event_id=e.content_erasure_event_id
			WHERE c.owner_resident_id=? ORDER BY c.claim_id`, plan.ResidentID)
		if err != nil {
			return err
		}
		for rows.Next() {
			var claim, contentID, state string
			var hash []byte
			var eventID, contentEventID, pairCommit, pairResident, erasedContentID, erasedContentCommit sql.NullString
			if err := rows.Scan(&claim, &contentID, &hash, &eventID, &contentEventID, &pairCommit, &pairResident, &state, &erasedContentID, &erasedContentCommit); err != nil {
				_ = rows.Close()
				return err
			}
			if _, targeted := targets[contentID]; targeted {
				continue
			}
			planned, ok := existingByClaim[claim]
			if !ok || len(hash) != 0 || state != "erased" || !eventID.Valid || !contentEventID.Valid || !pairCommit.Valid || !pairResident.Valid ||
				eventID.String != planned.ClaimStatementErasureEventID || contentEventID.String != planned.ContentErasureEventID || pairCommit.String != planned.CanonicalCommitID ||
				pairResident.String != plan.ResidentID || planned.StatementContentID != contentID || erasedContentID.String != contentID || erasedContentCommit.String != pairCommit.String {
				_ = rows.Close()
				return erasure.ErrPlanStale
			}
			delete(existingByClaim, claim)
		}
		if err := rows.Err(); err != nil {
			_ = rows.Close()
			return err
		}
		if err := rows.Close(); err != nil {
			return err
		}
	}
	if len(newByClaim) != 0 || len(existingByClaim) != 0 {
		return erasure.ErrPlanStale
	}
	return nil
}

func (u *canonicalUoW) revalidateDirectErasureImpacts(ctx context.Context, plan erasure.Plan, targets map[string]erasure.EffectiveTarget) error {
	want := map[string]struct{}{}
	directRules := map[string]struct{}{}
	for _, d := range contentref.DirectDescriptors() {
		directRules[d.RuleID] = struct{}{}
	}
	for _, impact := range plan.Impacts {
		if _, ok := directRules[impact.RuleID]; ok {
			want[impact.RuleID+"\x00"+impact.ReferrerKind+"\x00"+impact.ReferrerID+"\x00"+impact.ReferrerField] = struct{}{}
		}
	}
	actual := map[string]struct{}{}
	for _, descriptor := range contentref.DirectDescriptors() {
		query := fmt.Sprintf("SELECT %s FROM %s WHERE %s=? ORDER BY %s", descriptor.PrimaryKey, descriptor.Table, descriptor.ContentField, descriptor.PrimaryKey)
		for contentID := range targets {
			rows, err := u.tx.QueryContext(ctx, query, contentID)
			if err != nil {
				return err
			}
			for rows.Next() {
				var id string
				if err := rows.Scan(&id); err != nil {
					_ = rows.Close()
					return err
				}
				actual[descriptor.RuleID+"\x00"+descriptor.ReferrerKind+"\x00"+id+"\x00"+descriptor.ReferrerField] = struct{}{}
			}
			if err := rows.Err(); err != nil {
				_ = rows.Close()
				return err
			}
			if err := rows.Close(); err != nil {
				return err
			}
		}
	}
	if len(want) != len(actual) {
		return erasure.ErrPlanStale
	}
	for key := range actual {
		if _, ok := want[key]; !ok {
			return erasure.ErrPlanStale
		}
	}
	return nil
}

func (u *canonicalUoW) inspectErasureRetry(ctx context.Context, plan erasure.Plan) (*erasure.ApplyResult, error) {
	type lookup struct{ table, column, id string }
	lookups := []lookup{}
	for _, v := range plan.EffectiveTargets {
		lookups = append(lookups, lookup{"content_erasure_events", "content_erasure_event_id", v.ErasureEventID})
	}
	for _, v := range plan.PlannedFindings {
		lookups = append(lookups, lookup{"integrity_findings", "integrity_finding_id", v.IntegrityFindingID})
	}
	for _, v := range plan.PlannedQuarantines {
		lookups = append(lookups, lookup{"claim_status_transitions", "status_transition_id", v.StatusTransitionID})
	}
	for _, v := range plan.ClaimIdentityErasures {
		lookups = append(lookups, lookup{"claim_statement_erasure_events", "claim_statement_erasure_event_id", v.ClaimStatementErasureEventID})
	}
	if plan.ResidentTransition != nil {
		lookups = append(lookups, lookup{"resident_status_transitions", "resident_status_transition_id", plan.ResidentTransition.TransitionID})
	}
	found := 0
	commits := map[string]struct{}{}
	for _, v := range lookups {
		var commit string
		query := fmt.Sprintf("SELECT canonical_commit_id FROM %s WHERE %s=?", v.table, v.column)
		err := u.tx.QueryRowContext(ctx, query, v.id).Scan(&commit)
		if errors.Is(err, sql.ErrNoRows) {
			continue
		}
		if err != nil {
			return nil, err
		}
		found++
		commits[commit] = struct{}{}
	}
	if found == 0 {
		return nil, nil
	}
	if found != len(lookups) || len(commits) != 1 {
		return nil, erasure.ErrRetryConflict
	}
	var commitID string
	for id := range commits {
		commitID = id
	}
	var commitSeq int64
	if err := u.tx.QueryRowContext(ctx, `SELECT commit_seq FROM canonical_commits WHERE canonical_commit_id=?`, commitID).Scan(&commitSeq); err != nil {
		return nil, erasure.ErrRetryConflict
	}
	if err := u.verifyErasurePostcondition(ctx, plan, commitID); err != nil {
		return nil, errors.Join(erasure.ErrRetryConflict, err)
	}
	followupCount, err := contentMandatoryFollowupCount(ctx, u.tx, plan)
	if err != nil {
		return nil, err
	}
	durableSeq := u.metadata.CommitSeq.Int64() - 1
	var durableID string
	if err := u.tx.QueryRowContext(ctx, `SELECT canonical_commit_id FROM canonical_commits WHERE commit_seq=?`, durableSeq).Scan(&durableID); err != nil {
		return nil, err
	}
	result := &erasure.ApplyResult{ExistingCommit: true, CanonicalErasureCommitID: commitID, CanonicalErasureCommitSeq: strconv.FormatInt(commitSeq, 10), ProjectionTargetHeadID: durableID, ProjectionTargetHeadSeq: strconv.FormatInt(durableSeq, 10), MandatoryWorkFollowupCount: followupCount, ProjectionRebuilds: slices.Clone(plan.Rebuilds)}
	return result, nil
}

type erasureAliasPostcondition struct {
	residentID       string
	statementContent string
	pairEventID      string
	contentEventID   string
	pairCommitID     string
}

func (u *canonicalUoW) verifyErasureAliasPostcondition(ctx context.Context, plan erasure.Plan, retryCommitID string) error {
	expected := make(map[string]erasureAliasPostcondition, len(plan.ClaimIdentityErasures)+len(plan.ExistingClaimIdentityDependencies))
	for _, pair := range plan.ClaimIdentityErasures {
		if _, duplicate := expected[pair.ClaimID]; duplicate {
			return erasure.ErrRetryConflict
		}
		expected[pair.ClaimID] = erasureAliasPostcondition{
			residentID:       plan.ResidentID,
			statementContent: pair.StatementContentID,
			pairEventID:      pair.ClaimStatementErasureEventID,
			contentEventID:   pair.ContentErasureEventID,
			pairCommitID:     retryCommitID,
		}
	}
	for _, dependency := range plan.ExistingClaimIdentityDependencies {
		if _, duplicate := expected[dependency.ClaimID]; duplicate {
			return erasure.ErrRetryConflict
		}
		expected[dependency.ClaimID] = erasureAliasPostcondition{
			residentID:       plan.ResidentID,
			statementContent: dependency.StatementContentID,
			pairEventID:      dependency.ClaimStatementErasureEventID,
			contentEventID:   dependency.ContentErasureEventID,
			pairCommitID:     dependency.CanonicalCommitID,
		}
	}

	const columns = `SELECT
		c.claim_id,c.owner_resident_id,c.statement_content_id,c.statement_hash,
		p.claim_statement_erasure_event_id,p.resident_id,p.content_erasure_event_id,p.canonical_commit_id,
		o.owner_resident_id,o.erasure_state,ce.content_id,ce.canonical_commit_id
		FROM claims c
		LEFT JOIN content_objects o ON o.content_id=c.statement_content_id
		LEFT JOIN claim_statement_erasure_events p ON p.claim_id=c.claim_id
		LEFT JOIN content_erasure_events ce ON ce.content_erasure_event_id=p.content_erasure_event_id`
	seen := make(map[string]struct{}, len(expected))
	if plan.Scope == erasure.ScopeResident {
		rows, err := u.tx.QueryContext(ctx, columns+`
			WHERE c.owner_resident_id=? OR o.owner_resident_id=?
			ORDER BY c.claim_id,p.claim_statement_erasure_event_id`, plan.ResidentID, plan.ResidentID)
		if err != nil {
			return fmt.Errorf("sqlite: query resident erasure retry aliases: %w", err)
		}
		if err := verifyErasureAliasRows(rows, expected, seen); err != nil {
			return err
		}
	} else {
		for _, target := range plan.EffectiveTargets {
			rows, err := u.tx.QueryContext(ctx, columns+`
				WHERE c.statement_content_id=?
				ORDER BY c.claim_id,p.claim_statement_erasure_event_id`, target.ContentID)
			if err != nil {
				return fmt.Errorf("sqlite: query content erasure retry aliases: %w", err)
			}
			if err := verifyErasureAliasRows(rows, expected, seen); err != nil {
				return err
			}
		}
	}
	if len(seen) != len(expected) {
		return erasure.ErrRetryConflict
	}
	return nil
}

func verifyErasureAliasRows(rows *sql.Rows, expected map[string]erasureAliasPostcondition, seen map[string]struct{}) error {
	for rows.Next() {
		var claimID, claimOwner, statementContent string
		var statementHash []byte
		var pairEventID, pairResident, contentEventID, pairCommit sql.NullString
		var contentOwner, contentState, eventContent, eventCommit sql.NullString
		if err := rows.Scan(
			&claimID, &claimOwner, &statementContent, &statementHash,
			&pairEventID, &pairResident, &contentEventID, &pairCommit,
			&contentOwner, &contentState, &eventContent, &eventCommit,
		); err != nil {
			_ = rows.Close()
			return fmt.Errorf("sqlite: scan erasure retry aliases: %w", err)
		}
		if _, duplicate := seen[claimID]; duplicate {
			_ = rows.Close()
			return erasure.ErrRetryConflict
		}
		want, ok := expected[claimID]
		if !ok || claimOwner != want.residentID || statementContent != want.statementContent || statementHash != nil ||
			!pairEventID.Valid || pairEventID.String != want.pairEventID ||
			!pairResident.Valid || pairResident.String != want.residentID ||
			!contentEventID.Valid || contentEventID.String != want.contentEventID ||
			!pairCommit.Valid || pairCommit.String != want.pairCommitID ||
			!contentOwner.Valid || contentOwner.String != want.residentID ||
			!contentState.Valid || contentState.String != "erased" ||
			!eventContent.Valid || eventContent.String != want.statementContent ||
			!eventCommit.Valid || eventCommit.String != pairCommit.String {
			_ = rows.Close()
			return erasure.ErrRetryConflict
		}
		seen[claimID] = struct{}{}
	}
	if err := rows.Err(); err != nil {
		_ = rows.Close()
		return fmt.Errorf("sqlite: iterate erasure retry aliases: %w", err)
	}
	if err := rows.Close(); err != nil {
		return fmt.Errorf("sqlite: close erasure retry aliases: %w", err)
	}
	return nil
}

func (u *canonicalUoW) verifyErasurePostcondition(ctx context.Context, plan erasure.Plan, commitID string) error {
	for _, target := range plan.EffectiveTargets {
		var state, eventCommit, content, scope, actor, reason string
		var blob, salt []byte
		var source sql.NullString
		if err := u.tx.QueryRowContext(ctx, `SELECT o.erasure_state,o.blob_hash,o.commitment_salt,e.canonical_commit_id,e.content_id,e.erasure_scope,e.actor_principal_id,e.reason_code,e.source_erasure_event_id FROM content_objects o JOIN content_erasure_events e ON e.content_id=o.content_id WHERE e.content_erasure_event_id=?`, target.ErasureEventID).Scan(&state, &blob, &salt, &eventCommit, &content, &scope, &actor, &reason, &source); err != nil {
			return err
		}
		expectedSource := ""
		if target.SourceErasureEventID != nil {
			expectedSource = *target.SourceErasureEventID
		}
		if state != "erased" || blob != nil || salt != nil || eventCommit != commitID || content != target.ContentID || scope != plan.Scope || actor != plan.ActorPrincipalID || reason != plan.ReasonCode || source.String != expectedSource || source.Valid != (target.SourceErasureEventID != nil) {
			return erasure.ErrRetryConflict
		}
	}
	if err := u.verifyErasureAliasPostcondition(ctx, plan, commitID); err != nil {
		return err
	}
	for _, finding := range plan.PlannedFindings {
		var rowCommit, resident, kind, pipeline, rule, targetKind, targetID, targetField string
		var claim, source sql.NullString
		var fingerprint []byte
		if err := u.tx.QueryRowContext(ctx, `SELECT canonical_commit_id,resident_id,claim_id,finding_kind,source_content_erasure_event_id,pipeline_version_id,finding_fingerprint,rule_code,target_kind,target_id,target_field FROM integrity_findings WHERE integrity_finding_id=?`, finding.IntegrityFindingID).Scan(&rowCommit, &resident, &claim, &kind, &source, &pipeline, &fingerprint, &rule, &targetKind, &targetID, &targetField); err != nil {
			return err
		}
		wantFingerprint, _ := decodeWireDigest(finding.FindingFingerprint)
		if rowCommit != commitID || resident != finding.ResidentID || claim.String != stringValue(finding.ClaimID) || claim.Valid != (finding.ClaimID != nil) || kind != finding.FindingKind || source.String != stringValue(finding.SourceContentErasureEventID) || source.Valid != (finding.SourceContentErasureEventID != nil) || pipeline != finding.PipelineVersionID || !slices.Equal(fingerprint, wantFingerprint) || rule != finding.RuleCode || targetKind != finding.TargetKind || targetID != finding.TargetID || targetField != finding.TargetField {
			return erasure.ErrRetryConflict
		}
	}
	for _, dependency := range plan.ExistingFindingDependencies {
		var claim, kind, rule, pipeline string
		var source sql.NullString
		var fingerprint []byte
		if err := u.tx.QueryRowContext(ctx, `SELECT claim_id,finding_kind,rule_code,source_content_erasure_event_id,pipeline_version_id,finding_fingerprint FROM integrity_findings WHERE integrity_finding_id=?`, dependency.IntegrityFindingID).Scan(&claim, &kind, &rule, &source, &pipeline, &fingerprint); err != nil {
			return err
		}
		want, _ := decodeWireDigest(dependency.FindingFingerprint)
		if claim != dependency.ClaimID || kind != dependency.FindingKind || rule != dependency.RuleCode || source.String != stringValue(dependency.SourceContentErasureEventID) || source.Valid != (dependency.SourceContentErasureEventID != nil) || pipeline != dependency.PipelineVersionID || !slices.Equal(fingerprint, want) {
			return erasure.ErrRetryConflict
		}
	}
	for _, transition := range plan.PlannedQuarantines {
		var rowCommit, claim, from, to, decision, trigger, finding, pipeline, metrics, reason string
		if err := u.tx.QueryRowContext(ctx, `SELECT canonical_commit_id,claim_id,from_status,to_status,decision_kind,trigger_kind,trigger_integrity_finding_id,pipeline_version_id,gate_metrics,decision_reason_code FROM claim_status_transitions WHERE status_transition_id=?`, transition.StatusTransitionID).Scan(&rowCommit, &claim, &from, &to, &decision, &trigger, &finding, &pipeline, &metrics, &reason); err != nil {
			return err
		}
		if rowCommit != commitID || claim != transition.ClaimID || from != transition.FromStatus || to != transition.ToStatus || decision != transition.DecisionKind || trigger != transition.TriggerKind || finding != transition.TriggerIntegrityFindingID || pipeline != transition.PipelineVersionID || metrics != transition.GateMetrics || reason != transition.DecisionReasonCode {
			return erasure.ErrRetryConflict
		}
	}
	if plan.ResidentTransition != nil {
		transition := plan.ResidentTransition
		var rowCommit, resident, from, to, actor, reason string
		if err := u.tx.QueryRowContext(ctx, `SELECT canonical_commit_id,resident_id,from_status,to_status,actor_principal_id,reason_code FROM resident_status_transitions WHERE resident_status_transition_id=?`, transition.TransitionID).Scan(&rowCommit, &resident, &from, &to, &actor, &reason); err != nil {
			return err
		}
		if rowCommit != commitID || resident != transition.ResidentID || from != transition.FromStatus || to != transition.ToStatus || actor != transition.ActorPrincipalID || reason != transition.ReasonCode {
			return erasure.ErrRetryConflict
		}
	}
	if plan.Scope == erasure.ScopeResident {
		var presentContents int
		if err := u.tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM content_objects WHERE owner_resident_id=? AND erasure_state='present'`, plan.ResidentID).Scan(&presentContents); err != nil {
			return err
		}
		if presentContents != 0 {
			return erasure.ErrRetryConflict
		}
		var active sql.NullString
		if err := u.tx.QueryRowContext(ctx, `SELECT active_resident_id FROM runtime_config WHERE singleton_id=1`).Scan(&active); err != nil && !errors.Is(err, sql.ErrNoRows) {
			return err
		}
		if active.Valid && active.String == plan.ResidentID {
			return erasure.ErrRetryConflict
		}
	}
	return nil
}

func (u *canonicalUoW) currentClaimStatusRaw(ctx context.Context, claimID string) (string, error) {
	id, err := canonical.ParseID(claimID)
	if err != nil {
		return "", err
	}
	status, err := u.currentClaimStatus(ctx, id)
	return string(status), err
}
func (u *canonicalUoW) residentErasureBusy(ctx context.Context, residentID canonical.ID) (bool, error) {
	running, err := captureErasureRunning(ctx, u.tx, residentID)
	if err != nil {
		return false, err
	}
	if len(running) != 0 {
		return true, nil
	}
	count, err := mandatoryErasureWorkCount(ctx, u.tx, residentID)
	return count != 0, err
}

func mandatoryErasureWorkCount(ctx context.Context, q mandatoryRecoveryQueryer, residentID canonical.ID) (int, error) {
	rows, err := q.QueryContext(ctx, `SELECT event_id,event_type FROM events WHERE resident_id=? AND event_type IN ('user_message','self_talk') ORDER BY seq`, residentID.String())
	if err != nil {
		return 0, err
	}
	defer rows.Close()
	count := 0
	for rows.Next() {
		var eventRaw, eventType string
		if err := rows.Scan(&eventRaw, &eventType); err != nil {
			return 0, err
		}
		eventID, err := canonical.ParseID(eventRaw)
		if err != nil {
			return 0, err
		}
		kinds := []domain.MandatoryRecoveryKind{}
		if eventType == "user_message" {
			kinds = append(kinds, domain.MandatoryRecoveryDialogue)
		}
		policy, err := resolveEventTimeMandatoryPolicy(ctx, q, residentID, eventID, eventType)
		if err != nil {
			return 0, err
		}
		if !policy.Resolvable || policy.Mandatory {
			kinds = append(kinds, domain.MandatoryRecoveryMemoryExtraction)
		}
		for _, kind := range kinds {
			key := ""
			if kind == domain.MandatoryRecoveryDialogue {
				key = domain.DialogueObligation(eventID)
			} else {
				key = domain.MemoryExtractionObligation(eventID)
			}
			var runRaw string
			err := q.QueryRowContext(ctx, `SELECT generation_run_id FROM generation_runs WHERE resident_id=? AND idempotency_key=?`, residentID.String(), key).Scan(&runRaw)
			if errors.Is(err, sql.ErrNoRows) {
				count++
				continue
			}
			if err != nil {
				return 0, err
			}
			runID, err := canonical.ParseID(runRaw)
			if err != nil {
				return 0, err
			}
			summary, err := (generationOutcomeRepository{}).Summary(ctx, q, runID, residentID)
			if err != nil {
				return 0, err
			}
			if summary.Latest.State == "running" {
				count++
				continue
			}
			if summary.Latest.State == "failed" || summary.Latest.State == "cancelled" {
				if summary.Latest.ErrorClass.Valid {
					code, parseErr := generation.ParseOutcomeErrorCode(summary.Latest.ErrorClass.String)
					if parseErr == nil && (code.Retryable() || code.Class() == generation.ErrorForegroundPreempted) {
						count++
					}
				}
			}
		}
	}
	return count, rows.Err()
}
func contentMandatoryFollowupCount(ctx context.Context, q mandatoryRecoveryQueryer, plan erasure.Plan) (int, error) {
	if plan.Scope != erasure.ScopeContent {
		return 0, nil
	}
	resident, err := canonical.ParseID(plan.ResidentID)
	if err != nil {
		return 0, err
	}
	return mandatoryErasureWorkCount(ctx, q, resident)
}
func decodeWireDigest(value string) ([]byte, error) {
	if !strings.HasPrefix(value, "sha256:") {
		return nil, fmt.Errorf("sqlite: invalid wire digest")
	}
	decoded, err := hex.DecodeString(strings.TrimPrefix(value, "sha256:"))
	if err != nil || len(decoded) != 32 {
		return nil, fmt.Errorf("sqlite: invalid wire digest")
	}
	return decoded, nil
}
func optionalString(value *string) any {
	if value == nil {
		return nil
	}
	return *value
}
func stringValue(value *string) string {
	if value == nil {
		return ""
	}
	return *value
}
func requireRows(result sql.Result, want int64, operation string) error {
	got, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if got != want {
		return fmt.Errorf("%w: %s affected %d rows", erasure.ErrPlanStale, operation, got)
	}
	return nil
}
func runErasureFailpoint(f erasure.Failpoint, name string) error {
	if f == nil {
		return nil
	}
	return f(name)
}

var _ erasure.Mutator = (*canonicalUoW)(nil)
