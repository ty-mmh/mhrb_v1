-- +goose Up
CREATE TRIGGER trg_canonical_commits_no_update BEFORE UPDATE ON canonical_commits BEGIN SELECT RAISE(ABORT, 'canonical_commits is append-only'); END;
CREATE TRIGGER trg_canonical_commits_no_delete BEFORE DELETE ON canonical_commits BEGIN SELECT RAISE(ABORT, 'canonical_commits is append-only'); END;
CREATE TRIGGER trg_principals_no_update BEFORE UPDATE ON principals BEGIN SELECT RAISE(ABORT, 'principals is append-only'); END;
CREATE TRIGGER trg_principals_no_delete BEFORE DELETE ON principals BEGIN SELECT RAISE(ABORT, 'principals is append-only'); END;
CREATE TRIGGER trg_residents_no_update BEFORE UPDATE ON residents BEGIN SELECT RAISE(ABORT, 'residents is append-only'); END;
CREATE TRIGGER trg_residents_no_delete BEFORE DELETE ON residents BEGIN SELECT RAISE(ABORT, 'residents is append-only'); END;
CREATE TRIGGER trg_resident_revisions_no_update BEFORE UPDATE ON resident_revisions BEGIN SELECT RAISE(ABORT, 'resident_revisions is append-only'); END;
CREATE TRIGGER trg_resident_revisions_no_delete BEFORE DELETE ON resident_revisions BEGIN SELECT RAISE(ABORT, 'resident_revisions is append-only'); END;
CREATE TRIGGER trg_resident_revision_approvals_no_update BEFORE UPDATE ON resident_revision_approvals BEGIN SELECT RAISE(ABORT, 'resident_revision_approvals is append-only'); END;
CREATE TRIGGER trg_resident_revision_approvals_no_delete BEFORE DELETE ON resident_revision_approvals BEGIN SELECT RAISE(ABORT, 'resident_revision_approvals is append-only'); END;
CREATE TRIGGER trg_resident_revision_activations_no_update BEFORE UPDATE ON resident_revision_activations BEGIN SELECT RAISE(ABORT, 'resident_revision_activations is append-only'); END;
CREATE TRIGGER trg_resident_revision_activations_no_delete BEFORE DELETE ON resident_revision_activations BEGIN SELECT RAISE(ABORT, 'resident_revision_activations is append-only'); END;
CREATE TRIGGER trg_resident_status_transitions_no_update BEFORE UPDATE ON resident_status_transitions BEGIN SELECT RAISE(ABORT, 'resident_status_transitions is append-only'); END;
CREATE TRIGGER trg_resident_status_transitions_no_delete BEFORE DELETE ON resident_status_transitions BEGIN SELECT RAISE(ABORT, 'resident_status_transitions is append-only'); END;
CREATE TRIGGER trg_content_erasure_events_no_update BEFORE UPDATE ON content_erasure_events BEGIN SELECT RAISE(ABORT, 'content_erasure_events is append-only'); END;
CREATE TRIGGER trg_content_erasure_events_no_delete BEFORE DELETE ON content_erasure_events BEGIN SELECT RAISE(ABORT, 'content_erasure_events is append-only'); END;
CREATE TRIGGER trg_pipeline_versions_no_update BEFORE UPDATE ON pipeline_versions BEGIN SELECT RAISE(ABORT, 'pipeline_versions is append-only'); END;
CREATE TRIGGER trg_pipeline_versions_no_delete BEFORE DELETE ON pipeline_versions BEGIN SELECT RAISE(ABORT, 'pipeline_versions is append-only'); END;
CREATE TRIGGER trg_sessionization_policy_versions_no_update BEFORE UPDATE ON sessionization_policy_versions BEGIN SELECT RAISE(ABORT, 'sessionization_policy_versions is append-only'); END;
CREATE TRIGGER trg_sessionization_policy_versions_no_delete BEFORE DELETE ON sessionization_policy_versions BEGIN SELECT RAISE(ABORT, 'sessionization_policy_versions is append-only'); END;
CREATE TRIGGER trg_recall_runs_no_update BEFORE UPDATE ON recall_runs BEGIN SELECT RAISE(ABORT, 'recall_runs is append-only'); END;
CREATE TRIGGER trg_recall_runs_no_delete BEFORE DELETE ON recall_runs BEGIN SELECT RAISE(ABORT, 'recall_runs is append-only'); END;
CREATE TRIGGER trg_generation_runs_no_update BEFORE UPDATE ON generation_runs BEGIN SELECT RAISE(ABORT, 'generation_runs is append-only'); END;
CREATE TRIGGER trg_generation_runs_no_delete BEFORE DELETE ON generation_runs BEGIN SELECT RAISE(ABORT, 'generation_runs is append-only'); END;
CREATE TRIGGER trg_generation_run_inputs_no_update BEFORE UPDATE ON generation_run_inputs BEGIN SELECT RAISE(ABORT, 'generation_run_inputs is append-only'); END;
CREATE TRIGGER trg_generation_run_inputs_no_delete BEFORE DELETE ON generation_run_inputs BEGIN SELECT RAISE(ABORT, 'generation_run_inputs is append-only'); END;
CREATE TRIGGER trg_generation_run_outcomes_no_update BEFORE UPDATE ON generation_run_outcomes BEGIN SELECT RAISE(ABORT, 'generation_run_outcomes is append-only'); END;
CREATE TRIGGER trg_generation_run_outcomes_no_delete BEFORE DELETE ON generation_run_outcomes BEGIN SELECT RAISE(ABORT, 'generation_run_outcomes is append-only'); END;
CREATE TRIGGER trg_events_no_update BEFORE UPDATE ON events BEGIN SELECT RAISE(ABORT, 'events is append-only'); END;
CREATE TRIGGER trg_events_no_delete BEFORE DELETE ON events BEGIN SELECT RAISE(ABORT, 'events is append-only'); END;
CREATE TRIGGER trg_claims_no_update BEFORE UPDATE ON claims BEGIN SELECT RAISE(ABORT, 'claims is append-only'); END;
CREATE TRIGGER trg_claims_no_delete BEFORE DELETE ON claims BEGIN SELECT RAISE(ABORT, 'claims is append-only'); END;
CREATE TRIGGER trg_claim_evidence_no_update BEFORE UPDATE ON claim_evidence BEGIN SELECT RAISE(ABORT, 'claim_evidence is append-only'); END;
CREATE TRIGGER trg_claim_evidence_no_delete BEFORE DELETE ON claim_evidence BEGIN SELECT RAISE(ABORT, 'claim_evidence is append-only'); END;
CREATE TRIGGER trg_claim_stage_transitions_no_update BEFORE UPDATE ON claim_stage_transitions BEGIN SELECT RAISE(ABORT, 'claim_stage_transitions is append-only'); END;
CREATE TRIGGER trg_claim_stage_transitions_no_delete BEFORE DELETE ON claim_stage_transitions BEGIN SELECT RAISE(ABORT, 'claim_stage_transitions is append-only'); END;
CREATE TRIGGER trg_claim_stage_transition_dependencies_no_update BEFORE UPDATE ON claim_stage_transition_dependencies BEGIN SELECT RAISE(ABORT, 'claim_stage_transition_dependencies is append-only'); END;
CREATE TRIGGER trg_claim_stage_transition_dependencies_no_delete BEFORE DELETE ON claim_stage_transition_dependencies BEGIN SELECT RAISE(ABORT, 'claim_stage_transition_dependencies is append-only'); END;
CREATE TRIGGER trg_claim_relations_no_update BEFORE UPDATE ON claim_relations BEGIN SELECT RAISE(ABORT, 'claim_relations is append-only'); END;
CREATE TRIGGER trg_claim_relations_no_delete BEFORE DELETE ON claim_relations BEGIN SELECT RAISE(ABORT, 'claim_relations is append-only'); END;
CREATE TRIGGER trg_integrity_findings_no_update BEFORE UPDATE ON integrity_findings BEGIN SELECT RAISE(ABORT, 'integrity_findings is append-only'); END;
CREATE TRIGGER trg_integrity_findings_no_delete BEFORE DELETE ON integrity_findings BEGIN SELECT RAISE(ABORT, 'integrity_findings is append-only'); END;
CREATE TRIGGER trg_claim_status_transitions_no_update BEFORE UPDATE ON claim_status_transitions BEGIN SELECT RAISE(ABORT, 'claim_status_transitions is append-only'); END;
CREATE TRIGGER trg_claim_status_transitions_no_delete BEFORE DELETE ON claim_status_transitions BEGIN SELECT RAISE(ABORT, 'claim_status_transitions is append-only'); END;
CREATE TRIGGER trg_claim_validity_assertions_no_update BEFORE UPDATE ON claim_validity_assertions BEGIN SELECT RAISE(ABORT, 'claim_validity_assertions is append-only'); END;
CREATE TRIGGER trg_claim_validity_assertions_no_delete BEFORE DELETE ON claim_validity_assertions BEGIN SELECT RAISE(ABORT, 'claim_validity_assertions is append-only'); END;
CREATE TRIGGER trg_claim_view_scope_assertions_no_update BEFORE UPDATE ON claim_view_scope_assertions BEGIN SELECT RAISE(ABORT, 'claim_view_scope_assertions is append-only'); END;
CREATE TRIGGER trg_claim_view_scope_assertions_no_delete BEFORE DELETE ON claim_view_scope_assertions BEGIN SELECT RAISE(ABORT, 'claim_view_scope_assertions is append-only'); END;
CREATE TRIGGER trg_claim_usages_no_update BEFORE UPDATE ON claim_usages BEGIN SELECT RAISE(ABORT, 'claim_usages is append-only'); END;
CREATE TRIGGER trg_claim_usages_no_delete BEFORE DELETE ON claim_usages BEGIN SELECT RAISE(ABORT, 'claim_usages is append-only'); END;
CREATE TRIGGER trg_blobs_no_update BEFORE UPDATE ON blobs BEGIN SELECT RAISE(ABORT, 'blobs are write-once'); END;

CREATE TRIGGER trg_content_objects_erasure_only
BEFORE UPDATE ON content_objects
WHEN NOT (
    OLD.erasure_state = 'present' AND NEW.erasure_state = 'erased'
    AND NEW.content_id IS OLD.content_id
    AND NEW.owner_resident_id IS OLD.owner_resident_id
    AND NEW.content_class IS OLD.content_class
    AND NEW.blob_hash IS NULL
    AND NEW.blob_hash_algorithm IS OLD.blob_hash_algorithm
    AND NEW.commitment IS OLD.commitment
    AND NEW.commitment_salt IS NULL
    AND NEW.commitment_hash_algorithm IS OLD.commitment_hash_algorithm
    AND NEW.commitment_domain IS OLD.commitment_domain
    AND NEW.canonicalization_version IS OLD.canonicalization_version
    AND NEW.erasure_policy IS OLD.erasure_policy
    AND NEW.created_at IS OLD.created_at
    AND NEW.created_tz IS OLD.created_tz
)
BEGIN
    SELECT RAISE(ABORT, 'content_objects allow only present-to-erased transition');
END;

CREATE TRIGGER trg_content_objects_no_delete BEFORE DELETE ON content_objects BEGIN SELECT RAISE(ABORT, 'content_objects cannot be deleted'); END;

-- v0.1.2 resident-scope defenses.
-- These triggers reject structurally cross-resident references that can be
-- determined from immutable parent rows by indexed primary-key lookups.
-- Polymorphic and Projection/Operational references remain Runtime checks.

CREATE TRIGGER trg_residents_content_scope
BEFORE INSERT ON residents
WHEN NEW.description_content_id IS NOT NULL
 AND NOT EXISTS (
    SELECT 1
      FROM content_objects co
     WHERE co.content_id = NEW.description_content_id
       AND co.owner_resident_id = NEW.resident_id
 )
BEGIN
    SELECT RAISE(ABORT, 'residents.description_content_id must belong to the resident');
END;

CREATE TRIGGER trg_resident_revisions_resident_scope
BEFORE INSERT ON resident_revisions
WHEN NOT EXISTS (
        SELECT 1 FROM content_objects co
         WHERE co.content_id = NEW.content_id
           AND co.owner_resident_id = NEW.resident_id
     )
  OR (NEW.parent_revision_id IS NOT NULL AND NOT EXISTS (
        SELECT 1 FROM resident_revisions pr
         WHERE pr.revision_id = NEW.parent_revision_id
           AND pr.resident_id = NEW.resident_id
           AND pr.revision_class = NEW.revision_class
     ))
  OR (NEW.created_by_run_id IS NOT NULL AND NOT EXISTS (
        SELECT 1 FROM generation_runs gr
         WHERE gr.generation_run_id = NEW.created_by_run_id
           AND gr.resident_id = NEW.resident_id
     ))
  OR (NEW.reason_content_id IS NOT NULL AND NOT EXISTS (
        SELECT 1 FROM content_objects co
         WHERE co.content_id = NEW.reason_content_id
           AND co.owner_resident_id = NEW.resident_id
     ))
BEGIN
    SELECT RAISE(ABORT, 'resident_revisions contains a cross-resident reference');
END;

CREATE TRIGGER trg_resident_revision_approvals_resident_scope
BEFORE INSERT ON resident_revision_approvals
WHEN NEW.reason_content_id IS NOT NULL
 AND NOT EXISTS (
    SELECT 1
      FROM resident_revisions rr
      JOIN content_objects co ON co.content_id = NEW.reason_content_id
     WHERE rr.revision_id = NEW.revision_id
       AND co.owner_resident_id = rr.resident_id
 )
BEGIN
    SELECT RAISE(ABORT, 'resident_revision_approvals reason content must belong to the revision resident');
END;

CREATE TRIGGER trg_resident_revision_activations_resident_scope
BEFORE INSERT ON resident_revision_activations
WHEN NOT EXISTS (
        SELECT 1 FROM resident_revisions rr
         WHERE rr.revision_id = NEW.revision_id
           AND rr.resident_id = NEW.resident_id
     )
  OR (NEW.approval_id IS NOT NULL AND NOT EXISTS (
        SELECT 1 FROM resident_revision_approvals ra
         WHERE ra.approval_id = NEW.approval_id
           AND ra.revision_id = NEW.revision_id
     ))
  OR (NEW.reason_content_id IS NOT NULL AND NOT EXISTS (
        SELECT 1 FROM content_objects co
         WHERE co.content_id = NEW.reason_content_id
           AND co.owner_resident_id = NEW.resident_id
     ))
BEGIN
    SELECT RAISE(ABORT, 'resident_revision_activations contains a cross-resident reference');
END;

CREATE TRIGGER trg_resident_status_transitions_resident_scope
BEFORE INSERT ON resident_status_transitions
WHEN NEW.reason_content_id IS NOT NULL
 AND NOT EXISTS (
    SELECT 1 FROM content_objects co
     WHERE co.content_id = NEW.reason_content_id
       AND co.owner_resident_id = NEW.resident_id
 )
BEGIN
    SELECT RAISE(ABORT, 'resident_status_transitions reason content must belong to the resident');
END;

CREATE TRIGGER trg_content_erasure_events_resident_scope
BEFORE INSERT ON content_erasure_events
WHEN (NEW.reason_content_id IS NOT NULL AND NOT EXISTS (
        SELECT 1
          FROM content_objects target
          JOIN content_objects reason ON reason.content_id = NEW.reason_content_id
         WHERE target.content_id = NEW.content_id
           AND reason.owner_resident_id = target.owner_resident_id
      ))
   OR (NEW.source_erasure_event_id IS NOT NULL AND NOT EXISTS (
        SELECT 1
          FROM content_objects target
          JOIN content_erasure_events src ON src.content_erasure_event_id = NEW.source_erasure_event_id
          JOIN content_objects src_content ON src_content.content_id = src.content_id
         WHERE target.content_id = NEW.content_id
           AND src_content.owner_resident_id = target.owner_resident_id
      ))
BEGIN
    SELECT RAISE(ABORT, 'content_erasure_events contains a cross-resident reference');
END;

CREATE TRIGGER trg_recall_runs_resident_scope
BEFORE INSERT ON recall_runs
WHEN NOT EXISTS (
        SELECT 1 FROM resident_revisions rr
         WHERE rr.revision_id = NEW.memory_policy_revision_id
           AND rr.resident_id = NEW.resident_id
           AND rr.revision_class = 'memory_policy'
     )
  OR (NEW.query_content_id IS NOT NULL AND NOT EXISTS (
        SELECT 1 FROM content_objects co
         WHERE co.content_id = NEW.query_content_id
           AND co.owner_resident_id = NEW.resident_id
     ))
BEGIN
    SELECT RAISE(ABORT, 'recall_runs contains a cross-resident reference');
END;

CREATE TRIGGER trg_generation_runs_resident_scope
BEFORE INSERT ON generation_runs
WHEN NOT EXISTS (
        SELECT 1 FROM resident_revisions rr
         WHERE rr.revision_id = NEW.principles_revision_id
           AND rr.resident_id = NEW.resident_id
           AND rr.revision_class = 'principles'
     )
  OR NOT EXISTS (
        SELECT 1 FROM resident_revisions rr
         WHERE rr.revision_id = NEW.persona_revision_id
           AND rr.resident_id = NEW.resident_id
           AND rr.revision_class = 'persona'
     )
  OR NOT EXISTS (
        SELECT 1 FROM resident_revisions rr
         WHERE rr.revision_id = NEW.memory_policy_revision_id
           AND rr.resident_id = NEW.resident_id
           AND rr.revision_class = 'memory_policy'
     )
  OR (NEW.recall_run_id IS NOT NULL AND NOT EXISTS (
        SELECT 1 FROM recall_runs r
         WHERE r.recall_run_id = NEW.recall_run_id
           AND r.resident_id = NEW.resident_id
     ))
BEGIN
    SELECT RAISE(ABORT, 'generation_runs contains a cross-resident or wrong-class revision reference');
END;

CREATE TRIGGER trg_generation_run_inputs_resident_scope
BEFORE INSERT ON generation_run_inputs
WHEN NOT EXISTS (
    SELECT 1
      FROM generation_runs gr
      JOIN content_objects co ON co.content_id = NEW.content_id
     WHERE gr.generation_run_id = NEW.generation_run_id
       AND co.owner_resident_id = gr.resident_id
 )
BEGIN
    SELECT RAISE(ABORT, 'generation_run_inputs content must belong to the generation resident');
END;

CREATE TRIGGER trg_generation_run_outcomes_resident_scope
BEFORE INSERT ON generation_run_outcomes
WHEN (NEW.output_content_id IS NOT NULL AND NOT EXISTS (
        SELECT 1
          FROM generation_runs gr
          JOIN content_objects co ON co.content_id = NEW.output_content_id
         WHERE gr.generation_run_id = NEW.generation_run_id
           AND co.owner_resident_id = gr.resident_id
      ))
   OR (NEW.error_detail_content_id IS NOT NULL AND NOT EXISTS (
        SELECT 1
          FROM generation_runs gr
          JOIN content_objects co ON co.content_id = NEW.error_detail_content_id
         WHERE gr.generation_run_id = NEW.generation_run_id
           AND co.owner_resident_id = gr.resident_id
      ))
BEGIN
    SELECT RAISE(ABORT, 'generation_run_outcomes content must belong to the generation resident');
END;

CREATE TRIGGER trg_events_resident_scope
BEFORE INSERT ON events
WHEN NOT EXISTS (
        SELECT 1 FROM content_objects co
         WHERE co.content_id = NEW.content_id
           AND co.owner_resident_id = NEW.resident_id
     )
  OR (NEW.generation_run_id IS NOT NULL AND NOT EXISTS (
        SELECT 1 FROM generation_runs gr
         WHERE gr.generation_run_id = NEW.generation_run_id
           AND gr.resident_id = NEW.resident_id
     ))
BEGIN
    SELECT RAISE(ABORT, 'events contains a cross-resident content or generation reference');
END;

CREATE TRIGGER trg_claims_resident_scope
BEFORE INSERT ON claims
WHEN NOT EXISTS (
        SELECT 1 FROM content_objects co
         WHERE co.content_id = NEW.statement_content_id
           AND co.owner_resident_id = NEW.owner_resident_id
     )
  OR NOT EXISTS (
        SELECT 1 FROM generation_runs gr
         WHERE gr.generation_run_id = NEW.created_by_run_id
           AND gr.resident_id = NEW.owner_resident_id
     )
BEGIN
    SELECT RAISE(ABORT, 'claims contains a cross-resident content or generation reference');
END;

CREATE TRIGGER trg_claim_evidence_resident_scope
BEFORE INSERT ON claim_evidence
WHEN NOT EXISTS (
        SELECT 1
          FROM claims c
          JOIN events e ON e.event_id = NEW.event_id
         WHERE c.claim_id = NEW.claim_id
           AND c.owner_resident_id = e.resident_id
     )
  OR (NEW.source_evidence_id IS NOT NULL AND NOT EXISTS (
        SELECT 1
          FROM claims c
          JOIN claim_evidence se ON se.evidence_id = NEW.source_evidence_id
          JOIN claims sc ON sc.claim_id = se.claim_id
         WHERE c.claim_id = NEW.claim_id
           AND sc.owner_resident_id = c.owner_resident_id
     ))
  OR NOT EXISTS (
        SELECT 1
          FROM claims c
          JOIN resident_revisions rr ON rr.revision_id = NEW.memory_policy_revision_id
         WHERE c.claim_id = NEW.claim_id
           AND rr.resident_id = c.owner_resident_id
           AND rr.revision_class = 'memory_policy'
     )
  OR NOT EXISTS (
        SELECT 1
          FROM claims c
          JOIN generation_runs gr ON gr.generation_run_id = NEW.created_by_run_id
         WHERE c.claim_id = NEW.claim_id
           AND gr.resident_id = c.owner_resident_id
     )
  OR (NEW.reason_content_id IS NOT NULL AND NOT EXISTS (
        SELECT 1
          FROM claims c
          JOIN content_objects co ON co.content_id = NEW.reason_content_id
         WHERE c.claim_id = NEW.claim_id
           AND co.owner_resident_id = c.owner_resident_id
     ))
BEGIN
    SELECT RAISE(ABORT, 'claim_evidence contains a cross-resident reference');
END;

CREATE TRIGGER trg_claim_stage_transitions_resident_scope
BEFORE INSERT ON claim_stage_transitions
WHEN NOT EXISTS (
        SELECT 1
          FROM claims c
          JOIN resident_revisions rr ON rr.revision_id = NEW.memory_policy_revision_id
         WHERE c.claim_id = NEW.claim_id
           AND rr.resident_id = c.owner_resident_id
           AND rr.revision_class = 'memory_policy'
     )
  OR (NEW.generation_run_id IS NOT NULL AND NOT EXISTS (
        SELECT 1
          FROM claims c
          JOIN generation_runs gr ON gr.generation_run_id = NEW.generation_run_id
         WHERE c.claim_id = NEW.claim_id
           AND gr.resident_id = c.owner_resident_id
     ))
  OR (NEW.reason_content_id IS NOT NULL AND NOT EXISTS (
        SELECT 1
          FROM claims c
          JOIN content_objects co ON co.content_id = NEW.reason_content_id
         WHERE c.claim_id = NEW.claim_id
           AND co.owner_resident_id = c.owner_resident_id
     ))
BEGIN
    SELECT RAISE(ABORT, 'claim_stage_transitions contains a cross-resident reference');
END;

CREATE TRIGGER trg_claim_stage_dependency_resident_scope
BEFORE INSERT ON claim_stage_transition_dependencies
WHEN NOT EXISTS (
    SELECT 1
      FROM claim_stage_transitions st
      JOIN claims target ON target.claim_id = st.claim_id
      JOIN claims dependency ON dependency.claim_id = NEW.dependency_claim_id
     WHERE st.stage_transition_id = NEW.stage_transition_id
       AND dependency.owner_resident_id = target.owner_resident_id
 )
BEGIN
    SELECT RAISE(ABORT, 'claim stage dependency must not cross resident scope');
END;

CREATE TRIGGER trg_claim_relations_resident_scope
BEFORE INSERT ON claim_relations
WHEN NOT EXISTS (
        SELECT 1
          FROM claims f
          JOIN claims t ON t.claim_id = NEW.to_claim_id
         WHERE f.claim_id = NEW.from_claim_id
           AND f.owner_resident_id = t.owner_resident_id
     )
  OR (NEW.generation_run_id IS NOT NULL AND NOT EXISTS (
        SELECT 1
          FROM claims f
          JOIN generation_runs gr ON gr.generation_run_id = NEW.generation_run_id
         WHERE f.claim_id = NEW.from_claim_id
           AND gr.resident_id = f.owner_resident_id
     ))
  OR (NEW.reason_content_id IS NOT NULL AND NOT EXISTS (
        SELECT 1
          FROM claims f
          JOIN content_objects co ON co.content_id = NEW.reason_content_id
         WHERE f.claim_id = NEW.from_claim_id
           AND co.owner_resident_id = f.owner_resident_id
     ))
BEGIN
    SELECT RAISE(ABORT, 'claim_relations contains a cross-resident reference');
END;

CREATE TRIGGER trg_integrity_findings_resident_scope
BEFORE INSERT ON integrity_findings
WHEN (NEW.claim_id IS NOT NULL AND NOT EXISTS (
        SELECT 1 FROM claims c
         WHERE c.claim_id = NEW.claim_id
           AND c.owner_resident_id = NEW.resident_id
      ))
   OR (NEW.source_content_erasure_event_id IS NOT NULL AND NOT EXISTS (
        SELECT 1
          FROM content_erasure_events cee
          JOIN content_objects co ON co.content_id = cee.content_id
         WHERE cee.content_erasure_event_id = NEW.source_content_erasure_event_id
           AND co.owner_resident_id = NEW.resident_id
      ))
   OR (NEW.details_content_id IS NOT NULL AND NOT EXISTS (
        SELECT 1 FROM content_objects co
         WHERE co.content_id = NEW.details_content_id
           AND co.owner_resident_id = NEW.resident_id
      ))
BEGIN
    SELECT RAISE(ABORT, 'integrity_findings contains a cross-resident reference');
END;

CREATE TRIGGER trg_claim_status_transitions_resident_scope
BEFORE INSERT ON claim_status_transitions
WHEN (NEW.trigger_event_id IS NOT NULL AND NOT EXISTS (
        SELECT 1
          FROM claims c
          JOIN events e ON e.event_id = NEW.trigger_event_id
         WHERE c.claim_id = NEW.claim_id
           AND e.resident_id = c.owner_resident_id
      ))
   OR (NEW.trigger_evidence_id IS NOT NULL AND NOT EXISTS (
        SELECT 1
          FROM claims c
          JOIN claim_evidence ev ON ev.evidence_id = NEW.trigger_evidence_id
          JOIN claims ec ON ec.claim_id = ev.claim_id
         WHERE c.claim_id = NEW.claim_id
           AND ec.owner_resident_id = c.owner_resident_id
      ))
   OR (NEW.trigger_claim_relation_id IS NOT NULL AND NOT EXISTS (
        SELECT 1
          FROM claims c
          JOIN claim_relations cr ON cr.claim_relation_id = NEW.trigger_claim_relation_id
          JOIN claims rc ON rc.claim_id = cr.from_claim_id
         WHERE c.claim_id = NEW.claim_id
           AND rc.owner_resident_id = c.owner_resident_id
      ))
   OR (NEW.trigger_integrity_finding_id IS NOT NULL AND NOT EXISTS (
        SELECT 1
          FROM claims c
          JOIN integrity_findings f ON f.integrity_finding_id = NEW.trigger_integrity_finding_id
         WHERE c.claim_id = NEW.claim_id
           AND f.resident_id = c.owner_resident_id
      ))
   OR (NEW.memory_policy_revision_id IS NOT NULL AND NOT EXISTS (
        SELECT 1
          FROM claims c
          JOIN resident_revisions rr ON rr.revision_id = NEW.memory_policy_revision_id
         WHERE c.claim_id = NEW.claim_id
           AND rr.resident_id = c.owner_resident_id
           AND rr.revision_class = 'memory_policy'
      ))
   OR (NEW.decision_reason_content_id IS NOT NULL AND NOT EXISTS (
        SELECT 1
          FROM claims c
          JOIN content_objects co ON co.content_id = NEW.decision_reason_content_id
         WHERE c.claim_id = NEW.claim_id
           AND co.owner_resident_id = c.owner_resident_id
      ))
BEGIN
    SELECT RAISE(ABORT, 'claim_status_transitions contains a cross-resident reference');
END;

CREATE TRIGGER trg_claim_validity_assertions_resident_scope
BEFORE INSERT ON claim_validity_assertions
WHEN (NEW.evidence_event_id IS NOT NULL AND NOT EXISTS (
        SELECT 1
          FROM claims c
          JOIN events e ON e.event_id = NEW.evidence_event_id
         WHERE c.claim_id = NEW.claim_id
           AND e.resident_id = c.owner_resident_id
      ))
   OR (NEW.reason_content_id IS NOT NULL AND NOT EXISTS (
        SELECT 1
          FROM claims c
          JOIN content_objects co ON co.content_id = NEW.reason_content_id
         WHERE c.claim_id = NEW.claim_id
           AND co.owner_resident_id = c.owner_resident_id
      ))
BEGIN
    SELECT RAISE(ABORT, 'claim_validity_assertions contains a cross-resident reference');
END;

CREATE TRIGGER trg_claim_view_scope_assertions_resident_scope
BEFORE INSERT ON claim_view_scope_assertions
WHEN (NEW.generation_run_id IS NOT NULL AND NOT EXISTS (
        SELECT 1
          FROM claims c
          JOIN generation_runs gr ON gr.generation_run_id = NEW.generation_run_id
         WHERE c.claim_id = NEW.claim_id
           AND gr.resident_id = c.owner_resident_id
      ))
   OR (NEW.memory_policy_revision_id IS NOT NULL AND NOT EXISTS (
        SELECT 1
          FROM claims c
          JOIN resident_revisions rr ON rr.revision_id = NEW.memory_policy_revision_id
         WHERE c.claim_id = NEW.claim_id
           AND rr.resident_id = c.owner_resident_id
           AND rr.revision_class = 'memory_policy'
      ))
   OR (NEW.reason_content_id IS NOT NULL AND NOT EXISTS (
        SELECT 1
          FROM claims c
          JOIN content_objects co ON co.content_id = NEW.reason_content_id
         WHERE c.claim_id = NEW.claim_id
           AND co.owner_resident_id = c.owner_resident_id
      ))
BEGIN
    SELECT RAISE(ABORT, 'claim_view_scope_assertions contains a cross-resident reference');
END;

CREATE TRIGGER trg_claim_usages_resident_scope
BEFORE INSERT ON claim_usages
WHEN (NEW.recall_run_id IS NOT NULL AND NOT EXISTS (
        SELECT 1
          FROM claims c
          JOIN recall_runs rr ON rr.recall_run_id = NEW.recall_run_id
         WHERE c.claim_id = NEW.claim_id
           AND rr.resident_id = c.owner_resident_id
      ))
   OR (NEW.generation_run_id IS NOT NULL AND NOT EXISTS (
        SELECT 1
          FROM claims c
          JOIN generation_runs gr ON gr.generation_run_id = NEW.generation_run_id
         WHERE c.claim_id = NEW.claim_id
           AND gr.resident_id = c.owner_resident_id
      ))
   OR (NEW.detected_by_run_id IS NOT NULL AND NOT EXISTS (
        SELECT 1
          FROM claims c
          JOIN generation_runs gr ON gr.generation_run_id = NEW.detected_by_run_id
         WHERE c.claim_id = NEW.claim_id
           AND gr.resident_id = c.owner_resident_id
      ))
   OR NOT EXISTS (
        SELECT 1
          FROM claims c
          JOIN resident_revisions rr ON rr.revision_id = NEW.memory_policy_revision_id
         WHERE c.claim_id = NEW.claim_id
           AND rr.resident_id = c.owner_resident_id
           AND rr.revision_class = 'memory_policy'
      )
BEGIN
    SELECT RAISE(ABORT, 'claim_usages contains a cross-resident reference');
END;

-- +goose Down
DROP TRIGGER IF EXISTS trg_claim_usages_resident_scope;
DROP TRIGGER IF EXISTS trg_claim_view_scope_assertions_resident_scope;
DROP TRIGGER IF EXISTS trg_claim_validity_assertions_resident_scope;
DROP TRIGGER IF EXISTS trg_claim_status_transitions_resident_scope;
DROP TRIGGER IF EXISTS trg_integrity_findings_resident_scope;
DROP TRIGGER IF EXISTS trg_claim_relations_resident_scope;
DROP TRIGGER IF EXISTS trg_claim_stage_dependency_resident_scope;
DROP TRIGGER IF EXISTS trg_claim_stage_transitions_resident_scope;
DROP TRIGGER IF EXISTS trg_claim_evidence_resident_scope;
DROP TRIGGER IF EXISTS trg_claims_resident_scope;
DROP TRIGGER IF EXISTS trg_events_resident_scope;
DROP TRIGGER IF EXISTS trg_generation_run_outcomes_resident_scope;
DROP TRIGGER IF EXISTS trg_generation_run_inputs_resident_scope;
DROP TRIGGER IF EXISTS trg_generation_runs_resident_scope;
DROP TRIGGER IF EXISTS trg_recall_runs_resident_scope;
DROP TRIGGER IF EXISTS trg_content_erasure_events_resident_scope;
DROP TRIGGER IF EXISTS trg_resident_status_transitions_resident_scope;
DROP TRIGGER IF EXISTS trg_resident_revision_activations_resident_scope;
DROP TRIGGER IF EXISTS trg_resident_revision_approvals_resident_scope;
DROP TRIGGER IF EXISTS trg_resident_revisions_resident_scope;
DROP TRIGGER IF EXISTS trg_residents_content_scope;
DROP TRIGGER IF EXISTS trg_content_objects_erasure_only;
DROP TRIGGER IF EXISTS trg_content_objects_no_delete;
DROP TRIGGER IF EXISTS trg_blobs_no_update;
DROP TRIGGER IF EXISTS trg_claim_usages_no_update;
DROP TRIGGER IF EXISTS trg_claim_usages_no_delete;
DROP TRIGGER IF EXISTS trg_claim_view_scope_assertions_no_update;
DROP TRIGGER IF EXISTS trg_claim_view_scope_assertions_no_delete;
DROP TRIGGER IF EXISTS trg_claim_validity_assertions_no_update;
DROP TRIGGER IF EXISTS trg_claim_validity_assertions_no_delete;
DROP TRIGGER IF EXISTS trg_claim_status_transitions_no_update;
DROP TRIGGER IF EXISTS trg_claim_status_transitions_no_delete;
DROP TRIGGER IF EXISTS trg_integrity_findings_no_update;
DROP TRIGGER IF EXISTS trg_integrity_findings_no_delete;
DROP TRIGGER IF EXISTS trg_claim_relations_no_update;
DROP TRIGGER IF EXISTS trg_claim_relations_no_delete;
DROP TRIGGER IF EXISTS trg_claim_stage_transition_dependencies_no_update;
DROP TRIGGER IF EXISTS trg_claim_stage_transition_dependencies_no_delete;
DROP TRIGGER IF EXISTS trg_claim_stage_transitions_no_update;
DROP TRIGGER IF EXISTS trg_claim_stage_transitions_no_delete;
DROP TRIGGER IF EXISTS trg_claim_evidence_no_update;
DROP TRIGGER IF EXISTS trg_claim_evidence_no_delete;
DROP TRIGGER IF EXISTS trg_claims_no_update;
DROP TRIGGER IF EXISTS trg_claims_no_delete;
DROP TRIGGER IF EXISTS trg_events_no_update;
DROP TRIGGER IF EXISTS trg_events_no_delete;
DROP TRIGGER IF EXISTS trg_generation_run_outcomes_no_update;
DROP TRIGGER IF EXISTS trg_generation_run_outcomes_no_delete;
DROP TRIGGER IF EXISTS trg_generation_run_inputs_no_update;
DROP TRIGGER IF EXISTS trg_generation_run_inputs_no_delete;
DROP TRIGGER IF EXISTS trg_generation_runs_no_update;
DROP TRIGGER IF EXISTS trg_generation_runs_no_delete;
DROP TRIGGER IF EXISTS trg_recall_runs_no_update;
DROP TRIGGER IF EXISTS trg_recall_runs_no_delete;
DROP TRIGGER IF EXISTS trg_sessionization_policy_versions_no_update;
DROP TRIGGER IF EXISTS trg_sessionization_policy_versions_no_delete;
DROP TRIGGER IF EXISTS trg_pipeline_versions_no_update;
DROP TRIGGER IF EXISTS trg_pipeline_versions_no_delete;
DROP TRIGGER IF EXISTS trg_content_erasure_events_no_update;
DROP TRIGGER IF EXISTS trg_content_erasure_events_no_delete;
DROP TRIGGER IF EXISTS trg_resident_status_transitions_no_update;
DROP TRIGGER IF EXISTS trg_resident_status_transitions_no_delete;
DROP TRIGGER IF EXISTS trg_resident_revision_activations_no_update;
DROP TRIGGER IF EXISTS trg_resident_revision_activations_no_delete;
DROP TRIGGER IF EXISTS trg_resident_revision_approvals_no_update;
DROP TRIGGER IF EXISTS trg_resident_revision_approvals_no_delete;
DROP TRIGGER IF EXISTS trg_resident_revisions_no_update;
DROP TRIGGER IF EXISTS trg_resident_revisions_no_delete;
DROP TRIGGER IF EXISTS trg_residents_no_update;
DROP TRIGGER IF EXISTS trg_residents_no_delete;
DROP TRIGGER IF EXISTS trg_principals_no_update;
DROP TRIGGER IF EXISTS trg_principals_no_delete;
DROP TRIGGER IF EXISTS trg_canonical_commits_no_update;
DROP TRIGGER IF EXISTS trg_canonical_commits_no_delete;
