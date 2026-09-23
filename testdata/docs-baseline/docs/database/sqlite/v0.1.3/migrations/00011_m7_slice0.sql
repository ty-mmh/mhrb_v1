-- +goose Up
-- mahoroba: migration-policy=forward-only
-- M7 Slice 0 makes claim identity erasure a one-way, auditable exception to
-- the v0.1.2 append-only claim contract.  The claims table is rebuilt rather
-- than altered because SQLite cannot change a NOT NULL column in place.

DROP TRIGGER IF EXISTS trg_claims_no_update;
DROP TRIGGER IF EXISTS trg_claims_no_delete;
DROP TRIGGER IF EXISTS trg_claims_resident_scope;

-- Defer references while populated v10 claims are rebuilt in place.
PRAGMA defer_foreign_keys = ON;

CREATE TABLE claims_m7_slice0 (
    claim_id TEXT PRIMARY KEY CHECK (length(claim_id) = 26 AND claim_id = upper(claim_id) AND substr(claim_id, 1, 1) BETWEEN '0' AND '7' AND claim_id NOT GLOB '*[^0123456789ABCDEFGHJKMNPQRSTVWXYZ]*'),
    canonical_commit_id TEXT NOT NULL CHECK (length(canonical_commit_id) = 26 AND canonical_commit_id = upper(canonical_commit_id) AND substr(canonical_commit_id, 1, 1) BETWEEN '0' AND '7' AND canonical_commit_id NOT GLOB '*[^0123456789ABCDEFGHJKMNPQRSTVWXYZ]*'),
    owner_resident_id TEXT NOT NULL CHECK (length(owner_resident_id) = 26 AND owner_resident_id = upper(owner_resident_id) AND substr(owner_resident_id, 1, 1) BETWEEN '0' AND '7' AND owner_resident_id NOT GLOB '*[^0123456789ABCDEFGHJKMNPQRSTVWXYZ]*'),
    subject_principal_id TEXT NOT NULL CHECK (length(subject_principal_id) = 26 AND subject_principal_id = upper(subject_principal_id) AND substr(subject_principal_id, 1, 1) BETWEEN '0' AND '7' AND subject_principal_id NOT GLOB '*[^0123456789ABCDEFGHJKMNPQRSTVWXYZ]*'),
    perspective_principal_id TEXT NOT NULL CHECK (length(perspective_principal_id) = 26 AND perspective_principal_id = upper(perspective_principal_id) AND substr(perspective_principal_id, 1, 1) BETWEEN '0' AND '7' AND perspective_principal_id NOT GLOB '*[^0123456789ABCDEFGHJKMNPQRSTVWXYZ]*'),
    kind TEXT NULL CHECK (kind IS NULL OR kind IN ('direct', 'other', 'meta')),
    temporal_kind TEXT NOT NULL CHECK (temporal_kind IN ('stable', 'volatile', 'episodic')),
    statement_content_id TEXT NOT NULL CHECK (length(statement_content_id) = 26 AND statement_content_id = upper(statement_content_id) AND substr(statement_content_id, 1, 1) BETWEEN '0' AND '7' AND statement_content_id NOT GLOB '*[^0123456789ABCDEFGHJKMNPQRSTVWXYZ]*'),
    statement_hash BLOB NULL CHECK (statement_hash IS NULL OR length(statement_hash) = 32),
    statement_hash_algorithm TEXT NOT NULL CHECK (statement_hash_algorithm = 'sha256'),
    statement_normalization_version TEXT NOT NULL,
    created_by_run_id TEXT NOT NULL CHECK (length(created_by_run_id) = 26 AND created_by_run_id = upper(created_by_run_id) AND substr(created_by_run_id, 1, 1) BETWEEN '0' AND '7' AND created_by_run_id NOT GLOB '*[^0123456789ABCDEFGHJKMNPQRSTVWXYZ]*'),
    recorded_at INTEGER NOT NULL,
    recorded_tz TEXT NOT NULL CHECK (length(recorded_tz) > 0),
    FOREIGN KEY (canonical_commit_id) REFERENCES canonical_commits(canonical_commit_id)
        DEFERRABLE INITIALLY DEFERRED,
    FOREIGN KEY (owner_resident_id) REFERENCES residents(resident_id)
        DEFERRABLE INITIALLY DEFERRED,
    FOREIGN KEY (subject_principal_id) REFERENCES principals(principal_id)
        DEFERRABLE INITIALLY DEFERRED,
    FOREIGN KEY (perspective_principal_id) REFERENCES principals(principal_id)
        DEFERRABLE INITIALLY DEFERRED,
    FOREIGN KEY (statement_content_id) REFERENCES content_objects(content_id)
        DEFERRABLE INITIALLY DEFERRED,
    FOREIGN KEY (created_by_run_id) REFERENCES generation_runs(generation_run_id)
        DEFERRABLE INITIALLY DEFERRED
) STRICT;

INSERT INTO claims_m7_slice0(
    claim_id, canonical_commit_id, owner_resident_id, subject_principal_id,
    perspective_principal_id, kind, temporal_kind, statement_content_id,
    statement_hash, statement_hash_algorithm, statement_normalization_version,
    created_by_run_id, recorded_at, recorded_tz
)
SELECT claim_id, canonical_commit_id, owner_resident_id, subject_principal_id,
       perspective_principal_id, kind, temporal_kind, statement_content_id,
       statement_hash, statement_hash_algorithm, statement_normalization_version,
       created_by_run_id, recorded_at, recorded_tz
FROM claims;

DROP TRIGGER IF EXISTS trg_claim_evidence_resident_scope;
DROP TRIGGER IF EXISTS trg_claim_stage_transitions_resident_scope;
DROP TRIGGER IF EXISTS trg_claim_stage_dependency_resident_scope;
DROP TRIGGER IF EXISTS trg_claim_relations_resident_scope;
DROP TRIGGER IF EXISTS trg_integrity_findings_resident_scope;
DROP TRIGGER IF EXISTS trg_claim_status_transitions_resident_scope;
DROP TRIGGER IF EXISTS trg_claim_validity_assertions_resident_scope;
DROP TRIGGER IF EXISTS trg_claim_view_scope_assertions_resident_scope;
DROP TRIGGER IF EXISTS trg_claim_usages_resident_scope;
DROP TABLE claims;
ALTER TABLE claims_m7_slice0 RENAME TO claims;

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

CREATE TABLE claim_statement_erasure_events (
    claim_statement_erasure_event_id TEXT PRIMARY KEY CHECK (length(claim_statement_erasure_event_id) = 26 AND claim_statement_erasure_event_id = upper(claim_statement_erasure_event_id) AND substr(claim_statement_erasure_event_id, 1, 1) BETWEEN '0' AND '7' AND claim_statement_erasure_event_id NOT GLOB '*[^0123456789ABCDEFGHJKMNPQRSTVWXYZ]*'),
    canonical_commit_id TEXT NOT NULL CHECK (length(canonical_commit_id) = 26 AND canonical_commit_id = upper(canonical_commit_id) AND substr(canonical_commit_id, 1, 1) BETWEEN '0' AND '7' AND canonical_commit_id NOT GLOB '*[^0123456789ABCDEFGHJKMNPQRSTVWXYZ]*'),
    resident_id TEXT NOT NULL CHECK (length(resident_id) = 26 AND resident_id = upper(resident_id) AND substr(resident_id, 1, 1) BETWEEN '0' AND '7' AND resident_id NOT GLOB '*[^0123456789ABCDEFGHJKMNPQRSTVWXYZ]*'),
    claim_id TEXT NOT NULL CHECK (length(claim_id) = 26 AND claim_id = upper(claim_id) AND substr(claim_id, 1, 1) BETWEEN '0' AND '7' AND claim_id NOT GLOB '*[^0123456789ABCDEFGHJKMNPQRSTVWXYZ]*'),
    content_erasure_event_id TEXT NOT NULL CHECK (length(content_erasure_event_id) = 26 AND content_erasure_event_id = upper(content_erasure_event_id) AND substr(content_erasure_event_id, 1, 1) BETWEEN '0' AND '7' AND content_erasure_event_id NOT GLOB '*[^0123456789ABCDEFGHJKMNPQRSTVWXYZ]*'),
    recorded_at INTEGER NOT NULL,
    recorded_tz TEXT NOT NULL CHECK (length(recorded_tz) > 0),
    UNIQUE (claim_id),
    FOREIGN KEY (canonical_commit_id) REFERENCES canonical_commits(canonical_commit_id)
        DEFERRABLE INITIALLY DEFERRED,
    FOREIGN KEY (resident_id) REFERENCES residents(resident_id)
        DEFERRABLE INITIALLY DEFERRED,
    FOREIGN KEY (claim_id) REFERENCES claims(claim_id)
        DEFERRABLE INITIALLY DEFERRED,
    FOREIGN KEY (content_erasure_event_id) REFERENCES content_erasure_events(content_erasure_event_id)
        DEFERRABLE INITIALLY DEFERRED
) STRICT;

DROP INDEX IF EXISTS idx_content_erasure_events_content;
CREATE UNIQUE INDEX idx_content_erasure_events_content
    ON content_erasure_events(content_id);
CREATE INDEX idx_claims_owner_recorded
    ON claims(owner_resident_id, recorded_at);
CREATE INDEX idx_claims_subject_kind
    ON claims(subject_principal_id, kind, recorded_at);
CREATE INDEX idx_claim_statement_erasure_events_claim
    ON claim_statement_erasure_events(claim_id);
CREATE INDEX idx_claim_statement_erasure_events_content_event
    ON claim_statement_erasure_events(content_erasure_event_id);

CREATE TRIGGER trg_claims_no_delete
BEFORE DELETE ON claims
BEGIN
    SELECT RAISE(ABORT, 'claims is append-only');
END;

CREATE TRIGGER trg_claims_update_contract
BEFORE UPDATE ON claims
WHEN NOT (
    OLD.statement_hash IS NOT NULL
    AND NEW.statement_hash IS NULL
    AND NEW.claim_id IS OLD.claim_id
    AND NEW.canonical_commit_id IS OLD.canonical_commit_id
    AND NEW.owner_resident_id IS OLD.owner_resident_id
    AND NEW.subject_principal_id IS OLD.subject_principal_id
    AND NEW.perspective_principal_id IS OLD.perspective_principal_id
    AND NEW.kind IS OLD.kind
    AND NEW.temporal_kind IS OLD.temporal_kind
    AND NEW.statement_content_id IS OLD.statement_content_id
    AND NEW.statement_hash_algorithm IS OLD.statement_hash_algorithm
    AND NEW.statement_normalization_version IS OLD.statement_normalization_version
    AND NEW.created_by_run_id IS OLD.created_by_run_id
    AND NEW.recorded_at IS OLD.recorded_at
    AND NEW.recorded_tz IS OLD.recorded_tz
    AND EXISTS (
        SELECT 1
        FROM claim_statement_erasure_events event
        JOIN content_erasure_events content_event
          ON content_event.content_erasure_event_id = event.content_erasure_event_id
        JOIN content_objects content
          ON content.content_id = OLD.statement_content_id
        WHERE event.claim_id = OLD.claim_id
          AND event.resident_id = OLD.owner_resident_id
          AND content_event.canonical_commit_id = event.canonical_commit_id
          AND content_event.content_id = OLD.statement_content_id
          AND content_event.erasure_scope = 'content'
          AND content.erasure_state IN ('present', 'erased')
    )
)
BEGIN
    SELECT RAISE(ABORT, 'claims allow only paired statement identity erasure');
END;

CREATE TRIGGER trg_claims_resident_scope
BEFORE INSERT ON claims
WHEN NOT EXISTS (
        SELECT 1 FROM content_objects co
         WHERE co.content_id = NEW.statement_content_id
           AND co.owner_resident_id = NEW.owner_resident_id
           AND co.erasure_state = 'present'
           AND NEW.statement_hash IS NOT NULL
     )
  OR NOT EXISTS (
        SELECT 1 FROM generation_runs gr
         WHERE gr.generation_run_id = NEW.created_by_run_id
           AND gr.resident_id = NEW.owner_resident_id
     )
BEGIN
    SELECT RAISE(ABORT, 'claims contains a cross-resident or erased-content reference');
END;

CREATE TRIGGER trg_claim_statement_erasure_events_scope
BEFORE INSERT ON claim_statement_erasure_events
WHEN NOT EXISTS (
        SELECT 1
        FROM claims claim
        JOIN content_objects content
          ON content.content_id = claim.statement_content_id
        JOIN content_erasure_events content_event
          ON content_event.content_erasure_event_id = NEW.content_erasure_event_id
        WHERE claim.claim_id = NEW.claim_id
          AND claim.statement_hash IS NOT NULL
          AND claim.owner_resident_id = NEW.resident_id
          AND content.owner_resident_id = NEW.resident_id
          AND content.erasure_state IN ('present', 'erased')
          AND content_event.content_id = claim.statement_content_id
          AND content_event.canonical_commit_id = NEW.canonical_commit_id
          AND content_event.erasure_scope = 'content'
    )
BEGIN
    SELECT RAISE(ABORT, 'claim statement erasure event scope or pairing mismatch');
END;

CREATE TRIGGER trg_claim_statement_erasure_events_no_update
BEFORE UPDATE ON claim_statement_erasure_events
BEGIN
    SELECT RAISE(ABORT, 'claim_statement_erasure_events is append-only');
END;

CREATE TRIGGER trg_claim_statement_erasure_events_no_delete
BEFORE DELETE ON claim_statement_erasure_events
BEGIN
    SELECT RAISE(ABORT, 'claim_statement_erasure_events is append-only');
END;

-- M7 logical identity is the entity plus its Canonical commit. The activation
-- family needs the joined revision class because one commit may activate the
-- principles, persona, and memory-policy revisions together.
CREATE UNIQUE INDEX uq_resident_status_transitions_entity_commit
    ON resident_status_transitions(resident_id, canonical_commit_id);
CREATE UNIQUE INDEX uq_resident_revision_activations_entity_commit_revision
    ON resident_revision_activations(resident_id, canonical_commit_id, revision_id);
CREATE UNIQUE INDEX uq_claim_stage_transitions_entity_commit
    ON claim_stage_transitions(claim_id, canonical_commit_id);
CREATE UNIQUE INDEX uq_claim_status_transitions_entity_commit
    ON claim_status_transitions(claim_id, canonical_commit_id);

CREATE TRIGGER trg_resident_revision_activations_identity_contract
BEFORE INSERT ON resident_revision_activations
WHEN EXISTS (
    SELECT 1
      FROM resident_revision_activations existing
      JOIN resident_revisions existing_revision ON existing_revision.revision_id = existing.revision_id
      JOIN resident_revisions incoming_revision ON incoming_revision.revision_id = NEW.revision_id
     WHERE existing.resident_id = NEW.resident_id
       AND existing.canonical_commit_id = NEW.canonical_commit_id
       AND existing_revision.revision_class = incoming_revision.revision_class
       AND incoming_revision.resident_id = NEW.resident_id
)
BEGIN
    SELECT RAISE(ABORT, 'resident revision activation identity already exists for canonical commit');
END;

CREATE TRIGGER trg_resident_status_transitions_identity_contract
BEFORE INSERT ON resident_status_transitions
WHEN EXISTS (
    SELECT 1 FROM resident_status_transitions existing
     WHERE existing.resident_id = NEW.resident_id
       AND existing.canonical_commit_id = NEW.canonical_commit_id
)
BEGIN
    SELECT RAISE(ABORT, 'resident status transition identity already exists for canonical commit');
END;

CREATE TRIGGER trg_claim_stage_transitions_identity_contract
BEFORE INSERT ON claim_stage_transitions
WHEN EXISTS (
    SELECT 1 FROM claim_stage_transitions existing
     WHERE existing.claim_id = NEW.claim_id
       AND existing.canonical_commit_id = NEW.canonical_commit_id
)
AND EXISTS (
    SELECT 1
      FROM claims claim
      JOIN resident_revisions revision ON revision.revision_id = NEW.memory_policy_revision_id
      JOIN generation_runs run ON run.generation_run_id = NEW.generation_run_id
     WHERE claim.claim_id = NEW.claim_id
       AND claim.owner_resident_id = revision.resident_id
       AND claim.owner_resident_id = run.resident_id
)
BEGIN
    SELECT RAISE(ABORT, 'claim stage transition identity already exists for canonical commit');
END;

CREATE TRIGGER trg_claim_status_transitions_identity_contract
BEFORE INSERT ON claim_status_transitions
WHEN EXISTS (
    SELECT 1 FROM claim_status_transitions existing
     WHERE existing.claim_id = NEW.claim_id
       AND existing.canonical_commit_id = NEW.canonical_commit_id
)
BEGIN
    SELECT RAISE(ABORT, 'claim status transition identity already exists for canonical commit');
END;

-- +goose Down
-- +goose StatementBegin
CREATE TEMP TABLE IF NOT EXISTS mahoroba_forward_only_guard (id INTEGER);
CREATE TEMP TRIGGER mahoroba_forward_only_guard_abort
BEFORE INSERT ON mahoroba_forward_only_guard
BEGIN
    SELECT RAISE(ABORT, 'mahoroba: migration 11 is forward-only');
END;
INSERT INTO mahoroba_forward_only_guard DEFAULT VALUES;
-- +goose StatementEnd
DROP TRIGGER IF EXISTS trg_claim_statement_erasure_events_no_delete;
DROP TRIGGER IF EXISTS trg_claim_statement_erasure_events_no_update;
DROP TRIGGER IF EXISTS trg_claim_statement_erasure_events_scope;
DROP TRIGGER IF EXISTS trg_claims_resident_scope;
DROP TRIGGER IF EXISTS trg_claims_update_contract;
DROP TRIGGER IF EXISTS trg_claims_no_delete;
DROP INDEX IF EXISTS idx_claim_statement_erasure_events_content_event;
DROP INDEX IF EXISTS idx_claim_statement_erasure_events_claim;
DROP TABLE IF EXISTS claim_statement_erasure_events;
DROP TRIGGER IF EXISTS trg_claim_status_transitions_identity_contract;
DROP TRIGGER IF EXISTS trg_claim_stage_transitions_identity_contract;
DROP TRIGGER IF EXISTS trg_resident_status_transitions_identity_contract;
DROP TRIGGER IF EXISTS trg_resident_revision_activations_identity_contract;
DROP INDEX IF EXISTS uq_claim_status_transitions_entity_commit;
DROP INDEX IF EXISTS uq_claim_stage_transitions_entity_commit;
DROP INDEX IF EXISTS uq_resident_revision_activations_entity_commit_revision;
DROP INDEX IF EXISTS uq_resident_status_transitions_entity_commit;
