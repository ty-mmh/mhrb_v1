-- +goose Up
-- mahoroba: migration-policy=forward-only
-- M7 Slice 1 extends the legacy integrity finding envelope without rewriting
-- historical rows and fixes the identity of assertion and outcome reads.

ALTER TABLE integrity_findings ADD COLUMN finding_fingerprint BLOB NULL
    CHECK (finding_fingerprint IS NULL OR length(finding_fingerprint) = 32);
ALTER TABLE integrity_findings ADD COLUMN rule_code TEXT NULL;
ALTER TABLE integrity_findings ADD COLUMN target_kind TEXT NULL;
ALTER TABLE integrity_findings ADD COLUMN target_id TEXT NULL;
ALTER TABLE integrity_findings ADD COLUMN target_field TEXT NULL;

CREATE UNIQUE INDEX uq_integrity_findings_fingerprint
    ON integrity_findings(resident_id, finding_fingerprint)
    WHERE finding_fingerprint IS NOT NULL;

CREATE INDEX idx_generation_run_outcomes_latest_v12
    ON generation_run_outcomes(generation_run_id, attempt_no DESC, state, outcome_id);

CREATE UNIQUE INDEX uq_claim_validity_assertions_entity_commit
    ON claim_validity_assertions(claim_id, canonical_commit_id);
CREATE UNIQUE INDEX uq_claim_view_scope_assertions_entity_commit
    ON claim_view_scope_assertions(claim_id, canonical_commit_id);

CREATE TRIGGER trg_integrity_findings_m7_writer_guard
BEFORE INSERT ON integrity_findings
WHEN EXISTS (
        SELECT 1
          FROM pipeline_versions pipeline
         WHERE pipeline.pipeline_version_id = NEW.pipeline_version_id
           AND pipeline.pipeline_kind = 'integrity_check'
     )
 AND (
        NEW.finding_fingerprint IS NULL
     OR NEW.rule_code IS NULL
     OR NEW.target_kind IS NULL
     OR NEW.target_id IS NULL
     OR NEW.target_field IS NULL
 )
BEGIN
    SELECT RAISE(ABORT, 'M7 integrity finding envelope is incomplete');
END;

-- Slice 4 Resident Erasure uses the same one-commit alias pairing contract as
-- Content Erasure. Migration 00011 remains byte-immutable; v12 succeeds its
-- two content-only guards without weakening claim/content/commit/resident
-- identity checks.
DROP TRIGGER trg_claims_update_contract;
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
          AND content_event.erasure_scope IN ('content', 'resident')
          AND content.erasure_state IN ('present', 'erased')
    )
)
BEGIN
    SELECT RAISE(ABORT, 'claims allow only paired statement identity erasure');
END;

DROP TRIGGER trg_claim_statement_erasure_events_scope;
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
          AND content_event.erasure_scope IN ('content', 'resident')
    )
BEGIN
    SELECT RAISE(ABORT, 'claim statement erasure event scope or pairing mismatch');
END;

-- +goose Down
-- +goose StatementBegin
CREATE TEMP TABLE IF NOT EXISTS mahoroba_forward_only_guard (id INTEGER);
CREATE TEMP TRIGGER mahoroba_forward_only_guard_abort
BEFORE INSERT ON mahoroba_forward_only_guard
BEGIN
    SELECT RAISE(ABORT, 'mahoroba: migration 12 is forward-only');
END;
INSERT INTO mahoroba_forward_only_guard DEFAULT VALUES;
-- +goose StatementEnd
DROP TRIGGER IF EXISTS trg_integrity_findings_m7_writer_guard;
DROP INDEX IF EXISTS uq_claim_view_scope_assertions_entity_commit;
DROP INDEX IF EXISTS uq_claim_validity_assertions_entity_commit;
DROP INDEX IF EXISTS idx_generation_run_outcomes_latest_v12;
DROP INDEX IF EXISTS uq_integrity_findings_fingerprint;
