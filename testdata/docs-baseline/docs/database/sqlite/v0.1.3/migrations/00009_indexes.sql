-- +goose Up
CREATE INDEX idx_canonical_commits_resident_seq
    ON canonical_commits(resident_id, commit_seq);

CREATE INDEX idx_principals_kind ON principals(kind);
CREATE INDEX idx_residents_parent ON residents(parent_resident_id, branched_from_seq);
CREATE INDEX idx_resident_revisions_class_time
    ON resident_revisions(resident_id, revision_class, recorded_at);
CREATE INDEX idx_resident_revision_activations_resident_time
    ON resident_revision_activations(resident_id, recorded_at);
CREATE INDEX idx_resident_revision_activations_revision
    ON resident_revision_activations(revision_id, recorded_at);
CREATE INDEX idx_resident_status_transitions_resident_time
    ON resident_status_transitions(resident_id, recorded_at);

CREATE INDEX idx_content_objects_owner_state
    ON content_objects(owner_resident_id, erasure_state, content_class);
CREATE INDEX idx_content_erasure_events_content
    ON content_erasure_events(content_id, recorded_at);
CREATE INDEX idx_content_erasure_events_source
    ON content_erasure_events(source_erasure_event_id)
    WHERE source_erasure_event_id IS NOT NULL;

CREATE INDEX idx_pipeline_versions_kind_key
    ON pipeline_versions(pipeline_kind, version_key);
CREATE INDEX idx_sessionization_policy_key
    ON sessionization_policy_versions(version_key);
CREATE INDEX idx_recall_runs_resident_time
    ON recall_runs(resident_id, recorded_at);
CREATE INDEX idx_generation_runs_resident_purpose_requested
    ON generation_runs(resident_id, purpose, requested_at);
CREATE INDEX idx_generation_run_inputs_run_ordinal
    ON generation_run_inputs(generation_run_id, ordinal);
CREATE INDEX idx_generation_run_outcomes_run_attempt
    ON generation_run_outcomes(generation_run_id, attempt_no DESC, recorded_at DESC);
CREATE UNIQUE INDEX uq_generation_run_outcomes_running
    ON generation_run_outcomes(generation_run_id, attempt_no)
    WHERE state = 'running';
CREATE UNIQUE INDEX uq_generation_run_outcomes_terminal
    ON generation_run_outcomes(generation_run_id, attempt_no)
    WHERE state IN ('succeeded', 'failed', 'cancelled');

CREATE INDEX idx_events_resident_type_seq
    ON events(resident_id, event_type, seq);
CREATE INDEX idx_events_resident_recorded
    ON events(resident_id, recorded_at, seq);
CREATE INDEX idx_events_actor_recorded
    ON events(actor_principal_id, recorded_at);

CREATE INDEX idx_claims_owner_recorded
    ON claims(owner_resident_id, recorded_at);
CREATE INDEX idx_claims_subject_kind
    ON claims(subject_principal_id, kind, recorded_at);
CREATE INDEX idx_claim_evidence_claim
    ON claim_evidence(claim_id, recorded_at);
CREATE INDEX idx_claim_evidence_event
    ON claim_evidence(event_id, claim_id);
CREATE INDEX idx_claim_evidence_source
    ON claim_evidence(source_evidence_id)
    WHERE source_evidence_id IS NOT NULL;
CREATE INDEX idx_claim_stage_transitions_claim
    ON claim_stage_transitions(claim_id, recorded_at);
CREATE INDEX idx_claim_stage_transition_dependencies_dependency_claim
    ON claim_stage_transition_dependencies(dependency_claim_id);
CREATE INDEX idx_claim_relations_from
    ON claim_relations(from_claim_id, relation_type);
CREATE INDEX idx_claim_relations_to
    ON claim_relations(to_claim_id, relation_type);
CREATE INDEX idx_integrity_findings_claim
    ON integrity_findings(claim_id, recorded_at)
    WHERE claim_id IS NOT NULL;
CREATE INDEX idx_claim_status_transitions_claim
    ON claim_status_transitions(claim_id, recorded_at);
CREATE INDEX idx_claim_status_trigger_evidence
    ON claim_status_transitions(trigger_evidence_id)
    WHERE trigger_evidence_id IS NOT NULL;
CREATE INDEX idx_claim_status_trigger_relation
    ON claim_status_transitions(trigger_claim_relation_id)
    WHERE trigger_claim_relation_id IS NOT NULL;
CREATE INDEX idx_claim_status_trigger_finding
    ON claim_status_transitions(trigger_integrity_finding_id)
    WHERE trigger_integrity_finding_id IS NOT NULL;
CREATE INDEX idx_claim_validity_claim
    ON claim_validity_assertions(claim_id, recorded_at);
CREATE INDEX idx_claim_view_scope_claim
    ON claim_view_scope_assertions(claim_id, recorded_at);
CREATE INDEX idx_claim_usages_claim_time
    ON claim_usages(claim_id, recorded_at);
CREATE INDEX idx_claim_usages_recall
    ON claim_usages(recall_run_id, usage_type, ordinal)
    WHERE recall_run_id IS NOT NULL;
CREATE INDEX idx_claim_usages_generation
    ON claim_usages(generation_run_id, usage_type, ordinal)
    WHERE generation_run_id IS NOT NULL;

CREATE INDEX idx_claim_states_resident_status_stage
    ON claim_states(resident_id, status, stage);
CREATE INDEX idx_claim_states_resident_salience
    ON claim_states(resident_id, salience DESC);
CREATE INDEX idx_content_references_blob
    ON content_references(dedupe_scope_id, blob_hash_algorithm, blob_hash)
    WHERE blob_hash IS NOT NULL;
CREATE INDEX idx_projection_watermarks_source
    ON projection_watermarks(source_commit_seq);

CREATE INDEX idx_analytics_outbox_pending
    ON analytics_outbox(status, available_at)
    WHERE status IN ('pending', 'failed');
CREATE INDEX idx_search_outbox_pending
    ON search_outbox(status, available_at)
    WHERE status IN ('pending', 'failed');

-- v0.1.2: monotonic claim-stage arrival and Store-wide event-hash defense.
CREATE UNIQUE INDEX uq_claim_stage_transitions_stage
    ON claim_stage_transitions(claim_id, to_stage);

CREATE UNIQUE INDEX uq_events_event_hash
    ON events(event_hash_algorithm, event_hash_domain, event_hash);


-- +goose Down

DROP INDEX IF EXISTS uq_events_event_hash;
DROP INDEX IF EXISTS uq_claim_stage_transitions_stage;
DROP INDEX IF EXISTS idx_search_outbox_pending;
DROP INDEX IF EXISTS idx_analytics_outbox_pending;
DROP INDEX IF EXISTS idx_projection_watermarks_source;
DROP INDEX IF EXISTS idx_content_references_blob;
DROP INDEX IF EXISTS idx_claim_states_resident_salience;
DROP INDEX IF EXISTS idx_claim_states_resident_status_stage;
DROP INDEX IF EXISTS idx_claim_usages_generation;
DROP INDEX IF EXISTS idx_claim_usages_recall;
DROP INDEX IF EXISTS idx_claim_usages_claim_time;
DROP INDEX IF EXISTS idx_claim_view_scope_claim;
DROP INDEX IF EXISTS idx_claim_validity_claim;
DROP INDEX IF EXISTS idx_claim_status_trigger_finding;
DROP INDEX IF EXISTS idx_claim_status_trigger_relation;
DROP INDEX IF EXISTS idx_claim_status_trigger_evidence;
DROP INDEX IF EXISTS idx_claim_status_transitions_claim;
DROP INDEX IF EXISTS idx_integrity_findings_claim;
DROP INDEX IF EXISTS idx_claim_relations_to;
DROP INDEX IF EXISTS idx_claim_relations_from;
DROP INDEX IF EXISTS idx_claim_stage_transition_dependencies_dependency_claim;
DROP INDEX IF EXISTS idx_claim_stage_transitions_claim;
DROP INDEX IF EXISTS idx_claim_evidence_source;
DROP INDEX IF EXISTS idx_claim_evidence_event;
DROP INDEX IF EXISTS idx_claim_evidence_claim;
DROP INDEX IF EXISTS idx_claims_subject_kind;
DROP INDEX IF EXISTS idx_claims_owner_recorded;
DROP INDEX IF EXISTS idx_events_actor_recorded;
DROP INDEX IF EXISTS idx_events_resident_recorded;
DROP INDEX IF EXISTS idx_events_resident_type_seq;
DROP INDEX IF EXISTS uq_generation_run_outcomes_terminal;
DROP INDEX IF EXISTS uq_generation_run_outcomes_running;
DROP INDEX IF EXISTS idx_generation_run_outcomes_run_attempt;
DROP INDEX IF EXISTS idx_generation_run_inputs_run_ordinal;
DROP INDEX IF EXISTS idx_generation_runs_resident_purpose_requested;
DROP INDEX IF EXISTS idx_recall_runs_resident_time;
DROP INDEX IF EXISTS idx_sessionization_policy_key;
DROP INDEX IF EXISTS idx_pipeline_versions_kind_key;
DROP INDEX IF EXISTS idx_content_erasure_events_source;
DROP INDEX IF EXISTS idx_content_erasure_events_content;
DROP INDEX IF EXISTS idx_content_objects_owner_state;
DROP INDEX IF EXISTS idx_resident_status_transitions_resident_time;
DROP INDEX IF EXISTS idx_resident_revision_activations_revision;
DROP INDEX IF EXISTS idx_resident_revision_activations_resident_time;
DROP INDEX IF EXISTS idx_resident_revisions_class_time;
DROP INDEX IF EXISTS idx_residents_parent;
DROP INDEX IF EXISTS idx_principals_kind;
DROP INDEX IF EXISTS idx_canonical_commits_resident_seq;
