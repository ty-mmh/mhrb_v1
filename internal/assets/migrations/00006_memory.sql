-- +goose Up
CREATE TABLE claims (
    claim_id TEXT PRIMARY KEY CHECK (length(claim_id) = 26 AND claim_id = upper(claim_id) AND substr(claim_id, 1, 1) BETWEEN '0' AND '7' AND claim_id NOT GLOB '*[^0123456789ABCDEFGHJKMNPQRSTVWXYZ]*'),
    canonical_commit_id TEXT NOT NULL CHECK (length(canonical_commit_id) = 26 AND canonical_commit_id = upper(canonical_commit_id) AND substr(canonical_commit_id, 1, 1) BETWEEN '0' AND '7' AND canonical_commit_id NOT GLOB '*[^0123456789ABCDEFGHJKMNPQRSTVWXYZ]*'),
    owner_resident_id TEXT NOT NULL CHECK (length(owner_resident_id) = 26 AND owner_resident_id = upper(owner_resident_id) AND substr(owner_resident_id, 1, 1) BETWEEN '0' AND '7' AND owner_resident_id NOT GLOB '*[^0123456789ABCDEFGHJKMNPQRSTVWXYZ]*'),
    subject_principal_id TEXT NOT NULL CHECK (length(subject_principal_id) = 26 AND subject_principal_id = upper(subject_principal_id) AND substr(subject_principal_id, 1, 1) BETWEEN '0' AND '7' AND subject_principal_id NOT GLOB '*[^0123456789ABCDEFGHJKMNPQRSTVWXYZ]*'),
    perspective_principal_id TEXT NOT NULL CHECK (length(perspective_principal_id) = 26 AND perspective_principal_id = upper(perspective_principal_id) AND substr(perspective_principal_id, 1, 1) BETWEEN '0' AND '7' AND perspective_principal_id NOT GLOB '*[^0123456789ABCDEFGHJKMNPQRSTVWXYZ]*'),
    kind TEXT NULL CHECK (kind IS NULL OR kind IN ('direct', 'other', 'meta')),
    temporal_kind TEXT NOT NULL CHECK (temporal_kind IN ('stable', 'volatile', 'episodic')),
    statement_content_id TEXT NOT NULL CHECK (length(statement_content_id) = 26 AND statement_content_id = upper(statement_content_id) AND substr(statement_content_id, 1, 1) BETWEEN '0' AND '7' AND statement_content_id NOT GLOB '*[^0123456789ABCDEFGHJKMNPQRSTVWXYZ]*'),
    statement_hash BLOB NOT NULL CHECK (length(statement_hash) = 32),
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

CREATE TABLE claim_evidence (
    evidence_id TEXT PRIMARY KEY CHECK (length(evidence_id) = 26 AND evidence_id = upper(evidence_id) AND substr(evidence_id, 1, 1) BETWEEN '0' AND '7' AND evidence_id NOT GLOB '*[^0123456789ABCDEFGHJKMNPQRSTVWXYZ]*'),
    canonical_commit_id TEXT NOT NULL CHECK (length(canonical_commit_id) = 26 AND canonical_commit_id = upper(canonical_commit_id) AND substr(canonical_commit_id, 1, 1) BETWEEN '0' AND '7' AND canonical_commit_id NOT GLOB '*[^0123456789ABCDEFGHJKMNPQRSTVWXYZ]*'),
    claim_id TEXT NOT NULL CHECK (length(claim_id) = 26 AND claim_id = upper(claim_id) AND substr(claim_id, 1, 1) BETWEEN '0' AND '7' AND claim_id NOT GLOB '*[^0123456789ABCDEFGHJKMNPQRSTVWXYZ]*'),
    event_id TEXT NOT NULL CHECK (length(event_id) = 26 AND event_id = upper(event_id) AND substr(event_id, 1, 1) BETWEEN '0' AND '7' AND event_id NOT GLOB '*[^0123456789ABCDEFGHJKMNPQRSTVWXYZ]*'),
    polarity TEXT NOT NULL CHECK (polarity IN ('support', 'contradict')),
    grade TEXT NOT NULL CHECK (grade IN ('stated', 'observed', 'inferred')),
    trust_level TEXT NOT NULL CHECK (trust_level IN ('trusted', 'untrusted')),
    weight INTEGER NOT NULL CHECK (weight >= 0),
    derivation TEXT NOT NULL CHECK (derivation IN ('extracted', 'inherited')),
    source_evidence_id TEXT NULL CHECK (source_evidence_id IS NULL OR (length(source_evidence_id) = 26 AND source_evidence_id = upper(source_evidence_id) AND substr(source_evidence_id, 1, 1) BETWEEN '0' AND '7' AND source_evidence_id NOT GLOB '*[^0123456789ABCDEFGHJKMNPQRSTVWXYZ]*')),
    memory_policy_revision_id TEXT NOT NULL CHECK (length(memory_policy_revision_id) = 26 AND memory_policy_revision_id = upper(memory_policy_revision_id) AND substr(memory_policy_revision_id, 1, 1) BETWEEN '0' AND '7' AND memory_policy_revision_id NOT GLOB '*[^0123456789ABCDEFGHJKMNPQRSTVWXYZ]*'),
    created_by_run_id TEXT NOT NULL CHECK (length(created_by_run_id) = 26 AND created_by_run_id = upper(created_by_run_id) AND substr(created_by_run_id, 1, 1) BETWEEN '0' AND '7' AND created_by_run_id NOT GLOB '*[^0123456789ABCDEFGHJKMNPQRSTVWXYZ]*'),
    reason_code TEXT NOT NULL,
    reason_content_id TEXT NULL CHECK (reason_content_id IS NULL OR (length(reason_content_id) = 26 AND reason_content_id = upper(reason_content_id) AND substr(reason_content_id, 1, 1) BETWEEN '0' AND '7' AND reason_content_id NOT GLOB '*[^0123456789ABCDEFGHJKMNPQRSTVWXYZ]*')),
    recorded_at INTEGER NOT NULL,
    recorded_tz TEXT NOT NULL CHECK (length(recorded_tz) > 0),
    CHECK (
        (derivation = 'extracted' AND source_evidence_id IS NULL)
        OR
        (derivation = 'inherited' AND source_evidence_id IS NOT NULL)
    ),
    CHECK (source_evidence_id IS NULL OR source_evidence_id <> evidence_id),
    FOREIGN KEY (canonical_commit_id) REFERENCES canonical_commits(canonical_commit_id)
        DEFERRABLE INITIALLY DEFERRED,
    FOREIGN KEY (claim_id) REFERENCES claims(claim_id)
        DEFERRABLE INITIALLY DEFERRED,
    FOREIGN KEY (event_id) REFERENCES events(event_id)
        DEFERRABLE INITIALLY DEFERRED,
    FOREIGN KEY (source_evidence_id) REFERENCES claim_evidence(evidence_id)
        DEFERRABLE INITIALLY DEFERRED,
    FOREIGN KEY (memory_policy_revision_id) REFERENCES resident_revisions(revision_id)
        DEFERRABLE INITIALLY DEFERRED,
    FOREIGN KEY (created_by_run_id) REFERENCES generation_runs(generation_run_id)
        DEFERRABLE INITIALLY DEFERRED,
    FOREIGN KEY (reason_content_id) REFERENCES content_objects(content_id)
        DEFERRABLE INITIALLY DEFERRED
) STRICT;

CREATE TABLE claim_stage_transitions (
    stage_transition_id TEXT PRIMARY KEY CHECK (length(stage_transition_id) = 26 AND stage_transition_id = upper(stage_transition_id) AND substr(stage_transition_id, 1, 1) BETWEEN '0' AND '7' AND stage_transition_id NOT GLOB '*[^0123456789ABCDEFGHJKMNPQRSTVWXYZ]*'),
    canonical_commit_id TEXT NOT NULL CHECK (length(canonical_commit_id) = 26 AND canonical_commit_id = upper(canonical_commit_id) AND substr(canonical_commit_id, 1, 1) BETWEEN '0' AND '7' AND canonical_commit_id NOT GLOB '*[^0123456789ABCDEFGHJKMNPQRSTVWXYZ]*'),
    claim_id TEXT NOT NULL CHECK (length(claim_id) = 26 AND claim_id = upper(claim_id) AND substr(claim_id, 1, 1) BETWEEN '0' AND '7' AND claim_id NOT GLOB '*[^0123456789ABCDEFGHJKMNPQRSTVWXYZ]*'),
    from_stage TEXT NULL CHECK (from_stage IS NULL OR from_stage IN ('floating', 'sediment', 'settled')),
    to_stage TEXT NOT NULL CHECK (to_stage IN ('floating', 'sediment', 'settled')),
    gate_metrics TEXT NOT NULL CHECK (json_valid(gate_metrics)),
    pipeline_version_id TEXT NOT NULL CHECK (length(pipeline_version_id) = 26 AND pipeline_version_id = upper(pipeline_version_id) AND substr(pipeline_version_id, 1, 1) BETWEEN '0' AND '7' AND pipeline_version_id NOT GLOB '*[^0123456789ABCDEFGHJKMNPQRSTVWXYZ]*'),
    memory_policy_revision_id TEXT NOT NULL CHECK (length(memory_policy_revision_id) = 26 AND memory_policy_revision_id = upper(memory_policy_revision_id) AND substr(memory_policy_revision_id, 1, 1) BETWEEN '0' AND '7' AND memory_policy_revision_id NOT GLOB '*[^0123456789ABCDEFGHJKMNPQRSTVWXYZ]*'),
    generation_run_id TEXT NULL CHECK (generation_run_id IS NULL OR (length(generation_run_id) = 26 AND generation_run_id = upper(generation_run_id) AND substr(generation_run_id, 1, 1) BETWEEN '0' AND '7' AND generation_run_id NOT GLOB '*[^0123456789ABCDEFGHJKMNPQRSTVWXYZ]*')),
    reason_code TEXT NOT NULL,
    reason_content_id TEXT NULL CHECK (reason_content_id IS NULL OR (length(reason_content_id) = 26 AND reason_content_id = upper(reason_content_id) AND substr(reason_content_id, 1, 1) BETWEEN '0' AND '7' AND reason_content_id NOT GLOB '*[^0123456789ABCDEFGHJKMNPQRSTVWXYZ]*')),
    occurred_at INTEGER NOT NULL,
    occurred_tz TEXT NOT NULL CHECK (length(occurred_tz) > 0),
    recorded_at INTEGER NOT NULL,
    recorded_tz TEXT NOT NULL CHECK (length(recorded_tz) > 0),
    CHECK (
        (from_stage IS NULL AND to_stage = 'floating') OR
        (from_stage = 'floating' AND to_stage = 'sediment') OR
        (from_stage = 'sediment' AND to_stage = 'settled')
    ),
    FOREIGN KEY (canonical_commit_id) REFERENCES canonical_commits(canonical_commit_id)
        DEFERRABLE INITIALLY DEFERRED,
    FOREIGN KEY (claim_id) REFERENCES claims(claim_id)
        DEFERRABLE INITIALLY DEFERRED,
    FOREIGN KEY (pipeline_version_id) REFERENCES pipeline_versions(pipeline_version_id)
        DEFERRABLE INITIALLY DEFERRED,
    FOREIGN KEY (memory_policy_revision_id) REFERENCES resident_revisions(revision_id)
        DEFERRABLE INITIALLY DEFERRED,
    FOREIGN KEY (generation_run_id) REFERENCES generation_runs(generation_run_id)
        DEFERRABLE INITIALLY DEFERRED,
    FOREIGN KEY (reason_content_id) REFERENCES content_objects(content_id)
        DEFERRABLE INITIALLY DEFERRED
) STRICT;

CREATE TABLE claim_stage_transition_dependencies (
    stage_transition_dependency_id TEXT PRIMARY KEY CHECK (length(stage_transition_dependency_id) = 26 AND stage_transition_dependency_id = upper(stage_transition_dependency_id) AND substr(stage_transition_dependency_id, 1, 1) BETWEEN '0' AND '7' AND stage_transition_dependency_id NOT GLOB '*[^0123456789ABCDEFGHJKMNPQRSTVWXYZ]*'),
    canonical_commit_id TEXT NOT NULL CHECK (length(canonical_commit_id) = 26 AND canonical_commit_id = upper(canonical_commit_id) AND substr(canonical_commit_id, 1, 1) BETWEEN '0' AND '7' AND canonical_commit_id NOT GLOB '*[^0123456789ABCDEFGHJKMNPQRSTVWXYZ]*'),
    stage_transition_id TEXT NOT NULL CHECK (length(stage_transition_id) = 26 AND stage_transition_id = upper(stage_transition_id) AND substr(stage_transition_id, 1, 1) BETWEEN '0' AND '7' AND stage_transition_id NOT GLOB '*[^0123456789ABCDEFGHJKMNPQRSTVWXYZ]*'),
    dependency_kind TEXT NOT NULL CHECK (dependency_kind = 'meta_alignment'),
    dependency_claim_id TEXT NOT NULL CHECK (length(dependency_claim_id) = 26 AND dependency_claim_id = upper(dependency_claim_id) AND substr(dependency_claim_id, 1, 1) BETWEEN '0' AND '7' AND dependency_claim_id NOT GLOB '*[^0123456789ABCDEFGHJKMNPQRSTVWXYZ]*'),
    UNIQUE (stage_transition_id, dependency_kind, dependency_claim_id),
    FOREIGN KEY (canonical_commit_id) REFERENCES canonical_commits(canonical_commit_id)
        DEFERRABLE INITIALLY DEFERRED,
    FOREIGN KEY (stage_transition_id) REFERENCES claim_stage_transitions(stage_transition_id)
        DEFERRABLE INITIALLY DEFERRED,
    FOREIGN KEY (dependency_claim_id) REFERENCES claims(claim_id)
        DEFERRABLE INITIALLY DEFERRED
) STRICT;

CREATE TABLE claim_relations (
    claim_relation_id TEXT PRIMARY KEY CHECK (length(claim_relation_id) = 26 AND claim_relation_id = upper(claim_relation_id) AND substr(claim_relation_id, 1, 1) BETWEEN '0' AND '7' AND claim_relation_id NOT GLOB '*[^0123456789ABCDEFGHJKMNPQRSTVWXYZ]*'),
    canonical_commit_id TEXT NOT NULL CHECK (length(canonical_commit_id) = 26 AND canonical_commit_id = upper(canonical_commit_id) AND substr(canonical_commit_id, 1, 1) BETWEEN '0' AND '7' AND canonical_commit_id NOT GLOB '*[^0123456789ABCDEFGHJKMNPQRSTVWXYZ]*'),
    from_claim_id TEXT NOT NULL CHECK (length(from_claim_id) = 26 AND from_claim_id = upper(from_claim_id) AND substr(from_claim_id, 1, 1) BETWEEN '0' AND '7' AND from_claim_id NOT GLOB '*[^0123456789ABCDEFGHJKMNPQRSTVWXYZ]*'),
    to_claim_id TEXT NOT NULL CHECK (length(to_claim_id) = 26 AND to_claim_id = upper(to_claim_id) AND substr(to_claim_id, 1, 1) BETWEEN '0' AND '7' AND to_claim_id NOT GLOB '*[^0123456789ABCDEFGHJKMNPQRSTVWXYZ]*'),
    relation_type TEXT NOT NULL,
    reason_code TEXT NOT NULL,
    reason_content_id TEXT NULL CHECK (reason_content_id IS NULL OR (length(reason_content_id) = 26 AND reason_content_id = upper(reason_content_id) AND substr(reason_content_id, 1, 1) BETWEEN '0' AND '7' AND reason_content_id NOT GLOB '*[^0123456789ABCDEFGHJKMNPQRSTVWXYZ]*')),
    generation_run_id TEXT NULL CHECK (generation_run_id IS NULL OR (length(generation_run_id) = 26 AND generation_run_id = upper(generation_run_id) AND substr(generation_run_id, 1, 1) BETWEEN '0' AND '7' AND generation_run_id NOT GLOB '*[^0123456789ABCDEFGHJKMNPQRSTVWXYZ]*')),
    occurred_at INTEGER NOT NULL,
    occurred_tz TEXT NOT NULL CHECK (length(occurred_tz) > 0),
    recorded_at INTEGER NOT NULL,
    recorded_tz TEXT NOT NULL CHECK (length(recorded_tz) > 0),
    CHECK (from_claim_id <> to_claim_id),
    UNIQUE (from_claim_id, to_claim_id, relation_type),
    FOREIGN KEY (canonical_commit_id) REFERENCES canonical_commits(canonical_commit_id)
        DEFERRABLE INITIALLY DEFERRED,
    FOREIGN KEY (from_claim_id) REFERENCES claims(claim_id)
        DEFERRABLE INITIALLY DEFERRED,
    FOREIGN KEY (to_claim_id) REFERENCES claims(claim_id)
        DEFERRABLE INITIALLY DEFERRED,
    FOREIGN KEY (reason_content_id) REFERENCES content_objects(content_id)
        DEFERRABLE INITIALLY DEFERRED,
    FOREIGN KEY (generation_run_id) REFERENCES generation_runs(generation_run_id)
        DEFERRABLE INITIALLY DEFERRED
) STRICT;

CREATE TABLE integrity_findings (
    integrity_finding_id TEXT PRIMARY KEY CHECK (length(integrity_finding_id) = 26 AND integrity_finding_id = upper(integrity_finding_id) AND substr(integrity_finding_id, 1, 1) BETWEEN '0' AND '7' AND integrity_finding_id NOT GLOB '*[^0123456789ABCDEFGHJKMNPQRSTVWXYZ]*'),
    canonical_commit_id TEXT NOT NULL CHECK (length(canonical_commit_id) = 26 AND canonical_commit_id = upper(canonical_commit_id) AND substr(canonical_commit_id, 1, 1) BETWEEN '0' AND '7' AND canonical_commit_id NOT GLOB '*[^0123456789ABCDEFGHJKMNPQRSTVWXYZ]*'),
    resident_id TEXT NOT NULL CHECK (length(resident_id) = 26 AND resident_id = upper(resident_id) AND substr(resident_id, 1, 1) BETWEEN '0' AND '7' AND resident_id NOT GLOB '*[^0123456789ABCDEFGHJKMNPQRSTVWXYZ]*'),
    claim_id TEXT NULL CHECK (claim_id IS NULL OR (length(claim_id) = 26 AND claim_id = upper(claim_id) AND substr(claim_id, 1, 1) BETWEEN '0' AND '7' AND claim_id NOT GLOB '*[^0123456789ABCDEFGHJKMNPQRSTVWXYZ]*')),
    finding_kind TEXT NOT NULL CHECK (finding_kind IN (
        'required_provenance_erased', 'provenance_unresolvable', 'canonical_invariant_violation'
    )),
    source_content_erasure_event_id TEXT NULL CHECK (source_content_erasure_event_id IS NULL OR (length(source_content_erasure_event_id) = 26 AND source_content_erasure_event_id = upper(source_content_erasure_event_id) AND substr(source_content_erasure_event_id, 1, 1) BETWEEN '0' AND '7' AND source_content_erasure_event_id NOT GLOB '*[^0123456789ABCDEFGHJKMNPQRSTVWXYZ]*')),
    pipeline_version_id TEXT NOT NULL CHECK (length(pipeline_version_id) = 26 AND pipeline_version_id = upper(pipeline_version_id) AND substr(pipeline_version_id, 1, 1) BETWEEN '0' AND '7' AND pipeline_version_id NOT GLOB '*[^0123456789ABCDEFGHJKMNPQRSTVWXYZ]*'),
    details_content_id TEXT NULL CHECK (details_content_id IS NULL OR (length(details_content_id) = 26 AND details_content_id = upper(details_content_id) AND substr(details_content_id, 1, 1) BETWEEN '0' AND '7' AND details_content_id NOT GLOB '*[^0123456789ABCDEFGHJKMNPQRSTVWXYZ]*')),
    occurred_at INTEGER NOT NULL,
    occurred_tz TEXT NOT NULL CHECK (length(occurred_tz) > 0),
    recorded_at INTEGER NOT NULL,
    recorded_tz TEXT NOT NULL CHECK (length(recorded_tz) > 0),
    FOREIGN KEY (canonical_commit_id) REFERENCES canonical_commits(canonical_commit_id)
        DEFERRABLE INITIALLY DEFERRED,
    FOREIGN KEY (resident_id) REFERENCES residents(resident_id)
        DEFERRABLE INITIALLY DEFERRED,
    FOREIGN KEY (claim_id) REFERENCES claims(claim_id)
        DEFERRABLE INITIALLY DEFERRED,
    FOREIGN KEY (source_content_erasure_event_id) REFERENCES content_erasure_events(content_erasure_event_id)
        DEFERRABLE INITIALLY DEFERRED,
    FOREIGN KEY (pipeline_version_id) REFERENCES pipeline_versions(pipeline_version_id)
        DEFERRABLE INITIALLY DEFERRED,
    FOREIGN KEY (details_content_id) REFERENCES content_objects(content_id)
        DEFERRABLE INITIALLY DEFERRED
) STRICT;

CREATE TABLE claim_status_transitions (
    status_transition_id TEXT PRIMARY KEY CHECK (length(status_transition_id) = 26 AND status_transition_id = upper(status_transition_id) AND substr(status_transition_id, 1, 1) BETWEEN '0' AND '7' AND status_transition_id NOT GLOB '*[^0123456789ABCDEFGHJKMNPQRSTVWXYZ]*'),
    canonical_commit_id TEXT NOT NULL CHECK (length(canonical_commit_id) = 26 AND canonical_commit_id = upper(canonical_commit_id) AND substr(canonical_commit_id, 1, 1) BETWEEN '0' AND '7' AND canonical_commit_id NOT GLOB '*[^0123456789ABCDEFGHJKMNPQRSTVWXYZ]*'),
    claim_id TEXT NOT NULL CHECK (length(claim_id) = 26 AND claim_id = upper(claim_id) AND substr(claim_id, 1, 1) BETWEEN '0' AND '7' AND claim_id NOT GLOB '*[^0123456789ABCDEFGHJKMNPQRSTVWXYZ]*'),
    from_status TEXT NOT NULL CHECK (from_status IN ('active', 'invalidated', 'superseded', 'quarantined')),
    to_status TEXT NOT NULL CHECK (to_status IN ('active', 'invalidated', 'superseded', 'quarantined')),
    decision_kind TEXT NOT NULL CHECK (decision_kind IN ('human', 'automatic')),
    actor_principal_id TEXT NULL CHECK (actor_principal_id IS NULL OR (length(actor_principal_id) = 26 AND actor_principal_id = upper(actor_principal_id) AND substr(actor_principal_id, 1, 1) BETWEEN '0' AND '7' AND actor_principal_id NOT GLOB '*[^0123456789ABCDEFGHJKMNPQRSTVWXYZ]*')),
    trigger_kind TEXT NULL CHECK (trigger_kind IS NULL OR trigger_kind IN ('event', 'claim_evidence', 'claim_relation', 'integrity_finding')),
    trigger_event_id TEXT NULL CHECK (trigger_event_id IS NULL OR (length(trigger_event_id) = 26 AND trigger_event_id = upper(trigger_event_id) AND substr(trigger_event_id, 1, 1) BETWEEN '0' AND '7' AND trigger_event_id NOT GLOB '*[^0123456789ABCDEFGHJKMNPQRSTVWXYZ]*')),
    trigger_evidence_id TEXT NULL CHECK (trigger_evidence_id IS NULL OR (length(trigger_evidence_id) = 26 AND trigger_evidence_id = upper(trigger_evidence_id) AND substr(trigger_evidence_id, 1, 1) BETWEEN '0' AND '7' AND trigger_evidence_id NOT GLOB '*[^0123456789ABCDEFGHJKMNPQRSTVWXYZ]*')),
    trigger_claim_relation_id TEXT NULL CHECK (trigger_claim_relation_id IS NULL OR (length(trigger_claim_relation_id) = 26 AND trigger_claim_relation_id = upper(trigger_claim_relation_id) AND substr(trigger_claim_relation_id, 1, 1) BETWEEN '0' AND '7' AND trigger_claim_relation_id NOT GLOB '*[^0123456789ABCDEFGHJKMNPQRSTVWXYZ]*')),
    trigger_integrity_finding_id TEXT NULL CHECK (trigger_integrity_finding_id IS NULL OR (length(trigger_integrity_finding_id) = 26 AND trigger_integrity_finding_id = upper(trigger_integrity_finding_id) AND substr(trigger_integrity_finding_id, 1, 1) BETWEEN '0' AND '7' AND trigger_integrity_finding_id NOT GLOB '*[^0123456789ABCDEFGHJKMNPQRSTVWXYZ]*')),
    pipeline_version_id TEXT NULL CHECK (pipeline_version_id IS NULL OR (length(pipeline_version_id) = 26 AND pipeline_version_id = upper(pipeline_version_id) AND substr(pipeline_version_id, 1, 1) BETWEEN '0' AND '7' AND pipeline_version_id NOT GLOB '*[^0123456789ABCDEFGHJKMNPQRSTVWXYZ]*')),
    memory_policy_revision_id TEXT NULL CHECK (memory_policy_revision_id IS NULL OR (length(memory_policy_revision_id) = 26 AND memory_policy_revision_id = upper(memory_policy_revision_id) AND substr(memory_policy_revision_id, 1, 1) BETWEEN '0' AND '7' AND memory_policy_revision_id NOT GLOB '*[^0123456789ABCDEFGHJKMNPQRSTVWXYZ]*')),
    gate_metrics TEXT NULL CHECK ((gate_metrics IS NULL OR json_valid(gate_metrics))),
    decision_reason_code TEXT NOT NULL,
    decision_reason_content_id TEXT NULL CHECK (decision_reason_content_id IS NULL OR (length(decision_reason_content_id) = 26 AND decision_reason_content_id = upper(decision_reason_content_id) AND substr(decision_reason_content_id, 1, 1) BETWEEN '0' AND '7' AND decision_reason_content_id NOT GLOB '*[^0123456789ABCDEFGHJKMNPQRSTVWXYZ]*')),
    occurred_at INTEGER NOT NULL,
    occurred_tz TEXT NOT NULL CHECK (length(occurred_tz) > 0),
    recorded_at INTEGER NOT NULL,
    recorded_tz TEXT NOT NULL CHECK (length(recorded_tz) > 0),
    CHECK (from_status <> to_status),
    CHECK (
        (from_status = 'active' AND to_status IN ('invalidated', 'superseded', 'quarantined'))
        OR
        (from_status = 'quarantined' AND to_status IN ('active', 'invalidated', 'superseded'))
    ),
    CHECK (
        (trigger_kind IS NULL AND trigger_event_id IS NULL AND trigger_evidence_id IS NULL AND trigger_claim_relation_id IS NULL AND trigger_integrity_finding_id IS NULL)
        OR
        (trigger_kind = 'event' AND trigger_event_id IS NOT NULL AND trigger_evidence_id IS NULL AND trigger_claim_relation_id IS NULL AND trigger_integrity_finding_id IS NULL)
        OR
        (trigger_kind = 'claim_evidence' AND trigger_event_id IS NULL AND trigger_evidence_id IS NOT NULL AND trigger_claim_relation_id IS NULL AND trigger_integrity_finding_id IS NULL)
        OR
        (trigger_kind = 'claim_relation' AND trigger_event_id IS NULL AND trigger_evidence_id IS NULL AND trigger_claim_relation_id IS NOT NULL AND trigger_integrity_finding_id IS NULL)
        OR
        (trigger_kind = 'integrity_finding' AND trigger_event_id IS NULL AND trigger_evidence_id IS NULL AND trigger_claim_relation_id IS NULL AND trigger_integrity_finding_id IS NOT NULL)
    ),
    CHECK (
        (decision_kind = 'human' AND actor_principal_id IS NOT NULL)
        OR
        (decision_kind = 'automatic' AND trigger_kind IS NOT NULL AND pipeline_version_id IS NOT NULL AND gate_metrics IS NOT NULL)
    ),
    CHECK (
        decision_kind = 'human' OR
        (decision_reason_code = 'explicit_supersession' AND trigger_kind = 'claim_relation' AND to_status = 'superseded') OR
        (decision_reason_code = 'explicit_correction' AND trigger_kind = 'claim_evidence' AND to_status = 'invalidated') OR
        (decision_reason_code = 'structural_quarantine' AND trigger_kind = 'integrity_finding' AND to_status = 'quarantined')
    ),
    FOREIGN KEY (canonical_commit_id) REFERENCES canonical_commits(canonical_commit_id)
        DEFERRABLE INITIALLY DEFERRED,
    FOREIGN KEY (claim_id) REFERENCES claims(claim_id)
        DEFERRABLE INITIALLY DEFERRED,
    FOREIGN KEY (actor_principal_id) REFERENCES principals(principal_id)
        DEFERRABLE INITIALLY DEFERRED,
    FOREIGN KEY (trigger_event_id) REFERENCES events(event_id)
        DEFERRABLE INITIALLY DEFERRED,
    FOREIGN KEY (trigger_evidence_id) REFERENCES claim_evidence(evidence_id)
        DEFERRABLE INITIALLY DEFERRED,
    FOREIGN KEY (trigger_claim_relation_id) REFERENCES claim_relations(claim_relation_id)
        DEFERRABLE INITIALLY DEFERRED,
    FOREIGN KEY (trigger_integrity_finding_id) REFERENCES integrity_findings(integrity_finding_id)
        DEFERRABLE INITIALLY DEFERRED,
    FOREIGN KEY (pipeline_version_id) REFERENCES pipeline_versions(pipeline_version_id)
        DEFERRABLE INITIALLY DEFERRED,
    FOREIGN KEY (memory_policy_revision_id) REFERENCES resident_revisions(revision_id)
        DEFERRABLE INITIALLY DEFERRED,
    FOREIGN KEY (decision_reason_content_id) REFERENCES content_objects(content_id)
        DEFERRABLE INITIALLY DEFERRED
) STRICT;

CREATE TABLE claim_validity_assertions (
    validity_assertion_id TEXT PRIMARY KEY CHECK (length(validity_assertion_id) = 26 AND validity_assertion_id = upper(validity_assertion_id) AND substr(validity_assertion_id, 1, 1) BETWEEN '0' AND '7' AND validity_assertion_id NOT GLOB '*[^0123456789ABCDEFGHJKMNPQRSTVWXYZ]*'),
    canonical_commit_id TEXT NOT NULL CHECK (length(canonical_commit_id) = 26 AND canonical_commit_id = upper(canonical_commit_id) AND substr(canonical_commit_id, 1, 1) BETWEEN '0' AND '7' AND canonical_commit_id NOT GLOB '*[^0123456789ABCDEFGHJKMNPQRSTVWXYZ]*'),
    claim_id TEXT NOT NULL CHECK (length(claim_id) = 26 AND claim_id = upper(claim_id) AND substr(claim_id, 1, 1) BETWEEN '0' AND '7' AND claim_id NOT GLOB '*[^0123456789ABCDEFGHJKMNPQRSTVWXYZ]*'),
    assertion_type TEXT NOT NULL,
    valid_from INTEGER NULL,
    valid_from_tz TEXT NULL,
    valid_to INTEGER NULL,
    valid_to_tz TEXT NULL,
    evidence_event_id TEXT NULL CHECK (evidence_event_id IS NULL OR (length(evidence_event_id) = 26 AND evidence_event_id = upper(evidence_event_id) AND substr(evidence_event_id, 1, 1) BETWEEN '0' AND '7' AND evidence_event_id NOT GLOB '*[^0123456789ABCDEFGHJKMNPQRSTVWXYZ]*')),
    confidence INTEGER NULL CHECK (confidence IS NULL OR (confidence BETWEEN 0 AND 1000000)),
    actor_principal_id TEXT NULL CHECK (actor_principal_id IS NULL OR (length(actor_principal_id) = 26 AND actor_principal_id = upper(actor_principal_id) AND substr(actor_principal_id, 1, 1) BETWEEN '0' AND '7' AND actor_principal_id NOT GLOB '*[^0123456789ABCDEFGHJKMNPQRSTVWXYZ]*')),
    reason_code TEXT NOT NULL,
    reason_content_id TEXT NULL CHECK (reason_content_id IS NULL OR (length(reason_content_id) = 26 AND reason_content_id = upper(reason_content_id) AND substr(reason_content_id, 1, 1) BETWEEN '0' AND '7' AND reason_content_id NOT GLOB '*[^0123456789ABCDEFGHJKMNPQRSTVWXYZ]*')),
    recorded_at INTEGER NOT NULL,
    recorded_tz TEXT NOT NULL CHECK (length(recorded_tz) > 0),
    CHECK ((valid_from IS NULL) = (valid_from_tz IS NULL)),
    CHECK ((valid_to IS NULL) = (valid_to_tz IS NULL)),
    CHECK (valid_from IS NULL OR valid_to IS NULL OR valid_from <= valid_to),
    FOREIGN KEY (canonical_commit_id) REFERENCES canonical_commits(canonical_commit_id)
        DEFERRABLE INITIALLY DEFERRED,
    FOREIGN KEY (claim_id) REFERENCES claims(claim_id)
        DEFERRABLE INITIALLY DEFERRED,
    FOREIGN KEY (evidence_event_id) REFERENCES events(event_id)
        DEFERRABLE INITIALLY DEFERRED,
    FOREIGN KEY (actor_principal_id) REFERENCES principals(principal_id)
        DEFERRABLE INITIALLY DEFERRED,
    FOREIGN KEY (reason_content_id) REFERENCES content_objects(content_id)
        DEFERRABLE INITIALLY DEFERRED
) STRICT;

CREATE TABLE claim_view_scope_assertions (
    view_scope_assertion_id TEXT PRIMARY KEY CHECK (length(view_scope_assertion_id) = 26 AND view_scope_assertion_id = upper(view_scope_assertion_id) AND substr(view_scope_assertion_id, 1, 1) BETWEEN '0' AND '7' AND view_scope_assertion_id NOT GLOB '*[^0123456789ABCDEFGHJKMNPQRSTVWXYZ]*'),
    canonical_commit_id TEXT NOT NULL CHECK (length(canonical_commit_id) = 26 AND canonical_commit_id = upper(canonical_commit_id) AND substr(canonical_commit_id, 1, 1) BETWEEN '0' AND '7' AND canonical_commit_id NOT GLOB '*[^0123456789ABCDEFGHJKMNPQRSTVWXYZ]*'),
    claim_id TEXT NOT NULL CHECK (length(claim_id) = 26 AND claim_id = upper(claim_id) AND substr(claim_id, 1, 1) BETWEEN '0' AND '7' AND claim_id NOT GLOB '*[^0123456789ABCDEFGHJKMNPQRSTVWXYZ]*'),
    view_scope TEXT NOT NULL CHECK (view_scope IN ('resident_ui', 'admin_only')),
    actor_principal_id TEXT NULL CHECK (actor_principal_id IS NULL OR (length(actor_principal_id) = 26 AND actor_principal_id = upper(actor_principal_id) AND substr(actor_principal_id, 1, 1) BETWEEN '0' AND '7' AND actor_principal_id NOT GLOB '*[^0123456789ABCDEFGHJKMNPQRSTVWXYZ]*')),
    generation_run_id TEXT NULL CHECK (generation_run_id IS NULL OR (length(generation_run_id) = 26 AND generation_run_id = upper(generation_run_id) AND substr(generation_run_id, 1, 1) BETWEEN '0' AND '7' AND generation_run_id NOT GLOB '*[^0123456789ABCDEFGHJKMNPQRSTVWXYZ]*')),
    memory_policy_revision_id TEXT NULL CHECK (memory_policy_revision_id IS NULL OR (length(memory_policy_revision_id) = 26 AND memory_policy_revision_id = upper(memory_policy_revision_id) AND substr(memory_policy_revision_id, 1, 1) BETWEEN '0' AND '7' AND memory_policy_revision_id NOT GLOB '*[^0123456789ABCDEFGHJKMNPQRSTVWXYZ]*')),
    reason_code TEXT NOT NULL,
    reason_content_id TEXT NULL CHECK (reason_content_id IS NULL OR (length(reason_content_id) = 26 AND reason_content_id = upper(reason_content_id) AND substr(reason_content_id, 1, 1) BETWEEN '0' AND '7' AND reason_content_id NOT GLOB '*[^0123456789ABCDEFGHJKMNPQRSTVWXYZ]*')),
    recorded_at INTEGER NOT NULL,
    recorded_tz TEXT NOT NULL CHECK (length(recorded_tz) > 0),
    CHECK (actor_principal_id IS NOT NULL OR generation_run_id IS NOT NULL),
    FOREIGN KEY (canonical_commit_id) REFERENCES canonical_commits(canonical_commit_id)
        DEFERRABLE INITIALLY DEFERRED,
    FOREIGN KEY (claim_id) REFERENCES claims(claim_id)
        DEFERRABLE INITIALLY DEFERRED,
    FOREIGN KEY (actor_principal_id) REFERENCES principals(principal_id)
        DEFERRABLE INITIALLY DEFERRED,
    FOREIGN KEY (generation_run_id) REFERENCES generation_runs(generation_run_id)
        DEFERRABLE INITIALLY DEFERRED,
    FOREIGN KEY (memory_policy_revision_id) REFERENCES resident_revisions(revision_id)
        DEFERRABLE INITIALLY DEFERRED,
    FOREIGN KEY (reason_content_id) REFERENCES content_objects(content_id)
        DEFERRABLE INITIALLY DEFERRED
) STRICT;

CREATE TABLE claim_usages (
    claim_usage_id TEXT PRIMARY KEY CHECK (length(claim_usage_id) = 26 AND claim_usage_id = upper(claim_usage_id) AND substr(claim_usage_id, 1, 1) BETWEEN '0' AND '7' AND claim_usage_id NOT GLOB '*[^0123456789ABCDEFGHJKMNPQRSTVWXYZ]*'),
    canonical_commit_id TEXT NOT NULL CHECK (length(canonical_commit_id) = 26 AND canonical_commit_id = upper(canonical_commit_id) AND substr(canonical_commit_id, 1, 1) BETWEEN '0' AND '7' AND canonical_commit_id NOT GLOB '*[^0123456789ABCDEFGHJKMNPQRSTVWXYZ]*'),
    claim_id TEXT NOT NULL CHECK (length(claim_id) = 26 AND claim_id = upper(claim_id) AND substr(claim_id, 1, 1) BETWEEN '0' AND '7' AND claim_id NOT GLOB '*[^0123456789ABCDEFGHJKMNPQRSTVWXYZ]*'),
    recall_run_id TEXT NULL CHECK (recall_run_id IS NULL OR (length(recall_run_id) = 26 AND recall_run_id = upper(recall_run_id) AND substr(recall_run_id, 1, 1) BETWEEN '0' AND '7' AND recall_run_id NOT GLOB '*[^0123456789ABCDEFGHJKMNPQRSTVWXYZ]*')),
    generation_run_id TEXT NULL CHECK (generation_run_id IS NULL OR (length(generation_run_id) = 26 AND generation_run_id = upper(generation_run_id) AND substr(generation_run_id, 1, 1) BETWEEN '0' AND '7' AND generation_run_id NOT GLOB '*[^0123456789ABCDEFGHJKMNPQRSTVWXYZ]*')),
    usage_type TEXT NOT NULL CHECK (usage_type IN ('candidate', 'selected', 'prompt_included', 'explicitly_referenced')),
    ordinal INTEGER NULL CHECK (ordinal IS NULL OR ordinal >= 0),
    memory_policy_revision_id TEXT NOT NULL CHECK (length(memory_policy_revision_id) = 26 AND memory_policy_revision_id = upper(memory_policy_revision_id) AND substr(memory_policy_revision_id, 1, 1) BETWEEN '0' AND '7' AND memory_policy_revision_id NOT GLOB '*[^0123456789ABCDEFGHJKMNPQRSTVWXYZ]*'),
    exclusion_reason TEXT NULL,
    detection_method TEXT NULL,
    detection_confidence INTEGER NULL CHECK (detection_confidence IS NULL OR (detection_confidence BETWEEN 0 AND 1000000)),
    detected_by_run_id TEXT NULL CHECK (detected_by_run_id IS NULL OR (length(detected_by_run_id) = 26 AND detected_by_run_id = upper(detected_by_run_id) AND substr(detected_by_run_id, 1, 1) BETWEEN '0' AND '7' AND detected_by_run_id NOT GLOB '*[^0123456789ABCDEFGHJKMNPQRSTVWXYZ]*')),
    recorded_at INTEGER NOT NULL,
    recorded_tz TEXT NOT NULL CHECK (length(recorded_tz) > 0),
    CHECK (recall_run_id IS NOT NULL OR generation_run_id IS NOT NULL),
    CHECK (usage_type <> 'explicitly_referenced' OR detected_by_run_id IS NOT NULL),
    FOREIGN KEY (canonical_commit_id) REFERENCES canonical_commits(canonical_commit_id)
        DEFERRABLE INITIALLY DEFERRED,
    FOREIGN KEY (claim_id) REFERENCES claims(claim_id)
        DEFERRABLE INITIALLY DEFERRED,
    FOREIGN KEY (recall_run_id) REFERENCES recall_runs(recall_run_id)
        DEFERRABLE INITIALLY DEFERRED,
    FOREIGN KEY (generation_run_id) REFERENCES generation_runs(generation_run_id)
        DEFERRABLE INITIALLY DEFERRED,
    FOREIGN KEY (memory_policy_revision_id) REFERENCES resident_revisions(revision_id)
        DEFERRABLE INITIALLY DEFERRED,
    FOREIGN KEY (detected_by_run_id) REFERENCES generation_runs(generation_run_id)
        DEFERRABLE INITIALLY DEFERRED
) STRICT;

-- +goose Down
DROP TABLE IF EXISTS claim_usages;
DROP TABLE IF EXISTS claim_view_scope_assertions;
DROP TABLE IF EXISTS claim_validity_assertions;
DROP TABLE IF EXISTS claim_status_transitions;
DROP TABLE IF EXISTS integrity_findings;
DROP TABLE IF EXISTS claim_relations;
DROP TABLE IF EXISTS claim_stage_transition_dependencies;
DROP TABLE IF EXISTS claim_stage_transitions;
DROP TABLE IF EXISTS claim_evidence;
DROP TABLE IF EXISTS claims;
