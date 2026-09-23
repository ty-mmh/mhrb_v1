-- 00001_canonical_core.sql
CREATE TABLE canonical_commits (
    canonical_commit_id TEXT PRIMARY KEY CHECK (length(canonical_commit_id) = 26 AND canonical_commit_id = upper(canonical_commit_id) AND substr(canonical_commit_id, 1, 1) BETWEEN '0' AND '7' AND canonical_commit_id NOT GLOB '*[^0123456789ABCDEFGHJKMNPQRSTVWXYZ]*'),
    commit_seq INTEGER NOT NULL UNIQUE CHECK (commit_seq > 0),
    resident_id TEXT NULL CHECK (resident_id IS NULL OR (length(resident_id) = 26 AND resident_id = upper(resident_id) AND substr(resident_id, 1, 1) BETWEEN '0' AND '7' AND resident_id NOT GLOB '*[^0123456789ABCDEFGHJKMNPQRSTVWXYZ]*')),
    committed_at INTEGER NOT NULL,
    committed_tz TEXT NOT NULL CHECK (length(committed_tz) > 0),
    FOREIGN KEY (resident_id) REFERENCES residents(resident_id)
        DEFERRABLE INITIALLY DEFERRED
) STRICT;

CREATE TABLE principals (
    principal_id TEXT PRIMARY KEY CHECK (length(principal_id) = 26 AND principal_id = upper(principal_id) AND substr(principal_id, 1, 1) BETWEEN '0' AND '7' AND principal_id NOT GLOB '*[^0123456789ABCDEFGHJKMNPQRSTVWXYZ]*'),
    canonical_commit_id TEXT NOT NULL CHECK (length(canonical_commit_id) = 26 AND canonical_commit_id = upper(canonical_commit_id) AND substr(canonical_commit_id, 1, 1) BETWEEN '0' AND '7' AND canonical_commit_id NOT GLOB '*[^0123456789ABCDEFGHJKMNPQRSTVWXYZ]*'),
    kind TEXT NOT NULL CHECK (kind IN ('human', 'resident', 'system')),
    display_name TEXT NOT NULL CHECK (length(display_name) > 0),
    created_at INTEGER NOT NULL,
    created_tz TEXT NOT NULL CHECK (length(created_tz) > 0),
    FOREIGN KEY (canonical_commit_id) REFERENCES canonical_commits(canonical_commit_id)
        DEFERRABLE INITIALLY DEFERRED
) STRICT;

CREATE TABLE residents (
    resident_id TEXT PRIMARY KEY CHECK (length(resident_id) = 26 AND resident_id = upper(resident_id) AND substr(resident_id, 1, 1) BETWEEN '0' AND '7' AND resident_id NOT GLOB '*[^0123456789ABCDEFGHJKMNPQRSTVWXYZ]*'),
    canonical_commit_id TEXT NOT NULL CHECK (length(canonical_commit_id) = 26 AND canonical_commit_id = upper(canonical_commit_id) AND substr(canonical_commit_id, 1, 1) BETWEEN '0' AND '7' AND canonical_commit_id NOT GLOB '*[^0123456789ABCDEFGHJKMNPQRSTVWXYZ]*'),
    principal_id TEXT NOT NULL UNIQUE CHECK (length(principal_id) = 26 AND principal_id = upper(principal_id) AND substr(principal_id, 1, 1) BETWEEN '0' AND '7' AND principal_id NOT GLOB '*[^0123456789ABCDEFGHJKMNPQRSTVWXYZ]*'),
    name TEXT NOT NULL CHECK (length(name) > 0),
    description_content_id TEXT NULL CHECK (description_content_id IS NULL OR (length(description_content_id) = 26 AND description_content_id = upper(description_content_id) AND substr(description_content_id, 1, 1) BETWEEN '0' AND '7' AND description_content_id NOT GLOB '*[^0123456789ABCDEFGHJKMNPQRSTVWXYZ]*')),
    seed_key TEXT NULL,
    parent_resident_id TEXT NULL CHECK (parent_resident_id IS NULL OR (length(parent_resident_id) = 26 AND parent_resident_id = upper(parent_resident_id) AND substr(parent_resident_id, 1, 1) BETWEEN '0' AND '7' AND parent_resident_id NOT GLOB '*[^0123456789ABCDEFGHJKMNPQRSTVWXYZ]*')),
    branched_from_seq INTEGER NULL CHECK (branched_from_seq IS NULL OR branched_from_seq > 0),
    branched_at INTEGER NULL,
    branched_tz TEXT NULL,
    created_at INTEGER NOT NULL,
    created_tz TEXT NOT NULL CHECK (length(created_tz) > 0),
    CHECK (
        (parent_resident_id IS NULL AND branched_from_seq IS NULL AND branched_at IS NULL AND branched_tz IS NULL)
        OR
        (parent_resident_id IS NOT NULL AND branched_from_seq IS NOT NULL AND branched_at IS NOT NULL AND branched_tz IS NOT NULL)
    ),
    CHECK (parent_resident_id IS NULL OR parent_resident_id <> resident_id),
    FOREIGN KEY (canonical_commit_id) REFERENCES canonical_commits(canonical_commit_id)
        DEFERRABLE INITIALLY DEFERRED,
    FOREIGN KEY (principal_id) REFERENCES principals(principal_id)
        DEFERRABLE INITIALLY DEFERRED,
    FOREIGN KEY (description_content_id) REFERENCES content_objects(content_id)
        DEFERRABLE INITIALLY DEFERRED,
    FOREIGN KEY (parent_resident_id) REFERENCES residents(resident_id)
        DEFERRABLE INITIALLY DEFERRED
) STRICT;

-- 00002_resident_revisions_and_lifecycle.sql
CREATE TABLE resident_revisions (
    revision_id TEXT PRIMARY KEY CHECK (length(revision_id) = 26 AND revision_id = upper(revision_id) AND substr(revision_id, 1, 1) BETWEEN '0' AND '7' AND revision_id NOT GLOB '*[^0123456789ABCDEFGHJKMNPQRSTVWXYZ]*'),
    canonical_commit_id TEXT NOT NULL CHECK (length(canonical_commit_id) = 26 AND canonical_commit_id = upper(canonical_commit_id) AND substr(canonical_commit_id, 1, 1) BETWEEN '0' AND '7' AND canonical_commit_id NOT GLOB '*[^0123456789ABCDEFGHJKMNPQRSTVWXYZ]*'),
    resident_id TEXT NOT NULL CHECK (length(resident_id) = 26 AND resident_id = upper(resident_id) AND substr(resident_id, 1, 1) BETWEEN '0' AND '7' AND resident_id NOT GLOB '*[^0123456789ABCDEFGHJKMNPQRSTVWXYZ]*'),
    revision_class TEXT NOT NULL CHECK (revision_class IN ('principles', 'persona', 'memory_policy')),
    content_id TEXT NOT NULL CHECK (length(content_id) = 26 AND content_id = upper(content_id) AND substr(content_id, 1, 1) BETWEEN '0' AND '7' AND content_id NOT GLOB '*[^0123456789ABCDEFGHJKMNPQRSTVWXYZ]*'),
    parent_revision_id TEXT NULL CHECK (parent_revision_id IS NULL OR (length(parent_revision_id) = 26 AND parent_revision_id = upper(parent_revision_id) AND substr(parent_revision_id, 1, 1) BETWEEN '0' AND '7' AND parent_revision_id NOT GLOB '*[^0123456789ABCDEFGHJKMNPQRSTVWXYZ]*')),
    created_by_run_id TEXT NULL CHECK (created_by_run_id IS NULL OR (length(created_by_run_id) = 26 AND created_by_run_id = upper(created_by_run_id) AND substr(created_by_run_id, 1, 1) BETWEEN '0' AND '7' AND created_by_run_id NOT GLOB '*[^0123456789ABCDEFGHJKMNPQRSTVWXYZ]*')),
    reason_content_id TEXT NULL CHECK (reason_content_id IS NULL OR (length(reason_content_id) = 26 AND reason_content_id = upper(reason_content_id) AND substr(reason_content_id, 1, 1) BETWEEN '0' AND '7' AND reason_content_id NOT GLOB '*[^0123456789ABCDEFGHJKMNPQRSTVWXYZ]*')),
    recorded_at INTEGER NOT NULL,
    recorded_tz TEXT NOT NULL CHECK (length(recorded_tz) > 0),
    CHECK (parent_revision_id IS NULL OR parent_revision_id <> revision_id),
    FOREIGN KEY (canonical_commit_id) REFERENCES canonical_commits(canonical_commit_id)
        DEFERRABLE INITIALLY DEFERRED,
    FOREIGN KEY (resident_id) REFERENCES residents(resident_id)
        DEFERRABLE INITIALLY DEFERRED,
    FOREIGN KEY (content_id) REFERENCES content_objects(content_id)
        DEFERRABLE INITIALLY DEFERRED,
    FOREIGN KEY (parent_revision_id) REFERENCES resident_revisions(revision_id)
        DEFERRABLE INITIALLY DEFERRED,
    FOREIGN KEY (created_by_run_id) REFERENCES generation_runs(generation_run_id)
        DEFERRABLE INITIALLY DEFERRED,
    FOREIGN KEY (reason_content_id) REFERENCES content_objects(content_id)
        DEFERRABLE INITIALLY DEFERRED
) STRICT;

CREATE TABLE resident_revision_approvals (
    approval_id TEXT PRIMARY KEY CHECK (length(approval_id) = 26 AND approval_id = upper(approval_id) AND substr(approval_id, 1, 1) BETWEEN '0' AND '7' AND approval_id NOT GLOB '*[^0123456789ABCDEFGHJKMNPQRSTVWXYZ]*'),
    canonical_commit_id TEXT NOT NULL CHECK (length(canonical_commit_id) = 26 AND canonical_commit_id = upper(canonical_commit_id) AND substr(canonical_commit_id, 1, 1) BETWEEN '0' AND '7' AND canonical_commit_id NOT GLOB '*[^0123456789ABCDEFGHJKMNPQRSTVWXYZ]*'),
    revision_id TEXT NOT NULL CHECK (length(revision_id) = 26 AND revision_id = upper(revision_id) AND substr(revision_id, 1, 1) BETWEEN '0' AND '7' AND revision_id NOT GLOB '*[^0123456789ABCDEFGHJKMNPQRSTVWXYZ]*'),
    approver_principal_id TEXT NOT NULL CHECK (length(approver_principal_id) = 26 AND approver_principal_id = upper(approver_principal_id) AND substr(approver_principal_id, 1, 1) BETWEEN '0' AND '7' AND approver_principal_id NOT GLOB '*[^0123456789ABCDEFGHJKMNPQRSTVWXYZ]*'),
    decision TEXT NOT NULL CHECK (decision IN ('approved', 'rejected')),
    reason_content_id TEXT NULL CHECK (reason_content_id IS NULL OR (length(reason_content_id) = 26 AND reason_content_id = upper(reason_content_id) AND substr(reason_content_id, 1, 1) BETWEEN '0' AND '7' AND reason_content_id NOT GLOB '*[^0123456789ABCDEFGHJKMNPQRSTVWXYZ]*')),
    recorded_at INTEGER NOT NULL,
    recorded_tz TEXT NOT NULL CHECK (length(recorded_tz) > 0),
    FOREIGN KEY (canonical_commit_id) REFERENCES canonical_commits(canonical_commit_id)
        DEFERRABLE INITIALLY DEFERRED,
    FOREIGN KEY (revision_id) REFERENCES resident_revisions(revision_id)
        DEFERRABLE INITIALLY DEFERRED,
    FOREIGN KEY (approver_principal_id) REFERENCES principals(principal_id)
        DEFERRABLE INITIALLY DEFERRED,
    FOREIGN KEY (reason_content_id) REFERENCES content_objects(content_id)
        DEFERRABLE INITIALLY DEFERRED
) STRICT;

CREATE TABLE resident_revision_activations (
    activation_id TEXT PRIMARY KEY CHECK (length(activation_id) = 26 AND activation_id = upper(activation_id) AND substr(activation_id, 1, 1) BETWEEN '0' AND '7' AND activation_id NOT GLOB '*[^0123456789ABCDEFGHJKMNPQRSTVWXYZ]*'),
    canonical_commit_id TEXT NOT NULL CHECK (length(canonical_commit_id) = 26 AND canonical_commit_id = upper(canonical_commit_id) AND substr(canonical_commit_id, 1, 1) BETWEEN '0' AND '7' AND canonical_commit_id NOT GLOB '*[^0123456789ABCDEFGHJKMNPQRSTVWXYZ]*'),
    resident_id TEXT NOT NULL CHECK (length(resident_id) = 26 AND resident_id = upper(resident_id) AND substr(resident_id, 1, 1) BETWEEN '0' AND '7' AND resident_id NOT GLOB '*[^0123456789ABCDEFGHJKMNPQRSTVWXYZ]*'),
    revision_id TEXT NOT NULL CHECK (length(revision_id) = 26 AND revision_id = upper(revision_id) AND substr(revision_id, 1, 1) BETWEEN '0' AND '7' AND revision_id NOT GLOB '*[^0123456789ABCDEFGHJKMNPQRSTVWXYZ]*'),
    actor_principal_id TEXT NOT NULL CHECK (length(actor_principal_id) = 26 AND actor_principal_id = upper(actor_principal_id) AND substr(actor_principal_id, 1, 1) BETWEEN '0' AND '7' AND actor_principal_id NOT GLOB '*[^0123456789ABCDEFGHJKMNPQRSTVWXYZ]*'),
    approval_id TEXT NULL CHECK (approval_id IS NULL OR (length(approval_id) = 26 AND approval_id = upper(approval_id) AND substr(approval_id, 1, 1) BETWEEN '0' AND '7' AND approval_id NOT GLOB '*[^0123456789ABCDEFGHJKMNPQRSTVWXYZ]*')),
    reason_code TEXT NOT NULL,
    reason_content_id TEXT NULL CHECK (reason_content_id IS NULL OR (length(reason_content_id) = 26 AND reason_content_id = upper(reason_content_id) AND substr(reason_content_id, 1, 1) BETWEEN '0' AND '7' AND reason_content_id NOT GLOB '*[^0123456789ABCDEFGHJKMNPQRSTVWXYZ]*')),
    recorded_at INTEGER NOT NULL,
    recorded_tz TEXT NOT NULL CHECK (length(recorded_tz) > 0),
    FOREIGN KEY (canonical_commit_id) REFERENCES canonical_commits(canonical_commit_id)
        DEFERRABLE INITIALLY DEFERRED,
    FOREIGN KEY (resident_id) REFERENCES residents(resident_id)
        DEFERRABLE INITIALLY DEFERRED,
    FOREIGN KEY (revision_id) REFERENCES resident_revisions(revision_id)
        DEFERRABLE INITIALLY DEFERRED,
    FOREIGN KEY (actor_principal_id) REFERENCES principals(principal_id)
        DEFERRABLE INITIALLY DEFERRED,
    FOREIGN KEY (approval_id) REFERENCES resident_revision_approvals(approval_id)
        DEFERRABLE INITIALLY DEFERRED,
    FOREIGN KEY (reason_content_id) REFERENCES content_objects(content_id)
        DEFERRABLE INITIALLY DEFERRED
) STRICT;

CREATE TABLE resident_status_transitions (
    resident_status_transition_id TEXT PRIMARY KEY CHECK (length(resident_status_transition_id) = 26 AND resident_status_transition_id = upper(resident_status_transition_id) AND substr(resident_status_transition_id, 1, 1) BETWEEN '0' AND '7' AND resident_status_transition_id NOT GLOB '*[^0123456789ABCDEFGHJKMNPQRSTVWXYZ]*'),
    canonical_commit_id TEXT NOT NULL CHECK (length(canonical_commit_id) = 26 AND canonical_commit_id = upper(canonical_commit_id) AND substr(canonical_commit_id, 1, 1) BETWEEN '0' AND '7' AND canonical_commit_id NOT GLOB '*[^0123456789ABCDEFGHJKMNPQRSTVWXYZ]*'),
    resident_id TEXT NOT NULL CHECK (length(resident_id) = 26 AND resident_id = upper(resident_id) AND substr(resident_id, 1, 1) BETWEEN '0' AND '7' AND resident_id NOT GLOB '*[^0123456789ABCDEFGHJKMNPQRSTVWXYZ]*'),
    from_status TEXT NULL CHECK (from_status IS NULL OR from_status IN ('draft', 'active', 'archived', 'erased')),
    to_status TEXT NOT NULL CHECK (to_status IN ('draft', 'active', 'archived', 'erased')),
    actor_principal_id TEXT NOT NULL CHECK (length(actor_principal_id) = 26 AND actor_principal_id = upper(actor_principal_id) AND substr(actor_principal_id, 1, 1) BETWEEN '0' AND '7' AND actor_principal_id NOT GLOB '*[^0123456789ABCDEFGHJKMNPQRSTVWXYZ]*'),
    reason_code TEXT NOT NULL,
    reason_content_id TEXT NULL CHECK (reason_content_id IS NULL OR (length(reason_content_id) = 26 AND reason_content_id = upper(reason_content_id) AND substr(reason_content_id, 1, 1) BETWEEN '0' AND '7' AND reason_content_id NOT GLOB '*[^0123456789ABCDEFGHJKMNPQRSTVWXYZ]*')),
    occurred_at INTEGER NOT NULL,
    occurred_tz TEXT NOT NULL CHECK (length(occurred_tz) > 0),
    recorded_at INTEGER NOT NULL,
    recorded_tz TEXT NOT NULL CHECK (length(recorded_tz) > 0),
    CHECK (
        (from_status IS NULL AND to_status = 'draft') OR
        (from_status = 'draft' AND to_status IN ('active', 'erased')) OR
        (from_status = 'active' AND to_status IN ('archived', 'erased')) OR
        (from_status = 'archived' AND to_status = 'erased')
    ),
    FOREIGN KEY (canonical_commit_id) REFERENCES canonical_commits(canonical_commit_id)
        DEFERRABLE INITIALLY DEFERRED,
    FOREIGN KEY (resident_id) REFERENCES residents(resident_id)
        DEFERRABLE INITIALLY DEFERRED,
    FOREIGN KEY (actor_principal_id) REFERENCES principals(principal_id)
        DEFERRABLE INITIALLY DEFERRED,
    FOREIGN KEY (reason_content_id) REFERENCES content_objects(content_id)
        DEFERRABLE INITIALLY DEFERRED
) STRICT;

-- 00003_content.sql
CREATE TABLE blobs (
    dedupe_scope_id TEXT NOT NULL CHECK (length(dedupe_scope_id) = 26 AND dedupe_scope_id = upper(dedupe_scope_id) AND substr(dedupe_scope_id, 1, 1) BETWEEN '0' AND '7' AND dedupe_scope_id NOT GLOB '*[^0123456789ABCDEFGHJKMNPQRSTVWXYZ]*'),
    hash_algorithm TEXT NOT NULL CHECK (hash_algorithm = 'sha256'),
    blob_hash BLOB NOT NULL CHECK (length(blob_hash) = 32),
    content BLOB NOT NULL,
    byte_size INTEGER NOT NULL CHECK (byte_size >= 0),
    encoding TEXT NOT NULL,
    compression TEXT NOT NULL,
    created_at INTEGER NOT NULL,
    created_tz TEXT NOT NULL CHECK (length(created_tz) > 0),
    PRIMARY KEY (dedupe_scope_id, hash_algorithm, blob_hash),
    FOREIGN KEY (dedupe_scope_id) REFERENCES residents(resident_id)
        DEFERRABLE INITIALLY DEFERRED
) STRICT, WITHOUT ROWID;

CREATE TABLE content_objects (
    content_id TEXT PRIMARY KEY CHECK (length(content_id) = 26 AND content_id = upper(content_id) AND substr(content_id, 1, 1) BETWEEN '0' AND '7' AND content_id NOT GLOB '*[^0123456789ABCDEFGHJKMNPQRSTVWXYZ]*'),
    owner_resident_id TEXT NOT NULL CHECK (length(owner_resident_id) = 26 AND owner_resident_id = upper(owner_resident_id) AND substr(owner_resident_id, 1, 1) BETWEEN '0' AND '7' AND owner_resident_id NOT GLOB '*[^0123456789ABCDEFGHJKMNPQRSTVWXYZ]*'),
    content_class TEXT NOT NULL CHECK (content_class IN (
        'event_payload', 'generation_input', 'generation_output', 'claim_statement',
        'reason_text', 'error_detail', 'principles_text', 'persona_text', 'memory_policy_text'
    )),
    blob_hash BLOB NULL CHECK (blob_hash IS NULL OR length(blob_hash) = 32),
    blob_hash_algorithm TEXT NOT NULL CHECK (blob_hash_algorithm = 'sha256'),
    commitment BLOB NOT NULL CHECK (length(commitment) = 32),
    commitment_salt BLOB NULL CHECK (commitment_salt IS NULL OR length(commitment_salt) = 32),
    commitment_hash_algorithm TEXT NOT NULL CHECK (commitment_hash_algorithm = 'sha256'),
    commitment_domain TEXT NOT NULL CHECK (commitment_domain = 'mahoroba:content-commitment:v1'),
    canonicalization_version TEXT NOT NULL CHECK (canonicalization_version = 'mahoroba-jcs-v1'),
    erasure_state TEXT NOT NULL CHECK (erasure_state IN ('present', 'erased')),
    erasure_policy TEXT NOT NULL CHECK (erasure_policy IN ('independent', 'resident_only')),
    created_at INTEGER NOT NULL,
    created_tz TEXT NOT NULL CHECK (length(created_tz) > 0),
    CHECK (
        (erasure_state = 'present' AND blob_hash IS NOT NULL AND commitment_salt IS NOT NULL)
        OR
        (erasure_state = 'erased' AND blob_hash IS NULL AND commitment_salt IS NULL)
    ),
    CHECK (
        (content_class IN ('principles_text', 'persona_text', 'memory_policy_text') AND erasure_policy = 'resident_only')
        OR
        (content_class NOT IN ('principles_text', 'persona_text', 'memory_policy_text'))
    ),
    FOREIGN KEY (owner_resident_id) REFERENCES residents(resident_id)
        DEFERRABLE INITIALLY DEFERRED,
    FOREIGN KEY (owner_resident_id, blob_hash_algorithm, blob_hash)
        REFERENCES blobs(dedupe_scope_id, hash_algorithm, blob_hash)
        DEFERRABLE INITIALLY DEFERRED
) STRICT;

CREATE TABLE content_erasure_events (
    content_erasure_event_id TEXT PRIMARY KEY CHECK (length(content_erasure_event_id) = 26 AND content_erasure_event_id = upper(content_erasure_event_id) AND substr(content_erasure_event_id, 1, 1) BETWEEN '0' AND '7' AND content_erasure_event_id NOT GLOB '*[^0123456789ABCDEFGHJKMNPQRSTVWXYZ]*'),
    canonical_commit_id TEXT NOT NULL CHECK (length(canonical_commit_id) = 26 AND canonical_commit_id = upper(canonical_commit_id) AND substr(canonical_commit_id, 1, 1) BETWEEN '0' AND '7' AND canonical_commit_id NOT GLOB '*[^0123456789ABCDEFGHJKMNPQRSTVWXYZ]*'),
    content_id TEXT NOT NULL CHECK (length(content_id) = 26 AND content_id = upper(content_id) AND substr(content_id, 1, 1) BETWEEN '0' AND '7' AND content_id NOT GLOB '*[^0123456789ABCDEFGHJKMNPQRSTVWXYZ]*'),
    erasure_scope TEXT NOT NULL CHECK (erasure_scope IN ('content', 'resident')),
    actor_principal_id TEXT NOT NULL CHECK (length(actor_principal_id) = 26 AND actor_principal_id = upper(actor_principal_id) AND substr(actor_principal_id, 1, 1) BETWEEN '0' AND '7' AND actor_principal_id NOT GLOB '*[^0123456789ABCDEFGHJKMNPQRSTVWXYZ]*'),
    reason_code TEXT NOT NULL,
    reason_content_id TEXT NULL CHECK (reason_content_id IS NULL OR (length(reason_content_id) = 26 AND reason_content_id = upper(reason_content_id) AND substr(reason_content_id, 1, 1) BETWEEN '0' AND '7' AND reason_content_id NOT GLOB '*[^0123456789ABCDEFGHJKMNPQRSTVWXYZ]*')),
    source_erasure_event_id TEXT NULL CHECK (source_erasure_event_id IS NULL OR (length(source_erasure_event_id) = 26 AND source_erasure_event_id = upper(source_erasure_event_id) AND substr(source_erasure_event_id, 1, 1) BETWEEN '0' AND '7' AND source_erasure_event_id NOT GLOB '*[^0123456789ABCDEFGHJKMNPQRSTVWXYZ]*')),
    occurred_at INTEGER NOT NULL,
    occurred_tz TEXT NOT NULL CHECK (length(occurred_tz) > 0),
    recorded_at INTEGER NOT NULL,
    recorded_tz TEXT NOT NULL CHECK (length(recorded_tz) > 0),
    CHECK (source_erasure_event_id IS NULL OR source_erasure_event_id <> content_erasure_event_id),
    FOREIGN KEY (canonical_commit_id) REFERENCES canonical_commits(canonical_commit_id)
        DEFERRABLE INITIALLY DEFERRED,
    FOREIGN KEY (content_id) REFERENCES content_objects(content_id)
        DEFERRABLE INITIALLY DEFERRED,
    FOREIGN KEY (actor_principal_id) REFERENCES principals(principal_id)
        DEFERRABLE INITIALLY DEFERRED,
    FOREIGN KEY (reason_content_id) REFERENCES content_objects(content_id)
        DEFERRABLE INITIALLY DEFERRED,
    FOREIGN KEY (source_erasure_event_id) REFERENCES content_erasure_events(content_erasure_event_id)
        DEFERRABLE INITIALLY DEFERRED
) STRICT;

-- 00004_versions_generation_recall.sql
CREATE TABLE pipeline_versions (
    pipeline_version_id TEXT PRIMARY KEY CHECK (length(pipeline_version_id) = 26 AND pipeline_version_id = upper(pipeline_version_id) AND substr(pipeline_version_id, 1, 1) BETWEEN '0' AND '7' AND pipeline_version_id NOT GLOB '*[^0123456789ABCDEFGHJKMNPQRSTVWXYZ]*'),
    canonical_commit_id TEXT NOT NULL CHECK (length(canonical_commit_id) = 26 AND canonical_commit_id = upper(canonical_commit_id) AND substr(canonical_commit_id, 1, 1) BETWEEN '0' AND '7' AND canonical_commit_id NOT GLOB '*[^0123456789ABCDEFGHJKMNPQRSTVWXYZ]*'),
    pipeline_kind TEXT NOT NULL,
    version_key TEXT NOT NULL,
    definition TEXT NOT NULL CHECK (json_valid(definition)),
    recorded_at INTEGER NOT NULL,
    recorded_tz TEXT NOT NULL CHECK (length(recorded_tz) > 0),
    UNIQUE (pipeline_kind, version_key),
    FOREIGN KEY (canonical_commit_id) REFERENCES canonical_commits(canonical_commit_id)
        DEFERRABLE INITIALLY DEFERRED
) STRICT;

CREATE TABLE sessionization_policy_versions (
    sessionization_policy_version_id TEXT PRIMARY KEY CHECK (length(sessionization_policy_version_id) = 26 AND sessionization_policy_version_id = upper(sessionization_policy_version_id) AND substr(sessionization_policy_version_id, 1, 1) BETWEEN '0' AND '7' AND sessionization_policy_version_id NOT GLOB '*[^0123456789ABCDEFGHJKMNPQRSTVWXYZ]*'),
    canonical_commit_id TEXT NOT NULL CHECK (length(canonical_commit_id) = 26 AND canonical_commit_id = upper(canonical_commit_id) AND substr(canonical_commit_id, 1, 1) BETWEEN '0' AND '7' AND canonical_commit_id NOT GLOB '*[^0123456789ABCDEFGHJKMNPQRSTVWXYZ]*'),
    version_key TEXT NOT NULL UNIQUE,
    definition TEXT NOT NULL CHECK (json_valid(definition)),
    recorded_at INTEGER NOT NULL,
    recorded_tz TEXT NOT NULL CHECK (length(recorded_tz) > 0),
    FOREIGN KEY (canonical_commit_id) REFERENCES canonical_commits(canonical_commit_id)
        DEFERRABLE INITIALLY DEFERRED
) STRICT;

CREATE TABLE recall_runs (
    recall_run_id TEXT PRIMARY KEY CHECK (length(recall_run_id) = 26 AND recall_run_id = upper(recall_run_id) AND substr(recall_run_id, 1, 1) BETWEEN '0' AND '7' AND recall_run_id NOT GLOB '*[^0123456789ABCDEFGHJKMNPQRSTVWXYZ]*'),
    canonical_commit_id TEXT NOT NULL CHECK (length(canonical_commit_id) = 26 AND canonical_commit_id = upper(canonical_commit_id) AND substr(canonical_commit_id, 1, 1) BETWEEN '0' AND '7' AND canonical_commit_id NOT GLOB '*[^0123456789ABCDEFGHJKMNPQRSTVWXYZ]*'),
    resident_id TEXT NOT NULL CHECK (length(resident_id) = 26 AND resident_id = upper(resident_id) AND substr(resident_id, 1, 1) BETWEEN '0' AND '7' AND resident_id NOT GLOB '*[^0123456789ABCDEFGHJKMNPQRSTVWXYZ]*'),
    query_content_id TEXT NULL CHECK (query_content_id IS NULL OR (length(query_content_id) = 26 AND query_content_id = upper(query_content_id) AND substr(query_content_id, 1, 1) BETWEEN '0' AND '7' AND query_content_id NOT GLOB '*[^0123456789ABCDEFGHJKMNPQRSTVWXYZ]*')),
    query_conditions TEXT NOT NULL CHECK (json_valid(query_conditions)),
    pipeline_version_id TEXT NOT NULL CHECK (length(pipeline_version_id) = 26 AND pipeline_version_id = upper(pipeline_version_id) AND substr(pipeline_version_id, 1, 1) BETWEEN '0' AND '7' AND pipeline_version_id NOT GLOB '*[^0123456789ABCDEFGHJKMNPQRSTVWXYZ]*'),
    memory_policy_revision_id TEXT NOT NULL CHECK (length(memory_policy_revision_id) = 26 AND memory_policy_revision_id = upper(memory_policy_revision_id) AND substr(memory_policy_revision_id, 1, 1) BETWEEN '0' AND '7' AND memory_policy_revision_id NOT GLOB '*[^0123456789ABCDEFGHJKMNPQRSTVWXYZ]*'),
    as_of INTEGER NOT NULL,
    as_of_tz TEXT NOT NULL CHECK (length(as_of_tz) > 0),
    context_constraints TEXT NOT NULL CHECK (json_valid(context_constraints)),
    recorded_at INTEGER NOT NULL,
    recorded_tz TEXT NOT NULL CHECK (length(recorded_tz) > 0),
    FOREIGN KEY (canonical_commit_id) REFERENCES canonical_commits(canonical_commit_id)
        DEFERRABLE INITIALLY DEFERRED,
    FOREIGN KEY (resident_id) REFERENCES residents(resident_id)
        DEFERRABLE INITIALLY DEFERRED,
    FOREIGN KEY (query_content_id) REFERENCES content_objects(content_id)
        DEFERRABLE INITIALLY DEFERRED,
    FOREIGN KEY (pipeline_version_id) REFERENCES pipeline_versions(pipeline_version_id)
        DEFERRABLE INITIALLY DEFERRED,
    FOREIGN KEY (memory_policy_revision_id) REFERENCES resident_revisions(revision_id)
        DEFERRABLE INITIALLY DEFERRED
) STRICT;

CREATE TABLE generation_runs (
    generation_run_id TEXT PRIMARY KEY CHECK (length(generation_run_id) = 26 AND generation_run_id = upper(generation_run_id) AND substr(generation_run_id, 1, 1) BETWEEN '0' AND '7' AND generation_run_id NOT GLOB '*[^0123456789ABCDEFGHJKMNPQRSTVWXYZ]*'),
    canonical_commit_id TEXT NOT NULL CHECK (length(canonical_commit_id) = 26 AND canonical_commit_id = upper(canonical_commit_id) AND substr(canonical_commit_id, 1, 1) BETWEEN '0' AND '7' AND canonical_commit_id NOT GLOB '*[^0123456789ABCDEFGHJKMNPQRSTVWXYZ]*'),
    resident_id TEXT NOT NULL CHECK (length(resident_id) = 26 AND resident_id = upper(resident_id) AND substr(resident_id, 1, 1) BETWEEN '0' AND '7' AND resident_id NOT GLOB '*[^0123456789ABCDEFGHJKMNPQRSTVWXYZ]*'),
    purpose TEXT NOT NULL,
    idempotency_key TEXT NOT NULL,
    provider TEXT NOT NULL,
    model TEXT NOT NULL,
    model_version TEXT NULL,
    prompt_template_version TEXT NOT NULL,
    pipeline_version_id TEXT NOT NULL CHECK (length(pipeline_version_id) = 26 AND pipeline_version_id = upper(pipeline_version_id) AND substr(pipeline_version_id, 1, 1) BETWEEN '0' AND '7' AND pipeline_version_id NOT GLOB '*[^0123456789ABCDEFGHJKMNPQRSTVWXYZ]*'),
    context_policy_version TEXT NOT NULL,
    sessionization_policy_version_id TEXT NULL CHECK (sessionization_policy_version_id IS NULL OR (length(sessionization_policy_version_id) = 26 AND sessionization_policy_version_id = upper(sessionization_policy_version_id) AND substr(sessionization_policy_version_id, 1, 1) BETWEEN '0' AND '7' AND sessionization_policy_version_id NOT GLOB '*[^0123456789ABCDEFGHJKMNPQRSTVWXYZ]*')),
    memory_rendering_version TEXT NOT NULL,
    principles_revision_id TEXT NOT NULL CHECK (length(principles_revision_id) = 26 AND principles_revision_id = upper(principles_revision_id) AND substr(principles_revision_id, 1, 1) BETWEEN '0' AND '7' AND principles_revision_id NOT GLOB '*[^0123456789ABCDEFGHJKMNPQRSTVWXYZ]*'),
    persona_revision_id TEXT NOT NULL CHECK (length(persona_revision_id) = 26 AND persona_revision_id = upper(persona_revision_id) AND substr(persona_revision_id, 1, 1) BETWEEN '0' AND '7' AND persona_revision_id NOT GLOB '*[^0123456789ABCDEFGHJKMNPQRSTVWXYZ]*'),
    memory_policy_revision_id TEXT NOT NULL CHECK (length(memory_policy_revision_id) = 26 AND memory_policy_revision_id = upper(memory_policy_revision_id) AND substr(memory_policy_revision_id, 1, 1) BETWEEN '0' AND '7' AND memory_policy_revision_id NOT GLOB '*[^0123456789ABCDEFGHJKMNPQRSTVWXYZ]*'),
    recall_run_id TEXT NULL CHECK (recall_run_id IS NULL OR (length(recall_run_id) = 26 AND recall_run_id = upper(recall_run_id) AND substr(recall_run_id, 1, 1) BETWEEN '0' AND '7' AND recall_run_id NOT GLOB '*[^0123456789ABCDEFGHJKMNPQRSTVWXYZ]*')),
    temperature INTEGER NULL CHECK (temperature IS NULL OR temperature >= 0),
    top_p INTEGER NULL CHECK (top_p IS NULL OR (top_p BETWEEN 0 AND 1000000)),
    max_tokens INTEGER NULL CHECK (max_tokens IS NULL OR max_tokens >= 0),
    seed INTEGER NULL,
    generator_params TEXT NOT NULL CHECK (json_valid(generator_params)),
    as_of INTEGER NOT NULL,
    as_of_tz TEXT NOT NULL CHECK (length(as_of_tz) > 0),
    budget_exceeded INTEGER NOT NULL CHECK (budget_exceeded IN (0, 1)),
    dropped_input_summary TEXT NOT NULL CHECK (json_valid(dropped_input_summary)),
    requested_at INTEGER NOT NULL,
    requested_tz TEXT NOT NULL CHECK (length(requested_tz) > 0),
    UNIQUE (resident_id, idempotency_key),
    FOREIGN KEY (canonical_commit_id) REFERENCES canonical_commits(canonical_commit_id)
        DEFERRABLE INITIALLY DEFERRED,
    FOREIGN KEY (resident_id) REFERENCES residents(resident_id)
        DEFERRABLE INITIALLY DEFERRED,
    FOREIGN KEY (pipeline_version_id) REFERENCES pipeline_versions(pipeline_version_id)
        DEFERRABLE INITIALLY DEFERRED,
    FOREIGN KEY (sessionization_policy_version_id) REFERENCES sessionization_policy_versions(sessionization_policy_version_id)
        DEFERRABLE INITIALLY DEFERRED,
    FOREIGN KEY (principles_revision_id) REFERENCES resident_revisions(revision_id)
        DEFERRABLE INITIALLY DEFERRED,
    FOREIGN KEY (persona_revision_id) REFERENCES resident_revisions(revision_id)
        DEFERRABLE INITIALLY DEFERRED,
    FOREIGN KEY (memory_policy_revision_id) REFERENCES resident_revisions(revision_id)
        DEFERRABLE INITIALLY DEFERRED,
    FOREIGN KEY (recall_run_id) REFERENCES recall_runs(recall_run_id)
        DEFERRABLE INITIALLY DEFERRED
) STRICT;

CREATE TABLE generation_run_inputs (
    generation_run_input_id TEXT PRIMARY KEY CHECK (length(generation_run_input_id) = 26 AND generation_run_input_id = upper(generation_run_input_id) AND substr(generation_run_input_id, 1, 1) BETWEEN '0' AND '7' AND generation_run_input_id NOT GLOB '*[^0123456789ABCDEFGHJKMNPQRSTVWXYZ]*'),
    canonical_commit_id TEXT NOT NULL CHECK (length(canonical_commit_id) = 26 AND canonical_commit_id = upper(canonical_commit_id) AND substr(canonical_commit_id, 1, 1) BETWEEN '0' AND '7' AND canonical_commit_id NOT GLOB '*[^0123456789ABCDEFGHJKMNPQRSTVWXYZ]*'),
    generation_run_id TEXT NOT NULL CHECK (length(generation_run_id) = 26 AND generation_run_id = upper(generation_run_id) AND substr(generation_run_id, 1, 1) BETWEEN '0' AND '7' AND generation_run_id NOT GLOB '*[^0123456789ABCDEFGHJKMNPQRSTVWXYZ]*'),
    ordinal INTEGER NOT NULL CHECK (ordinal >= 0),
    role TEXT NOT NULL,
    source_type TEXT NOT NULL,
    source_id TEXT NULL CHECK (source_id IS NULL OR (length(source_id) = 26 AND source_id = upper(source_id) AND substr(source_id, 1, 1) BETWEEN '0' AND '7' AND source_id NOT GLOB '*[^0123456789ABCDEFGHJKMNPQRSTVWXYZ]*')),
    inclusion_mode TEXT NOT NULL CHECK (inclusion_mode IN (
        'resident_definition', 'runtime_projection', 'current_input',
        'live_context', 'context_backfill', 'memory_recall'
    )),
    content_id TEXT NOT NULL CHECK (length(content_id) = 26 AND content_id = upper(content_id) AND substr(content_id, 1, 1) BETWEEN '0' AND '7' AND content_id NOT GLOB '*[^0123456789ABCDEFGHJKMNPQRSTVWXYZ]*'),
    recorded_at INTEGER NOT NULL,
    recorded_tz TEXT NOT NULL CHECK (length(recorded_tz) > 0),
    UNIQUE (generation_run_id, ordinal),
    FOREIGN KEY (canonical_commit_id) REFERENCES canonical_commits(canonical_commit_id)
        DEFERRABLE INITIALLY DEFERRED,
    FOREIGN KEY (generation_run_id) REFERENCES generation_runs(generation_run_id)
        DEFERRABLE INITIALLY DEFERRED,
    FOREIGN KEY (content_id) REFERENCES content_objects(content_id)
        DEFERRABLE INITIALLY DEFERRED
) STRICT;

CREATE TABLE generation_run_outcomes (
    outcome_id TEXT PRIMARY KEY CHECK (length(outcome_id) = 26 AND outcome_id = upper(outcome_id) AND substr(outcome_id, 1, 1) BETWEEN '0' AND '7' AND outcome_id NOT GLOB '*[^0123456789ABCDEFGHJKMNPQRSTVWXYZ]*'),
    canonical_commit_id TEXT NOT NULL CHECK (length(canonical_commit_id) = 26 AND canonical_commit_id = upper(canonical_commit_id) AND substr(canonical_commit_id, 1, 1) BETWEEN '0' AND '7' AND canonical_commit_id NOT GLOB '*[^0123456789ABCDEFGHJKMNPQRSTVWXYZ]*'),
    generation_run_id TEXT NOT NULL CHECK (length(generation_run_id) = 26 AND generation_run_id = upper(generation_run_id) AND substr(generation_run_id, 1, 1) BETWEEN '0' AND '7' AND generation_run_id NOT GLOB '*[^0123456789ABCDEFGHJKMNPQRSTVWXYZ]*'),
    attempt_no INTEGER NOT NULL CHECK (attempt_no >= 0),
    state TEXT NOT NULL CHECK (state IN ('running', 'succeeded', 'failed', 'cancelled')),
    output_content_id TEXT NULL CHECK (output_content_id IS NULL OR (length(output_content_id) = 26 AND output_content_id = upper(output_content_id) AND substr(output_content_id, 1, 1) BETWEEN '0' AND '7' AND output_content_id NOT GLOB '*[^0123456789ABCDEFGHJKMNPQRSTVWXYZ]*')),
    prompt_tokens INTEGER NULL CHECK (prompt_tokens IS NULL OR prompt_tokens >= 0),
    completion_tokens INTEGER NULL CHECK (completion_tokens IS NULL OR completion_tokens >= 0),
    latency INTEGER NULL CHECK (latency IS NULL OR latency >= 0),
    estimated_cost INTEGER NULL,
    error_class TEXT NULL,
    error_detail_content_id TEXT NULL CHECK (error_detail_content_id IS NULL OR (length(error_detail_content_id) = 26 AND error_detail_content_id = upper(error_detail_content_id) AND substr(error_detail_content_id, 1, 1) BETWEEN '0' AND '7' AND error_detail_content_id NOT GLOB '*[^0123456789ABCDEFGHJKMNPQRSTVWXYZ]*')),
    recorded_at INTEGER NOT NULL,
    recorded_tz TEXT NOT NULL CHECK (length(recorded_tz) > 0),
    CHECK (
        (state = 'running' AND output_content_id IS NULL AND error_class IS NULL)
        OR
        (state = 'succeeded' AND output_content_id IS NOT NULL AND error_class IS NULL)
        OR
        (state IN ('failed', 'cancelled') AND error_class IS NOT NULL)
    ),
    FOREIGN KEY (canonical_commit_id) REFERENCES canonical_commits(canonical_commit_id)
        DEFERRABLE INITIALLY DEFERRED,
    FOREIGN KEY (generation_run_id) REFERENCES generation_runs(generation_run_id)
        DEFERRABLE INITIALLY DEFERRED,
    FOREIGN KEY (output_content_id) REFERENCES content_objects(content_id)
        DEFERRABLE INITIALLY DEFERRED,
    FOREIGN KEY (error_detail_content_id) REFERENCES content_objects(content_id)
        DEFERRABLE INITIALLY DEFERRED
) STRICT;

-- 00005_events.sql
CREATE TABLE events (
    event_id TEXT PRIMARY KEY CHECK (length(event_id) = 26 AND event_id = upper(event_id) AND substr(event_id, 1, 1) BETWEEN '0' AND '7' AND event_id NOT GLOB '*[^0123456789ABCDEFGHJKMNPQRSTVWXYZ]*'),
    canonical_commit_id TEXT NOT NULL CHECK (length(canonical_commit_id) = 26 AND canonical_commit_id = upper(canonical_commit_id) AND substr(canonical_commit_id, 1, 1) BETWEEN '0' AND '7' AND canonical_commit_id NOT GLOB '*[^0123456789ABCDEFGHJKMNPQRSTVWXYZ]*'),
    resident_id TEXT NOT NULL CHECK (length(resident_id) = 26 AND resident_id = upper(resident_id) AND substr(resident_id, 1, 1) BETWEEN '0' AND '7' AND resident_id NOT GLOB '*[^0123456789ABCDEFGHJKMNPQRSTVWXYZ]*'),
    seq INTEGER NOT NULL CHECK (seq > 0),
    event_type TEXT NOT NULL CHECK (event_type IN ('user_message', 'resident_message', 'self_talk', 'outbound_initiative')),
    visibility TEXT NOT NULL CHECK (visibility IN ('conversation', 'internal')),
    delivery_screen INTEGER NOT NULL CHECK (delivery_screen IN (0, 1)),
    delivery_audio INTEGER NOT NULL CHECK (delivery_audio IN (0, 1)),
    ingress TEXT NOT NULL CHECK (ingress IN ('local_ui', 'resident_runtime')),
    trust_level TEXT NOT NULL CHECK (trust_level IN ('trusted', 'untrusted')),
    actor_principal_id TEXT NOT NULL CHECK (length(actor_principal_id) = 26 AND actor_principal_id = upper(actor_principal_id) AND substr(actor_principal_id, 1, 1) BETWEEN '0' AND '7' AND actor_principal_id NOT GLOB '*[^0123456789ABCDEFGHJKMNPQRSTVWXYZ]*'),
    target_principal_id TEXT NULL CHECK (target_principal_id IS NULL OR (length(target_principal_id) = 26 AND target_principal_id = upper(target_principal_id) AND substr(target_principal_id, 1, 1) BETWEEN '0' AND '7' AND target_principal_id NOT GLOB '*[^0123456789ABCDEFGHJKMNPQRSTVWXYZ]*')),
    generation_run_id TEXT NULL UNIQUE CHECK (generation_run_id IS NULL OR (length(generation_run_id) = 26 AND generation_run_id = upper(generation_run_id) AND substr(generation_run_id, 1, 1) BETWEEN '0' AND '7' AND generation_run_id NOT GLOB '*[^0123456789ABCDEFGHJKMNPQRSTVWXYZ]*')),
    occurred_at INTEGER NOT NULL,
    occurred_tz TEXT NOT NULL CHECK (length(occurred_tz) > 0),
    recorded_at INTEGER NOT NULL,
    recorded_tz TEXT NOT NULL CHECK (length(recorded_tz) > 0),
    content_id TEXT NOT NULL CHECK (length(content_id) = 26 AND content_id = upper(content_id) AND substr(content_id, 1, 1) BETWEEN '0' AND '7' AND content_id NOT GLOB '*[^0123456789ABCDEFGHJKMNPQRSTVWXYZ]*'),
    payload_commitment BLOB NOT NULL CHECK (length(payload_commitment) = 32),
    prev_event_hash BLOB NULL CHECK (prev_event_hash IS NULL OR length(prev_event_hash) = 32),
    event_hash BLOB NOT NULL CHECK (length(event_hash) = 32),
    event_hash_algorithm TEXT NOT NULL CHECK (event_hash_algorithm = 'sha256'),
    event_hash_domain TEXT NOT NULL CHECK (event_hash_domain = 'mahoroba:event-hash:v1'),
    canonicalization_version TEXT NOT NULL CHECK (canonicalization_version = 'mahoroba-jcs-v1'),
    UNIQUE (resident_id, seq),
    CHECK (
        (event_type = 'user_message' AND visibility = 'conversation' AND ingress = 'local_ui' AND generation_run_id IS NULL)
        OR
        (event_type = 'resident_message' AND visibility = 'conversation' AND ingress = 'resident_runtime' AND generation_run_id IS NOT NULL)
        OR
        (event_type = 'self_talk' AND visibility = 'internal' AND delivery_screen = 0 AND delivery_audio = 0 AND ingress = 'resident_runtime' AND generation_run_id IS NOT NULL)
        OR
        (event_type = 'outbound_initiative' AND visibility = 'conversation' AND ingress = 'resident_runtime' AND generation_run_id IS NOT NULL)
    ),
    CHECK (trust_level = 'trusted'),
    FOREIGN KEY (canonical_commit_id) REFERENCES canonical_commits(canonical_commit_id)
        DEFERRABLE INITIALLY DEFERRED,
    FOREIGN KEY (resident_id) REFERENCES residents(resident_id)
        DEFERRABLE INITIALLY DEFERRED,
    FOREIGN KEY (actor_principal_id) REFERENCES principals(principal_id)
        DEFERRABLE INITIALLY DEFERRED,
    FOREIGN KEY (target_principal_id) REFERENCES principals(principal_id)
        DEFERRABLE INITIALLY DEFERRED,
    FOREIGN KEY (generation_run_id) REFERENCES generation_runs(generation_run_id)
        DEFERRABLE INITIALLY DEFERRED,
    FOREIGN KEY (content_id) REFERENCES content_objects(content_id)
        DEFERRABLE INITIALLY DEFERRED
) STRICT;

-- 00006_memory.sql
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

-- 00007_projections.sql
CREATE TABLE resident_current_revision (
    resident_id TEXT NOT NULL CHECK (length(resident_id) = 26 AND resident_id = upper(resident_id) AND substr(resident_id, 1, 1) BETWEEN '0' AND '7' AND resident_id NOT GLOB '*[^0123456789ABCDEFGHJKMNPQRSTVWXYZ]*'),
    revision_class TEXT NOT NULL CHECK (revision_class IN ('principles', 'persona', 'memory_policy')),
    revision_id TEXT NOT NULL CHECK (length(revision_id) = 26 AND revision_id = upper(revision_id) AND substr(revision_id, 1, 1) BETWEEN '0' AND '7' AND revision_id NOT GLOB '*[^0123456789ABCDEFGHJKMNPQRSTVWXYZ]*'),
    activation_id TEXT NOT NULL CHECK (length(activation_id) = 26 AND activation_id = upper(activation_id) AND substr(activation_id, 1, 1) BETWEEN '0' AND '7' AND activation_id NOT GLOB '*[^0123456789ABCDEFGHJKMNPQRSTVWXYZ]*'),
    PRIMARY KEY (resident_id, revision_class)
) STRICT, WITHOUT ROWID;

CREATE TABLE resident_current_status (
    resident_id TEXT PRIMARY KEY CHECK (length(resident_id) = 26 AND resident_id = upper(resident_id) AND substr(resident_id, 1, 1) BETWEEN '0' AND '7' AND resident_id NOT GLOB '*[^0123456789ABCDEFGHJKMNPQRSTVWXYZ]*'),
    status TEXT NOT NULL CHECK (status IN ('draft', 'active', 'archived', 'erased')),
    source_transition_id TEXT NOT NULL CHECK (length(source_transition_id) = 26 AND source_transition_id = upper(source_transition_id) AND substr(source_transition_id, 1, 1) BETWEEN '0' AND '7' AND source_transition_id NOT GLOB '*[^0123456789ABCDEFGHJKMNPQRSTVWXYZ]*')
) STRICT;

CREATE TABLE claim_states (
    claim_id TEXT PRIMARY KEY CHECK (length(claim_id) = 26 AND claim_id = upper(claim_id) AND substr(claim_id, 1, 1) BETWEEN '0' AND '7' AND claim_id NOT GLOB '*[^0123456789ABCDEFGHJKMNPQRSTVWXYZ]*'),
    resident_id TEXT NOT NULL CHECK (length(resident_id) = 26 AND resident_id = upper(resident_id) AND substr(resident_id, 1, 1) BETWEEN '0' AND '7' AND resident_id NOT GLOB '*[^0123456789ABCDEFGHJKMNPQRSTVWXYZ]*'),
    stage TEXT NOT NULL CHECK (stage IN ('floating', 'sediment', 'settled')),
    status TEXT NOT NULL CHECK (status IN ('active', 'invalidated', 'superseded', 'quarantined')),
    salience REAL NOT NULL,
    confidence INTEGER NOT NULL CHECK (confidence BETWEEN 0 AND 1000000),
    currentness INTEGER NOT NULL CHECK (currentness BETWEEN 0 AND 1000000),
    temporal_relation TEXT NOT NULL CHECK (temporal_relation IN ('future', 'current', 'past', 'stale_unknown')),
    last_referenced_at INTEGER NULL,
    evidence_count INTEGER NOT NULL CHECK (evidence_count >= 0)
) STRICT;

CREATE TABLE claim_view_scope_current (
    claim_id TEXT PRIMARY KEY CHECK (length(claim_id) = 26 AND claim_id = upper(claim_id) AND substr(claim_id, 1, 1) BETWEEN '0' AND '7' AND claim_id NOT GLOB '*[^0123456789ABCDEFGHJKMNPQRSTVWXYZ]*'),
    resident_id TEXT NOT NULL CHECK (length(resident_id) = 26 AND resident_id = upper(resident_id) AND substr(resident_id, 1, 1) BETWEEN '0' AND '7' AND resident_id NOT GLOB '*[^0123456789ABCDEFGHJKMNPQRSTVWXYZ]*'),
    view_scope TEXT NOT NULL CHECK (view_scope IN ('resident_ui', 'admin_only')),
    source_assertion_id TEXT NOT NULL CHECK (length(source_assertion_id) = 26 AND source_assertion_id = upper(source_assertion_id) AND substr(source_assertion_id, 1, 1) BETWEEN '0' AND '7' AND source_assertion_id NOT GLOB '*[^0123456789ABCDEFGHJKMNPQRSTVWXYZ]*')
) STRICT;

CREATE TABLE runtime_states (
    resident_id TEXT PRIMARY KEY CHECK (length(resident_id) = 26 AND resident_id = upper(resident_id) AND substr(resident_id, 1, 1) BETWEEN '0' AND '7' AND resident_id NOT GLOB '*[^0123456789ABCDEFGHJKMNPQRSTVWXYZ]*'),
    last_user_event_seq INTEGER NULL CHECK (last_user_event_seq IS NULL OR last_user_event_seq > 0),
    last_resident_event_seq INTEGER NULL CHECK (last_resident_event_seq IS NULL OR last_resident_event_seq > 0),
    unresolved_reference_markers TEXT NOT NULL CHECK (json_valid(unresolved_reference_markers)),
    idle_duration INTEGER NOT NULL CHECK (idle_duration >= 0)
) STRICT;

CREATE TABLE content_references (
    content_id TEXT NOT NULL CHECK (length(content_id) = 26 AND content_id = upper(content_id) AND substr(content_id, 1, 1) BETWEEN '0' AND '7' AND content_id NOT GLOB '*[^0123456789ABCDEFGHJKMNPQRSTVWXYZ]*'),
    resident_id TEXT NOT NULL CHECK (length(resident_id) = 26 AND resident_id = upper(resident_id) AND substr(resident_id, 1, 1) BETWEEN '0' AND '7' AND resident_id NOT GLOB '*[^0123456789ABCDEFGHJKMNPQRSTVWXYZ]*'),
    referrer_kind TEXT NOT NULL,
    referrer_id TEXT NOT NULL CHECK (length(referrer_id) = 26 AND referrer_id = upper(referrer_id) AND substr(referrer_id, 1, 1) BETWEEN '0' AND '7' AND referrer_id NOT GLOB '*[^0123456789ABCDEFGHJKMNPQRSTVWXYZ]*'),
    referrer_field TEXT NOT NULL,
    dedupe_scope_id TEXT NULL CHECK (dedupe_scope_id IS NULL OR (length(dedupe_scope_id) = 26 AND dedupe_scope_id = upper(dedupe_scope_id) AND substr(dedupe_scope_id, 1, 1) BETWEEN '0' AND '7' AND dedupe_scope_id NOT GLOB '*[^0123456789ABCDEFGHJKMNPQRSTVWXYZ]*')),
    blob_hash_algorithm TEXT NULL,
    blob_hash BLOB NULL CHECK (blob_hash IS NULL OR length(blob_hash) = 32),
    PRIMARY KEY (content_id, referrer_kind, referrer_id, referrer_field)
) STRICT, WITHOUT ROWID;

CREATE TABLE projection_watermarks (
    projection_name TEXT NOT NULL,
    resident_id TEXT NOT NULL CHECK (length(resident_id) = 26 AND resident_id = upper(resident_id) AND substr(resident_id, 1, 1) BETWEEN '0' AND '7' AND resident_id NOT GLOB '*[^0123456789ABCDEFGHJKMNPQRSTVWXYZ]*'),
    projection_version TEXT NOT NULL,
    source_commit_seq INTEGER NOT NULL CHECK (source_commit_seq > 0),
    as_of INTEGER NOT NULL,
    as_of_tz TEXT NOT NULL CHECK (length(as_of_tz) > 0),
    PRIMARY KEY (projection_name, resident_id)
) STRICT, WITHOUT ROWID;

CREATE TABLE projection_watermark_dependencies (
    projection_name TEXT NOT NULL,
    resident_id TEXT NOT NULL CHECK (length(resident_id) = 26 AND resident_id = upper(resident_id) AND substr(resident_id, 1, 1) BETWEEN '0' AND '7' AND resident_id NOT GLOB '*[^0123456789ABCDEFGHJKMNPQRSTVWXYZ]*'),
    dependency_kind TEXT NOT NULL CHECK (dependency_kind IN ('sessionization_policy', 'memory_policy')),
    dependency_version_id TEXT NOT NULL CHECK (length(dependency_version_id) = 26 AND dependency_version_id = upper(dependency_version_id) AND substr(dependency_version_id, 1, 1) BETWEEN '0' AND '7' AND dependency_version_id NOT GLOB '*[^0123456789ABCDEFGHJKMNPQRSTVWXYZ]*'),
    PRIMARY KEY (projection_name, resident_id, dependency_kind, dependency_version_id),
    FOREIGN KEY (projection_name, resident_id)
        REFERENCES projection_watermarks(projection_name, resident_id)
        ON DELETE CASCADE
) STRICT, WITHOUT ROWID;

-- 00008_operational.sql
CREATE TABLE runtime_config (
    singleton_id INTEGER PRIMARY KEY CHECK (singleton_id = 1),
    active_resident_id TEXT NULL CHECK (active_resident_id IS NULL OR (length(active_resident_id) = 26 AND active_resident_id = upper(active_resident_id) AND substr(active_resident_id, 1, 1) BETWEEN '0' AND '7' AND active_resident_id NOT GLOB '*[^0123456789ABCDEFGHJKMNPQRSTVWXYZ]*')),
    desired_sessionization_policy_version_id TEXT NULL CHECK (desired_sessionization_policy_version_id IS NULL OR (length(desired_sessionization_policy_version_id) = 26 AND desired_sessionization_policy_version_id = upper(desired_sessionization_policy_version_id) AND substr(desired_sessionization_policy_version_id, 1, 1) BETWEEN '0' AND '7' AND desired_sessionization_policy_version_id NOT GLOB '*[^0123456789ABCDEFGHJKMNPQRSTVWXYZ]*')),
    updated_at INTEGER NOT NULL,
    updated_tz TEXT NOT NULL CHECK (length(updated_tz) > 0)
) STRICT;

CREATE TABLE analytics_outbox (
    outbox_id TEXT PRIMARY KEY CHECK (length(outbox_id) = 26 AND outbox_id = upper(outbox_id) AND substr(outbox_id, 1, 1) BETWEEN '0' AND '7' AND outbox_id NOT GLOB '*[^0123456789ABCDEFGHJKMNPQRSTVWXYZ]*'),
    resident_id TEXT NULL CHECK (resident_id IS NULL OR (length(resident_id) = 26 AND resident_id = upper(resident_id) AND substr(resident_id, 1, 1) BETWEEN '0' AND '7' AND resident_id NOT GLOB '*[^0123456789ABCDEFGHJKMNPQRSTVWXYZ]*')),
    source_commit_seq INTEGER NOT NULL CHECK (source_commit_seq > 0),
    payload TEXT NOT NULL CHECK (json_valid(payload)),
    status TEXT NOT NULL CHECK (status IN ('pending', 'delivered', 'failed')),
    attempt_count INTEGER NOT NULL CHECK (attempt_count >= 0),
    last_error TEXT NULL,
    available_at INTEGER NOT NULL,
    dispatched_at INTEGER NULL
) STRICT;

CREATE TABLE search_outbox (
    outbox_id TEXT PRIMARY KEY CHECK (length(outbox_id) = 26 AND outbox_id = upper(outbox_id) AND substr(outbox_id, 1, 1) BETWEEN '0' AND '7' AND outbox_id NOT GLOB '*[^0123456789ABCDEFGHJKMNPQRSTVWXYZ]*'),
    resident_id TEXT NULL CHECK (resident_id IS NULL OR (length(resident_id) = 26 AND resident_id = upper(resident_id) AND substr(resident_id, 1, 1) BETWEEN '0' AND '7' AND resident_id NOT GLOB '*[^0123456789ABCDEFGHJKMNPQRSTVWXYZ]*')),
    source_commit_seq INTEGER NOT NULL CHECK (source_commit_seq > 0),
    payload TEXT NOT NULL CHECK (json_valid(payload)),
    status TEXT NOT NULL CHECK (status IN ('pending', 'delivered', 'failed')),
    attempt_count INTEGER NOT NULL CHECK (attempt_count >= 0),
    last_error TEXT NULL,
    available_at INTEGER NOT NULL,
    dispatched_at INTEGER NULL
) STRICT;

-- 00009_indexes.sql
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

-- 00010_immutability_triggers.sql
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

-- 00011_m7_slice0.sql
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
