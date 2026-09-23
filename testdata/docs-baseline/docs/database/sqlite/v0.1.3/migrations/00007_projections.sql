-- +goose Up
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

-- +goose Down
DROP TABLE IF EXISTS projection_watermark_dependencies;
DROP TABLE IF EXISTS projection_watermarks;
DROP TABLE IF EXISTS content_references;
DROP TABLE IF EXISTS runtime_states;
DROP TABLE IF EXISTS claim_view_scope_current;
DROP TABLE IF EXISTS claim_states;
DROP TABLE IF EXISTS resident_current_status;
DROP TABLE IF EXISTS resident_current_revision;
