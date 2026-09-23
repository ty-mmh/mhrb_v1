-- +goose Up
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

-- +goose Down
DROP TABLE IF EXISTS content_erasure_events;
DROP TABLE IF EXISTS content_objects;
DROP TABLE IF EXISTS blobs;
