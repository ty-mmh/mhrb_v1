-- +goose Up
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

-- +goose Down
DROP TABLE IF EXISTS residents;
DROP TABLE IF EXISTS principals;
DROP TABLE IF EXISTS canonical_commits;
