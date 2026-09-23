-- +goose Up
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

-- +goose Down
DROP TABLE IF EXISTS search_outbox;
DROP TABLE IF EXISTS analytics_outbox;
DROP TABLE IF EXISTS runtime_config;
