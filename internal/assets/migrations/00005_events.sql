-- +goose Up
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

-- +goose Down
DROP TABLE IF EXISTS events;
