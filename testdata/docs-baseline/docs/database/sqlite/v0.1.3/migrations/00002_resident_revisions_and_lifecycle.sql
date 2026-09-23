-- +goose Up
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

-- +goose Down
DROP TABLE IF EXISTS resident_status_transitions;
DROP TABLE IF EXISTS resident_revision_activations;
DROP TABLE IF EXISTS resident_revision_approvals;
DROP TABLE IF EXISTS resident_revisions;
