-- +goose Up
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

-- +goose Down
DROP TABLE IF EXISTS generation_run_outcomes;
DROP TABLE IF EXISTS generation_run_inputs;
DROP TABLE IF EXISTS generation_runs;
DROP TABLE IF EXISTS recall_runs;
DROP TABLE IF EXISTS sessionization_policy_versions;
DROP TABLE IF EXISTS pipeline_versions;
