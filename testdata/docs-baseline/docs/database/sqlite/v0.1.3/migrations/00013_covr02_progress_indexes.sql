-- +goose Up
-- mahoroba: migration-policy=forward-only
-- COVR-02 bounds re-extraction discovery by a fixed commit and a run keyset.

CREATE INDEX idx_generation_runs_commit_resident_purpose_run
    ON generation_runs(
        canonical_commit_id,
        resident_id,
        purpose,
        generation_run_id
    );

-- +goose Down
-- +goose StatementBegin
CREATE TEMP TABLE IF NOT EXISTS mahoroba_forward_only_guard (id INTEGER);
CREATE TEMP TRIGGER mahoroba_forward_only_guard_abort
BEFORE INSERT ON mahoroba_forward_only_guard
BEGIN
    SELECT RAISE(ABORT, 'mahoroba: migration 13 is forward-only');
END;
INSERT INTO mahoroba_forward_only_guard DEFAULT VALUES;
-- +goose StatementEnd
DROP INDEX IF EXISTS idx_generation_runs_commit_resident_purpose_run;
