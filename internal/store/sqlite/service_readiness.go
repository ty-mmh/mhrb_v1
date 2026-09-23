package sqlite

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"

	"mahoroba.local/mahoroba/internal/canonical"
	"mahoroba.local/mahoroba/internal/domain"
	"mahoroba.local/mahoroba/internal/integrity"
	"mahoroba.local/mahoroba/internal/memory"
	"mahoroba.local/mahoroba/internal/readiness"
)

// ServiceReadinessSource returns the shared read-only readiness source. Each
// Capture call uses one SQLite read transaction for the rich Canonical head,
// runtime selection, policy resolution, current structural predicates, and
// exact finding coverage.
func (s *Store) ServiceReadinessSource() readiness.Source {
	return &serviceReadinessSource{reader: s.reader}
}

func (inspection *Inspection) ServiceReadinessSource() readiness.Source {
	return &serviceReadinessSource{reader: inspection.store.reader}
}

type serviceReadinessSource struct {
	reader    *sql.DB
	afterHead func()
}

func (source *serviceReadinessSource) CaptureServiceReadiness(
	ctx context.Context,
) (_ readiness.Snapshot, resultErr error) {
	if source == nil || source.reader == nil {
		return readiness.Snapshot{}, errors.New("sqlite: service readiness reader is required")
	}
	tx, err := source.reader.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return readiness.Snapshot{}, fmt.Errorf("sqlite: begin service readiness snapshot: %w", err)
	}
	defer func() {
		if resultErr != nil {
			_ = tx.Rollback()
		}
	}()

	snapshot := readiness.Snapshot{BlockingPredicates: []readiness.PredicateSummary{}}
	snapshot.CapturedHead, err = captureServiceReadinessHead(ctx, tx)
	if err != nil {
		return readiness.Snapshot{}, err
	}
	if source.afterHead != nil {
		source.afterHead()
	}
	snapshot.ActiveResidentID, err = captureReadinessSelection(ctx, tx)
	if err != nil {
		return readiness.Snapshot{}, err
	}
	if snapshot.ActiveResidentID != nil {
		snapshot.ActiveResidentStatus, err = captureReadinessResidentStatus(ctx, tx, *snapshot.ActiveResidentID)
		if err != nil {
			return readiness.Snapshot{}, err
		}
		snapshot.MemoryPolicyServiceCurrent, err = captureReadinessMemoryPolicyServiceCurrent(
			ctx, tx, *snapshot.ActiveResidentID,
		)
		if err != nil {
			return readiness.Snapshot{}, err
		}
		snapshot.SessionPolicyID, err = resolveReadinessSessionPolicy(ctx, tx, *snapshot.ActiveResidentID)
		if err != nil {
			return readiness.Snapshot{}, err
		}
	}
	snapshot.BlockingPredicates, err = captureReadinessPredicates(ctx, tx)
	if err != nil {
		return readiness.Snapshot{}, err
	}
	if err := tx.Commit(); err != nil {
		return readiness.Snapshot{}, fmt.Errorf("sqlite: finish service readiness snapshot: %w", err)
	}
	if err := snapshot.Validate(); err != nil {
		return readiness.Snapshot{}, fmt.Errorf("sqlite: invalid service readiness snapshot: %w", err)
	}
	return snapshot, nil
}

func captureReadinessMemoryPolicyServiceCurrent(
	ctx context.Context,
	tx *sql.Tx,
	residentID canonical.ID,
) (bool, error) {
	var content []byte
	err := tx.QueryRowContext(ctx, `SELECT blob.content
		FROM resident_revision_activations activation
		JOIN canonical_commits activation_commit
		  ON activation_commit.canonical_commit_id = activation.canonical_commit_id
		JOIN resident_revisions revision ON revision.revision_id = activation.revision_id
		JOIN content_objects object ON object.content_id = revision.content_id
		LEFT JOIN blobs blob ON blob.dedupe_scope_id = object.owner_resident_id
		 AND blob.hash_algorithm = object.blob_hash_algorithm
		 AND blob.blob_hash = object.blob_hash
		WHERE activation.resident_id = ? AND revision.resident_id = ?
		  AND revision.revision_class = 'memory_policy'
		ORDER BY activation_commit.commit_seq DESC, activation.activation_id DESC
		LIMIT 1`, residentID.String(), residentID.String()).Scan(&content)
	if errors.Is(err, sql.ErrNoRows) || content == nil {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("sqlite: capture readiness memory policy: %w", err)
	}
	policy, _, err := memory.ParsePolicy(content)
	if err != nil {
		return false, nil
	}
	return policy.Version == memory.PolicyVersionV1 || policy.Version == memory.PolicyVersionV4, nil
}

func captureServiceReadinessHead(ctx context.Context, tx *sql.Tx) (readiness.Head, error) {
	var commitRaw, timezoneRaw string
	var seq, committedAt int64
	err := tx.QueryRowContext(ctx, `SELECT
		canonical_commit_id, commit_seq, committed_at, committed_tz
	FROM canonical_commits
	ORDER BY commit_seq DESC
	LIMIT 1`).Scan(&commitRaw, &seq, &committedAt, &timezoneRaw)
	if errors.Is(err, sql.ErrNoRows) {
		return readiness.Head{}, nil
	}
	if err != nil {
		return readiness.Head{}, fmt.Errorf("sqlite: capture service readiness head: %w", err)
	}
	commitID, err := canonical.ParseID(commitRaw)
	if err != nil {
		return readiness.Head{}, fmt.Errorf("sqlite: parse service readiness head ID: %w", err)
	}
	commitSeq, err := canonical.NewCommitSeq(seq)
	if err != nil {
		return readiness.Head{}, fmt.Errorf("sqlite: parse service readiness head sequence: %w", err)
	}
	timezone, err := canonical.ParseTimezone(timezoneRaw)
	if err != nil {
		return readiness.Head{}, fmt.Errorf("sqlite: parse service readiness head timezone: %w", err)
	}
	return readiness.Head{
		Exists: true, CommitID: commitID, CommitSeq: commitSeq,
		CommittedAt: canonical.Instant(committedAt), CommittedTZ: timezone,
	}, nil
}

func captureReadinessSelection(ctx context.Context, tx *sql.Tx) (*canonical.ID, error) {
	var raw sql.NullString
	err := tx.QueryRowContext(ctx, `SELECT active_resident_id
		FROM runtime_config WHERE singleton_id = 1`).Scan(&raw)
	if errors.Is(err, sql.ErrNoRows) || !raw.Valid {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("sqlite: capture service readiness selection: %w", err)
	}
	id, err := canonical.ParseID(raw.String)
	if err != nil {
		return nil, fmt.Errorf("sqlite: parse service readiness selection: %w", err)
	}
	return &id, nil
}

func captureReadinessResidentStatus(
	ctx context.Context,
	tx *sql.Tx,
	residentID canonical.ID,
) (string, error) {
	var status string
	err := tx.QueryRowContext(ctx, `SELECT transition.to_status
		FROM resident_status_transitions transition
		JOIN canonical_commits commit_row
		  ON commit_row.canonical_commit_id = transition.canonical_commit_id
		WHERE transition.resident_id = ?
		ORDER BY commit_row.commit_seq DESC, transition.resident_status_transition_id DESC
		LIMIT 1`, residentID.String()).Scan(&status)
	if errors.Is(err, sql.ErrNoRows) {
		return "", nil
	}
	if err != nil {
		return "", fmt.Errorf("sqlite: capture service readiness resident status: %w", err)
	}
	return status, nil
}

// resolveReadinessSessionPolicy follows the fixed priority without an implicit
// newest/default: runtime_config first, otherwise exactly one dependency on
// the selected resident's saved activity_sessions watermark. Invalid or
// conflicting semantic candidates resolve to nil and make readiness false.
func resolveReadinessSessionPolicy(
	ctx context.Context,
	tx *sql.Tx,
	residentID canonical.ID,
) (*canonical.ID, error) {
	var configured sql.NullString
	err := tx.QueryRowContext(ctx, `SELECT desired_sessionization_policy_version_id
		FROM runtime_config
		WHERE singleton_id = 1 AND active_resident_id = ?`, residentID.String()).Scan(&configured)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("sqlite: capture readiness session selection: %w", err)
	}

	if configured.Valid {
		return loadValidReadinessSessionPolicy(ctx, tx, configured.String)
	}
	rows, err := tx.QueryContext(ctx, `SELECT DISTINCT dependency.dependency_version_id
		FROM projection_watermark_dependencies dependency
		JOIN projection_watermarks watermark
		  ON watermark.projection_name = dependency.projection_name
		 AND watermark.resident_id = dependency.resident_id
		WHERE dependency.projection_name = 'activity_sessions'
		  AND dependency.resident_id = ?
		  AND dependency.dependency_kind = 'sessionization_policy'
		ORDER BY dependency.dependency_version_id`, residentID.String())
	if err != nil {
		return nil, fmt.Errorf("sqlite: inspect readiness activity_sessions dependency: %w", err)
	}
	defer rows.Close()
	var candidates []string
	for rows.Next() {
		var candidate string
		if err := rows.Scan(&candidate); err != nil {
			return nil, fmt.Errorf("sqlite: scan readiness activity_sessions dependency: %w", err)
		}
		candidates = append(candidates, candidate)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("sqlite: iterate readiness activity_sessions dependency: %w", err)
	}
	valid := make([]canonical.ID, 0, len(candidates))
	for _, candidate := range candidates {
		policyID, err := loadValidReadinessSessionPolicy(ctx, tx, candidate)
		if err != nil {
			return nil, err
		}
		if policyID != nil {
			valid = append(valid, *policyID)
		}
	}
	if len(valid) != 1 {
		return nil, nil
	}
	return &valid[0], nil
}

func loadValidReadinessSessionPolicy(
	ctx context.Context,
	tx *sql.Tx,
	policyRaw string,
) (*canonical.ID, error) {
	policyID, err := canonical.ParseID(policyRaw)
	if err != nil {
		return nil, nil
	}
	var versionKey, definitionRaw string
	err = tx.QueryRowContext(ctx, `SELECT version_key, definition
		FROM sessionization_policy_versions
		WHERE sessionization_policy_version_id = ?`, policyRaw).Scan(&versionKey, &definitionRaw)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("sqlite: load readiness session policy: %w", err)
	}
	if versionKey != domain.SessionPolicyVersion || !validReadinessSessionDefinition(definitionRaw) {
		return nil, nil
	}
	return &policyID, nil
}

func validReadinessSessionDefinition(raw string) bool {
	definition, err := canonical.ParseCanonicalJSON([]byte(raw))
	if err != nil || definition.String() != raw {
		return false
	}
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(definition.Bytes(), &fields); err != nil || len(fields) != 2 {
		return false
	}
	var decoded struct {
		Version string             `json:"version"`
		IdleGap canonical.Duration `json:"idle_gap_microseconds"`
	}
	if err := json.Unmarshal(definition.Bytes(), &decoded); err != nil {
		return false
	}
	return decoded.Version == domain.SessionPolicyVersion && decoded.IdleGap.Microseconds() > 0
}

func captureReadinessPredicates(
	ctx context.Context,
	tx *sql.Tx,
) ([]readiness.PredicateSummary, error) {
	loaders := []func(context.Context, *sql.Tx) ([]integrity.CandidateInput, error){
		loadErasedActiveRevisionCandidates,
		loadErasedRunningInputCandidates,
		loadCancellationEnvelopeCandidates,
	}
	type counts struct {
		current  uint64
		recorded uint64
	}
	byRule := map[readiness.BlockingRule]counts{}
	for _, load := range loaders {
		inputs, err := load(ctx, tx)
		if err != nil {
			return nil, fmt.Errorf("sqlite: evaluate service readiness integrity predicate: %w", err)
		}
		for _, input := range inputs {
			candidate, err := integrity.NewCandidate(input)
			if err != nil {
				return nil, fmt.Errorf("sqlite: validate service readiness integrity predicate: %w", err)
			}
			if !candidate.ReadinessBlocking() {
				return nil, fmt.Errorf("sqlite: non-blocking rule %s entered readiness source", candidate.RuleCode)
			}
			rule, err := readinessRule(candidate.RuleCode)
			if err != nil {
				return nil, err
			}
			value := byRule[rule]
			value.current++
			_, exists, err := (&canonicalUoW{tx: tx}).classifyIntegrityFinding(ctx, candidate)
			if err != nil {
				return nil, fmt.Errorf("sqlite: verify service readiness finding coverage: %w", err)
			}
			if exists {
				value.recorded++
			}
			byRule[rule] = value
		}
	}

	orderedRules := []readiness.BlockingRule{
		readiness.RuleActiveRequiredRevisionErased,
		readiness.RuleRunningAttemptInputErased,
		readiness.RuleCancellationEnvelopeUnresolvable,
	}
	result := make([]readiness.PredicateSummary, 0, len(byRule))
	for _, rule := range orderedRules {
		value := byRule[rule]
		if value.current == 0 {
			continue
		}
		result = append(result, readiness.PredicateSummary{
			Rule: rule, CurrentCount: value.current, RecordedCount: value.recorded,
		})
	}
	return result, nil
}

func readinessRule(rule integrity.RuleCode) (readiness.BlockingRule, error) {
	switch rule {
	case integrity.RuleActiveRequiredRevisionErased:
		return readiness.RuleActiveRequiredRevisionErased, nil
	case integrity.RuleRunningAttemptInputErased:
		return readiness.RuleRunningAttemptInputErased, nil
	case integrity.RuleCancellationEnvelopeUnresolvable:
		return readiness.RuleCancellationEnvelopeUnresolvable, nil
	default:
		return "", fmt.Errorf("sqlite: integrity rule %s has no readiness mapping", rule)
	}
}

var _ readiness.Source = (*serviceReadinessSource)(nil)
