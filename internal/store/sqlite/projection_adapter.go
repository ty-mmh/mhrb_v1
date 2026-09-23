package sqlite

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"math"
	"slices"
	"time"

	"mahoroba.local/mahoroba/internal/canonical"
	"mahoroba.local/mahoroba/internal/projection"
	"mahoroba.local/mahoroba/internal/surfaceref"
)

const defaultProjectionTransactionTimeout = 2 * time.Second

var activeProjectionDefinitions = []projection.Definition{
	{Name: projection.ResidentCurrentStatusName, Version: "resident-current-status-v1"},
	{Name: projection.ResidentCurrentRevisionName, Version: "resident-current-revision-v1"},
	{Name: projection.RuntimeStatesName, Version: "runtime-states-v2", TimeSensitive: true},
	projection.ClaimStatesDefinition(),
	projection.ClaimViewScopeCurrentDefinition(),
	projection.ContentReferencesDefinition(),
}

// ActiveProjectionRegistry is the fixed production registry.
func ActiveProjectionRegistry() (*projection.Registry, error) {
	return projection.NewRegistry(activeProjectionDefinitions...)
}

type ProjectionOptions struct {
	TransactionTimeout  time.Duration
	ClaimStateEvaluator projection.ClaimStateEvaluator
}

// ProjectionRepository is both the bounded Canonical source and the logically
// separate Projection store. It never exposes a raw SQL transaction.
type ProjectionRepository struct {
	store               *Store
	transactionTimeout  time.Duration
	registry            *projection.Registry
	claimStateEvaluator projection.ClaimStateEvaluator
	applyHook           func(context.Context, string) error // tests only
}

func (s *Store) Projection() *ProjectionRepository {
	return s.ProjectionWithOptions(ProjectionOptions{})
}

// Projection returns the query-only Projection surface for status tooling.
// Apply and Drop are intentionally unreachable through Inspection's public
// type even though both views share the same read implementation internally.
func (inspection *Inspection) Projection() *ProjectionInspection {
	return &ProjectionInspection{repository: &ProjectionRepository{
		store: inspection.store, transactionTimeout: defaultProjectionTransactionTimeout,
		claimStateEvaluator: newSQLiteClaimStateEvaluator(inspection.store),
	}}
}

type ProjectionInspection struct {
	repository *ProjectionRepository
}

func (inspection *ProjectionInspection) Head(ctx context.Context) (canonical.Head, error) {
	return inspection.repository.Head(ctx)
}

func (inspection *ProjectionInspection) Residents(ctx context.Context, target projection.Target) ([]canonical.ID, error) {
	return inspection.repository.Residents(ctx, target)
}

func (inspection *ProjectionInspection) ResolveDependencies(ctx context.Context, request projection.DependencyRequest) ([]projection.Dependency, error) {
	return inspection.repository.ResolveDependencies(ctx, request)
}

func (inspection *ProjectionInspection) DependencyActivated(ctx context.Context, request projection.ActivationRequest) (bool, error) {
	return inspection.repository.DependencyActivated(ctx, request)
}

func (inspection *ProjectionInspection) Evaluate(ctx context.Context, request projection.EvaluationRequest) (projection.Evaluation, error) {
	return inspection.repository.Evaluate(ctx, request)
}

func (inspection *ProjectionInspection) Watermark(ctx context.Context, name projection.Name, residentID canonical.ID) (projection.Watermark, bool, error) {
	return inspection.repository.Watermark(ctx, name, residentID)
}

func (s *Store) ProjectionWithOptions(options ProjectionOptions) *ProjectionRepository {
	timeout := options.TransactionTimeout
	if timeout <= 0 {
		timeout = defaultProjectionTransactionTimeout
	}
	evaluator := options.ClaimStateEvaluator
	if evaluator == nil {
		evaluator = newSQLiteClaimStateEvaluator(s)
	}
	return &ProjectionRepository{store: s, transactionTimeout: timeout, claimStateEvaluator: evaluator}
}

type ResidentStatusProjection struct {
	Status             string
	SourceTransitionID canonical.ID
}

type ResidentRevisionProjection struct {
	Class        string
	RevisionID   canonical.ID
	ActivationID canonical.ID
}

type RuntimeStateProjection struct {
	LastUserEventSeq           *canonical.Seq
	LastResidentEventSeq       *canonical.Seq
	UnresolvedReferenceMarkers []surfaceref.Marker
	IdleDuration               canonical.Duration
}

const runtimeLatestUserEventQuery = `SELECT e.seq, e.recorded_at, object.erasure_state, blob.content
	FROM events e INDEXED BY idx_events_resident_type_seq
	JOIN canonical_commits commit_row ON commit_row.canonical_commit_id = e.canonical_commit_id
	JOIN content_objects object ON object.content_id = e.content_id
	LEFT JOIN blobs blob ON blob.dedupe_scope_id = object.owner_resident_id
	 AND blob.hash_algorithm = object.blob_hash_algorithm AND blob.blob_hash = object.blob_hash
	WHERE e.resident_id = ? AND e.event_type = 'user_message'
	  AND commit_row.commit_seq <= ?
	ORDER BY e.seq DESC LIMIT 1`

const runtimeLatestResidentEventQuery = `SELECT max(
	COALESCE((SELECT e.seq FROM events e INDEXED BY idx_events_resident_type_seq
		JOIN canonical_commits commit_row ON commit_row.canonical_commit_id = e.canonical_commit_id
		WHERE e.resident_id = ? AND e.event_type = 'resident_message'
		  AND commit_row.commit_seq <= ? ORDER BY e.seq DESC LIMIT 1), 0),
	COALESCE((SELECT e.seq FROM events e INDEXED BY idx_events_resident_type_seq
		JOIN canonical_commits commit_row ON commit_row.canonical_commit_id = e.canonical_commit_id
		WHERE e.resident_id = ? AND e.event_type = 'self_talk'
		  AND commit_row.commit_seq <= ? ORDER BY e.seq DESC LIMIT 1), 0),
	COALESCE((SELECT e.seq FROM events e INDEXED BY idx_events_resident_type_seq
		JOIN canonical_commits commit_row ON commit_row.canonical_commit_id = e.canonical_commit_id
		WHERE e.resident_id = ? AND e.event_type = 'outbound_initiative'
		  AND commit_row.commit_seq <= ? ORDER BY e.seq DESC LIMIT 1), 0)
)`

func (repository *ProjectionRepository) Head(ctx context.Context) (canonical.Head, error) {
	return repository.store.Canonical().LoadHead(ctx)
}

// Residents is bounded by the already-captured Canonical target.
func (repository *ProjectionRepository) Residents(ctx context.Context, target projection.Target) ([]canonical.ID, error) {
	if err := target.Validate(); err != nil {
		return nil, err
	}
	if !target.Head.Exists {
		return nil, nil
	}
	rows, err := repository.store.reader.QueryContext(ctx, `SELECT r.resident_id
		FROM residents r
		JOIN canonical_commits c ON c.canonical_commit_id = r.canonical_commit_id
		WHERE c.commit_seq <= ?
		ORDER BY c.commit_seq, r.resident_id`, target.Head.CommitSeq.Int64())
	if err != nil {
		return nil, fmt.Errorf("sqlite: list Projection residents: %w", err)
	}
	defer rows.Close()
	var residents []canonical.ID
	for rows.Next() {
		var raw string
		if err := rows.Scan(&raw); err != nil {
			return nil, err
		}
		residentID, err := canonical.ParseID(raw)
		if err != nil {
			return nil, err
		}
		residents = append(residents, residentID)
	}
	return residents, rows.Err()
}

func (repository *ProjectionRepository) ResolveDependencies(ctx context.Context, request projection.DependencyRequest) ([]projection.Dependency, error) {
	if request.Definition.Name == projection.ClaimStatesName {
		if err := repository.validateStoredClaimStatesDependency(ctx, request); err != nil {
			return nil, err
		}
	}
	dependencies := make([]projection.Dependency, 0, len(request.Definition.Dependencies))
	for _, kind := range request.Definition.Dependencies {
		switch kind {
		case projection.SessionizationPolicyDependency:
			versionID, err := repository.resolveSessionizationDependency(ctx, request)
			if err != nil {
				return nil, err
			}
			dependencies = append(dependencies, projection.Dependency{Kind: kind, VersionID: versionID})
		case projection.MemoryPolicyDependency:
			versionID, err := repository.resolveMemoryPolicyDependency(ctx, request.ResidentID,
				request.Target.Head.CommitSeq, request.Target.AsOf)
			if err != nil {
				return nil, err
			}
			dependencies = append(dependencies, projection.Dependency{Kind: kind, VersionID: versionID})
		default:
			return nil, fmt.Errorf("%w: %q", projection.ErrUnknownDependency, kind)
		}
	}
	return dependencies, nil
}

func (repository *ProjectionRepository) validateStoredClaimStatesDependency(ctx context.Context, request projection.DependencyRequest) error {
	stored := request.Previous
	if stored == nil {
		return nil
	}
	if err := stored.Validate(); err != nil {
		return fmt.Errorf("%w: %v", projection.ErrInvalidWatermarkMetadata, err)
	}
	if stored.ProjectionName != projection.ClaimStatesName || stored.ResidentID != request.ResidentID {
		return fmt.Errorf("%w: claim_states watermark identity differs from request", projection.ErrInvalidWatermarkMetadata)
	}
	if len(stored.Dependencies) == 0 {
		return nil
	}
	if len(stored.Dependencies) != 1 || stored.Dependencies[0].Kind != projection.MemoryPolicyDependency {
		return fmt.Errorf("%w: claim_states stored dependency count/kind is invalid", projection.ErrInvalidWatermarkMetadata)
	}
	resolved, err := repository.resolveMemoryPolicyDependency(ctx, request.ResidentID, stored.SourceCommitSeq, stored.AsOf)
	if err != nil {
		return fmt.Errorf("%w: stored claim_states dependency is not resolvable: %v", projection.ErrInvalidWatermarkMetadata, err)
	}
	if resolved != stored.Dependencies[0].VersionID {
		return fmt.Errorf("%w: stored claim_states dependency does not match its Head/as_of closure", projection.ErrInvalidWatermarkMetadata)
	}
	return nil
}

func (repository *ProjectionRepository) resolveMemoryPolicyDependency(ctx context.Context, residentID canonical.ID, through canonical.CommitSeq, asOf canonical.Instant) (canonical.ID, error) {
	var raw string
	err := repository.store.reader.QueryRowContext(ctx, `SELECT a.revision_id
		FROM resident_revision_activations a
		JOIN resident_revisions r ON r.revision_id = a.revision_id AND r.resident_id = a.resident_id
		JOIN canonical_commits c ON c.canonical_commit_id = a.canonical_commit_id
		WHERE a.resident_id = ? AND r.revision_class = 'memory_policy'
		  AND c.commit_seq <= ? AND a.recorded_at <= ?
		ORDER BY c.commit_seq DESC, a.activation_id DESC LIMIT 1`, residentID.String(), through.Int64(), asOf.UnixMicro()).Scan(&raw)
	if errors.Is(err, sql.ErrNoRows) {
		return canonical.ID{}, fmt.Errorf("%w: resident %s has no active memory policy through commit %s and as_of %s", projection.ErrUnresolvedDependency, residentID, through, asOf)
	}
	if err != nil {
		return canonical.ID{}, fmt.Errorf("sqlite: resolve memory policy dependency: %w", err)
	}
	versionID, err := canonical.ParseID(raw)
	if err != nil {
		return canonical.ID{}, err
	}
	if _, err := loadSQLiteMemoryPolicy(ctx, repository.store, versionID); err != nil {
		return canonical.ID{}, err
	}
	return versionID, nil
}

func (repository *ProjectionRepository) resolveSessionizationDependency(ctx context.Context, request projection.DependencyRequest) (canonical.ID, error) {
	var raw string
	err := repository.store.reader.QueryRowContext(ctx, `SELECT desired_sessionization_policy_version_id
		FROM runtime_config
		WHERE singleton_id = 1 AND active_resident_id = ?
		  AND desired_sessionization_policy_version_id IS NOT NULL`, request.ResidentID.String()).Scan(&raw)
	if errors.Is(err, sql.ErrNoRows) && request.Previous != nil {
		for _, dependency := range request.Previous.Dependencies {
			if dependency.Kind == projection.SessionizationPolicyDependency {
				if raw != "" && raw != dependency.VersionID.String() {
					return canonical.ID{}, fmt.Errorf("%w: conflicting saved sessionization dependencies", projection.ErrUnresolvedDependency)
				}
				raw = dependency.VersionID.String()
			}
		}
		if raw != "" {
			err = nil
		}
	}
	if errors.Is(err, sql.ErrNoRows) || raw == "" {
		return canonical.ID{}, fmt.Errorf("%w: resident %s has neither runtime_config nor a saved sessionization dependency", projection.ErrUnresolvedDependency, request.ResidentID)
	}
	if err != nil {
		return canonical.ID{}, fmt.Errorf("sqlite: resolve desired sessionization policy dependency: %w", err)
	}
	var createdAtCommit int64
	if err := repository.store.reader.QueryRowContext(ctx, `SELECT c.commit_seq
		FROM sessionization_policy_versions p
		JOIN canonical_commits c ON c.canonical_commit_id = p.canonical_commit_id
		WHERE p.sessionization_policy_version_id = ?`, raw).Scan(&createdAtCommit); err != nil {
		return canonical.ID{}, fmt.Errorf("%w: sessionization policy %s is unavailable: %v", projection.ErrUnresolvedDependency, raw, err)
	}
	if createdAtCommit > request.Target.Head.CommitSeq.Int64() {
		return canonical.ID{}, fmt.Errorf("%w: sessionization policy %s was created after captured head", projection.ErrUnresolvedDependency, raw)
	}
	return canonical.ParseID(raw)
}

func (repository *ProjectionRepository) DependencyActivated(ctx context.Context, request projection.ActivationRequest) (bool, error) {
	switch request.Kind {
	case projection.MemoryPolicyDependency:
		var count int
		err := repository.store.reader.QueryRowContext(ctx, `SELECT count(*)
			FROM resident_revision_activations a
			JOIN resident_revisions r ON r.revision_id = a.revision_id
			JOIN canonical_commits c ON c.canonical_commit_id = a.canonical_commit_id
			WHERE a.resident_id = ? AND r.revision_class = 'memory_policy'
			  AND c.commit_seq > ? AND c.commit_seq <= ?`, request.ResidentID.String(), request.After.Int64(), request.Through.Int64()).Scan(&count)
		return count > 0, err
	case projection.SessionizationPolicyDependency:
		return false, nil
	default:
		return false, fmt.Errorf("%w: %q", projection.ErrUnknownDependency, request.Kind)
	}
}

func (repository *ProjectionRepository) Evaluate(ctx context.Context, request projection.EvaluationRequest) (projection.Evaluation, error) {
	switch request.Definition.Name {
	case projection.ResidentCurrentStatusName:
		value, err := repository.evaluateStatus(ctx, request.ResidentID, request.Target.Head.CommitSeq)
		return projection.Evaluation{Value: value}, err
	case projection.ResidentCurrentRevisionName:
		value, err := repository.evaluateRevisions(ctx, request.ResidentID, request.Target.Head.CommitSeq)
		return projection.Evaluation{Value: value}, err
	case projection.RuntimeStatesName:
		value, err := repository.evaluateRuntimeState(ctx, request.ResidentID, request.Target)
		return projection.Evaluation{Value: value}, err
	case projection.ClaimStatesName:
		value, err := repository.evaluateClaimStates(ctx, request)
		return projection.Evaluation{Value: value}, err
	case projection.ClaimViewScopeCurrentName:
		value, err := repository.evaluateClaimViewScopes(ctx, request.ResidentID, request.Target.Head.CommitSeq)
		return projection.Evaluation{Value: value}, err
	case projection.ContentReferencesName:
		value, err := repository.evaluateContentReferences(ctx, request.ResidentID, request.Target.Head)
		return projection.Evaluation{Value: value}, err
	default:
		return projection.Evaluation{}, fmt.Errorf("%w: %q", projection.ErrUndeclaredProjection, request.Definition.Name)
	}
}

func (repository *ProjectionRepository) evaluateStatus(ctx context.Context, residentID canonical.ID, through canonical.CommitSeq) (ResidentStatusProjection, error) {
	rows, err := repository.store.reader.QueryContext(ctx, `SELECT t.resident_status_transition_id, t.from_status, t.to_status
		FROM resident_status_transitions t
		JOIN canonical_commits c ON c.canonical_commit_id = t.canonical_commit_id
		WHERE t.resident_id = ? AND c.commit_seq <= ?
		ORDER BY c.commit_seq, t.resident_status_transition_id`, residentID.String(), through.Int64())
	if err != nil {
		return ResidentStatusProjection{}, err
	}
	defer rows.Close()
	var result ResidentStatusProjection
	seen := false
	for rows.Next() {
		var transitionRaw, toStatus string
		var fromStatus sql.NullString
		if err := rows.Scan(&transitionRaw, &fromStatus, &toStatus); err != nil {
			return ResidentStatusProjection{}, err
		}
		if (!seen && fromStatus.Valid) || (seen && (!fromStatus.Valid || fromStatus.String != result.Status)) {
			return ResidentStatusProjection{}, fmt.Errorf("sqlite: invalid resident status transition chain through commit %s", through)
		}
		transitionID, err := canonical.ParseID(transitionRaw)
		if err != nil {
			return ResidentStatusProjection{}, err
		}
		result = ResidentStatusProjection{Status: toStatus, SourceTransitionID: transitionID}
		seen = true
	}
	if err := rows.Err(); err != nil {
		return ResidentStatusProjection{}, err
	}
	if !seen {
		return ResidentStatusProjection{}, fmt.Errorf("sqlite: resident %s has no status through commit %s", residentID, through)
	}
	return result, nil
}

func (repository *ProjectionRepository) evaluateRevisions(ctx context.Context, residentID canonical.ID, through canonical.CommitSeq) ([]ResidentRevisionProjection, error) {
	rows, err := repository.store.reader.QueryContext(ctx, `SELECT r.revision_class, a.revision_id, a.activation_id
		FROM resident_revision_activations a
		JOIN resident_revisions r ON r.revision_id = a.revision_id
		JOIN canonical_commits c ON c.canonical_commit_id = a.canonical_commit_id
		WHERE a.resident_id = ? AND c.commit_seq <= ?
		ORDER BY c.commit_seq, a.activation_id`, residentID.String(), through.Int64())
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	current := make(map[string]ResidentRevisionProjection)
	for rows.Next() {
		var class, revisionRaw, activationRaw string
		if err := rows.Scan(&class, &revisionRaw, &activationRaw); err != nil {
			return nil, err
		}
		revisionID, err := canonical.ParseID(revisionRaw)
		if err != nil {
			return nil, err
		}
		activationID, err := canonical.ParseID(activationRaw)
		if err != nil {
			return nil, err
		}
		current[class] = ResidentRevisionProjection{Class: class, RevisionID: revisionID, ActivationID: activationID}
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	ordered := make([]ResidentRevisionProjection, 0, 3)
	for _, class := range []string{"principles", "persona", "memory_policy"} {
		if value, ok := current[class]; ok {
			ordered = append(ordered, value)
		}
	}
	return ordered, nil
}

func (repository *ProjectionRepository) evaluateRuntimeState(ctx context.Context, residentID canonical.ID, target projection.Target) (RuntimeStateProjection, error) {
	var result RuntimeStateProjection
	var lastUserAt canonical.Instant
	var rawUserSeq, recordedAt int64
	var erasureState string
	var content []byte
	err := repository.store.reader.QueryRowContext(ctx, runtimeLatestUserEventQuery,
		residentID.String(), target.Head.CommitSeq.Int64(),
	).Scan(&rawUserSeq, &recordedAt, &erasureState, &content)
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return RuntimeStateProjection{}, fmt.Errorf("sqlite: evaluate latest runtime user event: %w", err)
	}
	if err == nil {
		seq, parseErr := canonical.NewSeq(rawUserSeq)
		if parseErr != nil {
			return RuntimeStateProjection{}, parseErr
		}
		result.LastUserEventSeq = &seq
		lastUserAt = canonical.Instant(recordedAt)
		if erasureState == "present" {
			if content == nil {
				return RuntimeStateProjection{}, errors.New("sqlite: latest runtime user content blob is unavailable")
			}
			markers, detectErr := surfaceref.Detect(content)
			if detectErr != nil {
				return RuntimeStateProjection{}, fmt.Errorf("sqlite: detect latest runtime user surface reference: %w", detectErr)
			}
			result.UnresolvedReferenceMarkers = markers
		}
	}

	var rawResidentSeq int64
	if err := repository.store.reader.QueryRowContext(ctx, runtimeLatestResidentEventQuery,
		residentID.String(), target.Head.CommitSeq.Int64(),
		residentID.String(), target.Head.CommitSeq.Int64(),
		residentID.String(), target.Head.CommitSeq.Int64(),
	).Scan(&rawResidentSeq); err != nil {
		return RuntimeStateProjection{}, fmt.Errorf("sqlite: evaluate latest runtime resident event: %w", err)
	}
	if rawResidentSeq > 0 {
		seq, parseErr := canonical.NewSeq(rawResidentSeq)
		if parseErr != nil {
			return RuntimeStateProjection{}, parseErr
		}
		result.LastResidentEventSeq = &seq
	}
	status, err := repository.evaluateStatus(ctx, residentID, target.Head.CommitSeq)
	if err != nil {
		return RuntimeStateProjection{}, err
	}
	if status.Status == "active" && result.LastUserEventSeq != nil && target.AsOf > lastUserAt {
		result.IdleDuration = canonical.Duration(target.AsOf - lastUserAt)
	}
	return result, nil
}

func (repository *ProjectionRepository) evaluateClaimStates(ctx context.Context, request projection.EvaluationRequest) ([]projection.ClaimState, error) {
	activePolicy, err := exactMemoryPolicyDependency(request.Dependencies)
	if err != nil {
		return nil, err
	}
	claims, err := repository.loadClaimSeeds(ctx, request.ResidentID, request.Target.Head.CommitSeq, &request.Target.AsOf)
	if err != nil {
		return nil, err
	}
	evidence, err := repository.loadClaimEvidence(ctx, request.ResidentID, request.Target)
	if err != nil {
		return nil, err
	}
	usages, err := repository.loadClaimUsages(ctx, request.ResidentID, request.Target)
	if err != nil {
		return nil, err
	}
	validity, err := repository.loadClaimValidity(ctx, request.ResidentID, request.Target)
	if err != nil {
		return nil, err
	}
	statusTransitions, err := repository.loadClaimTransitions(ctx, request.ResidentID, request.Target, false)
	if err != nil {
		return nil, err
	}
	stageTransitions, err := repository.loadClaimTransitions(ctx, request.ResidentID, request.Target, true)
	if err != nil {
		return nil, err
	}
	var previousAsOf *canonical.Instant
	if request.Previous != nil {
		value := request.Previous.AsOf
		previousAsOf = &value
	}
	return projection.ReplayClaimStates(ctx, projection.ClaimReplayInput{
		ThroughCommitSeq: request.Target.Head.CommitSeq, AsOf: request.Target.AsOf,
		PreviousAsOf: previousAsOf, ActivePolicyRevisionID: activePolicy,
		Claims: claims, Evidence: evidence, Usages: usages, ValidityAssertions: validity,
		StatusTransitions: statusTransitions, StageTransitions: stageTransitions,
	}, repository.claimStateEvaluator)
}

func exactMemoryPolicyDependency(dependencies []projection.Dependency) (canonical.ID, error) {
	if len(dependencies) != 1 || dependencies[0].Kind != projection.MemoryPolicyDependency {
		return canonical.ID{}, fmt.Errorf("%w: claim_states requires exactly one memory_policy dependency", projection.ErrUnresolvedDependency)
	}
	if err := dependencies[0].Validate(); err != nil {
		return canonical.ID{}, err
	}
	return dependencies[0].VersionID, nil
}

func (repository *ProjectionRepository) loadClaimSeeds(ctx context.Context, residentID canonical.ID, through canonical.CommitSeq, asOf *canonical.Instant) ([]projection.ClaimSeed, error) {
	var asOfValue any
	if asOf != nil {
		asOfValue = asOf.UnixMicro()
	}
	rows, err := repository.store.reader.QueryContext(ctx, `SELECT cl.claim_id, cl.temporal_kind, c.commit_seq, cl.recorded_at
		FROM claims cl
		JOIN canonical_commits c ON c.canonical_commit_id = cl.canonical_commit_id
		WHERE cl.owner_resident_id = ? AND c.commit_seq <= ?
		  AND (? IS NULL OR cl.recorded_at <= ?)
		ORDER BY c.commit_seq, cl.claim_id`, residentID.String(), through.Int64(), asOfValue, asOfValue)
	if err != nil {
		return nil, fmt.Errorf("sqlite: load claim seeds: %w", err)
	}
	defer rows.Close()
	var result []projection.ClaimSeed
	for rows.Next() {
		var claimRaw, temporalKind string
		var commitSeq, recordedAt int64
		if err := rows.Scan(&claimRaw, &temporalKind, &commitSeq, &recordedAt); err != nil {
			return nil, err
		}
		claimID, err := canonical.ParseID(claimRaw)
		if err != nil {
			return nil, err
		}
		commit, err := canonical.NewCommitSeq(commitSeq)
		if err != nil {
			return nil, err
		}
		result = append(result, projection.ClaimSeed{
			ClaimID: claimID, TemporalKind: projection.ClaimTemporalKind(temporalKind),
			CommitSeq: commit, RecordedAt: canonical.Instant(recordedAt),
		})
	}
	return result, rows.Err()
}

func (repository *ProjectionRepository) loadClaimEvidence(ctx context.Context, residentID canonical.ID, target projection.Target) ([]projection.ClaimEvidence, error) {
	rows, err := repository.store.reader.QueryContext(ctx, `SELECT e.evidence_id, e.claim_id, e.event_id,
		e.source_evidence_id, c.commit_seq, e.recorded_at, e.memory_policy_revision_id,
		ev.event_type, e.polarity, e.grade, e.trust_level, e.derivation,
		CASE WHEN e.source_evidence_id IS NULL THEN 0
		     WHEN parent.source_evidence_id IS NULL THEN 1 ELSE 2 END,
		e.reason_code, ev.actor_principal_id = cl.subject_principal_id,
		ev.actor_principal_id = cl.perspective_principal_id, e.weight
		FROM claim_evidence e
		JOIN claims cl ON cl.claim_id = e.claim_id
		JOIN events ev ON ev.event_id = e.event_id
		LEFT JOIN claim_evidence parent ON parent.evidence_id = e.source_evidence_id
		JOIN canonical_commits c ON c.canonical_commit_id = e.canonical_commit_id
		WHERE cl.owner_resident_id = ? AND c.commit_seq <= ? AND e.recorded_at <= ?
		ORDER BY c.commit_seq, e.evidence_id`, residentID.String(), target.Head.CommitSeq.Int64(), target.AsOf.UnixMicro())
	if err != nil {
		return nil, fmt.Errorf("sqlite: load claim evidence: %w", err)
	}
	defer rows.Close()
	var result []projection.ClaimEvidence
	for rows.Next() {
		var evidenceRaw, claimRaw, eventRaw, policyRaw, eventType, polarity, grade, trust, derivation, reasonCode string
		var sourceEvidenceRaw sql.NullString
		var commitSeq, recordedAt, inheritanceDepth, weightRaw int64
		var actorIsSubject, actorIsPerspective bool
		if err := rows.Scan(&evidenceRaw, &claimRaw, &eventRaw, &sourceEvidenceRaw, &commitSeq, &recordedAt,
			&policyRaw, &eventType, &polarity, &grade, &trust, &derivation, &inheritanceDepth, &reasonCode,
			&actorIsSubject, &actorIsPerspective, &weightRaw); err != nil {
			return nil, err
		}
		evidenceID, err := canonical.ParseID(evidenceRaw)
		if err != nil {
			return nil, err
		}
		claimID, err := canonical.ParseID(claimRaw)
		if err != nil {
			return nil, err
		}
		eventID, err := canonical.ParseID(eventRaw)
		if err != nil {
			return nil, err
		}
		policyID, err := canonical.ParseID(policyRaw)
		if err != nil {
			return nil, err
		}
		commit, err := canonical.NewCommitSeq(commitSeq)
		if err != nil {
			return nil, err
		}
		weight, err := canonical.NewWeight(weightRaw)
		if err != nil {
			return nil, err
		}
		value := projection.ClaimEvidence{
			EvidenceID: evidenceID, ClaimID: claimID, SourceEventID: eventID,
			CommitSeq: commit, RecordedAt: canonical.Instant(recordedAt), PolicyRevisionID: policyID,
			SourceEventType: projection.ClaimSourceEventType(eventType), Polarity: projection.ClaimEvidencePolarity(polarity),
			Grade: projection.ClaimEvidenceGrade(grade), TrustLevel: projection.ClaimTrustLevel(trust),
			Derivation: projection.ClaimEvidenceDerivation(derivation), InheritanceDepth: inheritanceDepth,
			ReasonCode: reasonCode, ActorIsSubject: actorIsSubject, ActorIsPerspective: actorIsPerspective, Weight: weight,
		}
		if sourceEvidenceRaw.Valid {
			sourceEvidenceID, err := canonical.ParseID(sourceEvidenceRaw.String)
			if err != nil {
				return nil, err
			}
			value.SourceEvidenceID = &sourceEvidenceID
		}
		result = append(result, value)
	}
	return result, rows.Err()
}

func (repository *ProjectionRepository) loadClaimUsages(ctx context.Context, residentID canonical.ID, target projection.Target) ([]projection.ClaimUsage, error) {
	rows, err := repository.store.reader.QueryContext(ctx, `SELECT u.claim_usage_id, u.claim_id, u.recall_run_id,
		c.commit_seq, u.recorded_at, u.memory_policy_revision_id, u.usage_type
		FROM claim_usages u
		JOIN claims cl ON cl.claim_id = u.claim_id
		JOIN canonical_commits c ON c.canonical_commit_id = u.canonical_commit_id
		WHERE cl.owner_resident_id = ? AND c.commit_seq <= ? AND u.recorded_at <= ?
		ORDER BY c.commit_seq, u.claim_usage_id`, residentID.String(), target.Head.CommitSeq.Int64(), target.AsOf.UnixMicro())
	if err != nil {
		return nil, fmt.Errorf("sqlite: load claim usages: %w", err)
	}
	defer rows.Close()
	var result []projection.ClaimUsage
	for rows.Next() {
		var usageRaw, claimRaw, policyRaw, usageType string
		var recallRunRaw sql.NullString
		var commitSeq, recordedAt int64
		if err := rows.Scan(&usageRaw, &claimRaw, &recallRunRaw, &commitSeq, &recordedAt, &policyRaw, &usageType); err != nil {
			return nil, err
		}
		usageID, err := canonical.ParseID(usageRaw)
		if err != nil {
			return nil, err
		}
		claimID, err := canonical.ParseID(claimRaw)
		if err != nil {
			return nil, err
		}
		policyID, err := canonical.ParseID(policyRaw)
		if err != nil {
			return nil, err
		}
		commit, err := canonical.NewCommitSeq(commitSeq)
		if err != nil {
			return nil, err
		}
		usage := projection.ClaimUsage{
			UsageID: usageID, ClaimID: claimID, CommitSeq: commit, RecordedAt: canonical.Instant(recordedAt),
			PolicyRevisionID: policyID, UsageType: projection.ClaimUsageType(usageType),
		}
		if recallRunRaw.Valid {
			recallRunID, err := canonical.ParseID(recallRunRaw.String)
			if err != nil {
				return nil, err
			}
			usage.RecallRunID = &recallRunID
		}
		result = append(result, usage)
	}
	return result, rows.Err()
}

func (repository *ProjectionRepository) loadClaimValidity(ctx context.Context, residentID canonical.ID, target projection.Target) ([]projection.ClaimValidityAssertion, error) {
	rows, err := repository.store.reader.QueryContext(ctx, `SELECT v.validity_assertion_id, v.claim_id, c.commit_seq, v.recorded_at,
		v.valid_from, v.valid_to, v.confidence
		FROM claim_validity_assertions v
		JOIN claims cl ON cl.claim_id = v.claim_id
		JOIN canonical_commits c ON c.canonical_commit_id = v.canonical_commit_id
		WHERE cl.owner_resident_id = ? AND c.commit_seq <= ? AND v.recorded_at <= ?
		ORDER BY c.commit_seq, v.validity_assertion_id`, residentID.String(), target.Head.CommitSeq.Int64(), target.AsOf.UnixMicro())
	if err != nil {
		return nil, fmt.Errorf("sqlite: load claim validity: %w", err)
	}
	defer rows.Close()
	var result []projection.ClaimValidityAssertion
	for rows.Next() {
		var assertionRaw, claimRaw string
		var commitSeq, recordedAt int64
		var validFromRaw, validToRaw, confidenceRaw sql.NullInt64
		if err := rows.Scan(&assertionRaw, &claimRaw, &commitSeq, &recordedAt, &validFromRaw, &validToRaw, &confidenceRaw); err != nil {
			return nil, err
		}
		assertionID, err := canonical.ParseID(assertionRaw)
		if err != nil {
			return nil, err
		}
		claimID, err := canonical.ParseID(claimRaw)
		if err != nil {
			return nil, err
		}
		commit, err := canonical.NewCommitSeq(commitSeq)
		if err != nil {
			return nil, err
		}
		assertion := projection.ClaimValidityAssertion{
			AssertionID: assertionID, ClaimID: claimID, CommitSeq: commit, RecordedAt: canonical.Instant(recordedAt),
		}
		if validFromRaw.Valid {
			value := canonical.Instant(validFromRaw.Int64)
			assertion.ValidFrom = &value
		}
		if validToRaw.Valid {
			value := canonical.Instant(validToRaw.Int64)
			assertion.ValidTo = &value
		}
		if confidenceRaw.Valid {
			value, err := canonical.NewRatio(confidenceRaw.Int64)
			if err != nil {
				return nil, err
			}
			assertion.Confidence = &value
		}
		result = append(result, assertion)
	}
	return result, rows.Err()
}

func (repository *ProjectionRepository) loadClaimTransitions(ctx context.Context, residentID canonical.ID, target projection.Target, stage bool) ([]projection.ClaimTransition, error) {
	query := `SELECT t.status_transition_id, t.claim_id, c.commit_seq, t.recorded_at, t.from_status, t.to_status
		FROM claim_status_transitions t
		JOIN claims cl ON cl.claim_id = t.claim_id
		JOIN canonical_commits c ON c.canonical_commit_id = t.canonical_commit_id
		WHERE cl.owner_resident_id = ? AND c.commit_seq <= ? AND t.recorded_at <= ?
		ORDER BY c.commit_seq, t.status_transition_id`
	if stage {
		query = `SELECT t.stage_transition_id, t.claim_id, c.commit_seq, t.recorded_at, t.from_stage, t.to_stage
			FROM claim_stage_transitions t
			JOIN claims cl ON cl.claim_id = t.claim_id
			JOIN canonical_commits c ON c.canonical_commit_id = t.canonical_commit_id
			WHERE cl.owner_resident_id = ? AND c.commit_seq <= ? AND t.recorded_at <= ?
			ORDER BY c.commit_seq, t.stage_transition_id`
	}
	rows, err := repository.store.reader.QueryContext(ctx, query, residentID.String(), target.Head.CommitSeq.Int64(), target.AsOf.UnixMicro())
	if err != nil {
		return nil, fmt.Errorf("sqlite: load claim transitions: %w", err)
	}
	defer rows.Close()
	var result []projection.ClaimTransition
	for rows.Next() {
		var transitionRaw, claimRaw, toValue string
		var fromValue sql.NullString
		var commitSeq, recordedAt int64
		if err := rows.Scan(&transitionRaw, &claimRaw, &commitSeq, &recordedAt, &fromValue, &toValue); err != nil {
			return nil, err
		}
		transitionID, err := canonical.ParseID(transitionRaw)
		if err != nil {
			return nil, err
		}
		claimID, err := canonical.ParseID(claimRaw)
		if err != nil {
			return nil, err
		}
		commit, err := canonical.NewCommitSeq(commitSeq)
		if err != nil {
			return nil, err
		}
		result = append(result, projection.ClaimTransition{
			TransitionID: transitionID, ClaimID: claimID, CommitSeq: commit, RecordedAt: canonical.Instant(recordedAt),
			From: fromValue.String, To: toValue,
		})
	}
	return result, rows.Err()
}

func (repository *ProjectionRepository) evaluateClaimViewScopes(ctx context.Context, residentID canonical.ID, through canonical.CommitSeq) ([]projection.ClaimViewScopeState, error) {
	claims, err := repository.loadClaimSeeds(ctx, residentID, through, nil)
	if err != nil {
		return nil, err
	}
	rows, err := repository.store.reader.QueryContext(ctx, `SELECT a.view_scope_assertion_id, a.claim_id,
		c.commit_seq, a.recorded_at, a.view_scope
		FROM claim_view_scope_assertions a
		JOIN claims cl ON cl.claim_id = a.claim_id
		JOIN canonical_commits c ON c.canonical_commit_id = a.canonical_commit_id
		WHERE cl.owner_resident_id = ? AND c.commit_seq <= ?
		ORDER BY c.commit_seq, a.view_scope_assertion_id`, residentID.String(), through.Int64())
	if err != nil {
		return nil, fmt.Errorf("sqlite: load claim view scopes: %w", err)
	}
	defer rows.Close()
	var assertions []projection.ClaimViewScopeAssertion
	for rows.Next() {
		var assertionRaw, claimRaw, viewScope string
		var commitSeq, recordedAt int64
		if err := rows.Scan(&assertionRaw, &claimRaw, &commitSeq, &recordedAt, &viewScope); err != nil {
			return nil, err
		}
		assertionID, err := canonical.ParseID(assertionRaw)
		if err != nil {
			return nil, err
		}
		claimID, err := canonical.ParseID(claimRaw)
		if err != nil {
			return nil, err
		}
		commit, err := canonical.NewCommitSeq(commitSeq)
		if err != nil {
			return nil, err
		}
		assertions = append(assertions, projection.ClaimViewScopeAssertion{
			AssertionID: assertionID, ClaimID: claimID, CommitSeq: commit,
			RecordedAt: canonical.Instant(recordedAt), ViewScope: projection.ClaimViewScope(viewScope),
		})
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return projection.ReplayClaimViewScopes(through, claims, assertions)
}

func (repository *ProjectionRepository) Watermark(ctx context.Context, name projection.Name, residentID canonical.ID) (projection.Watermark, bool, error) {
	return readProjectionWatermark(ctx, repository.store.reader, name, residentID)
}

func (repository *ProjectionRepository) Apply(ctx context.Context, request projection.ApplyRequest) error {
	if ctx == nil {
		return errors.New("sqlite: nil Projection apply context")
	}
	if err := repository.validateProjectionApply(request); err != nil {
		return err
	}
	release, err := repository.store.writes.acquireProjection(ctx)
	if err != nil {
		return err
	}
	defer release()
	if err := repository.store.verifyWritableBoundary("before Projection apply transaction"); err != nil {
		return err
	}
	txCtx, cancel := context.WithTimeout(ctx, repository.transactionTimeout)
	defer cancel()
	tx, err := repository.store.writer.BeginTx(txCtx, &sql.TxOptions{Isolation: sql.LevelSerializable})
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()
	if err := repository.store.verifyWritableBoundary("after Projection apply transaction open"); err != nil {
		return err
	}
	if err := verifyProjectionCAS(txCtx, tx, request.Definition.Name, request.ResidentID, request.Observed); err != nil {
		return err
	}
	if err := replaceProjectionBody(txCtx, tx, request.Definition.Name, request.ResidentID, request.Evaluation.Value); err != nil {
		return err
	}
	if repository.applyHook != nil {
		if err := repository.applyHook(txCtx, "after_body"); err != nil {
			return err
		}
	}
	if err := replaceProjectionWatermark(txCtx, tx, request.Watermark); err != nil {
		return err
	}
	if repository.applyHook != nil {
		if err := repository.applyHook(txCtx, "after_watermark"); err != nil {
			return err
		}
	}
	if err := repository.store.verifyWritableBoundary("before Projection apply commit"); err != nil {
		return err
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("sqlite: commit Projection transaction: %w", err)
	}
	return repository.store.verifyWritableBoundary("after Projection apply commit")
}

func (repository *ProjectionRepository) Drop(ctx context.Context, request projection.DropRequest) error {
	if ctx == nil {
		return errors.New("sqlite: nil Projection drop context")
	}
	if err := repository.validateRegisteredDefinition(request.Definition); err != nil {
		return err
	}
	if err := request.ResidentID.Validate(); err != nil {
		return err
	}
	release, err := repository.store.writes.acquireProjection(ctx)
	if err != nil {
		return err
	}
	defer release()
	if err := repository.store.verifyWritableBoundary("before Projection drop transaction"); err != nil {
		return err
	}
	txCtx, cancel := context.WithTimeout(ctx, repository.transactionTimeout)
	defer cancel()
	tx, err := repository.store.writer.BeginTx(txCtx, &sql.TxOptions{Isolation: sql.LevelSerializable})
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()
	if err := repository.store.verifyWritableBoundary("after Projection drop transaction open"); err != nil {
		return err
	}
	if err := verifyProjectionCAS(txCtx, tx, request.Definition.Name, request.ResidentID, request.Observed); err != nil {
		return err
	}
	if err := deleteProjectionBody(txCtx, tx, request.Definition.Name, request.ResidentID); err != nil {
		return err
	}
	if _, err := tx.ExecContext(txCtx, `DELETE FROM projection_watermarks WHERE projection_name = ? AND resident_id = ?`, request.Definition.Name, request.ResidentID.String()); err != nil {
		return err
	}
	if err := repository.store.verifyWritableBoundary("before Projection drop commit"); err != nil {
		return err
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("sqlite: commit Projection drop transaction: %w", err)
	}
	return repository.store.verifyWritableBoundary("after Projection drop commit")
}

func (repository *ProjectionRepository) validateProjectionApply(request projection.ApplyRequest) error {
	if err := repository.validateRegisteredDefinition(request.Definition); err != nil {
		return err
	}
	if err := request.ResidentID.Validate(); err != nil {
		return err
	}
	if err := request.Watermark.Validate(); err != nil {
		return err
	}
	if request.Watermark.ProjectionName != request.Definition.Name || request.Watermark.ResidentID != request.ResidentID || request.Watermark.ProjectionVersion != request.Definition.Version {
		return errors.New("sqlite: Projection apply watermark identity mismatch")
	}
	if request.Observed != nil {
		if request.Watermark.AsOf < request.Observed.AsOf {
			return projection.ErrAsOfRegression
		}
		if request.Watermark.SourceCommitSeq < request.Observed.SourceCommitSeq {
			return projection.ErrSourceRegression
		}
	}
	if err := validateProjectionApplyPlan(request); err != nil {
		return err
	}
	declared := make(map[projection.DependencyKind]struct{}, len(request.Definition.Dependencies))
	for _, kind := range request.Definition.Dependencies {
		declared[kind] = struct{}{}
	}
	counts := make(map[projection.DependencyKind]int, len(request.Watermark.Dependencies))
	for _, dependency := range request.Watermark.Dependencies {
		if _, ok := declared[dependency.Kind]; !ok {
			return fmt.Errorf("sqlite: Projection watermark contains undeclared dependency %q", dependency.Kind)
		}
		counts[dependency.Kind]++
	}
	for kind := range declared {
		if counts[kind] != 1 {
			return fmt.Errorf("%w: Projection watermark requires exactly one %s dependency", projection.ErrUnresolvedDependency, kind)
		}
	}
	return nil
}

func (repository *ProjectionRepository) validateRegisteredDefinition(definition projection.Definition) error {
	registry := repository.registry
	if registry == nil {
		var err error
		registry, err = ActiveProjectionRegistry()
		if err != nil {
			return err
		}
	}
	registered, err := registry.Definition(definition.Name)
	if err != nil {
		return err
	}
	if registered.Version != definition.Version || registered.TimeSensitive != definition.TimeSensitive ||
		!slices.Equal(registered.Dependencies, definition.Dependencies) ||
		!slices.Equal(registered.RebuildOnActivation, definition.RebuildOnActivation) {
		return fmt.Errorf("sqlite: Projection definition is not registered exactly")
	}
	return nil
}

func validateProjectionApplyPlan(request projection.ApplyRequest) error {
	plan := request.Plan
	switch plan.Kind {
	case projection.FullBuild:
		if request.Observed != nil {
			return errors.New("sqlite: full build requires an absent observed watermark")
		}
		if !plan.NeedCommitCatchUp || plan.NeedAsOfReEvaluation != request.Definition.TimeSensitive {
			return errors.New("sqlite: full build cursor components do not match the definition")
		}
	case projection.FullRebuild:
		if request.Observed == nil {
			return errors.New("sqlite: full rebuild requires an observed watermark")
		}
		if !plan.NeedCommitCatchUp || plan.NeedAsOfReEvaluation != request.Definition.TimeSensitive {
			return errors.New("sqlite: full rebuild cursor components do not match the definition")
		}
	case projection.Update:
		if request.Observed == nil {
			return errors.New("sqlite: incremental update requires an observed watermark")
		}
		if !plan.NeedCommitCatchUp && !plan.NeedAsOfReEvaluation {
			return errors.New("sqlite: incremental update executes no cursor component")
		}
		if plan.NeedAsOfReEvaluation && !request.Definition.TimeSensitive {
			return errors.New("sqlite: non-time-sensitive Projection cannot execute as-of evaluation")
		}
	case projection.UpToDate:
		return errors.New("sqlite: up-to-date Projection must not be applied")
	default:
		return fmt.Errorf("sqlite: unknown Projection update kind %q", plan.Kind)
	}
	if request.Observed == nil {
		return nil
	}
	observed := request.Observed
	if plan.Kind == projection.Update {
		if plan.NeedCommitCatchUp {
			if request.Watermark.SourceCommitSeq <= observed.SourceCommitSeq {
				return errors.New("sqlite: executed commit catch-up did not advance source cursor")
			}
		} else if request.Watermark.SourceCommitSeq != observed.SourceCommitSeq {
			return errors.New("sqlite: unexecuted commit cursor changed")
		}
		if plan.NeedAsOfReEvaluation {
			if request.Watermark.AsOf <= observed.AsOf {
				return errors.New("sqlite: executed as-of evaluation did not advance as_of")
			}
		} else if request.Watermark.AsOf != observed.AsOf || request.Watermark.AsOfTZ != observed.AsOfTZ {
			return errors.New("sqlite: unexecuted as-of cursor changed")
		}
	}
	return nil
}

type projectionRowQuerier interface {
	QueryContext(context.Context, string, ...any) (*sql.Rows, error)
}

func readProjectionWatermark(ctx context.Context, query projectionRowQuerier, name projection.Name, residentID canonical.ID) (projection.Watermark, bool, error) {
	rows, err := query.QueryContext(ctx, `SELECT w.projection_version, w.source_commit_seq, w.as_of, w.as_of_tz,
		d.dependency_kind, d.dependency_version_id
		FROM projection_watermarks w
		LEFT JOIN projection_watermark_dependencies d
		  ON d.projection_name = w.projection_name AND d.resident_id = w.resident_id
		WHERE w.projection_name = ? AND w.resident_id = ?
		ORDER BY d.dependency_kind, d.dependency_version_id`, name, residentID.String())
	if err != nil {
		return projection.Watermark{}, false, err
	}
	defer rows.Close()
	var watermark projection.Watermark
	found := false
	for rows.Next() {
		var version, asOfTZ string
		var sourceCommit, asOf int64
		var kind, raw sql.NullString
		if err := rows.Scan(&version, &sourceCommit, &asOf, &asOfTZ, &kind, &raw); err != nil {
			return projection.Watermark{}, false, err
		}
		if !found {
			commitSeq, err := canonical.NewCommitSeq(sourceCommit)
			if err != nil {
				return projection.Watermark{}, false, err
			}
			timezone, err := canonical.ParseTimezone(asOfTZ)
			if err != nil {
				return projection.Watermark{}, false, err
			}
			watermark = projection.Watermark{
				ProjectionName: name, ResidentID: residentID, ProjectionVersion: projection.Version(version),
				SourceCommitSeq: commitSeq, AsOf: canonical.Instant(asOf), AsOfTZ: timezone,
			}
			found = true
		}
		if kind.Valid != raw.Valid {
			return projection.Watermark{}, false, errors.New("sqlite: partial Projection dependency row")
		}
		if kind.Valid {
			versionID, err := canonical.ParseID(raw.String)
			if err != nil {
				return projection.Watermark{}, false, err
			}
			watermark.Dependencies = append(watermark.Dependencies, projection.Dependency{Kind: projection.DependencyKind(kind.String), VersionID: versionID})
		}
	}
	if err := rows.Err(); err != nil {
		return projection.Watermark{}, false, err
	}
	return watermark, found, nil
}

func verifyProjectionCAS(ctx context.Context, tx *sql.Tx, name projection.Name, residentID canonical.ID, observed *projection.Watermark) error {
	current, exists, err := readProjectionWatermark(ctx, tx, name, residentID)
	if err != nil {
		return err
	}
	if observed == nil {
		if exists {
			return projection.ErrCASConflict
		}
		return nil
	}
	if !exists || !watermarksEqual(*observed, current) {
		return projection.ErrCASConflict
	}
	return nil
}

func watermarksEqual(left, right projection.Watermark) bool {
	if left.ProjectionName != right.ProjectionName || left.ResidentID != right.ResidentID ||
		left.ProjectionVersion != right.ProjectionVersion || left.SourceCommitSeq != right.SourceCommitSeq ||
		left.AsOf != right.AsOf || left.AsOfTZ != right.AsOfTZ {
		return false
	}
	equal, err := projection.DependencySetEqual(left.Dependencies, right.Dependencies)
	return err == nil && equal
}

func replaceProjectionBody(ctx context.Context, tx *sql.Tx, name projection.Name, residentID canonical.ID, value any) error {
	if err := deleteProjectionBody(ctx, tx, name, residentID); err != nil {
		return err
	}
	switch name {
	case projection.ResidentCurrentStatusName:
		status, ok := value.(ResidentStatusProjection)
		if !ok {
			return fmt.Errorf("sqlite: unexpected status Projection value %T", value)
		}
		_, err := tx.ExecContext(ctx, `INSERT INTO resident_current_status(resident_id, status, source_transition_id) VALUES (?, ?, ?)`, residentID.String(), status.Status, status.SourceTransitionID.String())
		return err
	case projection.ResidentCurrentRevisionName:
		revisions, ok := value.([]ResidentRevisionProjection)
		if !ok {
			return fmt.Errorf("sqlite: unexpected revision Projection value %T", value)
		}
		for _, revision := range revisions {
			if _, err := tx.ExecContext(ctx, `INSERT INTO resident_current_revision(resident_id, revision_class, revision_id, activation_id) VALUES (?, ?, ?, ?)`, residentID.String(), revision.Class, revision.RevisionID.String(), revision.ActivationID.String()); err != nil {
				return err
			}
		}
		return nil
	case projection.RuntimeStatesName:
		state, ok := value.(RuntimeStateProjection)
		if !ok {
			return fmt.Errorf("sqlite: unexpected runtime Projection value %T", value)
		}
		markers, err := surfaceref.CanonicalJSON(state.UnresolvedReferenceMarkers)
		if err != nil {
			return fmt.Errorf("sqlite: encode runtime Projection reference markers: %w", err)
		}
		var userSeq, residentSeq any
		if state.LastUserEventSeq != nil {
			userSeq = state.LastUserEventSeq.Int64()
		}
		if state.LastResidentEventSeq != nil {
			residentSeq = state.LastResidentEventSeq.Int64()
		}
		_, err = tx.ExecContext(ctx, `INSERT INTO runtime_states(resident_id, last_user_event_seq, last_resident_event_seq, unresolved_reference_markers, idle_duration) VALUES (?, ?, ?, ?, ?)`, residentID.String(), userSeq, residentSeq, markers.String(), state.IdleDuration.Microseconds())
		return err
	case projection.ClaimStatesName:
		states, ok := value.([]projection.ClaimState)
		if !ok {
			return fmt.Errorf("sqlite: unexpected claim_states Projection value %T", value)
		}
		for _, state := range states {
			if err := state.ClaimID.Validate(); err != nil {
				return err
			}
			if err := state.Stage.Validate(); err != nil {
				return err
			}
			if err := state.Status.Validate(); err != nil {
				return err
			}
			if math.IsNaN(state.Salience) || math.IsInf(state.Salience, 0) {
				return errors.New("sqlite: claim salience must be finite")
			}
			if err := state.Confidence.Validate(); err != nil {
				return err
			}
			if err := state.Currentness.Validate(); err != nil {
				return err
			}
			if err := state.TemporalRelation.Validate(); err != nil {
				return err
			}
			if err := state.EvidenceCount.Validate(); err != nil {
				return err
			}
			var lastReferencedAt any
			if state.LastReferencedAt != nil {
				lastReferencedAt = state.LastReferencedAt.UnixMicro()
			}
			if _, err := tx.ExecContext(ctx, `INSERT INTO claim_states(
				claim_id, resident_id, stage, status, salience, confidence, currentness,
				temporal_relation, last_referenced_at, evidence_count
			) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`, state.ClaimID.String(), residentID.String(),
				state.Stage, state.Status, state.Salience, state.Confidence.Millionths(), state.Currentness.Millionths(),
				state.TemporalRelation, lastReferencedAt, state.EvidenceCount.Int64()); err != nil {
				return err
			}
		}
		return nil
	case projection.ClaimViewScopeCurrentName:
		states, ok := value.([]projection.ClaimViewScopeState)
		if !ok {
			return fmt.Errorf("sqlite: unexpected claim_view_scope_current Projection value %T", value)
		}
		for _, state := range states {
			if err := state.ClaimID.Validate(); err != nil {
				return err
			}
			if err := state.ViewScope.Validate(); err != nil {
				return err
			}
			if err := state.SourceAssertionID.Validate(); err != nil {
				return err
			}
			if _, err := tx.ExecContext(ctx, `INSERT INTO claim_view_scope_current(
				claim_id, resident_id, view_scope, source_assertion_id
			) VALUES (?, ?, ?, ?)`, state.ClaimID.String(), residentID.String(), state.ViewScope,
				state.SourceAssertionID.String()); err != nil {
				return err
			}
		}
		return nil
	case projection.ContentReferencesName:
		references, ok := value.([]projection.ContentReference)
		if !ok {
			return fmt.Errorf("sqlite: unexpected content_references Projection value %T", value)
		}
		if err := projection.ValidateContentReferences(references, residentID); err != nil {
			return err
		}
		for _, reference := range references {
			var dedupeScopeID, algorithm, digest any
			if reference.DedupeScopeID != nil {
				dedupeScopeID = reference.DedupeScopeID.String()
				algorithm = *reference.BlobHashAlgorithm
				digest = reference.BlobHash.Bytes()
			}
			if _, err := tx.ExecContext(ctx, `INSERT INTO content_references(
				content_id, resident_id, referrer_kind, referrer_id, referrer_field,
				dedupe_scope_id, blob_hash_algorithm, blob_hash
			) VALUES (?, ?, ?, ?, ?, ?, ?, ?)`, reference.ContentID.String(), residentID.String(),
				reference.ReferrerKind, reference.ReferrerID.String(), reference.ReferrerField,
				dedupeScopeID, algorithm, digest); err != nil {
				return err
			}
		}
		return nil
	default:
		return fmt.Errorf("%w: %q", projection.ErrUndeclaredProjection, name)
	}
}

func deleteProjectionBody(ctx context.Context, tx *sql.Tx, name projection.Name, residentID canonical.ID) error {
	var table string
	switch name {
	case projection.ResidentCurrentStatusName:
		table = "resident_current_status"
	case projection.ResidentCurrentRevisionName:
		table = "resident_current_revision"
	case projection.RuntimeStatesName:
		table = "runtime_states"
	case projection.ClaimStatesName:
		table = "claim_states"
	case projection.ClaimViewScopeCurrentName:
		table = "claim_view_scope_current"
	case projection.ContentReferencesName:
		table = "content_references"
	default:
		return fmt.Errorf("%w: %q", projection.ErrUndeclaredProjection, name)
	}
	_, err := tx.ExecContext(ctx, `DELETE FROM `+table+` WHERE resident_id = ?`, residentID.String())
	return err
}

func replaceProjectionWatermark(ctx context.Context, tx *sql.Tx, watermark projection.Watermark) error {
	if _, err := tx.ExecContext(ctx, `DELETE FROM projection_watermarks WHERE projection_name = ? AND resident_id = ?`, watermark.ProjectionName, watermark.ResidentID.String()); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO projection_watermarks(projection_name, resident_id, projection_version, source_commit_seq, as_of, as_of_tz) VALUES (?, ?, ?, ?, ?, ?)`, watermark.ProjectionName, watermark.ResidentID.String(), watermark.ProjectionVersion, watermark.SourceCommitSeq.Int64(), watermark.AsOf.UnixMicro(), watermark.AsOfTZ.String()); err != nil {
		return err
	}
	for _, dependency := range watermark.Dependencies {
		if _, err := tx.ExecContext(ctx, `INSERT INTO projection_watermark_dependencies(projection_name, resident_id, dependency_kind, dependency_version_id) VALUES (?, ?, ?, ?)`, watermark.ProjectionName, watermark.ResidentID.String(), dependency.Kind, dependency.VersionID.String()); err != nil {
			return err
		}
	}
	return nil
}

var _ projection.Source = (*ProjectionRepository)(nil)
var _ projection.Store = (*ProjectionRepository)(nil)
