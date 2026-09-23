package sqlite

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"fmt"

	"mahoroba.local/mahoroba/internal/canonical"
	"mahoroba.local/mahoroba/internal/domain"
)

type CanonicalRepository struct {
	store *Store
}

func (s *Store) Canonical() *CanonicalRepository {
	return &CanonicalRepository{store: s}
}

func (r *CanonicalRepository) LoadHead(ctx context.Context) (canonical.Head, error) {
	if err := r.verifyCanonicalBoundary("before Canonical head read"); err != nil {
		return canonical.Head{}, err
	}
	var seq int64
	var committedAt int64
	err := r.store.reader.QueryRowContext(ctx, `
		SELECT commit_seq, committed_at
		FROM canonical_commits
		ORDER BY commit_seq DESC
		LIMIT 1`).Scan(&seq, &committedAt)
	if errors.Is(err, sql.ErrNoRows) {
		if err := r.verifyCanonicalBoundary("after Canonical head read"); err != nil {
			return canonical.Head{}, err
		}
		return canonical.Head{}, nil
	}
	if err != nil {
		return canonical.Head{}, fmt.Errorf("load canonical head: %w", err)
	}
	commitSeq, err := canonical.NewCommitSeq(seq)
	if err != nil {
		return canonical.Head{}, err
	}
	if err := r.verifyCanonicalBoundary("after Canonical head read"); err != nil {
		return canonical.Head{}, err
	}
	return canonical.Head{Exists: true, CommitSeq: commitSeq, CommittedAt: canonical.Instant(committedAt)}, nil
}

func (r *CanonicalRepository) Begin(ctx context.Context, metadata canonical.CommitMetadata) (canonical.CanonicalUoW, error) {
	if err := r.verifyCanonicalBoundary("before Canonical write admission"); err != nil {
		return nil, err
	}
	release, err := r.store.writes.acquireHigh(ctx)
	if err != nil {
		return nil, fmt.Errorf("acquire Canonical write priority: %w", err)
	}
	tx, err := r.store.writer.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelSerializable})
	if err != nil {
		release()
		return nil, err
	}
	if err := r.verifyCanonicalBoundary("after Canonical transaction open"); err != nil {
		_ = tx.Rollback()
		release()
		return nil, err
	}
	var resident any
	if residentID, ok := metadata.Scope.ResidentID(); ok {
		resident = residentID.String()
	}
	_, err = tx.ExecContext(ctx, `
		INSERT INTO canonical_commits(
			canonical_commit_id, commit_seq, resident_id, committed_at, committed_tz
		) VALUES (?, ?, ?, ?, ?)`,
		metadata.CommitID.String(), metadata.CommitSeq.Int64(), resident,
		metadata.CommittedAt.UnixMicro(), metadata.CommittedTZ.String())
	if err != nil {
		_ = tx.Rollback()
		release()
		return nil, fmt.Errorf("insert canonical commit: %w", err)
	}
	return &canonicalUoW{
		tx: tx, metadata: metadata, releaseWrite: release,
		boundary: r.store.canonicalBoundary,
	}, nil
}

func (r *CanonicalRepository) verifyCanonicalBoundary(stage string) error {
	if r == nil || r.store == nil {
		return nil
	}
	return r.store.verifyWritableBoundary(stage)
}

type canonicalUoW struct {
	tx           *sql.Tx
	metadata     canonical.CommitMetadata
	closed       bool
	releaseWrite func()
	boundary     interface{ Verify() error }
}

func (u *canonicalUoW) Metadata() canonical.CommitMetadata { return u.metadata }

func (u *canonicalUoW) Commit(context.Context) error {
	if u.closed {
		return errors.New("sqlite: canonical UoW already closed")
	}
	if u.boundary != nil {
		if err := u.boundary.Verify(); err != nil {
			u.closed = true
			rollbackErr := u.tx.Rollback()
			u.releaseWrite()
			return errors.Join(fmt.Errorf("sqlite: Canonical pre-commit database boundary: %w", err), rollbackErr)
		}
	}
	u.closed = true
	defer u.releaseWrite()
	err := u.tx.Commit()
	if err == nil && u.boundary != nil {
		if boundaryErr := u.boundary.Verify(); boundaryErr != nil {
			return fmt.Errorf("sqlite: Canonical post-commit database boundary: %w", boundaryErr)
		}
	}
	return err
}

func (u *canonicalUoW) Rollback(context.Context) error {
	if u.closed {
		return nil
	}
	u.closed = true
	err := u.tx.Rollback()
	u.releaseWrite()
	if errors.Is(err, sql.ErrTxDone) {
		return nil
	}
	return err
}

func (u *canonicalUoW) InsertGlobalBootstrap(ctx context.Context, value domain.GlobalBootstrap) error {
	m := u.metadata
	if !m.Scope.IsGlobal() {
		return errors.New("sqlite: global bootstrap requires global scope")
	}
	if err := domain.ValidateExactDialoguePipelineDefinition(domain.PipelineVersionDefinition{
		ID: value.PipelineVersionID, Kind: "dialogue", VersionKey: domain.DialoguePipelineVersionV4,
		Definition: value.PipelineDefinition,
	}); err != nil {
		return err
	}
	for _, principal := range []struct {
		id, kind, name string
	}{
		{value.SystemPrincipalID.String(), "system", "Mahoroba System"},
		{value.OwnerPrincipalID.String(), "human", value.OwnerDisplayName},
	} {
		if _, err := u.tx.ExecContext(ctx, `INSERT INTO principals(
			principal_id, canonical_commit_id, kind, display_name, created_at, created_tz
		) VALUES (?, ?, ?, ?, ?, ?)`, principal.id, m.CommitID.String(), principal.kind, principal.name,
			m.CommittedAt.UnixMicro(), m.CommittedTZ.String()); err != nil {
			return fmt.Errorf("insert %s principal: %w", principal.kind, err)
		}
	}
	if _, err := u.tx.ExecContext(ctx, `INSERT INTO pipeline_versions(
		pipeline_version_id, canonical_commit_id, pipeline_kind, version_key, definition, recorded_at, recorded_tz
	) VALUES (?, ?, 'dialogue', ?, ?, ?, ?)`, value.PipelineVersionID.String(), m.CommitID.String(),
		domain.DialoguePipelineVersionV4, value.PipelineDefinition.String(), m.CommittedAt.UnixMicro(), m.CommittedTZ.String()); err != nil {
		return fmt.Errorf("insert dialogue pipeline: %w", err)
	}
	if _, err := u.tx.ExecContext(ctx, `INSERT INTO sessionization_policy_versions(
		sessionization_policy_version_id, canonical_commit_id, version_key, definition, recorded_at, recorded_tz
	) VALUES (?, ?, ?, ?, ?, ?)`, value.SessionPolicyID.String(), m.CommitID.String(), domain.SessionPolicyVersion,
		value.SessionDefinition.String(), m.CommittedAt.UnixMicro(), m.CommittedTZ.String()); err != nil {
		return fmt.Errorf("insert sessionization policy: %w", err)
	}
	return nil
}

func (u *canonicalUoW) InsertDraftResident(ctx context.Context, value domain.DraftResident) error {
	m := u.metadata
	if err := u.requireResidentScope(value.ResidentID); err != nil {
		return err
	}
	var duplicate int
	if err := u.tx.QueryRowContext(ctx, `SELECT count(*) FROM residents WHERE name = ? AND seed_key = ?`, value.Name, value.SeedKey).Scan(&duplicate); err != nil {
		return fmt.Errorf("check duplicate resident bootstrap: %w", err)
	}
	if duplicate != 0 {
		return fmt.Errorf("resident bootstrap already exists for name %q and seed %q", value.Name, value.SeedKey)
	}
	if _, err := u.tx.ExecContext(ctx, `INSERT INTO principals(
		principal_id, canonical_commit_id, kind, display_name, created_at, created_tz
	) VALUES (?, ?, 'resident', ?, ?, ?)`, value.ResidentPrincipalID.String(), m.CommitID.String(), value.Name,
		m.CommittedAt.UnixMicro(), m.CommittedTZ.String()); err != nil {
		return fmt.Errorf("insert resident principal: %w", err)
	}
	if _, err := u.tx.ExecContext(ctx, `INSERT INTO residents(
		resident_id, canonical_commit_id, principal_id, name, description_content_id, seed_key,
		parent_resident_id, branched_from_seq, branched_at, branched_tz, created_at, created_tz
	) VALUES (?, ?, ?, ?, NULL, ?, NULL, NULL, NULL, NULL, ?, ?)`, value.ResidentID.String(), m.CommitID.String(),
		value.ResidentPrincipalID.String(), value.Name, value.SeedKey, m.CommittedAt.UnixMicro(), m.CommittedTZ.String()); err != nil {
		return fmt.Errorf("insert resident: %w", err)
	}
	if err := u.insertContent(ctx, value.PrinciplesContent); err != nil {
		return err
	}
	if _, err := u.tx.ExecContext(ctx, `INSERT INTO resident_revisions(
		revision_id, canonical_commit_id, resident_id, revision_class, content_id, parent_revision_id,
		created_by_run_id, reason_content_id, recorded_at, recorded_tz
	) VALUES (?, ?, ?, 'principles', ?, NULL, NULL, NULL, ?, ?)`, value.PrinciplesRevisionID.String(),
		m.CommitID.String(), value.ResidentID.String(), value.PrinciplesContent.ID.String(), m.CommittedAt.UnixMicro(), m.CommittedTZ.String()); err != nil {
		return fmt.Errorf("insert principles revision: %w", err)
	}
	if _, err := u.tx.ExecContext(ctx, `INSERT INTO resident_status_transitions(
		resident_status_transition_id, canonical_commit_id, resident_id, from_status, to_status,
		actor_principal_id, reason_code, reason_content_id, occurred_at, occurred_tz, recorded_at, recorded_tz
	) VALUES (?, ?, ?, NULL, 'draft', ?, 'bootstrap', NULL, ?, ?, ?, ?)`, value.StatusTransitionID.String(),
		m.CommitID.String(), value.ResidentID.String(), value.OwnerPrincipalID.String(), m.CommittedAt.UnixMicro(),
		m.CommittedTZ.String(), m.CommittedAt.UnixMicro(), m.CommittedTZ.String()); err != nil {
		return fmt.Errorf("insert draft transition: %w", err)
	}
	return nil
}

func (u *canonicalUoW) ApproveAndActivatePrinciples(ctx context.Context, value domain.ApprovePrinciples) error {
	m := u.metadata
	if err := u.requireResidentScope(value.ResidentID); err != nil {
		return err
	}
	if err := u.requireOwnerHuman(ctx, value.ResidentID, value.OwnerPrincipalID); err != nil {
		return err
	}
	status, err := u.currentStatus(ctx, value.ResidentID)
	if err != nil {
		return err
	}
	if status != "draft" && status != "active" {
		return fmt.Errorf("sqlite: principles approval requires draft or already-active resident, got %s", status)
	}
	var revisionResident, revisionClass string
	if err := u.tx.QueryRowContext(ctx, `SELECT resident_id, revision_class FROM resident_revisions WHERE revision_id = ?`, value.RevisionID.String()).Scan(&revisionResident, &revisionClass); err != nil {
		return fmt.Errorf("resolve principles revision: %w", err)
	}
	if revisionResident != value.ResidentID.String() || revisionClass != "principles" {
		return errors.New("sqlite: revision is not this resident's principles")
	}
	var latestRevision string
	if err := u.tx.QueryRowContext(ctx, `SELECT r.revision_id
		FROM resident_revisions r
		JOIN canonical_commits c ON c.canonical_commit_id = r.canonical_commit_id
		WHERE r.resident_id = ? AND r.revision_class = 'principles'
		ORDER BY c.commit_seq DESC LIMIT 1`, value.ResidentID.String()).Scan(&latestRevision); err != nil {
		return fmt.Errorf("resolve latest principles revision: %w", err)
	}
	if latestRevision != value.RevisionID.String() {
		return errors.New("sqlite: bootstrap approval revision is not the resident's current principles")
	}

	matching, total, err := u.bootstrapPrinciplesActivations(ctx, value)
	if err != nil {
		return err
	}
	if total != 0 {
		if total == 1 && matching == 1 {
			return canonical.ErrNoMutation
		}
		return errors.New("sqlite: conflicting principles approval or activation already exists")
	}
	var approvalCount int
	if err := u.tx.QueryRowContext(ctx, `SELECT count(*)
		FROM resident_revision_approvals p
		JOIN resident_revisions r ON r.revision_id = p.revision_id
		WHERE r.resident_id = ? AND r.revision_class = 'principles'`, value.ResidentID.String()).Scan(&approvalCount); err != nil {
		return fmt.Errorf("check existing principles approvals: %w", err)
	}
	if approvalCount != 0 {
		return errors.New("sqlite: unpaired or conflicting principles approval already exists")
	}
	if status != "draft" {
		return errors.New("sqlite: active resident lacks the matching bootstrap principles approval")
	}
	if _, err := u.tx.ExecContext(ctx, `INSERT INTO resident_revision_approvals(
		approval_id, canonical_commit_id, revision_id, approver_principal_id, decision,
		reason_content_id, recorded_at, recorded_tz
	) VALUES (?, ?, ?, ?, 'approved', NULL, ?, ?)`, value.ApprovalID.String(), m.CommitID.String(), value.RevisionID.String(),
		value.OwnerPrincipalID.String(), m.CommittedAt.UnixMicro(), m.CommittedTZ.String()); err != nil {
		return fmt.Errorf("insert principles approval: %w", err)
	}
	if _, err := u.tx.ExecContext(ctx, `INSERT INTO resident_revision_activations(
		activation_id, canonical_commit_id, resident_id, revision_id, actor_principal_id, approval_id,
		reason_code, reason_content_id, recorded_at, recorded_tz
	) VALUES (?, ?, ?, ?, ?, ?, 'bootstrap_approved', NULL, ?, ?)`, value.ActivationID.String(), m.CommitID.String(),
		value.ResidentID.String(), value.RevisionID.String(), value.OwnerPrincipalID.String(), value.ApprovalID.String(),
		m.CommittedAt.UnixMicro(), m.CommittedTZ.String()); err != nil {
		return fmt.Errorf("activate principles: %w", err)
	}
	return nil
}

func (u *canonicalUoW) FinalizeResident(ctx context.Context, value domain.FinalizeResident) error {
	m := u.metadata
	if err := u.requireResidentScope(value.ResidentID); err != nil {
		return err
	}
	if value.PersonaContent.ResidentID != value.ResidentID || value.PersonaContent.Class != "persona_text" ||
		value.MemoryContent.ResidentID != value.ResidentID || value.MemoryContent.Class != "memory_policy_text" {
		return errors.New("sqlite: finalize content scope or class mismatch")
	}
	if err := u.requireOwnerHuman(ctx, value.ResidentID, value.OwnerPrincipalID); err != nil {
		return err
	}
	status, err := u.currentStatus(ctx, value.ResidentID)
	if err != nil {
		return err
	}
	var approved int
	if err := u.tx.QueryRowContext(ctx, `SELECT EXISTS(
		SELECT 1 FROM resident_revision_activations a
		JOIN resident_revisions r ON r.revision_id = a.revision_id
		JOIN resident_revision_approvals p ON p.approval_id = a.approval_id AND p.decision = 'approved'
		WHERE a.resident_id = ? AND r.revision_class = 'principles'
		  AND a.actor_principal_id = ? AND p.approver_principal_id = ?
		  AND a.reason_code = 'bootstrap_approved')`, value.ResidentID.String(), value.OwnerPrincipalID.String(), value.OwnerPrincipalID.String()).Scan(&approved); err != nil || approved != 1 {
		return errors.New("sqlite: principles approval and activation required before finalize")
	}
	if status == "active" {
		matches, err := u.bootstrapFinalizationMatches(ctx, value)
		if err != nil {
			return err
		}
		if matches {
			return canonical.ErrNoMutation
		}
		return errors.New("sqlite: conflicting bootstrap persona or memory policy already finalized")
	}
	if status != "draft" {
		return fmt.Errorf("sqlite: finalize requires draft or identical already-active state, got %s", status)
	}
	for _, item := range []struct {
		class                string
		revision, activation canonical.ID
		content              domain.Content
	}{
		{"persona", value.PersonaRevisionID, value.PersonaActivationID, value.PersonaContent},
		{"memory_policy", value.MemoryRevisionID, value.MemoryActivationID, value.MemoryContent},
	} {
		if err := u.insertContent(ctx, item.content); err != nil {
			return err
		}
		if _, err := u.tx.ExecContext(ctx, `INSERT INTO resident_revisions(
			revision_id, canonical_commit_id, resident_id, revision_class, content_id, parent_revision_id,
			created_by_run_id, reason_content_id, recorded_at, recorded_tz
		) VALUES (?, ?, ?, ?, ?, NULL, NULL, NULL, ?, ?)`, item.revision.String(), m.CommitID.String(),
			value.ResidentID.String(), item.class, item.content.ID.String(), m.CommittedAt.UnixMicro(), m.CommittedTZ.String()); err != nil {
			return fmt.Errorf("insert %s revision: %w", item.class, err)
		}
		if _, err := u.tx.ExecContext(ctx, `INSERT INTO resident_revision_activations(
			activation_id, canonical_commit_id, resident_id, revision_id, actor_principal_id, approval_id,
			reason_code, reason_content_id, recorded_at, recorded_tz
		) VALUES (?, ?, ?, ?, ?, NULL, 'bootstrap_finalize', NULL, ?, ?)`, item.activation.String(), m.CommitID.String(),
			value.ResidentID.String(), item.revision.String(), value.OwnerPrincipalID.String(), m.CommittedAt.UnixMicro(), m.CommittedTZ.String()); err != nil {
			return fmt.Errorf("activate %s revision: %w", item.class, err)
		}
	}
	if _, err := u.tx.ExecContext(ctx, `INSERT INTO resident_status_transitions(
		resident_status_transition_id, canonical_commit_id, resident_id, from_status, to_status,
		actor_principal_id, reason_code, reason_content_id, occurred_at, occurred_tz, recorded_at, recorded_tz
	) VALUES (?, ?, ?, 'draft', 'active', ?, 'bootstrap_finalize', NULL, ?, ?, ?, ?)`, value.StatusTransitionID.String(),
		m.CommitID.String(), value.ResidentID.String(), value.OwnerPrincipalID.String(), m.CommittedAt.UnixMicro(), m.CommittedTZ.String(),
		m.CommittedAt.UnixMicro(), m.CommittedTZ.String()); err != nil {
		return fmt.Errorf("activate resident: %w", err)
	}
	return nil
}

func (u *canonicalUoW) ArchiveResident(ctx context.Context, value domain.ArchiveResident) error {
	m := u.metadata
	if err := u.requireResidentScope(value.ResidentID); err != nil {
		return err
	}
	if err := u.requireOwnerHuman(ctx, value.ResidentID, value.OwnerPrincipalID); err != nil {
		return err
	}
	status, err := u.currentStatus(ctx, value.ResidentID)
	if err != nil {
		return err
	}
	if status != "active" {
		return fmt.Errorf("sqlite: archive requires active status, got %s", status)
	}
	_, err = u.tx.ExecContext(ctx, `INSERT INTO resident_status_transitions(
		resident_status_transition_id, canonical_commit_id, resident_id, from_status, to_status,
		actor_principal_id, reason_code, reason_content_id, occurred_at, occurred_tz, recorded_at, recorded_tz
	) VALUES (?, ?, ?, 'active', 'archived', ?, 'admin_archive', NULL, ?, ?, ?, ?)`, value.StatusTransitionID.String(),
		m.CommitID.String(), value.ResidentID.String(), value.OwnerPrincipalID.String(), m.CommittedAt.UnixMicro(), m.CommittedTZ.String(),
		m.CommittedAt.UnixMicro(), m.CommittedTZ.String())
	if err != nil {
		return err
	}
	// Operational selection must never point at a Canonically archived
	// resident. This update shares the archive transaction, so a concurrent
	// selection either commits first and is cleared here, or observes archived
	// status and fails after this transaction commits.
	if _, err := u.tx.ExecContext(ctx, `UPDATE runtime_config
		SET active_resident_id = NULL, desired_sessionization_policy_version_id = NULL,
			updated_at = ?, updated_tz = ?
		WHERE singleton_id = 1 AND active_resident_id = ?`, m.CommittedAt.UnixMicro(),
		m.CommittedTZ.String(), value.ResidentID.String()); err != nil {
		return fmt.Errorf("sqlite: clear archived resident selection: %w", err)
	}
	return nil
}

func (u *canonicalUoW) insertContent(ctx context.Context, content domain.Content) error {
	if err := content.Validate(); err != nil {
		return err
	}
	m := u.metadata
	if err := u.requireResidentScope(content.ResidentID); err != nil {
		return err
	}
	if _, err := u.tx.ExecContext(ctx, `INSERT OR IGNORE INTO blobs(
		dedupe_scope_id, hash_algorithm, blob_hash, content, byte_size, encoding, compression, created_at, created_tz
	) VALUES (?, 'sha256', ?, ?, ?, 'utf-8', 'none', ?, ?)`, content.ResidentID.String(), content.BlobHash.Bytes(),
		content.Bytes, len(content.Bytes), m.CommittedAt.UnixMicro(), m.CommittedTZ.String()); err != nil {
		return fmt.Errorf("insert blob: %w", err)
	}
	if _, err := u.tx.ExecContext(ctx, `INSERT INTO content_objects(
		content_id, owner_resident_id, content_class, blob_hash, blob_hash_algorithm, commitment,
		commitment_salt, commitment_hash_algorithm, commitment_domain, canonicalization_version,
		erasure_state, erasure_policy, created_at, created_tz
	) VALUES (?, ?, ?, ?, 'sha256', ?, ?, 'sha256', ?, ?, 'present', ?, ?, ?)`, content.ID.String(),
		content.ResidentID.String(), content.Class, content.BlobHash.Bytes(), content.Commitment.Bytes(), content.CommitmentSalt.Bytes(),
		canonical.ContentCommitmentDomain, canonical.CanonicalizationVersion, content.ErasurePolicy,
		m.CommittedAt.UnixMicro(), m.CommittedTZ.String()); err != nil {
		return fmt.Errorf("insert content object: %w", err)
	}
	return nil
}

func (u *canonicalUoW) requireResidentScope(residentID canonical.ID) error {
	scoped, ok := u.metadata.Scope.ResidentID()
	if !ok || scoped != residentID {
		return errors.New("sqlite: canonical UoW resident scope mismatch")
	}
	return nil
}

func (u *canonicalUoW) requireHuman(ctx context.Context, principalID canonical.ID) error {
	var kind string
	if err := u.tx.QueryRowContext(ctx, `SELECT kind FROM principals WHERE principal_id = ?`, principalID.String()).Scan(&kind); err != nil {
		return fmt.Errorf("resolve actor principal: %w", err)
	}
	if kind != "human" {
		return errors.New("sqlite: owner human principal required")
	}
	return nil
}

func (u *canonicalUoW) requireOwnerHuman(ctx context.Context, residentID, principalID canonical.ID) error {
	if err := u.requireHuman(ctx, principalID); err != nil {
		return err
	}
	var owner string
	if err := u.tx.QueryRowContext(ctx, `SELECT t.actor_principal_id
		FROM resident_status_transitions t
		JOIN canonical_commits c ON c.canonical_commit_id = t.canonical_commit_id
		WHERE t.resident_id = ? AND t.from_status IS NULL AND t.to_status = 'draft'
		ORDER BY c.commit_seq ASC LIMIT 1`, residentID.String()).Scan(&owner); err != nil {
		return fmt.Errorf("resolve resident owner: %w", err)
	}
	if owner != principalID.String() {
		return errors.New("sqlite: resident owner human principal required")
	}
	return nil
}

func (u *canonicalUoW) bootstrapPrinciplesActivations(ctx context.Context, value domain.ApprovePrinciples) (matching, total int, resultErr error) {
	rows, err := u.tx.QueryContext(ctx, `SELECT a.revision_id, a.actor_principal_id, a.reason_code,
		p.approver_principal_id, p.decision
		FROM resident_revision_activations a
		JOIN resident_revisions r ON r.revision_id = a.revision_id
		LEFT JOIN resident_revision_approvals p ON p.approval_id = a.approval_id
		WHERE a.resident_id = ? AND r.revision_class = 'principles'`, value.ResidentID.String())
	if err != nil {
		return 0, 0, fmt.Errorf("check principles activations: %w", err)
	}
	defer func() { resultErr = errors.Join(resultErr, rows.Close()) }()
	for rows.Next() {
		var revision, actor, reason string
		var approver, decision sql.NullString
		if err := rows.Scan(&revision, &actor, &reason, &approver, &decision); err != nil {
			return 0, 0, fmt.Errorf("scan principles activation: %w", err)
		}
		total++
		if revision == value.RevisionID.String() && actor == value.OwnerPrincipalID.String() &&
			reason == "bootstrap_approved" && approver.String == value.OwnerPrincipalID.String() &&
			decision.String == "approved" {
			matching++
		}
	}
	if err := rows.Err(); err != nil {
		return 0, 0, fmt.Errorf("iterate principles activations: %w", err)
	}
	return matching, total, nil
}

func (u *canonicalUoW) bootstrapFinalizationMatches(ctx context.Context, value domain.FinalizeResident) (bool, error) {
	var actor, reason string
	err := u.tx.QueryRowContext(ctx, `SELECT t.actor_principal_id, t.reason_code
		FROM resident_status_transitions t
		JOIN canonical_commits c ON c.canonical_commit_id = t.canonical_commit_id
		WHERE t.resident_id = ? AND t.from_status = 'draft' AND t.to_status = 'active'
		ORDER BY c.commit_seq DESC LIMIT 1`, value.ResidentID.String()).Scan(&actor, &reason)
	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("resolve bootstrap activation transition: %w", err)
	}
	if actor != value.OwnerPrincipalID.String() || reason != "bootstrap_finalize" {
		return false, nil
	}

	for _, item := range []struct {
		class   string
		content domain.Content
	}{
		{"persona", value.PersonaContent},
		{"memory_policy", value.MemoryContent},
	} {
		var revisionActor, revisionReason string
		var content []byte
		err := u.tx.QueryRowContext(ctx, `SELECT a.actor_principal_id, a.reason_code, b.content
			FROM resident_revision_activations a
			JOIN canonical_commits c ON c.canonical_commit_id = a.canonical_commit_id
			JOIN resident_revisions r ON r.revision_id = a.revision_id
			JOIN content_objects co ON co.content_id = r.content_id
			LEFT JOIN blobs b ON b.dedupe_scope_id = co.owner_resident_id
			 AND b.hash_algorithm = co.blob_hash_algorithm AND b.blob_hash = co.blob_hash
			WHERE a.resident_id = ? AND r.revision_class = ?
			ORDER BY c.commit_seq DESC LIMIT 1`, value.ResidentID.String(), item.class).Scan(&revisionActor, &revisionReason, &content)
		if errors.Is(err, sql.ErrNoRows) {
			return false, nil
		}
		if err != nil {
			return false, fmt.Errorf("resolve active %s bootstrap revision: %w", item.class, err)
		}
		if revisionActor != value.OwnerPrincipalID.String() || revisionReason != "bootstrap_finalize" ||
			content == nil || !sameBootstrapContent(item.class, content, item.content.Bytes) {
			return false, nil
		}
	}
	return true, nil
}

func sameBootstrapContent(class string, durable, candidate []byte) bool {
	if class != "memory_policy" {
		return bytes.Equal(durable, candidate)
	}
	durableJSON, durableErr := canonical.CanonicalizeRFC8785(durable)
	candidateJSON, candidateErr := canonical.CanonicalizeRFC8785(candidate)
	return durableErr == nil && candidateErr == nil && bytes.Equal(durableJSON.Bytes(), candidateJSON.Bytes())
}

func (u *canonicalUoW) currentStatus(ctx context.Context, residentID canonical.ID) (string, error) {
	var status string
	err := u.tx.QueryRowContext(ctx, `SELECT t.to_status
		FROM resident_status_transitions t
		JOIN canonical_commits c ON c.canonical_commit_id = t.canonical_commit_id
		WHERE t.resident_id = ? ORDER BY c.commit_seq DESC LIMIT 1`, residentID.String()).Scan(&status)
	if err != nil {
		return "", fmt.Errorf("resolve resident status: %w", err)
	}
	return status, nil
}

var _ canonical.Backend = (*CanonicalRepository)(nil)
var _ domain.MutationStore = (*canonicalUoW)(nil)
