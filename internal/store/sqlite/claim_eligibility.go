package sqlite

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"fmt"

	"mahoroba.local/mahoroba/internal/canonical"
	"mahoroba.local/mahoroba/internal/domain"
	"mahoroba.local/mahoroba/internal/memory"
)

var errClaimContentIntegrity = errors.New("sqlite: eligible claim content integrity failure")

type claimQueryer interface {
	QueryRowContext(context.Context, string, ...any) *sql.Row
}

type eligibleClaimStatement struct {
	ClaimID          canonical.ID
	ResidentID       canonical.ID
	SubjectID        canonical.ID
	PerspectiveID    canonical.ID
	Kind             memory.ClaimKind
	TemporalKind     memory.TemporalKind
	TemporalRelation memory.TemporalRelation
	Statement        []byte
}

// loadEligibleClaimStatement is the only semantic claim-content loader. The
// owner and both erasure identity columns are checked in the same SELECT so a
// malformed or cross-resident row cannot be repaired by choosing another
// claim. A row that passes the dual guard but has missing or mismatched bytes
// is an integrity error, not an ineligible-source result.
func loadEligibleClaimStatement(
	ctx context.Context,
	q claimQueryer,
	residentID canonical.ID,
	claimID canonical.ID,
) (eligibleClaimStatement, error) {
	var residentRaw, subjectRaw, perspectiveRaw, kindRaw, temporalKindRaw, temporalRelationRaw string
	var statementHashAlgorithm, statementNormalizationVersion string
	var erasureState, contentOwnerRaw, blobHashAlgorithm sql.NullString
	var statementHash, blobHash, storedBlobHash, statement []byte
	var blobByteSize sql.NullInt64
	if err := q.QueryRowContext(ctx, `SELECT claim.owner_resident_id,
		claim.subject_principal_id, claim.perspective_principal_id, COALESCE(claim.kind, ''),
		claim.temporal_kind, COALESCE(state.temporal_relation, 'current'), content.erasure_state,
		claim.statement_hash, claim.statement_hash_algorithm, claim.statement_normalization_version,
		content.owner_resident_id, content.blob_hash_algorithm, content.blob_hash,
		blob.blob_hash, blob.byte_size, blob.content
		FROM claims claim
		LEFT JOIN content_objects content ON content.content_id = claim.statement_content_id
		LEFT JOIN claim_states state ON state.claim_id = claim.claim_id AND state.resident_id = claim.owner_resident_id
		LEFT JOIN blobs blob ON blob.dedupe_scope_id = content.owner_resident_id
		 AND blob.hash_algorithm = content.blob_hash_algorithm
		 AND blob.blob_hash = content.blob_hash
		WHERE claim.claim_id = ? AND claim.owner_resident_id = ?`, claimID.String(), residentID.String()).
		Scan(&residentRaw, &subjectRaw, &perspectiveRaw, &kindRaw, &temporalKindRaw, &temporalRelationRaw,
			&erasureState, &statementHash, &statementHashAlgorithm, &statementNormalizationVersion,
			&contentOwnerRaw, &blobHashAlgorithm, &blobHash, &storedBlobHash, &blobByteSize, &statement); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return eligibleClaimStatement{}, fmt.Errorf("%w: claim %s is missing, cross-resident, erased, or has a null statement hash", domain.ErrClaimSourceIneligible, claimID)
		}
		return eligibleClaimStatement{}, fmt.Errorf("sqlite: load eligible claim %s: %w", claimID, err)
	}
	// The claim-side null identity remains the first erasure guard even if the
	// referenced content row is also corrupt or absent. A non-null identity with
	// an erased content state is the other fail-closed half-state.
	if residentRaw != residentID.String() || statementHash == nil ||
		(erasureState.Valid && erasureState.String != "present") {
		return eligibleClaimStatement{}, fmt.Errorf("%w: claim %s is missing, cross-resident, erased, or has a null statement hash", domain.ErrClaimSourceIneligible, claimID)
	}
	if !erasureState.Valid || !contentOwnerRaw.Valid || !blobHashAlgorithm.Valid {
		return eligibleClaimStatement{}, fmt.Errorf("%w: claim %s statement content row is missing", errClaimContentIntegrity, claimID)
	}
	if contentOwnerRaw.String != residentID.String() {
		return eligibleClaimStatement{}, fmt.Errorf("%w: claim %s content owner does not match its resident", errClaimContentIntegrity, claimID)
	}
	if blobHashAlgorithm.String != canonical.HashAlgorithm {
		return eligibleClaimStatement{}, fmt.Errorf("%w: claim %s blob hash algorithm is %q", errClaimContentIntegrity, claimID, blobHashAlgorithm.String)
	}
	if storedBlobHash == nil || !blobByteSize.Valid || statement == nil {
		return eligibleClaimStatement{}, fmt.Errorf("%w: claim %s blob is missing", errClaimContentIntegrity, claimID)
	}
	if !bytes.Equal(storedBlobHash, blobHash) {
		return eligibleClaimStatement{}, fmt.Errorf("%w: claim %s blob identity does not match its content object", errClaimContentIntegrity, claimID)
	}
	if blobByteSize.Int64 != int64(len(statement)) {
		return eligibleClaimStatement{}, fmt.Errorf("%w: claim %s blob byte size does not match its content", errClaimContentIntegrity, claimID)
	}
	if actual := canonical.HashBlob(statement); !bytes.Equal(actual.Bytes(), blobHash) {
		return eligibleClaimStatement{}, fmt.Errorf("%w: claim %s blob digest does not match its content", errClaimContentIntegrity, claimID)
	}
	if statementHashAlgorithm != canonical.HashAlgorithm {
		return eligibleClaimStatement{}, fmt.Errorf("%w: claim %s statement hash algorithm is %q", errClaimContentIntegrity, claimID, statementHashAlgorithm)
	}
	if statementNormalizationVersion != memory.NormalizationVersionV1 {
		return eligibleClaimStatement{}, fmt.Errorf("%w: claim %s statement normalization version is %q", errClaimContentIntegrity, claimID, statementNormalizationVersion)
	}
	normalized, err := memory.NormalizeStatementV1(string(statement))
	if err != nil || normalized == "" {
		return eligibleClaimStatement{}, fmt.Errorf("%w: normalize claim %s statement: %v", errClaimContentIntegrity, claimID, err)
	}
	if actual := canonical.HashBlob([]byte(normalized)); !bytes.Equal(actual.Bytes(), statementHash) {
		return eligibleClaimStatement{}, fmt.Errorf("%w: claim %s normalized statement digest does not match its identity", errClaimContentIntegrity, claimID)
	}
	subjectID, err := canonical.ParseID(subjectRaw)
	if err != nil {
		return eligibleClaimStatement{}, fmt.Errorf("%w: parse claim %s subject: %v", errClaimContentIntegrity, claimID, err)
	}
	perspectiveID, err := canonical.ParseID(perspectiveRaw)
	if err != nil {
		return eligibleClaimStatement{}, fmt.Errorf("%w: parse claim %s perspective: %v", errClaimContentIntegrity, claimID, err)
	}
	kind := memory.ClaimKindUnclassified
	if kindRaw != "" {
		kind = memory.ClaimKind(kindRaw)
	}
	if err := kind.Validate(); err != nil {
		return eligibleClaimStatement{}, fmt.Errorf("%w: parse claim %s kind: %v", errClaimContentIntegrity, claimID, err)
	}
	temporalKind := memory.TemporalKind(temporalKindRaw)
	if err := temporalKind.Validate(); err != nil {
		return eligibleClaimStatement{}, fmt.Errorf("%w: parse claim %s temporal kind: %v", errClaimContentIntegrity, claimID, err)
	}
	temporalRelation := memory.TemporalRelation(temporalRelationRaw)
	if err := temporalRelation.Validate(); err != nil {
		return eligibleClaimStatement{}, fmt.Errorf("%w: parse claim %s temporal relation: %v", errClaimContentIntegrity, claimID, err)
	}
	return eligibleClaimStatement{
		ClaimID: claimID, ResidentID: residentID, SubjectID: subjectID, PerspectiveID: perspectiveID,
		Kind: kind, TemporalKind: temporalKind, TemporalRelation: temporalRelation,
		Statement: append([]byte(nil), statement...),
	}, nil
}
