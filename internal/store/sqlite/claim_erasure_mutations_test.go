package sqlite

import (
	"context"
	"database/sql"
	"errors"
	"strings"
	"testing"

	"mahoroba.local/mahoroba/internal/canonical"
	"mahoroba.local/mahoroba/internal/domain"
)

type m7ClaimErasureEvidence struct {
	Metadata canonical.CommitMetadata
	Value    domain.ClaimStatementErasureResult
	ClaimID  string
	Content  string
	DB       *sql.DB
}

func performM7ClaimErasure(t *testing.T) m7ClaimErasureEvidence {
	t.Helper()
	fixture, closeFixture := newSemanticFixture(t)
	t.Cleanup(closeFixture)
	makeSemanticFixtureMinimumValid(t, fixture)
	insertResolvableDialogueCancellationHistory(t, fixture, "A")
	claimID, err := canonical.ParseID(fixture.claim["A"])
	if err != nil {
		t.Fatal(err)
	}
	residentID, err := canonical.ParseID(fixture.resident["A"])
	if err != nil {
		t.Fatal(err)
	}
	actorID, err := canonical.ParseID(fixture.principal["human"])
	if err != nil {
		t.Fatal(err)
	}
	contentID, err := canonical.ParseID(fixture.content["A"]["claim"])
	if err != nil {
		t.Fatal(err)
	}
	commitID, err := canonical.ParseID(fixture.ids.new())
	if err != nil {
		t.Fatal(err)
	}
	contentEventID, err := canonical.ParseID(fixture.ids.new())
	if err != nil {
		t.Fatal(err)
	}
	claimEventID, err := canonical.ParseID(fixture.ids.new())
	if err != nil {
		t.Fatal(err)
	}
	commitSeq, err := canonical.NewCommitSeq(4)
	if err != nil {
		t.Fatal(err)
	}
	scope, err := canonical.ResidentScope(residentID)
	if err != nil {
		t.Fatal(err)
	}
	metadata := canonical.CommitMetadata{
		CommitID: commitID, CommitSeq: commitSeq, Scope: scope,
		CommittedAt: canonical.Instant(semanticTime + 100),
		CommittedTZ: canonical.MustTimezone(semanticTZ),
	}
	tx, err := fixture.db.BeginTx(context.Background(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	if _, err := tx.Exec(`INSERT INTO canonical_commits(
		canonical_commit_id, commit_seq, resident_id, committed_at, committed_tz
	) VALUES (?, ?, ?, ?, ?)`, commitID.String(), commitSeq.Int64(), residentID.String(),
		metadata.CommittedAt.UnixMicro(), metadata.CommittedTZ.String()); err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec(`INSERT INTO resident_status_transitions(
		resident_status_transition_id, canonical_commit_id, resident_id, from_status, to_status,
		actor_principal_id, reason_code, reason_content_id, occurred_at, occurred_tz, recorded_at, recorded_tz
	) VALUES (?, ?, ?, NULL, 'draft', ?, 'fixture_owner', NULL, ?, ?, ?, ?)`,
		fixture.ids.new(), fixture.commit["A"], residentID.String(), fixture.principal["human"],
		semanticTime, semanticTZ, semanticTime, semanticTZ); err != nil {
		t.Fatal(err)
	}
	uow := &canonicalUoW{tx: tx, metadata: metadata}
	result, err := uow.EraseClaimStatement(context.Background(), domain.EraseClaimStatement{
		ResidentID: residentID, ClaimID: claimID,
		ClaimStatementErasureEventID: claimEventID,
		ContentErasureEventID:        contentEventID,
		ActorPrincipalID:             actorID,
		ReasonCode:                   "m7_test",
		OccurredAt:                   canonical.Instant(semanticTime + 7),
		OccurredTZ:                   canonical.MustTimezone(semanticTZ),
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	var hash []byte
	var state string
	if err := fixture.db.QueryRow(`SELECT statement_hash, content.erasure_state
		FROM claims claim JOIN content_objects content
		  ON content.content_id = claim.statement_content_id
		WHERE claim.claim_id = ?`, claimID.String()).Scan(&hash, &state); err != nil {
		t.Fatal(err)
	}
	if hash != nil || state != "erased" {
		t.Fatalf("erasure state hash=%x state=%s", hash, state)
	}
	return m7ClaimErasureEvidence{Metadata: metadata, Value: result, ClaimID: claimID.String(), Content: contentID.String(), DB: fixture.db}
}

func TestM7I1ClaimIdentityErasureUsesPairedCanonicalEvent(t *testing.T) {
	evidence := performM7ClaimErasure(t)
	if evidence.Value.ClaimErasureEventID.IsZero() || evidence.Value.ContentErasureEventID.IsZero() {
		t.Fatalf("missing erasure event IDs: %+v", evidence.Value)
	}
	t.Run("shared_alias_batch", func(t *testing.T) {
		batch := performM7BatchAliasErasure(t, nil)
		var aliases, pairs int
		if err := batch.db.QueryRow(`SELECT COUNT(*),(SELECT COUNT(*) FROM claim_statement_erasure_events e JOIN claims c ON c.claim_id=e.claim_id WHERE c.statement_content_id=?) FROM claims WHERE statement_content_id=?`, batch.contentID, batch.contentID).Scan(&aliases, &pairs); err != nil {
			t.Fatal(err)
		}
		if aliases != 2 || pairs != aliases {
			t.Fatalf("aliases/pairs=%d/%d", aliases, pairs)
		}
	})
}

func TestM7I2ContentErasureIsTheOnlyWriteOnceContentMutation(t *testing.T) {
	evidence := performM7ClaimErasure(t)
	var commitment, salt []byte
	if err := evidence.DB.QueryRow("SELECT commitment, commitment_salt FROM content_objects WHERE content_id = ?", evidence.Content).Scan(&commitment, &salt); err != nil {
		t.Fatal(err)
	}
	if len(commitment) != 32 || salt != nil {
		t.Fatalf("commitment/salt after erasure = %x/%x", commitment, salt)
	}
	t.Run("shared_alias_batch", func(t *testing.T) {
		batch := performM7BatchAliasErasure(t, nil)
		var events int
		if err := batch.db.QueryRow(`SELECT COUNT(*) FROM content_erasure_events WHERE content_id=?`, batch.contentID).Scan(&events); err != nil {
			t.Fatal(err)
		}
		if events != 1 {
			t.Fatalf("content events=%d", events)
		}
	})
}

func TestM7I7ClaimStatementCannotBeReplacedOrReusedAfterErase(t *testing.T) {
	evidence := performM7ClaimErasure(t)
	var count int
	if err := evidence.DB.QueryRow(`SELECT count(*) FROM claims WHERE statement_hash IS NULL AND claim_id = ?`, evidence.ClaimID).Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 1 {
		t.Fatalf("erased claim count = %d", count)
	}
	t.Run("shared_alias_batch", func(t *testing.T) {
		batch := performM7BatchAliasErasure(t, nil)
		var present int
		if err := batch.db.QueryRow(`SELECT COUNT(statement_hash) FROM claims WHERE statement_content_id=?`, batch.contentID).Scan(&present); err != nil {
			t.Fatal(err)
		}
		if present != 0 {
			t.Fatalf("present alias hashes=%d", present)
		}
		if _, err := batch.db.Exec(`INSERT INTO claims SELECT ?,canonical_commit_id,owner_resident_id,subject_principal_id,perspective_principal_id,kind,temporal_kind,statement_content_id,?,'sha256',statement_normalization_version,created_by_run_id,recorded_at,recorded_tz FROM claims WHERE claim_id=?`, "01J00000000000000000000ZZZ", make([]byte, 32), batch.claimIDs[0]); err == nil {
			t.Fatal("reused erased statement content")
		}
	})
}

func TestM7I64ClaimIdentityErasureBelongsToOneCanonicalCommit(t *testing.T) {
	evidence := performM7ClaimErasure(t)
	var eventCommit, contentEventCommit string
	if err := evidence.DB.QueryRow(`SELECT claim_event.canonical_commit_id, content_event.canonical_commit_id
		FROM claim_statement_erasure_events claim_event
		JOIN content_erasure_events content_event
		  ON content_event.content_erasure_event_id = claim_event.content_erasure_event_id
		WHERE claim_event.claim_id = ?`, evidence.ClaimID).Scan(&eventCommit, &contentEventCommit); err != nil {
		t.Fatal(err)
	}
	if eventCommit == "" || eventCommit != evidence.Metadata.CommitID.String() || contentEventCommit != eventCommit {
		t.Fatalf("erasure event commits = %s/%s, want %s", eventCommit, contentEventCommit, evidence.Metadata.CommitID)
	}
	t.Run("shared_alias_batch", func(t *testing.T) {
		batch := performM7BatchAliasErasure(t, nil)
		rows, err := batch.db.Query(`SELECT DISTINCT e.canonical_commit_id FROM claim_statement_erasure_events e JOIN claims c ON c.claim_id=e.claim_id WHERE c.statement_content_id=?`, batch.contentID)
		if err != nil {
			t.Fatal(err)
		}
		defer rows.Close()
		commits := []string{}
		for rows.Next() {
			var id string
			if err := rows.Scan(&id); err != nil {
				t.Fatal(err)
			}
			commits = append(commits, id)
		}
		if len(commits) != 1 || commits[0] != batch.commitID {
			t.Fatalf("pair commits=%v want %s", commits, batch.commitID)
		}
	})
}

func TestM7I65ClaimIdentityErasureUsesSingleLedgerTime(t *testing.T) {
	evidence := performM7ClaimErasure(t)
	var contentAt, claimAt int64
	if err := evidence.DB.QueryRow(`SELECT content_event.recorded_at, claim_event.recorded_at
		FROM content_erasure_events content_event
		JOIN claim_statement_erasure_events claim_event
		  ON claim_event.content_erasure_event_id = content_event.content_erasure_event_id
		WHERE claim_event.claim_id = ?`, evidence.ClaimID).Scan(&contentAt, &claimAt); err != nil {
		t.Fatal(err)
	}
	if contentAt != evidence.Metadata.CommittedAt.UnixMicro() || claimAt != contentAt {
		t.Fatalf("erasure ledger times = %d/%d, want %d", contentAt, claimAt, evidence.Metadata.CommittedAt.UnixMicro())
	}
	t.Run("shared_alias_batch", func(t *testing.T) {
		batch := performM7BatchAliasErasure(t, nil)
		rows, err := batch.db.Query(`SELECT recorded_at FROM content_erasure_events WHERE content_id=? UNION SELECT e.recorded_at FROM claim_statement_erasure_events e JOIN claims c ON c.claim_id=e.claim_id WHERE c.statement_content_id=?`, batch.contentID, batch.contentID)
		if err != nil {
			t.Fatal(err)
		}
		defer rows.Close()
		for rows.Next() {
			var at int64
			if err := rows.Scan(&at); err != nil {
				t.Fatal(err)
			}
			if at != batch.commitTime {
				t.Fatalf("ledger time=%d want %d", at, batch.commitTime)
			}
		}
	})
}

func TestM7ClaimStatementErasureRequiresResidentOwnerHuman(t *testing.T) {
	for _, actorKey := range []string{"human2", "system", "A"} {
		t.Run(actorKey, func(t *testing.T) {
			fixture, closeFixture := newSemanticFixture(t)
			defer closeFixture()
			residentID, _ := canonical.ParseID(fixture.resident["A"])
			claimID, _ := canonical.ParseID(fixture.claim["A"])
			actorID, _ := canonical.ParseID(fixture.principal[actorKey])
			commitID, _ := canonical.ParseID(fixture.ids.new())
			contentEventID, _ := canonical.ParseID(fixture.ids.new())
			claimEventID, _ := canonical.ParseID(fixture.ids.new())
			commitSeq, _ := canonical.NewCommitSeq(200)
			metadata := canonical.CommitMetadata{
				CommitID: commitID, CommitSeq: commitSeq,
				Scope:       mustResidentScope(t, residentID),
				CommittedAt: canonical.Instant(semanticTime + 200), CommittedTZ: canonical.MustTimezone(semanticTZ),
			}
			tx, err := fixture.db.BeginTx(context.Background(), nil)
			if err != nil {
				t.Fatal(err)
			}
			defer tx.Rollback()
			if _, err := tx.Exec(`INSERT INTO canonical_commits(canonical_commit_id, commit_seq, resident_id, committed_at, committed_tz) VALUES (?, ?, ?, ?, ?)`,
				commitID.String(), commitSeq.Int64(), residentID.String(), metadata.CommittedAt.UnixMicro(), metadata.CommittedTZ.String()); err != nil {
				t.Fatal(err)
			}
			if _, err := tx.Exec(`INSERT INTO resident_status_transitions(
				resident_status_transition_id, canonical_commit_id, resident_id, from_status, to_status,
				actor_principal_id, reason_code, reason_content_id, occurred_at, occurred_tz, recorded_at, recorded_tz
			) VALUES (?, ?, ?, NULL, 'draft', ?, 'fixture_owner', NULL, ?, ?, ?, ?)`,
				fixture.ids.new(), fixture.commit["A"], residentID.String(), fixture.principal["human"],
				semanticTime, semanticTZ, semanticTime, semanticTZ); err != nil {
				t.Fatal(err)
			}
			uow := &canonicalUoW{tx: tx, metadata: metadata}
			_, err = uow.EraseClaimStatement(context.Background(), domain.EraseClaimStatement{
				ResidentID: residentID, ClaimID: claimID,
				ClaimStatementErasureEventID: claimEventID, ContentErasureEventID: contentEventID,
				ActorPrincipalID: actorID, ReasonCode: "m7_auth_test",
				OccurredAt: canonical.Instant(semanticTime + 200), OccurredTZ: canonical.MustTimezone(semanticTZ),
			})
			if err == nil || (!strings.Contains(err.Error(), "resident owner human principal required") && !strings.Contains(err.Error(), "owner human principal required")) {
				t.Fatalf("actor %s erasure error = %v", actorKey, err)
			}
			var claimCount, contentCount int
			if err := tx.QueryRow("SELECT COUNT(*) FROM claims WHERE statement_hash IS NOT NULL").Scan(&claimCount); err != nil {
				t.Fatal(err)
			}
			if err := tx.QueryRow("SELECT COUNT(*) FROM content_objects WHERE erasure_state = 'present'").Scan(&contentCount); err != nil {
				t.Fatal(err)
			}
			if claimCount == 0 || contentCount == 0 {
				t.Fatal("authorization failure changed canonical content")
			}
		})
	}
}

type m7ClaimErasureRollbackState struct {
	commitCount             int
	commitHead              int64
	contentState            string
	blobHash                string
	commitmentSalt          string
	commitment              string
	claimAHash              string
	claimBHash              string
	claimReferences         int
	presentClaimIdentities  int
	contentErasureEventRows int
	claimErasureEventRows   int
}

func TestM7EraseClaimStatementRejectsSharedStatementContentAndRollsBack(t *testing.T) {
	for _, testCase := range []struct {
		name          string
		selectedClaim string
		mixedHashes   bool
	}{
		{name: "select_original_alias", selectedClaim: "A"},
		{name: "select_second_alias", selectedClaim: "B"},
		{name: "reject_mixed_hash_alias_group", selectedClaim: "A", mixedHashes: true},
		{name: "reject_mixed_hash_alias_group_from_erased_identity", selectedClaim: "B", mixedHashes: true},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			fixture, closeFixture := newSemanticFixture(t)
			defer closeFixture()

			claimA := fixture.claim["A"]
			claimB := addM7SharedClaimAlias(t, fixture, claimA)
			addM7ResidentOwnerTransition(t, fixture)
			if testCase.mixedHashes {
				// Construct a deliberately malformed restore/import-style fixture.
				// Production SQL cannot create this state because the update
				// contract requires a paired erasure event.
				mustExec(t, fixture.db, "DROP TRIGGER trg_claims_update_contract")
				mustExec(t, fixture.db, "UPDATE claims SET statement_hash = NULL WHERE claim_id = ?", claimB)
			}

			contentID := fixture.content["A"]["claim"]
			before := captureM7ClaimErasureRollbackState(t, fixture.db, contentID, claimA, claimB)
			selected := claimA
			if testCase.selectedClaim == "B" {
				selected = claimB
			}

			err := attemptM7SingleClaimErasure(t, fixture, selected, contentID, claimA, claimB, before)
			if err == nil || !errors.Is(err, domain.ErrClaimStatementErasureRequiresBatch) {
				t.Fatalf("shared statement erasure error = %v, want ErrClaimStatementErasureRequiresBatch", err)
			}

			after := captureM7ClaimErasureRollbackState(t, fixture.db, contentID, claimA, claimB)
			if after != before {
				t.Fatalf("failed shared statement erasure changed Canonical state\nbefore: %+v\nafter:  %+v", before, after)
			}
		})
	}
}

func addM7SharedClaimAlias(t *testing.T, fixture *semanticFixture, sourceClaimID string) string {
	t.Helper()
	aliasID := fixture.ids.new()
	result, err := fixture.db.Exec(`INSERT INTO claims(
		claim_id, canonical_commit_id, owner_resident_id, subject_principal_id,
		perspective_principal_id, kind, temporal_kind, statement_content_id,
		statement_hash, statement_hash_algorithm, statement_normalization_version,
		created_by_run_id, recorded_at, recorded_tz
	)
	SELECT ?, canonical_commit_id, owner_resident_id, subject_principal_id,
	       perspective_principal_id, kind, temporal_kind, statement_content_id,
	       statement_hash, statement_hash_algorithm, statement_normalization_version,
	       created_by_run_id, recorded_at, recorded_tz
	FROM claims WHERE claim_id = ?`, aliasID, sourceClaimID)
	if err != nil {
		t.Fatal(err)
	}
	if affected, err := result.RowsAffected(); err != nil || affected != 1 {
		t.Fatalf("shared claim alias rows affected = %d, err = %v", affected, err)
	}
	return aliasID
}

func addM7ResidentOwnerTransition(t *testing.T, fixture *semanticFixture) {
	t.Helper()
	mustExec(t, fixture.db, `INSERT INTO resident_status_transitions(
		resident_status_transition_id, canonical_commit_id, resident_id, from_status, to_status,
		actor_principal_id, reason_code, reason_content_id, occurred_at, occurred_tz, recorded_at, recorded_tz
	) VALUES (?, ?, ?, NULL, 'draft', ?, 'fixture_owner', NULL, ?, ?, ?, ?)`,
		fixture.ids.new(), fixture.commit["A"], fixture.resident["A"], fixture.principal["human"],
		semanticTime, semanticTZ, semanticTime, semanticTZ)
}

func attemptM7SingleClaimErasure(
	t *testing.T,
	fixture *semanticFixture,
	claimRaw, contentID, claimA, claimB string,
	before m7ClaimErasureRollbackState,
) error {
	t.Helper()
	residentID, err := canonical.ParseID(fixture.resident["A"])
	if err != nil {
		t.Fatal(err)
	}
	claimID, err := canonical.ParseID(claimRaw)
	if err != nil {
		t.Fatal(err)
	}
	actorID, err := canonical.ParseID(fixture.principal["human"])
	if err != nil {
		t.Fatal(err)
	}
	commitID, err := canonical.ParseID(fixture.ids.new())
	if err != nil {
		t.Fatal(err)
	}
	contentEventID, err := canonical.ParseID(fixture.ids.new())
	if err != nil {
		t.Fatal(err)
	}
	claimEventID, err := canonical.ParseID(fixture.ids.new())
	if err != nil {
		t.Fatal(err)
	}
	commitSeq, err := canonical.NewCommitSeq(300)
	if err != nil {
		t.Fatal(err)
	}
	metadata := canonical.CommitMetadata{
		CommitID: commitID, CommitSeq: commitSeq,
		Scope:       mustResidentScope(t, residentID),
		CommittedAt: canonical.Instant(semanticTime + 300),
		CommittedTZ: canonical.MustTimezone(semanticTZ),
	}
	tx, err := fixture.db.BeginTx(context.Background(), nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec(`INSERT INTO canonical_commits(
		canonical_commit_id, commit_seq, resident_id, committed_at, committed_tz
	) VALUES (?, ?, ?, ?, ?)`, commitID.String(), commitSeq.Int64(), residentID.String(),
		metadata.CommittedAt.UnixMicro(), metadata.CommittedTZ.String()); err != nil {
		_ = tx.Rollback()
		t.Fatal(err)
	}
	uow := &canonicalUoW{tx: tx, metadata: metadata}
	_, erasureErr := uow.EraseClaimStatement(context.Background(), domain.EraseClaimStatement{
		ResidentID: residentID, ClaimID: claimID,
		ClaimStatementErasureEventID: claimEventID,
		ContentErasureEventID:        contentEventID,
		ActorPrincipalID:             actorID,
		ReasonCode:                   "m7_shared_alias_test",
		OccurredAt:                   canonical.Instant(semanticTime + 300),
		OccurredTZ:                   canonical.MustTimezone(semanticTZ),
	})
	withinTransaction := captureM7ClaimErasureRollbackState(t, tx, contentID, claimA, claimB)
	expectedWithinTransaction := before
	expectedWithinTransaction.commitCount++
	expectedWithinTransaction.commitHead = commitSeq.Int64()
	if withinTransaction != expectedWithinTransaction {
		_ = tx.Rollback()
		t.Fatalf("shared statement guard mutated its open transaction\nwant: %+v\ngot:  %+v", expectedWithinTransaction, withinTransaction)
	}
	if err := tx.Rollback(); err != nil {
		t.Fatal(err)
	}
	return erasureErr
}

type m7ClaimErasureQueryer interface {
	QueryRow(query string, args ...any) *sql.Row
}

func captureM7ClaimErasureRollbackState(t *testing.T, db m7ClaimErasureQueryer, contentID, claimA, claimB string) m7ClaimErasureRollbackState {
	t.Helper()
	var state m7ClaimErasureRollbackState
	if err := db.QueryRow(`SELECT COUNT(*), COALESCE(MAX(commit_seq), 0) FROM canonical_commits`).Scan(
		&state.commitCount, &state.commitHead,
	); err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRow(`SELECT erasure_state, hex(blob_hash), hex(commitment_salt), hex(commitment)
		FROM content_objects WHERE content_id = ?`, contentID).Scan(
		&state.contentState, &state.blobHash, &state.commitmentSalt, &state.commitment,
	); err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRow(`SELECT hex(statement_hash) FROM claims WHERE claim_id = ?`, claimA).Scan(&state.claimAHash); err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRow(`SELECT hex(statement_hash) FROM claims WHERE claim_id = ?`, claimB).Scan(&state.claimBHash); err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRow(`SELECT COUNT(*), COUNT(statement_hash) FROM claims WHERE statement_content_id = ?`, contentID).Scan(
		&state.claimReferences, &state.presentClaimIdentities,
	); err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRow(`SELECT COUNT(*) FROM content_erasure_events`).Scan(&state.contentErasureEventRows); err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRow(`SELECT COUNT(*) FROM claim_statement_erasure_events`).Scan(&state.claimErasureEventRows); err != nil {
		t.Fatal(err)
	}
	return state
}

func mustResidentScope(t *testing.T, residentID canonical.ID) canonical.Scope {
	t.Helper()
	scope, err := canonical.ResidentScope(residentID)
	if err != nil {
		t.Fatal(err)
	}
	return scope
}
