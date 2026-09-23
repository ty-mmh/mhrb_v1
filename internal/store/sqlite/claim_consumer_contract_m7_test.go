package sqlite

import (
	"context"
	"database/sql"
	"encoding/base64"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"mahoroba.local/mahoroba/internal/autonomy"
	"mahoroba.local/mahoroba/internal/canonical"
	"mahoroba.local/mahoroba/internal/domain"
	"mahoroba.local/mahoroba/internal/memory"
	"mahoroba.local/mahoroba/internal/projection"
	"mahoroba.local/mahoroba/internal/testsupport"
)

func TestM7ErasedClaimConsumerSourceInventory(t *testing.T) {
	_, file, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("runtime.Caller failed")
	}
	root := filepath.Clean(filepath.Join(filepath.Dir(file), "..", "..", ".."))
	cases := []struct {
		path     string
		section  string
		required []string
	}{
		{"internal/store/sqlite/memory_recall_reads.go", "loadRecall", []string{"content.erasure_state = 'present'", "claim.statement_hash IS NOT NULL"}},
		{"internal/store/sqlite/memory_persona_reads.go", "loadPersonaEligibleClaims", []string{"content.erasure_state = 'present'", "claim.statement_hash IS NOT NULL"}},
		{"internal/store/sqlite/memory_alignment_reads.go", "loadAlignmentClaims", []string{"content.erasure_state = 'present'", "claim.statement_hash IS NOT NULL"}},
		{"internal/store/sqlite/memory_derivation_reads.go", "PrepareMemoryDerivation", []string{"domain.ErrClaimSourceIneligible"}},
		{"internal/store/sqlite/autonomy_generation_reads.go", "relation_type = 'contradicts'", []string{"source_content.erasure_state = 'present'", "source.statement_hash IS NOT NULL"}},
		{"internal/store/sqlite/projection_adapter.go", "loadClaimSeeds", []string{"cl.claim_id", "cl.temporal_kind", "c.commit_seq", "cl.recorded_at"}},
		{"internal/store/sqlite/memory_admin_reads.go", "Statement = \"[erased]\"", []string{"Statement = \"[erased]\""}},
	}
	for _, testCase := range cases {
		body, err := os.ReadFile(filepath.Join(root, testCase.path))
		if err != nil {
			t.Fatal(err)
		}
		text := string(body)
		if !strings.Contains(text, testCase.section) {
			t.Fatalf("consumer matrix section %s missing from %s", testCase.section, testCase.path)
		}
		for _, required := range testCase.required {
			if !strings.Contains(text, required) {
				t.Fatalf("consumer %s missing contract %q", testCase.path, required)
			}
		}
	}
}

func TestM7ErasedClaimSemanticLoaderFailsClosedWithoutFallback(t *testing.T) {
	evidence := performM7ClaimErasure(t)
	var residentRaw string
	if err := evidence.DB.QueryRowContext(context.Background(),
		"SELECT owner_resident_id FROM claims WHERE claim_id = ?", evidence.ClaimID,
	).Scan(&residentRaw); err != nil {
		t.Fatal(err)
	}
	residentID, err := canonical.ParseID(residentRaw)
	if err != nil {
		t.Fatal(err)
	}
	claimID, err := canonical.ParseID(evidence.ClaimID)
	if err != nil {
		t.Fatal(err)
	}
	_, err = loadEligibleClaimStatement(context.Background(), evidence.DB, residentID, claimID)
	if err == nil || !errors.Is(err, domain.ErrClaimSourceIneligible) {
		t.Fatalf("erased claim loader error = %v, want ErrClaimSourceIneligible", err)
	}
	if errors.Is(err, errClaimContentIntegrity) {
		t.Fatalf("erased claim was classified as content integrity failure: %v", err)
	}
}

func TestM7ErasedClaimAutomaticConsumersFilterBeforeLimit(t *testing.T) {
	t.Run("recall", func(t *testing.T) {
		fixture := newMemoryClaimMutationFixture(t)
		eventID := fixture.seedEvent(t, memory.EventUserMessage, fixture.ownerPrincipal, memory.TrustTrusted)
		seed := []seededClaimEvidence{trustedEvidence(
			eventID, memory.EventUserMessage, fixture.ownerPrincipal, memory.PolaritySupport,
		)}
		erasedID := claimMutationParseID(t, fixture.semantic.claim["A"])
		presentIDs := make(map[canonical.ID]struct{}, domain.MaxRecallCandidates)
		for range domain.MaxRecallCandidates {
			claimID, _ := fixture.seedClaim(t, memory.ClaimKindOther, fixture.ownerPrincipal,
				fixture.residentPrincipal, seed, memory.StageFloating)
			presentIDs[claimID] = struct{}{}
		}
		addProjection := func(claimID canonical.ID, salience float64) {
			t.Helper()
			mustExec(t, fixture.semantic.db, `INSERT INTO claim_states(
				claim_id, resident_id, stage, status, salience, confidence, currentness,
				temporal_relation, last_referenced_at, evidence_count
			) VALUES (?, ?, 'floating', 'active', ?, 1000000, 1000000, 'current', NULL, 1)`,
				claimID.String(), fixture.residentID.String(), salience)
			mustExec(t, fixture.semantic.db, `INSERT INTO claim_view_scope_current(
				claim_id, resident_id, view_scope, source_assertion_id
			) VALUES (?, ?, 'resident_ui', ?)`, claimID.String(), fixture.residentID.String(),
				fixture.newID(t).String())
		}
		// The erased row would rank first and consume one of the fixed 64 slots
		// if eligibility were applied after score/retention.
		addProjection(erasedID, 1)
		for claimID := range presentIDs {
			addProjection(claimID, 0.5)
		}
		eraseM7FixtureClaim(t, fixture, erasedID)

		var headRaw int64
		if err := fixture.semantic.db.QueryRow("SELECT MAX(commit_seq) FROM canonical_commits").Scan(&headRaw); err != nil {
			t.Fatal(err)
		}
		head, err := canonical.NewCommitSeq(headRaw)
		if err != nil {
			t.Fatal(err)
		}
		tx, err := fixture.semantic.db.BeginTx(context.Background(), &sql.TxOptions{ReadOnly: true})
		if err != nil {
			t.Fatal(err)
		}
		defer tx.Rollback()
		candidates, err := (&canonicalUoW{tx: tx}).loadRecallCandidates(
			context.Background(), fixture.residentID, head, memory.DefaultPolicyV2(),
		)
		if err != nil {
			t.Fatal(err)
		}
		if len(candidates) != domain.MaxRecallCandidates {
			t.Fatalf("Recall candidates = %d, want full present page %d", len(candidates), domain.MaxRecallCandidates)
		}
		for _, candidate := range candidates {
			if candidate.ClaimID == erasedID {
				t.Fatalf("Recall returned erased high-score claim %s", erasedID)
			}
			if _, ok := presentIDs[candidate.ClaimID]; !ok {
				t.Fatalf("Recall returned unexpected claim %s", candidate.ClaimID)
			}
		}
	})

	t.Run("persona", func(t *testing.T) {
		fixture := newMemoryClaimMutationFixture(t)
		eventID := fixture.seedEvent(t, memory.EventUserMessage, fixture.ownerPrincipal, memory.TrustTrusted)
		seed := []seededClaimEvidence{trustedEvidence(
			eventID, memory.EventUserMessage, fixture.ownerPrincipal, memory.PolaritySupport,
		)}
		presentID, _ := fixture.seedClaim(t, memory.ClaimKindOther, fixture.ownerPrincipal,
			fixture.residentPrincipal, seed, memory.StageSettled)
		addM7ClaimScope(t, fixture, presentID)
		erasedID, _ := fixture.seedClaim(t, memory.ClaimKindOther, fixture.ownerPrincipal,
			fixture.residentPrincipal, seed, memory.StageSettled)
		addM7ClaimScope(t, fixture, erasedID)
		eraseM7FixtureClaim(t, fixture, erasedID)

		tx, err := fixture.semantic.db.BeginTx(context.Background(), &sql.TxOptions{ReadOnly: true})
		if err != nil {
			t.Fatal(err)
		}
		defer tx.Rollback()
		work := domain.PersonaRevisionWork{ResidentID: fixture.residentID}
		if err := loadPersonaEligibleClaims(context.Background(), tx, &work, 1); err != nil {
			t.Fatal(err)
		}
		if len(work.Claims) != 1 || work.Claims[0].ClaimID != presentID {
			t.Fatalf("persona page = %+v, want present claim %s after later erased row", work.Claims, presentID)
		}
	})

	t.Run("alignment direct and meta", func(t *testing.T) {
		fixture := newMemoryClaimMutationFixture(t)
		eventID := fixture.seedEvent(t, memory.EventUserMessage, fixture.ownerPrincipal, memory.TrustTrusted)
		seed := []seededClaimEvidence{trustedEvidence(
			eventID, memory.EventUserMessage, fixture.ownerPrincipal, memory.PolaritySupport,
		)}
		erasedDirect, _ := fixture.seedClaim(t, memory.ClaimKindDirect, fixture.ownerPrincipal,
			fixture.residentPrincipal, seed, memory.StageSediment)
		presentDirect, _ := fixture.seedClaim(t, memory.ClaimKindDirect, fixture.ownerPrincipal,
			fixture.residentPrincipal, seed, memory.StageSediment)
		erasedMeta, _ := fixture.seedClaim(t, memory.ClaimKindMeta, fixture.ownerPrincipal,
			fixture.residentPrincipal, seed, memory.StageSediment)
		presentMeta, _ := fixture.seedClaim(t, memory.ClaimKindMeta, fixture.ownerPrincipal,
			fixture.residentPrincipal, seed, memory.StageSediment)
		eraseM7FixtureClaim(t, fixture, erasedDirect)
		eraseM7FixtureClaim(t, fixture, erasedMeta)

		tx, err := fixture.semantic.db.BeginTx(context.Background(), &sql.TxOptions{ReadOnly: true})
		if err != nil {
			t.Fatal(err)
		}
		defer tx.Rollback()
		reader := &canonicalUoW{tx: tx}
		policy := memory.DefaultPolicyV2()
		directs, err := loadAlignmentClaims(context.Background(), reader, fixture.residentID,
			memory.ClaimKindDirect, 1, policy)
		if err != nil {
			t.Fatal(err)
		}
		metas, err := loadAlignmentClaims(context.Background(), reader, fixture.residentID,
			memory.ClaimKindMeta, 1, policy)
		if err != nil {
			t.Fatal(err)
		}
		if len(directs) != 1 || directs[0].record.ID != presentDirect {
			t.Fatalf("direct alignment page = %+v, want %s", directs, presentDirect)
		}
		if len(metas) != 1 || metas[0].record.ID != presentMeta {
			t.Fatalf("meta alignment page = %+v, want %s", metas, presentMeta)
		}
	})

	t.Run("dependency discovery", func(t *testing.T) {
		fixture := newMemoryClaimMutationFixture(t)
		activateM7MemoryPolicyV3(t, fixture)
		seed := func() (canonical.ID, canonical.ID, canonical.ID) {
			t.Helper()
			directEvent := fixture.seedEvent(t, memory.EventUserMessage, fixture.ownerPrincipal, memory.TrustTrusted)
			directID, _ := fixture.seedClaim(t, memory.ClaimKindDirect, fixture.ownerPrincipal,
				fixture.residentPrincipal, []seededClaimEvidence{trustedEvidence(directEvent,
					memory.EventUserMessage, fixture.ownerPrincipal, memory.PolaritySupport)}, memory.StageSettled)
			metaEvent := fixture.seedEvent(t, memory.EventUserMessage, fixture.ownerPrincipal, memory.TrustTrusted)
			metaID, _ := fixture.seedClaim(t, memory.ClaimKindMeta, fixture.ownerPrincipal,
				fixture.residentPrincipal, []seededClaimEvidence{trustedEvidence(metaEvent,
					memory.EventUserMessage, fixture.ownerPrincipal, memory.PolaritySupport)}, memory.StageSediment)
			addM7AlignmentDependency(t, fixture, directID, metaID)
			return directID, metaID, addM7ClaimStatusTransition(t, fixture, metaID)
		}
		_, erasedMeta, _ := seed()
		eraseM7FixtureClaim(t, fixture, erasedMeta)
		presentDirect, _, presentStatus := seed()

		repository := (&Store{reader: fixture.semantic.db}).Canonical()
		triggers, err := repository.DiscoverReevaluationTriggers(context.Background(), fixture.residentID, 1)
		if err != nil {
			t.Fatal(err)
		}
		if len(triggers) != 1 || triggers[0].Kind != autonomy.TriggerMetaDependencyStatusChange ||
			triggers[0].SourceID != presentStatus || len(triggers[0].RelatedClaimIDs) != 1 ||
			triggers[0].RelatedClaimIDs[0] != presentDirect {
			t.Fatalf("dependency discovery page = %+v, want present trigger %s", triggers, presentStatus)
		}
	})

	t.Run("conflict discovery", func(t *testing.T) {
		fixture := newMemoryClaimMutationFixture(t)
		activateM7MemoryPolicyV3(t, fixture)
		seedClaim := func() canonical.ID {
			t.Helper()
			eventID := fixture.seedEvent(t, memory.EventUserMessage, fixture.ownerPrincipal, memory.TrustTrusted)
			claimID, _ := fixture.seedClaim(t, memory.ClaimKindDirect, fixture.ownerPrincipal,
				fixture.residentPrincipal, []seededClaimEvidence{trustedEvidence(eventID,
					memory.EventUserMessage, fixture.ownerPrincipal, memory.PolaritySupport)}, memory.StageSettled)
			return claimID
		}
		erasedLeft, erasedRight := seedClaim(), seedClaim()
		addM7Contradiction(t, fixture, erasedLeft, erasedRight)
		eraseM7FixtureClaim(t, fixture, erasedLeft)
		presentLeft, presentRight := seedClaim(), seedClaim()
		presentRelation := addM7Contradiction(t, fixture, presentLeft, presentRight)

		repository := (&Store{reader: fixture.semantic.db}).Canonical()
		triggers, err := repository.DiscoverReevaluationTriggers(context.Background(), fixture.residentID, 1)
		if err != nil {
			t.Fatal(err)
		}
		if len(triggers) != 1 || triggers[0].Kind != autonomy.TriggerSettledDirectConflict ||
			triggers[0].SourceID != presentRelation {
			t.Fatalf("conflict discovery page = %+v, want present relation %s", triggers, presentRelation)
		}
	})

	t.Run("temporal discovery", func(t *testing.T) {
		fixture := newMemoryClaimMutationFixture(t)
		addM7ClaimScope(t, fixture, claimMutationParseID(t, fixture.semantic.claim["A"]))
		validFrom := canonical.Instant(semanticTime + int64(30*time.Second/time.Microsecond))
		seed := func() canonical.ID {
			t.Helper()
			eventID := fixture.seedEvent(t, memory.EventUserMessage, fixture.ownerPrincipal, memory.TrustTrusted)
			claimID, _ := fixture.seedClaim(t, memory.ClaimKindDirect, fixture.ownerPrincipal,
				fixture.residentPrincipal, []seededClaimEvidence{trustedEvidence(eventID,
					memory.EventUserMessage, fixture.ownerPrincipal, memory.PolaritySupport)}, memory.StageSettled)
			addM7ClaimScope(t, fixture, claimID)
			addM7FutureValidity(t, fixture, claimID, validFrom)
			return claimID
		}
		erasedID := seed()
		eraseM7FixtureClaim(t, fixture, erasedID)
		presentID := seed()
		now := time.UnixMicro(semanticTime + int64(time.Minute/time.Microsecond))
		coordinator := newM7ClaimProjectionCoordinator(t, fixture, now)
		if err := coordinator.ReconcileResident(context.Background(), fixture.residentID); err != nil {
			t.Fatal(err)
		}

		repository := (&Store{reader: fixture.semantic.db}).Canonical()
		triggers, err := repository.DiscoverInitiativeTriggers(
			context.Background(), fixture.residentID, canonical.InstantFromTime(now), 5*time.Minute, 1,
		)
		if err != nil {
			t.Fatal(err)
		}
		if len(triggers) != 1 || triggers[0].Kind != autonomy.TriggerFutureToCurrent ||
			triggers[0].SourceID != presentID || triggers[0].Boundary != validFrom {
			t.Fatalf("temporal discovery page = %+v, want present claim %s", triggers, presentID)
		}
	})
}

func TestM7ClaimGenerationPurposeModeClosedMatrix(t *testing.T) {
	tests := []struct {
		name       string
		purpose    domain.GenerationPurpose
		mode       string
		wantRender bool
		wantError  bool
	}{
		{name: "dialogue recall", purpose: domain.GenerationPurposeDialogue, mode: "memory_recall", wantRender: true},
		{name: "persona recall", purpose: domain.GenerationPurposePersonaRevision, mode: "memory_recall"},
		{name: "alignment recall", purpose: domain.GenerationPurposeMemoryAlignment, mode: "memory_recall"},
		{name: "abstraction recall", purpose: domain.GenerationPurposeMemoryAbstraction, mode: "memory_recall"},
		{name: "differentiation recall", purpose: domain.GenerationPurposeMemoryDifferentiation, mode: "memory_recall"},
		{name: "self talk recall", purpose: domain.GenerationPurposeSelfTalk, mode: "memory_recall"},
		{name: "initiative recall", purpose: domain.GenerationPurposeOutboundInitiative, mode: "memory_recall"},
		{name: "dialogue live context", purpose: domain.GenerationPurposeDialogue, mode: "live_context", wantError: true},
		{name: "extraction recall", purpose: domain.GenerationPurposeMemoryExtraction, mode: "memory_recall", wantError: true},
		{name: "unknown background recall", purpose: domain.GenerationPurpose("future_background"), mode: "memory_recall", wantError: true},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			render, err := claimGenerationSourceContract(test.purpose, test.mode)
			if test.wantError {
				if err == nil {
					t.Fatalf("contract unexpectedly accepted purpose=%q mode=%q", test.purpose, test.mode)
				}
				return
			}
			if err != nil || render != test.wantRender {
				t.Fatalf("contract render/error = %t/%v, want %t/nil", render, err, test.wantRender)
			}
		})
	}
}

func TestM7EligibleClaimMissingBlobFailsIntegrityAutomaticConsumers(t *testing.T) {
	assertAutomaticIntegrity := func(t *testing.T, err error) {
		t.Helper()
		if err == nil || !errors.Is(err, errClaimContentIntegrity) ||
			errors.Is(err, domain.ErrClaimSourceIneligible) {
			t.Fatalf("automatic consumer error = %v, want private content integrity failure", err)
		}
	}
	t.Run("persona automatic consumer", func(t *testing.T) {
		fixture := newMemoryClaimMutationFixture(t)
		eventID := fixture.seedEvent(t, memory.EventUserMessage, fixture.ownerPrincipal, memory.TrustTrusted)
		claimID, _ := fixture.seedClaim(t, memory.ClaimKindOther, fixture.ownerPrincipal,
			fixture.residentPrincipal, []seededClaimEvidence{trustedEvidence(eventID,
				memory.EventUserMessage, fixture.ownerPrincipal, memory.PolaritySupport)}, memory.StageSettled)
		addM7ClaimScope(t, fixture, claimID)
		deleteM7ClaimBlob(t, fixture, claimID)
		tx, err := fixture.semantic.db.BeginTx(context.Background(), &sql.TxOptions{ReadOnly: true})
		if err != nil {
			t.Fatal(err)
		}
		defer tx.Rollback()
		work := domain.PersonaRevisionWork{ResidentID: fixture.residentID}
		err = loadPersonaEligibleClaims(context.Background(), tx, &work, 8)
		assertAutomaticIntegrity(t, err)
	})

	t.Run("alignment automatic consumer", func(t *testing.T) {
		fixture := newMemoryClaimMutationFixture(t)
		eventID := fixture.seedEvent(t, memory.EventUserMessage, fixture.ownerPrincipal, memory.TrustTrusted)
		claimID, _ := fixture.seedClaim(t, memory.ClaimKindDirect, fixture.ownerPrincipal,
			fixture.residentPrincipal, []seededClaimEvidence{trustedEvidence(eventID,
				memory.EventUserMessage, fixture.ownerPrincipal, memory.PolaritySupport)}, memory.StageSediment)
		deleteM7ClaimBlob(t, fixture, claimID)
		tx, err := fixture.semantic.db.BeginTx(context.Background(), &sql.TxOptions{ReadOnly: true})
		if err != nil {
			t.Fatal(err)
		}
		defer tx.Rollback()
		_, err = loadAlignmentClaims(context.Background(), &canonicalUoW{tx: tx}, fixture.residentID,
			memory.ClaimKindDirect, 8, memory.DefaultPolicyV2())
		assertAutomaticIntegrity(t, err)
	})

	t.Run("recall automatic consumer", func(t *testing.T) {
		fixture := newMemoryClaimMutationFixture(t)
		eventID := fixture.seedEvent(t, memory.EventUserMessage, fixture.ownerPrincipal, memory.TrustTrusted)
		claimID, _ := fixture.seedClaim(t, memory.ClaimKindOther, fixture.ownerPrincipal,
			fixture.residentPrincipal, []seededClaimEvidence{trustedEvidence(eventID,
				memory.EventUserMessage, fixture.ownerPrincipal, memory.PolaritySupport)}, memory.StageFloating)
		addProjection := func(id canonical.ID) {
			t.Helper()
			mustExec(t, fixture.semantic.db, `INSERT INTO claim_states(
				claim_id, resident_id, stage, status, salience, confidence, currentness,
				temporal_relation, last_referenced_at, evidence_count
			) VALUES (?, ?, 'floating', 'active', 1, 1000000, 1000000, 'current', NULL, 1)`,
				id.String(), fixture.residentID.String())
			mustExec(t, fixture.semantic.db, `INSERT INTO claim_view_scope_current(
				claim_id, resident_id, view_scope, source_assertion_id
			) VALUES (?, ?, 'resident_ui', ?)`, id.String(), fixture.residentID.String(),
				fixture.newID(t).String())
		}
		addProjection(claimMutationParseID(t, fixture.semantic.claim["A"]))
		addProjection(claimID)
		deleteM7ClaimBlob(t, fixture, claimID)
		var headRaw int64
		if err := fixture.semantic.db.QueryRow("SELECT MAX(commit_seq) FROM canonical_commits").Scan(&headRaw); err != nil {
			t.Fatal(err)
		}
		head, err := canonical.NewCommitSeq(headRaw)
		if err != nil {
			t.Fatal(err)
		}
		tx, err := fixture.semantic.db.BeginTx(context.Background(), &sql.TxOptions{ReadOnly: true})
		if err != nil {
			t.Fatal(err)
		}
		defer tx.Rollback()
		_, err = (&canonicalUoW{tx: tx}).loadRecallCandidates(
			context.Background(), fixture.residentID, head, memory.DefaultPolicyV2(),
		)
		assertAutomaticIntegrity(t, err)
	})

	t.Run("conflict discovery automatic consumer", func(t *testing.T) {
		fixture := newMemoryClaimMutationFixture(t)
		activateM7MemoryPolicyV3(t, fixture)
		seed := func() canonical.ID {
			t.Helper()
			eventID := fixture.seedEvent(t, memory.EventUserMessage, fixture.ownerPrincipal, memory.TrustTrusted)
			claimID, _ := fixture.seedClaim(t, memory.ClaimKindDirect, fixture.ownerPrincipal,
				fixture.residentPrincipal, []seededClaimEvidence{trustedEvidence(eventID,
					memory.EventUserMessage, fixture.ownerPrincipal, memory.PolaritySupport)}, memory.StageSettled)
			return claimID
		}
		left, right := seed(), seed()
		addM7Contradiction(t, fixture, left, right)
		deleteM7ClaimBlob(t, fixture, left)
		_, err := (&Store{reader: fixture.semantic.db}).Canonical().DiscoverReevaluationTriggers(
			context.Background(), fixture.residentID, 8,
		)
		assertAutomaticIntegrity(t, err)
	})
}

func TestM7ErasedClaimExplicitConsumersReturnIneligible(t *testing.T) {
	tests := []struct {
		name string
		load func(*semanticFixture) (canonical.ID, canonical.ID)
	}{
		{
			name: "cross resident",
			load: func(fixture *semanticFixture) (canonical.ID, canonical.ID) {
				return claimMutationParseID(t, fixture.resident["A"]), claimMutationParseID(t, fixture.claim["B"])
			},
		},
		{
			name: "missing",
			load: func(fixture *semanticFixture) (canonical.ID, canonical.ID) {
				return claimMutationParseID(t, fixture.resident["A"]), claimMutationParseID(t, fixture.ids.new())
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			fixture, closeFixture := newSemanticFixture(t)
			defer closeFixture()
			residentID, claimID := test.load(fixture)
			_, err := loadEligibleClaimStatement(context.Background(), fixture.db, residentID, claimID)
			assertM7ClaimIneligible(t, err)
		})
	}

	t.Run("erased", func(t *testing.T) {
		evidence := performM7ClaimErasure(t)
		residentID := m7ResidentForClaim(t, evidence.DB, evidence.ClaimID)
		claimID := claimMutationParseID(t, evidence.ClaimID)
		_, err := loadEligibleClaimStatement(context.Background(), evidence.DB, residentID, claimID)
		assertM7ClaimIneligible(t, err)
	})

	t.Run("null identity wins over missing content row", func(t *testing.T) {
		fixture, closeFixture := newSemanticFixture(t)
		defer closeFixture()
		ctx := context.Background()
		conn, err := fixture.db.Conn(ctx)
		if err != nil {
			t.Fatal(err)
		}
		defer conn.Close()
		mustM7Exec(t, conn, "PRAGMA foreign_keys = OFF")
		mustM7Exec(t, conn, "DROP TRIGGER trg_claims_update_contract")
		mustM7Exec(t, conn, "UPDATE claims SET statement_hash = NULL WHERE claim_id = ?", fixture.claim["A"])
		mustM7Exec(t, conn, "DROP TRIGGER trg_content_objects_no_delete")
		mustM7Exec(t, conn, `DELETE FROM content_objects WHERE content_id = (
			SELECT statement_content_id FROM claims WHERE claim_id = ?)`, fixture.claim["A"])
		residentID := claimMutationParseID(t, fixture.resident["A"])
		claimID := claimMutationParseID(t, fixture.claim["A"])
		_, err = loadEligibleClaimStatement(ctx, conn, residentID, claimID)
		assertM7ClaimIneligible(t, err)
	})

	t.Run("derivation preparation and landing Writer", func(t *testing.T) {
		fixture := newDerivedClaimMutationFixture(t)
		// PrepareMemoryDerivation deliberately travels through the public
		// CanonicalRepository snapshot before it reaches the semantic source
		// loader. Seed the production identities that the compact mutation
		// fixture otherwise does not need.
		mustExec(t, fixture.semantic.db, `INSERT INTO pipeline_versions(
			pipeline_version_id, canonical_commit_id, pipeline_kind, version_key,
			definition, recorded_at, recorded_tz
		) VALUES (?, ?, 'dialogue', ?, '{}', ?, ?)`, fixture.newID(t).String(),
			fixture.semantic.commit["global"], domain.DialoguePromptTemplateVersionV1, semanticTime, semanticTZ)
		mustExec(t, fixture.semantic.db, `INSERT INTO sessionization_policy_versions(
			sessionization_policy_version_id, canonical_commit_id, version_key,
			definition, recorded_at, recorded_tz
		) VALUES (?, ?, ?, ?, ?, ?)`, fixture.newID(t).String(), fixture.semantic.commit["global"],
			domain.SessionPolicyVersion,
			`{"idle_gap_microseconds":"1800000000","version":"sessionization-v1"}`,
			semanticTime, semanticTZ)
		sourceIDs, sourceEvidence := fixture.sourceClaims(t, memory.ScopeResidentUI, memory.ScopeResidentUI)
		value := fixture.abstractionValue(t, sourceIDs, sourceEvidence)
		eraseM7FixtureClaim(t, fixture.memoryClaimMutationFixture, sourceIDs[0])

		repository := (&Store{reader: fixture.semantic.db}).Canonical()
		_, err := repository.PrepareMemoryDerivation(
			context.Background(), fixture.residentID, domain.GenerationPurposeMemoryAbstraction, sourceIDs,
		)
		assertM7ClaimIneligible(t, err)

		uow, _ := fixture.beginUoW(t)
		_, err = uow.LandClaimAbstraction(context.Background(), value)
		if rollbackErr := uow.tx.Rollback(); rollbackErr != nil {
			t.Fatal(rollbackErr)
		}
		assertM7ClaimIneligible(t, err)
		var claimRows, succeeded int
		if err := fixture.semantic.db.QueryRow(`SELECT
			(SELECT COUNT(*) FROM claims WHERE claim_id = ?),
			(SELECT COUNT(*) FROM generation_run_outcomes WHERE generation_run_id = ? AND state = 'succeeded')`,
			value.ClaimID.String(), value.RunID.String()).Scan(&claimRows, &succeeded); err != nil {
			t.Fatal(err)
		}
		if claimRows != 0 || succeeded != 0 {
			t.Fatalf("ineligible derivation leaked claim/succeeded outcome = %d/%d", claimRows, succeeded)
		}
	})
}

func TestM7ErasedClaimAdminRedaction(t *testing.T) {
	tests := []struct {
		name string
		mode string
	}{
		{name: "paired erasure", mode: "paired"},
		{name: "null hash with present content half state", mode: "null_hash"},
		{name: "erased content with retained hash half state", mode: "erased_content"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			var db *sql.DB
			var residentID, claimID canonical.ID
			var retainedHash []byte
			if test.mode != "paired" {
				fixture, closeFixture := newSemanticFixture(t)
				defer closeFixture()
				db = fixture.db
				residentID = claimMutationParseID(t, fixture.resident["A"])
				claimID = claimMutationParseID(t, fixture.claim["A"])
				if err := db.QueryRow("SELECT statement_hash FROM claims WHERE claim_id = ?", claimID.String()).Scan(&retainedHash); err != nil {
					t.Fatal(err)
				}
				switch test.mode {
				case "null_hash":
					mustM7Exec(t, db, "DROP TRIGGER trg_claims_update_contract")
					mustM7Exec(t, db, "UPDATE claims SET statement_hash = NULL WHERE claim_id = ?", claimID.String())
				case "erased_content":
					mustM7Exec(t, db, "PRAGMA ignore_check_constraints = ON")
					mustM7Exec(t, db, "DROP TRIGGER trg_content_objects_erasure_only")
					mustM7Exec(t, db, `UPDATE content_objects SET erasure_state = 'erased' WHERE content_id = (
						SELECT statement_content_id FROM claims WHERE claim_id = ?)`, claimID.String())
				default:
					t.Fatalf("unknown Admin half-state mode %q", test.mode)
				}
				addM7AdminScope(t, db, fixture.commit["A"], claimID.String(), fixture.principal["human"])
			} else {
				evidence := performM7ClaimErasure(t)
				db = evidence.DB
				residentID = m7ResidentForClaim(t, db, evidence.ClaimID)
				claimID = claimMutationParseID(t, evidence.ClaimID)
				var actorRaw string
				if err := db.QueryRow("SELECT principal_id FROM residents WHERE resident_id = ?", residentID.String()).Scan(&actorRaw); err != nil {
					t.Fatal(err)
				}
				addM7AdminScope(t, db, evidence.Metadata.CommitID.String(), claimID.String(), actorRaw)
			}

			repository := (&Store{reader: db}).Canonical()
			claims, err := repository.ListMemoryClaims(context.Background(), domain.MemoryClaimFilter{
				ResidentID: residentID, Limit: 10,
			})
			if err != nil {
				t.Fatal(err)
			}
			if len(claims) != 1 || claims[0].ClaimID != claimID || !claims[0].StatementErased || claims[0].Statement != "[erased]" {
				t.Fatalf("admin claims = %+v", claims)
			}
			provenance, err := repository.MemoryClaimProvenance(context.Background(), residentID, claimID)
			if err != nil {
				t.Fatal(err)
			}
			encoded, err := json.Marshal(provenance)
			if err != nil {
				t.Fatal(err)
			}
			wire := string(encoded)
			if strings.Contains(wire, "A-claim") || strings.Contains(wire, "statement_hash") ||
				(len(retainedHash) != 0 && strings.Contains(wire, base64.StdEncoding.EncodeToString(retainedHash))) {
				t.Fatalf("admin provenance leaked statement/hash material: %s", encoded)
			}
		})
	}
}

func TestM7ErasedClaimProjectionRebuild(t *testing.T) {
	ctx := context.Background()
	fixture := newMemoryClaimMutationFixture(t)
	claimID := claimMutationParseID(t, fixture.semantic.claim["A"])
	addM7ClaimScope(t, fixture, claimID)
	eraseM7FixtureClaim(t, fixture, claimID)

	database := &Store{
		reader: fixture.semantic.db, writer: fixture.semantic.db, writes: newWritePriorityGate(),
	}
	surface := database.Projection()
	registry, err := projection.NewRegistry(
		projection.ClaimStatesDefinition(), projection.ClaimViewScopeCurrentDefinition(),
	)
	if err != nil {
		t.Fatal(err)
	}
	clock := testsupport.NewManualClock(time.UnixMicro(semanticTime + int64(time.Minute/time.Microsecond)))
	coordinator, err := projection.NewCoordinator(projection.CoordinatorOptions{
		Registry: registry, Source: surface, Store: surface, Clock: clock,
		Timezone: canonical.MustTimezone(semanticTZ), ScanInterval: time.Hour,
		AsOfRefreshInterval: time.Hour, RebuildRetryInterval: time.Hour, MaxStaleness: time.Hour,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := coordinator.ReconcileResident(ctx, fixture.residentID); err != nil {
		t.Fatal(err)
	}
	claimGolden := readClaimProjectionGolden(t, database, fixture.residentID)
	scopeGolden := readClaimViewScopeGolden(t, database, fixture.residentID)
	if !strings.Contains(claimGolden, claimID.String()) || !strings.Contains(scopeGolden, claimID.String()) {
		t.Fatalf("erased structural claim missing from Projection golden state=%q scope=%q", claimGolden, scopeGolden)
	}
	if strings.Contains(claimGolden, "A-claim") || strings.Contains(scopeGolden, "A-claim") {
		t.Fatalf("Projection golden leaked erased statement canary state=%q scope=%q", claimGolden, scopeGolden)
	}

	watermarks := make(map[projection.Name][]byte, 2)
	for _, definition := range registry.Definitions() {
		watermark, exists, err := surface.Watermark(ctx, definition.Name, fixture.residentID)
		if err != nil || !exists {
			t.Fatalf("initial %s watermark = %+v exists=%t err=%v", definition.Name, watermark, exists, err)
		}
		watermarks[definition.Name], err = json.Marshal(watermark)
		if err != nil {
			t.Fatal(err)
		}
		if err := surface.Drop(ctx, projection.DropRequest{
			Definition: definition, ResidentID: fixture.residentID, Observed: &watermark,
		}); err != nil {
			t.Fatal(err)
		}
	}
	if got := readClaimProjectionGolden(t, database, fixture.residentID); got != "" {
		t.Fatalf("claim Projection body after drop = %q", got)
	}
	if got := readClaimViewScopeGolden(t, database, fixture.residentID); got != "" {
		t.Fatalf("scope Projection body after drop = %q", got)
	}
	if err := coordinator.ReconcileResident(ctx, fixture.residentID); err != nil {
		t.Fatal(err)
	}
	if rebuilt := readClaimProjectionGolden(t, database, fixture.residentID); rebuilt != claimGolden {
		t.Fatalf("claim Projection rebuilt golden = %q, want %q", rebuilt, claimGolden)
	}
	if rebuilt := readClaimViewScopeGolden(t, database, fixture.residentID); rebuilt != scopeGolden {
		t.Fatalf("scope Projection rebuilt golden = %q, want %q", rebuilt, scopeGolden)
	}
	for _, definition := range registry.Definitions() {
		watermark, exists, err := surface.Watermark(ctx, definition.Name, fixture.residentID)
		if err != nil || !exists {
			t.Fatalf("rebuilt %s watermark = %+v exists=%t err=%v", definition.Name, watermark, exists, err)
		}
		encoded, err := json.Marshal(watermark)
		if err != nil {
			t.Fatal(err)
		}
		if string(encoded) != string(watermarks[definition.Name]) {
			t.Fatalf("rebuilt %s watermark = %s, want %s", definition.Name, encoded, watermarks[definition.Name])
		}
	}

	for _, table := range []string{"claim_states", "claim_view_scope_current"} {
		rows, err := fixture.semantic.db.Query("PRAGMA table_info(" + table + ")")
		if err != nil {
			t.Fatal(err)
		}
		for rows.Next() {
			var ordinal, notNull, primaryKey int
			var name, columnType string
			var defaultValue any
			if err := rows.Scan(&ordinal, &name, &columnType, &notNull, &defaultValue, &primaryKey); err != nil {
				_ = rows.Close()
				t.Fatal(err)
			}
			lower := strings.ToLower(name)
			for _, forbidden := range []string{"statement", "content", "blob", "hash"} {
				if strings.Contains(lower, forbidden) {
					_ = rows.Close()
					t.Fatalf("Projection table %s persists forbidden semantic column %s", table, name)
				}
			}
		}
		if err := rows.Close(); err != nil {
			t.Fatal(err)
		}
	}

	var blobCount int
	if err := fixture.semantic.db.QueryRow(`SELECT COUNT(*) FROM claims claim
		JOIN content_objects content ON content.content_id = claim.statement_content_id
		JOIN blobs blob ON blob.dedupe_scope_id = content.owner_resident_id
		 AND blob.hash_algorithm = content.blob_hash_algorithm AND blob.blob_hash = content.blob_hash
		WHERE claim.claim_id = ?`, claimID.String()).Scan(&blobCount); err != nil {
		t.Fatal(err)
	}
	if blobCount != 0 {
		t.Fatalf("erased statement blob count = %d, want 0", blobCount)
	}
}

func TestM7EligibleClaimMissingBlobFailsIntegrity(t *testing.T) {
	tests := []struct {
		name       string
		wantDetail string
		mutate     func(*testing.T, *semanticFixture, *sql.Conn)
	}{
		{
			name:       "missing content row",
			wantDetail: "statement content row is missing",
			mutate: func(t *testing.T, fixture *semanticFixture, conn *sql.Conn) {
				mustM7Exec(t, conn, "DROP TRIGGER trg_content_objects_no_delete")
				mustM7Exec(t, conn, `DELETE FROM content_objects WHERE content_id = (
					SELECT statement_content_id FROM claims WHERE claim_id = ?)`, fixture.claim["A"])
			},
		},
		{
			name:       "missing blob precedes statement algorithm",
			wantDetail: "blob is missing",
			mutate: func(t *testing.T, fixture *semanticFixture, conn *sql.Conn) {
				mustM7Exec(t, conn, `DELETE FROM blobs WHERE dedupe_scope_id = ? AND blob_hash = (
					SELECT content.blob_hash FROM claims claim
					JOIN content_objects content ON content.content_id = claim.statement_content_id
					WHERE claim.claim_id = ?)`, fixture.resident["A"], fixture.claim["A"])
				mustM7Exec(t, conn, "PRAGMA ignore_check_constraints = ON")
				mustM7Exec(t, conn, "DROP TRIGGER trg_claims_update_contract")
				mustM7Exec(t, conn, "UPDATE claims SET statement_hash_algorithm = 'sha512' WHERE claim_id = ?", fixture.claim["A"])
			},
		},
		{
			name:       "byte size precedes raw digest and statement algorithm",
			wantDetail: "blob byte size does not match",
			mutate: func(t *testing.T, fixture *semanticFixture, conn *sql.Conn) {
				mustM7Exec(t, conn, "DROP TRIGGER trg_blobs_no_update")
				mustM7Exec(t, conn, `UPDATE blobs SET byte_size = byte_size + 1, content = ? WHERE dedupe_scope_id = ?
					AND blob_hash = (SELECT content.blob_hash FROM claims claim JOIN content_objects content
					ON content.content_id = claim.statement_content_id WHERE claim.claim_id = ?)`, []byte("Z-claim"), fixture.resident["A"], fixture.claim["A"])
				mustM7Exec(t, conn, "PRAGMA ignore_check_constraints = ON")
				mustM7Exec(t, conn, "DROP TRIGGER trg_claims_update_contract")
				mustM7Exec(t, conn, "UPDATE claims SET statement_hash_algorithm = 'sha512' WHERE claim_id = ?", fixture.claim["A"])
			},
		},
		{
			name:       "raw digest precedes statement algorithm",
			wantDetail: "blob digest does not match",
			mutate: func(t *testing.T, fixture *semanticFixture, conn *sql.Conn) {
				mustM7Exec(t, conn, "DROP TRIGGER trg_blobs_no_update")
				mustM7Exec(t, conn, `UPDATE blobs SET content = ? WHERE dedupe_scope_id = ?
					AND blob_hash = (SELECT content.blob_hash FROM claims claim JOIN content_objects content
					ON content.content_id = claim.statement_content_id WHERE claim.claim_id = ?)`, []byte("Z-claim"), fixture.resident["A"], fixture.claim["A"])
				mustM7Exec(t, conn, "PRAGMA ignore_check_constraints = ON")
				mustM7Exec(t, conn, "DROP TRIGGER trg_claims_update_contract")
				mustM7Exec(t, conn, "UPDATE claims SET statement_hash_algorithm = 'sha512' WHERE claim_id = ?", fixture.claim["A"])
			},
		},
		{
			name:       "content owner precedes missing blob",
			wantDetail: "content owner does not match",
			mutate: func(t *testing.T, fixture *semanticFixture, conn *sql.Conn) {
				mustM7Exec(t, conn, "DROP TRIGGER trg_content_objects_erasure_only")
				mustM7Exec(t, conn, `UPDATE content_objects SET owner_resident_id = ? WHERE content_id = (
					SELECT statement_content_id FROM claims WHERE claim_id = ?)`, fixture.resident["B"], fixture.claim["A"])
			},
		},
		{
			name:       "statement hash algorithm precedes normalization metadata",
			wantDetail: "statement hash algorithm",
			mutate: func(t *testing.T, fixture *semanticFixture, conn *sql.Conn) {
				mustM7Exec(t, conn, "PRAGMA ignore_check_constraints = ON")
				mustM7Exec(t, conn, "DROP TRIGGER trg_claims_update_contract")
				mustM7Exec(t, conn, `UPDATE claims SET statement_hash_algorithm = 'sha512',
					statement_normalization_version = 'unknown', statement_hash = ? WHERE claim_id = ?`,
					canonical.HashBlob([]byte("different normalized statement")).Bytes(), fixture.claim["A"])
			},
		},
		{
			name:       "content blob hash algorithm precedes missing blob",
			wantDetail: "blob hash algorithm",
			mutate: func(t *testing.T, fixture *semanticFixture, conn *sql.Conn) {
				mustM7Exec(t, conn, "PRAGMA ignore_check_constraints = ON")
				mustM7Exec(t, conn, "DROP TRIGGER trg_content_objects_erasure_only")
				mustM7Exec(t, conn, `UPDATE content_objects SET blob_hash_algorithm = 'sha512' WHERE content_id = (
					SELECT statement_content_id FROM claims WHERE claim_id = ?)`, fixture.claim["A"])
			},
		},
		{
			name:       "normalization version precedes normalized digest",
			wantDetail: "statement normalization version",
			mutate: func(t *testing.T, fixture *semanticFixture, conn *sql.Conn) {
				mustM7Exec(t, conn, "DROP TRIGGER trg_claims_update_contract")
				mustM7Exec(t, conn, `UPDATE claims SET statement_normalization_version = 'unknown',
					statement_hash = ? WHERE claim_id = ?`, canonical.HashBlob([]byte("different normalized statement")).Bytes(), fixture.claim["A"])
			},
		},
		{
			name:       "normalized digest mismatch",
			wantDetail: "normalized statement digest does not match",
			mutate: func(t *testing.T, fixture *semanticFixture, conn *sql.Conn) {
				mustM7Exec(t, conn, "DROP TRIGGER trg_claims_update_contract")
				mustM7Exec(t, conn, "UPDATE claims SET statement_hash = ? WHERE claim_id = ?",
					canonical.HashBlob([]byte("different normalized statement")).Bytes(), fixture.claim["A"])
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			fixture, closeFixture := newSemanticFixture(t)
			defer closeFixture()
			ctx := context.Background()
			conn, err := fixture.db.Conn(ctx)
			if err != nil {
				t.Fatal(err)
			}
			defer conn.Close()
			mustM7Exec(t, conn, "PRAGMA foreign_keys = OFF")
			test.mutate(t, fixture, conn)
			residentID := claimMutationParseID(t, fixture.resident["A"])
			claimID := claimMutationParseID(t, fixture.claim["A"])
			_, err = loadEligibleClaimStatement(ctx, conn, residentID, claimID)
			if err == nil || !errors.Is(err, errClaimContentIntegrity) || errors.Is(err, domain.ErrClaimSourceIneligible) ||
				!strings.Contains(err.Error(), test.wantDetail) {
				t.Fatalf("integrity error = %v, want private integrity detail %q", err, test.wantDetail)
			}
		})
	}
}

func TestM7ErasedClaimDuplicateLookupCreatesNewIdentity(t *testing.T) {
	fixture := newMemoryClaimMutationFixture(t)
	firstEvent := fixture.seedEvent(t, memory.EventUserMessage, fixture.ownerPrincipal, memory.TrustTrusted)
	statementText := "The owner likes the same tea"
	extracted := memory.ExtractionClaim{
		Statement: statementText, Subject: memory.SelectorSourceActor,
		Perspective: memory.SelectorResident, TemporalKind: memory.TemporalStable,
		Grade: memory.GradeStated, SourceQuote: "same tea",
	}
	land := func(eventID canonical.ID) canonical.ID {
		t.Helper()
		landing := domain.ExtractedClaimLanding{
			ClaimID: fixture.newID(t), EvidenceID: fixture.newID(t),
			InitialStageID: fixture.newID(t), InitialViewScopeID: fixture.newID(t),
			Statement: fixture.content(t, "claim_statement", []byte(statementText), "independent"),
		}
		command := domain.LandMemoryExtraction{
			Attempt:       domain.Attempt{RunID: claimMutationParseID(t, fixture.semantic.run["A"]), ResidentID: fixture.residentID},
			SourceEventID: eventID, PipelineVersionID: fixture.maturationPipeline,
			MemoryPolicyRevisionID: fixture.policyID,
		}
		uow, _ := fixture.beginUoW(t)
		claimID, err := uow.landExtractedClaim(context.Background(), command, landing, extracted,
			memory.DefaultPolicyV2(), memory.EventUserMessage, memory.TrustTrusted,
			fixture.ownerPrincipal, fixture.residentPrincipal)
		if err != nil {
			_ = uow.tx.Rollback()
			t.Fatal(err)
		}
		if err := uow.tx.Commit(); err != nil {
			t.Fatal(err)
		}
		return claimID
	}
	oldID := land(firstEvent)
	eraseM7FixtureClaim(t, fixture, oldID)
	newID := land(fixture.seedEvent(t, memory.EventUserMessage, fixture.ownerPrincipal, memory.TrustTrusted))
	if newID == oldID {
		t.Fatalf("erased claim identity %s was reused", oldID)
	}
	var oldHash, newHash []byte
	var oldState, newState string
	if err := fixture.semantic.db.QueryRow(`SELECT old_claim.statement_hash, old_content.erasure_state,
		new_claim.statement_hash, new_content.erasure_state FROM claims old_claim
		JOIN content_objects old_content ON old_content.content_id = old_claim.statement_content_id
		JOIN claims new_claim ON new_claim.claim_id = ?
		JOIN content_objects new_content ON new_content.content_id = new_claim.statement_content_id
		WHERE old_claim.claim_id = ?`, newID.String(), oldID.String()).Scan(
		&oldHash, &oldState, &newHash, &newState,
	); err != nil {
		t.Fatal(err)
	}
	if oldHash != nil || oldState != "erased" || len(newHash) != 32 || newState != "present" {
		t.Fatalf("reinserted identities old=%x/%s new=%x/%s", oldHash, oldState, newHash, newState)
	}
	var oldEvidence, newEvidence int
	if err := fixture.semantic.db.QueryRow(`SELECT
		(SELECT COUNT(*) FROM claim_evidence WHERE claim_id = ?),
		(SELECT COUNT(*) FROM claim_evidence WHERE claim_id = ?)`, oldID.String(), newID.String()).Scan(
		&oldEvidence, &newEvidence,
	); err != nil {
		t.Fatal(err)
	}
	if oldEvidence != 1 || newEvidence != 1 {
		t.Fatalf("evidence counts old/new = %d/%d", oldEvidence, newEvidence)
	}

	t.Run("derivation duplicate lookup", func(t *testing.T) {
		fixture := newDerivedClaimMutationFixture(t)
		sourceIDs, sourceEvidence := fixture.sourceClaims(t, memory.ScopeResidentUI, memory.ScopeResidentUI)
		land := func(value domain.LandDerivedClaim) canonical.ID {
			t.Helper()
			uow, _ := fixture.beginUoW(t)
			result, err := uow.LandClaimAbstraction(context.Background(), value)
			if err != nil {
				_ = uow.tx.Rollback()
				t.Fatal(err)
			}
			if err := uow.tx.Commit(); err != nil {
				t.Fatal(err)
			}
			return result.ClaimID
		}
		oldDerivedID := land(fixture.abstractionValue(t, sourceIDs, sourceEvidence))
		eraseM7FixtureClaim(t, fixture.memoryClaimMutationFixture, oldDerivedID)
		newDerivedID := land(fixture.abstractionValue(t, sourceIDs, sourceEvidence))
		if newDerivedID == oldDerivedID {
			t.Fatalf("derivation reused erased claim identity %s", oldDerivedID)
		}
		var oldDerivedHash, newDerivedHash []byte
		var oldDerivedState, newDerivedState string
		var oldDerivedEvidence, newDerivedEvidence, oldRelations, newRelations int
		if err := fixture.semantic.db.QueryRow(`SELECT
			old_claim.statement_hash, old_content.erasure_state,
			new_claim.statement_hash, new_content.erasure_state,
			(SELECT COUNT(*) FROM claim_evidence WHERE claim_id = old_claim.claim_id),
			(SELECT COUNT(*) FROM claim_evidence WHERE claim_id = new_claim.claim_id),
			(SELECT COUNT(*) FROM claim_relations WHERE from_claim_id = old_claim.claim_id),
			(SELECT COUNT(*) FROM claim_relations WHERE from_claim_id = new_claim.claim_id)
			FROM claims old_claim
			JOIN content_objects old_content ON old_content.content_id = old_claim.statement_content_id
			JOIN claims new_claim ON new_claim.claim_id = ?
			JOIN content_objects new_content ON new_content.content_id = new_claim.statement_content_id
			WHERE old_claim.claim_id = ?`, newDerivedID.String(), oldDerivedID.String()).Scan(
			&oldDerivedHash, &oldDerivedState, &newDerivedHash, &newDerivedState,
			&oldDerivedEvidence, &newDerivedEvidence, &oldRelations, &newRelations,
		); err != nil {
			t.Fatal(err)
		}
		if oldDerivedHash != nil || oldDerivedState != "erased" || len(newDerivedHash) != 32 ||
			newDerivedState != "present" || oldDerivedEvidence != 2 || newDerivedEvidence != 2 ||
			oldRelations != 2 || newRelations != 2 {
			t.Fatalf("derived reinsert old=%x/%s evidence=%d relations=%d new=%x/%s evidence=%d relations=%d",
				oldDerivedHash, oldDerivedState, oldDerivedEvidence, oldRelations,
				newDerivedHash, newDerivedState, newDerivedEvidence, newRelations)
		}
	})
}

func eraseM7FixtureClaim(t *testing.T, fixture *memoryClaimMutationFixture, claimID canonical.ID) {
	t.Helper()
	uow, metadata := fixture.beginUoW(t)
	_, err := uow.EraseClaimStatement(context.Background(), domain.EraseClaimStatement{
		ResidentID: fixture.residentID, ClaimID: claimID,
		ClaimStatementErasureEventID: fixture.newID(t), ContentErasureEventID: fixture.newID(t),
		ActorPrincipalID: fixture.ownerPrincipal, ReasonCode: "m7_consumer_test",
		OccurredAt: metadata.CommittedAt, OccurredTZ: metadata.CommittedTZ,
	})
	if err != nil {
		_ = uow.tx.Rollback()
		t.Fatal(err)
	}
	if err := uow.tx.Commit(); err != nil {
		t.Fatal(err)
	}
}

func addM7ClaimScope(t *testing.T, fixture *memoryClaimMutationFixture, claimID canonical.ID) {
	t.Helper()
	mustExec(t, fixture.semantic.db, `INSERT INTO claim_view_scope_assertions(
		view_scope_assertion_id, canonical_commit_id, claim_id, view_scope, actor_principal_id,
		generation_run_id, memory_policy_revision_id, reason_code, reason_content_id, recorded_at, recorded_tz
	) VALUES (?, ?, ?, 'resident_ui', ?, NULL, ?, 'm7_test', NULL, ?, ?)`,
		fixture.newID(t).String(), fixture.addStageCommit(t), claimID.String(), fixture.ownerPrincipal.String(),
		fixture.policyID.String(), semanticTime, semanticTZ)
}

func activateM7MemoryPolicyV3(t *testing.T, fixture *memoryClaimMutationFixture) {
	t.Helper()
	encoded, err := memory.DefaultPolicyV3().CanonicalJSON()
	if err != nil {
		t.Fatal(err)
	}
	commitID := fixture.addStageCommit(t)
	contentID := fixture.semantic.addContent(t, "A", "memory_policy_text", encoded.String(), "resident_only")
	revisionID := fixture.newID(t)
	mustExec(t, fixture.semantic.db, `INSERT INTO resident_revisions(
		revision_id, canonical_commit_id, resident_id, revision_class, content_id,
		parent_revision_id, created_by_run_id, reason_content_id, recorded_at, recorded_tz
	) VALUES (?, ?, ?, 'memory_policy', ?, ?, NULL, NULL, ?, ?)`, revisionID.String(), commitID,
		fixture.residentID.String(), contentID, fixture.policyID.String(), semanticTime, semanticTZ)
	mustExec(t, fixture.semantic.db, `INSERT INTO resident_revision_activations(
		activation_id, canonical_commit_id, resident_id, revision_id, actor_principal_id,
		approval_id, reason_code, reason_content_id, recorded_at, recorded_tz
	) VALUES (?, ?, ?, ?, ?, NULL, 'm7_autonomy_test', NULL, ?, ?)`, fixture.newID(t).String(),
		commitID, fixture.residentID.String(), revisionID.String(), fixture.ownerPrincipal.String(),
		semanticTime, semanticTZ)
	fixture.policyID = revisionID
}

func addM7AlignmentDependency(
	t *testing.T,
	fixture *memoryClaimMutationFixture,
	directClaimID, dependencyClaimID canonical.ID,
) {
	t.Helper()
	var transitionRaw string
	if err := fixture.semantic.db.QueryRow(`SELECT transition.stage_transition_id
		FROM claim_stage_transitions transition
		JOIN canonical_commits commit_row ON commit_row.canonical_commit_id = transition.canonical_commit_id
		WHERE transition.claim_id = ? AND transition.to_stage = 'settled'
		ORDER BY commit_row.commit_seq DESC, transition.stage_transition_id DESC LIMIT 1`,
		directClaimID.String()).Scan(&transitionRaw); err != nil {
		t.Fatal(err)
	}
	mustExec(t, fixture.semantic.db, `INSERT INTO claim_stage_transition_dependencies(
		stage_transition_dependency_id, canonical_commit_id, stage_transition_id,
		dependency_kind, dependency_claim_id
	) VALUES (?, ?, ?, 'meta_alignment', ?)`, fixture.newID(t).String(), fixture.addStageCommit(t),
		transitionRaw, dependencyClaimID.String())
}

func addM7ClaimStatusTransition(
	t *testing.T,
	fixture *memoryClaimMutationFixture,
	claimID canonical.ID,
) canonical.ID {
	t.Helper()
	transitionID := fixture.newID(t)
	mustExec(t, fixture.semantic.db, `INSERT INTO claim_status_transitions(
		status_transition_id, canonical_commit_id, claim_id, from_status, to_status,
		decision_kind, actor_principal_id, trigger_kind, trigger_event_id, trigger_evidence_id,
		trigger_claim_relation_id, trigger_integrity_finding_id, pipeline_version_id,
		memory_policy_revision_id, gate_metrics, decision_reason_code, decision_reason_content_id,
		occurred_at, occurred_tz, recorded_at, recorded_tz
	) VALUES (?, ?, ?, 'active', 'quarantined', 'human', ?, NULL, NULL, NULL, NULL, NULL,
		NULL, NULL, NULL, 'm7_dependency_change', NULL, ?, ?, ?, ?)`, transitionID.String(),
		fixture.addStageCommit(t), claimID.String(), fixture.ownerPrincipal.String(),
		semanticTime, semanticTZ, semanticTime, semanticTZ)
	return transitionID
}

func addM7Contradiction(
	t *testing.T,
	fixture *memoryClaimMutationFixture,
	fromClaimID, toClaimID canonical.ID,
) canonical.ID {
	t.Helper()
	relationID := fixture.newID(t)
	mustExec(t, fixture.semantic.db, `INSERT INTO claim_relations(
		claim_relation_id, canonical_commit_id, from_claim_id, to_claim_id, relation_type,
		reason_code, reason_content_id, generation_run_id, occurred_at, occurred_tz,
		recorded_at, recorded_tz
	) VALUES (?, ?, ?, ?, 'contradicts', 'm7_conflict', NULL, NULL, ?, ?, ?, ?)`,
		relationID.String(), fixture.addStageCommit(t), fromClaimID.String(), toClaimID.String(),
		semanticTime, semanticTZ, semanticTime, semanticTZ)
	return relationID
}

func addM7FutureValidity(
	t *testing.T,
	fixture *memoryClaimMutationFixture,
	claimID canonical.ID,
	validFrom canonical.Instant,
) {
	t.Helper()
	mustExec(t, fixture.semantic.db, `INSERT INTO claim_validity_assertions(
		validity_assertion_id, canonical_commit_id, claim_id, assertion_type, valid_from,
		valid_from_tz, valid_to, valid_to_tz, evidence_event_id, confidence,
		actor_principal_id, reason_code, reason_content_id, recorded_at, recorded_tz
	) VALUES (?, ?, ?, 'interval', ?, ?, NULL, NULL, NULL, 1000000, ?, 'm7_future', NULL, ?, ?)`,
		fixture.newID(t).String(), fixture.addStageCommit(t), claimID.String(), validFrom.UnixMicro(),
		semanticTZ, fixture.ownerPrincipal.String(), semanticTime, semanticTZ)
}

func newM7ClaimProjectionCoordinator(
	t *testing.T,
	fixture *memoryClaimMutationFixture,
	now time.Time,
) *projection.Coordinator {
	t.Helper()
	database := &Store{
		reader: fixture.semantic.db,
		writer: fixture.semantic.db,
		writes: newWritePriorityGate(),
	}
	surface := database.Projection()
	registry, err := projection.NewRegistry(
		projection.ClaimStatesDefinition(), projection.ClaimViewScopeCurrentDefinition(),
	)
	if err != nil {
		t.Fatal(err)
	}
	coordinator, err := projection.NewCoordinator(projection.CoordinatorOptions{
		Registry: registry, Source: surface, Store: surface, Clock: testsupport.NewManualClock(now),
		Timezone: canonical.MustTimezone(semanticTZ), ScanInterval: time.Hour,
		AsOfRefreshInterval: time.Hour, RebuildRetryInterval: time.Hour, MaxStaleness: time.Hour,
	})
	if err != nil {
		t.Fatal(err)
	}
	return coordinator
}

func deleteM7ClaimBlob(t *testing.T, fixture *memoryClaimMutationFixture, claimID canonical.ID) {
	t.Helper()
	ctx := context.Background()
	conn, err := fixture.semantic.db.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	mustM7Exec(t, conn, "PRAGMA foreign_keys = OFF")
	mustM7Exec(t, conn, `DELETE FROM blobs WHERE dedupe_scope_id = ? AND blob_hash = (
		SELECT content.blob_hash FROM claims claim
		JOIN content_objects content ON content.content_id = claim.statement_content_id
		WHERE claim.claim_id = ?)`, fixture.residentID.String(), claimID.String())
}

func addM7AdminScope(t *testing.T, db *sql.DB, commitID, claimID, actorID string) {
	t.Helper()
	mustM7Exec(t, db, `INSERT INTO claim_view_scope_assertions(
		view_scope_assertion_id, canonical_commit_id, claim_id, view_scope, actor_principal_id,
		generation_run_id, memory_policy_revision_id, reason_code, reason_content_id, recorded_at, recorded_tz
	) VALUES ('70000000000000000000000001', ?, ?, 'admin_only', ?, NULL, NULL, 'm7_admin_test', NULL, ?, ?)`,
		commitID, claimID, actorID, semanticTime, semanticTZ)
}

func m7ResidentForClaim(t *testing.T, db *sql.DB, claimID string) canonical.ID {
	t.Helper()
	var residentRaw string
	if err := db.QueryRow("SELECT owner_resident_id FROM claims WHERE claim_id = ?", claimID).Scan(&residentRaw); err != nil {
		t.Fatal(err)
	}
	return claimMutationParseID(t, residentRaw)
}

func assertM7ClaimIneligible(t *testing.T, err error) {
	t.Helper()
	if err == nil || !errors.Is(err, domain.ErrClaimSourceIneligible) || errors.Is(err, errClaimContentIntegrity) {
		t.Fatalf("claim source error = %v, want only ErrClaimSourceIneligible", err)
	}
}

type m7Execer interface {
	ExecContext(context.Context, string, ...any) (sql.Result, error)
}

func mustM7Exec(t *testing.T, execer m7Execer, query string, args ...any) {
	t.Helper()
	if _, err := execer.ExecContext(context.Background(), query, args...); err != nil {
		t.Fatalf("M7 fixture SQL failed: %v\n%s", err, query)
	}
}
