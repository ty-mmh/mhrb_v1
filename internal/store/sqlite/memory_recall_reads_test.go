package sqlite

import (
	"context"
	"testing"
	"time"

	"mahoroba.local/mahoroba/internal/canonical"
	"mahoroba.local/mahoroba/internal/domain"
	"mahoroba.local/mahoroba/internal/memory"
	"mahoroba.local/mahoroba/internal/projection"
)

func TestRecallProjectionFreshnessRequiresExactVersionDependencyHeadAndAsOf(t *testing.T) {
	tests := []struct {
		name            string
		version         string
		sourceDelta     int64
		asOfDelta       time.Duration
		wrongDependency bool
		want            domain.MemoryRecallFallbackReason
	}{
		{name: "exact", version: "claim-states-v1"},
		{name: "version", version: "claim-states-v2", want: domain.MemoryRecallProjectionProvenanceUnknown},
		{name: "head", version: "claim-states-v1", sourceDelta: -1, want: domain.MemoryRecallProjectionStale},
		{name: "as_of", version: "claim-states-v1", asOfDelta: -(5*time.Minute + time.Microsecond), want: domain.MemoryRecallProjectionStale},
		{name: "dependency", version: "claim-states-v1", wrongDependency: true, want: domain.MemoryRecallProjectionProvenanceUnknown},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			fixture := newMemoryClaimMutationFixture(t)
			uow, metadata := fixture.beginUoW(t)
			defer func() { _ = uow.tx.Rollback() }()
			head, err := canonical.NewCommitSeq(metadata.CommitSeq.Int64() - 1)
			if err != nil {
				t.Fatal(err)
			}
			source := head.Int64() + test.sourceDelta
			asOf := metadata.CommittedAt.UnixMicro() + test.asOfDelta.Microseconds()
			if _, err := uow.tx.Exec(`INSERT INTO projection_watermarks(
				projection_name, resident_id, projection_version, source_commit_seq, as_of, as_of_tz
			) VALUES (?, ?, ?, ?, ?, ?)`, projection.ClaimStatesName, fixture.residentID.String(),
				test.version, source, asOf, metadata.CommittedTZ.String()); err != nil {
				t.Fatal(err)
			}
			dependency := fixture.policyID
			if test.wrongDependency {
				dependency = fixture.newID(t)
			}
			if _, err := uow.tx.Exec(`INSERT INTO projection_watermark_dependencies(
				projection_name, resident_id, dependency_kind, dependency_version_id
			) VALUES (?, ?, ?, ?)`, projection.ClaimStatesName, fixture.residentID.String(),
				projection.MemoryPolicyDependency, dependency.String()); err != nil {
				t.Fatal(err)
			}
			reason, err := uow.inspectRecallProjection(
				context.Background(), fixture.residentID, fixture.policyID, head,
				metadata.CommittedAt, projection.ClaimStatesDefinition(),
			)
			if err != nil {
				t.Fatal(err)
			}
			if reason != test.want {
				t.Fatalf("reason = %q, want %q", reason, test.want)
			}
		})
	}
}

func TestRecallProjectionWithZeroDeclaredDependenciesRejectsUnexpectedRow(t *testing.T) {
	fixture := newMemoryClaimMutationFixture(t)
	uow, metadata := fixture.beginUoW(t)
	defer func() { _ = uow.tx.Rollback() }()
	head, err := canonical.NewCommitSeq(metadata.CommitSeq.Int64() - 1)
	if err != nil {
		t.Fatal(err)
	}
	definition := projection.ClaimViewScopeCurrentDefinition()
	if _, err := uow.tx.Exec(`INSERT INTO projection_watermarks(
		projection_name, resident_id, projection_version, source_commit_seq, as_of, as_of_tz
	) VALUES (?, ?, ?, ?, ?, ?)`, definition.Name, fixture.residentID.String(), definition.Version,
		head.Int64(), metadata.CommittedAt.UnixMicro(), metadata.CommittedTZ.String()); err != nil {
		t.Fatal(err)
	}
	if _, err := uow.tx.Exec(`INSERT INTO projection_watermark_dependencies(
		projection_name, resident_id, dependency_kind, dependency_version_id
	) VALUES (?, ?, ?, ?)`, definition.Name, fixture.residentID.String(),
		projection.MemoryPolicyDependency, fixture.policyID.String()); err != nil {
		t.Fatal(err)
	}
	reason, err := uow.inspectRecallProjection(
		context.Background(), fixture.residentID, fixture.policyID, head,
		metadata.CommittedAt, definition,
	)
	if err != nil {
		t.Fatal(err)
	}
	if reason != domain.MemoryRecallProjectionProvenanceUnknown {
		t.Fatalf("unexpected zero-dependency row reason = %q", reason)
	}
}

func TestLoadRecallCandidatesMarksOnlyAbstractsRelationSourceAsAbstract(t *testing.T) {
	fixture := newMemoryClaimMutationFixture(t)
	eventID := fixture.seedEvent(t, memory.EventUserMessage, fixture.ownerPrincipal, memory.TrustTrusted)
	sourceClaimID, _ := fixture.seedClaim(
		t, memory.ClaimKindOther, fixture.ownerPrincipal, fixture.residentPrincipal,
		[]seededClaimEvidence{trustedEvidence(
			eventID, memory.EventUserMessage, fixture.ownerPrincipal, memory.PolaritySupport,
		)}, memory.StageFloating,
	)
	abstractClaimID, _ := fixture.seedClaim(
		t, memory.ClaimKindOther, fixture.ownerPrincipal, fixture.residentPrincipal,
		[]seededClaimEvidence{trustedEvidence(
			eventID, memory.EventUserMessage, fixture.ownerPrincipal, memory.PolaritySupport,
		)}, memory.StageFloating,
	)
	addProjection := func(claimID canonical.ID) {
		t.Helper()
		mustExec(t, fixture.semantic.db, `INSERT INTO claim_states(
			claim_id, resident_id, stage, status, salience, confidence, currentness,
			temporal_relation, last_referenced_at, evidence_count
		) VALUES (?, ?, 'floating', 'active', 1, 1000000, 1000000, 'current', NULL, 1)`,
			claimID.String(), fixture.residentID.String())
		mustExec(t, fixture.semantic.db, `INSERT INTO claim_view_scope_current(
			claim_id, resident_id, view_scope, source_assertion_id
		) VALUES (?, ?, 'resident_ui', ?)`, claimID.String(), fixture.residentID.String(),
			fixture.newID(t).String())
	}
	addProjection(claimMutationParseID(t, fixture.semantic.claim["A"]))
	addProjection(sourceClaimID)
	addProjection(abstractClaimID)
	mustExec(t, fixture.semantic.db, `INSERT INTO claim_relations(
		claim_relation_id, canonical_commit_id, from_claim_id, to_claim_id,
		relation_type, reason_code, reason_content_id, generation_run_id,
		occurred_at, occurred_tz, recorded_at, recorded_tz
	) VALUES (?, ?, ?, ?, 'abstracts', 'test_abstraction', NULL, NULL, ?, ?, ?, ?)`,
		fixture.newID(t).String(), fixture.semantic.commit["A"], abstractClaimID.String(), sourceClaimID.String(),
		semanticTime, semanticTZ, semanticTime, semanticTZ)

	uow, metadata := fixture.beginUoW(t)
	defer func() { _ = uow.tx.Rollback() }()
	head, err := canonical.NewCommitSeq(metadata.CommitSeq.Int64() - 1)
	if err != nil {
		t.Fatal(err)
	}
	candidates, err := uow.loadRecallCandidates(
		context.Background(), fixture.residentID, head, memory.DefaultPolicyV2(),
	)
	if err != nil {
		t.Fatalf("loadRecallCandidates: %v", err)
	}
	byClaimID := make(map[canonical.ID]memory.RecallCandidate, len(candidates))
	for _, candidate := range candidates {
		byClaimID[candidate.ClaimID] = candidate
	}
	abstractCandidate, found := byClaimID[abstractClaimID]
	if !found {
		t.Fatalf("abstract claim %s missing from Recall candidates", abstractClaimID)
	}
	if !abstractCandidate.Abstract {
		t.Fatalf("abstracts relation source %s was not marked abstract", abstractClaimID)
	}
	sourceCandidate, found := byClaimID[sourceClaimID]
	if !found {
		t.Fatalf("source claim %s missing from Recall candidates", sourceClaimID)
	}
	if sourceCandidate.Abstract {
		t.Fatalf("abstracts relation target %s was incorrectly marked abstract", sourceClaimID)
	}
	if sourceCandidate.Currentness.Millionths() != 1_000_000 {
		t.Fatalf("source currentness = %s, want 1000000", sourceCandidate.Currentness)
	}
	if sourceCandidate.LastConfirmed == nil || sourceCandidate.LastConfirmed.UnixMicro() != semanticTime {
		t.Fatalf("source last confirmed = %v, want %d", sourceCandidate.LastConfirmed, semanticTime)
	}
}

func TestLoadRecallCandidatesUsesPinnedCurrentnessAndLatestSupportRecordedAt(t *testing.T) {
	fixture := newMemoryClaimMutationFixture(t)
	firstEvent := fixture.seedEvent(t, memory.EventUserMessage, fixture.ownerPrincipal, memory.TrustTrusted)
	secondEvent := fixture.seedEvent(t, memory.EventUserMessage, fixture.ownerPrincipal, memory.TrustTrusted)
	contradictEvent := fixture.seedEvent(t, memory.EventUserMessage, fixture.ownerPrincipal, memory.TrustTrusted)
	claimID, evidenceIDs := fixture.seedClaim(
		t, memory.ClaimKindOther, fixture.ownerPrincipal, fixture.residentPrincipal,
		[]seededClaimEvidence{
			trustedEvidence(firstEvent, memory.EventUserMessage, fixture.ownerPrincipal, memory.PolaritySupport),
		}, memory.StageFloating,
	)
	contradictOnlyID, _ := fixture.seedClaim(
		t, memory.ClaimKindOther, fixture.ownerPrincipal, fixture.residentPrincipal,
		[]seededClaimEvidence{
			trustedEvidence(contradictEvent, memory.EventUserMessage, fixture.ownerPrincipal, memory.PolarityContradict),
		}, memory.StageFloating,
	)
	appendEvidence := func(eventID canonical.ID, polarity memory.EvidencePolarity) canonical.Instant {
		t.Helper()
		uow, metadata := fixture.beginUoW(t)
		evidenceID := fixture.newID(t)
		if _, err := uow.tx.Exec(`INSERT INTO claim_evidence(
			evidence_id, canonical_commit_id, claim_id, event_id, polarity, grade,
			trust_level, weight, derivation, source_evidence_id, memory_policy_revision_id,
			created_by_run_id, reason_code, reason_content_id, recorded_at, recorded_tz
		) SELECT ?, ?, claim_id, ?, ?, grade, trust_level, weight, derivation,
			source_evidence_id, memory_policy_revision_id, created_by_run_id, reason_code,
			reason_content_id, ?, ? FROM claim_evidence WHERE evidence_id = ?`,
			evidenceID.String(), metadata.CommitID.String(), eventID.String(), string(polarity),
			metadata.CommittedAt.UnixMicro(), metadata.CommittedTZ.String(), evidenceIDs[0].String()); err != nil {
			_ = uow.tx.Rollback()
			t.Fatal(err)
		}
		if err := uow.tx.Commit(); err != nil {
			t.Fatal(err)
		}
		return metadata.CommittedAt
	}
	latestSupportAt := appendEvidence(secondEvent, memory.PolaritySupport)
	_ = appendEvidence(contradictEvent, memory.PolarityContradict)

	addState := func(id canonical.ID, currentness int64, relation string, evidenceCount int64) {
		t.Helper()
		mustExec(t, fixture.semantic.db, `INSERT INTO claim_states(
			claim_id, resident_id, stage, status, salience, confidence, currentness,
			temporal_relation, last_referenced_at, evidence_count
		) VALUES (?, ?, 'floating', 'active', 1, 750000, ?, ?, NULL, ?)`,
			id.String(), fixture.residentID.String(), currentness, relation, evidenceCount)
		mustExec(t, fixture.semantic.db, `INSERT INTO claim_view_scope_current(
			claim_id, resident_id, view_scope, source_assertion_id
		) VALUES (?, ?, 'resident_ui', ?)`, id.String(), fixture.residentID.String(), fixture.newID(t).String())
	}
	addState(claimMutationParseID(t, fixture.semantic.claim["A"]), 1_000_000, "current", 1)
	addState(claimID, 345_678, "stale_unknown", 3)
	addState(contradictOnlyID, 400_000, "stale_unknown", 1)

	uow, metadata := fixture.beginUoW(t)
	defer func() { _ = uow.tx.Rollback() }()
	head, err := canonical.NewCommitSeq(metadata.CommitSeq.Int64() - 1)
	if err != nil {
		t.Fatal(err)
	}
	// A later SUPPORT row in the Writer transaction is above the pinned head
	// and must not alter this Assembly Target's confirmation time.
	futureEvidenceID := fixture.newID(t)
	if _, err := uow.tx.Exec(`INSERT INTO claim_evidence(
		evidence_id, canonical_commit_id, claim_id, event_id, polarity, grade,
		trust_level, weight, derivation, source_evidence_id, memory_policy_revision_id,
		created_by_run_id, reason_code, reason_content_id, recorded_at, recorded_tz
	) SELECT ?, ?, claim_id, event_id, polarity, grade, trust_level, weight,
		derivation, source_evidence_id, memory_policy_revision_id, created_by_run_id,
		reason_code, reason_content_id, ?, recorded_tz
		FROM claim_evidence WHERE evidence_id = ?`, futureEvidenceID.String(), metadata.CommitID.String(),
		semanticTime+40, evidenceIDs[0].String()); err != nil {
		t.Fatal(err)
	}

	candidates, err := uow.loadRecallCandidates(
		context.Background(), fixture.residentID, head, memory.DefaultPolicyV2(),
	)
	if err != nil {
		t.Fatalf("loadRecallCandidates: %v", err)
	}
	byClaimID := make(map[canonical.ID]memory.RecallCandidate, len(candidates))
	for _, candidate := range candidates {
		byClaimID[candidate.ClaimID] = candidate
	}
	candidate, found := byClaimID[claimID]
	if !found {
		t.Fatalf("candidate %s missing", claimID)
	}
	if candidate.Currentness.Millionths() != 345_678 || candidate.TemporalRelation != memory.RelationStaleUnknown {
		t.Fatalf("candidate temporal display state = %s/%s", candidate.Currentness, candidate.TemporalRelation)
	}
	if candidate.LastConfirmed == nil || *candidate.LastConfirmed != latestSupportAt {
		t.Fatalf("last confirmed = %v, want %s", candidate.LastConfirmed, latestSupportAt)
	}
	contradictOnly, found := byClaimID[contradictOnlyID]
	if !found {
		t.Fatalf("contradict-only candidate %s missing", contradictOnlyID)
	}
	if contradictOnly.LastConfirmed != nil {
		t.Fatalf("contradict-only last confirmed = %v, want nil", contradictOnly.LastConfirmed)
	}
}
