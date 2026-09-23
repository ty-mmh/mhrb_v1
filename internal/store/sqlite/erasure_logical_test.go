package sqlite

import (
	"context"
	"slices"
	"testing"

	"mahoroba.local/mahoroba/internal/canonical"
	"mahoroba.local/mahoroba/internal/erasure"
)

func TestM7ErasureLogicalProvenanceSQLRegistryCoversTwelveRules(t *testing.T) {
	want := []string{
		"claim_evidence_event_provenance_v1",
		"claim_evidence_source_chain_v1",
		"claim_relation_from_v1",
		"claim_relation_to_v1",
		"claim_stage_dependency_v1",
		"claim_status_trigger_event_v1",
		"claim_status_trigger_evidence_v1",
		"claim_status_trigger_finding_v1",
		"claim_status_trigger_relation_v1",
		"claim_usage_claim_v1",
		"claim_validity_evidence_event_v1",
		"generation_recall_v1",
	}
	got := make([]string, 0, len(erasureLogicalReferenceQueries))
	seenTuples := map[string]struct{}{}
	for _, descriptor := range erasureLogicalReferenceQueries {
		got = append(got, descriptor.rule)
		key := descriptor.rule + "\x00" + descriptor.kind + "\x00" + descriptor.field
		if _, duplicate := seenTuples[key]; duplicate {
			t.Fatalf("duplicate logical descriptor %s", key)
		}
		seenTuples[key] = struct{}{}
		if descriptor.query == "" {
			t.Fatalf("logical descriptor %s has no production query", descriptor.rule)
		}
	}
	slices.Sort(got)
	if !slices.Equal(got, want) {
		t.Fatalf("logical SQL registry=%v, want %v", got, want)
	}
}

func TestM7ErasureLogicalProvenanceMalformedSourceChainsBlockFailClosed(t *testing.T) {
	tests := []struct {
		name        string
		corrupt     func(*testing.T, *semanticFixture)
		wantBlocked bool
	}{
		{
			name: "direct_extracted_terminal_control",
			corrupt: func(t *testing.T, fixture *semanticFixture) {
				targetClaim := addMinimumCheckSharedClaimAlias(t, fixture, fixture.claim["A"])
				source := fixture.evidence["A"]
				cloneM7LogicalEvidenceForClaim(t, fixture, fixture.ids.new(), targetClaim, "inherited", &source)
			},
			wantBlocked: false,
		},
		{
			name: "missing_terminal",
			corrupt: func(t *testing.T, fixture *semanticFixture) {
				missing := fixture.ids.new()
				mustExec(t, fixture.db, "PRAGMA foreign_keys=OFF")
				mustExec(t, fixture.db, "DROP TRIGGER trg_claim_evidence_no_update")
				mustExec(t, fixture.db, `UPDATE claim_evidence
					SET derivation='inherited',source_evidence_id=? WHERE evidence_id=?`,
					missing, fixture.evidence["A"])
				mustExec(t, fixture.db, "PRAGMA foreign_keys=ON")
			},
			wantBlocked: true,
		},
		{
			name: "cycle",
			corrupt: func(t *testing.T, fixture *semanticFixture) {
				second := fixture.ids.new()
				cloneM7LogicalEvidence(t, fixture, second, "extracted", nil)
				mustExec(t, fixture.db, "DROP TRIGGER trg_claim_evidence_no_update")
				mustExec(t, fixture.db, `UPDATE claim_evidence
					SET derivation='inherited',source_evidence_id=? WHERE evidence_id=?`,
					second, fixture.evidence["A"])
				mustExec(t, fixture.db, `UPDATE claim_evidence
					SET derivation='inherited',source_evidence_id=? WHERE evidence_id=?`,
					fixture.evidence["A"], second)
			},
			wantBlocked: true,
		},
		{
			name: "inherited_to_inherited",
			corrupt: func(t *testing.T, fixture *semanticFixture) {
				terminal := fixture.evidence["A"]
				middle := fixture.ids.new()
				cloneM7LogicalEvidence(t, fixture, middle, "inherited", &terminal)
				top := fixture.ids.new()
				cloneM7LogicalEvidence(t, fixture, top, "inherited", &middle)
			},
			wantBlocked: true,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			fixture, closeFixture := newSemanticFixture(t)
			t.Cleanup(closeFixture)
			test.corrupt(t, fixture)

			baseSeq, err := canonical.NewCommitSeq(3)
			if err != nil {
				t.Fatal(err)
			}
			result, err := (&Store{reader: fixture.db}).ErasureSource(nil).CaptureErasureLogicalProvenance(
				context.Background(), erasure.LogicalProvenanceRequest{
					ResidentID:        mustCanonicalID(t, fixture.resident["A"]),
					BaseHeadCommitID:  mustCanonicalID(t, fixture.commit["B"]),
					BaseHeadCommitSeq: baseSeq,
					Targets: []erasure.LogicalErasureTarget{{
						ContentID:      mustCanonicalID(t, fixture.content["A"]["description"]),
						ErasureEventID: mustCanonicalID(t, fixture.ids.new()),
					}},
				})
			if err != nil {
				t.Fatal(err)
			}
			if !test.wantBlocked {
				if len(result.Blockers) != 0 {
					t.Fatalf("direct extracted terminal was blocked: %+v", result.Blockers)
				}
				return
			}
			if len(result.Blockers) != 1 {
				t.Fatalf("source chain blockers=%+v", result.Blockers)
			}
			blocker := result.Blockers[0]
			if blocker.Code != "preexisting_integrity_break" || blocker.TargetKind != "claim" ||
				blocker.TargetID == nil || *blocker.TargetID != fixture.claim["A"] ||
				blocker.TargetField == nil || *blocker.TargetField != "provenance" ||
				!slices.Equal(blocker.RequiredActionCodes, []string{"run_integrity_scan"}) {
				t.Fatalf("malformed source chain was not a typed blocker: %+v", blocker)
			}
		})
	}
}

func cloneM7LogicalEvidence(t *testing.T, fixture *semanticFixture, evidenceID, derivation string, sourceEvidenceID *string) {
	cloneM7LogicalEvidenceForClaim(t, fixture, evidenceID, fixture.claim["A"], derivation, sourceEvidenceID)
}

func cloneM7LogicalEvidenceForClaim(t *testing.T, fixture *semanticFixture, evidenceID, claimID, derivation string, sourceEvidenceID *string) {
	t.Helper()
	var source any
	if sourceEvidenceID != nil {
		source = *sourceEvidenceID
	}
	result, err := fixture.db.Exec(`INSERT INTO claim_evidence(
		evidence_id,canonical_commit_id,claim_id,event_id,polarity,grade,trust_level,weight,
		derivation,source_evidence_id,memory_policy_revision_id,created_by_run_id,reason_code,
		reason_content_id,recorded_at,recorded_tz
	) SELECT ?,canonical_commit_id,?,event_id,polarity,grade,trust_level,weight,
		?, ?,memory_policy_revision_id,created_by_run_id,reason_code,reason_content_id,recorded_at,recorded_tz
	FROM claim_evidence WHERE evidence_id=?`, evidenceID, claimID, derivation, source, fixture.evidence["A"])
	if err != nil {
		t.Fatal(err)
	}
	if rows, err := result.RowsAffected(); err != nil || rows != 1 {
		t.Fatalf("clone evidence rows=%d error=%v", rows, err)
	}
}
