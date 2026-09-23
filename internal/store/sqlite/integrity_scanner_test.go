package sqlite

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"testing"

	"mahoroba.local/mahoroba/internal/canonical"
	"mahoroba.local/mahoroba/internal/domain"
	"mahoroba.local/mahoroba/internal/integrity"
	"mahoroba.local/mahoroba/internal/memory"
)

func TestM7IntegrityPipelineV1RegistrationIsExactAndIdempotent(t *testing.T) {
	t.Run("exact replay", func(t *testing.T) {
		fixture, closeFixture := newSemanticFixture(t)
		defer closeFixture()
		pipelineID := mustIntegrityID(t, "00000000000000000000000701")
		definitions, err := domain.IntegrityPipelineDefinition(pipelineID)
		if err != nil {
			t.Fatal(err)
		}
		metadata := insertIntegrityTestCommit(t, fixture.db, fixture.ids.new(), 100, nil)
		tx, err := fixture.db.BeginTx(context.Background(), nil)
		if err != nil {
			t.Fatal(err)
		}
		uow := &canonicalUoW{tx: tx, metadata: metadata}
		if err := uow.RegisterPipelineVersions(context.Background(), definitions); err != nil {
			t.Fatal(err)
		}
		if err := tx.Commit(); err != nil {
			t.Fatal(err)
		}
		var definition string
		if err := fixture.db.QueryRow(`SELECT definition FROM pipeline_versions
			WHERE pipeline_version_id = ?`, pipelineID.String()).Scan(&definition); err != nil {
			t.Fatal(err)
		}
		if definition != integrityPipelineDefinitionV1 {
			t.Fatalf("definition = %q, want exact %q", definition, integrityPipelineDefinitionV1)
		}

		tx, err = fixture.db.BeginTx(context.Background(), nil)
		if err != nil {
			t.Fatal(err)
		}
		uow = &canonicalUoW{tx: tx, metadata: metadata}
		if err := uow.RegisterPipelineVersions(context.Background(), definitions); !errors.Is(err, canonical.ErrNoMutation) {
			t.Fatalf("exact replay = %v, want ErrNoMutation", err)
		}
		_ = tx.Rollback()
	})

	t.Run("different bytes conflict", func(t *testing.T) {
		fixture, closeFixture := newSemanticFixture(t)
		defer closeFixture()
		pipelineID := mustIntegrityID(t, "00000000000000000000000702")
		mustExec(t, fixture.db, `INSERT INTO pipeline_versions(
			pipeline_version_id, canonical_commit_id, pipeline_kind, version_key,
			definition, recorded_at, recorded_tz
		) VALUES (?, ?, 'integrity_check', 'integrity-check-v1', ?, ?, ?)`,
			pipelineID.String(), fixture.commit["global"], `{ "version": "integrity-check-v1" }`,
			semanticTime, semanticTZ)
		definitions, err := domain.IntegrityPipelineDefinition(mustIntegrityID(t, "00000000000000000000000703"))
		if err != nil {
			t.Fatal(err)
		}
		tx, err := fixture.db.BeginTx(context.Background(), nil)
		if err != nil {
			t.Fatal(err)
		}
		uow := &canonicalUoW{tx: tx, metadata: canonical.CommitMetadata{
			CommitID: mustIntegrityID(t, fixture.commit["global"]), CommitSeq: mustCommitSeq(t, 1),
			Scope: canonical.GlobalScope(), CommittedAt: semanticInstant(semanticTime),
			CommittedTZ: canonical.MustTimezone(semanticTZ),
		}}
		if err := uow.RegisterPipelineVersions(context.Background(), definitions); err == nil {
			t.Fatal("non-canonical pipeline bytes were accepted as an exact replay")
		}
		_ = tx.Rollback()
	})
}

func TestM7IntegrityScannerRecordsInitialKindsAndDeduplicates(t *testing.T) {
	ctx := context.Background()
	evidence := performM7ClaimErasure(t)
	residentID, ok := evidence.Metadata.Scope.ResidentID()
	if !ok {
		t.Fatal("erasure fixture is not resident scoped")
	}
	pipelineID := mustIntegrityID(t, "00000000000000000000000710")
	memoryStatusID := mustIntegrityID(t, "00000000000000000000000709")
	insertExactIntegrityPipeline(t, evidence.DB, pipelineID)
	insertExactMemoryStatusPipeline(t, evidence.DB, memoryStatusID)

	var runID, sourceCommitID, presentInputContent string
	if err := evidence.DB.QueryRow(`SELECT generation_run_id, canonical_commit_id
		FROM generation_runs WHERE resident_id = ? ORDER BY generation_run_id LIMIT 1`, residentID.String()).
		Scan(&runID, &sourceCommitID); err != nil {
		t.Fatal(err)
	}
	if err := evidence.DB.QueryRow(`SELECT content_id FROM content_objects
		WHERE owner_resident_id = ? AND content_class = 'generation_input' AND erasure_state = 'present'
		ORDER BY content_id LIMIT 1`, residentID.String()).Scan(&presentInputContent); err != nil {
		t.Fatal(err)
	}
	missingInputID := mustIntegrityID(t, "00000000000000000000000711")
	erasedInputID := mustIntegrityID(t, "00000000000000000000000712")
	missingSourceID := mustIntegrityID(t, "00000000000000000000000713")
	mustExec(t, evidence.DB, `INSERT INTO generation_run_inputs(
		generation_run_input_id, canonical_commit_id, generation_run_id, ordinal, role,
		source_type, source_id, inclusion_mode, content_id, recorded_at, recorded_tz
	) VALUES (?, ?, ?, 0, 'user', 'event', ?, 'current_input', ?, ?, ?)`,
		missingInputID.String(), sourceCommitID, runID, missingSourceID.String(), presentInputContent,
		semanticTime, semanticTZ)
	mustExec(t, evidence.DB, `INSERT INTO generation_run_inputs(
		generation_run_input_id, canonical_commit_id, generation_run_id, ordinal, role,
		source_type, source_id, inclusion_mode, content_id, recorded_at, recorded_tz
	) VALUES (?, ?, ?, 1, 'system', 'runtime_projection', NULL, 'runtime_projection', ?, ?, ?)`,
		erasedInputID.String(), sourceCommitID, runID, evidence.Content, semanticTime, semanticTZ)
	mustExec(t, evidence.DB, `INSERT INTO generation_run_outcomes(
		outcome_id, canonical_commit_id, generation_run_id, attempt_no, state,
		output_content_id, prompt_tokens, completion_tokens, latency, estimated_cost,
		error_class, error_detail_content_id, recorded_at, recorded_tz
	) VALUES (?, ?, ?, 1, 'running', NULL, NULL, NULL, NULL, NULL, NULL, NULL, ?, ?)`,
		mustIntegrityID(t, "00000000000000000000000714").String(), sourceCommitID, runID,
		semanticTime, semanticTZ)

	scan, err := newSQLiteIntegrityScanner(evidence.DB).Scan(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(scan.Candidates) != 3 {
		t.Fatalf("candidate count = %d, want one for each initial finding kind: %+v", len(scan.Candidates), scan.Candidates)
	}
	kinds := make(map[integrity.FindingKind]int)
	for _, candidate := range scan.Candidates {
		kinds[candidate.Kind]++
	}
	for _, kind := range []integrity.FindingKind{
		integrity.FindingRequiredProvenanceErased,
		integrity.FindingProvenanceUnresolvable,
		integrity.FindingCanonicalInvariant,
	} {
		if kinds[kind] != 1 {
			t.Fatalf("kind %s count = %d, want 1", kind, kinds[kind])
		}
	}

	first := recordIntegrityScan(t, evidence.DB, residentID, pipelineID, memoryStatusID, scan, 101, 720)
	if len(first.Created) != 3 || len(first.Existing) != 0 {
		t.Fatalf("first record result = %+v", first)
	}
	if len(first.Quarantined) != 1 {
		t.Fatalf("first quarantine result = %+v, want one active structural claim", first.Quarantined)
	}
	var findings, details, fingerprints int
	if err := evidence.DB.QueryRow(`SELECT COUNT(*), COUNT(details_content_id), COUNT(finding_fingerprint)
		FROM integrity_findings WHERE pipeline_version_id = ?`, pipelineID.String()).
		Scan(&findings, &details, &fingerprints); err != nil {
		t.Fatal(err)
	}
	if findings != 3 || details != 0 || fingerprints != 3 {
		t.Fatalf("persisted findings=%d details=%d fingerprints=%d", findings, details, fingerprints)
	}

	second := recordIntegrityScanNoMutation(t, evidence.DB, residentID, pipelineID, memoryStatusID, scan, 102, 730)
	if len(second.Existing) != 3 || len(second.Created) != 0 {
		t.Fatalf("idempotent record result = %+v", second)
	}
	for _, existing := range second.Existing {
		if existing.Commit.CommitID.String() != firstCommitID(101) ||
			existing.Commit.CommitSeq.Int64() != 101 {
			t.Fatalf("existing finding commit evidence = %+v", existing.Commit)
		}
	}
	assertIntegrityFindingCount(t, evidence.DB, pipelineID, 3)

	// A later scan containing existing exact candidates and one missing
	// candidate must commit only the missing row.
	additionalInputID := mustIntegrityID(t, "00000000000000000000000740")
	mustExec(t, evidence.DB, `INSERT INTO generation_run_inputs(
		generation_run_input_id, canonical_commit_id, generation_run_id, ordinal, role,
		source_type, source_id, inclusion_mode, content_id, recorded_at, recorded_tz
	) VALUES (?, ?, ?, 2, 'user', 'event', ?, 'live_context', ?, ?, ?)`,
		additionalInputID.String(), firstCommitID(101), runID,
		mustIntegrityID(t, "00000000000000000000000741").String(), presentInputContent,
		semanticTime+1, semanticTZ)
	mixedScan, err := newSQLiteIntegrityScanner(evidence.DB).Scan(ctx)
	if err != nil {
		t.Fatal(err)
	}
	mixed := recordIntegrityScan(t, evidence.DB, residentID, pipelineID, memoryStatusID, mixedScan, 102, 750)
	if len(mixed.Existing) != 3 || len(mixed.Created) != 1 {
		t.Fatalf("mixed record result = %+v", mixed)
	}
	assertIntegrityFindingCount(t, evidence.DB, pipelineID, 4)
}

func TestM7IntegrityFindingSemanticConflictRollsBackBatch(t *testing.T) {
	ctx := context.Background()
	evidence := performM7ClaimErasure(t)
	residentID, _ := evidence.Metadata.Scope.ResidentID()
	pipelineID := mustIntegrityID(t, "00000000000000000000000760")
	memoryStatusID := mustIntegrityID(t, "00000000000000000000000759")
	insertExactIntegrityPipeline(t, evidence.DB, pipelineID)
	insertExactMemoryStatusPipeline(t, evidence.DB, memoryStatusID)
	scan, err := newSQLiteIntegrityScanner(evidence.DB).Scan(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(scan.Candidates) != 1 {
		t.Fatalf("candidate count = %d, want 1", len(scan.Candidates))
	}
	_ = recordIntegrityScan(t, evidence.DB, residentID, pipelineID, memoryStatusID, scan, 101, 770)
	assertIntegrityFindingCount(t, evidence.DB, pipelineID, 1)

	// Corrupt only this private fixture to model a persisted SHA collision or
	// semantic mismatch. Production append-only triggers remain unchanged.
	mustExec(t, evidence.DB, `DROP TRIGGER trg_integrity_findings_no_update`)
	mustExec(t, evidence.DB, `UPDATE integrity_findings SET target_field = 'qualifying_support'
		WHERE pipeline_version_id = ?`, pipelineID.String())
	var runID, presentInputContent string
	if err := evidence.DB.QueryRow(`SELECT generation_run_id FROM generation_runs
		WHERE resident_id = ? ORDER BY generation_run_id LIMIT 1`, residentID.String()).Scan(&runID); err != nil {
		t.Fatal(err)
	}
	if err := evidence.DB.QueryRow(`SELECT content_id FROM content_objects
		WHERE owner_resident_id = ? AND content_class = 'generation_input' AND erasure_state = 'present'
		ORDER BY content_id LIMIT 1`, residentID.String()).Scan(&presentInputContent); err != nil {
		t.Fatal(err)
	}
	mustExec(t, evidence.DB, `INSERT INTO generation_run_inputs(
		generation_run_input_id, canonical_commit_id, generation_run_id, ordinal, role,
		source_type, source_id, inclusion_mode, content_id, recorded_at, recorded_tz
	) VALUES (?, ?, ?, 0, 'user', 'event', ?, 'current_input', ?, ?, ?)`,
		mustIntegrityID(t, "00000000000000000000000780").String(), firstCommitID(101), runID,
		mustIntegrityID(t, "00000000000000000000000781").String(), presentInputContent,
		semanticTime, semanticTZ)
	conflictingScan, err := newSQLiteIntegrityScanner(evidence.DB).Scan(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(conflictingScan.Candidates) != 2 {
		t.Fatalf("conflict batch candidate count = %d, want existing conflict plus missing", len(conflictingScan.Candidates))
	}
	beforeCommits := countIntegrityRows(t, evidence.DB, `SELECT COUNT(*) FROM canonical_commits`)

	metadata := insertIntegrityTestCommitTx(t, evidence.DB, 102, &residentID)
	tx := metadata.tx
	uow := &canonicalUoW{tx: tx, metadata: metadata.metadata}
	value := plannedIntegrityRecord(t, residentID, pipelineID, memoryStatusID, conflictingScan, 790)
	_, err = uow.RecordIntegrityFindings(ctx, value)
	if !errors.Is(err, integrity.ErrFindingConflict) {
		t.Fatalf("semantic conflict error = %v, want ErrFindingConflict", err)
	}
	_ = tx.Rollback()
	if got := countIntegrityRows(t, evidence.DB, `SELECT COUNT(*) FROM canonical_commits`); got != beforeCommits {
		t.Fatalf("conflict changed commit count from %d to %d", beforeCommits, got)
	}
	assertIntegrityFindingCount(t, evidence.DB, pipelineID, 1)
}

func TestM7QualifyingSupportHistoryReplayMatrix(t *testing.T) {
	t.Run("first attributable erasure is stable across restart and later contradiction", func(t *testing.T) {
		fixture, closeFixture := newSemanticFixture(t)
		defer closeFixture()
		eraseFixtureEventContent(t, fixture, "A")

		first, err := newSQLiteIntegrityScanner(fixture.db).Scan(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		candidate := requireIntegrityRule(t, first, integrity.RuleClaimQualifyingSupportErased)
		if candidate.SourceContentErasureEventID == nil ||
			candidate.SourceContentErasureEventID.String() != fixture.erasure["A"] {
			t.Fatalf("qualifying-support source = %v, want first erasure %s",
				candidate.SourceContentErasureEventID, fixture.erasure["A"])
		}

		// A contradiction is semantic input, not qualifying support and not an
		// integrity failure. Adding it after the break cannot move the v1
		// fingerprint to a later or different cause.
		mustExec(t, fixture.db, `INSERT INTO claim_evidence(
			evidence_id, canonical_commit_id, claim_id, event_id, polarity, grade,
			trust_level, weight, derivation, source_evidence_id,
			memory_policy_revision_id, created_by_run_id, reason_code,
			reason_content_id, recorded_at, recorded_tz
		) VALUES (?, ?, ?, ?, 'contradict', 'stated', 'trusted', 1000000,
			'extracted', NULL, ?, ?, ?, NULL, ?, ?)`,
			fixture.ids.new(), fixture.commit["A"], fixture.claim["A"], fixture.event["A"],
			fixture.revision["A"]["memory_policy"], fixture.run["A"],
			string(memory.EvidenceReasonSourceStated), semanticTime+1, semanticTZ)
		second, err := newSQLiteIntegrityScanner(fixture.db).Scan(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		restarted := requireIntegrityRule(t, second, integrity.RuleClaimQualifyingSupportErased)
		if restarted.Fingerprint != candidate.Fingerprint {
			t.Fatalf("restart fingerprint = %s, want stable %s", restarted.Fingerprint, candidate.Fingerprint)
		}
	})

	t.Run("another valid support survives", func(t *testing.T) {
		fixture, closeFixture := newSemanticFixture(t)
		defer closeFixture()
		addSecondQualifyingSupport(t, fixture, "A")
		eraseFixtureEventContent(t, fixture, "A")
		scan, err := newSQLiteIntegrityScanner(fixture.db).Scan(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		assertIntegrityRuleAbsent(t, scan, integrity.RuleClaimQualifyingSupportErased)
		assertIntegrityRuleAbsent(t, scan, integrity.RuleClaimQualifyingSupportUnresolvable)
	})

	t.Run("broken before attributable erasure is unresolvable", func(t *testing.T) {
		fixture, closeFixture := newSemanticFixture(t)
		defer closeFixture()
		mustExec(t, fixture.db, "DROP TRIGGER trg_claim_evidence_no_update")
		mustExec(t, fixture.db, `UPDATE claim_evidence SET reason_code = 'source_inferred'
			WHERE evidence_id = ?`, fixture.evidence["A"])
		scan, err := newSQLiteIntegrityScanner(fixture.db).Scan(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		candidate := requireIntegrityRule(t, scan, integrity.RuleClaimQualifyingSupportUnresolvable)
		if candidate.SourceContentErasureEventID != nil {
			t.Fatalf("unresolvable support invented erasure source %s", candidate.SourceContentErasureEventID)
		}
	})

	t.Run("contradict only is not an integrity failure", func(t *testing.T) {
		fixture, closeFixture := newSemanticFixture(t)
		defer closeFixture()
		mustExec(t, fixture.db, "DROP TRIGGER trg_claim_evidence_no_update")
		mustExec(t, fixture.db, `UPDATE claim_evidence SET polarity = 'contradict'
			WHERE evidence_id = ?`, fixture.evidence["A"])
		scan, err := newSQLiteIntegrityScanner(fixture.db).Scan(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		assertIntegrityRuleAbsent(t, scan, integrity.RuleClaimQualifyingSupportErased)
		assertIntegrityRuleAbsent(t, scan, integrity.RuleClaimQualifyingSupportUnresolvable)
	})
}

func TestM7CancellationEnvelopeRuleRequiresUnresolvableFrozenHistory(t *testing.T) {
	tests := []struct {
		name             string
		resolvable       bool
		eraseLatestClass string
		zeroSessionGap   bool
		want             bool
	}{
		{name: "missing frozen dependencies", want: true},
		{name: "exact frozen dependencies", resolvable: true, want: false},
		{name: "latest principles bytes erased", resolvable: true, eraseLatestClass: "principles", want: true},
		{name: "latest persona bytes erased", resolvable: true, eraseLatestClass: "persona", want: true},
		{name: "zero session idle gap", resolvable: true, zeroSessionGap: true, want: true},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			fixture, closeFixture := newSemanticFixture(t)
			defer closeFixture()
			if test.resolvable {
				insertResolvableDialogueCancellationHistory(t, fixture, "A")
			}
			if test.eraseLatestClass != "" {
				insertLatestErasedCancellationRevision(t, fixture, "A", test.eraseLatestClass)
			}
			if test.zeroSessionGap {
				mustExec(t, fixture.db, `UPDATE sessionization_policy_versions
					SET definition = '{"idle_gap_microseconds":"0","version":"sessionization-v1"}'
					WHERE sessionization_policy_version_id = ?`, fixture.session)
			}
			eraseFixtureEventContent(t, fixture, "A")
			scan, err := newSQLiteIntegrityScanner(fixture.db).Scan(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			found := hasIntegrityRule(scan, integrity.RuleCancellationEnvelopeUnresolvable)
			if found != test.want {
				t.Fatalf("cancellation-envelope finding = %v, want %v; candidates=%+v", found, test.want, scan.Candidates)
			}
		})
	}
}

func insertLatestErasedCancellationRevision(
	t *testing.T,
	fixture *semanticFixture,
	residentKey, revisionClass string,
) {
	t.Helper()
	contentClass := map[string]string{
		"principles": "principles_text",
		"persona":    "persona_text",
	}[revisionClass]
	if contentClass == "" {
		t.Fatalf("unsupported cancellation revision class %q", revisionClass)
	}
	contentID := fixture.addContent(t, residentKey, contentClass,
		"latest-erased-"+revisionClass, "resident_only")
	revisionID := fixture.ids.new()
	revisionCommitID := fixture.ids.new()
	sourceCommitID := fixture.ids.new()
	mustExec(t, fixture.db, `INSERT INTO canonical_commits(
		canonical_commit_id, commit_seq, resident_id, committed_at, committed_tz
	) VALUES (?, 4, ?, ?, ?)`, revisionCommitID, fixture.resident[residentKey], semanticTime+3, semanticTZ)
	mustExec(t, fixture.db, `INSERT INTO canonical_commits(
		canonical_commit_id, commit_seq, resident_id, committed_at, committed_tz
	) VALUES (?, 5, ?, ?, ?)`, sourceCommitID, fixture.resident[residentKey], semanticTime+4, semanticTZ)
	mustExec(t, fixture.db, `INSERT INTO resident_revisions(
		revision_id, canonical_commit_id, resident_id, revision_class, content_id,
		parent_revision_id, created_by_run_id, reason_content_id, recorded_at, recorded_tz
	) VALUES (?, ?, ?, ?, ?, NULL, NULL, NULL, ?, ?)`,
		revisionID, revisionCommitID, fixture.resident[residentKey], revisionClass,
		contentID, semanticTime, semanticTZ)
	mustExec(t, fixture.db, `INSERT INTO resident_revision_activations(
		activation_id, canonical_commit_id, resident_id, revision_id,
		actor_principal_id, approval_id, reason_code, reason_content_id,
		recorded_at, recorded_tz
	) VALUES (?, ?, ?, ?, ?, NULL, 'test_latest', NULL, ?, ?)`,
		fixture.ids.new(), revisionCommitID, fixture.resident[residentKey],
		revisionID, fixture.principal["human"], semanticTime, semanticTZ)
	mustExec(t, fixture.db, "DROP TRIGGER trg_events_no_update")
	mustExec(t, fixture.db, `UPDATE events SET canonical_commit_id = ? WHERE event_id = ?`,
		sourceCommitID, fixture.event[residentKey])
	mustExec(t, fixture.db, `UPDATE content_objects
		SET erasure_state = 'erased', blob_hash = NULL, commitment_salt = NULL
		WHERE content_id = ?`, contentID)
}

func TestM7IntegrityApplyQuarantinesMatchingActiveClaimInSameCommit(t *testing.T) {
	evidence := performM7ClaimErasure(t)
	residentID, _ := evidence.Metadata.Scope.ResidentID()
	integrityPipelineID := mustIntegrityID(t, "00000000000000000000000801")
	memoryStatusID := mustIntegrityID(t, "00000000000000000000000802")
	insertExactIntegrityPipeline(t, evidence.DB, integrityPipelineID)
	insertExactMemoryStatusPipeline(t, evidence.DB, memoryStatusID)
	scan, err := newSQLiteIntegrityScanner(evidence.DB).Scan(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	result := recordIntegrityScan(t, evidence.DB, residentID, integrityPipelineID, memoryStatusID, scan, 101, 810)
	if len(result.Created) != 1 || len(result.Quarantined) != 1 {
		t.Fatalf("apply result = %+v, want one finding and one quarantine", result)
	}
	var findingCommit, statusCommit, claimRaw, fromStatus, toStatus, decisionKind string
	var triggerKind, triggerFinding, pipelineRaw sql.NullString
	if err := evidence.DB.QueryRow(`SELECT
		finding.canonical_commit_id, status.canonical_commit_id, status.claim_id,
		status.from_status, status.to_status, status.decision_kind,
		status.trigger_kind, status.trigger_integrity_finding_id, status.pipeline_version_id
	FROM integrity_findings finding
	JOIN claim_status_transitions status
	  ON status.trigger_integrity_finding_id = finding.integrity_finding_id
	WHERE finding.integrity_finding_id = ?`, result.Created[0].FindingID.String()).Scan(
		&findingCommit, &statusCommit, &claimRaw, &fromStatus, &toStatus, &decisionKind,
		&triggerKind, &triggerFinding, &pipelineRaw,
	); err != nil {
		t.Fatal(err)
	}
	if findingCommit != statusCommit || claimRaw != evidence.ClaimID ||
		fromStatus != "active" || toStatus != "quarantined" || decisionKind != "automatic" ||
		!triggerKind.Valid || triggerKind.String != "integrity_finding" ||
		!triggerFinding.Valid || triggerFinding.String != result.Created[0].FindingID.String() ||
		!pipelineRaw.Valid || pipelineRaw.String != memoryStatusID.String() {
		t.Fatalf("finding/quarantine UoW mismatch: commits=%s/%s claim=%s status=%s->%s kind=%s trigger=%v/%v pipeline=%v",
			findingCommit, statusCommit, claimRaw, fromStatus, toStatus, decisionKind,
			triggerKind, triggerFinding, pipelineRaw)
	}
}

func TestM7IntegrityApplyRejectsWrongMemoryStatusDependencyAndRollsBack(t *testing.T) {
	evidence := performM7ClaimErasure(t)
	residentID, _ := evidence.Metadata.Scope.ResidentID()
	integrityPipelineID := mustIntegrityID(t, "00000000000000000000000820")
	memoryStatusID := mustIntegrityID(t, "00000000000000000000000821")
	insertExactIntegrityPipeline(t, evidence.DB, integrityPipelineID)
	var globalCommit string
	if err := evidence.DB.QueryRow(`SELECT canonical_commit_id FROM canonical_commits
		WHERE resident_id IS NULL ORDER BY commit_seq LIMIT 1`).Scan(&globalCommit); err != nil {
		t.Fatal(err)
	}
	mustExec(t, evidence.DB, `INSERT INTO pipeline_versions(
		pipeline_version_id, canonical_commit_id, pipeline_kind, version_key,
		definition, recorded_at, recorded_tz
	) VALUES (?, ?, 'memory_status', 'memory-status-v1', ?, ?, ?)`,
		memoryStatusID.String(), globalCommit, `{"extra":true,"version":"memory-status-v1"}`, semanticTime, semanticTZ)
	scan, err := newSQLiteIntegrityScanner(evidence.DB).Scan(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	beforeFindings := countIntegrityRows(t, evidence.DB, `SELECT COUNT(*) FROM integrity_findings`)
	transaction := insertIntegrityTestCommitTx(t, evidence.DB, 101, &residentID)
	uow := &canonicalUoW{tx: transaction.tx, metadata: transaction.metadata}
	_, err = uow.RecordIntegrityFindings(context.Background(),
		plannedIntegrityRecord(t, residentID, integrityPipelineID, memoryStatusID, scan, 830))
	if !errors.Is(err, integrity.ErrQuarantineConflict) {
		_ = transaction.tx.Rollback()
		t.Fatalf("wrong memory-status dependency error = %v, want ErrQuarantineConflict", err)
	}
	_ = transaction.tx.Rollback()
	if got := countIntegrityRows(t, evidence.DB, `SELECT COUNT(*) FROM integrity_findings`); got != beforeFindings {
		t.Fatalf("failed dependency changed findings from %d to %d", beforeFindings, got)
	}
	if got := countIntegrityRows(t, evidence.DB, `SELECT COUNT(*) FROM claim_status_transitions`); got != 0 {
		t.Fatalf("failed dependency wrote %d claim status transitions", got)
	}
}

func TestM7ExistingFindingDirectlyTriggersReactivatedClaimQuarantine(t *testing.T) {
	evidence := performM7ClaimErasure(t)
	residentID, _ := evidence.Metadata.Scope.ResidentID()
	integrityPipelineID := mustIntegrityID(t, "00000000000000000000000860")
	memoryStatusID := mustIntegrityID(t, "00000000000000000000000861")
	insertExactIntegrityPipeline(t, evidence.DB, integrityPipelineID)
	insertExactMemoryStatusPipeline(t, evidence.DB, memoryStatusID)
	firstScan, err := newSQLiteIntegrityScanner(evidence.DB).Scan(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	first := recordIntegrityScan(t, evidence.DB, residentID, integrityPipelineID, memoryStatusID, firstScan, 101, 870)
	if len(first.Created) != 1 || len(first.Quarantined) != 1 {
		t.Fatalf("initial structural apply = %+v", first)
	}

	reactivation := insertIntegrityTestCommit(t, evidence.DB, firstCommitID(102), 102, &residentID)
	var humanID string
	if err := evidence.DB.QueryRow(`SELECT principal_id FROM principals
		WHERE kind = 'human' ORDER BY principal_id LIMIT 1`).Scan(&humanID); err != nil {
		t.Fatal(err)
	}
	mustExec(t, evidence.DB, `INSERT INTO claim_status_transitions(
		status_transition_id, canonical_commit_id, claim_id, from_status, to_status,
		decision_kind, actor_principal_id, trigger_kind, trigger_event_id,
		trigger_evidence_id, trigger_claim_relation_id, trigger_integrity_finding_id,
		pipeline_version_id, memory_policy_revision_id, gate_metrics,
		decision_reason_code, decision_reason_content_id, occurred_at, occurred_tz,
		recorded_at, recorded_tz
	) VALUES (?, ?, ?, 'quarantined', 'active', 'human', ?, NULL, NULL, NULL,
		NULL, NULL, NULL, NULL, NULL, 'reactivate_after_review', NULL, ?, ?, ?, ?)`,
		mustIntegrityID(t, "00000000000000000000000880").String(), reactivation.CommitID.String(),
		evidence.ClaimID, humanID, reactivation.CommittedAt.UnixMicro(), reactivation.CommittedTZ.String(),
		reactivation.CommittedAt.UnixMicro(), reactivation.CommittedTZ.String())

	secondScan, err := newSQLiteIntegrityScanner(evidence.DB).Scan(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	second := recordIntegrityScan(t, evidence.DB, residentID, integrityPipelineID, memoryStatusID, secondScan, 103, 890)
	if len(second.Created) != 0 || len(second.Existing) != 1 || len(second.Quarantined) != 1 {
		t.Fatalf("reactivated structural apply = %+v, want existing finding plus new quarantine", second)
	}
	if second.Quarantined[0].FindingID != first.Created[0].FindingID {
		t.Fatalf("reactivation trigger = %s, want existing finding %s",
			second.Quarantined[0].FindingID, first.Created[0].FindingID)
	}
	assertIntegrityFindingCount(t, evidence.DB, integrityPipelineID, 1)
}

func TestM7IntegrityApplyRevalidatesStructuralPredicateBeforeMutation(t *testing.T) {
	fixture, closeFixture := newSemanticFixture(t)
	defer closeFixture()
	mustExec(t, fixture.db, "DROP TRIGGER trg_claim_evidence_no_update")
	mustExec(t, fixture.db, `UPDATE claim_evidence SET reason_code = 'source_inferred'
		WHERE evidence_id = ?`, fixture.evidence["A"])
	scan, err := newSQLiteIntegrityScanner(fixture.db).Scan(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if !hasIntegrityRule(scan, integrity.RuleClaimQualifyingSupportUnresolvable) {
		t.Fatalf("stale-candidate fixture lacks qualifying-support finding: %+v", scan.Candidates)
	}
	mustExec(t, fixture.db, `UPDATE claim_evidence SET reason_code = ? WHERE evidence_id = ?`,
		string(memory.EvidenceReasonSourceStated), fixture.evidence["A"])
	residentID := mustIntegrityID(t, fixture.resident["A"])
	integrityPipelineID := mustIntegrityID(t, "00000000000000000000000840")
	memoryStatusID := mustIntegrityID(t, "00000000000000000000000841")
	insertExactIntegrityPipeline(t, fixture.db, integrityPipelineID)
	insertExactMemoryStatusPipeline(t, fixture.db, memoryStatusID)
	transaction := insertIntegrityTestCommitTx(t, fixture.db, 100, &residentID)
	uow := &canonicalUoW{tx: transaction.tx, metadata: transaction.metadata}
	_, err = uow.RecordIntegrityFindings(context.Background(),
		plannedIntegrityRecord(t, residentID, integrityPipelineID, memoryStatusID, scan, 850))
	if !errors.Is(err, integrity.ErrFindingConflict) {
		_ = transaction.tx.Rollback()
		t.Fatalf("stale structural candidate error = %v, want ErrFindingConflict", err)
	}
	_ = transaction.tx.Rollback()
	assertIntegrityFindingCount(t, fixture.db, integrityPipelineID, 0)
}

func TestM7FatalMinimumCheckNeverWritesFinding(t *testing.T) {
	fixture, closeFixture := newSemanticFixture(t)
	defer closeFixture()
	aliasID := addMinimumCheckSharedClaimAlias(t, fixture, fixture.claim["A"])
	mustExec(t, fixture.db, "DROP TRIGGER trg_claims_update_contract")
	mustExec(t, fixture.db, "UPDATE claims SET statement_hash = NULL WHERE claim_id = ?", aliasID)
	before := countIntegrityRows(t, fixture.db, `SELECT COUNT(*) FROM integrity_findings`)
	err := newSQLiteMinimumChecker(fixture.db).Check(context.Background())
	if !errors.Is(err, integrity.ErrFatal) {
		t.Fatalf("MinimumCheck() error = %v, want integrity.ErrFatal", err)
	}
	if after := countIntegrityRows(t, fixture.db, `SELECT COUNT(*) FROM integrity_findings`); after != before {
		t.Fatalf("fatal MinimumCheck changed finding count from %d to %d", before, after)
	}
}

func eraseFixtureEventContent(t *testing.T, fixture *semanticFixture, residentKey string) {
	t.Helper()
	contentID := fixture.content[residentKey]["event"]
	var owner, algorithm string
	var blobHash []byte
	if err := fixture.db.QueryRow(`SELECT owner_resident_id, blob_hash_algorithm, blob_hash
		FROM content_objects WHERE content_id = ?`, contentID).Scan(&owner, &algorithm, &blobHash); err != nil {
		t.Fatal(err)
	}
	mustExec(t, fixture.db, `UPDATE content_objects
		SET erasure_state = 'erased', blob_hash = NULL, commitment_salt = NULL
		WHERE content_id = ?`, contentID)
	mustExec(t, fixture.db, `DELETE FROM blobs
		WHERE dedupe_scope_id = ? AND hash_algorithm = ? AND blob_hash = ?`, owner, algorithm, blobHash)
}

func addSecondQualifyingSupport(t *testing.T, fixture *semanticFixture, residentKey string) {
	t.Helper()
	contentID := fixture.addContent(t, residentKey, "event_payload", "second-support", "independent")
	eventID := fixture.ids.new()
	evidenceID := fixture.ids.new()
	mustExec(t, fixture.db, "INSERT INTO events VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
		eventID, fixture.commit[residentKey], fixture.resident[residentKey], 2,
		"user_message", "conversation", 1, 0, "local_ui", "trusted",
		fixture.principal["human"], fixture.principal[residentKey], nil,
		semanticTime+1, semanticTZ, semanticTime+1, semanticTZ, contentID,
		semanticDigest("second-payload"), semanticDigest("first-event"), semanticDigest("second-event"),
		"sha256", "mahoroba:event-hash:v1", "mahoroba-jcs-v1")
	mustExec(t, fixture.db, `INSERT INTO claim_evidence(
		evidence_id, canonical_commit_id, claim_id, event_id, polarity, grade,
		trust_level, weight, derivation, source_evidence_id,
		memory_policy_revision_id, created_by_run_id, reason_code,
		reason_content_id, recorded_at, recorded_tz
	) VALUES (?, ?, ?, ?, 'support', 'stated', 'trusted', 1000000,
		'extracted', NULL, ?, ?, ?, NULL, ?, ?)`,
		evidenceID, fixture.commit[residentKey], fixture.claim[residentKey], eventID,
		fixture.revision[residentKey]["memory_policy"], fixture.run[residentKey],
		string(memory.EvidenceReasonSourceStated), semanticTime+1, semanticTZ)
}

func insertResolvableDialogueCancellationHistory(t *testing.T, fixture *semanticFixture, residentKey string) {
	t.Helper()
	for _, class := range []string{"principles", "persona", "memory_policy"} {
		mustExec(t, fixture.db, `INSERT INTO resident_revision_activations(
			activation_id, canonical_commit_id, resident_id, revision_id,
			actor_principal_id, approval_id, reason_code, reason_content_id,
			recorded_at, recorded_tz
		) VALUES (?, ?, ?, ?, ?, NULL, 'test', NULL, ?, ?)`,
			fixture.ids.new(), fixture.commit[residentKey], fixture.resident[residentKey],
			fixture.revision[residentKey][class], fixture.principal["human"], semanticTime, semanticTZ)
	}
	pipelineDefinition, err := canonical.MarshalCanonical(struct {
		Version string `json:"version"`
		Purpose string `json:"purpose"`
	}{Version: domain.DialoguePipelineVersionV1, Purpose: "dialogue"})
	if err != nil {
		t.Fatal(err)
	}
	mustExec(t, fixture.db, `INSERT INTO pipeline_versions(
		pipeline_version_id, canonical_commit_id, pipeline_kind, version_key,
		definition, recorded_at, recorded_tz
	) VALUES (?, ?, 'dialogue', ?, ?, ?, ?)`, fixture.ids.new(), fixture.commit["global"],
		domain.DialoguePipelineVersionV1, pipelineDefinition.String(), semanticTime, semanticTZ)
	memoryPipelineDefinition, err := canonical.MarshalCanonical(struct {
		Version string `json:"version"`
	}{Version: domain.MemoryExtractionPipelineVersion})
	if err != nil {
		t.Fatal(err)
	}
	mustExec(t, fixture.db, `INSERT INTO pipeline_versions(
		pipeline_version_id, canonical_commit_id, pipeline_kind, version_key,
		definition, recorded_at, recorded_tz
	) VALUES (?, ?, 'memory_extraction', ?, ?, ?, ?)`, fixture.ids.new(), fixture.commit["global"],
		domain.MemoryExtractionPipelineVersion, memoryPipelineDefinition.String(), semanticTime, semanticTZ)
	mustExec(t, fixture.db, "DROP TRIGGER trg_sessionization_policy_versions_no_update")
	mustExec(t, fixture.db, `UPDATE sessionization_policy_versions
		SET definition = ? WHERE sessionization_policy_version_id = ?`,
		`{"idle_gap_microseconds":"1800000000","version":"sessionization-v1"}`, fixture.session)
}

func requireIntegrityRule(t *testing.T, scan integrity.ScanResult, rule integrity.RuleCode) integrity.Candidate {
	t.Helper()
	for _, candidate := range scan.Candidates {
		if candidate.RuleCode == rule {
			return candidate
		}
	}
	t.Fatalf("missing integrity rule %s in %+v", rule, scan.Candidates)
	return integrity.Candidate{}
}

func hasIntegrityRule(scan integrity.ScanResult, rule integrity.RuleCode) bool {
	for _, candidate := range scan.Candidates {
		if candidate.RuleCode == rule {
			return true
		}
	}
	return false
}

func assertIntegrityRuleAbsent(t *testing.T, scan integrity.ScanResult, rule integrity.RuleCode) {
	t.Helper()
	if hasIntegrityRule(scan, rule) {
		t.Fatalf("unexpected integrity rule %s in %+v", rule, scan.Candidates)
	}
}

func recordIntegrityScan(
	t *testing.T,
	db integrityTestDB,
	residentID, pipelineID, memoryStatusID canonical.ID,
	scan integrity.ScanResult,
	commitSeq int64,
	findingBase int,
) integrity.RecordFindingsResult {
	t.Helper()
	transaction := insertIntegrityTestCommitTx(t, db, commitSeq, &residentID)
	uow := &canonicalUoW{tx: transaction.tx, metadata: transaction.metadata}
	result, err := uow.RecordIntegrityFindings(context.Background(), plannedIntegrityRecord(t, residentID, pipelineID, memoryStatusID, scan, findingBase))
	if err != nil {
		_ = transaction.tx.Rollback()
		t.Fatal(err)
	}
	if err := transaction.tx.Commit(); err != nil {
		t.Fatal(err)
	}
	return result
}

func recordIntegrityScanNoMutation(
	t *testing.T,
	db integrityTestDB,
	residentID, pipelineID, memoryStatusID canonical.ID,
	scan integrity.ScanResult,
	commitSeq int64,
	findingBase int,
) integrity.RecordFindingsResult {
	t.Helper()
	transaction := insertIntegrityTestCommitTx(t, db, commitSeq, &residentID)
	uow := &canonicalUoW{tx: transaction.tx, metadata: transaction.metadata}
	result, err := uow.RecordIntegrityFindings(context.Background(), plannedIntegrityRecord(t, residentID, pipelineID, memoryStatusID, scan, findingBase))
	if !errors.Is(err, canonical.ErrNoMutation) {
		_ = transaction.tx.Rollback()
		t.Fatalf("idempotent RecordIntegrityFindings() error = %v, want ErrNoMutation", err)
	}
	_ = transaction.tx.Rollback()
	return result
}

func plannedIntegrityRecord(
	t *testing.T,
	residentID, pipelineID, memoryStatusID canonical.ID,
	scan integrity.ScanResult,
	findingBase int,
) integrity.RecordFindings {
	t.Helper()
	value := integrity.RecordFindings{
		ResidentID: residentID, CapturedHead: scan.CapturedHead, PipelineVersionID: pipelineID,
		Findings: make([]integrity.PlannedFinding, len(scan.Candidates)),
	}
	structural := false
	for index, candidate := range scan.Candidates {
		planned := integrity.PlannedFinding{
			FindingID: mustIntegrityID(t, firstCommitID(findingBase+index)), Candidate: candidate,
		}
		if isStructuralClaimRule(candidate.RuleCode) {
			transitionID := mustIntegrityID(t, firstCommitID(findingBase+100+index))
			planned.QuarantineTransitionID = &transitionID
			structural = true
		}
		value.Findings[index] = planned
	}
	if structural {
		value.MemoryStatusPipelineVersionID = &memoryStatusID
	}
	return value
}

type integrityTestDB interface {
	BeginTx(context.Context, *sql.TxOptions) (*sql.Tx, error)
	Exec(string, ...any) (sql.Result, error)
}

type integrityTestTransaction struct {
	tx       *sql.Tx
	metadata canonical.CommitMetadata
}

func insertIntegrityTestCommitTx(
	t *testing.T,
	db integrityTestDB,
	seq int64,
	residentID *canonical.ID,
) integrityTestTransaction {
	t.Helper()
	tx, err := db.BeginTx(context.Background(), nil)
	if err != nil {
		t.Fatal(err)
	}
	commitID := mustIntegrityID(t, firstCommitID(int(seq)))
	var resident any
	scope := canonical.GlobalScope()
	if residentID != nil {
		resident = residentID.String()
		scope, err = canonical.ResidentScope(*residentID)
		if err != nil {
			_ = tx.Rollback()
			t.Fatal(err)
		}
	}
	metadata := canonical.CommitMetadata{
		CommitID: commitID, CommitSeq: mustCommitSeq(t, seq), Scope: scope,
		CommittedAt: canonical.Instant(semanticTime + seq),
		CommittedTZ: canonical.MustTimezone(semanticTZ),
	}
	if _, err := tx.Exec(`INSERT INTO canonical_commits(
		canonical_commit_id, commit_seq, resident_id, committed_at, committed_tz
	) VALUES (?, ?, ?, ?, ?)`, commitID.String(), seq, resident,
		metadata.CommittedAt.UnixMicro(), metadata.CommittedTZ.String()); err != nil {
		_ = tx.Rollback()
		t.Fatal(err)
	}
	return integrityTestTransaction{tx: tx, metadata: metadata}
}

func insertIntegrityTestCommit(
	t *testing.T,
	db *sql.DB,
	commitIDRaw string,
	seq int64,
	residentID *canonical.ID,
) canonical.CommitMetadata {
	t.Helper()
	commitID := mustIntegrityID(t, commitIDRaw)
	var resident any
	scope := canonical.GlobalScope()
	if residentID != nil {
		resident = residentID.String()
		var err error
		scope, err = canonical.ResidentScope(*residentID)
		if err != nil {
			t.Fatal(err)
		}
	}
	mustExec(t, db, `INSERT INTO canonical_commits(
		canonical_commit_id, commit_seq, resident_id, committed_at, committed_tz
	) VALUES (?, ?, ?, ?, ?)`, commitID.String(), seq, resident, semanticTime+seq, semanticTZ)
	return canonical.CommitMetadata{
		CommitID: commitID, CommitSeq: mustCommitSeq(t, seq), Scope: scope,
		CommittedAt: canonical.Instant(semanticTime + seq), CommittedTZ: canonical.MustTimezone(semanticTZ),
	}
}

func insertExactIntegrityPipeline(t *testing.T, db *sql.DB, pipelineID canonical.ID) {
	t.Helper()
	var globalCommit string
	if err := db.QueryRow(`SELECT canonical_commit_id FROM canonical_commits
		WHERE resident_id IS NULL ORDER BY commit_seq LIMIT 1`).Scan(&globalCommit); err != nil {
		t.Fatal(err)
	}
	mustExec(t, db, `INSERT INTO pipeline_versions(
		pipeline_version_id, canonical_commit_id, pipeline_kind, version_key,
		definition, recorded_at, recorded_tz
	) VALUES (?, ?, 'integrity_check', 'integrity-check-v1', ?, ?, ?)`,
		pipelineID.String(), globalCommit, integrityPipelineDefinitionV1, semanticTime, semanticTZ)
}

func insertExactMemoryStatusPipeline(t *testing.T, db *sql.DB, pipelineID canonical.ID) {
	t.Helper()
	var globalCommit string
	if err := db.QueryRow(`SELECT canonical_commit_id FROM canonical_commits
		WHERE resident_id IS NULL ORDER BY commit_seq LIMIT 1`).Scan(&globalCommit); err != nil {
		t.Fatal(err)
	}
	mustExec(t, db, `INSERT INTO pipeline_versions(
		pipeline_version_id, canonical_commit_id, pipeline_kind, version_key,
		definition, recorded_at, recorded_tz
	) VALUES (?, ?, 'memory_status', 'memory-status-v1', ?, ?, ?)`,
		pipelineID.String(), globalCommit, memoryStatusPipelineDefinitionV1, semanticTime, semanticTZ)
}

func assertIntegrityFindingCount(t *testing.T, db *sql.DB, pipelineID canonical.ID, want int) {
	t.Helper()
	var got int
	if err := db.QueryRow(`SELECT COUNT(*) FROM integrity_findings
		WHERE pipeline_version_id = ?`, pipelineID.String()).Scan(&got); err != nil {
		t.Fatal(err)
	}
	if got != want {
		t.Fatalf("finding count = %d, want %d", got, want)
	}
}

func countIntegrityRows(t *testing.T, db *sql.DB, query string) int {
	t.Helper()
	var count int
	if err := db.QueryRow(query).Scan(&count); err != nil {
		t.Fatal(err)
	}
	return count
}

func firstCommitID(value int) string { return fmt.Sprintf("%026d", value) }

func mustIntegrityID(t *testing.T, raw string) canonical.ID {
	t.Helper()
	id, err := canonical.ParseID(raw)
	if err != nil {
		t.Fatal(err)
	}
	return id
}
