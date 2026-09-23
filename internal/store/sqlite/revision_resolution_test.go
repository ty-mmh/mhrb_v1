package sqlite

import (
	"context"
	"errors"
	"testing"

	"mahoroba.local/mahoroba/internal/canonical"
	"mahoroba.local/mahoroba/internal/memory"
)

func TestM5ActiveRevisionResolutionUsesLatestActivationCommit(t *testing.T) {
	fixture, closeFixture := newSemanticFixture(t)
	defer closeFixture()

	residentID := fixture.resident["A"]
	ownerID := fixture.principal["human"]
	oldRevisionID := fixture.revision["A"]["memory_policy"]
	oldContentID := fixture.content["A"]["memory"]

	commit4 := fixture.ids.new()
	commit5 := fixture.ids.new()
	newRevisionID := fixture.ids.new()
	newContentID := fixture.addContent(t, "A", "memory_policy_text", "new-memory-policy", "resident_only")
	mustExec(t, fixture.db, `INSERT INTO canonical_commits(
		canonical_commit_id, commit_seq, resident_id, committed_at, committed_tz
	) VALUES (?, 4, ?, ?, ?)`, commit4, residentID, semanticTime+3, semanticTZ)
	mustExec(t, fixture.db, `INSERT INTO resident_revisions(
		revision_id, canonical_commit_id, resident_id, revision_class, content_id,
		parent_revision_id, created_by_run_id, reason_content_id, recorded_at, recorded_tz
	) VALUES (?, ?, ?, 'memory_policy', ?, ?, NULL, NULL, ?, ?)`,
		newRevisionID, commit4, residentID, newContentID, oldRevisionID, semanticTime+3, semanticTZ)
	mustExec(t, fixture.db, `INSERT INTO resident_revision_activations(
		activation_id, canonical_commit_id, resident_id, revision_id, actor_principal_id,
		approval_id, reason_code, reason_content_id, recorded_at, recorded_tz
	) VALUES (?, ?, ?, ?, ?, NULL, 'admin_memory_policy', NULL, ?, ?)`,
		fixture.ids.new(), commit4, residentID, newRevisionID, ownerID, semanticTime+3, semanticTZ)

	repository := fixtureStoreRepository(fixture)
	parsedResidentID := projectionTestID(t, residentID)
	resolved, content, found, err := repository.latestRevision(context.Background(), parsedResidentID, "memory_policy")
	if err != nil || !found || resolved.String() != newRevisionID || content != "new-memory-policy" {
		t.Fatalf("latest activated revision = %s / %q / %v / %v", resolved, content, found, err)
	}

	mustExec(t, fixture.db, `INSERT INTO canonical_commits(
		canonical_commit_id, commit_seq, resident_id, committed_at, committed_tz
	) VALUES (?, 5, ?, ?, ?)`, commit5, residentID, semanticTime+4, semanticTZ)
	mustExec(t, fixture.db, `INSERT INTO resident_revision_activations(
		activation_id, canonical_commit_id, resident_id, revision_id, actor_principal_id,
		approval_id, reason_code, reason_content_id, recorded_at, recorded_tz
	) VALUES (?, ?, ?, ?, ?, NULL, 'admin_memory_policy', NULL, ?, ?)`,
		fixture.ids.new(), commit5, residentID, oldRevisionID, ownerID, semanticTime+4, semanticTZ)

	resolved, content, found, err = repository.latestRevision(context.Background(), parsedResidentID, "memory_policy")
	defaultPolicy, encodeErr := memory.DefaultPolicyV2().CanonicalJSON()
	if encodeErr != nil {
		t.Fatal(encodeErr)
	}
	if err != nil || !found || resolved.String() != oldRevisionID || content != defaultPolicy.String() {
		t.Fatalf("reactivated revision = %s / %q / %v / %v", resolved, content, found, err)
	}

	var persistedOldContentID string
	if err := fixture.db.QueryRow(`SELECT content_id FROM resident_revisions WHERE revision_id = ?`, oldRevisionID).Scan(&persistedOldContentID); err != nil {
		t.Fatal(err)
	}
	if persistedOldContentID != oldContentID {
		t.Fatalf("old revision content changed: got %s want %s", persistedOldContentID, oldContentID)
	}
}

func TestM5RevisionResolversPinHeadEventAndAsOf(t *testing.T) {
	fixture, closeFixture := newSemanticFixture(t)
	defer closeFixture()
	ctx := context.Background()
	residentID := projectionTestID(t, fixture.resident["A"])
	oldRevisionID := fixture.revision["A"]["memory_policy"]

	commit4 := fixture.ids.new()
	commit5 := fixture.ids.new()
	newRevisionID := fixture.ids.new()
	newContentID := fixture.addContent(t, "A", "memory_policy_text", "policy-v2", "resident_only")
	mustExec(t, fixture.db, `INSERT INTO resident_revision_activations(
		activation_id, canonical_commit_id, resident_id, revision_id, actor_principal_id,
		approval_id, reason_code, reason_content_id, recorded_at, recorded_tz
	) VALUES (?, ?, ?, ?, ?, NULL, 'bootstrap_finalize', NULL, ?, ?)`,
		fixture.ids.new(), fixture.commit["A"], fixture.resident["A"], oldRevisionID,
		fixture.principal["human"], semanticTime, semanticTZ)
	mustExec(t, fixture.db, `INSERT INTO canonical_commits(
		canonical_commit_id, commit_seq, resident_id, committed_at, committed_tz
	) VALUES (?, 4, ?, ?, ?)`, commit4, fixture.resident["A"], semanticTime+100, semanticTZ)
	mustExec(t, fixture.db, `INSERT INTO resident_revisions(
		revision_id, canonical_commit_id, resident_id, revision_class, content_id,
		parent_revision_id, created_by_run_id, reason_content_id, recorded_at, recorded_tz
	) VALUES (?, ?, ?, 'memory_policy', ?, ?, NULL, NULL, ?, ?)`,
		newRevisionID, commit4, fixture.resident["A"], newContentID, oldRevisionID, semanticTime+100, semanticTZ)
	mustExec(t, fixture.db, `INSERT INTO resident_revision_activations(
		activation_id, canonical_commit_id, resident_id, revision_id, actor_principal_id,
		approval_id, reason_code, reason_content_id, recorded_at, recorded_tz
	) VALUES (?, ?, ?, ?, ?, NULL, 'admin_memory_policy', NULL, ?, ?)`,
		fixture.ids.new(), commit4, fixture.resident["A"], newRevisionID,
		fixture.principal["human"], semanticTime+100, semanticTZ)

	before, err := fixtureStoreRepository(fixture).ActiveRevisionAtHead(ctx, residentID, "memory_policy", mustCommitSeq(t, 2))
	if err != nil || before.RevisionID.String() != oldRevisionID {
		t.Fatalf("head 2 revision = %s / %v", before.RevisionID, err)
	}
	after, err := fixtureStoreRepository(fixture).ActiveRevisionAtHead(ctx, residentID, "memory_policy", mustCommitSeq(t, 4))
	if err != nil || after.RevisionID.String() != newRevisionID || after.Content != "policy-v2" {
		t.Fatalf("head 4 revision = %s / %q / %v", after.RevisionID, after.Content, err)
	}
	asOfBefore, err := fixtureStoreRepository(fixture).ActiveRevisionAsOf(ctx, residentID, "memory_policy", mustCommitSeq(t, 4), semanticInstant(semanticTime+50))
	if err != nil || asOfBefore.RevisionID.String() != oldRevisionID {
		t.Fatalf("as-of revision = %s / %v", asOfBefore.RevisionID, err)
	}

	// The source event belongs to commit 2, so a policy activated at commit 4
	// must never be selected retroactively.
	forEvent, err := fixtureStoreRepository(fixture).ActiveRevisionForEvent(
		ctx, residentID, "memory_policy", projectionTestID(t, fixture.event["A"]),
	)
	if err != nil || forEvent.RevisionID.String() != oldRevisionID {
		t.Fatalf("event-time revision = %s / %v", forEvent.RevisionID, err)
	}

	mustExec(t, fixture.db, `INSERT INTO canonical_commits(
		canonical_commit_id, commit_seq, resident_id, committed_at, committed_tz
	) VALUES (?, 5, ?, ?, ?)`, commit5, fixture.resident["A"], semanticTime+101, semanticTZ)
	if _, err := fixtureStoreRepository(fixture).ActiveRevisionAtHead(ctx, residentID, "unknown", mustCommitSeq(t, 5)); err == nil {
		t.Fatal("unknown revision class unexpectedly resolved")
	}
	otherResident := projectionTestID(t, fixture.resident["B"])
	if _, err := fixtureStoreRepository(fixture).ActiveRevisionForEvent(
		ctx, otherResident, "memory_policy", projectionTestID(t, fixture.event["A"]),
	); !errors.Is(err, ErrActiveRevisionUnresolved) {
		t.Fatalf("cross-resident event resolution error = %v", err)
	}
}

func mustCommitSeq(t *testing.T, value int64) canonical.CommitSeq {
	t.Helper()
	seq, err := canonical.NewCommitSeq(value)
	if err != nil {
		t.Fatal(err)
	}
	return seq
}

func semanticInstant(value int64) canonical.Instant { return canonical.Instant(value) }
