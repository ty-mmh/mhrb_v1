package projection

import (
	"testing"
)

func TestM5ClaimViewScopeDefinitionHasZeroDependencies(t *testing.T) {
	definition := ClaimViewScopeCurrentDefinition()
	if definition.Name != ClaimViewScopeCurrentName || definition.Version != "claim-view-scope-current-v1" ||
		definition.TimeSensitive || len(definition.Dependencies) != 0 || len(definition.RebuildOnActivation) != 0 {
		t.Fatalf("claim_view_scope_current definition = %+v", definition)
	}
}

func TestM5ClaimViewScopeReplayUsesLatestCapturedAssertion(t *testing.T) {
	claimOne := projectionFixtureID(t, "00000000000000000000000031")
	claimTwo := projectionFixtureID(t, "00000000000000000000000032")
	states, err := ReplayClaimViewScopes(projectionFixtureCommit(t, 5), []ClaimSeed{
		{ClaimID: claimTwo, CommitSeq: projectionFixtureCommit(t, 1), TemporalKind: ClaimTemporalStable},
		{ClaimID: claimOne, CommitSeq: projectionFixtureCommit(t, 1), TemporalKind: ClaimTemporalStable},
	}, []ClaimViewScopeAssertion{
		{AssertionID: projectionFixtureID(t, "00000000000000000000000033"), ClaimID: claimOne, CommitSeq: projectionFixtureCommit(t, 2), ViewScope: ClaimViewScopeResidentUI},
		{AssertionID: projectionFixtureID(t, "00000000000000000000000034"), ClaimID: claimOne, CommitSeq: projectionFixtureCommit(t, 4), ViewScope: ClaimViewScopeAdminOnly},
		{AssertionID: projectionFixtureID(t, "00000000000000000000000035"), ClaimID: claimOne, CommitSeq: projectionFixtureCommit(t, 6), ViewScope: ClaimViewScopeResidentUI},
		{AssertionID: projectionFixtureID(t, "00000000000000000000000036"), ClaimID: claimTwo, CommitSeq: projectionFixtureCommit(t, 3), ViewScope: ClaimViewScopeResidentUI},
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(states) != 2 || states[0].ClaimID != claimOne || states[0].ViewScope != ClaimViewScopeAdminOnly ||
		states[1].ClaimID != claimTwo || states[1].ViewScope != ClaimViewScopeResidentUI {
		t.Fatalf("view scope states = %+v", states)
	}
}

func TestM5ClaimViewScopeReplayFailsClosedWithoutInitialAssertion(t *testing.T) {
	claimID := projectionFixtureID(t, "00000000000000000000000037")
	_, err := ReplayClaimViewScopes(projectionFixtureCommit(t, 1), []ClaimSeed{{
		ClaimID: claimID, CommitSeq: projectionFixtureCommit(t, 1), TemporalKind: ClaimTemporalStable,
	}}, nil)
	if err == nil {
		t.Fatal("missing initial view scope unexpectedly accepted")
	}
}
