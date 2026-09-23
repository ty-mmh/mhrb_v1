package readiness_test

import (
	"context"
	"errors"
	"reflect"
	"testing"

	"mahoroba.local/mahoroba/internal/canonical"
	"mahoroba.local/mahoroba/internal/cliresult"
	"mahoroba.local/mahoroba/internal/readiness"
)

type fixedSource struct {
	snapshot readiness.Snapshot
	err      error
}

func (source fixedSource) CaptureServiceReadiness(context.Context) (readiness.Snapshot, error) {
	return source.snapshot, source.err
}

func TestM7ReadinessReasonCatalogMatchesCLIResultPriority(t *testing.T) {
	want := []string{
		cliresult.HealthReasonStartupIncomplete,
		cliresult.HealthReasonResidentUnselected,
		cliresult.HealthReasonResidentNotActive,
		cliresult.HealthReasonMemoryPolicy,
		cliresult.HealthReasonSessionUnresolved,
		cliresult.HealthReasonIntegrityBlocked,
		cliresult.HealthReasonProjectionCurrent,
		cliresult.HealthReasonShutdown,
	}
	gotCodes := readiness.NotReadyReasonOrder()
	got := make([]string, len(gotCodes))
	for index, code := range gotCodes {
		got[index] = string(code)
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("readiness reason catalog = %v, want CLI catalog %v", got, want)
	}
	if string(readiness.ReasonReady) != cliresult.HealthReasonReady {
		t.Fatalf("ready reason = %q, want %q", readiness.ReasonReady, cliresult.HealthReasonReady)
	}
}

func TestM7EvaluateServiceReadinessSelectionPolicyAndProjection(t *testing.T) {
	ready := readyReadinessSnapshot(t)
	tests := []struct {
		name           string
		mutate         func(*readiness.Snapshot)
		projection     bool
		wantReasons    []readiness.ReasonCode
		wantProjection bool
	}{
		{
			name: "unselected",
			mutate: func(snapshot *readiness.Snapshot) {
				snapshot.ActiveResidentID = nil
				snapshot.ActiveResidentStatus = ""
				snapshot.MemoryPolicyServiceCurrent = false
				snapshot.SessionPolicyID = nil
			},
			projection: true, wantReasons: []readiness.ReasonCode{readiness.ReasonResidentUnselected},
		},
		{
			name: "historical memory policy",
			mutate: func(snapshot *readiness.Snapshot) {
				snapshot.MemoryPolicyServiceCurrent = false
			},
			projection: true, wantReasons: []readiness.ReasonCode{readiness.ReasonMemoryPolicy},
		},
		{
			name: "nonactive",
			mutate: func(snapshot *readiness.Snapshot) {
				snapshot.ActiveResidentStatus = "archived"
			},
			projection: true, wantReasons: []readiness.ReasonCode{readiness.ReasonResidentNotActive},
		},
		{
			name: "unresolved session",
			mutate: func(snapshot *readiness.Snapshot) {
				snapshot.SessionPolicyID = nil
			},
			projection: true, wantReasons: []readiness.ReasonCode{readiness.ReasonSessionUnresolved},
		},
		{
			name: "projection noncurrent", mutate: func(*readiness.Snapshot) {},
			projection: false, wantReasons: []readiness.ReasonCode{readiness.ReasonProjectionCurrent},
		},
		{
			name: "ready", mutate: func(*readiness.Snapshot) {}, projection: true,
			wantReasons: []readiness.ReasonCode{readiness.ReasonReady}, wantProjection: true,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			snapshot := cloneReadinessSnapshot(ready)
			test.mutate(&snapshot)
			projectionCalled := false
			result, err := readiness.EvaluateServiceReadiness(context.Background(), fixedSource{snapshot: snapshot}, readiness.Request{
				StartupComplete: true,
				Projection: readiness.ProjectionCheckFunc(func(_ context.Context, requirement readiness.ProjectionRequirement) (bool, error) {
					projectionCalled = true
					if requirement.CapturedHead != snapshot.CapturedHead || snapshot.ActiveResidentID == nil ||
						requirement.ResidentID != *snapshot.ActiveResidentID || snapshot.SessionPolicyID == nil ||
						requirement.SessionPolicyID != *snapshot.SessionPolicyID {
						t.Fatal("Projection requirement was not bound to the readiness snapshot")
					}
					return test.projection, nil
				}),
			})
			if err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(result.ReasonCodes, test.wantReasons) {
				t.Fatalf("reason codes = %v, want %v", result.ReasonCodes, test.wantReasons)
			}
			if result.Ready != (len(test.wantReasons) == 1 && test.wantReasons[0] == readiness.ReasonReady) {
				t.Fatalf("ready = %v for reasons %v", result.Ready, result.ReasonCodes)
			}
			applicable := snapshot.ActiveResidentID != nil && snapshot.ActiveResidentStatus == "active" &&
				snapshot.MemoryPolicyServiceCurrent && snapshot.SessionPolicyID != nil
			if projectionCalled != applicable {
				t.Fatalf("Projection called = %v, applicable = %v", projectionCalled, applicable)
			}
			if result.ProjectionCurrent != test.wantProjection {
				t.Fatalf("Projection current = %v, want %v", result.ProjectionCurrent, test.wantProjection)
			}
		})
	}
}

func TestM7EvaluateServiceReadinessCurrentPredicatesIgnoreHistoricalFindingCount(t *testing.T) {
	for _, rule := range []readiness.BlockingRule{
		readiness.RuleActiveRequiredRevisionErased,
		readiness.RuleRunningAttemptInputErased,
		readiness.RuleCancellationEnvelopeUnresolvable,
	} {
		for _, recorded := range []uint64{0, 1} {
			name := string(rule) + "/finding_missing"
			if recorded == 1 {
				name = string(rule) + "/finding_exists"
			}
			t.Run(name, func(t *testing.T) {
				snapshot := readyReadinessSnapshot(t)
				snapshot.BlockingPredicates = []readiness.PredicateSummary{{
					Rule: rule, CurrentCount: 1, RecordedCount: recorded,
				}}
				result, err := evaluateWithCurrentProjection(snapshot)
				if err != nil {
					t.Fatal(err)
				}
				if result.Ready || !reflect.DeepEqual(result.ReasonCodes, []readiness.ReasonCode{readiness.ReasonIntegrityBlocked}) {
					t.Fatalf("current predicate result = %+v", result)
				}
				if result.IntegrityScanIncomplete != (recorded == 0) {
					t.Fatalf("scan incomplete = %v, recorded = %d", result.IntegrityScanIncomplete, recorded)
				}
			})
		}
	}

	// Append-only findings are intentionally absent from the source contract
	// once their underlying current predicate is false.
	result, err := evaluateWithCurrentProjection(readyReadinessSnapshot(t))
	if err != nil {
		t.Fatal(err)
	}
	if !result.Ready || !reflect.DeepEqual(result.ReasonCodes, []readiness.ReasonCode{readiness.ReasonReady}) {
		t.Fatalf("resolved historical finding affected current readiness: %+v", result)
	}
}

func TestM7EvaluateServiceReadinessReturnsAllReasonsInClosedOrder(t *testing.T) {
	snapshot := readyReadinessSnapshot(t)
	snapshot.BlockingPredicates = []readiness.PredicateSummary{
		{Rule: readiness.RuleActiveRequiredRevisionErased, CurrentCount: 1},
		{Rule: readiness.RuleCancellationEnvelopeUnresolvable, CurrentCount: 2, RecordedCount: 1},
	}
	result, err := readiness.EvaluateServiceReadiness(context.Background(), fixedSource{snapshot: snapshot}, readiness.Request{
		StartupComplete: false, ShutdownInProgress: true,
		Projection: readiness.ProjectionCheckFunc(func(context.Context, readiness.ProjectionRequirement) (bool, error) {
			return false, nil
		}),
	})
	if err != nil {
		t.Fatal(err)
	}
	want := []readiness.ReasonCode{
		readiness.ReasonStartupIncomplete,
		readiness.ReasonIntegrityBlocked,
		readiness.ReasonProjectionCurrent,
		readiness.ReasonShutdown,
	}
	if !reflect.DeepEqual(result.ReasonCodes, want) || !result.IntegrityScanIncomplete {
		t.Fatalf("ordered result = %+v, want reasons %v and incomplete scan", result, want)
	}
}

func TestM7CanonicalProjectionProofRejectsAnotherCapturedHead(t *testing.T) {
	snapshot := readyReadinessSnapshot(t)
	proof := readiness.CanonicalProjectionProof{
		ResidentID:      *snapshot.ActiveResidentID,
		SessionPolicyID: *snapshot.SessionPolicyID,
		Head:            snapshot.CapturedHead.Canonical(),
		IsCurrent:       true,
	}
	if _, err := readiness.EvaluateServiceReadiness(context.Background(), fixedSource{snapshot: snapshot}, readiness.Request{
		StartupComplete: true, Projection: proof,
	}); err != nil {
		t.Fatalf("matching Projection proof = %v", err)
	}
	wrongSeq, _ := canonical.NewCommitSeq(snapshot.CapturedHead.CommitSeq.Int64() + 1)
	proof.Head.CommitSeq = wrongSeq
	if _, err := readiness.EvaluateServiceReadiness(context.Background(), fixedSource{snapshot: snapshot}, readiness.Request{
		StartupComplete: true, Projection: proof,
	}); err == nil {
		t.Fatal("Projection proof for another head was accepted")
	}
	proof.Head = snapshot.CapturedHead.Canonical()
	otherPolicyID, err := canonical.ParseID("01ARZ3NDEKTSV4RRFFQ69G5FAY")
	if err != nil {
		t.Fatal(err)
	}
	proof.SessionPolicyID = otherPolicyID
	if _, err := readiness.EvaluateServiceReadiness(context.Background(), fixedSource{snapshot: snapshot}, readiness.Request{
		StartupComplete: true, Projection: proof,
	}); err == nil {
		t.Fatal("Projection proof for another session policy was accepted")
	}
}

func TestM7EvaluateServiceReadinessFailsClosedOnSourceOrProjectionError(t *testing.T) {
	if _, err := readiness.EvaluateServiceReadiness(context.Background(), fixedSource{err: errors.New("read failed")}, readiness.Request{}); err == nil {
		t.Fatal("source error was ignored")
	}
	snapshot := readyReadinessSnapshot(t)
	if _, err := readiness.EvaluateServiceReadiness(context.Background(), fixedSource{snapshot: snapshot}, readiness.Request{
		StartupComplete: true,
		Projection: readiness.ProjectionCheckFunc(func(context.Context, readiness.ProjectionRequirement) (bool, error) {
			return false, errors.New("projection failed")
		}),
	}); err == nil {
		t.Fatal("Projection error was converted into a valid not-ready snapshot")
	}
}

func evaluateWithCurrentProjection(snapshot readiness.Snapshot) (readiness.Result, error) {
	return readiness.EvaluateServiceReadiness(context.Background(), fixedSource{snapshot: snapshot}, readiness.Request{
		StartupComplete: true,
		Projection: readiness.ProjectionCheckFunc(func(context.Context, readiness.ProjectionRequirement) (bool, error) {
			return true, nil
		}),
	})
}

func readyReadinessSnapshot(t *testing.T) readiness.Snapshot {
	t.Helper()
	commitID, err := canonical.ParseID("01ARZ3NDEKTSV4RRFFQ69G5FAV")
	if err != nil {
		t.Fatal(err)
	}
	residentID, err := canonical.ParseID("01ARZ3NDEKTSV4RRFFQ69G5FAW")
	if err != nil {
		t.Fatal(err)
	}
	policyID, err := canonical.ParseID("01ARZ3NDEKTSV4RRFFQ69G5FAX")
	if err != nil {
		t.Fatal(err)
	}
	seq, _ := canonical.NewCommitSeq(7)
	return readiness.Snapshot{
		CapturedHead: readiness.Head{
			Exists: true, CommitID: commitID, CommitSeq: seq,
			CommittedAt: canonical.Instant(1_700_000_000_000_000),
			CommittedTZ: canonical.MustTimezone("UTC"),
		},
		ActiveResidentID: &residentID, ActiveResidentStatus: "active",
		MemoryPolicyServiceCurrent: true, SessionPolicyID: &policyID,
		BlockingPredicates: []readiness.PredicateSummary{},
	}
}

func cloneReadinessSnapshot(snapshot readiness.Snapshot) readiness.Snapshot {
	copy := snapshot
	if snapshot.ActiveResidentID != nil {
		value := *snapshot.ActiveResidentID
		copy.ActiveResidentID = &value
	}
	if snapshot.SessionPolicyID != nil {
		value := *snapshot.SessionPolicyID
		copy.SessionPolicyID = &value
	}
	copy.BlockingPredicates = append([]readiness.PredicateSummary(nil), snapshot.BlockingPredicates...)
	return copy
}
