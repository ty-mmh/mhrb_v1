package main

import (
	"context"
	"errors"
	"slices"
	"testing"

	"mahoroba.local/mahoroba/internal/canonical"
	"mahoroba.local/mahoroba/internal/projection"
	"mahoroba.local/mahoroba/internal/readiness"
)

type fixedHealthStatuses struct {
	values []projection.Status
	err    error
}

func (source fixedHealthStatuses) Status(context.Context, projection.StatusFilter) ([]projection.Status, error) {
	return append([]projection.Status(nil), source.values...), source.err
}

func TestM7HealthProjectionCheckerBindsAllServiceViewsToCapturedHead(t *testing.T) {
	residentID := testCanonicalID(t, "01ARZ3NDEKTSV4RRFFQ69G5FAV")
	policyID := testCanonicalID(t, "01ARZ3NDEKTSV4RRFFQ69G5FAW")
	head := canonical.Head{Exists: true, CommitSeq: 7}
	requirement := readiness.ProjectionRequirement{
		CapturedHead: readiness.Head{Exists: true, CommitSeq: 7},
		ResidentID:   residentID, SessionPolicyID: policyID,
	}
	statuses := currentHealthStatuses(residentID, head, 7)
	checker := healthProjectionChecker{statuses: fixedHealthStatuses{values: statuses}}
	if current, err := checker.Current(context.Background(), requirement); err != nil || !current {
		t.Fatalf("current = %v, err=%v", current, err)
	}

	invalid := append([]projection.Status(nil), statuses...)
	invalid[0].Head.CommitSeq = 8
	if current, err := (healthProjectionChecker{statuses: fixedHealthStatuses{values: invalid}}).Current(context.Background(), requirement); err != nil || current {
		t.Fatalf("head-mismatched current = %v, err=%v", current, err)
	}
	if current, err := (healthProjectionChecker{statuses: fixedHealthStatuses{values: statuses[:len(statuses)-1]}}).Current(context.Background(), requirement); err != nil || current {
		t.Fatalf("incomplete current = %v, err=%v", current, err)
	}
}

func TestCOV56HealthProjectionCheckerRequiresExactMemoryProjectionTarget(t *testing.T) {
	residentID := testCanonicalID(t, "01ARZ3NDEKTSV4RRFFQ69G5FAV")
	policyID := testCanonicalID(t, "01ARZ3NDEKTSV4RRFFQ69G5FAW")
	head := canonical.Head{Exists: true, CommitSeq: 7}
	requirement := readiness.ProjectionRequirement{
		CapturedHead: readiness.Head{Exists: true, CommitSeq: 7},
		ResidentID:   residentID, SessionPolicyID: policyID,
	}
	base := currentHealthStatuses(residentID, head, 7)

	reversed := cloneHealthStatuses(base)
	slices.Reverse(reversed)
	if current, err := (healthProjectionChecker{statuses: fixedHealthStatuses{values: reversed}}).Current(context.Background(), requirement); err != nil || !current {
		t.Fatalf("reordered exact target current = %v, err=%v", current, err)
	}

	tests := []struct {
		name   string
		mutate func([]projection.Status) []projection.Status
	}{
		{name: "four millisecond as-of skew", mutate: func(values []projection.Status) []projection.Status {
			statusForHealth(t, values, projection.RuntimeStatesName).Watermark.AsOf += canonical.Instant(4_000)
			return values
		}},
		{name: "timezone mismatch", mutate: func(values []projection.Status) []projection.Status {
			statusForHealth(t, values, projection.RuntimeStatesName).Watermark.AsOfTZ = canonical.MustTimezone("Asia/Tokyo")
			return values
		}},
		{name: "source sequence mismatch", mutate: func(values []projection.Status) []projection.Status {
			statusForHealth(t, values, projection.RuntimeStatesName).Watermark.SourceCommitSeq = 6
			return values
		}},
		{name: "nil claim watermark", mutate: func(values []projection.Status) []projection.Status {
			statusForHealth(t, values, projection.ClaimStatesName).Watermark = nil
			return values
		}},
		{name: "missing claim status", mutate: func(values []projection.Status) []projection.Status {
			for index := range values {
				if values[index].Name == projection.ClaimStatesName {
					return append(values[:index], values[index+1:]...)
				}
			}
			return values
		}},
		{name: "duplicate runtime status", mutate: func(values []projection.Status) []projection.Status {
			duplicate := *statusForHealth(t, values, projection.RuntimeStatesName)
			watermark := *duplicate.Watermark
			duplicate.Watermark = &watermark
			return append(values, duplicate)
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			statuses := test.mutate(cloneHealthStatuses(base))
			current, err := (healthProjectionChecker{statuses: fixedHealthStatuses{values: statuses}}).Current(context.Background(), requirement)
			if err != nil || current {
				t.Fatalf("current = %v, err=%v", current, err)
			}
		})
	}
}

func TestCOV56HealthExactTargetGuardFollowsServiceCurrentPolicyGate(t *testing.T) {
	residentID := testCanonicalID(t, "01ARZ3NDEKTSV4RRFFQ69G5FAV")
	policyID := testCanonicalID(t, "01ARZ3NDEKTSV4RRFFQ69G5FAW")
	commitID := testCanonicalID(t, "01ARZ3NDEKTSV4RRFFQ69G5FAX")
	head := readiness.Head{
		Exists: true, CommitID: commitID, CommitSeq: 7,
		CommittedAt: canonical.Instant(1_700_000_000_000_000), CommittedTZ: canonical.MustTimezone("UTC"),
	}
	statuses := currentHealthStatuses(residentID, head.Canonical(), head.CommitSeq)
	statusForHealth(t, statuses, projection.RuntimeStatesName).Watermark.AsOf += canonical.Instant(4_000)

	for _, version := range []string{"memory-policy-v2", "memory-policy-v3"} {
		t.Run(version, func(t *testing.T) {
			state := &serveHealthState{
				source: fixedHealthReadinessSource{snapshot: readiness.Snapshot{
					CapturedHead: head, ActiveResidentID: &residentID, ActiveResidentStatus: "active",
					MemoryPolicyServiceCurrent: false, SessionPolicyID: &policyID,
				}},
				projections: fixedHealthStatuses{err: errors.New("Projection checker must not run")},
			}
			state.MarkStarted()
			result, err := state.Evaluate(context.Background())
			if err != nil || result.Ready || !slices.Equal(result.ReasonCodes, []readiness.ReasonCode{readiness.ReasonMemoryPolicy}) {
				t.Fatalf("result = %+v, err=%v", result, err)
			}
		})
	}

	for _, version := range []string{"memory-policy-v1-disabled", "memory-policy-v4"} {
		t.Run(version, func(t *testing.T) {
			state := &serveHealthState{
				source: fixedHealthReadinessSource{snapshot: readiness.Snapshot{
					CapturedHead: head, ActiveResidentID: &residentID, ActiveResidentStatus: "active",
					MemoryPolicyServiceCurrent: true, SessionPolicyID: &policyID,
				}},
				projections: fixedHealthStatuses{values: statuses},
			}
			state.MarkStarted()
			result, err := state.Evaluate(context.Background())
			if err != nil || result.Ready || !slices.Equal(result.ReasonCodes, []readiness.ReasonCode{readiness.ReasonProjectionCurrent}) {
				t.Fatalf("result = %+v, err=%v", result, err)
			}
		})
	}
}

type fixedHealthReadinessSource struct {
	snapshot readiness.Snapshot
}

func (source fixedHealthReadinessSource) CaptureServiceReadiness(context.Context) (readiness.Snapshot, error) {
	return source.snapshot, nil
}

func currentHealthStatuses(residentID canonical.ID, head canonical.Head, sourceSeq canonical.CommitSeq) []projection.Status {
	statuses := make([]projection.Status, 0, len(projection.ServiceRequiredNames()))
	for _, name := range projection.ServiceRequiredNames() {
		watermark := projection.Watermark{
			ProjectionName: name, ResidentID: residentID, ProjectionVersion: "v1", SourceCommitSeq: sourceSeq,
			AsOf: canonical.Instant(1_700_000_000_000_000), AsOfTZ: canonical.MustTimezone("UTC"),
		}
		statuses = append(statuses, projection.Status{
			Head: head, ResidentID: residentID, Name: name, Version: "v1",
			Built: true, Watermark: &watermark, UpToDate: true,
		})
	}
	return statuses
}

func cloneHealthStatuses(values []projection.Status) []projection.Status {
	cloned := append([]projection.Status(nil), values...)
	for index := range cloned {
		if cloned[index].Watermark != nil {
			watermark := *cloned[index].Watermark
			watermark.Dependencies = append([]projection.Dependency(nil), watermark.Dependencies...)
			cloned[index].Watermark = &watermark
		}
		cloned[index].Dependencies = append([]projection.Dependency(nil), cloned[index].Dependencies...)
	}
	return cloned
}

func statusForHealth(t *testing.T, statuses []projection.Status, name projection.Name) *projection.Status {
	t.Helper()
	for index := range statuses {
		if statuses[index].Name == name {
			return &statuses[index]
		}
	}
	t.Fatalf("status %s not found", name)
	return nil
}

func testCanonicalID(t *testing.T, raw string) canonical.ID {
	t.Helper()
	id, err := canonical.ParseID(raw)
	if err != nil {
		t.Fatal(err)
	}
	return id
}
