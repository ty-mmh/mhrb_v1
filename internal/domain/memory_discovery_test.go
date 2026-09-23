package domain

import (
	"testing"
	"time"

	"mahoroba.local/mahoroba/internal/canonical"
)

func TestProductionMemoryDiscoveryBudget(t *testing.T) {
	budget := ProductionMemoryDiscoveryBudget()
	if budget.PageSize != 128 || budget.Candidates != 1024 || budget.Pages != 8 ||
		budget.Elapsed != 2*time.Second {
		t.Fatalf("production memory discovery budget = %+v", budget)
	}
	if err := budget.Validate(); err != nil {
		t.Fatalf("production memory discovery budget validation: %v", err)
	}
}

func TestMemoryDiscoveryRequestValidation(t *testing.T) {
	through := canonical.Seq(8)
	valid := MemoryDiscoveryRequest{
		ThroughSeq:  &through,
		MaxAttempts: 3,
		Budget: MemoryDiscoveryBudget{
			PageSize: 2, Candidates: 4, Pages: 2, Elapsed: time.Second,
		},
	}
	if err := valid.Validate(); err != nil {
		t.Fatalf("valid request: %v", err)
	}

	tests := []struct {
		name   string
		mutate func(*MemoryDiscoveryRequest)
	}{
		{name: "zero through", mutate: func(request *MemoryDiscoveryRequest) {
			zero := canonical.Seq(0)
			request.ThroughSeq = &zero
		}},
		{name: "zero attempts", mutate: func(request *MemoryDiscoveryRequest) { request.MaxAttempts = 0 }},
		{name: "zero elapsed", mutate: func(request *MemoryDiscoveryRequest) { request.Budget.Elapsed = 0 }},
		{name: "page exceeds candidates", mutate: func(request *MemoryDiscoveryRequest) { request.Budget.Candidates = 1 }},
		{name: "candidates exceed pages", mutate: func(request *MemoryDiscoveryRequest) { request.Budget.Candidates = 5 }},
		{name: "production bound", mutate: func(request *MemoryDiscoveryRequest) { request.Budget.Pages = 9 }},
		{name: "cursor after cycle", mutate: func(request *MemoryDiscoveryRequest) {
			after := canonical.Seq(9)
			request.Cursor = &MemoryDiscoveryCursor{AfterSeq: &after, CycleThroughSeq: canonical.Seq(8)}
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			request := valid
			test.mutate(&request)
			if err := request.Validate(); err == nil {
				t.Fatalf("invalid request was accepted: %+v", request)
			}
		})
	}
}

func TestMemoryDiscoveryRequestAllowsRepositoryCapturedCeiling(t *testing.T) {
	request := MemoryDiscoveryRequest{
		MaxAttempts: 3,
		Budget: MemoryDiscoveryBudget{
			PageSize: 2, Candidates: 4, Pages: 2, Elapsed: time.Second,
		},
	}
	if err := request.Validate(); err != nil {
		t.Fatalf("repository-captured ceiling request: %v", err)
	}
}

func TestMemoryDiscoveryCursorAllowsStartAndCompletedBoundary(t *testing.T) {
	start := MemoryDiscoveryCursor{CycleThroughSeq: canonical.Seq(4)}
	if err := start.Validate(); err != nil {
		t.Fatalf("start cursor: %v", err)
	}
	after := canonical.Seq(4)
	complete := MemoryDiscoveryCursor{AfterSeq: &after, CycleThroughSeq: canonical.Seq(4)}
	if err := complete.Validate(); err != nil {
		t.Fatalf("completed-boundary cursor: %v", err)
	}
}

func TestMemoryReextractionDiscoveryRequestAndCursorValidation(t *testing.T) {
	commitID := mustMemoryDiscoveryID(t, "01J00000000000000000000001")
	runID := mustMemoryDiscoveryID(t, "01J00000000000000000000002")
	completed := canonical.CommitSeq(19)
	request := MemoryReextractionDiscoveryRequest{
		Cursor: &MemoryReextractionDiscoveryCursor{
			CycleThroughCommitSeq:     canonical.CommitSeq(20),
			CompletedThroughCommitSeq: &completed,
			ActiveCommit: &MemoryReextractionCommitCursor{
				CommitID: commitID, CommitSeq: canonical.CommitSeq(20), AfterRunID: &runID,
			},
		},
		MaxAttempts: 3,
		Budget: MemoryDiscoveryBudget{
			PageSize: 2, Candidates: 4, Pages: 2, Elapsed: time.Second,
		},
	}
	if err := request.Validate(); err != nil {
		t.Fatalf("valid re-extraction request: %v", err)
	}

	tests := []struct {
		name   string
		mutate func(*MemoryReextractionDiscoveryRequest)
	}{
		{name: "zero attempts", mutate: func(value *MemoryReextractionDiscoveryRequest) { value.MaxAttempts = 0 }},
		{name: "invalid cycle commit", mutate: func(value *MemoryReextractionDiscoveryRequest) {
			value.Cursor = &MemoryReextractionDiscoveryCursor{
				CycleThroughCommitSeq: 0,
			}
		}},
		{name: "completed after cycle", mutate: func(value *MemoryReextractionDiscoveryRequest) {
			after := canonical.CommitSeq(21)
			value.Cursor = &MemoryReextractionDiscoveryCursor{
				CycleThroughCommitSeq: canonical.CommitSeq(20), CompletedThroughCommitSeq: &after,
			}
		}},
		{name: "invalid active commit ID", mutate: func(value *MemoryReextractionDiscoveryRequest) {
			value.Cursor = &MemoryReextractionDiscoveryCursor{
				CycleThroughCommitSeq: canonical.CommitSeq(20),
				ActiveCommit:          &MemoryReextractionCommitCursor{CommitSeq: canonical.CommitSeq(20)},
			}
		}},
		{name: "active after cycle", mutate: func(value *MemoryReextractionDiscoveryRequest) {
			value.Cursor = &MemoryReextractionDiscoveryCursor{
				CycleThroughCommitSeq: canonical.CommitSeq(20),
				ActiveCommit: &MemoryReextractionCommitCursor{
					CommitID: commitID, CommitSeq: canonical.CommitSeq(21),
				},
			}
		}},
		{name: "active not after completed", mutate: func(value *MemoryReextractionDiscoveryRequest) {
			at := canonical.CommitSeq(20)
			value.Cursor = &MemoryReextractionDiscoveryCursor{
				CycleThroughCommitSeq:     canonical.CommitSeq(20),
				CompletedThroughCommitSeq: &at,
				ActiveCommit: &MemoryReextractionCommitCursor{
					CommitID: commitID, CommitSeq: canonical.CommitSeq(20),
				},
			}
		}},
		{name: "invalid active run position", mutate: func(value *MemoryReextractionDiscoveryRequest) {
			zero := canonical.ID{}
			value.Cursor = &MemoryReextractionDiscoveryCursor{
				CycleThroughCommitSeq: canonical.CommitSeq(20),
				ActiveCommit: &MemoryReextractionCommitCursor{
					CommitID: commitID, CommitSeq: canonical.CommitSeq(20), AfterRunID: &zero,
				},
			}
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			candidate := request
			test.mutate(&candidate)
			if err := candidate.Validate(); err == nil {
				t.Fatalf("invalid re-extraction request was accepted: %+v", candidate)
			}
		})
	}
}

func TestMemoryReextractionDiscoveryCursorAllowsCompletedCeilingAndActiveCommit(t *testing.T) {
	completed := canonical.CommitSeq(20)
	finished := MemoryReextractionDiscoveryCursor{
		CycleThroughCommitSeq: canonical.CommitSeq(20), CompletedThroughCommitSeq: &completed,
	}
	if err := finished.Validate(); err != nil {
		t.Fatalf("completed-ceiling cursor: %v", err)
	}

	completed = canonical.CommitSeq(19)
	active := MemoryReextractionDiscoveryCursor{
		CycleThroughCommitSeq:     canonical.CommitSeq(20),
		CompletedThroughCommitSeq: &completed,
		ActiveCommit: &MemoryReextractionCommitCursor{
			CommitID:  mustMemoryDiscoveryID(t, "01J00000000000000000000001"),
			CommitSeq: canonical.CommitSeq(20),
		},
	}
	if err := active.Validate(); err != nil {
		t.Fatalf("active-commit cursor: %v", err)
	}
}

func mustMemoryDiscoveryID(t *testing.T, raw string) canonical.ID {
	t.Helper()
	id, err := canonical.ParseID(raw)
	if err != nil {
		t.Fatalf("parse test ID %q: %v", raw, err)
	}
	return id
}
