package domain

import (
	"testing"
	"time"

	"mahoroba.local/mahoroba/internal/canonical"
)

func TestProductionDialogueDiscoveryBudget(t *testing.T) {
	budget := ProductionDialogueDiscoveryBudget()
	if budget.RecentCandidates != 128 || budget.OlderPageSize != 128 ||
		budget.OlderCandidates != 1024 || budget.OlderPages != 8 ||
		budget.Elapsed != 2*time.Second {
		t.Fatalf("production dialogue discovery budget = %+v", budget)
	}
	if err := budget.Validate(); err != nil {
		t.Fatalf("production dialogue discovery budget is invalid: %v", err)
	}
}

func TestDialogueDiscoveryBudgetValidation(t *testing.T) {
	valid := DialogueDiscoveryBudget{
		RecentCandidates: 2,
		OlderPageSize:    2,
		OlderCandidates:  4,
		OlderPages:       2,
		Elapsed:          time.Second,
	}
	tests := []struct {
		name   string
		mutate func(*DialogueDiscoveryBudget)
	}{
		{name: "zero elapsed", mutate: func(budget *DialogueDiscoveryBudget) { budget.Elapsed = 0 }},
		{name: "negative elapsed", mutate: func(budget *DialogueDiscoveryBudget) { budget.Elapsed = -time.Nanosecond }},
		{name: "zero recent candidates", mutate: func(budget *DialogueDiscoveryBudget) { budget.RecentCandidates = 0 }},
		{name: "page exceeds candidates", mutate: func(budget *DialogueDiscoveryBudget) { budget.OlderCandidates = 1 }},
		{name: "candidates exceed page capacity", mutate: func(budget *DialogueDiscoveryBudget) { budget.OlderCandidates = 5 }},
		{name: "production upper bound", mutate: func(budget *DialogueDiscoveryBudget) { budget.OlderPages = 9 }},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			budget := valid
			test.mutate(&budget)
			if err := budget.Validate(); err == nil {
				t.Fatalf("budget was accepted: %+v", budget)
			}
		})
	}
}

func TestDialogueDiscoveryRequestValidation(t *testing.T) {
	request := DialogueDiscoveryRequest{
		MaxAttempts: 1,
		Budget: DialogueDiscoveryBudget{
			RecentCandidates: 1,
			OlderPageSize:    1,
			OlderCandidates:  1,
			OlderPages:       1,
			Elapsed:          time.Second,
		},
	}
	if err := request.Validate(); err != nil {
		t.Fatalf("valid request: %v", err)
	}

	request.MaxAttempts = 0
	if err := request.Validate(); err == nil {
		t.Fatal("zero maximum attempt count was accepted")
	}
	request.MaxAttempts = 1
	request.Cursor = &DialogueDiscoveryCursor{BeforeSeq: canonical.Seq(0)}
	if err := request.Validate(); err == nil {
		t.Fatal("invalid cursor was accepted")
	}
}
