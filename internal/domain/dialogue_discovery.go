package domain

import (
	"errors"
	"fmt"
	"time"

	"mahoroba.local/mahoroba/internal/canonical"
)

const (
	productionDialogueRecentCandidates = 128
	productionDialogueOlderPageSize    = 128
	productionDialogueOlderCandidates  = 1024
	productionDialogueOlderPages       = 8
)

const productionDialogueDiscoveryElapsed = 2 * time.Second

// DialogueDiscoveryCursor is an Operational, process-local keyset cursor. It
// is deliberately not Canonical: losing it may repeat work, but cannot skip a
// durable dialogue obligation.
type DialogueDiscoveryCursor struct {
	BeforeSeq canonical.Seq
}

func (cursor DialogueDiscoveryCursor) Validate() error {
	if err := cursor.BeforeSeq.Validate(); err != nil {
		return fmt.Errorf("dialogue discovery cursor: %w", err)
	}
	return nil
}

// DialogueDiscoveryBudget bounds one repository pass independently of the
// amount of Canonical event history. A caller may choose smaller positive
// values for focused work or deterministic tests.
type DialogueDiscoveryBudget struct {
	RecentCandidates int
	OlderPageSize    int
	OlderCandidates  int
	OlderPages       int
	Elapsed          time.Duration
}

func ProductionDialogueDiscoveryBudget() DialogueDiscoveryBudget {
	return DialogueDiscoveryBudget{
		RecentCandidates: productionDialogueRecentCandidates,
		OlderPageSize:    productionDialogueOlderPageSize,
		OlderCandidates:  productionDialogueOlderCandidates,
		OlderPages:       productionDialogueOlderPages,
		Elapsed:          productionDialogueDiscoveryElapsed,
	}
}

func (budget DialogueDiscoveryBudget) Validate() error {
	if budget.RecentCandidates < 1 || budget.OlderPageSize < 1 ||
		budget.OlderCandidates < 1 || budget.OlderPages < 1 {
		return errors.New("dialogue discovery budget requires positive row and page limits")
	}
	if budget.Elapsed <= 0 {
		return errors.New("dialogue discovery budget requires a positive elapsed limit")
	}
	if budget.RecentCandidates > productionDialogueRecentCandidates ||
		budget.OlderPageSize > productionDialogueOlderPageSize ||
		budget.OlderCandidates > productionDialogueOlderCandidates ||
		budget.OlderPages > productionDialogueOlderPages ||
		budget.Elapsed > productionDialogueDiscoveryElapsed {
		return errors.New("dialogue discovery budget exceeds the production upper bound")
	}
	if budget.OlderPageSize > budget.OlderCandidates {
		return errors.New("dialogue discovery page size exceeds its candidate budget")
	}
	if budget.OlderCandidates > budget.OlderPageSize*budget.OlderPages {
		return errors.New("dialogue discovery candidate budget exceeds its page budget")
	}
	return nil
}

type DialogueDiscoveryRequest struct {
	Cursor      *DialogueDiscoveryCursor
	MaxAttempts int
	Budget      DialogueDiscoveryBudget
}

func (request DialogueDiscoveryRequest) Validate() error {
	if request.MaxAttempts < 1 {
		return errors.New("dialogue discovery requires a positive maximum attempt count")
	}
	if err := request.Budget.Validate(); err != nil {
		return err
	}
	if request.Cursor != nil {
		if err := request.Cursor.Validate(); err != nil {
			return err
		}
	}
	return nil
}

// DialogueDiscoveryResult reports both work and the exact bounded scan
// progress that produced it. When ActionablePage is true, NextCursor is the
// boundary before that page, so a one-at-a-time scheduler can retain it until
// every actionable sibling on the page has reached a durable next state.
type DialogueDiscoveryResult struct {
	Work             []DialogueWork
	NextCursor       *DialogueDiscoveryCursor
	RecentScanned    int
	OlderScanned     int
	OlderPageQueries int
	CycleComplete    bool
	ActionablePage   bool
	BudgetExhausted  bool
}
