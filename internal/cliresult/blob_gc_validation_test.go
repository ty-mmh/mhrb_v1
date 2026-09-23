package cliresult

import (
	"path/filepath"
	"testing"
)

func TestM7BlobGCResultAndPartialActionAreClosed(t *testing.T) {
	result := validBlobGCValidationResult()
	warning, err := NewWarning(WarningPhysicalErasureNotGuaranteed, result.ResidentID)
	if err != nil {
		t.Fatal(err)
	}
	action, err := NewRequiredAction(ActionRerunBlobGCDryRun, []string{
		"mahoroba", "blob", "gc", "--resident", result.ResidentID,
		"--data-dir", filepath.Clean(t.TempDir()),
	}, []string{result.ResidentID}, nil)
	if err != nil {
		t.Fatal(err)
	}
	partial := NewPartial(CommandBlobGC, ErrorOperationFailed, StageCanonical, result)
	partial.TargetIDs = []string{result.ResidentID}
	partial.Warnings = []Warning{warning}
	partial.RequiredActions = []RequiredAction{action}
	if err := partial.Validate(); err != nil {
		t.Fatalf("valid partial rejected: %v", err)
	}

	missingAction := partial
	missingAction.RequiredActions = []RequiredAction{}
	if err := missingAction.Validate(); err == nil {
		t.Fatal("partial blob GC without fresh dry-run action was accepted")
	}
	wrongCounts := *result
	wrongCounts.RemainingCount = "0"
	success := NewSuccess(CommandBlobGC, &wrongCounts)
	success.Warnings = []Warning{warning}
	if err := success.Validate(); err == nil {
		t.Fatal("blob GC count relationship mismatch was accepted")
	}
	success = NewSuccess(CommandBlobGC, result)
	success.Warnings = []Warning{warning}
	success.RequiredActions = []RequiredAction{action}
	if err := success.Validate(); err == nil {
		t.Fatal("successful blob GC with a retry action was accepted")
	}
}

func validBlobGCValidationResult() *BlobGCResult {
	commitID := "01J00000000000000000000002"
	commitSeq := "1"
	committedAt := "100"
	timezone := "UTC"
	return &BlobGCResult{
		ResidentID: "01J00000000000000000000001",
		CapturedHead: Head{
			Exists: true, CommitID: &commitID, CommitSeq: &commitSeq,
			CommittedAt: &committedAt, CommittedTZ: &timezone,
		},
		PlanDigest:     "sha256:0000000000000000000000000000000000000000000000000000000000000000",
		CandidateCount: "1", CandidateBytes: "4", DeletedCount: "0", RemainingCount: "1",
	}
}
