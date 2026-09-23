package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"slices"
	"strings"
	"testing"

	"mahoroba.local/mahoroba/internal/blob"
	"mahoroba.local/mahoroba/internal/blobgc"
	blobgcservice "mahoroba.local/mahoroba/internal/blobgc/service"
	"mahoroba.local/mahoroba/internal/canonical"
)

func TestM7BlobGCCLIEmitsOnlyClosedAggregateResultAndWarning(t *testing.T) {
	residentID, result, candidateDigest := commandBlobGCResult(t)
	dataDir := t.TempDir()
	var stdout, stderr bytes.Buffer
	var request blobgcservice.Request
	exit := runBlobGCWithExecutor(context.Background(), []string{
		"--resident", residentID.String(), "--data-dir", dataDir,
	}, &stdout, &stderr, func(_ context.Context, value blobgcservice.Request) (blobgc.Result, error) {
		request = value
		return result, nil
	})
	if exit != 0 {
		t.Fatalf("exit=%d stdout=%s stderr=%s", exit, stdout.String(), stderr.String())
	}
	if request.Apply || request.Confirm != "" {
		t.Fatalf("default blob GC request was not a dry run: apply=%t confirm=%q", request.Apply, request.Confirm)
	}
	for _, required := range []string{
		`"command":"blob.gc"`, `"outcome":"success"`, `"candidate_count":"1"`,
		`"candidate_bytes":"4"`, `"deleted_count":"0"`, `"remaining_count":"1"`,
		`"plan_digest":"` + result.Plan.Digest() + `"`,
	} {
		if !strings.Contains(stdout.String(), required) {
			t.Fatalf("stdout missing %s: %s", required, stdout.String())
		}
	}
	if strings.Contains(stdout.String(), candidateDigest.Hex()) || strings.Contains(stderr.String(), candidateDigest.Hex()) ||
		strings.Contains(stdout.String(), "candidates") || strings.Contains(stdout.String(), "path") || strings.Contains(stdout.String(), "content") {
		t.Fatalf("candidate authority leaked: stdout=%s stderr=%s", stdout.String(), stderr.String())
	}
	if strings.Count(stderr.String(), `"warning_code":"physical_erasure_not_guaranteed"`) != 1 {
		t.Fatalf("physical warning = %s", stderr.String())
	}
}

func TestM7BlobGCCLIPartialRequiresExactlyOneFreshDryRunAction(t *testing.T) {
	residentID, result, _ := commandBlobGCResult(t)
	result.DeletedCount = 1
	result.RemainingCount = 0
	result.PhysicalMutation = true
	dataDir := t.TempDir()
	var stdout, stderr bytes.Buffer
	exit := runBlobGCWithExecutor(context.Background(), []string{
		"--resident", residentID.String(), "--apply", "--confirm", result.Plan.Digest(), "--data-dir", dataDir,
	}, &stdout, &stderr, func(context.Context, blobgcservice.Request) (blobgc.Result, error) {
		return result, &blobgc.PartialError{Cause: errors.New("injected crash")}
	})
	if exit != 1 {
		t.Fatalf("exit=%d stdout=%s stderr=%s", exit, stdout.String(), stderr.String())
	}
	if !strings.Contains(stdout.String(), `"outcome":"partial"`) || !strings.Contains(stdout.String(), `"error_code":"operation_partial"`) ||
		strings.Count(stdout.String(), `"code":"rerun_blob_gc_dry_run"`) != 1 ||
		strings.Contains(stdout.String(), `"--apply"`) || strings.Contains(stdout.String(), `"--confirm"`) {
		t.Fatalf("partial stdout = %s", stdout.String())
	}
	var envelope struct {
		RequiredActions []struct {
			Argv []string `json:"argv"`
		} `json:"required_actions"`
	}
	if err := json.Unmarshal(stdout.Bytes(), &envelope); err != nil {
		t.Fatalf("decode partial envelope: %v: %s", err, stdout.String())
	}
	wantArgv := []string{"mahoroba", "blob", "gc", "--resident", residentID.String(), "--data-dir", dataDir}
	if len(envelope.RequiredActions) != 1 || !slices.Equal(envelope.RequiredActions[0].Argv, wantArgv) {
		t.Fatalf("fresh dry-run argv = %v want %v", envelope.RequiredActions, wantArgv)
	}
	lines := strings.Split(strings.TrimSpace(stderr.String()), "\n")
	if len(lines) != 2 || !strings.Contains(lines[0], `"warning_code":"physical_erasure_not_guaranteed"`) ||
		!strings.Contains(lines[1], `"error_code":"operation_failed"`) {
		t.Fatalf("partial stderr ordering = %q", lines)
	}
}

func TestM7BlobGCCLIRejectsMalformedApplyConfirmationBeforeExecutor(t *testing.T) {
	residentID, _, _ := commandBlobGCResult(t)
	for _, arguments := range [][]string{
		{"--resident", residentID.String(), "--apply", "--data-dir", t.TempDir()},
		{"--resident", residentID.String(), "--confirm", "sha256:" + strings.Repeat("0", 64), "--data-dir", t.TempDir()},
		{"--resident", residentID.String(), "--apply", "--confirm", "sha256:" + strings.Repeat("A", 64), "--data-dir", t.TempDir()},
		{"--resident", residentID.String(), "--resident", residentID.String(), "--data-dir", t.TempDir()},
	} {
		called := false
		var stdout, stderr bytes.Buffer
		exit := runBlobGCWithExecutor(context.Background(), arguments, &stdout, &stderr,
			func(context.Context, blobgcservice.Request) (blobgc.Result, error) {
				called = true
				return blobgc.Result{}, nil
			})
		if exit != 2 || called || stdout.Len() != 0 || !strings.Contains(stderr.String(), "usage: mahoroba blob gc") {
			t.Fatalf("args=%v exit=%d called=%v stdout=%s stderr=%s", arguments, exit, called, stdout.String(), stderr.String())
		}
	}
}

type commandBlobGCRepository struct{ snapshot blobgc.Snapshot }

func (repository commandBlobGCRepository) Capture(context.Context, canonical.ID, []blob.FinalObject) (blobgc.Snapshot, error) {
	return repository.snapshot, nil
}
func (commandBlobGCRepository) BeginCandidate(context.Context, blobgc.Candidate, blobgc.CapturedHead) (blobgc.CandidateTransaction, error) {
	return nil, errors.New("unexpected apply")
}
func (commandBlobGCRepository) Maintenance(context.Context) error { return nil }

type commandBlobGCFiles struct{}

func (commandBlobGCFiles) WalkFinal(context.Context, canonical.ID) ([]blob.FinalObject, error) {
	return []blob.FinalObject{}, nil
}
func (commandBlobGCFiles) RemoveFinal(context.Context, blob.FinalObject) (bool, error) {
	return false, errors.New("unexpected remove")
}

func commandBlobGCResult(t *testing.T) (canonical.ID, blobgc.Result, canonical.Digest) {
	t.Helper()
	residentID := commandBlobGCID(t, "01J00000000000000000000001")
	commitID := commandBlobGCID(t, "01J00000000000000000000002")
	seq, _ := canonical.NewCommitSeq(1)
	at := canonical.Instant(100)
	tz := canonical.MustTimezone("UTC")
	digest := canonical.HashBlob([]byte("test"))
	size, _ := canonical.NewByteSize(4)
	repository := commandBlobGCRepository{snapshot: blobgc.Snapshot{
		CapturedHead: blobgc.CapturedHead{
			Exists: true, CommitID: &commitID, CommitSeq: &seq, CommittedAt: &at, CommittedTZ: &tz,
		},
		ProjectionName: blobgc.ProjectionName, ProjectionVersion: blobgc.ProjectionVersion,
		DependencyVersions: []blobgc.DependencyVersion{},
		Locators: []blobgc.LocatorState{{
			ResidentID: residentID, HashAlgorithm: canonical.HashAlgorithm, Digest: digest,
			SQLitePresent: true, SQLiteByteSize: &size,
		}},
	}}
	result, err := blobgc.Execute(context.Background(), repository, commandBlobGCFiles{}, blobgc.Request{ResidentID: residentID})
	if err != nil {
		t.Fatal(err)
	}
	return residentID, result, digest
}

func commandBlobGCID(t *testing.T, raw string) canonical.ID {
	t.Helper()
	id, err := canonical.ParseID(raw)
	if err != nil {
		t.Fatal(err)
	}
	return id
}
