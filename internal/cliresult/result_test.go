package cliresult

import (
	"bytes"
	"encoding/json"
	"strings"
	"testing"
)

const (
	testResident = "01ARZ3NDEKTSV4RRFFQ69G5FAV"
	testCommit   = "01ARZ3NDEKTSV4RRFFQ69G5FAW"
	testMarker   = "01ARZ3NDEKTSV4RRFFQ69G5FAX"
)

func TestM7CLIResultSuccessGoldenAndWarningOneToOne(t *testing.T) {
	warning, err := NewWarning(WarningRuntimeUnmanagedCopy)
	if err != nil {
		t.Fatal(err)
	}
	envelope := NewSuccess(CommandExportJSONL, &ExportJSONLResult{
		ArtifactPath:       "/var/lib/mahoroba/export.jsonl",
		FormatVersion:      JSONLFormatVersion,
		CapturedHead:       testHead(),
		RecordCount:        "12",
		ByteCount:          "4096",
		ExternalCopyNotice: ExternalCopyNotice,
	})
	envelope.Warnings = []Warning{warning}

	var stdout, stderr bytes.Buffer
	exit, err := Render(&stdout, &stderr, envelope)
	if err != nil || exit != ExitSuccess {
		t.Fatalf("Render() = exit %d, err %v", exit, err)
	}
	wantStdout := `{"format_version":"mahoroba-cli-result-v1","command":"export.jsonl","outcome":"success","canonical_applied":false,"canonical_commits":[],"required_actions":[],"warnings":[{"warning_code":"runtime_unmanaged_copy","target_ids":[]}],"result":{"artifact_path":"/var/lib/mahoroba/export.jsonl","format_version":"mahoroba-jsonl-v1","captured_head":{"exists":true,"commit_id":"01ARZ3NDEKTSV4RRFFQ69G5FAW","commit_seq":"7","committed_at":"1787220000000000","committed_tz":"UTC"},"record_count":"12","byte_count":"4096","external_copy_notice":"runtime_unmanaged_copy_not_covered_by_future_erasure"}}` + "\n"
	if stdout.String() != wantStdout {
		t.Fatalf("stdout mismatch\n got: %s\nwant: %s", stdout.String(), wantStdout)
	}
	wantWarning := `{"command":"export.jsonl","format_version":"mahoroba-cli-result-v1","outcome":"warning","target_ids":[],"warning_code":"runtime_unmanaged_copy"}` + "\n"
	if stderr.String() != wantWarning {
		t.Fatalf("stderr mismatch\n got: %s\nwant: %s", stderr.String(), wantWarning)
	}
}

func TestM7CLIResultPartialKeepsFamilyResultAndWritesRootErrorLast(t *testing.T) {
	warning, err := NewWarning(WarningProjectionNotApplicable, testResident)
	if err != nil {
		t.Fatal(err)
	}
	envelope := NewPartial(CommandAdminErasureApply, ErrorProjectionNotCurrent, StageDerived, &ErasureApplyResult{
		ExistingCommit:             false,
		CanonicalErasureCommitID:   testCommit,
		CanonicalErasureCommitSeq:  "7",
		ProjectionTargetHead:       testHead(),
		TargetCount:                "2",
		MandatoryWorkFollowupCount: "1",
		ProjectionState:            ProjectionStateNotCurrent,
	})
	envelope.CanonicalApplied = true
	envelope.CanonicalCommits = []CanonicalCommit{{
		ResidentID:  stringPointer(testResident),
		CommitID:    testCommit,
		CommitSeq:   "7",
		Disposition: DispositionCreated,
		Effects:     []EffectCode{EffectClaimIdentityErasureRecorded, EffectContentErased},
	}}
	envelope.TargetIDs = []string{testResident}
	envelope.Warnings = []Warning{warning}

	var stdout, stderr bytes.Buffer
	exit, err := Render(&stdout, &stderr, envelope)
	if err != nil || exit != ExitOperational {
		t.Fatalf("Render() = exit %d, err %v", exit, err)
	}
	if strings.Count(stdout.String(), "\n") != 1 {
		t.Fatalf("partial stdout is not exactly one object: %q", stdout.String())
	}
	stderrLines := strings.Split(strings.TrimSuffix(stderr.String(), "\n"), "\n")
	if len(stderrLines) != 2 || !strings.Contains(stderrLines[0], `"outcome":"warning"`) ||
		!strings.Contains(stderrLines[1], `"error_code":"projection_not_current"`) {
		t.Fatalf("partial stderr order = %q", stderr.String())
	}
	var partial, terminal map[string]any
	if err := json.Unmarshal(stdout.Bytes(), &partial); err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal([]byte(stderrLines[1]), &terminal); err != nil {
		t.Fatal(err)
	}
	if partial["outcome"] != "partial" || partial["error_code"] != "operation_partial" ||
		terminal["outcome"] != "error" {
		t.Fatalf("partial/root classification = %#v / %#v", partial, terminal)
	}
	for _, key := range []string{"canonical_applied", "canonical_commits", "required_actions", "warnings", "result"} {
		if !deepJSONEqual(partial[key], terminal[key]) {
			t.Fatalf("%s differs between partial and terminal error", key)
		}
	}
}

func TestM7CLIResultErrorAndUsageKeepStdoutEmpty(t *testing.T) {
	notReady := false
	reason := HealthReasonIntegrityBlocked
	checked := "1787220000000000"
	healthError := NewFailure(CommandHealthcheck, ErrorHealthcheckNotReady, StageReadiness, &ErrorResult{
		ErrorStage:          StageReadiness,
		Ready:               &notReady,
		ReasonCode:          &reason,
		CheckedAtUnixMicros: &checked,
	})
	var stdout, stderr bytes.Buffer
	exit, err := Render(&stdout, &stderr, healthError)
	if err != nil || exit != ExitOperational || stdout.Len() != 0 {
		t.Fatalf("health error = exit %d stdout %q err %v", exit, stdout.String(), err)
	}
	want := `{"canonical_applied":false,"canonical_commits":[],"command":"healthcheck","error_code":"healthcheck_not_ready","format_version":"mahoroba-cli-result-v1","outcome":"error","required_actions":[],"result":{"artifact_path":null,"captured_head":null,"checked_at_unix_micros":"1787220000000000","error_stage":"readiness","published":null,"ready":false,"reason_code":"integrity_readiness_blocked","staging_marker_id":null},"target_ids":[],"warnings":[]}` + "\n"
	if stderr.String() != want {
		t.Fatalf("health stderr\n got: %s\nwant: %s", stderr.String(), want)
	}

	stdout.Reset()
	stderr.Reset()
	usage := NewFailure(CommandUnknown, ErrorCLIUsage, StageUsage, nil)
	exit, err = Render(&stdout, &stderr, usage)
	if err != nil || exit != ExitUsage || stdout.Len() != 0 {
		t.Fatalf("usage = exit %d stdout %q err %v", exit, stdout.String(), err)
	}
	lines := strings.Split(strings.TrimSuffix(stderr.String(), "\n"), "\n")
	if len(lines) != 2 || lines[0] != "usage: mahoroba <command> [options]" ||
		!strings.Contains(lines[1], `"error_code":"cli_usage"`) {
		t.Fatalf("usage stderr = %q", stderr.String())
	}
}

func TestM7CLIResultFailClosedBeforeWritingRawOrMalformedFields(t *testing.T) {
	envelope := NewFailure(CommandBackupCreate, ErrorOperationFailed, StagePreflight, nil)
	envelope.TargetIDs = []string{"raw statement bytes must never escape"}
	var stdout, stderr bytes.Buffer
	if exit, err := Render(&stdout, &stderr, envelope); err == nil || exit != ExitOperational {
		t.Fatalf("malformed target accepted: exit=%d err=%v", exit, err)
	}
	if stdout.Len() != 0 || stderr.Len() != 0 {
		t.Fatalf("invalid boundary wrote output: stdout=%q stderr=%q", stdout.String(), stderr.String())
	}

	envelope = NewSuccess(CommandHealthcheck, &HealthcheckResult{
		Ready: true, ReasonCode: HealthReasonReady, CheckedAtUnixMicros: "1",
	})
	envelope.FormatVersion = "mahoroba-cli-result-v2"
	if _, err := Render(&stdout, &stderr, envelope); err == nil {
		t.Fatal("future format unexpectedly accepted")
	}
}

func TestM7CLIResultCanonicalCommitAndWarningOrderAreEnforced(t *testing.T) {
	envelope := NewSuccess(CommandAdminIntegrityScan, &IntegrityScanResult{
		CapturedHead: testHead(), ResultHead: testHead(), ExistingFindings: "0",
		CreatedFindings: "0", CreatedQuarantines: "0",
	})
	envelope.CanonicalApplied = true
	envelope.CanonicalCommits = []CanonicalCommit{{
		ResidentID: stringPointer(testResident), CommitID: testCommit, CommitSeq: "7",
		Disposition: DispositionCreated,
		Effects:     []EffectCode{EffectContentErased, EffectClaimIdentityErasureRecorded},
	}}
	if err := envelope.Validate(); err == nil {
		t.Fatal("unsorted effects unexpectedly accepted")
	}
	envelope.CanonicalCommits[0].Effects = []EffectCode{EffectClaimIdentityErasureRecorded, EffectContentErased}
	first, _ := NewWarning(WarningServiceNotReady)
	second, _ := NewWarning(WarningRuntimeUnmanagedCopy)
	envelope.Warnings = []Warning{first, second}
	if err := envelope.Validate(); err == nil {
		t.Fatal("unsorted warnings unexpectedly accepted")
	}
}

func TestM7CLIResultCommandFamilyMatrixIsClosed(t *testing.T) {
	digest := "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	start, err := NewRequiredAction(ActionStartService,
		[]string{"mahoroba", "serve", "--data-dir", "/var/lib/mahoroba-restored"}, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	exportWarning, _ := NewWarning(WarningRuntimeUnmanagedCopy)
	gcWarning, _ := NewWarning(WarningPhysicalErasureNotGuaranteed, testResident)
	cases := []Envelope{
		NewSuccess(CommandAdminIntegrityScan, &IntegrityScanResult{
			CapturedHead: testHead(), ResultHead: testHead(), ExistingFindings: "0",
			CreatedFindings: "1", CreatedQuarantines: "1",
		}),
		NewSuccess(CommandBackupCreate, &BackupResult{
			ArtifactPath: "/var/lib/mahoroba/backup", FormatVersion: BackupFormatVersion,
			CapturedHead: testHead(), FileCount: "3", ByteCount: "12",
		}),
		NewSuccess(CommandBackupVerify, &BackupResult{
			ArtifactPath: "/var/lib/mahoroba/backup", FormatVersion: BackupFormatVersion,
			CapturedHead: testHead(), FileCount: "3", ByteCount: "12",
		}),
		NewSuccess(CommandBackupRestore, validRestoreResult(true)),
		NewSuccess(CommandExportJSONL, &ExportJSONLResult{
			ArtifactPath: "/var/lib/mahoroba/export.jsonl", FormatVersion: JSONLFormatVersion,
			CapturedHead: testHead(), RecordCount: "1", ByteCount: "2", ExternalCopyNotice: ExternalCopyNotice,
		}),
		NewSuccess(CommandAdminRecoveryTerminalize, &RecoveryTerminalizeResult{
			AllExisting: true, TerminalizedAttempts: "0", CancelledMandatoryWork: "0", ResultHead: testHead(),
		}),
		NewSuccess(CommandAdminSessionPolicySelect, &SessionPolicySelectResult{
			SelectedVersionID: testMarker, ServiceReady: true,
		}),
		NewSuccess(CommandAdminErasurePlanContent, &ErasurePlanResult{
			ArtifactPath: "/var/lib/mahoroba/content-plan.json", PlanState: "ready", BaseHead: testHead(),
			Digest: digest, TargetCount: "1", ImpactCounts: ImpactCounts{Safe: "0", NeedsRebuild: "0", NeedsReview: "0", MustErase: "1"}, BlockerCount: "0",
		}),
		NewSuccess(CommandAdminErasurePlanResident, &ErasurePlanResult{
			ArtifactPath: "/var/lib/mahoroba/resident-plan.json", PlanState: "review_required", BaseHead: testHead(),
			Digest: digest, TargetCount: "1", ImpactCounts: ImpactCounts{Safe: "1", NeedsRebuild: "1", NeedsReview: "1", MustErase: "1"}, BlockerCount: "1",
		}),
		NewSuccess(CommandAdminErasureDecide, &ErasurePlanResult{
			ArtifactPath: "/var/lib/mahoroba/decided-plan.json", PlanState: "ready", BaseHead: testHead(),
			Digest: digest, TargetCount: "1", ImpactCounts: ImpactCounts{Safe: "0", NeedsRebuild: "0", NeedsReview: "0", MustErase: "1"}, BlockerCount: "0",
		}),
		NewSuccess(CommandAdminErasureApply, &ErasureApplyResult{
			CanonicalErasureCommitID: testCommit, CanonicalErasureCommitSeq: "7",
			ProjectionTargetHead: testHead(), TargetCount: "1", MandatoryWorkFollowupCount: "0",
			ProjectionState: ProjectionStateCurrent,
		}),
		NewSuccess(CommandBlobGC, &BlobGCResult{
			ResidentID: testResident, CapturedHead: testHead(), PlanDigest: digest,
			CandidateCount: "0", CandidateBytes: "0", DeletedCount: "0", RemainingCount: "0",
		}),
		NewSuccess(CommandAdminDiagnostics, unknownDiagnosticsResult()),
		NewSuccess(CommandHealthcheck, &HealthcheckResult{
			Ready: true, ReasonCode: HealthReasonReady, CheckedAtUnixMicros: "1",
		}),
	}
	cases[3].RequiredActions = []RequiredAction{start}
	cases[4].Warnings = []Warning{exportWarning}
	cases[10].CanonicalApplied = true
	cases[10].CanonicalCommits = []CanonicalCommit{{
		ResidentID: stringPointer(testResident), CommitID: testCommit, CommitSeq: "7",
		Disposition: DispositionCreated, Effects: []EffectCode{EffectContentErased},
	}}
	cases[11].Warnings = []Warning{gcWarning}
	for _, envelope := range cases {
		if err := envelope.Validate(); err != nil {
			t.Errorf("%s valid family rejected: %v", envelope.Command, err)
		}
	}
	wrong := NewSuccess(CommandBackupVerify, &HealthcheckResult{
		Ready: true, ReasonCode: HealthReasonReady, CheckedAtUnixMicros: "1",
	})
	if err := wrong.Validate(); err == nil {
		t.Fatal("cross-family result unexpectedly accepted")
	}
}

func TestM7CLIResultErrorCodeAllowlistIsExhaustive(t *testing.T) {
	cases := map[ErrorCode]struct {
		command CommandID
		stage   Stage
	}{
		ErrorAdminLockBusy:                           {CommandAdminIntegrityScan, StagePreflight},
		ErrorResidentAdmissionClosed:                 {CommandAdminIntegrityScan, StagePreflight},
		ErrorConfigurationInvalid:                    {CommandHealthcheck, StagePreflight},
		ErrorSourceUnavailable:                       {CommandBackupVerify, StagePreflight},
		ErrorArtifactTargetExists:                    {CommandExportJSONL, StagePreflight},
		ErrorArtifactIOFailed:                        {CommandExportJSONL, StagePublish},
		ErrorIntegrityFatal:                          {CommandAdminIntegrityScan, StagePreflight},
		ErrorIntegrityApplyConflict:                  {CommandAdminIntegrityScan, StageCanonical},
		ErrorBackupTargetExists:                      {CommandBackupCreate, StagePreflight},
		ErrorBackupInvalid:                           {CommandBackupVerify, StagePreflight},
		ErrorRestoreTargetExists:                     {CommandBackupRestore, StagePreflight},
		ErrorRestoreNamespaceBusy:                    {CommandBackupRestore, StagePreflight},
		ErrorPublishDurabilityUnknown:                {CommandExportJSONL, StagePublish},
		ErrorLegacyClaimIdentityRequiresStaticRepair: {CommandAdminIntegrityScan, StagePreflight},
		ErrorIntegrityPipelineRequired:               {CommandAdminIntegrityScan, StageCanonical},
		ErrorErasureReviewRequired:                   {CommandAdminErasureDecide, StagePreflight},
		ErrorErasurePlanStale:                        {CommandAdminErasureApply, StageCanonical},
		ErrorErasureIneligible:                       {CommandAdminErasureApply, StagePreflight},
		ErrorErasureRetryConflict:                    {CommandAdminErasureApply, StageCanonical},
		ErrorProjectionNotCurrent:                    {CommandAdminErasureApply, StageDerived},
		ErrorGCPlanStale:                             {CommandBlobGC, StageCanonical},
		ErrorStaticDesignReopenRequired:              {CommandAdminIntegrityScan, StagePreflight},
		ErrorCLIUsage:                                {CommandUnknown, StageUsage},
		ErrorOperationPartial:                        {CommandAdminIntegrityScan, StageCanonical},
		ErrorRecoveryAttemptOverflow:                 {CommandAdminRecoveryTerminalize, StageCanonical},
		ErrorHealthcheckNotReady:                     {CommandHealthcheck, StageReadiness},
		ErrorHealthcheckUnreachable:                  {CommandHealthcheck, StagePreflight},
		ErrorHealthcheckInvalidResponse:              {CommandHealthcheck, StagePreflight},
		ErrorSecretSourceConflict:                    {CommandBackupCreate, StagePreflight},
		ErrorSecretFileInvalid:                       {CommandBackupCreate, StagePreflight},
		ErrorSecretFileUnsupportedPlatform:           {CommandBackupCreate, StagePreflight},
		ErrorOperationFailed:                         {CommandAdminDiagnostics, StagePreflight},
	}
	if len(cases) != 32 {
		t.Fatalf("stable error matrix has %d entries, want 32", len(cases))
	}
	for code, test := range cases {
		if err := validateErrorPair(test.command, code, test.stage); err != nil {
			t.Errorf("%s valid pair rejected: %v", code, err)
		}
	}
	if err := validateErrorPair(CommandHealthcheck, ErrorErasurePlanStale, StagePreflight); err == nil {
		t.Fatal("invalid command/code pairing accepted")
	}
	if err := validateErrorPair(CommandBackupRestore, ErrorProjectionNotCurrent, StagePreflight); err == nil {
		t.Fatal("invalid code/stage pairing accepted")
	}
}

func testHead() Head {
	return Head{
		Exists: true, CommitID: stringPointer(testCommit), CommitSeq: stringPointer("7"),
		CommittedAt: stringPointer("1787220000000000"), CommittedTZ: stringPointer("UTC"),
	}
}

func stringPointer(value string) *string { return &value }

func deepJSONEqual(left, right any) bool {
	leftJSON, _ := json.Marshal(left)
	rightJSON, _ := json.Marshal(right)
	return bytes.Equal(leftJSON, rightJSON)
}
