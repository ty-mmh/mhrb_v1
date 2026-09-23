package cliresult

import (
	"bytes"
	"slices"
	"testing"
)

func TestM7CLIRequiredActionIDGoldenAndExactArgv(t *testing.T) {
	action, err := NewRequiredAction(ActionRunIntegrityScan, []string{
		"mahoroba", "admin", "integrity", "scan", "--resident", testResident,
		"--config", "/etc/mahoroba/config.toml", "--data-dir", "/var/lib/mahoroba",
	}, []string{testResident}, nil)
	if err != nil {
		t.Fatal(err)
	}
	const want = "sha256:e9f6effcfb5a953837aa96a196d0998d7a9c6e8ea134649b19cf97fb8dda7a53"
	if action.ActionID != want {
		t.Fatalf("action ID = %s, want %s", action.ActionID, want)
	}
	if action.Argv[len(action.Argv)-2] != "--data-dir" {
		t.Fatalf("source argv does not end in --data-dir: %#v", action.Argv)
	}

	bad := action
	bad.Argv = append([]string{}, action.Argv...)
	bad.Argv[7] = "relative.toml"
	bad.ActionID, _ = requiredActionID(bad.Code, bad.Argv, bad.TargetIDs)
	if err := validateActionShape(bad); err == nil {
		t.Fatal("relative config path unexpectedly accepted")
	}
	bad = action
	bad.Argv = append(bad.Argv, "--secret", "plaintext")
	bad.ActionID, _ = requiredActionID(bad.Code, bad.Argv, bad.TargetIDs)
	if err := validateActionShape(bad); err == nil {
		t.Fatal("extra/secret argv unexpectedly accepted")
	}
}

func TestM7CLIRequiredActionDAGAndRestoreStartPrerequisites(t *testing.T) {
	selectResident, err := NewRequiredAction(ActionSelectActiveResident, nil, []string{testResident}, nil)
	if err != nil {
		t.Fatal(err)
	}
	start, err := NewRequiredAction(ActionStartService,
		[]string{"mahoroba", "serve", "--data-dir", "/var/lib/mahoroba-restored"},
		nil, []string{selectResident.ActionID})
	if err != nil {
		t.Fatal(err)
	}
	actions := []RequiredAction{selectResident, start}
	if !slices.IsSortedFunc(actions, func(left, right RequiredAction) int {
		if left.Code < right.Code {
			return -1
		}
		if left.Code > right.Code {
			return 1
		}
		return bytes.Compare([]byte(left.ActionID), []byte(right.ActionID))
	}) {
		t.Fatal("test action fixture is not catalog-sorted")
	}
	envelope := NewSuccess(CommandBackupRestore, validRestoreResult(false))
	envelope.RequiredActions = actions
	warning, _ := NewWarning(WarningServiceNotReady)
	envelope.Warnings = []Warning{warning}
	if err := envelope.Validate(); err != nil {
		t.Fatalf("valid restore DAG rejected: %v", err)
	}

	missing := envelope
	missing.RequiredActions = append([]RequiredAction{}, actions...)
	missing.RequiredActions[1].PrerequisiteActionIDs = []string{}
	if err := missing.Validate(); err == nil {
		t.Fatal("restore start missing selection prerequisite unexpectedly accepted")
	}

	cycle := envelope
	cycle.RequiredActions = append([]RequiredAction{}, actions...)
	cycle.RequiredActions[0].PrerequisiteActionIDs = []string{start.ActionID}
	if err := cycle.Validate(); err == nil {
		t.Fatal("cyclic prerequisite graph unexpectedly accepted")
	}

	dangling := envelope
	dangling.RequiredActions = append([]RequiredAction{}, actions...)
	dangling.RequiredActions[1].PrerequisiteActionIDs = []string{"sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"}
	if err := dangling.Validate(); err == nil {
		t.Fatal("missing prerequisite unexpectedly accepted")
	}
}

func TestM7CLIRequiredActionRejectsPlaceholderAndTargetMismatch(t *testing.T) {
	tests := []struct {
		name    string
		code    ActionCode
		argv    []string
		targets []string
	}{
		{
			name: "placeholder integrity resident", code: ActionRunIntegrityScan,
			argv:    []string{"mahoroba", "admin", "integrity", "scan", "--resident", "R", "--data-dir", "/var/lib/mahoroba"},
			targets: []string{testResident},
		},
		{
			name: "partial command", code: ActionRunRecoveryTerminalize,
			argv:    []string{"mahoroba", "admin", "recovery", "--data-dir", "/var/lib/mahoroba"},
			targets: []string{testResident},
		},
		{
			name: "shell command", code: ActionStartService,
			argv: []string{"mahoroba serve --data-dir /var/lib/mahoroba"},
		},
		{
			name: "GC resident mismatch", code: ActionRerunBlobGCDryRun,
			argv:    []string{"mahoroba", "blob", "gc", "--resident", testMarker, "--data-dir", "/var/lib/mahoroba"},
			targets: []string{testResident},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if _, err := NewRequiredAction(test.code, test.argv, test.targets, nil); err == nil {
				t.Fatal("malformed action unexpectedly accepted")
			}
		})
	}
}

func validRestoreResult(serviceReady bool) *BackupRestoreResult {
	return &BackupRestoreResult{
		TargetDataDir: "/var/lib/mahoroba-restored", DatabaseFilename: "mahoroba.db",
		SourceHead: testHead(), RestoredHead: testHead(), TerminalizedAttempts: "0",
		FindingsCreated: "0", ProjectionsRebuilt: "0", Published: true,
		StagingMarkerID: testMarker, ServiceReady: serviceReady,
	}
}
