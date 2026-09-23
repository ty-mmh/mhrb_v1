package durablepublish

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"runtime"
	"slices"
	"testing"

	"mahoroba.local/mahoroba/internal/canonical"
	"mahoroba.local/mahoroba/internal/fssecure"
)

func TestM7DurablePublishMarkerAndRecoveryStateMachine(t *testing.T) {
	id := mustPublishID(t)
	digestText := "sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
	input, err := ProducerInputDigest(CommandBackupRestore, BackupRestoreProducerInput{
		BundleRootIdentity: digestText, BundleManifestSHA256: digestText,
		TargetDatabaseFilename: "mahoroba.db",
	})
	if err != nil {
		t.Fatal(err)
	}
	payload := canonical.HashBlob([]byte("payload"))
	marker, err := NewMarker(id, VariantDirectory, CommandBackupRestore, input, "target", ".target.staging", payload)
	if err != nil {
		t.Fatal(err)
	}
	body, err := marker.Bytes()
	if err != nil {
		t.Fatal(err)
	}
	parsed, err := ParseMarker(body)
	if err != nil || parsed != marker {
		t.Fatalf("ParseMarker = %+v, %v", parsed, err)
	}
	unknown := append([]byte(nil), body[:len(body)-2]...)
	unknown = append(unknown, []byte(",\"unknown\":true}\n")...)
	if _, err := ParseMarker(unknown); !errors.Is(err, ErrInvalidMarker) {
		t.Fatalf("unknown-field marker error = %v", err)
	}

	tests := []struct {
		name string
		in   RecoveryObservation
		want RecoveryAction
		err  bool
	}{
		{"new", RecoveryObservation{Variant: VariantDirectory, NamespaceIdentityValid: true}, RecoveryStartNew, false},
		{"single resume", RecoveryObservation{Variant: VariantSingleFile, NamespaceIdentityValid: true, MarkerPresent: true, MarkerMatchesInput: true, StagingPresent: true, StagingPayloadMatches: true}, RecoveryResumeSingleFile, false},
		{"directory never resumes", RecoveryObservation{Variant: VariantDirectory, NamespaceIdentityValid: true, MarkerPresent: true, MarkerMatchesInput: true, StagingPresent: true, StagingPayloadMatches: true}, RecoveryRepairRequired, true},
		{"published completion", RecoveryObservation{Variant: VariantDirectory, NamespaceIdentityValid: true, MarkerPresent: true, MarkerMatchesInput: true, TargetPresent: true, TargetPayloadMatches: true}, RecoveryCompletePublished, false},
		{"stale marker", RecoveryObservation{Variant: VariantSingleFile, NamespaceIdentityValid: true, MarkerPresent: true, MarkerMatchesInput: true}, RecoveryRemoveStaleMarker, false},
		{"payload mismatch", RecoveryObservation{Variant: VariantDirectory, NamespaceIdentityValid: true, MarkerPresent: true, MarkerMatchesInput: true, TargetPresent: true}, RecoveryRepairRequired, true},
		{"input mismatch", RecoveryObservation{Variant: VariantDirectory, NamespaceIdentityValid: true, MarkerPresent: true}, RecoveryRepairRequired, true},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got, err := DecideRecovery(test.in)
			if got != test.want || (err != nil) != test.err {
				t.Fatalf("DecideRecovery = %q, %v, want %q error=%v", got, err, test.want, test.err)
			}
		})
	}
}

func TestM7DurablePublishProducerInputIsExactAndCommandBound(t *testing.T) {
	digestText := "sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
	valid := BackupRestoreProducerInput{
		BundleRootIdentity: digestText, BundleManifestSHA256: digestText,
		TargetDatabaseFilename: "mahoroba.db",
	}
	first, err := ProducerInputDigest(CommandBackupRestore, valid)
	if err != nil {
		t.Fatal(err)
	}
	second, err := ProducerInputDigest(CommandBackupRestore, valid)
	if err != nil || first != second {
		t.Fatalf("stable restore input digest = %v, %v, want %v", second, err, first)
	}
	invalidConfig := valid
	invalidConfig.TargetConfigSHA256 = func() *string { value := "SHA256:bad"; return &value }()
	for name, test := range map[string]struct {
		command string
		input   any
	}{
		"wrong command":       {CommandExportJSONL, valid},
		"anonymous schema":    {CommandBackupRestore, struct{}{}},
		"invalid config hash": {CommandBackupRestore, invalidConfig},
		"wrong database name": {CommandBackupRestore, BackupRestoreProducerInput{BundleRootIdentity: digestText, BundleManifestSHA256: digestText, TargetDatabaseFilename: "other.db"}},
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := ProducerInputDigest(test.command, test.input); !errors.Is(err, ErrInvalidProducerInput) {
				t.Fatalf("ProducerInputDigest error = %v", err)
			}
		})
	}
	if _, err := NewMarker(mustPublishID(t), VariantSingleFile, "unknown.producer", first, "target", ".target.staging", canonical.HashBlob([]byte("payload"))); !errors.Is(err, ErrInvalidMarker) {
		t.Fatalf("unknown marker producer error = %v", err)
	}
	if _, err := NewMarker(mustPublishID(t), VariantSingleFile, CommandBackupRestore, first, "target", ".target.staging", canonical.HashBlob([]byte("payload"))); !errors.Is(err, ErrInvalidMarker) {
		t.Fatalf("wrong producer variant error = %v", err)
	}
}

func TestM7DurablePublishDirectoryAndSingleFileBarrier(t *testing.T) {
	t.Run("directory", func(t *testing.T) {
		parent := t.TempDir()
		stagingPath := filepath.Join(parent, ".artifact.staging")
		policy, err := fssecure.CurrentSecurityPolicy()
		if err != nil {
			t.Fatal(err)
		}
		root, err := fssecure.OpenOrCreateRoot(stagingPath, policy)
		if err != nil {
			t.Fatal(err)
		}
		writeTestFile(t, root, "payload", []byte("directory payload"))
		writeTestFile(t, root, "CALLER_STAGING", []byte("marker\n"))
		if err := root.Close(); err != nil {
			t.Fatal(err)
		}
		root, err = fssecure.OpenRootForPublish(stagingPath, policy)
		if err != nil {
			skipWindowsCapabilitySandbox(t, err)
			t.Fatal(err)
		}
		payload, err := DirectoryPayloadDigest(context.Background(), root, "CALLER_STAGING")
		if err != nil {
			t.Fatal(err)
		}
		marker := testMarker(t, VariantDirectory, "artifact", ".artifact.staging", payload.SHA256)
		result, err := PublishDirectory(context.Background(), DirectoryRequest{
			Staging: root, Marker: marker, CallerMarkers: []string{"CALLER_STAGING"},
		})
		if err != nil {
			t.Fatal(err)
		}
		if result.Payload != payload {
			t.Fatalf("payload = %+v, want %+v", result.Payload, payload)
		}
		if err := root.Close(); err != nil {
			t.Fatal(err)
		}
		for _, path := range []string{
			filepath.Join(parent, ".artifact.staging"),
			filepath.Join(parent, "artifact", MarkerName),
			filepath.Join(parent, "artifact", "CALLER_STAGING"),
			filepath.Join(parent, mustSiblingName(t, marker)),
		} {
			if _, err := os.Lstat(path); !errors.Is(err, os.ErrNotExist) {
				t.Fatalf("reserved path %s remains: %v", path, err)
			}
		}
		if body, err := os.ReadFile(filepath.Join(parent, "artifact", "payload")); err != nil || string(body) != "directory payload" {
			t.Fatalf("published payload = %q, %v", body, err)
		}
	})

	t.Run("single file", func(t *testing.T) {
		parentPath := filepath.Join(t.TempDir(), "managed")
		policy, err := fssecure.CurrentSecurityPolicy()
		if err != nil {
			t.Fatal(err)
		}
		parent, err := fssecure.OpenOrCreateRoot(parentPath, policy)
		if err != nil {
			t.Fatal(err)
		}
		staging, err := parent.CreateRegular(".plan.staging")
		if err != nil {
			_ = parent.Close()
			skipWindowsCapabilitySandbox(t, err)
			t.Fatal(err)
		}
		if _, err := staging.File().Write([]byte("single payload")); err != nil {
			t.Fatal(err)
		}
		if err := staging.Seal(); err != nil {
			t.Fatal(err)
		}
		payload, err := SingleFilePayloadDigest(context.Background(), staging)
		if err != nil {
			t.Fatal(err)
		}
		marker := testMarker(t, VariantSingleFile, "plan", ".plan.staging", payload.SHA256)
		if _, err := PublishSingleFile(context.Background(), SingleFileRequest{
			Parent: parent, Staging: staging, Marker: marker,
		}); err != nil {
			t.Fatal(err)
		}
		if err := staging.Close(); err != nil {
			t.Fatal(err)
		}
		if err := parent.Close(); err != nil {
			t.Fatal(err)
		}
		if body, err := os.ReadFile(filepath.Join(parentPath, "plan")); err != nil || string(body) != "single payload" {
			t.Fatalf("single-file payload = %q, %v", body, err)
		}
		if _, err := os.Lstat(filepath.Join(parentPath, mustSiblingName(t, marker))); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("single-file sibling marker remains: %v", err)
		}
	})
}

func TestM7DurablePublishDirectoryFailpointsPreserveRepairEvidence(t *testing.T) {
	points := []Failpoint{
		FailpointAfterMarkersDurable,
		FailpointAfterPayloadRename,
		FailpointAfterTargetCleanup,
		FailpointAfterSiblingRemoval,
	}
	for _, point := range points {
		t.Run(string(point), func(t *testing.T) {
			parent := t.TempDir()
			stagingPath := filepath.Join(parent, ".target.staging")
			policy, err := fssecure.CurrentSecurityPolicy()
			if err != nil {
				t.Fatal(err)
			}
			root, err := fssecure.OpenOrCreateRoot(stagingPath, policy)
			if err != nil {
				t.Fatal(err)
			}
			writeTestFile(t, root, "payload", []byte("payload"))
			writeTestFile(t, root, "RESTORE_STAGING", []byte("marker\n"))
			if err := root.Close(); err != nil {
				t.Fatal(err)
			}
			root, err = fssecure.OpenRootForPublish(stagingPath, policy)
			if err != nil {
				skipWindowsCapabilitySandbox(t, err)
				t.Fatal(err)
			}
			payload, err := DirectoryPayloadDigest(context.Background(), root, "RESTORE_STAGING")
			if err != nil {
				t.Fatal(err)
			}
			marker := testMarker(t, VariantDirectory, "target", ".target.staging", payload.SHA256)
			seen := []Failpoint{}
			_, err = PublishDirectory(context.Background(), DirectoryRequest{
				Staging: root, Marker: marker, CallerMarkers: []string{"RESTORE_STAGING"},
				Failpoint: func(actual Failpoint) error {
					seen = append(seen, actual)
					if actual == point {
						return errors.New("injected crash")
					}
					return nil
				},
			})
			if err == nil {
				t.Fatal("failpoint publication succeeded")
			}
			if !slices.Contains(seen, point) {
				t.Fatalf("observed failpoints = %v", seen)
			}
			_ = root.Close()

			target := filepath.Join(parent, "target")
			staging := filepath.Join(parent, ".target.staging")
			sibling := filepath.Join(parent, mustSiblingName(t, marker))
			switch point {
			case FailpointAfterMarkersDurable:
				assertPathState(t, staging, true)
				assertPathState(t, filepath.Join(staging, MarkerName), true)
				assertPathState(t, sibling, true)
				assertPathState(t, target, false)
			case FailpointAfterPayloadRename:
				assertPathState(t, target, true)
				assertPathState(t, filepath.Join(target, MarkerName), true)
				assertPathState(t, sibling, true)
			case FailpointAfterTargetCleanup:
				assertPathState(t, target, true)
				assertPathState(t, filepath.Join(target, MarkerName), false)
				assertPathState(t, filepath.Join(target, "RESTORE_STAGING"), false)
				assertPathState(t, sibling, true)
			case FailpointAfterSiblingRemoval:
				assertPathState(t, target, true)
				assertPathState(t, filepath.Join(target, MarkerName), false)
				assertPathState(t, sibling, false)
			}
		})
	}
}

func TestM7DurablePublishSingleFileFailpointsPreserveRepairEvidence(t *testing.T) {
	points := []Failpoint{
		FailpointAfterMarkersDurable,
		FailpointAfterPayloadRename,
		FailpointAfterSiblingRemoval,
		FailpointAfterFinalParentBarrier,
	}
	for _, point := range points {
		t.Run(string(point), func(t *testing.T) {
			parentPath := filepath.Join(t.TempDir(), "managed")
			policy, err := fssecure.CurrentSecurityPolicy()
			if err != nil {
				t.Fatal(err)
			}
			parent, err := fssecure.OpenOrCreateRoot(parentPath, policy)
			if err != nil {
				t.Fatal(err)
			}
			staging, err := parent.CreateRegular(".artifact.staging")
			if err != nil {
				_ = parent.Close()
				skipWindowsCapabilitySandbox(t, err)
				t.Fatal(err)
			}
			if _, err := staging.File().Write([]byte("payload")); err != nil {
				t.Fatal(err)
			}
			if err := staging.Seal(); err != nil {
				t.Fatal(err)
			}
			payload, err := SingleFilePayloadDigest(context.Background(), staging)
			if err != nil {
				t.Fatal(err)
			}
			marker := testMarker(t, VariantSingleFile, "artifact", ".artifact.staging", payload.SHA256)
			_, err = PublishSingleFile(context.Background(), SingleFileRequest{
				Parent: parent, Staging: staging, Marker: marker,
				Failpoint: func(actual Failpoint) error {
					if actual == point {
						return errors.New("injected crash")
					}
					return nil
				},
			})
			if err == nil {
				t.Fatal("failpoint publication succeeded")
			}
			_ = staging.Close()
			_ = parent.Close()

			target := filepath.Join(parentPath, "artifact")
			stagingPath := filepath.Join(parentPath, ".artifact.staging")
			sibling := filepath.Join(parentPath, mustSiblingName(t, marker))
			switch point {
			case FailpointAfterMarkersDurable:
				assertPathState(t, target, false)
				assertPathState(t, stagingPath, true)
				assertPathState(t, sibling, true)
			case FailpointAfterPayloadRename:
				assertPathState(t, target, true)
				assertPathState(t, stagingPath, false)
				assertPathState(t, sibling, true)
			case FailpointAfterSiblingRemoval, FailpointAfterFinalParentBarrier:
				assertPathState(t, target, true)
				assertPathState(t, stagingPath, false)
				assertPathState(t, sibling, false)
			}
		})
	}
}

func TestM7DurablePublishDirectoryRecoveryCompletesEveryPostRenameState(t *testing.T) {
	for _, point := range []Failpoint{FailpointAfterPayloadRename, FailpointAfterTargetCleanup} {
		t.Run(string(point), func(t *testing.T) {
			parent := t.TempDir()
			stagingPath := filepath.Join(parent, ".target.staging")
			policy, err := fssecure.CurrentSecurityPolicy()
			if err != nil {
				t.Fatal(err)
			}
			root, err := fssecure.OpenOrCreateRoot(stagingPath, policy)
			if err != nil {
				t.Fatal(err)
			}
			writeTestFile(t, root, "payload", []byte("payload"))
			writeTestFile(t, root, "RESTORE_STAGING", []byte("marker\n"))
			if err := root.Close(); err != nil {
				t.Fatal(err)
			}
			root, err = fssecure.OpenRootForPublish(stagingPath, policy)
			if err != nil {
				skipWindowsCapabilitySandbox(t, err)
				t.Fatal(err)
			}
			payload, err := DirectoryPayloadDigest(context.Background(), root, "RESTORE_STAGING")
			if err != nil {
				t.Fatal(err)
			}
			marker := testMarker(t, VariantDirectory, "target", ".target.staging", payload.SHA256)
			_, err = PublishDirectory(context.Background(), DirectoryRequest{
				Staging: root, Marker: marker, CallerMarkers: []string{"RESTORE_STAGING"},
				Failpoint: func(actual Failpoint) error {
					if actual == point {
						return errors.New("injected crash")
					}
					return nil
				},
			})
			if !errors.Is(err, ErrDurabilityUnknown) {
				t.Fatalf("post-rename error = %v", err)
			}
			if err := root.Close(); err != nil {
				t.Fatal(err)
			}
			root, err = fssecure.OpenRootForPublish(filepath.Join(parent, "target"), policy)
			if err != nil {
				t.Fatal(err)
			}
			discovered, err := DiscoverPublishedDirectoryMarker(root, marker.ProducerCommand, marker.ProducerInputDigest)
			if err != nil || discovered != marker {
				t.Fatalf("DiscoverPublishedDirectoryMarker = %+v, %v", discovered, err)
			}
			recovered, err := RecoverPublishedDirectory(context.Background(), PublishedDirectoryRecoveryRequest{
				Target: root, Expected: marker, CallerMarkers: []string{"RESTORE_STAGING"},
			})
			if err != nil || recovered.Action != RecoveryCompletePublished || recovered.Payload != payload {
				t.Fatalf("RecoverPublishedDirectory = %+v, %v", recovered, err)
			}
			if err := root.Close(); err != nil {
				t.Fatal(err)
			}
			assertPathState(t, filepath.Join(parent, "target", MarkerName), false)
			assertPathState(t, filepath.Join(parent, "target", "RESTORE_STAGING"), false)
			assertPathState(t, filepath.Join(parent, mustSiblingName(t, marker)), false)
		})
	}

	t.Run("internal marker recovers lost sibling", func(t *testing.T) {
		parent := t.TempDir()
		stagingPath := filepath.Join(parent, ".target.staging")
		policy, err := fssecure.CurrentSecurityPolicy()
		if err != nil {
			t.Fatal(err)
		}
		root, err := fssecure.OpenOrCreateRoot(stagingPath, policy)
		if err != nil {
			t.Fatal(err)
		}
		writeTestFile(t, root, "payload", []byte("payload"))
		if err := root.Close(); err != nil {
			t.Fatal(err)
		}
		root, err = fssecure.OpenRootForPublish(stagingPath, policy)
		if err != nil {
			skipWindowsCapabilitySandbox(t, err)
			t.Fatal(err)
		}
		payload, err := DirectoryPayloadDigest(context.Background(), root)
		if err != nil {
			t.Fatal(err)
		}
		marker := testMarker(t, VariantDirectory, "target", ".target.staging", payload.SHA256)
		_, err = PublishDirectory(context.Background(), DirectoryRequest{
			Staging: root, Marker: marker,
			Failpoint: func(actual Failpoint) error {
				if actual == FailpointAfterPayloadRename {
					return errors.New("injected crash")
				}
				return nil
			},
		})
		if !errors.Is(err, ErrDurabilityUnknown) {
			t.Fatal(err)
		}
		sibling, err := root.OpenSiblingRegularForUpdate(mustSiblingName(t, marker))
		if err != nil {
			t.Fatal(err)
		}
		if err := markDeleteAndClose(sibling); err != nil {
			t.Fatal(err)
		}
		discovered, err := DiscoverPublishedDirectoryMarker(root, marker.ProducerCommand, marker.ProducerInputDigest)
		if err != nil || discovered != marker {
			t.Fatalf("internal-only discovery = %+v, %v", discovered, err)
		}
		recovered, err := RecoverPublishedDirectory(context.Background(), PublishedDirectoryRecoveryRequest{
			Target: root, Expected: marker,
		})
		if err != nil || recovered.Action != RecoveryCompletePublished {
			t.Fatalf("internal-only recovery = %+v, %v", recovered, err)
		}
		_ = root.Close()
	})
}

func TestM7DurablePublishSingleFileRecoveryExecutesClosedStateMachine(t *testing.T) {
	tests := []struct {
		name      string
		point     Failpoint
		deleteTmp bool
		want      RecoveryAction
	}{
		{name: "resume staged", point: FailpointAfterMarkersDurable, want: RecoveryResumeSingleFile},
		{name: "complete renamed", point: FailpointAfterPayloadRename, want: RecoveryCompletePublished},
		{name: "remove stale", point: FailpointAfterMarkersDurable, deleteTmp: true, want: RecoveryRemoveStaleMarker},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			parentPath := filepath.Join(t.TempDir(), "managed")
			policy, err := fssecure.CurrentSecurityPolicy()
			if err != nil {
				t.Fatal(err)
			}
			parent, err := fssecure.OpenOrCreateRoot(parentPath, policy)
			if err != nil {
				t.Fatal(err)
			}
			staging, err := parent.CreateRegular(".artifact.staging")
			if err != nil {
				_ = parent.Close()
				skipWindowsCapabilitySandbox(t, err)
				t.Fatal(err)
			}
			if _, err := staging.File().Write([]byte("payload")); err != nil {
				t.Fatal(err)
			}
			if err := staging.Seal(); err != nil {
				t.Fatal(err)
			}
			payload, err := SingleFilePayloadDigest(context.Background(), staging)
			if err != nil {
				t.Fatal(err)
			}
			marker := testMarker(t, VariantSingleFile, "artifact", ".artifact.staging", payload.SHA256)
			_, err = PublishSingleFile(context.Background(), SingleFileRequest{
				Parent: parent, Staging: staging, Marker: marker,
				Failpoint: func(actual Failpoint) error {
					if actual == test.point {
						return errors.New("injected crash")
					}
					return nil
				},
			})
			if err == nil {
				t.Fatal("crash setup unexpectedly succeeded")
			}
			if test.deleteTmp {
				if err := staging.MarkDeleteOnClose(); err != nil {
					t.Fatal(err)
				}
			}
			if err := staging.Close(); err != nil {
				t.Fatal(err)
			}
			recovered, err := RecoverSingleFile(context.Background(), SingleFileRecoveryRequest{
				Parent: parent, Expected: marker,
			})
			if err != nil || recovered.Action != test.want {
				t.Fatalf("RecoverSingleFile = %+v, %v, want %s", recovered, err, test.want)
			}
			if err := parent.Close(); err != nil {
				t.Fatal(err)
			}
			assertPathState(t, filepath.Join(parentPath, mustSiblingName(t, marker)), false)
			if test.want == RecoveryRemoveStaleMarker {
				assertPathState(t, filepath.Join(parentPath, "artifact"), false)
			} else {
				assertPathState(t, filepath.Join(parentPath, "artifact"), true)
			}
		})
	}
}

func testMarker(t *testing.T, variant Variant, target, staging string, payload canonical.Digest) Marker {
	t.Helper()
	input := canonical.HashBlob([]byte("input"))
	command := CommandBackupRestore
	if variant == VariantSingleFile {
		command = CommandExportJSONL
	}
	marker, err := NewMarker(mustPublishID(t), variant, command, input, target, staging, payload)
	if err != nil {
		t.Fatal(err)
	}
	return marker
}

func mustPublishID(t *testing.T) canonical.ID {
	t.Helper()
	id, err := canonical.NewSecureIDGenerator().New()
	if err != nil {
		t.Fatal(err)
	}
	return id
}

func mustSiblingName(t *testing.T, marker Marker) string {
	t.Helper()
	name, err := SiblingMarkerBasename(marker.TargetBasename, marker.PublishID)
	if err != nil {
		t.Fatal(err)
	}
	return name
}

func writeTestFile(t *testing.T, directory *fssecure.Directory, name string, body []byte) {
	t.Helper()
	handle, err := directory.CreateRegular(name)
	if err != nil {
		_ = directory.Close()
		skipWindowsCapabilitySandbox(t, err)
		t.Fatal(err)
	}
	if _, err := handle.File().Write(body); err != nil {
		t.Fatal(err)
	}
	if err := handle.Seal(); err != nil {
		t.Fatal(err)
	}
	if err := handle.Close(); err != nil {
		t.Fatal(err)
	}
}

func skipWindowsCapabilitySandbox(t *testing.T, err error) {
	t.Helper()
	reason, hasReason := fssecure.Reason(err)
	knownACLCondition := errors.Is(err, os.ErrPermission) || hasReason &&
		(reason == fssecure.ReasonUnsafeACL || reason == fssecure.ReasonOwnerMismatch)
	if runtime.GOOS == "windows" && os.Getenv("CI") == "" && err != nil && knownACLCondition {
		t.Skip("Windows capability sandbox cannot exercise the exact protected DACL; Hosted/non-sandbox evidence is required")
	}
}

func assertPathState(t *testing.T, path string, exists bool) {
	t.Helper()
	_, err := os.Lstat(path)
	if exists && err != nil {
		t.Fatalf("%s does not exist: %v", path, err)
	}
	if !exists && !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("%s unexpectedly exists: %v", path, err)
	}
}
