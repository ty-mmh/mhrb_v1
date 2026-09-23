//go:build windows

package namespacelock

import (
	"encoding/binary"
	"errors"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"unsafe"

	"golang.org/x/sys/windows"
)

func TestM7WindowsManagedNamespaceRetainsExactProtectedDACL(t *testing.T) {
	parent := newProtectedWindowsTestParent(t)
	lock, err := AcquireExistingParent(filepath.Join(parent, "runtime"))
	if err != nil {
		t.Fatal(err)
	}
	defer lock.Close()
	if err := validateWindowsProtectedSecurity(lock.file); err != nil {
		t.Fatalf("created rendezvous DACL: %v", err)
	}
	target, err := lock.OpenOrCreateTarget(0o700)
	if err != nil {
		t.Fatal(err)
	}
	defer target.Close()
	if err := validateWindowsProtectedSecurity(target.file); err != nil {
		t.Fatalf("created target DACL: %v", err)
	}
	inner, err := target.OpenOrCreateRegular(".mahoroba.lock", 0o600)
	if err != nil {
		t.Fatal(err)
	}
	defer inner.Close()
	if err := validateWindowsProtectedSecurity(inner); err != nil {
		t.Fatalf("created inner lock DACL: %v", err)
	}

	descriptor, err := protectedWindowsSecurityDescriptor()
	if err != nil {
		t.Fatal(err)
	}
	view, err := windowsSecurityViewFromDescriptor(descriptor)
	if err != nil {
		t.Fatal(err)
	}
	administrators, err := windows.StringToSid(windowsAdministratorsSID)
	if err != nil {
		t.Fatal(err)
	}
	wrongOwner := view
	wrongOwner.owner = administrators
	if err := validateWindowsExactProtectedSecurityView(wrongOwner, windows.NO_INHERITANCE); !errors.Is(err, ErrUnsafeTarget) {
		t.Fatalf("Administrators-owned managed entry error = %v", err)
	}

	user, err := currentWindowsUserSID()
	if err != nil {
		t.Fatal(err)
	}
	system, err := windows.StringToSid(windowsSystemSID)
	if err != nil {
		t.Fatal(err)
	}
	everyone, err := windows.StringToSid("S-1-1-0")
	if err != nil {
		t.Fatal(err)
	}
	extraACE := windowsSecurityView{
		owner:   user,
		control: windows.SE_DACL_PRESENT | windows.SE_DACL_PROTECTED,
		dacl: windowsTestACL(t, []windowsTestACE{
			{sid: user, mask: windowsFileAllAccess, mode: windows.GRANT_ACCESS},
			{sid: system, mask: windowsFileAllAccess, mode: windows.GRANT_ACCESS},
			{sid: administrators, mask: windowsFileAllAccess, mode: windows.GRANT_ACCESS},
			{sid: everyone, mask: windows.FILE_GENERIC_READ, mode: windows.GRANT_ACCESS},
		}),
	}
	if err := validateWindowsExactProtectedSecurityView(extraACE, windows.NO_INHERITANCE); !errors.Is(err, ErrUnsafeTarget) {
		t.Fatalf("extra managed ACE error = %v", err)
	}
	inherited := extraACE
	inherited.dacl = windowsTestACL(t, []windowsTestACE{
		{sid: user, mask: windowsFileAllAccess, mode: windows.GRANT_ACCESS, flags: windows.INHERITED_ACE},
		{sid: system, mask: windowsFileAllAccess, mode: windows.GRANT_ACCESS},
		{sid: administrators, mask: windowsFileAllAccess, mode: windows.GRANT_ACCESS},
	})
	if err := validateWindowsExactProtectedSecurityView(inherited, windows.NO_INHERITANCE); !errors.Is(err, ErrUnsafeTarget) {
		t.Fatalf("inherited managed ACE error = %v", err)
	}
}

func TestM7WindowsExecutionEvidenceChildRetainsExactProtectedDACL(t *testing.T) {
	parent := newProtectedWindowsTestParent(t)
	child, err := PrepareProtectedCITestChild(parent, "evidence")
	if err != nil {
		t.Fatal(err)
	}
	if err := ValidateProtectedCITestChild(parent, child); err != nil {
		t.Fatalf("validate protected evidence child: %v", err)
	}
	if _, err := PrepareProtectedCITestChild(parent, "evidence"); err == nil {
		t.Fatal("existing evidence child was repaired or reused")
	}
	file := mustOpenDirectoryForSecurityTest(t, child)
	defer file.Close()
	if err := validateWindowsInheritableProtectedSecurity(file); err != nil {
		t.Fatalf("evidence child exact inheritable DACL: %v", err)
	}
}

func TestM7WindowsCreatedTargetAndRendezvousHaveExactProtectedDACL(t *testing.T) {
	TestM7WindowsManagedNamespaceRetainsExactProtectedDACL(t)
}

func TestM7WindowsRejectsExistingWeakRendezvousDACL(t *testing.T) {
	parent := newProtectedWindowsTestParent(t)
	path := filepath.Join(parent, ".runtime.mahoroba-namespace.lock")
	if err := os.WriteFile(path, nil, 0o600); err != nil {
		t.Fatal(err)
	}
	if lock, err := AcquireExistingParent(filepath.Join(parent, "runtime")); !errors.Is(err, ErrUnsafeTarget) {
		if lock != nil {
			_ = lock.Close()
		}
		t.Fatalf("Acquire with inherited rendezvous DACL = %v, want ErrUnsafeTarget", err)
	}
}

func TestM7WindowsOpenOrCreateRejectsInheritedTargetDACLWithoutRepair(t *testing.T) {
	parent := newProtectedWindowsTestParent(t)
	targetPath := filepath.Join(parent, "runtime")
	lock, err := AcquireExistingParent(targetPath)
	if err != nil {
		t.Fatal(err)
	}
	defer lock.Close()
	descriptor, err := protectedWindowsSecurityDescriptorWithInheritance(windows.SUB_CONTAINERS_AND_OBJECTS_INHERIT)
	if err != nil {
		t.Fatal(err)
	}
	weakTarget, _, err := ntOpenRelative(
		lock.parent,
		"runtime",
		windows.FILE_GENERIC_READ|windows.FILE_GENERIC_WRITE|windows.READ_CONTROL,
		windows.FILE_SHARE_READ|windows.FILE_SHARE_WRITE,
		windows.FILE_CREATE,
		windows.FILE_DIRECTORY_FILE|windows.FILE_OPEN_REPARSE_POINT|windows.FILE_SYNCHRONOUS_IO_NONALERT,
		descriptor,
	)
	if err != nil {
		t.Fatal(err)
	}
	if err := weakTarget.Close(); err != nil {
		t.Fatal(err)
	}
	if exists, err := lock.TargetExists(); err != nil || !exists {
		t.Fatalf("TargetExists inherited DACL = %v, %v, want true, nil", exists, err)
	}
	if target, err := lock.OpenExistingTarget(); !errors.Is(err, ErrUnsafeTarget) {
		if target != nil {
			_ = target.Close()
		}
		t.Fatalf("OpenExistingTarget inherited DACL = %v, want ErrUnsafeTarget", err)
	}
	if target, err := lock.OpenOrCreateTarget(0o700); !errors.Is(err, ErrUnsafeTarget) {
		if target != nil {
			_ = target.Close()
		}
		t.Fatalf("OpenOrCreateTarget inherited DACL = %v, want ErrUnsafeTarget", err)
	}
	file := mustOpenDirectoryForSecurityTest(t, targetPath)
	defer file.Close()
	if err := validateWindowsProtectedSecurity(file); err == nil {
		t.Fatal("rejected target DACL was silently repaired")
	}
	if err := validateWindowsInheritableProtectedSecurity(file); err != nil {
		t.Fatalf("read-only observation mutated inherited target DACL: %v", err)
	}
}

func TestM7WindowsDiagnosticSiblingObservationDoesNotConferManagedAuthority(t *testing.T) {
	parent := newProtectedWindowsTestParent(t)
	lock, err := AcquireExistingParent(filepath.Join(parent, "runtime"))
	if err != nil {
		t.Fatal(err)
	}
	defer lock.Close()

	inherited, err := protectedWindowsSecurityDescriptorWithInheritance(windows.SUB_CONTAINERS_AND_OBJECTS_INHERIT)
	if err != nil {
		t.Fatal(err)
	}
	name := ".runtime.restore-staging.observed"
	directory, _, err := ntOpenRelative(
		lock.parent,
		name,
		windows.FILE_GENERIC_READ|windows.FILE_GENERIC_WRITE|windows.READ_CONTROL,
		windows.FILE_SHARE_READ|windows.FILE_SHARE_WRITE,
		windows.FILE_CREATE,
		windows.FILE_DIRECTORY_FILE|windows.FILE_OPEN_REPARSE_POINT|windows.FILE_SYNCHRONOUS_IO_NONALERT,
		inherited,
	)
	if err != nil {
		t.Fatal(err)
	}
	markerDescriptor, err := protectedWindowsSecurityDescriptor()
	if err != nil {
		_ = directory.Close()
		t.Fatal(err)
	}
	marker, _, err := ntOpenRelative(
		directory,
		"RESTORE_STAGING",
		windows.FILE_GENERIC_READ|windows.FILE_GENERIC_WRITE|windows.READ_CONTROL,
		windows.FILE_SHARE_READ|windows.FILE_SHARE_WRITE,
		windows.FILE_CREATE,
		windows.FILE_NON_DIRECTORY_FILE|windows.FILE_OPEN_REPARSE_POINT|windows.FILE_SYNCHRONOUS_IO_NONALERT,
		markerDescriptor,
	)
	if err != nil {
		_ = directory.Close()
		t.Fatal(err)
	}
	if _, err := marker.Write([]byte("observed\n")); err != nil {
		_ = marker.Close()
		_ = directory.Close()
		t.Fatal(err)
	}
	if err := marker.Close(); err != nil {
		_ = directory.Close()
		t.Fatal(err)
	}
	if err := directory.Close(); err != nil {
		t.Fatal(err)
	}

	observations, err := lock.InspectDiagnosticSiblings(128)
	if err != nil {
		t.Fatal(err)
	}
	if len(observations) != 1 || observations[0].Basename != name || !observations[0].Readable ||
		!observations[0].MarkerPresent || string(observations[0].Marker) != "observed\n" {
		t.Fatalf("diagnostic observations = %+v", observations)
	}

	directory = mustOpenDirectoryForSecurityTest(t, filepath.Join(parent, name))
	defer directory.Close()
	if err := validateWindowsProtectedSecurity(directory); !errors.Is(err, ErrUnsafeTarget) {
		t.Fatalf("observed sibling acquired managed authority: %v", err)
	}
	if err := validateWindowsInheritableProtectedSecurity(directory); err != nil {
		t.Fatalf("diagnostic observation mutated sibling DACL: %v", err)
	}
}

func TestM7WindowsCanonicalParentAllowsOnlyTrustedOwnershipAndNoUntrustedMutation(t *testing.T) {
	user := mustCurrentWindowsSID(t)
	system := mustStringWindowsSID(t, windowsSystemSID)
	administrators := mustStringWindowsSID(t, windowsAdministratorsSID)
	everyone := mustStringWindowsSID(t, "S-1-1-0")

	trustedFull := []windowsTestACE{{sid: user, mask: windowsFileAllAccess, mode: windows.GRANT_ACCESS}}
	for name, owner := range map[string]*windows.SID{
		"current user":   user,
		"SYSTEM":         system,
		"Administrators": administrators,
	} {
		t.Run("trusted owner "+name, func(t *testing.T) {
			view := windowsObservedTestView(t, owner, trustedFull)
			if err := validateWindowsObservedSecurityView(view); err != nil {
				t.Fatalf("trusted owner rejected: %v", err)
			}
		})
	}

	t.Run("untrusted owner", func(t *testing.T) {
		view := windowsObservedTestView(t, everyone, trustedFull)
		if err := validateWindowsObservedSecurityView(view); !errors.Is(err, ErrUnsafeTarget) {
			t.Fatalf("untrusted owner error = %v", err)
		}
	})

	t.Run("read only untrusted allow", func(t *testing.T) {
		view := windowsObservedTestView(t, user, append(trustedFull, windowsTestACE{
			sid: everyone,
			mask: windows.ACCESS_MASK(windows.FILE_LIST_DIRECTORY | windows.FILE_READ_EA | windows.FILE_TRAVERSE |
				windows.FILE_READ_ATTRIBUTES | windows.READ_CONTROL | windows.SYNCHRONIZE),
			mode: windows.GRANT_ACCESS,
		}))
		if err := validateWindowsObservedSecurityView(view); err != nil {
			t.Fatalf("read-only untrusted ACE rejected: %v", err)
		}
	})

	t.Run("generic read execute untrusted allow", func(t *testing.T) {
		view := windowsObservedTestView(t, user, append(trustedFull, windowsTestACE{
			sid: everyone, mask: windows.ACCESS_MASK(windows.GENERIC_READ | windows.GENERIC_EXECUTE), mode: windows.GRANT_ACCESS,
		}))
		if err := validateWindowsObservedSecurityView(view); err != nil {
			t.Fatalf("mapped read-only generic ACE rejected: %v", err)
		}
	})

	mutations := map[string]windows.ACCESS_MASK{
		"write data":       windows.FILE_WRITE_DATA,
		"append data":      windows.FILE_APPEND_DATA,
		"write EA":         windows.FILE_WRITE_EA,
		"write attributes": windows.FILE_WRITE_ATTRIBUTES,
		"delete child":     windows.ACCESS_MASK(0x40),
		"delete":           windows.DELETE,
		"write DACL":       windows.WRITE_DAC,
		"write owner":      windows.WRITE_OWNER,
		"generic write":    windows.GENERIC_WRITE,
		"generic all":      windows.GENERIC_ALL,
		"maximum allowed":  windows.MAXIMUM_ALLOWED,
		"system security":  windows.ACCESS_SYSTEM_SECURITY,
	}
	for name, mask := range mutations {
		t.Run("reject "+name, func(t *testing.T) {
			view := windowsObservedTestView(t, user, append(trustedFull, windowsTestACE{sid: everyone, mask: mask, mode: windows.GRANT_ACCESS}))
			if err := validateWindowsObservedSecurityView(view); !errors.Is(err, ErrUnsafeTarget) {
				t.Fatalf("mutation mask %#x error = %v", mask, err)
			}
		})
	}

	for name, flags := range map[string]uint32{
		"inherited":    windows.INHERITED_ACE,
		"inherit only": windows.CONTAINER_INHERIT_ACE | windows.INHERIT_ONLY_ACE,
	} {
		t.Run("reject "+name+" mutation", func(t *testing.T) {
			view := windowsSecurityView{
				owner: user, control: windows.SE_DACL_PRESENT,
				dacl: rawWindowsTestACL(t, windows.ACCESS_ALLOWED_ACE_TYPE, uint8(flags), windows.FILE_WRITE_DATA, everyone, false),
			}
			if err := validateWindowsObservedSecurityView(view); !errors.Is(err, ErrUnsafeTarget) {
				t.Fatalf("%s mutation error = %v", name, err)
			}
		})
	}

	t.Run("deny does not grant and does not cancel unsafe allow", func(t *testing.T) {
		denyOnly := windowsObservedTestView(t, user, append(trustedFull, windowsTestACE{sid: everyone, mask: windowsFileAllAccess, mode: windows.DENY_ACCESS}))
		if err := validateWindowsObservedSecurityView(denyOnly); err != nil {
			t.Fatalf("basic deny rejected: %v", err)
		}
		denyAndAllow := windowsObservedTestView(t, user, append(trustedFull,
			windowsTestACE{sid: everyone, mask: windowsFileAllAccess, mode: windows.DENY_ACCESS},
			windowsTestACE{sid: everyone, mask: windows.FILE_WRITE_DATA, mode: windows.GRANT_ACCESS},
		))
		if err := validateWindowsObservedSecurityView(denyAndAllow); !errors.Is(err, ErrUnsafeTarget) {
			t.Fatalf("deny incorrectly cancelled unsafe allow: %v", err)
		}
	})

	t.Run("duplicate allows are unioned", func(t *testing.T) {
		view := windowsObservedTestView(t, user, append(trustedFull,
			windowsTestACE{sid: everyone, mask: windows.FILE_LIST_DIRECTORY, mode: windows.GRANT_ACCESS},
			windowsTestACE{sid: everyone, mask: windows.FILE_READ_ATTRIBUTES, mode: windows.GRANT_ACCESS},
			windowsTestACE{sid: everyone, mask: windows.FILE_READ_ATTRIBUTES, mode: windows.GRANT_ACCESS},
		))
		if err := validateWindowsObservedSecurityView(view); err != nil {
			t.Fatalf("safe duplicate allow rejected: %v", err)
		}
	})

	t.Run("absent null and defaulted DACL", func(t *testing.T) {
		valid := windowsObservedTestView(t, user, trustedFull)
		cases := []windowsSecurityView{
			{owner: user},
			{owner: user, control: windows.SE_DACL_PRESENT},
			{owner: user, control: windows.SE_DACL_PRESENT | windows.SE_DACL_DEFAULTED, dacl: valid.dacl},
			{owner: user, control: windows.SE_DACL_PRESENT, dacl: valid.dacl, daclDefaulted: true},
		}
		for index, view := range cases {
			if err := validateWindowsObservedSecurityView(view); !errors.Is(err, ErrUnsafeTarget) {
				t.Fatalf("invalid DACL case %d error = %v", index, err)
			}
		}
	})

	for name, acl := range map[string]*windows.ACL{
		"object ACE":        rawWindowsTestACL(t, windows.ACCESS_ALLOWED_ACE_TYPE+5, 0, windows.FILE_READ_DATA, everyone, false),
		"callback ACE":      rawWindowsTestACL(t, windows.ACCESS_ALLOWED_ACE_TYPE+9, 0, windows.FILE_READ_DATA, everyone, false),
		"unknown ACE":       rawWindowsTestACL(t, 0xff, 0, windows.FILE_READ_DATA, everyone, false),
		"invalid ACE flags": rawWindowsTestACL(t, windows.ACCESS_ALLOWED_ACE_TYPE, 0x80, windows.FILE_READ_DATA, everyone, false),
		"invalid ACE size":  invalidSizeWindowsTestACL(t, everyone),
		"invalid SID":       rawWindowsTestACL(t, windows.ACCESS_ALLOWED_ACE_TYPE, 0, windows.FILE_READ_DATA, everyone, true),
	} {
		t.Run(name, func(t *testing.T) {
			view := windowsSecurityView{owner: user, control: windows.SE_DACL_PRESENT, dacl: acl}
			if err := validateWindowsObservedSecurityView(view); !errors.Is(err, ErrUnsafeTarget) {
				t.Fatalf("malformed ACL error = %v", err)
			}
		})
	}
}

func TestM7WindowsProtectedTempSupportsManagedNamespaceLifecycle(t *testing.T) {
	knownFolderParent, err := hostedCITestTempParent()
	if err != nil {
		t.Fatal(err)
	}
	t.Run("Known Folder ignores ambient path variables", func(t *testing.T) {
		ambientRoot := t.TempDir()
		for _, name := range []string{"LOCALAPPDATA", "USERPROFILE", "TEMP", "TMP", "RUNNER_TEMP"} {
			t.Setenv(name, filepath.Join(ambientRoot, "ambient-"+strings.ToLower(name)+"-must-not-be-authority"))
		}
		knownFolderAfterAmbientChange, err := hostedCITestTempParent()
		if err != nil {
			t.Fatal(err)
		}
		if !strings.EqualFold(filepath.Clean(knownFolderAfterAmbientChange), filepath.Clean(knownFolderParent)) {
			t.Fatalf("current-token Known Folder changed with ambient environment: before=%q after=%q", knownFolderParent, knownFolderAfterAmbientChange)
		}
	})
	if strings.EqualFold(filepath.Base(knownFolderParent), "Temp") {
		t.Fatalf("unsafe LocalAppData Temp child selected instead of Known Folder root: %q", knownFolderParent)
	}
	t.Run("generic CI does not claim strict marker authority", func(t *testing.T) {
		t.Setenv("CI", "true")
		t.Setenv("GITHUB_ACTIONS", "")
		t.Setenv("MAHOROBA_M7_WINDOWS_LOCAL_CLOSURE", "")
		t.Setenv(localClosureSuiteChildEnvironmentName, "")
		t.Setenv("MAHOROBA_CI_WINDOWS_TEST_TEMP", "")
		if marker := verifyStrictWindowsTempEnvironment(t, knownFolderParent); marker != "" {
			t.Fatalf("generic CI acquired strict Windows marker authority: %q", marker)
		}
	})
	evidenceParent := verifyStrictWindowsTempEnvironment(t, knownFolderParent)

	parent := newProtectedWindowsTestParent(t)
	for name, input := range map[string]struct{ parent, basename string }{
		"relative parent":  {parent: "relative", basename: "child"},
		"drive root":       {parent: filepath.VolumeName(parent) + `\`, basename: "child"},
		"parent escape":    {parent: parent, basename: `..\escape`},
		"alternate stream": {parent: parent, basename: "child:stream"},
		"trailing alias":   {parent: parent, basename: "child."},
	} {
		t.Run("reject "+name, func(t *testing.T) {
			if _, err := PrepareCITestTemp(input.parent, input.basename); !errors.Is(err, ErrUnsafeTarget) {
				t.Fatalf("invalid CI temp input error = %v", err)
			}
		})
	}
	protected, err := PrepareCITestTemp(parent, "strict-protected")
	if err != nil {
		t.Fatal(err)
	}
	file := mustOpenDirectoryForSecurityTest(t, protected)
	if err := validateWindowsInheritableProtectedSecurity(file); err != nil {
		_ = file.Close()
		t.Fatalf("protected temp DACL: %v", err)
	}
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := PrepareCITestTemp(parent, "strict-protected"); err == nil {
		t.Fatal("create-new protected temp reused an existing directory")
	}

	lock, err := AcquireExistingParent(filepath.Join(protected, "runtime"))
	if err != nil {
		t.Fatal(err)
	}
	defer lock.Close()
	target, err := lock.OpenOrCreateTarget(0o700)
	if err != nil {
		t.Fatal(err)
	}
	defer target.Close()
	regular, err := target.OpenOrCreateRegular(".mahoroba.lock", 0o600)
	if err != nil {
		t.Fatal(err)
	}
	defer regular.Close()
	for name, entry := range map[string]*os.File{"rendezvous": lock.file, "target": target.file, "regular": regular} {
		if err := validateWindowsProtectedSecurity(entry); err != nil {
			t.Fatalf("%s exact managed DACL: %v", name, err)
		}
	}
	if evidenceParent != "" {
		exerciseStrictManagedNamespaceLifecycle(t, evidenceParent)
	}
}

func verifyStrictWindowsTempEnvironment(t *testing.T, knownFolderParent string) string {
	t.Helper()
	context, err := strictWindowsTempContextFromValues(
		os.Getenv("CI"),
		os.Getenv("GITHUB_ACTIONS"),
		os.Getenv("MAHOROBA_M7_WINDOWS_LOCAL_CLOSURE"),
		os.Getenv(localClosureSuiteChildEnvironmentName),
	)
	if err != nil {
		t.Fatalf("contradictory strict Windows temp context: %v", err)
	}
	if !context.githubActions && !context.localClosure {
		return ""
	}
	marker := strings.TrimSpace(os.Getenv("MAHOROBA_CI_WINDOWS_TEST_TEMP"))
	if marker == "" || !filepath.IsAbs(marker) ||
		!strings.EqualFold(filepath.Clean(filepath.Dir(marker)), filepath.Clean(knownFolderParent)) {
		t.Fatalf("strict protected temp marker is missing or outside current-token LocalAppData: %q", marker)
	}
	if err := validateStrictWindowsTempVariables(marker, context, os.Getenv); err != nil {
		t.Fatalf("strict protected temp variables are invalid: %v", err)
	}
	if err := ValidateCITestTemp(marker); err != nil {
		t.Fatalf("strict protected temp exported validation: %v", err)
	}
	file, identity, err := platformOpenParent(marker)
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	if err := validateWindowsInheritableProtectedSecurity(file); err != nil {
		t.Fatalf("strict protected temp exact DACL: %v", err)
	}
	binding, err := platformBindParentPath(marker, identity)
	if err != nil {
		t.Fatal(err)
	}
	defer binding.close()
	if err := binding.verify(); err != nil {
		t.Fatalf("strict protected temp binding: %v", err)
	}
	return marker
}

const (
	localClosureEvidenceBasename          = "evidence"
	localClosureGoAuthorityBasename       = "windows-go-authority"
	localClosureSuiteChildEnvironmentName = "MAHOROBA_M7_WINDOWS_SUITE_CHILD"
)

var localClosureGoCacheVariables = []struct {
	name     string
	basename string
}{
	{name: "GOPATH", basename: "gopath"},
	{name: "GOMODCACHE", basename: "gomodcache"},
	{name: "GOCACHE", basename: "gocache"},
	{name: "GOTMPDIR", basename: "gotmp"},
}

type strictWindowsTempContext struct {
	githubActions bool
	localClosure  bool
	suiteChild    bool
}

func strictWindowsTempContextFromValues(ci, githubActions, localClosure, suiteChild string) (strictWindowsTempContext, error) {
	for _, value := range []string{githubActions, localClosure, suiteChild} {
		if value != "" && value != "true" {
			return strictWindowsTempContext{}, ErrUnsafeTarget
		}
	}
	github := githubActions == "true"
	local := localClosure == "true"
	child := suiteChild == "true"
	if github && ci != "true" || github && local || github && child || child && !local {
		return strictWindowsTempContext{}, ErrUnsafeTarget
	}
	return strictWindowsTempContext{githubActions: github, localClosure: local, suiteChild: child}, nil
}

func validateStrictWindowsTempVariables(marker string, context strictWindowsTempContext, lookup func(string) string) error {
	if !filepath.IsAbs(marker) || lookup == nil {
		return ErrUnsafeTarget
	}
	for _, name := range []string{"TMP", "TEMP", "TMPDIR"} {
		if !sameWindowsTestPath(lookup(name), marker) {
			return ErrUnsafeTarget
		}
	}
	if !context.suiteChild {
		if !sameWindowsTestPath(lookup("GOTMPDIR"), marker) {
			return ErrUnsafeTarget
		}
		return nil
	}
	if !context.localClosure || context.githubActions {
		return ErrUnsafeTarget
	}
	evidence := filepath.Join(marker, localClosureEvidenceBasename)
	authority := filepath.Join(evidence, localClosureGoAuthorityBasename)
	if err := ValidateProtectedCITestChild(marker, evidence); err != nil {
		return err
	}
	if err := ValidateProtectedCITestChild(evidence, authority); err != nil {
		return err
	}
	for _, cache := range localClosureGoCacheVariables {
		path := filepath.Join(authority, cache.basename)
		if !sameWindowsTestPath(lookup(cache.name), path) {
			return ErrUnsafeTarget
		}
		if err := ValidateProtectedCITestChild(authority, path); err != nil {
			return err
		}
	}
	return nil
}

func sameWindowsTestPath(left, right string) bool {
	return filepath.IsAbs(left) && filepath.IsAbs(right) &&
		strings.EqualFold(filepath.Clean(left), filepath.Clean(right))
}

func TestM7WindowsStrictGOTMPDIRSeparatesHostedAndLocalClosureAuthorities(t *testing.T) {
	t.Run("context markers reject incomplete and contradictory authority", func(t *testing.T) {
		for _, test := range []struct {
			name                     string
			ci, github, local, child string
			wantErr                  bool
		}{
			{name: "inactive"},
			{name: "Hosted", ci: "true", github: "true"},
			{name: "local preflight", local: "true"},
			{name: "local suite child", local: "true", child: "true"},
			{name: "GitHub without CI", github: "true", wantErr: true},
			{name: "GitHub and local", ci: "true", github: "true", local: "true", wantErr: true},
			{name: "Hosted child", ci: "true", github: "true", child: "true", wantErr: true},
			{name: "child without local", child: "true", wantErr: true},
			{name: "nonexact child marker", local: "true", child: "TRUE", wantErr: true},
		} {
			t.Run(test.name, func(t *testing.T) {
				_, err := strictWindowsTempContextFromValues(test.ci, test.github, test.local, test.child)
				if (err != nil) != test.wantErr {
					t.Fatalf("context error = %v, wantErr=%v", err, test.wantErr)
				}
			})
		}
	})

	type fixture struct {
		marker    string
		authority string
		values    map[string]string
	}
	makeProtectedCache := func(t *testing.T) fixture {
		t.Helper()
		marker := newProtectedWindowsTestParent(t)
		evidence, err := PrepareProtectedCITestChild(marker, localClosureEvidenceBasename)
		if err != nil {
			t.Fatal(err)
		}
		authority, err := PrepareProtectedCITestChild(evidence, localClosureGoAuthorityBasename)
		if err != nil {
			t.Fatal(err)
		}
		values := map[string]string{"TMP": marker, "TEMP": marker, "TMPDIR": marker}
		for _, cache := range localClosureGoCacheVariables {
			path, err := PrepareProtectedCITestChild(authority, cache.basename)
			if err != nil {
				t.Fatal(err)
			}
			values[cache.name] = path
		}
		return fixture{marker: marker, authority: authority, values: values}
	}
	lookup := func(values map[string]string) func(string) string {
		return func(name string) string { return values[name] }
	}

	t.Run("Hosted requires marker", func(t *testing.T) {
		fixture := makeProtectedCache(t)
		values := map[string]string{"TMP": fixture.marker, "TEMP": fixture.marker, "TMPDIR": fixture.marker, "GOTMPDIR": fixture.marker}
		context := strictWindowsTempContext{githubActions: true}
		if err := validateStrictWindowsTempVariables(fixture.marker, context, lookup(values)); err != nil {
			t.Fatalf("Hosted marker rejected: %v", err)
		}
		values["GOTMPDIR"] = fixture.values["GOTMPDIR"]
		if err := validateStrictWindowsTempVariables(fixture.marker, context, lookup(values)); !errors.Is(err, ErrUnsafeTarget) {
			t.Fatalf("Hosted nested Go temp error = %v, want ErrUnsafeTarget", err)
		}
	})

	t.Run("local preflight requires marker", func(t *testing.T) {
		fixture := makeProtectedCache(t)
		values := map[string]string{"TMP": fixture.marker, "TEMP": fixture.marker, "TMPDIR": fixture.marker, "GOTMPDIR": fixture.marker}
		if err := validateStrictWindowsTempVariables(fixture.marker, strictWindowsTempContext{localClosure: true}, lookup(values)); err != nil {
			t.Fatalf("local preflight marker rejected: %v", err)
		}
		values["GOTMPDIR"] = fixture.values["GOTMPDIR"]
		if err := validateStrictWindowsTempVariables(fixture.marker, strictWindowsTempContext{localClosure: true}, lookup(values)); !errors.Is(err, ErrUnsafeTarget) {
			t.Fatalf("local preflight nested Go temp error = %v, want ErrUnsafeTarget", err)
		}
	})

	t.Run("local suite child requires exact protected cache environment", func(t *testing.T) {
		fixture := makeProtectedCache(t)
		if err := validateStrictWindowsTempVariables(fixture.marker, strictWindowsTempContext{localClosure: true, suiteChild: true}, lookup(fixture.values)); err != nil {
			t.Fatalf("local protected Go temp rejected: %v", err)
		}
	})

	for _, test := range []struct {
		name  string
		build func(t *testing.T) fixture
	}{
		{
			name: "marker in suite child",
			build: func(t *testing.T) fixture {
				fixture := makeProtectedCache(t)
				fixture.values["GOTMPDIR"] = fixture.marker
				return fixture
			},
		},
		{
			name: "arbitrary nested child",
			build: func(t *testing.T) fixture {
				fixture := makeProtectedCache(t)
				nested, err := PrepareProtectedCITestChild(fixture.values["GOTMPDIR"], "nested")
				if err != nil {
					t.Fatal(err)
				}
				fixture.values["GOTMPDIR"] = nested
				return fixture
			},
		},
		{
			name: "wrong authority basename",
			build: func(t *testing.T) fixture {
				marker := newProtectedWindowsTestParent(t)
				evidence, err := PrepareProtectedCITestChild(marker, localClosureEvidenceBasename)
				if err != nil {
					t.Fatal(err)
				}
				authority, err := PrepareProtectedCITestChild(evidence, "wrong-go-authority")
				if err != nil {
					t.Fatal(err)
				}
				values := map[string]string{"TMP": marker, "TEMP": marker, "TMPDIR": marker}
				for _, cache := range localClosureGoCacheVariables {
					path, err := PrepareProtectedCITestChild(authority, cache.basename)
					if err != nil {
						t.Fatal(err)
					}
					values[cache.name] = path
				}
				return fixture{marker: marker, authority: authority, values: values}
			},
		},
		{
			name: "weak direct children",
			build: func(t *testing.T) fixture {
				marker := newProtectedWindowsTestParent(t)
				authority := filepath.Join(marker, localClosureEvidenceBasename, localClosureGoAuthorityBasename)
				if err := os.MkdirAll(authority, 0o700); err != nil {
					t.Fatal(err)
				}
				values := map[string]string{"TMP": marker, "TEMP": marker, "TMPDIR": marker}
				for _, cache := range localClosureGoCacheVariables {
					path := filepath.Join(authority, cache.basename)
					if err := os.Mkdir(path, 0o700); err != nil {
						t.Fatal(err)
					}
					values[cache.name] = path
				}
				return fixture{marker: marker, authority: authority, values: values}
			},
		},
		{
			name: "missing cache sibling",
			build: func(t *testing.T) fixture {
				fixture := makeProtectedCache(t)
				if err := os.Remove(fixture.values["GOCACHE"]); err != nil {
					t.Fatal(err)
				}
				return fixture
			},
		},
		{
			name: "replaced weak cache child",
			build: func(t *testing.T) fixture {
				fixture := makeProtectedCache(t)
				path := fixture.values["GOTMPDIR"]
				if err := os.Remove(path); err != nil {
					t.Fatal(err)
				}
				if err := os.Mkdir(path, 0o700); err != nil {
					t.Fatal(err)
				}
				return fixture
			},
		},
		{
			name: "wrong GOPATH binding",
			build: func(t *testing.T) fixture {
				fixture := makeProtectedCache(t)
				fixture.values["GOPATH"] = fixture.values["GOCACHE"]
				return fixture
			},
		},
	} {
		t.Run("local closure rejects "+test.name, func(t *testing.T) {
			fixture := test.build(t)
			context := strictWindowsTempContext{localClosure: true, suiteChild: true}
			if err := validateStrictWindowsTempVariables(fixture.marker, context, lookup(fixture.values)); err == nil {
				t.Fatal("unsafe local cache environment was accepted")
			}
		})
	}
}

func exerciseStrictManagedNamespaceLifecycle(t *testing.T, parent string) {
	t.Helper()
	lock, err := AcquireExistingParent(filepath.Join(parent, "hosted-managed-lifecycle"))
	if err != nil {
		t.Fatal(err)
	}
	defer lock.Close()
	target, err := lock.OpenOrCreateTarget(0o700)
	if err != nil {
		t.Fatal(err)
	}
	defer target.Close()
	regular, err := target.OpenOrCreateRegular(".mahoroba.hosted.lifecycle", 0o600)
	if err != nil {
		t.Fatal(err)
	}
	defer regular.Close()
	for name, entry := range map[string]*os.File{"hosted rendezvous": lock.file, "hosted target": target.file, "hosted regular": regular} {
		if err := validateWindowsProtectedSecurity(entry); err != nil {
			t.Fatalf("%s exact managed DACL: %v", name, err)
		}
	}
}

func mustOpenDirectoryForSecurityTest(t *testing.T, path string) *os.File {
	t.Helper()
	file, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	return file
}

type windowsTestACE struct {
	sid   *windows.SID
	mask  windows.ACCESS_MASK
	mode  windows.ACCESS_MODE
	flags uint32
}

func windowsObservedTestView(t *testing.T, owner *windows.SID, entries []windowsTestACE) windowsSecurityView {
	t.Helper()
	return windowsSecurityView{
		owner:   owner,
		control: windows.SE_DACL_PRESENT,
		dacl:    windowsTestACL(t, entries),
	}
}

func windowsTestACL(t *testing.T, specifications []windowsTestACE) *windows.ACL {
	t.Helper()
	entries := make([]windows.EXPLICIT_ACCESS, 0, len(specifications))
	var pinner runtime.Pinner
	defer pinner.Unpin()
	for _, specification := range specifications {
		if specification.sid == nil {
			t.Fatal("nil test SID")
		}
		pinner.Pin(specification.sid)
		entries = append(entries, windows.EXPLICIT_ACCESS{
			AccessPermissions: specification.mask,
			AccessMode:        specification.mode,
			Inheritance:       specification.flags,
			Trustee: windows.TRUSTEE{
				TrusteeForm:  windows.TRUSTEE_IS_SID,
				TrusteeType:  windows.TRUSTEE_IS_UNKNOWN,
				TrusteeValue: windows.TrusteeValueFromSID(specification.sid),
			},
		})
	}
	acl, err := windows.ACLFromEntries(entries, nil)
	if err != nil {
		t.Fatal(err)
	}
	return acl
}

func rawWindowsTestACL(t *testing.T, aceType, flags uint8, mask windows.ACCESS_MASK, sid *windows.SID, corruptSID bool) *windows.ACL {
	t.Helper()
	if sid == nil || !sid.IsValid() {
		t.Fatal("invalid source test SID")
	}
	sidBytes := unsafe.Slice((*byte)(unsafe.Pointer(sid)), sid.Len())
	aceSize := 8 + len(sidBytes)
	raw := make([]byte, 8+aceSize)
	raw[0] = 2 // ACL_REVISION
	binary.LittleEndian.PutUint16(raw[2:4], uint16(len(raw)))
	binary.LittleEndian.PutUint16(raw[4:6], 1)
	raw[8] = aceType
	raw[9] = flags
	binary.LittleEndian.PutUint16(raw[10:12], uint16(aceSize))
	binary.LittleEndian.PutUint32(raw[12:16], uint32(mask))
	copy(raw[16:], sidBytes)
	if corruptSID {
		raw[16] = 2
	}
	return (*windows.ACL)(unsafe.Pointer(&raw[0]))
}

func invalidSizeWindowsTestACL(t *testing.T, sid *windows.SID) *windows.ACL {
	t.Helper()
	acl := rawWindowsTestACL(t, windows.ACCESS_ALLOWED_ACE_TYPE, 0, windows.FILE_READ_DATA, sid, false)
	raw := unsafe.Slice((*byte)(unsafe.Pointer(acl)), 12)
	binary.LittleEndian.PutUint16(raw[10:12], 4)
	return acl
}

func mustCurrentWindowsSID(t *testing.T) *windows.SID {
	t.Helper()
	sid, err := currentWindowsUserSID()
	if err != nil || sid == nil {
		t.Fatal(err)
	}
	return sid
}

func mustStringWindowsSID(t *testing.T, value string) *windows.SID {
	t.Helper()
	sid, err := windows.StringToSid(value)
	if err != nil || sid == nil {
		t.Fatal(err)
	}
	return sid
}

func newProtectedWindowsTestParent(t *testing.T) string {
	t.Helper()
	outer := t.TempDir()
	name, err := windows.UTF16PtrFromString(outer)
	if err != nil {
		t.Fatal(err)
	}
	handle, err := windows.CreateFile(
		name,
		windows.FILE_GENERIC_READ|windows.FILE_GENERIC_WRITE|windows.READ_CONTROL,
		windows.FILE_SHARE_READ|windows.FILE_SHARE_WRITE,
		nil,
		windows.OPEN_EXISTING,
		windows.FILE_FLAG_BACKUP_SEMANTICS|windows.FILE_FLAG_OPEN_REPARSE_POINT,
		0,
	)
	if err != nil {
		t.Fatal(err)
	}
	outerFile := os.NewFile(uintptr(handle), outer)
	if outerFile == nil {
		_ = windows.CloseHandle(handle)
		t.Fatal("wrap Windows test parent handle")
	}
	defer outerFile.Close()
	descriptor, err := protectedWindowsSecurityDescriptorWithInheritance(windows.SUB_CONTAINERS_AND_OBJECTS_INHERIT)
	if err != nil {
		t.Fatal(err)
	}
	protected, _, err := ntOpenRelative(
		outerFile,
		"protected",
		windows.FILE_GENERIC_READ|windows.FILE_GENERIC_WRITE|windows.READ_CONTROL,
		windows.FILE_SHARE_READ|windows.FILE_SHARE_WRITE,
		windows.FILE_CREATE,
		windows.FILE_DIRECTORY_FILE|windows.FILE_OPEN_REPARSE_POINT|windows.FILE_SYNCHRONOUS_IO_NONALERT,
		descriptor,
	)
	if err != nil {
		t.Fatal(err)
	}
	if err := validateWindowsInheritableProtectedSecurity(protected); err != nil {
		_ = protected.Close()
		t.Fatal(err)
	}
	if err := protected.Close(); err != nil {
		t.Fatal(err)
	}
	return filepath.Join(outer, "protected")
}

func namespaceLockTestTempDir(t *testing.T) string {
	t.Helper()
	return newProtectedWindowsTestParent(t)
}

func TestM7WindowsCaseAliasUsesOnePhysicalNamespaceLock(t *testing.T) {
	parent := newProtectedWindowsTestParent(t)
	first, err := AcquireExistingParent(filepath.Join(parent, "runtime"))
	if err != nil {
		t.Fatal(err)
	}
	defer first.Close()
	if second, err := AcquireExistingParent(filepath.Join(parent, strings.ToUpper("runtime"))); !errors.Is(err, ErrBusy) {
		if second != nil {
			_ = second.Close()
		}
		t.Fatalf("case-alias Acquire = %v, want ErrBusy", err)
	}
}

func TestM7WindowsLiveHandlesDenyDeleteSharing(t *testing.T) {
	parent := newProtectedWindowsTestParent(t)
	targetPath := filepath.Join(parent, "runtime")
	lock, err := AcquireExistingParent(targetPath)
	if err != nil {
		t.Fatal(err)
	}
	defer lock.Close()
	lockPath := filepath.Join(parent, ".runtime.mahoroba-namespace.lock")
	if err := os.Remove(lockPath); err == nil {
		t.Fatal("live namespace rendezvous allowed delete")
	}
	target, err := lock.OpenOrCreateTarget(0o700)
	if err != nil {
		t.Fatal(err)
	}
	defer target.Close()
	if err := os.Remove(targetPath); err == nil {
		t.Fatal("live target handle allowed delete")
	}
}

func TestM7WindowsNamespaceLockRejectsAncestorReplacement(t *testing.T) {
	container := newProtectedWindowsTestParent(t)
	ancestor := filepath.Join(container, "ancestor")
	parent := filepath.Join(ancestor, "parent")
	if err := os.MkdirAll(parent, 0o700); err != nil {
		t.Fatal(err)
	}
	lock, err := AcquireExistingParent(filepath.Join(parent, "runtime"))
	if err != nil {
		t.Fatal(err)
	}
	defer lock.Close()

	detached := filepath.Join(container, "detached-ancestor")
	if err := os.Rename(ancestor, detached); err != nil {
		// Denying the rename while the binding is retained is stronger than
		// detecting it at the next operation.
		return
	}
	if err := os.MkdirAll(parent, 0o700); err != nil {
		t.Fatal(err)
	}
	if _, err := lock.TargetExists(); !errors.Is(err, ErrUnsafeTarget) {
		t.Fatalf("TargetExists after ancestor replacement = %v, want ErrUnsafeTarget", err)
	}
}
