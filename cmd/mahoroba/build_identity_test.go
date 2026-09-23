package main

import (
	"strings"
	"testing"
)

func TestM7OCIInjectedRevisionBindsBackupBuildIdentity(t *testing.T) {
	previous := buildGitRevision
	t.Cleanup(func() { buildGitRevision = previous })

	want := strings.Repeat("a", 40)
	buildGitRevision = want
	if got := currentBackupBuildIdentity().GitRevision; got != want {
		t.Fatalf("injected backup git revision = %q, want %q", got, want)
	}

	buildGitRevision = "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"
	if got := currentBackupBuildIdentity().GitRevision; got == buildGitRevision {
		t.Fatal("malformed injected revision became backup authority")
	}
}
