//go:build windows

package fssecure

import (
	"errors"
	"fmt"
	"testing"

	"golang.org/x/sys/windows"
)

func TestWindowsCreatedSecurityDescriptorMatchesExactPolicy(t *testing.T) {
	policy, err := CurrentSecurityPolicy()
	if err != nil {
		t.Fatal(err)
	}
	sd, err := buildWindowsSecurityDescriptor(policy)
	if err != nil {
		t.Fatal(err)
	}
	if err := validateWindowsSecurityDescriptor(sd, policy); err != nil {
		t.Fatalf("created descriptor rejected: %v", err)
	}
}

func TestWindowsSecurityDescriptorRejectsUnknownOrInheritedACE(t *testing.T) {
	policy, err := CurrentSecurityPolicy()
	if err != nil {
		t.Fatal(err)
	}
	valid := func(dacl string) *windows.SECURITY_DESCRIPTOR {
		t.Helper()
		sd, err := windows.SecurityDescriptorFromString(fmt.Sprintf("O:%s%s", policy.ownerSID, dacl))
		if err != nil {
			t.Fatal(err)
		}
		return sd
	}
	base := fmt.Sprintf("D:P(A;;FA;;;%s)(A;;FA;;;SY)(A;;FA;;;BA)", policy.ownerSID)
	if err := validateWindowsSecurityDescriptor(valid(base), policy); err != nil {
		t.Fatalf("valid descriptor rejected: %v", err)
	}
	unknown := fmt.Sprintf("D:P(A;;FA;;;%s)(A;;FA;;;SY)(A;;FA;;;BA)(A;;FA;;;WD)", policy.ownerSID)
	if err := validateWindowsSecurityDescriptor(valid(unknown), policy); !errors.Is(err, ErrUnsafeFilesystem) {
		t.Fatalf("unknown SID error = %v", err)
	}
	inherited := fmt.Sprintf("D:P(A;CI;FA;;;%s)(A;;FA;;;SY)(A;;FA;;;BA)", policy.ownerSID)
	if err := validateWindowsSecurityDescriptor(valid(inherited), policy); !errors.Is(err, ErrUnsafeFilesystem) {
		t.Fatalf("inherited ACE error = %v", err)
	}
}
