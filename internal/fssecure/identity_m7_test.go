package fssecure

import (
	"errors"
	"testing"
)

func TestM7IdentityAcceptsWindowsLowFileIndex(t *testing.T) {
	identity := Identity{volume: 7, first: 0, second: 42, kind: KindRegular}
	if !identity.Valid() {
		t.Fatal("Windows identity with zero high index and nonzero low index was rejected")
	}
	if !identity.Equal(identity) {
		t.Fatal("valid Windows identity does not equal itself")
	}
}

func TestM7IdentityEqualityIncludesStableGeneration(t *testing.T) {
	identity := Identity{
		volume: 7, first: 42, mount: 11,
		generationSeconds: 1_700_000_000, generationNanos: 123, kind: KindRegular,
	}
	replacement := identity
	replacement.generationNanos++
	if identity.Equal(replacement) || replacement.Equal(identity) {
		t.Fatal("identity equality ignored stable generation evidence")
	}
	identityField, identityVolume := identity.artifactIdentityFields()
	replacementField, replacementVolume := replacement.artifactIdentityFields()
	if identityField == replacementField || identityVolume != replacementVolume {
		t.Fatalf("artifact identity generation binding = %q/%q vs %q/%q",
			identityField, identityVolume, replacementField, replacementVolume)
	}
}

func TestM7UnsupportedFilesystemSentinelMatrix(t *testing.T) {
	err := unsupportedFilesystem("fixture", errors.New("platform primitive unavailable"))
	if !errors.Is(err, ErrUnsafeFilesystem) || !errors.Is(err, ErrUnsupportedSecureFilesystem) {
		t.Fatalf("unsupported error sentinel matrix = %v", err)
	}
	if reason, ok := Reason(err); !ok || reason != ReasonUnsupportedFilesystem {
		t.Fatalf("unsupported reason = %q/%v", reason, ok)
	}
}

func TestM7SameVolumeBoundaryRejectsCrossVolume(t *testing.T) {
	first := Identity{volume: 7, first: 1, kind: KindDirectory}
	sameVolume := Identity{volume: 7, first: 2, kind: KindRegular}
	if err := requireSameVolume("fixture", first, sameVolume); err != nil {
		t.Fatalf("same-volume identity rejected: %v", err)
	}
	crossVolume := Identity{volume: 8, first: 2, kind: KindRegular}
	err := requireSameVolume("fixture", first, crossVolume)
	if !errors.Is(err, ErrUnsafeFilesystem) || !errors.Is(err, ErrUnsupportedSecureFilesystem) {
		t.Fatalf("cross-volume sentinel matrix = %v", err)
	}
	if reason, ok := Reason(err); !ok || reason != ReasonUnsupportedFilesystem {
		t.Fatalf("cross-volume reason = %q/%v", reason, ok)
	}
}
