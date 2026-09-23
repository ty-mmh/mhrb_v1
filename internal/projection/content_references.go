package projection

import (
	"fmt"
	"slices"
	"strings"

	"mahoroba.local/mahoroba/internal/canonical"
)

const (
	ContentReferencesName    Name    = "content_references"
	ContentReferencesVersion Version = "content-references-v1"
)

// ContentReferencesDefinition is the resident-scoped, snapshot-complete
// reachability Projection used by physical blob GC. It intentionally has no
// dependency versions: every row is derived directly from one Canonical
// snapshot at the requested head.
func ContentReferencesDefinition() Definition {
	return Definition{
		Name:          ContentReferencesName,
		Version:       ContentReferencesVersion,
		TimeSensitive: false,
		Dependencies:  []DependencyKind{},
	}
}

// ContentReference is one typed row in content_references. Erased content
// keeps its structural referrer rows but has all three locator fields nil.
type ContentReference struct {
	ContentID         canonical.ID
	ResidentID        canonical.ID
	ReferrerKind      string
	ReferrerID        canonical.ID
	ReferrerField     string
	DedupeScopeID     *canonical.ID
	BlobHashAlgorithm *string
	BlobHash          *canonical.Digest
}

func (reference ContentReference) Validate() error {
	if err := reference.ContentID.Validate(); err != nil {
		return fmt.Errorf("projection: invalid content reference content: %w", err)
	}
	if err := reference.ResidentID.Validate(); err != nil {
		return fmt.Errorf("projection: invalid content reference resident: %w", err)
	}
	if err := reference.ReferrerID.Validate(); err != nil {
		return fmt.Errorf("projection: invalid content reference referrer: %w", err)
	}
	if !validReferenceToken(reference.ReferrerKind) || !validReferenceToken(reference.ReferrerField) {
		return fmt.Errorf("projection: invalid content reference tuple %q/%q", reference.ReferrerKind, reference.ReferrerField)
	}
	presentLocator := reference.DedupeScopeID != nil || reference.BlobHashAlgorithm != nil || reference.BlobHash != nil
	if presentLocator {
		if reference.DedupeScopeID == nil || reference.BlobHashAlgorithm == nil || reference.BlobHash == nil {
			return fmt.Errorf("projection: content reference locator must be wholly present or wholly absent")
		}
		if err := reference.DedupeScopeID.Validate(); err != nil {
			return fmt.Errorf("projection: invalid content reference dedupe scope: %w", err)
		}
		if *reference.BlobHashAlgorithm != "sha256" {
			return fmt.Errorf("projection: unsupported content reference hash algorithm %q", *reference.BlobHashAlgorithm)
		}
	}
	return nil
}

// ValidateContentReferences fixes the complete body order to the physical
// primary-key order and rejects duplicate structural references.
func ValidateContentReferences(values []ContentReference, residentID canonical.ID) error {
	if err := residentID.Validate(); err != nil {
		return err
	}
	previous := ""
	for index, value := range values {
		if err := value.Validate(); err != nil {
			return fmt.Errorf("projection: content reference %d: %w", index, err)
		}
		if value.ResidentID != residentID {
			return fmt.Errorf("projection: content reference %d belongs to another resident", index)
		}
		key := contentReferenceKey(value)
		if index > 0 && key <= previous {
			return fmt.Errorf("projection: content references are unordered or duplicated")
		}
		previous = key
	}
	return nil
}

func SortContentReferences(values []ContentReference) {
	slices.SortFunc(values, func(left, right ContentReference) int {
		return strings.Compare(contentReferenceKey(left), contentReferenceKey(right))
	})
}

func contentReferenceKey(value ContentReference) string {
	return value.ContentID.String() + "\x00" + value.ReferrerKind + "\x00" +
		value.ReferrerID.String() + "\x00" + value.ReferrerField
}

func validReferenceToken(value string) bool {
	if value == "" || value[0] < 'a' || value[0] > 'z' {
		return false
	}
	for _, character := range value[1:] {
		if (character < 'a' || character > 'z') && (character < '0' || character > '9') && character != '_' {
			return false
		}
	}
	return !strings.Contains(value, "__")
}
