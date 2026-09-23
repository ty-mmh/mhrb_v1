package projection

import (
	"strings"
	"testing"

	"mahoroba.local/mahoroba/internal/canonical"
)

func TestM7ContentReferencesDefinitionIsProductionContract(t *testing.T) {
	definition := ContentReferencesDefinition()
	if definition.Name != ContentReferencesName || definition.Version != ContentReferencesVersion ||
		definition.TimeSensitive || len(definition.Dependencies) != 0 || len(definition.RebuildOnActivation) != 0 {
		t.Fatalf("unexpected content references definition: %#v", definition)
	}
}

func TestM7ContentReferencesRequireCompleteLocatorAndStableOrder(t *testing.T) {
	residentID := contentReferenceID(t, "01J00000000000000000000001")
	contentID := contentReferenceID(t, "01J00000000000000000000002")
	referrerID := contentReferenceID(t, "01J00000000000000000000003")
	dedupeID := residentID
	algorithm := "sha256"
	digest := canonical.HashBlob([]byte("present"))
	present := ContentReference{
		ContentID: contentID, ResidentID: residentID, ReferrerKind: "content_object",
		ReferrerID: referrerID, ReferrerField: "blob", DedupeScopeID: &dedupeID,
		BlobHashAlgorithm: &algorithm, BlobHash: &digest,
	}
	erased := ContentReference{
		ContentID: contentID, ResidentID: residentID, ReferrerKind: "event",
		ReferrerID: referrerID, ReferrerField: "content_id",
	}
	values := []ContentReference{erased, present}
	SortContentReferences(values)
	if err := ValidateContentReferences(values, residentID); err != nil {
		t.Fatalf("valid present/erased references rejected: %v", err)
	}

	partial := present
	partial.BlobHash = nil
	if err := partial.Validate(); err == nil || !strings.Contains(err.Error(), "wholly present") {
		t.Fatalf("partial locator accepted: %v", err)
	}
	if err := ValidateContentReferences([]ContentReference{present, present}, residentID); err == nil {
		t.Fatal("duplicate reference accepted")
	}
}

func contentReferenceID(t *testing.T, value string) canonical.ID {
	t.Helper()
	parsed, err := canonical.ParseID(value)
	if err != nil {
		t.Fatal(err)
	}
	return parsed
}
