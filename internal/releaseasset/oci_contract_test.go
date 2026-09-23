package releaseasset

import (
	"encoding/json"
	"strings"
	"testing"
)

func TestM7EmbeddedAndOCIReleaseLicenseInventoryAreSameSource(t *testing.T) {
	licenses := ThirdPartyLicenses()
	if len(licenses) < 1024 || !strings.HasPrefix(string(licenses), "mahoroba-third-party-license-inventory-v1\n") {
		t.Fatal("embedded third-party inventory is missing or malformed")
	}
	digest := ThirdPartyLicensesSHA256()
	if len(digest) != len("sha256:")+64 {
		t.Fatalf("license inventory digest = %q", digest)
	}
	if _, err := json.Marshal(map[string]string{"sha256": digest}); err != nil {
		t.Fatal(err)
	}
}
