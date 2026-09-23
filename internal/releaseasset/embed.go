// Package releaseasset embeds immutable release collateral in the production
// binary. The same source bytes are copied into archives and OCI images.
package releaseasset

import (
	"crypto/sha256"
	_ "embed"
	"encoding/hex"
)

//go:embed THIRD_PARTY_LICENSES.txt
var thirdPartyLicenses []byte

func ThirdPartyLicenses() []byte {
	return append([]byte(nil), thirdPartyLicenses...)
}

func ThirdPartyLicensesSHA256() string {
	digest := sha256.Sum256(thirdPartyLicenses)
	return "sha256:" + hex.EncodeToString(digest[:])
}
