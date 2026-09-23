//go:build !windows && !aix && !darwin && !dragonfly && !freebsd && !linux && !netbsd && !openbsd && !solaris

package fssecure

import "os"

func CurrentSecurityPolicy() (SecurityPolicy, error) {
	return SecurityPolicy{}, unsupportedFilesystem("security policy unavailable", nil)
}

func identityFromHandle(*os.File, EntryKind) (Identity, error) {
	return Identity{}, unsupportedFilesystem("stable handle identity unavailable", nil)
}
func validateOpenedSecurity(*os.File, EntryKind, SecurityPolicy) error {
	return unsupportedFilesystem("opened security validation unavailable", nil)
}
func validateDirectoryHandle(*os.File, Identity, SecurityPolicy) error {
	return unsupportedFilesystem("directory handle validation unavailable", nil)
}
func syncDirectory(*os.File) error {
	return unsupportedFilesystem("directory sync unavailable", nil)
}
