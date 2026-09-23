//go:build !linux && !windows

package fssecure

import "os"

func openRootRelative(string, bool, SecurityPolicy) (*os.File, *os.File, *directoryBinding, string, error) {
	return nil, nil, nil, "", unsupportedFilesystem("parent-relative root traversal is unavailable", nil)
}
func openRootRelativeReadOnly(string, SecurityPolicy) (*os.File, *os.File, *directoryBinding, string, error) {
	return nil, nil, nil, "", unsupportedFilesystem("read-only root traversal is unavailable", nil)
}
func openDirectoryRelative(*os.File, string, bool, SecurityPolicy) (*os.File, error) {
	return nil, unsupportedFilesystem("parent-relative directory open is unavailable", nil)
}
func openDirectoryRelativeReadOnly(*os.File, string, SecurityPolicy) (*os.File, error) {
	return nil, unsupportedFilesystem("read-only relative directory open is unavailable", nil)
}
func openDirectoryRelativeForPublish(*os.File, string, SecurityPolicy) (*os.File, error) {
	return nil, unsupportedFilesystem("directory publication is unsupported", nil)
}
func openRootRelativeForPublish(string, SecurityPolicy) (*os.File, *os.File, *directoryBinding, string, error) {
	return nil, nil, nil, "", unsupportedFilesystem("directory publication is unsupported", nil)
}
func openRegularRelative(*os.File, string, regularOpenMode, SecurityPolicy) (*os.File, bool, error) {
	return nil, false, unsupportedFilesystem("parent-relative regular open is unavailable", nil)
}
func duplicateOpenFile(*os.File, string) (*os.File, error) {
	return nil, unsupportedFilesystem("directory handle duplication is unavailable", nil)
}
func renameRelativeNoReplace(*os.File, string, *os.File, *os.File, string) error {
	return unsupportedFilesystem("parent-relative no-replace rename is unavailable", nil)
}
func removeRelative(*os.File, string, *os.File, SecurityPolicy) error {
	return unsupportedFilesystem("handle-bound delete is unavailable", nil)
}
func identityRelativeEntry(*os.File, string, EntryKind, SecurityPolicy) (Identity, error) {
	return Identity{}, unsupportedFilesystem("relative identity verification is unavailable", nil)
}
func validateRawDirectoryHandle(*os.File, Identity) error {
	return unsupportedFilesystem("raw ancestor handle verification is unavailable", nil)
}
func identityRelativeDirectoryRaw(*os.File, string) (Identity, error) {
	return Identity{}, unsupportedFilesystem("raw ancestor identity verification is unavailable", nil)
}
