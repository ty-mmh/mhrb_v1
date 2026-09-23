//go:build !linux && !windows

package namespacelock

import (
	"os"
)

type platformIdentity struct{}

func platformOpenParent(string) (*os.File, platformIdentity, error) {
	return nil, platformIdentity{}, ErrUnsupported
}
func platformBindParentPath(string, platformIdentity) (parentPathBinding, error) {
	return nil, ErrUnsupported
}
func platformOpenRegular(*os.File, string, os.FileMode) (*os.File, error) {
	return nil, ErrUnsupported
}
func platformOpenRegularRead(*os.File, string) (*os.File, error) { return nil, ErrUnsupported }
func platformOpenObservedRegularRead(*os.File, string) (*os.File, error) {
	return nil, ErrUnsupported
}
func platformOpenTarget(*os.File, string, bool, os.FileMode) (*os.File, bool, error) {
	return nil, false, ErrUnsupported
}
func platformOpenObservedTarget(*os.File, string) (*os.File, bool, error) {
	return nil, false, ErrUnsupported
}
func platformIdentityOf(*os.File, entryKind) (platformIdentity, error) {
	return platformIdentity{}, ErrUnsupported
}
func platformVerifyIdentity(*os.File, platformIdentity, entryKind) error { return ErrUnsupported }
func platformLock(*os.File) error                                        { return ErrUnsupported }
func platformUnlock(*os.File) error                                      { return ErrUnsupported }
