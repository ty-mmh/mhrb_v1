//go:build !windows && !aix && !darwin && !dragonfly && !freebsd && !linux && !netbsd && !openbsd && !solaris

package fssecure

import "os"

func platformLock(*os.File) error {
	return unsupportedFilesystem("namespace locking unavailable", nil)
}
func platformUnlock(*os.File) error { return nil }
