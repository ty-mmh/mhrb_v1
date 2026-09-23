//go:build aix || darwin || dragonfly || freebsd || netbsd || openbsd || solaris

package fssecure

import (
	"os"

	"golang.org/x/sys/unix"
)

func bindStableIdentityGeneration(*os.File, unix.Stat_t, Identity) (Identity, error) {
	return Identity{}, unsupportedFilesystem("stable filesystem identity generation unavailable", nil)
}
