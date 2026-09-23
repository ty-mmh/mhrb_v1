//go:build !linux && !windows

package descriptorpath

import (
	"errors"
	"os"
)

// ReadOnly fails closed where the platform cannot expose an already-open file
// to a path-based read-only consumer without reopening mutable authority.
func ReadOnly(*os.File, string) (string, error) {
	return "", errors.New("descriptorpath: retained descriptor paths are unsupported")
}
