//go:build !linux && !windows

package sqlite

import (
	"context"
	"errors"
	"os"
)

func OpenImmutableDescriptorInspection(context.Context, *os.File, string) (*Inspection, error) {
	return nil, errors.New("sqlite: immutable descriptor inspection is unsupported")
}
