//go:build windows

package sqlite

import (
	"context"
	"os"

	"mahoroba.local/mahoroba/internal/descriptorpath"
)

// OpenImmutableDescriptorInspection validates that fallbackPath still names
// the caller-retained Windows handle, whose sharing mode denies write/delete,
// and retains caller ownership of descriptor for the inspection lifetime.
func OpenImmutableDescriptorInspection(ctx context.Context, descriptor *os.File, fallbackPath string) (*Inspection, error) {
	path, err := descriptorpath.ReadOnly(descriptor, fallbackPath)
	if err != nil {
		return nil, err
	}
	return OpenImmutableInspection(ctx, path)
}
