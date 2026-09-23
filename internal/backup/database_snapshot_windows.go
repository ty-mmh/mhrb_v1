//go:build windows

package backup

import (
	"context"
	"encoding/hex"
	"errors"
	"os"
	"strconv"

	"mahoroba.local/mahoroba/internal/fssecure"
)

// Windows secure bundle handles allow read sharing only, which prevents
// external write/delete/rename for the lifetime of verification and copy.
func captureVerifiedDatabase(
	ctx context.Context,
	source *fssecure.Handle,
	descriptor DatabaseFile,
) (*os.File, bool, int64, error) {
	if ctx == nil || source == nil {
		return nil, false, 0, errors.New("backup: database snapshot source is required")
	}
	digest, size, err := source.Hash(ctx)
	if err != nil {
		return nil, false, size, err
	}
	if strconv.FormatInt(size, 10) != descriptor.ByteSize ||
		"sha256:"+hex.EncodeToString(digest[:]) != descriptor.SHA256 {
		return nil, false, size, errors.New("backup: database snapshot hash or size differs")
	}
	return source.File(), false, size, nil
}
