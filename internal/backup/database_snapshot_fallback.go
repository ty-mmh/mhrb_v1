//go:build !linux && !windows

package backup

import (
	"context"
	"errors"
	"os"

	"mahoroba.local/mahoroba/internal/fssecure"
)

func captureVerifiedDatabase(context.Context, *fssecure.Handle, DatabaseFile) (*os.File, bool, int64, error) {
	return nil, false, 0, errors.New("backup: immutable database snapshots are unsupported")
}
