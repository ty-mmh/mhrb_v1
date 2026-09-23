//go:build linux

package backup

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"strconv"

	"golang.org/x/sys/unix"
	"mahoroba.local/mahoroba/internal/fssecure"
)

// captureVerifiedDatabase copies the source descriptor once into an anonymous
// sealed memfd while computing the manifest digest. Semantic SQLite reads and
// restore streaming then consume the same immutable bytes, so an in-place
// write/restore ABA against the bundle file cannot split verification from
// the copied payload.
func captureVerifiedDatabase(
	ctx context.Context,
	source *fssecure.Handle,
	descriptor DatabaseFile,
) (*os.File, bool, int64, error) {
	if ctx == nil || source == nil {
		return nil, false, 0, errors.New("backup: database snapshot source is required")
	}
	expectedSize, err := strconv.ParseInt(descriptor.ByteSize, 10, 64)
	if err != nil || expectedSize < 0 {
		return nil, false, 0, errors.New("backup: database snapshot size is invalid")
	}
	if err := source.VerifyBound(); err != nil {
		return nil, false, 0, err
	}
	if _, err := source.File().Seek(0, io.SeekStart); err != nil {
		return nil, false, 0, err
	}
	fd, err := unix.MemfdCreate("mahoroba-backup-database", unix.MFD_CLOEXEC|unix.MFD_ALLOW_SEALING)
	if err != nil {
		return nil, false, 0, fmt.Errorf("backup: create sealed database snapshot: %w", err)
	}
	snapshot := os.NewFile(uintptr(fd), "mahoroba-backup-database")
	if snapshot == nil {
		_ = unix.Close(fd)
		return nil, false, 0, errors.New("backup: create sealed database snapshot handle")
	}
	failed := true
	defer func() {
		if failed {
			_ = snapshot.Close()
		}
	}()
	hasher := sha256.New()
	size, err := io.Copy(io.MultiWriter(snapshot, hasher), &bundleContextReader{ctx: ctx, reader: source.File()})
	if err != nil {
		return nil, false, size, err
	}
	if size != expectedSize || "sha256:"+hex.EncodeToString(hasher.Sum(nil)) != descriptor.SHA256 {
		return nil, false, size, errors.New("backup: database snapshot hash or size differs")
	}
	if _, err := snapshot.Seek(0, io.SeekStart); err != nil {
		return nil, false, size, err
	}
	seals := unix.F_SEAL_WRITE | unix.F_SEAL_SHRINK | unix.F_SEAL_GROW | unix.F_SEAL_SEAL
	if _, err := unix.FcntlInt(snapshot.Fd(), unix.F_ADD_SEALS, seals); err != nil {
		return nil, false, size, fmt.Errorf("backup: seal database snapshot: %w", err)
	}
	actualSeals, err := unix.FcntlInt(snapshot.Fd(), unix.F_GET_SEALS, 0)
	if err != nil || actualSeals&seals != seals {
		return nil, false, size, fmt.Errorf("backup: verify database snapshot seals: %w", err)
	}
	if err := source.VerifyBound(); err != nil {
		return nil, false, size, err
	}
	failed = false
	return snapshot, true, size, nil
}
