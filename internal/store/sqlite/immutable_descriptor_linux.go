//go:build linux

package sqlite

import (
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"net/url"
	"os"
	"strconv"
	"sync"

	"golang.org/x/sys/unix"
	"modernc.org/sqlite/vfs"
)

const immutableDescriptorFilename = "mahoroba-sealed-database.sqlite"

const requiredImmutableDescriptorSeals = unix.F_SEAL_WRITE | unix.F_SEAL_SHRINK | unix.F_SEAL_GROW | unix.F_SEAL_SEAL

// OpenImmutableDescriptorInspection opens a closed SQLite snapshot through a
// read-only VFS whose only file is the retained, sealed Linux descriptor.
// fallbackPath is deliberately ignored on Linux: inspection never reopens a
// mutable namespace name. The caller owns descriptor and must retain it until
// the returned inspection is closed.
func OpenImmutableDescriptorInspection(ctx context.Context, descriptor *os.File, fallbackPath string) (*Inspection, error) {
	if ctx == nil {
		return nil, errors.New("sqlite: nil immutable descriptor inspection context")
	}
	filesystem, err := newImmutableDescriptorFS(descriptor)
	if err != nil {
		return nil, err
	}
	vfsName, registered, err := vfs.New(filesystem)
	if err != nil {
		return nil, fmt.Errorf("sqlite: register immutable descriptor VFS: %w", err)
	}
	options, err := normalizeOptions(DefaultOptions())
	if err != nil {
		_ = registered.Close()
		return nil, err
	}
	query := make(url.Values)
	query.Set("mode", "ro")
	query.Add("_pragma", "foreign_keys(1)")
	query.Add("_pragma", "trusted_schema(OFF)")
	query.Add("_pragma", "query_only(1)")
	query.Add("_pragma", "busy_timeout("+strconv.FormatInt(options.BusyTimeout.Milliseconds(), 10)+")")
	query.Set("immutable", "1")
	query.Set("vfs", vfsName)
	dsn := "file:" + immutableDescriptorFilename + "?" + query.Encode()
	reader, err := openReaderDSN(ctx, dsn, options)
	if err != nil {
		return nil, errors.Join(err, registered.Close())
	}
	return validatedInspection(ctx, "linux:sealed-descriptor", reader, registered.Close)
}

type immutableDescriptorFS struct {
	descriptor *os.File
	identity   os.FileInfo
}

func newImmutableDescriptorFS(descriptor *os.File) (*immutableDescriptorFS, error) {
	if descriptor == nil {
		return nil, errors.New("sqlite: immutable descriptor is required")
	}
	identity, err := descriptor.Stat()
	if err != nil {
		return nil, fmt.Errorf("sqlite: stat immutable descriptor: %w", err)
	}
	if !identity.Mode().IsRegular() {
		return nil, errors.New("sqlite: immutable descriptor must be a regular file")
	}
	if err := verifyImmutableDescriptor(descriptor, identity); err != nil {
		return nil, err
	}
	return &immutableDescriptorFS{descriptor: descriptor, identity: identity}, nil
}

func (filesystem *immutableDescriptorFS) Open(name string) (fs.File, error) {
	if filesystem == nil || filesystem.descriptor == nil || name != immutableDescriptorFilename {
		return nil, fs.ErrNotExist
	}
	if err := verifyImmutableDescriptor(filesystem.descriptor, filesystem.identity); err != nil {
		return nil, err
	}
	duplicated, err := unix.FcntlInt(filesystem.descriptor.Fd(), unix.F_DUPFD_CLOEXEC, 0)
	if err != nil {
		return nil, fmt.Errorf("sqlite: duplicate immutable descriptor: %w", err)
	}
	file := os.NewFile(uintptr(duplicated), immutableDescriptorFilename)
	if file == nil {
		_ = unix.Close(duplicated)
		return nil, errors.New("sqlite: construct duplicated immutable descriptor")
	}
	if err := verifyImmutableDescriptor(file, filesystem.identity); err != nil {
		_ = file.Close()
		return nil, err
	}
	return &immutableDescriptorFile{file: file, size: filesystem.identity.Size()}, nil
}

func verifyImmutableDescriptor(descriptor *os.File, identity os.FileInfo) error {
	current, err := descriptor.Stat()
	if err != nil {
		return fmt.Errorf("sqlite: stat retained immutable descriptor: %w", err)
	}
	if !current.Mode().IsRegular() || !os.SameFile(identity, current) || current.Size() != identity.Size() {
		return errors.New("sqlite: retained immutable descriptor identity or size differs")
	}
	seals, err := unix.FcntlInt(descriptor.Fd(), unix.F_GET_SEALS, 0)
	if err != nil {
		return fmt.Errorf("sqlite: inspect immutable descriptor seals: %w", err)
	}
	if seals&requiredImmutableDescriptorSeals != requiredImmutableDescriptorSeals {
		return errors.New("sqlite: immutable descriptor is not completely sealed")
	}
	return nil
}

// immutableDescriptorFile gives each VFS open an independent logical offset.
// dup(2) shares the kernel open-file-description offset, so Read uses pread via
// os.File.ReadAt instead of the duplicated descriptor's shared offset.
type immutableDescriptorFile struct {
	file   *os.File
	size   int64
	mu     sync.Mutex
	offset int64
}

func (file *immutableDescriptorFile) Read(value []byte) (int, error) {
	file.mu.Lock()
	defer file.mu.Unlock()
	if file.file == nil {
		return 0, fs.ErrClosed
	}
	count, err := file.file.ReadAt(value, file.offset)
	file.offset += int64(count)
	return count, err
}

func (file *immutableDescriptorFile) Seek(offset int64, whence int) (int64, error) {
	file.mu.Lock()
	defer file.mu.Unlock()
	var next int64
	switch whence {
	case io.SeekStart:
		next = offset
	case io.SeekCurrent:
		next = file.offset + offset
	case io.SeekEnd:
		next = file.size + offset
	default:
		return 0, errors.New("sqlite: invalid immutable descriptor seek origin")
	}
	if next < 0 {
		return 0, errors.New("sqlite: negative immutable descriptor seek")
	}
	file.offset = next
	return next, nil
}

func (file *immutableDescriptorFile) Stat() (os.FileInfo, error) {
	file.mu.Lock()
	defer file.mu.Unlock()
	if file.file == nil {
		return nil, fs.ErrClosed
	}
	return file.file.Stat()
}

func (file *immutableDescriptorFile) Close() error {
	file.mu.Lock()
	defer file.mu.Unlock()
	if file.file == nil {
		return nil
	}
	err := file.file.Close()
	file.file = nil
	return err
}
