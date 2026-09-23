package durablepublish

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"slices"
	"strconv"
	"strings"

	"mahoroba.local/mahoroba/internal/canonical"
	"mahoroba.local/mahoroba/internal/fssecure"
)

const maxDigestOpenHandles = 16

type PayloadSummary struct {
	SHA256    canonical.Digest
	FileCount int64
	ByteCount int64
}

type inventoryEntry struct {
	Path     string `json:"path"`
	ByteSize string `json:"byte_size"`
	SHA256   string `json:"sha256"`
}

// DirectoryPayloadDigest hashes a sorted inventory and excludes only the
// exact registered reserved marker basenames. Traversal retains at most 16
// no-follow directory/file handles.
func DirectoryPayloadDigest(
	ctx context.Context,
	root *fssecure.Directory,
	reservedMarkers ...string,
) (PayloadSummary, error) {
	var result PayloadSummary
	if ctx == nil || root == nil {
		return result, errors.New("durable publish: nil directory digest input")
	}
	reserved := map[string]struct{}{MarkerName: {}}
	for _, name := range reservedMarkers {
		if !validBasename(name) {
			return result, fmt.Errorf("durable publish: invalid reserved marker %q", name)
		}
		reserved[name] = struct{}{}
	}
	var entries []inventoryEntry
	var walk func(*fssecure.Directory, string, int) error
	walk = func(directory *fssecure.Directory, prefix string, depth int) error {
		if depth >= maxDigestOpenHandles-1 {
			return errors.New("durable publish: directory nesting exceeds handle bound")
		}
		children, err := directory.ReadDir()
		if err != nil {
			return err
		}
		slices.SortFunc(children, func(left, right os.DirEntry) int {
			return strings.Compare(left.Name(), right.Name())
		})
		for _, child := range children {
			if err := ctx.Err(); err != nil {
				return err
			}
			name := child.Name()
			if _, excluded := reserved[name]; excluded && prefix == "" {
				continue
			}
			relative := name
			if prefix != "" {
				relative = prefix + "/" + name
			}
			if child.IsDir() {
				opened, err := directory.OpenDirectory(name)
				if err != nil {
					return err
				}
				walkErr := walk(opened, relative, depth+1)
				closeErr := opened.Close()
				if walkErr != nil || closeErr != nil {
					return errors.Join(walkErr, closeErr)
				}
				continue
			}
			if child.Type()&os.ModeType != 0 {
				return fmt.Errorf("durable publish: payload %q is not regular", relative)
			}
			handle, err := directory.OpenRegularRead(name)
			if err != nil {
				return err
			}
			digest, size, hashErr := handle.Hash(ctx)
			closeErr := handle.Close()
			if hashErr != nil || closeErr != nil {
				return errors.Join(hashErr, closeErr)
			}
			if size < 0 || result.ByteCount > int64(^uint64(0)>>1)-size {
				return errors.New("durable publish: payload byte count overflows")
			}
			result.ByteCount += size
			result.FileCount++
			entries = append(entries, inventoryEntry{
				Path: relative, ByteSize: strconv.FormatInt(size, 10),
				SHA256: "sha256:" + hex.EncodeToString(digest[:]),
			})
		}
		return nil
	}
	if err := walk(root, "", 0); err != nil {
		return PayloadSummary{}, err
	}
	slices.SortFunc(entries, func(left, right inventoryEntry) int {
		return strings.Compare(left.Path, right.Path)
	})
	inventory, err := canonical.MarshalCanonical(entries)
	if err != nil {
		return PayloadSummary{}, err
	}
	material := make([]byte, 0, len(directoryDigestDomain)+1+len(inventory.Bytes()))
	material = append(material, directoryDigestDomain...)
	material = append(material, 0)
	material = append(material, inventory.Bytes()...)
	result.SHA256 = canonical.HashBlob(material)
	return result, nil
}

func SingleFilePayloadDigest(ctx context.Context, handle *fssecure.Handle) (PayloadSummary, error) {
	if ctx == nil || handle == nil {
		return PayloadSummary{}, errors.New("durable publish: nil single-file digest input")
	}
	digest, size, err := handle.Hash(ctx)
	if err != nil {
		return PayloadSummary{}, err
	}
	parsed, err := canonical.DigestFromBytes(digest[:])
	if err != nil {
		return PayloadSummary{}, err
	}
	return PayloadSummary{SHA256: parsed, FileCount: 1, ByteCount: size}, nil
}
