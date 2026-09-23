package integrity

import (
	"bytes"
	"context"
	"errors"
	"fmt"

	"mahoroba.local/mahoroba/internal/canonical"
)

const (
	// PresentBlobCopyInvalid identifies a disagreement between the Canonical
	// SQLite blob and the independently published filesystem object.
	PresentBlobCopyInvalid FatalCode = "present_blob_copy_invalid"
)

// PresentBlobCopy is the database side of the dual-copy contract. The raw
// bytes never appear in a diagnostic or FatalError.
type PresentBlobCopy struct {
	ContentID    string
	ResidentID   canonical.ID
	ExpectedHash canonical.Digest
	DatabaseBlob []byte
	DeclaredSize int64
	BlobFound    bool
}

// PresentBlobSource enumerates every present content object, including rows
// whose SQLite blob is missing. Implementations must use a stable read view.
type PresentBlobSource interface {
	LoadPresentBlobCopies(context.Context) ([]PresentBlobCopy, error)
}

// BlobObjectReader is the narrow, handle-bound filesystem authority required
// by MinimumCheck.
type BlobObjectReader interface {
	Read(context.Context, canonical.ID, canonical.Digest) ([]byte, error)
}

func checkPresentBlobCopies(ctx context.Context, source PresentBlobSource, objects BlobObjectReader) error {
	copies, err := source.LoadPresentBlobCopies(ctx)
	if err != nil {
		return fmt.Errorf("integrity: load present blob copies: %w", err)
	}
	for _, copy := range copies {
		if err := ctx.Err(); err != nil {
			return err
		}
		if copy.ContentID == "" || copy.ResidentID.IsZero() {
			return blobFatal(copy.ContentID, "database identity is invalid")
		}
		if !copy.BlobFound || copy.DeclaredSize < 0 || int64(len(copy.DatabaseBlob)) != copy.DeclaredSize ||
			canonical.HashBlob(copy.DatabaseBlob) != copy.ExpectedHash {
			return blobFatal(copy.ContentID, "database blob is missing or does not match its declared identity")
		}
		filesystemBlob, readErr := objects.Read(ctx, copy.ResidentID, copy.ExpectedHash)
		if readErr != nil {
			// Preserve cancellation for callers, but collapse filesystem details so
			// paths and blob identities cannot leak through public diagnostics.
			if errors.Is(readErr, context.Canceled) || errors.Is(readErr, context.DeadlineExceeded) {
				return readErr
			}
			return blobFatal(copy.ContentID, "filesystem blob is missing or unreadable")
		}
		if !bytes.Equal(filesystemBlob, copy.DatabaseBlob) {
			return blobFatal(copy.ContentID, "database and filesystem blob bytes differ")
		}
	}
	return nil
}

func blobFatal(contentID, reason string) error {
	return &FatalError{Code: PresentBlobCopyInvalid, TargetKind: "content", TargetID: contentID, Reason: reason}
}
