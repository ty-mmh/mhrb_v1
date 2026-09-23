// Package service owns the production offline boundary for physical blob GC.
package service

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"

	"mahoroba.local/mahoroba/internal/blob"
	"mahoroba.local/mahoroba/internal/blobgc"
	"mahoroba.local/mahoroba/internal/canonical"
	"mahoroba.local/mahoroba/internal/hostlock"
	"mahoroba.local/mahoroba/internal/operationalmetrics"
	store "mahoroba.local/mahoroba/internal/store/sqlite"
)

type Request struct {
	SourceDataDir    string
	DatabaseFilename string
	ResidentID       canonical.ID
	Apply            bool
	Confirm          string
	Failpoint        blobgc.Failpoint
	Observer         interface {
		SetFinalOrphanCount(uint64)
		OperationFinished(operationalmetrics.Operation, operationalmetrics.OperationResult)
	}
}

func Run(ctx context.Context, request Request) (result blobgc.Result, returnErr error) {
	if ctx == nil || request.SourceDataDir == "" || !filepath.IsAbs(request.SourceDataDir) ||
		filepath.Clean(request.SourceDataDir) != request.SourceDataDir || filepath.Base(request.DatabaseFilename) != request.DatabaseFilename ||
		request.DatabaseFilename == "" {
		return result, fmt.Errorf("%w: invalid source configuration", blobgc.ErrSourceUnavailable)
	}
	coreRequest := blobgc.Request{
		ResidentID: request.ResidentID, Apply: request.Apply, Confirm: request.Confirm,
		Failpoint: request.Failpoint, Observer: request.Observer,
	}
	if err := coreRequest.Validate(); err != nil {
		return result, err
	}
	dataLock, err := hostlock.Acquire(request.SourceDataDir)
	if err != nil {
		return result, err
	}
	defer func() { returnErr = joinCloseResult(result, returnErr, dataLock.Close()) }()
	if !request.Apply {
		boundary, err := store.OpenReadOnlyBoundDatabase(ctx, request.SourceDataDir, request.DatabaseFilename)
		if err != nil {
			return result, fmt.Errorf("%w: open protected SQLite inspection: %v", blobgc.ErrSourceUnavailable, err)
		}
		defer func() { returnErr = joinCloseResult(result, returnErr, boundary.Close()) }()
		inspection := boundary.Inspection()
		coreRequest.Boundary = boundary
		objects, err := blob.OpenFileStoreReadOnly(filepath.Join(boundary.DataDir(), "blobs"))
		if err != nil {
			return result, fmt.Errorf("%w: open blob store: %v", blobgc.ErrSourceUnavailable, err)
		}
		if err := inspection.MinimumCheckerWithBlobObjects(objects).Check(ctx); err != nil {
			return result, err
		}
		return blobgc.Execute(ctx, inspection.BlobGC(), objects, coreRequest)
	}
	boundary, err := store.OpenWritableBoundDatabase(request.SourceDataDir, request.DatabaseFilename)
	if err != nil {
		return result, fmt.Errorf("%w: open protected SQLite boundary: %v", blobgc.ErrSourceUnavailable, err)
	}
	defer func() { returnErr = joinCloseResult(result, returnErr, boundary.Close()) }()
	coreRequest.Boundary = boundary
	database, err := store.Open(ctx, filepath.Join(boundary.DataDir(), request.DatabaseFilename))
	if err != nil {
		return result, fmt.Errorf("%w: open SQLite store: %v", blobgc.ErrSourceUnavailable, err)
	}
	defer func() { returnErr = joinCloseResult(result, returnErr, database.Close()) }()
	if err := boundary.Verify(); err != nil {
		return result, fmt.Errorf("%w: verify opened SQLite store: %v", blobgc.ErrSourceUnavailable, err)
	}
	objects, err := blob.OpenFileStoreExisting(filepath.Join(boundary.DataDir(), "blobs"))
	if err != nil {
		return result, fmt.Errorf("%w: open blob store: %v", blobgc.ErrSourceUnavailable, err)
	}
	// This is an offline data command. Holding the source host lock excludes the
	// long-lived Projection coordinator and any cooperating rebuild while the
	// currentness and deletion transactions execute.
	if err := database.MinimumCheckerWithBlobObjects(objects).Check(ctx); err != nil {
		return result, err
	}
	if err := boundary.Verify(); err != nil {
		return result, fmt.Errorf("%w: verify preflight SQLite store: %v", blobgc.ErrSourceUnavailable, err)
	}
	return blobgc.Execute(ctx, database.BlobGC(), objects, coreRequest)
}

func joinCloseResult(result blobgc.Result, operationErr, closeErr error) error {
	if closeErr == nil {
		return operationErr
	}
	if result.PhysicalMutation || result.DeletedCount > 0 {
		return errors.Join(operationErr, &blobgc.PartialError{Cause: closeErr})
	}
	return errors.Join(operationErr, closeErr)
}
