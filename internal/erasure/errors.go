// Package erasure owns the closed M7 destructive-erasure plan, decision, and
// Canonical apply contracts. Public Apply remains a host-level policy gate;
// this package deliberately exposes no CLI or network surface.
package erasure

import "errors"

var (
	ErrInvalidPlan               = errors.New("erasure: invalid plan")
	ErrPlanDigestMismatch        = errors.New("erasure: plan digest mismatch")
	ErrPlanStale                 = errors.New("erasure: plan stale")
	ErrRetryConflict             = errors.New("erasure: retry conflict")
	ErrReviewRequired            = errors.New("erasure: review required")
	ErrBlocked                   = errors.New("erasure: blocked")
	ErrOwnerHumanRequired        = errors.New("erasure: owner human required")
	ErrIntegrityPipelineRequired = errors.New("erasure: exact integrity pipelines required")
	ErrSafetyBusy                = errors.New("erasure: resident safety busy")
	ErrDesignReopen              = errors.New("erasure: static design reopen required")
)
