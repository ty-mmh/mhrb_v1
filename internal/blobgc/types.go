// Package blobgc implements the M7 resident-scoped physical blob garbage
// collector. Plans are deterministic internal evidence; command results expose
// only aggregate counts and the plan digest, never candidate locators.
package blobgc

import (
	"context"
	"errors"
	"fmt"

	"mahoroba.local/mahoroba/internal/blob"
	"mahoroba.local/mahoroba/internal/canonical"
	"mahoroba.local/mahoroba/internal/operationalmetrics"
)

const (
	PlanFormatVersion = "mahoroba-blob-gc-plan-v1"
	PlanDigestDomain  = "mahoroba:gc-plan-digest:v1"
	ProjectionName    = "content_references"
	ProjectionVersion = "content-references-v1"
)

var (
	ErrProjectionNotCurrent = errors.New("blob gc: content references Projection is not current")
	ErrContentIntegrity     = errors.New("blob gc: physical blob content integrity failed")
	ErrPlanStale            = errors.New("blob gc: plan is stale")
	ErrPartial              = errors.New("blob gc: physical deletion is partial")
	ErrSourceUnavailable    = errors.New("blob gc: source unavailable")
)

// CapturedHead is the exact common M7 head wire object.
type CapturedHead struct {
	Exists      bool                 `json:"exists"`
	CommitID    *canonical.ID        `json:"commit_id"`
	CommitSeq   *canonical.CommitSeq `json:"commit_seq"`
	CommittedAt *canonical.Instant   `json:"committed_at"`
	CommittedTZ *canonical.Timezone  `json:"committed_tz"`
}

func (head CapturedHead) Validate() error {
	if !head.Exists {
		if head.CommitID != nil || head.CommitSeq != nil || head.CommittedAt != nil || head.CommittedTZ != nil {
			return errors.New("blob gc: empty head contains cursor state")
		}
		return nil
	}
	if head.CommitID == nil || head.CommitSeq == nil || head.CommittedAt == nil || head.CommittedTZ == nil {
		return errors.New("blob gc: existing head is incomplete")
	}
	if err := head.CommitID.Validate(); err != nil {
		return err
	}
	if err := head.CommitSeq.Validate(); err != nil {
		return err
	}
	return head.CommittedTZ.Validate()
}

func (head CapturedHead) Equal(other CapturedHead) bool {
	if head.Validate() != nil || other.Validate() != nil {
		return false
	}
	if head.Exists != other.Exists {
		return false
	}
	if !head.Exists {
		return true
	}
	return *head.CommitID == *other.CommitID && *head.CommitSeq == *other.CommitSeq &&
		*head.CommittedAt == *other.CommittedAt && *head.CommittedTZ == *other.CommittedTZ
}

type DependencyVersion struct {
	ProjectionName    string `json:"projection_name"`
	ProjectionVersion string `json:"projection_version"`
}

// LocatorState is a validated union member captured by one SQLite read
// snapshot. The repository has already hashed a present SQLite copy.
type LocatorState struct {
	ResidentID              canonical.ID
	HashAlgorithm           string
	Digest                  canonical.Digest
	SQLitePresent           bool
	SQLiteByteSize          *canonical.ByteSize
	AuthoritativeReferences int64
	ProjectionReferences    int64
}

type Snapshot struct {
	CapturedHead       CapturedHead
	ProjectionName     string
	ProjectionVersion  string
	DependencyVersions []DependencyVersion
	Locators           []LocatorState
}

// Repository is the narrow SQLite capability used by GC. Capture must hold a
// single read-only snapshot from head/watermark capture through all locator
// and reference counts. BeginCandidate must start an immediate Operational
// transaction and repeat the head/watermark/zero-reference checks.
type Repository interface {
	Capture(context.Context, canonical.ID, []blob.FinalObject) (Snapshot, error)
	BeginCandidate(context.Context, Candidate, CapturedHead) (CandidateTransaction, error)
	Maintenance(context.Context) error
}

type CandidateTransaction interface {
	DeleteSQLite(context.Context) (bool, error)
	Commit(context.Context) error
	Rollback(context.Context) error
}

type FinalStore interface {
	WalkFinal(context.Context, canonical.ID) ([]blob.FinalObject, error)
	RemoveFinal(context.Context, blob.FinalObject) (bool, error)
}

type Failpoint func(string) error

// Boundary is transient protected root/database identity authority. It is
// absent from the plan wire format and verified around every destructive
// candidate transaction.
type Boundary interface {
	Verify() error
}

type Request struct {
	ResidentID canonical.ID
	Apply      bool
	Confirm    string
	Boundary   Boundary
	Failpoint  Failpoint
	Observer   interface {
		SetFinalOrphanCount(uint64)
		OperationFinished(operationalmetrics.Operation, operationalmetrics.OperationResult)
	}
}

type Result struct {
	Plan                  Plan
	CandidateCount        int64
	CandidateBytes        int64
	DeletedCount          int64
	RemainingCount        int64
	PhysicalMutation      bool
	MaintenanceIncomplete bool
}

func (result Result) Validate() error {
	if err := result.Plan.Validate(); err != nil {
		return err
	}
	if result.CandidateCount < 0 || result.CandidateBytes < 0 || result.DeletedCount < 0 || result.RemainingCount < 0 ||
		result.DeletedCount > result.CandidateCount || result.RemainingCount != result.CandidateCount-result.DeletedCount {
		return errors.New("blob gc: invalid result counters")
	}
	return nil
}

type PartialError struct{ Cause error }

func (err *PartialError) Error() string {
	if err == nil || err.Cause == nil {
		return ErrPartial.Error()
	}
	return fmt.Sprintf("%s: %v", ErrPartial, err.Cause)
}
func (err *PartialError) Unwrap() error { return errors.Join(ErrPartial, err.Cause) }
