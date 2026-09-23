// Package restore implements the offline M7 directory-bundle restore gate.
// It never starts providers, workers, listeners, or a source-bundle writer.
package restore

import (
	"errors"
	"time"

	"mahoroba.local/mahoroba/internal/backup"
	"mahoroba.local/mahoroba/internal/canonical"
	"mahoroba.local/mahoroba/internal/durablepublish"
	"mahoroba.local/mahoroba/internal/operationalmetrics"
	"mahoroba.local/mahoroba/internal/readiness"
)

const (
	DatabaseFilename       = "mahoroba.db"
	RestoreStagingMarker   = "RESTORE_STAGING"
	PublishPendingMarker   = durablepublish.MarkerName
	RestoreStagingFormat   = "mahoroba-restore-staging-v1"
	RestoreProducerCommand = durablepublish.CommandBackupRestore
)

var (
	ErrTargetExists      = errors.New("restore: target exists")
	ErrUnsafeOverlap     = errors.New("restore: bundle and target overlap")
	ErrNamespaceBusy     = errors.New("restore: target namespace is busy")
	ErrInvalidBundle     = errors.New("restore: invalid backup bundle")
	ErrStagingFailed     = errors.New("restore: staged restore failed")
	ErrDurabilityUnknown = errors.New("restore: publication durability is unknown")
	ErrArtifactIO        = errors.New("restore: artifact I/O failed")
)

type Request struct {
	BundleRoot    string
	TargetDataDir string
	Observer      Observer
}

// Observer is the closed, bounded metrics surface shared by every private
// component used during restore. No raw content, path, identifier, or error
// value can enter the process-local observer.
type Observer interface {
	WriterAccepted()
	WriterQueued()
	WriterInFlight()
	WriterFinished(operationalmetrics.WriterOutcome, time.Duration)
	ProviderFinished(operationalmetrics.ProviderTerminal, time.Duration)
	ProjectionObserved(commitLag uint64, rebuildDuration time.Duration, rebuilt, failed bool)
	SetMandatoryWorkCounts(operationalmetrics.MandatoryWorkCounts)
	MandatoryWorkerFailed(operationalmetrics.MandatoryWorkerFailure)
	SetStagingOrphanCount(uint64)
	SetFinalOrphanCount(uint64)
	OperationFinished(operationalmetrics.Operation, operationalmetrics.OperationResult)
}

type CommitDisposition string

const (
	CommitCreated  CommitDisposition = "created"
	CommitExisting CommitDisposition = "existing"
)

// CommitEffects is the restore-owned closed projection of effects created or
// confirmed inside the staged database. It cannot carry free-form summaries.
type CommitEffects struct {
	GenerationAttemptTerminalized bool
	MandatoryWorkCancelled        bool
	PipelineVersionRegistered     bool
	IntegrityFindingRecorded      bool
	ClaimStatusQuarantined        bool
}

type Commit struct {
	Metadata    canonical.CommitMetadata
	Disposition CommitDisposition
	Effects     CommitEffects
}

// Result keeps staged Canonical effects private until directory publication
// is durable. On every pre-publish failure Published and CanonicalApplied are
// false and CanonicalCommits is empty even when the abandoned staging DB
// contains recovery commits.
type Result struct {
	FormatVersion            string
	DatabaseFilename         string
	Bundle                   backup.VerifiedBundle
	SourceHead               readiness.Head
	RestoredHead             readiness.Head
	TerminalizedAttempts     int
	CancelledMandatoryWork   int
	CreatedIntegrityFindings int
	CreatedQuarantines       int
	ProjectionsRebuilt       int
	FileCount                int64
	ByteCount                int64
	Published                bool
	CanonicalApplied         bool
	CanonicalCommits         []Commit
	ServiceReady             bool
	ReadinessReasons         []readiness.ReasonCode
	RestoreID                canonical.ID
	StagingBasename          string
}
