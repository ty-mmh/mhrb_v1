package domain

import (
	"context"
	"fmt"

	"mahoroba.local/mahoroba/internal/canonical"
)

const (
	SessionPolicyVersion   = "sessionization-v1"
	MaxExplicitEventRefs   = 4
	DialogueLiveEventLimit = 7
	MaxDialogueInputs      = 19
	MaxRecallCandidates    = 64
	MaxRecallSelected      = 8
	MaxRecallPromptInputs  = 4
	MaxRecallUsages        = MaxRecallCandidates + MaxRecallSelected + MaxRecallPromptInputs
)

type Content struct {
	ID             canonical.ID
	ResidentID     canonical.ID
	Class          string
	Bytes          []byte
	BlobHash       canonical.Digest
	Commitment     canonical.Digest
	CommitmentSalt canonical.ContentSalt
	ErasurePolicy  string
}

func (c Content) Validate() error {
	if err := c.ID.Validate(); err != nil {
		return err
	}
	if err := c.ResidentID.Validate(); err != nil {
		return err
	}
	if c.Class == "" || (c.ErasurePolicy != "independent" && c.ErasurePolicy != "resident_only") {
		return fmt.Errorf("domain: invalid content metadata")
	}
	if got := canonical.HashBlob(c.Bytes); got != c.BlobHash {
		return fmt.Errorf("domain: content blob hash mismatch")
	}
	commitment, err := canonical.CommitContent(c.Class, c.CommitmentSalt, c.Bytes)
	if err != nil || commitment != c.Commitment {
		return fmt.Errorf("domain: content commitment mismatch")
	}
	return nil
}

type GlobalBootstrap struct {
	SystemPrincipalID  canonical.ID
	OwnerPrincipalID   canonical.ID
	OwnerDisplayName   string
	PipelineVersionID  canonical.ID
	PipelineDefinition canonical.CanonicalJSON
	SessionPolicyID    canonical.ID
	SessionDefinition  canonical.CanonicalJSON
}

type DraftResident struct {
	ResidentID           canonical.ID
	ResidentPrincipalID  canonical.ID
	OwnerPrincipalID     canonical.ID
	Name                 string
	SeedKey              string
	PrinciplesRevisionID canonical.ID
	PrinciplesContent    Content
	StatusTransitionID   canonical.ID
}

type ApprovePrinciples struct {
	ResidentID       canonical.ID
	RevisionID       canonical.ID
	OwnerPrincipalID canonical.ID
	ApprovalID       canonical.ID
	ActivationID     canonical.ID
}

type FinalizeResident struct {
	ResidentID          canonical.ID
	OwnerPrincipalID    canonical.ID
	PersonaRevisionID   canonical.ID
	PersonaActivationID canonical.ID
	PersonaContent      Content
	MemoryRevisionID    canonical.ID
	MemoryActivationID  canonical.ID
	MemoryContent       Content
	StatusTransitionID  canonical.ID
}

type ArchiveResident struct {
	ResidentID         canonical.ID
	OwnerPrincipalID   canonical.ID
	StatusTransitionID canonical.ID
}

type Event struct {
	ID                canonical.ID
	ResidentID        canonical.ID
	Seq               canonical.Seq
	Type              string
	ActorPrincipalID  canonical.ID
	TargetPrincipalID *canonical.ID
	GenerationRunID   *canonical.ID
	OccurredAt        canonical.Instant
	OccurredTZ        canonical.Timezone
	RecordedAt        canonical.Instant
	RecordedTZ        canonical.Timezone
	ContentID         canonical.ID
	Content           string
	ContentErased     bool
	Commitment        canonical.Digest
	PrevHash          *canonical.Digest
	Hash              canonical.Digest
}

type IngressUserMessage struct {
	EventID             canonical.ID
	ResidentID          canonical.ID
	OwnerPrincipalID    canonical.ID
	ResidentPrincipalID canonical.ID
	Content             Content
	OccurredAt          canonical.Instant
	OccurredTZ          canonical.Timezone
}

// IngressRequest is the complete local-UI request. ExplicitEventIDs is
// metadata, not syntax embedded in RawText; RawText is persisted byte-for-byte.
type IngressRequest struct {
	RawText          string
	ExplicitEventIDs []canonical.ID
}

type GenerationInput struct {
	ID            canonical.ID
	Ordinal       int64
	Role          string
	SourceType    string
	SourceID      *canonical.ID
	InclusionMode string
	Content       Content
}

type PrepareGeneration struct {
	RunID                  canonical.ID
	ResidentID             canonical.ID
	Purpose                GenerationPurpose
	IdempotencyKey         string
	Provider               string
	Model                  string
	PromptTemplateVersion  string
	ContextPolicyVersion   string
	MemoryRenderingVersion string
	PipelineVersionID      canonical.ID
	SessionPolicyID        *canonical.ID
	PrinciplesRevisionID   canonical.ID
	PersonaRevisionID      canonical.ID
	MemoryPolicyRevisionID canonical.ID
	RecallRunID            *canonical.ID
	AsOf                   canonical.Instant
	AsOfTZ                 canonical.Timezone
	BudgetExceeded         bool
	DroppedInputSummary    canonical.CanonicalJSON
	GeneratorParams        canonical.CanonicalJSON
	Inputs                 []GenerationInput
	RunningOutcomeID       canonical.ID
}

// CancelDialogue records a terminal decision for a dialogue obligation that
// cannot execute because its source was erased or its resident is inactive.
// Generation contains the immutable envelope used only when no run exists;
// its RunningOutcomeID is also the next-attempt running ID when an existing
// retry_pending run must be terminalized without calling a provider.
type CancelDialogue struct {
	Generation         PrepareGeneration
	SourceEventID      canonical.ID
	CancelledOutcomeID canonical.ID
	ErrorClass         string
	// ExpectedRunID and ExpectedAttemptNo bind a provider-landing cancellation
	// to the exact running attempt whose sources were revalidated. They are
	// transient Writer guards and are deliberately not persisted. Both must be
	// supplied together, and only source_content_erased cancellation uses them.
	ExpectedRunID     *canonical.ID
	ExpectedAttemptNo *int64
}

type DialogueCancellationResult struct {
	RunID     canonical.ID
	AttemptNo int64
}

type Attempt struct {
	RunID      canonical.ID
	ResidentID canonical.ID
	AttemptNo  int64
	OutcomeID  canonical.ID
	// MaxAttempts is the current runtime retry policy supplied to Writer
	// revalidation. It is required for autonomous starts and landings and is
	// deliberately not part of the persisted generation envelope.
	MaxAttempts int
}

type FailAttempt struct {
	Attempt
	State      string
	ErrorClass string
}

// RejectGenerationEnvelope terminalizes an executable obligation when the
// persisted provider/configuration envelope cannot be served safely. The UoW
// performs a retry transition and rejection atomically when the latest attempt
// is already retry-pending.
type RejectGenerationEnvelope struct {
	RunID             canonical.ID
	ResidentID        canonical.ID
	RunningOutcomeID  canonical.ID
	RejectedOutcomeID canonical.ID
	ErrorClass        string
}

type GenerationRejectionResult struct {
	AttemptNo int64
}

type LandDialogue struct {
	Attempt
	EventID             canonical.ID
	ResidentID          canonical.ID
	ResidentPrincipalID canonical.ID
	OwnerPrincipalID    canonical.ID
	Output              Content
	OccurredAt          canonical.Instant
	OccurredTZ          canonical.Timezone
	PromptTokens        *int64
	CompletionTokens    *int64
	LatencyMicros       int64
}

type ResidentSnapshot struct {
	ResidentID             canonical.ID
	ResidentPrincipalID    canonical.ID
	OwnerPrincipalID       canonical.ID
	Name                   string
	SeedKey                string
	Status                 string
	PrinciplesRevisionID   canonical.ID
	Principles             string
	PersonaRevisionID      canonical.ID
	Persona                string
	MemoryPolicyRevisionID canonical.ID
	MemoryPolicy           string
	PipelineVersionID      canonical.ID
	SessionPolicyID        canonical.ID
	SessionIdleGap         canonical.Duration
}

type BootstrapState struct {
	Initialized       bool
	SystemPrincipalID canonical.ID
	OwnerPrincipalID  canonical.ID
	OwnerDisplayName  string
	PipelineVersionID canonical.ID
	SessionPolicyID   canonical.ID
	Residents         []ResidentSnapshot
}

type WorkState string

const (
	WorkPending        WorkState = "pending"
	WorkRunning        WorkState = "running"
	WorkRetryPending   WorkState = "retry_pending"
	WorkSucceeded      WorkState = "succeeded"
	WorkTerminalFailed WorkState = "terminal_failed"
)

type DialogueWork struct {
	UserEvent        Event
	RunID            *canonical.ID
	AttemptNo        int64
	State            WorkState
	CancellationCode string
}

type RunningAttempt struct {
	RunID      canonical.ID
	ResidentID canonical.ID
	AttemptNo  int64
}

// PreparedGeneration is the durable provider request reconstructed from the
// canonical generation envelope. A retry uses its exact provider identity,
// model, parameter bytes, and ordered inputs instead of current defaults.
type PreparedGeneration struct {
	RunID                    canonical.ID
	ResidentID               canonical.ID
	Purpose                  GenerationPurpose
	IdempotencyKey           string
	Provider                 string
	Model                    string
	PromptTemplateVersion    string
	ContextPolicyVersion     string
	MemoryRenderingVersion   string
	PipelineVersionID        canonical.ID
	PipelineVersionKey       string
	SessionPolicyID          *canonical.ID
	PrinciplesRevisionID     canonical.ID
	PersonaRevisionID        canonical.ID
	MemoryPolicyRevisionID   canonical.ID
	RecallRunID              *canonical.ID
	GeneratorParams          GeneratorParams
	CanonicalGeneratorParams canonical.CanonicalJSON
	AsOf                     canonical.Instant
	AsOfTZ                   canonical.Timezone
	AttemptNo                int64
	State                    WorkState
	Inputs                   []GenerationInput
}

type MutationStore interface {
	InsertGlobalBootstrap(context.Context, GlobalBootstrap) error
	InsertDraftResident(context.Context, DraftResident) error
	ApproveAndActivatePrinciples(context.Context, ApprovePrinciples) error
	FinalizeResident(context.Context, FinalizeResident) error
	ArchiveResident(context.Context, ArchiveResident) error
	IngressUserMessage(context.Context, IngressUserMessage) (Event, error)
	PrepareDialogue(context.Context, PrepareDialogue) (PrepareDialogueResult, error)
	PrepareGeneration(context.Context, PrepareGeneration) error
	CancelDialogue(context.Context, CancelDialogue) (DialogueCancellationResult, error)
	StartAttempt(context.Context, Attempt) error
	FailAttempt(context.Context, FailAttempt) error
	RejectGenerationEnvelope(context.Context, RejectGenerationEnvelope) (GenerationRejectionResult, error)
	LandDialogue(context.Context, LandDialogue) (Event, error)
}

type Repository interface {
	BootstrapSnapshot(context.Context) (BootstrapState, error)
	ListResidents(context.Context) ([]ResidentSnapshot, error)
	Resident(context.Context, canonical.ID) (ResidentSnapshot, error)
	SelectActiveResident(context.Context, canonical.ID, canonical.Instant, canonical.Timezone) error
	ActiveResident(context.Context) (ResidentSnapshot, error)
	History(context.Context, canonical.ID, int) ([]Event, error)
	Event(context.Context, canonical.ID, canonical.ID) (Event, error)
	DiscoverDialogueWork(context.Context, canonical.ID, DialogueDiscoveryRequest) (DialogueDiscoveryResult, error)
	RunningAttempts(context.Context, int) ([]RunningAttempt, error)
	Generation(context.Context, canonical.ID) (PreparedGeneration, error)
}

func DialogueObligation(eventID canonical.ID) string {
	return "dialogue:v1:" + eventID.String()
}
