// Package app coordinates canonical commands, provider calls, recovery, and
// the read-only surface consumed by the localhost UI and Admin CLI.
package app

import (
	"bytes"
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"sync"
	"time"
	"unicode/utf8"

	"mahoroba.local/mahoroba/internal/autonomy"
	"mahoroba.local/mahoroba/internal/blob"
	"mahoroba.local/mahoroba/internal/canonical"
	"mahoroba.local/mahoroba/internal/domain"
	"mahoroba.local/mahoroba/internal/generation"
	"mahoroba.local/mahoroba/internal/operationalmetrics"
)

type Options struct {
	Writer                 *canonical.Writer
	CommitNotifier         CommitNotifier
	Repository             domain.Repository
	IDs                    *canonical.IDGenerator
	Clock                  canonical.Clock
	Timezone               canonical.Timezone
	Blobs                  blob.Store
	Generator              generation.Generator
	Provider               string
	Model                  string
	StructuredOutputMode   generation.StructuredOutputMode
	MaxAttempts            int
	RetryBackoff           []time.Duration
	MaxInputBytes          int
	MaxOutputBytes         int
	SafetyScanInterval     time.Duration
	AutonomyPolicy         *autonomy.Policy
	AutonomySource         autonomy.Source
	AutonomyClock          autonomy.SchedulerClock
	ProjectionMaxStaleness time.Duration
	ErasureCandidateSink   ErasureCandidateSink
	OperationalObserver    OperationalObserver
}

// OperationalObserver is the closed metrics boundary shared by provider
// dispatch and mandatory-work recovery. It accepts only bounded enums,
// durations, and counts; raw prompts, content, paths, and errors cannot cross
// this interface.
type OperationalObserver interface {
	ProviderFinished(operationalmetrics.ProviderTerminal, time.Duration)
	SetMandatoryWorkCounts(operationalmetrics.MandatoryWorkCounts)
	MandatoryWorkerFailed(operationalmetrics.MandatoryWorkerFailure)
}

// CommitNotifier receives best-effort hints after a Canonical commit is
// durable. Implementations must be non-blocking; correctness comes from their
// independent reconciliation scan rather than delivery of every hint.
type CommitNotifier interface {
	NotifyCommit(canonical.CommitMetadata) bool
}

type Observer interface {
	UserCommitted(domain.Event)
	GenerationStarted(residentID, runID canonical.ID, attempt int64)
	GenerationDelta(residentID, runID canonical.ID, text string)
	ResidentCommitted(domain.Event)
	GenerationFailed(residentID, runID canonical.ID, attempt int64, class string, terminal bool)
}

// dialogueScanCycle is process-local progress for one bounded historical
// dialogue scan. Epoch is captured when the cycle starts and remains fixed
// until that cursor reaches the end of history. Keeping the original epoch is
// essential: resetting the cursor on every foreground ingress could starve old
// obligations under a continuous dialogue backlog.
type dialogueScanCycle struct {
	Epoch  uint64
	Cursor *domain.DialogueDiscoveryCursor
}

type Application struct {
	writer               *canonical.Writer
	commitNotifier       CommitNotifier
	dialogueReconciler   dialogueProjectionReconciler
	repository           domain.Repository
	ids                  *canonical.IDGenerator
	clock                canonical.Clock
	timezone             canonical.Timezone
	blobs                blob.Store
	generator            generation.Generator
	provider             string
	model                string
	structuredOutputMode generation.StructuredOutputMode

	maxAttempts            int
	retryBackoff           []time.Duration
	maxInputBytes          int
	maxOutputBytes         int
	safetyScanInterval     time.Duration
	autonomyPolicy         *autonomy.Policy
	autonomySource         autonomy.Source
	autonomyClock          autonomy.SchedulerClock
	projectionMaxStaleness time.Duration
	autonomyHints          chan canonical.ID
	autonomyAnchorMu       sync.Mutex
	autonomyAnchors        map[canonical.ID]autonomy.RuntimeAnchors
	erasureCandidateSink   ErasureCandidateSink
	operationalObserver    OperationalObserver

	observerMu sync.RWMutex
	observer   Observer
	// residentBoundaryMu serializes durable resident selection and archive
	// boundaries. Each boundary closes background admission and drains only the
	// narrow provider-landing locks before its Canonical mutation can commit.
	residentBoundaryMu  sync.Mutex
	workMu              sync.Mutex
	work                map[canonical.ID]*sync.Mutex
	backgroundLandingMu sync.Mutex
	backgroundLandings  map[canonical.ID]*sync.Mutex
	backgroundMu        sync.Mutex
	background          map[canonical.ID]*backgroundCall
	// backgroundBlocked is a process-local hard generation fence used while a
	// resident selection or archive is draining provider landings and committing.
	// It closes the interval in which a new fair or optional provider could
	// otherwise register after the old lease was cancelled but before commit.
	backgroundBlocked map[canonical.ID]bool
	// backgroundSelectionBlocked closes admission for every resident when the
	// former selection cannot be resolved before a selection boundary. It is
	// cleared only after the durable selected resident is known.
	backgroundSelectionBlocked bool
	// backgroundRegistrationHook is a deterministic test seam for the tiny
	// interval between epoch capture and atomic registration. Production leaves
	// it nil.
	backgroundRegistrationHook func()
	// backgroundLandingHook is a deterministic test seam immediately before a
	// provider-success landing enters its narrow resident lock. Production
	// leaves it nil.
	backgroundLandingHook func()
	// foregroundEpoch advances after every durable user ingress. Background
	// work captures the resident epoch before its final foreground check and
	// atomically compares it while registering the provider call, closing the
	// otherwise lossy gap between Canonical foreground work and cancellation.
	foregroundEpoch map[canonical.ID]uint64
	// dialogueCleanEpoch is present only after a complete historical scan found
	// no unfinished dialogue while foregroundEpoch remained unchanged. Keeping
	// the proof under backgroundMu makes proof validation and provider
	// registration one atomic admission decision.
	dialogueCleanEpoch map[canonical.ID]uint64
	// dialogueScanCycles are Operational cursors. Losing them on restart safely
	// repeats immutable history; a restarted process has no clean proof until a
	// fresh cycle reaches the end.
	dialogueStateMu    sync.RWMutex
	dialogueScanCycles map[canonical.ID]dialogueScanCycle
	// memoryDiscoveryCursors continue bounded oldest-first fair-memory cycles
	// across dialogue turns. Normal mandatory extraction and Admin-requested
	// re-extraction use separate cursors: progress in one queue must never move
	// another queue past durable work. All cursor and completeness state is
	// Operational and may be lost safely; restart repeats immutable-history
	// classification instead of skipping an obligation.
	memoryStateMu                 sync.Mutex
	memoryDiscoveryCursors        map[canonical.ID]*domain.MemoryDiscoveryCursor
	normalMemoryDiscoveryCursors  map[canonical.ID]*domain.MemoryDiscoveryCursor
	memoryNormalCycleEpochs       map[canonical.ID]uint64
	memoryReextractionCursors     map[canonical.ID]*domain.MemoryReextractionDiscoveryCursor
	memoryReextractionCycles      map[canonical.ID]bool
	memoryReextractionCycleEpochs map[canonical.ID]uint64
	// memoryReextractionRescanPending records an Admin re-extraction admitted
	// after a fixed re-extraction ceiling was captured. The current finite cycle
	// may finish, but it cannot publish optional-work completeness until a fresh
	// normal/re-extraction verification includes the new durable request.
	memoryReextractionRescanPending map[canonical.ID]bool
	memoryNormalCyclesDirty         map[canonical.ID]bool
	memoryReextractionCyclesDirty   map[canonical.ID]bool
	memoryNormalCompleteEpochs      map[canonical.ID]uint64
	memoryExtractionScansComplete   map[canonical.ID]bool

	lifecycleMu    sync.Mutex
	runCtx         context.Context
	runCancel      context.CancelFunc
	started        bool
	workersStarted bool
	activationGate WorkerStartGate
	stopping       bool
	workers        sync.WaitGroup
	workersDone    chan struct{}
}

var (
	ErrApplicationStopping           = errors.New("app: application is stopping")
	ErrApplicationNotActivated       = errors.New("app: runtime activation gate is closed")
	ErrApplicationNotStopping        = errors.New("app: application has not been stopped")
	ErrApplicationWorkersLive        = errors.New("app: application workers are still live")
	ErrGenerationEnvelopeUnsupported = errors.New("app: recorded generation envelope is unsupported")
	ErrGenerationProviderMismatch    = errors.New("app: recorded generation provider is not served by this process")
	errMandatoryWorkerLaunch         = errors.New("app: mandatory worker could not be launched")
)

func New(options Options) (*Application, error) {
	if options.Writer == nil || options.Repository == nil || options.IDs == nil || options.Clock == nil || options.Blobs == nil {
		return nil, errors.New("app: writer, repository, IDs, clock, and blob store are required")
	}
	if err := options.Timezone.Validate(); err != nil {
		return nil, err
	}
	if options.MaxAttempts < 1 || options.MaxInputBytes < 1 || options.MaxOutputBytes < 1 || options.SafetyScanInterval <= 0 {
		return nil, errors.New("app: invalid generation or scan limits")
	}
	dialogueReconciler, _ := options.CommitNotifier.(dialogueProjectionReconciler)
	if len(options.RetryBackoff) != options.MaxAttempts-1 {
		return nil, errors.New("app: retry backoff length must equal max attempts minus one")
	}
	structuredMode := options.StructuredOutputMode
	if structuredMode == "" {
		// Preserve source compatibility for embedders created before M5. The
		// production host always passes the mode resolved from strict config.
		structuredMode = generation.StructuredOutputPrompt
	}
	if err := structuredMode.Validate(); err != nil {
		return nil, err
	}
	var autonomyPolicy *autonomy.Policy
	autonomySource := options.AutonomySource
	autonomyClock := options.AutonomyClock
	if options.AutonomyPolicy != nil {
		copyPolicy := *options.AutonomyPolicy
		if err := copyPolicy.Validate(); err != nil {
			return nil, err
		}
		autonomyPolicy = &copyPolicy
		if autonomySource == nil {
			autonomySource, _ = options.Repository.(autonomy.Source)
		}
		if autonomyClock == nil {
			autonomyClock = autonomy.NewSystemSchedulerClock()
		}
		if (copyPolicy.SelfTalk.Enabled || copyPolicy.Initiative.Enabled ||
			copyPolicy.Retention.Mode == autonomy.RetentionCandidateAfter) && autonomySource == nil {
			return nil, errors.New("app: enabled autonomy requires an autonomy source")
		}
		if copyPolicy.Initiative.Enabled && options.ProjectionMaxStaleness <= 0 {
			return nil, errors.New("app: initiative requires a positive Projection staleness bound")
		}
	}
	providerIdentity := options.Provider
	if identified, ok := options.Generator.(generation.IdentifiedGenerator); ok {
		providerIdentity = identified.ProviderIdentity()
		if providerIdentity == "" {
			return nil, errors.New("app: generator returned an empty provider identity")
		}
	}
	generator := options.Generator
	if generator != nil && options.OperationalObserver != nil {
		generator = observedGenerator{
			delegate: generator, observer: options.OperationalObserver, providerIdentity: providerIdentity,
		}
	}
	return &Application{
		writer: options.Writer, commitNotifier: options.CommitNotifier, dialogueReconciler: dialogueReconciler,
		repository: options.Repository, ids: options.IDs,
		clock: options.Clock, timezone: options.Timezone, blobs: options.Blobs,
		generator: generator, provider: providerIdentity, model: options.Model,
		structuredOutputMode: structuredMode,
		maxAttempts:          options.MaxAttempts, retryBackoff: append([]time.Duration(nil), options.RetryBackoff...),
		maxInputBytes: options.MaxInputBytes, maxOutputBytes: options.MaxOutputBytes,
		safetyScanInterval: options.SafetyScanInterval, work: make(map[canonical.ID]*sync.Mutex),
		backgroundLandings: make(map[canonical.ID]*sync.Mutex),
		autonomyPolicy:     autonomyPolicy, autonomySource: autonomySource, autonomyClock: autonomyClock,
		projectionMaxStaleness: options.ProjectionMaxStaleness,
		autonomyHints:          make(chan canonical.ID, 64), autonomyAnchors: make(map[canonical.ID]autonomy.RuntimeAnchors),
		erasureCandidateSink:            options.ErasureCandidateSink,
		operationalObserver:             options.OperationalObserver,
		background:                      make(map[canonical.ID]*backgroundCall),
		backgroundBlocked:               make(map[canonical.ID]bool),
		foregroundEpoch:                 make(map[canonical.ID]uint64),
		dialogueCleanEpoch:              make(map[canonical.ID]uint64),
		dialogueScanCycles:              make(map[canonical.ID]dialogueScanCycle),
		memoryDiscoveryCursors:          make(map[canonical.ID]*domain.MemoryDiscoveryCursor),
		normalMemoryDiscoveryCursors:    make(map[canonical.ID]*domain.MemoryDiscoveryCursor),
		memoryNormalCycleEpochs:         make(map[canonical.ID]uint64),
		memoryReextractionCursors:       make(map[canonical.ID]*domain.MemoryReextractionDiscoveryCursor),
		memoryReextractionCycles:        make(map[canonical.ID]bool),
		memoryReextractionCycleEpochs:   make(map[canonical.ID]uint64),
		memoryReextractionRescanPending: make(map[canonical.ID]bool),
		memoryNormalCyclesDirty:         make(map[canonical.ID]bool),
		memoryReextractionCyclesDirty:   make(map[canonical.ID]bool),
		memoryNormalCompleteEpochs:      make(map[canonical.ID]uint64),
		memoryExtractionScansComplete:   make(map[canonical.ID]bool),
		workersDone:                     make(chan struct{}),
	}, nil
}

func (a *Application) SetObserver(observer Observer) {
	a.observerMu.Lock()
	a.observer = observer
	a.observerMu.Unlock()
}

func (a *Application) ActiveResident(ctx context.Context) (domain.ResidentSnapshot, error) {
	return a.repository.ActiveResident(ctx)
}

func (a *Application) History(ctx context.Context, residentID canonical.ID, limit int) ([]domain.Event, error) {
	return a.repository.History(ctx, residentID, limit)
}

func (a *Application) Ingress(ctx context.Context, text string) (domain.Event, error) {
	return a.IngressWithMetadata(ctx, domain.IngressRequest{RawText: text})
}

func (a *Application) IngressWithMetadata(ctx context.Context, request domain.IngressRequest) (domain.Event, error) {
	if err := a.ensureAccepting(); err != nil {
		return domain.Event{}, err
	}
	text := request.RawText
	if !utf8.ValidString(text) || text == "" {
		return domain.Event{}, errors.New("app: message must be non-empty UTF-8")
	}
	if len([]byte(text)) > a.maxInputBytes {
		return domain.Event{}, fmt.Errorf("app: message exceeds %d-byte limit", a.maxInputBytes)
	}
	if len(request.ExplicitEventIDs) != 0 {
		return domain.Event{}, fmt.Errorf(
			"%w: explicit event references require a durable Context Reference contract",
			domain.ErrInvalidEventReference,
		)
	}
	resident, err := a.repository.ActiveResident(ctx)
	if err != nil {
		return domain.Event{}, err
	}
	content, err := a.newContent(resident.ResidentID, "event_payload", []byte(text), "independent")
	if err != nil {
		return domain.Event{}, err
	}
	eventID, err := a.ids.New()
	if err != nil {
		return domain.Event{}, err
	}
	now := canonical.InstantFromTime(a.clock.Now())
	message := domain.IngressUserMessage{
		EventID: eventID, ResidentID: resident.ResidentID, OwnerPrincipalID: resident.OwnerPrincipalID,
		ResidentPrincipalID: resident.ResidentPrincipalID, Content: content, OccurredAt: now, OccurredTZ: a.timezone,
	}
	result, err := a.submitForegroundWithContent(ctx, domain.IngressUserMessageCommand(message), []domain.Content{content})
	if err != nil {
		return domain.Event{}, err
	}
	event, ok := result.Value.(domain.Event)
	if !ok {
		return domain.Event{}, errors.New("app: ingress writer returned an unexpected value")
	}
	// submitForegroundWithContent advances the foreground epoch before releasing
	// the resident's landing boundary. Assembly and Prepare are deliberately
	// best-effort follow-up work: their failure cannot retroactively turn
	// accepted ingress into an error.
	a.observeAutonomyEvent(event)
	a.withObserver(func(observer Observer) { observer.UserCommitted(event) })
	launched := a.launchWorker(func(workerCtx context.Context) {
		err := a.processResident(workerCtx, resident.ResidentID)
		if err != nil {
			a.observeMandatoryWorkerFailure(
				resident.ResidentID, event.ID,
				operationalmetrics.MandatoryWorkerPhaseWorkerLaunch,
				err, workerCtx.Err() == nil,
			)
		}
	})
	if !launched {
		a.observeMandatoryWorkerFailure(
			resident.ResidentID, event.ID,
			operationalmetrics.MandatoryWorkerPhaseWorkerLaunch,
			errMandatoryWorkerLaunch, false,
		)
	}
	return event, nil
}

type backgroundCall struct {
	cancel context.CancelCauseFunc
}

// backgroundCallLease freezes the foreground epoch before the final durable
// dialogue check and carries that epoch through provider execution and
// landing.  Every background purpose must use the lease instead of registering
// a cancellable call directly; otherwise a user ingress committed between the
// last queue scan and provider registration can be lost.
type backgroundCallLease struct {
	Context    context.Context
	residentID canonical.ID
	epoch      uint64
	finish     func()
	registered bool
	fair       bool
}

var errForegroundPreempted = errors.New("app: background generation was preempted by foreground dialogue")
var errResidentSelectionChanged = errors.New("app: active resident changed before the durable resident operation")

func (a *Application) beginBackgroundCall(parent context.Context, residentID canonical.ID) backgroundCallLease {
	epoch := a.captureForegroundEpoch(residentID)
	ctx, finish, registered := a.beginBackgroundCallAtEpoch(parent, residentID, epoch, true)
	return backgroundCallLease{
		Context: ctx, residentID: residentID, epoch: epoch, finish: finish, registered: registered,
	}
}

// beginFairBackgroundCallAtEpoch is reserved for the single mandatory memory
// item handed off by a completed foreground dialogue. It deliberately ignores
// the clean-cycle gate, but atomically requires the exact epoch captured by the
// fair selector. It must never recapture the epoch internally.
func (a *Application) beginFairBackgroundCallAtEpoch(
	parent context.Context,
	residentID canonical.ID,
	expected uint64,
) backgroundCallLease {
	ctx, finish, registered := a.registerBackgroundCallAtEpoch(parent, residentID, expected, true, false)
	return backgroundCallLease{
		Context: ctx, residentID: residentID, epoch: expected, finish: finish, registered: registered, fair: true,
	}
}

func (a *Application) backgroundCallPreempted(ctx context.Context, lease backgroundCallLease) bool {
	_ = ctx
	return !lease.registered || errors.Is(context.Cause(lease.Context), errForegroundPreempted) ||
		a.foregroundAdvanced(lease.residentID, lease.epoch)
}

// beginBackgroundCallAtEpoch performs normal background admission. A normal
// provider may register only when the expected foreground epoch is still
// current and a complete dialogue scan proved that exact epoch clean.
func (a *Application) beginBackgroundCallAtEpoch(
	parent context.Context,
	residentID canonical.ID,
	expected uint64,
	requireExpected bool,
) (context.Context, func(), bool) {
	return a.registerBackgroundCallAtEpoch(parent, residentID, expected, requireExpected, true)
}

func (a *Application) registerBackgroundCallAtEpoch(
	parent context.Context,
	residentID canonical.ID,
	expected uint64,
	requireExpected bool,
	requireClean bool,
) (context.Context, func(), bool) {
	return a.registerBackgroundCallAtEpochWithHook(
		parent, residentID, expected, requireExpected, requireClean, true,
	)
}

func (a *Application) registerBackgroundCallAtEpochWithHook(
	parent context.Context,
	residentID canonical.ID,
	expected uint64,
	requireExpected bool,
	requireClean bool,
	invokeHook bool,
) (context.Context, func(), bool) {
	ctx, cancel := context.WithCancelCause(parent)
	call := &backgroundCall{cancel: cancel}
	if invokeHook && a.backgroundRegistrationHook != nil {
		a.backgroundRegistrationHook()
	}
	a.backgroundMu.Lock()
	current := a.foregroundEpoch[residentID]
	cleanEpoch, clean := a.dialogueCleanEpoch[residentID]
	if a.backgroundSelectionBlocked || a.backgroundBlocked[residentID] ||
		(requireExpected && current != expected) ||
		(requireClean && (!clean || cleanEpoch != expected || current != expected)) {
		a.backgroundMu.Unlock()
		cancel(errForegroundPreempted)
		return ctx, func() {}, false
	}
	if a.background == nil {
		a.background = make(map[canonical.ID]*backgroundCall)
	}
	a.background[residentID] = call
	a.backgroundMu.Unlock()
	return ctx, func() {
		a.backgroundMu.Lock()
		if a.background[residentID] == call {
			delete(a.background, residentID)
		}
		a.backgroundMu.Unlock()
		cancel(nil)
	}, true
}

func (a *Application) captureForegroundEpoch(residentID canonical.ID) uint64 {
	a.backgroundMu.Lock()
	defer a.backgroundMu.Unlock()
	return a.foregroundEpoch[residentID]
}

func (a *Application) foregroundAdvanced(residentID canonical.ID, expected uint64) bool {
	a.backgroundMu.Lock()
	defer a.backgroundMu.Unlock()
	return a.foregroundEpoch[residentID] != expected
}

func (a *Application) foregroundCleanAtEpoch(residentID canonical.ID, expected uint64) bool {
	a.backgroundMu.Lock()
	defer a.backgroundMu.Unlock()
	cleanEpoch, ok := a.dialogueCleanEpoch[residentID]
	return ok && a.foregroundEpoch[residentID] == expected && cleanEpoch == expected
}

func (a *Application) currentForegroundClean(residentID canonical.ID) bool {
	a.backgroundMu.Lock()
	defer a.backgroundMu.Unlock()
	current := a.foregroundEpoch[residentID]
	cleanEpoch, ok := a.dialogueCleanEpoch[residentID]
	return ok && cleanEpoch == current
}

// recordDialogueCleanAtEpoch atomically compares the epoch captured at the
// start of the historical scan and publishes its clean proof. If ingress
// advanced concurrently, the stale cycle is discarded without admitting
// background work.
func (a *Application) recordDialogueCleanAtEpoch(residentID canonical.ID, expected uint64) bool {
	a.backgroundMu.Lock()
	defer a.backgroundMu.Unlock()
	if a.foregroundEpoch[residentID] != expected {
		return false
	}
	if a.dialogueCleanEpoch == nil {
		a.dialogueCleanEpoch = make(map[canonical.ID]uint64)
	}
	a.dialogueCleanEpoch[residentID] = expected
	return true
}

func (a *Application) preemptBackground(residentID canonical.ID) {
	a.backgroundMu.Lock()
	if a.foregroundEpoch == nil {
		a.foregroundEpoch = make(map[canonical.ID]uint64)
	}
	a.foregroundEpoch[residentID]++
	call := a.background[residentID]
	a.backgroundMu.Unlock()
	if call != nil {
		call.cancel(errForegroundPreempted)
	}
}

// invalidateResidentRuntimeLocked is the Operational half of a hard resident
// boundary. The caller must hold backgroundMu so epoch invalidation, clean
// proof removal, lease cancellation, and memory-state reset are atomic with
// respect to every provider registration.
func (a *Application) invalidateResidentRuntimeLocked(residentID canonical.ID) {
	if residentID.IsZero() {
		return
	}
	if a.foregroundEpoch == nil {
		a.foregroundEpoch = make(map[canonical.ID]uint64)
	}
	a.foregroundEpoch[residentID]++
	delete(a.dialogueCleanEpoch, residentID)
	if call := a.background[residentID]; call != nil {
		call.cancel(errForegroundPreempted)
	}
	a.resetMemoryDiscovery(residentID)
}

func (a *Application) Start(parent context.Context) error {
	if err := a.PreflightMandatory(parent); err != nil {
		return err
	}
	if err := a.Prepare(parent); err != nil {
		return err
	}
	if _, err := a.RecoverMandatory(parent); err != nil {
		a.Stop()
		return err
	}
	return a.StartWorkers(nil)
}

// WorkerStartGate is the runtime activation barrier. Startup recovery runs
// before the gate; long-lived workers do not perform work until it opens.
type WorkerStartGate interface {
	Wait(context.Context) error
	IsOpen() bool
}

// Prepare owns the mutation-capable startup recovery phase but deliberately
// does not launch background workers.
func (a *Application) Prepare(parent context.Context) error {
	if parent == nil {
		return errors.New("app: nil run context")
	}
	a.lifecycleMu.Lock()
	if a.stopping {
		a.lifecycleMu.Unlock()
		return ErrApplicationStopping
	}
	if a.started {
		a.lifecycleMu.Unlock()
		return errors.New("app: application is already started")
	}
	runCtx, cancel := context.WithCancel(parent)
	a.runCtx = runCtx
	a.runCancel = cancel
	a.started = true
	a.lifecycleMu.Unlock()

	if _, err := a.RecoverRunning(runCtx); err != nil {
		a.Stop()
		return err
	}
	return nil
}

// StartWorkers registers every long-lived worker. When gate is non-nil, each
// worker blocks before its first repository or provider operation.
func (a *Application) StartWorkers(gate WorkerStartGate) error {
	a.lifecycleMu.Lock()
	if !a.started || a.runCtx == nil {
		a.lifecycleMu.Unlock()
		return errors.New("app: application is not prepared")
	}
	if a.stopping {
		a.lifecycleMu.Unlock()
		return ErrApplicationStopping
	}
	if a.workersStarted {
		a.lifecycleMu.Unlock()
		return errors.New("app: application workers are already started")
	}
	a.workersStarted = true
	a.activationGate = gate
	runCtx := a.runCtx
	a.lifecycleMu.Unlock()

	resident, err := a.repository.ActiveResident(runCtx)
	if err != nil {
		a.Stop()
		return err
	}
	if resident.Status == "active" {
		if !a.launchGatedWorker(gate, func(ctx context.Context) {
			err := a.ProcessResident(ctx, resident.ResidentID)
			if err != nil {
				a.observeMandatoryWorkerFailure(
					resident.ResidentID, canonical.ID{},
					operationalmetrics.MandatoryWorkerPhaseWorkerLaunch,
					err, ctx.Err() == nil,
				)
			}
		}) {
			a.observeMandatoryWorkerFailure(
				resident.ResidentID, canonical.ID{},
				operationalmetrics.MandatoryWorkerPhaseWorkerLaunch,
				errMandatoryWorkerLaunch, false,
			)
		}
	}
	a.launchGatedWorker(gate, a.scanLoop)
	if a.autonomySchedulingEnabled() {
		a.launchGatedWorker(gate, a.autonomyLoop)
	}
	if a.retentionSchedulingEnabled() {
		a.launchGatedWorker(gate, a.retentionLoop)
	}
	return nil
}

func (a *Application) launchGatedWorker(gate WorkerStartGate, task func(context.Context)) bool {
	if gate == nil {
		return a.launchWorker(task)
	}
	return a.launchWorker(func(ctx context.Context) {
		if err := gate.Wait(ctx); err != nil {
			return
		}
		task(ctx)
	})
}

// Stop atomically closes application-level acceptance and cancels the owned
// run context. It does not close the Canonical Writer or SQLite store; hosts
// must call Wait first and only close persistence after Wait succeeds.
func (a *Application) Stop() {
	a.lifecycleMu.Lock()
	if a.stopping {
		a.lifecycleMu.Unlock()
		return
	}
	a.stopping = true
	if a.runCancel != nil {
		a.runCancel()
	}
	done := a.workersDone
	a.lifecycleMu.Unlock()
	go func() {
		a.workers.Wait()
		close(done)
	}()
}

// Wait joins every application-owned scan/generation goroutine, then closes
// any running attempt that escaped an interrupted path before returning. The
// supplied context bounds both joining and the final Canonical recovery pass.
func (a *Application) Wait(ctx context.Context) error {
	if ctx == nil {
		return errors.New("app: nil wait context")
	}
	a.lifecycleMu.Lock()
	stopping := a.stopping
	started := a.started
	done := a.workersDone
	a.lifecycleMu.Unlock()
	if !stopping {
		return ErrApplicationNotStopping
	}
	select {
	case <-done:
	default:
		select {
		case <-done:
		case <-ctx.Done():
			return errors.Join(ErrApplicationWorkersLive, ctx.Err())
		}
	}
	if !started {
		return nil
	}
	return a.Recover(ctx)
}

func (a *Application) ensureAccepting() error {
	a.lifecycleMu.Lock()
	defer a.lifecycleMu.Unlock()
	if a.stopping || (a.started && (a.runCtx == nil || a.runCtx.Err() != nil)) {
		return ErrApplicationStopping
	}
	if a.started && (!a.workersStarted || (a.activationGate != nil && !a.activationGate.IsOpen())) {
		return ErrApplicationNotActivated
	}
	return nil
}

func (a *Application) launchWorker(task func(context.Context)) bool {
	if task == nil {
		return false
	}
	a.lifecycleMu.Lock()
	if !a.started || a.stopping || a.runCtx == nil || a.runCtx.Err() != nil {
		a.lifecycleMu.Unlock()
		return false
	}
	ctx := a.runCtx
	a.workers.Add(1)
	a.lifecycleMu.Unlock()
	go func() {
		defer a.workers.Done()
		task(ctx)
	}()
	return true
}

func (a *Application) scanLoop(ctx context.Context) {
	ticker := time.NewTicker(a.safetyScanInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := a.cancelUnavailableResidentWork(ctx); err != nil {
				selected, selectedErr := a.repository.ActiveResident(ctx)
				if selectedErr == nil {
					a.observeMandatoryWorkerFailure(
						selected.ResidentID, canonical.ID{},
						operationalmetrics.MandatoryWorkerPhaseDialogueScan,
						err, ctx.Err() == nil,
					)
				}
			}
			resident, err := a.repository.ActiveResident(ctx)
			if err == nil && resident.Status == "active" {
				if processErr := a.ProcessResident(ctx, resident.ResidentID); processErr != nil {
					a.observeMandatoryWorkerFailure(
						resident.ResidentID, canonical.ID{},
						operationalmetrics.MandatoryWorkerPhaseDialogueScan,
						processErr, ctx.Err() == nil,
					)
				}
			} else if err != nil {
				// No trustworthy resident identity is available, so the closed
				// observer cannot emit an identifier-bearing event. The next
				// successful scan remains the recovery mechanism.
			}
		}
	}
}

func (a *Application) cancelUnavailableResidentWork(ctx context.Context) error {
	selected, err := a.repository.ActiveResident(ctx)
	if err != nil {
		return fmt.Errorf("app: resolve operational resident for cancellation scan: %w", err)
	}
	residents, err := a.repository.ListResidents(ctx)
	if err != nil {
		return fmt.Errorf("app: list residents for cancellation scan: %w", err)
	}
	for _, resident := range residents {
		if resident.Status == "active" && resident.ResidentID == selected.ResidentID {
			continue
		}
		if err := a.processResident(ctx, resident.ResidentID); err != nil {
			return fmt.Errorf("app: cancel unavailable resident %s work: %w", resident.ResidentID, err)
		}
		if err := a.cancelAutonomousResidentWork(ctx, resident.ResidentID); err != nil {
			return fmt.Errorf("app: cancel unavailable resident %s autonomous work: %w", resident.ResidentID, err)
		}
	}
	return nil
}

func (a *Application) Recover(ctx context.Context) error {
	terminalizer, err := a.newRecoveryTerminalizer()
	if err != nil {
		return err
	}
	_, err = terminalizer.Terminalize(ctx)
	return err
}

// RecoverRunning is the pre-integrity startup phase. It is public so serve,
// restore, and offline Admin orchestration can preserve the exact M7 order.
func (a *Application) RecoverRunning(ctx context.Context) (RecoveryTerminalizationResult, error) {
	terminalizer, err := a.newRecoveryTerminalizer()
	if err != nil {
		return RecoveryTerminalizationResult{}, err
	}
	return terminalizer.TerminalizeRunning(ctx)
}

// PreflightMandatory performs the command-wide overflow check without a
// provider call or Canonical mutation. Offline recovery invokes it before any
// running-attempt terminalization.
func (a *Application) PreflightMandatory(ctx context.Context) error {
	terminalizer, err := a.newRecoveryTerminalizer()
	if err != nil {
		return err
	}
	return terminalizer.PreflightMandatory(ctx)
}

// RecoverMandatory is the post-integrity startup phase. Unresolvable
// cancellation envelopes remain typed candidates in the returned result;
// this method never dispatches a provider.
func (a *Application) RecoverMandatory(ctx context.Context) (RecoveryTerminalizationResult, error) {
	terminalizer, err := a.newRecoveryTerminalizer()
	if err != nil {
		return RecoveryTerminalizationResult{}, err
	}
	return terminalizer.TerminalizeMandatory(ctx)
}

func (a *Application) newRecoveryTerminalizer() (*RecoveryTerminalizer, error) {
	options := RecoveryTerminalizerOptions{
		Repository: a.repository,
		IDs:        a.ids,
		Submit:     a.submit,
		Observer:   a.operationalObserver,
	}
	mandatory, hasMandatory := a.repository.(MandatoryWorkRepository)
	resolver, hasResolver := a.repository.(CancellationEnvelopeResolver)
	if hasMandatory != hasResolver {
		return nil, errors.New("app: repository has an incomplete mandatory recovery capability")
	}
	if hasMandatory {
		options.MandatoryRepository = mandatory
		options.EnvelopeResolver = resolver
	}
	return NewRecoveryTerminalizer(options)
}

func (a *Application) residentLock(residentID canonical.ID) *sync.Mutex {
	a.workMu.Lock()
	defer a.workMu.Unlock()
	lock := a.work[residentID]
	if lock == nil {
		lock = &sync.Mutex{}
		a.work[residentID] = lock
	}
	return lock
}

func (a *Application) backgroundLandingLock(residentID canonical.ID) *sync.Mutex {
	a.backgroundLandingMu.Lock()
	defer a.backgroundLandingMu.Unlock()
	if a.backgroundLandings == nil {
		a.backgroundLandings = make(map[canonical.ID]*sync.Mutex)
	}
	lock := a.backgroundLandings[residentID]
	if lock == nil {
		lock = &sync.Mutex{}
		a.backgroundLandings[residentID] = lock
	}
	return lock
}

func (a *Application) newContent(residentID canonical.ID, class string, bytesValue []byte, erasurePolicy string) (domain.Content, error) {
	id, err := a.ids.New()
	if err != nil {
		return domain.Content{}, err
	}
	salt, err := canonical.NewContentSalt(rand.Reader)
	if err != nil {
		return domain.Content{}, err
	}
	commitment, err := canonical.CommitContent(class, salt, bytesValue)
	if err != nil {
		return domain.Content{}, err
	}
	return domain.Content{
		ID: id, ResidentID: residentID, Class: class, Bytes: append([]byte(nil), bytesValue...),
		BlobHash: canonical.HashBlob(bytesValue), Commitment: commitment, CommitmentSalt: salt,
		ErasurePolicy: erasurePolicy,
	}, nil
}

func (a *Application) submitWithContent(ctx context.Context, command canonical.Command, contents []domain.Content) (canonical.CommandResult, error) {
	if len(contents) == 0 {
		return a.submit(ctx, command)
	}
	residentID, staged, err := a.stageContentCommand(ctx, command, contents)
	if err != nil {
		return canonical.CommandResult{}, err
	}
	result, err := a.submit(ctx, command)
	if err != nil {
		return canonical.CommandResult{}, err
	}
	a.acknowledgeStagedContent(ctx, residentID, staged, result)
	return result, nil
}

func (a *Application) stageContentCommand(
	ctx context.Context,
	command canonical.Command,
	contents []domain.Content,
) (canonical.ID, []blob.Staged, error) {
	if command == nil {
		return canonical.ID{}, nil, errors.New("app: nil content command")
	}
	residentID, residentScoped := command.Scope().ResidentID()
	if !residentScoped {
		return canonical.ID{}, nil, errors.New("app: content command must have a resident scope")
	}
	staged := make([]blob.Staged, 0, len(contents))
	for _, content := range contents {
		if content.ResidentID != residentID {
			return canonical.ID{}, nil, errors.New("app: content resident does not match command scope")
		}
		item, err := a.blobs.Stage(ctx, residentID, bytes.NewReader(content.Bytes))
		if err != nil {
			return canonical.ID{}, nil, err
		}
		if item.ResidentID() != residentID || item.Digest() != content.BlobHash {
			return canonical.ID{}, nil, errors.New("app: staged content hash mismatch")
		}
		staged = append(staged, item)
	}
	// Runtime §7.2 requires every physical object to exist before the
	// Canonical transaction creates its reference. Finalize retains a recovery
	// marker, so any later rollback or ambiguous commit remains discoverable.
	for _, item := range staged {
		if _, err := a.blobs.Finalize(ctx, residentID, item); err != nil {
			return canonical.ID{}, nil, err
		}
	}
	return residentID, staged, nil
}

func (a *Application) acknowledgeStagedContent(
	ctx context.Context,
	residentID canonical.ID,
	staged []blob.Staged,
	result canonical.CommandResult,
) {
	if result.Commit.CommitID.IsZero() {
		// ErrNoMutation is a successful logical retry, but its provisional
		// Canonical transaction was rolled back. Retain the sealed markers so
		// authoritative orphan recovery can distinguish an already-referenced
		// deduplicated object from an unreferenced candidate.
		return
	}
	for _, item := range staged {
		// Canonical success never depends on post-commit filesystem cleanup.
		// A failed acknowledgement leaves a marker which recovery will retain
		// after its authoritative reference check.
		_ = a.blobs.Acknowledge(context.WithoutCancel(ctx), residentID, item)
	}
}

func (a *Application) submitForegroundWithContent(
	ctx context.Context,
	command canonical.Command,
	contents []domain.Content,
) (canonical.CommandResult, error) {
	residentID, staged, err := a.stageContentCommand(ctx, command, contents)
	if err != nil {
		return canonical.CommandResult{}, err
	}
	if err := ctx.Err(); err != nil {
		return canonical.CommandResult{}, err
	}
	landing := a.backgroundLandingLock(residentID)
	landing.Lock()
	a.backgroundMu.Lock()
	blocked := a.backgroundSelectionBlocked || a.backgroundBlocked[residentID]
	a.backgroundMu.Unlock()
	if !blocked {
		selected, selectedErr := a.repository.ActiveResident(context.WithoutCancel(ctx))
		blocked = selectedErr != nil || selected.Status != "active" || selected.ResidentID != residentID
	}
	if blocked {
		landing.Unlock()
		return canonical.CommandResult{}, errResidentSelectionChanged
	}
	result, submitErr := a.writer.Submit(context.WithoutCancel(ctx), command)
	// A Writer error may be post-commit/ambiguous. Invalidate optional work
	// fail-closed before another background landing can cross this boundary.
	a.preemptBackground(residentID)
	landing.Unlock()
	a.afterSubmit(result, submitErr, residentID, true)
	if submitErr != nil {
		return canonical.CommandResult{}, submitErr
	}
	a.acknowledgeStagedContent(ctx, residentID, staged, result)
	return result, nil
}

// submitBackgroundMutationAndBeginCall linearizes a background attempt
// transition and its provider lease with foreground ingress, resident
// selection, and archive. Content is staged before the narrow boundary; the
// Canonical mutation and lease registration happen while the resident landing
// lock is held. Consequently a lifecycle/foreground boundary either wins
// before the mutation (no run/attempt is created) or observes an already
// registered lease which it can preempt after the mutation committed.
func (a *Application) submitBackgroundMutationAndBeginCall(
	ctx context.Context,
	expectedEpoch uint64,
	requireClean bool,
	command canonical.Command,
	contents []domain.Content,
) (canonical.CommandResult, backgroundCallLease, error) {
	residentID, staged, err := a.stageContentCommand(ctx, command, contents)
	if err != nil {
		return canonical.CommandResult{}, backgroundCallLease{}, err
	}
	if err := ctx.Err(); err != nil {
		return canonical.CommandResult{}, backgroundCallLease{}, err
	}
	// Keep the deterministic race seam outside the landing lock. A foreground
	// or lifecycle boundary released by the hook can therefore complete, after
	// which the authoritative checks below reject without a Canonical mutation.
	if a.backgroundRegistrationHook != nil {
		a.backgroundRegistrationHook()
	}
	landing := a.backgroundLandingLock(residentID)
	landing.Lock()
	if err := ctx.Err(); err != nil {
		landing.Unlock()
		return canonical.CommandResult{}, backgroundCallLease{}, err
	}
	selected, selectedErr := a.repository.ActiveResident(context.WithoutCancel(ctx))
	if selectedErr != nil || selected.Status != "active" || selected.ResidentID != residentID {
		landing.Unlock()
		if selectedErr != nil {
			return canonical.CommandResult{}, backgroundCallLease{}, selectedErr
		}
		return canonical.CommandResult{}, backgroundCallLease{}, errForegroundPreempted
	}
	leaseCtx, finish, registered := a.registerBackgroundCallAtEpochWithHook(
		ctx, residentID, expectedEpoch, true, requireClean, false,
	)
	lease := backgroundCallLease{
		Context: leaseCtx, residentID: residentID, epoch: expectedEpoch,
		finish: finish, registered: registered, fair: !requireClean,
	}
	if !registered {
		landing.Unlock()
		return canonical.CommandResult{}, lease, errForegroundPreempted
	}
	result, submitErr := a.writer.Submit(context.WithoutCancel(ctx), command)
	landing.Unlock()
	a.afterSubmit(result, submitErr, residentID, true)
	if submitErr != nil {
		lease.finish()
		return canonical.CommandResult{}, backgroundCallLease{}, submitErr
	}
	a.acknowledgeStagedContent(ctx, residentID, staged, result)
	return result, lease, nil
}

func (a *Application) submitBackgroundLandingWithContent(
	ctx context.Context,
	lease backgroundCallLease,
	command canonical.Command,
	contents []domain.Content,
) (canonical.CommandResult, error) {
	residentID, staged, err := a.stageContentCommand(ctx, command, contents)
	if err != nil {
		return canonical.CommandResult{}, err
	}
	if residentID != lease.residentID || !lease.registered {
		return canonical.CommandResult{}, errForegroundPreempted
	}
	if a.backgroundLandingHook != nil {
		a.backgroundLandingHook()
	}
	landing := a.backgroundLandingLock(residentID)
	landing.Lock()
	a.backgroundMu.Lock()
	preempted := a.backgroundSelectionBlocked || a.backgroundBlocked[residentID] ||
		a.foregroundEpoch[residentID] != lease.epoch ||
		errors.Is(context.Cause(lease.Context), errForegroundPreempted)
	a.backgroundMu.Unlock()
	if !preempted {
		selected, selectedErr := a.repository.ActiveResident(context.WithoutCancel(ctx))
		preempted = selectedErr != nil || selected.Status != "active" || selected.ResidentID != residentID
	}
	if preempted {
		landing.Unlock()
		return canonical.CommandResult{}, errForegroundPreempted
	}
	// The lifecycle/foreground checks and the durable landing are one narrow
	// process-local critical section. Notifications and blob acknowledgements
	// run only after releasing it, so callback reentry cannot deadlock selection.
	result, submitErr := a.writer.Submit(context.WithoutCancel(ctx), command)
	landing.Unlock()
	a.afterSubmit(result, submitErr, residentID, true)
	if submitErr != nil {
		return canonical.CommandResult{}, submitErr
	}
	a.acknowledgeStagedContent(ctx, residentID, staged, result)
	return result, nil
}

func (a *Application) submit(ctx context.Context, command canonical.Command) (canonical.CommandResult, error) {
	var residentID canonical.ID
	var residentScoped bool
	if command != nil {
		residentID, residentScoped = command.Scope().ResidentID()
	}
	result, err := a.writer.Submit(ctx, command)
	a.afterSubmit(result, err, residentID, residentScoped)
	return result, err
}

// afterSubmit performs only post-commit, best-effort notifications. Keeping it
// separate lets hard lifecycle boundaries run Writer.Submit under their own
// admission fence and release that fence before an arbitrary notifier callback.
func (a *Application) afterSubmit(
	result canonical.CommandResult,
	err error,
	residentID canonical.ID,
	residentScoped bool,
) {
	if err == nil && !result.Commit.CommitID.IsZero() && a.commitNotifier != nil {
		// Projection notification is deliberately lossy and cannot change the
		// result of an already-durable Canonical commit.
		_ = a.commitNotifier.NotifyCommit(result.Commit)
	}
	if err == nil && !result.Commit.CommitID.IsZero() && residentScoped && a.autonomySchedulingEnabled() {
		select {
		case a.autonomyHints <- residentID:
		default:
		}
	}
}

func (a *Application) withObserver(call func(Observer)) {
	a.observerMu.RLock()
	observer := a.observer
	a.observerMu.RUnlock()
	if observer != nil {
		call(observer)
	}
}
