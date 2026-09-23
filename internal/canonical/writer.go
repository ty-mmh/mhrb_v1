package canonical

import (
	"context"
	"errors"
	"fmt"
	"math"
	"reflect"
	"runtime/debug"
	"sync"
	"time"

	"mahoroba.local/mahoroba/internal/operationalmetrics"
)

// Scope explicitly distinguishes a global command from a command that can
// mutate at most one resident.
type Scope struct {
	residentID *ID
}

func GlobalScope() Scope { return Scope{} }

func ResidentScope(residentID ID) (Scope, error) {
	if err := residentID.Validate(); err != nil {
		return Scope{}, fmt.Errorf("%w: resident ID: %v", ErrInvalidScope, err)
	}
	copyID := residentID
	return Scope{residentID: &copyID}, nil
}

func (scope Scope) IsGlobal() bool { return scope.residentID == nil }

func (scope Scope) ResidentID() (ID, bool) {
	if scope.residentID == nil {
		return ID{}, false
	}
	return *scope.residentID, true
}

func (scope Scope) Equal(other Scope) bool {
	leftResident, leftOK := scope.ResidentID()
	rightResident, rightOK := other.ResidentID()
	return leftOK == rightOK && (!leftOK || leftResident == rightResident)
}

func (scope Scope) Validate() error {
	if scope.residentID == nil {
		return nil
	}
	if err := scope.residentID.Validate(); err != nil {
		return fmt.Errorf("%w: %v", ErrInvalidScope, err)
	}
	return nil
}

// CommitMetadata is allocated exactly once for a Canonical Unit of Work. All
// canonical rows created by a command must use this metadata and ledger time.
type CommitMetadata struct {
	CommitID    ID
	CommitSeq   CommitSeq
	Scope       Scope
	CommittedAt Instant
	CommittedTZ Timezone
}

func (metadata CommitMetadata) Validate() error {
	if err := metadata.CommitID.Validate(); err != nil {
		return fmt.Errorf("canonical: invalid commit ID: %w", err)
	}
	if err := metadata.CommitSeq.Validate(); err != nil {
		return err
	}
	if err := metadata.Scope.Validate(); err != nil {
		return err
	}
	if err := metadata.CommittedTZ.Validate(); err != nil {
		return err
	}
	return nil
}

func (metadata CommitMetadata) Equal(other CommitMetadata) bool {
	if metadata.CommitID != other.CommitID || metadata.CommitSeq != other.CommitSeq ||
		metadata.CommittedAt != other.CommittedAt || metadata.CommittedTZ != other.CommittedTZ {
		return false
	}
	return metadata.Scope.Equal(other.Scope)
}

// Head is the persisted Canonical Commit head. Exists avoids using a zero
// CommitSeq or sentinel timestamp to represent an empty store.
type Head struct {
	Exists      bool
	CommitSeq   CommitSeq
	CommittedAt Instant
}

func (head Head) Validate() error {
	if !head.Exists {
		if head.CommitSeq != 0 || head.CommittedAt != 0 {
			return fmt.Errorf("canonical: empty head contains cursor state")
		}
		return nil
	}
	return head.CommitSeq.Validate()
}

// Backend is the only persistence interface required by Writer. An adapter is
// responsible for making Begin atomic and for returning a UoW with any narrow,
// command-specific capability interfaces. It must not expose sql.Tx here.
type Backend interface {
	LoadHead(ctx context.Context) (Head, error)
	Begin(ctx context.Context, metadata CommitMetadata) (CanonicalUoW, error)
}

// CanonicalUoW is intentionally minimal. A concrete adapter may also implement
// domain capability interfaces (for example EventAppender); commands assert
// only the capability they require. Generic table CRUD is not part of this API.
type CanonicalUoW interface {
	Metadata() CommitMetadata
	Commit(ctx context.Context) error
	Rollback(ctx context.Context) error
}

// Command represents one indivisible Canonical meaning operation.
//
// Validate is run before enqueue and again after Begin. Execute therefore runs
// inside the adapter UoW and is responsible for final state-dependent checks
// using a narrow capability exposed by that UoW.
type Command interface {
	Name() string
	Scope() Scope
	Validate() error
	Execute(ctx context.Context, uow CanonicalUoW) (any, error)
}

type CommandResult struct {
	Commit CommitMetadata
	Value  any
}

type WriterOptions struct {
	Backend       Backend
	IDs           *IDGenerator
	Clock         Clock
	Timezone      Timezone
	QueueCapacity int
	Observer      interface {
		WriterAccepted()
		WriterQueued()
		WriterInFlight()
		WriterFinished(operationalmetrics.WriterOutcome, time.Duration)
	}
}

type commandRequest struct {
	ctx        context.Context
	command    Command
	name       string
	scope      Scope
	activity   ActivityToken
	response   chan commandResponse
	admitted   bool
	observedAt time.Time
}

type commandResponse struct {
	result CommandResult
	err    error
}

// Writer serializes every Canonical mutation through one bounded queue.
type Writer struct {
	backend  Backend
	ids      *IDGenerator
	clock    Clock
	timezone Timezone
	observer interface {
		WriterAccepted()
		WriterQueued()
		WriterInFlight()
		WriterFinished(operationalmetrics.WriterOutcome, time.Duration)
	}

	queue      chan commandRequest
	queueSlots chan struct{}
	stop       chan struct{}
	done       chan struct{}

	acceptMu       sync.Mutex
	accept         bool
	pendingEnqueue int
	enqueueDone    chan struct{}
	closeOnce      sync.Once

	poisonMu sync.RWMutex
	poisoned error

	// admissionMu is deliberately separate from acceptMu. acceptMu owns the
	// irreversible Close protocol; admissionMu owns the reversible runtime
	// activation fence. A pause first rejects new commands and then waits for
	// every command admitted before the pause to finish, so a caller can take a
	// stable read snapshot without a queued Canonical mutation racing it.
	admissionMu      sync.Mutex
	admissionOpen    bool
	admissionActive  int
	admissionDrained chan struct{}

	head       Head // owned exclusively by run after OpenWriter returns
	activities *activityRegistry
}

func OpenWriter(ctx context.Context, options WriterOptions) (*Writer, error) {
	if ctx == nil {
		return nil, fmt.Errorf("canonical: nil open context")
	}
	if options.Backend == nil {
		return nil, fmt.Errorf("canonical: nil writer backend")
	}
	if options.IDs == nil {
		return nil, fmt.Errorf("canonical: nil ID generator")
	}
	if options.Clock == nil {
		return nil, fmt.Errorf("canonical: nil ledger clock")
	}
	if err := options.Timezone.Validate(); err != nil {
		return nil, err
	}
	if options.QueueCapacity <= 0 {
		return nil, fmt.Errorf("canonical: queue capacity must be positive")
	}

	head, err := options.Backend.LoadHead(ctx)
	if err != nil {
		return nil, fmt.Errorf("canonical: load head: %w", err)
	}
	if err := head.Validate(); err != nil {
		return nil, fmt.Errorf("canonical: invalid persisted head: %w", err)
	}

	writer := &Writer{
		backend:       options.Backend,
		ids:           options.IDs,
		clock:         options.Clock,
		timezone:      options.Timezone,
		observer:      options.Observer,
		queue:         make(chan commandRequest, options.QueueCapacity),
		queueSlots:    make(chan struct{}, options.QueueCapacity),
		stop:          make(chan struct{}),
		done:          make(chan struct{}),
		enqueueDone:   make(chan struct{}),
		accept:        true,
		admissionOpen: true,
		head:          head,
		activities:    newActivityRegistry(),
	}
	go writer.run()
	return writer, nil
}

func (writer *Writer) Submit(ctx context.Context, command Command) (CommandResult, error) {
	if ctx == nil {
		return CommandResult{}, fmt.Errorf("canonical: nil command context")
	}
	if command == nil {
		return CommandResult{}, fmt.Errorf("canonical: nil command")
	}
	name, scope, err := snapshotCommand(command)
	if err != nil {
		return CommandResult{}, err
	}
	if err := writer.poisonError(); err != nil {
		return CommandResult{}, err
	}
	if !writer.beginAdmission() {
		return CommandResult{}, ErrWriterAdmissionPaused
	}
	fence := false
	if command, ok := command.(residentAdmissionFenceCommand); ok {
		fence = command.RequiresResidentAdmissionFence()
	}
	activity, err := writer.activities.admit(scope, fence)
	if err != nil {
		writer.endAdmission()
		return CommandResult{}, err
	}

	request := commandRequest{
		ctx:        ctx,
		command:    command,
		name:       name,
		scope:      scope,
		activity:   activity,
		response:   make(chan commandResponse, 1),
		admitted:   true,
		observedAt: time.Now(),
	}
	if writer.observer != nil {
		writer.observer.WriterAccepted()
	}

	if !writer.reserveEnqueue() {
		writer.activities.finish(activity)
		writer.endAdmission()
		writer.observeWriterFinished(request, operationalmetrics.WriterOutcomeFailed)
		return CommandResult{}, ErrWriterClosed
	}
	slotAcquired := false
	select {
	case writer.queueSlots <- struct{}{}:
		slotAcquired = true
	case <-ctx.Done():
	}
	if !slotAcquired {
		writer.finishEnqueue()
		writer.activities.finish(activity)
		writer.endAdmission()
		writer.observeWriterFinished(request, operationalmetrics.WriterOutcomeFailed)
		return CommandResult{}, ctx.Err()
	}
	if err := writer.activities.transition(activity, activityQueued); err != nil && activity.Valid() {
		<-writer.queueSlots
		writer.finishEnqueue()
		writer.activities.finish(activity)
		writer.endAdmission()
		writer.observeWriterFinished(request, operationalmetrics.WriterOutcomeFailed)
		return CommandResult{}, err
	}
	if writer.observer != nil {
		writer.observer.WriterQueued()
	}
	writer.queue <- request
	writer.finishEnqueue()

	select {
	case response := <-request.response:
		return response.result, response.err
	case <-ctx.Done():
		// The queued command observes the same context and will rollback if it
		// has not committed. If commit wins the race, idempotency remains the
		// caller's recovery mechanism.
		return CommandResult{}, ctx.Err()
	}
}

// PauseAdmissions closes the reversible runtime admission fence and waits
// until every command admitted before the close has returned. It does not
// close the Writer and is idempotent while paused.
func (writer *Writer) PauseAdmissions(ctx context.Context) error {
	if ctx == nil {
		return fmt.Errorf("canonical: nil admission pause context")
	}
	writer.admissionMu.Lock()
	writer.admissionOpen = false
	if writer.admissionActive == 0 {
		writer.admissionMu.Unlock()
		return nil
	}
	if writer.admissionDrained == nil {
		writer.admissionDrained = make(chan struct{})
	}
	drained := writer.admissionDrained
	writer.admissionMu.Unlock()
	select {
	case <-drained:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// ResumeAdmissions reopens a successfully drained runtime admission fence.
// Resuming while an earlier pause is still draining is rejected instead of
// allowing a new command to enter the supposedly stable interval.
func (writer *Writer) ResumeAdmissions() error {
	writer.admissionMu.Lock()
	defer writer.admissionMu.Unlock()
	if writer.admissionActive != 0 {
		return errors.New("canonical: cannot resume admissions before drain completes")
	}
	writer.admissionOpen = true
	writer.admissionDrained = nil
	return nil
}

// AdmissionsPaused reports the reversible fence state for startup
// orchestration and diagnostics. It does not imply that the pre-pause work
// has drained; PauseAdmissions returning successfully is that proof.
func (writer *Writer) AdmissionsPaused() bool {
	writer.admissionMu.Lock()
	defer writer.admissionMu.Unlock()
	return !writer.admissionOpen
}

// ResidentActivity returns accepted, queued, and in-flight counts for one
// resident. The snapshot grants no exclusion authority.
func (writer *Writer) ResidentActivity(residentID ID) (ActivitySnapshot, error) {
	if err := residentID.Validate(); err != nil {
		return ActivitySnapshot{}, err
	}
	return writer.activities.snapshot(residentID, 0), nil
}

func (writer *Writer) beginAdmission() bool {
	writer.admissionMu.Lock()
	defer writer.admissionMu.Unlock()
	if !writer.admissionOpen {
		return false
	}
	writer.admissionActive++
	return true
}

func (writer *Writer) endAdmission() {
	writer.admissionMu.Lock()
	defer writer.admissionMu.Unlock()
	writer.admissionActive--
	if writer.admissionActive < 0 {
		panic("canonical: negative active admission count")
	}
	if writer.admissionActive == 0 && writer.admissionDrained != nil {
		close(writer.admissionDrained)
		writer.admissionDrained = nil
	}
}

// Close stops acceptance and drains every command accepted before the gate was
// closed. An accepted submission may still be waiting for queue capacity; run
// therefore keeps draining until all such enqueue attempts have either
// completed or observed their own context cancellation.
func (writer *Writer) Close(ctx context.Context) error {
	if ctx == nil {
		return fmt.Errorf("canonical: nil close context")
	}
	writer.closeOnce.Do(func() {
		writer.acceptMu.Lock()
		writer.accept = false
		if writer.pendingEnqueue == 0 {
			close(writer.enqueueDone)
		}
		close(writer.stop)
		writer.acceptMu.Unlock()
	})
	select {
	case <-writer.done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (writer *Writer) Done() <-chan struct{} { return writer.done }

// reserveEnqueue linearizes submission acceptance with Close without keeping
// a lock held while a bounded queue send may block. The queue is deliberately
// never closed, so an accepted sender can never race a send against close.
func (writer *Writer) reserveEnqueue() bool {
	writer.acceptMu.Lock()
	defer writer.acceptMu.Unlock()
	if !writer.accept {
		return false
	}
	writer.pendingEnqueue++
	return true
}

func (writer *Writer) finishEnqueue() {
	writer.acceptMu.Lock()
	defer writer.acceptMu.Unlock()
	writer.pendingEnqueue--
	if !writer.accept && writer.pendingEnqueue == 0 {
		close(writer.enqueueDone)
	}
}

func (writer *Writer) run() {
	defer close(writer.done)
	for {
		select {
		case request := <-writer.queue:
			writer.handle(request)
		case <-writer.stop:
			writer.drain()
			return
		}
	}
}

func (writer *Writer) drain() {
	for {
		select {
		case request := <-writer.queue:
			writer.handle(request)
		case <-writer.enqueueDone:
			// No sender can add another request after enqueueDone closes. Drain
			// the fixed remainder before publishing Writer completion.
			for {
				select {
				case request := <-writer.queue:
					writer.handle(request)
				default:
					return
				}
			}
		}
	}
}

func (writer *Writer) handle(request commandRequest) {
	select {
	case <-writer.queueSlots:
	default:
		panic("canonical: queued command has no reserved slot")
	}
	response := func() commandResponse {
		// Complete both activity and global-admission accounting before the
		// response becomes observable. An unbuffered response send otherwise
		// allows the caller to snapshot a transient in-flight entry before these
		// defers run, most visibly after execute recovers a command panic.
		if request.admitted {
			defer writer.endAdmission()
		}
		defer writer.activities.finish(request.activity)
		if err := writer.activities.transition(request.activity, activityInFlight); err != nil && request.activity.Valid() {
			writer.observeWriterFinished(request, operationalmetrics.WriterOutcomeFailed)
			return commandResponse{err: err}
		}
		if writer.observer != nil {
			writer.observer.WriterInFlight()
		}
		ctx := contextWithActivityToken(request.ctx, request.activity)
		result, err := writer.execute(ctx, request.name, request.scope, request.command)
		outcome := operationalmetrics.WriterOutcomeNoMutation
		if err != nil {
			outcome = operationalmetrics.WriterOutcomeFailed
		} else if !result.Commit.CommitID.IsZero() {
			outcome = operationalmetrics.WriterOutcomeCommitted
		}
		writer.observeWriterFinished(request, outcome)
		return commandResponse{result: result, err: err}
	}()
	request.response <- response
}

func (writer *Writer) observeWriterFinished(request commandRequest, outcome operationalmetrics.WriterOutcome) {
	if writer.observer == nil {
		return
	}
	writer.observer.WriterFinished(outcome, time.Since(request.observedAt))
}

func (writer *Writer) execute(ctx context.Context, name string, scope Scope, command Command) (result CommandResult, returnedErr error) {
	if err := writer.poisonError(); err != nil {
		return CommandResult{}, err
	}
	if err := ctx.Err(); err != nil {
		return CommandResult{}, err
	}

	metadata, err := writer.nextMetadata(scope)
	if err != nil {
		return CommandResult{}, err
	}
	if err := metadata.Validate(); err != nil {
		return CommandResult{}, err
	}
	uow, err := writer.backend.Begin(ctx, metadata)
	if err != nil {
		return CommandResult{}, fmt.Errorf("canonical: begin %s: %w", name, err)
	}
	if uow == nil {
		err := fmt.Errorf("canonical: backend returned nil UoW")
		writer.poison(err)
		return CommandResult{}, errors.Join(ErrWriterPoisoned, err)
	}
	defer func() {
		if recovered := recover(); recovered != nil {
			panicErr := fmt.Errorf("canonical: command %s panicked: %v\n%s", name, recovered, debug.Stack())
			rollbackErr := uow.Rollback(context.WithoutCancel(ctx))
			writer.poison(errors.Join(panicErr, rollbackErr))
			result = CommandResult{}
			returnedErr = errors.Join(ErrWriterPoisoned, panicErr, rollbackErr)
		}
	}()
	if !uow.Metadata().Equal(metadata) {
		err := fmt.Errorf("canonical: backend UoW metadata mismatch")
		rollbackErr := uow.Rollback(context.WithoutCancel(ctx))
		writer.poison(errors.Join(err, rollbackErr))
		return CommandResult{}, errors.Join(ErrWriterPoisoned, err, rollbackErr)
	}

	if err := validateCommandSnapshot(command, name, scope); err != nil {
		return CommandResult{}, writer.rollback(ctx, uow, err)
	}
	value, err := command.Execute(ctx, uow)
	if err != nil {
		// Only the exact trusted sentinel is a successful no-op. A joined or
		// wrapped error must remain fail-closed rather than hiding another cause.
		if err == ErrNoMutation {
			if rollbackErr := uow.Rollback(context.WithoutCancel(ctx)); rollbackErr != nil {
				writer.poison(errors.Join(err, rollbackErr))
				return CommandResult{}, errors.Join(ErrWriterPoisoned, err, rollbackErr)
			}
			return CommandResult{Value: value}, nil
		}
		return CommandResult{}, writer.rollback(ctx, uow, err)
	}
	if err := uow.Commit(ctx); err != nil {
		// A commit error may be ambiguous. Stop all subsequent writes and rely
		// on restart/head reload rather than risking cursor reuse.
		commitErr := fmt.Errorf("canonical: commit %s: %w", name, err)
		writer.poison(commitErr)
		return CommandResult{}, errors.Join(ErrWriterPoisoned, commitErr)
	}

	writer.head = Head{Exists: true, CommitSeq: metadata.CommitSeq, CommittedAt: metadata.CommittedAt}
	return CommandResult{Commit: metadata, Value: value}, nil
}

func (writer *Writer) nextMetadata(scope Scope) (CommitMetadata, error) {
	var next int64 = 1
	if writer.head.Exists {
		if writer.head.CommitSeq.Int64() == math.MaxInt64 {
			return CommitMetadata{}, ErrCommitSeqOverflow
		}
		next = writer.head.CommitSeq.Int64() + 1
	}
	commitSeq, err := NewCommitSeq(next)
	if err != nil {
		return CommitMetadata{}, err
	}
	commitID, err := writer.ids.New()
	if err != nil {
		return CommitMetadata{}, fmt.Errorf("canonical: allocate commit ID: %w", err)
	}
	ledgerTime := InstantFromTime(writer.clock.Now())
	if writer.head.Exists && ledgerTime <= writer.head.CommittedAt {
		if writer.head.CommittedAt == Instant(math.MaxInt64) {
			return CommitMetadata{}, fmt.Errorf("canonical: ledger time overflow")
		}
		ledgerTime = writer.head.CommittedAt + 1
	}
	return CommitMetadata{
		CommitID:    commitID,
		CommitSeq:   commitSeq,
		Scope:       scope,
		CommittedAt: ledgerTime,
		CommittedTZ: writer.timezone,
	}, nil
}

func (writer *Writer) rollback(ctx context.Context, uow CanonicalUoW, cause error) error {
	rollbackErr := uow.Rollback(context.WithoutCancel(ctx))
	if rollbackErr != nil {
		writer.poison(errors.Join(cause, rollbackErr))
		return errors.Join(ErrWriterPoisoned, cause, rollbackErr)
	}
	return cause
}

func (writer *Writer) poison(cause error) {
	writer.poisonMu.Lock()
	defer writer.poisonMu.Unlock()
	if writer.poisoned == nil {
		writer.poisoned = cause
	}
}

func (writer *Writer) poisonError() error {
	writer.poisonMu.RLock()
	defer writer.poisonMu.RUnlock()
	if writer.poisoned == nil {
		return nil
	}
	return errors.Join(ErrWriterPoisoned, writer.poisoned)
}

func snapshotCommand(command Command) (string, Scope, error) {
	value := reflect.ValueOf(command)
	if value.Kind() == reflect.Pointer && value.IsNil() {
		return "", Scope{}, fmt.Errorf("canonical: nil command")
	}
	name := command.Name()
	scope := command.Scope()
	if err := validateCommandSnapshot(command, name, scope); err != nil {
		return "", Scope{}, err
	}
	return name, scope, nil
}

func validateCommandSnapshot(command Command, name string, scope Scope) error {
	if name == "" {
		return fmt.Errorf("canonical: command name is empty")
	}
	if err := scope.Validate(); err != nil {
		return err
	}
	if command.Name() != name || !command.Scope().Equal(scope) {
		return fmt.Errorf("canonical: command identity or scope changed after submission")
	}
	if err := command.Validate(); err != nil {
		return fmt.Errorf("canonical: validate %s: %w", name, err)
	}
	return nil
}
