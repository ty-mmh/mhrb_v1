package projection

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"sync"
	"time"

	"mahoroba.local/mahoroba/internal/canonical"
)

const defaultNotificationCapacity = 64

type CoordinatorOptions struct {
	Registry             *Registry
	Source               Source
	Store                Store
	Clock                canonical.Clock
	Timezone             canonical.Timezone
	ScanInterval         time.Duration
	AsOfRefreshInterval  time.Duration
	RebuildRetryInterval time.Duration
	MaxStaleness         time.Duration
	Overrides            map[Name]ScheduleOverride
	NotificationCapacity int
	OnError              func(error)
	Observer             interface {
		ProjectionObserved(commitLag uint64, rebuildDuration time.Duration, rebuilt, failed bool)
	}
}

// ScheduleOverride changes only coordination frequency/staleness reporting;
// zero fields inherit the global setting and never enter evaluator inputs.
type ScheduleOverride struct {
	ScanInterval         time.Duration
	AsOfRefreshInterval  time.Duration
	RebuildRetryInterval time.Duration
	MaxStaleness         time.Duration
}

type schedule struct {
	ScanInterval         time.Duration
	AsOfRefreshInterval  time.Duration
	RebuildRetryInterval time.Duration
	MaxStaleness         time.Duration
}

type scheduleUnit struct {
	definitions         []Definition
	scanInterval        time.Duration
	asOfRefreshInterval time.Duration
}

type targetKey struct {
	name       Name
	residentID canonical.ID
}

type failure struct {
	err         error
	retryAt     time.Time
	refreshAsOf bool
}

type retrySignal struct {
	key         targetKey
	retryAt     time.Time
	refreshAsOf bool
}

type notification struct {
	residentID *canonical.ID
}

// Coordinator serializes work per resident, accepts lossy post-commit hints,
// and periodically reconciles every registered Projection. Periodic scans are
// correctness-critical; notifications are only a latency optimization.
type Coordinator struct {
	registry *Registry
	source   Source
	store    Store
	clock    canonical.Clock
	timezone canonical.Timezone

	schedules map[Name]schedule
	cohort    []Definition
	onError   func(error)
	observer  interface {
		ProjectionObserved(commitLag uint64, rebuildDuration time.Duration, rebuilt, failed bool)
	}

	notifications chan notification
	retries       chan retrySignal

	lifecycleMu   sync.Mutex
	started       bool
	stopped       bool
	retryClosing  bool
	cancel        context.CancelFunc
	done          chan struct{}
	ready         chan struct{}
	startupErr    error
	retryStop     chan struct{}
	retryStopOnce sync.Once
	retryWG       sync.WaitGroup

	locksMu sync.Mutex
	locks   map[canonical.ID]*sync.Mutex

	failuresMu sync.RWMutex
	failures   map[targetKey]failure
}

func NewCoordinator(options CoordinatorOptions) (*Coordinator, error) {
	if options.Registry == nil || len(options.Registry.Definitions()) == 0 {
		return nil, errors.New("projection: a non-empty registry is required")
	}
	if options.Source == nil || options.Store == nil || options.Clock == nil {
		return nil, errors.New("projection: source, store, and clock are required")
	}
	if err := options.Timezone.Validate(); err != nil {
		return nil, err
	}
	if options.ScanInterval <= 0 || options.AsOfRefreshInterval <= 0 ||
		options.RebuildRetryInterval <= 0 || options.MaxStaleness <= 0 {
		return nil, errors.New("projection: scheduling durations must be positive")
	}
	if options.NotificationCapacity < 0 {
		return nil, errors.New("projection: notification capacity must not be negative")
	}
	baseSchedule := schedule{
		ScanInterval: options.ScanInterval, AsOfRefreshInterval: options.AsOfRefreshInterval,
		RebuildRetryInterval: options.RebuildRetryInterval, MaxStaleness: options.MaxStaleness,
	}
	schedules := make(map[Name]schedule, len(options.Registry.Definitions()))
	for _, definition := range options.Registry.Definitions() {
		effective := baseSchedule
		if override, ok := options.Overrides[definition.Name]; ok {
			if override.ScanInterval < 0 || override.AsOfRefreshInterval < 0 || override.RebuildRetryInterval < 0 || override.MaxStaleness < 0 {
				return nil, fmt.Errorf("projection: negative schedule override for %s", definition.Name)
			}
			if override.ScanInterval > 0 {
				effective.ScanInterval = override.ScanInterval
			}
			if override.AsOfRefreshInterval > 0 {
				effective.AsOfRefreshInterval = override.AsOfRefreshInterval
			}
			if override.RebuildRetryInterval > 0 {
				effective.RebuildRetryInterval = override.RebuildRetryInterval
			}
			if override.MaxStaleness > 0 {
				effective.MaxStaleness = override.MaxStaleness
			}
		}
		schedules[definition.Name] = effective
	}
	for name := range options.Overrides {
		if _, err := options.Registry.Definition(name); err != nil {
			return nil, err
		}
	}
	var cohort []Definition
	claimStates, claimStatesRegistered := definitionByName(options.Registry.Definitions(), ClaimStatesName)
	runtimeStates, runtimeStatesRegistered := definitionByName(options.Registry.Definitions(), RuntimeStatesName)
	if claimStatesRegistered && runtimeStatesRegistered {
		if !claimStates.TimeSensitive || !runtimeStates.TimeSensitive {
			return nil, errors.New("projection: claim_states/runtime_states scheduling cohort requires both definitions to be time-sensitive")
		}
		// The order is part of the closed cohort contract, independent of the
		// Registry's general name ordering.
		cohort = []Definition{claimStates, runtimeStates}
	}
	capacity := options.NotificationCapacity
	if capacity == 0 {
		capacity = defaultNotificationCapacity
	}
	return &Coordinator{
		registry: options.Registry, source: options.Source, store: options.Store,
		clock: options.Clock, timezone: options.Timezone,
		schedules: schedules, cohort: cohort,
		onError: options.OnError, observer: options.Observer, notifications: make(chan notification, capacity),
		retries:   make(chan retrySignal, capacity),
		done:      make(chan struct{}),
		ready:     make(chan struct{}),
		retryStop: make(chan struct{}), locks: make(map[canonical.ID]*sync.Mutex),
		failures: make(map[targetKey]failure),
	}, nil
}

// Start schedules startup reconciliation asynchronously. Hosts that expose a
// service must additionally call WaitReady; command/test embedders retain the
// legacy asynchronous Start behavior.
func (coordinator *Coordinator) Start(parent context.Context) error {
	if parent == nil {
		return errors.New("projection: nil coordinator context")
	}
	coordinator.lifecycleMu.Lock()
	defer coordinator.lifecycleMu.Unlock()
	if coordinator.started {
		return errors.New("projection: coordinator was already started")
	}
	if coordinator.stopped {
		return ErrCoordinatorStopped
	}
	ctx, cancel := context.WithCancel(parent)
	coordinator.cancel = cancel
	coordinator.started = true
	go coordinator.run(ctx)
	return nil
}

// WaitReady joins the Coordinator's first all-resident reconciliation. It is
// the long-lived Coordinator initialization boundary used by RuntimeStartGate.
func (coordinator *Coordinator) WaitReady(ctx context.Context) error {
	if ctx == nil {
		return errors.New("projection: nil ready context")
	}
	coordinator.lifecycleMu.Lock()
	started := coordinator.started
	ready := coordinator.ready
	coordinator.lifecycleMu.Unlock()
	if !started {
		return errors.New("projection: coordinator has not started")
	}
	select {
	case <-ready:
		coordinator.lifecycleMu.Lock()
		err := coordinator.startupErr
		coordinator.lifecycleMu.Unlock()
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}

// NotifyCommit is non-blocking. False means the hint was rejected or dropped;
// the periodic full-resident scan will recover it.
func (coordinator *Coordinator) NotifyCommit(metadata canonical.CommitMetadata) bool {
	if err := metadata.Validate(); err != nil {
		return false
	}
	coordinator.lifecycleMu.Lock()
	stopped := coordinator.stopped
	coordinator.lifecycleMu.Unlock()
	if stopped {
		return false
	}
	message := notification{}
	if residentID, ok := metadata.Scope.ResidentID(); ok {
		message.residentID = &residentID
	}
	select {
	case coordinator.notifications <- message:
		return true
	default:
		return false
	}
}

func (coordinator *Coordinator) Stop() {
	coordinator.lifecycleMu.Lock()
	if !coordinator.stopped {
		coordinator.stopped = true
		if coordinator.cancel != nil {
			coordinator.cancel()
		}
	}
	coordinator.lifecycleMu.Unlock()
}

func (coordinator *Coordinator) Wait(ctx context.Context) error {
	if ctx == nil {
		return errors.New("projection: nil wait context")
	}
	coordinator.lifecycleMu.Lock()
	started := coordinator.started
	done := coordinator.done
	coordinator.lifecycleMu.Unlock()
	if !started {
		return errors.New("projection: coordinator has not started")
	}
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (coordinator *Coordinator) run(ctx context.Context) {
	defer close(coordinator.done)
	startupErr := coordinator.reconcile(ctx, nil, false, true, nil, true)
	coordinator.report(startupErr)
	coordinator.lifecycleMu.Lock()
	coordinator.startupErr = startupErr
	close(coordinator.ready)
	coordinator.lifecycleMu.Unlock()
	var schedules sync.WaitGroup
	for _, unit := range coordinator.scheduleUnits() {
		unit := unit
		schedules.Add(1)
		go func() {
			defer schedules.Done()
			coordinator.runSchedule(ctx, unit)
		}()
	}
	for {
		select {
		case <-ctx.Done():
			coordinator.lifecycleMu.Lock()
			coordinator.retryClosing = true
			coordinator.lifecycleMu.Unlock()
			coordinator.retryStopOnce.Do(func() { close(coordinator.retryStop) })
			schedules.Wait()
			coordinator.retryWG.Wait()
			return
		case message := <-coordinator.notifications:
			coordinator.report(coordinator.reconcile(ctx, message.residentID, false, true, nil, false))
		case retry := <-coordinator.retries:
			if !coordinator.retryIsCurrent(retry) {
				continue
			}
			residentID := retry.key.residentID
			name := retry.key.name
			coordinator.report(coordinator.reconcile(ctx, &residentID, false, false, &name, retry.refreshAsOf))
		}
	}
}

func (coordinator *Coordinator) runSchedule(ctx context.Context, unit scheduleUnit) {
	scanTicker := time.NewTicker(unit.scanInterval)
	asOfTicker := time.NewTicker(unit.asOfRefreshInterval)
	defer scanTicker.Stop()
	defer asOfTicker.Stop()
	name := unit.definitions[0].Name
	for {
		select {
		case <-ctx.Done():
			return
		case <-scanTicker.C:
			coordinator.report(coordinator.reconcile(ctx, nil, false, true, &name, false))
		case <-asOfTicker.C:
			coordinator.report(coordinator.reconcile(ctx, nil, true, true, &name, true))
		}
	}
}

func (coordinator *Coordinator) scheduleUnits() []scheduleUnit {
	definitions := coordinator.registry.Definitions()
	units := make([]scheduleUnit, 0, len(definitions))
	cohortAdded := false
	for _, definition := range definitions {
		if coordinator.isCohortMember(definition.Name) {
			if cohortAdded {
				continue
			}
			cohortAdded = true
			unit := scheduleUnit{definitions: append([]Definition(nil), coordinator.cohort...)}
			for _, member := range coordinator.cohort {
				policy := coordinator.schedules[member.Name]
				unit.scanInterval = minimumPositiveDuration(unit.scanInterval, policy.ScanInterval)
				unit.asOfRefreshInterval = minimumPositiveDuration(unit.asOfRefreshInterval, policy.AsOfRefreshInterval)
			}
			units = append(units, unit)
			continue
		}
		policy := coordinator.schedules[definition.Name]
		units = append(units, scheduleUnit{
			definitions: []Definition{definition}, scanInterval: policy.ScanInterval,
			asOfRefreshInterval: policy.AsOfRefreshInterval,
		})
	}
	return units
}

func minimumPositiveDuration(current, candidate time.Duration) time.Duration {
	if candidate <= 0 {
		return current
	}
	if current <= 0 || candidate < current {
		return candidate
	}
	return current
}

func definitionByName(definitions []Definition, name Name) (Definition, bool) {
	for _, definition := range definitions {
		if definition.Name == name {
			return definition, true
		}
	}
	return Definition{}, false
}

func (coordinator *Coordinator) report(err error) {
	if err != nil && !errors.Is(err, context.Canceled) && coordinator.onError != nil {
		coordinator.onError(err)
	}
}

// Reconcile captures one head and as_of and brings all registered resident
// projections toward that immutable target.
func (coordinator *Coordinator) Reconcile(ctx context.Context) error {
	return coordinator.reconcile(ctx, nil, false, false, nil, true)
}

func (coordinator *Coordinator) ReconcileResident(ctx context.Context, residentID canonical.ID) error {
	return coordinator.reconcile(ctx, &residentID, false, false, nil, true)
}

func (coordinator *Coordinator) reconcile(ctx context.Context, onlyResident *canonical.ID, onlyTimeSensitive, automatic bool, onlyDefinition *Name, refreshAsOf bool) error {
	if ctx == nil {
		return errors.New("projection: nil reconcile context")
	}
	residents, err := coordinator.resolveResidentsForReconcile(ctx, onlyResident)
	if err != nil {
		return err
	}
	var result error
	groups, err := coordinator.reconcileGroups(onlyDefinition)
	if err != nil {
		return err
	}
	for _, residentID := range residents {
		lock := coordinator.residentLock(residentID)
		lock.Lock()
		target, captureErr := coordinator.captureTarget(ctx)
		if captureErr != nil {
			lock.Unlock()
			result = errors.Join(result, captureErr)
			continue
		}
		for _, group := range groups {
			if onlyTimeSensitive && !group[0].TimeSensitive {
				continue
			}
			if automatic && coordinator.groupRetryPending(group, residentID) {
				continue
			}
			groupTarget := target
			exactGroupAsOf := false
			if coordinator.isCohortGroup(group) && !refreshAsOf {
				exactGroupAsOf = true
				groupTarget, err = coordinator.preservedCohortTarget(ctx, target, residentID, group)
				if err != nil {
					for _, definition := range group {
						coordinator.recordFailure(targetKey{name: definition.Name, residentID: residentID}, err, refreshAsOf)
					}
					result = errors.Join(result, fmt.Errorf("projection: resolve preserved cohort target/%s: %w", residentID, err))
					continue
				}
			}
			for _, definition := range group {
				key := targetKey{name: definition.Name, residentID: residentID}
				if err := coordinator.updateWithTarget(ctx, groupTarget, residentID, definition, false, refreshAsOf, exactGroupAsOf); err != nil {
					coordinator.recordFailure(key, err, refreshAsOf)
					result = errors.Join(result, fmt.Errorf("projection: reconcile %s/%s: %w", definition.Name, residentID, err))
				} else {
					coordinator.clearFailure(key)
				}
			}
		}
		lock.Unlock()
	}
	return result
}

func (coordinator *Coordinator) reconcileGroups(onlyDefinition *Name) ([][]Definition, error) {
	if onlyDefinition != nil {
		definition, err := coordinator.registry.Definition(*onlyDefinition)
		if err != nil {
			return nil, err
		}
		if coordinator.isCohortMember(definition.Name) {
			return [][]Definition{append([]Definition(nil), coordinator.cohort...)}, nil
		}
		return [][]Definition{{definition}}, nil
	}

	definitions := coordinator.registry.Definitions()
	groups := make([][]Definition, 0, len(definitions))
	cohortAdded := false
	for _, definition := range definitions {
		if coordinator.isCohortMember(definition.Name) {
			if !cohortAdded {
				groups = append(groups, append([]Definition(nil), coordinator.cohort...))
				cohortAdded = true
			}
			continue
		}
		groups = append(groups, []Definition{definition})
	}
	return groups, nil
}

func (coordinator *Coordinator) isCohortMember(name Name) bool {
	return len(coordinator.cohort) == 2 && (name == ClaimStatesName || name == RuntimeStatesName)
}

func (coordinator *Coordinator) isCohortGroup(definitions []Definition) bool {
	return len(definitions) == 2 && definitions[0].Name == ClaimStatesName && definitions[1].Name == RuntimeStatesName
}

func (coordinator *Coordinator) preservedCohortTarget(ctx context.Context, captured Target, residentID canonical.ID, definitions []Definition) (Target, error) {
	watermarks := make([]Watermark, 0, len(definitions))
	for _, definition := range definitions {
		watermark, exists, err := coordinator.store.Watermark(ctx, definition.Name, residentID)
		if err != nil {
			return Target{}, err
		}
		if !exists {
			continue
		}
		if err := watermark.Validate(); err != nil {
			return Target{}, err
		}
		watermarks = append(watermarks, watermark)
	}
	switch len(watermarks) {
	case 0:
		return captured, nil
	case 1:
		captured.AsOf = watermarks[0].AsOf
		captured.AsOfTZ = watermarks[0].AsOfTZ
		return captured, nil
	}
	if watermarks[0].AsOf == watermarks[1].AsOf && watermarks[0].AsOfTZ == watermarks[1].AsOfTZ {
		captured.AsOf = watermarks[0].AsOf
		captured.AsOfTZ = watermarks[0].AsOfTZ
		return captured, nil
	}

	// Skew is already readiness-blocking. Recover deterministically at the
	// greatest captured/stored as_of (so neither member can regress) and the
	// coordinator's configured timezone. Exact-target updates below also heal
	// the equal-as_of/different-timezone case.
	for _, watermark := range watermarks {
		if watermark.AsOf > captured.AsOf {
			captured.AsOf = watermark.AsOf
		}
	}
	captured.AsOfTZ = coordinator.timezone
	return captured, nil
}

func (coordinator *Coordinator) groupRetryPending(definitions []Definition, residentID canonical.ID) bool {
	for _, definition := range definitions {
		if coordinator.retryPending(targetKey{name: definition.Name, residentID: residentID}) {
			return true
		}
	}
	return false
}

// Rebuild forces one registered Projection to replace its resident body using
// the current head and current as_of. Rebuilding either member of the
// claim_states/runtime_states cohort rebuilds both members against one exact
// Target. It never permits a historic as_of write.
func (coordinator *Coordinator) Rebuild(ctx context.Context, residentID canonical.ID, name Name) error {
	if ctx == nil {
		return errors.New("projection: nil rebuild context")
	}
	definition, err := coordinator.registry.Definition(name)
	if err != nil {
		return err
	}
	if err := residentID.Validate(); err != nil {
		return err
	}
	definitions := []Definition{definition}
	if coordinator.isCohortMember(name) {
		definitions = append([]Definition(nil), coordinator.cohort...)
	}
	lock := coordinator.residentLock(residentID)
	lock.Lock()
	defer lock.Unlock()
	target, err := coordinator.captureTarget(ctx)
	if err != nil {
		return err
	}
	var result error
	for _, definition := range definitions {
		key := targetKey{name: definition.Name, residentID: residentID}
		if updateErr := coordinator.update(ctx, target, residentID, definition, true, true); updateErr != nil {
			coordinator.recordFailure(key, updateErr, true)
			if len(definitions) == 1 {
				return updateErr
			}
			result = errors.Join(result, fmt.Errorf(
				"projection: rebuild cohort member %s/%s: %w",
				definition.Name,
				residentID,
				updateErr,
			))
			continue
		}
		coordinator.clearFailure(key)
	}
	return result
}

func (coordinator *Coordinator) RebuildAll(ctx context.Context, residentID canonical.ID) error {
	if ctx == nil {
		return errors.New("projection: nil rebuild context")
	}
	if err := residentID.Validate(); err != nil {
		return err
	}
	lock := coordinator.residentLock(residentID)
	lock.Lock()
	defer lock.Unlock()
	target, err := coordinator.captureTarget(ctx)
	if err != nil {
		return err
	}
	var result error
	for _, definition := range coordinator.registry.Definitions() {
		key := targetKey{name: definition.Name, residentID: residentID}
		if err := coordinator.update(ctx, target, residentID, definition, true, true); err != nil {
			coordinator.recordFailure(key, err, true)
			result = errors.Join(result, fmt.Errorf("projection: rebuild %s/%s: %w", definition.Name, residentID, err))
		} else {
			coordinator.clearFailure(key)
		}
	}
	return result
}

func (coordinator *Coordinator) update(ctx context.Context, target Target, residentID canonical.ID, definition Definition, force, refreshAsOf bool) (returnErr error) {
	return coordinator.updateWithTarget(ctx, target, residentID, definition, force, refreshAsOf, false)
}

func (coordinator *Coordinator) updateWithTarget(ctx context.Context, target Target, residentID canonical.ID, definition Definition, force, refreshAsOf, exactAsOf bool) (returnErr error) {
	started := time.Now()
	var commitLag uint64
	rebuilt := false
	if target.Head.Exists {
		commitLag = uint64(target.Head.CommitSeq.Int64())
	}
	defer func() {
		if coordinator.observer != nil {
			coordinator.observer.ProjectionObserved(commitLag, time.Since(started), rebuilt, returnErr != nil)
		}
	}()
	for attempt := 0; attempt < 2; attempt++ {
		stored, exists, err := coordinator.store.Watermark(ctx, definition.Name, residentID)
		if err != nil {
			return err
		}
		var previous *Watermark
		if exists {
			copyWatermark := stored.clone()
			previous = &copyWatermark
			if target.Head.Exists && stored.SourceCommitSeq < target.Head.CommitSeq {
				commitLag = uint64(target.Head.CommitSeq.Int64() - stored.SourceCommitSeq.Int64())
			} else {
				commitLag = 0
			}
			if definition.TimeSensitive && !refreshAsOf && !exactAsOf {
				target.AsOf = previous.AsOf
				target.AsOfTZ = previous.AsOfTZ
			}
		}
		input := PlanInput{Definition: definition, ResidentID: residentID, Target: target, Stored: previous}
		dependencies, err := coordinator.source.ResolveDependencies(ctx, DependencyRequest{
			Definition: definition, ResidentID: residentID, Target: target, Previous: previous,
		})
		if err != nil {
			return err
		}
		input.DesiredDependencies = dependencies
		plan, err := coordinator.plan(ctx, input)
		if err != nil {
			return err
		}
		if force {
			plan = UpdatePlan{Kind: FullRebuild, Reason: ReasonManualRebuild, NeedCommitCatchUp: true, NeedAsOfReEvaluation: definition.TimeSensitive}
			if previous == nil {
				plan.Kind = FullBuild
			}
		}
		if exactAsOf && previous != nil && definition.TimeSensitive && previous.AsOf == target.AsOf && previous.AsOfTZ != target.AsOfTZ {
			switch plan.Kind {
			case UpToDate:
				plan = UpdatePlan{Kind: Update, Reason: ReasonAsOfAdvance, NeedAsOfReEvaluation: true}
			case Update:
				plan.NeedAsOfReEvaluation = true
				if plan.NeedCommitCatchUp {
					plan.Reason = ReasonCursorAndAsOfAdvance
				} else {
					plan.Reason = ReasonAsOfAdvance
				}
			}
		}
		if plan.Kind == FullBuild || plan.Kind == FullRebuild {
			rebuilt = true
		}
		if !plan.ChangesState() {
			return nil
		}
		request := EvaluationRequest{
			Definition: definition, ResidentID: residentID, Target: target, Previous: cloneWatermarkPointer(previous),
			Dependencies: append([]Dependency(nil), dependencies...), Plan: plan,
		}
		evaluation, err := coordinator.source.Evaluate(ctx, request)
		if err != nil {
			return err
		}
		input.Stored = previous
		watermark := targetWatermark(input, plan)
		if err := watermark.Validate(); err != nil {
			return err
		}
		err = coordinator.store.Apply(ctx, ApplyRequest{
			Definition: definition, ResidentID: residentID, Observed: cloneWatermarkPointer(previous),
			Plan: plan, Evaluation: evaluation, Watermark: watermark,
		})
		if err == nil {
			return nil
		}
		if !errors.Is(err, ErrCASConflict) || attempt == 1 {
			return err
		}
	}
	return ErrCASConflict
}

func (coordinator *Coordinator) plan(ctx context.Context, input PlanInput) (UpdatePlan, error) {
	plan, err := PlanUpdate(input)
	if err != nil || plan.Kind == FullBuild || plan.Kind == FullRebuild || input.Stored == nil {
		return plan, err
	}
	if input.Stored.SourceCommitSeq >= input.Target.Head.CommitSeq {
		return plan, nil
	}
	for _, kind := range input.Definition.RebuildOnActivation {
		activated, err := coordinator.source.DependencyActivated(ctx, ActivationRequest{
			Definition: input.Definition, Kind: kind, ResidentID: input.ResidentID,
			After: input.Stored.SourceCommitSeq, Through: input.Target.Head.CommitSeq,
		})
		if err != nil {
			return UpdatePlan{}, err
		}
		if activated {
			input.ActivationDetected = true
			return PlanUpdate(input)
		}
	}
	return plan, nil
}

func (coordinator *Coordinator) captureTarget(ctx context.Context) (Target, error) {
	head, err := coordinator.source.Head(ctx)
	if err != nil {
		return Target{}, fmt.Errorf("projection: capture Canonical head: %w", err)
	}
	// A Canonical Writer may advance same-clock commits by a microsecond to
	// preserve a strict ledger order.  Never evaluate a target whose as_of
	// predates the head it claims to include: activation-scoped dependencies
	// (notably memory-policy-v4) would otherwise be invisible at that target.
	asOf := canonical.InstantFromTime(coordinator.clock.Now())
	if head.Exists && head.CommittedAt > asOf {
		asOf = head.CommittedAt
	}
	target := Target{
		Head: head, AsOf: asOf, AsOfTZ: coordinator.timezone,
	}
	if err := target.Validate(); err != nil {
		return Target{}, err
	}
	return target, nil
}

func (coordinator *Coordinator) resolveResidents(ctx context.Context, target Target, only *canonical.ID) ([]canonical.ID, error) {
	if only != nil {
		if err := only.Validate(); err != nil {
			return nil, err
		}
		return []canonical.ID{*only}, nil
	}
	residents, err := coordinator.source.Residents(ctx, target)
	if err != nil {
		return nil, fmt.Errorf("projection: list residents: %w", err)
	}
	seen := make(map[canonical.ID]struct{}, len(residents))
	for _, residentID := range residents {
		if err := residentID.Validate(); err != nil {
			return nil, fmt.Errorf("projection: source returned invalid resident: %w", err)
		}
		if _, exists := seen[residentID]; exists {
			return nil, fmt.Errorf("projection: source returned duplicate resident %s", residentID)
		}
		seen[residentID] = struct{}{}
	}
	slices.SortFunc(residents, func(left, right canonical.ID) int {
		if left.String() < right.String() {
			return -1
		}
		if left.String() > right.String() {
			return 1
		}
		return 0
	})
	return residents, nil
}

func (coordinator *Coordinator) resolveResidentsForReconcile(ctx context.Context, only *canonical.ID) ([]canonical.ID, error) {
	if only != nil {
		return coordinator.resolveResidents(ctx, Target{}, only)
	}
	target, err := coordinator.captureTarget(ctx)
	if err != nil {
		return nil, err
	}
	return coordinator.resolveResidents(ctx, target, nil)
}

func (coordinator *Coordinator) residentLock(residentID canonical.ID) *sync.Mutex {
	coordinator.locksMu.Lock()
	defer coordinator.locksMu.Unlock()
	lock := coordinator.locks[residentID]
	if lock == nil {
		lock = &sync.Mutex{}
		coordinator.locks[residentID] = lock
	}
	return lock
}

func (coordinator *Coordinator) retryPending(key targetKey) bool {
	coordinator.failuresMu.RLock()
	failure, exists := coordinator.failures[key]
	coordinator.failuresMu.RUnlock()
	return exists && coordinator.clock.Now().Before(failure.retryAt)
}

func (coordinator *Coordinator) recordFailure(key targetKey, err error, refreshAsOf bool) {
	delay := coordinator.schedules[key.name].RebuildRetryInterval
	retryAt := coordinator.clock.Now().Add(delay)
	coordinator.failuresMu.Lock()
	coordinator.failures[key] = failure{err: err, retryAt: retryAt, refreshAsOf: refreshAsOf}
	coordinator.failuresMu.Unlock()

	coordinator.lifecycleMu.Lock()
	started, stopped, retryClosing := coordinator.started, coordinator.stopped, coordinator.retryClosing
	if !started || stopped || retryClosing {
		coordinator.lifecycleMu.Unlock()
		return
	}
	coordinator.retryWG.Add(1)
	coordinator.lifecycleMu.Unlock()
	go func(signal retrySignal) {
		defer coordinator.retryWG.Done()
		timer := time.NewTimer(delay)
		defer timer.Stop()
		select {
		case <-timer.C:
			select {
			case coordinator.retries <- signal:
			case <-coordinator.retryStop:
			}
		case <-coordinator.retryStop:
		}
	}(retrySignal{key: key, retryAt: retryAt, refreshAsOf: refreshAsOf})
}

func (coordinator *Coordinator) retryIsCurrent(signal retrySignal) bool {
	coordinator.failuresMu.RLock()
	current, exists := coordinator.failures[signal.key]
	coordinator.failuresMu.RUnlock()
	return exists && current.retryAt.Equal(signal.retryAt) && current.refreshAsOf == signal.refreshAsOf
}

func (coordinator *Coordinator) clearFailure(key targetKey) {
	coordinator.failuresMu.Lock()
	delete(coordinator.failures, key)
	coordinator.failuresMu.Unlock()
}

func cloneWatermarkPointer(watermark *Watermark) *Watermark {
	if watermark == nil {
		return nil
	}
	copyWatermark := watermark.clone()
	return &copyWatermark
}
