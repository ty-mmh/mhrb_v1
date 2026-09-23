package app

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"sort"
	"strings"
	"sync"

	"mahoroba.local/mahoroba/internal/canonical"
	"mahoroba.local/mahoroba/internal/domain"
)

type BootstrapInput struct {
	OwnerName  string
	Name       string
	SeedKey    string
	Principles string
}

func (a *Application) BootstrapInit(ctx context.Context, input BootstrapInput) (domain.BootstrapState, error) {
	if input.OwnerName == "" || input.Name == "" || input.SeedKey == "" || input.Principles == "" {
		return domain.BootstrapState{}, errors.New("app: bootstrap owner, resident name, seed, and principles are required")
	}
	state, err := a.repository.BootstrapSnapshot(ctx)
	if err != nil {
		return domain.BootstrapState{}, err
	}
	if !state.Initialized {
		ids, err := a.allocateIDs(4)
		if err != nil {
			return domain.BootstrapState{}, err
		}
		pipeline, err := canonical.MarshalCanonical(struct {
			Version string `json:"version"`
			Purpose string `json:"purpose"`
		}{Version: domain.DialoguePipelineVersionV4, Purpose: "dialogue"})
		if err != nil {
			return domain.BootstrapState{}, err
		}
		session, err := canonical.MarshalCanonical(struct {
			Version string             `json:"version"`
			IdleGap canonical.Duration `json:"idle_gap_microseconds"`
		}{Version: domain.SessionPolicyVersion, IdleGap: canonical.Duration((30 * 60 * 1_000_000))})
		if err != nil {
			return domain.BootstrapState{}, err
		}
		_, err = a.submit(ctx, domain.GlobalBootstrapCommand(domain.GlobalBootstrap{
			SystemPrincipalID: ids[0], OwnerPrincipalID: ids[1], OwnerDisplayName: input.OwnerName,
			PipelineVersionID: ids[2], PipelineDefinition: pipeline,
			SessionPolicyID: ids[3], SessionDefinition: session,
		}))
		if err != nil {
			return domain.BootstrapState{}, fmt.Errorf("app: global bootstrap: %w", err)
		}
		state, err = a.repository.BootstrapSnapshot(ctx)
		if err != nil {
			return domain.BootstrapState{}, err
		}
	}
	if state.OwnerDisplayName != input.OwnerName {
		return domain.BootstrapState{}, errors.New("app: bootstrap retry conflicts with the initialized owner")
	}
	for _, resident := range state.Residents {
		if resident.Name == input.Name && resident.SeedKey == input.SeedKey {
			if resident.Principles != input.Principles {
				return domain.BootstrapState{}, errors.New("app: bootstrap retry conflicts with the initialized principles")
			}
			return state, nil
		}
	}
	{
		ids, err := a.allocateIDs(4)
		if err != nil {
			return domain.BootstrapState{}, err
		}
		content, err := a.newContent(ids[0], "principles_text", []byte(input.Principles), "resident_only")
		if err != nil {
			return domain.BootstrapState{}, err
		}
		_, err = a.submitWithContent(ctx, domain.DraftResidentCommand(domain.DraftResident{
			ResidentID: ids[0], ResidentPrincipalID: ids[1], OwnerPrincipalID: state.OwnerPrincipalID,
			Name: input.Name, SeedKey: input.SeedKey, PrinciplesRevisionID: ids[2],
			PrinciplesContent: content, StatusTransitionID: ids[3],
		}), []domain.Content{content})
		if err != nil {
			return domain.BootstrapState{}, fmt.Errorf("app: draft resident bootstrap: %w", err)
		}
	}
	return a.repository.BootstrapSnapshot(ctx)
}

func (a *Application) ApprovePrinciples(ctx context.Context, residentID canonical.ID) error {
	state, err := a.repository.BootstrapSnapshot(ctx)
	if err != nil {
		return err
	}
	resident, err := a.repository.Resident(ctx, residentID)
	if err != nil {
		return err
	}
	if resident.Status != "draft" && resident.Status != "active" {
		return fmt.Errorf("app: principles approval requires draft or already-active resident, got %s", resident.Status)
	}
	ids, err := a.allocateIDs(2)
	if err != nil {
		return err
	}
	// The UoW performs the authoritative lifecycle/owner/idempotency check. An
	// identical durable retry is rolled back as a successful no-op by Writer.
	_, err = a.submit(ctx, domain.ApprovePrinciplesCommand(domain.ApprovePrinciples{
		ResidentID: residentID, RevisionID: resident.PrinciplesRevisionID,
		OwnerPrincipalID: state.OwnerPrincipalID, ApprovalID: ids[0], ActivationID: ids[1],
	}))
	return err
}

func (a *Application) FinalizeBootstrap(ctx context.Context, residentID canonical.ID, persona, memoryPolicy string) error {
	if persona == "" || memoryPolicy == "" {
		return errors.New("app: persona and memory policy are required")
	}
	if err := validateInitialMemoryPolicy(memoryPolicy); err != nil {
		return err
	}
	state, err := a.repository.BootstrapSnapshot(ctx)
	if err != nil {
		return err
	}
	resident, err := a.repository.Resident(ctx, residentID)
	if err != nil {
		return err
	}
	switch resident.Status {
	case "active":
		if resident.Persona != persona || !sameInitialMemoryPolicy(resident.MemoryPolicy, memoryPolicy) {
			return errors.New("app: bootstrap already finalized with a conflicting persona or memory policy")
		}
	case "draft":
		// The Canonical UoW below remains authoritative if a concurrent
		// finalization wins after this read.
	default:
		return fmt.Errorf("app: finalize requires draft or identical already-active state, got %s", resident.Status)
	}
	ids, err := a.allocateIDs(5)
	if err != nil {
		return err
	}
	personaContent, err := a.newContent(residentID, "persona_text", []byte(persona), "resident_only")
	if err != nil {
		return err
	}
	memoryContent, err := a.newContent(residentID, "memory_policy_text", []byte(memoryPolicy), "resident_only")
	if err != nil {
		return err
	}
	// Finalization payload equality is deliberately checked in the UoW. This
	// keeps a retry safe even if another lifecycle command commits after the
	// application-level snapshot above.
	command := domain.FinalizeResidentCommand(domain.FinalizeResident{
		ResidentID: residentID, OwnerPrincipalID: state.OwnerPrincipalID,
		PersonaRevisionID: ids[0], PersonaActivationID: ids[1], PersonaContent: personaContent,
		MemoryRevisionID: ids[2], MemoryActivationID: ids[3], MemoryContent: memoryContent,
		StatusTransitionID: ids[4],
	})
	if resident.Status == "active" {
		// The durable definitions already own their physical blobs. Submit only
		// the logical command so the UoW can recheck lifecycle and equality
		// without publishing an unreferenced candidate object.
		_, err = a.submit(ctx, command)
		return err
	}
	_, err = a.submitWithContent(ctx, command, []domain.Content{personaContent, memoryContent})
	return err
}

func sameInitialMemoryPolicy(durable, candidate string) bool {
	durableJSON, durableErr := canonical.CanonicalizeRFC8785([]byte(durable))
	candidateJSON, candidateErr := canonical.CanonicalizeRFC8785([]byte(candidate))
	return durableErr == nil && candidateErr == nil && bytes.Equal(durableJSON.Bytes(), candidateJSON.Bytes())
}

func validateInitialMemoryPolicy(raw string) error {
	if !json.Valid([]byte(raw)) {
		return errors.New("app: initial memory policy must be valid JSON")
	}
	decoder := json.NewDecoder(strings.NewReader(raw))
	decoder.DisallowUnknownFields()
	var policy struct {
		MandatoryEventTypes []string `json:"mandatory_event_types"`
		MemoryRecallEnabled bool     `json:"memory_recall_enabled"`
		Version             string   `json:"version"`
	}
	if err := decoder.Decode(&policy); err != nil {
		return fmt.Errorf("app: decode initial memory policy: %w", err)
	}
	if err := requireJSONEnd(decoder); err != nil {
		return fmt.Errorf("app: decode initial memory policy: %w", err)
	}
	var fields map[string]json.RawMessage
	if err := json.Unmarshal([]byte(raw), &fields); err != nil {
		return fmt.Errorf("app: decode initial memory policy fields: %w", err)
	}
	for _, required := range []string{"mandatory_event_types", "memory_recall_enabled", "version"} {
		if _, ok := fields[required]; !ok {
			return fmt.Errorf("app: initial memory policy is missing %s", required)
		}
	}
	if len(policy.MandatoryEventTypes) != 0 || policy.MemoryRecallEnabled || policy.Version != "memory-policy-v1" {
		return errors.New("app: M3 requires memory-policy-v1 with zero mandatory event types and recall disabled")
	}
	return nil
}

func requireJSONEnd(decoder *json.Decoder) error {
	var extra any
	err := decoder.Decode(&extra)
	if errors.Is(err, io.EOF) {
		return nil
	}
	if err == nil {
		return errors.New("additional JSON value")
	}
	return err
}

func (a *Application) SelectResident(ctx context.Context, residentID canonical.ID) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	a.residentBoundaryMu.Lock()
	defer a.residentBoundaryMu.Unlock()

	// Close admission and invalidate every possibly affected Operational epoch
	// before waiting for providers which are inside their narrow landing lock. The
	// durable selection transaction starts only after those landings have drained,
	// so an old-resident provider cannot land on the far side of unselection.
	// A missing former selection is expected during bootstrap; in that case the
	// global selection fence rejects registrations until the post-transaction
	// selected resident can be resolved.
	previous, previousErr := a.repository.ActiveResident(ctx)
	targets := map[canonical.ID]struct{}{residentID: {}}
	if previousErr == nil {
		targets[previous.ResidentID] = struct{}{}
	}
	a.backgroundMu.Lock()
	if a.backgroundBlocked == nil {
		a.backgroundBlocked = make(map[canonical.ID]bool)
	}
	if previousErr != nil {
		a.backgroundSelectionBlocked = true
		// A provider removes its registration before post-processing and landing.
		// Snapshot every resident known to the serialized worker map while global
		// admission is closed so an unresolved former selection cannot omit its
		// narrow landing boundary.
		a.workMu.Lock()
		for target := range a.work {
			targets[target] = struct{}{}
		}
		a.workMu.Unlock()
		for target := range a.background {
			targets[target] = struct{}{}
		}
	}
	for target := range targets {
		if target.IsZero() {
			continue
		}
		a.backgroundBlocked[target] = true
		a.invalidateResidentRuntimeLocked(target)
	}
	a.backgroundMu.Unlock()

	unlockLandings := a.lockBackgroundLandingTargets(targets)
	defer unlockLandings()

	selectionErr := a.repository.SelectActiveResident(
		ctx, residentID, canonical.InstantFromTime(a.clock.Now()), a.timezone,
	)
	var selectedAfter canonical.ID
	selectedKnown := selectionErr == nil
	if selectedKnown {
		selectedAfter = residentID
	} else if current, err := a.repository.ActiveResident(context.WithoutCancel(ctx)); err == nil {
		selectedAfter = current.ResidentID
		selectedKnown = true
	}

	// A selected resident may accept fresh work after the transaction result is
	// authoritative. Non-selected targets stay blocked until a later successful
	// selection, and an unknown result retains the global fail-closed fence.
	a.backgroundMu.Lock()
	if selectedKnown {
		a.backgroundSelectionBlocked = false
		for target := range targets {
			if target == selectedAfter {
				delete(a.backgroundBlocked, target)
			} else if !target.IsZero() {
				a.backgroundBlocked[target] = true
			}
		}
		delete(a.backgroundBlocked, selectedAfter)
	}
	a.backgroundMu.Unlock()
	return selectionErr
}

func (a *Application) ArchiveResident(ctx context.Context, residentID canonical.ID) error {
	if _, ok := a.repository.(CancellationEnvelopeResolver); !ok {
		return errors.New("app: archive requires source-time mandatory cancellation capability")
	}
	if _, ok := a.repository.(ResidentRunningAttemptRepository); !ok {
		return errors.New("app: archive requires resident-scoped running-attempt discovery")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	a.residentBoundaryMu.Lock()

	// Close provider admission before waiting for the landing lock. Existing
	// providers observe the epoch advance or finish landing before the archive;
	// new fair and optional leases remain rejected for the entire boundary.
	a.backgroundMu.Lock()
	if a.backgroundBlocked == nil {
		a.backgroundBlocked = make(map[canonical.ID]bool)
	}
	wasBlocked := a.backgroundBlocked[residentID]
	a.backgroundBlocked[residentID] = true
	a.invalidateResidentRuntimeLocked(residentID)
	a.backgroundMu.Unlock()

	landing := a.backgroundLandingLock(residentID)
	landing.Lock()
	// Once the hard boundary starts, wait for the Writer's authoritative result
	// even if the caller is cancelled. Writer.Submit may otherwise return while
	// an admitted command is still capable of committing after the fence opens.
	hardCtx := context.WithoutCancel(ctx)
	resident, residentErr := a.repository.Resident(hardCtx, residentID)
	alreadyArchived := residentErr == nil && resident.Status == "archived"
	var result canonical.CommandResult
	var archiveErr error
	submitted := false
	if !alreadyArchived {
		if residentErr != nil {
			archiveErr = residentErr
		} else {
			transitionID, err := a.ids.New()
			if err != nil {
				archiveErr = err
			} else {
				command := domain.ArchiveResidentCommand(domain.ArchiveResident{
					ResidentID: residentID, OwnerPrincipalID: resident.OwnerPrincipalID,
					StatusTransitionID: transitionID,
				})
				submitted = true
				result, archiveErr = a.writer.Submit(hardCtx, command)
			}
		}
	}
	archived := alreadyArchived || (submitted && archiveErr == nil)
	if archiveErr != nil {
		authoritative, statusErr := a.repository.Resident(hardCtx, residentID)
		if statusErr == nil && authoritative.Status == "archived" {
			archived = true
		} else if statusErr == nil {
			a.backgroundMu.Lock()
			if wasBlocked {
				a.backgroundBlocked[residentID] = true
			} else {
				delete(a.backgroundBlocked, residentID)
			}
			a.backgroundMu.Unlock()
		}
	}
	landing.Unlock()
	// The durable archive result and admission fence are now authoritative.
	// Release lifecycle serialization before arbitrary notification callbacks
	// and cleanup submits, which do not participate in the commit boundary.
	a.residentBoundaryMu.Unlock()
	if submitted {
		a.afterSubmit(result, archiveErr, residentID, true)
	}
	if !archived {
		return archiveErr
	}

	cleanupErr := a.completeArchivedResident(hardCtx, residentID)
	if archiveErr != nil && cleanupErr != nil {
		return errors.Join(archiveErr, cleanupErr)
	}
	if archiveErr != nil {
		return archiveErr
	}
	return cleanupErr
}

func (a *Application) completeArchivedResident(ctx context.Context, residentID canonical.ID) error {
	// Archive is a hard generation boundary. Drain only this resident's bounded
	// inventories without reacquiring residentLock: Observer and metrics
	// callbacks may invoke ArchiveResident synchronously while their outer turn
	// already owns it. Production repositories expose the source-time resolver
	// needed to synthesize runless cancellation envelopes. Minimal legacy
	// adapters remain source-compatible and can still terminalize durable runs;
	// they simply cannot invent an absent historical envelope.
	_, canCancelRunless := a.repository.(CancellationEnvelopeResolver)
	if err := a.cancelArchivedResidentRunningAttempts(ctx, residentID); err != nil {
		return err
	}
	if err := a.cancelArchivedResidentDialogues(ctx, residentID, canCancelRunless); err != nil {
		return err
	}
	if err := a.cancelArchivedResidentMemoryExtractions(ctx, residentID, canCancelRunless); err != nil {
		return err
	}
	if err := a.cancelArchivedResidentMemoryReextractions(ctx, residentID); err != nil {
		return err
	}
	return a.cancelAutonomousResidentWork(ctx, residentID)
}

func (a *Application) lockBackgroundLandingTargets(targets map[canonical.ID]struct{}) func() {
	residentIDs := make([]canonical.ID, 0, len(targets))
	for residentID := range targets {
		if !residentID.IsZero() {
			residentIDs = append(residentIDs, residentID)
		}
	}
	sort.Slice(residentIDs, func(left, right int) bool {
		return residentIDs[left].String() < residentIDs[right].String()
	})
	locks := make([]*sync.Mutex, 0, len(residentIDs))
	for _, residentID := range residentIDs {
		lock := a.backgroundLandingLock(residentID)
		lock.Lock()
		locks = append(locks, lock)
	}
	return func() {
		for index := len(locks) - 1; index >= 0; index-- {
			locks[index].Unlock()
		}
	}
}

func (a *Application) ListResidents(ctx context.Context) ([]domain.ResidentSnapshot, error) {
	return a.repository.ListResidents(ctx)
}

func (a *Application) Resident(ctx context.Context, residentID canonical.ID) (domain.ResidentSnapshot, error) {
	return a.repository.Resident(ctx, residentID)
}

func (a *Application) allocateIDs(count int) ([]canonical.ID, error) {
	ids := make([]canonical.ID, count)
	for index := range ids {
		id, err := a.ids.New()
		if err != nil {
			return nil, err
		}
		ids[index] = id
	}
	return ids, nil
}
