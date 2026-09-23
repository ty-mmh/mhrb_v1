package domain

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"unicode/utf8"

	"mahoroba.local/mahoroba/internal/canonical"
)

// ErrPrepareDialogueInactive is returned by persistence adapters until the
// readiness-gated COV cutover connects PrepareDialogue to production writes.
var ErrPrepareDialogueInactive = errors.New("domain: PrepareDialogue is inactive")

// AssemblyTarget is the Canonical/time observation captured once at the
// beginning of one dialogue assembly attempt. It deliberately lives in the
// domain package rather than aliasing projection.Target or a memory type.
type AssemblyTarget struct {
	Head   canonical.Head
	AsOf   canonical.Instant
	AsOfTZ canonical.Timezone
}

func (target AssemblyTarget) Validate() error {
	if err := target.Head.Validate(); err != nil {
		return fmt.Errorf("domain: invalid dialogue Assembly head: %w", err)
	}
	if !target.Head.Exists {
		return errors.New("domain: dialogue Assembly requires a Canonical head")
	}
	if target.AsOf < target.Head.CommittedAt {
		return errors.New("domain: dialogue Assembly as_of precedes its Canonical head")
	}
	if err := target.AsOfTZ.Validate(); err != nil {
		return fmt.Errorf("domain: invalid dialogue Assembly timezone: %w", err)
	}
	return nil
}

// RecallDisposition is the exactly-one branch selected for a normal dialogue
// Prepare commit. A successful query remains success when it found no claims.
type RecallDisposition string

const (
	RecallDispositionSuccess          RecallDisposition = "success"
	RecallDispositionPolicyDisabled   RecallDisposition = "policy_disabled"
	RecallDispositionExplicitFallback RecallDisposition = "explicit_fallback"
)

func (disposition RecallDisposition) Validate() error {
	switch disposition {
	case RecallDispositionSuccess, RecallDispositionPolicyDisabled, RecallDispositionExplicitFallback:
		return nil
	default:
		return fmt.Errorf("domain: invalid dialogue Recall disposition %q", disposition)
	}
}

// RecallCandidateSnapshot is the domain-owned, memory-package-neutral input
// needed by the Writer revalidation boundary. Stage, Status, and
// TemporalRelation retain their stable wire values without importing memory;
// Currentness and LastConfirmed freeze the structured rendering inputs.
type RecallCandidateSnapshot struct {
	ClaimID              canonical.ID
	Statement            string
	ContextCompatibility canonical.Ratio
	Salience             canonical.Ratio
	Confidence           canonical.Ratio
	Currentness          canonical.Ratio
	LastConfirmed        *canonical.Instant
	Status               string
	Stage                string
	TemporalRelation     string
	SourceEventIDs       []canonical.ID
	Abstract             bool
}

func (candidate RecallCandidateSnapshot) Validate() error {
	if err := candidate.ClaimID.Validate(); err != nil {
		return fmt.Errorf("domain: invalid dialogue Recall candidate claim: %w", err)
	}
	if candidate.Statement == "" || !utf8.ValidString(candidate.Statement) {
		return errors.New("domain: dialogue Recall candidate statement must be non-empty UTF-8")
	}
	for label, value := range map[string]canonical.Ratio{
		"context compatibility": candidate.ContextCompatibility,
		"salience":              candidate.Salience,
		"confidence":            candidate.Confidence,
		"currentness":           candidate.Currentness,
	} {
		if err := value.Validate(); err != nil {
			return fmt.Errorf("domain: invalid dialogue Recall candidate %s: %w", label, err)
		}
	}
	if !oneOf(candidate.Status, "active", "invalidated", "superseded", "quarantined") {
		return fmt.Errorf("domain: invalid dialogue Recall candidate status %q", candidate.Status)
	}
	if !oneOf(candidate.Stage, "floating", "sediment", "settled") {
		return fmt.Errorf("domain: invalid dialogue Recall candidate stage %q", candidate.Stage)
	}
	if !oneOf(candidate.TemporalRelation, "future", "current", "past", "stale_unknown") {
		return fmt.Errorf("domain: invalid dialogue Recall candidate temporal relation %q", candidate.TemporalRelation)
	}
	if len(candidate.SourceEventIDs) == 0 {
		return errors.New("domain: dialogue Recall candidate requires source provenance")
	}
	seen := make(map[canonical.ID]struct{}, len(candidate.SourceEventIDs))
	for _, sourceID := range candidate.SourceEventIDs {
		if err := sourceID.Validate(); err != nil {
			return fmt.Errorf("domain: invalid dialogue Recall candidate source event: %w", err)
		}
		if _, duplicate := seen[sourceID]; duplicate {
			return fmt.Errorf("domain: duplicate dialogue Recall candidate source event %s", sourceID)
		}
		seen[sourceID] = struct{}{}
	}
	return nil
}

type PrepareDialogue struct {
	SourceEventID        canonical.ID
	Target               AssemblyTarget
	MaxInputBytes        int64
	LiveEventLimit       int64
	Generation           PrepareGeneration
	RecallDisposition    RecallDisposition
	RecallFallbackReason MemoryRecallFallbackReason
	Recall               *DialogueRecall
	RecallCandidates     []RecallCandidateSnapshot
}

// PrepareDialogueResolution identifies how the Commit-B writer resolved a
// dialogue obligation. Only current-v3 preparations are idempotent replays;
// exact v1/v2 runs are immutable historical work that must be dispatched
// through the persisted frozen-run path instead of compared with a new plan.
type PrepareDialogueResolution string

const (
	PrepareDialoguePreparedCurrentV3         PrepareDialogueResolution = "prepared_current_v3"
	PrepareDialogueExistingCurrentV3         PrepareDialogueResolution = "existing_current_v3"
	PrepareDialogueDispatchExistingFrozenRun PrepareDialogueResolution = "dispatch_existing_frozen_run"
)

func (resolution PrepareDialogueResolution) Validate() error {
	switch resolution {
	case PrepareDialoguePreparedCurrentV3,
		PrepareDialogueExistingCurrentV3,
		PrepareDialogueDispatchExistingFrozenRun:
		return nil
	default:
		return fmt.Errorf("domain: unsupported PrepareDialogue resolution %q", resolution)
	}
}

type PrepareDialogueResult struct {
	RunID      canonical.ID
	Resolution PrepareDialogueResolution
}

func (result PrepareDialogueResult) Validate() error {
	if err := result.RunID.Validate(); err != nil {
		return fmt.Errorf("domain: invalid prepared dialogue run: %w", err)
	}
	return result.Resolution.Validate()
}

func PrepareDialogueCommand(value PrepareDialogue) canonical.Command {
	scope, _ := canonical.ResidentScope(value.Generation.ResidentID)
	return command{
		name:  "PrepareDialogue",
		scope: scope,
		validate: func() error {
			return validatePrepareDialogue(value)
		},
		execute: func(ctx context.Context, store MutationStore) (any, error) {
			return store.PrepareDialogue(ctx, value)
		},
	}
}

func validatePrepareDialogue(value PrepareDialogue) error {
	if err := value.SourceEventID.Validate(); err != nil {
		return fmt.Errorf("domain: invalid dialogue source event: %w", err)
	}
	if err := value.Target.Validate(); err != nil {
		return err
	}
	if err := value.RecallDisposition.Validate(); err != nil {
		return err
	}
	if value.MaxInputBytes < 1 || value.LiveEventLimit != DialogueLiveEventLimit {
		return errors.New("domain: PrepareDialogue requires fixed positive Assembly limits")
	}

	generation := value.Generation
	purpose := generation.Purpose.Effective()
	if err := purpose.Validate(); err != nil {
		return err
	}
	if purpose != GenerationPurposeDialogue {
		return errors.New("domain: PrepareDialogue requires dialogue purpose")
	}
	if generation.IdempotencyKey != DialogueObligation(value.SourceEventID) {
		return errors.New("domain: PrepareDialogue obligation key does not match its source event")
	}
	if generation.Provider == "" || generation.Model == "" || generation.GeneratorParams.IsZero() ||
		generation.DroppedInputSummary.IsZero() || len(generation.Inputs) == 0 || len(generation.Inputs) > MaxDialogueInputs {
		return errors.New("domain: incomplete PrepareDialogue generation envelope")
	}
	params, _, err := ParseGeneratorParams(generation.GeneratorParams.Bytes())
	if err != nil {
		return err
	}
	if err := ValidateNewGeneratorParamsForPurpose(GenerationPurposeDialogue, params); err != nil {
		return err
	}
	if err := ValidateNewDialogueNormalExecutionContract(DialogueExecutionContract{
		PipelineVersionKey:     DialoguePipelineVersionV3,
		PromptTemplateVersion:  generation.PromptTemplateVersion,
		ContextPolicyVersion:   generation.ContextPolicyVersion,
		MemoryRenderingVersion: generation.MemoryRenderingVersion,
	}); err != nil {
		return err
	}
	if err := validateGenerationSessionPolicy(GenerationPurposeDialogue, generation.SessionPolicyID); err != nil {
		return err
	}
	for _, id := range []canonical.ID{
		generation.RunID, generation.ResidentID, generation.PipelineVersionID,
		generation.PrinciplesRevisionID, generation.PersonaRevisionID,
		generation.MemoryPolicyRevisionID, generation.RunningOutcomeID,
	} {
		if err := id.Validate(); err != nil {
			return err
		}
	}
	if generation.AsOf != value.Target.AsOf || generation.AsOfTZ != value.Target.AsOfTZ {
		return errors.New("domain: PrepareDialogue generation time differs from its Assembly Target")
	}

	fallbackInSummary, err := dialogueDroppedRecallReason(generation.DroppedInputSummary)
	if err != nil {
		return err
	}
	memoryInputClaims, err := validatePrepareDialogueInputs(generation, value.SourceEventID)
	if err != nil {
		return err
	}

	switch value.RecallDisposition {
	case RecallDispositionSuccess:
		if value.RecallFallbackReason != "" || fallbackInSummary != "" {
			return errors.New("domain: successful dialogue Recall cannot have a fallback reason")
		}
		if value.Recall == nil || generation.RecallRunID == nil {
			return errors.New("domain: successful dialogue Recall requires Recall provenance")
		}
		if *generation.RecallRunID != value.Recall.RunID {
			return errors.New("domain: dialogue generation and Recall run IDs differ")
		}
		graph, err := validateDialogueRecallV3(*value.Recall, generation, value.Target)
		if err != nil {
			return err
		}
		if err := validateRecallCandidateSnapshots(value.RecallCandidates, graph.candidateClaims); err != nil {
			return err
		}
		if !equalIDs(memoryInputClaims, graph.promptClaims) {
			return errors.New("domain: memory Recall inputs and prompt-included usages differ")
		}
	case RecallDispositionPolicyDisabled:
		if value.Recall != nil || generation.RecallRunID != nil || len(value.RecallCandidates) != 0 ||
			len(memoryInputClaims) != 0 || value.RecallFallbackReason != "" || fallbackInSummary != "" {
			return errors.New("domain: policy-disabled dialogue Recall path contains Recall or fallback data")
		}
	case RecallDispositionExplicitFallback:
		if value.Recall != nil || generation.RecallRunID != nil || len(value.RecallCandidates) != 0 || len(memoryInputClaims) != 0 {
			return errors.New("domain: explicit dialogue Recall fallback contains Recall data")
		}
		if err := value.RecallFallbackReason.Validate(); err != nil {
			return err
		}
		if fallbackInSummary != value.RecallFallbackReason {
			return errors.New("domain: dialogue Recall fallback reason differs from dropped-input provenance")
		}
	}
	return nil
}

func validatePrepareDialogueInputs(generation PrepareGeneration, sourceEventID canonical.ID) ([]canonical.ID, error) {
	inputIDs := make(map[canonical.ID]struct{}, len(generation.Inputs))
	contentIDs := make(map[canonical.ID]struct{}, len(generation.Inputs))
	revisions := map[canonical.ID]bool{
		generation.PrinciplesRevisionID:   false,
		generation.PersonaRevisionID:      false,
		generation.MemoryPolicyRevisionID: false,
	}
	eventSources := make(map[canonical.ID]struct{})
	currentInputs := 0
	runtimeInputs := 0
	var memoryInputClaims []canonical.ID
	for index, input := range generation.Inputs {
		if input.Ordinal != int64(index) {
			return nil, errors.New("domain: PrepareDialogue input ordinals must be contiguous")
		}
		if err := input.ID.Validate(); err != nil {
			return nil, err
		}
		if _, duplicate := inputIDs[input.ID]; duplicate {
			return nil, errors.New("domain: duplicate PrepareDialogue input ID")
		}
		inputIDs[input.ID] = struct{}{}
		if input.Role == "" || input.SourceType == "" || input.InclusionMode == "" {
			return nil, errors.New("domain: incomplete PrepareDialogue input provenance")
		}
		if input.SourceID != nil {
			if err := input.SourceID.Validate(); err != nil {
				return nil, err
			}
		}
		if err := input.Content.Validate(); err != nil {
			return nil, err
		}
		if input.Content.ResidentID != generation.ResidentID || input.Content.Class != "generation_input" {
			return nil, errors.New("domain: PrepareDialogue input content resident differs")
		}
		if _, duplicate := contentIDs[input.Content.ID]; duplicate {
			return nil, errors.New("domain: duplicate PrepareDialogue input content ID")
		}
		contentIDs[input.Content.ID] = struct{}{}

		switch input.SourceType {
		case "resident_revision":
			if input.Role != "system" || input.InclusionMode != "resident_definition" || input.SourceID == nil {
				return nil, errors.New("domain: invalid resident definition input provenance")
			}
			used, expected := revisions[*input.SourceID]
			if !expected || used {
				return nil, errors.New("domain: resident definition inputs do not match the pinned revisions")
			}
			revisions[*input.SourceID] = true
		case "runtime_projection":
			if input.Role != "system" || input.InclusionMode != "runtime_projection" || input.SourceID != nil {
				return nil, errors.New("domain: invalid runtime projection input provenance")
			}
			runtimeInputs++
		case "event":
			if input.SourceID == nil ||
				(input.Role != "user" && input.Role != "assistant") ||
				(input.InclusionMode != "live_context" && input.InclusionMode != "context_backfill" && input.InclusionMode != "current_input") {
				return nil, errors.New("domain: invalid dialogue event input provenance")
			}
			if _, duplicate := eventSources[*input.SourceID]; duplicate {
				return nil, errors.New("domain: duplicate dialogue event input source")
			}
			eventSources[*input.SourceID] = struct{}{}
			if input.InclusionMode == "current_input" {
				currentInputs++
				if input.Role != "user" || *input.SourceID != sourceEventID {
					return nil, errors.New("domain: PrepareDialogue current input differs from its source event")
				}
			} else if *input.SourceID == sourceEventID {
				return nil, errors.New("domain: dialogue source event may only appear as current input")
			}
		case "claim":
			if input.Role != "system" || input.InclusionMode != "memory_recall" || input.SourceID == nil {
				return nil, errors.New("domain: inconsistent memory Recall input provenance")
			}
			memoryInputClaims = append(memoryInputClaims, *input.SourceID)
		default:
			return nil, fmt.Errorf("domain: unsupported PrepareDialogue input source type %q", input.SourceType)
		}
	}
	if currentInputs != 1 {
		return nil, errors.New("domain: PrepareDialogue requires exactly one current input")
	}
	if runtimeInputs != 1 {
		return nil, errors.New("domain: PrepareDialogue requires exactly one runtime projection input")
	}
	for _, present := range revisions {
		if !present {
			return nil, errors.New("domain: PrepareDialogue requires each pinned resident definition exactly once")
		}
	}
	return memoryInputClaims, nil
}

type dialogueRecallUsageGraph struct {
	candidateClaims []canonical.ID
	promptClaims    []canonical.ID
}

func validateDialogueRecallV3(
	recall DialogueRecall,
	generation PrepareGeneration,
	target AssemblyTarget,
) (dialogueRecallUsageGraph, error) {
	for _, id := range []canonical.ID{
		recall.RunID, recall.ResidentID, recall.QueryContentID,
		recall.PipelineVersionID, recall.MemoryPolicyRevisionID,
	} {
		if err := id.Validate(); err != nil {
			return dialogueRecallUsageGraph{}, err
		}
	}
	if err := recall.AsOfTZ.Validate(); err != nil {
		return dialogueRecallUsageGraph{}, err
	}
	if recall.QueryConditions.IsZero() || recall.ContextConstraints.IsZero() {
		return dialogueRecallUsageGraph{}, errors.New("domain: incomplete dialogue Recall provenance")
	}
	var queryTarget struct {
		ProjectionHead canonical.CommitSeq `json:"projection_head"`
	}
	if err := json.Unmarshal(recall.QueryConditions.Bytes(), &queryTarget); err != nil ||
		queryTarget.ProjectionHead != target.Head.CommitSeq {
		return dialogueRecallUsageGraph{}, errors.New("domain: dialogue Recall query head differs from its Assembly Target")
	}
	if recall.ResidentID != generation.ResidentID || recall.MemoryPolicyRevisionID != generation.MemoryPolicyRevisionID ||
		recall.AsOf != target.AsOf || recall.AsOfTZ != target.AsOfTZ {
		return dialogueRecallUsageGraph{}, errors.New("domain: dialogue Recall provenance differs from its generation Assembly")
	}
	if len(recall.Usages) > MaxRecallUsages {
		return dialogueRecallUsageGraph{}, errors.New("domain: dialogue Recall has too many usages")
	}

	ids := make(map[canonical.ID]struct{}, len(recall.Usages))
	typeClaims := make(map[string]struct{}, len(recall.Usages))
	nextOrdinal := map[string]int64{"candidate": 0, "selected": 0, "prompt_included": 0}
	candidates := make(map[canonical.ID]struct{})
	selected := make(map[canonical.ID]RecallUsage)
	prompt := make(map[canonical.ID]struct{})
	graph := dialogueRecallUsageGraph{}
	for _, usage := range recall.Usages {
		if err := usage.ID.Validate(); err != nil {
			return dialogueRecallUsageGraph{}, err
		}
		if err := usage.ClaimID.Validate(); err != nil {
			return dialogueRecallUsageGraph{}, err
		}
		kind := string(usage.Type)
		if _, allowed := nextOrdinal[kind]; !allowed {
			return dialogueRecallUsageGraph{}, fmt.Errorf("domain: invalid dialogue Recall usage type %q", kind)
		}
		if err := usage.Ordinal.Validate(); err != nil {
			return dialogueRecallUsageGraph{}, err
		}
		if usage.Ordinal.Int64() != nextOrdinal[kind] {
			return dialogueRecallUsageGraph{}, errors.New("domain: dialogue Recall usage ordinals must be contiguous by type")
		}
		nextOrdinal[kind]++
		if _, duplicate := ids[usage.ID]; duplicate {
			return dialogueRecallUsageGraph{}, errors.New("domain: duplicate dialogue Recall usage ID")
		}
		ids[usage.ID] = struct{}{}
		key := kind + "\x00" + usage.ClaimID.String()
		if _, duplicate := typeClaims[key]; duplicate {
			return dialogueRecallUsageGraph{}, errors.New("domain: duplicate dialogue Recall claim usage")
		}
		typeClaims[key] = struct{}{}

		exclusion := string(usage.ExclusionReason)
		switch kind {
		case "candidate":
			if exclusion != "" {
				return dialogueRecallUsageGraph{}, errors.New("domain: candidate Recall usage cannot have an exclusion reason")
			}
			candidates[usage.ClaimID] = struct{}{}
			graph.candidateClaims = append(graph.candidateClaims, usage.ClaimID)
		case "selected":
			if exclusion != "" && !oneOf(exclusion, "provenance_duplicate", "token_budget", "policy_filter") {
				return dialogueRecallUsageGraph{}, fmt.Errorf("domain: invalid context-v2 Recall exclusion reason %q", exclusion)
			}
			selected[usage.ClaimID] = usage
		case "prompt_included":
			if exclusion != "" {
				return dialogueRecallUsageGraph{}, errors.New("domain: prompt-included Recall usage cannot have an exclusion reason")
			}
			prompt[usage.ClaimID] = struct{}{}
			graph.promptClaims = append(graph.promptClaims, usage.ClaimID)
		}
	}
	if len(candidates) > MaxRecallCandidates || len(selected) > MaxRecallSelected || len(prompt) > MaxRecallPromptInputs {
		return dialogueRecallUsageGraph{}, errors.New("domain: dialogue Recall usage type exceeds its fixed limit")
	}
	for claimID, selectedUsage := range selected {
		if _, exists := candidates[claimID]; !exists {
			return dialogueRecallUsageGraph{}, errors.New("domain: selected Recall usage lacks candidate usage")
		}
		_, included := prompt[claimID]
		hasExclusion := selectedUsage.ExclusionReason != ""
		if included == hasExclusion {
			return dialogueRecallUsageGraph{}, errors.New("domain: selected Recall inclusion and exclusion provenance disagree")
		}
	}
	for claimID := range prompt {
		if _, exists := selected[claimID]; !exists {
			return dialogueRecallUsageGraph{}, errors.New("domain: prompt-included Recall usage lacks selected usage")
		}
	}
	return graph, nil
}

func validateRecallCandidateSnapshots(candidates []RecallCandidateSnapshot, usageClaims []canonical.ID) error {
	if len(candidates) != len(usageClaims) {
		return errors.New("domain: dialogue Recall candidates and candidate usages differ")
	}
	seen := make(map[canonical.ID]struct{}, len(candidates))
	for index, candidate := range candidates {
		if err := candidate.Validate(); err != nil {
			return err
		}
		if _, duplicate := seen[candidate.ClaimID]; duplicate {
			return errors.New("domain: duplicate dialogue Recall candidate claim")
		}
		seen[candidate.ClaimID] = struct{}{}
		if candidate.ClaimID != usageClaims[index] {
			return errors.New("domain: dialogue Recall candidate order differs from usage ordinals")
		}
	}
	return nil
}

func dialogueDroppedRecallReason(value canonical.CanonicalJSON) (MemoryRecallFallbackReason, error) {
	var object map[string]json.RawMessage
	if err := json.Unmarshal(value.Bytes(), &object); err != nil || object == nil {
		return "", errors.New("domain: dropped input summary must be a JSON object")
	}
	raw, exists := object["memory_recall"]
	if !exists {
		return "", nil
	}
	var reason MemoryRecallFallbackReason
	if err := json.Unmarshal(raw, &reason); err != nil {
		return "", errors.New("domain: invalid memory Recall fallback provenance")
	}
	if err := reason.Validate(); err != nil {
		return "", err
	}
	return reason, nil
}

func equalIDs(left, right []canonical.ID) bool {
	if len(left) != len(right) {
		return false
	}
	for index := range left {
		if left[index] != right[index] {
			return false
		}
	}
	return true
}

func oneOf(value string, allowed ...string) bool {
	for _, candidate := range allowed {
		if value == candidate {
			return true
		}
	}
	return false
}
