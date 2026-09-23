package domain

import (
	"fmt"

	"mahoroba.local/mahoroba/internal/canonical"
)

const (
	DialoguePipelineVersionV1       = "dialogue-v1"
	DialoguePipelineVersionV2       = "dialogue-v2"
	DialoguePipelineVersionV3       = "dialogue-v3"
	DialoguePipelineVersionV4       = "dialogue-v4"
	DialoguePromptTemplateVersionV1 = "dialogue-v1"
	DialogueContextPolicyVersionV1  = "dialogue-context-v1"
	DialogueContextPolicyVersionV2  = "dialogue-context-v2"
	DialogueContextPolicyVersionV3  = "dialogue-context-v3"
	DialogueContextPolicyVersionV4  = "dialogue-context-v4"
	MemoryRenderingVersionNoneV1    = "memory-none-v1"
	MemoryRenderingVersionV1        = "memory-rendering-v1"
	MemoryRenderingVersionV2        = "memory-rendering-v2"
)

// DialogueExecutionContract is the complete version tuple used to classify a
// dialogue envelope. PipelineVersionKey is resolved from pipeline_version_id
// by the persistence adapter; treating the other fields as independent allow
// lists would admit mixed legacy/current envelopes.
type DialogueExecutionContract struct {
	PipelineVersionKey     string
	PromptTemplateVersion  string
	ContextPolicyVersion   string
	MemoryRenderingVersion string
}

type DialogueEnvelopeKind string

const (
	DialogueEnvelopeNormal              DialogueEnvelopeKind = "normal"
	DialogueEnvelopeSyntheticNoDispatch DialogueEnvelopeKind = "synthetic_no_dispatch"
)

type DialogueExecutionClass string

const (
	DialogueExecutionLegacyNormal          DialogueExecutionClass = "legacy_normal"
	DialogueExecutionPriorSplitNormal      DialogueExecutionClass = "prior_split_normal"
	DialogueExecutionPriorBoundedNormal    DialogueExecutionClass = "prior_bounded_normal"
	DialogueExecutionCurrentNormal         DialogueExecutionClass = "current_normal"
	DialogueExecutionSyntheticLegacy       DialogueExecutionClass = "synthetic_legacy"
	DialogueExecutionSyntheticPriorSplit   DialogueExecutionClass = "synthetic_prior_split"
	DialogueExecutionSyntheticPriorBounded DialogueExecutionClass = "synthetic_prior_bounded"
	DialogueExecutionSyntheticCurrent      DialogueExecutionClass = "synthetic_current"
)

func LegacyDialogueNormalExecutionContract() DialogueExecutionContract {
	return DialogueExecutionContract{
		PipelineVersionKey: DialoguePipelineVersionV1, PromptTemplateVersion: DialoguePromptTemplateVersionV1,
		ContextPolicyVersion: DialogueContextPolicyVersionV1, MemoryRenderingVersion: MemoryRenderingVersionNoneV1,
	}
}

func PriorSplitDialogueNormalExecutionContract() DialogueExecutionContract {
	return DialogueExecutionContract{
		PipelineVersionKey: DialoguePipelineVersionV2, PromptTemplateVersion: DialoguePromptTemplateVersionV1,
		ContextPolicyVersion: DialogueContextPolicyVersionV2, MemoryRenderingVersion: MemoryRenderingVersionV1,
	}
}

func PriorBoundedDialogueNormalExecutionContract() DialogueExecutionContract {
	return DialogueExecutionContract{
		PipelineVersionKey: DialoguePipelineVersionV3, PromptTemplateVersion: DialoguePromptTemplateVersionV1,
		ContextPolicyVersion: DialogueContextPolicyVersionV3, MemoryRenderingVersion: MemoryRenderingVersionV2,
	}
}

func CurrentDialogueNormalExecutionContract() DialogueExecutionContract {
	return DialogueExecutionContract{
		PipelineVersionKey: DialoguePipelineVersionV4, PromptTemplateVersion: DialoguePromptTemplateVersionV1,
		ContextPolicyVersion: DialogueContextPolicyVersionV4, MemoryRenderingVersion: MemoryRenderingVersionV2,
	}
}

func SyntheticDialogueExecutionContract(pipelineVersionKey string) (DialogueExecutionContract, error) {
	switch pipelineVersionKey {
	case DialoguePipelineVersionV1:
		return LegacyDialogueNormalExecutionContract(), nil
	case DialoguePipelineVersionV2:
		return DialogueExecutionContract{
			PipelineVersionKey: DialoguePipelineVersionV2, PromptTemplateVersion: DialoguePromptTemplateVersionV1,
			ContextPolicyVersion: DialogueContextPolicyVersionV2, MemoryRenderingVersion: MemoryRenderingVersionNoneV1,
		}, nil
	case DialoguePipelineVersionV3:
		return DialogueExecutionContract{
			PipelineVersionKey: DialoguePipelineVersionV3, PromptTemplateVersion: DialoguePromptTemplateVersionV1,
			ContextPolicyVersion: DialogueContextPolicyVersionV3, MemoryRenderingVersion: MemoryRenderingVersionNoneV1,
		}, nil
	case DialoguePipelineVersionV4:
		return DialogueExecutionContract{
			PipelineVersionKey: DialoguePipelineVersionV4, PromptTemplateVersion: DialoguePromptTemplateVersionV1,
			ContextPolicyVersion: DialogueContextPolicyVersionV4, MemoryRenderingVersion: MemoryRenderingVersionNoneV1,
		}, nil
	default:
		return DialogueExecutionContract{}, fmt.Errorf("domain: unsupported dialogue pipeline version %q", pipelineVersionKey)
	}
}

func ClassifyPersistedDialogueExecutionContract(actual DialogueExecutionContract, kind DialogueEnvelopeKind) (DialogueExecutionClass, error) {
	if actual.PipelineVersionKey == "" || actual.PromptTemplateVersion == "" ||
		actual.ContextPolicyVersion == "" || actual.MemoryRenderingVersion == "" {
		return "", fmt.Errorf("domain: incomplete dialogue execution contract")
	}
	switch kind {
	case DialogueEnvelopeNormal:
		switch actual {
		case LegacyDialogueNormalExecutionContract():
			return DialogueExecutionLegacyNormal, nil
		case PriorSplitDialogueNormalExecutionContract():
			return DialogueExecutionPriorSplitNormal, nil
		case PriorBoundedDialogueNormalExecutionContract():
			return DialogueExecutionPriorBoundedNormal, nil
		case CurrentDialogueNormalExecutionContract():
			return DialogueExecutionCurrentNormal, nil
		}
	case DialogueEnvelopeSyntheticNoDispatch:
		legacy, _ := SyntheticDialogueExecutionContract(DialoguePipelineVersionV1)
		priorSplit, _ := SyntheticDialogueExecutionContract(DialoguePipelineVersionV2)
		priorBounded, _ := SyntheticDialogueExecutionContract(DialoguePipelineVersionV3)
		current, _ := SyntheticDialogueExecutionContract(DialoguePipelineVersionV4)
		switch actual {
		case legacy:
			return DialogueExecutionSyntheticLegacy, nil
		case priorSplit:
			return DialogueExecutionSyntheticPriorSplit, nil
		case priorBounded:
			return DialogueExecutionSyntheticPriorBounded, nil
		case current:
			return DialogueExecutionSyntheticCurrent, nil
		}
	default:
		return "", fmt.Errorf("domain: unsupported dialogue envelope kind %q", kind)
	}
	return "", fmt.Errorf("domain: mixed or unsupported dialogue execution contract for %s", kind)
}

func ValidateNewDialogueNormalExecutionContract(actual DialogueExecutionContract) error {
	if actual != CurrentDialogueNormalExecutionContract() {
		return fmt.Errorf("domain: new dialogue write requires the current execution contract")
	}
	return nil
}

func ValidateDialogueDispatchExecutionContract(actual DialogueExecutionContract) error {
	_, err := ClassifyPersistedDialogueExecutionContract(actual, DialogueEnvelopeNormal)
	return err
}

func ValidateSyntheticDialogueExecutionContract(actual DialogueExecutionContract, sourcePipelineVersionKey string) error {
	expected, err := SyntheticDialogueExecutionContract(sourcePipelineVersionKey)
	if err != nil {
		return err
	}
	if actual != expected {
		return fmt.Errorf("domain: synthetic dialogue contract does not match the source-event pipeline contract")
	}
	return nil
}

// ValidateDialogueCancellationVersionContract accepts only complete dialogue
// tuples that can occur on an existing normal run or on a source-bound
// provider-free synthetic cancellation. Persistence still resolves the
// pipeline ID and source-event pipeline key authoritatively before a runless
// synthetic row is written.
func ValidateDialogueCancellationVersionContract(actual GenerationVersionContract) error {
	allowed := []DialogueExecutionContract{
		LegacyDialogueNormalExecutionContract(),
		PriorSplitDialogueNormalExecutionContract(),
		PriorBoundedDialogueNormalExecutionContract(),
		CurrentDialogueNormalExecutionContract(),
	}
	priorSplitSynthetic, _ := SyntheticDialogueExecutionContract(DialoguePipelineVersionV2)
	priorBoundedSynthetic, _ := SyntheticDialogueExecutionContract(DialoguePipelineVersionV3)
	currentSynthetic, _ := SyntheticDialogueExecutionContract(DialoguePipelineVersionV4)
	allowed = append(allowed, priorSplitSynthetic, priorBoundedSynthetic, currentSynthetic)
	for _, candidate := range allowed {
		if actual.PromptTemplateVersion == candidate.PromptTemplateVersion &&
			actual.ContextPolicyVersion == candidate.ContextPolicyVersion &&
			actual.MemoryRenderingVersion == candidate.MemoryRenderingVersion {
			return nil
		}
	}
	return fmt.Errorf("domain: mixed or unsupported dialogue cancellation version contract")
}

// DialoguePipelineDefinition returns the exact idempotent Canonical row shape
// used by bootstrap and the readiness-gated startup registration path.
func DialoguePipelineDefinition(id canonical.ID, versionKey string) (PipelineVersionDefinition, error) {
	if err := id.Validate(); err != nil {
		return PipelineVersionDefinition{}, err
	}
	if versionKey != DialoguePipelineVersionV1 && versionKey != DialoguePipelineVersionV2 &&
		versionKey != DialoguePipelineVersionV3 && versionKey != DialoguePipelineVersionV4 {
		return PipelineVersionDefinition{}, fmt.Errorf("domain: unsupported dialogue pipeline version %q", versionKey)
	}
	definition, err := canonical.MarshalCanonical(struct {
		Purpose string `json:"purpose"`
		Version string `json:"version"`
	}{Purpose: "dialogue", Version: versionKey})
	if err != nil {
		return PipelineVersionDefinition{}, err
	}
	return PipelineVersionDefinition{ID: id, Kind: "dialogue", VersionKey: versionKey, Definition: definition}, nil
}

// ValidateExactDialoguePipelineDefinition defends the Canonical registration
// boundary against a caller reusing a supported version key with different
// executable meaning. Non-dialogue definitions are outside this contract.
func ValidateExactDialoguePipelineDefinition(value PipelineVersionDefinition) error {
	if value.Kind != "dialogue" {
		return nil
	}
	expected, err := DialoguePipelineDefinition(value.ID, value.VersionKey)
	if err != nil {
		return err
	}
	if value.Definition.String() != expected.Definition.String() {
		return fmt.Errorf("domain: dialogue pipeline %s definition is not exact", value.VersionKey)
	}
	return nil
}
