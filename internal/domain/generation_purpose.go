package domain

import "fmt"

// GenerationPurpose is the closed purpose contract used by M5 landing
// handlers. The zero value is accepted only as a source-compatibility alias
// for the pre-M5 dialogue envelope; durable rows always contain `dialogue`.
type GenerationPurpose string

const (
	GenerationPurposeDialogue              GenerationPurpose = "dialogue"
	GenerationPurposeMemoryExtraction      GenerationPurpose = "memory_extraction"
	GenerationPurposeMemoryAlignment       GenerationPurpose = "memory_alignment"
	GenerationPurposeMemoryAbstraction     GenerationPurpose = "memory_abstraction"
	GenerationPurposeMemoryDifferentiation GenerationPurpose = "memory_differentiation"
	GenerationPurposePersonaRevision       GenerationPurpose = "persona_revision"
	GenerationPurposeSelfTalk              GenerationPurpose = "self_talk"
	GenerationPurposeOutboundInitiative    GenerationPurpose = "outbound_initiative"
)

func (purpose GenerationPurpose) Effective() GenerationPurpose {
	if purpose == "" {
		return GenerationPurposeDialogue
	}
	return purpose
}

func (purpose GenerationPurpose) Validate() error {
	switch purpose.Effective() {
	case GenerationPurposeDialogue, GenerationPurposeMemoryExtraction, GenerationPurposeMemoryAlignment,
		GenerationPurposeMemoryAbstraction, GenerationPurposeMemoryDifferentiation, GenerationPurposePersonaRevision,
		GenerationPurposeSelfTalk, GenerationPurposeOutboundInitiative:
		return nil
	default:
		return fmt.Errorf("domain: unsupported generation purpose %q", purpose)
	}
}

func (purpose GenerationPurpose) RequiresStructuredOutput() bool {
	switch purpose.Effective() {
	case GenerationPurposeMemoryExtraction, GenerationPurposeMemoryAlignment,
		GenerationPurposeMemoryAbstraction, GenerationPurposeMemoryDifferentiation,
		GenerationPurposePersonaRevision:
		return true
	default:
		return false
	}
}

// GenerationVersionContract is the immutable semantic envelope recorded with
// a generation run. It is separate from the pipeline_version_id: the latter
// identifies executable orchestration, while these strings pin prompt,
// context selection, and memory rendering across retry and restart.
type GenerationVersionContract struct {
	PromptTemplateVersion  string
	ContextPolicyVersion   string
	MemoryRenderingVersion string
}

func GenerationVersionsForPurpose(purpose GenerationPurpose) (GenerationVersionContract, error) {
	purpose = purpose.Effective()
	if err := purpose.Validate(); err != nil {
		return GenerationVersionContract{}, err
	}
	if purpose == GenerationPurposeDialogue {
		return GenerationVersionContract{}, fmt.Errorf(
			"domain: dialogue versions require an explicit normal or synthetic execution contract",
		)
	}
	wire := string(purpose)
	return GenerationVersionContract{
		PromptTemplateVersion: wire + "-prompt-v1", ContextPolicyVersion: wire + "-context-v1",
		MemoryRenderingVersion: "memory-rendering-v1",
	}, nil
}

func ValidateGenerationVersions(purpose GenerationPurpose, actual GenerationVersionContract) error {
	expected, err := GenerationVersionsForPurpose(purpose)
	if err != nil {
		return err
	}
	if actual.PromptTemplateVersion == "" || actual.ContextPolicyVersion == "" || actual.MemoryRenderingVersion == "" {
		return fmt.Errorf("domain: incomplete generation version contract")
	}
	if actual != expected {
		return fmt.Errorf("domain: generation version contract mismatch for purpose %q", purpose.Effective())
	}
	return nil
}
