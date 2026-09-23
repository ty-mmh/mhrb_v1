package sqlite

import (
	"testing"

	"mahoroba.local/mahoroba/internal/domain"
)

func TestGenerationRunVersionsMatchPurposeContract(t *testing.T) {
	for _, test := range []struct {
		purpose                    domain.GenerationPurpose
		prompt, context, rendering string
	}{
		{purpose: domain.GenerationPurposeMemoryExtraction, prompt: "memory_extraction-prompt-v1", context: "memory_extraction-context-v1", rendering: "memory-rendering-v1"},
		{purpose: domain.GenerationPurposeMemoryAlignment, prompt: "memory_alignment-prompt-v1", context: "memory_alignment-context-v1", rendering: "memory-rendering-v1"},
		{purpose: domain.GenerationPurposeMemoryAbstraction, prompt: "memory_abstraction-prompt-v1", context: "memory_abstraction-context-v1", rendering: "memory-rendering-v1"},
		{purpose: domain.GenerationPurposeMemoryDifferentiation, prompt: "memory_differentiation-prompt-v1", context: "memory_differentiation-context-v1", rendering: "memory-rendering-v1"},
		{purpose: domain.GenerationPurposePersonaRevision, prompt: "persona_revision-prompt-v1", context: "persona_revision-context-v1", rendering: "memory-rendering-v1"},
		{purpose: domain.GenerationPurposeSelfTalk, prompt: "self_talk-prompt-v1", context: "self_talk-context-v1", rendering: "memory-rendering-v1"},
		{purpose: domain.GenerationPurposeOutboundInitiative, prompt: "outbound_initiative-prompt-v1", context: "outbound_initiative-context-v1", rendering: "memory-rendering-v1"},
	} {
		t.Run(string(test.purpose), func(t *testing.T) {
			versions, err := domain.GenerationVersionsForPurpose(test.purpose)
			if err != nil {
				t.Fatal(err)
			}
			if versions.PromptTemplateVersion != test.prompt || versions.ContextPolicyVersion != test.context ||
				versions.MemoryRenderingVersion != test.rendering {
				t.Fatalf("versions=%q/%q/%q want=%q/%q/%q", versions.PromptTemplateVersion,
					versions.ContextPolicyVersion, versions.MemoryRenderingVersion,
					test.prompt, test.context, test.rendering)
			}
			if err := domain.ValidateGenerationVersions(test.purpose, versions); err != nil {
				t.Fatal(err)
			}
		})
	}
	if _, err := domain.GenerationVersionsForPurpose("unknown"); err == nil {
		t.Fatal("unknown purpose received a generation version contract")
	}
	if _, err := domain.GenerationVersionsForPurpose(domain.GenerationPurposeDialogue); err == nil {
		t.Fatal("ambiguous dialogue purpose received a generic version contract")
	}
	if err := domain.ValidateGenerationVersions(domain.GenerationPurposeDialogue, domain.GenerationVersionContract{
		PromptTemplateVersion: domain.DialoguePromptTemplateVersionV1, ContextPolicyVersion: domain.DialogueContextPolicyVersionV2,
		MemoryRenderingVersion: domain.MemoryRenderingVersionV1,
	}); err == nil {
		t.Fatal("dialogue version tuple bypassed the explicit normal/synthetic execution contract")
	}
}
