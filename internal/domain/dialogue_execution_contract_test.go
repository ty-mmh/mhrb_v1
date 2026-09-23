package domain

import (
	"testing"

	"mahoroba.local/mahoroba/internal/canonical"
)

func TestCOV2DialogueExecutionContractClassifiesOnlyExactTuples(t *testing.T) {
	legacy := LegacyDialogueNormalExecutionContract()
	priorSplit := PriorSplitDialogueNormalExecutionContract()
	priorBounded := PriorBoundedDialogueNormalExecutionContract()
	priorBoundedSynthetic, _ := SyntheticDialogueExecutionContract(DialoguePipelineVersionV3)
	current := CurrentDialogueNormalExecutionContract()
	legacySynthetic, err := SyntheticDialogueExecutionContract(DialoguePipelineVersionV1)
	if err != nil {
		t.Fatal(err)
	}
	priorSplitSynthetic, err := SyntheticDialogueExecutionContract(DialoguePipelineVersionV2)
	if err != nil {
		t.Fatal(err)
	}
	currentSynthetic, err := SyntheticDialogueExecutionContract(DialoguePipelineVersionV4)
	if err != nil {
		t.Fatal(err)
	}
	for _, test := range []struct {
		name  string
		value DialogueExecutionContract
		kind  DialogueEnvelopeKind
		want  DialogueExecutionClass
	}{
		{name: "legacy normal", value: legacy, kind: DialogueEnvelopeNormal, want: DialogueExecutionLegacyNormal},
		{name: "prior split normal", value: priorSplit, kind: DialogueEnvelopeNormal, want: DialogueExecutionPriorSplitNormal},
		{name: "prior bounded normal", value: priorBounded, kind: DialogueEnvelopeNormal, want: DialogueExecutionPriorBoundedNormal},
		{name: "prior bounded synthetic", value: priorBoundedSynthetic, kind: DialogueEnvelopeSyntheticNoDispatch, want: DialogueExecutionSyntheticPriorBounded},
		{name: "current normal", value: current, kind: DialogueEnvelopeNormal, want: DialogueExecutionCurrentNormal},
		{name: "legacy synthetic", value: legacySynthetic, kind: DialogueEnvelopeSyntheticNoDispatch, want: DialogueExecutionSyntheticLegacy},
		{name: "prior split synthetic", value: priorSplitSynthetic, kind: DialogueEnvelopeSyntheticNoDispatch, want: DialogueExecutionSyntheticPriorSplit},
		{name: "current synthetic", value: currentSynthetic, kind: DialogueEnvelopeSyntheticNoDispatch, want: DialogueExecutionSyntheticCurrent},
	} {
		t.Run(test.name, func(t *testing.T) {
			class, err := ClassifyPersistedDialogueExecutionContract(test.value, test.kind)
			if err != nil || class != test.want {
				t.Fatalf("class/error = %q / %v, want %q", class, err, test.want)
			}
		})
	}
}

func TestCOV2DialogueExecutionContractRejectsMixedAndWrongUseTuples(t *testing.T) {
	mixed := CurrentDialogueNormalExecutionContract()
	mixed.MemoryRenderingVersion = MemoryRenderingVersionNoneV1
	if _, err := ClassifyPersistedDialogueExecutionContract(mixed, DialogueEnvelopeNormal); err == nil {
		t.Fatal("mixed current normal tuple was accepted")
	}
	if err := ValidateNewDialogueNormalExecutionContract(LegacyDialogueNormalExecutionContract()); err == nil {
		t.Fatal("legacy tuple was accepted for a new normal write")
	}
	if err := ValidateDialogueDispatchExecutionContract(mixed); err == nil {
		t.Fatal("mixed tuple was accepted for dispatch")
	}
	currentSynthetic, err := SyntheticDialogueExecutionContract(DialoguePipelineVersionV4)
	if err != nil {
		t.Fatal(err)
	}
	if err := ValidateSyntheticDialogueExecutionContract(currentSynthetic, DialoguePipelineVersionV2); err == nil {
		t.Fatal("post-v3 synthetic tuple was accepted for a pre-v3 source contract")
	}
	if currentSynthetic.MemoryRenderingVersion != MemoryRenderingVersionNoneV1 ||
		CurrentDialogueNormalExecutionContract().MemoryRenderingVersion != MemoryRenderingVersionV2 {
		t.Fatal("current synthetic and normal dialogue contracts do not keep rendering versions separate")
	}
	if _, err := ClassifyPersistedDialogueExecutionContract(currentSynthetic, DialogueEnvelopeNormal); err == nil {
		t.Fatal("current synthetic tuple was accepted as a normal provider-dispatched run")
	}
	if _, err := ClassifyPersistedDialogueExecutionContract(
		CurrentDialogueNormalExecutionContract(), DialogueEnvelopeSyntheticNoDispatch,
	); err == nil {
		t.Fatal("current normal tuple was accepted as a provider-free synthetic cancellation")
	}
}

func TestCOV2DialogueCancellationValidatorAllowsOnlyCompletePersistedOrSyntheticTuples(t *testing.T) {
	priorSplitSynthetic, err := SyntheticDialogueExecutionContract(DialoguePipelineVersionV2)
	if err != nil {
		t.Fatal(err)
	}
	currentSynthetic, err := SyntheticDialogueExecutionContract(DialoguePipelineVersionV4)
	if err != nil {
		t.Fatal(err)
	}
	priorBoundedSynthetic, _ := SyntheticDialogueExecutionContract(DialoguePipelineVersionV3)
	for _, contract := range []DialogueExecutionContract{
		LegacyDialogueNormalExecutionContract(),
		PriorSplitDialogueNormalExecutionContract(),
		CurrentDialogueNormalExecutionContract(),
		priorSplitSynthetic,
		PriorBoundedDialogueNormalExecutionContract(),
		priorBoundedSynthetic,
		currentSynthetic,
	} {
		if err := ValidateDialogueCancellationVersionContract(GenerationVersionContract{
			PromptTemplateVersion:  contract.PromptTemplateVersion,
			ContextPolicyVersion:   contract.ContextPolicyVersion,
			MemoryRenderingVersion: contract.MemoryRenderingVersion,
		}); err != nil {
			t.Fatalf("valid cancellation tuple %+v rejected: %v", contract, err)
		}
	}
	if err := ValidateDialogueCancellationVersionContract(GenerationVersionContract{
		PromptTemplateVersion:  DialoguePromptTemplateVersionV1,
		ContextPolicyVersion:   DialogueContextPolicyVersionV1,
		MemoryRenderingVersion: MemoryRenderingVersionV1,
	}); err == nil {
		t.Fatal("mixed legacy-context/current-rendering cancellation tuple was accepted")
	}
}

func TestCOV2DialoguePipelineDefinitionIsExactAndVersioned(t *testing.T) {
	id := mustProjectionTestIDForDialogueContract(t, "01ARZ3NDEKTSV4RRFFQ69G5FAV")
	definition, err := DialoguePipelineDefinition(id, DialoguePipelineVersionV3)
	if err != nil {
		t.Fatal(err)
	}
	if definition.ID != id || definition.Kind != "dialogue" || definition.VersionKey != DialoguePipelineVersionV3 ||
		definition.Definition.String() != `{"purpose":"dialogue","version":"dialogue-v3"}` {
		t.Fatalf("dialogue pipeline definition = %+v", definition)
	}
	tampered, err := canonical.MarshalCanonical(struct {
		Purpose string `json:"purpose"`
		Version string `json:"version"`
	}{Purpose: "dialogue", Version: "dialogue-v3-tampered"})
	if err != nil {
		t.Fatal(err)
	}
	definition.Definition = tampered
	if err := ValidateExactDialoguePipelineDefinition(definition); err == nil {
		t.Fatal("tampered dialogue pipeline definition was accepted")
	}
}

func TestCOV2GlobalBootstrapRequiresExactCurrentDialogueDefinition(t *testing.T) {
	pipelineID := mustProjectionTestIDForDialogueContract(t, "01ARZ3NDEKTSV4RRFFQ69G5FAV")
	exact, err := DialoguePipelineDefinition(pipelineID, DialoguePipelineVersionV4)
	if err != nil {
		t.Fatal(err)
	}
	session, err := canonical.MarshalCanonical(struct {
		Version string `json:"version"`
	}{Version: SessionPolicyVersion})
	if err != nil {
		t.Fatal(err)
	}
	value := GlobalBootstrap{
		SystemPrincipalID:  mustProjectionTestIDForDialogueContract(t, "01ARZ3NDEKTSV4RRFFQ69G5FAW"),
		OwnerPrincipalID:   mustProjectionTestIDForDialogueContract(t, "01ARZ3NDEKTSV4RRFFQ69G5FAX"),
		OwnerDisplayName:   "owner",
		PipelineVersionID:  pipelineID,
		PipelineDefinition: exact.Definition,
		SessionPolicyID:    mustProjectionTestIDForDialogueContract(t, "01ARZ3NDEKTSV4RRFFQ69G5FAY"),
		SessionDefinition:  session,
	}
	if err := GlobalBootstrapCommand(value).Validate(); err != nil {
		t.Fatalf("exact bootstrap definition rejected: %v", err)
	}
	tampered, err := canonical.MarshalCanonical(struct {
		Purpose string `json:"purpose"`
		Version string `json:"version"`
	}{Purpose: "dialogue", Version: DialoguePipelineVersionV3 + "-tampered"})
	if err != nil {
		t.Fatal(err)
	}
	value.PipelineDefinition = tampered
	if err := GlobalBootstrapCommand(value).Validate(); err == nil {
		t.Fatal("tampered bootstrap dialogue definition was accepted")
	}
}

func mustProjectionTestIDForDialogueContract(t *testing.T, raw string) canonical.ID {
	t.Helper()
	id, err := canonical.ParseID(raw)
	if err != nil {
		t.Fatal(err)
	}
	return id
}
