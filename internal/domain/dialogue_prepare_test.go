package domain

import (
	"testing"

	"mahoroba.local/mahoroba/internal/canonical"
)

func TestCOV1GenericPrepareGenerationRejectsDialogue(t *testing.T) {
	value := validPrepareDialogue(t).Generation
	if err := PrepareGenerationCommand(value).Validate(); err == nil {
		t.Fatal("generic PrepareGeneration unexpectedly accepted a dialogue envelope")
	}
}

func TestCOV1PrepareDialogueCommandAcceptsZeroCandidateRecallSuccess(t *testing.T) {
	value := validPrepareDialogue(t)
	command := PrepareDialogueCommand(value)
	if command.Name() != "PrepareDialogue" {
		t.Fatalf("command name = %q", command.Name())
	}
	if err := command.Validate(); err != nil {
		t.Fatalf("PrepareDialogue validation: %v", err)
	}
}

func TestCOV1PrepareDialogueCommandSeparatesRecallBranches(t *testing.T) {
	t.Run("policy disabled", func(t *testing.T) {
		value := validPrepareDialogue(t)
		value.RecallDisposition = RecallDispositionPolicyDisabled
		value.Recall = nil
		value.Generation.RecallRunID = nil
		if err := PrepareDialogueCommand(value).Validate(); err != nil {
			t.Fatalf("policy-disabled validation: %v", err)
		}
	})

	t.Run("explicit fallback", func(t *testing.T) {
		value := validPrepareDialogue(t)
		value.RecallDisposition = RecallDispositionExplicitFallback
		value.RecallFallbackReason = MemoryRecallProjectionUnavailable
		value.Recall = nil
		value.Generation.RecallRunID = nil
		value.Generation.DroppedInputSummary = prepareDialogueDroppedSummary(t, value.RecallFallbackReason)
		if err := PrepareDialogueCommand(value).Validate(); err != nil {
			t.Fatalf("explicit-fallback validation: %v", err)
		}
	})

	t.Run("mixed policy-disabled branch", func(t *testing.T) {
		value := validPrepareDialogue(t)
		value.RecallDisposition = RecallDispositionPolicyDisabled
		if err := PrepareDialogueCommand(value).Validate(); err == nil {
			t.Fatal("mixed policy-disabled branch was accepted")
		}
	})

	t.Run("fallback reason not persisted", func(t *testing.T) {
		value := validPrepareDialogue(t)
		value.RecallDisposition = RecallDispositionExplicitFallback
		value.RecallFallbackReason = MemoryRecallProjectionUnavailable
		value.Recall = nil
		value.Generation.RecallRunID = nil
		if err := PrepareDialogueCommand(value).Validate(); err == nil {
			t.Fatal("explicit fallback without dropped-input provenance was accepted")
		}
	})
}

func TestCOV1PrepareDialogueCommandBindsAssemblyTargetAndCurrentTuple(t *testing.T) {
	t.Run("Assembly time", func(t *testing.T) {
		value := validPrepareDialogue(t)
		value.Generation.AsOf++
		if err := PrepareDialogueCommand(value).Validate(); err == nil {
			t.Fatal("generation with a different Assembly time was accepted")
		}
	})

	t.Run("legacy tuple", func(t *testing.T) {
		value := validPrepareDialogue(t)
		legacy := LegacyDialogueNormalExecutionContract()
		value.Generation.PromptTemplateVersion = legacy.PromptTemplateVersion
		value.Generation.ContextPolicyVersion = legacy.ContextPolicyVersion
		value.Generation.MemoryRenderingVersion = legacy.MemoryRenderingVersion
		if err := PrepareDialogueCommand(value).Validate(); err == nil {
			t.Fatal("new PrepareDialogue accepted the legacy normal tuple")
		}
	})

	t.Run("wrong obligation", func(t *testing.T) {
		value := validPrepareDialogue(t)
		value.Generation.IdempotencyKey += ":changed"
		if err := PrepareDialogueCommand(value).Validate(); err == nil {
			t.Fatal("PrepareDialogue accepted a mismatched obligation key")
		}
	})
}

func TestCOVR04PrepareDialogueResultRequiresTypedResolution(t *testing.T) {
	runID := validPrepareDialogue(t).Generation.RunID
	for _, resolution := range []PrepareDialogueResolution{
		PrepareDialoguePreparedCurrentV3,
		PrepareDialogueExistingCurrentV3,
		PrepareDialogueDispatchExistingFrozenRun,
	} {
		t.Run(string(resolution), func(t *testing.T) {
			if err := (PrepareDialogueResult{RunID: runID, Resolution: resolution}).Validate(); err != nil {
				t.Fatalf("valid PrepareDialogue result: %v", err)
			}
		})
	}

	for _, resolution := range []PrepareDialogueResolution{"", "existing_legacy"} {
		t.Run("reject "+string(resolution), func(t *testing.T) {
			if err := (PrepareDialogueResult{RunID: runID, Resolution: resolution}).Validate(); err == nil {
				t.Fatalf("unsupported PrepareDialogue resolution %q was accepted", resolution)
			}
		})
	}

	if err := (PrepareDialogueResult{Resolution: PrepareDialoguePreparedCurrentV3}).Validate(); err == nil {
		t.Fatal("PrepareDialogue result without a run ID was accepted")
	}
}

func validPrepareDialogue(t *testing.T) PrepareDialogue {
	t.Helper()
	eventID := mustParseID(t, "01ARZ3NDEKTSV4RRFFQ69G5FAV")
	residentID := mustParseID(t, "01ARZ3NDEKTSV4RRFFQ69G5FAW")
	runID := mustParseID(t, "01ARZ3NDEKTSV4RRFFQ69G5FAX")
	pipelineID := mustParseID(t, "01ARZ3NDEKTSV4RRFFQ69G5FAY")
	sessionID := mustParseID(t, "01ARZ3NDEKTSV4RRFFQ69G5FAZ")
	principlesID := mustParseID(t, "01ARZ3NDEKTSV4RRFFQ69G5FB0")
	personaID := mustParseID(t, "01ARZ3NDEKTSV4RRFFQ69G5FB1")
	memoryID := mustParseID(t, "01ARZ3NDEKTSV4RRFFQ69G5FB2")
	outcomeID := mustParseID(t, "01ARZ3NDEKTSV4RRFFQ69G5FB3")
	inputIDs := []canonical.ID{
		mustParseID(t, "01ARZ3NDEKTSV4RRFFQ69G5FC0"),
		mustParseID(t, "01ARZ3NDEKTSV4RRFFQ69G5FC1"),
		mustParseID(t, "01ARZ3NDEKTSV4RRFFQ69G5FC2"),
		mustParseID(t, "01ARZ3NDEKTSV4RRFFQ69G5FC3"),
		mustParseID(t, "01ARZ3NDEKTSV4RRFFQ69G5FC4"),
	}
	inputContentIDs := []canonical.ID{
		mustParseID(t, "01ARZ3NDEKTSV4RRFFQ69G5FD0"),
		mustParseID(t, "01ARZ3NDEKTSV4RRFFQ69G5FD1"),
		mustParseID(t, "01ARZ3NDEKTSV4RRFFQ69G5FD2"),
		mustParseID(t, "01ARZ3NDEKTSV4RRFFQ69G5FD3"),
		mustParseID(t, "01ARZ3NDEKTSV4RRFFQ69G5FD4"),
	}
	recallRunID := mustParseID(t, "01ARZ3NDEKTSV4RRFFQ69G5FB6")
	queryContentID := mustParseID(t, "01ARZ3NDEKTSV4RRFFQ69G5FB7")
	recallPipelineID := mustParseID(t, "01ARZ3NDEKTSV4RRFFQ69G5FB8")

	newInputContent := func(id canonical.ID, text string) Content {
		body := []byte(text)
		var salt canonical.ContentSalt
		commitment, err := canonical.CommitContent("generation_input", salt, body)
		if err != nil {
			t.Fatal(err)
		}
		return Content{
			ID: id, ResidentID: residentID, Class: "generation_input",
			Bytes: body, BlobHash: canonical.HashBlob(body), Commitment: commitment,
			CommitmentSalt: salt, ErasurePolicy: "independent",
		}
	}
	_, params, err := NewUnstructuredGeneratorParams(false, canonical.ByteSize(1024))
	if err != nil {
		t.Fatal(err)
	}
	empty, err := canonical.ParseCanonicalJSON([]byte(`{}`))
	if err != nil {
		t.Fatal(err)
	}
	target := AssemblyTarget{
		Head: canonical.Head{Exists: true, CommitSeq: 10, CommittedAt: 150},
		AsOf: 200, AsOfTZ: canonical.MustTimezone("UTC"),
	}
	query, err := canonical.MarshalCanonical(struct {
		ProjectionHead canonical.CommitSeq `json:"projection_head"`
	}{ProjectionHead: target.Head.CommitSeq})
	if err != nil {
		t.Fatal(err)
	}
	return PrepareDialogue{
		SourceEventID:     eventID,
		Target:            target,
		MaxInputBytes:     4096,
		LiveEventLimit:    DialogueLiveEventLimit,
		RecallDisposition: RecallDispositionSuccess,
		Generation: PrepareGeneration{
			RunID: runID, ResidentID: residentID, Purpose: GenerationPurposeDialogue,
			IdempotencyKey: DialogueObligation(eventID), Provider: "test", Model: "model",
			PromptTemplateVersion:  DialoguePromptTemplateVersionV1,
			ContextPolicyVersion:   DialogueContextPolicyVersionV3,
			MemoryRenderingVersion: MemoryRenderingVersionV2,
			PipelineVersionID:      pipelineID,
			SessionPolicyID:        &sessionID,
			PrinciplesRevisionID:   principlesID,
			PersonaRevisionID:      personaID,
			MemoryPolicyRevisionID: memoryID,
			RecallRunID:            &recallRunID,
			AsOf:                   target.AsOf,
			AsOfTZ:                 target.AsOfTZ,
			DroppedInputSummary:    prepareDialogueDroppedSummary(t, ""),
			GeneratorParams:        params,
			Inputs: []GenerationInput{
				{ID: inputIDs[0], Ordinal: 0, Role: "system", SourceType: "resident_revision", SourceID: &principlesID,
					InclusionMode: "resident_definition", Content: newInputContent(inputContentIDs[0], "principles")},
				{ID: inputIDs[1], Ordinal: 1, Role: "system", SourceType: "resident_revision", SourceID: &personaID,
					InclusionMode: "resident_definition", Content: newInputContent(inputContentIDs[1], "persona")},
				{ID: inputIDs[2], Ordinal: 2, Role: "system", SourceType: "resident_revision", SourceID: &memoryID,
					InclusionMode: "resident_definition", Content: newInputContent(inputContentIDs[2], "memory")},
				{ID: inputIDs[3], Ordinal: 3, Role: "system", SourceType: "runtime_projection",
					InclusionMode: "runtime_projection", Content: newInputContent(inputContentIDs[3], "runtime")},
				{ID: inputIDs[4], Ordinal: 4, Role: "user", SourceType: "event", SourceID: &eventID,
					InclusionMode: "current_input", Content: newInputContent(inputContentIDs[4], "hello")},
			},
			RunningOutcomeID: outcomeID,
		},
		Recall: &DialogueRecall{
			RunID: recallRunID, ResidentID: residentID, QueryContentID: queryContentID,
			PipelineVersionID: recallPipelineID, MemoryPolicyRevisionID: memoryID,
			AsOf: target.AsOf, AsOfTZ: target.AsOfTZ,
			QueryConditions: query, ContextConstraints: empty,
		},
	}
}

func prepareDialogueDroppedSummary(t *testing.T, reason MemoryRecallFallbackReason) canonical.CanonicalJSON {
	t.Helper()
	value, err := canonical.MarshalCanonical(struct {
		Backfill     canonical.Count            `json:"backfill"`
		Live         canonical.Count            `json:"live_context"`
		MemoryRecall MemoryRecallFallbackReason `json:"memory_recall,omitempty"`
	}{MemoryRecall: reason})
	if err != nil {
		t.Fatal(err)
	}
	return value
}
