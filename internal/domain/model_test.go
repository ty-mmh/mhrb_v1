package domain

import (
	"strings"
	"testing"

	"mahoroba.local/mahoroba/internal/canonical"
	"mahoroba.local/mahoroba/internal/generation"
)

func TestIngressRequestDoesNotInterpretEventSyntaxInRawText(t *testing.T) {
	text := "literal event:01ARZ3NDEKTSV4RRFFQ69G5FAV"
	request := IngressRequest{RawText: text}
	if request.RawText != text || len(request.ExplicitEventIDs) != 0 {
		t.Fatalf("request = %#v, want unchanged RawText and no inferred metadata", request)
	}
}

func TestDialogueObligationIsNamespacedByEventID(t *testing.T) {
	const raw = "01ARZ3NDEKTSV4RRFFQ69G5FAV"
	id := mustParseID(t, raw)
	if got, want := DialogueObligation(id), "dialogue:v1:"+raw; got != want {
		t.Fatalf("obligation = %q, want %q", got, want)
	}
}

func TestM7AttemptBoundDialogueCancellationCommandValidation(t *testing.T) {
	base := validM7DialogueCancellation(t)
	expectedRun := mustParseID(t, "01ARZ3NDEKTSV4RRFFQ69G5FB0")
	expectedAttempt := int64(1)
	tests := []struct {
		name      string
		mutate    func(*CancelDialogue)
		wantError string
	}{
		{name: "unbound"},
		{name: "bound", mutate: func(value *CancelDialogue) {
			value.ExpectedRunID, value.ExpectedAttemptNo = &expectedRun, &expectedAttempt
		}},
		{name: "run only", mutate: func(value *CancelDialogue) {
			value.ExpectedRunID = &expectedRun
		}, wantError: "supplied together"},
		{name: "attempt only", mutate: func(value *CancelDialogue) {
			value.ExpectedAttemptNo = &expectedAttempt
		}, wantError: "supplied together"},
		{name: "zero attempt", mutate: func(value *CancelDialogue) {
			zero := int64(0)
			value.ExpectedRunID, value.ExpectedAttemptNo = &expectedRun, &zero
		}, wantError: "must be positive"},
		{name: "non erasure code", mutate: func(value *CancelDialogue) {
			value.ExpectedRunID, value.ExpectedAttemptNo = &expectedRun, &expectedAttempt
			value.ErrorClass = generation.MustOutcomeErrorCode(generation.ErrorResidentInactive, 0).String()
		}, wantError: "only erased-source"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			value := base
			if test.mutate != nil {
				test.mutate(&value)
			}
			err := CancelDialogueCommand(value).Validate()
			if test.wantError == "" {
				if err != nil {
					t.Fatal(err)
				}
			} else if err == nil || !strings.Contains(err.Error(), test.wantError) {
				t.Fatalf("validation error = %v, want %q", err, test.wantError)
			}
		})
	}
}

func validM7DialogueCancellation(t *testing.T) CancelDialogue {
	t.Helper()
	eventID := mustParseID(t, "01ARZ3NDEKTSV4RRFFQ69G5FAV")
	id := mustParseID(t, "01ARZ3NDEKTSV4RRFFQ69G5FAW")
	sessionID := id
	dropped, err := canonical.MarshalCanonical(struct {
		Reason string `json:"reason"`
	}{Reason: "source_content_erased"})
	if err != nil {
		t.Fatal(err)
	}
	_, params, err := NewUnstructuredGeneratorParams(false, canonical.ByteSize(1024))
	if err != nil {
		t.Fatal(err)
	}
	return CancelDialogue{
		Generation: PrepareGeneration{
			RunID: id, ResidentID: id, IdempotencyKey: DialogueObligation(eventID),
			Provider: "test", Model: "model", PromptTemplateVersion: DialoguePromptTemplateVersionV1,
			ContextPolicyVersion: DialogueContextPolicyVersionV1, MemoryRenderingVersion: MemoryRenderingVersionNoneV1,
			PipelineVersionID: id, SessionPolicyID: &sessionID, PrinciplesRevisionID: id,
			PersonaRevisionID: id, MemoryPolicyRevisionID: id, RunningOutcomeID: id,
			DroppedInputSummary: dropped, GeneratorParams: params,
		},
		SourceEventID: eventID, CancelledOutcomeID: id,
		ErrorClass: generation.MustOutcomeErrorCode(generation.ErrorSourceContentErased, 0).String(),
	}
}

func mustParseID(t *testing.T, raw string) canonical.ID {
	t.Helper()
	id, err := canonical.ParseID(raw)
	if err != nil {
		t.Fatal(err)
	}
	return id
}
