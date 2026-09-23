package domain

import (
	"testing"

	"mahoroba.local/mahoroba/internal/canonical"
	"mahoroba.local/mahoroba/internal/generation"
)

func TestM7CancellationEnvelopeSemanticDigestGoldenAndRestoreInvariance(t *testing.T) {
	ids := []string{
		"01ARZ3NDEKTSV4RRFFQ69G5FAV",
		"01ARZ3NDEKTSV4RRFFQ69G5FAW",
		"01ARZ3NDEKTSV4RRFFQ69G5FAX",
		"01ARZ3NDEKTSV4RRFFQ69G5FAY",
		"01ARZ3NDEKTSV4RRFFQ69G5FAZ",
	}
	parsed := make([]canonical.ID, len(ids))
	for index, raw := range ids {
		var err error
		parsed[index], err = canonical.ParseID(raw)
		if err != nil {
			t.Fatal(err)
		}
	}
	dropped, err := canonical.MarshalCanonical(struct {
		Backfill canonical.Count `json:"backfill"`
		Live     canonical.Count `json:"live_context"`
		Reason   string          `json:"reason"`
	}{Reason: "source_content_erased"})
	if err != nil {
		t.Fatal(err)
	}
	_, params, err := NewUnstructuredGeneratorParams(false, 1)
	if err != nil {
		t.Fatal(err)
	}
	sessionID := parsed[2]
	envelope := PrepareGeneration{
		RunID: parsed[1], RunningOutcomeID: parsed[2], ResidentID: parsed[0],
		Purpose: GenerationPurposeDialogue, IdempotencyKey: "dialogue:01ARZ3NDEKTSV4RRFFQ69G5FAV",
		Provider: "mahoroba-internal", Model: "not-dispatched",
		PromptTemplateVersion: DialoguePromptTemplateVersionV1, ContextPolicyVersion: DialogueContextPolicyVersionV1,
		MemoryRenderingVersion: MemoryRenderingVersionNoneV1, PipelineVersionID: parsed[1], SessionPolicyID: &sessionID,
		PrinciplesRevisionID: parsed[2], PersonaRevisionID: parsed[3], MemoryPolicyRevisionID: parsed[4],
		AsOf: canonical.Instant(1700000000123456), AsOfTZ: canonical.MustTimezone("UTC"),
		DroppedInputSummary: dropped, GeneratorParams: params,
	}
	reason := generation.MustOutcomeErrorCode(generation.ErrorSourceContentErased, 0).String()
	digest, projection, err := CancellationEnvelopeSemanticDigest(envelope, reason)
	if err != nil {
		t.Fatal(err)
	}
	const wantProjection = `{"as_of":"1700000000123456","as_of_tz":"UTC","budget_exceeded":false,"context_policy_version":"dialogue-context-v1","dropped_input_summary":{"backfill":"0","live_context":"0","reason":"source_content_erased"},"generator_params":{"max_output_bytes":"1","streaming":false,"version":"generator-params-v2"},"idempotency_key":"dialogue:01ARZ3NDEKTSV4RRFFQ69G5FAV","memory_policy_revision_id":"01ARZ3NDEKTSV4RRFFQ69G5FAZ","memory_rendering_version":"memory-none-v1","model":"not-dispatched","model_version":null,"persona_revision_id":"01ARZ3NDEKTSV4RRFFQ69G5FAY","pipeline_version_id":"01ARZ3NDEKTSV4RRFFQ69G5FAW","principles_revision_id":"01ARZ3NDEKTSV4RRFFQ69G5FAX","prompt_template_version":"dialogue-v1","provider":"mahoroba-internal","purpose":"dialogue","reason":"source_content_erased","resident_id":"01ARZ3NDEKTSV4RRFFQ69G5FAV","sampling":{"max_tokens":null,"seed":null,"temperature":null,"top_p":null},"sessionization_policy_version_id":"01ARZ3NDEKTSV4RRFFQ69G5FAX"}`
	const wantDigest = "fb95a6faf16b09808f9a1127713b828699e84c1eb1ab71ad68ab80f9983afb11"
	if projection.String() != wantProjection {
		t.Fatalf("semantic projection = %s", projection.String())
	}
	if digest.Hex() != wantDigest {
		t.Fatalf("semantic digest = %s", digest.Hex())
	}

	// Restore-time generated identities are metadata, not replay meaning.
	restored := envelope
	restored.RunID = parsed[3]
	restored.RunningOutcomeID = parsed[4]
	restoredDigest, restoredProjection, err := CancellationEnvelopeSemanticDigest(restored, reason)
	if err != nil {
		t.Fatal(err)
	}
	if restoredDigest != digest || restoredProjection.String() != projection.String() {
		t.Fatal("restore-time generated identity changed cancellation semantics")
	}

	changedReason := generation.MustOutcomeErrorCode(generation.ErrorResidentInactive, 0).String()
	changedSummary, err := canonical.MarshalCanonical(struct {
		Backfill canonical.Count `json:"backfill"`
		Live     canonical.Count `json:"live_context"`
		Reason   string          `json:"reason"`
	}{Reason: changedReason})
	if err != nil {
		t.Fatal(err)
	}
	changedEnvelope := envelope
	changedEnvelope.DroppedInputSummary = changedSummary
	if changed, _, err := CancellationEnvelopeSemanticDigest(changedEnvelope, changedReason); err != nil || changed == digest {
		t.Fatalf("reason change digest = %s, error = %v", changed, err)
	}
	if _, _, err := CancellationEnvelopeSemanticDigest(envelope,
		generation.MustOutcomeErrorCode(generation.ErrorTimeout, 0).String()); err == nil {
		t.Fatal("retryable cancellation reason unexpectedly accepted")
	}
}

func TestM7CancellationEnvelopeSemanticDigestValidatesCompleteClosedEnvelope(t *testing.T) {
	envelope, reason := cancellationSemanticTestEnvelope(t)
	baseline, _, err := CancellationEnvelopeSemanticDigest(envelope, reason)
	if err != nil {
		t.Fatal(err)
	}
	otherID, err := canonical.ParseID("01ARZ3NDEKTSV4RRFFQ69G5FB0")
	if err != nil {
		t.Fatal(err)
	}
	changedDropped, err := canonical.MarshalCanonical(struct {
		Backfill canonical.Count `json:"backfill"`
		Live     canonical.Count `json:"live_context"`
		Reason   string          `json:"reason"`
	}{Backfill: 1, Reason: reason})
	if err != nil {
		t.Fatal(err)
	}
	_, changedParams, err := NewUnstructuredGeneratorParams(false, 2)
	if err != nil {
		t.Fatal(err)
	}

	tests := []struct {
		name   string
		mutate func(*PrepareGeneration)
	}{
		{name: "resident", mutate: func(value *PrepareGeneration) { value.ResidentID = otherID }},
		{name: "idempotency", mutate: func(value *PrepareGeneration) { value.IdempotencyKey += ":changed" }},
		{name: "provider", mutate: func(value *PrepareGeneration) { value.Provider = "restore-config-provider" }},
		{name: "model", mutate: func(value *PrepareGeneration) { value.Model = "restore-config-model" }},
		{name: "pipeline", mutate: func(value *PrepareGeneration) { value.PipelineVersionID = otherID }},
		{name: "principles", mutate: func(value *PrepareGeneration) { value.PrinciplesRevisionID = otherID }},
		{name: "persona", mutate: func(value *PrepareGeneration) { value.PersonaRevisionID = otherID }},
		{name: "memory policy", mutate: func(value *PrepareGeneration) { value.MemoryPolicyRevisionID = otherID }},
		{name: "session", mutate: func(value *PrepareGeneration) { value.SessionPolicyID = &otherID }},
		{name: "as-of", mutate: func(value *PrepareGeneration) { value.AsOf++ }},
		{name: "timezone", mutate: func(value *PrepareGeneration) { value.AsOfTZ = canonical.MustTimezone("Asia/Tokyo") }},
		{name: "budget", mutate: func(value *PrepareGeneration) { value.BudgetExceeded = true }},
		{name: "prompt version", mutate: func(value *PrepareGeneration) { value.PromptTemplateVersion = "future" }},
		{name: "context version", mutate: func(value *PrepareGeneration) { value.ContextPolicyVersion = "future" }},
		{name: "rendering version", mutate: func(value *PrepareGeneration) { value.MemoryRenderingVersion = "future" }},
		{name: "dropped summary", mutate: func(value *PrepareGeneration) { value.DroppedInputSummary = changedDropped }},
		{name: "generator params", mutate: func(value *PrepareGeneration) { value.GeneratorParams = changedParams }},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			changed := envelope
			test.mutate(&changed)
			digest, _, digestErr := CancellationEnvelopeSemanticDigest(changed, reason)
			if digestErr == nil && digest == baseline {
				t.Fatal("semantic field change was ignored")
			}
		})
	}

	for _, purpose := range []GenerationPurpose{"", GenerationPurposeSelfTalk, GenerationPurposePersonaRevision} {
		changed := envelope
		changed.Purpose = purpose
		if _, _, err := CancellationEnvelopeSemanticDigest(changed, reason); err == nil {
			t.Fatalf("non-cancellation purpose %q was accepted", purpose)
		}
	}
	withoutSession := envelope
	withoutSession.SessionPolicyID = nil
	if _, _, err := CancellationEnvelopeSemanticDigest(withoutSession, reason); err == nil {
		t.Fatal("dialogue envelope without a session policy was accepted")
	}
	withRecall := envelope
	withRecall.RecallRunID = &otherID
	if _, _, err := CancellationEnvelopeSemanticDigest(withRecall, reason); err == nil {
		t.Fatal("provider-free cancellation with recall was accepted")
	}
}

func cancellationSemanticTestEnvelope(t *testing.T) (PrepareGeneration, string) {
	t.Helper()
	ids := []string{
		"01ARZ3NDEKTSV4RRFFQ69G5FAV",
		"01ARZ3NDEKTSV4RRFFQ69G5FAW",
		"01ARZ3NDEKTSV4RRFFQ69G5FAX",
		"01ARZ3NDEKTSV4RRFFQ69G5FAY",
		"01ARZ3NDEKTSV4RRFFQ69G5FAZ",
	}
	parsed := make([]canonical.ID, len(ids))
	for index, raw := range ids {
		var err error
		parsed[index], err = canonical.ParseID(raw)
		if err != nil {
			t.Fatal(err)
		}
	}
	reason := generation.MustOutcomeErrorCode(generation.ErrorSourceContentErased, 0).String()
	dropped, err := canonical.MarshalCanonical(struct {
		Backfill canonical.Count `json:"backfill"`
		Live     canonical.Count `json:"live_context"`
		Reason   string          `json:"reason"`
	}{Reason: reason})
	if err != nil {
		t.Fatal(err)
	}
	_, params, err := NewUnstructuredGeneratorParams(false, 1)
	if err != nil {
		t.Fatal(err)
	}
	sessionID := parsed[2]
	return PrepareGeneration{
		ResidentID: parsed[0], Purpose: GenerationPurposeDialogue,
		IdempotencyKey: "dialogue:01ARZ3NDEKTSV4RRFFQ69G5FAV",
		Provider:       "mahoroba-internal", Model: "not-dispatched",
		PromptTemplateVersion: DialoguePromptTemplateVersionV1, ContextPolicyVersion: DialogueContextPolicyVersionV1,
		MemoryRenderingVersion: MemoryRenderingVersionNoneV1, PipelineVersionID: parsed[1], SessionPolicyID: &sessionID,
		PrinciplesRevisionID: parsed[2], PersonaRevisionID: parsed[3], MemoryPolicyRevisionID: parsed[4],
		AsOf: canonical.Instant(1700000000123456), AsOfTZ: canonical.MustTimezone("UTC"),
		DroppedInputSummary: dropped, GeneratorParams: params,
	}, reason
}
